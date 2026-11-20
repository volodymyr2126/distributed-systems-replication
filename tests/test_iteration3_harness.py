import asyncio
import time
import unittest

from tests.mock_transport import MockedTransport
from tests.replicated_log import MasterNode, SecondaryNode

# The task text uses 10 s; the same scenarios are run on a smaller time scale.
RETRY = 0.2


class Iteration3HarnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.transport = MockedTransport()
        self.master = MasterNode(
            self.transport, retry_interval=RETRY, ack_timeout=RETRY
        )
        self.s1 = SecondaryNode(self.transport, "secondary-1")
        self.s2 = SecondaryNode(self.transport, "secondary-2")

    async def test_dropped_messages_are_retried(self) -> None:
        self.transport.drop_message(self.master, self.s1)
        await self.master.append_msg("m1", w=1)  # w=1 must not block

        await asyncio.sleep(RETRY * 5)  # several failed attempts
        self.assertEqual(self.s1.list_msgs(), [])

        self.transport.remove_drop(self.master, self.s1)
        await asyncio.sleep(RETRY * 3)
        self.assertEqual(self.s1.list_msgs(), ["m1"])

    async def test_retries_with_delay_are_deduplicated(self) -> None:
        self.transport.set_delay(self.master, self.s2, RETRY * 4)
        await self.master.append_msg("m1", w=1)
        await self.master.append_msg("m2", w=1)

        deliveries = []
        original = self.s2.receive
        self.s2.receive = lambda m: (deliveries.append(m.message), original(m))[1]

        await asyncio.sleep(RETRY * 10)  # delay exceeds the ack timeout: retries
        self.transport.remove_delay(self.master, self.s2)
        await asyncio.sleep(RETRY * 5)

        self.assertEqual(self.s2.list_msgs(), ["m1", "m2"])
        self.assertGreater(deliveries.count("m2"), 1, "m2 should arrive repeatedly")

    async def test_total_order_hides_messages_after_a_gap(self) -> None:
        # Retry interval is large so that the state can be checked in between.
        self.master.retry_interval = RETRY * 5
        await self.master.append_msg("m1", w=1)
        await self.master.append_msg("m2", w=1)
        await asyncio.sleep(0.05)

        self.transport.drop_message(self.master, self.s1)
        await self.master.append_msg("m3", w=1)
        await asyncio.sleep(0.05)
        self.transport.remove_drop(self.master, self.s1)
        await self.master.append_msg("m4", w=1)
        await asyncio.sleep(0.05)

        # m4 reached S1 but is hidden until m3 arrives.
        self.assertEqual(self.s1.list_msgs(), ["m1", "m2"])
        self.assertEqual(self.s2.list_msgs(), ["m1", "m2", "m3", "m4"])

        await asyncio.sleep(RETRY * 8)  # after the retry interval
        self.assertEqual(self.s1.list_msgs(), ["m1", "m2", "m3", "m4"])

    async def test_acceptance_scenario_secondary_joins_late(self) -> None:
        transport = MockedTransport()
        master = MasterNode(
            transport, retry_interval=RETRY, ack_timeout=RETRY, expected_secondaries=2
        )
        SecondaryNode(transport, "S1")

        await master.append_msg("Msg1", w=1)
        await master.append_msg("Msg2", w=2)
        msg3 = asyncio.create_task(master.append_msg("Msg3", w=3))
        await asyncio.sleep(0.1)
        self.assertFalse(msg3.done(), "w=3 must wait for the missing secondary")

        # Other clients are not blocked by the waiting one.
        await asyncio.wait_for(master.append_msg("Msg4", w=1), timeout=0.5)
        self.assertFalse(msg3.done())

        s2 = SecondaryNode(transport, "S2")
        await asyncio.wait_for(msg3, timeout=2)
        await master.wait_for_replication()
        self.assertEqual(s2.list_msgs(), ["Msg1", "Msg2", "Msg3", "Msg4"])

    async def test_parallel_client_is_not_blocked_by_unavailable_secondary(self) -> None:
        self.transport.drop_message(self.master, self.s2)
        blocked = asyncio.create_task(self.master.append_msg("a", w=3))
        await asyncio.sleep(0)  # let "a" get its id first
        started = time.perf_counter()
        await self.master.append_msg("b", w=2)  # S1 is fine
        self.assertLess(time.perf_counter() - started, RETRY)
        self.assertFalse(blocked.done())

        self.transport.remove_drop(self.master, self.s2)
        await asyncio.wait_for(blocked, timeout=2)
        await self.master.wait_for_replication()
        for node in (self.master, self.s1, self.s2):
            self.assertEqual(node.list_msgs(), ["a", "b"])


if __name__ == "__main__":
    unittest.main()
