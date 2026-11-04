import asyncio
import time
import unittest

from tests.mock_transport import MockedTransport
from tests.replicated_log import MasterNode, ReplicationError, SecondaryNode

DELAY = 0.5


class Iteration2HarnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.transport = MockedTransport()
        self.master = MasterNode(self.transport)
        self.s1 = SecondaryNode(self.transport, "secondary-1")
        self.s2 = SecondaryNode(self.transport, "secondary-2")

    async def test_acceptance_scenario_with_eventual_consistency(self) -> None:
        self.transport.set_delay(self.master, self.s2, DELAY)

        await self.master.append_msg("Msg1", w=1)
        await self.master.append_msg("Msg2", w=2)

        # Msg3 (w=3) must wait for slow S2; Msg4 (w=1) must not be blocked by it.
        start = time.perf_counter()
        msg3 = asyncio.create_task(self.master.append_msg("Msg3", w=3))
        await asyncio.sleep(0.05)
        await self.master.append_msg("Msg4", w=1)
        self.assertLess(time.perf_counter() - start, DELAY / 2)
        self.assertFalse(msg3.done())

        await asyncio.sleep(0.05)  # let the zero-delay S1 delivery land

        # Temporary inconsistency: S2 lags behind master and S1.
        self.assertEqual(self.master.list_msgs(), ["Msg1", "Msg2", "Msg3", "Msg4"])
        self.assertEqual(self.s1.list_msgs(), ["Msg1", "Msg2", "Msg3", "Msg4"])
        lagging = self.s2.list_msgs()
        self.assertLess(len(lagging), 4)
        self.assertEqual(lagging, self.master.list_msgs()[: len(lagging)])

        await msg3
        self.assertGreaterEqual(time.perf_counter() - start, DELAY * 0.9)

        # Eventual consistency.
        await self.master.wait_for_replication()
        self.assertEqual(self.s2.list_msgs(), ["Msg1", "Msg2", "Msg3", "Msg4"])

    async def test_w1_does_not_wait_and_w2_waits_for_fastest_secondary(self) -> None:
        self.transport.set_delay(self.master, self.s1, DELAY)
        self.transport.set_delay(self.master, self.s2, DELAY * 3)

        start = time.perf_counter()
        await self.master.append_msg("a", w=1)
        self.assertLess(time.perf_counter() - start, DELAY / 2)

        await self.master.append_msg("b", w=2)
        self.assertGreaterEqual(time.perf_counter() - start, DELAY * 0.9)
        self.assertLess(time.perf_counter() - start, DELAY * 2)
        await self.master.wait_for_replication()

    async def test_invalid_write_concern_is_rejected(self) -> None:
        for w in (0, 4):
            with self.assertRaises(ValueError):
                await self.master.append_msg("x", w=w)

    async def test_w_unmet_because_of_link_failures(self) -> None:
        self.transport.set_failure(self.master, self.s1)
        self.transport.set_failure(self.master, self.s2)
        await self.master.append_msg("ok", w=1)
        with self.assertRaises(ReplicationError):
            await self.master.append_msg("bad", w=2)

    async def test_reordered_messages_are_listed_in_order(self) -> None:
        self.transport.reorder_messages(self.master, self.s1, messages_count=2)

        await asyncio.gather(
            self.master.append_msg("m1"), self.master.append_msg("m2")
        )

        self.assertEqual(self.s1.list_msgs(), ["m1", "m2"])
        self.assertEqual(self.s2.list_msgs(), ["m1", "m2"])

    async def test_reordering_really_delivers_in_reverse_wire_order(self) -> None:
        self.transport.reorder_messages(self.master, self.s1, messages_count=2)
        delivered = []
        original = self.s1.receive
        self.s1.receive = lambda m: (delivered.append(m.message), original(m))[1]

        await asyncio.gather(
            self.master.append_msg("m1"), self.master.append_msg("m2")
        )
        self.assertEqual(delivered, ["m2", "m1"])
        self.assertEqual(self.s1.list_msgs(), ["m1", "m2"])

    async def test_total_order_under_many_concurrent_reordered_writes(self) -> None:
        self.transport.reorder_messages(self.master, self.s1, messages_count=4)
        await asyncio.gather(
            *(self.master.append_msg(f"m{i}") for i in range(1, 9))
        )
        expected = [f"m{i}" for i in range(1, 9)]
        for node in (self.master, self.s1, self.s2):
            self.assertEqual(node.list_msgs(), expected)


if __name__ == "__main__":
    unittest.main()
