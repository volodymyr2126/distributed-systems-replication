import asyncio
import time
import unittest

from tests.mock_transport import MockedTransport
from tests.replicated_log import MasterNode, ReplicationError, SecondaryNode


class ReplicationHarnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.transport = MockedTransport()
        self.master = MasterNode(self.transport)
        self.secondaries = [
            SecondaryNode(self.transport, "secondary-1"),
            SecondaryNode(self.transport, "secondary-2"),
        ]

    async def test_replication_runs_in_parallel_and_waits_for_all_acks(self) -> None:
        first, second = self.secondaries
        self.transport.set_delay(self.master, first, 0.25)
        self.transport.set_delay(self.master, second, 0.65)
        started = time.perf_counter()

        append_task = asyncio.create_task(self.master.append_msg("m1"))
        await asyncio.wait_for(
            asyncio.gather(
                *(
                    self.transport.started_event(self.master, secondary).wait()
                    for secondary in self.secondaries
                )
            ),
            timeout=1,
        )

        self.assertFalse(append_task.done())
        self.assertEqual(self.master.list_msgs(), ["m1"])
        self.assertEqual(first.list_msgs(), [])
        self.assertEqual(second.list_msgs(), [])

        message = await append_task
        elapsed = time.perf_counter() - started

        self.assertEqual(message.message, "m1")
        self.assertEqual(first.list_msgs(), ["m1"])
        self.assertEqual(second.list_msgs(), ["m1"])
        self.assertTrue(
            all(
                self.transport.acknowledged_event(self.master, secondary).is_set()
                for secondary in self.secondaries
            )
        )
        self.assertGreaterEqual(elapsed, 0.65)
        self.assertLess(elapsed, 0.8)

    async def test_delays_can_be_removed_and_every_node_keeps_the_log(self) -> None:
        first, second = self.secondaries
        self.transport.set_delay(self.master, first, 0.05)
        self.transport.set_delay(self.master, second, 0.08)
        await self.master.append_msg("m1")

        self.transport.remove_delay(self.master, first)
        self.transport.remove_delay(self.master, second)
        await self.master.append_msg("m2")
        await self.master.append_msg("m3")

        expected_messages = ["m1", "m2", "m3"]
        self.assertEqual(self.master.list_msgs(), expected_messages)
        self.assertEqual(first.list_msgs(), expected_messages)
        self.assertEqual(second.list_msgs(), expected_messages)

    async def test_transport_can_simulate_a_link_failure(self) -> None:
        first, second = self.secondaries
        self.transport.set_failure(self.master, second)

        with self.assertRaisesRegex(ReplicationError, "not acknowledged by all"):
            await self.master.append_msg("m1")

        self.assertEqual(self.master.list_msgs(), ["m1"])
        self.assertEqual(first.list_msgs(), ["m1"])
        self.assertEqual(second.list_msgs(), [])
