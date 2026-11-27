import asyncio
import tempfile
import unittest

import httpx

from main.src import main as master
from secondary.src import main as secondary


class WriteConcernHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.original_state = (
            master.messages,
            master.replicas,
            master.expected_replicas,
            master.lock,
            master.client,
        )
        self.original_retry = master.retry_initial_delay, master.retry_max_delay
        master.retry_initial_delay = master.retry_max_delay = 0.05
        master.messages = []
        master.replicas = [master.Replica(i, f"http://replica{i}") for i in range(1, 4)]
        master.expected_replicas = len(master.replicas)
        master.lock = asyncio.Lock()

        self.started = {i: asyncio.Event() for i in range(1, 4)}
        self.release = {i: asyncio.Event() for i in range(1, 4)}
        self.received = {i: [] for i in range(1, 4)}
        self.failing = set()
        for event in self.release.values():
            event.set()

        async def replica_handler(request: httpx.Request) -> httpx.Response:
            replica_id = int(request.url.host.removeprefix("replica"))
            message_id = int(request.url.params["message_id"])
            message = request.url.params["message"]
            self.started[replica_id].set()
            await self.release[replica_id].wait()
            if replica_id in self.failing:
                return httpx.Response(500, json={"detail": "boom"})
            self.received[replica_id].append(message_id)
            return httpx.Response(
                200, json={"message_id": message_id, "message": message}
            )

        master.client = httpx.AsyncClient(
            transport=httpx.MockTransport(replica_handler), timeout=1.0
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=master.app), base_url="http://master"
        )

    async def asyncTearDown(self) -> None:
        for event in self.release.values():
            event.set()
        tasks = [
            replica.task for replica in master.replicas if replica.task is not None
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        master.retry_initial_delay, master.retry_max_delay = self.original_retry

        await self.client.aclose()
        await master.client.aclose()
        (
            master.messages,
            master.replicas,
            master.expected_replicas,
            master.lock,
            master.client,
        ) = self.original_state

    async def _post(self, message: str, w: int) -> httpx.Response:
        return await self.client.post(
            "/message", params={"message": message, "w": w}
        )

    async def _wait_until_replicated(self, count: int) -> None:
        async def done() -> None:
            while any(r.acked_up_to < count for r in master.replicas):
                await asyncio.sleep(0.01)

        await asyncio.wait_for(done(), timeout=2)

    async def test_w1_returns_without_waiting_for_secondary_acks(self) -> None:
        for event in self.release.values():
            event.clear()

        response = await self._post("m1", w=1)

        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["acknowledged_count"], 1)
        self.assertEqual(result["acknowledged_by"], [])
        self.assertTrue(result["write_concern_met"])
        self.assertFalse(result["replication_complete"])
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in self.started.values())),
            timeout=1,
        )
        self.assertEqual(master.messages[0].message_id, 1)
        self.assertEqual(self.received, {1: [], 2: [], 3: []})

    async def test_w2_returns_after_one_secondary_ack(self) -> None:
        self.release[2].clear()
        self.release[3].clear()

        response = await self._post("m1", w=2)

        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["acknowledged_count"], 2)
        self.assertEqual(result["acknowledged_by"], [1])
        self.assertFalse(result["replication_complete"])
        await asyncio.wait_for(
            asyncio.gather(self.started[2].wait(), self.started[3].wait()),
            timeout=1,
        )
        self.assertEqual(self.received[1], [1])
        self.assertEqual(self.received[2], [])
        self.assertEqual(self.received[3], [])

    async def test_w3_returns_after_two_secondary_acks(self) -> None:
        self.release[3].clear()

        response = await self._post("m1", w=3)

        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["acknowledged_count"], 3)
        self.assertEqual(result["acknowledged_by"], [1, 2])
        self.assertFalse(result["replication_complete"])
        await asyncio.wait_for(self.started[3].wait(), timeout=1)
        self.assertEqual(self.received[3], [])

    async def test_concurrent_writes_reach_each_secondary_in_message_order(self) -> None:
        for event in self.release.values():
            event.clear()

        first_task = asyncio.create_task(self._post("m1", w=1))
        second_task = asyncio.create_task(self._post("m2", w=1))
        first_response, second_response = await asyncio.gather(
            first_task, second_task
        )

        self.assertEqual(
            sorted(
                [
                    first_response.json()["message_id"],
                    second_response.json()["message_id"],
                ]
            ),
            [1, 2],
        )
        self.assertEqual([message.message_id for message in master.messages], [1, 2])
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in self.started.values())),
            timeout=1,
        )
        self.assertEqual(self.received, {1: [], 2: [], 3: []})

        for event in self.release.values():
            event.set()
        await self._wait_until_replicated(2)
        self.assertEqual(self.received, {1: [1, 2], 2: [1, 2], 3: [1, 2]})

    async def test_write_concern_above_cluster_size_is_rejected(self) -> None:
        response = await self._post("m1", w=5)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(master.messages, [])

    async def test_failed_delivery_is_retried_and_w3_blocks_until_it_succeeds(self) -> None:
        self.failing.add(3)
        task = asyncio.create_task(self._post("m1", w=4))
        await asyncio.sleep(0.3)
        self.assertFalse(task.done(), "w=4 must block while replica 3 fails")

        # Parallel clients with a lower write concern are not blocked.
        response = await asyncio.wait_for(self._post("m2", w=3), timeout=1)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(task.done())

        self.failing.clear()
        response = await asyncio.wait_for(task, timeout=2)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["acknowledged_by"], [1, 2, 3])
        await self._wait_until_replicated(2)
        self.assertEqual(self.received[3], [1, 2])

    async def test_no_quorum_makes_master_read_only(self) -> None:
        self.assertEqual((await self._post("ok", w=1)).status_code, 200)
        for replica in master.replicas[:3]:
            replica.status = master.UNHEALTHY

        response = await self._post("rejected", w=1)

        self.assertEqual(response.status_code, 503)
        self.assertIn("read-only", response.json()["detail"])
        self.assertEqual([m.message for m in master.messages], ["ok"])
        self.assertEqual((await self.client.get("/message")).status_code, 200)
        health = (await self.client.get("/health")).json()
        self.assertTrue(health["read_only"])

    async def test_health_reports_secondary_statuses(self) -> None:
        master.replicas[1].status = master.SUSPECTED
        health = (await self.client.get("/health")).json()
        self.assertFalse(health["read_only"])
        self.assertEqual(
            [s["status"] for s in health["secondaries"]],
            ["healthy", "suspected", "healthy"],
        )

    async def test_writes_accepted_before_all_secondaries_registered(self) -> None:
        master.expected_replicas = 4
        response = await self._post("m1", w=1)
        self.assertEqual(response.status_code, 200)
        master.expected_replicas = 3

    async def test_retry_delay_stays_bounded_after_unlimited_attempts(self) -> None:
        for attempt in (0, 31, 1024, 1_000_000):
            with self.subTest(attempt=attempt):
                delay = master.backoff_delay(attempt)
                self.assertLessEqual(delay, master.retry_max_delay)
                self.assertGreaterEqual(delay, 0)

    async def _register(self, url: str, node_id: str) -> int:
        response = await self.client.post(
            "/replica", params={"replica_url": url, "node_id": node_id}
        )
        self.assertEqual(response.status_code, 200)
        return response.json()["replica_id"]

    async def test_secondary_with_new_url_is_not_counted_twice(self) -> None:
        master.replicas = []
        master.expected_replicas = 2
        first_id = await self._register("http://replica1", "node-a")
        task = asyncio.create_task(self._post("m1", w=3))
        await self._wait_until_replicated(1)

        # The same node comes back under another address and gets m1 again.
        self.failing.add(1)
        self.assertEqual(await self._register("http://replica3", "node-a"), first_id)
        await self._wait_until_replicated(1)
        await asyncio.sleep(0.1)
        self.assertEqual([r.url for r in master.replicas], ["http://replica3"])
        self.assertFalse(task.done(), "one node must count as one acknowledgement")

        await self._register("http://replica2", "node-b")
        response = await asyncio.wait_for(task, timeout=2)
        self.assertEqual(len(response.json()["acknowledged_by"]), 2)
        self.assertTrue(response.json()["replication_complete"])

    async def test_url_taken_over_by_another_node_replaces_the_stale_entry(
        self,
    ) -> None:
        master.replicas = []
        master.expected_replicas = 2
        await self._register("http://replica1", "node-a")
        stale_sender = master.replicas[0].task

        await self._register("http://replica1", "node-b")

        self.assertEqual([r.node_id for r in master.replicas], ["node-b"])
        await asyncio.gather(stale_sender, return_exceptions=True)
        self.assertTrue(stale_sender.cancelled())

    async def test_late_answer_from_before_reregistration_is_ignored(self) -> None:
        master.replicas = []
        master.expected_replicas = 1
        self.release[1].clear()  # the old instance does not answer yet
        await self._register("http://replica1", "node-a")
        await self._post("m1", w=1)
        await asyncio.wait_for(self.started[1].wait(), timeout=1)

        self.release[2].clear()
        await self._register("http://replica2", "node-a")
        self.release[1].set()
        await asyncio.sleep(0.1)

        self.assertEqual(master.replicas[0].acked_up_to, 0)

    async def test_secondary_node_id_is_generated_once_and_reused(self) -> None:
        original = secondary.configured_node_id, secondary.node_id_file
        try:
            with tempfile.TemporaryDirectory() as directory:
                secondary.configured_node_id = ""
                secondary.node_id_file = ""
                self.assertIsNone(secondary.get_node_id())

                secondary.node_id_file = f"{directory}/node/id"
                generated = secondary.get_node_id()
                self.assertTrue(generated)
                self.assertEqual(secondary.get_node_id(), generated)

                secondary.configured_node_id = "explicit"
                self.assertEqual(secondary.get_node_id(), "explicit")
        finally:
            secondary.configured_node_id, secondary.node_id_file = original

    async def test_secondary_buffers_out_of_order_and_deduplicates(self) -> None:
        original_state = secondary.messages, secondary.buffer, secondary.message_lock
        secondary.messages, secondary.buffer = [], {}
        secondary.message_lock = asyncio.Lock()
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=secondary.app),
            base_url="http://secondary",
        )
        try:
            async def send(message_id: int, message: str) -> httpx.Response:
                return await client.post(
                    "/message", params={"message": message, "message_id": message_id}
                )

            self.assertEqual((await send(2, "m2")).status_code, 200)
            self.assertEqual(secondary.messages, [])  # hidden until m1 arrives
            self.assertEqual((await send(1, "m1")).status_code, 200)
            self.assertEqual((await send(1, "m1")).status_code, 200)  # duplicate
            self.assertEqual((await send(2, "m2")).status_code, 200)  # duplicate
            self.assertEqual((await send(2, "other")).status_code, 409)
            self.assertEqual(
                [message.message_id for message in secondary.messages], [1, 2]
            )
            self.assertEqual((await client.get("/health")).json()["last_id"], 2)
        finally:
            await client.aclose()
            secondary.messages, secondary.buffer, secondary.message_lock = original_state


if __name__ == "__main__":
    unittest.main()
