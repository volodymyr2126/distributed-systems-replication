import asyncio
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
        master.messages = []
        master.replicas = [master.Replica(i, f"http://replica{i}") for i in range(1, 4)]
        master.expected_replicas = len(master.replicas)
        master.lock = asyncio.Lock()

        self.started = {i: asyncio.Event() for i in range(1, 4)}
        self.release = {i: asyncio.Event() for i in range(1, 4)}
        self.received = {i: [] for i in range(1, 4)}
        for event in self.release.values():
            event.set()

        async def replica_handler(request: httpx.Request) -> httpx.Response:
            replica_id = int(request.url.host.removeprefix("replica"))
            message_id = int(request.url.params["message_id"])
            message = request.url.params["message"]
            self.started[replica_id].set()
            await self.release[replica_id].wait()
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
            replica.last_replication
            for replica in master.replicas
            if replica.last_replication is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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
        await asyncio.wait_for(
            asyncio.gather(
                *(
                    replica.last_replication
                    for replica in master.replicas
                    if replica.last_replication is not None
                )
            ),
            timeout=1,
        )
        self.assertEqual(self.received, {1: [1, 2], 2: [1, 2], 3: [1, 2]})

    async def test_write_concern_above_cluster_size_is_rejected(self) -> None:
        response = await self._post("m1", w=5)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(master.messages, [])

    async def test_secondary_rejects_out_of_order_message_ids(self) -> None:
        original_state = secondary.messages, secondary.message_lock
        secondary.messages = []
        secondary.message_lock = asyncio.Lock()
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=secondary.app),
            base_url="http://secondary",
        )
        try:
            out_of_order = await client.post(
                "/message", params={"message": "m2", "message_id": 2}
            )
            first = await client.post(
                "/message", params={"message": "m1", "message_id": 1}
            )
            second = await client.post(
                "/message", params={"message": "m2", "message_id": 2}
            )

            self.assertEqual(out_of_order.status_code, 409)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(
                [message.message_id for message in secondary.messages], [1, 2]
            )
        finally:
            await client.aclose()
            secondary.messages, secondary.message_lock = original_state


if __name__ == "__main__":
    unittest.main()
