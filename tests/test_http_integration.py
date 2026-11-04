import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import subprocess
import time
import unittest
from uuid import uuid4

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ReplicationHttpIntegrationTests(unittest.TestCase):
    @classmethod
    def _compose(cls, *args: str) -> str:
        result = subprocess.run(
            ["docker", "compose", *args],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    @classmethod
    def _local_url(cls, service: str, index: int | None = None) -> str:
        args = ["port"]
        if index is not None:
            args.extend(["--index", str(index)])
        args.extend([service, "80"])
        binding = cls._compose(*args).splitlines()[0]
        _, port = binding.rsplit(":", 1)
        return f"http://127.0.0.1:{port}"

    @classmethod
    def setUpClass(cls) -> None:
        try:
            master_containers = cls._compose("ps", "-q", "main").splitlines()
            secondary_containers = cls._compose("ps", "-q", "secondary").splitlines()
            if not master_containers or not secondary_containers:
                raise unittest.SkipTest(
                    "start the Docker Compose services before running HTTP integration tests"
                )

            cls.master_url = cls._local_url("main")
            cls.secondary_urls = [
                cls._local_url("secondary", index)
                for index in range(1, len(secondary_containers) + 1)
            ]
            compose_config = cls._compose("config", "--format", "json")
            cls.secondary_delay = float(
                json.loads(compose_config)["services"]["secondary"]
                ["environment"].get("SLEEP_AFTER_REQUESTS", "0")
            )
        except FileNotFoundError as error:
            raise unittest.SkipTest(f"Docker Compose CLI is unavailable: {error}") from error
        except subprocess.CalledProcessError as error:
            output = error.stderr.strip() or error.stdout.strip()
            raise RuntimeError(f"Docker Compose command failed: {output}") from error

        cls.client = httpx.Client(timeout=max(30.0, cls.secondary_delay + 5))

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "client"):
            cls.client.close()

    def test_post_waits_for_acks_and_messages_are_readable_everywhere(self) -> None:
        message = f"integration-{uuid4()}"
        started = time.perf_counter()
        response = self.client.post(
            f"{self.master_url}/message",
            params={"message": message, "w": len(self.secondary_urls) + 1},
        )
        elapsed = time.perf_counter() - started

        response.raise_for_status()
        post_result = response.json()
        self.assertTrue(post_result["stored_on_master"])
        self.assertTrue(post_result["replication_complete"])
        self.assertEqual(len(post_result["acknowledged_by"]), len(self.secondary_urls))
        self.assertEqual(post_result["failed"], {})
        self.assertGreaterEqual(elapsed, self.secondary_delay)

        master_response = self.client.get(f"{self.master_url}/message")
        master_response.raise_for_status()
        master_messages = master_response.json()
        self.assertIn(
            {"message_id": post_result["message_id"], "message": message},
            master_messages,
        )

        for secondary_url in self.secondary_urls:
            with self.subTest(secondary_url=secondary_url):
                secondary_response = self.client.get(f"{secondary_url}/message")
                secondary_response.raise_for_status()
                self.assertIn(
                    {"message_id": post_result["message_id"], "message": message},
                    secondary_response.json(),
                )

    # ---- helpers -------------------------------------------------------

    def _messages(self, url: str) -> list[dict]:
        response = self.client.get(f"{url}/message")
        response.raise_for_status()
        return response.json()

    def _tagged(self, url: str, tag: str) -> list[str]:
        return [m["message"] for m in self._messages(url) if tag in m["message"]]

    def _post(self, message: str, w: int) -> httpx.Response:
        return self.client.post(
            f"{self.master_url}/message", params={"message": message, "w": w}
        )

    def _wait_until_replicated(self, timeout: float | None = None) -> None:
        """Block until every secondary holds as many messages as the master."""
        timeout = timeout or (self.secondary_delay * 10 + 15)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            expected = len(self._messages(self.master_url))
            if all(len(self._messages(u)) == expected for u in self.secondary_urls):
                return
            time.sleep(0.25)
        self.fail("secondaries did not catch up with the master in time")

    def _require_delay(self) -> None:
        if self.secondary_delay < 1:
            self.skipTest("needs SLEEP_AFTER_REQUESTS >= 1 on the secondaries")

    def setUp(self) -> None:
        # Earlier tests leave background replication running; start quiescent.
        self._wait_until_replicated()

    # ---- health / validation -------------------------------------------

    def test_health_endpoints(self) -> None:
        for url in [self.master_url, *self.secondary_urls]:
            with self.subTest(url=url):
                response = self.client.get(f"{url}/health")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {"status": "healthy"})

    def test_invalid_write_concern_is_rejected_and_nothing_is_stored(self) -> None:
        tag = f"invalid-{uuid4()}"
        before = len(self._messages(self.master_url))
        too_high = len(self.secondary_urls) + 2
        for params in (
            {"message": tag, "w": 0},
            {"message": tag, "w": -1},
            {"message": tag, "w": too_high},
            {"message": tag, "w": "abc"},
            {"message": tag},  # w is required
        ):
            with self.subTest(params=params):
                response = self.client.post(f"{self.master_url}/message", params=params)
                self.assertEqual(response.status_code, 422)
        self.assertEqual(len(self._messages(self.master_url)), before)

    # ---- write concern semantics ----------------------------------------

    def test_w1_returns_before_secondaries_acknowledge(self) -> None:
        self._require_delay()
        tag = f"w1-{uuid4()}"
        started = time.perf_counter()
        response = self._post(tag, w=1)
        elapsed = time.perf_counter() - started

        response.raise_for_status()
        result = response.json()
        self.assertLess(elapsed, self.secondary_delay / 2)
        self.assertTrue(result["write_concern_met"])
        self.assertEqual(result["acknowledged_count"], 1)
        self.assertFalse(result["replication_complete"])
        self.assertEqual(self._tagged(self.master_url, tag), [tag])

        # Eventual consistency: secondaries get it without any further request.
        self._wait_until_replicated()
        for url in self.secondary_urls:
            self.assertEqual(self._tagged(url, tag), [tag])

    def test_w2_waits_for_one_secondary(self) -> None:
        self._require_delay()
        if len(self.secondary_urls) < 1:
            self.skipTest("needs at least one secondary")
        started = time.perf_counter()
        response = self._post(f"w2-{uuid4()}", w=2)
        elapsed = time.perf_counter() - started

        response.raise_for_status()
        self.assertGreaterEqual(elapsed, self.secondary_delay * 0.9)
        self.assertGreaterEqual(response.json()["acknowledged_count"], 2)

    def test_acceptance_scenario_w1_w2_w3_with_slow_secondaries(self) -> None:
        self._require_delay()
        if len(self.secondary_urls) < 2:
            self.skipTest("needs at least two secondaries")
        tag = f"acc-{uuid4()}"
        msgs = [f"{tag}-Msg{i}" for i in range(1, 5)]

        self.assertEqual(self._post(msgs[0], w=1).status_code, 200)
        self.assertEqual(self._post(msgs[1], w=2).status_code, 200)

        with ThreadPoolExecutor(max_workers=1) as pool:
            started = time.perf_counter()
            msg3 = pool.submit(self._post, msgs[2], 3)
            time.sleep(0.5)  # make sure Msg3 is in flight first

            # Msg4 (w=1) must not be blocked by the pending w=3 write.
            msg4_started = time.perf_counter()
            self.assertEqual(self._post(msgs[3], w=1).status_code, 200)
            self.assertLess(time.perf_counter() - msg4_started, self.secondary_delay / 2)
            self.assertFalse(msg3.done(), "w=3 should still be waiting")

            # Master has everything; secondaries are temporarily behind.
            self.assertEqual(self._tagged(self.master_url, tag), msgs)
            for url in self.secondary_urls:
                lagging = self._tagged(url, tag)
                self.assertNotIn(msgs[3], lagging)
                self.assertEqual(lagging, msgs[: len(lagging)])

            response = msg3.result()
            response.raise_for_status()
            self.assertGreaterEqual(response.json()["acknowledged_count"], 3)
            self.assertGreaterEqual(time.perf_counter() - started, self.secondary_delay * 0.9)

        # Eventually every node converges on the same ordered log.
        self._wait_until_replicated()
        for url in [self.master_url, *self.secondary_urls]:
            self.assertEqual(self._tagged(url, tag), msgs)

    # ---- total ordering --------------------------------------------------

    def test_concurrent_writes_get_one_total_order_on_every_node(self) -> None:
        tag = f"order-{uuid4()}"
        count = 4
        with ThreadPoolExecutor(max_workers=count) as pool:
            responses = list(
                pool.map(lambda i: self._post(f"{tag}-{i}", w=1), range(count))
            )
        for response in responses:
            response.raise_for_status()
        ids = sorted(r.json()["message_id"] for r in responses)
        self.assertEqual(ids, list(range(ids[0], ids[0] + count)), "IDs must be gap-free")

        self._wait_until_replicated()
        master_log = self._messages(self.master_url)
        self.assertEqual(
            [m["message_id"] for m in master_log], list(range(1, len(master_log) + 1))
        )
        for url in self.secondary_urls:
            self.assertEqual(self._messages(url), master_log)

    # ---- secondary guards ------------------------------------------------

    def test_secondary_rejects_gaps_and_conflicting_duplicates_but_accepts_redelivery(
        self,
    ) -> None:
        self.assertEqual(self._post(f"guard-{uuid4()}", w=1).status_code, 200)
        self._wait_until_replicated()
        secondary = self.secondary_urls[0]
        existing = self._messages(secondary)[-1]
        before = self._messages(secondary)

        # Gap: ahead of the next expected id.
        gap = self.client.post(
            f"{secondary}/message",
            params={"message": "gap", "message_id": existing["message_id"] + 5},
        )
        self.assertEqual(gap.status_code, 409)

        # Same id, different content.
        conflict = self.client.post(
            f"{secondary}/message",
            params={"message": "different", "message_id": existing["message_id"]},
        )
        self.assertEqual(conflict.status_code, 409)

        # Identical re-delivery is idempotent (lost-ACK retry).
        redelivery = self.client.post(
            f"{secondary}/message",
            params={"message": existing["message"], "message_id": existing["message_id"]},
        )
        self.assertEqual(redelivery.status_code, 200)

        self.assertEqual(self._messages(secondary), before)


if __name__ == "__main__":
    unittest.main()
