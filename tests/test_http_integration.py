import json
import os
from pathlib import Path
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
            f"{self.master_url}/message", params={"message": message}
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


if __name__ == "__main__":
    unittest.main()
