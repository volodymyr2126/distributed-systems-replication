from fastapi import FastAPI, HTTPException, Query
from pydantic import AnyHttpUrl
from contextlib import asynccontextmanager
from typing import List
import os
import random
import asyncio
import httpx
from common import Message, configure_logger

lock = asyncio.Lock()
# Per-request timeout for one delivery attempt; a slow/unresponsive secondary
# makes the attempt fail and the delivery is retried.
replication_timeout = float(os.getenv("REPLICATION_TIMEOUT", "60"))
retry_initial_delay = float(os.getenv("RETRY_INITIAL_DELAY", "0.5"))
retry_max_delay = float(os.getenv("RETRY_MAX_DELAY", "10"))
heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL", "2"))
heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT", "2"))
# Missed heartbeats: >=1 -> suspected, >= this value -> unhealthy.
heartbeat_unhealthy_after = int(os.getenv("HEARTBEAT_UNHEALTHY_AFTER", "3"))
client = httpx.AsyncClient(timeout=replication_timeout)

logger = configure_logger(__name__)

HEALTHY = "healthy"
SUSPECTED = "suspected"
UNHEALTHY = "unhealthy"


class Replica:

    def __init__(self, replica_id: int, url: str):
        self.replica_id = replica_id
        self.url = url
        # Messages 1..acked_up_to are acknowledged by this secondary. The
        # sender delivers strictly in id order, one message at a time.
        self.acked_up_to = 0
        self.status = HEALTHY
        self.missed_heartbeats = 0
        self.wake = asyncio.Event()  # new message to send
        self.retry_now = asyncio.Event()  # cut the retry back-off short
        self.task: asyncio.Task | None = None

    def __repr__(self):
        return f"Replica(id={self.replica_id}, url={self.url})"


messages: List[Message] = []
replicas: List[Replica] = []
expected_replicas = int(os.getenv("SECONDARY_REPLICAS_COUNT", 2))
# Futures of POST /message handlers waiting for more acknowledgements.
ack_waiters: set[asyncio.Future] = set()
heartbeat_task: asyncio.Task | None = None


def notify_acks() -> None:
    for waiter in list(ack_waiters):
        if not waiter.done():
            waiter.set_result(None)
    ack_waiters.clear()


def backoff_delay(attempt: int) -> float:
    """Exponential back-off with jitter, capped at ``retry_max_delay``."""
    delay = min(retry_max_delay, retry_initial_delay * (2 ** attempt))
    return delay / 2 + random.uniform(0, delay / 2)


def quorum_size() -> int:
    return (expected_replicas + 1) // 2 + 1


def alive_nodes() -> int:
    """Master plus every registered secondary that is not unhealthy."""
    return 1 + sum(1 for replica in replicas if replica.status != UNHEALTHY)


def has_quorum() -> bool:
    return alive_nodes() >= quorum_size()


async def replicate_message(replica: Replica, message: Message) -> bool:
    """One delivery attempt. Returns True when the secondary acknowledged."""
    try:
        response = await client.post(
            f"{replica.url}/message",
            params={"message": message.message, "message_id": message.message_id},
        )
        response.raise_for_status()
        acknowledgement = response.json()
        if (
            not isinstance(acknowledgement, dict)
            or acknowledgement.get("message_id") != message.message_id
        ):
            raise ValueError(
                f"Replica {replica.replica_id} acknowledged the wrong message"
            )
    except (httpx.HTTPError, ValueError) as error:
        logger.warning(
            "Delivery of message %s to %s failed: %r",
            message.message_id, replica.url, error,
        )
        return False

    last_id = acknowledgement.get("last_id")
    if isinstance(last_id, int) and last_id < message.message_id:
        # The secondary has a gap (e.g. it restarted and lost its log):
        # resume from what it really holds instead of counting this ack.
        replica.acked_up_to = min(replica.acked_up_to, last_id)
        return True
    replica.acked_up_to = max(replica.acked_up_to, message.message_id)
    notify_acks()
    return True


async def replica_sender(replica: Replica) -> None:
    """Delivers the log to one secondary in order, retrying forever."""
    attempt = 0
    while True:
        if replica.acked_up_to >= len(messages):
            replica.wake.clear()
            if replica.acked_up_to >= len(messages):
                await replica.wake.wait()
            continue

        message = messages[replica.acked_up_to]
        if await replicate_message(replica, message):
            attempt = 0
            continue

        replica.retry_now.clear()
        try:
            await asyncio.wait_for(
                replica.retry_now.wait(), timeout=backoff_delay(attempt)
            )
        except asyncio.TimeoutError:
            pass
        attempt += 1


def ensure_sender(replica: Replica) -> None:
    if replica.task is None or replica.task.done():
        replica.task = asyncio.create_task(replica_sender(replica))
    replica.wake.set()


async def check_replica(replica: Replica) -> None:
    try:
        response = await client.get(
            f"{replica.url}/health", timeout=heartbeat_timeout
        )
        response.raise_for_status()
        body = response.json()
    except (httpx.HTTPError, ValueError):
        replica.missed_heartbeats += 1
        replica.status = (
            UNHEALTHY
            if replica.missed_heartbeats >= heartbeat_unhealthy_after
            else SUSPECTED
        )
        return

    recovered = replica.status != HEALTHY
    replica.missed_heartbeats = 0
    replica.status = HEALTHY
    last_id = body.get("last_id") if isinstance(body, dict) else None
    if isinstance(last_id, int) and last_id < replica.acked_up_to:
        logger.warning(
            "Replica %s lost messages (has %s, acked %s); resending",
            replica.replica_id, last_id, replica.acked_up_to,
        )
        replica.acked_up_to = last_id
        recovered = True
    if recovered:
        ensure_sender(replica)
        replica.retry_now.set()


async def heartbeat_loop() -> None:
    while True:
        await asyncio.gather(*(check_replica(r) for r in list(replicas)))
        await asyncio.sleep(heartbeat_interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global heartbeat_task
    heartbeat_task = asyncio.create_task(heartbeat_loop())
    yield
    heartbeat_task.cancel()
    for replica in replicas:
        if replica.task is not None:
            replica.task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health_check():
    read_only = not has_quorum()
    return {
        "status": "read-only" if read_only else "healthy",
        "read_only": read_only,
        "quorum": quorum_size(),
        "alive_nodes": alive_nodes(),
        "cluster_size": expected_replicas + 1,
        "secondaries": [
            {
                "replica_id": replica.replica_id,
                "url": replica.url,
                "status": replica.status,
                "acked_up_to": replica.acked_up_to,
            }
            for replica in replicas
        ],
    }


@app.post("/replica")
async def register_replica(replica_url: AnyHttpUrl):
    normalized_url = str(replica_url).rstrip("/")

    async with lock:
        for replica in replicas:
            if replica.url == normalized_url:
                # A rejoining secondary may have lost its in-memory log.
                replica.acked_up_to = 0
                replica.status = HEALTHY
                replica.missed_heartbeats = 0
                ensure_sender(replica)
                replica.retry_now.set()
                return {"replica_id": replica.replica_id, "registered": True}
        replica_id = len(replicas) + 1
        replica = Replica(replica_id, normalized_url)
        replicas.append(replica)
        ensure_sender(replica)
        logger.info("Registered replica %s at %s", replica_id, normalized_url)
        return {"replica_id": replica_id, "registered": True}


@app.get("/message")
def read_messages():
    return messages


@app.post("/message")
async def post_message(message: str, w: int = Query(..., ge=1)):
    if w > expected_replicas + 1:
        raise HTTPException(
            status_code=422,
            detail=f"w must be between 1 and {expected_replicas + 1}",
        )

    # Only id assignment and scheduling happen under the lock; waiting for
    # acknowledgements does not, so a blocked client never blocks the others.
    async with lock:
        if not has_quorum():
            raise HTTPException(
                status_code=503,
                detail=(
                    "No quorum: master is in read-only mode "
                    f"({alive_nodes()} of {expected_replicas + 1} nodes alive, "
                    f"{quorum_size()} required)"
                ),
            )

        message_id = len(messages) + 1
        messages.append(Message(message_id, message))
        for replica in replicas:
            ensure_sender(replica)

    secondary_acks_required = w - 1
    loop = asyncio.get_running_loop()
    while True:
        acknowledged = [
            replica.replica_id
            for replica in replicas
            if replica.acked_up_to >= message_id
        ]
        if len(acknowledged) >= secondary_acks_required:
            break
        waiter = loop.create_future()
        ack_waiters.add(waiter)
        try:
            await waiter
        finally:
            ack_waiters.discard(waiter)

    return {
        "message_id": message_id,
        "stored_on_master": True,
        "write_concern": w,
        "write_concern_met": True,
        "acknowledged_count": 1 + len(acknowledged),
        "replication_complete": (
            len(acknowledged) == len(replicas) == expected_replicas
        ),
        "acknowledged_by": sorted(acknowledged),
        "failed": {},
    }
