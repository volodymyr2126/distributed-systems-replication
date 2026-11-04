from fastapi import FastAPI, HTTPException, Query
from pydantic import AnyHttpUrl
from typing import List
import os
import asyncio
import httpx
from common import Message, configure_logger

lock = asyncio.Lock()
# Must exceed the secondaries' SLEEP_AFTER_REQUESTS, or slow replicas look failed.
replication_timeout = float(os.getenv("REPLICATION_TIMEOUT", "60"))
client = httpx.AsyncClient(timeout=replication_timeout)

app = FastAPI()
logger = configure_logger(__name__)

class Replica:
    
    def __init__(self, replica_id: int, url: str):
        self.replica_id = replica_id
        self.url = url
        self.last_replication: asyncio.Task[tuple[int, str | None]] | None = None
    
    def __repr__(self):
        return f"Replica(id={self.replica_id}, url={self.url})"

messages: List[Message] = []
replicas: List[Replica] = []
expected_replicas = int(os.getenv("SECONDARY_REPLICAS_COUNT", 2))


async def replicate_message(
    replica: Replica, message_id: int, message: str
) -> tuple[int, str | None]:
    try:
        response = await client.post(
            f"{replica.url}/message",
            params={"message": message, "message_id": message_id},
        )
        response.raise_for_status()
        acknowledgement = response.json()
        if (
            not isinstance(acknowledgement, dict)
            or acknowledgement.get("message_id") != message_id
        ):
            raise ValueError(
                f"Replica {replica.replica_id} acknowledged the wrong message"
            )
        return replica.replica_id, None
    except (httpx.HTTPError, ValueError) as error:
        logger.exception("Error replicating message to %s", replica.url)
        return replica.replica_id, str(error)


async def replicate_in_order(
    replica: Replica,
    previous_replication: asyncio.Task[tuple[int, str | None]] | None,
    message_id: int,
    message: str,
) -> tuple[int, str | None]:
    if previous_replication is not None:
        await previous_replication
    return await replicate_message(replica, message_id, message)


@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/replica")
async def register_replica(replica_url: AnyHttpUrl):
    normalized_url = str(replica_url).rstrip("/")

    async with lock:
        for replica in replicas:
            if replica.url == normalized_url:
                return {"replica_id": replica.replica_id, "registered": True}
        replica_id = len(replicas) + 1
        replicas.append(Replica(replica_id, normalized_url))
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

    async with lock:
        if len(replicas) != expected_replicas:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Waiting for replicas: registered {len(replicas)} of "
                    f"{expected_replicas}"
                ),
            )

        message_id = len(messages) + 1
        messages.append(Message(message_id, message))

        replication_tasks = []
        for replica in replicas:
            task = asyncio.create_task(
                replicate_in_order(
                    replica,
                    replica.last_replication,
                    message_id,
                    message,
                )
            )
            replica.last_replication = task
            replication_tasks.append(task)

    acknowledged = []
    failures = {}
    pending = set(replication_tasks)
    secondary_acks_required = w - 1

    while len(acknowledged) < secondary_acks_required and pending:
        completed, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        for task in completed:
            replica_id, error = task.result()
            if error is None:
                acknowledged.append(replica_id)
            else:
                failures[replica_id] = error

    if len(acknowledged) < secondary_acks_required:
        raise HTTPException(
            status_code=503,
            detail={
                "message_id": message_id,
                "write_concern": w,
                "acknowledged_count": 1 + len(acknowledged),
                "acknowledged_by": sorted(acknowledged),
                "failed": failures,
            },
        )

    return {
        "message_id": message_id,
        "stored_on_master": True,
        "write_concern": w,
        "write_concern_met": True,
        "acknowledged_count": 1 + len(acknowledged),
        "replication_complete": (
            len(acknowledged) == len(replicas) and not failures
        ),
        "acknowledged_by": sorted(acknowledged),
        "failed": failures,
    }