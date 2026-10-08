from fastapi import FastAPI, HTTPException
from pydantic import AnyHttpUrl
from typing import List
import os
import asyncio
import httpx
from common import Message, configure_logger

lock = asyncio.Lock()
client = httpx.AsyncClient(timeout=5.0)

app = FastAPI()
logger = configure_logger(__name__)

class Replica:
    
    def __init__(self, replica_id: int, url: str):
        self.replica_id = replica_id
        self.url = url
    
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
            f"{replica.url}/message", params={"message": message}
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
async def post_message(message: str):
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
        new_message = Message(message_id, message)
        messages.append(new_message)
        results = await asyncio.gather(
            *(replicate_message(replica, message_id, message) for replica in replicas)
        )
        acknowledged = [replica_id for replica_id, error in results if error is None]
        failures = {
            replica_id: error
            for replica_id, error in results
            if error is not None
        }

        return {
            "message_id": message_id,
            "stored_on_master": True,
            "replication_complete": not failures,
            "acknowledged_by": acknowledged,
            "failed": failures,
        }