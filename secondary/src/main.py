import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import socket
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query
from common import Message, configure_logger

logger = configure_logger(__name__)

messages = []
# Messages received ahead of a gap; shown only once every earlier id arrived.
buffer: dict[int, Message] = {}
message_lock = asyncio.Lock()
master_url = os.getenv("MASTER_URL", "http://localhost:8080").rstrip("/")
secondary_url = os.getenv("SECONDARY_URL", "").rstrip("/")
# Identity reported to the master: NODE_ID (unique per secondary), or an id
# generated once and kept in NODE_ID_FILE.
configured_node_id = os.getenv("NODE_ID", "")
node_id_file = os.getenv("NODE_ID_FILE", "")
sleep_after_requests = float(os.getenv("SLEEP_AFTER_REQUESTS", "0"))
if sleep_after_requests < 0:
    raise ValueError("SLEEP_AFTER_REQUESTS must be non-negative")


def get_secondary_url() -> str:
    if secondary_url:
        return secondary_url

    master = urlsplit(master_url)
    if not master.hostname:
        raise ValueError("MASTER_URL must contain a hostname")

    port = master.port or (443 if master.scheme == "https" else 80)
    address = socket.getaddrinfo(
        master.hostname, port, family=socket.AF_INET, type=socket.SOCK_DGRAM
    )[0][4]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
        connection.connect(address)
        secondary_ip = connection.getsockname()[0]

    return f"http://{secondary_ip}:80"


def get_node_id() -> str | None:
    """Returns None when no identity is configured (the master then uses the URL)."""
    if configured_node_id:
        return configured_node_id
    if not node_id_file:
        return None

    path = Path(node_id_file)
    if path.exists():
        stored = path.read_text().strip()
        if stored:
            return stored
    generated = uuid4().hex
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(generated)
    return generated


async def register_with_master() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                params = {"replica_url": get_secondary_url()}
                node_id = get_node_id()
                if node_id:
                    params["node_id"] = node_id
                response = await client.post(
                    f"{master_url}/replica", params=params
                )
                response.raise_for_status()
                registration = response.json()
                if (
                    not isinstance(registration, dict)
                    or registration.get("registered") is not True
                ):
                    raise ValueError("Master returned an invalid registration response")
                logger.info(
                    "Registered with master as replica %s",
                    registration.get("replica_id"),
                )
                return
            except (httpx.HTTPError, OSError, ValueError) as error:
                logger.warning("Could not register with master: %s; retrying", error)
                await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await register_with_master()
    yield


app = FastAPI(lifespan=lifespan)

@app.get("/health")
def read_root():
    return {"status": "healthy", "last_id": len(messages)}


@app.get("/message")
def read_messages():
    return messages


@app.post("/message")
async def receive_message(message: str, message_id: int = Query(..., ge=1)):
    async with message_lock:
        is_new_message = False
        existing = (
            messages[message_id - 1]
            if message_id <= len(messages)
            else buffer.get(message_id)
        )
        if existing is not None:
            # Deduplication: a retried delivery is acknowledged but not stored twice.
            if existing.message != message:
                raise HTTPException(
                    status_code=409,
                    detail=f"Message ID {message_id} already has different content",
                )
        else:
            buffer[message_id] = Message(message_id, message)
            is_new_message = True
            # Total order: expose a message only when all earlier ones are present.
            while len(messages) + 1 in buffer:
                messages.append(buffer.pop(len(messages) + 1))
            logger.info("Received message %s", message_id)

        if sleep_after_requests and is_new_message:
            await asyncio.sleep(sleep_after_requests)

        last_id = len(messages)

    return {"message_id": message_id, "message": message, "last_id": last_id}
