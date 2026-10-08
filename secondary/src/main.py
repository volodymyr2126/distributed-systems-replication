import asyncio
from contextlib import asynccontextmanager
import os
import socket
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI
from common import Message, configure_logger

logger = configure_logger(__name__)

messages = []
master_url = os.getenv("MASTER_URL", "http://localhost:8080").rstrip("/")
secondary_url = os.getenv("SECONDARY_URL", "").rstrip("/")
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


async def register_with_master() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                response = await client.post(
                    f"{master_url}/replica",
                    params={"replica_url": get_secondary_url()},
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
    return {"status": "healthy"}


@app.get("/message")
def read_messages():
    return messages

@app.post("/message")
async def receive_message(message: str):
    message_id = len(messages) + 1
    new_message = Message(message_id, message)
    messages.append(new_message)
    logger.info("Received message %s", message_id)

    if sleep_after_requests:
        await asyncio.sleep(sleep_after_requests)

    return {"message_id": message_id, "message": message}