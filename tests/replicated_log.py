from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from common import Message

if TYPE_CHECKING:
    from tests.mock_transport import MockedTransport


class ReplicationError(RuntimeError):
    pass


class MasterNode:
    def __init__(self, transport: MockedTransport) -> None:
        self.name = "master"
        self.transport = transport
        self._messages: list[Message] = []
        self._append_lock = asyncio.Lock()
        self._background: set[asyncio.Task] = set()
        self.transport.register_master(self)

    async def append_msg(self, message: str, w: int | None = None) -> Message:
        """Append a message and wait for ``w`` ACKs (master counts as one).

        ``w=None`` waits for every secondary. Replication to the remaining
        secondaries continues in the background after returning.
        """
        total = len(self.transport.secondaries) + 1
        w = total if w is None else w
        if not 1 <= w <= total:
            raise ValueError(f"w must be between 1 and {total}")

        # Only id assignment + scheduling is serialized, never the waiting,
        # so a slow w=3 write does not block a later w=1 write.
        async with self._append_lock:
            new_message = Message(len(self._messages) + 1, message)
            self._messages.append(new_message)
            tasks = [
                asyncio.create_task(
                    self.transport.send(self, secondary, new_message)
                )
                for secondary in self.transport.secondaries
            ]
            self._background.update(tasks)
            for task in tasks:
                task.add_done_callback(self._background.discard)
                # Mark failures as retrieved; they are reported via w handling.
                task.add_done_callback(lambda t: t.cancelled() or t.exception())

        needed = w - 1
        acked = 0
        errors: list[BaseException] = []
        pending = set(tasks)
        while acked < needed and pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task.exception() is not None:
                    errors.append(task.exception())
                elif task.result() == new_message:
                    acked += 1
                else:
                    errors.append(ReplicationError("invalid ACK"))
        if acked < needed:
            raise ReplicationError(
                f"Message {new_message.message_id} was not acknowledged by "
                f"all required secondaries ({acked} of {needed} ACKs): "
                f"{'; '.join(map(str, errors))}"
            )
        return new_message

    async def wait_for_replication(self) -> None:
        """Wait until all in-flight background replication has finished."""
        while self._background:
            await asyncio.gather(*list(self._background), return_exceptions=True)

    def list_msgs(self) -> list[str]:
        return [message.message for message in self._messages]


class SecondaryNode:
    def __init__(
        self, transport: MockedTransport, name: str | None = None
    ) -> None:
        self.transport = transport
        self.name = name or f"secondary-{id(self)}"
        self._messages: list[Message] = []
        self._buffer: dict[int, Message] = {}
        self.transport.register_secondary(self)

    def receive(self, message: Message) -> Message:
        """Accept a message; expose it only once all earlier ids arrived.

        Out-of-order and duplicate deliveries are tolerated, which keeps the
        visible log in the master's total order.
        """
        if message.message_id > len(self._messages):
            self._buffer[message.message_id] = message
            while len(self._messages) + 1 in self._buffer:
                self._messages.append(self._buffer.pop(len(self._messages) + 1))
        return message

    def list_msgs(self) -> list[str]:
        return [message.message for message in self._messages]
