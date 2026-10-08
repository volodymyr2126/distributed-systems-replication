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
        self.transport.register_master(self)

    async def append_msg(self, message: str) -> Message:
        async with self._append_lock:
            new_message = Message(len(self._messages) + 1, message)
            self._messages.append(new_message)

            results = await asyncio.gather(
                *(
                    self.transport.send(self, secondary, new_message)
                    for secondary in self.transport.secondaries
                ),
                return_exceptions=True,
            )
            errors = [
                result
                for result in results
                if isinstance(result, BaseException)
            ]
            if errors:
                raise ReplicationError(
                    f"Message {new_message.message_id} was not acknowledged by "
                    f"all secondaries: {'; '.join(map(str, errors))}"
                )
            if any(result != new_message for result in results):
                raise ReplicationError(
                    f"Message {new_message.message_id} received an invalid ACK"
                )
            return new_message

    def list_msgs(self) -> list[str]:
        return [message.message for message in self._messages]


class SecondaryNode:
    def __init__(
        self, transport: MockedTransport, name: str | None = None
    ) -> None:
        self.transport = transport
        self.name = name or f"secondary-{id(self)}"
        self._messages: list[Message] = []
        self.transport.register_secondary(self)

    def receive(self, message: Message) -> Message:
        self._messages.append(message)
        return message

    def list_msgs(self) -> list[str]:
        return [message.message for message in self._messages]
