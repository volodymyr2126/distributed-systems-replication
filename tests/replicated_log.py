from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from common import Message

if TYPE_CHECKING:
    from tests.mock_transport import MockedTransport


class ReplicationError(RuntimeError):
    pass


class MasterNode:
    """In-memory master with semi-synchronous replication and retries.

    Every (message, secondary) pair gets its own delivery task that keeps
    retrying until the secondary acknowledges (unlimited attempts, fixed
    ``retry_interval`` between them, ``ack_timeout`` per attempt). Secondaries
    deduplicate and buffer by id, so retries and reordering are harmless.
    """

    def __init__(
        self,
        transport: MockedTransport,
        retry_interval: float = 0.05,
        ack_timeout: float = 5.0,
        expected_secondaries: int | None = None,
    ) -> None:
        self.name = "master"
        self.transport = transport
        self.retry_interval = retry_interval
        self.ack_timeout = ack_timeout
        self.expected_secondaries = expected_secondaries
        self._messages: list[Message] = []
        self._acked: dict[str, set[int]] = {}
        self._known: list[SecondaryNode] = []
        self._waiters: set[asyncio.Future] = set()
        self._background: set[asyncio.Task] = set()
        self.transport.register_master(self)
        for secondary in self.transport.secondaries:
            self.secondary_joined(secondary)

    def secondary_joined(self, secondary: SecondaryNode) -> None:
        """A (new or restarted) secondary appeared: replicate what it misses."""
        if secondary in self._known:
            return
        self._known.append(secondary)
        self._acked[secondary.name] = set()
        for message in self._messages:
            self._start_delivery(secondary, message)

    def _start_delivery(self, secondary: SecondaryNode, message: Message) -> None:
        task = asyncio.create_task(self._deliver(secondary, message))
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _deliver(self, secondary: SecondaryNode, message: Message) -> None:
        while True:
            try:
                acknowledgement = await asyncio.wait_for(
                    self.transport.send(self, secondary, message), self.ack_timeout
                )
                if acknowledgement == message:
                    break
            except Exception:  # timeout, link failure, ...: retry
                pass
            await asyncio.sleep(self.retry_interval)
        self._acked[secondary.name].add(message.message_id)
        for waiter in list(self._waiters):
            if not waiter.done():
                waiter.set_result(None)

    def _ack_count(self, message_id: int) -> int:
        return sum(message_id in acked for acked in self._acked.values())

    async def append_msg(self, message: str, w: int | None = None) -> Message:
        """Append a message and wait for ``w`` ACKs (master counts as one).

        ``w=None`` waits for every expected secondary. If the required
        secondaries are unavailable the call blocks (retrying in the
        background) until they acknowledge; other appends are not affected.
        """
        expected = self.expected_secondaries
        total = (len(self._known) if expected is None else expected) + 1
        w = total if w is None else w
        if not 1 <= w <= total:
            raise ValueError(f"w must be between 1 and {total}")

        # No awaits here, so id assignment + scheduling is atomic.
        new_message = Message(len(self._messages) + 1, message)
        self._messages.append(new_message)
        for secondary in self._known:
            self._start_delivery(secondary, new_message)

        loop = asyncio.get_running_loop()
        while self._ack_count(new_message.message_id) < w - 1:
            waiter = loop.create_future()
            self._waiters.add(waiter)
            try:
                await waiter
            finally:
                self._waiters.discard(waiter)
        return new_message

    async def wait_for_replication(self) -> None:
        """Wait until every known secondary acknowledged every message."""
        while any(
            len(acked) < len(self._messages) for acked in self._acked.values()
        ):
            await asyncio.sleep(0.01)

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
        if self.transport.master is not None:
            self.transport.master.secondary_joined(self)

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
