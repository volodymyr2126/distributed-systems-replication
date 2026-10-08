from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from common import Message

if TYPE_CHECKING:
    from tests.replicated_log import MasterNode, SecondaryNode


class MockedTransport:
    def __init__(self) -> None:
        self._delays: dict[tuple[str, str], float] = {}
        self._failures: set[tuple[str, str]] = set()
        self._started_events: dict[tuple[str, str], asyncio.Event] = {}
        self._acknowledged_events: dict[tuple[str, str], asyncio.Event] = {}
        self.master: MasterNode | None = None
        self.secondaries: list[SecondaryNode] = []

    def register_master(self, master: MasterNode) -> None:
        if self.master is not None and self.master is not master:
            raise ValueError("A MockedTransport supports one master")
        self.master = master

    def register_secondary(self, secondary: SecondaryNode) -> None:
        if secondary not in self.secondaries:
            self.secondaries.append(secondary)

    @staticmethod
    def _connection(master: MasterNode, secondary: SecondaryNode) -> tuple[str, str]:
        return master.name, secondary.name

    def set_delay(
        self, master: MasterNode, secondary: SecondaryNode, seconds: float
    ) -> None:
        if seconds < 0:
            raise ValueError("Delay must be non-negative")
        self._delays[self._connection(master, secondary)] = seconds

    def remove_delay(self, master: MasterNode, secondary: SecondaryNode) -> None:
        self._delays.pop(self._connection(master, secondary), None)

    def set_failure(self, master: MasterNode, secondary: SecondaryNode) -> None:
        self._failures.add(self._connection(master, secondary))

    def remove_failure(self, master: MasterNode, secondary: SecondaryNode) -> None:
        self._failures.discard(self._connection(master, secondary))

    def started_event(
        self, master: MasterNode, secondary: SecondaryNode
    ) -> asyncio.Event:
        connection = self._connection(master, secondary)
        return self._started_events.setdefault(connection, asyncio.Event())

    def acknowledged_event(
        self, master: MasterNode, secondary: SecondaryNode
    ) -> asyncio.Event:
        connection = self._connection(master, secondary)
        return self._acknowledged_events.setdefault(connection, asyncio.Event())

    async def send(
        self, master: MasterNode, secondary: SecondaryNode, message: Message
    ) -> Message:
        connection = self._connection(master, secondary)
        self.started_event(master, secondary).set()
        await asyncio.sleep(self._delays.get(connection, 0))

        if connection in self._failures:
            raise ConnectionError(
                f"Simulated link failure from {master.name} to {secondary.name}"
            )

        acknowledgement = secondary.receive(message)
        self.acknowledged_event(master, secondary).set()
        return acknowledgement
