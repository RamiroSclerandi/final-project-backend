"""Transport seam: keeps paho-mqtt out of the rest of the worker."""

import queue
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class InboundMessage:
    """Raw data envelope; carrying bytes keeps the network callback O(1)."""

    topic: str
    payload: bytes
    received_at: datetime


@dataclass(frozen=True)
class DeviceStatus:
    """Parsed retained `online`/`offline` status of one device."""

    device_mac: str
    online: bool
    received_at: datetime


class MessageSource(Protocol):
    """A running message source.

    `start()`/`stop()` run on the main thread. Messages arrive on the transport's
    own thread, which only enqueues and never does database I/O.
    """

    def start(self) -> None:
        """Connect and block until `stop()`; run on the main thread so it gets OS signals."""
        ...

    def stop(self) -> None:
        """Disconnect from the broker, which causes `start()` to return."""
        ...

    @property
    def inbound_queue(self) -> "queue.Queue[InboundMessage]":
        """Bounded queue of raw data envelopes awaiting the writer thread."""
        ...


def device_mac_from_topic(topic: str) -> str:
    """Extract the MAC segment from a `dl/v1/{MAC}/{data|status}` topic, or "" if malformed."""
    parts = topic.split("/")
    return parts[2] if len(parts) == 4 else ""
