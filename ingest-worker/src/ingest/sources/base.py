"""Transport seam between the MQTT broker and the ingestion pipeline.

`MessageSource` is the protocol every message source implements (HiveMQ
today; a future LoRaWAN/TTN transport is explicitly out of scope). The seam
exists so the rest of the worker never imports paho-mqtt directly, and so
the queueing/dropping/counting behavior is testable without a live broker
(see design decisions D1-D3, sdd/worker-ingesta-mqtt/design).
"""

import queue
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class InboundMessage:
    """One raw MQTT data envelope handed from the transport to the writer.

    Carries the raw topic and payload bytes, not a parsed reading (design
    decision D1). This keeps the network thread's callback O(1) and makes
    the queue bound expressible in bytes:
    `INGEST_QUEUE_MAX * MQTT_MAX_PAYLOAD_BYTES`.
    """

    topic: str
    payload: bytes
    received_at: datetime


@dataclass(frozen=True)
class DeviceStatus:
    """One parsed retained status envelope from a device's status topic.

    Unlike `InboundMessage` this is already parsed into `online`/`offline` —
    the status contract is two values, not the full `datalogger.v1` schema —
    but it is still not written to the database in this slice; no sink
    exists yet.
    """

    device_mac: str
    online: bool
    received_at: datetime


class MessageSource(Protocol):
    """A running message source: connects, subscribes, and queues raw envelopes.

    Threading contract: `start()` and `stop()` are called from the main
    thread. Once started, the transport delivers messages on its own network
    thread; that thread only validates payload size and puts an
    `InboundMessage` onto the bounded `inbound_queue` — it never performs
    database I/O or blocks on anything slow. A separate consumer (writer)
    thread drains `inbound_queue`. `queue.Queue` is thread-safe on its own
    and needs no external lock for the handoff itself.
    """

    def start(self) -> None:
        """Connect to the broker and begin receiving messages.

        Blocks the calling thread for the lifetime of the connection; the
        composition root is expected to run this on the main thread so
        Python delivers OS signals to it (design decision D5).
        """
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
