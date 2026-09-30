"""HiveMQ Cloud source: paho-mqtt v2 client, TLS, bounded queue.

See docs/SDD_Worker_Ingesta.md and design decisions D1-D3, D6-D8
(sdd/worker-ingesta-mqtt/design). `on_connect(client, userdata, flags,
reason_code, properties)` and `on_disconnect`/`on_subscribe` use the paho v2
callback signatures (`CallbackAPIVersion.VERSION2`); `on_message(client,
userdata, message)` is unchanged from v1 (research claim C1,
sdd/worker-ingesta-mqtt/research).

`on_message` runs on paho's own network thread and must never block on slow
work: it only measures payload size and puts a value onto one of the two
bounded queues below. All other I/O (archiving, parsing, persistence)
belongs to a separate writer thread (Phase 8, out of scope here) that drains
`inbound_queue`/`status_queue`.
"""

import logging
import queue
import threading
from datetime import UTC, datetime
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from ingest.config import Settings
from ingest.sources.base import DeviceStatus, InboundMessage, device_mac_from_topic

logger = logging.getLogger(__name__)

# Exponential backoff bounds for paho's built-in reconnect (research claim
# C2). Matches the values already fixed in the design's Error Taxonomy table:
# start at 1s, cap at 60s, reset to 1s on the next successful CONNACK.
_RECONNECT_MIN_DELAY_S = 1
_RECONNECT_MAX_DELAY_S = 60


def _parse_online_offline(payload: bytes) -> bool:
    """Parse a retained status payload into an online flag.

    Raises:
        ValueError: If the payload is not exactly `online` or `offline`.
    """
    text = payload.decode("utf-8").strip().lower()
    if text == "online":
        return True
    if text == "offline":
        return False
    raise ValueError(f"unrecognized status payload: {text!r}")


class HiveMQSource:
    """paho-mqtt v2 based `MessageSource` implementation for HiveMQ Cloud.

    Implements the `MessageSource` protocol (`sources/base.py`) structurally:
    `start()`, `stop()`, `inbound_queue`. Also exposes `status_queue` for the
    retained device online/offline topic, which this slice parses but does
    not persist — no sink exists yet.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._queue: queue.Queue[InboundMessage] = queue.Queue(maxsize=settings.ingest_queue_max)
        self._status_queue: queue.Queue[DeviceStatus] = queue.Queue(
            maxsize=settings.ingest_queue_max
        )
        self._counts_lock = threading.Lock()
        self._dropped_count = 0
        self._oversized_count = 0

        self._client: mqtt.Client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
        )
        self._client.username_pw_set(settings.mqtt_user, settings.mqtt_password.get_secret_value())
        # Empty MQTT_CA_CERT_PATH means the default system/certifi CA bundle;
        # a configured path pins a custom CA. tls_insecure_set is never
        # called (design decision D8; spike S2 is still open).
        self._client.tls_set(ca_certs=settings.mqtt_ca_cert_path or None)
        self._client.reconnect_delay_set(
            min_delay=_RECONNECT_MIN_DELAY_S, max_delay=_RECONNECT_MAX_DELAY_S
        )
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self._client.on_subscribe = self._on_subscribe

    def start(self) -> None:
        """Connect to the broker and block, delivering messages until `stop()`."""
        self._client.connect(self._settings.mqtt_host, self._settings.mqtt_port, keepalive=60)
        self._client.loop_forever()

    def stop(self) -> None:
        """Disconnect, which makes the blocked `loop_forever()` in `start()` return."""
        self._client.disconnect()

    @property
    def inbound_queue(self) -> "queue.Queue[InboundMessage]":
        """Bounded queue of raw data envelopes awaiting the writer thread."""
        return self._queue

    @property
    def status_queue(self) -> "queue.Queue[DeviceStatus]":
        """Bounded queue of parsed device status changes awaiting the writer thread."""
        return self._status_queue

    @property
    def dropped_count(self) -> int:
        """Messages dropped because a queue was full (design decision D3)."""
        with self._counts_lock:
            return self._dropped_count

    @property
    def oversized_count(self) -> int:
        """Messages rejected for exceeding `MQTT_MAX_PAYLOAD_BYTES`."""
        with self._counts_lock:
            return self._oversized_count

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ) -> None:
        if reason_code.is_failure:
            logger.error("MQTT connect failed: reason=%s", reason_code)
            return
        # Subscriptions are issued here every time on_connect fires —
        # including after an automatic reconnect — never once at startup.
        # Correct whether or not the broker restores subscriptions, and
        # eliminates the connected-but-deaf failure mode (design decision D6).
        client.subscribe(self._settings.mqtt_topic_data, qos=0)
        client.subscribe(self._settings.mqtt_topic_status, qos=0)
        logger.info(
            "MQTT connected and subscribed: data=%s status=%s",
            self._settings.mqtt_topic_data,
            self._settings.mqtt_topic_status,
        )

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: ReasonCode | None,
        properties: Properties | None,
    ) -> None:
        logger.warning("MQTT disconnected: reason=%s", reason_code)

    def _on_subscribe(
        self,
        client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[ReasonCode],
        properties: Properties | None,
    ) -> None:
        logger.debug("MQTT subscribe acknowledged: mid=%d reason_codes=%s", mid, reason_code_list)

    def _on_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        payload_size = len(message.payload)
        if payload_size > self._settings.mqtt_max_payload_bytes:
            self._increment_oversized()
            logger.warning(
                "rejected oversized MQTT payload: topic=%s size=%d max=%d",
                message.topic,
                payload_size,
                self._settings.mqtt_max_payload_bytes,
            )
            return

        received_at = datetime.now(UTC)

        if mqtt.topic_matches_sub(self._settings.mqtt_topic_status, message.topic):
            self._enqueue_status(message.topic, message.payload, received_at)
            return

        self._enqueue_inbound(message.topic, message.payload, received_at)

    def _enqueue_inbound(self, topic: str, payload: bytes, received_at: datetime) -> None:
        inbound = InboundMessage(topic=topic, payload=payload, received_at=received_at)
        try:
            self._queue.put_nowait(inbound)
        except queue.Full:
            self._increment_dropped()
            logger.warning(
                "dropped MQTT message: inbound queue full, topic=%s size=%d",
                topic,
                len(payload),
            )

    def _enqueue_status(self, topic: str, payload: bytes, received_at: datetime) -> None:
        try:
            online = _parse_online_offline(payload)
        except ValueError:
            logger.warning(
                "dropped unparseable status payload: topic=%s size=%d", topic, len(payload)
            )
            return

        status = DeviceStatus(
            device_mac=device_mac_from_topic(topic), online=online, received_at=received_at
        )
        try:
            self._status_queue.put_nowait(status)
        except queue.Full:
            self._increment_dropped()
            logger.warning("dropped status update: status queue full, topic=%s", topic)

    def _increment_dropped(self) -> None:
        with self._counts_lock:
            self._dropped_count += 1

    def _increment_oversized(self) -> None:
        with self._counts_lock:
            self._oversized_count += 1
