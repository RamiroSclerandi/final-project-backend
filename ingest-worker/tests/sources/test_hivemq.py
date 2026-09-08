"""RED/GREEN tests for the HiveMQ paho-mqtt v2 source and its bounded queue.

See design decisions D1 (raw envelope handoff), D2/D3 (queue bound and drop
policy), D6 (subscribe inside `on_connect`), D7 (unique client id)
(sdd/worker-ingesta-mqtt/design). paho-mqtt is a third party this project
does not own: these tests never mock it. They construct real
`paho.mqtt.client.MQTTMessage`/`ConnectFlags`/`ReasonCode` objects and call
the source's own callback methods directly, then assert on what ends up in
its queues and counters — observable behavior, not "was a mock called".
"""

import pytest
from paho.mqtt.client import ConnectFlags, MQTTMessage
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from ingest.config import Settings
from ingest.sources.hivemq import HiveMQSource

REQUIRED_ENV = {
    "MQTT_HOST": "test.hivemq.cloud",
    "MQTT_USER": "worker",
    "MQTT_PASSWORD": "dummy-password",
    "SUPABASE_URL": "https://test.supabase.co",
    "SUPABASE_SERVICE_ROLE_KEY": "dummy-service-role-key",
}


class FakeSubscriber:
    """Records `subscribe()` calls; stands in for `paho.mqtt.client.Client`.

    Not a mock: a plain fake implementing the one method `_on_connect` calls
    on its `client` argument. Tests assert on the recorded topics —
    observable state on the fake — never on whether a mock was invoked.
    """

    def __init__(self) -> None:
        self.subscribed_topics: list[tuple[str, int]] = []

    def subscribe(self, topic: str, qos: int = 0) -> tuple[int, int]:
        self.subscribed_topics.append((topic, qos))
        return (0, 1)


def _settings_with_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> Settings:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)
    return Settings()


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    return _settings_with_env(monkeypatch)


def _connack_success() -> ReasonCode:
    return ReasonCode(PacketTypes.CONNACK)


def _data_message(topic: str, payload: bytes) -> MQTTMessage:
    message = MQTTMessage(topic=topic.encode("utf-8"))
    message.payload = payload
    return message


def test_valid_message_lands_in_the_inbound_queue(settings: Settings) -> None:
    source = HiveMQSource(settings)
    message = _data_message("dl/v1/AABBCCDDEEFF/data", b'{"v":1}')

    source._on_message(FakeSubscriber(), None, message)

    queued = source.inbound_queue.get_nowait()
    assert queued.topic == "dl/v1/AABBCCDDEEFF/data"
    assert queued.payload == b'{"v":1}'
    assert source.inbound_queue.empty()


def test_oversized_payload_is_rejected_and_counted(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings_with_env(monkeypatch, MQTT_MAX_PAYLOAD_BYTES="10")
    source = HiveMQSource(settings)
    oversized = _data_message("dl/v1/AABBCCDDEEFF/data", b"x" * 11)

    source._on_message(FakeSubscriber(), None, oversized)

    assert source.inbound_queue.empty()
    assert source.oversized_count == 1


def test_full_queue_drops_and_counts_without_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings_with_env(monkeypatch, INGEST_QUEUE_MAX="1")
    source = HiveMQSource(settings)
    first = _data_message("dl/v1/AABBCCDDEEFF/data", b'{"v":1}')
    second = _data_message("dl/v1/AABBCCDDEEFF/data", b'{"v":2}')

    source._on_message(FakeSubscriber(), None, first)
    source._on_message(FakeSubscriber(), None, second)

    assert source.inbound_queue.qsize() == 1
    assert source.dropped_count == 1


def test_on_connect_issues_both_subscriptions(settings: Settings) -> None:
    source = HiveMQSource(settings)
    fake_client = FakeSubscriber()

    source._on_connect(
        fake_client, None, ConnectFlags(session_present=False), _connack_success(), None
    )

    assert fake_client.subscribed_topics == [
        (settings.mqtt_topic_data, 0),
        (settings.mqtt_topic_status, 0),
    ]


def test_reconnect_reissues_subscriptions(settings: Settings) -> None:
    source = HiveMQSource(settings)
    fake_client = FakeSubscriber()

    source._on_connect(
        fake_client, None, ConnectFlags(session_present=False), _connack_success(), None
    )
    source._on_connect(
        fake_client, None, ConnectFlags(session_present=True), _connack_success(), None
    )

    assert fake_client.subscribed_topics.count((settings.mqtt_topic_data, 0)) == 2
    assert fake_client.subscribed_topics.count((settings.mqtt_topic_status, 0)) == 2


def test_failed_connect_does_not_subscribe(settings: Settings) -> None:
    source = HiveMQSource(settings)
    fake_client = FakeSubscriber()
    failure = ReasonCode(PacketTypes.CONNACK, "Unspecified error")

    source._on_connect(fake_client, None, ConnectFlags(session_present=False), failure, None)

    assert fake_client.subscribed_topics == []


def test_status_topic_yields_online(settings: Settings) -> None:
    source = HiveMQSource(settings)
    message = _data_message("dl/v1/AABBCCDDEEFF/status", b"online")

    source._on_message(FakeSubscriber(), None, message)

    status = source.status_queue.get_nowait()
    assert status.online is True
    assert status.device_mac == "AABBCCDDEEFF"


def test_status_topic_yields_offline(settings: Settings) -> None:
    source = HiveMQSource(settings)
    message = _data_message("dl/v1/AABBCCDDEEFF/status", b"offline")

    source._on_message(FakeSubscriber(), None, message)

    status = source.status_queue.get_nowait()
    assert status.online is False


def test_client_id_is_passed_to_the_paho_client_constructor(settings: Settings) -> None:
    source = HiveMQSource(settings)

    # paho-mqtt exposes no public getter for the configured client id; reading
    # the private attribute is the only way to observe it was actually passed
    # to the constructor rather than merely read from settings elsewhere.
    assert source._client._client_id == settings.mqtt_client_id.encode("utf-8")
