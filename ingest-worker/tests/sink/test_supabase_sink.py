"""RED/GREEN tests for the idempotent measurement sink (writer thread).

See docs/SDD_Worker_Ingesta.md sections 5.4-5.6 and design decisions D4, D9,
D11 (sdd/worker-ingesta-mqtt/design), and the measured spike S1 result
(sdd/worker-ingesta-mqtt/spike-s1-result): a batch upsert with
on_conflict="sensor_id,timestamp" and ignore_duplicates=True is PARTIAL, not
atomic, and `response.data` holds only the rows actually written. supabase-py
is a third party this project does not own: `FakeSinkStore` and
`FakeRegistryStore` are hand-written in-memory implementations of the
`SinkStore`/`RegistryStore` ports, never a mock of supabase-py itself.
`FakeRegistryStore` mirrors `tests/test_registry.py`'s fake of the same
protocol, trimmed to what these tests need — the insert-race recovery path
is already covered there.
"""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from ingest.main import build_supabase_client
from ingest.registry import DeviceRecord, Registry, SensorRecord, SensorTypeRecord
from ingest.sink.supabase_sink import (
    MeasurementSink,
    SinkPermanentError,
    SinkTransientError,
    SupabaseStore,
    build_raw_message_row,
    record_from_row,
)
from ingest.sources.base import DeviceStatus, InboundMessage

RECEIVED_AT = datetime(2026, 9, 9, 12, 0, 0, tzinfo=UTC)


def _data_envelope(*, ts: int = 1788804294, temperature: float = 21.5) -> bytes:
    payload = {
        "v": 1,
        "dev": "AABBCCDDEEFF",
        "ts": ts,
        "seq": 1,
        "meta": {
            "rssi": -60,
            "fw": "1.2.3",
            "boot": 1,
            "ts_src": "device",
            "store": {"k": "ram", "pct": 0, "pend": 0, "drop": 0},
        },
        "ch": [{"c": "temperature", "u": "C", "src": "bmp280", "ok": True, "val": temperature}],
    }
    return json.dumps(payload).encode("utf-8")


def _inbound(payload: bytes, *, received_at: datetime = RECEIVED_AT) -> InboundMessage:
    return InboundMessage(topic="dl/v1/AABBCCDDEEFF/data", payload=payload, received_at=received_at)


class FakeClock:
    """Manually-advanced monotonic clock; `sleep()` advances it too.

    Makes batch-age thresholds and retry backoff deterministically testable
    without a real wait, and records every `sleep()` call so a test can
    assert on the exact backoff sequence used.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeRegistryStore:
    """In-memory `RegistryStore`; mirrors `tests/test_registry.py`'s fake."""

    def __init__(self) -> None:
        self.devices: dict[str, DeviceRecord] = {}
        self.sensor_types: dict[tuple[str, str], SensorTypeRecord] = {}
        self.sensors: dict[tuple[str, str, str, str], SensorRecord] = {}

    def select_device_by_mac(self, mac_address: str) -> DeviceRecord | None:
        return self.devices.get(mac_address)

    def insert_device(self, mac_address: str, name: str) -> DeviceRecord:
        record = DeviceRecord(id=str(uuid4()), mac_address=mac_address, name=name)
        self.devices[mac_address] = record
        return record

    def select_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None:
        return self.sensor_types.get((name, unit))

    def insert_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None:
        if (name, unit) in self.sensor_types:
            return None
        record = SensorTypeRecord(id=str(uuid4()), name=name, unit=unit)
        self.sensor_types[(name, unit)] = record
        return record

    def select_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None:
        return self.sensors.get((device_id, type_id, source, tag))

    def insert_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None:
        key = (device_id, type_id, source, tag)
        if key in self.sensors:
            return None
        record = SensorRecord(
            id=str(uuid4()), device_id=device_id, type_id=type_id, source=source, tag=tag
        )
        self.sensors[key] = record
        return record


class FakeSinkStore:
    """In-memory `SinkStore`.

    `upsert_measurements` simulates the real PostgREST semantics measured in
    spike S1 (sdd/worker-ingesta-mqtt/spike-s1-result): a row already
    present for its `(sensor_id, timestamp)` conflict target is silently
    skipped, its batch siblings are written, and only the actually-written
    row count is returned — never raises on a duplicate. `errors_to_raise`
    lets a test queue up failures for `upsert_measurements` to simulate
    transient/permanent write failures without a real network call.
    """

    def __init__(self, known_devices: dict[str, DeviceRecord] | None = None) -> None:
        # Mirrors a real UPDATE: a row that does not exist is not created and
        # the write is silently lost. Without this, a status update for an
        # unregistered device looks successful in tests and vanishes in
        # production, which is exactly what happened against the real schema.
        self.known_devices = known_devices if known_devices is not None else {}
        self.raw_messages: list[dict[str, Any]] = []
        self.processed_raw_message_ids: list[int] = []
        self.measurements: dict[tuple[str, str], dict[str, Any]] = {}
        self.upsert_calls: list[list[dict[str, Any]]] = []
        self.errors_to_raise: list[Exception] = []
        self.archive_errors: list[Exception] = []
        self.last_seen_errors: list[Exception] = []
        self.device_status: dict[str, bool] = {}
        self.device_last_seen: list[tuple[str, datetime, str | None]] = []

    def archive_raw_message(
        self, topic: str, payload: bytes, received_at: datetime, error: str | None
    ) -> int:
        if self.archive_errors:
            raise self.archive_errors.pop(0)
        self.raw_messages.append(
            {"topic": topic, "payload": payload, "received_at": received_at, "error": error}
        )
        return len(self.raw_messages)

    def mark_raw_messages_processed(self, raw_message_ids: list[int]) -> None:
        self.processed_raw_message_ids.extend(raw_message_ids)

    def upsert_measurements(self, rows: list[dict[str, Any]]) -> int:
        self.upsert_calls.append(rows)
        if self.errors_to_raise:
            raise self.errors_to_raise.pop(0)
        written = 0
        for row in rows:
            key = (row["sensor_id"], row["timestamp"])
            if key in self.measurements:
                continue
            self.measurements[key] = row
            written += 1
        return written

    def update_device_status(self, device_mac: str, online: bool, at: datetime) -> None:
        if device_mac not in self.known_devices:
            return
        self.device_status[device_mac] = online

    def update_device_last_seen(
        self, device_mac: str, at: datetime, firmware_version: str | None
    ) -> None:
        if self.last_seen_errors:
            raise self.last_seen_errors.pop(0)
        self.device_last_seen.append((device_mac, at, firmware_version))


def _make_sink(
    *,
    batch_max_size: int = 100,
    batch_max_age_ms: int = 2000,
    sink_store: FakeSinkStore | None = None,
    registry_store: FakeRegistryStore | None = None,
    clock: FakeClock | None = None,
) -> tuple[MeasurementSink, FakeSinkStore]:
    clock = clock or FakeClock()
    registry_store = registry_store or FakeRegistryStore()
    sink_store = sink_store if sink_store is not None else FakeSinkStore(registry_store.devices)
    registry = Registry(registry_store, ttl_seconds=900, clock=clock)
    sink = MeasurementSink(
        store=sink_store,
        registry=registry,
        batch_max_size=batch_max_size,
        batch_max_age_ms=batch_max_age_ms,
        clock=clock,
        sleep=clock.sleep,
    )
    return sink, sink_store


def test_batch_flushes_when_it_reaches_batch_max_size() -> None:
    sink, store = _make_sink(batch_max_size=2, batch_max_age_ms=60_000)

    sink.handle_message(_inbound(_data_envelope(ts=1788804294)))
    assert store.upsert_calls == []
    sink.handle_message(_inbound(_data_envelope(ts=1788804300)))

    assert len(store.measurements) == 2
    assert sink.pending_count == 0


def test_batch_flushes_when_batch_max_age_elapses() -> None:
    clock = FakeClock()
    sink, store = _make_sink(batch_max_size=100, batch_max_age_ms=2000, clock=clock)

    sink.handle_message(_inbound(_data_envelope()))
    assert store.upsert_calls == []

    clock.advance(2.001)
    sink.flush_if_due()

    assert len(store.measurements) == 1
    assert sink.pending_count == 0


def test_replaying_an_identical_batch_changes_no_row_count_and_counts_duplicates_skipped() -> None:
    sink, store = _make_sink(batch_max_size=1)
    message = _inbound(_data_envelope(ts=1788804294))

    sink.handle_message(message)
    sink.handle_message(message)

    assert len(store.measurements) == 1
    assert sink.duplicates_skipped_count == 1
    assert sink.batches_written_count == 2


def test_reading_outside_the_expected_range_is_marked_out_of_range_and_still_written() -> None:
    registry_store = FakeRegistryStore()
    registry_store.sensor_types[("temperature", "C")] = SensorTypeRecord(
        id="type-1", name="temperature", unit="C", expected_min=-40.0, expected_max=85.0
    )
    sink, store = _make_sink(batch_max_size=1, registry_store=registry_store)

    sink.handle_message(_inbound(_data_envelope(temperature=999.0)))

    row = next(iter(store.measurements.values()))
    assert row["quality"] == "out_of_range"


def test_reading_inside_the_expected_range_is_marked_ok() -> None:
    registry_store = FakeRegistryStore()
    registry_store.sensor_types[("temperature", "C")] = SensorTypeRecord(
        id="type-1", name="temperature", unit="C", expected_min=-40.0, expected_max=85.0
    )
    sink, store = _make_sink(batch_max_size=1, registry_store=registry_store)

    sink.handle_message(_inbound(_data_envelope(temperature=21.5)))

    row = next(iter(store.measurements.values()))
    assert row["quality"] == "ok"


def test_malformed_payload_is_archived_with_its_error_and_does_not_stall_the_queue() -> None:
    sink, store = _make_sink(batch_max_size=1)

    sink.handle_message(_inbound(b"not json"))
    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.raw_messages) == 2
    assert store.raw_messages[0]["error"] is not None
    assert store.raw_messages[1]["error"] is None
    assert len(store.measurements) == 1


def test_last_seen_and_firmware_version_are_throttled_to_at_most_one_update_per_interval() -> None:
    clock = FakeClock()
    sink, store = _make_sink(batch_max_size=100, batch_max_age_ms=60_000, clock=clock)

    sink.handle_message(_inbound(_data_envelope(ts=1788804294)))
    clock.advance(1.0)
    sink.handle_message(_inbound(_data_envelope(ts=1788804300)))
    clock.advance(1.0)
    sink.handle_message(_inbound(_data_envelope(ts=1788804400)))

    assert len(store.device_last_seen) == 1

    clock.advance(61.0)
    sink.handle_message(_inbound(_data_envelope(ts=1788804500)))

    assert len(store.device_last_seen) == 2
    assert store.device_last_seen[-1][2] == "1.2.3"


def test_device_status_from_the_status_topic_is_never_throttled() -> None:
    sink, store = _make_sink()

    sink.handle_status(
        DeviceStatus(device_mac="AABBCCDDEEFF", online=True, received_at=RECEIVED_AT)
    )
    sink.handle_status(
        DeviceStatus(device_mac="AABBCCDDEEFF", online=False, received_at=RECEIVED_AT)
    )

    assert store.device_status["AABBCCDDEEFF"] is False


def test_a_transient_batch_failure_is_retried_and_succeeds() -> None:
    clock = FakeClock()
    store = FakeSinkStore()
    store.errors_to_raise = [SinkTransientError("timeout"), SinkTransientError("timeout")]
    sink, _ = _make_sink(batch_max_size=1, sink_store=store, clock=clock)

    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.measurements) == 1
    assert len(store.upsert_calls) == 3
    assert sink.batch_retries_count == 2
    assert sink.batch_failed_count == 0
    assert clock.slept == [0.5, 1.0]


def test_a_permanent_batch_failure_is_not_retried_and_is_counted_as_failed() -> None:
    store = FakeSinkStore()
    store.errors_to_raise = [SinkPermanentError("bad request")]
    sink, _ = _make_sink(batch_max_size=1, sink_store=store)

    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.measurements) == 0
    assert len(store.upsert_calls) == 1
    assert sink.batch_failed_count == 1


def test_a_transient_failure_that_persists_through_every_retry_is_counted_as_failed() -> None:
    clock = FakeClock()
    store = FakeSinkStore()
    store.errors_to_raise = [SinkTransientError("timeout")] * 4
    sink, _ = _make_sink(batch_max_size=1, sink_store=store, clock=clock)

    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.measurements) == 0
    assert len(store.upsert_calls) == 4
    assert sink.batch_retries_count == 3
    assert sink.batch_failed_count == 1


class FlakyRegistryStore(FakeRegistryStore):
    """`FakeRegistryStore` whose device lookup fails with queued errors first."""

    def __init__(self, errors: list[Exception]) -> None:
        super().__init__()
        self.errors = errors

    def select_device_by_mac(self, mac_address: str) -> DeviceRecord | None:
        if self.errors:
            raise self.errors.pop(0)
        return super().select_device_by_mac(mac_address)


def test_a_transient_archive_failure_is_retried_and_the_message_is_kept() -> None:
    clock = FakeClock()
    store = FakeSinkStore()
    store.archive_errors = [SinkTransientError("timeout"), SinkTransientError("timeout")]
    sink, _ = _make_sink(batch_max_size=1, sink_store=store, clock=clock)

    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.raw_messages) == 1
    assert len(store.measurements) == 1
    assert clock.slept == [0.5, 1.0]


def test_a_transient_archive_failure_that_persists_through_every_retry_is_raised() -> None:
    clock = FakeClock()
    store = FakeSinkStore()
    store.archive_errors = [SinkTransientError("timeout")] * 4
    sink, _ = _make_sink(batch_max_size=1, sink_store=store, clock=clock)

    with pytest.raises(SinkTransientError):
        sink.handle_message(_inbound(_data_envelope()))

    assert clock.slept == [0.5, 1.0, 2.0]
    assert len(store.raw_messages) == 0


def test_a_permanent_archive_failure_is_not_retried() -> None:
    clock = FakeClock()
    store = FakeSinkStore()
    store.archive_errors = [SinkPermanentError("bad request")]
    sink, _ = _make_sink(batch_max_size=1, sink_store=store, clock=clock)

    with pytest.raises(SinkPermanentError):
        sink.handle_message(_inbound(_data_envelope()))

    assert clock.slept == []


def test_a_transient_registry_failure_is_retried_and_the_readings_are_written() -> None:
    clock = FakeClock()
    registry_store = FlakyRegistryStore([SinkTransientError("timeout")])
    sink, store = _make_sink(batch_max_size=1, registry_store=registry_store, clock=clock)

    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.measurements) == 1
    assert clock.slept == [0.5]


def test_a_transient_registry_failure_on_a_status_update_is_retried() -> None:
    clock = FakeClock()
    registry_store = FlakyRegistryStore([SinkTransientError("timeout")])
    sink, store = _make_sink(registry_store=registry_store, clock=clock)

    sink.handle_status(
        DeviceStatus(device_mac="AABBCCDDEEFF", online=True, received_at=RECEIVED_AT)
    )

    assert store.device_status == {"AABBCCDDEEFF": True}
    assert clock.slept == [0.5]


def test_a_transient_last_seen_failure_is_retried() -> None:
    clock = FakeClock()
    store = FakeSinkStore()
    store.last_seen_errors = [SinkTransientError("timeout")]
    sink, _ = _make_sink(batch_max_size=1, sink_store=store, clock=clock)

    sink.handle_message(_inbound(_data_envelope()))

    assert len(store.device_last_seen) == 1
    assert clock.slept == [0.5]


@pytest.mark.parametrize(
    "call",
    [
        lambda store: store.archive_raw_message(
            "dl/v1/AABBCCDDEEFF/data", b"{}", RECEIVED_AT, None
        ),
        lambda store: store.select_device_by_mac("AABBCCDDEEFF"),
        lambda store: store.update_device_last_seen("AABBCCDDEEFF", RECEIVED_AT, "1.2.0"),
    ],
    ids=["archive", "registry", "last_seen"],
)
def test_an_unreachable_supabase_is_reported_as_transient(call: Any) -> None:
    # A real client against a closed local port: a genuine connection error, no mock.
    store = SupabaseStore(build_supabase_client("http://127.0.0.1:9", "header.payload.signature"))

    with pytest.raises(SinkTransientError):
        call(store)


# --- Row shape against the real schema ---
#
# `SupabaseStore` is deliberately not exercised against a live project, so a
# column the schema requires and the code never sets is invisible to every
# test above: the in-memory fakes accept any dictionary. `raw_messages.source`
# was exactly that, and it only surfaced when the worker ran against the real
# database. These tests cover the row-building itself.

RAW_MESSAGE_REQUIRED_COLUMNS = {"topic", "payload", "source"}
RAW_MESSAGE_SOURCES = {"hivemq", "ttn", "chirpstack", "http"}


def test_archived_row_carries_every_column_the_schema_requires() -> None:
    row = build_raw_message_row(
        topic="dl/v1/4022D83D6618/data",
        payload=b'{"v":1}',
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        error=None,
        source="hivemq",
    )

    missing = RAW_MESSAGE_REQUIRED_COLUMNS - row.keys()

    assert not missing, f"raw_messages NOT NULL columns never set: {sorted(missing)}"
    assert all(row[column] is not None for column in RAW_MESSAGE_REQUIRED_COLUMNS)


def test_archived_row_source_is_one_the_check_constraint_admits() -> None:
    row = build_raw_message_row(
        topic="dl/v1/4022D83D6618/data",
        payload=b'{"v":1}',
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        error=None,
        source="hivemq",
    )

    assert row["source"] in RAW_MESSAGE_SOURCES


def test_a_malformed_payload_is_still_archived_with_its_error_and_not_processed() -> None:
    row = build_raw_message_row(
        topic="dl/v1/4022D83D6618/data",
        payload=b"not json",
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        error="boom",
        source="hivemq",
    )

    assert row["error"] == "boom"
    assert row["processed"] is False
    assert row["payload"] == "not json"


def test_a_json_payload_is_archived_as_an_object_and_not_yet_processed() -> None:
    row = build_raw_message_row(
        topic="dl/v1/4022D83D6618/data",
        payload=b'{"v":1}',
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        error=None,
        source="hivemq",
    )

    assert row["payload"] == {"v": 1}
    assert row["processed"] is False


def test_a_row_with_columns_the_record_does_not_declare_is_still_accepted() -> None:
    # PostgREST returns every column of an inserted row, and the schema grows
    # columns the worker never asked for. Splatting the row straight into a
    # dataclass broke on `sensors.label` the first time the worker met the
    # real database.
    row = {
        "id": "s-1",
        "device_id": "d-1",
        "type_id": "t-1",
        "source": "BMP280",
        "tag": "",
        "label": "a column this record does not declare",
        "created_at": "2026-09-10T12:00:00+00:00",
    }

    record = record_from_row(SensorRecord, row)

    assert record.id == "s-1"
    assert record.source == "BMP280"


def test_a_row_missing_a_column_the_record_requires_still_fails_loudly() -> None:
    row = {"id": "s-1", "device_id": "d-1"}

    with pytest.raises(TypeError):
        record_from_row(SensorRecord, row)


def test_a_status_for_an_unregistered_device_registers_it_before_updating() -> None:
    # The retained status arrives when the worker subscribes, before any data
    # message has registered the device. Updating a row that does not exist
    # yet loses the update silently, and the device stays at the schema
    # default until it happens to reconnect.
    registry_store = FakeRegistryStore()
    sink, store = _make_sink(registry_store=registry_store)

    sink.handle_status(
        DeviceStatus(device_mac="AABBCCDDEEFF", online=True, received_at=RECEIVED_AT)
    )

    assert "AABBCCDDEEFF" in registry_store.devices
    assert store.device_status["AABBCCDDEEFF"] is True


def test_rejects_data_message_whose_topic_mac_differs_from_payload_dev() -> None:
    sink, store = _make_sink(batch_max_size=1)
    spoofed = InboundMessage(
        topic="dl/v1/112233445566/data", payload=_data_envelope(), received_at=RECEIVED_AT
    )

    sink.handle_message(spoofed)

    assert store.measurements == {}
    assert "does not match" in (store.raw_messages[0]["error"] or "")


def test_a_message_is_marked_processed_only_after_its_batch_persists() -> None:
    sink, store = _make_sink(batch_max_size=2)

    sink.handle_message(_inbound(_data_envelope(ts=1788804294)))
    assert store.processed_raw_message_ids == []

    sink.handle_message(_inbound(_data_envelope(ts=1788804295)))
    assert store.processed_raw_message_ids == [1, 2]


def test_a_message_stays_unprocessed_when_its_batch_fails() -> None:
    sink, store = _make_sink(batch_max_size=1)
    store.errors_to_raise = [SinkPermanentError("bad request")]

    sink.handle_message(_inbound(_data_envelope()))

    assert store.processed_raw_message_ids == []


def test_a_valid_message_without_readings_is_marked_processed_immediately() -> None:
    sink, store = _make_sink()
    message = json.loads(_data_envelope())
    message["ch"] = [{"c": "temperature", "u": "C", "src": "bmp280", "ok": False}]

    sink.handle_message(_inbound(json.dumps(message).encode()))

    assert store.processed_raw_message_ids == [1]


@pytest.mark.parametrize(
    "payload",
    [b'{"v":NaN}', b'{"v":Infinity}', b'{"v":"\ud800"}', b'{"v":' + b"9" * 5000 + b"}"],
    ids=["nan", "infinity", "lone-surrogate", "huge-int"],
)
def test_a_payload_json_cannot_carry_is_archived_as_text(payload: bytes) -> None:
    row = build_raw_message_row(
        topic="dl/v1/4022D83D6618/data",
        payload=payload,
        received_at=datetime(2026, 9, 10, 12, 0, tzinfo=UTC),
        error="boom",
        source="hivemq",
    )

    assert row["payload"] == payload.decode()
    json.dumps(row, ensure_ascii=False, allow_nan=False).encode("utf-8")


def test_a_message_that_fails_mid_normalize_buffers_none_of_its_readings() -> None:
    sink, store = _make_sink(batch_max_size=10)
    message = json.loads(_data_envelope())
    message["ch"].append({"c": "humidity", "u": "%", "src": "dht22", "ok": True, "val": 40.0})
    sink._registry.resolve = _fail_on_humidity(sink._registry.resolve)  # type: ignore[method-assign]

    with pytest.raises(LookupError):
        sink.handle_message(_inbound(json.dumps(message).encode()))

    assert sink.pending_count == 0


def _fail_on_humidity(resolve: Any) -> Any:
    def wrapped(reading: Any) -> Any:
        if reading.channel == "humidity":
            raise LookupError("simulated registry failure")
        return resolve(reading)

    return wrapped
