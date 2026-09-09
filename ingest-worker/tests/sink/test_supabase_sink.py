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

from ingest.registry import DeviceRecord, Registry, SensorRecord, SensorTypeRecord
from ingest.sink.supabase_sink import MeasurementSink, SinkPermanentError, SinkTransientError
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

    def __init__(self) -> None:
        self.raw_messages: list[dict[str, Any]] = []
        self.measurements: dict[tuple[str, str], dict[str, Any]] = {}
        self.upsert_calls: list[list[dict[str, Any]]] = []
        self.errors_to_raise: list[Exception] = []
        self.device_status: dict[str, bool] = {}
        self.device_last_seen: list[tuple[str, datetime, str | None]] = []

    def archive_raw_message(
        self, topic: str, payload: bytes, received_at: datetime, error: str | None
    ) -> None:
        self.raw_messages.append(
            {"topic": topic, "payload": payload, "received_at": received_at, "error": error}
        )

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
        self.device_status[device_mac] = online

    def update_device_last_seen(
        self, device_mac: str, at: datetime, firmware_version: str | None
    ) -> None:
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
    sink_store = sink_store if sink_store is not None else FakeSinkStore()
    registry = Registry(registry_store or FakeRegistryStore(), ttl_seconds=900, clock=clock)
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
