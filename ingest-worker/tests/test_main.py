"""RED/GREEN tests for the writer loop (`Worker`) and graceful shutdown.

See sdd/worker-ingesta-mqtt/design's architecture diagram and Shutdown
Sequence section. `_FakeSource`/`_FakeSinkStore`/`_FakeRegistryStore` are
hand-written in-memory stand-ins, never a mock of paho-mqtt or supabase-py;
`_FakeSinkStore`/`_FakeRegistryStore` mirror `tests/sink/test_supabase_sink.py`'s
fakes, trimmed to what these tests need. Every test but one calls
`Worker.run()` synchronously after pre-populating the source's queues and
requesting shutdown up front -- deterministic, no sleep-based
synchronization. The one exception proves the loop actually stops when told
to from a different thread while it is running, using `Thread.join` with a
generous bounded timeout, never a sleep-based guess.

`main()` itself (the composition root: building the real object graph,
installing signal handlers, blocking in `HiveMQSource.start()`) is not
covered here -- it only wires already-tested components together and calls
blocking OS-level APIs, which is not meaningfully unit-testable. `Worker` is
where the real logic lives, and it is fully exercised below.
"""

import json
import queue
import threading
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from ingest.main import Worker, build_supabase_client, run_until_stopped
from ingest.observability import Metrics, SeqGapTracker
from ingest.registry import DeviceRecord, Registry, SensorRecord, SensorTypeRecord
from ingest.sink.supabase_sink import MeasurementSink
from ingest.sources.base import DeviceStatus, InboundMessage

RECEIVED_AT = datetime(2026, 9, 9, 12, 0, 0, tzinfo=UTC)


def _data_envelope(
    *,
    dev: str = "AABBCCDDEEFF",
    seq: int = 1,
    boot: int = 1,
    ts: int | None = None,
    channels: list[dict[str, Any]] | None = None,
) -> bytes:
    payload = {
        "v": 1,
        "dev": dev,
        # Defaults to a value derived from seq so two distinct messages
        # never collide on the sink's (sensor_id, timestamp) dedup key
        # unless a test deliberately reuses the same seq/ts to prove that
        # duplicate handling.
        "ts": ts if ts is not None else 1788804294 + seq,
        "seq": seq,
        "meta": {
            "rssi": -60,
            "fw": "1.2.3",
            "boot": boot,
            "ts_src": "device",
            "store": {"k": "ram", "pct": 0, "pend": 0, "drop": 0},
        },
        "ch": channels
        if channels is not None
        else [{"c": "temperature", "u": "C", "src": "bmp280", "ok": True, "val": 21.5}],
    }
    return json.dumps(payload).encode("utf-8")


def _inbound(payload: bytes, *, topic: str = "dl/v1/AABBCCDDEEFF/data") -> InboundMessage:
    return InboundMessage(topic=topic, payload=payload, received_at=RECEIVED_AT)


class _FakeClock:
    """Manually-advanced monotonic clock, for a deterministic deadline test."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _FakeSource:
    """Minimal `WorkerSource`: real bounded queues, plain counters, no paho-mqtt."""

    def __init__(self) -> None:
        self.inbound_queue: queue.Queue[InboundMessage] = queue.Queue()
        self.status_queue: queue.Queue[DeviceStatus] = queue.Queue()
        self.dropped_count = 0
        self.oversized_count = 0


class _FakeRegistryStore:
    """In-memory `RegistryStore`; mirrors `tests/sink/test_supabase_sink.py`'s fake."""

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


class _FakeSinkStore:
    """In-memory `SinkStore`; mirrors `tests/sink/test_supabase_sink.py`'s fake.

    `raise_on_archive_for_topic` lets one test simulate an unexpected store
    failure for a single message, to prove the writer loop isolates it
    instead of stalling.
    """

    def __init__(self, *, raise_on_archive_for_topic: str | None = None) -> None:
        self.raw_messages: list[dict[str, Any]] = []
        self.measurements: dict[tuple[str, str], dict[str, Any]] = {}
        self.device_status: dict[str, bool] = {}
        self.device_last_seen: list[tuple[str, datetime, str | None]] = []
        self._raise_on_archive_for_topic = raise_on_archive_for_topic

    def archive_raw_message(
        self, topic: str, payload: bytes, received_at: datetime, error: str | None
    ) -> int:
        if topic == self._raise_on_archive_for_topic:
            raise RuntimeError("simulated archive failure")
        self.raw_messages.append(
            {"topic": topic, "payload": payload, "received_at": received_at, "error": error}
        )
        return len(self.raw_messages)

    def mark_raw_messages_processed(self, raw_message_ids: list[int]) -> None:
        pass

    def upsert_measurements(self, rows: list[dict[str, Any]]) -> int:
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


def _make_worker(
    *,
    batch_max_size: int = 100,
    batch_max_age_ms: int = 60_000,
    sink_store: _FakeSinkStore | None = None,
    shutdown_grace_s: float = 5.0,
    poll_timeout_s: float = 0.01,
    clock: _FakeClock | None = None,
) -> tuple[Worker, _FakeSource, _FakeSinkStore, Metrics]:
    source = _FakeSource()
    store = sink_store if sink_store is not None else _FakeSinkStore()
    registry = Registry(_FakeRegistryStore(), ttl_seconds=900)
    sink = MeasurementSink(
        store=store,
        registry=registry,
        batch_max_size=batch_max_size,
        batch_max_age_ms=batch_max_age_ms,
    )
    metrics = Metrics()
    worker = Worker(
        source,
        sink,
        metrics,
        SeqGapTracker(),
        shutdown_grace_s=shutdown_grace_s,
        poll_timeout_s=poll_timeout_s,
        clock=clock or time.monotonic,
    )
    return worker, source, store, metrics


def test_worker_processes_queued_data_messages_then_stops() -> None:
    worker, source, store, metrics = _make_worker(batch_max_size=1)
    source.inbound_queue.put(_inbound(_data_envelope(seq=1)))
    source.inbound_queue.put(_inbound(_data_envelope(seq=2)))
    worker.request_shutdown()

    worker.run()

    assert len(store.measurements) == 2
    assert metrics.messages_received_data_total == 2
    assert metrics.raw_messages_archived_total == 2


def test_worker_processes_queued_status_messages_then_stops() -> None:
    worker, source, store, metrics = _make_worker()
    source.status_queue.put(
        DeviceStatus(device_mac="AABBCCDDEEFF", online=False, received_at=RECEIVED_AT)
    )
    worker.request_shutdown()

    worker.run()

    assert store.device_status["AABBCCDDEEFF"] is False
    assert metrics.messages_received_status_total == 1
    assert metrics.device_status_updates_total == 1


def test_worker_flushes_the_pending_batch_on_shutdown_even_below_batch_max_size() -> None:
    worker, source, store, _ = _make_worker(batch_max_size=100, batch_max_age_ms=60_000)
    source.inbound_queue.put(_inbound(_data_envelope()))
    worker.request_shutdown()

    worker.run()

    assert len(store.measurements) == 1
    assert worker.shutdown_flushed_rows_total == 1
    assert worker.shutdown_undrained_total == 0


def test_worker_archives_a_malformed_payload_and_still_processes_the_next_message() -> None:
    worker, source, store, _ = _make_worker(batch_max_size=1)
    source.inbound_queue.put(_inbound(b"not json"))
    source.inbound_queue.put(_inbound(_data_envelope()))
    worker.request_shutdown()

    worker.run()

    assert len(store.raw_messages) == 2
    assert store.raw_messages[0]["error"] is not None
    assert store.raw_messages[1]["error"] is None
    assert len(store.measurements) == 1


def test_worker_counts_channels_that_reported_ok_false() -> None:
    worker, source, store, metrics = _make_worker(batch_max_size=1)
    channels = [
        {"c": "temperature", "u": "C", "src": "bmp280", "ok": True, "val": 21.5},
        {"c": "humidity", "u": "%", "src": "bmp280", "ok": False},
    ]
    source.inbound_queue.put(_inbound(_data_envelope(channels=channels)))
    worker.request_shutdown()

    worker.run()

    assert metrics.channels_failed_total == 1
    assert len(store.measurements) == 1


def test_worker_records_a_seq_gap_when_seq_jumps_for_the_same_device() -> None:
    worker, source, _, metrics = _make_worker()
    source.inbound_queue.put(_inbound(_data_envelope(seq=7, boot=1)))
    source.inbound_queue.put(_inbound(_data_envelope(seq=10, boot=1)))
    worker.request_shutdown()

    worker.run()

    assert metrics.seq_gap_detected_total == 1


def test_worker_does_not_record_a_seq_gap_across_a_boot_change() -> None:
    worker, source, _, metrics = _make_worker()
    source.inbound_queue.put(_inbound(_data_envelope(seq=50, boot=1)))
    source.inbound_queue.put(_inbound(_data_envelope(seq=1, boot=2)))
    worker.request_shutdown()

    worker.run()

    assert metrics.seq_gap_detected_total == 0


def test_worker_records_submitted_and_written_rows_per_batch_flush() -> None:
    worker, source, _, metrics = _make_worker(batch_max_size=1)
    source.inbound_queue.put(_inbound(_data_envelope(seq=1)))
    source.inbound_queue.put(_inbound(_data_envelope(seq=2)))
    worker.request_shutdown()

    worker.run()

    assert metrics.measurements_rows_submitted_total == 2
    assert metrics.measurements_rows_written_total == 2


def test_worker_counts_a_duplicate_reading_as_submitted_but_not_written() -> None:
    worker, source, _, metrics = _make_worker(batch_max_size=1)
    message = _inbound(_data_envelope(seq=1))
    source.inbound_queue.put(message)
    source.inbound_queue.put(message)
    worker.request_shutdown()

    worker.run()

    assert metrics.measurements_rows_submitted_total == 2
    assert metrics.measurements_rows_written_total == 1


def test_worker_continues_after_an_unexpected_error_processing_one_message() -> None:
    store = _FakeSinkStore(raise_on_archive_for_topic="dl/v1/AABBCCDDEEFF/data")
    worker, source, _, _ = _make_worker(batch_max_size=1, sink_store=store)
    source.inbound_queue.put(_inbound(_data_envelope(seq=1)))
    source.inbound_queue.put(
        _inbound(_data_envelope(dev="112233445566", seq=2), topic="dl/v1/112233445566/data")
    )
    worker.request_shutdown()

    worker.run()

    assert len(store.raw_messages) == 1
    assert len(store.measurements) == 1


def test_worker_stops_draining_once_the_shutdown_deadline_has_already_passed() -> None:
    clock = _FakeClock()
    worker, source, store, _ = _make_worker(shutdown_grace_s=0.0, clock=clock)
    source.inbound_queue.put(_inbound(_data_envelope()))
    worker.request_shutdown()

    worker.run()

    assert len(store.measurements) == 0
    assert worker.shutdown_undrained_total == 1


def test_worker_stops_running_when_shutdown_is_requested_from_another_thread() -> None:
    worker, _source, _, _ = _make_worker(poll_timeout_s=0.01)
    thread = threading.Thread(target=worker.run)
    thread.start()

    worker.request_shutdown()
    thread.join(timeout=5.0)

    assert thread.is_alive() is False


def test_worker_survives_an_unexpected_error_in_the_idle_flush() -> None:
    worker, _source, _, _ = _make_worker()
    worker._sink.flush_if_due = _raise_runtime_error  # type: ignore[method-assign]
    worker.request_shutdown()

    worker.run()

    assert worker.has_crashed is False


def test_worker_marks_itself_crashed_instead_of_dying_silently() -> None:
    worker, source, _, metrics = _make_worker()
    metrics.record_message_received = _raise_runtime_error  # type: ignore[method-assign]
    source.inbound_queue.put(_inbound(_data_envelope()))

    worker.run()

    assert worker.has_crashed is True


def _raise_runtime_error(*_args: Any) -> None:
    raise RuntimeError("unexpected failure")


class _RefusingBroker:
    def start(self) -> None:
        raise ConnectionRefusedError("broker unreachable")

    def stop(self) -> None:
        pass


class _BlockingBroker:
    """Blocks in `start()` until `stop()`, like paho's `loop_forever()`."""

    def __init__(self) -> None:
        self.stopped = threading.Event()

    def start(self) -> None:
        self.stopped.wait(timeout=5)

    def stop(self) -> None:
        self.stopped.set()


class _CrashingWorker:
    has_crashed = True

    def run(self) -> None:
        pass

    def request_shutdown(self) -> None:
        pass


def test_run_until_stopped_stops_the_source_and_exits_non_zero_when_the_writer_crashes() -> None:
    broker = _BlockingBroker()

    with pytest.raises(SystemExit) as exit_info:
        run_until_stopped(broker, _CrashingWorker(), join_timeout_s=2.0)

    assert exit_info.value.code == 1
    assert broker.stopped.is_set()


class _RecordingWorker:
    """Writer stand-in that records its thread; `is_stuck` ignores shutdown requests."""

    has_crashed = False

    def __init__(self, *, is_stuck: bool = False) -> None:
        self.is_stuck = is_stuck
        self.thread: threading.Thread | None = None
        self.shutdown_requested = threading.Event()
        self.release = threading.Event()

    def run(self) -> None:
        self.thread = threading.current_thread()
        (self.release if self.is_stuck else self.shutdown_requested).wait(timeout=5)

    def request_shutdown(self) -> None:
        self.shutdown_requested.set()


def test_run_until_stopped_releases_the_writer_when_the_source_fails_to_start() -> None:
    worker = _RecordingWorker()

    with pytest.raises(ConnectionRefusedError):
        run_until_stopped(_RefusingBroker(), worker, join_timeout_s=2.0)

    assert worker.thread is not None
    assert not worker.thread.is_alive()


def test_run_until_stopped_does_not_let_a_stuck_writer_keep_the_process_alive() -> None:
    worker = _RecordingWorker(is_stuck=True)

    with pytest.raises(ConnectionRefusedError):
        run_until_stopped(_RefusingBroker(), worker, join_timeout_s=0.05)

    assert worker.thread is not None
    assert worker.thread.is_alive()
    assert worker.thread.daemon
    worker.release.set()


def test_supabase_client_bounds_postgrest_calls_to_ten_seconds() -> None:
    client = build_supabase_client("http://localhost:54321", "header.payload.signature")

    assert client.postgrest.session.timeout.read == 10.0
