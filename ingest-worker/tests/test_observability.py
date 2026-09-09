"""RED/GREEN tests for structured logging, the metrics registry, and the
Python port of the `(boot, seq)` gap query.

See docs/SDD_Worker_Ingesta.md and design decisions D10 (the `(boot, seq)`
gap is authoritative only as a SQL query over persisted `measurements`,
never an in-process counter) and D11 (metrics transport is a periodic
JSON-line log record on stdout) (sdd/worker-ingesta-mqtt/design).
`compute_seq_gaps` mirrors `docs/queries/seq_gaps.sql` exactly, so its
`SELECT DISTINCT`-before-window-function contract is testable without a
live Postgres connection — no third party is mocked, per project
convention.
"""

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from ingest.domain.payload import DataloggerV1
from ingest.observability import (
    Metrics,
    SeqGapTracker,
    SeqReading,
    build_metrics_snapshot,
    compute_seq_gaps,
    configure_logging,
    count_failed_channels,
    log_event,
    log_metrics_snapshot,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())  # type: ignore[no-any-return]


def _full_envelope(channels_fixture_name: str) -> dict[str, Any]:
    base = _load_fixture("no_aggregation.json")
    fragment = _load_fixture(channels_fixture_name)
    return {**base, "ch": fragment["ch"]}


class _StubSource:
    """Structurally satisfies `SourceCounters` without touching paho-mqtt."""

    def __init__(self, dropped: int, oversized: int) -> None:
        self.dropped_count = dropped
        self.oversized_count = oversized


class _StubSink:
    """Structurally satisfies `SinkCounters` without touching supabase-py."""

    def __init__(self, batches: int, retries: int, failed: int, duplicates: int) -> None:
        self.batches_written_count = batches
        self.batch_retries_count = retries
        self.batch_failed_count = failed
        self.duplicates_skipped_count = duplicates


@pytest.fixture(autouse=True)
def _restore_root_logger() -> Iterator[None]:
    """Undo `configure_logging`'s global root-logger mutation after each test.

    `configure_logging` replaces `logging.getLogger().handlers`, which is
    process-wide state; leaving a handler bound to a torn-down `capsys`
    stream would break logging calls made by every test that runs after
    this file's, in this file or any other.
    """
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    root.handlers = original_handlers
    root.setLevel(original_level)


# --- Structured logging -----------------------------------------------------


def test_configure_logging_sets_the_requested_level() -> None:
    configure_logging("DEBUG")

    assert logging.getLogger().level == logging.DEBUG


def test_configure_logging_emits_one_json_object_per_line_on_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")

    log_event("worker_started", version="1.0.0")

    line = capsys.readouterr().out.strip()
    record = json.loads(line)
    assert record["event"] == "worker_started"
    assert record["level"] == "INFO"
    assert record["version"] == "1.0.0"


def test_log_event_records_topic_and_size_without_leaking_payload_content(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("WARNING")
    payload = b'{"mqtt_password": "super-secret-value"}'

    log_event(
        "message_dropped",
        level=logging.WARNING,
        topic="dl/v1/AABBCCDDEEFF/data",
        size=len(payload),
    )

    out = capsys.readouterr().out
    assert "super-secret-value" not in out
    record = json.loads(out.strip())
    assert record["topic"] == "dl/v1/AABBCCDDEEFF/data"
    assert record["size"] == len(payload)


# --- Metrics registry --------------------------------------------------------


def test_record_message_received_increments_the_counter_for_its_kind() -> None:
    metrics = Metrics()

    metrics.record_message_received("data")
    metrics.record_message_received("data")
    metrics.record_message_received("status")

    assert metrics.messages_received_data_total == 2
    assert metrics.messages_received_status_total == 1
    assert metrics.messages_received_total == 3


def test_record_message_received_rejects_an_unknown_kind() -> None:
    metrics = Metrics()

    with pytest.raises(ValueError, match="unknown message kind"):
        metrics.record_message_received("bogus")


def test_record_raw_message_archived_increments_the_counter() -> None:
    metrics = Metrics()

    metrics.record_raw_message_archived()
    metrics.record_raw_message_archived()

    assert metrics.raw_messages_archived_total == 2


def test_record_measurement_rows_accumulates_submitted_and_written_separately() -> None:
    metrics = Metrics()

    metrics.record_measurement_rows(submitted=5, written=4)
    metrics.record_measurement_rows(submitted=2, written=2)

    assert metrics.measurements_rows_submitted_total == 7
    assert metrics.measurements_rows_written_total == 6


def test_record_channels_failed_accumulates_the_count() -> None:
    metrics = Metrics()

    metrics.record_channels_failed(1)
    metrics.record_channels_failed(2)

    assert metrics.channels_failed_total == 3


def test_record_device_status_update_increments_the_counter() -> None:
    metrics = Metrics()

    metrics.record_device_status_update()

    assert metrics.device_status_updates_total == 1


def test_record_seq_gap_detected_increments_the_counter() -> None:
    metrics = Metrics()

    metrics.record_seq_gap_detected()
    metrics.record_seq_gap_detected()

    assert metrics.seq_gap_detected_total == 2


# --- Snapshot: surfacing existing counters, not duplicating them -----------


def test_build_metrics_snapshot_surfaces_the_sources_dropped_and_oversized_counters() -> None:
    metrics = Metrics()
    source = _StubSource(dropped=3, oversized=1)

    snapshot = build_metrics_snapshot(metrics, source=source)

    assert snapshot["ingest_queue_dropped_total"] == 3
    assert snapshot["payload_rejected_total"] == 1


def test_build_metrics_snapshot_surfaces_the_sinks_batch_and_duplicate_counters() -> None:
    metrics = Metrics()
    sink = _StubSink(batches=4, retries=2, failed=1, duplicates=5)

    snapshot = build_metrics_snapshot(metrics, sink=sink)

    assert snapshot["sink_batches_total"] == 4
    assert snapshot["sink_batch_retries_total"] == 2
    assert snapshot["sink_batch_failed_total"] == 1
    assert snapshot["duplicates_skipped_total"] == 5


def test_build_metrics_snapshot_omits_source_and_sink_counters_when_neither_is_given() -> None:
    metrics = Metrics()
    metrics.record_message_received("data")
    metrics.record_channels_failed(2)

    snapshot = build_metrics_snapshot(metrics)

    assert snapshot["mqtt_messages_received_total"] == 1
    assert snapshot["channels_failed_total"] == 2
    assert "ingest_queue_dropped_total" not in snapshot
    assert "sink_batches_total" not in snapshot


def test_log_metrics_snapshot_writes_one_json_record_with_every_merged_field(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    metrics = Metrics()
    metrics.record_device_status_update()
    source = _StubSource(dropped=1, oversized=0)
    sink = _StubSink(batches=1, retries=0, failed=0, duplicates=0)

    log_metrics_snapshot(metrics, source=source, sink=sink)

    record = json.loads(capsys.readouterr().out.strip())
    assert record["event"] == "metrics"
    assert record["device_status_updates_total"] == 1
    assert record["ingest_queue_dropped_total"] == 1
    assert record["sink_batches_total"] == 1


# --- Per-channel failure count (closes the spec's observability gap) -------


def test_count_failed_channels_is_zero_when_every_channel_succeeded() -> None:
    payload = DataloggerV1.model_validate(_load_fixture("no_aggregation.json"))

    assert count_failed_channels(payload) == 0


def test_count_failed_channels_counts_channels_with_ok_false() -> None:
    payload = DataloggerV1.model_validate(_full_envelope("failed_channel.json"))

    assert count_failed_channels(payload) == 1


# --- Live (boot, seq) signal (D10) ------------------------------------------


def test_seq_gap_tracker_reports_no_gap_for_the_first_message_of_a_device() -> None:
    tracker = SeqGapTracker()

    assert tracker.observe("AABBCCDDEEFF", boot=1, seq=1) is False


def test_seq_gap_tracker_reports_no_gap_for_consecutive_seq() -> None:
    tracker = SeqGapTracker()
    tracker.observe("AABBCCDDEEFF", boot=1, seq=7)

    assert tracker.observe("AABBCCDDEEFF", boot=1, seq=8) is False


def test_seq_gap_tracker_reports_a_gap_when_seq_jumps() -> None:
    tracker = SeqGapTracker()
    tracker.observe("AABBCCDDEEFF", boot=1, seq=7)

    assert tracker.observe("AABBCCDDEEFF", boot=1, seq=10) is True


def test_seq_gap_tracker_does_not_report_a_gap_across_a_boot_change() -> None:
    tracker = SeqGapTracker()
    tracker.observe("AABBCCDDEEFF", boot=1, seq=50)

    assert tracker.observe("AABBCCDDEEFF", boot=2, seq=1) is False


def test_seq_gap_tracker_tracks_each_device_independently() -> None:
    tracker = SeqGapTracker()
    tracker.observe("AAAAAAAAAAAA", boot=1, seq=7)

    assert tracker.observe("BBBBBBBBBBBB", boot=1, seq=1) is False


# --- compute_seq_gaps: Python port of docs/queries/seq_gaps.sql ------------


def test_compute_seq_gaps_finds_no_gap_for_consecutive_seq() -> None:
    rows = [
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=7),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=8),
    ]

    assert compute_seq_gaps(rows) == []


def test_compute_seq_gaps_reports_a_gap_of_the_right_size_for_a_jump() -> None:
    rows = [
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=5),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=9),
    ]

    gaps = compute_seq_gaps(rows)

    assert len(gaps) == 1
    assert gaps[0].previous_seq == 5
    assert gaps[0].seq == 9
    assert gaps[0].size == 4


def test_compute_seq_gaps_does_not_report_a_gap_across_a_boot_change() -> None:
    rows = [
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=50),
        SeqReading(device_mac="AABBCCDDEEFF", boot=2, seq=1),
    ]

    assert compute_seq_gaps(rows) == []


def test_compute_seq_gaps_ignores_duplicate_rows_from_the_same_multichannel_message() -> None:
    rows = [
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=7),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=7),  # second channel, same message
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=8),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=8),  # second channel, same message
    ]

    assert compute_seq_gaps(rows) == []


def test_compute_seq_gaps_reports_a_real_gap_even_amid_duplicate_channel_rows() -> None:
    rows = [
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=7),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=7),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=9),
        SeqReading(device_mac="AABBCCDDEEFF", boot=1, seq=9),
    ]

    gaps = compute_seq_gaps(rows)

    assert len(gaps) == 1
    assert gaps[0].previous_seq == 7
    assert gaps[0].seq == 9


def test_compute_seq_gaps_finds_no_gap_for_the_real_two_channel_capture_pair() -> None:
    """The exact negative case tests/fixtures ships for this: two real
    messages, same boot, consecutive seq, two channels each -- must never
    be flagged (sdd/worker-ingesta-mqtt/tasks, Phase 9 note)."""
    first = DataloggerV1.model_validate(_load_fixture("live_capture_seq7.json"))
    second = DataloggerV1.model_validate(_load_fixture("live_capture_seq8.json"))
    rows = [
        SeqReading(device_mac=payload.dev, boot=payload.meta.boot, seq=payload.seq)
        for payload in (first, second)
        for _ in payload.ch
    ]

    assert len(rows) == 4  # sanity: 2 messages x 2 channels each
    assert compute_seq_gaps(rows) == []
