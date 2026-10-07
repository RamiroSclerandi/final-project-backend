"""Structured JSON logging and in-process metrics.

Counters owned by the source and sink are read by `build_metrics_snapshot`,
never duplicated here. `SeqGapTracker` is a live, non-authoritative signal;
the authoritative check is `docs/queries/seq_gaps.sql`, which
`compute_seq_gaps` ports for testing.
"""

import json
import logging
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast

from ingest.domain.payload import DataloggerV1

logger = logging.getLogger(__name__)


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line, with `log_event` fields as top-level keys."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        fields = getattr(record, "ingest_fields", None)
        if isinstance(fields, dict):
            payload.update(cast(dict[str, object], fields))
        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    """Log JSON lines to stdout, the container's only log transport."""
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


def log_event(event: str, level: int = logging.INFO, **fields: object) -> None:
    """Emit one structured record; pass ids, sizes and counts, never payloads or secrets."""
    logger.log(level, event, extra={"ingest_fields": fields})


@dataclass
class Metrics:
    """Counters this module owns; the source and sink keep their own."""

    messages_received_data_total: int = 0
    messages_received_status_total: int = 0
    raw_messages_archived_total: int = 0
    measurements_rows_submitted_total: int = 0
    measurements_rows_written_total: int = 0
    channels_failed_total: int = 0
    device_status_updates_total: int = 0
    seq_gap_detected_total: int = 0

    @property
    def messages_received_total(self) -> int:
        """Data-topic plus status-topic messages received, combined."""
        return self.messages_received_data_total + self.messages_received_status_total

    def record_message_received(self, kind: str) -> None:
        """Count one message from the `data` or `status` topic."""
        if kind == "data":
            self.messages_received_data_total += 1
        elif kind == "status":
            self.messages_received_status_total += 1
        else:
            raise ValueError(f"unknown message kind: {kind!r}")

    def record_raw_message_archived(self) -> None:
        """Record one message archived to `raw_messages`."""
        self.raw_messages_archived_total += 1

    def record_measurement_rows(self, submitted: int, written: int) -> None:
        """Add one batch's submitted and written row counts."""
        self.measurements_rows_submitted_total += submitted
        self.measurements_rows_written_total += written

    def record_channels_failed(self, count: int) -> None:
        """Record channels with `ok: false` in one message (per-channel failure rate)."""
        self.channels_failed_total += count

    def record_device_status_update(self) -> None:
        """Record one retained online/offline status update."""
        self.device_status_updates_total += 1

    def record_seq_gap_detected(self) -> None:
        """Record one live, non-authoritative `(boot, seq)` gap signal."""
        self.seq_gap_detected_total += 1


class SourceCounters(Protocol):
    """The subset of `HiveMQSource` a metrics snapshot reads, structurally."""

    @property
    def dropped_count(self) -> int: ...

    @property
    def oversized_count(self) -> int: ...


class SinkCounters(Protocol):
    """The subset of `MeasurementSink` a metrics snapshot reads, structurally."""

    batches_written_count: int
    batch_retries_count: int
    batch_failed_count: int
    duplicates_skipped_count: int


def build_metrics_snapshot(
    metrics: Metrics,
    source: SourceCounters | None = None,
    sink: SinkCounters | None = None,
) -> dict[str, int]:
    """Merge these counters with the source's and sink's, when given."""
    snapshot: dict[str, int] = {
        "mqtt_messages_received_total": metrics.messages_received_total,
        "raw_messages_archived_total": metrics.raw_messages_archived_total,
        "measurements_rows_submitted_total": metrics.measurements_rows_submitted_total,
        "measurements_rows_written_total": metrics.measurements_rows_written_total,
        "channels_failed_total": metrics.channels_failed_total,
        "device_status_updates_total": metrics.device_status_updates_total,
        "seq_gap_detected_total": metrics.seq_gap_detected_total,
    }
    if source is not None:
        snapshot["ingest_queue_dropped_total"] = source.dropped_count
        snapshot["payload_rejected_total"] = source.oversized_count
    if sink is not None:
        snapshot["sink_batches_total"] = sink.batches_written_count
        snapshot["sink_batch_retries_total"] = sink.batch_retries_count
        snapshot["sink_batch_failed_total"] = sink.batch_failed_count
        snapshot["duplicates_skipped_total"] = sink.duplicates_skipped_count
    return snapshot


def log_metrics_snapshot(
    metrics: Metrics,
    source: SourceCounters | None = None,
    sink: SinkCounters | None = None,
) -> None:
    """Emit one periodic JSON-line `metrics` record on stdout."""
    log_event("metrics", **build_metrics_snapshot(metrics, source, sink))


def count_failed_channels(payload: DataloggerV1) -> int:
    """Count channels with `ok: false`; they leave no row to count later."""
    return sum(1 for channel in payload.ch if not channel.ok)


class SeqGapTracker:
    """Live per-device `(boot, seq)` gap signal; blind to gaps while the worker is down."""

    def __init__(self) -> None:
        self._last: dict[str, tuple[int, int]] = {}

    def observe(self, device_mac: str, boot: int, seq: int) -> bool:
        """Record one message's `(boot, seq)`; True on a jump within the same boot.

        Call once per message, not per channel: channels share one `seq`.
        """
        previous = self._last.get(device_mac)
        self._last[device_mac] = (boot, seq)
        if previous is None:
            return False
        previous_boot, previous_seq = previous
        if boot != previous_boot:
            return False
        return seq - previous_seq > 1


@dataclass(frozen=True)
class SeqReading:
    """The columns `docs/queries/seq_gaps.sql` reads for one row."""

    device_mac: str
    boot: int
    seq: int


@dataclass(frozen=True)
class SeqGap:
    """One detected discontinuity in a device's `(boot, seq)` sequence."""

    device_mac: str
    boot: int
    previous_seq: int
    seq: int

    @property
    def size(self) -> int:
        """How many sequence numbers are missing."""
        return self.seq - self.previous_seq


def compute_seq_gaps(rows: Iterable[SeqReading]) -> list[SeqGap]:
    """Python port of `docs/queries/seq_gaps.sql`.

    Rows are first deduplicated, like the query's `SELECT DISTINCT`, because
    every channel of a message repeats its `seq`.
    """
    distinct_rows = sorted({(row.device_mac, row.boot, row.seq) for row in rows})
    gaps: list[SeqGap] = []
    previous_seq: dict[tuple[str, int], int] = {}
    for device_mac, boot, seq in distinct_rows:
        key = (device_mac, boot)
        previous = previous_seq.get(key)
        if previous is not None and seq - previous > 1:
            gaps.append(SeqGap(device_mac, boot, previous, seq))
        previous_seq[key] = seq
    return gaps
