"""Structured logging and in-process metrics for the ingestion worker.

This module is deliberately self-contained: it owns exactly the
counters that have no existing home elsewhere. The transport's
`dropped_count`/`oversized_count` (`ingest.sources.hivemq.HiveMQSource`) and
the sink's `batches_written_count`/`batch_retries_count`/
`batch_failed_count`/`duplicates_skipped_count`
(`ingest.sink.supabase_sink.MeasurementSink`) already exist —
`build_metrics_snapshot` surfaces them instead of duplicating their state,
so each counter is incremented in exactly one place.

Wiring `record_*` calls and `SeqGapTracker.observe()` into the running
transport/sink is the composition root's job; this module only exposes the
standalone units it drives.

The `(boot, seq)` gap itself has two independent implementations by design:
the live `SeqGapTracker` below is a non-authoritative,
in-process early signal that cannot observe a gap caused by the worker being
down; the authoritative check is `docs/queries/seq_gaps.sql`, a SQL query
over the persisted `measurements` table. `compute_seq_gaps` is a Python port
of that exact query, kept here so its `SELECT DISTINCT`-before-window-
function contract is unit-testable without a live Postgres connection.
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
    """Renders one JSON object per log line.

    Every structured field passed to `log_event` ends up as a top-level key
    alongside `timestamp`/`level`/`logger`/`event`, so a log aggregator can
    parse each line without a custom grammar.
    """

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
    """Configure the root logger for one JSON object per line on stdout.

    This worker is a containerized, long-lived process with stdout as its
    only log transport — never a file, never
    rotation, never a logging framework beyond the standard library.

    Args:
        level: A standard logging level name (`Settings.log_level`, i.e.
            `LOG_LEVEL`), case-insensitive.
    """
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


def log_event(event: str, level: int = logging.INFO, **fields: object) -> None:
    """Emit one structured log record.

    Callers pass identifiers, topics, sizes, and counts as keyword fields —
    never a raw payload or a secret value (`SUPABASE_SERVICE_ROLE_KEY`,
    `MQTT_PASSWORD`). There is no "log the payload" helper in this module on
    purpose: computing `size=len(payload)` at the call site is what a caller
    needs, and it is also all that can safely reach a log line.

    Args:
        event: A short, stable event name (e.g. `"message_dropped"`).
        level: A `logging` level constant. Defaults to `INFO`.
        **fields: Structured fields merged into the JSON record.
    """
    logger.log(level, event, extra={"ingest_fields": fields})


@dataclass
class Metrics:
    """In-process counters this module is the sole source of truth for.

    Everything else the worker already counts (queue drops, oversized
    payloads, batch outcomes, duplicates skipped) is surfaced by
    `build_metrics_snapshot` from its existing owner instead of being
    duplicated here — see the module docstring.
    """

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
        """Record one message received on the data or status topic.

        Args:
            kind: `"data"` or `"status"`.

        Raises:
            ValueError: If `kind` is neither.
        """
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
        """Record one batch's submitted and actually-written row counts.

        Args:
            submitted: Rows sent to `upsert_measurements`.
            written: Rows the upsert response actually returned
                (`submitted - written` is the exact duplicate count, already
                tracked by `MeasurementSink.duplicates_skipped_count` and
                surfaced by `build_metrics_snapshot` — this records the two
                inputs, not a third derived counter).
        """
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
    """Merge this registry's own counters with the source's and sink's.

    Args:
        metrics: This module's owned counters.
        source: The running `HiveMQSource`, if available. Its
            `dropped_count`/`oversized_count` are read directly, not
            duplicated.
        sink: The running `MeasurementSink`, if available. Its batch and
            duplicate counters are read directly, not duplicated.

    Returns:
        A flat `{counter_name: value}` mapping. A counter is present only
        when its owner was passed.
    """
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
    """Count channels with `ok: false` in one validated envelope.

    There is no persisted metric for per-channel failure rate, because a
    failed channel produces no `measurements` row (see
    `ingest.domain.normalize`) and becomes invisible again once the raw
    message is archived.

    Args:
        payload: A validated `datalogger.v1` envelope.

    Returns:
        The number of channels in this message that reported `ok: false`.
    """
    return sum(1 for channel in payload.ch if not channel.ok)


class SeqGapTracker:
    """Live, non-authoritative `(boot, seq)` signal, per device.

    This is a convenience early-warning signal only, computed while the
    worker is running. It CANNOT observe a gap that occurred while the
    worker was down, which is why the
    authoritative check is `compute_seq_gaps`/`docs/queries/seq_gaps.sql`
    over the persisted `measurements` table instead.
    """

    def __init__(self) -> None:
        self._last: dict[str, tuple[int, int]] = {}

    def observe(self, device_mac: str, boot: int, seq: int) -> bool:
        """Record one message's `(boot, seq)` for a device.

        Call this once per MESSAGE, not once per channel/reading: multiple
        channels in one message share a single `seq` value, and calling
        this per-reading would compare a seq value against itself.

        Args:
            device_mac: The device's MAC address.
            boot: The device's boot counter for this message.
            seq: The device's sequence counter for this message.

        Returns:
            `True` if `seq` jumped by more than 1 since the last message
            from this device within the same `boot`. A boot change resets
            the sequence and is never reported as a gap.
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
    """One `measurements` row's identity for gap detection.

    Mirrors exactly the columns `docs/queries/seq_gaps.sql` reads:
    `devices.mac_address`, `measurements.boot`, `measurements.seq`.
    """

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
    """Python port of `docs/queries/seq_gaps.sql`, for testing without Postgres.

    `measurements` holds one row per CHANNEL of one message, so `seq`
    repeats once per channel within a message. Rows are first reduced to
    distinct `(device_mac, boot, seq)` triples — exactly the query's
    `SELECT DISTINCT` step — before comparing consecutive values; skipping
    that step would let a multi-channel message's duplicate rows distort
    the comparison.

    Args:
        rows: Candidate `measurements` rows, already joined to their
            device's `mac_address`.

    Returns:
        One `SeqGap` per discontinuity, ordered by device then boot then
        seq. A `boot` change never produces a gap — it means the device
        restarted and its sequence legitimately reset.
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
