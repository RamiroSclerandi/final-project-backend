"""Idempotent measurement sink and its Supabase-backed persistence adapter.

Covers quality banding, device status/metadata and raw archival.

This sink batches across messages (flush at `BATCH_MAX_SIZE` rows or
`BATCH_MAX_AGE_MS` since the first buffered row, whichever comes first)
instead of one batch per message. A node emits 2 channels every 15s; per-message batching
would mean a PostgREST round trip every 15s for no benefit, since
`on_conflict="sensor_id,timestamp"` upsert idempotency is unchanged either
way. Kept deliberately; do not "fix" this back to per-message batching.

A batch upsert with `on_conflict="sensor_id,timestamp"` and
`ignore_duplicates=True` is PARTIAL, not atomic: a conflicting row is
silently skipped, its batch siblings still land, and `response.data`
contains only the rows actually written. That is why `upsert_measurements`
returns a row count instead of raising on a partial write, why the sink
compares rows submitted to rows written to count duplicates skipped (a free
duplicate-rate metric), and why retry is per batch with no atomicity
branch: whole-batch replay is idempotent under either outcome.
"""

import json
import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import fields
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

import httpx
from postgrest.exceptions import APIError
from pydantic import ValidationError

from ingest.domain.normalize import Reading, normalize
from ingest.domain.payload import DataloggerV1
from ingest.registry import (
    DeviceRecord,
    Registry,
    SensorRecord,
    SensorTypeRecord,
)
from ingest.sources.base import DeviceStatus, InboundMessage, device_mac_from_topic

if TYPE_CHECKING:
    from supabase import Client

logger = logging.getLogger(__name__)

# Per-batch retry backoff: up to 3 retries beyond the initial attempt, on
# timeout/5xx only. A 4xx is never retried — the request itself is wrong and
# retrying will not fix it.
_RETRY_DELAYS_S = (0.5, 1.0, 2.0)

# Internal tunable, not exposed as a config var — same precedent as
# `registry.py`'s cache-size constant. A node publishes 2 channels every
# 15s, so untrottled writes would touch `devices` up to 8x/minute;
# `last_seen`/`firmware_version` do not need sub-minute freshness.
_DEVICE_TOUCH_MIN_INTERVAL_S = 60.0


class SinkTransientError(Exception):
    """A batch write failed for a reason expected to succeed on retry.

    Maps to a request timeout or a Supabase 5xx response.
    """


class SinkPermanentError(Exception):
    """A batch write failed for a reason retry will not fix (a 4xx response)."""


def record_from_row[RecordT](record_type: type[RecordT], row: dict[str, Any]) -> RecordT:
    """Build a record from a database row, ignoring columns it does not declare.

    PostgREST returns every column of an inserted row, and the schema carries
    columns the worker never asks for. Splatting a row straight into a
    dataclass therefore breaks the first time someone adds a column — which
    is how `sensors.label` stopped ingestion dead. A row missing a field the
    record requires still raises, because that is a real mismatch.

    Args:
        record_type: The dataclass to build.
        row: The row as PostgREST returned it.

    Returns:
        The record, built from the fields it declares.
    """
    declared = {field.name for field in fields(record_type)}  # type: ignore[arg-type]
    return record_type(**{key: value for key, value in row.items() if key in declared})


def build_raw_message_row(
    *,
    topic: str,
    payload: bytes,
    received_at: datetime,
    error: str | None,
    source: str,
) -> dict[str, Any]:
    """Build one `raw_messages` row.

    Kept separate from the store so the row's shape can be checked without a
    live database. `source` is NOT NULL and constrained to a known transport,
    and it was omitted here until the worker first ran against the real
    schema — the in-memory fakes accept any dictionary, so no unit test could
    have caught it.

    Args:
        topic: The MQTT topic the message arrived on.
        payload: The raw bytes, stored as a JSON value when they parse and as a
            permissively decoded string otherwise, so nothing is lost.
        received_at: Server arrival time.
        error: The validation error, when the payload did not parse.
        source: The transport that delivered it, one of the values
            `raw_messages_source_valid` admits.

    Returns:
        The row to insert, unprocessed until its readings persist.
    """
    return {
        "topic": topic,
        "payload": _as_json_value(payload.decode("utf-8", errors="replace")),
        "received_at": received_at.isoformat(),
        "error": error,
        "processed": False,
        "source": source,
    }


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-finite JSON number {name}")


def _as_json_value(text: str) -> Any:
    """Parse `text` for a JSONB column, keeping it as a string when strict JSON cannot carry it."""
    try:
        value = json.loads(text, parse_constant=_reject_constant)
        json.dumps(value, ensure_ascii=False).encode("utf-8")
    except (ValueError, RecursionError):
        return text
    return value


class SinkStore(Protocol):
    """Persistence port the measurement sink needs, independent of any DB client."""

    def archive_raw_message(
        self, topic: str, payload: bytes, received_at: datetime, error: str | None
    ) -> int:
        """Archive one dequeued message to `raw_messages` and return its id."""
        ...

    def mark_raw_messages_processed(self, raw_message_ids: list[int]) -> None:
        """Flag archived messages whose readings are persisted.

        Raises:
            SinkTransientError: Timeout or a 5xx response.
            SinkPermanentError: A 4xx response.
        """
        ...

    def upsert_measurements(self, rows: list[dict[str, Any]]) -> int:
        """Upsert one batch into `measurements`.

        Returns:
            The number of rows actually written (a duplicate
            inside the batch is silently skipped, not written and not an
            error — `len(rows) - returned` is the duplicate count).

        Raises:
            SinkTransientError: Timeout or a 5xx response — the caller retries.
            SinkPermanentError: A 4xx response — the caller does not retry.
        """
        ...

    def update_device_status(self, device_mac: str, online: bool, at: datetime) -> None:
        """Reflect a retained online/offline status update. Never throttled."""
        ...

    def update_device_last_seen(
        self, device_mac: str, at: datetime, firmware_version: str | None
    ) -> None:
        """Update `last_seen`/`firmware_version`. Caller throttles the cadence."""
        ...


def _band_quality(value: float, expected_min: float | None, expected_max: float | None) -> str:
    """Classify a reading against its sensor type's accepted range.

    Never returns `"suspect"`: `measurements.quality`'s CHECK constraint
    allows that value, but no source document defines when a reading is
    suspect, so this sink only ever produces `"ok"` or `"out_of_range"`.
    """
    if expected_min is not None and value < expected_min:
        return "out_of_range"
    if expected_max is not None and value > expected_max:
        return "out_of_range"
    return "ok"


def _row(data: object) -> dict[str, Any]:
    """Narrow one postgrest response row (recursive `JSON`) to a plain dict.

    The worker trusts its own table schema for the shape of a row it just
    selected or inserted; this cast documents that trust boundary instead
    of threading `JSON`'s recursive union through every record constructor.
    """
    return cast(dict[str, Any], data)


def _to_measurement_row(reading: Reading, sensor_id: str, quality: str) -> dict[str, Any]:
    return {
        "sensor_id": sensor_id,
        "value": reading.value,
        "timestamp": reading.recorded_at.isoformat(),
        "ts_source": reading.ts_source,
        "quality": quality,
        "rssi": reading.rssi,
        "seq": reading.seq,
        "boot": reading.boot,
        "value_min": reading.value_min,
        "value_max": reading.value_max,
        "sample_count": reading.sample_count,
        "lost": reading.lost,
        "store_drop": reading.store_drop,
    }


class MeasurementSink:
    """Writer-thread logic: archive, validate, band, batch, and upsert readings.

    Threading contract: confined to the single writer thread that owns the
    pending batch and the Supabase client. Composition (draining the
    transport's queues on that thread, periodically calling `flush_if_due()`,
    and shutdown draining) is the composition root's job — this class exposes
    the testable processing unit it drives.
    """

    def __init__(
        self,
        store: SinkStore,
        registry: Registry,
        batch_max_size: int,
        batch_max_age_ms: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._store = store
        self._registry = registry
        self._batch_max_size = batch_max_size
        self._batch_max_age_ms = batch_max_age_ms
        self._clock = clock
        self._sleep = sleep
        self._pending: list[dict[str, Any]] = []
        self._pending_raw_message_ids: list[int] = []
        self._pending_since: float | None = None
        self._device_last_touch: dict[str, float] = {}
        self.batches_written_count = 0
        self.batch_retries_count = 0
        self.batch_failed_count = 0
        self.duplicates_skipped_count = 0

    @property
    def pending_count(self) -> int:
        """Rows buffered and not yet flushed."""
        return len(self._pending)

    def handle_message(self, message: InboundMessage) -> None:
        """Archive, validate, normalize, band, and buffer one data message.

        Every dequeued message is archived to `raw_messages` exactly once,
        regardless of whether it parses: a malformed payload is archived with its `error` and
        processing moves on without stalling the queue.
        """
        error: str | None = None
        payload: DataloggerV1 | None = None
        try:
            payload = DataloggerV1.model_validate_json(message.payload)
        except ValidationError as exc:
            error = str(exc)
        topic_mac = device_mac_from_topic(message.topic)
        if payload is not None and payload.dev != topic_mac:
            error = f"topic MAC {topic_mac!r} does not match payload dev {payload.dev!r}"
            payload = None

        raw_message_id = self._call_with_retry(
            lambda: self._store.archive_raw_message(
                topic=message.topic,
                payload=message.payload,
                received_at=message.received_at,
                error=error,
            )
        )
        if payload is None:
            logger.warning("archived unparseable message: topic=%s", message.topic)
            return

        readings = normalize(payload, message.received_at)
        rows = self._call_with_retry(lambda: [self._to_row(reading) for reading in readings])
        if rows:
            if not self._pending:
                self._pending_since = self._clock()
            self._pending.extend(rows)
            self._pending_raw_message_ids.append(raw_message_id)
        else:
            self._mark_processed([raw_message_id])

        self._touch_device(payload.dev, payload.meta.fw, message.received_at)
        self.flush_if_due()

    def handle_status(self, status: DeviceStatus) -> None:
        """Reflect a retained online/offline status update.

        Never throttled: unlike `last_seen`/`firmware_version`, a status
        change is rare (one per connect/disconnect) and every one must be
        visible.
        """
        self._call_with_retry(lambda: self._registry.ensure_device(status.device_mac))
        self._store.update_device_status(status.device_mac, status.online, status.received_at)

    def flush_if_due(self) -> None:
        """Flush the pending batch if it has reached size or age."""
        if not self._pending:
            return
        assert self._pending_since is not None
        size_due = len(self._pending) >= self._batch_max_size
        age_ms = (self._clock() - self._pending_since) * 1000
        if size_due or age_ms >= self._batch_max_age_ms:
            self.flush()

    def flush(self) -> None:
        """Force-flush the pending batch, even if below `batch_max_size`."""
        if not self._pending:
            return
        rows, self._pending = self._pending, []
        raw_message_ids, self._pending_raw_message_ids = self._pending_raw_message_ids, []
        self._pending_since = None
        try:
            written = self._write_batch_with_retry(rows)
        except (SinkTransientError, SinkPermanentError) as exc:
            self.batch_failed_count += 1
            logger.error("batch failed, dropping %d row(s): %s", len(rows), exc)
            return
        self.batches_written_count += 1
        self.duplicates_skipped_count += len(rows) - written
        self._mark_processed(raw_message_ids)

    def _mark_processed(self, raw_message_ids: list[int]) -> None:
        try:
            self._store.mark_raw_messages_processed(raw_message_ids)
        except (SinkTransientError, SinkPermanentError) as exc:
            logger.warning(
                "could not mark %d raw message(s) processed: %s", len(raw_message_ids), exc
            )

    def _to_row(self, reading: Reading) -> dict[str, Any]:
        expected_min, expected_max = self._registry.expected_range(reading.channel, reading.unit)
        quality = _band_quality(reading.value, expected_min, expected_max)
        return _to_measurement_row(reading, self._registry.resolve(reading), quality)

    def _touch_device(self, device_mac: str, firmware_version: str, at: datetime) -> None:
        last_touch = self._device_last_touch.get(device_mac)
        if last_touch is not None and (self._clock() - last_touch) < _DEVICE_TOUCH_MIN_INTERVAL_S:
            return
        self._call_with_retry(
            lambda: self._store.update_device_last_seen(device_mac, at, firmware_version)
        )
        self._device_last_touch[device_mac] = self._clock()

    def _call_with_retry[ResultT](self, operation: Callable[[], ResultT]) -> ResultT:
        """Run a store or registry call, retrying a transient failure with the batch backoff.

        Registry resolution is select-then-insert-on-conflict, so a replay is
        idempotent; a replayed archive at worst stores the same message twice.
        """
        for delay in _RETRY_DELAYS_S:
            try:
                return operation()
            except SinkTransientError:
                self._sleep(delay)
        return operation()

    def _write_batch_with_retry(self, rows: list[dict[str, Any]]) -> int:
        last_error: SinkTransientError | None = None
        for attempt in range(len(_RETRY_DELAYS_S) + 1):
            if attempt > 0:
                self._sleep(_RETRY_DELAYS_S[attempt - 1])
                self.batch_retries_count += 1
            try:
                return self._store.upsert_measurements(rows)
            except SinkTransientError as exc:
                last_error = exc
        assert last_error is not None
        raise last_error


class SupabaseStore:
    """Supabase-backed `RegistryStore` and `SinkStore`, wrapping one client.

    Every method is a thin, parameterized call through the supabase-py
    client (`.table(...).select/insert/upsert(...).execute()`) — never a
    hand-built SQL string. Not covered by unit tests: those exercise
    `MeasurementSink`/`Registry` against hand-written in-memory fakes of
    these two protocols instead (see tests/sink/test_supabase_sink.py and
    tests/test_registry.py), never a live Supabase project.
    """

    def __init__(self, client: "Client", source: str = "hivemq") -> None:
        self._client = client
        self._source = source

    # --- RegistryStore ---

    def select_device_by_mac(self, mac_address: str) -> DeviceRecord | None:
        with _classified_errors():
            rows = (
                self._client.table("devices")
                .select("id,mac_address,name")
                .eq("mac_address", mac_address)
                .execute()
                .data
            )
        return DeviceRecord(**_row(rows[0])) if rows else None

    def insert_device(self, mac_address: str, name: str) -> DeviceRecord:
        with _classified_errors():
            row = _row(
                self._client.table("devices")
                .insert({"mac_address": mac_address, "name": name})
                .execute()
                .data[0]
            )
        return DeviceRecord(id=row["id"], mac_address=row["mac_address"], name=row["name"])

    def select_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None:
        with _classified_errors():
            rows = (
                self._client.table("sensor_types")
                .select("id,name,unit,expected_min,expected_max")
                .eq("name", name)
                .eq("unit", unit)
                .execute()
                .data
            )
        return record_from_row(SensorTypeRecord, _row(rows[0])) if rows else None

    def insert_sensor_type(self, name: str, unit: str) -> SensorTypeRecord | None:
        with _classified_errors():
            rows = (
                self._client.table("sensor_types")
                .upsert(
                    {"name": name, "unit": unit}, on_conflict="name,unit", ignore_duplicates=True
                )
                .execute()
                .data
            )
        return record_from_row(SensorTypeRecord, _row(rows[0])) if rows else None

    def select_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None:
        with _classified_errors():
            rows = (
                self._client.table("sensors")
                .select("id,device_id,type_id,source,tag")
                .eq("device_id", device_id)
                .eq("type_id", type_id)
                .eq("source", source)
                .eq("tag", tag)
                .execute()
                .data
            )
        return record_from_row(SensorRecord, _row(rows[0])) if rows else None

    def insert_sensor(
        self, device_id: str, type_id: str, source: str, tag: str
    ) -> SensorRecord | None:
        with _classified_errors():
            rows = (
                self._client.table("sensors")
                .upsert(
                    {"device_id": device_id, "type_id": type_id, "source": source, "tag": tag},
                    on_conflict="device_id,type_id,source,tag",
                    ignore_duplicates=True,
                )
                .execute()
                .data
            )
        return record_from_row(SensorRecord, _row(rows[0])) if rows else None

    # --- SinkStore ---

    def archive_raw_message(
        self, topic: str, payload: bytes, received_at: datetime, error: str | None
    ) -> int:
        with _classified_errors():
            # A retry after a lost response hits raw_messages_archive_key and gets the
            # existing row back; it is still unprocessed, so the merge rewrites equal values.
            rows = (
                self._client.table("raw_messages")
                .upsert(
                    build_raw_message_row(
                        topic=topic,
                        payload=payload,
                        received_at=received_at,
                        error=error,
                        source=self._source,
                    ),
                    on_conflict="topic,received_at,payload_md5",
                )
                .execute()
                .data
            )
        return int(_row(rows[0])["id"])

    def mark_raw_messages_processed(self, raw_message_ids: list[int]) -> None:
        try:
            self._client.table("raw_messages").update({"processed": True}).in_(
                "id", raw_message_ids
            ).execute()
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise SinkTransientError(str(exc)) from exc
        except APIError as exc:
            raise _classify_api_error(exc) from exc

    def upsert_measurements(self, rows: list[dict[str, Any]]) -> int:
        try:
            response = (
                self._client.table("measurements")
                .upsert(rows, on_conflict="sensor_id,timestamp", ignore_duplicates=True)
                .execute()
            )
        except httpx.TimeoutException as exc:
            raise SinkTransientError(str(exc)) from exc
        except httpx.TransportError as exc:
            raise SinkTransientError(str(exc)) from exc
        except APIError as exc:
            raise _classify_api_error(exc) from exc
        return len(response.data)

    def update_device_status(self, device_mac: str, online: bool, at: datetime) -> None:
        self._client.table("devices").update({"status": online}).eq(
            "mac_address", device_mac
        ).execute()

    def update_device_last_seen(
        self, device_mac: str, at: datetime, firmware_version: str | None
    ) -> None:
        with _classified_errors():
            self._client.table("devices").update(
                {"last_seen": at.isoformat(), "firmware_version": firmware_version}
            ).eq("mac_address", device_mac).execute()


@contextmanager
def _classified_errors() -> Iterator[None]:
    """Map PostgREST failures to the sink's error taxonomy, so a caller can retry transient ones."""
    try:
        yield
    except httpx.TransportError as exc:
        raise SinkTransientError(str(exc)) from exc
    except APIError as exc:
        raise _classify_api_error(exc) from exc


def _classify_api_error(exc: APIError) -> SinkTransientError | SinkPermanentError:
    """Classify a `postgrest.exceptions.APIError` as transient or permanent.

    `APIError.code` is a Postgres SQLSTATE (e.g. `"53300"`, too-many-
    connections) when PostgREST returns a JSON error body, or the raw HTTP
    status code when it does not. A numeric code >= 500 is transient
    (retry); anything else — a 4xx, or a non-numeric SQLSTATE from a JSON
    error body — is treated as permanent, since retrying an unchanged
    request will not fix a validation or constraint failure.
    """
    code = exc.code
    if code is not None and str(code).isdigit() and int(code) >= 500:
        return SinkTransientError(str(exc))
    return SinkPermanentError(str(exc))
