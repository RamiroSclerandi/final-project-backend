"""Composition root: builds the object graph and owns the writer loop.

paho blocks the main thread, so SIGTERM/SIGINT land there, and only enqueues.
`Worker` drains the queues on a writer thread, each item in its own
try/except, so one bad item never stalls the queue.

Shutdown: the signal handler stops the source and opens an 8 s grace period
(inside Docker's 10 s). The writer drains until the queues are empty or the
deadline passes, then force-flushes the pending batch.
"""

import logging
import queue
import signal
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import Protocol

from pydantic import ValidationError
from supabase import Client, ClientOptions, create_client

from ingest.config import Settings
from ingest.domain.payload import DataloggerV1
from ingest.observability import (
    Metrics,
    SeqGapTracker,
    SourceCounters,
    configure_logging,
    count_failed_channels,
    log_event,
    log_metrics_snapshot,
)
from ingest.registry import Registry
from ingest.sink.supabase_sink import MeasurementSink, SupabaseStore
from ingest.sources.base import DeviceStatus, InboundMessage
from ingest.sources.hivemq import HiveMQSource

logger = logging.getLogger(__name__)

_QUEUE_POLL_TIMEOUT_S = 0.5
_SHUTDOWN_GRACE_S = 8.0
# supabase-py defaults to 120 s, long enough to stall the writer past the shutdown grace.
_POSTGREST_TIMEOUT_S = 10


class WorkerSource(SourceCounters, Protocol):
    """The subset of a running `MessageSource` the writer loop drains and reports on."""

    @property
    def inbound_queue(self) -> "queue.Queue[InboundMessage]": ...

    @property
    def status_queue(self) -> "queue.Queue[DeviceStatus]": ...


class Worker:
    """Drains a source's queues on one thread and owns graceful shutdown."""

    def __init__(
        self,
        source: WorkerSource,
        sink: MeasurementSink,
        metrics: Metrics,
        seq_gap_tracker: SeqGapTracker,
        shutdown_grace_s: float = _SHUTDOWN_GRACE_S,
        poll_timeout_s: float = _QUEUE_POLL_TIMEOUT_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._sink = sink
        self._metrics = metrics
        self._seq_gap_tracker = seq_gap_tracker
        self._shutdown_grace_s = shutdown_grace_s
        self._poll_timeout_s = poll_timeout_s
        self._clock = clock
        self._shutdown_event = threading.Event()
        self._deadline: float | None = None
        self.shutdown_flushed_rows_total = 0
        self.shutdown_undrained_total = 0
        self.has_crashed = False

    def request_shutdown(self) -> None:
        """Stop once the queues drain or the grace period ends; idempotent, any thread."""
        if not self._shutdown_event.is_set():
            self._deadline = self._clock() + self._shutdown_grace_s
        self._shutdown_event.set()

    def run(self) -> None:
        """Drain until told to stop, then force-flush; a crash sets `has_crashed`."""
        log_event("writer_loop_started")
        try:
            while not self._shutdown_deadline_passed():
                drained = self._drain_once()
                if self._shutdown_event.is_set() and not drained:
                    break
        except Exception as exc:
            self.has_crashed = True
            log_event("writer_loop_crashed", level=logging.ERROR, error=str(exc))
            self.request_shutdown()
        self._finish()

    def _shutdown_deadline_passed(self) -> bool:
        return (
            self._shutdown_event.is_set()
            and self._deadline is not None
            and self._clock() >= self._deadline
        )

    def _drain_once(self) -> bool:
        drained = False
        try:
            message = self._source.inbound_queue.get(timeout=self._poll_timeout_s)
        except queue.Empty:
            message = None
        if message is not None:
            drained = True
            self._handle_data_message(message)
        else:
            # Idle: a partial batch must still flush once it reaches BATCH_MAX_AGE_MS.
            try:
                self._track_and_flush(self._sink.flush_if_due)
            except Exception as exc:
                log_event("writer_loop_flush_failed", level=logging.ERROR, error=str(exc))

        try:
            status = self._source.status_queue.get_nowait()
        except queue.Empty:
            status = None
        if status is not None:
            drained = True
            self._handle_status_message(status)

        return drained

    def _handle_data_message(self, message: InboundMessage) -> None:
        self._metrics.record_message_received("data")
        payload = self._try_parse(message.payload)
        added = sum(1 for channel in payload.ch if channel.ok) if payload is not None else 0
        try:
            self._track_and_flush(lambda: self._sink.handle_message(message), added=added)
        except Exception as exc:
            log_event(
                "writer_loop_message_failed",
                level=logging.ERROR,
                topic=message.topic,
                error=str(exc),
            )
            return
        self._metrics.record_raw_message_archived()
        if payload is not None:
            self._record_payload_signals(payload)

    def _handle_status_message(self, status: DeviceStatus) -> None:
        self._metrics.record_message_received("status")
        try:
            self._sink.handle_status(status)
        except Exception as exc:
            log_event(
                "writer_loop_status_failed",
                level=logging.ERROR,
                device_mac=status.device_mac,
                error=str(exc),
            )
            return
        self._metrics.record_device_status_update()

    def _record_payload_signals(self, payload: DataloggerV1) -> None:
        failed = count_failed_channels(payload)
        if failed:
            self._metrics.record_channels_failed(failed)
        gap = self._seq_gap_tracker.observe(payload.dev, boot=payload.meta.boot, seq=payload.seq)
        if gap:
            self._metrics.record_seq_gap_detected()
            log_event(
                "seq_gap_detected", device_mac=payload.dev, boot=payload.meta.boot, seq=payload.seq
            )

    def _track_and_flush(self, flush: Callable[[], None], added: int = 0) -> int:
        """Run a call that may flush one batch and record it; returns rows submitted."""
        pending_before = self._sink.pending_count
        written_before = self._sink.batches_written_count
        duplicates_before = self._sink.duplicates_skipped_count
        failed_before = self._sink.batch_failed_count

        flush()

        flushed = self._sink.batches_written_count > written_before
        failed = self._sink.batch_failed_count > failed_before
        if not (flushed or failed):
            return 0

        submitted = pending_before + added
        written = (
            submitted - (self._sink.duplicates_skipped_count - duplicates_before) if flushed else 0
        )
        self._metrics.record_measurement_rows(submitted=submitted, written=written)
        log_metrics_snapshot(self._metrics, source=self._source, sink=self._sink)
        return submitted

    def _try_parse(self, payload: bytes) -> DataloggerV1 | None:
        try:
            return DataloggerV1.model_validate_json(payload)
        except ValidationError:
            return None

    def _finish(self) -> None:
        undrained = self._source.inbound_queue.qsize() + self._source.status_queue.qsize()
        try:
            flushed = self._track_and_flush(self._sink.flush)
        except Exception as exc:
            log_event("writer_loop_final_flush_failed", level=logging.ERROR, error=str(exc))
            flushed = 0
        self.shutdown_flushed_rows_total = flushed
        self.shutdown_undrained_total = undrained
        log_event(
            "writer_loop_stopped",
            shutdown_flushed_rows_total=flushed,
            shutdown_undrained_total=undrained,
        )


class BlockingSource(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...


class Drainable(Protocol):
    @property
    def has_crashed(self) -> bool: ...
    def run(self) -> None: ...
    def request_shutdown(self) -> None: ...


def run_until_stopped(source: BlockingSource, worker: Drainable, join_timeout_s: float) -> None:
    """Run the writer thread while `source.start()` blocks.

    The daemon writer is released even when `start()` raises. A crashed writer
    stops the source and exits with code 1 so a restart policy sees it.
    """

    def _run_writer() -> None:
        worker.run()
        if worker.has_crashed:
            source.stop()

    writer_thread = threading.Thread(target=_run_writer, name="ingest-writer", daemon=True)
    writer_thread.start()
    try:
        source.start()
    finally:
        worker.request_shutdown()
        writer_thread.join(timeout=join_timeout_s)
    if worker.has_crashed:
        raise SystemExit(1)


def build_supabase_client(url: str, key: str) -> Client:
    """Create the Supabase client with PostgREST calls bounded to `_POSTGREST_TIMEOUT_S`."""
    return create_client(url, key, ClientOptions(postgrest_client_timeout=_POSTGREST_TIMEOUT_S))


def _load_settings() -> Settings:
    """Load `Settings` or exit with code 1, logging the field name but never its value."""
    try:
        # Fields come from the environment; mypy cannot see that without the pydantic plugin.
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        configure_logging("INFO")
        log_event("startup_configuration_invalid", level=logging.ERROR, error=str(exc))
        raise SystemExit(1) from exc


def main() -> None:
    """Build the object graph, start the writer thread and block on the network thread."""
    settings = _load_settings()
    configure_logging(settings.log_level)
    log_event("worker_starting", client_id=settings.mqtt_client_id)

    client = build_supabase_client(
        settings.supabase_url, settings.supabase_service_role_key.get_secret_value()
    )
    store = SupabaseStore(client)
    registry = Registry(store, ttl_seconds=settings.registry_cache_ttl_s)
    sink = MeasurementSink(
        store=store,
        registry=registry,
        batch_max_size=settings.batch_max_size,
        batch_max_age_ms=settings.batch_max_age_ms,
    )
    source = HiveMQSource(settings)
    worker = Worker(source, sink, Metrics(), SeqGapTracker())

    def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
        log_event("shutdown_signal_received", signal=signal.Signals(signum).name)
        source.stop()
        worker.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    run_until_stopped(source, worker, join_timeout_s=_SHUTDOWN_GRACE_S + 1)
    log_event("worker_stopped")


if __name__ == "__main__":
    main()
