"""Composition root: builds the object graph and owns the writer loop.

Two threads, one queue, per sdd/worker-ingesta-mqtt/design's architecture
diagram: the paho network thread (blocking in `HiveMQSource.start()`,
design decision D5 -- kept on the *main* thread specifically so Python
delivers SIGTERM/SIGINT there) only enqueues; `Worker` below runs on a
separate writer thread and owns every side effect. `MeasurementSink
.handle_message` (Phase 8) already does the archive/validate/normalize/
band/buffer/flush work; the piece deliberately left out until this phase is
the loop that drains `HiveMQSource.inbound_queue`/`status_queue` and drives
it -- plus wiring the `Metrics`/`SeqGapTracker` that `observability.py`
(Phase 9) already defines but nothing calls yet.

Each dequeued item is processed inside its own `try/except Exception`
(design's Error Taxonomy table: "a failure discards that item only ... no
payload can stall the queue" -- CA-7). `MeasurementSink.handle_message`
already isolates a malformed JSON payload internally (it archives the error
and returns without raising); the broader catch here additionally isolates
the rarer case of an unexpected failure past that point (for example, a
registry resolution error or a store failure), which `MeasurementSink` does
not catch on its own.

`Metrics.measurements_rows_submitted_total`/`_written_total` (defined by
Phase 9, unwired since then -- see sdd/worker-ingesta-mqtt/apply-progress's
slice 6 notes) are wired here purely from `MeasurementSink`'s existing
PUBLIC counters (`pending_count`, `batches_written_count`,
`batch_failed_count`, `duplicates_skipped_count`) plus the reading count
derived from the payload this loop already parses for
`channels_failed_total`/`SeqGapTracker`. No change to `sink/supabase_sink.py`
was needed or made.

Shutdown sequence (SIGTERM/SIGINT, budget `_SHUTDOWN_GRACE_S=8s` inside
Docker's 10s Linux grace period -- design's Shutdown Sequence section):
1. The signal handler (main thread) calls `HiveMQSource.stop()`
   (`client.disconnect()`), which makes the blocking `start()` call on the
   main thread return, and `Worker.request_shutdown()`, which records the
   deadline and flips the writer thread's stop flag.
2. The writer thread keeps draining (`queue.Queue.get(timeout=...)`) until
   both queues are empty or the deadline passes.
3. The writer thread force-flushes the pending batch unconditionally (even
   below `BATCH_MAX_SIZE`) and reports `shutdown_flushed_rows_total`/
   `shutdown_undrained_total`.

Deviation from the design's literal step 2 ("on_message additionally checks
shutdown_event and drops"): `sources/hivemq.py` is unmodified this slice --
`client.disconnect()` already stops the broker from delivering further
messages for all practical purposes, and the brief window between calling
`disconnect()` and the socket actually closing was judged not worth
touching a file outside this phase's scope (main.py, Dockerfile, README,
docs only). Flagged for `sdd-verify`.
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
from supabase import create_client

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


class WorkerSource(SourceCounters, Protocol):
    """The subset of a running `MessageSource` the writer loop drains and reports on."""

    @property
    def inbound_queue(self) -> "queue.Queue[InboundMessage]": ...

    @property
    def status_queue(self) -> "queue.Queue[DeviceStatus]": ...


class Worker:
    """Drains a source's bounded queues on one thread and owns graceful shutdown.

    See the module docstring for the shutdown sequence and the
    `measurements_rows_*` wiring approach. Threading contract: `run()` is
    meant to execute on its own thread; `request_shutdown()` is meant to be
    called from a different thread (the signal handler, on the main thread)
    and is safe to call more than once.
    """

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

    def request_shutdown(self) -> None:
        """Stop draining once the queues are empty or the grace period elapses.

        Safe to call from a different thread than `run()`, and safe to call
        more than once (a second call, e.g. SIGINT arriving after SIGTERM,
        does not push the deadline back).
        """
        if not self._shutdown_event.is_set():
            self._deadline = self._clock() + self._shutdown_grace_s
        self._shutdown_event.set()

    def run(self) -> None:
        """Drain both queues until told to stop, then force-flush and return."""
        log_event("writer_loop_started")
        while not self._shutdown_deadline_passed():
            drained = self._drain_once()
            if self._shutdown_event.is_set() and not drained:
                break
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
            # Idle: still check the age-based threshold so a partial batch
            # never sits past BATCH_MAX_AGE_MS just because no new message
            # arrived to trigger `handle_message`'s own `flush_if_due` call.
            self._track_and_flush(self._sink.flush_if_due)

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
            # Isolates a failure past `handle_message`'s own JSON-validation
            # guard (e.g. a registry resolution error) -- design's Error
            # Taxonomy table: one item's failure never stalls the queue.
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
        """Run a call that may flush the sink's pending batch, and record it.

        Args:
            flush: `MeasurementSink.handle_message`, `.flush_if_due`, or
                `.flush` -- anything that may trigger at most one batch
                write.
            added: Readings this call buffers before it might flush (0 for
                a call that only flushes, e.g. the idle/shutdown paths).

        Returns:
            The number of rows submitted in this flush, or 0 if no flush
            happened (`MeasurementSink.flush()` is a no-op when nothing is
            pending).
        """
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


def _load_settings() -> Settings:
    """Load and validate `Settings`, failing loudly before any connection.

    Raises:
        SystemExit: If a required variable is missing, blank, or invalid.
            The field name is logged; its value never is (Error Taxonomy:
            "Missing/blank secret ... Fatal at startup").
    """
    try:
        # pydantic-settings sources required fields from the environment at
        # runtime; mypy's stub-based view of BaseSettings does not know that
        # without the pydantic mypy plugin, which this project does not
        # enable (see tests/test_config.py for the same bare `Settings()`
        # call, exercised only outside mypy's `src`-only scope).
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        configure_logging("INFO")
        log_event("startup_configuration_invalid", level=logging.ERROR, error=str(exc))
        raise SystemExit(1) from exc


def main() -> None:
    """Build the object graph, run the writer thread, and block on the network thread.

    Not unit-tested: this function only wires already-tested components
    together and calls blocking OS-level APIs (`HiveMQSource.start()`,
    `signal.signal`). `Worker` -- the part with real logic -- is tested
    directly in `tests/test_main.py` against a fake source and a fake sink
    store, per this project's convention of never mocking paho-mqtt or
    supabase-py.
    """
    settings = _load_settings()
    configure_logging(settings.log_level)
    log_event("worker_starting", client_id=settings.mqtt_client_id)

    client = create_client(
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

    writer_thread = threading.Thread(target=worker.run, name="ingest-writer")
    writer_thread.start()

    source.start()  # blocks on the main thread until stop() is called (D5)

    writer_thread.join(timeout=_SHUTDOWN_GRACE_S + 1)
    log_event("worker_stopped")


if __name__ == "__main__":
    main()
