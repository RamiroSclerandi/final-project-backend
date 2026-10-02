# ingest-worker

MQTT-to-Supabase ingestion worker. Subscribes to `dl/v1/+/data` and
`dl/v1/+/status` on the HiveMQ Cloud broker, validates and normalizes
`datalogger.v1` payloads, auto-registers devices/sensors on first sight, and
persists measurements idempotently to Supabase.

See `SDD_Worker_Ingesta.md` for the full behavioral spec, and
`sdd/worker-ingesta-mqtt/design` (Engram) for the architecture and design
decisions this implementation follows.

## Configuration

All configuration is read from the environment at startup and validated
before any connection is attempted (`src/ingest/config.py`). A missing or
blank required variable makes the process exit non-zero immediately, naming
the field -- never the value.

Copy `.env.example` to `.env` and fill in the deployment-specific values:

```bash
cp .env.example .env
```

**The worker's MQTT credential MUST be subscribe-only and distinct from the
device's credential.** The ESP32 devices publish data/status and subscribe
to their own config topic; the worker only ever subscribes. Sharing a
credential between the two, or granting the worker publish rights, violates
the spec's "MQTT Credential Scope, Uniqueness, and No-Publish" requirement
and defeats the point of scoping broker permissions per role.

`MQTT_CLIENT_ID_PREFIX` is combined with a per-process random suffix at
runtime (`Settings.mqtt_client_id`) so that an overlapping deploy never
shares a client id with a still-running instance -- a duplicate id makes the
broker disconnect one of them (MQTT-3.1.4-2).

`MQTT_CA_CERT_PATH` can stay empty: the default certifi CA bundle validates
the HiveMQ Cloud broker's certificate chain (Let's Encrypt) without any
custom CA -- confirmed by spike S2, recorded in Engram
`sdd/worker-ingesta-mqtt/spike-s2-writeup`.

## Running locally

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest -q          # 150 unit tests, no live broker or database required
uv run pytest -m integration -q   # 32 integration tests, needs a container runtime
uv run ruff check .
uv run ruff format --check .
uv run mypy src

uv run python -m ingest.main   # requires a valid .env or exported env vars
```

Stop a local run with Ctrl+C (SIGINT): the worker stops accepting new
messages, drains what is already queued, force-flushes the pending batch,
and exits -- the same sequence a container's `docker stop` triggers with
SIGTERM.

## Running in a container

```bash
docker build -t ingest-worker .
docker run --rm --env-file .env ingest-worker
```

Never bake `.env` or any credential into the image (`.dockerignore` excludes
it, and no `COPY . .` is used in the `Dockerfile`); pass configuration at run
time only, e.g. `--env-file` or individual `-e` flags.

The `Dockerfile` uses exec-form `CMD ["python", "-m", "ingest.main"]` so the
Python process is PID 1 and receives `SIGTERM` directly -- a shell-form CMD
would put a shell at PID 1 and the signal would never reach the interpreter.

`docker stop` sends `SIGTERM`, waits 10 seconds by default (Linux), then
sends `SIGKILL`. The worker budgets 8 of those 10 seconds
(`_SHUTDOWN_GRACE_S` in `src/ingest/main.py`) to drain its queue and flush
the pending batch, leaving headroom before `SIGKILL` would otherwise cut it
off mid-write. To verify this manually against a running container:

```bash
docker run -d --name ingest-worker-test --env-file .env ingest-worker
# publish a few messages, then:
docker stop ingest-worker-test   # sends SIGTERM
docker logs ingest-worker-test   # look for the writer_loop_stopped event:
                                  # shutdown_flushed_rows_total and
                                  # shutdown_undrained_total, both logged
                                  # as structured JSON on stdout
```

A non-zero `shutdown_undrained_total` means messages were still queued when
the grace period ran out -- visible in the logs, not silently dropped.

## Manual end-to-end verification

Phase 12 of `sdd/worker-ingesta-mqtt/tasks` (Engram) is a manual protocol
against the real broker and a real Supabase project, covering CA-1 through
CA-9: connected-window delivery, batch idempotency, auto-registration,
`ts:0` clock handling, corrupt-payload resilience, the `(boot, seq)` outage
gap (`docs/queries/seq_gaps.sql`), and confirming the worker's credential is
genuinely subscribe-only. It is not part of this repository's automated
tests.
