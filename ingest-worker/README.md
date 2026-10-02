# ingest-worker

MQTT-to-Supabase ingestion worker. Subscribes to `dl/v1/+/data` and
`dl/v1/+/status` on the HiveMQ Cloud broker, validates and normalizes
`datalogger.v1` payloads, auto-registers devices/sensors on first sight, and
persists measurements idempotently to Supabase.

## How it works

Two threads share two bounded queues:

- The **network thread** (paho-mqtt, `src/ingest/sources/hivemq.py`) only
  receives messages and enqueues them. When a queue is full the message is
  dropped and counted, so a slow database never blocks the broker connection.
- The **writer thread** (`Worker` in `src/ingest/main.py`) drains the queues
  and owns every side effect, through `MeasurementSink`
  (`src/ingest/sink/supabase_sink.py`):
  1. archives the raw message in `raw_messages` (idempotent: a retried
     archive returns the existing row; a daily job purges processed rows after
     7 days and the rest after 15);
  2. validates it against the `datalogger.v1` model (`src/ingest/domain/`);
  3. resolves each reading to its sensor, registering unseen devices, sensor
     types and sensors (`src/ingest/registry.py`, cached with a TTL);
  4. classifies each value as `ok` or `out_of_range` against its sensor type;
  5. batches readings and upserts them into `measurements` on
     `(sensor_id, timestamp)`, so a replayed message writes nothing new;
  6. marks the raw message processed once its readings are stored.

A message with `ts: 0` (device clock not yet synchronized) is stamped with the
arrival time and tagged `ts_source = 'server'`; any other timestamp comes from
the device and is tagged `device`.

Each row also stores the firmware's loss counters for its boot: `lost`
(readings lost before emission) and `store_drop` (records dropped from the
device buffer), or NULL when the firmware does not report them.
`docs/queries/loss_attribution.sql` uses them to split each boot's `seq` gaps
into buffer drops and transport or broker loss.

Failure handling:

- A malformed or inconsistent message is archived with its error and skipped;
  it never stalls the queue.
- Timeouts, network errors and 5xx responses from Supabase are retried with
  backoff (0.5, 1 and 2 s). A 4xx is not retried.
- If the writer thread hits an unexpected error, it stops the MQTT client and
  the process exits with code 1, so the container's restart policy acts.
- If the broker rejects the credentials, the process exits with code 1 instead
  of reconnecting forever. Other connection failures reconnect with backoff
  (1 s up to 60 s).

Logs and metrics are structured JSON on stdout (`src/ingest/observability.py`).

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
custom CA.

## Running locally

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest -q          # 154 unit tests, no live broker or database required
uv run pytest -m integration -q   # 48 integration tests, needs a container runtime
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

### Running on a server

The base images are multi-architecture, so building on the server itself
produces a native image (for example `linux/arm64` on an ARM VM). To build
for another architecture from a workstation, use
`docker buildx build --platform linux/arm64 -t ingest-worker .`.

Run it detached with a restart policy, so a non-zero exit (writer crash,
rejected credentials) restarts the worker:

```bash
docker run -d --name ingest-worker --restart unless-stopped --env-file .env ingest-worker
docker logs -f ingest-worker
```

## Manual end-to-end verification

Some behaviors need the real broker and a real Supabase project, so they are
checked by hand rather than in the automated tests: delivery while connected, batch idempotency,
auto-registration, `ts:0` clock handling, resilience to corrupt payloads, the
`(boot, seq)` gap after an outage (`docs/queries/seq_gaps.sql`), and that the
worker's credential is subscribe-only.
