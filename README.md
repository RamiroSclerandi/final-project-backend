# proyecto-final-backend

Backend of the IoT datalogger platform. ESP32 nodes publish sensor readings
over MQTT to a HiveMQ Cloud broker; this repository receives them, stores them
in Supabase (Postgres), and exposes them to the web dashboard.

```
ESP32 nodes ──MQTT/TLS──> HiveMQ Cloud ──> ingest-worker ──> Supabase (Postgres)
                               ^                                   │
                               └── set-sampling-interval <─────────┤ REST / Realtime
                                   (Edge Function)                 v
                                                             web dashboard
```

## Repository layout

| Path | Contents |
|---|---|
| `ingest-worker/` | Python service that subscribes to the broker, validates and normalizes each `datalogger.v1` message, and writes it to Supabase. See its [README](ingest-worker/README.md). |
| `supabase/migrations/` | Database schema: devices, sensors, sensor types, measurements, raw message archive, device configuration, hourly and daily aggregate views, Row Level Security policies, and the `pg_cron` jobs that refresh the aggregates and purge old raw messages. |
| `supabase/rollbacks/` | Rollback scripts for migrations that need one. |
| `supabase/seed.sql` | Reference data (sensor types and their expected ranges). |
| `supabase/functions/set-sampling-interval/` | Edge Function the dashboard calls to change a node's sampling interval; it stores the configuration and publishes it to the node over MQTT. |
| `supabase/manual/` | Checks that need the full local Supabase stack. See [supabase/README.md](supabase/README.md). |
| `docs/` | Technical documentation: the worker's behavioral specification and firmware notes. |

## Data flow

1. A node publishes a JSON envelope to `dl/v1/<MAC>/data` and its online state
   to `dl/v1/<MAC>/status`.
2. The worker archives every message in `raw_messages` (processed rows are
   kept 7 days, failed ones 15), validates it, and registers unseen devices
   and sensors automatically.
3. Each valid reading becomes one row in `measurements`. Writes are idempotent
   on `(sensor_id, timestamp)`, so a resent message never duplicates data.
4. `pg_cron` refreshes the hourly and daily aggregate views; the dashboard
   reads them, plus live updates through Supabase Realtime.

## Development

Requirements: [uv](https://docs.astral.sh/uv/) for the worker, a container
runtime (Docker or Podman) for integration tests, [Deno](https://deno.com/)
for the Edge Function, and the [Supabase CLI](https://supabase.com/docs/guides/cli)
for the local stack.

```bash
# Worker: unit tests, integration tests, lint and types
cd ingest-worker
uv sync
uv run pytest -q
uv run pytest -m integration -q
uv run ruff check . && uv run ruff format --check . && uv run mypy src

# Edge Function (from the repository root)
cd supabase/functions
deno fmt --check && deno lint
deno test --allow-env --allow-net set-sampling-interval/
```

CI (`.github/workflows/ci.yml`) runs the same three groups as separate jobs:
worker unit tests with lint and types, worker integration tests against
Postgres and PostgREST containers, and the Edge Function checks.

## Deployment

- **Database and Edge Function:** applied to the Supabase project with the
  Supabase CLI (`supabase db push`, `supabase functions deploy`).
- **Worker:** a container image built from `ingest-worker/Dockerfile`, with
  configuration passed as environment variables at run time. See the worker
  README for the variables and the container commands.
