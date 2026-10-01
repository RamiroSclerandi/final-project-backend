# supabase

Schema migrations, seed data and the `set-sampling-interval` Edge Function.
Automated coverage of this directory runs in CI: the ingest-worker integration
suite (`ingest-worker/tests/integration/`, Postgres + PostgREST, plus a
Supabase Postgres container for the pg_cron schedule) and the Deno unit tests
under `functions/`.

## Manual local checks

Two checks need the full local stack and are deliberately not part of CI (a
`supabase start` is about a dozen containers). Neither is collected by
`deno test`; run them explicitly. Both create their own user, device and
sensor with random identifiers and delete them when they finish, even on
failure.

Prerequisites: [Supabase CLI](https://supabase.com/docs/guides/cli), Deno 2,
and a container runtime (Docker, or a running Podman machine). On a small host
you can skip the services these checks do not use:

```bash
supabase start -x studio,imgproxy,vector,logflare,mailpit,supavisor,postgres-meta,storage-api
```

Export the local keys (printed by `supabase start`, or `supabase status -o env`)
for the scripts:

```bash
export SUPABASE_ANON_KEY=<ANON_KEY>
export SUPABASE_SERVICE_ROLE_KEY=<SERVICE_ROLE_KEY>
```

### Realtime latency

An authenticated client subscribes to `postgres_changes` for `INSERT` on
`measurements` and `UPDATE` on `devices`; the service role then writes one
row of each. The INSERT must arrive in under 2 s (the script prints the
measured time); the UPDATE must arrive within 5 s.

```bash
deno run --allow-env --allow-net supabase/manual/realtime-latency.local.ts
```

### Broker round trip

The real `set-sampling-interval` function (running in the local edge runtime)
publishes to a throwaway, anonymous `eclipse-mosquitto` broker. The script
subscribes to `dl/v1/<MAC>/config`, calls the function with a real user JWT,
and asserts the `{"samplingInterval": <ms>}` payload and the
`device_configs.sampling_interval_ms` row. No Cloud credential is involved.

1. Point the function at the broker. `supabase/functions/.env` is gitignored;
   the edge runtime reads it at `supabase start`, so create it before starting
   the stack (or run `supabase stop` and start again if it already runs):

   ```bash
   printf 'MQTT_WS_URL=ws://mosquitto:9001\nMQTT_USER=test\nMQTT_PASSWORD=test\n' > supabase/functions/.env
   ```

2. Start the broker on the network `supabase start` created. The edge runtime
   runs in a container, so it can only reach the broker by container name on
   that network; the published port is for the script on the host.

   ```bash
   docker run -d --name mosquitto-manual \
     --network supabase_network_proyecto-final-backend --network-alias mosquitto \
     -p 9001:9001 eclipse-mosquitto:2 \
     sh -c 'printf "listener 9001\nprotocol websockets\nallow_anonymous true\n" > /tmp/m.conf && exec mosquitto -c /tmp/m.conf'
   ```

   (`podman` works the same way.) The network name is
   `supabase_network_<project_id>`; `project_id` is set in `config.toml`.

3. Run the check:

   ```bash
   cd supabase/functions/set-sampling-interval
   deno run --allow-env --allow-net broker-roundtrip.local.ts
   ```

   Set `MQTT_TEST_BROKER_URL` if the broker is not at `ws://127.0.0.1:9001`.

### Cleanup

Stop what you started, so nothing keeps holding memory:

```bash
docker rm -f mosquitto-manual
supabase stop
```

The `.env` file can stay; it only holds dummy values and is ignored by git.
