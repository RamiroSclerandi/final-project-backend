# Port backend integration suites from the frontend

## Objective

Cover in this repository the backend behavior that the frontend integration
suites used to assert before they were dropped (frontend PR #33,
`test/drop-backend-integration-tests`).

## Problem and why

Eight frontend suites tested RLS, grants, pg_cron, the Realtime publication and
the Edge Function instead of frontend code. They were removed from the
frontend, leaving that behavior unverified. The source of truth for the gaps is
`Proyecto Final/docs/Desarrollo/Tests_Integracion_Backend_Pendientes.md`.

Original suites: `git show b9d3d4b:tests/integration/<name>.int.test.ts` in
`proyecto-final-frontend` (first parent of merge `19506c4`).

## Scope

In: `auth-rls`, `measurements-rls`, `device-management`, `node-health`,
`aggregation-schedule`, `remote-config-broker`, plus the Realtime publication
membership.

Out: `remote-config` (covered by `test_device_configs_rls.py`, PR #27/#28),
`data-export` and `historical-aggregates` (stay in the frontend).
`remote-config-cors` needs no port: every JSON response, including the 500
paths, already carries CORS (`index.ts:48-53`) and `index.test.ts:248,277`
verify it. The only remaining gap is an unexpected throw outside the handled
paths; fixing it is a behavior change and needs separate authorization.

## Decision (2026-10-01): option A

- Everything that fits the existing testcontainers harness (Postgres +
  PostgREST, minted JWTs) runs in CI.
- Realtime delivery and signup are verified by proxies: publication membership
  from the catalog, and `enable_signup = false` from `supabase/config.toml`.
- pg_cron runs in CI against a dedicated `supabase/postgres` container used
  only by that test.
- The real broker round trip and the Realtime latency check become manual
  local scripts in the style of `integration.local.ts`, not CI.

Rejected: a second tier against `supabase start` with mosquitto (about twelve
containers on a 7.3 GB host, heavy and brittle in CI).

## Constraints

- TDD: strict (session configuration). These are characterization tests of
  existing behavior, so RED is observed by running each new assertion against a
  deliberately wrong expectation before setting the real one.
- Runner: `uv run pytest -m integration -q` from `ingest-worker` (needs Podman
  up); Deno: `deno test --allow-env --allow-net set-sampling-interval/` from
  `supabase/functions`.
- Gates before each commit: ruff, ruff format, mypy, pytest unit, pytest
  integration (and deno fmt/lint/check for T7).
- Planning heuristic of about 400 authored lines per task, advisory only.
- Delivery strategy: ask-on-risk. Forecast 400-600 lines, likely two PRs:
  PR 1 = T1-T5, PR 2 = T6-T7.

## Tasks

- [x] T1 `device-management`: column grants on `devices` and `sensors`.
  Authenticated rename of `name`/`location_ref` and sensor `label` succeeds;
  update including `mac_address` fails with 42501 and the MAC is unchanged;
  anon update leaves the row unchanged. Route: inline (one test file).
- [x] T2 `auth-rls` + `measurements-rls`: authenticated reads `devices`,
  `measurements`, `sensors`; anon gets an error or `[]`; authenticated INSERT
  into `measurements` is rejected. Route: inline.
- [x] T3 `node-health`: `raw_messages` returns `[]` for authenticated and anon;
  a sensor's first measurement appears in `v_latest_readings`. Route: inline.
- [x] T4 Realtime publication: `pg_publication_tables` for `supabase_realtime`
  includes `public.measurements` and `public.devices`. Route: inline.
- [x] T5 Signup disabled: parse `supabase/config.toml` with `tomllib` and
  assert `[auth] enable_signup = false`. Route: inline (unit test, no Docker).
- [x] T6 `aggregation-schedule`: dedicated `supabase/postgres` container,
  apply migrations, assert `pg_cron` in `pg_extension` and `cron.job` rows
  `refresh-hourly` = `5 * * * *` and `refresh-daily` = `10 0 * * *`, both
  active. Spike result: the image (matching `major_version = 17`) already
  preloads pg_cron and ships the Supabase roles and `auth.users`, so the real
  migrations apply unchanged as the `postgres` role; no bootstrap, no
  `ci.yml` change. Route: delegated (fixture plus test).
- [x] T7 Manual local scripts: broker round trip (Edge Function to ephemeral
  `eclipse-mosquitto`, assert payload `{samplingInterval}` and the
  `device_configs` row) and Realtime latency (< 2 s) against `supabase start`.
  Documented in `supabase/README.md`. Route: delegated.
- [x] T8 Follow-ups from review of PR #30 (`fix/integration-test-denial-assertions`,
  merged): anon denial asserts the exact empty result and propagates any other
  error; seed cleanup runs inside try/finally; `query_scalar` runs without a
  shell; `view_reloptions` allowlists the relation name. Commits 7509c53,
  8aab5f7, c3efcc6. Route: inline.

## Acceptance criteria

- T1-T6 run green in CI under the `integration` marker (T5 under unit).
- T7 scripts run locally and are documented; they are not collected by CI.
- `Tests_Integracion_Backend_Pendientes.md` can mark every row covered or
  explicitly manual.

## Progress

- 2026-10-01: plan created, option A chosen. No code written yet.
- 2026-10-01: T1 done, commit ed1e635 (RED: wrong code XX000 vs actual 42501; GREEN 4 passed).
- 2026-10-01: T2 done (RED: inverted assertions failed 6, wrong code XX000 failed 1; GREEN 7 passed).
- 2026-10-01: T2 commit 44c52b0; T3 commit 7a8a59f (RED: inverted assertions failed 2, GREEN passed).
- 2026-10-01: T4+T5 commit 7d82b49 (RED: wrong table name failed; signup asserted True failed; GREEN passed). Added query_scalar fixture in conftest.
- 2026-10-01: the earlier Windows+Podman flaky docker-API ConnectionError had a
  root cause: the Podman service idle timeout (`service_timeout` 5 s). Fixed in
  the machine config (`service_timeout=0`); the local integration suite is green.
- 2026-10-01: T8 recorded: PR #30 merged (commits 7509c53, 8aab5f7, c3efcc6).
- 2026-10-01: T6 done, commit ac1305d (RED: wrong hourly schedule `6 * * * *`
  failed against actual `5 * * * *`; GREEN 2 passed). Image
  `public.ecr.aws/supabase/postgres:17.6.1.167`: about 370 MB compressed,
  1.28 GB on disk; test module takes about 10 s with the image cached.
- 2026-10-01: T7 done, commit 3cbfb20. Observed locally against `supabase start`
  and an ephemeral mosquitto: Realtime INSERT delivered in 479 ms, UPDATE in
  513 ms; broker round trip returned 200, payload `{"samplingInterval":15000}`
  on `dl/v1/<MAC>/config` and the `device_configs` row persisted.

## Next step

T1-T8 done. T1-T5 and T8 shipped in PR #29/#30; T6-T7 are on
feature/port-pg-cron-and-manual-suites, ready for the user to push and open the
PR. Then mark every row of `Tests_Integracion_Backend_Pendientes.md` covered or
explicitly manual.
