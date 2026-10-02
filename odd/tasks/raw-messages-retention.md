# raw_messages retention and archive deduplication (B-7, partial)

## Objective

Bound the growth of `raw_messages` and stop a retried archive from storing the
same message twice. Part of audit follow-up B-7.

## Problem and why

`raw_messages` grows without limit (about 500 B per message) and the Supabase
Free plan turns the project read-only past 500 MB, which would make every
worker insert fail. Since B-2, a retried `archive_raw_message` whose first
insert landed leaves a duplicate row with `processed = false` forever, which
pollutes the `processed = false` monitor.

## Scope

In: daily purge job, unique archive key, worker upsert on that key, rollback.
Out (rest of B-7, needs a separate decision): `lost`/`store.drop` options
A/B/C and the `seq_gaps.sql` extension.

## Decisions (user, 2026-10-02)

- Processed rows are kept 7 days; unprocessed or errored rows 15 days.
- Sampling interval planned at 30 s to 1 min, so the Free plan holds the
  thesis data; no migration off Supabase.

## Constraints

- TDD: on. Runner: `uv run pytest -m integration -q` (migrations are only
  exercised by the integration suite), plus `uv run pytest -q`.
- Migration must be idempotent (the rollback test reapplies it) and ship a
  tested rollback. No destructive statement beyond the purge itself.
- The purge function must not be callable by `anon` or `authenticated`.

## Tasks

Route: direct inline (one migration, one rollback, one store method, their
integration tests; all read already).

- [x] T1 Purge function, daily pg_cron job, rollback, integration tests.
- [x] T2 Unique archive key and worker upsert, integration test.
- [x] T3 README and audit document update.

## Progress and evidence

| Task | Commit | Tests | Review assessment |
|---|---|---|---|
| T1 | `d563720` | 4 in `test_raw_messages_retention.py` (RED: 3 failed; the client-permission case passed vacuously before the function existed and was rechecked after GREEN), cron schedule expectation updated (RED then GREEN), cron rollback test added after the code (no RED) | medium, under budget |
| T2 | `c837db6` | 2 in `test_supabase_store.py` (duplicate archive RED then GREEN; distinct-message guard passes by design), archive-key rollback test | see PR |
| T3 | docs commit | READMEs: retention, idempotent archive, 41 integration tests | passive |

Gate on `c837db6`: `uv run pytest -q` 150 passed; `uv run pytest -m integration -q`
41 passed; `ruff check`, `ruff format --check`, `mypy src` clean.

Notes:

- The first rollback draft referenced `cron.job` in the same `IF` that checks
  for pg_cron; PL/pgSQL plans the whole condition, so it failed on a server
  without pg_cron. Fixed with a nested `IF`, caught by the rollback test.
- The archive upsert merges on conflict. That only happens on an immediate
  retry, while the row is still unprocessed, so the merge rewrites equal
  values. A broker redelivery has a new arrival time and is a new row.
- Rows double-encoded before PR #26 are older than 15 days, so the first purge
  run removes them.
- Before applying in Cloud: `raw_messages_archive_key` fails if duplicates
  already exist. None are expected (archive retries only exist since PR #32,
  which is not deployed yet).

## Next step

PR open; rest of B-7 (`lost`/`store.drop` options) pending a user decision.
