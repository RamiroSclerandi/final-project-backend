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

- [ ] T1 Purge function, daily pg_cron job, rollback, integration tests.
- [ ] T2 Unique archive key and worker upsert, integration test.
- [ ] T3 README and audit document update.

## Progress and evidence

(updated per task)

## Next step

T1.
