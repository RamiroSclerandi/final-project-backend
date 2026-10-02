# Persist firmware loss counters (B-7, option A)

## Objective

Store `meta.lost` and `meta.store.drop` with each measurement, so data-loss
attribution covers the whole capture and not only the 7 days `raw_messages`
now retains.

## Problem and why

Firmware 1.2.0 reports pre-emission losses (`meta.lost`) and buffer drops
(`meta.store.drop`), both monotonic per boot. They only lived in
`raw_messages.payload`, which is purged after 7 days since PR #35.

## Scope

In: option A as described in `Handoff_Firmware_1.2.0_Backend_Frontend.md`
§3.1-3.2: nullable `lost` and `store_drop` columns on `measurements`, the
worker writing them, and the loss-attribution query (`seq_gaps.sql` extension,
handoff §3.2). Out: options B and C, frontend changes.

## Decisions (user, 2026-10-02)

- Option A.

## Constraints

- TDD: on. Runners: `uv run pytest -q` and `uv run pytest -m integration -q`.
- Idempotent migration with a tested rollback.
- A counter the firmware did not send is stored as NULL, not 0, so firmware
  1.1.0 rows do not claim zero losses.

## Tasks

Route: direct inline (one migration, one domain change, one query file, their
tests).

- [ ] T1 Migration adding `lost` and `store_drop` to `measurements`, rollback,
      integration test.
- [ ] T2 Worker writes both counters (`Reading`, `normalize`, measurement row).
- [ ] T3 Loss-attribution query and its integration test.
- [ ] T4 README, feature document and audit update.

## Progress and evidence

(updated per task)

## Next step

T1.
