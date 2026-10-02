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

- [x] T1 Migration adding `lost` and `store_drop` to `measurements`, rollback,
      integration test.
- [x] T2 Worker writes both counters (`Reading`, `normalize`, measurement row).
- [x] T3 Loss-attribution query and its integration test.
- [x] T4 README, feature document and audit update.

## Progress and evidence

| Task | Commit | Tests |
|---|---|---|
| T1 | `0fad007` | 3 in `test_loss_counters.py` (round trip, non-negative check, rollback), RED (`PGRST204`, missing rollback file) then GREEN |
| T2 | `e4693d1` | 2 in `test_normalize.py`, 1 in `test_supabase_sink.py`, RED then GREEN |
| T3 | `1a9f00e` | 1 in `test_loss_counters.py`: two channels, seq 1, 2, 5 with one buffer drop gives gap 2, store_drop 1, transport loss 1, lost 2; RED (missing file) then GREEN |
| T4 | docs commit | Worker README: loss counters and query, 45 integration tests |

Gate: `uv run pytest -q` 153 passed; `uv run pytest -m integration -q` 45
passed; `ruff check`, `ruff format --check`, `mypy src` clean.

Notes:

- `Meta.lost` defaults to 0 so firmware 1.1.0 still validates; `normalize`
  checks `model_fields_set` and stores NULL when the field was not sent.
- `loss_attribution.sql` follows the handoff sketch (§3.2) and adds
  `delta_lost`.
- Frontend: if its type-drift CI job regenerates types from the schema, it
  will see the two new nullable columns.

## Next step

PR open. B-7 is complete once it merges.
