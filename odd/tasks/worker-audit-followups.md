# Worker audit follow-ups B-1 to B-5

## Objective

Close the backend follow-ups B-1 to B-5 from
`Proyecto Final/docs/Desarrollo/Auditoria_E2E_Arquitectura.md` (section
"Follow-ups por sistema", Backend), one work unit per item, with TDD.

## Problem and why

B-1 to B-3 are P1: they must be closed before the unattended worker deployment
(Oracle ARM, B-10). Today the writer thread can die silently while the paho
thread keeps the process alive, a short Supabase outage loses a whole message,
and a rejected MQTT login retries forever. B-4 and B-5 are P2 debt.

## Scope

In: only B-1 to B-5 as written in the audit document. Out: B-6 to B-10, and
`update_device_status` retries (not listed in B-2).

## Constraints

- Surgical changes; no refactor of adjacent code.
- TDD: on (user request). Runner: `uv run pytest -q` from `ingest-worker/`.
- Full gate before push: `uv run pytest -q`, `uv run pytest -m integration -q`,
  `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`.

## Delivery

Two PRs, sequential so they never conflict:

1. `fix/worker-unattended-hardening`: B-1, B-2, B-3.
2. Opened from `main` after PR 1 merges: B-4, B-5 (the README test count must
   include PR 1's tests).

## Tasks

Route for every task: direct inline (one source file plus its test file each,
already understood from reading `main.py`, `hivemq.py`, `supabase_sink.py`,
`normalize.py`).

- [x] T1 (B-1) Writer thread cannot die silently: isolate idle-flush failures,
      guard `Worker.run`, stop the source and exit non-zero when the writer
      crashes.
- [x] T2 (B-2) Bounded retries with `_RETRY_DELAYS_S` for `archive_raw_message`,
      registry calls and `update_device_last_seen`.
- [x] T3 (B-3) Rejected MQTT credentials (`Bad user name or password`,
      `Not authorized`) stop the client and make `start()` raise.
- [ ] T4 (B-4) A non-zero device `ts` is always tagged `ts_source = "device"`.
- [ ] T5 (B-5) README test counts; decide the two untracked demo SQL files.
- [ ] T6 Update the audit document with the status and evidence of each ID.

## Progress and evidence

| Task | Commit | Tests | Review assessment |
|---|---|---|---|
| T1 (B-1) | `a65c05a` | 3 new in `tests/test_main.py`, RED then GREEN | medium, under budget |
| T2 (B-2) | `a875c5e` | 8 new in `tests/sink/test_supabase_sink.py`, RED then GREEN (the 4xx case passed before, as expected) | medium, slice budget reached: review granted, approved and acknowledged (lineage `review-761c523bbe4997fa`, one reliability lens) |
| T3 (B-3) | `343e9a2` | 3 new in `tests/sources/test_hivemq.py`, RED (import error) then GREEN | medium, under budget (reviewed boundary `a875c5e`) |

Gate on `343e9a2`: `uv run pytest -q` 149 passed; `uv run pytest -m integration -q`
32 passed; `ruff check`, `ruff format --check`, `mypy src` clean. Deno checks not
run: no TypeScript changed.

Design notes:

- B-1: idle-flush failures are isolated and logged, like per-message ones;
  anything else escaping `Worker.run`'s loop marks `has_crashed`, the writer
  stops the source, and `run_until_stopped` raises `SystemExit(1)`.
- B-2: `SupabaseStore` maps transport errors and 5xx to `SinkTransientError`
  for archive, registry and `last_seen` calls; `MeasurementSink._call_with_retry`
  retries them with `_RETRY_DELAYS_S`. A retried archive whose first insert
  landed can store the message twice; registry replay is idempotent.
- B-3: paho maps MQTT 3.1.1 CONNACK rc 4 and 5 to the two reason names
  (checked against `convert_connack_rc_to_reason_code`); `disconnect()` inside
  `on_connect` makes `loop_forever()` return (`should_exit` on
  `MQTT_CS_DISCONNECTING`).

Review findings (informational, non-blocking) and disposition:

- `_finish` raising skips `source.stop()`: refuted, `_finish` already catches
  the flush exception (`main.py`, `Worker._finish`).
- Idle flush swallows errors: kept by design; `flush()` swaps the batch out
  before writing, so a failure does not repeat on the same rows.
- Unreachable-Supabase test uses a real closed port (about 2 s per case on
  Windows): kept, no mock of supabase-py; follow-up if it proves flaky.
- Partial registry failure mid-payload is untested: replay is idempotent; not
  added, out of B-2's stated scope.

## Next step

Push `fix/worker-unattended-hardening`, open PR 1, wait for merge. Then T4 and
T5 on a new branch from `main`.
