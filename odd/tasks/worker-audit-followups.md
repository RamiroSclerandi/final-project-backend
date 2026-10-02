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

- [ ] T1 (B-1) Writer thread cannot die silently: isolate idle-flush failures,
      guard `Worker.run`, stop the source and exit non-zero when the writer
      crashes.
- [ ] T2 (B-2) Bounded retries with `_RETRY_DELAYS_S` for `archive_raw_message`,
      registry calls and `update_device_last_seen`.
- [ ] T3 (B-3) Rejected MQTT credentials (`Bad user name or password`,
      `Not authorized`) stop the client and make `start()` raise.
- [ ] T4 (B-4) A non-zero device `ts` is always tagged `ts_source = "device"`.
- [ ] T5 (B-5) README test counts; decide the two untracked demo SQL files.
- [ ] T6 Update the audit document with the status and evidence of each ID.

## Progress and evidence

(updated per task)

## Next step

T1.
