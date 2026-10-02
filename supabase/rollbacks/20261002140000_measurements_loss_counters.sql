-- Reverts migrations/20261002140000_measurements_loss_counters.sql. The worker
-- writes both columns, so roll the worker back first. The stored counters are
-- lost. Idempotent, so a partial apply can be rolled back safely.
ALTER TABLE measurements DROP CONSTRAINT IF EXISTS measurements_loss_counters_non_negative;
ALTER TABLE measurements DROP COLUMN IF EXISTS lost, DROP COLUMN IF EXISTS store_drop;

NOTIFY pgrst, 'reload schema';
