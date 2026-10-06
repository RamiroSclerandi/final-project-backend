-- Reverts migrations/20261006120000_get_sensor_series.sql: removes the
-- bucketing RPC. No data depends on it.
-- Idempotent, so a partial apply can be rolled back safely.

DROP FUNCTION IF EXISTS public.get_sensor_series(uuid, timestamptz, timestamptz, text);

NOTIFY pgrst, 'reload schema';
