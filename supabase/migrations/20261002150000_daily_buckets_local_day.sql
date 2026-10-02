-- Daily aggregates follow the Argentine calendar day. date_trunc('day', ...)
-- on a timestamptz cuts at midnight of the session time zone, UTC in Supabase,
-- so a "day" ran from 21:00 to 21:00 local time and mixed two local days.
-- Buckets stay timestamptz (local midnight, 03:00 UTC); only the cut moves.
-- The named zone follows any future change to Argentina's offset.
--
-- Idempotent, so it can be reapplied after its rollback.

DROP MATERIALIZED VIEW IF EXISTS mv_measurements_daily;

CREATE MATERIALIZED VIEW mv_measurements_daily AS
SELECT
    m.sensor_id,
    date_trunc('day', m.timestamp, 'America/Argentina/Buenos_Aires') AS bucket,
    avg(m.value)   AS avg_value,
    min(m.value)   AS min_value,
    max(m.value)   AS max_value,
    count(*)       AS sample_count
FROM measurements m
WHERE m.quality = 'ok'
GROUP BY m.sensor_id, date_trunc('day', m.timestamp, 'America/Argentina/Buenos_Aires');

CREATE UNIQUE INDEX idx_mv_daily ON mv_measurements_daily (sensor_id, bucket);

REVOKE ALL ON mv_measurements_daily FROM anon;
GRANT SELECT ON mv_measurements_daily TO authenticated;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron') THEN
        CREATE EXTENSION IF NOT EXISTS pg_cron;
        -- 03:10 UTC is 00:10 in Argentina, just after the local day closes.
        PERFORM cron.schedule(
            'refresh-daily',
            '10 3 * * *',
            $sql$REFRESH MATERIALIZED VIEW CONCURRENTLY mv_measurements_daily$sql$
        );
    END IF;
END
$$;

NOTIFY pgrst, 'reload schema';
