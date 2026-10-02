-- Reverts migrations/20261002150000_daily_buckets_local_day.sql: daily buckets
-- cut at UTC midnight again and the refresh returns to 00:10 UTC.
-- Idempotent, so a partial apply can be rolled back safely.

DROP MATERIALIZED VIEW IF EXISTS mv_measurements_daily;

CREATE MATERIALIZED VIEW mv_measurements_daily AS
SELECT
    m.sensor_id,
    date_trunc('day', m.timestamp) AS bucket,
    avg(m.value)   AS avg_value,
    min(m.value)   AS min_value,
    max(m.value)   AS max_value,
    count(*)       AS sample_count
FROM measurements m
WHERE m.quality = 'ok'
GROUP BY m.sensor_id, date_trunc('day', m.timestamp);

CREATE UNIQUE INDEX idx_mv_daily ON mv_measurements_daily (sensor_id, bucket);

REVOKE ALL ON mv_measurements_daily FROM anon;
GRANT SELECT ON mv_measurements_daily TO authenticated;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule(
            'refresh-daily',
            '10 0 * * *',
            $sql$REFRESH MATERIALIZED VIEW CONCURRENTLY mv_measurements_daily$sql$
        );
    END IF;
END
$$;

NOTIFY pgrst, 'reload schema';
