-- On-demand bucketing of one sensor's readings for the history chart. The
-- materialized views only cover hourly and daily buckets and lag their
-- refresh; this serves minute buckets and live ranges straight from
-- measurements, through idx_measurements_sensor_time.
--
-- Same criterion as the views: only quality = 'ok' readings, and days cut at
-- the Argentine calendar day like mv_measurements_daily (20261002150000).
-- Minute and hour buckets are unaffected by the zone, whose offset is whole
-- hours. The range is half-open, [p_from, p_to).
--
-- SECURITY INVOKER keeps RLS on measurements in force for the caller. Each
-- bucket has a maximum range width so a call stays bounded.
--
-- Idempotent, so it can be reapplied after its rollback.

CREATE OR REPLACE FUNCTION public.get_sensor_series(
    p_sensor_id uuid,
    p_from      timestamptz,
    p_to        timestamptz,
    p_bucket    text
)
RETURNS TABLE (
    bucket       timestamptz,
    avg_value    double precision,
    min_value    double precision,
    max_value    double precision,
    sample_count bigint
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $$
    -- LANGUAGE sql has no RAISE: a violation is a message that fails its cast
    -- to integer, aborting the call before any row is read.
    WITH request AS (
        SELECT CASE
            WHEN p_bucket IS NULL OR p_bucket NOT IN ('minute', 'hour', 'day') THEN
                pg_catalog.format(
                    'get_sensor_series: p_bucket must be minute, hour or day, got %L', p_bucket)
            WHEN (p_to > p_from) IS NOT TRUE THEN
                'get_sensor_series: p_to must be later than p_from'
            WHEN p_to - p_from > CASE p_bucket
                                     WHEN 'minute' THEN interval '7 days'
                                     WHEN 'hour'   THEN interval '400 days'
                                     ELSE               interval '3650 days'
                                 END THEN
                pg_catalog.format(
                    'get_sensor_series: range too wide for p_bucket %L (max 7 days for minute, '
                    '400 days for hour, 3650 days for day)', p_bucket)
        END AS violation
    )
    SELECT
        pg_catalog.date_trunc(p_bucket, m.timestamp, 'America/Argentina/Buenos_Aires') AS bucket,
        pg_catalog.avg(m.value)   AS avg_value,
        pg_catalog.min(m.value)   AS min_value,
        pg_catalog.max(m.value)   AS max_value,
        pg_catalog.count(*)       AS sample_count
    FROM public.measurements AS m
    WHERE (SELECT r.violation::integer FROM request AS r) IS NULL
      AND m.sensor_id = p_sensor_id
      AND m.timestamp >= p_from
      AND m.timestamp < p_to
      AND m.quality = 'ok'
    GROUP BY 1
    ORDER BY 1;
$$;

COMMENT ON FUNCTION public.get_sensor_series(uuid, timestamptz, timestamptz, text) IS
    'Buckets one sensor''s ok readings in [p_from, p_to) by minute, hour or day (Argentine calendar day).';

-- Functions are executable by PUBLIC by default, and Supabase also grants
-- anon explicitly; only signed-in users may call this one.
REVOKE EXECUTE ON FUNCTION public.get_sensor_series(uuid, timestamptz, timestamptz, text)
    FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.get_sensor_series(uuid, timestamptz, timestamptz, text)
    TO authenticated;

NOTIFY pgrst, 'reload schema';
