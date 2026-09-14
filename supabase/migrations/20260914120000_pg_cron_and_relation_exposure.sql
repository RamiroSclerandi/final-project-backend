-- Activates the pg_cron refresh schedule left commented out in the
-- canonical schema, and closes the relation-exposure defect measured and
-- confirmed in proyecto-final/matview-anon-leak and
-- proyecto-final/view-security-invoker-leak: `anon` could read both
-- materialized views and v_latest_readings with no session, and
-- v_latest_readings executed with its owner's privileges, bypassing the RLS
-- of every table it joins.
--
-- The REVOKE/GRANT statements below re-assert an end state already applied
-- manually on the deployed Cloud project as a stopgap (see
-- sdd/frontend-dashboard/delivery-and-remediation); they are idempotent, so
-- running them again there changes nothing. Without this migration the
-- manual fix is one schema recreation away from being undone, since
-- 20260909000000_initial_schema.sql recreates the schema by design.

-- =============================================================================
-- 1. pg_cron: scheduled refresh for both aggregate materialized views
-- =============================================================================
--
-- Guarded by a pg_available_extensions check instead of a bare CREATE
-- EXTENSION: Supabase (local CLI and Cloud) ships pg_cron preloaded, but the
-- bare postgres:16 image the ingest-worker integration suite runs against
-- does not. When the extension is unavailable, the schedule is skipped and
-- the access-control statements in sections 2-3 below still apply.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron') THEN
        CREATE EXTENSION IF NOT EXISTS pg_cron;

        -- A named job makes cron.schedule() an upsert (pg_cron >= 1.4): a
        -- re-run of this migration updates the existing job in place
        -- instead of scheduling a duplicate.
        PERFORM cron.schedule(
            'refresh-hourly',
            '5 * * * *',
            $sql$REFRESH MATERIALIZED VIEW CONCURRENTLY mv_measurements_hourly$sql$
        );

        -- No prior draft covered the daily view. 00:10 UTC mirrors the
        -- hourly job's "a few minutes past the boundary" pattern, giving
        -- the freshly-rolled-over daily bucket a short buffer before the
        -- refresh reads it.
        PERFORM cron.schedule(
            'refresh-daily',
            '10 0 * * *',
            $sql$REFRESH MATERIALIZED VIEW CONCURRENTLY mv_measurements_daily$sql$
        );
    ELSE
        RAISE NOTICE 'pg_cron unavailable in this environment; skipping schedule activation';
    END IF;
END
$$;

-- =============================================================================
-- 2. Materialized views: anon has no access; a matview cannot carry RLS, so
--    GRANT/REVOKE is its entire access-control surface (REQ-AGG-4)
-- =============================================================================

REVOKE ALL ON mv_measurements_hourly, mv_measurements_daily FROM anon;
GRANT SELECT ON mv_measurements_hourly, mv_measurements_daily TO authenticated;

-- =============================================================================
-- 3. v_latest_readings: anon has no access, and the view now applies the
--    caller's RLS instead of its owner's privileges
-- =============================================================================
--
-- Verified safe: all four joined tables (measurements, sensors,
-- sensor_types, devices) carry `FOR SELECT TO authenticated USING (true)`
-- policies, so switching to security_invoker changes nothing an
-- authenticated user currently sees. It closes a defect that stays
-- invisible until owner-based RLS lands for multi-tenancy -- at that point
-- this view would otherwise keep returning every owner's rows regardless of
-- the tables' new policies.

REVOKE ALL ON v_latest_readings FROM anon;
GRANT SELECT ON v_latest_readings TO authenticated;
ALTER VIEW v_latest_readings SET (security_invoker = on);
