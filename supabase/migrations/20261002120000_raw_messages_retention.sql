-- Bounds the growth of raw_messages. The archive exists to reprocess recent
-- traffic after a parser fix; measurements hold the data itself, so old raw
-- rows only cost space. On the Supabase Free plan the project turns read-only
-- past 500 MB, which would make every worker insert fail.
--
-- Retention (decided 2026-10-02): processed rows 7 days; unprocessed or failed
-- rows 15 days, since those are the ones worth inspecting.
--
-- Idempotent: the rollback test reapplies it after reverting.

CREATE OR REPLACE FUNCTION purge_raw_messages()
RETURNS bigint
LANGUAGE sql
SET search_path = public
AS $$
    WITH purged AS (
        DELETE FROM raw_messages
        WHERE (processed AND error IS NULL AND received_at < now() - interval '7 days')
           OR received_at < now() - interval '15 days'
        RETURNING 1
    )
    SELECT count(*) FROM purged;
$$;

COMMENT ON FUNCTION purge_raw_messages() IS
    'Deletes processed raw_messages older than 7 days and any other row older than 15 days; returns the number deleted.';

-- Functions are executable by PUBLIC by default, and PostgREST would expose
-- this one as an RPC to every client. Only the scheduler needs it.
REVOKE EXECUTE ON FUNCTION purge_raw_messages() FROM PUBLIC, anon, authenticated;

-- Same guard as the refresh jobs (20260914120000): the bare postgres image of
-- the integration harness has no pg_cron.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron') THEN
        CREATE EXTENSION IF NOT EXISTS pg_cron;
        -- 03:30 UTC, clear of the hourly (:05) and daily (00:10) refreshes.
        PERFORM cron.schedule(
            'purge-raw-messages',
            '30 3 * * *',
            $sql$SELECT purge_raw_messages()$sql$
        );
    ELSE
        RAISE NOTICE 'pg_cron unavailable in this environment; skipping purge schedule';
    END IF;
END
$$;
