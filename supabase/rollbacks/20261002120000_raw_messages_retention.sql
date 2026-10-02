-- Reverts migrations/20261002120000_raw_messages_retention.sql: removes the
-- purge job and its function. Rows already purged are not restored.
-- Idempotent, so a partial apply can be rolled back safely.
DO $$
BEGIN
    -- Nested: PL/pgSQL plans the whole condition, so cron.job cannot appear in
    -- the same IF that checks whether pg_cron exists.
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'purge-raw-messages') THEN
            PERFORM cron.unschedule('purge-raw-messages');
        END IF;
    END IF;
END
$$;

DROP FUNCTION IF EXISTS purge_raw_messages();
