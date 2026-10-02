-- Makes archiving a message idempotent. The worker retries a failed archive
-- call; when the first insert landed but its response was lost, the retry
-- stored a second copy that stayed processed = false forever. A retry replays
-- the same topic, payload and arrival time, so those three identify a message.
--
-- Adding the constraint fails loudly if duplicates already exist; remove them
-- by hand first rather than letting a migration pick which copy survives.
--
-- Idempotent, so it can be reapplied after its rollback.

ALTER TABLE raw_messages
    ADD COLUMN IF NOT EXISTS payload_md5 TEXT GENERATED ALWAYS AS (md5(payload::text)) STORED;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'raw_messages_archive_key') THEN
        ALTER TABLE raw_messages
            ADD CONSTRAINT raw_messages_archive_key UNIQUE (topic, received_at, payload_md5);
    END IF;
END
$$;

-- PostgREST resolves upsert conflict targets from its schema cache.
NOTIFY pgrst, 'reload schema';
