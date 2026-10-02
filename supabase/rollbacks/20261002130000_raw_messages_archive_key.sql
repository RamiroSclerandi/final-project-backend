-- Reverts migrations/20261002130000_raw_messages_archive_key.sql. The worker
-- archives with an upsert on this key, so roll the worker back first.
-- Idempotent, so a partial apply can be rolled back safely.
ALTER TABLE raw_messages DROP CONSTRAINT IF EXISTS raw_messages_archive_key;
ALTER TABLE raw_messages DROP COLUMN IF EXISTS payload_md5;

NOTIFY pgrst, 'reload schema';
