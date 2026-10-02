-- Persists the firmware loss counters with each measurement (handoff option A).
-- They are boot-scoped and monotonic: `lost` counts readings lost before
-- emission (no seq gap), `store_drop` counts records dropped after emission
-- (they open a seq gap). Until now they only lived in raw_messages.payload,
-- which is purged after 7 days. NULL means the firmware did not report it.
--
-- Idempotent, so it can be reapplied after its rollback.

ALTER TABLE measurements
    ADD COLUMN IF NOT EXISTS lost       INTEGER,
    ADD COLUMN IF NOT EXISTS store_drop INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'measurements_loss_counters_non_negative') THEN
        ALTER TABLE measurements
            ADD CONSTRAINT measurements_loss_counters_non_negative
            CHECK ((lost IS NULL OR lost >= 0) AND (store_drop IS NULL OR store_drop >= 0));
    END IF;
END
$$;

NOTIFY pgrst, 'reload schema';
