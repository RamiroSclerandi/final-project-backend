-- Reconciles the deployed Cloud schema with the canonical one.
--
-- The Cloud project was created by pasting an earlier revision of the schema
-- into the SQL editor, before the aggregation columns and the boot counter
-- existed, and it was never re-applied. measurements had 10 columns there
-- against the 14 the worker and the frontend write and read. Nothing caught it
-- because the worker had only ever run against the local stack.
--
-- Every statement is idempotent: on environments built from the canonical
-- migration (local, CI) this is a no-op; on Cloud it adds only what is missing.
-- Indexes and constraints are re-asserted the same way in case the pasted
-- revision predated them too.

ALTER TABLE measurements
    ADD COLUMN IF NOT EXISTS boot         INTEGER,
    ADD COLUMN IF NOT EXISTS value_min    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS value_max    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sample_count INTEGER;

CREATE INDEX IF NOT EXISTS idx_measurements_sensor_time
    ON measurements (sensor_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_measurements_time_brin
    ON measurements USING BRIN (timestamp);

CREATE INDEX IF NOT EXISTS idx_measurements_quality
    ON measurements (timestamp DESC) WHERE quality <> 'ok';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'measurements_unique_reading') THEN
        ALTER TABLE measurements
            ADD CONSTRAINT measurements_unique_reading UNIQUE (sensor_id, timestamp);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'measurements_ts_source_valid') THEN
        ALTER TABLE measurements
            ADD CONSTRAINT measurements_ts_source_valid CHECK (ts_source IN ('device', 'server'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'measurements_quality_valid') THEN
        ALTER TABLE measurements
            ADD CONSTRAINT measurements_quality_valid CHECK (quality IN ('ok', 'out_of_range', 'suspect'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'measurements_battery_range') THEN
        ALTER TABLE measurements
            ADD CONSTRAINT measurements_battery_range
                CHECK (battery_level IS NULL OR battery_level BETWEEN 0 AND 100);
    END IF;
END
$$;
