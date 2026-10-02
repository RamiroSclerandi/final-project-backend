-- Authoritative (boot, seq) gap query.
--
-- The worker's own `SeqGapTracker` (src/ingest/observability.py) only gives
-- a live, non-authoritative signal while the process is running. A gap that
-- spans a window during which the worker was DOWN is invisible to a counter
-- living in that process.
-- This query is the ground truth: it runs over `measurements.boot`/`seq`,
-- which are persisted independently of whether the worker was up to see
-- them land.
--
-- CRITICAL: `measurements` holds one row PER CHANNEL of one message, so the
-- same `seq` value repeats once per channel within a message. Without the
-- SELECT DISTINCT step below, a window function ordered over every row
-- mixes those repeated rows into its comparison instead of comparing one
-- value per message, which is not what "a gap between two messages" means.
-- Collapsing to one row per (device, boot, seq) first is what makes the
-- window function's delta meaningful.
--
-- `src/ingest/observability.py`'s `compute_seq_gaps` is a Python port of
-- this exact query, unit-tested (including against the real two-channel
-- capture pair `tests/fixtures/live_capture_seq7.json` /
-- `live_capture_seq8.json`) since PostgREST offers no way to run a window
-- function from the worker itself. Run this file directly against Postgres
-- (psql, or the Supabase SQL editor) for the authoritative check.
--
-- Usage: replace :device_mac with the target device's mac_address, or drop
-- the WHERE clause entirely to scan every device at once.

WITH distinct_readings AS (
    SELECT DISTINCT
        d.mac_address,
        m.boot,
        m.seq,
        m.timestamp
    FROM measurements m
    JOIN sensors s ON s.id = m.sensor_id
    JOIN devices d ON d.id = s.device_id
    WHERE d.mac_address = :device_mac
      AND m.boot IS NOT NULL
      AND m.seq IS NOT NULL
),
with_lag AS (
    SELECT
        mac_address,
        boot,
        seq,
        timestamp,
        LAG(seq) OVER (PARTITION BY mac_address, boot ORDER BY seq) AS previous_seq,
        LAG(timestamp) OVER (PARTITION BY mac_address, boot ORDER BY seq) AS previous_timestamp
    FROM distinct_readings
)
SELECT
    mac_address,
    boot,
    previous_seq,
    seq AS current_seq,
    seq - previous_seq AS gap_size,
    previous_timestamp,
    timestamp AS current_timestamp
FROM with_lag
WHERE previous_seq IS NOT NULL
  AND seq - previous_seq > 1
ORDER BY mac_address, boot, seq;
