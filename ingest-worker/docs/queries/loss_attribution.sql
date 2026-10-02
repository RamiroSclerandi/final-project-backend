-- Data-loss attribution per (device, boot), from the firmware counters stored
-- on measurements (lost, store_drop) and the seq gaps of seq_gaps.sql.
--
--   seq gap               = transport/broker loss + delta store_drop
--   total device loss     = delta lost + delta store_drop
--
-- Both counters reset on every boot, so deltas are taken within one boot.
-- `lost` counts readings lost before emission (they never got a seq, so they
-- open no gap); `store_drop` counts records dropped from the buffer after
-- emission (they do open a gap). A NULL delta means the firmware did not report
-- that counter. Only boots with at least one seq gap are listed.

WITH distinct_readings AS (
    SELECT DISTINCT d.mac_address, m.boot, m.seq, m.lost, m.store_drop
    FROM measurements m
    JOIN sensors s ON s.id = m.sensor_id
    JOIN devices d ON d.id = s.device_id
    WHERE m.boot IS NOT NULL
      AND m.seq IS NOT NULL
),
gaps_per_boot AS (
    SELECT mac_address, boot, SUM(seq - previous_seq - 1) AS total_seq_gap
    FROM (
        SELECT mac_address, boot, seq,
               LAG(seq) OVER (PARTITION BY mac_address, boot ORDER BY seq) AS previous_seq
        FROM distinct_readings
    ) lagged
    WHERE previous_seq IS NOT NULL
      AND seq > previous_seq + 1
    GROUP BY mac_address, boot
),
counter_deltas AS (
    SELECT mac_address, boot,
           MAX(store_drop) - MIN(store_drop) AS delta_store_drop,
           MAX(lost) - MIN(lost) AS delta_lost
    FROM distinct_readings
    GROUP BY mac_address, boot
)
SELECT g.mac_address,
       g.boot,
       g.total_seq_gap,
       COALESCE(c.delta_store_drop, 0) AS delta_store_drop,
       g.total_seq_gap - COALESCE(c.delta_store_drop, 0) AS true_transport_loss,
       c.delta_lost
FROM gaps_per_boot g
LEFT JOIN counter_deltas c USING (mac_address, boot)
ORDER BY g.mac_address, g.boot;
