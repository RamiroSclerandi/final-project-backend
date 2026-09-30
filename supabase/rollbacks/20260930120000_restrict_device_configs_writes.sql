-- Reverts migrations/20260930120000_restrict_device_configs_writes.sql by
-- restoring the original policy from 20260909000000_initial_schema.sql.
-- Idempotent, so a partial apply can be rolled back safely.
DROP POLICY IF EXISTS "upsert_device_configs" ON device_configs;
CREATE POLICY "upsert_device_configs" ON device_configs
    FOR ALL TO authenticated USING (true) WITH CHECK (true);
