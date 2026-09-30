-- Audit G-13: `upsert_device_configs` (FOR ALL ... USING (true)) let any signed-in
-- user write or delete any device's configuration straight through PostgREST.
-- Users keep `read_device_configs`; writes go only through the set-sampling-interval
-- Edge Function, which authorizes under the caller's RLS and writes as service_role.
-- Deploy that function before applying this migration.
-- Rollback: supabase/rollbacks/20260930120000_restrict_device_configs_writes.sql
DROP POLICY IF EXISTS "upsert_device_configs" ON device_configs;
