-- Bootstrap for supabase/migrations/20260909000000_initial_schema.sql on a
-- bare Postgres container. That migration is not replayable as-is outside a
-- real Supabase project: it references the `auth` schema and grants to the
-- `authenticated` role, neither of which exist on a fresh Postgres.
--
-- This file recreates ONLY what the migration itself depends on:
--   - `auth.users(id)`, the FK target of `devices.owner_id`.
--   - the `authenticated` role, the target of every RLS policy and GRANT the
--     migration defines.
-- It is not a second copy of the schema and must never grow beyond this.

CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.users (
    id UUID PRIMARY KEY
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated NOLOGIN;
    END IF;
END
$$;
