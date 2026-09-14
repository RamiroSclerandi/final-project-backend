-- Roles PostgREST needs to serve this database, standing in for what a real
-- Supabase project provisions once at platform setup -- never inside a
-- migration. `authenticator` is the login role PostgREST connects as and
-- switches away from per request, based on the JWT `role` claim
-- (PGRST_JWT_SECRET verifies the token; see tests/integration/conftest.py
-- for how it is minted). `anon` itself is created earlier, in
-- 00_bootstrap_auth.sql, because the relation-exposure migration's REVOKE
-- statements need it to already exist.
--
-- `service_role` gets BYPASSRLS: the worker authenticates as service_role in
-- production specifically to bypass the read-only RLS policies the real
-- migration installs, and this harness must reproduce that or it tests a
-- database the worker never actually runs against.
--
-- No blanket `GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon` here: the
-- relation-exposure migration explicitly revokes `anon` from
-- mv_measurements_hourly, mv_measurements_daily and v_latest_readings, and
-- this file runs after every migration. Granting `anon` here again would
-- silently re-leak exactly what that migration fixes. `authenticated` still
-- gets the blanket grant -- it approximates the platform default for
-- ordinary tables, which RLS keeps safe, and nothing in this suite depends
-- on it being narrower.
--
-- The __AUTHENTICATOR_PASSWORD__ placeholder is substituted at test time
-- with a password generated per run (see conftest.py) -- never a literal
-- secret committed here.

CREATE ROLE service_role NOLOGIN BYPASSRLS;
CREATE ROLE authenticator NOINHERIT LOGIN PASSWORD '__AUTHENTICATOR_PASSWORD__';

GRANT anon TO authenticator;
GRANT authenticated TO authenticator;
GRANT service_role TO authenticator;

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;
