-- Reproduces the Supabase Cloud platform default this harness cannot get for
-- free from a bare Postgres image: `auto_expose_new_tables` grants `anon`
-- and `authenticated` SELECT on every new relation in `public` at creation
-- time, including materialized views and views -- which is exactly what
-- made mv_measurements_hourly, mv_measurements_daily and v_latest_readings
-- readable by `anon` with no session.
--
-- Applied right after the base schema migration and before any later
-- migration, so a relation-exposure fix that revokes `anon` afterward is not
-- immediately undone by this simulated default. Vanilla PostgreSQL's
-- `GRANT ... ON ALL TABLES IN SCHEMA` does not cover materialized views, so
-- that generic grant (see 02_postgrest_roles.sql) cannot stand in for this
-- platform behaviour; these three relations need an explicit grant.

GRANT SELECT ON mv_measurements_hourly, mv_measurements_daily, v_latest_readings
    TO anon, authenticated;
