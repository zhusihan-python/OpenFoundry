-- =====================================================================
-- WP3.2 / T8 — pipeline outbox CDC prep for an EXISTING database
-- (pipeline library: BC `pipeline_authoring` on the pg-runtime-config
-- cluster, database `app`).
--
-- WHY THIS FILE EXISTS: the CNPG bootstrap SQL only runs at FIRST
-- init; existing clusters never re-run it. Enabling the outbox-pipeline
-- Debezium connector on an existing database needs this one-time,
-- RE-RUNNABLE, SCOPED preparation instead.
--
-- Run AS the CNPG cluster superuser (`postgres`). A database/table
-- owner alone is NOT sufficient — PG16 privileges (both verified
-- against 16.16 with a CREATEROLE non-superuser):
--   * CREATE ROLE ... REPLICATION → "Only roles with the REPLICATION
--     attribute may create roles with the REPLICATION attribute";
--   * ALTER ROLE ... REPLICATION → "Only roles with the CREATEROLE
--     attribute and the ADMIN option on role ... may alter this role".
-- Superuser satisfies both (plus publication creation and GRANTs), so
-- the artifact stays single-stage; no owner/operator split is needed.
-- Run AGAINST database `app` (the database holding outbox.events).
--
-- Invocation (psql ≥ 10; the role password comes from a variable so it
-- is never inlined in this file):
--   psql -X -v ON_ERROR_STOP=1 \
--        -v cdc_password="${CDC_OUTBOX_PASSWORD:?set}" \
--        -d app \
--        -f pipeline-outbox-cdc-prep.sql
-- NOTE: psql -v places the value in the process argv; run from an
-- operator shell / a Secret-mounting Job, not from shared CI logs.
--
-- SCOPE (least privilege, mirrors pg-policy's `debezium_cdc` precedent):
--   * login role `debezium_pipeline_cdc` with REPLICATION, NOINHERIT,
--     no superuser, no BYPASSRLS;
--   * USAGE on schema `outbox` + SELECT on `outbox.events` ONLY —
--     never INSERT/UPDATE/DELETE, never any other table/schema;
--   * publication `debezium_outbox_pipeline_pub` containing EXACTLY
--     outbox.events, created here by the OWNER side. The connector
--     runs publication.autocreate.mode=disabled (Debezium 2.7.2: a
--     missing publication makes the connector throw and stop), because
--     a role with only REPLICATION+SELECT cannot create publications
--     (CREATE PUBLICATION requires the CREATE privilege on the current
--     database — PG16; that privilege is deliberately NOT granted and
--     defensively revoked below).
-- The connector creates the replication slot itself on first start;
-- this file only REPORTS whether the slot exists.
-- =====================================================================

-- 0. Fail fast — non-zero exit — if the password variable is missing
--    or empty. psql's \quit cannot carry an exit code (no argument
--    until psql 17's ON_ERROR_EXIT semantics change), so under
--    ON_ERROR_STOP we abort through a genuine SQL exception instead:
--    psql exits 3. Verified on PG16; do NOT "simplify" back to \quit,
--    which would let an unprepared run exit 0.
\if :{?cdc_password}
SELECT length(:'cdc_password') > 0 AS cdc_password_nonempty \gset
\if :cdc_password_nonempty
\else
\echo 'ERROR: cdc_password is empty — pass a non-empty secret value'
DO $$ BEGIN RAISE EXCEPTION 'cdc_password psql variable must be non-empty'; END $$;
\endif
\else
\echo 'ERROR: run psql with -v cdc_password=... (see file header); aborting with non-zero exit'
DO $$ BEGIN RAISE EXCEPTION 'cdc_password psql variable is required'; END $$;
\endif

-- ─── 1. Preconditions (read-only; fail loudly) ────────────────────────
DO $$
DECLARE
  v_replident "char";
  v_slots  int;
  v_senders int;
BEGIN
  SELECT c.relreplident INTO v_replident
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'outbox' AND c.relname = 'events' AND c.relkind = 'r';

  IF v_replident IS NULL THEN
    RAISE EXCEPTION 'outbox.events not found — apply the event-scheduler/scheduling migrations first (libs/event-scheduler/migrations/0002 or services/pipeline-build-service/migrations/20261117100000)';
  END IF;
  IF v_replident <> 'f' THEN
    RAISE EXCEPTION 'outbox.events must have REPLICA IDENTITY FULL (found %) — it is applied idempotently by migration 0002; do not skip it here', v_replident;
  END IF;

  IF current_setting('wal_level') <> 'logical' THEN
    RAISE EXCEPTION 'wal_level is %, must be logical (postgres-clusters pgRuntimeConfig.walLevel; CNPG applies it via rolling restart)', current_setting('wal_level');
  END IF;

  SELECT setting::int INTO v_slots   FROM pg_settings WHERE name = 'max_replication_slots';
  SELECT setting::int INTO v_senders FROM pg_settings WHERE name = 'max_wal_senders';
  RAISE NOTICE 'wal_level=logical, max_replication_slots=%, max_wal_senders=% (CNPG HA standbys already consume walsenders/slots; the connector adds 1 of each)', v_slots, v_senders;

  IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_outbox_pipeline') THEN
    RAISE NOTICE 'replication slot debezium_outbox_pipeline already exists (state=%, plugin=%)', (SELECT active FROM pg_replication_slots WHERE slot_name='debezium_outbox_pipeline'), (SELECT plugin FROM pg_replication_slots WHERE slot_name='debezium_outbox_pipeline');
  ELSE
    RAISE NOTICE 'replication slot debezium_outbox_pipeline does not exist yet — the connector creates it on first start; schedules-tick CDC gate refuses to fire until it is active';
  END IF;
END
$$;

-- ─── 2. CDC login role (idempotent; attributes re-asserted) ──────────
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_pipeline_cdc') THEN
    CREATE ROLE debezium_pipeline_cdc LOGIN REPLICATION NOINHERIT;
    RAISE NOTICE 'created role debezium_pipeline_cdc';
  ELSE
    RAISE NOTICE 'role debezium_pipeline_cdc already exists';
  END IF;
END
$$;
ALTER ROLE debezium_pipeline_cdc LOGIN REPLICATION NOINHERIT NOSUPERUSER NOBYPASSRLS;
-- Defensive: a REPLICATION+SELECT role must not be able to create
-- publications (that is exactly why the owner pre-creates one below).
REVOKE CREATE ON DATABASE app FROM debezium_pipeline_cdc;
ALTER ROLE debezium_pipeline_cdc PASSWORD :'cdc_password';

-- ─── 3. Grants: outbox.events ONLY ───────────────────────────────────
GRANT USAGE ON SCHEMA outbox TO debezium_pipeline_cdc;
GRANT SELECT ON TABLE outbox.events TO debezium_pipeline_cdc;

-- ─── 4. Publication: owner-precreated, exactly outbox.events ─────────
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'debezium_outbox_pipeline_pub') THEN
    CREATE PUBLICATION debezium_outbox_pipeline_pub FOR TABLE outbox.events;
    RAISE NOTICE 'created publication debezium_outbox_pipeline_pub FOR TABLE outbox.events';
  ELSE
    RAISE NOTICE 'publication debezium_outbox_pipeline_pub already exists';
  END IF;
END
$$;
DO $$
BEGIN
  IF (SELECT count(*) FROM pg_publication_tables
       WHERE pubname = 'debezium_outbox_pipeline_pub') <> 1
     OR NOT EXISTS (SELECT 1 FROM pg_publication_tables
                     WHERE pubname = 'debezium_outbox_pipeline_pub'
                       AND schemaname = 'outbox' AND tablename = 'events') THEN
    RAISE EXCEPTION 'publication debezium_outbox_pipeline_pub must contain EXACTLY outbox.events — fix manually (ALTER PUBLICATION ... SET TABLE outbox.events); refusing to guess';
  END IF;
END
$$;

-- ─── 5. Post-conditions (fail the whole run if anything drifted) ─────
DO $$
BEGIN
  IF NOT has_schema_privilege('debezium_pipeline_cdc', 'outbox', 'USAGE') THEN
    RAISE EXCEPTION 'post-condition failed: no USAGE on schema outbox';
  END IF;
  IF NOT has_table_privilege('debezium_pipeline_cdc', 'outbox.events', 'SELECT') THEN
    RAISE EXCEPTION 'post-condition failed: no SELECT on outbox.events';
  END IF;
  IF has_table_privilege('debezium_pipeline_cdc', 'outbox.events', 'INSERT, UPDATE, DELETE')
     OR has_database_privilege('debezium_pipeline_cdc', current_database(), 'CREATE') THEN
    RAISE EXCEPTION 'post-condition failed: role has write or CREATE privileges it must not have';
  END IF;
  RAISE NOTICE 'prep OK: role debezium_pipeline_cdc (REPLICATION, NOINHERIT) / USAGE+SELECT on outbox.events only / publication debezium_outbox_pipeline_pub = exactly outbox.events';
END
$$;
