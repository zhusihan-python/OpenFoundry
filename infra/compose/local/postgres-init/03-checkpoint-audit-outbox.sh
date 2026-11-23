#!/bin/bash
# Owner-run, repeatable bootstrap for the Compose checkpoint audit relay (PG16).
set -euo pipefail

if [[ -z "${CHECKPOINT_AUDIT_CDC_PASSWORD:-}" ]]; then
    echo "Checkpoint audit CDC is disabled (no password supplied)."
    exit 0
fi

psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname openfoundry_auth_service <<'SQL'
BEGIN;
DO $$
BEGIN
    IF current_setting('wal_level') <> 'logical' THEN
        RAISE EXCEPTION 'Checkpoint audit CDC requires wal_level=logical; restart PostgreSQL first';
    END IF;
END $$;
-- Use the same standalone table layout as policy-service migration 0008.
CREATE SCHEMA IF NOT EXISTS outbox;
CREATE TABLE IF NOT EXISTS outbox.events (
    event_id uuid PRIMARY KEY,
    aggregate text NOT NULL,
    aggregate_id text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    topic text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
SELECT 'ALTER TABLE outbox.events REPLICA IDENTITY FULL'
FROM pg_class WHERE oid = 'outbox.events'::regclass AND relreplident <> 'f'\gexec

\getenv cdc_password CHECKPOINT_AUDIT_CDC_PASSWORD
SELECT format('CREATE ROLE checkpoint_audit_cdc WITH LOGIN REPLICATION PASSWORD %L', :'cdc_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'checkpoint_audit_cdc')\gexec
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles r WHERE r.rolname = 'checkpoint_audit_cdc' AND r.rolcanlogin
          AND r.rolreplication AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls)
          AND NOT EXISTS (SELECT FROM pg_auth_members WHERE member = r.oid)
    ) THEN
        RAISE EXCEPTION 'Existing checkpoint_audit_cdc role has unexpected privileges';
    END IF;
END $$;
GRANT CONNECT ON DATABASE openfoundry_auth_service TO checkpoint_audit_cdc;
GRANT USAGE ON SCHEMA outbox TO checkpoint_audit_cdc;
GRANT SELECT ON outbox.events TO checkpoint_audit_cdc;

-- INSERT + DELETE in one application transaction emits only the audit INSERT.
-- PostgreSQL's row filter limits which outbox topics reach this connector.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'of_checkpoint_audit_pub') THEN
        CREATE PUBLICATION of_checkpoint_audit_pub FOR TABLE outbox.events
            WHERE (topic = 'audit.events.v1') WITH (publish = 'insert');
    ELSIF NOT EXISTS (
        SELECT FROM pg_publication p
        WHERE p.pubname = 'of_checkpoint_audit_pub'
          AND NOT p.puballtables AND p.pubinsert AND NOT (p.pubdelete OR p.pubupdate OR p.pubtruncate)
          AND (SELECT count(*) FROM pg_publication_tables t WHERE t.pubname = p.pubname) = 1
          AND EXISTS (SELECT FROM pg_publication_tables t WHERE t.pubname = p.pubname
                      AND t.schemaname = 'outbox' AND t.tablename = 'events'
                      AND t.rowfilter = '(topic = ''audit.events.v1''::text)')
    ) THEN
        RAISE EXCEPTION 'Existing of_checkpoint_audit_pub has unexpected settings; inspect it before starting CDC';
    END IF;
END $$;
COMMIT;
SQL
