#!/bin/bash
# Owner-run, repeatable bootstrap for the checkpoint audit relay (PG16).
set -euo pipefail

if [[ -z "${CHECKPOINT_AUDIT_CDC_PASSWORD:-}" ]]; then
    echo "Checkpoint audit CDC is disabled (no password supplied)."
    exit 0
fi

psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${CHECKPOINT_AUDIT_DBNAME:-openfoundry_auth_service}" <<'SQL'
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
SELECT format('GRANT CONNECT ON DATABASE %I TO checkpoint_audit_cdc', current_database())\gexec
GRANT USAGE ON SCHEMA outbox TO checkpoint_audit_cdc;
GRANT SELECT ON outbox.events TO checkpoint_audit_cdc;

-- Accept the initial topic-only definition for an owner-run upgrade. Preserve
-- the publication identity and slot while narrowing the relay to this producer.
DO $$
DECLARE
    current_filter text;
    checkpoint_filter CONSTANT text := $filter$((topic = 'audit.events.v1'::text) AND ((payload ->> 'kind'::text) = 'purpose_checkpoint.evaluated'::text) AND ((payload ->> 'source_service'::text) = 'authorization-policy-service'::text))$filter$;
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'of_checkpoint_audit_pub') THEN
        CREATE PUBLICATION of_checkpoint_audit_pub WITH (publish = 'insert');
    ELSE
        SELECT t.rowfilter INTO current_filter
        FROM pg_publication p JOIN pg_publication_tables t USING (pubname)
        JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prrelid = 'outbox.events'::regclass
        WHERE p.pubname = 'of_checkpoint_audit_pub' AND NOT p.puballtables AND p.pubinsert
          AND NOT (p.pubdelete OR p.pubupdate OR p.pubtruncate)
          AND (SELECT count(*) FROM pg_publication_tables WHERE pubname = p.pubname) = 1
          AND t.schemaname = 'outbox' AND t.tablename = 'events' AND r.prattrs IS NULL;
        IF current_filter = checkpoint_filter THEN
            RETURN;
        ELSIF current_filter IS DISTINCT FROM '(topic = ''audit.events.v1''::text)' THEN
            RAISE EXCEPTION 'Existing of_checkpoint_audit_pub has unexpected settings; inspect it before starting CDC';
        END IF;
    END IF;
    ALTER PUBLICATION of_checkpoint_audit_pub SET TABLE outbox.events WHERE
        (topic = 'audit.events.v1' AND payload ->> 'kind' = 'purpose_checkpoint.evaluated'
         AND payload ->> 'source_service' = 'authorization-policy-service');
END $$;
COMMIT;
SQL
