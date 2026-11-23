-- Only the evaluated decision path assigns this subject. Rows written by
-- the legacy manual-record API retain NULL, including historical rows.
ALTER TABLE purpose_records ADD COLUMN IF NOT EXISTS enforced_by UUID;

INSERT INTO permissions (resource, action, description)
VALUES ('purpose-checkpoint', 'enforce', 'Evaluate a purpose checkpoint for an authenticated runtime actor'),
       ('purpose-checkpoint', 'read', 'Read enforced purpose checkpoint records')
ON CONFLICT DO NOTHING;

-- pg-policy provisions and owns this shared table. A standalone database
-- initializes the same ADR-0022 layout when the table is absent.
DO $$
BEGIN
    IF to_regclass('outbox.events') IS NULL THEN
        IF to_regnamespace('outbox') IS NULL THEN
            CREATE SCHEMA outbox;
        END IF;
        CREATE TABLE outbox.events (
            event_id UUID PRIMARY KEY,
            aggregate TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            payload JSONB NOT NULL,
            headers JSONB NOT NULL DEFAULT '{}'::jsonb,
            topic TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        ALTER TABLE outbox.events REPLICA IDENTITY FULL;
    ELSIF (SELECT relreplident FROM pg_catalog.pg_class WHERE oid = 'outbox.events'::regclass) <> 'f' THEN
        RAISE EXCEPTION 'The outbox owner must set REPLICA IDENTITY FULL on outbox.events';
    END IF;
END $$;
