-- Only the evaluated decision path assigns this subject. Rows written by
-- the legacy manual-record API retain NULL, including historical rows.
ALTER TABLE purpose_records ADD COLUMN IF NOT EXISTS enforced_by UUID;

INSERT INTO permissions (resource, action, description)
VALUES ('purpose-checkpoint', 'enforce', 'Evaluate a purpose checkpoint for an authenticated runtime actor'),
       ('purpose-checkpoint', 'read', 'Read enforced purpose checkpoint records')
ON CONFLICT DO NOTHING;

-- Transactional audit events use the shared outbox wire format (ADR-0022).
CREATE SCHEMA IF NOT EXISTS outbox;
CREATE TABLE IF NOT EXISTS outbox.events (
    event_id UUID PRIMARY KEY,
    aggregate TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    topic TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE outbox.events REPLICA IDENTITY FULL;
