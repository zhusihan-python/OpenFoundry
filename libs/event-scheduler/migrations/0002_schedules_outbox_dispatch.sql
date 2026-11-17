-- T8 scheduling: outbox dispatch mode (T8 decision §1/§3).
--
-- OWNERSHIP NOTE: services get ALL of these objects from the
-- pipeline-build-service migration
-- 20261117100000_scheduling_consumer.sql (+ the quarantine columns the
-- architecture owner adds to it per the 2026-11-17 decision). This
-- file exists for STANDALONE libs/event-scheduler databases and is
-- idempotent with the service migration (IF NOT EXISTS everywhere;
-- same names). Verified deltas against the service migration as of
-- 2026-11-17 (none of them conflict when both files are applied):
--   * CHECK forms below already match the service migration's FIXED
--     shapes (tenant_kind IS NOT NULL AND IN (...); org grants
--     require org_id IS NOT NULL AND org_id = tenant_id) — do not
--     "simplify" them back, NULL slips through bare IN() checks.
--   * `ALTER TABLE outbox.events REPLICA IDENTITY FULL` — canonical
--     everywhere else (infra/compose init scripts, pg-policy
--     bootstrap-sql, libs/outbox docs); the service migration
--     currently omits it, this file applies it idempotently.
--
-- Producer contract (worker B reads, never migrates the service DB):
--   * schedules.definitions binding columns + quarantine columns
--   * outbox.events               (shared with consumer; WAL/CDC only)
--   * scheduling.grants           (producer JOINs generation metadata;
--                                  token_ciphertext is NEVER selected)
--   * scheduling.dispatch_audit   (producer's permanent per-opportunity
--                                  record written in transaction ①)

-- ─── 1. Binding + quarantine columns on schedules.definitions ──────
ALTER TABLE schedules.definitions
    ADD COLUMN IF NOT EXISTS dispatch_mode        TEXT NULL,
    ADD COLUMN IF NOT EXISTS tenant_kind          TEXT NULL,
    ADD COLUMN IF NOT EXISTS tenant_id            UUID NULL,
    ADD COLUMN IF NOT EXISTS schedule_key         TEXT NULL,
    ADD COLUMN IF NOT EXISTS definition_revision  INT  NULL,
    ADD COLUMN IF NOT EXISTS pipeline_rid         TEXT NULL,
    ADD COLUMN IF NOT EXISTS build_branch         TEXT NULL,
    ADD COLUMN IF NOT EXISTS grant_ref            UUID NULL,
    -- quarantine (root decision 2026-11-17): a permanently bad
    -- definition is disabled + stamped IN the claiming transaction;
    -- cleared only by a verified definition update / explicit restore.
    ADD COLUMN IF NOT EXISTS quarantined_at       TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS quarantine_reason    TEXT        NULL;

CREATE UNIQUE INDEX IF NOT EXISTS schedules_definitions_mode_key_uidx
    ON schedules.definitions (tenant_kind, tenant_id, schedule_key)
    WHERE dispatch_mode = 'pipeline_outbox_v1';

ALTER TABLE schedules.definitions
    DROP CONSTRAINT IF EXISTS schedules_definitions_outbox_binding_complete,
    ADD CONSTRAINT schedules_definitions_outbox_binding_complete CHECK (
        dispatch_mode IS DISTINCT FROM 'pipeline_outbox_v1'
        OR (
            tenant_kind IS NOT NULL AND tenant_kind IN ('org','personal')
            AND tenant_id IS NOT NULL
            AND schedule_key IS NOT NULL
            AND definition_revision IS NOT NULL AND definition_revision >= 1
            AND pipeline_rid IS NOT NULL
            AND build_branch IS NOT NULL
            AND grant_ref IS NOT NULL
        )
    );

-- Hot path for the outbox tick: due scheduling-mode rows that are not
-- quarantined (mirrors the claim query in outbox_scheduler.go).
CREATE INDEX IF NOT EXISTS schedules_definitions_outbox_due_idx
    ON schedules.definitions (next_run_at)
    WHERE enabled
      AND dispatch_mode = 'pipeline_outbox_v1'
      AND quarantined_at IS NULL;

-- ─── 2. Transactional outbox (shared with the consumer) ────────────
CREATE SCHEMA IF NOT EXISTS outbox;
CREATE TABLE IF NOT EXISTS outbox.events (
    event_id     UUID        PRIMARY KEY,
    aggregate    TEXT        NOT NULL,
    aggregate_id TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    topic        TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Row is DELETEd in the same transaction as INSERT (libs/outbox.Enqueue);
-- no polling, no permanent retention, never a dedup store. REPLICA
-- IDENTITY FULL matches the canonical outbox shape everywhere else in
-- the repo (pg-policy bootstrap-sql, compose init, libs/outbox docs).
ALTER TABLE outbox.events REPLICA IDENTITY FULL;

-- ─── 3. Producer-side scheduling state ─────────────────────────────
CREATE SCHEMA IF NOT EXISTS scheduling;

-- Grants (column shapes identical to the service migration; the
-- producer only JOINs id -> generation and never selects
-- token_ciphertext).
CREATE TABLE IF NOT EXISTS scheduling.grants (
    id              UUID PRIMARY KEY,
    tenant_kind     TEXT NOT NULL CHECK (tenant_kind IN ('org','personal')),
    tenant_id       UUID NOT NULL,
    org_id          UUID NULL,
    subject         UUID NOT NULL,
    schedule_key    TEXT NOT NULL,
    pipeline_rid    TEXT NOT NULL,
    generation      INT  NOT NULL CHECK (generation >= 1),
    state           TEXT NOT NULL CHECK (state IN ('active','suspended','revoked','expired')),
    expires_at      TIMESTAMPTZ NOT NULL,
    token_ciphertext BYTEA NOT NULL,
    created_by      UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (tenant_kind <> 'org' OR (org_id IS NOT NULL AND org_id = tenant_id)),
    CHECK (tenant_kind <> 'personal' OR (org_id IS NULL AND tenant_id = subject))
);

CREATE UNIQUE INDEX IF NOT EXISTS scheduling_grants_active_uidx
    ON scheduling.grants (tenant_kind, tenant_id, schedule_key)
    WHERE state = 'active';

-- Permanent per-opportunity audit record written by the producer's
-- tick transaction (worker B writes; the consumer never does). The
-- outbox row is deleted in the same transaction, so THIS table — not
-- outbox.events — is the durable proof of what fired.
CREATE TABLE IF NOT EXISTS scheduling.dispatch_audit (
    event_id            UUID PRIMARY KEY,
    definition_id       UUID NOT NULL,
    tenant_kind         TEXT NOT NULL CHECK (tenant_kind IN ('org','personal')),
    tenant_id           UUID NOT NULL,
    schedule_key        TEXT NOT NULL,
    definition_revision INT  NOT NULL,
    scheduled_for       TIMESTAMPTZ NOT NULL,
    fired_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scheduling_dispatch_audit_definition_idx
    ON scheduling.dispatch_audit (definition_id, scheduled_for DESC);
