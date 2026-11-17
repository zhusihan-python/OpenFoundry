-- T8 scheduling consumer (WP3.2) — see
-- docs/architecture/data-integration-t8-scheduling-decision.md §3.
--
-- This migration is owned by the main WP3.2 worker (A). It provisions:
--   1. scheduling-mode binding columns on schedules.definitions for the
--      producer-side tick (owned by worker B, which reads but never
--      migrates this database),
--   2. the standard outbox.events table (INSERT+DELETE, WAL/CDC only —
--      never polled, never a permanent dedup store),
--   3. the consumer-side scheduling.* state: per-opportunity dispatches,
--      cross-opportunity pipeline activity slots, bounded revocable
--      grants with encrypted caller tokens, keyed publish receipts,
--      and poison-message counters.
--
-- Legacy Rust-era tables (schedules, schedule_runs, schedule_scope,
-- service_principals — 20260504000080..082) are intentionally NOT
-- reused: they predate tenant isolation and stay read-only legacy.

-- ─── 1. Producer binding columns on schedules.definitions ────────────
-- dispatch_mode rules (frozen, T8 decision §3):
--   * legacy tick (unchanged behavior) processes rows where
--     dispatch_mode IS DISTINCT FROM 'pipeline_outbox_v1';
--   * new tick processes only dispatch_mode = 'pipeline_outbox_v1'.
-- The two CLIs can therefore coexist during rollover without double
-- firing a row.
ALTER TABLE schedules.definitions
    ADD COLUMN IF NOT EXISTS dispatch_mode        TEXT NULL,
    ADD COLUMN IF NOT EXISTS tenant_kind          TEXT NULL,
    ADD COLUMN IF NOT EXISTS tenant_id            UUID NULL,
    ADD COLUMN IF NOT EXISTS schedule_key         TEXT NULL,
    ADD COLUMN IF NOT EXISTS definition_revision  INT  NULL,
    ADD COLUMN IF NOT EXISTS pipeline_rid         TEXT NULL,
    ADD COLUMN IF NOT EXISTS build_branch         TEXT NULL,
    ADD COLUMN IF NOT EXISTS grant_ref            UUID NULL,
    -- Quarantine (T8 decision §3): set ONLY for PERMANENTLY broken
    -- rows (bad cron / payload failing local schema) in the same tx as
    -- enabled=false, with a sanitized reason, so the tick continues
    -- with other due rows. Transient producer-side failures (registry
    -- unreachable/auth/version not ready) keep the row due for retry
    -- and MUST NOT quarantine. A verified definition update or an
    -- explicit recovery clears both columns.
    ADD COLUMN IF NOT EXISTS quarantined_at       TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS quarantine_reason    TEXT NULL;

-- One active pipeline_outbox_v1 binding per (tenant_kind, tenant_id,
-- schedule_key): the SAME UUID under org and personal is two different
-- tenants (S1 domains, T8 decision §3).
CREATE UNIQUE INDEX IF NOT EXISTS schedules_definitions_mode_key_uidx
    ON schedules.definitions (tenant_kind, tenant_id, schedule_key)
    WHERE dispatch_mode = 'pipeline_outbox_v1';

-- New-mode rows must be fully bound. Kept as a CHECK so a half-written
-- binding cannot reach the tick.
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

-- ─── 2. Transactional outbox (standard DDL, shared with producer) ────
CREATE SCHEMA IF NOT EXISTS outbox;
CREATE TABLE IF NOT EXISTS outbox.events (
    event_id    UUID        PRIMARY KEY,
    aggregate   TEXT        NOT NULL,
    aggregate_id TEXT       NOT NULL,
    payload     JSONB       NOT NULL,
    headers     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    topic       TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Row is DELETEd in the same transaction as INSERT (libs/outbox.Enqueue);
-- no index beyond the PK, no polling, no permanent retention. The
-- PUBLISH signal is the WAL INSERT (Debezium captures the insert; the
-- EventRouter filters out the follow-up delete). FULL replica identity
-- is kept so the filtered delete event still carries complete rows
-- instead of a PK-only tombstone. Mirrors libs/outbox migration 0002.
ALTER TABLE outbox.events REPLICA IDENTITY FULL;

-- ─── 3. Consumer-side scheduling state ───────────────────────────────
CREATE SCHEMA IF NOT EXISTS scheduling;

-- grants: bounded, revocable scheduled-execution authorization.
-- Tenant domains (S1 compatibility): tenant_kind distinguishes org
-- (tenant_id = claims OrgID, org_id = same) from personal (tenant_id =
-- claims Sub, org_id NULL). The same UUID under different kinds is two
-- different tenants and never shares a grant. generation is an
-- EXACT-match requirement: the consumer rejects every queued/replayed
-- event whose grant_generation differs from the row's current
-- generation. Re-granting bumps the generation and invalidates
-- everything still in flight. token_ciphertext is AES-256-GCM under
-- the dedicated SCHEDULER_CREDENTIAL_ENCRYPTION_KEY (never derived
-- from the JWT secret; errors sanitized).
CREATE TABLE IF NOT EXISTS scheduling.grants (
    id              UUID PRIMARY KEY,
    tenant_kind     TEXT NOT NULL CHECK (tenant_kind IN ('org','personal')),
    tenant_id       UUID NOT NULL,
    org_id          UUID NULL,                -- org domain: == tenant_id; personal: NULL
    subject         UUID NOT NULL,            -- JWT sub; zero-sub JWTs rejected at ingest
    schedule_key    TEXT NOT NULL,
    pipeline_rid    TEXT NOT NULL,
    generation      INT  NOT NULL CHECK (generation >= 1),
    state           TEXT NOT NULL CHECK (state IN ('active','suspended','revoked','expired')),
    expires_at      TIMESTAMPTZ NOT NULL,     -- == stored JWT exp; exp-less JWTs are rejected at ingest
    token_ciphertext BYTEA NOT NULL,
    created_by      UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (tenant_kind <> 'org' OR (org_id IS NOT NULL AND org_id = tenant_id)),
    CHECK (tenant_kind <> 'personal' OR (org_id IS NULL AND tenant_id = subject))
);

-- At most one active grant per (tenant_kind, tenant_id, schedule_key).
CREATE UNIQUE INDEX IF NOT EXISTS scheduling_grants_active_uidx
    ON scheduling.grants (tenant_kind, tenant_id, schedule_key)
    WHERE state = 'active';

CREATE INDEX IF NOT EXISTS scheduling_grants_pipeline_idx
    ON scheduling.grants (tenant_kind, tenant_id, pipeline_rid);

-- dispatches: the per-opportunity admission row. event_id (== the
-- deterministic trigger event id) is the once-per-opportunity fence:
-- ON CONFLICT DO NOTHING + state routing implements at-least-once
-- redelivery dedup WITHOUT a record-before-process idempotency store.
-- attempts persists event-handling failures ACROSS restarts; >=5 (the
-- SCHEDULER_MAX_EVENT_ATTEMPTS default) stops retrying and DLQs.
CREATE TABLE IF NOT EXISTS scheduling.dispatches (
    event_id            UUID PRIMARY KEY,
    tenant_kind         TEXT NOT NULL CHECK (tenant_kind IN ('org','personal')),
    tenant_id           UUID NOT NULL,
    schedule_key        TEXT NOT NULL,
    definition_revision INT  NOT NULL CHECK (definition_revision >= 1),
    scheduled_for       TIMESTAMPTZ NOT NULL,
    pipeline_rid        TEXT NOT NULL,
    build_branch        TEXT NOT NULL,
    grant_ref           UUID NOT NULL,
    grant_generation    INT  NOT NULL,
    build_id            UUID NOT NULL UNIQUE,   -- DeriveBuildID(trigger event id)
    -- Frozen expectation of the resolved plan: [{"node_id":..,"dataset_rid":..},..].
    -- Written at admission (M3 fills it from the resolver). NULL means
    -- NOT YET SEALED: a succeeded finish is impossible until the sets are
    -- sealed; '[]' is a sealed decision that there are no outputs. The
    -- completeness check requires a committed receipt bound to this
    -- dispatch for EVERY expected (node, dataset) pair — universally
    -- quantified, not counted.
    expected_outputs    JSONB NULL,
    -- Frozen expected node ids of the resolved plan (M3 seals them from
    -- the resolver; the consumer cross-checks executor outcomes against
    -- this set). NULL = not sealed => success impossible.
    expected_nodes      JSONB NULL,
    state               TEXT NOT NULL CHECK (state IN (
                            'admitted','executing','succeeded','failed',
                            'skipped','blocked','unknown_publish','reconcile')),
    attempts            INT  NOT NULL DEFAULT 0,
    lease_owner         TEXT NULL,
    fence_token         BIGINT NOT NULL DEFAULT 0,
    lease_expires_at    TIMESTAMPTZ NULL,
    error               TEXT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scheduling_dispatches_pipeline_idx
    ON scheduling.dispatches (tenant_kind, tenant_id, pipeline_rid, scheduled_for);
CREATE INDEX IF NOT EXISTS scheduling_dispatches_state_lease_idx
    ON scheduling.dispatches (state, lease_expires_at)
    WHERE state IN ('admitted','executing');

-- pipeline_activity: cross-opportunity non-overlap. TAKEOVER RULES are
-- frozen (T8 decision §3): a live row with a non-terminal active
-- dispatch blocks every NEW opportunity (dispatch -> 'blocked'); an
-- expired lease only lets the ORIGINAL dispatch's recovery flow resume
-- (fence-checked), never a new opportunity; rows with
-- unknown_publish/reconcile/unconfirmed remote effects MUST keep the
-- slot; the row is deleted only when the active dispatch is terminal
-- AND external publishes are confirmed (or safely aborted).
CREATE TABLE IF NOT EXISTS scheduling.pipeline_activity (
    tenant_kind        TEXT NOT NULL CHECK (tenant_kind IN ('org','personal')),
    tenant_id          UUID NOT NULL,
    pipeline_rid       TEXT NOT NULL,
    active_dispatch_id UUID NOT NULL,
    fence_token        BIGINT NOT NULL,
    heartbeat_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    blocked_reason     TEXT NULL,
    PRIMARY KEY (tenant_kind, tenant_id, pipeline_rid)
);

-- publish_receipts: keyed-commit ledger for scheduled output publishes.
-- The pending row (original request bytes + fingerprint) is persisted
-- BEFORE the first POST; a DVS 404 on the key does NOT prove no
-- in-flight operation — only same-key byte-identical replay or
-- reconciliation is allowed, never a new key.
CREATE TABLE IF NOT EXISTS scheduling.publish_receipts (
    key                TEXT PRIMARY KEY,        -- DerivePublishKey: "sch:"+UUIDv5
    dispatch_event_id  UUID NOT NULL,
    node_id            TEXT NOT NULL,
    dataset_rid        TEXT NOT NULL,
    request_body       BYTEA NOT NULL,
    request_sha256     TEXT NOT NULL,
    state              TEXT NOT NULL CHECK (state IN ('pending','committed','unknown')),
    receipt            JSONB NULL,              -- usableACK payload (Status COMMITTED + DatasetID match)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scheduling_publish_receipts_dispatch_idx
    ON scheduling.publish_receipts (dispatch_event_id);

-- Unique binding: one receipt per (dispatch, node, dataset) — duplicate
-- committed receipts cannot double-cover a partial expectation (the
-- completeness check is universal anyway; this makes the invariant
-- structural).
CREATE UNIQUE INDEX IF NOT EXISTS scheduling_publish_receipts_pair_uidx
    ON scheduling.publish_receipts (dispatch_event_id, node_id, dataset_rid);

-- poison_messages: stable per-(topic,partition,offset) identity for
-- undecodable/forged events (bad JSON, mismatched event_id). Counted
-- across restarts; at >=5 attempts the message is DLQ'd via an outbox
-- intent, and the Kafka offset is committed only after that intent is
-- committed.
CREATE TABLE IF NOT EXISTS scheduling.poison_messages (
    topic             TEXT NOT NULL,
    partition_id      INT  NOT NULL,
    partition_offset  BIGINT NOT NULL,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts          INT NOT NULL DEFAULT 0,
    last_error        TEXT NULL,
    state             TEXT NOT NULL CHECK (state IN ('counting','dlq_enqueued','dlq_published')),
    -- Stable dead-letter event identity: derived from (topic,partition,
    -- offset) so a crash between the DLQ outbox commit and the Kafka
    -- ACK — or a restart re-handling the same offset — can never mint a
    -- second logical DLQ event (the outbox row is INSERT+DELETE'd in
    -- one tx; its PK is NOT a permanent dedup store).
    dlq_event_id      UUID NULL,
    PRIMARY KEY (topic, partition_id, partition_offset)
);

-- dispatch_audit: the permanent per-opportunity record written by the
-- producer's tick transaction (worker B writes; consumer never does).
-- The outbox row is deleted in the same transaction, so this table —
-- not outbox.events — is the audit source of what fired.
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
