-- P2.4 observable scheduled scheduling (design-draft v2 + implementation-contract).
--
-- Scope: batch_sync_defs (REST snapshot batch syncs) only. Media-set syncs stay
-- dormant. This is the root-approved, explicitly limited business-platform
-- exception to ADR-0025 (see Connector README): a DB-claimed scheduler for this
-- one surface, not a platform scheduler and not a Temporal migration.
--
-- Compatibility rules (root review contract):
--   * Legacy rows keep their schedule_cron verbatim and migrate DISABLED with a
--     NULL cursor — no backfill, no implicit activation, no UPDATE statements.
--   * schedule_revision starts at 1; every actual change to cron/time zone/
--     enabled/output target/file_glob bumps it and recomputes a future cursor
--     from the locked DB time (disable sets the cursor back to NULL).
--   * sync_runs is NOT touched: scheduled provenance lives in
--     sync_schedule_ticks.run_id plus sync_runs.requested_by.

ALTER TABLE batch_sync_defs
    ADD COLUMN IF NOT EXISTS schedule_time_zone       TEXT        NOT NULL DEFAULT 'UTC',
    ADD COLUMN IF NOT EXISTS schedule_enabled         BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS schedule_revision        INT         NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS schedule_next_fire_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS schedule_last_fire_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS schedule_last_tick_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS schedule_last_tick_outcome TEXT;

-- Scheduler hot path: candidates are scanned by the due cursor (keyset
-- round-robin); mirrors libs/event-scheduler's partial-index shape.
CREATE INDEX IF NOT EXISTS idx_batch_sync_defs_schedule_due
    ON batch_sync_defs (schedule_next_fire_at)
    WHERE schedule_enabled;

-- Tick history: append-only, pruned to the most recent 500 rows per def inside
-- the claim transaction. The slot-unique constraint is the hard single-fire
-- guarantee per (def, revision, scheduled_for); duplicate handling in code
-- matches THIS constraint only. detail is bounded and must never carry
-- credentials, DSNs or URL query strings.
CREATE TABLE IF NOT EXISTS sync_schedule_ticks (
    id                UUID PRIMARY KEY,
    sync_def_id       UUID NOT NULL REFERENCES batch_sync_defs(id) ON DELETE CASCADE,
    schedule_revision INT  NOT NULL,
    -- The overdue window that was claimed (UTC storage of the cron slot).
    scheduled_for     TIMESTAMPTZ NOT NULL,
    -- Post-lock database clock at claim time (the authoritative time anchor).
    claimed_at        TIMESTAMPTZ NOT NULL,
    outcome           TEXT NOT NULL,
    run_id            UUID REFERENCES sync_runs(id),
    actor             UUID,
    reason            TEXT,
    detail            TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sync_schedule_ticks_slot_uniq
        UNIQUE (sync_def_id, schedule_revision, scheduled_for),
    CONSTRAINT sync_schedule_ticks_detail_bound
        CHECK (detail IS NULL OR char_length(detail) <= 256)
);

CREATE INDEX IF NOT EXISTS idx_sync_schedule_ticks_def_time
    ON sync_schedule_ticks (sync_def_id, created_at DESC);

-- NONSECRET cross-instance arm adjudication. Every successful authenticated
-- arm writes a fresh random grant_id (epoch); boot_id is the per-process
-- schedule boot UUID (independent of the private lease-holder identity).
-- expires_at is min(verified JWT exp, armed_at + TTL) and is written ONLY by
-- arm — heartbeats touch last_seen_at alone and never revive or extend a
-- grant. No Authorization/JWT material ever lives in this table.
CREATE TABLE IF NOT EXISTS sync_schedule_grants (
    sync_def_id       UUID PRIMARY KEY REFERENCES batch_sync_defs(id) ON DELETE CASCADE,
    grant_id          UUID NOT NULL,
    schedule_revision INT  NOT NULL,
    boot_id           UUID NOT NULL,
    actor_subject     UUID NOT NULL,
    -- Trusted tenant snapshot captured at arm time (NULL org = personal domain).
    org_id            UUID,
    owner_subject     UUID NOT NULL,
    source_id         UUID NOT NULL,
    output_dataset_id UUID NOT NULL,
    armed_at          TIMESTAMPTZ NOT NULL,
    expires_at        TIMESTAMPTZ NOT NULL,
    last_seen_at      TIMESTAMPTZ NOT NULL
);
