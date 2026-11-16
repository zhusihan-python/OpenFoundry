-- P2.6 idempotent publish + commit recovery (root-decisions-v2 + v2.1).
--
-- Additions, all opt-in and backward compatible:
--
--  1. sync_runs.recovery_protocol: the admission-frozen protocol marker.
--     'p26-v1' is written ONLY when the source config carries a typed
--     sync.recovery.enabled=true at admission (v2.1: incremental NOT
--     required — snapshot runs freeze it too). Pre-P26 rows keep NULL —
--     never backfilled, never inferred; NULL forever means "old protocol"
--     and every P2.6 recovery path refuses such runs outright.
--  2. sync_runs.recovery_source_id / recovery_output_dataset_id /
--     recovery_binding_fingerprint: the v1 frozen identity triple. Snapshot
--     v1 runs freeze the snapshot-recovery binding fingerprint here;
--     incremental v1 runs mirror their cp_* values (same transaction, same
--     locks). NULL on every pre-P26 row.
--  3. sync_run_publish_intents: the durable publish intent of a p26-v1 run
--     (incremental AND snapshot). Born in the SAME ordered transaction that
--     stamps sync_runs.publish_entered_at (gate <=> intent IFF, v1 only),
--     carrying everything recovery needs to verify or replay the exact
--     publish WITHOUT re-pulling the source:
--       * the exact serialized HTTP JSON body (bounded bytea) and its
--         sha256 request fingerprint — the DVS idempotency byte contract;
--       * the frozen run baseline mirrored from the run row at gate time;
--       * separate, restart-persistent send/query attempt budgets and the
--         recovery wait state machine.
--     Authorization material (JWTs, credentials) is NEVER stored here.
--     The raw body bytes, the opaque completed checkpoint (NULL for
--     snapshot runs), the binding fingerprint and the publish key never
--     enter public JSON.

-- Routing authority only; write-once at admission, never updated.
ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS recovery_protocol TEXT;

-- v1 frozen identity triple (write-once at admission, never updated).
ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS recovery_source_id UUID,
    ADD COLUMN IF NOT EXISTS recovery_output_dataset_id UUID,
    ADD COLUMN IF NOT EXISTS recovery_binding_fingerprint TEXT;

COMMENT ON COLUMN sync_runs.recovery_protocol IS
    'P2.6 admission-frozen recovery protocol (''p26-v1''); NULL = pre-P26 row, never backfilled, recovery paths refuse it';
COMMENT ON COLUMN sync_runs.recovery_binding_fingerprint IS
    'v1 frozen binding: snapshot-recovery fingerprint (snapshot runs) or mirror of cp_binding_fingerprint (incremental runs)';

-- P26-only mutation fence support (SetCredential / effective file_glob or
-- target edits refuse while a v1 pending/running run exists).
CREATE INDEX IF NOT EXISTS idx_sync_runs_recovery_active
    ON sync_runs (sync_def_id)
    WHERE recovery_protocol IS NOT NULL AND status IN ('pending','running');

CREATE TABLE IF NOT EXISTS sync_run_publish_intents (
    run_id              UUID PRIMARY KEY REFERENCES sync_runs(id) ON DELETE CASCADE,
    sync_def_id         UUID NOT NULL,
    source_id           UUID NOT NULL,
    org_id              UUID,
    -- Mirrors the frozen run protocol at gate time ('p26-v1'); a defense
    -- check — the run row stays the routing authority.
    recovery_protocol   TEXT NOT NULL,
    -- Fixed run-scoped DVS idempotency key: 'of-p26-pub-v1:<runUUID>'.
    -- NEVER includes generation/attempt/content; target/branch/body are
    -- bound by request_fingerprint instead, so a key cannot be swapped to
    -- evade a conflict.
    publish_key         TEXT NOT NULL,
    -- sha256 hex over the EXACT serialized HTTP JSON body (key included).
    -- The DVS recomputes this itself; the Connector only has to keep the
    -- bytes identical.
    request_fingerprint TEXT NOT NULL,
    -- The exact request body bytes (single serialization, persisted before
    -- dispatch, replayed verbatim). Bounded raw bytes — the bound applies
    -- to the SERIALIZED JSON BODY, not to any decoded payload.
    publish_body        BYTEA NOT NULL,
    output_dataset_id   UUID NOT NULL,
    -- Explicit branch — the keyed DVS request rejects silent fallbacks.
    branch              TEXT NOT NULL,
    logical_path        TEXT NOT NULL,
    -- sha256 over the NDJSON payload only (P25 content semantics).
    content_hash        TEXT NOT NULL,
    rows_written        BIGINT NOT NULL CHECK (rows_written >= 0),
    bytes_written       BIGINT NOT NULL CHECK (bytes_written >= 0),
    -- Opaque completion-checkpoint candidate of this batch. NULL for
    -- snapshot runs (no data checkpoint), non-NULL for incremental runs
    -- (raw-byte rules mirror the P25 bound; never echoed into errors/logs).
    completed_checkpoint TEXT
                        CHECK (completed_checkpoint IS NULL
                               OR octet_length(completed_checkpoint) BETWEEN 1 AND 512),
    -- Frozen baseline mirrored from the run row under the gate locks
    -- (cp_mode is '' for snapshot runs; cp_base_value NULL = absent token).
    cp_mode             TEXT NOT NULL,
    cp_base_epoch       INT  NOT NULL,
    cp_base_value       TEXT,
    cp_binding_fingerprint TEXT NOT NULL,
    -- Recovery state machine (internal; the safe public projection exposes
    -- state/counts/times only):
    --   armed                  gate written, live execution owns the run
    --   waiting                deferred with reason + next_action_at
    --   send_budget_exhausted  sticky: total send budget (3) consumed
    --   query_budget_exhausted sticky: automated verification budget (5)
    --                          consumed (manual explicit queries still count
    --                          observably beyond it)
    --   finalized              committed receipt reconciled (same tx as
    --                          success + [incremental: epoch advance + owner
    --                          release])
    recovery_state      TEXT NOT NULL DEFAULT 'armed',
    last_wait_reason    TEXT,
    next_action_at      TIMESTAMPTZ,
    send_attempts       INT NOT NULL DEFAULT 0 CHECK (send_attempts >= 0),
    query_attempts      INT NOT NULL DEFAULT 0 CHECK (query_attempts >= 0),
    -- The committed DVS transaction recorded by the finalize (from the
    -- immutable receipt).
    txn_id              UUID,
    resolved_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sync_run_publish_intents_body_bound
        CHECK (octet_length(publish_body) BETWEEN 2 AND 67108864),
    CONSTRAINT sync_run_publish_intents_fingerprint_shape
        CHECK (octet_length(request_fingerprint) = 64)
);

-- One publish identity per run; the fixed run-scoped key also makes this a
-- run-uniqueness guard.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_run_publish_intents_key
    ON sync_run_publish_intents (publish_key);

-- Waiting-scan support for the (default-off) automated recovery worker.
CREATE INDEX IF NOT EXISTS idx_sync_run_publish_intents_waiting
    ON sync_run_publish_intents (next_action_at) WHERE recovery_state = 'waiting';
