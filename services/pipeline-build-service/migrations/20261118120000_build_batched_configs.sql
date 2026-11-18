-- WP3.3 increment 3B — batched build execution bookkeeping.
--
-- One row per opt-in batched build (single JobSpec / single input /
-- single output, logic_payload.transform_type == "batched_dataset_copy").
-- The row is inserted in the SAME transaction that opens the builds row,
-- with stage='PREPARING', so execute/abort can route on it (and answer
-- 409 while resolution is still in flight) instead of falling back to
-- the legacy empty-plan path. The configuration columns are frozen at
-- CreateBuild time and never updated; only the execution columns are
-- mutable, guarded by the lease fence. No JWT/credential material is
-- stored here — ActorSubject is the verified JWT subject UUID only.
--
-- Human-review item (additive migration): see increment-3b-plan §7.

CREATE TABLE IF NOT EXISTS build_batched_configs (
    build_id              UUID PRIMARY KEY REFERENCES builds(id) ON DELETE CASCADE,

    -- Frozen configuration (written once at CreateBuild).
    transform_type        TEXT NOT NULL,
    input_dataset_rid     TEXT NOT NULL,
    input_branch          TEXT NOT NULL,
    input_version         INT  NOT NULL CHECK (input_version > 0),
    input_path            TEXT NOT NULL DEFAULT '',
    all_files             BOOLEAN NOT NULL DEFAULT FALSE,
    predicate             TEXT NOT NULL DEFAULT '',
    output_dataset_rid    TEXT NOT NULL,
    output_branch         TEXT NOT NULL,
    logical_path          TEXT NOT NULL,
    job_spec_rid          TEXT NOT NULL,
    job_spec_content_hash TEXT NOT NULL DEFAULT '',
    actor_subject         UUID NOT NULL,
    reservation_rid       TEXT NOT NULL DEFAULT '',

    -- Execution state machine: PREPARING → PENDING → STAGING → STAGED →
    -- PUBLISHING → OUTCOME_UNKNOWN → DONE, with FAILED/ABORTED reachable
    -- only before PUBLISHING (once the commit intent is persisted, abort
    -- is always refused and every unresolved outcome stays publish-
    -- pending for exact-byte replay).
    stage                 TEXT NOT NULL CHECK (stage IN (
                              'PREPARING','PENDING','STAGING','STAGED',
                              'PUBLISHING','OUTCOME_UNKNOWN','DONE','FAILED','ABORTED'
                          )),
    lease_owner           UUID NULL,
    lease_expires_at      TIMESTAMPTZ NULL,
    abort_requested       BOOLEAN NOT NULL DEFAULT FALSE,

    staged_object_id      TEXT NOT NULL DEFAULT '',
    staged_size_bytes     BIGINT NOT NULL DEFAULT 0,
    staged_sha256         TEXT NOT NULL DEFAULT '',
    staged_records        BIGINT NOT NULL DEFAULT 0,

    commit_idempotency_key TEXT NOT NULL DEFAULT '',
    commit_request_body    BYTEA NULL,
    commit_response_body   BYTEA NULL,
    published_transaction_rid TEXT NOT NULL DEFAULT '',
    error_message          TEXT NOT NULL DEFAULT '',

    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Cancel/decision scans and abort retries look builds up by state.
CREATE INDEX IF NOT EXISTS idx_build_batched_configs_stage
    ON build_batched_configs (stage, updated_at);
