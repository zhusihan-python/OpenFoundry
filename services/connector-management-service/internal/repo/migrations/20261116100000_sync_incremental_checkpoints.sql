-- P2.5 per-def incremental completion checkpoint + durable single-flight owner
-- (design-draft v2.1 / implementation-contract v1.1).
--
-- Additive only; replay-safe under the whole-batch advisory-lock replay (the
-- service replays every embedded migration on each start; there is no ledger).
-- No backfill/adoption for any pre-P25 row: the legacy controlled cohort
-- (old4 pins + P24 postboot B/C) is untouched, and no existing run ever gains
-- cp_* values — pre-P25 rows keep cp_mode NULL and stay non-incremental.
--
-- Lock order (global partial order): connections(Source) ->
-- batch_sync_defs(Def) -> sync_def_checkpoints(Checkpoint) -> sync_runs(Run).
-- Checkpoint is always a leaf: protected writers (UpdateConnection/
-- UpdateConnectionConfig/SetCredential/delete) hold the Source lock and the
-- Def locks (sorted by def id) BEFORE touching connections/source_credentials
-- rows, so the Def->Checkpoint->Run finalize chain can reread the binding
-- without a reverse Source lock.

CREATE TABLE IF NOT EXISTS sync_def_checkpoints (
    sync_def_id         UUID PRIMARY KEY REFERENCES batch_sync_defs(id) ON DELETE CASCADE,
    source_id           UUID NOT NULL,
    output_dataset_id   UUID NOT NULL,
    mode                TEXT NOT NULL DEFAULT 'cursor',
    -- sha256 hex over the canonical binding JSON (models.IncrementalBinding).
    -- Never a cursor value; never plaintext auth — only the final hash.
    binding_fingerprint TEXT NOT NULL,
    -- Confirmed completion checkpoint (opaque provider token). NULL = never
    -- confirmed (first run reads from the stream start / initial_value).
    -- The bound is RAW BYTES (octet_length), mirroring the application rule.
    checkpoint_value    TEXT CHECK (checkpoint_value IS NULL
                                     OR octet_length(checkpoint_value) BETWEEN 1 AND 512),
    -- Monotonic confirmed-batch counter. The ONLY ordering authority: opaque
    -- tokens are never compared.
    confirmed_epoch     INT NOT NULL DEFAULT 0,
    -- Durable single-flight owner. NULL = free. Set only AFTER the owned run
    -- row exists (FK), cleared only by the contract's terminal transitions.
    active_run_id       UUID REFERENCES sync_runs(id),
    active_since        TIMESTAMPTZ,
    -- Forensics of the last confirmed batch (never secrets).
    advanced_by_run     UUID,
    advanced_at         TIMESTAMPTZ,
    last_batch_rows     BIGINT,
    last_batch_bytes    BIGINT,
    last_batch_hash     TEXT,
    last_batch_txn      UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Anti-join support for the takeover guard (a checkpoint-owned run is never
-- takeover-eligible) and for unresolved-owner queries. Partial: free rows
-- (NULL owner) are not indexed.
CREATE INDEX IF NOT EXISTS idx_sync_def_checkpoints_owner
    ON sync_def_checkpoints (active_run_id) WHERE active_run_id IS NOT NULL;

-- Admission-frozen immutable baseline on the run row. Internal only: never in
-- runPublicColumns or any public projection; the executor reads them via the
-- lease-claim internal scan. cp_base_value reuses NULL/non-NULL to express
-- input-token absence (the application layer owns the raw-byte rules).
ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS cp_mode                TEXT,
    ADD COLUMN IF NOT EXISTS cp_base_epoch          INT,
    ADD COLUMN IF NOT EXISTS cp_base_value          TEXT,
    ADD COLUMN IF NOT EXISTS cp_binding_fingerprint TEXT,
    ADD COLUMN IF NOT EXISTS cp_output_dataset_id   UUID;
