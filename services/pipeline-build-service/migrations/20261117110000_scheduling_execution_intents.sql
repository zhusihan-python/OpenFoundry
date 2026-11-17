-- Scheduling execution intents + persisted node results (WP3.2 A2).
-- A2-owned migration: does NOT touch the frozen 20261117100000 objects.
--
-- execution_intents marks the pre-RPC resolve window: the runner durably
-- records "about to open remote output transactions" BEFORE calling the
-- resolver. A dispatch found with no jobs/pending records but WITH this
-- marker may have an OPEN remote transaction whose ACK or local record
-- was lost (no RID anywhere) — recovery must PARK, never re-open and
-- never guess (T8 decision §4, A2 checkpoint §1/§2-1).
--
-- node_run_results persists the fact that a node's COMPUTATION succeeded
-- (runner returned a result) at publish time. When a keyed commit's ACK
-- is lost the executor marks the node FAILED; after same-key
-- reconciliation confirms the receipt, this persisted evidence is what
-- legitimately flips the job to COMPLETED — a real logic failure has no
-- row here and can never be flipped (A2 checkpoint §2-2 / root trap 2).
-- Metadata is stored WITHOUT bulk row payloads (data_rows/sample_rows
-- stripped by the writer); the content hash is the computation evidence.

CREATE TABLE IF NOT EXISTS scheduling.execution_intents (
    event_id   UUID PRIMARY KEY REFERENCES scheduling.dispatches(event_id) ON DELETE CASCADE,
    kind       TEXT NOT NULL CHECK (kind IN ('resolve_started')),
    details    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheduling.node_run_results (
    dispatch_event_id  UUID NOT NULL REFERENCES scheduling.dispatches(event_id) ON DELETE CASCADE,
    node_id            TEXT NOT NULL,
    output_content_hash TEXT NOT NULL DEFAULT '',
    result_metadata    JSONB NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dispatch_event_id, node_id)
);
