-- P2.3 per-source persistent sync admission.
--
-- connections.sync_last_admitted_at records the database clock of the most
-- recent SUCCESSFUL admission of a sync run for the source (written in the
-- same transaction as the successful run INSERT, from a FRESH write-time
-- database clock — clock_timestamp() at the advance UPDATE itself, not the
-- gate's earlier decision clock, which only adjudicates that one interval
-- decision). It is PRIVATE: not part of the public Connection model, not
-- returned by any API, never reset by migration replays, and never backfilled
-- from historical runs. Even policy-off admissions advance it, so enabling
-- min_interval_ms later interprets real history instead of a stale zero.
--
-- idx_sync_runs_active_def is a partial index backing the admission count
-- (pending+running runs by def), which executes while the source row lock is
-- held — the existing idx_batch_sync_defs_source remains the source->defs
-- leg of the join. Guard predicates elsewhere reuse the same shape.
--
-- Idempotent by construction (ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT
-- EXISTS), replayed inside the batch advisory lock like every other file; no
-- historical SQL is modified and no CONCURRENTLY build is used.

ALTER TABLE connections
    ADD COLUMN IF NOT EXISTS sync_last_admitted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_sync_runs_active_def
    ON sync_runs(sync_def_id)
    WHERE status IN ('pending','running');
