-- P2.2 interruptible runs: explicit control marker, durable cancel requests
-- and the publish-entry gate on sync_runs.
--
-- control_mode distinguishes tasks whose execution this code version actually
-- controls ('controlled' — written by every new EnqueueSyncRun) from rows that
-- predate P2.2 ('legacy' — the migration default). A legacy running row says
-- nothing about whether its dataset publish already happened, so cancel is
-- refused for it (409 unsupported/legacy) instead of guessing. NULL-everything
-- is never read as "not published".
--
-- cancel_requested_at / cancelled_by persist a durable, queryable cancel
-- request. Writing the request never fabricates an abort: only the holder of
-- a still-valid lease (or a pending-row transition inside the run lock)
-- produces the terminal 'aborted' state. An expired lease with an
-- unconfirmed request simply stays running + request visible.
--
-- publish_entered_at is the stage marker written by BeginPublishGate inside
-- the run row lock: it is set atomically with a full-TTL renewal only when no
-- cancel is requested. Cancel-first => the gate refuses and the run can still
-- abort with zero publishes; gate-first => later cancels are observably
-- too-late and the run finishes under its real outcome.
--
-- Replay safety (the service replays every migration on every start): all
-- statements are IF NOT EXISTS and none of them reset lease, cancel or stage
-- values on existing rows.
ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS control_mode TEXT NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancelled_by UUID,
    ADD COLUMN IF NOT EXISTS publish_entered_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_sync_runs_cancel_requested
    ON sync_runs(cancel_requested_at) WHERE cancel_requested_at IS NOT NULL;
