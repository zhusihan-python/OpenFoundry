-- P1.2 tenant boundary for Source (connections), Sync definitions
-- (batch_sync_defs) and sync runs.
--
-- org_id is derived exclusively from the verified JWT OrgID at creation time;
-- it is NULL for legacy personal-domain rows. NULL rows stay reachable only
-- by their original owner when the caller has no OrgID. Request headers and
-- body fields can never widen the boundary.
--
-- sync_runs.rows_written records the real record count of each snapshot run.

ALTER TABLE connections
    ADD COLUMN IF NOT EXISTS org_id UUID;

ALTER TABLE batch_sync_defs
    ADD COLUMN IF NOT EXISTS org_id UUID;

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS org_id UUID,
    ADD COLUMN IF NOT EXISTS rows_written BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_connections_org ON connections(org_id);
CREATE INDEX IF NOT EXISTS idx_batch_sync_defs_org ON batch_sync_defs(org_id);
CREATE INDEX IF NOT EXISTS idx_sync_runs_org ON sync_runs(org_id);
