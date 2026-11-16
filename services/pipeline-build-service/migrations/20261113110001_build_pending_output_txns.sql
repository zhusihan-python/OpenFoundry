-- Failure-compensation bookkeeping: every output transaction a build
-- resolution OPENS is recorded here the moment it is opened (including
-- ones whose lock attempt subsequently fails), so a later retry can
-- abort exactly the dangling remote transactions of THIS build. Rows are
-- cleared only after a confirmed remote abort. Tenant isolation rides on
-- the parent build's tenant_id via joins; never copied into this table.
CREATE TABLE IF NOT EXISTS build_pending_output_txns (
    build_id       UUID NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    dataset_rid    TEXT NOT NULL,
    branch         TEXT NOT NULL,
    transaction_rid TEXT NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (build_id, transaction_rid)
);
CREATE INDEX IF NOT EXISTS idx_build_pending_output_txns_build ON build_pending_output_txns(build_id);
