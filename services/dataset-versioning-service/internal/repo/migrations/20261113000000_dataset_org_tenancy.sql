-- P1.1 — Dataset tenant isolation.
--
-- Datasets gain a persistent organization ownership column. `org_id` is assigned
-- exclusively from the verified JWT OrgID at creation time (handler layer);
-- request parameters, bodies and forwarded headers never populate it.
--
-- Semantics:
--   * org_id = <uuid>  → the dataset belongs to that organization. Only
--     callers whose verified JWT carries the same OrgID may reach it.
--   * org_id IS NULL   → personal domain. Only the original owner calling
--     WITHOUT an OrgID may reach it. Organization requests never claim
--     legacy rows implicitly; moving a legacy dataset into an organization
--     requires an explicit, separate migration of that row.
--
-- Backfill: none by design. Every statement is idempotent, so re-running
-- the migration set on an already-migrated (or fresh) database is a no-op
-- and existing NULL rows keep their personal-domain behavior.

ALTER TABLE datasets
    ADD COLUMN IF NOT EXISTS org_id UUID;

-- Partial index: organization lookups filter on a concrete org_id; the
-- NULL (personal) majority of legacy rows stays out of the index.
CREATE INDEX IF NOT EXISTS idx_datasets_org_id
    ON datasets(org_id) WHERE org_id IS NOT NULL;

COMMENT ON COLUMN datasets.org_id IS
    'Owning organization (tenant) assigned from verified JWT OrgID at creation; NULL = personal domain of owner_id, explicit migration required to move into an org';
