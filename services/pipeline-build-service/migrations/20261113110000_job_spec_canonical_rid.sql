-- Canonical job-spec binding (separate migration: databases that already
-- applied 20261113100000_tenant_isolation.sql skip that file by the
-- filename ledger, so an appended column would never run there).
--
-- job_specs.rid is the canonical "ri.foundry.main.job_spec.<uuid>" the
-- publish API returns, while pipeline_job_specs.rid is a
-- differently-prefixed generated column that keeps the FIRST row's rid on
-- same-key republishes. pipeline_job_specs.job_spec_rid stores the
-- CURRENT canonical rid so Lookup returns the rid Jobs/LoadPlan bind to.
-- The backfill derives the value from each row's own id — no
-- cross-domain reclaiming. Every statement is re-runnable.
ALTER TABLE pipeline_job_specs ADD COLUMN IF NOT EXISTS job_spec_rid TEXT NULL;
UPDATE pipeline_job_specs
SET job_spec_rid = 'ri.foundry.main.job_spec.' || id::text
WHERE job_spec_rid IS NULL;
