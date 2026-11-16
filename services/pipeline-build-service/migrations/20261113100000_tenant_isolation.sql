-- P1.1 tenant isolation: persist an organization (tenant) owner on the
-- pipeline-authored resources that are reached by RID or have no FK to
-- pipelines. Resources with a FK chain to pipelines/builds
-- (pipeline_runs, pipeline_versions, jobs, job_logs) inherit the boundary
-- via joins instead of duplicating the column.
--
-- Legacy rows deliberately keep tenant_id NULL: NULL is the personal
-- domain of the original owner (pipelines.owner_id / builds.requested_by /
-- *_specs.created_by), so old data stays reachable by its owner and is
-- invisible to organization tokens. Moving personal data into an
-- organization requires an explicit ownership migration, never an
-- implicit claim.

ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS tenant_id UUID NULL;
CREATE INDEX IF NOT EXISTS idx_pipelines_tenant ON pipelines(tenant_id);

ALTER TABLE builds ADD COLUMN IF NOT EXISTS tenant_id UUID NULL;
CREATE INDEX IF NOT EXISTS idx_builds_tenant ON builds(tenant_id);

ALTER TABLE job_specs ADD COLUMN IF NOT EXISTS tenant_id UUID NULL;
CREATE INDEX IF NOT EXISTS idx_job_specs_tenant ON job_specs(tenant_id);

ALTER TABLE pipeline_job_specs ADD COLUMN IF NOT EXISTS tenant_id UUID NULL;

-- The old global UNIQUE (pipeline_rid, branch_name, output_dataset_rid)
-- let a foreign tenant's upsert overwrite another tenant's spec row when
-- both reference the same pipeline RID string, and — within the personal
-- (tenant-NULL) domain — one user's upsert could overwrite another user's
-- row for the same key. Replace it with a domain-aware unique index:
--   * org rows key on tenant_id (sentinel owner column);
--   * personal rows key on published_by (sentinel tenant column).
-- The sentinel UUID keeps every expression non-NULL so plain UNIQUE
-- semantics apply; no NULLS DISTINCT caveats. Republishing the same key
-- within the SAME domain still upserts exactly like the legacy constraint.
DO $$
DECLARE legacy_unique NAME;
BEGIN
    SELECT conname INTO legacy_unique
    FROM pg_constraint
    WHERE conrelid = 'pipeline_job_specs'::regclass
      AND contype = 'u'
      AND pg_get_constraintdef(oid) LIKE '%(pipeline_rid, branch_name, output_dataset_rid)%';
    IF legacy_unique IS NOT NULL THEN
        EXECUTE format('ALTER TABLE pipeline_job_specs DROP CONSTRAINT %I', legacy_unique);
    END IF;
END $$;
DROP INDEX IF EXISTS idx_pipeline_job_specs_key_tenant;
-- CASE is not a function call, so as an index element it must be
-- parenthesized (COALESCE is function-call syntax and needs no wrap).
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_job_specs_key_domain
    ON pipeline_job_specs (
        pipeline_rid,
        branch_name,
        output_dataset_rid,
        COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::uuid),
        (CASE WHEN tenant_id IS NULL
              THEN published_by
              ELSE '00000000-0000-0000-0000-000000000000'::uuid END)
    );

ALTER TABLE pipeline_run_submissions ADD COLUMN IF NOT EXISTS tenant_id UUID NULL;
ALTER TABLE pipeline_run_submissions ADD COLUMN IF NOT EXISTS created_by UUID NULL;
CREATE INDEX IF NOT EXISTS idx_pipeline_run_submissions_tenant ON pipeline_run_submissions(tenant_id);
