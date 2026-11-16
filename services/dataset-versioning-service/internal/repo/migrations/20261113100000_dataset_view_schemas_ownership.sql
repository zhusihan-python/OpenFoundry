-- P1 Build/Job HTTP support — dataset_view_schemas ownership repair.
--
-- Read-only review found a real break: the schema API queries
-- (GetViewSchema / PutViewSchema) reference dataset_id, branch and
-- updated_at on dataset_view_schemas, but 20260503000001 created the
-- table without those columns and no later migration added them — on a
-- real database every schema read fails with SQLSTATE 42703. The unit
-- suite never noticed because pgxmock matches by regex prefix and the
-- in-memory fake has no SQL at all.
--
-- This migration reconciles the table additively and re-runnably:
--
--   * dataset_id is BACKFILLED ONLY from the authoritative ownership
--     relation dataset_views.view_id -> dataset_views.dataset_id.
--     Orphan schema rows (no dataset_views parent) keep dataset_id
--     NULL and can never be claimed through a request — the handler's
--     view-ownership guard treats a NULL owner as foreign and answers
--     404, and PutViewSchema's upsert cannot overwrite one either.
--   * branch is backfilled from the view's branch (dataset_branches
--     name); NULL-branch legacy views leave it NULL.
--   * updated_at mirrors PutViewSchema's ON CONFLICT bookkeeping.
--   * fn_dataset_view_schemas_from_txn (the commit-time schema
--     materialiser) now stamps dataset_id / branch / updated_at too,
--     so freshly committed schemas are never orphans.
--
-- Every statement is idempotent: ADD COLUMN IF NOT EXISTS skips the
-- whole clause (column constraints included) when the column exists,
-- the backfill UPDATE is guarded by IS DISTINCT FROM, and the function
-- replace / trigger rebuild are self-idempotent.

ALTER TABLE dataset_view_schemas
    ADD COLUMN IF NOT EXISTS dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS branch TEXT,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Authoritative backfill: ownership comes from dataset_views (and its
-- branch relation), never from request data. Orphans stay NULL.
UPDATE dataset_view_schemas s
   SET dataset_id = v.dataset_id,
       branch = b.name
  FROM dataset_views v
  LEFT JOIN dataset_branches b ON b.id = v.branch_id
 WHERE s.view_id = v.id
   AND (s.dataset_id IS DISTINCT FROM v.dataset_id
        OR s.branch IS DISTINCT FROM b.name);

CREATE OR REPLACE FUNCTION fn_dataset_view_schemas_from_txn() RETURNS trigger AS $$
DECLARE
    v_view_id UUID;
    v_schema  JSONB;
    v_format  TEXT;
    v_meta    JSONB;
    v_hash    TEXT;
BEGIN
    -- Only on OPEN → COMMITTED transitions. ABORTED never carries a
    -- schema we want to honour.
    IF NEW.status <> 'COMMITTED' OR OLD.status = 'COMMITTED' THEN
        RETURN NEW;
    END IF;

    v_schema := NEW.metadata -> 'schema';
    IF v_schema IS NULL OR jsonb_typeof(v_schema) <> 'object' THEN
        RETURN NEW;
    END IF;

    -- Normalise file_format. Accept lower-case aliases coming from API
    -- callers and map CSV/TSV/JSON onto TEXT.
    v_format := upper(COALESCE(v_schema ->> 'file_format', 'PARQUET'));
    IF v_format IN ('CSV','TSV','JSON') THEN
        v_format := 'TEXT';
    END IF;
    IF v_format NOT IN ('PARQUET','AVRO','TEXT') THEN
        v_format := 'PARQUET';
    END IF;

    v_meta := v_schema -> 'custom_metadata';
    v_hash := md5(v_schema::text);

    -- Materialise a placeholder view row keyed by the committed txn so
    -- the schema FK has a target. ON CONFLICT keeps lazy materialisation
    -- (compute_view_at) idempotent.
    INSERT INTO dataset_views
            (id, dataset_id, branch_id, head_transaction_id,
             computed_at, file_count, size_bytes)
    VALUES  (gen_random_uuid(), NEW.dataset_id, NEW.branch_id, NEW.id,
             NOW(), 0, 0)
    ON CONFLICT (dataset_id, branch_id, head_transaction_id) DO UPDATE
        SET computed_at = NOW()
    RETURNING id INTO v_view_id;

    INSERT INTO dataset_view_schemas
            (view_id, dataset_id, branch, schema_json, file_format,
             custom_metadata, content_hash, created_at, updated_at)
    VALUES  (v_view_id, NEW.dataset_id, NEW.branch_name, v_schema, v_format,
             v_meta, v_hash, NOW(), NOW())
    ON CONFLICT (view_id) DO UPDATE
        SET dataset_id      = EXCLUDED.dataset_id,
            branch          = EXCLUDED.branch,
            schema_json     = EXCLUDED.schema_json,
            file_format     = EXCLUDED.file_format,
            custom_metadata = EXCLUDED.custom_metadata,
            content_hash    = EXCLUDED.content_hash,
            updated_at      = NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dataset_view_schemas_from_txn
    ON dataset_transactions;

CREATE TRIGGER trg_dataset_view_schemas_from_txn
    AFTER UPDATE OF status ON dataset_transactions
    FOR EACH ROW
    WHEN (NEW.status = 'COMMITTED' AND OLD.status IS DISTINCT FROM 'COMMITTED')
    EXECUTE FUNCTION fn_dataset_view_schemas_from_txn();
