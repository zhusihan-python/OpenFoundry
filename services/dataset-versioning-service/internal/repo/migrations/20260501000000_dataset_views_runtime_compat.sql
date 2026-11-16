-- Runtime compatibility bridge for databases whose `dataset_views` /
-- `dataset_transactions` were created by the legacy catalog-shape
-- migration 20260425173000_dataset_views_transactions.sql.
--
-- 20260501000001_versioning_init.sql re-declares both tables with the
-- Foundry runtime shape, but `CREATE TABLE IF NOT EXISTS` is a no-op
-- when the legacy tables already exist (PG does not reconcile schemas),
-- and unlike `dataset_transactions` it ships no backfill for
-- `dataset_views`. Its `idx_dataset_views_dataset_branch` then fails
-- with SQLSTATE 42703 (undefined column `branch_id`) on every startup.
--
-- This migration runs lexically before 20260501000001 and reconciles
-- the legacy tables additively — legacy columns and rows are preserved:
--
--   * `dataset_views` gains the runtime view fields (branch / head
--     transaction FKs, computed_at, file_count, size_bytes, metadata).
--   * `dataset_views.name` / `dataset_views.sql_text` get the runtime
--     DEFAULT '' so the trg_dataset_view_schemas_from_txn trigger
--     (20260503000001), which materialises snapshot views without
--     either column, can insert.
--   * the runtime snapshot key UNIQUE(dataset_id, branch_id,
--     head_transaction_id) is added as a unique index. Legacy rows
--     have NULL branch_id / head_transaction_id and never collide
--     (default NULLS DISTINCT).
--   * the legacy UNIQUE(dataset_id, name) is replaced by an equivalent
--     partial unique index covering only NAMED views (name <> '').
--     Anonymous snapshot views (default '') may now repeat per commit;
--     named-view upsert semantics are unchanged (the CreateView handler
--     rejects empty names, so its ON CONFLICT target stays satisfied).
--   * `dataset_transactions.operation` gets the runtime DEFAULT '' —
--     the legacy column is NOT NULL without a default and
--     StartTransaction does not name it (SQLSTATE 23502 otherwise).
--
-- Every statement is idempotent, so re-running the migration set on an
-- already-reconciled (or fresh runtime-shaped) database is a no-op.

-- ── dataset_views: runtime fields on the legacy shape ────────────────────
ALTER TABLE dataset_views
    ADD COLUMN IF NOT EXISTS branch_id           UUID,
    ADD COLUMN IF NOT EXISTS head_transaction_id UUID,
    ADD COLUMN IF NOT EXISTS computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS file_count          INT  NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS size_bytes          BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS metadata            JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Snapshot views are materialised by the commit trigger without a name
-- and without a sql_text; the runtime shape defaults both to '' (the
-- legacy columns are NOT NULL with no default, SQLSTATE 23502 otherwise).
ALTER TABLE dataset_views
    ALTER COLUMN name     SET DEFAULT '',
    ALTER COLUMN sql_text SET DEFAULT '';

-- FKs are added guarded (not inline on ADD COLUMN) so re-runs and
-- runtime-shaped databases never end up with duplicate constraints.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
         WHERE c.conrelid = 'dataset_views'::regclass
           AND c.contype = 'f'
           AND c.conkey = ARRAY[(SELECT attnum FROM pg_attribute
                                  WHERE attrelid = 'dataset_views'::regclass
                                    AND attname = 'branch_id')]
    ) THEN
        ALTER TABLE dataset_views
            ADD CONSTRAINT fk_dataset_views_branch
            FOREIGN KEY (branch_id)
            REFERENCES dataset_branches(id) ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
         WHERE c.conrelid = 'dataset_views'::regclass
           AND c.contype = 'f'
           AND c.conkey = ARRAY[(SELECT attnum FROM pg_attribute
                                  WHERE attrelid = 'dataset_views'::regclass
                                    AND attname = 'head_transaction_id')]
    ) THEN
        ALTER TABLE dataset_views
            ADD CONSTRAINT fk_dataset_views_head_txn
            FOREIGN KEY (head_transaction_id)
            REFERENCES dataset_transactions(id) ON DELETE CASCADE;
    END IF;
END $$;

-- Runtime snapshot key, backing the trigger's ON CONFLICT target. The
-- name coincides with the constraint index a fresh CREATE TABLE in
-- 20260501000001 would have produced, so runtime-shaped databases do
-- not grow a duplicate index.
CREATE UNIQUE INDEX IF NOT EXISTS dataset_views_dataset_id_branch_id_head_transaction_id_key
    ON dataset_views(dataset_id, branch_id, head_transaction_id);

-- ── dataset_views: named-view uniqueness without anonymous collisions ────
-- Same pattern as uq_dataset_branches_dataset_id_name_active in
-- 20260502000001_dataset_branches_v2.sql: drop the inline legacy
-- constraint (auto-named dataset_views_dataset_id_name_key) and replace
-- it with a partial unique index that ignores anonymous ('') snapshot
-- views so one dataset can carry one per commit.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'dataset_views_dataset_id_name_key'
           AND conrelid = 'dataset_views'::regclass
    ) THEN
        ALTER TABLE dataset_views
            DROP CONSTRAINT dataset_views_dataset_id_name_key;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dataset_views_dataset_id_name_named
    ON dataset_views(dataset_id, name)
    WHERE name <> '';

-- ── dataset_transactions: legacy operation column needs the default ──────
ALTER TABLE dataset_transactions
    ALTER COLUMN operation SET DEFAULT '';
