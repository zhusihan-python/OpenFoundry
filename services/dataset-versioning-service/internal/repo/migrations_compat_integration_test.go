//go:build integration

package repo

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// bootPool honours DATASET_DB_DSN when set (standalone container) and
// otherwise falls back to the standard testcontainers harness.
func bootPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()
	if dsn := os.Getenv("DATASET_DB_DSN"); dsn != "" {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect DATASET_DB_DSN: %v", err)
		}
		if err := pool.Ping(ctx); err != nil {
			t.Fatalf("ping DATASET_DB_DSN: %v", err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	return testingx.BootPostgres(ctx, t).Pool
}

// TestMigrationsFreshDBRerunAndRepeatedSnapshots locks the phase-3
// startup/commit fixes against a real empty Postgres 16:
//
//  1. the full lexically-ordered migration set applies on a fresh
//     database (previously 20260501000001 failed with SQLSTATE 42703
//     because the legacy dataset_views from 20260425173000 lacks
//     branch_id/computed_at);
//  2. it applies a second time unchanged (every startup replays the
//     set, so each statement must be re-runnable);
//  3. branch creation works (EnsureDefaultBranch/CreateBranch
//     previously hit "inconsistent types deduced for parameter $2:
//     text versus uuid", SQLSTATE 42804);
//  4. the same dataset commits two consecutive SNAPSHOT transactions
//     (previously blocked by dataset_transactions.operation having no
//     default, SQLSTATE 23502, and by legacy UNIQUE(dataset_id, name)
//     colliding with the trigger's anonymous '' views);
//  5. named catalog views keep upsert semantics on the partial unique
//     index, and legacy columns survive.
func TestMigrationsFreshDBRerunAndRepeatedSnapshots(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first Migrate on empty DB: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate (re-run): %v", err)
	}

	r := &Repo{Pool: pool}
	dataset, err := r.CreateDataset(ctx, &models.CreateDatasetRequest{Name: "phase3-fixes"}, uuid.New())
	if err != nil {
		t.Fatalf("CreateDataset: %v", err)
	}

	// Root branch (EnsureDefaultBranch $2 cast) and an output branch
	// (CreateBranch $2 cast).
	if err := r.EnsureDefaultBranch(ctx, dataset); err != nil {
		t.Fatalf("EnsureDefaultBranch: %v", err)
	}
	if _, err := r.CreateBranch(ctx, dataset, &models.CreateDatasetBranchRequest{Name: "output"}); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	branch, err := r.GetBranch(ctx, dataset.ID, "main")
	if err != nil || branch == nil {
		t.Fatalf("GetBranch(main): %v, %v", branch, err)
	}

	// Two consecutive SNAPSHOT commits carrying a schema payload so the
	// trg_dataset_view_schemas_from_txn trigger materialises a
	// dataset_views row (name defaults to '') per commit.
	schemaMeta := models.JSONValue([]byte(`{"schema":{"file_format":"parquet","fields":[{"name":"a","type":"string"}]}}`))
	for i, path := range []string{"part-0.parquet", "part-1.parquet"} {
		txn, err := r.StartTransaction(ctx, dataset.ID, branch.ID, branch.Name,
			models.TransactionTypeSnapshot, "phase3 snapshot", schemaMeta, uuid.New())
		if err != nil {
			t.Fatalf("snapshot %d StartTransaction: %v", i+1, err)
		}
		files := []models.StageTransactionFile{{
			LogicalPath:  path,
			PhysicalPath: "s3://bucket/" + path,
			SizeBytes:    int64(100 * (i + 1)),
			Operation:    models.FileOperationAdd,
		}}
		if err := r.StageTransactionFiles(ctx, dataset.ID, txn.ID, files); err != nil {
			t.Fatalf("snapshot %d StageTransactionFiles: %v", i+1, err)
		}
		if err := r.MergeTransactionMetadata(ctx, dataset.ID, txn.ID, schemaMeta); err != nil {
			t.Fatalf("snapshot %d MergeTransactionMetadata: %v", i+1, err)
		}
		if err := r.CommitTransaction(ctx, dataset.ID, txn.ID); err != nil {
			t.Fatalf("snapshot %d CommitTransaction: %v", i+1, err)
		}
	}

	// Anonymous snapshot views: exactly one per commit despite the
	// legacy UNIQUE(dataset_id, name) era.
	var anonViews int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM dataset_views WHERE dataset_id = $1 AND name = ''`,
		dataset.ID).Scan(&anonViews); err != nil {
		t.Fatalf("count anonymous views: %v", err)
	}
	if anonViews != 2 {
		t.Fatalf("anonymous snapshot views = %d, want 2", anonViews)
	}

	// Named catalog views keep upsert semantics: same name twice
	// updates in place (partial unique index WHERE name <> '').
	desc := "first"
	for i := 0; i < 2; i++ {
		if _, err := r.CreateView(ctx, dataset.ID, &models.CreateDatasetViewRequest{
			Name:        "named-v1",
			Description: &desc,
			SQL:         "SELECT 1",
		}); err != nil {
			t.Fatalf("CreateView call %d: %v", i+1, err)
		}
	}
	var namedViews int
	var namedDesc string
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*), MIN(description) FROM dataset_views WHERE dataset_id = $1 AND name = 'named-v1'`,
		dataset.ID).Scan(&namedViews, &namedDesc); err != nil {
		t.Fatalf("count named views: %v", err)
	}
	if namedViews != 1 {
		t.Fatalf("named views = %d, want 1 (upsert must update in place)", namedViews)
	}

	// Legacy catalog columns must survive the compat bridge.
	for _, col := range []string{"sql_text", "schema_fields", "materialized"} {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns
			  WHERE table_name = 'dataset_views' AND column_name = $1)`, col).Scan(&exists); err != nil {
			t.Fatalf("column probe %s: %v", col, err)
		}
		if !exists {
			t.Fatalf("legacy column dataset_views.%s missing", col)
		}
	}
}
