//go:build integration

package repo

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// TestGetTransactionOutputCommitMatchesDatasetTransactionAndBranch: the
// receipt of a keyed commit is found by the dataset, the transaction it cut
// and its branch, and keeps the declared schema beside its one file. Another
// branch, another dataset, an unknown transaction and a commit made without a
// key all answer nil.
func TestGetTransactionOutputCommitMatchesDatasetTransactionAndBranch(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	org, owner := uuid.New(), uuid.New()
	scope := TenantScope{OrgID: &org, Subject: owner}
	repo := &Repo{Pool: pool}
	dataset, err := repo.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "receipt-by-transaction-" + uuid.NewString()[:8]}, owner, &org)
	if err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	declared := &models.DatasetSchema{FileFormat: models.FileFormatText, Fields: []models.Field{{Name: "currency", Type: models.FieldTypeString}}}
	fixture := keyedBuildFixture(dataset.ID.String(), scope, "order-lines.ndjson", owner)
	key := "noon:" + uuid.NewString()
	receipt, replayed, err := repo.WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, keyedFingerprint(keyedFixtureBody(t)),
		func(ctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
			response, err := fixture(ctx, tx)
			response.Schema = declared
			return response, err
		})
	if err != nil || replayed {
		t.Fatalf("keyed commit: replayed=%v err=%v", replayed, err)
	}

	commit, err := repo.GetTransactionOutputCommit(ctx, dataset.ID, receipt.Transaction.ID, "main")
	if err != nil || commit == nil {
		t.Fatalf("receipt of the cutting transaction: commit=%v err=%v", commit, err)
	}
	if !reflect.DeepEqual(commit.Schema, declared) || len(commit.Files) != 1 || commit.Files[0].Path != "order-lines.ndjson" ||
		commit.Transaction.ID != receipt.Transaction.ID || commit.Branch != "main" {
		t.Fatalf("receipt must keep the declared schema and its one file, got %+v", commit)
	}

	// A commit made without a key cuts a version but leaves no receipt.
	if err := repo.EnsureDefaultBranch(ctx, dataset); err != nil {
		t.Fatalf("ensure default branch: %v", err)
	}
	branch, err := repo.GetRuntimeBranch(ctx, dataset.ID, "main")
	if err != nil || branch == nil {
		t.Fatalf("default branch: branch=%v err=%v", branch, err)
	}
	txn, err := repo.StartTransaction(ctx, dataset.ID, branch.ID, branch.Name, models.TransactionTypeSnapshot,
		"commit without a key", models.JSONValue([]byte(`{}`)), owner)
	if err != nil {
		t.Fatalf("start transaction: %v", err)
	}
	if err := repo.CommitTransaction(ctx, dataset.ID, txn.ID); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	for name, lookup := range map[string]struct {
		datasetID     uuid.UUID
		transactionID uuid.UUID
		branch        string
	}{
		"another branch":      {dataset.ID, receipt.Transaction.ID, "release"},
		"another dataset":     {uuid.New(), receipt.Transaction.ID, "main"},
		"unknown transaction": {dataset.ID, uuid.New(), "main"},
		"commit without key":  {dataset.ID, txn.ID, "main"},
	} {
		got, err := repo.GetTransactionOutputCommit(ctx, lookup.datasetID, lookup.transactionID, lookup.branch)
		if err != nil || got != nil {
			t.Fatalf("%s: expected no receipt, got %+v err=%v", name, got, err)
		}
	}
}
