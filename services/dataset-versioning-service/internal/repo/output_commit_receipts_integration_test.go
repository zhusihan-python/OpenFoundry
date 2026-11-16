//go:build integration

// P2.6 W1 — keyed outputs:commit against a real Postgres.
//
// NOT executed in W1 (no PG container in this phase); this is the
// ready-to-run concurrency acceptance source for the follow-up review:
//
//   - two independent pools ("instances") racing the same key + identical
//     body must produce exactly one first-commit and one byte-identical
//     replay, one version advance and one receipt row;
//   - racing the same key with DIFFERENT bodies must produce exactly one
//     winner and one typed conflict, zero duplicate receipts;
//   - the receipt GET stays tenant-scoped.
//
// Run explicitly (see tools/smoke/business-platform-p2-6): start the PG
// fixture, then `go test -tags integration ./internal/repo/ -run Keyed`.
package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// keyedBuildFixture is a minimal, deterministic build closure mirroring the
// handler's keyed path without a backing filesystem: branch resolution,
// Start/Stage/Merge/Publish all through the tx view, response assembled
// from the in-memory snapshot.
func keyedBuildFixture(rawRef string, scope TenantScope, file string, startedBy uuid.UUID) func(context.Context, OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
	return func(ctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		var zero models.CommitDatasetOutputResponse
		dataset, err := tx.ResolveDatasetForTenant(ctx, rawRef, scope)
		if err != nil {
			return zero, err
		}
		if dataset == nil {
			return zero, ErrNotFound
		}
		if err := tx.EnsureDefaultBranch(ctx, dataset); err != nil {
			return zero, err
		}
		branch, err := tx.GetRuntimeBranch(ctx, dataset.ID, "main")
		if err != nil && !errors.Is(err, ErrNotFound) {
			return zero, err
		}
		if branch == nil {
			return zero, ErrNotFound
		}
		txn, err := tx.StartTransaction(ctx, dataset.ID, branch.ID, branch.Name, models.TransactionTypeSnapshot, "integration keyed commit", models.JSONValue([]byte(`{}`)), startedBy)
		if err != nil {
			return zero, err
		}
		staged := []models.StageTransactionFile{{LogicalPath: file, PhysicalPath: "datasets/" + dataset.ID.String() + "/out/" + file, SizeBytes: 3, Operation: models.FileOperationAdd}}
		if err := tx.StageTransactionFiles(ctx, dataset.ID, txn.ID, staged); err != nil {
			return zero, err
		}
		if err := tx.MergeTransactionMetadata(ctx, dataset.ID, txn.ID, models.JSONValue([]byte(`{"pipeline_output":true}`))); err != nil {
			return zero, err
		}
		snapshot, err := tx.CommitTransactionWithPublish(ctx, dataset.ID, txn.ID, []models.PutDatasetFileIndexEntry{{Path: file, StoragePath: "datasets/" + dataset.ID.String() + "/out/" + file, SizeBytes: ptrInt64(3)}}, nil)
		if err != nil {
			return zero, err
		}
		return models.CommitDatasetOutputResponse{
			DatasetID:   dataset.ID,
			DatasetRID:  "ri.foundry.main.dataset." + dataset.ID.String(),
			Branch:      branch.Name,
			Transaction: snapshot.Transaction,
			Files:       snapshot.Files,
			Preview:     snapshot.Preview,
		}, nil
	}
}

func ptrInt64(v int64) *int64 { return &v }

func keyedFixtureBody(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"idempotency_key": "of-p26-pub-v1:" + uuid.NewString(),
		"branch":          "main",
		"files":           []map[string]any{{"logical_path": "part-00000.ndjson", "size_bytes": 3}},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return string(raw)
}

func keyedFingerprint(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// TestWithIdempotentOutputCommitConcurrentTwoInstances: two pools race the
// same key+body. Expected: exactly one first-commit (receipt durable) and
// one replay returning byte-identical stored bytes; exactly one version
// advance; exactly one receipt row; no OPEN transactions left behind.
func TestWithIdempotentOutputCommitConcurrentTwoInstances(t *testing.T) {
	ctx := context.Background()
	poolA := bootPool(ctx, t)
	poolB, err := pgxpool.New(ctx, poolA.Config().ConnString())
	if err != nil {
		t.Fatalf("second pool: %v", err)
	}
	t.Cleanup(poolB.Close)
	if err := Migrate(ctx, poolB); err != nil {
		t.Fatalf("migrate on second pool: %v", err)
	}

	org, owner := uuid.New(), uuid.New()
	scope := TenantScope{OrgID: &org, Subject: owner}
	dataset, err := (&Repo{Pool: poolA}).CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "keyed-race-" + uuid.NewString()[:8]}, owner, &org)
	if err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	rawRef := dataset.ID.String()

	key := "of-p26-pub-v1:" + uuid.NewString()
	body := keyedFixtureBody(t)
	fingerprint := keyedFingerprint(body)

	var firstCount, replayCount atomic.Int32
	var firstBody, replayBody []byte
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, pool := range []*pgxpool.Pool{poolA, poolB} {
		wg.Add(1)
		go func(p *pgxpool.Pool) {
			defer wg.Done()
			r := &Repo{Pool: p}
			receipt, replayed, cerr := r.WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, keyedBuildFixture(rawRef, scope, "part-00000.ndjson", owner))
			if cerr != nil {
				t.Errorf("instance attempt: %v", cerr)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if replayed {
				replayCount.Add(1)
				replayBody = receipt.ResponseBody
			} else {
				firstCount.Add(1)
				firstBody = receipt.ResponseBody
			}
		}(pool)
	}
	wg.Wait()

	if got := firstCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 first commit, got %d", got)
	}
	if got := replayCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 replay, got %d", got)
	}
	if string(firstBody) != string(replayBody) {
		t.Fatalf("replay bytes differ from first response:\nfirst:  %s\nreplay: %s", firstBody, replayBody)
	}

	var receipts, versions, openTxns int
	if err := poolA.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, dataset.ID, key).Scan(&receipts); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if err := poolA.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, dataset.ID).Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if err := poolA.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_transactions WHERE dataset_id=$1 AND status='OPEN'`, dataset.ID).Scan(&openTxns); err != nil {
		t.Fatalf("count open txns: %v", err)
	}
	if receipts != 1 {
		t.Fatalf("expected 1 receipt row, got %d", receipts)
	}
	if versions != 1 {
		t.Fatalf("expected the race to advance the version exactly once, got %d versions", versions)
	}
	if openTxns != 0 {
		t.Fatalf("expected no OPEN transactions after the race, got %d", openTxns)
	}

	// A different body under the same key is a typed conflict with zero
	// side effects, and the original receipt stays replayable.
	other := keyedFixtureBody(t)
	if _, _, err := (&Repo{Pool: poolA}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, keyedFingerprint(other), keyedBuildFixture(rawRef, scope, "other.ndjson", owner)); !errors.Is(err, ErrIdempotencyKeyConflict) {
		t.Fatalf("expected ErrIdempotencyKeyConflict, got %v", err)
	}
	again, replayed, err := (&Repo{Pool: poolB}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, keyedBuildFixture(rawRef, scope, "part-00000.ndjson", owner))
	if err != nil || !replayed {
		t.Fatalf("original body must replay: replayed=%v err=%v", replayed, err)
	}
	if string(again.ResponseBody) != string(firstBody) {
		t.Fatalf("replay after conflict must keep the original bytes")
	}

	// Cross-tenant GET never sees the receipt.
	foreignOrg := uuid.New()
	foreign := TenantScope{OrgID: &foreignOrg, Subject: uuid.New()}
	if got, err := (&Repo{Pool: poolA}).GetOutputCommitReceipt(ctx, foreign, dataset.ID, key); err != nil || got != nil {
		t.Fatalf("foreign tenant must get (nil,nil), got (%v, %v)", got, err)
	}
	own, err := (&Repo{Pool: poolA}).GetOutputCommitReceipt(ctx, scope, dataset.ID, key)
	if err != nil || own == nil {
		t.Fatalf("own tenant must get the receipt, got (%v, %v)", own, err)
	}
	if string(own.ResponseBody) != string(firstBody) {
		t.Fatalf("GET must serve the stored bytes")
	}
}

// TestWithIdempotentOutputCommitConcurrentFingerprintDivergence: two
// instances race the same key with DIFFERENT bodies. Exactly one commits;
// the loser gets a typed conflict (never a duplicate publish).
func TestWithIdempotentOutputCommitConcurrentFingerprintDivergence(t *testing.T) {
	ctx := context.Background()
	poolA := bootPool(ctx, t)
	poolB, err := pgxpool.New(ctx, poolA.Config().ConnString())
	if err != nil {
		t.Fatalf("second pool: %v", err)
	}
	t.Cleanup(poolB.Close)
	if err := Migrate(ctx, poolB); err != nil {
		t.Fatalf("migrate on second pool: %v", err)
	}

	org, owner := uuid.New(), uuid.New()
	scope := TenantScope{OrgID: &org, Subject: owner}
	dataset, err := (&Repo{Pool: poolA}).CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "keyed-div-" + uuid.NewString()[:8]}, owner, &org)
	if err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	rawRef := dataset.ID.String()
	key := "of-p26-pub-v1:" + uuid.NewString()

	var wins, conflicts atomic.Int32
	var wg sync.WaitGroup
	add := func(p *pgxpool.Pool, body, file string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replayed, cerr := (&Repo{Pool: p}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, keyedFingerprint(body), keyedBuildFixture(rawRef, scope, file, owner))
			switch {
			case cerr == nil && !replayed:
				wins.Add(1)
			case errors.Is(cerr, ErrIdempotencyKeyConflict):
				conflicts.Add(1)
			default:
				t.Errorf("unexpected outcome: %v", cerr)
			}
		}()
	}
	add(poolA, keyedFixtureBody(t), "a.ndjson")
	add(poolB, keyedFixtureBody(t), "b.ndjson")
	wg.Wait()

	if wins.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("expected 1 win + 1 typed conflict, got %d wins + %d conflicts", wins.Load(), conflicts.Load())
	}
	var receipts int
	if err := poolA.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, dataset.ID, key).Scan(&receipts); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if receipts != 1 {
		t.Fatalf("expected exactly 1 receipt, got %d", receipts)
	}
}
