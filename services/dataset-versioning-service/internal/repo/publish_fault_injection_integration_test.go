//go:build integration

// Real-PostgreSQL-16 fault injection for the atomic publish path. These
// tests drive the FULL handler stack (HTTP handler → tenant resolution →
// outputs:commit → CommitTransactionWithPublish) against a real database
// with injected faults at three publish stages, proving that a failed
// publish leaves the v1 snapshot byte-identical on every observable
// surface and that retrying after the fault clears succeeds.
package repo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

func faultPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
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

// datasetState is the observable publish surface snapshot compared before
// and after each faulted commit attempt.
type datasetState struct {
	currentVersion int32
	headTxn        *uuid.UUID
	filePaths      string
	indexPaths     string
	lineageTargets string
	fileCount      int
}

func loadDatasetState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, datasetID uuid.UUID) datasetState {
	t.Helper()
	var s datasetState
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT current_version FROM datasets WHERE id = $1`, datasetID).Scan(&s.currentVersion))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT head_transaction_id FROM dataset_branches WHERE dataset_id = $1 AND name = 'main'`, datasetID).Scan(&s.headTxn))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*), COALESCE(string_agg(logical_path, ',' ORDER BY logical_path), '')
		FROM dataset_files WHERE dataset_id = $1`, datasetID).Scan(&s.fileCount, &s.filePaths))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*), COALESCE(string_agg(path, ',' ORDER BY path), '')
		FROM dataset_file_index WHERE dataset_id = $1`, datasetID).Scan(new(int), &s.indexPaths))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*), COALESCE(string_agg(target_rid, ',' ORDER BY target_rid), '')
		FROM dataset_lineage_links WHERE dataset_id = $1`, datasetID).Scan(new(int), &s.lineageTargets))
	return s
}

func commitViaHandler(t *testing.T, h *handlers.Handlers, rid string, body string, claims *authmw.Claims) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", strings.NewReader(body))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", rid)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	h.CommitDatasetOutput(rec, req)
	return rec
}

// TestCommitDatasetOutputPublishFaultInjection: after a successful v1
// publish, three fault classes hit the second commit — a file-index
// constraint failure, a lineage constraint failure and a response-read
// failure (the lineage trigger drops the file-index table inside the
// publish transaction). Every attempt must answer 5xx, leave
// current_version, branch head, dataset_files, file index and lineage
// identical to v1, and end its transaction ABORTED; once the fault
// clears the retry publishes v2.
func TestCommitDatasetOutputPublishFaultInjection(t *testing.T) {
	ctx := context.Background()
	pool := faultPool(ctx, t)
	require.NoError(t, repo.Migrate(ctx, pool))

	r := &repo.Repo{Pool: pool}
	h := &handlers.Handlers{Repo: r}

	org := uuid.New()
	owner := uuid.New()
	claims := &authmw.Claims{Sub: owner, OrgID: &org, Permissions: []string{"dataset.write"}}
	target, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "fault-target"}, owner, &org)
	require.NoError(t, err)
	sibling, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "fault-sibling"}, owner, &org)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, target))
	rid := "ri.foundry.main.dataset." + target.ID.String()
	siblingRID := "ri.foundry.main.dataset." + sibling.ID.String()

	commitBody := func(path string) string {
		return `{"branch":"main",` +
			`"files":[{"logical_path":"` + path + `","size_bytes":6,"storage_path":"datasets/` + target.ID.String() + `/out/` + path + `"}],` +
			`"lineage_links":[{"direction":"upstream","target_rid":"` + siblingRID + `"}]}`
	}

	// v1: a clean publish establishes the baseline snapshot.
	rec := commitViaHandler(t, h, rid, commitBody("v1.ndjson"), claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var v1 models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v1))
	require.Equal(t, models.TransactionStatusCommitted, v1.Transaction.Status)
	baseline := loadDatasetState(ctx, t, pool, target.ID)
	require.Equal(t, "v1.ndjson", baseline.indexPaths, "v1 index must be published")

	lastTxnID := func() uuid.UUID {
		var id uuid.UUID
		require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM dataset_transactions WHERE dataset_id = $1
			ORDER BY started_at DESC, id DESC LIMIT 1`, target.ID).Scan(&id))
		return id
	}
	assertStateMatches := func(label string) {
		t.Helper()
		after := loadDatasetState(ctx, t, pool, target.ID)
		assert.Equal(t, baseline, after, "%s: publish surfaces must stay identical to v1", label)
		var status string
		require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM dataset_transactions WHERE id = $1`, lastTxnID()).Scan(&status))
		assert.Equal(t, "ABORTED", status, "%s: failed publish must finalise its transaction as ABORTED", label)
		t.Logf("%s: handler answered 5xx and state matches v1 (version=%d head=%v files=%d index=%q lineage=%q, txn=%s)",
			label, after.currentVersion, after.headTxn != nil, after.fileCount, after.indexPaths, after.lineageTargets, status)
	}

	// Fault 1 — file-index rewrite fails: the injected constraint rejects
	// the new index row (entry_type). NOT VALID keeps the v1 rows intact
	// while still failing every new INSERT.
	_, err = pool.Exec(ctx, `ALTER TABLE dataset_file_index ADD CONSTRAINT fault_entry_type CHECK (entry_type = 'directory') NOT VALID`)
	require.NoError(t, err)
	rec = commitViaHandler(t, h, rid, commitBody("fault1.ndjson"), claims)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "index constraint failure must 500, got body %s", rec.Body.String())
	assertStateMatches("index rewrite failure")
	_, err = pool.Exec(ctx, `ALTER TABLE dataset_file_index DROP CONSTRAINT fault_entry_type`)
	require.NoError(t, err)

	// Fault 2 — lineage rewrite fails: the injected constraint rejects the
	// new lineage direction. NOT VALID keeps the v1 rows intact while
	// still failing every new INSERT (per the PG16 ALTER TABLE docs, the
	// constraint "will still be enforced against subsequent inserts or
	// updates" while the initial table scan "is skipped").
	_, err = pool.Exec(ctx, `ALTER TABLE dataset_lineage_links ADD CONSTRAINT fault_direction CHECK (direction = 'downstream') NOT VALID`)
	require.NoError(t, err)
	rec = commitViaHandler(t, h, rid, commitBody("fault2.ndjson"), claims)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "lineage constraint failure must 500, got body %s", rec.Body.String())
	assertStateMatches("lineage rewrite failure")
	_, err = pool.Exec(ctx, `ALTER TABLE dataset_lineage_links DROP CONSTRAINT fault_direction`)
	require.NoError(t, err)

	// Fault 3 — response read fails: the lineage trigger drops the
	// file-index table mid-transaction, so the snapshot read of the
	// published index errors. PostgreSQL's transactional DDL restores the
	// table (and its v1 rows) when the publish rolls back.
	_, err = pool.Exec(ctx, `CREATE OR REPLACE FUNCTION fault_drop_index() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN EXECUTE 'DROP TABLE dataset_file_index'; RETURN NEW; END $$`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE TRIGGER fault_drop_index_trg BEFORE INSERT ON dataset_lineage_links
		FOR EACH ROW EXECUTE FUNCTION fault_drop_index()`)
	require.NoError(t, err)
	rec = commitViaHandler(t, h, rid, commitBody("fault3.ndjson"), claims)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "response read failure must 500, got body %s", rec.Body.String())
	assertStateMatches("response read failure")
	_, err = pool.Exec(ctx, `DROP TRIGGER fault_drop_index_trg ON dataset_lineage_links`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `DROP FUNCTION fault_drop_index()`)
	require.NoError(t, err)

	// All faults cleared: the retry publishes v2 and the version advances
	// by exactly one.
	rec = commitViaHandler(t, h, rid, commitBody("v2.ndjson"), claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	final := loadDatasetState(ctx, t, pool, target.ID)
	assert.Equal(t, baseline.currentVersion+1, final.currentVersion, "retry must advance the version by one")
	assert.Equal(t, "v2.ndjson", final.indexPaths, "retry must publish the new index")
	assert.Equal(t, baseline.lineageTargets, final.lineageTargets, "retry republishes the same lineage edge")
	assert.Equal(t, baseline.fileCount+1, final.fileCount, "trigger projection appends the retried commit's file")

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM dataset_transactions WHERE id = $1`, lastTxnID()).Scan(&status))
	assert.Equal(t, "COMMITTED", status, "retry must commit cleanly")
}
