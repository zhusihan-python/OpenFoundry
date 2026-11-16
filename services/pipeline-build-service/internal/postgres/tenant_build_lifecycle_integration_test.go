//go:build integration

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	livellogs "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/logs"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/migrations"
)

// buildFlowFixture wires the real repository (builds/locks/jobs/specs)
// against an httptest dataset service through the production HTTP
// adapter, reproducing the split-database compose topology.
type buildFlowFixture struct {
	pool        *pgxpool.Pool
	repo        *Repository
	dataset     *httptest.Server
	dsState     *flowDatasetState
	restore     []func()
	org         *uuid.UUID
	sub         uuid.UUID
	pipelineRID string
}

type flowDatasetState struct {
	branches      map[string]bool
	abortFail     bool
	openFailOnNth int
	aborts        []string
	opens         []string
	deniedRIDs    map[string]bool
	txns          map[string]*flowTxn
}

// flowTxn is the stateful remote transaction record: status transitions
// OPEN→ABORTED are tracked per handle so tests assert real terminal
// states instead of call counts.
type flowTxn struct {
	DatasetRID string
	Branch     string
	Status     string
}

func (st *flowDatasetState) txnStatus(txnRID string) (string, bool) {
	tx, ok := st.txns[txnRID]
	if !ok {
		return "", false
	}
	return tx.Status, true
}

func newBuildFlowFixture(ctx context.Context, t *testing.T) *buildFlowFixture {
	t.Helper()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)

	// Minimal co-located dataset schema so the repo-side co-located
	// helpers (abort semantics test) have real tables; the build flow
	// itself goes through the HTTP adapter.
	for _, ddl := range []string{
		`CREATE TABLE datasets (id UUID PRIMARY KEY, rid TEXT UNIQUE, owner_id UUID NOT NULL, org_id UUID)`,
		`CREATE TABLE dataset_branches (id UUID PRIMARY KEY, dataset_id UUID NOT NULL REFERENCES datasets(id), dataset_rid TEXT NOT NULL, name TEXT NOT NULL, deleted_at TIMESTAMPTZ, head_transaction_id UUID)`,
		`CREATE TABLE dataset_transactions (id UUID PRIMARY KEY, dataset_id UUID NOT NULL, branch_id UUID, branch_name TEXT NOT NULL, tx_type TEXT NOT NULL, status TEXT NOT NULL, operation TEXT, aborted_at TIMESTAMPTZ)`,
	} {
		_, err := pool.Exec(ctx, ddl)
		require.NoError(t, err)
	}

	state := &flowDatasetState{branches: map[string]bool{}, deniedRIDs: map[string]bool{}, txns: map[string]*flowTxn{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			writeFlowJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		rid := flowRIDFromPath(r.URL.Path)
		if state.deniedRIDs[rid] {
			writeFlowJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden", "message": "dataset " + rid + " belongs to another tenant"})
			return
		}
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/branches"):
			writeFlowJSON(w, http.StatusOK, map[string]any{"data": []map[string]any{{"name": "main"}}, "has_more": false})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/schema"):
			writeFlowJSON(w, http.StatusOK, map[string]any{"schema": json.RawMessage(`{"columns":[]}`)})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":abort"):
			if state.abortFail {
				writeFlowJSON(w, http.StatusInternalServerError, map[string]string{"error": "boom"})
				return
			}
			state.aborts = append(state.aborts, r.URL.Path)
			pathSuffix := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			txnRID := "ri.foundry.main.transaction." + strings.TrimSuffix(pathSuffix, ":abort")
			tx, ok := state.txns[txnRID]
			if !ok {
				writeFlowJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "transaction not found"})
				return
			}
			if tx.Status != "OPEN" && tx.Status != "ABORTED" {
				writeFlowJSON(w, http.StatusConflict, map[string]string{"error": "conflict", "message": "transaction is " + tx.Status})
				return
			}
			tx.Status = "ABORTED"
			writeFlowJSON(w, http.StatusOK, map[string]string{"status": "ABORTED"})
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/transactions"):
			state.opens = append(state.opens, r.URL.Path)
			if state.openFailOnNth > 0 && len(state.opens) == state.openFailOnNth {
				writeFlowJSON(w, http.StatusInternalServerError, map[string]string{"error": "boom"})
				return
			}
			txnID := uuid.NewString()
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/datasets/"), "/")
			// /v1/datasets/{rid}/branches/{branch}/transactions
			if len(parts) >= 4 {
				state.txns["ri.foundry.main.transaction."+txnID] = &flowTxn{DatasetRID: parts[0], Branch: parts[2], Status: "OPEN"}
			}
			writeFlowJSON(w, http.StatusCreated, map[string]any{"id": txnID})
		default:
			writeFlowJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		}
	}))
	t.Cleanup(server.Close)

	adapter := handler.DatasetHTTPVersioning{BaseURL: server.URL, Client: server.Client()}
	f := &buildFlowFixture{pool: pool, repo: repo, dataset: server, dsState: state}
	// Real pipeline parent + authoring wiring: validatePipelineParent
	// must prove tenant ownership through the actual table.
	f.org = uuidPtr(uuid.New())
	f.sub = uuid.New()
	scopeCtx := handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: f.org, Subject: f.sub})
	pipeline, err := repo.CreatePipeline(scopeCtx, models.CreatePipelineRequest{Name: "flow-pipeline"}, f.sub)
	require.NoError(t, err)
	f.pipelineRID = "ri.foundry.main.pipeline." + pipeline.ID.String()

	f.restore = append(f.restore,
		handler.SetBuildLifecyclePorts(handler.BuildLifecyclePorts{JobSpecs: repo, Versioning: adapter, Locks: repo, Builds: repo}),
		handler.SetPipelineAuthoringRepository(repo),
		handler.SetBuildQueryRepository(repo),
		handler.SetExecutionPorts(handler.ExecutionPorts{Plans: repo, Runs: repo, Transactions: repo}),
	)
	t.Cleanup(func() {
		for _, r := range f.restore {
			r()
		}
	})
	return f
}

func writeFlowJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *buildFlowFixture) scopedRequest(method, path, body string, orgID *uuid.UUID, sub uuid.UUID) *http.Request {
	r := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	r.Header.Set("Authorization", "Bearer flow-token")
	ctx := authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: sub, OrgID: orgID})
	return r.WithContext(ctx)
}

// TestBuildFlowCancelledContextFinalizesState proves the cancellation
// closure: when the caller hangs up mid-resolution, the remote reads fail
// with the request context, yet the build row still lands in BUILD_FAILED
// (the failure mark runs on a detached context that keeps the tenant
// values).
func TestBuildFlowCancelledContextFinalizesState(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	pipelineRID := f.pipelineRID
	outRID := "ri.foundry.main.dataset." + uuid.NewString()
	_, err := f.repo.PublishJobSpec(handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: f.org, Subject: f.sub}), "TRANSFORM",
		handler.CreateJobSpecRequest{PipelineRID: pipelineRID, BranchName: "main", OutputDatasetRIDs: []string{outRID}, LogicPayload: []byte(`{"t":1}`)}, f.sub.String())
	require.NoError(t, err)

	// Cancel the request context exactly when the FIRST output
	// transaction is being opened: the preflight has passed, the build
	// row exists, and resolution dies mid-flight — the failure mark on
	// the detached context must still land.
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	var cancelOnce bool
	hookRT := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/transactions") && strings.HasSuffix(req.URL.Path, "transactions") && !cancelOnce {
			cancelOnce = true
			cancelRequest()
		}
		return http.DefaultTransport.RoundTrip(req)
	})
	adapter := handler.DatasetHTTPVersioning{BaseURL: f.dataset.URL, Client: &http.Client{Transport: hookRT}}
	restoreAdapter := handler.SetBuildLifecyclePorts(handler.BuildLifecyclePorts{JobSpecs: f.repo, Versioning: adapter, Locks: f.repo, Builds: f.repo})
	defer restoreAdapter()

	body := fmt.Sprintf(`{"pipeline_rid":%q,"build_branch":"main","output_dataset_rids":[%q]}`, pipelineRID, outRID)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/builds", bytes.NewReader([]byte(body)))
	r.Header.Set("Authorization", "Bearer flow-token")
	r = r.WithContext(authmw.ContextWithClaims(requestCtx, &authmw.Claims{Sub: f.sub, OrgID: f.org}))
	rr := httptest.NewRecorder()
	handler.CreateBuild(rr, r)
	require.NotEqual(t, http.StatusAccepted, rr.Code, "cancelled resolution cannot succeed")

	var state string
	err = f.pool.QueryRow(ctx, `SELECT state FROM builds ORDER BY created_at DESC LIMIT 1`).Scan(&state)
	require.NoError(t, err)
	require.Equal(t, "BUILD_FAILED", state, "failure mark must land on the detached context")
}

// TestBuildFlowPublishDedupRebindsMapping covers A→B→A on real
// Postgres: republishing the ORIGINAL definition returns the original
// rid AND rebinds the branch mapping to it, while the intervening
// definition's job_specs row survives untouched.
func TestBuildFlowPublishDedupRebindsMapping(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	org := uuidPtr(uuid.New())
	sub := uuid.New()
	scopeCtx := handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: org, Subject: sub})

	pipelineRID := "ri.foundry.main.pipeline." + uuid.NewString()
	outRID := "ri.foundry.main.dataset." + uuid.NewString()
	publish := func(payload string) handler.PublishedJobSpec {
		published, err := f.repo.PublishJobSpec(scopeCtx, "TRANSFORM",
			handler.CreateJobSpecRequest{PipelineRID: pipelineRID, BranchName: "main", OutputDatasetRIDs: []string{outRID}, LogicPayload: []byte(payload)}, sub.String())
		require.NoError(t, err)
		return published
	}

	a := publish(`{"v":"A"}`)
	b := publish(`{"v":"B"}`)
	aAgain := publish(`{"v":"A"}`)

	require.Equal(t, a.RID, aAgain.RID, "identical content dedups to the historical rid")
	require.NotEqual(t, a.RID, b.RID)

	var mapped string
	err := f.pool.QueryRow(ctx, `SELECT COALESCE(job_spec_rid, rid) FROM pipeline_job_specs WHERE pipeline_rid=$1 AND branch_name='main'`, pipelineRID).Scan(&mapped)
	require.NoError(t, err)
	require.Equal(t, a.RID, mapped, "mapping rebound to the rid the API returned")

	var specs int
	err = f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_specs WHERE pipeline_rid=$1`, pipelineRID).Scan(&specs)
	require.NoError(t, err)
	require.Equal(t, 2, specs, "A and B definitions both remain for their historical jobs")
}

// TestBuildFlowLedgerUpgradeAppliesNewMigrations proves the filename
// ledger upgrade path: a database that already applied the P1 tenant
// migration set (recorded in pipeline_build_schema_migrations) still
// receives the job_spec_rid column and the pending-txn table from the
// follow-up migrations, and the whole set re-runs cleanly.
func TestBuildFlowLedgerUpgradeAppliesNewMigrations(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)

	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS pipeline_build_schema_migrations (filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)
	require.NoError(t, err)
	entries, err := migrations.FS.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= "20261113110000" {
			continue
		}
		raw, err := migrations.FS.ReadFile(entry.Name())
		require.NoError(t, err)
		_, err = pool.Exec(ctx, string(raw))
		require.NoError(t, err, "pre-upgrade migration %s", entry.Name())
		_, err = pool.Exec(ctx, `INSERT INTO pipeline_build_schema_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`, entry.Name())
		require.NoError(t, err)
	}

	// Pre-upgrade schema lacks the new column.
	var hasCol bool
	err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='pipeline_job_specs' AND column_name='job_spec_rid')`).Scan(&hasCol)
	require.NoError(t, err)
	require.False(t, hasCol)

	require.NoError(t, RunMigrations(ctx, pool))
	require.NoError(t, RunMigrations(ctx, pool), "re-run stays idempotent")

	err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='pipeline_job_specs' AND column_name='job_spec_rid')`).Scan(&hasCol)
	require.NoError(t, err)
	require.True(t, hasCol, "ledger upgrade adds the canonical rid column")
	var hasTable bool
	err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='build_pending_output_txns')`).Scan(&hasTable)
	require.NoError(t, err)
	require.True(t, hasTable, "pending txn table created on upgrade")
}

// TestBuildFlowAbortTransactionTerminalSemantics covers the co-located
// abort predicate: OPEN→ABORTED, already-ABORTED idempotent, COMMITTED
// conflicts, missing rows 404.
func TestBuildFlowAbortTransactionTerminalSemantics(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	org := uuidPtr(uuid.New())
	sub := uuid.New()
	scopeCtx := handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: org, Subject: sub})

	dsRID := "ri.foundry.main.dataset." + uuid.NewString()
	dsID := uuid.New()
	_, err := f.pool.Exec(ctx, `INSERT INTO datasets (id, rid, owner_id, org_id) VALUES ($1,$2,$3,$4)`, dsID, dsRID, sub, org)
	require.NoError(t, err)

	mk := func(status string) string {
		id := uuid.New()
		_, err := f.pool.Exec(ctx, `INSERT INTO dataset_transactions (id, dataset_id, branch_name, tx_type, status) VALUES ($1,$2,'main','UPDATE',$3)`, id, dsID, status)
		require.NoError(t, err)
		return "ri.foundry.main.transaction." + id.String()
	}
	openTx, abortedTx, committedTx := mk("OPEN"), mk("ABORTED"), mk("COMMITTED")

	require.NoError(t, f.repo.AbortTransaction(scopeCtx, dsRID, "main", openTx))
	require.NoError(t, f.repo.AbortTransaction(scopeCtx, dsRID, "main", abortedTx), "already-aborted is idempotent")
	var status string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM dataset_transactions WHERE id=$1`, strings.TrimPrefix(openTx, "ri.foundry.main.transaction.")).Scan(&status))
	require.Equal(t, "ABORTED", status)

	err = f.repo.AbortTransaction(scopeCtx, dsRID, "main", committedTx)
	require.ErrorIs(t, err, handler.ErrTransactionConflict, "COMMITTED can never be overwritten")

	err = f.repo.AbortTransaction(scopeCtx, dsRID, "main", "ri.foundry.main.transaction."+uuid.NewString())
	require.ErrorIs(t, err, handler.ErrTransactionNotFound)
}

// TestBuildFlowPendingRetryClosure is the full P1 positive path with an
// injected transient dataset abort failure: create → jobs non-empty →
// abort (fails, references+locks retained) → abort retry (succeeds,
// references cleared, locks released) → create the same output again.
func TestBuildFlowPendingRetryClosure(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	scopeCtx := handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: f.org, Subject: f.sub})

	outRID := "ri.foundry.main.dataset." + uuid.NewString()
	_, err := f.repo.PublishJobSpec(scopeCtx, "TRANSFORM",
		handler.CreateJobSpecRequest{PipelineRID: f.pipelineRID, BranchName: "main", OutputDatasetRIDs: []string{outRID}, LogicPayload: []byte(`{"t":1}`)}, f.sub.String())
	require.NoError(t, err)

	createBody := fmt.Sprintf(`{"pipeline_rid":%q,"build_branch":"main","output_dataset_rids":[%q]}`, f.pipelineRID, outRID)

	// 1. Create → 202 with non-empty jobs.
	rr := httptest.NewRecorder()
	handler.CreateBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds", createBody, f.org, f.sub))
	require.Equal(t, http.StatusAccepted, rr.Code, rr.Body.String())
	var created struct {
		BuildID uuid.UUID `json:"build_id"`
	}
	require.NoError(t, json.NewDecoder(bytes.NewReader(rr.Body.Bytes())).Decode(&created))
	jobs, err := f.repo.ListJobsForBuildID(scopeCtx, created.BuildID.String())
	require.NoError(t, err)
	require.NotEmpty(t, jobs, "jobs must be persisted for the resolved build")

	var pending int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, created.BuildID).Scan(&pending))
	require.Equal(t, 1, pending, "the opened transaction is durably recorded")

	// 2. Abort with a transient dataset failure → references+locks kept.
	f.dsState.abortFail = true
	rr = httptest.NewRecorder()
	handler.AbortBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds/"+created.BuildID.String()+"/abort", "", f.org, f.sub))
	require.Equal(t, http.StatusBadGateway, rr.Code, "dataset 5xx surfaces as 502 with transaction errors")
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, created.BuildID).Scan(&pending))
	require.Equal(t, 1, pending, "pending reference retained while abort unconfirmed")
	var locks int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, created.BuildID).Scan(&locks))
	require.Equal(t, 1, locks, "lock retained while abort unconfirmed")

	// 3. Retry after recovery → success clears references and locks.
	f.dsState.abortFail = false
	rr = httptest.NewRecorder()
	handler.AbortBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds/"+created.BuildID.String()+"/abort", "", f.org, f.sub))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, created.BuildID).Scan(&pending))
	require.Equal(t, 0, pending)
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, created.BuildID).Scan(&locks))
	require.Equal(t, 0, locks)
	var state string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, created.BuildID).Scan(&state))
	require.Equal(t, "BUILD_ABORTED", state)

	// 4. Same output builds again now that the lock is free.
	rr = httptest.NewRecorder()
	handler.CreateBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds", createBody, f.org, f.sub))
	require.Equal(t, http.StatusAccepted, rr.Code, "freed lock lets the same output build again: %s", rr.Body.String())

	// 5. DryRun on the same definition resolves through the HTTP
	// adapter with the caller's bearer (no side effects on dataset).
	dryBody := fmt.Sprintf(`{"pipeline_rid":%q,"build_branch":"main","output_dataset_rids":[%q]}`, f.pipelineRID, outRID)
	rr = httptest.NewRecorder()
	handler.DryRunResolve(rr, f.scopedRequest(http.MethodPost, "/api/v1/dry-run/resolve", dryBody, f.org, f.sub))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Equal(t, len(f.dsState.opens), 2, "dry-run must not open transactions")
}

// compile-time: models import retained for fixture types.
var _ = models.BuildResolution

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// injectingBuilds wraps the real repository to fail specific writes on
// the Nth call (record / release injection points).
type injectingBuilds struct {
	*Repository
	failRecordOnNth  int
	recordCalls      int
	failReleaseOnNth int
	releaseCalls     int
	failPersist      bool
}

func (i *injectingBuilds) PersistResolvedBuild(ctx context.Context, resolved *models.ResolvedBuild) error {
	if i.failPersist {
		return fmt.Errorf("injected persist failure")
	}
	return i.Repository.PersistResolvedBuild(ctx, resolved)
}

func (i *injectingBuilds) RecordPendingTxn(ctx context.Context, buildID uuid.UUID, datasetRID, branch, txnRID string) error {
	i.recordCalls++
	if i.failRecordOnNth > 0 && i.recordCalls >= i.failRecordOnNth {
		return fmt.Errorf("injected record failure")
	}
	return i.Repository.RecordPendingTxn(ctx, buildID, datasetRID, branch, txnRID)
}

func (i *injectingBuilds) ReleaseLock(ctx context.Context, outputDatasetRID string, buildID uuid.UUID) error {
	i.releaseCalls++
	if i.releaseCalls == i.failReleaseOnNth {
		return fmt.Errorf("injected release failure")
	}
	return i.Repository.ReleaseLock(ctx, outputDatasetRID, buildID)
}

func (f *buildFlowFixture) installInjectingBuilds(inj *injectingBuilds) func() {
	restore := handler.SetBuildLifecyclePorts(handler.BuildLifecyclePorts{JobSpecs: f.repo, Versioning: handler.DatasetHTTPVersioning{BaseURL: f.dataset.URL, Client: f.dataset.Client()}, Locks: inj, Builds: inj})
	return restore
}

func (f *buildFlowFixture) counts(ctx context.Context, t *testing.T, buildID uuid.UUID) (pending, locks int) {
	t.Helper()
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pending))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, buildID).Scan(&locks))
	return
}

func (f *buildFlowFixture) publishSpec(ctx context.Context, t *testing.T, outRID string) {
	t.Helper()
	_, err := f.repo.PublishJobSpec(handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: f.org, Subject: f.sub}), "TRANSFORM",
		handler.CreateJobSpecRequest{PipelineRID: f.pipelineRID, BranchName: "main", OutputDatasetRIDs: []string{outRID}, LogicPayload: []byte(`{"t":1}`)}, f.sub.String())
	require.NoError(t, err)
}

func (f *buildFlowFixture) createBuild(t *testing.T, outRIDs ...string) (*httptest.ResponseRecorder, uuid.UUID) {
	t.Helper()
	quoted := make([]string, 0, len(outRIDs))
	for _, rid := range outRIDs {
		quoted = append(quoted, fmt.Sprintf("%q", rid))
	}
	body := fmt.Sprintf(`{"pipeline_rid":%q,"build_branch":"main","output_dataset_rids":[%s]}`, f.pipelineRID, strings.Join(quoted, ","))
	rr := httptest.NewRecorder()
	handler.CreateBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds", body, f.org, f.sub))
	var created struct {
		BuildID uuid.UUID `json:"build_id"`
	}
	_ = json.NewDecoder(bytes.NewReader(rr.Body.Bytes())).Decode(&created)
	return rr, created.BuildID
}

// TestBuildFlowSecondRecordFailureCompensatesEverything: the second
// output's pending-txn recording fails (both attempts) — the unified
// compensation must abort BOTH remote transactions, release the held lock
// and leave zero pending references; a retry build succeeds.
func TestBuildFlowSecondRecordFailureCompensatesEverything(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	out1 := "ri.foundry.main.dataset." + uuid.NewString()
	out2 := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, out1)
	f.publishSpec(ctx, t, out2)

	inj := &injectingBuilds{Repository: f.repo, failRecordOnNth: 2}
	restore := f.installInjectingBuilds(inj)
	defer restore()

	rr, _ := f.createBuild(t, out1, out2)
	t.Logf("SecondRecord actual code=%d body=%q", rr.Code, rr.Body.String())
	require.Equal(t, http.StatusInternalServerError, rr.Code, "body: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "recording failed")
	buildID := f.latestBuildID(ctx, t)

	require.Equal(t, 2, len(f.dsState.opens), "both transactions were opened")
	require.GreaterOrEqual(t, len(f.dsState.aborts), 2, "both transactions were aborted (the current one may be aborted directly before the unified compensation)")
	pending, locks := f.counts(ctx, t, buildID)
	require.Equal(t, 0, pending)
	require.Equal(t, 0, locks)
	var state string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID).Scan(&state))
	require.Equal(t, "BUILD_FAILED", state)

	restore()
	rr, _ = f.createBuild(t, out1)
	require.Equal(t, http.StatusAccepted, rr.Code, "retry after full compensation succeeds: %s", rr.Body.String())
}

// TestBuildFlowSecondOpenFailureCompensatesFirst: the second output's
// OpenTransaction fails remotely — the first output's transaction must be
// aborted, its lock released and its pending reference cleared.
func TestBuildFlowSecondOpenFailureCompensatesFirst(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	out1 := "ri.foundry.main.dataset." + uuid.NewString()
	out2 := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, out1)
	f.publishSpec(ctx, t, out2)

	f.dsState.openFailOnNth = 2
	rr, buildID := f.createBuild(t, out1, out2)
	require.NotEqual(t, http.StatusAccepted, rr.Code)
	require.Equal(t, 2, len(f.dsState.opens))
	require.Equal(t, 1, len(f.dsState.aborts), "the first, successfully opened transaction is compensated")
	pending, locks := f.counts(ctx, t, buildID)
	require.Equal(t, 0, pending)
	require.Equal(t, 0, locks)
}

// TestBuildFlowReleaseFailureKeepsReference: the lock release inside the
// compensation fails — the pending reference and the lock row stay for
// the retry; the same-build abort retry then converges to zero and the
// output can be rebuilt.
func TestBuildFlowReleaseFailureKeepsReference(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	out1 := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, out1)

	// Force the build into FAILED with a retained pending reference:
	// persist-failure path with an injected release failure.
	inj := &injectingBuilds{Repository: f.repo, failReleaseOnNth: 1, failRecordOnNth: 0}
	inj.failPersist = true
	restore := f.installInjectingBuilds(inj)
	rr, _ := f.createBuild(t, out1)
	require.Equal(t, http.StatusInternalServerError, rr.Code, rr.Body.String())
	buildID := f.latestBuildID(ctx, t)
	t.Logf("ReleaseFailure code=%d body=%q opens=%d aborts=%d", rr.Code, rr.Body.String(), len(f.dsState.opens), len(f.dsState.aborts))
	require.Contains(t, rr.Body.String(), "reference retained")

	pending, locks := f.counts(ctx, t, buildID)
	require.Equal(t, 1, pending, "pending reference kept on release failure")
	require.Equal(t, 1, locks)

	// Failed-build abort retry with a healthy lock port converges.
	restore()
	rr = httptest.NewRecorder()
	handler.AbortBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds/"+buildID.String()+"/abort", "", f.org, f.sub))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	pending, locks = f.counts(ctx, t, buildID)
	require.Equal(t, 0, pending)
	require.Equal(t, 0, locks)

	rr, _ = f.createBuild(t, out1)
	require.Equal(t, http.StatusAccepted, rr.Code, "output rebuilds after retry cleanup: %s", rr.Body.String())
}

// TestBuildFlowCancelAfterOpenCompensatesOnDetachedContext: the caller
// cancels while the SECOND transaction is being opened — the first
// transaction (already opened and recorded) is compensated on the
// bounded detached context and nothing leaks.
func TestBuildFlowCancelAfterOpenCompensatesOnDetachedContext(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	out1 := "ri.foundry.main.dataset." + uuid.NewString()
	out2 := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, out1)
	f.publishSpec(ctx, t, out2)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	var cancelled bool
	hookRT := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/transactions") && strings.Count(req.URL.Path, "/transactions") == 1 {
			if cancelled {
				cancelRequest()
			}
			if len(f.dsState.opens) >= 1 && req.Context().Err() == nil && cancelled == false && len(f.dsState.opens) == 1 {
				cancelled = true
				cancelRequest()
			}
		}
		return http.DefaultTransport.RoundTrip(req)
	})
	adapter := handler.DatasetHTTPVersioning{BaseURL: f.dataset.URL, Client: &http.Client{Transport: hookRT}}
	restore := handler.SetBuildLifecyclePorts(handler.BuildLifecyclePorts{JobSpecs: f.repo, Versioning: adapter, Locks: f.repo, Builds: f.repo})
	defer restore()

	body := fmt.Sprintf(`{"pipeline_rid":%q,"build_branch":"main","output_dataset_rids":["%s","%s"]}`, f.pipelineRID, out1, out2)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/builds", bytes.NewReader([]byte(body)))
	r.Header.Set("Authorization", "Bearer flow-token")
	r = r.WithContext(authmw.ContextWithClaims(requestCtx, &authmw.Claims{Sub: f.sub, OrgID: f.org}))
	rr := httptest.NewRecorder()
	handler.CreateBuild(rr, r)
	require.NotEqual(t, http.StatusAccepted, rr.Code)

	var state string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT state FROM builds ORDER BY created_at DESC LIMIT 1`).Scan(&state))
	require.Equal(t, "BUILD_FAILED", state)

	var pending, locks int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns`).Scan(&pending))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks`).Scan(&locks))
	require.Equal(t, 0, pending, "detached compensation left no pending references")
	require.Equal(t, 0, locks)
	require.GreaterOrEqual(t, len(f.dsState.aborts), 1, "the opened transaction was aborted despite cancellation")
}

// TestBuildFlowJobSpecPublishOwnership: the v1 publish surface enforces
// ownership — the org owner and a same-org admin both succeed with the
// same body; a foreign org is rejected before either table gains a row.
func TestBuildFlowJobSpecPublishOwnership(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	outRID := "ri.foundry.main.dataset." + uuid.NewString()
	body := fmt.Sprintf(`{"pipeline_rid":%q,"branch_name":"main","output_dataset_rids":[%q],"logic_payload":{"x":1}}`, f.pipelineRID, outRID)

	post := func(claims *authmw.Claims) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/job-specs/TRANSFORM", bytes.NewReader([]byte(body)))
		r.Header.Set("Authorization", "Bearer flow-token")
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("kind", "TRANSFORM")
		r = r.WithContext(context.WithValue(authmw.ContextWithClaims(r.Context(), claims), chi.RouteCtxKey, rctx))
		rr := httptest.NewRecorder()
		handler.CreateJobSpecV1(rr, r)
		return rr
	}

	ownerRR := post(&authmw.Claims{Sub: f.sub, OrgID: f.org})
	require.Equal(t, http.StatusCreated, ownerRR.Code, "org owner publishes: %s", ownerRR.Body.String())
	admin := uuid.New()
	adminRR := post(&authmw.Claims{Sub: admin, OrgID: f.org, Roles: []string{"admin"}})
	require.Equal(t, http.StatusCreated, adminRR.Code, "same-org admin publishes: %s", adminRR.Body.String())

	var specsBefore, mappingsBefore int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_specs`).Scan(&specsBefore))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pipeline_job_specs`).Scan(&mappingsBefore))

	foreign := uuidPtr(uuid.New())
	rr := post(&authmw.Claims{Sub: f.sub, OrgID: foreign})
	require.NotEqual(t, http.StatusCreated, rr.Code, "foreign org cannot publish against another tenant's pipeline")

	var specsAfter, mappingsAfter int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_specs`).Scan(&specsAfter))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pipeline_job_specs`).Scan(&mappingsAfter))
	require.Equal(t, specsBefore, specsAfter, "no job_specs row for the foreign publish")
	require.Equal(t, mappingsBefore, mappingsAfter, "no mapping row for the foreign publish")
}

// TestBuildFlowForeignInputSpecRejectedZeroSideEffects covers the
// acceptance alignment: a HISTORICALLY persisted job spec (written
// directly against the real tables, parent pipeline in-domain) whose
// INPUT dataset belongs to a foreign tenant is rejected by CreateBuild's
// preflight with zero new builds, jobs, pending references, locks or
// remote transactions — and the same holds for DryRun inline_specs.
func TestBuildFlowForeignInputSpecRejectedZeroSideEffects(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	foreignInput := "ri.foundry.main.dataset." + uuid.NewString()
	f.dsState.deniedRIDs[foreignInput] = true
	outRID := "ri.foundry.main.dataset." + uuid.NewString()

	// Historical spec (as if published before the foreign binding was
	// moved): real tables, real in-domain parent pipeline.
	_, err := f.repo.PublishJobSpec(handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: f.org, Subject: f.sub}), "TRANSFORM",
		handler.CreateJobSpecRequest{PipelineRID: f.pipelineRID, BranchName: "main", OutputDatasetRIDs: []string{outRID}, Inputs: []models.InputSpec{{DatasetRID: foreignInput}}, LogicPayload: []byte(`{"t":1}`)}, f.sub.String())
	require.NoError(t, err)

	// Sentinel lock from ANOTHER build of the same tenant must be
	// untouched by any of the rejection paths.
	otherBuild := uuid.New()
	_, err = f.pool.Exec(ctx, `INSERT INTO builds (id, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, requested_by, tenant_id) VALUES ($1,'other','main','{}','BUILD_RUNNING','MANUAL',false,$2,$3)`,
		otherBuild, f.sub.String(), f.org)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `INSERT INTO build_input_locks (output_dataset_rid, build_id, transaction_rid) VALUES ($1,$2,'ri.foundry.main.transaction.sentinel')`, outRID, otherBuild)
	require.NoError(t, err)

	counts := func() (builds, jobs, pending, locks int) {
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM builds WHERE id<>$1`, otherBuild).Scan(&builds))
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&jobs))
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns`).Scan(&pending))
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, otherBuild).Scan(&locks))
		return
	}
	b0, j0, p0, l0 := counts()
	require.Equal(t, 1, l0, "sentinel lock present")
	opens0 := len(f.dsState.opens)

	// CreateBuild rejects the foreign input during preflight — before
	// the build row, any transaction or any lock.
	rr, buildID := f.createBuild(t, outRID)
	require.Equal(t, http.StatusForbidden, rr.Code, "foreign input dataset: %s", rr.Body.String())
	require.Equal(t, uuid.Nil, buildID)
	b1, j1, p1, l1 := counts()
	require.Equal(t, b0, b1, "no new build rows")
	require.Equal(t, j0, j1, "no new job rows")
	require.Equal(t, p0, p1, "no pending references")
	require.Equal(t, l0, l1, "the other build's lock untouched")
	require.Equal(t, opens0, len(f.dsState.opens), "no remote transactions opened")

	// DryRun inline_specs with the same foreign input is likewise
	// rejected through the caller-bearing adapter.
	inline := fmt.Sprintf(`{"pipeline_rid":%q,"build_branch":"main","output_dataset_rids":[%q],"inline_specs":[{"rid":"ri.foundry.main.job_spec.inline","pipeline_rid":%q,"branch_name":"main","inputs":[{"dataset_rid":%q}],"output_dataset_rids":[%q],"logic_kind":"TRANSFORM","logic_payload":{"t":1}}]}`,
		f.pipelineRID, outRID, f.pipelineRID, foreignInput, outRID)
	rr = httptest.NewRecorder()
	handler.DryRunResolve(rr, f.scopedRequest(http.MethodPost, "/api/v1/data-integration/pipelines/"+strings.TrimPrefix(f.pipelineRID, "ri.foundry.main.pipeline.")+"/dry-run-resolve", inline, f.org, f.sub))
	// The post-spec visibility gate rejects the foreign input with the
	// real permission answer (strict 403/404), before branch resolution.
	require.Contains(t, []int{http.StatusForbidden, http.StatusNotFound}, rr.Code, "inline foreign input rejected: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), "forbidden")
	require.Contains(t, rr.Body.String(), foreignInput)
	b2, _, p2, l2 := counts()
	require.Equal(t, b0, b2)
	require.Equal(t, p0, p2)
	require.Equal(t, l0, l2)
}

// TestBuildFlowMultiOutputPGStateAssertions upgrades the multi-output
// failure regressions with real Postgres state checks: after the second
// output fails to open, THIS build holds zero pending references and zero
// locks while another build's lock survives untouched.
func TestBuildFlowMultiOutputPGStateAssertions(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	out1 := "ri.foundry.main.dataset." + uuid.NewString()
	out2 := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, out1)
	f.publishSpec(ctx, t, out2)

	otherBuild := uuid.New()
	_, err := f.pool.Exec(ctx, `INSERT INTO builds (id, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, requested_by, tenant_id) VALUES ($1,'other','main','{}','BUILD_RUNNING','MANUAL',false,$2,$3)`,
		otherBuild, f.sub.String(), f.org)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `INSERT INTO build_input_locks (output_dataset_rid, build_id, transaction_rid) VALUES ($1,$2,'ri.foundry.main.transaction.sentinel')`, out2, otherBuild)
	require.NoError(t, err)

	f.dsState.openFailOnNth = 2
	rr, buildID := f.createBuild(t, out1, out2)
	require.NotEqual(t, http.StatusAccepted, rr.Code)

	var pending, ownLocks, otherLocks int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pending))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, buildID).Scan(&ownLocks))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, otherBuild).Scan(&otherLocks))
	require.Equal(t, 0, pending, "own pending references fully compensated")
	require.Equal(t, 0, ownLocks, "own locks fully compensated")
	require.Equal(t, 1, otherLocks, "the other build's lock untouched")
}

// TestBuildFlowRecordAndAbortBothFailKeepsRecoverableHandle: when the
// durable recording fails twice AND the compensating remote abort fails
// too, the error names dataset/branch/transaction for recovery — the
// handle is never silently dropped nor claimed cleaned.
func TestBuildFlowRecordAndAbortBothFailKeepsRecoverableHandle(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	out1 := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, out1)

	inj := &injectingBuilds{Repository: f.repo, failRecordOnNth: 1}
	restore := f.installInjectingBuilds(inj)
	f.dsState.abortFail = true

	// Sentinel lock from another build must survive the whole loop.
	otherBuild := uuid.New()
	_, err := f.pool.Exec(ctx, `INSERT INTO builds (id, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, requested_by, tenant_id) VALUES ($1,'other','main','{}','BUILD_RUNNING','MANUAL',false,$2,$3)`,
		otherBuild, f.sub.String(), f.org)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `INSERT INTO build_input_locks (output_dataset_rid, build_id, transaction_rid) VALUES ($1,$2,'ri.foundry.main.transaction.sentinel')`, "ri.foundry.main.dataset."+uuid.NewString(), otherBuild)
	require.NoError(t, err)

	// 1. Double failure: durable recording fails twice AND the remote
	//    abort fails — the failure detail carries the full recoverable
	//    handle.
	rr, _ := f.createBuild(t, out1)
	require.Equal(t, http.StatusInternalServerError, rr.Code, rr.Body.String())
	body := rr.Body.String()
	require.Contains(t, body, "pending_txn_record_failed")
	require.Contains(t, body, out1, "dataset named for recovery")
	require.Contains(t, body, "ri.foundry.main.transaction.", "transaction rid named for recovery")
	require.Contains(t, body, "manual_cleanup_required")
	buildID := f.latestBuildID(ctx, t)
	var state string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT state, error_message FROM builds WHERE id=$1`, buildID).Scan(&state, &body))
	require.Equal(t, "BUILD_FAILED", state)
	require.Contains(t, body, "manual_cleanup_required dataset="+out1+" branch=main txn=ri.foundry.main.transaction.", "persisted failure carries the machine-readable handle")

	var pendingRows int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pendingRows))

	// 2. First FAILED abort retry while the remote abort is still
	//    broken: the handle is re-tracked durably but the abort fails —
	//    the retry must NOT report success, and the reference stays.
	f.dsState.abortFail = true
	rr = httptest.NewRecorder()
	handler.AbortBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds/"+buildID.String()+"/abort", "", f.org, f.sub))
	require.NotEqual(t, http.StatusOK, rr.Code, "first retry cannot claim success while the remote abort still fails: %s", rr.Body.String())
	require.Contains(t, rr.Body.String(), out1, "retry answer names the pending handle")
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pendingRows))
	require.Equal(t, 1, pendingRows, "re-tracked handle stays for the next retry")

	// 3. Recovery: remote abort heals; the manual retry converges —
	//    remote transaction reaches terminal ABORTED, pending and this
	//    build's lock drain to zero, the other build's lock survives.
	f.dsState.abortFail = false
	rr = httptest.NewRecorder()
	handler.AbortBuild(rr, f.scopedRequest(http.MethodPost, "/api/v1/builds/"+buildID.String()+"/abort", "", f.org, f.sub))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	handles := 0
	for txnRID, tx := range f.dsState.txns {
		if tx.DatasetRID != out1 {
			continue
		}
		handles++
		status, ok := f.dsState.txnStatus(txnRID)
		require.True(t, ok, "handle tracked")
		require.Equal(t, "ABORTED", status, "remote transaction %s reached terminal ABORTED", txnRID)
	}
	require.Equal(t, 1, handles, "exactly one remote transaction existed for the failed output")

	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pendingRows))
	require.Equal(t, 0, pendingRows)
	var ownLocks, otherLocks int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, buildID).Scan(&ownLocks))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, otherBuild).Scan(&otherLocks))
	require.Equal(t, 0, ownLocks)
	require.Equal(t, 1, otherLocks, "the other build's lock untouched")

	restore()
}

func flowRIDFromPath(path string) string {
	const prefix = "/v1/datasets/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	if idx := strings.Index(rest, "/"); idx >= 0 {
		return rest[:idx]
	}
	return rest
}

func (f *buildFlowFixture) latestBuildID(ctx context.Context, t *testing.T) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT id FROM builds ORDER BY created_at DESC LIMIT 1`).Scan(&id))
	return id
}

// TestBuildFlowEmitJobLogTenantGating is the PG twin of the q3v9 finding:
// POST /v1/jobs/{rid}/logs must answer 404 (zero rows written) for a
// cross-org admin and foreign personal users, succeed for the same-domain
// owner/admin, and 404 for unknown RIDs — only real DB failures are 500.
func TestBuildFlowEmitJobLogTenantGating(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)

	// Real build + job in the fixture's org, directly through the repo.
	scope := &handler.TenantScope{TenantID: f.org, Subject: f.sub}
	scopeCtx := handler.ContextWithTenantScope(ctx, scope)
	outRID := "ri.foundry.main.dataset." + uuid.NewString()
	f.publishSpec(ctx, t, outRID)
	buildID := uuid.New()
	require.NoError(t, f.repo.OpenBuild(scopeCtx, resolveArgsFor(f.pipelineRID, f.sub), buildID))
	jobID := uuid.New()
	var jobRID string
	require.NoError(t, f.pool.QueryRow(ctx, `INSERT INTO jobs (id, build_id, job_spec_rid, state) VALUES ($1,$2,'ri.foundry.main.job_spec.flow','WAITING') RETURNING rid`, jobID, buildID).Scan(&jobRID))

	restoreLogs := handler.SetJobLogService(&livellogs.Service{Store: f.repo, Subscriber: livellogs.NewMemoryService()})
	defer restoreLogs()

	emit := func(claims *authmw.Claims, rid string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+rid+"/logs", bytes.NewReader([]byte(`{"level":"INFO","message":"smoke line"}`)))
		r.Header.Set("Authorization", "Bearer flow-token")
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("rid", rid)
		r = r.WithContext(context.WithValue(authmw.ContextWithClaims(r.Context(), claims), chi.RouteCtxKey, rctx))
		rr := httptest.NewRecorder()
		handler.EmitJobLogV1(rr, r)
		return rr
	}
	logCount := func() (n int) {
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_logs WHERE job_id=$1`, jobID).Scan(&n))
		return
	}

	adminSameOrg := uuid.New()
	require.Equal(t, http.StatusOK, emit(&authmw.Claims{Sub: adminSameOrg, OrgID: f.org, Roles: []string{"admin"}}, jobRID).Code, "same-domain admin writes")
	require.Equal(t, 1, logCount())

	orgB := uuidPtr(uuid.New())
	require.Equal(t, http.StatusNotFound, emit(&authmw.Claims{Sub: f.sub, OrgID: orgB, Roles: []string{"admin"}}, jobRID).Code, "cross-org admin (q3v9 build_isolation case) is 404")
	require.Equal(t, 1, logCount(), "cross-org admin appended nothing")

	require.Equal(t, http.StatusNotFound, emit(&authmw.Claims{Sub: uuid.New()}, jobRID).Code, "foreign personal user is 404")
	require.Equal(t, 1, logCount())

	require.Equal(t, http.StatusNotFound, emit(&authmw.Claims{Sub: f.sub, OrgID: f.org}, "ri.foundry.main.job."+uuid.NewString()).Code, "unknown rid is 404")
	require.Equal(t, 1, logCount())
}
