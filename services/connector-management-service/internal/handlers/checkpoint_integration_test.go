//go:build integration

// P2.5 checkpoint — real-handler + real-PostgreSQL integration regressions
// (PG-test track, exclusive file per root implementation supplement
// 2026-11-14: the repo integration file lives in package repo and cannot
// import handlers without an import cycle, so the handler-side proof owns
// this separate integration-tagged file. Primary's non-PG handler tests are
// untouched).
//
// What this file proves that the repo package cannot (pg-test-plan §4 T10):
// with the REAL executeSnapshotSync driving a captured incremental run, the
// executor holds NO database row locks while the publish port call is in
// flight (root: prove by a bounded SELECT ... FOR UPDATE NOWAIT sweep over
// Source→Def→Checkpoint→Run from a second transaction — never by counting
// pg_locks), and the injected-port acknowledgement contract decides the
// outcome: a usable ACK (exact target + COMMITTED + non-nil txn) advances
// and releases atomically; a timeout after dispatch fails the run and
// RETAINS the owner (P26).
//
// The upstream REST source is an in-process httptest server (the test
// binary itself — no external HTTP service), and the dataset publish port
// is the injectable Handlers.DatasetVersioning double. COMPILE-ONLY phase:
// nothing here has been executed; real PG execution waits for root's
// release via run_pg_isolated_p25.sh.
package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// p25hBoot boots a fresh migrated database and returns the DSN-bearing repo
// (same contract as the repo package's integration boot: CONNECTOR_DB_DSN
// when set — the isolated runner always sets it — otherwise the shared
// testcontainers harness).
func p25hBoot(t *testing.T) *repo.Repo {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("CONNECTOR_DB_DSN")
	var pool *pgxpool.Pool
	if dsn != "" {
		admin, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect CONNECTOR_DB_DSN: %v", err)
		}
		if err := admin.Ping(ctx); err != nil {
			t.Fatalf("ping CONNECTOR_DB_DSN: %v", err)
		}
		dbName := "p25h_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
			t.Fatalf("create fresh database: %v", err)
		}
		t.Cleanup(func() {
			_, _ = admin.Exec(ctx, "DROP DATABASE "+dbName)
			admin.Close()
		})
		idx := strings.LastIndex(dsn, "/")
		fresh := dsn[:idx+1] + dbName
		pool, err = pgxpool.New(ctx, fresh)
		if err != nil {
			t.Fatalf("connect fresh database: %v", err)
		}
		t.Cleanup(pool.Close)
	} else {
		harness := testingx.BootPostgres(ctx, t)
		t.Cleanup(func() { harness.Stop(ctx) })
		pool = harness.Pool
	}
	require.NoError(t, repo.Migrate(ctx, pool))
	return &repo.Repo{Pool: pool}
}

// p25ServingSource is a one-page incremental REST source: every page answers
// the configured completion checkpoint and an EMPTY next_cursor, so the
// single final response carries the completed token.
func p25ServingSource(t *testing.T, completedToken string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"items":[{"id":1,"note":"p25"}],"next_cursor":"","completed":%q}`, completedToken)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// p25LoopingSource is a hostile-but-legal incremental source: every page
// answers nonempty records with the SAME next_cursor (a pagination loop),
// so the fetch fails with the repeated-cursor error whose wording must
// never echo the raw opaque token (review-source-round1 #2).
func p25LoopingSource(t *testing.T, token string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"items":[{"id":1}],"next_cursor":%q,"completed":"never-reached"}`, token)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// p25ParkedPort is the injectable dataset port: GetDataset echoes the
// requested target (the precheck passes); CommitOutput signals that the
// publish call is IN FLIGHT and then parks until the test answers.
type p25ParkedPort struct {
	parkedOnce sync.Once
	parked     chan struct{}
	answer     chan p25PortAnswer
}

type p25PortAnswer struct {
	result *cmruntime.DatasetCommitResult
	err    error
}

func newP25ParkedPort() *p25ParkedPort {
	return &p25ParkedPort{parked: make(chan struct{}), answer: make(chan p25PortAnswer)}
}

func (p *p25ParkedPort) CommitOutput(ctx context.Context, req cmruntime.DatasetCommitRequest, authorization string) (*cmruntime.DatasetCommitResult, error) {
	p.parkedOnce.Do(func() { close(p.parked) })
	select {
	case ans := <-p.answer:
		if ans.err != nil {
			return nil, ans.err
		}
		res := *ans.result
		if res.DatasetID == uuid.Nil {
			res.DatasetID = req.DatasetID
		}
		return &res, nil
	case <-ctx.Done():
		return nil, &cmruntime.DependencyError{Op: "commit_output", Timeout: true}
	}
}

func (p *p25ParkedPort) GetDataset(ctx context.Context, id uuid.UUID, authorization string) (*cmruntime.DatasetInfo, error) {
	return &cmruntime.DatasetInfo{ID: id, Name: "p25-target"}, nil
}

// p25HandlerFixture boots the database, the serving source and one captured
// pending incremental run through the PUBLIC entrypoints (EnqueueSyncRun now
// routes incremental admission), then claims it.
func p25HandlerFixture(t *testing.T, completedToken string) (*repo.Repo, *pgxpool.Pool, repo.Tenant, uuid.UUID, uuid.UUID, *repo.LeaseClaim, *models.SyncJob, *models.Connection) {
	t.Helper()
	ctx := context.Background()
	r := p25hBoot(t)
	srv := p25ServingSource(t, completedToken)

	tenant := repo.Tenant{OwnerID: uuid.New()}
	org := uuid.New()
	tenant.OrgID = &org
	config := fmt.Sprintf(`{
		"base_url": %q,
		"auth": {"type": "none"},
		"runtime": {"worker": "foundry"},
		"permissions": {"syncable": true},
		"egress": {"allow_insecure_http": true},
		"sync": {
			"resource_path": "/items",
			"records_path": "items",
			"pagination": {"type": "cursor", "cursor_param": "cursor_token", "next_cursor_path": "next_cursor"},
			"incremental": {"enabled": true, "request_param": "since_token", "completed_checkpoint_path": "completed"}
		}
	}`, srv.URL)
	conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
		Name: "p25-handler-source", ConnectorType: "rest_api", Config: []byte(config),
	}, tenant)
	require.NoError(t, err)
	require.NotNil(t, conn)
	target := uuid.New()
	job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
		SourceID: conn.ID, OutputDatasetID: target}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job)

	run, err := r.EnqueueSyncRun(ctx, job.ID, conn.ID, tenant, uuid.New())
	require.NoError(t, err, "public admission accepts the incremental def")
	require.NotNil(t, run)

	claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "p25h-holder", "p25h-token", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim, "captured run claims through the routed path")
	require.Equal(t, repo.CPModeCursor, claim.Run.CPMode)

	fullConn, err := r.GetConnectionForTenant(ctx, conn.ID, tenant)
	require.NoError(t, err)
	require.NotNil(t, fullConn)
	return r, r.Pool, tenant, conn.ID, job.ID, claim, job, fullConn
}

// p25hNowaitSweep proves NO row locks are held across the parked publish
// call: a bounded second transaction takes Source→Def→Checkpoint→Run with
// FOR UPDATE NOWAIT — every statement must succeed — then rolls back.
func p25hNowaitSweep(t *testing.T, pool *pgxpool.Pool, sourceID, defID, runID uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.WithoutCancel(ctx))
	for _, step := range []struct {
		sql  string
		args []any
	}{
		{`SELECT id FROM connections WHERE id = $1 FOR UPDATE NOWAIT`, []any{sourceID}},
		{`SELECT id FROM batch_sync_defs WHERE id = $1 FOR UPDATE NOWAIT`, []any{defID}},
		{`SELECT sync_def_id FROM sync_def_checkpoints WHERE sync_def_id = $1 FOR UPDATE NOWAIT`, []any{defID}},
		{`SELECT id FROM sync_runs WHERE id = $1 FOR UPDATE NOWAIT`, []any{runID}},
	} {
		var locked uuid.UUID
		require.NoError(t, tx.QueryRow(ctx, step.sql, step.args...).Scan(&locked),
			"the executor must hold NO lock on %s while the publish call is parked", step.sql)
	}
	require.NoError(t, tx.Rollback(ctx))
}

type p25ExecResult struct {
	run *models.SyncRun
	err error
}

// TestPGCheckpointNoDBLockAcrossPublish drives the REAL executor for a
// captured incremental run with the injectable publish port:
//
//   - while CommitOutput is parked, a NOWAIT sweep over
//     Source→Def→Checkpoint→Run succeeds (no DB lock across the publish
//     call — root round-1 #4's method, never a pg_locks count);
//   - released with a usable ACK (exact target + COMMITTED + non-nil txn)
//     the run succeeds and the checkpoint advances+releases atomically;
//   - a timeout AFTER dispatch closes the run as failed and RETAINS the
//     owner (unknown-after-dispatch, P26).
func TestPGCheckpointNoDBLockAcrossPublish(t *testing.T) {
	t.Run("parked_publish_holds_no_db_locks_then_usable_ack_advances", func(t *testing.T) {
		r, pool, tenant, sourceID, defID, claim, job, conn := p25HandlerFixture(t, "cp-final-A")

		port := newP25ParkedPort()
		h := &Handlers{
			Repo:              r,
			DatasetVersioning: port,
			Config: RuntimeConfig{
				AllowPrivateNetworkEgress: true,
				AllowedEgressHosts:        []string{"127.0.0.1"},
			},
		}

		done := make(chan p25ExecResult, 1)
		go func() {
			run, err := h.executeSnapshotSync(context.Background(), "Bearer p25-test",
				tenant, claim, job, conn, false)
			done <- p25ExecResult{run: run, err: err}
		}()

		select {
		case <-port.parked:
		case <-time.After(30 * time.Second):
			t.Fatal("executor never reached the parked publish call")
		}

		// The core proof: nothing is locked while the publish is in flight.
		p25hNowaitSweep(t, pool, sourceID, defID, claim.Run.ID)

		txn := uuid.New()
		port.answer <- p25PortAnswer{result: &cmruntime.DatasetCommitResult{
			DatasetID: job.OutputDatasetID, TransactionID: txn, Status: "COMMITTED",
		}}

		select {
		case res := <-done:
			require.NoError(t, res.err)
			require.NotNil(t, res.run)
			assert.Equal(t, "succeeded", res.run.Status)
		case <-time.After(30 * time.Second):
			t.Fatal("executor did not finish after the usable ACK")
		}

		var value *string
		var epoch int
		var owner *uuid.UUID
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT checkpoint_value, confirmed_epoch, active_run_id
			   FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).
			Scan(&value, &epoch, &owner))
		require.NotNil(t, value)
		assert.Equal(t, "cp-final-A", *value)
		assert.Equal(t, 1, epoch)
		assert.Nil(t, owner, "owner released by the atomic advance")
	})

	t.Run("ack_timeout_after_dispatch_fails_and_retains_owner", func(t *testing.T) {
		r, pool, tenant, _, defID, claim, job, conn := p25HandlerFixture(t, "cp-final-B")

		port := newP25ParkedPort()
		h := &Handlers{
			Repo:              r,
			DatasetVersioning: port,
			Config: RuntimeConfig{
				AllowPrivateNetworkEgress: true,
				AllowedEgressHosts:        []string{"127.0.0.1"},
			},
		}
		_ = tenant

		done := make(chan p25ExecResult, 1)
		go func() {
			run, err := h.executeSnapshotSync(context.Background(), "Bearer p25-test",
				tenant, claim, job, conn, false)
			done <- p25ExecResult{run: run, err: err}
		}()
		select {
		case <-port.parked:
		case <-time.After(30 * time.Second):
			t.Fatal("executor never reached the parked publish call")
		}
		port.answer <- p25PortAnswer{err: &cmruntime.DependencyError{Op: "commit_output", Timeout: true}}

		select {
		case res := <-done:
			// A post-dispatch unknown is an honest failed run (never a
			// fabricated success); the DB residue is asserted below.
			if res.err != nil {
				t.Logf("executor returned error after unknown dispatch: %v", res.err)
			}
			if res.run != nil {
				assert.Equal(t, "failed", res.run.Status)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("executor did not finish after the timed-out ACK")
		}

		var status string
		var epoch int
		var owner *uuid.UUID
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT r.status, cp.confirmed_epoch, cp.active_run_id
			   FROM sync_def_checkpoints cp
			   JOIN sync_runs r ON r.id = cp.active_run_id
			  WHERE cp.sync_def_id = $1`, defID).Scan(&status, &epoch, &owner))
		assert.Equal(t, "failed", status, "post-dispatch unknown closes as failed")
		assert.Equal(t, 0, epoch, "the data checkpoint never advances")
		require.NotNil(t, owner, "the owner is retained (requires_reconciliation)")
		assert.Equal(t, claim.Run.ID, *owner)
	})

	t.Run("context_death_during_dispatch_fails_and_retains_owner", func(t *testing.T) {
		// Contract §4.1/R5 at the handler level: once the publish is
		// dispatched (the port call is IN FLIGHT), a dying context is an
		// UNKNOWN outcome — honest failed run, owner RETAINED, data
		// checkpoint untouched (requires_reconciliation, never aborted,
		// never released).
		r, pool, tenant, _, defID, claim, job, conn := p25HandlerFixture(t, "cp-final-C")

		port := newP25ParkedPort()
		h := &Handlers{
			Repo:              r,
			DatasetVersioning: port,
			Config: RuntimeConfig{
				AllowPrivateNetworkEgress: true,
				AllowedEgressHosts:        []string{"127.0.0.1"},
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan p25ExecResult, 1)
		go func() {
			run, err := h.executeSnapshotSync(ctx, "Bearer p25-test",
				tenant, claim, job, conn, false)
			done <- p25ExecResult{run: run, err: err}
		}()
		select {
		case <-port.parked:
		case <-time.After(30 * time.Second):
			t.Fatal("executor never reached the parked publish call")
		}
		cancel() // the request dies while the publish is in flight

		select {
		case res := <-done:
			if res.err != nil {
				t.Logf("executor returned error after context death: %v", res.err)
			}
			if res.run != nil {
				assert.Equal(t, "failed", res.run.Status)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("executor did not finish after the context death")
		}

		var status string
		var epoch int
		var owner *uuid.UUID
		var value *string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT r.status, cp.confirmed_epoch, cp.active_run_id, cp.checkpoint_value
			   FROM sync_def_checkpoints cp
			   JOIN sync_runs r ON r.id = cp.active_run_id
			  WHERE cp.sync_def_id = $1`, defID).Scan(&status, &epoch, &owner, &value))
		assert.Equal(t, "failed", status, "dispatch-unknown context death closes as failed")
		assert.Equal(t, 0, epoch)
		assert.Nil(t, value)
		require.NotNil(t, owner, "the owner is retained (requires_reconciliation)")
		assert.Equal(t, claim.Run.ID, *owner)

		view, err := r.GetCheckpointView(context.Background(), defID, tenant)
		require.NoError(t, err)
		require.NotNil(t, view)
		require.NotNil(t, view.Owner)
		assert.Equal(t, "requires_reconciliation", view.Owner.BlockedReason)
	})

	t.Run("repeated_cursor_error_never_leaks_the_raw_token", func(t *testing.T) {
		// review-source-round1 #2: the loop error's wording must never echo
		// the opaque token into Run.error or any public surface; the failed
		// run is provably pre-gate (zero dispatch), so the owner releases
		// and the data checkpoint stays untouched.
		const secret = "SECRETTOKEN-p25-loop-7f3a"
		r := p25hBoot(t)
		pool := r.Pool
		ctx := context.Background()
		srv := p25LoopingSource(t, secret)
		tenant := repo.Tenant{OwnerID: uuid.New()}
		org := uuid.New()
		tenant.OrgID = &org
		config := fmt.Sprintf(`{
			"base_url": %q,
			"auth": {"type": "none"},
			"runtime": {"worker": "foundry"},
			"permissions": {"syncable": true},
			"egress": {"allow_insecure_http": true},
			"sync": {
				"resource_path": "/items",
				"records_path": "items",
				"pagination": {"type": "cursor", "cursor_param": "cursor_token", "next_cursor_path": "next_cursor"},
				"incremental": {"enabled": true, "request_param": "since_token", "completed_checkpoint_path": "completed"}
			}
		}`, srv.URL)
		conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "loop-source", ConnectorType: "rest_api", Config: []byte(config),
		}, tenant)
		require.NoError(t, err)
		require.NotNil(t, conn)
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job)
		run, err := r.EnqueueSyncRun(ctx, job.ID, conn.ID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run)
		claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "loop-holder", "loop-token", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		fullConn, err := r.GetConnectionForTenant(ctx, conn.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, fullConn)

		h := &Handlers{
			Repo: r,
			Config: RuntimeConfig{
				AllowPrivateNetworkEgress: true,
				AllowedEgressHosts:        []string{"127.0.0.1"},
			},
		}
		done := make(chan p25ExecResult, 1)
		go func() {
			out, execErr := h.executeSnapshotSync(context.Background(), "Bearer p25-test",
				tenant, claim, job, fullConn, false)
			done <- p25ExecResult{run: out, err: execErr}
		}()
		select {
		case res := <-done:
			// The fetch failure is honestly terminal-ized: when the routed
			// failure write succeeds the executor returns the FAILED run
			// with a NIL error — either shape is acceptable; the DB residue
			// below carries the proof (first-run finding, fixture fix).
			if res.run != nil {
				assert.Equal(t, "failed", res.run.Status)
			} else {
				require.Error(t, res.err, "without a run the failure must surface as an error")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("executor did not finish on the looping upstream")
		}

		var runErr *string
		var status string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT error, status FROM sync_runs WHERE id = $1`, claim.Run.ID).Scan(&runErr, &status))
		assert.Equal(t, "failed", status, "pre-gate failure closes the run")
		require.NotNil(t, runErr, "the failure cause is persisted")
		assert.NotContains(t, *runErr, secret,
			"the raw opaque token never reaches Run.error")

		var epoch int
		var owner *uuid.UUID
		var value *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT confirmed_epoch, active_run_id, checkpoint_value
			   FROM sync_def_checkpoints WHERE sync_def_id = $1`, job.ID).
			Scan(&epoch, &owner, &value))
		assert.Equal(t, 0, epoch, "no advance on a pre-gate failure")
		assert.Nil(t, value)
		assert.Nil(t, owner, "provably zero dispatch ⇒ the owner releases")
	})
}
