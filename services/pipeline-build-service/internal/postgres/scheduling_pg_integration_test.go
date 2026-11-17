//go:build integration

package postgres

// Real-PostgreSQL integration tests for the T8 scheduling consumer
// state machine (WP3.2). These run only when PIPELINE_DB_DSN points at
// a dedicated container (ofwp32-* prefix per the T8 decision §5); the
// real schema and real concurrency are the source of truth, mocks are
// not. Timing is created ONLY through controlled SQL (rewinding lease
// expiry), never sleeps.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

// seedSpecRID is the job identity the fixtures' sealed expectation
// refers to (jobs.job_spec_rid comparison target).
const seedSpecRID = "ri.foundry.main.job_spec.seed"

type schedulingFixture struct {
	pool   *pgxpool.Pool
	repo   *Repository
	params AdmitParams
	grant  SchedulingGrant
}

func seedScheduling(ctx context.Context, t *testing.T, kind scheduleevents.TenantKind) *schedulingFixture {
	t.Helper()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)

	subj := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	tenant := scheduleevents.TenantRef{Kind: kind, ID: uuid.MustParse("33333333-3333-3333-3333-333333333333")}
	if kind == scheduleevents.TenantKindPersonal {
		tenant = scheduleevents.TenantRef{Kind: kind, ID: subj}
	}
	scheduledFor := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	event := scheduleevents.DeriveEventID(tenant, "nightly", 1, scheduledFor, scheduleevents.EventTypeTriggered)

	org := tenant.ID
	var orgPtr *uuid.UUID
	if kind == scheduleevents.TenantKindOrg {
		orgPtr = &org
	}
	g := SchedulingGrant{
		TenantKind: string(kind), TenantID: tenant.ID, OrgID: orgPtr,
		Subject: subj, ScheduleKey: "nightly",
		PipelineRID: "ri.foundry.main.pipeline.11111111-1111-1111-1111-111111111111",
		ExpiresAt: time.Now().Add(2 * time.Hour), TokenCiphertext: []byte("ct"),
		CreatedBy: subj,
	}
	grantID, generation, err := repo.UpsertGrant(ctx, g)
	require.NoError(t, err)
	require.Equal(t, 1, generation)

	// Definition binding: admission requires the CURRENT
	// pipeline_outbox_v1 binding to match the event tuple (revision 1).
	pipelineRID := g.PipelineRID
	_, err = pool.Exec(ctx,
		`INSERT INTO schedules.definitions
		   (id, name, cron_expr, cron_flavor, time_zone, enabled, topic,
		    payload_template, next_run_at, dispatch_mode, tenant_kind, tenant_id,
		    schedule_key, definition_revision, pipeline_rid, build_branch, grant_ref)
		 VALUES ($1,'fixture','0 2 * * *','unix5','UTC',true,'pipeline.builds.v1','{}'::jsonb,
		         NOW() + interval '1 hour','pipeline_outbox_v1',$2,$3,'nightly',1,$4,'main',$5)
		 ON CONFLICT (id) DO UPDATE SET definition_revision=1, pipeline_rid=EXCLUDED.pipeline_rid,
		   build_branch='main', grant_ref=EXCLUDED.grant_ref, enabled=true, dispatch_mode='pipeline_outbox_v1'`,
		uuid.NewSHA1(uuid.NameSpaceOID, []byte("fixture-def")), string(kind), g.TenantID, pipelineRID, grantID)
	require.NoError(t, err)

	return &schedulingFixture{
		pool: pool, repo: repo,
		params: AdmitParams{
			EventID: event, Tenant: tenant, ScheduleKey: "nightly",
			DefinitionRevision: 1, ScheduledFor: scheduledFor,
			PipelineRID: g.PipelineRID, BuildBranch: "main",
			GrantRef: grantID, GrantGeneration: generation,
			BuildID: scheduleevents.DeriveBuildID(event),
			// Sealed expectations (the resolver-equivalent for these
			// fixtures): one node (job identity = seedSpecRID, the
			// agreed job_spec_rid mapping) publishing dataset .9.
			ExpectedOutputs:      []ExpectedOutput{{NodeID: "node-1", DatasetRID: "ri.foundry.main.dataset.9"}},
			ExpectedOutputsSealed: true,
			ExpectedNodes:        []string{seedSpecRID},
			ExpectedNodesSealed:  true,
		},
		grant: g,
	}
}

func (f *schedulingFixture) admit(ctx context.Context, t *testing.T) *AdmissionResult {
	t.Helper()
	res, err := f.repo.AdmitDispatch(ctx, f.params, nil)
	require.NoError(t, err)
	require.Equal(t, AdmitAdmitted, res.Outcome)
	return res
}

func (f *schedulingFixture) dispatchState(ctx context.Context, t *testing.T) (string, *int64) {
	t.Helper()
	var state string
	var fence *int64
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT state, fence_token FROM scheduling.dispatches WHERE event_id=$1`, f.params.EventID).Scan(&state, &fence))
	return state, fence
}

func (f *schedulingFixture) activityExists(ctx context.Context, t *testing.T) bool {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM scheduling.pipeline_activity WHERE active_dispatch_id=$1`, f.params.EventID).Scan(&n))
	return n > 0
}

func (f *schedulingFixture) buildState(ctx context.Context, t *testing.T) string {
	t.Helper()
	var state string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT state FROM builds WHERE id=$1`, f.params.BuildID).Scan(&state))
	return state
}

func (f *schedulingFixture) seedCompletedJob(ctx context.Context, t *testing.T) {
	t.Helper()
	_, err := f.pool.Exec(ctx,
		`INSERT INTO jobs (id, build_id, job_spec_rid, state)
		 VALUES ($1,$2,$3,'COMPLETED')`,
		uuid.New(), f.params.BuildID, seedSpecRID)
	require.NoError(t, err)
}

func TestPG_LeaseExpiryTakeoverChain(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)

	fenceA, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), fenceA)

	// Controlled SQL rewinds the lease into the past — no sleeps.
	_, err = f.pool.Exec(ctx,
		`UPDATE scheduling.dispatches SET lease_expires_at = NOW() - interval '1 minute' WHERE event_id=$1`,
		f.params.EventID)
	require.NoError(t, err)

	// Lease expired but nobody took over yet: the old worker's finish is
	// STILL rejected (stale lease, not just stale fence).
	require.ErrorIs(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fenceA, DispatchStateSucceeded, "", nil),
		ErrStaleFence)

	// The expired worker cannot renew itself.
	require.ErrorIs(t, f.repo.HeartbeatDispatch(ctx, f.params.EventID, "worker-a", fenceA, 30*time.Second), ErrStaleFence)

	// A recovery claim by worker-b takes over with a NEW fence.
	fenceB, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-b", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, fenceA+1, fenceB)

	// The old worker's writes all fail closed with zero side effects.
	key := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")
	require.ErrorIs(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fenceA, key, "node-1", "ri.foundry.main.dataset.9", []byte(`{}`)), ErrStaleFence)
	var receipts int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduling.publish_receipts`).Scan(&receipts))
	require.Equal(t, 0, receipts, "stale worker must leave no receipt rows")

	f.seedCompletedJob(ctx, t)
	staleExtra := func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO outbox.events (event_id, aggregate, aggregate_id, payload, headers, topic)
			VALUES (gen_random_uuid(),'stale-marker','x','{}'::jsonb,'{}'::jsonb,'__marker__')`)
		return err
	}
	require.ErrorIs(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fenceA, DispatchStateSucceeded, "", staleExtra),
		ErrStaleFence)
	var markers int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox.events WHERE topic='__marker__'`).Scan(&markers))
	require.Equal(t, 0, markers, "stale finish must leave no outbox side effects")
	curState, _ := f.dispatchState(ctx, t)
	require.Equal(t, "executing", curState, "stale finish must change nothing")
	// Build untouched by the stale finish (still BUILD_RESOLUTION from
	// admission; the consumer sets BUILD_RUNNING when execution starts).
	require.Equal(t, "BUILD_RESOLUTION", f.buildState(ctx, t))

	// worker-b owns the new fence: activity fence synced, then it records
	// the pending receipt (the stale worker left none), confirms the
	// usableACK, and finishes.
	var activityFence int64
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT fence_token FROM scheduling.pipeline_activity WHERE active_dispatch_id=$1`, f.params.EventID).Scan(&activityFence))
	require.Equal(t, fenceB, activityFence, "activity fence must track the live owner")
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-b", fenceB, key, "node-1", "ri.foundry.main.dataset.9", []byte(`{}`)))
	commit := []byte(`{"Status":"COMMITTED","DatasetID":"9"}`)
	require.NoError(t, f.repo.MarkReceiptCommitted(ctx, f.params.EventID, "worker-b", fenceB, key, commit))

	// The new worker finishes cleanly: receipt committed, job completed.
	require.NoError(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-b", fenceB, DispatchStateSucceeded, "", nil))
	finalState, _ := f.dispatchState(ctx, t)
	require.Equal(t, DispatchStateSucceeded, finalState)
	require.Equal(t, "BUILD_COMPLETED", f.buildState(ctx, t))
	require.False(t, f.activityExists(ctx, t), "terminal + all receipts committed releases the slot")
}

func TestPG_ReceiptFinishSerialization(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)

	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	key := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")

	// A pending receipt (POST decided, usableACK not yet confirmed).
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fence, key, "node-1", "ri.foundry.main.dataset.9", []byte(`{"rows":1}`)))

	// Success is forbidden while a receipt is pending — even with every
	// job completed; the empty-receipt shortcut is not a shortcut.
	f.seedCompletedJob(ctx, t)
	err = f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil)
	require.ErrorIs(t, err, ErrFinishIncomplete)
	require.True(t, f.activityExists(ctx, t), "incomplete finish must retain the slot")

	// The dispatch stays executing: the worker must reconcile the
	// publish (mark committed) and only then finish.
	state, _ := f.dispatchState(ctx, t)
	require.Equal(t, "executing", state)

	require.NoError(t, f.repo.MarkReceiptCommitted(ctx, f.params.EventID, "worker-a", fence, key, []byte(`{"Status":"COMMITTED","DatasetID":"9"}`)))
	require.NoError(t, f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil))
	require.False(t, f.activityExists(ctx, t))

	// A LATE receipt write after the finish is rejected: the lease is
	// gone (finish cleared it), the fence no longer owns anything.
	require.ErrorIs(t,
		f.repo.MarkReceiptUnknown(ctx, f.params.EventID, "worker-a", fence, key, "late result"),
		ErrStaleFence)
	var receiptState string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT state FROM scheduling.publish_receipts WHERE key=$1`, key).Scan(&receiptState))
	require.Equal(t, "committed", receiptState, "committed never downgrades, not even by a late writer")
}

func TestPG_AdmittedNeverClaimedIsRecoverable(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindPersonal)
	f.admit(ctx, t)

	// Crash window: admission committed (event acked) but never claimed.
	stale, err := f.repo.ExpireStaleExecutions(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, stale, 1, "admitted with NULL lease must appear in the recovery scan")
	require.Equal(t, f.params.EventID, stale[0].EventID)

	// Recovery re-claims the ORIGINAL dispatch (no new opportunity).
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "recovery", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), fence)

	// Personal-domain build row: tenant_id NULL, requested_by = subject.
	var tenantID *string
	var requestedBy string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT tenant_id::text, requested_by::text FROM builds WHERE id=$1`, f.params.BuildID).Scan(&tenantID, &requestedBy))
	require.Nil(t, tenantID)
	require.Equal(t, f.grant.Subject.String(), requestedBy)
}

// pinnedSession binds a Repository to ONE dedicated pool connection so
// a barrier test controls exactly which backend holds which lock.
type pinnedSession struct {
	repo   *Repository
	conn   *pgxpool.Conn
	pid    int
	release func()
}

func pinSession(ctx context.Context, t *testing.T, pool *pgxpool.Pool) *pinnedSession {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	var pid int
	require.NoError(t, conn.Conn().QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
	s := &pinnedSession{repo: NewRepository(conn.Conn()), conn: conn, pid: pid}
	t.Cleanup(func() { conn.Release() })
	return s
}

// waitBlockedBy polls on an OBSERVER session until blockerPID shows up
// in pg_blocking_pids(blockedPID) — the precise lock-wait edge, not a
// cluster-wide guess. Bounded; returns false on deadline.
func waitBlockedBy(ctx context.Context, t *testing.T, observer *pgxpool.Pool, blockedPID, blockerPID int) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := observer.QueryRow(ctx,
			`SELECT $1 = ANY(pg_blocking_pids($2))`, blockerPID, blockedPID).Scan(&blocked); err == nil && blocked {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestPG_Barrier_ReceiptBlocksFinish: T1 (pinned) holds the dispatch row
// lock with an uncommitted pending receipt; T2 (pinned) Finish must WAIT
// on that row lock and, after T1 commits, observe the pending receipt
// and refuse with ErrFinishIncomplete. A read-before-lock finish would
// succeed here — this is the race the test forbids.
func TestPG_Barrier_ReceiptBlocksFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	f.seedCompletedJob(ctx, t)

	t1 := pinSession(ctx, t, f.pool)
	t2 := pinSession(ctx, t, f.pool)

	tx1, err := t1.conn.Conn().Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background()) //nolint:errcheck
	_, err = tx1.Exec(ctx, `SELECT 1 FROM scheduling.dispatches WHERE event_id=$1 FOR UPDATE`, f.params.EventID)
	require.NoError(t, err)
	key := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")
	_, err = tx1.Exec(ctx,
		`INSERT INTO scheduling.publish_receipts
		   (key, dispatch_event_id, node_id, dataset_rid, request_body, request_sha256, state)
		 VALUES ($1,$2,'node-1','ri.foundry.main.dataset.9',$3,'sum-t1','pending')`,
		key+"-t1", f.params.EventID, []byte(`{"rows":1}`))
	require.NoError(t, err)

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		err := t2.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil)
		done <- result{err: err}
	}()

	require.True(t, waitBlockedBy(ctx, t, f.pool, t2.pid, t1.pid),
		"T2 Finish must block on T1's dispatch row lock")
	require.NoError(t, tx1.Commit(ctx))
	res := <-done
	require.ErrorIs(t, res.err, ErrFinishIncomplete)

	state, _ := f.dispatchState(ctx, t)
	require.Equal(t, "executing", state)
	require.True(t, f.activityExists(ctx, t))
}

// TestPG_Barrier_GrantRevokeBlocksClaim: T1 (pinned) holds the grant row
// lock mid-revoke; T2's claim (FOR SHARE) must WAIT for it and then see
// state='revoked' and refuse. Without the row lock the claim would read
// the pre-revoke snapshot and win — this is the FOR SHARE proof.
func TestPG_Barrier_GrantRevokeBlocksClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)

	t1 := pinSession(ctx, t, f.pool)
	t2 := pinSession(ctx, t, f.pool)

	tx1, err := t1.conn.Conn().Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background()) //nolint:errcheck
	_, err = tx1.Exec(ctx, `SELECT 1 FROM scheduling.grants WHERE id=$1 FOR UPDATE`, f.params.GrantRef)
	require.NoError(t, err)

	type result struct {
		fence int64
		ok    bool
		err   error
	}
	done := make(chan result, 1)
	go func() {
		fence, ok, err := t2.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 30*time.Second)
		done <- result{fence, ok, err}
	}()

	require.True(t, waitBlockedBy(ctx, t, f.pool, t2.pid, t1.pid),
		"T2 claim must wait on the revoking grant row lock")
	_, err = tx1.Exec(ctx, `UPDATE scheduling.grants SET state='revoked' WHERE id=$1`, f.params.GrantRef)
	require.NoError(t, err)
	require.NoError(t, tx1.Commit(ctx))

	res := <-done
	require.NoError(t, res.err)
	require.False(t, res.ok, "claim must refuse after the revoke committed under the lock")
	state, _ := f.dispatchState(ctx, t)
	require.Equal(t, "admitted", state)
}

// TestPG_Barrier_FinishHoldingLockBlocksWriter (reverse edge): a
// Finish transaction holding the dispatch row lock inside its extra
// writes blocks a concurrent fenced writer (RecordPendingReceipt) on
// the SAME row; once the Finish commits (terminal state, lease
// cleared), the late writer must fail with ErrStaleFence — proving the
// writer itself takes the row lock rather than racing past it.
func TestPG_Barrier_FinishHoldingLockBlocksWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	f.seedCompletedJob(ctx, t)
	key := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fence, key, "node-1", "ri.foundry.main.dataset.9", []byte(`{}`)))
	require.NoError(t, f.repo.MarkReceiptCommitted(ctx, f.params.EventID, "worker-a", fence, key, []byte(`{"Status":"COMMITTED"}`)))

	t1 := pinSession(ctx, t, f.pool)
	t2 := pinSession(ctx, t, f.pool)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	finishDone := make(chan error, 1)
	writerDone := make(chan error, 1)
	// Cleanup registered BEFORE any goroutine starts: on ANY exit path
	// (including Fatal inside the barriers below) release -> cancel ->
	// drain both goroutines -> only then t.Cleanup releases the pinned
	// connections.
	defer func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		for _, ch := range []<-chan error{finishDone, writerDone} {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	go func() {
		extra := func(ctx context.Context, tx pgx.Tx) error {
			close(entered)
			// Hold the Finish transaction (and its row lock) open, but
			// stay cancellable by the test context.
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		finishDone <- t1.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", extra)
	}()
	// If the Finish fails before reaching its extra writes, surface the
	// error instead of waiting forever.
	select {
	case <-entered:
	case err := <-finishDone:
		t.Fatalf("Finish failed before entering extra writes: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for Finish to enter extra writes")
	}

	lateKey := scheduleevents.DerivePublishKey(f.params.EventID, "node-2")
	go func() {
		writerDone <- t2.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fence, lateKey, "node-2", "ri.foundry.main.dataset.10", []byte(`{}`))
	}()

	require.True(t, waitBlockedBy(ctx, t, f.pool, t2.pid, t1.pid),
		"the fenced writer must block on the Finish transaction's row lock")
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-finishDone, "the legitimate Finish commits")
	require.ErrorIs(t, <-writerDone, ErrStaleFence, "late writer must fail closed once the lease is gone")

	finalState, _ := f.dispatchState(ctx, t)
	require.Equal(t, DispatchStateSucceeded, finalState)
	require.False(t, f.activityExists(ctx, t))
	// The late writer left nothing behind.
	var lateReceipts int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM scheduling.publish_receipts WHERE node_id='node-2'`).Scan(&lateReceipts))
	require.Equal(t, 0, lateReceipts)
}

// unknown_publish dispatches and the extended recovery scan.
func TestPG_ReconcileDispatchGate(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	// Unknown publish outcome: recoverable — dispatch-only, slot kept.
	require.NoError(t, f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateUnknownPublish, "receipt timeout", nil))
	state, _ := f.dispatchState(ctx, t)
	require.Equal(t, DispatchStateUnknownPublish, state)
	require.True(t, f.activityExists(ctx, t))
	require.Equal(t, "BUILD_RESOLUTION", f.buildState(ctx, t), "recoverable finish must not write a build terminal state")

	// The recovery scan (not event redelivery) is what surfaces it.
	stale, err := f.repo.ExpireStaleExecutions(ctx, time.Now().UTC())
	require.NoError(t, err)
	found := false
	for _, d := range stale {
		if d.EventID == f.params.EventID {
			found = true
		}
	}
	require.True(t, found, "unknown_publish must appear in the recovery scan")

	// Reconcile re-claims the ORIGINAL dispatch with a fresh fence.
	fence2, ok, err := f.repo.ReconcileDispatch(ctx, f.params.EventID, "reconciler", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, fence+1, fence2)

	// After the grant is revoked, reconcile refuses (parked).
	_, err = f.pool.Exec(ctx, `UPDATE scheduling.grants SET state='revoked' WHERE id=$1`, f.params.GrantRef)
	require.NoError(t, err)
	require.NoError(t, f.repo.FinishDispatch(ctx, f.params.EventID, "reconciler", fence2, DispatchStateUnknownPublish, "still unknown", nil))
	_, ok, err = f.repo.ReconcileDispatch(ctx, f.params.EventID, "reconciler", 30*time.Second)
	require.NoError(t, err)
	require.False(t, ok, "revoked grant must refuse reconcile")
}

// TestPG_UnsealedExpectationsBlockSuccess proves the NULL gate: without
// sealed expectation sets, success is impossible even with committed
// receipts and completed jobs — unknown ≠ sealed-empty, and M3 must
// seal the durable plan before any succeeded finish.
func TestPG_UnsealedExpectationsBlockSuccess(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.params.ExpectedOutputsSealed = false
	f.params.ExpectedNodesSealed = false
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	f.seedCompletedJob(ctx, t)
	key := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fence, key, "node-1", "ri.foundry.main.dataset.9", []byte(`{}`)))
	require.NoError(t, f.repo.MarkReceiptCommitted(ctx, f.params.EventID, "worker-a", fence, key, []byte(`{"Status":"COMMITTED"}`)))
	require.ErrorIs(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil),
		ErrFinishIncomplete)
	state, _ := f.dispatchState(ctx, t)
	require.Equal(t, "executing", state)
	require.True(t, f.activityExists(ctx, t))
}

// TestPG_PartialExpectationBlocksSuccess proves UNIVERSAL coverage: two
// expected outputs with only one committed receipt must not pass —
// duplicates of the committed one are structurally impossible (unique
// pair index), and the check quantifies over expected pairs instead of
// counting matches.
func TestPG_PartialExpectationBlocksSuccess(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	// Widen the sealed expectation to two outputs AFTER admission via
	// controlled SQL (the resolver-equivalent of a two-output plan).
	_, err := f.pool.Exec(ctx,
		`UPDATE scheduling.dispatches SET expected_outputs=$2::jsonb WHERE event_id=$1`,
		f.params.EventID,
		`[{"node_id":"node-1","dataset_rid":"ri.foundry.main.dataset.9"},{"node_id":"node-2","dataset_rid":"ri.foundry.main.dataset.10"}]`)
	require.NoError(t, err)

	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	f.seedCompletedJob(ctx, t)
	key1 := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fence, key1, "node-1", "ri.foundry.main.dataset.9", []byte(`{}`)))
	require.NoError(t, f.repo.MarkReceiptCommitted(ctx, f.params.EventID, "worker-a", fence, key1, []byte(`{"Status":"COMMITTED"}`)))

	// node-2's expected output has NO receipt: success must be refused.
	require.ErrorIs(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil),
		ErrFinishIncomplete)

	// A duplicate receipt for the SAME (dispatch,node,dataset) pair is
	// structurally rejected by the unique binding.
	_, err = f.pool.Exec(ctx,
		`INSERT INTO scheduling.publish_receipts
		   (key, dispatch_event_id, node_id, dataset_rid, request_body, request_sha256, state)
		 VALUES ('sch:duplicate', $1, 'node-1', 'ri.foundry.main.dataset.9', '{}'::bytea, 'x', 'pending')`,
		f.params.EventID)
	require.Error(t, err, "duplicate (dispatch,node,dataset) receipt must violate the unique binding")
}

// TestPG_UnrelatedJobDoesNotSatisfyExpectedNodes: the explicit
// counterexample — expected nodes [a, b], sealed-empty outputs, and a
// single COMPLETED job of an UNRELATED spec must NOT pass. The check
// is universal over expected job-spec rids, not over whatever jobs
// happen to exist.
func TestPG_UnrelatedJobDoesNotSatisfyExpectedNodes(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	// Resolver-equivalent: two expected nodes (job spec rids a and b),
	// sealed-empty outputs.
	_, err = f.pool.Exec(ctx,
		`UPDATE scheduling.dispatches SET expected_nodes=$2::jsonb, expected_outputs='[]'::jsonb WHERE event_id=$1`,
		f.params.EventID, `["ri.foundry.main.job_spec.a","ri.foundry.main.job_spec.b"]`)
	require.NoError(t, err)

	// A single COMPLETED job of an UNRELATED spec exists.
	_, err = f.pool.Exec(ctx,
		`INSERT INTO jobs (id, build_id, job_spec_rid, state)
		 VALUES ($1,$2,'ri.foundry.main.job_spec.unrelated','COMPLETED')`,
		uuid.New(), f.params.BuildID)
	require.NoError(t, err)

	require.ErrorIs(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil),
		ErrFinishIncomplete, "an unrelated completed job must not satisfy expected nodes")

	// Completing BOTH expected specs (plus the unrelated one) unlocks
	// success: universal coverage, extras merely have to be COMPLETED.
	for _, spec := range []string{"ri.foundry.main.job_spec.a", "ri.foundry.main.job_spec.b"} {
		_, err = f.pool.Exec(ctx,
			`INSERT INTO jobs (id, build_id, job_spec_rid, state) VALUES ($1,$2,$3,'COMPLETED')`,
			uuid.New(), f.params.BuildID, spec)
		require.NoError(t, err)
	}
	require.NoError(t,
		f.repo.FinishDispatch(ctx, f.params.EventID, "worker-a", fence, DispatchStateSucceeded, "", nil))
	finalState, _ := f.dispatchState(ctx, t)
	require.Equal(t, DispatchStateSucceeded, finalState)
}

// TestPG_SealedNilSerializesAsEmptyArray: sealed=true with a nil slice
// must persist '[]' (a JSON array), never SQL NULL / JSON null — the
// NULL gate stays airtight for genuinely unsealed rows.
func TestPG_SealedNilSerializesAsEmptyArray(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.params.ExpectedOutputs = nil
	f.params.ExpectedNodes = nil
	f.admit(ctx, t)
	var outType, nodeType string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT jsonb_typeof(expected_outputs), jsonb_typeof(expected_nodes)
		   FROM scheduling.dispatches WHERE event_id=$1`, f.params.EventID).Scan(&outType, &nodeType))
	require.Equal(t, "array", outType)
	require.Equal(t, "array", nodeType)
}

// TestPG_ClaimUsesWallClockAfterLockWait: T1 holds the grant row lock
// WITHOUT updating it (no EPQ re-evaluation to mask anything) until
// the grant's expiry has passed on the DB clock; the claim — which
// takes FOR SHARE and only THEN reads the wall clock — must refuse.
func TestPG_ClaimUsesWallClockAfterLockWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	// Grant expires in 3 seconds.
	_, err := f.pool.Exec(ctx,
		`UPDATE scheduling.grants SET expires_at = clock_timestamp() + interval '3 seconds' WHERE id=$1`,
		f.params.GrantRef)
	require.NoError(t, err)
	f.admit(ctx, t)

	t1 := pinSession(ctx, t, f.pool)
	t2 := pinSession(ctx, t, f.pool)
	tx1, err := t1.conn.Conn().Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background()) //nolint:errcheck
	_, err = tx1.Exec(ctx, `SELECT 1 FROM scheduling.grants WHERE id=$1 FOR UPDATE`, f.params.GrantRef)
	require.NoError(t, err)

	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, ok, err := t2.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 30*time.Second)
		done <- result{ok, err}
	}()
	require.True(t, waitBlockedBy(ctx, t, f.pool, t2.pid, t1.pid),
		"claim must be waiting on the grant row lock")

	// Deterministic condition (not a timing sleep): hold the lock until
	// the DB clock passes the grant expiry.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var expired bool
		require.NoError(t, f.pool.QueryRow(ctx,
			`SELECT clock_timestamp() > expires_at FROM scheduling.grants WHERE id=$1`,
			f.params.GrantRef).Scan(&expired))
		if expired || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, tx1.Commit(ctx)) // no UPDATE, no EPQ — pure lock wait
	res := <-done
	require.NoError(t, res.err)
	require.False(t, res.ok, "claim must refuse a grant that expired while blocked on the lock")
}

// TestPG_Barrier_FencedWriteExpiryDuringLockWait is the root-cause
// regression for the pre-lock ExecQual hole: T1 holds the dispatch row
// lock WITHOUT modifying the tuple (no EPQ recheck will fire); T2
// starts a fenced write while the lease is live, blocks on the row
// lock, and the DB clock crosses the lease expiry before T1 releases.
// T2 must fail with ErrStaleFence and leave zero rows behind — a
// WHERE-on-lease SELECT ... FOR UPDATE would have evaluated its
// predicate before waiting and run fn on the stale slot.
func TestPG_Barrier_FencedWriteExpiryDuringLockWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker-a", 5*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	// Short live lease: expires 3s after now.
	_, err = f.pool.Exec(ctx,
		`UPDATE scheduling.dispatches SET lease_expires_at = clock_timestamp() + interval '3 seconds' WHERE event_id=$1`,
		f.params.EventID)
	require.NoError(t, err)

	t1 := pinSession(ctx, t, f.pool)
	t2 := pinSession(ctx, t, f.pool)
	tx1, err := t1.conn.Conn().Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background()) //nolint:errcheck
	_, err = tx1.Exec(ctx, `SELECT 1 FROM scheduling.dispatches WHERE event_id=$1 FOR UPDATE`, f.params.EventID)
	require.NoError(t, err)

	key := scheduleevents.DerivePublishKey(f.params.EventID, "node-1")
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- t2.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker-a", fence, key, "node-1", "ri.foundry.main.dataset.9", []byte(`{}`))
	}()
	require.True(t, waitBlockedBy(ctx, t, f.pool, t2.pid, t1.pid),
		"fenced writer must block on the row lock")

	// Deterministic: hold until the DB clock passes the lease expiry.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var expired bool
		require.NoError(t, f.pool.QueryRow(ctx,
			`SELECT clock_timestamp() > lease_expires_at FROM scheduling.dispatches WHERE event_id=$1`,
			f.params.EventID).Scan(&expired))
		if expired || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, tx1.Commit(ctx)) // pure lock hold, no tuple change
	require.ErrorIs(t, <-writerDone, ErrStaleFence)

	var receipts int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduling.publish_receipts`).Scan(&receipts))
	require.Equal(t, 0, receipts, "expired-lease fenced write must leave zero rows")
}

// TestPG_PoisonCountAndIdempotentDLQ covers the real poison lifecycle:
// first RecordPoisonAttempt INSERT (state column — 23502 regression),
// then EnqueueDLQViaOutbox TWICE (crash between DLQ commit and ACK):
// the second call is an idempotent no-op with the SAME stable event id,
// and a DLQ attempt without a poison row is refused.
func TestPG_PoisonCountAndIdempotentDLQ(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)

	n, err := f.repo.RecordPoisonAttempt(ctx, "pipeline.builds.v1", 7, 101, "bad json")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = f.repo.RecordPoisonAttempt(ctx, "pipeline.builds.v1", 7, 101, "bad json 2")
	require.NoError(t, err)
	require.Equal(t, 2, n)

	require.NoError(t, f.repo.EnqueueDLQViaOutbox(ctx, "pipeline.builds.v1", 7, 101, []byte("raw"), "budget spent"))
	// Crash-and-retry: second enqueue must not mint a new event.
	require.NoError(t, f.repo.EnqueueDLQViaOutbox(ctx, "pipeline.builds.v1", 7, 101, []byte("raw"), "budget spent"))
	var state string
	var dlqID *string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT state, dlq_event_id::text FROM scheduling.poison_messages
		  WHERE topic='pipeline.builds.v1' AND partition_id=7 AND partition_offset=101`).Scan(&state, &dlqID))
	require.Equal(t, "dlq_enqueued", state)
	require.NotNil(t, dlqID, "stable DLQ event id must be persisted")
	// The outbox row is INSERT+DELETE in one tx: nothing lingers.
	var outboxRows int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox.events`).Scan(&outboxRows))
	require.Equal(t, 0, outboxRows)

	// Stateless DLQ (no poison row) is refused — never a silent success.
	require.Error(t, f.repo.EnqueueDLQViaOutbox(ctx, "pipeline.builds.v1", 7, 999, []byte("x"), "no row"))
}
