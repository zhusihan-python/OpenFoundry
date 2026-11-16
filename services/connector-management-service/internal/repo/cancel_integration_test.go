//go:build integration

// Real PostgreSQL 16 regressions for the P2.2 cancel/publish-gate repository
// layer (cancel.go): pending cancel-vs-claim with real independent sessions in
// both deterministic orders (the run row's FIFO lock-wait queue, proven via
// pg_blocking_pids' queue-ahead reporting — never random sleeps), durable and
// idempotent running-run cancel requests with first-actor/first-timestamp
// preservation, cancel↔gate serialization in both lock orders with the loser
// observable exactly, gate fencing (wrong/expired/taken-over/repeated/
// terminal materials are Fenced, never masked as cancellations), gate TTL
// anchored to the post-lock DB clock, the controlled terminal stage matrix
// versus the legacy P2.1 semantics, tenant/parent-binding rejection with the
// full row untouched, and genuine pre-P2.2 database upgrade/replay safety.
//
// Full-row consistency assertions compare the RAW row_to_json snapshot but
// only ever PRINT a lease-token-redacted copy on failure: lease-internal
// material never enters the archive (every token/holder below is synthetic
// test material anyway).
package repo

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// ---------- file-local helpers ----------

var cancelLeaseTokenRe = regexp.MustCompile(`"lease_token"\s*:\s*"[^"]*"`)

// redactLeaseToken strips lease-internal token values from a full-row JSON
// snapshot before anything is printed.
func redactLeaseToken(s string) string {
	return cancelLeaseTokenRe.ReplaceAllString(s, `"lease_token":"<redacted>"`)
}

// assertCancelRowEqual asserts two full sync_runs snapshots (from runRowJSON)
// are byte-identical, printing only redacted copies on mismatch.
func assertCancelRowEqual(t *testing.T, before, after string, msg string) {
	t.Helper()
	if before != after {
		t.Errorf("%s: full sync_runs row changed\nbefore: %s\nafter:  %s",
			msg, redactLeaseToken(before), redactLeaseToken(after))
	}
}

// cancelBarrier holds the run row FOR UPDATE WITHOUT writing it (the stronger
// blocker: no new row version is produced, so EvalPlanQual never re-runs).
// Sessions that queue behind it are granted strictly in arrival order, and
// pg_blocking_pids reports queue-ahead sessions too — the tests use both as
// the deterministic ordering proof.
type cancelBarrier struct {
	conn *pgx.Conn
	pid  int32
	tx   pgx.Tx
}

func holdCancelBarrier(t *testing.T, dsn string, runID uuid.UUID) *cancelBarrier {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	var locked uuid.UUID
	require.NoError(t, tx.QueryRow(ctx, `SELECT id FROM sync_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&locked))
	return &cancelBarrier{conn: conn, pid: pid, tx: tx}
}

func (b *cancelBarrier) release(t *testing.T) {
	t.Helper()
	require.NoError(t, b.tx.Rollback(context.Background()))
}

// cancelCall/gateCall/claimCall carry background op results across goroutines.
type cancelCall struct {
	res *CancelResult
	err error
}

type gateCall struct {
	res *PublishGateResult
	err error
}

type claimCall struct {
	claim *LeaseClaim
	err   error
}

// goRequestCancel/goClaim/goBeginGate run one repo op in the background on a
// dedicated session; the test orchestrates the lock queue before collecting.
func goRequestCancel(t *testing.T, r *Repo, runID, syncID uuid.UUID, tenant Tenant, actor *uuid.UUID, cause string) <-chan cancelCall {
	t.Helper()
	ch := make(chan cancelCall, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res, err := r.RequestCancelSyncRun(ctx, runID, syncID, tenant, actor, cause)
		ch <- cancelCall{res, err}
	}()
	return ch
}

func goClaim(t *testing.T, r *Repo, runID uuid.UUID, tenant Tenant, holder, token string, ttl time.Duration) <-chan claimCall {
	t.Helper()
	ch := make(chan claimCall, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, holder, token, ttl)
		ch <- claimCall{claim, err}
	}()
	return ch
}

func goBeginGate(t *testing.T, r *Repo, runID uuid.UUID, tenant Tenant, holder, token string, generation int, ttl time.Duration) <-chan gateCall {
	t.Helper()
	ch := make(chan gateCall, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		res, err := r.BeginPublishGate(ctx, runID, tenant, holder, token, generation, ttl)
		ch <- gateCall{res, err}
	}()
	return ch
}

func mustRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(20 * time.Second):
		t.Fatalf("%s never returned", what)
		return *new(T)
	}
}

// queueProof proves waiter is waiting on the run-row lock with ahead ahead of
// it in the wait queue (pg_blocking_pids reports queue-ahead sessions as
// blockers too), pinning the FIFO grant order before the barrier releases.
func queueProof(t *testing.T, observer *pgxpool.Pool, waiter, ahead int32) {
	t.Helper()
	waitUntilBlockedBy(t, observer, waiter, ahead)
}

// ---------- group 1: pending cancel-vs-claim, both deterministic orders ----------

func TestPGCancelPendingVsClaimBothOrders(t *testing.T) {
	t.Run("cancel_first_aborts_pending_claim_loses", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := leaseFixture(t)
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())

		bar := holdCancelBarrier(t, dsn, runID)

		cancelRepo := singleConnRepo(t, dsn)
		cancelPID := backendPID(t, cancelRepo.Pool)
		actor := uuid.New()
		cancelCh := goRequestCancel(t, cancelRepo, runID, defID, tenant, &actor, "user aborted while pending")
		queueProof(t, r.Pool, cancelPID, bar.pid)

		claimRepo := singleConnRepo(t, dsn)
		claimPID := backendPID(t, claimRepo.Pool)
		claimCh := goClaim(t, claimRepo, runID, tenant, "race-loser", "race-loser-token", time.Minute)
		queueProof(t, r.Pool, claimPID, cancelPID) // strictly BEHIND the cancel in the queue
		bar.release(t)

		got := mustRecv(t, cancelCh, "cancel behind barrier")
		require.NoError(t, got.err)
		require.NotNil(t, got.res)
		assert.Equal(t, CancelOutcomeAborted, got.res.Outcome)
		require.NotNil(t, got.res.Run)
		assert.Equal(t, "aborted", got.res.Run.Status)
		assert.Equal(t, 0, got.res.Run.Generation, "pending abort must not bump generation")
		assert.Equal(t, 0, got.res.Run.Attempt)
		require.NotNil(t, got.res.Run.CancelRequestedAt)
		require.NotNil(t, got.res.Run.CancelledBy)
		assert.Equal(t, actor, *got.res.Run.CancelledBy)
		require.NotNil(t, got.res.Run.FinishedAt)
		require.NotNil(t, got.res.Run.Error)
		assert.Equal(t, "user aborted while pending", *got.res.Run.Error)
		assert.Zero(t, got.res.Run.BytesWritten)
		assert.Zero(t, got.res.Run.FilesWritten)
		assert.Zero(t, got.res.Run.RowsWritten)
		assert.Nil(t, got.res.Run.DatasetVersionID, "pending abort proves zero publishes")
		assert.Nil(t, got.res.Run.ContentHash)

		lost := mustRecv(t, claimCh, "claim queued behind the cancel")
		require.NoError(t, lost.err)
		assert.Nil(t, lost.claim, "claim must lose after the pending row went terminal")

		// The terminal state and the zero-stats provenance are durable for an
		// independent session; no lease material was ever installed.
		other := singleConnRepo(t, dsn)
		var status string
		var gen, attempt int
		var leaseToken, version, hash *string
		require.NoError(t, other.Pool.QueryRow(context.Background(),
			`SELECT status, generation, attempt, lease_token, dataset_version_id::text, content_hash
			   FROM sync_runs WHERE id = $1`, runID).
			Scan(&status, &gen, &attempt, &leaseToken, &version, &hash))
		assert.Equal(t, "aborted", status)
		assert.Equal(t, 0, gen)
		assert.Equal(t, 0, attempt)
		assert.Nil(t, leaseToken, "the losing claim must not have installed a lease")
		assert.Nil(t, version)
		assert.Nil(t, hash)
	})

	t.Run("claim_first_running_then_request_visible_holder_aborts", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := leaseFixture(t)
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())

		bar := holdCancelBarrier(t, dsn, runID)

		claimRepo := singleConnRepo(t, dsn)
		claimPID := backendPID(t, claimRepo.Pool)
		claimCh := goClaim(t, claimRepo, runID, tenant, "race-winner", "race-winner-token", time.Minute)
		queueProof(t, r.Pool, claimPID, bar.pid)

		cancelRepo := singleConnRepo(t, dsn)
		cancelPID := backendPID(t, cancelRepo.Pool)
		actor := uuid.New()
		cancelCh := goRequestCancel(t, cancelRepo, runID, defID, tenant, &actor, "user abort")
		queueProof(t, r.Pool, cancelPID, claimPID) // strictly BEHIND the claim in the queue
		bar.release(t)

		won := mustRecv(t, claimCh, "claim behind barrier")
		require.NoError(t, won.err)
		require.NotNil(t, won.claim, "claim queued first must win the pending row")
		assert.Equal(t, "running", won.claim.Run.Status)
		assert.Equal(t, 1, won.claim.Run.Generation)
		assert.Equal(t, 1, won.claim.Run.Attempt)

		got := mustRecv(t, cancelCh, "cancel queued behind the claim")
		require.NoError(t, got.err)
		require.NotNil(t, got.res)
		assert.Equal(t, CancelOutcomeRequested, got.res.Outcome,
			"a claim that won first does NOT block cancellation — Requested is the legal outcome")
		require.NotNil(t, got.res.Run)
		assert.Equal(t, "running", got.res.Run.Status)
		require.NotNil(t, got.res.Run.CancelRequestedAt)
		require.NotNil(t, got.res.Run.CancelledBy)
		assert.Equal(t, actor, *got.res.Run.CancelledBy)

		// The request is durable and visible to a brand-new independent
		// session; the run is still running (no fabricated abort).
		other := singleConnRepo(t, dsn)
		var status string
		var cancelAt *time.Time
		require.NoError(t, other.Pool.QueryRow(context.Background(),
			`SELECT status, cancel_requested_at FROM sync_runs WHERE id = $1`, runID).Scan(&status, &cancelAt))
		assert.Equal(t, "running", status)
		require.NotNil(t, cancelAt)
		assert.True(t, cancelAt.Equal(*got.res.Run.CancelRequestedAt))

		// The executor's lock-free probe observes lease validity AND the
		// persisted request in one read.
		leaseOK, cancelRequested, err := r.CheckSyncRunControl(context.Background(), runID, tenant,
			"race-winner", "race-winner-token", 1)
		require.NoError(t, err)
		assert.True(t, leaseOK)
		assert.True(t, cancelRequested)

		// Only the still-valid holder confirms the abort — and exactly once.
		aborted, err := claimRepo.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			"race-winner", "race-winner-token", 1, "aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, aborted)
		assert.Equal(t, "aborted", aborted.Status)
		assert.Nil(t, aborted.PublishEnteredAt, "abort happened before any publish gate")
		again, err := claimRepo.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			"race-winner", "race-winner-token", 1, "aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, again, "terminal state closes exactly once")
	})
}

// ---------- group 2: running cancel durability, idempotency, actors, no forged stats ----------

func TestPGCancelRunningRequestDurableIdempotent(t *testing.T) {
	r, dsn, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()

	// runA: the first REAL actor must survive a second, different actor's
	// idempotent repeat.
	runA := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claimA, err := r.ClaimSyncRun(ctx, runA, tenant, "dur-hold", "dur-tok", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimA)

	// Pre-request state: a brand-new session sees no cancel flag yet.
	fresh := singleConnRepo(t, dsn)
	var seen bool
	require.NoError(t, fresh.Pool.QueryRow(ctx,
		`SELECT cancel_requested_at IS NOT NULL FROM sync_runs WHERE id = $1`, runA).Scan(&seen))
	assert.False(t, seen)

	actorA, actorB := uuid.New(), uuid.New()
	first, err := r.RequestCancelSyncRun(ctx, runA, defID, tenant, &actorA, "first actor")
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NotNil(t, first.Run)
	assert.Equal(t, CancelOutcomeRequested, first.Outcome)
	require.NotNil(t, first.Run.CancelledBy)
	assert.Equal(t, actorA, *first.Run.CancelledBy)
	require.NotNil(t, first.Run.CancelRequestedAt)
	firstAt := *first.Run.CancelRequestedAt

	// Durable and visible to a NEW independent session.
	require.NoError(t, fresh.Pool.QueryRow(ctx,
		`SELECT cancel_requested_at IS NOT NULL AND cancelled_by = $2 FROM sync_runs WHERE id = $1`,
		runA, actorA).Scan(&seen))
	assert.True(t, seen)

	afterFirst := runRowJSON(t, r.Pool, runA)

	// Repeat by another actor: idempotent — the FULL row is byte-identical,
	// first timestamp and first actor preserved.
	second, err := r.RequestCancelSyncRun(ctx, runA, defID, tenant, &actorB, "second actor")
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, CancelOutcomeRequested, second.Outcome)
	require.NotNil(t, second.Run.CancelledBy)
	assert.Equal(t, actorA, *second.Run.CancelledBy, "first actor must never be overwritten")
	assert.True(t, second.Run.CancelRequestedAt.Equal(firstAt), "first cancel timestamp preserved")
	assertCancelRowEqual(t, afterFirst, runRowJSON(t, r.Pool, runA), "repeated cancel")

	// runB: a system nil-actor closure request first, then a user request —
	// the nil actor and the first timestamp stay.
	runB := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claimB, err := r.ClaimSyncRun(ctx, runB, tenant, "dur2-hold", "dur2-tok", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimB)
	sys, err := r.RequestCancelSyncRun(ctx, runB, defID, tenant, nil, "shutdown")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeRequested, sys.Outcome)
	require.NotNil(t, sys.Run)
	assert.Nil(t, sys.Run.CancelledBy, "system-initiated request records no actor")
	require.NotNil(t, sys.Run.CancelRequestedAt)
	afterSys := runRowJSON(t, r.Pool, runB)
	user := uuid.New()
	rep, err := r.RequestCancelSyncRun(ctx, runB, defID, tenant, &user, "user follows shutdown")
	require.NoError(t, err)
	assert.Equal(t, CancelOutcomeRequested, rep.Outcome)
	require.NotNil(t, rep.Run)
	assert.Nil(t, rep.Run.CancelledBy, "a later user request must not fill the nil actor")
	assert.True(t, rep.Run.CancelRequestedAt.Equal(*sys.Run.CancelRequestedAt))
	assertCancelRowEqual(t, afterSys, runRowJSON(t, r.Pool, runB), "user repeat after nil-actor request")

	// A cancel request must never become a vehicle for fabricated success
	// stats: runA (cancel requested) cannot be completed 'succeeded', and the
	// rejected write leaves every stats/hash/version column untouched.
	beforeForge := runRowJSON(t, r.Pool, runA)
	fakeVersion, fakeHash := uuid.New(), "forged-hash"
	done, err := r.CompleteLeasedSyncRun(ctx, runA, tenant, "dur-hold", "dur-tok", 1,
		"succeeded", 987654, 99, 999, nil, nil, &fakeVersion, &fakeHash)
	require.NoError(t, err)
	assert.Nil(t, done, "cancel-won run can never be recorded as succeeded")
	assertCancelRowEqual(t, beforeForge, runRowJSON(t, r.Pool, runA), "forged succeeded rejection")

	// The honest terminal for a cancel-won run is aborted with zero stats.
	aborted, err := r.CompleteLeasedSyncRun(ctx, runA, tenant, "dur-hold", "dur-tok", 1,
		"aborted", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, aborted)
	assert.Equal(t, "aborted", aborted.Status)
	assert.Zero(t, aborted.BytesWritten)
	assert.Zero(t, aborted.FilesWritten)
	assert.Zero(t, aborted.RowsWritten)
	assert.Nil(t, aborted.DatasetVersionID)
	assert.Nil(t, aborted.ContentHash)
}

// ---------- group 3: cancel vs publish gate, both deterministic lock orders ----------

func TestPGCancelVsPublishGateDeterministicOrder(t *testing.T) {
	t.Run("cancel_first_gate_refuses_zero_publishes", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "cg-hold", "cg-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)

		bar := holdCancelBarrier(t, dsn, runID)

		cancelRepo := singleConnRepo(t, dsn)
		cancelPID := backendPID(t, cancelRepo.Pool)
		actor := uuid.New()
		cancelCh := goRequestCancel(t, cancelRepo, runID, defID, tenant, &actor, "beat the gate")
		queueProof(t, r.Pool, cancelPID, bar.pid)

		gateRepo := singleConnRepo(t, dsn)
		gatePID := backendPID(t, gateRepo.Pool)
		gateCh := goBeginGate(t, gateRepo, runID, tenant, "cg-hold", "cg-tok", 1, time.Minute)
		queueProof(t, r.Pool, gatePID, cancelPID) // gate strictly BEHIND the cancel
		bar.release(t)

		got := mustRecv(t, cancelCh, "cancel behind barrier")
		require.NoError(t, got.err)
		require.NotNil(t, got.res)
		assert.Equal(t, CancelOutcomeRequested, got.res.Outcome)

		gate := mustRecv(t, gateCh, "gate queued behind the cancel")
		require.NoError(t, gate.err)
		require.NotNil(t, gate.res)
		assert.Equal(t, PublishGateCancelled, gate.res.Outcome,
			"the cancel committed first: the gate must refuse, not publish")
		assert.Nil(t, gate.res.Claim, "a cancelled gate returns no claim")
		require.NotNil(t, gate.res.Run, "the cancelled gate returns the run projection")
		assert.NotNil(t, gate.res.Run.CancelRequestedAt)
		assert.Nil(t, gate.res.Run.PublishEnteredAt)

		// Zero-publish state reads (plain reads, NOT a side-effect proof: the
		// cancel's own write precedes any read here, so no clean full-row
		// baseline exists inside this race; the serial proof that a Cancelled
		// gate writes nothing is TestPGPublishGateFencingMatrix/
		// live_holder_with_cancel_request_is_cancelled).
		var stageAt *time.Time
		var statusAfterRace string
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT publish_entered_at, status FROM sync_runs WHERE id = $1`, runID).Scan(&stageAt, &statusAfterRace))
		assert.Nil(t, stageAt, "the cancelled gate must leave no stage marker")
		assert.Equal(t, "running", statusAfterRace)

		// The cancel winner can never be recorded as succeeded...
		version := uuid.New()
		done, err := gateRepo.CompleteLeasedSyncRun(ctx, runID, tenant, "cg-hold", "cg-tok", 1,
			"succeeded", 1, 1, 1, nil, nil, &version, nil)
		require.NoError(t, err)
		assert.Nil(t, done)

		// ...but aborts legally exactly once, with the request and the zero
		// publishes durable in the terminal row.
		aborted, err := gateRepo.CompleteLeasedSyncRun(ctx, runID, tenant, "cg-hold", "cg-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, aborted)
		assert.Equal(t, "aborted", aborted.Status)
		assert.Nil(t, aborted.PublishEnteredAt)
		assert.Nil(t, aborted.DatasetVersionID)

		// A later gate on the terminal (cancel-flag-carrying) row is Fenced,
		// never Cancelled — fencing outranks cancellation classification.
		late, err := gateRepo.BeginPublishGate(ctx, runID, tenant, "cg-hold", "cg-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, late)
		assert.Equal(t, PublishGateFenced, late.Outcome)
		assert.Nil(t, late.Claim)
	})

	t.Run("gate_first_cancel_too_late_run_completes_real_outcome", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "gc-hold", "gc-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)

		bar := holdCancelBarrier(t, dsn, runID)

		gateRepo := singleConnRepo(t, dsn)
		gatePID := backendPID(t, gateRepo.Pool)
		gateCh := goBeginGate(t, gateRepo, runID, tenant, "gc-hold", "gc-tok", 1, time.Minute)
		queueProof(t, r.Pool, gatePID, bar.pid)

		cancelRepo := singleConnRepo(t, dsn)
		cancelPID := backendPID(t, cancelRepo.Pool)
		actor := uuid.New()
		cancelCh := goRequestCancel(t, cancelRepo, runID, defID, tenant, &actor, "too late")
		queueProof(t, r.Pool, cancelPID, gatePID) // cancel strictly BEHIND the gate
		bar.release(t)

		gate := mustRecv(t, gateCh, "gate behind barrier")
		require.NoError(t, gate.err)
		require.NotNil(t, gate.res)
		assert.Equal(t, PublishGateEntered, gate.res.Outcome)
		require.NotNil(t, gate.res.Claim, "the live holder's gate entered and renewed the lease")
		assert.Equal(t, 1, gate.res.Claim.Run.Generation)
		var publishEntered bool
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT publish_entered_at IS NOT NULL FROM sync_runs WHERE id = $1`, runID).Scan(&publishEntered))
		assert.True(t, publishEntered)

		got := mustRecv(t, cancelCh, "cancel queued behind the gate")
		require.NoError(t, got.err)
		require.NotNil(t, got.res)
		assert.Equal(t, CancelOutcomeTooLate, got.res.Outcome,
			"the gate entered first: the cancel is observably too late, never 'aborted'")
		require.NotNil(t, got.res.Run)
		assert.Nil(t, got.res.Run.CancelRequestedAt)
		assert.NotNil(t, got.res.Run.PublishEnteredAt)

		// Baseline taken AFTER both racing ops completed — only valid for the
		// serial rejections below. (The full-row proof that a too-late cancel
		// is row-inert is the serial too_late_cancel_is_row_inert_serial
		// subtest: no baseline between the two racing ops is observable here.)
		afterRaces := runRowJSON(t, r.Pool, runID)

		// Counter-example stage guard: a gate-entered run can no longer be
		// dragged back to 'aborted' (a shutdown helper must not fake-abort a
		// possibly-published run).
		forcedAbort, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "gc-hold", "gc-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, forcedAbort, "aborted is illegal after the publish gate was entered")
		assertCancelRowEqual(t, afterRaces, runRowJSON(t, r.Pool, runID), "forced abort rejection")

		// The real outcome completes exactly once, with real artifacts.
		version, hash := uuid.New(), "final-hash"
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "gc-hold", "gc-tok", 1,
			"succeeded", 10, 2, 20, nil, nil, &version, &hash)
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "succeeded", done.Status)
		require.NotNil(t, done.DatasetVersionID)
		assert.Equal(t, version, *done.DatasetVersionID)
		require.NotNil(t, done.ContentHash)
		assert.Equal(t, hash, *done.ContentHash)
		repeat, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "gc-hold", "gc-tok", 1,
			"succeeded", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, repeat, "terminal states are immutable")

		// A post-terminal cancel is an honest already-terminal observation.
		terminalRow := runRowJSON(t, r.Pool, runID)
		late, err := r.RequestCancelSyncRun(ctx, runID, defID, tenant, &actor, "late cancel")
		require.NoError(t, err)
		require.NotNil(t, late)
		assert.Equal(t, CancelOutcomeAlreadyTerminal, late.Outcome)
		require.NotNil(t, late.Run)
		assert.Equal(t, "succeeded", late.Run.Status)
		assert.Nil(t, late.Run.CancelRequestedAt)
		assertCancelRowEqual(t, terminalRow, runRowJSON(t, r.Pool, runID), "already-terminal cancel")
	})

	t.Run("too_late_cancel_is_row_inert_serial", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "st-hold", "st-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		gate, err := r.BeginPublishGate(ctx, runID, tenant, "st-hold", "st-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, PublishGateEntered, gate.Outcome)

		// Serial before→refused cancel→after: the too-late refusal leaves the
		// ENTIRE row unchanged — the side-effect-freedom proof the racing
		// variant above cannot take a baseline for.
		before := runRowJSON(t, r.Pool, runID)
		res, err := r.RequestCancelSyncRun(ctx, runID, defID, tenant, nil, "serial too late")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeTooLate, res.Outcome)
		require.NotNil(t, res.Run)
		assert.Nil(t, res.Run.CancelRequestedAt)
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "serial too-late cancel")
	})
}

// ---------- group 4: gate fencing matrix + lock-wait clock regressions ----------

func TestPGPublishGateFencingMatrix(t *testing.T) {
	t.Run("material_mismatch_and_boundaries_fence_without_touching_the_row", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "mx-hold", "mx-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		before := runRowJSON(t, r.Pool, runID)

		mismatches := []struct {
			name       string
			holder     string
			token      string
			generation int
		}{
			{"wrong holder", "mx-other", "mx-tok", 1},
			{"wrong token", "mx-hold", "mx-other", 1},
			{"wrong generation", "mx-hold", "mx-tok", 2},
		}
		for _, tc := range mismatches {
			g, err := r.BeginPublishGate(ctx, runID, tenant, tc.holder, tc.token, tc.generation, time.Minute)
			require.NoError(t, err, tc.name)
			require.NotNil(t, g, tc.name)
			assert.Equal(t, PublishGateFenced, g.Outcome, tc.name)
			assert.Nil(t, g.Claim, tc.name)
			assert.Nil(t, g.Run, tc.name)
		}
		// Invalid arguments are rejected before any lock or write.
		bad := []struct {
			name       string
			holder     string
			token      string
			generation int
			ttl        time.Duration
		}{
			{"blank holder", " ", "mx-tok", 1, time.Minute},
			{"blank token", "mx-hold", "", 1, time.Minute},
			{"generation zero", "mx-hold", "mx-tok", 0, time.Minute},
			{"negative generation", "mx-hold", "mx-tok", -2, time.Minute},
			{"zero ttl", "mx-hold", "mx-tok", 1, 0},
			{"sub-microsecond ttl", "mx-hold", "mx-tok", 1, 300 * time.Nanosecond},
			{"over-max ttl", "mx-hold", "mx-tok", 1, 25 * time.Hour},
		}
		for _, tc := range bad {
			g, err := r.BeginPublishGate(ctx, runID, tenant, tc.holder, tc.token, tc.generation, tc.ttl)
			assert.Error(t, err, tc.name)
			assert.Nil(t, g, tc.name)
		}
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "fenced/invalid gate calls")
	})

	t.Run("expired_lease_then_takeover_fences_old_material_forever", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "exp-hold", "exp-tok", 120*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claim)

		waitForDBExpiry(t, r.Pool, runID)
		expiredRow := runRowJSON(t, r.Pool, runID)
		g, err := r.BeginPublishGate(ctx, runID, tenant, "exp-hold", "exp-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, g)
		assert.Equal(t, PublishGateFenced, g.Outcome, "expired-but-not-taken-over lease must not enter the gate")
		assert.Nil(t, g.Claim)
		assert.Nil(t, g.Run)
		assertCancelRowEqual(t, expiredRow, runRowJSON(t, r.Pool, runID), "expired gate refusal")

		over, err := r.TakeoverExpiredSyncRun(ctx, runID, tenant, "tk-hold", "tk-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, over)
		assert.Equal(t, 2, over.Run.Generation)
		afterTakeover := runRowJSON(t, r.Pool, runID)

		// The pre-takeover generation's material is dead forever: Fenced (not
		// Cancelled), with zero row change.
		old, err := r.BeginPublishGate(ctx, runID, tenant, "exp-hold", "exp-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, old)
		assert.Equal(t, PublishGateFenced, old.Outcome)
		assert.Nil(t, old.Claim)
		assert.Nil(t, old.Run)
		assertCancelRowEqual(t, afterTakeover, runRowJSON(t, r.Pool, runID), "old-generation gate refusal")

		// The new live holder enters exactly once; a repeat is Fenced.
		first, err := r.BeginPublishGate(ctx, runID, tenant, "tk-hold", "tk-tok", 2, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, PublishGateEntered, first.Outcome)
		require.NotNil(t, first.Claim)
		assert.Equal(t, 2, first.Claim.Run.Generation)
		entered := runRowJSON(t, r.Pool, runID)
		repeat, err := r.BeginPublishGate(ctx, runID, tenant, "tk-hold", "tk-tok", 2, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, repeat)
		assert.Equal(t, PublishGateFenced, repeat.Outcome, "the gate is single-shot")
		assert.Nil(t, repeat.Claim)
		assert.Nil(t, repeat.Run)
		assertCancelRowEqual(t, entered, runRowJSON(t, r.Pool, runID), "repeated gate")
	})

	t.Run("terminal_run_with_cancel_flag_fences_never_cancelled", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "tm-hold", "tm-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		actor := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, runID, defID, tenant, &actor, "terminal flag")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeRequested, res.Outcome)
		aborted, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "tm-hold", "tm-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, aborted)
		terminalRow := runRowJSON(t, r.Pool, runID)

		// The (still unexpired, still material-matching) lease on a terminal
		// row is Fenced — the cancel flag must not reclassify it.
		g, err := r.BeginPublishGate(ctx, runID, tenant, "tm-hold", "tm-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, g)
		assert.Equal(t, PublishGateFenced, g.Outcome)
		assert.Nil(t, g.Claim)
		assert.Nil(t, g.Run)
		assertCancelRowEqual(t, terminalRow, runRowJSON(t, r.Pool, runID), "terminal gate refusal")
	})

	t.Run("live_holder_with_cancel_request_is_cancelled", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "lc-hold", "lc-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		res, err := r.RequestCancelSyncRun(ctx, runID, defID, tenant, nil, "gate vs cancel")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeRequested, res.Outcome)
		before := runRowJSON(t, r.Pool, runID)

		// Fence priority on a live, cancel-flagged running row: mismatched
		// material is Fenced (never masked as Cancelled) and writes nothing;
		// only the CURRENT valid material may observe the cancellation.
		for _, tc := range []struct {
			name       string
			holder     string
			token      string
			generation int
		}{
			{"wrong token with cancel flag", "lc-hold", "lc-other", 1},
			{"wrong generation with cancel flag", "lc-hold", "lc-tok", 2},
		} {
			bad, err := r.BeginPublishGate(ctx, runID, tenant, tc.holder, tc.token, tc.generation, time.Minute)
			require.NoError(t, err, tc.name)
			require.NotNil(t, bad, tc.name)
			assert.Equal(t, PublishGateFenced, bad.Outcome, tc.name)
			assert.Nil(t, bad.Claim, tc.name)
			assert.Nil(t, bad.Run, tc.name)
		}
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "cancel-flagged wrong-material gates")

		g, err := r.BeginPublishGate(ctx, runID, tenant, "lc-hold", "lc-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, g)
		assert.Equal(t, PublishGateCancelled, g.Outcome,
			"a live, unexpired, controlled holder with a persisted request sees Cancelled")
		assert.Nil(t, g.Claim)
		require.NotNil(t, g.Run)
		assert.NotNil(t, g.Run.CancelRequestedAt)
		assert.Nil(t, g.Run.PublishEnteredAt)
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID),
			"cancelled gate must not renew the lease or write the stage")
	})

	t.Run("expired_running_cancel_requests_without_force_abort", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "xr-hold", "xr-tok", 120*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claim)
		waitForDBExpiry(t, r.Pool, runID) // DB-clock proof that the lease expired

		// The request is persisted (Requested) and the run STAYS running — no
		// fabricated force-abort — readable from a brand-new session.
		res, err := r.RequestCancelSyncRun(ctx, runID, defID, tenant, nil, "expired lease")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeRequested, res.Outcome)
		require.NotNil(t, res.Run)
		assert.Equal(t, "running", res.Run.Status, "an expired lease must not be force-aborted by the request")
		other := singleConnRepo(t, dsn)
		var status string
		var requested bool
		require.NoError(t, other.Pool.QueryRow(ctx,
			`SELECT status, cancel_requested_at IS NOT NULL FROM sync_runs WHERE id = $1`, runID).
			Scan(&status, &requested))
		assert.Equal(t, "running", status)
		assert.True(t, requested, "the request stays durable and queryable")

		// The expired holder's gate AND complete are both refused — the
		// expiry fence outranks the persisted cancel flag — with the full row
		// (request included) unchanged across both refusals.
		before := runRowJSON(t, r.Pool, runID)
		g, err := r.BeginPublishGate(ctx, runID, tenant, "xr-hold", "xr-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, g)
		assert.Equal(t, PublishGateFenced, g.Outcome,
			"an expired holder is Fenced even with a persisted cancel request")
		assert.Nil(t, g.Claim)
		assert.Nil(t, g.Run)
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "expired gate refusal under cancel flag")
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "xr-hold", "xr-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, done, "an expired holder cannot confirm the abort either")
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "expired complete refusal under cancel flag")
	})

	t.Run("legacy_running_row_fences", func(t *testing.T) {
		r, _, tenant, _, defID := leaseFixture(t)
		ctx := context.Background()
		legacy, err := r.RunSyncJob(ctx, defID, tenant)
		require.NoError(t, err)
		require.NotNil(t, legacy)
		before := runRowJSON(t, r.Pool, legacy.ID)
		g, err := r.BeginPublishGate(ctx, legacy.ID, tenant, "any-hold", "any-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, g)
		assert.Equal(t, PublishGateFenced, g.Outcome, "legacy rows never carry a trustworthy stage")
		assert.Nil(t, g.Claim)
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, legacy.ID), "legacy gate refusal")
	})
}

// TestPGPublishGateLockWaitExpiryRegression is the READ COMMITTED clock proof
// for the gate itself: the blocker holds the run row FOR UPDATE WITHOUT
// writing it; the gate enters the row-lock wait while the DB clock is still
// INSIDE the lease (proven, so a single-statement impl would already have
// passed its clock qual); the lease expires while it waits; the blocker
// ROLLs BACK (no new row version → EvalPlanQual never runs). The two-phase
// gate must still refuse via the post-lock clock re-check, row unchanged.
func TestPGPublishGateLockWaitExpiryRegression(t *testing.T) {
	r, dsn, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "ex-hold", "ex-tok", 2*time.Second)
	require.NoError(t, err)
	require.NotNil(t, claim)
	before := runRowJSON(t, r.Pool, runID)

	blocker, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = blocker.Close(ctx) })
	var blockerPID int32
	require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID))
	txB, err := blocker.Begin(ctx)
	require.NoError(t, err)
	var locked uuid.UUID
	require.NoError(t, txB.QueryRow(ctx, `SELECT id FROM sync_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&locked))

	gateRepo := singleConnRepo(t, dsn)
	gatePID := backendPID(t, gateRepo.Pool)
	gateCh := goBeginGate(t, gateRepo, runID, tenant, "ex-hold", "ex-tok", 1, time.Minute)

	waitUntilBlockedBy(t, r.Pool, gatePID, blockerPID)
	// The wait genuinely started while the lease was still valid.
	var stillValid bool
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`, runID).Scan(&stillValid))
	require.True(t, stillValid, "test invalid: lease already expired when the gate entered the wait")

	waitForDBExpiry(t, r.Pool, runID)
	require.NoError(t, txB.Rollback(ctx)) // release WITHOUT a new row version

	gate := mustRecv(t, gateCh, "gate after lock release")
	require.NoError(t, gate.err)
	require.NotNil(t, gate.res)
	assert.Equal(t, PublishGateFenced, gate.res.Outcome,
		"the gate must re-check expiry with the post-lock clock and refuse")
	assert.Nil(t, gate.res.Claim)
	assert.Nil(t, gate.res.Run)
	assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "post-lock expiry gate rejection")
}

// TestPGPublishGateTTLAnchoredPostLock is the positive counterpart: a gate
// that waits behind the row lock for LONGER than its own renewal TTL must
// still install a lease renewal (and stage marker) anchored to the POST-LOCK
// DB clock — never born expired.
func TestPGPublishGateTTLAnchoredPostLock(t *testing.T) {
	r, dsn, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "an-hold", "an-tok", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, claim, "original lease must be long enough to survive the block")
	newTTL := 400 * time.Millisecond

	blocker, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = blocker.Close(ctx) })
	var blockerPID int32
	require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID))
	txB, err := blocker.Begin(ctx)
	require.NoError(t, err)
	_, err = txB.Exec(ctx, `SELECT id FROM sync_runs WHERE id = $1 FOR UPDATE`, runID)
	require.NoError(t, err)

	gateRepo := singleConnRepo(t, dsn)
	gatePID := backendPID(t, gateRepo.Pool)
	gateCh := goBeginGate(t, gateRepo, runID, tenant, "an-hold", "an-tok", 1, newTTL)

	waitUntilBlockedBy(t, r.Pool, gatePID, blockerPID)
	enteredAt := dbClock(t, r.Pool)
	// Hold the lock for LONGER than the renewal TTL before releasing.
	for dbClock(t, r.Pool).Sub(enteredAt) < 700*time.Millisecond {
		time.Sleep(20 * time.Millisecond)
	}
	unlockBefore := dbClock(t, r.Pool)
	require.NoError(t, txB.Rollback(ctx))

	gate := mustRecv(t, gateCh, "gate after long block")
	require.NoError(t, gate.err)
	require.NotNil(t, gate.res)
	assert.Equal(t, PublishGateEntered, gate.res.Outcome,
		"holding the lock longer than the NEW ttl must not expire the still-valid lease")
	require.NotNil(t, gate.res.Claim)
	assert.True(t, gate.res.Claim.ExpiresAt.After(unlockBefore.Add(newTTL-30*time.Millisecond)),
		"gate renewal must be anchored to the post-lock DB clock (ttl=%v)", newTTL)
	var live bool
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`, runID).Scan(&live))
	assert.True(t, live, "the just-renewed lease must be live")
	var publishEnteredAt *time.Time
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT publish_entered_at FROM sync_runs WHERE id = $1`, runID).Scan(&publishEnteredAt))
	require.NotNil(t, publishEnteredAt, "the stage marker must be set")
}

// ---------- group 5: controlled terminal stage matrix + legacy semantics ----------

func TestPGCancelControlledTerminalStageMatrix(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)

	newRun := func(t *testing.T, holder, token string) uuid.UUID {
		t.Helper()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(context.Background(), runID, tenant, holder, token, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		return runID
	}

	t.Run("pregate_succeeded_rejected_failed_is_honest", func(t *testing.T) {
		ctx := context.Background()
		runID := newRun(t, "m1-hold", "m1-tok")
		version, hash := uuid.New(), "matrix-hash"
		before := runRowJSON(t, r.Pool, runID)
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m1-hold", "m1-tok", 1,
			"succeeded", 5, 1, 5, nil, nil, &version, &hash)
		require.NoError(t, err)
		assert.Nil(t, done, "succeeded requires the publish gate first")
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "pregate succeeded rejection")

		// 'failed' stays the honest uncertain outcome without a gate.
		msg := "boom"
		failed, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m1-hold", "m1-tok", 1,
			"failed", 0, 0, 0, &msg, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, failed)
		assert.Equal(t, "failed", failed.Status)
		rewrite, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m1-hold", "m1-tok", 1,
			"failed", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, rewrite, "terminal states are immutable")
	})

	t.Run("cancel_first_succeeded_rejected_then_aborted_once", func(t *testing.T) {
		ctx := context.Background()
		runID := newRun(t, "m2-hold", "m2-tok")
		res, err := r.RequestCancelSyncRun(ctx, runID, defID, tenant, nil, "matrix cancel")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeRequested, res.Outcome)
		before := runRowJSON(t, r.Pool, runID)

		version, hash := uuid.New(), "matrix-hash"
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m2-hold", "m2-tok", 1,
			"succeeded", 5, 1, 5, nil, nil, &version, &hash)
		require.NoError(t, err)
		assert.Nil(t, done, "a cancel-won run can never be recorded as succeeded")
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "cancel-won succeeded rejection")

		aborted, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m2-hold", "m2-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, aborted)
		assert.Equal(t, "aborted", aborted.Status)
		for _, status := range []string{"aborted", "succeeded", "failed"} {
			again, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m2-hold", "m2-tok", 1,
				status, 0, 0, 0, nil, nil, nil, nil)
			require.NoError(t, err, status)
			assert.Nil(t, again, status+": terminal states are immutable")
		}
	})

	t.Run("gate_first_aborted_rejected_then_succeeded_once", func(t *testing.T) {
		ctx := context.Background()
		runID := newRun(t, "m3-hold", "m3-tok")
		gate, err := r.BeginPublishGate(ctx, runID, tenant, "m3-hold", "m3-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, PublishGateEntered, gate.Outcome)
		before := runRowJSON(t, r.Pool, runID)

		forced, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m3-hold", "m3-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, forced, "aborted is illegal once the publish gate was entered")
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "post-gate forced abort rejection")

		version, hash := uuid.New(), "matrix-hash"
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m3-hold", "m3-tok", 1,
			"succeeded", 7, 1, 7, nil, nil, &version, &hash)
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "succeeded", done.Status)
		require.NotNil(t, done.DatasetVersionID)
		assert.Equal(t, version, *done.DatasetVersionID)
		for _, status := range []string{"succeeded", "failed", "aborted"} {
			again, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "m3-hold", "m3-tok", 1,
				status, 0, 0, 0, nil, nil, nil, nil)
			require.NoError(t, err, status)
			assert.Nil(t, again, status+": terminal states are immutable")
		}
	})
}

func TestPGCancelLegacyUnsupportedSemantics(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()

	// Legacy running row: P2.1 semantics intact — no gate, no cancel.
	legacy, err := r.RunSyncJob(ctx, defID, tenant)
	require.NoError(t, err)
	require.NotNil(t, legacy)
	assert.Equal(t, "running", legacy.Status)
	before := runRowJSON(t, r.Pool, legacy.ID)

	actor := uuid.New()
	res, err := r.RequestCancelSyncRun(ctx, legacy.ID, defID, tenant, &actor, "legacy cancel")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, CancelOutcomeUnsupported, res.Outcome,
		"legacy rows never publish-gate: NULL stage columns prove nothing")
	require.NotNil(t, res.Run)
	assert.Equal(t, "running", res.Run.Status)
	assert.Nil(t, res.Run.CancelRequestedAt)
	assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, legacy.ID), "unsupported legacy cancel")

	g, err := r.BeginPublishGate(ctx, legacy.ID, tenant, "any-hold", "any-tok", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, g)
	assert.Equal(t, PublishGateFenced, g.Outcome)
	assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, legacy.ID), "legacy gate refusal")

	leaseOK, cancelRequested, err := r.CheckSyncRunControl(ctx, legacy.ID, tenant, "any-hold", "any-tok", 1)
	require.NoError(t, err)
	assert.False(t, leaseOK)
	assert.False(t, cancelRequested)

	// The legacy completion path still works exactly once, WITHOUT any gate —
	// the two stage machines never bleed into each other.
	version := uuid.New()
	closed, err := r.CompleteSyncRun(ctx, legacy.ID, tenant, "succeeded", 3, 1, 3, nil, nil, &version, nil)
	require.NoError(t, err)
	require.NotNil(t, closed)
	assert.Equal(t, "succeeded", closed.Status)
	rewrite, err := r.CompleteSyncRun(ctx, legacy.ID, tenant, "failed", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, rewrite)

	// A legacy TERMINAL row still classifies as Unsupported (control mode
	// outranks state in the classification — never AlreadyTerminal).
	res2, err := r.RequestCancelSyncRun(ctx, legacy.ID, defID, tenant, &actor, "again")
	require.NoError(t, err)
	require.NotNil(t, res2)
	assert.Equal(t, CancelOutcomeUnsupported, res2.Outcome)

	// A CONTROLLED run cannot be smuggled through the legacy no-token path.
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "sm-hold", "sm-tok", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim)
	controlledBefore := runRowJSON(t, r.Pool, runID)
	smuggled, err := r.CompleteSyncRun(ctx, runID, tenant, "succeeded", 9, 9, 9, nil, nil, &version, nil)
	require.NoError(t, err)
	assert.Nil(t, smuggled, "leased controlled runs must use the lease CAS path")
	assertCancelRowEqual(t, controlledBefore, runRowJSON(t, r.Pool, runID), "legacy-path smuggling rejection")
}

// ---------- group 6: tenant & parent binding matrix ----------
//
// RepoTenant carries no admin role, so no repo-level admin case exists here;
// HTTP-layer admin/Source-write authorization is covered by separate real-HTTP
// evidence, not by this file.

func TestPGCancelBindingMatrix(t *testing.T) {
	sweep := func(t *testing.T, r *Repo, owner Tenant, defID uuid.UUID, runID uuid.UUID, attackers map[string]Tenant) {
		t.Helper()
		ctx := context.Background()
		before := runRowJSON(t, r.Pool, runID)
		actor := uuid.New()
		for name, attacker := range attackers {
			res, err := r.RequestCancelSyncRun(ctx, runID, defID, attacker, &actor, "nope")
			require.NoError(t, err, name)
			require.NotNil(t, res, name)
			assert.Equal(t, CancelOutcomeNotVisible, res.Outcome, name)
			assert.Nil(t, res.Run, name)

			run, err := r.GetSyncRunForTenant(ctx, defID, runID, attacker)
			require.NoError(t, err, name)
			assert.Nil(t, run, name+": cross-domain get")

			g, err := r.BeginPublishGate(ctx, runID, attacker, "bd-hold", "bd-tok", 1, time.Minute)
			require.NoError(t, err, name)
			require.NotNil(t, g, name)
			assert.Equal(t, PublishGateFenced, g.Outcome, name+": cross-domain gate")
			assert.Nil(t, g.Claim, name)
			assert.Nil(t, g.Run, name)

			leaseOK, cancelRequested, err := r.CheckSyncRunControl(ctx, runID, attacker, "bd-hold", "bd-tok", 1)
			require.NoError(t, err, name)
			assert.False(t, leaseOK, name+": cross-domain control probe lease")
			assert.False(t, cancelRequested, name+": cross-domain control probe cancel")
		}
		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "foreign binding attempts")
	}

	t.Run("org_domain_row_vs_foreign_callers", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "bd-hold", "bd-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		sweep(t, r, tenant, defID, runID, map[string]Tenant{
			"same subject personal domain": {OwnerID: tenant.OwnerID},
			"same subject other org":       {OwnerID: tenant.OwnerID, OrgID: orgPtr(uuid.New())},
			"other owner other org":        {OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())},
			"other personal owner":         {OwnerID: uuid.New()},
		})
	})

	t.Run("personal_domain_row_vs_foreign_callers", func(t *testing.T) {
		r, _, personal, sourceID, defID := leaseFixtureDomain(t, false)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, personal, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, personal, "bd-hold", "bd-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		sweep(t, r, personal, defID, runID, map[string]Tenant{
			"same subject in an org": {OwnerID: personal.OwnerID, OrgID: orgPtr(uuid.New())},
			"other owner in an org":  {OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())},
			"other personal owner":   {OwnerID: uuid.New()},
		})
	})

	t.Run("parent_sync_and_missing_run_bindings", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, tenant, "pb-hold", "pb-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		before := runRowJSON(t, r.Pool, runID)

		// A different def in the SAME tenant: the run↔parent binding must
		// fail for every API that can express it.
		otherJob, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: sourceID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, otherJob)

		mismatch, err := r.RequestCancelSyncRun(ctx, runID, otherJob.ID, tenant, nil, "wrong parent")
		require.NoError(t, err)
		require.NotNil(t, mismatch)
		assert.Equal(t, CancelOutcomeNotVisible, mismatch.Outcome, "run↔sync mismatch must be invisible")
		assert.Nil(t, mismatch.Run)

		missingRun, err := r.GetSyncRunForTenant(ctx, otherJob.ID, runID, tenant)
		require.NoError(t, err)
		assert.Nil(t, missingRun)

		// BeginGate/CheckSyncRunControl bind by runID+tenant only (their API
		// carries no parent id); the nonexistent-run forms still fence
		// cleanly.
		ghost := uuid.New()
		g, err := r.BeginPublishGate(ctx, ghost, tenant, "pb-hold", "pb-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, g)
		assert.Equal(t, PublishGateFenced, g.Outcome)

		res, err := r.RequestCancelSyncRun(ctx, ghost, defID, tenant, nil, "ghost")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeNotVisible, res.Outcome)

		run, err := r.GetSyncRunForTenant(ctx, defID, ghost, tenant)
		require.NoError(t, err)
		assert.Nil(t, run)

		leaseOK, cancelRequested, err := r.CheckSyncRunControl(ctx, ghost, tenant, "pb-hold", "pb-tok", 1)
		require.NoError(t, err)
		assert.False(t, leaseOK)
		assert.False(t, cancelRequested)

		assertCancelRowEqual(t, before, runRowJSON(t, r.Pool, runID), "binding mismatch attempts")
	})
}

// ---------- group 7: genuine pre-P2.2 database upgrade & replay ----------

// TestPGCancelPreP22UpgradeReplaySafety builds a REAL pre-P2.2 database (only
// migrations strictly before the cancel/gate file are applied; the control/
// cancel/stage columns provably do not exist yet — never a new table with
// dropped columns), seeds P2.1-era rows, upgrades, and proves the replay
// never resets old artifacts or live/new lease and stage state.
func TestPGCancelPreP22UpgradeReplaySafety(t *testing.T) {
	r, _ := bootRepoWithDSN(t)
	ctx := context.Background()
	applyMigrationsExcept(ctx, t, r.Pool, "20261113110000")

	var p22Cols int
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM information_schema.columns
		  WHERE table_name = 'sync_runs'
		    AND column_name IN ('control_mode','cancel_requested_at','cancelled_by','publish_entered_at')`).Scan(&p22Cols))
	require.Zero(t, p22Cols, "fixture must be a genuine pre-P2.2 database")

	// Seed P2.1-era rows: a terminal run with real artifacts, a lease-less
	// running row, and a running row holding a live lease.
	legacyOwner := uuid.New()
	personal := Tenant{OwnerID: legacyOwner}
	legacySource := uuid.New()
	_, err := r.Pool.Exec(ctx,
		`INSERT INTO connections (id, name, connector_type, config, owner_id) VALUES ($1,'p22-legacy','postgresql','{}',$2)`,
		legacySource, legacyOwner)
	require.NoError(t, err)
	legacyDef := uuid.New()
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO batch_sync_defs (id, source_id, output_dataset_id) VALUES ($1,$2,$3)`,
		legacyDef, legacySource, uuid.New())
	require.NoError(t, err)

	legacyTerminal := uuid.New()
	legacyVersion := uuid.New()
	legacyHash := "p22-legacy-hash-keep"
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, dataset_version_id, content_hash, bytes_written, rows_written, finished_at)
		 VALUES ($1, $2, 'succeeded', $3, $4, 22766, 151, now())`, legacyTerminal, legacyDef, legacyVersion, legacyHash)
	require.NoError(t, err)
	legacyRunning := uuid.New()
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, started_at) VALUES ($1, $2, 'running', now())`,
		legacyRunning, legacyDef)
	require.NoError(t, err)
	legacyLeased := uuid.New()
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, started_at, attempt, generation, requested_by,
		                        lease_holder, lease_token, lease_expires_at, heartbeat_at)
		 VALUES ($1, $2, 'running', now(), 1, 1, $3, 'seed-hold', 'seed-tok', now() + interval '1 hour', now())`,
		legacyLeased, legacyDef, legacyOwner)
	require.NoError(t, err)

	// Upgrade, then replay: nothing resets; old rows default to legacy.
	require.NoError(t, Migrate(ctx, r.Pool))
	require.NoError(t, Migrate(ctx, r.Pool), "replay stays idempotent")

	var mode string
	var gotVersion uuid.UUID
	var gotHash string
	var gotBytes, gotRows int64
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT control_mode, dataset_version_id, content_hash, bytes_written, rows_written
		   FROM sync_runs WHERE id = $1`, legacyTerminal).
		Scan(&mode, &gotVersion, &gotHash, &gotBytes, &gotRows))
	assert.Equal(t, models.RunControlLegacy, mode, "upgraded old rows default to legacy control")
	assert.Equal(t, legacyVersion, gotVersion, "old terminal version preserved")
	assert.Equal(t, legacyHash, gotHash, "old terminal hash preserved")
	assert.Equal(t, int64(22766), gotBytes)
	assert.Equal(t, int64(151), gotRows)

	// Old NULL stage columns are NOT read as "not published": cancel refuses
	// on both legacy running rows and the live legacy lease cannot gate.
	res, err := r.RequestCancelSyncRun(ctx, legacyRunning, legacyDef, personal, nil, "legacy")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, CancelOutcomeUnsupported, res.Outcome)
	legacyRunningBefore := runRowJSON(t, r.Pool, legacyRunning)

	res3, err := r.RequestCancelSyncRun(ctx, legacyLeased, legacyDef, personal, nil, "legacy lease")
	require.NoError(t, err)
	require.NotNil(t, res3)
	assert.Equal(t, CancelOutcomeUnsupported, res3.Outcome)

	g, err := r.BeginPublishGate(ctx, legacyLeased, personal, "seed-hold", "seed-tok", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, g)
	assert.Equal(t, PublishGateFenced, g.Outcome, "a legacy-mode lease must not enter the publish gate")
	assertCancelRowEqual(t, legacyRunningBefore, runRowJSON(t, r.Pool, legacyRunning), "legacy running cancel refusal")

	// The old live lease keeps the bare P2.1 completion semantics: succeeded
	// WITHOUT any gate, exactly once.
	closed, err := r.CompleteLeasedSyncRun(ctx, legacyLeased, personal, "seed-hold", "seed-tok", 1,
		"succeeded", 7, 1, 7, nil, nil, &legacyVersion, &legacyHash)
	require.NoError(t, err)
	require.NotNil(t, closed)
	assert.Equal(t, "succeeded", closed.Status)
	rewrite, err := r.CompleteLeasedSyncRun(ctx, legacyLeased, personal, "seed-hold", "seed-tok", 1,
		"failed", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, rewrite)

	// New controlled lifecycle on the upgraded database survives replays
	// without a single column being reset.
	runID := leaseEnqueue(t, r, personal, legacySource, legacyDef, legacyOwner)
	var controlled string
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT control_mode FROM sync_runs WHERE id = $1`, runID).Scan(&controlled))
	assert.Equal(t, models.RunControlControlled, controlled, "every new enqueue is explicitly controlled")
	claim, err := r.ClaimSyncRun(ctx, runID, personal, "up-hold", "up-tok", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim)
	leaseOK, cancelRequested, err := r.CheckSyncRunControl(ctx, runID, personal, "up-hold", "up-tok", 1)
	require.NoError(t, err)
	assert.True(t, leaseOK)
	assert.False(t, cancelRequested)
	res2, err := r.RequestCancelSyncRun(ctx, runID, legacyDef, personal, nil, "upgrade cancel")
	require.NoError(t, err)
	require.NotNil(t, res2)
	assert.Equal(t, CancelOutcomeRequested, res2.Outcome)
	g2, err := r.BeginPublishGate(ctx, runID, personal, "up-hold", "up-tok", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, g2)
	assert.Equal(t, PublishGateCancelled, g2.Outcome)
	aborted, err := r.CompleteLeasedSyncRun(ctx, runID, personal, "up-hold", "up-tok", 1,
		"aborted", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, aborted)
	terminalRow := runRowJSON(t, r.Pool, runID)
	require.NoError(t, Migrate(ctx, r.Pool))
	require.NoError(t, Migrate(ctx, r.Pool))
	assertCancelRowEqual(t, terminalRow, runRowJSON(t, r.Pool, runID), "migration replay over a terminal controlled row")

	// A gate-entered (publishing) row also survives replay untouched.
	run2 := leaseEnqueue(t, r, personal, legacySource, legacyDef, legacyOwner)
	claim2, err := r.ClaimSyncRun(ctx, run2, personal, "up2-hold", "up2-tok", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim2)
	g3, err := r.BeginPublishGate(ctx, run2, personal, "up2-hold", "up2-tok", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, g3)
	assert.Equal(t, PublishGateEntered, g3.Outcome)
	gatedRow := runRowJSON(t, r.Pool, run2)
	require.NoError(t, Migrate(ctx, r.Pool))
	require.NoError(t, Migrate(ctx, r.Pool))
	assertCancelRowEqual(t, gatedRow, runRowJSON(t, r.Pool, run2), "migration replay over a gate-entered row")
}
