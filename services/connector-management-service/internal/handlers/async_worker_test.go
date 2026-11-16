// P2.2 interruptible-worker behaviour at the HTTP edge and inside the
// coordinator: async early-return with a real background publish, durable
// cancellation racing the publish gate in both lock orders, too-late
// observability, bounded admission (queue-full, shutdown races, TTLs anchored
// at Reserve), one executor with worker-bounded concurrency for BOTH waiting
// modes, honest stop budgets, restart non-replay, and full registry/goroutine
// convergence. No lease-internal value ever reaches a response.
package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// gatedUpstream serves one page per request (bodies[i], falling back to an
// empty-data page); while blocked, every request parks on the gate. It
// tracks peak in-flight requests (worker-pool bound proof) and records when
// a parked page's context is cancelled by the CLIENT side (real transport
// disconnect propagation), as a unix-nano timestamp (0 = not fired).
type gatedUpstream struct {
	mu       sync.Mutex
	gate     chan struct{}
	bodies   []string
	inFlight atomic.Int32
	peak     atomic.Int32
	served   atomic.Int32
	attempts atomic.Int32 // total page REQUESTS (parked ones included) — the honest "no next page" probe
	ctxDone  atomic.Int64
}

func (u *gatedUpstream) block() {
	u.mu.Lock()
	if u.gate == nil {
		u.gate = make(chan struct{})
	}
	u.mu.Unlock()
}

func (u *gatedUpstream) release() {
	u.mu.Lock()
	ch := u.gate
	u.gate = nil
	u.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (u *gatedUpstream) handler(w http.ResponseWriter, r *http.Request) {
	u.attempts.Add(1)
	u.inFlight.Add(1)
	cur := u.inFlight.Load()
	for {
		p := u.peak.Load()
		if cur <= p || u.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	u.mu.Lock()
	ch := u.gate
	u.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-r.Context().Done():
			// A parked page never becomes a stranded handler: a test failure
			// must not wedge Server.Close. Record the REAL client-side
			// cancellation time for disconnect-propagation assertions.
			u.ctxDone.Store(time.Now().UnixNano())
			u.inFlight.Add(-1)
			return
		}
	}
	idx := int(u.served.Add(1)) - 1
	u.inFlight.Add(-1)
	body := `{"data":[]}`
	u.mu.Lock()
	if idx < len(u.bodies) {
		body = u.bodies[idx]
	}
	u.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

func (u *gatedUpstream) peakInFlight() int32       { return u.peak.Load() }
func (u *gatedUpstream) servedPages() int32        { return u.served.Load() }
func (u *gatedUpstream) attemptsPages() int32      { return u.attempts.Load() }
func (u *gatedUpstream) clientCtxDoneNanos() int64 { return u.ctxDone.Load() }

// asyncEnv is a full rig: fake store + gated rest_api upstream + explicit
// coordinator with test bounds + fake commit port.
type asyncEnv struct {
	store     *fakeStore
	h         *handlers.Handlers
	coord     *handlers.RunCoordinator
	committer *fakeCommitPort
	upstream  *gatedUpstream
	job       *models.SyncJob
	owner     uuid.UUID
}

func newAsyncEnv(t *testing.T, cfg handlers.CoordinatorConfig) *asyncEnv {
	t.Helper()
	owner := uuid.New()
	up := &gatedUpstream{}
	srv := httptest.NewServer(http.HandlerFunc(up.handler))
	// Cleanup LIFO (reverse registration order): release the gate FIRST so no
	// parked page strands a handler, then Stop the coordinator, then close
	// the server. A require.Fatal mid-test can therefore never wedge
	// Server.Close.
	t.Cleanup(srv.Close)
	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(srv.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)
	// Fast cancel observation for interrupt tests.
	h.Config.SyncCancelPollInterval = 10 * time.Millisecond
	coord := handlers.NewRunCoordinator(context.Background(), cfg, h)
	h.Coordinator = coord
	t.Cleanup(func() { _ = coord.Stop() })
	t.Cleanup(up.release) // runs FIRST (LIFO)
	return &asyncEnv{store: store, h: h, coord: coord, committer: committer, upstream: up, job: job, owner: owner}
}

func (e *asyncEnv) postRun(t *testing.T, async bool) *httptest.ResponseRecorder {
	t.Helper()
	target := "/syncs/" + e.job.ID.String() + "/run"
	if async {
		target += "?async=true"
	}
	req := withRouteParam(authedReq(http.MethodPost, target, "", e.owner), "sync_id", e.job.ID.String())
	req.Header.Set("Authorization", "Bearer caller-platform-jwt")
	rec := httptest.NewRecorder()
	e.h.RunSyncJob(rec, req)
	return rec
}

func (e *asyncEnv) postRunCtx(t *testing.T, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+e.job.ID.String()+"/run", "", e.owner).WithContext(ctx), "sync_id", e.job.ID.String())
	req.Header.Set("Authorization", "Bearer caller-platform-jwt")
	rec := httptest.NewRecorder()
	e.h.RunSyncJob(rec, req)
	return rec
}

func (e *asyncEnv) getRun(t *testing.T, runID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	req := withRouteParam(withRouteParam(authedReq(http.MethodGet, "/syncs/"+e.job.ID.String()+"/runs/"+runID.String(), "", e.owner), "id", e.job.ID.String()), "run_id", runID.String())
	rec := httptest.NewRecorder()
	e.h.GetSyncRun(rec, req)
	return rec
}

func (e *asyncEnv) cancelRun(t *testing.T, runID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	req := withRouteParam(withRouteParam(authedReq(http.MethodPost, "/syncs/"+e.job.ID.String()+"/runs/"+runID.String()+"/cancel", "", e.owner), "id", e.job.ID.String()), "run_id", runID.String())
	rec := httptest.NewRecorder()
	e.h.CancelSyncRun(rec, req)
	return rec
}

func decodeRun(t *testing.T, rec *httptest.ResponseRecorder) models.SyncRun {
	t.Helper()
	var run models.SyncRun
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &run), rec.Body.String())
	return run
}

// decodeCancelRun parses the cancel endpoint's wrapper ({error, code, run})
// and verifies the embedded run is the one asked about.
func decodeCancelRun(t *testing.T, rec *httptest.ResponseRecorder, wantRunID uuid.UUID) models.SyncRun {
	t.Helper()
	var body struct {
		Error string          `json:"error"`
		Code  string          `json:"code"`
		Run   *models.SyncRun `json:"run"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	if body.Run == nil {
		t.Fatalf("cancel response carries no run: %s", rec.Body.String())
	}
	require.Equal(t, wantRunID, body.Run.ID, "cancel response must embed the requested run")
	return *body.Run
}

func (e *asyncEnv) storedRunByID(t *testing.T, runID uuid.UUID) models.SyncRun {
	t.Helper()
	run, err := e.store.GetSyncRunForTenant(t.Context(), e.job.ID, runID, repo.Tenant{OwnerID: e.owner})
	require.NoError(t, err)
	require.NotNil(t, run)
	return *run
}

func (e *asyncEnv) eventuallyStatus(t *testing.T, runID uuid.UUID, statuses ...string) models.SyncRun {
	t.Helper()
	var last models.SyncRun
	require.Eventually(t, func() bool {
		last = e.storedRunByID(t, runID)
		for _, s := range statuses {
			if last.Status == s {
				return true
			}
		}
		return false
	}, 5*time.Second, 15*time.Millisecond, "run should reach %v, last: %s", statuses, last.Status)
	return last
}

func fastCfg() handlers.CoordinatorConfig {
	return handlers.CoordinatorConfig{QueueDepth: 4, Workers: 1, AuthMemoryTTL: 5 * time.Second, TaskDeadline: 5 * time.Second, StopBudget: 5 * time.Second}
}

// 1. Async returns 202 with the pending run BEFORE any publish; the real
// publish happens after the response, carrying the caller's Authorization.
func TestAsyncRunEarlyReturnPublishesAfterResponse(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block() // the pull cannot finish before we release it

	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	run := decodeRun(t, rec)
	assert.Equal(t, "pending", run.Status, "202 must carry the pending run, not a fabricated outcome")
	assert.Equal(t, models.RunControlControlled, run.ControlMode)

	// Response returned while the page is still blocked: prove ordering.
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	env.upstream.release()
	done := env.eventuallyStatus(t, run.ID, "succeeded")
	assert.Equal(t, "succeeded", done.Status)
	reqs, auths := env.committer.snapshot()
	require.Len(t, reqs, 1, "exactly one real publish after the response")
	assert.Equal(t, "Bearer caller-platform-jwt", auths[0], "publish carries the caller's forwarded JWT")
	assert.NotNil(t, done.DatasetVersionID)
}

// 2. Durable cancel interrupts a slow in-flight page through the DB probe
// (not a local map): run ends aborted, zero publishes.
func TestAsyncCancelDuringSlowPullAbortsNoPublish(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()

	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code)
	run := decodeRun(t, rec)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	cancelRec := env.cancelRun(t, run.ID)
	require.Equal(t, http.StatusAccepted, cancelRec.Code, cancelRec.Body.String())
	cancelled := decodeCancelRun(t, cancelRec, run.ID)
	require.NotNil(t, cancelled.CancelRequestedAt, "the request must be durably persisted and visible")

	done := env.eventuallyStatus(t, run.ID, "aborted")
	assert.Equal(t, "aborted", done.Status)
	assert.Contains(t, *done.Error, "cancel")
	reqs, _ := env.committer.snapshot()
	assert.Empty(t, reqs, "no publish may happen for a cancelled run")
	env.upstream.release() // let the parked page handler exit
}

// 2b. The DEFAULT synchronous path is equally cancellable: the handler
// answers 202 with the aborted terminal run.
func TestSyncCancelDuringSlowPullAborts(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { recCh <- env.postRun(t, false) }()
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)
	runs, err := env.store.ListSyncRuns(t.Context(), env.job.ID, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)

	cancelRec := env.cancelRun(t, runs[0].ID)
	require.Equal(t, http.StatusAccepted, cancelRec.Code, cancelRec.Body.String())

	rec := <-recCh
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	done := decodeRun(t, rec)
	assert.Equal(t, "aborted", done.Status, "synchronous trigger reports the persisted aborted state")
	reqs, _ := env.committer.snapshot()
	assert.Empty(t, reqs)
	env.upstream.release()
}

// 4. Cancel that wins BEFORE the gate: the gate refuses, the holder confirms
// aborted, and CommitOutput is never called — even though the pull finished.
func TestCancelBeforeGateAbortsAtGateZeroPublishes(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	// Pull completes; the cancel lands inside the gate decision window.
	env.store.gateHook = func() (*repo.PublishGateResult, error) {
		_, err := env.store.RequestCancelSyncRun(context.Background(), mustRunID(t, env), env.job.ID, repo.Tenant{OwnerID: env.owner}, &env.owner, "cancelled by request")
		require.NoError(t, err)
		cp := env.storedRunByID(t, mustRunID(t, env))
		return &repo.PublishGateResult{Outcome: repo.PublishGateCancelled, Run: &cp}, nil
	}

	rec := env.postRun(t, false)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	done := decodeRun(t, rec)
	assert.Equal(t, "aborted", done.Status)
	assert.Nil(t, done.PublishEnteredAt, "cancel won: the gate must not be entered")
	reqs, _ := env.committer.snapshot()
	assert.Empty(t, reqs, "cancel-first proves zero CommitOutput calls")
}

func mustRunID(t *testing.T, env *asyncEnv) uuid.UUID {
	t.Helper()
	runs, err := env.store.ListSyncRuns(t.Context(), env.job.ID, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	return runs[0].ID
}

// 5. Gate-first: a cancel arriving after the publish gate is 409 too-late
// with a machine-readable code; the run completes under its REAL outcome and
// is never marked aborted.
func TestCancelAfterGateIsTooLateRunCompletes(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	// Pull finishes instantly; the publish itself blocks so the cancel
	// deterministically lands after the gate entered.
	env.committer.mu.Lock()
	env.committer.block = make(chan struct{})
	env.committer.mu.Unlock()

	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code)
	run := decodeRun(t, rec)

	// The executor reached the publish (gate entered) and is parked there.
	require.Eventually(t, func() bool {
		reqs, _ := env.committer.snapshot()
		return len(reqs) == 1
	}, 3*time.Second, 10*time.Millisecond)
	stored := env.storedRunByID(t, run.ID)
	require.NotNil(t, stored.PublishEnteredAt, "gate must be entered before the publish port is called")

	cancelRec := env.cancelRun(t, run.ID)
	require.Equal(t, http.StatusConflict, cancelRec.Code, cancelRec.Body.String())
	var body struct {
		Error string         `json:"error"`
		Code  string         `json:"code"`
		Run   models.SyncRun `json:"run"`
	}
	require.NoError(t, json.Unmarshal(cancelRec.Body.Bytes(), &body))
	assert.Equal(t, "cancel_too_late", body.Code, "machine-readable too-late marker")
	assert.NotNil(t, body.Run.PublishEnteredAt, "the stage marker is observable on the refused cancel")
	assert.Equal(t, "running", body.Run.Status)

	// Release the publish: the run completes under its real outcome.
	env.committer.mu.Lock()
	close(env.committer.block)
	env.committer.block = nil
	env.committer.mu.Unlock()
	done := env.eventuallyStatus(t, run.ID, "succeeded")
	assert.Equal(t, "succeeded", done.Status, "a too-late cancel must never abort a gate-entered run")
}

// 7. Bounded admission: capacity is reserved BEFORE any durable row; a full
// queue answers 503 with zero new rows.
func TestQueueFullReturns503WithZeroRows(t *testing.T) {
	env := newAsyncEnv(t, handlers.CoordinatorConfig{QueueDepth: 1, Workers: 1, AuthMemoryTTL: 5 * time.Second, TaskDeadline: 5 * time.Second, StopBudget: 5 * time.Second})
	env.upstream.block()

	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	second := env.postRun(t, true)
	require.Equal(t, http.StatusServiceUnavailable, second.Code, second.Body.String())
	runs, err := env.store.ListSyncRuns(t.Context(), env.job.ID, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	assert.Len(t, runs, 1, "queue-full must not create any durable row")

	env.upstream.release()
}

// 8 (A/D barrier). A shutdown racing an in-flight admission: Stop waits for
// the reservation; the late Dispatch refuses, the handler closes the durable
// row FIRST and releases after — no silent pending, no pool-close-before-
// cleanup window.
func TestAdmissionShutdownRaceClosesPendingNotSilent(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()

	// First async task occupies the worker.
	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	// Second async POST parks INSIDE EnqueueSyncRun.
	env.store.mu.Lock()
	env.store.enqueueBlock = make(chan struct{})
	env.store.enqueueEntered = make(chan struct{}, 1)
	env.store.mu.Unlock()
	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { recCh <- env.postRun(t, true) }()
	<-env.store.enqueueEntered

	stopErr := make(chan error, 1)
	go func() { stopErr <- env.coord.Stop() }()
	// Let Stop observe the world (gate closes, running execution interrupted).
	time.Sleep(100 * time.Millisecond)
	close(env.store.enqueueBlock)

	rec := <-recCh
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())

	// The 503 body is an error object — capture the real second row from the
	// store (the only row that is not the first run) instead of parsing it as
	// a run.
	firstRun := decodeRun(t, first)
	runs, err := env.store.ListSyncRuns(t.Context(), env.job.ID, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	secondID := uuid.Nil
	for _, r := range runs {
		if r.ID != firstRun.ID {
			secondID = r.ID
		}
	}
	require.NotEqual(t, uuid.Nil, secondID)

	// Both rows converge; nothing stays silently pending.
	env.eventuallyStatus(t, firstRun.ID, "aborted", "failed")
	done := env.eventuallyStatus(t, secondID, "aborted")
	assert.Contains(t, *done.Error, "shutdown")

	require.NoError(t, <-stopErr, "Stop must join every reservation, including the raced one")
	require.Nil(t, env.coord.Reserve(), "no admissions after Stop")
	env.upstream.release()
}

// 9 (B). Stop drains the queue deterministically, interrupts the live
// execution, joins everything, and the registry/active sets converge.
func TestStopDrainsQueueInterruptsActiveAndJoins(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()

	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)
	queued := []uuid.UUID{}
	for i := 0; i < 2; i++ {
		rec := env.postRun(t, true)
		require.Equal(t, http.StatusAccepted, rec.Code)
		queued = append(queued, decodeRun(t, rec).ID)
	}
	require.Eventually(t, func() bool { return env.coord.OutstandingAdmissions() == 3 }, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, env.coord.Stop())
	// Executing task: interrupted pre-gate → aborted (finalize-on-interrupt)
	// or failed; queued tasks: deterministically aborted.
	env.eventuallyStatus(t, decodeRun(t, first).ID, "aborted", "failed")
	for _, id := range queued {
		done := env.eventuallyStatus(t, id, "aborted")
		assert.Contains(t, *done.Error, "shutdown before dispatch")
	}
	assert.Zero(t, env.coord.AuthRefCount(), "registry must drain")
	assert.Zero(t, env.coord.ActiveExecutions(), "active executions must drain")
	require.Nil(t, env.coord.Reserve())
	env.upstream.release()
}

// 10 (B). A stop whose close path is slower than the budget: Stop returns an
// explicit error naming the unconfirmed rows — within the budget — and never
// fabricates their terminal state.
func TestStopBudgetExhaustedReportsHonestError(t *testing.T) {
	env := newAsyncEnv(t, handlers.CoordinatorConfig{QueueDepth: 4, Workers: 1, AuthMemoryTTL: 5 * time.Second, TaskDeadline: 5 * time.Second, StopBudget: 300 * time.Millisecond})
	env.upstream.block()

	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)
	var queuedIDs []uuid.UUID
	for i := 0; i < 2; i++ {
		rec := env.postRun(t, true)
		require.Equal(t, http.StatusAccepted, rec.Code)
		queuedIDs = append(queuedIDs, decodeRun(t, rec).ID)
	}
	require.Eventually(t, func() bool { return env.coord.OutstandingAdmissions() == 3 }, 2*time.Second, 10*time.Millisecond)

	// Every durable cancel blocks: the closes cannot possibly finish.
	env.store.mu.Lock()
	env.store.cancelBlock = make(chan struct{})
	env.store.mu.Unlock()

	start := time.Now()
	err := env.coord.Stop()
	elapsed := time.Since(start)
	require.Error(t, err, "an exhausted stop must be reported, never claimed graceful")
	assert.Contains(t, err.Error(), "queued tasks could not be closed")
	assert.Less(t, elapsed, 5*time.Second, "Stop must respect its shared budget")

	for _, id := range queuedIDs {
		run := env.storedRunByID(t, id)
		assert.Equal(t, "pending", run.Status, "unclosed rows stay honestly pending, never fabricated")
	}
	assert.Zero(t, env.coord.AuthRefCount(), "slots and auth refs are still released")
	env.upstream.release()
	close(env.store.cancelBlock)
}

// 11 (C barrier). TTLs anchor at Reserve: an enqueue slower than the auth
// TTL dooms the task regardless of when Dispatch happens.
func TestAuthTTLAnchoredAtReserveSlowEnqueue(t *testing.T) {
	env := newAsyncEnv(t, handlers.CoordinatorConfig{QueueDepth: 4, Workers: 1, AuthMemoryTTL: 60 * time.Millisecond, TaskDeadline: 5 * time.Second, StopBudget: 5 * time.Second})
	env.store.mu.Lock()
	env.store.enqueueBlock = make(chan struct{})
	env.store.enqueueEntered = make(chan struct{}, 1)
	env.store.mu.Unlock()

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { recCh <- env.postRun(t, true) }()
	<-env.store.enqueueEntered
	time.Sleep(120 * time.Millisecond) // > AuthMemoryTTL: the budget is spent
	close(env.store.enqueueBlock)

	rec := <-recCh
	require.Equal(t, http.StatusAccepted, rec.Code)
	run := decodeRun(t, rec)

	done := env.eventuallyStatus(t, run.ID, "aborted")
	require.NotNil(t, done.Error)
	assert.Contains(t, *done.Error, "authorization memory TTL expired", "the expiry must be anchored at Reserve, not Dispatch")
	reqs, _ := env.committer.snapshot()
	assert.Empty(t, reqs)
	require.Eventually(t, func() bool { return env.coord.AuthRefCount() == 0 }, 2*time.Second, 10*time.Millisecond)
	adm := env.coord.Reserve() // capacity came back
	require.NotNil(t, adm)
	adm.Release()
}

// 12 (C). The overall task deadline bounds the execution itself (deadline
// exceeded mid-pull converges to failed, never hangs).
func TestTaskDeadlineBoundsExecution(t *testing.T) {
	env := newAsyncEnv(t, handlers.CoordinatorConfig{QueueDepth: 4, Workers: 1, AuthMemoryTTL: 5 * time.Second, TaskDeadline: 150 * time.Millisecond, StopBudget: 5 * time.Second})
	env.upstream.block()

	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code)
	run := decodeRun(t, rec)

	done := env.eventuallyStatus(t, run.ID, "failed")
	require.NotNil(t, done.Error)
	// The runtime sanitizes timeout causes; both phrasings prove the deadline
	// (not a hang) ended the pull.
	if !strings.Contains(*done.Error, "context deadline exceeded") && !strings.Contains(*done.Error, "timed out") {
		t.Fatalf("expected a deadline/timeout cause, got %q", *done.Error)
	}
	env.upstream.release()
}

// 13 (E barrier). ONE executor, worker-bounded concurrency for BOTH modes:
// with Workers=1, an async task blocking the worker plus a burst of
// synchronous requests — and a sync-only burst — never exceed 1 concurrent
// upstream request.
func TestWorkerPoolBoundsPeakConcurrencyBothModes(t *testing.T) {
	env := newAsyncEnv(t, handlers.CoordinatorConfig{QueueDepth: 8, Workers: 1, AuthMemoryTTL: 10 * time.Second, TaskDeadline: 10 * time.Second, StopBudget: 5 * time.Second})
	env.upstream.block()

	// Phase A: async blocked + four concurrent sync triggers.
	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	recs := make(chan *httptest.ResponseRecorder, 4)
	for i := 0; i < 4; i++ {
		go func() { recs <- env.postRun(t, false) }()
	}
	require.Eventually(t, func() bool { return env.coord.OutstandingAdmissions() == 5 }, 3*time.Second, 10*time.Millisecond,
		"all five admissions must be registered before release")

	env.upstream.release()
	for i := 0; i < 4; i++ {
		rec := <-recs
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	}
	env.eventuallyStatus(t, decodeRun(t, first).ID, "succeeded")

	// Phase B: sync-only burst.
	env.upstream.block()
	for i := 0; i < 3; i++ {
		go func() { recs <- env.postRun(t, false) }()
	}
	require.Eventually(t, func() bool { return env.coord.OutstandingAdmissions() == 3 }, 3*time.Second, 10*time.Millisecond)
	env.upstream.release()
	for i := 0; i < 3; i++ {
		rec := <-recs
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	}

	assert.LessOrEqual(t, env.upstream.peakInFlight(), int32(1),
		"with one worker, peak upstream concurrency must stay at 1 for both modes")
}

// 14. Legacy rows: cancel is refused with a machine-readable legacy code —
// their NULL stage columns prove nothing about remote publishes.
func TestLegacyRunCancelRejected409(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	legacyRun := models.SyncRun{ID: uuid.New(), SyncDefID: env.job.ID, Status: "running", StartedAt: time.Now().UTC(), ControlMode: models.RunControlLegacy}
	env.store.mu.Lock()
	env.store.runs[env.job.ID] = append(env.store.runs[env.job.ID], legacyRun)
	env.store.mu.Unlock()

	rec := env.cancelRun(t, legacyRun.ID)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	var body struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "cancel_unsupported_legacy", body.Code)
}

// 15. Double binding: a run fetched or cancelled under the WRONG parent sync
// id (or a foreign tenant) is a 404, rows untouched.
func TestGetAndCancelEnforceDoubleBinding(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	otherOwner := uuid.New()
	// A SECOND def in the SAME tenant (the fake store refuses cross-tenant
	// defs for this source — which is itself the isolation under test).
	otherJob, err := env.store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: env.job.SourceID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	require.NotEqual(t, env.job.ID, otherJob.ID)

	env.upstream.block()
	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	run := decodeRun(t, first)

	// Same tenant, wrong parent sync: 404 via the sync-binding mismatch.
	req := withRouteParam(withRouteParam(authedReq(http.MethodGet, "/syncs/"+otherJob.ID.String()+"/runs/"+run.ID.String(), "", env.owner), "id", otherJob.ID.String()), "run_id", run.ID.String())
	rec := httptest.NewRecorder()
	env.h.GetSyncRun(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	req = withRouteParam(withRouteParam(authedReq(http.MethodPost, "/syncs/"+otherJob.ID.String()+"/runs/"+run.ID.String()+"/cancel", "", env.owner), "id", otherJob.ID.String()), "run_id", run.ID.String())
	rec = httptest.NewRecorder()
	env.h.CancelSyncRun(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Foreign tenant: 404, row untouched.
	foreignReq := withRouteParam(withRouteParam(authedReq(http.MethodGet, "/syncs/"+env.job.ID.String()+"/runs/"+run.ID.String(), "", otherOwner), "id", env.job.ID.String()), "run_id", run.ID.String())
	rec = httptest.NewRecorder()
	env.h.GetSyncRun(rec, foreignReq)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	stored := env.storedRunByID(t, run.ID)
	// Claim-vs-check timing is genuinely transient with a blocked page: the
	// INVARIANT is that the row is still an uncancelled active state.
	assert.Contains(t, []string{"pending", "running"}, stored.Status)
	assert.Nil(t, stored.CancelRequestedAt, "no refused cross-binding call may leave a cancel request")
	env.upstream.release()
	env.eventuallyStatus(t, run.ID, "succeeded")
}

// 16. Restart non-replay: a coordinator NEVER adopts pre-existing pending
// rows — no claim, no upstream request, no publish.
func TestCoordinatorNeverAdoptsPreexistingPendingRows(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	run, err := env.store.EnqueueSyncRun(t.Context(), env.job.ID, env.job.SourceID, repo.Tenant{OwnerID: env.owner}, env.owner)
	require.NoError(t, err)
	require.NotNil(t, run)

	time.Sleep(150 * time.Millisecond) // give a (wrong) worker every chance
	stored := env.storedRunByID(t, run.ID)
	assert.Equal(t, "pending", stored.Status, "pre-existing rows are never claimed or replayed")
	env.store.mu.Lock()
	claims := env.store.claimCalls
	env.store.mu.Unlock()
	assert.Zero(t, claims)
	assert.Zero(t, env.upstream.servedPages())
	reqs, _ := env.committer.snapshot()
	assert.Empty(t, reqs)
}

// 17. Idempotent cancel: repeats keep the FIRST request verbatim (actor
// included) and return the same row.
func TestCancelIdempotentKeepsFirstRequest(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()
	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code)
	run := decodeRun(t, rec)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	first := env.cancelRun(t, run.ID)
	require.Equal(t, http.StatusAccepted, first.Code)
	firstRun := decodeCancelRun(t, first, run.ID)
	require.NotNil(t, firstRun.CancelRequestedAt)
	require.NotNil(t, firstRun.CancelledBy)
	assert.Equal(t, env.owner, *firstRun.CancelledBy)

	// A system nil-actor close and a second user cancel must not overwrite.
	_, err := env.store.RequestCancelSyncRun(t.Context(), run.ID, env.job.ID, repo.Tenant{OwnerID: env.owner}, nil, "system cause")
	require.NoError(t, err)
	other := uuid.New()
	_, err = env.store.RequestCancelSyncRun(t.Context(), run.ID, env.job.ID, repo.Tenant{OwnerID: env.owner}, &other, "second user")
	require.NoError(t, err)
	stored := env.storedRunByID(t, run.ID)
	assert.Equal(t, *firstRun.CancelRequestedAt, *stored.CancelRequestedAt, "first timestamp preserved")
	require.NotNil(t, stored.CancelledBy)
	assert.Equal(t, env.owner, *stored.CancelledBy, "first actor preserved (nil system overwrite forbidden)")

	env.eventuallyStatus(t, run.ID, "aborted")
	env.upstream.release()
}

// 20. Cancelling a still-QUEUED task aborts it outright and immediately
// drops the Authorization reference (bounded registry), while the queue
// handle itself converges when the worker eventually pops it.
func TestCancelQueuedTaskAbortsAndDropsAuthRef(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()

	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)
	second := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, second.Code)
	secondRun := decodeRun(t, second)
	require.Equal(t, 2, env.coord.OutstandingAdmissions())

	rec := env.cancelRun(t, secondRun.ID)
	require.Equal(t, http.StatusOK, rec.Code, "pending cancel is an immediate abort")
	aborted := decodeCancelRun(t, rec, secondRun.ID)
	assert.Equal(t, "aborted", aborted.Status)
	assert.True(t, env.coord.AuthRefCleared(secondRun.ID), "terminal row: the auth reference must be dropped eagerly")

	env.upstream.release()
	env.eventuallyStatus(t, decodeRun(t, first).ID, "succeeded")
	require.Eventually(t, func() bool { return env.coord.AuthRefCount() == 0 }, 3*time.Second, 10*time.Millisecond,
		"the queued handle converges when the worker pops the already-terminal row")
}

// 19. No lease-internal value ever appears in any new endpoint's response.
func TestNoLeaseInternalsInResponses(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()
	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code)
	run := decodeRun(t, rec)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	env.store.mu.Lock()
	var token, holder string
	if ls := env.store.lease[run.ID]; ls != nil {
		token, holder = ls.token, ls.holder
	}
	env.store.mu.Unlock()
	require.NotEmpty(t, token)

	for _, body := range []string{rec.Body.String(), env.getRun(t, run.ID).Body.String(), env.cancelRun(t, run.ID).Body.String()} {
		assert.NotContains(t, body, token, "the lease token must never appear in any response")
		assert.NotContains(t, body, holder, "the lease holder must never appear in any response")
	}
	env.upstream.release()
	env.eventuallyStatus(t, run.ID, "aborted")
}

// 21. Full convergence: after Stop, no executeSnapshotSync closures, no
// active executions, no registry entries — proven by stack signature.
func TestStopConvergesAllGoroutines(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()
	rec := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, env.coord.Stop())
	env.upstream.release()

	countExec := func() int {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		return strings.Count(string(buf[:n]), "executeSnapshotSync.func")
	}
	deadline := time.Now().Add(3 * time.Second)
	for countExec() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	assert.Zero(t, countExec(), "executor closures must all join")
	assert.Zero(t, env.coord.ActiveExecutions())
	assert.Zero(t, env.coord.AuthRefCount())
}

// Invalid async spelling: 400 with zero new rows.
func TestInvalidAsyncValueIs400WithZeroRows(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	for _, spelling := range []string{"false", "1", "TRUE", ""} {
		req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+env.job.ID.String()+"/run?async="+spelling, "", env.owner), "sync_id", env.job.ID.String())
		rec := httptest.NewRecorder()
		env.h.RunSyncJob(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "async="+fmt.Sprintf("%q", spelling))
	}
	runs, err := env.store.ListSyncRuns(t.Context(), env.job.ID, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	assert.Empty(t, runs, "no durable row may exist for rejected async spellings")
}

// 2-barrier. A queued SYNCHRONOUS waiter must be answered BY THE STOP DRAIN —
// not left hanging for the full task TTL + grace while Shutdown waits.
func TestStopAnswersQueuedSyncWaiterPromptly(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.upstream.block()

	first := env.postRun(t, true)
	require.Equal(t, http.StatusAccepted, first.Code)
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { recCh <- env.postRun(t, false) }()
	// Deterministic queued barrier (review round3 item 3): the first worker
	// is blocked inside the upstream fetch, so waiting until the sync task is
	// genuinely IN THE QUEUE proves the Stop drain — not a stopped-before-
	// dispatch race (OutstandingAdmissions counts the reserved window too).
	require.Eventually(t, func() bool { return handlers.CoordinatorQueuedTaskCountForTest(env.coord) == 1 }, 2*time.Second, 10*time.Millisecond,
		"the sync waiter must be queued before Stop is invoked")

	stopDone := make(chan error, 1)
	start := time.Now()
	go func() { stopDone <- env.coord.Stop() }()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-recCh:
	case <-time.After(5 * time.Second):
		t.Fatal("queued sync waiter was not answered by the stop drain")
	}
	assert.Less(t, time.Since(start), 5*time.Second, "the waiter must not hang for taskTTL+grace")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "shutdown before dispatch")
	assert.Contains(t, rec.Body.String(), "closed as aborted", "a pending row closed by the drain IS abortable — said precisely")
	require.NoError(t, <-stopDone)
	env.upstream.release()
}

// 3. Claim committed but the ack was lost, and the bounded close can only
// persist a REQUEST (the row is genuinely running): the report must say
// exactly that — never "closed as aborted" — and the row must stay running
// with the request queryable.
func TestClaimAckLossWithRequestedOnlyNeverSaysAborted(t *testing.T) {
	env := newAsyncEnv(t, fastCfg())
	env.store.mu.Lock()
	env.store.claimErr = fmt.Errorf("connection reset after commit")
	env.store.mu.Unlock()

	rec := env.postRun(t, false)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	body := rec.Body.String()
	assert.Contains(t, body, "connection reset after commit")
	assert.Contains(t, body, "cancel request is now durably persisted", "aftermath must say the request is unconfirmed")
	assert.NotContains(t, body, "closed as aborted", "a merely-requested close must never be described as aborted")
	assert.NotContains(t, body, "already in a terminal state")

	runs, err := env.store.ListSyncRuns(t.Context(), env.job.ID, repo.Tenant{OwnerID: env.owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "running", runs[0].Status, "the row stays honestly running — not fabricated terminal")
	require.NotNil(t, runs[0].CancelRequestedAt, "the request is durable and queryable")
	assert.Nil(t, runs[0].PublishEnteredAt)

	// GET shows the same truth: running with a pending cancel request.
	getRec := env.getRun(t, runs[0].ID)
	require.Equal(t, http.StatusOK, getRec.Code)
	shown := decodeRun(t, getRec)
	assert.Equal(t, "running", shown.Status)
	require.NotNil(t, shown.CancelRequestedAt)
}

// restSyncSourceConfigWithTimeout is the gated-rig source config with an
// EXPLICIT per-request timeout so nothing can hang forever even if a
// transport gap ever reappears.
func restSyncSourceConfigWithTimeout(baseURL string, timeoutMS int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"base_url": %q,
		"auth": {"type": "bearer", "secret_ref": "oauth_token"},
		"sync": {
			"resource_path": "/items",
			"records_path": "data",
			"pagination": {"type": "page", "page_size": 2},
			"limits": {"max_pages": 10, "max_records": 100, "request_timeout_ms": %d}
		}
	}`, baseURL, timeoutMS))
}
