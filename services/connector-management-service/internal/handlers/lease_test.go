// P2.1 lease behaviour at the HTTP edge: the manual run path enqueues a
// durable pending task, claims it with a fencing lease, keeps it alive with
// bounded heartbeats, and NEVER fabricates a terminal state. Lease loss,
// claim loss and terminal-persist failures must surface as explicit HTTP
// errors — a response can only carry a state that is actually persisted.
package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// leaseTestEnv is a source+sync+credential rig on a page/cursor REST upstream.
type leaseTestEnv struct {
	store     *fakeStore
	h         *handlers.Handlers
	committer *fakeCommitPort
	job       *models.SyncJob
	owner     uuid.UUID
}

func newLeaseTestEnv(t *testing.T) *leaseTestEnv {
	t.Helper()
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[{"sku":"00100","qty":1}]}`, `{"data":[]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(sourceServer.Close)
	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "source-secret-token")
	job, err := store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	return &leaseTestEnv{store: store, h: newSyncHandlers(store, committer), committer: committer, job: job, owner: owner}
}

func (e *leaseTestEnv) triggerRun(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+e.job.ID.String()+"/run", "", e.owner), "sync_id", e.job.ID.String())
	req.Header.Set("Authorization", "Bearer caller-platform-jwt")
	rec := httptest.NewRecorder()
	e.h.RunSyncJob(rec, req)
	return rec
}

func (e *leaseTestEnv) storedRun(t *testing.T) models.SyncRun {
	t.Helper()
	runs, err := e.store.ListSyncRuns(t.Context(), e.job.ID, repo.Tenant{OwnerID: e.owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	return runs[0]
}

func TestRunSyncJobLeasedHappyPathExposesCountersNotToken(t *testing.T) {
	env := newLeaseTestEnv(t)
	rec := env.triggerRun(t)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	var run models.SyncRun
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &run))
	assert.Equal(t, "succeeded", run.Status)
	assert.Equal(t, 1, run.Attempt, "exactly one claim so far")
	assert.Equal(t, 1, run.Generation)
	require.NotNil(t, run.RequestedBy, "requested_by must record the JWT subject")
	assert.Equal(t, env.owner, *run.RequestedBy)
	require.NotNil(t, run.OwnerID, "owner_id must snapshot the source owner")
	assert.Equal(t, env.owner, *run.OwnerID)

	// The claim secret never appears in any public JSON shape.
	for _, forbidden := range []string{"token", "lease", "holder", "heartbeat"} {
		assert.NotContains(t, rec.Body.String(), forbidden, "public run JSON must not carry lease internals")
	}

	stored := env.storedRun(t)
	assert.Equal(t, "succeeded", stored.Status)
	assert.Equal(t, 1, stored.Generation)
}

func TestRunSyncJobClaimLostReturnsConflictAndLeavesDurableTask(t *testing.T) {
	env := newLeaseTestEnv(t)
	env.store.claimFails = true
	rec := env.triggerRun(t)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())

	// The enqueued pending task survives for a later execution; nothing was
	// pulled or published by this request.
	stored := env.storedRun(t)
	assert.Equal(t, "pending", stored.Status)
	requests, _ := env.committer.snapshot()
	assert.Empty(t, requests)
}

func TestRunSyncJobLeaseLostAtPreCommitReturnsConflictAndPublishesNothing(t *testing.T) {
	env := newLeaseTestEnv(t)
	// P2.2: the pre-commit gate is BeginPublishGate — the P2.1 denyRenew
	// injection moves to the gate (same intent: the lease decision at the
	// pre-publish point fails and NOTHING may be published).
	env.store.denyGate = true
	rec := env.triggerRun(t)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "lease was lost")

	// The pull happened, but the publish gate aborted before any publish
	// side effect and before fabricating a terminal state.
	stored := env.storedRun(t)
	assert.Equal(t, "running", stored.Status, "run stays running for takeover; no fake terminal")
	assert.Nil(t, stored.PublishEnteredAt, "the gate must not be entered when it refuses")
	requests, _ := env.committer.snapshot()
	assert.Empty(t, requests, "no publish may happen after the lease gate fails")
}

func TestRunSyncJobTerminalPersistFailsReportsUncertainNotFakeSuccess(t *testing.T) {
	env := newLeaseTestEnv(t)
	env.store.completeErr = fmt.Errorf("db unavailable during terminal write")
	rec := env.triggerRun(t)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "uncertain", "outcome must be reported as uncertain, never as persisted success")

	// The publish DID happen — that is exactly why faking a persisted
	// succeeded run would be a lie. The stored row keeps running.
	requests, _ := env.committer.snapshot()
	require.Len(t, requests, 1, "the publish itself succeeded before the persist failed")
	stored := env.storedRun(t)
	assert.Equal(t, "running", stored.Status)
}

func TestRunSyncJobFailPathFencedIsConflictNotFakeFailure(t *testing.T) {
	env := newLeaseTestEnv(t)
	// Make the pull itself fail (invalid credentials) and fence the terminal
	// write: the failure must not be fabricated into a persisted failed run.
	env.store.credentials = map[uuid.UUID]map[string][]byte{}
	env.store.denyComplete = true
	rec := env.triggerRun(t)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "lease was lost")

	stored := env.storedRun(t)
	assert.Equal(t, "running", stored.Status, "fenced fail path must leave the run open for takeover")
}

func TestRunSyncJobLegacyCompleteCannotTouchLeasedRuns(t *testing.T) {
	env := newLeaseTestEnv(t)
	rec := env.triggerRun(t)
	require.Equal(t, http.StatusAccepted, rec.Code)
	stored := env.storedRun(t)
	require.Equal(t, "succeeded", stored.Status)

	// The legacy no-token path can neither rewrite the terminal state nor
	// hijack a leased run.
	v, err := env.store.CompleteSyncRun(t.Context(), stored.ID, repo.Tenant{OwnerID: env.owner}, "failed", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, v, "legacy completion must be refused for leased/terminal rows")
	after := env.storedRun(t)
	assert.Equal(t, "succeeded", after.Status)
	assert.Equal(t, int64(1), after.FilesWritten, "terminal fields are immutable")
}

func TestRunSyncJobHeartbeatLoopConvergesWithoutGoroutineLeak(t *testing.T) {
	env := newLeaseTestEnv(t)
	for i := 0; i < 5; i++ {
		env.store.runs = map[uuid.UUID][]models.SyncRun{}
		env.store.lease = map[uuid.UUID]*fakeLease{}
		rec := env.triggerRun(t)
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	}
	// Heartbeat goroutines are the executeSnapshotSync closure: they must all
	// return once the terminal write path (or the lease gate) stops them.
	// Idle HTTP transport goroutines from the pull client are unrelated and
	// live longer, so count by stack signature instead of total goroutines.
	// Bounded polling: no long sleeps.
	countHeartbeats := func() int {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		return strings.Count(string(buf[:n]), "executeSnapshotSync.func")
	}
	deadline := time.Now().Add(3 * time.Second)
	for countHeartbeats() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	assert.Zero(t, countHeartbeats(), "heartbeat goroutines must converge after each request")
}

// slowSyncUpstream serves pages with a small fixture latency so a fast
// heartbeat cadence can interleave with an in-flight pull. All access to the
// inner request log goes through the inner mutex (no lock-free peeks).
type slowSyncUpstream struct {
	inner  syncUpstream
	delays []time.Duration
}

func (u *slowSyncUpstream) handler(w http.ResponseWriter, r *http.Request) {
	idx := len(u.inner.requests())
	if idx < len(u.delays) {
		time.Sleep(u.delays[idx])
	}
	u.inner.handler(w, r)
}

func (u *slowSyncUpstream) requestCount() int {
	return len(u.inner.requests())
}

// fenceAfterFirstPage wires the fake store so the first page-gate probe
// succeeds and every later one reports the lease as gone, with the terminal
// CAS fenced to match — the shape of an expiry/takeover observed mid-pull.
func fenceAfterFirstPage(t *testing.T, store *fakeStore) {
	t.Helper()
	store.checkHook = func(call int) (bool, error) {
		if call == 1 {
			return true, nil
		}
		return false, nil
	}
	store.denyComplete = true
}

// TestRunSyncJobPageGateStopsPagingAfterLeaseLoss proves the per-page DB
// gate: when the lease is fenced between pages, the NEXT page request is
// never issued, nothing is published, and no fake terminal is returned.
func TestRunSyncJobPageGateStopsPagingAfterLeaseLoss(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{
		`{"data":[{"sku":"00100","qty":1},{"sku":"00200","qty":2}]}`,
		`{"data":[{"sku":"00300","qty":3}]}`,
		`{"data":[]}`,
	}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(sourceServer.Close)
	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	// Page 1 passes the gate; from page 2 on the DB reports the lease as
	// invalid (expired/taken over) and the terminal CAS is fenced to match.
	fenceAfterFirstPage(t, store)

	rec := httptest.NewRecorder()
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+job.ID.String()+"/run", "", owner), "sync_id", job.ID.String())
	h.RunSyncJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.LessOrEqual(t, len(upstream.requests()), 1, "no further pages may be requested after the lease gate fails")
	requests, _ := committer.snapshot()
	assert.Empty(t, requests, "no publish after lease loss")
	runs, err := store.ListSyncRuns(t.Context(), job.ID, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "running", runs[0].Status, "fenced terminal must leave the row for takeover")
}

// TestRunSyncJobHeartbeatTickStickyNotRevived: a REAL heartbeat tick
// observes a transient renew error mid-pull; the database "recovers" (later
// renew calls would succeed — the hook proves it by returning a valid claim),
// but the execution stays dead: no revival renew, no further pages, no
// publish, no fake success.
func TestRunSyncJobHeartbeatTickStickyNotRevived(t *testing.T) {
	owner := uuid.New()
	// Page 1 is slow enough that the 25ms heartbeat tick lands while it is
	// still in flight, so the error is observed by a tick — not by accident
	// at pre-commit.
	upstream := &slowSyncUpstream{
		inner:  syncUpstream{bodies: []string{`{"data":[{"sku":"00100","qty":1}]}`, `{"data":[]}`}},
		delays: []time.Duration{150 * time.Millisecond},
	}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(server.Close)
	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(server.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)
	h.Config.SyncLeaseHeartbeatInterval = 25 * time.Millisecond

	// Renew #1 (the tick) fails transiently; every later renew WOULD succeed
	// — the sticky flag must ensure no such revival call ever happens.
	store.renewHook = func(call int) (*repo.LeaseClaim, error) {
		if call == 1 {
			return nil, fmt.Errorf("transient db hiccup on heartbeat tick")
		}
		return &repo.LeaseClaim{Token: "recovered", Holder: "h", ExpiresAt: time.Now().Add(time.Minute)}, nil
	}

	rec := httptest.NewRecorder()
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+job.ID.String()+"/run", "", owner), "sync_id", job.ID.String())
	h.RunSyncJob(rec, req)

	// A tick really happened (the hook fired) — bounded wait, no long sleep.
	deadline := time.Now().Add(3 * time.Second)
	for store.renewCallCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.GreaterOrEqual(t, store.renewCallCount(), 1, "a heartbeat tick must have issued a renew")

	// The sticky outcome: exactly ONE renew (no revival despite recovery), no
	// page 2, no publish, never a success.
	assert.Equal(t, 1, store.renewCallCount(), "no second (revival) renew may be issued after the sticky failure")
	assert.LessOrEqual(t, upstream.requestCount(), 1, "no further pages after the sticky failure")
	requests, _ := committer.snapshot()
	assert.Empty(t, requests, "sticky loss must not be revived into publishing")
	assert.NotContains(t, rec.Body.String(), `"succeeded"`, rec.Body.String())

	runs, err := store.ListSyncRuns(t.Context(), job.ID, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Contains(t, []string{"running", "failed"}, runs[0].Status,
		"run is either left open for takeover or honestly closed as failed")
}

// TestRunSyncJobParentCancelAfterFetchBlocksPublish: the pull completes ALL
// pages; the request is cancelled exactly during the publish gate, which
// still ENTERS (a valid decision) — the second cancellation gate must keep
// the known-dead request away from the publish port.
func TestRunSyncJobParentCancelAfterFetchBlocksPublish(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{
		`{"data":[{"sku":"00100","qty":1}]}`,
		`{"data":[]}`,
	}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(server.Close)
	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(server.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	reqCtx, cancelReq := context.WithCancel(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	// Cancel DURING the publish gate, which still enters: the cancellation
	// races to success but must not hand a known-dead request to the publish
	// port.
	store.gateHook = func() (*repo.PublishGateResult, error) {
		cancelReq()
		// Mirror the SQL: an Entered decision atomically stamps the stage.
		store.mu.Lock()
		for defID, runs := range store.runs {
			for i := range runs {
				now := time.Now().UTC()
				store.runs[defID][i].PublishEnteredAt = &now
			}
		}
		store.mu.Unlock()
		return &repo.PublishGateResult{Outcome: repo.PublishGateEntered, Claim: &repo.LeaseClaim{Token: "t", Holder: "h", ExpiresAt: time.Now().Add(time.Minute)}}, nil
	}

	rec := httptest.NewRecorder()
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+job.ID.String()+"/run", "", owner).WithContext(reqCtx), "sync_id", job.ID.String())
	h.RunSyncJob(rec, req)

	// The fetch completed in full; the gate ran exactly once and entered; the
	// publish port was never called.
	assert.Equal(t, 2, len(upstream.requests()), "all pages fetched before the cancellation")
	store.mu.Lock()
	gateCalls := store.gateCalls
	store.mu.Unlock()
	assert.Equal(t, 1, gateCalls, "the publish gate ran and entered")
	requests, _ := committer.snapshot()
	assert.Empty(t, requests, "a cancelled request must not reach the publish port even with a valid lease")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), `"succeeded"`)

	runs, err := store.ListSyncRuns(t.Context(), job.ID, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "running", runs[0].Status, "no terminal attempt: the run stays open for takeover")
}

func TestSourceMutationsRefusedWhileRunsActive(t *testing.T) {
	env := newLeaseTestEnv(t)
	tenant := repo.Tenant{OwnerID: env.owner}
	source := env.store.connections[0]
	env.store.EnqueueSyncRun(t.Context(), env.job.ID, source.ID, tenant, env.owner)

	// Hard delete is refused at the HTTP edge while a durable task is active.
	delRec := httptest.NewRecorder()
	env.h.DeleteConnection(delRec, withRouteParam(authedReq(http.MethodDelete, "/connections/"+source.ID.String(), "", env.owner), "id", source.ID.String()))
	assert.Equal(t, http.StatusConflict, delRec.Code, delRec.Body.String())

	// Config mutation and target retarget are refused; same-target writes
	// stay allowed.
	_, err := env.store.UpdateConnection(t.Context(), source.ID, tenant, &models.UpdateConnectionRequest{Config: json.RawMessage(`{"changed":true}`)})
	assert.ErrorIs(t, err, repo.ErrActiveRuns)
	retarget := uuid.New()
	_, err = env.store.UpdateSyncJob(t.Context(), env.job.ID, &models.UpdateSyncJobRequest{OutputDatasetID: &retarget}, tenant)
	assert.ErrorIs(t, err, repo.ErrActiveRuns)
	sameTarget := env.job.OutputDatasetID
	_, err = env.store.UpdateSyncJob(t.Context(), env.job.ID, &models.UpdateSyncJobRequest{OutputDatasetID: &sameTarget}, tenant)
	assert.NoError(t, err, "same-target writes stay allowed")

	// Once terminal, the pre-P2.1 semantics return.
	env.store.mu.Lock()
	for defID, runs := range env.store.runs {
		for i := range runs {
			env.store.runs[defID][i].Status = "succeeded"
		}
	}
	env.store.mu.Unlock()
	deleted, err := env.store.DeleteConnection(t.Context(), source.ID, tenant)
	assert.NoError(t, err)
	assert.True(t, deleted, "terminal runs allow deletion again")
}
