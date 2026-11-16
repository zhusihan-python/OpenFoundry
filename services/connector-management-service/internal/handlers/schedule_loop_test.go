// P2.4 schedule runner unit tests (internal package).
package handlers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

type fakeLoopStore struct {
	mu sync.Mutex
	// pages models the due-candidate set: each call to ListDueScheduleCandidates
	// returns the next page honoring the keyset cursor.
	pages [][]repo.ScheduleCandidate
	// claims maps defID → claim result (default: not claimed stale).
	claims map[uuid.UUID]*repo.ScheduleClaimResult
	claimFn func(cand repo.ScheduleCandidate, arm *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error)

	scanCalls  int
	lastAfter  *repo.ScheduleScanKey

	cancelCalls []uuid.UUID
	recordCalls []uuid.UUID
}

func (f *fakeLoopStore) ListDueScheduleCandidates(_ context.Context, _ time.Time, after *repo.ScheduleScanKey, limit int) ([]repo.ScheduleCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanCalls++
	f.lastAfter = after
	if len(f.pages) == 0 {
		return nil, nil
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	if len(page) > limit {
		page = page[:limit]
	}
	return page, nil
}

func (f *fakeLoopStore) ClaimDueScheduleSlot(_ context.Context, cand repo.ScheduleCandidate, arm *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error) {
	f.mu.Lock()
	fn := f.claimFn
	f.mu.Unlock()
	if fn != nil {
		return fn(cand, arm)
	}
	f.mu.Lock()
	res := f.claims[cand.SyncDefID]
	f.mu.Unlock()
	if res != nil {
		return res, nil
	}
	return &repo.ScheduleClaimResult{Skip: repo.ScheduleSkipStale}, nil
}

// GetSyncRunForTenant feeds the v1 cleanup skip; nil keeps the legacy close.
func (f *fakeLoopStore) GetSyncRunForTenant(context.Context, uuid.UUID, uuid.UUID, repo.Tenant) (*models.SyncRun, error) {
	return nil, nil
}

func (f *fakeLoopStore) RecordScheduleTickDispatchFailure(_ context.Context, defID uuid.UUID, _ int, _ time.Time, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordCalls = append(f.recordCalls, defID)
	return true, nil
}

func (f *fakeLoopStore) RequestCancelSyncRun(_ context.Context, runID, _ uuid.UUID, _ repo.Tenant, _ *uuid.UUID, _ string) (*repo.CancelResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, runID)
	return &repo.CancelResult{Outcome: repo.CancelOutcomeAborted}, nil
}

func newLoopTestRunner(t *testing.T, store ScheduleLoopStore) (*ScheduleRunner, *RunCoordinator) {
	t.Helper()
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 8, Workers: 1, StopBudget: 5 * time.Second}, &Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewScheduleRunner(ScheduleRunnerConfig{Interval: time.Hour}, store, coord, nil, nil)
	return runner, coord
}

func candidate(defID uuid.UUID, due time.Time) repo.ScheduleCandidate {
	return repo.ScheduleCandidate{SyncDefID: defID, SourceID: uuid.New(), Revision: 1, ScheduledFor: due}
}

func TestScheduleRunnerCountsStructuredSkips(t *testing.T) {
	due := time.Now()
	c1, c2 := candidate(uuid.New(), due), candidate(uuid.New(), due)
	store := &fakeLoopStore{pages: [][]repo.ScheduleCandidate{{c1, c2}}}
	skipFor := map[uuid.UUID]repo.ScheduleClaimSkip{
		c1.SyncDefID: repo.ScheduleSkipRemoteOwner,
		c2.SyncDefID: repo.ScheduleSkipContention,
	}
	store.claimFn = func(cand repo.ScheduleCandidate, _ *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error) {
		return &repo.ScheduleClaimResult{Skip: skipFor[cand.SyncDefID]}, nil
	}
	runner, _ := newLoopTestRunner(t, store)
	summary := runner.RunOnce(context.Background())
	assert.Equal(t, 2, summary.Scanned)
	assert.Equal(t, 0, summary.Claimed)
	assert.Equal(t, 1, summary.Skips["remote_owner"])
	assert.Equal(t, 1, summary.Skips["contention"])
}

func TestScheduleRunnerEnqueuedDispatchSuccessTransfersOwnership(t *testing.T) {
	due := time.Now()
	cand := candidate(uuid.New(), due)
	run := &models.SyncRun{ID: uuid.New(), SyncDefID: cand.SyncDefID, Status: "pending"}
	store := &fakeLoopStore{
		pages: [][]repo.ScheduleCandidate{{cand}},
	}
	store.claimFn = func(repo.ScheduleCandidate, *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error) {
		return &repo.ScheduleClaimResult{
			Claimed: true, Outcome: repo.TickOutcomeEnqueued,
			TickID: uuid.New(), ScheduledFor: due, Run: run, ArmTenant: repo.Tenant{OwnerID: uuid.New()},
		}, nil
	}
	// A live local arm is part of the enqueued contract: install one through
	// the authorizer's real public surface.
	authStore := newFakeAuthStore()
	authz := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4, MaintainEvery: time.Hour}, authStore, uuid.New())
	defer func() { require.NoError(t, authz.Stop()) }()
	require.NoError(t, activateHelperErr(activateHelper(t, authz, authStore, cand.SyncDefID, uuid.New(), 1, time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond))))

	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 8, Workers: 1, StopBudget: 5 * time.Second}, &Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewScheduleRunner(ScheduleRunnerConfig{Interval: time.Hour}, store, coord, authz, nil)
	var dispatchedWith *Admission
	runner.dispatch = func(admission *Admission, _ *repo.ScheduleClaimResult, _ repo.ScheduleCandidate, _ *repo.ScheduleArmClaim) bool {
		dispatchedWith = admission
		// Ownership transfers to the dispatcher in production; release here
		// so the coordinator's test Stop joins cleanly.
		admission.Release()
		return true
	}
	summary := runner.RunOnce(context.Background())
	require.NotNil(t, dispatchedWith, "dispatch must be attempted for an enqueued result")
	assert.Equal(t, 1, summary.Scanned)
	assert.Equal(t, 1, summary.Claimed)
	assert.Equal(t, 1, summary.Enqueued)
	store.mu.Lock()
	cancels, records := len(store.cancelCalls), len(store.recordCalls)
	store.mu.Unlock()
	assert.Equal(t, 0, cancels, "successful dispatch must not close the run")
	assert.Equal(t, 0, records)
}

func TestScheduleRunnerDispatchFailureClosesRunThenRecordsAftermath(t *testing.T) {
	due := time.Now().UTC()
	cand := candidate(uuid.New(), due)
	run := &models.SyncRun{ID: uuid.New(), SyncDefID: cand.SyncDefID, Status: "pending"}
	store := &fakeLoopStore{pages: [][]repo.ScheduleCandidate{{cand}}}
	store.claimFn = func(repo.ScheduleCandidate, *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error) {
		return &repo.ScheduleClaimResult{
			Claimed: true, Outcome: repo.TickOutcomeEnqueued,
			TickID: uuid.New(), ScheduledFor: due, Run: run, ArmTenant: repo.Tenant{OwnerID: uuid.New()},
		}, nil
	}
	runner, _ := newLoopTestRunner(t, store)
	runner.dispatch = func(*Admission, *repo.ScheduleClaimResult, repo.ScheduleCandidate, *repo.ScheduleArmClaim) bool {
		return false // shutdown race after the commit
	}
	summary := runner.RunOnce(context.Background())
	assert.Equal(t, 1, summary.Enqueued)

	store.mu.Lock()
	defer store.mu.Unlock()
	// Commit-unknown discipline: the bounded durable close happens FIRST,
	// then the honest aftermath recording — never a fabricated dispatch
	// success and never a bare drop.
	require.Len(t, store.cancelCalls, 1, "bounded close must run exactly once")
	require.Len(t, store.recordCalls, 1, "aftermath recording must run exactly once")
	assert.Equal(t, run.ID, store.cancelCalls[0])
	assert.Equal(t, cand.SyncDefID, store.recordCalls[0])
}

func TestScheduleRunnerEnqueuedWithoutArmClosesHonestly(t *testing.T) {
	due := time.Now()
	cand := candidate(uuid.New(), due)
	run := &models.SyncRun{ID: uuid.New(), SyncDefID: cand.SyncDefID, Status: "pending"}
	store := &fakeLoopStore{pages: [][]repo.ScheduleCandidate{{cand}}}
	store.claimFn = func(repo.ScheduleCandidate, *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error) {
		return &repo.ScheduleClaimResult{
			Claimed: true, Outcome: repo.TickOutcomeEnqueued,
			TickID: uuid.New(), ScheduledFor: due, Run: run, ArmTenant: repo.Tenant{OwnerID: uuid.New()},
		}, nil
	}
	runner, _ := newLoopTestRunner(t, store)
	summary := runner.RunOnce(context.Background())
	assert.Equal(t, 1, summary.Enqueued)
	store.mu.Lock()
	cancels, records := len(store.cancelCalls), len(store.recordCalls)
	store.mu.Unlock()
	assert.Equal(t, 1, cancels, "unreachable-by-construction path must still close honestly")
	assert.Equal(t, 1, records)
}

func TestScheduleRunnerKeysetFairnessAcrossPages(t *testing.T) {
	due := time.Now()
	// A full first page (BatchSize=8) then a short second page: the runner
	// must continue past the first page (keyset) and wrap after the short
	// one, so contended oldest rows cannot starve later defs.
	big := make([]repo.ScheduleCandidate, 8)
	for i := range big {
		big[i] = candidate(uuid.New(), due)
	}
	short := []repo.ScheduleCandidate{candidate(uuid.New(), due)}
	store := &fakeLoopStore{pages: [][]repo.ScheduleCandidate{big, short}}
	runner, _ := newLoopTestRunner(t, store)
	summary := runner.RunOnce(context.Background())
	assert.Equal(t, 9, summary.Scanned)

	// Cursor wrapped to the start after the short page.
	runner.mu.Lock()
	cursor := runner.scanAfter
	runner.mu.Unlock()
	assert.Nil(t, cursor, "short page must wrap the round-robin cursor")

	// The second page's scan received the first page's last key.
	store.mu.Lock()
	afterSeen := store.lastAfter
	store.mu.Unlock()
	require.NotNil(t, afterSeen)
	assert.Equal(t, big[7].SyncDefID, afterSeen.DefID)
}

func TestScheduleRunnerScanErrorIsCountedNotFatal(t *testing.T) {
	store := &fakeLoopStore{}
	runner, _ := newLoopTestRunner(t, store)
	// No pages: an empty due set simply ends the pass (scanCalls recorded).
	summary := runner.RunOnce(context.Background())
	assert.Equal(t, 0, summary.Errors)
	assert.Equal(t, 0, summary.Scanned)

	// A claim storage error counts as an error, not a panic/fatal.
	cand := candidate(uuid.New(), time.Now())
	store.pages = [][]repo.ScheduleCandidate{{cand}}
	store.claimFn = func(repo.ScheduleCandidate, *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error) {
		return nil, errors.New("db down")
	}
	summary = runner.RunOnce(context.Background())
	assert.Equal(t, 1, summary.Errors)
	assert.Equal(t, 1, summary.Scanned)
}
