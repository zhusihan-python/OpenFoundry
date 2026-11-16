package handlers

// P2.6 executor tests (internal package: direct access to the recoverable
// dispatch frame). The store embeds the Store INTERFACE itself and implements
// only the RecoveryStore surface — every legacy method stays a nil stub the
// p26-v1 dispatch provably never calls. Ordering, exact-body replay and the
// never-terminalize discipline are asserted against the REAL
// executeRecoverablePublish, not a re-implementation.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

// p26Store: the embedded nil Store interface satisfies the Handlers.Repo
// type; only the RecoveryStore methods are real (and recorded).
type p26Store struct {
	Store

	mu  sync.Mutex
	seq []string

	gateOutcome       repo.IncrementalPublishGateOutcome
	gateErr           error
	gateMaterial      *repo.FrozenPublishMaterial
	sendBudget        repo.BudgetResult
	sendBudgetErr     error
	waits             []repo.RecoveryWaitReason
	finReceipt        *repo.PublishReceipt
	finRun            *models.SyncRun
	finErr            error
	queryBudget       repo.BudgetResult
	renews            atomic.Int64
	budgetCalls       int
	lookupRun         *models.SyncRun
	lookupErr         error
	sendBudgetFn      func(call int) repo.BudgetResult
	cands             []repo.RecoveryCandidate
	autoClaim         *repo.RecoveryClaimResult
	fixedJob          *models.SyncJob
	fixedConn         *models.Connection
	controlBlock      chan struct{} // when non-nil, the control check after controlSkip calls blocks until closed
	controlEntered    chan struct{} // closed ONCE when a control call is inside the armed barrier
	controlCallsCount int
	controlSkip       int // how many control calls pass freely before the barrier
	onAutoClaim       func()
}

func (s *p26Store) record(step string) {
	s.mu.Lock()
	s.seq = append(s.seq, step)
	s.mu.Unlock()
}

func (s *p26Store) sequence() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seq...)
}

func (s *p26Store) sequenceString() string { return strings.Join(s.sequence(), ",") }

func (s *p26Store) BeginRecoverablePublishGate(_ context.Context, lease *repo.LeaseClaim, _ repo.Tenant, mat repo.FrozenPublishMaterial) (*repo.IncrementalPublishGateResult, error) {
	s.record("gate:" + string(mat.Branch))
	s.mu.Lock()
	s.gateMaterial = &mat
	outcome, gerr := s.gateOutcome, s.gateErr
	s.mu.Unlock()
	if gerr != nil {
		return nil, gerr
	}
	return &repo.IncrementalPublishGateResult{Outcome: outcome, Claim: lease}, nil
}

func (s *p26Store) ConsumePublishSendBudget(context.Context, *repo.LeaseClaim, repo.Tenant) (repo.BudgetResult, error) {
	s.record("send_budget")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendBudgetFn != nil {
		s.budgetCalls++
		return s.sendBudgetFn(s.budgetCalls), s.sendBudgetErr
	}
	return s.sendBudget, s.sendBudgetErr
}

func (s *p26Store) RecordRecoveryWait(_ context.Context, _ *repo.LeaseClaim, _ repo.Tenant, reason repo.RecoveryWaitReason, _ time.Duration) error {
	s.record("wait:" + string(reason))
	s.mu.Lock()
	s.waits = append(s.waits, reason)
	s.mu.Unlock()
	return nil
}

func (s *p26Store) FinalizeRecoveredRun(_ context.Context, _ *repo.LeaseClaim, _ repo.Tenant, receipt repo.PublishReceipt) (*repo.RecoveryFinalizeResult, error) {
	s.record("finalize")
	s.mu.Lock()
	s.finReceipt = &receipt
	run, ferr := s.finRun, s.finErr
	s.mu.Unlock()
	if ferr != nil {
		return nil, ferr
	}
	return &repo.RecoveryFinalizeResult{Outcome: repo.RecoveryFinalizeCommitted, Run: run}, nil
}

func (s *p26Store) ClaimRecovery(context.Context, uuid.UUID, repo.Tenant, string, string, time.Duration) (*repo.RecoveryClaimResult, error) {
	s.record("claim_recovery")
	return nil, errors.New("not scripted in executor tests")
}

func (s *p26Store) ConsumeRecoveryQueryAttemptManual(context.Context, *repo.LeaseClaim, repo.Tenant, repo.RecoveryWaitReason) (repo.BudgetResult, error) {
	s.record("query_budget_manual")
	return repo.BudgetResult{}, nil
}

func (s *p26Store) GetPublishIntentView(context.Context, uuid.UUID, repo.Tenant) (*repo.PublishIntentView, error) {
	s.record("intent_view")
	return nil, nil
}

func (s *p26Store) GetPublishIntentMaterial(context.Context, uuid.UUID, repo.Tenant) (*repo.PublishIntentMaterial, error) {
	return nil, nil
}

// CompleteLeasedSyncRun records the pre-gate terminal write the cancelled
// gate outcome legitimately performs (provably zero dispatch).
func (s *p26Store) CompleteLeasedSyncRun(_ context.Context, _ uuid.UUID, _ repo.Tenant, _, _ string, _ int, status string, _, _, _ int64, _ *string, _ *string, _ *uuid.UUID, _ *string) (*models.SyncRun, error) {
	s.record("complete:" + status)
	return &models.SyncRun{Status: status}, nil
}

// Legacy/lease surfaces for the FULL executeSnapshotSync path: the legacy
// gates are counted (and must stay at zero for p26-v1), renewals are counted
// so heartbeat continuity across a slow send is provable.
func (s *p26Store) BeginPublishGate(context.Context, uuid.UUID, repo.Tenant, string, string, int, time.Duration) (*repo.PublishGateResult, error) {
	s.record("legacy_gate")
	return nil, errors.New("legacy gate must never run for p26-v1")
}

func (s *p26Store) BeginIncrementalPublishGate(context.Context, uuid.UUID, repo.Tenant, string, string, int, time.Duration) (*repo.IncrementalPublishGateResult, error) {
	s.record("legacy_inc_gate")
	return nil, errors.New("legacy incremental gate must never run for p26-v1")
}

func (s *p26Store) RenewSyncRunLease(_ context.Context, _ uuid.UUID, _ repo.Tenant, _, _ string, _ int, _ time.Duration) (*repo.LeaseClaim, error) {
	s.renews.Add(1)
	return &repo.LeaseClaim{}, nil
}

func (s *p26Store) CheckSyncRunLease(context.Context, uuid.UUID, repo.Tenant, string, string, int) (bool, error) {
	return true, nil
}

func (s *p26Store) CheckSyncRunControl(context.Context, uuid.UUID, repo.Tenant, string, string, int) (bool, bool, error) {
	s.mu.Lock()
	s.controlCallsCount++
	block := s.controlBlock
	armed := block != nil && s.controlCallsCount > s.controlSkip
	entered := s.controlEntered
	if armed {
		s.controlBlock = nil // one-shot barrier
		s.controlEntered = nil
	}
	s.mu.Unlock()
	if armed {
		if entered != nil {
			close(entered) // PROOF the control call is now blocked INSIDE the barrier
		}
		<-block
	}
	return true, false, nil
}

func (s *p26Store) GetSourceCredentialCiphertext(context.Context, uuid.UUID, repo.Tenant, string) ([]byte, error) {
	return nil, nil
}

// GetSyncRunForTenant serves the parent-chain and cleanup-skip lookups.
func (s *p26Store) GetSyncRunForTenant(_ context.Context, _, runID uuid.UUID, _ repo.Tenant) (*models.SyncRun, error) {
	s.record("run_lookup")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	if s.lookupRun != nil && s.lookupRun.ID == runID {
		cp := *s.lookupRun
		return &cp, nil
	}
	return nil, nil
}

// Automatic seam surfaces.
func (s *p26Store) ListRecoveryCandidates(_ context.Context, _ time.Time, after *repo.RecoveryScanKey, limit int) ([]repo.RecoveryCandidate, error) {
	s.record("scan")
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]repo.RecoveryCandidate, 0, len(s.cands))
	for _, c := range s.cands { // deterministic (ReadyAt,RunID) keyset semantics
		if after != nil && !scanKeyAfter(c, *after) {
			continue
		}
		out = append(out, c)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func scanKeyAfter(c repo.RecoveryCandidate, after repo.RecoveryScanKey) bool {
	if c.ReadyAt.After(after.ReadyAt) {
		return true
	}
	if c.ReadyAt.Equal(after.ReadyAt) {
		return c.RunID.String() > after.RunID.String()
	}
	return false
}

func (s *p26Store) ClaimAutomaticRecovery(_ context.Context, _ uuid.UUID, _ *repo.ScheduleArmClaim, _, _ string, _ time.Duration) (*repo.RecoveryClaimResult, error) {
	s.record("claim_auto")
	s.mu.Lock()
	hook := s.onAutoClaim
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.autoClaim, nil
}

func (s *p26Store) ConsumeRecoveryQueryAttempt(context.Context, *repo.LeaseClaim, repo.Tenant, repo.RecoveryWaitReason) (repo.BudgetResult, error) {
	s.record("query_budget_auto")
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queryBudget, nil
}

// GetSyncJob/GetConnectionForTenant serve the guarded pre-gate adoption.
func (s *p26Store) GetSyncJob(context.Context, uuid.UUID, repo.Tenant) (*models.SyncJob, error) {
	s.record("get_job")
	return s.fixedJob, nil
}

func (s *p26Store) GetConnectionForTenant(_ context.Context, id uuid.UUID, _ repo.Tenant) (*models.Connection, error) {
	s.record("get_conn")
	if s.fixedConn != nil && s.fixedConn.ID == id {
		return s.fixedConn, nil
	}
	return nil, nil
}

// RequestCancelSyncRun records the unsigned durable cancel the v1 cleanup
// must NEVER issue.
func (s *p26Store) RequestCancelSyncRun(_ context.Context, _, _ uuid.UUID, _ repo.Tenant, _ *uuid.UUID, _ string) (*repo.CancelResult, error) {
	s.record("unsigned_cancel")
	return &repo.CancelResult{Outcome: repo.CancelOutcomeRequested}, nil
}

// p26Port scripts the keyed-commit client.
type p26Port struct {
	mu         sync.Mutex
	sentBodies [][]byte
	sentKeys   []string
	commitErr  error
	commitFn   func(prepared cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error)
	getFn      func(datasetID uuid.UUID, key string) (*cmruntime.DatasetCommitReceipt, error)
	getCalls   int
}

func (p *p26Port) CommitPreparedOutput(_ context.Context, prepared cmruntime.PreparedCommit, _ string) (*cmruntime.DatasetCommitReceipt, error) {
	p.mu.Lock()
	p.sentBodies = append(p.sentBodies, append([]byte(nil), prepared.Body...))
	p.sentKeys = append(p.sentKeys, prepared.Key)
	fn, errFn := p.commitFn, p.commitErr
	p.mu.Unlock()
	if errFn != nil {
		return nil, errFn
	}
	if fn != nil {
		return fn(prepared)
	}
	return receiptForPreparedTest(prepared), nil
}

func (p *p26Port) GetCommitReceipt(_ context.Context, datasetID uuid.UUID, key string, _ string) (*cmruntime.DatasetCommitReceipt, error) {
	p.mu.Lock()
	p.getCalls++
	fn := p.getFn
	p.mu.Unlock()
	if fn != nil {
		return fn(datasetID, key)
	}
	return nil, cmruntime.ErrCommitReceiptNotFound
}

// receiptForPreparedTest renders the receipt the DVS would return for THIS
// prepared commit (strictly matching every expectation).
func receiptForPreparedTest(p cmruntime.PreparedCommit) *cmruntime.DatasetCommitReceipt {
	return &cmruntime.DatasetCommitReceipt{
		DatasetID:          p.DatasetID,
		Branch:             p.Branch,
		IdempotencyKey:     p.Key,
		RequestFingerprint: p.Fingerprint,
		TransactionID:      uuid.New(),
		Status:             "COMMITTED",
		ContentHash:        p.ContentHash,
		Lineage:            p.Lineage,
	}
}

type execFrameOutcome struct {
	failed  bool
	failErr error
	stopped bool // set by the frame's stopHeartbeat hook
}

// recoverableTestFrame builds a live dispatch frame with recording hooks.
func recoverableTestFrame(h *Handlers, outcome *execFrameOutcome) recoverableSyncFrame {
	content := []byte("{\"a\":1}\n{\"a\":2}\n")
	sum := sha256.Sum256(content)
	run := models.SyncRun{ID: uuid.New(), SyncDefID: uuid.New(), Generation: 2, Status: "running"}
	job := &models.SyncJob{ID: run.SyncDefID, OutputDatasetID: uuid.New()}
	conn := &models.Connection{ID: uuid.New(), ConnectorType: "rest_api"}
	snapshot := &cmruntime.SnapshotResult{
		NDJSON:      content,
		Rows:        2,
		Bytes:       int64(len(content)),
		ContentHash: hex.EncodeToString(sum[:]),
		Pages:       1,
	}
	workCtx, cancel := context.WithCancel(context.Background())
	_ = cancel
	return recoverableSyncFrame{
		ctx:     context.Background(),
		workCtx: workCtx,
		stopHeartbeat: func() {
			outcome.stopped = true
		},
		fail: func(err error, _ *string) (*models.SyncRun, error) {
			outcome.failed = true
			outcome.failErr = err
			return nil, err
		},
		tenant:        repo.Tenant{OwnerID: uuid.New()},
		lease:         &repo.LeaseClaim{Holder: "h", Token: "t", Run: run},
		job:           job,
		conn:          conn,
		snapshot:      snapshot,
		activeBranch:  "release",
		authorization: "Bearer fresh-jwt",
	}
}

func TestExecuteRecoverablePublishOrderingAndExactBody(t *testing.T) {
	store := &p26Store{gateOutcome: repo.IncrementalPublishGateEntered, sendBudget: repo.BudgetResult{Allowed: true, Attempt: 1, Remaining: 2}}
	store.finRun = &models.SyncRun{ID: uuid.New(), Status: "succeeded"}
	outcome := &execFrameOutcome{}
	heartbeatAliveAtSend := false
	port := &p26Port{commitFn: func(p cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error) {
		heartbeatAliveAtSend = !outcome.stopped
		return receiptForPreparedTest(p), nil
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	frame := recoverableTestFrame(h, outcome)

	run, err := h.executeRecoverablePublish(frame)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", run.Status)
	assert.False(t, outcome.failed, "a full success path must never hit the pre-gate fail closure")
	assert.True(t, heartbeatAliveAtSend, "heartbeats stay alive THROUGH the send window (root v2 item 5)")
	assert.True(t, outcome.stopped, "and they stop once the dispatch converges")

	// Real executor ordering: gate → budget → send → finalize.
	assert.Equal(t, []string{"gate:release", "send_budget", "finalize"}, store.sequence())

	// The frozen material and the dispatched bytes are the SAME bytes the
	// client serialized once; the verified receipt goes to the finalize.
	require.NotNil(t, store.gateMaterial)
	require.Len(t, port.sentBodies, 1)
	assert.Equal(t, store.gateMaterial.ExactBody, port.sentBodies[0], "the persisted intent body is dispatched verbatim")
	assert.Equal(t, repo.DerivePublishKey(frame.lease.Run.ID), port.sentKeys[0])
	assert.Equal(t, "release", store.gateMaterial.Branch)
	assert.Nil(t, store.gateMaterial.CompletedCheckpoint, "snapshot mode freezes a NULL candidate")
	require.NotNil(t, store.finReceipt)
	assert.Equal(t, "COMMITTED", store.finReceipt.Status)
	assert.Equal(t, frame.lease.Run.ID, store.finReceipt.MetadataSyncRunID)
}

func TestExecuteRecoverablePublishIncrementalFreezesCandidate(t *testing.T) {
	store := &p26Store{gateOutcome: repo.IncrementalPublishGateEntered, sendBudget: repo.BudgetResult{Allowed: true, Attempt: 1}}
	store.finRun = &models.SyncRun{ID: uuid.New(), Status: "succeeded"}
	h := &Handlers{Repo: store, DatasetRecovery: &p26Port{}}
	outcome := &execFrameOutcome{}
	frame := recoverableTestFrame(h, outcome)
	frame.incremental = true
	frame.snapshot.CompletedCheckpoint = "ckpt-xyz"
	frame.lease.Run.CPMode = "cursor"

	_, err := h.executeRecoverablePublish(frame)
	require.NoError(t, err)
	require.NotNil(t, store.gateMaterial)
	assert.NotNil(t, store.gateMaterial.CompletedCheckpoint, "incremental mode freezes the completed candidate")
	assert.Equal(t, "ckpt-xyz", *store.gateMaterial.CompletedCheckpoint)
	// The opaque candidate never enters the serialized body.
	assert.NotContains(t, string(store.gateMaterial.ExactBody), "ckpt-xyz")
}

func TestExecuteRecoverablePublishGateOutcomesNeverDispatch(t *testing.T) {
	cases := []struct {
		name    string
		outcome repo.IncrementalPublishGateOutcome
		gateErr error
		wantSeq string
		wantRun string // expected terminal status, "" for no run
	}{
		{"cancelled", repo.IncrementalPublishGateCancelled, nil, "gate:release,complete:aborted", "aborted"},
		{"binding stale", repo.IncrementalPublishGateBindingStale, nil, "gate:release", ""},
		{"fenced", repo.IncrementalPublishGateFenced, nil, "gate:release", ""},
		{"owner lost", repo.IncrementalPublishGateOwnerLost, nil, "gate:release", ""},
		{"gate error", "", errors.New("gate db unreachable"), "gate:release", ""},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := &p26Store{gateOutcome: testCase.outcome, gateErr: testCase.gateErr, sendBudget: repo.BudgetResult{Allowed: true}}
			port := &p26Port{}
			h := &Handlers{Repo: store, DatasetRecovery: port}
			outcome := &execFrameOutcome{}
			frame := recoverableTestFrame(h, outcome)

			run, err := h.executeRecoverablePublish(frame)
			if testCase.wantRun != "" {
				require.NoError(t, err)
				require.NotNil(t, run)
				assert.Equal(t, testCase.wantRun, run.Status)
			} else {
				require.Error(t, err)
				assert.Nil(t, run)
			}
			// Zero-dispatch: the budget/send/finalize steps never run.
			assert.Equal(t, testCase.wantSeq, store.sequenceString())
			assert.Empty(t, port.sentBodies)
			assert.NotContains(t, store.sequenceString(), "finalize")
		})
	}
}

func TestExecuteRecoverablePublishDependencyFailureWaitsWithoutTerminalizing(t *testing.T) {
	// One budgeted dispatch fails as a dependency error; the next budget
	// read refuses (persisted cap reached) — the loop stops after recording
	// the safe wait, never fabricating a terminal state.
	store := &p26Store{
		gateOutcome: repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult {
			return repo.BudgetResult{Allowed: call == 1, Attempt: call}
		},
	}
	port := &p26Port{commitErr: &cmruntime.DependencyError{Op: "commit_prepared_output", Status: 503}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}
	frame := recoverableTestFrame(h, outcome)
	frame.backoff = func(int) time.Duration { return 0 }

	run, err := h.executeRecoverablePublish(frame)
	require.Error(t, err)
	assert.Nil(t, run)
	assert.False(t, outcome.failed, "post-gate failures must NOT use the pre-gate fail closure")
	assert.Equal(t, []string{"gate:release", "send_budget", "wait:send_deferred", "send_budget"}, store.sequence(),
		"dependency failure -> safe wait -> budget refusal ends the loop")
	require.Len(t, store.waits, 1)
	assert.Equal(t, repo.RecoveryWaitSendDeferred, store.waits[0])
	assert.NotContains(t, store.sequenceString(), "finalize")
}

func TestExecuteRecoverablePublishMismatchNeverWaitsOrFinalizes(t *testing.T) {
	store := &p26Store{gateOutcome: repo.IncrementalPublishGateEntered, sendBudget: repo.BudgetResult{Allowed: true, Attempt: 1}}
	port := &p26Port{commitFn: func(p cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error) {
		r := receiptForPreparedTest(p)
		r.DatasetID = uuid.New()
		return r, nil
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}

	run, err := h.executeRecoverablePublish(recoverableTestFrame(h, outcome))
	require.Error(t, err)
	assert.Nil(t, run)
	assert.False(t, outcome.failed)
	assert.Equal(t, []string{"gate:release", "send_budget"}, store.sequence(),
		"mismatch is not a dependency wait and must never finalize")
}

func TestExecuteRecoverablePublishBudgetRefusedNeverSends(t *testing.T) {
	store := &p26Store{gateOutcome: repo.IncrementalPublishGateEntered, sendBudget: repo.BudgetResult{Allowed: false}}
	port := &p26Port{}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}

	_, err := h.executeRecoverablePublish(recoverableTestFrame(h, outcome))
	require.Error(t, err)
	assert.Equal(t, []string{"gate:release", "send_budget"}, store.sequence())
	assert.Empty(t, port.sentBodies, "no budget, no dispatch")
}

func TestExecuteRecoverablePublishReplaysExactPersistedBytes(t *testing.T) {
	// Two invocations through the real dispatch (first send, then a replay
	// after an unknown outcome): the persisted intent body is byte-identical
	// on the wire every time.
	store := &p26Store{gateOutcome: repo.IncrementalPublishGateEntered}
	port := &p26Port{}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}
	frame := recoverableTestFrame(h, outcome)

	for attempt := 1; attempt <= 2; attempt++ {
		store.mu.Lock()
		store.sendBudget = repo.BudgetResult{Allowed: true, Attempt: attempt, Remaining: 3 - attempt}
		store.mu.Unlock()
		_, err := h.executeRecoverablePublish(frame)
		require.NoError(t, err)
	}
	require.Len(t, port.sentBodies, 2)
	assert.Equal(t, port.sentBodies[0], port.sentBodies[1], "replays are byte-identical to the first send")
}

func TestWriteIncrementalFenceErrorMapsRecoveryRunsActive(t *testing.T) {
	w := httptest.NewRecorder()
	handled := writeIncrementalFenceError(w, fmt.Errorf("wrapped: %w", repo.ErrRecoveryRunsActive))
	assert.True(t, handled)
	assert.Equal(t, http.StatusConflict, w.Code)
}

// ---------------------------------------------------------------------------
// Full-path protocol routing through the REAL executeSnapshotSync
// ---------------------------------------------------------------------------

// p26DatasetPort prechecks with an explicit active branch and refuses any
// legacy CommitOutput call (a p26-v1 run must never use it).
type p26DatasetPort struct{ branch string }

func (p p26DatasetPort) GetDataset(_ context.Context, id uuid.UUID, _ string) (*cmruntime.DatasetInfo, error) {
	return &cmruntime.DatasetInfo{ID: id, Name: "orders", ActiveBranch: p.branch}, nil
}

func (p p26DatasetPort) CommitOutput(context.Context, cmruntime.DatasetCommitRequest, string) (*cmruntime.DatasetCommitResult, error) {
	return nil, errors.New("legacy CommitOutput must never run for a p26-v1 run")
}

// slowPort delays the send so the heartbeat loop provably renews the lease
// DURING the send window.
type slowPort struct {
	p26Port
	delay time.Duration
}

func (p *slowPort) CommitPreparedOutput(ctx context.Context, prepared cmruntime.PreparedCommit, auth string) (*cmruntime.DatasetCommitReceipt, error) {
	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return nil, &cmruntime.DependencyError{Op: "commit_prepared_output", Timeout: true}
	}
	return p.p26Port.CommitPreparedOutput(ctx, prepared, auth)
}

// TestExecuteSnapshotSyncRoutesV1ThroughRecoverableDispatch drives the REAL
// executor (http fetch → precheck → gate → budget → send → finalize) for both
// frozen modes and asserts the dispatch placement: the legacy gates and the
// legacy heartbeat-stop-before-gate order are NEVER taken — the recoverable
// gate runs exactly once and the lease is renewed while the slow send is in
// flight (root v2 item 5).
func TestExecuteSnapshotSyncRoutesV1ThroughRecoverableDispatch(t *testing.T) {
	for _, mode := range []struct {
		name   string
		cpcmde string
	}{
		{"snapshot", ""},
		{"incremental", repo.CPModeCursor},
	} {
		t.Run(mode.name, func(t *testing.T) {
			var sourceHits atomic.Int64
			var sourceServer *httptest.Server
			if mode.cpcmde == "" {
				// Page pagination: page 1 carries records, page 2 is empty.
				sourceServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					sourceHits.Add(1)
					if r.URL.Query().Get("page") == "1" || r.URL.Query().Get("page") == "" {
						fmt.Fprint(w, `{"data":[{"id":1},{"id":2}]}`)
						return
					}
					fmt.Fprint(w, `{"data":[]}`)
				}))
			} else {
				// Cursor pagination: one page, terminal (no next cursor) and
				// a completed-checkpoint candidate.
				sourceServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					sourceHits.Add(1)
					assert.Equal(t, "tok-0", r.URL.Query().Get("since_token"), "the frozen input token is sent")
					fmt.Fprint(w, `{"items":[{"id":7}],"completed":"ckpt-9"}`)
				}))
			}
			defer sourceServer.Close()

			syncCfg := fmt.Sprintf(`{
				"base_url": %q,
				"auth": {"type": "none"},
				"sync": {"resource_path": "/items", "records_path": "data",
				         "pagination": {"type": "page", "page_size": 2},
				         "limits": {"max_pages": 5, "max_records": 100, "request_timeout_ms": 2000}}
			}`, sourceServer.URL)
			if mode.cpcmde != "" {
				syncCfg = fmt.Sprintf(`{
					"base_url": %q,
					"auth": {"type": "none"},
					"sync": {"resource_path": "/items", "records_path": "items",
					         "pagination": {"type": "cursor", "cursor_param": "cursor_token", "next_cursor_path": "next_cursor"},
					         "incremental": {"enabled": true, "request_param": "since_token", "completed_checkpoint_path": "completed"},
					         "limits": {"max_records": 100, "request_timeout_ms": 2000}}
				}`, sourceServer.URL)
			}

			store := &p26Store{
				gateOutcome: repo.IncrementalPublishGateEntered,
				sendBudget:  repo.BudgetResult{Allowed: true, Attempt: 1, Remaining: 2},
				finRun:      &models.SyncRun{Status: "succeeded"},
			}
			port := &slowPort{p26Port: p26Port{}, delay: 150 * time.Millisecond}
			run := models.SyncRun{
				ID: uuid.New(), SyncDefID: uuid.New(), Status: "running",
				Generation: 4, RecoveryProtocol: models.RunRecoveryProtocolP26V1,
				CPMode: mode.cpcmde, CPBaseValue: func() string {
					if mode.cpcmde == "" {
						return ""
					}
					return "tok-0"
				}(),
			}
			job := &models.SyncJob{ID: run.SyncDefID, SourceID: uuid.New(), OutputDatasetID: uuid.New()}
			conn := &models.Connection{ID: job.SourceID, ConnectorType: "rest_api", Config: json.RawMessage(syncCfg)}
			h := &Handlers{
				Repo:              store,
				DatasetVersioning: p26DatasetPort{branch: "release"},
				DatasetRecovery:   port,
				Config:            RuntimeConfig{SyncLeaseHeartbeatInterval: 30 * time.Millisecond, SyncCancelPollInterval: 40 * time.Millisecond},
			}

			runResult, err := h.executeSnapshotSync(context.Background(), "Bearer fresh", repo.Tenant{OwnerID: uuid.New()},
				&repo.LeaseClaim{Holder: "h", Token: "t", Run: run}, job, conn, true)
			require.NoError(t, err)
			require.NotNil(t, runResult)
			assert.Equal(t, "succeeded", runResult.Status)

			// Dispatch placement: exactly ONE recoverable gate; the legacy
			// gates and the legacy terminal write are never entered.
			seq := store.sequenceString()
			assert.NotContains(t, seq, "legacy_gate", "p26-v1 must route BEFORE the legacy publish gate")
			assert.NotContains(t, seq, "legacy_inc_gate", "p26-v1 must route BEFORE the legacy incremental gate")
			assert.NotContains(t, seq, "complete:", "the success goes through FinalizeRecoveredRun, not completeLeased")
			require.Contains(t, seq, "gate:release")
			require.Contains(t, seq, "finalize")

			// Heartbeat continuity: the lease was renewed WHILE the slow
			// send (150ms at 30ms cadence) was in flight.
			assert.GreaterOrEqual(t, store.renews.Load(), int64(1),
				"heartbeats keep renewing through the send window")

			// Frozen mode pairing reached the material.
			require.NotNil(t, store.gateMaterial)
			if mode.cpcmde == "" {
				assert.Nil(t, store.gateMaterial.CompletedCheckpoint, "snapshot freezes a NULL candidate")
			} else {
				require.NotNil(t, store.gateMaterial.CompletedCheckpoint)
				assert.Equal(t, "ckpt-9", *store.gateMaterial.CompletedCheckpoint)
				assert.NotContains(t, string(store.gateMaterial.ExactBody), "ckpt-9", "candidate never serializes")
			}
			// The exact persisted bytes went to the wire exactly once.
			require.Len(t, port.sentBodies, 1)
			assert.Equal(t, store.gateMaterial.ExactBody, port.sentBodies[0])
			assert.GreaterOrEqual(t, sourceHits.Load(), int64(1), "the pre-gate fetch ran once")
		})
	}
}

// Review round1 item 4: a not-yet-executed p26-v1 task is NEVER durably
// cancelled by the coordinator cleanup paths (queue/Stop/auth/claim
// failure): the row stays for fresh-auth recovery; a legacy row keeps the
// unsigned close.
func TestCloseTaskDurableSkipsV1Only(t *testing.T) {
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 1, Workers: 1}, &Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	syncID, runID := uuid.New(), uuid.New()
	task := coordinatorTask{runID: runID, syncID: syncID, tenant: repo.Tenant{OwnerID: uuid.New()}}

	t.Run("v1 row is left for fresh-auth recovery", func(t *testing.T) {
		store := &p26Store{lookupRun: &models.SyncRun{ID: runID, SyncDefID: syncID, RecoveryProtocol: models.RunRecoveryProtocolP26V1}}
		coord.h = &Handlers{Repo: store}
		outcome, err := coord.closeTaskDurable(context.Background(), task, "test")
		require.NoError(t, err)
		assert.Equal(t, cancelOutcomeV1LeftForRecovery, outcome)
		assert.NotContains(t, store.sequenceString(), "unsigned_cancel", "no nil-generation cancel on a v1 row")
	})

	t.Run("legacy row keeps the durable close", func(t *testing.T) {
		store := &p26Store{lookupRun: &models.SyncRun{ID: runID, SyncDefID: syncID}}
		coord.h = &Handlers{Repo: store}
		outcome, err := coord.closeTaskDurable(context.Background(), task, "test")
		require.NoError(t, err)
		assert.Equal(t, repo.CancelOutcomeRequested, outcome)
		assert.Contains(t, store.sequenceString(), "unsigned_cancel")
	})
}

// ---------------------------------------------------------------------------
// Bounded REAL dependency retry loop (review pending scope)
// ---------------------------------------------------------------------------

func TestExecuteRecoverablePublishBoundedDependencyRetrySucceeds(t *testing.T) {
	store := &p26Store{
		gateOutcome: repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult { // persisted cap 3
			return repo.BudgetResult{Allowed: true, Attempt: call, Remaining: 3 - call}
		},
		finRun: &models.SyncRun{Status: "succeeded"},
	}
	depFail := &cmruntime.DependencyError{Op: "commit_prepared_output", Status: 503}
	attempts := 0
	port := &p26Port{commitFn: func(p cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error) {
		attempts++
		if attempts <= 2 {
			return nil, depFail // two dependency failures, third succeeds
		}
		return receiptForPreparedTest(p), nil
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}
	frame := recoverableTestFrame(h, outcome)
	frame.backoff = func(int) time.Duration { return 0 } // deterministic, no sleeps

	run, err := h.executeRecoverablePublish(frame)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", run.Status)
	assert.Equal(t, 3, attempts, "exactly three dispatches: two failed + the successful one")
	require.Len(t, port.sentBodies, 3)
	assert.Equal(t, port.sentBodies[0], port.sentBodies[2], "every retry sends the SAME exact bytes")
	assert.Equal(t, []string{"gate:release", "send_budget", "wait:send_deferred", "send_budget", "wait:send_deferred", "send_budget", "finalize"}, store.sequence())
}

func TestExecuteRecoverablePublishRetryStopsAtPersistedBudgetCap(t *testing.T) {
	store := &p26Store{
		gateOutcome: repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult { // sticky refusal after 3
			return repo.BudgetResult{Allowed: call <= 3, Attempt: call}
		},
	}
	depFail := &cmruntime.DependencyError{Op: "commit_prepared_output", Timeout: true}
	attempts := 0
	port := &p26Port{commitFn: func(p cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error) {
		attempts++
		return nil, depFail // never succeeds
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}
	frame := recoverableTestFrame(h, outcome)
	frame.backoff = func(int) time.Duration { return 0 }

	run, err := h.executeRecoverablePublish(frame)
	require.Error(t, err)
	assert.Nil(t, run)
	assert.Contains(t, err.Error(), "budget is exhausted")
	assert.Equal(t, 3, attempts, "the persisted cap 3 bounds the loop — no fourth send")
	assert.NotContains(t, store.sequenceString(), "finalize", "no fabricated terminal state")
	require.Len(t, store.waits, 3)
}

func TestExecuteRecoverablePublishTypedRefusalNeverRetries(t *testing.T) {
	store := &p26Store{
		gateOutcome:  repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult { return repo.BudgetResult{Allowed: true, Attempt: call} },
	}
	conflict := cmruntime.ErrCommitIdempotencyConflict
	attempts := 0
	port := &p26Port{commitFn: func(p cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error) {
		attempts++
		return nil, conflict
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	outcome := &execFrameOutcome{}
	frame := recoverableTestFrame(h, outcome)
	frame.backoff = func(int) time.Duration { return 0 }

	_, err := h.executeRecoverablePublish(frame)
	require.Error(t, err)
	assert.Equal(t, 1, attempts, "a typed permanent conflict is never retried")
	assert.NotContains(t, store.sequenceString(), "wait:", "no dependency wait for typed refusals")
}

// ---------------------------------------------------------------------------
// Automatic recovery round (default-off seam)
// ---------------------------------------------------------------------------

// A disabled boot never calls RunOnce; a live round with NO local arm for a
// candidate skips it with zero claims, zero remote, zero writes.
func TestAutoRecoveryRoundSkipsWithoutArm(t *testing.T) {
	store := &p26Store{cands: []repo.RecoveryCandidate{{
		RunID: uuid.New(), SyncDefID: uuid.New(), SourceID: uuid.New(), ReadyAt: time.Now(),
	}}}
	port := &p26Port{}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	// No ScheduleAuthorizer wired: every candidate is a no-arm aggregate skip.
	runner := NewAutoRecoveryRunner(h, coord, nil, AutoRecoveryConfig{MaxCandidates: 1, RoundDeadline: 5 * time.Second})
	runner.RunOnce(context.Background())

	counts := runner.Counts()
	assert.Equal(t, int64(1), counts.Rounds)
	assert.Equal(t, int64(1), counts.Scanned)
	assert.Equal(t, int64(1), counts.SkippedNoArm)
	assert.Equal(t, int64(0), counts.ClaimsRefused+counts.Recovered+counts.StillUnknown)
	assert.Equal(t, "scan", store.sequenceString(), "zero claims, zero remote, zero writes")
	require.Empty(t, port.sentBodies)
}

// With a live arm the ONLY adoption entry is ClaimAutomaticRecovery (never a
// bare ClaimRecovery); a granted post-intent claim runs the shared post-gate
// service under the AUTOMATIC query budget.
func TestAutoRecoveryAdoptsViaClaimAutomaticRecovery(t *testing.T) {
	runID := uuid.New()
	// Build a fully consistent intent through the real serialization site.
	content := []byte(`{"a":1}` + "\n")
	sum := sha256.Sum256(content)
	defID, sourceID, datasetID := uuid.New(), uuid.New(), uuid.New()
	prepared, err := cmruntime.PrepareRecoverableCommit(cmruntime.DatasetCommitRequest{
		DatasetID: datasetID, Branch: "release", Format: "TEXT",
		LogicalPath: "sync-" + runID.String() + ".ndjson", ContentType: "application/x-ndjson",
		Content: content, RowCount: 1, ContentHash: hex.EncodeToString(sum[:]),
		Summary: "Connector snapshot sync " + defID.String(),
		Metadata: map[string]any{
			"content_hash": hex.EncodeToString(sum[:]), "rows": int64(1), "bytes": int64(len(content)), "pages": 1,
			"lineage": map[string]any{"source_id": sourceID, "sync_def_id": defID, "sync_run_id": runID},
		},
	}, repo.DerivePublishKey(runID))
	require.NoError(t, err)
	intent := &repo.PublishIntentMaterial{
		RunID: runID, SyncDefID: defID, SourceID: sourceID,
		RecoveryProtocol: models.RunRecoveryProtocolP26V1,
		PublishKey:       prepared.Key, RequestFingerprint: prepared.Fingerprint,
		ExactBody: prepared.Body, OutputDatasetID: datasetID,
		Branch: prepared.Branch, LogicalPath: prepared.LogicalPath,
		ContentHash: prepared.ContentHash, Rows: prepared.Rows, Bytes: prepared.Bytes,
		RecoveryState: repo.IntentStateWaiting,
	}
	store := &p26Store{
		cands: []repo.RecoveryCandidate{{RunID: runID, SyncDefID: defID, SourceID: sourceID, ReadyAt: time.Now()}},
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted,
			Claim:   &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID, Generation: 5}},
			Intent:  intent, IntentExists: true,
		},
		queryBudget: repo.BudgetResult{Allowed: true, Attempt: 1},
		finRun:      &models.SyncRun{ID: runID, Status: "succeeded"},
	}
	port := &p26Port{getFn: func(datasetID uuid.UUID, key string) (*cmruntime.DatasetCommitReceipt, error) {
		return &cmruntime.DatasetCommitReceipt{
			DatasetID: datasetID, Branch: intent.Branch, IdempotencyKey: key,
			RequestFingerprint: intent.RequestFingerprint, TransactionID: uuid.New(),
			Status: "COMMITTED", ContentHash: intent.ContentHash,
			Lineage: cmruntime.DatasetLineageIdentity{SourceID: sourceID, SyncDefID: defID, SyncRunID: runID},
		}, nil
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), Actor: uuid.New(), SourceID: sourceID,
		OutputDatasetID: datasetID, Tenant: repo.Tenant{OwnerID: uuid.New()},
		Authorization: "Bearer arm-jwt", ExpiresAt: time.Now().Add(time.Minute)}
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{defID: arm}}
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{MaxCandidates: 10, RoundDeadline: 10 * time.Second})

	runner.processCandidate(context.Background(), store, store.cands[0], arm)

	seq := store.sequenceString()
	assert.Contains(t, seq, "claim_auto", "adoption goes through ClaimAutomaticRecovery")
	assert.NotContains(t, seq, "claim_recovery", "never a bare ClaimRecovery")
	assert.Contains(t, seq, "query_budget_auto", "the automatic query budget (cap 5) is consumed")
	assert.Contains(t, seq, "finalize")
	assert.Equal(t, int64(1), runner.Counts().Recovered)
}

// The pre-intent shape is NEVER adopted automatically (fresh pulls need the
// manual fresh-auth execution path).
func TestAutoRecoveryRefusesPreIntentClaim(t *testing.T) {
	runID := uuid.New()
	store := &p26Store{
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted,
			Claim:   &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID}},
		}, // IntentExists false
	}
	h := &Handlers{Repo: store, DatasetRecovery: &p26Port{}}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, nil, AutoRecoveryConfig{})

	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer x", ExpiresAt: time.Now().Add(time.Minute)}
	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: uuid.New()}, arm)

	assert.Equal(t, int64(1), runner.Counts().ClaimsRefused)
	assert.Equal(t, "claim_auto,get_job", store.sequenceString(), "parent read then typed refusal — zero remote/writes")
}

// ---------------------------------------------------------------------------
// Round1 followup narrow tests
// ---------------------------------------------------------------------------

// A failed protocol probe NEVER authorizes the unsigned durable close: the
// read error is returned verbatim and nothing is cancelled.
func TestCloseTaskDurableFailsClosedOnProbeError(t *testing.T) {
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 1, Workers: 1}, &Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	store := &p26Store{lookupErr: errors.New("db probe failed")}
	coord.h = &Handlers{Repo: store}
	task := coordinatorTask{runID: uuid.New(), syncID: uuid.New(), tenant: repo.Tenant{OwnerID: uuid.New()}}

	outcome, err := coord.closeTaskDurable(context.Background(), task, "test")
	require.Error(t, err, "the probe failure surfaces honestly")
	assert.Empty(t, outcome)
	assert.NotContains(t, store.sequenceString(), "unsigned_cancel", "no unfenced close from a failed probe")
}

// An invisible run is not closed at all (not-visible, zero writes).
func TestCloseTaskDurableInvisibleRunNotClosed(t *testing.T) {
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 1, Workers: 1}, &Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	store := &p26Store{} // lookupRun nil => not visible
	coord.h = &Handlers{Repo: store}
	task := coordinatorTask{runID: uuid.New(), syncID: uuid.New(), tenant: repo.Tenant{OwnerID: uuid.New()}}

	outcome, err := coord.closeTaskDurable(context.Background(), task, "test")
	require.NoError(t, err)
	assert.Equal(t, repo.CancelOutcomeNotVisible, outcome)
	assert.NotContains(t, store.sequenceString(), "unsigned_cancel")
}

// An injected port answering (nil, nil) is an honest unknown — the executor
// never panics and never treats it as a usable ACK.
func TestExecuteRecoverablePublishNilReceiptIsHonestUnknown(t *testing.T) {
	store := &p26Store{
		gateOutcome:  repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult { return repo.BudgetResult{Allowed: true, Attempt: call} },
	}
	port := &p26Port{commitFn: func(cmruntime.PreparedCommit) (*cmruntime.DatasetCommitReceipt, error) {
		return nil, nil // broken injected port
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	frame := recoverableTestFrame(h, &execFrameOutcome{})
	frame.backoff = func(int) time.Duration { return 0 }

	run, err := h.executeRecoverablePublish(frame)
	require.Error(t, err)
	assert.Nil(t, run)
	assert.Contains(t, err.Error(), "strict prepared-commit match")
	assert.NotContains(t, store.sequenceString(), "finalize", "a nil receipt never finalizes")
}

// The ticket deadline is capped by the ALREADY-VERIFIED JWT expiry.
func TestRecoveryTicketCappedByJWTExpiry(t *testing.T) {
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{
		QueueDepth: 2, Workers: 1, AuthMemoryTTL: time.Hour, TaskDeadline: time.Hour,
	}, &Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	// JWT expiring in the past: the ticket context is dead on arrival.
	past := time.Now().Add(-time.Second)
	ticket := coord.ReserveRecoveryTicket(context.Background(), uuid.New(), past)
	require.NotNil(t, ticket)
	assert.Error(t, ticket.Context().Err(), "a stale JWT can never start recovery work")
	ticket.Release()
	// A live JWT in the middle only shortens, never extends.
	live := time.Now().Add(50 * time.Millisecond)
	short := coord.ReserveRecoveryTicket(context.Background(), uuid.New(), live)
	require.NotNil(t, short)
	require.NoError(t, short.Context().Err())
	time.Sleep(80 * time.Millisecond)
	assert.Error(t, short.Context().Err(), "the JWT cap ends the ticket before the local TTL")
	short.Release()
}

func TestJWTExpiryFromClaims(t *testing.T) {
	assert.True(t, jwtExpiryFromClaims(nil).IsZero())
	assert.True(t, jwtExpiryFromClaims(&authmw.Claims{}).IsZero(), "no exp claim => no cap")
	exp := time.Now().Add(time.Hour).Unix()
	got := jwtExpiryFromClaims(&authmw.Claims{EXP: exp})
	require.False(t, got.IsZero())
	assert.Equal(t, exp, got.Unix())
}

// fakeArmProvider scripts the live-arm seam.
type fakeArmProvider struct {
	mu   sync.Mutex
	arms map[uuid.UUID]*repo.ScheduleArmClaim
}

func (f *fakeArmProvider) ArmClaim(defID uuid.UUID) *repo.ScheduleArmClaim {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.arms[defID]
}

func (f *fakeArmProvider) Drop(defID uuid.UUID) {
	f.mu.Lock()
	delete(f.arms, defID)
	f.mu.Unlock()
}

func (f *fakeArmProvider) Set(defID uuid.UUID, arm *repo.ScheduleArmClaim) {
	f.mu.Lock()
	f.arms[defID] = arm
	f.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Review auto-round2 narrow fixes
// ---------------------------------------------------------------------------

// Item 1: the process-local keyset cursor carries ACROSS rounds — a first
// page of arm-less candidates never permanently hides later candidates, and
// the tail wraps fairly.
func TestAutoRecoveryFairKeysetCursorAcrossRounds(t *testing.T) {
	defNoArm := uuid.New() // three candidates with NO live arm
	cands := make([]repo.RecoveryCandidate, 0, 3)
	for i := 0; i < 3; i++ {
		cands = append(cands, repo.RecoveryCandidate{
			RunID: uuid.New(), SyncDefID: defNoArm, SourceID: uuid.New(),
			ReadyAt: time.Now().Add(time.Duration(i) * time.Second),
		})
	}
	store := &p26Store{cands: cands}
	h := &Handlers{Repo: store, DatasetRecovery: &p26Port{}}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 4, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, nil, AutoRecoveryConfig{MaxCandidates: 2, RoundDeadline: 10 * time.Second})

	runner.RunOnce(context.Background())
	assert.Equal(t, int64(2), runner.Counts().Scanned, "round one stops at MaxCandidates=2")
	runner.RunOnce(context.Background())
	counts := runner.Counts()
	assert.Equal(t, int64(3), counts.Scanned, "round two reaches the third candidate the cursor carried to")
	assert.Equal(t, int64(3), counts.SkippedNoArm)
	// Tail wrap: the cursor reset to nil after the empty tail page.
	runner.mu.Lock()
	wrapped := runner.scanAfter == nil
	runner.mu.Unlock()
	assert.True(t, wrapped, "an empty tail page wraps the cursor fairly")
	runner.RunOnce(context.Background())
	assert.Equal(t, int64(5), runner.Counts().Scanned, "after the wrap the next round starts from the head")
}

// Item 2: the arm is dropped while the receipt GET is in flight and the GET
// answers 404 — the replay POST NEVER starts (sticky live-arm guard).
func TestAutoRecoveryGetBarrierDropThen404ZeroPost(t *testing.T) {
	runID, defID := uuid.New(), uuid.New()
	content := []byte(`{"a":1}` + "\n")
	sum := sha256.Sum256(content)
	prepared, err := cmruntime.PrepareRecoverableCommit(cmruntime.DatasetCommitRequest{
		DatasetID: uuid.New(), Branch: "release", LogicalPath: "sync-" + runID.String() + ".ndjson",
		Content: content, RowCount: 1, ContentHash: hex.EncodeToString(sum[:]),
		Metadata: map[string]any{
			"content_hash": hex.EncodeToString(sum[:]), "rows": int64(1), "bytes": int64(len(content)),
			"lineage": map[string]any{"source_id": uuid.New(), "sync_def_id": defID, "sync_run_id": runID},
		},
	}, repo.DerivePublishKey(runID))
	require.NoError(t, err)
	intent := &repo.PublishIntentMaterial{
		RunID: runID, SyncDefID: defID, SourceID: uuid.New(),
		PublishKey: prepared.Key, RequestFingerprint: prepared.Fingerprint, ExactBody: prepared.Body,
		OutputDatasetID: prepared.DatasetID, Branch: prepared.Branch, LogicalPath: prepared.LogicalPath,
		ContentHash: prepared.ContentHash, Rows: prepared.Rows, Bytes: prepared.Bytes,
		RecoveryState: repo.IntentStateArmed,
	}
	store := &p26Store{
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted,
			Claim:   &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID, Generation: 2}},
			Intent:  intent, IntentExists: true,
		},
		queryBudget: repo.BudgetResult{Allowed: true, Attempt: 1},
		finRun:      &models.SyncRun{Status: "succeeded"},
	}
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	baseArm := &repo.ScheduleArmClaim{GrantID: uuid.New(), BootID: uuid.New(), Actor: uuid.New(),
		SourceID: intent.SourceID, OutputDatasetID: intent.OutputDatasetID,
		Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer arm-jwt",
		ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, baseArm)
	port := &p26Port{getFn: func(uuid.UUID, string) (*cmruntime.DatasetCommitReceipt, error) {
		arms.Drop(defID) // revoked while the GET is in flight
		return nil, cmruntime.ErrCommitReceiptNotFound
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID}, baseArm)

	require.Empty(t, port.sentBodies, "the 404 replay POST never starts after the arm loss")
	assert.Equal(t, int64(1), runner.Counts().StillUnknown)
	assert.NotContains(t, store.sequenceString(), "send_budget")
}

// Item 2: a REPLACED grant (new revision) is a sticky fail-closed loss, and
// re-arming alone does not resurrect the in-flight recovery.
func TestAutoRecoveryArmReplacementFailsClosedSticky(t *testing.T) {
	runID, defID := uuid.New(), uuid.New()
	store := &p26Store{
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted,
			Claim:   &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID, Generation: 2}},
			Intent: &repo.PublishIntentMaterial{RunID: runID, SyncDefID: defID, PublishKey: repo.DerivePublishKey(runID),
				RequestFingerprint: strings.Repeat("ab", 32), Branch: "release", ContentHash: strings.Repeat("cd", 32),
				OutputDatasetID: uuid.New()},
			IntentExists: true,
		},
		queryBudget: repo.BudgetResult{Allowed: true, Attempt: 1},
	}
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	original := &repo.ScheduleArmClaim{GrantID: uuid.New(), Revision: 3, Tenant: repo.Tenant{OwnerID: uuid.New()},
		Authorization: "Bearer old", ExpiresAt: time.Now().Add(time.Hour), SourceID: uuid.New(), OutputDatasetID: uuid.New()}
	arms.Set(defID, original)
	port := &p26Port{getFn: func(uuid.UUID, string) (*cmruntime.DatasetCommitReceipt, error) {
		replacement := *original
		replacement.Revision = 4 // re-armed with a NEW revision mid-flight
		arms.Set(defID, &replacement)
		return nil, cmruntime.ErrCommitReceiptNotFound
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID}, original)

	require.Empty(t, port.sentBodies, "a replaced grant is fail-closed — no POST with cached material")
	// And the guard is sticky: even with the arm live again, this recovery is done.
	guard := runner.liveArmGuard(defID, original)
	require.Error(t, guard())
}

// Item 3: a pre-gate cancelled run converges NOW (fenced aborted, zero
// source/budget/remote) instead of being discarded after the strong claim.
func TestAutoRecoveryPregateCancelledConvergesAborted(t *testing.T) {
	runID, defID := uuid.New(), uuid.New()
	store := &p26Store{
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted, CancelRequested: true, IntentExists: false,
			Claim: &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID, SyncDefID: defID, Generation: 2}},
		},
	}
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer x", ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, arm)
	port := &p26Port{}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID}, arm)

	assert.Equal(t, int64(1), runner.Counts().ConvergedAborted)
	assert.Contains(t, store.sequenceString(), "complete:aborted", "the fenced pre-gate terminal write ran")
	assert.NotContains(t, store.sequenceString(), "gate:")
	require.Empty(t, port.sentBodies)
}

// Item 3: a pre-gate crashed run is EXECUTED through the guarded normal
// executor on the SAME claim (source fetched once, intent+publish finalized).
func TestAutoRecoveryPregateExecutesViaGuardedExecutor(t *testing.T) {
	sourceHits := 0
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHits++
		if r.URL.Query().Get("page") == "1" || r.URL.Query().Get("page") == "" {
			fmt.Fprint(w, `{"data":[{"id":1}]}`)
			return
		}
		fmt.Fprint(w, `{"data":[]}`)
	}))
	defer sourceServer.Close()
	runID, defID, sourceID, datasetID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store := &p26Store{
		gateOutcome: repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult {
			return repo.BudgetResult{Allowed: true, Attempt: call}
		},
		finRun: &models.SyncRun{Status: "succeeded"},
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted, IntentExists: false,
			Claim: &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{
				ID: runID, SyncDefID: defID, Generation: 2, Status: "running",
				RecoveryProtocol: models.RunRecoveryProtocolP26V1,
			}},
		},
		fixedJob: &models.SyncJob{ID: defID, SourceID: sourceID, OutputDatasetID: datasetID},
		fixedConn: &models.Connection{ID: sourceID, ConnectorType: "rest_api", Config: json.RawMessage(fmt.Sprintf(`{
			"base_url": %q, "auth": {"type": "none"},
			"sync": {"resource_path": "/items", "records_path": "data",
			         "pagination": {"type": "page", "page_size": 1},
			         "limits": {"max_pages": 5, "max_records": 10, "request_timeout_ms": 2000}}
		}`, sourceServer.URL))},
	}
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), Actor: uuid.New(), SourceID: sourceID, OutputDatasetID: datasetID,
		Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer arm-jwt", ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, arm)
	h := &Handlers{
		Repo:              store,
		DatasetVersioning: p26DatasetPort{branch: "release"},
		DatasetRecovery:   &p26Port{},
		Config:            RuntimeConfig{SyncLeaseHeartbeatInterval: 50 * time.Millisecond, SyncCancelPollInterval: 60 * time.Millisecond},
	}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID, SourceID: sourceID}, arm)

	assert.Equal(t, int64(1), runner.Counts().Recovered)
	assert.Equal(t, 2, sourceHits, "the guarded executor pulled the source exactly once (2 pages)")
	seq := store.sequenceString()
	assert.Contains(t, seq, "gate:release", "the frozen ActiveBranch reached the intent")
	assert.Contains(t, seq, "finalize")
}

// Item 3: the per-page guard drops the arm mid-pull — the next page NEVER
// starts, the run converges as a provable pre-gate failure, zero gate/send.
func TestAutoRecoveryPregatePerPageArmDropZeroNextPage(t *testing.T) {
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	defID, sourceID := uuid.New(), uuid.New()
	fixedJob := &models.SyncJob{ID: defID, SourceID: sourceID, OutputDatasetID: uuid.New()}
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), SourceID: sourceID, OutputDatasetID: fixedJob.OutputDatasetID,
		Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer x", ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, arm)
	sourceHits := 0
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHits++
		if sourceHits == 1 {
			arms.Drop(defID) // revoked after the first page
			fmt.Fprint(w, `{"data":[{"id":1}]}`)
			return
		}
		fmt.Fprint(w, `{"data":[]}`)
	}))
	defer sourceServer.Close()
	runID := uuid.New()
	store := &p26Store{
		gateOutcome: repo.IncrementalPublishGateEntered,
		sendBudgetFn: func(call int) repo.BudgetResult {
			return repo.BudgetResult{Allowed: true, Attempt: call}
		},
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted, IntentExists: false,
			Claim: &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{
				ID: runID, SyncDefID: defID, Generation: 2, RecoveryProtocol: models.RunRecoveryProtocolP26V1,
			}},
		},
		fixedJob: fixedJob,
		fixedConn: &models.Connection{ID: sourceID, ConnectorType: "rest_api", Config: json.RawMessage(fmt.Sprintf(`{
			"base_url": %q, "auth": {"type": "none"},
			"sync": {"resource_path": "/items", "records_path": "data",
			         "pagination": {"type": "page", "page_size": 1},
			         "limits": {"max_pages": 5, "max_records": 10, "request_timeout_ms": 2000}}
		}`, sourceServer.URL))},
	}
	h := &Handlers{
		Repo:              store,
		DatasetVersioning: p26DatasetPort{branch: "release"},
		DatasetRecovery:   &p26Port{},
		Config:            RuntimeConfig{SyncLeaseHeartbeatInterval: 50 * time.Millisecond, SyncCancelPollInterval: 60 * time.Millisecond},
	}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID, SourceID: sourceID}, arm)

	assert.Equal(t, 1, sourceHits, "the second page never starts after the arm drop")
	seq := store.sequenceString()
	assert.NotContains(t, seq, "gate:", "zero gate after the guarded pull refusal")
	assert.Contains(t, seq, "complete:failed", "provable pre-gate failure converges honestly")
}

// ---------------------------------------------------------------------------
// Review round3: guards AFTER known blocking steps
// ---------------------------------------------------------------------------

// Item 1: the DB control check blocks; the arm is dropped while it blocks;
// the release must still refuse the next source page (guard re-checked
// AFTER the blocking call). Zero new source pages, no sleeps.
func TestGuardedExecutorDBControlBarrierDropZeroNextPage(t *testing.T) {
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	defID, sourceID := uuid.New(), uuid.New()
	fixedJob := &models.SyncJob{ID: defID, SourceID: sourceID, OutputDatasetID: uuid.New()}
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), SourceID: sourceID, OutputDatasetID: fixedJob.OutputDatasetID,
		Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer x", ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, arm)
	var sourceHits atomic.Int64
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHits.Add(1)
		fmt.Fprint(w, `{"data":[{"id":1}]}`) // page_size 1: every page carries one row
	}))
	defer sourceServer.Close()
	runID := uuid.New()
	barrier := make(chan struct{})
	store := &p26Store{
		controlBlock: barrier, controlSkip: 1, // first (page-1) check passes, page-2 check blocks
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted, IntentExists: false,
			Claim: &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{
				ID: runID, SyncDefID: defID, Generation: 2, RecoveryProtocol: models.RunRecoveryProtocolP26V1,
			}},
		},
		fixedJob: fixedJob,
		fixedConn: &models.Connection{ID: sourceID, ConnectorType: "rest_api", Config: json.RawMessage(fmt.Sprintf(`{
			"base_url": %q, "auth": {"type": "none"},
			"sync": {"resource_path": "/items", "records_path": "data",
			         "pagination": {"type": "page", "page_size": 1},
			         "limits": {"max_pages": 5, "max_records": 10, "request_timeout_ms": 2000}}
		}`, sourceServer.URL))},
	}
	h := &Handlers{
		Repo:              store,
		DatasetVersioning: p26DatasetPort{branch: "release"},
		DatasetRecovery:   &p26Port{},
		Config:            RuntimeConfig{SyncLeaseHeartbeatInterval: 50 * time.Millisecond, SyncCancelPollInterval: 10 * time.Second},
	}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	entered := make(chan struct{})
	store.mu.Lock()
	store.controlEntered = entered
	store.mu.Unlock()
	dropped := make(chan struct{})
	go func() {
		// Wait for PROOF that the SECOND page's DB control call is blocked
		// INSIDE the barrier (not merely that page 1 finished): only then
		// drop the arm and release — no polling, no sleeps.
		<-entered
		arms.Drop(defID)
		close(dropped)
		close(barrier)
	}()

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID, SourceID: sourceID}, arm)
	<-dropped

	assert.Equal(t, int64(1), sourceHits.Load(), "the page after the blocking control check NEVER starts once the arm was dropped mid-block")
	assert.Contains(t, store.sequenceString(), "complete:failed", "honest pre-gate convergence")
	assert.NotContains(t, store.sequenceString(), "gate:")
}

// Item 2: the send-budget consumption blocks; the arm is dropped while it
// blocks; the release must NOT dispatch the POST with the cached JWT. The
// pre-budget check stays, the post-budget recheck is the barrier fix.
func TestPostgateBudgetBarrierDropZeroPost(t *testing.T) {
	runID, defID := uuid.New(), uuid.New()
	content := []byte(`{"a":1}` + "\n")
	sum := sha256.Sum256(content)
	prepared, err := cmruntime.PrepareRecoverableCommit(cmruntime.DatasetCommitRequest{
		DatasetID: uuid.New(), Branch: "release", LogicalPath: "sync-" + runID.String() + ".ndjson",
		Content: content, RowCount: 1, ContentHash: hex.EncodeToString(sum[:]),
		Metadata: map[string]any{
			"content_hash": hex.EncodeToString(sum[:]), "rows": int64(1), "bytes": int64(len(content)),
			"lineage": map[string]any{"source_id": uuid.New(), "sync_def_id": defID, "sync_run_id": runID},
		},
	}, repo.DerivePublishKey(runID))
	require.NoError(t, err)
	intent := &repo.PublishIntentMaterial{
		RunID: runID, SyncDefID: defID, SourceID: uuid.New(),
		PublishKey: prepared.Key, RequestFingerprint: prepared.Fingerprint, ExactBody: prepared.Body,
		OutputDatasetID: prepared.DatasetID, Branch: prepared.Branch, LogicalPath: prepared.LogicalPath,
		ContentHash: prepared.ContentHash, Rows: prepared.Rows, Bytes: prepared.Bytes,
		RecoveryState: repo.IntentStateArmed,
	}
	started := make(chan struct{})
	release := make(chan struct{})
	store := &p26Store{
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted,
			Claim:   &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID, Generation: 2}},
			Intent:  intent, IntentExists: true,
		},
		queryBudget: repo.BudgetResult{Allowed: true, Attempt: 1},
		sendBudgetFn: func(call int) repo.BudgetResult {
			if call == 1 {
				close(started)
				<-release // the budget consumption blocks here
			}
			return repo.BudgetResult{Allowed: true, Attempt: call, Remaining: 3 - call}
		},
	}
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), SourceID: intent.SourceID, OutputDatasetID: intent.OutputDatasetID,
		Tenant: repo.Tenant{OwnerID: uuid.New()}, Authorization: "Bearer arm-jwt", ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, arm)
	port := &p26Port{getFn: func(uuid.UUID, string) (*cmruntime.DatasetCommitReceipt, error) {
		return nil, cmruntime.ErrCommitReceiptNotFound // replay path
	}}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	go func() {
		<-started
		arms.Drop(defID) // lost while the budget call blocks
		close(release)
	}()
	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID}, arm)

	require.Empty(t, port.sentBodies, "no POST after the arm was dropped during the budget block")
	assert.Contains(t, store.sequenceString(), "send_budget", "the budget was consumed before the loss was observable")
	assert.Equal(t, int64(1), runner.Counts().StillUnknown)
}

// Item 2: an ALREADY-observed drop at the entry of the post-gate recovery
// consumes ZERO query budget and makes zero remote calls.
func TestPostgateDroppedEntryZeroQueryBudget(t *testing.T) {
	runID, defID := uuid.New(), uuid.New()
	arms := &fakeArmProvider{arms: map[uuid.UUID]*repo.ScheduleArmClaim{}}
	arm := &repo.ScheduleArmClaim{GrantID: uuid.New(), Tenant: repo.Tenant{OwnerID: uuid.New()},
		Authorization: "Bearer x", ExpiresAt: time.Now().Add(time.Hour)}
	arms.Set(defID, arm)
	store := &p26Store{
		autoClaim: &repo.RecoveryClaimResult{
			Outcome: repo.RecoveryClaimGranted,
			Claim:   &repo.LeaseClaim{Holder: "auto", Token: "t", Run: models.SyncRun{ID: runID, Generation: 2}},
			Intent: &repo.PublishIntentMaterial{RunID: runID, SyncDefID: defID, PublishKey: repo.DerivePublishKey(runID),
				RequestFingerprint: strings.Repeat("ab", 32), Branch: "release", ContentHash: strings.Repeat("cd", 32),
				OutputDatasetID: uuid.New()},
			IntentExists: true,
		},
		onAutoClaim: func() { arms.Drop(defID) }, // observed loss lands between claim and budget
	}
	port := &p26Port{}
	h := &Handlers{Repo: store, DatasetRecovery: port}
	coord := NewRunCoordinator(context.Background(), CoordinatorConfig{QueueDepth: 2, Workers: 1}, h)
	t.Cleanup(func() { _ = coord.Stop() })
	runner := NewAutoRecoveryRunner(h, coord, arms, AutoRecoveryConfig{})

	runner.processCandidate(context.Background(), store, repo.RecoveryCandidate{RunID: runID, SyncDefID: defID}, arm)

	seq := store.sequenceString()
	assert.NotContains(t, seq, "query_budget_auto", "zero query budget after an already-observed drop")
	assert.NotContains(t, seq, "send_budget")
	require.Empty(t, port.sentBodies)
	assert.Zero(t, port.getCallsCount(), "zero remote calls")
}

func (p *p26Port) getCallsCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.getCalls
}

// normalHTTPAdmissionStore exercises RunSyncJob's real admission/worker boundary.
// The Source view intentionally disagrees with the returned admission-frozen
// protocol: selecting by current Source configuration would choose the wrong cap.
type normalHTTPAdmissionStore struct {
	*p26Store
	admitted     models.SyncRun
	deadlineSeen chan time.Time
	captureOnly  bool
}

func (s *normalHTTPAdmissionStore) EnqueueSyncRun(context.Context, uuid.UUID, uuid.UUID, repo.Tenant, uuid.UUID) (*models.SyncRun, error) {
	run := s.admitted
	return &run, nil
}

func (s *normalHTTPAdmissionStore) ClaimSyncRun(ctx context.Context, _ uuid.UUID, _ repo.Tenant, holder, token string, _ time.Duration) (*repo.LeaseClaim, error) {
	deadline, _ := ctx.Deadline()
	s.deadlineSeen <- deadline
	if s.captureOnly {
		return nil, nil // stop after capturing the REAL worker context deadline
	}
	run := s.admitted
	run.Status, run.Generation = "running", 1
	return &repo.LeaseClaim{Run: run, Holder: holder, Token: token, ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func normalHTTPJWTRequest(jobID, owner uuid.UUID, expiry int64, async bool) *http.Request {
	target := "/syncs/" + jobID.String() + "/run"
	if async {
		target += "?async=true"
	}
	route := chi.NewRouteContext()
	route.URLParams.Add("sync_id", jobID.String())
	ctx := authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner, EXP: expiry})
	ctx = context.WithValue(ctx, chi.RouteCtxKey, route)
	req := httptest.NewRequest(http.MethodPost, target, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer original-caller-jwt")
	return req
}

func TestRunSyncJobJWTDeadlineUsesAdmittedProtocol(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, protocol := range []string{"", models.RunRecoveryProtocolP26V1} {
			t.Run(fmt.Sprintf("async=%t/protocol=%q", async, protocol), func(t *testing.T) {
				owner, defID, sourceID := uuid.New(), uuid.New(), uuid.New()
				run := models.SyncRun{ID: uuid.New(), SyncDefID: defID, Status: "pending", OwnerID: &owner, RequestedBy: &owner, ControlMode: models.RunControlControlled, RecoveryProtocol: protocol}
				store := &normalHTTPAdmissionStore{
					p26Store: &p26Store{fixedJob: &models.SyncJob{ID: defID, SourceID: sourceID}, fixedConn: &models.Connection{
						ID: sourceID, OwnerID: owner, ConnectorType: "rest_api",
						Config: json.RawMessage(fmt.Sprintf(`{"sync":{"recovery":{"enabled":%t}}}`, protocol == "")),
					}},
					admitted: run, deadlineSeen: make(chan time.Time, 1), captureOnly: true,
				}
				h := &Handlers{Repo: store}
				coord := NewRunCoordinator(context.Background(), CoordinatorConfig{Workers: 1, QueueDepth: 1, AuthMemoryTTL: time.Hour, TaskDeadline: time.Hour}, h)
				h.Coordinator = coord
				t.Cleanup(func() { _ = coord.Stop() })
				expiry := time.Now().Add(30 * time.Second).Unix()
				rec := httptest.NewRecorder()
				h.RunSyncJob(rec, normalHTTPJWTRequest(defID, owner, expiry, async))
				if async {
					require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
				} else {
					require.Equal(t, http.StatusConflict, rec.Code, "scripted claim loss occurs only after the real worker deadline was captured: %s", rec.Body.String())
				}
				select {
				case deadline := <-store.deadlineSeen:
					if protocol == models.RunRecoveryProtocolP26V1 {
						assert.Equal(t, time.Unix(expiry, 0), deadline, "admitted v1 remains capped by the verified JWT expiry")
					} else {
						assert.True(t, deadline.After(time.Unix(expiry, 0).Add(30*time.Minute)), "legacy NULL protocol must retain local auth/task TTLs, not the caller JWT deadline: %s", deadline)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("RunSyncJob never reached the worker claim boundary")
				}
				require.Eventually(t, func() bool { return coord.ActiveExecutions() == 0 && coord.AuthRefCount() == 0 && len(coord.sem) == 0 }, time.Second, time.Millisecond)
			})
		}
	}
}

// A normal HTTP v1 admission expires while its REAL source HTTP page is parked.
// The cap must still cancel that pull before either publish gate/send path and
// release every local coordinator slot/auth/execution reference in both modes.
func TestRunSyncJobV1JWTExpiryDuringPullZeroGateSendAndSlots(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			entered, cancelled := make(chan struct{}), make(chan struct{})
			var hits atomic.Int64
			sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				close(entered)
				<-r.Context().Done()
				close(cancelled)
			}))
			t.Cleanup(sourceServer.Close)
			owner, defID, sourceID := uuid.New(), uuid.New(), uuid.New()
			run := models.SyncRun{ID: uuid.New(), SyncDefID: defID, Status: "pending", OwnerID: &owner, RequestedBy: &owner, ControlMode: models.RunControlControlled, RecoveryProtocol: models.RunRecoveryProtocolP26V1}
			store := &normalHTTPAdmissionStore{
				p26Store: &p26Store{lookupRun: &run, fixedJob: &models.SyncJob{ID: defID, SourceID: sourceID, OutputDatasetID: uuid.New()}, fixedConn: &models.Connection{
					ID: sourceID, OwnerID: owner, ConnectorType: "rest_api", Config: json.RawMessage(fmt.Sprintf(`{
						"base_url":%q,"auth":{"type":"none"},
						"sync":{"resource_path":"/items","records_path":"data","pagination":{"type":"page","page_size":1},
						"recovery":{"enabled":false},"limits":{"max_pages":2,"max_records":2,"request_timeout_ms":10000}}}`, sourceServer.URL)),
				}},
				admitted: run, deadlineSeen: make(chan time.Time, 1),
			}
			port := &p26Port{}
			h := &Handlers{Repo: store, DatasetVersioning: p26DatasetPort{branch: "main"}, DatasetRecovery: port, Config: RuntimeConfig{AllowPrivateNetworkEgress: true}}
			coord := NewRunCoordinator(context.Background(), CoordinatorConfig{Workers: 1, QueueDepth: 1, AuthMemoryTTL: time.Minute, TaskDeadline: time.Minute}, h)
			h.Coordinator = coord
			t.Cleanup(func() { _ = coord.Stop() })
			expiry := time.Now().Add(2 * time.Second).Unix()
			done := make(chan struct{})
			go func() {
				h.RunSyncJob(httptest.NewRecorder(), normalHTTPJWTRequest(defID, owner, expiry, async))
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("the real source pull did not start before caller expiry")
			}
			select {
			case <-cancelled:
			case <-time.After(3 * time.Second):
				t.Fatal("JWT deadline failed to cancel the parked v1 source pull")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("HTTP waiter did not converge after the expiry")
			}
			require.Eventually(t, func() bool { return coord.ActiveExecutions() == 0 && coord.AuthRefCount() == 0 && len(coord.sem) == 0 }, time.Second, time.Millisecond)
			assert.Equal(t, time.Unix(expiry, 0), <-store.deadlineSeen)
			assert.Equal(t, int64(1), hits.Load())
			assert.NotContains(t, store.sequenceString(), "gate")
			assert.NotContains(t, store.sequenceString(), "unsigned_cancel")
			assert.Empty(t, port.sentBodies)
		})
	}
}
