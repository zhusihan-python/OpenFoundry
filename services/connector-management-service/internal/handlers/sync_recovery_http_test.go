package handlers_test

// P2.6 manual recover endpoint + safe projection tests, through the REAL
// handler and the REAL coordinator ticket path. The store embeds the legacy
// fakeStore (parent-chain reads) and adds the recording RecoveryStore
// surface; the keyed-commit client is a scripted port. The pre-gate recover
// branch (full re-pull through executeSnapshotSync) and cross-instance
// crash proofs stay for the PG/HTTP phase.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-chi/chi/v5"
	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

// p26HTTPStore adds the recording RecoveryStore surface to the legacy fake.
type p26HTTPStore struct {
	*fakeStore

	mu  sync.Mutex
	seq []string

	claimOutcome string
	claimCancel  bool
	intent       *repo.PublishIntentMaterial
	intentExists bool
	view         *repo.PublishIntentView
	queryBudget  repo.BudgetResult
	sendBudget   repo.BudgetResult
	finRun       *models.SyncRun

	denyLeaseRenewal bool // the sticky-loss injection for freshAndRenew

	sendBudgetFn2 func(call int) repo.BudgetResult
	sendCalls     int

	viewFirst *repo.PublishIntentView // first read (short circuit); later reads use view
	viewCalls int
}

// CompleteLeasedSyncRun records the fenced pre-gate terminal write.
func (s *p26HTTPStore) CompleteLeasedSyncRun(_ context.Context, _ uuid.UUID, _ repo.Tenant, _, _ string, _ int, status string, _, _, _ int64, _ *string, _ *string, _ *uuid.UUID, _ *string) (*models.SyncRun, error) {
	s.record("complete:" + status)
	return &models.SyncRun{Status: status}, nil
}

// Automatic seam surfaces.
func (s *p26HTTPStore) ListRecoveryCandidates(context.Context, time.Time, *repo.RecoveryScanKey, int) ([]repo.RecoveryCandidate, error) {
	s.record("scan")
	return nil, nil
}

func (s *p26HTTPStore) ClaimAutomaticRecovery(context.Context, uuid.UUID, *repo.ScheduleArmClaim, string, string, time.Duration) (*repo.RecoveryClaimResult, error) {
	s.record("claim_auto")
	return nil, nil
}

func (s *p26HTTPStore) ConsumeRecoveryQueryAttempt(context.Context, *repo.LeaseClaim, repo.Tenant, repo.RecoveryWaitReason) (repo.BudgetResult, error) {
	s.record("query_budget_auto")
	return repo.BudgetResult{}, nil
}

// RenewSyncRunLease is the recovery lease renewal under the handler's
// control (independent of the legacy fake lease table).
func (s *p26HTTPStore) RenewSyncRunLease(context.Context, uuid.UUID, repo.Tenant, string, string, int, time.Duration) (*repo.LeaseClaim, error) {
	s.record("renew")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.denyLeaseRenewal {
		return nil, nil
	}
	return &repo.LeaseClaim{}, nil
}

func (s *p26HTTPStore) record(step string) {
	s.mu.Lock()
	s.seq = append(s.seq, step)
	s.mu.Unlock()
}

func (s *p26HTTPStore) sequenceString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.seq, ",")
}

func (s *p26HTTPStore) BeginRecoverablePublishGate(context.Context, *repo.LeaseClaim, repo.Tenant, repo.FrozenPublishMaterial) (*repo.IncrementalPublishGateResult, error) {
	s.record("gate")
	return nil, fmt.Errorf("not scripted")
}

func (s *p26HTTPStore) ConsumePublishSendBudget(context.Context, *repo.LeaseClaim, repo.Tenant) (repo.BudgetResult, error) {
	s.record("send_budget")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendBudgetFn2 != nil {
		s.sendCalls++
		return s.sendBudgetFn2(s.sendCalls), nil
	}
	return s.sendBudget, nil
}

func (s *p26HTTPStore) RecordRecoveryWait(_ context.Context, _ *repo.LeaseClaim, _ repo.Tenant, reason repo.RecoveryWaitReason, _ time.Duration) error {
	s.record("wait:" + string(reason))
	return nil
}

func (s *p26HTTPStore) FinalizeRecoveredRun(_ context.Context, _ *repo.LeaseClaim, _ repo.Tenant, _ repo.PublishReceipt) (*repo.RecoveryFinalizeResult, error) {
	s.record("finalize")
	s.mu.Lock()
	defer s.mu.Unlock()
	return &repo.RecoveryFinalizeResult{Outcome: repo.RecoveryFinalizeCommitted, Run: s.finRun}, nil
}

func (s *p26HTTPStore) ClaimRecovery(_ context.Context, _ uuid.UUID, _ repo.Tenant, _, _ string, _ time.Duration) (*repo.RecoveryClaimResult, error) {
	s.record("claim_recovery")
	s.mu.Lock()
	defer s.mu.Unlock()
	return &repo.RecoveryClaimResult{
		Outcome:         repo.RecoveryClaimOutcome(s.claimOutcome),
		Claim:           &repo.LeaseClaim{Holder: "recovery", Token: "t", Run: models.SyncRun{ID: uuid.New(), Generation: 2}},
		Intent:          s.intent,
		IntentExists:    s.intentExists,
		CancelRequested: s.claimCancel,
	}, nil
}

func (s *p26HTTPStore) ConsumeRecoveryQueryAttemptManual(context.Context, *repo.LeaseClaim, repo.Tenant, repo.RecoveryWaitReason) (repo.BudgetResult, error) {
	s.record("query_budget_manual")
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queryBudget, nil
}

func (s *p26HTTPStore) GetPublishIntentView(context.Context, uuid.UUID, repo.Tenant) (*repo.PublishIntentView, error) {
	s.record("intent_view")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.viewCalls > 0 {
		return s.view, nil // later reads see the (possibly concurrent) finalization
	}
	s.viewCalls++
	return s.viewFirst, nil
}

func (s *p26HTTPStore) GetPublishIntentMaterial(context.Context, uuid.UUID, repo.Tenant) (*repo.PublishIntentMaterial, error) {
	return s.intent, nil
}

// p26HTTPPort scripts the keyed-commit client.
type p26HTTPPort struct {
	mu           sync.Mutex
	sentBodies   [][]byte
	getFn        func(datasetID uuid.UUID, key string) (*cmruntime.DatasetCommitReceipt, error)
	commitErrors []error // one entry per send; nil entry = success; exhausted = success
}

func (p *p26HTTPPort) CommitPreparedOutput(_ context.Context, prepared cmruntime.PreparedCommit, _ string) (*cmruntime.DatasetCommitReceipt, error) {
	p.mu.Lock()
	p.sentBodies = append(p.sentBodies, append([]byte(nil), prepared.Body...))
	var scripted error
	if len(p.commitErrors) > 0 {
		scripted = p.commitErrors[0]
		p.commitErrors = p.commitErrors[1:]
	}
	p.mu.Unlock()
	if scripted != nil {
		return nil, scripted
	}
	return &cmruntime.DatasetCommitReceipt{
		DatasetID:          prepared.DatasetID,
		Branch:             prepared.Branch,
		IdempotencyKey:     prepared.Key,
		RequestFingerprint: prepared.Fingerprint,
		TransactionID:      uuid.New(),
		Status:             "COMMITTED",
		ContentHash:        prepared.ContentHash,
		Lineage:            prepared.Lineage,
	}, nil
}

func (p *p26HTTPPort) GetCommitReceipt(_ context.Context, datasetID uuid.UUID, key string, _ string) (*cmruntime.DatasetCommitReceipt, error) {
	p.mu.Lock()
	fn := p.getFn
	p.mu.Unlock()
	if fn != nil {
		return fn(datasetID, key)
	}
	return nil, cmruntime.ErrCommitReceiptNotFound
}

type recoverFixture struct {
	h      *handlers.Handlers
	store  *p26HTTPStore
	port   *p26HTTPPort
	syncID uuid.UUID
	runID  uuid.UUID
	claims *authmw.Claims
}

func newRecoverFixture(t *testing.T) *recoverFixture {
	t.Helper()
	sub := uuid.New()
	store := &p26HTTPStore{fakeStore: &fakeStore{}}
	port := &p26HTTPPort{}
	syncID, runID := uuid.New(), uuid.New()
	sourceID := uuid.New()
	store.connections = []models.Connection{{
		ID: sourceID, Name: "src", ConnectorType: "rest_api",
		OwnerID: sub, Config: []byte(`{}`),
	}}
	store.syncJobs = map[uuid.UUID][]models.SyncJob{
		sourceID: {{ID: syncID, SourceID: sourceID, OutputDatasetID: uuid.New()}},
	}
	owner := sub
	store.runs = map[uuid.UUID][]models.SyncRun{
		syncID: {{ID: runID, SyncDefID: syncID, Status: "running", OwnerID: &owner, RecoveryProtocol: models.RunRecoveryProtocolP26V1, Generation: 1}},
	}
	h := &handlers.Handlers{Repo: store, DatasetRecovery: port}
	return &recoverFixture{h: h, store: store, port: port, syncID: syncID, runID: runID, claims: &authmw.Claims{Sub: sub}}
}

// withTestCoordinator installs a coordinator with test-friendly deadlines so
// ticket expiry behavior is observable through the real handler.
func (f *recoverFixture) withTestCoordinator(t *testing.T, authTTL, taskDeadline time.Duration) {
	t.Helper()
	coord := handlers.NewRunCoordinator(context.Background(), handlers.CoordinatorConfig{
		QueueDepth: 4, Workers: 1, AuthMemoryTTL: authTTL, TaskDeadline: taskDeadline,
	}, f.h)
	f.h.Coordinator = coord
	t.Cleanup(func() { _ = coord.Stop() })
}

func (f *recoverFixture) recoverRequest(t *testing.T, authorization string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/v1/data-connection/syncs/%s/runs/%s/recover", f.syncID, f.runID), nil)
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), f.claims))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", f.syncID.String())
	rctx.URLParams.Add("run_id", f.runID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	w := httptest.NewRecorder()
	f.h.RecoverSyncRun(w, req)
	return w
}

// intentFixture builds a consistent persisted intent through the REAL single
// serialization site (cmruntime.PrepareRecoverableCommit).
func (f *recoverFixture) intentFixture(t *testing.T) *repo.PublishIntentMaterial {
	t.Helper()
	content := []byte("{\"a\":1}\n")
	sum := sha256.Sum256(content)
	connID := f.store.connections[0].ID
	job := f.store.syncJobs[connID][0]
	prepared, err := cmruntime.PrepareRecoverableCommit(cmruntime.DatasetCommitRequest{
		DatasetID:   job.OutputDatasetID,
		Branch:      "release",
		Format:      "TEXT",
		LogicalPath: "sync-" + f.runID.String() + ".ndjson",
		ContentType: "application/x-ndjson",
		Content:     content,
		RowCount:    1,
		ContentHash: hex.EncodeToString(sum[:]),
		Summary:     "Connector snapshot sync " + job.ID.String(),
		Metadata: map[string]any{
			"content_hash": hex.EncodeToString(sum[:]),
			"rows":         int64(1),
			"bytes":        int64(len(content)),
			"pages":        1,
			"lineage": map[string]any{
				"source_id":   connID,
				"sync_def_id": job.ID,
				"sync_run_id": f.runID,
			},
		},
	}, repo.DerivePublishKey(f.runID))
	require.NoError(t, err)
	return &repo.PublishIntentMaterial{
		RunID: f.runID, SyncDefID: job.ID, SourceID: connID,
		RecoveryProtocol:   models.RunRecoveryProtocolP26V1,
		PublishKey:         prepared.Key,
		RequestFingerprint: prepared.Fingerprint,
		ExactBody:          prepared.Body,
		OutputDatasetID:    job.OutputDatasetID,
		Branch:             prepared.Branch,
		LogicalPath:        prepared.LogicalPath,
		ContentHash:        prepared.ContentHash,
		Rows:               prepared.Rows,
		Bytes:              prepared.Bytes,
		RecoveryState:      repo.IntentStateArmed,
	}
}

func TestRecoverSyncRunRequiresFreshAuth(t *testing.T) {
	f := newRecoverFixture(t)
	w := f.recoverRequest(t, "")
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Empty(t, f.store.sequenceString(), "no fresh auth: no claim, no budget, nothing remote")
}

func TestRecoverSyncRunAlreadyFinalizedShortCircuit(t *testing.T) {
	f := newRecoverFixture(t)
	runCopy := f.store.runs[f.syncID][0]
	runCopy.Status = "succeeded"
	f.store.runs[f.syncID][0] = runCopy
	finalizedView := &repo.PublishIntentView{Protocol: "p26-v1", State: repo.IntentStateFinalized}
	f.store.viewFirst = finalizedView
	f.store.view = finalizedView

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "\"recovery\"")
	assert.Equal(t, "intent_view", f.store.sequenceString(), "read-only: no ticket/claim path taken")
	assert.NotContains(t, w.Body.String(), "publish_body", "no material ever serializes")
}

func TestRecoverSyncRunPostIntentVerifyAndFinalize(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.store.finRun = &models.SyncRun{ID: f.runID, Status: "succeeded"}
	f.port.getFn = func(datasetID uuid.UUID, key string) (*cmruntime.DatasetCommitReceipt, error) {
		assert.Equal(t, intent.OutputDatasetID, datasetID)
		assert.Equal(t, intent.PublishKey, key)
		return &cmruntime.DatasetCommitReceipt{
			DatasetID:          datasetID,
			Branch:             intent.Branch,
			IdempotencyKey:     key,
			RequestFingerprint: intent.RequestFingerprint,
			TransactionID:      uuid.New(),
			Status:             "COMMITTED",
			ContentHash:        intent.ContentHash,
			Lineage:            cmruntime.DatasetLineageIdentity{SourceID: intent.SourceID, SyncDefID: intent.SyncDefID, SyncRunID: intent.RunID},
		}, nil
	}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusOK, w.Code)
	// Manual ordering: one query budget, one GET verification, one finalize,
	// then the refreshed safe view for the response.
	assert.Equal(t, "intent_view,claim_recovery,query_budget_manual,renew,renew,finalize,intent_view", f.store.sequenceString())
	require.Len(t, f.port.sentBodies, 0, "a visible committed receipt needs no replay send")
	assert.NotContains(t, w.Body.String(), intent.PublishKey, "the key never appears in API output")
}

func TestRecoverSyncRunReceiptNotFoundReplaysExactBytesWithinBudget(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.store.sendBudget = repo.BudgetResult{Allowed: true, Attempt: 2, Remaining: 1}
	f.store.finRun = &models.SyncRun{ID: f.runID, Status: "succeeded"}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "intent_view,claim_recovery,query_budget_manual,renew,renew,send_budget,renew,finalize,intent_view", f.store.sequenceString())
	require.Len(t, f.port.sentBodies, 1)
	assert.Equal(t, intent.ExactBody, f.port.sentBodies[0], "the replay dispatches the persisted bytes verbatim")
}

func TestRecoverSyncRunExhaustedBudgetNeverSends(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.store.sendBudget = repo.BudgetResult{Allowed: false, Attempt: 3}
	f.store.view = &repo.PublishIntentView{Protocol: "p26-v1", State: repo.IntentStateSendBudgetExhausted}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Equal(t, "intent_view,claim_recovery,query_budget_manual,renew,renew,send_budget,intent_view", f.store.sequenceString())
	require.Empty(t, f.port.sentBodies, "exhausted budget: verification only, never a new send")
}

func TestRecoverSyncRunMismatchRefusedZeroWrite(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	// A committed receipt for the SAME key but a DIFFERENT body fingerprint.
	f.port.getFn = func(datasetID uuid.UUID, key string) (*cmruntime.DatasetCommitReceipt, error) {
		return &cmruntime.DatasetCommitReceipt{
			DatasetID:          datasetID,
			Branch:             intent.Branch,
			IdempotencyKey:     key,
			RequestFingerprint: strings.Repeat("ab", 32),
			TransactionID:      uuid.New(),
			Status:             "COMMITTED",
			ContentHash:        intent.ContentHash,
			Lineage:            cmruntime.DatasetLineageIdentity{SourceID: intent.SourceID, SyncDefID: intent.SyncDefID, SyncRunID: intent.RunID},
		}, nil
	}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.NotContains(t, f.store.sequenceString(), "finalize", "a mismatched receipt never finalizes")
	assert.NotContains(t, w.Body.String(), intent.RequestFingerprint)
}

func TestRecoverSyncRunClaimRefusalsTyped(t *testing.T) {
	cases := []struct {
		name    string
		outcome repo.RecoveryClaimOutcome
		want    int
	}{
		{"not visible", repo.RecoveryClaimNotVisible, http.StatusNotFound},
		{"live holder busy", repo.RecoveryClaimBusyLiveHolder, http.StatusConflict},
		{"binding stale", repo.RecoveryClaimBindingStale, http.StatusConflict},
		{"not eligible", repo.RecoveryClaimNotEligible, http.StatusConflict},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			f := newRecoverFixture(t)
			f.store.claimOutcome = string(testCase.outcome)
			w := f.recoverRequest(t, "Bearer fresh")
			assert.Equal(t, testCase.want, w.Code)
		})
	}
}

// A concurrent recovery finalized the run between the short circuit and the
// claim: already_resolved answers the read-only success, zero writes.
func TestRecoverSyncRunAlreadyResolvedAnswersReadOnlySuccess(t *testing.T) {
	f := newRecoverFixture(t)
	runCopy := f.store.runs[f.syncID][0]
	runCopy.Status = "succeeded"
	f.store.runs[f.syncID][0] = runCopy
	f.store.claimOutcome = string(repo.RecoveryClaimAlreadyResolved)
	f.store.intentExists = false
	f.store.viewFirst = nil // short-circuit read: not finalized yet
	f.store.view = &repo.PublishIntentView{Protocol: "p26-v1", State: repo.IntentStateFinalized}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "succeeded")
	assert.Equal(t, "intent_view,claim_recovery,intent_view", f.store.sequenceString(), "read-only: claim + view, nothing else")
}

// Manual replay runs the bounded dependency-retry loop inside the persisted
// budget: the first replay fails as a dependency error, the second (same
// bytes) succeeds and finalizes. The dependency backoff between the attempts
// is the real repo curve (1s for attempt 1).
func TestRecoverSyncRunReplayBoundedDependencyRetry(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.store.finRun = &models.SyncRun{ID: f.runID, Status: "succeeded"}
	f.store.sendBudgetFn2 = func(call int) repo.BudgetResult {
		return repo.BudgetResult{Allowed: true, Attempt: call, Remaining: 3 - call}
	}
	sends := 0
	f.port.commitErrors = []error{
		&cmruntime.DependencyError{Op: "commit_prepared_output", Status: 503}, // 1st: dependency
		nil, // 2nd: success
	}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusOK, w.Code)
	seq := f.store.sequenceString()
	assert.Contains(t, seq, "wait:send_deferred", "the failed replay records the safe wait")
	assert.Contains(t, seq, "finalize")
	sends = len(f.port.sentBodies)
	assert.Equal(t, 2, sends, "two same-byte dispatches inside the persisted budget")
	if len(f.port.sentBodies) == 2 {
		assert.Equal(t, intent.ExactBody, f.port.sentBodies[0])
		assert.Equal(t, f.port.sentBodies[0], f.port.sentBodies[1])
	}
}

func TestRecoverSyncRunCancelRequestedPostIntentRefused(t *testing.T) {
	f := newRecoverFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.claimCancel = true
	f.store.intent = f.intentFixture(t)
	f.store.intentExists = true // post-intent: zero-dispatch refusal only
	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.NotContains(t, f.store.sequenceString(), "complete:", "post-intent cancel NEVER releases the owner here")
}

// Review followup: a pre-gate crashed run with a persisted cancel request
// converges NOW under the fresh recovery lease — fenced terminal 'aborted'
// with zero source pull / budget / remote — answering the actual outcome.
func TestRecoverSyncRunPregateCancelledConvergesAborted(t *testing.T) {
	f := newRecoverFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.claimCancel = true
	f.store.intentExists = false // pre-gate crash

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "\"aborted\"")
	assert.Contains(t, f.store.sequenceString(), "complete:aborted", "the fenced pre-gate terminal write ran")
	assert.NotContains(t, f.store.sequenceString(), "send_budget", "zero budget, zero dispatch")
	assert.Empty(t, f.port.sentBodies, "zero remote")
}

func TestRecoverSyncRunVerifyDependencyFailureWaitsAndAnswers502(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.port.getFn = func(uuid.UUID, string) (*cmruntime.DatasetCommitReceipt, error) {
		return nil, &cmruntime.DependencyError{Op: "get_commit_receipt", Status: 500}
	}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusBadGateway, w.Code)
	assert.Contains(t, f.store.sequenceString(), "wait:verify_unavailable")
	assert.NotContains(t, f.store.sequenceString(), "finalize")
}

func TestGetSyncRunAttachesRecoveryView(t *testing.T) {
	f := newRecoverFixture(t)
	waitingView := &repo.PublishIntentView{Protocol: "p26-v1", State: repo.IntentStateWaiting, SendAttempts: 1, SendBudget: 3}
	f.store.viewFirst = waitingView
	f.store.view = waitingView
	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/api/v1/data-connection/syncs/%s/runs/%s", f.syncID, f.runID), nil)
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), f.claims))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", f.syncID.String())
	rctx.URLParams.Add("run_id", f.runID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	f.h.GetSyncRun(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "\"recovery\"")
	assert.Contains(t, body, "\"state\":\"waiting\"")
	assert.NotContains(t, body, "publish_body", "no material in the safe view")
}

func TestReserveRecoveryTicketDedupCapacityAndStop(t *testing.T) {
	coord := handlers.NewRunCoordinator(context.Background(), handlers.CoordinatorConfig{QueueDepth: 1, Workers: 1}, &handlers.Handlers{})
	t.Cleanup(func() { _ = coord.Stop() })
	runID := uuid.New()

	first := coord.ReserveRecoveryTicket(context.Background(), runID, time.Time{})
	require.NotNil(t, first)
	require.NoError(t, first.Context().Err(), "a fresh ticket context is live")
	assert.Equal(t, 1, coord.OutstandingAdmissions())

	// Same-run dedup and exhausted shared capacity both refuse without state.
	assert.Nil(t, coord.ReserveRecoveryTicket(context.Background(), runID, time.Time{}), "same-run recovery is deduplicated")
	assert.Nil(t, coord.ReserveRecoveryTicket(context.Background(), uuid.New(), time.Time{}), "capacity is shared and bounded")

	first.Release()
	assert.Equal(t, 0, coord.OutstandingAdmissions(), "the ticket returns its slot exactly once")
	assert.Error(t, first.Context().Err(), "release cancels the ticket context")
	first.Release() // idempotent

	again := coord.ReserveRecoveryTicket(context.Background(), runID, time.Time{})
	require.NotNil(t, again, "after release the run is recoverable again")
	again.Release()

	coord.CloseAdmission()
	assert.Nil(t, coord.ReserveRecoveryTicket(context.Background(), uuid.New(), time.Time{}), "no tickets after the gate closes")
}

// Review round1 item 3: the ticket participates in REAL service cancellation
// and request cancellation — Stop interrupts an in-flight recovery (its ctx
// dies, so no further remote dispatch can start) and Stop joins the ticket
// before returning.
func TestRecoveryTicketLinkedToServiceAndRequest(t *testing.T) {
	coord := handlers.NewRunCoordinator(context.Background(), handlers.CoordinatorConfig{QueueDepth: 2, Workers: 1}, &handlers.Handlers{})
	runID := uuid.New()

	// Service-root link: a ticket held across Stop gets its ctx cancelled
	// (Stop's root cancel interrupts the in-flight recovery) and Stop JOINS
	// the exact ticket (gateWG) before returning.
	held := coord.ReserveRecoveryTicket(context.Background(), runID, time.Time{})
	require.NotNil(t, held)
	go func() {
		time.Sleep(100 * time.Millisecond) // let Stop observe the live ticket
		held.Release()
	}()
	require.NoError(t, coord.Stop())
	assert.Error(t, held.Context().Err(), "Stop interrupts the in-flight manual recovery")

	// Request link: a cancelled request kills its own ticket only.
	coord2 := handlers.NewRunCoordinator(context.Background(), handlers.CoordinatorConfig{QueueDepth: 2, Workers: 1}, &handlers.Handlers{})
	t.Cleanup(func() { _ = coord2.Stop() })
	reqCtx, cancelReq := context.WithCancel(context.Background())
	byRequest := coord2.ReserveRecoveryTicket(reqCtx, uuid.New(), time.Time{})
	require.NotNil(t, byRequest)
	other := coord2.ReserveRecoveryTicket(context.Background(), uuid.New(), time.Time{})
	require.NotNil(t, other)
	cancelReq()
	require.Eventually(t, func() bool { return byRequest.Context().Err() != nil },
		time.Second, 5*time.Millisecond, "the dead request stops its ticket")
	require.NoError(t, other.Context().Err(), "unrelated tickets stay live")
	byRequest.Release()
	other.Release()
}

// Review round1 item 2: the parent chain is verified UNCONDITIONALLY before
// any ticket/claim/budget/remote — a same-tenant run living under another
// def cannot be claimed through this route.
func TestRecoverSyncRunWrongParent404ZeroChange(t *testing.T) {
	f := newRecoverFixture(t)
	otherDef := uuid.New()
	f.store.runs = map[uuid.UUID][]models.SyncRun{ // run lives under DefB
		otherDef: {f.store.runs[f.syncID][0]},
	}
	delete(f.store.runs, f.syncID)

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Empty(t, f.store.sequenceString(), "no intent view, no claim, no budget, nothing remote")
	assert.Empty(t, f.port.sentBodies)
}

func TestRecoverSyncRunNonV1ProtocolRefused(t *testing.T) {
	f := newRecoverFixture(t)
	runCopy := f.store.runs[f.syncID][0]
	runCopy.RecoveryProtocol = ""
	f.store.runs[f.syncID][0] = runCopy

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Empty(t, f.store.sequenceString(), "non-v1 refuses before anything durable")
}

// Review round1 item 3: a lost/expired recovery lease is sticky — the renew
// before the remote query fails and NO remote call is made.
func TestRecoverSyncRunRenewFailureStopsAllRemote(t *testing.T) {
	f := newRecoverFixture(t)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.store.denyLeaseRenewal = true // set before any request; no concurrent access
	getCalls := 0
	f.port.getFn = func(uuid.UUID, string) (*cmruntime.DatasetCommitReceipt, error) {
		getCalls++
		return nil, cmruntime.ErrCommitReceiptNotFound
	}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Zero(t, getCalls, "the remote verification never starts after a lost lease")
	assert.Empty(t, f.port.sentBodies, "and no replay send either")
}

// Review round1 item 3: the ticket deadline bounds the remote query — an
// auth-TTL shorter than the 30s verification interrupts it as a dependency
// timeout and nothing further is dispatched.
func TestRecoverSyncRunTicketDeadlineInterruptsQuery(t *testing.T) {
	f := newRecoverFixture(t)
	f.withTestCoordinator(t, 60*time.Millisecond, 5*time.Second)
	intent := f.intentFixture(t)
	f.store.claimOutcome = string(repo.RecoveryClaimGranted)
	f.store.intent = intent
	f.store.intentExists = true
	f.store.queryBudget = repo.BudgetResult{Allowed: true, Attempt: 1}
	f.port.getFn = func(uuid.UUID, string) (*cmruntime.DatasetCommitReceipt, error) {
		time.Sleep(400 * time.Millisecond) // outlives the 60ms ticket deadline
		return nil, &cmruntime.DependencyError{Op: "get_commit_receipt", Timeout: true}
	}

	w := f.recoverRequest(t, "Bearer fresh")
	assert.Equal(t, http.StatusBadGateway, w.Code)
	assert.Empty(t, f.port.sentBodies, "no send after the deadline cut the query short")
	assert.Contains(t, f.store.sequenceString(), "wait:verify_unavailable")
}
