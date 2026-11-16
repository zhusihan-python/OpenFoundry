// P2.6 recoverable publish wiring (W3 integration, root-decisions-v2 v2.1 +
// w3-integration-authorized): the p26-v1 executor dispatch, the fresh-auth
// manual recovery endpoint, its coordinator ticket and the safe recovery
// projection. The legacy executor path is untouched — p26-v1 runs branch off
// by the ADMISSION-FROZEN run protocol before the legacy publish gate.
//
// Invariants carried here (root v2 items 3–6, 9; v2.1):
//   - the exact publish body is serialized once (cmruntime.PrepareRecoverable
//     Commit), frozen atomically with the gate, and every later dispatch —
//     first send or recovery replay — reuses the persisted bytes verbatim;
//     once an intent exists the source is NEVER pulled again.
//   - every remote send consumes the persisted budget FIRST (max 3 total,
//     manual included, restart-persistent); the client never retries alone.
//   - post-gate outcomes are never fabricated: unknown keeps the run running
//     with a safe wait state; only a strictly verified COMMITTED receipt may
//     finalize (success + checkpoint advance + owner release in ONE repo
//     transaction, re-verified independently inside the repo).
//   - authorization is the caller's fresh JWT from THIS request: header
//     memory only, never a persisted grant; without it no remote request and
//     no budget is spent.
//   - recovery coordinates through an independent ticket that deduplicates
//     per run and NEVER touches the admission authReg/active handles, and no
//     recovery failure path may RequestCancel the original run.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	syncdomain "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain/sync"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

// RecoveryStore is the P2.6 slice of the store, implemented by *repo.Repo.
// It is deliberately a SEPARATE optional interface (asserted at runtime)
// instead of new methods on Store: every legacy fake and embedder keeps
// compiling unchanged; p26-v1 runs fail closed with a typed error when the
// wired store does not support recovery.
type RecoveryStore interface {
	BeginRecoverablePublishGate(ctx context.Context, lease *repo.LeaseClaim, t repo.Tenant, mat repo.FrozenPublishMaterial) (*repo.IncrementalPublishGateResult, error)
	ConsumePublishSendBudget(ctx context.Context, lease *repo.LeaseClaim, t repo.Tenant) (repo.BudgetResult, error)
	RecordRecoveryWait(ctx context.Context, lease *repo.LeaseClaim, t repo.Tenant, reason repo.RecoveryWaitReason, retryAfter time.Duration) error
	FinalizeRecoveredRun(ctx context.Context, lease *repo.LeaseClaim, t repo.Tenant, receipt repo.PublishReceipt) (*repo.RecoveryFinalizeResult, error)
	ClaimRecovery(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, ttl time.Duration) (*repo.RecoveryClaimResult, error)
	ConsumeRecoveryQueryAttemptManual(ctx context.Context, lease *repo.LeaseClaim, t repo.Tenant, reason repo.RecoveryWaitReason) (repo.BudgetResult, error)
	// Automatic (default-off) seam: bounded keyset scan + the ONLY
	// automatic adoption entry, which re-adjudicates the full P24 grant in
	// the same lock transaction (never a bare ClaimRecovery after ArmClaim).
	ListRecoveryCandidates(ctx context.Context, hint time.Time, after *repo.RecoveryScanKey, limit int) ([]repo.RecoveryCandidate, error)
	ClaimAutomaticRecovery(ctx context.Context, runID uuid.UUID, arm *repo.ScheduleArmClaim, holder, token string, ttl time.Duration) (*repo.RecoveryClaimResult, error)
	// Automatic verification budget (cap 5, persisted, sticky).
	ConsumeRecoveryQueryAttempt(ctx context.Context, lease *repo.LeaseClaim, t repo.Tenant, reason repo.RecoveryWaitReason) (repo.BudgetResult, error)
	GetPublishIntentView(ctx context.Context, runID uuid.UUID, t repo.Tenant) (*repo.PublishIntentView, error)
	GetPublishIntentMaterial(ctx context.Context, runID uuid.UUID, t repo.Tenant) (*repo.PublishIntentMaterial, error)
}

// recoveryStoreFor returns the recovery-capable view of the wired store, or
// a typed refusal. Never falls back to a partial implementation: a p26-v1
// run MUST be able to gate, budget, send and finalize.
func (h *Handlers) recoveryStoreFor() (RecoveryStore, error) {
	rs, ok := h.Repo.(RecoveryStore)
	if !ok {
		return nil, errors.New("the wired store does not support the p26-v1 recovery protocol")
	}
	return rs, nil
}

// recoveryPort resolves the keyed-commit client port. Production wires
// Handlers.DatasetRecovery (or falls back to the same dataset-versioning
// base URL as the legacy port); tests inject fakes.
func (h *Handlers) recoveryPort() cmruntime.DatasetRecoveryPort {
	if h.DatasetRecovery != nil {
		return h.DatasetRecovery
	}
	if strings.TrimSpace(h.Config.DatasetServiceURL) == "" {
		return nil
	}
	return cmruntime.HTTPDatasetVersioningClient{BaseURL: h.Config.DatasetServiceURL}
}

// recoverableSyncFrame carries the live-execution state the p26-v1 helper
// needs from executeSnapshotSync: the work/cancel plumbing and the pre-gate
// failure closures are owned by the caller so both protocols keep ONE
// heartbeat, sticky-loss and interrupt semantics.
type recoverableSyncFrame struct {
	ctx           context.Context // the triggering request/service context
	workCtx       context.Context // cancellable work context (cancel/lease loss)
	stopHeartbeat func()
	fail          func(err error, contentHash *string) (*models.SyncRun, error)
	tenant        repo.Tenant
	lease         *repo.LeaseClaim
	job           *models.SyncJob
	conn          *models.Connection
	snapshot      *cmruntime.SnapshotResult
	activeBranch  string
	incremental   bool
	authorization string
	// backoff spaces the bounded in-invocation dependency retries; nil uses
	// repo.RecoveryBackoff. The persisted send budget (cap 3) is the hard
	// outer bound — the loop can never exceed it.
	backoff func(attempt int) time.Duration
	// guard is the optional per-outward check (automatic recovery's sticky
	// live-arm revalidation); it runs immediately before EVERY send attempt.
	guard func() error
}

// executeRecoverablePublish runs the p26-v1 dispatch: single serialization →
// atomic gate+intent → persisted budget → one 30s send of the exact bytes →
// strict usable-ACK → generic finalize. Entered BEFORE any legacy gate: the
// pre-gate cancel/sticky/interrupt checks have already run in the caller.
//
// Terminal-state discipline: everything up to (and including) gate entry is
// provably zero-dispatch, so fail/finishAborted remain legal there; after
// entry NOTHING may write a terminal state — dependency failures record a
// safe wait state and return an honest unknown.
func (h *Handlers) executeRecoverablePublish(f recoverableSyncFrame) (*models.SyncRun, error) {
	runID := f.lease.Run.ID
	rs, err := h.recoveryStoreFor()
	if err != nil {
		// Pre-gate (no intent, no dispatch): an honest fenced failure.
		return f.fail(err, nil)
	}
	port := h.recoveryPort()
	if port == nil {
		return f.fail(errors.New("dataset-versioning url is not configured — recoverable snapshot cannot be published"), nil)
	}
	content := f.snapshot.NDJSON
	if content == nil {
		content = []byte{}
	}
	publishMetadata := map[string]any{
		"content_hash": f.snapshot.ContentHash,
		"rows":         f.snapshot.Rows,
		"bytes":        f.snapshot.Bytes,
		"pages":        f.snapshot.Pages,
		"lineage": map[string]any{
			"source_id":   f.conn.ID,
			"sync_def_id": f.job.ID,
			"sync_run_id": runID,
		},
	}
	if f.incremental {
		// Lengths and counters only — the opaque checkpoint value never
		// enters publish metadata (root Q4 ruling).
		publishMetadata["incremental"] = map[string]any{
			"mode":                     f.lease.Run.CPMode,
			"base_epoch":               f.lease.Run.CPBaseEpoch,
			"input_checkpoint_len":     len(f.lease.Run.CPBaseValue),
			"completed_checkpoint_len": f.snapshot.CompletedCheckpointLen,
		}
	}
	// Single serialization. The branch comes from the pre-frozen precheck
	// (DatasetInfo.ActiveBranch) — never re-resolved after the intent exists.
	prepared, err := cmruntime.PrepareRecoverableCommit(cmruntime.DatasetCommitRequest{
		DatasetID:       f.job.OutputDatasetID,
		Branch:          f.activeBranch,
		Format:          "TEXT",
		LogicalPath:     fmt.Sprintf("sync-%s.ndjson", runID),
		ContentType:     "application/x-ndjson",
		Content:         content,
		RowCount:        f.snapshot.Rows,
		ContentHash:     f.snapshot.ContentHash,
		Summary:         "Connector snapshot sync " + f.job.ID.String(),
		CreateIfMissing: false,
		Metadata:        publishMetadata,
	}, repo.DerivePublishKey(runID))
	if err != nil {
		return f.fail(err, &f.snapshot.ContentHash)
	}
	var candidate *string
	if f.incremental {
		c := f.snapshot.CompletedCheckpoint
		candidate = &c
	}
	// Heartbeats stay ALIVE through the gate→budget→send window (root v2
	// item 5: multi-30s-send executions must keep a bounded heartbeat); this
	// defer is the single stop point for EVERY exit path of the dispatch
	// (idempotent with the caller's own stop via sync.Once).
	defer f.stopHeartbeat()
	gateCtx, cancelGate := context.WithTimeout(f.workCtx, syncLeaseCallTimeout)
	gate, err := rs.BeginRecoverablePublishGate(gateCtx, f.lease, f.tenant, repo.FrozenPublishMaterial{
		SourceID:            f.conn.ID,
		SyncDefID:           f.job.ID,
		Branch:              prepared.Branch,
		LogicalPath:         prepared.LogicalPath,
		ExactBody:           prepared.Body,
		CompletedCheckpoint: candidate,
		ContentHash:         prepared.ContentHash,
		Rows:                prepared.Rows,
		Bytes:               prepared.Bytes,
	})
	cancelGate()
	if err != nil {
		// The gate decision (intent+stamp) is atomic in the repo: an error
		// means the outcome is undecided — no dispatch may start and no
		// terminal state may be fabricated here. The run stays for takeover.
		return nil, fmt.Errorf("recoverable publish gate failed: %w", err)
	}
	switch gate.Outcome {
	case repo.IncrementalPublishGateEntered:
		// dispatch below
	case repo.IncrementalPublishGateCancelled:
		return h.finishAborted(f.ctx, f.tenant, f.lease, nil, "cancelled by request at the recoverable publish gate")
	case repo.IncrementalPublishGateBindingStale:
		return f.fail(errors.New("recovery binding changed since admission"), nil)
	default: // fenced / owner_lost: zero write, owner retained
		return nil, ErrLeaseLost
	}
	if err := f.workCtx.Err(); err != nil {
		// An intent now exists: verification/replay only. This execution is
		// done — the run stays running and the intent carries the truth.
		return nil, fmt.Errorf("recoverable publish interrupted after the gate: %w", err)
	}
	// Bounded REAL dependency retry loop (review round1 pending scope): the
	// persisted send budget (max 3 total, restart-persistent, manual
	// included) is the hard outer bound; every iteration re-checks the work
	// context first, consumes one budget unit, dispatches the SAME exact
	// bytes once (30s), and only dependency-shaped failures continue after
	// the recorded backoff wait. Typed caller rejections, conflicts and
	// receipt mismatches never retry and never terminalize a post-gate run;
	// heartbeats keep renewing through the whole window (the caller's loop
	// is still live — the protocol branch skipped the legacy stop).
	backoff := f.backoff
	if backoff == nil {
		backoff = repo.RecoveryBackoff
	}
	var receipt *cmruntime.DatasetCommitReceipt
	for attempt := 1; ; attempt++ {
		if err := f.workCtx.Err(); err != nil {
			return nil, fmt.Errorf("recoverable publish interrupted in the send loop: %w", err)
		}
		// Pre-budget guard (review round3 item 2): an already-observed loss
		// (e.g. during the gate wait) consumes no new send budget.
		if f.guard != nil {
			if gerr := f.guard(); gerr != nil {
				return nil, fmt.Errorf("outward guard refused the send before budget: %w", gerr)
			}
		}
		budgetCtx, cancelBudget := context.WithTimeout(context.WithoutCancel(f.workCtx), syncLeaseCallTimeout)
		budget, err := rs.ConsumePublishSendBudget(budgetCtx, f.lease, f.tenant)
		cancelBudget()
		if err != nil {
			return nil, fmt.Errorf("publish send budget check failed (no dispatch): %w", err)
		}
		if !budget.Allowed {
			// Sticky exhaustion: never send, never fake an outcome. Recovery
			// verification (manual fresh auth / auto query) is the only
			// forward path.
			if receipt == nil {
				return nil, errors.New("publish send budget is exhausted; dispatch refused, run stays recoverable")
			}
			break
		}
		// Post-budget recheck IMMEDIATELY before the dispatch (review round3
		// item 2): the budget call itself can block; an arm lost during that
		// window must not dispatch. Post-gate refusal stays an honest unknown.
		if err := f.workCtx.Err(); err != nil {
			return nil, fmt.Errorf("recoverable publish interrupted after budget: %w", err)
		}
		if f.guard != nil {
			if gerr := f.guard(); gerr != nil {
				return nil, fmt.Errorf("outward guard refused the recoverable send after budget: %w", gerr)
			}
		}
		sendCtx, cancelSend := context.WithTimeout(f.workCtx, syncPublishBudget)
		receipt, err = port.CommitPreparedOutput(sendCtx, *prepared, f.authorization)
		cancelSend()
		if err == nil {
			break
		}
		var dep *cmruntime.DependencyError
		if !errors.As(err, &dep) {
			// Typed rejection / conflict / mismatch: visible, final for this
			// invocation, never fabricated into a terminal state.
			slog.Warn("recoverable publish dispatch refused; outcome unknown, run stays running",
				slog.String("error", cmruntime.SanitizeSyncError(err.Error())), slog.String("sync_run_id", runID.String()))
			return nil, fmt.Errorf("recoverable publish dispatch refused (outcome unknown): %w", err)
		}
		// Dependency failure: record the safe wait, then wait the bounded
		// backoff (interruptible) before the next budgeted same-byte retry.
		waitCtx, cancelWait := context.WithTimeout(context.WithoutCancel(f.ctx), syncLeaseCallTimeout)
		if werr := rs.RecordRecoveryWait(waitCtx, f.lease, f.tenant, repo.RecoveryWaitSendDeferred, backoff(budget.Attempt)); werr != nil {
			slog.Warn("recovery wait state could not be recorded", slog.String("error", werr.Error()), slog.String("sync_run_id", runID.String()))
		}
		cancelWait()
		slog.Warn("recoverable publish dispatch failed; bounded retry pending",
			slog.String("error", cmruntime.SanitizeSyncError(err.Error())), slog.String("sync_run_id", runID.String()))
		select {
		case <-time.After(backoff(budget.Attempt)):
		case <-f.workCtx.Done():
			return nil, fmt.Errorf("recoverable publish interrupted during dependency backoff: %w", f.workCtx.Err())
		}
	}
	// Defense in depth: the runtime client already refuses a non-matching
	// receipt, but the executor re-applies the strict prepared-commit match so
	// an injected port cannot bypass the usable-ACK contract (P2.5 correction
	// 6 spirit; the repo re-verifies independently inside the finalize).
	if err := prepared.ValidateReceipt(receipt); err != nil {
		slog.Warn("recoverable publish receipt failed the strict match; run stays running",
			slog.String("error", cmruntime.SanitizeSyncError(err.Error())), slog.String("sync_run_id", runID.String()))
		return nil, fmt.Errorf("recoverable publish receipt was rejected (outcome unknown, run stays running): %w", err)
	}
	finCtx, cancelFin := context.WithTimeout(context.WithoutCancel(f.ctx), syncFinalWriteWindow)
	defer cancelFin()
	fin, err := rs.FinalizeRecoveredRun(finCtx, f.lease, f.tenant, publishReceiptFrom(receipt))
	if err != nil {
		slog.Warn("recovery finalize did not reach a persisted terminal state",
			slog.String("error", err.Error()), slog.String("sync_run_id", runID.String()))
		return nil, fmt.Errorf("recovery finalize did not reach a terminal state (receipt verified, run stays recoverable): %w", err)
	}
	return fin.Run, nil
}

// publishReceiptFrom maps the runtime receipt into the repo's immutable
// receipt evidence shape (PublishIntentMaterial is the repo-side truth).
// The full lineage triple and branch are carried: the repo-side
// ReceiptMatchesIntent verifies every one of them independently.
func publishReceiptFrom(r *cmruntime.DatasetCommitReceipt) repo.PublishReceipt {
	return repo.PublishReceipt{
		IdempotencyKey:      r.IdempotencyKey,
		RequestFingerprint:  r.RequestFingerprint,
		DatasetID:           r.DatasetID,
		Branch:              r.Branch,
		TransactionID:       r.TransactionID,
		Status:              r.Status,
		MetadataContentHash: r.ContentHash,
		MetadataSyncRunID:   r.Lineage.SyncRunID,
		MetadataSourceID:    r.Lineage.SourceID,
		MetadataSyncDefID:   r.Lineage.SyncDefID,
	}
}

// preparedFromIntent rebuilds the runtime prepared commit from the
// persisted immutable intent — the ONLY legal replay input. Any mismatch
// between these fields and the stored exact body is caught client-side
// before anything reaches the wire (fingerprint + material deep check).
func preparedFromIntent(m *repo.PublishIntentMaterial) cmruntime.PreparedCommit {
	return cmruntime.PreparedCommit{
		DatasetID:   m.OutputDatasetID,
		Key:         m.PublishKey,
		Branch:      m.Branch,
		LogicalPath: m.LogicalPath,
		Body:        m.ExactBody,
		Fingerprint: m.RequestFingerprint,
		ContentHash: m.ContentHash,
		Rows:        m.Rows,
		Bytes:       m.Bytes,
		Lineage: cmruntime.DatasetLineageIdentity{
			SourceID:  m.SourceID,
			SyncDefID: m.SyncDefID,
			SyncRunID: m.RunID,
		},
	}
}

// preGateRecoveryView is the safe projection for a p26-v1 run that has no
// intent yet (frozen protocol, budgets untouched, nothing persisted).
func preGateRecoveryView() *repo.PublishIntentView {
	return &repo.PublishIntentView{
		Protocol:    models.RunRecoveryProtocolP26V1,
		State:       "pre_gate",
		SendBudget:  repo.MaxPublishSendAttempts,
		QueryBudget: repo.MaxRecoveryQueryAttempts,
	}
}

// recoveryViewFor assembles the safe recovery projection for a run: the
// persisted intent view when one exists, the pre-gate marker otherwise. It
// NEVER carries the body, candidate, key or fingerprint.
func recoveryViewFor(ctx context.Context, rs RecoveryStore, runID uuid.UUID, t repo.Tenant) (*repo.PublishIntentView, error) {
	view, err := rs.GetPublishIntentView(ctx, runID, t)
	if err != nil || view != nil {
		return view, err
	}
	return preGateRecoveryView(), nil
}

// ---------------------------------------------------------------------------
// Manual fresh-auth recovery endpoint
// ---------------------------------------------------------------------------

// recoveryTicket is the independent per-run recovery admission: it shares
// the coordinator's capacity accounting (so recovery is bounded with
// everything else) but lives in its own registry — it never touches the
// admission authReg or the active execution handles, and releasing it never
// cancels anything else (root v2 item 9).
//
// The ticket carries a REAL execution context (review round1 item 3): a
// child of the service root, linked to the triggering request, bounded by
// min(localAuthTTL, taskDeadline) anchored at reserve time. Stop's root
// cancel therefore interrupts an in-flight manual recovery and its gateWG
// count makes Stop join the exact ticket before main can close the pool; a
// request disconnect or an expiry deadline stops any NEW remote dispatch
// (checked before every query/send/renew), while the durable outcome is
// always left untouched.
type recoveryTicket struct {
	c       *RunCoordinator
	runID   uuid.UUID
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	release func()
}

// Context returns the ticket's bounded execution context. Every remote and
// lease operation of the recovery runs under it.
func (t *recoveryTicket) Context() context.Context {
	if t == nil {
		return context.Background()
	}
	return t.ctx
}

// Release returns the ticket exactly once (durable outcome is untouched:
// the recovery claim lease expires on its own).
func (t *recoveryTicket) Release() {
	if t == nil {
		return
	}
	t.once.Do(t.release)
}

// closeUnexecutedRunForShutdown is the shutdown-race durable close for a run
// whose admission never dispatched — EXCEPT p26-v1 rows (review round1 item
// 4): a v1 run stays pending/running for fresh-auth recovery instead of
// receiving an unsigned nil-generation cancel.
func (h *Handlers) closeUnexecutedRunForShutdown(ctx context.Context, runID, syncID uuid.UUID, tenant repo.Tenant, cause string) error {
	// FAIL CLOSED (review followup): a failed probe never authorizes the
	// unsigned close; an invisible run is not closed at all.
	run, rerr := h.Repo.GetSyncRunForTenant(ctx, syncID, runID, tenant)
	if rerr != nil {
		return rerr
	}
	if run == nil {
		return nil // nothing visible to close
	}
	if run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		slog.Info("shutdown left p26-v1 run for fresh-auth recovery instead of durable close",
			slog.String("sync_run_id", runID.String()))
		return nil
	}
	_, err := h.Repo.RequestCancelSyncRun(ctx, runID, syncID, tenant, nil, cause)
	return err
}

// jwtExpiryFromClaims returns the ALREADY-VERIFIED caller JWT's absolute
// expiry (zero when the token carries no exp claim — dev/local tokens).
func jwtExpiryFromClaims(claims *authmw.Claims) time.Time {
	if claims == nil || claims.EXP <= 0 {
		return time.Time{}
	}
	return time.Unix(claims.EXP, 0)
}

// ReserveRecoveryTicket reserves a bounded, per-run-deduplicated recovery
// ticket whose context is linked to BOTH the service root and the given
// request context and expires at min(reserve+AuthMemoryTTL,
// reserve+TaskDeadline) — the same deadline rule class the admission path
// applies, without touching any other run's authReg/active registration.
// nil means refused: coordinator stopped/root dead, another recovery for
// the SAME run in flight, or capacity exhausted.
func (c *RunCoordinator) ReserveRecoveryTicket(parent context.Context, runID uuid.UUID, authExpiry time.Time) *recoveryTicket {
	c.gateMu.Lock()
	if c.stopped || c.rootCtx.Err() != nil {
		c.gateMu.Unlock()
		return nil
	}
	c.recoveryMu.Lock()
	if _, dup := c.recoveryTickets[runID]; dup {
		c.recoveryMu.Unlock()
		c.gateMu.Unlock()
		return nil
	}
	select {
	case c.sem <- struct{}{}:
		c.gateWG.Add(1)
	default:
		c.recoveryMu.Unlock()
		c.gateMu.Unlock()
		return nil
	}
	baseCtx, baseCancel := context.WithCancel(c.rootCtx)
	var stopLink func() bool
	if parent != nil {
		stopLink = context.AfterFunc(parent, baseCancel)
	}
	// Deadline = min(task deadline, local auth-memory TTL, the caller's
	// already-verified JWT expiry). A zero authExpiry is a no-op cap.
	deadlineAt := time.Now().Add(c.cfg.TaskDeadline)
	if authEnd := time.Now().Add(c.cfg.AuthMemoryTTL); authEnd.Before(deadlineAt) {
		deadlineAt = authEnd
	}
	if !authExpiry.IsZero() && authExpiry.Before(deadlineAt) {
		deadlineAt = authExpiry
	}
	ticket := &recoveryTicket{c: c, runID: runID}
	dlCtx, dlCancel := context.WithDeadline(baseCtx, deadlineAt)
	ticket.ctx = dlCtx
	ticket.cancel = func() {
		if stopLink != nil {
			stopLink()
		}
		dlCancel()
		baseCancel()
	}
	ticket.release = func() {
		c.recoveryMu.Lock()
		delete(c.recoveryTickets, runID)
		c.recoveryMu.Unlock()
		ticket.cancel()
		c.releaseSlot()
	}
	c.recoveryTickets[runID] = ticket
	c.recoveryMu.Unlock()
	c.gateMu.Unlock()
	return ticket
}

// syncRunRecoveryResponse is the safe run projection plus the recovery view.
type syncRunRecoveryResponse struct {
	models.SyncRun
	Recovery *repo.PublishIntentView `json:"recovery,omitempty"`
}

// writeRecoveryOutcome answers a recovery invocation with machine-safe
// material only: the run (when one was read) and the safe recovery view.
func (h *Handlers) writeRecoveryOutcome(w http.ResponseWriter, status int, run *models.SyncRun, view *repo.PublishIntentView) {
	if run == nil {
		writeJSON(w, status, map[string]any{"recovery": view})
		return
	}
	writeJSON(w, status, syncRunRecoveryResponse{SyncRun: *run, Recovery: view})
}

// RecoverSyncRun serves POST /syncs/{id}/runs/{run_id}/recover — the P2.6
// manual fresh-auth recovery entry. The caller's Authorization header from
// THIS request is the only remote credential; without it nothing remote
// runs and no budget is spent. Post-intent recoveries verify exactly once
// (manual budget), may replay the exact persisted bytes only within the
// persisted send budget, and finalize only on a strictly verified receipt.
func (h *Handlers) RecoverSyncRun(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	syncID, param, err := routeUUIDParam(r, "id", "sync_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	runID, param, err := routeUUIDParam(r, "run_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if authorization == "" {
		// No fresh auth: no remote request, no budget, no ticket.
		writeJSONErr(w, http.StatusUnauthorized, "recovery requires a fresh caller authorization header")
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), syncID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	conn, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if conn == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(conn, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	rs, err := h.recoveryStoreFor()
	if err != nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "recovery is not available on the wired store")
		return
	}
	// UNCONDITIONAL parent-chain verification before anything durable
	// (review round1 item 2): the EXACT (syncID, runID) pair under this
	// tenant, frozen on the p26-v1 protocol. A same-tenant run that lives
	// under a different def must NOT be claimable through this route's
	// permissions — wrong parent answers 404 with zero changes, a non-v1
	// run answers the typed not-eligible conflict.
	run, err := h.Repo.GetSyncRunForTenant(r.Context(), syncID, runID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if run == nil {
		writeJSONErr(w, http.StatusNotFound, "sync run not found")
		return
	}
	if run.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		writeJSONErr(w, http.StatusConflict, "run is not a p26-v1 recovery run")
		return
	}
	// Read-only already-finalized short circuit: a succeeded run with a
	// finalized intent answers with the persisted success — verified by
	// reading only, without claiming anything or requiring the current
	// checkpoint owner (later batches may have advanced it).
	if view, verr := rs.GetPublishIntentView(r.Context(), runID, tenant); verr != nil {
		writeJSONErr(w, http.StatusInternalServerError, verr.Error())
		return
	} else if view != nil && view.State == repo.IntentStateFinalized {
		if run.Status == syncdomain.RunStatusSucceeded {
			h.writeRecoveryOutcome(w, http.StatusOK, run, view)
			return
		}
		// Finalized intent without a succeeded run is out-of-band corruption:
		// report it honestly instead of guessing a claim outcome.
		writeJSONErr(w, http.StatusConflict, "publish intent is finalized but the run is not succeeded; manual investigation required")
		return
	}
	// Independent bounded ticket, per-run deduplicated, BEFORE any claim.
	// Linked to this request + the service root + the auth/task deadlines.
	ticket := h.coordinatorFor().ReserveRecoveryTicket(r.Context(), runID, jwtExpiryFromClaims(claims))
	if ticket == nil {
		writeJSONErr(w, http.StatusConflict, "a recovery for this run is already in flight or the service is shutting down")
		return
	}
	defer ticket.Release()
	// Everything from here runs under the TICKET's bounded context (linked
	// to this request, the service root and the auth/task deadlines), not
	// the raw request context (review round1 item 3).
	tctx := ticket.Context()

	token, err := newLeaseToken()
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "lease token could not be minted")
		return
	}
	claimCtx, cancelClaim := context.WithTimeout(tctx, syncLeaseCallTimeout)
	claim, err := rs.ClaimRecovery(claimCtx, runID, tenant, h.leaseHolder(), token, repo.RecoveryGateLeaseTTL)
	cancelClaim()
	if err != nil {
		writeJSONErr(w, http.StatusBadGateway, "recovery claim failed: "+cmruntime.SanitizeSyncError(err.Error()))
		return
	}
	switch claim.Outcome {
	case repo.RecoveryClaimNotVisible:
		writeJSONErr(w, http.StatusNotFound, "sync run not found")
		return
	case repo.RecoveryClaimBusyLiveHolder:
		writeJSONErr(w, http.StatusConflict, "a live execution holds the run; retry after it converges or its lease expires")
		return
	case repo.RecoveryClaimBindingStale:
		writeJSONErr(w, http.StatusConflict, "recovery binding changed since the run was frozen")
		return
	case repo.RecoveryClaimNotEligible:
		writeJSONErr(w, http.StatusConflict, "run is not eligible for recovery")
		return
	case repo.RecoveryClaimAlreadyResolved:
		// A concurrent recovery finalized the run between the read-only
		// short circuit and this claim: answer the same read-only success
		// (verified below), zero writes.
		if view, verr := rs.GetPublishIntentView(tctx, runID, tenant); verr == nil && view != nil {
			if fresh, ferr := h.Repo.GetSyncRunForTenant(tctx, syncID, runID, tenant); ferr == nil && fresh != nil && fresh.Status == syncdomain.RunStatusSucceeded {
				h.writeRecoveryOutcome(w, http.StatusOK, fresh, view)
				return
			}
		}
		writeJSONErr(w, http.StatusConflict, "run was already resolved by another recovery")
		return
	}
	if claim.CancelRequested {
		if !claim.IntentExists {
			// Pre-gate crashed + cancelled run: nothing was ever dispatched
			// (no intent), the legacy takeover refuses v1 and the persisted
			// cancel flag would otherwise strand the run forever. Converge it
			// NOW through the fenced pre-gate terminal write under THIS fresh
			// recovery lease — zero source pull, zero budget, zero remote —
			// and answer only the actual persisted outcome.
			aborted, aerr := h.finishAborted(tctx, tenant, claim.Claim, nil, "cancelled by request (recovery converged a pre-gate crashed run)")
			if aerr != nil {
				writeJSONErr(w, http.StatusBadGateway, "pre-gate cancel convergence failed; the run stays honestly unconfirmed")
				return
			}
			view, verr := recoveryViewFor(tctx, rs, runID, tenant)
			if verr != nil {
				writeJSONErr(w, http.StatusInternalServerError, verr.Error())
				return
			}
			h.writeRecoveryOutcome(w, http.StatusOK, aborted, view)
			return
		}
		writeJSONErr(w, http.StatusConflict, "a cancellation request is persisted for this run; recovery refused")
		return
	}

	// Sticky dispatch guard (review round1 item 3): the single 60s claim
	// cannot cover a 30s query followed by a 30s replay, so EVERY remote
	// step first renews the recovery lease (bounded) and re-checks the
	// ticket's freshness (request/service/deadline). The first observed
	// loss stops all further dispatch — no renewal recovery, no new send.
	var stickyLoss error
	freshAndRenew := func() error {
		if stickyLoss != nil {
			return stickyLoss
		}
		if err := tctx.Err(); err != nil {
			stickyLoss = fmt.Errorf("recovery ticket expired or cancelled before dispatch: %w", err)
			return stickyLoss
		}
		rctx, cancel := context.WithTimeout(tctx, syncLeaseCallTimeout)
		defer cancel()
		renewed, rerr := h.Repo.RenewSyncRunLease(rctx, runID, tenant, claim.Claim.Holder, claim.Claim.Token, claim.Claim.Run.Generation, repo.RecoveryGateLeaseTTL)
		if rerr != nil || renewed == nil {
			stickyLoss = fmt.Errorf("recovery lease renewal failed; no further remote dispatch")
			return stickyLoss
		}
		return nil
	}

	if !claim.IntentExists {
		// PRE-GATE crash recovery: no intent exists, so a fresh execution on
		// the frozen baseline is legal. The normal executor path re-pulls the
		// source ONCE, and the protocol routing inside executeSnapshotSync
		// drives the p26-v1 dispatch (gate → budget → exact-body send).
		if err := freshAndRenew(); err != nil {
			writeJSONErr(w, http.StatusConflict, cmruntime.SanitizeSyncError(err.Error()))
			return
		}
		run, execErr := h.executeSnapshotSync(tctx, authorization, tenant, claim.Claim, job, conn, true)
		view, verr := recoveryViewFor(tctx, rs, runID, tenant)
		if verr != nil {
			writeJSONErr(w, http.StatusInternalServerError, verr.Error())
			return
		}
		if execErr != nil {
			// Honest unknown: the run keeps its own durable state (likely
			// still running with a wait reason) — never fabricated here.
			slog.Warn("manual recovery execution did not converge", slog.String("error", cmruntime.SanitizeSyncError(execErr.Error())), slog.String("sync_run_id", runID.String()))
			h.writeRecoveryOutcome(w, http.StatusAccepted, run, view)
			return
		}
		h.writeRecoveryOutcome(w, http.StatusOK, run, view)
		return
	}

	// POST-GATE recovery through the SHARED invocation service (manual and
	// the default-off automatic loop run the same decision tree; only the
	// query-budget surface differs).
	out := h.runPostGateRecovery(tctx, rs, claim, tenant, runID, authorization, true, nil)
	if out.err != nil {
		writeJSONErr(w, out.status, cmruntime.SanitizeSyncError(out.err.Error()))
		return
	}
	h.writeRecoveryOutcome(w, out.status, out.run, out.view)
}

// postGateOutcome is the shared manual/automatic post-intent recovery
// result. status is the manual surface's HTTP status; the automatic loop
// only logs it. err is ALWAYS pre-sanitized (no material ever carried).
type postGateOutcome struct {
	status int
	run    *models.SyncRun
	view   *repo.PublishIntentView
	err    error
}

// runPostGateRecovery is the single post-intent recovery service: one
// (manual) or capped-automatic query budget, sticky fresh+renewal before
// EVERY outward step, a bounded same-byte replay loop inside the persisted
// send budget, the two-layer receipt verification and the generic finalize.
// It never fabricates a terminal state: every failure keeps the run
// recoverable and reports the honest unknown.
func (h *Handlers) runPostGateRecovery(ctx context.Context, rs RecoveryStore, claim *repo.RecoveryClaimResult, tenant repo.Tenant, runID uuid.UUID, authorization string, manual bool, guard func() error) postGateOutcome {
	fail := func(status int, err error) postGateOutcome {
		return postGateOutcome{status: status, err: err}
	}
	intent := claim.Intent
	port := h.recoveryPort()
	if port == nil {
		return fail(http.StatusServiceUnavailable, errors.New("dataset-versioning url is not configured"))
	}
	// Sticky dispatch guard: every outward step first re-checks the context
	// freshness and renews the recovery lease (single 60s claim cannot cover
	// query+replays); the first observed loss stops all further dispatch.
	var stickyLoss error
	freshAndRenew := func() error {
		if stickyLoss != nil {
			return stickyLoss
		}
		if err := ctx.Err(); err != nil {
			stickyLoss = fmt.Errorf("recovery context expired or cancelled before dispatch")
			return stickyLoss
		}
		rctx, cancel := context.WithTimeout(ctx, syncLeaseCallTimeout)
		defer cancel()
		renewed, rerr := h.Repo.RenewSyncRunLease(rctx, runID, tenant, claim.Claim.Holder, claim.Claim.Token, claim.Claim.Run.Generation, repo.RecoveryGateLeaseTTL)
		if rerr != nil || renewed == nil {
			stickyLoss = errors.New("recovery lease renewal failed; no further remote dispatch")
			return stickyLoss
		}
		return nil
	}
	// Entry guard BEFORE any budget consumption (review round3 item 2): an
	// already-observed arm loss must spend zero query budget and make zero
	// remote calls. (Manual passes a nil guard and is unaffected.)
	if guard != nil {
		if gerr := guard(); gerr != nil {
			return fail(http.StatusConflict, fmt.Errorf("outward guard refused the recovery before any budget: %w", gerr))
		}
	}
	qCtx, cancelQ := context.WithTimeout(ctx, syncLeaseCallTimeout)
	var qBudget repo.BudgetResult
	var qerr error
	if manual {
		// Exactly one query per explicit manual invocation.
		qBudget, qerr = rs.ConsumeRecoveryQueryAttemptManual(qCtx, claim.Claim, tenant, repo.RecoveryWaitVerifyUnavailable)
	} else {
		qBudget, qerr = rs.ConsumeRecoveryQueryAttempt(qCtx, claim.Claim, tenant, repo.RecoveryWaitVerifyUnavailable)
	}
	cancelQ()
	if qerr != nil {
		return fail(http.StatusBadGateway, fmt.Errorf("recovery query budget could not be consumed: %w", qerr))
	}
	if !qBudget.Allowed {
		return fail(http.StatusConflict, errors.New("recovery query was refused by the persisted intent state"))
	}
	if err := freshAndRenew(); err != nil {
		return fail(http.StatusConflict, err)
	}
	if guard != nil {
		if gerr := guard(); gerr != nil {
			return fail(http.StatusConflict, fmt.Errorf("outward guard refused the verification: %w", gerr))
		}
	}
	verifyCtx, cancelVerify := context.WithTimeout(ctx, syncPublishBudget)
	receipt, err := port.GetCommitReceipt(verifyCtx, intent.OutputDatasetID, intent.PublishKey, authorization)
	cancelVerify()
	if err != nil && !errors.Is(err, cmruntime.ErrCommitReceiptNotFound) {
		var dep *cmruntime.DependencyError
		if errors.As(err, &dep) {
			wCtx, cancelW := context.WithTimeout(context.WithoutCancel(ctx), syncLeaseCallTimeout)
			_ = rs.RecordRecoveryWait(wCtx, claim.Claim, tenant, repo.RecoveryWaitVerifyUnavailable, repo.RecoveryBackoff(qBudget.Attempt))
			cancelW()
		}
		return fail(http.StatusBadGateway, errors.New("recovery verification failed; the run stays recoverable"))
	}
	if errors.Is(err, cmruntime.ErrCommitReceiptNotFound) {
		// No committed receipt visible YET — never proof of zero
		// publication. Same-byte replay is the only forward send, inside the
		// persisted budget (manual included, never reset) with a fresh
		// renewal per dispatch and the bounded dependency backoff.
		prepared := preparedFromIntent(intent)
		for {
			if err := freshAndRenew(); err != nil {
				return fail(http.StatusConflict, err)
			}
			// Pre-budget guard (retained): an already-observed loss spends no
			// new send budget.
			if guard != nil {
				if gerr := guard(); gerr != nil {
					return fail(http.StatusConflict, fmt.Errorf("outward guard refused the replay send: %w", gerr))
				}
			}
			sbCtx, cancelSB := context.WithTimeout(ctx, syncLeaseCallTimeout)
			sendBudget, serr := rs.ConsumePublishSendBudget(sbCtx, claim.Claim, tenant)
			cancelSB()
			if serr != nil {
				return fail(http.StatusBadGateway, fmt.Errorf("recovery send budget could not be consumed: %w", serr))
			}
			if !sendBudget.Allowed {
				if view, verr := recoveryViewFor(ctx, rs, runID, tenant); verr == nil {
					return postGateOutcome{status: http.StatusConflict, view: view,
						err: errors.New("send budget exhausted and no committed receipt is visible; run stays recoverable for verification only")}
				}
				return fail(http.StatusConflict, errors.New("send budget exhausted and no committed receipt is visible; run stays recoverable for verification only"))
			}
			// Post-budget recheck IMMEDIATELY before the POST (review round3
			// item 2): the budget consumption itself can block; an arm lost
			// during that window must not dispatch with the cached JWT.
			if err := ctx.Err(); err != nil {
				return fail(http.StatusConflict, errors.New("recovery context ended before the replay send; the run stays recoverable"))
			}
			if guard != nil {
				if gerr := guard(); gerr != nil {
					return fail(http.StatusConflict, fmt.Errorf("outward guard refused the replay send after budget: %w", gerr))
				}
			}
			replayCtx, cancelReplay := context.WithTimeout(ctx, syncPublishBudget)
			receipt, err = port.CommitPreparedOutput(replayCtx, prepared, authorization)
			cancelReplay()
			if err == nil {
				break
			}
			var dep *cmruntime.DependencyError
			if !errors.As(err, &dep) {
				slog.Warn("recovery replay refused; outcome unknown", slog.String("error", cmruntime.SanitizeSyncError(err.Error())), slog.String("sync_run_id", runID.String()))
				return fail(http.StatusBadGateway, errors.New("recovery replay was refused; the publish outcome is unknown and the run stays recoverable"))
			}
			wCtx, cancelW := context.WithTimeout(context.WithoutCancel(ctx), syncLeaseCallTimeout)
			_ = rs.RecordRecoveryWait(wCtx, claim.Claim, tenant, repo.RecoveryWaitSendDeferred, repo.RecoveryBackoff(sendBudget.Attempt))
			cancelW()
			select {
			case <-time.After(repo.RecoveryBackoff(sendBudget.Attempt)):
			case <-ctx.Done():
				return fail(http.StatusConflict, errors.New("recovery context ended during replay backoff; the run stays recoverable"))
			}
		}
	}
	// Two-layer verification, both refusing zero-write — the NIL-SAFE
	// runtime strict match FIRST (an injected port answering (nil,nil) is
	// an honest unknown, never a usable ACK and never a panic), then the
	// repo's independent intent match.
	prepared := preparedFromIntent(intent)
	if err := prepared.ValidateReceipt(receipt); err != nil {
		return fail(http.StatusConflict, errors.New("committed receipt failed the strict prepared-commit match; manual investigation required"))
	}
	if err := repo.ReceiptMatchesIntent(publishReceiptFrom(receipt), *intent); err != nil {
		return fail(http.StatusConflict, errors.New("committed receipt does not match the frozen publish intent; manual investigation required"))
	}
	if err := freshAndRenew(); err != nil {
		return fail(http.StatusConflict, err)
	}
	finCtx, cancelFin := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
	defer cancelFin()
	fin, err := rs.FinalizeRecoveredRun(finCtx, claim.Claim, tenant, publishReceiptFrom(receipt))
	if err != nil {
		return fail(http.StatusBadGateway, errors.New("recovery finalize did not reach a terminal state; the run stays recoverable"))
	}
	view, verr := recoveryViewFor(ctx, rs, runID, tenant)
	if verr != nil {
		return fail(http.StatusInternalServerError, verr)
	}
	return postGateOutcome{status: http.StatusOK, run: fin.Run, view: view}
}
