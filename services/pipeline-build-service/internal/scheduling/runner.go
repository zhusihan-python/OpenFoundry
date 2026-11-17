// Canonical scheduling Runner (WP3.2 A2) — drives one claimed dispatch
// through the canonical chain published JobSpec → Preflight → admitted
// Build → Resolve → PersistResolved(+seal+RUNNING, ONE fenced tx) →
// Plans.LoadPlan → executor.Execute, with crash-phase recovery decided
// from PERSISTED state only. Never touches run-due or pipeline_runs.
// Phase model, five-step reservation handoff and terminal mapping are
// frozen in docs/architecture/data-integration-wp3-2-a2-checkpoint.md.
package scheduling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/outbox"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
)

// RunnerStore is the execution-side persistence surface (implemented by
// *postgres.Repository; scheduling_repo.go's ConsumerStore methods are
// consumed by the worker, not the runner). It also satisfies
// handler.ScheduledFencedStore structurally.
type RunnerStore interface {
	ScheduledBuildState(uuid.UUID, string, int64) *postgres.ScheduledBuildState

	ClearExecutionIntent(context.Context, uuid.UUID, string, int64) error

	RecordPendingPublishWithResult(context.Context, uuid.UUID, string, int64, string, string, string, []byte, json.RawMessage) error

	GetGrantByRef(context.Context, uuid.UUID) (*postgres.SchedulingGrant, error)

	MarkExecutionIntent(context.Context, uuid.UUID, string, int64, string, json.RawMessage) error

	FinishDispatch(ctx context.Context, eventID uuid.UUID, owner string, fence int64, state, errMsg string, extra postgres.ExtraTx) error
	LoadDispatchExecutionState(ctx context.Context, eventID uuid.UUID) (*postgres.DispatchExecutionSnapshot, error)
	PersistResolvedAndSeal(ctx context.Context, eventID uuid.UUID, owner string, fence int64, resolved *models.ResolvedBuild, expectedOutputs []postgres.ExpectedOutput, expectedNodes []string) error
	RecordAuditEventFenced(ctx context.Context, eventID uuid.UUID, owner string, fence int64, event executor.AuditEvent) error
	AssertDispatchFence(ctx context.Context, eventID uuid.UUID, owner string, fence int64) error
	RecordPendingReceipt(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key, nodeID, datasetRID string, body []byte) error
	MarkReceiptUnknown(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, reason string) error
	CompleteScheduledPublish(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, receipt json.RawMessage, datasetRID, reservationTxnRID, realTxnRID string) error
	LoadDefinitionBinding(ctx context.Context, tenant scheduleevents.TenantRef, scheduleKey string) (postgres.DefinitionBinding, error)
	ListPipelineOutputDatasets(ctx context.Context, pipelineRID, buildBranch string, fallbackChain []string) ([]string, error)
	MarkBuildFailed(ctx context.Context, buildID uuid.UUID, reason string) error
	ReleaseLock(ctx context.Context, outputDatasetRID string, buildID uuid.UUID) error
}

// CanonicalDeps carries the canonical chain ports plus the claim
// identity. Owner MUST be the same owner string the Consumer was built
// with (the fence pair is (owner, fence)).
type CanonicalDeps struct {
	Store      RunnerStore
	JobSpecs   resolver.JobSpecRepository
	Versioning resolver.DatasetVersioningRepository
	Locks      resolver.BranchLockRepository
	Builds     handler.BuildRepository
	Plans      handler.BuildPlanRepository
	NodeRunner executor.NodeRunner
	// MetadataCommitter/MetadataTransactions are the production manual
	// adapters, used only for NON-dataset outputs and job_outputs flags
	// (manual semantics unchanged for those).
	MetadataCommitter    executor.OutputCommitter
	MetadataTransactions executor.TransactionManager
	DVSBaseURL           string
	HTTPClient           *http.Client
	JWT                  *authmw.JWTConfig
	Cipher               *TokenCipher
	Owner                string
	Log                  *slog.Logger
}

// NewCanonicalRunner builds the Runner closure the ExecutionWorker drives.
func NewCanonicalRunner(deps CanonicalDeps) Runner {
	return (&canonicalRunner{deps: deps}).Run
}

type canonicalRunner struct {
	deps CanonicalDeps
}

func (r *canonicalRunner) log() *slog.Logger {
	if r.deps.Log != nil {
		return r.deps.Log
	}
	return slog.Default()
}

// dispatchTenant maps the dispatch's persisted tenant columns into a
// scheduleevents.TenantRef (kind+id, S1 domains).
func dispatchTenant(d postgres.SchedulingDispatch) (scheduleevents.TenantRef, error) {
	return scheduleevents.ParseTenantRef(fmt.Sprintf("%s:%s", d.TenantKind, d.TenantID.String()))
}

// verifyExecution re-verifies the CURRENT grant (exact generation, active,
// unexpired, tuple match), decrypts + re-validates the stored JWT
// (signature/exp/sub/DOMAIN match: org events execute with an org ticket,
// personal events with a bare-sub ticket), and re-checks the definition
// binding (enabled/revision/pipeline/branch/grant) before ANY execution
// or publish side effect (T8 decision §4, A2 checkpoint §3).
func (r *canonicalRunner) verifyExecution(ctx context.Context, d postgres.SchedulingDispatch, grant *postgres.SchedulingGrant) (string, error) {
	if grant == nil {
		return "", errors.New("grant_missing")
	}
	tenant, err := dispatchTenant(d)
	if err != nil {
		return "", err
	}
	switch {
	case grant.ID != d.GrantRef:
		return "", errors.New("grant_ref_mismatch")
	case grant.Generation != d.GrantGeneration:
		return "", errors.New("grant_generation_mismatch")
	case grant.State != "active":
		return "", fmt.Errorf("grant_state_%s", grant.State)
	case !grant.ExpiresAt.After(time.Now().UTC()):
		return "", errors.New("grant_expired")
	case grant.PipelineRID != d.PipelineRID:
		return "", errors.New("grant_pipeline_mismatch")
	case grant.TenantKind != d.TenantKind || grant.TenantID != d.TenantID:
		return "", errors.New("grant_tenant_mismatch")
	case grant.Subject == uuid.Nil:
		return "", errors.New("grant_subject_nil")
	}
	if r.deps.Cipher == nil {
		return "", errors.New("scheduler cipher unavailable")
	}
	tokenBytes, err := r.deps.Cipher.Decrypt(grant.TokenCiphertext)
	if err != nil {
		return "", errors.New("grant_token_decrypt_failed")
	}
	identity, err := ValidateGrantToken(r.deps.JWT, string(tokenBytes), time.Now().UTC())
	if err != nil {
		return "", errors.New("grant_token_invalid")
	}
	if identity.Tenant != tenant {
		return "", errors.New("grant_token_domain_mismatch")
	}
	if identity.Subject != grant.Subject {
		return "", errors.New("grant_token_subject_mismatch")
	}
	binding, err := r.deps.Store.LoadDefinitionBinding(ctx, tenant, d.ScheduleKey)
	if err != nil {
		return "", err
	}
	switch {
	case !binding.Found:
		return "", errors.New("definition_unbound")
	case !binding.Enabled:
		return "", errors.New("definition_disabled")
	case binding.DefinitionRevision != d.DefinitionRevision:
		return "", errors.New("definition_revision_mismatch")
	case binding.PipelineRID != d.PipelineRID:
		return "", errors.New("definition_pipeline_mismatch")
	case binding.BuildBranch != d.BuildBranch:
		return "", errors.New("definition_branch_mismatch")
	case binding.GrantRef != d.GrantRef:
		return "", errors.New("definition_grant_mismatch")
	}
	return string(tokenBytes), nil
}

// executionContext builds the tenant-scoped, bearer-carrying context the
// canonical chain runs under (org: TenantID=&org; personal: TenantID=nil,
// subject never stuffed into the org pointer — tenancy.go rules).
func (r *canonicalRunner) executionContext(ctx context.Context, d postgres.SchedulingDispatch, grant *postgres.SchedulingGrant, bearer string) context.Context {
	scope := &handler.TenantScope{Subject: grant.Subject}
	if d.TenantKind == string(scheduleevents.TenantKindOrg) {
		org := d.TenantID
		scope.TenantID = &org
	}
	ctx = handler.ContextWithTenantScope(ctx, scope)
	return handler.ContextWithSchedulerBearer(ctx, bearer)
}

type runnerPhase int

const (
	phaseUnresolved runnerPhase = iota
	phaseFresh
	phaseExecuted
)

func classifyPhase(snap *postgres.DispatchExecutionSnapshot) runnerPhase {
	if len(snap.Jobs) == 0 {
		return phaseUnresolved
	}
	for _, j := range snap.Jobs {
		if j.State != "WAITING" {
			return phaseExecuted
		}
	}
	for _, o := range snap.Outputs {
		if o.Committed || o.Aborted {
			return phaseExecuted
		}
	}
	if len(snap.Receipts) > 0 {
		return phaseExecuted
	}
	return phaseFresh
}

// Run executes one claimed dispatch. The fence is the dispatch row's
// CURRENT fence token read after the claim (drive() reloads fresh state
// between claim and runner); every durable child write re-verifies
// (owner, fence) server-side, so a taken-over dispatch fails closed.
func (r *canonicalRunner) Run(ctx context.Context, d postgres.SchedulingDispatch, grant *postgres.SchedulingGrant, stop <-chan struct{}) error {
	fence := d.FenceToken
	var err error
	grant, err = r.deps.Store.GetGrantByRef(ctx, d.GrantRef)
	if err != nil {
		return fmt.Errorf("load current execution grant: %w", err)
	}
	bearer, err := r.verifyExecution(ctx, d, grant)
	if err != nil {
		// The admitted opportunity cannot execute (revoked/re-granted/
		// re-bound/edited): honest terminal failure — failed (not skipped:
		// skipped/blocked belong to admission, and only failed releases
		// the activity slot this dispatch holds).
		rerr := fmt.Errorf("execution verify: %w", err)
		if ferr := r.finishTerminal(ctx, d, fence, postgres.DispatchStateFailed, rerr.Error()); ferr != nil {
			return fmt.Errorf("%v (finish also failed: %w)", rerr, ferr)
		}
		return nil
	}
	execCtx := r.executionContext(ctx, d, grant, bearer)

	snap, err := r.deps.Store.LoadDispatchExecutionState(ctx, d.EventID)
	if err != nil {
		return fmt.Errorf("execution snapshot: %w", err)
	}
	switch classifyPhase(snap) {
	case phaseUnresolved:
		if snap.IntentKind != "" {
			return r.finishRecoverable(ctx, d, fence, "unconfirmed_resolve_intent")
		}
		if len(snap.PendingTxns) > 0 {
			return r.recoverDanglingResolve(ctx, execCtx, d, fence, snap)
		}
		return r.runFreshChain(ctx, execCtx, d, fence, grant, stop)
	case phaseFresh:
		if !snap.ExpectedOutputsSealed || !snap.ExpectedNodesSealed {
			// Jobs persisted but sets unsealed (legacy seam): success is
			// impossible and the sets must never be guessed.
			return r.finishRecoverable(ctx, d, fence, "unsealed_resolved_plan")
		}
		return r.executePlan(ctx, execCtx, d, fence, snap, stop)
	default:
		return r.assessExecuted(ctx, execCtx, d, fence, snap)
	}
}

// runFreshChain is the P0-clean path: full canonical resolution into the
// SAME deterministic build row admission created.
func (r *canonicalRunner) runFreshChain(ctx context.Context, execCtx context.Context, d postgres.SchedulingDispatch, fence int64, grant *postgres.SchedulingGrant, stop <-chan struct{}) error {
	outputs, err := r.deps.Store.ListPipelineOutputDatasets(execCtx, d.PipelineRID, d.BuildBranch, nil)
	if err != nil {
		return fmt.Errorf("list pipeline outputs: %w", err)
	}
	if len(outputs) == 0 {
		return r.failBuild(ctx, d, fence, "no job specs published for pipeline on branch")
	}
	args := resolver.ResolveBuildArgs{
		BuildID:           &d.BuildID,
		PipelineRID:       d.PipelineRID,
		BuildBranch:       d.BuildBranch,
		JobSpecFallback:   []string{},
		OutputDatasetRIDs: outputs,
		RequestedBy:       grant.Subject.String(),
		TriggerKind:       "SCHEDULED",
		AbortPolicy:       string(models.AbortDependentOnly),
	}
	if _, _, err := resolver.PreflightBuild(execCtx, args, r.deps.JobSpecs, r.deps.Versioning); err != nil {
		return r.failBuild(ctx, d, fence, err.Error())
	}
	recorder := r.deps.Store.ScheduledBuildState(d.EventID, r.deps.Owner, fence)
	var recorders []resolver.PendingTxnRecorder
	if recorder != nil {
		recorders = append(recorders, recorder)
	}
	if err := r.deps.Store.MarkExecutionIntent(ctx, d.EventID, r.deps.Owner, fence, "resolve_started", nil); err != nil {
		return err
	}
	resolved, err := resolver.ResolveBuild(execCtx, args, r.deps.JobSpecs, r.deps.Versioning, r.deps.Store.ScheduledBuildState(d.EventID, r.deps.Owner, fence), recorders...)
	if err != nil {
		// A failed remote open may have succeeded without a returned RID.
		// The durable intent retains the activity slot for reconciliation.
		return r.finishRecoverable(ctx, d, fence, "resolve_outcome_unknown: "+err.Error())
	}
	if resolved.State == models.BuildQueued {
		if err := r.deps.Store.ClearExecutionIntent(ctx, d.EventID, r.deps.Owner, fence); err != nil {
			return err
		}
		// Upstream build in progress: no side effects yet; park for a
		// later scan-driven re-resolve (idempotent reads).
		return r.finishRecoverable(ctx, d, fence, "build_queued_upstream")
	}
	if len(resolved.Jobs) == 0 {
		return r.finishRecoverable(ctx, d, fence, "empty_resolved_plan_unknown")
	}
	expectedOutputs, expectedNodes := expectedSets(resolved)
	if err := r.deps.Store.PersistResolvedAndSeal(ctx, d.EventID, r.deps.Owner, fence, resolved, expectedOutputs, expectedNodes); err != nil {
		if errors.Is(err, postgres.ErrStaleFence) {
			return err // taken over; recovery owns it now
		}
		return fmt.Errorf("persist+seal: %w", err)
	}
	snap, err := r.deps.Store.LoadDispatchExecutionState(ctx, d.EventID)
	if err != nil {
		return fmt.Errorf("post-seal snapshot: %w", err)
	}
	return r.executePlan(ctx, execCtx, d, fence, snap, stop)
}

// expectedSets derives the frozen success contract from the resolved
// plan: every (job spec RID, output dataset) pair, and every job spec
// RID — the sets FinishDispatch later enforces universally.
func expectedSets(resolved *models.ResolvedBuild) ([]postgres.ExpectedOutput, []string) {
	seenPair := map[string]struct{}{}
	pairs := make([]postgres.ExpectedOutput, 0)
	nodes := make([]string, 0, len(resolved.JobSpecs))
	seenNode := map[string]struct{}{}
	for _, spec := range resolved.JobSpecs {
		if _, dup := seenNode[spec.RID]; !dup {
			seenNode[spec.RID] = struct{}{}
			nodes = append(nodes, spec.RID)
		}
		for _, output := range spec.OutputDatasetRIDs {
			key := spec.RID + "\x00" + output
			if _, dup := seenPair[key]; dup {
				continue
			}
			seenPair[key] = struct{}{}
			pairs = append(pairs, postgres.ExpectedOutput{NodeID: spec.RID, DatasetRID: output})
		}
	}
	return pairs, nodes
}

// executePlan is the only path that ever runs executor.Execute (P1:
// all-WAITING jobs, zero side-effect evidence). Root trap 2: any
// executed plan is assessed, never re-executed.
func (r *canonicalRunner) executePlan(ctx context.Context, execCtx context.Context, d postgres.SchedulingDispatch, fence int64, snap *postgres.DispatchExecutionSnapshot, stop <-chan struct{}) error {
	plan, err := r.deps.Plans.LoadPlan(execCtx, d.BuildID)
	if err != nil {
		return fmt.Errorf("load plan: %w", err)
	}
	reservations := map[string]string{}
	receipted := map[string]bool{}
	for _, rec := range snap.Receipts {
		receipted[rec.DatasetRID] = true
	}
	for i := range plan.Nodes {
		node := &plan.Nodes[i]
		for j := range node.Outputs {
			out := &node.Outputs[j]
			out.OutputNodeID = node.ID // job spec RID — the publish identity
			out.Branch = d.BuildBranch
			out.PipelineRID = d.PipelineRID
			out.CreateIfMissing = false
			if out.TransactionRID != "" {
				reservations[out.DatasetRID] = out.TransactionRID
			}
		}
	}
	aborter, _ := r.deps.Versioning.(resolver.TransactionAborter)
	committer := &handler.ScheduledOutputCommitter{
		Store: r.deps.Store, Aborter: aborter, Metadata: r.deps.MetadataCommitter,
		BaseURL: r.deps.DVSBaseURL, EventID: d.EventID, Owner: r.deps.Owner, Fence: fence,
		BuildBranch: d.BuildBranch, Reservations: reservations, ExistingReceipts: receipted,
	}
	if r.deps.HTTPClient != nil {
		committer.Client = r.deps.HTTPClient
	}
	txManager := &handler.ScheduledTransactionManager{
		Aborter: aborter, Metadata: r.deps.MetadataTransactions, ReceiptedDatasets: receipted,
	}
	runCtx, cancel := context.WithCancel(execCtx)
	defer cancel()
	watchStop(stop, cancel)
	audit := &fencedAuditSink{
		store: r.deps.Store, eventID: d.EventID, owner: r.deps.Owner, fence: fence,
		cancel: cancel, log: r.log(),
	}
	outcome, execErr := executor.Execute(runCtx, plan, gatedNodeRunner{runner: r.deps.NodeRunner, gate: func(c context.Context) error {
		if err := r.deps.Store.AssertDispatchFence(c, d.EventID, r.deps.Owner, fence); err != nil {
			return err
		}
		freshGrant, err := r.deps.Store.GetGrantByRef(c, d.GrantRef)
		if err != nil {
			return err
		}
		_, err = r.verifyExecution(c, d, freshGrant)
		return err
	}}, txManager, committer, audit)
	if ctx.Err() != nil {
		// Shutdown/lost lease mid-run: do NOT finish — the dispatch keeps
		// its executing state and recovery re-assesses from persistence.
		return ctx.Err()
	}
	if execErr != nil {
		r.log().Error("scheduled executor error", "event", d.EventID.String(), "err", execErr.Error())
		if len(outcome.Nodes) == 0 {
			// Deterministic plan-level error before any node ran.
			return r.failBuild(ctx, d, fence, truncate(execErr.Error(), 400))
		}
	}
	reason := outcomeReason(outcome)
	return r.finalize(ctx, execCtx, d, fence, reason)
}

// watchStop cancels the run when the worker signals a lost fence (the
// consumer closes stop when the heartbeat goroutine exits).
func watchStop(stop <-chan struct{}, cancel context.CancelFunc) {
	if stop == nil {
		return
	}
	go func() {
		<-stop
		cancel()
	}()
}

// fencedAuditSink aligns node-state writes with the claim fence. It is
// observability, not the lost-fence guard: a stale fence here cancels
// the run context (subsequent nodes abort), while the publish-side hard
// gates live in the fenced receipt writes (A2 checkpoint §2-3).
type fencedAuditSink struct {
	store   RunnerStore
	eventID uuid.UUID
	owner   string
	fence   int64
	cancel  context.CancelFunc
	log     *slog.Logger
}

func (s *fencedAuditSink) Record(ctx context.Context, event executor.AuditEvent) error {
	err := s.store.RecordAuditEventFenced(ctx, s.eventID, s.owner, s.fence, event)
	if err != nil && errors.Is(err, postgres.ErrStaleFence) {
		s.log.Error("fenced audit lost fence; cancelling scheduled run", "event", s.eventID.String())
		if s.cancel != nil {
			s.cancel()
		}
	}
	return err
}

// recoverDanglingResolve handles a crash between the resolver's remote
// transaction opens and the persisted plan: never re-resolve (double
// open), reconcile through the existing pending-txn mechanism instead.
func (r *canonicalRunner) recoverDanglingResolve(ctx context.Context, execCtx context.Context, d postgres.SchedulingDispatch, fence int64, snap *postgres.DispatchExecutionSnapshot) error {
	infos := make([]resolver.OpenedTransactionInfo, 0, len(snap.PendingTxns))
	for _, p := range snap.PendingTxns {
		infos = append(infos, resolver.OpenedTransactionInfo{DatasetRID: p.DatasetRID, TransactionRID: p.TransactionRID, Branch: p.Branch})
	}
	recorder := r.deps.Store.ScheduledBuildState(d.EventID, r.deps.Owner, fence)
	cleanupErrs := resolver.CleanupOpenedTransactions(execCtx, r.deps.Versioning, r.deps.Store.ScheduledBuildState(d.EventID, r.deps.Owner, fence), d.BuildID, d.BuildBranch, infos, recorder)
	if len(cleanupErrs) > 0 {
		return r.finishRecoverable(ctx, d, fence, "resolve_crash_recovered_incomplete: "+errList(cleanupErrs))
	}
	return r.failBuild(ctx, d, fence, "resolve_crash_recovered")
}

// assessExecuted is the P2 path: NO node re-execution. Reconcile
// unconfirmed publishes by stable key + original stored bytes, then map
// to a terminal (or park) from persisted evidence only.
func (r *canonicalRunner) assessExecuted(ctx context.Context, execCtx context.Context, d postgres.SchedulingDispatch, fence int64, snap *postgres.DispatchExecutionSnapshot) error {
	return r.finalize(ctx, execCtx, d, fence, "")
}

// finalize is the shared terminal decision (A2 checkpoint §4): reconcile
// receipts first, then evidence → succeeded / failed / unknown_publish.
func (r *canonicalRunner) finalize(ctx context.Context, execCtx context.Context, d postgres.SchedulingDispatch, fence int64, execReason string) error {
	snap, err := r.deps.Store.LoadDispatchExecutionState(ctx, d.EventID)
	if err != nil {
		return fmt.Errorf("finalize snapshot: %w", err)
	}
	// 1. Reconcile unconfirmed publishes: SAME key, ORIGINAL persisted
	// bytes, no node re-run.
	if len(snap.Receipts) > 0 {
		if rerr := r.reconcileReceipts(ctx, execCtx, d, fence, snap); rerr != nil {
			r.log().Warn("scheduled receipt reconcile incomplete", "event", d.EventID.String(), "err", rerr.Error())
		}
		snap, err = r.deps.Store.LoadDispatchExecutionState(ctx, d.EventID)
		if err != nil {
			return fmt.Errorf("post-reconcile snapshot: %w", err)
		}
	}
	for _, rec := range snap.Receipts {
		if rec.State != "committed" {
			return r.finishRecoverable(ctx, d, fence, "publish_unconfirmed: "+rec.Key)
		}
	}
	if len(snap.Jobs) == 0 || !snap.ExpectedOutputsSealed || !snap.ExpectedNodesSealed {
		return r.finishRecoverable(ctx, d, fence, "plan_not_sealed")
	}
	allCompleted := true
	anyFailed := false
	for _, j := range snap.Jobs {
		switch j.State {
		case "COMPLETED":
		case "FAILED", "ABORTED":
			anyFailed = true
			allCompleted = false
		default:
			allCompleted = false
		}
	}
	if allCompleted {
		if err := r.finishTerminal(ctx, d, fence, postgres.DispatchStateSucceeded, ""); err != nil {
			if errors.Is(err, postgres.ErrFinishIncomplete) {
				// The same-tx completeness check refused: park, never
				// loosen (M2 frozen).
				return r.finishRecoverable(ctx, d, fence, "finish_incomplete")
			}
			return err
		}
		return nil
	}
	if anyFailed {
		// Compensate receipt-less reservations through the existing
		// pending-txn mechanism (abort + release + clear). Committed
		// publishes survive; their locks release below with the rest.
		var infos []resolver.OpenedTransactionInfo
		for _, p := range snap.PendingTxns {
			infos = append(infos, resolver.OpenedTransactionInfo{DatasetRID: p.DatasetRID, TransactionRID: p.TransactionRID, Branch: p.Branch})
		}
		if len(infos) > 0 {
			recorder := r.deps.Store.ScheduledBuildState(d.EventID, r.deps.Owner, fence)
			if cleanupErrs := resolver.CleanupOpenedTransactions(execCtx, r.deps.Versioning, r.deps.Store.ScheduledBuildState(d.EventID, r.deps.Owner, fence), d.BuildID, d.BuildBranch, infos, recorder); len(cleanupErrs) > 0 {
				return r.finishRecoverable(ctx, d, fence, "failure_compensation_incomplete: "+errList(cleanupErrs))
			}
		}
		reason := execReason
		if reason == "" {
			reason = "jobs failed"
		}
		return r.finishTerminal(ctx, d, fence, postgres.DispatchStateFailed, reason)
	}
	// Incomplete (WAITING / stale RUNNING) with no unconfirmed receipts:
	// park — no blind re-run, no fake failure (root trap 2).
	return r.finishRecoverable(ctx, d, fence, "execution_incomplete")
}

// reconcileReceipts replays every pending/unknown receipt with its stored
// bytes under its original key.
func (r *canonicalRunner) reconcileReceipts(ctx context.Context, execCtx context.Context, d postgres.SchedulingDispatch, fence int64, snap *postgres.DispatchExecutionSnapshot) error {
	if r.deps.Cipher == nil {
		return errors.New("scheduler cipher unavailable")
	}
	aborter, _ := r.deps.Versioning.(resolver.TransactionAborter)
	committer := &handler.ScheduledOutputCommitter{
		Store: r.deps.Store, Aborter: aborter, Metadata: r.deps.MetadataCommitter,
		BaseURL: r.deps.DVSBaseURL, EventID: d.EventID, Owner: r.deps.Owner, Fence: fence,
		BuildBranch: d.BuildBranch, Reservations: map[string]string{}, ExistingReceipts: map[string]bool{},
	}
	if r.deps.HTTPClient != nil {
		committer.Client = r.deps.HTTPClient
	}
	reservationByDataset := map[string]string{}
	for _, o := range snap.Outputs {
		reservationByDataset[o.DatasetRID] = o.TransactionRID
	}
	var errs []error
	for _, rec := range snap.Receipts {
		if rec.State == "committed" {
			continue
		}
		if err := r.deps.Store.AssertDispatchFence(ctx, d.EventID, r.deps.Owner, fence); err != nil {
			errs = append(errs, fmt.Errorf("fence: %w", err))
			break
		}
		if err := committer.ReplayReceipt(execCtx, rec.Key, rec.NodeID, rec.DatasetRID, rec.RequestBody, reservationByDataset[rec.DatasetRID]); err != nil {
			errs = append(errs, fmt.Errorf("replay %s: %w", rec.Key, err))
		}
	}
	return errors.Join(errs...)
}

func (r *canonicalRunner) failBuild(ctx context.Context, d postgres.SchedulingDispatch, fence int64, reason string) error {
	return r.finishTerminal(ctx, d, fence, postgres.DispatchStateFailed, reason)
}

// finishTerminal records a CONFIRMED terminal state + the one-shot
// outcome event in the same FinishDispatch transaction.
func (r *canonicalRunner) finishTerminal(ctx context.Context, d postgres.SchedulingDispatch, fence int64, state, reason string) error {
	return r.deps.Store.FinishDispatch(ctx, d.EventID, r.deps.Owner, fence, state, reason, func(c context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(c, `DELETE FROM build_input_locks WHERE build_id=$1`, d.BuildID); err != nil {
			return err
		}
		return outcomeExtra(d, state, reason)(c, tx)
	})
}

// finishRecoverable parks the dispatch (unknown_publish) — no outcome
// event, slot retained, lease cleared for the recovery scan.
func (r *canonicalRunner) finishRecoverable(ctx context.Context, d postgres.SchedulingDispatch, fence int64, reason string) error {
	return r.deps.Store.FinishDispatch(ctx, d.EventID, r.deps.Owner, fence, postgres.DispatchStateUnknownPublish, reason, nil)
}

// outcomeExtra builds the same-transaction outcome enqueue for terminal
// finishes (succeeded/failed here; skipped/blocked variants live in the
// admission hook). Exactly one event per confirmed terminal state.
func outcomeExtra(d postgres.SchedulingDispatch, state, reason string) postgres.ExtraTx {
	return func(ctx context.Context, tx pgx.Tx) error {
		outcomeState := scheduleevents.OutcomeFailed
		if state == postgres.DispatchStateSucceeded {
			outcomeState = scheduleevents.OutcomeSucceeded
		}
		env, err := scheduleevents.BuildOutcomeEnvelope(scheduleevents.OutcomePayload{
			TenantKind:         d.TenantKind,
			TenantID:           d.TenantID.String(),
			ScheduleKey:        d.ScheduleKey,
			DefinitionRevision: d.DefinitionRevision,
			ScheduledFor:       d.ScheduledFor,
			BuildID:            d.BuildID,
			State:              outcomeState,
			Error:              reason,
			TriggerEvent:       d.EventID,
		}, time.Now().UTC(), d.EventID.String(), d.EventID.String())
		if err != nil {
			return fmt.Errorf("outcome envelope: %w", err)
		}
		raw, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshal outcome envelope: %w", err)
		}
		event := outbox.New(env.EventID, scheduleevents.AggregateType, env.AggregateID, scheduleevents.TopicOutcomes, raw)
		return outbox.Enqueue(ctx, tx, event)
	}
}

// outcomeReason summarizes an executor outcome for the dispatch error
// field (bounded, no secrets — reasons are transform/commit errors).
func outcomeReason(outcome executor.Outcome) string {
	if outcome.FinalState == models.BuildCompleted && outcome.Failed == 0 && outcome.Aborted == 0 {
		return ""
	}
	var parts []string
	for _, nodeID := range sortedNodeIDs(outcome.Reasons) {
		if reason := strings.TrimSpace(outcome.Reasons[nodeID]); reason != "" {
			parts = append(parts, nodeID+": "+truncate(reason, 200))
			if len(parts) >= 3 {
				break
			}
		}
	}
	state := string(outcome.FinalState)
	if len(parts) == 0 {
		return state
	}
	return state + " (" + strings.Join(parts, "; ") + ")"
}

func sortedNodeIDs(reasons map[string]string) []string {
	out := make([]string, 0, len(reasons))
	for id := range reasons {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func errList(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		parts = append(parts, truncate(err.Error(), 200))
	}
	return strings.Join(parts, "; ")
}

type gatedNodeRunner struct {
	runner executor.NodeRunner
	gate   func(context.Context) error
}

func (g gatedNodeRunner) Run(ctx context.Context, n executor.NodeContext) (executor.NodeResult, error) {
	if err := g.gate(ctx); err != nil {
		return executor.NodeResult{}, err
	}
	result, err := g.runner.Run(ctx, n)
	if err != nil {
		return result, err
	}
	if err = g.gate(ctx); err != nil {
		return executor.NodeResult{}, err
	}
	return result, nil
}
