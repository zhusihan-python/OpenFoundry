package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/joblifecycle"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// AbortBuildRepository is the persistence port needed to make AbortBuild real.
// Implementations should lock the build/jobs rows while loading the snapshot and
// persist the requested build/job state transitions atomically where possible.
type AbortBuildRepository interface {
	LoadBuildForAbort(ctx context.Context, id string) (*AbortBuildSnapshot, error)
	MarkBuildAborting(ctx context.Context, buildID uuid.UUID, reason string) error
	TransitionJob(ctx context.Context, jobID uuid.UUID, from, to models.JobState, reason string) error
	MarkBuildAborted(ctx context.Context, buildID uuid.UUID, reason string) error
}

type AbortBuildSnapshot struct {
	ID           uuid.UUID          `json:"id"`
	RID          string             `json:"rid"`
	State        models.BuildState  `json:"state"`
	BuildBranch  string             `json:"build_branch"`
	ErrorMessage string             `json:"error_message,omitempty"`
	Jobs         []AbortJobSnapshot `json:"jobs,omitempty"`
}

type AbortJobSnapshot struct {
	ID      uuid.UUID                    `json:"id"`
	State   models.JobState              `json:"state"`
	Outputs []executor.OutputTransaction `json:"outputs,omitempty"`
}

type abortBuildResponse struct {
	RID                 string    `json:"rid,omitempty"`
	BuildID             uuid.UUID `json:"build_id"`
	State               string    `json:"state"`
	CancelledExecution  bool      `json:"cancelled_execution"`
	AbortedTransactions int       `json:"aborted_transactions"`
	TransactionErrors   []string  `json:"transaction_errors,omitempty"`
}

// AbortBuild mirrors the Rust v1 abort flow for build rows and extends it with
// executor cancellation + best-effort open-output transaction aborts.
func AbortBuild(w http.ResponseWriter, r *http.Request) {
	id := buildIDParam(r)
	if strings.TrimSpace(id) == "" {
		writeJSON(w, http.StatusBadRequest, "build id is required")
		return
	}
	// WP3.3 3B opt-in builds abort through the batched guard BEFORE the
	// legacy abort ports are even required (the batched path needs
	// neither the transaction manager nor the committer): the legacy
	// job-outputs sweep below must never touch a build whose commit
	// intent may be durable. Non-batched builds fall through untouched.
	if abortBatchedBuildIfConfigured(w, r, id) {
		return
	}
	ports, abortRepo, ok := requireAbortPorts(w)
	if !ok {
		return
	}
	// Dataset aborts run as the calling user (remote transactions were
	// opened under their identity).
	r = r.WithContext(withCallerBearer(r))

	snapshot, err := abortRepo.LoadBuildForAbort(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if snapshot == nil {
		writeJSON(w, http.StatusNotFound, nil)
		return
	}
	if snapshot.State == models.BuildCompleted {
		w.WriteHeader(http.StatusConflict)
		return
	}
	if snapshot.State == models.BuildFailed {
		// A failed build may still hold dangling remote transactions
		// (compensation could not confirm every abort). The same-tenant
		// retry below cleans exactly those recorded references — no job
		// transitions, the FAILED terminal state stays untouched.
		abortPendingForFailedBuild(w, r, snapshot)
		return
	}

	// Idempotent retries for already-aborting/aborted builds are accepted. We
	// still try to cancel any registered execution and abort any open outputs.
	if snapshot.State != models.BuildAborting && snapshot.State != models.BuildAborted {
		if err := abortRepo.MarkBuildAborting(r.Context(), snapshot.ID, "aborted by user"); err != nil {
			writeJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	cancelled := cancelExecution(snapshot.ID)

	// Remote dataset aborts run FIRST, against the build's REAL branch
	// (never a default): the local aborted markers are only set after the
	// dataset side actually released the transactions. Already-aborted
	// transactions count as success (idempotent retries).
	branch := snapshot.BuildBranch
	if branch == "" {
		branch = "main"
	}
	var txErrors []string
	aborted := 0
	// datasetTransactionAborter carries the CALLER's bearer so remote
	// aborts stay under the aborting user's identity; each call runs on
	// a bounded context that survives request cancellation.
	if aborter := datasetTransactionAborter(r.Context()); aborter != nil {
		for _, job := range snapshot.Jobs {
			for _, output := range job.Outputs {
				if output.TransactionRID == "" {
					continue
				}
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
				err := aborter.AbortTransaction(cleanupCtx, output.DatasetRID, branch, output.TransactionRID)
				cancel()
				if err == nil {
					aborted++
					continue
				}
				// Only a confirmed terminal abort counts: an
				// already-aborted transaction is answered 2xx by the
				// dataset service (natural idempotency). 409 COMMITTED
				// and 404 wrong-parent/invisible must stay errors so the
				// build remains in ABORTING with the lock observable.
				txErrors = append(txErrors, fmt.Sprintf("%s: %v", output.TransactionRID, err))
			}
		}
	}
	if len(txErrors) > 0 {
		writeAbortBuildResponse(w, abortBuildResponse{RID: snapshot.RID, BuildID: snapshot.ID, State: string(models.BuildAborting), CancelledExecution: cancelled, AbortedTransactions: aborted, TransactionErrors: txErrors})
		return
	}

	transitionErrs := transitionAbortJobs(r.Context(), abortRepo, snapshot.Jobs)
	if len(transitionErrs) > 0 {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "abort_transitions_failed", "detail": fmt.Sprintf("%v", transitionErrs), "build_id": snapshot.ID})
		return
	}
	if _, txErrors := abortOpenOutputs(r.Context(), ports.Transactions, snapshot.Jobs); len(txErrors) > 0 {
		writeAbortBuildResponse(w, abortBuildResponse{RID: snapshot.RID, BuildID: snapshot.ID, State: string(models.BuildAborting), CancelledExecution: cancelled, AbortedTransactions: aborted, TransactionErrors: txErrors})
		return
	}
	// All remote aborts confirmed: drop this build's durable pending
	// references before finalizing the state (locks go via
	// MarkBuildAborted's build-scoped delete).
	if clearer, ok := abortRepo.(PendingTxnBuildClearer); ok {
		if err := clearer.ClearPendingTxnsForBuild(r.Context(), snapshot.ID); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "clear_pending_failed", "detail": err.Error(), "build_id": snapshot.ID})
			return
		}
	}
	if err := abortRepo.MarkBuildAborted(r.Context(), snapshot.ID, "aborted by user"); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "mark_build_aborted_failed", "detail": err.Error(), "build_id": snapshot.ID})
		return
	}

	writeAbortBuildResponse(w, abortBuildResponse{RID: snapshot.RID, BuildID: snapshot.ID, State: string(models.BuildAborted), CancelledExecution: cancelled, AbortedTransactions: aborted})
}

func requireAbortPorts(w http.ResponseWriter) (ExecutionPorts, AbortBuildRepository, bool) {
	ports, ok := currentExecutionPorts()
	if !ok || ports.Transactions == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "execution_ports_not_configured", "detail": "AbortBuild requires transaction and abort persistence ports"})
		return ExecutionPorts{}, nil, false
	}
	abortRepo, ok := abortRepositoryFromPorts(ports)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "abort_ports_not_configured", "detail": "AbortBuild requires an AbortBuildRepository"})
		return ExecutionPorts{}, nil, false
	}
	return ports, abortRepo, true
}

func writeAbortBuildResponse(w http.ResponseWriter, response abortBuildResponse) {
	status := http.StatusOK
	if len(response.TransactionErrors) > 0 {
		status = http.StatusBadGateway
	}
	writeJSON(w, status, response)
}

func abortRepositoryFromPorts(ports ExecutionPorts) (AbortBuildRepository, bool) {
	if repo, ok := ports.Plans.(AbortBuildRepository); ok && repo != nil {
		return repo, true
	}
	if repo, ok := ports.Runs.(AbortBuildRepository); ok && repo != nil {
		return repo, true
	}
	return nil, false
}

func buildIDParam(r *http.Request) string {
	for _, key := range []string{"id", "rid", "build_id", "run_id"} {
		if value := chi.URLParam(r, key); value != "" {
			return value
		}
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for i, part := range parts {
		if part == "builds" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func transitionAbortJobs(ctx context.Context, repo AbortBuildRepository, jobs []AbortJobSnapshot) []error {
	var errs []error
	for _, job := range jobs {
		targets := abortTargets(job.State)
		from := job.State
		for _, to := range targets {
			if !joblifecycle.IsValidTransition(from, to) && from != to {
				continue
			}
			if err := repo.TransitionJob(ctx, job.ID, from, to, "build aborted by user"); err != nil {
				errs = append(errs, fmt.Errorf("job %s %s→%s: %w", job.ID, from, to, err))
			}
			from = to
		}
	}
	return errs
}

// datasetTransactionAborter resolves the versioning port's aborter and
// binds the CALLER's bearer to it. The aborter re-attaches that bearer to
// whatever context each call receives — including the bounded
// WithoutCancel cleanup contexts used after request cancellation — so the
// identity survives while the deadline policy comes from the caller.
func datasetTransactionAborter(ctx context.Context) TransactionAborter {
	ports, ok := currentBuildLifecyclePorts()
	if !ok {
		return nil
	}
	aborter, ok := ports.Versioning.(TransactionAborter)
	if !ok {
		return nil
	}
	return &bearerContextAborter{aborter: aborter, bearer: callerBearerFromContext(ctx)}
}

type bearerContextAborter struct {
	aborter TransactionAborter
	bearer  string
}

func (b *bearerContextAborter) AbortTransaction(ctx context.Context, datasetRID, branch, txnRID string) error {
	if b.bearer != "" {
		ctx = contextWithCallerBearer(ctx, b.bearer)
	}
	return b.aborter.AbortTransaction(ctx, datasetRID, branch, txnRID)
}

func abortTargets(state models.JobState) []models.JobState {
	switch state {
	case models.JobRunning, models.JobRunPending:
		return []models.JobState{models.JobAbortPending, models.JobAborted}
	case models.JobWaiting:
		return []models.JobState{models.JobAborted}
	case models.JobAbortPending:
		return []models.JobState{models.JobAborted}
	default:
		return nil
	}
}

func abortOpenOutputs(ctx context.Context, txManager executor.TransactionManager, jobs []AbortJobSnapshot) (int, []string) {
	seen := map[string]struct{}{}
	aborted := 0
	txErrors := []string{}
	for _, job := range jobs {
		for _, output := range job.Outputs {
			key := output.DatasetRID + "\x00" + output.TransactionRID
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if err := txManager.Abort(ctx, output); err != nil {
				txErrors = append(txErrors, fmt.Sprintf("%s: %s", output.DatasetRID, err.Error()))
				continue
			}
			aborted++
		}
	}
	return aborted, txErrors
}

// abortPendingForFailedBuild retries compensation for a FAILED build:
// every durably recorded pending transaction is aborted (confirmed
// terminal only), its reference cleared and the build's own lock for that
// output released. The build state itself is not rewritten.
func abortPendingForFailedBuild(w http.ResponseWriter, r *http.Request, snapshot *AbortBuildSnapshot) {
	aborter := datasetTransactionAborter(r.Context())
	lister, hasLister := func() (AbortBuildRepository, bool) {
		p, ok := currentExecutionPorts()
		if !ok {
			return nil, false
		}
		return abortRepositoryFromPorts(p)
	}()
	pendingLister, _ := lister.(PendingTxnLister)
	releaser, _ := lister.(resolver.LockReleaser)
	recorder, _ := lister.(resolver.PendingTxnRecorder)
	if aborter == nil || !hasLister || pendingLister == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "abort_retry_ports_not_configured", "detail": "pending transaction retry requires the dataset aborter and the pending lister"})
		return
	}
	pending, err := pendingLister.ListPendingTxns(r.Context(), snapshot.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "list_pending_failed", "detail": err.Error()})
		return
	}
	var txErrors []string
	// Untracked handles from a double failure (recording AND abort both
	// failed) live in the persisted failure detail. They are re-recorded
	// durably first; if the database is still unwritable the retry
	// answers 503 manual_cleanup_required — never a fake all-clear.
	byTxn := map[string]PendingOutputTxn{}
	for _, tx := range pending {
		byTxn[tx.TransactionRID] = tx
	}
	for _, handle := range parseManualCleanupHandles(snapshot.ErrorMessage) {
		if _, known := byTxn[handle.TransactionRID]; known {
			continue
		}
		if recorder == nil {
			txErrors = append(txErrors, fmt.Sprintf("manual_cleanup_required dataset=%s branch=%s txn=%s (no recorder to re-track)", handle.DatasetRID, handle.Branch, handle.TransactionRID))
			continue
		}
		if err := recorder.RecordPendingTxn(r.Context(), snapshot.ID, handle.DatasetRID, handle.Branch, handle.TransactionRID); err != nil {
			txErrors = append(txErrors, fmt.Sprintf("manual_cleanup_required dataset=%s branch=%s txn=%s (re-track failed: %v)", handle.DatasetRID, handle.Branch, handle.TransactionRID, err))
			continue
		}
		byTxn[handle.TransactionRID] = handle
		pending = append(pending, handle)
	}
	if len(txErrors) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "manual_cleanup_required", "detail": strings.Join(txErrors, "; "), "build_id": snapshot.ID})
		return
	}
	aborted := 0
	for _, tx := range pending {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
		err := aborter.AbortTransaction(cleanupCtx, tx.DatasetRID, firstNonEmpty(tx.Branch, snapshot.BuildBranch), tx.TransactionRID)
		cancel()
		if err != nil {
			txErrors = append(txErrors, fmt.Sprintf("dataset=%s branch=%s txn=%s: %v", tx.DatasetRID, firstNonEmpty(tx.Branch, snapshot.BuildBranch), tx.TransactionRID, err))
			continue
		}
		aborted++
		// Confirmed ABORTED → release this build's lock first; the
		// pending reference (the only retry线索) clears last and stays
		// on any release failure.
		if releaser != nil {
			if err := releaser.ReleaseLock(r.Context(), tx.DatasetRID, snapshot.ID); err != nil {
				txErrors = append(txErrors, fmt.Sprintf("release %s: %v (reference retained)", tx.DatasetRID, err))
				continue
			}
		}
		if recorder != nil {
			if err := recorder.ClearPendingTxn(r.Context(), snapshot.ID, tx.TransactionRID); err != nil {
				txErrors = append(txErrors, fmt.Sprintf("clear %s: %v", tx.TransactionRID, err))
			}
		}
	}
	if len(txErrors) > 0 {
		writeAbortBuildResponse(w, abortBuildResponse{RID: snapshot.RID, BuildID: snapshot.ID, State: string(models.BuildFailed), AbortedTransactions: aborted, TransactionErrors: txErrors})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"build_id": snapshot.ID, "state": string(models.BuildFailed), "aborted_transactions": aborted, "pending_remaining": 0})
}

// PendingTxnBuildClearer clears all pending associations of a build at
// once after its remote aborts are confirmed.
type PendingTxnBuildClearer interface {
	ClearPendingTxnsForBuild(ctx context.Context, buildID uuid.UUID) error
}

// PendingTxnLister exposes the durable pending-transaction associations.
type PendingTxnLister interface {
	ListPendingTxns(ctx context.Context, buildID uuid.UUID) ([]PendingOutputTxn, error)
}

// parseManualCleanupHandles extracts the machine-readable
// manual_cleanup_required dataset=… branch=… txn=… tokens persisted in a
// build's failure detail.
func parseManualCleanupHandles(detail string) []PendingOutputTxn {
	out := []PendingOutputTxn{}
	for _, part := range strings.Split(detail, ";") {
		part = strings.TrimSpace(part)
		if !strings.HasPrefix(part, "manual_cleanup_required") {
			continue
		}
		handle := PendingOutputTxn{}
		for _, field := range strings.Fields(part) {
			value, ok := strings.CutPrefix(field, "dataset=")
			if ok {
				handle.DatasetRID = value
			}
			if value, ok := strings.CutPrefix(field, "branch="); ok {
				handle.Branch = value
			}
			if value, ok := strings.CutPrefix(field, "txn="); ok {
				handle.TransactionRID = value
			}
		}
		if handle.DatasetRID != "" && handle.TransactionRID != "" {
			out = append(out, handle)
		}
	}
	return out
}
