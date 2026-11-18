package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// ClientError mirrors Rust build_resolution::ClientError.
type ClientError struct{ Message string }

func (e *ClientError) Error() string { return "dataset client error: " + e.Message }

// JobSpecRepository loads pipeline-authoring JobSpecs by declared output.
type JobSpecRepository interface {
	Lookup(ctx context.Context, pipelineRID, outputDatasetRID, buildBranch string, fallbackChain []string) (*models.JobSpec, error)
}

// DatasetVersioningRepository is the resolver-facing dataset-versioning API.
type DatasetVersioningRepository interface {
	ListBranches(ctx context.Context, datasetRID string) ([]models.BranchSnapshot, error)
	OpenTransaction(ctx context.Context, datasetRID, branch string) (string, error)
	ViewSchema(ctx context.Context, datasetRID, branch string) (json.RawMessage, error)
}

// ErrPendingTxnRecordFailed marks a durable-recording failure for an
// opened output transaction (server-side fault after the remote write
// already happened). HTTP callers receive 5xx, never a client 400.
var ErrPendingTxnRecordFailed = errors.New("pending_txn_record_failed")

// PendingTxnRecorder durably associates every just-opened output
// transaction with its build so failure compensation can be retried
// later (the in-memory opened list dies with the request). Recording
// happens BEFORE the lock attempt; rows clear only after a confirmed
// remote abort.
type PendingTxnRecorder interface {
	RecordPendingTxn(ctx context.Context, buildID uuid.UUID, datasetRID, branch, transactionRID string) error
	ClearPendingTxn(ctx context.Context, buildID uuid.UUID, transactionRID string) error
}

// TransactionAborter aborts an output transaction opened earlier in the
// SAME resolution run (the dataset side matches parent dataset and branch).
// Implemented by the HTTP adapter and the co-located repository; when the
// port does not implement it, failure cleanup degrades to best effort.
type TransactionAborter interface {
	AbortTransaction(ctx context.Context, datasetRID, branch, transactionRID string) error
}

// BuildAwareTransactionOpener opens an output transaction whose providence
// carries the build-bound reservation marker. WP3.3 3B opt-in builds use it
// (DatasetHTTPVersioning implements it with
// providence {origin, purpose, build_id}); every other resolution keeps the
// plain OpenTransaction call. A purpose-carrying resolve whose port does not
// implement this interface fails closed before any transaction is opened.
type BuildAwareTransactionOpener interface {
	OpenTransactionForBuild(ctx context.Context, datasetRID, branch string, buildID uuid.UUID, purpose string) (string, error)
}

// LockReleaser releases a build lock acquired earlier in the SAME run for
// (outputDatasetRID, buildID). Implementations must not touch locks held
// by other builds.
type LockReleaser interface {
	ReleaseLock(ctx context.Context, outputDatasetRID string, buildID uuid.UUID) error
}

// CleanupOpenedTransactions aborts the given transactions and releases the
// locks this run acquired. It runs on a bounded context that survives
// caller cancellation/timeouts, and returns any secondary failures so the
// caller can surface them instead of silently losing cleanup state. Locks
// and transactions belonging to other builds or tenants are unreachable
// by construction (build-scoped lock release, parent+branch+tenant
// guarded aborts).
func CleanupOpenedTransactions(ctx context.Context, versioning DatasetVersioningRepository, locks BranchLockRepository, buildID uuid.UUID, buildBranch string, opened []OpenedTransactionInfo, pendingClearer ...PendingTxnRecorder) []error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	aborter, _ := versioning.(TransactionAborter)
	releaser, _ := locks.(LockReleaser)
	var errs []error
	for _, tx := range opened {
		if aborter != nil {
			if err := aborter.AbortTransaction(cleanupCtx, tx.DatasetRID, branchFor(tx, buildBranch), tx.TransactionRID); err != nil {
				// The remote transaction is NOT confirmed terminated:
				// keep this build's lock row AND its pending reference so
				// the dangling OPEN transaction stays observable and
				// retryable, and cannot be silently replaced by a new
				// build.
				errs = append(errs, fmt.Errorf("abort %s: %w (lock+reference retained)", tx.TransactionRID, err))
				continue
			}
		}
		// Confirmed remote ABORTED → release THIS build's lock → only
		// then clear the durable pending reference. The reference is the
		// only retry线索: a failed release keeps it (and the row stays
		// for the next retry, which re-releases idempotently).
		if releaser != nil {
			if err := releaser.ReleaseLock(cleanupCtx, tx.DatasetRID, buildID); err != nil {
				errs = append(errs, fmt.Errorf("release lock %s: %w (reference retained)", tx.DatasetRID, err))
				continue
			}
		}
		if len(pendingClearer) > 0 && pendingClearer[0] != nil {
			if err := pendingClearer[0].ClearPendingTxn(cleanupCtx, buildID, tx.TransactionRID); err != nil {
				errs = append(errs, fmt.Errorf("clear pending %s: %w", tx.TransactionRID, err))
			}
		}
	}
	return errs
}

// OpenedTransactionInfo is the cleanup view of an opened transaction.
type OpenedTransactionInfo struct {
	DatasetRID     string
	TransactionRID string
	Branch         string
}

func branchFor(tx OpenedTransactionInfo, fallback string) string {
	if tx.Branch != "" {
		return tx.Branch
	}
	return fallback
}

// clientOpenAbort aborts a just-opened transaction when its durable
// recording failed, so no untracked remote transaction is left behind.
func clientOpenAbort(ctx context.Context, versioning DatasetVersioningRepository, datasetRID, branch, txnRID string) error {
	aborter, ok := versioning.(TransactionAborter)
	if !ok {
		return nil
	}
	return aborter.AbortTransaction(ctx, datasetRID, branch, txnRID)
}

// BranchLockRepository stores output-dataset build locks. ok=false means the
// lock is already held by holderBuildID; the repository is the injected lock primitive.
type BranchLockRepository interface {
	TryAcquire(ctx context.Context, outputDatasetRID string, buildID uuid.UUID, transactionRID string) (holderBuildID uuid.UUID, ok bool, err error)
	HasUpstreamInProgress(ctx context.Context, inputDatasetRIDs []string, selfBuildID uuid.UUID) (bool, error)
}

// ResolveBuildArgs mirrors Rust build_resolution::ResolveBuildArgs.
type ResolveBuildArgs struct {
	// BuildID lets HTTP/repo adapters open the build record before resolution,
	// matching the Rust lifecycle transaction. When nil, ResolveBuild creates one.
	BuildID           *uuid.UUID
	PipelineRID       string
	BuildBranch       string
	JobSpecFallback   []string
	OutputDatasetRIDs []string
	ForceBuild        bool
	RequestedBy       string
	TriggerKind       string
	AbortPolicy       string
	// ReservationPurpose, when non-empty, opens output transactions through
	// BuildAwareTransactionOpener so the remote providence carries the
	// build-bound marker (WP3.3 3B opt-in). Empty keeps the plain
	// OpenTransaction call — legacy resolution is untouched. A non-empty
	// purpose against a port without the opener fails closed before any
	// transaction is opened.
	ReservationPurpose string
	// SpecFreezer, when non-nil, is invoked with the specs ResolveBuild
	// ITSELF loaded (its internal PreflightBuild re-runs the spec load),
	// right after preflight and before any lock or transaction exists. An
	// error fails the whole resolution with zero side effects. WP3.3 3B
	// uses it to prove the freshly loaded graph still equals the spec the
	// CreateBuild preflight froze the batched configuration from.
	SpecFreezer func([]models.JobSpec) error
}

// BranchNotFoundError marks an output dataset whose branch listing is
// visible but does not contain the requested build branch.
type BranchNotFoundError struct {
	DatasetRID string
	Branch     string
}

func (e *BranchNotFoundError) Error() string {
	return fmt.Sprintf("branch %q not found for output dataset %s", e.Branch, e.DatasetRID)
}

// MissingJobSpecError matches Rust BuildResolutionError::MissingJobSpec.
type MissingJobSpecError struct {
	DatasetRID string
	Tried      []string
}

func (e *MissingJobSpecError) Error() string {
	return fmt.Sprintf("missing JobSpec for output dataset %s (tried branches: %s)", e.DatasetRID, formatQuotedList(e.Tried))
}

// CycleDetectedError matches Rust BuildResolutionError::CycleDetected.
type CycleDetectedError struct{ CyclePath []string }

func (e *CycleDetectedError) Error() string {
	return "cycle detected in JobSpec graph: " + strings.Join(e.CyclePath, " → ")
}

// InputNotFoundError matches Rust BuildResolutionError::InputNotFound.
type InputNotFoundError struct{ DatasetRID string }

func (e *InputNotFoundError) Error() string { return "input dataset " + e.DatasetRID + " not found" }

// InputBranchMissingError matches Rust BuildResolutionError::InputBranchMissing.
type InputBranchMissingError struct {
	DatasetRID  string
	BuildBranch string
	Chain       []string
}

func (e *InputBranchMissingError) Error() string {
	return fmt.Sprintf("input dataset %s has no branch matching build='%s' (chain: %s)", e.DatasetRID, e.BuildBranch, formatQuotedList(e.Chain))
}

// StaleInputError matches Rust BuildResolutionError::StaleInput.
type StaleInputError struct{ DatasetRID string }

func (e *StaleInputError) Error() string {
	return "input " + e.DatasetRID + " resolved to fallback branch but require_fresh=true"
}

// LockHeldError matches Rust BuildResolutionError::LockHeld.
type LockHeldError struct {
	DatasetRID    string
	HolderBuildID uuid.UUID
}

func (e *LockHeldError) Error() string {
	return fmt.Sprintf("output dataset %s already locked by build %s", e.DatasetRID, e.HolderBuildID)
}

// ClientResolutionError matches Rust BuildResolutionError::Client.
type ClientResolutionError struct{ Message string }

func (e *ClientResolutionError) Error() string {
	return "dataset versioning client error: " + e.Message
}

// InvalidLogicKindError matches Rust BuildResolutionError::InvalidLogicKind.
type InvalidLogicKindError struct {
	JobSpecRID string
	Reason     string
}

func (e *InvalidLogicKindError) Error() string {
	return fmt.Sprintf("invalid logic_kind on JobSpec %s: %s", e.JobSpecRID, e.Reason)
}

// ViewFilterResolutionError matches Rust BuildResolutionError::ViewFilterResolution.
type ViewFilterResolutionError struct {
	JobSpecRID string
	Errors     []string
}

func (e *ViewFilterResolutionError) Error() string {
	return fmt.Sprintf("view filter resolution failed for JobSpec %s: %s", e.JobSpecRID, formatQuotedList(e.Errors))
}

// ResolveBuild drives the resolver without HTTP or a DAG executor.
// PreflightBuild runs every read-only resolution step (spec loading,
// logic validation, cycle detection, input-view validation and output
// dataset visibility via the branch listing). Handlers call it BEFORE any
// write (build row, remote transaction, local lock) so deterministic
// errors reject the request with zero side effects; ResolveBuild reuses
// it internally.
func PreflightBuild(ctx context.Context, args ResolveBuildArgs, jobSpecs JobSpecRepository, versioning DatasetVersioningRepository) ([]models.JobSpec, []models.ResolvedInputView, error) {
	specs, err := LoadJobSpecs(ctx, args.PipelineRID, args.BuildBranch, args.JobSpecFallback, args.OutputDatasetRIDs, jobSpecs)
	if err != nil {
		return nil, nil, err
	}
	for _, spec := range specs {
		if reason := ValidateLogicKind(spec.LogicKind, len(spec.OutputDatasetRIDs)); reason != "" {
			return nil, nil, &InvalidLogicKindError{JobSpecRID: spec.RID, Reason: reason}
		}
	}
	if err := DetectCycles(specs); err != nil {
		return nil, nil, err
	}
	inputViews, err := ValidateInputs(ctx, args.BuildBranch, specs, versioning)
	if err != nil {
		return nil, nil, err
	}
	// Output datasets (including outputs the specs declare beyond the
	// request's own list) must be visible through the tenant-checked
	// branch listing before anything is written.
	seen := map[string]struct{}{}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			if _, dup := seen[output]; dup {
				continue
			}
			seen[output] = struct{}{}
			branches, err := versioning.ListBranches(ctx, output)
			if err != nil {
				return nil, nil, mapClientErr(err)
			}
			found := false
			for _, branch := range branches {
				if branch.Name == args.BuildBranch {
					found = true
					break
				}
			}
			if !found {
				return nil, nil, &BranchNotFoundError{DatasetRID: output, Branch: args.BuildBranch}
			}
		}
	}
	return specs, inputViews, nil
}

func ResolveBuild(ctx context.Context, args ResolveBuildArgs, jobSpecs JobSpecRepository, versioning DatasetVersioningRepository, locks BranchLockRepository, recorder ...PendingTxnRecorder) (*models.ResolvedBuild, error) {
	started := time.Now().UTC()
	if args.TriggerKind == "" {
		args.TriggerKind = "MANUAL"
	}
	if args.AbortPolicy == "" {
		args.AbortPolicy = string(models.AbortDependentOnly)
	}

	specs, inputViews, err := PreflightBuild(ctx, args, jobSpecs, versioning)
	if err != nil {
		return nil, err
	}
	// The freeze hook sees the graph THIS resolution will consume — never
	// a first preflight stitched onto a different freshly loaded graph —
	// and runs before any lock or transaction exists, so a mismatch fails
	// closed with zero side effects.
	if args.SpecFreezer != nil {
		if err := args.SpecFreezer(specs); err != nil {
			return nil, err
		}
	}

	var buildID uuid.UUID
	if args.BuildID != nil {
		buildID = *args.BuildID
	} else {
		var err error
		buildID, err = uuid.NewV7()
		if err != nil {
			buildID = uuid.New()
		}
	}
	if locks != nil {
		inputRIDs := externalInputDatasetRIDs(specs)
		upstream, err := locks.HasUpstreamInProgress(ctx, inputRIDs, buildID)
		if err != nil {
			return nil, mapClientErr(err)
		}
		if upstream {
			reason := "upstream build in progress"
			return &models.ResolvedBuild{BuildID: buildID, State: models.BuildQueued, JobSpecs: specs, InputViews: inputViews, QueuedReason: &reason, ResolvedAt: started}, nil
		}
	}

	opened, err := AcquireLocks(ctx, buildID, specs, args.BuildBranch, args.ReservationPurpose, versioning, locks, recorder...)
	if err != nil {
		return nil, err
	}
	jobs, stages := BuildFanOutPlan(buildID, specs, opened)
	return &models.ResolvedBuild{BuildID: buildID, State: models.BuildResolution, JobSpecs: specs, InputViews: inputViews, OpenedTransactions: opened, Jobs: jobs, FanOutStages: stages, ResolvedAt: started}, nil
}

// LoadJobSpecs performs Rust step a, including deterministic RID de-duplication.
func LoadJobSpecs(ctx context.Context, pipelineRID, buildBranch string, fallbackChain, outputDatasetRIDs []string, repo JobSpecRepository) ([]models.JobSpec, error) {
	specs := make([]models.JobSpec, 0, len(outputDatasetRIDs))
	for _, output := range outputDatasetRIDs {
		spec, err := repo.Lookup(ctx, pipelineRID, output, buildBranch, cloneStrings(fallbackChain))
		if err != nil {
			return nil, mapClientErr(err)
		}
		if spec == nil {
			tried := append([]string{buildBranch}, fallbackChain...)
			return nil, &MissingJobSpecError{DatasetRID: output, Tried: tried}
		}
		specs = append(specs, *spec)
	}
	sort.SliceStable(specs, func(i, j int) bool { return specs[i].RID < specs[j].RID })
	out := specs[:0]
	var last string
	for i, spec := range specs {
		if i == 0 || spec.RID != last {
			out = append(out, spec)
			last = spec.RID
		}
	}
	return out, nil
}

// DetectCycles ports the Rust Kahn scan and representative cycle path recovery.
func DetectCycles(specs []models.JobSpec) error {
	producer := map[string]string{}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			producer[output] = spec.RID
		}
	}
	graph := map[string][]string{}
	indegree := map[string]int{}
	for _, spec := range specs {
		graph[spec.RID] = graph[spec.RID]
		indegree[spec.RID] = indegree[spec.RID]
	}
	for _, spec := range specs {
		for _, input := range spec.Inputs {
			upstream, ok := producer[input.DatasetRID]
			if !ok {
				continue
			}
			if upstream == spec.RID {
				return &CycleDetectedError{CyclePath: []string{spec.RID, spec.RID}}
			}
			graph[spec.RID] = append(graph[spec.RID], upstream)
			indegree[upstream]++
		}
	}
	zero := make([]string, 0)
	for node, degree := range indegree {
		if degree == 0 {
			zero = append(zero, node)
		}
	}
	sort.Strings(zero)
	popped := 0
	for len(zero) > 0 {
		node := zero[0]
		zero = zero[1:]
		popped++
		neighbours := append([]string(nil), graph[node]...)
		sort.Strings(neighbours)
		for _, next := range neighbours {
			if indegree[next] > 0 {
				indegree[next]--
			}
			if indegree[next] == 0 {
				zero = append(zero, next)
				sort.Strings(zero)
			}
		}
	}
	if popped == len(specs) {
		return nil
	}
	if cycle := findCyclePath(graph, indegree); len(cycle) > 0 {
		return &CycleDetectedError{CyclePath: cycle}
	}
	fallback := make([]string, len(specs))
	for i, spec := range specs {
		fallback[i] = spec.RID
	}
	return &CycleDetectedError{CyclePath: fallback}
}

// ValidateInputs ports Rust step c and skips inputs produced inside this build.
func ValidateInputs(ctx context.Context, buildBranch string, specs []models.JobSpec, client DatasetVersioningRepository) ([]models.ResolvedInputView, error) {
	producerOutputs := outputSet(specs)
	seen := map[string]struct{}{}
	resolved := make([]models.ResolvedInputView, 0)
	for _, spec := range specs {
		for _, input := range spec.Inputs {
			if _, internal := producerOutputs[input.DatasetRID]; internal {
				continue
			}
			if _, ok := seen[input.DatasetRID]; ok {
				continue
			}
			seen[input.DatasetRID] = struct{}{}
			snapshots, err := client.ListBranches(ctx, input.DatasetRID)
			if err != nil {
				return nil, mapClientErr(err)
			}
			if len(snapshots) == 0 {
				return nil, &InputNotFoundError{DatasetRID: input.DatasetRID}
			}
			branches := make([]string, 0, len(snapshots))
			for _, snapshot := range snapshots {
				branches = append(branches, snapshot.Name)
			}
			outcome, err := ResolveInputDataset(buildBranch, input.FallbackChain, branches)
			if err != nil {
				var noMatch *NoMatchError
				var ancestry *IncompatibleAncestryError
				if errors.As(err, &noMatch) || errors.As(err, &ancestry) {
					return nil, &InputBranchMissingError{DatasetRID: input.DatasetRID, BuildBranch: buildBranch, Chain: cloneStrings(input.FallbackChain)}
				}
				return nil, err
			}
			if input.RequireFresh && outcome.FallbackIndex > 0 {
				return nil, &StaleInputError{DatasetRID: input.DatasetRID}
			}
			schema, err := client.ViewSchema(ctx, input.DatasetRID, outcome.Branch)
			if err != nil {
				return nil, mapClientErr(err)
			}
			if len(schema) == 0 {
				schema = json.RawMessage(`null`)
			}
			resolved = append(resolved, models.ResolvedInputView{DatasetRID: input.DatasetRID, Branch: outcome.Branch, Schema: schema})
		}
	}
	return resolved, nil
}

// AcquireLocks opens one transaction per output and delegates lock persistence.
// reservationPurpose routes the open through BuildAwareTransactionOpener
// (build-bound providence marker) when non-empty; the port must then
// implement it or the resolution fails closed before any transaction.
func AcquireLocks(ctx context.Context, buildID uuid.UUID, specs []models.JobSpec, buildBranch, reservationPurpose string, client DatasetVersioningRepository, locks BranchLockRepository, recorder ...PendingTxnRecorder) ([]models.OpenedTransaction, error) {
	opened := make([]models.OpenedTransaction, 0)
	seenOutputs := map[string]struct{}{}
	fail := func(err error) ([]models.OpenedTransaction, error) {
		// Resolution failed mid-way: abort the transactions THIS run
		// opened (including the one whose recording or lock attempt just
		// failed) and release the locks THIS run acquired. Cleanup
		// failures are appended to the original error, never dropped.
		infos := make([]OpenedTransactionInfo, 0, len(opened))
		for _, tx := range opened {
			infos = append(infos, OpenedTransactionInfo{DatasetRID: tx.DatasetRID, TransactionRID: tx.TransactionRID, Branch: buildBranch})
		}
		if cleanupErrs := CleanupOpenedTransactions(ctx, client, locks, buildID, buildBranch, infos, recorder...); len(cleanupErrs) > 0 {
			return nil, fmt.Errorf("%w (cleanup failures: %v)", err, cleanupErrs)
		}
		return nil, err
	}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			if _, seen := seenOutputs[output]; seen {
				continue
			}
			seenOutputs[output] = struct{}{}
			var txnRID string
			var err error
			if reservationPurpose != "" {
				opener, openerOK := client.(BuildAwareTransactionOpener)
				if !openerOK {
					// Fail closed before anything is opened: the opt-in
					// reservation protocol requires the build-aware opener.
					return fail(&ClientResolutionError{Message: "batched_reservation_opener_unavailable: the versioning port cannot open build-bound reservation transactions"})
				}
				txnRID, err = opener.OpenTransactionForBuild(ctx, output, buildBranch, buildID, reservationPurpose)
			} else {
				txnRID, err = client.OpenTransaction(ctx, output, buildBranch)
			}
			if err != nil {
				return fail(mapClientErr(err))
			}
			// Persist the association BEFORE attempting the lock: if
			// anything after this point fails (lock conflict, later
			// output, cancellation), the transaction stays retryably
			// attributable to THIS build. A recording write failure
			// aborts the transaction immediately and surfaces — never a
			// silent untracked transaction.
			if len(recorder) > 0 && recorder[0] != nil {
				if err := recorder[0].RecordPendingTxn(ctx, buildID, output, buildBranch, txnRID); err != nil {
					// The recording write failed (often just request
					// cancellation). Retry once on a bounded detached
					// context so the transaction stays tracked; if that
					// also fails, include THIS transaction in the
					// unified compensation (everything opened so far) —
					// never a silent untracked remote transaction. If
					// the compensating abort fails too, the error (with
					// the transaction RID) lands on the build's failure
					// message for a manual retry.
					recordCtx, cancelRecord := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
					recordErr := recorder[0].RecordPendingTxn(recordCtx, buildID, output, buildBranch, txnRID)
					cancelRecord()
					if recordErr == nil {
						// Recording eventually succeeded on the retry:
						// the normal flow below tracks it exactly once.
					} else {
						// Both record attempts failed. The remote abort
						// is attempted via fail(); if THAT also fails
						// the handle must stay recoverable, so a final
						// record try runs on a fresh bounded context and
						// the failure text carries the full
						// dataset/branch/transaction handle plus a
						// machine-readable manual_cleanup_required token.
						opened = append(opened, models.OpenedTransaction{DatasetRID: output, TransactionRID: txnRID})
						finalErr := fmt.Errorf("pending txn recording failed for %s: %w (detached retry: %v): %w", txnRID, err, recordErr, ErrPendingTxnRecordFailed)
						if aborter, ok := client.(TransactionAborter); ok {
							abortCtx, cancelAbort := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
							abortErr := aborter.AbortTransaction(abortCtx, output, buildBranch, txnRID)
							cancelAbort()
							if abortErr != nil {
								lastCtx, cancelLast := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
								lastRecordErr := recorder[0].RecordPendingTxn(lastCtx, buildID, output, buildBranch, txnRID)
								cancelLast()
								if lastRecordErr != nil {
									return fail(fmt.Errorf("%w; manual_cleanup_required dataset=%s branch=%s txn=%s (durable recording unavailable: %v)", finalErr, output, buildBranch, txnRID, lastRecordErr))
								}
								// Recorded at the last attempt: tracked —
								// fail() retries the abort and retains
								// the reference when it still fails.
							}
						}
						return fail(finalErr)
					}
				}
			}
			// Record the open transaction BEFORE attempting the lock:
			// a lock conflict or error must still abort this remote
			// transaction, not leak it.
			opened = append(opened, models.OpenedTransaction{DatasetRID: output, TransactionRID: txnRID})
			if locks != nil {
				holder, ok, err := locks.TryAcquire(ctx, output, buildID, txnRID)
				if err != nil {
					return fail(mapClientErr(err))
				}
				if !ok {
					return fail(&LockHeldError{DatasetRID: output, HolderBuildID: holder})
				}
			}
		}
	}
	return opened, nil
}

// BuildFanOutPlan returns resolver-created jobs and topological execution stages.
func BuildFanOutPlan(buildID uuid.UUID, specs []models.JobSpec, opened []models.OpenedTransaction) ([]models.ResolvedJob, [][]string) {
	producer := map[string]string{}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			producer[output] = spec.RID
		}
	}
	openedByDataset := map[string]string{}
	for _, txn := range opened {
		openedByDataset[txn.DatasetRID] = txn.TransactionRID
	}
	jobs := make([]models.ResolvedJob, 0, len(specs))
	dependencies := map[string][]string{}
	for _, spec := range specs {
		depSet := map[string]struct{}{}
		for _, input := range spec.Inputs {
			if upstream, ok := producer[input.DatasetRID]; ok && upstream != spec.RID {
				depSet[upstream] = struct{}{}
			}
		}
		deps := setKeys(depSet)
		dependencies[spec.RID] = deps
		txnRIDs := make([]string, 0, len(spec.OutputDatasetRIDs))
		for _, output := range spec.OutputDatasetRIDs {
			if txn, ok := openedByDataset[output]; ok {
				txnRIDs = append(txnRIDs, txn)
			}
		}
		jobID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(buildID.String()+":"+spec.RID))
		jobs = append(jobs, models.ResolvedJob{ID: jobID, JobSpecRID: spec.RID, OutputTransactionRIDs: txnRIDs, DependsOnJobSpecRIDs: deps})
	}
	return jobs, fanOutStages(dependencies)
}

// ValidateLogicKind mirrors the resolver-time runner arity check subset.
func ValidateLogicKind(kind string, outputCount int) string {
	switch strings.ToUpper(kind) {
	case "SYNC", "TRANSFORM", "HEALTH_CHECK", "ANALYTICAL", "EXPORT":
		if outputCount == 0 {
			return "at least one output dataset is required"
		}
		return ""
	default:
		return "unsupported logic_kind " + kind
	}
}

func findCyclePath(graph map[string][]string, indegree map[string]int) []string {
	starts := make([]string, 0)
	for node, degree := range indegree {
		if degree > 0 {
			starts = append(starts, node)
		}
	}
	sort.Strings(starts)
	visited := map[string]struct{}{}
	stack := []string{}
	var dfs func(string) []string
	dfs = func(node string) []string {
		if pos := indexOf(stack, node); pos >= 0 {
			cycle := append([]string(nil), stack[pos:]...)
			cycle = append(cycle, node)
			return cycle
		}
		if _, ok := visited[node]; ok {
			return nil
		}
		visited[node] = struct{}{}
		stack = append(stack, node)
		neighbours := append([]string(nil), graph[node]...)
		sort.Strings(neighbours)
		for _, next := range neighbours {
			if cycle := dfs(next); len(cycle) > 0 {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		return nil
	}
	for _, start := range starts {
		if cycle := dfs(start); len(cycle) > 0 {
			return cycle
		}
	}
	return nil
}

func fanOutStages(dependencies map[string][]string) [][]string {
	remaining := map[string]struct{}{}
	for rid := range dependencies {
		remaining[rid] = struct{}{}
	}
	completed := map[string]struct{}{}
	stages := [][]string{}
	for len(remaining) > 0 {
		stage := make([]string, 0)
		for rid := range remaining {
			ready := true
			for _, dep := range dependencies[rid] {
				if _, ok := completed[dep]; !ok {
					ready = false
					break
				}
			}
			if ready {
				stage = append(stage, rid)
			}
		}
		sort.Strings(stage)
		if len(stage) == 0 {
			break
		}
		stages = append(stages, stage)
		for _, rid := range stage {
			delete(remaining, rid)
			completed[rid] = struct{}{}
		}
	}
	return stages
}

func externalInputDatasetRIDs(specs []models.JobSpec) []string {
	outputs := outputSet(specs)
	inputSet := map[string]struct{}{}
	for _, spec := range specs {
		for _, input := range spec.Inputs {
			if _, internal := outputs[input.DatasetRID]; !internal {
				inputSet[input.DatasetRID] = struct{}{}
			}
		}
	}
	return setKeys(inputSet)
}

func outputSet(specs []models.JobSpec) map[string]struct{} {
	out := map[string]struct{}{}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			out[output] = struct{}{}
		}
	}
	return out
}

func setKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func mapClientErr(err error) error {
	if err == nil {
		return nil
	}
	var client *ClientError
	if errors.As(err, &client) {
		return &ClientResolutionError{Message: client.Message}
	}
	return err
}

func formatQuotedList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = fmt.Sprintf("%q", item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
