package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// BuildRepository is the minimal persistence adapter needed by the HTTP build
// surface. Production wiring should persist builds/jobs/transactions in the
// Rust-compatible tables; tests inject fakes instead of silently stubbing IO.
type BuildRepository interface {
	OpenBuild(ctx context.Context, args resolver.ResolveBuildArgs, buildID uuid.UUID) error
	PersistResolvedBuild(ctx context.Context, resolved *models.ResolvedBuild) error
	MarkBuildFailed(ctx context.Context, buildID uuid.UUID, reason string) error
}

type BuildLifecyclePorts struct {
	JobSpecs   resolver.JobSpecRepository
	Versioning resolver.DatasetVersioningRepository
	Locks      resolver.BranchLockRepository
	Builds     BuildRepository
}

type buildLifecycleSlot struct{ ports BuildLifecyclePorts }

// buildPipelineLoader is implemented by stores that can load a pipeline's
// graph. The production postgres repository always can; narrower fakes keep
// the legacy behavior and rely on the runManualBuild gate.
type buildPipelineLoader interface {
	LoadPipeline(ctx context.Context, id uuid.UUID) (*models.Pipeline, error)
}

// ontologyOutputNodesOfBuildPipeline reports the object/link output nodes a
// build's parent pipeline graph declares. A nil/empty answer means "no
// ontology targets visible from the build path".
func ontologyOutputNodesOfBuildPipeline(ctx context.Context, store any, pipelineRID string) []string {
	loader, ok := store.(buildPipelineLoader)
	if !ok || loader == nil {
		return nil
	}
	raw := strings.TrimSpace(pipelineRID)
	raw = strings.TrimPrefix(raw, "ri.foundry.main.pipeline.")
	id, err := uuid.Parse(raw)
	if err != nil {
		return nil
	}
	pipeline, err := loader.LoadPipeline(ctx, id)
	if err != nil || pipeline == nil {
		return nil
	}
	ir, err := pipeline.RuntimeIR()
	if err != nil {
		return nil
	}
	return ontologyOutputIRNodes(ir)
}

func ontologyOutputIRNodes(ir models.PipelineIR) []string {
	var out []string
	for _, node := range ir.Nodes {
		if kind := outputKindForIRNode(node, parseOutputDatasetConfig(node.Config)); kind == "object_type" || kind == "link_type" {
			out = append(out, node.ID)
		}
	}
	return out
}

var buildLifecyclePorts atomic.Value // stores *buildLifecycleSlot

// SetBuildLifecyclePorts injects resolver and persistence ports for CreateBuild
// and DryRunResolve. It returns a restore function so tests remain isolated.
func SetBuildLifecyclePorts(ports BuildLifecyclePorts) func() {
	previous, _ := buildLifecyclePorts.Load().(*buildLifecycleSlot)
	buildLifecyclePorts.Store(&buildLifecycleSlot{ports: ports})
	return func() { buildLifecyclePorts.Store(previous) }
}

// pipelineParentValidator is implemented by the authoring repository so
// scoped build requests can prove the pipeline parent belongs to the
// caller's tenant before anything is written.
type pipelineParentValidator interface {
	GetPipeline(ctx context.Context, id uuid.UUID) (*models.Pipeline, error)
}

// validatePipelineParent rejects scoped callers whose pipeline RID does
// not resolve to a tenant-visible pipeline. Unscoped (legacy internal)
// callers keep the old behaviour.
func validatePipelineParent(ctx context.Context, ports BuildLifecyclePorts, pipelineRID string) error {
	if TenantScopeFromContext(ctx) == nil {
		return nil
	}
	// Scoped callers must PROVE ownership: a missing or non-validating
	// authoring repository is a fail-closed 503, never a silent bypass.
	authoring, ok := currentPipelineAuthoringRepository()
	if !ok || authoring == nil {
		return ErrPipelineParentValidationUnavailable
	}
	validator, isValidator := authoring.(pipelineParentValidator)
	if !isValidator {
		return ErrPipelineParentValidationUnavailable
	}
	raw := strings.TrimSpace(pipelineRID)
	raw = strings.TrimPrefix(raw, "ri.foundry.main.pipeline.")
	id, err := uuid.Parse(raw)
	if err != nil {
		// Malformed reference: the caller's request is invalid.
		return &resolver.ClientResolutionError{Message: fmt.Sprintf("pipeline_rid %q is not a valid pipeline reference", pipelineRID)}
	}
	pipeline, err := validator.GetPipeline(ctx, id)
	if err != nil {
		return err
	}
	if pipeline == nil {
		// Well-formed but invisible (foreign tenant or gone): 404, the
		// isolation answer — never a generic 400.
		return ErrPipelineParentNotFound
	}
	return nil
}

// writeResolutionHTTPError maps resolution failures to precise statuses:
// dataset 404/403/409 pass through, upstream unavailability is 503, the
// capability gate is its own 503, everything else stays a client 400.
func writeResolutionHTTPError(w http.ResponseWriter, err error) {
	var httpErr *DatasetHTTPError
	switch {
	case errors.Is(err, ErrDatasetTenantBoundaryUnsupported):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "dataset_tenant_boundary_unsupported", "detail": err.Error()})
	case errors.Is(err, ErrPipelineParentNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline_not_found_in_tenant", "detail": err.Error()})
	case errors.Is(err, ErrPipelineParentValidationUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "pipeline_parent_validation_unavailable", "detail": err.Error()})
	case errors.Is(err, resolver.ErrPendingTxnRecordFailed):
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "pending_txn_record_failed", "detail": err.Error()})
	case errors.Is(err, ErrDatasetCallerRequired):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "dataset_caller_required", "detail": err.Error()})
	case errors.As(err, &httpErr):
		status := httpErr.Status
		if httpErr.Status >= http.StatusInternalServerError {
			status = http.StatusServiceUnavailable // upstream 5xx → dependency unavailable, not "unsupported"
		} // every 4xx (404/403/409/400 invalid references …) passes through as-is
		code := httpErr.Code
		if code == "" {
			code = "dataset_error"
		}
		writeJSON(w, status, map[string]string{"error": code, "detail": httpErr.Detail})
	default:
		writeResolutionError(w, err)
	}
}

// preflightRequestedDatasetVisibility proves every referenced dataset is
// visible to the CALLER before spec loading: a foreign requested output
// must answer with the real 403/404, never a misleading missing-spec 400
// or a schema-miss. Scoped callers only — legacy unscoped harnesses keep
// the old contract.
func preflightRequestedDatasetVisibility(ctx context.Context, ports BuildLifecyclePorts, rids ...string) error {
	if TenantScopeFromContext(ctx) == nil {
		return nil
	}
	seen := map[string]struct{}{}
	for _, rid := range rids {
		rid = strings.TrimSpace(rid)
		if rid == "" {
			continue
		}
		if _, dup := seen[rid]; dup {
			continue
		}
		seen[rid] = struct{}{}
		if _, err := ports.Versioning.ListBranches(ctx, rid); err != nil {
			return err
		}
	}
	return nil
}

// datasetRIDSetOf returns the distinct external input datasets plus all
// outputs the specs declare (beyond the request's own list), for the
// post-spec visibility gate.
func datasetRIDSetOf(specs []models.JobSpec, extra ...string) []string {
	seen := map[string]struct{}{}
	add := func(rid string) {
		if rid = strings.TrimSpace(rid); rid != "" {
			seen[rid] = struct{}{}
		}
	}
	for _, rid := range extra {
		add(rid)
	}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			add(output)
		}
		for _, input := range spec.Inputs {
			add(input.DatasetRID)
		}
	}
	out := make([]string, 0, len(seen))
	for rid := range seen {
		out = append(out, rid)
	}
	return out
}

// withCallerBearer attaches the request's bearer so the dataset HTTP
// adapter (and any downstream service call) acts as the calling user.
func withCallerBearer(r *http.Request) context.Context {
	return contextWithCallerBearer(r.Context(), callerBearerFromRequest(r))
}

// PendingOutputTxn is one durable build↔transaction association awaiting
// a confirmed remote abort.
type PendingOutputTxn struct {
	DatasetRID     string `json:"dataset_rid"`
	Branch         string `json:"branch"`
	TransactionRID string `json:"transaction_rid"`
}

// ErrTransactionConflict marks an abort attempt against a transaction in
// a non-abortable terminal state (e.g. COMMITTED). HTTP callers get 409.
var ErrTransactionConflict = fmt.Errorf("transaction_conflict: transaction is in a terminal state that cannot be aborted")

// ErrPipelineParentNotFound marks a well-formed pipeline reference that
// does not resolve under the caller's tenant (foreign or deleted) —
// mapped to 404.
var ErrPipelineParentNotFound = fmt.Errorf("pipeline_not_found_in_tenant: pipeline is not visible in the caller's tenant")

// ErrPipelineParentValidationUnavailable marks a scoped request whose
// pipeline parent cannot be proven (authoring repository missing or does
// not support validation). Scoped callers fail closed with 503.
var ErrPipelineParentValidationUnavailable = fmt.Errorf("pipeline_parent_validation_unavailable: pipeline ownership cannot be verified in this deployment")

// ErrTransactionNotFound marks an abort that matched no transaction row
// under the given parent dataset / branch / tenant.
var ErrTransactionNotFound = fmt.Errorf("transaction_not_found: no open transaction matches the parent dataset, branch and tenant")

// ErrDatasetTenantBoundaryUnsupported marks a scoped build-resolution
// request that cannot be served safely because the reachable dataset
// schema does not expose tenant columns (split database / older schema).
// HTTP callers receive a stable 503 with error code
// "dataset_tenant_boundary_unsupported".
var ErrDatasetTenantBoundaryUnsupported = errors.New("dataset_tenant_boundary_unsupported: the dataset schema does not expose tenant columns")

// datasetBoundaryProber is implemented by the production versioning
// repository so build resolution can reject scoped requests BEFORE any
// database write when the co-located dataset schema cannot enforce the
// tenant boundary (split-database deployments, older dataset schema).
type datasetBoundaryProber interface {
	DatasetTenantBoundarySupported(ctx context.Context) bool
}

// requireDatasetBoundarySupported writes the stable 503 used by the
// acceptance harness when a scoped build-resolution request cannot be
// served safely. Ports without the prober (test fakes, future HTTP
// adapters) are treated as boundary-capable.
func requireDatasetBoundarySupported(w http.ResponseWriter, r *http.Request, ports BuildLifecyclePorts) bool {
	if TenantScopeFromContext(r.Context()) == nil {
		return true
	}
	prober, ok := ports.Versioning.(datasetBoundaryProber)
	if !ok {
		return true
	}
	if prober.DatasetTenantBoundarySupported(r.Context()) {
		return true
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{
		"error":  "dataset_tenant_boundary_unsupported",
		"detail": "the dataset schema reachable by this deployment does not expose tenant columns; route build resolution through a co-located dataset schema with org columns or the dataset service HTTP adapter",
	})
	return false
}

func currentBuildLifecyclePorts() (BuildLifecyclePorts, bool) {
	slot, _ := buildLifecyclePorts.Load().(*buildLifecycleSlot)
	if slot == nil || slot.ports.JobSpecs == nil || slot.ports.Versioning == nil {
		return BuildLifecyclePorts{}, false
	}
	return slot.ports, true
}

func requireBuildLifecyclePorts(w http.ResponseWriter, detail string, needBuilds bool) (BuildLifecyclePorts, bool) {
	ports, ok := currentBuildLifecyclePorts()
	if !ok || (needBuilds && ports.Builds == nil) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "build_lifecycle_ports_not_configured", "detail": detail})
		return BuildLifecyclePorts{}, false
	}
	return ports, true
}

// CreateBuild resolves a Rust-compatible build plan, persists the opened build
// and job plan through injected ports, and returns the same accepted envelope as
// the Rust v1 handler.
func CreateBuild(w http.ResponseWriter, r *http.Request) {
	ports, ok := requireBuildLifecyclePorts(w, "CreateBuild requires JobSpec, dataset versioning and build persistence ports", true)
	if !ok {
		return
	}

	var body models.CreateBuildRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_json", "detail": err.Error()})
		return
	}
	args, err := createBuildArgs(r.Context(), body)
	if err != nil {
		if errors.Is(err, ErrDatasetTenantBoundaryUnsupported) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "dataset_tenant_boundary_unsupported", "detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if !requireDatasetBoundarySupported(w, r, ports) {
		return
	}
	ctx := withCallerBearer(r)
	if err := validatePipelineParent(ctx, ports, body.PipelineRID); err != nil {
		writeResolutionHTTPError(w, err)
		return
	}
	// Requested outputs are proven visible BEFORE spec loading: a
	// foreign output is a real permission answer (403/404), not a
	// missing-spec 400.
	if err := preflightRequestedDatasetVisibility(ctx, ports, body.OutputDatasetRIDs...); err != nil {
		writeResolutionHTTPError(w, err)
		return
	}
	// The build path publishes DATASETS only. A pipeline whose graph
	// declares object/link outputs must not resolve into a build that
	// would later report COMPLETED with the ontology side silently
	// missing — reject before the build row, any reservation or any
	// remote transaction exists. Graph-declared ontology delivery runs
	// through POST /pipelines/{id}/runs, which wires the ontology
	// committer. Stores that cannot load the pipeline graph keep the
	// legacy behavior (the spec grammar itself is dataset-only); the
	// execute-time gate in runManualBuild still catches those builds.
	if nodes := ontologyOutputNodesOfBuildPipeline(ctx, ports.Builds, body.PipelineRID); len(nodes) > 0 {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error":  "manual_build_ontology_output_unsupported",
			"detail": fmt.Sprintf("pipeline declares object/link output nodes (%s); the build path publishes datasets only — trigger the graph run path (POST /api/v1/pipelines/{id}/runs) for ontology outputs", strings.Join(nodes, ", ")),
		})
		return
	}
	// Read-only preflight (specs, cycles, inputs, output visibility)
	// rejects deterministic errors BEFORE the build row, any remote
	// transaction or any local lock exists.
	specs, _, err := resolver.PreflightBuild(ctx, args, ports.JobSpecs, ports.Versioning)
	if err != nil {
		writeResolutionHTTPError(w, err)
		return
	}
	// WP3.3 3B opt-in detection runs on the REAL preflight specs: the
	// frozen batched configuration, the build-bound reservation protocol
	// and the active-branch proof all gate BEFORE the build row exists.
	batchedCfg, err := prepareBatchedCreateBuild(ctx, ports, body, &args, specs)
	if err != nil {
		var specErr *batchedSpecError
		if errors.As(err, &specErr) {
			writeJSON(w, specErr.Status, map[string]string{"error": specErr.Code, "detail": specErr.Detail})
			return
		}
		writeResolutionHTTPError(w, err)
		return
	}
	stagedGroup, err := stagedManualGroupConfig(specs)
	if err != nil || (stagedGroup != nil && (batchedCfg != nil || (args.TriggerKind != "MANUAL" && args.TriggerKind != "FORCE"))) {
		if err == nil {
			err = errors.New("staged output groups require a manual build")
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "staged_group_config_invalid", "detail": err.Error()})
		return
	}
	var frozenStagedGroup json.RawMessage
	if stagedGroup != nil {
		switch ports.Versioning.(type) {
		case DatasetHTTPVersioning, *DatasetHTTPVersioning:
		default:
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "staged_group_versioning_unavailable"})
			return
		}
		frozenStagedGroup, err = json.Marshal(stagedGroup)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "staged_group_config_invalid"})
			return
		}
		args.ReservationPurpose = batchedReservationPurpose
		args.SpecFreezer = func(fresh []models.JobSpec) error {
			group, err := stagedManualGroupConfig(fresh)
			if err != nil {
				return err
			}
			raw, err := json.Marshal(group)
			if err != nil || string(raw) != string(frozenStagedGroup) {
				return errors.New("staged group JobSpecs changed during resolution")
			}
			return nil
		}
	}
	if batchedCfg != nil {
		frozen := batchedCfg
		args.SpecFreezer = func(fresh []models.JobSpec) error { return batchedSpecsEquivalent(frozen, fresh) }
	} else if stagedGroup == nil && (args.TriggerKind == "MANUAL" || args.TriggerKind == "FORCE") {
		switch ports.Versioning.(type) {
		case DatasetHTTPVersioning, *DatasetHTTPVersioning:
			args.ReservationPurpose = manualOutputPurpose
		}
	}
	buildID, err := uuid.NewV7()
	if err != nil {
		buildID = uuid.New()
	}
	args.BuildID = &buildID

	// detachedWrite gives EACH fate-finalizing write its own bounded,
	// cancellation-proof context: a single budget started before OpenBuild
	// must not expire under a long resolution and starve later writes.
	// Tenant/bearer values survive WithoutCancel.
	detachedWrite := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	}

	openCtx, cancelOpen := detachedWrite()
	defer cancelOpen()
	if batchedCfg != nil {
		// The build row and the opt-in marker land in ONE transaction:
		// from the moment the build exists, execute/abort route on the
		// batched configuration — there is no window that could fall
		// back into the legacy empty-plan path.
		preparer, preparerOK := ports.Builds.(batchedPrepareStore)
		if !preparerOK {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "batched_persistence_unavailable", "detail": "batched builds require the batched prepare store"})
			return
		}
		batchedCfg.BuildID = buildID
		if err := preparer.OpenBatchedPrepare(openCtx, args, buildID, batchedCfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "build_open_failed", "detail": err.Error()})
			return
		}
	} else if err := ports.Builds.OpenBuild(openCtx, args, buildID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "build_open_failed", "detail": err.Error()})
		return
	}
	var recorder resolver.PendingTxnRecorder
	if candidate, ok := ports.Builds.(resolver.PendingTxnRecorder); ok {
		recorder = candidate
	} else if candidate, ok := ports.Versioning.(resolver.PendingTxnRecorder); ok {
		recorder = candidate
	}
	var resolverRecorder []resolver.PendingTxnRecorder
	if recorder != nil {
		resolverRecorder = append(resolverRecorder, recorder)
	}

	resolved, err := resolver.ResolveBuild(ctx, args, ports.JobSpecs, ports.Versioning, ports.Locks, resolverRecorder...)
	if err != nil {
		failCtx, cancelFail := detachedWrite()
		defer cancelFail()
		if failErr := ports.Builds.MarkBuildFailed(failCtx, buildID, err.Error()); failErr != nil {
			slog.Error("mark build failed", slog.String("build_id", buildID.String()), slog.String("error", failErr.Error()))
		}
		writeResolutionHTTPError(w, err)
		return
	}
	persistCtx, cancelPersist := detachedWrite()
	defer cancelPersist()
	if batchedCfg != nil {
		// The frozen configuration rides the resolved build into the SAME
		// persistence transaction (PREPARING → PENDING with the opened
		// reservation RID); execute reads exactly this row.
		resolved.BatchConfig = batchedCfg
	}
	resolved.ManualStagedGroup = frozenStagedGroup
	if err := ports.Builds.PersistResolvedBuild(persistCtx, resolved); err != nil {
		// Persistence failed after transactions were opened: compensate
		// (abort this run's transactions, release this build's locks)
		// on a bounded detached context, then record the failure.
		infos := make([]resolver.OpenedTransactionInfo, 0, len(resolved.OpenedTransactions))
		for _, tx := range resolved.OpenedTransactions {
			infos = append(infos, resolver.OpenedTransactionInfo{DatasetRID: tx.DatasetRID, TransactionRID: tx.TransactionRID, Branch: args.BuildBranch})
		}
		cleanupErrs := resolver.CleanupOpenedTransactions(persistCtx, ports.Versioning, ports.Locks, buildID, args.BuildBranch, infos, resolverRecorder...)
		detail := err.Error()
		if len(cleanupErrs) > 0 {
			detail = fmt.Sprintf("%s (cleanup failures: %v)", detail, cleanupErrs)
		}
		// Fresh bounded context for the failure mark (persistCtx's
		// budget may be exhausted), and the FULL reason — including
		// compensation failures — is what gets persisted.
		failCtx, cancelFail := detachedWrite()
		defer cancelFail()
		if failErr := ports.Builds.MarkBuildFailed(failCtx, buildID, detail); failErr != nil {
			slog.Error("mark build failed", slog.String("build_id", buildID.String()), slog.String("error", failErr.Error()))
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "build_persist_failed", "detail": detail})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"build_id":            resolved.BuildID,
		"state":               string(resolved.State),
		"queued_reason":       resolved.QueuedReason,
		"job_count":           len(resolved.JobSpecs),
		"output_transactions": resolved.OpenedTransactions,
	})
}

type dryRunResolveRequest struct {
	PipelineRID       string           `json:"pipeline_rid"`
	BuildBranch       string           `json:"build_branch"`
	JobSpecFallback   []string         `json:"job_spec_fallback,omitempty"`
	OutputDatasetRIDs []string         `json:"output_dataset_rids"`
	InlineSpecs       []models.JobSpec `json:"inline_specs,omitempty"`
}

type dryRunResolveResponse struct {
	Jobs   []dryRunJob   `json:"jobs"`
	Errors []dryRunError `json:"errors"`
}

type dryRunJob struct {
	JobSpecRID            string         `json:"job_spec_rid"`
	ResolvedJobSpecBranch string         `json:"resolved_jobspec_branch"`
	OutputDatasetRIDs     []string       `json:"output_dataset_rids"`
	ResolvedOutputs       []dryRunOutput `json:"resolved_outputs"`
	ResolvedInputs        []dryRunInput  `json:"resolved_inputs"`
	DependsOnJobSpecRIDs  []string       `json:"depends_on_job_spec_rids,omitempty"`
	OutputTransactionRIDs []string       `json:"output_transaction_rids,omitempty"`
}

type dryRunInput struct {
	DatasetRID          string   `json:"dataset_rid"`
	ResolvedInputBranch *string  `json:"resolved_input_branch"`
	FallbackIndex       *int     `json:"fallback_index"`
	FallbackChain       []string `json:"fallback_chain"`
}

type dryRunOutput struct {
	DatasetRID     string  `json:"dataset_rid"`
	ResolvedOutput string  `json:"resolved_output"`
	CreatesBranch  bool    `json:"creates_branch"`
	FromBranch     *string `json:"from_branch"`
}

type dryRunError struct {
	DatasetRID *string `json:"dataset_rid"`
	Kind       string  `json:"kind"`
	Message    string  `json:"message"`
}

// DryRunResolve uses the resolver's load/validate/branch-resolution steps but
// intentionally avoids OpenBuild, OpenTransaction, lock acquisition and final persistence.
func DryRunResolve(w http.ResponseWriter, r *http.Request) {
	ports, ok := requireBuildLifecyclePorts(w, "DryRunResolve requires JobSpec and dataset versioning ports", false)
	if !ok {
		return
	}
	var body dryRunResolveRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_json", "detail": err.Error()})
		return
	}
	for _, key := range []string{"id", "pipeline_rid"} {
		if rid := chi.URLParam(r, key); body.PipelineRID == "" && rid != "" {
			body.PipelineRID = rid
		}
	}
	if err := validateResolveRequest(body.PipelineRID, body.BuildBranch, body.OutputDatasetRIDs); err != nil {
		writeJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if !requireDatasetBoundarySupported(w, r, ports) {
		return
	}
	ctx := withCallerBearer(r)
	if err := validatePipelineParent(ctx, ports, body.PipelineRID); err != nil {
		writeResolutionHTTPError(w, err)
		return
	}

	// Requested outputs prove visibility before spec loading (real
	// 403/404 for foreign outputs, not a missing-spec contract break).
	if err := preflightRequestedDatasetVisibility(ctx, ports, body.OutputDatasetRIDs...); err != nil {
		writeResolutionHTTPError(w, err)
		return
	}
	jobRepo := ports.JobSpecs
	if len(body.InlineSpecs) > 0 {
		// Inline specs carry their own pipeline references: every one
		// must prove its parent before resolution proceeds.
		for i := range body.InlineSpecs {
			if ref := strings.TrimSpace(body.InlineSpecs[i].PipelineRID); ref != "" {
				if err := validatePipelineParent(ctx, ports, ref); err != nil {
					writeResolutionHTTPError(w, err)
					return
				}
			}
		}
		jobRepo = newInlineJobSpecRepo(body.InlineSpecs)
	}
	specs, err := resolver.LoadJobSpecs(ctx, body.PipelineRID, body.BuildBranch, body.JobSpecFallback, body.OutputDatasetRIDs, jobRepo)
	if err != nil {
		writeJSON(w, http.StatusOK, dryRunResolveResponse{Errors: []dryRunError{dryRunErrorFromErr(err)}})
		return
	}
	if err := resolver.DetectCycles(specs); err != nil {
		writeJSON(w, http.StatusOK, dryRunResolveResponse{Errors: []dryRunError{dryRunErrorFromErr(err)}})
		return
	}
	// With specs in hand, EVERY referenced dataset (inputs and spec
	// outputs beyond the request list) passes the same caller-visibility
	// gate BEFORE branch resolution: permission failures are 403/404
	// HTTP answers, while ordinary branch/logic resolution issues keep
	// the 200+errors contract below.
	if err := preflightRequestedDatasetVisibility(ctx, ports, datasetRIDSetOf(specs, body.OutputDatasetRIDs...)...); err != nil {
		writeResolutionHTTPError(w, err)
		return
	}

	jobs := make([]dryRunJob, 0, len(specs))
	dryErrors := make([]dryRunError, 0)
	planJobs, _ := resolver.BuildFanOutPlan(uuid.Nil, specs, nil)
	depsBySpec := map[string][]string{}
	for _, job := range planJobs {
		depsBySpec[job.JobSpecRID] = job.DependsOnJobSpecRIDs
	}
	for _, spec := range specs {
		inputs := make([]dryRunInput, 0, len(spec.Inputs))
		for _, input := range spec.Inputs {
			resolvedInput := dryRunInput{DatasetRID: input.DatasetRID, FallbackChain: input.FallbackChain}
			branches, err := ports.Versioning.ListBranches(ctx, input.DatasetRID)
			if err != nil {
				dryErrors = append(dryErrors, dryRunError{DatasetRID: strPtr(input.DatasetRID), Kind: "VERSIONING_CLIENT_ERROR", Message: err.Error()})
				inputs = append(inputs, resolvedInput)
				continue
			}
			branchNames := branchNames(branches)
			outcome, err := resolver.ResolveInputDataset(body.BuildBranch, input.FallbackChain, branchNames)
			if err != nil {
				dryErrors = append(dryErrors, dryRunError{DatasetRID: strPtr(input.DatasetRID), Kind: "INPUT_NOT_RESOLVABLE", Message: err.Error()})
				inputs = append(inputs, resolvedInput)
				continue
			}
			resolvedInput.ResolvedInputBranch = strPtr(outcome.Branch)
			resolvedInput.FallbackIndex = intPtr(outcome.FallbackIndex)
			inputs = append(inputs, resolvedInput)
		}

		outputs := make([]dryRunOutput, 0, len(spec.OutputDatasetRIDs))
		for _, outputRID := range spec.OutputDatasetRIDs {
			resolvedOutput := dryRunOutput{DatasetRID: outputRID, ResolvedOutput: body.BuildBranch}
			branches, err := ports.Versioning.ListBranches(ctx, outputRID)
			if err != nil {
				dryErrors = append(dryErrors, dryRunError{DatasetRID: strPtr(outputRID), Kind: "VERSIONING_CLIENT_ERROR", Message: err.Error()})
				outputs = append(outputs, resolvedOutput)
				continue
			}
			outcome, err := resolver.ResolveOutputDataset(body.BuildBranch, body.JobSpecFallback, branchNames(branches))
			if err != nil {
				dryErrors = append(dryErrors, dryRunError{DatasetRID: strPtr(outputRID), Kind: "OUTPUT_NOT_RESOLVABLE", Message: err.Error()})
				outputs = append(outputs, resolvedOutput)
				continue
			}
			if outcome.Kind == resolver.ResolvedOutputCreateFrom {
				resolvedOutput.ResolvedOutput = outcome.NewBranch
				resolvedOutput.CreatesBranch = true
				resolvedOutput.FromBranch = strPtr(outcome.From)
			} else {
				resolvedOutput.ResolvedOutput = outcome.Branch
			}
			outputs = append(outputs, resolvedOutput)
		}
		jobs = append(jobs, dryRunJob{JobSpecRID: spec.RID, ResolvedJobSpecBranch: spec.BranchName, OutputDatasetRIDs: spec.OutputDatasetRIDs, ResolvedOutputs: outputs, ResolvedInputs: inputs, DependsOnJobSpecRIDs: depsBySpec[spec.RID]})
	}
	writeJSON(w, http.StatusOK, dryRunResolveResponse{Jobs: jobs, Errors: dryErrors})
}

func createBuildArgs(ctx context.Context, body models.CreateBuildRequest) (resolver.ResolveBuildArgs, error) {
	if err := validateResolveRequest(body.PipelineRID, body.BuildBranch, body.OutputDatasetRIDs); err != nil {
		return resolver.ResolveBuildArgs{}, err
	}
	triggerKind := "MANUAL"
	if body.TriggerKind != nil && strings.TrimSpace(*body.TriggerKind) != "" {
		triggerKind = strings.ToUpper(strings.TrimSpace(*body.TriggerKind))
	}
	abortPolicy := string(models.AbortDependentOnly)
	if body.AbortPolicy != nil {
		abortPolicy = string(*body.AbortPolicy)
	}
	requestedBy := ""
	if user, ok := authmw.AuthUserFromContext(ctx); ok && user.Claims != nil {
		requestedBy = user.Claims.Sub.String()
	}
	return resolver.ResolveBuildArgs{PipelineRID: body.PipelineRID, BuildBranch: body.BuildBranch, JobSpecFallback: body.JobSpecFallback, OutputDatasetRIDs: body.OutputDatasetRIDs, ForceBuild: body.ForceBuild, RequestedBy: requestedBy, TriggerKind: triggerKind, AbortPolicy: abortPolicy}, nil
}

func validateResolveRequest(pipelineRID, buildBranch string, outputs []string) error {
	if strings.TrimSpace(pipelineRID) == "" {
		return errors.New("pipeline_rid is required")
	}
	if strings.TrimSpace(buildBranch) == "" {
		return errors.New("build_branch is required")
	}
	if len(outputs) == 0 {
		return errors.New("output_dataset_rids must declare at least one dataset")
	}
	return nil
}

func writeResolutionError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, err.Error())
}

func dryRunErrorFromErr(err error) dryRunError {
	var missing *resolver.MissingJobSpecError
	var cycle *resolver.CycleDetectedError
	var inputMissing *resolver.InputBranchMissingError
	var notFound *resolver.InputNotFoundError
	switch {
	case errors.As(err, &missing):
		return dryRunError{DatasetRID: strPtr(missing.DatasetRID), Kind: "MISSING_JOB_SPEC", Message: err.Error()}
	case errors.As(err, &cycle):
		return dryRunError{Kind: "CYCLE_DETECTED", Message: err.Error()}
	case errors.As(err, &inputMissing):
		return dryRunError{DatasetRID: strPtr(inputMissing.DatasetRID), Kind: "INPUT_NOT_RESOLVABLE", Message: err.Error()}
	case errors.As(err, &notFound):
		return dryRunError{DatasetRID: strPtr(notFound.DatasetRID), Kind: "INPUT_NOT_FOUND", Message: err.Error()}
	default:
		return dryRunError{Kind: "RESOLUTION_ERROR", Message: err.Error()}
	}
}

type inlineJobSpecRepo struct{ specs map[string]models.JobSpec }

func newInlineJobSpecRepo(specs []models.JobSpec) *inlineJobSpecRepo {
	repo := &inlineJobSpecRepo{specs: map[string]models.JobSpec{}}
	for _, spec := range specs {
		for _, output := range spec.OutputDatasetRIDs {
			repo.specs[output] = spec
		}
	}
	return repo
}

func (r *inlineJobSpecRepo) Lookup(_ context.Context, _, outputDatasetRID, _ string, _ []string) (*models.JobSpec, error) {
	spec, ok := r.specs[outputDatasetRID]
	if !ok {
		return nil, nil
	}
	return &spec, nil
}

func branchNames(branches []models.BranchSnapshot) []string {
	out := make([]string, 0, len(branches))
	for _, branch := range branches {
		out = append(out, branch.Name)
	}
	return out
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }
