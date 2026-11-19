package handler

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// WP3.3 3B — the opt-in batched build: one canonical TRANSFORM JobSpec
// (logic_payload.transform_type == "batched_dataset_copy"), one explicit
// pinned input (branch == build branch, positive version, path or
// all_files, optional eq predicate), one output. The frozen
// configuration is persisted when the build row opens; execute runs a
// closed manual orchestration (scan → stage → intent → exact-byte keyed
// commit → atomic finalize) instead of the generic executor, and abort
// is refused the moment the commit intent is durable.
//
// Identity rules: every dataset call carries the current caller's JWT;
// the acting subject must equal the recorded actor; nothing about the
// JWT is persisted.

const (
	// batchedTransformType is the entire closed grammar v1.
	batchedTransformType        = "batched_dataset_copy"
	batchedJSONFlattenTransform = "batched_json_flatten"
	// batchedReservationPurpose rides the reservation providence.
	batchedReservationPurpose = "wp33-staged-output"
	// Logical/output path bounds mirror the dataset scan cursor path cap.
	batchedPathMaxLen         = 512
	batchedPredicateMaxLen    = 256
	batchedDefaultLogicalPath = "part-00000.ndjson"
	batchedDefaultLeaseTTL    = 30 * time.Minute
	batchedAbortLeaseTTL      = 15 * time.Minute
)

// Batched store sentinels: defined here (not in postgres) so the HTTP
// layer maps them without an import cycle — the production repository
// returns them from its batched_build.go.
var (
	// ErrBatchedLeaseHeld: another attempt holds a live lease.
	ErrBatchedLeaseHeld = errors.New("batched_build_lease_held: another execution attempt holds the lease")
	// ErrBatchedPreparing: resolution has not persisted yet.
	ErrBatchedPreparing = errors.New("batched_build_preparing: build resolution is still in progress")
	// ErrBatchedQueued: the build is queued behind an upstream build.
	ErrBatchedQueued = errors.New("batched_build_queued: the build is queued behind an upstream build")
	// ErrBatchedTerminal: the build reached a terminal stage (execute refuses).
	ErrBatchedTerminal = errors.New("batched_build_terminal: the batched build already reached a terminal stage")
	// ErrBatchedPublishing: the commit intent is persisted; abort is refused.
	ErrBatchedPublishing = errors.New("batched_publish_outcome_unknown: a commit intent exists; resolve it by exact-byte replay, abort is refused")
	// ErrBatchedFenceLost: the caller no longer owns the lease fence.
	ErrBatchedFenceLost = errors.New("batched_fence_lost: the execution lease was taken over; cleanup belongs to the new owner")
	// ErrBatchedNotFound: no batched configuration for the build.
	ErrBatchedNotFound = errors.New("batched_build_not_found: no batched configuration exists for this build")
)

// batchedSpecError is a deterministic grammar rejection (HTTP status +
// stable code); it is produced only BEFORE any build row exists.
type batchedSpecError struct {
	Status int
	Code   string
	Detail string
}

func (e *batchedSpecError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func batchedSpecErrorf(status int, code, format string, args ...any) *batchedSpecError {
	return &batchedSpecError{Status: status, Code: code, Detail: fmt.Sprintf(format, args...)}
}

// validBatchedObjectPath accepts canonical logical object keys only:
// non-empty, bounded, valid UTF-8 without NUL, no dot segments, no
// empty segments, no edge slashes (the dataset service's scan/stage
// path validators accept exactly this class).
func validBatchedObjectPath(p string) bool {
	if p == "" || len(p) > batchedPathMaxLen {
		return false
	}
	if strings.ContainsRune(p, 0) || !utf8.ValidString(p) {
		return false
	}
	if path.Clean(p) != p {
		return false
	}
	if strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// batchedPayload is the strict shape of logic_payload for the grammar.
type batchedPayload struct {
	TransformType string               `json:"transform_type"`
	Input         batchedPayloadInput  `json:"input"`
	Output        batchedPayloadOutput `json:"output"`
}

type batchedPayloadInput struct {
	Version   int    `json:"version"`
	Branch    string `json:"branch"`
	Path      string `json:"path"`
	AllFiles  bool   `json:"all_files"`
	Predicate string `json:"predicate"`
}

type batchedPayloadOutput struct {
	LogicalPath   string `json:"logical_path"`
	Format        string `json:"format,omitempty"`
	SchemaVersion string `json:"schema_version,omitempty"`
}

// detectBatchedFamily reports whether any loaded spec declares the
// batched transform type — including in shapes that will then be
// REJECTED (multi-spec etc.), so a half-declared opt-in can never
// silently fall back to the legacy runtime.
func detectBatchedFamily(specs []models.JobSpec) bool {
	for _, spec := range specs {
		if len(spec.LogicPayload) == 0 {
			continue
		}
		// Decode ONLY the transform_type discriminant: a payload that
		// declares the batched type but carries a malformed body (e.g. a
		// string input.version) must be REJECTED by the full parse below,
		// not silently re-routed to the legacy runtime because the whole
		// strict unmarshal failed.
		var probe struct {
			TransformType string `json:"transform_type"`
		}
		if json.Unmarshal(spec.LogicPayload, &probe) != nil {
			continue
		}
		if probe.TransformType == batchedTransformType || probe.TransformType == batchedJSONFlattenTransform {
			return true
		}
	}
	return false
}

// parseBatchedSpec validates the closed grammar against the REAL
// preflight specs of one CreateBuild request and freezes the persisted
// configuration. Returns (nil, nil) when the request is not batched at
// all; every other outcome is either a frozen config or a rejection
// that must surface before the build row is written.
func parseBatchedSpec(specs []models.JobSpec, body models.CreateBuildRequest, args resolver.ResolveBuildArgs, actor uuid.UUID) (*models.BatchedBuildConfig, error) {
	if !detectBatchedFamily(specs) {
		return nil, nil
	}
	bad := func(format string, args ...any) *batchedSpecError {
		return batchedSpecErrorf(http.StatusBadRequest, "batched_spec_invalid", format, args...)
	}
	if len(specs) != 1 {
		return nil, bad("batched builds require exactly one JobSpec, got %d", len(specs))
	}
	spec := specs[0]
	if strings.ToUpper(spec.LogicKind) != "TRANSFORM" {
		return nil, bad("batched builds require logic_kind TRANSFORM, got %q", spec.LogicKind)
	}
	if args.TriggerKind != "MANUAL" {
		return nil, bad("batched builds are manual-only, trigger_kind %q is not supported", args.TriggerKind)
	}
	if args.ForceBuild {
		return nil, bad("force_build has no batched semantics and is rejected for batched builds")
	}
	if len(body.OutputDatasetRIDs) != 1 {
		return nil, bad("batched builds declare exactly one output_dataset_rid, got %d", len(body.OutputDatasetRIDs))
	}
	if len(spec.OutputDatasetRIDs) != 1 {
		return nil, bad("the batched JobSpec must declare exactly one output, got %d", len(spec.OutputDatasetRIDs))
	}
	outputRID, err := canonicalDatasetRID(spec.OutputDatasetRIDs[0])
	if err != nil {
		return nil, bad("output dataset reference invalid: %v", err)
	}
	requestedRID, err := canonicalDatasetRID(body.OutputDatasetRIDs[0])
	if err != nil {
		return nil, bad("output_dataset_rids entry invalid: %v", err)
	}
	if outputRID != requestedRID {
		return nil, bad("requested output %s does not match the JobSpec output %s", requestedRID, outputRID)
	}
	if len(spec.Inputs) != 1 {
		return nil, bad("batched builds require exactly one input, got %d", len(spec.Inputs))
	}
	input := spec.Inputs[0]
	if len(input.FallbackChain) > 0 {
		return nil, bad("batched inputs must not declare fallback_chain")
	}
	if len(input.ViewFilter) > 0 {
		return nil, bad("batched inputs must not declare view_filter")
	}
	if input.RequireFresh {
		return nil, bad("batched inputs must not declare require_fresh")
	}
	inputRID, err := canonicalDatasetRID(input.DatasetRID)
	if err != nil {
		return nil, bad("input dataset reference invalid: %v", err)
	}
	if inputRID == outputRID {
		return nil, bad("input and output datasets must differ, both are %s", inputRID)
	}

	var payload batchedPayload
	if err := json.Unmarshal(spec.LogicPayload, &payload); err != nil {
		return nil, bad("logic_payload is not the batched object: %v", err)
	}
	if payload.TransformType != batchedTransformType && payload.TransformType != batchedJSONFlattenTransform {
		return nil, bad("unsupported batched transform_type %q", payload.TransformType)
	}
	if payload.Input.Version < 1 {
		return nil, bad("input.version must be a positive integer, got %d", payload.Input.Version)
	}
	if payload.Input.Version > 1<<31-1 {
		return nil, bad("input.version %d exceeds the version range", payload.Input.Version)
	}
	// v1 scope: the standard resolver binds a no-fallback input to the
	// build branch, so the pinned input branch must equal it EXACTLY —
	// anything else would freeze a plan the resolver never validated.
	if strings.TrimSpace(payload.Input.Branch) == "" {
		return nil, bad("input.branch is required")
	}
	if payload.Input.Branch != args.BuildBranch {
		return nil, bad("input.branch %q must equal the build branch %q (explicit cross-branch input resolution is not supported in v1)", payload.Input.Branch, args.BuildBranch)
	}
	if payload.Input.Path == "" && !payload.Input.AllFiles {
		return nil, bad("input must pin exactly one of path or all_files=true")
	}
	if payload.Input.Path != "" && payload.Input.AllFiles {
		return nil, bad("input.path and input.all_files are mutually exclusive")
	}
	if payload.Input.Path != "" && !validBatchedObjectPath(payload.Input.Path) {
		return nil, bad("input.path %q is not a canonical logical path", payload.Input.Path)
	}
	predicate := strings.TrimSpace(payload.Input.Predicate)
	if len(payload.Input.Predicate) > batchedPredicateMaxLen {
		return nil, bad("input.predicate exceeds %d characters", batchedPredicateMaxLen)
	}
	// The predicate grammar is authoritative on the dataset service; PBS
	// only bounds it here (an empty-but-present predicate is a typo, not
	// "no predicate").
	if payload.Input.Predicate != "" && predicate == "" {
		return nil, bad("input.predicate must not be blank")
	}
	logicalPath := strings.TrimSpace(payload.Output.LogicalPath)
	switch payload.Output.Format {
	case "":
		if payload.Output.SchemaVersion != "" {
			return nil, bad("output.schema_version requires output.format parquet")
		}
		if logicalPath == "" {
			logicalPath = batchedDefaultLogicalPath
		}
		if !strings.HasSuffix(logicalPath, ".ndjson") {
			return nil, bad("output.logical_path %q must end with .ndjson", logicalPath)
		}
	case batchedParquetFormat:
		if payload.TransformType == batchedJSONFlattenTransform {
			return nil, bad("batched_json_flatten requires NDJSON output")
		}
		if payload.Output.SchemaVersion != ordersSchemaVersion {
			return nil, bad("output.format parquet requires output.schema_version %q", ordersSchemaVersion)
		}
		if !batchedParquetPath(logicalPath) {
			return nil, bad("output.logical_path %q must name a file with the .parquet extension", logicalPath)
		}
	default:
		return nil, bad("unsupported output.format %q", payload.Output.Format)
	}
	if !validBatchedObjectPath(logicalPath) {
		return nil, bad("output.logical_path %q is not a canonical logical path", logicalPath)
	}
	if strings.SplitN(logicalPath, "/", 2)[0] == "transactions" {
		return nil, bad("output.logical_path %q collides with the reserved transactions/ subtree", logicalPath)
	}
	return &models.BatchedBuildConfig{
		TransformType:      payload.TransformType,
		InputDatasetRID:    inputRID,
		InputBranch:        payload.Input.Branch,
		InputVersion:       int32(payload.Input.Version),
		InputPath:          payload.Input.Path,
		AllFiles:           payload.Input.AllFiles,
		Predicate:          predicate,
		OutputDatasetRID:   outputRID,
		OutputBranch:       args.BuildBranch,
		LogicalPath:        logicalPath,
		JobSpecRID:         spec.RID,
		JobSpecContentHash: spec.ContentHash,
		ActorSubject:       actor,
	}, nil
}

// batchedSpecsEquivalent re-derives the frozen configuration from the
// freshly loaded specs and proves it identical — the CreateBuild
// freeze hook. A republished JobSpec between preflight and resolve
// fails closed here, BEFORE any reservation or lock exists.
func batchedSpecsEquivalent(frozen *models.BatchedBuildConfig, specs []models.JobSpec) error {
	// The freshly loaded graph must still be a single batched spec whose
	// derived configuration and spec identity match the frozen row.
	if len(specs) != 1 {
		return batchedSpecErrorf(http.StatusBadRequest, "batched_spec_redefined", "the JobSpec graph changed during resolution: %d specs loaded, one frozen", len(specs))
	}
	spec := specs[0]
	if spec.RID != frozen.JobSpecRID || spec.ContentHash != frozen.JobSpecContentHash {
		return batchedSpecErrorf(http.StatusBadRequest, "batched_spec_redefined", "JobSpec %s was republished during resolution (content hash changed)", frozen.JobSpecRID)
	}
	rederived, err := parseBatchedSpec(specs, models.CreateBuildRequest{OutputDatasetRIDs: []string{frozen.OutputDatasetRID}}, resolver.ResolveBuildArgs{BuildBranch: frozen.OutputBranch, TriggerKind: "MANUAL"}, frozen.ActorSubject)
	if err != nil {
		return batchedSpecErrorf(http.StatusBadRequest, "batched_spec_redefined", "the republished JobSpec no longer satisfies the batched grammar: %v", err)
	}
	if rederived == nil {
		return batchedSpecErrorf(http.StatusBadRequest, "batched_spec_redefined", "the republished JobSpec no longer declares the batched transform")
	}
	rederived.JobSpecRID = frozen.JobSpecRID
	rederived.JobSpecContentHash = frozen.JobSpecContentHash
	if !batchedConfigEqual(frozen, rederived) {
		return batchedSpecErrorf(http.StatusBadRequest, "batched_spec_redefined", "the republished JobSpec resolves to a different batched configuration")
	}
	return nil
}

func batchedConfigEqual(a, b *models.BatchedBuildConfig) bool {
	return a.TransformType == b.TransformType &&
		a.InputDatasetRID == b.InputDatasetRID &&
		a.InputBranch == b.InputBranch &&
		a.InputVersion == b.InputVersion &&
		a.InputPath == b.InputPath &&
		a.AllFiles == b.AllFiles &&
		a.Predicate == b.Predicate &&
		a.OutputDatasetRID == b.OutputDatasetRID &&
		a.OutputBranch == b.OutputBranch &&
		a.LogicalPath == b.LogicalPath
}

// batchedActiveBranchChecker is the capability the preflight uses to
// prove the build branch is the output dataset's active branch before
// the build row exists (the dataset runtime re-checks at stage/commit).
type batchedActiveBranchChecker interface {
	DatasetActiveBranch(ctx context.Context, datasetRID string) (string, error)
}

// batchedPrepareStore is the narrow CreateBuild-side persistence port.
type batchedPrepareStore interface {
	OpenBatchedPrepare(ctx context.Context, args resolver.ResolveBuildArgs, buildID uuid.UUID, cfg *models.BatchedBuildConfig) error
}

// prepareBatchedCreateBuild runs the pre-write opt-in gates on the REAL
// preflight specs: grammar freeze, HTTP-adapter capability (the
// build-bound reservation opener), and the output dataset's active
// branch. It returns the frozen configuration (nil for legacy builds)
// and mutates args to carry the reservation purpose and freeze hook.
func prepareBatchedCreateBuild(ctx context.Context, ports BuildLifecyclePorts, body models.CreateBuildRequest, args *resolver.ResolveBuildArgs, specs []models.JobSpec) (*models.BatchedBuildConfig, error) {
	cfg, err := parseBatchedSpec(specs, body, *args, uuid.Nil)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil // legacy build: nothing batched applies
	}
	actor := uuid.Nil
	if args.RequestedBy != "" {
		if parsed, perr := uuid.Parse(args.RequestedBy); perr == nil {
			actor = parsed
		}
	}
	if actor == uuid.Nil {
		return nil, batchedSpecErrorf(http.StatusForbidden, "batched_actor_required", "batched builds require an authenticated creator subject")
	}
	cfg.ActorSubject = actor
	// Capability gate before any write: the build-bound reservation
	// protocol needs the HTTP adapter (the co-located repository cannot
	// carry the marker providence) — fail closed, not silently legacy.
	if _, ok := ports.Versioning.(resolver.BuildAwareTransactionOpener); !ok {
		return nil, batchedSpecErrorf(http.StatusServiceUnavailable, "batched_reservation_opener_unavailable", "the batched reservation protocol requires the dataset HTTP versioning adapter")
	}
	// The staged commit publishes to the dataset's ACTIVE branch; prove
	// the build branch is it now (early answer — the dataset runtime
	// still guards the race at stage/commit time).
	checker, ok := ports.Versioning.(batchedActiveBranchChecker)
	if !ok {
		return nil, batchedSpecErrorf(http.StatusServiceUnavailable, "batched_active_branch_unavailable", "the versioning adapter cannot verify the dataset active branch")
	}
	active, err := checker.DatasetActiveBranch(ctx, cfg.OutputDatasetRID)
	if err != nil {
		return nil, err // dataset 403/404/… pass through via writeResolutionHTTPError
	}
	if active != cfg.OutputBranch {
		return nil, batchedSpecErrorf(http.StatusBadRequest, "batched_output_branch_not_active", "output branch %q is not the dataset's active branch %q", cfg.OutputBranch, active)
	}
	args.ReservationPurpose = batchedReservationPurpose
	return cfg, nil
}

// batchedBuildStore is the execute/abort persistence port implemented by
// the production repository.
type batchedBuildStore interface {
	GetBatchedConfig(ctx context.Context, idOrRID string) (*models.BatchedBuildConfig, error)
	AcquireBatchedRun(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) (*models.BatchedBuildConfig, error)
	AcquireBatchedReplay(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) (*models.BatchedBuildConfig, error)
	MarkBatchedStaged(ctx context.Context, buildID, owner uuid.UUID, stagedObjectID, sha256 string, size, records int64) error
	PersistBatchedCommitIntent(ctx context.Context, buildID, owner uuid.UUID, key string, body []byte) error
	MarkBatchedOutcomeUnknown(ctx context.Context, buildID, owner uuid.UUID, reason string) error
	FinalizeBatchedCommit(ctx context.Context, buildID, owner uuid.UUID, responseBody []byte, publishedTxnRID string) error
	FailBatchedBuild(ctx context.Context, buildID, owner uuid.UUID, reason string) error
	ReleaseBatchedLease(ctx context.Context, buildID, owner uuid.UUID) error
	RenewBatchedLease(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) error
	BeginBatchedAbort(ctx context.Context, idOrRID string, takeoverOwner uuid.UUID, leaseTTL time.Duration) (*BatchedAbortDecision, error)
	CompleteBatchedAbort(ctx context.Context, buildID, owner uuid.UUID, keepFailed bool, outputDatasetRID string) error
	// ListPendingTxns backs the precise cancel: the durable pending
	// references are the recoverable truth when the config row carries no
	// reservation (CreateBuild failed before PersistResolvedBuild).
	ListPendingTxns(ctx context.Context, buildID uuid.UUID) ([]PendingOutputTxn, error)
}

// BatchedAbortDecision is the serialized abort verdict for one batched
// build (produced under the config row lock).
type BatchedAbortDecision struct {
	// Kind: preparing | publishing | done | aborted | flagged | takeover |
	// failed_retry.
	Kind      string
	Config    *models.BatchedBuildConfig
	Owner     uuid.UUID // takeover fence owner
	Permanent bool      // publishing conflict that replay cannot resolve
	Detail    string
}

// Abort decision kinds.
const (
	BatchedAbortKindPreparing    = "preparing"
	BatchedAbortKindPublishing   = "publishing"
	BatchedAbortKindDone         = "done"
	BatchedAbortKindAborted      = "aborted"
	BatchedAbortKindFlagged      = "flagged"
	BatchedAbortKindTakeover     = "takeover"
	BatchedAbortKindFailedRetry  = "failed_retry"
	BatchedAbortKindFailedActive = "failed_active"
)

func batchedStoreFromPorts(ports ExecutionPorts) (batchedBuildStore, bool) {
	store, ok := ports.Plans.(batchedBuildStore)
	if !ok || store == nil {
		return nil, false
	}
	return store, true
}

// batchedExecuteDetail is the additive execute-envelope extension.
type batchedExecuteDetail struct {
	StagedObjectID  string `json:"staged_object_id"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	Records         int64  `json:"record_count"`
	PublishedTxnRID string `json:"published_transaction_rid"`
	DatasetRID      string `json:"dataset_rid"`
	Branch          string `json:"branch"`
	IdempotencyKey  string `json:"idempotency_key"`
	Replay          bool   `json:"replay"`
}

type executeBatchedResponse struct {
	BuildID   uuid.UUID                     `json:"build_id"`
	State     string                        `json:"state"`
	Completed int                           `json:"completed"`
	Failed    int                           `json:"failed"`
	Aborted   int                           `json:"aborted"`
	Attempts  map[string]int                `json:"attempts"`
	Nodes     map[string]executor.NodeState `json:"nodes"`
	Reason    string                        `json:"reason,omitempty"`
	Batched   *batchedExecuteDetail         `json:"batched,omitempty"`
}

// runBatchedBuildIfConfigured intercepts POST /api/v1/execute for builds
// carrying a batched configuration. It returns true when the request was
// answered (batched handling or a batched-specific rejection); false
// lets the legacy executor path proceed untouched.
func runBatchedBuildIfConfigured(w http.ResponseWriter, r *http.Request, ports ExecutionPorts, body executePipelineRequest) bool {
	if body.BuildID == nil {
		return false
	}
	store, ok := batchedStoreFromPorts(ports)
	if !ok {
		return false // batched persistence unwired; legacy behavior answers
	}
	ctx := contextWithCallerBearer(r.Context(), callerBearerFromRequest(r))
	cfg, err := store.GetBatchedConfig(ctx, body.BuildID.String())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_build_lookup_failed", "detail": err.Error()})
		return true
	}
	if cfg == nil {
		return false // not a batched build
	}
	if len(body.Nodes) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "batched_inline_not_supported", "detail": "batched builds execute only by build_id; inline nodes are rejected"})
		return true
	}
	runBatchedBuild(w, r.WithContext(ctx), store, cfg)
	return true
}

// runBatchedBuild is the closed manual orchestration for one batched
// build: lease-fenced stage machine, exact-byte commit intent, verified
// ACK, atomic finalize. See increment-3b-plan §3.4.
func runBatchedBuild(w http.ResponseWriter, r *http.Request, store batchedBuildStore, cfg *models.BatchedBuildConfig) {
	ctx := r.Context()
	client, err := newBatchedDatasetClient()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "batched_client_not_configured", "detail": err.Error()})
		return
	}
	leaseTTL := client.cfg.LeaseTTL
	// Identity binding: the acting subject must be the recorded actor
	// (the dataset side independently enforces started_by on every call).
	scope := TenantScopeFromContext(ctx)
	if scope == nil || scope.Subject != cfg.ActorSubject {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "batched_build_actor_mismatch", "detail": "batched builds execute as the subject that created them"})
		return
	}
	switch cfg.Stage {
	case models.BatchedStagePreparing:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_preparing", "detail": "build resolution is still in progress"})
		return
	case models.BatchedStageDone, models.BatchedStageFailed, models.BatchedStageAborted:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_terminal", "detail": "build already reached stage " + cfg.Stage})
		return
	}

	owner := uuid.New()
	acquire := store.AcquireBatchedRun
	replay := false
	if cfg.Stage == models.BatchedStagePublishing || cfg.Stage == models.BatchedStageOutcomeUnknown {
		acquire = store.AcquireBatchedReplay
		replay = true
	}
	acquired, err := acquire(ctx, cfg.BuildID, owner, leaseTTL)
	if err != nil {
		writeBatchedAcquireError(w, err)
		return
	}

	if !replay && acquired.AbortRequested {
		// A cancel persisted before the previous owner died: the takeover
		// must complete the exact cancel — never re-stage or re-commit.
		completeBatchedCancel(w, ctx, store, acquired, owner, false, "canceled before the previous attempt finished (flag honored on takeover)")
		return
	}
	if replay {
		replayBatchedCommit(w, ctx, store, client, acquired, owner)
		return
	}
	executeBatchedRun(w, r, store, client, acquired, owner, leaseTTL)
}

func writeBatchedAcquireError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrBatchedLeaseHeld):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_lease_held", "detail": "another execution attempt holds the lease; retry after it completes or its lease expires"})
	case errors.Is(err, ErrBatchedPreparing):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_preparing", "detail": "build resolution is still in progress"})
	case errors.Is(err, ErrBatchedPublishing):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_publish_outcome_unknown", "detail": "a commit intent exists; re-execute replays the exact stored bytes"})
	case errors.Is(err, ErrBatchedTerminal):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_terminal", "detail": err.Error()})
	case errors.Is(err, ErrBatchedQueued):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_queued", "detail": "the build is queued behind an upstream build; it must be re-created once the upstream completes"})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_acquire_failed", "detail": err.Error()})
	}
}

// executeBatchedRun drives a fresh (or restartable pre-intent) run:
// scan → stage → verify → intent → send → classify.
func executeBatchedRun(w http.ResponseWriter, r *http.Request, store batchedBuildStore, client batchedDatasetClient, cfg *models.BatchedBuildConfig, owner uuid.UUID, leaseTTL time.Duration) {
	ctx := r.Context()
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	unregister := registerExecutionCancel(cfg.BuildID, cancel)
	defer unregister()

	reservationUUID, resvErr := batchedReservationUUID(cfg.ReservationRID)
	if resvErr != nil {
		handleBatchedStageFailure(w, ctx, store, cfg, owner, resvErr)
		return
	}
	receipt, digest, stageErr := stageBatchedInput(execCtx, client, cfg)
	if stageErr != nil {
		if errors.Is(stageErr, context.Canceled) {
			// A recorded abort cancels the in-flight scan/stage: this is
			// the REQUEST-ABORT path (ABORTED terminal), never FAILED. A
			// cancellation without an abort flag (caller disconnect)
			// releases the lease for a clean retry.
			if current, gerr := store.GetBatchedConfig(ctx, cfg.BuildID.String()); gerr == nil && current != nil && current.AbortRequested {
				completeBatchedCancel(w, ctx, store, cfg, owner, false, "aborted during staging")
				return
			}
			releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancelRelease()
			if rerr := store.ReleaseBatchedLease(releaseCtx, cfg.BuildID, owner); rerr != nil && !errors.Is(rerr, ErrBatchedFenceLost) {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_lease_release_failed", "detail": rerr.Error()})
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "batched_stage_interrupted", "detail": "execution was canceled before the commit intent; re-execute to retry (stage is idempotent)"})
			return
		}
		handleBatchedStageFailure(w, ctx, store, cfg, owner, stageErr)
		return
	}
	// Stage receipt binding: EVERY facet must echo what this request
	// pinned (dataset/branch/reservation/build/logical path/key) — the
	// sealed descriptor is what the keyed commit later claims, so a
	// mismatched receipt is unusable by construction.
	if receipt.DatasetRID != cfg.OutputDatasetRID ||
		receipt.Branch != cfg.OutputBranch ||
		receipt.ReservationID != reservationUUID.String() ||
		receipt.BuildID != cfg.BuildID.String() ||
		receipt.LogicalPath != cfg.LogicalPath ||
		receipt.IdempotencyKey != batchedStableKey(cfg.BuildID) {
		handleBatchedStageFailure(w, ctx, store, cfg, owner, fmt.Errorf("batched_stage_receipt_mismatch: receipt binding (%s/%s/%s/%s/%s/%s) does not echo the request",
			receipt.DatasetRID, receipt.Branch, receipt.ReservationID, receipt.BuildID, receipt.LogicalPath, receipt.IdempotencyKey))
		return
	}
	// Three-way agreement: PBS-computed, declared (none — the receipt IS
	// the service's sealed authority) and sealed values must match what
	// PBS streamed.
	if receipt.SizeBytes != digest.size || receipt.SHA256 != digest.sha || receipt.RecordCount != digest.records {
		handleBatchedStageFailure(w, ctx, store, cfg, owner, fmt.Errorf("batched_stage_mismatch: streamed %d bytes/%d records (%s) but the service sealed %d bytes/%d records (%s)",
			digest.size, digest.records, digest.sha, receipt.SizeBytes, receipt.RecordCount, receipt.SHA256))
		return
	}
	if err := store.MarkBatchedStaged(ctx, cfg.BuildID, owner, receipt.StagedObjectID, receipt.SHA256, receipt.SizeBytes, receipt.RecordCount); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_fence_lost", "detail": err.Error()})
		return
	}

	// The publish boundary: ONE marshal, durable bytes + key. Abort is
	// refused from the instant this write commits; the CAS refuses a
	// concurrently recorded abort request (that path completes the exact
	// cancel itself).
	intent := batchedCommitRequest{
		IdempotencyKey:  batchedStableKey(cfg.BuildID),
		Branch:          cfg.OutputBranch,
		TransactionType: "SNAPSHOT",
		ReservationID:   reservationUUID,
		BuildID:         cfg.BuildID,
		Schema:          digest.schema,
		Files: []batchedCommitFile{{
			LogicalPath:    cfg.LogicalPath,
			Operation:      "STAGED_ADD",
			StagedObjectID: receipt.StagedObjectID,
			SizeBytes:      receipt.SizeBytes,
			SHA256:         receipt.SHA256,
		}},
	}
	if batchedParquetPath(cfg.LogicalPath) {
		intent.Files[0].ContentType = batchedParquetContentType
		intent.Files[0].Metadata = map[string]string{"schema_version": ordersSchemaVersion}
	}
	commitBody, err := json.Marshal(intent)
	if err != nil {
		handleBatchedStageFailure(w, ctx, store, cfg, owner, fmt.Errorf("batched_commit_marshal: %w", err))
		return
	}
	if err := store.PersistBatchedCommitIntent(ctx, cfg.BuildID, owner, intent.IdempotencyKey, commitBody); err != nil {
		if errors.Is(err, ErrBatchedPublishing) {
			// A concurrent abort request won the race before the intent
			// landed: complete the exact cancel under our fence.
			completeBatchedCancel(w, ctx, store, cfg, owner, false, "aborted before publish")
			return
		}
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_fence_lost", "detail": err.Error()})
		return
	}

	detail := &batchedExecuteDetail{
		StagedObjectID: receipt.StagedObjectID,
		SizeBytes:      receipt.SizeBytes,
		SHA256:         receipt.SHA256,
		Records:        receipt.RecordCount,
		DatasetRID:     cfg.OutputDatasetRID,
		Branch:         cfg.OutputBranch,
		IdempotencyKey: intent.IdempotencyKey,
	}
	classifyBatchedCommit(w, ctx, store, client, cfg, owner, commitBody, detail)
}

// replayBatchedCommit resends the PERSISTED intent bytes — never
// re-marshaled — and classifies the answer exactly like a first send.
func replayBatchedCommit(w http.ResponseWriter, ctx context.Context, store batchedBuildStore, client batchedDatasetClient, cfg *models.BatchedBuildConfig, owner uuid.UUID) {
	if len(cfg.CommitRequestBody) == 0 || cfg.CommitIDempotencyKey == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_replay_body_missing", "detail": "publishing stage without a persisted intent body"})
		return
	}
	detail := &batchedExecuteDetail{
		StagedObjectID: cfg.StagedObjectID,
		SizeBytes:      cfg.StagedSize,
		SHA256:         cfg.StagedSHA256,
		Records:        cfg.StagedRecords,
		DatasetRID:     cfg.OutputDatasetRID,
		Branch:         cfg.OutputBranch,
		IdempotencyKey: cfg.CommitIDempotencyKey,
		Replay:         true,
	}
	classifyBatchedCommit(w, ctx, store, client, cfg, owner, cfg.CommitRequestBody, detail)
}

// classifyBatchedCommit sends (or resends) the exact bytes and maps every
// outcome onto the publish-pending protocol: after the intent exists,
// NOTHING transitions to FAILED and no legacy cleanup runs — unresolved
// outcomes persist as OUTCOME_UNKNOWN for exact-byte replay.
func classifyBatchedCommit(w http.ResponseWriter, ctx context.Context, store batchedBuildStore, client batchedDatasetClient, cfg *models.BatchedBuildConfig, owner uuid.UUID, body []byte, detail *batchedExecuteDetail) {
	status, respBody, err := client.CommitExact(ctx, cfg.OutputDatasetRID, body)
	if err != nil {
		recordBatchedOutcomeUnknown(w, ctx, store, cfg, owner, fmt.Sprintf("transport: %v", err), http.StatusServiceUnavailable)
		return
	}
	switch {
	case status == http.StatusCreated || status == http.StatusOK:
		published, ackErr := verifyBatchedCommitACK(respBody, cfg)
		if ackErr != nil {
			recordBatchedOutcomeUnknown(w, ctx, store, cfg, owner, fmt.Sprintf("ack validation: %v", ackErr), http.StatusBadGateway)
			return
		}
		detail.PublishedTxnRID = published
		finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := store.FinalizeBatchedCommit(finalizeCtx, cfg.BuildID, owner, respBody, published); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_fence_lost", "detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, executeBatchedResponse{
			BuildID: cfg.BuildID, State: string(models.BuildCompleted), Completed: 1,
			Attempts: map[string]int{cfg.JobSpecRID: 1},
			Nodes:    map[string]executor.NodeState{cfg.JobSpecRID: executor.NodeCompleted},
			Batched:  detail,
		})
	case status == http.StatusConflict:
		reason := batchedErrorFromResponse(status, respBody).Error()
		recordBatchedOutcomeUnknown(w, ctx, store, cfg, owner, "permanent conflict: "+reason, http.StatusConflict)
	case status >= 500:
		recordBatchedOutcomeUnknown(w, ctx, store, cfg, owner, fmt.Sprintf("dataset service answered %d", status), http.StatusServiceUnavailable)
	default:
		recordBatchedOutcomeUnknown(w, ctx, store, cfg, owner, fmt.Sprintf("dataset service rejected the exact replay with %d", status), http.StatusBadGateway)
	}
}

func recordBatchedOutcomeUnknown(w http.ResponseWriter, ctx context.Context, store batchedBuildStore, cfg *models.BatchedBuildConfig, owner uuid.UUID, reason string, status int) {
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := store.MarkBatchedOutcomeUnknown(markCtx, cfg.BuildID, owner, reason); err != nil && !errors.Is(err, ErrBatchedFenceLost) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_outcome_record_failed", "detail": err.Error()})
		return
	}
	writeJSON(w, status, map[string]any{
		"error":    "batched_publish_outcome_unknown",
		"detail":   reason + "; the exact request body is persisted — re-execute as the same subject replays it byte-for-byte",
		"build_id": cfg.BuildID,
	})
}

// verifyBatchedCommitACK validates the commit receipt against the frozen
// output binding and returns the canonical published transaction RID.
// Wire facts (3A source): first commit answers 201, receipt replay 200;
// the envelope carries dataset_id (UUID), dataset_rid, branch; the inner
// transaction carries id (UUID), dataset_id, branch_name, tx_type and
// status — all must line up, and the published transaction must differ
// from the reservation.
func verifyBatchedCommitACK(body []byte, cfg *models.BatchedBuildConfig) (string, error) {
	var ack struct {
		DatasetID   uuid.UUID `json:"dataset_id"`
		DatasetRID  string    `json:"dataset_rid"`
		Branch      string    `json:"branch"`
		Transaction struct {
			ID         uuid.UUID `json:"id"`
			DatasetID  uuid.UUID `json:"dataset_id"`
			BranchName string    `json:"branch_name"`
			TxType     string    `json:"tx_type"`
			Status     string    `json:"status"`
		} `json:"transaction"`
	}
	if err := json.Unmarshal(body, &ack); err != nil {
		return "", fmt.Errorf("receipt is not JSON: %w", err)
	}
	expectedUUID, err := batchedDatasetUUID(cfg.OutputDatasetRID)
	if err != nil {
		return "", err
	}
	if ack.Transaction.Status != "COMMITTED" {
		return "", fmt.Errorf("transaction status %q is not COMMITTED", ack.Transaction.Status)
	}
	if ack.DatasetRID != cfg.OutputDatasetRID {
		return "", fmt.Errorf("receipt dataset %q != output %q", ack.DatasetRID, cfg.OutputDatasetRID)
	}
	if ack.DatasetID != expectedUUID {
		return "", fmt.Errorf("receipt dataset_id %s != output %s", ack.DatasetID, expectedUUID)
	}
	if ack.Branch != cfg.OutputBranch || ack.Transaction.BranchName != cfg.OutputBranch {
		return "", fmt.Errorf("receipt branch (%q/%q) != output branch %q", ack.Branch, ack.Transaction.BranchName, cfg.OutputBranch)
	}
	if ack.Transaction.DatasetID != expectedUUID {
		return "", fmt.Errorf("transaction dataset_id %s != output %s", ack.Transaction.DatasetID, expectedUUID)
	}
	if ack.Transaction.TxType != "SNAPSHOT" {
		return "", fmt.Errorf("transaction tx_type %q is not SNAPSHOT", ack.Transaction.TxType)
	}
	if ack.Transaction.ID == uuid.Nil {
		return "", errors.New("transaction id is missing")
	}
	reservationUUID, err := batchedReservationUUID(cfg.ReservationRID)
	if err == nil && ack.Transaction.ID == reservationUUID {
		return "", errors.New("published transaction equals the reservation — the staged commit must open a new SNAPSHOT transaction")
	}
	return "ri.foundry.main.transaction." + ack.Transaction.ID.String(), nil
}

// batchedCommitRequest is the exact keyed commit wire shape (ONE marshal
// per build; the bytes are what gets persisted and replayed).
type batchedCommitRequest struct {
	IdempotencyKey  string              `json:"idempotency_key"`
	Branch          string              `json:"branch"`
	TransactionType string              `json:"transaction_type"`
	ReservationID   uuid.UUID           `json:"reservation_id"`
	BuildID         uuid.UUID           `json:"build_id"`
	Files           []batchedCommitFile `json:"files"`
	Schema          *datasetRowsSchema  `json:"schema,omitempty"`
}

type batchedCommitFile struct {
	LogicalPath    string            `json:"logical_path"`
	Operation      string            `json:"operation"`
	StagedObjectID string            `json:"staged_object_id"`
	SizeBytes      int64             `json:"size_bytes"`
	SHA256         string            `json:"sha256"`
	ContentType    string            `json:"content_type,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// batchedStableKey is THE one stable idempotency key of a batched build,
// used by BOTH outputs:stage and outputs:commit: the dataset service's
// ValidateStagedObjectForCommit compares the sealed descriptor's key
// against the commit body's key — they must be identical.
func batchedStableKey(buildID uuid.UUID) string { return "wp33." + buildID.String() }

// batchedDigest accumulates the streamed stage body facts.
type batchedDigest struct {
	sha     string
	size    int64
	records int64
	schema  *datasetRowsSchema
}

// stageBatchedInput streams the pinned input scan into ONE stage upload:
// records flow page-by-page through a bounded buffered writer with
// running SHA-256/counters; no page array is ever retained and no
// data_rows are produced.
func stageBatchedInput(ctx context.Context, client batchedDatasetClient, cfg *models.BatchedBuildConfig) (batchedStageResponse, batchedDigest, error) {
	req := batchedStageRequest{
		DatasetRID:     cfg.OutputDatasetRID,
		Branch:         cfg.OutputBranch,
		ReservationRID: cfg.ReservationRID,
		BuildID:        cfg.BuildID,
		LogicalPath:    cfg.LogicalPath,
		IdempotencyKey: batchedStableKey(cfg.BuildID),
	}
	if batchedParquetPath(cfg.LogicalPath) {
		req.Format = batchedParquetFormat
	}
	input := batchedScanInput{
		DatasetRID: cfg.InputDatasetRID,
		Branch:     cfg.InputBranch,
		Version:    cfg.InputVersion,
		Path:       cfg.InputPath,
		Predicate:  cfg.Predicate,
	}
	hash := sha256.New()
	var size, records int64
	var schema *datasetRowsSchema
	maxRecord := client.cfg.StageMaxRecordBytes
	receipt, err := client.StageStream(ctx, req, func(feedCtx context.Context, w *bufio.Writer) error {
		// feedCtx is the stage attempt context: an early HTTP answer (or
		// timeout) tears the upload down and cancels the scan mid-page.
		if req.Format == batchedParquetFormat {
			var err error
			size, records, err = streamOrdersParquet(feedCtx, w, hash, maxRecord, func(emit func([]byte) error) error {
				_, err := client.ScanStream(feedCtx, input, emit)
				return err
			})
			return err
		}
		input.flattenJSON = cfg.TransformType == batchedJSONFlattenTransform
		input.schemaInference = scanSchemaInference(input.flattenJSON)
		stats, scanErr := client.ScanStream(feedCtx, input, func(record []byte) error {
			rec := record
			if bytes.ContainsAny(rec, "\n\r") {
				// Raw control characters cannot occur in valid JSON, but
				// the stream stays NDJSON-safe regardless: compact
				// (bounded by the record cap below).
				var buf bytes.Buffer
				if err := json.Compact(&buf, rec); err != nil {
					return fmt.Errorf("batched_record_invalid: %w", err)
				}
				rec = buf.Bytes()
			}
			if !json.Valid(rec) {
				return errors.New("batched_record_invalid: scan returned a non-JSON record")
			}
			if int64(len(rec))+1 > maxRecord {
				return fmt.Errorf("batched_record_too_large: %d bytes (with newline) exceeds the %d-byte scan line cap", len(rec)+1, maxRecord)
			}
			if _, err := w.Write(rec); err != nil {
				return err
			}
			if err := w.WriteByte('\n'); err != nil {
				return err
			}
			hash.Write(rec)
			hash.Write([]byte{'\n'})
			size += int64(len(rec)) + 1
			records++
			return nil
		})
		if scanErr == nil && (stats.Metadata.Format == "csv" || input.flattenJSON) {
			schema = stats.Metadata.Schema
		}
		return scanErr
	})
	if err != nil {
		return batchedStageResponse{}, batchedDigest{}, err
	}
	return receipt, batchedDigest{sha: hex.EncodeToString(hash.Sum(nil)), size: size, records: records, schema: schema}, nil
}

// handleBatchedStageFailure terminates a PRE-INTENT failure: the precise
// cancel is attempted (bounded, detached); a confirmed cancel lands
// FAILED with this build's lock/pending released in one transaction, a
// failed cancel keeps the references for the abort-retry path exactly
// like the legacy FAILED-build semantics.
func handleBatchedStageFailure(w http.ResponseWriter, ctx context.Context, store batchedBuildStore, cfg *models.BatchedBuildConfig, owner uuid.UUID, cause error) {
	if err := store.RenewBatchedLease(ctx, cfg.BuildID, owner, batchedAbortLeaseTTL); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_fence_lost", "detail": "the failure-cleanup fence was taken over; the new owner completes the cleanup"})
		return
	}
	cancelErr := preciseCancelBatchedReservation(ctx, store, cfg)
	if cancelErr == nil {
		completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := store.CompleteBatchedAbort(completeCtx, cfg.BuildID, owner, true, cfg.OutputDatasetRID); err != nil && !errors.Is(err, ErrBatchedFenceLost) {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_failure_finalize_failed", "detail": fmt.Sprintf("%v (finalize: %v)", cause, err)})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    "batched_stage_failed",
			"detail":   cause.Error(),
			"build_id": cfg.BuildID,
			"state":    string(models.BuildFailed),
		})
		return
	}
	failCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	reason := fmt.Sprintf("%s (reservation cancel failed, references retained for abort retry: %v)", cause.Error(), cancelErr)
	if err := store.FailBatchedBuild(failCtx, cfg.BuildID, owner, reason); err != nil && !errors.Is(err, ErrBatchedFenceLost) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_failure_finalize_failed", "detail": reason + " (state write: " + err.Error() + ")"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]any{
		"error":    "batched_stage_failed",
		"detail":   reason,
		"build_id": cfg.BuildID,
		"state":    string(models.BuildFailed),
	})
}

// completeBatchedCancel finishes an abort-requested run: precise cancel
// then the terminal ABORTED write (fence-checked; a lost fence hands the
// cleanup to the takeover owner, which performs its own cancel).
func completeBatchedCancel(w http.ResponseWriter, ctx context.Context, store batchedBuildStore, cfg *models.BatchedBuildConfig, owner uuid.UUID, keepFailed bool, reason string) {
	// The fence must still be OURS while the remote abort is in flight —
	// an expired-and-taken-over fence hands the cancel to the new owner.
	if err := store.RenewBatchedLease(ctx, cfg.BuildID, owner, batchedAbortLeaseTTL); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_fence_lost", "detail": "the cancel fence was taken over before the remote abort; the new owner completes it"})
		return
	}
	if err := preciseCancelBatchedReservation(ctx, store, cfg); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":    "batched_abort_cancel_failed",
			"detail":   fmt.Sprintf("%v (retry the abort; references retained)", err),
			"build_id": cfg.BuildID,
		})
		return
	}
	completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := store.CompleteBatchedAbort(completeCtx, cfg.BuildID, owner, keepFailed, cfg.OutputDatasetRID); err != nil && !errors.Is(err, ErrBatchedFenceLost) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_abort_finalize_failed", "detail": err.Error()})
		return
	}
	state := models.BuildAborted
	if keepFailed {
		state = models.BuildFailed
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"build_id": cfg.BuildID,
		"state":    string(state),
		"detail":   reason,
	})
}

// preciseCancelBatchedReservation aborts EXACTLY this build's
// reservation(s) as the calling user, on a bounded detached context. The
// durable pending references are ALWAYS consulted: a CreateBuild failure
// before PersistResolvedBuild leaves the config's reservation empty while
// build_pending_output_txns still holds the real (possibly dangling)
// reservation — treating an empty config field as "nothing to cancel"
// would discard the only recoverable reference. Cross-tenant 404s and
// terminal-state conflicts are errors — never assumed success.
func preciseCancelBatchedReservation(ctx context.Context, store batchedBuildStore, cfg *models.BatchedBuildConfig) error {
	targets := []string{}
	if cfg.ReservationRID != "" {
		targets = append(targets, cfg.ReservationRID)
	}
	if lister, ok := store.(PendingTxnLister); ok {
		pending, err := lister.ListPendingTxns(ctx, cfg.BuildID)
		if err != nil {
			return fmt.Errorf("pending reference lookup failed: %w", err)
		}
		configUUID, configErr := batchedDatasetUUID(cfg.OutputDatasetRID)
		if configErr != nil {
			return fmt.Errorf("batched output reference invalid: %w", configErr)
		}
		for _, ref := range pending {
			// Pending rows carry the JobSpec's VERBATIM output spelling;
			// match by parsed UUID so a bare-UUID spec still finds its
			// reservation references.
			refUUID, refErr := batchedDatasetUUID(ref.DatasetRID)
			if refErr != nil || refUUID != configUUID {
				continue
			}
			if ref.Branch != "" && ref.Branch != cfg.OutputBranch {
				continue
			}
			known := false
			for _, t := range targets {
				if t == ref.TransactionRID {
					known = true
					break
				}
			}
			if !known {
				targets = append(targets, ref.TransactionRID)
			}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	aborter := datasetTransactionAborter(ctx)
	if aborter == nil {
		return errors.New("dataset aborter unavailable")
	}
	var firstErr error
	for _, txnRID := range targets {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		err := aborter.AbortTransaction(cleanupCtx, cfg.OutputDatasetRID, cfg.OutputBranch, txnRID)
		cancel()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// abortBatchedBuildIfConfigured is the abort-route guard: batched builds
// never run the legacy job-outputs abort sweep. Returns true when the
// request was answered.
func abortBatchedBuildIfConfigured(w http.ResponseWriter, r *http.Request, idOrRID string) bool {
	ports, ok := currentExecutionPorts()
	if !ok {
		return false
	}
	store, ok := batchedStoreFromPorts(ports)
	if !ok {
		return false
	}
	ctx := withCallerBearer(r)
	cfg, err := store.GetBatchedConfig(ctx, idOrRID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_build_lookup_failed", "detail": err.Error()})
		return true
	}
	if cfg == nil {
		return false
	}
	// Same-org is not same-actor: only the recorded creating subject may
	// cancel or take over the lease — a colleague in the organization must
	// not flip abort flags on someone else's build (the dataset side would
	// independently refuse the reservation abort; PBS refuses earlier and
	// cleaner).
	scope := TenantScopeFromContext(ctx)
	if scope == nil || scope.Subject != cfg.ActorSubject {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "batched_build_actor_mismatch", "detail": "batched builds abort as the subject that created them"})
		return true
	}
	decision, err := store.BeginBatchedAbort(ctx, idOrRID, uuid.New(), batchedAbortLeaseTTL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "batched_abort_decision_failed", "detail": err.Error()})
		return true
	}
	switch decision.Kind {
	case BatchedAbortKindPreparing:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "batched_build_preparing", "detail": "build resolution is still in progress"})
	case BatchedAbortKindPublishing:
		body := map[string]any{
			"error":    "batched_publish_outcome_unknown",
			"detail":   "a commit intent is durable; resolve it by exact-byte replay (re-execute as the creating subject) — abort is refused",
			"build_id": cfg.BuildID,
		}
		if decision.Permanent {
			body["permanent_conflict"] = true
			if decision.Detail != "" {
				body["last_outcome"] = decision.Detail
			}
		}
		writeJSON(w, http.StatusConflict, body)
	case BatchedAbortKindDone:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "build_already_completed", "detail": "the build already completed"})
	case BatchedAbortKindAborted:
		writeJSON(w, http.StatusOK, map[string]any{"build_id": cfg.BuildID, "state": string(models.BuildAborted), "aborted_transactions": 0})
	case BatchedAbortKindFlagged:
		canceledExecution := cancelExecution(cfg.BuildID)
		writeJSON(w, http.StatusOK, map[string]any{
			"build_id": cfg.BuildID, "state": string(models.BuildAborting),
			"cancelled_execution": canceledExecution, //nolint:misspell // wire key matches the legacy abort envelope
			"detail":              "abort requested; the in-flight attempt completes the exact cancel at its next checkpoint (before the commit intent)",
		})
	case BatchedAbortKindFailedActive:
		writeJSON(w, http.StatusOK, map[string]any{
			"build_id": cfg.BuildID, "state": string(models.BuildFailed),
			"detail": "the failing attempt still holds the cleanup lease; retry the abort once it completes or its lease expires",
		})
	case BatchedAbortKindTakeover, BatchedAbortKindFailedRetry:
		completeBatchedCancel(w, ctx, store, decision.Config, decision.Owner, decision.Kind == BatchedAbortKindFailedRetry, "aborted by user")
	}
	return true
}
