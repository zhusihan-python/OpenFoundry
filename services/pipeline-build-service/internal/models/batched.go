package models

import (
	"time"

	"github.com/google/uuid"
)

// Batched execution (WP3.3 3B): the closed-grammar, single-JobSpec /
// single-input / single-output dataset copy mode. A build participates
// only when its one TRANSFORM JobSpec declares
// logic_payload.transform_type == "batched_dataset_copy"; every other
// shape keeps the legacy resolution/execution paths untouched.
//
// The resolved configuration is frozen at CreateBuild time (validated
// against the REAL preflight specs of that request), persisted in the
// same PBS transaction that opens the build row, and never re-derived
// from the editable job_specs/pipeline_job_specs mapping at execute
// time. The execution-stage columns below are the only mutable part.

// Batched build execution stages (closed state machine, persisted in
// build_batched_configs.stage).
const (
	// BatchedStagePreparing: the build row and this config were inserted
	// but resolution has not persisted yet. Execute/abort answer 409.
	BatchedStagePreparing = "PREPARING"
	// BatchedStagePending: resolution persisted (BUILD_RESOLUTION or
	// BUILD_QUEUED); ready to execute.
	BatchedStagePending = "PENDING"
	// BatchedStageStaging: an execute attempt holds the lease and is
	// scanning/staging.
	BatchedStageStaging = "STAGING"
	// BatchedStageStaged: the staged object was sealed and verified
	// (three-way size/sha/records) but no commit intent exists yet.
	BatchedStageStaged = "STAGED"
	// BatchedStagePublishing: the exact keyed commit request body is
	// durably persisted. From this point on abort is ALWAYS 409.
	BatchedStagePublishing = "PUBLISHING"
	// BatchedStageOutcomeUnknown: the commit request was sent and the
	// outcome is unresolved (transport error / OUTCOME_UNKNOWN /
	// response mismatch / key conflict). Exact-byte replay is the only
	// recovery; never FAILED, never legacy cleanup.
	BatchedStageOutcomeUnknown = "OUTCOME_UNKNOWN"
	// BatchedStageDone: commit ACK verified and finalized atomically.
	BatchedStageDone = "DONE"
	// BatchedStageFailed: failed BEFORE the commit intent (no publish
	// attempt was made).
	BatchedStageFailed = "FAILED"
	// BatchedStageAborted: precisely canceled before the commit intent.
	BatchedStageAborted = "ABORTED"
)

// BatchedStageTerminal reports whether the stage admits no further
// execute transitions (FAILED still admits abort-retry cleanup, like
// the legacy FAILED-build abort retry).
func BatchedStageTerminal(stage string) bool {
	switch stage {
	case BatchedStageDone, BatchedStageFailed, BatchedStageAborted:
		return true
	}
	return false
}

// BatchedBuildConfig is the persisted frozen configuration plus the
// mutable execution bookkeeping for one opt-in batched build. The
// configuration columns are written once (CreateBuild) and never
// updated; only the execution columns change afterwards. No JWT or
// credential material is ever stored here.
type BatchedBuildConfig struct {
	BuildID uuid.UUID `json:"build_id"`

	// Frozen configuration (CreateBuild-time, from the real preflight
	// specs of that request).
	TransformType      string `json:"transform_type"`
	InputDatasetRID    string `json:"input_dataset_rid"`
	InputBranch        string `json:"input_branch"`
	InputVersion       int32  `json:"input_version"`
	InputPath          string `json:"input_path,omitempty"` // "" = all_files
	AllFiles           bool   `json:"all_files"`
	Predicate          string `json:"predicate,omitempty"`
	OutputDatasetRID   string `json:"output_dataset_rid"`
	OutputBranch       string `json:"output_branch"`
	LogicalPath        string `json:"logical_path"`
	JobSpecRID         string `json:"job_spec_rid"`
	JobSpecContentHash string `json:"job_spec_content_hash"`
	ActorSubject       uuid.UUID
	ReservationRID     string `json:"reservation_rid,omitempty"`

	// Execution state (mutable, lease-fenced where noted).
	Stage          string     `json:"stage"`
	LeaseOwner     *uuid.UUID `json:"-"`
	LeaseExpiresAt *time.Time `json:"-"`
	AbortRequested bool       `json:"-"`

	StagedObjectID string `json:"staged_object_id,omitempty"`
	StagedSize     int64  `json:"staged_size_bytes,omitempty"`
	StagedSHA256   string `json:"staged_sha256,omitempty"`
	StagedRecords  int64  `json:"staged_records,omitempty"`

	CommitIDempotencyKey string `json:"commit_idempotency_key,omitempty"`
	CommitRequestBody    []byte `json:"-"`
	CommitResponseBody   []byte `json:"-"`
	PublishedTxnRID      string `json:"published_transaction_rid,omitempty"`
	ErrorMessage         string `json:"error_message,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
