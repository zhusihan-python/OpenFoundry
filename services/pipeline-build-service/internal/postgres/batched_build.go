package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// Batched build execution persistence (WP3.3 3B). Every mutation of the
// execution columns is lease-fenced (CAS on lease_owner); the frozen
// configuration columns are written once by OpenBatchedPrepare and never
// updated. Errors below are package-level sentinels the HTTP layer maps
// to stable codes.

// Batched store sentinels live in the handler package (this package
// imports it); they are re-aliased here for local readability.
var (
	ErrBatchedLeaseHeld  = handler.ErrBatchedLeaseHeld
	ErrBatchedPreparing  = handler.ErrBatchedPreparing
	ErrBatchedQueued     = handler.ErrBatchedQueued
	ErrBatchedTerminal   = handler.ErrBatchedTerminal
	ErrBatchedPublishing = handler.ErrBatchedPublishing
	ErrBatchedFenceLost  = handler.ErrBatchedFenceLost
	ErrBatchedNotFound   = handler.ErrBatchedNotFound
)

const batchedConfigColumns = `c.build_id, c.transform_type, c.input_dataset_rid, c.input_branch, c.input_version,
c.input_path, c.all_files, c.predicate, c.output_dataset_rid, c.output_branch, c.logical_path,
c.job_spec_rid, c.job_spec_content_hash, c.actor_subject, c.reservation_rid,
c.stage, c.lease_owner, c.lease_expires_at, c.abort_requested,
c.staged_object_id, c.staged_size_bytes, c.staged_sha256, c.staged_records,
c.commit_idempotency_key, c.commit_request_body, c.commit_response_body, c.published_transaction_rid,
c.error_message, c.created_at, c.updated_at`

// datasetUUIDFromRef parses a dataset reference that may be a bare UUID or
// a canonical RID, in any case. JobSpec outputs are persisted VERBATIM
// (bare UUIDs stay bare), while the frozen batched configuration is
// canonical — every cross-match goes through the parsed UUID, never the
// string spelling.
func datasetUUIDFromRef(ref string) (uuid.UUID, bool) {
	trimmed := strings.TrimSpace(ref)
	if parsed, err := uuid.Parse(trimmed); err == nil {
		return parsed, true
	}
	if tail := strings.TrimPrefix(trimmed, "ri.foundry.main.dataset."); tail != trimmed {
		if parsed, err := uuid.Parse(tail); err == nil {
			return parsed, true
		}
	}
	return uuid.Nil, false
}

// GetBatchedConfig loads the batched configuration for a build (id, RID or
// UUID text). Returns (nil, nil) when the build is not a batched build.
// Tenant-scoped through the builds join.
func (r *Repository) GetBatchedConfig(ctx context.Context, idOrRID string) (*models.BatchedBuildConfig, error) {
	where := "WHERE (b.id::text=$1 OR b.rid=$1)"
	args := []any{idOrRID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		// "builds b" (not the bare alias) so the personal-domain branch
		// maps to builds.requested_by — a bare alias would fall through to
		// the pipelines owner column and emit b.owner_id, which does not
		// exist on builds.
		appendTenantClause(sb, scope, "builds b")
		where += sb.whereSQL()
		args = sb.args
	}
	row := r.db.QueryRow(ctx, `SELECT `+batchedConfigColumns+`
FROM build_batched_configs c JOIN builds b ON b.id=c.build_id `+where, args...)
	cfg, err := scanBatchedConfig(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

type batchedConfigScanner interface {
	Scan(dest ...any) error
}

func scanBatchedConfig(row batchedConfigScanner) (*models.BatchedBuildConfig, error) {
	var c models.BatchedBuildConfig
	err := row.Scan(&c.BuildID, &c.TransformType, &c.InputDatasetRID, &c.InputBranch, &c.InputVersion,
		&c.InputPath, &c.AllFiles, &c.Predicate, &c.OutputDatasetRID, &c.OutputBranch, &c.LogicalPath,
		&c.JobSpecRID, &c.JobSpecContentHash, &c.ActorSubject, &c.ReservationRID,
		&c.Stage, &c.LeaseOwner, &c.LeaseExpiresAt, &c.AbortRequested,
		&c.StagedObjectID, &c.StagedSize, &c.StagedSHA256, &c.StagedRecords,
		&c.CommitIDempotencyKey, &c.CommitRequestBody, &c.CommitResponseBody, &c.PublishedTxnRID,
		&c.ErrorMessage, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// OpenBatchedPrepare inserts the builds row AND the PREPARING batched
// configuration in ONE transaction, so execute/abort observe the opt-in
// marker from the moment the build row exists (no window that could
// route back into the legacy empty-plan path). It is the batched
// counterpart of OpenBuild.
func (r *Repository) OpenBatchedPrepare(ctx context.Context, args resolver.ResolveBuildArgs, buildID uuid.UUID, cfg *models.BatchedBuildConfig) error {
	if cfg == nil {
		return errors.New("batched configuration is required")
	}
	if args.TriggerKind == "" {
		args.TriggerKind = "MANUAL"
	}
	if args.JobSpecFallback == nil {
		args.JobSpecFallback = []string{}
	}
	if args.AbortPolicy == "" {
		args.AbortPolicy = string(models.AbortDependentOnly)
	}
	exec := func(ctx context.Context, db DB) error {
		if _, err := db.Exec(ctx, `INSERT INTO builds (id, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, requested_by, abort_policy, tenant_id)
VALUES ($1,$2,$3,$4,'BUILD_RESOLUTION',$5,$6,$7,$8,$9)`,
			buildID, args.PipelineRID, args.BuildBranch, args.JobSpecFallback, args.TriggerKind, args.ForceBuild, args.RequestedBy, args.AbortPolicy, tenantValueForInsert(ctx)); err != nil {
			return fmt.Errorf("batched_prepare_build_conflict: build %s already exists: %w", buildID, err)
		}
		_, err := db.Exec(ctx, `INSERT INTO build_batched_configs (
	build_id, transform_type, input_dataset_rid, input_branch, input_version, input_path, all_files, predicate,
	output_dataset_rid, output_branch, logical_path, job_spec_rid, job_spec_content_hash, actor_subject,
	stage, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'PREPARING',NOW(),NOW())`,
			buildID, cfg.TransformType, cfg.InputDatasetRID, cfg.InputBranch, cfg.InputVersion, cfg.InputPath, cfg.AllFiles, cfg.Predicate,
			cfg.OutputDatasetRID, cfg.OutputBranch, cfg.LogicalPath, cfg.JobSpecRID, cfg.JobSpecContentHash, cfg.ActorSubject)
		return err
	}
	return r.inTx(ctx, exec)
}

// ResolveBatchedPrepared moves PREPARING → PENDING and records the opened
// reservation RID. Called inside the PersistResolvedBuild transaction.
func ResolveBatchedPrepared(ctx context.Context, db DB, buildID uuid.UUID, reservationRID string) error {
	_, err := db.Exec(ctx, `UPDATE build_batched_configs
SET stage='PENDING', reservation_rid=$2, updated_at=NOW()
WHERE build_id=$1 AND stage='PREPARING'`, buildID, reservationRID)
	return err
}

// FailBatchedPreparing moves a stuck PREPARING row to FAILED (CreateBuild
// error paths). No-op for builds without a batched configuration.
func (r *Repository) FailBatchedPreparing(ctx context.Context, buildID uuid.UUID, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE build_batched_configs
SET stage='FAILED', error_message=$2, lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW()
WHERE build_id=$1 AND stage='PREPARING'`, buildID, reason)
	return err
}

// ReleaseBatchedLease drops the caller's lease WITHOUT a terminal stage
// (retryable pre-intent failure). Fence-checked.
func (r *Repository) ReleaseBatchedLease(ctx context.Context, buildID, owner uuid.UUID) error {
	tag, err := r.db.Exec(ctx, `UPDATE build_batched_configs
SET lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2`, buildID, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBatchedFenceLost
	}
	return nil
}

// AcquireBatchedRun takes the execution lease for a fresh (or restartable
// pre-intent) run: PENDING/STAGING/STAGED with a free or expired lease.
// The same transaction flips the build to BUILD_RUNNING and drives the
// job rows WAITING → RUN_PENDING → RUNNING so the audit trail stays
// lifecycle-valid.
func (r *Repository) AcquireBatchedRun(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) (*models.BatchedBuildConfig, error) {
	return r.acquireBatched(ctx, buildID, owner, leaseTTL, []string{models.BatchedStagePending, models.BatchedStageStaging, models.BatchedStageStaged}, "STAGING")
}

// AcquireBatchedReplay takes the execution lease for an exact-byte replay:
// PUBLISHING/OUTCOME_UNKNOWN with a free or expired lease (a live lease
// means an attempt is in flight → conflict).
func (r *Repository) AcquireBatchedReplay(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) (*models.BatchedBuildConfig, error) {
	return r.acquireBatched(ctx, buildID, owner, leaseTTL, []string{models.BatchedStagePublishing, models.BatchedStageOutcomeUnknown}, "PUBLISHING")
}

func (r *Repository) acquireBatched(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration, from []string, targetStage string) (*models.BatchedBuildConfig, error) {
	var cfg *models.BatchedBuildConfig
	err := r.inTx(ctx, func(ctx context.Context, db DB) error {
		// Serialize against abort decisions and other acquire attempts on
		// the same row before reading the stage (the build state joins in
		// so queued builds are refused before any transition).
		current, err := scanBatchedConfig(db.QueryRow(ctx, `SELECT `+batchedConfigColumns+`
FROM build_batched_configs c JOIN builds b ON b.id=c.build_id WHERE c.build_id=$1 FOR UPDATE OF c`, buildID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrBatchedNotFound
		}
		if err != nil {
			return err
		}
		var buildState string
		if err := db.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID).Scan(&buildState); err != nil {
			return err
		}
		if buildState == "BUILD_QUEUED" && targetStage == "STAGING" {
			// Queued batched builds never auto-start (no scheduler): the
			// empty-plan legacy fallback is refused explicitly.
			return ErrBatchedQueued
		}
		switch current.Stage {
		case models.BatchedStagePreparing:
			return ErrBatchedPreparing
		case models.BatchedStageDone, models.BatchedStageFailed, models.BatchedStageAborted:
			return ErrBatchedTerminal
		case models.BatchedStagePublishing, models.BatchedStageOutcomeUnknown:
			if targetStage != "PUBLISHING" {
				// Run entry must not silently skip a persisted intent.
				return ErrBatchedPublishing
			}
		}
		allowed := false
		for _, stage := range from {
			if current.Stage == stage {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("batched_stage_conflict: stage %s does not admit this operation", current.Stage)
		}
		if current.LeaseExpiresAt != nil && current.LeaseExpiresAt.After(time.Now()) {
			return ErrBatchedLeaseHeld
		}
		tag, err := db.Exec(ctx, `UPDATE build_batched_configs
SET stage=$3, lease_owner=$2, lease_expires_at=NOW() + make_interval(secs => $4), updated_at=NOW()
WHERE build_id=$1 AND stage=$5 AND (lease_expires_at IS NULL OR lease_expires_at < NOW())`,
			buildID, owner, targetStage, leaseTTL.Seconds(), current.Stage)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrBatchedLeaseHeld
		}
		if targetStage == "STAGING" {
			// WAITING → RUN_PENDING → RUNNING keeps joblifecycle valid;
			// each hop lands in the append-only transition trail.
			if err := transitionBuildJobs(ctx, db, buildID, models.JobWaiting, models.JobRunPending, "batched execution lease acquired"); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobRunPending, models.JobRunning, "batched execution started"); err != nil {
				return err
			}
			if _, err := db.Exec(ctx, `UPDATE builds SET state='BUILD_RUNNING', started_at=COALESCE(started_at, NOW()) WHERE id=$1 AND state IN ('BUILD_RESOLUTION','BUILD_QUEUED')`, buildID); err != nil {
				return err
			}
		}
		updated, err := scanBatchedConfig(db.QueryRow(ctx, `SELECT `+batchedConfigColumns+`
FROM build_batched_configs c WHERE c.build_id=$1 FOR UPDATE`, buildID))
		if err != nil {
			return err
		}
		cfg = updated
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// transitionBuildJobs moves every job of a build between two states in the
// caller's transaction, recording the audit rows the lifecycle trail
// expects. Rows already past the from-state are skipped idempotently.
func transitionBuildJobs(ctx context.Context, db DB, buildID uuid.UUID, from, to models.JobState, reason string) error {
	rows, err := db.Query(ctx, `SELECT id FROM jobs WHERE build_id=$1 AND state=$2`, buildID, string(from))
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := db.Exec(ctx, `UPDATE jobs SET state=$3, state_changed_at=NOW() WHERE id=$1 AND state=$2`, id, string(from), string(to)); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, `INSERT INTO job_state_transitions (job_id, from_state, to_state, reason) VALUES ($1,$2,$3,$4)`, id, string(from), string(to), reason); err != nil {
			return err
		}
	}
	return nil
}

// RenewBatchedLease extends the caller's live lease (heartbeat for long
// stage streams). Fence-checked; a lost fence is an error, never a no-op.
func (r *Repository) RenewBatchedLease(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) error {
	tag, err := r.db.Exec(ctx, `UPDATE build_batched_configs
SET lease_expires_at=NOW() + make_interval(secs => $3), updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2 AND lease_expires_at > NOW()`, buildID, owner, leaseTTL.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBatchedFenceLost
	}
	return nil
}

// MarkBatchedStaged records the verified sealed staged object
// (STAGING → STAGED), fence-checked.
func (r *Repository) MarkBatchedStaged(ctx context.Context, buildID, owner uuid.UUID, stagedObjectID, sha256 string, size, records int64) error {
	tag, err := r.db.Exec(ctx, `UPDATE build_batched_configs
SET stage='STAGED', staged_object_id=$3, staged_sha256=$4, staged_size_bytes=$5, staged_records=$6, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2 AND stage='STAGING' AND lease_expires_at > NOW()`,
		buildID, owner, stagedObjectID, sha256, size, records)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBatchedFenceLost
	}
	return nil
}

// PersistBatchedCommitIntent is THE publish boundary: the exact commit
// request bytes and the stable idempotency key land durably
// (STAGED → PUBLISHING). From this instant abort is always refused and
// every unresolved outcome stays publish-pending for exact-byte replay.
// Fence-checked.
func (r *Repository) PersistBatchedCommitIntent(ctx context.Context, buildID, owner uuid.UUID, key string, body []byte) error {
	tag, err := r.db.Exec(ctx, `UPDATE build_batched_configs
SET stage='PUBLISHING', commit_idempotency_key=$3, commit_request_body=$4, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2 AND stage='STAGED' AND lease_expires_at > NOW() AND abort_requested=FALSE`,
		buildID, owner, key, body)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Distinguish a concurrent abort request from a lost fence.
		current, err := scanBatchedConfig(r.db.QueryRow(ctx, `SELECT `+batchedConfigColumns+`
FROM build_batched_configs c WHERE c.build_id=$1`, buildID))
		if err != nil {
			return err
		}
		if current != nil && current.AbortRequested {
			return ErrBatchedPublishing // refused; treated as aborted-pending below by caller
		}
		return ErrBatchedFenceLost
	}
	return nil
}

// MarkBatchedOutcomeUnknown records an unresolved publish outcome. The
// exact body, lock and pending references are all retained; permanent
// conflict hints ride the error message. The lease is RELEASED: the
// OUTCOME_UNKNOWN state itself is the durable marker, and a held lease
// would block the exact-byte replay for a full TTL.
func (r *Repository) MarkBatchedOutcomeUnknown(ctx context.Context, buildID, owner uuid.UUID, reason string) error {
	tag, err := r.db.Exec(ctx, `UPDATE build_batched_configs
SET stage='OUTCOME_UNKNOWN', error_message=$3, lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2 AND stage='PUBLISHING'`, buildID, owner, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBatchedFenceLost
	}
	return nil
}

// FinalizeBatchedCommit applies a verified commit ACK atomically: the
// config row goes DONE with the receipt bytes and the ACTUAL published
// transaction RID, the job completes, job_outputs is repointed to the
// published transaction, the build completes, and this build's lock and
// pending references are released — one transaction, all or nothing.
// Fence-checked.
func (r *Repository) FinalizeBatchedCommit(ctx context.Context, buildID, owner uuid.UUID, responseBody []byte, publishedTxnRID string) error {
	return r.inTx(ctx, func(ctx context.Context, db DB) error {
		var outputDatasetRID string
		err := db.QueryRow(ctx, `UPDATE build_batched_configs
SET stage='DONE', commit_response_body=$3, published_transaction_rid=$4, error_message='', lease_owner=NULL, lease_expires_at=NULL, abort_requested=FALSE, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2 AND stage IN ('PUBLISHING','OUTCOME_UNKNOWN')
RETURNING output_dataset_rid`, buildID, owner, responseBody, publishedTxnRID).Scan(&outputDatasetRID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrBatchedFenceLost
		}
		if err != nil {
			return err
		}
		if err := transitionBuildJobs(ctx, db, buildID, models.JobRunning, models.JobCompleted, "batched output published"); err != nil {
			return err
		}
		// job_outputs rows carry the JobSpec's VERBATIM output spelling
		// (bare UUID or RID); match by UUID-equivalent pair so a spelling
		// divergence can never leave the reservation in place.
		outputUUID, ok := datasetUUIDFromRef(outputDatasetRID)
		if !ok {
			return fmt.Errorf("batched_finalize_invalid_output_ref: %q", outputDatasetRID)
		}
		bare := outputUUID.String()
		// The reservation spelling is whatever resolution actually recorded —
		// read it BEFORE job_outputs is repointed below.
		reservationRID := reservationRIDForJobs(ctx, db, buildID, bare, outputDatasetRID)
		repointTag, err := db.Exec(ctx, `UPDATE job_outputs jo SET transaction_rid=$3, committed=TRUE, aborted=FALSE
FROM jobs j WHERE jo.job_id=j.id AND j.build_id=$1 AND jo.output_dataset_rid IN ($2, $4) AND jo.transaction_rid=$5`,
			buildID, outputDatasetRID, publishedTxnRID, bare, reservationRID)
		if err != nil {
			return err
		}
		if repointTag.RowsAffected() != 1 {
			// Zero or multiple updates mean the bookkeeping diverged from
			// the frozen plan: roll back and stay PUBLISHING (the exact
			// body remains replayable) instead of finalizing a lie.
			return fmt.Errorf("batched_finalize_job_outputs_mismatch: expected exactly one job output on reservation %s, updated %d", reservationRID, repointTag.RowsAffected())
		}
		// The public job GET/list surface reads jobs.output_transaction_rids:
		// flip the reservation array to the ACTUAL published transaction in
		// the same transaction.
		if reservationRID != "" {
			arrayTag, err := db.Exec(ctx, `UPDATE jobs SET output_transaction_rids=ARRAY[$3] WHERE build_id=$1 AND $2=ANY(output_transaction_rids)`, buildID, reservationRID, publishedTxnRID)
			if err != nil {
				return err
			}
			if arrayTag.RowsAffected() != 1 {
				return fmt.Errorf("batched_finalize_jobs_array_mismatch: expected exactly one job holding reservation %s, updated %d", reservationRID, arrayTag.RowsAffected())
			}
		}
		if _, err := db.Exec(ctx, `UPDATE builds SET state='BUILD_COMPLETED', finished_at=NOW() WHERE id=$1 AND state IN ('BUILD_RESOLUTION','BUILD_QUEUED','BUILD_RUNNING')`, buildID); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, `DELETE FROM build_input_locks WHERE build_id=$1`, buildID); err != nil {
			return err
		}
		_, err = db.Exec(ctx, `DELETE FROM build_pending_output_txns WHERE build_id=$1`, buildID)
		return err
	})
}

// reservationRIDForJobs returns the transaction RID currently recorded on
// the build's job rows (the reservation), matched through job_outputs so
// the spelling is whatever resolution actually wrote.
func reservationRIDForJobs(ctx context.Context, db DB, buildID uuid.UUID, bare, canonical string) string {
	var txnRID string
	err := db.QueryRow(ctx, `SELECT jo.transaction_rid FROM job_outputs jo JOIN jobs j ON j.id=jo.job_id
WHERE j.build_id=$1 AND jo.output_dataset_rid IN ($2, $3) LIMIT 1`, buildID, bare, canonical).Scan(&txnRID)
	if err != nil {
		return ""
	}
	return txnRID
}

// FailBatchedBuild terminates a PRE-INTENT run: jobs FAILED, build
// BUILD_FAILED, lease dropped. Lock/pending cleanup is the caller's
// precise-cancel business (see handler); this write only records the
// terminal stage, fence-checked.
func (r *Repository) FailBatchedBuild(ctx context.Context, buildID, owner uuid.UUID, reason string) error {
	// One transaction: config FAILED + jobs FAILED + build BUILD_FAILED
	// land together or not at all (a partial write would strand a RUNNING
	// build over a FAILED config).
	return r.inTx(ctx, func(ctx context.Context, db DB) error {
		tag, err := db.Exec(ctx, `UPDATE build_batched_configs
SET stage='FAILED', error_message=$3, lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2 AND stage IN ('STAGING','STAGED')`, buildID, owner, reason)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrBatchedFenceLost
		}
		if err := transitionBuildJobs(ctx, db, buildID, models.JobRunning, models.JobFailed, reason); err != nil {
			return err
		}
		if err := transitionBuildJobs(ctx, db, buildID, models.JobRunPending, models.JobFailed, reason); err != nil {
			return err
		}
		// WAITING → FAILED is not a lifecycle edge: hop RUN_PENDING.
		if err := transitionBuildJobs(ctx, db, buildID, models.JobWaiting, models.JobRunPending, reason); err != nil {
			return err
		}
		if err := transitionBuildJobs(ctx, db, buildID, models.JobRunPending, models.JobFailed, reason); err != nil {
			return err
		}
		_, execErr := db.Exec(ctx, `UPDATE builds SET state='BUILD_FAILED', error_message=$2, finished_at=NOW() WHERE id=$1 AND state IN ('BUILD_RESOLUTION','BUILD_QUEUED','BUILD_RUNNING')`, buildID, reason)
		return execErr
	})
}

// BeginBatchedAbort serializes the abort decision against execute leases
// on the config row lock and returns what the caller must do. When the
// lease is live the abort is only RECORDED (abort_requested) — the
// in-flight attempt completes the exact cancel at its next checkpoint;
// when the lease is free/expired the abort takes the fence itself and the
// caller performs the bounded precise cancel. Once the commit intent
// exists (PUBLISHING/OUTCOME_UNKNOWN) the answer is always conflict.
func (r *Repository) BeginBatchedAbort(ctx context.Context, idOrRID string, takeoverOwner uuid.UUID, leaseTTL time.Duration) (*handler.BatchedAbortDecision, error) {
	var decision *handler.BatchedAbortDecision
	err := r.inTx(ctx, func(ctx context.Context, db DB) error {
		where := "WHERE (b.id::text=$1 OR b.rid=$1)"
		args := []any{idOrRID}
		if scope := scopeFromContext(ctx); scope != nil {
			sb := &sqlBuilder{args: args}
			appendTenantClause(sb, scope, "builds b")
			where += sb.whereSQL()
			args = sb.args
		}
		cfg, err := scanBatchedConfig(db.QueryRow(ctx, `SELECT `+batchedConfigColumns+`
FROM build_batched_configs c JOIN builds b ON b.id=c.build_id `+where+` FOR UPDATE OF c`, args...))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrBatchedNotFound
		}
		if err != nil {
			return err
		}
		leaseLive := cfg.LeaseExpiresAt != nil && cfg.LeaseExpiresAt.After(time.Now())
		switch cfg.Stage {
		case models.BatchedStagePreparing:
			decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindPreparing, Config: cfg}
			return nil
		case models.BatchedStagePublishing, models.BatchedStageOutcomeUnknown:
			permanent := cfg.Stage == models.BatchedStageOutcomeUnknown && cfg.ErrorMessage != ""
			decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindPublishing, Config: cfg, Permanent: permanent, Detail: cfg.ErrorMessage}
			return nil
		case models.BatchedStageDone:
			decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindDone, Config: cfg}
			return nil
		case models.BatchedStageAborted:
			decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindAborted, Config: cfg}
			return nil
		case models.BatchedStageFailed:
			// Terminal, but a dangling reservation may remain. With a LIVE
			// lease the failing attempt is still cleaning up — the abort
			// must not hand out a fence it does not hold; only an expired
			// lease is taken over for the exact cancel.
			if leaseLive {
				decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindFailedActive, Config: cfg}
				return nil
			}
			if _, err := db.Exec(ctx, `UPDATE build_batched_configs SET lease_owner=$2, lease_expires_at=NOW() + make_interval(secs => $3), abort_requested=TRUE, updated_at=NOW() WHERE build_id=$1 AND (lease_expires_at IS NULL OR lease_expires_at < NOW())`,
				cfg.BuildID, takeoverOwner, leaseTTL.Seconds()); err != nil {
				return err
			}
			decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindFailedRetry, Config: cfg, Owner: takeoverOwner}
			return nil
		}
		// PENDING / STAGING / STAGED.
		if leaseLive {
			if _, err := db.Exec(ctx, `UPDATE build_batched_configs SET abort_requested=TRUE, updated_at=NOW() WHERE build_id=$1`, cfg.BuildID); err != nil {
				return err
			}
			decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindFlagged, Config: cfg}
			return nil
		}
		tag, err := db.Exec(ctx, `UPDATE build_batched_configs
SET lease_owner=$2, lease_expires_at=NOW() + make_interval(secs => $3), abort_requested=TRUE, updated_at=NOW()
WHERE build_id=$1 AND (lease_expires_at IS NULL OR lease_expires_at < NOW())`, cfg.BuildID, takeoverOwner, leaseTTL.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrBatchedLeaseHeld
		}
		decision = &handler.BatchedAbortDecision{Kind: handler.BatchedAbortKindTakeover, Config: cfg, Owner: takeoverOwner}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return decision, nil
}

// CompleteBatchedAbort records a confirmed precise cancel (stage ABORTED,
// jobs/build terminal, this build's lock and pending references released)
// in one transaction. Fence-checked; a lost fence means the new owner is
// responsible and this call is a clean no-op error.
func (r *Repository) CompleteBatchedAbort(ctx context.Context, buildID, owner uuid.UUID, keepFailed bool, cfgOutputDatasetRID string) error {
	return r.inTx(ctx, func(ctx context.Context, db DB) error {
		targetStage := "ABORTED"
		jobState := models.JobAborted
		reason := "batched build aborted before publish"
		if keepFailed {
			targetStage = "FAILED"
			jobState = models.JobFailed
			reason = "batched build failure cleanup"
		}
		tag, err := db.Exec(ctx, `UPDATE build_batched_configs
SET stage=$3, lease_owner=NULL, lease_expires_at=NULL, abort_requested=FALSE, updated_at=NOW()
WHERE build_id=$1 AND lease_owner=$2`, buildID, owner, targetStage)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrBatchedFenceLost
		}
		if jobState == models.JobAborted {
			// RUNNING/RUN_PENDING → ABORTED must pass ABORT_PENDING
			// (joblifecycle); WAITING → ABORTED is a legal direct edge.
			if err := transitionBuildJobs(ctx, db, buildID, models.JobRunning, models.JobAbortPending, reason); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobRunPending, models.JobAbortPending, reason); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobAbortPending, models.JobAborted, reason); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobWaiting, models.JobAborted, reason); err != nil {
				return err
			}
		} else {
			// Failure terminal: RUNNING/RUN_PENDING → FAILED are legal;
			// WAITING must hop through RUN_PENDING (WAITING→FAILED is not
			// in the lifecycle).
			if err := transitionBuildJobs(ctx, db, buildID, models.JobRunning, models.JobFailed, reason); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobRunPending, models.JobFailed, reason); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobWaiting, models.JobRunPending, reason); err != nil {
				return err
			}
			if err := transitionBuildJobs(ctx, db, buildID, models.JobRunPending, models.JobFailed, reason); err != nil {
				return err
			}
		}
		// The build row flips in the SAME transaction as the config/job
		// rows and the reference cleanup — neither terminal may leave the
		// build RUNNING while everything else says ABORTED/FAILED.
		if keepFailed {
			if _, err := db.Exec(ctx, `UPDATE builds SET state='BUILD_FAILED', error_message=$2, finished_at=NOW() WHERE id=$1 AND state IN ('BUILD_RESOLUTION','BUILD_QUEUED','BUILD_RUNNING')`, buildID, reason); err != nil {
				return err
			}
		} else {
			if _, err := db.Exec(ctx, `UPDATE builds SET state='BUILD_ABORTED', error_message='aborted by user', finished_at=NOW() WHERE id=$1 AND state IN ('BUILD_RESOLUTION','BUILD_QUEUED','BUILD_RUNNING','BUILD_ABORTING')`, buildID); err != nil {
				return err
			}
		}
		// The confirmed-cancel terminal also flags this build's job_outputs
		// row aborted (the multi-output audit invariant reads these flags)
		// in the SAME transaction — matched through the UUID pair because
		// the row may carry the spec's verbatim bare-UUID spelling.
		outputUUID, outputOK := datasetUUIDFromRef(cfgOutputDatasetRID)
		if !outputOK {
			return fmt.Errorf("batched_abort_invalid_output_ref: %q", cfgOutputDatasetRID)
		}
		outputTag, err := db.Exec(ctx, `UPDATE job_outputs jo SET aborted=TRUE, committed=FALSE
FROM jobs j WHERE jo.job_id=j.id AND j.build_id=$1 AND jo.output_dataset_rid IN ($2, $3)`,
			buildID, cfgOutputDatasetRID, outputUUID.String())
		if err != nil {
			return err
		}
		if outputTag.RowsAffected() != 1 {
			return fmt.Errorf("batched_abort_job_outputs_mismatch: expected exactly one job output, updated %d", outputTag.RowsAffected())
		}
		if _, err := db.Exec(ctx, `DELETE FROM build_input_locks WHERE build_id=$1`, buildID); err != nil {
			return err
		}
		_, err = db.Exec(ctx, `DELETE FROM build_pending_output_txns WHERE build_id=$1`, buildID)
		return err
	})
}

// inTx runs fn in one transaction when the underlying DB supports it
// (mirrors PersistResolvedBuild's pattern).
func (r *Repository) inTx(ctx context.Context, fn func(ctx context.Context, db DB) error) error {
	if beginner, ok := r.db.(txBeginner); ok {
		tx, err := beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := fn(ctx, tx.(DB)); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	return fn(ctx, r.db)
}
