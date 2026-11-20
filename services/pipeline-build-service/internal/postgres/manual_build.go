package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// The HTTP path requires the authenticated creator and tenant even when a
// caller reaches these methods only with a persisted build ID.
func manualBuildOwner(ctx context.Context, buildID uuid.UUID) (*sqlBuilder, error) {
	scope := scopeFromContext(ctx)
	if scope == nil || scope.Subject == uuid.Nil {
		return nil, errors.New("manual build requires an authenticated creator")
	}
	sb := &sqlBuilder{args: []any{buildID, scope.Subject.String()}}
	sb.and("b.requested_by=$2")
	sb.and("b.trigger_kind IN ('MANUAL','FORCE')")
	appendTenantClause(sb, scope, "builds b")
	return sb, nil
}

func (r *Repository) ListManualBuildOutputs(ctx context.Context, buildID uuid.UUID) ([]handler.ManualBuildOutput, error) {
	sb, err := manualBuildOwner(ctx, buildID)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, `SELECT j.id, p.dataset_rid, p.branch, p.transaction_rid
FROM build_pending_output_txns p
JOIN builds b ON b.id=p.build_id AND b.build_branch=p.branch
JOIN build_input_locks l ON l.build_id=p.build_id AND l.output_dataset_rid=p.dataset_rid AND l.transaction_rid=p.transaction_rid
JOIN jobs j ON j.build_id=b.id
JOIN job_outputs o ON o.job_id=j.id AND o.output_dataset_rid=p.dataset_rid AND o.transaction_rid=p.transaction_rid
WHERE b.id=$1 AND NOT o.committed AND NOT o.aborted`+sb.whereSQL(), sb.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var outputs []handler.ManualBuildOutput
	for rows.Next() {
		var o handler.ManualBuildOutput
		if err := rows.Scan(&o.JobID, &o.DatasetRID, &o.Branch, &o.TransactionRID); err != nil {
			return nil, err
		}
		outputs = append(outputs, o)
	}
	return outputs, rows.Err()
}

func (r *Repository) MoveManualBuild(ctx context.Context, buildID uuid.UUID, from, to models.BuildState) (bool, error) {
	sb, err := manualBuildOwner(ctx, buildID)
	if err != nil {
		return false, err
	}
	next := sb.placeholder(to)
	current := sb.placeholder(from)
	tag, err := r.db.Exec(ctx, `UPDATE builds b SET state=`+next+`,
started_at=CASE WHEN `+next+`='BUILD_RUNNING' THEN NOW() ELSE started_at END
WHERE b.id=$1 AND b.state=`+current+sb.whereSQL(), sb.args...)
	return tag.RowsAffected() == 1, err
}

func (r *Repository) SettleManualBuildOutput(ctx context.Context, buildID uuid.UUID, output handler.ManualBuildOutput, published string) error {
	return r.SettleManualBuildOutputs(ctx, buildID, []handler.ManualBuildPublication{{Output: output, TransactionRID: published}})
}

func (r *Repository) SettleManualBuildOutputs(ctx context.Context, buildID uuid.UUID, publications []handler.ManualBuildPublication) error {
	sb, err := manualBuildOwner(ctx, buildID)
	if err != nil {
		return err
	}
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return errors.New("manual output persistence requires a database transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackManualBuild(ctx, tx)
	// Serialize PBS receipt recovery, cleanup and terminalization for the
	// build, without holding a local row lock across a network call.
	var state string
	if err := tx.QueryRow(ctx, `SELECT b.state FROM builds b WHERE b.id=$1`+sb.whereSQL()+` FOR UPDATE`, sb.args...).Scan(&state); err != nil {
		return err
	}
	for _, publication := range publications {
		if err := settleManualBuildOutput(ctx, tx, buildID, state, publication.Output, publication.TransactionRID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func settleManualBuildOutput(ctx context.Context, tx pgx.Tx, buildID uuid.UUID, state string, output handler.ManualBuildOutput, published string) error {
	var current string
	var committed, aborted bool
	if err := tx.QueryRow(ctx, `SELECT o.transaction_rid, o.committed, o.aborted
FROM job_outputs o JOIN jobs j ON j.id=o.job_id
WHERE j.build_id=$1 AND j.id=$2 AND o.output_dataset_rid=$3`,
		buildID, output.JobID, output.DatasetRID).Scan(&current, &committed, &aborted); err != nil {
		return err
	}
	if (published != "" && current == published && committed && !aborted) ||
		(published == "" && current == output.TransactionRID && !committed && aborted) {
		return nil
	}
	if (state != string(models.BuildRunning) && state != string(models.BuildAborting)) ||
		current != output.TransactionRID || committed || aborted {
		return fmt.Errorf("manual output %s no longer matches its pending reservation", output.DatasetRID)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM build_pending_output_txns
WHERE build_id=$1 AND dataset_rid=$2 AND branch=$3 AND transaction_rid=$4`,
		buildID, output.DatasetRID, output.Branch, output.TransactionRID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("manual output %s has no owned pending reservation", output.DatasetRID)
	}
	tag, err = tx.Exec(ctx, `DELETE FROM build_input_locks
WHERE build_id=$1 AND output_dataset_rid=$2 AND transaction_rid=$3`,
		buildID, output.DatasetRID, output.TransactionRID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("manual output %s has no owned reservation lock", output.DatasetRID)
	}
	if published == "" {
		_, err = tx.Exec(ctx, `UPDATE job_outputs SET aborted=TRUE WHERE job_id=$1 AND output_dataset_rid=$2`, output.JobID, output.DatasetRID)
	} else {
		_, err = tx.Exec(ctx, `UPDATE job_outputs SET committed=TRUE, transaction_rid=$3 WHERE job_id=$1 AND output_dataset_rid=$2`, output.JobID, output.DatasetRID, published)
		if err == nil {
			tag, err = tx.Exec(ctx, `UPDATE jobs SET output_transaction_rids=array_replace(output_transaction_rids,$2,$3)
WHERE id=$1 AND $2=ANY(output_transaction_rids)`, output.JobID, output.TransactionRID, published)
			if err == nil && tag.RowsAffected() != 1 {
				return fmt.Errorf("manual output %s has no matching job reservation", output.DatasetRID)
			}
		}
	}
	return err
}

func (r *Repository) FinishManualBuild(ctx context.Context, buildID uuid.UUID, from models.BuildState, outcome executor.Outcome) (bool, error) {
	sb, err := manualBuildOwner(ctx, buildID)
	if err != nil {
		return false, err
	}
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return false, errors.New("manual build finalization requires a database transaction")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackManualBuild(ctx, tx)
	next := sb.placeholder(outcome.FinalState)
	current := sb.placeholder(from)
	var message string
	for _, reason := range outcome.Reasons {
		if message == "" || reason < message {
			message = reason
		}
	}
	reason := sb.placeholder(message)
	tag, err := tx.Exec(ctx, `UPDATE builds b SET state=`+next+`, finished_at=NOW(), error_message=NULLIF(`+reason+`,'')
WHERE b.id=$1 AND b.state=`+current+sb.whereSQL()+`
AND NOT EXISTS (SELECT 1 FROM build_pending_output_txns p WHERE p.build_id=b.id)`, sb.args...)
	if err != nil || tag.RowsAffected() == 0 {
		return false, err
	}
	for specRID, state := range outcome.Nodes {
		// Reconcile the exact executor outcome with its audit transitions.
		// The transition context may have expired after a DVS receipt landed.
		_, err := tx.Exec(ctx, `UPDATE jobs SET state=$3, state_changed_at=NOW(),
attempt=GREATEST(attempt,$4), failure_reason=NULLIF($5,'')
WHERE build_id=$1 AND job_spec_rid=$2 AND state<>$3`,
			buildID, specRID, state, outcome.Attempts[specRID], outcome.Reasons[specRID])
		if err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
}

func (r *Repository) RecoverManualBuildCompletion(ctx context.Context, buildID uuid.UUID) (bool, error) {
	sb, err := manualBuildOwner(ctx, buildID)
	if err != nil {
		return false, err
	}
	// A receipt can be recovered after the process dies between DVS COMMIT
	// and PBS persistence. Only a committed output for each unfinished job
	// proves its runner completed. A failed/aborted output can never turn
	// into a success through this path.
	tag, err := r.db.Exec(ctx, `
WITH completed AS (
  UPDATE builds b SET state='BUILD_COMPLETED', finished_at=NOW()
  WHERE b.id=$1 AND b.state IN ('BUILD_RUNNING','BUILD_ABORTING')`+sb.whereSQL()+`
    AND EXISTS (SELECT 1 FROM jobs j WHERE j.build_id=b.id)
    AND NOT EXISTS (SELECT 1 FROM build_pending_output_txns p WHERE p.build_id=b.id)
    AND NOT EXISTS (
      SELECT 1 FROM jobs j WHERE j.build_id=b.id AND (
        EXISTS (SELECT 1 FROM job_outputs o WHERE o.job_id=j.id AND NOT o.committed) OR
        (j.state<>'COMPLETED' AND NOT EXISTS (SELECT 1 FROM job_outputs o WHERE o.job_id=j.id))
      )
    )
  RETURNING b.id
)
UPDATE jobs j SET state='COMPLETED', state_changed_at=NOW(), attempt=GREATEST(j.attempt,1), failure_reason=NULL
FROM completed c WHERE j.build_id=c.id`, sb.args...)
	return tag.RowsAffected() > 0, err
}

func rollbackManualBuild(ctx context.Context, tx pgx.Tx) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(cleanupCtx)
}
