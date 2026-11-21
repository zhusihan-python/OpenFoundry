// Package postgres contains production Postgres adapters for pipeline-build-service.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	livellogs "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/logs"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
	sparkpkg "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/spark"
)

// DB is the pgx/pgxmock surface used by the repository. *pgxpool.Pool and
// pgxmock.PgxPoolIface both satisfy it.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Repository implements the production persistence ports used by the handlers.
type Repository struct {
	db DB
	// datasetOrgOnce lazily probes whether the co-located dataset schema
	// carries the org/owner columns (dataset-versioning P1.1). Until that
	// migration has been applied — or in a split-database deployment where
	// the dataset tables are foreign — the dataset-touching helpers keep
	// their legacy SQL instead of referencing a column that may not exist.
	datasetOrgOnce    sync.Once
	datasetOrgCapable bool
}

// DatasetTenantBoundarySupported reports whether this repository can
// enforce the tenant boundary on the co-located dataset schema (the
// columns exist and the probe succeeded). Handlers use it to reject
// scoped build-resolution requests with a stable 503 BEFORE any database
// write when the deployment cannot enforce the boundary.
func (r *Repository) DatasetTenantBoundarySupported(ctx context.Context) bool {
	return r.datasetOrgColumns(ctx)
}

// datasetOrgColumns reports whether datasets.org_id / datasets.owner_id
// exist in the current schema. Probed once per repository instance; a
// failed probe falls back to the legacy unfiltered SQL.
func (r *Repository) datasetOrgColumns(ctx context.Context) bool {
	r.datasetOrgOnce.Do(func() {
		const probe = `SELECT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema = current_schema() AND table_name = 'datasets'
    AND column_name IN ('org_id', 'owner_id')
  HAVING COUNT(*) = 2)`
		var capable bool
		if err := r.db.QueryRow(ctx, probe).Scan(&capable); err == nil {
			r.datasetOrgCapable = capable
		}
	})
	return r.datasetOrgCapable
}

func NewRepository(db DB) *Repository { return &Repository{db: db} }

func NewRepositoryFromPool(pool *pgxpool.Pool) *Repository { return NewRepository(pool) }

// Lookup implements resolver.JobSpecRepository.
func (r *Repository) Lookup(ctx context.Context, pipelineRID, outputDatasetRID, buildBranch string, fallbackChain []string) (*models.JobSpec, error) {
	branches := append([]string{buildBranch}, fallbackChain...)
	for _, branch := range branches {
		branch = strings.TrimSpace(branch)
		if branch == "" {
			continue
		}
		spec, err := r.lookupJobSpecOnBranch(ctx, pipelineRID, outputDatasetRID, branch)
		if err != nil {
			return nil, err
		}
		if spec != nil {
			return spec, nil
		}
	}
	return nil, nil
}

func (r *Repository) lookupJobSpecOnBranch(ctx context.Context, pipelineRID, outputDatasetRID, branch string) (*models.JobSpec, error) {
	where := "pipeline_rid=$1 AND output_dataset_rid=$2 AND branch_name=$3"
	args := []any{pipelineRID, outputDatasetRID, branch}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipeline_job_specs")
		where += sb.whereSQL()
		args = sb.args
	}
	var spec models.JobSpec
	var inputsRaw, jobSpecRaw []byte
	var singleOutput string
	err := r.db.QueryRow(ctx, `SELECT COALESCE(job_spec_rid, rid), pipeline_rid, branch_name, inputs, output_dataset_rid, job_spec_json, content_hash
FROM pipeline_job_specs
WHERE `+where, args...).Scan(&spec.RID, &spec.PipelineRID, &spec.BranchName, &inputsRaw, &singleOutput, &jobSpecRaw, &spec.ContentHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(inputsRaw) > 0 {
		if err := json.Unmarshal(inputsRaw, &spec.Inputs); err != nil {
			return nil, fmt.Errorf("decode job spec inputs: %w", err)
		}
	}
	var body struct {
		LogicKind         string          `json:"logic_kind"`
		LogicPayload      json.RawMessage `json:"logic_payload"`
		OutputDatasetRIDs []string        `json:"output_dataset_rids"`
	}
	if len(jobSpecRaw) > 0 {
		if err := json.Unmarshal(jobSpecRaw, &body); err != nil {
			return nil, fmt.Errorf("decode job_spec_json: %w", err)
		}
	}
	spec.LogicKind = body.LogicKind
	spec.LogicPayload = body.LogicPayload
	spec.OutputDatasetRIDs = body.OutputDatasetRIDs
	if len(spec.OutputDatasetRIDs) == 0 {
		spec.OutputDatasetRIDs = []string{singleOutput}
	}
	return &spec, nil
}

// ListBranches implements resolver.DatasetVersioningRepository using the
// dataset-versioning schema when it is co-located in the same database.
func (r *Repository) ListBranches(ctx context.Context, datasetRID string) ([]models.BranchSnapshot, error) {
	where := "b.dataset_rid=$1 AND b.deleted_at IS NULL"
	args := []any{datasetRID}
	if scope := scopeFromContext(ctx); scope != nil {
		if !r.datasetOrgColumns(ctx) {
			return nil, handler.ErrDatasetTenantBoundaryUnsupported
		}
		sb := &sqlBuilder{args: args}
		appendDatasetOrgClause(sb, scope)
		where += sb.whereSQL()
		args = sb.args
		// Explicit ownership check: a scoped caller referencing a
		// dataset that is not visible (foreign tenant / absent) gets a
		// precise 404 — an empty-but-legitimate branch list for an OWNED
		// dataset still returns an empty slice, never an error.
		dsBuilder := &sqlBuilder{args: []any{datasetRID}}
		dsWhere := "d.rid=$1"
		appendDatasetOrgClause(dsBuilder, scope)
		dsWhere += dsBuilder.whereSQL()
		var visible bool
		if err := r.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM datasets d WHERE `+dsWhere+`)`, dsBuilder.args...).Scan(&visible); err != nil {
			return nil, err
		}
		if !visible {
			return nil, &handler.DatasetHTTPError{Status: 404, Code: "dataset_not_found", Detail: "dataset is not visible in the caller's tenant"}
		}
	}
	rows, err := r.db.Query(ctx, `SELECT b.name, COALESCE('ri.foundry.main.transaction.' || b.head_transaction_id::text, '')
FROM dataset_branches b
JOIN datasets d ON d.id = b.dataset_id
WHERE `+where+`
ORDER BY b.name`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.BranchSnapshot{}
	for rows.Next() {
		var item models.BranchSnapshot
		var head string
		if err := rows.Scan(&item.Name, &head); err != nil {
			return nil, err
		}
		if head != "" {
			item.HeadTransactionRID = &head
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (r *Repository) OpenTransaction(ctx context.Context, datasetRID, branch string) (string, error) {
	txID := uuid.New()
	where := "d.rid=$2 AND b.name=$3 AND b.deleted_at IS NULL"
	args := []any{txID, datasetRID, branch}
	if scope := scopeFromContext(ctx); scope != nil {
		if !r.datasetOrgColumns(ctx) {
			return "", handler.ErrDatasetTenantBoundaryUnsupported
		}
		sb := &sqlBuilder{args: args}
		appendDatasetOrgClause(sb, scope)
		where += sb.whereSQL()
		args = sb.args
	}
	tag, err := r.db.Exec(ctx, `INSERT INTO dataset_transactions (id, dataset_id, branch_id, branch_name, tx_type, status, operation)
SELECT $1, d.id, b.id, b.name, 'UPDATE', 'OPEN', 'pipeline-build'
FROM datasets d JOIN dataset_branches b ON b.dataset_id=d.id
WHERE `+where, args...)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", fmt.Errorf("dataset branch not found: %s@%s", datasetRID, branch)
	}
	return "ri.foundry.main.transaction." + txID.String(), nil
}

func (r *Repository) ViewSchema(ctx context.Context, datasetRID, branch string) (json.RawMessage, error) {
	where := "d.rid=$1 AND b.name=$2 AND b.deleted_at IS NULL"
	args := []any{datasetRID, branch}
	if scope := scopeFromContext(ctx); scope != nil {
		if !r.datasetOrgColumns(ctx) {
			return nil, handler.ErrDatasetTenantBoundaryUnsupported
		}
		sb := &sqlBuilder{args: args}
		appendDatasetOrgClause(sb, scope)
		where += sb.whereSQL()
		args = sb.args
	}
	var raw []byte
	err := r.db.QueryRow(ctx, `SELECT COALESCE(v.metadata->'schema', '{}'::jsonb)
FROM datasets d
JOIN dataset_branches b ON b.dataset_id=d.id
LEFT JOIN dataset_views v ON v.dataset_id=d.id AND v.branch_id=b.id
WHERE `+where+`
ORDER BY v.computed_at DESC NULLS LAST
LIMIT 1`, args...).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return json.RawMessage(`{}`), nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	return json.RawMessage(raw), nil
}

// TryAcquire implements resolver.BranchLockRepository.
func (r *Repository) TryAcquire(ctx context.Context, outputDatasetRID string, buildID uuid.UUID, transactionRID string) (uuid.UUID, bool, error) {
	const insert = `INSERT INTO build_input_locks (output_dataset_rid, build_id, transaction_rid) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`
	tag, err := r.db.Exec(ctx, insert, outputDatasetRID, buildID, transactionRID)
	if err != nil {
		return uuid.Nil, false, err
	}
	if tag.RowsAffected() == 1 {
		return uuid.Nil, true, nil
	}
	var holder uuid.UUID
	err = r.db.QueryRow(ctx, `SELECT build_id FROM build_input_locks WHERE output_dataset_rid=$1`, outputDatasetRID).Scan(&holder)
	if err != nil {
		return uuid.Nil, false, err
	}
	return holder, false, nil
}

// ReleaseLock implements resolver.LockReleaser: it drops ONLY the lock
// row this build holds for the given output dataset (build_id is unique
// per build, so a foreign build's or tenant's lock can never be touched).
func (r *Repository) ReleaseLock(ctx context.Context, outputDatasetRID string, buildID uuid.UUID) error {
	_, err := r.db.Exec(ctx, `DELETE FROM build_input_locks WHERE build_id=$1 AND output_dataset_rid=$2`, buildID, outputDatasetRID)
	return err
}

// RecordPendingTxn implements resolver.PendingTxnRecorder: persists the
// build↔transaction association the moment the transaction is opened.
func (r *Repository) RecordPendingTxn(ctx context.Context, buildID uuid.UUID, datasetRID, branch, transactionRID string) error {
	guard := "id=$1"
	args := []any{buildID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds")
		guard += sb.whereSQL()
		args = sb.args
	}
	var ok bool
	if err := r.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM builds WHERE `+guard+`)`, args...).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("build %s not visible in caller tenant", buildID)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO build_pending_output_txns (build_id, dataset_rid, branch, transaction_rid) VALUES ($1,$2,$3,$4) ON CONFLICT (build_id, transaction_rid) DO NOTHING`, buildID, datasetRID, branch, transactionRID)
	return err
}

// ClearPendingTxn removes the association only after a confirmed abort,
// and only when the build is visible under the caller's tenant.
func (r *Repository) ClearPendingTxn(ctx context.Context, buildID uuid.UUID, transactionRID string) error {
	where := `b.id = p.build_id AND p.build_id = $1 AND p.transaction_rid = $2`
	args := []any{buildID, transactionRID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds b")
		where += sb.whereSQL()
		args = sb.args
	}
	_, err := r.db.Exec(ctx, `DELETE FROM build_pending_output_txns p USING builds b WHERE `+where, args...)
	return err
}

// ListPendingTxns returns the build's retryable remote transactions,
// tenant-guarded through the parent build.
func (r *Repository) ListPendingTxns(ctx context.Context, buildID uuid.UUID) ([]handler.PendingOutputTxn, error) {
	where := "p.build_id = $1"
	args := []any{buildID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds b")
		where += sb.whereSQL()
		args = sb.args
	}
	rows, err := r.db.Query(ctx, `SELECT p.dataset_rid, p.branch, p.transaction_rid
FROM build_pending_output_txns p JOIN builds b ON b.id = p.build_id
WHERE `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []handler.PendingOutputTxn{}
	for rows.Next() {
		var item handler.PendingOutputTxn
		if err := rows.Scan(&item.DatasetRID, &item.Branch, &item.TransactionRID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ClearPendingTxnsForBuild drops every pending association of the build
// at once (used after the main abort path confirmed all transactions);
// tenant-guarded through the parent build.
func (r *Repository) ClearPendingTxnsForBuild(ctx context.Context, buildID uuid.UUID) error {
	where := "b.id = p.build_id AND p.build_id = $1"
	args := []any{buildID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds b")
		where += sb.whereSQL()
		args = sb.args
	}
	_, err := r.db.Exec(ctx, `DELETE FROM build_pending_output_txns p USING builds b WHERE `+where, args...)
	return err
}

// AbortTransaction implements handler.TransactionAborter against the
// co-located dataset schema. The row must match the parent dataset RID,
// the branch name and the caller's tenant boundary; scoped callers fail
// closed when the dataset org columns are unavailable.
func (r *Repository) AbortTransaction(ctx context.Context, datasetRID, branch, transactionRID string) error {
	txID := strings.TrimPrefix(transactionRID, "ri.foundry.main.transaction.")
	if _, err := uuid.Parse(txID); err != nil {
		return fmt.Errorf("invalid transaction rid: %w", err)
	}
	where := "t.id=$1 AND t.branch_name=$2 AND d.rid=$3"
	args := []any{txID, branch, datasetRID}
	if scope := scopeFromContext(ctx); scope != nil {
		if !r.datasetOrgColumns(ctx) {
			return handler.ErrDatasetTenantBoundaryUnsupported
		}
		sb := &sqlBuilder{args: args}
		appendDatasetOrgClause(sb, scope)
		where += sb.whereSQL()
		args = sb.args
	}
	// Terminal-state semantics, race-safe: a single conditional UPDATE
	// transitions only status='OPEN' rows; zero affected rows are then
	// classified by re-reading the committed state (ABORTED is
	// idempotent success, anything else conflicts, missing is 404).
	tag, err := r.db.Exec(ctx, `UPDATE dataset_transactions t
SET status='ABORTED', aborted_at=NOW()
FROM datasets d
WHERE d.id = t.dataset_id AND `+where+` AND t.status='OPEN'`, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = r.db.QueryRow(ctx, `SELECT t.status
FROM dataset_transactions t
JOIN datasets d ON d.id = t.dataset_id
WHERE `+where, args...).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return handler.ErrTransactionNotFound
	}
	if err != nil {
		return err
	}
	if status == "ABORTED" {
		return nil
	}
	return handler.ErrTransactionConflict
}

func (r *Repository) HasUpstreamInProgress(ctx context.Context, inputDatasetRIDs []string, selfBuildID uuid.UUID) (bool, error) {
	if len(inputDatasetRIDs) == 0 {
		return false, nil
	}
	where := `jo.output_dataset_rid = ANY($1) AND b.id<>$2 AND b.state IN ('BUILD_RESOLUTION','BUILD_QUEUED','BUILD_RUNNING','BUILD_ABORTING')`
	args := []any{inputDatasetRIDs, selfBuildID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds b")
		where += sb.whereSQL()
		args = sb.args
	}
	var exists bool
	if err := r.db.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM job_outputs jo
  JOIN jobs j ON j.id=jo.job_id
  JOIN builds b ON b.id=j.build_id
  WHERE `+where+`
)`, args...).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// OpenBuild implements handler.BuildRepository.
func (r *Repository) OpenBuild(ctx context.Context, args resolver.ResolveBuildArgs, buildID uuid.UUID) error {
	if args.TriggerKind == "" {
		args.TriggerKind = "MANUAL"
	}
	if args.JobSpecFallback == nil {
		// A nil slice binds as SQL NULL, violating the NOT NULL
		// DEFAULT '{}' column (SQLSTATE 23502) when the request omits
		// the optional job_spec_fallback — bind the empty array instead.
		args.JobSpecFallback = []string{}
	}
	if args.AbortPolicy == "" {
		args.AbortPolicy = string(models.AbortDependentOnly)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO builds (id, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, requested_by, abort_policy, tenant_id)
VALUES ($1,$2,$3,$4,'BUILD_RESOLUTION',$5,$6,$7,$8,$9)
ON CONFLICT (id) DO NOTHING`, buildID, args.PipelineRID, args.BuildBranch, args.JobSpecFallback, args.TriggerKind, args.ForceBuild, args.RequestedBy, args.AbortPolicy, tenantValueForInsert(ctx))
	return err
}

func (r *Repository) PersistResolvedBuild(ctx context.Context, resolved *models.ResolvedBuild) error {
	if resolved == nil {
		return errors.New("resolved build is nil")
	}
	if beginner, ok := r.db.(txBeginner); ok {
		tx, err := beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		if err := persistResolvedBuild(ctx, tx, resolved); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	return persistResolvedBuild(ctx, r.db, resolved)
}

func persistResolvedBuild(ctx context.Context, db DB, resolved *models.ResolvedBuild) error {
	state := string(resolved.State)
	if resolved.QueuedReason != nil {
		state = string(models.BuildQueued)
	}
	_, err := db.Exec(ctx, `UPDATE builds SET state=$2, queued_at=CASE WHEN $2='BUILD_QUEUED' THEN NOW() ELSE queued_at END WHERE id=$1`, resolved.BuildID, state)
	if err != nil {
		return err
	}
	if len(resolved.ManualStagedGroup) != 0 {
		if _, err := db.Exec(ctx, `UPDATE builds SET manual_staged_group=$2 WHERE id=$1`,
			resolved.BuildID, resolved.ManualStagedGroup); err != nil {
			return err
		}
	}
	jobBySpec := map[string]uuid.UUID{}
	for _, job := range resolved.Jobs {
		jobBySpec[job.JobSpecRID] = job.ID
		_, err := db.Exec(ctx, `INSERT INTO jobs (id, build_id, job_spec_rid, state, output_transaction_rids)
VALUES ($1,$2,$3,'WAITING',$4)
ON CONFLICT (id) DO UPDATE SET output_transaction_rids=EXCLUDED.output_transaction_rids`, job.ID, resolved.BuildID, job.JobSpecRID, job.OutputTransactionRIDs)
		if err != nil {
			return err
		}
		for _, txRID := range job.OutputTransactionRIDs {
			datasetRID := datasetForTransaction(resolved.OpenedTransactions, txRID)
			_, err := db.Exec(ctx, `INSERT INTO job_outputs (job_id, output_dataset_rid, transaction_rid)
VALUES ($1,$2,$3) ON CONFLICT (job_id, output_dataset_rid) DO UPDATE SET transaction_rid=EXCLUDED.transaction_rid`, job.ID, datasetRID, txRID)
			if err != nil {
				return err
			}
		}
	}
	for _, job := range resolved.Jobs {
		for _, depSpec := range job.DependsOnJobSpecRIDs {
			depID, ok := jobBySpec[depSpec]
			if !ok {
				continue
			}
			_, err := db.Exec(ctx, `INSERT INTO job_dependencies (job_id, depends_on_job_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, job.ID, depID)
			if err != nil {
				return err
			}
		}
	}
	if resolved.BatchConfig != nil {
		// Same transaction as the jobs: PREPARING → PENDING records the
		// opened reservation so execute/abort never see a resolved batched
		// build without its reservation binding. BUILD_QUEUED builds (no
		// jobs, no reservation) still record PENDING with an empty
		// reservation — execute refuses them explicitly (queued), never
		// the legacy empty-plan fallback.
		reservation := ""
		configUUID, configOK := datasetUUIDFromRef(resolved.BatchConfig.OutputDatasetRID)
		for _, opened := range resolved.OpenedTransactions {
			// The spec's output spelling (possibly a bare UUID) and the
			// frozen configuration's canonical RID must meet as the SAME
			// parsed UUID — a string mismatch here would leave the
			// reservation empty and the run unschedulable.
			openedUUID, openedOK := datasetUUIDFromRef(opened.DatasetRID)
			if configOK && openedOK && openedUUID == configUUID {
				reservation = opened.TransactionRID
				break
			}
		}
		if err := ResolveBatchedPrepared(ctx, db, resolved.BuildID, reservation); err != nil {
			return err
		}
	}
	return nil
}

func datasetForTransaction(items []models.OpenedTransaction, txRID string) string {
	for _, item := range items {
		if item.TransactionRID == txRID {
			return item.DatasetRID
		}
	}
	return txRID
}

func (r *Repository) MarkBuildFailed(ctx context.Context, buildID uuid.UUID, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE builds SET state='BUILD_FAILED', error_message=$2, finished_at=NOW() WHERE id=$1`, buildID, reason)
	if err != nil {
		return err
	}
	// A batched build whose CreateBuild failed after the prepare insert
	// leaves its marker in PREPARING — terminalize it so execute/abort
	// stop answering "preparing". No-op for non-batched builds.
	return r.FailBatchedPreparing(ctx, buildID, reason)
}

// ListBuilds and GetBuild are production query helpers used by route handlers
// and repository tests.
func (r *Repository) ListBuilds(ctx context.Context, q models.ListBuildsQuery) ([]models.BuildEnvelope, error) {
	limit := int64(50)
	if q.Limit != nil && *q.Limit > 0 && *q.Limit <= 500 {
		limit = *q.Limit
	}
	var cursor *time.Time
	if q.Cursor != "" {
		if parsed, err := time.Parse(time.RFC3339, q.Cursor); err == nil {
			cursor = &parsed
		} else if parsed, err := time.Parse(time.RFC3339Nano, q.Cursor); err == nil {
			cursor = &parsed
		}
	}
	where := `($1='' OR pipeline_rid=$1) AND ($2='' OR state=$2) AND ($3='' OR build_branch=$3)
  AND ($4::timestamptz IS NULL OR created_at >= $4)
  AND ($5::timestamptz IS NULL OR created_at < $5)`
	args := []any{q.PipelineRID, q.Status, q.Branch, q.Since, cursor, limit}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds")
		where += sb.whereSQL()
		args = sb.args
	}
	rows, err := r.db.Query(ctx, `SELECT id, rid, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, abort_policy, queued_at, started_at, finished_at, error_message, requested_by, created_at
FROM builds
WHERE `+where+`
ORDER BY created_at DESC
LIMIT $6`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.BuildEnvelope{}
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, models.BuildEnvelope{Build: b})
	}
	return out, rows.Err()
}

func (r *Repository) GetBuild(ctx context.Context, idOrRID string) (*models.BuildEnvelope, error) {
	where := "WHERE id::text=$1 OR rid=$1"
	args := []any{idOrRID}
	if scope := scopeFromContext(ctx); scope != nil {
		// Parenthesize: AND binds tighter than OR, so the tenant clause
		// must guard BOTH the UUID and the RID branch. The WHERE keyword
		// stays outside the parentheses.
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds")
		where = "WHERE (id::text=$1 OR rid=$1)" + sb.whereSQL()
		args = sb.args
	}
	var b models.Build
	err := r.db.QueryRow(ctx, `SELECT id, rid, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, abort_policy, queued_at, started_at, finished_at, error_message, requested_by, created_at
FROM builds `+where, args...).Scan(&b.ID, &b.RID, &b.PipelineRID, &b.BuildBranch, &b.JobSpecFallback, &b.State, &b.TriggerKind, &b.ForceBuild, &b.AbortPolicy, &b.QueuedAt, &b.StartedAt, &b.FinishedAt, &b.ErrorMessage, &b.RequestedBy, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	jobs, err := r.listJobsForBuild(ctx, b.ID)
	if err != nil {
		return nil, err
	}
	return &models.BuildEnvelope{Build: b, Jobs: jobs}, nil
}

func scanBuild(rows pgx.Rows) (models.Build, error) {
	var b models.Build
	err := rows.Scan(&b.ID, &b.RID, &b.PipelineRID, &b.BuildBranch, &b.JobSpecFallback, &b.State, &b.TriggerKind, &b.ForceBuild, &b.AbortPolicy, &b.QueuedAt, &b.StartedAt, &b.FinishedAt, &b.ErrorMessage, &b.RequestedBy, &b.CreatedAt)
	return b, err
}

func (r *Repository) ListJobsForBuildID(ctx context.Context, idOrRID string) ([]models.Job, error) {
	where := "WHERE id::text=$1 OR rid=$1"
	args := []any{idOrRID}
	if scope := scopeFromContext(ctx); scope != nil {
		// Parenthesized so the tenant clause guards both id branches;
		// the WHERE keyword stays outside.
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds")
		where = "WHERE (id::text=$1 OR rid=$1)" + sb.whereSQL()
		args = sb.args
	}
	var buildID uuid.UUID
	err := r.db.QueryRow(ctx, `SELECT id FROM builds `+where, args...).Scan(&buildID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.listJobsForBuild(ctx, buildID)
}

func (r *Repository) GetJob(ctx context.Context, idOrRID string) (*models.Job, error) {
	where := "WHERE id::text=$1 OR rid=$1"
	args := []any{idOrRID}
	if scope := scopeFromContext(ctx); scope != nil {
		// Jobs are subresources of their build: the build's tenant
		// boundary decides visibility, guarding BOTH id branches —
		// AND binds tighter than an unparenthesized OR, and the WHERE
		// keyword stays outside the parentheses.
		sb := &sqlBuilder{args: args}
		appendBuildParentClause(sb, scope, "jobs")
		where = "WHERE (id::text=$1 OR rid=$1)" + sb.whereSQL()
		args = sb.args
	}
	var j models.Job
	err := r.db.QueryRow(ctx, `SELECT id, rid, build_id, job_spec_rid, state, output_transaction_rids, state_changed_at, attempt, stale_skipped, failure_reason, output_content_hash, created_at
FROM jobs `+where, args...).Scan(&j.ID, &j.RID, &j.BuildID, &j.JobSpecRID, &j.State, &j.OutputTransactionRIDs, &j.StateChangedAt, &j.Attempt, &j.StaleSkipped, &j.FailureReason, &j.OutputContentHash, &j.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (r *Repository) listJobsForBuild(ctx context.Context, buildID uuid.UUID) ([]models.Job, error) {
	rows, err := r.db.Query(ctx, `SELECT id, rid, build_id, job_spec_rid, state, output_transaction_rids, state_changed_at, attempt, stale_skipped, failure_reason, output_content_hash, created_at FROM jobs WHERE build_id=$1 ORDER BY created_at`, buildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.Job{}
	for rows.Next() {
		var j models.Job
		if err := rows.Scan(&j.ID, &j.RID, &j.BuildID, &j.JobSpecRID, &j.State, &j.OutputTransactionRIDs, &j.StateChangedAt, &j.Attempt, &j.StaleSkipped, &j.FailureReason, &j.OutputContentHash, &j.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (r *Repository) LoadPlan(ctx context.Context, buildID uuid.UUID) (executor.Plan, error) {
	where := "WHERE id=$1"
	args := []any{buildID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds")
		where += sb.whereSQL()
		args = sb.args
	}
	var plan executor.Plan
	var abortPolicy string
	var forceBuild bool
	err := r.db.QueryRow(ctx, `SELECT id, build_branch, abort_policy, force_build FROM builds `+where, args...).Scan(&plan.BuildID, &plan.BuildBranch, &abortPolicy, &forceBuild)
	if err != nil {
		return executor.Plan{}, err
	}
	plan.AbortPolicy = executor.AbortPolicy(abortPolicy)
	plan.Parallelism = executor.DefaultParallelism
	plan.MaxAttempts = 1
	jobs, err := r.listJobsForBuild(ctx, buildID)
	if err != nil {
		return executor.Plan{}, err
	}
	deps, err := r.dependenciesForBuild(ctx, buildID)
	if err != nil {
		return executor.Plan{}, err
	}
	specs, err := r.jobSpecsByRID(ctx, buildID)
	if err != nil {
		return executor.Plan{}, err
	}
	for _, job := range jobs {
		outputs, err := r.outputsForJob(ctx, job.ID)
		if err != nil {
			return executor.Plan{}, err
		}
		metadata := map[string]any{
			"job_spec_rid": job.JobSpecRID,
			"force_build":  forceBuild,
		}
		if spec, ok := specs[job.JobSpecRID]; ok {
			if spec.LogicKind != "" {
				metadata["logic_kind"] = spec.LogicKind
			}
			if len(spec.LogicPayload) > 0 {
				if err := handler.BindDatasetOutputSchema(spec.LogicPayload, outputs); err != nil {
					return executor.Plan{}, fmt.Errorf("job spec %s: %w", job.JobSpecRID, err)
				}
				metadata["logic_payload"] = spec.LogicPayload
			}
			if spec.TransformType != "" {
				metadata["transform_type"] = spec.TransformType
			}
			if len(spec.InputDatasetRIDs) > 0 {
				metadata["input_dataset_ids"] = append([]string(nil), spec.InputDatasetRIDs...)
			}
			if len(spec.OutputDatasetRIDs) > 0 {
				metadata["output_dataset_id"] = spec.OutputDatasetRIDs[0]
			}
		}
		plan.Nodes = append(plan.Nodes, executor.Node{ID: job.JobSpecRID, JobID: job.ID, DependsOn: deps[job.ID], Outputs: outputs, MaxAttempts: 1, Metadata: metadata})
	}
	return plan, nil
}

type jobSpecMetadata struct {
	LogicKind         string
	LogicPayload      json.RawMessage
	TransformType     string
	InputDatasetRIDs  []string
	OutputDatasetRIDs []string
}

// jobSpecsByRID joins jobs with the published job_specs table to recover the
// spec body needed by the runner (logic_kind, logic_payload, transform_type,
// input/output dataset rids). Missing rows just yield empty metadata; the
// runner falls back to defaults the same way it does for inline-built plans.
func (r *Repository) jobSpecsByRID(ctx context.Context, buildID uuid.UUID) (map[string]jobSpecMetadata, error) {
	rows, err := r.db.Query(ctx, `SELECT js.rid, js.logic_kind, js.logic_payload, js.inputs, js.output_dataset_rids
FROM jobs j
JOIN job_specs js ON js.rid = j.job_spec_rid
WHERE j.build_id = $1`, buildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]jobSpecMetadata{}
	for rows.Next() {
		var rid string
		var logicKind string
		var payload, inputsRaw []byte
		var outputs []string
		if err := rows.Scan(&rid, &logicKind, &payload, &inputsRaw, &outputs); err != nil {
			return nil, err
		}
		spec := jobSpecMetadata{LogicKind: logicKind, LogicPayload: append(json.RawMessage(nil), payload...), OutputDatasetRIDs: outputs}
		var inputs []struct {
			DatasetRID    string `json:"dataset_rid"`
			TransformType string `json:"transform_type"`
		}
		if len(inputsRaw) > 0 {
			_ = json.Unmarshal(inputsRaw, &inputs)
		}
		for _, item := range inputs {
			if item.DatasetRID != "" {
				spec.InputDatasetRIDs = append(spec.InputDatasetRIDs, item.DatasetRID)
			}
			if item.TransformType != "" && spec.TransformType == "" {
				spec.TransformType = item.TransformType
			}
		}
		if spec.TransformType == "" && len(payload) > 0 {
			var pcfg struct {
				TransformType string `json:"transform_type"`
			}
			if json.Unmarshal(payload, &pcfg) == nil && pcfg.TransformType != "" {
				spec.TransformType = pcfg.TransformType
			}
		}
		out[rid] = spec
	}
	return out, rows.Err()
}

func (r *Repository) dependenciesForBuild(ctx context.Context, buildID uuid.UUID) (map[uuid.UUID][]string, error) {
	rows, err := r.db.Query(ctx, `SELECT jd.job_id, dep.job_spec_rid FROM job_dependencies jd JOIN jobs dep ON dep.id=jd.depends_on_job_id JOIN jobs j ON j.id=jd.job_id WHERE j.build_id=$1`, buildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID][]string{}
	for rows.Next() {
		var jobID uuid.UUID
		var depSpec string
		if err := rows.Scan(&jobID, &depSpec); err != nil {
			return nil, err
		}
		out[jobID] = append(out[jobID], depSpec)
	}
	return out, rows.Err()
}

func (r *Repository) outputsForJob(ctx context.Context, jobID uuid.UUID) ([]executor.OutputTransaction, error) {
	rows, err := r.db.Query(ctx, `SELECT output_dataset_rid, transaction_rid FROM job_outputs WHERE job_id=$1 ORDER BY output_dataset_rid`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []executor.OutputTransaction{}
	for rows.Next() {
		var tx executor.OutputTransaction
		if err := rows.Scan(&tx.DatasetRID, &tx.TransactionRID); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, rows.Err()
}

func (r *Repository) LoadPipeline(ctx context.Context, pipelineID uuid.UUID) (*models.Pipeline, error) {
	where := "WHERE id=$1"
	args := []any{pipelineID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipelines")
		where += sb.whereSQL()
		args = sb.args
	}
	var p models.Pipeline
	err := r.db.QueryRow(ctx, `SELECT `+pipelineSelectColumns+` FROM pipelines `+where, args...).Scan(pipelineScanDest(&p)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &p, err
}

func (r *Repository) OpenPipelineRun(ctx context.Context, pipeline *models.Pipeline, req models.TriggerPipelineRequest, startedBy *uuid.UUID, contextJSON json.RawMessage) (*models.PipelineRun, error) {
	return r.OpenPipelineRunWithOptions(ctx, pipeline, req, startedBy, "manual", req.FromNodeID, nil, 1, contextJSON)
}

func (r *Repository) OpenPipelineRunWithOptions(ctx context.Context, pipeline *models.Pipeline, req models.TriggerPipelineRequest, startedBy *uuid.UUID, triggerType string, fromNodeID *string, retryOfRunID *uuid.UUID, attemptNumber int32, contextJSON json.RawMessage) (*models.PipelineRun, error) {
	id := uuid.New()
	if triggerType == "" {
		triggerType = "manual"
	}
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	var run models.PipelineRun
	err := r.db.QueryRow(ctx, `INSERT INTO pipeline_runs (id, pipeline_id, status, started_by, trigger_type, attempt_number, started_from_node_id, retry_of_run_id, execution_context)
VALUES ($1,$2,'queued',$3,$4,$5,$6,$7,$8)
RETURNING id, pipeline_id, status, trigger_type, started_by, attempt_number, started_from_node_id, retry_of_run_id, execution_context, node_results, error_message, started_at, finished_at`, id, pipeline.ID, startedBy, triggerType, attemptNumber, fromNodeID, retryOfRunID, contextJSON).Scan(&run.ID, &run.PipelineID, &run.Status, &run.TriggerType, &run.StartedBy, &run.AttemptNumber, &run.StartedFromNodeID, &run.RetryOfRunID, &run.ExecutionContext, &run.NodeResults, &run.ErrorMessage, &run.StartedAt, &run.FinishedAt)
	if err != nil {
		return nil, err
	}
	_ = req
	return &run, nil
}

func (r *Repository) MarkPipelineRunRunning(ctx context.Context, runID uuid.UUID) error {
	_, err := r.db.Exec(ctx, `UPDATE pipeline_runs SET status='running' WHERE id=$1 AND status IN ('queued','pending','BUILD_QUEUED')`, runID)
	return err
}

func (r *Repository) ListPipelineRuns(ctx context.Context, pipelineID uuid.UUID, page, perPage int64) ([]models.PipelineRun, error) {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 20
	}
	if perPage > 100 {
		perPage = 100
	}
	where := "WHERE pipeline_id=$1"
	args := []any{pipelineID, perPage, (page - 1) * perPage}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendPipelineParentClause(sb, scope, "pipeline_runs.pipeline_id")
		where += sb.whereSQL()
		args = sb.args
	}
	return r.queryPipelineRuns(ctx, `SELECT id, pipeline_id, status, trigger_type, started_by, attempt_number, started_from_node_id, retry_of_run_id, execution_context, node_results, error_message, started_at, finished_at FROM pipeline_runs `+where+` ORDER BY started_at DESC LIMIT $2 OFFSET $3`, args...)
}

func (r *Repository) GetPipelineRun(ctx context.Context, pipelineID, runID uuid.UUID) (*models.PipelineRun, error) {
	where := "WHERE id=$1 AND pipeline_id=$2"
	args := []any{runID, pipelineID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendPipelineParentClause(sb, scope, "pipeline_runs.pipeline_id")
		where += sb.whereSQL()
		args = sb.args
	}
	var run models.PipelineRun
	err := r.db.QueryRow(ctx, `SELECT id, pipeline_id, status, trigger_type, started_by, attempt_number, started_from_node_id, retry_of_run_id, execution_context, node_results, error_message, started_at, finished_at FROM pipeline_runs `+where, args...).Scan(&run.ID, &run.PipelineID, &run.Status, &run.TriggerType, &run.StartedBy, &run.AttemptNumber, &run.StartedFromNodeID, &run.RetryOfRunID, &run.ExecutionContext, &run.NodeResults, &run.ErrorMessage, &run.StartedAt, &run.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &run, err
}

func (r *Repository) ListBuildQueue(ctx context.Context, query handler.BuildQueueQuery) ([]models.PipelineRun, error) {
	if query.Page < 1 {
		query.Page = 1
	}
	if query.PerPage < 1 {
		query.PerPage = 50
	}
	if query.PerPage > 200 {
		query.PerPage = 200
	}
	where := "($1='' OR status=$1) AND ($2='' OR trigger_type=$2) AND ($3::uuid IS NULL OR pipeline_id=$3)"
	args := []any{query.Status, query.TriggerType, query.PipelineID, query.PerPage, (query.Page - 1) * query.PerPage}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendPipelineParentClause(sb, scope, "pipeline_runs.pipeline_id")
		where += sb.whereSQL()
		args = sb.args
	}
	return r.queryPipelineRuns(ctx, `SELECT id, pipeline_id, status, trigger_type, started_by, attempt_number, started_from_node_id, retry_of_run_id, execution_context, node_results, error_message, started_at, finished_at FROM pipeline_runs WHERE `+where+` ORDER BY started_at DESC LIMIT $4 OFFSET $5`, args...)
}

func (r *Repository) AbortPipelineRun(ctx context.Context, runID uuid.UUID) (*models.PipelineRun, bool, error) {
	cancelWhere := "id=$1 AND status IN ('queued','pending','running','BUILD_QUEUED','BUILD_RUNNING')"
	cancelArgs := []any{runID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: cancelArgs}
		appendPipelineParentClause(sb, scope, "pipeline_runs.pipeline_id")
		cancelWhere += sb.whereSQL()
		cancelArgs = sb.args
	}
	var run models.PipelineRun
	err := r.db.QueryRow(ctx, `UPDATE pipeline_runs SET status='cancelled', error_message=COALESCE(error_message, 'cancelled by user'), finished_at=NOW() WHERE `+cancelWhere+` RETURNING id, pipeline_id, status, trigger_type, started_by, attempt_number, started_from_node_id, retry_of_run_id, execution_context, node_results, error_message, started_at, finished_at`, cancelArgs...).Scan(&run.ID, &run.PipelineID, &run.Status, &run.TriggerType, &run.StartedBy, &run.AttemptNumber, &run.StartedFromNodeID, &run.RetryOfRunID, &run.ExecutionContext, &run.NodeResults, &run.ErrorMessage, &run.StartedAt, &run.FinishedAt)
	if err == nil {
		return &run, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	existsWhere := "id=$1"
	existsArgs := []any{runID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: existsArgs}
		appendPipelineParentClause(sb, scope, "pipeline_runs.pipeline_id")
		existsWhere += sb.whereSQL()
		existsArgs = sb.args
	}
	var exists bool
	err = r.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_runs WHERE `+existsWhere+`)`, existsArgs...).Scan(&exists)
	return nil, exists, err
}

func (r *Repository) QueueSummary(ctx context.Context) (map[string]int64, error) {
	where := "started_at > NOW() - INTERVAL '24 hours'"
	args := []any{}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendPipelineParentClause(sb, scope, "pipeline_runs.pipeline_id")
		where += sb.whereSQL()
		args = sb.args
	}
	rows, err := r.db.Query(ctx, `SELECT status, COUNT(*)::bigint FROM pipeline_runs WHERE `+where+` GROUP BY status`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		out[status] = count
	}
	return out, rows.Err()
}

func (r *Repository) ListDuePipelines(ctx context.Context) ([]models.Pipeline, error) {
	where := "status='active' AND next_run_at IS NOT NULL AND next_run_at <= NOW()"
	args := []any{}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipelines")
		where += sb.whereSQL()
		args = sb.args
	}
	rows, err := r.db.Query(ctx, `SELECT `+pipelineSelectColumns+` FROM pipelines WHERE `+where+` ORDER BY next_run_at ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.Pipeline{}
	for rows.Next() {
		var p models.Pipeline
		if err := rows.Scan(pipelineScanDest(&p)...); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *Repository) UpdatePipelineNextRun(ctx context.Context, pipelineID uuid.UUID, nextRunAt *time.Time) error {
	where := "id=$1"
	args := []any{pipelineID, nextRunAt}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipelines")
		where += sb.whereSQL()
		args = sb.args
	}
	_, err := r.db.Exec(ctx, `UPDATE pipelines SET next_run_at=$2, updated_at=NOW() WHERE `+where, args...)
	return err
}

func (r *Repository) queryPipelineRuns(ctx context.Context, sql string, args ...any) ([]models.PipelineRun, error) {
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.PipelineRun{}
	for rows.Next() {
		var run models.PipelineRun
		if err := rows.Scan(&run.ID, &run.PipelineID, &run.Status, &run.TriggerType, &run.StartedBy, &run.AttemptNumber, &run.StartedFromNodeID, &run.RetryOfRunID, &run.ExecutionContext, &run.NodeResults, &run.ErrorMessage, &run.StartedAt, &run.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (r *Repository) FinishPipelineRun(ctx context.Context, runID uuid.UUID, status string, nodeResults json.RawMessage, errorMessage *string) error {
	_, err := r.db.Exec(ctx, `UPDATE pipeline_runs SET status=$2, node_results=$3, error_message=$4, finished_at=NOW() WHERE id=$1`, runID, status, nodeResults, errorMessage)
	return err
}

func (r *Repository) LoadBuildForAbort(ctx context.Context, id string) (*handler.AbortBuildSnapshot, error) {
	where := "id::text=$1 OR rid=$1"
	args := []any{id}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds")
		where = "(" + where + ")" + sb.whereSQL()
		args = sb.args
	}
	var snap handler.AbortBuildSnapshot
	err := r.db.QueryRow(ctx, `SELECT id, rid, state, COALESCE(build_branch, 'main'), COALESCE(error_message, '') FROM builds WHERE `+where, args...).Scan(&snap.ID, &snap.RID, &snap.State, &snap.BuildBranch, &snap.ErrorMessage)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	jobs, err := r.abortJobs(ctx, snap.ID)
	if err != nil {
		return nil, err
	}
	snap.Jobs = jobs
	return &snap, nil
}

func (r *Repository) abortJobs(ctx context.Context, buildID uuid.UUID) ([]handler.AbortJobSnapshot, error) {
	rows, err := r.db.Query(ctx, `SELECT id, state FROM jobs WHERE build_id=$1`, buildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []handler.AbortJobSnapshot{}
	for rows.Next() {
		var job handler.AbortJobSnapshot
		if err := rows.Scan(&job.ID, &job.State); err != nil {
			return nil, err
		}
		job.Outputs, err = r.outputsForJob(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

func (r *Repository) MarkBuildAborting(ctx context.Context, buildID uuid.UUID, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE builds SET state='BUILD_ABORTING', error_message=$2 WHERE id=$1`, buildID, reason)
	return err
}

func (r *Repository) TransitionJob(ctx context.Context, jobID uuid.UUID, from, to models.JobState, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE jobs SET state=$2, state_changed_at=NOW(), failure_reason=$3 WHERE id=$1 AND state=$4`, jobID, string(to), reason, string(from))
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `INSERT INTO job_state_transitions (job_id, from_state, to_state, reason) VALUES ($1,$2,$3,$4)`, jobID, string(from), string(to), reason)
	return err
}

func (r *Repository) MarkBuildAborted(ctx context.Context, buildID uuid.UUID, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE builds SET state='BUILD_ABORTED', error_message=$2, finished_at=NOW() WHERE id=$1`, buildID, reason)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `DELETE FROM build_input_locks WHERE build_id=$1`, buildID)
	return err
}

func (r *Repository) History(ctx context.Context, jobRID string, query livellogs.Query) ([]livellogs.LogEntry, error) {
	jobID, err := r.resolveJobID(ctx, jobRID)
	if err != nil {
		return nil, err
	}
	if jobID == uuid.Nil {
		return nil, nil
	}
	limit := query.Limit
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	rows, err := r.db.Query(ctx, `SELECT sequence, $1::text, ts, level, message, params FROM job_logs
WHERE job_id=$2 AND sequence >= $3 AND ($4::timestamptz IS NULL OR ts >= $4) AND ($5::timestamptz IS NULL OR ts < $5) AND (cardinality($6::text[]) = 0 OR level = ANY($6::text[]))
ORDER BY sequence ASC LIMIT $7`, jobRID, jobID, query.FromSequence, query.Since, query.Until, logLevels(query.Levels), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []livellogs.LogEntry{}
	for rows.Next() {
		var entry livellogs.LogEntry
		if err := rows.Scan(&entry.Sequence, &entry.JobRID, &entry.TS, &entry.Level, &entry.Message, &entry.Params); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

func (r *Repository) AppendLog(ctx context.Context, jobID uuid.UUID, level livellogs.LogLevel, message string, params json.RawMessage) (livellogs.LogEntry, error) {
	id := uuid.New()
	var entry livellogs.LogEntry
	err := r.db.QueryRow(ctx, `INSERT INTO job_logs (id, job_id, level, message, params) VALUES ($1,$2,$3,$4,$5)
RETURNING sequence, ts, level, message, params`, id, jobID, string(level), message, params).Scan(&entry.Sequence, &entry.TS, &entry.Level, &entry.Message, &entry.Params)
	entry.JobRID = jobID.String()
	return entry, err
}

func logLevels(levels []livellogs.LogLevel) []string {
	out := make([]string, len(levels))
	for i, level := range levels {
		out[i] = string(level)
	}
	return out
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

var _ = sortedStrings

// Abort implements executor.TransactionManager for already-opened output rows.
func (r *Repository) Abort(ctx context.Context, tx executor.OutputTransaction) error {
	if tx.Inline {
		return nil
	}
	_, err := r.db.Exec(ctx, `UPDATE job_outputs SET aborted=TRUE WHERE output_dataset_rid=$1 AND transaction_rid=$2`, tx.DatasetRID, tx.TransactionRID)
	return err
}

// Commit implements executor.OutputCommitter for already-opened output rows.
func (r *Repository) Commit(ctx context.Context, tx executor.OutputTransaction, _ executor.NodeResult) error {
	if tx.Inline {
		return nil
	}
	_, err := r.db.Exec(ctx, `UPDATE job_outputs SET committed=TRUE WHERE output_dataset_rid=$1 AND transaction_rid=$2`, tx.DatasetRID, tx.TransactionRID)
	return err
}

// Record implements executor.AuditSink. It mirrors Rust transition_job_in_tx by
// flipping jobs.state on a matching `from` row and appending a
// job_state_transitions row, but only when the event carries a real state
// transition (build-terminal events and per-output commit/abort markers are
// dropped — those are surfaced via build_events / job_outputs flags instead).
func (r *Repository) Record(ctx context.Context, event executor.AuditEvent) error {
	if event.JobID == uuid.Nil || event.From == "" || event.To == "" || event.From == event.To {
		return nil
	}
	from := string(event.From)
	to := string(event.To)
	reason := event.Reason
	var tag pgconn.CommandTag
	var err error
	if isFailureState(event.To) {
		tag, err = r.db.Exec(ctx, `UPDATE jobs SET state=$2, state_changed_at=NOW(), failure_reason=$3, attempt=GREATEST(attempt, $5) WHERE id=$1 AND state=$4`, event.JobID, to, reason, from, event.Attempt)
	} else {
		tag, err = r.db.Exec(ctx, `UPDATE jobs SET state=$2, state_changed_at=NOW(), attempt=GREATEST(attempt, $4) WHERE id=$1 AND state=$3`, event.JobID, to, from, event.Attempt)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	_, err = r.db.Exec(ctx, `INSERT INTO job_state_transitions (job_id, from_state, to_state, reason) VALUES ($1,$2,$3,$4)`, event.JobID, from, to, reason)
	return err
}

func isFailureState(state executor.NodeState) bool {
	switch state {
	case executor.NodeFailed, executor.NodeAborted, executor.NodeAbortPending:
		return true
	}
	return false
}

func (r *Repository) ListDatasetBuilds(ctx context.Context, datasetRID string, limit int64) ([]models.Build, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	where := "jo.output_dataset_rid=$1"
	args := []any{datasetRID, limit}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "builds b")
		where += sb.whereSQL()
		args = sb.args
	}
	rows, err := r.db.Query(ctx, `SELECT DISTINCT b.id, b.rid, b.pipeline_rid, b.build_branch, b.job_spec_fallback, b.state, b.trigger_kind, b.force_build, b.abort_policy, b.queued_at, b.started_at, b.finished_at, b.error_message, b.requested_by, b.created_at
FROM builds b
JOIN jobs j ON j.build_id=b.id
JOIN job_outputs jo ON jo.job_id=j.id
WHERE `+where+`
ORDER BY b.created_at DESC
LIMIT $2`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.Build{}
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (r *Repository) GetJobOutputs(ctx context.Context, jobRID string) (*handler.JobOutputsResponse, error) {
	where := "rid=$1 OR id::text=$1"
	args := []any{jobRID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendBuildParentClause(sb, scope, "jobs")
		where = "(" + where + ")" + sb.whereSQL()
		args = sb.args
	}
	var jobID uuid.UUID
	var state string
	var stale bool
	err := r.db.QueryRow(ctx, `SELECT id, state, stale_skipped FROM jobs WHERE `+where, args...).Scan(&jobID, &state, &stale)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, `SELECT output_dataset_rid, transaction_rid, committed, aborted FROM job_outputs WHERE job_id=$1 ORDER BY output_dataset_rid ASC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := &handler.JobOutputsResponse{RID: jobRID, State: state, StaleSkipped: stale, Outputs: []handler.JobOutputRow{}}
	for rows.Next() {
		var row handler.JobOutputRow
		if err := rows.Scan(&row.OutputDatasetRID, &row.TransactionRID, &row.Committed, &row.Aborted); err != nil {
			return nil, err
		}
		out.Outputs = append(out.Outputs, row)
	}
	return out, rows.Err()
}

func (r *Repository) GetJobInputResolutions(ctx context.Context, jobRID string) (json.RawMessage, error) {
	where := "rid=$1 OR id::text=$1"
	args := []any{jobRID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendBuildParentClause(sb, scope, "jobs")
		where = "(" + where + ")" + sb.whereSQL()
		args = sb.args
	}
	var raw []byte
	err := r.db.QueryRow(ctx, `SELECT input_view_resolutions FROM jobs WHERE `+where, args...).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		raw = []byte(`[]`)
	}
	return json.RawMessage(raw), nil
}

func (r *Repository) PublishJobSpec(ctx context.Context, kind string, req handler.CreateJobSpecRequest, createdBy string) (handler.PublishedJobSpec, error) {
	if req.PipelineRID == "" || req.BranchName == "" {
		return handler.PublishedJobSpec{}, errors.New("pipeline_rid and branch_name are required")
	}
	if kind == "" {
		return handler.PublishedJobSpec{}, errors.New("logic kind is required")
	}
	contentHash := ""
	if req.ContentHash != nil {
		contentHash = *req.ContentHash
	}
	if contentHash == "" {
		contentHash = deriveJobSpecHash(kind, req)
	}
	outputs := append([]string(nil), req.OutputDatasetRIDs...)
	if len(outputs) == 0 && kind != "EXPORT" {
		return handler.PublishedJobSpec{}, errors.New("output_dataset_rids must declare at least one dataset")
	}
	dedupWhere := "pipeline_rid=$1 AND branch_name=$2 AND logic_kind=$3 AND output_dataset_rids=$4 AND content_hash=$5"
	dedupArgs := []any{req.PipelineRID, req.BranchName, kind, outputs, contentHash}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: dedupArgs}
		appendTenantClause(sb, scope, "job_specs")
		dedupWhere += sb.whereSQL()
		dedupArgs = sb.args
	}
	id := uuid.New()
	rid := "ri.foundry.main.job_spec." + id.String()
	payload := req.LogicPayload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	inputsRaw, _ := json.Marshal(req.Inputs)
	if len(inputsRaw) == 0 {
		inputsRaw = []byte(`[]`)
	}
	if createdBy == "" {
		createdBy = uuid.Nil.String()
	}
	tenantID := tenantValueForInsert(ctx)
	jobSpecJSON, _ := json.Marshal(map[string]any{"logic_kind": kind, "logic_payload": json.RawMessage(payload), "output_dataset_rids": outputs})
	publisher, _ := uuid.Parse(createdBy)
	var existing handler.PublishedJobSpec
	var storedPayload, storedInputs []byte
	dedupErr := r.db.QueryRow(ctx, `SELECT rid, logic_kind, content_hash, logic_payload, inputs FROM job_specs WHERE `+dedupWhere+` ORDER BY created_at DESC LIMIT 1`, dedupArgs...).Scan(&existing.RID, &existing.LogicKind, &existing.ContentHash, &storedPayload, &storedInputs)
	if dedupErr != nil && !errors.Is(dedupErr, pgx.ErrNoRows) {
		return handler.PublishedJobSpec{}, dedupErr
	}
	if dedupErr == nil {
		// Identical content was published before (A→B→A): reuse the
		// historical job_specs row BUT rebind the branch mapping to it —
		// returning early would leave pipeline_job_specs bound to the
		// intervening definition while the API answered with this rid.
		//
		// For pinned inputs or typed outputs, ensure the branch mapping and
		// the canonical JobSpec restored by LoadPlan agree on the frozen
		// definition, even when a caller supplied a colliding hash.
		if jobSpecPinnedReuseConflict(storedPayload, storedInputs, payload, inputsRaw) {
			return handler.PublishedJobSpec{}, fmt.Errorf("job_spec_content_conflict: existing spec with content hash %s (rid %s) has a different frozen dataset_snapshot or output_schema_version definition; publish with its own content hash", contentHash, existing.RID)
		}
		if len(outputs) == 0 {
			return existing, nil
		}
		if beginner, ok := r.db.(txBeginner); ok {
			tx, err := beginner.Begin(ctx)
			if err != nil {
				return handler.PublishedJobSpec{}, err
			}
			defer tx.Rollback(ctx) //nolint:errcheck
			txCtx := context.WithValue(ctx, ctxTxKey{}, tx.(DB))
			if err := r.upsertPipelineJobSpecMappings(txCtx, req, outputs, inputsRaw, jobSpecJSON, existing.ContentHash, existing.RID, publisher, tenantID); err != nil {
				return handler.PublishedJobSpec{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return handler.PublishedJobSpec{}, err
			}
			return existing, nil
		}
		if len(outputs) > 1 {
			return handler.PublishedJobSpec{}, errors.New("multi-output JobSpec mapping requires a database transaction")
		}
		if err := r.upsertPipelineJobSpecMappings(ctx, req, outputs, inputsRaw, jobSpecJSON, existing.ContentHash, existing.RID, publisher, tenantID); err != nil {
			return handler.PublishedJobSpec{}, err
		}
		return existing, nil
	}
	if len(outputs) == 0 {
		if _, err := r.db.Exec(ctx, `INSERT INTO job_specs (id, rid, pipeline_rid, branch_name, logic_kind, inputs, output_dataset_rids, logic_payload, content_hash, created_by, tenant_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id, rid, req.PipelineRID, req.BranchName, kind, inputsRaw, outputs, payload, contentHash, createdBy, tenantID); err != nil {
			return handler.PublishedJobSpec{}, err
		}
		return handler.PublishedJobSpec{RID: rid, LogicKind: kind, ContentHash: contentHash}, nil
	}
	// Dual-table publish in ONE local transaction so the branch-mapping
	// row and the canonical job_specs row land (or roll back) together.
	// pipeline_job_specs.job_spec_rid carries the CURRENT canonical
	// job_specs.rid: a same-key definition change rebinds new builds to
	// the new rid while jobs already persisted keep the old one.
	execInTx := func(db DB) error {
		txCtx := context.WithValue(ctx, ctxTxKey{}, db)
		if _, err := db.Exec(txCtx, `INSERT INTO job_specs (id, rid, pipeline_rid, branch_name, logic_kind, inputs, output_dataset_rids, logic_payload, content_hash, created_by, tenant_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id, rid, req.PipelineRID, req.BranchName, kind, inputsRaw, outputs, payload, contentHash, createdBy, tenantID); err != nil {
			return err
		}
		return r.upsertPipelineJobSpecMappings(txCtx, req, outputs, inputsRaw, jobSpecJSON, contentHash, rid, publisher, tenantID)
	}
	if beginner, ok := r.db.(txBeginner); ok {
		tx, err := beginner.Begin(ctx)
		if err != nil {
			return handler.PublishedJobSpec{}, err
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		if err := execInTx(tx); err != nil {
			return handler.PublishedJobSpec{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return handler.PublishedJobSpec{}, err
		}
	} else {
		if len(outputs) > 1 {
			return handler.PublishedJobSpec{}, errors.New("multi-output JobSpec publishing requires a database transaction")
		}
		if err := execInTx(r.db); err != nil {
			return handler.PublishedJobSpec{}, err
		}
	}
	return handler.PublishedJobSpec{RID: rid, LogicKind: kind, ContentHash: contentHash}, nil
}

// upsertPipelineJobSpecMappings rebinds every declared output to the
// given canonical job_spec rid (used for fresh publishes AND dedup hits
// so A→B→A republishes leave the mapping pointing at the rid the API
// returned). db must be the surrounding transaction when one is open.
func (r *Repository) upsertPipelineJobSpecMappings(ctx context.Context, req handler.CreateJobSpecRequest, outputs []string, inputsRaw []byte, jobSpecJSON []byte, contentHash, canonicalRID string, publisher uuid.UUID, tenantID *uuid.UUID) error {
	db := r.db
	if tx, ok := txFromContext(ctx); ok {
		db = tx
	}
	// Keep lock order stable for concurrent publishes with shared outputs.
	sortedOutputs := append([]string(nil), outputs...)
	sort.Strings(sortedOutputs)
	for _, keyOutput := range sortedOutputs {
		_, err := db.Exec(ctx, `INSERT INTO pipeline_job_specs (id, pipeline_rid, branch_name, output_dataset_rid, output_branch, job_spec_json, inputs, content_hash, published_by, tenant_id, job_spec_rid)
VALUES ($1,$2,$3,$4,$3,$5,$6,$7,$8,$9,$10)
ON CONFLICT (pipeline_rid, branch_name, output_dataset_rid,
    COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::uuid),
    (CASE WHEN tenant_id IS NULL THEN published_by ELSE '00000000-0000-0000-0000-000000000000'::uuid END)) DO UPDATE SET
  job_spec_json=EXCLUDED.job_spec_json,
  inputs=EXCLUDED.inputs,
  content_hash=EXCLUDED.content_hash,
  job_spec_rid=EXCLUDED.job_spec_rid,
  version=CASE WHEN pipeline_job_specs.content_hash=EXCLUDED.content_hash THEN pipeline_job_specs.version ELSE pipeline_job_specs.version + 1 END,
  published_at=NOW()
`, uuid.New(), req.PipelineRID, req.BranchName, keyOutput, jobSpecJSON, inputsRaw, contentHash, publisher, tenantID, canonicalRID)
		if err != nil {
			return err
		}
	}
	return nil
}

// txFromContext lets helpers join the caller's open transaction.
type ctxTxKey struct{}

func txFromContext(ctx context.Context) (DB, bool) {
	tx, ok := ctx.Value(ctxTxKey{}).(DB)
	return tx, ok
}

func (r *Repository) AppendLogByRID(ctx context.Context, jobRID string, level livellogs.LogLevel, message string, params json.RawMessage) (livellogs.LogEntry, error) {
	jobID, err := r.resolveJobID(ctx, jobRID)
	if err != nil {
		return livellogs.LogEntry{}, err
	}
	if jobID == uuid.Nil {
		return livellogs.LogEntry{}, pgx.ErrNoRows
	}
	return r.AppendLog(ctx, jobID, level, message, params)
}

// JobRIDInScope reports whether jobRID resolves to a job that is visible
// under the caller's tenant scope. Used to gate live log subscriptions.
func (r *Repository) JobRIDInScope(ctx context.Context, jobRID string) (bool, error) {
	id, err := r.resolveJobID(ctx, jobRID)
	if err != nil {
		return false, err
	}
	return id != uuid.Nil, nil
}

// resolveJobID maps a job RID (or bare UUID) to its job id, enforcing the
// owning build's tenant boundary for scoped callers. uuid.Nil with a nil
// error means "no such job in this scope".
func (r *Repository) resolveJobID(ctx context.Context, jobRID string) (uuid.UUID, error) {
	scope := scopeFromContext(ctx)
	if parsed, err := uuid.Parse(jobRID); err == nil {
		if scope == nil {
			return parsed, nil
		}
		// A bare job UUID is still a subresource of its build: verify
		// the build boundary before resolving it.
		where := "id=$1"
		args := []any{parsed}
		sb := &sqlBuilder{args: args}
		appendBuildParentClause(sb, scope, "jobs")
		var id uuid.UUID
		err := r.db.QueryRow(ctx, `SELECT id FROM jobs WHERE `+where+sb.whereSQL(), sb.args...).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, nil
		}
		return id, err
	}
	where := "rid=$1"
	args := []any{jobRID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendBuildParentClause(sb, scope, "jobs")
		where += sb.whereSQL()
		args = sb.args
	}
	var id uuid.UUID
	err := r.db.QueryRow(ctx, `SELECT id FROM jobs WHERE `+where, args...).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil
	}
	return id, err
}

func deriveJobSpecHash(kind string, req handler.CreateJobSpecRequest) string {
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte("|"))
	h.Write([]byte(req.PipelineRID))
	h.Write([]byte("|"))
	h.Write([]byte(req.BranchName))
	h.Write([]byte("|"))
	h.Write(req.LogicPayload)
	for _, output := range req.OutputDatasetRIDs {
		h.Write([]byte("|"))
		h.Write([]byte(output))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// jobSpecPinnedReuseConflict reports whether reusing an existing canonical
// job_specs row for this publish would change a dataset_snapshot pin.
// Unpinned publishes keep the historical hash-dedup behavior untouched;
// when EITHER side declares a pin (detected along the config chain with
// the runtime's exact key semantics via the shared handler probe), reuse
// requires the logic_payload AND the declared inputs to be equal —
// anything else would bind build planning (branch mapping) and LoadPlan
// (canonical row) to different pinned definitions under one rid.
func jobSpecPinnedReuseConflict(storedPayload, storedInputs, newPayload, newInputs []byte) bool {
	if !handler.PayloadDeclaresDatasetSnapshot(storedPayload) && !handler.PayloadDeclaresDatasetSnapshot(newPayload) &&
		!handler.PayloadDeclaresDatasetOutputSchema(storedPayload) && !handler.PayloadDeclaresDatasetOutputSchema(newPayload) {
		return false
	}
	return !jsonEqualBytes(storedPayload, newPayload) || !jsonEqualBytes(storedInputs, newInputs)
}

// jsonEqualBytes compares two JSON documents by canonical re-encoding, so
// semantically identical payloads with different key order or whitespace
// still dedupe while any value difference (a pin change) does not.
// UseNumber keeps numeric literals as text — a float64 round-trip would
// erase exactly the big-integer differences a pin must catch.
func jsonEqualBytes(a, b []byte) bool {
	if string(a) == string(b) {
		return true
	}
	decode := func(p []byte) (any, bool) {
		dec := json.NewDecoder(bytes.NewReader(p))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, false
		}
		if dec.More() {
			return nil, false // trailing content: not one document
		}
		return v, true
	}
	va, okA := decode(a)
	vb, okB := decode(b)
	if !okA || !okB {
		return false
	}
	na, _ := json.Marshal(va)
	nb, _ := json.Marshal(vb)
	return string(na) == string(nb)
}

// SaveSparkSubmission upserts the Rust-compatible SparkApplication submission row.
// The DO UPDATE arm only fires within the caller's own domain, so a
// caller-supplied pipeline_run_id cannot hijack a foreign tenant's row.
func (r *Repository) SaveSparkSubmission(ctx context.Context, submission handler.SparkSubmission) error {
	tag, err := r.db.Exec(ctx, `INSERT INTO pipeline_run_submissions
    (pipeline_run_id, spark_app_name, namespace, status, error_message, tenant_id, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (pipeline_run_id) DO UPDATE
SET spark_app_name=EXCLUDED.spark_app_name,
    namespace=EXCLUDED.namespace,
    status=EXCLUDED.status,
    error_message=EXCLUDED.error_message,
    submitted_at=NOW(),
    last_observed_at=NOW()
WHERE pipeline_run_submissions.tenant_id IS NOT DISTINCT FROM EXCLUDED.tenant_id
  AND (EXCLUDED.tenant_id IS NOT NULL
       OR pipeline_run_submissions.created_by IS NOT DISTINCT FROM EXCLUDED.created_by)`,
		submission.PipelineRunID, submission.SparkAppName, submission.Namespace, string(submission.Status), submission.ErrorMessage, tenantValueForInsert(ctx), submitterForInsert(ctx))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return handler.ErrSparkSubmissionDomainConflict
	}
	return nil
}

// GetSparkSubmission loads the persisted SparkApplication control-plane mapping.
func (r *Repository) GetSparkSubmission(ctx context.Context, pipelineRunID uuid.UUID) (*handler.SparkSubmission, error) {
	where := "pipeline_run_id=$1"
	args := []any{pipelineRunID}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipeline_run_submissions")
		where += sb.whereSQL()
		args = sb.args
	}
	var sub handler.SparkSubmission
	var status string
	err := r.db.QueryRow(ctx, `SELECT pipeline_run_id, namespace, spark_app_name, status, error_message, submitted_at, last_observed_at
FROM pipeline_run_submissions WHERE `+where, args...).Scan(&sub.PipelineRunID, &sub.Namespace, &sub.SparkAppName, &status, &sub.ErrorMessage, &sub.SubmittedAt, &sub.LastObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sub.Status = sparkStatus(status)
	return &sub, nil
}

// SparkSubmissionByName resolves a submission by its cluster-side
// (namespace, spark_app_name) pair under the caller's tenant boundary, so
// Spark status lookups cannot probe foreign tenants by guessing names.
func (r *Repository) SparkSubmissionByName(ctx context.Context, namespace, name string) (*handler.SparkSubmission, error) {
	where := "namespace=$1 AND spark_app_name=$2"
	args := []any{namespace, name}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipeline_run_submissions")
		where += sb.whereSQL()
		args = sb.args
	}
	var sub handler.SparkSubmission
	var status string
	err := r.db.QueryRow(ctx, `SELECT pipeline_run_id, namespace, spark_app_name, status, error_message, submitted_at, last_observed_at
FROM pipeline_run_submissions WHERE `+where, args...).Scan(&sub.PipelineRunID, &sub.Namespace, &sub.SparkAppName, &status, &sub.ErrorMessage, &sub.SubmittedAt, &sub.LastObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sub.Status = sparkStatus(status)
	return &sub, nil
}

// UpdateSparkSubmissionStatus refreshes the observed SparkApplication status.
func (r *Repository) UpdateSparkSubmissionStatus(ctx context.Context, pipelineRunID uuid.UUID, status sparkpkg.SparkRunStatus, errorMessage *string) error {
	where := "pipeline_run_id=$1"
	args := []any{pipelineRunID, string(status), errorMessage}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipeline_run_submissions")
		where += sb.whereSQL()
		args = sb.args
	}
	_, err := r.db.Exec(ctx, `UPDATE pipeline_run_submissions
SET status=$2, error_message=$3, last_observed_at=NOW()
WHERE `+where, args...)
	return err
}

// ListSparkSubmissions returns recent SparkApplication submissions.
func (r *Repository) ListSparkSubmissions(ctx context.Context, limit int64) ([]handler.SparkSubmission, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	where := "TRUE"
	args := []any{limit}
	if scope := scopeFromContext(ctx); scope != nil {
		sb := &sqlBuilder{args: args}
		appendTenantClause(sb, scope, "pipeline_run_submissions")
		where += sb.whereSQL()
		args = sb.args
	}
	rows, err := r.db.Query(ctx, `SELECT pipeline_run_id, namespace, spark_app_name, status, error_message, submitted_at, last_observed_at
FROM pipeline_run_submissions
WHERE `+where+`
ORDER BY submitted_at DESC
LIMIT $1`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []handler.SparkSubmission{}
	for rows.Next() {
		var sub handler.SparkSubmission
		var status string
		if err := rows.Scan(&sub.PipelineRunID, &sub.Namespace, &sub.SparkAppName, &status, &sub.ErrorMessage, &sub.SubmittedAt, &sub.LastObservedAt); err != nil {
			return nil, err
		}
		sub.Status = sparkStatus(status)
		items = append(items, sub)
	}
	return items, rows.Err()
}

func sparkStatus(status string) sparkpkg.SparkRunStatus {
	switch sparkpkg.SparkRunStatus(status) {
	case sparkpkg.SparkRunSubmitted, sparkpkg.SparkRunRunning, sparkpkg.SparkRunSucceeded, sparkpkg.SparkRunFailed, sparkpkg.SparkRunUnknown:
		return sparkpkg.SparkRunStatus(status)
	default:
		return sparkpkg.SparkRunUnknown
	}
}
