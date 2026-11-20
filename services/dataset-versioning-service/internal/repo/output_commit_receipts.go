// P2.6 W1 — idempotent keyed dataset output commits.
//
// WithIdempotentOutputCommit runs a keyed outputs:commit inside a single
// outer READ COMMITTED transaction. The trusted tenant predicate locks the
// target datasets row FOR UPDATE before any new transaction INSERT (and its
// FK share lock) can happen; the very next statement — a separate SELECT,
// never a CTE fused with the lock, because each READ COMMITTED statement
// takes a fresh snapshot — reads the receipt. A committed receipt with the
// same fingerprint is returned as-is with zero side effects: no branch,
// backing or lineage existence checks, no head/index writes. Without a
// receipt the whole Start/Stage/Merge/Publish pipeline runs through a
// tx-bound Repo (its Begin/Commit are pgx SAVEPOINT/RELEASE), the response
// is serialized exactly once, and the receipt INSERT lands in the same outer
// transaction as every publish write — one real COMMIT decides everything.
//
// Failure handling follows the P2.6 root decisions: ctx cancellation, process
// death or any build error rolls the entire transaction back (no OPEN
// transaction, staged rows or receipt survive; unreferenced physical objects
// of the failed attempt stay isolated under its fresh random txnUUID), the
// cleanup rollback runs on its own bounded context and never calls the
// global AbortTransaction, and a lost COMMIT acknowledgement is surfaced as
// ErrCommitOutcomeUnknown — never guessed to be a rollback — so the caller
// retries the same key+body or queries the receipt.
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// ErrIdempotencyKeyConflict reports that the idempotency key is already
// durably bound to a different request body (fingerprint mismatch). Zero
// side effects: the transaction rolls back untouched.
var ErrIdempotencyKeyConflict = errors.New("idempotency key is bound to a different request body")

// ErrCommitOutcomeUnknown reports that the outer transaction's COMMIT
// returned an error (e.g. lost acknowledgement): the commit may or may not
// have landed. Callers must not assume a rollback — retry the same key and
// body, or query the receipt first.
var ErrCommitOutcomeUnknown = errors.New("output commit outcome is unknown; retry with the same idempotency key")

// DefaultOutputCommitLockTimeout bounds how long a keyed commit waits on the
// datasets row lock before failing (PG lock_timeout). Concurrent keyed
// commits on one dataset serialize here; the wait is also cancellable
// through the caller's context.
const DefaultOutputCommitLockTimeout = 10 * time.Second

// outputCommitRollbackTimeout bounds the cleanup rollback after a failed
// build: it runs on a fresh context so a cancelled request context cannot
// leak the transaction open.
const outputCommitRollbackTimeout = 5 * time.Second

// OutputCommitTxStore is the transaction-scoped store surface handed to the
// keyed commit build callback. *Repo satisfies it when bound to the open
// transaction; every Start/Stage/Merge/Publish call of the build must go
// through this view, never through the global Repo.
type OutputCommitTxStore interface {
	EnsureDefaultBranch(ctx context.Context, dataset *models.Dataset) error
	GetRuntimeBranch(ctx context.Context, datasetID uuid.UUID, branch string) (*models.RuntimeBranch, error)
	ResolveDatasetForTenant(ctx context.Context, raw string, scope TenantScope) (*models.Dataset, error)
	GetTransactionStatus(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID) (string, bool, error)
	StartTransaction(ctx context.Context, datasetID uuid.UUID, branchID uuid.UUID, branchName string, txType models.TransactionType, summary string, providence models.JSONValue, startedBy uuid.UUID) (*models.RuntimeTransaction, error)
	StageTransactionFiles(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, files []models.StageTransactionFile) error
	MergeTransactionMetadata(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, metadata models.JSONValue) error
	CommitTransactionWithPublish(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID, files []models.PutDatasetFileIndexEntry, links []models.PutDatasetLineageLink) (*PublishSnapshot, error)
	// WP3.3 staged commits (all row locks are taken inside this outer
	// transaction, after the datasets row lock):
	GetRuntimeTransactionForUpdate(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) (*models.RuntimeTransaction, error)
	AbortReservedTransaction(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) error
	LoadStagedObjectForUpdate(ctx context.Context, datasetID uuid.UUID, stagedObjectID uuid.UUID) (*StagedOutputObject, error)
	ConsumeStagedObject(ctx context.Context, stagedObjectID uuid.UUID, consumingTxn uuid.UUID) error
}

// outputCommitReceiptSelect qualifies every column with the `r` alias: the
// GET form joins datasets (which also has id and created_at columns), so an
// unqualified reference would be an ambiguity error (PG 42702) on a real
// database.
const outputCommitReceiptSelect = `SELECT r.dataset_id, r.idempotency_key, r.request_fingerprint,
	r.branch_name, r.transaction_id, r.transaction, r.response_body, r.created_at
	FROM dataset_output_commit_receipts r`

// WithIdempotentOutputCommit executes one keyed outputs:commit attempt.
// replayed=true means a committed receipt with the same fingerprint was
// found and its stored bytes must be served verbatim (HTTP 200); false means
// this call performed the first commit and its receipt became durable in the
// single outer COMMIT (HTTP 201). See the package comment for the lock
// ordering and failure semantics.
func (r *Repo) WithIdempotentOutputCommit(ctx context.Context, scope TenantScope, datasetID uuid.UUID, key string, fingerprint string, build func(ctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error)) (*models.OutputCommitReceipt, bool, error) {
	tx, err := r.beginOutputCommit(ctx)
	if err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackOutputCommit(tx)
		}
	}()
	if err := lockOutputCommitDataset(ctx, tx, scope, datasetID); err != nil {
		return nil, false, err
	}

	// Statement 2 — receipt lookup as its own statement so its READ
	// COMMITTED snapshot is taken after the lock was acquired: a receipt
	// committed by a peer while we waited on the lock is visible here.
	receipt, err := loadOutputCommitReceipt(ctx, tx, datasetID, key)
	if err != nil {
		return nil, false, err
	}
	if receipt != nil {
		if receipt.RequestFingerprint != fingerprint {
			return nil, false, ErrIdempotencyKeyConflict
		}
		// Replay: serve the stored bytes. Nothing was written; the deferred
		// rollback just releases the lock. Branch, backing and lineage are
		// deliberately not re-checked and head/index stay untouched.
		return receipt, true, nil
	}

	// No receipt: run the full commit pipeline inside this transaction.
	// The build callback must issue every store call through txStore.
	txStore := &Repo{Pool: tx, OutputCommitLockTimeout: r.OutputCommitLockTimeout}
	response, err := build(ctx, txStore)
	if err != nil {
		return nil, false, err
	}
	receipt, err = writeOutputCommitReceipt(ctx, tx, datasetID, key, fingerprint, response)
	if err != nil {
		return nil, false, err
	}
	if err := commitOutputTransaction(ctx, tx); err != nil {
		return nil, false, err
	}
	committed = true
	receipt.CreatedAt = time.Now().UTC()
	return receipt, false, nil
}

func (r *Repo) beginOutputCommit(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	// Pin the isolation level explicitly (must precede the first data
	// statement): the replay snapshot logic relies on READ COMMITTED's
	// per-statement snapshots, whatever the server default is. The DB
	// interface stays untouched — txRepo needs a plain pgx.Tx.
	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`); err != nil {
		rollbackOutputCommit(tx)
		return nil, err
	}
	// Bound the datasets-row lock wait. lock_timeout accepts no parameters;
	// the value is formatted from an integer duration, not caller input.
	// A sub-millisecond positive duration must not format as '0ms' — PG
	// treats 0 as disabled (wait forever) — so it is clamped to 1ms.
	lockTimeout := r.OutputCommitLockTimeout
	if lockTimeout <= 0 {
		lockTimeout = DefaultOutputCommitLockTimeout
	}
	ms := lockTimeout.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", ms)); err != nil {
		rollbackOutputCommit(tx)
		return nil, err
	}

	return tx, nil
}

func rollbackOutputCommit(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), outputCommitRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func lockOutputCommitDataset(ctx context.Context, tx pgx.Tx, scope TenantScope, datasetID uuid.UUID) error {
	// Statement 1 — tenant-scoped row lock, taken before any new transaction
	// INSERT and its FK share lock. Cross-tenant and missing datasets are
	// indistinguishable (ErrNotFound).
	var lockedID uuid.UUID
	err := tx.QueryRow(ctx,
		`SELECT id FROM datasets WHERE id = $1 AND `+tenantVisibility("$2", "$3")+` FOR UPDATE`,
		datasetID, scope.OrgID, scope.Subject).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func commitOutputTransaction(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		// The peer may already have committed. Retry the same bytes or read the receipt.
		return fmt.Errorf("%w: %s", ErrCommitOutcomeUnknown, err.Error())
	}
	return nil
}

func writeOutputCommitReceipt(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, key, fingerprint string, response models.CommitDatasetOutputResponse) (*models.OutputCommitReceipt, error) {
	response.IdempotencyKey = key
	response.RequestFingerprint = fingerprint

	// Serialize the response exactly once: these bytes are what the first
	// caller receives, what lands in the receipt, and what every replay and
	// GET returns — byte-stable by construction.
	body, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	txnBody, err := json.Marshal(response.Transaction)
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `INSERT INTO dataset_output_commit_receipts
		(id, dataset_id, idempotency_key, request_fingerprint, branch_name, transaction_id, transaction, response_body)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)`,
		uuid.New(), datasetID, key, fingerprint, response.Branch, response.Transaction.ID, txnBody, string(body)); err != nil {
		return nil, err
	}

	return &models.OutputCommitReceipt{
		DatasetID:          datasetID,
		IdempotencyKey:     key,
		RequestFingerprint: fingerprint,
		Branch:             response.Branch,
		Transaction:        response.Transaction,
		ResponseBody:       body,
		CreatedAt:          time.Now().UTC(),
	}, nil
}

// GetOutputCommitReceipt loads the committed receipt for (dataset, key)
// under the caller's tenant scope. nil means no committed receipt is visible
// — which does NOT imply no operation is in flight. Never queries by key
// across datasets or tenants. The tenant predicate is spelled with the d.
// qualifier: both joined tables carry id/created_at columns, so this query
// must stay fully qualified (an unqualified copy of tenantVisibility would
// compile fine and fail with 42702 on a real database).
func (r *Repo) GetOutputCommitReceipt(ctx context.Context, scope TenantScope, datasetID uuid.UUID, key string) (*models.OutputCommitReceipt, error) {
	row := r.Pool.QueryRow(ctx,
		outputCommitReceiptSelect+`
		JOIN datasets d ON d.id = r.dataset_id
		WHERE r.dataset_id = $1 AND r.idempotency_key = $2
		  AND (d.org_id = $3 OR (d.org_id IS NULL AND d.owner_id = $4 AND $3 IS NULL))`,
		datasetID, key, scope.OrgID, scope.Subject)
	receipt, err := scanOutputCommitReceipt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

// loadOutputCommitReceipt reads one receipt by (dataset, key) on the open
// transaction — the tenant predicate was already enforced by the locked
// datasets row, so no join is needed here.
func loadOutputCommitReceipt(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, key string) (*models.OutputCommitReceipt, error) {
	row := tx.QueryRow(ctx,
		outputCommitReceiptSelect+` WHERE r.dataset_id = $1 AND r.idempotency_key = $2`, datasetID, key)
	receipt, err := scanOutputCommitReceipt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

func scanOutputCommitReceipt(r rowLikeT) (*models.OutputCommitReceipt, error) {
	v := &models.OutputCommitReceipt{}
	var txnBody []byte
	var body []byte
	if err := r.Scan(&v.DatasetID, &v.IdempotencyKey, &v.RequestFingerprint,
		&v.Branch, &v.Transaction.ID, &txnBody, &body, &v.CreatedAt); err != nil {
		return nil, err
	}
	v.ResponseBody = json.RawMessage(body)
	return v, json.Unmarshal(txnBody, &v.Transaction)
}
