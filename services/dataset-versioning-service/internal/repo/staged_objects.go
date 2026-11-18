// WP3.3 increment 3A — durable staging descriptors for sealed NDJSON
// output objects and the reservation protocol that guards them.
//
// The single-artifact model: one row per reservation
// (dataset_staged_output_objects.reservation_id is UNIQUE). A row moves
// STAGING -> SEALED -> CONSUMED and never backwards; there is no supersede
// chain, so repeated re-transmissions can only reuse the sealed descriptor
// (byte-identical content) or fail with an explicit conflict.
//
// Lock order everywhere is the publish order: the tenant-scoped datasets
// row FOR UPDATE first (the descriptor INSERT's foreign key also locks the
// dataset, so locking it any later could deadlock against a publish),
// then the current branch row behind the name, then the exact reservation,
// then the descriptor. The methods run on *Repo whether its Pool is the
// connection pool (stage path) or the keyed commit's outer transaction.

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

// Staged object row states.
const (
	StagedObjectStatusStaging  = "STAGING"
	StagedObjectStatusSealed   = "SEALED"
	StagedObjectStatusConsumed = "CONSUMED"
)

// StagedReservationPurpose is the providence marker a WP3.3 reservation
// must carry; together with its build_id it binds the reservation to one
// build. Legacy providence (no marker) can never satisfy a staged binding.
const StagedReservationPurpose = "wp33-staged-output"

var (
	// ErrStagedReservationInvalid reports a binding or state mismatch on
	// the reservation (wrong branch/type/status/actor, a branch that was
	// recreated under the same name, or a missing or foreign build
	// marker). Zero side effects.
	ErrStagedReservationInvalid = errors.New("staged reservation binding is invalid")
	// ErrStagedUploadInFlight reports a live lease held by another upload
	// attempt on the same reservation.
	ErrStagedUploadInFlight = errors.New("another staged upload is in flight for this reservation")
	// ErrStagedObjectAlreadyPublished reports a re-stage against a
	// reservation whose staged object was already consumed by a commit.
	ErrStagedObjectAlreadyPublished = errors.New("staged object for this reservation was already published")
	// ErrStagedObjectConflict reports a re-transmission whose content
	// differs from the live sealed object of the same reservation.
	ErrStagedObjectConflict = errors.New("a different staged object is already sealed for this reservation")
	// ErrStagedObjectNotSealed reports a staged object referenced by a
	// commit that is not in the SEALED state (still staging or consumed).
	ErrStagedObjectNotSealed = errors.New("staged object is not sealed")
	// ErrStagedObjectExpired reports a sealed object whose TTL elapsed.
	ErrStagedObjectExpired = errors.New("staged object has expired")
	// ErrStagedBindingMismatch reports a commit-side descriptor binding
	// that does not equal the declared request binding.
	ErrStagedBindingMismatch = errors.New("staged object binding does not match the request")
	// ErrStagedDigestMismatch reports declared size/sha256 disagreeing
	// with the sealed authoritative values.
	ErrStagedDigestMismatch = errors.New("declared size or sha256 does not match the sealed staged object")
	// ErrStagedLeaseLost reports a sealing attempt whose STAGING row no
	// longer belongs to it, or whose lease expired before the seal.
	ErrStagedLeaseLost = errors.New("staging lease expired or was taken over by another attempt")
)

// StagedOutputObject mirrors one dataset_staged_output_objects row.
type StagedOutputObject struct {
	ID                    uuid.UUID
	DatasetID             uuid.UUID
	BranchName            string
	ReservationID         uuid.UUID
	BuildID               uuid.UUID
	ActorID               uuid.UUID
	IdempotencyKey        string
	LogicalPath           string
	Status                string
	LeaseExpiresAt        *time.Time
	SizeBytes             int64
	SHA256                string
	RecordCount           int64
	SealedPath            string
	CreatedAt             time.Time
	SealedAt              *time.Time
	ExpiresAt             *time.Time
	ConsumedAt            *time.Time
	ConsumedByTransaction *uuid.UUID
}

// StagedBinding is the full binding a stage upload or STAGED_ADD commit
// declares; both sides must agree on every field.
type StagedBinding struct {
	DatasetID      uuid.UUID
	BranchName     string
	ReservationID  uuid.UUID
	BuildID        uuid.UUID
	ActorID        uuid.UUID
	IdempotencyKey string
	LogicalPath    string
}

// stagedProvidenceBuildID extracts the reservation marker: providence must
// be a JSON object carrying purpose=StagedReservationPurpose and a UUID
// build_id. Anything else (including legacy providence) is not a staged
// reservation.
func stagedProvidenceBuildID(providence models.JSONValue) (uuid.UUID, bool) {
	if len(providence) == 0 {
		return uuid.Nil, false
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(providence, &raw); err != nil {
		return uuid.Nil, false
	}
	var purpose string
	if v, ok := raw["purpose"]; ok {
		_ = json.Unmarshal(v, &purpose)
	}
	if purpose != StagedReservationPurpose {
		return uuid.Nil, false
	}
	var buildID string
	if v, ok := raw["build_id"]; ok {
		_ = json.Unmarshal(v, &buildID)
	}
	id, err := uuid.Parse(buildID)
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// lockDatasetForStaging takes the tenant-scoped datasets row lock every
// staged operation starts with — the same first lock a publish takes —
// and re-reads the dataset's CURRENT active branch under that lock: a
// name that stopped being the active branch between the caller's fail-fast
// check and here is a binding failure, not a redirect.
func lockDatasetForStaging(ctx context.Context, tx pgx.Tx, scope TenantScope, datasetID uuid.UUID, branchName string) error {
	var lockedID uuid.UUID
	var activeBranch *string
	err := tx.QueryRow(ctx,
		`SELECT id, active_branch FROM datasets WHERE id = $1 AND `+tenantVisibility("$2", "$3")+` FOR UPDATE`,
		datasetID, scope.OrgID, scope.Subject).Scan(&lockedID, &activeBranch)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if activeBranch == nil || *activeBranch != branchName {
		return ErrStagedReservationInvalid
	}
	return nil
}

// activeBranchIDFor returns the id of the LIVE branch row the dataset
// currently knows under this name (soft-deleted and archived rows are
// filtered exactly like the canonical GetRuntimeBranch read). A
// reservation whose branch_id points at a soft-deleted predecessor
// recreated under the same name fails ValidateStagedReservation.
func activeBranchIDFor(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, branchName string) (uuid.UUID, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM dataset_branches WHERE dataset_id = $1 AND name = $2 AND deleted_at IS NULL AND archived_at IS NULL`, datasetID, branchName).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrStagedReservationInvalid
	}
	return id, err
}

// ValidateStagedReservation checks the locked reservation row against the
// declared binding: exact dataset (foreign rows are plain ErrNotFound so
// existence never leaks), the CURRENT branch row behind the name (UUID and
// name must both match), OPEN state, UPDATE type, same actor, and the
// build marker. A nil currentBranchID skips the UUID cross-check (the
// keyed commit path passes the already-locked branch id explicitly).
func ValidateStagedReservation(resv *models.RuntimeTransaction, b StagedBinding, currentBranchID uuid.UUID) error {
	if resv == nil || resv.DatasetID != b.DatasetID {
		return ErrNotFound
	}
	if resv.BranchName != b.BranchName ||
		(currentBranchID != uuid.Nil && resv.BranchID != currentBranchID) ||
		resv.Status != models.TransactionStatusOpen ||
		resv.TxType != models.TransactionTypeUpdate ||
		resv.StartedBy == nil || *resv.StartedBy != b.ActorID {
		return ErrStagedReservationInvalid
	}
	buildID, ok := stagedProvidenceBuildID(resv.Providence)
	if !ok || buildID != b.BuildID {
		return ErrStagedReservationInvalid
	}
	return nil
}

// GetRuntimeTransactionForUpdate loads one transaction row of the dataset
// under a row lock, for callers that must serialize against the keyed
// commit's reservation takeover (lock order: datasets row first, then this).
func (r *Repo) GetRuntimeTransactionForUpdate(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) (*models.RuntimeTransaction, error) {
	row := r.Pool.QueryRow(ctx, runtimeTransactionSelect()+` WHERE dataset_id = $1 AND id = $2 FOR UPDATE`, datasetID, txnID)
	v, err := scanRuntimeTransaction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// AbortReservedTransaction atomically finalizes exactly this OPEN
// reservation as ABORTED. It never touches transactions in any other
// state — ErrInvalidTransition means someone else already moved it.
func (r *Repo) AbortReservedTransaction(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) error {
	cmd, err := r.Pool.Exec(ctx, `UPDATE dataset_transactions
		SET status = 'ABORTED', aborted_at = NOW()
		WHERE dataset_id = $1 AND id = $2 AND status = 'OPEN'`, datasetID, txnID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

const stagedObjectSelect = `SELECT id, dataset_id, branch_name, reservation_id, build_id, actor_id,
	idempotency_key, logical_path, status, lease_expires_at, size_bytes, sha256, record_count,
	sealed_path, created_at, sealed_at, expires_at, consumed_at, consumed_by_transaction
	FROM dataset_staged_output_objects`

func scanStagedObject(r rowLikeT) (*StagedOutputObject, error) {
	v := &StagedOutputObject{}
	err := r.Scan(&v.ID, &v.DatasetID, &v.BranchName, &v.ReservationID, &v.BuildID, &v.ActorID,
		&v.IdempotencyKey, &v.LogicalPath, &v.Status, &v.LeaseExpiresAt, &v.SizeBytes, &v.SHA256,
		&v.RecordCount, &v.SealedPath, &v.CreatedAt, &v.SealedAt, &v.ExpiresAt, &v.ConsumedAt,
		&v.ConsumedByTransaction)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// AcquireStagedUploadLease takes the single in-flight upload slot of the
// reservation. Returned states: a fresh STAGING row for this attempt
// (supersededAttemptID = nil), a fresh STAGING row after taking over an
// expired attempt (supersededAttemptID points at the dead attempt — the
// caller may remove exactly that attempt's server-derived files after this
// transaction committed), or the existing SEALED row (sealed=true — a
// re-transmission that must be hashed against io.Discard and compared; it
// never gets a lease or a temp file).
//
// All lease judgements (live vs expired, takeover, SEALED TTL) and the
// new lease's TTL reuse ONE clock_timestamp() read taken AFTER every row
// lock this path needs: NOW()/CURRENT_TIMESTAMP would report this
// transaction's START — i.e. the time BEFORE waiting on any lock — so a
// lease or TTL that expires while this request waited would be judged on
// the stale side forever. The transaction pins READ COMMITTED like the
// keyed commit, so a row locked FOR UPDATE is re-read at its latest
// committed version after the wait.
func (r *Repo) AcquireStagedUploadLease(ctx context.Context, scope TenantScope, b StagedBinding, attemptID uuid.UUID, leaseTTL time.Duration) (obj *StagedOutputObject, supersededAttemptID *uuid.UUID, sealed bool, err error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			rbCtx, cancel := context.WithTimeout(context.Background(), outputCommitRollbackTimeout)
			defer cancel()
			_ = tx.Rollback(rbCtx)
		}
	}()

	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`); err != nil {
		return nil, nil, false, err
	}
	if err := lockDatasetForStaging(ctx, tx, scope, b.DatasetID, b.BranchName); err != nil {
		return nil, nil, false, err
	}
	branchID, err := activeBranchIDFor(ctx, tx, b.DatasetID, b.BranchName)
	if err != nil {
		return nil, nil, false, err
	}
	resv, err := loadRuntimeTxnForUpdate(ctx, tx, b.DatasetID, b.ReservationID)
	if err != nil {
		return nil, nil, false, err
	}
	if err := ValidateStagedReservation(resv, b, branchID); err != nil {
		return nil, nil, false, err
	}

	existing, err := scanStagedObject(tx.QueryRow(ctx, stagedObjectSelect+` WHERE reservation_id = $1 FOR UPDATE`, b.ReservationID))
	if err != nil {
		return nil, nil, false, err
	}
	// ONE pinned database wall clock, read AFTER every row lock this path
	// needs (datasets, branch, reservation, descriptor): NOW() would
	// report the transaction's start — before the lock waits — so leases
	// or TTLs that expire while this request waited on any lock would be
	// judged stale-side forever. Every branch below reuses this value.
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, nil, false, err
	}
	leaseExpires := now.Add(leaseTTL)
	if existing != nil {
		switch existing.Status {
		case StagedObjectStatusConsumed:
			return nil, nil, false, ErrStagedObjectAlreadyPublished
		case StagedObjectStatusSealed:
			// A re-transmission may only reuse the live artifact of the
			// very same binding — a different key, logical path or actor
			// is rejected here, before a single body byte is read — and
			// only while the artifact is still within its TTL (database
			// wall clock, taken under the locks).
			if existing.DatasetID != b.DatasetID || existing.BranchName != b.BranchName ||
				existing.ReservationID != b.ReservationID || existing.BuildID != b.BuildID ||
				existing.ActorID != b.ActorID || existing.IdempotencyKey != b.IdempotencyKey ||
				existing.LogicalPath != b.LogicalPath {
				return nil, nil, false, ErrStagedBindingMismatch
			}
			if existing.ExpiresAt != nil && now.After(*existing.ExpiresAt) {
				return nil, nil, false, ErrStagedObjectExpired
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, nil, false, fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
			}
			committed = true
			return existing, nil, true, nil
		default: // STAGING
			if existing.LeaseExpiresAt != nil && existing.LeaseExpiresAt.After(now) {
				return nil, nil, false, ErrStagedUploadInFlight
			}
			// Expired lease: hand the row to this attempt atomically. A
			// STAGING row is never referenced by anything, so replacing it
			// is safe; the caller removes only the dead attempt's own
			// files (never the whole reservation directory, which a
			// concurrent re-transmission may be hashing against).
			if _, err := tx.Exec(ctx, `DELETE FROM dataset_staged_output_objects WHERE id = $1 AND status = 'STAGING'`, existing.ID); err != nil {
				return nil, nil, false, err
			}
			fresh, err := insertStagingRow(ctx, tx, b, attemptID, leaseExpires)
			if err != nil {
				return nil, nil, false, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, nil, false, fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
			}
			committed = true
			return fresh, &existing.ID, false, nil
		}
	}
	fresh, err := insertStagingRow(ctx, tx, b, attemptID, leaseExpires)
	if err != nil {
		return nil, nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, false, fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
	}
	committed = true
	return fresh, nil, false, nil
}

func insertStagingRow(ctx context.Context, tx pgx.Tx, b StagedBinding, attemptID uuid.UUID, leaseExpires time.Time) (*StagedOutputObject, error) {
	_, err := tx.Exec(ctx, `INSERT INTO dataset_staged_output_objects
		(id, dataset_id, branch_name, reservation_id, build_id, actor_id, idempotency_key, logical_path,
		 status, lease_expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'STAGING',$9)`,
		attemptID, b.DatasetID, b.BranchName, b.ReservationID, b.BuildID, b.ActorID, b.IdempotencyKey, b.LogicalPath, leaseExpires)
	if err != nil {
		return nil, err
	}
	return scanStagedObject(tx.QueryRow(ctx, stagedObjectSelect+` WHERE id = $1`, attemptID))
}

// SealStagedOutput finalizes this attempt's STAGING row with the sealed
// digest. The reservation is revalidated under its lock, and the attempt
// must still own a live lease measured against the DATABASE wall clock
// read under the locks — never a timestamp captured before the upload —
// so a long upload that outlives its lease cannot seal, and the artifact
// TTL starts at the actual seal moment.
func (r *Repo) SealStagedOutput(ctx context.Context, scope TenantScope, b StagedBinding, attemptID uuid.UUID, sizeBytes int64, sha256Hex string, recordCount int64, sealedPath string, ttl time.Duration) (*StagedOutputObject, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if tx != nil {
			rbCtx, cancel := context.WithTimeout(context.Background(), outputCommitRollbackTimeout)
			defer cancel()
			_ = tx.Rollback(rbCtx)
		}
	}()

	// Same isolation pin as the keyed commit: READ COMMITTED re-reads a
	// FOR UPDATE row at its latest committed version after any lock wait.
	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`); err != nil {
		return nil, err
	}
	if err := lockDatasetForStaging(ctx, tx, scope, b.DatasetID, b.BranchName); err != nil {
		return nil, err
	}
	branchID, err := activeBranchIDFor(ctx, tx, b.DatasetID, b.BranchName)
	if err != nil {
		return nil, err
	}
	resv, err := loadRuntimeTxnForUpdate(ctx, tx, b.DatasetID, b.ReservationID)
	if err != nil {
		return nil, err
	}
	if err := ValidateStagedReservation(resv, b, branchID); err != nil {
		return nil, err
	}
	row, err := scanStagedObject(tx.QueryRow(ctx, stagedObjectSelect+` WHERE reservation_id = $1 FOR UPDATE`, b.ReservationID))
	if err != nil {
		return nil, err
	}
	if row == nil || row.ID != attemptID || row.Status != StagedObjectStatusStaging {
		return nil, ErrStagedLeaseLost
	}
	// clock_timestamp() AFTER the locks: NOW() would report this
	// transaction's start — before the lock wait — and a lease that
	// expired during the wait would look live.
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, err
	}
	if row.LeaseExpiresAt == nil || !row.LeaseExpiresAt.After(dbNow) {
		return nil, ErrStagedLeaseLost
	}
	if _, err := tx.Exec(ctx, `UPDATE dataset_staged_output_objects
		SET status = 'SEALED', size_bytes = $2, sha256 = $3, record_count = $4,
		    sealed_path = $5, sealed_at = $6, expires_at = $7
		WHERE id = $1 AND status = 'STAGING'`,
		attemptID, sizeBytes, sha256Hex, recordCount, sealedPath, dbNow, dbNow.Add(ttl)); err != nil {
		return nil, err
	}
	sealed, err := scanStagedObject(tx.QueryRow(ctx, stagedObjectSelect+` WHERE id = $1`, attemptID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		// Unknown COMMIT outcome: the SEALED row (and thus the reference
		// to sealedPath) may already be durable — callers must keep the
		// sealed file.
		return nil, fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
	}
	tx = nil
	return sealed, nil
}

// ReleaseStagedLease drops this attempt's STAGING row after a definite
// failure (before or during streaming). Best-effort: a taken-over row is
// simply gone already.
func (r *Repo) ReleaseStagedLease(ctx context.Context, attemptID uuid.UUID) error {
	_, err := r.Pool.Exec(ctx, `DELETE FROM dataset_staged_output_objects WHERE id = $1 AND status = 'STAGING'`, attemptID)
	return err
}

// LoadStagedObjectForUpdate loads one staged object of the dataset under a
// row lock, inside the keyed commit's transaction (lock order: datasets
// row, reservation, then this).
func (r *Repo) LoadStagedObjectForUpdate(ctx context.Context, datasetID uuid.UUID, stagedObjectID uuid.UUID) (*StagedOutputObject, error) {
	obj, err := scanStagedObject(r.Pool.QueryRow(ctx, stagedObjectSelect+` WHERE dataset_id = $1 AND id = $2 FOR UPDATE`, datasetID, stagedObjectID))
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, ErrNotFound
	}
	return obj, nil
}

// ConsumeStagedObject moves the SEALED row to CONSUMED inside the
// publishing transaction; ErrInvalidTransition means it was not SEALED
// (already consumed — impossible unless replayed past the receipt).
func (r *Repo) ConsumeStagedObject(ctx context.Context, stagedObjectID uuid.UUID, consumingTxn uuid.UUID) error {
	cmd, err := r.Pool.Exec(ctx, `UPDATE dataset_staged_output_objects
		SET status = 'CONSUMED', consumed_at = NOW(), consumed_by_transaction = $2
		WHERE id = $1 AND status = 'SEALED'`, stagedObjectID, consumingTxn)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

// loadRuntimeTxnForUpdate is the tx-scoped core of
// GetRuntimeTransactionForUpdate for use inside this package's own
// transactions.
func loadRuntimeTxnForUpdate(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, txnID uuid.UUID) (*models.RuntimeTransaction, error) {
	row := tx.QueryRow(ctx, runtimeTransactionSelect()+` WHERE dataset_id = $1 AND id = $2 FOR UPDATE`, datasetID, txnID)
	v, err := scanRuntimeTransaction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// ValidateStagedObjectForCommit checks the locked descriptor against the
// commit's declared binding, sealed state, TTL and declared digest.
func ValidateStagedObjectForCommit(obj *StagedOutputObject, b StagedBinding, stagedObjectID uuid.UUID, declaredSize int64, declaredSHA string, now time.Time) error {
	if obj == nil {
		return ErrNotFound
	}
	if obj.ID != stagedObjectID ||
		obj.DatasetID != b.DatasetID ||
		obj.BranchName != b.BranchName ||
		obj.ReservationID != b.ReservationID ||
		obj.BuildID != b.BuildID ||
		obj.ActorID != b.ActorID ||
		obj.IdempotencyKey != b.IdempotencyKey ||
		obj.LogicalPath != b.LogicalPath {
		return ErrStagedBindingMismatch
	}
	if obj.Status == StagedObjectStatusStaging {
		return ErrStagedObjectNotSealed
	}
	if obj.Status == StagedObjectStatusConsumed {
		return ErrStagedObjectAlreadyPublished
	}
	if obj.ExpiresAt != nil && now.After(*obj.ExpiresAt) {
		return ErrStagedObjectExpired
	}
	if obj.SizeBytes != declaredSize || obj.SHA256 != declaredSHA {
		return ErrStagedDigestMismatch
	}
	return nil
}
