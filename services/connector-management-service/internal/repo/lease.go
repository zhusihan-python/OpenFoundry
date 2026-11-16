// P2.1 durable sync-run tasks and leases. Every mutating lease operation is a
// short two-phase transaction: phase 1 locks the target row (FOR UPDATE /
// FOR SHARE) by id + tenant only, phase 2 — after the lock is held — issues
// separate statements whose clock_timestamp() quals are evaluated with the
// post-lock clock and whose TTL starts from that same clock. A
// single-statement CAS is NOT sufficient under READ COMMITTED: when the
// blocker holds FOR UPDATE without updating the row, the waiting statement
// never re-evaluates its quals (EvalPlanQual only runs for a blocker-produced
// new row version), so an expiry check evaluated before the wait would let an
// expired lease renew. The same reasoning applies to the active-period
// guards: the source/def row is locked first and the pending/running check is
// re-run after the lock, so an enqueue (or claim) that commits while a delete
// waits is actually observed by the re-check.
package repo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// LeaseClaim is the in-memory proof of holding a run's lease. Token is the
// unguessable per-claim secret: it never appears in any public JSON model and
// is only handed back to the lease CAS methods of this package. The fencing
// counters live on Run (Run.Generation / Run.Attempt) — deliberately NOT
// duplicated here, so a stale zero copy can never fence a valid holder.
type LeaseClaim struct {
	Run       models.SyncRun
	Token     string
	Holder    string
	ExpiresAt time.Time
}

// ErrActiveRuns reports that a source/sync mutation was refused because
// pending or running sync runs still exist for it.
var ErrActiveRuns = errors.New("source or sync has pending/running runs")

// errRunNotVisible marks phase 1 finding no run row in the caller's tenant.
var errRunNotVisible = errors.New("sync run not visible in tenant")

// Lease TTL bounds. The minimum (1ms) stays representable at PostgreSQL's
// microsecond timestamp precision — anything smaller would round the computed
// lease_expires_at onto (or before) the claiming clock and produce a lease
// that is born expired. The maximum keeps a buggy caller from pinning a run
// for a day.
const (
	minLeaseTTL = time.Millisecond
	maxLeaseTTL = 24 * time.Hour
)

// validateLeaseInstall rejects malformed claim/takeover inputs before any
// transaction opens: with invalid arguments not even a row lock is taken, so
// attempt/generation/heartbeat can never be touched by a rejected call.
func validateLeaseInstall(holder, token string, ttl time.Duration) error {
	if strings.TrimSpace(holder) == "" {
		return errors.New("lease holder must not be blank")
	}
	if strings.TrimSpace(token) == "" {
		return errors.New("lease token must not be blank")
	}
	if ttl < minLeaseTTL {
		return fmt.Errorf("lease ttl %s is below the minimum %s", ttl, minLeaseTTL)
	}
	if ttl > maxLeaseTTL {
		return fmt.Errorf("lease ttl %s exceeds the maximum %s", ttl, maxLeaseTTL)
	}
	return nil
}

// validateLeaseCAS additionally checks the fencing counter a renew/complete
// must present. Generation 0 is never a valid claim: claiming always bumps
// the persisted generation to >= 1.
func validateLeaseCAS(holder, token string, generation int, ttl time.Duration) error {
	if err := validateLeaseInstall(holder, token, ttl); err != nil {
		return err
	}
	if generation < 1 {
		return fmt.Errorf("lease generation %d is never valid: claims start at 1", generation)
	}
	return nil
}

// leasedRunColumns is the public sync_runs projection plus the lease columns
// needed to build a LeaseClaim, plus the P2.5 internal cp_* baseline columns
// (json:"-" on models.SyncRun — they never appear in any public projection;
// the executor uses Run.CPMode as its routing authority). It must stay in
// sync with scanLeasedRun and only ever targets rows that hold a live lease
// (never pending rows, whose lease columns are NULL).
const leasedRunColumns = `r.id, r.sync_def_id, r.status, r.org_id, r.started_at, r.finished_at,
	r.bytes_written, r.files_written, r.rows_written, r.error, r.ingest_job_id,
	r.dataset_version_id, r.content_hash, r.requested_by, r.owner_id, r.attempt, r.generation,
	r.control_mode, r.cancel_requested_at, r.cancelled_by, r.publish_entered_at,
	r.lease_holder, r.lease_token, r.lease_expires_at,
	COALESCE(r.cp_mode, ''), COALESCE(r.cp_base_epoch, 0), r.cp_base_value,
	COALESCE(r.cp_binding_fingerprint, ''), COALESCE(r.cp_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
	COALESCE(r.recovery_protocol, '')`

func scanLeasedRun(r rowLikeT) (*LeaseClaim, error) {
	v := &LeaseClaim{}
	var cpBaseValue *string
	if err := r.Scan(&v.Run.ID, &v.Run.SyncDefID, &v.Run.Status, &v.Run.OrgID, &v.Run.StartedAt,
		&v.Run.FinishedAt, &v.Run.BytesWritten, &v.Run.FilesWritten, &v.Run.RowsWritten,
		&v.Run.Error, &v.Run.IngestJobID, &v.Run.DatasetVersionID, &v.Run.ContentHash,
		&v.Run.RequestedBy, &v.Run.OwnerID, &v.Run.Attempt, &v.Run.Generation,
		&v.Run.ControlMode, &v.Run.CancelRequestedAt, &v.Run.CancelledBy, &v.Run.PublishEnteredAt,
		&v.Holder, &v.Token, &v.ExpiresAt,
		&v.Run.CPMode, &v.Run.CPBaseEpoch, &cpBaseValue,
		&v.Run.CPBindingFingerprint, &v.Run.CPOutputDatasetID,
		// Root review-w2-round1 item 2: the frozen protocol marker travels
		// with every leased projection so W3's mode routing can never mistake
		// a p26-v1 run for legacy.
		&v.Run.RecoveryProtocol); err != nil {
		return nil, err
	}
	if cpBaseValue != nil {
		v.Run.CPBaseValue = *cpBaseValue
	}
	return v, nil
}

// withRunLock opens a transaction, locks the run row by id + tenant, and runs
// fn with the lock held. fn's statements run on the same connection/tx, so
// their clock_timestamp() quals are evaluated after the lock was acquired.
func (r *Repo) withRunLock(ctx context.Context, runID uuid.UUID, t Tenant, fn func(tx pgx.Tx) error) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	predicate, arg := syncRunTenantPredicate(t, 2)
	var locked uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT r.id FROM sync_runs r WHERE r.id = $1 AND `+predicate+` FOR UPDATE`,
		runID, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return errRunNotVisible
	}
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// EnqueueSyncRun persists a *pending* durable task for the sync def. Each
// manual trigger inserts its own row — posts are never merged; the P2.3
// admission policy bounds how many may exist unfinished. org_id follows the
// def; owner_id snapshots the Source owner (not the triggerer) and
// requested_by records the verified JWT subject that triggered the run; the
// two are never mixed. P2.2: every new task is explicitly marked
// control_mode='controlled' — only controlled runs carry trustworthy stage
// columns and may be cancelled.
//
// P2.3 protocol (pinned): LOCKLESS pool pre-checks run BEFORE the transaction
// opens — the trusted-tenant def/source parent-chain check answers the clean
// legacy nil (no lock, no quota consulted) for an invisible or unbound def,
// and the policy pre-read ONLY selects the wait mode: an admission-enabled
// source locks FOR UPDATE NOWAIT so contention is a fast typed 429 instead of
// parked reservations; everything else takes plain FOR UPDATE. Under the held
// source lock the def binding is re-verified (FOR KEY SHARE on the def, the
// INSERT's own Source→Def order), and only then does admissionGate re-read
// the authoritative policy, count pending+running runs and sample the DB
// clock. A successful INSERT is followed — same transaction, last statement —
// by the sync_last_admitted_at advance re-anchored on the write-time DB
// clock. The barrier semantics against a guarded delete are unchanged for
// no-policy sources (the blocking FOR UPDATE path): a delete that wins
// leaves this a clean no-row result (never a foreign-key violation) and an
// enqueue that wins is observed by the delete's post-lock re-check. For an
// ADMISSION-ENABLED source the NOWAIT lock statement instead answers the
// typed contention 429 while ANY writer (including that delete) holds the
// row lock; a retry after the delete commits then sees the clean no-row
// result. Returns (nil, nil) when the source/def is not visible in the
// tenant.
func (r *Repo) EnqueueSyncRun(ctx context.Context, syncID, sourceID uuid.UUID, t Tenant, requestedBy uuid.UUID) (*models.SyncRun, error) {
	bound, err := defBoundToSourceVisible(ctx, r.Pool, syncID, sourceID, t)
	if err != nil {
		return nil, err
	}
	if !bound {
		return nil, nil
	}
	nowait := r.sourceAdmissionNowait(ctx, sourceID, t)
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackAdmissionTx(tx)
	err = lockSourceForAdmission(ctx, tx, sourceID, t, nowait)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		// 55P03 maps to the typed contention rejection ONLY for the NOWAIT
		// source-lock statement. The transaction is in a failed state; the
		// deferred bounded rollback closes it and nothing else runs on it.
		if nowait && isLockNotAvailable(err) {
			return nil, &AdmissionRejection{Reason: AdmissionReasonContention, RetryAfter: advisoryRetryAfter}
		}
		return nil, err
	}
	// Parent chain before quota, post-lock: a def that is no longer bound or
	// visible answers the clean nil before any admission state is consulted.
	// P2.5 (repo review item 1): the Source-lock-held LIGHTWEIGHT probe only
	// selects the Def lock mode; the FULL authoritative binding is computed
	// AFTER the Def strong lock, so Def target/fileglob writers (which take
	// only the Def lock) can never race the captured fingerprint. No
	// KEY SHARE->FOR UPDATE upgrade is ever attempted in-place.
	incremental, perr := sourceIncrementalEnabled(ctx, tx, sourceID, t)
	if perr != nil {
		return nil, perr
	}
	// P2.6 (v2.1): a recovery-enabled snapshot source needs the same Def
	// STRONG lock as incremental — the snapshot recovery binding is frozen
	// from the def/source rows under it (fingerprint race-freedom, same
	// reasoning as P25 repo review item 1). The probe never adjudicates and
	// keeps legacy admissions byte-identical.
	snapshotRecovery := false
	if !incremental {
		snapshotRecovery = sourceRecoveryEnabled(ctx, tx, sourceID, t)
	}
	if incremental || snapshotRecovery {
		bound, err = lockDefBoundToSourceStrong(ctx, tx, syncID, sourceID, t)
	} else {
		bound, err = lockDefBoundToSource(ctx, tx, syncID, sourceID, t)
	}
	if err != nil {
		return nil, err
	}
	if !bound {
		return nil, nil
	}
	if err := admissionGate(ctx, tx, sourceID); err != nil {
		return nil, err
	}
	var run *models.SyncRun
	if incremental {
		// Full authoritative binding UNDER the held Def strong lock (item 1):
		// enabled-but-invalid configs fail closed here.
		binding, fingerprint, ferr := readIncrementalBinding(ctx, tx, syncID, sourceID, t)
		if ferr != nil {
			return nil, ferr
		}
		// P2.5: lock-or-init the checkpoint row, verify owner/binding, INSERT
		// the pending run with the frozen baseline, THEN set the owner (FK
		// order — root review-round1 §2). Busy/stale are typed rejections
		// with zero durable side effects.
		var rejection *IncrementalRejection
		run, rejection, err = r.AcquireCheckpointForAdmission(ctx, tx, syncID, sourceID, t, requestedBy, binding, fingerprint)
		if err != nil {
			if errors.Is(err, errCheckpointLockContention) {
				return nil, &AdmissionRejection{Reason: AdmissionReasonContention, RetryAfter: advisoryRetryAfter}
			}
			return nil, err
		}
		if rejection != nil {
			return nil, rejection
		}
		if run == nil {
			return nil, nil
		}
	} else if snapshotRecovery {
		// P2.6 (v2.1): recovery-enabled snapshot admission — freeze the
		// snapshot recovery binding (computed under the held Source+Def
		// strong locks) and the v1 protocol marker on the pending run. No
		// checkpoint row, no epoch, no owner: snapshot semantics stay
		// last-commit-wins; the freeze only protects the recoverable
		// publish identity.
		binding, fingerprint, ferr := readSnapshotRecoveryBinding(ctx, tx, syncID, sourceID, t)
		if ferr != nil {
			return nil, ferr
		}
		run, err = insertPendingSyncRunWithRecovery(ctx, tx, uuid.New(), syncID, sourceID, t, requestedBy, binding, fingerprint)
		if err != nil {
			return nil, err
		}
		if run == nil {
			return nil, nil
		}
	} else {
		// P2.4: the durable enqueue INSERT is shared verbatim with the
		// scheduled claim path (insertPendingSyncRun) so manual and scheduled
		// runs can never drift apart in shape or tenant verification.
		run, err = insertPendingSyncRun(ctx, tx, uuid.New(), syncID, sourceID, t, requestedBy)
		if err != nil {
			return nil, err
		}
		if run == nil {
			return nil, nil
		}
	}
	if err := advanceSyncLastAdmitted(ctx, tx, sourceID); err != nil {
		return nil, err
	}
	// Commit before returning: the deferred Rollback alone would silently
	// undo the pending row. The commit is what makes the durable task
	// visible to every other connection (and to the guarded mutations).
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return run, nil
}

// (P2.5 admission binding helper removed — superseded by the two-phase
// sourceIncrementalEnabled probe + post-Def-lock readIncrementalBinding,
// repo review items 1+9.)

// ClaimSyncRun atomically transitions a pending run to running and installs a
// fresh lease (generation+1, new unguessable token). Multiple connections
// racing on the same pending run are serialized by the row lock; every loser
// re-checks status='pending' after acquiring the lock and affects 0 rows, so
// exactly one claimer wins. Returns (nil, nil) when the run is not visible,
// not pending, or was claimed first. Invalid holder/token/TTL are rejected
// before any lock or write happens.
//
// P2.5 routing: captured incremental work (run.cp_mode set, frozen at
// admission) is routed to the checkpoint-aware ordered claim BEFORE any
// Run-first lock is taken (implementation-release correction 2).
func (r *Repo) ClaimSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, ttl time.Duration) (*LeaseClaim, error) {
	if err := validateLeaseInstall(holder, token, ttl); err != nil {
		return nil, err
	}
	if pre, err := r.runInternalPreRead(ctx, runID, t); err != nil {
		return nil, err
	} else if isIncrementalRun(pre) {
		return r.claimIncrementalSyncRun(ctx, runID, t, holder, token, ttl, pre)
	}
	var out *LeaseClaim
	err := r.withRunLock(ctx, runID, t, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET status = 'running',
			        started_at = clock_timestamp(),
			        attempt = r.attempt + 1,
			        generation = r.generation + 1,
			        lease_holder = $2,
			        lease_token = $3,
			        lease_expires_at = clock_timestamp() + make_interval(secs => $4::float8),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'pending'
			  RETURNING `+leasedRunColumns,
			runID, holder, token, ttl.Seconds())
		claim, err := scanLeasedRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // lost the race / not pending: caller sees nil claim
		}
		if err != nil {
			return err
		}
		out = claim
		return nil
	})
	if errors.Is(err, errRunNotVisible) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RenewSyncRunLease extends the lease of a running run. The CAS matches run
// id + tenant + status='running' + holder + token + generation and requires
// the DB clock (evaluated after the row lock is held) to still be inside the
// lease; the new expiry starts from that same post-lock clock. An expired
// lease is rejected even when nobody has taken it over yet. Returns
// (nil, nil) on any mismatch, race loss, or expiry.
func (r *Repo) RenewSyncRunLease(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, ttl time.Duration) (*LeaseClaim, error) {
	if err := validateLeaseCAS(holder, token, generation, ttl); err != nil {
		return nil, err
	}
	var out *LeaseClaim
	err := r.withRunLock(ctx, runID, t, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET lease_expires_at = clock_timestamp() + make_interval(secs => $5::float8),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running'
			    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			    AND clock_timestamp() < r.lease_expires_at
			  RETURNING `+leasedRunColumns,
			runID, holder, token, generation, ttl.Seconds())
		claim, err := scanLeasedRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		out = claim
		return nil
	})
	if errors.Is(err, errRunNotVisible) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CompleteLeasedSyncRun writes the terminal state of a leased running run
// under the same lease CAS as RenewSyncRunLease (holder + token + generation
// + unexpired). Terminal states can therefore never be overwritten: once
// terminal, the status='running' guard fails every later CAS. status must be
// one of succeeded/failed/aborted.
//
// P2.2 stage machine for controlled runs (legacy rows keep the bare P2.1
// CAS): 'succeeded' requires the publish gate to have been entered AND no
// cancel request to have won first; 'aborted' requires the gate NOT entered.
// This fences the terminal write itself — a shutdown/cancel helper can no
// longer mark a gate-entered row aborted, and a run whose cancel won cannot
// be recorded as succeeded. 'failed' stays unconstrained: it is the honest
// uncertain outcome both before and after a publish attempt. Returns
// (nil, nil) when the CAS or the stage guard fails (lease lost / expired /
// already terminal / illegal stage transition); the caller must NOT fabricate
// a terminal result in that case.
//
// P2.5 routing (BEFORE any Run-first lock): captured incremental work never
// reaches this body — 'succeeded' is refused outright (only
// CompleteIncrementalSyncRun may write a P25 success, atomically with the
// checkpoint advance) and 'failed'/'aborted' go to FailIncrementalSyncRun,
// whose DB-asserted release boundary decides owner retention.
func (r *Repo) CompleteLeasedSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, status string, bytesWritten, filesWritten, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error) {
	switch status {
	case "succeeded", "failed", "aborted":
	default:
		return nil, errors.New("invalid terminal status: " + status)
	}
	if pre, err := r.runInternalPreRead(ctx, runID, t); err != nil {
		return nil, err
	} else if isIncrementalRun(pre) {
		if status == "succeeded" {
			// P2.6: a p26-v1 success may ONLY be written by the recovery
			// finalize (intent + epoch + owner release in one transaction).
			if pre.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
				return nil, ErrRecoveryRequiresFinalize
			}
			return nil, ErrIncrementalRequiresAdvance
		}
		// v1 failed/aborted keep the P25 routing; FailIncrementalSyncRun
		// itself refuses a post-gate v1 close (unknown stays running).
		return r.FailIncrementalSyncRun(ctx, runID, t, holder, token, generation, status, errMsg, contentHash)
	} else if pre != nil && pre.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		// Snapshot p26-v1 run: success must go through the recovery
		// finalize; failed/aborted are only provable BEFORE the gate.
		return r.CompleteRecoverableSnapshotRun(ctx, runID, t, holder, token, generation, status, bytesWritten, filesWritten, rowsWritten, errMsg, ingestJobID, datasetVersionID, contentHash)
	}
	// No TTL is involved in a terminal write; still reject blank fencing
	// material and generation 0 before any lock or write happens.
	if strings.TrimSpace(holder) == "" {
		return nil, errors.New("lease holder must not be blank")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("lease token must not be blank")
	}
	if generation < 1 {
		return nil, fmt.Errorf("lease generation %d is never valid: claims start at 1", generation)
	}
	var out *models.SyncRun
	err := r.withRunLock(ctx, runID, t, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET status = $5,
			        finished_at = clock_timestamp(),
			        bytes_written = $6,
			        files_written = $7,
			        rows_written = $8,
			        error = $9,
			        ingest_job_id = $10,
			        dataset_version_id = $11,
			        content_hash = $12,
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running'
			    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			    AND clock_timestamp() < r.lease_expires_at
			    AND (r.control_mode = 'legacy'
			      OR ($5 = 'succeeded' AND r.publish_entered_at IS NOT NULL AND r.cancel_requested_at IS NULL)
			      OR ($5 = 'aborted' AND r.publish_entered_at IS NULL)
			      OR ($5 = 'failed'))
			  RETURNING `+leasedRunColumns,
			runID, holder, token, generation, status, bytesWritten, filesWritten,
			rowsWritten, errMsg, ingestJobID, datasetVersionID, contentHash)
		claim, err := scanLeasedRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		out = &claim.Run
		return nil
	})
	if errors.Is(err, errRunNotVisible) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TakeoverExpiredSyncRun re-claims a running run whose lease has expired,
// bumping generation and rotating the token so every older (token,
// generation) pair — including one held again by the same holder — is
// permanently rejected afterwards (no ABA). Rows without a lease (legacy
// pre-P2.1 running rows, lease_expires_at NULL) are never taken over and are
// never re-published by this path. This is storage capability only: P2.1
// wires no worker or HTTP trigger to it.
//
// P2.5 anti-backdoor (implementation-release correction 2): a run that owns
// a checkpoint (sync_def_checkpoints.active_run_id) is NEVER takeover-
// eligible — unresolved P25 work must not be re-executed or re-published
// through this storage path; P26 owns resolution.
//
// P2.6 (v2.1): a p26-v1 run is NEVER generic-takeover eligible either — its
// expired lease is recovered exclusively through ClaimRecovery (fresh-auth
// fenced claim that distinguishes the pre-gate re-pull shape from the
// post-gate verify/replay shape). A bare takeover must never re-pull a
// source whose publish intent may exist.
func (r *Repo) TakeoverExpiredSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, ttl time.Duration) (*LeaseClaim, error) {
	if err := validateLeaseInstall(holder, token, ttl); err != nil {
		return nil, err
	}
	var out *LeaseClaim
	err := r.withRunLock(ctx, runID, t, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET attempt = r.attempt + 1,
			        generation = r.generation + 1,
			        lease_holder = $2,
			        lease_token = $3,
			        lease_expires_at = clock_timestamp() + make_interval(secs => $4::float8),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running'
			    AND r.lease_token IS NOT NULL
			    AND r.lease_expires_at IS NOT NULL
			    AND clock_timestamp() >= r.lease_expires_at
			    AND COALESCE(r.recovery_protocol, '') <> 'p26-v1'
			    AND NOT EXISTS (SELECT 1 FROM sync_def_checkpoints cp WHERE cp.active_run_id = r.id)
			  RETURNING `+leasedRunColumns,
			runID, holder, token, ttl.Seconds())
		claim, err := scanLeasedRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		out = claim
		return nil
	})
	if errors.Is(err, errRunNotVisible) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CheckSyncRunLease is a lock-free, read-only validity probe: does the row
// still hold exactly this lease (status running, holder, token, generation)
// and is the DB clock still inside lease_expires_at? It is used as a cheap
// per-page gate during pulls; the authoritative transitions remain the
// two-phase CAS methods. clock_timestamp() is evaluated at read time, so an
// expiry that happened since the last heartbeat is observed here. Returns
// false (not an error) on any mismatch.
func (r *Repo) CheckSyncRunLease(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int) (bool, error) {
	if err := validateLeaseCAS(holder, token, generation, minLeaseTTL); err != nil {
		return false, err
	}
	predicate, arg := syncRunTenantPredicate(t, 2)
	var ok bool
	err := r.Pool.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM sync_runs r
		     WHERE r.id = $1 AND `+predicate+`
		       AND r.status = 'running'
		       AND r.lease_holder = $3 AND r.lease_token = $4 AND r.generation = $5
		       AND clock_timestamp() < r.lease_expires_at)`,
		runID, arg, holder, token, generation).Scan(&ok)
	return ok, err
}

// HasActiveSyncRunsForSource reports whether any pending/running run exists
// for any sync def of the source. Handler-level pre-check; the authoritative
// race-closing checks are the two-phase guards inside UpdateConnection and
// DeleteConnection, which re-run this predicate while holding the source row
// lock.
func (r *Repo) HasActiveSyncRunsForSource(ctx context.Context, sourceID uuid.UUID, t Tenant) (bool, error) {
	predicate, arg := syncRunTenantPredicate(t, 2)
	var exists bool
	err := r.Pool.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM sync_runs r
		      JOIN batch_sync_defs d ON d.id = r.sync_def_id
		      JOIN connections c ON c.id = d.source_id
		     WHERE c.id = $1 AND `+predicate+`
		       AND r.status IN ('pending','running'))`,
		sourceID, arg).Scan(&exists)
	return exists, err
}

// HasActiveSyncRunsForDef is HasActiveSyncRunsForSource scoped to one sync
// def; the authoritative target-change guard is the two-phase re-check inside
// UpdateSyncJob. Terminal runs keep the pre-P2.1 semantics: mutations and
// deletes are allowed again.
func (r *Repo) HasActiveSyncRunsForDef(ctx context.Context, defID uuid.UUID, t Tenant) (bool, error) {
	predicate, arg := syncRunTenantPredicate(t, 2)
	var exists bool
	err := r.Pool.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM sync_runs r
		     WHERE r.sync_def_id = $1 AND `+predicate+`
		       AND r.status IN ('pending','running'))`,
		defID, arg).Scan(&exists)
	return exists, err
}
