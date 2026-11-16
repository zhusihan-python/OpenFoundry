// P2.3 per-source persistent sync admission, shared by BOTH run-creation
// paths (EnqueueSyncRun and the legacy RunSyncJob). The protocol is the
// proven two-phase shape of lease.go with a pinned lock-mode twist:
//
//  1. Lockless pre-checks on the pool BEFORE the transaction opens (a pool
//     query while the transaction holds the only connection would
//     self-deadlock single-connection pools): the trusted tenant + def/source
//     parent-chain pre-check answers the clean legacy nil for an invisible or
//     unbound def BEFORE any lock or quota state, and the policy pre-read
//     ONLY selects the wait mode of the lock statement — admission-enabled
//     source → FOR UPDATE NOWAIT (a hot source can never park local
//     reservations waiting on the row lock), anything else → plain FOR
//     UPDATE (the old blocking semantics, including the delete-race
//     barrier). Neither pre-check ever adjudicates.
//  2. After the source row lock is held, the def binding is re-verified with
//     a FOR KEY SHARE on the def row (Source→Def lock order — exactly the
//     order the INSERT's FK takes, preserving the DefLockVsRetarget
//     serialization; UpdateSyncJob takes the def FOR UPDATE and never locks
//     the source, so no reverse source lock can form). Only then does
//     admissionGate re-read the policy from the locked row (authoritative —
//     the pre-reads and any handler-side read are never quota evidence),
//     count pending+running runs of the source in SQL and sample
//     clock_timestamp() in the SAME statement, so cap, interval and clock
//     are one consistent post-lock decision.
//  3. On success the caller INSERTs the run and then calls
//     advanceSyncLastAdmitted inside the SAME transaction as the LAST
//     statement — also when the policy is currently off, so a later
//     min_interval_ms enablement interprets real history. The advance
//     re-anchors on clock_timestamp() AT WRITE TIME: the gate's sampled
//     clock only adjudicates THIS interval decision and must not be
//     replanted as last_admitted_at if the INSERT (or its FK wait) crossed
//     the interval boundary.
//
// Failure discipline: a 55P03 (lock_not_available) is translated to the typed
// contention rejection ONLY for the NOWAIT lock statement itself; every other
// error (including 57014 cancellations and 40P01 deadlocks) keeps the honest
// error path. After any statement error the transaction is in a failed state
// — rollbackAdmissionTx closes it with its OWN bounded context; nothing is
// retried on it.
//
// Lock order is strictly source-row → def KEY SHARE → run INSERT; no terminal
// transition (claim/renew/complete/cancel/gate) writes the connections row,
// so no run→source reverse lock can form.
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// admissionRollbackBudget bounds the deferred cleanup rollback of the two
// admission transactions independently of the caller's (possibly already
// cancelled) context, so a 55P03 typed return releases the source lock and
// the pool connection promptly.
const admissionRollbackBudget = 5 * time.Second

// admissionQuerier is the one-method surface both the pool (pre-transaction
// pre-checks) and an open transaction (in-tx reads) offer. Using the tx as
// the querier keeps single-connection pools self-deadlock-free.
type admissionQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// rollbackAdmissionTx is the bounded deferred cleanup for the admission
// entry points (the generic withRunLock/lease helpers are untouched). After a
// successful commit the rollback is a no-op error that is deliberately
// ignored, exactly like the previous defer shape.
func rollbackAdmissionTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), admissionRollbackBudget)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// AdmissionRejectionReason enumerates the machine-readable refusal reasons of
// the P2.3 429 response (code=source_sync_throttled).
type AdmissionRejectionReason string

const (
	// AdmissionReasonCapacity: pending+running runs of the source already sit
	// at the configured cap. Retry timing is unknowable at enqueue (it depends
	// on real run completion), so the hint is advisory only.
	AdmissionReasonCapacity AdmissionRejectionReason = "capacity"
	// AdmissionReasonMinInterval: the minimum interval since the last
	// successful admission has not elapsed under the post-lock DB clock.
	AdmissionReasonMinInterval AdmissionRejectionReason = "min_interval"
	// AdmissionReasonContention: the NOWAIT lock statement hit SQLSTATE
	// 55P03 — the source row lock is held by SOME writer (a config update,
	// a guarded delete or another admission); which one is not knowable.
	AdmissionReasonContention AdmissionRejectionReason = "contention"
)

// advisoryRetryAfter is the pinned 1-second hint for the reasons whose real
// release time cannot be computed at admission time. It is a suggestion, not
// a promise that capacity or the lock is free by then.
const advisoryRetryAfter = time.Second

// AdmissionRejection is the typed P2.3 refusal. RetryAfter is always a whole
// number of seconds >= 1: the min_interval case is the ceiling of the
// post-lock DB remaining time, the others are the advisory 1s.
type AdmissionRejection struct {
	Reason     AdmissionRejectionReason
	RetryAfter time.Duration
}

func (e *AdmissionRejection) Error() string {
	switch e.Reason {
	case AdmissionReasonCapacity:
		return "source sync admission refused: the source already holds its maximum number of pending or running sync runs"
	case AdmissionReasonMinInterval:
		return "source sync admission refused: the minimum interval since the last admitted run has not elapsed"
	case AdmissionReasonContention:
		return "source sync admission refused: the source is busy with another writer holding its lock; retry shortly"
	}
	return "source sync admission refused"
}

// isLockNotAvailable matches ONLY SQLSTATE 55P03 (could not obtain lock
// WITHOUT waiting), verified against the PG16 errcodes appendix. It must not
// be widened to cancellations (57014), deadlocks (40P01) or anything else.
func isLockNotAvailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// admissionLockClause returns the FOR UPDATE spelling for the source lock
// statement: NOWAIT when the pre-read observed an admission-enabled source.
func admissionLockClause(nowait bool) string {
	if nowait {
		return ` FOR UPDATE NOWAIT`
	}
	return ` FOR UPDATE`
}

// lockSourceForAdmission takes the source row lock inside the caller's open
// transaction and reports visibility like every tenant-scoped writer: no row
// → pgx.ErrNoRows (the caller answers its clean nil/no-FK contract). A 55P03
// on the NOWAIT variant is returned verbatim for the caller to map to the
// typed contention rejection; all other errors propagate unchanged.
func lockSourceForAdmission(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID, t Tenant, nowait bool) error {
	predicate, arg := connectionTenantPredicate(t, 2)
	var locked uuid.UUID
	err := tx.QueryRow(ctx,
		`SELECT c.id FROM connections c WHERE c.id = $1 AND `+predicate+admissionLockClause(nowait),
		sourceID, arg).Scan(&locked)
	return err
}

// defBoundToSourceVisible is the LOCKLESS parent-chain pre-check: the def
// must be visible in the trusted tenant AND bound to exactly this source.
// It runs on the pool BEFORE the transaction opens (never while the tx holds
// the pool's only connection). False answers the clean legacy nil with no
// lock taken, no quota consulted and no state touched. It never adjudicates;
// the authoritative re-check is lockDefBoundToSource under the source lock.
func defBoundToSourceVisible(ctx context.Context, q admissionQuerier, syncID, sourceID uuid.UUID, t Tenant) (bool, error) {
	predicate, arg := syncDefTenantPredicate(t, 3)
	var id uuid.UUID
	err := q.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND d.source_id = $2 AND `+predicate,
		syncID, sourceID, arg).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// lockDefBoundToSource re-verifies the def binding AFTER the source row lock
// is held, taking FOR KEY SHARE on the def row — the same lock the INSERT's
// FK acquires, in the same Source→Def order, so the DefLockVsRetarget
// serialization is preserved and no reverse (def→source) lock order is
// introduced (UpdateSyncJob locks the def FOR UPDATE and never the source).
// False → the def vanished or was never bound: the caller answers the clean
// nil BEFORE any quota decision.
func lockDefBoundToSource(ctx context.Context, tx pgx.Tx, syncID, sourceID uuid.UUID, t Tenant) (bool, error) {
	predicate, arg := syncDefTenantPredicate(t, 3)
	var id uuid.UUID
	err := tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND d.source_id = $2 AND `+predicate+` FOR KEY SHARE`,
		syncID, sourceID, arg).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// lockDefBoundToSourceStrong is the P2.5 opt-in variant: the checkpoint
// admission path needs the FOR UPDATE strong lock — KEY SHARE is insufficient
// to serialize same-def checkpoint initialization/binding writers (root
// review-round1 §2). The legacy no-opt-in path keeps lockDefBoundToSource.
func lockDefBoundToSourceStrong(ctx context.Context, tx pgx.Tx, syncID, sourceID uuid.UUID, t Tenant) (bool, error) {
	predicate, arg := syncDefTenantPredicate(t, 3)
	var id uuid.UUID
	err := tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND d.source_id = $2 AND `+predicate+` FOR UPDATE`,
		syncID, sourceID, arg).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// sourceAdmissionNowait is the lockless mode-selection pre-read on the pool
// (pre-transaction: the caller must not already hold the pool's only
// connection). Any error, an invisible row or an unreadable policy maps to
// the BLOCKING path — the conservative direction: the worst case is waiting
// on the lock, never skipping the authoritative gate or an admission.
func (r *Repo) sourceAdmissionNowait(ctx context.Context, sourceID uuid.UUID, t Tenant) bool {
	return admissionNowait(ctx, r.Pool, sourceID, t)
}

// admissionNowait is the tx-usable core of the mode pre-read (the legacy
// path resolves its source inside the transaction and must not ask the pool
// for a second connection).
func admissionNowait(ctx context.Context, q admissionQuerier, sourceID uuid.UUID, t Tenant) bool {
	predicate, arg := connectionTenantPredicate(t, 2)
	var config []byte
	err := q.QueryRow(ctx,
		`SELECT c.config FROM connections c WHERE c.id = $1 AND `+predicate, sourceID, arg).Scan(&config)
	if err != nil {
		return false
	}
	policy, err := models.ParseSyncAdmissionPolicy(config)
	if err != nil || policy == nil {
		return false
	}
	return policy.Enabled()
}

// admissionGate is the post-lock, same-transaction admission decision. The
// caller must already hold the source row lock AND have re-verified the def
// binding in tx. The gate re-reads the policy from the locked row
// (authoritative), counts pending+running runs of the source in SQL and
// samples clock_timestamp() in the SAME statement — the sampled clock only
// adjudicates THIS decision (and the Retry-After hint); the persisted
// sync_last_admitted_at is re-anchored at write time by
// advanceSyncLastAdmitted. A typed *AdmissionRejection means nothing was
// written and the caller's bounded rollback restores the pre-call state byte
// for byte.
//
// An unreadable policy fails CLOSED: every config write surface validates the
// subtree, so this is only reachable through out-of-band row corruption and
// must surface as an honest error, never as a silent admission.
func admissionGate(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID) error {
	var config []byte
	var lastAdmitted *time.Time
	if err := tx.QueryRow(ctx,
		`SELECT c.config, c.sync_last_admitted_at FROM connections c WHERE c.id = $1`,
		sourceID).Scan(&config, &lastAdmitted); err != nil {
		return err
	}
	policy, err := models.ParseSyncAdmissionPolicy(config)
	if err != nil {
		return fmt.Errorf("source sync admission policy is unreadable: %w", err)
	}
	if policy == nil || !policy.Enabled() {
		return nil
	}
	// One statement, one snapshot: the active-run count and the DB clock the
	// interval is judged against are sampled together, post-lock.
	var active int
	var now time.Time
	if err := tx.QueryRow(ctx,
		`SELECT (SELECT COUNT(*) FROM sync_runs r
		           JOIN batch_sync_defs d ON d.id = r.sync_def_id
		          WHERE d.source_id = $1 AND r.status IN ('pending','running')),
		       clock_timestamp()`,
		sourceID).Scan(&active, &now); err != nil {
		return err
	}
	if policy.CapEnabled() && active >= *policy.MaxInflightRuns {
		return &AdmissionRejection{Reason: AdmissionReasonCapacity, RetryAfter: advisoryRetryAfter}
	}
	if policy.IntervalEnabled() && lastAdmitted != nil {
		eligibleAt := lastAdmitted.Add(time.Duration(*policy.MinIntervalMS) * time.Millisecond)
		if remaining := eligibleAt.Sub(now); remaining > 0 {
			seconds := (remaining + time.Second - 1) / time.Second // ceil to whole seconds
			if seconds < 1 {
				seconds = 1
			}
			return &AdmissionRejection{Reason: AdmissionReasonMinInterval, RetryAfter: seconds * time.Second}
		}
	}
	return nil
}

// advanceSyncLastAdmitted records a successful admission. Called AFTER the
// run INSERT succeeded, inside the same transaction, as the LAST statement
// before commit — a rejected, failed or rolled-back admission never reaches
// it. The timestamp is clock_timestamp() AT WRITE TIME (the gate's decision
// clock is deliberately NOT replanted: the INSERT or its FK wait may cross
// the interval boundary). It writes ONLY sync_last_admitted_at: the
// user-facing config, updated_at and every other connections column stay
// untouched.
func advanceSyncLastAdmitted(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID) error {
	_, err := tx.Exec(ctx,
		`UPDATE connections SET sync_last_admitted_at = clock_timestamp() WHERE id = $1`,
		sourceID)
	return err
}
