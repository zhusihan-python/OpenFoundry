// P2.2 interruptible runs: durable cancel requests and the publish-entry
// gate. Both racing operations — RequestCancelSyncRun and BeginPublishGate —
// take the same run row lock first (the two-phase protocol proven in
// lease.go), so their ordering is serialized and the post-lock decision is
// final: either the cancel request is durably recorded before the gate runs
// (the gate then refuses and the run can still abort with zero publishes) or
// the gate enters first (later cancels are observably too-late and the run
// finishes under its real outcome). Neither outcome is ever fabricated: a
// persisted cancel request alone never writes 'aborted' — only the holder of
// a still-valid lease confirms an abort, and a pending row transitions inside
// the same lock that claim uses.
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// CancelOutcome is the machine-readable result of RequestCancelSyncRun.
type CancelOutcome string

const (
	// CancelOutcomeNotVisible: no such run under this parent sync id + tenant.
	CancelOutcomeNotVisible CancelOutcome = "not_visible"
	// CancelOutcomeUnsupported: pre-P2.2 (legacy) row — its NULL stage columns
	// prove nothing about remote publishes, so cancellation is refused.
	CancelOutcomeUnsupported CancelOutcome = "unsupported"
	// CancelOutcomeAborted: the run was pending and is now terminally aborted.
	CancelOutcomeAborted CancelOutcome = "aborted"
	// CancelOutcomeRequested: the run is running; the cancel request is now
	// durably persisted and the still-valid holder will confirm the abort.
	CancelOutcomeRequested CancelOutcome = "requested"
	// CancelOutcomeTooLate: the publish gate was entered first; the run will
	// complete under its real outcome and must not be reported as aborted.
	CancelOutcomeTooLate CancelOutcome = "too_late"
	// CancelOutcomeAlreadyTerminal: idempotent no-op on a terminal run.
	CancelOutcomeAlreadyTerminal CancelOutcome = "already_terminal"
)

// CancelResult pairs the outcome with the run row as it stands after the
// call. Run is nil only for CancelOutcomeNotVisible.
type CancelResult struct {
	Outcome CancelOutcome
	Run     *models.SyncRun
}

// PublishGateOutcome is the machine-readable result of BeginPublishGate.
type PublishGateOutcome string

const (
	// PublishGateEntered: the holder's lease was renewed for a full TTL and
	// the stage marker is set; publishing may proceed.
	PublishGateEntered PublishGateOutcome = "entered"
	// PublishGateCancelled: a cancel request won the race; the caller must
	// NOT publish and should confirm the abort while its lease still holds.
	PublishGateCancelled PublishGateOutcome = "cancel_requested"
	// PublishGateFenced: lease material mismatch, expiry, wrong state or a
	// non-controlled run; no stage marker was written.
	PublishGateFenced PublishGateOutcome = "fenced"
)

// PublishGateResult reports the gate decision. Claim is set only for
// PublishGateEntered; Run carries the current row for the cancelled case so
// callers can observe the persisted request without a second query.
type PublishGateResult struct {
	Outcome PublishGateOutcome
	Claim   *LeaseClaim
	Run     *models.SyncRun
}

// lockedRunByID reads the public projection of a run inside a transaction
// that already holds the row lock. The caller has verified visibility.
func lockedRunByID(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (*models.SyncRun, error) {
	row := tx.QueryRow(ctx, `SELECT `+runPublicColumns+` FROM sync_runs r WHERE r.id = $1`, runID)
	return scanSyncRun(row)
}

// RequestCancelSyncRun records a durable, queryable cancel request for a run,
// or directly aborts it while it is still pending. cancelledBy is the verified
// JWT subject of the canceller (nil for system-initiated closures such as
// shutdown or admission-deadline expiry); cause is the sanitized reason
// recorded on a pending→aborted transition. The call is idempotent: a repeat
// keeps the FIRST cancel_requested_at/cancelled_by and never rewrites an
// existing terminal state. Expired-lease running runs are NOT force-aborted:
// the request is persisted and stays queryable until a holder confirms it —
// no false "already aborted" is ever claimed.
//
// P2.5 routing: captured incremental work goes through the ordered
// Source→Def→Checkpoint→Run path (pending-abort releases the checkpoint owner
// in the same transaction) BEFORE any Run-first lock is taken — every
// internal cancel site (coordinator closeNow/TTL/Stop/missing-auth,
// dispatch-failure cleanup, HTTP cancel) shares this routing.
func (r *Repo) RequestCancelSyncRun(ctx context.Context, runID, syncID uuid.UUID, t Tenant, cancelledBy *uuid.UUID, cause string) (*CancelResult, error) {
	if pre, err := r.runInternalPreRead(ctx, runID, t); err != nil {
		return nil, err
	} else if isIncrementalRun(pre) {
		return r.RequestCancelIncrementalSyncRun(ctx, runID, syncID, pre.SourceID, t, cancelledBy, cause)
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	predicate, arg := syncRunTenantPredicate(t, 3)
	var (
		status, controlMode                 string
		cancelRequestedAt, publishEnteredAt *time.Time
	)
	err = tx.QueryRow(ctx,
		`SELECT r.status, r.control_mode, r.cancel_requested_at, r.publish_entered_at
		   FROM sync_runs r
		  WHERE r.id = $1 AND r.sync_def_id = $2 AND `+predicate+`
		  FOR UPDATE`,
		runID, syncID, arg).Scan(&status, &controlMode, &cancelRequestedAt, &publishEnteredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return &CancelResult{Outcome: CancelOutcomeNotVisible}, nil
	}
	if err != nil {
		return nil, err
	}
	if controlMode != models.RunControlControlled {
		// Legacy row: its stage columns cannot speak to remote publishes.
		run, err := lockedRunByID(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: CancelOutcomeUnsupported, Run: run}, nil
	}
	switch status {
	case "succeeded", "failed", "aborted":
		run, err := lockedRunByID(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: CancelOutcomeAlreadyTerminal, Run: run}, nil
	}
	if publishEnteredAt != nil {
		run, err := lockedRunByID(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: CancelOutcomeTooLate, Run: run}, nil
	}
	if status == "pending" {
		// Nothing has been claimed, hence nothing published: aborting here is
		// provable. The status='pending' guard in the UPDATE keeps the CAS
		// discipline against a claim that raced to the lock first.
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET status = 'aborted',
			        finished_at = clock_timestamp(),
			        error = $2,
			        cancel_requested_at = COALESCE(r.cancel_requested_at, clock_timestamp()),
			        cancelled_by = COALESCE(r.cancelled_by, $3),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'pending' AND r.control_mode = 'controlled'
			  RETURNING `+runPublicColumns,
			runID, cause, cancelledBy)
		run, err := scanSyncRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			// A claim won the lock race and flipped the row to running; read
			// the winner's state and report it honestly.
			run, rerr := lockedRunByID(ctx, tx, runID)
			if rerr != nil {
				return nil, rerr
			}
			if run.Status == "running" && run.CancelRequestedAt == nil {
				// Re-run the flag write for the now-running row: the caller
				// asked for cancellation and must not lose it to the race.
				row2 := tx.QueryRow(ctx,
					`UPDATE sync_runs r
					    SET cancel_requested_at = clock_timestamp(), cancelled_by = $2
					  WHERE r.id = $1 AND r.status = 'running' AND r.control_mode = 'controlled'
					    AND r.publish_entered_at IS NULL AND r.cancel_requested_at IS NULL
					  RETURNING `+runPublicColumns,
					runID, cancelledBy)
				run2, rerr := scanSyncRun(row2)
				if rerr != nil && !errors.Is(rerr, pgx.ErrNoRows) {
					return nil, rerr
				}
				if run2 != nil {
					run = run2
				}
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return &CancelResult{Outcome: CancelOutcomeRequested, Run: run}, nil
		}
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: CancelOutcomeAborted, Run: run}, nil
	}
	// status == 'running': persist the FIRST request only. A repeat (by a
	// user after a system nil-actor closure request, or any duplicate) must
	// return the row byte-identical with the original requester preserved —
	// COALESCE-style overwrites are forbidden.
	if cancelRequestedAt != nil {
		run, err := lockedRunByID(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: CancelOutcomeRequested, Run: run}, nil
	}
	row := tx.QueryRow(ctx,
		`UPDATE sync_runs r
		    SET cancel_requested_at = clock_timestamp(),
		        cancelled_by = $2
		  WHERE r.id = $1 AND r.status = 'running' AND r.control_mode = 'controlled'
		    AND r.publish_entered_at IS NULL
		    AND r.cancel_requested_at IS NULL
		  RETURNING `+runPublicColumns,
		runID, cancelledBy)
	run, err := scanSyncRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		// Only reachable through a state change we did not observe at the
		// locked SELECT (defensive): report the row as it stands.
		run, rerr := lockedRunByID(ctx, tx, runID)
		if rerr != nil {
			return nil, rerr
		}
		outcome := CancelOutcomeRequested
		if run.PublishEnteredAt != nil {
			outcome = CancelOutcomeTooLate
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: outcome, Run: run}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &CancelResult{Outcome: CancelOutcomeRequested, Run: run}, nil
}

// BeginPublishGate is the atomic publish-entry decision for a controlled run:
// under the run row lock it sets the stage marker and renews the lease for a
// full TTL, but only when the exact lease material still matches, the lease
// is unexpired under the post-lock clock, the run is controlled, no cancel
// request is persisted and the gate has not been entered before. A cancel
// request that committed first makes the gate refuse (PublishGateCancelled);
// a gate that entered first makes later cancels too-late. The gate must win
// before any CommitOutput call, so a cancelled gate proves zero publishes.
//
// P2.5 routing: captured incremental work goes through the checkpoint-aware
// ordered gate (binding/owner/base-epoch verified under Def→Checkpoint→Run
// locks) BEFORE any Run-first lock is taken. The gate ENTRY is P25's
// conservative unknown boundary.
func (r *Repo) BeginPublishGate(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, ttl time.Duration) (*PublishGateResult, error) {
	if pre, err := r.runInternalPreRead(ctx, runID, t); err != nil {
		return nil, err
	} else if pre != nil && pre.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		// P2.6 (review-w2-round1 item 3): a p26-v1 run may NEVER be gated
		// through the old entry — that would stamp publish_entered_at with
		// no publish intent, breaking the IFF invariant. Zero writes.
		return nil, ErrRecoveryRequiresRecoverableGate
	} else if isIncrementalRun(pre) {
		inc, err := r.BeginIncrementalPublishGate(ctx, runID, t, holder, token, generation, ttl)
		if err != nil || inc == nil {
			return nil, err
		}
		out := &PublishGateResult{Outcome: PublishGateOutcome(inc.Outcome)}
		switch inc.Outcome {
		case IncrementalPublishGateEntered:
			out.Outcome = PublishGateEntered
			out.Claim = inc.Claim
		case IncrementalPublishGateCancelled:
			out.Outcome = PublishGateCancelled
			out.Run = inc.Run
		case IncrementalPublishGateBindingStale, IncrementalPublishGateOwnerLost, IncrementalPublishGateFenced:
			out.Outcome = PublishGateFenced
		}
		return out, nil
	}
	if err := validateLeaseCAS(holder, token, generation, ttl); err != nil {
		return nil, err
	}
	var out *PublishGateResult
	err := r.withRunLock(ctx, runID, t, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET publish_entered_at = clock_timestamp(),
			        lease_expires_at = clock_timestamp() + make_interval(secs => $5::float8),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running' AND r.control_mode = 'controlled'
			    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			    AND clock_timestamp() < r.lease_expires_at
			    AND r.cancel_requested_at IS NULL
			    AND r.publish_entered_at IS NULL
			  RETURNING `+leasedRunColumns,
			runID, holder, token, generation, ttl.Seconds())
		claim, err := scanLeasedRun(row)
		if err == nil {
			out = &PublishGateResult{Outcome: PublishGateEntered, Claim: claim}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// The row lock is held, so this diagnosis is stable. A persisted
		// cancel request only counts as Cancelled when the CALLER's lease
		// material is still the live, unexpired, controlled, gate-not-entered
		// holder — otherwise the refusal is a fencing failure (expiry, old
		// generation, wrong token, terminal state) and must never be masked
		// as a cancellation.
		var (
			cancelRequestedAt *time.Time
			materialOK        bool
		)
		err = tx.QueryRow(ctx,
			`SELECT r.cancel_requested_at,
			       COALESCE(r.status = 'running' AND r.control_mode = 'controlled'
			         AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			         AND clock_timestamp() < r.lease_expires_at
			         AND r.publish_entered_at IS NULL, false) AS material_ok
			  FROM sync_runs r WHERE r.id = $1`,
			runID, holder, token, generation).Scan(&cancelRequestedAt, &materialOK)
		if errors.Is(err, pgx.ErrNoRows) {
			out = &PublishGateResult{Outcome: PublishGateFenced}
			return nil
		}
		if err != nil {
			return err
		}
		if !materialOK || cancelRequestedAt == nil {
			out = &PublishGateResult{Outcome: PublishGateFenced}
			return nil
		}
		run, rerr := lockedRunByID(ctx, tx, runID)
		if rerr != nil {
			return rerr
		}
		out = &PublishGateResult{Outcome: PublishGateCancelled, Run: run}
		return nil
	})
	if errors.Is(err, errRunNotVisible) {
		return &PublishGateResult{Outcome: PublishGateFenced}, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CheckSyncRunControl is a lock-free probe used by the executor's cancel
// poller and page gate: it reports both whether this exact lease is still the
// valid holder and whether a cancel request has been persisted, in one read.
// clock_timestamp() is evaluated at read time, so expiry and takeover between
// polls are observed here. Missing rows report (false, false, nil).
func (r *Repo) CheckSyncRunControl(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int) (bool, bool, error) {
	if err := validateLeaseCAS(holder, token, generation, minLeaseTTL); err != nil {
		return false, false, err
	}
	predicate, arg := syncRunTenantPredicate(t, 2)
	var leaseOK, cancelRequested bool
	// COALESCE: legacy rows have NULL lease columns, and NULL AND … is NULL,
	// which would make Scan(bool) fail — legacy/empty leases read as a plain
	// false, never an error.
	err := r.Pool.QueryRow(ctx,
		`SELECT COALESCE(r.status = 'running' AND r.lease_holder = $3 AND r.lease_token = $4
		         AND r.generation = $5 AND clock_timestamp() < r.lease_expires_at, false) AS lease_ok,
		       (r.cancel_requested_at IS NOT NULL) AS cancel_requested
		  FROM sync_runs r
		 WHERE r.id = $1 AND `+predicate,
		runID, arg, holder, token, generation).Scan(&leaseOK, &cancelRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return leaseOK, cancelRequested, nil
}

// GetSyncRunForTenant loads a single run with its double binding verified:
// the run must belong to the parent sync def AND to the caller's trusted
// tenant domain. Any mismatch is an invisible row (nil, nil).
func (r *Repo) GetSyncRunForTenant(ctx context.Context, syncID, runID uuid.UUID, t Tenant) (*models.SyncRun, error) {
	predicate, arg := syncRunTenantPredicate(t, 3)
	row := r.Pool.QueryRow(ctx,
		`SELECT `+runPublicColumns+`
		 FROM sync_runs r
		 WHERE r.id = $1 AND r.sync_def_id = $2 AND `+predicate,
		runID, syncID, arg)
	run, err := scanSyncRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return run, nil
}
