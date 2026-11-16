// P2.6 automatic recovery seam (root-authorized auto-recovery-seam.md).
//
// Two bounded repo surfaces for the default-off automatic recovery worker
// (W3 owns the loop/config/cmd wiring; this package owns the storage):
//
//   - ListRecoveryCandidates: a LOCKLESS, bounded, deterministic
//     (ReadyAt, RunID) keyset scan of claimable p26-v1 runs. The scan hint
//     only narrows the page; every authoritative decision happens under the
//     claim's locks with the DB clock. No body/key/candidate/token/org or
//     credential data is ever returned.
//   - ClaimAutomaticRecovery: the ONLY automatic adoption path. It reuses
//     the P24 grant adjudication predicates (readScheduleGrantRowTx /
//     matchesArm / grantDomainMatches / boundToLockedDef, plus the
//     ClaimDueScheduleSlot post-lock expiry + 90s-liveness rules) INSIDE the
//     same Source→Def→(CP, incremental only)→Run transaction as the shared
//     recoveryClaimDecision core — never "check the grant, then call the
//     plain ClaimRecovery" (that TOCTOU is exactly what this seam removes).
//     It never creates a run/tick/cursor/admission write; a missing/stale/
//     revoked/expired/not-due arm answers a typed zero-write outcome.
//
// The manual ClaimRecovery semantics and the review-w2-round1 fixes are
// unchanged; the automatic entry additionally requires the run's original
// requester to BE the arm's actor (automatic adoption never crosses
// identities) and the frozen parents/target to match the arm exactly.
package repo

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// Automatic-claim typed refusals (safe names only — no grant material ever
// leaks into outcomes, errors or logs).
const (
	// RecoveryClaimAuthRequired: no live grant matching the local arm (never
	// armed, other boot/revision/actor/domain, revoked, changed, expired
	// locally or in the DB, stale owner heartbeat) or the run's original
	// requester is not the arm's actor. Zero writes; the worker skips.
	RecoveryClaimAuthRequired RecoveryClaimOutcome = "auth_required"
	// RecoveryClaimNotDue: the run's intent is not yet actionable for the
	// AUTOMATED path (next_action_at in the future under the post-lock DB
	// clock, or the automated query budget is exhausted). Zero writes.
	RecoveryClaimNotDue RecoveryClaimOutcome = "not_due"
)

// RecoveryScanKey is the deterministic keyset cursor of the candidate scan.
type RecoveryScanKey struct {
	ReadyAt time.Time
	RunID   uuid.UUID
}

// RecoveryCandidate names one claimable run — ids and the ready time only.
type RecoveryCandidate struct {
	RunID, SyncDefID, SourceID uuid.UUID
	ReadyAt                    time.Time
}

// recoveryScanLimit bounds one scan page (same family as the schedule scan).
const recoveryScanLimit = 100

// ListRecoveryCandidates scans claimable p26-v1 runs locklessly with a
// deterministic (ReadyAt, RunID) keyset:
//
//   - pending v1 runs (dead admission tickets; ReadyAt = started_at), and
//     running v1 runs whose lease is provably expired under the scan read
//     (ReadyAt = max(intent.next_action_at, lease_expires_at));
//   - terminal rows and finalized intents are excluded; a send-exhausted
//     intent stays eligible while automated query budget remains;
//   - intents not yet actionable (next_action_at in the future) or with the
//     automated query cap consumed are excluded;
//   - NO cron/schedule_enabled filter — automatic recovery enablement and
//     grant truth are independent of the new-admission cron.
//
// `hint` only narrows the page (ReadyAt <= hint); `after` continues the
// keyset. Every decision is re-adjudicated under the claim's locks.
func (r *Repo) ListRecoveryCandidates(ctx context.Context, hint time.Time, after *RecoveryScanKey, limit int) ([]RecoveryCandidate, error) {
	if limit <= 0 || limit > recoveryScanLimit {
		limit = recoveryScanLimit
	}
	sql := `
		SELECT r.id, r.sync_def_id, d.source_id,
		       CASE WHEN r.status = 'pending' THEN r.started_at
		            ELSE GREATEST(COALESCE(i.next_action_at, r.lease_expires_at), r.lease_expires_at) END AS ready_at
		  FROM sync_runs r
		  JOIN batch_sync_defs d ON d.id = r.sync_def_id
		  LEFT JOIN sync_run_publish_intents i ON i.run_id = r.id
		 WHERE r.recovery_protocol = 'p26-v1'
		   AND (r.status = 'pending'
		        OR (r.status = 'running'
		            AND r.lease_expires_at IS NOT NULL
		            AND clock_timestamp() >= r.lease_expires_at))
		   AND (i.run_id IS NULL OR i.recovery_state <> 'finalized')
		   AND (i.run_id IS NULL OR i.next_action_at IS NULL OR i.next_action_at <= clock_timestamp())
		   AND (i.run_id IS NULL OR i.query_attempts < $2)
		   AND CASE WHEN r.status = 'pending' THEN r.started_at
		            ELSE GREATEST(COALESCE(i.next_action_at, r.lease_expires_at), r.lease_expires_at) END <= $1`
	args := []any{hint, MaxRecoveryQueryAttempts}
	if after != nil {
		sql += ` AND (CASE WHEN r.status = 'pending' THEN r.started_at
		                   ELSE GREATEST(COALESCE(i.next_action_at, r.lease_expires_at), r.lease_expires_at) END,
		             r.id) > ($3::timestamptz, $4::uuid)`
		args = append(args, after.ReadyAt, after.RunID)
	}
	sql += ` ORDER BY ready_at, r.id LIMIT ` + strconv.Itoa(limit)
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RecoveryCandidate{}
	for rows.Next() {
		var c RecoveryCandidate
		if err := rows.Scan(&c.RunID, &c.SyncDefID, &c.SourceID, &c.ReadyAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// (limit literal via strconv — same family as the schedule scan)

// ClaimAutomaticRecovery is the automatic worker's ONLY adoption path. The
// local arm (process-memory grant material, never persisted here) is
// adjudicated against the CURRENT DB grant INSIDE the same ordered chain
// transaction as the shared recovery claim core — the P24 predicates run
// under the held Def lock with the post-lock DB clock:
//
//   - exact grant identity: grant row live (not expired, owner heartbeat
//     inside the 90s freshness window), boundToLockedDef(revision, source,
//     output), grantDomainMatches the current def→source domain, and
//     matchesArm the local arm (grant id/boot/revision/actor/trusted
//     tenant/parents) with arm.ExpiresAt equal to the DB expiry and still
//     future;
//   - identity adoption guard: the run's ORIGINAL requested_by must be the
//     arm's actor; the run's frozen parents must match the arm exactly;
//   - automated due-ness: intent.next_action_at passed and automated query
//     budget remaining under the post-lock clock.
//
// Any refusal is a typed zero-write outcome (auth_required / not_due — never
// grant materials); a granted result carries the same shape as the manual
// claim (recoveryClaimDecision decides pending / expired shapes, IFF,
// binding, busy). No run/tick/cursor/admission writes ever happen here.
func (r *Repo) ClaimAutomaticRecovery(ctx context.Context, runID uuid.UUID, arm *ScheduleArmClaim, holder, token string, ttl time.Duration) (*RecoveryClaimResult, error) {
	if arm == nil {
		return nil, errors.New("ClaimAutomaticRecovery requires a local arm claim")
	}
	if err := validateLeaseInstall(holder, token, ttl); err != nil {
		return nil, err
	}
	t := arm.Tenant
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if pre == nil {
		return &RecoveryClaimResult{Outcome: RecoveryClaimNotVisible}, nil
	}
	if pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return &RecoveryClaimResult{Outcome: RecoveryClaimNotEligible}, nil
	}
	if pre.SourceID != arm.SourceID {
		// Fast pre-check only; the authoritative parent/target guard runs
		// under the locks below.
		return &RecoveryClaimResult{Outcome: RecoveryClaimAuthRequired}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *RecoveryClaimResult
	err = r.withRecoveryChainLock(ctx, pre, t, func(tx pgx.Tx, cp *checkpointRow) error {
		// --- Grant adjudication under the held Def lock (P24 predicates,
		// post-lock DB clock; mirrors ClaimDueScheduleSlot's live rule). ---
		var (
			revision    int
			defSource   uuid.UUID
			output      uuid.UUID
			defOrg      *uuid.UUID
			connOrg     *uuid.UUID
			sourceOwner uuid.UUID
			requestedBy *uuid.UUID
			dbNow       time.Time
		)
		err := tx.QueryRow(ctx, `
			SELECT d.schedule_revision, d.source_id, d.output_dataset_id, d.org_id,
			       c.org_id, c.owner_id, r.requested_by, clock_timestamp()
			  FROM batch_sync_defs d
			  JOIN connections c ON c.id = d.source_id
			  JOIN sync_runs r ON r.id = $2
			 WHERE d.id = $1`, pre.SyncDefID, runID).
			Scan(&revision, &defSource, &output, &defOrg, &connOrg, &sourceOwner, &requestedBy, &dbNow)
		if errors.Is(err, pgx.ErrNoRows) {
			out = &RecoveryClaimResult{Outcome: RecoveryClaimNotVisible}
			return nil
		}
		if err != nil {
			return err
		}
		grant, err := readScheduleGrantRowTx(ctx, tx, pre.SyncDefID)
		if err != nil {
			return err
		}
		live := grant != nil &&
			grant.boundToLockedDef(revision, defSource, output) &&
			grantDomainMatches(grant, defOrg, connOrg, sourceOwner) &&
			grant.ExpiresAt.After(dbNow) &&
			grant.LastSeenAt.After(dbNow.Add(-scheduleOwnerFreshness))
		armed := live &&
			grant.matchesArm(arm) &&
			arm.ExpiresAt.Equal(grant.ExpiresAt) &&
			arm.ExpiresAt.After(dbNow)
		if !armed {
			out = &RecoveryClaimResult{Outcome: RecoveryClaimAuthRequired}
			return nil
		}
		// Identity adoption guard: the ORIGINAL requester of this run is the
		// arm's actor, and the run's frozen parents/target match the arm.
		if requestedBy == nil || *requestedBy != arm.Actor ||
			pre.SourceID != arm.SourceID ||
			pre.RecoveryOutputDatasetID != arm.OutputDatasetID {
			out = &RecoveryClaimResult{Outcome: RecoveryClaimAuthRequired}
			return nil
		}
		// Automated due-ness under the post-lock clock (nothing was written
		// yet — a not-due answer is zero-write).
		intent, err := publishIntentByID(ctx, tx, runID)
		if err != nil {
			return err
		}
		if intent != nil {
			if intent.RecoveryState == IntentStateFinalized {
				out = &RecoveryClaimResult{Outcome: RecoveryClaimNotEligible}
				return nil
			}
			if (intent.NextActionAt != nil && intent.NextActionAt.After(dbNow)) ||
				intent.QueryAttempts >= MaxRecoveryQueryAttempts {
				out = &RecoveryClaimResult{Outcome: RecoveryClaimNotDue}
				return nil
			}
		}
		// Same-transaction delegation to the shared locked core.
		res, derr := recoveryClaimDecision(ctx, tx, pre, cp, t, holder, token, ttl)
		if derr != nil {
			return derr
		}
		out = res
		return nil
	})
	if errors.Is(err, errRunNotVisible) || errors.Is(err, errCPNotVisible) {
		return &RecoveryClaimResult{Outcome: RecoveryClaimNotVisible}, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}
