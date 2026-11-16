// P2.4 observable scheduled scheduling: DB claim protocol for batch sync defs.
//
// Operating model (design-draft v2 §6 / implementation-contract §3, round-1
// review incorporated):
//
//   - The scheduler scans due candidates locklessly with a keyset round-robin
//     cursor; the caller-supplied `now` is a SCAN HINT ONLY. After the Source
//     and Def row locks are held, a fresh clock_timestamp() is the single
//     authoritative time for due, grant expiry, freshness, next-fire and
//     admission decisions. A future-injected hint can therefore never fire an
//     early slot.
//   - One candidate per SHORT transaction, internally bounded by
//     scheduleOpBudget (5s) even when the caller's context has no deadline;
//     the bounded rollback budget is separate. The scheduler takes BOTH the
//     Source and the Def row FOR UPDATE NOWAIT — including policy-off
//     Sources — so one hot row can never park the poll loop; the public/manual
//     admission semantics (blocking FOR UPDATE for policy-off Sources) are
//     unchanged. A 55P03 aborts the transaction: the claim is unclaimed,
//     nothing is written, and the bounded deferred rollback is the ONLY thing
//     that may touch that transaction afterwards.
//   - Lock order is the pinned Source → Def order. UpdateSyncJob and the
//     arm/revoke transactions stay Def-only, so no reverse order exists.
//   - The claim re-verifies the candidate identity (expected revision AND
//     scheduled_for) after the locks; a mismatch is a clean stale_candidate
//     rejection with zero durable effects.
//   - Cron parse/validation happens on the LOCKED schedule, after the due
//     recheck and BEFORE every decision branch that needs a next cursor
//     (auth_required and the admission skips included). An unreadable
//     schedule (out-of-band corruption; write surfaces validate) records the
//     halted invalid_schedule tick with NO run and a NULL cursor pending
//     repair.
//   - Typed P2.3 admission refusals (capacity / min_interval) are Go-level
//     rejections on a still-live transaction: they atomically record the
//     skipped decision and advance the scheduling cursor WITHOUT touching the
//     the Source admission stamp (advanceSyncLastAdmitted runs ONLY on the
//     successful path, as the transaction's LAST database statement, after
//     cursor/tick/prune work).
//   - Duplicate-key handling matches the explicit slot-unique constraint
//     sync_schedule_ticks_slot_uniq ONLY; any other 23505 propagates.
//   - Grant adjudication: a live owner is a grant row whose revision AND
//     source/output bindings match the locked def, whose absolute expiry is in
//     the future and whose owner heartbeat is inside the freshness window.
//     Only a fully generation-matching LOCAL arm may consume the slot; a live
//     remote owner is never preempted; when NO live owner exists, any replica
//     records the honest auth_required tick and advances the cursor.
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

// scheduleOwnerFreshness bounds how stale a grant owner's heartbeat may be
// before the owner is considered dead (root contract: 90s — at the recommended
// 15–30s scheduler interval that tolerates at least three missed loops).
const scheduleOwnerFreshness = 90 * time.Second

// scheduleTickRetention is the per-def tick history bound pruned inside the
// claim transaction (500 rows ≈ 8h20m at one tick per minute).
const scheduleTickRetention = 500

// scheduleOpBudget bounds every P2.4 DB entry point internally (claim, arm,
// revoke, heartbeat, grant cleanup) even when the caller's context carries no
// deadline. It bounds the OPERATION wait, not the deferred rollback (that has
// its own 5s budget in rollbackAdmissionTx).
const scheduleOpBudget = 5 * time.Second

// ScheduleTickOutcome enumerates the durable decision of one claimed slot.
type ScheduleTickOutcome string

const (
	// TickOutcomeEnqueued: the run was durably admitted and enqueued. This
	// proves durable admission ONLY — not dispatch, and not execution.
	TickOutcomeEnqueued ScheduleTickOutcome = "enqueued"
	// TickOutcomeDispatchFailed: a later known Dispatch failure was recorded
	// with the real bounded-close aftermath. Run stays the execution truth.
	TickOutcomeDispatchFailed ScheduleTickOutcome = "dispatch_failed"
	// TickOutcomeAuthRequired: no live grant owner existed at claim time.
	TickOutcomeAuthRequired ScheduleTickOutcome = "auth_required"
	// TickOutcomeSkippedAdmissionCapacity: the shared P2.3 admission gate
	// refused the slot on capacity.
	TickOutcomeSkippedAdmissionCapacity ScheduleTickOutcome = "skipped_admission_capacity"
	// TickOutcomeSkippedAdmissionInterval: the shared gate refused on the
	// minimum-admission interval.
	TickOutcomeSkippedAdmissionInterval ScheduleTickOutcome = "skipped_admission_min_interval"
	// TickOutcomeErrorInvalidSchedule: the locked cron/time zone could not be
	// parsed (out-of-band corruption only — write surfaces validate). No run;
	// the cursor is parked NULL pending repair.
	TickOutcomeErrorInvalidSchedule ScheduleTickOutcome = "error_invalid_schedule"
	// TickOutcomeSkippedIncrementalBusy (P2.5): the def's checkpoint owner
	// was not free. A truthful no-run decision: the tick is recorded, the
	// CRON cursor advances, no Run is created, the source admission stamp is
	// NOT written and the DATA checkpoint does not move.
	TickOutcomeSkippedIncrementalBusy ScheduleTickOutcome = "skipped_incremental_busy"
	// TickOutcomeErrorBindingStale (P2.5): the stored checkpoint binding no
	// longer matches the authoritative source/def configuration. Same no-run
	// semantics as skipped_incremental_busy.
	TickOutcomeErrorBindingStale ScheduleTickOutcome = "error_binding_stale"
)

// auth_required reason codes recorded in sync_schedule_ticks.reason.
const (
	TickReasonNeverArmed     = "never_armed"
	TickReasonWindowTooShort = "window_too_short"
	TickReasonStaleRevision  = "stale_revision"
	TickReasonGrantExpired   = "grant_expired"
	TickReasonOwnerStale     = "owner_stale"
)

// ScheduleClaimSkip is the structured reason a claim attempt did not claim
// (nothing was written; the loop aggregates these as counters/logs only).
type ScheduleClaimSkip string

const (
	ScheduleSkipNotVisible  ScheduleClaimSkip = "not_visible"
	ScheduleSkipContention  ScheduleClaimSkip = "contention"
	ScheduleSkipStale       ScheduleClaimSkip = "stale_candidate"
	ScheduleSkipNotDue      ScheduleClaimSkip = "not_due"
	ScheduleSkipRemoteOwner ScheduleClaimSkip = "remote_owner"
)

// ScheduleScanKey is the keyset pagination cursor over due defs.
type ScheduleScanKey struct {
	NextFireAt time.Time
	DefID      uuid.UUID
}

// ScheduleCandidate is one lockless scan hit. Revision/ScheduledFor are the
// candidate identity the claim must re-verify under its locks.
type ScheduleCandidate struct {
	SyncDefID    uuid.UUID
	SourceID     uuid.UUID
	Revision     int
	ScheduledFor time.Time
}

// ScheduleArmClaim is the local grant material the armed path carries into the
// claim. Authorization is the caller's raw Authorization header — it exists in
// memory only, never in any column, log or response this package produces.
type ScheduleArmClaim struct {
	GrantID         uuid.UUID
	BootID          uuid.UUID
	Actor           uuid.UUID
	Revision        int
	Tenant          Tenant
	SourceID        uuid.UUID
	OutputDatasetID uuid.UUID
	Authorization   string
	ExpiresAt       time.Time
}

// ScheduleClaimResult is the complete observable outcome of one claim attempt.
type ScheduleClaimResult struct {
	// Claimed: a tick row was committed (with the cursor advance).
	Claimed bool
	// Skip is set iff !Claimed (structured, nothing written).
	Skip ScheduleClaimSkip
	// Outcome / TickID / ScheduledFor / NextFireAt are set when Claimed.
	Outcome      ScheduleTickOutcome
	TickID       uuid.UUID
	ScheduledFor time.Time
	NextFireAt   *time.Time
	// Run is the created pending run (enqueued only).
	Run *models.SyncRun
	// ArmTenant is the arm-captured trusted tenant (enqueued only; the worker
	// dispatches under it).
	ArmTenant Tenant
}

// ErrScheduleArmTargetChanged reports that the def's revision or output target
// moved between the arm precheck and the locked arm transaction: a clean
// retry signal, never a silent authorization of a newly retargeted output.
var ErrScheduleArmTargetChanged = errors.New("schedule arm target changed")

// ErrScheduleArmExpired reports that the arm input's absolute expiry (already
// clamped to min(verified JWT exp, reserve-time + TTL)) is not in the future
// under the post-lock database clock. The arm must be retried by a fresh
// caller, never written.
var ErrScheduleArmExpired = errors.New("schedule arm authorization already expired")

// ErrScheduleArmInput reports an inconsistent trusted arm input (mismatched
// def id, zero identifiers, or a snapshot that does not describe the locked
// def). Nothing is written.
var ErrScheduleArmInput = errors.New("schedule arm input inconsistent with locked definition")

// insertPendingSyncRun is the SINGLE durable-enqueue shape shared by the
// manual trigger (EnqueueSyncRun) and the scheduled claim: a pending
// controlled run whose org/owner snapshot and tenant-scoped parent-chain
// verification come from one INSERT..SELECT. Returns (nil, nil) when the def
// is not visible / not bound under the given tenant (no FK violation can ever
// surface to the caller).
func insertPendingSyncRun(ctx context.Context, tx pgx.Tx, runID, syncID, sourceID uuid.UUID, t Tenant, requestedBy uuid.UUID) (*models.SyncRun, error) {
	defPredicate, defArg := syncDefTenantPredicate(t, 4)
	row := tx.QueryRow(ctx,
		`INSERT INTO sync_runs AS r (id, sync_def_id, status, org_id, owner_id, requested_by, control_mode)
		 SELECT $1, d.id, 'pending', d.org_id, c.owner_id, $3, 'controlled'
		   FROM batch_sync_defs d JOIN connections c ON c.id = d.source_id
		  WHERE d.id = $2 AND d.source_id = $5 AND `+defPredicate+`
		 RETURNING `+runPublicColumns,
		runID, syncID, requestedBy, defArg, sourceID)
	run, err := scanSyncRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return run, nil
}

// ListDueScheduleCandidates scans due defs locklessly with keyset pagination.
// hint narrows the scan only; the claim re-adjudicates with the DB clock. An
// `after` key continues the round-robin past rows a previous page already
// handled (or skipped on contention), so contended oldest rows cannot starve
// later eligible defs.
func (r *Repo) ListDueScheduleCandidates(ctx context.Context, hint time.Time, after *ScheduleScanKey, limit int) ([]ScheduleCandidate, error) {
	sql := `SELECT d.id, d.source_id, d.schedule_revision, d.schedule_next_fire_at
	          FROM batch_sync_defs d
	         WHERE d.schedule_enabled AND d.schedule_next_fire_at IS NOT NULL
	           AND d.schedule_next_fire_at <= $1`
	args := []any{hint}
	if after != nil {
		sql += ` AND (d.schedule_next_fire_at, d.id) > ($2, $3)`
		args = append(args, after.NextFireAt, after.DefID)
	}
	sql += ` ORDER BY d.schedule_next_fire_at, d.id LIMIT ` + strconv.Itoa(limit)
	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ScheduleCandidate, 0)
	for rows.Next() {
		var c ScheduleCandidate
		if err := rows.Scan(&c.SyncDefID, &c.SourceID, &c.Revision, &c.ScheduledFor); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// scheduleGrantRow mirrors one sync_schedule_grants row for adjudication.
type scheduleGrantRow struct {
	GrantID, BootID, ActorSubject, OwnerSubject, SourceID, OutputDatasetID uuid.UUID
	OrgID                                                                  *uuid.UUID
	Revision                                                               int
	ArmedAt, ExpiresAt, LastSeenAt                                         time.Time
}

// readScheduleGrantRowTx reads the def's grant row inside the caller's open
// transaction (post-lock). Returns (nil, nil) when no row exists.
func readScheduleGrantRowTx(ctx context.Context, tx pgx.Tx, defID uuid.UUID) (*scheduleGrantRow, error) {
	row := &scheduleGrantRow{}
	err := tx.QueryRow(ctx,
		`SELECT g.grant_id, g.boot_id, g.actor_subject, g.owner_subject, g.source_id, g.output_dataset_id,
		        g.org_id, g.schedule_revision, g.armed_at, g.expires_at, g.last_seen_at
		   FROM sync_schedule_grants g WHERE g.sync_def_id = $1`, defID).
		Scan(&row.GrantID, &row.BootID, &row.ActorSubject, &row.OwnerSubject, &row.SourceID, &row.OutputDatasetID,
			&row.OrgID, &row.Revision, &row.ArmedAt, &row.ExpiresAt, &row.LastSeenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

// matchesArm reports whether a live DB grant row is exactly this local arm's
// generation (grant id, boot, revision, actor, trusted tenant and parent
// bindings). A local snapshot must never execute against another grant.
func (g *scheduleGrantRow) matchesArm(arm *ScheduleArmClaim) bool {
	return g.GrantID == arm.GrantID &&
		g.BootID == arm.BootID &&
		g.Revision == arm.Revision &&
		g.ActorSubject == arm.Actor &&
		g.SourceID == arm.SourceID &&
		g.OutputDatasetID == arm.OutputDatasetID &&
		g.OwnerSubject == arm.Tenant.OwnerID &&
		equalUUIDPtr(g.OrgID, arm.Tenant.OrgID)
}

// grantDomainMatches is the domain-truth check shared by claim adjudication,
// the heartbeat and the read projection: the grant's trusted org/owner
// snapshot must describe the CURRENT def→source domain exactly.
//
// Org domain: grant org = def org = source org AND owner_subject = actor.
// Personal domain: all orgs NULL AND owner_subject = actor = source owner.
//
// MVCC parent reads only — callers must never take Source/Def row locks in
// reverse order to obtain these columns.
func grantDomainMatches(g *scheduleGrantRow, defOrg, connOrg *uuid.UUID, connOwner uuid.UUID) bool {
	if g.OwnerSubject != g.ActorSubject {
		return false
	}
	if defOrg == nil || connOrg == nil {
		return g.OrgID == nil && defOrg == nil && connOrg == nil && g.ActorSubject == connOwner
	}
	return g.OrgID != nil && *g.OrgID == *defOrg && *defOrg == *connOrg
}

// boundToLockedDef reports whether the grant row still describes the locked
// def exactly (revision and parent bindings). Used by adjudication and the
// heartbeat's MVCC re-check.
func (g *scheduleGrantRow) boundToLockedDef(revision int, sourceID, outputID uuid.UUID) bool {
	return g.Revision == revision && g.SourceID == sourceID && g.OutputDatasetID == outputID
}

func equalUUIDPtr(a, b *uuid.UUID) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func sameOptTime(a *time.Time, b time.Time) bool {
	return a != nil && a.Equal(b)
}

// pruneScheduleTicks keeps only the most recent scheduleTickRetention tick
// rows of one def. Runs inside the claim transaction (last DML before the
// admission-stamp advance on the success path).
func pruneScheduleTicks(ctx context.Context, tx pgx.Tx, defID uuid.UUID) error {
	_, err := tx.Exec(ctx,
		`DELETE FROM sync_schedule_ticks
		  WHERE sync_def_id = $1
		    AND id NOT IN (SELECT id FROM sync_schedule_ticks WHERE sync_def_id = $1
		                    ORDER BY created_at DESC LIMIT $2)`,
		defID, scheduleTickRetention)
	return err
}

// insertScheduleTickTx writes one tick row. The ON CONFLICT targets the
// explicit slot-unique constraint ONLY; zero affected rows means another
// claimer already recorded this exact slot and the caller must roll back
// without side effects. detail must already be bounded/sanitized.
func insertScheduleTickTx(ctx context.Context, tx pgx.Tx, tickID, defID uuid.UUID, revision int, scheduledFor, claimedAt time.Time, outcome ScheduleTickOutcome, runID *uuid.UUID, actor *uuid.UUID, reason, detail string) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO sync_schedule_ticks
		       (id, sync_def_id, schedule_revision, scheduled_for, claimed_at, outcome, run_id, actor, reason, detail)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF(left($10, 256), ''))
		 ON CONFLICT ON CONSTRAINT sync_schedule_ticks_slot_uniq DO NOTHING`,
		tickID, defID, revision, scheduledFor, claimedAt, string(outcome), runID, actor, reason, detail)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ClaimDueScheduleSlot claims (at most) the ONE overdue window the candidate
// represents, under the protocol described in the package comment. See
// ScheduleClaimResult for the observable outcomes.
func (r *Repo) ClaimDueScheduleSlot(ctx context.Context, cand ScheduleCandidate, arm *ScheduleArmClaim) (*ScheduleClaimResult, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	skip := func(s ScheduleClaimSkip) (*ScheduleClaimResult, error) {
		return &ScheduleClaimResult{Skip: s}, nil
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackAdmissionTx(tx)

	// 1. Source lock — the scheduler ALWAYS uses NOWAIT (bounded loop; a hot
	// or policy-off Source answers contention instead of parking the poll).
	// The armed path scopes the lock to the arm's trusted tenant snapshot; the
	// unarmed path (auth_required adjudication) touches schedule state only.
	srcSQL := `SELECT c.owner_id, c.org_id FROM connections c WHERE c.id = $1`
	srcArgs := []any{cand.SourceID}
	if arm != nil {
		pred, arg := connectionTenantPredicate(arm.Tenant, 2)
		srcSQL += ` AND ` + pred
		srcArgs = append(srcArgs, arg)
	}
	var sourceOwner uuid.UUID
	var connOrg *uuid.UUID
	err = tx.QueryRow(ctx, srcSQL+` FOR UPDATE NOWAIT`, srcArgs...).Scan(&sourceOwner, &connOrg)
	if errors.Is(err, pgx.ErrNoRows) {
		return skip(ScheduleSkipNotVisible)
	}
	if err != nil {
		// 55P03 aborts the transaction; the deferred bounded rollback is the
		// only cleanup. Contention is "unclaimed": no tick, no cursor, no run,
		// no stamp.
		if isLockNotAvailable(err) {
			return skip(ScheduleSkipContention)
		}
		return nil, err
	}

	// 2. Def lock — also NOWAIT, for the same boundedness (a def writer such
	// as UpdateSyncJob/arm/revoke answers contention instead of parking the
	// loop). The authoritative post-lock schedule state follows.
	defSQL := `SELECT d.schedule_cron, d.schedule_time_zone, d.schedule_enabled, d.schedule_revision,
	                  d.schedule_next_fire_at, d.output_dataset_id, d.org_id
	           FROM batch_sync_defs d WHERE d.id = $1 AND d.source_id = $2`
	defArgs := []any{cand.SyncDefID, cand.SourceID}
	if arm != nil {
		pred, arg := syncDefTenantPredicate(arm.Tenant, 3)
		defSQL += ` AND ` + pred
		defArgs = append(defArgs, arg)
	}
	var cron *string
	var tz string
	var enabled bool
	var revision int
	var cursor *time.Time
	var output uuid.UUID
	var defOrg *uuid.UUID
	err = tx.QueryRow(ctx, defSQL+` FOR UPDATE NOWAIT`, defArgs...).
		Scan(&cron, &tz, &enabled, &revision, &cursor, &output, &defOrg)
	if errors.Is(err, pgx.ErrNoRows) {
		return skip(ScheduleSkipNotVisible)
	}
	if err != nil {
		if isLockNotAvailable(err) {
			return skip(ScheduleSkipContention)
		}
		return nil, err
	}

	// 3. Fresh post-lock DB clock — the only time authority from here on.
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, err
	}

	// 4. Candidate identity CAS + due check against the DB clock (a future
	// scan hint can never fire an early slot).
	if revision != cand.Revision || !sameOptTime(cursor, cand.ScheduledFor) {
		return skip(ScheduleSkipStale)
	}
	if !enabled || cursor == nil || cursor.After(dbNow) {
		return skip(ScheduleSkipNotDue)
	}
	scheduledFor := *cursor

	res := &ScheduleClaimResult{ScheduledFor: scheduledFor}

	// finishNoRun commits a no-run decision (auth_required / skipped / error)
	// with the tick row, the cursor advance and the def's last-tick columns.
	finishNoRun := func(outcome ScheduleTickOutcome, reason string, next *time.Time, detail string, actor *uuid.UUID) (*ScheduleClaimResult, error) {
		tickID := uuid.New()
		inserted, err := insertScheduleTickTx(ctx, tx, tickID, cand.SyncDefID, revision, scheduledFor, dbNow, outcome, nil, actor, reason, detail)
		if err != nil {
			return nil, err
		}
		if !inserted {
			// The exact slot was recorded by another claimer (defense in
			// depth on top of the Source-lock serialization): roll back clean.
			return skip(ScheduleSkipStale)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE batch_sync_defs
			    SET schedule_next_fire_at = $2, schedule_last_fire_at = $3,
			        schedule_last_tick_at = $4, schedule_last_tick_outcome = $5
			  WHERE id = $1`,
			cand.SyncDefID, next, scheduledFor, dbNow, string(outcome)); err != nil {
			return nil, err
		}
		if err := pruneScheduleTicks(ctx, tx, cand.SyncDefID); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		res.Claimed = true
		res.Outcome = outcome
		res.TickID = tickID
		res.NextFireAt = next
		return res, nil
	}

	// 5. Cron parse/validation on the LOCKED schedule — after the due
	// recheck, BEFORE every branch below that needs a next cursor (including
	// auth_required and the admission skips).
	cronExpr := ""
	if cron != nil {
		cronExpr = *cron
	}
	var next *time.Time
	n, fireErr := models.NextScheduleFire(cronExpr, tz, dbNow)
	if fireErr != nil {
		// error_invalid_schedule: no run; cursor parked NULL pending repair
		// (write surfaces validate, so this is out-of-band corruption only).
		return finishNoRun(TickOutcomeErrorInvalidSchedule, "schedule_unreadable", nil, boundDetail(fireErr.Error()), nil)
	}
	next = &n

	// 6. Grant adjudication (post-lock, DB clock): revision/source/output
	// bindings AND the trusted org/owner domain snapshot must describe the
	// locked def→source exactly.
	grant, err := readScheduleGrantRowTx(ctx, tx, cand.SyncDefID)
	if err != nil {
		return nil, err
	}
	live := grant != nil &&
		grant.boundToLockedDef(revision, cand.SourceID, output) &&
		grantDomainMatches(grant, defOrg, connOrg, sourceOwner) &&
		grant.ExpiresAt.After(dbNow) &&
		grant.LastSeenAt.After(dbNow.Add(-scheduleOwnerFreshness))

	if arm != nil && live && grant.matchesArm(arm) && arm.ExpiresAt.After(dbNow) {
		// Armed path — continue to admission.
	} else if !live {
		reason := TickReasonNeverArmed
		var actor *uuid.UUID
		switch {
		case grant == nil:
			reason = TickReasonNeverArmed
		case arm != nil && grant.GrantID == arm.GrantID && !arm.ExpiresAt.After(dbNow):
			reason = TickReasonWindowTooShort
			actor = &arm.Actor
		case !grant.boundToLockedDef(revision, cand.SourceID, output):
			reason = TickReasonStaleRevision
		case !grant.ExpiresAt.After(dbNow):
			reason = TickReasonGrantExpired
		default:
			reason = TickReasonOwnerStale
		}
		if actor == nil && grant != nil {
			a := grant.ActorSubject
			actor = &a
		}
		return finishNoRun(TickOutcomeAuthRequired, reason, next, "", actor)
	} else {
		// A live owner exists but this claimant is not it (remote replica, or
		// a stale local generation): leave the slot strictly alone.
		return skip(ScheduleSkipRemoteOwner)
	}

	// 7. Shared P2.3 admission gate. Typed refusals are Go-level decisions on
	// a live transaction: record the skip + cursor advance atomically, never
	// the admission stamp (the stamp advances only on success).
	gateErr := admissionGate(ctx, tx, cand.SourceID)
	if gateErr != nil {
		var rejection *AdmissionRejection
		if errors.As(gateErr, &rejection) {
			switch rejection.Reason {
			case AdmissionReasonCapacity:
				return finishNoRun(TickOutcomeSkippedAdmissionCapacity, "", next, "", &arm.Actor)
			case AdmissionReasonMinInterval:
				return finishNoRun(TickOutcomeSkippedAdmissionInterval, "", next, "", &arm.Actor)
			}
		}
		return nil, gateErr
	}

	// 8. Durable pending run under the arm's trusted tenant (the parent chain
	// is re-verified by the INSERT's own tenant predicate, exactly like a
	// manual enqueue). P2.5 (repo review items 1+9): the LIGHTWEIGHT probe
	// decides mode under the already-held Source lock WITHOUT any full REST
	// parse (legacy defs stay byte-identical); only a probed-true source gets
	// the full authoritative binding — computed here under the ALREADY-HELD
	// Def strong lock — and takes checkpoint ownership in the SAME
	// transaction. Busy/stale are truthful no-run outcomes with the cron
	// cursor advanced; contention on the checkpoint row aborts the
	// transaction, so it answers a contention SKIP — never finishNoRun on
	// the aborted transaction.
	incremental, perr := sourceIncrementalEnabled(ctx, tx, cand.SourceID, arm.Tenant)
	if perr != nil {
		return nil, perr
	}
	runID := uuid.New()
	var run *models.SyncRun
	if incremental {
		binding, fingerprint, ferr := readIncrementalBinding(ctx, tx, cand.SyncDefID, cand.SourceID, arm.Tenant)
		if ferr != nil {
			return nil, ferr
		}
		var rejection *IncrementalRejection
		run, rejection, err = r.AcquireCheckpointForAdmission(ctx, tx, cand.SyncDefID, cand.SourceID, arm.Tenant, arm.Actor, binding, fingerprint)
		if errors.Is(err, errCheckpointLockContention) {
			return skip(ScheduleSkipContention)
		}
		if err != nil {
			return nil, err
		}
		if rejection != nil {
			switch rejection.Kind {
			case IncrementalBusy:
				return finishNoRun(TickOutcomeSkippedIncrementalBusy, "incremental_busy", next, "", &arm.Actor)
			case IncrementalBindingStale:
				return finishNoRun(TickOutcomeErrorBindingStale, "incremental_binding_stale", next, "", &arm.Actor)
			}
		}
	} else if sourceRecoveryEnabled(ctx, tx, cand.SourceID, arm.Tenant) {
		// P2.6 (v2.1): recovery-enabled snapshot admission on the scheduled
		// path — the claim already holds the Source+Def strong locks, so the
		// snapshot recovery binding freeze is race-free; no checkpoint row,
		// no owner, last-commit-wins snapshot semantics unchanged.
		binding, fingerprint, ferr := readSnapshotRecoveryBinding(ctx, tx, cand.SyncDefID, cand.SourceID, arm.Tenant)
		if ferr != nil {
			return nil, ferr
		}
		run, err = insertPendingSyncRunWithRecovery(ctx, tx, runID, cand.SyncDefID, cand.SourceID, arm.Tenant, arm.Actor, binding, fingerprint)
	} else {
		run, err = insertPendingSyncRun(ctx, tx, runID, cand.SyncDefID, cand.SourceID, arm.Tenant, arm.Actor)
	}
	if err != nil {
		return nil, err
	}
	if run == nil {
		return skip(ScheduleSkipNotVisible)
	}

	// 9. Cursor advance, tick, prune — then the admission stamp as the LAST
	// database statement of the successful transaction.
	// Round2 fix 2: the tick must reference the ACTUAL admitted run id — the
	// incremental path's run is created inside AcquireCheckpointForAdmission
	// (its own uuid), so the pre-minted outer runID would FK-fail (23503) and
	// roll the whole incremental schedule back. tick.run_id ==
	// checkpoint owner == returned Run.ID, for BOTH paths.
	tickRunID := run.ID
	tickID := uuid.New()
	inserted, err := insertScheduleTickTx(ctx, tx, tickID, cand.SyncDefID, revision, scheduledFor, dbNow, TickOutcomeEnqueued, &tickRunID, &arm.Actor, "", "")
	if err != nil {
		return nil, err
	}
	if !inserted {
		return skip(ScheduleSkipStale)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE batch_sync_defs
		    SET schedule_next_fire_at = $2, schedule_last_fire_at = $3,
		        schedule_last_tick_at = $4, schedule_last_tick_outcome = $5
		  WHERE id = $1`,
		cand.SyncDefID, next, scheduledFor, dbNow, string(TickOutcomeEnqueued)); err != nil {
		return nil, err
	}
	if err := pruneScheduleTicks(ctx, tx, cand.SyncDefID); err != nil {
		return nil, err
	}
	if err := advanceSyncLastAdmitted(ctx, tx, cand.SourceID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	res.Claimed = true
	res.Outcome = TickOutcomeEnqueued
	res.TickID = tickID
	res.Run = run
	res.ArmTenant = arm.Tenant
	res.NextFireAt = next
	return res, nil
}

// boundDetail clamps a detail string to the column bound. Callers must still
// pass pre-sanitized text (never credentials, DSNs or URL queries).
func boundDetail(s string) string {
	if len(s) > 256 {
		return s[:256]
	}
	return s
}

// ArmScheduleGrant installs/refreshes the def's nonsecret grant row inside a
// Def-locked transaction (linearized against claims and revokes). Trusted
// input consistency is validated BEFORE any write: the input must describe
// exactly the locked def (def id, source id, revision and the prechecked
// output target), the trusted tenant snapshot (org/owner/actor all equal the
// VERIFIED CALLER tenant — t.OwnerID is the caller's sub even in the org
// domain; the Source owner only snapshots run.owner_id), and a FRESH grant
// id (reusing the currently persisted grant_id under the Def lock is refused
// — an authenticated renew always mints a new epoch).
//
// Expiry is checked against the database clock TWICE: once after the Def lock
// and once AFTER the UPSERT — the UPSERT itself can wait on the grant row
// lock held by a heartbeat transaction, and a clock sampled before that wait
// cannot speak for the post-wait instant. A failed final check rolls the
// WHOLE transaction back, leaving the previous grant byte-identical. The
// returned Live flag is computed from the actual values, never hard-coded.
// The def's cursor and revision are deliberately untouched: an arm can never
// re-fire a slot. Returns (nil, nil) when the def is not visible in the
// tenant.
func (r *Repo) ArmScheduleGrant(ctx context.Context, defID uuid.UUID, t Tenant, in models.ScheduleGrantWrite, expectRevision int, expectOutput uuid.UUID) (*models.ScheduleGrantView, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	// Trusted-input consistency before anything touches the database.
	if in.SyncDefID != defID {
		return nil, ErrScheduleArmInput
	}
	if in.GrantID == uuid.Nil || in.BootID == uuid.Nil || in.ActorSubject == uuid.Nil ||
		in.OwnerSubject == uuid.Nil || in.SourceID == uuid.Nil || in.OutputDatasetID == uuid.Nil {
		return nil, ErrScheduleArmInput
	}
	if in.OwnerSubject != t.OwnerID || in.ActorSubject != t.OwnerID || !equalUUIDPtr(in.OrgID, t.OrgID) {
		return nil, ErrScheduleArmInput
	}
	if in.Revision != expectRevision || in.OutputDatasetID != expectOutput {
		return nil, ErrScheduleArmInput
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackAdmissionTx(tx)
	predicate, arg := syncDefTenantPredicate(t, 2)
	var revision int
	var output, sourceID uuid.UUID
	var defOrg, connOrg *uuid.UUID
	var connOwner uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.schedule_revision, d.output_dataset_id, d.source_id, d.org_id,
		        c.org_id, c.owner_id
		   FROM batch_sync_defs d JOIN connections c ON c.id = d.source_id
		  WHERE d.id = $1 AND `+predicate+` FOR UPDATE OF d`,
		defID, arg).Scan(&revision, &output, &sourceID, &defOrg, &connOrg, &connOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if revision != expectRevision || output != expectOutput || sourceID != in.SourceID {
		return nil, ErrScheduleArmTargetChanged
	}
	// Domain truth for the WRITE: the grant's org snapshot must land exactly
	// in the def→source domain (org: g.org = d.org = c.org; personal: all
	// NULL with the caller as the source's own domain identity).
	if !equalUUIDPtr(in.OrgID, connOrg) || !equalUUIDPtr(defOrg, connOrg) {
		return nil, ErrScheduleArmInput
	}
	if connOrg == nil && in.ActorSubject != connOwner {
		return nil, ErrScheduleArmInput
	}
	// Grant-generation immutability: the current grant_id may not be reused
	// by a renew under the same Def lock.
	existing, err := readScheduleGrantRowTx(ctx, tx, defID)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.GrantID == in.GrantID {
		return nil, ErrScheduleArmInput
	}
	// Fresh post-Def-lock clock: reject an expiry that lock waits pushed into
	// the past (and nonfuture inputs generally).
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, err
	}
	if !in.ExpiresAt.After(dbNow) {
		return nil, ErrScheduleArmExpired
	}
	row := tx.QueryRow(ctx,
		`INSERT INTO sync_schedule_grants
		       (sync_def_id, grant_id, schedule_revision, boot_id, actor_subject, org_id, owner_subject,
		        source_id, output_dataset_id, armed_at, expires_at, last_seen_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp(), $10, clock_timestamp())
		 ON CONFLICT (sync_def_id) DO UPDATE SET
		       grant_id = EXCLUDED.grant_id,
		       schedule_revision = EXCLUDED.schedule_revision,
		       boot_id = EXCLUDED.boot_id,
		       actor_subject = EXCLUDED.actor_subject,
		       org_id = EXCLUDED.org_id,
		       owner_subject = EXCLUDED.owner_subject,
		       source_id = EXCLUDED.source_id,
		       output_dataset_id = EXCLUDED.output_dataset_id,
		       armed_at = clock_timestamp(),
		       expires_at = EXCLUDED.expires_at,
		       last_seen_at = clock_timestamp()
		 RETURNING grant_id, actor_subject, boot_id, schedule_revision, armed_at, expires_at, last_seen_at`,
		defID, in.GrantID, revision, in.BootID, in.ActorSubject, in.OrgID, in.OwnerSubject,
		sourceID, output, in.ExpiresAt)
	view := &models.ScheduleGrantView{}
	if err := row.Scan(&view.GrantID, &view.ActorSubject, &view.BootID, &view.Revision, &view.ArmedAt, &view.ExpiresAt, &view.LastSeenAt); err != nil {
		return nil, err
	}
	// SECOND clock AFTER the UPSERT: the upsert can wait on the grant row
	// lock (e.g. a heartbeat transaction); if the absolute expiry crossed
	// during that wait, roll the whole transaction back — the previous grant
	// stays byte-identical and the caller must re-arm.
	var afterUpsert time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&afterUpsert); err != nil {
		return nil, err
	}
	if !in.ExpiresAt.After(afterUpsert) {
		return nil, ErrScheduleArmExpired
	}
	// Live computed from the actual persisted values at the post-upsert
	// clock: revision binding verified above, expiry verified above,
	// last_seen_at is clock_timestamp() from this same transaction.
	view.Live = view.Revision == revision && view.ExpiresAt.After(afterUpsert)
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return view, nil
}

// DeleteScheduleGrantExact removes ONLY the named grant generation under the
// Def lock. Used by the arm handler when a DB-committed grant never activated
// (superseded/stopped): late cleanup must never touch a later successful arm.
// Returns true iff that exact generation was still the row and got deleted.
func (r *Repo) DeleteScheduleGrantExact(ctx context.Context, defID, grantID uuid.UUID, t Tenant) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackAdmissionTx(tx)
	predicate, arg := syncDefTenantPredicate(t, 2)
	var locked uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND `+predicate+` FOR UPDATE`,
		defID, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM sync_schedule_grants WHERE sync_def_id = $1 AND grant_id = $2`, defID, grantID)
	if err != nil {
		return false, err
	}
	deleted := tag.RowsAffected() > 0
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return deleted, nil
}

// RevokeScheduleGrant removes the def's grant row under the Def lock and
// returns the grant_id of the ACTUALLY deleted generation (nil when the def
// is not visible or no row existed — idempotent). The caller drops only that
// local generation, never an unconditionally chosen one, so a concurrent
// newer arm survives a revoke that raced it. Revocation affects future
// admissions only — an already admitted run keeps the existing
// cancel/publish-gate API.
func (r *Repo) RevokeScheduleGrant(ctx context.Context, defID uuid.UUID, t Tenant) (*uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackAdmissionTx(tx)
	predicate, arg := syncDefTenantPredicate(t, 2)
	var locked uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND `+predicate+` FOR UPDATE`,
		defID, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var deletedID *uuid.UUID
	row := tx.QueryRow(ctx, `DELETE FROM sync_schedule_grants WHERE sync_def_id = $1 RETURNING grant_id`, defID)
	var gid uuid.UUID
	err = row.Scan(&gid)
	if errors.Is(err, pgx.ErrNoRows) {
		deletedID = nil
	} else if err != nil {
		return nil, err
	} else {
		deletedID = &gid
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return deletedID, nil
}

// HeartbeatScheduleGrant refreshes ONLY last_seen_at of the exact grant
// generation (def, grant_id, boot, actor, revision), and never revives stale
// ownership or extends the absolute expiry.
//
// Two-phase on ONE connection, mirroring the P2.1/P21 proof: a single UPDATE
// whose WHERE clause contains clock predicates may evaluate those quals
// BEFORE waiting on a pure row-lock blocker and succeed after expiry without
// EvalPlanQual re-evaluation. Here phase 1 locks the exact generation FOR
// UPDATE (identity quals only, no clocks), phase 2 samples a FRESH
// clock_timestamp() after the lock, phase 3 re-validates expiry/freshness and
// the CURRENT def binding with MVCC reads (a plain def read — never a Def
// lock, which would invert the pinned order), and only then updates.
// Zero-rows/mismatch/expiry answers false: the caller must drop that local
// generation. The complete row is left untouched on every false path.
func (r *Repo) HeartbeatScheduleGrant(ctx context.Context, defID, grantID, bootID, actor uuid.UUID, revision int, freshness time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackAdmissionTx(tx)
	// Phase 1: lock the exact generation — identity quals only, no clocks.
	row := &scheduleGrantRow{}
	err = tx.QueryRow(ctx,
		`SELECT g.grant_id, g.boot_id, g.actor_subject, g.owner_subject, g.source_id, g.output_dataset_id,
		        g.org_id, g.schedule_revision, g.armed_at, g.expires_at, g.last_seen_at
		   FROM sync_schedule_grants g
		  WHERE g.sync_def_id = $1 AND g.grant_id = $2 AND g.boot_id = $3
		    AND g.actor_subject = $4 AND g.schedule_revision = $5
		   FOR UPDATE`,
		defID, grantID, bootID, actor, revision).
		Scan(&row.GrantID, &row.BootID, &row.ActorSubject, &row.OwnerSubject, &row.SourceID, &row.OutputDatasetID,
			&row.OrgID, &row.Revision, &row.ArmedAt, &row.ExpiresAt, &row.LastSeenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Phase 2: fresh post-lock database clock.
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return false, err
	}
	// Phase 3: MVCC re-validation — current def binding AND trusted-domain
	// truth against the live def→source parent chain (never a reverse row
	// lock), expiry, freshness.
	var defRevision int
	var defSource, defOutput, connOwner uuid.UUID
	var defOrg, connOrg *uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT d.schedule_revision, d.source_id, d.output_dataset_id, d.org_id,
		        c.org_id, c.owner_id
		   FROM batch_sync_defs d JOIN connections c ON c.id = d.source_id
		  WHERE d.id = $1`, defID).
		Scan(&defRevision, &defSource, &defOutput, &defOrg, &connOrg, &connOwner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if !row.boundToLockedDef(defRevision, defSource, defOutput) ||
		!grantDomainMatches(row, defOrg, connOrg, connOwner) ||
		!row.ExpiresAt.After(dbNow) ||
		!row.LastSeenAt.After(dbNow.Add(-freshness)) {
		return false, nil
	}
	// Phase 4: the heartbeat write itself.
	if _, err := tx.Exec(ctx,
		`UPDATE sync_schedule_grants SET last_seen_at = clock_timestamp()
		  WHERE sync_def_id = $1 AND grant_id = $2`, defID, grantID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// GetScheduleGrantView projects the def's grant row with its liveness computed
// IN THE SAME SQL against the ACTUAL joined def→source parent chain (revision
// AND source/output bindings AND the trusted org/owner domain — never a
// caller-supplied, possibly stale revision), at the database clock. Returns
// (nil, nil) when no row exists or the def is not visible in the tenant.
// Contains no secret material.
func (r *Repo) GetScheduleGrantView(ctx context.Context, defID uuid.UUID, t Tenant) (*models.ScheduleGrantView, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	predicate, arg := syncDefTenantPredicate(t, 3)
	row := r.Pool.QueryRow(ctx,
		`SELECT g.grant_id, g.actor_subject, g.boot_id, g.schedule_revision, g.armed_at, g.expires_at, g.last_seen_at,
		       (g.schedule_revision = d.schedule_revision
		        AND g.source_id = d.source_id
		        AND g.output_dataset_id = d.output_dataset_id
		        AND ((g.org_id IS NOT NULL AND g.org_id = d.org_id AND d.org_id = c.org_id
		              AND g.owner_subject = g.actor_subject)
		          OR (g.org_id IS NULL AND d.org_id IS NULL AND c.org_id IS NULL
		              AND g.owner_subject = g.actor_subject AND g.actor_subject = c.owner_id))
		        AND g.expires_at > clock_timestamp()
		        AND g.last_seen_at > clock_timestamp() - make_interval(secs => $2::float8)) AS live
		   FROM sync_schedule_grants g
		   JOIN batch_sync_defs d ON d.id = g.sync_def_id
		   JOIN connections c ON c.id = d.source_id
		  WHERE g.sync_def_id = $1 AND `+predicate,
		defID, scheduleOwnerFreshness.Seconds(), arg)
	view := &models.ScheduleGrantView{}
	err := row.Scan(&view.GrantID, &view.ActorSubject, &view.BootID, &view.Revision, &view.ArmedAt, &view.ExpiresAt, &view.LastSeenAt, &view.Live)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return view, nil
}

// ListScheduleTicks returns the def's tick history (newest first), scoped by
// the same trusted tenant predicate as every sync-run read.
func (r *Repo) ListScheduleTicks(ctx context.Context, defID uuid.UUID, t Tenant, limit int) ([]models.ScheduleTick, error) {
	if limit < 1 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	predicate, arg := syncDefTenantPredicate(t, 2)
	rows, err := r.Pool.Query(ctx,
		`SELECT t.id, t.sync_def_id, t.schedule_revision, t.scheduled_for, t.claimed_at, t.outcome,
		        t.run_id, t.actor, t.reason, t.detail, t.created_at
		   FROM sync_schedule_ticks t
		   JOIN batch_sync_defs d ON d.id = t.sync_def_id
		  WHERE t.sync_def_id = $1 AND `+predicate+`
		  ORDER BY t.created_at DESC LIMIT `+strconv.Itoa(limit),
		defID, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.ScheduleTick, 0)
	for rows.Next() {
		var v models.ScheduleTick
		if err := rows.Scan(&v.ID, &v.SyncDefID, &v.ScheduleRevision, &v.ScheduledFor, &v.ClaimedAt, &v.Outcome,
			&v.RunID, &v.Actor, &v.Reason, &v.Detail, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// RecordScheduleTickDispatchFailure marks a previously enqueued tick as
// dispatch_failed with the REAL bounded-close aftermath (bounded to the column
// limit). It matches the exact slot triple and only an enqueued outcome, and
// touches neither Source, Def, cursor nor stamp.
func (r *Repo) RecordScheduleTickDispatchFailure(ctx context.Context, defID uuid.UUID, revision int, scheduledFor time.Time, aftermath string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, scheduleOpBudget)
	defer cancel()
	tag, err := r.Pool.Exec(ctx,
		`UPDATE sync_schedule_ticks
		    SET outcome = $4, detail = left($5, 256)
		  WHERE sync_def_id = $1 AND schedule_revision = $2 AND scheduled_for = $3
		    AND outcome = $6`,
		defID, revision, scheduledFor, string(TickOutcomeDispatchFailed), boundDetail(aftermath), string(TickOutcomeEnqueued))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
