// P2.4 schedule runner: the per-instance tick loop that turns due schedule
// candidates into claimed ticks and dispatched runs.
//
// Loop shape (root contract): lockless keyset/round-robin candidate scan
// (never a fixed oldest LIMIT, which contended rows could starve); one
// candidate at a time through coordinator Reserve → claim → dispatch. The
// caller-supplied time is a scan hint only — every durable decision is
// re-adjudicated under the claim's locks with the database clock. Skips that
// never claimed anything (contention, local capacity, remote owner, stale or
// not-due candidates) are aggregated counters/logs, not rows; an `enqueued`
// tick proves durable admission only — dispatch happens after the commit and
// a known failure is recorded with the real bounded-close aftermath
// (dispatch_failed), while the run remains the execution/terminal truth.
package handlers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// ScheduleRunnerConfig carries the loop bounds.
type ScheduleRunnerConfig struct {
	Interval  time.Duration // ticker period (validated 1..30s in config)
	BatchSize int           // candidates per scan page (default 8)
	// MaxPages bounds the pages processed inside one RunOnce so a large due
	// backlog cannot monopolize a tick (default 10).
	MaxPages int
}

const (
	defaultScheduleBatchSize = 8
	defaultScheduleMaxPages  = 10
	// scheduleDispatchCloseTimeout bounds the bounded durable close after a
	// failed dispatch (same budget family as the coordinator's close paths).
	scheduleDispatchCloseTimeout = 5 * time.Second
)

func (c ScheduleRunnerConfig) withDefaults() ScheduleRunnerConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = defaultScheduleBatchSize
	}
	if c.MaxPages <= 0 {
		c.MaxPages = defaultScheduleMaxPages
	}
	return c
}

// ScheduleLoopStore is the runner's store subset.
type ScheduleLoopStore interface {
	ListDueScheduleCandidates(ctx context.Context, hint time.Time, after *repo.ScheduleScanKey, limit int) ([]repo.ScheduleCandidate, error)
	ClaimDueScheduleSlot(ctx context.Context, cand repo.ScheduleCandidate, arm *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error)
	RecordScheduleTickDispatchFailure(ctx context.Context, defID uuid.UUID, revision int, scheduledFor time.Time, aftermath string) (bool, error)
	RequestCancelSyncRun(ctx context.Context, runID, syncID uuid.UUID, t repo.Tenant, cancelledBy *uuid.UUID, cause string) (*repo.CancelResult, error)
	// GetSyncRunForTenant feeds the P2.6 cleanup skip (v1 rows stay for
	// fresh-auth recovery instead of the unsigned durable close).
	GetSyncRunForTenant(ctx context.Context, syncID, runID uuid.UUID, t repo.Tenant) (*models.SyncRun, error)
}

// ScheduleTickSummary aggregates one RunOnce for logging/observability.
type ScheduleTickSummary struct {
	Scanned  int
	Claimed  int
	Enqueued int
	Skips    map[string]int
	Errors   int
}

// scheduleAdmissions is the coordinator surface the runner needs (keeps the
// runner unit-testable without a live worker pool).
type scheduleAdmissions interface {
	Reserve() *Admission
}

// ScheduleRunner drives the loop for one process.
type ScheduleRunner struct {
	cfg   ScheduleRunnerConfig
	store ScheduleLoopStore
	coord scheduleAdmissions
	authz *ScheduleAuthorizer
	log   *slog.Logger
	now   func() time.Time

	// dispatch is injectable for tests; production uses DispatchScheduled.
	dispatch func(admission *Admission, res *repo.ScheduleClaimResult, cand repo.ScheduleCandidate, arm *repo.ScheduleArmClaim) bool

	mu        sync.Mutex
	scanAfter *repo.ScheduleScanKey
}

// NewScheduleRunner builds the runner.
func NewScheduleRunner(cfg ScheduleRunnerConfig, store ScheduleLoopStore, coord scheduleAdmissions, authz *ScheduleAuthorizer, log *slog.Logger) *ScheduleRunner {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	r := &ScheduleRunner{cfg: cfg, store: store, coord: coord, authz: authz, log: log, now: time.Now}
	r.dispatch = func(admission *Admission, res *repo.ScheduleClaimResult, cand repo.ScheduleCandidate, arm *repo.ScheduleArmClaim) bool {
		return admission.DispatchScheduled(res.Run.ID, cand.SyncDefID, cand.SourceID, res.ArmTenant, arm.Actor, arm.Authorization, arm.ExpiresAt)
	}
	return r
}

// Enabled reports whether the loop is wired (drives the arm response's
// scheduler_loop_running flag — an arm must never falsely promise execution).
func (r *ScheduleRunner) Enabled() bool { return r != nil }

func (r *ScheduleRunner) scanKey() *repo.ScheduleScanKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scanAfter
}

func (r *ScheduleRunner) setScanKey(key *repo.ScheduleScanKey) {
	r.mu.Lock()
	r.scanAfter = key
	r.mu.Unlock()
}

// RunOnce processes one bounded pass over the due candidates with the
// keyset/round-robin cursor: a short page continues past the rows this
// instance already handled or skipped on contention, and the cursor wraps to
// the beginning once a page runs dry — bounded fair progress across defs.
func (r *ScheduleRunner) RunOnce(ctx context.Context) ScheduleTickSummary {
	summary := ScheduleTickSummary{Skips: map[string]int{}}
	hint := r.now().UTC()
	for page := 0; page < r.cfg.MaxPages; page++ {
		cands, err := r.store.ListDueScheduleCandidates(ctx, hint, r.scanKey(), r.cfg.BatchSize)
		if err != nil {
			summary.Errors++
			r.log.Error("schedule candidate scan failed", slog.String("error", err.Error()))
			break
		}
		if len(cands) == 0 {
			r.setScanKey(nil)
			break
		}
		var last *repo.ScheduleScanKey
		for _, cand := range cands {
			last = &repo.ScheduleScanKey{NextFireAt: cand.ScheduledFor, DefID: cand.SyncDefID}
			r.processCandidate(ctx, cand, &summary)
		}
		if len(cands) < r.cfg.BatchSize {
			// Wrapped around the whole due set: continue from the oldest next
			// tick for fairness.
			r.setScanKey(nil)
			break
		}
		r.setScanKey(last)
	}
	if summary.Scanned > 0 || summary.Errors > 0 {
		r.log.Info("schedule tick",
			slog.Int("scanned", summary.Scanned),
			slog.Int("claimed", summary.Claimed),
			slog.Int("enqueued", summary.Enqueued),
			slog.Int("errors", summary.Errors),
			slog.Any("skips", summary.Skips))
	}
	return summary
}

func (r *ScheduleRunner) processCandidate(ctx context.Context, cand repo.ScheduleCandidate, summary *ScheduleTickSummary) {
	summary.Scanned++
	var arm *repo.ScheduleArmClaim
	if r.authz != nil {
		arm = r.authz.ArmClaim(cand.SyncDefID)
	}
	// Local capacity BEFORE any durable write (the P2.2 gate discipline): a
	// full coordinator answers local_full with zero DB effects.
	admission := r.coord.Reserve()
	if admission == nil {
		summary.Skips["local_full"]++
		return
	}
	res, err := r.store.ClaimDueScheduleSlot(ctx, cand, arm)
	if err != nil {
		admission.Release()
		summary.Errors++
		r.log.Error("schedule claim failed",
			slog.String("error", err.Error()), slog.String("sync_def_id", cand.SyncDefID.String()))
		return
	}
	if !res.Claimed {
		admission.Release()
		summary.Skips[string(res.Skip)]++
		return
	}
	summary.Claimed++
	if res.Outcome != repo.TickOutcomeEnqueued || res.Run == nil {
		admission.Release()
		return
	}
	summary.Enqueued++
	if arm == nil {
		// Unreachable by construction (an enqueued result implies the armed
		// path); honest close instead of a fabricated dispatch.
		r.closeAfterFailedDispatch(cand, res, summary, "internal error: enqueued tick without local arm material")
		admission.Release()
		return
	}
	if r.dispatch(admission, res, cand, arm) {
		return
	}
	// Shutdown race after the commit: the run stays the truth — bounded
	// durable close + the real aftermath recorded on the tick.
	r.closeAfterFailedDispatch(cand, res, summary, "aborted: service shutdown before schedule dispatch")
	admission.Release()
}

func (r *ScheduleRunner) closeAfterFailedDispatch(cand repo.ScheduleCandidate, res *repo.ScheduleClaimResult, summary *ScheduleTickSummary, cause string) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), scheduleDispatchCloseTimeout)
	defer cancel()
	// P2.6 (review round1 item 4 + followup): a p26-v1 row whose scheduled
	// dispatch never started is left for fresh-auth recovery — the unsigned
	// nil-generation cancel is skipped and the aftermath says so. The
	// decision uses the FROZEN protocol marker on the run row the claim
	// already returned (no probe read that could fail into an unfenced
	// close).
	if res.Run != nil && res.Run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		r.log.Info("schedule dispatch failure left p26-v1 run for fresh-auth recovery",
			slog.String("sync_run_id", res.Run.ID.String()))
		aftermath := cancelAftermath(cancelOutcomeV1LeftForRecovery, nil)
		recCtx, recCancel := context.WithTimeout(context.WithoutCancel(context.Background()), scheduleDispatchCloseTimeout)
		defer recCancel()
		if _, err := r.store.RecordScheduleTickDispatchFailure(recCtx, cand.SyncDefID, cand.Revision, res.ScheduledFor, aftermath); err != nil {
			r.log.Error("schedule dispatch-failure aftermath could not be recorded; tick stays enqueued honestly",
				slog.String("error", err.Error()), slog.String("sync_def_id", cand.SyncDefID.String()))
		}
		return
	}
	cancelRes, closeErr := r.store.RequestCancelSyncRun(closeCtx, res.Run.ID, cand.SyncDefID, res.ArmTenant, nil, cause)
	var outcome repo.CancelOutcome
	if cancelRes != nil {
		outcome = cancelRes.Outcome
	}
	aftermath := cancelAftermath(outcome, closeErr)
	recCtx, recCancel := context.WithTimeout(context.WithoutCancel(context.Background()), scheduleDispatchCloseTimeout)
	defer recCancel()
	if _, err := r.store.RecordScheduleTickDispatchFailure(recCtx, cand.SyncDefID, cand.Revision, res.ScheduledFor, aftermath); err != nil {
		r.log.Error("schedule dispatch-failure aftermath could not be recorded; tick stays enqueued honestly",
			slog.String("error", err.Error()), slog.String("sync_def_id", cand.SyncDefID.String()))
	}
	r.log.Warn("schedule tick did not dispatch; bounded close attempted",
		slog.String("aftermath", aftermath), slog.String("sync_def_id", cand.SyncDefID.String()))
}

// RunLoop ticks RunOnce at the configured interval until the context ends.
func (r *ScheduleRunner) RunLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = r.RunOnce(ctx)
		}
	}
}
