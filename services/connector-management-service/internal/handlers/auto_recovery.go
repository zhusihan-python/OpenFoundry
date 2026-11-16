// P2.6 default-off automatic recovery loop (auto-recovery-seam.md).
//
// The loop never invents authorization: adoption happens ONLY through
// ClaimAutomaticRecovery, which re-adjudicates the FULL P24 grant (boot/
// revision/domain/actor/expiry/liveness) inside the same lock transaction as
// the recovery claim — the process-local ArmClaim is just the address of a
// candidate grant, never the permission proof itself (ArmClaim alone cannot
// authorize a bare ClaimRecovery: revoke/revision TOCTOU). Candidates come
// from the bounded v1-only keyset scan; runs without a live local arm are
// aggregate skips with ZERO remote calls, budget consumption or run writes.
// The loop never creates runs/ticks/cursors and never touches the cron
// scheduler (ClaimDueScheduleSlot is deliberately never called).
//
// Disabled (the default) boots perform zero scans and zero DB writes — the
// runner is simply never started. A persisted grant without a live local arm
// (e.g. after a restart) always skips: only manual fresh-auth recovers then.
package handlers

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// armProvider is the process-local live-arm seam: *ScheduleAuthorizer in
// production, a scriptable fake in tests.
type armProvider interface {
	ArmClaim(defID uuid.UUID) *repo.ScheduleArmClaim
}

// AutoRecoveryConfig bounds one automatic recovery round.
type AutoRecoveryConfig struct {
	// Interval between rounds (validated ≥1s by the caller).
	Interval time.Duration
	// MaxCandidates bounds the candidates processed per round (pages of ≤100
	// from the keyset scan, fair wrap included).
	MaxCandidates int
	// RoundDeadline bounds one whole round.
	RoundDeadline time.Duration
}

const (
	defaultAutoRecoveryInterval  = 30 * time.Second
	defaultAutoRecoveryMaxCands  = 50
	defaultAutoRecoveryDeadline  = 4 * time.Minute // scan + query30s + up to 3 sends + finalize, with slack
	autoRecoveryScanPageLimit    = 100             // repo interface bound
	autoRecoveryRoundScanHintAge = 0               // hint = now (claim re-checks DB clock)
)

func (c AutoRecoveryConfig) withDefaults() AutoRecoveryConfig {
	if c.Interval <= 0 {
		c.Interval = defaultAutoRecoveryInterval
	}
	if c.MaxCandidates <= 0 {
		c.MaxCandidates = defaultAutoRecoveryMaxCands
	}
	if c.RoundDeadline <= 0 {
		c.RoundDeadline = defaultAutoRecoveryDeadline
	}
	return c
}

// AutoRecoveryCounts is the SAFE aggregate observability surface: counters
// only — never candidate identities, grant material or run projections.
type AutoRecoveryCounts struct {
	Rounds           int64
	Scanned          int64
	SkippedNoArm     int64
	ClaimsRefused    int64
	Recovered        int64
	ConvergedAborted int64
	StillUnknown     int64
}

// AutoRecoveryRunner drives the default-off automatic recovery rounds under
// the service lifecycle. It reuses the coordinator's ticket accounting
// (bounded, per-run deduplicated, Stop-joined) and the shared post-gate
// recovery service used by the manual endpoint.
type AutoRecoveryRunner struct {
	h      *Handlers
	co     *RunCoordinator
	auth   armProvider
	cfg    AutoRecoveryConfig
	counts AutoRecoveryCounts
	mu     sync.Mutex
	// scanAfter is the PROCESS-LOCAL keyset cursor carried ACROSS rounds
	// (review auto-round2 item 1): a first page of arm-less candidates can
	// never permanently hide later candidates with live arms.
	scanAfter *repo.RecoveryScanKey
	started   bool
	stop      chan struct{}
	done      chan struct{}
}

// NewAutoRecoveryRunner wires the loop; Start launches it under the given
// service context (call ONLY when config enables automatic recovery — a
// disabled boot must perform zero scans).
func NewAutoRecoveryRunner(h *Handlers, co *RunCoordinator, auth armProvider, cfg AutoRecoveryConfig) *AutoRecoveryRunner {
	return &AutoRecoveryRunner{
		h: h, co: co, auth: auth,
		cfg:  cfg.withDefaults(),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// Start launches the round loop; it returns immediately. The first round is
// delayed by one interval.
func (r *AutoRecoveryRunner) Start(ctx context.Context) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stop:
				return
			case <-ticker.C:
				r.RunOnce(ctx)
			}
		}
	}()
}

// Stop terminates the loop and waits for the in-flight round to converge.
func (r *AutoRecoveryRunner) Stop() {
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return
	}
	close(r.stop)
	<-r.done
}

// Counts returns the safe aggregate counters.
func (r *AutoRecoveryRunner) Counts() AutoRecoveryCounts {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts
}

func (r *AutoRecoveryRunner) bump(fn func(*AutoRecoveryCounts)) {
	r.mu.Lock()
	fn(&r.counts)
	r.mu.Unlock()
}

// RunOnce performs one bounded round: keyset scan (fair wrap) → per
// candidate: live local arm lookup (nil ⇒ aggregate skip, zero writes/remote)
// → ClaimAutomaticRecovery (full P24 grant re-adjudication in the claim
// transaction) → the shared post-gate recovery service under a bounded
// ticket context linked to the service root.
func (r *AutoRecoveryRunner) RunOnce(parent context.Context) {
	rs, err := r.h.recoveryStoreFor()
	if err != nil {
		return // store without recovery support: nothing to do, zero writes
	}
	port := r.h.recoveryPort()
	if port == nil {
		return // no remote surface configured: skip without any claim
	}
	roundCtx, cancelRound := context.WithTimeout(parent, r.cfg.RoundDeadline)
	defer cancelRound()
	r.bump(func(c *AutoRecoveryCounts) { c.Rounds++ })

	processed := 0
	for processed < r.cfg.MaxCandidates {
		if roundCtx.Err() != nil {
			return
		}
		limit := r.cfg.MaxCandidates - processed
		if limit > autoRecoveryScanPageLimit {
			limit = autoRecoveryScanPageLimit
		}
		r.mu.Lock()
		after := r.scanAfter
		r.mu.Unlock()
		cands, err := rs.ListRecoveryCandidates(roundCtx, time.Now(), after, limit)
		if err != nil {
			slog.Warn("auto recovery scan failed", slog.String("error", err.Error()))
			return
		}
		if len(cands) == 0 {
			// Tail reached (or the after-key rows are gone): fair bounded
			// wrap — the next scan restarts from the head.
			r.mu.Lock()
			r.scanAfter = nil
			r.mu.Unlock()
			return
		}
		for _, cand := range cands {
			if roundCtx.Err() != nil || processed >= r.cfg.MaxCandidates {
				return
			}
			processed++
			// The cursor advances per processed candidate, so hitting the
			// round cap mid-page still resumes AFTER the last processed row
			// next round.
			key := repo.RecoveryScanKey{ReadyAt: cand.ReadyAt, RunID: cand.RunID}
			r.mu.Lock()
			r.scanAfter = &key
			r.mu.Unlock()
			r.bump(func(c *AutoRecoveryCounts) { c.Scanned++ })
			// Live process-local arm only: the DB grant truth is adjudicated
			// atomically inside ClaimAutomaticRecovery — never before, never
			// separately.
			if r.auth == nil {
				r.bump(func(c *AutoRecoveryCounts) { c.SkippedNoArm++ })
				continue
			}
			arm := r.auth.ArmClaim(cand.SyncDefID)
			if arm == nil {
				r.bump(func(c *AutoRecoveryCounts) { c.SkippedNoArm++ })
				continue
			}
			r.processCandidate(roundCtx, rs, cand, arm)
		}
	}
}

// liveArmGuard returns the sticky per-outward revalidation bound to the arm
// that was STRONGLY claimed (review auto-round2 item 2): every check reloads
// the CURRENT live arm and requires grant/boot/revision/actor/tenant/source/
// target/expiry identity with the claimed snapshot. The first observed loss
// (drop, revoke, replacement, expiry) is sticky — no further outward call of
// this recovery can start. Raw JWT stays memory-only; the DB grant claim
// remains W2's atomic business.
func (r *AutoRecoveryRunner) liveArmGuard(defID uuid.UUID, claimed *repo.ScheduleArmClaim) func() error {
	var sticky error
	sameTenant := func(a, b repo.Tenant) bool {
		if a.OwnerID != b.OwnerID {
			return false
		}
		switch {
		case a.OrgID == nil && b.OrgID == nil:
			return true
		case a.OrgID == nil || b.OrgID == nil:
			return false
		default:
			return *a.OrgID == *b.OrgID
		}
	}
	return func() error {
		if sticky != nil {
			return sticky
		}
		if r.auth == nil {
			sticky = errors.New("live arm provider is gone")
			return sticky
		}
		current := r.auth.ArmClaim(defID)
		if current == nil {
			sticky = errors.New("live arm was dropped or revoked")
			return sticky
		}
		if current.GrantID != claimed.GrantID || current.BootID != claimed.BootID ||
			current.Revision != claimed.Revision || current.Actor != claimed.Actor ||
			current.SourceID != claimed.SourceID || current.OutputDatasetID != claimed.OutputDatasetID ||
			!current.ExpiresAt.Equal(claimed.ExpiresAt) || !sameTenant(current.Tenant, claimed.Tenant) {
			sticky = errors.New("live arm was replaced (grant/revision/identity changed)")
			return sticky
		}
		if !current.ExpiresAt.IsZero() && time.Now().After(current.ExpiresAt) {
			sticky = errors.New("live arm expired")
			return sticky
		}
		return nil
	}
}

// processCandidate adopts ONE candidate or skips it with zero writes.
func (r *AutoRecoveryRunner) processCandidate(roundCtx context.Context, rs RecoveryStore, cand repo.RecoveryCandidate, arm *repo.ScheduleArmClaim) {
	// Same ticket class as manual recovery: bounded, per-run deduplicated,
	// Stop-joined, never touching admission authReg/active.
	// The arm's absolute expiry (≤ the arm JWT's by P24 grant derivation)
	// caps the ticket alongside the local auth/task deadlines.
	ticket := r.co.ReserveRecoveryTicket(roundCtx, cand.RunID, arm.ExpiresAt)
	if ticket == nil {
		r.bump(func(c *AutoRecoveryCounts) { c.SkippedNoArm++ })
		return
	}
	defer ticket.Release()
	// The ticket context is additionally bounded by the arm's own expiry —
	// automatic work may never outlive the grant's local materialization.
	ctx := ticket.Context()
	if !arm.ExpiresAt.IsZero() {
		armCtx, cancel := context.WithDeadline(ctx, arm.ExpiresAt)
		defer cancel()
		ctx = armCtx
	}
	token, err := newLeaseToken()
	if err != nil {
		return
	}
	claimCtx, cancelClaim := context.WithTimeout(ctx, syncLeaseCallTimeout)
	claim, err := rs.ClaimAutomaticRecovery(claimCtx, cand.RunID, arm, r.h.leaseHolder()+"-auto", token, repo.RecoveryGateLeaseTTL)
	cancelClaim()
	if err != nil || claim.Outcome != repo.RecoveryClaimGranted {
		// Typed zero-write refusal (auth_required/not_due/busy/...): the
		// grant material itself never surfaces.
		r.bump(func(c *AutoRecoveryCounts) { c.ClaimsRefused++ })
		return
	}
	if claim.CancelRequested {
		if !claim.IntentExists {
			// Pre-gate crashed + cancelled: converge NOW through the fenced
			// pre-gate terminal write under THIS claim — zero source pull,
			// zero budget, zero remote (review auto-round2 item 3).
			aborted, aerr := r.h.finishAborted(ctx, arm.Tenant, claim.Claim, nil,
				"cancelled by request (automatic recovery converged a pre-gate crashed run)")
			if aerr != nil {
				slog.Warn("auto pre-gate cancel convergence failed; run stays honestly unconfirmed",
					slog.String("sync_run_id", cand.RunID.String()))
				r.bump(func(c *AutoRecoveryCounts) { c.StillUnknown++ })
				return
			}
			r.bump(func(c *AutoRecoveryCounts) { c.ConvergedAborted++ })
			_ = aborted
			return
		}
		// Post-intent cancel: zero-dispatch refusal; the owner is NOT released.
		r.bump(func(c *AutoRecoveryCounts) { c.ClaimsRefused++ })
		return
	}
	if !claim.IntentExists {
		// Pre-gate pending/expired-crashed run: the ORIGINAL authorized auto
		// support — a guarded fresh execution on the SAME claim and frozen
		// baseline (no new admission, run or tick). The sticky live-arm guard
		// runs before the target precheck, before EVERY source page and
		// before every recoverable send.
		guard := r.liveArmGuard(cand.SyncDefID, arm)
		job, jerr := r.h.Repo.GetSyncJob(ctx, cand.SyncDefID, arm.Tenant)
		if jerr != nil || job == nil || job.SourceID != cand.SourceID || job.OutputDatasetID != arm.OutputDatasetID {
			r.bump(func(c *AutoRecoveryCounts) { c.ClaimsRefused++ })
			return
		}
		conn, cerr := r.h.Repo.GetConnectionForTenant(ctx, job.SourceID, arm.Tenant)
		if cerr != nil || conn == nil || conn.ID != cand.SourceID {
			r.bump(func(c *AutoRecoveryCounts) { c.ClaimsRefused++ })
			return
		}
		if claim.Claim.Run.SyncDefID != cand.SyncDefID || claim.Claim.Run.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
			r.bump(func(c *AutoRecoveryCounts) { c.ClaimsRefused++ })
			return
		}
		if _, execErr := r.h.executeSnapshotSyncGuarded(ctx, arm.Authorization, arm.Tenant, claim.Claim, job, conn, true, guard); execErr != nil {
			slog.Info("automatic pre-gate execution did not converge; run keeps its durable state",
				slog.String("sync_run_id", cand.RunID.String()))
			r.bump(func(c *AutoRecoveryCounts) { c.StillUnknown++ })
			return
		}
		r.bump(func(c *AutoRecoveryCounts) { c.Recovered++ })
		return
	}
	// Post-intent: the shared post-gate service under the arm's tenant and
	// in-memory authorization, with the SAME sticky live-arm guard before
	// every query/send (review auto-round2 item 2).
	guard := r.liveArmGuard(cand.SyncDefID, arm)
	out := r.h.runPostGateRecovery(ctx, rs, claim, arm.Tenant, cand.RunID, arm.Authorization, false, guard)
	if out.err != nil {
		slog.Info("auto recovery did not converge; run stays recoverable",
			slog.String("outcome", "unknown"), slog.String("sync_run_id", cand.RunID.String()))
		r.bump(func(c *AutoRecoveryCounts) { c.StillUnknown++ })
		return
	}
	slog.Info("auto recovery round adopted a run", slog.String("sync_run_id", cand.RunID.String()))
	r.bump(func(c *AutoRecoveryCounts) { c.Recovered++ })
}
