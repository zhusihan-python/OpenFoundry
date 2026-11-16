// P2.4 schedule authorization: the process-local grant registry, its
// maintenance loop, and the arm/revoke/ticks HTTP surface.
//
// Security model (root contract): a scheduled execution acts under the
// identity of an explicitly re-armed trusted caller. The caller's
// Authorization header exists ONLY in this process's registry — never in any
// column, log or response. Every successful authenticated arm mints a fresh
// random grant_id (an epoch); the DB stores the NONSECRET generation
// (grant/boot/revision/actor/tenant snapshot/parent bindings/absolute
// expiry) for cross-instance adjudication. The absolute expiry is
// min(verified JWT exp, reserve-time + TTL) and is anchored BEFORE the
// output precheck and the DB round trips; nothing but a fresh authenticated
// arm ever extends it. A restart loses the in-memory copy: the DB row's
// heartbeat goes stale and claims honestly record auth_required until a
// fresh caller re-arms.
//
// Concurrency discipline (root round-1): reservations are IDENTIFIED
// tickets; stage → DB arm → activate for one def serialize under a per-def
// key mutex (never the registry mutex, which is never held across
// network/DB IO); a late or failed arm cleans only its own generation and
// can never replace or remove a later successful one. Stop closes the
// stage/activate gate first, then clears every credential and joins the
// bounded maintenance loop; a DB arm returning after Stop cannot repopulate
// a JWT or report armed=true.
package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// ScheduleGrantStore is the repo surface the arm/revoke/ticks handlers and
// the authorizer's activate-time DB verification need.
type ScheduleGrantStore interface {
	ArmScheduleGrant(ctx context.Context, defID uuid.UUID, t repo.Tenant, in models.ScheduleGrantWrite, expectRevision int, expectOutput uuid.UUID) (*models.ScheduleGrantView, error)
	RevokeScheduleGrant(ctx context.Context, defID uuid.UUID, t repo.Tenant) (*uuid.UUID, error)
	DeleteScheduleGrantExact(ctx context.Context, defID, grantID uuid.UUID, t repo.Tenant) (bool, error)
	GetScheduleGrantView(ctx context.Context, defID uuid.UUID, t repo.Tenant) (*models.ScheduleGrantView, error)
	ListScheduleTicks(ctx context.Context, defID uuid.UUID, t repo.Tenant, limit int) ([]models.ScheduleTick, error)
}

// ScheduleAuthStore is the authorizer's own DB dependency subset.
type ScheduleAuthStore interface {
	HeartbeatScheduleGrant(ctx context.Context, defID, grantID, bootID, actor uuid.UUID, revision int, freshness time.Duration) (bool, error)
	GetScheduleGrantView(ctx context.Context, defID uuid.UUID, t repo.Tenant) (*models.ScheduleGrantView, error)
}

// ScheduleAuthorizerConfig carries the validated bounds; zero fields fall
// back to the defaults below.
type ScheduleAuthorizerConfig struct {
	MemoryTTL     time.Duration // default 300s (config range 1..86400)
	RegistryCap   int           // default 1024
	Freshness     time.Duration // default 90s (must match repo's window)
	MaintainEvery time.Duration // default 30s
}

const (
	defaultScheduleAuthTTL     = 300 * time.Second
	defaultScheduleRegistryCap = 1024
	defaultScheduleFreshness   = 90 * time.Second
	defaultScheduleMaintain    = 30 * time.Second
	// scheduleMaintainRoundBudget bounds one maintenance round's heartbeat
	// IO so the active expiry sweep can never be starved by up to
	// RegistryCap × 5s of sequential heartbeats; remaining generations are
	// carried to the next round.
	scheduleMaintainRoundBudget = 25 * time.Second
	// scheduleHeartbeatTimeout bounds each individual heartbeat call.
	scheduleHeartbeatTimeout = 5 * time.Second
)

func (c ScheduleAuthorizerConfig) withDefaults() ScheduleAuthorizerConfig {
	if c.MemoryTTL <= 0 {
		c.MemoryTTL = defaultScheduleAuthTTL
	}
	if c.RegistryCap <= 0 {
		c.RegistryCap = defaultScheduleRegistryCap
	}
	if c.Freshness <= 0 {
		c.Freshness = defaultScheduleFreshness
	}
	if c.MaintainEvery <= 0 {
		c.MaintainEvery = defaultScheduleMaintain
	}
	return c
}

// localArm is the in-memory grant generation: the complete nonsecret
// material plus the Authorization string (the only place the secret lives).
type localArm struct {
	GrantID         uuid.UUID
	BootID          uuid.UUID
	Actor           uuid.UUID
	Revision        int
	Tenant          repo.Tenant
	SourceID        uuid.UUID
	OutputDatasetID uuid.UUID
	Authorization   string
	ExpiresAt       time.Time
}

// defKey is the bounded per-def serialization for the arm/revoke sequence:
// a channel-based mutex with a reference count so the auxiliary map entry is
// RECLAIMED the moment the last holder/waiter is gone (the map size is
// bounded by in-flight operations, never by the def population), and the
// acquisition honors context cancellation (no unbounded mutex queue).
type defKey struct {
	acquire chan struct{} // buffered(1): a token means "lock free"
	refs    int
}

// scheduleArmTicket identifies one staged reservation (identified staging:
// release drops exactly this reservation, never a later one's).
type scheduleArmTicket struct {
	defID    uuid.UUID
	nonce    uuid.UUID
	stagedAt time.Time
	consumed sync.Once
}

// activateStatus discriminates ActivateArm's typed outcomes.
type activateStatus int

const (
	activateActivated activateStatus = iota
	activateSuperseded
	activateStopped
)

// ScheduleAuthorizer owns the per-process schedule grant registry and its
// independent maintenance loop (expiry sweep + owner heartbeats). Safe for
// concurrent use.
type ScheduleAuthorizer struct {
	cfg    ScheduleAuthorizerConfig
	store  ScheduleAuthStore
	bootID uuid.UUID
	now    func() time.Time

	mu     sync.Mutex
	closed bool
	arms   map[uuid.UUID]*localArm // defID → active local generation
	staged int
	keys   map[uuid.UUID]*defKey // per-def stage→DB→activate serialization (reclaimed at refs==0)

	rootCtx    context.Context
	rootCancel context.CancelFunc
	wg         sync.WaitGroup
	stopOnce   sync.Once
	stopDone   chan struct{}
	stopErr    error
}

// NewScheduleAuthorizer builds the authorizer. bootID must be the dedicated
// per-process schedule boot UUID (never the private lease-holder identity).
func NewScheduleAuthorizer(cfg ScheduleAuthorizerConfig, store ScheduleAuthStore, bootID uuid.UUID) *ScheduleAuthorizer {
	cfg = cfg.withDefaults()
	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &ScheduleAuthorizer{
		cfg:        cfg,
		store:      store,
		bootID:     bootID,
		now:        time.Now,
		arms:       map[uuid.UUID]*localArm{},
		keys:       map[uuid.UUID]*defKey{},
		rootCtx:    rootCtx,
		rootCancel: rootCancel,
		stopDone:   make(chan struct{}),
	}
}

// BootID returns the schedule boot UUID (nonsecret).
func (a *ScheduleAuthorizer) BootID() uuid.UUID { return a.bootID }

// LockDef acquires the per-def key for the WHOLE arm/revoke sequence
// (reserve → precheck → DB → activate/drop) — concurrent same-def operations
// are serialized end-to-end, so a late arm can never overwrite a newer
// activated generation and a revoke can never race a paused activation into
// re-installing a dropped credential. The auxiliary key state is reclaimed
// when the last reference goes away (bounded memory), acquisition honors the
// request context (no unbounded mutex queue), and a busy answer (false) is
// returned when the auxiliary map is at its bound and this def has no key
// yet. The registry mutex is NEVER held while the key is held or awaited.
func (a *ScheduleAuthorizer) LockDef(ctx context.Context, defID uuid.UUID) (func(), bool) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, false
	}
	k, ok := a.keys[defID]
	if !ok {
		if len(a.keys) >= a.cfg.RegistryCap {
			a.mu.Unlock()
			return nil, false
		}
		k = &defKey{acquire: make(chan struct{}, 1)}
		k.acquire <- struct{}{}
		a.keys[defID] = k
	}
	k.refs++
	a.mu.Unlock()
	select {
	case <-k.acquire:
	case <-ctx.Done():
		a.releaseKeyRef(defID, k)
		return nil, false
	}
	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		k.acquire <- struct{}{}
		a.releaseKeyRef(defID, k)
	}, true
}

// releaseKeyRef drops one reference and reclaims the map entry at zero — no
// holder and no waiter can be left behind.
func (a *ScheduleAuthorizer) releaseKeyRef(defID uuid.UUID, k *defKey) {
	a.mu.Lock()
	k.refs--
	if k.refs <= 0 {
		delete(a.keys, defID)
	}
	a.mu.Unlock()
}

// ReserveArmSlot reserves one bounded registry capacity slot BEFORE the DB
// arm, anchored at the reservation instant (the absolute TTL deadline starts
// here — before the output precheck and every DB round trip). Nil when the
// authorizer has stopped, or when the registry is full AND this def holds no
// active local generation: an EXISTING def's renewal replaces its own entry
// under the same-def serialization without growing the registry, so a full
// registry can always renew an armed def but never admits a fresh one.
func (a *ScheduleAuthorizer) ReserveArmSlot(defID uuid.UUID) *scheduleArmTicket {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if _, exists := a.arms[defID]; !exists && a.staged+len(a.arms) >= a.cfg.RegistryCap {
		return nil
	}
	a.staged++
	return &scheduleArmTicket{defID: defID, nonce: uuid.New(), stagedAt: a.now()}
}

// ReleaseArmSlot returns exactly this ticket's staged reservation (idempotent).
func (a *ScheduleAuthorizer) ReleaseArmSlot(t *scheduleArmTicket) {
	if t == nil {
		return
	}
	t.consumed.Do(func() {
		a.mu.Lock()
		if a.staged > 0 {
			a.staged--
		}
		a.mu.Unlock()
	})
}

// AbsoluteDeadline clamps the staged reservation's TTL deadline with the
// verified JWT expiry: min(JWT exp, stagedAt + TTL). It must be computed
// right after ReserveArmSlot, before any network work. The result is
// normalized DOWN to PostgreSQL's microsecond timestamp precision (and to
// UTC) so the DB-persisted expiry and the in-memory copy compare exactly in
// MatchesLiveGeneration — never extending the original reserve-anchored
// cutoff.
func (a *ScheduleAuthorizer) AbsoluteDeadline(t *scheduleArmTicket, jwtExp time.Time) time.Time {
	deadline := t.stagedAt.Add(a.cfg.MemoryTTL)
	if jwtExp.Before(deadline) {
		deadline = jwtExp
	}
	return deadline.UTC().Truncate(time.Microsecond)
}

// ActivateArm installs the local generation after the DB arm committed. It
// must be called under LockDef(defID). The DB row is re-read and must be
// EXACTLY this ticket's live generation — grant_id, boot, actor, revision
// AND the persisted (microsecond-truncated) absolute expiry — with Live
// still true at the database clock; anything less (a later arm already
// overwrote the row, the grant lapsed, or the write raced a domain change)
// answers superseded, keeping the DB metadata and the local token from the
// same exact generation. The ticket is always consumed. On stopped, the gate
// was closed: nothing is stored and a DB arm that raced Stop leaves its
// committed-but-unactivated generation to the caller for exact cleanup.
func (a *ScheduleAuthorizer) ActivateArm(t *scheduleArmTicket, material models.ScheduleGrantWrite, authorization string) (activateStatus, error) {
	if t == nil {
		return activateStopped, errors.New("nil arm ticket")
	}
	defer a.ReleaseArmSlot(t)
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return activateStopped, nil
	}
	a.mu.Unlock()
	// DB verification happens under the caller's per-def lock, never holding
	// the registry mutex across the IO.
	view, err := a.store.GetScheduleGrantView(context.Background(), t.defID, repo.Tenant{OwnerID: material.OwnerSubject, OrgID: material.OrgID})
	if err != nil {
		return activateStopped, err
	}
	if view == nil || !view.Live ||
		view.GrantID != material.GrantID ||
		view.BootID != material.BootID ||
		view.ActorSubject != material.ActorSubject ||
		view.Revision != material.Revision ||
		!view.ExpiresAt.Equal(material.ExpiresAt) {
		return activateSuperseded, nil
	}
	arm := &localArm{
		GrantID:         material.GrantID,
		BootID:          material.BootID,
		Actor:           material.ActorSubject,
		Revision:        material.Revision,
		Tenant:          repo.Tenant{OwnerID: material.OwnerSubject, OrgID: material.OrgID},
		SourceID:        material.SourceID,
		OutputDatasetID: material.OutputDatasetID,
		Authorization:   authorization,
		ExpiresAt:       view.ExpiresAt,
	}
	a.mu.Lock()
	// Re-check closed: Stop may have raced the DB read.
	if a.closed {
		a.mu.Unlock()
		return activateStopped, nil
	}
	a.arms[t.defID] = arm
	a.mu.Unlock()
	return activateActivated, nil
}

// ArmClaim returns the claim material for the def's current local generation
// (including the Authorization secret), or nil when this process holds no
// active arm. An entry whose absolute expiry already passed is dropped
// IMMEDIATELY (pure memory, independent of every sweep loop) before answering
// nil. A revision-mismatched entry is still returned — the claim transaction
// adjudicates generation staleness under its locks.
func (a *ScheduleAuthorizer) ArmClaim(defID uuid.UUID) *repo.ScheduleArmClaim {
	a.mu.Lock()
	arm, ok := a.arms[defID]
	now := a.now()
	if ok && arm != nil && !arm.ExpiresAt.After(now) {
		delete(a.arms, defID)
		ok = false
	}
	a.mu.Unlock()
	if !ok || arm == nil {
		return nil
	}
	return &repo.ScheduleArmClaim{
		GrantID:         arm.GrantID,
		BootID:          arm.BootID,
		Actor:           arm.Actor,
		Revision:        arm.Revision,
		Tenant:          arm.Tenant,
		SourceID:        arm.SourceID,
		OutputDatasetID: arm.OutputDatasetID,
		Authorization:   arm.Authorization,
		ExpiresAt:       arm.ExpiresAt,
	}
}

// MatchesLiveGeneration is the honest locally_executable verdict: this
// process holds an active entry AND the DB view is live AND both describe
// exactly the same generation. Map membership alone never qualifies.
func (a *ScheduleAuthorizer) MatchesLiveGeneration(defID uuid.UUID, view *models.ScheduleGrantView) bool {
	if view == nil || !view.Live {
		return false
	}
	a.mu.Lock()
	arm, ok := a.arms[defID]
	a.mu.Unlock()
	if !ok || arm == nil {
		return false
	}
	return arm.GrantID == view.GrantID &&
		arm.BootID == view.BootID &&
		arm.Revision == view.Revision &&
		arm.Actor == view.ActorSubject &&
		arm.ExpiresAt.Equal(view.ExpiresAt)
}

// Drop removes the local generation only if it is still exactly grantID (a
// revoke/heartbeat result must never erase a later successful arm).
func (a *ScheduleAuthorizer) Drop(defID, grantID uuid.UUID) {
	a.mu.Lock()
	if arm, ok := a.arms[defID]; ok && arm.GrantID == grantID {
		delete(a.arms, defID)
	}
	a.mu.Unlock()
}

// Start launches the two INDEPENDENT maintenance loops (both run even with
// the scheduler loop off):
//
//   - the active expiry sweep (1s, pure memory): clears every credential the
//     moment its absolute deadline passes — never blocked by heartbeat IO,
//     a full registry, or the scheduler being off;
//   - the heartbeat round (every MaintainEvery): heartbeats the snapshot of
//     active generations without holding the registry mutex, bounded per
//     call (5s) and per round (25s), dropping generations whose
//     exact-generation heartbeat came back false.
//
// Stop cancels and joins both, bounded.
func (a *ScheduleAuthorizer) Start(ctx context.Context) {
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-a.rootCtx.Done():
				return
			case <-ticker.C:
				now := a.now()
				a.mu.Lock()
				for defID, arm := range a.arms {
					if !arm.ExpiresAt.After(now) {
						delete(a.arms, defID)
					}
				}
				a.mu.Unlock()
			}
		}
	}()
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.cfg.MaintainEvery)
		defer ticker.Stop()
		for {
			select {
			case <-a.rootCtx.Done():
				return
			case <-ticker.C:
				a.maintainOnce(ctx)
			}
		}
	}()
}

func (a *ScheduleAuthorizer) maintainOnce(_ context.Context) {
	// 1. Active expiry sweep — pure memory, always runs.
	now := a.now()
	type hb struct {
		defID, grantID, bootID, actor uuid.UUID
		revision                      int
	}
	var beats []hb
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	for defID, arm := range a.arms {
		if !arm.ExpiresAt.After(now) {
			delete(a.arms, defID)
			continue
		}
		beats = append(beats, hb{defID, arm.GrantID, arm.BootID, arm.Actor, arm.Revision})
	}
	a.mu.Unlock()

	// 2. Heartbeats: snapshot IO without the registry mutex, round-budgeted
	// so remaining generations carry to the next round.
	deadline := now.Add(scheduleMaintainRoundBudget)
	for _, b := range beats {
		if a.now().After(deadline) {
			return
		}
		if a.rootCtx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(a.rootCtx), scheduleHeartbeatTimeout)
		ok, err := a.store.HeartbeatScheduleGrant(ctx, b.defID, b.grantID, b.bootID, b.actor, b.revision, a.cfg.Freshness)
		cancel()
		if err != nil {
			slog.Warn("schedule grant heartbeat failed; keeping local generation for the next round",
				slog.String("error", err.Error()), slog.String("sync_def_id", b.defID.String()))
			continue
		}
		if !ok {
			// Exact generation gone or stale (superseded/revoked/expired/
			// freshness lapsed): drop only that generation.
			a.Drop(b.defID, b.grantID)
		}
	}
}

// Stop closes the stage/activate gate, clears every credential and joins the
// bounded maintenance loop. Idempotent; the shared result is returned to
// every caller.
func (a *ScheduleAuthorizer) Stop() error {
	a.stopOnce.Do(func() {
		defer close(a.stopDone)
		a.mu.Lock()
		a.closed = true
		// Zero the credentials immediately.
		a.arms = map[uuid.UUID]*localArm{}
		a.mu.Unlock()
		a.rootCancel()
		joined := make(chan struct{})
		go func() {
			a.wg.Wait()
			close(joined)
		}()
		select {
		case <-joined:
		case <-time.After(a.cfg.MaintainEvery + scheduleHeartbeatTimeout + 5*time.Second):
			a.stopErr = errors.New("schedule authorizer maintenance did not join within the stop budget")
			return
		}
	})
	<-a.stopDone
	return a.stopErr
}

// ArmScheduleAuthorization serves POST /syncs/{id}/schedule:arm.
//
//	200 armed (with the loop state — no false promise when the loop is off)
//	400 authorization already expired / invalid arm input
//	403 not allowed to write this source
//	404 def/source not visible in tenant
//	409 target changed under us (retry) or superseded by a later arm
//	503 authorizer not wired / registry full / shutting down / activation DB failure
//	5xx honest infrastructure failure
func (h *Handlers) ArmScheduleAuthorization(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	defID, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), defID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	conn, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if conn == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(conn, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	authz, grants := h.ScheduleAuthorizer, h.ScheduleGrants
	if authz == nil || grants == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "schedule authorization is not wired on this instance")
		return
	}

	// Whole arm sequence serialized per def (root round-1 TOCTOU rule); a
	// busy key answers 503 immediately instead of queueing the request.
	unlock, ok := authz.LockDef(r.Context(), defID)
	if !ok {
		writeJSONErr(w, http.StatusServiceUnavailable, "schedule authorization is busy for this sync or the service is shutting down; retry")
		return
	}
	defer unlock()

	ticket := authz.ReserveArmSlot(defID)
	if ticket == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "schedule authorization registry is full or the service is shutting down")
		return
	}
	// Absolute deadline anchored at the reservation, BEFORE precheck/DB
	// latency; clamped with the verified JWT exp.
	expiresAt := authz.AbsoluteDeadline(ticket, claims.ExpiresAt())
	if !expiresAt.After(time.Now()) {
		authz.ReleaseArmSlot(ticket)
		writeJSONErr(w, http.StatusBadRequest, "schedule arm authorization window has already elapsed; re-arm with a fresh token")
		return
	}
	authorization := r.Header.Get("Authorization")
	if err := h.precheckOutputDataset(r.Context(), job.OutputDatasetID, authorization); err != nil {
		authz.ReleaseArmSlot(ticket)
		writeDatasetPrecheckError(w, err)
		return
	}
	grantID := uuid.New()
	material := models.ScheduleGrantWrite{
		SyncDefID:       defID,
		GrantID:         grantID,
		BootID:          authz.BootID(),
		ActorSubject:    claims.Sub,
		OwnerSubject:    tenant.OwnerID,
		SourceID:        job.SourceID,
		OutputDatasetID: job.OutputDatasetID,
		OrgID:           tenant.OrgID,
		Revision:        job.ScheduleRevision,
		ExpiresAt:       expiresAt,
	}
	view, err := grants.ArmScheduleGrant(r.Context(), defID, tenant, material, job.ScheduleRevision, job.OutputDatasetID)
	if err != nil {
		authz.ReleaseArmSlot(ticket)
		switch {
		case errors.Is(err, repo.ErrScheduleArmTargetChanged):
			writeJSONErr(w, http.StatusConflict, "schedule arm target changed; retry with a fresh precheck")
		case errors.Is(err, repo.ErrScheduleArmExpired):
			writeJSONErr(w, http.StatusBadRequest, "schedule arm authorization window has already elapsed; re-arm with a fresh token")
		case errors.Is(err, repo.ErrScheduleArmInput):
			writeJSONErr(w, http.StatusBadRequest, "schedule arm input inconsistent")
		default:
			slog.Error("schedule arm DB failure", slog.String("error", err.Error()), slog.String("sync_def_id", defID.String()))
			writeJSONErr(w, http.StatusInternalServerError, "schedule arm failed")
		}
		return
	}
	if view == nil {
		authz.ReleaseArmSlot(ticket)
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	status, actErr := authz.ActivateArm(ticket, material, authorization)
	// The ticket is consumed by ActivateArm; only exact-generation cleanup
	// may follow on the failure statuses.
	cleanupOwnGeneration := func(reason string) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancel()
		if _, err := grants.DeleteScheduleGrantExact(ctx, defID, grantID, tenant); err != nil {
			slog.Error("schedule arm cleanup after failed activation left its DB generation in place (freshness will lapse honestly)",
				slog.String("reason", reason), slog.String("error", err.Error()), slog.String("sync_def_id", defID.String()))
		}
	}
	switch {
	case actErr != nil:
		// Activation's DB verification failed: the grant is committed but
		// unactivated — clean exactly this generation.
		cleanupOwnGeneration("activate db read failed")
		slog.Error("schedule activation DB verification failed", slog.String("error", actErr.Error()), slog.String("sync_def_id", defID.String()))
		writeJSONErr(w, http.StatusInternalServerError, "schedule activation could not be verified; retry")
	case status == activateStopped:
		cleanupOwnGeneration("authorizer stopped")
		writeJSONErr(w, http.StatusServiceUnavailable, "service is shutting down; the arm was not activated")
	case status == activateSuperseded:
		// A later arm owns the row; the exact delete is a safe no-op.
		cleanupOwnGeneration("superseded")
		writeJSONErr(w, http.StatusConflict, "schedule arm superseded by a newer arm; retry")
	default:
		writeJSON(w, http.StatusOK, map[string]any{
			"armed":                    true,
			"grant_id":                 view.GrantID,
			"actor_subject":            view.ActorSubject,
			"schedule_revision":        view.Revision,
			"authorization_expires_at": view.ExpiresAt,
			"scheduler_loop_running":   h.ScheduleRunner.Enabled(),
			"note":                     "per-instance authorization; arm on the instance that should execute. Restart or expiry requires a fresh arm.",
		})
	}
}

// RevokeScheduleAuthorization serves DELETE /syncs/{id}/schedule:arm. The
// DB revoke is Def-lock linearized and returns the ACTUALLY deleted
// generation; only that local generation is dropped, so a concurrent newer
// arm survives. Revocation affects future admissions only.
func (h *Handlers) RevokeScheduleAuthorization(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	defID, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), defID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	conn, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if conn == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(conn, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	authz, grants := h.ScheduleAuthorizer, h.ScheduleGrants
	if authz == nil || grants == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "schedule authorization is not wired on this instance")
		return
	}
	// Same per-def serialization as arm: a revoke must never race a paused
	// activation into re-installing a dropped credential, and the exact-drop
	// must follow the DB revoke inside the same serialized window.
	unlock, ok := authz.LockDef(r.Context(), defID)
	if !ok {
		writeJSONErr(w, http.StatusServiceUnavailable, "schedule authorization is busy for this sync or the service is shutting down; retry")
		return
	}
	defer unlock()
	deletedID, err := grants.RevokeScheduleGrant(r.Context(), defID, tenant)
	if err != nil {
		slog.Error("schedule revoke DB failure", slog.String("error", err.Error()), slog.String("sync_def_id", defID.String()))
		writeJSONErr(w, http.StatusInternalServerError, "schedule revoke failed")
		return
	}
	if deletedID != nil {
		authz.Drop(defID, *deletedID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"revoked": deletedID != nil})
}

// writeInvalidSchedule renders the stable 400 invalid_schedule error body.
func writeInvalidSchedule(w http.ResponseWriter, err error) {
	var invalid *models.InvalidScheduleError
	if errors.As(err, &invalid) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":  "invalid_schedule",
			"detail": invalid.Field + ": " + invalid.Reason,
		})
		return
	}
	writeJSONErr(w, http.StatusBadRequest, "invalid_schedule")
}

// validateCreateSchedule fast-validates a create body's schedule fields (a
// fresh row carries no legacy state: the full supplied spec is validated).
func validateCreateSchedule(body *models.CreateSyncJobRequest) error {
	cron := ""
	if body.ScheduleCron != nil {
		cron = *body.ScheduleCron
	}
	tz := ""
	if body.ScheduleTimeZone != nil {
		tz = *body.ScheduleTimeZone
	}
	enabled := body.ScheduleEnabled != nil && *body.ScheduleEnabled
	// An empty explicit cron on create is legal only when disabled.
	spec, err := models.NormalizeScheduleSpec(cron, tz, enabled)
	if err != nil {
		return err
	}
	if body.ScheduleCron != nil && spec.Cron == "" && enabled {
		return &models.InvalidScheduleError{Field: "cron", Reason: "expression must not be empty"}
	}
	return nil
}

// validateUpdateSchedule fast-validates a PATCH with the repo's exact
// legacy-compatibility rules: only explicitly supplied NEW non-empty values
// are validated (even while disabled); enabling validates the merged cron.
// An untouched legacy invalid disabled cron survives untouched.
func validateUpdateSchedule(job *models.SyncJob, body *models.UpdateSyncJobRequest) error {
	if body.ScheduleTimeZone != nil && simpleTrim(*body.ScheduleTimeZone) != "" {
		if _, err := models.ResolveScheduleTimeZone(*body.ScheduleTimeZone); err != nil {
			return err
		}
	}
	if body.ScheduleCron != nil && simpleTrim(*body.ScheduleCron) != "" {
		effectiveTZ := job.ScheduleTimeZone
		if body.ScheduleTimeZone != nil {
			effectiveTZ = *body.ScheduleTimeZone
		}
		if _, err := models.ParseScheduleCron(*body.ScheduleCron, effectiveTZ); err != nil {
			return err
		}
	}
	enabled := job.ScheduleEnabled
	if body.ScheduleEnabled != nil {
		enabled = *body.ScheduleEnabled
	}
	if body.ScheduleCron != nil && simpleTrim(*body.ScheduleCron) == "" && body.ScheduleEnabled == nil {
		enabled = false
	}
	if enabled {
		cron := ""
		if job.ScheduleCron != nil {
			cron = *job.ScheduleCron
		}
		if body.ScheduleCron != nil {
			cron = *body.ScheduleCron
		}
		tz := job.ScheduleTimeZone
		if body.ScheduleTimeZone != nil {
			tz = *body.ScheduleTimeZone
		}
		if _, err := models.NormalizeScheduleSpec(cron, tz, true); err != nil {
			return err
		}
	}
	return nil
}

func simpleTrim(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

// ListScheduleTicks serves GET /syncs/{id}/schedule/ticks (default 50, max
// 200). Permission matches the sync list surface: tenant visibility of the
// def AND read access to its Source (canReadSource) — the repo's domain
// predicates alone are not sufficient for a same-org non-owner without a
// read grant. The DTO carries no private fields.
func (h *Handlers) ListScheduleTicks(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	defID, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 200 {
			writeJSONErr(w, http.StatusBadRequest, "limit must be an integer in [1,200]")
			return
		}
		limit = n
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), defID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	conn, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if conn == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canReadSource(conn, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	if h.ScheduleGrants == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "schedule authorization is not wired on this instance")
		return
	}
	items, err := h.ScheduleGrants.ListScheduleTicks(r.Context(), defID, tenant, limit)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to list schedule ticks")
		return
	}
	writeJSON(w, http.StatusOK, items)
}
