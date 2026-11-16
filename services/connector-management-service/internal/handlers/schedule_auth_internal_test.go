// P2.4 schedule authorizer unit tests (internal package: exercises the
// registry's concurrency, shutdown and expiry discipline directly).
//
// Proof discipline (resumed-review-1): every critical invariant gets a
// minimal NEGATIVE regression — barrier handshakes instead of sleeps,
// observations that do not call the cleanup path under test, and concurrent
// sequences that must fail if the serialization/independent-sweep/Stop gates
// were removed.
package handlers

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// fakeAuthStore mirrors the authorizer's DB subset. GetView can be paused at
// a deterministic barrier (enter/release) and the heartbeat fn is injectable.
type fakeAuthStore struct {
	mu          sync.Mutex
	views       map[uuid.UUID]*models.ScheduleGrantView
	heartbeatFn func(defID uuid.UUID) (bool, error)

	// Activation-read barrier: when getEnter is non-nil it is closed on read
	// entry, and the read parks until getRelease (or the test deadline).
	getEnter   chan struct{}
	getRelease chan struct{}
}

func newFakeAuthStore() *fakeAuthStore {
	return &fakeAuthStore{views: map[uuid.UUID]*models.ScheduleGrantView{}}
}

func (f *fakeAuthStore) pauseNextGetView() (entered, release chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entered, release = make(chan struct{}), make(chan struct{})
	f.getEnter, f.getRelease = entered, release
	return entered, release
}

func (f *fakeAuthStore) GetScheduleGrantView(_ context.Context, defID uuid.UUID, _ repo.Tenant) (*models.ScheduleGrantView, error) {
	f.mu.Lock()
	enter, release := f.getEnter, f.getRelease
	view := f.views[defID]
	f.mu.Unlock()
	if enter != nil {
		// One-shot barrier: consume and clear so later reads run free.
		f.mu.Lock()
		f.getEnter, f.getRelease = nil, nil
		f.mu.Unlock()
		close(enter)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
			// Never hang the suite: an un-released barrier fails the test
			// via timeout below rather than blocking forever.
		}
	}
	if view == nil {
		return nil, nil
	}
	copied := *view
	return &copied, nil
}

func (f *fakeAuthStore) HeartbeatScheduleGrant(ctx context.Context, defID uuid.UUID, _, _, _ uuid.UUID, _ int, _ time.Duration) (bool, error) {
	fn := f.heartbeatFn
	if fn == nil {
		return true, nil
	}
	done := make(chan struct{})
	var ok bool
	var err error
	go func() { ok, err = fn(defID); close(done) }()
	select {
	case <-done:
		return ok, err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (f *fakeAuthStore) installView(defID uuid.UUID, view *models.ScheduleGrantView) {
	f.mu.Lock()
	f.views[defID] = view
	f.mu.Unlock()
}

func (f *fakeAuthStore) view(defID uuid.UUID) *models.ScheduleGrantView {
	f.mu.Lock()
	defer f.mu.Unlock()
	view, ok := f.views[defID]
	if !ok {
		return nil
	}
	copied := *view
	return &copied
}

// activateHelper arms the authorizer through its real public surface:
// reserve → (DB arm simulated by installing the fake view) → activate.
// It runs under the per-def key exactly like the HTTP handler.
func activateHelper(t *testing.T, a *ScheduleAuthorizer, store *fakeAuthStore, defID uuid.UUID, actor uuid.UUID, revision int, expiresAt time.Time) (activateStatus, error) {
	t.Helper()
	unlock, ok := a.LockDef(context.Background(), defID)
	require.True(t, ok)
	defer unlock()
	ticket := a.ReserveArmSlot(defID)
	require.NotNil(t, ticket)
	material := models.ScheduleGrantWrite{
		SyncDefID: defID, GrantID: uuid.New(), BootID: a.BootID(),
		ActorSubject: actor, OwnerSubject: actor,
		SourceID: uuid.New(), OutputDatasetID: uuid.New(),
		Revision: revision, ExpiresAt: expiresAt,
	}
	store.installView(defID, &models.ScheduleGrantView{
		GrantID: material.GrantID, ActorSubject: actor, BootID: material.BootID,
		Revision: revision, ExpiresAt: expiresAt, Live: true,
	})
	return a.ActivateArm(ticket, material, "Bearer test")
}

func activateHelperErr(_ activateStatus, err error) error { return err }

func TestScheduleAuthorizerIdentifiedTickets(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4}, store, uuid.New())
	defA, defB := uuid.New(), uuid.New()

	// Two staged tickets: a late/repeated release of A must never release B.
	tA := a.ReserveArmSlot(defA)
	tB := a.ReserveArmSlot(defB)
	require.NotNil(t, tA)
	require.NotNil(t, tB)
	a.ReleaseArmSlot(tA)
	a.ReleaseArmSlot(tA) // repeated release of A is idempotent
	a.mu.Lock()
	staged := a.staged
	a.mu.Unlock()
	require.Equal(t, 1, staged, "B's reservation must survive A's late releases")
	a.ReleaseArmSlot(tB)
	a.mu.Lock()
	staged = a.staged
	a.mu.Unlock()
	assert.Equal(t, 0, staged)

	// Stop closes staging permanently.
	require.NoError(t, a.Stop())
	assert.Nil(t, a.ReserveArmSlot(defA))
}

func TestScheduleAuthorizerFullRegistryRenewActivatesNewGeneration(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 2, MaintainEvery: time.Hour}, store, uuid.New())
	defer func() { require.NoError(t, a.Stop()) }()
	actor := uuid.New()
	later := a.now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	defA, defB := uuid.New(), uuid.New()

	require.NoError(t, activateHelperErr(activateHelper(t, a, store, defA, actor, 1, later)))
	require.NoError(t, activateHelperErr(activateHelper(t, a, store, defB, actor, 1, later)))
	oldGrant := a.ArmClaim(defA).GrantID

	// Full registry: a FRESH def is refused…
	assert.Nil(t, a.ReserveArmSlot(uuid.New()))

	// …but the EXISTING def renews through a REAL activation: the new
	// generation becomes active and the registry size stays at the cap.
	unlock, ok := a.LockDef(context.Background(), defA)
	require.True(t, ok)
	ticket := a.ReserveArmSlot(defA)
	require.NotNil(t, ticket)
	newMaterial := models.ScheduleGrantWrite{
		SyncDefID: defA, GrantID: uuid.New(), BootID: a.BootID(),
		ActorSubject: actor, OwnerSubject: actor, SourceID: uuid.New(),
		OutputDatasetID: uuid.New(), Revision: 1, ExpiresAt: later,
	}
	store.installView(defA, &models.ScheduleGrantView{
		GrantID: newMaterial.GrantID, ActorSubject: actor, BootID: newMaterial.BootID,
		Revision: 1, ExpiresAt: later, Live: true,
	})
	status, err := a.ActivateArm(ticket, newMaterial, "Bearer renewed")
	unlock()
	require.NoError(t, err)
	require.Equal(t, activateActivated, status)
	a.mu.Lock()
	size := len(a.arms)
	a.mu.Unlock()
	assert.Equal(t, 2, size, "renewal must replace its own entry, never grow the registry")
	claim := a.ArmClaim(defA)
	require.NotNil(t, claim)
	assert.Equal(t, newMaterial.GrantID, claim.GrantID)
	assert.NotEqual(t, oldGrant, claim.GrantID, "a renew is a fresh generation")
}

func TestScheduleAuthorizerActivateStrictGeneration(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4, MaintainEvery: time.Hour}, store, uuid.New())
	defer func() { require.NoError(t, a.Stop()) }()
	def := uuid.New()
	actor := uuid.New()
	later := a.now().Add(time.Minute).UTC().Truncate(time.Microsecond)

	build := func(mutate func(view *models.ScheduleGrantView)) (activateStatus, error) {
		unlock, ok := a.LockDef(context.Background(), def)
		require.True(t, ok)
		defer unlock()
		ticket := a.ReserveArmSlot(def)
		require.NotNil(t, ticket)
		material := models.ScheduleGrantWrite{
			SyncDefID: def, GrantID: uuid.New(), BootID: a.BootID(),
			ActorSubject: actor, OwnerSubject: actor, SourceID: uuid.New(),
			OutputDatasetID: uuid.New(), Revision: 3, ExpiresAt: later,
		}
		view := &models.ScheduleGrantView{
			GrantID: material.GrantID, ActorSubject: actor, BootID: material.BootID,
			Revision: 3, ExpiresAt: later, Live: true,
		}
		mutate(view)
		store.installView(def, view)
		return a.ActivateArm(ticket, material, "Bearer test")
	}

	// Exact generation activates.
	status, err := build(func(*models.ScheduleGrantView) {})
	require.NoError(t, err)
	assert.Equal(t, activateActivated, status)
	require.NotNil(t, a.ArmClaim(def))

	// Grant replaced by a later arm ⇒ superseded.
	status, err = build(func(v *models.ScheduleGrantView) { v.GrantID = uuid.New() })
	require.NoError(t, err)
	assert.Equal(t, activateSuperseded, status)

	// Not live at the DB clock ⇒ superseded (never install a dead grant).
	status, err = build(func(v *models.ScheduleGrantView) { v.Live = false })
	require.NoError(t, err)
	assert.Equal(t, activateSuperseded, status)

	// Nano-origin expiry mismatch ⇒ superseded (precision discipline).
	status, err = build(func(v *models.ScheduleGrantView) {
		v.ExpiresAt = later.Add(123 * time.Nanosecond)
	})
	require.NoError(t, err)
	assert.Equal(t, activateSuperseded, status)

	// Boot/actor/revision mismatches ⇒ superseded.
	status, err = build(func(v *models.ScheduleGrantView) { v.BootID = uuid.New() })
	require.NoError(t, err)
	assert.Equal(t, activateSuperseded, status)
	status, err = build(func(v *models.ScheduleGrantView) { v.ActorSubject = uuid.New() })
	require.NoError(t, err)
	assert.Equal(t, activateSuperseded, status)
	status, err = build(func(v *models.ScheduleGrantView) { v.Revision = 4 })
	require.NoError(t, err)
	assert.Equal(t, activateSuperseded, status)
}

func TestScheduleAuthorizerStopClosesLateActivation(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4, MaintainEvery: time.Hour}, store, uuid.New())
	def := uuid.New()
	actor := uuid.New()
	later := a.now().Add(time.Minute).UTC().Truncate(time.Microsecond)

	// Stage an arm and pause INSIDE ActivateArm's DB read.
	ticket := a.ReserveArmSlot(def)
	require.NotNil(t, ticket)
	material := models.ScheduleGrantWrite{
		SyncDefID: def, GrantID: uuid.New(), BootID: a.BootID(),
		ActorSubject: actor, OwnerSubject: actor, SourceID: uuid.New(),
		OutputDatasetID: uuid.New(), Revision: 1, ExpiresAt: later,
	}
	store.installView(def, &models.ScheduleGrantView{
		GrantID: material.GrantID, ActorSubject: actor, BootID: material.BootID,
		Revision: 1, ExpiresAt: later, Live: true,
	})
	entered, release := store.pauseNextGetView()
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }
	type result struct {
		status activateStatus
		err    error
	}
	resCh := make(chan result, 1)
	// Failure paths release the barrier and join the parked activation
	// boundedly (the fake's own 10s fallback is the outer bound).
	t.Cleanup(func() {
		doRelease()
		select {
		case <-resCh:
		case <-time.After(5 * time.Second):
		}
	})
	go func() {
		status, err := a.ActivateArm(ticket, material, "Bearer late")
		resCh <- result{status, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activation read never entered the barrier")
	}

	// Stop while the activation read is parked: the gate closes and every
	// credential clears.
	require.NoError(t, a.Stop())
	a.mu.Lock()
	arms := len(a.arms)
	a.mu.Unlock()
	assert.Equal(t, 0, arms)

	// Release the parked read: the activation must land on the SECOND closed
	// check (post-read), answer stopped, and never install the JWT.
	doRelease()
	select {
	case res := <-resCh:
		require.NoError(t, res.err)
		assert.Equal(t, activateStopped, res.status, "a DB arm returning after Stop must not activate")
	case <-time.After(5 * time.Second):
		t.Fatal("paused activation never returned")
	}
	assert.Nil(t, a.ArmClaim(def))
	a.mu.Lock()
	arms = len(a.arms)
	a.mu.Unlock()
	assert.Equal(t, 0, arms)

	// Staging stays closed; second Stop is idempotent.
	assert.Nil(t, a.ReserveArmSlot(def))
	assert.NoError(t, a.Stop())
}

func TestScheduleAuthorizerArmClaimDropsExpiredImmediately(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4, MaintainEvery: time.Hour}, store, uuid.New())
	defer func() { require.NoError(t, a.Stop()) }()
	def := uuid.New()
	actor := uuid.New()
	now := time.Now()
	a.now = func() time.Time { return now }
	// Nano-origin expiry (not second-aligned).
	expiry := now.Add(150 * time.Millisecond).UTC().Truncate(time.Microsecond)
	require.NoError(t, activateHelperErr(activateHelper(t, a, store, def, actor, 1, expiry)))
	require.NotNil(t, a.ArmClaim(def))

	now = now.Add(200 * time.Millisecond)
	assert.Nil(t, a.ArmClaim(def), "expired entry must answer nil immediately")
	a.mu.Lock()
	size := len(a.arms)
	a.mu.Unlock()
	assert.Equal(t, 0, size, "expired entry must be deleted, not merely skipped")
}

// TestScheduleAuthorizerExpirySweepIndependentOfHeartbeatIO proves the 1s
// pure-memory sweeper clears an expired credential while heartbeat IO is
// blocked IN FLIGHT. The observation reads the raw registry map directly —
// never ArmClaim, which performs its own expiry deletion and would keep the
// test green with the independent sweeper removed. The blocked heartbeat is
// released before Stop so no 10s goroutine lingers.
func TestScheduleAuthorizerExpirySweepIndependentOfHeartbeatIO(t *testing.T) {
	store := newFakeAuthStore()
	hbEntered := make(chan struct{})
	hbRelease := make(chan struct{})
	var hbReleaseOnce sync.Once
	doHbRelease := func() { hbReleaseOnce.Do(func() { close(hbRelease) }) }
	store.heartbeatFn = func(uuid.UUID) (bool, error) {
		select {
		case <-hbEntered:
		default:
			close(hbEntered)
		}
		<-hbRelease
		return true, nil
	}
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{
		RegistryCap: 4, MaintainEvery: 20 * time.Millisecond,
	}, store, uuid.New())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Start(ctx)
	defer func() { _ = a.Stop() }()

	def := uuid.New()
	actor := uuid.New()
	// Deadline comfortably after the first heartbeat round (20ms) so the
	// blocked IO provably precedes expiry; the 1s sweeper clears it after.
	expiry := time.Now().Add(400 * time.Millisecond).UTC().Truncate(time.Microsecond)
	require.NoError(t, activateHelperErr(activateHelper(t, a, store, def, actor, 1, expiry)))
	require.NotNil(t, a.ArmClaim(def))

	// Handshake: a heartbeat round is genuinely inside the blocked IO.
	select {
	case <-hbEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat IO never entered the barrier")
	}
	// The deadline passes while the heartbeat IO is still parked.

	// The independent sweeper must clear the raw registry reference while
	// the heartbeat IO is still blocked. The release is one-shot and a
	// cleanup guarantees it on every exit path (the heartbeat ctx bounds the
	// parked fn itself).
	t.Cleanup(doHbRelease)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		_, stillThere := a.arms[def]
		a.mu.Unlock()
		if !stillThere {
			doHbRelease()
			return // cleared by the sweep, not by ArmClaim
		}
		time.Sleep(20 * time.Millisecond)
	}
	doHbRelease()
	t.Fatal("expired credential was not swept within 3s despite blocked heartbeat IO")
}

// TestScheduleAuthorizerLockDefSerializeCancellableBounded proves, via
// explicit handshakes (no sleeps-as-proof): same-key mutual exclusion, key-map
// reclaim at zero references, the distinct-key busy cap, and cancellation.
func TestScheduleAuthorizerLockDefSerializeCancellableBounded(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 3, MaintainEvery: time.Hour}, store, uuid.New())
	defer func() { require.NoError(t, a.Stop()) }()
	def := uuid.New()

	holderActive := atomic.Bool{}
	holderActive.Store(true)
	unlockFirst, ok := a.LockDef(context.Background(), def)
	require.True(t, ok)

	attemptStarted := make(chan struct{})
	secondAcquired := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		close(attemptStarted)
		inner, got := a.LockDef(context.Background(), def)
		if !got {
			t.Error("second acquisition failed (busy/cancelled) — exclusion test broken")
			close(secondAcquired)
			return
		}
		// IMMEDIATELY on acquisition: sample the flag and publish. There is
		// deliberately NO channel to wait on here — a premature acquisition
		// (an exclusion bug) would observe firstActive==true while the
		// holder still holds, and the main goroutine's pre-release window
		// would see secondAcquired fire.
		firstStillActive := holderActive.Load()
		close(secondAcquired)
		if firstStillActive {
			t.Error("second acquirer entered while the first holder was still active")
		}
		inner()
	}()
	<-attemptStarted
	// The registered attempt must NOT acquire while the holder is active.
	select {
	case <-secondAcquired:
		t.Fatal("same-key lock did not exclude a concurrent acquirer")
	case <-time.After(150 * time.Millisecond):
	}

	// Cancellation: a cancelled context answers false immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, got := a.LockDef(ctx, def)
	assert.False(t, got)

	// Publish the holder's end BEFORE releasing the key: from here on, a
	// correct waiter acquires and observes firstActive==false; a buggy
	// waiter that had acquired early already failed above.
	holderActive.Store(false)
	unlockFirst()
	select {
	case <-secondAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("second acquirer never got the key after release")
	}
	// Bounded join of the waiter's critical section.
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second acquirer never finished its critical section")
	}

	// Reclaim: zero references must remove the auxiliary entry.
	a.mu.Lock()
	keys := len(a.keys)
	a.mu.Unlock()
	require.Equal(t, 0, keys)

	// Distinct-key cap: hold RegistryCap distinct keys, then a further
	// distinct def must answer busy; freeing one slot lets it in; all
	// released, the map is empty again.
	unlocks := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		u, ok := a.LockDef(context.Background(), uuid.New())
		require.True(t, ok)
		unlocks = append(unlocks, u)
	}
	_, got = a.LockDef(context.Background(), uuid.New())
	assert.False(t, got, "distinct-key lock beyond the cap must answer busy")
	unlocks[0]()
	u, got := a.LockDef(context.Background(), uuid.New())
	require.True(t, got, "freeing one distinct key must admit a new def")
	u()
	for _, f := range unlocks[1:] {
		f()
	}
	a.mu.Lock()
	keys = len(a.keys)
	a.mu.Unlock()
	assert.Equal(t, 0, keys, "auxiliary key map must be fully reclaimed")
}

func TestScheduleAuthorizerDropOnlyExactGeneration(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4, MaintainEvery: time.Hour}, store, uuid.New())
	defer func() { require.NoError(t, a.Stop()) }()
	def := uuid.New()
	actor := uuid.New()
	later := a.now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	require.NoError(t, activateHelperErr(activateHelper(t, a, store, def, actor, 1, later)))

	// Dropping a DIFFERENT generation must not erase the active one.
	a.Drop(def, uuid.New())
	require.NotNil(t, a.ArmClaim(def))

	// Dropping the exact generation removes it.
	a.mu.Lock()
	active := a.arms[def].GrantID
	a.mu.Unlock()
	a.Drop(def, active)
	assert.Nil(t, a.ArmClaim(def))
}

// TestAbsoluteDeadlineReserveAnchorJWTMinMicrosecond exercises the real
// deadline path: the anchor is the RESERVE instant (before precheck/DB
// latency), the verified JWT exp clamps it down, the result is normalized
// DOWN to PostgreSQL microsecond precision, and later recomputation with a
// larger JWT can never extend it.
func TestAbsoluteDeadlineReserveAnchorJWTMinMicrosecond(t *testing.T) {
	store := newFakeAuthStore()
	a := NewScheduleAuthorizer(ScheduleAuthorizerConfig{RegistryCap: 4, MemoryTTL: 300 * time.Second, MaintainEvery: time.Hour}, store, uuid.New())
	defer func() { require.NoError(t, a.Stop()) }()

	// Reserve at a staged instant with sub-microsecond nanos.
	base := time.Date(2026, 11, 14, 12, 0, 0, 123456789, time.UTC) // 789ns tail
	a.now = func() time.Time { return base }
	ticket := a.ReserveArmSlot(uuid.New())
	require.NotNil(t, ticket)

	// 1. JWT exp clamps below the TTL anchor (µs-truncated, like every
	// persisted deadline).
	jwtSoon := base.Add(10 * time.Second)
	d := a.AbsoluteDeadline(ticket, jwtSoon)
	assert.True(t, d.Equal(jwtSoon.UTC().Truncate(time.Microsecond)), "the verified JWT exp must win the min()")

	// 2. A far JWT keeps the Reserve-anchored TTL…
	dFar := a.AbsoluteDeadline(ticket, base.Add(time.Hour))
	// …normalized DOWN to microsecond precision (789ns tail truncated).
	expected := base.Add(300 * time.Second).UTC().Truncate(time.Microsecond)
	assert.True(t, dFar.Equal(expected), "reserve anchor + TTL, truncated down to µs")
	assert.Zero(t, dFar.Nanosecond()%1000, "sub-microsecond remainder must be gone")

	// 3. No latency extension: recomputing later (precheck/DB time passed)
	// with an even larger JWT returns the identical, never-larger deadline.
	later := base.Add(5 * time.Second)
	a.now = func() time.Time { return later }
	dAgain := a.AbsoluteDeadline(ticket, later.Add(time.Hour))
	assert.True(t, dAgain.Equal(dFar), "the deadline is anchored at Reserve and never extends with elapsed time")
	assert.False(t, dAgain.After(dFar))

	// 4. The REAL activation cannot extend it either: build the material
	// with the ticket's original deadline, let precheck/DB "latency" pass
	// (a.now advanced well past the reserve anchor), run the actual
	// ActivateArm path, and require the live claim to carry the ORIGINAL
	// deadline to the microsecond.
	material := models.ScheduleGrantWrite{
		SyncDefID: ticket.defID, GrantID: uuid.New(), BootID: a.BootID(),
		ActorSubject: uuid.New(), OwnerSubject: uuid.New(),
		SourceID: uuid.New(), OutputDatasetID: uuid.New(),
		Revision: 1, ExpiresAt: dFar,
	}
	store.installView(ticket.defID, &models.ScheduleGrantView{
		GrantID: material.GrantID, ActorSubject: material.ActorSubject, BootID: material.BootID,
		Revision: 1, ExpiresAt: dFar, Live: true,
	})
	unlock, ok := a.LockDef(context.Background(), ticket.defID)
	require.True(t, ok)
	status, actErr := a.ActivateArm(ticket, material, "Bearer deadline")
	unlock()
	require.NoError(t, actErr)
	require.Equal(t, activateActivated, status)
	claim := a.ArmClaim(ticket.defID)
	require.NotNil(t, claim)
	assert.True(t, claim.ExpiresAt.Equal(dFar), "activation must persist the original reserve-anchored deadline exactly, never a later one")
}

func TestClampDeadlinesCap(t *testing.T) {
	t.Parallel()
	anchor := time.Now()
	auth := anchor.Add(300 * time.Second)
	task := anchor.Add(300 * time.Second)
	capAt := anchor.Add(2 * time.Second)

	// Zero cap: no-op.
	gotAuth, gotTask := clampDeadlines(auth, task, time.Time{})
	assert.Equal(t, auth, gotAuth)
	assert.Equal(t, task, gotTask)

	// Cap clamps BOTH the auth-memory expiry and the task deadline.
	gotAuth, gotTask = clampDeadlines(auth, task, capAt)
	assert.Equal(t, capAt, gotAuth)
	assert.Equal(t, capAt, gotTask)

	// Cap later than both: unchanged (never extends).
	gotAuth, gotTask = clampDeadlines(auth, task, anchor.Add(time.Hour))
	assert.Equal(t, auth, gotAuth)
	assert.Equal(t, task, gotTask)

	// Queue wait consumes the same budget: a cap already in the past clamps
	// both into the past, which executeTask's deadline checks turn into the
	// honest pre-execution close.
	past := anchor.Add(-time.Second)
	gotAuth, gotTask = clampDeadlines(auth, task, past)
	assert.Equal(t, past, gotAuth)
	assert.Equal(t, past, gotTask)
}
