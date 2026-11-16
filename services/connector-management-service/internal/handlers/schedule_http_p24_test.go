// P2.4 HTTP surface tests: arm/revoke/ticks permission chains and statuses,
// invalid_schedule stability, and the legacy-cron PATCH compatibility rule.
package handlers_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// p24AuthFake implements BOTH handlers.ScheduleGrantStore and
// handlers.ScheduleAuthStore over one shared view map, so the handler's DB
// arm result and the authorizer's activation read observe the same state.
// GetScheduleGrantView can be paused at a one-shot deterministic barrier
// (pauseNextGetView) for concurrency regressions against the REAL handlers.
type p24AuthFake struct {
	mu      sync.Mutex
	views   map[uuid.UUID]*models.ScheduleGrantView
	armErr  error
	armFn   func(in models.ScheduleGrantWrite) (*models.ScheduleGrantView, error)
	armSeen []models.ScheduleGrantWrite
	exact   []uuid.UUID
	ticks   []models.ScheduleTick

	getEnter   chan struct{}
	getRelease chan struct{}
}

func newP24AuthFake() *p24AuthFake {
	return &p24AuthFake{views: map[uuid.UUID]*models.ScheduleGrantView{}}
}

func (f *p24AuthFake) installView(defID uuid.UUID, view *models.ScheduleGrantView) {
	f.mu.Lock()
	f.views[defID] = view
	f.mu.Unlock()
}

// pauseNextGetView parks the NEXT GetScheduleGrantView call until the
// returned release channel closes; the entered channel closes on entry.
func (f *p24AuthFake) pauseNextGetView() (entered, release chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entered, release = make(chan struct{}), make(chan struct{})
	f.getEnter, f.getRelease = entered, release
	return entered, release
}

func (f *p24AuthFake) ArmScheduleGrant(_ context.Context, _ uuid.UUID, _ repo.Tenant, in models.ScheduleGrantWrite, _ int, _ uuid.UUID) (*models.ScheduleGrantView, error) {
	f.mu.Lock()
	f.armSeen = append(f.armSeen, in)
	fn, armErr := f.armFn, f.armErr
	f.mu.Unlock()
	if armErr != nil {
		return nil, armErr
	}
	if fn != nil {
		return fn(in)
	}
	view := &models.ScheduleGrantView{
		GrantID: in.GrantID, ActorSubject: in.ActorSubject, BootID: in.BootID,
		Revision: in.Revision, ExpiresAt: in.ExpiresAt, Live: true,
	}
	f.mu.Lock()
	f.views[in.SyncDefID] = view
	f.mu.Unlock()
	return view, nil
}

// RevokeScheduleGrant mirrors the real DB DELETE ... RETURNING grant_id: it
// removes the def's current row and returns its generation id (nil when no
// row existed), so the handler's exact-Drop path runs against real data.
func (f *p24AuthFake) RevokeScheduleGrant(_ context.Context, defID uuid.UUID, _ repo.Tenant) (*uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	view, ok := f.views[defID]
	if !ok {
		return nil, nil
	}
	delete(f.views, defID)
	id := view.GrantID
	return &id, nil
}

func (f *p24AuthFake) DeleteScheduleGrantExact(_ context.Context, defID, grantID uuid.UUID, _ repo.Tenant) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exact = append(f.exact, grantID)
	return true, nil
}

func (f *p24AuthFake) GetScheduleGrantView(_ context.Context, defID uuid.UUID, _ repo.Tenant) (*models.ScheduleGrantView, error) {
	f.mu.Lock()
	enter, release := f.getEnter, f.getRelease
	view, ok := f.views[defID]
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
		}
	}
	if !ok {
		return nil, nil
	}
	copied := *view
	return &copied, nil
}

func (f *p24AuthFake) ListScheduleTicks(_ context.Context, defID uuid.UUID, _ repo.Tenant, limit int) ([]models.ScheduleTick, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]models.ScheduleTick, 0, limit)
	for i := range f.ticks {
		if f.ticks[i].SyncDefID == defID {
			out = append(out, f.ticks[i])
		}
	}
	return out, nil
}

func (f *p24AuthFake) HeartbeatScheduleGrant(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int, time.Duration) (bool, error) {
	return true, nil
}

func authedClaimsReq(method, target, body string, claims *authmw.Claims) *http.Request {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	return req.WithContext(authmw.ContextWithClaims(context.Background(), claims))
}

// orgSetup builds an org-scoped owner, a member with no grants, and a wired
// def on an org source.
type orgSetup struct {
	org      uuid.UUID
	owner    uuid.UUID
	member   uuid.UUID
	store    *fakeStore
	h        *handlers.Handlers
	sourceID uuid.UUID
	defID    uuid.UUID
	grants   *p24AuthFake
}

func newOrgSetup(t *testing.T) *orgSetup {
	t.Helper()
	s := &orgSetup{org: uuid.New(), owner: uuid.New(), member: uuid.New()}
	s.store = newFakeStore(s.owner)
	s.store.connections[0].OrgID = &s.org
	s.grants = newP24AuthFake()
	s.h = &handlers.Handlers{
		Repo:                s.store,
		ScheduleGrants:      s.grants,
		ScheduleAuthorizer:  handlers.NewScheduleAuthorizer(handlers.ScheduleAuthorizerConfig{MaintainEvery: time.Hour}, s.grants, uuid.New()),
	}
	t.Cleanup(func() {
		if s.h.ScheduleAuthorizer != nil {
			_ = s.h.ScheduleAuthorizer.Stop()
		}
	})
	s.sourceID = s.store.connections[0].ID
	s.defID = uuid.New()
	s.store.syncJobs[s.sourceID] = []models.SyncJob{{
		ID: s.defID, SourceID: s.sourceID, OutputDatasetID: uuid.New(),
		ScheduleTimeZone: "UTC", ScheduleEnabled: false, ScheduleRevision: 1,
		OrgID: &s.org, CreatedAt: time.Now().UTC(),
	}}
	return s
}

func (s *orgSetup) req(claims *authmw.Claims, method, path string) *httptest.ResponseRecorder {
	req := authedClaimsReq(method, path, "", claims)
	req = withRouteParam(req, "sync_id", s.defID.String())
	rec := httptest.NewRecorder()
	switch method {
	case http.MethodPost:
		s.h.ArmScheduleAuthorization(rec, req)
	case http.MethodDelete:
		s.h.RevokeScheduleAuthorization(rec, req)
	default:
		s.h.ListScheduleTicks(rec, req)
	}
	return rec
}

func ownerClaims(sub, org uuid.UUID) *authmw.Claims {
	return &authmw.Claims{Sub: sub, OrgID: &org, EXP: time.Now().Add(time.Hour).Unix()}
}

func TestP24ArmPermissionChainAndStatuses(t *testing.T) {
	t.Run("unauthenticated is 401", func(t *testing.T) {
		h := &handlers.Handlers{}
		rec := httptest.NewRecorder()
		h.ArmScheduleAuthorization(rec, httptest.NewRequest(http.MethodPost, "/x", nil))
		assert.Equal(t, 401, rec.Code)
	})
	t.Run("unknown def is 404", func(t *testing.T) {
		s := newOrgSetup(t)
		req := authedClaimsReq(http.MethodPost, "/x", "", ownerClaims(s.owner, s.org))
		req = withRouteParam(req, "sync_id", uuid.New().String())
		rec := httptest.NewRecorder()
		s.h.ArmScheduleAuthorization(rec, req)
		assert.Equal(t, 404, rec.Code)
	})
	t.Run("org member without write grant is 403", func(t *testing.T) {
		s := newOrgSetup(t)
		rec := s.req(&authmw.Claims{Sub: s.member, OrgID: &s.org}, http.MethodPost, "/x")
		assert.Equal(t, 403, rec.Code)
	})
	t.Run("unwired schedule surface is 503", func(t *testing.T) {
		owner := uuid.New()
		store := newFakeStore(owner)
		h := &handlers.Handlers{Repo: store}
		def := uuid.New()
		store.syncJobs[store.connections[0].ID] = []models.SyncJob{{ID: def, SourceID: store.connections[0].ID, OutputDatasetID: uuid.New(), ScheduleRevision: 1, CreatedAt: time.Now().UTC()}}
		req := authedClaimsReq(http.MethodPost, "/x", "", &authmw.Claims{Sub: owner})
		req = withRouteParam(req, "sync_id", def.String())
		rec := httptest.NewRecorder()
		h.ArmScheduleAuthorization(rec, req)
		assert.Equal(t, 503, rec.Code)
	})
}

func TestP24ArmHappyPathAndTypedFailures(t *testing.T) {
	t.Run("happy path arms with per-instance metadata", func(t *testing.T) {
		s := newOrgSetup(t)
		// Output precheck must succeed (owner identity forwards).
		s.h.DatasetVersioning = &fakeCommitPort{}
		req := authedClaimsReq(http.MethodPost, "/x", "", ownerClaims(s.owner, s.org))
		req.Header.Set("Authorization", "Bearer test-token")
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.ArmScheduleAuthorization(rec, req)
		require.Equal(t, 200, rec.Code)
		var body struct {
			Armed                bool      `json:"armed"`
			GrantID              uuid.UUID `json:"grant_id"`
			ActorSubject         uuid.UUID `json:"actor_subject"`
			ScheduleRevision     int       `json:"schedule_revision"`
			AuthorizationExpires time.Time `json:"authorization_expires_at"`
			SchedulerLoopRunning bool      `json:"scheduler_loop_running"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.True(t, body.Armed)
		assert.Equal(t, s.owner, body.ActorSubject)
		assert.Equal(t, 1, body.ScheduleRevision)
		assert.False(t, body.SchedulerLoopRunning, "no runner wired ⇒ no false execution promise")
		// The local generation is installed and executable.
		require.NotNil(t, s.h.ScheduleAuthorizer.ArmClaim(s.defID))
		// The Authorization never appears anywhere in the response.
		assert.NotContains(t, rec.Body.String(), "test-token")
	})
	t.Run("target changed is 409 with clean retry semantics", func(t *testing.T) {
		s := newOrgSetup(t)
		s.h.DatasetVersioning = &fakeCommitPort{}
		s.grants.mu.Lock()
		s.grants.armErr = repo.ErrScheduleArmTargetChanged
		s.grants.mu.Unlock()
		req := authedClaimsReq(http.MethodPost, "/x", "", ownerClaims(s.owner, s.org))
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.ArmScheduleAuthorization(rec, req)
		assert.Equal(t, 409, rec.Code)
		assert.Nil(t, s.h.ScheduleAuthorizer.ArmClaim(s.defID))
	})
	t.Run("expired input is 400", func(t *testing.T) {
		s := newOrgSetup(t)
		s.h.DatasetVersioning = &fakeCommitPort{}
		s.grants.mu.Lock()
		s.grants.armErr = repo.ErrScheduleArmExpired
		s.grants.mu.Unlock()
		req := authedClaimsReq(http.MethodPost, "/x", "", ownerClaims(s.owner, s.org))
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.ArmScheduleAuthorization(rec, req)
		assert.Equal(t, 400, rec.Code)
	})
	t.Run("superseded activation cleans its own generation", func(t *testing.T) {
		s := newOrgSetup(t)
		s.h.DatasetVersioning = &fakeCommitPort{}
		// The DB arm succeeds (returning the matching view WITHOUT installing
		// it), but the activation read observes a LATER grant: activation must
		// answer 409 and delete exactly its own committed-but-unactivated
		// generation.
		s.grants.mu.Lock()
		s.grants.armFn = func(in models.ScheduleGrantWrite) (*models.ScheduleGrantView, error) {
			return &models.ScheduleGrantView{
				GrantID: in.GrantID, ActorSubject: in.ActorSubject, BootID: in.BootID,
				Revision: in.Revision, ExpiresAt: in.ExpiresAt, Live: true,
			}, nil
		}
		// The activation read sees a different (newer) grant row.
		s.grants.views[s.defID] = &models.ScheduleGrantView{GrantID: uuid.New(), Live: true, ExpiresAt: time.Now().Add(time.Hour)}
		s.grants.mu.Unlock()
		req := authedClaimsReq(http.MethodPost, "/x", "", ownerClaims(s.owner, s.org))
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.ArmScheduleAuthorization(rec, req)
		assert.Equal(t, 409, rec.Code)
		s.grants.mu.Lock()
		exact := len(s.grants.exact)
		s.grants.mu.Unlock()
		assert.Equal(t, 1, exact, "the superseded arm must clean exactly its own generation")
		assert.Nil(t, s.h.ScheduleAuthorizer.ArmClaim(s.defID))
	})
}

func TestP24RevokeAndTicks(t *testing.T) {
	t.Run("revoke reports the actually deleted generation", func(t *testing.T) {
		s := newOrgSetup(t)
		// A live DB row exists (installed like a committed arm).
		gid := uuid.New()
		s.grants.installView(s.defID, &models.ScheduleGrantView{
			GrantID: gid, Live: true, ExpiresAt: time.Now().Add(time.Hour),
		})
		rec := s.req(ownerClaims(s.owner, s.org), http.MethodDelete, "/x")
		require.Equal(t, 200, rec.Code)
		assert.Contains(t, rec.Body.String(), `"revoked":true`)
		s.grants.mu.Lock()
		_, stillInDB := s.grants.views[s.defID]
		s.grants.mu.Unlock()
		assert.False(t, stillInDB, "the revoke must delete the DB row")
		// Idempotent second revoke (no row).
		rec = s.req(ownerClaims(s.owner, s.org), http.MethodDelete, "/x")
		assert.Equal(t, 200, rec.Code)
		assert.Contains(t, rec.Body.String(), `"revoked":false`)
	})
	t.Run("member without read grant is 403 on ticks", func(t *testing.T) {
		s := newOrgSetup(t)
		rec := s.req(&authmw.Claims{Sub: s.member, OrgID: &s.org}, http.MethodGet, "/x")
		assert.Equal(t, 403, rec.Code)
	})
	t.Run("owner lists ticks", func(t *testing.T) {
		s := newOrgSetup(t)
		s.grants.mu.Lock()
		s.grants.ticks = []models.ScheduleTick{{
			ID: uuid.New(), SyncDefID: s.defID, ScheduleRevision: 1,
			ScheduledFor: time.Now().UTC(), ClaimedAt: time.Now().UTC(),
			Outcome: string(repo.TickOutcomeAuthRequired), CreatedAt: time.Now().UTC(),
		}}
		s.grants.mu.Unlock()
		rec := s.req(ownerClaims(s.owner, s.org), http.MethodGet, "/x")
		require.Equal(t, 200, rec.Code)
		var ticks []models.ScheduleTick
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ticks))
		require.Len(t, ticks, 1)
		assert.Equal(t, "auth_required", ticks[0].Outcome)
	})
}

func TestP24InvalidScheduleStableCodeAndLegacyCompat(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store, DatasetVersioning: &fakeCommitPort{}}
	source := store.connections[0].ID

	t.Run("create with bad cron is 400 invalid_schedule", func(t *testing.T) {
		body := `{"source_id":"` + source.String() + `","output_dataset_id":"` + uuid.NewString() + `","schedule_cron":"*/5 * * *","schedule_enabled":true}`
		req := authedReq("POST", "/syncs", body, owner)
		rec := httptest.NewRecorder()
		h.CreateSyncJob(rec, req)
		require.Equal(t, 400, rec.Code)
		var parsed map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &parsed))
		assert.Equal(t, "invalid_schedule", parsed["error"])
		assert.Contains(t, parsed["detail"], "cron")
	})
	t.Run("create with Local timezone is 400 invalid_schedule", func(t *testing.T) {
		body := `{"source_id":"` + source.String() + `","output_dataset_id":"` + uuid.NewString() + `","schedule_cron":"*/5 * * * *","schedule_time_zone":"Local","schedule_enabled":true}`
		req := authedReq("POST", "/syncs", body, owner)
		rec := httptest.NewRecorder()
		h.CreateSyncJob(rec, req)
		require.Equal(t, 400, rec.Code)
		var parsed map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &parsed))
		assert.Equal(t, "invalid_schedule", parsed["error"])
	})
	t.Run("unrelated PATCH on a legacy invalid disabled cron survives", func(t *testing.T) {
		def := uuid.New()
		legacy := "not a cron"
		store.syncJobs[source] = append(store.syncJobs[source], models.SyncJob{
			ID: def, SourceID: source, OutputDatasetID: uuid.New(),
			ScheduleCron: &legacy, ScheduleTimeZone: "UTC", ScheduleEnabled: false,
			ScheduleRevision: 1, CreatedAt: time.Now().UTC(),
		})
		req := authedReq("PATCH", "/syncs/"+def.String(), `{"file_glob":"*.csv"}`, owner)
		req = withRouteParam(req, "sync_id", def.String())
		rec := httptest.NewRecorder()
		h.UpdateSyncJob(rec, req)
		assert.Equal(t, 200, rec.Code, "legacy untouched invalid disabled cron must survive unrelated patches")
	})
	t.Run("enabling a legacy invalid cron is 400 invalid_schedule", func(t *testing.T) {
		def := uuid.New()
		legacy := "not a cron"
		store.syncJobs[source] = append(store.syncJobs[source], models.SyncJob{
			ID: def, SourceID: source, OutputDatasetID: uuid.New(),
			ScheduleCron: &legacy, ScheduleTimeZone: "UTC", ScheduleEnabled: false,
			ScheduleRevision: 1, CreatedAt: time.Now().UTC(),
		})
		req := authedReq("PATCH", "/syncs/"+def.String(), `{"schedule_enabled":true}`, owner)
		req = withRouteParam(req, "sync_id", def.String())
		rec := httptest.NewRecorder()
		h.UpdateSyncJob(rec, req)
		assert.Equal(t, 400, rec.Code)
		var parsed map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &parsed))
		assert.Equal(t, "invalid_schedule", parsed["error"])
	})
}

func TestP24GetSyncJobScheduleProjectionSourceReadPermission(t *testing.T) {
	get := func(s *orgSetup, claims *authmw.Claims) *httptest.ResponseRecorder {
		req := authedClaimsReq(http.MethodGet, "/x", "", claims)
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.GetSyncJob(rec, req)
		return rec
	}

	t.Run("owner gets the projection", func(t *testing.T) {
		s := newOrgSetup(t)
		rec := get(s, ownerClaims(s.owner, s.org))
		require.Equal(t, 200, rec.Code)
		var job models.SyncJob
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &job))
		require.NotNil(t, job.ScheduleAuthorization, "wired surface must expose the projection")
		assert.False(t, job.ScheduleAuthorization.LocallyExecutable, "no local arm installed")
	})
	t.Run("same-org member with connections:read gets it", func(t *testing.T) {
		s := newOrgSetup(t)
		reader := &authmw.Claims{Sub: s.member, OrgID: &s.org, Permissions: []string{"connections:read"}}
		rec := get(s, reader)
		require.Equal(t, 200, rec.Code)
		assert.Contains(t, rec.Body.String(), "schedule_authorization")
	})
	t.Run("same-org member without read grant is 403", func(t *testing.T) {
		s := newOrgSetup(t)
		rec := get(s, &authmw.Claims{Sub: s.member, OrgID: &s.org})
		assert.Equal(t, 403, rec.Code, "grant metadata must not leak past canReadSource")
	})
	t.Run("cross-org caller is 404", func(t *testing.T) {
		s := newOrgSetup(t)
		otherOrg := uuid.New()
		rec := get(s, &authmw.Claims{Sub: uuid.New(), OrgID: &otherOrg, EXP: time.Now().Add(time.Hour).Unix()})
		assert.Equal(t, 404, rec.Code)
	})
	t.Run("unwired surface keeps pre-P2.4 tenant-only behavior", func(t *testing.T) {
		s := newOrgSetup(t)
		s.h.ScheduleGrants = nil
		s.h.ScheduleAuthorizer = nil
		rec := get(s, &authmw.Claims{Sub: s.member, OrgID: &s.org})
		assert.Equal(t, 200, rec.Code, "without the schedule surface the def read stays tenant-only")
		assert.NotContains(t, rec.Body.String(), "schedule_authorization")
	})
}

// TestP24RealArmRevokeSameKeyBarrier runs the REAL arm and revoke HTTP
// handlers concurrently with the activation read parked at a deterministic
// barrier: the revoke must wait on the same per-def key, the arm must
// complete 200, and the revoke must then delete the exact generation through
// the real Store path with the local registry entry dropped.
func TestP24RealArmRevokeSameKeyBarrier(t *testing.T) {
	s := newOrgSetup(t)
	s.h.DatasetVersioning = &fakeCommitPort{}
	entered, release := s.grants.pauseNextGetView()
	// release is one-shot: the failure paths and the cleanup can both fire it
	// without a double close, and every goroutine gets a bounded join.
	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }

	armDone := make(chan *httptest.ResponseRecorder, 1)
	revokeDone := make(chan *httptest.ResponseRecorder, 1)
	joinBounded := func() {
		doRelease()
		for _, ch := range []<-chan *httptest.ResponseRecorder{armDone, revokeDone} {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				return // the fake's own 10s fallback bounds the stragglers
			}
		}
	}
	t.Cleanup(joinBounded)

	go func() {
		req := authedClaimsReq(http.MethodPost, "/x", "", ownerClaims(s.owner, s.org))
		req.Header.Set("Authorization", "Bearer arm-token")
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.ArmScheduleAuthorization(rec, req)
		armDone <- rec
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("arm activation read never entered the barrier")
	}

	// Real concurrent revoke on the SAME def: it must NOT complete while the
	// arm sequence still holds the per-def key.
	go func() {
		req := authedClaimsReq(http.MethodDelete, "/x", "", ownerClaims(s.owner, s.org))
		req = withRouteParam(req, "sync_id", s.defID.String())
		rec := httptest.NewRecorder()
		s.h.RevokeScheduleAuthorization(rec, req)
		revokeDone <- rec
	}()
	select {
	case rec := <-revokeDone:
		t.Fatalf("revoke completed while the arm held the per-def key (status %d)", rec.Code)
	case <-time.After(150 * time.Millisecond):
	}

	// Release the arm: it must finish 200. The 200 itself proves the
	// activation succeeded (the handler answers armed=true only on the
	// activated status); no mid-flight registry sample — the serialized
	// revoke may legitimately have finished its exact delete by the time we
	// observe the arm's response, and that ordering is part of the contract.
	doRelease()
	select {
	case armRec := <-armDone:
		require.Equal(t, 200, armRec.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("arm handler never returned")
	}

	// The revoke serializes strictly behind the arm's key release: it now
	// completes with the exact-generation DB delete and the local drop.
	select {
	case revokeRec := <-revokeDone:
		require.Equal(t, 200, revokeRec.Code)
		assert.Contains(t, revokeRec.Body.String(), `"revoked":true`)
	case <-time.After(5 * time.Second):
		t.Fatal("revoke never completed after the arm released the key")
	}
	assert.Nil(t, s.h.ScheduleAuthorizer.ArmClaim(s.defID), "the exact generation must be dropped locally")
	s.grants.mu.Lock()
	_, stillInDB := s.grants.views[s.defID]
	s.grants.mu.Unlock()
	assert.False(t, stillInDB, "the DB row for the armed generation must be deleted")
}
