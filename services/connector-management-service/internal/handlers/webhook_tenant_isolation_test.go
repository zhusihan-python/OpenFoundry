package handlers_test

// WP2.1 webhook tenant isolation: InvokeWebhook and ListWebhookHistory must
// resolve the webhook through GetConnectionForTenant (the caller's tenant
// domain), so cross-tenant callers — admins and grant holders included — get
// a 404 before ANY side effect (outbound call, history write, rate-limit
// consumption), while same-tenant callers keep the owner/admin/permission
// authorization semantics. Personal (no-org) domains stay owner-only.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/observability"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/server"
)

type webhookTenantFixture struct {
	store     *fakeStore
	repo      *countingStore
	h         *handlers.Handlers
	upstream  *httptest.Server
	hits      *atomic.Int64
	orgA      uuid.UUID
	orgB      uuid.UUID
	ownerA    uuid.UUID // owns the org-A webhook
	memberA   uuid.UUID // plain org-A member (no grants)
	srcOrgA   uuid.UUID
	srcOrgB   uuid.UUID // webhook owned inside org B
	ownerB    uuid.UUID
	ownerP    uuid.UUID // personal-domain webhook owner (no org)
	srcPerson uuid.UUID
}

func (f *webhookTenantFixture) listCalls() int64 { return f.repo.listCalls.Load() }

// newWebhookTenantFixture wires a counting upstream plus three webhook
// sources: one in org A, one in org B, one in a personal (NULL-org) domain.
func newWebhookTenantFixture(t *testing.T, configTweak func(cfg map[string]any)) *webhookTenantFixture {
	t.Helper()
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"output_parameters":{"ok":true}}`))
	}))
	t.Cleanup(upstream.Close)

	webhookConfig := func(tweak func(cfg map[string]any)) json.RawMessage {
		cfg := map[string]any{
			"base_url": upstream.URL,
			"auth":     map[string]any{"type": "none"},
			"runtime": map[string]any{
				"worker":          "foundry",
				"allowed_methods": []string{"GET"},
			},
			"permissions": map[string]any{"invokable": true},
			"webhook": map[string]any{
				"method": "GET",
				"path":   "/v1/forecast",
				"history": map[string]any{
					"enabled":        true,
					"retention_days": 14,
					"store_outputs":  true,
				},
			},
		}
		if tweak != nil {
			tweak(cfg)
		}
		raw, err := json.Marshal(cfg)
		require.NoError(t, err)
		return raw
	}

	f := &webhookTenantFixture{
		upstream: upstream,
		hits:     &hits,
		orgA:     uuid.New(),
		orgB:     uuid.New(),
		ownerA:   uuid.New(),
		memberA:  uuid.New(),
		ownerB:   uuid.New(),
		ownerP:   uuid.New(),
	}
	orgA, orgB := f.orgA, f.orgB
	now := time.Now().UTC()
	f.store = &fakeStore{
		connections: []models.Connection{
			{ID: uuid.New(), Name: "org-a-webhook", ConnectorType: "rest_api", Config: webhookConfig(configTweak), Status: "connected", OwnerID: f.ownerA, OrgID: &orgA, CreatedAt: now, UpdatedAt: now},
			{ID: uuid.New(), Name: "org-b-webhook", ConnectorType: "rest_api", Config: webhookConfig(configTweak), Status: "connected", OwnerID: f.ownerB, OrgID: &orgB, CreatedAt: now, UpdatedAt: now},
			{ID: uuid.New(), Name: "personal-webhook", ConnectorType: "rest_api", Config: webhookConfig(configTweak), Status: "connected", OwnerID: f.ownerP, CreatedAt: now, UpdatedAt: now},
		},
		syncJobs: map[uuid.UUID][]models.SyncJob{}, mediaSyncs: map[uuid.UUID][]models.MediaSetSync{}, runs: map[uuid.UUID][]models.SyncRun{},
		links: map[string]models.VirtualTableSourceLink{}, vtables: map[string]models.VirtualTable{},
		registrations: map[uuid.UUID][]models.ConnectionRegistration{}, policies: map[uuid.UUID][]models.SourcePolicyBindingResponse{},
		agents: []models.ConnectorAgent{}, webhookHistory: map[uuid.UUID][]models.WebhookHistoryEntry{},
		listenerEvents: map[uuid.UUID][]models.InboundListenerEvent{}, credentials: map[uuid.UUID]map[string][]byte{},
		lease: map[uuid.UUID]*fakeLease{},
	}
	f.srcOrgA = f.store.connections[0].ID
	f.srcOrgB = f.store.connections[1].ID
	f.srcPerson = f.store.connections[2].ID
	f.repo = &countingStore{fakeStore: f.store}
	f.h = &handlers.Handlers{Repo: f.repo}
	return f
}

// countingStore wraps the fakeStore and counts ListWebhookHistory calls so
// tenant rejections can prove the lister was never reached (zero lister side
// effects — the 404 happens before the history query).
type countingStore struct {
	*fakeStore
	listCalls atomic.Int64
}

func (c *countingStore) ListWebhookHistory(ctx context.Context, sourceID uuid.UUID, limit int) ([]models.WebhookHistoryEntry, error) {
	c.listCalls.Add(1)
	return c.fakeStore.ListWebhookHistory(ctx, sourceID, limit)
}

func webhookClaims(sub uuid.UUID, org *uuid.UUID, roles []string, perms ...string) *authmw.Claims {
	return &authmw.Claims{Sub: sub, OrgID: org, Roles: roles, Permissions: perms}
}

func (f *webhookTenantFixture) invoke(t *testing.T, sourceID uuid.UUID, claims *authmw.Claims) *httptest.ResponseRecorder {
	t.Helper()
	req := withRouteParam(authedReq(http.MethodPost, "/webhooks/"+sourceID.String()+"/invoke", `{"inputs":{}}`, uuid.Nil), "id", sourceID.String())
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims)) // derive from req ctx: keep chi route params
	rec := httptest.NewRecorder()
	f.h.InvokeWebhook(rec, req)
	return rec
}

func (f *webhookTenantFixture) history(t *testing.T, sourceID uuid.UUID, claims *authmw.Claims, param string) *httptest.ResponseRecorder {
	t.Helper()
	req := withRouteParam(authedReq(http.MethodGet, "/webhooks/"+sourceID.String()+"/history", ``, uuid.Nil), param, sourceID.String())
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims)) // derive from req ctx: keep chi route params
	rec := httptest.NewRecorder()
	f.h.ListWebhookHistory(rec, req)
	return rec
}

func (f *webhookTenantFixture) historyCount(sourceID uuid.UUID) int {
	items, _ := f.store.ListWebhookHistory(context.Background(), sourceID, 1000)
	return len(items)
}

func TestInvokeWebhookTenantIsolation(t *testing.T) {
	t.Parallel()
	f := newWebhookTenantFixture(t, nil)
	orgA, orgB := f.orgA, f.orgB

	cases := []struct {
		name         string
		source       uuid.UUID
		claims       *authmw.Claims
		wantStatus   int
		wantUpstream int64 // upstream hits delta
		wantHistory  int   // history rows delta
	}{
		// Same-tenant positive paths keep the original grants.
		{name: "org owner invokes", source: f.srcOrgA, claims: webhookClaims(f.ownerA, &orgA, nil), wantStatus: http.StatusOK, wantUpstream: 1, wantHistory: 1},
		{name: "org admin invokes", source: f.srcOrgA, claims: webhookClaims(uuid.New(), &orgA, []string{"admin"}), wantStatus: http.StatusOK, wantUpstream: 1, wantHistory: 1},
		{name: "org member with connections:write", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil, "connections:write"), wantStatus: http.StatusOK, wantUpstream: 1, wantHistory: 1},
		{name: "org member with webhooks:invoke", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil, "webhooks:invoke"), wantStatus: http.StatusOK, wantUpstream: 1, wantHistory: 1},
		// Same tenant, insufficient grants → 403 (authorization, not existence).
		{name: "org member without grants", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil), wantStatus: http.StatusForbidden, wantUpstream: 0, wantHistory: 0},
		{name: "org member with read-only grant", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil, "connections:read"), wantStatus: http.StatusForbidden, wantUpstream: 0, wantHistory: 0},
		// Cross-tenant callers — even admins and grant holders — get 404 with
		// zero side effects: no outbound call, no history row.
		{name: "cross-org admin", source: f.srcOrgA, claims: webhookClaims(uuid.New(), &orgB, []string{"admin"}), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		{name: "cross-org holder of both grants", source: f.srcOrgA, claims: webhookClaims(uuid.New(), &orgB, nil, "connections:write", "webhooks:invoke"), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		{name: "same subject switched to org B", source: f.srcOrgA, claims: webhookClaims(f.ownerA, &orgB, nil), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		{name: "personal caller against org webhook", source: f.srcOrgA, claims: webhookClaims(f.ownerP, nil, []string{"admin"}), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		{name: "org caller against personal webhook", source: f.srcPerson, claims: webhookClaims(f.ownerA, &orgA, []string{"admin"}), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		{name: "other personal owner", source: f.srcPerson, claims: webhookClaims(uuid.New(), nil, []string{"admin"}), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		{name: "org B owner against org A webhook", source: f.srcOrgA, claims: webhookClaims(f.ownerB, &orgB, nil), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
		// Personal domain positive path stays owner-only.
		{name: "personal owner invokes own webhook", source: f.srcPerson, claims: webhookClaims(f.ownerP, nil, nil), wantStatus: http.StatusOK, wantUpstream: 1, wantHistory: 1},
		// Unknown webhook id is the same 404 as cross-tenant (no existence leak).
		{name: "unknown webhook id", source: uuid.New(), claims: webhookClaims(f.ownerA, &orgA, nil), wantStatus: http.StatusNotFound, wantUpstream: 0, wantHistory: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hitsBefore := f.hits.Load()
			historyBefore := f.historyCount(tc.source)
			rec := f.invoke(t, tc.source, tc.claims)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			require.Equal(t, tc.wantUpstream, f.hits.Load()-hitsBefore, "upstream hit delta")
			require.Equal(t, tc.wantHistory, f.historyCount(tc.source)-historyBefore, "history row delta")
		})
	}
}

func TestListWebhookHistoryTenantIsolation(t *testing.T) {
	t.Parallel()
	f := newWebhookTenantFixture(t, nil)
	orgA, orgB := f.orgA, f.orgB

	// Seed a REAL history row per source via a legitimate owner invoke (full
	// handler path: outbound call + history write). The caller's UserID is the
	// unique marker the content assertions below match against — a source with
	// no history can never satisfy them, so empty-data false positives fail.
	seedMarker := map[uuid.UUID]uuid.UUID{}
	for _, seed := range []struct {
		source uuid.UUID
		owner  uuid.UUID
		org    *uuid.UUID
	}{
		{f.srcOrgA, f.ownerA, &orgA}, {f.srcOrgB, f.ownerB, &orgB}, {f.srcPerson, f.ownerP, nil},
	} {
		hitsBefore := f.hits.Load()
		rec := f.invoke(t, seed.source, webhookClaims(seed.owner, seed.org, nil))
		require.Equal(t, http.StatusOK, rec.Code, "seed invoke for %s: %s", seed.source, rec.Body.String())
		require.Equal(t, int64(1), f.hits.Load()-hitsBefore, "seed invoke must hit upstream once")
		seeded, err := f.store.ListWebhookHistory(context.Background(), seed.source, 10)
		require.NoError(t, err)
		require.Len(t, seeded, 1, "seed invoke must persist exactly one history row")
		require.Equal(t, seed.source, seeded[0].SourceID)
		require.Equal(t, seed.owner, seeded[0].UserID, "unique marker (caller UserID)")
		require.Equal(t, "succeeded", seeded[0].Status)
		seedMarker[seed.source] = seed.owner
	}

	cases := []struct {
		name       string
		source     uuid.UUID
		claims     *authmw.Claims
		param      string
		wantStatus int
	}{
		// Same-tenant reads keep the original grant surface.
		{name: "org owner reads history", source: f.srcOrgA, claims: webhookClaims(f.ownerA, &orgA, nil), param: "id", wantStatus: http.StatusOK},
		{name: "org admin reads history", source: f.srcOrgA, claims: webhookClaims(uuid.New(), &orgA, []string{"admin"}), param: "id", wantStatus: http.StatusOK},
		{name: "org member with connections:read", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil, "connections:read"), param: "id", wantStatus: http.StatusOK},
		{name: "org member with webhooks:read", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil, "webhooks:read"), param: "id", wantStatus: http.StatusOK},
		{name: "org member without grants", source: f.srcOrgA, claims: webhookClaims(f.memberA, &orgA, nil), param: "id", wantStatus: http.StatusForbidden},
		// Cross-tenant reads are 404 regardless of role or grants.
		{name: "cross-org admin", source: f.srcOrgA, claims: webhookClaims(uuid.New(), &orgB, []string{"admin"}), param: "id", wantStatus: http.StatusNotFound},
		{name: "cross-org reader grants", source: f.srcOrgA, claims: webhookClaims(uuid.New(), &orgB, nil, "connections:read", "webhooks:read"), param: "id", wantStatus: http.StatusNotFound},
		{name: "same subject switched org", source: f.srcOrgA, claims: webhookClaims(f.ownerA, &orgB, nil), param: "id", wantStatus: http.StatusNotFound},
		{name: "personal cross-owner read", source: f.srcPerson, claims: webhookClaims(uuid.New(), nil, []string{"admin"}), param: "id", wantStatus: http.StatusNotFound},
		{name: "personal owner reads own history", source: f.srcPerson, claims: webhookClaims(f.ownerP, nil, nil), param: "id", wantStatus: http.StatusOK},
		{name: "unknown webhook id", source: uuid.New(), claims: webhookClaims(f.ownerA, &orgA, nil), param: "id", wantStatus: http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			listCallsBefore := f.listCalls()
			rowsBefore := f.historyCount(tc.source)
			rec := f.history(t, tc.source, tc.claims, tc.param)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())

			if tc.wantStatus == http.StatusOK {
				// Positive reads must return the SEEDED row's content, not an
				// empty 200: non-empty, right source, right marker caller.
				var list models.ListResponse[models.WebhookHistoryEntry]
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
				require.NotEmpty(t, list.Items, "history read must return the seeded rows")
				marker := seedMarker[tc.source]
				found := false
				for _, item := range list.Items {
					require.Equal(t, tc.source, item.SourceID, "no cross-source rows may leak into the response")
					if item.UserID == marker {
						found = true
					}
				}
				require.True(t, found, "seeded marker row (UserID %s) must be present", marker)
			} else {
				// Rejections: no lister call, no history mutation, and the
				// response body must not echo the source (no content leak).
				require.Equal(t, listCallsBefore, f.listCalls(), "rejected read must not reach the history lister")
				require.Equal(t, rowsBefore, f.historyCount(tc.source), "rejected read must not write history")
				if tc.wantStatus == http.StatusNotFound {
					require.NotContains(t, rec.Body.String(), tc.source.String(), "404 body must not leak the source id")
				}
			}
		})
	}
}

// The webhook-history URL aliases must stay isolated when driven through the
// REAL server router with SIGNED JWTs (authmw middleware verification), not
// just via handler-direct calls with injected claims.
func TestWebhookHistoryAliasesThroughRealServerJWT(t *testing.T) {
	t.Parallel()
	f := newWebhookTenantFixture(t, nil)
	orgA, orgB := f.orgA, f.orgB

	jwtCfg := authmw.NewJWTConfig("s1sec-unit-synthetic-jwt-secret") // synthetic, never real material
	cfg := &config.Config{}
	cfg.Service.Name = "connector-management-service"
	cfg.Service.Version = "test"
	srv := server.New(cfg, jwtCfg, f.h, observability.NewMetrics())
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()
	client := ts.Client()
	client.Timeout = 15 * time.Second

	mint := func(sub uuid.UUID, org *uuid.UUID, roles []string, perms ...string) string {
		claims := authmw.BuildAccessClaims(jwtCfg, authmw.AccessClaimsInput{
			UserID: sub, Email: "s1sec-unit@example.test", Name: "s1sec-unit",
			Roles: roles, Permissions: perms, OrgID: org,
		})
		tok, err := authmw.EncodeToken(jwtCfg, &claims)
		require.NoError(t, err)
		return "Bearer " + tok
	}
	do := func(method, path, auth string, body io.Reader) (*http.Response, string) {
		req, err := http.NewRequest(method, ts.URL+path, body)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		require.NoError(t, err)
		_ = resp.Body.Close()
		return resp, string(raw)
	}

	// Seed org-A history through the real invoke route with a signed JWT.
	resp, body := do(http.MethodPost, "/api/v1/webhooks/"+f.srcOrgA.String()+"/invoke",
		mint(f.ownerA, &orgA, nil), strings.NewReader(`{"inputs":{}}`))
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	primary := "/api/v1/webhooks/" + f.srcOrgA.String() + "/history"
	alias := "/api/v1/data-connection/sources/" + f.srcOrgA.String() + "/webhook-history"

	for _, route := range []struct{ name, path string }{
		{"primary route", primary}, {"source_id alias route", alias},
	} {
		t.Run(route.name+" owner sees seeded content", func(t *testing.T) {
			resp, body := do(http.MethodGet, route.path, mint(f.ownerA, &orgA, nil), nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, body)
			var list models.ListResponse[models.WebhookHistoryEntry]
			require.NoError(t, json.Unmarshal([]byte(body), &list))
			require.NotEmpty(t, list.Items, "history read must return the seeded rows")
			markerSeen := false
			for _, item := range list.Items {
				require.Equal(t, f.srcOrgA, item.SourceID, "no cross-source rows may leak")
				if item.UserID == f.ownerA {
					markerSeen = true
				}
			}
			require.True(t, markerSeen, "seeded marker row must be present")
		})

		t.Run(route.name+" cross-org admin 404 without leak", func(t *testing.T) {
			before := f.listCalls()
			resp, body := do(http.MethodGet, route.path, mint(uuid.New(), &orgB, []string{"admin"}), nil)
			require.Equal(t, http.StatusNotFound, resp.StatusCode, body)
			require.NotContains(t, body, f.srcOrgA.String(), "404 body must not leak the source id")
			require.Equal(t, before, f.listCalls(), "rejected read must not reach the history lister")
		})

		t.Run(route.name+" unsigned token 401", func(t *testing.T) {
			resp, _ := do(http.MethodGet, route.path, "Bearer not-a-jwt", nil)
			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		})
	}
}

// Rejections must happen BEFORE the rate limiter counts the request: a
// cross-tenant 404 consumes no invocation budget, so the next in-tenant
// caller is not throttled by an attacker's probing.
func TestInvokeWebhookRejectionDoesNotConsumeRateLimit(t *testing.T) {
	f := newWebhookTenantFixture(t, func(cfg map[string]any) {
		cfg["webhook"].(map[string]any)["rate_limit"] = map[string]any{"max_requests": 1, "per_seconds": 60}
	})
	orgA, orgB := f.orgA, f.orgB

	// Flood of cross-tenant attempts: all 404, no upstream calls.
	for i := 0; i < 5; i++ {
		rec := f.invoke(t, f.srcOrgA, webhookClaims(uuid.New(), &orgB, []string{"admin"}))
		require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	}
	require.Zero(t, f.hits.Load())

	// The single-request budget is still intact for the rightful tenant.
	rec := f.invoke(t, f.srcOrgA, webhookClaims(f.ownerA, &orgA, nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.EqualValues(t, 1, f.hits.Load())

	// Second in-tenant invoke is throttled (budget consumed by the legit call).
	rec = f.invoke(t, f.srcOrgA, webhookClaims(f.ownerA, &orgA, nil))
	require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	require.EqualValues(t, 1, f.hits.Load())
}

// InvokeWebhook must answer 503 (route pending) instead of panicking when no
// repository is configured — the same behavior ListWebhookHistory already had.
func TestInvokeWebhookNilRepoIsPendingNotPanic(t *testing.T) {
	h := &handlers.Handlers{}
	req := withRouteParam(authedReq(http.MethodPost, "/webhooks/"+uuid.NewString()+"/invoke", `{}`, uuid.New()), "id", uuid.NewString())
	rec := httptest.NewRecorder()
	require.NotPanics(t, func() { h.InvokeWebhook(rec, req) })
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
}
