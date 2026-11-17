package handlers_test

// S1 security acceptance (WP2.1): real-HTTP webhook tenant isolation.
// Unlike the fakeStore unit tests, this exercises the full server router
// (chi routes + authmw JWT verification) against a REAL PostgreSQL-backed
// repo.Repo. It is environment-gated on S1SEC_PG_DSN and orchestrated by
// tools/smoke/data-integration-s1-security/run.sh against a disposable,
// isolated PG16 instance — the DSN is always synthetic test data there.
// Rejection cases assert ZERO outbound upstream hits; tenant rejections
// additionally assert ZERO new webhook_history rows in PostgreSQL, so no
// side effect (outbound call, history write, rate-limit consumption) can
// precede the tenant rejection.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/observability"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/server"
)

type s1secCaseResult struct {
	Name         string `json:"name"`
	WantStatus   int    `json:"want_status"`
	GotStatus    int    `json:"got_status"`
	WantUpstream int64  `json:"want_upstream"`
	GotUpstream  int64  `json:"got_upstream"`
	WantHistory  int    `json:"want_history"`
	GotHistory   int    `json:"got_history"`
	Route        string `json:"route"`
	Auth         string `json:"auth"`
	Pass         bool   `json:"pass"`
}

type s1secReport struct {
	RunID       string            `json:"run_id"`
	UpstreamURL string            `json:"upstream_url"`
	Cases       []s1secCaseResult `json:"cases"`
	AllPassed   bool              `json:"all_passed"`
}

func TestS1SecWebhookTenantIsolationHTTPAgainstPG(t *testing.T) {
	dsn := os.Getenv("S1SEC_PG_DSN")
	if dsn == "" {
		t.Skip("S1SEC_PG_DSN not set — run via tools/smoke/data-integration-s1-security/run.sh (isolated PG16)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "connect isolated PG16")
	defer pool.Close()
	require.NoError(t, repo.Migrate(ctx, pool), "run embedded migrations")
	store := &repo.Repo{Pool: pool}

	// Controlled loopback upstream fixture; counts every inbound hit.
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		_, _ = w.Write([]byte(`{"output_parameters":{"ok":true}}`))
	}))
	defer upstream.Close()

	mkConn := func(name string, rateLimited bool, tnt repo.Tenant) uuid.UUID {
		webhook := map[string]any{
			"method": "GET",
			"path":   "/v1/forecast",
			"history": map[string]any{
				"enabled": true, "retention_days": 14, "store_outputs": true,
			},
		}
		if rateLimited {
			webhook["rate_limit"] = map[string]any{"max_requests": 1, "per_seconds": 600}
		}
		cfg := map[string]any{
			"base_url":    upstream.URL,
			"auth":        map[string]any{"type": "none"},
			"runtime":     map[string]any{"worker": "foundry", "allowed_methods": []string{"GET"}},
			"permissions": map[string]any{"invokable": true},
			"webhook":     webhook,
		}
		raw, err := json.Marshal(cfg)
		require.NoError(t, err)
		created, err := store.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: name, ConnectorType: "rest_api", Config: raw,
		}, tnt)
		require.NoError(t, err)
		return created.ID
	}

	orgA, orgB := uuid.New(), uuid.New()
	ownerA, memberA, ownerP := uuid.New(), uuid.New(), uuid.New()
	srcOrgA := mkConn("org-a-webhook", false, repo.Tenant{OwnerID: ownerA, OrgID: &orgA})
	srcOrgARL := mkConn("org-a-webhook-ratelimited", true, repo.Tenant{OwnerID: ownerA, OrgID: &orgA})
	ownerB := uuid.New()
	srcOrgB := mkConn("org-b-webhook", false, repo.Tenant{OwnerID: ownerB, OrgID: &orgB})
	srcPerson := mkConn("personal-webhook", false, repo.Tenant{OwnerID: ownerP})

	// Full server router with real JWT middleware + real PG repo.
	jwtCfg := authmw.NewJWTConfig("s1sec-synthetic-jwt-secret") // synthetic harness secret, never real env material
	cfg := &config.Config{}
	cfg.Service.Name = "connector-management-service"
	cfg.Service.Version = "s1sec-acceptance"
	srv := server.New(cfg, jwtCfg, &handlers.Handlers{Repo: store}, observability.NewMetrics())
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()
	client := ts.Client()
	client.Timeout = 30 * time.Second

	type who struct {
		sub  uuid.UUID
		org  *uuid.UUID
		perm []string
		role []string
	}
	mint := func(w who) string {
		claims := authmw.BuildAccessClaims(jwtCfg, authmw.AccessClaimsInput{
			UserID: w.sub, Email: "s1sec@example.test", Name: "s1sec",
			Roles: w.role, Permissions: w.perm, OrgID: w.org,
		})
		tok, err := authmw.EncodeToken(jwtCfg, &claims)
		require.NoError(t, err)
		return "Bearer " + tok
	}
	orgPtr := func(o uuid.UUID) *uuid.UUID { return &o }

	// Seed one REAL history row in each foreign domain (org B + personal) via
	// a legitimate owner invoke, so the later org-A history reads must prove
	// those rows never leak into an org-A response.
	seedOwnerInvoke := func(src, user uuid.UUID, org *uuid.UUID) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/v1/webhooks/"+src.String()+"/invoke", strings.NewReader(`{"inputs":{}}`))
		require.NoError(t, err)
		req.Header.Set("Authorization", mint(who{sub: user, org: org}))
		resp, err := client.Do(req)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "seed invoke for %s", src)
	}
	seedOwnerInvoke(srcOrgA, ownerA, orgPtr(orgA))
	seedOwnerInvoke(srcOrgB, ownerB, orgPtr(orgB))
	seedOwnerInvoke(srcPerson, ownerP, nil)

	historyCount := func(source uuid.UUID) int {
		items, err := store.ListWebhookHistory(ctx, source, 1000)
		require.NoError(t, err)
		return len(items)
	}
	// Every source the rejection matrix reads must ALREADY hold seeded rows —
	// a cross-tenant 404/403 read of a source with real history proves the
	// isolation hides existing content (an empty source could never
	// distinguish "isolated" from "nothing to leak").
	for _, src := range []uuid.UUID{srcOrgA, srcOrgB, srcPerson} {
		require.Positive(t, historyCount(src), "source %s must be seeded before the rejection matrix", src)
	}

	report := s1secReport{RunID: os.Getenv("S1SEC_RUN_ID"), UpstreamURL: "loopback-fixture (redacted)"}
	record := func(res s1secCaseResult, detail string) {
		report.Cases = append(report.Cases, res)
		if !res.Pass {
			t.Errorf("case %q: status=%d want=%d upstreamΔ=%d want=%d historyΔ=%d want=%d%s",
				res.Name, res.GotStatus, res.WantStatus, res.GotUpstream, res.WantUpstream, res.GotHistory, res.WantHistory, detail)
		}
	}
	check := func(name, method, path, auth string, wantStatus int, wantUpstream int64, wantHistory int, source uuid.UUID) {
		hitsBefore, histBefore := upstreamHits.Load(), historyCount(source)
		var body io.Reader
		if method == http.MethodPost {
			body = strings.NewReader(`{"inputs":{}}`)
		}
		req, err := http.NewRequestWithContext(ctx, method, ts.URL+path, body)
		require.NoError(t, err)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		require.NoError(t, err)
		_ = resp.Body.Close()
		res := s1secCaseResult{
			Name: name, WantStatus: wantStatus, GotStatus: resp.StatusCode,
			WantUpstream: wantUpstream, GotUpstream: upstreamHits.Load() - hitsBefore,
			WantHistory: wantHistory, GotHistory: historyCount(source) - histBefore,
			Route: method + " " + path,
			Auth: func() string {
				switch {
				case auth == "":
					return "none"
				case strings.HasPrefix(auth, "Bearer eyJ"): // real JWTs start with the base64 JSON header
					return "jwt"
				default:
					return "invalid-token"
				}
			}(),
		}
		res.Pass = res.GotStatus == wantStatus && res.GotUpstream == wantUpstream && res.GotHistory == wantHistory
		detail := ""
		if res.Pass && wantStatus != http.StatusOK {
			// Non-200 responses must be existence-opaque and content-free:
			// none of the fixture source ids may appear in the body.
			for _, fixture := range []uuid.UUID{srcOrgA, srcOrgARL, srcOrgB, srcPerson} {
				if strings.Contains(string(raw), fixture.String()) {
					res.Pass, detail = false, fmt.Sprintf("; %d body leaked fixture source id via %s", wantStatus, path)
					break
				}
			}
		}
		record(res, detail)
	}
	// checkHistoryContent drives a history GET and asserts the SEEDED rows'
	// content comes back: non-empty, only this source's rows, the marker
	// caller present — and no other fixture source ever appears (content
	// isolation, not just a status code).
	checkHistoryContent := func(name, path, auth string, source, markerUser uuid.UUID, others ...uuid.UUID) {
		hitsBefore, histBefore := upstreamHits.Load(), historyCount(source)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+path, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", auth)
		resp, err := client.Do(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		require.NoError(t, err)
		_ = resp.Body.Close()
		res := s1secCaseResult{
			Name: name, WantStatus: http.StatusOK, GotStatus: resp.StatusCode,
			WantUpstream: 0, GotUpstream: upstreamHits.Load() - hitsBefore,
			WantHistory: 0, GotHistory: historyCount(source) - histBefore,
			Route: http.MethodGet + " " + path, Auth: "jwt",
		}
		res.Pass = res.GotStatus == http.StatusOK && res.GotUpstream == 0 && res.GotHistory == 0
		detail := ""
		if res.Pass {
			var list models.ListResponse[models.WebhookHistoryEntry]
			if err := json.Unmarshal(raw, &list); err != nil {
				res.Pass, detail = false, "; body is not a history list"
			} else if len(list.Items) == 0 {
				res.Pass, detail = false, "; history read returned EMPTY items (expected seeded rows)"
			} else {
				markerSeen := false
				for _, item := range list.Items {
					if item.SourceID != source {
						res.Pass, detail = false, fmt.Sprintf("; cross-source row %s leaked into response", item.SourceID)
						break
					}
					if item.UserID == markerUser {
						markerSeen = true
					}
				}
				if res.Pass && !markerSeen {
					res.Pass, detail = false, fmt.Sprintf("; seeded marker row (user %s) missing", markerUser)
				}
				for _, other := range others {
					if strings.Contains(string(raw), other.String()) {
						res.Pass, detail = false, fmt.Sprintf("; response leaks other source id %s", other)
						break
					}
				}
			}
		}
		record(res, detail)
	}
	invokePath := func(src uuid.UUID) string { return "/api/v1/webhooks/" + src.String() + "/invoke" }

	// --- Cross-tenant rejections: 404, zero outbound, zero history rows. ---
	check("cross-org admin invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: uuid.New(), org: orgPtr(orgB), role: []string{"admin"}}), http.StatusNotFound, 0, 0, srcOrgA)
	check("cross-org both-grants invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: uuid.New(), org: orgPtr(orgB), perm: []string{"connections:write", "webhooks:invoke"}}), http.StatusNotFound, 0, 0, srcOrgA)
	check("same sub switched to org B", http.MethodPost, invokePath(srcOrgA), mint(who{sub: ownerA, org: orgPtr(orgB)}), http.StatusNotFound, 0, 0, srcOrgA)
	check("org admin against personal webhook", http.MethodPost, invokePath(srcPerson), mint(who{sub: ownerA, org: orgPtr(orgA), role: []string{"admin"}}), http.StatusNotFound, 0, 0, srcPerson)
	check("personal cross-owner invoke", http.MethodPost, invokePath(srcPerson), mint(who{sub: uuid.New(), role: []string{"admin"}}), http.StatusNotFound, 0, 0, srcPerson)
	check("cross-org admin history", http.MethodGet, "/api/v1/webhooks/"+srcOrgA.String()+"/history", mint(who{sub: uuid.New(), org: orgPtr(orgB), role: []string{"admin"}}), http.StatusNotFound, 0, 0, srcOrgA)
	check("cross-org history alias route", http.MethodGet, "/api/v1/data-connection/sources/"+srcOrgA.String()+"/webhook-history", mint(who{sub: uuid.New(), org: orgPtr(orgB), perm: []string{"connections:read", "webhooks:read"}}), http.StatusNotFound, 0, 0, srcOrgA)
	check("invalid jwt invoke", http.MethodPost, invokePath(srcOrgA), "Bearer s1sec-not-a-jwt", http.StatusUnauthorized, 0, 0, srcOrgA)
	check("missing jwt invoke", http.MethodPost, invokePath(srcOrgA), "", http.StatusUnauthorized, 0, 0, srcOrgA)
	check("invalid jwt history", http.MethodGet, "/api/v1/webhooks/"+srcOrgA.String()+"/history", "Bearer s1sec-not-a-jwt", http.StatusUnauthorized, 0, 0, srcOrgA)
	check("non-uuid id rejected before lookup", http.MethodPost, "/api/v1/webhooks/not-a-uuid/invoke", mint(who{sub: ownerA, org: orgPtr(orgA)}), http.StatusBadRequest, 0, 0, srcOrgA)

	unknown := uuid.New()
	check("unknown uuid is a plain 404", http.MethodPost, invokePath(unknown), mint(who{sub: ownerA, org: orgPtr(orgA)}), http.StatusNotFound, 0, 0, unknown)

	// Cross-tenant probing must not consume rate-limit budget: flood the
	// rate-limited org-A webhook from org B, then the rightful owner still
	// gets exactly one successful invoke before the 429 (whose failed
	// attempt IS recorded in history — original behavior).
	for i := 0; i < 4; i++ {
		check(fmt.Sprintf("rate-limit probe flood %d", i), http.MethodPost, invokePath(srcOrgARL), mint(who{sub: uuid.New(), org: orgPtr(orgB), role: []string{"admin"}}), http.StatusNotFound, 0, 0, srcOrgARL)
	}
	check("rate-limited owner invoke still allowed after flood", http.MethodPost, invokePath(srcOrgARL), mint(who{sub: ownerA, org: orgPtr(orgA)}), http.StatusOK, 1, 1, srcOrgARL)
	check("rate-limited second invoke throttled", http.MethodPost, invokePath(srcOrgARL), mint(who{sub: ownerA, org: orgPtr(orgA)}), http.StatusTooManyRequests, 0, 1, srcOrgARL)

	// --- Same-tenant positives: real outbound + persisted history row. ---
	check("org owner invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: ownerA, org: orgPtr(orgA)}), http.StatusOK, 1, 1, srcOrgA)
	check("org admin invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: uuid.New(), org: orgPtr(orgA), role: []string{"admin"}}), http.StatusOK, 1, 1, srcOrgA)
	check("org member connections:write invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: memberA, org: orgPtr(orgA), perm: []string{"connections:write"}}), http.StatusOK, 1, 1, srcOrgA)
	check("org member webhooks:invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: memberA, org: orgPtr(orgA), perm: []string{"webhooks:invoke"}}), http.StatusOK, 1, 1, srcOrgA)
	check("personal owner invoke", http.MethodPost, invokePath(srcPerson), mint(who{sub: ownerP}), http.StatusOK, 1, 1, srcPerson)

	// Same-tenant insufficient grants keep the 403 authorization answer.
	check("org member without grants invoke", http.MethodPost, invokePath(srcOrgA), mint(who{sub: memberA, org: orgPtr(orgA)}), http.StatusForbidden, 0, 0, srcOrgA)
	check("org member without grants history", http.MethodGet, "/api/v1/webhooks/"+srcOrgA.String()+"/history", mint(who{sub: memberA, org: orgPtr(orgA)}), http.StatusForbidden, 0, 0, srcOrgA)

	// History positive paths across both URL aliases — content-verified:
	// seeded rows (marker = the earlier owner invoke caller) must come back,
	// restricted to this source, with no other fixture source ever appearing.
	checkHistoryContent("org owner history", "/api/v1/webhooks/"+srcOrgA.String()+"/history", mint(who{sub: ownerA, org: orgPtr(orgA)}), srcOrgA, ownerA, srcOrgB, srcPerson, srcOrgARL)
	checkHistoryContent("org member webhooks:read history", "/api/v1/webhooks/"+srcOrgA.String()+"/history", mint(who{sub: memberA, org: orgPtr(orgA), perm: []string{"webhooks:read"}}), srcOrgA, ownerA, srcOrgB, srcPerson, srcOrgARL)
	checkHistoryContent("org owner history alias", "/api/v1/data-connection/sources/"+srcOrgA.String()+"/webhook-history", mint(who{sub: ownerA, org: orgPtr(orgA)}), srcOrgA, ownerA, srcOrgB, srcPerson, srcOrgARL)
	checkHistoryContent("personal owner history", "/api/v1/webhooks/"+srcPerson.String()+"/history", mint(who{sub: ownerP}), srcPerson, ownerP, srcOrgA, srcOrgB, srcOrgARL)

	report.AllPassed = !t.Failed()
	if out := os.Getenv("S1SEC_RESULTS_JSON"); out != "" {
		raw, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(out, raw, 0o644))
	}
}
