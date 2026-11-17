//go:build integration

// Real-HTTP + real-PostgreSQL acceptance tests for the T8 scheduling
// CRUD API (WP3.2 worker D). These run only when SCHEDULING_API_TEST_DSN
// points at a dedicated worker-D database (container ofwp32-d-pg, port
// 55433 — never ofwp32-pg-1, which belongs to worker A's fixtures).
//
// Fidelity rules:
//   - requests go through the REAL middleware chain (authmw.Middleware
//   - handler.TenantScopeMiddleware) and the REAL chi routing shape
//     mounted by server.go for /api/v1/scheduling/*;
//   - actors are real HS256 JWTs minted with the same authmw encoder
//     the platform uses;
//   - assertions read the actual database rows (state, ownership,
//     generation, revision, next_run_at) — never handler echoes.
//
// Identity/keys are randomized per run so repeated runs stay green on
// the persistent worker-D database.
package schedulingapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	eventscheduler "github.com/openfoundry/openfoundry-go/libs/event-scheduler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/schedulingapi"
)

const (
	testJWTSecret  = "ofwp32-d-jwt-secret"              // independent of the encryption key
	testCipherKey  = "0123456789abcdef0123456789abcdef" // exactly 32 bytes
	midnightCron   = "0 0 * * *"
	invalidCron    = "definitely not a cron"
	testBranch     = "main"
	tokenTTLForJWT = 2 * time.Hour
)

// apiEnv is one fully-wired scheduling API against the real database.
type apiEnv struct {
	pool   *pgxpool.Pool
	repo   *postgres.Repository
	jwt    *authmw.JWTConfig
	router http.Handler
}

func newAPIEnv(ctx context.Context, t *testing.T) *apiEnv {
	t.Helper()
	dsn := os.Getenv("SCHEDULING_API_TEST_DSN")
	if dsn == "" {
		t.Skip("SCHEDULING_API_TEST_DSN not set — worker-D scheduling API tests need the ofwp32-d-pg database")
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, postgres.RunMigrations(ctx, pool))
	repo := postgres.NewRepositoryFromPool(pool)

	jwt := authmw.NewJWTConfig(testJWTSecret)
	if len(testCipherKey) != 32 {
		t.Fatalf("test cipher key must be 32 bytes")
	}
	api, err := schedulingapi.New(repo, jwt, config.SchedulerConfig{
		Enabled: true, CredentialEncryptionKey: []byte(testCipherKey),
	})
	require.NoError(t, err)

	// The same route shape + middleware chain server.go mounts.
	r := chi.NewRouter()
	r.Route("/api/v1", func(apiGroup chi.Router) {
		apiGroup.Use(authmw.Middleware(jwt))
		apiGroup.Use(handler.TenantScopeMiddleware())
		apiGroup.Route("/scheduling", func(sch chi.Router) {
			sch.Post("/grants", api.CreateGrant)
			sch.Delete("/grants/{key}", api.RevokeGrant)
			sch.Post("/definitions", api.EnableDefinition)
			sch.Patch("/definitions/{key}/pause", api.PauseDefinition)
			sch.Patch("/definitions/{key}/resume", api.ResumeDefinition)
			sch.Delete("/definitions/{key}", api.DisableDefinition)
		})
	})
	return &apiEnv{pool: pool, repo: repo, jwt: jwt, router: r}
}

// mintToken builds a real signed JWT for one actor.
func (e *apiEnv) mintToken(t *testing.T, sub uuid.UUID, orgID *uuid.UUID, roles ...string) string {
	t.Helper()
	now := time.Now()
	claims := &authmw.Claims{
		Sub: sub, IAT: now.Unix(), EXP: now.Add(tokenTTLForJWT).Unix(),
		JTI: uuid.New(), Roles: roles, OrgID: orgID,
	}
	tok, err := authmw.EncodeToken(e.jwt, claims)
	require.NoError(t, err)
	return tok
}

// do executes one real HTTP request through the full chain and returns
// the recorder plus the decoded JSON body (when present).
func (e *apiEnv) do(t *testing.T, method, path, token string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec, out
}

// ─── DB state readers (assertions read the database, not echoes) ─────

type grantRow struct {
	ID          uuid.UUID
	Subject     uuid.UUID
	CreatedBy   uuid.UUID
	Generation  int
	State       string
	PipelineRID string
	ExpiresAt   time.Time
}

func (e *apiEnv) latestGrant(ctx context.Context, t *testing.T, kind string, tenantID uuid.UUID, key string) *grantRow {
	t.Helper()
	var g grantRow
	err := e.pool.QueryRow(ctx,
		`SELECT id, subject, created_by, generation, state, pipeline_rid, expires_at
		   FROM scheduling.grants
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3
		  ORDER BY generation DESC LIMIT 1`,
		kind, tenantID, key,
	).Scan(&g.ID, &g.Subject, &g.CreatedBy, &g.Generation, &g.State, &g.PipelineRID, &g.ExpiresAt)
	if err != nil {
		t.Logf("latest grant scan err=%v", err)
	}
	require.NoError(t, err)
	return &g
}

func (e *apiEnv) grantRowCount(ctx context.Context, t *testing.T, kind string, tenantID uuid.UUID, key string) int {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM scheduling.grants
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`,
		kind, tenantID, key).Scan(&n))
	return n
}

type definitionRow struct {
	ID            uuid.UUID
	CronExpr      string
	Enabled       bool
	DispatchMode  *string
	Revision      int
	PipelineRID   string
	BuildBranch   string
	GrantRef      uuid.UUID
	NextRunAt     time.Time
	QuarantinedAt *time.Time
}

func (e *apiEnv) definition(ctx context.Context, t *testing.T, kind string, tenantID uuid.UUID, key string) *definitionRow {
	t.Helper()
	var d definitionRow
	err := e.pool.QueryRow(ctx,
		`SELECT id, cron_expr, enabled, dispatch_mode, definition_revision,
		        pipeline_rid, build_branch, grant_ref, next_run_at, quarantined_at
		   FROM schedules.definitions
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`,
		kind, tenantID, key,
	).Scan(&d.ID, &d.CronExpr, &d.Enabled, &d.DispatchMode, &d.Revision,
		&d.PipelineRID, &d.BuildBranch, &d.GrantRef, &d.NextRunAt, &d.QuarantinedAt)
	require.NoError(t, err)
	return &d
}

func (e *apiEnv) dbNow(ctx context.Context, t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now))
	return now
}

// createGrantAs is the canonical happy-path helper.
func (e *apiEnv) createGrantAs(t *testing.T, sub uuid.UUID, orgID *uuid.UUID, key, pipelineRID string, roles ...string) (string, int) {
	t.Helper()
	tok := e.mintToken(t, sub, orgID, roles...)
	rec, body := e.do(t, "POST", "/api/v1/scheduling/grants", tok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipelineRID, "token": tok,
	})
	require.Equal(t, http.StatusCreated, rec.Code, "create grant: %v", body)
	gen := int(body["generation"].(float64))
	return body["id"].(string), gen
}

// enableAs is the canonical happy-path definition enable helper.
func (e *apiEnv) enableAs(t *testing.T, sub uuid.UUID, orgID *uuid.UUID, key, pipelineRID, grantID string, gen int, cron string) {
	t.Helper()
	tok := e.mintToken(t, sub, orgID)
	rec, body := e.do(t, "POST", "/api/v1/scheduling/definitions", tok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipelineRID, "build_branch": testBranch,
		"cron": cron, "grant_id": grantID, "grant_generation": gen,
	})
	require.Equal(t, http.StatusCreated, rec.Code, "enable definition: %v", body)
}

// ─── Org-domain grant authorization ───────────────────────────────────

// TestOrgGrantOwnership verifies the P1 fix: a same-org regular user
// can neither revoke nor re-grant another member's grant, an org admin
// can, and a foreign-org actor learns nothing (404, state untouched).
func TestOrgGrantOwnership(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	orgA := uuid.New()
	owner := uuid.New()
	member := uuid.New()
	admin := uuid.New()
	orgB := uuid.New()
	foreigner := uuid.New()
	key := "own-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()

	// Owner creates their grant.
	grantID, gen := e.createGrantAs(t, owner, &orgA, key, pipeline)
	require.Equal(t, 1, gen)
	g := e.latestGrant(ctx, t, "org", orgA, key)
	require.Equal(t, "active", g.State)
	require.Equal(t, owner, g.CreatedBy)
	require.Equal(t, owner, g.Subject)
	require.Equal(t, grantID, g.ID.String())

	// Same-org regular member: revoke refused, state untouched.
	memberTok := e.mintToken(t, member, &orgA)
	rec, body := e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, memberTok, nil)
	require.Equal(t, http.StatusForbidden, rec.Code, "%v", body)
	require.Equal(t, "forbidden", body["error"])
	require.Equal(t, "active", e.latestGrant(ctx, t, "org", orgA, key).State)

	// Same-org regular member: re-grant (create) refused, no row added.
	rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", memberTok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": memberTok,
	})
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Equal(t, 1, e.grantRowCount(ctx, t, "org", orgA, key))

	// Foreign-org actor: indistinguishable from a missing key (404),
	// and the owner's row is untouched.
	foreignTok := e.mintToken(t, foreigner, &orgB)
	rec, body = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, foreignTok, nil)
	require.Equal(t, http.StatusNotFound, rec.Code, "%v", body)
	require.Equal(t, "grant_not_found", body["error"])
	require.Equal(t, "active", e.latestGrant(ctx, t, "org", orgA, key).State)

	// Org admin revokes (owner-managed resource, admin override).
	adminTok := e.mintToken(t, admin, &orgA, "admin")
	rec, body = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, adminTok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, "revoked", body["status"])
	require.Equal(t, "revoked", e.latestGrant(ctx, t, "org", orgA, key).State)

	// Admin re-grants: generation bumps to 2 and ownership follows the
	// new row's creator (the admin).
	rec, body = e.do(t, "POST", "/api/v1/scheduling/grants", adminTok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": adminTok,
	})
	require.Equal(t, http.StatusCreated, rec.Code, "%v", body)
	require.Equal(t, 2, int(body["generation"].(float64)))
	g = e.latestGrant(ctx, t, "org", orgA, key)
	require.Equal(t, "active", g.State)
	require.Equal(t, admin, g.CreatedBy)
	require.Equal(t, 2, e.grantRowCount(ctx, t, "org", orgA, key)) // history kept
}

// TestGrantTokenBinding pins the ingest rules: the stored token must be
// a valid JWT matching the CALLER's derived domain and subject, and the
// body may never smuggle identity fields.
func TestGrantTokenBinding(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	orgA := uuid.New()
	owner := uuid.New()
	colleague := uuid.New()
	key := "tok-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
	ownerTok := e.mintToken(t, owner, &orgA)

	// Personal token (no org claim) under an org caller: 403.
	personalTok := e.mintToken(t, owner, nil)
	rec, body := e.do(t, "POST", "/api/v1/scheduling/grants", ownerTok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": personalTok,
	})
	require.Equal(t, http.StatusForbidden, rec.Code, "%v", body)
	require.Equal(t, 0, e.grantRowCount(ctx, t, "org", orgA, key))

	// Same org, different subject: 403.
	colleagueTok := e.mintToken(t, colleague, &orgA)
	rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", ownerTok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": colleagueTok,
	})
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Equal(t, 0, e.grantRowCount(ctx, t, "org", orgA, key))

	// Garbage token: 400.
	rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", ownerTok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": "not-a-jwt",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Identity-selecting body fields are rejected outright.
	rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", ownerTok, map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": ownerTok,
		"tenant_kind": "personal", "org_id": uuid.NewString(), "created_by": uuid.NewString(),
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, 0, e.grantRowCount(ctx, t, "org", orgA, key))

	// Missing fields: 400.
	rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", ownerTok, map[string]any{
		"schedule_key": key, "token": ownerTok,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Contract-violating schedule keys are rejected at grant creation
	// too ('/' would break aggregate ids; >200 chars breaks the schema).
	for _, badKey := range []string{"bad/key", strings.Repeat("k", 201)} {
		rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", ownerTok, map[string]any{
			"schedule_key": badKey, "pipeline_rid": pipeline, "token": ownerTok,
		})
		require.Equal(t, http.StatusBadRequest, rec.Code, "key %q must be rejected", badKey)
	}

	// Unauthenticated: the middleware chain answers 401 before the API.
	rec, _ = e.do(t, "POST", "/api/v1/scheduling/grants", "", map[string]any{
		"schedule_key": key, "pipeline_rid": pipeline, "token": ownerTok,
	})
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// ─── Personal domain ──────────────────────────────────────────────────

// TestPersonalDomainOwnerOnly pins the frozen rule: personal-domain
// resources are owner-only even for admin claims, a different personal
// subject sees 404 (not 403 — no oracle), and the SAME subject with an
// org ticket is a different tenant domain (personal grants never
// transfer across an org switch).
func TestPersonalDomainOwnerOnly(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	p1 := uuid.New()
	p2 := uuid.New()
	key := "me-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()

	_, gen := e.createGrantAs(t, p1, nil, key, pipeline)
	require.Equal(t, 1, gen)
	require.Equal(t, "active", e.latestGrant(ctx, t, "personal", p1, key).State)

	// Another personal subject (plain or admin): their domain has no
	// such key — 404 both times, owner's row untouched.
	p2Tok := e.mintToken(t, p2, nil)
	rec, body := e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, p2Tok, nil)
	require.Equal(t, http.StatusNotFound, rec.Code, "%v", body)
	p2AdminTok := e.mintToken(t, p2, nil, "admin")
	rec, _ = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, p2AdminTok, nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "active", e.latestGrant(ctx, t, "personal", p1, key).State)

	// Same subject, now carrying an org ticket: org domain — the
	// personal grant is invisible (404), never reusable.
	orgA := uuid.New()
	orgTok := e.mintToken(t, p1, &orgA)
	rec, _ = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, orgTok, nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "active", e.latestGrant(ctx, t, "personal", p1, key).State)

	// Owner revokes their own grant.
	p1Tok := e.mintToken(t, p1, nil)
	rec, body = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, p1Tok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, "revoked", e.latestGrant(ctx, t, "personal", p1, key).State)
}

// TestPersonalDefinitionOwnerOnly: definitions in the personal domain
// are managed by their owner only — another personal subject (even
// admin) sees 404 on pause/resume/delete.
func TestPersonalDefinitionOwnerOnly(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	p1 := uuid.New()
	p2 := uuid.New()
	key := "pdef-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()

	grantID, gen := e.createGrantAs(t, p1, nil, key, pipeline)
	e.enableAs(t, p1, nil, key, pipeline, grantID, gen, midnightCron)

	// p2 (plain and admin) manages: their domain has no such
	// definition — 404, revision untouched.
	before := e.definition(ctx, t, "personal", p1, key).Revision
	for _, tok := range []string{e.mintToken(t, p2, nil), e.mintToken(t, p2, nil, "admin")} {
		for _, call := range []struct{ method, path string }{
			{"PATCH", "/api/v1/scheduling/definitions/" + key + "/pause"},
			{"PATCH", "/api/v1/scheduling/definitions/" + key + "/resume"},
			{"DELETE", "/api/v1/scheduling/definitions/" + key},
		} {
			rec, _ := e.do(t, call.method, call.path, tok, nil)
			require.Equal(t, http.StatusNotFound, rec.Code, "%s %s", call.method, call.path)
		}
	}
	require.Equal(t, before, e.definition(ctx, t, "personal", p1, key).Revision)
}

// ─── Definition enablement: grant binding checks (fix #2) ────────────

// TestEnableDefinitionGrantChecks proves a grant for pipeline P1 can
// never arm a definition for P2, stale generations/revoked/expired
// grants are refused with 409s, and non-owners cannot arm someone
// else's grant — with no definition row created on any refusal.
func TestEnableDefinitionGrantChecks(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	orgA := uuid.New()
	owner := uuid.New()
	member := uuid.New()
	admin := uuid.New()
	key := "bind-" + run
	p1 := "ri.foundry.main.pipeline." + uuid.NewString()
	p2 := "ri.foundry.main.pipeline." + uuid.NewString()

	grantID, gen := e.createGrantAs(t, owner, &orgA, key, p1)
	ownerTok := e.mintToken(t, owner, &orgA)
	memberTok := e.mintToken(t, member, &orgA)

	enableBody := func(pipeline, gid string, generation int) map[string]any {
		return map[string]any{
			"schedule_key": key, "pipeline_rid": pipeline, "build_branch": testBranch,
			"cron": midnightCron, "grant_id": gid, "grant_generation": generation,
		}
	}

	// Grant is for P1; definition claims P2 → 409, no row.
	rec, body := e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, enableBody(p2, grantID, gen))
	require.Equal(t, http.StatusConflict, rec.Code, "%v", body)
	require.Equal(t, "grant_pipeline_mismatch", body["error"])
	e.requireNoDefinition(ctx, t, "org", orgA, key)

	// Stale generation → 409.
	rec, body = e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, enableBody(p1, grantID, gen+1))
	require.Equal(t, http.StatusConflict, rec.Code, "%v", body)
	require.Equal(t, "grant_generation_mismatch", body["error"])
	e.requireNoDefinition(ctx, t, "org", orgA, key)

	// Non-owner member arming the owner's grant → 403.
	rec, body = e.do(t, "POST", "/api/v1/scheduling/definitions", memberTok, enableBody(p1, grantID, gen))
	require.Equal(t, http.StatusForbidden, rec.Code, "%v", body)
	e.requireNoDefinition(ctx, t, "org", orgA, key)

	// Bogus grant id → 404.
	rec, body = e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, enableBody(p1, uuid.NewString(), gen))
	require.Equal(t, http.StatusNotFound, rec.Code, "%v", body)
	e.requireNoDefinition(ctx, t, "org", orgA, key)

	// Revoked grant → 409 grant_not_active.
	rec, _ = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, ownerTok, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec, body = e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, enableBody(p1, grantID, gen))
	require.Equal(t, http.StatusConflict, rec.Code, "%v", body)
	require.Equal(t, "grant_not_active", body["error"])
	e.requireNoDefinition(ctx, t, "org", orgA, key)

	// Expired grant (time advanced via SQL, per repo test convention)
	// → 409 grant_expired.
	grant2, gen2 := e.createGrantAs(t, owner, &orgA, key, p1)
	require.NoError(t, execAPI(t, e.pool, ctx,
		`UPDATE scheduling.grants SET state='active', expires_at=NOW() - interval '1 hour' WHERE id=$1`, grant2))
	rec, body = e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, enableBody(p1, grant2, gen2))
	require.Equal(t, http.StatusConflict, rec.Code, "%v", body)
	require.Equal(t, "grant_expired", body["error"])
	e.requireNoDefinition(ctx, t, "org", orgA, key)

	// Admin arms the (re-granted, unexpired) grant: 201.
	grant3, gen3 := e.createGrantAs(t, admin, &orgA, key, p1, "admin")
	e.enableAs(t, admin, &orgA, key, p1, grant3, gen3, midnightCron)
	d := e.definition(ctx, t, "org", orgA, key)
	require.Equal(t, "pipeline_outbox_v1", *d.DispatchMode)
	require.Equal(t, p1, d.PipelineRID)
}

func (e *apiEnv) requireNoDefinition(ctx context.Context, t *testing.T, kind string, tenantID uuid.UUID, key string) {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM schedules.definitions
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`,
		kind, tenantID, key).Scan(&n))
	require.Zero(t, n, "no definition row may exist after a refused enable")
}

// ─── First fire instant + revision lifecycle (fix #3) ────────────────

// TestEnableDefinitionCronNextRun pins the cron fix: a midnight cron
// registered mid-day arms at the NEXT MIDNIGHT, never now()+1 minute;
// invalid cron and contract-violating keys/branches are rejected before
// any write.
func TestEnableDefinitionCronNextRun(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	orgA := uuid.New()
	owner := uuid.New()
	key := "cron-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
	ownerTok := e.mintToken(t, owner, &orgA)

	grantID, gen := e.createGrantAs(t, owner, &orgA, key, pipeline)

	before := e.dbNow(ctx, t)
	e.enableAs(t, owner, &orgA, key, pipeline, grantID, gen, midnightCron)
	after := e.dbNow(ctx, t)
	d := e.definition(ctx, t, "org", orgA, key)

	nextAfter := func(from time.Time) time.Time {
		def := eventscheduler.ScheduleDefinition{CronExpr: midnightCron, CronFlavor: "unix5", TimeZone: "UTC", Name: key}
		next, err := eventscheduler.ComputeNextFire(&def, from)
		require.NoError(t, err)
		return next
	}
	expectedBefore, expectedAfter := nextAfter(before), nextAfter(after)
	require.True(t, d.NextRunAt.Equal(expectedBefore) || d.NextRunAt.Equal(expectedAfter),
		"next_run_at %s must be the cron's next slot (%s or %s)", d.NextRunAt, expectedBefore, expectedAfter)
	// Regression for the now()+1min bug: a midnight cron armed mid-day
	// is hours away, not ~a minute.
	require.Greater(t, d.NextRunAt.Sub(after), 90*time.Second,
		"next_run_at must not be a fixed now()+1min (%s vs now %s)", d.NextRunAt, after)
	var flavor, tz string
	require.NoError(t, e.pool.QueryRow(ctx,
		`SELECT cron_flavor, time_zone FROM schedules.definitions WHERE id=$1`, d.ID).Scan(&flavor, &tz))
	require.Equal(t, "unix5", flavor)
	require.Equal(t, "UTC", tz)
	require.Equal(t, "pipeline.builds.v1", e.topicOf(ctx, t, d.ID))

	// Invalid cron → 400, no row for a fresh key.
	key2 := "cronbad-" + run
	grant2, gen2 := e.createGrantAs(t, owner, &orgA, key2, pipeline)
	rec, body := e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, map[string]any{
		"schedule_key": key2, "pipeline_rid": pipeline, "build_branch": testBranch,
		"cron": invalidCron, "grant_id": grant2, "grant_generation": gen2,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code, "%v", body)
	require.Equal(t, "invalid_cron", body["error"])
	e.requireNoDefinition(ctx, t, "org", orgA, key2)

	// Contract-violating enable bodies (a '/' in the key, an over-long
	// key/branch) are rejected BEFORE any write even when a valid grant
	// exists for the caller.
	for _, bad := range []map[string]any{
		{"schedule_key": "bad/key-" + run, "pipeline_rid": pipeline, "build_branch": testBranch,
			"cron": midnightCron, "grant_id": grant2, "grant_generation": gen2},
		{"schedule_key": strings.Repeat("k", 201), "pipeline_rid": pipeline, "build_branch": testBranch,
			"cron": midnightCron, "grant_id": grant2, "grant_generation": gen2},
		{"schedule_key": key2, "pipeline_rid": pipeline, "build_branch": strings.Repeat("b", 201),
			"cron": midnightCron, "grant_id": grant2, "grant_generation": gen2},
	} {
		rec, body = e.do(t, "POST", "/api/v1/scheduling/definitions", ownerTok, bad)
		require.Equal(t, http.StatusBadRequest, rec.Code, "%v", body)
		require.Equal(t, "invalid_request", body["error"])
	}
	e.requireNoDefinition(ctx, t, "org", orgA, "bad/key-"+run)
	e.requireNoDefinition(ctx, t, "org", orgA, strings.Repeat("k", 201))
}

func (e *apiEnv) topicOf(ctx context.Context, t *testing.T, defID uuid.UUID) string {
	t.Helper()
	var topic string
	require.NoError(t, e.pool.QueryRow(ctx,
		`SELECT topic FROM schedules.definitions WHERE id=$1`, defID).Scan(&topic))
	return topic
}

// TestDefinitionRevisionLifecycle walks the full lifecycle asserting
// the REAL revision sequence and that failed transitions never bump:
// enable(1) → pause(2) → resume(3) → revoke grant → pause(4) →
// resume refused (grant revoked, stays 4) → disable(5, dead for both
// ticks) → re-enable(6) → hard-delete + audit floor(7).
func TestDefinitionRevisionLifecycle(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	orgA := uuid.New()
	owner := uuid.New()
	key := "rev-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
	ownerTok := e.mintToken(t, owner, &orgA)

	grantID, gen := e.createGrantAs(t, owner, &orgA, key, pipeline)
	e.enableAs(t, owner, &orgA, key, pipeline, grantID, gen, midnightCron)
	require.Equal(t, 1, e.definition(ctx, t, "org", orgA, key).Revision)

	// Pause: revision 2, still pipeline_outbox_v1, disabled for the new
	// tick only (mode kept — no legacy fallback).
	rec, body := e.do(t, "PATCH", "/api/v1/scheduling/definitions/"+key+"/pause", ownerTok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, "paused", body["status"])
	d := e.definition(ctx, t, "org", orgA, key)
	require.Equal(t, 2, d.Revision)
	require.False(t, d.Enabled)
	require.Equal(t, "pipeline_outbox_v1", *d.DispatchMode)

	// Resume: revision 3, grant still valid → re-armed at the next
	// future slot (a stale next_run_at is NOT replayed retroactively).
	require.NoError(t, execAPI(t, e.pool, ctx,
		`UPDATE schedules.definitions SET next_run_at = NOW() - interval '2 hours'
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`, "org", orgA, key))
	resumeBefore := e.dbNow(ctx, t)
	rec, body = e.do(t, "PATCH", "/api/v1/scheduling/definitions/"+key+"/resume", ownerTok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, "resumed", body["status"])
	d = e.definition(ctx, t, "org", orgA, key)
	require.Equal(t, 3, d.Revision)
	require.True(t, d.Enabled)
	require.True(t, d.NextRunAt.After(resumeBefore), "resume must re-arm at a future slot, got %s", d.NextRunAt)

	// Revoke the grant → pause(4) still allowed, resume refused with
	// 409 and NO revision bump.
	rec, _ = e.do(t, "DELETE", "/api/v1/scheduling/grants/"+key, ownerTok, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	rec, body = e.do(t, "PATCH", "/api/v1/scheduling/definitions/"+key+"/pause", ownerTok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, 4, e.definition(ctx, t, "org", orgA, key).Revision)
	rec, body = e.do(t, "PATCH", "/api/v1/scheduling/definitions/"+key+"/resume", ownerTok, nil)
	require.Equal(t, http.StatusConflict, rec.Code, "%v", body)
	require.Equal(t, "grant_not_active", body["error"])
	d = e.definition(ctx, t, "org", orgA, key)
	require.Equal(t, 4, d.Revision)
	require.False(t, d.Enabled)

	// Disable: revision 5, mode NULL AND enabled false — dead for BOTH
	// ticks (never a legacy fallback).
	rec, body = e.do(t, "DELETE", "/api/v1/scheduling/definitions/"+key, ownerTok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, "disabled", body["status"])
	d = e.definition(ctx, t, "org", orgA, key)
	require.Equal(t, 5, d.Revision)
	require.Nil(t, d.DispatchMode)
	require.False(t, d.Enabled)

	// Re-enable the same key (same deterministic definition id): the
	// ON CONFLICT branch advances to 6 — no identity collision.
	grant2, gen2 := e.createGrantAs(t, owner, &orgA, key, pipeline)
	e.enableAs(t, owner, &orgA, key, pipeline, grant2, gen2, midnightCron)
	d = e.definition(ctx, t, "org", orgA, key)
	require.Equal(t, 6, d.Revision)
	require.True(t, d.Enabled)

	// Hard-delete the row, then fake audit history at revision 6 (the
	// definition once fired). Re-enable must floor the fresh INSERT at
	// 6+1=7 — a re-created row can never re-mint an existing revision.
	require.NoError(t, execAPI(t, e.pool, ctx, `DELETE FROM schedules.definitions WHERE id=$1`, d.ID))
	require.NoError(t, execAPI(t, e.pool, ctx,
		`INSERT INTO scheduling.dispatch_audit (event_id, definition_id, tenant_kind, tenant_id,
		   schedule_key, definition_revision, scheduled_for)
		 VALUES ($1,$2,'org',$3,$4,6,NOW())`,
		uuid.New(), d.ID, orgA, key))
	grant3, gen3 := e.createGrantAs(t, owner, &orgA, key, pipeline)
	e.enableAs(t, owner, &orgA, key, pipeline, grant3, gen3, midnightCron)
	require.Equal(t, 7, e.definition(ctx, t, "org", orgA, key).Revision)
}

// TestDefinitionOwnershipMatrix: definition management follows the same
// owner/org-admin rule as grants; a same-org member is 403, a foreign
// org is 404, the admin passes.
func TestDefinitionOwnershipMatrix(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	run := uuid.NewString()[:8]
	orgA := uuid.New()
	owner := uuid.New()
	member := uuid.New()
	admin := uuid.New()
	orgB := uuid.New()
	foreigner := uuid.New()
	key := "dmat-" + run
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()

	grantID, gen := e.createGrantAs(t, owner, &orgA, key, pipeline)
	e.enableAs(t, owner, &orgA, key, pipeline, grantID, gen, midnightCron)
	rev0 := e.definition(ctx, t, "org", orgA, key).Revision

	// Same-org member: 403 on every transition, revision untouched.
	memberTok := e.mintToken(t, member, &orgA)
	for _, call := range []struct{ method, path string }{
		{"PATCH", "/api/v1/scheduling/definitions/" + key + "/pause"},
		{"PATCH", "/api/v1/scheduling/definitions/" + key + "/resume"},
		{"DELETE", "/api/v1/scheduling/definitions/" + key},
	} {
		rec, _ := e.do(t, call.method, call.path, memberTok, nil)
		require.Equal(t, http.StatusForbidden, rec.Code, "%s %s", call.method, call.path)
	}
	require.Equal(t, rev0, e.definition(ctx, t, "org", orgA, key).Revision)

	// Foreign org: 404 everywhere.
	foreignTok := e.mintToken(t, foreigner, &orgB)
	for _, call := range []struct{ method, path string }{
		{"PATCH", "/api/v1/scheduling/definitions/" + key + "/pause"},
		{"DELETE", "/api/v1/scheduling/definitions/" + key},
	} {
		rec, _ := e.do(t, call.method, call.path, foreignTok, nil)
		require.Equal(t, http.StatusNotFound, rec.Code, "%s %s", call.method, call.path)
	}
	require.Equal(t, rev0, e.definition(ctx, t, "org", orgA, key).Revision)

	// Admin pauses; a missing key in-domain is a 404.
	adminTok := e.mintToken(t, admin, &orgA, "admin")
	rec, body := e.do(t, "PATCH", "/api/v1/scheduling/definitions/"+key+"/pause", adminTok, nil)
	require.Equal(t, http.StatusOK, rec.Code, "%v", body)
	require.Equal(t, rev0+1, e.definition(ctx, t, "org", orgA, key).Revision)
	rec, _ = e.do(t, "DELETE", "/api/v1/scheduling/definitions/never-existed-"+run, adminTok, nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// TestSchedulerDisabledPath: with the consumer disabled the API is
// built without a cipher and grant creation fails closed (503).
func TestSchedulerDisabledPath(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("SCHEDULING_API_TEST_DSN")
	if dsn == "" {
		t.Skip("SCHEDULING_API_TEST_DSN not set")
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, postgres.RunMigrations(ctx, pool))
	jwt := authmw.NewJWTConfig(testJWTSecret)
	api, err := schedulingapi.New(postgres.NewRepositoryFromPool(pool), jwt, config.SchedulerConfig{Enabled: false})
	require.NoError(t, err)
	r := chi.NewRouter()
	r.Route("/api/v1", func(apiGroup chi.Router) {
		apiGroup.Use(authmw.Middleware(jwt))
		apiGroup.Post("/scheduling/grants", api.CreateGrant)
	})

	sub, org := uuid.New(), uuid.New()
	tok, err := authmw.EncodeToken(jwt, &authmw.Claims{Sub: sub, EXP: time.Now().Add(time.Hour).Unix(), JTI: uuid.New(), OrgID: &org})
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/api/v1/scheduling/grants",
		strings.NewReader(fmt.Sprintf(`{"schedule_key":"x","pipeline_rid":"p","token":%q}`, tok)))
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "scheduler_disabled")
}

// The first insert has no row to lock: identity serialization must cover it too.
func TestConcurrentGrantGenerations(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	org, owner := uuid.New(), uuid.New()
	for _, history := range []bool{false, true} {
		key := "race-" + uuid.NewString()[:8]
		pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
		base := 0
		if history {
			_, base = e.createGrantAs(t, owner, &org, key, pipeline)
		}
		actor := postgres.SchedulingActor{Tenant: scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: org}, Subject: owner}
		g := postgres.SchedulingGrant{TenantKind: "org", TenantID: org, OrgID: &org, Subject: owner, CreatedBy: owner, ScheduleKey: key, PipelineRID: pipeline, ExpiresAt: time.Now().Add(time.Hour), TokenCiphertext: []byte("fixture")}
		type result struct {
			gen int
			err error
		}
		ch := make(chan result, 2)
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			go func() { <-start; _, gen, err := e.repo.UpsertGrantAs(ctx, actor, g); ch <- result{gen, err} }()
		}
		close(start)
		a, b := <-ch, <-ch
		require.NoError(t, a.err)
		require.NoError(t, b.err)
		require.ElementsMatch(t, []int{base + 1, base + 2}, []int{a.gen, b.gen})
	}
}

func TestEnableWaitsForDefinitionBeforeExpiryCheck(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	org, owner := uuid.New(), uuid.New()
	key := "expiry-" + uuid.NewString()[:8]
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
	id, gen := e.createGrantAs(t, owner, &org, key, pipeline)
	actor := postgres.SchedulingActor{Tenant: scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: org}, Subject: owner}
	gid := uuid.MustParse(id)
	_, _, _, err := e.repo.EnableSchedulingDefinitionAs(ctx, actor, key, pipeline, testBranch, midnightCron, gid, gen)
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, `UPDATE scheduling.grants SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1`, gid)
	require.NoError(t, err)
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `SELECT id FROM schedules.definitions WHERE tenant_id=$1 AND schedule_key=$2 FOR UPDATE`, org, key)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, _, _, err := e.repo.EnableSchedulingDefinitionAs(ctx, actor, key, pipeline, testBranch, midnightCron, gid, gen)
		done <- err
	}()
	require.Eventually(t, func() bool {
		var n int
		err := e.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%schedules.definitions%'`).Scan(&n)
		return err == nil && n > 0
	}, time.Second, 10*time.Millisecond)
	var expired bool
	require.Eventually(t, func() bool {
		err := e.pool.QueryRow(ctx, `SELECT expires_at<=clock_timestamp() FROM scheduling.grants WHERE id=$1`, gid).Scan(&expired)
		return err == nil && expired
	}, 4*time.Second, 20*time.Millisecond)
	require.NoError(t, tx.Commit(ctx))
	require.True(t, errors.Is(<-done, postgres.ErrGrantExpired))
}

// A queued former owner must observe the admin's newly committed owner,
// not the historical row visible before the wait.
func TestQueuedFormerOwnerCannotRegrant(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	org, owner, admin := uuid.New(), uuid.New(), uuid.New()
	key := "takeover-" + uuid.NewString()[:8]
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
	_, _ = e.createGrantAs(t, owner, &org, key, pipeline)
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	identity := "org:" + org.String() + ":" + key
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, identity)
	require.NoError(t, err)
	actor := postgres.SchedulingActor{Tenant: scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: org}, Subject: owner}
	g := postgres.SchedulingGrant{TenantKind: "org", TenantID: org, OrgID: &org, Subject: owner, CreatedBy: owner, ScheduleKey: key, PipelineRID: pipeline, ExpiresAt: time.Now().Add(time.Hour), TokenCiphertext: []byte("fixture")}
	done := make(chan error, 1)
	go func() { _, _, err := e.repo.UpsertGrantAs(ctx, actor, g); done <- err }()
	require.Eventually(t, func() bool {
		var n int
		err := e.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory'`).Scan(&n)
		return err == nil && n > 0
	}, time.Second, 10*time.Millisecond)
	_, err = tx.Exec(ctx, `UPDATE scheduling.grants SET state='revoked' WHERE tenant_id=$1 AND schedule_key=$2`, org, key)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO scheduling.grants (id,tenant_kind,tenant_id,org_id,subject,schedule_key,pipeline_rid,generation,state,expires_at,token_ciphertext,created_by) VALUES ($1,'org',$2,$2,$3,$4,$5,2,'active',clock_timestamp()+interval '1 hour',$6,$3)`, uuid.New(), org, admin, key, pipeline, []byte("fixture"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	require.ErrorIs(t, <-done, postgres.ErrSchedulingForbidden)
	latest := e.latestGrant(ctx, t, "org", org, key)
	require.Equal(t, 2, latest.Generation)
	require.Equal(t, admin, latest.CreatedBy)
}

func execAPI(t *testing.T, pool *pgxpool.Pool, ctx context.Context, sql string, args ...any) error {
	t.Helper()
	_, err := pool.Exec(ctx, sql, args...)
	return err
}

func TestEnableWaitsForGrantBeforeExpiryCheck(t *testing.T) {
	ctx := context.Background()
	e := newAPIEnv(ctx, t)
	org, owner := uuid.New(), uuid.New()
	key := "expiry-" + uuid.NewString()[:8]
	pipeline := "ri.foundry.main.pipeline." + uuid.NewString()
	id, gen := e.createGrantAs(t, owner, &org, key, pipeline)
	actor := postgres.SchedulingActor{Tenant: scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: org}, Subject: owner}
	gid := uuid.MustParse(id)
	_, _, _, err := e.repo.EnableSchedulingDefinitionAs(ctx, actor, key, pipeline, testBranch, midnightCron, gid, gen)
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, `UPDATE scheduling.grants SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1`, gid)
	require.NoError(t, err)
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `SELECT id FROM scheduling.grants WHERE tenant_id=$1 AND schedule_key=$2 FOR UPDATE`, org, key)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, _, _, err := e.repo.EnableSchedulingDefinitionAs(ctx, actor, key, pipeline, testBranch, midnightCron, gid, gen)
		done <- err
	}()
	require.Eventually(t, func() bool {
		var n int
		err := e.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%scheduling.grants%'`).Scan(&n)
		return err == nil && n > 0
	}, time.Second, 10*time.Millisecond)
	var expired bool
	require.Eventually(t, func() bool {
		err := e.pool.QueryRow(ctx, `SELECT expires_at<=clock_timestamp() FROM scheduling.grants WHERE id=$1`, gid).Scan(&expired)
		return err == nil && expired
	}, 4*time.Second, 20*time.Millisecond)
	require.NoError(t, tx.Commit(ctx))
	require.True(t, errors.Is(<-done, postgres.ErrGrantExpired))
}
