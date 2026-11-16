package server_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/server"
)

// publicRoutes are the endpoints that intentionally skip authentication
// (liveness, metrics, capability registry).
var publicRoutes = map[string]bool{
	"/healthz":                 true,
	"/health":                  true,
	"/metrics":                 true,
	"/capabilities":            true,
	"/capabilities/dependency": true,
	"/capabilities/openapi":    true,
}

// TestEveryBusinessRouteRequiresAuthentication walks the production router
// and proves the P1.1 invariant mechanically: outside the public surface,
// EVERY mounted route rejects unauthenticated requests with 401 before any
// handler logic runs. A route group added without the auth + tenant-scope
// middleware (the state /api/v1/pipeline was in before this change) fails
// this test.
func TestEveryBusinessRouteRequiresAuthentication(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{}
	cfg.Service.Name = "pipeline-build-service"
	cfg.Service.Version = "test"
	cfg.JWTSecret = "test-secret-32bytes-test-secret-3"

	router := server.BuildRouter(cfg, nil)
	routes, ok := router.(chi.Routes)
	require.True(t, ok, "handler should expose chi routes")

	type routeCase struct{ method, path string }
	var audited []routeCase
	require.NoError(t, chi.Walk(routes, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		audited = append(audited, routeCase{method: method, path: route})
		return nil
	}))
	require.NotEmpty(t, audited)

	checked := 0
	for _, rc := range audited {
		if publicRoutes[rc.path] {
			continue
		}
		// Only business paths carry data; capability mounts under other
		// prefixes are public registry metadata.
		if !strings.HasPrefix(rc.path, "/api/") && !strings.HasPrefix(rc.path, "/v1") {
			continue
		}
		path := materializeRoute(rc.path)
		req := httptest.NewRequest(rc.method, path, nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equalf(t, http.StatusUnauthorized, rr.Code,
			"%s %s must reject unauthenticated calls with 401 (got %d; body=%q)",
			rc.method, path, rr.Code, strings.TrimSpace(rr.Body.String()))
		require.Equalf(t, "missing bearer token", errorField(rr), "%s %s: expected auth-middleware rejection", rc.method, path)
		checked++
	}
	require.Greater(t, checked, 40, "audit should cover the whole business surface")
	t.Logf("audited %d authenticated routes", checked)
}

// materializeRoute substitutes chi path parameters with parseable dummies
// so the router dispatches into the auth middleware instead of 404/400.
func materializeRoute(route string) string {
	out := route
	for _, param := range []string{"id", "rid", "run_id", "node_id", "version_id", "kind", "pipeline_rid"} {
		out = strings.ReplaceAll(out, "{"+param+"}", "ri.foundry.main.dummy.00000000-0000-0000-0000-000000000000")
	}
	return out
}

func errorField(rr *httptest.ResponseRecorder) string {
	body := rr.Body.String()
	if idx := strings.Index(body, `"error":"`); idx >= 0 {
		rest := body[idx+len(`"error":"`):]
		if end := strings.Index(rest, `"`); end >= 0 {
			return rest[:end]
		}
	}
	return ""
}
