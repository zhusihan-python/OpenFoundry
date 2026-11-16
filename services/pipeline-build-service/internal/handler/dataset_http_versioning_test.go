package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newFakeDatasetService(t *testing.T) (*httptest.Server, *fakeDatasetState) {
	t.Helper()
	state := &fakeDatasetState{branches: map[string][]string{}, schemas: map[string]string{}, openStatus: http.StatusCreated, openCalls: 0}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.lastAuth = r.Header.Get("Authorization")
		state.paths = append(state.paths, r.Method+" "+r.URL.Path)
		if state.lastAuth == "" {
			writeTestJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/branches"):
			rid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/datasets/"), "/branches")
			names, ok := state.branches[rid]
			if !ok {
				writeTestJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "dataset not found"})
				return
			}
			// Paginate with `limit` (the route's real parameter) and
			// has_more semantics so the adapter must follow pages.
			limit := 500
			if raw := r.URL.Query().Get("limit"); raw != "" {
				fmt.Sscanf(raw, "%d", &limit)
			}
			if state.pageClamp > 0 && limit > state.pageClamp {
				limit = state.pageClamp
			}
			offset := 0
			if raw := r.URL.Query().Get("cursor"); raw != "" {
				fmt.Sscanf(raw, "o%d", &offset)
			}
			end := offset + limit
			if end > len(names) {
				end = len(names)
			}
			rows := []map[string]any{}
			for _, name := range names[offset:end] {
				rows = append(rows, map[string]any{"name": name})
			}
			hasMore := end < len(names)
			var next any
			if hasMore {
				next = fmt.Sprintf("o%d", end)
			}
			writeTestJSON(w, http.StatusOK, map[string]any{"data": rows, "next_cursor": next, "has_more": hasMore})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/schema"):
			rid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/datasets/"), "/schema")
			if _, known := state.branches[rid]; !known {
				writeTestJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "dataset not found"})
				return
			}
			schema, ok := state.schemas[rid]
			if !ok {
				writeTestJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "schema not found"})
				return
			}
			writeTestJSON(w, http.StatusOK, map[string]any{"schema": json.RawMessage(schema), "branch": "main"})
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/transactions") && strings.HasSuffix(r.URL.Path, ":abort"):
			state.aborts = append(state.aborts, r.URL.Path)
			writeTestJSON(w, http.StatusOK, map[string]string{"status": "ABORTED"})
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/transactions"):
			state.openCalls++
			if state.openStatus != http.StatusCreated {
				writeTestJSON(w, state.openStatus, map[string]string{"error": "conflict", "message": "branch already has an OPEN transaction"})
				return
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			state.lastOpenBody = body
			writeTestJSON(w, http.StatusCreated, map[string]any{"id": "11111111-2222-3333-4444-555555555555", "branch_name": "main"})
		default:
			writeTestJSON(w, http.StatusNotFound, map[string]string{"error": "not_found", "message": "route not found"})
		}
	}))
	t.Cleanup(server.Close)
	return server, state
}

type fakeDatasetState struct {
	branches     map[string][]string
	schemas      map[string]string
	openStatus   int
	openCalls    int
	lastAuth     string
	lastOpenBody map[string]any
	paths        []string
	aborts       []string
	pageClamp    int
}

func writeTestJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func bearerCtx(ctx context.Context) context.Context {
	return contextWithCallerBearer(ctx, "caller-token")
}

const testDatasetRID = "ri.foundry.main.dataset.99999999-1111-2222-3333-444444444444"

func TestDatasetHTTPAdapterForwardsCallerAndUsesV1RIDRoutes(t *testing.T) {
	server, state := newFakeDatasetService(t)
	state.branches[testDatasetRID] = []string{"main", "release"}
	state.schemas[testDatasetRID] = `{"columns":[{"name":"sku"}]}`
	adapter := DatasetHTTPVersioning{BaseURL: server.URL, Client: server.Client()}

	// Bare UUID canonicalizes to the RID form; the read-only paginated
	// route is used (never the EnsureDefaultBranch UUID variant).
	branches, err := adapter.ListBranches(bearerCtx(t.Context()), "99999999-1111-2222-3333-444444444444")
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, "Bearer caller-token", state.lastAuth)
	require.Contains(t, state.paths[len(state.paths)-1], "/v1/datasets/"+testDatasetRID+"/branches")

	schema, err := adapter.ViewSchema(bearerCtx(t.Context()), testDatasetRID, "main")
	require.NoError(t, err)
	require.Contains(t, string(schema), "sku")

	rid, err := adapter.OpenTransaction(bearerCtx(t.Context()), testDatasetRID, "release")
	require.NoError(t, err)
	require.Equal(t, "ri.foundry.main.transaction.11111111-2222-3333-4444-555555555555", rid)
	require.Equal(t, "UPDATE", state.lastOpenBody["type"])
	require.Contains(t, state.paths[len(state.paths)-1], "/release/transactions")

	require.NoError(t, adapter.AbortTransaction(bearerCtx(t.Context()), testDatasetRID, "release", rid))
	require.Len(t, state.aborts, 1)
	require.Contains(t, state.aborts[0], ":abort")
}

func TestDatasetHTTPAdapterFailsClosedWithoutCaller(t *testing.T) {
	server, _ := newFakeDatasetService(t)
	adapter := DatasetHTTPVersioning{BaseURL: server.URL, Client: server.Client()}
	_, err := adapter.ListBranches(t.Context(), testDatasetRID)
	require.ErrorIs(t, err, ErrDatasetCallerRequired)
}

func TestDatasetHTTPAdapterErrorMapping(t *testing.T) {
	server, state := newFakeDatasetService(t)
	adapter := DatasetHTTPVersioning{BaseURL: server.URL, Client: server.Client()}

	// Invisible dataset: 404 with a non-schema message surfaces as-is.
	_, err := adapter.ViewSchema(bearerCtx(t.Context()), testDatasetRID, "main")
	require.Error(t, err)
	var httpErr *DatasetHTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusNotFound, httpErr.Status)
	require.False(t, httpErr.Upstream)

	// Explicit schema-not-found (parent visible, branch known) maps to
	// the compatible empty schema.
	state.branches[testDatasetRID] = []string{"main"}
	empty, err := adapter.ViewSchema(bearerCtx(t.Context()), testDatasetRID, "main")
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(empty))

	// Conflict on open → 409 passthrough.
	state.branches[testDatasetRID] = []string{"main"}
	state.openStatus = http.StatusConflict
	_, err = adapter.OpenTransaction(bearerCtx(t.Context()), testDatasetRID, "main")
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusConflict, httpErr.Status)

	// Invalid references are deterministic 400s before any request.
	_, err = adapter.ListBranches(bearerCtx(t.Context()), "not-a-rid")
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Status)
}

func TestDatasetHTTPAdapterUpstream5xxIsUnavailableNotUnsupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(w, http.StatusInternalServerError, map[string]string{"error": "boom"})
	}))
	t.Cleanup(server.Close)
	adapter := DatasetHTTPVersioning{BaseURL: server.URL, Client: &http.Client{Timeout: 5 * time.Second}}
	_, err := adapter.ListBranches(bearerCtx(t.Context()), testDatasetRID)
	var httpErr *DatasetHTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusInternalServerError, httpErr.Status, "original status preserved")
	require.True(t, httpErr.Upstream, "5xx is upstream unavailability, mapped to 503 by the HTTP error writer")
}

// TestDatasetHTTPAdapterFollowsFullPagination uses a fake that clamps
// every page to a single row: the adapter must follow has_more/next_cursor
// until complete instead of silently truncating.
func TestDatasetHTTPAdapterFollowsFullPagination(t *testing.T) {
	server, state := newFakeDatasetService(t)
	state.branches[testDatasetRID] = []string{"main", "release", "hotfix"}
	state.pageClamp = 1
	adapter := DatasetHTTPVersioning{BaseURL: server.URL, Client: server.Client()}
	branches, err := adapter.ListBranches(bearerCtx(t.Context()), testDatasetRID)
	require.NoError(t, err)
	require.Len(t, branches, 3)
	require.Equal(t, []string{"main", "release", "hotfix"}, []string{branches[0].Name, branches[1].Name, branches[2].Name})
}

// TestDatasetHTTPAdapterRejectsRedirects proves the caller JWT never
// follows a redirect to another origin: a 302 surfaces as an error.
func TestDatasetHTTPAdapterRejectsRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A policy-violating client would re-send the bearer here.
		state_redirectSawAuth = r.Header.Get("Authorization")
		writeTestJSON(w, http.StatusOK, map[string]any{"schema": json.RawMessage(`{}`)})
	}))
	t.Cleanup(target.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL+"/v1/datasets/elsewhere/branches")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(server.Close)
	adapter := DatasetHTTPVersioning{BaseURL: server.URL, Client: server.Client()}
	_, err := adapter.ListBranches(bearerCtx(t.Context()), testDatasetRID)
	require.Error(t, err, "redirect must not be followed silently")
	require.Empty(t, state_redirectSawAuth, "bearer must not leak to the redirect target")
	var httpErr *DatasetHTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusFound, httpErr.Status)
}

var state_redirectSawAuth string

// TestDatasetHTTPAdapterSchema200MustCarrySchema: a 200 without the
// schema envelope is a bad_response, never a silent empty schema.
func TestDatasetHTTPAdapterSchema200MustCarrySchema(t *testing.T) {
	raw200 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(raw200.Close)
	adapter200 := DatasetHTTPVersioning{BaseURL: raw200.URL, Client: raw200.Client()}
	_, err := adapter200.ViewSchema(bearerCtx(t.Context()), testDatasetRID, "main")
	var httpErr *DatasetHTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, "bad_response", httpErr.Code)
	require.Empty(t, state_redirectSawAuth)
}
