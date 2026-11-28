package proxy_test

import (
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
	"github.com/openfoundry/openfoundry-go/services/edge-gateway-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/edge-gateway-service/internal/proxy"
)

// The object adapter mounts /api/v1/ontology/types/{type_id}/objects[/...] behind the gateway.
// These tests hold the boundary the gateway keeps there: a verified identity of an organization —
// the tenant the adapter partitions by — and ontology:write for everything that is not a read.
const (
	objectCollection = "/api/v1/ontology/types/ProductCost/objects"
	objectID         = "0196a1b2-0000-7000-8000-000000000001"
	objectTenant     = "x-of-tenant"
)

type route struct {
	method, path string
}

// objectReadRoutes and objectWriteRoutes are the six routes the object adapter mounts
// (object-database-service, internal/server/server.go), by what they do to the objects.
var (
	objectReadRoutes = []route{
		{http.MethodGet, objectCollection},                  // list
		{http.MethodGet, objectCollection + "/" + objectID}, // fetch
		{http.MethodPost, objectCollection + "/query"},      // query
	}
	objectWriteRoutes = []route{
		{http.MethodPost, objectCollection},                    // create
		{http.MethodPatch, objectCollection + "/" + objectID},  // update
		{http.MethodDelete, objectCollection + "/" + objectID}, // delete
	}
)

// forwarded is one request as the gateway sent it upstream.
type forwarded struct {
	Method  string
	Path    string
	Tenants []string // every x-of-tenant value, in order
}

// recordingUpstream stands in for an upstream: it records what the gateway
// forwarded and answers 200.
type recordingUpstream struct {
	*httptest.Server
	mu   sync.Mutex
	seen []forwarded
}

func newRecordingUpstream(t *testing.T) *recordingUpstream {
	t.Helper()
	up := &recordingUpstream{}
	up.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up.mu.Lock()
		up.seen = append(up.seen, forwarded{
			Method:  r.Method,
			Path:    r.URL.Path,
			Tenants: append([]string(nil), r.Header.Values(objectTenant)...),
		})
		up.mu.Unlock()
		w.Header().Set("X-Recorded-Upstream", "true")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)
	return up
}

func (u *recordingUpstream) requests() []forwarded {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]forwarded(nil), u.seen...)
}

// objectGateway is the gateway with the object adapter's upstream apart from the
// other ontology upstreams the route table sends object-looking paths to.
type objectGateway struct {
	*httptest.Server
	jwt     *authmw.JWTConfig
	objects *recordingUpstream
	others  *recordingUpstream
}

func newObjectGateway(t *testing.T) *objectGateway {
	t.Helper()
	return newObjectGatewayAt(t, newRecordingUpstream(t), newRecordingUpstream(t))
}

// newObjectGatewayAt wires the object adapter's upstream and every other ontology
// upstream the route table can pick; the two may be the same server.
func newObjectGatewayAt(t *testing.T, objects, others *recordingUpstream) *objectGateway {
	t.Helper()
	cfg := &config.Config{}
	cfg.Service.Name = "edge-gateway-service"
	cfg.JWT.Secret = "test-secret-32bytes-test-secret-3"
	gw := &objectGateway{jwt: authmw.NewJWTConfig(cfg.JWT.Secret), objects: objects, others: others}
	cfg.Upstream = config.UpstreamURLs{
		ObjectDatabase:     objects.URL,
		OntologyActions:    others.URL,
		OntologyQuery:      others.URL,
		OntologyDefinition: others.URL,
	}
	gw.Server = httptest.NewServer(proxy.NewHandler(cfg, gw.jwt))
	t.Cleanup(gw.Close)
	return gw
}

// answer is what the gateway did with one request: the status, the code of its
// error envelope, and whether it forwarded the request instead of answering.
type answer struct {
	Status    int
	Code      string
	Forwarded bool
}

func (g *objectGateway) ask(t *testing.T, method, path, token string, header http.Header) answer {
	t.Helper()
	req, err := http.NewRequest(method, g.URL+path, strings.NewReader(`{"properties":{"sku":"0007"}}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, values := range header {
		req.Header[key] = values
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	got := answer{Status: resp.StatusCode, Forwarded: resp.Header.Get("X-Recorded-Upstream") == "true"}
	if !got.Forwarded {
		var envelope struct {
			Error struct{ Code string }
		}
		require.NoError(t, json.Unmarshal(body, &envelope), string(body))
		got.Code = envelope.Error.Code
	}
	return got
}

func (g *objectGateway) sign(t *testing.T, jwt *authmw.JWTConfig, c *authmw.Claims) string {
	t.Helper()
	tok, err := authmw.EncodeToken(jwt, c)
	require.NoError(t, err)
	return tok
}

// memberClaims is a member of org (nil = no organization) holding permissions.
func memberClaims(org *uuid.UUID, permissions ...string) *authmw.Claims {
	return &authmw.Claims{
		Sub:         uuid.New(),
		IAT:         time.Now().Unix(),
		EXP:         time.Now().Add(time.Hour).Unix(),
		JTI:         uuid.New(),
		Email:       "user@example.com",
		Name:        "User",
		Roles:       []string{"member"},
		OrgID:       org,
		Permissions: permissions,
	}
}

// adminClaims is an admin of org, scoped when scope is set.
func adminClaims(org *uuid.UUID, scope *authmw.SessionScope) *authmw.Claims {
	c := memberClaims(org)
	c.Roles, c.SessionScope = []string{"admin"}, scope
	return c
}

func scopedClaims(org *uuid.UUID, scope *authmw.SessionScope, permissions ...string) *authmw.Claims {
	c := memberClaims(org, permissions...)
	c.SessionScope = scope
	return c
}

func TestObjectAdapterDeniesUnverifiedIdentity(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	org := uuid.New()
	expired := memberClaims(&org)
	expired.IAT, expired.EXP = time.Now().Add(-2*time.Hour).Unix(), time.Now().Add(-time.Hour).Unix()
	foreignKey := authmw.NewJWTConfig("another-secret-32bytes-at-least-32")

	identities := []struct {
		name, authorization string
	}{
		{"no token", ""},
		{"malformed token", "not-a-jwt"},
		{"token signed with another key", gw.sign(t, foreignKey, memberClaims(&org))},
		{"expired token", gw.sign(t, gw.jwt, expired)},
	}
	for _, identity := range identities {
		for _, r := range append(append([]route{}, objectReadRoutes...), objectWriteRoutes...) {
			got := gw.ask(t, r.method, r.path, identity.authorization, nil)
			assert.Equal(t, answer{Status: http.StatusUnauthorized, Code: "authentication_required"}, got,
				"%s: %s %s", identity.name, r.method, r.path)
		}
	}
	assert.Empty(t, gw.objects.requests(), "nothing unverified may reach the object adapter")
}

func TestObjectAdapterDeniesIdentityWithoutOrganization(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	token := gw.sign(t, gw.jwt, memberClaims(nil))

	for _, r := range append(append([]route{}, objectReadRoutes...), objectWriteRoutes...) {
		got := gw.ask(t, r.method, r.path, token, nil)
		assert.Equal(t, answer{Status: http.StatusForbidden, Code: "organization_required"}, got,
			"%s %s", r.method, r.path)
	}
	assert.Empty(t, gw.objects.requests())
}

func TestObjectAdapterReaderCanRead(t *testing.T) {
	t.Parallel()
	for _, permissions := range [][]string{{}, {"ontology:read"}} {
		t.Run("permissions "+strings.Join(permissions, ","), func(t *testing.T) {
			t.Parallel()
			gw := newObjectGateway(t)
			org := uuid.New()
			token := gw.sign(t, gw.jwt, memberClaims(&org, permissions...))

			for _, r := range objectReadRoutes {
				got := gw.ask(t, r.method, r.path, token, nil)
				assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, got, "%s %s", r.method, r.path)
			}
			sent := gw.objects.requests()
			require.Len(t, sent, len(objectReadRoutes))
			for i, r := range objectReadRoutes {
				assert.Equal(t, route{sent[i].Method, sent[i].Path}, r)
				assert.Equal(t, []string{org.String()}, sent[i].Tenants, "bound to the verified organization")
			}
		})
	}
}

func TestObjectAdapterBindsTheVerifiedOrganization(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	org, other := uuid.New(), uuid.New()
	token := gw.sign(t, gw.jwt, memberClaims(&org))

	// A tenant of the client's own, under both spellings of the header.
	clientTenant := http.Header{}
	clientTenant["x-of-tenant"] = []string{other.String()}
	clientTenant["X-OF-TENANT"] = []string{"default"}

	got := gw.ask(t, http.MethodGet, objectCollection, token, clientTenant)
	assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, got)
	sent := gw.objects.requests()
	require.Len(t, sent, 1)
	assert.Equal(t, []string{org.String()}, sent[0].Tenants)
}

func TestObjectAdapterReaderCannotWrite(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	org := uuid.New()
	// The writes, and a method the adapter does not mount.
	writes := append(append([]route{}, objectWriteRoutes...), route{http.MethodPut, objectCollection + "/" + objectID})

	for _, permissions := range [][]string{{}, {"ontology:read"}, {"datasets:write"}, {"pipelines:read"}} {
		token := gw.sign(t, gw.jwt, memberClaims(&org, permissions...))
		for _, r := range writes {
			got := gw.ask(t, r.method, r.path, token, nil)
			assert.Equal(t, answer{Status: http.StatusForbidden, Code: "object_write_forbidden"}, got,
				"permissions %v: %s %s", permissions, r.method, r.path)
		}
	}
	assert.Empty(t, gw.objects.requests(), "no denied write may reach the object adapter")
}

func TestObjectAdapterWriterCanWrite(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	org := uuid.New()
	writers := []struct {
		name   string
		claims *authmw.Claims
	}{
		{"ontology:write", memberClaims(&org, "ontology:write")},
		{"ontology:*", memberClaims(&org, "ontology:*")},
		{"*:*", memberClaims(&org, "*:*")},
		{"admin role", adminClaims(&org, nil)},
	}
	for _, w := range writers {
		token := gw.sign(t, gw.jwt, w.claims)
		for _, r := range objectWriteRoutes {
			got := gw.ask(t, r.method, r.path, token, nil)
			assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, got, "%s: %s %s", w.name, r.method, r.path)
		}
	}
	sent := gw.objects.requests()
	require.Len(t, sent, len(writers)*len(objectWriteRoutes))
	for _, f := range sent {
		assert.Equal(t, []string{org.String()}, f.Tenants)
	}
}

func TestObjectAdapterSessionScopeStillApplies(t *testing.T) {
	t.Parallel()
	org, other := uuid.New(), uuid.New()
	inScope := []string{"/api/v1/ontology/types/"}

	t.Run("method and path are checked first", func(t *testing.T) {
		t.Parallel()
		gw := newObjectGateway(t)
		token := gw.sign(t, gw.jwt, scopedClaims(&org, &authmw.SessionScope{
			AllowedMethods:      []string{http.MethodGet},
			AllowedPathPrefixes: inScope,
		}))
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true},
			gw.ask(t, http.MethodGet, objectCollection, token, nil))
		// A read, but outside the methods of the scope.
		assert.Equal(t, answer{Status: http.StatusForbidden, Code: "scoped_session_method_denied"},
			gw.ask(t, http.MethodPost, objectCollection+"/query", token, nil))
		assert.Equal(t, answer{Status: http.StatusForbidden, Code: "scoped_session_path_denied"},
			gw.ask(t, http.MethodGet, "/api/v1/pipelines", token, nil))
		require.Len(t, gw.objects.requests(), 1)
	})

	t.Run("organization outside the scope's organizations", func(t *testing.T) {
		t.Parallel()
		gw := newObjectGateway(t)
		token := gw.sign(t, gw.jwt, scopedClaims(&org, &authmw.SessionScope{AllowedOrgIDs: []uuid.UUID{other}}))
		for _, r := range append(append([]route{}, objectReadRoutes...), objectWriteRoutes...) {
			assert.Equal(t, answer{Status: http.StatusForbidden, Code: "scoped_session_org_denied"},
				gw.ask(t, r.method, r.path, token, nil), "%s %s", r.method, r.path)
		}
		assert.Empty(t, gw.objects.requests())
	})

	t.Run("organization inside the scope's organizations", func(t *testing.T) {
		t.Parallel()
		gw := newObjectGateway(t)
		token := gw.sign(t, gw.jwt, scopedClaims(&org, &authmw.SessionScope{AllowedOrgIDs: []uuid.UUID{other, org}}))
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true},
			gw.ask(t, http.MethodGet, objectCollection, token, nil))
	})

	t.Run("an admin is allowed any organization, as the auth library decides", func(t *testing.T) {
		t.Parallel()
		gw := newObjectGateway(t)
		c := adminClaims(&org, &authmw.SessionScope{AllowedOrgIDs: []uuid.UUID{other}})
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true},
			gw.ask(t, http.MethodGet, objectCollection, gw.sign(t, gw.jwt, c), nil))
	})
}

func TestObjectAdapterAllZeroOrganizationIsAnOrganization(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	// docs/poc-online-retail/README.md issues its identities with the all-zero UUID as org_id.
	zero := uuid.Nil
	reader := gw.sign(t, gw.jwt, memberClaims(&zero))
	writer := gw.sign(t, gw.jwt, memberClaims(&zero, "ontology:write"))

	for _, r := range objectReadRoutes {
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, gw.ask(t, r.method, r.path, reader, nil),
			"%s %s", r.method, r.path)
	}
	for _, r := range objectWriteRoutes {
		assert.Equal(t, answer{Status: http.StatusForbidden, Code: "object_write_forbidden"},
			gw.ask(t, r.method, r.path, reader, nil), "%s %s", r.method, r.path)
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, gw.ask(t, r.method, r.path, writer, nil),
			"%s %s", r.method, r.path)
	}
	sent := gw.objects.requests()
	require.Len(t, sent, len(objectReadRoutes)+len(objectWriteRoutes))
	for _, f := range sent {
		assert.Equal(t, []string{"00000000-0000-0000-0000-000000000000"}, f.Tenants)
	}
}

func TestObjectAdapterJudgesThePathItForwards(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)
	org := uuid.New()
	reader := gw.sign(t, gw.jwt, memberClaims(&org))

	// The gateway forwards the decoded path, which is the path the adapter routes on.
	// These differ from it only on the wire, or sit deeper in the collection.
	for _, path := range []string{
		objectCollection + "/",
		objectCollection + "/" + objectID + "/",
		objectCollection + "/" + objectID + "/links",
		objectCollection + "%2F" + objectID,
		objectCollection + "/../objects/" + objectID,
		objectCollection + "//" + objectID,
	} {
		assert.Equal(t, answer{Status: http.StatusUnauthorized, Code: "authentication_required"},
			gw.ask(t, http.MethodGet, path, "", nil), "%s without an identity", path)
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true},
			gw.ask(t, http.MethodGet, path, reader, nil), "GET %s", path)
		assert.Equal(t, answer{Status: http.StatusForbidden, Code: "object_write_forbidden"},
			gw.ask(t, http.MethodPost, path, reader, nil), "POST %s", path)
	}
	for _, f := range gw.objects.requests() {
		assert.Equal(t, []string{org.String()}, f.Tenants, "GET %s", f.Path)
	}
}

func TestObjectAdapterBoundaryLeavesOtherRoutesAlone(t *testing.T) {
	t.Parallel()
	gw := newObjectGateway(t)

	// Object-looking paths the route table sends to other upstreams, and the
	// object database's own non-bridge route: as before, anonymous requests are
	// forwarded and the gateway sets no tenant.
	for _, r := range []route{
		{http.MethodGet, "/api/v1/ontology/types/ProductCost"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/rules"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/objects/knn"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/objects/inline-edit"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/objects/" + objectID + "/inline-edit/"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/objects/inline-edit-batch"},
		{http.MethodGet, "/api/v1/ontology/links/" + objectID + "/instances/" + objectID},
	} {
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, gw.ask(t, r.method, r.path, "", nil),
			"%s %s", r.method, r.path)
	}
	for _, f := range append(gw.others.requests(), gw.objects.requests()...) {
		assert.Empty(t, f.Tenants, "%s %s", f.Method, f.Path)
	}
}

func TestObjectAdapterBoundaryFollowsTheRouteNotTheAddress(t *testing.T) {
	t.Parallel()
	// The object database and the services whose routes only look like objects may share
	// one address. What decides is the route table's branch: the neighbors keep what they
	// had — no identity asked for, no tenant set — and the adapter's own routes behind the
	// same address are gated as before.
	shared := newRecordingUpstream(t)
	gw := newObjectGatewayAt(t, shared, shared)
	org := uuid.New()
	reader := gw.sign(t, gw.jwt, memberClaims(&org))

	neighbors := []route{
		{http.MethodPost, objectCollection + "/knn"},
		{http.MethodPost, objectCollection + "/" + objectID + "/inline-edit"},
		{http.MethodPatch, objectCollection + "/" + objectID + "/inline-edit"},
		{http.MethodPost, objectCollection + "/" + objectID + "/inline-edit/"},
		{http.MethodPost, objectCollection + "/inline-edit-batch"},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/rules"},
	}
	for _, r := range neighbors {
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, gw.ask(t, r.method, r.path, "", nil),
			"anonymous %s %s", r.method, r.path)
		assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true}, gw.ask(t, r.method, r.path, reader, nil),
			"reader %s %s", r.method, r.path)
	}
	sent := shared.requests()
	require.Len(t, sent, 2*len(neighbors))
	for _, f := range sent {
		assert.Empty(t, f.Tenants, "%s %s", f.Method, f.Path)
	}

	// The adapter's own routes, behind that same address.
	assert.Equal(t, answer{Status: http.StatusUnauthorized, Code: "authentication_required"},
		gw.ask(t, http.MethodGet, objectCollection, "", nil), "anonymous GET of the collection")
	assert.Equal(t, answer{Status: http.StatusForbidden, Code: "object_write_forbidden"},
		gw.ask(t, http.MethodPost, objectCollection, reader, nil), "reader POST to the collection")
	assert.Equal(t, answer{Status: http.StatusOK, Forwarded: true},
		gw.ask(t, http.MethodGet, objectCollection, reader, nil), "reader GET of the collection")
	sent = shared.requests()
	require.Len(t, sent, 2*len(neighbors)+1)
	assert.Equal(t, []string{org.String()}, sent[len(sent)-1].Tenants, "bound to the verified organization")
}
