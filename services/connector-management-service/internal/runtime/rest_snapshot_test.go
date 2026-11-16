package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// hostnameClient routes requests for a synthetic hostname to an httptest
// server, so egress decisions keyed on the hostname (not the 127.0.0.1
// binding) can be exercised without DNS.
func hostnameClient(server *httptest.Server, host string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if strings.HasPrefix(addr, host+":") {
					dialer := &net.Dialer{}
					return dialer.DialContext(ctx, network, server.Listener.Addr().String())
				}
				var dialer net.Dialer
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
}

func insecureOverride() *models.RESTAPIEgressConfig {
	yes := true
	return &models.RESTAPIEgressConfig{AllowInsecureHTTP: &yes}
}

type snapshotUpstream struct {
	mu sync.Mutex
	// pages holds the raw JSON bodies returned per 1-based request count.
	bodies     []string
	statuses   map[int]int // request count -> status override
	headerLog  []string    // captured Authorization headers
	requestLog []string
}

func (u *snapshotUpstream) handler(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.requestLog = append(u.requestLog, r.URL.String())
	if auth := r.Header.Get("Authorization"); auth != "" {
		u.headerLog = append(u.headerLog, auth)
	}
	idx := len(u.requestLog)
	if status, ok := u.statuses[idx]; ok {
		if status >= 300 && status < 400 {
			w.Header().Set("Location", "http://elsewhere.example.com/items")
		}
		w.WriteHeader(status)
		return
	}
	if idx-1 < len(u.bodies) {
		_, _ = w.Write([]byte(u.bodies[idx-1]))
		return
	}
	_, _ = w.Write([]byte(`{"data":[]}`))
}

func (u *snapshotUpstream) requests() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.requestLog...)
}

func pageConfig(resourcePath, recordsPath, paginationType string) models.RESTAPISyncConfig {
	cfg := models.RESTAPISyncConfig{ResourcePath: resourcePath, RecordsPath: recordsPath}
	cfg.Pagination.Type = paginationType
	return cfg
}

func hashOf(b []byte) string {
	digest := sha256.Sum256(b)
	return fmt.Sprintf("%x", digest[:])
}

func TestFetchRESTSnapshotPagePaginationReadsAllPages(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{
		`{"data":[{"sku":"001","qty":1},{"sku":"002","qty":2}]}`,
		`{"data":[{"sku":"003","qty":3},{"sku":"004","qty":4}]}`,
		`{"data":[{"sku":"005","qty":5},{"sku":"006","qty":6}]}`,
		`{"data":[]}`,
	}, statuses: map[int]int{}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	result, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.NoError(t, err)

	expected := strings.Join([]string{
		`{"sku":"001","qty":1}`, `{"sku":"002","qty":2}`,
		`{"sku":"003","qty":3}`, `{"sku":"004","qty":4}`,
		`{"sku":"005","qty":5}`, `{"sku":"006","qty":6}`,
	}, "\n") + "\n"
	assert.Equal(t, expected, string(result.NDJSON), "records must be preserved verbatim (leading zeroes intact)")
	assert.Equal(t, int64(6), result.Rows)
	assert.Equal(t, int64(len(expected)), result.Bytes)
	assert.Equal(t, hashOf([]byte(expected)), result.ContentHash)
	require.Len(t, upstream.requests(), 4)
	assert.Contains(t, upstream.requests()[0], "page=1")
	assert.Contains(t, upstream.requests()[1], "page=2")
}

func TestFetchRESTSnapshotCursorPaginationReadsAllPages(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{
		`{"items":[{"id":"a1"}],"next_cursor":"c2"}`,
		`{"items":[{"id":"a2"}],"next_cursor":"c3"}`,
		`{"items":[{"id":"a3"}],"next_cursor":""}`,
	}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/rows", "items", models.RESTAPISyncPaginationCursor)
	cfg.Pagination.NextCursorPath = "next_cursor"
	result, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Rows)
	assert.Equal(t, `{"id":"a1"}`+"\n"+`{"id":"a2"}`+"\n"+`{"id":"a3"}`+"\n", string(result.NDJSON))
	requests := upstream.requests()
	require.Len(t, requests, 3)
	assert.Contains(t, requests[1], "cursor=c2")
	assert.Contains(t, requests[2], "cursor=c3")
}

func TestFetchRESTSnapshotCursorLoopFails(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{
		`{"items":[{"id":"a1"}],"next_cursor":"same"}`,
		`{"items":[{"id":"a2"}],"next_cursor":"same"}`,
	}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/rows", "items", models.RESTAPISyncPaginationCursor)
	cfg.Pagination.NextCursorPath = "next_cursor"
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pagination loop")
}

func TestFetchRESTSnapshotPageLimitExceededFailsExplicitly(t *testing.T) {
	// The source always returns a full page; max_pages stops the pull and the
	// snapshot must fail instead of being truncated.
	upstream := &snapshotUpstream{bodies: []string{
		`{"data":[{"i":1}]}`, `{"data":[{"i":2}]}`, `{"data":[{"i":3}]}`,
	}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	cfg.Limits.MaxPages = 2
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_pages")
	assert.Len(t, upstream.requests(), 2, "the limit must stop further requests")
}

func TestFetchRESTSnapshotMaxRecordsFailsExplicitly(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{
		`{"data":[{"i":1},{"i":2}]}`,
		`{"data":[{"i":3}]}`,
		`{"data":[]}`,
	}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	cfg.Limits.MaxRecords = 2
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_records")
}

func TestFetchRESTSnapshotUpstreamFailurePropagates(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{
		`{"data":[{"i":1}]}`,
	}, statuses: map[int]int{2: 500}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestFetchRESTSnapshotInvalidJSONFails(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[not json`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationNone)
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "records_path")
}

func TestFetchRESTSnapshotRedirectRejected(t *testing.T) {
	upstream := &snapshotUpstream{statuses: map[int]int{1: 302}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationNone)
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redirect")
}

func TestFetchRESTSnapshotResponseSizeLimitFails(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[` + strings.Repeat(`{"pad":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, 4) + `]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationNone)
	cfg.Limits.MaxResponseBytes = 32
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_response_bytes")
}

func TestFetchRESTSnapshotEmptyResultYieldsEmptyNDJSON(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	result, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{BaseURL: server.URL, Sync: cfg})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Rows)
	assert.Equal(t, int64(0), result.Bytes)
	assert.NotEmpty(t, result.ContentHash, "empty snapshots still carry the hash of empty content")
	assert.Equal(t, hashOf(nil), result.ContentHash)
}

func TestFetchRESTSnapshotAppliesSourceCredentials(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	result, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{
		BaseURL: server.URL,
		Sync:    cfg,
		Auth:    RESTSnapshotAuth{Type: models.RESTAPIAuthBearer, Token: "source-secret"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer source-secret"}, upstream.headerLog)
	_ = result
}

func TestFetchRESTSnapshotEgressAllowlistBlocksOtherHosts(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", models.RESTAPISyncPaginationPage)
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{
		BaseURL:            server.URL,
		Sync:               cfg,
		AllowedEgressHosts: []string{"api.allowed.example.com"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not allow host")
}


func TestFetchRESTSnapshotHostnameHTTPDefaultRejected(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	_, err := FetchRESTSnapshot(context.Background(), hostnameClient(server, "fixture.local"), RESTSnapshotOptions{
		BaseURL: "http://fixture.local",
		Sync:    cfg,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insecure HTTP", "plain-HTTP hostnames stay rejected by default")
	assert.Empty(t, upstream.requests())
}

func TestFetchRESTSnapshotHostnameHTTPExplicitOverrideAllowed(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[{"i":1}]}`, `{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	result, err := FetchRESTSnapshot(context.Background(), hostnameClient(server, "fixture.local"), RESTSnapshotOptions{
		BaseURL: "http://fixture.local",
		Sync:    cfg,
		Egress:  insecureOverride(),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Rows)
}

func TestFetchRESTSnapshotSourceAllowlistStillConstrainsOverride(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	egress := insecureOverride()
	egress.AllowedHosts = []string{"api.example.com"}
	_, err := FetchRESTSnapshot(context.Background(), hostnameClient(server, "fixture.local"), RESTSnapshotOptions{
		BaseURL: "http://fixture.local",
		Sync:    cfg,
		Egress:  egress,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not allow host")
	assert.Contains(t, err.Error(), "source egress override", "the source allowlist narrows even with the HTTP override")
	assert.Empty(t, upstream.requests())
}

func TestFetchRESTSnapshotBlockedHostStillRejectedDespiteOverride(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	egress := insecureOverride()
	egress.BlockedHosts = []string{"fixture.local"}
	_, err := FetchRESTSnapshot(context.Background(), hostnameClient(server, "fixture.local"), RESTSnapshotOptions{
		BaseURL: "http://fixture.local",
		Sync:    cfg,
		Egress:  egress,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blocks host", "blocked_hosts always wins")
	assert.Empty(t, upstream.requests())
}

func TestFetchRESTSnapshotSourceAllowlistCannotWidenGlobal(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	egress := insecureOverride()
	egress.AllowedHosts = []string{"fixture.local"} // the source tries to bless a host the global allowlist never allowed
	_, err := FetchRESTSnapshot(context.Background(), hostnameClient(server, "fixture.local"), RESTSnapshotOptions{
		BaseURL:              "http://fixture.local",
		Sync:                 cfg,
		AllowedEgressHosts:   []string{"allowed.example.com"},
		AllowPrivateNetworks: true,
		Egress:               egress,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not allow host")
	assert.NotContains(t, err.Error(), "source egress override", "the GLOBAL ceiling must reject, not the source narrowing")
	assert.Empty(t, upstream.requests())
}

func TestFetchRESTSnapshotSourcePrivateTrueCannotLiftGlobalProhibition(t *testing.T) {
	cfg := pageConfig("/items", "data", "page")
	egress := insecureOverride()
	yes := true
	egress.AllowPrivateNetworks = &yes // tries to lift the global prohibition below
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{
		BaseURL:              "http://10.1.2.3:9999",
		Sync:                 cfg,
		AllowedEgressHosts:   nil,
		AllowPrivateNetworks: false,
		Egress:               egress,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "private-network", "a source cannot lift the global private-network prohibition")
}

func TestFetchRESTSnapshotFixtureComboSucceeds(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[{"sku":"00100"}]}`, `{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	yes := true
	egress := &models.RESTAPIEgressConfig{AllowInsecureHTTP: &yes, AllowPrivateNetworks: &yes}
	result, err := FetchRESTSnapshot(context.Background(), hostnameClient(server, "fixture.local"), RESTSnapshotOptions{
		BaseURL:              "http://fixture.local",
		Sync:                 cfg,
		AllowPrivateNetworks: true,
		Egress:               egress,
	})
	require.NoError(t, err)
	assert.Equal(t, `{"sku":"00100"}`+"\n", string(result.NDJSON))
}

func TestFetchRESTSnapshotBeforePageGate(t *testing.T) {
	upstream := &snapshotUpstream{bodies: []string{`{"data":[{"sku":"00100"}]}`, `{"data":[]}`}}
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer server.Close()

	// A hook error aborts before ANY upstream request is issued; the error
	// rides along unwiped so the leased-sync caller can classify it.
	gateErr := errors.New("lease fenced")
	_, err := FetchRESTSnapshot(context.Background(), http.DefaultClient, RESTSnapshotOptions{
		BaseURL:    server.URL,
		Sync:       pageConfig("/items", "data", "page"),
		BeforePage: func(context.Context) error { return gateErr },
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, gateErr)
	assert.Empty(t, upstream.requests(), "no page request may be issued after the gate refuses")

	// Gate consulted before every page: allow one, fence the second.
	var calls int
	upstream2 := &snapshotUpstream{bodies: []string{`{"data":[{"sku":"00100"}]}`, `{"data":[{"sku":"00200"}]}`}}
	server2 := httptest.NewServer(http.HandlerFunc(upstream2.handler))
	defer server2.Close()
	_, err = FetchRESTSnapshot(context.Background(), http.DefaultClient, RESTSnapshotOptions{
		BaseURL: server2.URL,
		Sync:    pageConfig("/items", "data", "page"),
		BeforePage: func(context.Context) error {
			calls++
			if calls > 1 {
				return gateErr
			}
			return nil
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, gateErr)
	assert.Equal(t, 1, len(upstream2.requests()), "only the first gated page may be fetched")
}
