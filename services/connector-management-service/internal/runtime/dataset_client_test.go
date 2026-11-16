package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeSyncErrorStripsQueryAndUserinfo(t *testing.T) {
	msg := `Get "http://127.0.0.1:9999/items?client_key=s3cr3t%2Bval%3D%3D&page=2": dial tcp 127.0.0.1:9999: connect: connection refused`
	out := SanitizeSyncError(msg)
	assert.NotContains(t, out, "client_key", "the whole query must go, not just known-sensitive names")
	assert.NotContains(t, out, "s3cr3t", "neither raw nor encoded credential fragments may survive")
	assert.NotContains(t, out, "?", "no query string may remain")
	assert.Contains(t, out, "http://127.0.0.1:9999/items", "host and path stay useful for diagnosis")
	assert.Contains(t, out, "connection refused", "the underlying failure text is preserved")

	basic := `Get "https://alice:hunter2@example.com/items": x`
	out = SanitizeSyncError(basic)
	assert.NotContains(t, out, "alice", "userinfo must be stripped")
	assert.NotContains(t, out, "hunter2")
	assert.Contains(t, out, "https://example.com/items")
}

// datasetCaseServer boots a dataset-versioning stand-in that answers GET
// /api/v1/datasets/{id} with the given canned case.
func datasetCaseServer(t *testing.T, testCase string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch testCase {
		case "ok":
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			w.Write([]byte(fmt.Sprintf(`{"id":%q,"name":"orders"}`, parts[len(parts)-1])))
		case "wrong-id":
			w.Write([]byte(fmt.Sprintf(`{"id":%q,"name":"other"}`, uuid.NewString())))
		case "nil-id":
			w.Write([]byte(`{"name":"anonymous"}`))
		case "401":
			w.WriteHeader(http.StatusUnauthorized)
		case "403":
			w.WriteHeader(http.StatusForbidden)
		case "404":
			w.WriteHeader(http.StatusNotFound)
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
}

func TestHTTPDatasetVersioningClientGetDatasetClassification(t *testing.T) {
	newClient := func(t *testing.T, testCase string) HTTPDatasetVersioningClient {
		server := datasetCaseServer(t, testCase)
		t.Cleanup(server.Close)
		return HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: 2 * time.Second}}
	}
	target := uuid.New()

	t.Run("ok echoes id and forwards jwt", func(t *testing.T) {
		info, err := newClient(t, "ok").GetDataset(context.Background(), target, "Bearer jwt")
		require.NoError(t, err)
		assert.Equal(t, target, info.ID)
	})
	t.Run("wrong id is an invalid dependency response", func(t *testing.T) {
		_, err := newClient(t, "wrong-id").GetDataset(context.Background(), target, "")
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Contains(t, err.Error(), "does not identify")
	})
	t.Run("nil id is an invalid dependency response", func(t *testing.T) {
		_, err := newClient(t, "nil-id").GetDataset(context.Background(), target, "")
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Contains(t, err.Error(), "does not identify")
	})
	t.Run("404 stays invisible", func(t *testing.T) {
		_, err := newClient(t, "404").GetDataset(context.Background(), target, "")
		assert.ErrorIs(t, err, ErrDatasetNotFound)
	})
	t.Run("403 is a caller rejection", func(t *testing.T) {
		_, err := newClient(t, "403").GetDataset(context.Background(), target, "")
		assert.ErrorIs(t, err, ErrDatasetForbidden)
	})
	t.Run("401 is classified separately from 403", func(t *testing.T) {
		_, err := newClient(t, "401").GetDataset(context.Background(), target, "")
		assert.ErrorIs(t, err, ErrDatasetUnauthorized)
	})
	t.Run("5xx is a dependency failure", func(t *testing.T) {
		_, err := newClient(t, "500").GetDataset(context.Background(), target, "")
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 500, dep.Status)
	})
}

func TestHTTPDatasetVersioningClientGetDatasetTimeoutIsDependencyError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer server.Close()
	client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: 50 * time.Millisecond}}
	_, err := client.GetDataset(context.Background(), uuid.New(), "")
	var dep *DependencyError
	require.ErrorAs(t, err, &dep)
	assert.True(t, dep.Timeout, "timeouts are dependency failures, not caller rejections")
	assert.NotContains(t, err.Error(), "?")
}

func TestHTTPDatasetVersioningClientUnconfiguredFailsClosed(t *testing.T) {
	client := HTTPDatasetVersioningClient{}
	_, err := client.GetDataset(context.Background(), uuid.New(), "")
	var dep *DependencyError
	require.ErrorAs(t, err, &dep)
	assert.False(t, errors.Is(err, ErrDatasetNotFound))
	assert.False(t, errors.Is(err, ErrDatasetForbidden))
}

func TestFetchRESTSnapshotTransportErrorNeverLeaksQueryCredentials(t *testing.T) {
	deadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadServer.Close() // unreachable from here on

	secret := `p@ss word+9/8==`
	cfg := pageConfig("/items", "data", "page")
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{
		BaseURL: deadServer.URL,
		Sync:    cfg,
		Auth:    RESTSnapshotAuth{Type: "api_key", QueryParam: "client_key", Token: secret},
	})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret, "raw credential must not leak through url.Error")
	assert.NotContains(t, err.Error(), url.QueryEscape(secret), "URL-encoded credential must not leak")
	assert.NotContains(t, err.Error(), url.PathEscape(secret))
	assert.Contains(t, err.Error(), "connection refused", "the failure cause stays diagnosable")
}

func TestFetchRESTSnapshotTimeoutErrorCarriesNoURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
	}))
	defer server.Close()

	cfg := pageConfig("/items", "data", "page")
	cfg.Limits.RequestTimeoutMS = 100
	_, err := FetchRESTSnapshot(context.Background(), nil, RESTSnapshotOptions{
		BaseURL: server.URL,
		Sync:    cfg,
		Auth:    RESTSnapshotAuth{Type: "api_key", QueryParam: "client_key", Token: "leak-me"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.NotContains(t, err.Error(), "client_key", "timeout errors carry no URL at all")
	assert.NotContains(t, err.Error(), "leak-me")
}
