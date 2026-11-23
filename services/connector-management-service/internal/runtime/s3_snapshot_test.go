package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotTestOptions(endpoint string) S3SnapshotOptions {
	return S3SnapshotOptions{
		Endpoint: endpoint, Region: "us-east-1", Bucket: "orders", ObjectKey: "incoming/café+1.ndjson",
		AccessKeyID: "source-access", SecretAccessKey: "source-secret", SessionToken: "source-session",
		AllowedEgressHosts: []string{"127.0.0.1"}, AllowPrivateNetworks: true,
	}
}

func TestFetchS3Snapshot(t *testing.T) {
	const row = `{"order_id":"0007","qty":9007199254740993,"price":1.10}`
	for _, tc := range []struct {
		name, object, expected string
		rows                   int64
		wantError              bool
	}{
		{name: "preserves numeric literals and terminates the last record", object: "\n" + row, expected: row + "\n", rows: 1},
		{name: "empty object"},
		{name: "invalid second row", object: row + "\n{", wantError: true},
		{name: "non-object JSON", object: "[]", wantError: true},
		{name: "invalid UTF-8", object: "{\"id\":\"\xff\"}", wantError: true},
		{name: "row limit", object: strings.Repeat("{}\n", maxS3ObjectRows+1), wantError: true},
		{name: "byte limit", object: strings.Repeat(" ", maxS3ObjectBytes+1), wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/orders/incoming/café+1.ndjson", r.URL.Path)
				assert.Contains(t, r.Header.Get("Authorization"), "Credential=source-access/")
				assert.NotEmpty(t, r.Header.Get("X-Amz-Content-Sha256"))
				assert.Equal(t, "source-session", r.Header.Get("X-Amz-Security-Token"))
				_, _ = fmt.Fprint(w, tc.object)
			}))
			defer server.Close()
			result, err := FetchS3Snapshot(context.Background(), snapshotTestOptions(server.URL))
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, string(result.NDJSON))
			assert.Equal(t, tc.rows, result.Rows)
			assert.Equal(t, int64(len(tc.expected)), result.Bytes)
			assert.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(tc.expected))), result.ContentHash)
		})
	}
}

func TestFetchS3SnapshotRefusesRedirectAndDiscardsUpstreamErrorBody(t *testing.T) {
	redirect := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("redirect target received a request")
	}))
	defer redirect.Close()
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", redirect.URL)
				w.WriteHeader(status)
				_, _ = fmt.Fprintln(w, r.Header.Get("Authorization"), r.Header.Get("X-Amz-Security-Token"))
			}))
			defer server.Close()
			result, err := FetchS3Snapshot(context.Background(), snapshotTestOptions(server.URL))
			require.EqualError(t, err, fmt.Sprintf("S3 GetObject returned HTTP %d", status))
			assert.Nil(t, result)
		})
	}
}

func TestFetchS3SnapshotChecksLeaseBeforeObjectRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("outward read after loss of lease")
	}))
	defer server.Close()
	opts := snapshotTestOptions(server.URL)
	leaseLost := errors.New("lease lost")
	opts.BeforePage = func(context.Context) error { return leaseLost }
	result, err := FetchS3Snapshot(context.Background(), opts)
	require.ErrorIs(t, err, leaseLost)
	assert.Nil(t, result)
}
