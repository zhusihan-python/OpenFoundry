package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// incrementalPage is one scripted page of the completion-checkpoint fixture.
type incrementalPage struct {
	body string
}

// newIncrementalFixture serves a cursor-paginated resource and records every
// page request's query so the fixed request_param invariant is assertable.
func newIncrementalFixture(t *testing.T, pages []incrementalPage) (*httptest.Server, *[][]string) {
	t.Helper()
	var mu sync.Mutex
	var queries [][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query()["since"])
		mu.Unlock()
		cursor := r.URL.Query().Get("cursor")
		idx := 0
		if cursor != "" {
			fmt.Sscanf(cursor, "p%d", &idx)
			idx++
		}
		if idx >= len(pages) {
			t.Errorf("unexpected extra page request (cursor=%q)", cursor)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, pages[idx].body)
	}))
	t.Cleanup(server.Close)
	return server, &queries
}

func incrementalFetchOpts(serverURL string, spec *IncrementalFetchSpec) RESTSnapshotOptions {
	return RESTSnapshotOptions{
		BaseURL: serverURL,
		Sync: models.RESTAPISyncConfig{
			ResourcePath: "/items",
			RecordsPath:  "data",
			Pagination:   models.RESTAPISyncPagination{Type: models.RESTAPISyncPaginationCursor, NextCursorPath: "next"},
			Limits:       models.RESTAPISyncLimits{MaxPages: 10, MaxRecords: 1000, MaxResponseBytes: 1 << 20, MaxTotalBytes: 4 << 20},
		},
		Auth:        RESTSnapshotAuth{Type: models.RESTAPIAuthNone},
		Incremental: spec,
	}
}

func TestFetchRESTSnapshotIncrementalProtocol(t *testing.T) {
	server, queries := newIncrementalFixture(t, []incrementalPage{
		{body: `{"data":[{"i":1},{"i":2}],"next":"p0","meta":{"completed":"STALE"}}`},
		{body: `{"data":[{"i":3}],"next":"p1","meta":{"completed":"STALE"}}`},
		{body: `{"data":[],"next":"","meta":{"completed":"T3"}}`},
	})
	opts := incrementalFetchOpts(server.URL, &IncrementalFetchSpec{
		RequestParam:            "since",
		RequestValue:            "T0",
		HasRequestValue:         true,
		CompletedCheckpointPath: "meta.completed",
	})
	result, err := FetchRESTSnapshot(context.Background(), nil, opts)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	// The fixed data-position param is present with the SAME value on EVERY
	// page (input-fixed invariant), while the pagination cursor varies.
	if len(*queries) != 3 {
		t.Fatalf("expected 3 page requests, got %d", len(*queries))
	}
	for i, q := range *queries {
		if len(q) != 1 || q[0] != "T0" {
			t.Fatalf("page %d: since=%v, want exactly [T0]", i, q)
		}
	}
	// Only the FINAL response's completion token is taken — the stale
	// mid-page values ("STALE") must never win.
	if result.CompletedCheckpoint != "T3" || result.CompletedCheckpointLen != 2 {
		t.Fatalf("completed checkpoint = %q(len %d), want T3(2)", result.CompletedCheckpoint, result.CompletedCheckpointLen)
	}
	if result.Rows != 3 {
		t.Fatalf("rows = %d, want 3", result.Rows)
	}
}

func TestFetchRESTSnapshotIncrementalEmptyBatchSameTokenOK(t *testing.T) {
	server, _ := newIncrementalFixture(t, []incrementalPage{
		{body: `{"data":[],"next":"","meta":{"completed":"T0"}}`},
	})
	opts := incrementalFetchOpts(server.URL, &IncrementalFetchSpec{
		RequestParam: "since", RequestValue: "T0", HasRequestValue: true,
		CompletedCheckpointPath: "meta.completed",
	})
	result, err := FetchRESTSnapshot(context.Background(), nil, opts)
	if err != nil {
		t.Fatalf("empty confirmed batch must succeed: %v", err)
	}
	if result.CompletedCheckpoint != "T0" || result.Rows != 0 {
		t.Fatalf("result = %+v, want T3==T0 rows==0", result)
	}
}

func TestFetchRESTSnapshotIncrementalNoProgressFails(t *testing.T) {
	server, _ := newIncrementalFixture(t, []incrementalPage{
		{body: `{"data":[{"i":1}],"next":"","meta":{"completed":"T0"}}`},
	})
	opts := incrementalFetchOpts(server.URL, &IncrementalFetchSpec{
		RequestParam: "since", RequestValue: "T0", HasRequestValue: true,
		CompletedCheckpointPath: "meta.completed",
	})
	_, err := FetchRESTSnapshot(context.Background(), nil, opts)
	if err != ErrIncrementalNoProgress {
		t.Fatalf("records with unchanged candidate must fail with ErrIncrementalNoProgress, got %v", err)
	}
}

func TestFetchRESTSnapshotIncrementalCandidateInvalid(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing", `{"data":[],"next":""}`},
		{"non-string", `{"data":[],"next":"","meta":{"completed":42}}`},
		{"empty string", `{"data":[],"next":"","meta":{"completed":""}}`},
		{"oversize", `{"data":[],"next":"","meta":{"completed":"` + makeOverlong() + `"}}`},
		{"control char", `{"data":[],"next":"","meta":{"completed":"a\u0001b"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newIncrementalFixture(t, []incrementalPage{{body: tc.body}})
			opts := incrementalFetchOpts(server.URL, &IncrementalFetchSpec{
				RequestParam: "since", RequestValue: "T0", HasRequestValue: true,
				CompletedCheckpointPath: "meta.completed",
			})
			_, err := FetchRESTSnapshot(context.Background(), nil, opts)
			if err != ErrIncrementalCandidateInvalid {
				t.Fatalf("want ErrIncrementalCandidateInvalid, got %v", err)
			}
		})
	}
}

func makeOverlong() string {
	out := make([]byte, 513)
	for i := range out {
		out[i] = 'a'
	}
	return string(out)
}

// TestFetchRESTSnapshotIncrementalLossyWireRejected uses REAL malformed wire
// JSON — a lone UTF-16 surrogate escape and a raw invalid UTF-8 byte — both
// of which json.Unmarshal silently coerces to U+FFFD (docs-verification.md).
// The candidate must be rejected, never silently normalized.
func TestFetchRESTSnapshotIncrementalLossyWireRejected(t *testing.T) {
	surrogateBody := `{"data":[],"next":"","meta":{"completed":"a\ud800b"}}`
	rawInvalidBody := []byte(`{"data":[],"next":"","meta":{"completed":"a`)
	rawInvalidBody = append(rawInvalidBody, 0xff)
	rawInvalidBody = append(rawInvalidBody, []byte(`b"}}`)...)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"lone surrogate escape", []byte(surrogateBody)},
		{"raw invalid utf8 byte", rawInvalidBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()
			opts := incrementalFetchOpts(server.URL, &IncrementalFetchSpec{
				RequestParam: "since", RequestValue: "T0", HasRequestValue: true,
				CompletedCheckpointPath: "meta.completed",
			})
			_, err := FetchRESTSnapshot(context.Background(), nil, opts)
			if err != ErrIncrementalCandidateInvalid {
				t.Fatalf("malformed wire candidate must be rejected, got %v", err)
			}
		})
	}
}

// TestFetchRESTSnapshotIncrementalLoopErrorRedactsToken: with a known input
// token and a repeated next value, the loop error must not echo the repeated
// cursor (which may equal/contain confirmed checkpoint material).
func TestFetchRESTSnapshotIncrementalLoopErrorRedactsToken(t *testing.T) {
	const inputToken = "SECRET-INPUT-TOKEN"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		w.Header().Set("Content-Type", "application/json")
		if cursor == "" {
			fmt.Fprint(w, `{"data":[{"i":1}],"next":"SECRET-INPUT-TOKEN"}`)
			return
		}
		// Repeat the same next token -> loop detection on the second page.
		fmt.Fprint(w, `{"data":[{"i":2}],"next":"SECRET-INPUT-TOKEN"}`)
	}))
	defer server.Close()
	opts := incrementalFetchOpts(server.URL, &IncrementalFetchSpec{
		RequestParam: "since", RequestValue: inputToken, HasRequestValue: true,
		CompletedCheckpointPath: "meta.completed",
	})
	_, err := FetchRESTSnapshot(context.Background(), nil, opts)
	if err == nil {
		t.Fatal("repeated cursor must fail")
	}
	if strings.Contains(err.Error(), inputToken) {
		t.Fatalf("loop error must not echo the token material: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "pagination loop") {
		t.Fatalf("unexpected error shape: %v", err)
	}
}

func TestFetchRESTSnapshotIncrementalRequiresCursor(t *testing.T) {
	opts := RESTSnapshotOptions{
		BaseURL: "https://api.example.com",
		Sync: models.RESTAPISyncConfig{
			ResourcePath: "/items",
			Pagination:   models.RESTAPISyncPagination{Type: models.RESTAPISyncPaginationNone},
		},
		Auth:        RESTSnapshotAuth{Type: models.RESTAPIAuthNone},
		Incremental: &IncrementalFetchSpec{RequestParam: "since", CompletedCheckpointPath: "c"},
	}
	if _, err := FetchRESTSnapshot(context.Background(), nil, opts); err == nil {
		t.Fatal("incremental fetch must refuse non-cursor pagination defensively")
	}
}

func TestFetchRESTSnapshotWithoutIncrementalUnchanged(t *testing.T) {
	// A nil Incremental spec must behave exactly like the pre-P2.5 snapshot
	// fetch: no since param is sent and no completion token is produced.
	server, queries := newIncrementalFixture(t, []incrementalPage{
		{body: `{"data":[{"i":1}],"next":""}`},
	})
	result, err := FetchRESTSnapshot(context.Background(), nil, incrementalFetchOpts(server.URL, nil))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(*queries) != 1 || len((*queries)[0]) != 0 {
		t.Fatalf("no since parameter expected, queries=%v", *queries)
	}
	if result.CompletedCheckpoint != "" || result.CompletedCheckpointLen != 0 {
		t.Fatalf("nil spec must not produce a completion checkpoint: %+v", result)
	}
}
