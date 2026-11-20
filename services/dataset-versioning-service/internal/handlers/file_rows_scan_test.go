package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type scanHarness struct {
	h       *handlers.Handlers
	store   *fakeStore
	backing *storageabstraction.LocalBackingFS
	dataset models.Dataset
	branch  models.DatasetBranch
	ownerID uuid.UUID
}

func newScanHarness(t *testing.T) *scanHarness {
	t.Helper()
	owner := uuid.New()
	dataset := models.Dataset{ID: uuid.New(), Name: "orders", ActiveBranch: "main", OwnerID: owner}
	branch := models.DatasetBranch{ID: uuid.New(), DatasetID: dataset.ID, Name: "main"}
	store := &fakeStore{
		datasets:            []models.Dataset{dataset},
		versions:            map[uuid.UUID][]models.DatasetVersion{},
		branches:            map[uuid.UUID][]models.DatasetBranch{dataset.ID: {branch}},
		runtimeTransactions: map[uuid.UUID]models.RuntimeTransaction{},
		stagedFiles:         map[uuid.UUID][]models.StageTransactionFile{},
	}
	backing := &storageabstraction.LocalBackingFS{BaseURL: "http://dataset-versioning.local", BaseDir: "data", RootDir: t.TempDir()}
	return &scanHarness{
		h:       &handlers.Handlers{Repo: store, BackingFS: backing},
		store:   store,
		backing: backing,
		dataset: dataset,
		branch:  branch,
		ownerID: owner,
	}
}

// commitSnapshotVersion mimics what a real outputs:commit with
// content_base64 leaves behind: a server-named transaction object at the
// outputCommitObjectKey layout, the staged file row, a COMMITTED SNAPSHOT
// transaction and the cut dataset version. It returns the new version.
func (hs *scanHarness) commitSnapshotVersion(t *testing.T, logical string, content []byte) int32 {
	t.Helper()
	txnID := uuid.New()
	objectKey := path.Join(hs.backing.BaseDirectory(), "datasets", hs.dataset.ID.String(), "transactions", txnID.String(), logical)
	require.NoError(t, hs.backing.WriteLocalObject(objectKey, content))
	hs.store.stagedFiles[txnID] = []models.StageTransactionFile{{
		LogicalPath: logical, PhysicalPath: objectKey, SizeBytes: int64(len(content)), Operation: models.FileOperationAdd,
	}}
	next := int32(len(hs.store.versions[hs.dataset.ID]) + 1)
	hs.store.runtimeTransactions[txnID] = models.RuntimeTransaction{
		ID: txnID, DatasetID: hs.dataset.ID, BranchID: hs.branch.ID, BranchName: hs.branch.Name,
		TxType: models.TransactionTypeSnapshot, Status: models.TransactionStatusCommitted,
	}
	hs.store.versions[hs.dataset.ID] = append(hs.store.versions[hs.dataset.ID], models.DatasetVersion{
		DatasetID: hs.dataset.ID, Version: next, TransactionID: &txnID,
	})
	return next
}

func (hs *scanHarness) scanRequest(query string, claims *authmw.Claims) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+hs.dataset.ID.String()+"/files/rows:scan"+query, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", hs.dataset.ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	if claims == nil {
		claims = &authmw.Claims{Sub: hs.ownerID}
	}
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	hs.h.ScanDatasetFileRows(rec, req)
	return rec
}

func scanQuery(version int32, logical string, cursor string, limit int) string {
	query := fmt.Sprintf("?branch=main&version=%d&path=%s", version, logical)
	if limit > 0 {
		query += fmt.Sprintf("&limit=%d", limit)
	}
	if cursor != "" {
		query += "&cursor=" + cursor
	}
	return query
}

func decodeScan(t *testing.T, rec *httptest.ResponseRecorder) handlersScanResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out handlersScanResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

// handlersScanResponse mirrors datasetFileScanResponse for test assertions.
type handlersScanResponse struct {
	Branch        string            `json:"branch"`
	Version       int32             `json:"version"`
	TransactionID string            `json:"transaction_id"`
	Path          string            `json:"path"`
	Rows          []json.RawMessage `json:"rows"`
	NextCursor    *string           `json:"next_cursor"`
	RowCount      int               `json:"row_count"`
	PageBytes     int64             `json:"page_bytes"`
	SizeBytes     int64             `json:"size_bytes"`
}

func scanNDJSONLines(n int, prefix string) []string {
	lines := make([]string, n)
	for i := 0; i < n; i++ {
		// Deliberately non-alphabetical key order: the scan must preserve
		// the record's own ordering verbatim.
		lines[i] = fmt.Sprintf(`{"z":%d,"a":"%s-%d","m":{"n":true}}`, i, prefix, i)
	}
	return lines
}

// TestScanPagesThroughLargeSnapshotValueByValue: a committed snapshot larger
// than the legacy 1000-row single-read ceiling pages through rows:scan with
// every record byte-identical, in order, no duplicates, no dropped rows,
// page byte accounting summing to the file size, and page row counts within
// the requested limit.
func TestScanPagesThroughLargeSnapshotValueByValue(t *testing.T) {
	hs := newScanHarness(t)
	lines := scanNDJSONLines(2505, "v1")
	content := []byte(joinLines(lines))
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", content)

	var got []string
	var bytesSum int64
	cursor := ""
	pages := 0
	for {
		rec := hs.scanRequest(scanQuery(version, "part-00000.ndjson", cursor, 1000), nil)
		page := decodeScan(t, rec)
		pages++
		require.LessOrEqual(t, page.RowCount, 1000, "page row cap")
		require.LessOrEqual(t, page.PageBytes, int64(8<<20), "page byte cap")
		for _, raw := range page.Rows {
			got = append(got, string(raw))
		}
		bytesSum += page.PageBytes
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}

	require.GreaterOrEqual(t, pages, 3, "fixed sample must span at least three pages")
	require.Len(t, got, len(lines))
	for i := range lines {
		assert.Equal(t, lines[i], got[i], "record %d must be verbatim and in order", i)
	}
	assert.Equal(t, int64(len(content)), bytesSum, "page bytes must account for the whole file")
}

// TestScanVersionPinningAcrossNewPublish: after fetching page 1 of version
// 1, a new snapshot of the same logical file is published. The old cursor
// keeps reading version 1's bytes; an explicit scan of the new version
// returns the new values; re-scanning version 1 explicitly still returns
// the old values.
func TestScanVersionPinningAcrossNewPublish(t *testing.T) {
	hs := newScanHarness(t)
	v1Lines := scanNDJSONLines(1500, "v1")
	v1 := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(v1Lines)))

	first := decodeScan(t, hs.scanRequest(scanQuery(v1, "part-00000.ndjson", "", 1000), nil))
	require.NotNil(t, first.NextCursor)
	require.Equal(t, "v1-0", jsonObjectValue(t, first.Rows[0], "a"))

	v2Lines := scanNDJSONLines(1500, "v2")
	v2 := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(v2Lines)))
	require.NotEqual(t, v1, v2)

	second := decodeScan(t, hs.scanRequest(scanQuery(v1, "part-00000.ndjson", *first.NextCursor, 1000), nil))
	require.Nil(t, second.NextCursor)
	require.Equal(t, "v1-1000", jsonObjectValue(t, second.Rows[0], "a"), "old cursor must continue the old version's rows")
	require.Len(t, second.Rows, 500)

	freshV2 := decodeScan(t, hs.scanRequest(scanQuery(v2, "part-00000.ndjson", "", 5), nil))
	require.Equal(t, "v2-0", jsonObjectValue(t, freshV2.Rows[0], "a"))

	freshV1 := decodeScan(t, hs.scanRequest(scanQuery(v1, "part-00000.ndjson", "", 5), nil))
	require.Equal(t, "v1-0", jsonObjectValue(t, freshV1.Rows[0], "a"))
}

// TestScanRejectsBadRequests walks the explicit-rejection negative controls.
func TestScanRejectsBadRequests(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte("{\"a\":1}\n{\"a\":2}\n"))

	otherOrg := uuid.New()
	foreignClaims := &authmw.Claims{Sub: uuid.New(), OrgID: &otherOrg}

	cases := []struct {
		name   string
		query  func() string
		claims *authmw.Claims
		status int
		errSub string
	}{
		{name: "missing branch", query: func() string { return "?version=1&path=part-00000.ndjson" }, status: 400, errSub: "branch is required"},
		{name: "version zero", query: func() string { return scanQuery(0, "part-00000.ndjson", "", 0) }, status: 400, errSub: "version must be a positive integer"},
		{name: "version negative", query: func() string { return scanQuery(-3, "part-00000.ndjson", "", 0) }, status: 400, errSub: "version must be a positive integer"},
		{name: "version not a number", query: func() string { return "?branch=main&version=abc&path=part-00000.ndjson" }, status: 400, errSub: "version must be a positive integer"},
		{name: "version missing", query: func() string { return "?branch=main&path=part-00000.ndjson" }, status: 400, errSub: "version must be a positive integer"},
		{name: "path missing", query: func() string { return "?branch=main&version=1" }, status: 400, errSub: "path is required"},
		{name: "limit zero", query: func() string { return scanQuery(version, "part-00000.ndjson", "", 0) + "&limit=0" }, status: 400, errSub: "limit must be a positive integer"},
		{name: "unknown branch", query: func() string { return "?branch=nope&version=1&path=part-00000.ndjson" }, status: 404, errSub: "branch not found"},
		{name: "unknown version", query: func() string { return scanQuery(99, "part-00000.ndjson", "", 0) }, status: 404, errSub: "dataset version not found"},
		{name: "unregistered path", query: func() string { return scanQuery(version, "part-99999.ndjson", "", 0) }, status: 404, errSub: "file not registered in dataset version"},
		{name: "unsupported format path", query: func() string { return scanQuery(version, "data.txt", "", 0) }, status: 422, errSub: "dataset_file_format_unsupported"},
		{name: "corrupt cursor", query: func() string { return scanQuery(version, "part-00000.ndjson", "QUJDRA", 0) }, status: 400, errSub: "dataset_scan_cursor_invalid"},
		// A cursor with an invalid percent-escape must 400, not silently
		// degrade into a first-page request (r.URL.Query() drops such
		// pairs; the handler parses RawQuery itself).
		{name: "cursor bad escape", query: func() string { return scanQuery(version, "part-00000.ndjson", "%zz", 0) }, status: 400, errSub: "invalid query encoding"},
		{name: "cross tenant", query: func() string { return scanQuery(version, "part-00000.ndjson", "", 0) }, claims: foreignClaims, status: 404, errSub: "dataset not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := hs.scanRequest(tc.query(), tc.claims)
			assert.Equal(t, tc.status, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.errSub)
		})
	}

	t.Run("unauthenticated", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+hs.dataset.ID.String()+"/files/rows:scan"+scanQuery(version, "part-00000.ndjson", "", 0), nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("rid", hs.dataset.ID.String())
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		rec := httptest.NewRecorder()
		hs.h.ScanDatasetFileRows(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

// TestScanRejectsNonServerNamedObjects: a staged file pointing anywhere but
// the transaction's server-named object — here a dataset-level upload key a
// normal upload could overwrite — is not a version-pinnable read.
func TestScanRejectsNonServerNamedObjects(t *testing.T) {
	hs := newScanHarness(t)
	txnID := uuid.New()
	datasetLevelKey := path.Join(hs.backing.BaseDirectory(), "datasets", hs.dataset.ID.String(), "uploads", "part-00000.ndjson")
	require.NoError(t, hs.backing.WriteLocalObject(datasetLevelKey, []byte("{\"a\":1}\n")))
	hs.store.stagedFiles[txnID] = []models.StageTransactionFile{{
		LogicalPath: "part-00000.ndjson", PhysicalPath: datasetLevelKey, SizeBytes: 9, Operation: models.FileOperationAdd,
	}}
	hs.store.runtimeTransactions[txnID] = models.RuntimeTransaction{
		ID: txnID, DatasetID: hs.dataset.ID, BranchID: hs.branch.ID, BranchName: hs.branch.Name,
		TxType: models.TransactionTypeSnapshot, Status: models.TransactionStatusCommitted,
	}
	hs.store.versions[hs.dataset.ID] = append(hs.store.versions[hs.dataset.ID], models.DatasetVersion{
		DatasetID: hs.dataset.ID, Version: 1, TransactionID: &txnID,
	})

	rec := hs.scanRequest(scanQuery(1, "part-00000.ndjson", "", 0), nil)
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_scan_object_not_transaction_scoped")
}

// TestScanRejectsTransactionShapeViolations: versions whose transaction is
// not a committed SNAPSHOT on the named branch are explicit rejections.
func TestScanRejectsTransactionShapeViolations(t *testing.T) {
	txnID := uuid.New()

	t.Run("version without transaction", func(t *testing.T) {
		hs := newScanHarness(t)
		hs.store.versions[hs.dataset.ID] = append(hs.store.versions[hs.dataset.ID], models.DatasetVersion{
			DatasetID: hs.dataset.ID, Version: 1,
		})
		rec := hs.scanRequest(scanQuery(1, "part-00000.ndjson", "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_version_has_no_transaction")
	})

	t.Run("append transaction", func(t *testing.T) {
		hs := newScanHarness(t)
		hs.store.runtimeTransactions[txnID] = models.RuntimeTransaction{
			ID: txnID, DatasetID: hs.dataset.ID, BranchID: hs.branch.ID, BranchName: hs.branch.Name,
			TxType: models.TransactionTypeAppend, Status: models.TransactionStatusCommitted,
		}
		hs.store.versions[hs.dataset.ID] = append(hs.store.versions[hs.dataset.ID], models.DatasetVersion{
			DatasetID: hs.dataset.ID, Version: 1, TransactionID: &txnID,
		})
		rec := hs.scanRequest(scanQuery(1, "part-00000.ndjson", "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_scan_transaction_type_unsupported")
	})

	t.Run("open transaction", func(t *testing.T) {
		hs := newScanHarness(t)
		hs.store.runtimeTransactions[txnID] = models.RuntimeTransaction{
			ID: txnID, DatasetID: hs.dataset.ID, BranchID: hs.branch.ID, BranchName: hs.branch.Name,
			TxType: models.TransactionTypeSnapshot, Status: models.TransactionStatusOpen,
		}
		hs.store.versions[hs.dataset.ID] = append(hs.store.versions[hs.dataset.ID], models.DatasetVersion{
			DatasetID: hs.dataset.ID, Version: 1, TransactionID: &txnID,
		})
		rec := hs.scanRequest(scanQuery(1, "part-00000.ndjson", "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "not_committed")
	})

	t.Run("transaction on another branch", func(t *testing.T) {
		hs := newScanHarness(t)
		otherBranch := models.DatasetBranch{ID: uuid.New(), DatasetID: hs.dataset.ID, Name: "feature"}
		hs.store.branches[hs.dataset.ID] = append(hs.store.branches[hs.dataset.ID], otherBranch)
		hs.store.runtimeTransactions[txnID] = models.RuntimeTransaction{
			ID: txnID, DatasetID: hs.dataset.ID, BranchID: otherBranch.ID, BranchName: otherBranch.Name,
			TxType: models.TransactionTypeSnapshot, Status: models.TransactionStatusCommitted,
		}
		hs.store.versions[hs.dataset.ID] = append(hs.store.versions[hs.dataset.ID], models.DatasetVersion{
			DatasetID: hs.dataset.ID, Version: 1, TransactionID: &txnID,
		})
		rec := hs.scanRequest(scanQuery(1, "part-00000.ndjson", "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_version_branch_mismatch")
	})
}

// TestScanCursorBoundToRequest: a cursor issued for one dataset cannot be
// replayed against another dataset's identical-looking request.
func TestScanCursorBoundToRequest(t *testing.T) {
	hsA := newScanHarness(t)
	hsB := newScanHarness(t)
	for _, hs := range []*scanHarness{hsA, hsB} {
		lines := scanNDJSONLines(5, "x")
		hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))
	}
	pageA := decodeScan(t, hsA.scanRequest(scanQuery(1, "part-00000.ndjson", "", 2), nil))
	require.NotNil(t, pageA.NextCursor)

	rec := hsB.scanRequest(scanQuery(1, "part-00000.ndjson", *pageA.NextCursor, 2), nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_scan_cursor_mismatch")

	// The same cursor replayed against its own request still works.
	pageA2 := decodeScan(t, hsA.scanRequest(scanQuery(1, "part-00000.ndjson", *pageA.NextCursor, 2), nil))
	require.Len(t, pageA2.Rows, 2)
}

// TestScanCursorPastEOFWhenSizeShrinks: a cursor whose offset exceeds the
// staged file size is rejected rather than reading past the object.
func TestScanCursorPastEOFWhenSizeShrinks(t *testing.T) {
	hs := newScanHarness(t)
	lines := scanNDJSONLines(5, "x")
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))
	page := decodeScan(t, hs.scanRequest(scanQuery(version, "part-00000.ndjson", "", 2), nil))
	require.NotNil(t, page.NextCursor)

	// Shrink the staged size record: the cursor's offset now exceeds it.
	var txnID uuid.UUID
	for id := range hs.store.stagedFiles {
		txnID = id
	}
	staged := &hs.store.stagedFiles[txnID][0]
	staged.SizeBytes = 2

	rec := hs.scanRequest(scanQuery(version, "part-00000.ndjson", *page.NextCursor, 2), nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "past the versioned file size")
}

// TestScanDeletedBranchStopsContinuation: removing the branch mid-scan
// fails the next page — every page re-resolves the branch through
// GetRuntimeBranch, so a cursor cannot bypass branch deletion.
func TestScanDeletedBranchStopsContinuation(t *testing.T) {
	hs := newScanHarness(t)
	lines := scanNDJSONLines(6, "x")
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))
	page := decodeScan(t, hs.scanRequest(scanQuery(version, "part-00000.ndjson", "", 2), nil))
	require.NotNil(t, page.NextCursor)

	// Simulate branch deletion: the runtime branch no longer resolves.
	delete(hs.store.branches, hs.dataset.ID)

	rec := hs.scanRequest(scanQuery(version, "part-00000.ndjson", *page.NextCursor, 2), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "branch not found")
}

// TestScanSizeMismatchRejected: a staged size record that disagrees with
// the object's real byte length is an explicit failure, so a truncated or
// tampered object can never report a successful page.
func TestScanSizeMismatchRejected(t *testing.T) {
	hs := newScanHarness(t)
	lines := scanNDJSONLines(4, "x")
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))
	var txnID uuid.UUID
	for id := range hs.store.stagedFiles {
		txnID = id
	}
	hs.store.stagedFiles[txnID][0].SizeBytes += 100

	rec := hs.scanRequest(scanQuery(version, "part-00000.ndjson", "", 0), nil)
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_size_mismatch")
}

// TestScanAuditsEachPage: every page request emits a files.rows_scan audit
// event through the existing sink mechanism.
func TestScanAuditsEachPage(t *testing.T) {
	hs := newScanHarness(t)
	lines := scanNDJSONLines(4, "x")
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))

	var events []handlers.AuditEvent
	hs.h.AuditSink = func(_ context.Context, e handlers.AuditEvent) { events = append(events, e) }

	cursor := ""
	for {
		page := decodeScan(t, hs.scanRequest(scanQuery(version, "part-00000.ndjson", cursor, 2), nil))
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	require.Len(t, events, 2)
	for _, e := range events {
		assert.Equal(t, "files.rows_scan", e.Action)
	}
}

// TestScanEmptyCommittedFile: a zero-byte committed object answers an empty
// first page with no cursor.
func TestScanEmptyCommittedFile(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte{})
	page := decodeScan(t, hs.scanRequest(scanQuery(version, "part-00000.ndjson", "", 0), nil))
	assert.Empty(t, page.Rows)
	assert.Nil(t, page.NextCursor)
	assert.Equal(t, int64(0), page.SizeBytes)
}

func joinLines(lines []string) string {
	out := ""
	for _, line := range lines {
		out += line + "\n"
	}
	return out
}

func jsonObjectValue(t *testing.T, raw json.RawMessage, key string) string {
	t.Helper()
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &object))
	value, ok := object[key]
	require.True(t, ok, "key %s present", key)
	var out string
	require.NoError(t, json.Unmarshal(value, &out))
	return out
}
