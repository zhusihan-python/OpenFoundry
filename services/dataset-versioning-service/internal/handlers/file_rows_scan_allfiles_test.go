package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// stagedObjectKey mirrors the harness's server-named transaction object
// layout for a logical path.
func stagedObjectKey(hs *scanHarness, txnID uuid.UUID, logical string) string {
	return path.Join(hs.backing.BaseDirectory(), "datasets", hs.dataset.ID.String(), "transactions", txnID.String(), logical)
}

// stagedSpec describes one file of a multi-file snapshot commit for the
// fake harness. Content-bearing specs get a real server-named object;
// content-less specs exercise rows a legacy metadata-only commit could
// leave behind.
type stagedSpec struct {
	path         string
	content      []byte
	op           models.FileOperation
	physical     string // override the derived object key when set
	sizeOverride *int64
}

// commitSnapshotVersionFiles registers the files under one transaction in
// the GIVEN order (the keyset order later differs when the paths are not
// pre-sorted — the out-of-order registration the acceptance asks for) and
// cuts one version. Returns the new version.
func (hs *scanHarness) commitSnapshotVersionFiles(t *testing.T, specs ...stagedSpec) int32 {
	t.Helper()
	txnID := uuid.New()
	rows := make([]models.StageTransactionFile, 0, len(specs))
	for _, spec := range specs {
		objectKey := stagedObjectKey(hs, txnID, spec.path)
		if spec.content != nil {
			require.NoError(t, hs.backing.WriteLocalObject(objectKey, spec.content))
		}
		size := int64(len(spec.content))
		if spec.sizeOverride != nil {
			size = *spec.sizeOverride
		}
		physical := objectKey
		if spec.physical != "" {
			physical = spec.physical
		}
		op := spec.op
		if op == "" {
			op = models.FileOperationAdd
		}
		rows = append(rows, models.StageTransactionFile{LogicalPath: spec.path, PhysicalPath: physical, SizeBytes: size, Operation: op})
	}
	hs.store.stagedFiles[txnID] = rows
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

func scanAllFilesQuery(version int32, cursor string, limit int) string {
	return scanAllFilesPredicateQuery(version, "", cursor, limit)
}

func scanAllFilesPredicateQuery(version int32, predicate string, cursor string, limit int) string {
	query := fmt.Sprintf("?branch=main&version=%d&all_files=true", version)
	if predicate != "" {
		query += "&predicate=" + url.QueryEscape(predicate)
	}
	if limit > 0 {
		query += fmt.Sprintf("&limit=%d", limit)
	}
	if cursor != "" {
		query += "&cursor=" + cursor
	}
	return query
}

type allFilesScanResponse struct {
	TransactionID string            `json:"transaction_id"`
	Mode          string            `json:"mode"`
	Format        string            `json:"format"`
	Rows          []json.RawMessage `json:"rows"`
	NextCursor    *string           `json:"next_cursor"`
	RowCount      int               `json:"row_count"`
	PageBytes     int64             `json:"page_bytes"`
	FilesAccessed int               `json:"files_accessed"`
}

func decodeAllFilesPage(t *testing.T, rec *httptest.ResponseRecorder) allFilesScanResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out allFilesScanResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Equal(t, "all_files", out.Mode)
	require.Equal(t, "ndjson", out.Format)
	return out
}

// walkAllFiles pages an all_files scan to the end, checking the page
// invariants (row/byte/file budgets) and returning the rows in order plus
// the summed page bytes and opens.
func walkAllFiles(t *testing.T, hs *scanHarness, version int32, predicate string, limit int) ([]string, int, int64, int) {
	t.Helper()
	var got []string
	pages := 0
	var bytesSum int64
	var opens int
	cursor := ""
	for {
		page := decodeAllFilesPage(t, hs.scanRequest(scanAllFilesPredicateQuery(version, predicate, cursor, limit), nil))
		pages++
		require.LessOrEqual(t, page.RowCount, limit, "row budget")
		require.LessOrEqual(t, page.PageBytes, int64(8<<20), "byte budget")
		require.LessOrEqual(t, page.FilesAccessed, 16, "file budget")
		require.Positive(t, page.FilesAccessed, "every page opens at least one file")
		for _, raw := range page.Rows {
			got = append(got, string(raw))
		}
		bytesSum += page.PageBytes
		opens += page.FilesAccessed
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		require.Less(t, pages, 100000, "scan must terminate without loops")
	}
	return got, pages, bytesSum, opens
}

// TestScanAllFilesOrderingAcrossFiles: files registered out of keyset
// order come back in stable logical-path order, records verbatim and in
// file order, with page bytes accounting for every byte exactly once and
// the final page carrying no cursor.
func TestScanAllFilesOrderingAcrossFiles(t *testing.T) {
	hs := newScanHarness(t)
	// Registration order deliberately differs from logical-path order.
	files := []stagedSpec{
		{path: "part-2.ndjson", content: []byte(joinLines([]string{`{"f":2,"i":0}`, `{"f":2,"i":1}`}))},
		{path: "part-0.ndjson", content: []byte(joinLines([]string{`{"f":0,"i":0}`, `{"f":0,"i":1}`, `{"f":0,"i":2}`}))},
		{path: "part-1.ndjson", content: []byte(joinLines([]string{`{"f":1,"i":0}`}))},
	}
	version := hs.commitSnapshotVersionFiles(t, files...)

	expected := []string{`{"f":0,"i":0}`, `{"f":0,"i":1}`, `{"f":0,"i":2}`, `{"f":1,"i":0}`, `{"f":2,"i":0}`, `{"f":2,"i":1}`}
	got, pages, bytesSum, opens := walkAllFiles(t, hs, version, "", 4)
	assert.GreaterOrEqual(t, pages, 2, "sample spans pages")
	assert.Equal(t, expected, got, "stable path order, in-file order preserved")
	var total int64
	for _, f := range files {
		total += int64(len(f.content))
	}
	assert.Equal(t, total, bytesSum, "page bytes account for every file byte exactly once")
	assert.Equal(t, 3, opens, "each file is opened exactly once when budgets allow")
}

// TestScanAllFilesFileBudgetPages: more files than the per-page open cap
// split across pages by the file budget without losing or duplicating rows.
func TestScanAllFilesFileBudgetPages(t *testing.T) {
	hs := newScanHarness(t)
	const fileCount = 25 // > scanMaxFilesPerPage (16)
	specs := make([]stagedSpec, 0, fileCount)
	var expected []string
	for i := 0; i < fileCount; i++ {
		// Register in reverse so keyset order differs from registration.
		name := fmt.Sprintf("part-%02d.ndjson", fileCount-1-i)
		specs = append(specs, stagedSpec{path: name, content: []byte(joinLines([]string{
			fmt.Sprintf(`{"f":%d,"i":0}`, fileCount-1-i), fmt.Sprintf(`{"f":%d,"i":1}`, fileCount-1-i),
		}))})
	}
	// expected in logical order = part-00..part-24
	for i := 0; i < fileCount; i++ {
		expected = append(expected, fmt.Sprintf(`{"f":%d,"i":0}`, i), fmt.Sprintf(`{"f":%d,"i":1}`, i))
	}
	version := hs.commitSnapshotVersionFiles(t, specs...)

	got, pages, _, _ := walkAllFiles(t, hs, version, "", 1000)
	assert.GreaterOrEqual(t, pages, 2, "file budget must split the scan")
	assert.Equal(t, expected, got, "no rows lost or duplicated across file-budget pages")
}

// TestScanAllFilesByteBudgetAcrossFiles: two whitespace-heavy files larger
// together than the 8MiB page byte budget page by consumed bytes with
// progress on every page (empty pages with advancing cursors are legal).
func TestScanAllFilesByteBudgetAcrossFiles(t *testing.T) {
	hs := newScanHarness(t)
	// ~4.5MiB of blank lines in each of two files: together > 8MiB, so the
	// byte budget must slice across files. One real record at each file
	// start/end keeps the data nonzero.
	var bigA, bigB strings.Builder
	bigA.WriteString(`{"f":"a","tag":"start"}` + "\n")
	for bigA.Len() < 4<<20+2<<19 {
		bigA.WriteString("   \n")
	}
	bigA.WriteString(`{"f":"a","tag":"end"}` + "\n")
	bigB.WriteString(`{"f":"b","tag":"start"}` + "\n")
	for bigB.Len() < 4<<20+2<<19 {
		bigB.WriteString("\t\n")
	}
	bigB.WriteString(`{"f":"b","tag":"end"}` + "\n")

	version := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "part-a.ndjson", content: []byte(bigA.String())},
		stagedSpec{path: "part-b.ndjson", content: []byte(bigB.String())},
	)
	got, pages, bytesSum, _ := walkAllFiles(t, hs, version, "", 1000)
	assert.GreaterOrEqual(t, pages, 2, "byte budget must slice across files")
	assert.Equal(t, []string{`{"f":"a","tag":"start"}`, `{"f":"a","tag":"end"}`, `{"f":"b","tag":"start"}`, `{"f":"b","tag":"end"}`}, got)
	assert.Equal(t, int64(len(bigA.String())+len(bigB.String())), bytesSum, "every consumed byte counted exactly once")
}

// TestScanAllFilesDefersFirstRecordOfNextFile: file A consumes the byte
// budget almost exactly, leaving less than file B's first record — B's
// first record is deferred whole to the next page and the scan still
// progresses (no loop, no loss).
func TestScanAllFilesDefersFirstRecordOfNextFile(t *testing.T) {
	hs := newScanHarness(t)
	// A = 1048574 records of 8 bytes + one 12-byte record = 8MiB - 4 bytes.
	var a strings.Builder
	for i := 0; i < 1048574; i++ {
		a.WriteString(`{"a":1}` + "\n") // 8 bytes
	}
	a.WriteString(`{"a":12345}` + "\n") // 12 bytes
	require.Equal(t, (8<<20)-4, a.Len())
	b := joinLines([]string{`{"b":1}`, `{"b":2}`})

	version := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "part-a.ndjson", content: []byte(a.String())},
		stagedSpec{path: "part-b.ndjson", content: []byte(b)},
	)
	// Row limit 1 keeps the page alive until the byte budget governs.
	got, pages, bytesSum, _ := walkAllFiles(t, hs, version, "", 1000)
	// The deferred record is `{"b":1}`; rows from A are the a-records —
	// assert only the b tail here to keep the expectation readable.
	assert.Equal(t, []string{`{"b":1}`, `{"b":2}`}, got[len(got)-2:], "deferred first record of B is delivered next page")
	assert.Equal(t, int64(a.Len()+len(b)), bytesSum, "no bytes re-consumed or lost")
	assert.GreaterOrEqual(t, pages, 2)
}

// TestScanAllFilesEmptyAndNoMatchWindows: an empty file, a whitespace-only
// file and a no-match predicate window all form empty-but-advancing pages
// and never end the scan early or loop.
func TestScanAllFilesEmptyAndNoMatchWindows(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "part-0.ndjson", content: []byte(joinLines([]string{`{"id":1}`, `{"id":2}`, `{"id":3}`}))},
		stagedSpec{path: "part-1.ndjson", content: []byte{}},              // empty file
		stagedSpec{path: "part-2.ndjson", content: []byte("  \n\t\n \n")}, // whitespace-only
		stagedSpec{path: "part-3.ndjson", content: []byte(joinLines([]string{`{"id":4}`, `{"id":5}`}))},
	)

	got, _, bytesSum, opens := walkAllFiles(t, hs, version, "", 10)
	assert.Equal(t, []string{`{"id":1}`, `{"id":2}`, `{"id":3}`, `{"id":4}`, `{"id":5}`}, got)
	assert.Equal(t, int64(len(joinLines([]string{`{"id":1}`, `{"id":2}`, `{"id":3}`}))+0+7+len(joinLines([]string{`{"id":4}`, `{"id":5}`}))), bytesSum)
	assert.Equal(t, 4, opens, "empty and whitespace files are opened (and size-verified) exactly once")

	// No-match predicate over everything (small files fit one page):
	// single empty page, full byte accounting, explicit cursor-less end.
	noMatch, pages, predBytes, _ := walkAllFiles(t, hs, version, `{"column":"id","op":"eq","value":99}`, 2)
	assert.Empty(t, noMatch)
	assert.Equal(t, 1, pages)
	assert.Equal(t, bytesSum, predBytes, "predicate scan consumes the same raw bytes")

	// Matching predicate spans files.
	matched, _, _, _ := walkAllFiles(t, hs, version, `{"column":"id","op":"eq","value":2}`, 10)
	assert.Equal(t, []string{`{"id":2}`}, matched)
}

// TestScanAllFilesEmptyAdvancingPageByFileBudget: when the predicate only
// matches files beyond the per-page open cap, earlier pages are EMPTY but
// still advance (cursor handed to the lookahead file) — no early
// termination, no loop.
func TestScanAllFilesEmptyAdvancingPageByFileBudget(t *testing.T) {
	hs := newScanHarness(t)
	const fileCount = 20 // first match sits in file 18, past the 16-open cap
	specs := make([]stagedSpec, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		content := `{"id":0}` + "\n"
		if i >= 17 {
			content = `{"id":7}` + "\n"
		}
		specs = append(specs, stagedSpec{path: fmt.Sprintf("part-%02d.ndjson", i), content: []byte(content)})
	}
	version := hs.commitSnapshotVersionFiles(t, specs...)

	got, pages, _, opens := walkAllFiles(t, hs, version, `{"column":"id","op":"eq","value":7}`, 100)
	require.Len(t, got, 3)
	assert.Equal(t, []string{`{"id":7}`, `{"id":7}`, `{"id":7}`}, got)
	assert.GreaterOrEqual(t, pages, 2, "file budget must split; the first page is empty but advancing")
	assert.Equal(t, fileCount, opens, "every file is opened exactly once across pages")
}

// TestScanAllFilesNoTrailingNewlineAtBoundary: a file ending without a
// trailing newline followed by another file — the boundary switch hands
// the next file a fresh offset-0 cursor instead of an end-of-file
// continuation (which the record-boundary check would reject).
func TestScanAllFilesNoTrailingNewlineAtBoundary(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n" + `{"x":2}`)}, // no trailing newline
		stagedSpec{path: "b.ndjson", content: []byte(`{"x":3}` + "\n")},
	)
	got, pages, _, _ := walkAllFiles(t, hs, version, "", 2)
	assert.Equal(t, []string{`{"x":1}`, `{"x":2}`, `{"x":3}`}, got)
	assert.GreaterOrEqual(t, pages, 2, "row limit forces the boundary switch mid-scan")
}

// TestScanAllFilesRejectsUnsupportedFiles: an unsupported format, a
// non-ADD operation, an unencodable/canonicality-violating logical path
// (as a legacy metadata-only commit could register), missing bytes and a
// tampered size each fail the whole scan explicitly — nothing is skipped.
func TestScanAllFilesRejectsUnsupportedFiles(t *testing.T) {
	t.Run("unsupported text file", func(t *testing.T) {
		hs := newScanHarness(t)
		version := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n")},
			stagedSpec{path: "notes.txt", content: []byte("x\n1\n")},
		)
		rec := hs.scanRequest(scanAllFilesQuery(version, "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_file_format_unsupported")
	})
	t.Run("replace operation", func(t *testing.T) {
		hs := newScanHarness(t)
		version := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n")},
			stagedSpec{path: "b.ndjson", content: []byte(`{"x":2}` + "\n"), op: models.FileOperationReplace},
		)
		rec := hs.scanRequest(scanAllFilesQuery(version, "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_scan_file_operation_unsupported")
	})
	t.Run("non-canonical path", func(t *testing.T) {
		hs := newScanHarness(t)
		version := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n")},
			stagedSpec{path: "../escape.ndjson", content: []byte(`{"x":2}` + "\n")},
		)
		rec := hs.scanRequest(scanAllFilesQuery(version, "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_scan_file_path_invalid")
	})
	t.Run("path too long to encode", func(t *testing.T) {
		hs := newScanHarness(t)
		long := strings.Repeat("d", 513) + ".ndjson"
		// No object write (the OS would refuse the file name anyway): the
		// staged row alone must fail the scan before any open.
		version := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n")},
			stagedSpec{path: long, content: nil},
		)
		rec := hs.scanRequest(scanAllFilesQuery(version, "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_scan_file_path_invalid")
	})
	t.Run("missing bytes", func(t *testing.T) {
		hs := newScanHarness(t)
		// Staged row with a canonical server key but no object written.
		version := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n")},
			stagedSpec{path: "b.ndjson", content: nil, sizeOverride: int64Ptr(8)},
		)
		rec := hs.scanRequest(scanAllFilesQuery(version, "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_file_bytes_missing")
	})
	t.Run("tampered size", func(t *testing.T) {
		hs := newScanHarness(t)
		version := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n"), sizeOverride: int64Ptr(999)},
		)
		rec := hs.scanRequest(scanAllFilesQuery(version, "", 0), nil)
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		assert.Contains(t, rec.Body.String(), "dataset_file_size_mismatch")
	})
}

func int64Ptr(v int64) *int64 { return &v }

// TestScanAllFilesCursorBinding: continuation rejects changed version,
// dropped/changed predicate and cross-mode cursors; publishing v2 between
// pages keeps the old cursor reading version 1's remaining files.
func TestScanAllFilesCursorBinding(t *testing.T) {
	hs := newScanHarness(t)
	v1 := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.ndjson", content: []byte(joinLines([]string{`{"v":1,"i":0}`, `{"v":1,"i":1}`, `{"v":1,"i":2}`}))},
		stagedSpec{path: "b.ndjson", content: []byte(joinLines([]string{`{"v":1,"i":3}`, `{"v":1,"i":4}`}))},
	)

	first := decodeAllFilesPage(t, hs.scanRequest(scanAllFilesQuery(v1, "", 2), nil))
	require.NotNil(t, first.NextCursor)
	require.Len(t, first.Rows, 2)
	cursor := *first.NextCursor

	// v2 lands between pages on the same branch.
	v2 := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.ndjson", content: []byte(joinLines([]string{`{"v":2,"i":0}`}))},
	)
	require.Greater(t, v2, v1)

	// Old cursor keeps reading v1's remaining rows.
	rest := decodeAllFilesPage(t, hs.scanRequest(scanAllFilesQuery(v1, cursor, 0), nil))
	var rows []string
	for _, raw := range rest.Rows {
		rows = append(rows, string(raw))
	}
	assert.Equal(t, []string{`{"v":1,"i":2}`, `{"v":1,"i":3}`, `{"v":1,"i":4}`}, rows, "old cursor continues the old version across files")
	assert.Nil(t, rest.NextCursor)

	// Explicit v2 scan returns the new content.
	fresh := decodeAllFilesPage(t, hs.scanRequest(scanAllFilesQuery(v2, "", 0), nil))
	require.Len(t, fresh.Rows, 1)
	assert.Equal(t, `{"v":2,"i":0}`, string(fresh.Rows[0]))

	// Rejections.
	for name, query := range map[string]string{
		"changed version":   scanAllFilesQuery(v2, cursor, 0),
		"changed predicate": scanAllFilesPredicateQuery(v1, `{"column":"v","op":"eq","value":1}`, cursor, 0),
		"s1 path cursor":    scanAllFilesQuery(v1, pathModeCursor(t, hs, v1), 0),
	} {
		rec := hs.scanRequest(query, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "case %q: %s", name, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "dataset_scan_cursor", "case %q", name)
	}
}

// pathModeCursor fetches an s1 cursor from a plain path scan on version.
func pathModeCursor(t *testing.T, hs *scanHarness, version int32) string {
	t.Helper()
	page := decodeScan(t, hs.scanRequest(scanQuery(version, "a.ndjson", "", 1), nil))
	require.NotNil(t, page.NextCursor)
	return *page.NextCursor
}

// TestScanAllFilesAuditMode: all_files pages audit with the mode recorded.
func TestScanAllFilesAuditMode(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.ndjson", content: []byte(`{"x":1}` + "\n")},
	)
	var events []handlers.AuditEvent
	hs.h.AuditSink = func(_ context.Context, e handlers.AuditEvent) { events = append(events, e) }
	page := decodeAllFilesPage(t, hs.scanRequest(scanAllFilesQuery(version, "", 0), nil))
	require.Nil(t, page.NextCursor)
	require.Len(t, events, 1)
	assert.Equal(t, "files.rows_scan", events[0].Action)
	assert.Equal(t, "all_files", events[0].Details["mode"])
}
