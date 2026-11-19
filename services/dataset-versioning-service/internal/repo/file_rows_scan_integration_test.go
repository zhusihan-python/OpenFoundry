//go:build integration

// WP3.3 increment 1 real-stack acceptance: the rows:scan endpoint driven
// through the real server router (chi + auth middleware with test JWTs)
// against a real PostgreSQL database and a temp local backing store.
// Publications go through the real outputs:commit HTTP endpoint, so the
// version-pinned objects under test are genuine server-named transaction
// objects.
package repo_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	observability "github.com/openfoundry/openfoundry-go/libs/observability"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/server"
)

type scanStack struct {
	srv     *http.Server
	pool    *pgxpool.Pool
	backing *storageabstraction.LocalBackingFS
	jwt     *authmw.JWTConfig
	ownerID uuid.UUID
	orgID   uuid.UUID
	tokenA  string
	tokenB  string
	dataset models.Dataset
	rid     string
}

func newScanStack(t *testing.T) *scanStack {
	t.Helper()
	ctx := context.Background()
	pool := faultPool(ctx, t)
	require.NoError(t, repo.Migrate(ctx, pool))

	r := &repo.Repo{Pool: pool}
	backing := &storageabstraction.LocalBackingFS{BaseURL: "http://dataset-versioning.local", BaseDir: "data", RootDir: t.TempDir()}
	h := &handlers.Handlers{Repo: r, BackingFS: backing}

	jwt := authmw.NewJWTConfig("wp33-scan-router-secret")
	cfg := &config.Config{}
	cfg.Service.Name = "dataset-versioning-service"
	cfg.Service.Version = "test"
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 0
	srv := server.New(cfg, jwt, h, observability.NewMetrics())

	owner := uuid.New()
	org := uuid.New()
	stack := &scanStack{srv: srv, pool: pool, backing: backing, jwt: jwt, ownerID: owner, orgID: org}
	stack.tokenA = scanToken(t, jwt, owner, &org)
	otherOrg := uuid.New()
	stack.tokenB = scanToken(t, jwt, uuid.New(), &otherOrg)

	dataset, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "wp33-scan"}, owner, &org)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, dataset))
	stack.dataset = *dataset
	stack.rid = "ri.foundry.main.dataset." + dataset.ID.String()
	return stack
}

func scanToken(t *testing.T, jwt *authmw.JWTConfig, sub uuid.UUID, org *uuid.UUID) string {
	t.Helper()
	now := time.Now()
	claims := &authmw.Claims{
		Sub:         sub,
		IAT:         now.Unix(),
		EXP:         now.Add(time.Hour).Unix(),
		JTI:         uuid.New(),
		Email:       "wp33@example.com",
		Name:        "WP33 Scan",
		Permissions: []string{"dataset.write"},
		OrgID:       org,
	}
	tok, err := authmw.EncodeToken(jwt, claims)
	require.NoError(t, err)
	return tok
}

func (s *scanStack) do(t *testing.T, method string, target string, contentType string, body io.Reader, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	return rec
}

// commitSnapshot publishes an NDJSON snapshot of content through the real
// outputs:commit endpoint and returns the cut version number.
func (s *scanStack) commitSnapshot(t *testing.T, logical string, content []byte) int32 {
	t.Helper()
	body := map[string]any{
		"branch":           "main",
		"transaction_type": "SNAPSHOT",
		"files": []map[string]any{{
			"logical_path":   logical,
			"content_base64": base64.StdEncoding.EncodeToString(content),
			"size_bytes":     len(content),
			"operation":      "ADD",
		}},
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	rec := s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/outputs:commit", "application/json", bytes.NewReader(payload), s.tokenA)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var created struct {
		Transaction struct {
			ID string `json:"id"`
		} `json:"transaction"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	var version int32
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT version FROM dataset_versions WHERE dataset_id = $1 AND transaction_id = $2`,
		s.dataset.ID, created.Transaction.ID).Scan(&version))
	return version
}

func (s *scanStack) scan(t *testing.T, query string, token string) scanHTTPResponse {
	t.Helper()
	rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+query, "", nil, token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out scanHTTPResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

type scanHTTPResponse struct {
	TransactionID string            `json:"transaction_id"`
	Rows          []json.RawMessage `json:"rows"`
	NextCursor    *string           `json:"next_cursor"`
	RowCount      int               `json:"row_count"`
	PageBytes     int64             `json:"page_bytes"`
	SizeBytes     int64             `json:"size_bytes"`
}

func scanLines(n int, prefix string) []string {
	lines := make([]string, n)
	for i := 0; i < n; i++ {
		// Key order deliberately non-alphabetical; values cover string
		// forms ("007" must stay a string), nested objects and unicode.
		lines[i] = fmt.Sprintf(`{"sku":"%s-%04d","qty":%d,"tag":"007","note":{"zh":"行-%d"},"big":123456789012345678}`, prefix, i, i%97, i)
	}
	return lines
}

func scanContent(lines []string) []byte {
	var b strings.Builder
	for _, line := range lines {
		b.WriteString(line)
		b.WriteString("\n")
	}
	return []byte(b.String())
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

// TestRowsScanRealRouterValueByValue: publish a >1000-row NDJSON snapshot
// through the real outputs:commit endpoint, then page rows:scan through the
// real router with a real JWT. Every record must come back verbatim, in
// order, across at least three pages, with page bytes accounting for the
// whole object. After page one a new snapshot of the same logical path is
// published: the old cursor keeps reading version 1, explicit scans of
// either version return that version's bytes.
func TestRowsScanRealRouterValueByValue(t *testing.T) {
	s := newScanStack(t)

	v1Lines := scanLines(2505, "v1")
	v1 := s.commitSnapshot(t, "part-00000.ndjson", scanContent(v1Lines))

	var got []string
	var bytesSum int64
	cursor := ""
	pages := 0
	for {
		page := s.scan(t, scanQuery(v1, "part-00000.ndjson", cursor, 1000), s.tokenA)
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
		if pages == 1 {
			// Publish v2 of the same logical file mid-scan.
			v2Lines := scanLines(2505, "v2")
			v2 := s.commitSnapshot(t, "part-00000.ndjson", scanContent(v2Lines))
			require.NotEqual(t, v1, v2)

			// The old-cursor continuation below must keep serving v1; a
			// fresh explicit scan of v2 returns the new bytes immediately.
			fresh := s.scan(t, scanQuery(v2, "part-00000.ndjson", "", 5), s.tokenA)
			require.Equal(t, v2Lines[0], string(fresh.Rows[0]))
		}
	}

	require.GreaterOrEqual(t, pages, 3, "2505 rows at limit 1000 must span at least three pages")
	require.Len(t, got, len(v1Lines), "no rows dropped or duplicated")
	for i := range v1Lines {
		require.Equal(t, v1Lines[i], got[i], "record %d must be verbatim and in order", i)
	}
	require.Equal(t, int64(len(scanContent(v1Lines))), bytesSum, "page bytes account for the whole v1 object")
}

// TestRowsScanRealRouterNegativeControls walks the rejection surface
// through the real router: cross-tenant 404, bad parameters, corrupt and
// mismatched cursors, unknown branch/version/path shapes, non-NDJSON
// paths, empty-file and oversize-record behavior, and the reserved-subtree
// upload overwrite attempt followed by a byte-stability recheck of the
// versioned page.
func TestRowsScanRealRouterNegativeControls(t *testing.T) {
	s := newScanStack(t)
	ctx := context.Background()

	v1Lines := scanLines(1500, "v1")
	content := scanContent(v1Lines)
	v1 := s.commitSnapshot(t, "part-00000.ndjson", content)

	page1 := s.scan(t, scanQuery(v1, "part-00000.ndjson", "", 1000), s.tokenA)
	require.NotNil(t, page1.NextCursor)
	var txnID string
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT transaction_id::text FROM dataset_versions WHERE dataset_id = $1 AND version = $2`,
		s.dataset.ID, v1).Scan(&txnID))

	rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+scanQuery(v1, "part-00000.ndjson", "", 0), "", nil, s.tokenB)
	require.Equal(t, http.StatusNotFound, rec.Code, "cross-tenant must 404")
	require.Contains(t, rec.Body.String(), "dataset not found")

	for _, tc := range []struct {
		name   string
		query  string
		status int
		errSub string
	}{
		{"bad version", "?branch=main&version=abc&path=part-00000.ndjson", 400, "version must be a positive integer"},
		{"version zero", scanQuery(0, "part-00000.ndjson", "", 0), 400, "version must be a positive integer"},
		{"missing branch", "?version=1&path=part-00000.ndjson", 400, "branch is required"},
		{"missing path", "?branch=main&version=1", 400, "path is required"},
		{"unknown branch", "?branch=nope&version=1&path=part-00000.ndjson", 404, "branch not found"},
		{"unknown version", scanQuery(99, "part-00000.ndjson", "", 0), 404, "dataset version not found"},
		{"unregistered path", scanQuery(v1, "part-99999.ndjson", "", 0), 404, "file not registered in dataset version"},
		{"unsupported format path", scanQuery(v1, "data.txt", "", 0), 422, "dataset_file_format_unsupported"},
		{"corrupt cursor", scanQuery(v1, "part-00000.ndjson", "QUJDRA", 0), 400, "dataset_scan_cursor_invalid"},
		{"cursor bad escape", "?branch=main&version=" + fmt.Sprint(v1) + "&path=part-00000.ndjson&cursor=%zz", 400, "invalid query encoding"},
		{"cursor wrong version", scanQuery(v1, "part-00000.ndjson", mintScanCursor(t, s, "part-00000.ndjson", v1+1), 0), 400, "dataset_scan_cursor_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+tc.query, "", nil, s.tokenA)
			assert.Equal(t, tc.status, rec.Code)
			assert.Contains(t, rec.Body.String(), tc.errSub)
		})
	}

	// The v1 continuation cursor replayed against a mismatched (but
	// existing) version parameter is rejected; against its own request it
	// still works.
	v2 := s.commitSnapshot(t, "part-00000.ndjson", scanContent(scanLines(1500, "v2")))
	rec = s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+scanQuery(v2, "part-00000.ndjson", *page1.NextCursor, 0), "", nil, s.tokenA)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "dataset_scan_cursor_mismatch")

	page2 := s.scan(t, scanQuery(v1, "part-00000.ndjson", *page1.NextCursor, 1000), s.tokenA)
	require.Len(t, page2.Rows, 500, "old cursor continues the old version")
	require.Nil(t, page2.NextCursor)

	// Upload overwrite attempt on the committed object: refused, and the
	// versioned page bytes are unchanged afterwards.
	var upload bytes.Buffer
	writer := multipart.NewWriter(&upload)
	require.NoError(t, writer.WriteField("logical_path", "transactions/"+txnID+"/part-00000.ndjson"))
	part, err := writer.CreateFormFile("file", "upload.bin")
	require.NoError(t, err)
	_, err = part.Write([]byte("{\"evil\":true}\n"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	rec = s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/upload", writer.FormDataContentType(), &upload, s.tokenA)
	require.Equal(t, http.StatusBadRequest, rec.Code, "reserved subtree upload must be refused")
	require.Contains(t, rec.Body.String(), "reserved")

	again := s.scan(t, scanQuery(v1, "part-00000.ndjson", "", 2), s.tokenA)
	require.Equal(t, v1Lines[0], string(again.Rows[0]), "versioned page bytes unchanged after refused overwrite")

	// The object on disk still holds the committed content.
	raw, err := s.backing.ReadLocalObject(path.Join("data", "datasets", s.dataset.ID.String(), "transactions", txnID, "part-00000.ndjson"))
	require.NoError(t, err)
	require.Equal(t, content, raw, "backing object bytes must be untouched")

	// Empty committed file: one empty page, no cursor.
	emptyVersion := s.commitSnapshot(t, "empty.ndjson", []byte{})
	empty := s.scan(t, scanQuery(emptyVersion, "empty.ndjson", "", 0), s.tokenA)
	require.Empty(t, empty.Rows)
	require.Nil(t, empty.NextCursor)

	// Oversize record: a single >1MiB record fails explicitly.
	bigLine := `{"k":"` + strings.Repeat("x", 1<<20) + `"}` + "\n"
	bigVersion := s.commitSnapshot(t, "big.ndjson", []byte(bigLine))
	rec = s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+scanQuery(bigVersion, "big.ndjson", "", 0), "", nil, s.tokenA)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	require.Contains(t, rec.Body.String(), "dataset_file_record_too_large")
}

// mintScanCursor builds the service's exact cursor wire format for this
// dataset/branch with an arbitrary version: decoding succeeds, so only the
// binding equality checks can reject it.
func mintScanCursor(t *testing.T, s *scanStack, logical string, version int32) string {
	t.Helper()
	var branchID uuid.UUID
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT id FROM dataset_branches WHERE dataset_id = $1 AND name = 'main'`, s.dataset.ID).Scan(&branchID))
	var headTxn string
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT head_transaction_id::text FROM dataset_branches WHERE dataset_id = $1 AND name = 'main'`, s.dataset.ID).Scan(&headTxn))
	buf := []byte("s1")
	buf = append(buf, s.dataset.ID[:]...)
	buf = append(buf, branchID[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(version))
	txn := uuid.MustParse(headTxn)
	buf = append(buf, txn[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, 8)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(logical)))
	buf = append(buf, logical...)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// --- WP3.3 increment 2: predicate + all_files over the real stack ---

// scanFileSpec is one file of a multi-file outputs:commit.
type scanFileSpec struct {
	logical string
	content []byte
}

// commitSnapshotFiles publishes one SNAPSHOT version carrying several
// files through the real outputs:commit endpoint, registered in the GIVEN
// order (callers pass a non-sorted order so keyset order must differ from
// registration order). Returns the cut version.
func (s *scanStack) commitSnapshotFiles(t *testing.T, files []scanFileSpec) int32 {
	t.Helper()
	body := map[string]any{
		"branch":           "main",
		"transaction_type": "SNAPSHOT",
		"files":            make([]map[string]any, 0, len(files)),
	}
	for _, f := range files {
		body["files"] = append(body["files"].([]map[string]any), map[string]any{
			"logical_path":   f.logical,
			"content_base64": base64.StdEncoding.EncodeToString(f.content),
			"size_bytes":     len(f.content),
			"operation":      "ADD",
		})
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	rec := s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/outputs:commit", "application/json", bytes.NewReader(raw), s.tokenA)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var created struct {
		Transaction struct {
			ID string `json:"id"`
		} `json:"transaction"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	var version int32
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT version FROM dataset_versions WHERE dataset_id = $1 AND transaction_id = $2`,
		s.dataset.ID, created.Transaction.ID).Scan(&version))
	return version
}

func allFilesQuery(version int32, cursor string, limit int) string {
	return allFilesPredicateQuery(version, "", cursor, limit)
}

func allFilesPredicateQuery(version int32, predicate string, cursor string, limit int) string {
	q := url.Values{}
	q.Set("branch", "main")
	q.Set("version", fmt.Sprint(version))
	q.Set("all_files", "true")
	if predicate != "" {
		q.Set("predicate", predicate)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprint(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	return "?" + q.Encode()
}

type allFilesHTTPResponse struct {
	TransactionID string            `json:"transaction_id"`
	Mode          string            `json:"mode"`
	Rows          []json.RawMessage `json:"rows"`
	NextCursor    *string           `json:"next_cursor"`
	RowCount      int               `json:"row_count"`
	PageBytes     int64             `json:"page_bytes"`
	FilesAccessed int               `json:"files_accessed"`
}

func (s *scanStack) scanAllFiles(t *testing.T, query string) allFilesHTTPResponse {
	t.Helper()
	rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+query, "", nil, s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out allFilesHTTPResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Equal(t, "all_files", out.Mode)
	return out
}

// walkAllFilesReal pages an all_files scan to its explicit end, asserting
// the page budgets on every page.
func (s *scanStack) walkAllFilesReal(t *testing.T, version int32, predicate string, limit int) ([]string, int, int64, int) {
	t.Helper()
	var got []string
	pages := 0
	var bytesSum int64
	var opens int
	cursor := ""
	for {
		page := s.scanAllFiles(t, allFilesPredicateQuery(version, predicate, cursor, limit))
		pages++
		assert.LessOrEqual(t, page.RowCount, 1000)
		assert.LessOrEqual(t, page.PageBytes, int64(8<<20))
		assert.LessOrEqual(t, page.FilesAccessed, 16)
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

// TestRowsScanAllFilesRealRouter: one real outputs:commit registers 20
// files (> the 16-open page cap) OUT OF keyset order — including an empty
// file, a whitespace-only file and a no-match window — and all_files pages
// every record back value-by-value in stable path order with in-file order
// preserved, honoring the row/byte/file budgets, accounting every byte
// exactly once and terminating with an explicit cursor-less final page.
func TestRowsScanAllFilesRealRouter(t *testing.T) {
	s := newScanStack(t)

	const fileCount = 20
	expected := make([]string, 0, fileCount*2)
	byPath := map[string][]byte{}
	for i := 0; i < fileCount; i++ {
		name := fmt.Sprintf("part-%02d.ndjson", i)
		var content []byte
		switch i {
		case 5:
			content = []byte{} // empty file
		case 6:
			content = []byte(" \n\t\n  \n") // whitespace-only window
		case 7:
			// no-match window for the predicate used below
			content = []byte("{\"bucket\":99,\"i\":0}\n{\"bucket\":99,\"i\":1}\n")
		default:
			content = []byte(fmt.Sprintf("{\"bucket\":%d,\"i\":0,\"sku\":\"F%02d\"}\n{\"bucket\":%d,\"i\":1}\n", i%3, i, i%3))
		}
		byPath[name] = content
		switch {
		case i == 7: // the no-match window still yields its rows unfiltered
			expected = append(expected, `{"bucket":99,"i":0}`, `{"bucket":99,"i":1}`)
		case i != 5 && i != 6: // empty and whitespace files yield no rows
			expected = append(expected,
				fmt.Sprintf(`{"bucket":%d,"i":0,"sku":"F%02d"}`, i%3, i),
				fmt.Sprintf(`{"bucket":%d,"i":1}`, i%3))
		}
	}
	// Register in REVERSE path order: keyset order must come from the DB,
	// not the registration order.
	files := make([]scanFileSpec, 0, fileCount)
	for i := fileCount - 1; i >= 0; i-- {
		files = append(files, scanFileSpec{logical: fmt.Sprintf("part-%02d.ndjson", i), content: byPath[fmt.Sprintf("part-%02d.ndjson", i)]})
	}
	v1 := s.commitSnapshotFiles(t, files)

	got, pages, bytesSum, opens := s.walkAllFilesReal(t, v1, "", 3)
	assert.GreaterOrEqual(t, pages, 2, "row limit 3 spans many pages")
	assert.Equal(t, expected, got, "stable path order, in-file order, no loss, no dup")
	var total int64
	for _, c := range byPath {
		total += int64(len(c))
	}
	assert.Equal(t, total, bytesSum, "raw page bytes account for every committed byte exactly once")
	assert.GreaterOrEqual(t, opens, fileCount, "every file is opened at least once")
	assert.LessOrEqual(t, opens, fileCount+pages, "at most one mid-file resume reopen per page")

	// Predicate over the same version: only buckets 0 survive (files with
	// i%3==0), the empty/whitespace/no-match files still consume bytes.
	predGot, _, predBytes, _ := s.walkAllFilesReal(t, v1, `{"column":"bucket","op":"eq","value":0}`, 2)
	var predWant []string
	for i := 0; i < fileCount; i++ {
		if i == 5 || i == 6 || i == 7 {
			continue
		}
		if i%3 == 0 {
			predWant = append(predWant,
				fmt.Sprintf(`{"bucket":0,"i":0,"sku":"F%02d"}`, i),
				`{"bucket":0,"i":1}`)
		}
	}
	assert.Equal(t, predWant, predGot)
	assert.Equal(t, total, predBytes, "predicate consumes the same raw bytes")

	// File-budget split at a high row limit: nothing but the 16-open cap
	// can stop a page — page one opens exactly 16 files and carries a
	// cursor, the final page opens the remaining 4 without one, and the
	// union is exactly the full expected row set (no loss, no duplicate).
	pageOne := s.scanAllFiles(t, allFilesQuery(v1, "", 1000))
	assert.Equal(t, 16, pageOne.FilesAccessed, "first page stops on the file-open cap")
	assert.NotNil(t, pageOne.NextCursor)
	pageTwo := s.scanAllFiles(t, allFilesQuery(v1, *pageOne.NextCursor, 1000))
	assert.Equal(t, 4, pageTwo.FilesAccessed, "final page opens the remaining files")
	assert.Nil(t, pageTwo.NextCursor, "explicit cursor-less final page")
	var capRows []json.RawMessage
	capRows = append(capRows, pageOne.Rows...)
	capRows = append(capRows, pageTwo.Rows...)
	capStrings := make([]string, 0, len(capRows))
	for _, raw := range capRows {
		capStrings = append(capStrings, string(raw))
	}
	assert.Equal(t, expected, capStrings, "file-cap pages deliver every row exactly once")
	assert.Equal(t, fileCount, pageOne.FilesAccessed+pageTwo.FilesAccessed, "each file opened exactly once when the row limit does not interfere")
}

// TestRowsScanAllFilesByteBudgetRealRouter: a >8MiB whitespace-only file
// followed by a one-row file — the page byte budget must slice across the
// whitespace (empty pages with advancing cursors), deliver the next file's
// first row afterwards, and account for every byte exactly once.
func TestRowsScanAllFilesByteBudgetRealRouter(t *testing.T) {
	s := newScanStack(t)

	var big strings.Builder
	for big.Len() < 9<<20 {
		big.WriteString("   \n")
	}
	small := `{"first":true}` + "\n"
	v1 := s.commitSnapshotFiles(t, []scanFileSpec{
		{logical: "blank.ndjson", content: []byte(big.String())},
		{logical: "row.ndjson", content: []byte(small)},
	})

	var got []string
	pages := 0
	var bytesSum int64
	cursor := ""
	emptyPages := 0
	for {
		page := s.scanAllFiles(t, allFilesQuery(v1, cursor, 0))
		pages++
		assert.LessOrEqual(t, page.PageBytes, int64(8<<20), "page byte cap")
		assert.LessOrEqual(t, page.FilesAccessed, 16)
		if len(page.Rows) == 0 {
			emptyPages++
		}
		for _, raw := range page.Rows {
			got = append(got, string(raw))
		}
		bytesSum += page.PageBytes
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		require.Less(t, pages, 1000, "must terminate")
	}
	assert.GreaterOrEqual(t, pages, 2, "9MiB of whitespace cannot fit one page")
	assert.Positive(t, emptyPages, "whitespace pages are empty but advancing")
	assert.Equal(t, []string{`{"first":true}`}, got, "the next file's first row is delivered after the whitespace")
	assert.Equal(t, int64(big.Len()+len(small)), bytesSum, "every byte consumed exactly once across pages")
}

// TestRowsScanAllFilesTypedPredicateRealRouter: exact-typed eq semantics
// over the real stack — big-integer exactness, 1 vs 1.0, string vs number,
// null vs missing, case-sensitive columns.
func TestRowsScanAllFilesTypedPredicateRealRouter(t *testing.T) {
	s := newScanStack(t)
	files := []scanFileSpec{
		{logical: "typed.ndjson", content: []byte(strings.Join([]string{
			`{"id":9007199254740992,"tag":"float64-collapse-a"}`,
			`{"id":9007199254740993,"tag":"float64-collapse-b"}`,
			`{"id":1,"tag":"one"}`,
			`{"id":1.0,"tag":"one-float"}`,
			`{"id":"1","tag":"one-string"}`,
			`{"note":null,"tag":"explicit-null"}`,
			`{"tag":"missing-note"}`,
			`{"Tag":"case","tag":"lower"}`,
		}, "\n") + "\n")},
	}
	version := s.commitSnapshotFiles(t, files)

	rowsFor := func(predicate string) []string {
		got, _, _, _ := s.walkAllFilesReal(t, version, predicate, 0)
		return got
	}
	assert.Equal(t, []string{`{"id":9007199254740992,"tag":"float64-collapse-a"}`}, rowsFor(`{"column":"id","op":"eq","value":9007199254740992}`))
	assert.Equal(t, []string{`{"id":9007199254740993,"tag":"float64-collapse-b"}`}, rowsFor(`{"column":"id","op":"eq","value":9007199254740993}`))
	assert.Equal(t, []string{`{"id":1,"tag":"one"}`, `{"id":1.0,"tag":"one-float"}`}, rowsFor(`{"column":"id","op":"eq","value":1}`))
	assert.Equal(t, []string{`{"id":"1","tag":"one-string"}`}, rowsFor(`{"column":"id","op":"eq","value":"1"}`))
	assert.Equal(t, []string{`{"note":null,"tag":"explicit-null"}`}, rowsFor(`{"column":"note","op":"eq","value":null}`))
	assert.Empty(t, rowsFor(`{"column":"Tag","op":"eq","value":"lower"}`), "column names are case-sensitive")
}

// TestRowsScanAllFilesV2BetweenPagesAndRejections: publishing v2 between
// pages keeps the old all_files cursor on version 1's remaining files;
// cross-mode/mode/predicate/version bindings are rejected; cross-tenant is
// 404; the increment-1 path scan keeps its s1 behavior on the same stack.
func TestRowsScanAllFilesV2BetweenPagesAndRejections(t *testing.T) {
	s := newScanStack(t)
	v1 := s.commitSnapshotFiles(t, []scanFileSpec{
		{logical: "a.ndjson", content: scanContent(scanLines(4, "v1a"))},
		{logical: "b.ndjson", content: scanContent(scanLines(4, "v1b"))},
	})

	first := s.scanAllFiles(t, allFilesQuery(v1, "", 5))
	require.Len(t, first.Rows, 5)
	require.NotNil(t, first.NextCursor, "row limit must stop mid-scan")

	v2 := s.commitSnapshotFiles(t, []scanFileSpec{
		{logical: "a.ndjson", content: scanContent(scanLines(2, "v2a"))},
	})
	require.Greater(t, v2, v1)

	// Old cursor keeps reading v1 to its explicit end.
	var tail []string
	cursor := *first.NextCursor
	for {
		page := s.scanAllFiles(t, allFilesQuery(v1, cursor, 0))
		for _, raw := range page.Rows {
			tail = append(tail, string(raw))
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	want := append([]string(nil), scanLines(4, "v1a")...)
	want = append(want, scanLines(4, "v1b")...)
	assert.Equal(t, want[5:], tail, "old cursor finishes version 1 across files")

	// Explicit v2 all_files scan returns the new content.
	fresh := s.scanAllFiles(t, allFilesQuery(v2, "", 0))
	require.Len(t, fresh.Rows, 2)
	assert.Equal(t, scanLines(2, "v2a")[0], string(fresh.Rows[0]))

	// Cross-tenant all_files: 404, no leak.
	rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+allFilesQuery(v1, "", 0), "", nil, s.tokenB)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset not found")

	// Binding rejections on the real router.
	for name, q := range map[string]string{
		"cursor on new version":         allFilesQuery(v2, *first.NextCursor, 0),
		"cursor with changed predicate": allFilesPredicateQuery(v1, `{"column":"sku","op":"eq","value":"v1a-0001"}`, *first.NextCursor, 0),
		"all_files with path":           "?branch=main&version=1&all_files=true&path=a.ndjson",
		"all_files false":               "?branch=main&version=1&all_files=false",
		"empty predicate":               "?branch=main&version=1&all_files=true&predicate=",
		"predicate bad json":            allFilesPredicateQuery(v1, "{nope", "", 0),
		"predicate non-scalar":          allFilesPredicateQuery(v1, `{"column":"a","op":"eq","value":{"b":1}}`, "", 0),
		"s1 cursor on all_files":        allFilesQuery(v1, mintScanCursor(t, s, "a.ndjson", v1), 0),
	} {
		t.Run(name, func(t *testing.T) {
			rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+q, "", nil, s.tokenA)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}

	// The increment-1 single-file path mode keeps its s1 cursor format and
	// behavior on the same stack: page with a real s1 cursor, then replay.
	pathPage := s.scan(t, scanQuery(v1, "a.ndjson", "", 3), s.tokenA)
	require.NotNil(t, pathPage.NextCursor)
	rawCursor, err := base64.RawURLEncoding.DecodeString(*pathPage.NextCursor)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rawCursor), 2)
	assert.Equal(t, "s1", string(rawCursor[:2]), "path mode still issues s1-format cursors")
	pathNext := s.scan(t, scanQuery(v1, "a.ndjson", *pathPage.NextCursor, 0), s.tokenA)
	assert.Len(t, pathNext.Rows, 1, "4-line file minus 3 from page one")
	assert.Nil(t, pathNext.NextCursor)
}
