package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// snapshotTestServer is a minimal DVS rows:scan double for the WP3.4
// pinned-input path: it records every request (method, path, query, auth)
// and serves fixed-shape pages over a fixed record set with a fixed
// cutting transaction, echoing branch/version from the query unless an
// override is installed.
type snapshotTestServer struct {
	server      *httptest.Server
	requests    []snapshotTestRequest
	records     [][]byte
	txn         string
	versionEcho string // "" = echo the requested version
	pageRows    int
	// txnByPage optionally overrides the reported transaction per page
	// (1-based) for later-page mismatch controls.
	txnByPage func(page int) string
	// rowsFirst writes the rows array before the binding fields so the
	// client can only post-validate them after the rows streamed.
	rowsFirst bool
}

type snapshotTestRequest struct {
	Path  string
	Query string
	Auth  string
}

func newSnapshotTestServer(t *testing.T, records [][]byte) *snapshotTestServer {
	t.Helper()
	s := &snapshotTestServer{records: records, txn: "11111111-1111-1111-1111-111111111111", pageRows: 500}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/datasets/", func(w http.ResponseWriter, r *http.Request) {
		s.requests = append(s.requests, snapshotTestRequest{Path: r.URL.Path, Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization")})
		if !strings.HasSuffix(r.URL.Path, "/files/rows:scan") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"unexpected path","code":"not_found"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.scanPage(r))
	})
	s.server = httptest.NewServer(mux)
	t.Cleanup(s.server.Close)
	return s
}

func (s *snapshotTestServer) scanPage(r *http.Request) []byte {
	q := r.URL.Query()
	version := q.Get("version")
	if s.versionEcho != "" {
		version = s.versionEcho
	}
	datasetUUID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/datasets/"), "/files/rows:scan")
	datasetUUID = strings.TrimPrefix(datasetUUID, "ri.foundry.main.dataset.")
	offset := 0
	if c := q.Get("cursor"); c != "" {
		fmt.Sscanf(c, "off%d", &offset)
	}
	end := offset + s.pageRows
	if end > len(s.records) {
		end = len(s.records)
	}
	txn := s.txn
	if s.txnByPage != nil {
		txn = s.txnByPage(len(s.requests))
	}
	var b strings.Builder
	if s.rowsFirst {
		// Binding fields AFTER the rows: the client can only post-validate
		// them once the object ends — the ordering used by the
		// unverified-sample negative controls.
		b.WriteString(`{"rows":[`)
		for i := offset; i < end; i++ {
			if i > offset {
				b.WriteByte(',')
			}
			b.Write(s.records[i])
		}
		b.WriteString(`],"row_count":` + fmt.Sprint(end-offset))
		if end < len(s.records) {
			b.WriteString(`,"next_cursor":"off` + fmt.Sprint(end) + `"`)
		} else {
			b.WriteString(`,"next_cursor":null`)
		}
		b.WriteString(`,"dataset_id":"` + datasetUUID + `","branch":` + jsonString(q.Get("branch")) + `,"version":` + version + `,"transaction_id":"` + txn + `","page_bytes":100}`)
		return []byte(b.String())
	}
	b.WriteString(`{"dataset_id":"` + datasetUUID + `","branch":` + jsonString(q.Get("branch")) + `,"version":` + version + `,"transaction_id":"` + txn + `","rows":[`)
	for i := offset; i < end; i++ {
		if i > offset {
			b.WriteByte(',')
		}
		b.Write(s.records[i])
	}
	b.WriteString(`],"row_count":` + fmt.Sprint(end-offset))
	if end < len(s.records) {
		b.WriteString(`,"next_cursor":"off` + fmt.Sprint(end) + `"`)
	} else {
		b.WriteString(`,"next_cursor":null`)
	}
	b.WriteString(`,"page_bytes":100}`)
	return []byte(b.String())
}

func snapshotConfig(datasetID string, snapshot string) string {
	return `{"dataset_id":"` + datasetID + `","dataset_snapshot":` + snapshot + `}`
}

const validSnapshot = `{"branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111"}`

func requireSnapshotParse(t *testing.T, raw string) *datasetInputSnapshot {
	t.Helper()
	snapshot, err := parseDatasetInputSnapshot(json.RawMessage(raw))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	return snapshot
}

func TestParseDatasetInputSnapshotFieldValidation(t *testing.T) {
	t.Run("valid canonical transaction RID", func(t *testing.T) {
		snapshot := requireSnapshotParse(t, validSnapshot)
		assert.Equal(t, "main", snapshot.Branch)
		assert.Equal(t, int32(3), snapshot.Version)
		assert.Equal(t, "11111111-1111-1111-1111-111111111111", snapshot.TransactionID.String())
	})
	t.Run("valid bare transaction UUID", func(t *testing.T) {
		requireSnapshotParse(t, `{"branch":"dev","version":1,"transaction_id":"22222222-2222-2222-2222-222222222222"}`)
	})
	t.Run("absent snapshot is nil, not an error", func(t *testing.T) {
		snapshot, err := parseDatasetInputSnapshot(nil)
		require.NoError(t, err)
		assert.Nil(t, snapshot)
	})
	for name, raw := range map[string]string{
		"missing branch":        `{"version":3,"transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"blank branch":          `{"branch":"  ","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"missing version":       `{"branch":"main","transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"zero version":          `{"branch":"main","version":0,"transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"negative version":      `{"branch":"main","version":-2,"transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"string version":        `{"branch":"main","version":"3","transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"fractional version":    `{"branch":"main","version":1.5,"transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"version out of range":  `{"branch":"main","version":2147483648,"transaction_id":"11111111-1111-1111-1111-111111111111"}`,
		"missing transaction":   `{"branch":"main","version":3}`,
		"transaction not uuid":  `{"branch":"main","version":3,"transaction_id":"not-a-rid"}`,
		"transaction wrong typ": `{"branch":"main","version":3,"transaction_id":42}`,
		"not an object":         `["main",3,"x"]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseDatasetInputSnapshot(json.RawMessage(raw))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "dataset_snapshot_invalid")
		})
	}
}

func TestInputNodeSnapshotWithLegacyVersionStillRejected(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"a":1}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source
	config := `{"dataset_id":"` + uuid.NewString() + `","dataset_version":"3","dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_version_not_supported")
	assert.Empty(t, source.calls)
}

func TestInputNodeSnapshotBranchConflictRejected(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	config := `{"dataset_id":"` + uuid.NewString() + `","dataset_branch":"dev","dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_conflict")
}

func TestInputNodeSnapshotAmbiguousReferenceRejected(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	a, b := uuid.NewString(), uuid.NewString()
	config := `{"dataset_rid":"ri.foundry.main.dataset.` + a + `","dataset_id":"` + b + `","dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_ambiguous_reference")
}

func TestInputNodeSnapshotWithoutDatasetReferenceRejected(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	config := `{"dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_invalid")
	assert.Contains(t, err.Error(), "without any dataset reference")
}

func TestInputNodeSnapshotPropagatesPinToSource(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"a":1}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source
	datasetID := uuid.NewString()
	config := snapshotConfig(datasetID, validSnapshot)
	result, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	call := source.calls[0]
	require.NotNil(t, call.Snapshot)
	assert.Equal(t, "main", call.Snapshot.Branch)
	assert.Equal(t, int32(3), call.Snapshot.Version)
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", call.Snapshot.TransactionID.String())
	assert.False(t, call.AllowTruncation)
	assert.Equal(t, maxDatasetInputRows, call.Limit)
	assert.Equal(t, "main", result.Metadata["dataset_branch"])
	assert.Equal(t, int32(3), result.Metadata["dataset_version"])
	assert.Equal(t, "ri.foundry.main.transaction.11111111-1111-1111-1111-111111111111", result.Metadata["dataset_transaction_id"])
}

func newSnapshotReaderTestEnv(t *testing.T, records [][]byte) (DatasetServiceRowReader, *snapshotTestServer) {
	t.Helper()
	fake := newSnapshotTestServer(t, records)
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	return DatasetServiceRowReader{BaseURL: fake.server.URL}, fake
}

func callerCtx() context.Context {
	return contextWithCallerBearer(context.Background(), "caller-jwt")
}

func snapshotRequest(datasetID string) DatasetRowsRequest {
	return DatasetRowsRequest{
		DatasetRID: datasetID,
		Limit:      maxDatasetInputRows,
		Snapshot:   &datasetInputSnapshot{Branch: "main", Version: 3, TransactionID: uuid.MustParse("11111111-1111-1111-1111-111111111111")},
	}
}

func TestDatasetSnapshotReadMaterializesScanPages(t *testing.T) {
	records := [][]byte{
		[]byte(`{"n":1,"big":123456789012345678901234567890}`),
		[]byte(`{"n":2}`),
		[]byte(`{"n":3}`),
		[]byte(`{"n":4}`),
	}
	fake := newSnapshotTestServer(t, records)
	fake.pageRows = 2 // two pages exercise the cursor loop
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	reader := DatasetServiceRowReader{BaseURL: fake.server.URL}

	datasetID := uuid.NewString()
	result, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(datasetID))
	require.NoError(t, err)
	require.Len(t, result.Rows, 4)
	assert.Equal(t, json.RawMessage(`123456789012345678901234567890`), result.Rows[0]["big"]) // numeric precision survives
	assert.Equal(t, datasetID, result.DatasetID)
	assert.False(t, result.Truncated)
	assert.Equal(t, "ndjson", result.Format)
	require.Len(t, fake.requests, 2)
	assert.Equal(t, "/v1/datasets/"+datasetID+"/files/rows:scan", fake.requests[0].Path)
	assert.Contains(t, fake.requests[0].Query, "branch=main")
	assert.Contains(t, fake.requests[0].Query, "version=3")
	assert.Contains(t, fake.requests[0].Query, "all_files=true")
	assert.Contains(t, fake.requests[1].Query, "cursor=off2")
	assert.Equal(t, "Bearer caller-jwt", fake.requests[0].Auth)
	assert.Equal(t, "Bearer caller-jwt", fake.requests[1].Auth)
}

func TestDatasetSnapshotReadWithPathSendsPathNotAllFiles(t *testing.T) {
	fake := newSnapshotTestServer(t, [][]byte{[]byte(`{"n":1}`)})
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
	req := snapshotRequest(uuid.NewString())
	req.Path = "part-00000.ndjson"
	_, err := reader.ReadDatasetRows(callerCtx(), req)
	require.NoError(t, err)
	require.Len(t, fake.requests, 1)
	assert.Contains(t, fake.requests[0].Query, "path=part-00000.ndjson")
	assert.NotContains(t, fake.requests[0].Query, "all_files")
}

func TestDatasetSnapshotReadFirstPageTransactionMismatchFails(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.txn = "33333333-3333-3333-3333-333333333333"
	_, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batched_scan_binding_invalid")
	assert.Contains(t, err.Error(), "11111111-1111-1111-1111-111111111111")
}

func TestDatasetSnapshotReadLaterPageTransactionMismatchFails(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)}
	fake := newSnapshotTestServer(t, records)
	fake.pageRows = 2
	fake.txnByPage = func(page int) string {
		if page > 1 {
			return "33333333-3333-3333-3333-333333333333"
		}
		return fake.txn
	}
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
	_, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batched_scan_binding_invalid")
	assert.GreaterOrEqual(t, len(fake.requests), 2, "the mismatch must surface on the later page")
}

func TestDatasetSnapshotReadVersionMismatchFails(t *testing.T) {
	fake := newSnapshotTestServer(t, [][]byte{[]byte(`{"n":1}`)})
	fake.versionEcho = "9"
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
	_, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "batched_scan_binding_invalid")
	assert.Contains(t, err.Error(), "version 9")
}

func TestDatasetSnapshotReadRequiresCallerBearer(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	reader.BearerToken = "service-token-that-must-not-be-used"
	_, err := reader.ReadDatasetRows(context.Background(), snapshotRequest(uuid.NewString()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_caller_required")
	assert.Empty(t, fake.requests, "no request may leave without the caller JWT — the service token is not a fallback")
}

func TestDatasetSnapshotReadExecutionRowCapFailsExplicitly(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)}
	reader, _ := newSnapshotReaderTestEnv(t, records)
	req := snapshotRequest(uuid.NewString())
	req.Limit = 2
	req.AllowTruncation = false
	_, err := reader.ReadDatasetRows(callerCtx(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_row_limit_exceeded")
}

func TestDatasetSnapshotReadPreviewOverLimitFails(t *testing.T) {
	// Pinned preview matches execution on over-limit: an early sampling
	// exit could return rows whose page binding fields (trailing the rows)
	// were never validated. Unpinned preview reads keep their legacy
	// sampling on the /files/rows route.
	records := [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)}
	reader, _ := newSnapshotReaderTestEnv(t, records)
	req := snapshotRequest(uuid.NewString())
	req.Limit = 2
	req.AllowTruncation = true
	_, err := reader.ReadDatasetRows(callerCtx(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_row_limit_exceeded")
}

func TestDatasetSnapshotReadRowsBeforeBindingNeverYieldsUnverifiedSample(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)}
	otherTxn := "33333333-3333-3333-3333-333333333333"

	t.Run("over limit fails before any binding is known", func(t *testing.T) {
		fake := newSnapshotTestServer(t, records)
		fake.rowsFirst = true
		fake.txn = otherTxn // wrong transaction, revealed only after rows
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
		t.Cleanup(restore)
		reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
		req := snapshotRequest(uuid.NewString())
		req.Limit = 2
		req.AllowTruncation = true // even preview must not sample unverified rows
		_, err := reader.ReadDatasetRows(callerCtx(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_input_row_limit_exceeded")
	})

	t.Run("bad trailing transaction fails the read despite streamed rows", func(t *testing.T) {
		fake := newSnapshotTestServer(t, records)
		fake.rowsFirst = true
		fake.txn = otherTxn
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
		t.Cleanup(restore)
		reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
		_, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "batched_scan_binding_invalid")
	})
}

func TestDatasetSnapshotReadByteCapFailsExplicitly(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1,"pad":"0123456789` + strings.Repeat("x", 32) + `"}`)}
	reader, _ := newSnapshotReaderTestEnv(t, records)
	reader.SnapshotMaxTotalBytes = 16
	_, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_bytes_exceeded")
}

func TestDatasetSnapshotReadWithoutSnapshotKeepsLegacyRowsRoute(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		rid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/datasets/"), "/files/rows")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"dataset_id":"` + rid + `","path":"f.ndjson","format":"ndjson","source":"backing_file","columns":[],"rows":[{"a":1}],"truncated":false}`))
	}))
	defer server.Close()
	reader := DatasetServiceRowReader{BaseURL: server.URL, BearerToken: "svc"}
	datasetID := uuid.NewString()
	result, err := reader.ReadDatasetRows(contextWithCallerBearer(context.Background(), "caller"), DatasetRowsRequest{DatasetRID: datasetID, Limit: 10})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, paths, 1)
	assert.Equal(t, "/v1/datasets/"+datasetID+"/files/rows", paths[0])
}

func TestScanStreamExpectedTransactionBindsFirstPage(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1}`)}
	fake := newSnapshotTestServer(t, records)
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	client, err := newBatchedDatasetClient()
	require.NoError(t, err)
	expected := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	input := batchedScanInput{DatasetRID: "ri.foundry.main.dataset." + uuid.NewString(), Branch: "main", Version: 3, ExpectedTransactionID: &expected}

	t.Run("matching transaction streams", func(t *testing.T) {
		stats, err := client.ScanStream(callerCtx(), input, func([]byte) error { return nil })
		require.NoError(t, err)
		assert.Equal(t, int64(1), stats.Rows)
	})
	t.Run("mismatching transaction fails page one", func(t *testing.T) {
		other := uuid.MustParse("33333333-3333-3333-3333-333333333333")
		bad := input
		bad.ExpectedTransactionID = &other
		_, err := client.ScanStream(callerCtx(), bad, func([]byte) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "batched_scan_binding_invalid")
	})
}

func TestInputNodeSnapshotPreviewEntranceReadsPin(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`), []byte(`{"n":3}`)}
	fake := newSnapshotTestServer(t, records)
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	rt := newLightweightTableRuntime()
	rt.datasets = DatasetServiceRowReader{BaseURL: fake.server.URL}
	rt.datasetRowsPreview = true
	datasetID := uuid.NewString()
	config := `{"dataset_id":"` + datasetID + `","dataset_snapshot":` + validSnapshot + `}`
	result, err := rt.Run(callerCtx(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)
	rows := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 3) // within the sample bound: everything the pin holds
	assert.Equal(t, false, result.Metadata["dataset_rows_truncated"])
	assert.Equal(t, int32(3), result.Metadata["dataset_version"])
}

func TestInputNodeSnapshotShadowedByInlineRowsRejected(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	config := `{"rows":[{"a":1}],"dataset_id":"` + uuid.NewString() + `","dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
	assert.Contains(t, err.Error(), "inline rows")
}

func TestInputNodeSnapshotShadowedByDependentPassthroughRejected(t *testing.T) {
	// The guard fires before the dependency read: a passthrough WITH
	// dependencies never reaches the pinned dataset read.
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	config := `{"dataset_id":"` + uuid.NewString() + `","dataset_snapshot":` + validSnapshot + `}`
	node := inputNodeContext("in", config, nil)
	node.Node.DependsOn = []string{"upstream"}
	_, err := rt.Run(context.Background(), node, json.RawMessage(config), "passthrough")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
}

func TestInputNodeSnapshotOnNonInputTransformRejected(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	config := `{"predicate":"n > 1","dataset_id":"` + uuid.NewString() + `","dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("f", config, nil), json.RawMessage(config), "filter")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
}

func TestInputNodeSnapshotMixedTopLevelAndNestedConfigRejected(t *testing.T) {
	const nested = `"config":{"dataset_id":"22222222-2222-2222-2222-222222222222","dataset_snapshot":` + validSnapshot + `}`
	for name, payload := range map[string]string{
		"pin at top level, other fields nested": `{"dataset_id":"` + uuid.NewString() + `","config":{"predicate":"n > 1"},"dataset_snapshot":` + validSnapshot + `}`,
		"pin nested, other fields top level":    `{"predicate":"n > 1","config":{"dataset_id":"` + uuid.NewString() + `","dataset_snapshot":` + validSnapshot + `}}`,
		"pin on both levels":                    `{"dataset_id":"` + uuid.NewString() + `","dataset_snapshot":` + validSnapshot + `,"config":{"dataset_snapshot":` + validSnapshot + `}}`,
		"pin nested, dataset RID top level":     `{"dataset_rid":"11111111-1111-1111-1111-111111111111",` + nested + `}`,
		"pin nested, legacy version top level":  `{"dataset_version":"3",` + nested + `}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseTableRuntimeConfig(json.RawMessage(payload))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
		})
	}
}

func TestInputNodeSnapshotPurelyNestedConfigWorks(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"a":1}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source
	datasetID := uuid.NewString()
	payload := `{"config":{"dataset_id":"` + datasetID + `","dataset_snapshot":` + validSnapshot + `}}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", payload, nil), json.RawMessage(payload), "dataset_input")
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	require.NotNil(t, source.calls[0].Snapshot)
}

func TestInputNodeSnapshotMixedConfigWithoutPinKeepsLegacyTopLevelWins(t *testing.T) {
	// No pin involved: the historical mixed-config behavior (top-level
	// fields win, nested config ignored) is unchanged.
	cfg, err := parseTableRuntimeConfig(json.RawMessage(`{"predicate":"a > 0","config":{"predicate":"b > 0"}}`))
	require.NoError(t, err)
	assert.Equal(t, "a > 0", cfg.Predicate)
}

func TestInputNodeSnapshotAllCandidatesMustAgree(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	a, b := uuid.NewString(), uuid.NewString()
	config := `{"dataset_id":"` + a + `","input_dataset_ids":["` + a + `","` + b + `"],"dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_ambiguous_reference")
}

func TestInputNodeSnapshotZeroUUIDDatasetRejected(t *testing.T) {
	rt := newLightweightTableRuntime()
	rt.datasets = &fakeDatasetRowSource{}
	config := `{"dataset_id":"00000000-0000-0000-0000-000000000000","dataset_snapshot":` + validSnapshot + `}`
	_, err := rt.Run(context.Background(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_invalid_reference")
}

func TestPreviewPipelineNodeSnapshotResolvesMetadataInputDatasetIDs(t *testing.T) {
	// The REAL preview helper (the same function the HTTP preview handler
	// calls): the pin's dataset comes from the persisted node's
	// InputDatasetIDs metadata, not from the payload.
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"n":1}`)}
	datasetID := uuid.NewString()
	nodes := []models.PipelineNode{{
		ID:              "in",
		TransformType:   "dataset_input",
		InputDatasetIDs: []uuid.UUID{uuid.MustParse(datasetID)},
		Config:          json.RawMessage(`{"dataset_snapshot":` + validSnapshot + `}`),
	}}
	ports := ExecutionPorts{DatasetRows: source}
	resp, err := executeLocalPipelinePreviewWithPorts(callerCtx(), uuid.New(), "in", nodes, 10, ports)
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	require.NotNil(t, source.calls[0].Snapshot)
	assert.Equal(t, "main", source.calls[0].Snapshot.Branch)
	require.Len(t, resp.Rows, 1)
}

func TestDatasetSnapshotReaderHonorsInjectedBaseURLWithoutGlobalSlot(t *testing.T) {
	records := [][]byte{[]byte(`{"n":1}`)}
	fake := newSnapshotTestServer(t, records)
	// No SetBatchedDatasetClient: the reader's own BaseURL must drive the
	// scan client (defaults stay the batched defaults).
	reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
	result, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, fake.requests, 1)
}

func TestDatasetSnapshotReaderRefusesRedirectsWithInjectedClient(t *testing.T) {
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://evil.invalid/x")
		w.WriteHeader(http.StatusFound)
	}))
	defer redirector.Close()
	reader := DatasetServiceRowReader{BaseURL: redirector.URL, Client: redirector.Client()}
	_, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
	require.Error(t, err)
	// The redirect was refused (first response returned): the 302 surfaces
	// as a dataset error, never as a followed redirect to a foreign host.
	assert.Contains(t, err.Error(), "batched_dataset_302")
}

func TestInputNodeSnapshotDeeplyNestedMixedConfigRejected(t *testing.T) {
	// Top-level fields + a pin two config levels down: the shared probe
	// walks the chain, so the shadowing rejection still fires.
	_, err := parseTableRuntimeConfig(json.RawMessage(`{"rows":[{"a":1}],"config":{"config":{"dataset_snapshot":` + validSnapshot + `}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
}

// runnerNodeContext builds a NodeContext with explicit dispatch metadata
// (inputNodeContext forces transform_type=dataset_input, which would hide
// the dispatch arms under test).
func runnerNodeContext(id, transform, payload string, extra map[string]any) executor.NodeContext {
	metadata := map[string]any{
		"logic_kind":     "transform",
		"transform_type": transform,
		"logic_payload":  json.RawMessage(payload),
	}
	for k, v := range extra {
		metadata[k] = v
	}
	return executor.NodeContext{
		BuildID:     uuid.New(),
		BuildBranch: "master",
		Attempt:     1,
		Node:        executor.Node{ID: id, JobID: uuid.New(), Metadata: metadata},
	}
}

func TestRuntimeNodeRunnerPinDispatchGuard(t *testing.T) {
	pinConfig := `{"dataset_id":"` + uuid.NewString() + `","dataset_snapshot":` + validSnapshot + `}`
	table := newLightweightTableRuntime()
	table.datasets = &fakeDatasetRowSource{result: datasetRowsResult(t, `{"a":1}`)}

	t.Run("python bypass rejected before sidecar check", func(t *testing.T) {
		runner := runtimeNodeRunner{Table: table} // Python nil on purpose
		_, err := runner.Run(context.Background(), runnerNodeContext("p", "python", pinConfig, nil))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
	})
	t.Run("distributed dispatch rejected", func(t *testing.T) {
		runner := runtimeNodeRunner{Table: table}
		_, err := runner.Run(context.Background(), runnerNodeContext("d", "sql", pinConfig, map[string]any{"pipeline_type": "distributed"}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
	})
	t.Run("non-lightweight fallback rejected", func(t *testing.T) {
		runner := runtimeNodeRunner{Table: table}
		_, err := runner.Run(context.Background(), runnerNodeContext("w", "some_exotic_transform", pinConfig, nil))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_snapshot_shadowed")
	})
	t.Run("lightweight dataset_input allowed", func(t *testing.T) {
		runner := runtimeNodeRunner{Table: table}
		result, err := runner.Run(context.Background(), runnerNodeContext("in", "dataset_input", pinConfig, nil))
		require.NoError(t, err)
		assert.NotEmpty(t, result.Metadata["dataset_transaction_id"])
	})
	t.Run("unpinned python keeps legacy error", func(t *testing.T) {
		runner := runtimeNodeRunner{Table: table}
		_, err := runner.Run(context.Background(), runnerNodeContext("p2", "python", `{"source":"print(1)"}`, nil))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "python_sidecar_not_configured")
	})
}

func TestPreviewPipelineNodeWithoutPinKeepsLegacySyntheticBehavior(t *testing.T) {
	// No pin: the preview metadata must NOT gain input_dataset_ids — a
	// reference-less input node keeps its synthetic rows.
	source := &fakeDatasetRowSource{}
	nodes := []models.PipelineNode{{
		ID:              "in",
		TransformType:   "dataset_input",
		InputDatasetIDs: []uuid.UUID{uuid.New()},
		Config:          json.RawMessage(`{}`),
	}}
	ports := ExecutionPorts{DatasetRows: source}
	resp, err := executeLocalPipelinePreviewWithPorts(context.Background(), uuid.New(), "in", nodes, 10, ports)
	require.NoError(t, err)
	assert.Empty(t, source.calls, "unpinned preview must not read datasets through newly attached metadata")
	assert.NotEmpty(t, resp.Rows)
}
