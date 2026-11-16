package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

func TestDatasetServiceOutputCommitterPostsMaterializedDatasetAndMetadata(t *testing.T) {
	outputID := uuid.New()
	inputID := uuid.New()
	var got datasetOutputCommitRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/datasets/ri.foundry.main.dataset."+outputID.String()+"/outputs:commit", r.URL.Path)
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"transaction":{"status":"COMMITTED"}}`))
	}))
	defer srv.Close()

	metadataCommitter := &recordingCommitter{}
	committer := DatasetServiceOutputCommitter{BaseURL: srv.URL, BearerToken: "test-token", Client: srv.Client(), Metadata: metadataCommitter}
	result := executor.NodeResult{
		OutputContentHash: "sha256:rows",
		Metadata: map[string]any{
			"runtime": "lightweight_table",
			"columns": []string{"trail_id", "distance_miles"},
			"data_rows": []map[string]json.RawMessage{
				{"trail_id": json.RawMessage(`"mesa"`), "distance_miles": json.RawMessage(`4.8`)},
				{"trail_id": json.RawMessage(`"green"`), "distance_miles": json.RawMessage(`6.1`)},
			},
		},
	}
	err := committer.Commit(context.Background(), executor.OutputTransaction{
		DatasetRID:       outputID.String(),
		TransactionRID:   "pipeline-run:one:output",
		DatasetName:      "Trail output",
		Branch:           "main",
		WriteMode:        "SNAPSHOT",
		FileFormat:       "PARQUET",
		LogicalPath:      "part-00000.ndjson",
		OutputNodeID:     "output",
		SourceNodeID:     "select",
		PipelineRID:      "pipeline-one",
		InputDatasetRIDs: []string{inputID.String()},
		CreateIfMissing:  true,
	}, result)
	require.NoError(t, err)

	require.True(t, got.CreateIfMissing)
	require.Equal(t, "Trail output", got.DatasetName)
	require.Equal(t, "main", got.Branch)
	require.Equal(t, "SNAPSHOT", got.TransactionType)
	require.NotNil(t, got.Schema)
	// data_rows are uploaded as NDJSON bytes, so the schema must report the
	// actual file format even when the transaction configured PARQUET.
	require.Equal(t, "TEXT", got.Schema.FileFormat)
	require.NotNil(t, got.Format)
	require.Equal(t, "TEXT", *got.Format)
	require.Equal(t, []datasetOutputField{
		{Name: "trail_id", Type: "STRING", Nullable: false},
		{Name: "distance_miles", Type: "DOUBLE", Nullable: false},
	}, got.Schema.Fields)
	require.Len(t, got.Files, 1)
	require.Equal(t, "part-00000.ndjson", got.Files[0].LogicalPath)
	require.Greater(t, got.Files[0].SizeBytes, int64(0))
	require.Equal(t, []string{"trail_id", "distance_miles"}, got.PreviewColumns)
	require.Len(t, got.PreviewRows, 2)
	require.Len(t, got.LineageLinks, 1)
	require.Equal(t, "ri.foundry.main.dataset."+inputID.String(), got.LineageLinks[0].TargetRID)
	require.Equal(t, []string{outputID.String()}, metadataCommitter.datasets)
	require.Equal(t, "sha256:rows", metadataCommitter.results[0].OutputContentHash)
}

func TestDatasetServiceOutputCommitterSkipsNonDatasetRIDs(t *testing.T) {
	metadataCommitter := &recordingCommitter{}
	committer := DatasetServiceOutputCommitter{BaseURL: "http://127.0.0.1:1", Metadata: metadataCommitter}
	err := committer.Commit(context.Background(), executor.OutputTransaction{DatasetRID: "out.alpha", TransactionRID: "txn"}, executor.NodeResult{OutputContentHash: "hash"})
	require.NoError(t, err)
	require.Equal(t, []string{"out.alpha"}, metadataCommitter.datasets)
}

func TestDatasetServiceOutputCommitterUploadsFullDataRowsContent(t *testing.T) {
	outputID := uuid.New()
	var rawBody []byte
	var got datasetOutputCommitRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		rawBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(rawBody, &got))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"transaction":{"status":"COMMITTED"}}`))
	}))
	defer srv.Close()

	rows := []map[string]json.RawMessage{
		{"trail_id": json.RawMessage(`"mesa"`)},
		{"trail_id": json.RawMessage(`"green"`)},
		{"trail_id": json.RawMessage(`"ridge"`)},
	}
	committer := DatasetServiceOutputCommitter{BaseURL: srv.URL, Client: srv.Client(), Metadata: &recordingCommitter{}}
	err := committer.Commit(context.Background(), executor.OutputTransaction{
		DatasetRID:      outputID.String(),
		TransactionRID:  "pipeline-run:full:output",
		Branch:          "main",
		LogicalPath:     "part-00000.ndjson",
		CreateIfMissing: true,
	}, executor.NodeResult{Metadata: map[string]any{
		"columns":   []string{"trail_id"},
		"data_rows": rows,
	}})
	require.NoError(t, err)

	require.Len(t, got.Files, 1)
	require.Equal(t, encodeNDJSON(rows), got.Files[0].ContentBase64)
	require.Equal(t, int64(len(encodeNDJSON(rows))), got.Files[0].SizeBytes)
	require.Contains(t, string(rawBody), `"content_base64":`)
}

func TestDatasetServiceOutputCommitterZeroRowsUploadsExplicitEmptyContent(t *testing.T) {
	outputID := uuid.New()
	var rawBody []byte
	var got datasetOutputCommitRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		rawBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(rawBody, &got))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"transaction":{"status":"COMMITTED"}}`))
	}))
	defer srv.Close()

	committer := DatasetServiceOutputCommitter{BaseURL: srv.URL, Client: srv.Client(), Metadata: &recordingCommitter{}}
	err := committer.Commit(context.Background(), executor.OutputTransaction{
		DatasetRID:      outputID.String(),
		TransactionRID:  "pipeline-run:empty:output",
		Branch:          "main",
		LogicalPath:     "part-00000.ndjson",
		CreateIfMissing: true,
	}, executor.NodeResult{Metadata: map[string]any{
		"columns":   []string{"trail_id"},
		"data_rows": []any{},
	}})
	require.NoError(t, err)

	require.Len(t, got.Files, 1)
	// Empty must stay distinguishable from absent on the wire: an explicit
	// "" uploads a zero-row NDJSON file instead of a metadata-only entry.
	require.NotNil(t, got.Files[0].ContentBase64)
	require.Empty(t, got.Files[0].ContentBase64)
	require.Contains(t, string(rawBody), `"content_base64":""`)
	require.Equal(t, int64(0), got.Files[0].SizeBytes)
}

func TestDatasetServiceOutputCommitterSampleOnlyStaysMetadataOnly(t *testing.T) {
	outputID := uuid.New()
	var got datasetOutputCommitRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"transaction":{"status":"COMMITTED"}}`))
	}))
	defer srv.Close()

	sample := []map[string]json.RawMessage{{"trail_id": json.RawMessage(`"mesa"`)}}
	committer := DatasetServiceOutputCommitter{BaseURL: srv.URL, Client: srv.Client(), Metadata: &recordingCommitter{}}
	err := committer.Commit(context.Background(), executor.OutputTransaction{
		DatasetRID:      outputID.String(),
		TransactionRID:  "pipeline-run:sample:output",
		Branch:          "main",
		FileFormat:      "PARQUET",
		LogicalPath:     "part-00000.ndjson",
		CreateIfMissing: true,
	}, executor.NodeResult{Metadata: map[string]any{
		"columns":     []string{"trail_id"},
		"sample_rows": sample,
	}})
	require.NoError(t, err)

	require.Len(t, got.Files, 1)
	require.Nil(t, got.Files[0].ContentBase64)
	require.Equal(t, int64(len(encodeNDJSON(sample))), got.Files[0].SizeBytes)
	// Metadata-only commits keep the configured format untouched.
	require.NotNil(t, got.Schema)
	require.Equal(t, "PARQUET", got.Schema.FileFormat)
}

func TestDatasetServiceOutputCommitterRejectsNonNDJSONLogicalPathWithFullRows(t *testing.T) {
	outputID := uuid.New()
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	committer := DatasetServiceOutputCommitter{BaseURL: srv.URL, Client: srv.Client(), Metadata: &recordingCommitter{}}
	err := committer.Commit(context.Background(), executor.OutputTransaction{
		DatasetRID:      outputID.String(),
		TransactionRID:  "pipeline-run:suffix:output",
		Branch:          "main",
		LogicalPath:     "part-00000.parquet",
		CreateIfMissing: true,
	}, executor.NodeResult{Metadata: map[string]any{
		"columns":   []string{"trail_id"},
		"data_rows": []map[string]json.RawMessage{{"trail_id": json.RawMessage(`"mesa"`)}},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "dataset_output_commit_invalid")
	require.Contains(t, err.Error(), "part-00000.parquet")
	require.Zero(t, requests)
}
