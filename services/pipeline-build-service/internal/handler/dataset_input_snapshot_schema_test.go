package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// expected_schema_hash inside a dataset_snapshot pin. Only an absent key
// skips the check; a present key must be the digest. The precondition is
// proved against the dataset service's versioned schema route as the
// caller, before the first scan page, and an unusable answer fails the
// node so nothing is scanned or published from that input.

const (
	schemaHash         = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherSchemaHash    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pinnedSnapshotTxn  = "11111111-1111-1111-1111-111111111111"
	otherSnapshotTxn   = "33333333-3333-3333-3333-333333333333"
	hashMismatchAnswer = `{"error":"dataset_schema_content_hash_mismatch: the committed schema does not match expected_content_hash","code":"dataset_schema_content_hash_mismatch"}`
)

// snapshotWithHash returns a complete pin that declares the given raw
// expected_schema_hash value verbatim, for example `null` or `""`.
func snapshotWithHash(rawHash string) string {
	return strings.TrimSuffix(validSnapshot, "}") + `,"expected_schema_hash":` + rawHash + `}`
}

// pinnedHashRequest reads the pin with a well-formed expected schema hash.
func pinnedHashRequest(datasetID string) DatasetRowsRequest {
	req := snapshotRequest(datasetID)
	snapshot, err := parseDatasetInputSnapshot(json.RawMessage(snapshotWithHash(`"` + schemaHash + `"`)))
	if err != nil {
		panic(err)
	}
	req.Snapshot = snapshot
	return req
}

// versionedSchemaAnswer is the dataset service's answer for the pinned
// dataset, branch "main", version 3, cut by the given transaction, with
// the given content hash.
func versionedSchemaAnswer(datasetID, branch, version, txn, hash string) []byte {
	return []byte(fmt.Sprintf(`{"dataset_id":%q,"branch":%q,"version":%s,"transaction_id":%q,"content_hash":%q,`+
		`"fields":[{"name":"n","field_type":"STRING","nullable":false}]}`, datasetID, branch, version, txn, hash))
}

// requestKinds lists the route each recorded request went to, in order:
// "schema" or "scan".
func requestKinds(requests []snapshotTestRequest) []string {
	kinds := make([]string, 0, len(requests))
	for _, request := range requests {
		switch {
		case strings.HasSuffix(request.Path, "/schema"):
			kinds = append(kinds, "schema")
		case strings.HasSuffix(request.Path, "/files/rows:scan"):
			kinds = append(kinds, "scan")
		default:
			kinds = append(kinds, "other")
		}
	}
	return kinds
}

func scanRequests(fake *snapshotTestServer) []snapshotTestRequest {
	var scans []snapshotTestRequest
	for _, request := range fake.requests {
		if strings.HasSuffix(request.Path, "/files/rows:scan") {
			scans = append(scans, request)
		}
	}
	return scans
}

// answerSchema installs a fixed answer for the double's schema route.
func (s *snapshotTestServer) answerSchema(status int, body []byte) {
	s.schemaStatus, s.schemaBody = status, body
}

func TestParseDatasetInputSnapshotExpectedSchemaHash(t *testing.T) {
	without, err := parseDatasetInputSnapshot(json.RawMessage(validSnapshot))
	require.NoError(t, err)
	assert.Equal(t, "", without.ExpectedSchemaHash, "an absent key keeps the pin unchecked")

	with, err := parseDatasetInputSnapshot(json.RawMessage(snapshotWithHash(`"` + schemaHash + `"`)))
	require.NoError(t, err)
	assert.Equal(t, schemaHash, with.ExpectedSchemaHash)

	// The #78 rule: the key present means the digest, so a generated pin
	// that passes null or "" can never silently drop its precondition.
	for name, raw := range map[string]string{
		"null":            `null`,
		"empty string":    `""`,
		"blank":           `" "`,
		"one short":       `"` + schemaHash[:63] + `"`,
		"one long":        `"` + schemaHash + `a"`,
		"uppercase":       `"` + strings.ToUpper(schemaHash) + `"`,
		"not hexadecimal": `"` + strings.Repeat("g", 64) + `"`,
		"number":          `64`,
		"boolean":         `true`,
		"array":           `[]`,
		"object":          `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, err := parseDatasetInputSnapshot(json.RawMessage(snapshotWithHash(raw)))
			require.Error(t, err)
			assert.Nil(t, snapshot)
			assert.Contains(t, err.Error(), "dataset_snapshot_invalid: expected_schema_hash must be a lowercase hexadecimal SHA-256 digest")
		})
	}
}

func TestDatasetSnapshotReadSchemaHashMismatchRejectsBeforeAnyScan(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusPreconditionFailed, []byte(hashMismatchAnswer))
	datasetID := uuid.NewString()

	_, err := reader.ReadDatasetRows(callerCtx(), pinnedHashRequest(datasetID))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_schema_rejected")
	assert.Contains(t, err.Error(), "dataset_schema_content_hash_mismatch")
	assert.Empty(t, scanRequests(fake), "a refused schema is answered before the first scan page")
	require.Len(t, fake.requests, 1, "the schema request is the whole exchange")
	assert.Equal(t, []string{"schema"}, requestKinds(fake.requests))
	schema := fake.requests[0]
	assert.Equal(t, "/v1/datasets/"+datasetID+"/schema", schema.Path)
	assert.Equal(t, "Bearer caller-jwt", schema.Auth, "the proof runs as the caller, never a service token")
	query, err := url.ParseQuery(schema.Query)
	require.NoError(t, err)
	assert.Equal(t, url.Values{
		"branch":                {"main"},
		"version":               {"3"},
		"transaction_id":        {pinnedSnapshotTxn},
		"expected_content_hash": {schemaHash},
	}, query)
}

func TestDatasetSnapshotReadSchemaHashRejectsUnusableAnswersBeforeAnyScan(t *testing.T) {
	datasetID := uuid.NewString()
	for _, tc := range []struct {
		name   string
		status int
		body   []byte
		want   string
	}{
		{"another transaction cut the version", http.StatusConflict,
			[]byte(`{"error":"dataset_schema_transaction_mismatch: version 3 was cut by transaction ` + pinnedSnapshotTxn + `, not ` + otherSnapshotTxn + `","code":"dataset_schema_transaction_mismatch"}`),
			"dataset_schema_transaction_mismatch"},
		{"schema of that format is unsupported", http.StatusUnprocessableEntity,
			[]byte(`{"error":"dataset_schema_format_unsupported: versioned schemas require Parquet ADD files or one NDJSON output with a schema in its commit receipt","code":"dataset_schema_format_unsupported"}`),
			"dataset_schema_format_unsupported"},
		{"dataset not found", http.StatusNotFound, []byte(`{"error":"not found","code":"not_found"}`), "batched_dataset_404"},
		{"dataset service failed", http.StatusInternalServerError, []byte(`{"error":"boom"}`), "batched_dataset_500"},
		{"answer is not JSON", http.StatusOK, []byte(`<html>gateway</html>`), "batched_schema_response_invalid"},
		{"answer is not an object", http.StatusOK, []byte(`[]`), "batched_schema_response_invalid"},
		{"answer names another dataset", http.StatusOK, versionedSchemaAnswer(uuid.NewString(), "main", "3", pinnedSnapshotTxn, schemaHash), "batched_schema_binding_invalid"},
		{"answer names another branch", http.StatusOK, versionedSchemaAnswer(datasetID, "dev", "3", pinnedSnapshotTxn, schemaHash), "batched_schema_binding_invalid"},
		{"answer names another version", http.StatusOK, versionedSchemaAnswer(datasetID, "main", "9", pinnedSnapshotTxn, schemaHash), "batched_schema_binding_invalid"},
		{"answer names another transaction", http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", otherSnapshotTxn, schemaHash), "batched_schema_binding_invalid"},
		{"answer names no transaction", http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", "00000000-0000-0000-0000-000000000000", schemaHash), "batched_schema_binding_invalid"},
		{"answer names another hash", http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", pinnedSnapshotTxn, otherSchemaHash), "batched_schema_binding_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
			fake.answerSchema(tc.status, tc.body)

			_, err := reader.ReadDatasetRows(callerCtx(), pinnedHashRequest(datasetID))

			require.Error(t, err)
			assert.Contains(t, err.Error(), "dataset_input_schema_rejected")
			assert.Contains(t, err.Error(), tc.want)
			assert.Empty(t, scanRequests(fake), "an unusable answer is refused before the first scan page")
			assert.Equal(t, []string{"schema"}, requestKinds(fake.requests))
		})
	}
}

func TestDatasetSnapshotReadSchemaHashAcceptedScansAfterTheProof(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`)})
	fake.pageRows = 1
	datasetID := uuid.NewString()
	fake.answerSchema(http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", pinnedSnapshotTxn, schemaHash))

	result, err := reader.ReadDatasetRows(callerCtx(), pinnedHashRequest(datasetID))

	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	assert.Equal(t, []string{"schema", "scan", "scan"}, requestKinds(fake.requests), "the proof is one request ahead of the first page")
	for _, request := range fake.requests {
		assert.Equal(t, "Bearer caller-jwt", request.Auth)
	}
}

func TestDatasetSnapshotReadSchemaHashRequiresCallerBearer(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusOK, versionedSchemaAnswer(uuid.NewString(), "main", "3", pinnedSnapshotTxn, schemaHash))
	reader.BearerToken = "service-token-that-must-not-be-used"

	_, err := reader.ReadDatasetRows(context.Background(), pinnedHashRequest(uuid.NewString()))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_caller_required")
	assert.Empty(t, fake.requests, "no request may leave without the caller JWT, the schema proof included")
}

func TestDatasetSnapshotReadWithoutExpectedSchemaHashKeyMakesNoSchemaRequest(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusPreconditionFailed, []byte(hashMismatchAnswer)) // would refuse if asked

	result, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, []string{"scan"}, requestKinds(fake.requests), "an absent key keeps the read as it was")
}

func TestInputNodeSnapshotNullAndEmptySchemaHashFailBeforeAnyRequest(t *testing.T) {
	for name, raw := range map[string]string{"null": `null`, "empty string": `""`} {
		t.Run(name, func(t *testing.T) {
			reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
			fake.answerSchema(http.StatusOK, versionedSchemaAnswer(uuid.NewString(), "main", "3", pinnedSnapshotTxn, schemaHash))
			runtime := newLightweightTableRuntime()
			runtime.datasets = reader
			config := snapshotConfig(uuid.NewString(), snapshotWithHash(raw))

			_, err := runtime.Run(callerCtx(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "dataset_snapshot_invalid: expected_schema_hash must be a lowercase hexadecimal SHA-256 digest")
			assert.Empty(t, fake.requests, "a declared key that is not a digest fails the node before any schema or scan request")
		})
	}
}

func TestInputNodeSchemaHashMismatchFailsTheNodeBeforeAnyScan(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusPreconditionFailed, []byte(hashMismatchAnswer))
	runtime := newLightweightTableRuntime()
	runtime.datasets = reader
	config := snapshotConfig(uuid.NewString(), snapshotWithHash(`"`+schemaHash+`"`))

	_, err := runtime.Run(callerCtx(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `dataset_input_load_failed: node "in" failed to load dataset`)
	assert.Contains(t, err.Error(), "dataset_input_schema_rejected")
	assert.Contains(t, err.Error(), "dataset_schema_content_hash_mismatch")
	assert.Equal(t, []string{"schema"}, requestKinds(fake.requests))
}

// The production run path: the handler's own caller bearer reaches the
// schema proof, and a refused input ends the run before any scan or commit.
func TestPipelineRunRejectsSchemaHashMismatchBeforeScanAndPublish(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusPreconditionFailed, []byte(hashMismatchAnswer))
	pipelineID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	datasetID := uuid.NewString()
	outputID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	nodes := []models.PipelineNode{
		{ID: "input", Label: "Pinned dataset", TransformType: "dataset_input", Config: json.RawMessage(snapshotConfig(datasetID, snapshotWithHash(`"`+schemaHash+`"`)))},
		{ID: "output", Label: "Out", TransformType: "output_dataset", DependsOn: []string{"input"}, Config: json.RawMessage(`{}`), OutputDatasetID: &outputID},
	}
	runRepo := newRecordingPipelineRunsWithNodes(pipelineID, nodes)
	runRepo.pipeline.PipelineType = models.PipelineTypeFaster
	committer := &recordingCommitter{}
	restore := SetExecutionPorts(ExecutionPorts{Runs: runRepo, Committer: committer, Transactions: &recordingTransactions{}, DatasetRows: reader, Parallelism: 1})
	defer restore()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/22222222-2222-2222-2222-222222222222/runs", bytes.NewReader([]byte(`{"skip_unchanged":false}`)))
	request.Header.Set("Authorization", "Bearer caller-jwt")
	recorder := httptest.NewRecorder()

	TriggerPipelineRun(recorder, request)

	require.Equal(t, http.StatusCreated, recorder.Result().StatusCode, recorder.Body.String())
	require.Equal(t, 1, runRepo.opened)
	require.Equal(t, "failed", runRepo.finishedStatus)
	var results []models.PipelineNodeResult
	require.NoError(t, json.Unmarshal(runRepo.nodeResults, &results))
	require.Len(t, results, 2)
	input, output := results[0], results[1]
	require.Equal(t, "input", input.NodeID)
	assert.Equal(t, "failed", input.Status)
	require.NotNil(t, input.Error)
	assert.Contains(t, *input.Error, "dataset_input_schema_rejected")
	assert.Contains(t, *input.Error, "dataset_schema_content_hash_mismatch")
	require.Equal(t, "output", output.NodeID)
	assert.NotEqual(t, "succeeded", output.Status, "the output node never runs on a refused input")
	// The executor opens the output transaction when the run starts and
	// aborts it when an upstream node fails: the invariant is no commit.
	for _, resource := range output.OutputResources {
		assert.Equal(t, "aborted", resource.Status, "a refused input leaves no committed output")
	}
	assert.Empty(t, committer.datasets, "nothing is committed from a refused input")
	assert.Equal(t, []string{"schema"}, requestKinds(fake.requests))
	assert.Equal(t, "Bearer caller-jwt", fake.requests[0].Auth, "the handler's caller is the identity of the proof")
}

// VerifyInputSchema stays as it was for the batched builds, which pass no
// expected transaction; a dataset_snapshot pin adds the cross-check.
func TestVerifyInputSchemaTransactionCrossCheck(t *testing.T) {
	datasetID := uuid.NewString()
	client := func(t *testing.T) (batchedDatasetClient, *snapshotTestServer) {
		t.Helper()
		fake := newSnapshotTestServer(t, nil)
		return batchedDatasetClient{cfg: BatchedClientConfig{BaseURL: fake.server.URL}}, fake
	}
	pinned := uuid.MustParse(pinnedSnapshotTxn)

	t.Run("without an expected transaction no transaction_id is sent", func(t *testing.T) {
		batch, fake := client(t)
		other := uuid.MustParse(otherSnapshotTxn)
		fake.answerSchema(http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", otherSnapshotTxn, schemaHash))

		txn, err := batch.VerifyInputSchema(callerCtx(), batchedScanInput{DatasetRID: datasetID, Branch: "main", Version: 3}, schemaHash)

		require.NoError(t, err)
		assert.Equal(t, other, txn, "the batched read adopts the transaction the answer names")
		query, err := url.ParseQuery(fake.requests[0].Query)
		require.NoError(t, err)
		assert.NotContains(t, query, "transaction_id")
	})

	t.Run("a pin sends its transaction and the answer must name it", func(t *testing.T) {
		batch, fake := client(t)
		fake.answerSchema(http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", otherSnapshotTxn, schemaHash))

		_, err := batch.VerifyInputSchema(callerCtx(), batchedScanInput{DatasetRID: datasetID, Branch: "main", Version: 3, ExpectedTransactionID: &pinned}, schemaHash)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "batched_schema_binding_invalid")
		query, err := url.ParseQuery(fake.requests[0].Query)
		require.NoError(t, err)
		assert.Equal(t, pinnedSnapshotTxn, query.Get("transaction_id"))
	})

	t.Run("a pin is answered with its own transaction", func(t *testing.T) {
		batch, fake := client(t)
		fake.answerSchema(http.StatusOK, versionedSchemaAnswer(datasetID, "main", "3", pinnedSnapshotTxn, schemaHash))

		txn, err := batch.VerifyInputSchema(callerCtx(), batchedScanInput{DatasetRID: datasetID, Branch: "main", Version: 3, ExpectedTransactionID: &pinned}, schemaHash)

		require.NoError(t, err)
		assert.Equal(t, pinned, txn)
	})
}

// stagedGroupSpec builds the one JobSpec of a staged output group pinned to
// the given snapshot. Its three outputs are the two worksheets and the
// error dataset of the workbook group.
func stagedGroupSpec(t *testing.T, snapshot string, workbookGroup bool) []models.JobSpec {
	t.Helper()
	targets := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	payload := map[string]any{
		"transform_type": "dataset_input", "staged_output_group": true,
		"dataset_path": "book.xlsx", "dataset_snapshot": json.RawMessage(snapshot),
	}
	if workbookGroup {
		payload["excel_outputs"] = map[string]any{
			"worksheets": []map[string]any{
				{"sheet": "Orders", "dataset_rid": targets[0], "mapping": []map[string]any{{"source_index": 0, "target": "id"}}},
				{"sheet": "Lines", "dataset_rid": targets[1], "mapping": []map[string]any{{"source_index": 0, "target": "id"}}},
			},
			"error_dataset_rid": targets[2],
		}
	}
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	return []models.JobSpec{{RID: "spec.root", LogicPayload: encoded, ContentHash: "content-hash",
		Inputs:            []models.InputSpec{{DatasetRID: uuid.NewString()}},
		OutputDatasetRIDs: targets}}
}

// The workbook group rebuilds its pin from the manifest, which carries no
// schema hash, so a declared hash is refused there. The group whose pinned
// root read proves the hash keeps accepting the same pin.
func TestStagedGroupRefusesExpectedSchemaHashForWorkbookOutputs(t *testing.T) {
	pinned := snapshotWithHash(`"` + schemaHash + `"`)

	config, err := stagedManualGroupConfig(stagedGroupSpec(t, pinned, true))

	require.Nil(t, config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "excel_outputs does not support expected_schema_hash")

	config, err = stagedManualGroupConfig(stagedGroupSpec(t, validSnapshot, true))
	require.NoError(t, err, "the workbook group without a hash is configured as before")
	require.NotNil(t, config.Excel)

	config, err = stagedManualGroupConfig(stagedGroupSpec(t, pinned, false))
	require.NoError(t, err, "the group whose pinned root read proves the hash keeps the pin")
	assert.Nil(t, config.Excel)
}
