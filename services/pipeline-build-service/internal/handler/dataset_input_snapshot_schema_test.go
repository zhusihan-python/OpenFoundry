package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
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
//
// An optional expected_schema holds the fields that hash covers. A refused
// proof then reads the committed schema of the pinned version once more,
// without the hash, and names each difference.

const (
	schemaHash         = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherSchemaHash    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pinnedSnapshotTxn  = "11111111-1111-1111-1111-111111111111"
	otherSnapshotTxn   = "33333333-3333-3333-3333-333333333333"
	hashMismatchAnswer = `{"error":"dataset_schema_content_hash_mismatch: the committed schema does not match expected_content_hash","code":"dataset_schema_content_hash_mismatch"}`
	// hashMismatchRefusal is the client's error for hashMismatchAnswer.
	hashMismatchRefusal = "batched_dataset_412_dataset_schema_content_hash_mismatch: " +
		"dataset_schema_content_hash_mismatch: the committed schema does not match expected_content_hash"
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
			[]byte(`{"error":"dataset_schema_format_unsupported: versioned schemas require Parquet ADD files or one NDJSON or CSV output with a schema in its commit receipt","code":"dataset_schema_format_unsupported"}`),
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

// reportSchema is the three nullable columns of a monthly report. total is a
// DOUBLE while its cells are amounts and a STRING once one holds text.
func reportSchema(totalType string) []snapshotSchemaField {
	return []snapshotSchemaField{
		{Name: "order postal", FieldType: "STRING", Nullable: true},
		{Name: "quantity", FieldType: "LONG", Nullable: true},
		{Name: "total", FieldType: totalType, Nullable: true},
	}
}

// schemaJSON and schemaDigest are the plain encoding of a field list and
// the content hash the dataset service computes over it.
func schemaJSON(fields []snapshotSchemaField) string {
	encoded, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func schemaDigest(fields []snapshotSchemaField) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(schemaJSON(fields))))
}

// schemaFieldsAnswer is the versioned schema answer with the given binding
// and content hash over the given fields.
func schemaFieldsAnswer(datasetID, branch, version, txn, hash string, fields []snapshotSchemaField) []byte {
	return []byte(fmt.Sprintf(`{"dataset_id":%q,"branch":%q,"version":%s,"transaction_id":%q,"content_hash":%q,"fields":%s}`,
		datasetID, branch, version, txn, hash, schemaJSON(fields)))
}

// committedSchemaAnswer is the plain schema request's answer for the pinned
// dataset, version 3 and transaction: fields bound to their own hash.
func committedSchemaAnswer(datasetID string, fields []snapshotSchemaField) []byte {
	return schemaFieldsAnswer(datasetID, "main", "3", pinnedSnapshotTxn, schemaDigest(fields), fields)
}

// snapshotWithSchema is the pin text that expects the given fields next to
// the hash of their encoding.
func snapshotWithSchema(fields []snapshotSchemaField) string {
	return strings.TrimSuffix(validSnapshot, "}") + `,"expected_schema_hash":"` + schemaDigest(fields) + `","expected_schema":` + schemaJSON(fields) + `}`
}

// pinnedSchemaRequest reads a pin that expects the given fields.
func pinnedSchemaRequest(datasetID string, fields []snapshotSchemaField) DatasetRowsRequest {
	req := snapshotRequest(datasetID)
	snapshot, err := parseDatasetInputSnapshot(json.RawMessage(snapshotWithSchema(fields)))
	if err != nil {
		panic(err)
	}
	req.Snapshot = snapshot
	return req
}

// newSchemaDifferenceEnv is a snapshot double whose schema route answers the
// proof, the request that carries expected_content_hash, with the first
// status and bytes, and the plain request of the diagnostic read with the
// second. Scan pages are served as the snapshot double serves them.
func newSchemaDifferenceEnv(t *testing.T, proofStatus int, proof []byte, committedStatus int, committed []byte) (DatasetServiceRowReader, *snapshotTestServer) {
	t.Helper()
	fake := &snapshotTestServer{txn: pinnedSnapshotTxn, pageRows: 500}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/datasets/", func(w http.ResponseWriter, r *http.Request) {
		fake.requests = append(fake.requests, snapshotTestRequest{Path: r.URL.Path, Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization")})
		switch {
		case strings.HasSuffix(r.URL.Path, "/schema"):
			status, body := committedStatus, committed
			if _, conditional := r.URL.Query()["expected_content_hash"]; conditional {
				status, body = proofStatus, proof
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(body)
		case strings.HasSuffix(r.URL.Path, "/files/rows:scan"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fake.scanPage(r))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: fake.server.URL})
	t.Cleanup(restore)
	return DatasetServiceRowReader{BaseURL: fake.server.URL}, fake
}

// assertNoEmptyClause fails when a reason ends in a colon or lists an
// empty clause.
func assertNoEmptyClause(t *testing.T, message string) {
	t.Helper()
	assert.False(t, strings.HasSuffix(message, ":"), "the reason ends in a colon")
	assert.False(t, strings.HasSuffix(message, ": "), "the reason ends in a colon and a space")
	assert.NotContains(t, message, ": ;")
	assert.NotContains(t, message, "; ;")
}

// The business case: the pin expects an amount in total and the report
// committed text there. The refusal names that one field and nothing else,
// and no row is scanned.
func TestDatasetSnapshotReadNamesTheDifferingFieldOfARefusedSchema(t *testing.T) {
	datasetID := uuid.NewString()
	reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer),
		http.StatusOK, committedSchemaAnswer(datasetID, reportSchema("STRING")))

	_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, reportSchema("DOUBLE")))

	require.Error(t, err)
	assert.Equal(t, "dataset_input_schema_rejected: "+hashMismatchRefusal+
		`; the committed schema of version 3 differs from the expected schema: `+
		`field "total" is STRING in the committed schema and DOUBLE in the expected schema`, err.Error())
	assertNoEmptyClause(t, err.Error())
	assert.Empty(t, scanRequests(fake), "a refused schema is answered before the first scan page")
	assert.Equal(t, []string{"schema", "schema"}, requestKinds(fake.requests), "the proof, then the diagnostic read")
	// The diagnostic read is the plain route for the pin, as the caller.
	proof, diagnostic := fake.requests[0], fake.requests[1]
	assert.Equal(t, proof.Path, diagnostic.Path)
	assert.Equal(t, "Bearer caller-jwt", diagnostic.Auth)
	query, err := url.ParseQuery(diagnostic.Query)
	require.NoError(t, err)
	assert.Equal(t, url.Values{"branch": {"main"}, "version": {"3"}, "transaction_id": {pinnedSnapshotTxn}}, query)
}

func TestDatasetSnapshotReadWithExpectedSchemaScansAfterAnAcceptedProof(t *testing.T) {
	fields := reportSchema("DOUBLE")
	datasetID := uuid.NewString()
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusOK, schemaFieldsAnswer(datasetID, "main", "3", pinnedSnapshotTxn, schemaDigest(fields), fields))

	result, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, fields))

	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, []string{"schema", "scan"}, requestKinds(fake.requests), "an accepted proof reads the schema once")
}

// Without expected_schema the refusal is exactly the one the pin got before
// the key existed: the same text, and no second schema request.
func TestDatasetSnapshotReadWithoutExpectedSchemaKeepsTheRefusal(t *testing.T) {
	reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
	fake.answerSchema(http.StatusPreconditionFailed, []byte(hashMismatchAnswer))

	_, err := reader.ReadDatasetRows(callerCtx(), pinnedHashRequest(uuid.NewString()))

	require.Error(t, err)
	assert.Equal(t, "dataset_input_schema_rejected: "+hashMismatchRefusal, err.Error())
	assert.Equal(t, []string{"schema"}, requestKinds(fake.requests), "no diagnostic read without the fields")
	assert.Empty(t, scanRequests(fake))
}

// Only the schema mismatch asks for the committed schema. Any other
// refusal is returned as it was, although the pin holds fields.
func TestDatasetSnapshotReadWithExpectedSchemaKeepsOtherRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{
			"another code with the same status", http.StatusPreconditionFailed,
			`{"error":"stale","code":"dataset_stale"}`, "batched_dataset_412_dataset_stale: stale",
		},
		{
			"another transaction cut the version", http.StatusConflict,
			`{"error":"dataset_schema_transaction_mismatch: version 3 was cut by transaction ` + pinnedSnapshotTxn + `, not ` + otherSnapshotTxn + `","code":"dataset_schema_transaction_mismatch"}`,
			"batched_dataset_409_dataset_schema_transaction_mismatch: dataset_schema_transaction_mismatch: version 3 was cut by transaction " + pinnedSnapshotTxn + ", not " + otherSnapshotTxn,
		},
		{
			"dataset service failed", http.StatusInternalServerError,
			`{"error":"boom"}`, "batched_dataset_500: boom",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			datasetID := uuid.NewString()
			reader, fake := newSnapshotReaderTestEnv(t, [][]byte{[]byte(`{"n":1}`)})
			fake.answerSchema(tc.status, []byte(tc.body))

			_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, reportSchema("DOUBLE")))

			require.Error(t, err)
			assert.Equal(t, "dataset_input_schema_rejected: "+tc.want, err.Error())
			assert.Equal(t, []string{"schema"}, requestKinds(fake.requests), "no diagnostic read")
			assert.Empty(t, scanRequests(fake))
		})
	}
}

// A diagnostic read that cannot be trusted keeps the refusal and says why
// no difference is named.
func TestDatasetSnapshotReadKeepsTheRefusalWhenTheCommittedSchemaIsUnusable(t *testing.T) {
	datasetID := uuid.NewString()
	text := reportSchema("STRING")
	for _, tc := range []struct {
		name   string
		status int
		body   []byte
		want   string
	}{
		{"dataset service failed", http.StatusInternalServerError, []byte(`{"error":"boom"}`), "batched_dataset_500: boom"},
		{"answer names another dataset", http.StatusOK, committedSchemaAnswer(uuid.NewString(), text), "batched_schema_binding_invalid"},
		{"answer names another branch", http.StatusOK, schemaFieldsAnswer(datasetID, "dev", "3", pinnedSnapshotTxn, schemaDigest(text), text), "batched_schema_binding_invalid"},
		{"answer names another version", http.StatusOK, schemaFieldsAnswer(datasetID, "main", "9", pinnedSnapshotTxn, schemaDigest(text), text), "batched_schema_binding_invalid"},
		{"answer names another transaction", http.StatusOK, schemaFieldsAnswer(datasetID, "main", "3", otherSnapshotTxn, schemaDigest(text), text), "batched_schema_binding_invalid"},
		{"fields do not hash to content_hash", http.StatusOK, schemaFieldsAnswer(datasetID, "main", "3", pinnedSnapshotTxn, schemaDigest(reportSchema("DOUBLE")), text), "batched_schema_binding_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer), tc.status, tc.body)

			_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, reportSchema("DOUBLE")))

			require.Error(t, err)
			assert.Contains(t, err.Error(), "dataset_input_schema_rejected: "+hashMismatchRefusal)
			assert.Contains(t, err.Error(), "; the committed schema of version 3 could not be read for a field-level reason (")
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, err.Error(), "differs from the expected schema")
			assert.NotContains(t, err.Error(), `field "`)
			assert.Empty(t, scanRequests(fake))
			assert.Equal(t, []string{"schema", "schema"}, requestKinds(fake.requests))
		})
	}
}

func TestDatasetSnapshotReadBoundsTheFieldLevelReason(t *testing.T) {
	datasetID := uuid.NewString()
	// refused reads a pin that expects the fields against the diagnostic
	// answer and returns what the refusal adds to the original error.
	refused := func(t *testing.T, expected []snapshotSchemaField, status int, committed []byte) string {
		t.Helper()
		reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer), status, committed)

		_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, expected))

		require.Error(t, err)
		assert.Equal(t, []string{"schema", "schema"}, requestKinds(fake.requests))
		return strings.TrimPrefix(err.Error(), "dataset_input_schema_rejected: "+hashMismatchRefusal)
	}

	// Ten differing fields name eight and count the rest.
	var expected, committed []snapshotSchemaField
	clauses := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("f%02d", i)
		expected = append(expected, snapshotSchemaField{Name: name, FieldType: "DOUBLE", Nullable: true})
		committed = append(committed, snapshotSchemaField{Name: name, FieldType: "STRING", Nullable: true})
		clauses = append(clauses, fmt.Sprintf(`field %q is STRING in the committed schema and DOUBLE in the expected schema`, name))
	}
	assert.Equal(t, "; the committed schema of version 3 differs from the expected schema: "+
		strings.Join(clauses[:8], "; ")+"; and 2 more", refused(t, expected, http.StatusOK, committedSchemaAnswer(datasetID, committed)))

	// A quoted name is cut to 128 bytes on a rune boundary, then "...". A
	// 300-byte name quotes to 302 bytes: the cut keeps 127 one-byte or 63
	// two-byte runes after the opening quote.
	for _, tc := range []struct{ name, kept string }{
		{strings.Repeat("n", 300), strings.Repeat("n", 127)},
		{strings.Repeat("é", 150), strings.Repeat("é", 63)},
	} {
		expected = []snapshotSchemaField{{Name: tc.name, FieldType: "DOUBLE", Nullable: true}}
		committed = []snapshotSchemaField{{Name: tc.name, FieldType: "STRING", Nullable: true}}
		assert.Equal(t, `; the committed schema of version 3 differs from the expected schema: field "`+tc.kept+
			`... is STRING in the committed schema and DOUBLE in the expected schema`, refused(t, expected, http.StatusOK, committedSchemaAnswer(datasetID, committed)))
	}

	// The text of a failed diagnostic read is cut to 200 bytes on a rune
	// boundary. "batched_dataset_500: " leaves room for 89 two-byte runes.
	assert.Equal(t, "; the committed schema of version 3 could not be read for a field-level reason (batched_dataset_500: "+
		strings.Repeat("é", 89)+")", refused(t, reportSchema("DOUBLE"), http.StatusInternalServerError, []byte(`{"error":"`+strings.Repeat("é", 300)+`"}`)))
}

// Every kind of difference, in order: the pin's fields first, a field that
// differs in two ways as its type and then its nullability, then the fields
// only the committed schema holds.
func TestDatasetSnapshotReadNamesEveryKindOfDifferenceInOrder(t *testing.T) {
	// The pin order is a, both, gone, z. The committed order is z, both, a,
	// extra 1, extra 2, so that the clauses follow the pin is visible.
	expected := []snapshotSchemaField{
		{Name: "a", FieldType: "STRING"},
		{Name: "both", FieldType: "DOUBLE", Nullable: true},
		{Name: "gone", FieldType: "LONG", Nullable: true},
		{Name: "z", FieldType: "STRING", Nullable: true},
	}
	committed := []snapshotSchemaField{
		{Name: "z", FieldType: "STRING"},
		{Name: "both", FieldType: "STRING"},
		{Name: "a", FieldType: "STRING", Nullable: true},
		{Name: "extra 1", FieldType: "STRING"},
		{Name: "extra 2", FieldType: "STRING"},
	}
	datasetID := uuid.NewString()
	reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer),
		http.StatusOK, committedSchemaAnswer(datasetID, committed))

	_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, expected))

	require.Error(t, err)
	assert.Equal(t, "; the committed schema of version 3 differs from the expected schema: "+
		`field "a" is nullable in the committed schema and not nullable in the expected schema; `+
		`field "both" is STRING in the committed schema and DOUBLE in the expected schema; `+
		`field "both" is not nullable in the committed schema and nullable in the expected schema; `+
		`field "gone" is in the expected schema and not in the committed schema; `+
		`field "z" is not nullable in the committed schema and nullable in the expected schema; `+
		`field "extra 1" is in the committed schema and not in the expected schema; `+
		`field "extra 2" is in the committed schema and not in the expected schema`,
		strings.TrimPrefix(err.Error(), "dataset_input_schema_rejected: "+hashMismatchRefusal))
	assert.Empty(t, scanRequests(fake))
}

// The same fields in another order hash differently, so the proof is
// refused although no field differs. The reason says that and never ends
// in an empty clause.
func TestDatasetSnapshotReadNamesAnotherOrderOfTheSameFields(t *testing.T) {
	expected := []snapshotSchemaField{
		{Name: "order postal", FieldType: "STRING", Nullable: true},
		{Name: "quantity", FieldType: "LONG", Nullable: true},
		{Name: "total", FieldType: "DOUBLE", Nullable: true},
	}
	committed := []snapshotSchemaField{expected[2], expected[0], expected[1]}
	datasetID := uuid.NewString()
	reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer),
		http.StatusOK, committedSchemaAnswer(datasetID, committed))

	_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, expected))

	require.Error(t, err)
	assert.Equal(t, "dataset_input_schema_rejected: "+hashMismatchRefusal+
		"; the committed schema of version 3 differs from the expected schema: the same fields in another order", err.Error())
	assertNoEmptyClause(t, err.Error())
	assert.Empty(t, scanRequests(fake))
}

// A second read that answers the expected schema contradicts the refusal.
// The refusal is kept as it was, with a note in place of a difference.
func TestDatasetSnapshotReadKeepsTheRefusalWhenTheSchemaReadBackHasTheExpectedHash(t *testing.T) {
	fields := reportSchema("DOUBLE")
	datasetID := uuid.NewString()
	reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer),
		http.StatusOK, committedSchemaAnswer(datasetID, fields))

	_, err := reader.ReadDatasetRows(callerCtx(), pinnedSchemaRequest(datasetID, fields))

	require.Error(t, err)
	assert.Equal(t, "dataset_input_schema_rejected: "+hashMismatchRefusal+
		"; the schema read back for version 3 has the expected hash, so no field difference can be named", err.Error())
	assertNoEmptyClause(t, err.Error())
	assert.NotContains(t, err.Error(), "differs from the expected schema")
	assert.Empty(t, scanRequests(fake))
	assert.Equal(t, []string{"schema", "schema"}, requestKinds(fake.requests))
}

func TestParseDatasetInputSnapshotExpectedSchema(t *testing.T) {
	const (
		total       = `[{"name":"total","field_type":"DOUBLE","nullable":true}]`
		array       = "dataset_snapshot_invalid: expected_schema must be a JSON array of objects with the keys name, field_type and nullable"
		nullable    = "dataset_snapshot_invalid: expected_schema field 1 must state nullable as true or false"
		decimal     = `[{"name":"total","field_type":"DECIMAL","nullable":true}]`
		unnamed     = `[{"name":"","field_type":"DOUBLE","nullable":true}]`
		repeated    = `[{"name":"total","field_type":"DOUBLE","nullable":true},{"name":"total","field_type":"STRING","nullable":false}]`
		notNullable = `[{"name":"total","field_type":"DOUBLE","nullable":false}]`
	)
	digestOf := func(text string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(text))) }
	pin := func(hash, schema string) string {
		return strings.TrimSuffix(validSnapshot, "}") + `,"expected_schema_hash":"` + hash + `","expected_schema":` + schema + `}`
	}
	// pinRawHash states the hash as written, so null and "abc" are JSON
	// values and not strings.
	pinRawHash := func(rawHash, schema string) string {
		return strings.TrimSuffix(validSnapshot, "}") + `,"expected_schema_hash":` + rawHash + `,"expected_schema":` + schema + `}`
	}

	// The hash is over the list as the dataset service encodes it.
	for _, list := range []string{total, notNullable} {
		snapshot, err := parseDatasetInputSnapshot(json.RawMessage(pin(digestOf(list), list)))
		require.NoError(t, err)
		assert.Equal(t, digestOf(list), snapshot.ExpectedSchemaHash)
		assert.Equal(t, []snapshotSchemaField{{Name: "total", FieldType: "DOUBLE", Nullable: list == total}}, snapshot.ExpectedSchema)
	}

	// Each nullable case hashes to the list with nullable false, which is
	// what reading a missing or null nullable as false would produce.
	for name, tc := range map[string]struct {
		raw  string
		want string
	}{
		"without the hash":          {strings.TrimSuffix(validSnapshot, "}") + `,"expected_schema":` + total + `}`, "dataset_snapshot_invalid: expected_schema requires expected_schema_hash"},
		"with a null hash":          {pinRawHash(`null`, total), "dataset_snapshot_invalid: expected_schema_hash must be a lowercase hexadecimal SHA-256 digest"},
		"with an invalid hash":      {pinRawHash(`"abc"`, total), "dataset_snapshot_invalid: expected_schema_hash must be a lowercase hexadecimal SHA-256 digest"},
		"null":                      {pin(digestOf(total), `null`), "dataset_snapshot_invalid: expected_schema must be a non-empty JSON array"},
		"empty array":               {pin(digestOf(`[]`), `[]`), "dataset_snapshot_invalid: expected_schema must be a non-empty JSON array"},
		"not an array":              {pin(digestOf(total), `{"name":"total"}`), array},
		"unknown key":               {pin(digestOf(total), `[{"name":"total","field_type":"DOUBLE","nullable":true,"unit":"x"}]`), `json: unknown field "unit"`},
		"unknown type":              {pin(digestOf(decimal), decimal), `dataset_snapshot_invalid: expected_schema field "total" has an unsupported field_type "DECIMAL"`},
		"empty name":                {pin(digestOf(unnamed), unnamed), "dataset_snapshot_invalid: expected_schema field 1 must have a non-empty name"},
		"duplicate names":           {pin(digestOf(repeated), repeated), `dataset_snapshot_invalid: expected_schema names must be unique: "total"`},
		"does not hash to the hash": {pin(schemaHash, total), "dataset_snapshot_invalid: expected_schema does not hash to expected_schema_hash"},
		"nullable missing":          {pin(digestOf(notNullable), `[{"name":"total","field_type":"DOUBLE"}]`), nullable},
		"nullable null":             {pin(digestOf(notNullable), `[{"name":"total","field_type":"DOUBLE","nullable":null}]`), nullable},
		"nullable string":           {pin(digestOf(notNullable), `[{"name":"total","field_type":"DOUBLE","nullable":"true"}]`), nullable},
		"nullable number":           {pin(digestOf(notNullable), `[{"name":"total","field_type":"DOUBLE","nullable":0}]`), nullable},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, err := parseDatasetInputSnapshot(json.RawMessage(tc.raw))

			require.Error(t, err)
			assert.Nil(t, snapshot)
			assert.Contains(t, err.Error(), "dataset_snapshot_invalid: ")
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// The input node carries the reason: its own error is what the run returns
// for a refused input.
func TestInputNodeSchemaDifferenceFailsTheNodeWithTheFieldReason(t *testing.T) {
	expected := reportSchema("DOUBLE")
	datasetID := uuid.NewString()
	reader, fake := newSchemaDifferenceEnv(t, http.StatusPreconditionFailed, []byte(hashMismatchAnswer),
		http.StatusOK, committedSchemaAnswer(datasetID, reportSchema("STRING")))
	runtime := newLightweightTableRuntime()
	runtime.datasets = reader
	config := snapshotConfig(datasetID, snapshotWithSchema(expected))

	_, err := runtime.Run(callerCtx(), inputNodeContext("in", config, nil), json.RawMessage(config), "dataset_input")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `dataset_input_load_failed: node "in" failed to load dataset`)
	assert.Contains(t, err.Error(), "dataset_input_schema_rejected")
	assert.Contains(t, err.Error(), `field "total" is STRING in the committed schema and DOUBLE in the expected schema`)
	assert.Equal(t, []string{"schema", "schema"}, requestKinds(fake.requests))
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
