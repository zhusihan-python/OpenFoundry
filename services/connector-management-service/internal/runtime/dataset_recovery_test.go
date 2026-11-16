package runtime

// P2.6 recoverable publish client tests. The legacy surface (CommitOutput,
// GetDataset, usable-ACK contract) is covered by sync_ack_test.go and
// dataset_client_test.go; these tests exercise ONLY the new keyed protocol:
// single serialization + verbatim replay, caller-mutation isolation, strict
// receipt matching, typed error classification and secret-safe errors.
// Fixtures are canned server responses — no dataset-versioning behaviour is
// reimplemented here.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The concrete client keeps satisfying both ports side by side: the legacy
// P25 surface is untouched by the P2.6 additions.
var (
	_ DatasetVersioningPort = HTTPDatasetVersioningClient{}
	_ DatasetRecoveryPort   = HTTPDatasetVersioningClient{}
)

// recoveryFixture assembles a valid keyed-publish request for the given
// content (rows = newline count, so zero-byte content means a zero-row
// publish) with the P25 handler's metadata shape.
func recoveryFixture(datasetID, runID, defID, sourceID uuid.UUID, content []byte, rows int64) DatasetCommitRequest {
	sum := sha256.Sum256(content)
	contentHash := hex.EncodeToString(sum[:])
	return DatasetCommitRequest{
		DatasetID:       datasetID,
		Branch:          "release",
		Format:          "TEXT",
		LogicalPath:     "sync-" + runID.String() + ".ndjson",
		ContentType:     "application/x-ndjson",
		Content:         content,
		RowCount:        rows,
		ContentHash:     contentHash,
		Summary:         "Connector snapshot sync " + defID.String(),
		CreateIfMissing: false,
		TransactionType: "SNAPSHOT",
		Metadata: map[string]any{
			"content_hash": contentHash,
			"rows":         rows,
			"bytes":        int64(len(content)),
			"pages":        2,
			"lineage": map[string]any{
				"source_id":   sourceID,
				"sync_def_id": defID,
				"sync_run_id": runID,
			},
		},
	}
}

// keyedReceiptJSON renders the frozen keyed-commit response shape with
// caller-chosen values; mismatch tests simply pass a wrong value in.
func keyedReceiptJSON(datasetID uuid.UUID, branch, key, fingerprint string, txnID uuid.UUID, contentHash string, sourceID, defID, runID uuid.UUID) string {
	return fmt.Sprintf(`{
	  "dataset_id": %q, "dataset_rid": "ri.foundry.main.dataset.%s", "branch": %q,
	  "transaction": {
	    "id": %q, "status": "COMMITTED",
	    "metadata": {"content_hash": %q, "pipeline_output": true,
	                 "lineage": {"source_id": %q, "sync_def_id": %q, "sync_run_id": %q}}
	  },
	  "files": [{"logical_path": "sync.ndjson", "size_bytes": 2}],
	  "preview": {"columns": [], "rows": [], "format": "json", "limit": 10, "offset": 0},
	  "idempotency_key": %q,
	  "request_fingerprint": %q
	}`, datasetID, datasetID, branch, txnID, contentHash, sourceID, defID, runID, key, fingerprint)
}

// recoveryRecorder captures every request a test server saw.
type recoveryRecorder struct {
	mu             sync.Mutex
	bodies         [][]byte
	authorizations []string
	paths          []string
	contentTypes   []string
	methods        []string
}

func (rec *recoveryRecorder) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.bodies = append(rec.bodies, body)
	rec.authorizations = append(rec.authorizations, r.Header.Get("Authorization"))
	rec.paths = append(rec.paths, r.URL.Path)
	rec.contentTypes = append(rec.contentTypes, r.Header.Get("Content-Type"))
	rec.methods = append(rec.methods, r.Method)
}

func (rec *recoveryRecorder) count() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return len(rec.bodies)
}

// recoveryServer boots a scripted DVS stand-in that records requests.
func recoveryServer(t *testing.T, respond func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, *recoveryRecorder) {
	t.Helper()
	rec := &recoveryRecorder{}
	return recoveryServerWith(t, rec, respond), rec
}

// recoveryServerWith wires a caller-owned recorder into the stand-in so
// handlers can branch on how many requests have arrived (the record happens
// BEFORE respond, so rec.count() inside the handler includes the current
// request).
func recoveryServerWith(t *testing.T, rec *recoveryRecorder, respond func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		respond(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}

// preparedFixture prepares a valid commit for standard fixture ids.
func preparedFixture(t *testing.T, content []byte, rows int64) (*PreparedCommit, uuid.UUID, uuid.UUID) {
	t.Helper()
	datasetID, runID := uuid.New(), uuid.New()
	defID, sourceID := uuid.New(), uuid.New()
	prepared, err := PrepareRecoverableCommit(
		recoveryFixture(datasetID, runID, defID, sourceID, content, rows),
		"of-p26-pub-v1:"+runID.String())
	require.NoError(t, err)
	return prepared, defID, sourceID
}

func TestPrepareRecoverableCommitSerializesOnceShapeIsFrozen(t *testing.T) {
	datasetID, runID := uuid.New(), uuid.New()
	defID, sourceID := uuid.New(), uuid.New()
	content := []byte("{\"a\":1}\n{\"a\":2}\n")
	req := recoveryFixture(datasetID, runID, defID, sourceID, content, 2)

	prepared, err := PrepareRecoverableCommit(req, "of-p26-pub-v1:"+runID.String())
	require.NoError(t, err)

	assert.Equal(t, datasetID, prepared.DatasetID)
	assert.Equal(t, "of-p26-pub-v1:"+runID.String(), prepared.Key)
	assert.Equal(t, "release", prepared.Branch)
	assert.Equal(t, req.LogicalPath, prepared.LogicalPath)
	assert.Equal(t, int64(2), prepared.Rows)
	assert.Equal(t, int64(len(content)), prepared.Bytes)
	assert.Equal(t, req.ContentHash, prepared.ContentHash)
	assert.Equal(t, DatasetLineageIdentity{SourceID: sourceID, SyncDefID: defID, SyncRunID: runID}, prepared.Lineage)
	// The fingerprint is over the exact produced bytes.
	digest := sha256.Sum256(prepared.Body)
	assert.Equal(t, hex.EncodeToString(digest[:]), prepared.Fingerprint)
	assert.Equal(t, RequestFingerprintOfBody(prepared.Body), prepared.Fingerprint)

	var body map[string]any
	require.NoError(t, json.Unmarshal(prepared.Body, &body))
	// Keyed protocol invariants the server relies on.
	assert.Equal(t, false, body["create_if_missing"])
	assert.Equal(t, "release", body["branch"])
	assert.Equal(t, "of-p26-pub-v1:"+runID.String(), body["idempotency_key"])
	files, ok := body["files"].([]any)
	require.True(t, ok, "files must be an array")
	require.Len(t, files, 1)
	file := files[0].(map[string]any)
	assert.Equal(t, req.LogicalPath, file["logical_path"])
	assert.Equal(t, float64(len(content)), file["size_bytes"])
	assert.Equal(t, "ADD", file["operation"])
	assert.Equal(t, "application/x-ndjson", file["content_type"])
	// []byte marshals as standard base64 of the exact content.
	assert.Equal(t, base64.StdEncoding.EncodeToString(content), file["content_base64"])
	// No checkpoint candidate and no auth material ever enter the body.
	raw := string(prepared.Body)
	assert.NotContains(t, raw, "checkpoint")
	assert.NotContains(t, raw, "authorization")
	assert.NotContains(t, raw, "Authorization")
	assert.NotContains(t, raw, "bearer")
	assert.NotContains(t, raw, "Bearer")
	assert.NotContains(t, raw, "token")
	// Metadata rides verbatim: content hash + lineage survive.
	metadata := body["metadata"].(map[string]any)
	assert.Equal(t, req.ContentHash, metadata["content_hash"])
}

func TestPrepareRecoverableCommitZeroRowBodyCarriesEmptyContent(t *testing.T) {
	prepared, _, _ := preparedFixture(t, []byte{}, 0)
	raw := string(prepared.Body)
	assert.Contains(t, raw, `"content_base64":""`)
	assert.Contains(t, raw, `"size_bytes":0`)
	assert.Equal(t, sha256HexForTest([]byte{}), prepared.ContentHash)
}

func sha256HexForTest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestPrepareRecoverableCommitRejectsInvalidInput(t *testing.T) {
	datasetID, runID := uuid.New(), uuid.New()
	defID, sourceID := uuid.New(), uuid.New()
	content := []byte("{\"a\":1}\n")
	newReq := func() DatasetCommitRequest {
		return recoveryFixture(datasetID, runID, defID, sourceID, content, 1)
	}
	cases := []struct {
		name    string
		mutate  func(req *DatasetCommitRequest, key *string)
		wantMsg string
	}{
		{"blank branch", func(req *DatasetCommitRequest, _ *string) { req.Branch = "  " }, "explicit branch"},
		{"create_if_missing", func(req *DatasetCommitRequest, _ *string) { req.CreateIfMissing = true }, "create_if_missing"},
		{"nil dataset", func(req *DatasetCommitRequest, _ *string) { req.DatasetID = uuid.Nil }, "target dataset"},
		{"blank logical path", func(req *DatasetCommitRequest, _ *string) { req.LogicalPath = "" }, "logical path"},
		{"negative rows", func(req *DatasetCommitRequest, _ *string) { req.RowCount = -1; req.Metadata["rows"] = int64(-1) }, "non-negative"},
		{"short content hash", func(req *DatasetCommitRequest, _ *string) {
			req.ContentHash = "abc"
			req.Metadata["content_hash"] = "abc"
		}, "64-lowercase-hex"},
		{"hash content mismatch", func(req *DatasetCommitRequest, _ *string) {
			other := sha256HexForTest([]byte("other"))
			req.ContentHash = other
			req.Metadata["content_hash"] = other
		}, "does not match the content"},
		{"metadata missing content_hash", func(req *DatasetCommitRequest, _ *string) { delete(req.Metadata, "content_hash") }, "missing content_hash"},
		{"metadata hash mismatch", func(req *DatasetCommitRequest, _ *string) {
			req.Metadata["content_hash"] = sha256HexForTest([]byte("x"))
		}, "content_hash"},
		{"metadata missing rows", func(req *DatasetCommitRequest, _ *string) { delete(req.Metadata, "rows") }, "missing rows"},
		{"metadata rows mismatch", func(req *DatasetCommitRequest, _ *string) { req.Metadata["rows"] = int64(7) }, "rows"},
		{"metadata missing bytes", func(req *DatasetCommitRequest, _ *string) { delete(req.Metadata, "bytes") }, "missing bytes"},
		{"metadata bytes mismatch", func(req *DatasetCommitRequest, _ *string) { req.Metadata["bytes"] = int64(len(content) + 1) }, "bytes must equal"},
		{"metadata missing lineage", func(req *DatasetCommitRequest, _ *string) { delete(req.Metadata, "lineage") }, "missing lineage"},
		{"lineage missing sync_run_id", func(req *DatasetCommitRequest, _ *string) {
			delete(req.Metadata["lineage"].(map[string]any), "sync_run_id")
		}, "sync_run_id"},
		{"lineage non-uuid source", func(req *DatasetCommitRequest, _ *string) {
			req.Metadata["lineage"].(map[string]any)["source_id"] = "not-a-uuid"
		}, "not a uuid"},
		{"nil lineage uuid", func(req *DatasetCommitRequest, _ *string) {
			req.Metadata["lineage"].(map[string]any)["sync_def_id"] = uuid.Nil
		}, "non-nil"},
		{"blank key", func(_ *DatasetCommitRequest, key *string) { *key = " " }, "idempotency key"},
		{"key with slash", func(_ *DatasetCommitRequest, key *string) { *key = "of-p26-pub-v1/" + runID.String() }, "invalid characters"},
		{"key with query separator", func(_ *DatasetCommitRequest, key *string) { *key = "k?x=1" }, "invalid characters"},
		{"overlong key", func(_ *DatasetCommitRequest, key *string) { *key = "of-p26-pub-v1:" + strings.Repeat("a", 128) }, "exceeds"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			req := newReq()
			key := "of-p26-pub-v1:" + runID.String()
			testCase.mutate(&req, &key)
			prepared, err := PrepareRecoverableCommit(req, key)
			require.Error(t, err)
			assert.Nil(t, prepared)
			assert.Contains(t, err.Error(), testCase.wantMsg)
			assert.NotContains(t, err.Error(), "Bearer", "errors never carry auth material")
		})
	}
}

func TestPrepareRecoverableCommitIsolatedFromLaterCallerMutations(t *testing.T) {
	content := []byte("{\"a\":1}\n{\"b\":2}\n")
	datasetID, runID := uuid.New(), uuid.New()
	defID, sourceID := uuid.New(), uuid.New()
	req := recoveryFixture(datasetID, runID, defID, sourceID, content, 2)

	prepared, err := PrepareRecoverableCommit(req, "of-p26-pub-v1:"+runID.String())
	require.NoError(t, err)
	bodyBefore := append([]byte(nil), prepared.Body...)

	// Caller-side mutations after the single serialization must not leak in.
	content[0] = 'X'
	req.Metadata["rows"] = int64(999)
	req.Metadata["content_hash"] = sha256HexForTest([]byte("tampered"))
	req.Metadata["lineage"].(map[string]any)["sync_run_id"] = uuid.New()
	req.Branch = "hijacked"
	req.LogicalPath = "other.ndjson"

	assert.Equal(t, bodyBefore, prepared.Body, "persisted bytes are a frozen copy")
	assert.Equal(t, RequestFingerprintOfBody(bodyBefore), prepared.Fingerprint)
}

func TestCommitPreparedOutputReplaysExactBytes(t *testing.T) {
	content := []byte("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n")
	prepared, defID, sourceID := preparedFixture(t, content, 3)
	rec := &recoveryRecorder{}
	savedTxnID := uuid.New() // the DVS saves the first response; replays return it verbatim
	server := recoveryServerWith(t, rec, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if rec.count() == 1 { // the very first dispatch is the 201 first commit
			w.WriteHeader(http.StatusCreated)
		}
		fmt.Fprint(w, keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key,
			prepared.Fingerprint, savedTxnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID))
	})
	client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: 5 * time.Second}}

	first, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer caller-jwt")
	require.NoError(t, err)
	assert.False(t, first.Replay, "201 is the first commit")
	assert.Equal(t, "COMMITTED", first.Status)
	require.NoError(t, prepared.ValidateReceipt(first))

	replay, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer caller-jwt")
	require.NoError(t, err)
	assert.True(t, replay.Replay, "200 is a same-key replay")
	require.NoError(t, prepared.ValidateReceipt(replay))
	assert.Equal(t, first.TransactionID, replay.TransactionID, "the DVS returns the saved receipt")

	// Verbatim bytes: every dispatched body equals the prepared body exactly.
	require.Equal(t, 2, rec.count())
	for i := 0; i < 2; i++ {
		assert.Equal(t, prepared.Body, rec.bodies[i], "dispatch %d must be the persisted bytes verbatim", i)
		assert.Equal(t, "application/json", rec.contentTypes[i])
		assert.Equal(t, "Bearer caller-jwt", rec.authorizations[i])
		assert.True(t, strings.HasSuffix(rec.paths[i], "/outputs:commit"), "frozen endpoint path, got %s", rec.paths[i])
		assert.Equal(t, http.MethodPost, rec.methods[i])
	}

	// Concurrent replays stay byte-identical (and race-detected).
	var wg sync.WaitGroup
	var receiptMu sync.Mutex
	receipts := []*DatasetCommitReceipt{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			receipt, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer caller-jwt")
			assert.NoError(t, err)
			if err == nil {
				receiptMu.Lock()
				receipts = append(receipts, receipt)
				receiptMu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 10, rec.count())
	for i := 0; i < rec.count(); i++ {
		assert.Equal(t, prepared.Body, rec.bodies[i], "concurrent dispatch %d drifted", i)
	}
	for _, receipt := range receipts {
		require.NoError(t, prepared.ValidateReceipt(receipt))
	}
}

func TestCommitPreparedOutputZeroRowPublish(t *testing.T) {
	prepared, defID, sourceID := preparedFixture(t, []byte{}, 0)
	server, rec := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key,
			prepared.Fingerprint, uuid.New(), prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID))
	})
	client := HTTPDatasetVersioningClient{BaseURL: server.URL}
	receipt, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer jwt")
	require.NoError(t, err)
	assert.Equal(t, int64(0), prepared.Rows)
	assert.Contains(t, string(rec.bodies[0]), `"content_base64":""`)
	require.NoError(t, prepared.ValidateReceipt(receipt))
}

func TestCommitPreparedOutputStrictReceiptMismatchRejections(t *testing.T) {
	prepared, defID, sourceID := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	txnID := uuid.New()
	cases := []struct {
		name string
		// receipt override; nil means build from the valid fixture then tweak
		body func(valid string) string
	}{
		{"wrong dataset", func(valid string) string {
			return keyedReceiptJSON(uuid.New(), prepared.Branch, prepared.Key, prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)
		}},
		{"wrong branch", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, "main", prepared.Key, prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)
		}},
		{"wrong key", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, prepared.Branch, "of-p26-pub-v1:"+uuid.New().String(), prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)
		}},
		{"wrong fingerprint", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key, sha256HexForTest([]byte("other body")), txnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)
		}},
		{"wrong content hash", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key, prepared.Fingerprint, txnID, sha256HexForTest([]byte("x")), sourceID, defID, prepared.Lineage.SyncRunID)
		}},
		{"wrong lineage source", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key, prepared.Fingerprint, txnID, prepared.ContentHash, uuid.New(), defID, prepared.Lineage.SyncRunID)
		}},
		{"wrong lineage run", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key, prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, defID, uuid.New())
		}},
		{"wrong lineage def", func(valid string) string {
			return keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key, prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, uuid.New(), prepared.Lineage.SyncRunID)
		}},
		{"status not committed", func(valid string) string {
			return strings.Replace(valid, `"status": "COMMITTED"`, `"status": "OPEN"`, 1)
		}},
		{"nil transaction", func(valid string) string {
			return strings.Replace(valid, txnID.String(), uuid.Nil.String(), 1)
		}},
		{"metadata without content hash", func(valid string) string {
			return strings.Replace(valid, `"metadata": {"content_hash"`, `"metadata": {"other_hash"`, 1)
		}},
		{"metadata without lineage", func(valid string) string {
			return strings.Replace(valid, `"lineage": {"source_id"`, `"lineage_x": {"source_id"`, 1)
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			valid := keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key,
				prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)
			server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				fmt.Fprint(w, testCase.body(valid))
			})
			client := HTTPDatasetVersioningClient{BaseURL: server.URL}
			receipt, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer jwt")
			require.Error(t, err, "a wrong or unknown receipt is never a usable ACK")
			assert.Nil(t, receipt)
			// Either a typed mismatch or a typed invalid-receipt dependency
			// error — both keep the outcome unknown, never success.
			var mismatch *ReceiptMismatchError
			var dep *DependencyError
			require.True(t, errors.As(err, &mismatch) || errors.As(err, &dep),
				"must be typed, got %T: %v", err, err)
		})
	}
}

func quoteFor(s string) string {
	return `"` + s + `"`
}

func TestValidateReceiptNamesEveryFailingCheck(t *testing.T) {
	prepared, _, _ := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	valid := &DatasetCommitReceipt{
		DatasetID:          prepared.DatasetID,
		Branch:             prepared.Branch,
		IdempotencyKey:     prepared.Key,
		RequestFingerprint: prepared.Fingerprint,
		TransactionID:      uuid.New(),
		Status:             "COMMITTED",
		ContentHash:        prepared.ContentHash,
		Lineage:            prepared.Lineage,
	}
	require.NoError(t, prepared.ValidateReceipt(valid))

	cases := []struct {
		name   string
		mutate func(r *DatasetCommitReceipt)
		check  string
	}{
		{"dataset", func(r *DatasetCommitReceipt) { r.DatasetID = uuid.New() }, "dataset"},
		{"branch", func(r *DatasetCommitReceipt) { r.Branch = "main" }, "branch"},
		{"key", func(r *DatasetCommitReceipt) { r.IdempotencyKey = "other" }, "idempotency_key"},
		{"fingerprint", func(r *DatasetCommitReceipt) { r.RequestFingerprint = sha256HexForTest([]byte("x")) }, "request_fingerprint"},
		{"nil txn", func(r *DatasetCommitReceipt) { r.TransactionID = uuid.Nil }, "transaction_id"},
		{"status", func(r *DatasetCommitReceipt) { r.Status = "PENDING" }, "status"},
		{"content hash", func(r *DatasetCommitReceipt) { r.ContentHash = sha256HexForTest([]byte("x")) }, "content_hash"},
		{"lineage", func(r *DatasetCommitReceipt) { r.Lineage.SyncDefID = uuid.New() }, "lineage"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := *valid
			testCase.mutate(&candidate)
			err := prepared.ValidateReceipt(&candidate)
			var mismatch *ReceiptMismatchError
			require.ErrorAs(t, err, &mismatch)
			assert.Equal(t, testCase.check, mismatch.Check)
		})
	}
	// A nil receipt is a mismatch, not a panic.
	err := prepared.ValidateReceipt(nil)
	var mismatch *ReceiptMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.Equal(t, "receipt is nil", mismatch.Check)
}

func TestCommitPreparedOutputErrorClassification(t *testing.T) {
	prepared, _, _ := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	const secretMarker = "RAW-BODY-SECRET-9f8e7d"
	newCase := func(t *testing.T, status int) *httptest.Server {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			fmt.Fprint(w, `{"error":"`+secretMarker+`","token":"Bearer leaked"}`)
		})
		return server
	}
	send := func(t *testing.T, server *httptest.Server) error {
		client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: 2 * time.Second}}
		_, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer caller-jwt-xyz")
		return err
	}
	assertSecretSafe := func(t *testing.T, err error) {
		require.Error(t, err)
		assert.NotContains(t, err.Error(), secretMarker, "raw response bodies never surface in errors")
		assert.NotContains(t, err.Error(), "caller-jwt-xyz", "the forwarded token never surfaces")
	}

	t.Run("401 forwarded-jwt rejected", func(t *testing.T) {
		err := send(t, newCase(t, http.StatusUnauthorized))
		assertSecretSafe(t, err)
		assert.ErrorIs(t, err, ErrDatasetUnauthorized)
	})
	t.Run("403 caller rejection", func(t *testing.T) {
		err := send(t, newCase(t, http.StatusForbidden))
		assertSecretSafe(t, err)
		assert.ErrorIs(t, err, ErrDatasetForbidden)
	})
	t.Run("404 target dataset", func(t *testing.T) {
		err := send(t, newCase(t, http.StatusNotFound))
		assertSecretSafe(t, err)
		assert.ErrorIs(t, err, ErrDatasetNotFound)
	})
	t.Run("409 typed idempotency conflict is permanent", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"error":"same idempotency key committed with different bytes","code":"IDEMPOTENCY_KEY_CONFLICT"}`)
		})
		err := send(t, server)
		assertSecretSafe(t, err)
		assert.ErrorIs(t, err, ErrCommitIdempotencyConflict)
	})
	t.Run("plain 409 without code is dependency-busy", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"error":"transaction already open for dataset"}`)
		})
		err := send(t, server)
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 409, dep.Status, "unknown-outcome busy, never a typed permanent conflict")
		assert.NotContains(t, err.Error(), "already open", "conflict body text never surfaces")
	})
	t.Run("409 with another code is dependency-busy", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"error":"branch busy","code":"BRANCH_LOCKED"}`)
		})
		err := send(t, server)
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 409, dep.Status)
	})
	t.Run("409 unparseable body stays dependency-busy", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, "<html>gateway noise</html>")
		})
		err := send(t, server)
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 409, dep.Status)
	})
	t.Run("5xx dependency failure", func(t *testing.T) {
		err := send(t, newCase(t, http.StatusServiceUnavailable))
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 503, dep.Status)
		assert.Equal(t, "commit_prepared_output", dep.Op)
	})
	t.Run("non-frozen 2xx refused", func(t *testing.T) {
		err := send(t, newCase(t, http.StatusAccepted))
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 202, dep.Status)
	})
	t.Run("redirect refused, never followed", func(t *testing.T) {
		var requests int
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests++
		}))
		defer target.Close()
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", target.URL+"/api/v1/datasets/"+prepared.DatasetID.String()+"/outputs:commit")
			w.WriteHeader(http.StatusFound)
		})
		err := send(t, server)
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 302, dep.Status)
		assert.Zero(t, requests, "a keyed commit must not silently move targets")
	})
	t.Run("transport failure sanitized", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		dead.Close()
		err := send(t, dead)
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.NotContains(t, err.Error(), "?", "no query string survives")
	})
	t.Run("timeout is a dependency timeout", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(400 * time.Millisecond)
		})
		client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: 50 * time.Millisecond}}
		_, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer caller-jwt-xyz")
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.True(t, dep.Timeout)
	})
	t.Run("oversized receipt refused", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			w.Write(make([]byte, maxCommitReceiptBytes+2))
		})
		err := send(t, server)
		assertSecretSafe(t, err)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Contains(t, dep.Detail, "size bound")
	})
}

func TestCommitPreparedOutputRefusesDriftedBody(t *testing.T) {
	prepared, _, _ := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	server, rec := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	client := HTTPDatasetVersioningClient{BaseURL: server.URL}

	pristine := *prepared
	drifted := *prepared
	drifted.Body = append(append([]byte(nil), prepared.Body...), ' ')
	_, err := client.CommitPreparedOutput(context.Background(), drifted, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fingerprint")
	assert.Zero(t, rec.count(), "refusal happens before dispatch")

	// A reassembled prepared whose FIELDS disagree with its own exact bytes
	// (fingerprint intact) is caught against the actual serialized material,
	// never dispatched (root-decisions-v2.1: never trust the declarations).
	reassembled := *prepared
	reassembled.Branch = "main"
	_, err = client.CommitPreparedOutput(context.Background(), reassembled, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "branch")
	assert.Zero(t, rec.count(), "field-drift refusal also happens before dispatch")

	reassembled = *prepared
	reassembled.ContentHash = sha256HexForTest([]byte("declared-not-actual"))
	_, err = client.CommitPreparedOutput(context.Background(), reassembled, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "content hash")
	assert.Zero(t, rec.count())

	// The pristine copy still sends its exact bytes.
	_, err = client.CommitPreparedOutput(context.Background(), pristine, "")
	require.Error(t, err, "test server answers 500")
	var dep *DependencyError
	require.ErrorAs(t, err, &dep)
	assert.Equal(t, 500, dep.Status)
	assert.Equal(t, 1, rec.count(), "pristine bytes were dispatched once")
}

func TestGetCommitReceiptFetchesSavedReceipt(t *testing.T) {
	prepared, defID, sourceID := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	txnID := uuid.New()
	server, rec := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key,
			prepared.Fingerprint, txnID, prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID))
	})
	client := HTTPDatasetVersioningClient{BaseURL: server.URL}

	receipt, err := client.GetCommitReceipt(context.Background(), prepared.DatasetID, prepared.Key, "Bearer caller-jwt")
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateReceipt(receipt), "the executor-side strict match accepts the fetched receipt")
	assert.Equal(t, txnID, receipt.TransactionID)
	assert.False(t, receipt.Replay, "a GET cannot claim replay semantics")
	assert.Equal(t, http.MethodGet, rec.methods[0])
	assert.Equal(t, "/api/v1/datasets/"+prepared.DatasetID.String()+"/outputs/commits/"+prepared.Key, rec.paths[0])
	assert.Equal(t, "Bearer caller-jwt", rec.authorizations[0])
}

func TestGetCommitReceiptClassification(t *testing.T) {
	prepared, defID, sourceID := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	valid := keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key,
		prepared.Fingerprint, uuid.New(), prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)

	statusCase := func(t *testing.T, status int) error {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		})
		client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: time.Second}}
		_, err := client.GetCommitReceipt(context.Background(), prepared.DatasetID, prepared.Key, "")
		return err
	}
	t.Run("404 is typed not-committed-visible", func(t *testing.T) {
		err := statusCase(t, http.StatusNotFound)
		assert.ErrorIs(t, err, ErrCommitReceiptNotFound)
	})
	t.Run("401", func(t *testing.T) { assert.ErrorIs(t, statusCase(t, http.StatusUnauthorized), ErrDatasetUnauthorized) })
	t.Run("403", func(t *testing.T) { assert.ErrorIs(t, statusCase(t, http.StatusForbidden), ErrDatasetForbidden) })
	t.Run("5xx dependency", func(t *testing.T) {
		err := statusCase(t, http.StatusInternalServerError)
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.Equal(t, 500, dep.Status)
		assert.Equal(t, "get_commit_receipt", dep.Op)
	})
	t.Run("timeout", func(t *testing.T) {
		server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) { time.Sleep(300 * time.Millisecond) })
		client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: 50 * time.Millisecond}}
		_, err := client.GetCommitReceipt(context.Background(), prepared.DatasetID, prepared.Key, "")
		var dep *DependencyError
		require.ErrorAs(t, err, &dep)
		assert.True(t, dep.Timeout)
	})

	structural := []struct {
		name string
		body string
	}{
		{"wrong dataset echo", keyedReceiptJSON(uuid.New(), prepared.Branch, prepared.Key, prepared.Fingerprint, uuid.New(), prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)},
		{"wrong key echo", keyedReceiptJSON(prepared.DatasetID, prepared.Branch, "of-p26-pub-v1:"+uuid.NewString(), prepared.Fingerprint, uuid.New(), prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)},
		{"not committed", strings.Replace(valid, `"status": "COMMITTED"`, `"status": "ABORTED"`, 1)},
		{"no metadata", strings.Replace(valid, `"metadata": {`, `"metadata_x": {`, 1)},
		{"no lineage in metadata", strings.Replace(valid, `"lineage": {"source_id": `+quoteFor(sourceID.String()), `"lineage_x": {"source_id": `+quoteFor(sourceID.String()), 1)},
		{"no key field", strings.Replace(valid, `"idempotency_key": `+quoteFor(prepared.Key), `"idempotency_key_x": `+quoteFor(prepared.Key), 1)},
		{"no fingerprint field", strings.Replace(valid, `"request_fingerprint": `+quoteFor(prepared.Fingerprint), `"request_fingerprint_x": `+quoteFor(prepared.Fingerprint), 1)},
	}
	for _, testCase := range structural {
		t.Run(testCase.name, func(t *testing.T) {
			server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, testCase.body)
			})
			client := HTTPDatasetVersioningClient{BaseURL: server.URL}
			receipt, err := client.GetCommitReceipt(context.Background(), prepared.DatasetID, prepared.Key, "")
			require.Error(t, err, "an invalid receipt is never a success")
			assert.Nil(t, receipt)
			var dep *DependencyError
			require.ErrorAs(t, err, &dep)
		})
	}

	t.Run("invalid lookup arguments refused before the wire", func(t *testing.T) {
		server, rec := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {})
		client := HTTPDatasetVersioningClient{BaseURL: server.URL}
		_, err := client.GetCommitReceipt(context.Background(), uuid.Nil, prepared.Key, "")
		require.Error(t, err)
		_, err = client.GetCommitReceipt(context.Background(), prepared.DatasetID, "bad/key", "")
		require.Error(t, err)
		assert.Zero(t, rec.count())
	})
}

// A transport (or the caller) mutating the ORIGINAL Body backing array while
// the dispatch is in flight — after validateForSend has hashed the bytes —
// must not be able to change what reaches the wire: validation and sending
// operate on one private copy (de-aliased at the entry of
// CommitPreparedOutput). Deterministic: the RoundTripper performs the
// mutation itself at exactly the wire moment.
func TestCommitPreparedOutputWireBytesImmutableToCallerMutation(t *testing.T) {
	prepared, defID, sourceID := preparedFixture(t, []byte("{\"a\":1}\n"), 1)
	origWire := append([]byte(nil), prepared.Body...)
	receiptBody := keyedReceiptJSON(prepared.DatasetID, prepared.Branch, prepared.Key,
		prepared.Fingerprint, uuid.New(), prepared.ContentHash, sourceID, defID, prepared.Lineage.SyncRunID)

	var wireBytes []byte
	client := HTTPDatasetVersioningClient{
		BaseURL: "http://dvs.internal",
		Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			wireBytes = body
			// Alias attack at the wire moment: flip the first byte of the
			// caller's original array AFTER validation has hashed it.
			prepared.Body[0] ^= 0xFF
			return &http.Response{
				StatusCode: http.StatusCreated,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(receiptBody)),
				Request:    r,
			}, nil
		})},
	}
	receipt, err := client.CommitPreparedOutput(context.Background(), *prepared, "Bearer jwt")
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateReceipt(receipt))

	assert.Equal(t, origWire, wireBytes, "the wire sees the validated bytes, not the mutated array")
	assert.NotEqual(t, origWire[0], prepared.Body[0], "the mutation really happened (test validity)")
	// The fingerprint contract survives the attack.
	assert.Equal(t, RequestFingerprintOfBody(wireBytes), prepared.Fingerprint)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// The P2.6 target precheck reads the dataset's CURRENT active branch (no
// server-side active/main fallback applied) to freeze an explicit branch at
// first intent. Once an intent exists, recovery replays the PERSISTED branch
// only — ActiveBranch is never re-consulted to amend a prepared body.
func TestGetDatasetResolvesActiveBranchForPrecheck(t *testing.T) {
	target := uuid.New()
	cases := []struct {
		name string
		body string
		want string
	}{
		{"reports active branch", fmt.Sprintf(`{"id":%q,"name":"orders","active_branch":"release"}`, target), "release"},
		{"absent field decodes to empty (precheck fails closed upstream)", fmt.Sprintf(`{"id":%q,"name":"orders"}`, target), ""},
		{"blank field reports empty, never a whitespace name", fmt.Sprintf(`{"id":%q,"name":"orders","active_branch":"  "}`, target), ""},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server, _ := recoveryServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, testCase.body)
			})
			client := HTTPDatasetVersioningClient{BaseURL: server.URL, Client: &http.Client{Timeout: time.Second}}
			info, err := client.GetDataset(context.Background(), target, "Bearer jwt")
			require.NoError(t, err)
			assert.Equal(t, target, info.ID)
			assert.Equal(t, testCase.want, info.ActiveBranch)
		})
	}
}
