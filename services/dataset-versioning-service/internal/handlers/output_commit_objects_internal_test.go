package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// flakyBackingFS delegates to a real LocalBackingFS but fails the write for
// every key after the first N succeed, simulating a mid-commit backing store
// failure.
type flakyBackingFS struct {
	inner     *storageabstraction.LocalBackingFS
	failAfter int
	written   int
}

func (f *flakyBackingFS) FSID() string          { return f.inner.FSID() }
func (f *flakyBackingFS) BaseDirectory() string { return f.inner.BaseDirectory() }
func (f *flakyBackingFS) PresignedURL(location storageabstraction.PhysicalLocation, ttl time.Duration) (storageabstraction.PresignedURL, error) {
	return f.inner.PresignedURL(location, ttl)
}
func (f *flakyBackingFS) ReadLocalObject(key string) ([]byte, error) {
	return f.inner.ReadLocalObject(key)
}
func (f *flakyBackingFS) VerifyLocalSignature(key string, expires time.Time, sig string) bool {
	return f.inner.VerifyLocalSignature(key, expires, sig)
}
func (f *flakyBackingFS) WriteLocalObject(key string, data []byte) error {
	f.written++
	if f.written > f.failAfter {
		return errors.New("simulated backing store failure")
	}
	return f.inner.WriteLocalObject(key, data)
}

func TestWriteOutputCommitObjectsPartialFailureLeavesNoVisibleVersion(t *testing.T) {
	root := t.TempDir()
	inner := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("internal"))
	inner.RootDir = root
	flaky := &flakyBackingFS{inner: inner, failAfter: 1}
	h := &Handlers{BackingFS: flaky}

	datasetID := uuid.New()
	txnID := uuid.New()
	body := models.CommitDatasetOutputRequest{
		Files: []models.CommitOutputFile{
			{LogicalPath: "a.ndjson", SizeBytes: 3, ContentBase64: []byte("{}\n")},
			{LogicalPath: "b.ndjson", SizeBytes: 4, ContentBase64: []byte("{}\n\n")},
		},
	}
	backing, err := h.outputCommitBackingFor(true)
	require.NoError(t, err)
	staged, _, writes, err := outputCommitFiles(datasetID, txnID, body, backing)
	require.NoError(t, err)
	require.Len(t, writes, 2)
	require.Equal(t, writes[0].Key, staged[0].PhysicalPath)
	require.Equal(t, writes[1].Key, staged[1].PhysicalPath)

	err = h.writeOutputCommitObjects(writes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to write output object")

	// The first object landed, the second did not — but neither is staged
	// in a committed transaction, so no dataset version references them.
	first, readErr := inner.ReadLocalObject(writes[0].Key)
	require.NoError(t, readErr)
	require.Equal(t, "{}\n", string(first))
	_, readErr = inner.ReadLocalObject(writes[1].Key)
	require.Error(t, readErr)

	// The write failure maps to a server error, not a client error.
	rec := httptest.NewRecorder()
	writeOutputCommitObjectError(rec, err)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "failed to write output object")

	// Backing unavailability keeps its dedicated 503 code.
	rec = httptest.NewRecorder()
	writeOutputCommitObjectError(rec, errOutputCommitBackingUnavailable)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "local_backing_filesystem_unavailable")
}

func TestMaxOutputCommitBodyBytesCoversBase64Inflation(t *testing.T) {
	h := &Handlers{}
	require.Equal(t, DefaultMaxOutputCommitContentBytes+DefaultMaxOutputCommitContentBytes/3+1<<20, h.maxOutputCommitBodyBytes())
	h.MaxOutputCommitContentBytes = 9
	require.Equal(t, int64(9+3+1<<20), h.maxOutputCommitBodyBytes())
}

func TestOutputCommitObjectKeyScopesByDatasetAndTransaction(t *testing.T) {
	datasetID := uuid.New()
	txnA := uuid.New()
	txnB := uuid.New()
	keyA := outputCommitObjectKey("store", datasetID, txnA, "part.ndjson")
	keyB := outputCommitObjectKey("store", datasetID, txnB, "part.ndjson")
	require.True(t, strings.HasPrefix(keyA, "store/datasets/"+datasetID.String()+"/transactions/"))
	require.NotEqual(t, keyA, keyB)
	require.True(t, safeObjectKey(keyA))
	require.Equal(t, "local:///"+keyA, physicalURIForOutputObject("local", keyA))
}
