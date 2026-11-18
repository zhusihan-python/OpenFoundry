package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"path"

	"github.com/google/uuid"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
)

// DefaultMaxOutputCommitContentBytes caps the total content bytes a single
// outputs:commit request may carry. Requests exceeding the cap fail with 413
// so callers fall back to writing through the regular upload/presign flow
// instead of inflating service memory.
const DefaultMaxOutputCommitContentBytes int64 = 64 << 20

var (
	errOutputCommitContentTooLarge    = errors.New("content_base64 total size exceeds the output commit limit")
	errOutputCommitBackingUnavailable = errors.New("local backing filesystem required for content-bearing output commits")
)

// outputCommitBacking describes the backing store available for server-side
// object writes during an output commit. The zero value means no backing
// store, which is only acceptable for metadata-only commits.
type outputCommitBacking struct {
	fsid    string
	baseDir string
	limit   int64
}

// outputObjectWrite is one physical object the dataset service writes itself
// while committing pipeline output files.
type outputObjectWrite struct {
	Key     string
	Content []byte
}

// maxOutputCommitBodyBytes bounds the request body before JSON decoding: the
// decoded-content limit alone does not stop the server from buffering an
// oversized body first. The cap leaves room for base64 inflation (4/3) plus
// JSON structure and preview rows on top of the content limit.
func (h *Handlers) maxOutputCommitBodyBytes() int64 {
	limit := h.outputCommitContentLimit()
	return limit + limit/3 + 1<<20
}

func (h *Handlers) outputCommitContentLimit() int64 {
	if limit := h.MaxOutputCommitContentBytes; limit > 0 {
		return limit
	}
	return DefaultMaxOutputCommitContentBytes
}

// outputCommitBackingFor resolves the backing store for a commit body. When no
// file carries content the result is the zero value so metadata-only commits
// keep working without a configured backing filesystem.
func (h *Handlers) outputCommitBackingFor(bodyHasContent bool) (outputCommitBacking, error) {
	if !bodyHasContent {
		return outputCommitBacking{}, nil
	}
	if h.BackingFS == nil || h.BackingFS.FSID() != "local" {
		return outputCommitBacking{}, errOutputCommitBackingUnavailable
	}
	if _, ok := h.BackingFS.(localObjectStore); !ok {
		return outputCommitBacking{}, errOutputCommitBackingUnavailable
	}
	return outputCommitBacking{fsid: h.BackingFS.FSID(), baseDir: h.BackingFS.BaseDirectory(), limit: h.outputCommitContentLimit()}, nil
}

// outputCommitObjectKey derives the server-owned physical key for a committed
// output file. The key is scoped by dataset and transaction so retries and
// concurrent commits never overwrite each other, and callers can never direct
// writes at arbitrary paths.
func outputCommitObjectKey(baseDir string, datasetID uuid.UUID, txnID uuid.UUID, logical string) string {
	return path.Join(baseDir, "datasets", datasetID.String(), "transactions", txnID.String(), logical)
}

// writeOutputCommitObjects persists content-bearing output files through the
// backing store before the transaction is committed. A failed write aborts
// the commit (the handler's deferred abort rolls the transaction back), so no
// dataset version ever references missing bytes.
func (h *Handlers) writeOutputCommitObjects(writes []outputObjectWrite) error {
	if len(writes) == 0 {
		return nil
	}
	local, _ := h.BackingFS.(localObjectStore)
	if local == nil {
		return errOutputCommitBackingUnavailable
	}
	for _, write := range writes {
		if !safeObjectKey(write.Key) {
			return fmt.Errorf("invalid output object key: %s", write.Key)
		}
		if err := local.WriteLocalObject(write.Key, write.Content); err != nil {
			return fmt.Errorf("failed to write output object %s: %w", write.Key, err)
		}
	}
	return nil
}

// physicalURIForOutputObject returns the canonical persisted URI for a
// server-written output object. It must stay in sync with the
// trg_dataset_files_from_committed_txn trigger, which derives
// dataset_files.physical_uri as local:/// plus the staged physical path.
func physicalURIForOutputObject(fsid string, key string) string {
	return storageabstraction.PhysicalLocation{FSID: fsid, RelativePath: key}.URI()
}

func writeOutputCommitFileError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInlineParquetInterrupted):
		writeJSONErr(w, http.StatusGatewayTimeout, err.Error())
	case errors.Is(err, errOutputCommitContentTooLarge), errors.Is(err, errInlineParquetWorkLimit):
		writeJSONErr(w, http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, errOutputCommitBackingUnavailable):
		writeDependencyUnavailable(w, "local_backing_filesystem_unavailable", err.Error())
	default:
		writeJSONErr(w, http.StatusBadRequest, err.Error())
	}
}

func writeOutputCommitObjectError(w http.ResponseWriter, err error) {
	if errors.Is(err, errOutputCommitBackingUnavailable) {
		writeDependencyUnavailable(w, "local_backing_filesystem_unavailable", err.Error())
		return
	}
	writeJSONErr(w, http.StatusInternalServerError, err.Error())
}
