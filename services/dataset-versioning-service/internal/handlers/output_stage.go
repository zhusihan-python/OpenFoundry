// WP3.3 increment 3A — the streaming outputs:stage endpoint.
//
// One raw NDJSON request body is streamed, validated record by record
// through the exact rows:scan record reader (same datasetFileRowsMaxLineBytes
// cap, newline included, same JSON-object rule), incrementally hashed and
// counted while a fresh attempt spools into the private staging directory;
// only a confirmed close (fsync + close checked) seals it. The
// single-artifact model is enforced through the repository lease (one
// in-flight upload and one live sealed object per reservation): a
// re-transmission against a live sealed object is hashed against
// io.Discard — no second temp file — and either reuses the sealed
// descriptor byte-identically or fails with an explicit conflict. A lease
// takeover removes only the dead attempt's own server-derived files. A
// descriptor COMMIT whose outcome is unknown never deletes the sealed
// file — it may already be referenced.

package handlers

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// Staging defaults; deployment may override through the Handlers fields
// (env wiring lives in cmd/dataset-versioning-service/main.go).
const (
	DefaultStagedOutputMaxBytes   int64 = 256 << 20
	DefaultStagedOutputMaxRecords int64 = 5_000_000
	DefaultStagedOutputLeaseTTL         = 15 * time.Minute
	DefaultStagedOutputTTL              = 24 * time.Hour
	// stagedLeaseReleaseTimeout bounds the detached lease cleanup after a
	// failed attempt, so a stalled request context can neither block it
	// nor leave it running indefinitely.
	stagedLeaseReleaseTimeout = 5 * time.Second
)

var (
	errStagedOutputUnavailable = errors.New("staged output staging directory is not configured")
	errStagedBodyTooLarge      = errors.New("staged body exceeds the maximum staged output size")
	errStagedRecordCountLimit  = errors.New("staged record count exceeds the maximum")
	errStagedSpoolFailed       = errors.New("failed to seal the staged spool")
)

func (h *Handlers) stagedOutputMaxBytes() int64 {
	if v := h.StagedOutputMaxBytes; v > 0 {
		return v
	}
	return DefaultStagedOutputMaxBytes
}

func (h *Handlers) stagedOutputMaxRecords() int64 {
	if v := h.StagedOutputMaxRecords; v > 0 {
		return v
	}
	return DefaultStagedOutputMaxRecords
}

func (h *Handlers) stagedOutputLeaseTTL() time.Duration {
	if v := h.StagedOutputLeaseTTL; v > 0 {
		return v
	}
	return DefaultStagedOutputLeaseTTL
}

func (h *Handlers) stagedOutputTTL() time.Duration {
	if v := h.StagedOutputTTL; v > 0 {
		return v
	}
	return DefaultStagedOutputTTL
}

// stagedOutputPaths resolves the private staging root against the backing
// filesystem and rejects any overlap with the backing filesystem's FULL
// root directory on the REAL resolved paths: readers (LocalPresignProxy,
// LocalBackingFS.ReadLocalObject) resolve keys against RootDir regardless
// of BaseDir, so equal, nested either way, or a symlink alias all fail
// closed. The overlap verdict is taken on the nearest EXISTING ancestor's
// resolved path BEFORE anything is created (a rejected configuration
// leaves no directory behind), the root is then created and re-verified —
// an ancestor symlink swapped in the window makes the freshly created
// root be removed again. A backing root of "/" rejects every staging
// root. Errors never echo absolute paths to HTTP callers.
type stagedOutputPaths struct {
	stageRootAbs string
	backingRoot  string // absolute filesystem root the backing keys resolve against
	backing      *storageabstraction.LocalBackingFS
}

var errStagedRootOverlaps = errors.New("staged output root overlaps the backing filesystem root")

// stageRootOverlaps reports whether the staging root collides with the
// backing filesystem root (equal or nested either way), with an explicit
// guard for a backing root of the filesystem root itself.
func stageRootOverlaps(stageAbs, rootAbs string) bool {
	if rootAbs == string(filepath.Separator) {
		return true // a staging root can never live outside "/"
	}
	if stageAbs == rootAbs {
		return true
	}
	stageUnder := strings.HasPrefix(stageAbs+string(filepath.Separator), rootAbs+string(filepath.Separator))
	rootUnder := strings.HasPrefix(rootAbs+string(filepath.Separator), stageAbs+string(filepath.Separator))
	return stageUnder || rootUnder
}

func (h *Handlers) stagedOutputPaths() (*stagedOutputPaths, error) {
	if strings.TrimSpace(h.StagedOutputRoot) == "" {
		return nil, errStagedOutputUnavailable
	}
	backing, ok := h.BackingFS.(*storageabstraction.LocalBackingFS)
	if !ok || h.BackingFS == nil || h.BackingFS.FSID() != "local" {
		return nil, errOutputCommitBackingUnavailable
	}
	rootDir := backing.RootDir
	if strings.TrimSpace(rootDir) == "" {
		rootDir = "."
	}
	rootAbs, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	rootAbs, err = resolveDirSymlinks(rootAbs)
	if err != nil {
		return nil, err
	}
	stageRaw, err := filepath.Abs(strings.TrimSpace(h.StagedOutputRoot))
	if err != nil {
		return nil, err
	}
	preResolved, err := resolveDirSymlinks(stageRaw)
	if err != nil {
		return nil, err
	}
	if stageRootOverlaps(preResolved, rootAbs) {
		return nil, errStagedRootOverlaps
	}
	if err := os.MkdirAll(stageRaw, 0o700); err != nil {
		// Fixed message only: the underlying os error carries the
		// absolute path and must not reach an HTTP response.
		slog.ErrorContext(context.Background(), "staged output root creation failed", slog.String("error", err.Error()))
		return nil, errStagedOutputUnavailable
	}
	stageAbs, err := resolveDirSymlinks(stageRaw)
	if err != nil {
		return nil, err
	}
	if stageRootOverlaps(stageAbs, rootAbs) {
		_ = os.Remove(stageRaw)
		return nil, errStagedRootOverlaps
	}
	return &stagedOutputPaths{stageRootAbs: stageAbs, backingRoot: rootAbs, backing: backing}, nil
}

// resolveDirSymlinks evaluates symlinks for the directory itself and, when
// it does not exist yet, for its closest existing ancestor (the remainder
// is then already canonical because the writers below only create fresh
// leaves under it).
func resolveDirSymlinks(dir string) (string, error) {
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		return resolved, nil
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return filepath.Abs(dir)
	}
	resolvedParent, err := resolveDirSymlinks(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(dir)), nil
}

// StageDatasetOutput streams one raw NDJSON body into the private staging
// area and seals it as the reservation's staged object.
//
// Route: POST /v1/datasets/{rid}/outputs:stage
// (?branch&reservation_id&build_id&logical_path&idempotency_key, all
// required). Authorization follows the outputs:commit family order:
// 401, then the dataset.write scope check, then tenant resolution (404
// without leaking cross-tenant existence).
func (h *Handlers) StageDatasetOutput(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !canWriteDataset(claims) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "forbidden", "required_scope": "dataset.write", "dataset_rid": datasetIDParam(r)})
		return
	}
	q := r.URL.Query()
	branch := strings.TrimSpace(q.Get("branch"))
	reservationRaw := strings.TrimSpace(q.Get("reservation_id"))
	buildRaw := strings.TrimSpace(q.Get("build_id"))
	logical := strings.TrimSpace(q.Get("logical_path"))
	key := q.Get("idempotency_key")
	if branch == "" || reservationRaw == "" || buildRaw == "" || logical == "" || key == "" {
		writeJSONErr(w, http.StatusBadRequest, "branch, reservation_id, build_id, logical_path and idempotency_key are required")
		return
	}
	reservationID, err := uuid.Parse(reservationRaw)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "reservation_id must be a UUID")
		return
	}
	buildID, err := uuid.Parse(buildRaw)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "build_id must be a UUID")
		return
	}
	if !validIdempotencyKey(key) {
		writeJSONErr(w, http.StatusBadRequest, "invalid idempotency_key")
		return
	}
	// The logical path must be consumable by rows:scan: reuse its exact
	// staged-path validator on top of the ndjson suffix and the reserved
	// server subtree rule.
	if !validScanStagedPath(logical) || !strings.HasSuffix(logical, ".ndjson") {
		writeJSONErr(w, http.StatusBadRequest, "logical_path must be a rows:scan-consumable object key ending in .ndjson")
		return
	}
	if reservedServerObjectSubtree(logical) {
		writeJSONErr(w, http.StatusBadRequest, "logical_path lands in the server-reserved transactions subtree")
		return
	}
	scope := tenantScopeFromClaims(claims)
	dataset, err := h.Repo.ResolveDatasetForTenant(r.Context(), datasetIDParam(r), scope)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		writeJSONErr(w, http.StatusInternalServerError, "failed to resolve dataset")
		return
	}
	if dataset == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	if branch != dataset.ActiveBranch {
		writeJSONErr(w, http.StatusBadRequest, "staged outputs require the dataset's active branch")
		return
	}
	paths, err := h.stagedOutputPaths()
	if err != nil {
		writeStagedUnavailable(w, err)
		return
	}

	binding := repo.StagedBinding{
		DatasetID:      dataset.ID,
		BranchName:     branch,
		ReservationID:  reservationID,
		BuildID:        buildID,
		ActorID:        claims.Sub,
		IdempotencyKey: key,
		LogicalPath:    logical,
	}
	attemptID := uuid.New()
	obj, supersededAttemptID, sealedExisting, err := h.Repo.AcquireStagedUploadLease(r.Context(), scope, binding, attemptID, h.stagedOutputLeaseTTL())
	if err != nil {
		writeStageAcquireError(w, err)
		return
	}
	resvDir := filepath.Join(paths.stageRootAbs, "datasets", dataset.ID.String(), "reservations", reservationID.String())
	if supersededAttemptID != nil {
		// The lease hand-over committed. Remove only the dead attempt's
		// own server-derived files; a concurrent re-transmission hashing
		// against the live sealed object, or another future attempt, may
		// legitimately hold files in the same directory.
		_ = os.Remove(filepath.Join(resvDir, supersededAttemptID.String()+".tmp"))
		_ = os.Remove(filepath.Join(resvDir, supersededAttemptID.String()+".ndjson"))
	}

	if sealedExisting {
		// The repository already matched the full binding and TTL under
		// the locks (before any body byte is read). The sealed file
		// itself must also still exist, be regular and match its sealed
		// size — otherwise the artifact is unusable and the re-transmit
		// is refused without streaming anything.
		if _, verr := verifySealedArtifact(paths.stageRootAbs, obj); verr != nil {
			h.releaseStagedLeaseBounded(attemptID)
			writeJSONErr(w, http.StatusInternalServerError, verr.Error())
			return
		}
		// Re-transmission against a live sealed object: validate and hash
		// the body against io.Discard (no lease, no temp file), then
		// either reuse the sealed descriptor byte-identically or fail
		// with an explicit conflict. The sealed file is never touched.
		digest, size, _, err := h.streamStageBody(w, r, io.Discard)
		if err != nil {
			return // streamStageBody already wrote the HTTP error
		}
		if obj.SizeBytes == size && obj.SHA256 == digest {
			writeJSON(w, http.StatusOK, stagedResponseFromObject(obj, dataset.ID, branch))
			return
		}
		writeJSON(w, http.StatusConflict, models.ErrorEnvelope{Error: repo.ErrStagedObjectConflict.Error(), Code: strPtr("STAGED_OBJECT_CONFLICT")})
		return
	}

	if err := os.MkdirAll(resvDir, 0o700); err != nil {
		h.releaseStagedLeaseBounded(attemptID)
		writeJSONErr(w, http.StatusInternalServerError, "failed to create staging directory")
		return
	}
	tmpPath := filepath.Join(resvDir, attemptID.String()+".tmp")
	sealedPath := filepath.Join(resvDir, attemptID.String()+".ndjson")
	sealedRel := path.Join("datasets", dataset.ID.String(), "reservations", reservationID.String(), attemptID.String()+".ndjson")

	// The temp spool always goes away on exit (a successful close renamed
	// it first); the sealed file's lifetime is decided by the outcome.
	defer func() { _ = os.Remove(tmpPath) }()

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		h.releaseStagedLeaseBounded(attemptID)
		writeJSONErr(w, http.StatusInternalServerError, "failed to open staging spool")
		return
	}
	digest, size, records, err := h.streamStageBody(w, r, f)
	if err != nil {
		_ = f.Close()
		h.releaseStagedLeaseBounded(attemptID)
		_ = os.Remove(sealedPath)
		return // streamStageBody already wrote the HTTP error
	}
	// Confirmed durability before the seal: fsync data, then close, both
	// checked — only then does the rename make the file the sealed path.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		h.releaseStagedLeaseBounded(attemptID)
		slog.ErrorContext(r.Context(), "staged spool fsync failed", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, errStagedSpoolFailed.Error())
		return
	}
	if err := f.Close(); err != nil {
		h.releaseStagedLeaseBounded(attemptID)
		slog.ErrorContext(r.Context(), "staged spool close failed", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, errStagedSpoolFailed.Error())
		return
	}
	if err := os.Rename(tmpPath, sealedPath); err != nil {
		h.releaseStagedLeaseBounded(attemptID)
		_ = os.Remove(sealedPath)
		slog.ErrorContext(r.Context(), "staged spool rename failed", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, errStagedSpoolFailed.Error())
		return
	}

	sealed, err := h.Repo.SealStagedOutput(r.Context(), scope, binding, attemptID, size, digest, records, sealedRel, h.stagedOutputTTL())
	if err != nil {
		if errors.Is(err, repo.ErrCommitOutcomeUnknown) {
			// The SEALED row (and its reference to sealedPath) may already
			// be durable: keep the file, surface the unknown outcome.
			writeJSON(w, http.StatusServiceUnavailable, models.ErrorEnvelope{Error: repo.ErrCommitOutcomeUnknown.Error(), Code: strPtr("OUTCOME_UNKNOWN"), RetryAfter: int64Ptr(1)})
			return
		}
		// Definite failure: nothing references the sealed file.
		_ = os.Remove(sealedPath)
		h.releaseStagedLeaseBounded(attemptID)
		if errors.Is(err, repo.ErrStagedLeaseLost) {
			writeJSONErr(w, http.StatusConflict, err.Error())
			return
		}
		writeStageAcquireError(w, err)
		return
	}
	h.emitAudit(r.Context(), AuditEvent{
		Actor:      claims.Sub.String(),
		Action:     "dataset.output.stage",
		DatasetRID: "ri.foundry.main.dataset." + dataset.ID.String(),
		Details: map[string]any{
			"staged_object_id": sealed.ID.String(),
			"reservation_id":   reservationID.String(),
			"build_id":         buildID.String(),
			"logical_path":     logical,
			"size_bytes":       size,
			"record_count":     records,
		},
	})
	writeJSON(w, http.StatusCreated, stagedResponseFromObject(sealed, dataset.ID, branch))
}

// verifySealedArtifact checks the sealed file behind a descriptor on the
// real filesystem before anything may rely on it. The durable sealed_path
// must be EXACTLY the path derived from the row's own server-assigned
// UUIDs (dataset/reservation/staged-object) — a drifted or tampered path
// never resolves — and the joined path must stay inside the staging root.
// The file itself must exist, be a regular file (never a symlink —
// os.Link does not dereference oldname) and match the sealed size
// exactly. Returns its absolute path.
func verifySealedArtifact(stageRoot string, obj *repo.StagedOutputObject) (string, error) {
	expected := path.Join("datasets", obj.DatasetID.String(), "reservations", obj.ReservationID.String(), obj.ID.String()+".ndjson")
	if obj.SealedPath != expected {
		return "", errStagedSealedFileMissing
	}
	abs := filepath.Join(stageRoot, filepath.FromSlash(obj.SealedPath))
	if abs != stageRoot && !strings.HasPrefix(abs+string(filepath.Separator), stageRoot+string(filepath.Separator)) {
		return "", errStagedSealedFileMissing
	}
	fi, err := os.Lstat(abs)
	if err != nil {
		return "", errStagedSealedFileMissing
	}
	if !fi.Mode().IsRegular() {
		return "", errStagedSealedFileMissing
	}
	if fi.Size() != obj.SizeBytes {
		return "", fmt.Errorf("sealed staged object %s has %d bytes on disk but %d sealed", obj.ID, fi.Size(), obj.SizeBytes)
	}
	return abs, nil
}

func stagedResponseFromObject(obj *repo.StagedOutputObject, datasetID uuid.UUID, branch string) models.StageDatasetOutputResponse {
	expiresAt := time.Time{}
	if obj.ExpiresAt != nil {
		expiresAt = *obj.ExpiresAt
	}
	return models.StageDatasetOutputResponse{
		StagedObjectID: obj.ID.String(),
		DatasetRID:     "ri.foundry.main.dataset." + datasetID.String(),
		Branch:         branch,
		ReservationID:  obj.ReservationID.String(),
		BuildID:        obj.BuildID.String(),
		LogicalPath:    obj.LogicalPath,
		IdempotencyKey: obj.IdempotencyKey,
		SizeBytes:      obj.SizeBytes,
		SHA256:         obj.SHA256,
		RecordCount:    obj.RecordCount,
		ExpiresAt:      expiresAt,
	}
}

// streamStageBody validates and hashes the raw NDJSON request body into
// sink (the fresh attempt's spool file, or io.Discard for a
// re-transmission). The record constraint is byte-for-byte the one
// rows:scan enforces on read — a record INCLUDING its newline may not
// exceed datasetFileRowsMaxLineBytes — and the JSON-object rule is the
// same (bytesTrim + validNDJSONRecord), so anything stage seals is exactly
// what scan can read back. It returns the hex SHA256 over the exact bytes
// seen, the byte count and the record count. Every failure writes the
// HTTP response carrying the actual reason.
func (h *Handlers) streamStageBody(w http.ResponseWriter, r *http.Request, sink io.Writer) (string, int64, int64, error) {
	body := http.MaxBytesReader(w, r.Body, h.stagedOutputMaxBytes())
	reader := bufio.NewReaderSize(body, 64<<10)
	hasher := sha256.New()
	out := io.MultiWriter(sink, hasher)
	var size, records int64
	var buf []byte
	for {
		chunk, rerr := reader.ReadSlice('\n')
		buf = append(buf, chunk...)
		if len(buf) > datasetFileRowsMaxLineBytes {
			err := fmt.Errorf("staged record %d exceeds %d bytes (newline included)", records+1, datasetFileRowsMaxLineBytes)
			writeStageError(w, err, http.StatusRequestEntityTooLarge, "STAGED_RECORD_TOO_LARGE")
			return "", 0, 0, err
		}
		if rerr == nil || (errors.Is(rerr, io.EOF) && len(buf) > 0) {
			if !validNDJSONRecord(bytesTrim(buf)) {
				err := fmt.Errorf("staged record %d is not a JSON object", records+1)
				writeStageError(w, err, http.StatusBadRequest, "STAGED_RECORD_INVALID")
				return "", 0, 0, err
			}
			if _, werr := out.Write(buf); werr != nil {
				writeJSONErr(w, http.StatusInternalServerError, "failed to write staging spool")
				return "", 0, 0, werr
			}
			size += int64(len(buf))
			records++
			buf = buf[:0]
			if records > h.stagedOutputMaxRecords() {
				err := fmt.Errorf("%w: %d", errStagedRecordCountLimit, h.stagedOutputMaxRecords())
				writeStageError(w, err, http.StatusRequestEntityTooLarge, "STAGED_RECORD_COUNT_EXCEEDED")
				return "", 0, 0, err
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil && !errors.Is(rerr, bufio.ErrBufferFull) {
			var maxErr *http.MaxBytesError
			if errors.As(rerr, &maxErr) {
				err := fmt.Errorf("%w: %d bytes", errStagedBodyTooLarge, h.stagedOutputMaxBytes())
				writeStageError(w, err, http.StatusRequestEntityTooLarge, "STAGED_BODY_TOO_LARGE")
				return "", 0, 0, err
			}
			writeJSONErr(w, http.StatusBadRequest, "invalid staged body: "+rerr.Error())
			return "", 0, 0, rerr
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, records, nil
}

func writeStageError(w http.ResponseWriter, err error, status int, code string) {
	writeJSON(w, status, models.ErrorEnvelope{Error: err.Error(), Code: strPtr(code)})
}

// releaseStagedLeaseBounded drops this attempt's STAGING lease on a
// bounded, detached context: a canceled or stalled request context must
// neither block the cleanup nor outlive it.
func (h *Handlers) releaseStagedLeaseBounded(attemptID uuid.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), stagedLeaseReleaseTimeout)
	defer cancel()
	_ = h.Repo.ReleaseStagedLease(ctx, attemptID)
}

func writeStagedUnavailable(w http.ResponseWriter, err error) {
	if errors.Is(err, errStagedOutputUnavailable) || errors.Is(err, errStagedRootOverlaps) {
		writeDependencyUnavailable(w, "staged_output_staging_unavailable", err.Error())
		return
	}
	if errors.Is(err, errOutputCommitBackingUnavailable) {
		writeDependencyUnavailable(w, "local_backing_filesystem_unavailable", err.Error())
		return
	}
	writeDependencyUnavailable(w, "staged_output_staging_unavailable", err.Error())
}

func writeStageAcquireError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repo.ErrNotFound):
		writeJSONErr(w, http.StatusNotFound, "staged reservation not found")
	case errors.Is(err, repo.ErrStagedReservationInvalid):
		writeJSONErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, repo.ErrStagedBindingMismatch), errors.Is(err, repo.ErrStagedObjectExpired):
		// A re-transmission that does not match the live artifact's exact
		// binding, or whose artifact is past its TTL, is a client error —
		// both are refused before any body byte is read.
		writeJSONErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, repo.ErrStagedUploadInFlight):
		writeJSON(w, http.StatusConflict, models.ErrorEnvelope{Error: err.Error(), Code: strPtr("STAGED_UPLOAD_IN_FLIGHT")})
	case errors.Is(err, repo.ErrStagedObjectAlreadyPublished):
		writeJSON(w, http.StatusConflict, models.ErrorEnvelope{Error: err.Error(), Code: strPtr("STAGED_OBJECT_ALREADY_PUBLISHED")})
	case errors.Is(err, repo.ErrCommitOutcomeUnknown):
		writeJSON(w, http.StatusServiceUnavailable, models.ErrorEnvelope{Error: repo.ErrCommitOutcomeUnknown.Error(), Code: strPtr("OUTCOME_UNKNOWN"), RetryAfter: int64Ptr(1)})
	default:
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
	}
}
