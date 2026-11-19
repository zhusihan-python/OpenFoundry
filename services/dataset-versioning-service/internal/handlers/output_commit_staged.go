// WP3.3 increment 3A — the STAGED_ADD branch of the keyed outputs:commit.
//
// A staged commit publishes one previously sealed staged object through
// the ordinary keyed pipeline: the same fingerprint/receipt machinery, the
// same datasets row lock, the same atomic publish. Inside the receipt-miss
// build callback the exact reservation is locked and verified (state,
// actor, branch, build marker), atomically finalized as ABORTED, and a
// fresh SNAPSHOT transaction installs the sealed file into the backing
// namespace with a no-replace hard link before the staged-file row (as
// ADD — STAGED_ADD never reaches the database, whose op CHECK only
// accepts ADD/REPLACE/REMOVE and thereby makes older services fail
// closed), the file index, lineage, version cut and receipt all land in
// one COMMIT.

package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

var (
	errStagedCommitShape          = errors.New("invalid staged commit request")
	errStagedActiveBranchRequired = errors.New("staged commits require the dataset's active branch")
	errStagedReservationNotFound  = errors.New("staged reservation not found")
	errStagedObjectNotFound       = errors.New("staged object not found")
	errStagedSealedFileMissing    = errors.New("sealed staged object file is missing")
	errStagedInstallFailed        = errors.New("failed to install sealed staged object")
)

// stagedCommitRequested reports whether the body asks for the staged
// path: any STAGED_ADD file, any file-level staged marker (staged_object_id
// or the staged sha256 declaration), or staged binding fields — every one
// of which must fail closed through the staged shape validation instead of
// silently degrading to a legacy or metadata-only publish that would
// ignore the staged reference.
func stagedCommitRequested(body models.CommitDatasetOutputRequest) bool {
	for _, f := range body.Files {
		if f.Operation == models.FileOperationStagedAdd ||
			f.StagedObjectID != "" || f.SHA256 != "" {
			return true
		}
	}
	return body.ReservationID != "" || body.BuildID != ""
}

// validateStagedCommitRequest is the deterministic, body-intrinsic shape
// check: pure function of the bytes, so a byte-identical replay can never
// fail it after the fingerprint short-circuit.
func validateStagedCommitRequest(body models.CommitDatasetOutputRequest) error {
	if body.ReservationID == "" || body.BuildID == "" {
		return fmt.Errorf("%w: reservation_id and build_id are required", errStagedCommitShape)
	}
	if _, err := uuid.Parse(body.ReservationID); err != nil {
		return fmt.Errorf("%w: reservation_id must be a UUID", errStagedCommitShape)
	}
	if _, err := uuid.Parse(body.BuildID); err != nil {
		return fmt.Errorf("%w: build_id must be a UUID", errStagedCommitShape)
	}
	if len(body.Files) != 1 {
		return fmt.Errorf("%w: exactly one file is required", errStagedCommitShape)
	}
	file := body.Files[0]
	if file.Operation != models.FileOperationStagedAdd {
		return fmt.Errorf("%w: reservation_id/build_id require operation STAGED_ADD", errStagedCommitShape)
	}
	if file.StoragePath != "" {
		return fmt.Errorf("%w: storage_path is not accepted for staged files", errStagedCommitShape)
	}
	if file.ContentBase64 != nil {
		return fmt.Errorf("%w: content_base64 is not accepted for staged files", errStagedCommitShape)
	}
	if file.StagedObjectID == "" {
		return fmt.Errorf("%w: staged_object_id is required", errStagedCommitShape)
	}
	if _, err := uuid.Parse(file.StagedObjectID); err != nil {
		return fmt.Errorf("%w: staged_object_id must be a UUID", errStagedCommitShape)
	}
	if len(file.SHA256) != 64 {
		return fmt.Errorf("%w: sha256 must be a 64-char hex digest", errStagedCommitShape)
	}
	if _, err := hex.DecodeString(file.SHA256); err != nil {
		return fmt.Errorf("%w: sha256 must be a hex digest", errStagedCommitShape)
	}
	if file.SizeBytes < 0 {
		return fmt.Errorf("%w: size_bytes must be non-negative", errStagedCommitShape)
	}
	logical := strings.TrimSpace(file.LogicalPath)
	if strings.HasSuffix(logical, stagedParquetExtension) {
		if !validStagedParquetPath(file.LogicalPath) {
			return fmt.Errorf("%w: logical_path must be a canonical object key ending in .parquet", errStagedCommitShape)
		}
		if file.ContentType == nil || *file.ContentType != stagedParquetContentType {
			return fmt.Errorf("%w: files[].content_type must be %s for Parquet", errStagedCommitShape, stagedParquetContentType)
		}
	} else if !safeObjectKey(logical) || !strings.HasSuffix(logical, ".ndjson") {
		return fmt.Errorf("%w: logical_path must be a safe object key ending in .ndjson", errStagedCommitShape)
	}
	if reservedServerObjectSubtree(logical) {
		return fmt.Errorf("%w: logical_path lands in the server-reserved transactions subtree", errStagedCommitShape)
	}
	if body.PreviewRows != nil {
		return fmt.Errorf("%w: preview_rows are not accepted for staged commits", errStagedCommitShape)
	}
	if body.TransactionType != "" && body.TransactionType != models.TransactionTypeSnapshot {
		return fmt.Errorf("%w: staged commits publish a SNAPSHOT", errStagedCommitShape)
	}
	return nil
}

// stagedKeyedOutputCommitBuild is the staged twin of
// keyedOutputCommitBuild: every store call goes through the tx view, all
// row locks (reservation, staged object) are taken inside the outer keyed
// transaction after its datasets row lock, and the sealed file is
// installed with a no-replace hard link from the private staging root.
func (h *Handlers) stagedKeyedOutputCommitBuild(r *http.Request, body models.CommitDatasetOutputRequest, branchName string, datasetID uuid.UUID, scope repo.TenantScope, claims *authmw.Claims) func(context.Context, repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
	file := body.Files[0]
	reservationID, _ := uuid.Parse(body.ReservationID)
	buildID, _ := uuid.Parse(body.BuildID)
	stagedObjectID, _ := uuid.Parse(file.StagedObjectID)
	logical := strings.TrimSpace(file.LogicalPath)
	isParquet := strings.HasSuffix(logical, stagedParquetExtension)
	summary := strings.TrimSpace(body.Summary)
	if summary == "" {
		summary = "Pipeline staged dataset output commit"
	}
	rawRef := datasetIDParam(r)
	binding := repo.StagedBinding{
		DatasetID:      datasetID,
		BranchName:     branchName,
		ReservationID:  reservationID,
		BuildID:        buildID,
		ActorID:        claims.Sub,
		IdempotencyKey: body.IdempotencyKey,
		LogicalPath:    logical,
	}
	return func(ctx context.Context, tx repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		var zero models.CommitDatasetOutputResponse
		if len(body.LineageLinks) > 0 {
			if err := validateLineageTargetsInTx(ctx, tx, body.LineageLinks, scope); err != nil {
				if errors.Is(err, repo.ErrNotFound) {
					return zero, errKeyedLineageTarget
				}
				return zero, err
			}
		}
		dataset, err := tx.ResolveDatasetForTenant(ctx, rawRef, scope)
		if err != nil {
			return zero, err
		}
		if dataset == nil {
			return zero, repo.ErrNotFound
		}
		if dataset.ActiveBranch != branchName {
			return zero, errStagedActiveBranchRequired
		}
		if err := tx.EnsureDefaultBranch(ctx, dataset); err != nil {
			return zero, err
		}
		branch, err := tx.GetRuntimeBranch(ctx, dataset.ID, branchName)
		if err != nil && !errors.Is(err, repo.ErrNotFound) {
			return zero, err
		}
		if branch == nil {
			return zero, errKeyedBranchMissing
		}

		if body.ArtifactManifest != nil {
			if err := validateManifestSource(ctx, tx, scope, body.ArtifactManifest); err != nil {
				return zero, err
			}
		}

		// Reservation: lock the exact row, verify every binding facet —
		// including that its branch_id is the branch row the dataset
		// currently knows under this name, so a soft-deleted and recreated
		// branch cannot be claimed through name equality alone — and only
		// ever finalize this one reservation. A foreign or stale OPEN
		// transaction is never treated as a hand-off.
		resv, err := tx.GetRuntimeTransactionForUpdate(ctx, dataset.ID, reservationID)
		if err != nil {
			return zero, err
		}
		if resv == nil {
			return zero, errStagedReservationNotFound
		}
		if resv.BranchID != branch.ID {
			return zero, repo.ErrStagedReservationInvalid
		}
		if err := repo.ValidateStagedReservation(resv, binding, branch.ID); err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return zero, errStagedReservationNotFound
			}
			return zero, err
		}

		// Staged object: lock and verify binding, sealed state, TTL and
		// the three-way digest agreement (caller-computed == declared ==
		// sealed).
		obj, err := tx.LoadStagedObjectForUpdate(ctx, dataset.ID, stagedObjectID)
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return zero, errStagedObjectNotFound
			}
			return zero, err
		}
		if err := repo.ValidateStagedObjectForCommit(obj, binding, stagedObjectID, file.SizeBytes, file.SHA256, time.Now().UTC()); err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return zero, errStagedObjectNotFound
			}
			return zero, err
		}

		// Atomic reservation finalization and fresh SNAPSHOT — same outer
		// transaction, so a later failure rolls this back too.
		if err := tx.AbortReservedTransaction(ctx, dataset.ID, reservationID); err != nil {
			return zero, err
		}
		provenance := models.JSONValue([]byte(`{"source":"pipeline-build-service","purpose":"` + repo.StagedReservationPurpose + `","staged_commit":{"build_id":"` + buildID.String() + `","reservation_id":"` + reservationID.String() + `","staged_object_id":"` + stagedObjectID.String() + `"}}`))
		txn, err := tx.StartTransaction(ctx, dataset.ID, branch.ID, branch.Name, models.TransactionTypeSnapshot, summary, provenance, claims.Sub)
		if err != nil {
			return zero, err
		}

		// Metadata is built and validated BEFORE the install: malformed or
		// null caller metadata aborts with zero side effects, and the
		// server-stamped fields (staged object identity, digests,
		// file_count, pipeline_output) are written after the caller's own
		// keys so they can never be forged through body/file metadata.
		commitMeta, merr := stagedCommitMetadata(body, txArtifactManifest(dataset.ID, txn.ID, obj, body.ArtifactManifest), obj)
		if merr != nil {
			return zero, keyedFileError{merr}
		}
		entryType := models.FileEntryTypeFile
		contentType := "application/x-ndjson"
		if isParquet {
			contentType = stagedParquetContentType
		}
		now := time.Now().UTC()
		sizeBytes := obj.SizeBytes
		meta, merr := stagedFileMetadata(dataset.ID, txn.ID, obj, body)
		if merr != nil {
			return zero, keyedFileError{merr}
		}
		paths, err := h.stagedOutputPaths()
		if err != nil {
			return zero, err
		}
		storageKey := outputCommitObjectKey(h.BackingFS.BaseDirectory(), dataset.ID, txn.ID, logical)
		indexEntries := []models.PutDatasetFileIndexEntry{{
			Path:         logical,
			StoragePath:  physicalURIForOutputObject(h.BackingFS.FSID(), storageKey),
			EntryType:    &entryType,
			SizeBytes:    &sizeBytes,
			ContentType:  &contentType,
			Metadata:     meta,
			LastModified: &now,
		}}

		// Install the sealed bytes into the backing namespace through the
		// strictest containment available: the final storage key (which
		// embeds the CONFIGURED BaseDir) must first pass the same lexical
		// object-key rule uploads use, then a Go 1.25 os.Root anchored at
		// the nearest COMMON TRUSTED PARENT of the backing and staging
		// roots performs the mkdir and the no-replace link — any symlink
		// component trying to escape that parent is refused by the Root
		// itself. On top of that, the resolved destination directory must
		// still sit INSIDE the backing root (a detour to another subtree
		// of the parent, e.g. the private staging root, is rejected), and
		// the linked target is re-checked as a regular file of the sealed
		// size. Any anomaly aborts before a single row is written, with a
		// fixed message (never an absolute path) — the detail goes to the
		// log. No generic upload permission is reopened anywhere.
		if !safeObjectKey(storageKey) {
			slog.ErrorContext(ctx, "staged install storage key rejected by lexical object-key rule", slog.String("base_dir", h.BackingFS.BaseDirectory()))
			return zero, errStagedInstallFailed
		}
		srcAbs, verr := verifySealedArtifact(paths.stageRootAbs, obj)
		if verr != nil {
			if !errors.Is(verr, errStagedSealedFileMissing) {
				slog.ErrorContext(ctx, "sealed staged object failed verification", slog.String("staged_object", obj.ID.String()), slog.String("error", verr.Error()))
			}
			return zero, errStagedSealedFileMissing
		}
		dstAbs, ierr := installSealedArtifact(paths, srcAbs, storageKey)
		if ierr != nil {
			slog.ErrorContext(ctx, "staged install failed", slog.String("staged_object", obj.ID.String()), slog.String("error", ierr.Error()))
			return zero, errStagedInstallFailed
		}
		// A failure from here on rolls the outer transaction back BEFORE
		// its COMMIT: this attempt's freshly installed target is uniquely
		// ours and unreferenced, so it is removed again. Once the build
		// succeeds (or the outer COMMIT's outcome is unknown) nothing is
		// ever deleted — a residue that survives is bounded and listed.
		installed := dstAbs
		buildSucceeded := false
		defer func() {
			if installed != "" && !buildSucceeded {
				if rmErr := os.Remove(installed); rmErr != nil {
					slog.ErrorContext(ctx, "staged install orphan cleanup failed (bounded residue, kept)", slog.String("error", rmErr.Error()))
				}
			}
		}()

		// Verify the linked bytes before reporting a manifest digest.
		if isParquet || body.ArtifactManifest != nil {
			if err := verifyInstalledStagedDigest(dstAbs, obj); err != nil {
				slog.ErrorContext(ctx, "installed artifact failed verification", slog.String("staged_object", obj.ID.String()), slog.String("error", err.Error()))
				return zero, errStagedSealedFileMissing
			}
		}

		// The staged-file row is ADD: STAGED_ADD is a wire marker only.
		staged := []models.StageTransactionFile{{
			LogicalPath:  logical,
			PhysicalPath: storageKey,
			SizeBytes:    obj.SizeBytes,
			Operation:    models.FileOperationAdd,
		}}
		commitBaseDir := h.BackingFS.BaseDirectory()
		for _, stagedFile := range staged {
			if err := h.validateStorageRefInTx(ctx, tx, stagedFile.PhysicalPath, commitBaseDir, dataset.ID, &txn.ID); err != nil {
				return zero, keyedFileError{err}
			}
		}
		for _, entry := range indexEntries {
			if err := h.validateStorageRefInTx(ctx, tx, entry.StoragePath, commitBaseDir, dataset.ID, &txn.ID); err != nil {
				return zero, keyedFileError{err}
			}
		}
		if err := tx.StageTransactionFiles(ctx, dataset.ID, txn.ID, staged); err != nil {
			return zero, err
		}
		if err := tx.MergeTransactionMetadata(ctx, dataset.ID, txn.ID, commitMeta); err != nil {
			return zero, err
		}
		if err := tx.ConsumeStagedObject(ctx, obj.ID, txn.ID); err != nil {
			return zero, err
		}
		snapshot, err := tx.CommitTransactionWithPublish(ctx, dataset.ID, txn.ID, indexEntries, body.LineageLinks)
		if err != nil {
			return zero, err
		}
		buildSucceeded = true
		return models.CommitDatasetOutputResponse{
			DatasetID:    dataset.ID,
			DatasetRID:   "ri.foundry.main.dataset." + dataset.ID.String(),
			Branch:       branch.Name,
			Transaction:  snapshot.Transaction,
			Schema:       body.Schema,
			Files:        snapshot.Files,
			Preview:      snapshot.Preview,
			LineageLinks: snapshot.LineageLinks,
		}, nil
	}
}

// stagedFileMetadata merges caller metadata (file-level first, then
// body-level) and stamps the server-owned fields LAST, so no caller key —
// including a JSON null body/file metadata, which unmarshals into a nil
// map — can crash the merge or forge the staged-object identity.
func stagedFileMetadata(datasetID uuid.UUID, txnID uuid.UUID, obj *repo.StagedOutputObject, body models.CommitDatasetOutputRequest) (models.JSONValue, error) {
	meta := map[string]any{}
	if file := body.Files[0]; len(file.Metadata) > 0 {
		if err := json.Unmarshal(file.Metadata, &meta); err != nil {
			return nil, err
		}
		if meta == nil {
			meta = map[string]any{}
		}
	}
	if len(body.Metadata) > 0 {
		if err := json.Unmarshal(body.Metadata, &meta); err != nil {
			return nil, err
		}
		if meta == nil {
			meta = map[string]any{}
		}
	}
	delete(meta, artifactManifestKey)
	meta["dataset_id"] = datasetID.String()
	meta["transaction_id"] = txnID.String()
	meta["staged_object_id"] = obj.ID.String()
	meta["sha256"] = obj.SHA256
	meta["record_count"] = obj.RecordCount
	meta["build_id"] = obj.BuildID.String()
	if strings.HasSuffix(obj.LogicalPath, stagedParquetExtension) {
		meta["format"] = stagedParquetFormat
		meta["schema_version"] = stagedParquetSchemaVersion
	}
	if body.Schema != nil {
		meta["schema"] = body.Schema
	}
	raw, err := json.Marshal(meta)
	return models.JSONValue(raw), err
}

// stagedCommitMetadata applies the same rule at transaction level: caller
// keys first (null-safe), then the server-stamped pipeline_output /
// file_count / staged_output block is rewritten so it can never be
// forged through body metadata.
func stagedCommitMetadata(body models.CommitDatasetOutputRequest, manifest *artifactManifest, obj *repo.StagedOutputObject) (models.JSONValue, error) {
	meta := map[string]any{}
	if len(body.Metadata) > 0 {
		if err := json.Unmarshal(body.Metadata, &meta); err != nil {
			return nil, err
		}
		if meta == nil {
			meta = map[string]any{}
		}
	}
	delete(meta, artifactManifestKey)
	if manifest != nil {
		meta[artifactManifestKey] = manifest
	}
	if body.Schema != nil {
		meta["schema"] = body.Schema
	}
	meta["pipeline_output"] = true
	meta["file_count"] = 1
	meta["staged_output"] = map[string]any{
		"staged_object_id": obj.ID.String(),
		"reservation_id":   obj.ReservationID.String(),
		"build_id":         obj.BuildID.String(),
		"sha256":           obj.SHA256,
		"record_count":     obj.RecordCount,
	}
	raw, err := json.Marshal(meta)
	return models.JSONValue(raw), err
}

// installSealedArtifact hard-links the verified sealed file into the
// backing namespace at storageKey under strict containment:
//  1. the destination directory is created through an os.Root anchored at
//     the nearest common ancestor of the backing and staging roots, so a
//     symlink component cannot lead the mkdir outside that parent;
//  2. after creation, the directory's REAL resolved path must still be
//     the backing root itself or below it (a detour into any sibling
//     subtree of the parent — including the private staging root — is
//     rejected);
//  3. the link itself goes through the same Root (no-replace), and the
//     linked target is re-checked as a regular file of the sealed size.
//
// It returns the absolute installed path. Errors carry no absolute paths.
func installSealedArtifact(paths *stagedOutputPaths, srcAbs string, storageKey string) (string, error) {
	common := commonParentDir(paths.backingRoot, paths.stageRootAbs)
	root, err := os.OpenRoot(common)
	if err != nil {
		return "", err
	}
	defer func() { _ = root.Close() }()
	toRel := func(abs string) (string, error) {
		rel, err := filepath.Rel(common, abs)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
			return "", errStagedInstallFailed
		}
		return filepath.ToSlash(rel), nil
	}
	dstAbsRaw := filepath.Join(paths.backingRoot, filepath.FromSlash(storageKey))
	dstRel, err := toRel(dstAbsRaw)
	if err != nil {
		return "", err
	}
	if err := root.MkdirAll(filepath.ToSlash(filepath.Dir(dstRel)), 0o755); err != nil {
		return "", err
	}
	dstDirAbs := filepath.Dir(dstAbsRaw)
	resolvedDir, err := filepath.EvalSymlinks(dstDirAbs)
	if err != nil {
		return "", err
	}
	if resolvedDir != paths.backingRoot &&
		!strings.HasPrefix(resolvedDir+string(filepath.Separator), paths.backingRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("install target resolves outside the backing root")
	}
	srcRel, err := toRel(srcAbs)
	if err != nil {
		return "", err
	}
	dstAbs := filepath.Join(resolvedDir, filepath.Base(storageKey))
	dstRel, err = toRel(dstAbs)
	if err != nil {
		return "", err
	}
	if err := root.Link(srcRel, dstRel); err != nil {
		return "", err
	}
	fi, err := os.Lstat(dstAbs)
	if err != nil || !fi.Mode().IsRegular() {
		return "", fmt.Errorf("installed target is not a regular file")
	}
	return dstAbs, nil
}

func verifyInstalledStagedDigest(installed string, obj *repo.StagedOutputObject) error {
	f, err := os.Open(installed)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() != obj.SizeBytes {
		return repo.ErrStagedDigestMismatch
	}
	hasher := sha256.New()
	n, err := io.Copy(hasher, io.LimitReader(f, obj.SizeBytes))
	if err != nil {
		return err
	}
	if n != obj.SizeBytes || hex.EncodeToString(hasher.Sum(nil)) != obj.SHA256 {
		return repo.ErrStagedDigestMismatch
	}
	return nil
}

// commonParentDir returns the deepest existing directory that is an
// ancestor of both absolute paths (both must exist and be resolved).
func commonParentDir(a, b string) string {
	as := strings.Split(a, string(filepath.Separator))
	bs := strings.Split(b, string(filepath.Separator))
	common := string(filepath.Separator)
	for i := 1; i < len(as) && i < len(bs); i++ {
		if as[i] != bs[i] {
			break
		}
		common = filepath.Join(common, as[i])
	}
	return common
}

// writeStagedCommitError maps staged-commit failures onto their HTTP
// contracts inside writeKeyedCommitError.
func writeStagedCommitError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, errStagedCommitShape):
		writeJSONErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errStagedActiveBranchRequired):
		writeJSONErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errStagedReservationNotFound), errors.Is(err, errStagedObjectNotFound):
		writeJSONErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, repo.ErrStagedReservationInvalid),
		errors.Is(err, repo.ErrStagedBindingMismatch),
		errors.Is(err, repo.ErrStagedDigestMismatch),
		errors.Is(err, repo.ErrStagedObjectExpired):
		writeJSONErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, repo.ErrStagedObjectNotSealed), errors.Is(err, repo.ErrStagedObjectAlreadyPublished):
		writeJSON(w, http.StatusConflict, models.ErrorEnvelope{Error: err.Error(), Code: strPtr("STAGED_OBJECT_NOT_SEALED")})
	case errors.Is(err, errStagedSealedFileMissing), errors.Is(err, errStagedInstallFailed):
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
	case errors.Is(err, errStagedOutputUnavailable):
		writeDependencyUnavailable(w, "staged_output_staging_unavailable", err.Error())
	case errors.Is(err, errOutputCommitBackingUnavailable):
		writeDependencyUnavailable(w, "local_backing_filesystem_unavailable", err.Error())
	default:
		return false
	}
	return true
}
