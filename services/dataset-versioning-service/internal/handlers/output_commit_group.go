package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

type groupOutputCommitStore interface {
	WithIdempotentOutputCommitGroup(context.Context, repo.TenantScope, string, []repo.OutputCommitMember) ([]*models.OutputCommitReceipt, bool, error)
}

type outputCommitGroupMember struct {
	DatasetRID string                            `json:"dataset_rid"`
	Output     models.CommitDatasetOutputRequest `json:"output"`
}

func (h *Handlers) CommitDatasetOutputGroup(w http.ResponseWriter, r *http.Request) {
	h.commitOutputGroup(w, r, false)
}

func (h *Handlers) CommitStagedDatasetOutputGroup(w http.ResponseWriter, r *http.Request) {
	h.commitOutputGroup(w, r, true)
}

func (h *Handlers) commitOutputGroup(w http.ResponseWriter, r *http.Request, staged bool) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !canWriteDataset(claims) {
		writeJSONErr(w, http.StatusForbidden, "dataset.write required")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxOutputCommitBodyBytes()))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeOutputCommitFileError(w, errOutputCommitContentTooLarge)
		} else {
			writeJSONErr(w, http.StatusBadRequest, "invalid body")
		}
		return
	}
	var request struct {
		Members []outputCommitGroupMember `json:"members"`
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	var validationErr error
	if staged {
		validationErr = validateStagedOutputCommitGroup(request.Members)
	} else {
		validationErr = validateOutputCommitGroup(request.Members, claims.Sub, h.outputCommitContentLimit())
	}
	if validationErr != nil {
		writeOutputCommitFileError(w, validationErr)
		return
	}
	store, ok := h.Repo.(groupOutputCommitStore)
	if !ok {
		writeDependencyUnavailable(w, "output_commit_group_unavailable", "output group store unavailable")
		return
	}
	objects := groupOutputObjects{backing: h.BackingFS}
	defer objects.close()
	var links stagedCommitLinks
	defer links.close()
	scope := tenantScopeFromClaims(claims)
	members := make([]repo.OutputCommitMember, 0, len(request.Members))
	for _, member := range request.Members {
		id, err := outputDatasetIDFromRID(member.DatasetRID)
		if err != nil {
			writeJSONErr(w, http.StatusNotFound, "dataset not found")
			return
		}
		body := member.Output
		branch := strings.TrimSpace(body.Branch)
		var build func(context.Context, repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error)
		if staged {
			build = h.stagedKeyedOutputCommitBuild(member.DatasetRID, body, branch, id, scope, claims, &links)
		} else {
			build = h.keyedOutputCommitBuild(member.DatasetRID, body, branch, id, scope, claims, objects.write)
		}
		members = append(members, repo.OutputCommitMember{DatasetID: id, Key: body.IdempotencyKey, Build: build})
	}
	receipts, replay, err := store.WithIdempotentOutputCommitGroup(r.Context(), scope, requestFingerprint(raw), members)
	if err != nil {
		// A lost COMMIT response is not proof of rollback. Its file bytes
		// may already be published and must survive an exact replay.
		if !errors.Is(err, repo.ErrCommitOutcomeUnknown) {
			if cleanupErr := errors.Join(objects.remove(), links.remove()); cleanupErr != nil {
				slog.ErrorContext(r.Context(), "output group file cleanup failed", "error", cleanupErr)
				writeJSONErr(w, http.StatusInternalServerError, "output group rolled back but file cleanup failed")
				return
			}
		}
		if errors.Is(err, repo.ErrInvalidOutputCommitGroup) {
			writeJSONErr(w, http.StatusBadRequest, err.Error())
		} else {
			writeKeyedCommitError(w, err)
		}
		return
	}
	response := make([]json.RawMessage, len(receipts))
	for i, receipt := range receipts {
		response[i] = receipt.ResponseBody
	}
	status := http.StatusCreated
	if replay {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string][]json.RawMessage{"receipts": response})
}

func validateOutputCommitGroup(members []outputCommitGroupMember, subject uuid.UUID, contentLimit int64) error {
	// Bound how many dataset locks can be held by a single HTTP request.
	if len(members) < 2 || len(members) > 16 {
		return errors.New("output group requires 2 to 16 datasets")
	}
	var build uuid.UUID
	var total int64
	for _, member := range members {
		body := member.Output
		if body.ManualReservation == nil || !validIdempotencyKey(body.IdempotencyKey) || body.CreateIfMissing ||
			strings.TrimSpace(body.Branch) == "" || len(body.Files) == 0 {
			return errors.New("group members require keyed inline manual snapshot publication on an explicit branch")
		}
		if err := validateManualOutputRequest(body, subject); err != nil {
			return err
		}
		if build == uuid.Nil {
			build = body.ManualReservation.BuildID
		}
		if body.ManualReservation.BuildID != build {
			return errors.New("group reservations must belong to one build")
		}
		for _, link := range body.LineageLinks {
			if err := validateLineageLink(link.Direction, link.TargetRID); err != nil {
				return err
			}
		}
		if bytes.Equal(bytes.TrimSpace(body.Metadata), []byte("null")) {
			return errors.New("group output metadata must be an object when specified")
		}
		for _, file := range body.Files {
			if file.ContentBase64 == nil || (file.Operation != "" && file.Operation != models.FileOperationAdd) ||
				bytes.Equal(bytes.TrimSpace(file.Metadata), []byte("null")) {
				return errors.New("group files require inline ADD content and optional object metadata")
			}
			total += int64(len(file.ContentBase64))
			if total > contentLimit {
				return errOutputCommitContentTooLarge
			}
		}
	}
	return nil
}

func validateStagedOutputCommitGroup(members []outputCommitGroupMember) error {
	if len(members) < 2 || len(members) > 16 {
		return errors.New("output group requires 2 to 16 datasets")
	}
	var buildID uuid.UUID
	var source models.PipelineArtifactManifestInput
	for i, member := range members {
		body := member.Output
		if !validIdempotencyKey(body.IdempotencyKey) || body.CreateIfMissing ||
			strings.TrimSpace(body.Branch) == "" || body.ManualReservation != nil ||
			body.OutputSchemaVersion != nil || body.ArtifactManifest == nil {
			return errors.New("staged group members require keyed staged snapshot publication with a manifest")
		}
		if err := validateStagedCommitRequest(body); err != nil {
			return err
		}
		memberBuild, _ := uuid.Parse(body.BuildID)
		if memberBuild == uuid.Nil {
			return errors.New("staged group members require a nonzero build ID")
		}
		if i == 0 {
			buildID = memberBuild
			source = *body.ArtifactManifest
		}
		if memberBuild != buildID || *body.ArtifactManifest != source {
			return errors.New("staged group members must use the same build, input pin and JobSpec")
		}
		for _, link := range body.LineageLinks {
			if err := validateLineageLink(link.Direction, link.TargetRID); err != nil {
				return err
			}
		}
	}
	return nil
}

// groupOutputObjects records only files this request created, under the
// server-owned transaction paths. The outer rollback is responsible for
// projections and receipts. Local cleanup is confined to the pinned root.
type groupOutputObjects struct {
	backing storageabstraction.BackingFS
	root    *os.Root
	keys    []string
}

func (g *groupOutputObjects) write(writes []outputObjectWrite) error {
	if g.root == nil {
		backing, ok := g.backing.(*storageabstraction.LocalBackingFS)
		if !ok {
			return errOutputCommitBackingUnavailable
		}
		dir := backing.RootDir
		if strings.TrimSpace(dir) == "" {
			dir = "."
		}
		root, err := os.OpenRoot(dir)
		if err != nil {
			return err
		}
		g.root = root
	}
	for _, write := range writes {
		if !safeObjectKey(write.Key) {
			return errors.New("invalid group output object key")
		}
		key := filepath.FromSlash(write.Key)
		if err := g.root.MkdirAll(filepath.Dir(key), 0o755); err != nil {
			return err
		}
		file, err := g.root.OpenFile(key, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		g.keys = append(g.keys, key)
		_, err = file.Write(write.Content)
		if err := errors.Join(err, file.Close()); err != nil {
			return err
		}
	}
	return nil
}

func (g *groupOutputObjects) remove() error {
	var failure error
	for _, key := range g.keys {
		if err := g.root.Remove(key); err != nil && !errors.Is(err, os.ErrNotExist) {
			failure = errors.Join(failure, err)
		}
	}
	return failure
}

func (g *groupOutputObjects) close() {
	if g.root != nil {
		_ = g.root.Close()
	}
}
