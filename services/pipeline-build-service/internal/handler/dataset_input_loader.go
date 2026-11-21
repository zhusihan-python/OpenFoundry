package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

// maxDatasetInputRows caps how many dataset rows one input node pulls into
// the lightweight runtime, mirroring the dataset service rows limit.
const maxDatasetInputRows = 1000

// PipelineDatasetRowSource reads rows of a dataset's registered files. The
// production implementation talks to dataset-versioning-service; tests inject
// fakes.
type PipelineDatasetRowSource interface {
	ReadDatasetRows(ctx context.Context, req DatasetRowsRequest) (DatasetRowsResult, error)
}

type DatasetRowsRequest struct {
	// DatasetRID is the dataset reference from the node config: a bare uuid
	// or a ri.foundry.main.dataset.<uuid> RID.
	DatasetRID string
	// Path optionally pins a registered logical file; empty lets the
	// dataset service select (single file, or newest in preview mode).
	Path string
	// Branch optionally reads the branch's committed transaction files
	// instead of the latest file index.
	Branch string
	// Limit is clamped to [1, maxDatasetInputRows].
	Limit int
	// AllowTruncation marks preview-style reads, which may pick the newest
	// file and truncate rows at the limit. Formal execution keeps it false
	// so ambiguity and over-limit files fail explicitly.
	AllowTruncation bool
	// Snapshot, when non-nil, pins the read to an explicit dataset
	// snapshot {branch, version, transaction_id} through the DVS
	// rows:scan route (caller bearer only, expected transaction bound to
	// the first and every page). It overrides Branch.
	Snapshot *datasetInputSnapshot
}

type DatasetRowsResult struct {
	DatasetID     string                       `json:"dataset_id"`
	Path          string                       `json:"path"`
	Format        string                       `json:"format"`
	SchemaVersion string                       `json:"schema_version,omitempty"`
	Schema        *datasetRowsSchema           `json:"schema,omitempty"`
	Source        string                       `json:"source"`
	Columns       []string                     `json:"columns"`
	Rows          []map[string]json.RawMessage `json:"rows"`
	Truncated     bool                         `json:"truncated"`
	PhysicalRows  []int                        `json:"physical_rows,omitempty"`
}

// DatasetServiceRowReader reads parsed rows from dataset-versioning-service.
// It only ever calls the configured service base URL with a dataset
// reference; it never accepts caller-supplied hosts, filesystem paths, or
// presigned URLs, and it refuses to follow redirects — including with an
// injected *http.Client. Manual preview/run requests forward the caller's
// bearer token from the request context; the static service token is only
// used when no caller identity is present (e.g. scheduled runs) — and
// NEVER for dataset_snapshot reads, which are caller-only.
type DatasetServiceRowReader struct {
	BaseURL     string
	BearerToken string
	Client      *http.Client
	// SnapshotMaxTotalBytes optionally overrides the hard aggregate byte
	// bound of a dataset_snapshot materialization (tests; zero keeps the
	// production default).
	SnapshotMaxTotalBytes int64
}

func (r DatasetServiceRowReader) ReadDatasetRows(ctx context.Context, req DatasetRowsRequest) (DatasetRowsResult, error) {
	if req.Snapshot != nil {
		// Pinned reads take the rows:scan route with the caller's bearer
		// only — the service token fallback below deliberately does NOT
		// apply to snapshot reads.
		return r.readDatasetSnapshotRows(ctx, req)
	}
	base := strings.TrimRight(strings.TrimSpace(r.BaseURL), "/")
	if base == "" {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_source_not_configured: set DATASET_SERVICE_URL to read dataset_input nodes")
	}
	rid, err := normaliseDatasetRID(req.DatasetRID)
	if err != nil {
		return DatasetRowsResult{}, err
	}
	if _, err := url.ParseRequestURI(base); err != nil {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_source_not_configured: invalid DATASET_SERVICE_URL: %w", err)
	}
	query := url.Values{}
	query.Set("limit", fmt.Sprintf("%d", clampDatasetInputRows(req.Limit)))
	if req.AllowTruncation {
		query.Set("mode", "preview")
	} else {
		query.Set("mode", "execution")
	}
	if path := strings.TrimSpace(req.Path); path != "" {
		query.Set("path", path)
	}
	if branch := strings.TrimSpace(req.Branch); branch != "" {
		query.Set("branch", branch)
	}
	endpoint := base + "/v1/datasets/" + url.PathEscape(rid) + "/files/rows?" + query.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_request_build_failed: %w", err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if token := datasetReadBearerToken(ctx, r.BearerToken); token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	// The rows endpoint must answer directly. Shadow-copy the client so even
	// an injected client never follows a redirect to a presigned or foreign
	// URL; the first response is returned and 3xx fails below.
	redirectless := *client
	redirectless.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := redirectless.Do(httpReq)
	if err != nil {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_read_failed: dataset-versioning returned %s: %s", resp.Status, strings.TrimSpace(string(detail)))
	}
	var out DatasetRowsResult
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&out); err != nil {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_response_decode_failed: %w", err)
	}
	return out, nil
}

// callerBearerKey carries the raw bearer token of the user who triggered the
// current preview or run, so dataset reads happen as the caller, not as the
// service account.
type callerBearerKey struct{}

// contextWithCallerBearer stashes the caller's bearer for downstream
// service-to-service reads. Empty bearers are ignored.
func contextWithCallerBearer(ctx context.Context, bearer string) context.Context {
	bearer = strings.TrimSpace(bearer)
	if bearer == "" {
		return ctx
	}
	return context.WithValue(ctx, callerBearerKey{}, bearer)
}

func callerBearerFromContext(ctx context.Context) string {
	bearer, _ := ctx.Value(callerBearerKey{}).(string)
	return strings.TrimSpace(bearer)
}

// callerBearerFromRequest extracts a Bearer token from an incoming request.
func callerBearerFromRequest(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	const prefix = "Bearer "
	if len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return ""
}

// datasetReadBearerToken prefers the caller's forwarded token; the static
// service token is the fallback for contexts without a user (scheduled
// runs, internal calls).
func datasetReadBearerToken(ctx context.Context, serviceToken string) string {
	if caller := callerBearerFromContext(ctx); caller != "" {
		return caller
	}
	return strings.TrimSpace(serviceToken)
}

// normaliseDatasetRID accepts a bare dataset uuid or a
// ri.foundry.main.dataset.<uuid> RID and rejects everything else, so no
// arbitrary host or path can enter the service URL.
func normaliseDatasetRID(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("dataset_input_invalid_reference: dataset reference is empty")
	}
	if strings.HasPrefix(trimmed, datasetRIDPrefix) {
		suffix := strings.TrimPrefix(trimmed, datasetRIDPrefix)
		if _, err := uuid.Parse(suffix); err == nil {
			return trimmed, nil
		}
		return "", fmt.Errorf("dataset_input_invalid_reference: %q is not a dataset RID", trimmed)
	}
	if _, err := uuid.Parse(trimmed); err == nil {
		return trimmed, nil
	}
	return "", fmt.Errorf("dataset_input_invalid_reference: %q is not a dataset RID", trimmed)
}

func clampDatasetInputRows(limit int) int {
	if limit < 1 {
		return maxDatasetInputRows
	}
	if limit > maxDatasetInputRows {
		return maxDatasetInputRows
	}
	return limit
}

// datasetInputReference is the resolved dataset binding of an input-shaped
// node.
type datasetInputReference struct {
	DatasetRID string
	Path       string
	Branch     string
	Version    string
	Snapshot   *datasetInputSnapshot
}

// datasetInputReferenceFrom extracts the dataset reference for an
// input-shaped node: config.dataset_rid, then config.dataset_id, then the
// first of config.input_dataset_ids, then the node-level input_dataset_ids
// metadata the executor attaches for persisted pipelines.
//
// When the node declares a dataset_snapshot, the reference resolution must
// be UNAMBIGUOUS: EVERY non-empty candidate across the config and the
// node metadata (dataset_rid, dataset_id, all input_dataset_ids entries)
// must agree after canonicalisation, a present dataset_branch must equal
// the snapshot branch, and the snapshot itself must be complete and
// well-formed. Anything else is an error — a pinned read never falls back
// to a guessed dataset or to the branch's latest state.
func datasetInputReferenceFrom(node executor.NodeContext, cfg tableRuntimeConfig) (datasetInputReference, bool, error) {
	snapshot, err := parseDatasetInputSnapshot(cfg.DatasetSnapshot)
	if err != nil {
		return datasetInputReference{}, false, err
	}
	ref := datasetInputReference{
		Path:     strings.TrimSpace(cfg.DatasetPath),
		Branch:   strings.TrimSpace(cfg.DatasetBranch),
		Version:  strings.TrimSpace(cfg.DatasetVersion),
		Snapshot: snapshot,
	}
	if snapshot != nil && ref.Branch != "" && ref.Branch != snapshot.Branch {
		return datasetInputReference{}, false, fmt.Errorf("dataset_snapshot_conflict: node %q pins dataset_branch %q but dataset_snapshot.branch is %q", node.Node.ID, ref.Branch, snapshot.Branch)
	}
	cfgCandidates := append([]string{strings.TrimSpace(cfg.DatasetRID), strings.TrimSpace(cfg.DatasetID)}, cfg.InputDatasetIDs...)
	metaCandidates := metadataStringSlice(node.Node.Metadata, "input_dataset_ids")
	present := make([]string, 0, len(cfgCandidates)+len(metaCandidates))
	for _, candidate := range append(cfgCandidates, metaCandidates...) {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			present = append(present, trimmed)
		}
	}
	if len(present) == 0 {
		if snapshot != nil {
			return datasetInputReference{}, false, fmt.Errorf("dataset_snapshot_invalid: node %q declares dataset_snapshot without any dataset reference (dataset_rid/dataset_id/input_dataset_ids)", node.Node.ID)
		}
		return datasetInputReference{}, false, nil
	}
	// Unpinned nodes keep the historical first-non-empty precedence.
	ref.DatasetRID = present[0]
	if snapshot != nil {
		// Pinned nodes validate the FULL candidate set: any disagreement
		// is an ambiguous binding the pin must not silently resolve.
		canonical := ""
		for _, candidate := range present {
			id, err := canonicalDatasetRIDForComparison(candidate)
			if err != nil {
				return datasetInputReference{}, false, err
			}
			if canonical == "" {
				canonical = id
				continue
			}
			if canonical != id {
				return datasetInputReference{}, false, fmt.Errorf("dataset_snapshot_ambiguous_reference: node %q carries conflicting dataset references %q and %q; a pinned read requires an unambiguous dataset", node.Node.ID, canonical, id)
			}
		}
	}
	return ref, true, nil
}
