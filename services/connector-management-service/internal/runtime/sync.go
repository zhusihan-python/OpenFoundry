// Package runtime contains ports and HTTP adapters for sync execution owned by
// neighbouring services.
package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Caller-visible dataset rejections: the target dataset does not exist or is
// outside the caller's tenant domain. These map to 404/403 at the HTTP edge.
var (
	// ErrDatasetNotFound reports a 404 from dataset-versioning for the target.
	ErrDatasetNotFound = errors.New("dataset not found")
	// ErrDatasetUnauthorized reports a 401: the forwarded caller JWT was
	// rejected upstream (e.g. expired between services). Distinct from 403 so
	// the edge can answer 401 instead of blaming the dataset binding.
	ErrDatasetUnauthorized = errors.New("caller credentials rejected by dataset-versioning")
	// ErrDatasetForbidden reports a 403: the dataset exists but the caller's
	// forwarded JWT may not access it (e.g. another organization's dataset).
	ErrDatasetForbidden = errors.New("dataset not accessible in caller domain")
)

// DependencyError reports a dataset-versioning failure that is NOT a caller
// rejection — transport errors, timeouts, or 5xx responses. Callers fail
// closed on it but must not blame the request.
type DependencyError struct {
	Op      string // "get_dataset" | "commit_output"
	Status  int    // 0 for transport/timeout failures
	Timeout bool
	Detail  string // pre-sanitized; never carries URLs with query strings
}

func (e *DependencyError) Error() string {
	switch {
	case e.Timeout:
		return fmt.Sprintf("dataset-versioning %s timed out", e.Op)
	case e.Status != 0:
		return fmt.Sprintf("dataset-versioning %s responded with status %d", e.Op, e.Status)
	default:
		return fmt.Sprintf("dataset-versioning %s failed: %s", e.Op, e.Detail)
	}
}

// DatasetInfo is the minimum target-dataset view needed for binding checks.
// ActiveBranch (P2.6) is the dataset's current active branch as reported by
// dataset-versioning, WITHOUT the silent active/main fallback the server
// applies on commit: it is the caller-side material for freezing an explicit
// keyed-publish branch at first intent. Empty means the precheck observed no
// usable branch name — P2.6 callers must fail closed on that (never let an
// empty name reach the keyed commit, where the server would resolve it).
type DatasetInfo struct {
	ID           uuid.UUID
	Name         string
	ActiveBranch string
}

// DatasetVersioningPort publishes snapshot content to dataset-versioning-service.
//
// CommitOutput is the P1.3 contract: real NDJSON bytes go through the
// /api/v1/datasets/{rid}/outputs:commit file-write path, and a run only counts
// as published when the response carries a non-nil transaction id. The older
// metadata-only Register surface remains on the concrete client for callers
// that still append version metadata without file content; the sync run path
// must not use it.
//
// GetDataset prechecks a target dataset under the caller's forwarded JWT:
// ErrDatasetNotFound / ErrDatasetForbidden are caller rejections, anything
// else is a *DependencyError (fail closed, but distinguishable).
type DatasetVersioningPort interface {
	CommitOutput(ctx context.Context, req DatasetCommitRequest, authorization string) (*DatasetCommitResult, error)
	GetDataset(ctx context.Context, id uuid.UUID, authorization string) (*DatasetInfo, error)
}

// DatasetCommitRequest is the snapshot publish payload for one sync run.
type DatasetCommitRequest struct {
	// DatasetID is the target dataset uuid (or RID suffix) owned by the
	// caller's tenant; dataset-versioning-service enforces that boundary
	// against the forwarded caller JWT.
	DatasetID       uuid.UUID
	Branch          string
	Format          string // dataset file format, e.g. "TEXT" for NDJSON
	LogicalPath     string // e.g. "sync-<run id>.ndjson"
	ContentType     string // e.g. "application/x-ndjson"
	Content         []byte // exact NDJSON bytes; empty (non-nil) for zero rows
	RowCount        int64
	ContentHash     string // sha256 over Content only
	Summary         string
	CreateIfMissing bool
	TransactionType string
	Metadata        map[string]any
}

// DatasetCommitResult is the publish acknowledgement. TransactionID comes from
// the dataset service's committed transaction response. Status carries the
// dataset transaction status ("COMMITTED" for a usable acknowledgement); the
// P2.5 handler path requires Status=="COMMITTED" and DatasetID to match the
// request exactly — a 2xx with an arbitrary transaction UUID alone is NOT a
// usable ACK.
type DatasetCommitResult struct {
	DatasetID     uuid.UUID
	TransactionID uuid.UUID
	Status        string
}

// HTTPDatasetVersioningClient is the production adapter for dataset-versioning-service.
type HTTPDatasetVersioningClient struct {
	BaseURL string
	Client  *http.Client
}

// CommitOutput posts the snapshot bytes to the dataset file-write + commit
// endpoint, forwarding the caller's platform JWT via the Authorization header.
// The caller's identity — never a source credential — authenticates the
// publish.
func (c HTTPDatasetVersioningClient) CommitOutput(ctx context.Context, req DatasetCommitRequest, authorization string) (*DatasetCommitResult, error) {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return nil, fmt.Errorf("dataset-versioning url is not configured")
	}
	if req.Content == nil {
		req.Content = []byte{}
	}
	contentType := strings.TrimSpace(req.ContentType)
	if contentType == "" {
		contentType = "application/x-ndjson"
	}
	transactionType := strings.TrimSpace(req.TransactionType)
	if transactionType == "" {
		transactionType = "SNAPSHOT"
	}
	if len(req.Metadata) == 0 {
		req.Metadata = map[string]any{}
	}
	body, err := json.Marshal(map[string]any{
		"create_if_missing": req.CreateIfMissing,
		"format":            req.Format,
		"branch":            req.Branch,
		"transaction_type":  transactionType,
		"summary":           req.Summary,
		"files": []map[string]any{{
			"logical_path":   req.LogicalPath,
			"size_bytes":     int64(len(req.Content)),
			"content_type":   contentType,
			"operation":      "ADD",
			"content_base64": req.Content,
		}},
		"metadata": req.Metadata,
	})
	if err != nil {
		return nil, err
	}
	httpClient := c.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/api/v1/datasets/%s/outputs:commit", base, req.DatasetID), bytes.NewReader(body))
	if err != nil {
		return nil, &DependencyError{Op: "commit_output", Detail: SanitizeSyncError(err.Error())}
	}
	hreq.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authorization) != "" {
		hreq.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(hreq)
	if err != nil {
		return nil, transportDependencyError("commit_output", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &DependencyError{Op: "commit_output", Status: resp.StatusCode}
	}
	var decoded struct {
		DatasetID   uuid.UUID `json:"dataset_id"`
		Transaction struct {
			ID     uuid.UUID `json:"id"`
			Status string    `json:"status"`
		} `json:"transaction"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, &DependencyError{Op: "commit_output", Detail: SanitizeSyncError(err.Error())}
	}
	if decoded.Transaction.ID == uuid.Nil {
		return nil, &DependencyError{Op: "commit_output", Detail: "response has no transaction id"}
	}
	// P2.5 usable-ACK contract (adapter side; the handler validates the same
	// contract independently so injected ports cannot bypass it): the dataset
	// must echo the exact requested target and report a COMMITTED
	// transaction. A 2xx that fails either check is an unknown-after-dispatch
	// dependency failure, never a success.
	if decoded.DatasetID != req.DatasetID {
		return nil, &DependencyError{Op: "commit_output", Detail: "response dataset does not match the requested target"}
	}
	if strings.TrimSpace(decoded.Transaction.Status) != "COMMITTED" {
		return nil, &DependencyError{Op: "commit_output", Detail: "transaction status is not COMMITTED"}
	}
	return &DatasetCommitResult{DatasetID: decoded.DatasetID, TransactionID: decoded.Transaction.ID, Status: decoded.Transaction.Status}, nil
}

// transportDependencyError classifies a transport failure. url.Error messages
// embed the full request URL — including any query string — so the detail is
// sanitized before it can reach run records or API responses.
func transportDependencyError(op string, err error) *DependencyError {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &DependencyError{Op: op, Timeout: true}
	}
	return &DependencyError{Op: op, Detail: SanitizeSyncError(err.Error())}
}

// GetDataset resolves a target dataset under the caller's forwarded platform
// JWT. 404/403 are caller rejections (missing / cross-org target); everything
// else — timeouts, connection failures, 5xx — is a *DependencyError so the
// HTTP edge can answer 502/503 instead of blaming the caller.
func (c HTTPDatasetVersioningClient) GetDataset(ctx context.Context, id uuid.UUID, authorization string) (*DatasetInfo, error) {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return nil, &DependencyError{Op: "get_dataset", Detail: "url is not configured"}
	}
	httpClient := c.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v1/datasets/%s", base, id), nil)
	if err != nil {
		return nil, &DependencyError{Op: "get_dataset", Detail: SanitizeSyncError(err.Error())}
	}
	hreq.Header.Set("Accept", "application/json")
	if strings.TrimSpace(authorization) != "" {
		hreq.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(hreq)
	if err != nil {
		return nil, transportDependencyError("get_dataset", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrDatasetNotFound
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, ErrDatasetUnauthorized
	case resp.StatusCode == http.StatusForbidden:
		return nil, ErrDatasetForbidden
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		detail := strings.TrimSpace(string(body))
		if detail == "" {
			return nil, &DependencyError{Op: "get_dataset", Status: resp.StatusCode}
		}
		return nil, &DependencyError{Op: "get_dataset", Status: resp.StatusCode, Detail: SanitizeSyncError(detail)}
	}
	var decoded struct {
		ID           uuid.UUID `json:"id"`
		Name         string    `json:"name"`
		ActiveBranch string    `json:"active_branch"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&decoded); err != nil {
		return nil, &DependencyError{Op: "get_dataset", Detail: SanitizeSyncError(err.Error())}
	}
	if decoded.ID == uuid.Nil || decoded.ID != id {
		// A 200 that does not echo the requested dataset id is an invalid
		// dependency response, never a successful binding.
		return nil, &DependencyError{Op: "get_dataset", Detail: "response does not identify the requested dataset"}
	}
	// Absent/blank active_branch decodes to "" and is reported as such: the
	// P2.6 precheck consumer fails closed on it; older callers that never
	// read the field see no behaviour change.
	return &DatasetInfo{ID: decoded.ID, Name: decoded.Name, ActiveBranch: strings.TrimSpace(decoded.ActiveBranch)}, nil
}

// RegisterLegacyAppend appends version metadata without file content. It is
// retained for compatibility with pre-P1.3 callers; the sync run path must
// publish through CommitOutput so snapshots carry real bytes.
func (c HTTPDatasetVersioningClient) RegisterLegacyAppend(ctx context.Context, datasetID uuid.UUID, body map[string]any, authorization string) error {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return fmt.Errorf("dataset-versioning url is not configured")
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	httpClient := c.Client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/api/v1/datasets/%s/append", base, datasetID), bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authorization) != "" {
		hreq.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(hreq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dataset-versioning responded with status %d", resp.StatusCode)
	}
	return nil
}
