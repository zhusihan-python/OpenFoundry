package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// DatasetHTTPVersioning is the split-database Dataset adapter for the
// build-resolution surface. Every request carries the CALLER's bearer
// from the execution context — the dataset service enforces the tenant
// boundary against the caller's verified JWT, so the adapter never needs
// (and never uses) a static service token that would widen the domain.
// A missing caller fails closed.
//
// Wire contracts verified against dataset-versioning-service handlers:
//   - GET  /api/v1/datasets/{rid}/branches        → Page[RuntimeBranch],
//     read-only (the bare-array UUID variant is NOT used: it calls
//     EnsureDefaultBranch and would mutate state on a dry-run).
//   - GET  /api/v1/datasets/{rid}/schema?branch=  → 200 schema; 404 with
//     "schema not found" (parent+branch verified by the service) maps to
//     the resolver-compatible empty schema; any other 404/5xx is an error.
//   - POST /api/v1/datasets/{rid}/branches/{b}/transactions
//     body {"type":"UPDATE",...} → 201 RuntimeTransaction; id → RID.
//   - POST /api/v1/datasets/{rid}/branches/{b}/transactions/{uuid}:abort
//     → 2xx; the service matches both parent dataset and branch.
type DatasetHTTPVersioning struct {
	BaseURL string
	Client  *http.Client
}

// canonicalDatasetRID strictly parses a bare UUID or a full
// ri.foundry.main.dataset.<uuid> reference into the canonical RID form
// used for the read-only paginated dataset API. Anything else is a
// deterministic pre-write error.
func canonicalDatasetRID(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("dataset reference is empty")
	}
	if strings.HasPrefix(trimmed, datasetRIDPrefix) {
		suffix := strings.TrimPrefix(trimmed, datasetRIDPrefix)
		if _, err := uuid.Parse(suffix); err != nil {
			return "", fmt.Errorf("dataset reference %q is not a valid dataset RID", trimmed)
		}
		return trimmed, nil
	}
	if _, err := uuid.Parse(trimmed); err == nil {
		return datasetRIDPrefix + trimmed, nil
	}
	return "", fmt.Errorf("dataset reference %q is neither a UUID nor a dataset RID", trimmed)
}

// DatasetHTTPError preserves the dataset service's status and machine code
// so CreateBuild/DryRunResolve can answer 404/403/409 precisely instead of
// flattening every failure into a 500 or an "unsupported" 503.
type DatasetHTTPError struct {
	Status   int
	Code     string
	Detail   string
	Upstream bool
}

func (e *DatasetHTTPError) Error() string {
	if e.Upstream {
		return fmt.Sprintf("dataset_upstream_unavailable: %s", e.Detail)
	}
	if e.Code != "" {
		return fmt.Sprintf("dataset_%d_%s: %s", e.Status, e.Code, e.Detail)
	}
	return fmt.Sprintf("dataset_http_%d: %s", e.Status, e.Detail)
}

// ErrDatasetCallerRequired marks a scoped build-resolution request whose
// context carries no caller bearer — the adapter refuses to act rather
// than falling back to a service token.
var ErrDatasetCallerRequired = fmt.Errorf("dataset_caller_required: no caller bearer in context; build resolution must run as the requesting user")

// Aborter/abort transaction support for lifecycle cleanup.
var _ TransactionAborter = DatasetHTTPVersioning{}

// TransactionAborter aborts a previously opened output transaction on the
// dataset side. Implemented by the HTTP adapter and the co-located
// repository.
type TransactionAborter interface {
	AbortTransaction(ctx context.Context, datasetRID, branch, transactionRID string) error
}

// DatasetTenantBoundarySupported reports the boundary capability: the
// adapter relies on the dataset service enforcing tenants from the
// forwarded caller JWT, so it is always capable.
func (DatasetHTTPVersioning) DatasetTenantBoundarySupported(context.Context) bool { return true }

// httpClient returns a client that never follows redirects (the caller
// JWT must not travel anywhere else); injected clients are cloned with
// the policy enforced — it is not optional.
func (d DatasetHTTPVersioning) httpClient() *http.Client {
	base := d.Client
	if base == nil {
		base = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *base
	clone.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &clone
}

func (d DatasetHTTPVersioning) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	bearer := callerBearerFromContext(ctx)
	if bearer == "" {
		return 0, nil, ErrDatasetCallerRequired
	}
	base := strings.TrimRight(d.BaseURL, "/")
	if base == "" {
		return 0, nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "not_configured", Detail: "DATASET_SERVICE_URL is not set", Upstream: true}
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.httpClient().Do(req)
	if err != nil {
		return 0, nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "unreachable", Detail: err.Error(), Upstream: true}
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, payload, nil
}

func datasetErrorFromResponse(status int, payload []byte) *DatasetHTTPError {
	var parsed map[string]any
	_ = json.Unmarshal(payload, &parsed)
	code, _ := parsed["error"].(string)
	message, _ := parsed["message"].(string)
	if message == "" {
		message, _ = parsed["detail"].(string)
	}
	return &DatasetHTTPError{Status: status, Code: code, Detail: message, Upstream: status >= http.StatusInternalServerError}
}

// ListBranches implements resolver.DatasetVersioningRepository via the
// read-only paginated RID form (never the EnsureDefaultBranch UUID form).
func (d DatasetHTTPVersioning) ListBranches(ctx context.Context, datasetRID string) ([]models.BranchSnapshot, error) {
	rid, err := canonicalDatasetRID(datasetRID)
	if err != nil {
		return nil, &DatasetHTTPError{Status: http.StatusBadRequest, Code: "invalid_dataset_rid", Detail: err.Error()}
	}
	datasetRID = rid
	var out []models.BranchSnapshot
	cursor := ""
	for page := 0; page < 100; page++ {
		// The branches route paginates with `limit` + cursor and reports
		// has_more; every page is followed so listings are complete.
		path := "/v1/datasets/" + url.PathEscape(datasetRID) + "/branches?limit=500"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		status, payload, err := d.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, datasetErrorFromResponse(status, payload)
		}
		var pageResult struct {
			Data       []datasetRuntimeBranch `json:"data"`
			NextCursor *string                `json:"next_cursor"`
			HasMore    bool                   `json:"has_more"`
		}
		if err := json.Unmarshal(payload, &pageResult); err != nil {
			return nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "bad_response", Detail: "branches decode: " + err.Error(), Upstream: true}
		}
		for _, branch := range pageResult.Data {
			snapshot := models.BranchSnapshot{Name: branch.Name}
			if branch.HeadTransactionID != nil {
				head := "ri.foundry.main.transaction." + branch.HeadTransactionID.String()
				snapshot.HeadTransactionRID = &head
			}
			out = append(out, snapshot)
		}
		if !pageResult.HasMore {
			return out, nil
		}
		// has_more=true REQUIRES a usable cursor: an empty/missing one
		// would silently truncate the listing, and repeating the same
		// cursor would loop forever — both are bad responses.
		if pageResult.NextCursor == nil || *pageResult.NextCursor == "" {
			return nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "bad_response", Detail: "branches page reports has_more without next_cursor", Upstream: true}
		}
		if *pageResult.NextCursor == cursor {
			return nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "bad_response", Detail: "branches pagination cursor did not advance", Upstream: true}
		}
		cursor = *pageResult.NextCursor
	}
	return nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "pagination_overflow", Detail: "branch listing exceeded 100 pages", Upstream: true}
}

type datasetRuntimeBranch struct {
	Name              string     `json:"name"`
	HeadTransactionID *uuid.UUID `json:"head_transaction_id"`
}

// ViewSchema implements resolver.DatasetVersioningRepository. Only the
// explicit "schema not found" 404 (parent + branch already verified by
// the dataset service) maps to the resolver-compatible empty schema;
// invisible datasets and upstream failures surface as errors.
func (d DatasetHTTPVersioning) ViewSchema(ctx context.Context, datasetRID, branch string) (json.RawMessage, error) {
	datasetRID, err := canonicalDatasetRID(datasetRID)
	if err != nil {
		return nil, &DatasetHTTPError{Status: http.StatusBadRequest, Code: "invalid_dataset_rid", Detail: err.Error()}
	}
	status, payload, err := d.do(ctx, http.MethodGet, "/v1/datasets/"+url.PathEscape(datasetRID)+"/schema?branch="+url.QueryEscape(branch), nil)
	if err != nil {
		return nil, err
	}
	switch {
	case status == http.StatusOK:
		// The service answers with SchemaResponse{schema: ...}; a 200
		// that does not decode into it is a bad response, never a
		// silent empty schema.
		var envelope struct {
			Schema json.RawMessage `json:"schema"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil || len(bytes.TrimSpace(envelope.Schema)) == 0 {
			return nil, &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "bad_response", Detail: "schema response missing \"schema\"", Upstream: true}
		}
		return json.RawMessage(envelope.Schema), nil
	case status == http.StatusNotFound && strings.Contains(strings.ToLower(string(payload)), "schema not found"):
		return json.RawMessage(`{}`), nil
	default:
		return nil, datasetErrorFromResponse(status, payload)
	}
}

// OpenTransaction implements resolver.DatasetVersioningRepository.
func (d DatasetHTTPVersioning) OpenTransaction(ctx context.Context, datasetRID, branch string) (string, error) {
	rid, err := canonicalDatasetRID(datasetRID)
	if err != nil {
		return "", &DatasetHTTPError{Status: http.StatusBadRequest, Code: "invalid_dataset_rid", Detail: err.Error()}
	}
	datasetRID = rid
	body, _ := json.Marshal(map[string]any{
		"type":       "UPDATE",
		"summary":    "pipeline-build output transaction",
		"providence": map[string]any{"origin": "pipeline-build-service"},
	})
	status, payload, err := d.do(ctx, http.MethodPost, "/v1/datasets/"+url.PathEscape(datasetRID)+"/branches/"+url.PathEscape(branch)+"/transactions", body)
	if err != nil {
		return "", err
	}
	if status != http.StatusCreated {
		return "", datasetErrorFromResponse(status, payload)
	}
	var created struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(payload, &created); err != nil || created.ID == uuid.Nil {
		return "", &DatasetHTTPError{Status: http.StatusServiceUnavailable, Code: "bad_response", Detail: "transaction response missing id", Upstream: true}
	}
	return "ri.foundry.main.transaction." + created.ID.String(), nil
}

// AbortTransaction implements TransactionAborter. The dataset service
// matches both the parent dataset and the branch before aborting.
func (d DatasetHTTPVersioning) AbortTransaction(ctx context.Context, datasetRID, branch, transactionRID string) error {
	rid, err := canonicalDatasetRID(datasetRID)
	if err != nil {
		return &DatasetHTTPError{Status: http.StatusBadRequest, Code: "invalid_dataset_rid", Detail: err.Error()}
	}
	datasetRID = rid
	txUUID, parseErr := uuid.Parse(strings.TrimPrefix(transactionRID, "ri.foundry.main.transaction."))
	if parseErr != nil {
		return &DatasetHTTPError{Status: http.StatusBadRequest, Code: "invalid_transaction_rid", Detail: parseErr.Error()}
	}
	status, payload, err := d.do(ctx, http.MethodPost, "/v1/datasets/"+url.PathEscape(datasetRID)+"/branches/"+url.PathEscape(branch)+"/transactions/"+txUUID.String()+":abort", nil)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return datasetErrorFromResponse(status, payload)
	}
	return nil
}
