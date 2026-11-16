// Bounded REST snapshot fetching for the P1.3 sync runtime.
//
// The fetcher pulls every page of a configured REST resource, materialises the
// records as NDJSON (each record is copied verbatim, no re-encoding) and
// returns content-derived statistics. Every bound — pages, records, response
// bytes, total bytes, per-request timeout — fails the fetch with an explicit
// error; output is never truncated. Egress reuses the http_runtime policy
// gate (allowlist + private-network rules); pagination never leaves the
// source host because every request URL is rebuilt from resource_path, and
// redirects are rejected outright.
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain/http_runtime"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// RESTSnapshotAuth carries external source credentials resolved (and decrypted)
// by the caller. It is the only material the fetcher uses to authenticate
// against the upstream REST source; the platform JWT never reaches it.
type RESTSnapshotAuth struct {
	Type       string // none | basic | bearer | api_key | custom_header
	Scheme     string
	Token      string
	Value      string
	Username   string
	Password   string
	HeaderName string
	QueryParam string
}

// applyToHeaders sets header-based authentication on an outbound request.
func (a RESTSnapshotAuth) applyToHeaders(header http.Header) {
	switch strings.ToLower(strings.TrimSpace(a.Type)) {
	case models.RESTAPIAuthBearer:
		scheme := strings.TrimSpace(a.Scheme)
		if scheme == "" {
			scheme = "Bearer"
		}
		header.Set("Authorization", scheme+" "+a.Token)
	case models.RESTAPIAuthBasic:
		raw := base64.StdEncoding.EncodeToString([]byte(a.Username + ":" + a.Password))
		header.Set("Authorization", "Basic "+raw)
	case models.RESTAPIAuthCustomHeader:
		value := a.Value
		if value == "" {
			value = a.Token
		}
		header.Set(a.HeaderName, value)
	case models.RESTAPIAuthAPIKey:
		if strings.TrimSpace(a.QueryParam) != "" {
			return
		}
		name := strings.TrimSpace(a.HeaderName)
		if name == "" {
			name = "X-API-Key"
		}
		header.Set(name, a.Token)
	}
}

// applyToQuery sets query-param authentication (api_key with query_param set).
func (a RESTSnapshotAuth) applyToQuery(query url.Values) {
	if strings.ToLower(strings.TrimSpace(a.Type)) == models.RESTAPIAuthAPIKey && strings.TrimSpace(a.QueryParam) != "" {
		query.Set(a.QueryParam, a.Token)
	}
}

// IncrementalFetchSpec drives the P2.5 independent completion-checkpoint
// protocol. It is deliberately separate from in-run pagination:
//
//   - RequestParam is a FIXED data-position query parameter sent with the SAME
//     RequestValue on EVERY page request (set once in the base query, so the
//     per-page input is identical by construction). HasRequestValue false
//     (first-ever run without initial_value) omits the parameter entirely.
//   - After pagination terminates (empty next_cursor), the FINAL response body
//     yields the new completion checkpoint via CompletedCheckpointPath. That
//     candidate — never any page's request/pagination cursor — is the only
//     durable data checkpoint material.
type IncrementalFetchSpec struct {
	RequestParam            string
	RequestValue            string
	HasRequestValue         bool
	CompletedCheckpointPath string
}

// RESTSnapshotOptions is the fully-resolved input for one snapshot fetch.
type RESTSnapshotOptions struct {
	BaseURL              string
	Sync                 models.RESTAPISyncConfig
	Auth                 RESTSnapshotAuth
	AllowedEgressHosts   []string
	AllowPrivateNetworks bool
	// Egress is the source's optional `egress` override block. It can only
	// narrow the service boundary — see policy() for the merge rules.
	Egress *models.RESTAPIEgressConfig
	// BeforePage, when set, is invoked before EVERY upstream page request
	// (including the first). A non-nil error aborts the fetch before the
	// request is issued. It lets the P2.1 leased-sync path re-validate its
	// lease at every page boundary instead of relying on heartbeat cadence
	// alone. nil (the default) leaves fetch behaviour untouched for every
	// other caller.
	BeforePage func(ctx context.Context) error
	// Incremental, when non-nil, activates the completion-checkpoint
	// protocol (cursor pagination required — the config layer enforces this;
	// the fetch enforces it defensively too). nil keeps behaviour identical
	// to the pre-P2.5 snapshot fetch.
	Incremental *IncrementalFetchSpec
}

// P2.5 completion-checkpoint failures. Both are PRE-PUBLISH failures by
// construction (they abort FetchRESTSnapshot before any return): the caller
// closes the run as a provable zero-dispatch failure. Error text never echoes
// the opaque candidate value.
var (
	// ErrIncrementalCandidateInvalid: the final response's completed
	// checkpoint is missing, non-string, empty, over the raw-byte bound,
	// not UTF-8, or contains control characters.
	ErrIncrementalCandidateInvalid = errors.New("rest sync: completed checkpoint candidate is missing or invalid")
	// ErrIncrementalNoProgress: the source returned records but the completed
	// checkpoint equals the run's input token — no positive progress can be
	// claimed, so nothing is published.
	ErrIncrementalNoProgress = errors.New("rest sync: records returned but completed checkpoint did not advance")
)

// SnapshotResult carries the materialised NDJSON and content-derived stats.
// ContentHash is sha256 over NDJSON only — no run id, no config envelope.
// CompletedCheckpoint (P2.5) is the validated candidate from the FINAL
// response; it is only set when opts.Incremental was non-nil.
type SnapshotResult struct {
	NDJSON                 []byte
	Rows                   int64
	Bytes                  int64
	ContentHash            string
	Pages                  int
	CompletedCheckpoint    string
	CompletedCheckpointLen int
}

// policy merges the service-level egress posture with the source's optional
// override WITHOUT the replace semantics of EgressPolicyFromState's config
// path: the service boundary is a ceiling the source can only narrow.
//
//   - AllowedHosts: the global allowlist stays authoritative; a source
//     allowed_hosts list may only narrow it further (per-host, checked via
//     sourceNarrowPolicy) and never widens it.
//   - BlockedHosts: the source appends blocks; blocking always wins.
//   - AllowPrivateNetworks: a global prohibition cannot be lifted; the source
//     may only tighten a permissive global to false.
//   - AllowInsecureHTTP: default false; only an explicit source override
//     enables insecure HTTP (the sanctioned P1.3 exception). Host allowlists
//     and blocked hosts still constrain those requests.
func (o *RESTSnapshotOptions) policy() *http_runtime.EgressPolicy {
	policy := http_runtime.EgressPolicyFromState(o.AllowedEgressHosts, o.AllowPrivateNetworks, nil)
	if o.Egress == nil {
		return policy
	}
	if o.Egress.AllowInsecureHTTP != nil && *o.Egress.AllowInsecureHTTP {
		policy.AllowInsecureHTTP = true
	}
	if o.Egress.AllowPrivateNetworks != nil && !*o.Egress.AllowPrivateNetworks {
		policy.AllowPrivateNetworks = false
	}
	if len(o.Egress.BlockedHosts) > 0 {
		policy.BlockedHosts = append(policy.BlockedHosts, o.Egress.BlockedHosts...)
	}
	return policy
}

// sourceNarrowPolicy isolates the source's allowed_hosts list as an
// additional allowlist check. The other gates are neutralised so the check
// only expresses "the source narrowed the host set".
func (o *RESTSnapshotOptions) sourceNarrowPolicy() *http_runtime.EgressPolicy {
	if o.Egress == nil || len(o.Egress.AllowedHosts) == 0 {
		return nil
	}
	return &http_runtime.EgressPolicy{
		AllowedHosts:         o.Egress.AllowedHosts,
		AllowInsecureHTTP:    true,
		AllowPrivateNetworks: true,
	}
}

// FetchRESTSnapshot pulls all pages of the configured resource and materialises
// the records as NDJSON.
func FetchRESTSnapshot(ctx context.Context, client *http.Client, opts RESTSnapshotOptions) (*SnapshotResult, error) {
	// NormalizeRESTAPISyncConfig is idempotent; running it here guarantees the
	// limits/pagination defaults even when a caller passed a hand-built config.
	if err := models.NormalizeRESTAPISyncConfig(&opts.Sync); err != nil {
		return nil, err
	}
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(opts.BaseURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("rest sync: source base_url %q is not a valid http(s) URL", opts.BaseURL)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("rest sync: source base_url must use http or https")
	}
	limits := opts.Sync.Limits
	timeout := time.Duration(limits.RequestTimeoutMS) * time.Millisecond
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	if client.Timeout == 0 {
		client.Timeout = timeout
	}
	// Redirects are rejected: an upstream that answers 3xx would silently move
	// the pull to another host, so treat them as explicit failures.
	if client.CheckRedirect == nil {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	policy := opts.policy()
	// Defensive: the config layer already rejects incremental on non-cursor
	// pagination; the fetch refuses to guess a completion checkpoint from
	// any other termination shape.
	if opts.Incremental != nil && opts.Sync.Pagination.Type != models.RESTAPISyncPaginationCursor {
		return nil, fmt.Errorf("rest sync: incremental fetch requires %q pagination", models.RESTAPISyncPaginationCursor)
	}

	var ndjson []byte
	var rows int64
	pages := 0
	seenCursors := map[string]bool{}
	appendRecords := func(records []json.RawMessage) error {
		for _, record := range records {
			rows++
			if rows > int64(limits.MaxRecords) {
				return fmt.Errorf("rest sync: exceeded max_records limit (%d) — refusing to truncate snapshot", limits.MaxRecords)
			}
			ndjson = append(ndjson, record...)
			ndjson = append(ndjson, '\n')
			if int64(len(ndjson)) > limits.MaxTotalBytes {
				return fmt.Errorf("rest sync: exceeded max_total_bytes limit (%d) — refusing to truncate snapshot", limits.MaxTotalBytes)
			}
		}
		return nil
	}

	fetchPage := func(query url.Values) (records []json.RawMessage, nextCursor string, rawBody []byte, err error) {
		if opts.BeforePage != nil {
			if err := opts.BeforePage(ctx); err != nil {
				return nil, "", nil, fmt.Errorf("rest sync: page gate refused the fetch: %w", err)
			}
		}
		opts.Auth.applyToQuery(query)
		target := *base
		target.Path = strings.TrimSuffix(target.Path, "/") + opts.Sync.ResourcePath
		target.RawQuery = query.Encode()
		if err := http_runtime.ValidateURL(&target, policy); err != nil {
			return nil, "", nil, err
		}
		if narrow := opts.sourceNarrowPolicy(); narrow != nil {
			if err := http_runtime.ValidateURL(&target, narrow); err != nil {
				return nil, "", nil, fmt.Errorf("rest sync: source egress override: %w", err)
			}
		}
		pageCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		hreq, err := http.NewRequestWithContext(pageCtx, http.MethodGet, target.String(), nil)
		if err != nil {
			return nil, "", nil, err
		}
		for name, value := range opts.Sync.Headers {
			hreq.Header.Set(name, value)
		}
		opts.Auth.applyToHeaders(hreq.Header)
		resp, err := client.Do(hreq)
		if err != nil {
			// url.Error embeds the full request URL — including any query
			// parameters, which is exactly where api_key auth places the
			// credential. Sanitize before the message can be persisted or
			// returned; both cancellation kinds get explicit, URL-free
			// descriptions. P2.2 split: a deadline (the request's own budget
			// expired) keeps the historical "timed out", while a cancellation
			// (the run was cancelled/interrupted) is now stated as such —
			// callers must be able to distinguish the two causes.
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				return nil, "", nil, fmt.Errorf("rest sync: upstream request to host %s timed out", target.Host)
			case errors.Is(err, context.Canceled):
				return nil, "", nil, fmt.Errorf("rest sync: upstream request to host %s context canceled", target.Host)
			}
			return nil, "", nil, fmt.Errorf("rest sync: upstream request failed: %s", SanitizeSyncError(err.Error()))
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			return nil, "", nil, fmt.Errorf("rest sync: upstream returned redirect status %d — redirects are not allowed", resp.StatusCode)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, "", nil, fmt.Errorf("rest sync: upstream returned status %d", resp.StatusCode)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, limits.MaxResponseBytes+1))
		if err != nil {
			return nil, "", nil, fmt.Errorf("rest sync: upstream read failed: %w", err)
		}
		if int64(len(body)) > limits.MaxResponseBytes {
			return nil, "", nil, fmt.Errorf("rest sync: upstream response exceeded max_response_bytes limit (%d)", limits.MaxResponseBytes)
		}
		records, err = extractRecords(body, opts.Sync.RecordsPath)
		if err != nil {
			return nil, "", nil, err
		}
		if opts.Sync.Pagination.Type == models.RESTAPISyncPaginationCursor {
			nextCursor, err = extractStringPath(body, opts.Sync.Pagination.NextCursorPath)
			if err != nil {
				return nil, "", nil, err
			}
		}
		return records, nextCursor, body, nil
	}

	baseQuery := url.Values{}
	for name, value := range opts.Sync.QueryParams {
		baseQuery.Set(name, value)
	}
	// P2.5: the incremental request parameter joins the BASE query, so every
	// per-page clone carries the identical fixed value (input-fixed-across-
	// pages invariant by construction). Absent input token => omitted.
	if opts.Incremental != nil && opts.Incremental.HasRequestValue {
		baseQuery.Set(opts.Incremental.RequestParam, opts.Incremental.RequestValue)
	}

	switch opts.Sync.Pagination.Type {
	case models.RESTAPISyncPaginationNone:
		records, _, _, err := fetchPage(cloneValues(baseQuery))
		if err != nil {
			return nil, err
		}
		if err := appendRecords(records); err != nil {
			return nil, err
		}
		pages = 1
	case models.RESTAPISyncPaginationPage:
		for page := opts.Sync.Pagination.StartPage; ; page++ {
			if pages >= limits.MaxPages {
				return nil, fmt.Errorf("rest sync: exceeded max_pages limit (%d) without an empty page — refusing to truncate snapshot", limits.MaxPages)
			}
			query := cloneValues(baseQuery)
			query.Set(opts.Sync.Pagination.PageParam, strconv.Itoa(page))
			if opts.Sync.Pagination.PageSize > 0 {
				query.Set(opts.Sync.Pagination.PageSizeParam, strconv.Itoa(opts.Sync.Pagination.PageSize))
			}
			records, _, _, err := fetchPage(query)
			if err != nil {
				return nil, err
			}
			pages++
			if len(records) == 0 {
				break
			}
			if err := appendRecords(records); err != nil {
				return nil, err
			}
		}
	case models.RESTAPISyncPaginationCursor:
		cursor := ""
		var finalBody []byte
		for {
			if pages >= limits.MaxPages {
				return nil, fmt.Errorf("rest sync: exceeded max_pages limit (%d) with another cursor pending — refusing to truncate snapshot", limits.MaxPages)
			}
			query := cloneValues(baseQuery)
			if cursor != "" {
				query.Set(opts.Sync.Pagination.CursorParam, cursor)
			}
			records, next, body, err := fetchPage(query)
			if err != nil {
				return nil, err
			}
			pages++
			if err := appendRecords(records); err != nil {
				return nil, err
			}
			if strings.TrimSpace(next) == "" {
				finalBody = body
				break
			}
			if seenCursors[next] {
				// Incremental runs must NOT echo the repeated cursor: the
				// upstream next value may equal or contain the confirmed data
				// checkpoint token, and that naked material survives URL
				// sanitization into Run.error (source review item 2). The
				// legacy snapshot wording is preserved verbatim.
				if opts.Incremental != nil {
					return nil, errors.New("rest sync: pagination cursor repeated — detected pagination loop")
				}
				return nil, fmt.Errorf("rest sync: pagination cursor %q repeated — detected pagination loop", next)
			}
			seenCursors[next] = true
			cursor = next
		}
		if opts.Incremental != nil {
			candidate, err := extractStringPath(finalBody, opts.Incremental.CompletedCheckpointPath)
			if err != nil {
				return nil, ErrIncrementalCandidateInvalid
			}
			if err := models.ValidateOpaqueCheckpointValue(candidate, "completed checkpoint"); err != nil {
				return nil, ErrIncrementalCandidateInvalid
			}
			if rows > 0 && opts.Incremental.HasRequestValue && candidate == opts.Incremental.RequestValue {
				return nil, ErrIncrementalNoProgress
			}
			digest := sha256.Sum256(ndjson)
			return &SnapshotResult{
				NDJSON:                 ndjson,
				Rows:                   rows,
				Bytes:                  int64(len(ndjson)),
				ContentHash:            fmt.Sprintf("%x", digest[:]),
				Pages:                  pages,
				CompletedCheckpoint:    candidate,
				CompletedCheckpointLen: len(candidate),
			}, nil
		}
	default:
		return nil, fmt.Errorf("rest sync: unsupported pagination type %q", opts.Sync.Pagination.Type)
	}

	digest := sha256.Sum256(ndjson)
	return &SnapshotResult{
		NDJSON:      ndjson,
		Rows:        rows,
		Bytes:       int64(len(ndjson)),
		ContentHash: fmt.Sprintf("%x", digest[:]),
		Pages:       pages,
	}, nil
}

// extractRecords walks a dot path through the response body and returns the
// records array. Records are returned as raw JSON so values survive verbatim.
func extractRecords(body []byte, recordsPath string) ([]json.RawMessage, error) {
	if len(body) == 0 {
		return nil, errors.New("rest sync: upstream response body is empty")
	}
	if recordsPath == "" {
		var records []json.RawMessage
		if err := json.Unmarshal(body, &records); err != nil {
			return nil, fmt.Errorf("rest sync: response body is not a JSON array (records_path is empty): %w", err)
		}
		return records, nil
	}
	current := json.RawMessage(body)
	for _, segment := range strings.Split(recordsPath, ".") {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(current, &obj); err != nil {
			return nil, fmt.Errorf("rest sync: records_path segment %q does not address a JSON object: %w", segment, err)
		}
		next, ok := obj[segment]
		if !ok {
			return nil, fmt.Errorf("rest sync: records_path segment %q is missing from the response", segment)
		}
		current = next
	}
	var records []json.RawMessage
	if err := json.Unmarshal(current, &records); err != nil {
		return nil, fmt.Errorf("rest sync: records_path %q does not address a JSON array: %w", recordsPath, err)
	}
	return records, nil
}

// extractStringPath reads a dot path to a string value (cursor extraction).
func extractStringPath(body []byte, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	current := json.RawMessage(body)
	for _, segment := range strings.Split(path, ".") {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(current, &obj); err != nil {
			return "", fmt.Errorf("rest sync: cursor path segment %q does not address a JSON object: %w", segment, err)
		}
		next, ok := obj[segment]
		if !ok {
			return "", nil
		}
		current = next
	}
	var value string
	if err := json.Unmarshal(current, &value); err != nil {
		return "", fmt.Errorf("rest sync: cursor path %q does not address a string: %w", path, err)
	}
	return value, nil
}

func cloneValues(in url.Values) url.Values {
	out := url.Values{}
	for name, values := range in {
		out[name] = append([]string(nil), values...)
	}
	return out
}
