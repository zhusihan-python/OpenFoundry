package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

// REST sync pagination strategies supported by the snapshot runtime.
const (
	RESTAPISyncPaginationPage   = "page"
	RESTAPISyncPaginationCursor = "cursor"
	RESTAPISyncPaginationNone   = "none"
)

// RESTAPISyncConfig is the `sync` block of a rest_api source config. It is a
// first-class field on RESTAPISourceConfig so NormalizeRESTAPISourceConfig
// round-trips it (unknown blocks are dropped when the struct is re-marshalled).
//
// Contract:
//   - resource_path is an absolute path on the source's base_url host; the
//     runtime never follows pagination links to another host.
//   - records_path is a dot path ("data.items") from the response body to the
//     records array; "" addresses the root array. Each record is copied into
//     the NDJSON snapshot verbatim (no re-encoding, so identifiers such as
//     SKUs with leading zeroes survive).
//   - Pagination is page-number based, cursor based, or single-shot.
//     Cursor values are opaque strings passed back via cursor_param; the
//     runtime rebuilds every request from resource_path so responses can
//     never redirect the sync to another host.
//   - All limits fail the run explicitly; snapshots are never truncated.
type RESTAPISyncConfig struct {
	ResourcePath string                 `json:"resource_path"`
	RecordsPath  string                 `json:"records_path,omitempty"`
	Pagination   RESTAPISyncPagination  `json:"pagination"`
	Limits       RESTAPISyncLimits      `json:"limits"`
	Headers      map[string]string      `json:"headers,omitempty"`
	QueryParams  map[string]string      `json:"query_params,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	// Admission is the optional P2.3 per-source admission policy. It bounds
	// admission of NEW runs (unfinished cap + minimum interval) and is
	// deliberately NOT part of Limits, which bounds a single run's pull.
	Admission *SyncAdmissionPolicy `json:"admission,omitempty"`
	// Incremental is the optional P2.5 cross-run completion-checkpoint opt-in.
	// Omitted/null/{} keeps the legacy full-snapshot behavior unchanged; an
	// enabled block requires cursor pagination and an independent completion
	// checkpoint protocol (see RESTAPIIncrementalConfig).
	Incremental *RESTAPIIncrementalConfig `json:"incremental,omitempty"`
	// Recovery is the optional P2.6 typed opt-in for the idempotent
	// publish + commit-recovery protocol. Omitted/null/disabled keeps the
	// P25 behavior byte-identical; enabling requires an enabled incremental
	// block (the protocol protects incremental publishes only) and freezes
	// recovery_protocol='p26-v1' on runs admitted from that point on.
	Recovery *RESTAPIRecoveryConfig `json:"recovery,omitempty"`
}

// RESTAPIRecoveryConfig is the `sync.recovery` block: the P2.6 opt-in. The
// block is deliberately minimal — protocol behavior is versioned by the
// frozen run marker, not by config knobs, so an admitted run keeps its
// protocol for life regardless of later config edits.
type RESTAPIRecoveryConfig struct {
	Enabled bool `json:"enabled"`
}

// RESTAPIIncrementalConfig is the `sync.incremental` block: the P2.5 opt-in
// for cross-run incremental reads. The protocol is deliberately separate from
// in-run pagination:
//
//   - RequestParam is a FIXED data-position query parameter (e.g. "since")
//     sent with the SAME value on EVERY page request of a run; the value is
//     the last confirmed completion checkpoint (or the optional InitialValue
//     on the first-ever run; absent => the parameter is omitted entirely).
//   - CompletedCheckpointPath is a dot path into the FINAL response — the
//     response whose next_cursor terminated pagination — yielding the new
//     opaque completion checkpoint. Pagination cursors are NEVER saved as
//     data checkpoints.
//
// Candidate tokens are opaque: never trimmed, never lexicographically
// compared, never echoed into errors/logs/metadata (lengths only).
type RESTAPIIncrementalConfig struct {
	Enabled                 bool   `json:"enabled"`
	RequestParam            string `json:"request_param"`
	CompletedCheckpointPath string `json:"completed_checkpoint_path"`
	// InitialValue is presence-tracked: omitted/null means absent (the first
	// run reads from the stream start); an explicit empty string is rejected.
	// When non-nil it must satisfy the opaque-token rules (nonempty, <=512
	// raw bytes, UTF-8, no control characters) and is never trimmed.
	InitialValue *string `json:"initial_value,omitempty"`
}

// RESTAPIEgressConfig is the optional `egress` block of a rest_api source
// config. It is the per-request override surface consumed by
// http_runtime.EgressPolicyFromState: absent keys inherit the service-level
// defaults (allowed-host allowlist, private-network posture, and rejecting
// insecure HTTP), so only an explicit source override can relax them.
type RESTAPIEgressConfig struct {
	AllowedHosts         []string `json:"allowed_hosts,omitempty"`
	BlockedHosts         []string `json:"blocked_hosts,omitempty"`
	AllowPrivateNetworks *bool    `json:"allow_private_networks,omitempty"`
	AllowInsecureHTTP    *bool    `json:"allow_insecure_http,omitempty"`
}

type RESTAPISyncPagination struct {
	Type           string `json:"type"`
	PageParam      string `json:"page_param,omitempty"`
	PageSizeParam  string `json:"page_size_param,omitempty"`
	PageSize       int    `json:"page_size,omitempty"`
	StartPage      int    `json:"start_page,omitempty"`
	CursorParam    string `json:"cursor_param,omitempty"`
	NextCursorPath string `json:"next_cursor_path,omitempty"`
}

type RESTAPISyncLimits struct {
	MaxPages         int   `json:"max_pages,omitempty"`
	MaxRecords       int   `json:"max_records,omitempty"`
	MaxResponseBytes int64 `json:"max_response_bytes,omitempty"`
	MaxTotalBytes    int64 `json:"max_total_bytes,omitempty"`
	RequestTimeoutMS int   `json:"request_timeout_ms,omitempty"`
}

// NormalizeRESTAPISyncConfig validates a sync block and fills in defaults.
func NormalizeRESTAPISyncConfig(cfg *RESTAPISyncConfig) error {
	if cfg == nil {
		return errors.New("rest_api source sync config is required for snapshot sync")
	}
	cfg.ResourcePath = strings.TrimSpace(cfg.ResourcePath)
	if cfg.ResourcePath == "" || !strings.HasPrefix(cfg.ResourcePath, "/") {
		return errors.New("rest_api source sync.resource_path must be an absolute path on the source host")
	}
	if strings.Contains(cfg.ResourcePath, "://") {
		return errors.New("rest_api source sync.resource_path must be a path, not a URL")
	}
	cfg.RecordsPath = strings.TrimSpace(cfg.RecordsPath)
	if cfg.RecordsPath != "" {
		for _, segment := range strings.Split(cfg.RecordsPath, ".") {
			if strings.TrimSpace(segment) == "" {
				return errors.New("rest_api source sync.records_path must be a dot path with non-empty segments")
			}
		}
	}
	pagination := &cfg.Pagination
	pagination.Type = strings.ToLower(strings.TrimSpace(pagination.Type))
	switch pagination.Type {
	case "":
		pagination.Type = RESTAPISyncPaginationPage
	case RESTAPISyncPaginationPage, RESTAPISyncPaginationCursor, RESTAPISyncPaginationNone:
	default:
		return fmt.Errorf("rest_api source sync.pagination.type must be %q, %q or %q", RESTAPISyncPaginationPage, RESTAPISyncPaginationCursor, RESTAPISyncPaginationNone)
	}
	pagination.PageParam = strings.TrimSpace(pagination.PageParam)
	if pagination.PageParam == "" {
		pagination.PageParam = "page"
	}
	pagination.PageSizeParam = strings.TrimSpace(pagination.PageSizeParam)
	if pagination.PageSizeParam == "" {
		pagination.PageSizeParam = "page_size"
	}
	if pagination.PageSize < 0 {
		pagination.PageSize = 0
	}
	if pagination.StartPage <= 0 {
		pagination.StartPage = 1
	}
	pagination.CursorParam = strings.TrimSpace(pagination.CursorParam)
	if pagination.CursorParam == "" {
		pagination.CursorParam = "cursor"
	}
	pagination.NextCursorPath = strings.TrimSpace(pagination.NextCursorPath)
	if pagination.Type == RESTAPISyncPaginationCursor && pagination.NextCursorPath == "" {
		return errors.New("rest_api source sync.pagination.next_cursor_path is required for cursor pagination")
	}
	if pagination.Type == RESTAPISyncPaginationPage && pagination.PageSize > 0 && pagination.PageSizeParam == "" {
		return errors.New("rest_api source sync.pagination.page_size_param is required when page_size is set")
	}
	limits := &cfg.Limits
	if limits.MaxPages <= 0 {
		limits.MaxPages = 100
	}
	if limits.MaxPages > 10000 {
		return errors.New("rest_api source sync.limits.max_pages must not exceed 10000")
	}
	if limits.MaxRecords <= 0 {
		limits.MaxRecords = 10000
	}
	if limits.MaxRecords > 1000000 {
		return errors.New("rest_api source sync.limits.max_records must not exceed 1000000")
	}
	if limits.MaxResponseBytes <= 0 {
		limits.MaxResponseBytes = 8 << 20
	}
	if limits.MaxTotalBytes <= 0 {
		limits.MaxTotalBytes = 64 << 20
	}
	if limits.RequestTimeoutMS <= 0 {
		limits.RequestTimeoutMS = 10000
	}
	if limits.RequestTimeoutMS < 100 || limits.RequestTimeoutMS > 120000 {
		return errors.New("rest_api source sync.limits.request_timeout_ms must be between 100 and 120000")
	}
	// P2.3: validate the admission subtree in place so the typed roundtrip
	// can never persist an illegal policy. Missing/null/{} stay valid and
	// enable nothing.
	if err := ValidateSyncAdmissionPolicy(cfg.Admission); err != nil {
		return err
	}
	// P2.5: validate the incremental subtree when enabled. A present but
	// disabled block enables nothing and is not deeply validated.
	if err := validateIncrementalConfig(cfg); err != nil {
		return err
	}
	// P2.6: validate the recovery opt-in. Same discipline: a present but
	// disabled block enables nothing; an enabled one fails closed unless the
	// P2.5 incremental protocol it builds on is also enabled.
	if err := validateRecoveryConfig(cfg); err != nil {
		return err
	}
	return nil
}

// validateRecoveryConfig enforces the P2.6 opt-in contract on the sync block
// alone. v2.1: recovery does NOT require incremental — the protocol covers
// both incremental and plain snapshot publishes, so the block itself carries
// no further cross-field rules today; the hook stays for future contract
// checks. Because `recovery` is a new field, no pre-P26 config can trip it.
func validateRecoveryConfig(cfg *RESTAPISyncConfig) error {
	_ = cfg
	return nil
}

// validateIncrementalConfig enforces the P2.5 completion-checkpoint contract
// on the sync block alone. Cross-block conflicts (the source auth query
// placement) are NOT visible here; those live in
// ValidateRESTAPISourceIncremental (rest_api_source.go), which the typed
// roundtrip and the authoritative Repo paths both call.
func validateIncrementalConfig(cfg *RESTAPISyncConfig) error {
	inc := cfg.Incremental
	if inc == nil || !inc.Enabled {
		return nil
	}
	if cfg.Pagination.Type != RESTAPISyncPaginationCursor {
		return fmt.Errorf("rest_api source sync.incremental requires pagination.type %q (got %q)", RESTAPISyncPaginationCursor, cfg.Pagination.Type)
	}
	inc.RequestParam = strings.TrimSpace(inc.RequestParam)
	if inc.RequestParam == "" {
		return errors.New("rest_api source sync.incremental.request_param is required")
	}
	for _, reserved := range []string{
		strings.TrimSpace(cfg.Pagination.PageParam),
		strings.TrimSpace(cfg.Pagination.PageSizeParam),
		strings.TrimSpace(cfg.Pagination.CursorParam),
	} {
		if inc.RequestParam == reserved {
			return fmt.Errorf("rest_api source sync.incremental.request_param %q collides with a pagination parameter", inc.RequestParam)
		}
	}
	for name := range cfg.QueryParams {
		if inc.RequestParam == strings.TrimSpace(name) {
			return fmt.Errorf("rest_api source sync.incremental.request_param %q collides with a configured query parameter", inc.RequestParam)
		}
	}
	inc.CompletedCheckpointPath = strings.TrimSpace(inc.CompletedCheckpointPath)
	if inc.CompletedCheckpointPath == "" {
		return errors.New("rest_api source sync.incremental.completed_checkpoint_path is required")
	}
	for _, segment := range strings.Split(inc.CompletedCheckpointPath, ".") {
		if strings.TrimSpace(segment) == "" {
			return errors.New("rest_api source sync.incremental.completed_checkpoint_path must be a dot path with non-empty segments")
		}
	}
	if inc.InitialValue != nil {
		if err := ValidateOpaqueCheckpointValue(*inc.InitialValue, "sync.incremental.initial_value"); err != nil {
			return err
		}
	}
	return nil
}

// MaxIncrementalCheckpointBytes is the raw-byte bound for opaque completion
// checkpoint values (config initial_value, extracted candidates, stored
// values). The database CHECK mirrors it with octet_length.
const MaxIncrementalCheckpointBytes = 512

// ValidateOpaqueCheckpointValue enforces the opaque-token rules on raw bytes:
// nonempty, <=512 bytes, valid UTF-8, no control characters (C0, DEL, C1),
// and no U+FFFD replacement rune. json.Unmarshal silently coerces malformed
// raw UTF-8 AND invalid UTF-16 surrogate escapes in quoted strings to U+FFFD
// (verified for both pinned toolchains, docs-verification.md), so a decoded
// token containing U+FFFD cannot prove the wire/config bytes were preserved —
// it is rejected under a documented narrow unsupported-token rule instead of
// being silently normalized. The value is NEVER trimmed and NEVER echoed in
// the returned error.
func ValidateOpaqueCheckpointValue(value, field string) error {
	if value == "" {
		return fmt.Errorf("rest_api source %s must not be empty", field)
	}
	if len(value) > MaxIncrementalCheckpointBytes {
		return fmt.Errorf("rest_api source %s exceeds the %d raw-byte limit", field, MaxIncrementalCheckpointBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("rest_api source %s must be valid UTF-8", field)
	}
	if strings.ContainsRune(value, utf8.RuneError) {
		return fmt.Errorf("rest_api source %s must not contain the Unicode replacement character", field)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return fmt.Errorf("rest_api source %s must not contain control characters", field)
		}
	}
	return nil
}

// CredFingerprint pairs a credential kind with its stored content fingerprint
// (sha256 of the plaintext, source_credentials.fingerprint). Only fingerprints
// enter the binding hash — plaintext credentials never persist anywhere new.
type CredFingerprint struct {
	Kind        string `json:"kind"`
	Fingerprint string `json:"fingerprint"`
}

// IncrementalBinding is the complete canonical input set of a def's P2.5 data
// checkpoint binding: source identity, the effective AUTH identity/config,
// sync-relevant source config, and the def-level output target + file glob.
// A change to ANY of these makes the stored binding_fingerprint stale.
type IncrementalBinding struct {
	SourceID        uuid.UUID
	OutputDatasetID uuid.UUID
	FileGlob        *string // nil-preserving
	BaseURL         string
	ResourcePath    string
	RecordsPath     string
	PaginationType  string
	CursorParam     string
	NextCursorPath  string
	Incremental     RESTAPIIncrementalConfig
	QueryParams     map[string]string
	Headers         map[string]string
	Auth            RESTAPIAuthConfig
	Credentials     []CredFingerprint
}

// incrementalBindingCanonical is the deterministic marshal shape: struct
// field order is fixed, maps marshal with sorted keys (encoding/json), and
// presence-sensitive fields (FileGlob, InitialValue) are explicit pointers
// that always serialize (null vs value distinguishes absence).
type incrementalBindingCanonical struct {
	SourceID                string            `json:"source_id"`
	OutputDatasetID         string            `json:"output_dataset_id"`
	FileGlob                *string           `json:"file_glob"`
	BaseURL                 string            `json:"base_url"`
	ResourcePath            string            `json:"resource_path"`
	RecordsPath             string            `json:"records_path"`
	PaginationType          string            `json:"pagination_type"`
	CursorParam             string            `json:"cursor_param"`
	NextCursorPath          string            `json:"next_cursor_path"`
	IncrementalEnabled      bool              `json:"incremental_enabled"`
	RequestParam            string            `json:"request_param"`
	CompletedCheckpointPath string            `json:"completed_checkpoint_path"`
	InitialValue            *string           `json:"initial_value"`
	QueryParams             map[string]string `json:"query_params"`
	Headers                 map[string]string `json:"headers"`
	AuthType                string            `json:"auth_type"`
	AuthScheme              string            `json:"auth_scheme"`
	AuthSecretRef           string            `json:"auth_secret_ref"`
	AuthHeaderName          string            `json:"auth_header_name"`
	AuthQueryParam          string            `json:"auth_query_param"`
	AuthToken               string            `json:"auth_token"`
	AuthValue               string            `json:"auth_value"`
	AuthUsernameSecretRef   string            `json:"auth_username_secret_ref"`
	AuthPasswordSecretRef   string            `json:"auth_password_secret_ref"`
	Credentials             []CredFingerprint `json:"credentials"`
}

// SnapshotRecoveryBinding is the P2.6 frozen identity of a NON-incremental
// (snapshot) recoverable publish: the read configuration (base URL, resource
// path, records path, the COMPLETE pagination block), the effective auth
// config plus the fingerprints of every credential kind it references, and
// the def-level output target + file glob (root-decisions-v2.1). It is a
// SEPARATE canonical shape from IncrementalBinding — the P2.5 algorithm is
// untouched; snapshot v1 runs freeze/verify against THIS fingerprint only.
type SnapshotRecoveryBinding struct {
	SourceID        uuid.UUID
	OutputDatasetID uuid.UUID
	FileGlob        *string // nil-preserving
	BaseURL         string
	ResourcePath    string
	RecordsPath     string
	PaginationType  string
	PageParam       string
	PageSizeParam   string
	PageSize        int
	StartPage       int
	CursorParam     string
	NextCursorPath  string
	QueryParams     map[string]string
	Headers         map[string]string
	Auth            RESTAPIAuthConfig
	Credentials     []CredFingerprint
}

// snapshotRecoveryBindingCanonical is the deterministic marshal shape (fixed
// field order; maps marshal with sorted keys; nil maps become {}).
type snapshotRecoveryBindingCanonical struct {
	SourceID              string            `json:"source_id"`
	OutputDatasetID       string            `json:"output_dataset_id"`
	FileGlob              *string           `json:"file_glob"`
	BaseURL               string            `json:"base_url"`
	ResourcePath          string            `json:"resource_path"`
	RecordsPath           string            `json:"records_path"`
	PaginationType        string            `json:"pagination_type"`
	PageParam             string            `json:"page_param"`
	PageSizeParam         string            `json:"page_size_param"`
	PageSize              int               `json:"page_size"`
	StartPage             int               `json:"start_page"`
	CursorParam           string            `json:"cursor_param"`
	NextCursorPath        string            `json:"next_cursor_path"`
	QueryParams           map[string]string `json:"query_params"`
	Headers               map[string]string `json:"headers"`
	AuthType              string            `json:"auth_type"`
	AuthScheme            string            `json:"auth_scheme"`
	AuthSecretRef         string            `json:"auth_secret_ref"`
	AuthHeaderName        string            `json:"auth_header_name"`
	AuthQueryParam        string            `json:"auth_query_param"`
	AuthToken             string            `json:"auth_token"`
	AuthValue             string            `json:"auth_value"`
	AuthUsernameSecretRef string            `json:"auth_username_secret_ref"`
	AuthPasswordSecretRef string            `json:"auth_password_secret_ref"`
	Credentials           []CredFingerprint `json:"credentials"`
}

// SnapshotRecoveryBindingFingerprint returns the sha256 hex over the
// canonical snapshot binding JSON (Credentials sorted by kind; nil maps as
// {}). Callers under the Source/Def locks supply authoritative values.
func SnapshotRecoveryBindingFingerprint(b SnapshotRecoveryBinding) string {
	creds := append([]CredFingerprint(nil), b.Credentials...)
	sort.Slice(creds, func(i, j int) bool { return creds[i].Kind < creds[j].Kind })
	canonical := snapshotRecoveryBindingCanonical{
		SourceID:              b.SourceID.String(),
		OutputDatasetID:       b.OutputDatasetID.String(),
		FileGlob:              b.FileGlob,
		BaseURL:               b.BaseURL,
		ResourcePath:          b.ResourcePath,
		RecordsPath:           b.RecordsPath,
		PaginationType:        b.PaginationType,
		PageParam:             b.PageParam,
		PageSizeParam:         b.PageSizeParam,
		PageSize:              b.PageSize,
		StartPage:             b.StartPage,
		CursorParam:           b.CursorParam,
		NextCursorPath:        b.NextCursorPath,
		QueryParams:           b.QueryParams,
		Headers:               b.Headers,
		AuthType:              b.Auth.Type,
		AuthScheme:            b.Auth.Scheme,
		AuthSecretRef:         b.Auth.SecretRef,
		AuthHeaderName:        b.Auth.HeaderName,
		AuthQueryParam:        b.Auth.QueryParam,
		AuthToken:             b.Auth.Token,
		AuthValue:             b.Auth.Value,
		AuthUsernameSecretRef: b.Auth.UsernameSecretRef,
		AuthPasswordSecretRef: b.Auth.PasswordSecretRef,
		Credentials:           creds,
	}
	if canonical.QueryParams == nil {
		canonical.QueryParams = map[string]string{}
	}
	if canonical.Headers == nil {
		canonical.Headers = map[string]string{}
	}
	// Deterministic by construction (struct order + sorted maps). A marshal
	// failure can only be a programming error; hash the error marker so
	// callers never see an empty fingerprint silently (same discipline as
	// IncrementalBindingFingerprint).
	raw, err := json.Marshal(canonical)
	if err != nil {
		raw = []byte("marshal-error:" + err.Error())
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// IncrementalBindingFingerprint returns the sha256 hex over the canonical
// binding JSON. Sort Credentials by kind first; callers under locks supply
// the authoritative field values.
func IncrementalBindingFingerprint(b IncrementalBinding) string {
	creds := append([]CredFingerprint(nil), b.Credentials...)
	sort.Slice(creds, func(i, j int) bool { return creds[i].Kind < creds[j].Kind })
	canonical := incrementalBindingCanonical{
		SourceID:                b.SourceID.String(),
		OutputDatasetID:         b.OutputDatasetID.String(),
		FileGlob:                b.FileGlob,
		BaseURL:                 b.BaseURL,
		ResourcePath:            b.ResourcePath,
		RecordsPath:             b.RecordsPath,
		PaginationType:          b.PaginationType,
		CursorParam:             b.CursorParam,
		NextCursorPath:          b.NextCursorPath,
		IncrementalEnabled:      b.Incremental.Enabled,
		RequestParam:            b.Incremental.RequestParam,
		CompletedCheckpointPath: b.Incremental.CompletedCheckpointPath,
		InitialValue:            b.Incremental.InitialValue,
		QueryParams:             b.QueryParams,
		Headers:                 b.Headers,
		AuthType:                b.Auth.Type,
		AuthScheme:              b.Auth.Scheme,
		AuthSecretRef:           b.Auth.SecretRef,
		AuthHeaderName:          b.Auth.HeaderName,
		AuthQueryParam:          b.Auth.QueryParam,
		AuthToken:               b.Auth.Token,
		AuthValue:               b.Auth.Value,
		AuthUsernameSecretRef:   b.Auth.UsernameSecretRef,
		AuthPasswordSecretRef:   b.Auth.PasswordSecretRef,
		Credentials:             creds,
	}
	// Both maps may be nil; marshal them as {} for determinism.
	if canonical.QueryParams == nil {
		canonical.QueryParams = map[string]string{}
	}
	if canonical.Headers == nil {
		canonical.Headers = map[string]string{}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		// Canonical shape is marshal-safe by construction; a failure here can
		// only be a programming error. Hash the error marker so callers never
		// see an empty fingerprint silently.
		encoded = []byte("marshal-error:" + err.Error())
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:])
}
