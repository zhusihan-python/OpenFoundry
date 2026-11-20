package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// WP3.3 3B batched dataset client: direct dataset-versioning-service
// access for the opt-in batched execution path — paged rows:scan
// consumption, ONE streaming outputs:stage upload, and the exact-byte
// keyed outputs:commit (re)send.
//
// Invariants enforced here (see increment-3b-plan §1/§3 and the review
// corrections):
//   - Every request carries the CALLER's bearer from the context; the
//     client has no service-token field at all and fails closed without
//     a caller. Redirects are always refused — the DVS base URL must be
//     the direct service address (never a buffering gateway).
//   - Scan pages are decoded row by row; the page envelope fields are
//     validated BOTH before streaming (when the service order provides
//     them first) and after (any order): a mismatch after rows have been
//     streamed surfaces as an error so the caller closes the stage
//     writer and nothing is sealed or published.
//   - The first page's transaction UUID pins the scan; every later page
//     must report the same transaction, dataset, branch and version.
//   - The stage upload runs under a global per-attempt timeout (default
//     30s) with a bounded buffered writer over an io.Pipe — memory stays
//     at (page window + write buffer) regardless of dataset size, and a
//     failed feed closes the pipe so the request body is truncated and
//     the dataset service discards its spool without sealing.
//   - A staged record INCLUDING its newline must stay within the scan
//     line cap (1MiB) so anything stage seals is exactly what rows:scan
//     can read back.

const (
	batchedScanCursorMaxLen    = 1024
	batchedStageWriteBuffer    = 256 << 10
	batchedStageResponseLimit  = 1 << 20
	batchedErrorBodyLimit      = 64 << 10
	batchedDefaultScanPageRows = 500
	// 8MiB raw page budget on the service side; JSON envelope overhead
	// plus per-row escaping can inflate the wire form, so the hard read
	// bound sits well above it while staying decodable incrementally.
	batchedDefaultScanMaxResponseBytes = 64 << 20
	// Loop guard against multi-value cursor cycling: above 10x the WP3.3
	// acceptance volume (15k rows / 500-row pages = 30 pages) and equal
	// to the dataset service's own 5M-record ceiling divided by the page
	// size — the real bound, not just a test constant.
	batchedDefaultScanMaxPages = 10000
	// datasetFileRowsMaxLineBytes parity (scan read side): a record plus
	// its newline may not exceed 1MiB.
	batchedMaxRecordBytes = 1 << 20
	// Global stage attempt timeout (review correction): the upload goes
	// direct to DVS; it must not hang past this bound.
	batchedDefaultStageTimeout = 30 * time.Second
)

// BatchedClientConfig is the deployment-tunable surface of the batched
// dataset client. Wired from env in cmd; tests override via
// SetBatchedDatasetClient.
type BatchedClientConfig struct {
	BaseURL string
	Client  *http.Client
	// ScanPageRows is the per-page rows:scan limit (default 500).
	ScanPageRows int
	// ScanMaxResponseBytes is the hard per-page HTTP body bound
	// (default 64MiB — a wire bound, NOT a memory target: rows stream
	// through the decoder one at a time).
	ScanMaxResponseBytes int64
	// ScanMaxPages bounds the pagination loop (default 10000).
	ScanMaxPages int
	// StageMaxRecordBytes bounds one staged record including its newline
	// (default 1MiB, rows:scan line-cap parity).
	StageMaxRecordBytes int64
	// StageTimeout is the global per-attempt stage upload timeout
	// (default 30s).
	StageTimeout time.Duration
	// LeaseTTL is the execute-lease fence duration (default 30m).
	LeaseTTL time.Duration
}

var batchedClientSlot atomic.Value // stores *BatchedClientConfig

// SetBatchedDatasetClient installs the batched client configuration and
// returns a restore function (test isolation, mirrors the ports setters).
func SetBatchedDatasetClient(cfg BatchedClientConfig) func() {
	previous, _ := batchedClientSlot.Load().(*BatchedClientConfig)
	batchedClientSlot.Store(&cfg)
	return func() { batchedClientSlot.Store(previous) }
}

func currentBatchedClientConfig() BatchedClientConfig {
	if cfg, ok := batchedClientSlot.Load().(*BatchedClientConfig); ok && cfg != nil {
		return *cfg
	}
	return BatchedClientConfig{}
}

func (c BatchedClientConfig) withDefaults() (BatchedClientConfig, error) {
	if strings.TrimSpace(c.BaseURL) == "" {
		return c, errors.New("batched_client_not_configured: DATASET_SERVICE_URL is not set for the batched execution path")
	}
	if c.ScanPageRows <= 0 {
		c.ScanPageRows = batchedDefaultScanPageRows
	}
	if c.ScanMaxResponseBytes <= 0 {
		c.ScanMaxResponseBytes = batchedDefaultScanMaxResponseBytes
	}
	if c.ScanMaxPages <= 0 {
		c.ScanMaxPages = batchedDefaultScanMaxPages
	}
	if c.StageMaxRecordBytes <= 0 {
		c.StageMaxRecordBytes = batchedMaxRecordBytes
	}
	if c.StageTimeout <= 0 {
		c.StageTimeout = batchedDefaultStageTimeout
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = batchedDefaultLeaseTTL
	}
	return c, nil
}

// batchedDatasetClient issues the three DVS calls of a batched run.
type batchedDatasetClient struct {
	cfg BatchedClientConfig
}

func newBatchedDatasetClient() (batchedDatasetClient, error) {
	cfg, err := currentBatchedClientConfig().withDefaults()
	if err != nil {
		return batchedDatasetClient{}, err
	}
	return batchedDatasetClient{cfg: cfg}, nil
}

// httpClient clones the injected client (or a default) with redirects
// refused — the caller JWT must never travel anywhere else.
func (c batchedDatasetClient) httpClient() *http.Client {
	base := c.cfg.Client
	if base == nil {
		base = &http.Client{Timeout: 60 * time.Second}
	}
	clone := *base
	clone.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &clone
}

func (c batchedDatasetClient) newRequest(ctx context.Context, method, rawURL string, body io.Reader, contentType string) (*http.Request, error) {
	bearer := callerBearerFromContext(ctx)
	if bearer == "" {
		return nil, ErrDatasetCallerRequired
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

func (c batchedDatasetClient) baseURL() string {
	return strings.TrimRight(c.cfg.BaseURL, "/")
}

// batchedDatasetUUID extracts the bare UUID from a canonical dataset RID
// (or accepts a bare UUID). Scan/commit responses echo bare dataset UUIDs.
func batchedDatasetUUID(datasetRID string) (uuid.UUID, error) {
	return uuid.Parse(strings.TrimPrefix(strings.TrimSpace(datasetRID), datasetRIDPrefix))
}

// batchedScanInput is the frozen input binding for one scan.
type batchedScanInput struct {
	schemaInference *datasetSchemaInference
	flattenJSON     bool
	DatasetRID      string // canonical ri.foundry.main.dataset.<uuid>
	Branch          string
	Version         int32
	Path            string // "" = all_files
	Predicate       string // "" = none
	// ExpectedTransactionID, when set, pins the scan BEFORE the first
	// page: page one must report exactly this transaction (as must every
	// later page). WP3.4 snapshot reads use it so the served version can
	// never be cut by a different transaction than the pinned one.
	ExpectedTransactionID *uuid.UUID
}

// batchedScanStats reports what a completed scan consumed.
type batchedScanStats struct {
	TransactionID uuid.UUID
	Pages         int
	Rows          int64
	PageBytes     int64
	Metadata      batchedScanMetadata
}

// batchedClientError carries the DVS status and machine code so the
// orchestration can classify retryable vs permanent failures.
type batchedClientError struct {
	Status int
	Code   string
	Detail string
}

func (e *batchedClientError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("batched_dataset_%d_%s: %s", e.Status, e.Code, e.Detail)
	}
	return fmt.Sprintf("batched_dataset_%d: %s", e.Status, e.Detail)
}

func batchedErrorFromResponse(status int, body []byte) *batchedClientError {
	// The dataset service error envelope is {"error": <description>,
	// "code": <machine code>} (models.ErrorEnvelope). Older/plain errors
	// may omit "code" — the description fields stay the fallbacks.
	parsed := map[string]any{}
	_ = json.Unmarshal(body, &parsed)
	code, _ := parsed["code"].(string)
	detail, _ := parsed["error"].(string)
	if detail == "" {
		detail, _ = parsed["detail"].(string)
	}
	if detail == "" {
		detail, _ = parsed["message"].(string)
	}
	return &batchedClientError{Status: status, Code: strings.TrimSpace(code), Detail: strings.TrimSpace(detail)}
}

// ScanStream pages through rows:scan for the pinned input, handing every
// record to emit exactly once and retaining nothing. emit receives the
// record bytes as returned by the service (a trimmed JSON object); the
// caller appends the newline and maintains the running digest.
func (c batchedDatasetClient) ScanStream(ctx context.Context, in batchedScanInput, emit func(record []byte) error) (batchedScanStats, error) {
	stats := batchedScanStats{}
	expectedDataset, err := batchedDatasetUUID(in.DatasetRID)
	if err != nil {
		return stats, fmt.Errorf("batched_scan_input_invalid: %w", err)
	}
	var pinnedTxn uuid.UUID
	if in.ExpectedTransactionID != nil {
		// An explicitly expected transaction pins the scan from page one:
		// validateScanBinding then compares EVERY page (the first
		// included) against it instead of adopting whatever the service
		// reports first.
		pinnedTxn = *in.ExpectedTransactionID
	}
	cursor := ""
	for page := 0; ; page++ {
		if page >= c.cfg.ScanMaxPages {
			return stats, fmt.Errorf("batched_scan_page_limit: exceeded %d pages without completion", c.cfg.ScanMaxPages)
		}
		stats.Pages++
		query := url.Values{}
		query.Set("branch", in.Branch)
		query.Set("version", strconv.FormatInt(int64(in.Version), 10))
		if in.Path == "" {
			query.Set("all_files", "true")
		} else {
			query.Set("path", in.Path)
		}
		if in.Predicate != "" {
			query.Set("predicate", in.Predicate)
		}
		if cursor != "" {
			if len(cursor) > batchedScanCursorMaxLen {
				return stats, errors.New("batched_scan_cursor_invalid: cursor exceeds the length bound")
			}
			query.Set("cursor", cursor)
		}
		query.Set("limit", strconv.Itoa(c.cfg.ScanPageRows))
		scanURL := c.baseURL() + "/v1/datasets/" + url.PathEscape(in.DatasetRID) + "/files/rows:scan?" + query.Encode()
		req, err := c.newRequest(ctx, http.MethodGet, scanURL, nil, "")
		if err != nil {
			return stats, err
		}
		resp, err := c.httpClient().Do(req)
		if err != nil {
			return stats, fmt.Errorf("batched_scan_failed: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, batchedErrorBodyLimit))
			_ = resp.Body.Close()
			return stats, batchedErrorFromResponse(resp.StatusCode, body)
		}
		pageStats, pageCursor, scanErr := c.decodeScanPage(resp.Body, expectedDataset, in, &pinnedTxn, &stats.Metadata, emit)
		_ = resp.Body.Close()
		if scanErr != nil {
			return stats, scanErr
		}
		stats.Rows += pageStats
		if pageCursor == nil {
			stats.TransactionID = pinnedTxn
			if in.schemaInference != nil && (stats.Metadata.Format == "csv" || in.flattenJSON) {
				stats.Metadata.Schema = in.schemaInference.schema()
			}
			return stats, nil
		}
		next := *pageCursor
		if next == cursor {
			return stats, errors.New("batched_scan_cursor_stalled: the service repeated the previous cursor")
		}
		cursor = next
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
	}
}

// decodeScanPage streams one scan response: envelope fields are captured
// as they appear; rows are handed to emit one at a time. Binding fields
// that appear before the rows are validated BEFORE any record streams
// (fail fast); every field is re-validated after the object ends, so a
// hostile/buggy order that only reveals a mismatch after rows still
// fails the page — the caller's writer is closed with the error and
// nothing seals or publishes.
func (c batchedDatasetClient) decodeScanPage(body io.Reader, expectedDataset uuid.UUID, in batchedScanInput, pinnedTxn *uuid.UUID, pinnedMetadata *batchedScanMetadata, emit func(record []byte) error) (rowsEmitted int64, nextCursor *string, err error) {
	// Count every byte the decoder pulls (its internal buffer may hold
	// bytes past the parsed value, so a later probe on the reader would
	// under-count): padding or junk past the hard bound is detected by
	// the total, whatever the decoder tolerated.
	counting := &countingReader{r: body}
	dec := json.NewDecoder(counting)
	open, err := dec.Token()
	if err != nil {
		return 0, nil, fmt.Errorf("batched_scan_decode: %w", err)
	}
	if delim, ok := open.(json.Delim); !ok || delim != '{' {
		return 0, nil, errors.New("batched_scan_decode: response is not a JSON object")
	}
	var (
		sawRows     bool
		pageRows    int64
		envDataset  string
		envBranch   string
		envTxn      string
		envVersion  int64 = -1
		envRowCount int64 = -1
		envNext     *string
		preRowCheck func() error
		metadata    batchedScanMetadata
		seenFormat  bool
		seenVersion bool
		seenSchema  bool
		parquetErr  error
		schemaErr   error
		csvColumns  json.RawMessage
	)
	preRowCheck = func() error {
		if envDataset == "" || envBranch == "" || envTxn == "" || envVersion < 0 {
			return nil // not all binding fields seen yet — post-validate
		}
		return validateScanBinding(expectedDataset, in, pinnedTxn, envDataset, envBranch, envTxn, envVersion)
	}
	for dec.More() {
		keyTok, terr := dec.Token()
		if terr != nil {
			return 0, nil, fmt.Errorf("batched_scan_decode: %w", terr)
		}
		key, _ := keyTok.(string)
		switch key {
		case "rows":
			if sawRows {
				return 0, nil, errors.New("batched_scan_decode: duplicate rows array")
			}
			sawRows = true
			if verr := preRowCheck(); verr != nil {
				return 0, nil, verr
			}
			arr, aerr := dec.Token()
			if aerr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", aerr)
			}
			if delim, ok := arr.(json.Delim); !ok || delim != '[' {
				return 0, nil, errors.New("batched_scan_decode: rows is not an array")
			}
			for dec.More() {
				var record json.RawMessage
				if derr := dec.Decode(&record); derr != nil {
					return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
				}
				// JSON object field order is immaterial. If the page's format
				// trails its rows, retain the first provisional Parquet row
				// validation error and apply it once the format is known.
				if !seenFormat || metadata.Format == batchedParquetFormat {
					if parquetErr == nil {
						if _, err := decodeParquetOrder(record); err != nil {
							parquetErr = fmt.Errorf("batched_scan_record_invalid: %s row %d: %w", ordersSchemaVersion, pageRows+1, err)
						}
					}
					if seenFormat && parquetErr != nil {
						return 0, nil, parquetErr
					}
				}
				if in.flattenJSON {
					if seenFormat && metadata.Format != "" && metadata.Format != "ndjson" {
						return 0, nil, errJSONFlattenInputFormat
					}
					flat, err := flattenJSONRecord(record)
					if err != nil {
						return 0, nil, err
					}
					record = flat
				}
				if in.schemaInference != nil && (in.flattenJSON || !seenFormat || metadata.Format == "csv") && schemaErr == nil {
					schemaErr = in.schemaInference.addRow(record)
				}
				if eerr := emit(record); eerr != nil {
					return 0, nil, eerr
				}
				pageRows++
			}
			closeArr, cerr := dec.Token()
			if cerr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", cerr)
			}
			if delim, ok := closeArr.(json.Delim); !ok || delim != ']' {
				return 0, nil, errors.New("batched_scan_decode: rows array not closed")
			}
		case "next_cursor":
			if derr := dec.Decode(&envNext); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
		case "row_count":
			if derr := dec.Decode(&envRowCount); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
		case "format":
			if seenFormat {
				return 0, nil, errors.New("batched_scan_decode: duplicate format field")
			}
			seenFormat = true
			if derr := dec.Decode(&metadata.Format); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: format: %w", derr)
			}
		case "columns":
			if derr := dec.Decode(&csvColumns); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: columns: %w", derr)
			}
		case "schema_version":
			if seenVersion {
				return 0, nil, errors.New("batched_scan_decode: duplicate schema_version field")
			}
			seenVersion = true
			if derr := dec.Decode(&metadata.SchemaVersion); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: schema_version: %w", derr)
			}
		case "schema":
			if seenSchema {
				return 0, nil, errors.New("batched_scan_decode: duplicate schema field")
			}
			seenSchema = true
			if derr := dec.Decode(&metadata.Schema); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: schema: %w", derr)
			}
		case "dataset_id":
			if derr := dec.Decode(&envDataset); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
			if verr := preRowCheck(); verr != nil {
				return 0, nil, verr
			}
		case "branch":
			if derr := dec.Decode(&envBranch); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
			if verr := preRowCheck(); verr != nil {
				return 0, nil, verr
			}
		case "transaction_id":
			if derr := dec.Decode(&envTxn); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
			if verr := preRowCheck(); verr != nil {
				return 0, nil, verr
			}
		case "version":
			if derr := dec.Decode(&envVersion); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
			if verr := preRowCheck(); verr != nil {
				return 0, nil, verr
			}
		default:
			var skip json.RawMessage
			if derr := dec.Decode(&skip); derr != nil {
				return 0, nil, fmt.Errorf("batched_scan_decode: %w", derr)
			}
		}
	}
	if _, terr := dec.Token(); terr != nil { // closing '}'
		return 0, nil, fmt.Errorf("batched_scan_decode: %w", terr)
	}
	if _, terr := dec.Token(); !errors.Is(terr, io.EOF) {
		// Anything but a clean EOF (a second JSON value, a syntax error,
		// truncated tail) is malformed.
		return 0, nil, errors.New("batched_scan_decode: trailing content after the page object")
	}
	if counting.total() > c.cfg.ScanMaxResponseBytes {
		return 0, nil, fmt.Errorf("batched_scan_response_too_large: page body exceeds the %d-byte bound", c.cfg.ScanMaxResponseBytes)
	}
	if !sawRows {
		return 0, nil, errors.New("batched_scan_decode: page object missing rows")
	}
	if verr := validateScanBinding(expectedDataset, in, pinnedTxn, envDataset, envBranch, envTxn, envVersion); verr != nil {
		return 0, nil, verr
	}
	if err := bindScanMetadata(in.Path, pinnedMetadata, metadata); err != nil {
		return 0, nil, err
	}
	if metadata.Format == batchedParquetFormat && parquetErr != nil {
		return 0, nil, parquetErr
	}
	if in.flattenJSON && metadata.Format != "" && metadata.Format != "ndjson" {
		return 0, nil, errJSONFlattenInputFormat
	}
	if in.schemaInference != nil && (in.flattenJSON || metadata.Format == "csv") {
		if schemaErr != nil {
			return 0, nil, schemaErr
		}
		if !in.flattenJSON {
			if err := in.schemaInference.addHeaders(csvColumns); err != nil {
				return 0, nil, err
			}
		}
	}
	if envRowCount >= 0 && envRowCount != pageRows {
		return 0, nil, fmt.Errorf("batched_scan_row_count_mismatch: page reports %d rows but %d were streamed", envRowCount, pageRows)
	}
	if envNext != nil && len(*envNext) > batchedScanCursorMaxLen {
		return 0, nil, errors.New("batched_scan_cursor_invalid: cursor exceeds the length bound")
	}
	return pageRows, envNext, nil
}

func validateScanBinding(expectedDataset uuid.UUID, in batchedScanInput, pinnedTxn *uuid.UUID, envDataset, envBranch, envTxn string, envVersion int64) error {
	gotDataset, err := uuid.Parse(envDataset)
	if err != nil {
		return fmt.Errorf("batched_scan_binding_invalid: dataset_id %q is not a UUID", envDataset)
	}
	if gotDataset != expectedDataset {
		return fmt.Errorf("batched_scan_binding_invalid: page belongs to dataset %s, expected %s", envDataset, expectedDataset)
	}
	if envBranch != in.Branch {
		return fmt.Errorf("batched_scan_binding_invalid: page branch %q != requested %q", envBranch, in.Branch)
	}
	if envVersion != int64(in.Version) {
		return fmt.Errorf("batched_scan_binding_invalid: page version %d != requested %d", envVersion, in.Version)
	}
	gotTxn, err := uuid.Parse(envTxn)
	if err != nil || gotTxn == uuid.Nil {
		return fmt.Errorf("batched_scan_binding_invalid: transaction_id %q is not a non-zero UUID", envTxn)
	}
	if *pinnedTxn == uuid.Nil {
		*pinnedTxn = gotTxn
		return nil
	}
	if *pinnedTxn != gotTxn {
		return fmt.Errorf("batched_scan_binding_invalid: page transaction %s != pinned %s", envTxn, pinnedTxn)
	}
	return nil
}

// errBatchedStageTeardown marks a feed error CAUSED by the HTTP attempt
// settling early (an early rejection): the service answer, not the feed
// teardown, is the primary error for classification.
var errBatchedStageTeardown = errors.New("batched_stage_ended_early")

// countingReader counts every byte read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) total() int64 { return c.n }

// batchedStageRequest is one outputs:stage upload. ReservationUUID is the
// bare UUID wire form (the reservation RID's UUID tail).
type batchedStageRequest struct {
	DatasetRID     string // canonical RID
	Branch         string
	ReservationRID string // canonical txn RID (ri.foundry.main.transaction.<uuid>)
	BuildID        uuid.UUID
	LogicalPath    string
	IdempotencyKey string
	Format         string
}

// batchedStageResponse mirrors the 3A stage receipt (dataset_rid form —
// the receipt carries no dataset_id).
type batchedStageResponse struct {
	StagedObjectID string    `json:"staged_object_id"`
	DatasetRID     string    `json:"dataset_rid"`
	Branch         string    `json:"branch"`
	ReservationID  string    `json:"reservation_id"`
	BuildID        string    `json:"build_id"`
	LogicalPath    string    `json:"logical_path"`
	IdempotencyKey string    `json:"idempotency_key"`
	SizeBytes      int64     `json:"size_bytes"`
	SHA256         string    `json:"sha256"`
	RecordCount    int64     `json:"record_count"`
	ExpiresAt      time.Time `json:"expires_at"`
	Format         string    `json:"format,omitempty"`
}

// StageStream performs one streaming stage upload. Ownership is strictly
// single-receiver (a dual-receiver handoff here deadlocked real 3C fault
// runs: the HTTP result was consumed by a watch goroutine and the main
// thread waited on the same channel forever):
//
//   - the PRODUCER goroutine is the only writer of the request body: it
//     runs feed under a separately cancelable producer context, flushes
//     and closes the pipe, and reports exactly one error on the buffered
//     feedDone channel — the main thread is its only receiver;
//   - the MAIN thread is the only receiver of the HTTP result: it runs
//     Do synchronously, and once it settles it deterministically tears
//     the body down (pipe close-with-error, producer cancel) and waits
//     for the producer's single result;
//   - the response body is read and closed IN PLACE while the stage
//     context is still alive — the deferred HTTP-context cancel must
//     never precede a body this classification needs.
//
// Classification keeps the established order: a GENUINE producer failure
// is primary (cancellation stays errors.Is-able); a producer torn down by
// the HTTP attempt settling early (errBatchedStageTeardown) defers to the
// service answer, machine code preserved.
func (c batchedDatasetClient) StageStream(ctx context.Context, req batchedStageRequest, feed func(ctx context.Context, w *bufio.Writer) error) (batchedStageResponse, error) {
	reservationUUID, err := batchedReservationUUID(req.ReservationRID)
	if err != nil {
		return batchedStageResponse{}, err
	}
	query := url.Values{}
	query.Set("branch", req.Branch)
	query.Set("reservation_id", reservationUUID.String())
	query.Set("build_id", req.BuildID.String())
	query.Set("logical_path", req.LogicalPath)
	query.Set("idempotency_key", req.IdempotencyKey)
	contentType := "application/x-ndjson"
	if req.Format == batchedParquetFormat {
		query.Set("format", req.Format)
		contentType = batchedParquetContentType
	}
	stageURL := c.baseURL() + "/v1/datasets/" + url.PathEscape(req.DatasetRID) + "/outputs:stage?" + query.Encode()

	stageCtx, cancel := context.WithTimeout(ctx, c.cfg.StageTimeout)
	defer cancel()
	// Cancel-CAUSE carries the distinction between OUR teardown and a real
	// caller cancellation: a producer parked in a scan GET unwinds with
	// context.Canceled in both cases, and only the cause separates
	// "answer the service result" from "surface the caller cancel".
	producerCtx, stopProducer := context.WithCancelCause(stageCtx)
	defer stopProducer(errBatchedStageTeardown)
	pipeReader, pipeWriter := io.Pipe()
	httpReq, err := c.newRequest(stageCtx, http.MethodPost, stageURL, pipeReader, contentType)
	if err != nil {
		_ = pipeReader.Close()
		_ = pipeWriter.Close()
		stopProducer(errBatchedStageTeardown)
		return batchedStageResponse{}, err
	}

	// Producer: single sender, single (main) receiver.
	feedDone := make(chan error, 1)
	go func() {
		writer := bufio.NewWriterSize(pipeWriter, batchedStageWriteBuffer)
		feedErr := feed(producerCtx, writer)
		if feedErr == nil {
			// No Flush on failure: buffered-but-unwritten bytes die with
			// the pipe close below.
			if flushErr := writer.Flush(); flushErr != nil {
				feedErr = flushErr
			}
		}
		if feedErr != nil {
			// The truncated request body makes the service discard its
			// spool; nothing seals.
			_ = pipeWriter.CloseWithError(feedErr)
		} else {
			_ = pipeWriter.Close()
		}
		feedDone <- feedErr
	}()

	// Main thread: the ONLY receiver of the HTTP result.
	resp, doErr := c.httpClient().Do(httpReq)

	// The attempt has settled: deterministically tear the body down so a
	// still-streaming producer unwinds instead of feeding a dead upload.
	// (A producer that already closed the pipe makes these no-ops.)
	if doErr != nil {
		_ = pipeWriter.CloseWithError(fmt.Errorf("batched_stage_request_failed: %w", doErr))
	} else {
		_ = pipeWriter.CloseWithError(fmt.Errorf("%w: the service answered before the upload completed", errBatchedStageTeardown))
	}
	stopProducer(errBatchedStageTeardown)
	feedErr := <-feedDone
	_ = pipeReader.Close()
	if feedErr != nil && errors.Is(feedErr, context.Canceled) && errors.Is(context.Cause(producerCtx), errBatchedStageTeardown) {
		// The producer was parked on OUR teardown (e.g. blocked in a scan
		// GET), not on a caller cancel: its context.Canceled carries no
		// verdict of its own — classify by the request result below. A
		// genuine caller cancel keeps its cause (and surfaces through
		// doErr / feedErr as context.Canceled).
		feedErr = nil
	}

	// Read (and close) the response body IN PLACE — the stage context is
	// canceled only by the deferred cancel after this classification.
	var status int
	var body []byte
	if doErr == nil {
		status = resp.StatusCode
		body, _ = io.ReadAll(io.LimitReader(resp.Body, batchedStageResponseLimit))
		_ = resp.Body.Close()
	}

	// Classification order:
	//   1. A GENUINE producer failure truncated the upload → the producer
	//      error is primary (cancellation semantics stay errors.Is-able).
	//   2. The producer was torn down BY the attempt settling early
	//      (errBatchedStageTeardown) or finished cleanly → the service
	//      answer is primary, with its machine code preserved.
	if feedErr != nil && !errors.Is(feedErr, errBatchedStageTeardown) {
		if doErr != nil {
			return batchedStageResponse{}, fmt.Errorf("batched_stage_failed: feed: %w (request: %w)", feedErr, doErr)
		}
		return batchedStageResponse{}, fmt.Errorf("batched_stage_failed: feed: %w (service answered %d: %s)", feedErr, status, strings.TrimSpace(string(body)))
	}
	if doErr != nil {
		return batchedStageResponse{}, fmt.Errorf("batched_stage_failed: %w", doErr)
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return batchedStageResponse{}, batchedErrorFromResponse(status, body)
	}
	if feedErr != nil {
		// The service acknowledged a torn-down upload — a contradiction
		// that must never surface as success.
		return batchedStageResponse{}, fmt.Errorf("batched_stage_failed: service acknowledged a truncated upload (%w)", feedErr)
	}
	var receipt batchedStageResponse
	if err := json.Unmarshal(body, &receipt); err != nil || receipt.StagedObjectID == "" {
		return batchedStageResponse{}, errors.New("batched_stage_bad_response: stage receipt missing staged_object_id")
	}
	if receipt.Format != req.Format {
		return batchedStageResponse{}, fmt.Errorf("batched_stage_receipt_mismatch: receipt format %q != requested format %q", receipt.Format, req.Format)
	}
	return receipt, nil
}

// batchedReservationUUID strips the canonical transaction RID prefix to
// the bare UUID the stage/commit wire expects.
func batchedReservationUUID(reservationRID string) (uuid.UUID, error) {
	tail := strings.TrimPrefix(strings.TrimSpace(reservationRID), "ri.foundry.main.transaction.")
	id, err := uuid.Parse(tail)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("batched_reservation_invalid: %q is not a transaction RID", reservationRID)
	}
	return id, nil
}

// CommitExact sends the PERSISTED commit request bytes verbatim to the
// keyed outputs:commit route and returns the raw status plus the bounded
// response body. Classification (201 first-commit / 200 receipt replay /
// OUTCOME_UNKNOWN / conflicts) is the orchestration's business.
func (c batchedDatasetClient) CommitExact(ctx context.Context, datasetRID string, body []byte) (int, []byte, error) {
	commitURL := c.baseURL() + "/v1/datasets/" + url.PathEscape(datasetRID) + "/outputs:commit"
	req, err := c.newRequest(ctx, http.MethodPost, commitURL, bytes.NewReader(body), "application/json")
	if err != nil {
		return 0, nil, err
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("batched_commit_failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, batchedStageResponseLimit))
	return resp.StatusCode, payload, nil
}
