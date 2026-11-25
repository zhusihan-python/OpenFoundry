package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// WP3.4 pinned-input snapshot reads. An input-shaped node may pin its read
// to an explicit dataset snapshot through config.dataset_snapshot
// {branch, version, transaction_id}. The dataset RID is NOT part of the
// snapshot object: it keeps coming from the node's existing dataset ID
// resolution semantics (dataset_rid, then dataset_id, then the first of
// input_dataset_ids, then the node-level metadata the executor attaches),
// and with a snapshot present that resolution must be unambiguous —
// disagreeing spellings are rejected instead of silently prioritized.
//
// The read itself reuses the batched dataset client's ScanStream over the
// unchanged DVS GET rows:scan route: the expected transaction binds the
// FIRST page and every later page (dataset/branch/version are verified the
// same way), the request carries the caller's bearer only (no service
// token fallback), and the materialized table stays bounded — the existing
// row cap plus a hard aggregate byte bound. Pinned reads NEVER truncate:
// exceeding a bound fails explicitly in preview and execution alike (an
// early preview exit could return rows that the page's post-stream
// binding validation never checked), while unpinned preview reads keep
// their legacy sampling behavior.
//
// An optional expected_schema_hash inside the pin adds a precondition:
// the committed schema of the pinned version must have that SHA-256
// content hash. It is proved against the dataset service's versioned
// schema route BEFORE the first scan page, under the caller's bearer and
// with the pinned (version, transaction) pair, and the answer must be
// bound to the dataset, branch, version, transaction and hash. A refused
// or unusable answer fails the read, so no row is scanned and nothing
// downstream is published from that input. Without the key nothing
// changes: no schema request is made.

// maxDatasetInputTotalBytes is the hard aggregate bound on the raw record
// bytes one snapshot read may materialize into the lightweight table
// runtime, in either mode. It mirrors the legacy rows-endpoint decode
// bound (64MiB): beyond it the read fails instead of truncating, so no
// partial output is ever produced from a pinned snapshot.
const maxDatasetInputTotalBytes = 64 << 20

// datasetInputSnapshot is the parsed, validated pin of one dataset_input
// node: branch, positive version and the transaction that cut it. All
// three are required — an incomplete pin is an error, never a latest read.
type datasetInputSnapshot struct {
	Branch        string
	Version       int32
	TransactionID uuid.UUID
	// ExpectedSchemaHash is the lowercase hexadecimal SHA-256 content hash
	// the pinned version's committed schema must have. It is "" only when
	// the pin declares no expected_schema_hash key at all: a declared key
	// is always valid or a parse error.
	ExpectedSchemaHash string
	FlattenJSON        bool
	Excel              json.RawMessage
	ExcelFiles         []excelSnapshotFile
}

// PayloadDeclaresDatasetSnapshot reports whether a logic_payload declares
// a dataset_snapshot pin anywhere along its config chain (top level or
// any nested "config" object — the same chain parseTableRuntimeConfig
// walks). Key matching mirrors the runtime exactly: the snapshot key
// matches the tableRuntimeConfig field binding (exact or case-folded,
// like encoding/json), while the "config" chain key is the envelope's
// exact map key. It is shared with the postgres PublishJobSpec reuse
// guard so the dedup layer cannot miss a pin shape the runtime would
// honor (or reject as shadowed) — including purely nested pins.
func PayloadDeclaresDatasetSnapshot(payload []byte) bool {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" || trimmed == "null" {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil || fields == nil {
		return false
	}
	for key, raw := range fields {
		// encoding/json binds struct fields case-insensitively when no
		// exact key matches; a "null" raw still reaches the runtime's
		// parser (which rejects it) — both count as declared.
		if strings.EqualFold(key, "dataset_snapshot") && strings.TrimSpace(string(raw)) != "" {
			return true
		}
	}
	if nested, ok := fields["config"]; ok && len(nested) > 0 {
		return PayloadDeclaresDatasetSnapshot(nested)
	}
	return false
}

// parseDatasetInputSnapshot decodes and validates the node's
// dataset_snapshot object. It returns (nil, nil) when the node declares no
// snapshot at all; every declared snapshot is either fully valid or a hard
// error.
func parseDatasetInputSnapshot(raw json.RawMessage) (*datasetInputSnapshot, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("dataset_snapshot_invalid: dataset_snapshot is not a JSON object: %w", err)
	}
	bad := func(format string, args ...any) error {
		return fmt.Errorf("dataset_snapshot_invalid: "+format, args...)
	}
	branchRaw, ok := fields["branch"]
	if !ok {
		return nil, bad("branch is required")
	}
	var branch string
	if err := json.Unmarshal(branchRaw, &branch); err != nil || strings.TrimSpace(branch) == "" {
		return nil, bad("branch must be a non-empty string")
	}
	versionRaw, ok := fields["version"]
	if !ok {
		return nil, bad("version is required")
	}
	var version int64
	if err := json.Unmarshal(versionRaw, &version); err != nil || version < 1 || version > 1<<31-1 {
		return nil, bad("version must be a positive integer within the version range")
	}
	txnRaw, ok := fields["transaction_id"]
	if !ok {
		return nil, bad("transaction_id is required")
	}
	var txn string
	if err := json.Unmarshal(txnRaw, &txn); err != nil {
		return nil, bad("transaction_id must be a string")
	}
	txnID, err := batchedReservationUUID(txn)
	if err != nil {
		return nil, bad("transaction_id %q is not a transaction RID", txn)
	}
	// Only an absent key skips the check. A present key must be the digest
	// (the #78 batched rule): null and "" are refused, never read as "off",
	// so a generated pin cannot silently drop its precondition.
	var expectedSchemaHash string
	if value, ok := fields["expected_schema_hash"]; ok {
		if json.Unmarshal(value, &expectedSchemaHash) != nil || !validSnapshotSchemaHash(expectedSchemaHash) {
			return nil, bad("expected_schema_hash must be a lowercase hexadecimal SHA-256 digest")
		}
	}
	var flatten bool
	if value, ok := fields["flatten_json"]; ok {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &flatten) != nil {
			return nil, bad("flatten_json must be a boolean")
		}
	}
	excel := bytes.TrimSpace(fields["excel"])
	if len(excel) > 0 && (excel[0] != '{' || flatten) {
		return nil, bad("excel must be an object and cannot be combined with flatten_json")
	}
	files, err := parseExcelSnapshotFiles(fields["excel_files"])
	if err != nil {
		return nil, bad("%v", err)
	}
	if len(files) != 0 && (len(excel) != 0 || flatten) {
		return nil, bad("excel_files cannot be combined with excel or flatten_json")
	}
	return &datasetInputSnapshot{Branch: strings.TrimSpace(branch), Version: int32(version), TransactionID: txnID, ExpectedSchemaHash: expectedSchemaHash, FlattenJSON: flatten, Excel: excel, ExcelFiles: files}, nil
}

// validSnapshotSchemaHash accepts exactly what the dataset service's
// versioned schema route accepts as expected_content_hash: a lowercase
// hexadecimal SHA-256 digest.
func validSnapshotSchemaHash(hash string) bool {
	if len(hash) != hex.EncodedLen(sha256.Size) || strings.ToLower(hash) != hash {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

// readDatasetSnapshotRows materializes the pinned snapshot through the
// batched dataset client's ScanStream. Bounded by construction: the row
// cap fails over-limit in BOTH modes (a pinned read never truncates, so no
// unverified sample can escape a page whose binding fields trail the rows)
// and the aggregate byte bound is hard (SnapshotMaxTotalBytes overrides
// the default bound for tests; zero keeps the production default). The
// client honors the reader's injected BaseURL/Client while keeping the
// ScanStream parameter defaults, redirect refusal and caller-only auth.
func (r DatasetServiceRowReader) readDatasetSnapshotRows(ctx context.Context, req DatasetRowsRequest) (DatasetRowsResult, error) {
	cfg := currentBatchedClientConfig()
	if base := strings.TrimSpace(r.BaseURL); base != "" {
		cfg.BaseURL = base
	}
	if r.Client != nil {
		cfg.Client = r.Client
	}
	normalized, err := cfg.withDefaults()
	if err != nil {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_source_not_configured: %w", err)
	}
	client := batchedDatasetClient{cfg: normalized}
	rid, err := normaliseDatasetRID(req.DatasetRID)
	if err != nil {
		return DatasetRowsResult{}, err
	}
	datasetUUID, err := batchedDatasetUUID(rid)
	if err != nil {
		return DatasetRowsResult{}, err
	}
	maxTotalBytes := int64(maxDatasetInputTotalBytes)
	if r.SnapshotMaxTotalBytes > 0 {
		maxTotalBytes = r.SnapshotMaxTotalBytes
	}
	limit := clampDatasetInputRows(req.Limit)
	expectedTxn := req.Snapshot.TransactionID
	if hash := req.Snapshot.ExpectedSchemaHash; hash != "" {
		// The precondition is proved before any read below asks for a page:
		// a refusal or an answer bound to another dataset, branch, version,
		// transaction or hash fails the node here, so nothing is scanned
		// and no output can be published from this input.
		verifiedTxn, err := client.VerifyInputSchema(ctx, batchedScanInput{
			DatasetRID:            rid,
			Branch:                req.Snapshot.Branch,
			Version:               req.Snapshot.Version,
			ExpectedTransactionID: &expectedTxn,
		}, hash)
		if err != nil {
			return DatasetRowsResult{}, fmt.Errorf("dataset_input_schema_rejected: %w", err)
		}
		expectedTxn = verifiedTxn
	}
	if len(req.Snapshot.ExcelFiles) > 0 {
		return client.readExcelSnapshotFiles(ctx, rid, datasetUUID, req, limit, maxTotalBytes)
	}
	if len(req.Snapshot.Excel) > 0 {
		return client.readExcelSnapshotRows(ctx, rid, datasetUUID, req, limit, maxTotalBytes)
	}
	rows := make([]map[string]json.RawMessage, 0, limit)
	var totalBytes int64
	scan, scanErr := client.ScanStream(ctx, batchedScanInput{
		schemaInference:       scanSchemaInference(req.Snapshot.FlattenJSON),
		flattenJSON:           req.Snapshot.FlattenJSON,
		DatasetRID:            rid,
		Branch:                req.Snapshot.Branch,
		Version:               req.Snapshot.Version,
		Path:                  req.Path,
		ExpectedTransactionID: &expectedTxn,
	}, func(record []byte) error {
		if totalBytes+int64(len(record)) > maxTotalBytes {
			return fmt.Errorf("dataset_input_bytes_exceeded: snapshot rows exceed the %d-byte materialization bound (%d bytes seen)", maxTotalBytes, totalBytes+int64(len(record)))
		}
		totalBytes += int64(len(record))
		if len(rows) >= limit {
			return fmt.Errorf("dataset_input_row_limit_exceeded: the pinned snapshot holds more than %d rows; narrow the pin (path/predicate) — pinned reads never truncate, in preview or execution", limit)
		}
		var row map[string]json.RawMessage
		if err := json.Unmarshal(record, &row); err != nil {
			return fmt.Errorf("dataset_input_snapshot_record_invalid: scan returned a non-object record: %w", err)
		}
		rows = append(rows, row)
		return nil
	})
	if scanErr != nil {
		return DatasetRowsResult{}, scanErr
	}
	return DatasetRowsResult{
		DatasetID:     datasetUUID.String(),
		Path:          req.Path,
		Format:        scan.Metadata.Format,
		SchemaVersion: scan.Metadata.SchemaVersion,
		Schema:        scan.Metadata.Schema,
		Source:        "dataset-versioning-scan",
		Columns:       scan.Metadata.Schema.columns(),
		Rows:          rows,
	}, nil
}

// canonicalDatasetRIDForComparison normalises any accepted dataset
// reference spelling (bare uuid or ri.foundry.main.dataset.<uuid>) to the
// bare uuid so ambiguity checks compare identities, not spellings. The
// zero UUID is rejected: a pinned read must name a real dataset.
func canonicalDatasetRIDForComparison(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	id, err := uuid.Parse(strings.TrimPrefix(trimmed, datasetRIDPrefix))
	if err != nil || id == uuid.Nil {
		return "", fmt.Errorf("dataset_input_invalid_reference: %q is not a dataset RID", raw)
	}
	return id.String(), nil
}
