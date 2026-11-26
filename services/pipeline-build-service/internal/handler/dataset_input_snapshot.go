package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

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
//
// An optional expected_schema inside the pin holds the fields that hash
// covers, as the dataset service lists them. A refused proof then reads the
// committed schema of the pinned version once more, without the hash so the
// route answers the schema instead of comparing it, and names the field
// differences and a changed field order. When that read fails, the refusal
// is kept and says so.

// maxDatasetInputTotalBytes is the hard aggregate bound on the raw record
// bytes one snapshot read may materialize into the lightweight table
// runtime, in either mode. It mirrors the legacy rows-endpoint decode
// bound (64MiB): beyond it the read fails instead of truncating, so no
// partial output is ever produced from a pinned snapshot.
const maxDatasetInputTotalBytes = 64 << 20

// A refusal names at most maxSchemaDifferenceClauses field differences, then
// a changed field order. One clause quotes its field name cut to
// maxSchemaFieldNameBytes and its type label cut to maxSchemaFieldTypeBytes,
// and a diagnostic read that fails is quoted cut to maxSchemaReasonBytes,
// each on a rune boundary so none can flood the node error.
const (
	maxSchemaDifferenceClauses = 8
	maxSchemaFieldNameBytes    = 128
	maxSchemaFieldTypeBytes    = 64
	maxSchemaReasonBytes       = 200
)

// snapshotSchemaFieldTypeShape is the shape of a type label as the dataset
// service's versioned schema route states it: an upper-case name, with
// parameters in brackets or without, such as DOUBLE, BINARY(16) or
// TIMESTAMP(MILLIS,UTC).
var snapshotSchemaFieldTypeShape = regexp.MustCompile(`^[A-Z][A-Z0-9_]*(\([A-Z0-9,_]+\))?$`)

// snapshotSchemaField is one field of a schema as the dataset service's
// versioned schema route lists it. The keys and their order are the route's
// own, so the plain json.Marshal of a field list hashes to its content_hash.
type snapshotSchemaField struct {
	Name      string `json:"name"`
	FieldType string `json:"field_type"`
	Nullable  bool   `json:"nullable"`
}

// snapshotSchemaElement is one element of expected_schema as the pin writes
// it. Into a bool a missing nullable would read as false, so the literal is
// kept raw until it is proved to be true or false.
type snapshotSchemaElement struct {
	Name      string          `json:"name"`
	FieldType string          `json:"field_type"`
	Nullable  json.RawMessage `json:"nullable"`
}

// snapshotSchemaNullable reads the nullability an element states. Only the
// literals true and false state it: a missing key, null or a value of
// another JSON type is never read as false.
func snapshotSchemaNullable(raw json.RawMessage) (nullable, stated bool) {
	switch string(bytes.TrimSpace(raw)) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

// snapshotSchemaDigest is the content hash of a field list: the lowercase
// hexadecimal SHA-256 over the plain json.Marshal of the list, in its order.
// No encoder of this package takes part.
func snapshotSchemaDigest(fields []snapshotSchemaField) (string, error) {
	encoded, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

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
	// ExpectedSchema is the field list that hash covers, read from the
	// pin's expected_schema. It is nil unless the pin declares both keys:
	// an expected_schema without a valid hash is a parse error.
	ExpectedSchema []snapshotSchemaField
	FlattenJSON    bool
	Excel          json.RawMessage
	ExcelFiles     []excelSnapshotFile
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
	var expectedSchema []snapshotSchemaField
	if value, ok := fields["expected_schema"]; ok {
		// The fields must hash to the declared digest, so the hash is
		// what vouches for them: without it the key is refused, never
		// read as a list to trust on its own.
		if expectedSchemaHash == "" {
			return nil, bad("expected_schema requires expected_schema_hash")
		}
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.DisallowUnknownFields()
		var elements []snapshotSchemaElement
		if err := decoder.Decode(&elements); err != nil {
			// The decoder's text can name an unknown key of the pin, so it is
			// cut like the text of any other answer.
			return nil, bad("expected_schema must be a JSON array of objects with the keys name, field_type and nullable: %s",
				cutRunes(err.Error(), maxSchemaReasonBytes))
		}
		if len(elements) == 0 {
			return nil, bad("expected_schema must be a non-empty JSON array")
		}
		expectedSchema = make([]snapshotSchemaField, 0, len(elements))
		names := make(map[string]struct{}, len(elements))
		for i, element := range elements {
			nullable, stated := snapshotSchemaNullable(element.Nullable)
			if !stated {
				return nil, bad("expected_schema field %d must state nullable as true or false", i+1)
			}
			if element.Name == "" {
				return nil, bad("expected_schema field %d must have a non-empty name", i+1)
			}
			if _, duplicate := names[element.Name]; duplicate {
				return nil, bad("expected_schema names must be unique: %s", quoteSchemaFieldName(element.Name))
			}
			if !validSnapshotSchemaFieldType(element.FieldType) {
				// The name and the label are quoted and cut: a pin must not
				// grow its own error. The stored field keeps the full name.
				return nil, bad("expected_schema field %s: field_type %s must be upper-case, up to 64 bytes, like DECIMAL(38,10)",
					quoteSchemaFieldName(element.Name), schemaFieldTypeText(element.FieldType))
			}
			names[element.Name] = struct{}{}
			expectedSchema = append(expectedSchema, snapshotSchemaField{Name: element.Name, FieldType: element.FieldType, Nullable: nullable})
		}
		digest, err := snapshotSchemaDigest(expectedSchema)
		if err != nil || digest != expectedSchemaHash {
			return nil, bad("expected_schema does not hash to expected_schema_hash")
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
	return &datasetInputSnapshot{
		Branch: strings.TrimSpace(branch), Version: int32(version), TransactionID: txnID, ExpectedSchemaHash: expectedSchemaHash,
		ExpectedSchema: expectedSchema, FlattenJSON: flatten, Excel: excel, ExcelFiles: files,
	}, nil
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

// validSnapshotSchemaFieldType accepts a type label of the shape the dataset
// service's versioned schema route states, non-empty and at most
// maxSchemaFieldTypeBytes long. The hash binding is what vouches for the
// list: the check only keeps what a refusal quotes of a label bounded and
// on one line.
func validSnapshotSchemaFieldType(fieldType string) bool {
	return len(fieldType) <= maxSchemaFieldTypeBytes && snapshotSchemaFieldTypeShape.MatchString(fieldType)
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
		input := batchedScanInput{
			DatasetRID:            rid,
			Branch:                req.Snapshot.Branch,
			Version:               req.Snapshot.Version,
			ExpectedTransactionID: &expectedTxn,
		}
		verifiedTxn, err := client.VerifyInputSchema(ctx, input, hash)
		if err != nil {
			return DatasetRowsResult{}, refuseSnapshotSchema(ctx, client, input, req.Snapshot, err)
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

// refuseSnapshotSchema is the read's error for a refused schema proof. When
// the dataset service answered a differing schema and the pin holds the
// fields it expects, the committed schema of the pinned version is read once
// more and each difference is named, the field differences first and then a
// changed order of the fields both schemas hold. That read never leaves a
// difference unnamed: a read that fails or answers the expected schema after
// all says which of these it was, and a difference with no clause at all is
// said to be one this service cannot name. Every other refusal is returned
// exactly as it was before the pin could hold fields. Nothing is scanned in
// any case: the caller returns before its first page.
func refuseSnapshotSchema(ctx context.Context, client batchedDatasetClient, in batchedScanInput, snapshot *datasetInputSnapshot, refusal error) error {
	var mismatch *batchedClientError
	if len(snapshot.ExpectedSchema) == 0 || !errors.As(refusal, &mismatch) ||
		mismatch.Status != http.StatusPreconditionFailed || mismatch.Code != batchedInputSchemaMismatchCode {
		return fmt.Errorf("dataset_input_schema_rejected: %w", refusal)
	}
	committed, err := client.committedInputSchema(ctx, in)
	if err != nil {
		return fmt.Errorf("dataset_input_schema_rejected: %w; the committed schema of version %d could not be read for a field-level reason (%s)",
			refusal, snapshot.Version, cutRunes(err.Error(), maxSchemaReasonBytes))
	}
	if readBack, err := snapshotSchemaDigest(committed); err == nil && readBack == snapshot.ExpectedSchemaHash {
		// The second read answers the very schema the first refused, which
		// contradicts that refusal. Nothing then supports a difference to
		// name, so the refusal is kept and says so.
		return fmt.Errorf("dataset_input_schema_rejected: %w; the schema read back for version %d has the expected hash, so no field difference can be named",
			refusal, snapshot.Version)
	}
	clauses := schemaDifferences(snapshot.ExpectedSchema, committed)
	if len(clauses) > maxSchemaDifferenceClauses {
		clauses = append(clauses[:maxSchemaDifferenceClauses:maxSchemaDifferenceClauses],
			fmt.Sprintf("and %d more", len(clauses)-maxSchemaDifferenceClauses))
	}
	if order := schemaOrderClause(snapshot.ExpectedSchema, committed); order != "" {
		// Appended after the bound, so a reorder that comes with field
		// differences is not cut away with them.
		clauses = append(clauses, order)
	}
	if len(clauses) == 0 {
		// Unreachable while the dataset service is consistent: fields with
		// no difference have one hash.
		return fmt.Errorf("dataset_input_schema_rejected: %w; the committed schema of version %d differs from the expected schema in a way this service cannot name",
			refusal, snapshot.Version)
	}
	return fmt.Errorf("dataset_input_schema_rejected: %w; the committed schema of version %d differs from the expected schema: %s",
		refusal, snapshot.Version, strings.Join(clauses, "; "))
}

// schemaDifferences names how the committed schema differs from the expected
// one: the expected fields in pin order, a field that differs in two ways as
// its type and then its nullability, then the committed-only fields in
// committed order. A type label read back is printed through
// schemaFieldTypeText: it is the dataset service's text, while the pin's own
// label was checked when parsed.
func schemaDifferences(expected, committed []snapshotSchemaField) []string {
	committedByName := make(map[string]snapshotSchemaField, len(committed))
	for _, field := range committed {
		committedByName[field.Name] = field
	}
	clauses := make([]string, 0, len(expected)+len(committed))
	for _, field := range expected {
		actual, present := committedByName[field.Name]
		if !present {
			clauses = append(clauses, fmt.Sprintf("field %s is in the expected schema and not in the committed schema", quoteSchemaFieldName(field.Name)))
			continue
		}
		if actual.FieldType != field.FieldType {
			clauses = append(clauses, fmt.Sprintf("field %s is %s in the committed schema and %s in the expected schema",
				quoteSchemaFieldName(field.Name), schemaFieldTypeText(actual.FieldType), field.FieldType))
		}
		if actual.Nullable != field.Nullable {
			nullability := "not nullable in the committed schema and nullable in the expected schema"
			if actual.Nullable {
				nullability = "nullable in the committed schema and not nullable in the expected schema"
			}
			clauses = append(clauses, fmt.Sprintf("field %s is %s", quoteSchemaFieldName(field.Name), nullability))
		}
	}
	expectedNames := schemaFieldNames(expected)
	for _, field := range committed {
		if _, present := expectedNames[field.Name]; !present {
			clauses = append(clauses, fmt.Sprintf("field %s is in the committed schema and not in the expected schema", quoteSchemaFieldName(field.Name)))
		}
	}
	return clauses
}

func schemaFieldNames(fields []snapshotSchemaField) map[string]struct{} {
	names := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		names[field.Name] = struct{}{}
	}
	return names
}

// schemaOrderClause names a changed relative order of the fields both
// schemas hold, at the first place the two orders differ. Fields only one
// schema holds are left out, so an addition or a removal alone is no change
// of order. It is "" when the order is the same.
func schemaOrderClause(expected, committed []snapshotSchemaField) string {
	committedNames := schemaFieldNames(committed)
	var expectedOrder, committedOrder []string
	for _, field := range expected {
		if _, present := committedNames[field.Name]; present {
			expectedOrder = append(expectedOrder, field.Name)
		}
	}
	expectedNames := schemaFieldNames(expected)
	for _, field := range committed {
		if _, present := expectedNames[field.Name]; present {
			committedOrder = append(committedOrder, field.Name)
		}
	}
	// The expected names are unique and each is in the committed schema, so
	// the committed order is at least as long: indexing it is always safe.
	for i, name := range expectedOrder {
		if name != committedOrder[i] {
			return fmt.Sprintf("the %d fields both schemas hold come in another order: the committed schema has %s where the expected schema has %s",
				len(expectedOrder), quoteSchemaFieldName(committedOrder[i]), quoteSchemaFieldName(name))
		}
	}
	return ""
}

// schemaFieldTypeText is a type label as an error prints it. A label of the
// stated shape is printed as it is. Any other label, which only the dataset
// service can answer or a pin can declare, is quoted, so that a line break or
// another control character never reaches the node error raw, and cut after
// quoting on a rune boundary, "..." marking the cut: an escaped byte is four
// times as wide as the raw one.
func schemaFieldTypeText(label string) string {
	if validSnapshotSchemaFieldType(label) {
		return label
	}
	quoted := strconv.Quote(label)
	if len(quoted) <= maxSchemaFieldTypeBytes {
		return quoted
	}
	return cutRunes(quoted, maxSchemaFieldTypeBytes) + "..."
}

// quoteSchemaFieldName quotes a field name for a refusal, cut on a rune
// boundary so one long name cannot crowd out the other differences.
func quoteSchemaFieldName(name string) string {
	quoted := strconv.Quote(name)
	if len(quoted) <= maxSchemaFieldNameBytes {
		return quoted
	}
	return cutRunes(quoted, maxSchemaFieldNameBytes) + "..."
}

// cutRunes shortens text to at most limit bytes without splitting a rune.
func cutRunes(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	for !utf8.ValidString(text[:limit]) {
		limit--
	}
	return text[:limit]
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
