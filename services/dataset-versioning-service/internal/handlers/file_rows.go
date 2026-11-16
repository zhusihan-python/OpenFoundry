package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// Limits for the registered-file rows reader. The byte cap matches the
// multipart ceiling enforced by UploadData so a file that could be uploaded
// in one request can always be read back in one request.
const (
	datasetFileRowsDefaultLimit = 100
	datasetFileRowsMaxLimit     = 1000
	datasetFileRowsMaxBytes     = 32 << 20
	datasetFileRowsMaxColumns   = 256
	datasetFileRowsMaxLineBytes = 1 << 20
)

// dataFileExtensions are the logical-file formats the rows reader parses
// today. NDJSON is included because pipeline outputs register
// part-*.ndjson files.
func datasetRowsFormat(logicalPath string) string {
	switch strings.ToLower(path.Ext(strings.TrimSpace(logicalPath))) {
	case ".csv":
		return "csv"
	case ".tsv":
		return "tsv"
	case ".ndjson", ".jsonl":
		return "ndjson"
	default:
		return ""
	}
}

type datasetFileRowsResponse struct {
	DatasetID string                        `json:"dataset_id"`
	Path      string                        `json:"path"`
	Format    string                        `json:"format"`
	Source    string                        `json:"source"`
	Columns   []string                      `json:"columns"`
	Rows      []map[string]models.JSONValue `json:"rows"`
	Truncated bool                          `json:"truncated"`
	SizeBytes int64                         `json:"size_bytes"`
}

type datasetRowsError struct {
	status int
	code   string
}

func (e datasetRowsError) Error() string { return e.code }

func datasetRowsErr(status int, format string, args ...any) error {
	return datasetRowsError{status: status, code: fmt.Sprintf(format, args...)}
}

func writeDatasetRowsError(w http.ResponseWriter, err error) {
	var rowsErr datasetRowsError
	if !errors.As(err, &rowsErr) {
		writeJSONErr(w, http.StatusInternalServerError, "failed to read dataset file rows")
		return
	}
	writeJSONErr(w, rowsErr.status, rowsErr.code)
}

// registeredDatasetFile is one readable candidate regardless of which
// registry it came from: the dataset file index (direct uploads) or the
// branch's committed transaction files.
type registeredDatasetFile struct {
	LogicalPath string
	StorageURI  string
	SizeBytes   int64
	LastSeen    time.Time
	Source      string
}

// ReadDatasetFileRows returns parsed rows for a logical file registered in
// this dataset. The caller names a logical path (or lets selection rules
// pick one) and the handler resolves the registered storage mapping
// server-side, verifying the object actually belongs to this dataset. No
// caller-supplied filesystem paths, hosts, or presigned URLs are involved.
//
// mode=execution (default) is strict: ambiguous file selection and files
// larger than the row limit fail explicitly. mode=preview may pick the
// newest registered data file and truncate rows at the limit.
//
// branch=<name> reads the branch's committed transaction files instead of
// the latest file index.
func (h *Handlers) ReadDatasetFileRows(w http.ResponseWriter, r *http.Request) {
	datasetID, ok := h.resolveDatasetForCatalog(w, r)
	if !ok {
		return
	}
	if !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	limit, mode, err := datasetRowsQuery(r)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	wantPath := strings.TrimSpace(r.URL.Query().Get("path"))
	branch := strings.TrimSpace(r.URL.Query().Get("branch"))

	file, err := h.selectRegisteredFile(r.Context(), datasetID, branch, wantPath, mode)
	if err != nil {
		writeDatasetRowsError(w, err)
		return
	}
	out, err := h.readRegisteredFileRows(r.Context(), datasetID, file, limit, mode)
	if err != nil {
		writeDatasetRowsError(w, err)
		return
	}
	out.DatasetID = datasetID.String()
	claims, _ := authmw.FromContext(r.Context())
	actor := "anonymous"
	if claims != nil {
		actor = claims.Sub.String()
	}
	h.emitAudit(r.Context(), AuditEvent{Actor: actor, Action: "files.rows_read", DatasetRID: datasetID.String(), Details: map[string]any{
		"path": file.LogicalPath, "branch": branch, "mode": mode, "limit": limit, "source": out.Source, "truncated": out.Truncated,
	}})
	writeJSON(w, http.StatusOK, out)
}

// requireDatasetRead authorizes the caller against the dataset's read
// boundary: the tenant rule from the verified claims. Admin role does not
// widen it — an org admin still cannot read another organization's or
// another user's personal datasets. It mirrors ownedDataset's lookup (404,
// not 403, so dataset existence is not leaked across tenants).
func (h *Handlers) requireDatasetRead(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID) bool {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	dataset, err := h.Repo.GetDatasetForTenant(r.Context(), datasetID, tenantScopeFromClaims(claims))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load dataset")
		return false
	}
	if dataset == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return false
	}
	return true
}

func datasetRowsQuery(r *http.Request) (int, string, error) {
	limit := datasetFileRowsDefaultLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return 0, "", errors.New("limit must be a positive integer")
		}
		limit = parsed
	}
	if limit > datasetFileRowsMaxLimit {
		limit = datasetFileRowsMaxLimit
	}
	mode := "execution"
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode"))) {
	case "", "execution":
		mode = "execution"
	case "preview":
		mode = "preview"
	default:
		return 0, "", fmt.Errorf("mode must be preview or execution, got %q", r.URL.Query().Get("mode"))
	}
	return limit, mode, nil
}

// selectRegisteredFile picks the file to read. An explicit logical path
// must match a registered entry. Without a path: execution requires exactly
// one candidate; preview takes the most recently modified one.
func (h *Handlers) selectRegisteredFile(ctx context.Context, datasetID uuid.UUID, branch string, wantPath string, mode string) (registeredDatasetFile, error) {
	candidates, err := h.registeredDataFiles(ctx, datasetID, branch)
	if err != nil {
		return registeredDatasetFile{}, err
	}
	if wantPath != "" {
		for _, candidate := range candidates {
			if candidate.LogicalPath == wantPath {
				return candidate, nil
			}
		}
		return registeredDatasetFile{}, datasetRowsErr(http.StatusNotFound, "file not registered in dataset")
	}
	if len(candidates) == 0 {
		if branch != "" {
			return registeredDatasetFile{}, datasetRowsErr(http.StatusUnprocessableEntity, "branch %q has no registered data files (csv, tsv, ndjson, jsonl)", branch)
		}
		return registeredDatasetFile{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset has no registered data files (csv, tsv, ndjson, jsonl)")
	}
	if mode != "preview" && len(candidates) > 1 {
		names := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			names = append(names, candidate.LogicalPath)
		}
		return registeredDatasetFile{}, datasetRowsErr(http.StatusBadRequest, "dataset_file_selection_ambiguous: %d data files registered (%s); specify one with path=", len(names), strings.Join(names, ", "))
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if !candidates[i].LastSeen.Equal(candidates[j].LastSeen) {
			return candidates[i].LastSeen.After(candidates[j].LastSeen)
		}
		return candidates[i].LogicalPath < candidates[j].LogicalPath
	})
	return candidates[0], nil
}

func (h *Handlers) registeredDataFiles(ctx context.Context, datasetID uuid.UUID, branch string) ([]registeredDatasetFile, error) {
	if branch != "" {
		branchRow, err := h.Repo.GetBranch(ctx, datasetID, branch)
		if err != nil {
			return nil, datasetRowsErr(http.StatusInternalServerError, "failed to load branch")
		}
		if branchRow == nil {
			return nil, datasetRowsErr(http.StatusNotFound, "branch not found")
		}
		// The branch's committed transaction files come from the branch
		// view — the files projection alone is time-cutoff filtered without
		// branch constraints and can mix in other branches.
		view, err := h.Repo.GetViewAt(ctx, datasetID, branch, nil, nil)
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return nil, datasetRowsErr(http.StatusNotFound, "branch not found")
			}
			return nil, datasetRowsErr(http.StatusInternalServerError, "failed to compute branch view")
		}
		if view == nil {
			return nil, datasetRowsErr(http.StatusInternalServerError, "failed to compute branch view")
		}
		out := make([]registeredDatasetFile, 0, len(view.Files))
		for _, file := range view.Files {
			if datasetRowsFormat(file.LogicalPath) == "" {
				continue
			}
			out = append(out, registeredDatasetFile{
				LogicalPath: file.LogicalPath, StorageURI: file.PhysicalPath, SizeBytes: file.SizeBytes,
				LastSeen: view.ComputedAt, Source: "branch_transaction",
			})
		}
		return out, nil
	}
	entries, err := h.Repo.ListDatasetFileIndex(ctx, datasetID)
	if err != nil {
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to load dataset file index")
	}
	out := make([]registeredDatasetFile, 0, len(entries))
	for _, entry := range entries {
		if entry.EntryType != "" && entry.EntryType != "file" {
			continue
		}
		if datasetRowsFormat(entry.Path) == "" {
			continue
		}
		lastSeen := entry.CreatedAt
		if entry.LastModified != nil {
			lastSeen = *entry.LastModified
		}
		out = append(out, registeredDatasetFile{
			LogicalPath: entry.Path, StorageURI: entry.StoragePath, SizeBytes: entry.SizeBytes,
			LastSeen: lastSeen, Source: "file_index",
		})
	}
	return out, nil
}

// readRegisteredFileRows parses the file's backing bytes. Missing bytes are
// a hard failure — registered preview metadata is never substituted for
// real input data.
func (h *Handlers) readRegisteredFileRows(ctx context.Context, datasetID uuid.UUID, file registeredDatasetFile, limit int, mode string) (*datasetFileRowsResponse, error) {
	format := datasetRowsFormat(file.LogicalPath)
	if format == "" {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_format_unsupported: %s is not a csv/tsv/ndjson/jsonl file", file.LogicalPath)
	}
	if file.SizeBytes > datasetFileRowsMaxBytes {
		return nil, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_file_too_large: %s is %d bytes, limit is %d", file.LogicalPath, file.SizeBytes, datasetFileRowsMaxBytes)
	}
	local, ok := h.BackingFS.(localObjectStore)
	if !ok || h.BackingFS == nil {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: rows reads require the local backing filesystem")
	}
	location := storageabstraction.ParsePhysicalURI(file.StorageURI)
	if location.FSID != "local" {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: storage %s is not readable by this deployment", location.FSID)
	}
	if err := h.requireDatasetOwnedObjectKey(ctx, datasetID, location.RelativePath); err != nil {
		return nil, err
	}
	data, readErr := local.ReadLocalObject(location.RelativePath)
	if readErr != nil {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_bytes_missing: %s has no readable backing bytes at its registered storage path", file.LogicalPath)
	}
	if int64(len(data)) > datasetFileRowsMaxBytes {
		return nil, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_file_too_large: %s is %d bytes, limit is %d", file.LogicalPath, int64(len(data)), datasetFileRowsMaxBytes)
	}
	columns, rows, truncated, parseErr := parseDatasetRowsFile(format, data, limit)
	if parseErr != nil {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_parse_failed: %s: %v", file.LogicalPath, parseErr)
	}
	if truncated && mode != "preview" {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_rows_exceeds_limit: %s has more than %d rows; execution refuses to run a truncated input (preview may truncate)", file.LogicalPath, limit)
	}
	return &datasetFileRowsResponse{
		Path: file.LogicalPath, Format: format, Source: file.Source,
		Columns: columns, Rows: rows, Truncated: truncated, SizeBytes: file.SizeBytes,
	}, nil
}

// requireDatasetOwnedObjectKey rejects registered storage paths that point
// outside this dataset's namespace: the dataset's own upload directory
// (baseDir/datasets/<id>/…), or outputs/transactions objects whose
// transaction actually belongs to this dataset. A stray index entry naming
// another dataset's object is therefore unreadable.
func (h *Handlers) requireDatasetOwnedObjectKey(ctx context.Context, datasetID uuid.UUID, key string) error {
	trimmed := strings.Trim(key, "/")
	if trimmed == "" {
		return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_storage_invalid: empty storage key")
	}
	baseDir := strings.Trim(h.BackingFS.BaseDirectory(), "/")
	relative := trimmed
	if baseDir != "" && strings.HasPrefix(relative, baseDir+"/") {
		relative = strings.TrimPrefix(relative, baseDir+"/")
	}
	if strings.HasPrefix(relative, path.Join("datasets", datasetID.String())+"/") {
		return nil
	}
	parts := strings.SplitN(relative, "/", 3)
	if len(parts) == 3 && (parts[0] == "outputs" || parts[0] == "transactions") {
		txnID, err := uuid.Parse(parts[1])
		if err != nil {
			return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_storage_invalid: %s does not name a transaction", trimmed)
		}
		if _, found, txnErr := h.Repo.GetTransactionStatus(ctx, datasetID, txnID); txnErr != nil {
			return datasetRowsErr(http.StatusInternalServerError, "failed to load transaction")
		} else if found {
			return nil
		}
	}
	return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_storage_outside_dataset: %s does not belong to dataset %s", trimmed, datasetID.String())
}

var datasetRowsNumericLiteral = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// csvCellValue types one delimited cell: empty becomes null, plain integers
// and decimals become JSON numbers, everything else — including values with
// leading zeros such as SKUs like "007" and integer literals too wide for
// float64 precision — stays an exact string.
func csvCellValue(cell string) models.JSONValue {
	trimmed := strings.TrimSpace(cell)
	if trimmed == "" {
		return models.JSONValue("null")
	}
	if datasetRowsNumericLiteral.MatchString(trimmed) {
		if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return models.JSONValue(trimmed)
		}
		digits, _, _ := strings.Cut(strings.TrimPrefix(trimmed, "-"), ".")
		if len(digits) <= 15 {
			if _, err := strconv.ParseFloat(trimmed, 64); err == nil {
				return models.JSONValue(trimmed)
			}
		}
	}
	encoded, err := json.Marshal(trimmed)
	if err != nil {
		return models.JSONValue("null")
	}
	return models.JSONValue(encoded)
}

// parseDatasetRowsFile parses CSV/TSV/NDJSON bytes into name-keyed rows,
// stopping after limit data rows and reporting whether more remained.
func parseDatasetRowsFile(format string, data []byte, limit int) ([]string, []map[string]models.JSONValue, bool, error) {
	switch format {
	case "csv", "tsv":
		return parseDelimitedRows(data, format == "tsv", limit)
	case "ndjson":
		return parseNDJSONRows(data, limit)
	default:
		return nil, nil, false, fmt.Errorf("unsupported format %q", format)
	}
}

func parseDelimitedRows(data []byte, tabSeparated bool, limit int) ([]string, []map[string]models.JSONValue, bool, error) {
	reader := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))))
	if tabSeparated {
		reader.Comma = '\t'
	}
	header, err := reader.Read()
	if err == io.EOF {
		return []string{}, []map[string]models.JSONValue{}, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("read header: %w", err)
	}
	columns := make([]string, 0, len(header))
	seen := map[string]bool{}
	for i, raw := range header {
		name := strings.TrimSpace(raw)
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		if seen[name] {
			return nil, nil, false, fmt.Errorf("duplicate column %q", name)
		}
		seen[name] = true
		columns = append(columns, name)
	}
	if len(columns) > datasetFileRowsMaxColumns {
		return nil, nil, false, fmt.Errorf("file has %d columns, max %d", len(columns), datasetFileRowsMaxColumns)
	}
	rows := make([]map[string]models.JSONValue, 0, limit)
	truncated := false
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, false, fmt.Errorf("read record %d: %w", len(rows)+1, err)
		}
		if len(rows) >= limit {
			truncated = true
			break
		}
		row := make(map[string]models.JSONValue, len(columns))
		for i, cell := range record {
			row[columns[i]] = csvCellValue(cell)
		}
		rows = append(rows, row)
	}
	return columns, rows, truncated, nil
}

func parseNDJSONRows(data []byte, limit int) ([]string, []map[string]models.JSONValue, bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), datasetFileRowsMaxLineBytes)
	columns := []string{}
	columnSet := map[string]bool{}
	rows := make([]map[string]models.JSONValue, 0, limit)
	truncated := false
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if len(rows) >= limit {
			truncated = true
			break
		}
		var object map[string]models.JSONValue
		if err := json.Unmarshal(line, &object); err != nil {
			return nil, nil, false, fmt.Errorf("line %d is not a JSON object: %w", len(rows)+1, err)
		}
		row := make(map[string]models.JSONValue, len(object))
		for key, value := range object {
			if !columnSet[key] {
				columnSet[key] = true
				columns = append(columns, key)
			}
			row[key] = append(models.JSONValue(nil), value...)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, false, fmt.Errorf("scan ndjson: %w", err)
	}
	if len(columns) > datasetFileRowsMaxColumns {
		return nil, nil, false, fmt.Errorf("file has %d columns, max %d", len(columns), datasetFileRowsMaxColumns)
	}
	// Deterministic column order across previews and repeated reads — NDJSON
	// objects carry no header, so first-seen order would follow file layout.
	sort.Strings(columns)
	return columns, rows, truncated, nil
}
