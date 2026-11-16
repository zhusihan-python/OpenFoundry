package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/xlsximp"
)

// P3.1 Excel sheet/header mapping import. The pre-create endpoints
// (/datasets/imports/excel/...) live under the dataset prefix so the edge
// gateway forwards them like every other dataset route; the import itself is
// dataset-scoped and requires the same write boundary as UploadData.
//
// Ordering guarantees: full validation and normalization happen before any
// write; the normalized NDJSON object is written into a fresh per-import UUID
// namespace (an object referenced by a previous index state is never
// rewritten); only then is the file index replaced in one transaction. A
// later failure leaves prior data and schema untouched — a stray object
// without an index entry is inert.

func excelErrStatus(kind string) int {
	switch kind {
	case xlsximp.ErrOversized:
		return http.StatusRequestEntityTooLarge
	case xlsximp.ErrInvalid, xlsximp.ErrZipUnsafe, xlsximp.ErrMacro,
		xlsximp.ErrExternalLinks, xlsximp.ErrEncrypted, xlsximp.ErrSheet,
		xlsximp.ErrParse, xlsximp.ErrRowLimit, xlsximp.ErrFormula,
		xlsximp.ErrOutputLimit:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusBadRequest
	}
}

func writeExcelErr(w http.ResponseWriter, err error) {
	writeJSONErr(w, excelErrStatus(xlsximp.KindOf(err)), err.Error())
}

// requireClaims authorizes the pre-create endpoints: a verified JWT must be
// present. No dataset boundary is involved because no dataset exists yet.
func requireClaims(w http.ResponseWriter, r *http.Request) bool {
	if _, ok := authmw.FromContext(r.Context()); !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	return true
}

// readExcelUpload reads the bounded multipart workbook. The request envelope
// is capped at the workbook size plus a small margin for headers and the
// config form field; the file part itself is independently capped at
// MaxCompressedBytes. The parsed form is always removed, including on error
// and cancellation paths.
func readExcelUpload(w http.ResponseWriter, r *http.Request) ([]byte, string, bool) {
	envelopeCap := int64(xlsximp.MaxCompressedBytes + xlsximp.MultipartEnvelopeMargin)
	r.Body = http.MaxBytesReader(w, r.Body, envelopeCap)
	if err := r.ParseMultipartForm(envelopeCap); err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			writeJSONErr(w, http.StatusRequestEntityTooLarge, "excel_oversized: workbook upload exceeds 32MiB")
			return nil, "", false
		}
		writeJSONErr(w, http.StatusBadRequest, "invalid multipart body")
		return nil, "", false
	}
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()
	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "file part is required")
		return nil, "", false
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, xlsximp.MaxCompressedBytes+1))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "failed to read file")
		return nil, "", false
	}
	if len(data) > xlsximp.MaxCompressedBytes {
		writeJSONErr(w, http.StatusRequestEntityTooLarge, "excel_oversized: workbook exceeds 32MiB")
		return nil, "", false
	}
	return data, header.Filename, true
}

func decodeExcelConfig(w http.ResponseWriter, r *http.Request) (xlsximp.Config, bool) {
	vals := r.MultipartForm.Value["config"]
	if len(vals) == 0 || strings.TrimSpace(vals[0]) == "" {
		writeJSONErr(w, http.StatusBadRequest, "excel_config_invalid: config form field is required")
		return xlsximp.Config{}, false
	}
	var cfg xlsximp.Config
	if err := json.Unmarshal([]byte(vals[0]), &cfg); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "excel_config_invalid: "+err.Error())
		return xlsximp.Config{}, false
	}
	return cfg, true
}

// ExcelInspect lists the selectable worksheets of an uploaded workbook.
func (h *Handlers) ExcelInspect(w http.ResponseWriter, r *http.Request) {
	if !requireClaims(w, r) {
		return
	}
	data, filename, ok := readExcelUpload(w, r)
	if !ok {
		return
	}
	out, err := xlsximp.Inspect(data, filename)
	if err != nil {
		writeExcelErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// ExcelPreview validates a configuration against the real workbook bytes.
// Without a mapping it returns the physical source columns for mapping UIs;
// with a mapping it returns the canonical normalized columns and samples.
// The row count is always fully counted so a later import cannot surprise
// the caller with a row-limit failure.
func (h *Handlers) ExcelPreview(w http.ResponseWriter, r *http.Request) {
	if !requireClaims(w, r) {
		return
	}
	data, _, ok := readExcelUpload(w, r)
	if !ok {
		return
	}
	cfg, ok := decodeExcelConfig(w, r)
	if !ok {
		return
	}
	sampleRows := xlsximp.DefaultSampleRows
	if vals := r.MultipartForm.Value["sample_rows"]; len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
		n, err := strconv.Atoi(strings.TrimSpace(vals[0]))
		if err != nil || n < 0 {
			writeJSONErr(w, http.StatusBadRequest, "excel_config_invalid: sample_rows must be a non-negative integer")
			return
		}
		sampleRows = n
	}
	out, err := xlsximp.Normalize(data, cfg, sampleRows)
	if err != nil {
		writeExcelErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// ExcelImport re-validates the exact previewed configuration, normalizes the
// selected sheet to NDJSON, writes the bytes to a fresh per-import object,
// and publishes the file index entry (with the parser configuration, source
// provenance, and normalized hash bound atomically to that entry) in one
// transaction. No legacy schema row is written: /schema derives from the
// same real bytes for these datasets.
func (h *Handlers) ExcelImport(w http.ResponseWriter, r *http.Request) {
	datasetID, ok := h.resolveDatasetForCatalog(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireDatasetWrite(w, r, datasetID); !ok {
		return
	}
	// The normalized output is NDJSON: registering it on a dataset whose
	// catalog format is not json would leave the catalog format lying about
	// the dataset's files.
	if claims, okClaims := authmw.FromContext(r.Context()); okClaims {
		dataset, derr := h.Repo.GetDatasetForTenant(r.Context(), datasetID, tenantScopeFromClaims(claims))
		if derr != nil || dataset == nil {
			writeJSONErr(w, http.StatusNotFound, "dataset not found")
			return
		}
		if !strings.EqualFold(dataset.Format, "json") {
			writeJSONErr(w, http.StatusUnprocessableEntity, "excel_config_invalid: excel import requires a dataset with format=json, this dataset is "+dataset.Format)
			return
		}
	}
	local, ok := h.BackingFS.(localObjectStore)
	if !ok || h.BackingFS == nil || h.BackingFS.FSID() != "local" {
		writeDependencyUnavailable(w, "local_backing_filesystem_unavailable", "local backing filesystem not configured")
		return
	}
	data, filename, ok := readExcelUpload(w, r)
	if !ok {
		return
	}
	cfg, ok := decodeExcelConfig(w, r)
	if !ok {
		return
	}
	if cfg.Mapping == nil || len(cfg.Mapping) == 0 {
		writeJSONErr(w, http.StatusBadRequest, "excel_mapping_invalid: import requires a non-empty mapping")
		return
	}
	logical := ""
	if vals := r.MultipartForm.Value["logical_path"]; len(vals) > 0 {
		logical = strings.TrimSpace(vals[0])
	}
	if logical == "" {
		logical = xlsximp.SuggestedLogicalPath(filename)
	} else if !strings.HasSuffix(strings.ToLower(logical), ".ndjson") && !strings.HasSuffix(strings.ToLower(logical), ".jsonl") {
		writeJSONErr(w, http.StatusBadRequest, "excel_config_invalid: logical_path must end with .ndjson or .jsonl")
		return
	}
	if !safeObjectKey(logical) {
		writeJSONErr(w, http.StatusBadRequest, "invalid logical_path")
		return
	}

	result, err := xlsximp.Normalize(data, cfg, 0)
	if err != nil {
		writeExcelErr(w, err)
		return
	}
	ndjson := result.NDJSON()
	sum := sha256.Sum256(data)
	sourceSHA := hex.EncodeToString(sum[:])

	// Fresh per-import namespace: an object referenced by any earlier index
	// state is never rewritten, so a failed DB step cannot corrupt prior
	// data.
	objectKey := path.Join(h.BackingFS.BaseDirectory(), "datasets", datasetID.String(), "excel", uuid.NewString(), "data.ndjson")
	if err := local.WriteLocalObject(objectKey, ndjson); err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to write object")
		return
	}
	physical := storageabstraction.PhysicalLocation{FSID: h.BackingFS.FSID(), RelativePath: objectKey}
	now := time.Now().UTC()
	entryType := "file"
	contentType := "application/x-ndjson"
	size := int64(len(ndjson))
	metadata, err := models.MarshalJSONValue(map[string]any{
		"excel_import":    1,
		"normalized":      true,
		"source_filename": filename,
		"source_sha256":   sourceSHA,
		"parser_config":   result.Config,
		"columns":         result.Columns,
		"row_count":       result.RowCount,
		"ndjson_sha256":   result.NDJSONSHA256(),
		"imported_at":     now.Format(time.RFC3339),
	})
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to encode metadata")
		return
	}

	current, err := h.Repo.ListDatasetFileIndex(r.Context(), datasetID)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load dataset file index")
		return
	}
	merged := make([]models.PutDatasetFileIndexEntry, 0, len(current)+1)
	for _, item := range current {
		entry := models.PutDatasetFileIndexEntry{Path: item.Path, StoragePath: item.StoragePath, EntryType: &item.EntryType, SizeBytes: &item.SizeBytes, ContentType: item.ContentType, Metadata: item.Metadata, LastModified: item.LastModified}
		if item.Path == logical {
			entry = models.PutDatasetFileIndexEntry{Path: logical, StoragePath: physical.URI(), EntryType: &entryType, SizeBytes: &size, ContentType: &contentType, Metadata: metadata, LastModified: &now}
		}
		merged = append(merged, entry)
	}
	if !containsLogicalPath(merged, logical) {
		merged = append(merged, models.PutDatasetFileIndexEntry{Path: logical, StoragePath: physical.URI(), EntryType: &entryType, SizeBytes: &size, ContentType: &contentType, Metadata: metadata, LastModified: &now})
	}
	// Catalog stats published in the same transaction as the index:
	// size_bytes sums every entry; row_count sums the exact counts recorded
	// for Excel-imported entries (see ReplaceDatasetFileIndexWithStats).
	var totalRows, totalBytes int64
	for _, e := range merged {
		if e.SizeBytes != nil {
			totalBytes += *e.SizeBytes
		}
		if e.Metadata != nil {
			var meta struct {
				ExcelImport int   `json:"excel_import"`
				RowCount    int64 `json:"row_count"`
			}
			if json.Unmarshal(e.Metadata, &meta) == nil && meta.ExcelImport == 1 {
				totalRows += meta.RowCount
			}
		}
	}
	if err := h.Repo.ReplaceDatasetFileIndexWithStats(r.Context(), datasetID, merged, totalRows, totalBytes); err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to update dataset file index")
		return
	}
	items, err := h.Repo.ListDatasetFileIndex(r.Context(), datasetID)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to list dataset file index")
		return
	}
	claims, _ := authmw.FromContext(r.Context())
	actor := "anonymous"
	if claims != nil {
		actor = claims.Sub.String()
	}
	h.emitAudit(r.Context(), AuditEvent{Actor: actor, Action: "excel.import", DatasetRID: datasetID.String(), Details: map[string]any{
		"path": logical, "sheet": result.Config.Sheet, "row_count": result.RowCount, "columns": len(result.Columns), "source_sha256": sourceSHA,
	}})
	writeJSON(w, http.StatusCreated, map[string]any{
		"path": logical, "physical_uri": physical.URI(), "size_bytes": size,
		"row_count": result.RowCount, "columns": result.Columns,
		"ndjson_sha256": result.NDJSONSHA256(), "files": items,
	})
}

func containsLogicalPath(entries []models.PutDatasetFileIndexEntry, logical string) bool {
	for _, e := range entries {
		if e.Path == logical {
			return true
		}
	}
	return false
}

// latestReadableIndexEntry picks the same file the generic dataset surfaces
// would choose: the most recently modified readable data file in the index
// (ties broken by path), regardless of origin.
func (h *Handlers) latestReadableIndexEntry(ctx context.Context, datasetID uuid.UUID) (*models.DatasetFileIndexEntry, error) {
	entries, err := h.Repo.ListDatasetFileIndex(ctx, datasetID)
	if err != nil {
		return nil, err
	}
	var best *models.DatasetFileIndexEntry
	for i := range entries {
		entry := entries[i]
		if entry.EntryType != "" && entry.EntryType != "file" {
			continue
		}
		if datasetRowsFormat(entry.Path) == "" {
			continue
		}
		if best == nil {
			best = &entries[i]
			continue
		}
		bestSeen := best.CreatedAt
		if best.LastModified != nil {
			bestSeen = *best.LastModified
		}
		seen := entry.CreatedAt
		if entry.LastModified != nil {
			seen = *entry.LastModified
		}
		if seen.After(bestSeen) || (seen.Equal(bestSeen) && entry.Path < best.Path) {
			best = &entries[i]
		}
	}
	return best, nil
}

func excelImportMarker(entry *models.DatasetFileIndexEntry) bool {
	if entry == nil || len(entry.Metadata) == 0 {
		return false
	}
	var meta struct {
		ExcelImport int  `json:"excel_import"`
		Normalized   bool `json:"normalized"`
	}
	if err := json.Unmarshal(entry.Metadata, &meta); err != nil {
		return false
	}
	return meta.ExcelImport == 1 && meta.Normalized
}

// readIndexEntryBytes loads the backing bytes of an index entry with the same
// ownership checks as the rows reader.
func (h *Handlers) readIndexEntryBytes(ctx context.Context, datasetID uuid.UUID, entry *models.DatasetFileIndexEntry) ([]byte, error) {
	local, ok := h.BackingFS.(localObjectStore)
	if !ok || h.BackingFS == nil {
		return nil, errKindDependency("dataset_rows_backing_driver_unavailable: rows reads require the local backing filesystem")
	}
	location := storageabstraction.ParsePhysicalURI(entry.StoragePath)
	if location.FSID != "local" {
		return nil, errKindDependency("dataset_rows_backing_driver_unsupported: storage " + location.FSID + " is not readable by this deployment")
	}
	if err := h.requireDatasetOwnedObjectKey(ctx, datasetID, location.RelativePath); err != nil {
		return nil, err
	}
	if entry.SizeBytes > datasetFileRowsMaxBytes {
		return nil, errKindDependency(fmt.Sprintf("dataset_file_too_large: %s is %d bytes, limit is %d", entry.Path, entry.SizeBytes, datasetFileRowsMaxBytes))
	}
	data, err := local.ReadLocalObject(location.RelativePath)
	if err != nil {
		return nil, errKindDependency("dataset_file_bytes_missing: " + entry.Path + " has no readable backing bytes")
	}
	if int64(len(data)) > datasetFileRowsMaxBytes {
		return nil, errKindDependency(fmt.Sprintf("dataset_file_too_large: %s exceeds %d bytes", entry.Path, datasetFileRowsMaxBytes))
	}
	return data, nil
}

// errKindDependency returns the same VALUE-typed datasetRowsError as
// datasetRowsErr so writeDatasetRowsError's errors.As(&rowsErr) matches and
// surfaces the 4xx status; a pointer here would silently become a 500.
func errKindDependency(msg string) error {
	return datasetRowsError{status: http.StatusUnprocessableEntity, code: msg}
}

// datasetIsExcelImported reports whether the dataset's newest readable data
// file is an Excel import — the scope where the P3.1 byte-reader surfaces
// (default preview/schema and explicit branch reads) apply. Only a confirmed
// non-Excel answer allows the legacy path; an index lookup error is returned
// so callers fail immediately instead of falling back to metadata samples.
func (h *Handlers) datasetIsExcelImported(ctx context.Context, datasetID uuid.UUID) (bool, error) {
	entry, err := h.latestReadableIndexEntry(ctx, datasetID)
	if err != nil {
		return false, err
	}
	return excelImportMarker(entry), nil
}

// branchRegisteredPreview serves an explicitly requested branch preview
// through the same registered-branch machinery as /files/rows?branch=:
// committed transaction files only, newest file in preview mode, real
// backing bytes, typed errors for missing branches/files. The global
// file-index preview_rows metadata is never consulted for branch reads.
func (h *Handlers) branchRegisteredPreview(ctx context.Context, datasetID uuid.UUID, branch string, q models.PreviewQuery) (*models.PreviewDataResponse, error) {
	limit := 100
	if q.Limit != nil && *q.Limit > 0 {
		limit = *q.Limit
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := 0
	if q.Offset != nil && *q.Offset > 0 {
		offset = *q.Offset
	}
	if offset+limit > 2000 {
		offset = 2000 - limit
		if offset < 0 {
			offset = 0
		}
	}
	file, err := h.selectRegisteredFile(ctx, datasetID, branch, "", "preview")
	if err != nil {
		return nil, err
	}
	out, err := h.readRegisteredFileRows(ctx, datasetID, file, offset+limit, "preview")
	if err != nil {
		return nil, err
	}
	matrix, windowed := rowsMatrix(out.Columns, out.Rows, offset, limit)
	return &models.PreviewDataResponse{Columns: out.Columns, Rows: matrix, Format: "json", Limit: limit, Offset: windowed}, nil
}

// rowsMatrix converts name-keyed rows to a column-ordered matrix and windows
// it by [offset, offset+limit); missing keys surface as explicit nulls.
func rowsMatrix(columns []string, rows []map[string]models.JSONValue, offset, limit int) ([][]models.JSONValue, int) {
	if offset > len(rows) {
		offset = len(rows)
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	out := make([][]models.JSONValue, 0, end-offset)
	for _, row := range rows[offset:end] {
		vals := make([]models.JSONValue, len(columns))
		for i, col := range columns {
			if v, ok := row[col]; ok {
				vals[i] = v
			} else {
				vals[i] = models.JSONValue("null")
			}
		}
		out = append(out, vals)
	}
	return out, offset
}

// excelImportedPreview serves GET /datasets/{rid}/preview for datasets whose
// newest readable data file is an Excel import: it reads the same real
// NDJSON bytes the rows reader and pipelines consume — a forged
// preview_rows metadata field can never shape this output. When the newest
// readable file is not an Excel import the caller falls through to the
// existing preview path; a newer non-Excel file is never skipped in favor of
// an older import.
func (h *Handlers) excelImportedPreview(ctx context.Context, datasetID uuid.UUID, q models.PreviewQuery) (*models.PreviewDataResponse, bool, error) {
	entry, err := h.latestReadableIndexEntry(ctx, datasetID)
	if err != nil {
		return nil, false, err
	}
	if !excelImportMarker(entry) {
		return nil, false, nil
	}
	data, err := h.readIndexEntryBytes(ctx, datasetID, entry)
	if err != nil {
		return nil, false, err
	}
	// Always read a fixed, small number of rows and window them in memory:
	// a caller-supplied offset must never control an allocation, and the
	// offset arithmetic cannot overflow.
	limit := 100
	if q.Limit != nil && *q.Limit > 0 {
		limit = *q.Limit
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := 0
	if q.Offset != nil && *q.Offset > 0 {
		offset = *q.Offset
	}
	columns, rows, _, err := parseNDJSONRows(data, 1000)
	if err != nil {
		return nil, false, errKindDependency(fmt.Sprintf("dataset_file_parse_failed: %s: %v", entry.Path, err))
	}
	out, windowed := rowsMatrix(columns, rows, offset, limit)
	return &models.PreviewDataResponse{Columns: columns, Rows: out, Format: "json", Limit: limit, Offset: windowed}, true, nil
}

// excelImportedSchema serves GET /datasets/{rid}/schema without an explicit
// branch for Excel-imported datasets. The fields come from the same real
// NDJSON bytes preview and pipelines read (the index metadata columns are a
// cross-check, not the source of truth), and the format is TEXT rather than
// the legacy PARQUET default.
func (h *Handlers) excelImportedSchema(ctx context.Context, datasetID uuid.UUID) (*models.SchemaResponse, bool, error) {
	entry, err := h.latestReadableIndexEntry(ctx, datasetID)
	if err != nil {
		return nil, false, err
	}
	if !excelImportMarker(entry) {
		return nil, false, nil
	}
	data, err := h.readIndexEntryBytes(ctx, datasetID, entry)
	if err != nil {
		return nil, false, err
	}
	columns, _, _, err := parseNDJSONRows(data, 1)
	if err != nil {
		return nil, false, errKindDependency(fmt.Sprintf("dataset_file_parse_failed: %s: %v", entry.Path, err))
	}
	sort.Strings(columns)
	fields := make([]models.Field, 0, len(columns))
	for _, name := range columns {
		fields = append(fields, models.Field{Name: name, Type: models.FieldTypeString, Nullable: true})
	}
	sum := sha256.Sum256(data)
	createdAt := entry.CreatedAt
	if entry.LastModified != nil {
		createdAt = *entry.LastModified
	}
	// The file-index import is unversioned: no committed branch is implied,
	// so Branch stays nil (a real branch view would name its branch through
	// the legacy schema path). ViewID nil marks the unversioned sentinel.
	return &models.SchemaResponse{
		ViewID:      uuid.Nil,
		DatasetID:   datasetID,
		Branch:      nil,
		Schema:      models.DatasetSchema{Fields: fields, FileFormat: models.FileFormatText},
		ContentHash: "excel-import:" + hex.EncodeToString(sum[:]),
		CreatedAt:   createdAt,
	}, true, nil
}
