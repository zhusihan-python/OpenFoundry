package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type historicalSchemaField struct {
	Name      string `json:"name"`
	FieldType string `json:"field_type"`
	Nullable  bool   `json:"nullable"`
}

type historicalSchemaResponse struct {
	DatasetID     uuid.UUID               `json:"dataset_id"`
	Branch        string                  `json:"branch"`
	Version       int32                   `json:"version"`
	TransactionID uuid.UUID               `json:"transaction_id"`
	ContentHash   string                  `json:"content_hash"`
	Fields        []historicalSchemaField `json:"fields"`
}

const (
	expectedHistoricalSchemaHash  = "expected_content_hash"
	historicalSchemaTransactionID = "transaction_id"
	// Both mismatch answers keep the long-standing "<code>: <text>" error
	// text and add the bare machine code for callers that match on it.
	schemaHashMismatchCode        = "dataset_schema_content_hash_mismatch"
	schemaTransactionMismatchCode = "dataset_schema_transaction_mismatch"
)

func (h *Handlers) getHistoricalSchema(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, query url.Values) {
	if !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	branch := strings.TrimSpace(query.Get("branch"))
	version, err := strconv.ParseInt(strings.TrimSpace(query.Get("version")), 10, 32)
	maxParams := 2
	expected, conditional := query[expectedHistoricalSchemaHash]
	if conditional {
		maxParams++
		if len(expected) != 1 || !validHistoricalSchemaHash(expected[0]) {
			writeJSONErr(w, http.StatusBadRequest, "expected_content_hash must be a single lowercase hexadecimal SHA-256 digest")
			return
		}
	}
	// A caller that holds a (version, transaction_id) pair, such as a
	// dataset_snapshot pin, can cross-check it: the version must have been
	// cut by exactly that transaction.
	var pinnedTxn uuid.UUID
	if requested, present := query[historicalSchemaTransactionID]; present {
		maxParams++
		parsed, parseErr := uuid.Parse(strings.TrimSpace(query.Get(historicalSchemaTransactionID)))
		if len(requested) != 1 || parseErr != nil || parsed == uuid.Nil {
			writeJSONErr(w, http.StatusBadRequest, "transaction_id must be a single transaction UUID")
			return
		}
		pinnedTxn = parsed
	}
	if len(query) != maxParams || len(query["branch"]) != 1 || len(query["version"]) != 1 || branch == "" || err != nil || version < 1 {
		writeJSONErr(w, http.StatusBadRequest, "versioned schema requires exactly one branch and positive integer version")
		return
	}
	target, ok := h.resolveScanTarget(w, r, datasetID, branch, int32(version))
	if !ok {
		return
	}
	if pinnedTxn != uuid.Nil && target.txn.ID != pinnedTxn {
		writeJSON(w, http.StatusConflict, models.ErrorEnvelope{
			Error: fmt.Sprintf("%s: version %d was cut by transaction %s, not %s",
				schemaTransactionMismatchCode, version, target.txn.ID, pinnedTxn),
			Code: strPtr(schemaTransactionMismatchCode),
		})
		return
	}
	ctx := r.Context()
	files, err := h.Repo.ListTransactionStagedFilesFrom(ctx, target.txn.ID, "", scanMaxFilesPerPage+1)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load staged files"))
		return
	}
	if len(files) == 0 || len(files) > scanMaxFilesPerPage {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_schema_file_count_unsupported: versioned schemas require 1 to %d Parquet files", scanMaxFilesPerPage))
		return
	}
	opener, ok := h.scanBackingOpener()
	if !ok {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_rows_backing_driver_unsupported: versioned schemas require the local backing filesystem"))
		return
	}
	var fields []historicalSchemaField
	// NDJSON has no footer. One NDJSON output answers from the schema its
	// keyed commit sealed in the receipt of the transaction that cut the
	// version; any other non-Parquet version stays unsupported.
	if textOutput := len(files) == 1 && datasetRowsFormat(files[0].LogicalPath) == "ndjson" &&
		files[0].Operation == models.FileOperationAdd; textOutput {
		var ok bool
		fields, ok = h.readHistoricalReceiptFields(w, ctx, opener, datasetID, branch, target.txn, files[0])
		if !ok {
			return
		}
	} else {
		for _, file := range files {
			if !validStagedParquetPath(file.LogicalPath) || file.Operation != models.FileOperationAdd {
				writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
					"dataset_schema_format_unsupported: versioned schemas require Parquet ADD files or one NDJSON output with a schema in its commit receipt"))
				return
			}
			if !h.validateScanStagedObject(w, ctx, datasetID, target.txn, file.LogicalPath, &file) {
				return
			}
			current, err := readHistoricalParquetFields(ctx, opener, file)
			if err != nil {
				writeDatasetRowsError(w, err)
				return
			}
			if fields != nil && !slices.Equal(fields, current) {
				writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
					"dataset_schema_files_mismatch: Parquet files in the version have different schemas"))
				return
			}
			fields = current
		}
	}
	// Hash only the ordered public field list, so changing row values or
	// republishing the same schema in another transaction preserves the digest.
	encoded, err := json.Marshal(fields)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to encode versioned schema")
		return
	}
	digest := sha256.Sum256(encoded)
	hash := hex.EncodeToString(digest[:])
	if conditional && expected[0] != hash {
		writeJSON(w, http.StatusPreconditionFailed, models.ErrorEnvelope{
			Error: schemaHashMismatchCode + ": the committed schema does not match expected_content_hash",
			Code:  strPtr(schemaHashMismatchCode),
		})
		return
	}
	writeJSON(w, http.StatusOK, historicalSchemaResponse{
		DatasetID: datasetID, Branch: branch, Version: int32(version),
		TransactionID: target.txn.ID, ContentHash: hash, Fields: fields,
	})
}

func validHistoricalSchemaHash(value string) bool {
	return len(value) == hex.EncodedLen(sha256.Size) &&
		strings.IndexFunc(value, func(r rune) bool {
			return (r < '0' || r > '9') && (r < 'a' || r > 'f')
		}) == -1
}

func readHistoricalParquetFields(ctx context.Context, opener localObjectOpener, file models.StageTransactionFile) ([]historicalSchemaField, error) {
	reader, err := openHistoricalSchemaFile(ctx, opener, file)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	at, ok := reader.(io.ReaderAt)
	if !ok {
		at = seekParquetReader{reader}
	}
	footer, _, err := readBoundedParquetFooter(ctx, at, file.SizeBytes)
	if err != nil {
		return nil, parquetScanReadError(err)
	}
	return historicalParquetFields(footer)
}

// openHistoricalSchemaFile opens the backing bytes of one versioned file and
// proves that they are still there at the committed size.
func openHistoricalSchemaFile(ctx context.Context, opener localObjectOpener, file models.StageTransactionFile) (io.ReadSeekCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, parquetScanReadError(err)
	}
	reader, err := opener.OpenLocalObject(file.PhysicalPath)
	if os.IsNotExist(err) {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path")
	}
	if err != nil {
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to open versioned file bytes")
	}
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		_ = reader.Close()
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to measure versioned file")
	}
	if size != file.SizeBytes {
		_ = reader.Close()
		return nil, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_file_size_mismatch: versioned file is %d bytes on disk but %d bytes in its committed file record", size, file.SizeBytes)
	}
	return reader, nil
}

// Read flat primitive schema elements directly from the bounded raw footer.
// The generic converter also descends into nested types and Arrow-specific
// metadata. Neither is needed for the supported historical schema profile.
func historicalParquetFields(meta *metadata.FileMetaData) ([]historicalSchemaField, error) {
	footer := meta.FileMetaData
	raw := footer.Schema
	if (footer.Version != 1 && footer.Version != 2) || footer.NumRows < 0 || len(raw) < 2 ||
		raw[0].IsSetType() || raw[0].IsSetLogicalType() || raw[0].IsSetConvertedType() ||
		int(raw[0].GetNumChildren()) != len(raw)-1 {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_schema_unsupported: expected a flat Parquet schema")
	}
	fields := make([]historicalSchemaField, 0, len(raw)-1)
	seen := make(map[string]struct{}, len(raw)-1)
	convertedTypes := schema.ConvertedTypes
	for _, field := range raw[1:] {
		name := field.Name
		_, duplicate := seen[name]
		repetition := parquet.Repetition(field.GetRepetitionType())
		if name == "" || !utf8.ValidString(name) || duplicate || !field.IsSetType() || field.GetNumChildren() != 0 ||
			!field.IsSetRepetitionType() || (repetition != parquet.Repetitions.Required && repetition != parquet.Repetitions.Optional) {
			return nil, datasetRowsErr(http.StatusUnprocessableEntity,
				"dataset_schema_unsupported: expected uniquely named flat, nonrepeated Parquet columns")
		}

		physical := parquet.Type(field.GetType())
		converted := convertedTypes.None
		if field.IsSetConvertedType() {
			converted = schema.ConvertedType(field.GetConvertedType())
		}
		precision, scale := field.GetPrecision(), field.GetScale()
		typeName := ""
		if logical := field.LogicalType; logical != nil {
			annotation := convertedTypes.None
			if logical.CountSetFieldsLogicalType() == 1 {
				switch {
				case logical.STRING != nil:
					annotation = convertedTypes.UTF8
				case logical.INTEGER != nil:
					annotation = historicalParquetInteger(logical.INTEGER.GetBitWidth(), logical.INTEGER.GetIsSigned())
				case logical.DATE != nil:
					annotation = convertedTypes.Date
				case logical.DECIMAL != nil:
					annotation = convertedTypes.Decimal
					precision, scale = logical.DECIMAL.GetPrecision(), logical.DECIMAL.GetScale()
					if (field.IsSetPrecision() && precision != field.GetPrecision()) ||
						(field.IsSetScale() && scale != field.GetScale()) {
						annotation = convertedTypes.None
					}
				case logical.TIMESTAMP != nil:
					stamp := logical.TIMESTAMP
					unit := stamp.GetUnit()
					if physical == parquet.Types.Int64 && unit != nil && unit.CountSetFieldsTimeUnit() == 1 {
						converter, suffix := convertedTypes.None, "NANOS"
						switch {
						case unit.GetMILLIS() != nil:
							converter, suffix = convertedTypes.TimestampMillis, "MILLIS"
						case unit.GetMICROS() != nil:
							converter, suffix = convertedTypes.TimestampMicros, "MICROS"
						}
						if converted == convertedTypes.None || converted == converter {
							typeName = historicalTimestamp(suffix, stamp.GetIsAdjustedToUTC())
						}
					}
				}
			}
			if typeName == "" {
				if annotation == convertedTypes.None || (converted != convertedTypes.None && converted != annotation) {
					return nil, unsupportedHistoricalParquetField(name)
				}
				converted = annotation
			}
		}
		if typeName == "" {
			typeName = historicalParquetType(physical, converted, field.GetTypeLength(), precision, scale)
		}
		if typeName == "" {
			return nil, unsupportedHistoricalParquetField(name)
		}
		fields = append(fields, historicalSchemaField{Name: name, FieldType: typeName, Nullable: repetition == parquet.Repetitions.Optional})
		seen[name] = struct{}{}
	}
	return fields, nil
}

func unsupportedHistoricalParquetField(name string) error {
	return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_schema_unsupported: unsupported Parquet type for field %q", name)
}

func historicalParquetInteger(width int8, signed bool) schema.ConvertedType {
	types := schema.ConvertedTypes
	switch width {
	case 8:
		if signed {
			return types.Int8
		}
		return types.Uint8
	case 16:
		if signed {
			return types.Int16
		}
		return types.Uint16
	case 32:
		if signed {
			return types.Int32
		}
		return types.Uint32
	case 64:
		if signed {
			return types.Int64
		}
		return types.Uint64
	default:
		return types.None
	}
}

func historicalParquetType(physical parquet.Type, converted schema.ConvertedType, length, precision, scale int32) string {
	types, annotations := parquet.Types, schema.ConvertedTypes
	switch converted {
	case annotations.None:
		switch physical {
		case types.Boolean:
			return "BOOLEAN"
		case types.Int32:
			return "INTEGER"
		case types.Int64:
			return "LONG"
		case types.Float:
			return "FLOAT"
		case types.Double:
			return "DOUBLE"
		case types.ByteArray:
			return "BINARY"
		case types.FixedLenByteArray:
			if length > 0 {
				return fmt.Sprintf("BINARY(%d)", length)
			}
		}
	case annotations.UTF8:
		if physical == types.ByteArray {
			return "STRING"
		}
	case annotations.Date:
		if physical == types.Int32 {
			return "DATE"
		}
	case annotations.Decimal:
		if precision > 0 && scale >= 0 && scale <= precision &&
			(physical == types.ByteArray || (physical == types.FixedLenByteArray && length > 0) ||
				(physical == types.Int32 && precision <= 9) || (physical == types.Int64 && precision <= 18)) {
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
	case annotations.TimestampMillis, annotations.TimestampMicros:
		if physical == types.Int64 {
			unit := "MICROS"
			if converted == annotations.TimestampMillis {
				unit = "MILLIS"
			}
			return historicalTimestamp(unit, true)
		}
	case annotations.Int8, annotations.Int16, annotations.Int32, annotations.Uint8, annotations.Uint16, annotations.Uint32:
		if physical == types.Int32 {
			switch converted {
			case annotations.Int8:
				return "BYTE"
			case annotations.Int16:
				return "SHORT"
			case annotations.Int32:
				return "INTEGER"
			default:
				return strings.Replace(converted.String(), "UINT_", "UINT", 1)
			}
		}
	case annotations.Int64, annotations.Uint64:
		if physical == types.Int64 {
			if converted == annotations.Uint64 {
				return "UINT64"
			}
			return "LONG"
		}
	}
	return ""
}

func historicalTimestamp(unit string, utc bool) string {
	zone := "LOCAL"
	if utc {
		zone = "UTC"
	}
	return "TIMESTAMP(" + unit + "," + zone + ")"
}
