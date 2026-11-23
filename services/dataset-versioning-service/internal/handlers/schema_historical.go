package handlers

import (
	"context"
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
	Fields        []historicalSchemaField `json:"fields"`
}

func (h *Handlers) getHistoricalSchema(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, query url.Values) {
	if !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	branch := strings.TrimSpace(query.Get("branch"))
	version, err := strconv.ParseInt(strings.TrimSpace(query.Get("version")), 10, 32)
	if len(query) != 2 || len(query["branch"]) != 1 || len(query["version"]) != 1 || branch == "" || err != nil || version < 1 {
		writeJSONErr(w, http.StatusBadRequest, "versioned schema requires exactly one branch and positive integer version")
		return
	}
	target, ok := h.resolveScanTarget(w, r, datasetID, branch, int32(version))
	if !ok {
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
	for _, file := range files {
		if !validStagedParquetPath(file.LogicalPath) || file.Operation != models.FileOperationAdd {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
				"dataset_schema_format_unsupported: versioned schemas require Parquet ADD files"))
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
	writeJSON(w, http.StatusOK, historicalSchemaResponse{
		DatasetID: datasetID, Branch: branch, Version: int32(version),
		TransactionID: target.txn.ID, Fields: fields,
	})
}

func readHistoricalParquetFields(ctx context.Context, opener localObjectOpener, file models.StageTransactionFile) ([]historicalSchemaField, error) {
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
	defer func() { _ = reader.Close() }()
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to measure versioned file")
	}
	if size != file.SizeBytes {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_file_size_mismatch: versioned file is %d bytes on disk but %d bytes in its committed file record", size, file.SizeBytes)
	}
	at, ok := reader.(io.ReaderAt)
	if !ok {
		at = seekParquetReader{reader}
	}
	footer, _, err := readBoundedParquetFooter(ctx, at, size)
	if err != nil {
		return nil, parquetScanReadError(err)
	}
	return historicalParquetFields(footer)
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
