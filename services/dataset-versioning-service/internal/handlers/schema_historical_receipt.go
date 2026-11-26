package handlers

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// readHistoricalReceiptFields answers the schema of a version that is one
// NDJSON or CSV output. Neither carries a schema footer, so the schema is the
// one the publisher declared in the formal schema field of its keyed commit,
// which the service serialized into the immutable receipt of that
// transaction. Transaction and file metadata are never read: a caller's
// free-form metadata can carry a schema key.
func (h *Handlers) readHistoricalReceiptFields(w http.ResponseWriter, ctx context.Context, opener localObjectOpener,
	datasetID uuid.UUID, branch string, txn *models.RuntimeTransaction, file models.StageTransactionFile) ([]historicalSchemaField, bool) {
	commit, err := h.Repo.GetTransactionOutputCommit(ctx, datasetID, txn.ID, branch)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to read the output commit receipt"))
		return nil, false
	}
	if commit == nil || commit.Schema == nil || commit.Schema.FileFormat != models.FileFormatText || commit.Schema.Fields == nil ||
		commit.DatasetID != datasetID || commit.Transaction.ID != txn.ID ||
		len(commit.Files) != 1 || commit.Files[0].Path != file.LogicalPath || commit.Files[0].SizeBytes != file.SizeBytes {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_schema_format_unsupported: the NDJSON or CSV output of this version has no schema in its commit receipt"))
		return nil, false
	}
	fields, err := historicalReceiptFields(commit.Schema.Fields)
	if err != nil {
		writeDatasetRowsError(w, err)
		return nil, false
	}
	if !h.validateScanStagedObject(w, ctx, datasetID, txn, file.LogicalPath, &file) {
		return nil, false
	}
	reader, err := openHistoricalSchemaFile(ctx, opener, file)
	if err != nil {
		writeDatasetRowsError(w, err)
		return nil, false
	}
	_ = reader.Close()
	return fields, true
}

// historicalReceiptFields keeps the scalar type labels that need no type
// parameter, which is all the versioned schema profile can state. Like a
// Parquet file without columns, a declared schema without fields is not a
// supported schema.
func historicalReceiptFields(columns []models.Field) ([]historicalSchemaField, error) {
	invalid := datasetRowsErr(http.StatusUnprocessableEntity,
		"dataset_schema_unsupported: expected uniquely named scalar fields without type parameters")
	if len(columns) == 0 {
		return nil, invalid
	}
	fields := make([]historicalSchemaField, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		_, duplicate := seen[column.Name]
		if column.Name == "" || duplicate || column.Precision != nil || column.Scale != nil ||
			column.ArraySubType != nil || column.MapKeyType != nil || column.MapValueType != nil || len(column.SubSchemas) != 0 {
			return nil, invalid
		}
		switch column.Type {
		case models.FieldTypeBoolean, models.FieldTypeByte, models.FieldTypeShort, models.FieldTypeInteger, models.FieldTypeLong,
			models.FieldTypeFloat, models.FieldTypeDouble, models.FieldTypeString, models.FieldTypeDate:
		default:
			return nil, invalid
		}
		seen[column.Name] = struct{}{}
		fields = append(fields, historicalSchemaField{Name: column.Name, FieldType: string(column.Type), Nullable: column.Nullable})
	}
	return fields, nil
}
