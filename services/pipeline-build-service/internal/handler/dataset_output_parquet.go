package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

// Match the DVS inline publication defaults. The writer's batch caps
// also keep rows below DVS's default record limit (4096 groups of at
// most 1024 rows). DVS still applies any tighter deployment settings.
// Larger outputs belong to the streaming STAGED_ADD protocol.
const (
	inlineParquetMaxBytes        = 64 << 20
	inlineParquetPreviewMaxBytes = 256 << 10
	inlineParquetMaxWorkBytes    = 128 << 20
	inlineParquetMaxFooterBytes  = 8 << 20
)

func datasetOutputConfigFields(payload []byte) (map[string]json.RawMessage, map[string]json.RawMessage) {
	var fields, nested map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return nil, nil
	}
	if raw := fields["_output"]; raw != nil {
		_ = json.Unmarshal(raw, &nested)
	}
	return fields, nested
}

// PayloadDeclaresDatasetOutputSchema participates in canonical JobSpec
// deduplication. Presence counts even when the declaration is malformed.
func PayloadDeclaresDatasetOutputSchema(payload []byte) bool {
	fields, nested := datasetOutputConfigFields(payload)
	return fields["output_schema_version"] != nil || nested["output_schema_version"] != nil
}

// BindDatasetOutputSchema restores a typed publication declaration from
// the persisted JobSpec or graph config. Existing specs without this new
// declaration keep their output transaction and encoding behavior.
func BindDatasetOutputSchema(payload json.RawMessage, outputs []executor.OutputTransaction) error {
	fields, nested := datasetOutputConfigFields(payload)
	if _, nestedDeclares := nested["output_schema_version"]; nestedDeclares {
		if fields["output_schema_version"] != nil {
			return errors.New("dataset_output_config_invalid: output_schema_version is declared twice")
		}
		if fields["file_format"] != nil || fields["logical_path"] != nil || fields["write_mode"] != nil {
			return errors.New("dataset_output_config_invalid: keep typed output settings together inside _output")
		}
		fields = nested
	} else if fields["output_schema_version"] != nil && !parseOutputDatasetConfig(fields["_output"]).empty() {
		return errors.New("dataset_output_config_invalid: declare output_schema_version inside _output")
	}
	if fields["output_schema_version"] == nil {
		return nil
	}
	if len(outputs) == 0 {
		return errors.New("dataset_output_config_invalid: output_schema_version requires a dataset output")
	}
	var version string
	if err := json.Unmarshal(fields["output_schema_version"], &version); err != nil || version != ordersSchemaVersion {
		return fmt.Errorf("dataset_output_config_invalid: output_schema_version must be %q", ordersSchemaVersion)
	}
	var format, logicalPath, writeMode string
	for _, entry := range []struct {
		name   string
		target *string
	}{
		{"file_format", &format},
		{"logical_path", &logicalPath},
		{"write_mode", &writeMode},
	} {
		if value, exists := fields[entry.name]; exists {
			if err := json.Unmarshal(value, entry.target); err != nil {
				return fmt.Errorf("dataset_output_config_invalid: %s must be a string: %w", entry.name, err)
			}
		}
	}
	for i := range outputs {
		tx := &outputs[i]
		tx.OutputSchemaVersion = version
		tx.FileFormat = format
		tx.LogicalPath = logicalPath
		if writeMode != "" {
			tx.WriteMode = writeMode
		}
		if _, err := selectTypedDatasetOutput(*tx); err != nil {
			return err
		}
	}
	return nil
}

func selectTypedDatasetOutput(tx executor.OutputTransaction) (bool, error) {
	if tx.OutputSchemaVersion == "" {
		return false, nil
	}
	if tx.OutputSchemaVersion != ordersSchemaVersion {
		return false, fmt.Errorf("dataset_output_config_invalid: unsupported output_schema_version %q", tx.OutputSchemaVersion)
	}
	datasetID, err := batchedDatasetUUID(tx.DatasetRID)
	if err != nil || datasetID == uuid.Nil || (tx.OutputKind != "" && tx.OutputKind != "dataset") {
		return false, errors.New("dataset_output_config_invalid: output_schema_version requires a dataset output")
	}

	logicalPath := strings.Trim(strings.TrimSpace(tx.LogicalPath), "/")
	isNDJSON := strings.HasSuffix(logicalPath, ".ndjson")
	isParquet := batchedParquetPath(logicalPath)
	if logicalPath != "" && !isNDJSON && !isParquet {
		return false, errors.New("dataset_output_config_invalid: output_schema_version requires a .parquet or .ndjson logical_path")
	}
	switch strings.ToUpper(strings.TrimSpace(tx.FileFormat)) {
	case "NDJSON", "TEXT":
		if isParquet {
			return false, errors.New("dataset_output_config_invalid: NDJSON output requires a .ndjson logical_path")
		}
		return false, nil
	case "":
		if isNDJSON {
			return false, nil
		}
	case "PARQUET":
		if isNDJSON {
			return false, errors.New("dataset_output_config_invalid: Parquet output requires a .parquet logical_path")
		}
	default:
		return false, fmt.Errorf("dataset_output_config_invalid: unsupported file_format %q for %s", tx.FileFormat, ordersSchemaVersion)
	}
	if tx.LogicalPath != "" && (!validBatchedObjectPath(tx.LogicalPath) ||
		!batchedParquetPath(tx.LogicalPath) || strings.TrimSpace(tx.LogicalPath) != tx.LogicalPath ||
		strings.SplitN(tx.LogicalPath, "/", 2)[0] == "transactions") {
		return false, errors.New("dataset_output_config_invalid: Parquet logical_path must be a canonical .parquet object key")
	}
	if transactionType := defaultString(strings.ToUpper(tx.WriteMode), "SNAPSHOT"); transactionType != "SNAPSHOT" {
		return false, errors.New("dataset_output_config_invalid: Parquet output requires SNAPSHOT write_mode")
	}
	return true, nil
}

// The supported production runners materialize rows using raw JSON values.
// Keeping that representation avoids lossy number conversions and makes
// malformed or sample-only metadata distinguishable from a real empty result.
func encodeInlineOrdersParquet(ctx context.Context, result executor.NodeResult) ([]byte, []map[string]json.RawMessage, string, error) {
	rows, ok := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	if !ok || rows == nil {
		return nil, nil, "", errors.New("dataset_output_parquet_invalid: Parquet requires complete data_rows")
	}
	var dst inlineParquetBuffer
	digest := sha256.New()
	_, _, err := streamOrdersParquet(ctx, &dst, digest, batchedMaxRecordBytes, func(emit func([]byte) error) error {
		var record bytes.Buffer
		encoder := json.NewEncoder(&record)
		encoder.SetEscapeHTML(false)
		for _, row := range rows {
			record.Reset()
			if err := encoder.Encode(row); err != nil {
				return err
			}
			if err := emit(bytes.TrimSuffix(record.Bytes(), []byte{'\n'})); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("dataset_output_parquet_invalid: %w", err)
	}
	if err := validateInlineParquetWork(dst.Bytes(), inlineParquetMaxWorkBytes); err != nil {
		return nil, nil, "", fmt.Errorf("dataset_output_parquet_invalid: %w", err)
	}
	return dst.Bytes(), rows, hex.EncodeToString(digest.Sum(nil)), nil
}

// Inspect only trusted writer metadata. DVS repeats this encoded-size plus
// uncompressed-group-size check when validating bytes independently.
func validateInlineParquetWork(content []byte, maxWorkBytes int64) error {
	if len(content) < 8 {
		return errors.New("invalid generated Parquet footer")
	}
	if footerBytes := binary.LittleEndian.Uint32(content[len(content)-8:]); footerBytes > inlineParquetMaxFooterBytes {
		return fmt.Errorf("inline Parquet footer exceeds the %d-byte limit", inlineParquetMaxFooterBytes)
	}
	reader, err := file.NewParquetReader(bytes.NewReader(content))
	if err != nil {
		return fmt.Errorf("inspect generated Parquet footer: %w", err)
	}
	defer func() { _ = reader.Close() }()
	remaining := maxWorkBytes - int64(len(content))
	for ordinal := range reader.NumRowGroups() {
		remaining -= reader.MetaData().RowGroup(ordinal).TotalByteSize()
		if remaining < 0 {
			break
		}
	}
	if remaining < 0 {
		return fmt.Errorf("inline Parquet exceeds the %d-byte validation work limit", maxWorkBytes)
	}
	return nil
}

type inlineParquetBuffer struct {
	bytes.Buffer
}

func (b *inlineParquetBuffer) Write(p []byte) (int, error) {
	if len(p) > inlineParquetMaxBytes-b.Len() {
		return 0, fmt.Errorf("inline Parquet output exceeds the %d-byte publication limit", inlineParquetMaxBytes)
	}
	return b.Buffer.Write(p)
}

// Keep a metadata preview within the inline JSON envelope's overhead budget.
// The data file always contains the complete result.
func inlineParquetPreview(columns []string, rows []map[string]json.RawMessage) [][]json.RawMessage {
	preview := make([][]json.RawMessage, 0, min(len(rows), 100))
	var size int
	for i := range min(len(rows), 100) {
		row := orderedPreviewRows(columns, rows[i:i+1], 1)[0]
		// Preview consumers parse JSON numbers as binary64. Use decimal
		// text for the validated order_id only in the browser sample.
		id := strings.TrimSpace(string(row[0]))
		row[0] = json.RawMessage(`"` + id + `"`)
		encoded, err := json.Marshal(row)
		if err != nil || len(encoded)+1 > inlineParquetPreviewMaxBytes-size {
			break
		}
		size += len(encoded) + 1
		preview = append(preview, row)
	}
	return preview
}
