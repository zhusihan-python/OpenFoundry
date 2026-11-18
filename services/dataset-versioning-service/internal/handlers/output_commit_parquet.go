package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// The inline HTTP body already caps encoded content. Also bound the
// aggregate of the Parquet bytes and its declared decompressed work.
const maxInlineParquetWorkBytes int64 = 128 << 20

var (
	errInlineParquetInvalid     = errors.New("invalid inline orders.v1 Parquet content")
	errInlineParquetWorkLimit   = errors.New("inline Parquet exceeds the validation work or record limit")
	errInlineParquetInterrupted = errors.New("inline Parquet validation was interrupted")
)

// A typed inline commit uses the ordinary output transaction and
// receipt. This prepares server-owned metadata before any bytes are
// written. Keyed callers run it only when a receipt is absent.
func (h *Handlers) prepareTypedInlineOutput(ctx context.Context, body *models.CommitDatasetOutputRequest) error {
	if body.OutputSchemaVersion == nil {
		return nil
	}
	var version string
	if err := json.Unmarshal(*body.OutputSchemaVersion, &version); err != nil || version != stagedParquetSchemaVersion {
		return errors.New("output_schema_version must be orders.v1")
	}
	if stagedCommitRequested(*body) {
		return errors.New("output_schema_version cannot be combined with staged commit fields")
	}
	if body.TransactionType != "" && body.TransactionType != models.TransactionTypeSnapshot {
		return errors.New("output_schema_version requires a SNAPSHOT transaction")
	}
	if len(body.Files) != 1 || body.Files[0].ContentBase64 == nil || body.Files[0].Operation != models.FileOperationAdd {
		return errors.New("output_schema_version requires exactly one content-bearing ADD file")
	}
	file := &body.Files[0]
	if !validStagedParquetPath(file.LogicalPath) || reservedServerObjectSubtree(file.LogicalPath) {
		return errors.New("output_schema_version requires a canonical .parquet logical_path")
	}
	if file.ContentType == nil || *file.ContentType != stagedParquetContentType {
		return errors.New("output_schema_version requires files[].content_type " + stagedParquetContentType)
	}
	size := int64(len(file.ContentBase64))
	if file.SizeBytes != size {
		return errors.New("size_bytes must match content_base64 length when content is provided")
	}
	if limit := h.outputCommitContentLimit(); size > limit {
		return fmt.Errorf("%w: %d bytes exceeds %d", errOutputCommitContentTooLarge, size, limit)
	}

	records, err := inspectInlineParquet(ctx, file.ContentBase64, h.stagedOutputMaxRecords())
	if err != nil {
		return err
	}
	sum := sha256.Sum256(file.ContentBase64)
	tags := map[string]any{
		"content_type":   stagedParquetContentType,
		"format":         stagedParquetFormat,
		"schema_version": stagedParquetSchemaVersion,
		"sha256":         hex.EncodeToString(sum[:]),
		"size_bytes":     size,
		"record_count":   records,
		"row_count":      records,
	}
	file.Metadata, err = stampInlineParquetMetadata(file.Metadata, tags)
	if err != nil {
		return err
	}
	body.Metadata, err = stampInlineParquetMetadata(body.Metadata, tags)
	if err != nil {
		return err
	}
	body.Schema = parquetOrdersSchema()
	body.Format = &body.Schema.FileFormat
	return nil
}

func stampInlineParquetMetadata(raw models.JSONValue, tags map[string]any) (models.JSONValue, error) {
	var meta map[string]any
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, err
		}
	}
	if meta == nil {
		meta = make(map[string]any, len(tags))
	}
	for key, value := range tags {
		meta[key] = value
	}
	return json.Marshal(meta)
}

func inspectInlineParquet(ctx context.Context, content []byte, maxRecords int64) (int64, error) {
	if ctx.Err() != nil {
		return 0, errInlineParquetInterrupted
	}
	reader := bytes.NewReader(content)
	size := int64(len(content))
	if size > maxInlineParquetWorkBytes {
		return 0, errInlineParquetWorkLimit
	}
	meta, _, err := readStagedParquetMetadata(ctx, reader, size, maxRecords)
	if err != nil {
		if ctx.Err() != nil {
			return 0, errInlineParquetInterrupted
		}
		if errors.Is(err, errStagedRecordCountLimit) {
			return 0, errInlineParquetWorkLimit
		}
		return 0, errInlineParquetInvalid
	}
	work := size
	for _, group := range meta.RowGroups {
		if group.TotalByteSize > maxInlineParquetWorkBytes-work {
			return 0, errInlineParquetWorkLimit
		}
		work += group.TotalByteSize
	}
	var pages stagedParquetPageValidator
	for i := range meta.RowGroups {
		rows, err := readParquetOrdersGroup(ctx, &pages, reader, meta, uint32(i))
		if err != nil {
			if ctx.Err() != nil {
				return 0, errInlineParquetInterrupted
			}
			return 0, errInlineParquetInvalid
		}
		for _, row := range rows {
			if _, _, err := row.dates(); err != nil {
				return 0, errInlineParquetInvalid
			}
		}
	}
	if ctx.Err() != nil {
		return 0, errInlineParquetInterrupted
	}
	return meta.NumRows, nil
}
