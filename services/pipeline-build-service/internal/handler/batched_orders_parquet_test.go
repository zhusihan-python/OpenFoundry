package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

const testParquetOrder = `{"order_id":9007199254740993,"amount":"12345678901234.5000","order_date":"2024-02-29","ordered_at":"2024-02-29T00:05:00.123456+05:45","source_time_zone":"Asia/Kathmandu","note":null}`

func setBatchedOrdersParquetSpec(t *testing.T, h *batchedHarness) {
	t.Helper()
	h.specRepo.specs[0] = batchedGrammarSpec(t, func(p *batchedPayload) {
		p.Output = batchedPayloadOutput{
			LogicalPath: "orders/orders.parquet", Format: "parquet", SchemaVersion: "orders.v1",
		}
	}, h.inputRID, h.outputRID)
}

func TestBatchedParquetOptIn(t *testing.T) {
	in, out := uuid.NewString(), uuid.NewString()
	for _, tc := range []struct {
		name   string
		output batchedPayloadOutput
		valid  bool
	}{
		{"legacy default", batchedPayloadOutput{}, true},
		{"explicit opt-in", batchedPayloadOutput{"orders.parquet", "parquet", "orders.v1"}, true},
		{"ambiguous suffix", batchedPayloadOutput{LogicalPath: "orders.parquet"}, false},
		{"disabled schema without format", batchedPayloadOutput{"orders.ndjson", "", "orders.v1"}, false},
		{"missing schema version", batchedPayloadOutput{"orders.parquet", "parquet", ""}, false},
		{"unsupported schema", batchedPayloadOutput{"orders.parquet", "parquet", "orders.v2"}, false},
		{"unsupported format", batchedPayloadOutput{"orders.csv", "csv", "orders.v1"}, false},
		{"wrong suffix", batchedPayloadOutput{"orders.ndjson", "parquet", "orders.v1"}, false},
		{"extension without file name", batchedPayloadOutput{"orders/.parquet", "parquet", "orders.v1"}, false},
		{"missing path", batchedPayloadOutput{"", "parquet", "orders.v1"}, false},
		{"reserved subtree", batchedPayloadOutput{"transactions/orders.parquet", "parquet", "orders.v1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := batchedGrammarSpec(t, func(p *batchedPayload) { p.Output = tc.output }, in, out)
			cfg, err := parseBatchedSpec([]models.JobSpec{spec}, models.CreateBuildRequest{OutputDatasetRIDs: []string{out}},
				resolver.ResolveBuildArgs{BuildBranch: "main", TriggerKind: "MANUAL"}, uuid.New())
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, tc.output.Format == "parquet", batchedParquetPath(cfg.LogicalPath))
			} else {
				require.Error(t, err)
				require.Nil(t, cfg)
			}
		})
	}
}

func TestBatchedParquetPublishesAndReplaysFile(t *testing.T) {
	records := [][]byte{
		[]byte(strings.Replace(testParquetOrder, `"note":null`, `"note":""`, 1)),
		[]byte(testParquetOrder),
	}
	h := newBatchedHarness(t, records)
	setBatchedOrdersParquetSpec(t, h)
	buildID, _ := h.createBuild()
	dropACK := http.StatusServiceUnavailable
	h.dvs.failCommitOnceWith = &dropACK
	first := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusServiceUnavailable, first.Code, first.Body.String())
	require.Len(t, h.dvs.commits, 1)
	cfg, err := h.store.GetBatchedConfig(t.Context(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageOutcomeUnknown, cfg.Stage)
	require.Equal(t, "orders/orders.parquet", cfg.LogicalPath)
	require.Equal(t, h.dvs.commits[0], string(cfg.CommitRequestBody))
	var intent batchedCommitRequest
	require.NoError(t, json.Unmarshal(cfg.CommitRequestBody, &intent))
	require.Len(t, intent.Files, 1)
	output := intent.Files[0]
	require.Equal(t, "STAGED_ADD", output.Operation)
	require.Equal(t, "application/vnd.apache.parquet", output.ContentType)
	require.Equal(t, "orders.v1", output.Metadata["schema_version"])
	body := h.dvs.stageBodies[intent.ReservationID.String()]
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(body)), output.SHA256)
	require.EqualValues(t, len(body), output.SizeBytes)

	reader := openOrdersParquet(t, body)
	require.EqualValues(t, 2, reader.NumRows())
	footerVersion := reader.MetaData().KeyValueMetadata().FindValue("openfoundry.schema_version")
	require.NotNil(t, footerVersion)
	require.Equal(t, "orders.v1", *footerVersion)
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	table, err := arrowReader.ReadTable(t.Context())
	require.NoError(t, err)
	defer table.Release()
	require.Equal(t, int64(9007199254740993), table.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
	require.Equal(t, decimal128.FromI64(123456789012345000), table.Column(1).Data().Chunk(0).(*array.Decimal128).Value(0))
	require.Equal(t, arrow.Date32FromTime(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)), table.Column(2).Data().Chunk(0).(*array.Date32).Value(0))
	atUTC := time.Date(2024, 2, 28, 18, 20, 0, 123456000, time.UTC)
	require.Equal(t, arrow.Timestamp(atUTC.UnixMicro()), table.Column(3).Data().Chunk(0).(*array.Timestamp).Value(0))
	require.Equal(t, "Asia/Kathmandu", table.Column(4).Data().Chunk(0).(*array.String).Value(0))
	notes := table.Column(5).Data().Chunk(0).(*array.String)
	require.False(t, notes.IsNull(0))
	require.Empty(t, notes.Value(0))
	require.True(t, notes.IsNull(1))

	retry := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, retry.Code, retry.Body.String())
	require.Len(t, h.dvs.commits, 1)
	require.Equal(t, 1, h.dvs.stageCalls)
	require.Equal(t, 2, h.dvs.commitCalls)
}

func TestBatchedParquetStageRejectsWrongFormatReceipt(t *testing.T) {
	h := newBatchedHarness(t, [][]byte{[]byte(testParquetOrder)})
	setBatchedOrdersParquetSpec(t, h)
	buildID, _ := h.createBuild()
	h.dvs.stageReceiptMutator = func(receipt map[string]any) { delete(receipt, "format") }
	response := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "batched_stage_receipt_mismatch")
	require.Zero(t, h.dvs.commitCalls)
	require.Len(t, h.versioning.abortedTargets, 1)
}

func TestDecodeParquetOrderRejectsLossyOrAmbiguousValues(t *testing.T) {
	for _, tc := range []struct{ from, to, want string }{
		{`9007199254740993`, `9223372036854775808`, "signed 64-bit JSON integer"},
		{`9007199254740993`, `9e2`, "signed 64-bit JSON integer"},
		{`9007199254740993`, `null`, "signed 64-bit JSON integer"},
		{`"12345678901234.5000"`, `"123456789012345.5000"`, "invalid DECIMAL"},
		{`"12345678901234.5000"`, `"1.23456"`, "invalid DECIMAL"},
		{`"12345678901234.5000"`, `1.2345`, "invalid DECIMAL"},
		{`2024-02-29"`, `2024-02-30"`, "valid YYYY-MM-DD"},
		{`123456+05:45`, `123456001+05:45`, "microsecond-aligned"},
		{`123456+05:45`, `123456+24:00`, "microsecond-aligned"},
		{`123456+05:45`, `1234560001+05:45`, "microsecond-aligned"},
		{`,"note":null`, ``, "all six fields"},
		{`"note":null`, `"note":"" ,"note":null`, "duplicate note"},
		{`"note":null`, `"note":[]`, "scalar value"},
		{`"note":null`, `"note":true`, "string or null"},
		{`"note":null`, `"Note":null`, "unexpected field"},
		{`"note":null`, `"note":"\uD800"`, "unpaired Unicode surrogate"},
		{`"note":null`, "\"note\":\"\xff\"", "invalid UTF-8"},
		{`"Asia/Kathmandu"`, `null`, "source_time_zone must be a string"},
	} {
		_, err := decodeParquetOrder([]byte(strings.Replace(testParquetOrder, tc.from, tc.to, 1)))
		require.ErrorContains(t, err, tc.want, "replacement %q", tc.to)
	}
	decoded, err := decodeParquetOrder([]byte(strings.Replace(testParquetOrder, `"note":null`, `"note":"\uD83D\uDE00 \ufffd \\uD800"`, 1)))
	require.NoError(t, err)
	require.Equal(t, `😀 � \uD800`, *decoded.note)
}

func TestStreamOrdersParquetBatchLimitsAndTypedEmpty(t *testing.T) {
	long := strings.Replace(testParquetOrder, `"note":null`, `"note":"`+strings.Repeat("x", 900_000)+`"`, 1)
	for _, tc := range []struct {
		name string
		n    int
		row  string
		want []int64
	}{
		{"row cap", 1025, testParquetOrder, []int64{1024, 1}},
		{"byte cap", 6, long, []int64{4, 2}},
		{"typed empty", 0, "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			digest := sha256.New()
			size, rows, err := streamOrdersParquet(t.Context(), &output, digest, batchedMaxRecordBytes, func(emit func([]byte) error) error {
				row := []byte(tc.row)
				for range tc.n {
					if err := emit(row); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, tc.n, rows)
			require.EqualValues(t, output.Len(), size)
			wantDigest := sha256.Sum256(output.Bytes())
			require.Equal(t, wantDigest[:], digest.Sum(nil))
			reader := openOrdersParquet(t, output.Bytes())
			require.EqualValues(t, len(tc.want), reader.NumRowGroups())
			for index, rows := range tc.want {
				require.Equal(t, rows, reader.RowGroup(index).NumRows())
			}
			require.EqualValues(t, tc.n, reader.NumRows())
			arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			require.NoError(t, err)
			schema, err := arrowReader.Schema()
			require.NoError(t, err)
			want := ordersParquetSchema()
			require.Equal(t, want.NumFields(), schema.NumFields())
			for i, field := range want.Fields() {
				got := schema.Field(i)
				require.Equal(t, field.Name, got.Name)
				require.Equal(t, field.Nullable, got.Nullable)
				require.True(t, arrow.TypeEqual(field.Type, got.Type), "field %s type %s, want %s", field.Name, got.Type, field.Type)
			}
		})
	}
}

func TestStreamOrdersParquetDoesNotFinalizeAfterFailure(t *testing.T) {
	for _, tc := range []string{"scan error", "invalid row", "context canceled", "write error"} {
		t.Run(tc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			destination := &ordersFailWriter{}
			failed := errors.New("injected " + tc)
			if tc == "write error" {
				destination.failure = failed
			}
			_, _, err := streamOrdersParquet(ctx, destination, sha256.New(), batchedMaxRecordBytes, func(emit func([]byte) error) error {
				for range ordersBatchRows + 1 {
					if err := emit([]byte(testParquetOrder)); err != nil {
						return err
					}
				}
				switch tc {
				case "invalid row":
					return emit([]byte(`{}`))
				case "context canceled":
					cancel()
					return nil
				default:
					return failed
				}
			})
			switch tc {
			case "invalid row":
				require.ErrorContains(t, err, "batched_record_invalid")
			case "context canceled":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.ErrorIs(t, err, failed)
			}
			require.True(t, bytes.HasPrefix(destination.Bytes(), []byte("PAR1")))
			require.False(t, bytes.HasSuffix(destination.Bytes(), []byte("PAR1")), "a failed stream cannot carry a valid footer")
		})
	}
}

type ordersFailWriter struct {
	bytes.Buffer
	failure error
}

func (w *ordersFailWriter) Write(p []byte) (int, error) {
	if w.failure != nil && w.Len() > 4 {
		return 0, w.failure
	}
	return w.Buffer.Write(p)
}

func openOrdersParquet(t *testing.T, data []byte) *file.Reader {
	t.Helper()
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	return reader
}
