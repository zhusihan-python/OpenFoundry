package handlers

import (
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func TestVerifiedInlineNDJSONCount(t *testing.T) {
	mime := "application/x-ndjson"
	foreignMIME := "application/octet-stream"
	input := []byte("{\"n\":1}\r\n{\"n\":2}")
	for _, tc := range []struct {
		name, path string
		mime       *string
		content    []byte
		valid      bool
		count      int64
	}{
		{"two objects with no trailing newline", "rows.ndjson", &mime, input, true, 2},
		{"one object with terminal newline", "rows.ndjson", &mime, []byte("{}\n"), true, 1},
		{"escaped newline is not another record", "rows.ndjson", &mime, []byte(`{"s":"a\nb"}`), true, 1},
		{"empty file", "rows.ndjson", &mime, []byte{}, true, 0},
		{"external file", "rows.ndjson", &mime, nil, false, 0},
		{"opaque content type", "rows.ndjson", &foreignMIME, input, false, 0},
		{"wrong extension", "rows.csv", &mime, input, false, 0},
		{"malformed record", "rows.ndjson", &mime, []byte("{}\n{"), false, 0},
		{"non-object record", "rows.ndjson", &mime, []byte("[]"), false, 0},
		{"blank record", "rows.ndjson", &mime, []byte("{}\n\n"), false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := models.CommitOutputFile{ContentType: tc.mime, ContentBase64: tc.content}
			got := verifiedInlineNDJSONCount(file, tc.path)
			if !tc.valid {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, tc.count, *got)
		})
	}
}

func TestOutputCommitFilesCountsUnreservedSnapshots(t *testing.T) {
	mime := "application/x-ndjson"
	opaque := "application/octet-stream"
	data := []byte("{}\n{\"n\":2}\n")
	cases := []struct {
		name     string
		txnType  models.TransactionType
		mime     *string
		content  []byte
		expected *int64
	}{
		{name: "implicit snapshot", mime: &mime, content: data, expected: countPointer(2)},
		{name: "snapshot", txnType: models.TransactionTypeSnapshot, mime: &mime, content: data, expected: countPointer(2)},
		{name: "empty snapshot", txnType: models.TransactionTypeSnapshot, mime: &mime, content: []byte{}, expected: countPointer(0)},
		{name: "update", txnType: models.TransactionTypeUpdate, mime: &mime, content: data},
		{name: "external file", mime: &mime},
		{name: "opaque file", mime: &opaque, content: data},
		{name: "malformed NDJSON", mime: &mime, content: []byte("{}\n{")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := models.CommitDatasetOutputRequest{
				TransactionType: tc.txnType,
				Metadata:        models.JSONValue(`{"row_count":991}`),
				Files: []models.CommitOutputFile{{
					LogicalPath:   "rows.ndjson",
					ContentType:   tc.mime,
					ContentBase64: tc.content,
					SizeBytes:     int64(len(tc.content)),
				}},
			}
			backing := outputCommitBacking{fsid: "local", baseDir: "store", limit: 1 << 20}
			_, entries, _, err := outputCommitFiles(uuid.New(), uuid.New(), body, backing)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, tc.expected, entries[0].VerifiedRowCount)
		})
	}
}

func countPointer(value int64) *int64 { return &value }
