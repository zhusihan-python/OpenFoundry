package handlers

import (
	"testing"

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
