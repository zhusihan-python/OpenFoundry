package handlers

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
)

// scanFS builds a LocalBackingFS over a temp dir and writes one object,
// returning the fs and the object key.
func scanFS(t *testing.T, content string) (*storageabstraction.LocalBackingFS, string) {
	t.Helper()
	fs := &storageabstraction.LocalBackingFS{BaseDir: "data", RootDir: t.TempDir()}
	key := "data/datasets/" + uuid.New().String() + "/transactions/" + uuid.New().String() + "/part-00000.ndjson"
	require.NoError(t, fs.WriteLocalObject(key, []byte(content)))
	return fs, key
}

func runScanPage(t *testing.T, fs *storageabstraction.LocalBackingFS, key, content string, offset int64, limit int) (scanPage, error) {
	t.Helper()
	return scanNDJSONPage(fs, key, int64(len(content)), offset, limit)
}

func TestScanCursorEncodeDecodeRoundTrip(t *testing.T) {
	in := scanCursor{
		datasetID: uuid.New(), branchID: uuid.New(), version: 7,
		txnID: uuid.New(), offset: 12345, path: "part-00000.ndjson",
	}
	out, err := decodeScanCursor(encodeScanCursor(in))
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestScanCursorDecodeRejects(t *testing.T) {
	valid := scanCursor{datasetID: uuid.New(), branchID: uuid.New(), version: 1, txnID: uuid.New(), offset: 3, path: "p.ndjson"}
	encoded := encodeScanCursor(valid)
	// Flip the first encoded character so the decoded magic breaks.
	first := encoded[0]
	if first == 'A' {
		first = 'B'
	} else {
		first = 'A'
	}
	wrongMagic := string(first) + encoded[1:]

	for name, raw := range map[string]string{
		"not base64":     "!!!!",
		"wrong magic":    wrongMagic,
		"truncated":      encoded[:len(encoded)-2],
		"trailing bytes": encoded + "AA",
	} {
		_, err := decodeScanCursor(raw)
		assert.Error(t, err, "cursor case %q must be rejected", name)
	}

	// A length-consistent cursor with a zero version is rejected too.
	bad := valid
	bad.version = 0
	_, err := decodeScanCursor(encodeScanCursor(bad))
	assert.Error(t, err, "zero version must be rejected")
}

func TestScanNDJSONPageEdges(t *testing.T) {
	t.Run("empty file", func(t *testing.T) {
		fs, key := scanFS(t, "")
		page, err := runScanPage(t, fs, key, "", 0, 100)
		require.NoError(t, err)
		assert.Empty(t, page.rows)
		assert.False(t, page.hasMore)
	})

	t.Run("size mismatch rejected", func(t *testing.T) {
		fs, key := scanFS(t, "{\"a\":1}\n")
		_, err := scanNDJSONPage(fs, key, 999, 0, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_file_size_mismatch")
	})

	t.Run("exactly full page has no cursor", func(t *testing.T) {
		content := "{\"a\":1}\n{\"a\":2}\n"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 0, 2)
		require.NoError(t, err)
		require.Len(t, page.rows, 2)
		assert.False(t, page.hasMore, "file ends exactly at the page boundary")
		assert.Equal(t, int64(len(content)), page.endOffset)
	})

	t.Run("full page with trailing data has cursor", func(t *testing.T) {
		content := "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 0, 2)
		require.NoError(t, err)
		require.Len(t, page.rows, 2)
		assert.True(t, page.hasMore)
		assert.Equal(t, int64(16), page.endOffset)

		next, err := runScanPage(t, fs, key, content, page.endOffset, 2)
		require.NoError(t, err)
		require.Len(t, next.rows, 1)
		assert.False(t, next.hasMore)
		assert.Equal(t, "{\"a\":3}", string(next.rows[0]), "continuation starts at the first unread record")
	})

	t.Run("blank lines skipped and consumed", func(t *testing.T) {
		content := "\n{\"a\":1}\n\n\n{\"a\":2}\n   \n"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 0, 10)
		require.NoError(t, err)
		require.Len(t, page.rows, 2)
		assert.False(t, page.hasMore)
		assert.Equal(t, int64(len(content)), page.endOffset)
	})

	t.Run("final record without newline", func(t *testing.T) {
		content := "{\"a\":1}\n{\"a\":2}"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 0, 10)
		require.NoError(t, err)
		require.Len(t, page.rows, 2)
		assert.False(t, page.hasMore)
	})

	t.Run("unaligned offset rejected", func(t *testing.T) {
		content := "{\"a\":1}\n{\"a\":2}\n"
		fs, key := scanFS(t, content)
		_, err := runScanPage(t, fs, key, content, 5, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not start a record")
	})

	t.Run("aligned mid-file offset works", func(t *testing.T) {
		content := "{\"a\":1}\n{\"a\":2}\n"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 8, 10)
		require.NoError(t, err)
		require.Len(t, page.rows, 1)
		assert.Equal(t, "{\"a\":2}", string(page.rows[0]))
	})

	t.Run("record too large", func(t *testing.T) {
		huge := "{\"k\":\"" + strings.Repeat("x", datasetFileRowsMaxLineBytes) + "\"}\n"
		fs, key := scanFS(t, huge)
		_, err := runScanPage(t, fs, key, huge, 0, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_file_record_too_large")
	})

	t.Run("non object record rejected", func(t *testing.T) {
		content := "{\"a\":1}\n[1,2]\n"
		fs, key := scanFS(t, content)
		_, err := runScanPage(t, fs, key, content, 0, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_file_parse_failed")
	})

	t.Run("invalid json record rejected", func(t *testing.T) {
		content := "{\"a\":}\n"
		fs, key := scanFS(t, content)
		_, err := runScanPage(t, fs, key, content, 0, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dataset_file_parse_failed")
	})

	t.Run("row limit page accounts bytes", func(t *testing.T) {
		content := "{\"z\":1,\"a\":2}\n{\"z\":3}\n"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 0, 1)
		require.NoError(t, err)
		require.Len(t, page.rows, 1)
		assert.True(t, page.hasMore, "a second record is still pending")
		assert.Equal(t, int64(len("{\"z\":1,\"a\":2}\n")), page.pageBytes, "only the consumed record counts")
	})

	t.Run("raw rows preserve key order verbatim", func(t *testing.T) {
		content := "{\"z\":1,\"a\":2,\"m\":{\"n\":true}}\n"
		fs, key := scanFS(t, content)
		page, err := runScanPage(t, fs, key, content, 0, 10)
		require.NoError(t, err)
		assert.Equal(t, content[:len(content)-1], string(page.rows[0]))
	})
}

// TestScanWhitespaceOnlyObjectPagesWithinByteCap: a blanks-only object
// bigger than the page byte cap must page on consumed bytes (rows may stay
// empty) and terminate, never read unbounded in one page.
func TestScanWhitespaceOnlyObjectPagesWithinByteCap(t *testing.T) {
	// ~9 MiB of whitespace in 1 KiB lines (each line under the record cap).
	var b strings.Builder
	for len(b.String()) < 9<<20 {
		b.WriteString(strings.Repeat(" ", 1023))
		b.WriteString("\n")
	}
	content := b.String()

	fs, key := scanFS(t, content)
	consumed := int64(0)
	pages := 0
	offset := int64(0)
	for {
		page, err := runScanPage(t, fs, key, content, offset, 1000)
		require.NoError(t, err)
		pages++
		assert.Empty(t, page.rows, "whitespace pages carry no rows")
		require.LessOrEqual(t, page.pageBytes, int64(datasetScanMaxPageBytes), "every page's raw consumption is capped")
		if !page.hasMore {
			require.Equal(t, int64(len(content)), offset+page.pageBytes, "final page reaches EOF")
			break
		}
		require.Greater(t, page.pageBytes, int64(0), "a cursor-issuing page must have consumed bytes")
		offset = page.endOffset
		consumed += page.pageBytes
	}
	consumed += int64(len(content)) - offset
	require.Greater(t, pages, 1, "a >8MiB whitespace object cannot fit one page")
	assert.Equal(t, int64(len(content)), consumed)
}

func TestScanRecordReaderBoundaries(t *testing.T) {
	t.Run("clean eof", func(t *testing.T) {
		fs, key := scanFS(t, "{\"a\":1}\n")
		reader, err := fs.OpenLocalObject(key)
		require.NoError(t, err)
		defer reader.Close()
		buf := bufio.NewReader(reader)
		rec, err := readRecord(buf, 1024)
		require.NoError(t, err)
		assert.Equal(t, "{\"a\":1}\n", string(rec))
		_, err = readRecord(buf, 1024)
		require.Equal(t, io.EOF, err)
	})

	t.Run("record without newline at eof", func(t *testing.T) {
		fs, key := scanFS(t, "{\"a\":1}")
		reader, err := fs.OpenLocalObject(key)
		require.NoError(t, err)
		defer reader.Close()
		buf := bufio.NewReader(reader)
		rec, err := readRecord(buf, 1024)
		require.NoError(t, err)
		assert.Equal(t, "{\"a\":1}", string(rec))
	})
}
