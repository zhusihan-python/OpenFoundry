package storageabstraction

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalBackingFSPresignedURLRoundTrip(t *testing.T) {
	fixed := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	fs := NewLocalBackingFS("http://files.local/", "datasets", []byte("secret"))
	fs.Now = func() time.Time { return fixed }

	signed, err := fs.PresignedURL(PhysicalLocation{FSID: "local", BaseDirectory: "datasets", RelativePath: "sales/part.parquet"}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "GET", signed.Method)
	require.Equal(t, fixed.Add(time.Minute), signed.ExpiresAt)
	require.True(t, strings.HasPrefix(signed.URL, "http://files.local/v1/_internal/local-fs/datasets/sales/part.parquet?"))
}

func TestParsePhysicalURI(t *testing.T) {
	require.Equal(t, PhysicalLocation{FSID: "s3:bucket", RelativePath: "base/file.parquet"}, ParsePhysicalURI("s3://bucket/base/file.parquet"))
	require.Equal(t, PhysicalLocation{FSID: "local", RelativePath: "base/file.parquet"}, ParsePhysicalURI("local:///base/file.parquet"))
	require.Equal(t, "local:///base/file.parquet", PhysicalLocation{FSID: "local", RelativePath: "base/file.parquet"}.URI())
}

func TestLocalBackingFSOpenLocalObject(t *testing.T) {
	root := t.TempDir()
	fs := NewLocalBackingFS("http://files.local/", "base", []byte("secret"))
	fs.RootDir = root

	require.NoError(t, fs.WriteLocalObject("base/datasets/x/rows.ndjson", []byte("line1\nline2\n")))

	reader, err := fs.OpenLocalObject("base/datasets/x/rows.ndjson")
	require.NoError(t, err)
	defer reader.Close()

	// Streaming read sees the written bytes without a whole-object buffer.
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "line1\nline2\n", string(got))

	// Seeking back works: the scanner contract needs byte-offset seeks.
	off, err := reader.Seek(6, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(6), off)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "line2\n", string(rest))

	// Round trip with ReadLocalObject on the same key.
	whole, err := fs.ReadLocalObject("base/datasets/x/rows.ndjson")
	require.NoError(t, err)
	require.Equal(t, "line1\nline2\n", string(whole))

	// Traversal-safety is the same as the byte readers: escaping keys are
	// rejected before any filesystem access.
	for _, key := range []string{"../escape", "base/../escape", "/abs", ""} {
		_, err := fs.OpenLocalObject(key)
		require.Error(t, err, "key %q must be rejected", key)
	}

	// A missing object reports the os error (caller maps it to a missing
	// backing object).
	_, err = fs.OpenLocalObject("base/datasets/x/missing.ndjson")
	require.True(t, os.IsNotExist(err), "want not-exist, got %v", err)

	// The opener never creates directories or files as a side effect.
	_, err = fs.OpenLocalObject(filepath.ToSlash(filepath.Join("base", "datasets", "y", "rows.ndjson")))
	require.True(t, os.IsNotExist(err), "want not-exist, got %v", err)
	entries, err := os.ReadDir(filepath.Join(root, "base", "datasets"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "open must not create sibling directories")
}

func TestLocalBackingFSOpenLocalObjectRefusesRootEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.ndjson"), []byte("{\"x\":1}\n"), 0o644))

	fs := NewLocalBackingFS("http://files.local/", "base", []byte("secret"))
	fs.RootDir = root
	require.NoError(t, os.MkdirAll(filepath.Join(root, "base", "datasets"), 0o755))
	// A symlink inside the tree pointing outside the root: lexical key
	// validation passes, only the root-restricted open can refuse it.
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "base", "datasets", "escape")))

	for _, key := range []string{
		"base/datasets/escape/secret.ndjson",
		"base/datasets/escape",
	} {
		_, err := fs.OpenLocalObject(key)
		require.Error(t, err, "symlink key %q must be refused", key)
	}

	// A symlink that stays inside the root is allowed: os.OpenInRoot
	// refuses exactly the escapes, not in-root resolution.
	require.NoError(t, os.WriteFile(filepath.Join(root, "base", "real.ndjson"), []byte("ok\n"), 0o644))
	require.NoError(t, os.Symlink("real.ndjson", filepath.Join(root, "base", "alias.ndjson")))
	opened, err := fs.OpenLocalObject("base/alias.ndjson")
	require.NoError(t, err, "in-root symlink resolves")
	require.NoError(t, opened.Close())

	// The byte readers keep their historical lexical-only behavior.
	_, err = fs.ReadLocalObject("base/datasets/escape/secret.ndjson")
	require.NoError(t, err, "ReadLocalObject behavior is unchanged")
}
