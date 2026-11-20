package handlers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
)

func TestGroupOutputObjectsCleanOnlyThisAttempt(t *testing.T) {
	root := t.TempDir()
	backing := &storageabstraction.LocalBackingFS{RootDir: root}
	existing := "dataset/existing.ndjson"
	require.NoError(t, backing.WriteLocalObject(existing, []byte("previous")))
	objects := &groupOutputObjects{backing: backing}
	t.Cleanup(objects.close)
	created := "dataset/attempt.ndjson"
	err := objects.write([]outputObjectWrite{
		{Key: created, Content: []byte("uncommitted")},
		{Key: existing, Content: []byte("must not replace")},
	})
	require.ErrorIs(t, err, os.ErrExist)
	require.NoError(t, objects.remove())
	require.NoFileExists(t, filepath.Join(root, created))
	data, err := backing.ReadLocalObject(existing)
	require.NoError(t, err)
	require.Equal(t, "previous", string(data))
}
