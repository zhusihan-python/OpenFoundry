package models

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPostgresSnapshotSourceOptIn(t *testing.T) {
	source, err := ParsePostgresSnapshotSource([]byte(`{"sync":{"postgres_table":{"schema":"Sales","name":"Daily Orders"}}}`))
	require.NoError(t, err)
	require.Equal(t, &PostgresTable{Schema: "Sales", Name: "Daily Orders"}, source)
	for _, raw := range []string{`{"sync":{"postgres_table":null}}`, `{"password":"inline","sync":{"postgres_table":{"schema":"sales","name":"orders"}}}`, `{"sync":{"postgres_table":{"schema":"public","name":"orders"},"query":"unused"}}`} {
		_, err := ParsePostgresSnapshotSource([]byte(raw))
		require.Error(t, err)
	}
	legacy, err := ParsePostgresSnapshotSource([]byte(`{"host":"legacy","tables":[]}`))
	require.NoError(t, err)
	require.Nil(t, legacy)
}
