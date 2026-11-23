package models

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPostgresSnapshotSourceOptIn(t *testing.T) {
	source, err := ParsePostgresSnapshotSource([]byte(`{"sync":{"postgres_table":{"schema":"Sales","name":"Daily Orders"}}}`))
	require.NoError(t, err)
	require.Equal(t, &SnapshotTable{Schema: "Sales", Name: "Daily Orders"}, source)
	for _, raw := range []string{`{"sync":{"postgres_table":null}}`, `{"password":"inline","sync":{"postgres_table":{"schema":"sales","name":"orders"}}}`, `{"sync":{"postgres_table":{"schema":"public","name":"orders"},"query":"unused"}}`} {
		_, err := ParsePostgresSnapshotSource([]byte(raw))
		require.Error(t, err)
	}
	legacy, err := ParsePostgresSnapshotSource([]byte(`{"host":"legacy","tables":[]}`))
	require.NoError(t, err)
	require.Nil(t, legacy)
}

func TestMySQLSnapshotSourceOptIn(t *testing.T) {
	source, err := ParseMySQLSnapshotSource([]byte(`{"sync":{"mysql_table":{"schema":"sales","name":"订单"}}}`))
	require.NoError(t, err)
	require.Equal(t, &SnapshotTable{Schema: "sales", Name: "订单"}, source)
	for _, raw := range []string{
		`{"sync":{"mysql_table":{"schema":"sales","name":"orders"},"admission":{"password":"inline"}}}`,
		`{"Sync":{"mysql_table":{"schema":"sales","name":"orders"},"admission":{"max_inflight_runs":1}}}`,
		`{"sync":{"mysql_table":{"schema":"sales","name":"orders"},"Admission":{"max_inflight_runs":1}}}`,
		`{"sync":{"mysql_table":null}}`, `{"password":"inline","sync":{"mysql_table":{"schema":"sales","name":"orders"}}}`, `{"sync":{"mysql_table":{"schema":"sales","name":"orders"},"query":"unused"}}`} {
		_, err := ParseMySQLSnapshotSource([]byte(raw))
		require.Error(t, err)
	}
	legacy, err := ParseMySQLSnapshotSource([]byte(`{"host":"legacy"}`))
	require.NoError(t, err)
	require.Nil(t, legacy)
}
