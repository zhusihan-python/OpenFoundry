package runtime

import (
	"context"
	"errors"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPostgresEndpointAndReadFence(t *testing.T) {
	u, err := PostgresEndpoint("postgresql://orders-db:5432/orders", []string{"orders-db"}, true)
	require.NoError(t, err)
	for _, raw := range []string{"postgresql://user:pw@orders-db/orders", "postgresql://orders-db/orders?sslmode=disable", "postgresql://orders-db,second/orders", "postgresql://other/orders", "postgresql://orders-db:0/orders", "postgresql://orders-db"} {
		_, err := PostgresEndpoint(raw, []string{"orders-db"}, true)
		require.Error(t, err)
	}
	lost := errors.New("lease lost")
	result, err := FetchPostgresSnapshot(context.Background(), TableSnapshotOptions{Endpoint: u, User: "reader", Password: "dummy", BeforeRead: func(context.Context) error { return lost }})
	require.ErrorIs(t, err, lost)
	require.Nil(t, result)
}

func TestPostgresDialChecksResolvedAddress(t *testing.T) {
	for _, address := range []string{"127.0.0.1:5432", "10.0.0.2:5432", "[::ffff:192.168.1.3]:5432", "[fe80::1]:5432", "[::1]:5432", "[fd00::1]:5432"} {
		require.False(t, databaseDialAddressAllowed("tcp", address, false))
		require.True(t, databaseDialAddressAllowed("tcp", address, true))
	}
	require.False(t, databaseDialAddressAllowed("tcp", "unresolved.local:5432", true))
	require.False(t, databaseDialAddressAllowed("tcp", "0.0.0.0:5432", true))
	require.True(t, databaseDialAddressAllowed("tcp", "203.0.113.1:5432", false))
}

func TestMySQLSourceBoundaries(t *testing.T) {
	for _, raw := range []string{"mysql://user:pw@orders-db/sales", "mysql://orders-db/sales?tls=false", "mysql://other/sales", "mysql://orders-db:0/sales", "postgresql://orders-db/sales"} {
		_, err := MySQLEndpoint(raw, []string{"orders-db"}, true)
		require.Error(t, err)
	}
	endpoint, err := MySQLEndpoint("mysql://orders-db/sales", []string{"orders-db"}, true)
	require.NoError(t, err)
	lost := errors.New("lease lost")
	opts := TableSnapshotOptions{Endpoint: endpoint, User: "reader", Password: "dummy", Table: models.SnapshotTable{Schema: "sales", Name: "orders"}, BeforeRead: func(context.Context) error { return lost }}
	_, err = FetchMySQLSnapshot(context.Background(), opts)
	require.ErrorIs(t, err, lost)
	opts.Table.Schema = "other"
	_, err = FetchMySQLSnapshot(context.Background(), opts)
	require.ErrorContains(t, err, "configured database")
	require.Equal(t, "`a``b`", mysqlIdentifier("a`b"))
}
