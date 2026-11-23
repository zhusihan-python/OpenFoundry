package runtime

import (
	"context"
	"errors"
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
	result, err := FetchPostgresSnapshot(context.Background(), PostgresSnapshotOptions{Endpoint: u, User: "reader", Password: "dummy", BeforeRead: func(context.Context) error { return lost }})
	require.ErrorIs(t, err, lost)
	require.Nil(t, result)
}

func TestPostgresDialChecksResolvedAddress(t *testing.T) {
	for _, address := range []string{"127.0.0.1:5432", "10.0.0.2:5432", "[::ffff:192.168.1.3]:5432", "[fe80::1]:5432", "[::1]:5432", "[fd00::1]:5432"} {
		require.False(t, postgresDialAddressAllowed("tcp", address, false))
		require.True(t, postgresDialAddressAllowed("tcp", address, true))
	}
	require.False(t, postgresDialAddressAllowed("tcp", "unresolved.local:5432", true))
	require.False(t, postgresDialAddressAllowed("tcp", "0.0.0.0:5432", true))
	require.True(t, postgresDialAddressAllowed("tcp", "203.0.113.1:5432", false))
}
