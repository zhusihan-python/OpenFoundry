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
