//go:build integration

package cassandrakernel

import (
	"context"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	repos "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
)

func TestObjectStoreWithRealCassandraGetListAndMarkings(t *testing.T) {
	session, keyspace := testingx.OpenCassandraTestKeyspace(t)
	require.NoError(t, Apply(session, keyspace, OntologyObjectStoreMigrations(keyspace)))
	store := NewObjectStoreWithKeyspace(session, keyspace)

	tenant := repos.TenantId(uuid.NewString())
	objectID := repos.ObjectId(gocql.TimeUUID().String())
	owner := repos.OwnerId(uuid.NewString())
	org := string(tenant)
	created := time.Now().Add(-time.Minute).UnixMilli()
	obj := repos.Object{
		Tenant:         tenant,
		ID:             objectID,
		TypeID:         repos.TypeId("aircraft"),
		Payload:        []byte(`{"callsign":"OF-1","altitude":12000}`),
		OrganizationID: &org,
		CreatedAtMs:    &created,
		UpdatedAtMs:    time.Now().UnixMilli(),
		Owner:          &owner,
		Markings:       []repos.MarkingId{"PUBLIC", "CONTROLLED"},
	}
	outcome, err := store.Put(context.Background(), obj, nil)
	require.NoError(t, err)
	require.Equal(t, repos.PutInserted, outcome.Kind)

	got, err := store.Get(context.Background(), tenant, objectID, repos.Eventual())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, tenant, got.Tenant)
	require.Equal(t, objectID, got.ID)
	require.JSONEq(t, string(obj.Payload), string(got.Payload))
	require.ElementsMatch(t, []repos.MarkingId{"PUBLIC", "CONTROLLED"}, got.Markings)
	require.Equal(t, &org, got.OrganizationID)

	list, err := store.ListByType(context.Background(), tenant, repos.TypeId("aircraft"), repos.Page{Size: 10}, repos.Eventual())
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Equal(t, objectID, list.Items[0].ID)
	require.ElementsMatch(t, []repos.MarkingId{"PUBLIC", "CONTROLLED"}, list.Items[0].Markings)

	otherTenant, err := store.Get(context.Background(), repos.TenantId(uuid.NewString()), objectID, repos.Eventual())
	require.NoError(t, err)
	require.Nil(t, otherTenant)
}
