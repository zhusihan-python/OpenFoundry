//go:build integration

package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cassandrakernel "github.com/openfoundry/openfoundry-go/libs/cassandra-kernel"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
)

func TestCassandraAdaptersIntegration(t *testing.T) {
	session, keyspace := testingx.OpenCassandraTestKeyspace(t)
	require.NoError(t, cassandrakernel.Apply(session, keyspace, cassandrakernel.OntologyObjectStoreMigrations(keyspace)))
	require.NoError(t, cassandrakernel.Apply(session, keyspace, cassandrakernel.OntologyLinkStoreMigrations(keyspace)))
	objects, links := NewCassandraStores(session, keyspace, keyspace)
	ctx := context.Background()

	tenant := TenantId("tenant-a")
	objectID := ObjectId(gocql.TimeUUID().String())
	owner := OwnerId(uuid.NewString())
	createdAt := time.Now().Add(-time.Second).UnixMilli()
	updatedAt := time.Now().UnixMilli()

	out, err := objects.Put(ctx, Object{
		Tenant:      tenant,
		ID:          objectID,
		TypeID:      "aircraft",
		Payload:     []byte(`{"tail":"N123OF"}`),
		CreatedAtMs: &createdAt,
		UpdatedAtMs: updatedAt,
		Owner:       &owner,
		Markings:    []MarkingId{"public"},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, PutInserted, out.Kind)

	got, err := objects.Get(ctx, tenant, objectID, ReadStrong)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, uint64(1), got.Version)
	assert.JSONEq(t, `{"tail":"N123OF"}`, string(got.Payload))

	linkPayload := json.RawMessage(`{"kind":"primary"}`)
	link := Link{Tenant: tenant, LinkType: "related_to", From: objectID, To: ObjectId(gocql.TimeUUID().String()), Payload: &linkPayload, CreatedAtMs: updatedAt}
	require.NoError(t, links.Put(ctx, link))
	outgoing, err := links.ListOutgoing(ctx, tenant, link.LinkType, link.From, Page{Size: 10}, ReadStrong)
	require.NoError(t, err)
	require.Len(t, outgoing.Items, 1)
	require.NotNil(t, outgoing.Items[0].Payload)
	assert.JSONEq(t, `{"kind":"primary"}`, string(*outgoing.Items[0].Payload))
}
