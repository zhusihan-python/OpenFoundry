//go:build integration

package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	cassandrakernel "github.com/openfoundry/openfoundry-go/libs/cassandra-kernel"
	repos "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/ontology-query-service/internal/handlers"
)

func TestHandlersReadObjectsFromRealCassandra(t *testing.T) {
	session, keyspace := testingx.OpenCassandraTestKeyspace(t)
	require.NoError(t, cassandrakernel.Apply(session, keyspace, cassandrakernel.OntologyObjectStoreMigrations(keyspace)))
	store := cassandrakernel.NewObjectStoreWithKeyspace(session, keyspace)

	tenantUUID := uuid.New()
	tenant := repos.TenantId(tenantUUID.String())
	objectID := repos.ObjectId(gocql.TimeUUID().String())
	owner := repos.OwnerId(uuid.NewString())
	org := tenantUUID.String()
	created := time.Now().Add(-time.Minute).UnixMilli()
	obj := repos.Object{
		Tenant:         tenant,
		ID:             objectID,
		TypeID:         repos.TypeId("aircraft"),
		Payload:        json.RawMessage(`{"callsign":"OF-QUERY-1"}`),
		OrganizationID: &org,
		CreatedAtMs:    &created,
		UpdatedAtMs:    time.Now().UnixMilli(),
		Owner:          &owner,
		Markings:       []repos.MarkingId{"PUBLIC", "CONTROLLED"},
	}
	outcome, err := store.Put(context.Background(), obj, nil)
	require.NoError(t, err)
	require.Equal(t, repos.PutInserted, outcome.Kind)

	h := handlers.New(handlers.AppState{Objects: store})
	allowedClaims := &authmw.Claims{
		Sub:          uuid.New(),
		OrgID:        &tenantUUID,
		Roles:        []string{"user"},
		SessionScope: &authmw.SessionScope{AllowedMarkings: []string{"PUBLIC", "CONTROLLED"}},
	}

	req := authedReq(http.MethodGet, "/objects/"+string(tenant)+"/"+string(objectID), map[string]string{
		"tenant": string(tenant), "object_id": string(objectID),
	}, allowedClaims)
	rec := httptest.NewRecorder()
	h.GetObject(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got repos.Object
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, objectID, got.ID)
	require.Equal(t, tenant, got.Tenant)
	require.JSONEq(t, string(obj.Payload), string(got.Payload))
	require.ElementsMatch(t, []repos.MarkingId{"PUBLIC", "CONTROLLED"}, got.Markings)

	req = authedReq(http.MethodGet, "/objects/"+string(tenant)+"/by-type/aircraft?size=10", map[string]string{
		"tenant": string(tenant), "type_id": "aircraft",
	}, allowedClaims)
	rec = httptest.NewRecorder()
	h.ListObjectsByType(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var list handlers.ListResponse[repos.Object]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list.Items, 1)
	require.Equal(t, objectID, list.Items[0].ID)

	otherTenant := uuid.New()
	forbiddenTenantClaims := &authmw.Claims{Sub: uuid.New(), OrgID: &otherTenant, Roles: []string{"user"}}
	req = authedReq(http.MethodGet, "/objects/"+string(tenant)+"/"+string(objectID), map[string]string{
		"tenant": string(tenant), "object_id": string(objectID),
	}, forbiddenTenantClaims)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "tenant access denied")

	markingDeniedClaims := &authmw.Claims{
		Sub:          uuid.New(),
		OrgID:        &tenantUUID,
		Roles:        []string{"user"},
		SessionScope: &authmw.SessionScope{AllowedMarkings: []string{"PUBLIC"}},
	}
	req = authedReq(http.MethodGet, "/objects/"+string(tenant)+"/"+string(objectID), map[string]string{
		"tenant": string(tenant), "object_id": string(objectID),
	}, markingDeniedClaims)
	rec = httptest.NewRecorder()
	h.GetObject(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "marking access denied")
}
