//go:build integration

package cassandrakernel

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

// ---- objects_by_type: one current row per object ----
//
// These scenarios also write rows straight to Cassandra: the way the code
// before the index cleanup did, the way a late write lands, and the state a
// write leaves when it fails between its statements. The store then meets
// rows it did not write itself.

type typeIndexRowOf struct {
	updatedAtMs int64
	summary     string
	owner       gocql.UUID
	markings    []string
}

func openTypeIndexStore(t *testing.T) (*ObjectStore, *gocql.Session, string) {
	t.Helper()
	session, keyspace := testingx.OpenCassandraTestKeyspace(t)
	require.NoError(t, Apply(session, keyspace, OntologyObjectStoreMigrations(keyspace)))
	return NewObjectStoreWithKeyspace(session, keyspace), session, keyspace
}

// typeIndexRowsOf reads straight from objects_by_type every row one object
// has in a type's partition, the current one and any left behind, oldest
// first.
func typeIndexRowsOf(t *testing.T, session *gocql.Session, keyspace, tenant, typeID string, objectID gocql.UUID) []typeIndexRowOf {
	t.Helper()
	iter := session.Query(fmt.Sprintf(
		`SELECT updated_at, properties_summary, owner_id, marking FROM %s.objects_by_type
		  WHERE tenant = ? AND type_id = ? AND object_id = ? ALLOW FILTERING`, keyspace),
		tenant, typeID, objectID).Iter()
	rows := []typeIndexRowOf{}
	var (
		updatedAt time.Time
		summary   *string
		owner     gocql.UUID
		markings  []string
	)
	for iter.Scan(&updatedAt, &summary, &owner, &markings) {
		text := ""
		if summary != nil {
			text = *summary
		}
		rows = append(rows, typeIndexRowOf{
			updatedAtMs: updatedAt.UnixMilli(),
			summary:     text,
			owner:       owner,
			markings:    append([]string(nil), markings...),
		})
	}
	require.NoError(t, iter.Close())
	sort.Slice(rows, func(i, j int) bool { return rows[i].updatedAtMs < rows[j].updatedAtMs })
	return rows
}

// pagingStateAfter reads `count` index rows the way the server pages them and
// returns the state that resumes after them, with the ids it read.
func pagingStateAfter(t *testing.T, session *gocql.Session, keyspace, tenant, typeID string, count int) ([]byte, []gocql.UUID) {
	t.Helper()
	iter := session.Query(fmt.Sprintf(
		`SELECT object_id FROM %s.objects_by_type WHERE tenant = ? AND type_id = ?`, keyspace),
		tenant, typeID).PageSize(count).Iter()
	ids := []gocql.UUID{}
	var objectID gocql.UUID
	for len(ids) < count && iter.Scan(&objectID) {
		ids = append(ids, objectID)
	}
	state := append([]byte(nil), iter.PageState()...)
	require.NoError(t, iter.Close())
	return state, ids
}

func version(n uint64) *uint64 { return &n }

func objectAt(tenant string, objectID, owner gocql.UUID, typeID string, updatedAtMs int64, payload string, markings ...string) repos.Object {
	ownerID := repos.OwnerId(owner.String())
	marks := make([]repos.MarkingId, 0, len(markings))
	for _, m := range markings {
		marks = append(marks, repos.MarkingId(m))
	}
	return repos.Object{
		Tenant:      repos.TenantId(tenant),
		ID:          repos.ObjectId(objectID.String()),
		TypeID:      repos.TypeId(typeID),
		Payload:     []byte(payload),
		CreatedAtMs: &updatedAtMs,
		UpdatedAtMs: updatedAtMs,
		Owner:       &ownerID,
		Markings:    marks,
	}
}

func putObject(t *testing.T, store *ObjectStore, obj repos.Object, expected *uint64) repos.PutOutcome {
	t.Helper()
	outcome, err := store.Put(context.Background(), obj, expected)
	require.NoError(t, err)
	return outcome
}

func listObjects(t *testing.T, store *ObjectStore, tenant, typeID string, page repos.Page) repos.PagedResult[repos.Object] {
	t.Helper()
	result, err := store.ListByType(context.Background(), repos.TenantId(tenant), repos.TypeId(typeID), page, repos.Strong())
	require.NoError(t, err)
	return result
}

func listedIDs(items []repos.Object) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, string(item.ID))
	}
	return ids
}

// writePrimaryRow and writeIndexRow write one row of one revision alone.
func writePrimaryRow(t *testing.T, session *gocql.Session, keyspace string, revision int64, obj repos.Object) {
	t.Helper()
	owner := gocql.UUID(uuid.MustParse(string(*obj.Owner)))
	marks := make([]string, 0, len(obj.Markings))
	for _, m := range obj.Markings {
		marks = append(marks, string(m))
	}
	require.NoError(t, session.Query(fmt.Sprintf(
		`INSERT INTO %s.objects_by_id (tenant, object_id, type_id, owner_id, properties, marking,
			revision_number, created_at, updated_at, deleted)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, false)`, keyspace),
		string(obj.Tenant), gocql.UUID(uuid.MustParse(string(obj.ID))), string(obj.TypeID), owner,
		string(obj.Payload), marks, revision, time.UnixMilli(*obj.CreatedAtMs).UTC(),
		time.UnixMilli(obj.UpdatedAtMs).UTC()).Exec())
}

func writeIndexRow(t *testing.T, session *gocql.Session, keyspace string, obj repos.Object, summary string) {
	t.Helper()
	owner := gocql.UUID(uuid.MustParse(string(*obj.Owner)))
	marks := make([]string, 0, len(obj.Markings))
	for _, m := range obj.Markings {
		marks = append(marks, string(m))
	}
	require.NoError(t, session.Query(fmt.Sprintf(
		`INSERT INTO %s.objects_by_type (tenant, type_id, updated_at, object_id, owner_id, marking,
			properties_summary, deleted)
		 VALUES (?, ?, ?, ?, ?, ?, ?, false)`, keyspace),
		string(obj.Tenant), string(obj.TypeID), time.UnixMilli(obj.UpdatedAtMs).UTC(),
		gocql.UUID(uuid.MustParse(string(obj.ID))), owner, marks, summary).Exec())
}

// writeAsTheEarlierCodeDid writes one revision the way the code before the
// index cleanup did: the primary row, and one more objects_by_type row.
func writeAsTheEarlierCodeDid(t *testing.T, session *gocql.Session, keyspace string, revision int64, obj repos.Object) {
	t.Helper()
	writePrimaryRow(t, session, keyspace, revision, obj)
	writeIndexRow(t, session, keyspace, obj, string(obj.Payload))
}

func TestObjectStoreUpdateLeavesOneCurrentTypeIndexRow(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	putObject(t, store, objectAt(tenant, objectID, owner, typeID, base, `{"n":1}`, "PUBLIC"), nil)
	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, owner, typeID, base+1000, `{"n":2}`, "PUBLIC", "CONTROLLED"), version(1)), 1, 2)

	rows := typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1, "one row, not one per revision")
	assert.Equal(t, base+1000, rows[0].updatedAtMs)
	assert.JSONEq(t, `{"n":2}`, rows[0].summary)
	assert.Equal(t, owner, rows[0].owner)
	assert.ElementsMatch(t, []string{"CONTROLLED", "PUBLIC"}, rows[0].markings)

	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1)
	item := list.Items[0]
	assert.Equal(t, repos.ObjectId(objectID.String()), item.ID)
	assert.Equal(t, repos.TypeId(typeID), item.TypeID)
	assert.EqualValues(t, 0, item.Version, "the list does not carry the revision")
	assert.JSONEq(t, `{"n":2}`, string(item.Payload))
	assert.EqualValues(t, owner.String(), *item.Owner)
	assert.Equal(t, []repos.MarkingId{"CONTROLLED", "PUBLIC"}, item.Markings)
	assert.EqualValues(t, base+1000, item.UpdatedAtMs)
}

func requireUpdated(t *testing.T, outcome repos.PutOutcome, previous, next uint64) {
	t.Helper()
	require.Equal(t, repos.PutUpdated, outcome.Kind, "%+v", outcome)
	assert.Equal(t, previous, outcome.PreviousVersion)
	assert.Equal(t, next, outcome.NewVersion)
}

func TestObjectStoreUpdateToAnotherTypeLeavesTheEarlierType(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()

	putObject(t, store, objectAt(tenant, objectID, owner, "aircraft", base, `{"n":1}`), nil)
	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, owner, "vessel", base+1000, `{"n":2}`), version(1)), 1, 2)

	assert.Empty(t, typeIndexRowsOf(t, session, keyspace, tenant, "aircraft", objectID))
	assert.Len(t, typeIndexRowsOf(t, session, keyspace, tenant, "vessel", objectID), 1)
	assert.Empty(t, listObjects(t, store, tenant, "aircraft", repos.Page{Size: 10}).Items)
	assert.Equal(t, []string{objectID.String()},
		listedIDs(listObjects(t, store, tenant, "vessel", repos.Page{Size: 10}).Items))
}

func TestObjectStoreSoftDeleteLeavesNoTypeIndexRow(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	putObject(t, store, objectAt(tenant, objectID, owner, typeID, base, `{"n":1}`), nil)
	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, owner, typeID, base+1000, `{"n":2}`), version(1)), 1, 2)

	deleted, err := store.Delete(context.Background(), repos.TenantId(tenant), repos.ObjectId(objectID.String()))
	require.NoError(t, err)
	require.True(t, deleted)

	assert.Empty(t, typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID), "no live row stays behind")
	got, err := store.Get(context.Background(), repos.TenantId(tenant), repos.ObjectId(objectID.String()), repos.Strong())
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Empty(t, listObjects(t, store, tenant, typeID, repos.Page{Size: 10}).Items)

	// A second delete is the no-op it always was.
	deleted, err = store.Delete(context.Background(), repos.TenantId(tenant), repos.ObjectId(objectID.String()))
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Empty(t, typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID))
}

func TestObjectStoreUpdatesInTheSameMillisecondKeepOneTypeIndexRow(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	same := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	putObject(t, store, objectAt(tenant, objectID, owner, typeID, same, `{"n":1}`), nil)
	requireUpdated(t, putObject(t, store, objectAt(tenant, objectID, owner, typeID, same, `{"n":2}`), version(1)), 1, 2)
	requireUpdated(t, putObject(t, store, objectAt(tenant, objectID, owner, typeID, same, `{"n":3}`), version(2)), 2, 3)

	// Every revision shares the key: the row is overwritten, never deleted.
	rows := typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1)
	assert.Equal(t, same, rows[0].updatedAtMs)
	assert.JSONEq(t, `{"n":3}`, rows[0].summary)

	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1)
	assert.JSONEq(t, `{"n":3}`, string(list.Items[0].Payload))

	// The next later millisecond leaves one row again, at its own key.
	requireUpdated(t, putObject(t, store, objectAt(tenant, objectID, owner, typeID, same+1, `{"n":4}`), version(3)), 3, 4)
	rows = typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1)
	assert.Equal(t, same+1, rows[0].updatedAtMs)
}

func TestObjectStoreListByTypeHoldsOnlyCurrentRows(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	live, ghost, orphan := gocql.TimeUUID(), gocql.TimeUUID(), gocql.TimeUUID()
	base := time.Now().Add(-2 * time.Hour).UnixMilli()
	const typeID = "aircraft"

	// A live object the earlier code wrote three times: one row per revision.
	for i := 0; i < 3; i++ {
		writeAsTheEarlierCodeDid(t, session, keyspace, int64(i+1),
			objectAt(tenant, live, owner, typeID, base+int64(i)*1000, fmt.Sprintf(`{"n":%d}`, i)))
	}
	// An object the earlier code soft-deleted: its index row stayed.
	writeAsTheEarlierCodeDid(t, session, keyspace, 1, objectAt(tenant, ghost, owner, typeID, base+5000, `{"n":9}`))
	require.NoError(t, session.Query(fmt.Sprintf(
		`UPDATE %s.objects_by_id SET deleted = true, updated_at = ? WHERE tenant = ? AND object_id = ?`, keyspace),
		time.UnixMilli(base+9000), tenant, ghost).Exec())
	// An index row whose primary row is missing.
	writeIndexRow(t, session, keyspace, objectAt(tenant, orphan, owner, typeID, base+7000, `{"n":8}`), `{"n":8}`)

	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1, listedIDs(list.Items))
	assert.Equal(t, repos.ObjectId(live.String()), list.Items[0].ID)
	assert.JSONEq(t, `{"n":2}`, string(list.Items[0].Payload), "the newest row of the object")
	assert.EqualValues(t, base+2000, list.Items[0].UpdatedAtMs)

	// An update written by this code supersedes the row the primary row names.
	requireUpdated(t, putObject(t, store,
		objectAt(tenant, live, owner, typeID, base+3000, `{"n":3}`), version(3)), 3, 4)
	for _, row := range typeIndexRowsOf(t, session, keyspace, tenant, typeID, live) {
		assert.NotEqual(t, base+2000, row.updatedAtMs, "the row the update superseded is gone")
	}
	list = listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1, listedIDs(list.Items))
	assert.JSONEq(t, `{"n":3}`, string(list.Items[0].Payload))
	assert.EqualValues(t, base+3000, list.Items[0].UpdatedAtMs)

	deleted, err := store.Delete(context.Background(), repos.TenantId(tenant), repos.ObjectId(live.String()))
	require.NoError(t, err)
	require.True(t, deleted)
	for _, row := range typeIndexRowsOf(t, session, keyspace, tenant, typeID, live) {
		assert.NotEqual(t, base+3000, row.updatedAtMs, "the deleted object's current row is gone")
	}
	assert.Empty(t, listObjects(t, store, tenant, typeID, repos.Page{Size: 10}).Items)
}

func TestObjectStoreListByTypeTokenPagesAreExactWithStaleRows(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"
	const live = 23

	// Live objects one millisecond apart; between them, a second row for
	// every third object and the row of an object that is soft-deleted.
	ids := make([]gocql.UUID, live)
	for i := range ids {
		ids[i] = gocql.TimeUUID()
		putObject(t, store, objectAt(tenant, ids[i], owner, typeID, base-int64(i)*1000, fmt.Sprintf(`{"i":%d}`, i)), nil)
		if i%3 == 0 {
			writeIndexRow(t, session, keyspace,
				objectAt(tenant, ids[i], owner, typeID, base-int64(i)*1000-500, ""), fmt.Sprintf(`{"i":%d,"stale":true}`, i))
		}
	}
	ghost := gocql.TimeUUID()
	writeAsTheEarlierCodeDid(t, session, keyspace, 1, objectAt(tenant, ghost, owner, typeID, base-250, `{"ghost":true}`))
	require.NoError(t, session.Query(fmt.Sprintf(
		`UPDATE %s.objects_by_id SET deleted = true, updated_at = ? WHERE tenant = ? AND object_id = ?`, keyspace),
		time.UnixMilli(base+9000), tenant, ghost).Exec())

	// Without a token the driver reads the partition to its end, as before.
	first := listObjects(t, store, tenant, typeID, repos.Page{Size: 5})
	assert.Nil(t, first.NextToken)
	require.Len(t, first.Items, live, listedIDs(first.Items))
	for i, item := range first.Items {
		assert.Equal(t, repos.ObjectId(ids[i].String()), item.ID)
	}

	// With a token, one server page at a time: the current rows after the
	// first five index rows, each once, in index order.
	state, skipped := pagingStateAfter(t, session, keyspace, tenant, typeID, 5)
	expected := []string{}
	for _, id := range ids {
		if !slices.Contains(skipped, id) {
			expected = append(expected, id.String())
		}
	}
	got := []string{}
	var err error
	for state != nil {
		page := listObjects(t, store, tenant, typeID, repos.Page{Size: 5, Token: encodePagingState(state)})
		got = append(got, listedIDs(page.Items)...)
		if page.NextToken == nil {
			break
		}
		state, err = decodePagingState(page.NextToken)
		require.NoError(t, err)
	}
	assert.Equal(t, expected, got, "no stale row, no object twice, none missing")
}

func TestObjectStoreTypeIndexCleanupStaysInItsTenantAndType(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	thisTenant, otherTenant := uuid.NewString(), uuid.NewString()
	owner, otherOwner := gocql.MustRandomUUID(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()

	putObject(t, store, objectAt(thisTenant, objectID, owner, "aircraft", base, `{"n":1}`), nil)
	putObject(t, store, objectAt(otherTenant, objectID, otherOwner, "aircraft", base, `{"other":true}`), nil)
	// A row of this tenant's object under another type.
	writeIndexRow(t, session, keyspace, objectAt(thisTenant, objectID, owner, "vessel", base, ""), `{"n":1}`)

	requireUpdated(t, putObject(t, store,
		objectAt(thisTenant, objectID, owner, "aircraft", base+1000, `{"n":2}`), version(1)), 1, 2)
	deleted, err := store.Delete(context.Background(), repos.TenantId(thisTenant), repos.ObjectId(objectID.String()))
	require.NoError(t, err)
	require.True(t, deleted)

	// The other tenant's object, under the same id, type and millisecond.
	rows := typeIndexRowsOf(t, session, keyspace, otherTenant, "aircraft", objectID)
	require.Len(t, rows, 1)
	assert.Equal(t, base, rows[0].updatedAtMs)
	assert.JSONEq(t, `{"other":true}`, rows[0].summary)
	assert.Equal(t, otherOwner, rows[0].owner)
	// This tenant's row under the other type.
	rows = typeIndexRowsOf(t, session, keyspace, thisTenant, "vessel", objectID)
	require.Len(t, rows, 1)
	assert.Equal(t, base, rows[0].updatedAtMs)

	assert.Equal(t, []string{objectID.String()},
		listedIDs(listObjects(t, store, otherTenant, "aircraft", repos.Page{Size: 10}).Items))
	// The object is of another type now, so that type does not list it.
	assert.Empty(t, listObjects(t, store, thisTenant, "vessel", repos.Page{Size: 10}).Items)
}

func TestObjectStoreConcurrentUpdatesLeaveOneTypeIndexRow(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"
	const workers, updates = 4, 8

	putObject(t, store, objectAt(tenant, objectID, owner, typeID, base, `{"n":0}`), nil)

	// Every worker reads, then updates on the version it read; a worker that
	// lost the race reads again. The object's timestamp grows with every
	// successful update, past any the writers might share.
	var stamp atomic.Int64
	failures := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for u := 0; u < updates; u++ {
				for {
					current, err := store.Get(context.Background(), repos.TenantId(tenant), repos.ObjectId(objectID.String()), repos.Strong())
					if err != nil {
						failures <- err
						return
					}
					if current == nil {
						failures <- fmt.Errorf("object %s is gone mid-run", objectID)
						return
					}
					next := *current
					next.UpdatedAtMs = base + 1 + stamp.Add(1)
					next.Payload = []byte(fmt.Sprintf(`{"w":%d,"u":%d}`, w, u))
					outcome, err := store.Put(context.Background(), next, &current.Version)
					if err != nil {
						failures <- err
						return
					}
					if outcome.Kind == repos.PutVersionConflict {
						continue
					}
					if outcome.Kind != repos.PutUpdated {
						failures <- fmt.Errorf("put answered %v", outcome.Kind)
						return
					}
					break
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}

	current, err := store.Get(context.Background(), repos.TenantId(tenant), repos.ObjectId(objectID.String()), repos.Strong())
	require.NoError(t, err)
	require.NotNil(t, current)
	require.EqualValues(t, workers*updates+1, current.Version)
	rows := typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1, "%+v", rows)
	assert.EqualValues(t, current.UpdatedAtMs, rows[0].updatedAtMs)
	assert.JSONEq(t, string(current.Payload), rows[0].summary)

	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1)
	assert.JSONEq(t, string(current.Payload), string(list.Items[0].Payload))
}

func TestObjectStaleCleanupLeavesAReusedIndexKey(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	putObject(t, store, objectAt(tenant, objectID, owner, typeID, base, `{"n":0}`), nil)
	// What the write that moves the object off this key reads before it writes.
	var writeTime int64
	require.NoError(t, session.Query(fmt.Sprintf(
		`SELECT WRITETIME(properties_summary) FROM %s.objects_by_type
		  WHERE tenant = ? AND type_id = ? AND updated_at = ? AND object_id = ?`, keyspace),
		tenant, typeID, time.UnixMilli(base), objectID).Scan(&writeTime))
	require.NotZero(t, writeTime)

	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, owner, typeID, base+1000, `{"n":1}`), version(1)), 1, 2)
	// A later revision lands on the first key again (the same millisecond as
	// the first revision, written later).
	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, owner, typeID, base, `{"n":2}`), version(2)), 2, 3)
	rows := typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1)
	assert.Equal(t, base, rows[0].updatedAtMs)
	assert.JSONEq(t, `{"n":2}`, rows[0].summary)

	// The move's cleanup, had it been delayed until after the later revision:
	// bounded by the write time it read, it does not reach the later write.
	require.NoError(t, session.Query(fmt.Sprintf(
		`DELETE FROM %s.objects_by_type USING TIMESTAMP ?
		  WHERE tenant = ? AND type_id = ? AND updated_at = ? AND object_id = ?`, keyspace),
		writeTime, tenant, typeID, time.UnixMilli(base), objectID).Exec())
	rows = typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1, "the later revision's row stays")
	assert.JSONEq(t, `{"n":2}`, rows[0].summary)
	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1)
	assert.JSONEq(t, `{"n":2}`, string(list.Items[0].Payload))

	// The same delete without the bound would have taken the current row.
	require.NoError(t, session.Query(fmt.Sprintf(
		`DELETE FROM %s.objects_by_type WHERE tenant = ? AND type_id = ? AND updated_at = ? AND object_id = ?`, keyspace),
		tenant, typeID, time.UnixMilli(base), objectID).Exec())
	assert.Empty(t, typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID))
}

func TestObjectStoreListShowsThePrimaryRowOverALateOldIndexWrite(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant := uuid.NewString()
	firstOwner, secondOwner := gocql.MustRandomUUID(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	same := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	putObject(t, store, objectAt(tenant, objectID, firstOwner, typeID, same, `{"n":1}`, "PUBLIC"), nil)
	// The next revision keeps the millisecond, so both share the index key.
	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, secondOwner, typeID, same, `{"n":2}`, "SECRET"), version(1)), 1, 2)
	// The first revision's index write arrives late and overwrites that row.
	writeIndexRow(t, session, keyspace, objectAt(tenant, objectID, firstOwner, typeID, same, "", "PUBLIC"), `{"n":1}`)

	rows := typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID)
	require.Len(t, rows, 1)
	assert.JSONEq(t, `{"n":1}`, rows[0].summary, "the index row itself holds the late, earlier write")

	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1)
	item := list.Items[0]
	assert.EqualValues(t, 0, item.Version)
	assert.JSONEq(t, `{"n":2}`, string(item.Payload))
	assert.EqualValues(t, secondOwner.String(), *item.Owner)
	assert.Equal(t, []repos.MarkingId{"SECRET"}, item.Markings)
	assert.EqualValues(t, same, item.UpdatedAtMs)
}

func TestObjectStoreListHoldsAnObjectWhoseIndexWriteDidNotLandOnlyAfterANextWrite(t *testing.T) {
	t.Parallel()
	store, session, keyspace := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	objectID := gocql.TimeUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	putObject(t, store, objectAt(tenant, objectID, owner, typeID, base, `{"n":1}`), nil)
	// The primary row of the next revision, without its index row: the state
	// a write leaves when it fails between its statements.
	writePrimaryRow(t, session, keyspace, 2, objectAt(tenant, objectID, owner, typeID, base+1000, `{"n":2}`))

	// Until the object is written again it is read by id and not listed: the
	// row at its earlier key would show the earlier revision.
	got, err := store.Get(context.Background(), repos.TenantId(tenant), repos.ObjectId(objectID.String()), repos.Strong())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.JSONEq(t, `{"n":2}`, string(got.Payload))
	assert.Empty(t, listObjects(t, store, tenant, typeID, repos.Page{Size: 10}).Items)

	requireUpdated(t, putObject(t, store,
		objectAt(tenant, objectID, owner, typeID, base+2000, `{"n":3}`), version(2)), 2, 3)
	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 1, listedIDs(list.Items))
	assert.JSONEq(t, `{"n":3}`, string(list.Items[0].Payload))
	assert.EqualValues(t, base+2000, list.Items[0].UpdatedAtMs)
	for _, row := range typeIndexRowsOf(t, session, keyspace, tenant, typeID, objectID) {
		assert.NotEqual(t, base+1000, row.updatedAtMs)
	}
}

func TestObjectStoreListSummaryKeepsTheIndexRowRule(t *testing.T) {
	t.Parallel()
	store, _, _ := openTypeIndexStore(t)
	tenant, owner := uuid.NewString(), gocql.MustRandomUUID()
	base := time.Now().Add(-time.Hour).UnixMilli()
	const typeID = "aircraft"

	// A summary cut at 1024 characters is not valid JSON: the list holds "{}",
	// as it did when the summary came from the index row.
	long := gocql.TimeUUID()
	putObject(t, store, objectAt(tenant, long, owner, typeID, base, `{"text":"`+strings.Repeat("x", 2000)+`"}`), nil)
	nested := gocql.TimeUUID()
	putObject(t, store, objectAt(tenant, nested, owner, typeID, base+1, `{ "b": 2, "a": [1, 2] }`), nil)

	list := listObjects(t, store, tenant, typeID, repos.Page{Size: 10})
	require.Len(t, list.Items, 2)
	require.Equal(t, repos.ObjectId(nested.String()), list.Items[0].ID)
	assert.JSONEq(t, `{"a":[1,2],"b":2}`, string(list.Items[0].Payload))
	require.Equal(t, repos.ObjectId(long.String()), list.Items[1].ID)
	assert.JSONEq(t, `{}`, string(list.Items[1].Payload))
}
