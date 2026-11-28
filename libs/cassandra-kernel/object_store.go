package cassandrakernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	repos "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
)

// ObjectStore (P2.5.2) is the Cassandra-backed implementation of
// repos.ObjectStore mirroring libs/cassandra-kernel/src/repos.rs::
// CassandraObjectStore. It writes against the `objects_by_id`
// primary table plus three secondary index tables
// (`objects_by_type`, `objects_by_owner`, `objects_by_marking`).
//
// Optimistic concurrency is enforced with single-row LWTs
// (INSERT IF NOT EXISTS / UPDATE IF revision_number = ?) per
// ADR-0020 §"opt-in strong reads".
//
// Index writes are best-effort fan-out (3 unbatched executes
// across 3 partitions): a write that fails between them leaves the
// primary row and an index disagreeing until the object is next written.
//
// The type index (objects_by_type) is keyed by the object's updated_at,
// so a write would otherwise leave one live row per revision behind. An
// update or a soft delete removes the row it supersedes, bounded by the
// write time that row held before the write, and ListByType keeps only
// the row that still is the object's current one.
type ObjectStore struct {
	session  *gocql.Session
	keyspace string
}

// NewObjectStore builds a store bound to the standard
// `ontology_objects` keyspace.
func NewObjectStore(session *gocql.Session) *ObjectStore {
	return &ObjectStore{session: session, keyspace: "ontology_objects"}
}

// NewObjectStoreWithKeyspace allows a custom keyspace (multi-tenant
// override).
func NewObjectStoreWithKeyspace(session *gocql.Session, keyspace string) *ObjectStore {
	return &ObjectStore{session: session, keyspace: keyspace}
}

// Compile-time interface assertion.
var _ repos.ObjectStore = (*ObjectStore)(nil)

// CQL prepared statements — gocql caches statements automatically
// per session, so we just re-issue the same string every call.
// These accessors make the SQL site-of-truth obvious.

func (s *ObjectStore) cqlInsertIfNotExists() string {
	return fmt.Sprintf(
		`INSERT INTO %s.objects_by_id
            (tenant, object_id, type_id, owner_id, properties, marking,
             organization_id, revision_number, created_at, updated_at, deleted)
         VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, false) IF NOT EXISTS`,
		s.keyspace)
}

func (s *ObjectStore) cqlUpdateIfVersion() string {
	return fmt.Sprintf(
		`UPDATE %s.objects_by_id
            SET type_id = ?, owner_id = ?, properties = ?, marking = ?,
                organization_id = ?, revision_number = ?, updated_at = ?,
                deleted = false
          WHERE tenant = ? AND object_id = ?
          IF revision_number = ?`, s.keyspace)
}

func (s *ObjectStore) cqlSelectByID() string {
	return fmt.Sprintf(
		`SELECT type_id, owner_id, properties, marking, organization_id,
                revision_number, created_at, updated_at, deleted
           FROM %s.objects_by_id WHERE tenant = ? AND object_id = ?`,
		s.keyspace)
}

func (s *ObjectStore) cqlSoftDeleteByID() string {
	return fmt.Sprintf(
		`UPDATE %s.objects_by_id SET deleted = true, updated_at = ?
          WHERE tenant = ? AND object_id = ?`, s.keyspace)
}

func (s *ObjectStore) cqlInsertIndexByType() string {
	return fmt.Sprintf(
		`INSERT INTO %s.objects_by_type
            (tenant, type_id, updated_at, object_id, owner_id, marking,
             properties_summary, deleted)
         VALUES (?, ?, ?, ?, ?, ?, ?, false)`, s.keyspace)
}

func (s *ObjectStore) cqlInsertIndexByOwner() string {
	return fmt.Sprintf(
		`INSERT INTO %s.objects_by_owner
            (tenant, owner_id, type_id, object_id, updated_at, deleted)
         VALUES (?, ?, ?, ?, ?, false)`, s.keyspace)
}

func (s *ObjectStore) cqlInsertIndexByMarking() string {
	return fmt.Sprintf(
		`INSERT INTO %s.objects_by_marking
            (tenant, marking_id, object_id, type_id, owner_id, updated_at, deleted)
         VALUES (?, ?, ?, ?, ?, ?, false)`, s.keyspace)
}

func (s *ObjectStore) cqlSelectByType() string {
	return fmt.Sprintf(
		`SELECT object_id, owner_id, marking, properties_summary,
                updated_at, deleted
           FROM %s.objects_by_type WHERE tenant = ? AND type_id = ?`,
		s.keyspace)
}

func (s *ObjectStore) cqlSelectByOwner() string {
	return fmt.Sprintf(
		`SELECT object_id, type_id, updated_at, deleted
           FROM %s.objects_by_owner WHERE tenant = ? AND owner_id = ?`,
		s.keyspace)
}

func (s *ObjectStore) cqlSelectByMarking() string {
	return fmt.Sprintf(
		`SELECT object_id, type_id, owner_id, updated_at, deleted
           FROM %s.objects_by_marking WHERE tenant = ? AND marking_id = ?`,
		s.keyspace)
}

// cqlSelectTypeIndexKeyOfID reads, from the primary row, the key its current
// state has in objects_by_type: the object's type and updated_at.
func (s *ObjectStore) cqlSelectTypeIndexKeyOfID() string {
	return fmt.Sprintf(
		`SELECT type_id, updated_at FROM %s.objects_by_id WHERE tenant = ? AND object_id = ?`,
		s.keyspace)
}

// cqlSelectTypeIndexWriteTime reads when the cell at one objects_by_type key
// was last written. Every write to the row writes all of its columns in one
// statement, so the summary's write time is the row's.
func (s *ObjectStore) cqlSelectTypeIndexWriteTime() string {
	return fmt.Sprintf(
		`SELECT WRITETIME(properties_summary) FROM %s.objects_by_type
          WHERE tenant = ? AND type_id = ? AND updated_at = ? AND object_id = ?`,
		s.keyspace)
}

// cqlDeleteTypeIndexAt deletes one objects_by_type row, bounded by a write
// time: it covers the cells written up to and including that time and leaves
// any later write to the same key in place.
func (s *ObjectStore) cqlDeleteTypeIndexAt() string {
	return fmt.Sprintf(
		`DELETE FROM %s.objects_by_type USING TIMESTAMP ?
          WHERE tenant = ? AND type_id = ? AND updated_at = ? AND object_id = ?`,
		s.keyspace)
}

// cqlSelectPrimaryRowsOfIDs reads what ListByType returns of `marks` objects
// from their primary rows, in one statement.
func (s *ObjectStore) cqlSelectPrimaryRowsOfIDs(marks int) string {
	return fmt.Sprintf(
		`SELECT object_id, type_id, owner_id, marking, properties, updated_at, deleted
           FROM %s.objects_by_id WHERE tenant = ? AND object_id IN (%s)`,
		s.keyspace, strings.TrimSuffix(strings.Repeat("?, ", marks), ", "))
}

// Get fetches one object by (tenant, id).
func (s *ObjectStore) Get(
	ctx context.Context,
	tenant repos.TenantId,
	id repos.ObjectId,
	consistency repos.ReadConsistency,
) (*repos.Object, error) {
	objectUUID, err := parseUUID("object_id", string(id))
	if err != nil {
		return nil, err
	}

	q := s.session.Query(s.cqlSelectByID(), tenantStr(tenant), objectUUID).
		WithContext(ctx).
		Consistency(cqlConsistency(consistency))

	var (
		typeID       string
		ownerID      gocql.UUID
		properties   string
		marking      []string
		orgID        *gocql.UUID
		revision     int64
		createdAt    time.Time
		updatedAt    time.Time
		deleted      bool
	)
	err = q.Scan(&typeID, &ownerID, &properties, &marking, &orgID, &revision, &createdAt, &updatedAt, &deleted)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, driverErr(err)
	}
	if deleted {
		return nil, nil
	}

	var payload json.RawMessage = []byte(properties)
	if !json.Valid(payload) {
		return nil, repos.Backendf("invalid stored JSON: not parseable")
	}

	var orgStr *string
	if orgID != nil {
		s := orgID.String()
		orgStr = &s
	}
	createdMs := createdAt.UnixMilli()
	owner := repos.OwnerId(ownerID.String())
	markings := make([]repos.MarkingId, 0, len(marking))
	for _, m := range marking {
		markings = append(markings, repos.MarkingId(m))
	}

	return &repos.Object{
		Tenant:         tenant,
		ID:             id,
		TypeID:         repos.TypeId(typeID),
		Version:        uint64(revision),
		Payload:        payload,
		OrganizationID: orgStr,
		CreatedAtMs:    &createdMs,
		UpdatedAtMs:    updatedAt.UnixMilli(),
		Owner:          &owner,
		Markings:       markings,
	}, nil
}

// Put inserts or updates with optimistic concurrency.
func (s *ObjectStore) Put(
	ctx context.Context,
	obj repos.Object,
	expectedVersion *uint64,
) (repos.PutOutcome, error) {
	objectUUID, err := parseUUID("object_id", string(obj.ID))
	if err != nil {
		return repos.PutOutcome{}, err
	}
	if obj.Owner == nil {
		return repos.PutOutcome{}, invalidArg("Object.owner is required by the Cassandra schema")
	}
	ownerUUID, err := parseUUID("owner", string(*obj.Owner))
	if err != nil {
		return repos.PutOutcome{}, err
	}
	orgUUID, err := parseUUIDOpt("organization_id", obj.OrganizationID)
	if err != nil {
		return repos.PutOutcome{}, err
	}
	properties, err := canonicalJSON(obj.Payload)
	if err != nil {
		return repos.PutOutcome{}, invalidArgf("payload is not serialisable: %v", err)
	}
	markings := make([]string, 0, len(obj.Markings))
	for _, m := range obj.Markings {
		markings = append(markings, string(m))
	}
	updatedAt := time.UnixMilli(obj.UpdatedAtMs).UTC()
	createdAt := updatedAt
	if obj.CreatedAtMs != nil {
		createdAt = time.UnixMilli(*obj.CreatedAtMs).UTC()
	}

	if expectedVersion == nil {
		// INSERT IF NOT EXISTS path.
		applied, actual, err := s.applyInsert(ctx, tenantStr(obj.Tenant), objectUUID,
			string(obj.TypeID), ownerUUID, properties, markings, orgUUID,
			createdAt, updatedAt)
		if err != nil {
			return repos.PutOutcome{}, err
		}
		if !applied {
			return repos.VersionConflict(0, actual), nil
		}
		if err := s.writeIndexes(ctx, tenantStr(obj.Tenant), objectUUID,
			string(obj.TypeID), ownerUUID, markings,
			truncateSummary(properties), updatedAt); err != nil {
			return repos.PutOutcome{}, err
		}
		return repos.Inserted(), nil
	}

	expected := *expectedVersion
	newVersion := int64(expected) + 1
	// Read before the write: the row the update supersedes, and when its cell
	// was written. The LWT below only applies while the primary row still is
	// what this read saw, so that is the row to remove afterwards.
	stale, err := s.readStaleTypeIndexRow(ctx, tenantStr(obj.Tenant), objectUUID, &typeIndexKey{string(obj.TypeID), updatedAt})
	if err != nil {
		return repos.PutOutcome{}, err
	}
	applied, actual, err := s.applyUpdate(ctx, tenantStr(obj.Tenant), objectUUID,
		string(obj.TypeID), ownerUUID, properties, markings, orgUUID,
		newVersion, updatedAt, int64(expected))
	if err != nil {
		return repos.PutOutcome{}, err
	}
	if !applied {
		if actual == 0 {
			actual = expected // best-effort fallback when LWT didn't surface a value
		}
		return repos.VersionConflict(expected, actual), nil
	}
	if err := s.writeIndexes(ctx, tenantStr(obj.Tenant), objectUUID,
		string(obj.TypeID), ownerUUID, markings,
		truncateSummary(properties), updatedAt); err != nil {
		return repos.PutOutcome{}, err
	}
	if err := s.removeStaleTypeIndexRow(ctx, tenantStr(obj.Tenant), objectUUID, stale); err != nil {
		return repos.PutOutcome{}, err
	}
	return repos.Updated(expected, uint64(newVersion)), nil
}

func (s *ObjectStore) applyInsert(
	ctx context.Context,
	tenant string,
	objectUUID gocql.UUID,
	typeID string,
	ownerUUID gocql.UUID,
	properties string,
	markings []string,
	orgUUID *gocql.UUID,
	createdAt, updatedAt time.Time,
) (applied bool, actualVersion uint64, err error) {
	q := s.session.Query(s.cqlInsertIfNotExists(),
		tenant, objectUUID, typeID, ownerUUID, properties, markings,
		orgUUID, createdAt, updatedAt).
		WithContext(ctx).
		SerialConsistency(gocql.LocalSerial)

	row := map[string]any{}
	applied, err = q.MapScanCAS(row)
	if err != nil {
		return false, 0, driverErr(err)
	}
	if !applied {
		actualVersion = revisionFromCAS(row)
	}
	return applied, actualVersion, nil
}

func (s *ObjectStore) applyUpdate(
	ctx context.Context,
	tenant string,
	objectUUID gocql.UUID,
	typeID string,
	ownerUUID gocql.UUID,
	properties string,
	markings []string,
	orgUUID *gocql.UUID,
	newVersion int64,
	updatedAt time.Time,
	expectedVersion int64,
) (applied bool, actualVersion uint64, err error) {
	q := s.session.Query(s.cqlUpdateIfVersion(),
		typeID, ownerUUID, properties, markings, orgUUID,
		newVersion, updatedAt,
		tenant, objectUUID, expectedVersion).
		WithContext(ctx).
		SerialConsistency(gocql.LocalSerial)

	row := map[string]any{}
	applied, err = q.MapScanCAS(row)
	if err != nil {
		return false, 0, driverErr(err)
	}
	if !applied {
		actualVersion = revisionFromCAS(row)
	}
	return applied, actualVersion, nil
}

// revisionFromCAS pulls revision_number out of a non-applied LWT
// result map. gocql's MapScanCAS populates the map with the columns
// the IF clause projects on conflict; revision_number is what our
// IF UPDATE compares so we project it on every miss.
func revisionFromCAS(row map[string]any) uint64 {
	if v, ok := row["revision_number"]; ok {
		switch n := v.(type) {
		case int64:
			return uint64(n)
		case int:
			return uint64(n)
		case int32:
			return uint64(n)
		}
	}
	// Fall back to any int64 column projected by the conflict — the
	// Rust impl had the same best-effort behaviour.
	for _, v := range row {
		if n, ok := v.(int64); ok {
			return uint64(n)
		}
	}
	return 0
}

// writeIndexes performs the 3-table fan-out write that the Rust
// impl does. We do not LOGGED-batch because the partitions sit
// across 3 tables (LOGGED batch is the worst case per ADR-0020).
// A failure between the executes leaves the primary row and an
// index disagreeing until the object is next written.
func (s *ObjectStore) writeIndexes(
	ctx context.Context,
	tenant string,
	objectUUID gocql.UUID,
	typeID string,
	ownerUUID gocql.UUID,
	markings []string,
	propertiesSummary string,
	updatedAt time.Time,
) error {
	if err := s.session.Query(s.cqlInsertIndexByType(),
		tenant, typeID, updatedAt, objectUUID, ownerUUID, markings, propertiesSummary).
		WithContext(ctx).Exec(); err != nil {
		return driverErr(err)
	}
	if err := s.session.Query(s.cqlInsertIndexByOwner(),
		tenant, ownerUUID, typeID, objectUUID, updatedAt).
		WithContext(ctx).Exec(); err != nil {
		return driverErr(err)
	}
	for _, m := range markings {
		if err := s.session.Query(s.cqlInsertIndexByMarking(),
			tenant, m, objectUUID, typeID, ownerUUID, updatedAt).
			WithContext(ctx).Exec(); err != nil {
			return driverErr(err)
		}
	}
	return nil
}

// typeIndexKey is one row of objects_by_type: inside the tenant's partition
// of a type, the object's type and updated_at locate the row.
type typeIndexKey struct {
	typeID    string
	updatedAt time.Time
}

func (k typeIndexKey) millis() int64 { return k.updatedAt.UnixMilli() }

// staleTypeIndexRow is the objects_by_type row a write supersedes: its key
// and the write time of the cell that key held before the write.
type staleTypeIndexRow struct {
	typeIndexKey
	writeTime int64 // zero when the key held no cell when read
}

// readStaleTypeIndexRow reads the objects_by_type row the object's primary
// row maps to. It must run before the write that changes the primary row: an
// update then only applies while the primary row still is what this read saw,
// so the key read here is the one the write supersedes, and any write that
// later reuses the key carries a write time past the one read here.
//
// keep names the key the write itself is going to write. When the stale key
// is that key (an update in the same millisecond and type) nothing is
// returned: the write's INSERT overwrites the row, and a delete would take it.
func (s *ObjectStore) readStaleTypeIndexRow(
	ctx context.Context,
	tenant string,
	objectUUID gocql.UUID,
	keep *typeIndexKey,
) (*staleTypeIndexRow, error) {
	var key typeIndexKey
	err := s.session.Query(s.cqlSelectTypeIndexKeyOfID(), tenant, objectUUID).
		WithContext(ctx).Consistency(gocql.LocalQuorum).Scan(&key.typeID, &key.updatedAt)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, driverErr(err)
	}
	if keep != nil && key.typeID == keep.typeID && key.millis() == keep.millis() {
		return nil, nil
	}
	stale := &staleTypeIndexRow{typeIndexKey: key}
	err = s.session.Query(s.cqlSelectTypeIndexWriteTime(), tenant, key.typeID, key.updatedAt, objectUUID).
		WithContext(ctx).Consistency(gocql.LocalQuorum).Scan(&stale.writeTime)
	if errors.Is(err, gocql.ErrNotFound) {
		return stale, nil // nothing at the key when read; a row landing later is left to ListByType
	}
	if err != nil {
		return nil, driverErr(err)
	}
	return stale, nil
}

// removeStaleTypeIndexRow deletes the row a write superseded. The delete is
// bounded by the write time read before that write, so it covers exactly the
// cell that was observed and leaves a write that since has reused the key.
func (s *ObjectStore) removeStaleTypeIndexRow(
	ctx context.Context,
	tenant string,
	objectUUID gocql.UUID,
	stale *staleTypeIndexRow,
) error {
	if stale == nil || stale.writeTime == 0 {
		return nil
	}
	return driverErr(s.session.Query(s.cqlDeleteTypeIndexAt(),
		stale.writeTime, tenant, stale.typeID, stale.updatedAt, objectUUID).
		WithContext(ctx).Exec())
}

// Delete soft-deletes the object by flipping `deleted = true` on
// the primary row and removes its row from objects_by_type, bounded by the
// write time read before the delete (readStaleTypeIndexRow). The owner and
// marking indexes keep their rows: consumers filter those on each index
// row's `deleted` column.
func (s *ObjectStore) Delete(
	ctx context.Context,
	tenant repos.TenantId,
	id repos.ObjectId,
) (bool, error) {
	objectUUID, err := parseUUID("object_id", string(id))
	if err != nil {
		return false, err
	}
	stale, err := s.readStaleTypeIndexRow(ctx, tenantStr(tenant), objectUUID, nil)
	if err != nil {
		return false, err
	}
	now := time.Now().UTC()
	if err := s.session.Query(s.cqlSoftDeleteByID(), now, tenantStr(tenant), objectUUID).
		WithContext(ctx).Exec(); err != nil {
		return false, driverErr(err)
	}
	if err := s.removeStaleTypeIndexRow(ctx, tenantStr(tenant), objectUUID, stale); err != nil {
		return false, err
	}
	// Cassandra does not surface rows_affected for non-LWT writes.
	// Caller is responsible for treating double-deletes as no-ops.
	return true, nil
}

// typeIndexLookupBatch is how many listed objects ListByType checks against
// their primary rows in one read.
const typeIndexLookupBatch = 100

// listedTypeIndexRow is one live objects_by_type row as ListByType read it.
type listedTypeIndexRow struct {
	objectID    gocql.UUID
	updatedAtMs int64
}

// primaryProjection is what ListByType returns of one object, read from its
// primary row.
type primaryProjection struct {
	typeID      string
	ownerID     gocql.UUID
	marking     []string
	payload     json.RawMessage
	updatedAtMs int64
}

// summaryOf is the payload ListByType returns of stored properties: the
// summary the index row carries, which is the canonical properties cut at
// 1024 characters, and "{}" once that cut leaves them short of valid JSON.
// It is a copy: nothing of the full properties stays allocated behind it.
func summaryOf(properties string) json.RawMessage {
	summary := []byte(truncateSummary(properties))
	if json.Valid(summary) {
		return summary
	}
	return []byte("{}")
}

// listedAs is the object as ListByType returns it. The index says which
// objects a type holds and in which order; everything else comes from the
// primary row, so the list shows the current revision even where an earlier
// one shares the index key (the same millisecond and type) and a late write
// of it overwrote the index row.
func (p primaryProjection) listedAs(
	tenant repos.TenantId,
	objectID gocql.UUID,
	typeID repos.TypeId,
) repos.Object {
	owner := repos.OwnerId(p.ownerID.String())
	markings := make([]repos.MarkingId, 0, len(p.marking))
	for _, m := range p.marking {
		markings = append(markings, repos.MarkingId(m))
	}
	updatedMs := p.updatedAtMs
	return repos.Object{
		Tenant:         tenant,
		ID:             repos.ObjectId(objectID.String()),
		TypeID:         typeID,
		Version:        0, // the list does not carry the revision; caller `Get`s for that.
		Payload:        p.payload,
		OrganizationID: organizationIDFromTenant(tenant),
		CreatedAtMs:    &updatedMs,
		UpdatedAtMs:    updatedMs,
		Owner:          &owner,
		Markings:       markings,
	}
}

// listedFromPrimary returns, in index order, what ListByType returns of the
// objects of one batch of index rows: only those whose primary row still maps
// to their row — the object exists, is not deleted, is of typeID and its
// updated_at is the row's key. Each primary row is cut to its summary as it
// is scanned, and nothing of these rows outlives the call.
func (s *ObjectStore) listedFromPrimary(
	ctx context.Context,
	tenant repos.TenantId,
	typeID repos.TypeId,
	listed []listedTypeIndexRow,
	consistency gocql.Consistency,
) ([]repos.Object, error) {
	args := make([]interface{}, 0, len(listed)+1)
	args = append(args, tenantStr(tenant))
	seen := make(map[gocql.UUID]struct{}, len(listed))
	for _, row := range listed {
		if _, dup := seen[row.objectID]; dup {
			continue
		}
		seen[row.objectID] = struct{}{}
		args = append(args, row.objectID)
	}

	primary := make(map[gocql.UUID]primaryProjection, len(seen))
	if len(seen) > 0 {
		iter := s.session.Query(s.cqlSelectPrimaryRowsOfIDs(len(seen)), args...).
			WithContext(ctx).Consistency(consistency).Iter()
		var (
			objectID   gocql.UUID
			rowType    string
			ownerID    gocql.UUID
			marking    []string
			properties *string
			updatedAt  time.Time
			deleted    bool
		)
		for iter.Scan(&objectID, &rowType, &ownerID, &marking, &properties, &updatedAt, &deleted) {
			if deleted {
				continue
			}
			row := primaryProjection{
				typeID:      rowType,
				ownerID:     ownerID,
				marking:     append([]string(nil), marking...),
				updatedAtMs: updatedAt.UnixMilli(),
				payload:     summaryOf(""),
			}
			if properties != nil {
				row.payload = summaryOf(*properties)
			}
			primary[objectID] = row
		}
		if err := iter.Close(); err != nil {
			return nil, driverErr(err)
		}
	}

	items := make([]repos.Object, 0, len(listed))
	for _, row := range listed {
		current, live := primary[row.objectID]
		if live && current.typeID == string(typeID) && current.updatedAtMs == row.updatedAtMs {
			items = append(items, current.listedAs(tenant, row.objectID, typeID))
		}
	}
	return items, nil
}

// ListByType pages through every object of a given type.
//
// The index is read as before. A live index row is returned only when the
// object's primary row still maps to it: the object exists, is not deleted,
// is of this type and its updated_at is the row's key. At most one row of an
// object passes, so rows that earlier writes left behind, rows of deleted
// objects and rows a later write superseded are not returned and no object
// repeats across pages. What is returned of the object is read from the
// primary row, one batch of typeIndexLookupBatch index rows at a time, and
// only the summary of each object is kept.
func (s *ObjectStore) ListByType(
	ctx context.Context,
	tenant repos.TenantId,
	typeID repos.TypeId,
	page repos.Page,
	consistency repos.ReadConsistency,
) (repos.PagedResult[repos.Object], error) {
	pagingState, err := decodePagingState(page.Token)
	if err != nil {
		return repos.PagedResult[repos.Object]{}, err
	}

	q := s.session.Query(s.cqlSelectByType(), tenantStr(tenant), string(typeID)).
		WithContext(ctx).
		Consistency(cqlConsistency(consistency)).
		PageSize(clampPageSize(page.Size))
	if len(pagingState) > 0 {
		q = q.PageState(pagingState)
	}
	iter := q.Iter()

	listed := make([]listedTypeIndexRow, 0)
	var (
		objectID  gocql.UUID
		updatedAt time.Time
		deleted   bool
	)
	// The row's owner, markings and summary are not read: what is returned
	// of the object comes from its primary row.
	for iter.Scan(&objectID, nil, nil, nil, &updatedAt, &deleted) {
		if deleted {
			continue
		}
		listed = append(listed, listedTypeIndexRow{objectID: objectID, updatedAtMs: updatedAt.UnixMilli()})
	}
	out := repos.PagedResult[repos.Object]{Items: []repos.Object{}}
	if pageBytes := iter.PageState(); len(pageBytes) > 0 {
		out.NextToken = encodePagingState(pageBytes)
	}
	if err := iter.Close(); err != nil {
		return repos.PagedResult[repos.Object]{}, driverErr(err)
	}

	for start := 0; start < len(listed); start += typeIndexLookupBatch {
		end := min(start+typeIndexLookupBatch, len(listed))
		items, err := s.listedFromPrimary(ctx, tenant, typeID, listed[start:end], cqlConsistency(consistency))
		if err != nil {
			return repos.PagedResult[repos.Object]{}, err
		}
		out.Items = append(out.Items, items...)
	}
	return out, nil
}

// ListByOwner pages through every object owned by `owner`.
func (s *ObjectStore) ListByOwner(
	ctx context.Context,
	tenant repos.TenantId,
	owner repos.OwnerId,
	page repos.Page,
	consistency repos.ReadConsistency,
) (repos.PagedResult[repos.Object], error) {
	ownerUUID, err := parseUUID("owner_id", string(owner))
	if err != nil {
		return repos.PagedResult[repos.Object]{}, err
	}
	pagingState, err := decodePagingState(page.Token)
	if err != nil {
		return repos.PagedResult[repos.Object]{}, err
	}

	q := s.session.Query(s.cqlSelectByOwner(), tenantStr(tenant), ownerUUID).
		WithContext(ctx).
		Consistency(cqlConsistency(consistency)).
		PageSize(clampPageSize(page.Size))
	if len(pagingState) > 0 {
		q = q.PageState(pagingState)
	}
	iter := q.Iter()

	out := repos.PagedResult[repos.Object]{Items: []repos.Object{}}
	var (
		objectID  gocql.UUID
		typeID    string
		updatedAt time.Time
		deleted   bool
	)
	for iter.Scan(&objectID, &typeID, &updatedAt, &deleted) {
		if deleted {
			continue
		}
		ownerCopy := owner
		updatedMs := updatedAt.UnixMilli()
		out.Items = append(out.Items, repos.Object{
			Tenant:         tenant,
			ID:             repos.ObjectId(objectID.String()),
			TypeID:         repos.TypeId(typeID),
			Version:        0,
			Payload:        json.RawMessage("{}"),
			OrganizationID: organizationIDFromTenant(tenant),
			CreatedAtMs:    &updatedMs,
			UpdatedAtMs:    updatedMs,
			Owner:          &ownerCopy,
			Markings:       nil,
		})
	}
	if pageBytes := iter.PageState(); len(pageBytes) > 0 {
		out.NextToken = encodePagingState(pageBytes)
	}
	if err := iter.Close(); err != nil {
		return repos.PagedResult[repos.Object]{}, driverErr(err)
	}
	return out, nil
}

// ListByMarking pages through every object bearing `marking`.
func (s *ObjectStore) ListByMarking(
	ctx context.Context,
	tenant repos.TenantId,
	marking repos.MarkingId,
	page repos.Page,
	consistency repos.ReadConsistency,
) (repos.PagedResult[repos.Object], error) {
	pagingState, err := decodePagingState(page.Token)
	if err != nil {
		return repos.PagedResult[repos.Object]{}, err
	}

	q := s.session.Query(s.cqlSelectByMarking(), tenantStr(tenant), string(marking)).
		WithContext(ctx).
		Consistency(cqlConsistency(consistency)).
		PageSize(clampPageSize(page.Size))
	if len(pagingState) > 0 {
		q = q.PageState(pagingState)
	}
	iter := q.Iter()

	out := repos.PagedResult[repos.Object]{Items: []repos.Object{}}
	var (
		objectID  gocql.UUID
		typeID    string
		ownerID   gocql.UUID
		updatedAt time.Time
		deleted   bool
	)
	for iter.Scan(&objectID, &typeID, &ownerID, &updatedAt, &deleted) {
		if deleted {
			continue
		}
		owner := repos.OwnerId(ownerID.String())
		markingCopy := marking
		updatedMs := updatedAt.UnixMilli()
		out.Items = append(out.Items, repos.Object{
			Tenant:         tenant,
			ID:             repos.ObjectId(objectID.String()),
			TypeID:         repos.TypeId(typeID),
			Version:        0,
			Payload:        json.RawMessage("{}"),
			OrganizationID: organizationIDFromTenant(tenant),
			CreatedAtMs:    &updatedMs,
			UpdatedAtMs:    updatedMs,
			Owner:          &owner,
			Markings:       []repos.MarkingId{markingCopy},
		})
	}
	if pageBytes := iter.PageState(); len(pageBytes) > 0 {
		out.NextToken = encodePagingState(pageBytes)
	}
	if err := iter.Close(); err != nil {
		return repos.PagedResult[repos.Object]{}, driverErr(err)
	}
	return out, nil
}

// canonicalJSON normalises a possibly-empty json.RawMessage into a
// canonical JSON string for storage. Empty/nil → "{}". Validates the
// input is parseable so we don't write corrupted JSON.
func canonicalJSON(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "{}", nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "{}", nil
	}
	if !json.Valid(raw) {
		return "", fmt.Errorf("payload is not valid JSON")
	}
	// Round-trip through Marshal to canonicalise key ordering.
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", err
	}
	out, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
