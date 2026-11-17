package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

func admitParams() AdmitParams {
	tenant := scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: uuid.MustParse("33333333-3333-3333-3333-333333333333")}
	event := scheduleevents.DeriveEventID(tenant, "nightly", 2, time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC), scheduleevents.EventTypeTriggered)
	return AdmitParams{
		EventID:            event,
		Tenant:             tenant,
		ScheduleKey:        "nightly",
		DefinitionRevision: 2,
		ScheduledFor:       time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC),
		PipelineRID:        "ri.foundry.main.pipeline.11111111-1111-1111-1111-111111111111",
		BuildBranch:        "main",
		GrantRef:           uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		GrantGeneration:    4,
		BuildID:            scheduleevents.DeriveBuildID(event),
	}
}

func grantRowFor(p AdmitParams) *SchedulingGrant {
	id := p.GrantRef
	org := p.Tenant.ID
	return &SchedulingGrant{
		ID: id, TenantKind: string(p.Tenant.Kind), TenantID: p.Tenant.ID, OrgID: &org,
		Subject: uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		ScheduleKey: p.ScheduleKey, PipelineRID: p.PipelineRID,
		Generation: p.GrantGeneration, State: "active",
		ExpiresAt: time.Now().Add(time.Hour), TokenCiphertext: []byte("ct"),
	}
}

func expectGrantRow(mock pgxmock.PgxPoolIface, p AdmitParams, g *SchedulingGrant) {
	rows := pgxmock.NewRows([]string{"id", "tenant_kind", "tenant_id", "org_id", "subject", "schedule_key", "pipeline_rid", "generation", "state", "expires_at", "token_ciphertext"})
	org := ""
	if g != nil && g.OrgID != nil {
		org = g.OrgID.String()
	}
	rows.AddRow(g.ID.String(), g.TenantKind, g.TenantID.String(), org, g.Subject.String(), g.ScheduleKey, g.PipelineRID, g.Generation, g.State, g.ExpiresAt, g.TokenCiphertext)
	mock.ExpectQuery(`SELECT id::text, tenant_kind, tenant_id::text, COALESCE\(org_id::text,''`).
		WithArgs(string(p.Tenant.Kind), p.Tenant.ID, p.ScheduleKey).WillReturnRows(rows)
}

// expectDefinitionBinding mocks the CURRENT binding matching p.
func expectDefinitionBinding(mock pgxmock.PgxPoolIface, p AdmitParams) {
	rows := pgxmock.NewRows([]string{"enabled", "definition_revision", "pipeline_rid", "build_branch", "grant_ref"})
	rows.AddRow(true, p.DefinitionRevision, p.PipelineRID, p.BuildBranch, p.GrantRef.String())
	mock.ExpectQuery("SELECT enabled, definition_revision, pipeline_rid, build_branch, grant_ref::text").
		WithArgs(string(p.Tenant.Kind), p.Tenant.ID, p.ScheduleKey).
		WillReturnRows(rows)
}

func TestAdmitDispatch_FirstAdmissionSucceeds(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	g := grantRowFor(p)

	mock.ExpectBegin()
	// Dispatch insert wins the race (RETURNING state).
	mock.ExpectQuery("INSERT INTO scheduling.dispatches").
		WithArgs(p.EventID, "org", p.Tenant.ID, p.ScheduleKey, p.DefinitionRevision, p.ScheduledFor,
			p.PipelineRID, p.BuildBranch, p.GrantRef, p.GrantGeneration, p.BuildID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"state"}).AddRow("admitted"))
	expectGrantRow(mock, p, g)
	expectDefinitionBinding(mock, p)
	// Activity insert acquires the slot.
	mock.ExpectExec("INSERT INTO scheduling.pipeline_activity").
		WithArgs("org", p.Tenant.ID, p.PipelineRID, p.EventID).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	// Build row created in-tx (requested_by = verified grant subject).
	mock.ExpectExec("INSERT INTO builds").
		WithArgs(p.BuildID, p.PipelineRID, p.BuildBranch, g.Subject.String(), p.Tenant.ID).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	res, err := repo.AdmitDispatch(context.Background(), p, nil)
	require.NoError(t, err)
	require.Equal(t, AdmitAdmitted, res.Outcome)
	require.Equal(t, DispatchStateAdmitted, res.Dispatch.State)
	require.NotNil(t, res.Grant)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAdmitDispatch_GrantGenerationExactMatch(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	// Re-granted schedule: current generation is 5, the event carries 4.
	stale := grantRowFor(p)
	stale.Generation = p.GrantGeneration + 1

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO scheduling.dispatches").
		WithArgs(p.EventID, "org", p.Tenant.ID, p.ScheduleKey, p.DefinitionRevision, p.ScheduledFor,
			p.PipelineRID, p.BuildBranch, p.GrantRef, p.GrantGeneration, p.BuildID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"state"}).AddRow("admitted"))
	expectGrantRow(mock, p, stale)
	mock.ExpectExec(`UPDATE scheduling.dispatches SET state=\$2`).
		WithArgs(p.EventID, DispatchStateSkipped, "grant_generation_mismatch").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	res, err := repo.AdmitDispatch(context.Background(), p, nil)
	require.NoError(t, err)
	require.Equal(t, AdmitSkippedGrant, res.Outcome)
	require.Equal(t, DispatchStateSkipped, res.Dispatch.State)
	require.Equal(t, "grant_generation_mismatch", *res.Dispatch.Error)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAdmitDispatch_GrantRefMismatchRejected(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	rotated := grantRowFor(p)
	rotated.ID = uuid.New() // re-granted under a new row id

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO scheduling.dispatches").
		WithArgs(p.EventID, "org", p.Tenant.ID, p.ScheduleKey, p.DefinitionRevision, p.ScheduledFor,
			p.PipelineRID, p.BuildBranch, p.GrantRef, p.GrantGeneration, p.BuildID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"state"}).AddRow("admitted"))
	expectGrantRow(mock, p, rotated)
	mock.ExpectExec(`UPDATE scheduling.dispatches SET state=\$2`).
		WithArgs(p.EventID, DispatchStateSkipped, "grant_ref_mismatch").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	res, err := repo.AdmitDispatch(context.Background(), p, nil)
	require.NoError(t, err)
	require.Equal(t, AdmitSkippedGrant, res.Outcome)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAdmitDispatch_OverlapIsTerminalBlocked(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	g := grantRowFor(p)

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO scheduling.dispatches").
		WithArgs(p.EventID, "org", p.Tenant.ID, p.ScheduleKey, p.DefinitionRevision, p.ScheduledFor,
			p.PipelineRID, p.BuildBranch, p.GrantRef, p.GrantGeneration, p.BuildID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"state"}).AddRow("admitted"))
	expectGrantRow(mock, p, g)
	expectDefinitionBinding(mock, p)
	// Slot already held by ANOTHER dispatch: insert no-ops...
	mock.ExpectExec("INSERT INTO scheduling.pipeline_activity").
		WithArgs("org", p.Tenant.ID, p.PipelineRID, p.EventID).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	// ...and the live slot points elsewhere.
	mock.ExpectQuery("SELECT active_dispatch_id FROM scheduling.pipeline_activity").
		WithArgs("org", p.Tenant.ID, p.PipelineRID).
		WillReturnRows(pgxmock.NewRows([]string{"active_dispatch_id"}).AddRow(uuid.New()))
	mock.ExpectExec(`UPDATE scheduling.dispatches SET state=\$2`).
		WithArgs(p.EventID, DispatchStateBlocked, "overlap").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	res, err := repo.AdmitDispatch(context.Background(), p, nil)
	require.NoError(t, err)
	require.Equal(t, AdmitBlockedOverlap, res.Outcome)
	require.Equal(t, DispatchStateBlocked, res.Dispatch.State) // terminal Forbid skip
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAdmitDispatch_DuplicateTerminalSkips(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()

	mock.ExpectBegin()
	// Insert loses the race: ON CONFLICT DO NOTHING -> no RETURNING row.
	mock.ExpectQuery("INSERT INTO scheduling.dispatches").
		WithArgs(p.EventID, "org", p.Tenant.ID, p.ScheduleKey, p.DefinitionRevision, p.ScheduledFor,
			p.PipelineRID, p.BuildBranch, p.GrantRef, p.GrantGeneration, p.BuildID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"state"})) // no row = ErrNoRows
	// Routing read: dispatch already terminal.
	mock.ExpectQuery("SELECT event_id, tenant_kind, tenant_id, schedule_key").
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{
			"event_id", "tenant_kind", "tenant_id", "schedule_key", "definition_revision",
			"scheduled_for", "pipeline_rid", "build_branch", "grant_ref", "grant_generation",
			"build_id", "state", "attempts", "fence_token"}).
			AddRow(p.EventID.String(), "org", p.Tenant.ID.String(), p.ScheduleKey, p.DefinitionRevision,
				p.ScheduledFor, p.PipelineRID, p.BuildBranch, p.GrantRef.String(), p.GrantGeneration,
				p.BuildID.String(), DispatchStateSucceeded, 0, int64(1)))
	mock.ExpectCommit() // duplicate-terminal route performs no writes; empty tx commits

	res, err := repo.AdmitDispatch(context.Background(), p, nil)
	require.NoError(t, err)
	require.Equal(t, AdmitDuplicateTerminal, res.Outcome)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClaimAndHeartbeatFence(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	grant := grantRowFor(p)

	// Claim: lock row -> verify grant -> verify activity -> new fence on
	// both rows, all in one transaction.
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT d.state, d.grant_ref::text").
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"state", "grant_ref", "grant_generation", "lease_expires_at"}).
			AddRow("admitted", grant.ID.String(), grant.Generation, nil))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	mock.ExpectQuery("SELECT state, expires_at FROM scheduling.grants").
		WithArgs(grant.ID.String(), grant.Generation).
		WillReturnRows(pgxmock.NewRows([]string{"state", "expires_at"}).
			AddRow("active", time.Now().Add(time.Hour)))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	mock.ExpectQuery("SELECT active_dispatch_id FROM scheduling.pipeline_activity").
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"active_dispatch_id"}).AddRow(p.EventID.String()))
	mock.ExpectQuery("UPDATE scheduling.dispatches").
		WithArgs(p.EventID, "worker-a", (30 * time.Second).String()).
		WillReturnRows(pgxmock.NewRows([]string{"fence_token"}).AddRow(int64(7)))
	mock.ExpectExec(`UPDATE scheduling.pipeline_activity SET fence_token=\$2`).
		WithArgs(p.EventID, int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	fence, ok, err := repo.ClaimDispatchForExecution(context.Background(), p.EventID, "worker-a", 30*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(7), fence)

	// Heartbeat with matching owner+fence+unexpired lease succeeds
	// (post-lock verify then unconditional extend).
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT state, COALESCE\(lease_owner,''\), fence_token, COALESCE\(lease_expires_at, to_timestamp\(0\)\), build_id::text`).
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"state", "lease_owner", "fence_token", "lease_expires_at", "build_id"}).
			AddRow("executing", "worker-a", int64(7), time.Now().Add(time.Minute), p.BuildID.String()))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	mock.ExpectExec("UPDATE scheduling.dispatches").
		WithArgs(p.EventID, (30 * time.Second).String()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()
	require.NoError(t, repo.HeartbeatDispatch(context.Background(), p.EventID, "worker-a", 7, 30*time.Second))

	// Stale fence fails closed at the post-lock verification: the row
	// carries fence 8 while the caller still believes 7.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT state, COALESCE\(lease_owner,''\), fence_token, COALESCE\(lease_expires_at, to_timestamp\(0\)\), build_id::text`).
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"state", "lease_owner", "fence_token", "lease_expires_at", "build_id"}).
			AddRow("executing", "worker-a", int64(8), time.Now().Add(time.Minute), p.BuildID.String()))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	mock.ExpectRollback()
	require.ErrorIs(t, repo.HeartbeatDispatch(context.Background(), p.EventID, "worker-a", 7, 30*time.Second), ErrStaleFence)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClaimRejectsRevokedGrant(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	grant := grantRowFor(p)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT d.state, d.grant_ref::text").
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"state", "grant_ref", "grant_generation", "lease_expires_at"}).
			AddRow("admitted", grant.ID.String(), grant.Generation, nil))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	// Grant was revoked between admission and claim: no claim.
	mock.ExpectQuery("SELECT state, expires_at FROM scheduling.grants").
		WithArgs(grant.ID.String(), grant.Generation).
		WillReturnRows(pgxmock.NewRows([]string{"state", "expires_at"}).
			AddRow("revoked", time.Now().Add(time.Hour)))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	mock.ExpectRollback()

	_, ok, err := repo.ClaimDispatchForExecution(context.Background(), p.EventID, "worker-a", 30*time.Second)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPoisonCounterPersists(t *testing.T) {
	mock, repo := newMockRepo(t)
	mock.ExpectQuery("INSERT INTO scheduling.poison_messages").
		WithArgs("pipeline.builds.v1", 3, int64(42), "bad json").
		WillReturnRows(pgxmock.NewRows([]string{"attempts"}).AddRow(1))
	n, err := repo.RecordPoisonAttempt(context.Background(), "pipeline.builds.v1", 3, 42, "bad json")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	mock.ExpectQuery("INSERT INTO scheduling.poison_messages").
		WithArgs("pipeline.builds.v1", 3, int64(42), "bad json 2").
		WillReturnRows(pgxmock.NewRows([]string{"attempts"}).AddRow(2))
	n, err = repo.RecordPoisonAttempt(context.Background(), "pipeline.builds.v1", 3, 42, "bad json 2")
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecordPendingReceiptFenced(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	key := scheduleevents.DerivePublishKey(p.EventID, "node-1")
	body := []byte(`{"rows":[]}`)

	// FencedWrite: lock+verify, then the receipt insert, in one tx.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT state, COALESCE\(lease_owner,''\), fence_token, COALESCE\(lease_expires_at, to_timestamp\(0\)\), build_id::text`).
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"state", "lease_owner", "fence_token", "lease_expires_at", "build_id"}).
			AddRow("executing", "worker-a", int64(7), time.Now().Add(time.Minute), p.BuildID.String()))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	mock.ExpectExec("INSERT INTO scheduling.publish_receipts").
		WithArgs(key, p.EventID, "node-1", "ri.foundry.main.dataset.55", body, sha256Hex(body)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	require.NoError(t, repo.RecordPendingReceipt(context.Background(), p.EventID, "worker-a", 7, key, "node-1", "ri.foundry.main.dataset.55", body))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecordPendingReceiptKeyConflictRejected(t *testing.T) {
	mock, repo := newMockRepo(t)
	p := admitParams()
	key := scheduleevents.DerivePublishKey(p.EventID, "node-1")

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT state, COALESCE\(lease_owner,''\), fence_token, COALESCE\(lease_expires_at, to_timestamp\(0\)\), build_id::text`).
		WithArgs(p.EventID).
		WillReturnRows(pgxmock.NewRows([]string{"state", "lease_owner", "fence_token", "lease_expires_at", "build_id"}).
			AddRow("executing", "worker-a", int64(7), time.Now().Add(time.Minute), p.BuildID.String()))
	mock.ExpectQuery("SELECT clock_timestamp").
		WillReturnRows(pgxmock.NewRows([]string{"clock_timestamp"}).AddRow(time.Now()))
	// Key already exists (conflict) with a DIFFERENT body hash.
	mock.ExpectExec("INSERT INTO scheduling.publish_receipts").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	mock.ExpectQuery("SELECT dispatch_event_id::text, node_id, request_sha256, dataset_rid").
		WithArgs(key).
		WillReturnRows(pgxmock.NewRows([]string{"dispatch_event_id", "node_id", "request_sha256", "dataset_rid"}).
			AddRow(p.EventID.String(), "node-1", "different-hash", "ri.foundry.main.dataset.55"))
	mock.ExpectRollback()

	err := repo.RecordPendingReceipt(context.Background(), p.EventID, "worker-a", 7, key, "node-1", "ri.foundry.main.dataset.55", []byte(`{"rows":[]}`))
	require.ErrorIs(t, err, ErrReceiptKeyConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPersonalDomainTenantIDMapping(t *testing.T) {
	// ⑫: personal builds keep tenant_id NULL on the build row; org
	// builds carry the org id. Verified at the parameter level.
	orgParams := admitParams()
	require.Equal(t, orgParams.Tenant.ID, tenantIDForScheduling(orgParams))

	personal := admitParams()
	personal.Tenant = scheduleevents.TenantRef{Kind: scheduleevents.TenantKindPersonal, ID: uuid.MustParse("44444444-4444-4444-4444-444444444444")}
	require.Nil(t, tenantIDForScheduling(personal))
}
