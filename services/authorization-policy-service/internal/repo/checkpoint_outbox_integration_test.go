//go:build integration

package repo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
)

func TestPurposeAuditWithPlatformOwnedOutbox(t *testing.T) {
	ctx := t.Context()
	db := testingx.BootPostgres(ctx, t)
	chartSQL := func(name string) string {
		t.Helper()
		body, err := os.ReadFile(filepath.Join("../../../../infra/helm/infra/postgres-clusters/files", name))
		require.NoError(t, err)
		return string(body)
	}
	db.MustExec(ctx, chartSQL("pg-policy-bootstrap.sql"))
	db.MustExec(ctx, "CREATE EXTENSION pgcrypto")
	cfg, err := pgxpool.ParseConfig(db.URL)
	require.NoError(t, err)
	cfg.ConnConfig.User = "svc_checkpoints_purpose"
	cfg.ConnConfig.Password = uuid.NewString() // generated UUID is SQL-literal safe
	db.MustExec(ctx, "ALTER ROLE svc_checkpoints_purpose PASSWORD '"+cfg.ConnConfig.Password+"'")
	cfg.ConnConfig.RuntimeParams["search_path"] = "checkpoints_purpose,public"
	writer, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer writer.Close()

	require.NoError(t, Migrate(ctx, writer))
	justification := "Reconcile synthetic orders for the weekly finance report."
	actor, caller := uuid.New(), uuid.New()
	req := authmw.PurposeCheckpointRequest{
		InteractionType: "ai_chat_completion", ActorID: &actor, PurposeJustification: &justification,
		Evidence: json.RawMessage(`{"operation":"chat/completions","network_scope":"local"}`),
	}
	repo := Repo{Pool: writer}

	// The pre-upgrade chart grants INSERT and DELETE but cannot read event_id.
	_, err = repo.EnforceChatPurpose(ctx, caller, req)
	require.ErrorContains(t, err, "permission denied")
	var receipts int
	require.NoError(t, writer.QueryRow(ctx, "SELECT COUNT(*) FROM purpose_records").Scan(&receipts))
	require.Zero(t, receipts)

	// The existing cluster owner can replay the same narrow grant included in initdb.
	grant := chartSQL("pg-policy-checkpoint-outbox-grant.sql")
	db.MustExec(ctx, grant)
	db.MustExec(ctx, grant)
	require.NoError(t, Migrate(ctx, writer))
	var leastPrivilege bool
	require.NoError(t, writer.QueryRow(ctx, `SELECT
		current_user = 'svc_checkpoints_purpose'
		AND pg_get_userbyid(relowner) <> current_user AND relreplident = 'f'
		AND NOT has_database_privilege(current_database(), 'CREATE')
		AND NOT has_schema_privilege('outbox', 'CREATE')
		AND has_column_privilege('outbox.events', 'event_id', 'SELECT')
		AND NOT has_column_privilege('outbox.events', 'payload', 'SELECT')
		AND NOT has_column_privilege('svc_cipher', 'outbox.events', 'event_id', 'SELECT')
		FROM pg_class WHERE oid = 'outbox.events'::regclass`).Scan(&leastPrivilege))
	require.True(t, leastPrivilege)
	receipt, err := repo.EnforceChatPurpose(ctx, caller, req)
	require.NoError(t, err)
	require.True(t, receipt.Approved)
	require.NoError(t, writer.QueryRow(ctx, "SELECT count(*) FROM purpose_records WHERE id = $1", receipt.RecordID).Scan(&receipts))
	require.Equal(t, 1, receipts)
	var pending int
	require.NoError(t, db.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox.events").Scan(&pending))
	require.Zero(t, pending, "the shared enqueue must delete the event in the same transaction")

	// A malformed shared CDC setup must not silently pass the startup check.
	db.MustExec(ctx, "ALTER TABLE outbox.events REPLICA IDENTITY DEFAULT")
	require.ErrorContains(t, Migrate(ctx, writer), "outbox owner must set REPLICA IDENTITY FULL")
}
