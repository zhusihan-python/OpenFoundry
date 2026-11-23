//go:build integration

package repo

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
)

func TestEnforcedChatPurposeLedger(t *testing.T) {
	ctx := t.Context()
	db := testingx.BootPostgres(ctx, t)
	require.NoError(t, Migrate(ctx, db.Pool))
	r := Repo{Pool: db.Pool}
	caller, actor := uuid.New(), uuid.New()
	rawJustification := "   Reconcile synthetic orders for the weekly finance report.   "
	req := authmw.PurposeCheckpointRequest{
		InteractionType: "ai_chat_completion", ActorID: &actor, PurposeJustification: &rawJustification,
		Tags: []string{"untrusted-secret"}, Evidence: json.RawMessage(`{"operation":"chat/completions","network_scope":"local","model_output":"untrusted-secret"}`),
	}
	// The canonical outbox deletes after enqueue. Observe the INSERT in this
	// test database so both the event contents and transaction are exercised.
	db.MustExec(ctx, `CREATE TABLE test_audit_payloads (payload jsonb);
		CREATE FUNCTION test_capture_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO test_audit_payloads VALUES (NEW.payload); RETURN NEW; END $$;
		CREATE TRIGGER observe_audit AFTER INSERT ON outbox.events FOR EACH ROW EXECUTE FUNCTION test_capture_audit();`)
	first, err := r.EnforceChatPurpose(ctx, caller, req)
	require.NoError(t, err)
	require.True(t, first.Approved)
	require.NotEqual(t, uuid.Nil, first.RecordID)
	short := "tiny"
	req.PurposeJustification = &short
	second, err := r.EnforceChatPurpose(ctx, caller, req)
	require.NoError(t, err)
	require.False(t, second.Approved)
	require.Equal(t, "pending_justification", second.Status)
	for _, viewer := range []uuid.UUID{actor, caller, uuid.New()} {
		items, err := r.ListPurposeRecordsByInteraction(ctx, req.InteractionType, 50, viewer, false)
		require.NoError(t, err)
		if viewer == actor || viewer == caller {
			require.Len(t, items, 2)
		} else {
			require.Empty(t, items)
		}
	}
	items, err := r.ListPurposeRecordsByInteraction(ctx, req.InteractionType, 50, uuid.New(), true)
	require.NoError(t, err)
	require.Len(t, items, 2)
	for _, item := range items {
		require.Equal(t, caller, *item.EnforcedBy)
		require.Equal(t, actor, *item.ActorID)
		require.NotContains(t, string(item.Evidence), "untrusted-secret")
		if item.ID == first.RecordID {
			require.Equal(t, "Reconcile synthetic orders for the weekly finance report.", *item.PurposeJustification)
		}
	}
	var n int
	require.NoError(t, db.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM test_audit_payloads WHERE payload->>'actor_id'=$1 AND payload->'payload'->>'enforced_by'=$2 AND payload->>'kind'='purpose_checkpoint.evaluated' AND payload::text NOT LIKE '%untrusted-secret%' AND payload::text NOT LIKE '%weekly finance%'`, actor.String(), caller.String()).Scan(&n))
	require.Equal(t, 2, n)

	db.MustExec(ctx, `ALTER TABLE outbox.events ADD CONSTRAINT test_refuse_audit CHECK (false)`)
	_, err = r.EnforceChatPurpose(ctx, caller, req)
	require.Error(t, err)
	require.NoError(t, db.Pool.QueryRow(ctx, `SELECT count(*) FROM purpose_records WHERE enforced_by IS NOT NULL`).Scan(&n))
	require.Equal(t, 2, n)
	db.MustExec(ctx, `ALTER TABLE outbox.events DROP CONSTRAINT test_refuse_audit;
		UPDATE checkpoint_policies SET rules='[{"key":"unsupported","expected":"true"}]' WHERE slug='ai-private-network'`)
	_, err = r.EnforceChatPurpose(ctx, caller, req)
	require.Error(t, err)
}
