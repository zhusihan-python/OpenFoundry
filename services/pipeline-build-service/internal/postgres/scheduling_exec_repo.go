// Scheduling execution-side persistence (WP3.2 A2). Companion to
// scheduling_repo.go (A1, frozen for A2): everything here is NEW surface
// owned by the canonical Runner — same-transaction plan persistence +
// expectation sealing, fenced audit, the execution snapshot (durable
// phase source of truth), and the fenced completion of keyed publishes.
// See docs/architecture/data-integration-wp3-2-a2-checkpoint.md §1–§5.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// ErrGrantStaleAtPublish reports that the grant no longer satisfies the
// dispatch tuple (state/generation/expiry) inside a pre-publish fenced
// transaction — the publish is refused, the dispatch parks.
var ErrGrantStaleAtPublish = errors.New("scheduling grant stale at publish gate")

// AssertDispatchFence verifies (owner, fence, executing, live lease) on the
// dispatch row inside a FencedWrite and commits an empty transaction. Side
// effect entries that must not publish under a lost lease (the reservation
// handoff, and any remote POST gated behind it) call this first: a stale
// fence fails here, BEFORE the external call.
func (r *Repository) AssertDispatchFence(ctx context.Context, eventID uuid.UUID, owner string, fence int64) error {
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		var ok bool
		err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM scheduling.dispatches d JOIN scheduling.grants g ON g.id=d.grant_ref JOIN schedules.definitions s ON s.tenant_kind=d.tenant_kind AND s.tenant_id=d.tenant_id AND s.schedule_key=d.schedule_key
 WHERE d.event_id=$1 AND g.generation=d.grant_generation AND g.state='active' AND g.expires_at>clock_timestamp()
 AND g.tenant_kind=d.tenant_kind AND g.tenant_id=d.tenant_id AND g.pipeline_rid=d.pipeline_rid
 AND s.enabled AND s.dispatch_mode='pipeline_outbox_v1' AND s.definition_revision=d.definition_revision
 AND s.pipeline_rid=d.pipeline_rid AND s.build_branch=d.build_branch AND s.grant_ref=d.grant_ref)`, eventID).Scan(&ok)
		if err != nil {
			return err
		}
		if !ok {
			return ErrGrantStaleAtPublish
		}
		return nil
	})
}

// MarkExecutionIntent durably records that the runner is ENTERING the
// remote-side-effect window (about to open output transactions through
// the resolver), BEFORE any RPC (root review: a lost open-ACK with no
// RID must be distinguishable from "never entered"). Fenced.
func (r *Repository) MarkExecutionIntent(ctx context.Context, eventID uuid.UUID, owner string, fence int64, kind string, details json.RawMessage) error {
	if len(details) == 0 {
		details = json.RawMessage(`{}`)
	}
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO scheduling.execution_intents (event_id, kind, details)
			 VALUES ($1,$2,$3)
			 ON CONFLICT (event_id) DO UPDATE SET kind=EXCLUDED.kind, details=EXCLUDED.details, updated_at=NOW()`,
			eventID, kind, details); err != nil {
			return fmt.Errorf("mark execution intent: %w", err)
		}
		return nil
	})
}

// ClearExecutionIntent removes the window marker when the window provably
// closed without opening anything (BuildQueued) — the dispatch may later
// re-enter resolve cleanly. Fenced.
func (r *Repository) ClearExecutionIntent(ctx context.Context, eventID uuid.UUID, owner string, fence int64) error {
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM scheduling.execution_intents WHERE event_id=$1`, eventID); err != nil {
			return fmt.Errorf("clear execution intent: %w", err)
		}
		return nil
	})
}

// RecordPendingPublishWithResult is the pre-POST durable gate for one
// keyed publish, all in ONE fenced transaction:
//   - the dispatch fence is re-verified,
//   - the CURRENT grant (id/generation/active/unexpired, DB clock) is
//     re-checked against the dispatch tuple — a revoked/re-granted grant
//     refuses the publish (root review: the gate needs FRESH state, not
//     the worker's startup snapshot),
//   - the pending receipt (original bytes + fingerprint) is persisted,
//     with the same key-conflict rules as RecordPendingReceipt,
//   - the node's COMPUTATION RESULT evidence is persisted (root review
//     trap 2: a later confirmed receipt + this row is what legitimately
//     flips a publish-unknown FAILED node to COMPLETED).
func (r *Repository) RecordPendingPublishWithResult(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key, nodeID, datasetRID string, body []byte, resultJSON json.RawMessage) error {
	hasResult := len(resultJSON) > 0
	if !hasResult {
		resultJSON = json.RawMessage(`{}`)
	}
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		// Fresh grant check under the same locks as the receipt write.
		var grantID string
		var gen int
		var gState string
		var gExpiry time.Time
		err := tx.QueryRow(ctx,
			`SELECT g.id::text, g.generation, g.state, g.expires_at
			   FROM scheduling.grants g, scheduling.dispatches d
			  WHERE d.event_id=$1 AND g.id=d.grant_ref AND g.generation=d.grant_generation
			    FOR SHARE OF g`, eventID,
		).Scan(&grantID, &gen, &gState, &gExpiry)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrGrantStaleAtPublish
		}
		if err != nil {
			return fmt.Errorf("publish gate grant read: %w", err)
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return fmt.Errorf("publish gate clock: %w", err)
		}
		if gState != "active" || !gExpiry.After(dbNow) {
			return ErrGrantStaleAtPublish
		}
		var bindingOK bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schedules.definitions s JOIN scheduling.dispatches d ON d.event_id=$1
          WHERE s.tenant_kind=d.tenant_kind AND s.tenant_id=d.tenant_id AND s.schedule_key=d.schedule_key
          AND s.dispatch_mode='pipeline_outbox_v1' AND s.enabled AND s.definition_revision=d.definition_revision
          AND s.pipeline_rid=d.pipeline_rid AND s.build_branch=d.build_branch AND s.grant_ref=d.grant_ref)`, eventID).Scan(&bindingOK); err != nil {
			return err
		}
		if !bindingOK {
			return ErrGrantStaleAtPublish
		}
		// Receipt insert with frozen RecordPendingReceipt conflict rules
		// (scheduling_repo.go is A1-frozen; the SQL is mirrored here so
		// the result row lands in the SAME transaction).
		sum := sha256Hex(body)
		tag, err := tx.Exec(ctx,
			`INSERT INTO scheduling.publish_receipts
			   (key, dispatch_event_id, node_id, dataset_rid, request_body, request_sha256, state)
			 VALUES ($1,$2,$3,$4,$5,$6,'pending')
			 ON CONFLICT (key) DO NOTHING`,
			key, eventID, nodeID, datasetRID, body, sum)
		if err != nil {
			return fmt.Errorf("insert pending receipt: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var existingDispatch, existingNode, existingSum, existingDataset string
			if err := tx.QueryRow(ctx,
				`SELECT dispatch_event_id::text, node_id, request_sha256, dataset_rid
				   FROM scheduling.publish_receipts WHERE key=$1`, key,
			).Scan(&existingDispatch, &existingNode, &existingSum, &existingDataset); err != nil {
				return fmt.Errorf("verify receipt key conflict: %w", err)
			}
			if existingDispatch != eventID.String() || existingNode != nodeID ||
				existingSum != sum || existingDataset != datasetRID {
				return ErrReceiptKeyConflict
			}
		}
		if hasResult {
			if _, err := tx.Exec(ctx,
				`INSERT INTO scheduling.node_run_results (dispatch_event_id, node_id, result_metadata)
			 VALUES ($1,$2,$3)
			 ON CONFLICT (dispatch_event_id, node_id) DO NOTHING`,
				eventID, nodeID, resultJSON); err != nil {
				return fmt.Errorf("persist node result: %w", err)
			}
		}
		return nil
	})
}

// PersistResolvedAndSeal persists the resolved plan AND seals the dispatch's
// expectation sets AND flips the build to BUILD_RUNNING in ONE fenced
// transaction (A2 checkpoint §2-1: no crash window between jobs persistence
// and sealing; the runner only reaches here while holding the claim fence).
// Idempotent re-entry (persistResolvedBuild upserts; the seal is
// first-write-wins with same-content tolerance).
func (r *Repository) PersistResolvedAndSeal(ctx context.Context, eventID uuid.UUID, owner string, fence int64, resolved *models.ResolvedBuild, expectedOutputs []ExpectedOutput, expectedNodes []string) error {
	if resolved == nil {
		return errors.New("persist resolved: nil build")
	}
	if len(resolved.Jobs) == 0 {
		// A queued build carries no jobs and must never be sealable —
		// an empty/all-WAITING plan cannot be proved executed.
		return errors.New("persist resolved: refusing to seal a jobless (queued) build")
	}
	outputsJSON, err := json.Marshal(expectedOutputs)
	if err != nil {
		return fmt.Errorf("marshal expected outputs: %w", err)
	}
	nodesJSON, err := json.Marshal(expectedNodes)
	if err != nil {
		return fmt.Errorf("marshal expected nodes: %w", err)
	}
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		if err := persistResolvedBuild(ctx, tx, resolved); err != nil {
			return fmt.Errorf("persist resolved build: %w", err)
		}
		tag, err := tx.Exec(ctx,
			`UPDATE scheduling.dispatches
			    SET expected_outputs=$2, expected_nodes=$3, updated_at=NOW()
			  WHERE event_id=$1
			    AND (expected_outputs IS NULL OR expected_outputs = $2)
			    AND (expected_nodes IS NULL OR expected_nodes = $3)`,
			eventID, outputsJSON, nodesJSON)
		if err != nil {
			return fmt.Errorf("seal expected sets: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Already sealed with DIFFERENT content: a divergence bug,
			// never overwrite (the sets are the frozen success contract).
			return errors.New("seal expected sets: already sealed with different content")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE builds SET state='BUILD_RUNNING', started_at=COALESCE(started_at, NOW())
			  WHERE id=$1 AND state IN ('BUILD_RESOLUTION','BUILD_QUEUED','BUILD_RUNNING')`,
			resolved.BuildID); err != nil {
			return fmt.Errorf("mark build running: %w", err)
		}
		// The persisted plan proves every remote open was recorded: the
		// pre-RPC window marker closes with this same transaction.
		if _, err := tx.Exec(ctx,
			`DELETE FROM scheduling.execution_intents WHERE event_id=$1`, eventID); err != nil {
			return fmt.Errorf("clear execution intent: %w", err)
		}
		return nil
	})
}

// recordAuditEventInTx mirrors Repository.Record's job-state CAS and
// transition append on a caller-provided transaction (Record itself is
// pool-bound and cannot run inside a FencedWrite). Kept in lockstep with
// repository.go's Record semantics: only real per-job transitions apply.
func recordAuditEventInTx(ctx context.Context, db DB, event executor.AuditEvent) error {
	if event.JobID == uuid.Nil || event.From == "" || event.To == "" || event.From == event.To {
		return nil
	}
	from := string(event.From)
	to := string(event.To)
	reason := event.Reason
	var tag pgconn.CommandTag
	var err error
	if isFailureState(event.To) {
		tag, err = db.Exec(ctx, `UPDATE jobs SET state=$2, state_changed_at=NOW(), failure_reason=$3, attempt=GREATEST(attempt, $5) WHERE id=$1 AND state=$4`,
			event.JobID, to, reason, from, event.Attempt)
	} else {
		tag, err = db.Exec(ctx, `UPDATE jobs SET state=$2, state_changed_at=NOW(), attempt=GREATEST(attempt, $4) WHERE id=$1 AND state=$3`,
			event.JobID, to, from, event.Attempt)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	_, err = db.Exec(ctx, `INSERT INTO job_state_transitions (job_id, from_state, to_state, reason) VALUES ($1,$2,$3,$4)`,
		event.JobID, from, to, reason)
	return err
}

// RecordAuditEventFenced applies one executor audit event (node state
// transition) inside a FencedWrite, so job-state writes during scheduled
// execution share the claim's ownership check. This is OBSERVABILITY
// alignment, not the lost-fence guard: the publish-side hard gate is
// RecordPendingReceipt/CompleteScheduledPublish (A2 checkpoint §2-3).
func (r *Repository) RecordAuditEventFenced(ctx context.Context, eventID uuid.UUID, owner string, fence int64, event executor.AuditEvent) error {
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		return recordAuditEventInTx(ctx, tx, event)
	})
}

// ─── Execution snapshot (durable phase source of truth) ────────────────

// DispatchJobState is one persisted job of the dispatch's build.
type DispatchJobState struct {
	JobID      uuid.UUID
	JobSpecRID string
	State      string
}

// DispatchOutputState is one job_outputs row (reservation bookkeeping).
type DispatchOutputState struct {
	JobSpecRID string
	DatasetRID string
	// TransactionRID is the CURRENT recorded transaction (reservation
	// before the keyed commit, the real COMMITTED txn after).
	TransactionRID string
	Committed      bool
	Aborted        bool
}

// DispatchReceiptState is one publish receipt of the dispatch.
type DispatchReceiptState struct {
	Key         string
	NodeID      string
	DatasetRID  string
	State       string
	RequestBody []byte
}

// DispatchPendingTxn is one outstanding build_pending_output_txns row.
type DispatchPendingTxn struct {
	DatasetRID     string
	Branch         string
	TransactionRID string
}

// DispatchExecutionSnapshot aggregates every persisted fact the runner's
// phase machine decides on. Nothing here is derived from memory: a
// restarted process reaches identical decisions.
type DispatchExecutionSnapshot struct {
	BuildState    string
	StartedAt     *time.Time
	BuildFinished bool

	// IntentKind reports the pre-RPC window marker ("" = never entered;
	// "resolve_started" = remote opens may exist without local records).
	IntentKind string

	ExpectedOutputs       []ExpectedOutput
	ExpectedOutputsSealed bool
	ExpectedNodes         []string
	ExpectedNodesSealed   bool

	Jobs []DispatchJobState
	// NodeResultNodes lists node ids with persisted computation results
	// (the FAILED→COMPLETED recovery evidence).
	NodeResultNodes []string
	Outputs         []DispatchOutputState
	Receipts        []DispatchReceiptState
	PendingTxns     []DispatchPendingTxn
}

// LoadDispatchExecutionState reads the full snapshot for a dispatch.
func (r *Repository) LoadDispatchExecutionState(ctx context.Context, eventID uuid.UUID) (*DispatchExecutionSnapshot, error) {
	snap := &DispatchExecutionSnapshot{}
	var buildID uuid.UUID
	var expectedOutputs, expectedNodes []byte
	err := r.db.QueryRow(ctx,
		`SELECT d.build_id, b.state, b.started_at, b.finished_at IS NOT NULL,
		        d.expected_outputs, d.expected_nodes,
		        COALESCE((SELECT i.kind FROM scheduling.execution_intents i WHERE i.event_id=d.event_id), '')
		   FROM scheduling.dispatches d
		   JOIN builds b ON b.id = d.build_id
		  WHERE d.event_id=$1`, eventID,
	).Scan(&buildID, &snap.BuildState, &snap.StartedAt, &snap.BuildFinished, &expectedOutputs, &expectedNodes, &snap.IntentKind)
	if err != nil {
		return nil, fmt.Errorf("snapshot dispatch/build: %w", err)
	}
	if expectedOutputs != nil {
		if err := json.Unmarshal(expectedOutputs, &snap.ExpectedOutputs); err != nil {
			return nil, fmt.Errorf("snapshot expected outputs: %w", err)
		}
		snap.ExpectedOutputsSealed = true
	}
	if expectedNodes != nil {
		if err := json.Unmarshal(expectedNodes, &snap.ExpectedNodes); err != nil {
			return nil, fmt.Errorf("snapshot expected nodes: %w", err)
		}
		snap.ExpectedNodesSealed = true
	}

	rows, err := r.db.Query(ctx,
		`SELECT id, job_spec_rid, state FROM jobs WHERE build_id=$1 ORDER BY job_spec_rid`, buildID)
	if err != nil {
		return nil, fmt.Errorf("snapshot jobs: %w", err)
	}
	for rows.Next() {
		var j DispatchJobState
		if err := rows.Scan(&j.JobID, &j.JobSpecRID, &j.State); err != nil {
			rows.Close()
			return nil, err
		}
		snap.Jobs = append(snap.Jobs, j)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rows, err = r.db.Query(ctx,
		`SELECT node_id FROM scheduling.node_run_results WHERE dispatch_event_id=$1 ORDER BY node_id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("snapshot node results: %w", err)
	}
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			rows.Close()
			return nil, err
		}
		snap.NodeResultNodes = append(snap.NodeResultNodes, nodeID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rows, err = r.db.Query(ctx,
		`SELECT j.job_spec_rid, o.output_dataset_rid, o.transaction_rid, COALESCE(o.committed,FALSE), COALESCE(o.aborted,FALSE)
		   FROM job_outputs o JOIN jobs j ON j.id=o.job_id
		  WHERE j.build_id=$1 ORDER BY o.output_dataset_rid`, buildID)
	if err != nil {
		return nil, fmt.Errorf("snapshot outputs: %w", err)
	}
	for rows.Next() {
		var o DispatchOutputState
		if err := rows.Scan(&o.JobSpecRID, &o.DatasetRID, &o.TransactionRID, &o.Committed, &o.Aborted); err != nil {
			rows.Close()
			return nil, err
		}
		snap.Outputs = append(snap.Outputs, o)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rows, err = r.db.Query(ctx,
		`SELECT key, node_id, dataset_rid, state, request_body
		   FROM scheduling.publish_receipts WHERE dispatch_event_id=$1 ORDER BY key`, eventID)
	if err != nil {
		return nil, fmt.Errorf("snapshot receipts: %w", err)
	}
	for rows.Next() {
		var rec DispatchReceiptState
		if err := rows.Scan(&rec.Key, &rec.NodeID, &rec.DatasetRID, &rec.State, &rec.RequestBody); err != nil {
			rows.Close()
			return nil, err
		}
		snap.Receipts = append(snap.Receipts, rec)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rows, err = r.db.Query(ctx,
		`SELECT dataset_rid, branch, transaction_rid
		   FROM build_pending_output_txns WHERE build_id=$1 ORDER BY transaction_rid`, buildID)
	if err != nil {
		return nil, fmt.Errorf("snapshot pending txns: %w", err)
	}
	for rows.Next() {
		var p DispatchPendingTxn
		if err := rows.Scan(&p.DatasetRID, &p.Branch, &p.TransactionRID); err != nil {
			rows.Close()
			return nil, err
		}
		snap.PendingTxns = append(snap.PendingTxns, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return snap, nil
}

// CompleteScheduledPublish finalizes a usableACK keyed publish in ONE
// fenced transaction (A2 checkpoint §3 step 4): the receipt flips to
// committed with the ACK payload, the job_outputs row's transaction_rid
// becomes the REAL COMMITTED transaction (reservation RID stays in the
// receipt provenance), the row is marked committed, and the reservation's
// pending-txn reference clears. Idempotent: re-completing an already
// committed key is a no-op.
func (r *Repository) CompleteScheduledPublish(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, receipt json.RawMessage, datasetRID, reservationTxnRID, realTxnRID string) error {
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE scheduling.publish_receipts
			    SET state='committed', receipt=$2, updated_at=NOW()
			  WHERE key=$1 AND dispatch_event_id=$3 AND dataset_rid=$4 AND state <> 'committed'`,
			key, receipt, eventID, datasetRID)
		if err != nil {
			return fmt.Errorf("complete publish receipt: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var state string
			if err := tx.QueryRow(ctx,
				`SELECT state FROM scheduling.publish_receipts WHERE key=$1 AND dispatch_event_id=$2 AND dataset_rid=$3`, key, eventID, datasetRID).Scan(&state); err != nil {
				return fmt.Errorf("verify completed receipt: %w", err)
			}
			if state != "committed" {
				return fmt.Errorf("receipt %s in state %s cannot be completed", key, state)
			}
		}
		if realTxnRID != "" && reservationTxnRID != "" {
			if _, err := tx.Exec(ctx, `UPDATE jobs j SET output_transaction_rids=array_replace(j.output_transaction_rids,$2,$3)
             FROM scheduling.dispatches d WHERE d.event_id=$1 AND j.build_id=d.build_id`, eventID, reservationTxnRID, realTxnRID); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx,
				`UPDATE job_outputs SET transaction_rid=$3, committed=TRUE
				  WHERE output_dataset_rid=$1 AND transaction_rid IN ($2,$3) AND job_id IN (SELECT j.id FROM jobs j JOIN scheduling.dispatches d ON d.build_id=j.build_id WHERE d.event_id=$4)`,
				datasetRID, reservationTxnRID, realTxnRID, eventID); err != nil {
				return fmt.Errorf("complete publish job output: %w", err)
			}
		}
		// A computation-success record plus every expected node receipt proves
		// a publish-only failure can be recovered without re-running the node.
		if _, err := tx.Exec(ctx, `UPDATE jobs j SET state='COMPLETED', failure_reason=NULL, state_changed_at=NOW()
          FROM scheduling.dispatches d, scheduling.node_run_results n
          WHERE d.event_id=$1 AND j.build_id=d.build_id AND n.dispatch_event_id=d.event_id
            AND n.node_id=j.job_spec_rid AND j.state IN ('FAILED','RUNNING')
            AND EXISTS (SELECT 1 FROM scheduling.publish_receipts p WHERE p.dispatch_event_id=d.event_id AND p.node_id=n.node_id)
            AND NOT EXISTS (SELECT 1 FROM job_outputs o WHERE o.job_id=j.id AND o.output_dataset_rid IS NOT NULL AND NOT EXISTS
              (SELECT 1 FROM scheduling.publish_receipts p WHERE p.dispatch_event_id=d.event_id AND p.node_id=n.node_id AND p.dataset_rid=o.output_dataset_rid AND p.state='committed'))`, eventID); err != nil {
			return err
		}
		if reservationTxnRID != "" {
			if _, err := tx.Exec(ctx,
				`DELETE FROM build_pending_output_txns p
				  USING scheduling.dispatches d
				  WHERE p.build_id=d.build_id AND d.event_id=$1 AND p.transaction_rid=$2`,
				eventID, reservationTxnRID); err != nil {
				return fmt.Errorf("complete publish clear pending: %w", err)
			}
		}
		return nil
	})
}

// DefinitionBinding is the CURRENT scheduling binding of a definition.
type DefinitionBinding struct {
	Found              bool
	Enabled            bool
	DefinitionRevision int
	PipelineRID        string
	BuildBranch        string
	GrantRef           uuid.UUID
}

// LoadDefinitionBinding re-reads the pipeline_outbox_v1 binding for the
// execution-time five-item check (enabled/revision/pipeline/branch/grant
// must still match the admitted dispatch tuple before any execution or
// publish side effect).
func (r *Repository) LoadDefinitionBinding(ctx context.Context, tenant scheduleevents.TenantRef, scheduleKey string) (DefinitionBinding, error) {
	var b DefinitionBinding
	var grantRef uuid.UUID
	err := r.db.QueryRow(ctx,
		`SELECT enabled, definition_revision, pipeline_rid, build_branch, grant_ref
		   FROM schedules.definitions
		  WHERE dispatch_mode='pipeline_outbox_v1'
		    AND tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`,
		string(tenant.Kind), tenant.ID, scheduleKey,
	).Scan(&b.Enabled, &b.DefinitionRevision, &b.PipelineRID, &b.BuildBranch, &grantRef)
	if errors.Is(err, pgx.ErrNoRows) {
		return DefinitionBinding{}, nil
	}
	if err != nil {
		return DefinitionBinding{}, fmt.Errorf("load definition binding: %w", err)
	}
	b.Found = true
	b.GrantRef = grantRef
	return b, nil
}

// ListPipelineOutputDatasets returns the distinct output datasets the
// pipeline's job specs declare for the build branch (then the fallback
// chain), tenant-scoped like Lookup. This is the scheduled resolve args'
// output set — the trigger payload deliberately carries no dataset list.
func (r *Repository) ListPipelineOutputDatasets(ctx context.Context, pipelineRID, buildBranch string, fallbackChain []string) ([]string, error) {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	branches := append([]string{buildBranch}, fallbackChain...)
	for _, branch := range branches {
		branch = strings.TrimSpace(branch)
		if branch == "" {
			continue
		}
		where := "pipeline_rid=$1 AND branch_name=$2"
		args := []any{pipelineRID, branch}
		if scope := scopeFromContext(ctx); scope != nil {
			sb := &sqlBuilder{args: args}
			appendTenantClause(sb, scope, "pipeline_job_specs")
			where += sb.whereSQL()
			args = sb.args
		}
		rows, err := r.db.Query(ctx, `SELECT DISTINCT output_dataset_rid FROM pipeline_job_specs WHERE `+where, args...)
		if err != nil {
			return nil, fmt.Errorf("list pipeline outputs: %w", err)
		}
		for rows.Next() {
			var rid string
			if err := rows.Scan(&rid); err != nil {
				rows.Close()
				return nil, err
			}
			if _, dup := seen[rid]; dup {
				continue
			}
			seen[rid] = struct{}{}
			out = append(out, rid)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}
