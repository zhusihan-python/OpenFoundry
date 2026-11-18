// Scheduled-only output commit path (WP3.2 A2). The manual committer
// semantics stay untouched (dataset_output_committer.go): this adapter
// wraps the SAME body builder with the DVS keyed protocol — stable
// publish key, durable pending receipt BEFORE the POST, usableACK-only
// confirmation — and never lets an unknown publish be compensated away
// as an ordinary failure. See A2 checkpoint §2-3/§2-4 and §3.
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
)

// ErrScheduledPublishUnknown marks a keyed publish whose outcome could not
// be confirmed (transport error, non-2xx, or a 2xx without a usable ACK).
// The runner MUST route the dispatch to unknown_publish on this sentinel —
// never to a compensated failure — so the activity slot, receipts and
// original request bytes survive for same-key reconciliation.
var ErrScheduledPublishUnknown = errors.New("scheduled publish outcome unknown")

// ScheduledFencedStore is the fenced persistence surface the scheduled
// committer needs. Implemented by *postgres.Repository (signatures kept
// import-cycle free on purpose).
type ScheduledFencedStore interface {
	AssertDispatchFence(ctx context.Context, eventID uuid.UUID, owner string, fence int64) error
	RecordPendingPublishWithResult(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key, nodeID, datasetRID string, body []byte, resultJSON json.RawMessage) error
	MarkReceiptUnknown(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, reason string) error
	CompleteScheduledPublish(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, receipt json.RawMessage, datasetRID, reservationTxnRID, realTxnRID string) error
}

// scheduledCommitRequest embeds the manual commit body (same package, so
// the unexported embedded type still serializes its exported fields) and
// adds the keyed-commit idempotency key. The fingerprint on the DVS side
// covers these exact bytes; a replay must reuse the stored bytes verbatim.
type scheduledCommitRequest struct {
	datasetOutputCommitRequest
	IdempotencyKey string `json:"idempotency_key"`
}

// ScheduledOutputCommitter commits scheduled dataset outputs through the
// DVS keyed protocol. One instance PER dispatch execution (it carries the
// claim identity and the reservation handoff state).
type ScheduledOutputCommitter struct {
	Store       ScheduledFencedStore
	Aborter     resolver.TransactionAborter
	Metadata    executor.OutputCommitter // non-dataset outputs (manual semantics)
	BaseURL     string
	Client      *http.Client
	EventID     uuid.UUID
	Owner       string
	Fence       int64
	BuildBranch string
	// Reservations maps dataset RID -> resolver-opened reservation txn
	// RID (from the persisted plan's job_outputs).
	Reservations map[string]string
	// ExistingReceipts marks datasets that already had a publish receipt
	// row before this run started (assessment knowledge) — those outputs
	// must never be remotely aborted by failure compensation.
	ExistingReceipts map[string]bool

	handoffMu sync.Mutex
	handedOff map[string]bool
}

// ContextWithSchedulerBearer attaches the decrypted grant caller token so
// the dataset HTTP adapters execute as the scheduled identity (same
// context key the manual handlers use).
func ContextWithSchedulerBearer(ctx context.Context, bearer string) context.Context {
	return contextWithCallerBearer(ctx, bearer)
}

// NewSchedulerNodeRunner exposes the runtime node runner wiring for the
// scheduled execution path (identical to the manual /execute path).
func NewSchedulerNodeRunner(ports ExecutionPorts) executor.NodeRunner {
	return newRuntimeNodeRunner(ports)
}

func (c *ScheduledOutputCommitter) httpClient() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}

func (c *ScheduledOutputCommitter) ensureHandoff(ctx context.Context, datasetRID string) error {
	c.handoffMu.Lock()
	defer c.handoffMu.Unlock()
	if c.handedOff == nil {
		c.handedOff = map[string]bool{}
	}
	if c.handedOff[datasetRID] {
		return nil
	}
	if err := c.Store.AssertDispatchFence(ctx, c.EventID, c.Owner, c.Fence); err != nil {
		return fmt.Errorf("scheduled handoff refused (fence): %w", err)
	}
	reservation := c.Reservations[datasetRID]
	if reservation != "" {
		if err := c.Aborter.AbortTransaction(ctx, datasetRID, c.BuildBranch, reservation); err != nil {
			if httpErr, ok := err.(*DatasetHTTPError); ok && httpErr.Status == http.StatusNotFound {
				// NotFound with our durable pending record: confirmed
				// terminal (the reservation never existed or is gone).
				c.handedOff[datasetRID] = true
				return nil
			}
			// Unconfirmed abort (transport/timeout/terminal anomaly like a
			// COMMITTED reservation): do NOT publish.
			return fmt.Errorf("%w: reservation handoff unconfirmed for %s: %v", ErrScheduledPublishUnknown, datasetRID, err)
		}
	}
	c.handedOff[datasetRID] = true
	return nil
}

// Commit implements executor.OutputCommitter for the scheduled path.
func (c *ScheduledOutputCommitter) Commit(ctx context.Context, tx executor.OutputTransaction, result executor.NodeResult) error {
	if !canCommitDatasetOutput(tx.DatasetRID) {
		if c.Metadata == nil {
			return nil
		}
		return c.Metadata.Commit(ctx, tx, result)
	}
	nodeID := strings.TrimSpace(tx.OutputNodeID)
	if nodeID == "" {
		// The runner stamps every plan output with its node id — an empty
		// one means a wiring bug, and a publish without identity is
		// unrecoverable by key. Refuse.
		return errors.New("scheduled commit requires OutputNodeID on the output transaction")
	}
	key := scheduleevents.DeriveOutputPublishKey(c.EventID, nodeID, tx.DatasetRID)
	inner, err := datasetOutputCommitBodyWithContext(ctx, tx, result)
	if err != nil {
		return err
	}
	// Keyed path invariants (DVS rejects create_if_missing and implicit
	// branches before any transaction work): the datasets resolved with a
	// visible branch and the resolver never creates them here.
	inner.CreateIfMissing = false
	body, err := json.Marshal(scheduledCommitRequest{datasetOutputCommitRequest: inner, IdempotencyKey: key})
	if err != nil {
		return fmt.Errorf("scheduled commit body: %w", err)
	}
	// Durable intent + fence gate BEFORE the POST: a lost lease fails
	// here, never mid-publish.
	metadata := make(map[string]any, len(result.Metadata))
	for k, v := range result.Metadata {
		if k != "data_rows" && k != "sample_rows" {
			metadata[k] = v
		}
	}
	resultJSON, err := json.Marshal(executor.NodeResult{OutputContentHash: result.OutputContentHash, Metadata: metadata})
	if err != nil {
		return err
	}
	if err := c.ensureHandoff(ctx, tx.DatasetRID); err != nil {
		return err
	}
	if err := c.Store.RecordPendingPublishWithResult(ctx, c.EventID, c.Owner, c.Fence, key, nodeID, tx.DatasetRID, body, resultJSON); err != nil {
		return fmt.Errorf("scheduled publish refused (pending receipt): %w", err)
	}
	return c.postKeyedCommit(ctx, key, nodeID, tx.DatasetRID, body, c.Reservations[tx.DatasetRID])
}

// postKeyedCommit POSTs the EXACT body bytes once and classifies the ACK.
// Used for both the first publish and same-key reconciliation replays.
func (c *ScheduledOutputCommitter) postKeyedCommit(ctx context.Context, key, nodeID, datasetRID string, body []byte, reservationTxnRID string) error {
	endpoint, err := (&DatasetServiceOutputCommitter{BaseURL: c.BaseURL}).commitEndpoint(datasetRID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	bearer := datasetReadBearerToken(ctx, "")
	if bearer == "" {
		// A scheduled publish without the grant bearer must not fall back
		// to any static service token.
		return fmt.Errorf("%w: no scheduler caller bearer in context for %s", ErrScheduledPublishUnknown, datasetRID)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return c.unknown(ctx, key, fmt.Sprintf("post: %v", err))
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail := strings.TrimSpace(string(raw))
		if len(detail) > 500 {
			detail = detail[:500]
		}
		return c.unknown(ctx, key, fmt.Sprintf("dataset-versioning returned %s: %s", resp.Status, detail))
	}
	var ack struct {
		DatasetID   uuid.UUID `json:"dataset_id"`
		Transaction struct {
			ID     uuid.UUID `json:"id"`
			Status string    `json:"status"`
		} `json:"transaction"`
	}
	if err := json.Unmarshal(raw, &ack); err != nil {
		return c.unknown(ctx, key, fmt.Sprintf("ack decode: %v", err))
	}
	// usableACK (Connector sync.go semantics): only Status==COMMITTED and
	// a matching dataset id prove the publish. Anything else is unknown.
	expected, parseErr := datasetUUIDFromRID(datasetRID)
	if parseErr != nil || ack.Transaction.ID == uuid.Nil || ack.Transaction.Status != "COMMITTED" || ack.DatasetID != expected {
		return c.unknown(ctx, key, fmt.Sprintf("unusable ack: status=%s dataset=%s (expected %s)", ack.Transaction.Status, ack.DatasetID, datasetRID))
	}
	if err := c.Store.CompleteScheduledPublish(ctx, c.EventID, c.Owner, c.Fence, key, json.RawMessage(raw), datasetRID, reservationTxnRID, "ri.foundry.main.transaction."+ack.Transaction.ID.String()); err != nil {
		// Remote COMMITTED but the durable completion failed: the receipt
		// stays pending and the same-key replay reconciles it later.
		return fmt.Errorf("%w: completion after committed ack failed: %v", ErrScheduledPublishUnknown, err)
	}
	return nil
}

func (c *ScheduledOutputCommitter) unknown(ctx context.Context, key, reason string) error {
	markErr := c.Store.MarkReceiptUnknown(ctx, c.EventID, c.Owner, c.Fence, key, reason)
	if markErr != nil {
		return fmt.Errorf("%w: %s (mark receipt unknown also failed: %v)", ErrScheduledPublishUnknown, reason, markErr)
	}
	return fmt.Errorf("%w: %s", ErrScheduledPublishUnknown, reason)
}

// ReplayReceipt reconciles one pending/unknown receipt with the ORIGINAL
// persisted bytes under the SAME key (assessment/recovery path only —
// nodes are never re-run to rebuild the body).
func (c *ScheduledOutputCommitter) ReplayReceipt(ctx context.Context, key, nodeID, datasetRID string, body []byte, reservationTxnRID string) error {
	if err := c.Store.RecordPendingPublishWithResult(ctx, c.EventID, c.Owner, c.Fence, key, nodeID, datasetRID, body, nil); err != nil {
		return err
	}
	return c.postKeyedCommit(ctx, key, nodeID, datasetRID, body, reservationTxnRID)
}

// ScheduledTransactionManager is the scheduled abort path. It refuses to
// abort any output that carries a publish receipt (pending/unknown/
// committed): unknown publishes must survive compensation and reconcile
// by key. Receipt-less reservations abort remotely first (the manual
// repo.Abort only flags metadata), then delegate for the flag.
type ScheduledTransactionManager struct {
	Aborter  resolver.TransactionAborter
	Metadata executor.TransactionManager
	// ReceiptedDatasets marks datasets with ANY receipt row for this
	// dispatch (preloaded from the execution snapshot).
	ReceiptedDatasets map[string]bool
}

// Abort implements executor.TransactionManager.
func (m *ScheduledTransactionManager) Abort(ctx context.Context, tx executor.OutputTransaction) error {
	// Executor compensation cannot infer current durable receipt state from
	// its initial plan. Canonical finalization owns fenced cleanup after loading
	// current receipts; never perform pool-bound metadata writes here.
	return fmt.Errorf("scheduled compensation deferred to durable reconciliation for %s", tx.DatasetRID)
}

// datasetUUIDFromRID parses the canonical dataset RID into its UUID.
func datasetUUIDFromRID(datasetRID string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(datasetRID)
	if strings.HasPrefix(trimmed, datasetRIDPrefix) {
		return uuid.Parse(strings.TrimPrefix(trimmed, datasetRIDPrefix))
	}
	return uuid.Parse(trimmed)
}
