package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

var ErrInvalidOutputCommitGroup = errors.New("invalid output commit group")

// OutputCommitMember declares one existing dataset and its receipt key.
// Build must use the provided store for all database writes. Irreversible
// external work is outside the transaction guarantee.
type OutputCommitMember struct {
	DatasetID uuid.UUID
	Key       string
	Build     func(context.Context, OutputCommitTxStore) (models.CommitDatasetOutputResponse, error)
}

// WithIdempotentOutputCommitGroup publishes a group in one database commit.
// requestFingerprint must identify the entire request, including member
// payloads. Members are built and returned in request order, regardless of
// lock order. Replay requires every receipt to match the original group.
func (r *Repo) WithIdempotentOutputCommitGroup(ctx context.Context, scope TenantScope, requestFingerprint string, members []OutputCommitMember) ([]*models.OutputCommitReceipt, bool, error) {
	if len(members) < 2 || requestFingerprint == "" {
		return nil, false, ErrInvalidOutputCommitGroup
	}
	type memberKey struct {
		DatasetID uuid.UUID `json:"dataset_id"`
		Key       string    `json:"key"`
	}
	keys := make([]memberKey, 0, len(members))
	lockOrder := make([]uuid.UUID, 0, len(members))
	seen := make(map[uuid.UUID]struct{}, len(members))
	for _, member := range members {
		_, duplicate := seen[member.DatasetID]
		if member.DatasetID == uuid.Nil || duplicate || member.Key == "" || member.Build == nil {
			return nil, false, ErrInvalidOutputCommitGroup
		}
		seen[member.DatasetID] = struct{}{}
		lockOrder = append(lockOrder, member.DatasetID)
		keys = append(keys, memberKey{DatasetID: member.DatasetID, Key: member.Key})
	}
	// Keep group receipts distinct from single-output receipts and bind
	// them to the complete ordered set, including keys on other datasets.
	encoded, err := json.Marshal(struct {
		Kind    string      `json:"kind"`
		Request string      `json:"request"`
		Members []memberKey `json:"members"`
	}{"output-commit-group-v1", requestFingerprint, keys})
	if err != nil {
		return nil, false, err
	}
	sum := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(sum[:])
	sort.Slice(lockOrder, func(i, j int) bool { return bytes.Compare(lockOrder[i][:], lockOrder[j][:]) < 0 })

	tx, err := r.beginOutputCommit(ctx)
	if err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackOutputCommit(tx)
		}
	}()
	for _, id := range lockOrder {
		if err := lockOutputCommitDataset(ctx, tx, scope, id); err != nil {
			return nil, false, err
		}
	}
	receipts := make([]*models.OutputCommitReceipt, len(members))
	found := 0
	for i, member := range members {
		receipt, err := loadOutputCommitReceipt(ctx, tx, member.DatasetID, member.Key)
		if err != nil {
			return nil, false, err
		}
		if receipt != nil {
			if receipt.RequestFingerprint != fingerprint {
				return nil, false, ErrIdempotencyKeyConflict
			}
			found++
			receipts[i] = receipt
		}
	}
	if found == len(members) {
		return receipts, true, nil
	}
	if found != 0 {
		return nil, false, ErrIdempotencyKeyConflict
	}

	txStore := &Repo{Pool: tx, OutputCommitLockTimeout: r.OutputCommitLockTimeout}
	for i, member := range members {
		response, err := member.Build(ctx, txStore)
		if err != nil {
			return nil, false, err
		}
		if response.DatasetID != member.DatasetID || response.Transaction.DatasetID != member.DatasetID ||
			response.Transaction.ID == uuid.Nil || response.Transaction.Status != models.TransactionStatusCommitted ||
			response.Branch == "" || response.Branch != response.Transaction.BranchName {
			return nil, false, ErrInvalidOutputCommitGroup
		}
		receipts[i], err = writeOutputCommitReceipt(ctx, tx, member.DatasetID, member.Key, fingerprint, response)
		if err != nil {
			return nil, false, err
		}
	}
	if err := commitOutputTransaction(ctx, tx); err != nil {
		return nil, false, err
	}
	committed = true
	return receipts, false, nil
}
