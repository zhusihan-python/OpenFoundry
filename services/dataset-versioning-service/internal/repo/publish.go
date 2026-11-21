// Atomic dataset publishing — P1 repair.
//
// outputs:commit used to run CommitTransaction first and replace the file
// index and lineage links afterwards, so a failure in either replacement
// returned 500 while the new version was already visible. The three
// mutations are now collected in CommitTransactionWithPublish, which
// drives them inside a single database transaction: any failure rolls
// the whole publish back and the previous snapshot stays authoritative.
// The standalone Replace* methods share the same step functions and wrap
// them in their own transaction, replacing the previous
// DELETE-then-INSERT-without-transaction shape.

package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// commitTransactionSteps is the commit core shared by CommitTransaction
// and CommitTransactionWithPublish: it validates the OPEN transaction,
// materialises the committed view, advances the branch head, cuts the
// next dataset version and updates the dataset counters. Callers wrap it
// in their own transaction and may append publish steps before COMMIT.
//
// The datasets row is locked FOR UPDATE before any transaction-row,
// branch or version locking: keyed commits (WithIdempotentOutputCommit)
// take the same lock first, so every publish path on a dataset acquires
// locks in one order and a keyed commit can never deadlock against a
// plain commit. Keyed callers already hold this lock in the same outer
// transaction — re-locking it is a no-op.
func commitTransactionSteps(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, txnID uuid.UUID, rowCount *int64) error {
	var lockedDataset uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM datasets WHERE id = $1 FOR UPDATE`, datasetID).Scan(&lockedDataset)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	row, err := loadTxnForUpdate(ctx, tx, txnID)
	if err != nil {
		return err
	}
	if row == nil || row.DatasetID != datasetID {
		return ErrNotFound
	}
	if row.Status != models.TransactionStatusOpen {
		return ErrInvalidTransition
	}

	staged, err := loadStagedFiles(ctx, tx, row.ID)
	if err != nil {
		return err
	}
	before, err := computeCommittedView(ctx, tx, row.BranchID, nil)
	if err != nil {
		return err
	}
	if err := validateCommit(row.TxType, staged, before); err != nil {
		return err
	}
	after, err := applyTransaction(before, row.TxType, staged)
	if err != nil {
		return err
	}
	var sizeBytes int64
	for _, f := range after {
		sizeBytes += f.SizeBytes
	}
	fileCount := len(after)

	metadataPatch, err := json.Marshal(map[string]any{"file_count": fileCount, "size_bytes": sizeBytes})
	if err != nil {
		return err
	}
	cmd, err := tx.Exec(ctx, `UPDATE dataset_transactions
		SET status = 'COMMITTED', committed_at = NOW(), metadata = metadata || $2::jsonb
		WHERE id = $1 AND dataset_id = $3 AND status = 'OPEN'`, row.ID, metadataPatch, datasetID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrInvalidTransition
	}

	if row.TxType != models.TransactionTypeSnapshot {
		rowCount = nil
	}
	if row.TxType == models.TransactionTypeSnapshot {
		if _, err := tx.Exec(ctx, `UPDATE dataset_transactions
			SET metadata = metadata || '{"historical": true}'::jsonb
			WHERE branch_id = $1 AND id <> $2 AND status = 'COMMITTED'`, row.BranchID, row.ID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE dataset_branches
		SET head_transaction_id = $2, last_activity_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND dataset_id = $3`, row.BranchID, row.ID, datasetID); err != nil {
		return err
	}

	nextVersion, err := nextVersionForUpdate(ctx, tx, datasetID)
	if err != nil {
		return err
	}
	var storagePath string
	if err := tx.QueryRow(ctx, `SELECT storage_path FROM datasets WHERE id = $1`, datasetID).Scan(&storagePath); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO dataset_versions
		(id, dataset_id, version, message, size_bytes, row_count, storage_path, transaction_id)
		VALUES ($1, $2, $3, $4, $5, COALESCE($6::bigint, 0), $7, $8)
		ON CONFLICT (dataset_id, version) DO NOTHING`, uuid.New(), datasetID, nextVersion, row.Summary, sizeBytes, rowCount, fmt.Sprintf("%s/v%d", storagePath, nextVersion), row.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE dataset_branches
		SET version = $3, updated_at = NOW()
		WHERE dataset_id = $1 AND name = $2`, datasetID, row.BranchName, nextVersion); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE datasets
		SET current_version = CASE WHEN active_branch = $2 THEN $3 ELSE current_version END,
		    size_bytes = CASE WHEN active_branch = $2 THEN $4 ELSE size_bytes END,
		    row_count = CASE WHEN active_branch = $2 THEN COALESCE($6::bigint, row_count) ELSE row_count END,
		    metadata = CASE WHEN $5 = 'UPDATE' THEN metadata || '{"incremental_friendly": false}'::jsonb ELSE metadata END,
		    updated_at = NOW()
		WHERE id = $1`, datasetID, row.BranchName, nextVersion, sizeBytes, row.TxType, rowCount); err != nil {
		return err
	}
	return nil
}

// replaceFileIndexSteps rewrites the dataset's file index inside a caller
// transaction.
func replaceFileIndexSteps(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, files []models.PutDatasetFileIndexEntry) error {
	if _, err := tx.Exec(ctx, `DELETE FROM dataset_file_index WHERE dataset_id = $1`, datasetID); err != nil {
		return err
	}
	for _, f := range files {
		entryType := "file"
		if f.EntryType != nil && *f.EntryType != "" {
			entryType = *f.EntryType
		}
		size := int64(0)
		if f.SizeBytes != nil {
			size = *f.SizeBytes
		}
		_, err := tx.Exec(ctx, `INSERT INTO dataset_file_index
			(id, dataset_id, path, storage_path, entry_type, size_bytes, content_type, metadata, last_modified)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
			ON CONFLICT (dataset_id, path) DO UPDATE SET storage_path = EXCLUDED.storage_path,
			entry_type = EXCLUDED.entry_type, size_bytes = EXCLUDED.size_bytes, content_type = EXCLUDED.content_type,
			metadata = EXCLUDED.metadata, last_modified = EXCLUDED.last_modified, updated_at = NOW()`,
			uuid.New(), datasetID, f.Path, f.StoragePath, entryType, size, f.ContentType, defaultRawObject(f.Metadata), f.LastModified)
		if err != nil {
			return err
		}
	}
	return nil
}

// replaceLineageSteps rewrites the dataset's lineage links inside a
// caller transaction.
func replaceLineageSteps(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, links []models.PutDatasetLineageLink) error {
	if _, err := tx.Exec(ctx, `DELETE FROM dataset_lineage_links WHERE dataset_id = $1`, datasetID); err != nil {
		return err
	}
	for _, l := range links {
		targetKind := "dataset"
		if l.TargetKind != nil && *l.TargetKind != "" {
			targetKind = *l.TargetKind
		}
		relationKind := "derives_from"
		if l.RelationKind != nil && *l.RelationKind != "" {
			relationKind = *l.RelationKind
		}
		_, err := tx.Exec(ctx, `INSERT INTO dataset_lineage_links
			(id, dataset_id, direction, target_rid, target_kind, relation_kind, pipeline_id, workflow_id, metadata)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
			ON CONFLICT (dataset_id, direction, target_rid, relation_kind) DO UPDATE
			SET target_kind = EXCLUDED.target_kind, pipeline_id = EXCLUDED.pipeline_id,
			workflow_id = EXCLUDED.workflow_id, metadata = EXCLUDED.metadata, updated_at = NOW()`,
			uuid.New(), datasetID, l.Direction, l.TargetRID, targetKind, relationKind, l.PipelineID, l.WorkflowID, defaultRawObject(l.Metadata))
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishSnapshot is the response payload a successful publish hands
// back. Every read that feeds the HTTP response is taken inside the
// publish transaction, so once CommitTransactionWithPublish returns the
// caller only serialises in-memory data — a post-commit read failure can
// no longer report a failed request for an already-published version.
type PublishSnapshot struct {
	Transaction  models.RuntimeTransaction
	Files        []models.DatasetFileIndexEntry
	Preview      models.PreviewDataResponse
	LineageLinks []models.DatasetLineageLink
}

// A complete count is available only if every published snapshot file was counted.
func verifiedSnapshotRowCount(files []models.PutDatasetFileIndexEntry) *int64 {
	if len(files) == 0 {
		return nil
	}
	var rows int64
	for _, file := range files {
		if file.VerifiedRowCount == nil {
			return nil
		}
		rows += *file.VerifiedRowCount
	}
	return &rows
}

// CommitTransactionWithPublish commits the transaction, publishes its
// file index and lineage links, and snapshots the response data — all
// inside one database transaction. The version cut, both replacements,
// the transaction state change and the response reads either all land
// or all roll back, so a failed publish never exposes a half-committed
// dataset and the previous snapshot stays readable.
func (r *Repo) CommitTransactionWithPublish(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID, files []models.PutDatasetFileIndexEntry, links []models.PutDatasetLineageLink) (*PublishSnapshot, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if err := commitTransactionSteps(ctx, tx, datasetID, txnID, verifiedSnapshotRowCount(files)); err != nil {
		return nil, err
	}
	if len(files) > 0 {
		if err := replaceFileIndexSteps(ctx, tx, datasetID, files); err != nil {
			return nil, err
		}
	}
	if len(links) > 0 {
		if err := replaceLineageSteps(ctx, tx, datasetID, links); err != nil {
			return nil, err
		}
	}
	// Response reads run inside the same transaction through a tx-bound
	// Repo (pgx.Tx satisfies the DB interface): a read failure here rolls
	// the whole publish back instead of 500-ing an already-published
	// version. Reads see the just-published rows before COMMIT.
	txRepo := &Repo{Pool: tx}
	snapshot := &PublishSnapshot{LineageLinks: []models.DatasetLineageLink{}}
	after, err := txRepo.GetRuntimeTransaction(ctx, datasetID, txnID)
	if err != nil {
		return nil, err
	}
	if after == nil {
		return nil, ErrNotFound
	}
	snapshot.Transaction = *after
	if snapshot.Files, err = txRepo.ListDatasetFileIndex(ctx, datasetID); err != nil {
		return nil, err
	}
	preview, err := txRepo.PreviewData(ctx, datasetID, nil, models.PreviewQuery{})
	if err != nil {
		return nil, err
	}
	if preview == nil {
		return nil, ErrNotFound
	}
	snapshot.Preview = *preview
	if len(links) > 0 {
		if snapshot.LineageLinks, err = txRepo.ListDatasetLineageLinks(ctx, datasetID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	return snapshot, nil
}
