package repo

import (
	"context"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// ReplaceDatasetFileIndexWithStats rewrites the dataset's file index and the
// dataset's catalog row_count/size_bytes in the SAME transaction, so a
// failure rolls both back and a success publishes both together.
//
// Stats semantics for the P3.1 Excel import surface (kept exact and
// explicit, never "latest file as the total"):
//   - size_bytes sums the size of every file-index entry — exact for any
//     mixture of import sources.
//   - row_count sums the exact normalized row counts recorded in the
//     metadata of Excel-imported entries. Entries without an exact count
//     (e.g. legacy direct uploads) contribute zero, so a mixed dataset
//     reports its imported rows, and an all-import dataset reports the
//     exact total.
//
// The caller computes both totals from the merged index it is publishing;
// this method does not reinterpret metadata.
func (r *Repo) ReplaceDatasetFileIndexWithStats(ctx context.Context, datasetID uuid.UUID, files []models.PutDatasetFileIndexEntry, rowCount, sizeBytes int64) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if err := replaceFileIndexSteps(ctx, tx, datasetID, files); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE datasets SET row_count = $2, size_bytes = $3, updated_at = NOW() WHERE id = $1`, datasetID, rowCount, sizeBytes); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
