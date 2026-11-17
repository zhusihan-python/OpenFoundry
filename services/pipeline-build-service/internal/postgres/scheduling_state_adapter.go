package postgres

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ScheduledBuildState applies canonical resolver bookkeeping under its lease.
type ScheduledBuildState struct {
	repo  *Repository
	event uuid.UUID
	owner string
	fence int64
}

func (r *Repository) ScheduledBuildState(event uuid.UUID, owner string, fence int64) *ScheduledBuildState {
	return &ScheduledBuildState{r, event, owner, fence}
}
func (s *ScheduledBuildState) write(ctx context.Context, fn func(*Repository) error) error {
	return s.repo.FencedWrite(ctx, s.event, s.owner, s.fence, func(c context.Context, tx pgx.Tx) error { return fn(NewRepository(tx)) })
}
func (s *ScheduledBuildState) TryAcquire(c context.Context, ds string, b uuid.UUID, txn string) (holder uuid.UUID, ok bool, err error) {
	err = s.write(c, func(r *Repository) error { var e error; holder, ok, e = r.TryAcquire(c, ds, b, txn); return e })
	return
}
func (s *ScheduledBuildState) HasUpstreamInProgress(c context.Context, inputs []string, b uuid.UUID) (bool, error) {
	return s.repo.HasUpstreamInProgress(c, inputs, b)
}
func (s *ScheduledBuildState) ReleaseLock(c context.Context, ds string, b uuid.UUID) error {
	// Locks survive cleanup until FinishDispatch commits terminal state.
	return s.write(c, func(r *Repository) error { return nil })
}
func (s *ScheduledBuildState) RecordPendingTxn(c context.Context, b uuid.UUID, ds, branch, txn string) error {
	return s.write(c, func(r *Repository) error { return r.RecordPendingTxn(c, b, ds, branch, txn) })
}
func (s *ScheduledBuildState) ClearPendingTxn(c context.Context, b uuid.UUID, txn string) error {
	return s.write(c, func(r *Repository) error { return r.ClearPendingTxn(c, b, txn) })
}
