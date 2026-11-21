//go:build integration

package repo

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type admissionTransactionActivity struct {
	PID       int32
	State     string
	StartedAt *time.Time
	WaitEvent string
	Query     string
}

func waitForAdmissionCleanup(t *testing.T, observer *pgxpool.Pool, timeout time.Duration, participants ...int32) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var last []admissionTransactionActivity
	for time.Now().Before(deadline) {
		current, err := admissionIdleTransactions(ctx, observer)
		if err != nil {
			// A socket deadline can fire before the context timer is dispatched.
			if !time.Now().Before(deadline) {
				break
			}
			return fmt.Errorf("observe admission cleanup, last active transactions %+v: %w", last, err)
		}
		if len(current) == 0 {
			return nil
		}
		for _, session := range current {
			if !slices.Contains(participants, session.PID) {
				return fmt.Errorf("unrelated session left an open transaction: %+v", session)
			}
		}
		if len(last) == 0 {
			t.Logf("admission cleanup still in progress: %+v", current)
		}
		last = current
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
	return fmt.Errorf("admission cleanup exceeded %v, active transactions %+v: %w", timeout, last, context.DeadlineExceeded)
}

func admissionIdleTransactions(ctx context.Context, observer *pgxpool.Pool) ([]admissionTransactionActivity, error) {
	rows, err := observer.Query(ctx, `
			SELECT pid, state, xact_start, concat_ws('/', wait_event_type, wait_event),
			       coalesce(left(query, 200), '')
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND state IN ('idle in transaction', 'idle in transaction (aborted)')
			ORDER BY pid`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[admissionTransactionActivity])
}

func TestPGAdmissionCleanupDetectsOpenTransaction(t *testing.T) {
	r, dsn, _, sourceID, _, _ := p23Fixture(t)
	barrier := p23HoldSourceBarrier(t, dsn, sourceID)

	err := waitForAdmissionCleanup(t, r.Pool, 2*time.Second)
	require.ErrorContains(t, err, "unrelated session left an open transaction")
	require.ErrorContains(t, err, fmt.Sprintf("PID:%d", barrier.pid))

	err = waitForAdmissionCleanup(t, r.Pool, 250*time.Millisecond, barrier.pid)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, fmt.Sprintf("PID:%d", barrier.pid))
	require.ErrorContains(t, err, "idle in transaction")
	require.ErrorContains(t, err, "SELECT id FROM connections")

	barrier.release(t)
	require.NoError(t, waitForAdmissionCleanup(t, r.Pool, 2*time.Second, barrier.pid))
}
