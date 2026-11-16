package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJoinStopsAttemptsEveryStop is the P2.4 main failure regression: an
// authorizer Stop error must NEVER short-circuit the coordinator Stop —
// both are always attempted, individual errors aggregate, and an all-nil
// run stays nil so runServiceLifecycle can proceed to the bounded joins.
func TestJoinStopsAttemptsEveryStop(t *testing.T) {
	t.Parallel()
	var calls []string
	authorizerErr := errors.New("schedule authorizer maintenance did not join within the stop budget")
	coordinatorErr := errors.New("coordinator stop incomplete")

	// 1. Authorizer fails, coordinator must STILL run and both errors join.
	joined := joinStops(
		func() error { calls = append(calls, "authorizer"); return authorizerErr },
		func() error { calls = append(calls, "coordinator"); return coordinatorErr },
	)
	err := joined()
	require.Error(t, err)
	assert.ErrorIs(t, err, authorizerErr)
	assert.ErrorIs(t, err, coordinatorErr)
	assert.Equal(t, []string{"authorizer", "coordinator"}, calls)

	// 2. Authorizer fails, coordinator succeeds: coordinator still called,
	// only the authorizer error surfaces.
	calls = nil
	err = joinStops(
		func() error { calls = append(calls, "authorizer"); return authorizerErr },
		func() error { calls = append(calls, "coordinator"); return nil },
	)()
	require.Error(t, err)
	assert.ErrorIs(t, err, authorizerErr)
	assert.NotErrorIs(t, err, coordinatorErr)
	assert.Equal(t, []string{"authorizer", "coordinator"}, calls)

	// 3. Both succeed: nil result, both called.
	calls = nil
	assert.NoError(t, joinStops(
		func() error { calls = append(calls, "authorizer"); return nil },
		func() error { calls = append(calls, "coordinator"); return nil },
	)())
	assert.Equal(t, []string{"authorizer", "coordinator"}, calls)

	// 4. Nil contributors are skipped safely.
	assert.NoError(t, joinStops(nil, func() error { return nil })())
}
