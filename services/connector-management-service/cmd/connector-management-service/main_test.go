// Real lifecycle control-flow tests for runServiceLifecycle: listen errors,
// signal-then-Shutdown-timeout ordering, incomplete-stop forbidding the pool
// close, and admission closing at signal time (before Stop) — asserted on the
// actual function main uses, with recorded edges instead of mock sequences.
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lifecycleRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *lifecycleRecorder) record(ev string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *lifecycleRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *lifecycleRecorder) indexOf(ev string) int {
	for i, e := range r.snapshot() {
		if e == ev {
			return i
		}
	}
	return -1
}

// count reports how many times an event fired (indexOf is a position, not a
// tally — "exactly once" assertions must use this).
func (r *lifecycleRecorder) count(ev string) int {
	n := 0
	for _, e := range r.snapshot() {
		if e == ev {
			n++
		}
	}
	return n
}

// TestLifecycleListenErrorStillStopsAndClosesPool: a serve failure (bind
// error, immediate return) must still run the full stop sequence and close
// the pool exactly once.
func TestLifecycleListenErrorStillStopsAndClosesPool(t *testing.T) {
	rec := &lifecycleRecorder{}
	err := runServiceLifecycle(
		context.Background(),
		func(context.Context) error { rec.record("serveErr"); return errors.New("bind: address in use") },
		func() { rec.record("closeAdmission") },
		func() { rec.record("cancelBG") },
		func() error { rec.record("stop"); return nil },
		func() bool { rec.record("bgJoin"); return true },
		func() { rec.record("poolClose") },
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bind: address in use")
	require.Equal(t, 1, rec.count("stop"), "stop must run exactly once")
	require.Equal(t, 1, rec.count("poolClose"), "pool must close exactly once")
	assert.Less(t, rec.indexOf("cancelBG"), rec.indexOf("stop"), "background roots cancelled explicitly before Stop (no signal on listen errors)")
	assert.Less(t, rec.indexOf("stop"), rec.indexOf("poolClose"), "stop strictly before pool close")
}

// TestLifecycleShutdownTimeoutOrdering: on signal, admission closes BEFORE
// the HTTP Shutdown (serve returns DeadlineExceeded after the drain window);
// afterwards Stop runs, joins, and only then the pool closes; the timeout is
// returned, not swallowed. The lifecycle runs in its own goroutine with a
// barrier before the cancel — running it synchronously would self-deadlock
// (serve parks on ctx.Done until the signal fires).
func TestLifecycleShutdownTimeoutOrdering(t *testing.T) {
	rec := &lifecycleRecorder{}
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveStarted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runServiceLifecycle(
			rootCtx,
			func(ctx context.Context) error {
				close(serveStarted)
				<-ctx.Done() // the DEDICATED drain context: cancelled only after admission closed
				time.Sleep(20 * time.Millisecond)
				rec.record("serveShutdownTimeout")
				return context.DeadlineExceeded
			},
			func() { rec.record("closeAdmission") },
			func() { rec.record("cancelBG") },
			func() error { rec.record("stop"); return nil },
			func() bool { rec.record("bgJoin"); return true },
			func() { rec.record("poolClose") },
		)
	}()
	<-serveStarted // the serve loop is parked on ctx.Done before we signal
	cancel()       // the signal

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle did not finish after the signal")
	}

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	// Ordering: admission closed at signal time — before serve even returned
	// — then stop, then background join, then the pool close.
	require.GreaterOrEqual(t, rec.indexOf("closeAdmission"), 0, "admission must close on signal")
	assert.Less(t, rec.indexOf("closeAdmission"), rec.indexOf("serveShutdownTimeout"),
		"admission closes at signal time, before the drain window ends")
	assert.Less(t, rec.indexOf("serveShutdownTimeout"), rec.indexOf("cancelBG"))
	assert.Less(t, rec.indexOf("cancelBG"), rec.indexOf("stop"))
	assert.Less(t, rec.indexOf("stop"), rec.indexOf("bgJoin"))
	assert.Less(t, rec.indexOf("bgJoin"), rec.indexOf("poolClose"))
	require.Equal(t, 1, rec.count("poolClose"))
}

// TestLifecycleStopIncompleteForbidsPoolClose: when Stop cannot join, live
// workers may still be writing — the pool must NOT be closed, and the error
// names the incomplete stop instead of claiming a graceful exit.
func TestLifecycleStopIncompleteForbidsPoolClose(t *testing.T) {
	rec := &lifecycleRecorder{}
	err := runServiceLifecycle(
		context.Background(),
		func(context.Context) error { rec.record("serveDone"); return nil },
		func() { rec.record("closeAdmission") },
		func() { rec.record("cancelBG") },
		func() error {
			rec.record("stop")
			return errors.New("coordinator stop incomplete: 2 queued tasks could not be closed")
		},
		func() bool { rec.record("bgJoin"); return true },
		func() { rec.record("poolClose") },
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "coordinator stop incomplete")
	assert.Contains(t, err.Error(), "honestly unconfirmed")
	assert.Equal(t, -1, rec.indexOf("poolClose"), "an unjoined stop must forbid closing the pool")
	assert.GreaterOrEqual(t, rec.indexOf("stop"), 0, "stop itself must still run")
}

// TestLifecycleBackgroundJoinFailureAlsoForbidsPoolClose: unjoined
// schedulers may still touch the pool — same rule as an unjoined stop.
func TestLifecycleBackgroundJoinFailureAlsoForbidsPoolClose(t *testing.T) {
	rec := &lifecycleRecorder{}
	err := runServiceLifecycle(
		context.Background(),
		func(context.Context) error { return nil },
		func() {},
		func() {},
		func() error { return nil },
		func() bool { return false },
		func() { rec.record("poolClose") },
	)
	require.Error(t, err)
	assert.Equal(t, -1, rec.indexOf("poolClose"))
}

// TestLifecycleGracefulExit: happy path — drain finishes before any timeout,
// everything joins, pool closes once, no error.
func TestLifecycleGracefulExit(t *testing.T) {
	rec := &lifecycleRecorder{}
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runServiceLifecycle(
			rootCtx,
			func(ctx context.Context) error {
				<-ctx.Done()
				rec.record("serveDrained")
				return context.Canceled
			},
			func() { rec.record("closeAdmission") },
			func() { rec.record("cancelBG") },
			func() error { rec.record("stop"); return nil },
			func() bool { rec.record("bgJoin"); return true },
			func() { rec.record("poolClose") },
		)
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("graceful lifecycle did not finish")
	}
	require.Equal(t, 1, rec.count("poolClose"))
	assert.Less(t, rec.indexOf("stop"), rec.indexOf("poolClose"))
}
