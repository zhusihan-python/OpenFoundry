// Consumer lifecycle for the T8 scheduler (WP3.2 A2): owns the message
// loop, the execution worker and the Kafka reader, and guarantees the
// shutdown order cancel → drain → reader close (no leaked partitions,
// no post-close polls). Construction FAILS CLOSED — with the consumer
// enabled, an init error must take the process down, never leave a
// healthy-but-dead scheduler behind.
package scheduling

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
)

// NewOwnerID builds the lease owner identity for this process instance
// (stable across the process lifetime, unique across restarts).
func NewOwnerID() string {
	hostname, err := os.Hostname()
	if hostname == "" || err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("pipeline-build-service-scheduler@%s@%s", hostname, uuid.NewString()[:8])
}

// ConsumerRuntime hosts the two loops plus the reader lifecycle.
type ConsumerRuntime struct {
	consumer  *Consumer
	source    *KafkaSource
	runner    Runner
	log       *slog.Logger
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// NewConsumerRuntime wires the runtime from validated config. Every
// piece (dialer, source, consumer, runner) must already be constructed —
// init failures happen at the caller (fail-closed exit), this type only
// runs what it is given.
func NewConsumerRuntime(consumer *Consumer, source *KafkaSource, runner Runner, log *slog.Logger) *ConsumerRuntime {
	if log == nil {
		log = slog.Default()
	}
	return &ConsumerRuntime{consumer: consumer, source: source, runner: runner, log: log}
}

// BuildRuntime is the one-shot constructor used by process wiring:
// dialer → reader → consumer. Any error MUST abort startup.
func BuildRuntime(cfg config.SchedulerConfig, store ConsumerStore, hook OutcomeHookBuilder, runner Runner, owner string, log *slog.Logger) (*ConsumerRuntime, error) {
	dialer, err := NewDialer(cfg)
	if err != nil {
		return nil, fmt.Errorf("scheduler kafka dialer: %w", err)
	}
	source := NewKafkaSource(cfg, dialer)
	consumer := NewConsumer(store, source, hook, owner,
		time.Duration(cfg.LeaseTTLSeconds)*time.Second,
		time.Duration(cfg.HeartbeatSeconds)*time.Second,
		cfg.MaxEventAttempts, log)
	return NewConsumerRuntime(consumer, source, runner, log), nil
}

// Start launches the message loop and the execution worker. Errors after
// startup are logged by the loops themselves; Start returns immediately.
func (rt *ConsumerRuntime) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	rt.cancel = cancel
	rt.wg.Add(2)
	go func() {
		defer rt.wg.Done()
		if err := rt.consumer.Run(runCtx); err != nil && runCtx.Err() == nil {
			rt.log.Error("scheduler consumer loop exited", "err", err.Error())
		}
	}()
	go func() {
		defer rt.wg.Done()
		if err := rt.consumer.ExecutionWorker(runCtx, rt.runner); err != nil && runCtx.Err() == nil {
			rt.log.Error("scheduler execution worker exited", "err", err.Error())
		}
	}()
}

// Stop cancels both loops, waits for them to drain, then closes the
// reader exactly once. Safe to call multiple times.
func (rt *ConsumerRuntime) Stop() {
	rt.closeOnce.Do(func() {
		if rt.cancel != nil {
			rt.cancel()
		}
		rt.wg.Wait()
		if rt.source != nil {
			if err := rt.source.Close(); err != nil {
				rt.log.Warn("scheduler kafka reader close", "err", err.Error())
			}
		}
	})
}

// compile-time interface check: the reader adapter satisfies the
// consumer's source contract.
var _ MessageSource = (*KafkaSource)(nil)
