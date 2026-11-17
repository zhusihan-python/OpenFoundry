package eventscheduler

import (
	"errors"
	"fmt"
)

// ErrorKind tags a [SchedulerError]. Mirrors the Rust thiserror enum.
type ErrorKind int

const (
	// ErrDB — underlying Postgres error.
	ErrDB ErrorKind = iota
	// ErrPublish — Kafka publish failed for the row identified by Name.
	ErrPublish
	// ErrInvalidCron — cron_expr could not be parsed.
	ErrInvalidCron
	// ErrUnknownFlavor — cron_flavor column held an unrecognised value.
	ErrUnknownFlavor
	// ErrInvalidTimeZone — time_zone column was not a valid IANA zone.
	ErrInvalidTimeZone
	// ErrNoFutureFire — the cron expression has no matching instant
	// within the evaluator's 10-year horizon.
	ErrNoFutureFire

	// ── Outbox-dispatch mode (T8 decision §1 transaction ①) ──────────
	//
	// Kinds raised by [OutboxScheduler.TickOutbox]. They are all
	// infrastructure-side failures: a row-level problem (bad cron,
	// incomplete binding, schema-violating fields) is NEVER returned as
	// an error — that row is quarantined and the tick continues
	// (WP3.1 assessment V7: one bad definition must not keep blocking
	// the other due definitions).

	// ErrSchemaMissing — a query touched a column or table the
	// scheduling-mode migrations have not applied
	// (schedules.definitions.dispatch_mode / quarantined_at,
	// scheduling.grants, scheduling.dispatch_audit, outbox.events).
	// Fail-closed with an actionable message. Applies to both tick
	// modes' NEW queries only: an unmigrated database simply must not
	// run this library version's tick until migrated; nothing is
	// silently degraded.
	ErrSchemaMissing
	// ErrRegistryUnreachable — the schema registry could not be
	// consulted (transport error, non-200, or artifact not yet
	// registered). The tick aborts BEFORE claiming any row, so every
	// due definition stays due and the next tick retries.
	ErrRegistryUnreachable
	// ErrRegistryContentMismatch — the registry is reachable but a
	// registered artifact's content hash does not match the pinned
	// fingerprint. A deploy/registry-state error, not a bad
	// definition: abort the whole tick fail-closed (rows stay due)
	// instead of quarantining innocent rows.
	ErrRegistryContentMismatch
	// ErrCDCNotReady — the CDC replication slot gate (WithCDCSlot) is
	// configured and the slot is missing or not active. The outbox
	// INSERT+DELETE pattern is WAL-only: firing before the connector
	// streams would silently lose events (unrecoverable by snapshot),
	// so the tick refuses to claim anything and retries next run.
	ErrCDCNotReady
)

// SchedulerError is the failure mode raised by [Scheduler.Tick] and
// helpers. Mirrors the Rust thiserror enum.
type SchedulerError struct {
	Kind ErrorKind
	// Name — schedule name, populated for every variant except ErrDB.
	Name string
	// Topic — Kafka topic, populated for ErrPublish.
	Topic string
	// Flavor — raw cron_flavor value, populated for ErrUnknownFlavor.
	Flavor string
	// TimeZone — raw time_zone value, populated for ErrInvalidTimeZone.
	TimeZone string
	// CronExpr — raw cron_expr value, populated for ErrNoFutureFire.
	CronExpr string
	// Cause — underlying error (DB / publish / cron parse).
	Cause error
	// Artifact — registry artifact id, populated for
	// ErrRegistryContentMismatch.
	Artifact string
	// WantSHA256 / GotSHA256 — pinned vs registered content hash,
	// populated for ErrRegistryContentMismatch.
	WantSHA256 string
	GotSHA256  string
}

// Error implements [error]. Mirrors the Rust thiserror messages
// byte-for-byte where the variants overlap.
func (e *SchedulerError) Error() string {
	switch e.Kind {
	case ErrDB:
		return fmt.Sprintf("database error: %s", e.Cause)
	case ErrPublish:
		return fmt.Sprintf("publish to topic `%s` failed for schedule `%s`: %s", e.Topic, e.Name, e.Cause)
	case ErrInvalidCron:
		return fmt.Sprintf("invalid cron expression for schedule `%s`: %s", e.Name, e.Cause)
	case ErrUnknownFlavor:
		return fmt.Sprintf("unknown cron flavor `%s` for schedule `%s` (expected `unix5` or `quartz6`)", e.Flavor, e.Name)
	case ErrInvalidTimeZone:
		return fmt.Sprintf("invalid time zone `%s` for schedule `%s`", e.TimeZone, e.Name)
	case ErrNoFutureFire:
		return fmt.Sprintf("schedule `%s` has no future fire within 10 years (cron: `%s`)", e.Name, e.CronExpr)
	case ErrSchemaMissing:
		return fmt.Sprintf("scheduling-mode schema missing: %s (apply the pipeline scheduling migration before running this tick)", e.Cause)
	case ErrRegistryUnreachable:
		return fmt.Sprintf("schema registry unreachable for artifact `%s`: %s (tick aborted before claiming; all due definitions stay due)", e.Artifact, e.Cause)
	case ErrRegistryContentMismatch:
		return fmt.Sprintf("registry artifact `%s` content sha256 %s != pinned %s (deploy drift: aborting tick, rows stay due)", e.Artifact, e.GotSHA256, e.WantSHA256)
	case ErrCDCNotReady:
		return fmt.Sprintf("cdc not ready for slot `%s`: %s (tick aborted before claiming; all due definitions stay due)", e.Name, e.Cause)
	}
	return "unknown scheduler error"
}

// Unwrap exposes Cause to errors.Is / errors.As.
func (e *SchedulerError) Unwrap() error { return e.Cause }

// IsKind is a convenience predicate for callers that want to switch
// on the underlying [ErrorKind] without unwrapping.
func IsKind(err error, kind ErrorKind) bool {
	var se *SchedulerError
	if !errors.As(err, &se) {
		return false
	}
	return se.Kind == kind
}
