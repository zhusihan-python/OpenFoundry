// P2.4 observable scheduled scheduling: schedule models and pure validation.
//
// Validation discipline (mirrors the P2.3 sync.admission rule): every write
// surface that can carry schedule fields validates through ParseScheduleSpec /
// ParseScheduleCron — HTTP handlers validate early for a clean 400, the repo
// re-validates the merged state authoritatively inside its transaction.
//
// Cron semantics reuse the repository-owned Foundry-parity parser
// libs/scheduling-cron verbatim (Unix5 and Quartz6, IANA time zones, DST
// gap-skip / overlap-double-fire). The flavor is inferred from the field count
// (5 fields → Unix5, 6 fields → Quartz6); expression length is bounded.
package models

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	schedulingcron "github.com/openfoundry/openfoundry-go/libs/scheduling-cron"
)

// ScheduleCronMaxLen bounds the stored cron expression (root contract: 256
// bytes is sufficient for both flavours).
const ScheduleCronMaxLen = 256

// InvalidScheduleError is the stable typed refusal for every illegal schedule
// input. Field is "cron", "time_zone" or "enabled"; Reason is pre-sanitized
// prose for the API detail message.
type InvalidScheduleError struct {
	Field  string
	Reason string
}

func (e *InvalidScheduleError) Error() string {
	return "invalid_schedule: " + e.Field + ": " + e.Reason
}

// InferCronFlavor splits the expression and maps the field count onto the
// scheduling-cron flavours: 5 fields → Unix5, 6 fields → Quartz6, anything
// else is invalid.
func InferCronFlavor(expr string) (schedulingcron.CronFlavor, error) {
	fields := strings.Fields(expr)
	switch len(fields) {
	case 5:
		return schedulingcron.Unix5, nil
	case 6:
		return schedulingcron.Quartz6, nil
	default:
		return 0, &InvalidScheduleError{Field: "cron", Reason: fmt.Sprintf("expression must have 5 (unix5) or 6 (quartz6) fields, got %d", len(fields))}
	}
}

// ParseScheduleCron validates and parses one (expression, time zone) pair into
// an in-memory CronSchedule. An empty expression is invalid here; callers
// decide whether "no cron" is legal for their state (see ParseScheduleSpec).
func ParseScheduleCron(expr, timeZone string) (*schedulingcron.CronSchedule, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, &InvalidScheduleError{Field: "cron", Reason: "expression must not be empty"}
	}
	if len(trimmed) > ScheduleCronMaxLen {
		return nil, &InvalidScheduleError{Field: "cron", Reason: fmt.Sprintf("expression exceeds %d bytes", ScheduleCronMaxLen)}
	}
	flavor, err := InferCronFlavor(trimmed)
	if err != nil {
		return nil, err
	}
	loc, err := ResolveScheduleTimeZone(timeZone)
	if err != nil {
		return nil, err
	}
	schedule, err := schedulingcron.ParseCron(trimmed, flavor, loc)
	if err != nil {
		return nil, &InvalidScheduleError{Field: "cron", Reason: err.Error()}
	}
	return &schedule, nil
}

// ResolveScheduleTimeZone validates an IANA time-zone name; empty means UTC
// (the column default). Go's LoadLocation would happily accept "Local", which
// is process-local and therefore NOT a persistable schedule time zone — it is
// rejected explicitly (Go 1.26.3 / 1.25.14 time.LoadLocation semantics,
// verified in the sibling docs-verification.md).
func ResolveScheduleTimeZone(timeZone string) (*time.Location, error) {
	name := strings.TrimSpace(timeZone)
	if name == "" {
		name = "UTC"
	}
	if name == "Local" {
		return nil, &InvalidScheduleError{Field: "time_zone", Reason: `"Local" is process-local and not persistable; use an explicit IANA name such as "UTC"`}
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, &InvalidScheduleError{Field: "time_zone", Reason: fmt.Sprintf("%q is not a valid IANA time zone", name)}
	}
	return loc, nil
}

// NextScheduleFire returns the next fire instant strictly after `after` for
// (expr, timeZone). The anchor must be the post-lock database clock on every
// mutating path (root contract); passing any other clock is only acceptable
// for pure display.
func NextScheduleFire(expr, timeZone string, after time.Time) (time.Time, error) {
	schedule, err := ParseScheduleCron(expr, timeZone)
	if err != nil {
		return time.Time{}, err
	}
	next, ok := schedulingcron.NextFireAfter(schedule, after)
	if !ok {
		return time.Time{}, &InvalidScheduleError{Field: "cron", Reason: "expression has no future occurrence"}
	}
	return next, nil
}

// ScheduleSpec is the merged, validated schedule state of one sync def.
type ScheduleSpec struct {
	// Cron is the empty string when the def carries no cron (which requires
	// Enabled=false). Non-empty cron is already validated.
	Cron string
	// TimeZone is a validated IANA name ("UTC" when unset).
	TimeZone string
	// Enabled reports whether the scheduler may fire this def.
	Enabled bool
}

// NormalizeScheduleSpec validates the merged (cron, timeZone, enabled) state:
//   - an explicitly supplied non-empty cron must parse (either flavour);
//   - an explicitly supplied non-empty time zone must resolve;
//   - enabled=true requires a valid non-empty cron.
//
// The bool pointers follow request semantics: nil = keep the current value
// (callers pass the current state), non-nil = the merged new value. An empty
// cron string is legal only together with Enabled=false (the explicit clear
// semantics: empty cron clears/disables).
func NormalizeScheduleSpec(cron string, timeZone string, enabled bool) (ScheduleSpec, error) {
	spec := ScheduleSpec{Cron: strings.TrimSpace(cron), TimeZone: strings.TrimSpace(timeZone), Enabled: enabled}
	if spec.TimeZone == "" {
		spec.TimeZone = "UTC"
	}
	if _, err := ResolveScheduleTimeZone(spec.TimeZone); err != nil {
		return ScheduleSpec{}, err
	}
	if spec.Cron == "" {
		if spec.Enabled {
			return ScheduleSpec{}, &InvalidScheduleError{Field: "enabled", Reason: "enabling a schedule requires a valid non-empty cron expression"}
		}
		return spec, nil
	}
	if _, err := ParseScheduleCron(spec.Cron, spec.TimeZone); err != nil {
		return ScheduleSpec{}, err
	}
	return spec, nil
}

// ScheduleAuthorizationView is the read-side projection of one def's arm
// state. It honestly separates the DB's last-granted metadata (visible to
// every replica) from whether THIS process holds a matching live local grant
// (LocallyExecutable) — an unconfirmed post-restart owner is never labelled
// executable.
type ScheduleAuthorizationView struct {
	DBGrant            *ScheduleGrantView `json:"db_grant,omitempty"`
	LocallyExecutable bool               `json:"locally_executable"`
}

// ScheduleGrantView is the nonsecret projection of one sync_schedule_grants
// row. Live is computed by the repo at read time with the database clock:
// revision matches the def, the absolute expiry is in the future and the
// owner's last heartbeat is inside the freshness window.
type ScheduleGrantView struct {
	GrantID      uuid.UUID `json:"grant_id"`
	ActorSubject uuid.UUID `json:"actor_subject"`
	BootID       uuid.UUID `json:"boot_id"`
	Revision     int       `json:"schedule_revision"`
	ArmedAt      time.Time `json:"armed_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	Live         bool      `json:"live"`
}

// ScheduleGrantWrite is the arm input written by repo.ArmScheduleGrant. It
// contains no secret material — the Authorization header stays in the
// process-local registry only.
type ScheduleGrantWrite struct {
	SyncDefID       uuid.UUID
	GrantID         uuid.UUID
	BootID          uuid.UUID
	ActorSubject    uuid.UUID
	OwnerSubject    uuid.UUID
	SourceID        uuid.UUID
	OutputDatasetID uuid.UUID
	OrgID           *uuid.UUID // trusted tenant snapshot (NULL = personal domain)
	Revision        int        // post-lock re-verified def revision
	ExpiresAt       time.Time  // min(verified JWT exp, armed_at+TTL)
}

// ScheduleTick is the public DTO of one sync_schedule_ticks row. It carries no
// private fields; Detail is bounded (≤256) and sanitized upstream.
type ScheduleTick struct {
	ID               uuid.UUID  `json:"id"`
	SyncDefID        uuid.UUID  `json:"sync_def_id"`
	ScheduleRevision int         `json:"schedule_revision"`
	ScheduledFor     time.Time  `json:"scheduled_for"`
	ClaimedAt        time.Time  `json:"claimed_at"`
	Outcome          string     `json:"outcome"`
	RunID            *uuid.UUID `json:"run_id,omitempty"`
	Actor            *uuid.UUID `json:"actor,omitempty"`
	Reason           *string    `json:"reason,omitempty"`
	Detail           *string    `json:"detail,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}
