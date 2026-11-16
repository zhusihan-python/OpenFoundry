// P2.3 per-source persistent sync admission policy: the optional typed
// `sync.admission` block of a source config. It bounds ADMISSION of new sync
// runs for the source (unfinished-run cap + minimum interval between
// successful admissions) — it is NOT the webhook invocation limiter and NOT
// the REST pagination `sync.limits` block.
//
// Contract (pinned by root):
//   - max_inflight_runs: optional pointer int; when present it must be an
//     explicit integer in [1, 4096] (0, negatives and out-of-range are
//     rejected); missing means the cap dimension is off.
//   - min_interval_ms: optional pointer int; when present it must be in
//     [0, 86400000]; 0 means the interval dimension is off; missing is off.
//   - A missing/null/empty admission block enables no dimension: sources
//     without it keep the pre-P2.3 behavior exactly.
//   - enabled = cap present || interval present and > 0.
//   - Illegal JSON types are decode errors (callers answer 400) and never
//     mutate the source or create a run.
//
// ParseSyncAdmissionPolicy reads the block generically out of ANY connector
// type's config (enqueue is type-agnostic), which is also why every config
// write surface validates through it.
package models

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Sync admission bounds. The cap must be a positive explicit integer; the
// interval may legitimately be zero (= dimension off).
const (
	SyncAdmissionMaxInflightRuns = 4096
	SyncAdmissionMaxIntervalMS   = 86400000
)

// SyncAdmissionPolicy is the typed `sync.admission` object. Both dimensions
// are optional pointers so "absent" stays distinguishable from "explicitly
// zero" (only the interval allows an explicit zero).
type SyncAdmissionPolicy struct {
	MaxInflightRuns *int `json:"max_inflight_runs,omitempty"`
	MinIntervalMS   *int `json:"min_interval_ms,omitempty"`
}

// CapEnabled reports whether the unfinished-run cap dimension is on.
func (p *SyncAdmissionPolicy) CapEnabled() bool {
	return p != nil && p.MaxInflightRuns != nil
}

// IntervalEnabled reports whether the minimum-interval dimension is on. An
// explicit 0 disables the dimension (contract), so only > 0 enables it.
func (p *SyncAdmissionPolicy) IntervalEnabled() bool {
	return p != nil && p.MinIntervalMS != nil && *p.MinIntervalMS > 0
}

// Enabled reports whether ANY admission dimension is active — the exact
// condition that switches enqueue onto the NOWAIT lock path.
func (p *SyncAdmissionPolicy) Enabled() bool {
	return p.CapEnabled() || p.IntervalEnabled()
}

// ValidateSyncAdmissionPolicy enforces the pinned bounds. nil is valid (the
// whole block is optional); an empty object {} is valid and enables nothing.
func ValidateSyncAdmissionPolicy(p *SyncAdmissionPolicy) error {
	if p == nil {
		return nil
	}
	if p.MaxInflightRuns != nil {
		if *p.MaxInflightRuns < 1 || *p.MaxInflightRuns > SyncAdmissionMaxInflightRuns {
			return fmt.Errorf("sync.admission.max_inflight_runs must be an integer between 1 and %d when present", SyncAdmissionMaxInflightRuns)
		}
	}
	if p.MinIntervalMS != nil {
		if *p.MinIntervalMS < 0 || *p.MinIntervalMS > SyncAdmissionMaxIntervalMS {
			return fmt.Errorf("sync.admission.min_interval_ms must be an integer between 0 and %d when present", SyncAdmissionMaxIntervalMS)
		}
	}
	return nil
}

// ParseSyncAdmissionPolicy extracts and validates the sync.admission subtree
// of a source config. It returns a nil policy (never an error) when the
// config, its `sync` block or the `admission` block is absent, null or empty
// — and also when the config root or the `sync` value is not a JSON object at
// all: such a config cannot syntactically carry an admission block, so the
// dimension is simply off and the source keeps its exact pre-P2.3 behavior
// (passthrough configs of arbitrary shape stay accepted). Only a PRESENT
// admission subtree that is malformed (wrong types, out-of-range values) is
// an error, so every write surface can reject it with 400 and the enqueue
// gate can fail closed instead of silently admitting.
func ParseSyncAdmissionPolicy(config json.RawMessage) (*SyncAdmissionPolicy, error) {
	trimmed := strings.TrimSpace(string(config))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(config, &root); err != nil {
		return nil, nil // non-object configs cannot carry sync.admission: off
	}
	syncRaw, ok := root["sync"]
	if !ok {
		return nil, nil
	}
	if trimmedSync := strings.TrimSpace(string(syncRaw)); trimmedSync == "" || trimmedSync == "null" {
		return nil, nil
	}
	var syncObj map[string]json.RawMessage
	if err := json.Unmarshal(syncRaw, &syncObj); err != nil {
		return nil, nil // a non-object sync block cannot carry admission: off
	}
	admissionRaw, ok := syncObj["admission"]
	if !ok {
		return nil, nil
	}
	if trimmedAdmission := strings.TrimSpace(string(admissionRaw)); trimmedAdmission == "" || trimmedAdmission == "null" {
		return nil, nil
	}
	var policy SyncAdmissionPolicy
	if err := json.Unmarshal(admissionRaw, &policy); err != nil {
		return nil, fmt.Errorf("sync.admission: %w", err)
	}
	if err := ValidateSyncAdmissionPolicy(&policy); err != nil {
		return nil, err
	}
	return &policy, nil
}
