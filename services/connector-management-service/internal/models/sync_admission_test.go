package models

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int { return &v }

func TestSyncAdmissionPolicyValidationMatrix(t *testing.T) {
	cases := []struct {
		name    string
		policy  *SyncAdmissionPolicy
		wantErr bool
	}{
		{"nil policy", nil, false},
		{"empty object", &SyncAdmissionPolicy{}, false},
		{"cap 1 lower bound", &SyncAdmissionPolicy{MaxInflightRuns: intPtr(1)}, false},
		{"cap 4096 upper bound", &SyncAdmissionPolicy{MaxInflightRuns: intPtr(SyncAdmissionMaxInflightRuns)}, false},
		{"cap 0 illegal", &SyncAdmissionPolicy{MaxInflightRuns: intPtr(0)}, true},
		{"cap negative illegal", &SyncAdmissionPolicy{MaxInflightRuns: intPtr(-1)}, true},
		{"cap 4097 illegal", &SyncAdmissionPolicy{MaxInflightRuns: intPtr(SyncAdmissionMaxInflightRuns + 1)}, true},
		{"interval 0 legal off", &SyncAdmissionPolicy{MinIntervalMS: intPtr(0)}, false},
		{"interval max legal", &SyncAdmissionPolicy{MinIntervalMS: intPtr(SyncAdmissionMaxIntervalMS)}, false},
		{"interval negative illegal", &SyncAdmissionPolicy{MinIntervalMS: intPtr(-1)}, true},
		{"interval over max illegal", &SyncAdmissionPolicy{MinIntervalMS: intPtr(SyncAdmissionMaxIntervalMS + 1)}, true},
		{"both dimensions", &SyncAdmissionPolicy{MaxInflightRuns: intPtr(2), MinIntervalMS: intPtr(500)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSyncAdmissionPolicy(tc.policy)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSyncAdmissionPolicyEnabledMatrix(t *testing.T) {
	assert.False(t, (*SyncAdmissionPolicy)(nil).Enabled(), "nil policy is never enabled")
	assert.False(t, (&SyncAdmissionPolicy{}).Enabled(), "empty object enables nothing")
	assert.True(t, (&SyncAdmissionPolicy{MaxInflightRuns: intPtr(1)}).Enabled())
	assert.False(t, (&SyncAdmissionPolicy{MinIntervalMS: intPtr(0)}).Enabled(), "explicit interval 0 is dimension-off")
	assert.True(t, (&SyncAdmissionPolicy{MinIntervalMS: intPtr(1)}).Enabled())
}

func TestParseSyncAdmissionPolicyFromArbitraryConfigs(t *testing.T) {
	off := func(t *testing.T, raw string) {
		t.Helper()
		p, err := ParseSyncAdmissionPolicy(json.RawMessage(raw))
		require.NoError(t, err, raw)
		assert.False(t, p.Enabled(), raw)
	}
	t.Run("absent dimensions stay off", func(t *testing.T) {
		off(t, ``)
		off(t, `null`)
		off(t, `{}`)
		off(t, `{"auto_register":{"enabled":true}}`)
		off(t, `{"sync":null}`)
		off(t, `{"sync":{}}`)
		off(t, `{"sync":{"resource_path":"/items"}}`)
		off(t, `{"sync":{"admission":null}}`)
		off(t, `{"sync":{"admission":{}}}`)
	})
	t.Run("valid policies parse", func(t *testing.T) {
		p, err := ParseSyncAdmissionPolicy(json.RawMessage(
			`{"sync":{"admission":{"max_inflight_runs":3,"min_interval_ms":2500}}}`))
		require.NoError(t, err)
		require.NotNil(t, p)
		require.NotNil(t, p.MaxInflightRuns)
		assert.Equal(t, 3, *p.MaxInflightRuns)
		require.NotNil(t, p.MinIntervalMS)
		assert.Equal(t, 2500, *p.MinIntervalMS)
		assert.True(t, p.Enabled())
	})
	t.Run("illegal admission subtrees are errors", func(t *testing.T) {
		for _, raw := range []string{
			`{"sync":{"admission":{"max_inflight_runs":0}}}`,
			`{"sync":{"admission":{"max_inflight_runs":-2}}}`,
			`{"sync":{"admission":{"max_inflight_runs":4097}}}`,
			`{"sync":{"admission":{"min_interval_ms":-1}}}`,
			`{"sync":{"admission":{"min_interval_ms":86400001}}}`,
			`{"sync":{"admission":{"max_inflight_runs":"3"}}}`,
			`{"sync":{"admission":{"max_inflight_runs":2.5}}}`,
			`{"sync":{"admission":{"min_interval_ms":1.0}}}`,
			`{"sync":{"admission":{"max_inflight_runs":true}}}`,
			`{"sync":{"admission":[1,2]}}`,
			`{"sync":{"admission":"nope"}}`,
		} {
			_, err := ParseSyncAdmissionPolicy(json.RawMessage(raw))
			assert.Error(t, err, raw)
		}
	})
	t.Run("configs that cannot carry admission stay off (pre-P2.3 compatibility)", func(t *testing.T) {
		// A non-object config root or sync value cannot syntactically contain
		// an admission block: the dimension is off and such passthrough
		// configs keep their exact pre-P2.3 acceptance and behavior.
		for _, raw := range []string{
			`[1,2]`,
			`"legacy-string-config"`,
			`{"sync":[1,2]}`,
			`{"sync":"later"}`,
		} {
			p, err := ParseSyncAdmissionPolicy(json.RawMessage(raw))
			require.NoError(t, err, raw)
			assert.False(t, p.Enabled(), raw)
		}
	})
	t.Run("unknown keys inside admission follow the existing drop idiom", func(t *testing.T) {
		// The typed config surface has always dropped unknown keys on
		// normalize; the parser only speaks the two pinned dimensions.
		p, err := ParseSyncAdmissionPolicy(json.RawMessage(
			`{"sync":{"admission":{"max_inflight_runs":2,"typo_key":9}}}`))
		require.NoError(t, err)
		assert.True(t, p.Enabled())
		assert.Equal(t, 2, *p.MaxInflightRuns)
	})
}

func TestNormalizeRESTAPISourceConfigRoundTripsSyncAdmission(t *testing.T) {
	raw := json.RawMessage(`{
		"base_url": "https://api.example.test",
		"sync": {
			"resource_path": "/items",
			"pagination": {"type": "page"},
			"admission": {"max_inflight_runs": 4, "min_interval_ms": 1500}
		}
	}`)
	normalized, err := NormalizeRESTAPISourceConfig(raw)
	require.NoError(t, err)
	cfg, err := ParseRESTAPISourceConfig(normalized)
	require.NoError(t, err)
	require.NotNil(t, cfg.Sync, "the sync block must roundtrip")
	require.NotNil(t, cfg.Sync.Admission, "the admission block must roundtrip")
	require.NotNil(t, cfg.Sync.Admission.MaxInflightRuns)
	assert.Equal(t, 4, *cfg.Sync.Admission.MaxInflightRuns)
	require.NotNil(t, cfg.Sync.Admission.MinIntervalMS)
	assert.Equal(t, 1500, *cfg.Sync.Admission.MinIntervalMS)
	assert.True(t, cfg.Sync.Admission.Enabled())

	// The normalized JSON still carries the subtree for the repo-side parser.
	p, err := ParseSyncAdmissionPolicy(normalized)
	require.NoError(t, err)
	assert.True(t, p.Enabled())
}

func TestNormalizeRESTAPISourceConfigRejectsIllegalSyncAdmission(t *testing.T) {
	base := `{
		"base_url": "https://api.example.test",
		"sync": {"resource_path": "/items", "admission": %s}
	}`
	for _, admission := range []string{
		`{"max_inflight_runs": 0}`,
		`{"max_inflight_runs": -1}`,
		`{"max_inflight_runs": 5000}`,
		`{"min_interval_ms": -5}`,
		`{"min_interval_ms": 90000000}`,
		`{"max_inflight_runs": "2"}`,
	} {
		_, err := NormalizeRESTAPISourceConfig(json.RawMessage(fmt.Sprintf(base, admission)))
		assert.Error(t, err, admission)
	}
}
