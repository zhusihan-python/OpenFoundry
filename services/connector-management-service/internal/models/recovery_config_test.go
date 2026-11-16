// P2.6 typed recovery opt-in validation tests (pure; no DB). v2.1: recovery
// no longer requires incremental — the protocol covers snapshot runs too.
package models

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func normalizeFromJSON(t *testing.T, sync map[string]any) error {
	t.Helper()
	raw, err := json.Marshal(sync)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var cfg RESTAPISyncConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return NormalizeRESTAPISyncConfig(&cfg)
}

func TestRecoveryOptInValidWithIncremental(t *testing.T) {
	sync := map[string]any{
		"resource_path": "/orders",
		"pagination":    map[string]any{"type": "cursor", "next_cursor_path": "next"},
		"incremental": map[string]any{
			"enabled":                   true,
			"request_param":             "since",
			"completed_checkpoint_path": "meta.completed",
		},
		"recovery": map[string]any{"enabled": true},
	}
	if err := normalizeFromJSON(t, sync); err != nil {
		t.Fatalf("recovery on top of incremental must validate: %v", err)
	}
}

func TestRecoveryOptInValidWithoutIncremental(t *testing.T) {
	// v2.1: plain snapshot recovery — recovery.enabled does NOT require
	// incremental.enabled.
	sync := map[string]any{
		"resource_path": "/orders",
		"pagination":    map[string]any{"type": "page"},
		"recovery":      map[string]any{"enabled": true},
	}
	if err := normalizeFromJSON(t, sync); err != nil {
		t.Fatalf("snapshot recovery opt-in must validate: %v", err)
	}
}

func TestRecoveryDisabledNeverValidatedDeeply(t *testing.T) {
	// A present-but-disabled block enables nothing — even without any
	// incremental block the config stays valid (legacy shape preserved).
	sync := map[string]any{
		"resource_path": "/orders",
		"pagination":    map[string]any{"type": "page"},
		"recovery":      map[string]any{"enabled": false},
	}
	if err := normalizeFromJSON(t, sync); err != nil {
		t.Fatalf("disabled recovery must not fail validation: %v", err)
	}
}

func TestLegacySyncConfigUnchanged(t *testing.T) {
	// The exact P1-era minimal shape: no recovery key at all.
	raw := []byte(`{"resource_path":"/x","pagination":{"type":"page"},"limits":{"request_timeout_ms":5000}}`)
	var cfg RESTAPISyncConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if cfg.Recovery != nil {
		t.Fatalf("legacy config must leave Recovery nil, got %+v", cfg.Recovery)
	}
	if err := NormalizeRESTAPISyncConfig(&cfg); err != nil {
		t.Fatalf("legacy config must still validate: %v", err)
	}
}

func TestSnapshotRecoveryBindingFingerprintDeterministicAndSensitive(t *testing.T) {
	base := SnapshotRecoveryBinding{
		SourceID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		OutputDatasetID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		BaseURL:         "https://api.example.com",
		ResourcePath:    "/orders",
		PaginationType:  "page",
		PageParam:       "page",
		PageSizeParam:   "page_size",
		PageSize:        500,
		StartPage:       1,
		QueryParams:     map[string]string{"status": "active"},
		Auth:            RESTAPIAuthConfig{Type: "bearer", Token: "t0k"},
		Credentials:     []CredFingerprint{{Kind: "api_key", Fingerprint: "aa"}},
	}
	fp := SnapshotRecoveryBindingFingerprint(base)
	if len(fp) != 64 {
		t.Fatalf("fingerprint length %d", len(fp))
	}
	if SnapshotRecoveryBindingFingerprint(base) != fp {
		t.Fatal("fingerprint must be deterministic")
	}
	// Credential order must not matter (sorted by kind).
	withZeta := base
	withZeta.Credentials = append(withZeta.Credentials, CredFingerprint{Kind: "zeta", Fingerprint: "zz"})
	shuffled := withZeta
	shuffled.Credentials = []CredFingerprint{
		{Kind: "zeta", Fingerprint: "zz"}, {Kind: "api_key", Fingerprint: "aa"},
	}
	if SnapshotRecoveryBindingFingerprint(shuffled) != SnapshotRecoveryBindingFingerprint(withZeta) {
		t.Fatal("credential order must not change the fingerprint")
	}
	// Sensitivity: every meaningful identity field changes the fingerprint.
	mutations := map[string]func(*SnapshotRecoveryBinding){
		"file_glob":    func(b *SnapshotRecoveryBinding) { g := "*.jsonl"; b.FileGlob = &g },
		"page_size":    func(b *SnapshotRecoveryBinding) { b.PageSize = 501 },
		"cursor_param": func(b *SnapshotRecoveryBinding) { b.CursorParam = "cur" },
		"target":       func(b *SnapshotRecoveryBinding) { b.OutputDatasetID = uuid.New() },
		// Fresh slice on purpose: a struct copy shares the Credentials
		// backing array, so an in-place write would alias into the reference.
		"credential": func(b *SnapshotRecoveryBinding) {
			b.Credentials = []CredFingerprint{b.Credentials[1], {Kind: "api_key", Fingerprint: "bb"}}
		},
	}
	for name, mutate := range mutations {
		mutated := withZeta
		mutate(&mutated)
		if SnapshotRecoveryBindingFingerprint(mutated) == SnapshotRecoveryBindingFingerprint(withZeta) {
			t.Fatalf("mutation %q must change the fingerprint", name)
		}
	}
	// nil and empty maps are semantically identical inputs.
	emptyMaps := withZeta
	emptyMaps.QueryParams = map[string]string{}
	nilMaps := withZeta
	nilMaps.QueryParams = nil
	if SnapshotRecoveryBindingFingerprint(emptyMaps) != SnapshotRecoveryBindingFingerprint(nilMaps) {
		t.Fatal("nil and empty maps must fingerprint identically")
	}
}
