package models

import (
	"encoding/json"
	"strings"
	"testing"
)

func incrementalSyncJSON(t *testing.T, block string) *RESTAPISyncConfig {
	t.Helper()
	raw := `{"resource_path":"/items","pagination":{"type":"cursor","next_cursor_path":"next"},"incremental":` + block + `}`
	var cfg RESTAPISyncConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &cfg
}

func TestIncrementalConfigValidation(t *testing.T) {
	cases := []struct {
		name  string
		block string
		ok    bool
	}{
		{"valid cursor", `{"enabled":true,"request_param":"since","completed_checkpoint_path":"meta.completed"}`, true},
		{"missing request_param", `{"enabled":true,"completed_checkpoint_path":"c"}`, false},
		{"missing completed path", `{"enabled":true,"request_param":"since"}`, false},
		{"dot path empty segment", `{"enabled":true,"request_param":"since","completed_checkpoint_path":"a..b"}`, false},
		{"initial value present", `{"enabled":true,"request_param":"since","completed_checkpoint_path":"c","initial_value":"tok-1"}`, true},
		{"initial value explicit empty rejected", `{"enabled":true,"request_param":"since","completed_checkpoint_path":"c","initial_value":""}`, false},
		{"initial value oversize rejected", `{"enabled":true,"request_param":"since","completed_checkpoint_path":"c","initial_value":"` + strings.Repeat("a", 513) + `"}`, false},
		{"initial value control char rejected", `{"enabled":true,"request_param":"since","completed_checkpoint_path":"c","initial_value":"a\u0000b"}`, false},
		{"disabled block not deeply validated", `{"enabled":false,"request_param":""}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := incrementalSyncJSON(t, tc.block)
			err := NormalizeRESTAPISyncConfig(cfg)
			if tc.ok && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
	// Enabled incremental on page (or none) pagination is rejected.
	for _, pageType := range []string{"page", "none"} {
		t.Run(pageType+"_pagination_rejected", func(t *testing.T) {
			raw := `{"resource_path":"/items","pagination":{"type":"` + pageType + `"},"incremental":{"enabled":true,"request_param":"since","completed_checkpoint_path":"c"}}`
			var cfg RESTAPISyncConfig
			if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := NormalizeRESTAPISyncConfig(&cfg); err == nil {
				t.Fatalf("incremental on %q pagination must be rejected", pageType)
			}
		})
	}
}

func TestIncrementalRequestParamCollisions(t *testing.T) {
	base := `{"resource_path":"/items","pagination":{"type":"cursor","next_cursor_path":"next","cursor_param":"cursor"},"query_params":{"q":"1"},"incremental":{"enabled":true,"request_param":"%s","completed_checkpoint_path":"c"}}`
	for _, reserved := range []string{"cursor", "page", "page_size", "q"} {
		var cfg RESTAPISyncConfig
		if err := json.Unmarshal([]byte(strings.Replace(base, "%s", reserved, 1)), &cfg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if err := NormalizeRESTAPISyncConfig(&cfg); err == nil {
			t.Fatalf("request_param %q must collide", reserved)
		}
	}
}

func TestValidateRESTAPISourceIncrementalAuthQueryCollision(t *testing.T) {
	src := &RESTAPISourceConfig{
		BaseURL: "https://api.example.com",
		Auth:    RESTAPIAuthConfig{Type: RESTAPIAuthAPIKey, QueryParam: "api_key"},
		Sync:    &RESTAPISyncConfig{Incremental: &RESTAPIIncrementalConfig{Enabled: true, RequestParam: "api_key", CompletedCheckpointPath: "c"}},
	}
	if err := ValidateRESTAPISourceIncremental(src); err == nil {
		t.Fatal("auth query placement collision must be rejected at Source level")
	}
	src.Sync.Incremental.RequestParam = "since"
	if err := ValidateRESTAPISourceIncremental(src); err != nil {
		t.Fatalf("distinct param must pass: %v", err)
	}
	// Source review item 3 (round2 micro-fix): only api_key auth actually
	// writes the query placement at runtime. The positive control uses the
	// SAME key on both sides — a bearer auth carrying that leftover
	// query_param must not falsely reject it (the old unconditional check
	// would have), while the active api_key case above rejects the same key.
	bearer := &RESTAPISourceConfig{
		BaseURL: "https://api.example.com",
		Auth:    RESTAPIAuthConfig{Type: RESTAPIAuthBearer, QueryParam: "since", SecretRef: "oauth_token"},
		Sync:    &RESTAPISyncConfig{Incremental: &RESTAPIIncrementalConfig{Enabled: true, RequestParam: "since", CompletedCheckpointPath: "c"}},
	}
	if err := ValidateRESTAPISourceIncremental(bearer); err != nil {
		t.Fatalf("bearer same-name leftover query_param must not collide: %v", err)
	}
	if err := ValidateRESTAPISourceIncremental(&RESTAPISourceConfig{
		BaseURL: "https://api.example.com",
		Auth:    RESTAPIAuthConfig{Type: RESTAPIAuthAPIKey, QueryParam: "since"},
		Sync:    &RESTAPISyncConfig{Incremental: &RESTAPIIncrementalConfig{Enabled: true, RequestParam: "since", CompletedCheckpointPath: "c"}},
	}); err == nil {
		t.Fatal("api_key same-name query placement must still be rejected")
	}
	// The same U+FFFD rule catches config-side lossy decoding: a lone
	// surrogate escape in initial_value unmarshals to U+FFFD and is rejected.
	var cfg RESTAPISyncConfig
	raw := `{"resource_path":"/items","pagination":{"type":"cursor","next_cursor_path":"next"},"incremental":{"enabled":true,"request_param":"since","completed_checkpoint_path":"c","initial_value":"a\ud800b"}}`
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := NormalizeRESTAPISyncConfig(&cfg); err == nil {
		t.Fatal("lossy surrogate-decoded initial_value must be rejected")
	}
}

func TestValidateOpaqueCheckpointValue(t *testing.T) {
	cases := []struct {
		name  string
		value string
		ok    bool
	}{
		{"empty", "", false},
		{"plain", "abc-123", true},
		{"512 bytes", strings.Repeat("a", 512), true},
		{"513 bytes", strings.Repeat("a", 513), false},
		{"tab control", "a\tb", false},
		{"c1 control", "a\u0085b", false},
		{"unicode ok", "tok-éà", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateOpaqueCheckpointValue(tc.value, "test")
			if tc.ok && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestIncrementalBindingFingerprintDeterminism(t *testing.T) {
	mk := func() IncrementalBinding {
		initial := "tok-1"
		return IncrementalBinding{
			BaseURL: "https://api.example.com", ResourcePath: "/items",
			PaginationType: "cursor", CursorParam: "cursor", NextCursorPath: "next",
			Incremental: RESTAPIIncrementalConfig{Enabled: true, RequestParam: "since", CompletedCheckpointPath: "meta.c", InitialValue: &initial},
			QueryParams: map[string]string{"z": "1", "a": "2"},
			Auth:        RESTAPIAuthConfig{Type: "bearer", SecretRef: "oauth_token"},
			Credentials: []CredFingerprint{{Kind: "b", Fingerprint: "fb"}, {Kind: "a", Fingerprint: "fa"}},
		}
	}
	first := IncrementalBindingFingerprint(mk())
	second := IncrementalBindingFingerprint(mk())
	if first != second || len(first) != 64 {
		t.Fatalf("fingerprint must be deterministic sha256 hex, got %q vs %q", first, second)
	}
	// Credential ORDER must not matter (sorted canonically).
	swapped := mk()
	swapped.Credentials = []CredFingerprint{{Kind: "a", Fingerprint: "fa"}, {Kind: "b", Fingerprint: "fb"}}
	if IncrementalBindingFingerprint(swapped) != first {
		t.Fatal("credential order must not change the fingerprint")
	}
	// Same-value credential rewrite keeps the fingerprint (same-value rule).
	// A REAL value change changes it.
	rotated := mk()
	rotated.Credentials = []CredFingerprint{{Kind: "a", Fingerprint: "fa"}, {Kind: "b", Fingerprint: "CHANGED"}}
	if IncrementalBindingFingerprint(rotated) == first {
		t.Fatal("credential value change must change the fingerprint")
	}
	// file_glob nil vs value are distinct bindings.
	globbed := mk()
	g := "orders/*.ndjson"
	globbed.FileGlob = &g
	if IncrementalBindingFingerprint(globbed) == first {
		t.Fatal("file_glob must participate in the binding")
	}
	// InitialValue presence is significant.
	noInitial := mk()
	noInitial.Incremental.InitialValue = nil
	if IncrementalBindingFingerprint(noInitial) == first {
		t.Fatal("initial_value presence must participate in the binding")
	}
}
