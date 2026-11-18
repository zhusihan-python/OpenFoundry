package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
)

// WP3.4: hash-dedup reuse of a canonical job_specs row must not silently
// swap a dataset_snapshot pin — build planning reads the branch mapping
// while LoadPlan restores the canonical logic_payload, so a colliding
// reuse would bind the two sides to different pinned definitions.

func TestPayloadDeclaresDatasetSnapshot(t *testing.T) {
	for name, tc := range map[string]struct {
		payload string
		want    bool
	}{
		"empty object":                {`{}`, false},
		"null payload":                {`null`, false},
		"empty bytes":                 {``, false},
		"not an object":               {`["dataset_snapshot"]`, false},
		"key as string value only":    {`{"note":"dataset_snapshot is cool"}`, false},
		"payload not json":            {`not-json`, false},
		"nested config not exact key": {`{"Config":{"dataset_snapshot":{"branch":"main"}}}`, false},
		"null snapshot":               {`{"dataset_snapshot":null}`, true},
		"empty snapshot object":       {`{"dataset_snapshot":{}}`, true},
		"present":                     {`{"dataset_snapshot":{"branch":"main","version":1,"transaction_id":"ri.foundry.main.transaction.11111111-1111-1111-1111-111111111111"}}`, true},
		"present with other fields":   {`{"dataset_id":"x","dataset_snapshot":{"branch":"main"}}`, true},
		"case-folded key binds":       {`{"dataset_Snapshot":{"branch":"main"}}`, true},
		"purely nested pin":           {`{"config":{"dataset_id":"x","dataset_snapshot":{"branch":"main"}}}`, true},
		"deeply nested pin":           {`{"config":{"config":{"dataset_snapshot":{"branch":"main"}}}}`, true},
		"nested pin under top fields": {`{"rows":[{"a":1}],"config":{"dataset_snapshot":{"branch":"main"}}}`, true},
		"nested without pin":          {`{"config":{"dataset_id":"x"}}`, false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, handler.PayloadDeclaresDatasetSnapshot([]byte(tc.payload)), tc.payload)
		})
	}
}

func TestJobSpecPinnedReuseConflictNestedPins(t *testing.T) {
	// The same-hash collision the top-level-only probe missed: nested
	// pins under a colliding explicit hash must conflict.
	nestedA := `{"config":{"dataset_id":"11111111-1111-1111-1111-111111111111","dataset_snapshot":{"branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111"}}}`
	nestedB := `{"config":{"dataset_id":"11111111-1111-1111-1111-111111111111","dataset_snapshot":{"branch":"main","version":4,"transaction_id":"22222222-2222-2222-2222-222222222222"}}}`
	inputs := `[{"dataset_rid":"ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"}]`
	assert.True(t, jobSpecPinnedReuseConflict([]byte(nestedA), []byte(inputs), []byte(nestedB), []byte(inputs)))
	assert.False(t, jobSpecPinnedReuseConflict([]byte(nestedA), []byte(inputs), []byte(nestedA), []byte(inputs)))
}

func TestJobSpecPinnedReuseConflict(t *testing.T) {
	pinA := `{"dataset_id":"11111111-1111-1111-1111-111111111111","dataset_snapshot":{"branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111"}}`
	pinB := `{"dataset_id":"11111111-1111-1111-1111-111111111111","dataset_snapshot":{"branch":"main","version":4,"transaction_id":"22222222-2222-2222-2222-222222222222"}}`
	pinAReordered := `{"dataset_snapshot":{"transaction_id":"11111111-1111-1111-1111-111111111111","version":3,"branch":"main"},"dataset_id":"11111111-1111-1111-1111-111111111111"}`
	inputsX := `[{"dataset_rid":"ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111"}]`
	inputsY := `[{"dataset_rid":"ri.foundry.main.dataset.99999999-9999-9999-9999-999999999999"}]`

	t.Run("no pin on either side keeps legacy dedup", func(t *testing.T) {
		assert.False(t, jobSpecPinnedReuseConflict([]byte(`{"v":"A"}`), []byte(inputsX), []byte(`{"v":"B"}`), []byte(inputsY)))
	})
	t.Run("identical pinned payload and inputs reuse", func(t *testing.T) {
		assert.False(t, jobSpecPinnedReuseConflict([]byte(pinA), []byte(inputsX), []byte(pinA), []byte(inputsX)))
	})
	t.Run("reordered but equal pinned payload reuses", func(t *testing.T) {
		assert.False(t, jobSpecPinnedReuseConflict([]byte(pinA), []byte(inputsX), []byte(pinAReordered), []byte(inputsX)))
	})
	t.Run("different pin conflicts", func(t *testing.T) {
		assert.True(t, jobSpecPinnedReuseConflict([]byte(pinA), []byte(inputsX), []byte(pinB), []byte(inputsX)))
	})
	t.Run("same pin with different declared inputs conflicts", func(t *testing.T) {
		assert.True(t, jobSpecPinnedReuseConflict([]byte(pinA), []byte(inputsX), []byte(pinA), []byte(inputsY)))
	})
	t.Run("stored pin, new unpinned payload conflicts", func(t *testing.T) {
		assert.True(t, jobSpecPinnedReuseConflict([]byte(pinA), []byte(inputsX), []byte(`{"v":"plain"}`), []byte(inputsX)))
	})
	t.Run("new pin against stored unpinned row conflicts", func(t *testing.T) {
		assert.True(t, jobSpecPinnedReuseConflict([]byte(`{"v":"plain"}`), []byte(inputsX), []byte(pinA), []byte(inputsX)))
	})
}

func TestJSONEqualBytesPrecision(t *testing.T) {
	t.Run("big integer difference is a difference", func(t *testing.T) {
		assert.False(t, jsonEqualBytes([]byte(`{"v":123456789012345678901234567890}`), []byte(`{"v":123456789012345678901234567891}`)))
	})
	t.Run("identical big integers equal", func(t *testing.T) {
		assert.True(t, jsonEqualBytes([]byte(`{"v":123456789012345678901234567890}`), []byte(`{"v":123456789012345678901234567890}`)))
	})
	t.Run("key reorder equal", func(t *testing.T) {
		assert.True(t, jsonEqualBytes([]byte(`{"a":1,"b":2}`), []byte(`{"b":2,"a":1}`)))
	})
	t.Run("trailing content unequal", func(t *testing.T) {
		assert.False(t, jsonEqualBytes([]byte(`{"a":1}`), []byte(`{"a":1} {"b":2}`)))
	})
	t.Run("invalid json unequal", func(t *testing.T) {
		assert.False(t, jsonEqualBytes([]byte(`{"a":1}`), []byte(`nope`)))
	})
}

func TestPinConflictHelpersCoverRawMessageUsage(t *testing.T) {
	// The repository passes []byte straight from pgx scans and from
	// json.Marshal outputs; ensure json.RawMessage values behave the same.
	raw := json.RawMessage(`{"dataset_snapshot":{"branch":"main"}}`)
	require.True(t, handler.PayloadDeclaresDatasetSnapshot(raw))
}
