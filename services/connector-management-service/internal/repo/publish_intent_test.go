// P2.6 W2 pure unit tests: publish-key derivation, backoff, frozen-material
// validation (ACTUAL inline-content cross-checks, mode candidate pairing,
// lineage), receipt-vs-intent verification and the safe-view projection. No
// database, no HTTP — the fenced SQL paths are exercised by the later
// authorized PG/HTTP acceptance runs.
package repo

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func hex64(s string) string {
	// Deterministic 64-hex filler for tests that don't assert hash equality
	// semantics themselves (ReceiptMatchesIntent compares strings verbatim).
	out := make([]byte, 64)
	alphabet := "0123456789abcdef"
	for i := range out {
		out[i] = alphabet[(int(s[i%len(s)])+i)%16]
	}
	return string(out)
}

// buildBody serializes a commit-request shape identical to the executor's
// single serialization site (W3): files[0] carries the ACTUAL inline content
// and the metadata mirrors hash/rows/bytes + the lineage triple.
func buildBody(t *testing.T, key, branch, logicalPath string, contentHash string, rows int64, content []byte, sourceID, defID, runID uuid.UUID) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"idempotency_key": key,
		"branch":          branch,
		"files": []map[string]any{{
			"logical_path":   logicalPath,
			"size_bytes":     len(content),
			"content_type":   "application/x-ndjson",
			"operation":      "ADD",
			"content_base64": content,
		}},
		"metadata": map[string]any{
			"content_hash": contentHash,
			"rows":         rows,
			"bytes":        len(content),
			"lineage": map[string]any{
				"source_id":   sourceID,
				"sync_def_id": defID,
				"sync_run_id": runID,
			},
		},
	})
	if err != nil {
		t.Fatalf("buildBody: %v", err)
	}
	return body
}

type matIDs struct{ runID, sourceID, defID uuid.UUID }

func ids(t *testing.T) matIDs {
	t.Helper()
	return matIDs{runID: uuid.New(), sourceID: uuid.New(), defID: uuid.New()}
}

func material(t *testing.T, id matIDs, cpMode string, candidate *string, content []byte, rows int64) FrozenPublishMaterial {
	t.Helper()
	digest := sha256.Sum256(content)
	mat := FrozenPublishMaterial{
		SourceID:            id.sourceID,
		SyncDefID:           id.defID,
		Branch:              "main",
		LogicalPath:         "sync-" + id.runID.String() + ".ndjson",
		CompletedCheckpoint: candidate,
		ContentHash:         fmt.Sprintf("%x", digest),
		Rows:                rows,
		Bytes:               int64(len(content)),
		CPMode:              cpMode,
	}
	mat.ExactBody = buildBody(t, DerivePublishKey(id.runID), mat.Branch, mat.LogicalPath,
		mat.ContentHash, mat.Rows, content, id.sourceID, id.defID, id.runID)
	return mat
}

func validIncrementalMaterial(t *testing.T, id matIDs) FrozenPublishMaterial {
	t.Helper()
	cand := "cpc-123"
	return material(t, id, "cursor", &cand, []byte("{\"a\":1}\n{\"b\":2}\n"), 2)
}

func validSnapshotMaterial(t *testing.T, id matIDs) FrozenPublishMaterial {
	t.Helper()
	return material(t, id, "", nil, []byte("{\"snap\":true}\n"), 1)
}

func TestDerivePublishKeyFixedRunScoped(t *testing.T) {
	runID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	key := DerivePublishKey(runID)
	if key != "of-p26-pub-v1:11111111-2222-3333-4444-555555555555" {
		t.Fatalf("unexpected key %q", key)
	}
	if key != DerivePublishKey(runID) {
		t.Fatal("key derivation must be deterministic")
	}
	if DerivePublishKey(uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")) == key {
		t.Fatal("different runs must derive different keys")
	}
}

func TestRecoveryBackoffCapped(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, {1, time.Second}, {2, 2 * time.Second},
		{3, 4 * time.Second}, {4, 8 * time.Second},
		{5, 16 * time.Second}, {9, 16 * time.Second},
	}
	for _, c := range cases {
		if got := RecoveryBackoff(c.attempt); got != c.want {
			t.Fatalf("RecoveryBackoff(%d) = %s, want %s", c.attempt, got, c.want)
		}
	}
}

func TestRequestFingerprintOfIsSha256OfExactBytes(t *testing.T) {
	a := RequestFingerprintOf([]byte(`{"a":1}`))
	if len(a) != 64 {
		t.Fatalf("fingerprint length %d", len(a))
	}
	if a != RequestFingerprintOf([]byte(`{"a":1}`)) {
		t.Fatal("same bytes must fingerprint identically")
	}
	if a == RequestFingerprintOf([]byte(`{"a": 1}`)) {
		t.Fatal("byte contract: whitespace difference must change the fingerprint")
	}
}

func TestValidateFrozenPublishMaterialAcceptsBothModes(t *testing.T) {
	id := ids(t)
	if err := ValidateFrozenPublishMaterial(id.runID, validIncrementalMaterial(t, id)); err != nil {
		t.Fatalf("valid incremental material rejected: %v", err)
	}
	if err := ValidateFrozenPublishMaterial(id.runID, validSnapshotMaterial(t, id)); err != nil {
		t.Fatalf("valid snapshot material rejected: %v", err)
	}
	// Zero-byte inline content (empty batch) is a legal inline shape.
	if err := ValidateFrozenPublishMaterial(id.runID, material(t, id, "", nil, []byte{}, 0)); err != nil {
		t.Fatalf("zero-byte snapshot material rejected: %v", err)
	}
}

func TestValidatePublishCandidateForMode(t *testing.T) {
	cand := "cpc-ok"
	if err := ValidatePublishCandidateForMode("cursor", &cand); err != nil {
		t.Fatalf("incremental with candidate must pass: %v", err)
	}
	if err := ValidatePublishCandidateForMode("cursor", nil); err == nil {
		t.Fatal("incremental without candidate must fail")
	}
	if err := ValidatePublishCandidateForMode("", &cand); err == nil {
		t.Fatal("snapshot with candidate must fail")
	}
	if err := ValidatePublishCandidateForMode("", nil); err != nil {
		t.Fatalf("snapshot without candidate must pass: %v", err)
	}
	bad := ""
	if err := ValidatePublishCandidateForMode("cursor", &bad); err == nil {
		t.Fatal("empty candidate must fail")
	}
}

// TestValidateFrozenMaterialParents (review-w2-round1 item 5): the gate's
// identity anchor — a caller-self-consistent material whose lineage names
// OTHER parents than the run's trusted frozen source/def must be refused,
// even though ValidateFrozenPublishMaterial alone would accept it.
func TestValidateFrozenMaterialParents(t *testing.T) {
	id := ids(t)
	mat := validIncrementalMaterial(t, id)
	if err := validateFrozenMaterialParents(id.sourceID, id.defID, mat); err != nil {
		t.Fatalf("matching parents must pass: %v", err)
	}
	// Build a material whose body lineage names a DIFFERENT def — internally
	// consistent (ValidateFrozenPublishMaterial passes) but foreign to the
	// run's frozen parents.
	foreign := ids(t)
	foreignMat := validIncrementalMaterial(t, foreign)
	if err := ValidateFrozenPublishMaterial(foreign.runID, foreignMat); err != nil {
		t.Fatalf("self-consistent foreign material must pass body validation: %v", err)
	}
	err := validateFrozenMaterialParents(id.sourceID, id.defID, foreignMat)
	if !errors.Is(err, ErrRecoveryMaterialParentMismatch) {
		t.Fatalf("foreign parents must be refused with the typed error, got: %v", err)
	}
}

func TestValidateFrozenPublishMaterialRejections(t *testing.T) {
	id := ids(t)

	blankBranch := validSnapshotMaterial(t, id)
	blankBranch.Branch = " "
	if err := ValidateFrozenPublishMaterial(id.runID, blankBranch); err == nil ||
		!strings.Contains(err.Error(), "branch") {
		t.Fatalf("blank branch must be rejected: %v", err)
	}

	otherKey := material(t, id, "", nil, []byte("x"), 1)
	otherKey.ExactBody = buildBody(t, "of-p26-pub-v1:"+uuid.New().String(), "main",
		otherKey.LogicalPath, otherKey.ContentHash, 1, []byte("x"), id.sourceID, id.defID, id.runID)
	if err := ValidateFrozenPublishMaterial(id.runID, otherKey); err == nil ||
		!strings.Contains(err.Error(), "idempotency key") {
		t.Fatalf("foreign key in body must be rejected: %v", err)
	}

	branchDrift := validSnapshotMaterial(t, id)
	branchDrift.Branch = "dev"
	if err := ValidateFrozenPublishMaterial(id.runID, branchDrift); err == nil {
		t.Fatal("material branch drift vs body must be rejected")
	}

	// v2.1: declared hash that does not match the ACTUAL bytes must fail even
	// when the metadata declaration is internally consistent.
	lyingHash := material(t, id, "", nil, []byte("real-bytes"), 1)
	lyingHash.ContentHash = hex64("declared-not-actual")
	lyingHash.ExactBody = buildBody(t, DerivePublishKey(id.runID), "main",
		lyingHash.LogicalPath, lyingHash.ContentHash, 1, []byte("real-bytes"), id.sourceID, id.defID, id.runID)
	if err := ValidateFrozenPublishMaterial(id.runID, lyingHash); err == nil ||
		!strings.Contains(err.Error(), "actual inline content") {
		t.Fatalf("declared-only hash must be rejected: %v", err)
	}

	// v2.1: byte-count drift vs the actual content length.
	lyingBytes := material(t, id, "", nil, []byte("real-bytes"), 1)
	lyingBytes.Bytes++
	if err := ValidateFrozenPublishMaterial(id.runID, lyingBytes); err == nil {
		t.Fatal("byte drift vs actual content must be rejected")
	}

	// v2.1: lineage mismatch.
	foreignLineage := material(t, id, "", nil, []byte("x"), 1)
	foreignLineage.ExactBody = buildBody(t, DerivePublishKey(id.runID), "main",
		foreignLineage.LogicalPath, foreignLineage.ContentHash, 1, []byte("x"),
		uuid.New(), id.defID, id.runID)
	if err := ValidateFrozenPublishMaterial(id.runID, foreignLineage); err == nil ||
		!strings.Contains(err.Error(), "lineage") {
		t.Fatalf("foreign lineage must be rejected: %v", err)
	}

	// v2.1: mode pairing — snapshot material carrying a candidate.
	snapshotWithCandidate := validSnapshotMaterial(t, id)
	cand := "cpc"
	snapshotWithCandidate.CompletedCheckpoint = &cand
	if err := ValidateFrozenPublishMaterial(id.runID, snapshotWithCandidate); err == nil {
		t.Fatal("snapshot material with a candidate must be rejected")
	}

	badCandidate := validIncrementalMaterial(t, id)
	empty := ""
	badCandidate.CompletedCheckpoint = &empty
	if err := ValidateFrozenPublishMaterial(id.runID, badCandidate); err == nil {
		t.Fatal("empty candidate must be rejected")
	}

	oversized := validSnapshotMaterial(t, id)
	oversized.ExactBody = append([]byte(nil), make([]byte, MaxPublishIntentBodyBytes+1)...)
	if err := ValidateFrozenPublishMaterial(id.runID, oversized); err == nil ||
		!strings.Contains(err.Error(), "body") {
		t.Fatalf("oversized body must be rejected: %v", err)
	}

	notJSON := validSnapshotMaterial(t, id)
	notJSON.ExactBody = []byte("not-json")
	if err := ValidateFrozenPublishMaterial(id.runID, notJSON); err == nil {
		t.Fatal("non-JSON body must be rejected")
	}

	badHash := validSnapshotMaterial(t, id)
	badHash.ContentHash = "XYZ"
	if err := ValidateFrozenPublishMaterial(id.runID, badHash); err == nil {
		t.Fatal("non-64-hex content hash must be rejected")
	}
}

func intentFixture(runID uuid.UUID) PublishIntentMaterial {
	cand := "cpc-123"
	return PublishIntentMaterial{
		RunID:                runID,
		SyncDefID:            uuid.New(),
		SourceID:             uuid.New(),
		RecoveryProtocol:     "p26-v1",
		PublishKey:           DerivePublishKey(runID),
		RequestFingerprint:   hex64("fingerprint"),
		ExactBody:            []byte(`{"idempotency_key":"` + DerivePublishKey(runID) + `"}`),
		OutputDatasetID:      uuid.MustParse("99999999-9999-9999-9999-999999999999"),
		Branch:               "main",
		LogicalPath:          "sync.ndjson",
		ContentHash:          hex64("content"),
		Rows:                 7,
		Bytes:                512,
		CompletedCheckpoint:  &cand,
		CPMode:               CPModeCursor,
		CPBaseEpoch:          3,
		CPBaseValue:          "tok",
		CPBindingFingerprint: hex64("binding"),
		RecoveryState:        IntentStateArmed,
	}
}

func matchingReceipt(m PublishIntentMaterial) PublishReceipt {
	return PublishReceipt{
		IdempotencyKey:      m.PublishKey,
		RequestFingerprint:  m.RequestFingerprint,
		DatasetID:           m.OutputDatasetID,
		Branch:              m.Branch,
		TransactionID:       uuid.MustParse("88888888-8888-8888-8888-888888888888"),
		Status:              "COMMITTED",
		MetadataContentHash: m.ContentHash,
		MetadataSyncRunID:   m.RunID,
		MetadataSourceID:    m.SourceID,
		MetadataSyncDefID:   m.SyncDefID,
	}
}

func TestReceiptMatchesIntentAcceptsExactMatch(t *testing.T) {
	m := intentFixture(uuid.New())
	if err := ReceiptMatchesIntent(matchingReceipt(m), m); err != nil {
		t.Fatalf("exact receipt rejected: %v", err)
	}
}

func TestReceiptMatchesIntentRejections(t *testing.T) {
	m := intentFixture(uuid.New())

	notCommitted := matchingReceipt(m)
	notCommitted.Status = "OPEN"
	if err := ReceiptMatchesIntent(notCommitted, m); err == nil {
		t.Fatal("OPEN receipt must be rejected")
	}

	wrongKey := matchingReceipt(m)
	wrongKey.IdempotencyKey = "of-p26-pub-v1:" + uuid.New().String()
	if err := ReceiptMatchesIntent(wrongKey, m); err == nil {
		t.Fatal("foreign key receipt must be rejected")
	}

	wrongDataset := matchingReceipt(m)
	wrongDataset.DatasetID = uuid.New()
	if err := ReceiptMatchesIntent(wrongDataset, m); err == nil {
		t.Fatal("foreign dataset receipt must be rejected")
	}

	nilTxn := matchingReceipt(m)
	nilTxn.TransactionID = uuid.Nil
	if err := ReceiptMatchesIntent(nilTxn, m); err == nil {
		t.Fatal("nil transaction receipt must be rejected")
	}

	wrongFingerprint := matchingReceipt(m)
	wrongFingerprint.RequestFingerprint = hex64("other")
	if err := ReceiptMatchesIntent(wrongFingerprint, m); err == nil {
		t.Fatal("fingerprint drift must be rejected (byte contract)")
	}

	wrongHash := matchingReceipt(m)
	wrongHash.MetadataContentHash = hex64("other")
	if err := ReceiptMatchesIntent(wrongHash, m); err == nil {
		t.Fatal("content hash drift must be rejected")
	}

	wrongRun := matchingReceipt(m)
	wrongRun.MetadataSyncRunID = uuid.New()
	if err := ReceiptMatchesIntent(wrongRun, m); err == nil {
		t.Fatal("foreign lineage run must be rejected")
	}

	// Root direction: the repo independently re-verifies EVERY receipt
	// identity field — branch and the full lineage triple included.
	wrongBranch := matchingReceipt(m)
	wrongBranch.Branch = "dev"
	if err := ReceiptMatchesIntent(wrongBranch, m); err == nil {
		t.Fatal("branch drift must be rejected")
	}

	wrongSource := matchingReceipt(m)
	wrongSource.MetadataSourceID = uuid.New()
	if err := ReceiptMatchesIntent(wrongSource, m); err == nil {
		t.Fatal("foreign lineage source must be rejected")
	}

	wrongDef := matchingReceipt(m)
	wrongDef.MetadataSyncDefID = uuid.New()
	if err := ReceiptMatchesIntent(wrongDef, m); err == nil {
		t.Fatal("foreign lineage def must be rejected")
	}
}

func TestPublishIntentSafeViewNeverCarriesRawMaterial(t *testing.T) {
	m := intentFixture(uuid.New())
	view := PublishIntentSafeView(&m)
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("marshal view: %v", err)
	}
	s := string(encoded)
	for _, forbidden := range []string{
		m.PublishKey, m.RequestFingerprint, *m.CompletedCheckpoint,
		m.CPBindingFingerprint, m.CPBaseValue, string(m.ExactBody),
	} {
		if forbidden != "" && strings.Contains(s, forbidden) {
			t.Fatalf("safe view leaked raw material %q", forbidden)
		}
	}
	if view.State != IntentStateArmed || view.SendBudget != MaxPublishSendAttempts ||
		view.QueryBudget != MaxRecoveryQueryAttempts || view.BodyBytes != len(m.ExactBody) ||
		view.CheckpointLen != len(*m.CompletedCheckpoint) {
		t.Fatalf("safe view counters wrong: %+v", view)
	}

	// Snapshot intents project a zero checkpoint length, never a value.
	snap := m
	snap.CPMode = ""
	snap.CompletedCheckpoint = nil
	snapView := PublishIntentSafeView(&snap)
	if snapView.CheckpointLen != 0 {
		t.Fatalf("snapshot view checkpoint length %d", snapView.CheckpointLen)
	}
}
