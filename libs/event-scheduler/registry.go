package eventscheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Schema registry verification for the outbox dispatch mode (T8
// decision §2: every event MUST be schema-validated before publish,
// and ADR-0038 §1 requires the schema to be REGISTERED — local
// validation alone is not enough; the producer must also prove the
// registered artifact is the one it validated against).
//
// The verifier consults the registry once per tick BEFORE any row is
// claimed. Two failure families, deliberately distinct:
//
//   - transport error / non-200 / artifact missing → ErrRegistryUnreachable
//     (retryable: abort tick, every due definition stays due);
//   - artifact reachable but content sha256 != pinned fingerprint →
//     ErrRegistryContentMismatch (deploy drift: abort tick, rows stay
//     due; never quarantine innocent definitions for it).
//
// Pinned version: apicurio/apicurio-registry-mem:2.6.13.Final, whose
// v2 REST API was verified against the OpenAPI spec shipped in the
// Apicurio source tree at tag 2.6.13.Final:
//
//	GET /apis/registry/v2/groups/{groupId}/artifacts/{artifactId}
//	  → getLatestArtifact: raw content bytes of the latest version
//	    (200 | 404 | 500)
//	POST /apis/registry/v2/groups/{groupId}/artifacts
//	  → createArtifact (X-Registry-ArtifactId / X-Registry-ArtifactType
//	    headers, ifExists query param) — used by the infra-side
//	    registration job, not by this producer.
//
// The 3.x registry line publishes no -mem/-sql container tags (verified
// on hub.docker.com and quay.io tag APIs 2026-11-17), so 2.6.13.Final +
// v2 API is the pinned, self-consistent choice for this round.

// Pinned schema fingerprints (sha256 over the exact bytes of
// libs/schedule-events/schema/*.json — the same bytes the registration
// job registers). A unit test re-computes these from the files, so a
// schema edit on the contract side fails the producer's build until
// the fingerprint (and the registered artifact) is updated.
const (
	// TriggerSchemaSHA256 pins pipeline.schedule.triggered.v1.json.
	TriggerSchemaSHA256 = "cd18c6212d2d9d35b0c092669daee58049c36e15d8a3174ca173e95e63eea92f"
	// OutcomeSchemaSHA256 pins pipeline.schedule.outcome.v1.json
	// (re-pinned 2026-11-17 after M1 added the schedule_key pattern to
	// the outcome payload — same constraint as the trigger schema).
	OutcomeSchemaSHA256 = "aef7a88509a097fe41c8ec50d484ca75d75910c2bc1881c3104138a56fc06a12"

	// TriggerSchemaArtifactID is the registry artifact id for the
	// trigger schema — the schema $id, kept stable across topics.
	TriggerSchemaArtifactID = "pipeline.schedule.triggered.v1"
	// OutcomeSchemaArtifactID is the registry artifact id for the
	// outcome schema.
	OutcomeSchemaArtifactID = "pipeline.schedule.outcome.v1"

	// DefaultRegistryGroup matches Apicurio's conventional bucket for
	// ungrouped artifacts (what the platform's other registration jobs
	// use).
	DefaultRegistryGroup = "default"
)

// SchemaRegistryVerifier consults the schema registry for the content
// of a registered artifact. Implementations must be safe for
// concurrent use.
type SchemaRegistryVerifier interface {
	// ContentSHA256 fetches the artifact's latest-version content and
	// returns the sha256 hex digest of the raw bytes. Any transport
	// error, non-200 status, or missing artifact is returned as an
	// error (the caller maps it to ErrRegistryUnreachable).
	ContentSHA256(ctx context.Context, group, artifactID string) (string, error)
}

// ApicurioRegistry is the [SchemaRegistryVerifier] for Apicurio
// Registry 2.6.x over the v2 REST API.
type ApicurioRegistry struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewApicurioRegistry builds a verifier for the registry rooted at
// baseURL (e.g. http://apicurio.apicurio.svc:8080). Nil client gets a
// sane default with a bounded timeout.
func NewApicurioRegistry(baseURL string, client *http.Client) *ApicurioRegistry {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &ApicurioRegistry{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: client}
}

// ContentSHA256 implements [SchemaRegistryVerifier].
func (r *ApicurioRegistry) ContentSHA256(ctx context.Context, group, artifactID string) (string, error) {
	url := fmt.Sprintf("%s/apis/registry/v2/groups/%s/artifacts/%s", r.BaseURL, group, artifactID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build request for artifact %s: %w", artifactID, err)
	}
	// Ask for the exact bytes; Apicurio echoes the stored content.
	req.Header.Set("Accept", "application/json")

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET artifact %s: %w", artifactID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount for the error message, then report
		// status — 404 means "not registered yet" (registration job
		// not run), 401/403 auth misconfig: both retryable registry
		// states, never a definition quarantine.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("GET artifact %s: status %d: %s", artifactID, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read artifact %s content: %w", artifactID, err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// VerifyRegistryFingerprints checks that both frozen schedule-event
// schemas are registered with exactly the pinned content. It runs once
// per tick, before any definition is claimed, so a registry problem
// can never leave a half-fired tick. A nil verifier is a
// misconfiguration and fails closed (root review round 1): production
// ticks must never silently skip registry verification — tests inject
// a fake [SchemaRegistryVerifier] instead.
func (s *OutboxScheduler) VerifyRegistryFingerprints(ctx context.Context) error {
	if s.verifier == nil {
		return &SchedulerError{
			Kind:     ErrRegistryUnreachable,
			Artifact: TriggerSchemaArtifactID,
			Cause:    errors.New("no schema registry verifier configured (set SCHEDULES_REGISTRY_URL)"),
		}
	}
	group := s.registryGroup
	for _, pin := range []struct {
		artifact string
		want     string
	}{
		{TriggerSchemaArtifactID, TriggerSchemaSHA256},
		{OutcomeSchemaArtifactID, OutcomeSchemaSHA256},
	} {
		got, err := s.verifier.ContentSHA256(ctx, group, pin.artifact)
		if err != nil {
			return &SchedulerError{
				Kind:     ErrRegistryUnreachable,
				Artifact: pin.artifact,
				Cause:    err,
			}
		}
		if got != pin.want {
			return &SchedulerError{
				Kind:       ErrRegistryContentMismatch,
				Artifact:   pin.artifact,
				WantSHA256: pin.want,
				GotSHA256:  got,
			}
		}
	}
	return nil
}
