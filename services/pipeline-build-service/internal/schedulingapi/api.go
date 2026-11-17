// T8 scheduling HTTP API (T8 decision §4/§6): bounded revocable grants
// and scheduling definitions. The tenant domain is ALWAYS derived from
// the validated JWT claims (never the request body); personal-domain
// access is owner-only even for admins; org access is owner-or-admin
// and always confined to the caller's own org. Every mutating handler
// performs its authorization against the LOCKED database row inside
// the same transaction that writes it (postgres *As CRUD) — the HTTP
// layer never trusts in-body identity fields and rejects unknown body
// fields outright.
// Package schedulingapi serves the T8 scheduling HTTP surface
// (/api/v1/scheduling/*). Kept out of the handler package to avoid the
// handler<->postgres import cycle.
package schedulingapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/go-chi/chi/v5"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/scheduling"
)

// SchedulingAPI serves /api/v1/scheduling/*.
type SchedulingAPI struct {
	repo   *postgres.Repository
	jwt    *authmw.JWTConfig
	cipher *scheduling.TokenCipher
}

// NewSchedulingAPI builds the API; cipher may be nil when the consumer
// is disabled (grant creation is then refused — fail-closed).
func New(repo *postgres.Repository, jwt *authmw.JWTConfig, cfg config.SchedulerConfig) (*SchedulingAPI, error) {
	api := &SchedulingAPI{repo: repo, jwt: jwt}
	if cfg.Enabled {
		c, err := scheduling.NewTokenCipher(cfg.CredentialEncryptionKey)
		if err != nil {
			return nil, err
		}
		api.cipher = c
	}
	return api, nil
}

type createGrantRequest struct {
	ScheduleKey string `json:"schedule_key"`
	PipelineRID string `json:"pipeline_rid"`
	Token       string `json:"token"` // the caller JWT to run scheduled executions as
}

type grantResponse struct {
	ID         uuid.UUID `json:"id"`
	Generation int       `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type enableDefinitionRequest struct {
	ScheduleKey     string `json:"schedule_key"`
	PipelineRID     string `json:"pipeline_rid"`
	BuildBranch     string `json:"build_branch"`
	Cron            string `json:"cron"`
	GrantID         string `json:"grant_id"`
	GrantGeneration int    `json:"grant_generation"`
}

// Event-contract field constraints (schedule-events schema,
// pipeline.schedule.triggered.v1): the schedule key charset excludes
// "/" so aggregate ids stay parseable; pipeline/branch lengths bound
// the trigger payload. Validated BEFORE any database work.
var (
	scheduleKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._~:-]{1,200}$`)
)

func validateScheduleKey(key string) error {
	if !scheduleKeyPattern.MatchString(key) {
		return errors.New("schedule_key must match ^[A-Za-z0-9._~:-]{1,200}$ (no '/'), per the trigger event contract")
	}
	return nil
}

func validatePipelineRID(rid string) error {
	if len(rid) < 1 || len(rid) > 300 {
		return errors.New("pipeline_rid must be 1..300 characters, per the trigger event contract")
	}
	return nil
}

func validateBuildBranch(branch string) error {
	if len(branch) < 1 || len(branch) > 200 {
		return errors.New("build_branch must be 1..200 characters, per the trigger event contract")
	}
	return nil
}

// actorFromRequest derives the trusted caller identity from the
// VALIDATED claims attached by the auth middleware: an OrgID claim
// selects the org domain, its absence the personal domain. The request
// body never contributes. A nil/zero sub cannot manage anything.
func actorFromRequest(r *http.Request) (postgres.SchedulingActor, error) {
	user, ok := authmw.AuthUserFromContext(r.Context())
	if !ok || user.Claims == nil {
		return postgres.SchedulingActor{}, invalidRequest("authenticated context required")
	}
	claims := user.Claims
	if claims.Sub == uuid.Nil {
		return postgres.SchedulingActor{}, invalidRequest("authenticated subject required")
	}
	actor := postgres.SchedulingActor{
		Subject: claims.Sub,
		Admin:   claims.HasRole("admin"),
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindPersonal, ID: claims.Sub},
	}
	if claims.OrgID != nil {
		id := *claims.OrgID
		if id == uuid.Nil {
			return postgres.SchedulingActor{}, invalidRequest("org claim carried a nil org id")
		}
		actor.Tenant = scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: id}
	}
	return actor, nil
}

// decodeBody decodes a JSON body with unknown fields REJECTED, so a
// caller can never smuggle tenant/admin/owner selectors into a struct
// a future implementation might read.
func decodeBody(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func chiParam(r *http.Request, key string) string {
	return chi.URLParam(r, key)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// notFoundCode picks the per-resource 404 vocabulary.
var notFoundCode = map[string]string{"grant": "grant_not_found", "definition": "definition_not_found"}

// writeSchedulingError maps CRUD sentinels onto the frozen HTTP error
// vocabulary. Cross-domain misses are the SAME 404 as in-domain ones;
// infrastructure failures are 500 with no detail (no oracle, no leak).
func writeSchedulingError(w http.ResponseWriter, resource string, err error) {
	var noDetail *noDetailError
	switch {
	case errors.Is(err, postgres.ErrSchedulingNotFound):
		code := "not_found"
		if c, ok := notFoundCode[resource]; ok {
			code = c
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": code})
	case errors.Is(err, postgres.ErrSchedulingForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
	case errors.Is(err, postgres.ErrGrantGenerationMismatch):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "grant_generation_mismatch"})
	case errors.Is(err, postgres.ErrGrantNotActive):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "grant_not_active"})
	case errors.Is(err, postgres.ErrGrantExpired):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "grant_expired"})
	case errors.Is(err, postgres.ErrGrantPipelineMismatch):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "grant_pipeline_mismatch"})
	case errors.Is(err, postgres.ErrInvalidCron):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_cron"})
	case errors.As(err, &noDetail):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
	default:
		// Bad JSON/missing fields/contract violations carry a helpful
		// detail; anything else is infrastructure and stays opaque.
		var derr *detailError
		if errors.As(err, &derr) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "detail": derr.msg})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
	}
}

// detailError marks a caller-input error whose message is safe to echo.
type detailError struct{ msg string }

func (e *detailError) Error() string { return e.msg }

func invalidRequest(msg string) error { return &detailError{msg: msg} }

// CreateGrant verifies+encrypts the token and re-grants the schedule.
// Request body never selects the tenant domain; re-granting a key that
// already has history requires ownership (org admin override) —
// verified inside the upsert transaction against the locked row.
func (a *SchedulingAPI) CreateGrant(w http.ResponseWriter, r *http.Request) {
	if a.cipher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "scheduler_disabled"})
		return
	}
	actor, err := actorFromRequest(r)
	if err != nil {
		writeSchedulingError(w, "", err)
		return
	}
	var body createGrantRequest
	if err := decodeBody(r, &body); err != nil {
		writeSchedulingError(w, "", invalidRequest("malformed request body: "+err.Error()))
		return
	}
	if body.ScheduleKey == "" || body.PipelineRID == "" || body.Token == "" {
		writeSchedulingError(w, "", invalidRequest("schedule_key, pipeline_rid and token are required"))
		return
	}
	if err := validateScheduleKey(body.ScheduleKey); err != nil {
		writeSchedulingError(w, "", invalidRequest(err.Error()))
		return
	}
	if err := validatePipelineRID(body.PipelineRID); err != nil {
		writeSchedulingError(w, "", invalidRequest(err.Error()))
		return
	}
	// The stored token is verified with the SAME verifier as HTTP auth;
	// its claims must match the CALLER's derived domain and subject (a
	// personal grant never carries an org ticket and vice versa, and a
	// subject may not grant under another subject's identity).
	tokenIdentity, err := scheduling.ValidateGrantToken(a.jwt, body.Token, time.Now())
	if err != nil {
		writeSchedulingError(w, "", invalidRequest("grant token rejected: "+err.Error()))
		return
	}
	if tokenIdentity.Tenant != actor.Tenant || tokenIdentity.Subject != actor.Subject {
		writeSchedulingError(w, "", &noDetailError{})
		return
	}
	sealed, err := a.cipher.Encrypt([]byte(body.Token))
	if err != nil {
		writeSchedulingError(w, "", err)
		return
	}
	var orgID *uuid.UUID
	if actor.Tenant.Kind == scheduleevents.TenantKindOrg {
		id := actor.Tenant.ID
		orgID = &id
	}
	id, generation, err := a.repo.UpsertGrantAs(r.Context(), actor, postgres.SchedulingGrant{
		TenantKind: string(actor.Tenant.Kind), TenantID: actor.Tenant.ID, OrgID: orgID,
		Subject: actor.Subject, ScheduleKey: body.ScheduleKey, PipelineRID: body.PipelineRID,
		ExpiresAt: tokenIdentity.EXP, TokenCiphertext: sealed, CreatedBy: actor.Subject,
	})
	if err != nil {
		writeSchedulingError(w, "grant", err)
		return
	}
	writeJSON(w, http.StatusCreated, grantResponse{ID: id, Generation: generation, ExpiresAt: tokenIdentity.EXP})
}

// noDetailError signals a 403 without echoing why (the mismatch detail
// could be used to probe which identities exist).
type noDetailError struct{}

func (*noDetailError) Error() string { return "forbidden" }

// RevokeGrant revokes (state transition; re-granting bumps generation).
// The caller must own the key's grant history (org admin override);
// a key unknown in the caller's domain — including keys that belong to
// another tenant — is a plain 404.
func (a *SchedulingAPI) RevokeGrant(w http.ResponseWriter, r *http.Request) {
	actor, err := actorFromRequest(r)
	if err != nil {
		writeSchedulingError(w, "", err)
		return
	}
	scheduleKey := chiParam(r, "key")
	if scheduleKey == "" {
		writeSchedulingError(w, "", invalidRequest("key path parameter required"))
		return
	}
	if err := a.repo.RevokeGrantAs(r.Context(), actor, scheduleKey); err != nil {
		writeSchedulingError(w, "grant", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

// EnableDefinition binds the schedule with an in-tx grant check:
// exact generation, matching pipeline, active+unexpired grant and
// caller ownership (org admin override), plus event-contract key/
// pipeline/branch/cron validation BEFORE any write. The first
// next_run_at is the schedule's real next cron instant.
func (a *SchedulingAPI) EnableDefinition(w http.ResponseWriter, r *http.Request) {
	actor, err := actorFromRequest(r)
	if err != nil {
		writeSchedulingError(w, "", err)
		return
	}
	var body enableDefinitionRequest
	if err := decodeBody(r, &body); err != nil {
		writeSchedulingError(w, "", invalidRequest("malformed request body: "+err.Error()))
		return
	}
	if body.ScheduleKey == "" || body.PipelineRID == "" || body.BuildBranch == "" ||
		body.Cron == "" || body.GrantID == "" || body.GrantGeneration < 1 {
		writeSchedulingError(w, "", invalidRequest("schedule_key, pipeline_rid, build_branch, cron, grant_id, grant_generation are required"))
		return
	}
	if err := validateScheduleKey(body.ScheduleKey); err != nil {
		writeSchedulingError(w, "", invalidRequest(err.Error()))
		return
	}
	if err := validatePipelineRID(body.PipelineRID); err != nil {
		writeSchedulingError(w, "", invalidRequest(err.Error()))
		return
	}
	if err := validateBuildBranch(body.BuildBranch); err != nil {
		writeSchedulingError(w, "", invalidRequest(err.Error()))
		return
	}
	grantRef, err := uuid.Parse(body.GrantID)
	if err != nil {
		writeSchedulingError(w, "", invalidRequest("grant_id must be a uuid"))
		return
	}
	if _, _, _, err := a.repo.EnableSchedulingDefinitionAs(r.Context(), actor,
		body.ScheduleKey, body.PipelineRID, body.BuildBranch, body.Cron,
		grantRef, body.GrantGeneration); err != nil {
		writeSchedulingError(w, "definition", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "enabled"})
}

// PauseDefinition / ResumeDefinition toggle enabled; every transition
// advances definition_revision. Resume re-validates the CURRENT grant
// and rearms next_run_at at the next future cron instant.
func (a *SchedulingAPI) PauseDefinition(w http.ResponseWriter, r *http.Request) {
	a.setDefinitionEnabled(w, r, false)
}

func (a *SchedulingAPI) ResumeDefinition(w http.ResponseWriter, r *http.Request) {
	a.setDefinitionEnabled(w, r, true)
}

func (a *SchedulingAPI) setDefinitionEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	actor, err := actorFromRequest(r)
	if err != nil {
		writeSchedulingError(w, "", err)
		return
	}
	if _, err := a.repo.SetSchedulingDefinitionStateAs(r.Context(), actor, chiParam(r, "key"), enabled); err != nil {
		writeSchedulingError(w, "definition", err)
		return
	}
	status := "paused"
	if enabled {
		status = "resumed"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

// DisableDefinition removes the binding: dispatch_mode NULL AND
// enabled=false in one revision-advancing transaction — dead for both
// ticks, never a legacy fallback.
func (a *SchedulingAPI) DisableDefinition(w http.ResponseWriter, r *http.Request) {
	actor, err := actorFromRequest(r)
	if err != nil {
		writeSchedulingError(w, "", err)
		return
	}
	if _, err := a.repo.DisableSchedulingDefinitionAs(r.Context(), actor, chiParam(r, "key")); err != nil {
		writeSchedulingError(w, "definition", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "disabled"})
}
