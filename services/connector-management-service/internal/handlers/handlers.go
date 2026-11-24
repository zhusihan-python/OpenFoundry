// Package handlers wires the HTTP endpoints for connector-management-service.
package handlers

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/adapters"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain"
	syncdomain "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain/sync"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

type Store interface {
	ListConnections(ctx context.Context, t repo.Tenant, ownerFilter *uuid.UUID) ([]models.Connection, error)
	GetConnection(ctx context.Context, id uuid.UUID) (*models.Connection, error)
	GetConnectionForTenant(ctx context.Context, id uuid.UUID, t repo.Tenant) (*models.Connection, error)
	CreateConnection(ctx context.Context, body *models.CreateConnectionRequest, t repo.Tenant) (*models.Connection, error)
	UpdateConnection(ctx context.Context, id uuid.UUID, t repo.Tenant, body *models.UpdateConnectionRequest) (*models.Connection, error)
	DeleteConnection(ctx context.Context, id uuid.UUID, t repo.Tenant) (bool, error)
	ListSyncJobs(ctx context.Context, sourceID uuid.UUID, t repo.Tenant) ([]models.SyncJob, error)
	GetSyncJob(ctx context.Context, id uuid.UUID, t repo.Tenant) (*models.SyncJob, error)
	CreateSyncJob(ctx context.Context, body *models.CreateSyncJobRequest, t repo.Tenant) (*models.SyncJob, error)
	UpdateSyncJob(ctx context.Context, id uuid.UUID, body *models.UpdateSyncJobRequest, t repo.Tenant) (*models.SyncJob, error)
	RunSyncJob(ctx context.Context, id uuid.UUID, t repo.Tenant) (*models.SyncRun, error)
	EnqueueSyncRun(ctx context.Context, syncID, sourceID uuid.UUID, t repo.Tenant, requestedBy uuid.UUID) (*models.SyncRun, error)
	ClaimSyncRun(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, ttl time.Duration) (*repo.LeaseClaim, error)
	RenewSyncRunLease(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, ttl time.Duration) (*repo.LeaseClaim, error)
	CheckSyncRunLease(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int) (bool, error)
	// P2.2 interruptible-run surface.
	RequestCancelSyncRun(ctx context.Context, runID, syncID uuid.UUID, t repo.Tenant, cancelledBy *uuid.UUID, cause string) (*repo.CancelResult, error)
	BeginPublishGate(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, ttl time.Duration) (*repo.PublishGateResult, error)
	GetSyncRunForTenant(ctx context.Context, syncID, runID uuid.UUID, t repo.Tenant) (*models.SyncRun, error)
	CheckSyncRunControl(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int) (bool, bool, error)
	CompleteLeasedSyncRun(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, status string, bytesWritten, filesWritten, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error)
	HasActiveSyncRunsForSource(ctx context.Context, sourceID uuid.UUID, t repo.Tenant) (bool, error)
	HasActiveSyncRunsForDef(ctx context.Context, defID uuid.UUID, t repo.Tenant) (bool, error)
	// P2.5 checkpoint surface (safe projections + checkpoint-aware ops).
	SyncRunIncrementalInfo(ctx context.Context, runID uuid.UUID, t repo.Tenant) (*models.RunIncrementalView, error)
	GetCheckpointView(ctx context.Context, defID uuid.UUID, t repo.Tenant) (*repo.CheckpointView, error)
	BeginIncrementalPublishGate(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, ttl time.Duration) (*repo.IncrementalPublishGateResult, error)
	CompleteIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, adv repo.IncrementalAdvance) (*models.SyncRun, error)
	CompleteSyncRun(ctx context.Context, runID uuid.UUID, t repo.Tenant, status string, bytesWritten int64, filesWritten int64, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error)
	ListSyncRuns(ctx context.Context, syncID uuid.UUID, t repo.Tenant) ([]models.SyncRun, error)
	ListCredentials(ctx context.Context, sourceID uuid.UUID, t repo.Tenant) ([]models.CredentialResponse, error)
	SetCredential(ctx context.Context, sourceID uuid.UUID, t repo.Tenant, kind string, ciphertext []byte, fingerprint string) (*models.CredentialResponse, error)
	GetSourceCredentialCiphertext(ctx context.Context, sourceID uuid.UUID, t repo.Tenant, kind string) ([]byte, error)
	ListConnectorAgents(ctx context.Context, ownerID uuid.UUID) ([]models.ConnectorAgent, error)
	RegisterConnectorAgent(ctx context.Context, body *models.RegisterAgentRequest, ownerID uuid.UUID) (*models.ConnectorAgent, error)
	HeartbeatConnectorAgent(ctx context.Context, id uuid.UUID, body *models.AgentHeartbeatRequest, ownerID uuid.UUID) (*models.ConnectorAgent, error)
	DeleteConnectorAgent(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (bool, error)
	ListSourcePolicies(ctx context.Context, sourceID uuid.UUID, t repo.Tenant) ([]models.SourcePolicyBindingResponse, error)
	AttachPolicy(ctx context.Context, sourceID uuid.UUID, t repo.Tenant, policyID uuid.UUID, kind string) (*models.SourcePolicyBindingResponse, error)
	DetachPolicy(ctx context.Context, sourceID uuid.UUID, t repo.Tenant, policyID uuid.UUID) (bool, error)
	ListMediaSetSyncs(ctx context.Context, sourceID uuid.UUID, ownerID uuid.UUID) ([]models.MediaSetSync, error)
	GetMediaSetSync(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (*models.MediaSetSync, error)
	CreateMediaSetSync(ctx context.Context, sourceID uuid.UUID, body *models.CreateMediaSetSyncRequest, ownerID uuid.UUID) (*models.MediaSetSync, error)
	UpdateMediaSetSync(ctx context.Context, id uuid.UUID, body *models.UpdateMediaSetSyncRequest, ownerID uuid.UUID) (*models.MediaSetSync, error)
	EnableVirtualTableSource(ctx context.Context, sourceRID string, body *models.EnableVirtualTableSourceRequest) (*models.VirtualTableSourceLink, error)
	CreateVirtualTable(ctx context.Context, sourceRID string, actorID string, body *models.CreateVirtualTableRequest) (*models.VirtualTable, error)
	ListVirtualTables(ctx context.Context, ownerID string, project, source string, limit int) ([]models.VirtualTable, error)
	GetVirtualTable(ctx context.Context, rid string, ownerID string) (*models.VirtualTable, error)
	ListRegistrations(ctx context.Context, sourceID uuid.UUID) ([]models.ConnectionRegistration, error)
	UpsertRegistration(ctx context.Context, sourceID uuid.UUID, source models.DiscoveredSource, mode string, autoSync bool, updateDetection bool, targetDatasetID *uuid.UUID, metadata json.RawMessage) (*models.ConnectionRegistration, error)
	GetRegistration(ctx context.Context, sourceID uuid.UUID, registrationID uuid.UUID) (*models.ConnectionRegistration, error)
	DeleteRegistration(ctx context.Context, sourceID uuid.UUID, registrationID uuid.UUID) (bool, error)
	UpdateConnectionConfig(ctx context.Context, id uuid.UUID, config json.RawMessage) (*models.Connection, error)
	ListIcebergNamespaces(ctx context.Context) ([]models.Connection, error)
	GetIcebergConnection(ctx context.Context, namespace string) (*models.Connection, error)
	ListIcebergTables(ctx context.Context, connectionID uuid.UUID) ([]models.ConnectionRegistration, error)
}

type RuntimeConfig struct {
	DatasetServiceURL            string
	PipelineServiceURL           string
	OntologyServiceURL           string
	IngestionReplicationGRPCURL  string
	NetworkBoundaryServiceURL    string
	SyncPollIntervalSecs         uint64
	AllowPrivateNetworkEgress    bool
	MySQLSyncEndpoint            string
	PostgresSyncEndpoint         string
	S3SyncEndpoint               string
	S3SyncRegion                 string
	AllowedEgressHosts           []string
	AgentStaleAfterSecs          uint64
	CredentialEncryptionKey      string
	CredentialKey                [32]byte
	SecretManagerURL             string
	OutboxEnabled                bool
	AutoRegistrationIntervalSecs uint64
	VendedCredentialsTTLSeconds  int64
	// SyncLeaseHeartbeatInterval overrides the lease heartbeat cadence
	// (default syncLeaseHeartbeatIn). Zero/negative falls back to the
	// default; tests use it to exercise heartbeat-driven loss without
	// waiting a full interval.
	SyncLeaseHeartbeatInterval time.Duration
	// SyncCancelPollInterval overrides the DB cancel-request observation
	// cadence (default 1s): how quickly a persisted cancel request or a lost
	// lease interrupts an in-flight page. Zero/negative falls back.
	SyncCancelPollInterval time.Duration
}

type Handlers struct {
	Repo              Store
	AdapterRegistry   *adapters.Registry
	MediaSetRuntime   MediaSetRuntime
	DatasetVersioning cmruntime.DatasetVersioningPort
	// DatasetRecovery is the P2.6 keyed-commit port (optional: nil falls
	// back to the same dataset-versioning base URL as the legacy port).
	DatasetRecovery cmruntime.DatasetRecoveryPort
	Config          RuntimeConfig
	// Coordinator manages interruptible executions under the service
	// lifecycle (both the default synchronous path and async admissions).
	// nil falls back to a lazy background-rooted default for tests and
	// embedders that never wire a service context.
	Coordinator *RunCoordinator
	// P2.4 schedule authorization surfaces (production main wires both to the
	// same repo; nil leaves the arm/revoke/ticks routes answering 503 and the
	// GET projection empty — legacy tests/embedders stay unaffected).
	ScheduleGrants     ScheduleGrantStore
	ScheduleAuthorizer *ScheduleAuthorizer
	// ScheduleRunner is non-nil iff the scheduler loop runs on this instance
	// (drives the arm response's scheduler_loop_running flag).
	ScheduleRunner *ScheduleRunner

	leaseOnce       sync.Once
	leaseOwnerID    string
	coordinatorOnce sync.Once
}

// Sync-lease runtime knobs. The TTL bounds how long a run stays claimed
// without heartbeats; heartbeats beat at TTL/3 while the pull is in flight
// and stop before the pre-commit lease check, which itself renews for a full
// TTL so publish + final persist get one full TTL budget. Every lease call
// gets its own bounded timeout so neither a heartbeat nor the terminal write
// can hang indefinitely.
const (
	syncLeaseTTL         = 60 * time.Second
	syncLeaseHeartbeatIn = syncLeaseTTL / 3
	syncLeaseCallTimeout = 5 * time.Second
	syncPublishBudget    = 30 * time.Second
	syncFinalWriteWindow = 30 * time.Second
)

// ErrLeaseLost reports that the executing request no longer holds the run's
// lease (expired, taken over, or fenced by generation/token mismatch). The
// run is deliberately NOT reported as failed in that case: another execution
// may legitimately own it now.
var ErrLeaseLost = errors.New("sync run lease lost or fenced")

// ErrRunCancelled reports that a durable cancel request was observed for the
// executing run. Unlike ErrLeaseLost the holder still (potentially) holds a
// valid lease and MUST attempt the terminal 'aborted' write — cancellation is
// never left as an unfinalizable "lost" state.
var ErrRunCancelled = errors.New("sync run cancellation requested")

// leaseHolder returns the non-sensitive execution identity of this process
// (hostname plus a random boot suffix). It is stored in sync_runs.lease_holder
// purely as fencing material — never any credential, JWT or Authorization
// material.
func (h *Handlers) leaseHolder() string {
	h.leaseOnce.Do(func() {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown-host"
		}
		var suffix [8]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			h.leaseOwnerID = host + ":fallback"
			return
		}
		h.leaseOwnerID = host + ":" + hex.EncodeToString(suffix[:])
	})
	return h.leaseOwnerID
}

// newLeaseToken mints the unguessable per-claim secret (256 bits). It is only
// ever passed to the repo lease CAS methods and never serialized into any
// public JSON response.
func newLeaseToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// tenantFromClaims derives the trusted access domain from verified JWT
// claims. OrgID != nil scopes the caller to that organization; otherwise the
// caller is confined to their personal domain (NULL-org rows they own).
// Client-supplied org headers or body fields are never consulted.
func tenantFromClaims(claims *authmw.Claims) repo.Tenant {
	t := repo.Tenant{OwnerID: claims.Sub}
	if claims.OrgID != nil {
		t.OrgID = claims.OrgID
	}
	return t
}

// canReadSource reuses the existing owner/scope grants — the owner, admins,
// or holders of connections read/write permissions — for sources that are
// already known to be inside the caller's tenant domain. The tenant match
// itself is enforced by the repository queries, so no grant can widen the
// boundary (admins included).
func canReadSource(c *models.Connection, claims *authmw.Claims) bool {
	if c.OwnerID == claims.Sub {
		return true
	}
	if claims.HasRole("admin") {
		return true
	}
	return claims.HasPermission("connections", "read") || claims.HasPermission("connections", "write")
}

// canWriteSource is the management grant (mutate, test, trigger): owner,
// admin, or connections:write — again only within the caller's tenant.
func canWriteSource(c *models.Connection, claims *authmw.Claims) bool {
	if c.OwnerID == claims.Sub {
		return true
	}
	if claims.HasRole("admin") {
		return true
	}
	return claims.HasPermission("connections", "write")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type routeError struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeRoutePending(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, routeError{Error: code, Code: code, Message: message})
}

func routeUUIDParam(r *http.Request, names ...string) (uuid.UUID, string, error) {
	for _, name := range names {
		if raw := chi.URLParam(r, name); raw != "" {
			id, err := uuid.Parse(raw)
			return id, name, err
		}
	}
	return uuid.Nil, names[0], errors.New("missing route parameter")
}

func (h *Handlers) notImplemented(w http.ResponseWriter, r *http.Request, code string, requireAuth bool) {
	if requireAuth {
		if _, ok := requireClaims(w, r); !ok {
			return
		}
	}
	writeRoutePending(w, http.StatusNotImplemented, code, "route mounted for Rust parity; implementation pending")
}

func (h *Handlers) GetConnectorCatalog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, models.BuildGalleryCatalog(models.SnapshotRuntimes{MySQL: h.Config.MySQLSyncEndpoint != "", S3: h.Config.S3SyncEndpoint != "", Postgres: h.Config.PostgresSyncEndpoint != ""}))
}

func (h *Handlers) GetConnectorContracts(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, models.BuildConnectorContractCatalog())
}

func (h *Handlers) ListStreamingSources(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string][]models.StreamingSourceContract{"data": models.StreamingSourceContracts()})
}

func (h *Handlers) GetConnectionCapabilities(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, _, err := routeUUIDParam(r, "id", "source_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	if h.Repo == nil {
		writeRoutePending(w, http.StatusServiceUnavailable, "repository_unavailable", "connection repository is not configured")
		return
	}
	connection, err := h.Repo.GetConnectionForTenant(r.Context(), id, tenantFromClaims(claims))
	if err != nil {
		slog.Error("connection capability lookup failed", "error", err)
		writeJSONErr(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	if connection == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !canReadSource(connection, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	contract, ok := models.ConnectorProfile(connectorTypeKey(connection.ConnectorType))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no connector contract available for " + connection.ConnectorType})
		return
	}
	capabilities := h.connectionEffectiveCapabilities(*connection, contract)
	writeJSON(w, http.StatusOK, models.ConnectionCapabilityResponse{ConnectionID: connection.ID, ConnectorType: connection.ConnectorType, Status: connection.Status, Contract: contract, Capabilities: capabilities})
}

func (h *Handlers) connectionEffectiveCapabilities(connection models.Connection, contract models.ConnectorContractProfile) models.ConnectionEffectiveCapabilities {
	configKeys, inferred := models.ConfigKeys(connection.Config)
	workers := []string{"foundry"}
	if contract.Auth.SupportsPrivateNetworkAgent {
		workers = append(workers, "agent")
	}
	warnings := []string{}
	privateAllowed := h.Config.AllowPrivateNetworkEgress
	if len(h.Config.AllowedEgressHosts) > 0 {
		privateAllowed = true
	}
	requiresAgent := contract.Auth.SupportsPrivateNetworkAgent && !privateAllowed
	if requiresAgent {
		warnings = append(warnings, "private network egress is disabled; route through a connector agent or allow egress for this source")
	}
	effective := models.ConnectionEffectiveCapabilities{
		ConnectionID:                connection.ID,
		ConnectorType:               connection.ConnectorType,
		Status:                      connection.Status,
		Modes:                       append([]string(nil), contract.Sync.Modes...),
		Workers:                     workers,
		SupportsConnectionTesting:   contract.Testing.SupportsConnectionTesting,
		SupportsDiscovery:           contract.Testing.SupportsDiscovery,
		SupportsSchemaIntrospection: contract.Testing.SupportsSchemaIntrospection,
		SupportsIncremental:         contract.Sync.SupportsIncremental || inferred.HasIncrementalCursor,
		SupportsCDC:                 contract.Sync.SupportsCDC && (inferred.HasCDCSelector || connection.ConnectorType == "kafka" || connection.ConnectorType == "kinesis" || connection.ConnectorType == "iot"),
		SupportsZeroCopy:            contract.Sync.SupportsZeroCopy || inferred.RequestsZeroCopy,
		SupportsPrivateNetworkAgent: contract.Auth.SupportsPrivateNetworkAgent,
		RequiresPrivateNetworkAgent: requiresAgent,
		PrivateNetworkEgressAllowed: privateAllowed,
		AllowedEgressHosts:          append([]string(nil), h.Config.AllowedEgressHosts...),
		ConfigKeys:                  configKeys,
		ConfigInferred:              inferred,
		PolicyWarnings:              warnings,
		FoundryCompute:              models.FoundryCompute{PythonSingleNode: true, PythonSpark: true, PipelineBuilderSingleNode: true, PipelineBuilderSpark: true},
	}
	switch connectorTypeKey(connection.ConnectorType) {
	case "rest_api":
		source, err := models.ParseRESTAPISourceConfig(connection.Config)
		effective.NativeSnapshotSync = err == nil && source.Sync != nil
	case "s3":
		source, err := models.ParseS3SnapshotSource(connection.Config)
		if err == nil && source != nil {
			setTableSnapshotCapabilities(&effective, h.Config.S3SyncEndpoint != "")
		}
	case "postgresql", "postgres", "mysql":
		source, endpoint, err := h.tableSource(&connection)
		if err == nil && source != nil {
			setTableSnapshotCapabilities(&effective, endpoint != "")
		}
	}
	return effective
}

func setTableSnapshotCapabilities(e *models.ConnectionEffectiveCapabilities, enabled bool) {
	e.NativeSnapshotSync = enabled
	e.Modes = []string{}
	if enabled {
		e.Modes = []string{"batch"}
	}
	e.SupportsConnectionTesting = enabled
	e.SupportsIncremental = false
	e.SupportsCDC = false
	e.SupportsZeroCopy = false
	e.SupportsDiscovery = false
	e.SupportsSchemaIntrospection = false
}

var validCredentialKinds = map[string]bool{
	"password": true, "api_key": true, "oauth_token": true, "aws_keys": true, "service_account_json": true,
}

func (h *Handlers) ListCredentials(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, _, err := routeUUIDParam(r, "id", "source_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canReadSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	items, err := h.Repo.ListCredentials(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to list credentials")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handlers) SetCredential(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, _, err := routeUUIDParam(r, "id", "source_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	var body models.SetCredentialRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	body.Kind = strings.TrimSpace(body.Kind)
	if !validCredentialKinds[body.Kind] {
		writeJSONErr(w, http.StatusBadRequest, fmt.Sprintf("unsupported credential kind: %s", body.Kind))
		return
	}
	if body.Value == "" {
		writeJSONErr(w, http.StatusBadRequest, "credential value required")
		return
	}
	digest := sha256.Sum256([]byte(body.Value))
	fingerprint := fmt.Sprintf("%x", digest[:])
	ciphertext, err := domain.EncryptCredential(h.Config.CredentialKey, []byte(body.Value))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "credential encryption failed")
		return
	}
	v, err := h.Repo.SetCredential(r.Context(), sourceID, tenant, body.Kind, ciphertext, fingerprint)
	if writeIncrementalFenceError(w, err) {
		return
	}
	if errors.Is(err, pgx.ErrNoRows) || v == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to store credential")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) ListConnectorAgents(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	items, err := h.Repo.ListConnectorAgents(r.Context(), claims.Sub)
	if err != nil {
		slog.Error("list connector agents", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list agents")
		return
	}
	writeJSON(w, http.StatusOK, models.ListResponse[models.ConnectorAgent]{Items: items})
}

func (h *Handlers) RegisterConnectorAgent(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	var body models.RegisterAgentRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	body.AgentURL = strings.TrimSpace(body.AgentURL)
	if body.Name == "" || body.AgentURL == "" {
		writeJSONErr(w, http.StatusBadRequest, "name and agent_url required")
		return
	}
	parsed, err := url.ParseRequestURI(body.AgentURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		writeJSONErr(w, http.StatusBadRequest, "agent_url must be an http(s) URL")
		return
	}
	if !normalizeJSONObject(&body.Capabilities, "capabilities", w) {
		return
	}
	if !normalizeJSONObject(&body.Metadata, "metadata", w) {
		return
	}
	v, err := h.Repo.RegisterConnectorAgent(r.Context(), &body, claims.Sub)
	if err != nil {
		slog.Error("register connector agent", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to register agent")
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusConflict, "agent_url already registered")
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handlers) HeartbeatConnectorAgent(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, _, err := routeUUIDParam(r, "id", "agent_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid agent_id")
		return
	}
	var body models.AgentHeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if !normalizeJSONObject(&body.Capabilities, "capabilities", w) {
		return
	}
	if !normalizeJSONObject(&body.Metadata, "metadata", w) {
		return
	}
	v, err := h.Repo.HeartbeatConnectorAgent(r.Context(), id, &body, claims.Sub)
	if err != nil {
		slog.Error("connector agent heartbeat", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to update heartbeat")
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "agent not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) DeleteConnectorAgent(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, _, err := routeUUIDParam(r, "id", "agent_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid agent_id")
		return
	}
	deleted, err := h.Repo.DeleteConnectorAgent(r.Context(), id, claims.Sub)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to delete agent")
		return
	}
	if !deleted {
		writeJSONErr(w, http.StatusNotFound, "agent not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func normalizeJSONObject(raw *json.RawMessage, field string, w http.ResponseWriter) bool {
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "" || trimmed == "null" {
		*raw = json.RawMessage(`{}`)
		return true
	}
	if !json.Valid(*raw) {
		writeJSONErr(w, http.StatusBadRequest, field+" must be valid JSON")
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(*raw, &obj); err != nil {
		writeJSONErr(w, http.StatusBadRequest, field+" must be a JSON object")
		return false
	}
	return true
}

func (h *Handlers) ListSourcePolicies(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, _, err := routeUUIDParam(r, "id", "source_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canReadSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	items, err := h.Repo.ListSourcePolicies(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to list source policies")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handlers) AttachPolicy(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, _, err := routeUUIDParam(r, "id", "source_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	var body models.AttachPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.PolicyID == uuid.Nil {
		writeJSONErr(w, http.StatusBadRequest, "policy_id required")
		return
	}
	kind := strings.TrimSpace(body.Kind)
	if kind == "" {
		kind = "direct"
	}
	if kind != "direct" && kind != "agent_proxy" {
		writeJSONErr(w, http.StatusBadRequest, "kind must be 'direct' or 'agent_proxy'")
		return
	}
	v, err := h.Repo.AttachPolicy(r.Context(), sourceID, tenant, body.PolicyID, kind)
	if errors.Is(err, pgx.ErrNoRows) || v == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to attach policy")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) DetachPolicy(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, _, err := routeUUIDParam(r, "id", "source_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	policyID, _, err := routeUUIDParam(r, "policy_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid policy_id")
		return
	}
	deleted, err := h.Repo.DetachPolicy(r.Context(), sourceID, tenant, policyID)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to detach policy")
		return
	}
	if !deleted {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) ListRuns(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	syncID, _, err := routeUUIDParam(r, "id", "sync_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid sync id")
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), syncID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	items, err := h.Repo.ListSyncRuns(r.Context(), syncID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to list sync runs")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

// GetSyncRun serves GET /syncs/{id}/runs/{run_id}: a single run with its
// double binding (parent sync def + trusted tenant) verified — read
// visibility matches the run list surface. Never exposes lease-internal
// values (holder/token stay repo-internal by projection).
func (h *Handlers) GetSyncRun(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	syncID, param, err := routeUUIDParam(r, "id", "sync_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	runID, param, err := routeUUIDParam(r, "run_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	run, err := h.Repo.GetSyncRunForTenant(r.Context(), syncID, runID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if run == nil {
		writeJSONErr(w, http.StatusNotFound, "sync run not found")
		return
	}
	// P2.5 safe projection: captured incremental runs append the mode +
	// base-epoch view (raw cp_base_value/fingerprint inputs never serialize).
	// P2.6 adds the safe recovery projection for protocol-frozen runs (state,
	// budgets, timestamps only — never body/candidate/key/fingerprint).
	type syncRunResponse struct {
		models.SyncRun
		Incremental *models.RunIncrementalView `json:"incremental,omitempty"`
		Recovery    *repo.PublishIntentView    `json:"recovery,omitempty"`
	}
	response := syncRunResponse{SyncRun: *run}
	if view, verr := h.Repo.SyncRunIncrementalInfo(r.Context(), runID, tenant); verr != nil {
		writeJSONErr(w, http.StatusInternalServerError, verr.Error())
		return
	} else if view != nil {
		response.Incremental = view
	}
	if run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		if rs, rerr := h.recoveryStoreFor(); rerr == nil {
			if rview, verr := recoveryViewFor(r.Context(), rs, runID, tenant); verr != nil {
				writeJSONErr(w, http.StatusInternalServerError, verr.Error())
				return
			} else if rview != nil {
				response.Recovery = rview
			}
		}
		// A store without recovery support cannot have p26-v1 rows; leave the
		// projection absent rather than failing the whole run read.
	}
	writeJSON(w, http.StatusOK, response)
}

// GetSyncCheckpoint serves GET /syncs/{id}/checkpoint: the P2.5 per-def
// completion-checkpoint view under the same parent-chain visibility as the
// run surfaces. The opaque stored value never appears — only its length and
// sha256, the confirmed epoch, the durable owner (with its blocked reason)
// and the last confirmed batch summary.
func (h *Handlers) GetSyncCheckpoint(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	syncID, param, err := routeUUIDParam(r, "id", "sync_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), syncID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	view, err := h.Repo.GetCheckpointView(r.Context(), syncID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if view == nil {
		writeJSONErr(w, http.StatusNotFound, "incremental checkpoint not initialized for this sync definition")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// cancelResponse is the machine-readable cancel answer. Code discriminates
// the durable outcomes; Run always carries the current row (its stage fields
// make too-late/pending-cancel observable).
type cancelResponse struct {
	Error string          `json:"error,omitempty"`
	Code  string          `json:"code,omitempty"`
	Run   *models.SyncRun `json:"run,omitempty"`
}

// syncAdmissionThrottledCode is the stable machine code of every P2.3
// per-source sync-admission refusal; Reason discriminates the cause and
// RetryAfterSeconds always matches the Retry-After header.
const syncAdmissionThrottledCode = "source_sync_throttled"

// syncAdmissionErrorResponse is the 429 body of a refused admission: a
// retryable, machine-readable answer with zero durable side effects (no run
// row, no interval consumed). The messages come from the repo layer's static
// sentences — no credentials, JWTs or lease material ever appear.
type syncAdmissionErrorResponse struct {
	Error             string `json:"error"`
	Code              string `json:"code"`
	Reason            string `json:"reason"`
	Message           string `json:"message"`
	RetryAfterSeconds int    `json:"retry_after_seconds"`
}

// writeSyncAdmissionRejection answers 429 for a typed admission refusal.
// retry_after_seconds is a positive whole number consistent with the
// Retry-After header: the min_interval reason carries the ceiling of the
// post-lock DB remaining time; capacity/contention carry the pinned 1-second
// advisory hint (a suggestion, not a promise of free capacity).
func writeSyncAdmissionRejection(w http.ResponseWriter, rejection *repo.AdmissionRejection) {
	seconds := int(rejection.RetryAfter / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	writeJSON(w, http.StatusTooManyRequests, syncAdmissionErrorResponse{
		Error:             syncAdmissionThrottledCode,
		Code:              syncAdmissionThrottledCode,
		Reason:            string(rejection.Reason),
		Message:           rejection.Error(),
		RetryAfterSeconds: seconds,
	})
}

// CancelSyncRun serves POST /syncs/{id}/runs/{run_id}/cancel. Permission
// matches triggering (canWriteSource within the tenant). Outcomes:
//
//	pending            → 200, run now terminally aborted
//	running, gate open → 202, request durably persisted; the still-valid
//	                     holder interrupts I/O and confirms 'aborted'
//	already terminal   → 200, idempotent no-op with the current row
//	gate entered first → 409 cancel_too_late; the run completes under its
//	                     real outcome and is never reported as aborted
//	legacy row         → 409 cancel_unsupported_legacy; pre-P2.2 stage
//	                     columns prove nothing about remote publishes
//	not visible        → 404 (wrong parent sync, foreign tenant, unknown id)
func (h *Handlers) CancelSyncRun(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	syncID, param, err := routeUUIDParam(r, "id", "sync_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	runID, param, err := routeUUIDParam(r, "run_id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), syncID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	conn, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if conn == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(conn, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	canceller := claims.Sub
	res, err := h.Repo.RequestCancelSyncRun(r.Context(), runID, syncID, tenant, &canceller, "cancelled by request")
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	switch res.Outcome {
	case repo.CancelOutcomeNotVisible:
		writeJSONErr(w, http.StatusNotFound, "sync run not found")
	case repo.CancelOutcomeUnsupported:
		writeJSON(w, http.StatusConflict, cancelResponse{
			Error: "cancellation is not supported for runs created before interruptible control; their publish state cannot be determined from stage columns",
			Code:  "cancel_unsupported_legacy",
			Run:   res.Run,
		})
	case repo.CancelOutcomeTooLate:
		writeJSON(w, http.StatusConflict, cancelResponse{
			Error: "publish already entered; the run will complete under its real outcome and must not be reported as aborted",
			Code:  "cancel_too_late",
			Run:   res.Run,
		})
	case repo.CancelOutcomeRequested:
		writeJSON(w, http.StatusAccepted, cancelResponse{Run: res.Run})
	default: // aborted / already_terminal
		// The row is terminal and will never execute: drop any queued
		// task's Authorization reference immediately (bounded registry).
		if h.Coordinator != nil {
			h.Coordinator.DropAuthRef(runID)
		}
		writeJSON(w, http.StatusOK, cancelResponse{Run: res.Run})
	}
}

func (h *Handlers) DevAuthLogin(w http.ResponseWriter, r *http.Request) {
	h.notImplemented(w, r, "dev_auth_pending", false)
}

func (h *Handlers) DevAuthRefresh(w http.ResponseWriter, r *http.Request) {
	h.notImplemented(w, r, "dev_auth_pending", false)
}

func (h *Handlers) DevAuthBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	h.notImplemented(w, r, "dev_auth_pending", false)
}

func (h *Handlers) DevAuthMe(w http.ResponseWriter, r *http.Request) {
	h.notImplemented(w, r, "dev_auth_me_pending", true)
}

func (h *Handlers) ListConnections(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	tenant := tenantFromClaims(claims)
	var ownerID *uuid.UUID
	if raw := r.URL.Query().Get("owner_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeJSONErr(w, http.StatusBadRequest, "invalid owner_id")
			return
		}
		// owner_id may only narrow the tenant-scoped list, never widen it.
		if id != claims.Sub && !claims.HasRole("admin") {
			writeJSON(w, http.StatusOK, models.ListResponse[models.Connection]{Items: []models.Connection{}})
			return
		}
		ownerID = &id
	}
	items, err := h.Repo.ListConnections(r.Context(), tenant, ownerID)
	if err != nil {
		slog.Error("list connections", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list connections")
		return
	}
	writeJSON(w, http.StatusOK, models.ListResponse[models.Connection]{Items: items})
}

func (h *Handlers) GetConnection(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	v, err := h.Repo.GetConnectionForTenant(r.Context(), id, tenantFromClaims(claims))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "connection not found")
		return
	}
	if !canReadSource(v, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) CreateConnection(w http.ResponseWriter, r *http.Request) {
	caller, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	var body models.CreateConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.Name == "" || body.ConnectorType == "" {
		writeJSONErr(w, http.StatusBadRequest, "name and connector_type required")
		return
	}
	if len(body.Config) > 0 && !json.Valid(body.Config) {
		writeJSONErr(w, http.StatusBadRequest, "config must be valid JSON")
		return
	}
	normalized, err := normalizeConnectionConfig(body.ConnectorType, body.Config)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	body.Config = normalized
	v, err := h.Repo.CreateConnection(r.Context(), &body, tenantFromClaims(caller))
	if err != nil {
		slog.Error("create connection", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handlers) UpdateConnection(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	tenant := tenantFromClaims(claims)
	current, err := h.Repo.GetConnectionForTenant(r.Context(), id, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if current == nil {
		writeJSONErr(w, http.StatusNotFound, "connection not found")
		return
	}
	if !canWriteSource(current, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	var body models.UpdateConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if len(body.Config) > 0 && !json.Valid(body.Config) {
		writeJSONErr(w, http.StatusBadRequest, "config must be valid JSON")
		return
	}
	if len(body.Config) > 0 {
		normalized, err := normalizeConnectionConfig(current.ConnectorType, body.Config)
		if err != nil {
			writeJSONErr(w, http.StatusBadRequest, err.Error())
			return
		}
		body.Config = normalized
	}
	v, err := h.Repo.UpdateConnection(r.Context(), id, tenant, &body)
	if err != nil {
		if writeIncrementalFenceError(w, err) {
			return
		}
		if errors.Is(err, repo.ErrActiveRuns) {
			writeJSONErr(w, http.StatusConflict, "source config cannot change while pending or running sync runs exist")
			return
		}
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "connection not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func connectorTypeKey(raw string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "-", "_"))
}

func normalizeConnectionConfig(connectorType string, raw json.RawMessage) (json.RawMessage, error) {
	switch connectorTypeKey(connectorType) {
	case "s3":
		_, err := models.ParseS3SnapshotSource(raw)
		return raw, err
	case "mysql":
		_, err := models.ParseMySQLSnapshotSource(raw)
		return raw, err
	case "postgresql", "postgres":
		_, err := models.ParsePostgresSnapshotSource(raw)
		return raw, err
	case "rest_api":
		return models.NormalizeRESTAPISourceConfig(raw)
	default:
		if len(raw) == 0 {
			return raw, nil
		}
		// P2.3: sync admission is enforced for EVERY connector type (the
		// enqueue gate is type-agnostic), so the sync.admission subtree must
		// pass the same validation on write for passthrough types too. The
		// config is otherwise returned byte-for-byte unchanged.
		if _, err := models.ParseSyncAdmissionPolicy(raw); err != nil {
			return nil, err
		}
		return raw, nil
	}
}

func (h *Handlers) DeleteConnection(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	tenant := tenantFromClaims(claims)
	current, err := h.Repo.GetConnectionForTenant(r.Context(), id, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if current == nil {
		writeJSONErr(w, http.StatusNotFound, "connection not found")
		return
	}
	if !canWriteSource(current, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	deleted, err := h.Repo.DeleteConnection(r.Context(), id, tenant)
	if err != nil {
		if writeIncrementalFenceError(w, err) {
			return
		}
		if errors.Is(err, repo.ErrActiveRuns) {
			// A hard delete would cascade-delete the durable task rows; refuse
			// while any pending/running run exists. Terminal runs keep the
			// pre-P2.1 delete semantics.
			writeJSONErr(w, http.StatusConflict, "source has pending or running sync runs; let them finish or fail before deleting")
			return
		}
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !deleted {
		writeJSONErr(w, http.StatusNotFound, "connection not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requireClaims(w http.ResponseWriter, r *http.Request) (*authmw.Claims, bool) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return nil, false
	}
	return claims, true
}

func (h *Handlers) ListSyncJobs(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), sourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canReadSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	items, err := h.Repo.ListSyncJobs(r.Context(), sourceID, tenant)
	if err != nil {
		slog.Error("list sync jobs", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list sync jobs")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handlers) GetSyncJob(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	v, err := h.Repo.GetSyncJob(r.Context(), id, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	// P2.4 read projection: the nonsecret DB grant metadata (liveness at the
	// database clock) plus the honest locally_executable verdict. The
	// projection carries Source-level read context, so — exactly like
	// ListScheduleTicks — the def must resolve to a visible Source AND the
	// caller must hold canReadSource (owner / admin / connections read or
	// write) before any grant metadata is assembled. The chain is enforced
	// only when the schedule surface is wired; nil wiring (legacy
	// tests/embedders) keeps the pre-P2.4 tenant-only behavior and omits the
	// projection. locally_executable means "this process holds a live,
	// generation-matching local grant" — it never implies the scheduler loop
	// is running.
	if h.ScheduleGrants != nil {
		conn, err := h.Repo.GetConnectionForTenant(r.Context(), v.SourceID, tenant)
		if err != nil {
			writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
			return
		}
		if conn == nil {
			writeJSONErr(w, http.StatusNotFound, "source not found")
			return
		}
		if !canReadSource(conn, claims) {
			writeJSONErr(w, http.StatusForbidden, "forbidden")
			return
		}
		view, err := h.ScheduleGrants.GetScheduleGrantView(r.Context(), id, tenant)
		if err != nil {
			writeJSONErr(w, http.StatusInternalServerError, "schedule authorization lookup failed")
			return
		}
		var local bool
		if h.ScheduleAuthorizer != nil {
			local = h.ScheduleAuthorizer.MatchesLiveGeneration(id, view)
		}
		v.ScheduleAuthorization = &models.ScheduleAuthorizationView{DBGrant: view, LocallyExecutable: local}
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) CreateSyncJob(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	var body models.CreateSyncJobRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.SourceID == uuid.Nil || body.OutputDatasetID == uuid.Nil {
		writeJSONErr(w, http.StatusBadRequest, "source_id and output_dataset_id required")
		return
	}
	// P2.4 fast validation (the repo re-validates authoritatively inside its
	// transaction — the P2.3 dual-layer discipline).
	if err := validateCreateSchedule(&body); err != nil {
		writeInvalidSchedule(w, err)
		return
	}
	tenant := tenantFromClaims(claims)
	source, err := h.Repo.GetConnectionForTenant(r.Context(), body.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "connection lookup failed")
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	if err := h.precheckOutputDataset(r.Context(), body.OutputDatasetID, r.Header.Get("Authorization")); err != nil {
		writeDatasetPrecheckError(w, err)
		return
	}
	v, err := h.Repo.CreateSyncJob(r.Context(), &body, tenant)
	if err != nil {
		var invalid *models.InvalidScheduleError
		if errors.As(err, &invalid) {
			writeInvalidSchedule(w, err)
			return
		}
		slog.Error("create sync job", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to create sync job")
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handlers) UpdateSyncJob(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	// Visibility and grant checks come first so cross-tenant updates get a
	// stable 404 even while the dataset dependency is failing, and so the
	// owner/scope grants (not just tenancy) protect existing bindings.
	job, err := h.Repo.GetSyncJob(r.Context(), id, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	source, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if source == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(source, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	var body models.UpdateSyncJobRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	// P2.4 fast validation with the same legacy-compatibility rules as the
	// repo (the repo re-validates the merged state under its Def lock).
	if err := validateUpdateSchedule(job, &body); err != nil {
		writeInvalidSchedule(w, err)
		return
	}
	effectiveTarget := job.OutputDatasetID
	if body.OutputDatasetID != nil {
		effectiveTarget = *body.OutputDatasetID
	}
	if err := h.precheckOutputDataset(r.Context(), effectiveTarget, r.Header.Get("Authorization")); err != nil {
		writeDatasetPrecheckError(w, err)
		return
	}
	v, err := h.Repo.UpdateSyncJob(r.Context(), id, &body, tenant)
	if err != nil {
		if writeIncrementalFenceError(w, err) {
			return
		}
		if errors.Is(err, repo.ErrActiveRuns) {
			writeJSONErr(w, http.StatusConflict, "sync target cannot be retargeted while pending or running runs exist")
			return
		}
		var invalid *models.InvalidScheduleError
		if errors.As(err, &invalid) {
			writeInvalidSchedule(w, err)
			return
		}
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) RunSyncJob(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	// P2.2 async opt-in: absent → the original synchronous contract, exactly
	// one accepted spelling ("true") → background execution. Anything else is
	// rejected before ANY database write (zero new rows).
	query := r.URL.Query()
	asyncMode := false
	if query.Has("async") {
		if query.Get("async") != "true" {
			writeJSONErr(w, http.StatusBadRequest, `async must be "true" when present`)
			return
		}
		asyncMode = true
	}
	id, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	tenant := tenantFromClaims(claims)
	job, err := h.Repo.GetSyncJob(r.Context(), id, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if job == nil {
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	conn, err := h.Repo.GetConnectionForTenant(r.Context(), job.SourceID, tenant)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if conn == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	if !canWriteSource(conn, claims) {
		writeJSONErr(w, http.StatusForbidden, "forbidden")
		return
	}
	// P2.2 body drain: the sync trigger assigns NO meaning to a request body,
	// but an UNREAD body stops Go's HTTP/1 server from starting its
	// connection-level background read — so a client TCP disconnect would
	// never cancel r.Context() and a synchronous trigger would keep pulling
	// for a dead caller. Drain the body to EOF (bounded), so the transport
	// detects disconnects; over-limit and unreadable bodies are rejected with
	// ZERO durable rows. Must run before any admission/DB work.
	if err := drainTriggerBody(w, r); err != nil {
		return
	}
	// P2.2: every execution — synchronous or async — goes through the same
	// bounded, shutdown-tracked admission gate. Capacity is reserved BEFORE
	// any durable row exists, so queue-full/shutting-down answers 503 with
	// zero new rows.
	coordinator := h.coordinatorFor()
	admission := coordinator.Reserve()
	if admission == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "sync execution capacity is full or the service is shutting down")
		return
	}
	// P2.1: persist a pending durable task, then atomically claim it with a
	// fencing lease before any work starts. Each POST enqueues its own run —
	// manual triggers are never merged, but P2.3 bounds their ADMISSION by
	// the source's sync.admission policy (an over-limit answer is the typed
	// 429 below; it creates no run).
	run, err := h.Repo.EnqueueSyncRun(r.Context(), id, job.SourceID, tenant, claims.Sub)
	if err != nil {
		admission.Release()
		// P2.3: a typed admission refusal answers 429 with a stable code,
		// reason and Retry-After — zero rows were created (the transaction
		// rolled back), the interval was not consumed and the local
		// reservation is returned above. Every other error keeps the honest
		// 5xx path (an unknown commit outcome is never dressed up as a
		// clean zero-row refusal).
		var rejection *repo.AdmissionRejection
		var incrementalRejection *repo.IncrementalRejection
		switch {
		case errors.As(err, &incrementalRejection):
			writeSyncIncrementalRejection(w, incrementalRejection)
			return
		case errors.As(err, &rejection):
			writeSyncAdmissionRejection(w, rejection)
			return
		}
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if run == nil {
		admission.Release()
		writeJSONErr(w, http.StatusNotFound, "sync job not found")
		return
	}
	// The returned admission freezes the protocol. Legacy runs keep their
	// original local TTLs even if the Source configuration changes later.
	dispatch := func(task coordinatorTask) bool {
		if run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
			return admission.DispatchWithJWTExpiry(task, r.Header.Get("Authorization"), jwtExpiryFromClaims(claims))
		}
		return admission.Dispatch(task, r.Header.Get("Authorization"))
	}
	if asyncMode {
		// Hand the durable task to the coordinator queue; the caller's
		// Authorization moves into the bounded, expiry-swept private registry
		// (never the queue, never persisted). 202 returns immediately with
		// the pending run; the worker claims and executes under the service
		// root context, independent of this request's lifetime.
		if !dispatch(coordinatorTask{
			runID:       run.ID,
			syncID:      id,
			sourceID:    job.SourceID,
			tenant:      tenant,
			requestedBy: claims.Sub,
		}) {
			// Shutdown raced after the durable admission. Ownership of the
			// permit stayed with THIS request: close the durable row first
			// (bounded), then release — never the other way around, so Stop
			// cannot return and let main close the pool before this cleanup.
			// A p26-v1 row is deliberately left for fresh-auth recovery.
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), syncLeaseCallTimeout)
			closeErr := h.closeUnexecutedRunForShutdown(closeCtx, run.ID, id, tenant, "aborted: service shutdown before dispatch")
			cancel()
			admission.Release()
			if closeErr != nil {
				slog.Error("failed to close run after shutdown race; run stays honestly unconfirmed",
					slog.String("error", closeErr.Error()), slog.String("sync_run_id", run.ID.String()))
			}
			writeJSONErr(w, http.StatusServiceUnavailable, "service is shutting down before dispatch; run was not executed: "+run.ID.String())
			return
		}
		writeJSON(w, http.StatusAccepted, run)
		return
	}
	// Synchronous mode — same queue, same worker, same bounded concurrency;
	// only the waiting differs. The worker owns the claim and execution under
	// the service lifecycle; this handler waits on a buffered result channel
	// while the triggering request's context stays linked to the execution
	// (client disconnect interrupts the work, P2.1 semantics preserved).
	resultCh := make(chan SyncExecResult, 1)
	if !dispatch(coordinatorTask{
		runID:       run.ID,
		syncID:      id,
		sourceID:    job.SourceID,
		tenant:      tenant,
		requestedBy: claims.Sub,
		syncWait:    true,
		parentCtx:   r.Context(),
		resultCh:    resultCh,
	}) {
		// Shutdown raced after the durable admission (sync mode): same
		// bounded close-then-release ordering; a p26-v1 row is deliberately
		// left for fresh-auth recovery.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), syncLeaseCallTimeout)
		closeErr := h.closeUnexecutedRunForShutdown(closeCtx, run.ID, id, tenant, "aborted: service shutdown before execution")
		cancel()
		admission.Release()
		if closeErr != nil {
			slog.Error("failed to close run after shutdown race; run stays honestly unconfirmed",
				slog.String("error", closeErr.Error()), slog.String("sync_run_id", run.ID.String()))
		}
		writeJSONErr(w, http.StatusServiceUnavailable, "service is shutting down before execution; run was not executed: "+run.ID.String())
		return
	}
	// The execution is bounded by the admission deadlines plus the publish
	// and final-write windows; wait defensively within that envelope. The
	// result channel is buffered, so the worker never blocks on us. A client
	// disconnect ALSO gets a bounded wrap-up: the execution is interrupted
	// through its request link and converges, so drain its answer with a
	// short grace instead of hanging until the task budget.
	_, taskDeadline := admission.Deadlines()
	waitBudget := time.Until(taskDeadline) + syncPublishBudget + syncFinalWriteWindow + 5*time.Second
	if waitBudget <= 0 {
		waitBudget = syncPublishBudget + syncFinalWriteWindow + 5*time.Second
	}
	graceBudget := syncPublishBudget + syncFinalWriteWindow + 5*time.Second
	var res SyncExecResult
	select {
	case res = <-resultCh:
	case <-r.Context().Done():
		select {
		case res = <-resultCh:
		case <-time.After(graceBudget):
			slog.Error("request cancelled and execution did not report within the grace budget; worker still converges",
				slog.String("sync_run_id", run.ID.String()))
			writeJSONErr(w, http.StatusServiceUnavailable, "sync run outcome is uncertain — request cancelled before the execution reported: "+run.ID.String())
			return
		}
	case <-time.After(waitBudget):
		slog.Error("sync execution did not report within the bounded wait; worker still converges",
			slog.String("sync_run_id", run.ID.String()))
		writeJSONErr(w, http.StatusServiceUnavailable, "sync run outcome is uncertain — execution did not report within its budget: "+run.ID.String())
		return
	}
	if res.Err != nil {
		slog.Warn("sync run did not reach a persisted terminal state",
			slog.String("error", res.Err.Error()), slog.String("sync_run_id", run.ID.String()))
		var claimFail *claimFailureError
		switch {
		case errors.Is(res.Err, errSyncClaimLost):
			writeJSONErr(w, http.StatusConflict, "sync run was claimed by another execution")
		case errors.As(res.Err, &claimFail):
			writeJSONErr(w, http.StatusServiceUnavailable, cmruntime.SanitizeSyncError(claimFail.Error())+": "+run.ID.String())
		case errors.Is(res.Err, ErrLeaseLost):
			writeJSONErr(w, http.StatusConflict, "sync run lease was lost or fenced; run left for re-execution: "+run.ID.String())
		default:
			writeJSONErr(w, http.StatusServiceUnavailable, "sync run outcome is uncertain — publish may or may not have happened, terminal state not persisted: "+cmruntime.SanitizeSyncError(res.Err.Error()))
		}
		return
	}
	writeJSON(w, http.StatusAccepted, res.Run)
}

// syncTriggerBodyLimit bounds how many semantically-meaningless body bytes
// the sync trigger is willing to consume (and thus how much memory/time a
// hostile caller can demand from a trigger that uses none of it).
const syncTriggerBodyLimit = 1 << 20 // 1 MiB

// syncTriggerBodyReadBudget bounds the WALL TIME the drain may take, so a
// trickling body cannot hold the trigger (and its worker slot) hostage.
const syncTriggerBodyReadBudget = 5 * time.Second

// drainTriggerBody consumes a request body the sync trigger never reads
// semantically, up to EOF, so the HTTP/1 connection's background read starts
// and client disconnects actually cancel r.Context(). Contract (merged
// review):
//   - the route-local read deadline (5s) MUST install. Any SetReadDeadline
//     failure — including http.ErrNotSupported — fails the trigger
//     immediately with Connection: close and zero durable rows: the real
//     server writer and the chi v5 Compress chain both support deadlines,
//     and without one a trickling body would be unbounded. (Bare
//     httptest.ResponseRecorder does not; handler-level tests must supply a
//     deadline-capable writer or a real server.)
//   - the deadline is cleared (to zero, and the clear must succeed) ONLY
//     after a true EOF within the byte cap — the pull that follows must not
//     inherit the 5s bound.
//   - refusals (oversize 413 / read-deadline 408 / other read error 400)
//     KEEP the short deadline and set Connection: close, so net/http never
//     auto-drains the remaining body (an unbounded read) to reuse the
//     connection. Generic error text is sanitized (never a raw URL).
//   - every refusal returns before Reserve/Enqueue: zero durable rows.
func drainTriggerBody(w http.ResponseWriter, r *http.Request) error {
	if r.Body == nil || (r.ContentLength <= 0 && len(r.TransferEncoding) == 0) {
		return nil
	}
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Now().Add(syncTriggerBodyReadBudget)); err != nil {
		w.Header().Set("Connection", "close")
		writeJSONErr(w, http.StatusInternalServerError, "sync trigger requires a request-body read deadline this connection does not support")
		return err
	}
	n, copyErr := io.Copy(io.Discard, io.LimitReader(r.Body, syncTriggerBodyLimit+1))
	if copyErr == nil && n <= syncTriggerBodyLimit {
		// True EOF inside the cap: clear the deadline — and the clear must
		// succeed, otherwise the execution would inherit an invisible 5s
		// read bound.
		if err := rc.SetReadDeadline(time.Time{}); err != nil {
			w.Header().Set("Connection", "close")
			writeJSONErr(w, http.StatusInternalServerError, "sync trigger body read deadline could not be cleared")
			return err
		}
		return nil
	}
	// Refusal paths keep the SHORT deadline and close the connection (no
	// unbounded auto-drain by net/http's connection reuse).
	w.Header().Set("Connection", "close")
	switch {
	case copyErr != nil && errors.Is(copyErr, os.ErrDeadlineExceeded):
		writeJSONErr(w, http.StatusRequestTimeout, "sync trigger request body was not received within the read deadline")
		copyErr = fmt.Errorf("sync trigger body read deadline exceeded after %d bytes", n)
	case copyErr != nil:
		writeJSONErr(w, http.StatusBadRequest, "sync trigger request body could not be read: "+cmruntime.SanitizeSyncError(copyErr.Error()))
	default: // n > syncTriggerBodyLimit
		writeJSONErr(w, http.StatusRequestEntityTooLarge, "sync trigger accepts no meaningful request body (limit is 1 MiB)")
		copyErr = fmt.Errorf("sync trigger body over limit: %d bytes", n)
	}
	return copyErr
}

// writeIncrementalFenceError maps the P2.5 unresolved-checkpoint-owner fence
// to a typed 409. The message carries def/run ids and active_since only —
// never checkpoint values.
func writeIncrementalFenceError(w http.ResponseWriter, err error) bool {
	if errors.Is(err, repo.ErrIncrementalOwnerUnresolved) {
		writeJSONErr(w, http.StatusConflict, "sync has an unresolved incremental checkpoint owner: "+err.Error())
		return true
	}
	// P2.6 mutation fence (root-decisions-v2.1): an effective protected
	// change (credential / file_glob) is refused while the source still has
	// pending/running p26-v1 recovery runs — a truthful conflict, not a 500.
	if errors.Is(err, repo.ErrRecoveryRunsActive) {
		writeJSONErr(w, http.StatusConflict, "source has active recoverable sync runs; the protected change is refused until they converge or are recovered")
		return true
	}
	return false
}

// writeSyncIncrementalRejection answers a P2.5 typed admission refusal with
// the ruled 409 (root Q1): a stable code, no owner leak to callers who cannot
// see the parent chain, and the newDef recovery hint for a stale binding.
func writeSyncIncrementalRejection(w http.ResponseWriter, rejection *repo.IncrementalRejection) {
	switch rejection.Kind {
	case repo.IncrementalBusy:
		writeJSON(w, http.StatusConflict, map[string]any{
			"code":          "sync_incremental_busy",
			"active_run_id": rejection.ActiveRunID,
			"active_since":  rejection.ActiveSince,
			"retry_hint":    "the previous incremental run's outcome is unconfirmed and requires reconciliation before a new run",
		})
	case repo.IncrementalBindingStale:
		writeJSON(w, http.StatusConflict, map[string]any{
			"code":     "sync_incremental_binding_stale",
			"recovery": "create a new sync definition (newDef-only reset); P25 has no reset API",
		})
	default:
		writeJSONErr(w, http.StatusConflict, "incremental admission rejected")
	}
}

// datasetVersioningPort returns the dataset publish port: the injected test
// double when present, otherwise the HTTP adapter pointed at
// DATASET_SERVICE_URL.
func (h *Handlers) datasetVersioningPort() cmruntime.DatasetVersioningPort {
	if h.DatasetVersioning != nil {
		return h.DatasetVersioning
	}
	if strings.TrimSpace(h.Config.DatasetServiceURL) == "" {
		return nil
	}
	return cmruntime.HTTPDatasetVersioningClient{BaseURL: h.Config.DatasetServiceURL}
}

// precheckOutputDataset validates a target dataset binding under the caller's
// forwarded platform JWT via the dataset-versioning runtime port. Missing
// dependencies fail closed. Returned errors distinguish caller rejections
// (cmruntime.ErrDatasetNotFound / ErrDatasetForbidden) from dependency
// failures (*cmruntime.DependencyError: 5xx, timeouts, transport).
func (h *Handlers) precheckOutputDataset(ctx context.Context, datasetID uuid.UUID, authorization string) error {
	_, err := h.precheckOutputDatasetInfo(ctx, datasetID, authorization)
	return err
}

// precheckOutputDatasetInfo is the P2.6-aware precheck: it also surfaces the
// dataset's CURRENT active branch (no server-side active/main fallback) so a
// p26-v1 run can freeze an explicit branch into its first intent. A missing
// or blank ActiveBranch is reported as "" — the p26 executor fails closed
// on it; legacy callers ignore the field entirely.
func (h *Handlers) precheckOutputDatasetInfo(ctx context.Context, datasetID uuid.UUID, authorization string) (*cmruntime.DatasetInfo, error) {
	port := h.datasetVersioningPort()
	if port == nil {
		return nil, &cmruntime.DependencyError{Op: "get_dataset", Detail: "dataset-versioning url is not configured"}
	}
	return port.GetDataset(ctx, datasetID, authorization)
}

// writeDatasetPrecheckError maps precheck failures to HTTP responses that
// separate caller rejections (404/403) from dependency failures (502/503).
func writeDatasetPrecheckError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, cmruntime.ErrDatasetNotFound):
		writeJSONErr(w, http.StatusNotFound, "output dataset not found")
	case errors.Is(err, cmruntime.ErrDatasetUnauthorized):
		writeJSONErr(w, http.StatusUnauthorized, "caller credentials were rejected by dataset-versioning")
	case errors.Is(err, cmruntime.ErrDatasetForbidden):
		writeJSONErr(w, http.StatusForbidden, "output dataset not accessible in caller domain")
	default:
		var dep *cmruntime.DependencyError
		if errors.As(err, &dep) && strings.Contains(dep.Detail, "url is not configured") {
			writeJSONErr(w, http.StatusServiceUnavailable, "dataset-versioning dependency is not configured")
			return true
		}
		writeJSONErr(w, http.StatusBadGateway, "dataset-versioning lookup failed: "+cmruntime.SanitizeSyncError(err.Error()))
	}
	return true
}

// executeSnapshotSync runs the P1.3 manual snapshot flow under the P2.1
// lease and the P2.2 interruptible control: bounded REST pull of every
// configured page with heartbeats keeping the lease alive (loss of the lease
// cancels the pull), a fast DB cancel poll that interrupts in-flight pages
// as soon as a durable cancel request appears, the P2.2 publish-entry gate
// (an atomic renew + cancel race decided under the run row lock),
// publication through the dataset outputs:commit file path under the
// caller's forwarded JWT, and a final leased terminal write with its own
// bounded timeout.
//
// finalizeOnInterrupt distinguishes the two callers: the async worker must
// converge every interruption to a persisted terminal state (aborted while
// provably pre-gate, failed once the gate is entered), while the synchronous
// path keeps the P2.1 behaviour of leaving cancelled pre-publish runs
// unwritten for takeover.
//
// The returned error is ErrLeaseLost when this execution no longer holds the
// lease, or the underlying storage error when the terminal write could not be
// persisted. In both cases NO terminal state is fabricated: the run stays
// pending takeover. Note the dataset publish itself is an HTTP call — it is
// not fenced by this database's CAS, and no exactly-once publish claim is
// made (see README).
// executeSnapshotSync is the legacy-shaped entry (normal + manual): no
// outward guard. The automatic recovery path uses executeSnapshotSyncGuarded
// with its sticky live-arm guard.
func (h *Handlers) executeSnapshotSync(ctx context.Context, authorization string, tenant repo.Tenant, lease *repo.LeaseClaim, job *models.SyncJob, conn *models.Connection, finalizeOnInterrupt bool) (*models.SyncRun, error) {
	return h.executeSnapshotSyncGuarded(ctx, authorization, tenant, lease, job, conn, finalizeOnInterrupt, nil)
}

// executeSnapshotSyncGuarded runs the SAME executor with an optional
// per-outward guard (review auto-round2 item 3): the guard runs before the
// target precheck, before EVERY upstream source page and — through the
// recoverable frame — before every recoverable send. A guard refusal cancels
// the work and prevents any new budget/remote dispatch; pre-gate refusals
// converge as provable zero-dispatch failures, post-gate ones as honest
// unknowns (never a fabricated terminal state).
func (h *Handlers) executeSnapshotSyncGuarded(ctx context.Context, authorization string, tenant repo.Tenant, lease *repo.LeaseClaim, job *models.SyncJob, conn *models.Connection, finalizeOnInterrupt bool, outwardGuard func() error) (*models.SyncRun, error) {
	runID := lease.Run.ID
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()

	// Sticky lease-failure state: once a heartbeat or page gate observes loss,
	// fencing or a renew error, this execution is dead — a later successful
	// renew on a detached context must never "revive" it into publishing. The
	// first observed cause wins.
	var stickyLost atomic.Pointer[error]
	markLost := func(err error) {
		if err == nil {
			err = ErrLeaseLost
		}
		stickyLost.CompareAndSwap(nil, &err)
	}
	stickyCause := func() error {
		if p := stickyLost.Load(); p != nil {
			return *p
		}
		return nil
	}
	// Sticky cancellation: a durable cancel request observed by the poller or
	// a page gate is final for this execution. Unlike lease loss it does NOT
	// strand the run — the holder still writes the terminal 'aborted' state
	// through its lease CAS (stage-guarded: only before the publish gate).
	var stickyCancelled atomic.Bool
	markCancelled := func() {
		stickyCancelled.Store(true)
		cancelWork()
	}

	// Bounded heartbeat + cancel-poll loop. The heartbeat renews the lease
	// while the pull is in flight (fail-closed and sticky on any renew
	// failure). The cancel poll is a cheap lock-free DB probe that interrupts
	// a slow in-flight page within one poll interval of the request being
	// persisted — a local in-memory map could be faster but would never be
	// visible cross-process, so the DB is the observation source of truth. A
	// transient probe error is skipped: fail-closed remains anchored on the
	// authoritative renew. The loop exits on stopHeartbeat (all paths call it
	// before any terminal write), on lease loss/cancellation, or when the
	// parent context is done — it always converges and never outlives the
	// execution by more than one bounded lease call.
	stopHB := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		heartbeatEvery := h.Config.SyncLeaseHeartbeatInterval
		if heartbeatEvery <= 0 {
			heartbeatEvery = syncLeaseHeartbeatIn
		}
		cancelEvery := h.Config.SyncCancelPollInterval
		if cancelEvery <= 0 {
			cancelEvery = defaultCoordinatorCancelPoll
		}
		hbTicker := time.NewTicker(heartbeatEvery)
		defer hbTicker.Stop()
		cancelTicker := time.NewTicker(cancelEvery)
		defer cancelTicker.Stop()
		for {
			select {
			case <-stopHB:
				return
			case <-ctx.Done():
				return
			case <-cancelTicker.C:
				chkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncLeaseCallTimeout)
				leaseOK, cancelRequested, err := h.Repo.CheckSyncRunControl(chkCtx, runID, tenant, lease.Holder, lease.Token, lease.Run.Generation)
				cancel()
				if err != nil {
					continue
				}
				if cancelRequested {
					markCancelled()
					return
				}
				if !leaseOK {
					markLost(ErrLeaseLost)
					cancelWork()
					return
				}
			case <-hbTicker.C:
				hbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncLeaseCallTimeout)
				renewed, err := h.Repo.RenewSyncRunLease(hbCtx, runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, syncLeaseTTL)
				cancel()
				if err != nil || renewed == nil {
					// Fail closed and STICKY: any renew failure (expiry,
					// takeover, DB error) ends this execution, even if the
					// database recovers a moment later.
					markLost(err)
					cancelWork()
					return
				}
			}
		}
	}()
	var stopOnce sync.Once
	stopHeartbeat := func() {
		stopOnce.Do(func() { close(stopHB) })
		<-hbDone
	}

	// Per-page lease+cancel gate: before every upstream page request the DB is
	// re-queried (cheap, lock-free) for the CURRENT validity of this exact
	// lease AND for a persisted cancel request — expiry, takeover or
	// cancellation between polls is observed at the next page boundary, and
	// the sticky flags keep the decision final for the whole execution.
	beforePage := func(pageCtx context.Context) error {
		if err := stickyCause(); err != nil {
			return err
		}
		if outwardGuard != nil {
			if err := outwardGuard(); err != nil {
				return fmt.Errorf("outward guard refused the source pull: %w", err)
			}
		}
		if stickyCancelled.Load() {
			return ErrRunCancelled
		}
		if err := workCtx.Err(); err != nil {
			return fmt.Errorf("work context cancelled before page: %w", err)
		}
		chkCtx, cancel := context.WithTimeout(context.WithoutCancel(pageCtx), syncLeaseCallTimeout)
		leaseOK, cancelRequested, err := h.Repo.CheckSyncRunControl(chkCtx, runID, tenant, lease.Holder, lease.Token, lease.Run.Generation)
		cancel()
		if err != nil {
			markLost(err)
			return fmt.Errorf("lease check failed: %w", err)
		}
		if !leaseOK {
			markLost(ErrLeaseLost)
			return ErrLeaseLost
		}
		if cancelRequested {
			markCancelled()
			return ErrRunCancelled
		}
		// Re-check AFTER the blocking control call (review round3 item 1):
		// an arm dropped while the DB check was in flight must still stop
		// the very next source page, and a dead work context must not start
		// new upstream work either.
		if err := workCtx.Err(); err != nil {
			return fmt.Errorf("work context cancelled before page: %w", err)
		}
		if outwardGuard != nil {
			if err := outwardGuard(); err != nil {
				return fmt.Errorf("outward guard refused the source pull: %w", err)
			}
		}
		return nil
	}
	fail := func(err error, contentHash *string) (*models.SyncRun, error) {
		// The terminal status write must survive a cancelled/dead request
		// context so a held lease still closes the run; its own bounded
		// timeout prevents an indefinite hang. A CAS or DB failure here is
		// returned as an error — never masked by a fabricated failed run.
		stopHeartbeat()
		finCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
		defer cancel()
		status := syncdomain.RunStatusFailed
		if stickyCancelled.Load() {
			// A cancel won before the publish gate: zero publishes are
			// provable (the gate precedes CommitOutput), so 'aborted' is the
			// honest terminal state. The stage guard in the repo still
			// rejects an 'aborted' write if the gate somehow entered first.
			status = syncdomain.RunStatusAborted
			if !errors.Is(err, ErrRunCancelled) {
				err = fmt.Errorf("%w: %v", ErrRunCancelled, err)
			}
		}
		return h.completeLeased(finCtx, tenant, lease, status, 0, 0, 0, err, nil, contentHash)
	}
	pull, err := h.prepareSnapshotPull(conn, tenant, lease, beforePage)
	if err != nil {
		return fail(err, nil)
	}
	incremental := lease.Run.CPMode != ""
	if outwardGuard != nil {
		if err := outwardGuard(); err != nil {
			return fail(fmt.Errorf("outward guard refused the target precheck: %w", err), nil)
		}
	}
	// Re-validate the saved target under the caller's JWT before any upstream
	// pull: cross-org / missing targets fail the run with nothing fetched and
	// nothing published. The final outputs:commit still re-adjudicates on the
	// dataset side, so races are covered too.
	datasetInfo, err := h.precheckOutputDatasetInfo(workCtx, job.OutputDatasetID, authorization)
	if err != nil {
		return fail(fmt.Errorf("output dataset validation failed: %s", cmruntime.SanitizeSyncError(err.Error())), nil)
	}
	// P2.6 keyed-publish branch material: the CURRENT active branch of the
	// target, frozen into the run's FIRST intent. An explicit branch is
	// mandatory — the keyed commit must never rely on the dataset service's
	// active/main fallback — so a missing/blank name fails closed BEFORE any
	// fetch (provable zero-publish failure).
	activeBranch := strings.TrimSpace(datasetInfo.ActiveBranch)
	if lease.Run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 && activeBranch == "" {
		return fail(fmt.Errorf("output dataset reported no usable active branch; keyed publish requires an explicit branch"), nil)
	}
	snapshot, err := pull(workCtx)
	if err != nil {
		return fail(err, nil)
	}
	port := h.datasetVersioningPort()
	if port == nil {
		return fail(fmt.Errorf("dataset-versioning url is not configured — snapshot cannot be published"), nil)
	}
	content := snapshot.NDJSON
	if content == nil {
		// Zero-row snapshots still publish a downloadable empty .ndjson file.
		content = []byte{}
	}
	// P2.6 protocol routing (v2.1: both frozen modes — snapshot AND
	// incremental — dispatch solely by the admission-frozen run protocol).
	// It MUST run BEFORE the legacy heartbeat stop below: a p26-v1 run keeps
	// its heartbeats alive through the gate→budget→send window (root v2
	// item 5) and never enters the legacy gate/CommitOutput path.
	if lease.Run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		// Pre-gate convergence checks (the legacy block's semantics,
		// evaluated while the poller is still live): a cancel or a dead
		// request before any gate entry is provably zero-dispatch.
		if stickyCancelled.Load() {
			return h.finishAborted(ctx, tenant, lease, nil, "cancelled by request before the recoverable publish gate")
		}
		if cause := stickyCause(); cause != nil {
			if errors.Is(cause, ErrLeaseLost) {
				return nil, ErrLeaseLost
			}
			return nil, fmt.Errorf("lease was marked unhealthy during the pull: %w", cause)
		}
		if err := workCtx.Err(); err != nil {
			if finalizeOnInterrupt {
				return h.finishAborted(ctx, tenant, lease, nil, "interrupted before the recoverable publish gate: "+err.Error())
			}
			return nil, fmt.Errorf("sync execution cancelled before recoverable publish: %w", err)
		}
		return h.executeRecoverablePublish(recoverableSyncFrame{
			ctx:           ctx,
			workCtx:       workCtx,
			stopHeartbeat: stopHeartbeat,
			fail:          fail,
			tenant:        tenant,
			lease:         lease,
			job:           job,
			conn:          conn,
			snapshot:      snapshot,
			activeBranch:  activeBranch,
			incremental:   incremental,
			authorization: authorization,
			guard:         outwardGuard,
		})
	}
	// P2.2 publish-entry gate: heartbeats stop, then BeginPublishGate decides
	// the cancel-vs-publish race atomically under the run row lock — it
	// renews for a full TTL AND stamps the stage marker only when no cancel
	// request is persisted. Cancel-first: no CommitOutput may be issued and
	// the holder confirms 'aborted' (provably zero publishes). Gate-first:
	// later cancels are too-late and this execution finishes under its real
	// outcome. Sticky losses and a dead work context remain FINAL here — a
	// request that is already known-dead must not start NEW remote publish
	// work.
	stopHeartbeat()
	if stickyCancelled.Load() {
		return h.finishAborted(ctx, tenant, lease, nil, "cancelled by request before the publish gate")
	}
	if cause := stickyCause(); cause != nil {
		if errors.Is(cause, ErrLeaseLost) {
			return nil, ErrLeaseLost
		}
		return nil, fmt.Errorf("lease was marked unhealthy during the pull: %w", cause)
	}
	if err := workCtx.Err(); err != nil {
		if finalizeOnInterrupt {
			return h.finishAborted(ctx, tenant, lease, nil, "interrupted before the publish gate: "+err.Error())
		}
		return nil, fmt.Errorf("sync execution cancelled before publish: %w", err)
	}
	preCommitCtx, cancelPre := context.WithTimeout(workCtx, syncLeaseCallTimeout)
	if incremental {
		// P2.5 checkpoint-aware gate: binding/owner/base-epoch are verified
		// under the ordered Def→Checkpoint→Run locks; ENTRY is the
		// conservative unknown boundary (release requires the DB-asserted
		// publish_entered_at IS NULL downstream).
		incGate, gateErr := h.Repo.BeginIncrementalPublishGate(preCommitCtx, runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, syncLeaseTTL)
		cancelPre()
		if gateErr != nil {
			return nil, fmt.Errorf("incremental publish gate failed: %w", gateErr)
		}
		switch incGate.Outcome {
		case repo.IncrementalPublishGateEntered:
			// continue to publish
		case repo.IncrementalPublishGateCancelled:
			return h.finishAborted(ctx, tenant, lease, nil, "cancelled by request at the incremental publish gate")
		case repo.IncrementalPublishGateBindingStale:
			// Zero-dispatch provable (gate refused before entry): failed +
			// release through the routed FailIncrementalSyncRun.
			return fail(fmt.Errorf("incremental binding changed since admission"), nil)
		default: // fenced / owner_lost: zero write, owner retained
			return nil, ErrLeaseLost
		}
	} else {
		gate, err := h.Repo.BeginPublishGate(preCommitCtx, runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, syncLeaseTTL)
		cancelPre()
		if err != nil {
			// The gate decision could not be made: no publish may start, and the
			// run is left unwritten for takeover (same P2.1 semantics as a failed
			// pre-commit renew).
			return nil, fmt.Errorf("publish gate failed: %w", err)
		}
		switch gate.Outcome {
		case repo.PublishGateFenced:
			return nil, ErrLeaseLost
		case repo.PublishGateCancelled:
			return h.finishAborted(ctx, tenant, lease, nil, "cancelled by request at the publish gate")
		}
	}
	// Second cancellation gate: the request may have died DURING the gate
	// itself (the gate deliberately runs on workCtx). Post-gate the stage
	// machine forbids 'aborted', so an async interruption here converges as
	// 'failed' with the precise cause; the synchronous path keeps the P2.1
	// no-write behaviour.
	if err := workCtx.Err(); err != nil {
		if finalizeOnInterrupt {
			finCtx, cancelFin := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
			defer cancelFin()
			return h.completeLeased(finCtx, tenant, lease, syncdomain.RunStatusFailed, 0, 0, 0,
				fmt.Errorf("interrupted after the publish gate before publish dispatch: %w", err), nil, nil)
		}
		return nil, fmt.Errorf("sync execution cancelled during pre-commit lease check: %w", err)
	}
	// Publish under a context that still RESPECTS work-context cancellation
	// (client gone / lease-canceled) and carries its own explicit publish
	// budget: a known-dead request must not start or continue remote publish
	// work. Only the post-publish terminal write is deliberately detached
	// (bounded) so an acknowledged publish is never left unpersisted.
	publishCtx, cancelPub := context.WithTimeout(workCtx, syncPublishBudget)
	defer cancelPub()
	publishMetadata := map[string]any{
		"content_hash": snapshot.ContentHash,
		"rows":         snapshot.Rows,
		"bytes":        snapshot.Bytes,
		"pages":        snapshot.Pages,
		"lineage": map[string]any{
			"source_id":   conn.ID,
			"sync_def_id": job.ID,
			"sync_run_id": runID,
		},
	}
	if incremental {
		// Lengths and counters only — the opaque checkpoint value never
		// enters publish metadata (root Q4 ruling).
		publishMetadata["incremental"] = map[string]any{
			"mode":                     lease.Run.CPMode,
			"base_epoch":               lease.Run.CPBaseEpoch,
			"input_checkpoint_len":     len(lease.Run.CPBaseValue),
			"completed_checkpoint_len": snapshot.CompletedCheckpointLen,
		}
	}
	commit, err := port.CommitOutput(publishCtx, cmruntime.DatasetCommitRequest{
		DatasetID:       job.OutputDatasetID,
		Format:          "TEXT",
		LogicalPath:     fmt.Sprintf("sync-%s.ndjson", runID),
		ContentType:     "application/x-ndjson",
		Content:         content,
		RowCount:        snapshot.Rows,
		ContentHash:     snapshot.ContentHash,
		Summary:         "Connector snapshot sync " + job.ID.String(),
		CreateIfMissing: false,
		Metadata:        publishMetadata,
	}, authorization)
	if err != nil {
		slog.Warn("dataset publish failed", slog.String("error", err.Error()), slog.String("sync_run_id", runID.String()))
		return h.completeFailedAfterCommitAttempt(ctx, tenant, lease, &snapshot.ContentHash, err)
	}
	if commit == nil || commit.TransactionID == uuid.Nil {
		return h.completeFailedAfterCommitAttempt(ctx, tenant, lease, &snapshot.ContentHash, fmt.Errorf("dataset publish returned no transaction id"))
	}
	if incremental {
		// P2.5 usable-ACK contract, handler side (injected ports included —
		// root correction 6): exact target + COMMITTED + nonnil txn. A 2xx
		// with an arbitrary txn UUID alone is NOT a usable ACK; this is an
		// unknown-after-dispatch failure (owner retained), never proof that
		// nothing committed.
		if commit.DatasetID != job.OutputDatasetID || commit.Status != "COMMITTED" {
			return h.completeFailedAfterCommitAttempt(ctx, tenant, lease, &snapshot.ContentHash,
				fmt.Errorf("dataset publish acknowledgement did not satisfy the incremental usable-ACK contract"))
		}
		// Success + advance + release: ONE fenced transaction, all-or-nothing.
		// A CAS miss writes nothing (typed uncertain); a lost COMMIT response
		// is an honest unknown (all-old or all-new, owner possibly cleared) —
		// never repaired or guessed here.
		finCtx, cancelFin := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
		defer cancelFin()
		run, finErr := h.Repo.CompleteIncrementalSyncRun(finCtx, runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, repo.IncrementalAdvance{
			CompletedCheckpoint: snapshot.CompletedCheckpoint,
			Rows:                snapshot.Rows,
			Bytes:               snapshot.Bytes,
			ContentHash:         snapshot.ContentHash,
			TxnID:               commit.TransactionID,
		})
		if finErr != nil {
			slog.Warn("incremental finalize did not reach a persisted terminal state",
				slog.String("error", finErr.Error()), slog.String("sync_run_id", runID.String()))
			return nil, finErr
		}
		return run, nil
	}
	version := commit.TransactionID
	// Terminal status write with its own bounded window, detached from the
	// request context so a client timeout between publish and persist cannot
	// strand the run in `running` while we still hold a valid lease.
	finCtx, cancelFin := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
	defer cancelFin()
	return h.completeLeased(finCtx, tenant, lease, syncdomain.RunStatusSucceeded, snapshot.Bytes, 1, snapshot.Rows, nil, &version, &snapshot.ContentHash)
}

// finishAborted writes the terminal 'aborted' state for a cancelled run
// through the holder's lease CAS. It is only called while the publish gate
// has NOT been entered (zero publishes provable); the repo stage guard is the
// authoritative backstop. A fenced CAS returns ErrLeaseLost — the persisted
// cancel request stays queryable and nothing is fabricated.
func (h *Handlers) finishAborted(ctx context.Context, tenant repo.Tenant, lease *repo.LeaseClaim, contentHash *string, cause string) (*models.SyncRun, error) {
	finCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
	defer cancel()
	return h.completeLeased(finCtx, tenant, lease, syncdomain.RunStatusAborted, 0, 0, 0, errors.New(cmruntime.SanitizeSyncError(cause)), nil, contentHash)
}

// completeFailedAfterCommitAttempt closes a run as failed after a publish
// attempt that did NOT return a usable acknowledgement. The upstream outcome
// is UNKNOWN in the failure cases that matter — a timed-out or dropped
// response may still have committed a dataset transaction — so this path
// records a failed run with the observed error and never claims "nothing was
// committed", never auto-retries, and never fabricates exactly-once. If the
// leased close fails, the error propagates — no fabricated terminal state.
func (h *Handlers) completeFailedAfterCommitAttempt(ctx context.Context, tenant repo.Tenant, lease *repo.LeaseClaim, contentHash *string, cause error) (*models.SyncRun, error) {
	finCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), syncFinalWriteWindow)
	defer cancel()
	return h.completeLeased(finCtx, tenant, lease, syncdomain.RunStatusFailed, 0, 0, 0, cause, nil, contentHash)
}

// resolveSnapshotAuth decrypts the external source credentials referenced by
// the source auth config. secret_ref values address credential kinds stored on
// the same source; missing or undecryptable credentials fail explicitly. The
// resolved material is only ever applied to the upstream REST source — the
// platform JWT is never sent there.
func (h *Handlers) resolveSnapshotAuth(ctx context.Context, tenant repo.Tenant, sourceID uuid.UUID, auth *models.RESTAPIAuthConfig) (cmruntime.RESTSnapshotAuth, error) {
	out := cmruntime.RESTSnapshotAuth{
		Type:       auth.Type,
		Scheme:     auth.Scheme,
		Token:      auth.Token,
		Value:      auth.Value,
		HeaderName: auth.HeaderName,
		QueryParam: auth.QueryParam,
	}
	resolve := func(ref, purpose string) (string, error) {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return "", nil
		}
		ciphertext, err := h.Repo.GetSourceCredentialCiphertext(ctx, sourceID, tenant, ref)
		if err != nil {
			return "", fmt.Errorf("credential lookup for %s failed: %w", ref, err)
		}
		if len(ciphertext) == 0 {
			return "", fmt.Errorf("credential %q referenced by source auth %s is not configured for this source", ref, purpose)
		}
		plaintext, err := domain.DecryptCredential(h.Config.CredentialKey, ciphertext)
		if err != nil {
			return "", fmt.Errorf("credential %q could not be decrypted: %w", ref, err)
		}
		return string(plaintext), nil
	}
	switch auth.Type {
	case models.RESTAPIAuthNone:
		return out, nil
	case models.RESTAPIAuthBearer, models.RESTAPIAuthAPIKey, models.RESTAPIAuthCustomHeader:
		if strings.TrimSpace(auth.SecretRef) != "" {
			token, err := resolve(auth.SecretRef, auth.Type)
			if err != nil {
				return out, err
			}
			out.Token = token
			if auth.Type == models.RESTAPIAuthCustomHeader && out.Value == "" {
				out.Value = token
			}
		}
		if out.Token == "" && out.Value == "" && auth.Type != models.RESTAPIAuthCustomHeader {
			return out, fmt.Errorf("rest_api source auth type %q requires a token, value or secret_ref", auth.Type)
		}
		return out, nil
	case models.RESTAPIAuthBasic:
		username, err := resolve(auth.UsernameSecretRef, "basic username")
		if err != nil {
			return out, err
		}
		password, err := resolve(auth.PasswordSecretRef, "basic password")
		if err != nil {
			return out, err
		}
		if username == "" || password == "" {
			return out, fmt.Errorf("rest_api source basic auth requires username_secret_ref and password_secret_ref credentials")
		}
		out.Username = username
		out.Password = password
		return out, nil
	default:
		return out, fmt.Errorf("rest_api source auth type %q is not supported by the sync runtime", auth.Type)
	}
}

// completeLeased writes the terminal state of a leased run via the lease CAS.
// It returns (nil, ErrLeaseLost) when the CAS no longer matches (lease
// expired or taken over, run already terminal) and (nil, err) on storage
// failure. The pre-P2.1 behaviour of fabricating a succeeded/failed run in
// memory after a failed persist is deliberately removed: an HTTP response can
// never claim a terminal state that is not in the database.
func (h *Handlers) completeLeased(ctx context.Context, tenant repo.Tenant, lease *repo.LeaseClaim, status string, bytesWritten int64, filesWritten int64, rowsWritten int64, runErr error, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error) {
	var errMsg *string
	if runErr != nil {
		// Defense in depth: run errors are persisted verbatim, so any URL
		// query/userinfo (e.g. query-param credentials inside transport
		// errors) is stripped before storage.
		msg := cmruntime.SanitizeSyncError(runErr.Error())
		errMsg = &msg
	}
	updated, err := h.Repo.CompleteLeasedSyncRun(ctx, lease.Run.ID, tenant, lease.Holder, lease.Token, lease.Run.Generation, status, bytesWritten, filesWritten, rowsWritten, errMsg, nil, datasetVersionID, contentHash)
	if err != nil {
		slog.Error("complete leased sync run", slog.String("error", err.Error()), slog.String("sync_run_id", lease.Run.ID.String()))
		return nil, fmt.Errorf("terminal state write failed: %w", err)
	}
	if updated == nil {
		slog.Warn("complete leased sync run fenced", slog.String("sync_run_id", lease.Run.ID.String()))
		return nil, ErrLeaseLost
	}
	return updated, nil
}

func (h *Handlers) EnableVirtualTableSource(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireClaims(w, r); !ok {
		return
	}
	sourceRID := strings.TrimSpace(chi.URLParam(r, "source_rid"))
	if sourceRID == "" {
		writeJSONErr(w, http.StatusBadRequest, "source_rid required")
		return
	}
	var body models.EnableVirtualTableSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	v, err := h.Repo.EnableVirtualTableSource(r.Context(), sourceRID, &body)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) CreateVirtualTable(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceRID := strings.TrimSpace(chi.URLParam(r, "source_rid"))
	if sourceRID == "" {
		writeJSONErr(w, http.StatusBadRequest, "source_rid required")
		return
	}
	var body models.CreateVirtualTableRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if strings.TrimSpace(body.ProjectRID) == "" || strings.TrimSpace(body.TableType) == "" {
		writeJSONErr(w, http.StatusBadRequest, "project_rid and table_type required")
		return
	}
	v, err := h.Repo.CreateVirtualTable(r.Context(), sourceRID, claims.Sub.String(), &body)
	if errors.Is(err, repo.ErrConflict) {
		writeJSONErr(w, http.StatusConflict, "virtual table already registered")
		return
	}
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "source not enabled")
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handlers) ListVirtualTables(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	items, err := h.Repo.ListVirtualTables(r.Context(), claims.Sub.String(), r.URL.Query().Get("project"), r.URL.Query().Get("source"), limit)
	if err != nil {
		slog.Error("list virtual tables", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list virtual tables")
		return
	}
	writeJSON(w, http.StatusOK, models.ListVirtualTablesResponse{Items: items})
}

func (h *Handlers) GetVirtualTable(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	rid := strings.TrimSpace(chi.URLParam(r, "rid"))
	if rid == "" {
		writeJSONErr(w, http.StatusBadRequest, "rid required")
		return
	}
	v, err := h.Repo.GetVirtualTable(r.Context(), rid, claims.Sub.String())
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "virtual table not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) ListMediaSetSyncs(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	items, err := h.Repo.ListMediaSetSyncs(r.Context(), sourceID, claims.Sub)
	if err != nil {
		slog.Error("list media set syncs", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list media set syncs")
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handlers) GetMediaSetSync(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	v, err := h.Repo.GetMediaSetSync(r.Context(), id, claims.Sub)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "media set sync not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) CreateMediaSetSync(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	sourceID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	var body models.CreateMediaSetSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if errs := body.Validate(); len(errs) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string][]string{"errors": errs})
		return
	}
	v, err := h.Repo.CreateMediaSetSync(r.Context(), sourceID, &body, claims.Sub)
	if err != nil {
		slog.Error("create media set sync", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to create media set sync")
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "source not found")
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handlers) UpdateMediaSetSync(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	var body models.UpdateMediaSetSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.Kind != nil && !body.Kind.Valid() {
		writeJSON(w, http.StatusBadRequest, map[string][]string{"errors": {"kind must be MEDIA_SET_SYNC or VIRTUAL_MEDIA_SET_SYNC"}})
		return
	}
	if body.TargetMediaSetRID != nil && !strings.HasPrefix(strings.TrimSpace(*body.TargetMediaSetRID), "ri.foundry.main.media_set.") {
		writeJSON(w, http.StatusBadRequest, map[string][]string{"errors": {"target_media_set_rid must start with ri.foundry.main.media_set."}})
		return
	}
	v, err := h.Repo.UpdateMediaSetSync(r.Context(), id, &body, claims.Sub)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "media set sync not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) RunMediaSetSync(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireClaims(w, r)
	if !ok {
		return
	}
	id, param, err := routeUUIDParam(r, "sync_id", "id")
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid "+param)
		return
	}
	var body models.RunMediaSetSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	sync, err := h.Repo.GetMediaSetSync(r.Context(), id, claims.Sub)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sync == nil {
		writeJSONErr(w, http.StatusNotFound, "media set sync not found")
		return
	}
	if h.MediaSetRuntime == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "media set runtime not configured")
		return
	}
	report, err := h.MediaSetRuntime.ExecuteMediaSetSync(r.Context(), sync, &body, r.Header.Get("Authorization"))
	if err != nil {
		writeJSONErr(w, runtimeHTTPStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, report)
}
