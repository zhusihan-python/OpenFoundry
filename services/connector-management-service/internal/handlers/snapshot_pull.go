package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

// prepareSnapshotPull validates the source before the destination precheck. The
// returned operation only resolves credentials and contacts the source after it.
func (h *Handlers) prepareSnapshotPull(conn *models.Connection, tenant repo.Tenant, lease *repo.LeaseClaim, beforePage func(context.Context) error) (func(context.Context) (*cmruntime.SnapshotResult, error), error) {
	switch connectorTypeKey(conn.ConnectorType) {
	case "postgresql", "postgres", "mysql":
		table, endpoint, err := h.tableSource(conn)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, fmt.Errorf("sync runtime for connector type %q is not available", conn.ConnectorType)
		}
		if endpoint == "" {
			return nil, errors.New("table snapshot requires a configured endpoint")
		}
		if lease.Run.CPMode != "" || lease.Run.RecoveryProtocol != "" {
			return nil, errors.New("table snapshots do not support checkpoint or recovery protocols")
		}
		return func(ctx context.Context) (*cmruntime.SnapshotResult, error) {
			return h.pullTableSnapshot(ctx, conn, tenant, table, beforePage)
		}, nil
	case "s3":
		source, err := models.ParseS3SnapshotSource(conn.Config)
		if err != nil {
			return nil, err
		}
		if source == nil || h.Config.S3SyncEndpoint == "" {
			return nil, errors.New("S3 snapshot sync requires a configured endpoint and sync.object_key")
		}
		if lease.Run.CPMode != "" || lease.Run.RecoveryProtocol != "" {
			return nil, errors.New("S3 snapshot source does not support checkpoint or recovery protocols")
		}
		return func(ctx context.Context) (*cmruntime.SnapshotResult, error) {
			return h.pullS3Snapshot(ctx, conn, tenant, source, beforePage)
		}, nil
	case "rest_api":
		// REST supports the checkpoint protocol in addition to a full snapshot.
	default:
		return nil, fmt.Errorf("sync runtime for connector type %q is not available", conn.ConnectorType)
	}
	source, err := models.ParseRESTAPISourceConfig(conn.Config)
	if err != nil {
		return nil, fmt.Errorf("invalid rest_api source config: %w", err)
	}
	if source.Sync == nil {
		return nil, fmt.Errorf("rest_api source %s has no sync configuration", conn.ID)
	}
	// P2.5: run.cp_mode is the authority (implementation-release correction
	// 2) — never the current source config. A captured incremental run whose
	// config no longer enables incremental is a fenced honest failure (the
	// admission fences make this out-of-band corruption only).
	incremental := lease.Run.CPMode != ""
	var incrementalSpec *cmruntime.IncrementalFetchSpec
	if incremental {
		inc := source.Sync.Incremental
		if inc == nil || !inc.Enabled || source.Sync.Pagination.Type != models.RESTAPISyncPaginationCursor {
			return nil, errors.New("incremental run captured, but the source config no longer enables incremental cursor sync")
		}
		// The admission-frozen input token: "" means absent (opaque tokens
		// are always nonempty when present), so the param is omitted on the
		// first-ever run without an initial_value.
		incrementalSpec = &cmruntime.IncrementalFetchSpec{
			RequestParam:            inc.RequestParam,
			CompletedCheckpointPath: inc.CompletedCheckpointPath,
			RequestValue:            lease.Run.CPBaseValue,
			HasRequestValue:         lease.Run.CPBaseValue != "",
		}
	}
	return func(workCtx context.Context) (*cmruntime.SnapshotResult, error) {
		auth, err := h.resolveSnapshotAuth(workCtx, tenant, conn.ID, &source.Auth)
		if err != nil {
			return nil, err
		}
		timeout := time.Duration(source.Sync.Limits.RequestTimeoutMS) * time.Millisecond
		client := &http.Client{
			Timeout: timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		return cmruntime.FetchRESTSnapshot(workCtx, client, cmruntime.RESTSnapshotOptions{
			BaseURL:              source.BaseURL,
			Sync:                 *source.Sync,
			Auth:                 auth,
			AllowedEgressHosts:   h.Config.AllowedEgressHosts,
			AllowPrivateNetworks: h.Config.AllowPrivateNetworkEgress,
			Egress:               source.Egress,
			BeforePage:           beforePage,
			Incremental:          incrementalSpec,
		})
	}, nil
}

func (h *Handlers) pullS3Snapshot(ctx context.Context, conn *models.Connection, tenant repo.Tenant, source *models.S3SnapshotSource, beforePage func(context.Context) error) (*cmruntime.SnapshotResult, error) {
	ciphertext, err := h.Repo.GetSourceCredentialCiphertext(ctx, conn.ID, tenant, "aws_keys")
	if err != nil || len(ciphertext) == 0 {
		return nil, errors.New("S3 source aws_keys credential unavailable")
	}
	raw, err := domain.DecryptCredential(h.Config.CredentialKey, ciphertext)
	if err != nil {
		return nil, errors.New("S3 source credential could not be decrypted")
	}
	var cred struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
		SessionToken    string `json:"session_token"`
	}
	if json.Unmarshal(raw, &cred) != nil || cred.AccessKeyID == "" || cred.SecretAccessKey == "" {
		return nil, errors.New("S3 source aws_keys requires access_key_id and secret_access_key")
	}
	return cmruntime.FetchS3Snapshot(ctx, cmruntime.S3SnapshotOptions{
		Endpoint: h.Config.S3SyncEndpoint, Region: h.Config.S3SyncRegion,
		Bucket: source.Bucket, ObjectKey: source.ObjectKey, AccessKeyID: cred.AccessKeyID,
		SecretAccessKey: cred.SecretAccessKey, SessionToken: cred.SessionToken,
		AllowedEgressHosts: h.Config.AllowedEgressHosts, AllowPrivateNetworks: h.Config.AllowPrivateNetworkEgress,
		BeforePage: beforePage,
	})
}

func (h *Handlers) pullTableSnapshot(ctx context.Context, conn *models.Connection, tenant repo.Tenant, table *models.SnapshotTable, beforeRead func(context.Context) error) (*cmruntime.SnapshotResult, error) {
	isMySQL := connectorTypeKey(conn.ConnectorType) == "mysql"
	parse, endpointValue, fetch := cmruntime.PostgresEndpoint, h.Config.PostgresSyncEndpoint, cmruntime.FetchPostgresSnapshot
	if isMySQL {
		parse, endpointValue, fetch = cmruntime.MySQLEndpoint, h.Config.MySQLSyncEndpoint, cmruntime.FetchMySQLSnapshot
	}
	endpoint, err := parse(endpointValue, h.Config.AllowedEgressHosts, h.Config.AllowPrivateNetworkEgress)
	if err != nil {
		return nil, err
	}
	ciphertext, err := h.Repo.GetSourceCredentialCiphertext(ctx, conn.ID, tenant, "password")
	if err != nil || len(ciphertext) == 0 {
		return nil, errors.New("table source password credential unavailable")
	}
	raw, err := domain.DecryptCredential(h.Config.CredentialKey, ciphertext)
	if err != nil {
		return nil, errors.New("table source credential could not be decrypted")
	}
	var login struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if json.Unmarshal(raw, &login) != nil || login.Username == "" || login.Password == "" {
		return nil, errors.New("table source credential requires username and password")
	}
	return fetch(ctx, cmruntime.TableSnapshotOptions{Endpoint: endpoint, Table: *table, AllowPrivateNetworks: h.Config.AllowPrivateNetworkEgress, User: login.Username, Password: login.Password, BeforeRead: beforeRead})
}

func (h *Handlers) tableSource(conn *models.Connection) (*models.SnapshotTable, string, error) {
	if connectorTypeKey(conn.ConnectorType) == "mysql" {
		t, e := models.ParseMySQLSnapshotSource(conn.Config)
		return t, h.Config.MySQLSyncEndpoint, e
	}
	t, e := models.ParsePostgresSnapshotSource(conn.Config)
	return t, h.Config.PostgresSyncEndpoint, e
}
