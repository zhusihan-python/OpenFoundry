package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

const (
	manualOutputPurpose   = "manual-canonical-output"
	manualOutputKeyPrefix = "pbs-manual-output-v1:"
	transactionRIDPrefix  = "ri.foundry.main.transaction."
)

type manualDatasetReservation struct {
	BuildID       uuid.UUID `json:"build_id"`
	ReservationID uuid.UUID `json:"reservation_id"`
}

type manualDatasetOutput struct {
	client  DatasetHTTPVersioning
	dataset string
	branch  string
	key     string
	subject uuid.UUID
	binding manualDatasetReservation
}

func newManualDatasetOutput(client DatasetHTTPVersioning, subject, buildID uuid.UUID, output executor.OutputTransaction) (manualDatasetOutput, error) {
	reservationID, err := uuid.Parse(strings.TrimPrefix(output.TransactionRID, transactionRIDPrefix))
	if err != nil || reservationID == uuid.Nil {
		return manualDatasetOutput{}, fmt.Errorf("invalid manual output reservation RID %q", output.TransactionRID)
	}
	datasetID, err := uuid.Parse(strings.TrimPrefix(output.DatasetRID, datasetRIDPrefix))
	if err != nil {
		return manualDatasetOutput{}, err
	}
	key := manualOutputKeyPrefix + subject.String() + ":" + buildID.String() + ":" + reservationID.String()
	return manualDatasetOutput{
		client: client, dataset: datasetRIDPrefix + datasetID.String(), branch: output.Branch, key: key, subject: subject,
		binding: manualDatasetReservation{BuildID: buildID, ReservationID: reservationID},
	}, nil
}

func (m manualDatasetOutput) commitPayload(output executor.OutputTransaction, result executor.NodeResult) ([]byte, string, error) {
	body, err := datasetOutputCommitBody(output, result)
	if err != nil {
		return nil, "", err
	}
	body.IdempotencyKey = m.key
	body.ManualReservation = &m.binding
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, "", err
	}
	fingerprint := sha256.Sum256(payload)
	return payload, hex.EncodeToString(fingerprint[:]), nil
}

func (m manualDatasetOutput) commit(ctx context.Context, payload []byte, fingerprint string) (string, error) {
	path := "/v1/datasets/" + url.PathEscape(m.dataset) + "/outputs:commit"
	status, raw, err := m.client.do(ctx, http.MethodPost, path, payload)
	if err != nil {
		return "", err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return "", datasetErrorFromResponse(status, raw)
	}
	return m.decodeReceipt(raw, fingerprint)
}

func (m manualDatasetOutput) receipt(ctx context.Context, expectedFingerprint string) (string, error) {
	path := "/v1/datasets/" + url.PathEscape(m.dataset) + "/outputs/commits/" + url.PathEscape(m.key)
	status, raw, err := m.client.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound {
		return "", nil
	}
	if status != http.StatusOK {
		return "", datasetErrorFromResponse(status, raw)
	}
	return m.decodeReceipt(raw, expectedFingerprint)
}

func (m manualDatasetOutput) decodeReceipt(raw []byte, expectedFingerprint string) (string, error) {
	var receipt struct {
		DatasetRID  string `json:"dataset_rid"`
		Branch      string `json:"branch"`
		Key         string `json:"idempotency_key"`
		Fingerprint string `json:"request_fingerprint"`
		Transaction struct {
			ID         uuid.UUID `json:"id"`
			BranchName string    `json:"branch_name"`
			Type       string    `json:"tx_type"`
			Status     string    `json:"status"`
			DatasetID  uuid.UUID `json:"dataset_id"`
			StartedBy  uuid.UUID `json:"started_by"`
		} `json:"transaction"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return "", fmt.Errorf("decode manual output receipt: %w", err)
	}
	if receipt.Key != m.key || receipt.DatasetRID != m.dataset || receipt.Branch != m.branch ||
		receipt.Transaction.BranchName != m.branch || receipt.Transaction.Status != "COMMITTED" ||
		receipt.Transaction.Type != "SNAPSHOT" || receipt.Transaction.ID == uuid.Nil ||
		datasetRIDPrefix+receipt.Transaction.DatasetID.String() != m.dataset || receipt.Transaction.StartedBy != m.subject ||
		receipt.Transaction.ID == m.binding.ReservationID || len(receipt.Fingerprint) != sha256.Size*2 ||
		(expectedFingerprint != "" && receipt.Fingerprint != expectedFingerprint) {
		return "", fmt.Errorf("manual output receipt has inconsistent publication identity")
	}
	return transactionRIDPrefix + receipt.Transaction.ID.String(), nil
}
