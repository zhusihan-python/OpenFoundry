package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

const (
	manualOutputKeyPrefix = "pbs-manual-output-v1:"
	manualOutputPurpose   = "manual-canonical-output"
)

var errManualOutputReservation = errors.New("manual output reservation does not match the active branch and build")

func validateManualOutputRequest(body models.CommitDatasetOutputRequest, subject uuid.UUID) error {
	binding := body.ManualReservation
	if binding == nil {
		if strings.HasPrefix(body.IdempotencyKey, manualOutputKeyPrefix) {
			return errors.New("manual output key requires a manual reservation")
		}
		return nil
	}
	if binding.BuildID == uuid.Nil || binding.ReservationID == uuid.Nil || stagedCommitRequested(body) {
		return errors.New("invalid manual output reservation binding")
	}
	if body.TransactionType != "" && body.TransactionType != models.TransactionTypeSnapshot {
		return errors.New("manual reservation requires a SNAPSHOT output")
	}
	// Receipts replay before the build callback runs. Authenticate the key
	// against the current subject before looking up the receipt.
	expected := manualOutputKeyPrefix + subject.String() + ":" + binding.BuildID.String() + ":" + binding.ReservationID.String()
	if body.IdempotencyKey != expected {
		return errors.New("invalid manual output idempotency_key")
	}
	return nil
}

func exchangeManualOutputReservation(ctx context.Context, tx repo.OutputCommitTxStore, dataset *models.Dataset, branch *models.RuntimeBranch, binding *models.ManualOutputReservation, subject uuid.UUID) error {
	// The file index and lineage are dataset-wide. The keyed commit already
	// locks the dataset row, so a concurrent active-branch change cannot
	// redirect these writes.
	if branch.Name != dataset.ActiveBranch {
		return errManualOutputReservation
	}
	reservation, err := tx.GetRuntimeTransactionForUpdate(ctx, dataset.ID, binding.ReservationID)
	if err != nil {
		return err
	}
	if reservation == nil || reservation.DatasetID != dataset.ID ||
		reservation.BranchID != branch.ID || reservation.BranchName != branch.Name ||
		reservation.Status != models.TransactionStatusOpen || reservation.TxType != models.TransactionTypeUpdate ||
		reservation.StartedBy == nil || *reservation.StartedBy != subject {
		return errManualOutputReservation
	}
	buildID, marked := manualOutputReservationBuild(reservation.Providence)
	if !marked || buildID != binding.BuildID {
		return errManualOutputReservation
	}
	// An error later in the publish callback rolls this back with the
	// SNAPSHOT, index and receipt. The branch is never released between them.
	return tx.AbortReservedTransaction(ctx, dataset.ID, reservation.ID)
}

func manualOutputReservationBuild(raw models.JSONValue) (uuid.UUID, bool) {
	var provenance struct {
		Origin  string    `json:"origin"`
		Purpose string    `json:"purpose"`
		BuildID uuid.UUID `json:"build_id"`
	}
	if err := json.Unmarshal(raw, &provenance); err != nil {
		return uuid.Nil, false
	}
	return provenance.BuildID, provenance.Origin == "pipeline-build-service" && provenance.Purpose == manualOutputPurpose
}

func manualOutputReceipt(datasetID uuid.UUID, branch string, transaction models.RuntimeTransaction) models.CommitDatasetOutputResponse {
	transaction.Summary = ""
	transaction.Metadata = nil
	transaction.Providence = nil
	return models.CommitDatasetOutputResponse{
		DatasetID: datasetID, DatasetRID: "ri.foundry.main.dataset." + datasetID.String(), Branch: branch,
		Transaction: transaction,
	}
}
