//go:build integration

package repo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func TestManualOutputCommitReservationAndReplay(t *testing.T) {
	s := newStagedStack(t, nil)
	old := s.commit(t, `{"branch":"main","idempotency_key":"f15-old","files":[{"logical_path":"old.ndjson","size_bytes":3,"content_base64":"e30K"}]}`)
	require.Equal(t, http.StatusCreated, old.Code, old.Body.String())
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "manual-canonical-output")
	before := loadDatasetState(context.Background(), t, s.pool, s.dataset.ID)
	content := []byte("{\"value\":2}\n")
	request := models.CommitDatasetOutputRequest{
		Branch: "main", TransactionType: models.TransactionTypeSnapshot,
		Files: []models.CommitOutputFile{{LogicalPath: "part-00000.ndjson", SizeBytes: int64(len(content)), ContentBase64: content}},
	}
	makeBody := func(actor, buildID uuid.UUID, mutate func(*models.CommitDatasetOutputRequest)) string {
		t.Helper()
		body := request
		body.Files = append([]models.CommitOutputFile(nil), request.Files...)
		body.ManualReservation = &models.ManualOutputReservation{BuildID: buildID, ReservationID: reservation}
		body.IdempotencyKey = "pbs-manual-output-v1:" + actor.String() + ":" + buildID.String() + ":" + reservation.String()
		if mutate != nil {
			mutate(&body)
		}
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		return string(raw)
	}
	assertPriorState := func() {
		t.Helper()
		require.Equal(t, before, loadDatasetState(context.Background(), t, s.pool, s.dataset.ID))
		require.Equal(t, "OPEN", s.txnStatus(t, reservation))
	}

	owner := s.claims.Sub
	rec := s.commit(t, makeBody(owner, uuid.New(), nil))
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	otherActor := uuid.New()
	otherToken := stagedToken(t, s.jwt, otherActor, s.claims.OrgID, []string{"dataset.write"})
	path := "/v1/datasets/" + s.rid
	txnPath := path + "/branches/main/transactions/" + reservation.String()
	rec = s.do(t, http.MethodPost, txnPath+":commit", "", nil, s.token)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	rec = s.do(t, http.MethodPost, txnPath+":abort", "", nil, otherToken)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	rec = s.do(t, http.MethodPost, path+"/outputs:commit", "application/json", strings.NewReader(makeBody(otherActor, build, nil)), otherToken)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	// Fails after the reservation exchange was attempted inside the keyed
	// transaction. Both the new SNAPSHOT and the abort must roll back.
	rec = s.commit(t, makeBody(owner, build, func(body *models.CommitDatasetOutputRequest) {
		body.Files[0].SizeBytes++
	}))
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assertPriorState()

	// A valid preview can exceed the build service's response budget.
	// Retain it on the committed dataset and keep the receipt compact.
	preview, err := json.Marshal(strings.Repeat("v", 5<<20))
	require.NoError(t, err)
	body := makeBody(owner, build, func(body *models.CommitDatasetOutputRequest) {
		body.PreviewColumns = []string{"value"}
		body.PreviewRows = [][]models.JSONValue{{preview}}
	})
	rec = s.commit(t, body)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Less(t, rec.Body.Len(), 4096)
	var first models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &first))
	require.NotEqual(t, reservation, first.Transaction.ID)
	require.Equal(t, "ABORTED", s.txnStatus(t, reservation))
	require.Equal(t, "COMMITTED", s.txnStatus(t, first.Transaction.ID))
	var previewSize int
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT length(metadata->'preview_rows'->0->>0)
FROM dataset_transactions WHERE id=$1`, first.Transaction.ID).Scan(&previewSize))
	require.Equal(t, 5<<20, previewSize)
	require.Equal(t, before.currentVersion+1, s.currentVersion(t))

	replay := s.commit(t, body)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	require.Equal(t, rec.Body.String(), replay.Body.String())
	wrongActorReplay := s.do(t, http.MethodPost, path+"/outputs:commit", "application/json", strings.NewReader(body), otherToken)
	require.Equal(t, http.StatusBadRequest, wrongActorReplay.Code, wrongActorReplay.Body.String())
	receiptPath := path + "/outputs/commits/" + first.IdempotencyKey
	receipt := s.do(t, http.MethodGet, receiptPath, "", nil, s.token)
	require.Equal(t, http.StatusOK, receipt.Code, receipt.Body.String())
	require.Equal(t, rec.Body.String(), receipt.Body.String())
	wrongActorReceipt := s.do(t, http.MethodGet, receiptPath, "", nil, otherToken)
	require.Equal(t, http.StatusNotFound, wrongActorReceipt.Code, wrongActorReceipt.Body.String())
	require.Equal(t, before.currentVersion+1, s.currentVersion(t))
}
