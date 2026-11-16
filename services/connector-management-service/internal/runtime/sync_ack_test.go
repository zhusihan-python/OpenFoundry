package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// ackServer is a scripted dataset-versioning outputs:commit responder.
func ackServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestCommitOutputUsableACKContract(t *testing.T) {
	target := uuid.New()
	txn := uuid.New()
	req := DatasetCommitRequest{DatasetID: target, Content: []byte("{}")}
	client := HTTPDatasetVersioningClient{}

	// Valid fixture: exact target echo + COMMITTED + txn id => usable ACK.
	ok := ackServer(t, fmt.Sprintf(`{"dataset_id":%q,"transaction":{"id":%q,"status":"COMMITTED"}}`, target, txn))
	client.BaseURL = ok.URL
	res, err := client.CommitOutput(context.Background(), req, "Bearer jwt")
	if err != nil {
		t.Fatalf("valid fixture must pass: %v", err)
	}
	if res.Status != "COMMITTED" || res.TransactionID != txn || res.DatasetID != target {
		t.Fatalf("usable ACK fields wrong: %+v", res)
	}

	// A 2xx with a non-COMMITTED status is NOT usable.
	pending := ackServer(t, fmt.Sprintf(`{"dataset_id":%q,"transaction":{"id":%q,"status":"PENDING"}}`, target, txn))
	client.BaseURL = pending.URL
	if _, err := client.CommitOutput(context.Background(), req, "Bearer jwt"); err == nil {
		t.Fatal("non-COMMITTED status must fail")
	}

	// A 2xx echoing a different dataset is NOT usable.
	mismatch := ackServer(t, fmt.Sprintf(`{"dataset_id":%q,"transaction":{"id":%q,"status":"COMMITTED"}}`, uuid.New(), txn))
	client.BaseURL = mismatch.URL
	if _, err := client.CommitOutput(context.Background(), req, "Bearer jwt"); err == nil {
		t.Fatal("mismatched dataset_id echo must fail")
	}

	// A 2xx without any transaction id is NOT usable (pre-existing rule).
	noTxn := ackServer(t, fmt.Sprintf(`{"dataset_id":%q,"transaction":{"id":"00000000-0000-0000-0000-000000000000","status":"COMMITTED"}}`, target))
	client.BaseURL = noTxn.URL
	if _, err := client.CommitOutput(context.Background(), req, "Bearer jwt"); err == nil {
		t.Fatal("nil transaction id must fail")
	}
}
