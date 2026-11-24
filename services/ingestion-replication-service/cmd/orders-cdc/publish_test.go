package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPublishPreservesWideCoordinates(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(token, []byte("fixture"), 0600))
	dataset := uuid.New()
	var delivered struct {
		Key      string         `json:"idempotency_key"`
		Metadata map[string]any `json:"metadata"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil || json.Unmarshal(data, &delivered) != nil {
			w.WriteHeader(400)
			return
		}
		reply := map[string]any{"dataset_id": dataset, "idempotency_key": delivered.Key, "branch": "main",
			"request_fingerprint": digest(data), "transaction": map[string]string{"id": uuid.NewString(), "status": "COMMITTED"}}
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	c := change{Op: "u"}
	c.Source.LSN = 9007199254740995
	worker := worker{dataset: dataset, topic: "p.sales.orders", endpoint: server.URL, tokenFile: token, client: server.Client()}
	require.NoError(t, worker.publish(context.Background(), "tx:42:1", "0007", c, 9007199254740993, []byte{}))
	require.Equal(t, "9007199254740993", delivered.Metadata["offset"])
	require.Equal(t, "9007199254740995", delivered.Metadata["lsn"])
}
