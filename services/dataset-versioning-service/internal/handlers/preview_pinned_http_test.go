package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
)

func previewRequest(hs *scanHarness, query string, subject uuid.UUID) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+hs.dataset.ID.String()+"/preview?"+query, nil)
	route := chi.NewRouteContext()
	route.URLParams.Add("rid", hs.dataset.ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), &authmw.Claims{Sub: subject}))
	rec := httptest.NewRecorder()
	hs.h.PreviewDataset(rec, req)
	return rec
}

func TestPreviewPinnedNDJSONRemainsOnSnapshot(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersion(t, "snapshot.ndjson", []byte("{\"order_id\":9007199254740993,\"source\":\"previous-first\"}\n{\"source\":\"previous-second\"}\n"))
	txnID := hs.store.versions[hs.dataset.ID][0].TransactionID.String()
	base := fmt.Sprintf("branch=main&version=%d&limit=1", version)
	rec := previewRequest(hs, base, hs.ownerID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var page struct {
		DatasetID     string                  `json:"dataset_id"`
		Branch        string                  `json:"branch"`
		Version       int32                   `json:"version"`
		TransactionID string                  `json:"transaction_id"`
		Format        string                  `json:"format"`
		Rows          []json.RawMessage       `json:"rows"`
		Columns       []struct{ Name string } `json:"columns"`
		NextCursor    *string                 `json:"next_cursor"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&page))
	require.Equal(t, hs.dataset.ID.String(), page.DatasetID)
	require.Equal(t, version, page.Version)
	require.Equal(t, "main", page.Branch)
	require.Equal(t, txnID, page.TransactionID)
	require.Equal(t, "ndjson", page.Format)
	require.Len(t, page.Columns, 2)
	require.Equal(t, "order_id", page.Columns[0].Name)
	require.Len(t, page.Rows, 1)
	require.Contains(t, string(page.Rows[0]), `"order_id":9007199254740993`)
	require.NotNil(t, page.NextCursor)

	nextVersion := hs.commitSnapshotVersion(t, "current.ndjson", []byte("{\"source\":\"new-head\"}\n"))
	cursorQuery := "&cursor=" + url.QueryEscape(*page.NextCursor)
	rec = previewRequest(hs, base+cursorQuery, hs.ownerID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&page))
	require.Equal(t, version, page.Version)
	require.Equal(t, txnID, page.TransactionID)
	require.Len(t, page.Rows, 1)
	require.Contains(t, string(page.Rows[0]), `"source":"previous-second"`)
	require.Nil(t, page.NextCursor)

	rec = previewRequest(hs, fmt.Sprintf("branch=main&version=%d&limit=1%s", nextVersion, cursorQuery), hs.ownerID)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	rec = previewRequest(hs, base+cursorQuery, uuid.New())
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}

func TestPreviewPinnedRejectsAmbiguousOptions(t *testing.T) {
	hs := newScanHarness(t)
	hs.commitSnapshotVersion(t, "snapshot.ndjson", []byte("{\"n\":1}\n"))
	for _, query := range []string{
		"version=1",
		"branch=main&cursor=opaque",
		"branch=main&version=%22not-a-number%22",
		"branch=main&version=1&version=2",
		"branch=main&branch=other&version=1",
		"branch=main&version=1&offset=0",
		"branch=main&version=1&format=csv",
		"branch=main&version=1&limit=0",
		"branch=main&version=1&cursor=",
		"branch=main&version=1&cursor=%ZZ",
	} {
		t.Run(query, func(t *testing.T) {
			rec := previewRequest(hs, query, hs.ownerID)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}
