//go:build integration

package repo_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// Publications wrote one logical path more than once. The head listing keeps the newest file of a
// path. The transaction filter names the files of the publication it is asked for, and the earlier
// file downloads.
func TestFilesTransactionFilterListsAnEarlierPublication(t *testing.T) {
	s := newScanStack(t)
	ctx := context.Background()
	// The files and branches routes name a dataset by its id, as a reader does.
	dataset := "/v1/datasets/" + s.dataset.ID.String()
	publish := func(base, key string, files ...[2]string) uuid.UUID {
		entries := make([]map[string]any, 0, len(files))
		for _, file := range files {
			entries = append(entries, map[string]any{"logical_path": file[0], "operation": "ADD",
				"content_type":   "application/x-ndjson",
				"content_base64": base64.StdEncoding.EncodeToString([]byte(file[1])), "size_bytes": len(file[1])})
		}
		body, err := json.Marshal(map[string]any{"idempotency_key": key, "branch": "main", "files": entries})
		require.NoError(t, err)
		rec := s.do(t, http.MethodPost, base+"/outputs:commit", "application/json", bytes.NewReader(body), s.tokenA)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		var published models.CommitDatasetOutputResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &published))
		return published.Transaction.ID
	}
	list := func(base, query, token string) (*httptest.ResponseRecorder, []models.DatasetFile) {
		rec := s.do(t, http.MethodGet, base+"/files?"+query, "", nil, token)
		if rec.Code != http.StatusOK {
			return rec, nil
		}
		var listed models.ListDatasetFilesResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
		return rec, listed.Files
	}
	paths := func(files []models.DatasetFile) []string {
		out := []string{}
		for _, file := range files {
			out = append(out, file.LogicalPath)
		}
		return out
	}
	fileOf := func(files []models.DatasetFile, path string) uuid.UUID {
		for _, file := range files {
			if file.LogicalPath == path {
				return file.ID
			}
		}
		return uuid.Nil
	}
	downloadLocation := func(fileID uuid.UUID) (int, string) {
		rec := s.do(t, http.MethodGet, dataset+"/files/"+fileID.String()+"/download", "", nil, s.tokenA)
		return rec.Code, rec.Header().Get("Location")
	}
	moveHead := func(transactionID uuid.UUID) {
		_, err := s.pool.Exec(ctx, `UPDATE dataset_branches SET head_transaction_id = $1 WHERE dataset_id = $2 AND name = 'main'`,
			transactionID, s.dataset.ID)
		require.NoError(t, err)
	}

	first := publish(dataset, "wp28-files-first", [2]string{"part-00000.ndjson", "{\"n\":1}\n"})
	second := publish(dataset, "wp28-files-second", [2]string{"part-00000.ndjson", "{\"n\":2}\n{\"n\":3}\n"})

	rec, head := list(dataset, "", s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, head, 1, "the head listing keeps one file per logical path")
	newer := head[0]
	assert.Equal(t, second, newer.TransactionID)

	rec, earlier := list(dataset, "transaction_id="+first.String(), s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, earlier, 1)
	older := earlier[0]
	assert.Equal(t, first, older.TransactionID)
	assert.Equal(t, "part-00000.ndjson", older.LogicalPath)
	assert.Equal(t, "active", older.Status)
	assert.NotEqual(t, newer.ID, older.ID, "each publication has its own file")
	rec, latest := list(dataset, "branch=main&transaction_id="+second.String(), s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, latest, 1)
	assert.Equal(t, newer.ID, latest[0].ID)

	status, location := downloadLocation(older.ID)
	require.Equal(t, http.StatusFound, status)
	assert.Contains(t, location, first.String(), "the earlier file downloads from its own transaction")
	status, location = downloadLocation(newer.ID)
	require.Equal(t, http.StatusFound, status)
	assert.Contains(t, location, second.String())

	// A branch created from the second publication reads the files it inherited from that commit.
	// The commit is not its own: the filter answers only a transaction committed on the branch it
	// is asked of, as rows:scan does. The branch is created through the RID route from that
	// transaction, and its head is read back before anything is inferred from the inheritance.
	rid := "/v1/datasets/" + s.rid
	rec = s.do(t, http.MethodPost, rid+"/branches", "application/json", strings.NewReader(
		`{"name":"side","parent_branch":"main","source":{"from_transaction_rid":"ri.foundry.main.transaction.`+
			second.String()+`"}}`), s.tokenA)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	rec = s.do(t, http.MethodGet, rid+"/branches/side", "", nil, s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var side struct {
		HeadTransactionID string `json:"head_transaction_id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &side))
	assert.Equal(t, second.String(), side.HeadTransactionID, "side was created from the second publication")
	rec, inherited := list(dataset, "branch=side", s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, []uuid.UUID{newer.ID}, []uuid.UUID{fileOf(inherited, "part-00000.ndjson")},
		"side reads the file of the commit it was created from")
	for name, query := range map[string]string{
		"the commit side was created from": "branch=side&transaction_id=" + second.String(),
		"an earlier commit of its parent":  "branch=side&transaction_id=" + first.String(),
	} {
		rec, _ = list(dataset, query, s.tokenA)
		assert.Equal(t, http.StatusNotFound, rec.Code, "%s: %s", name, rec.Body.String())
	}

	// The window of a branch ends at its head. With the head moved back to the first publication,
	// the second is still committed on the branch but after its head: neither listing reaches it.
	moveHead(first)
	rec, _ = list(dataset, "transaction_id="+second.String(), s.tokenA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "a commit after the head of its branch: %s", rec.Body.String())
	rec, head = list(dataset, "", s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, older.ID, fileOf(head, "part-00000.ndjson"), "the head listing ends at the head as well")
	rec, within := list(dataset, "transaction_id="+first.String(), s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, []uuid.UUID{older.ID}, []uuid.UUID{fileOf(within, "part-00000.ndjson")})
	moveHead(second)
	rec, _ = list(dataset, "transaction_id="+second.String(), s.tokenA)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// The prefix is text. An underscore or a percent sign in it matches that character only.
	third := publish(dataset, "wp28-files-third", [2]string{"late/part_1.ndjson", "{\"n\":4}\n"},
		[2]string{"late/partX1.ndjson", "{\"n\":5}\n"}, [2]string{"late/100%.ndjson", "{\"n\":6}\n"},
		[2]string{"late/1000.ndjson", "{\"n\":7}\n"})
	for prefix, want := range map[string][]string{
		"late/part_": {"late/part_1.ndjson"}, // not late/partX1.ndjson
		"late/100%":  {"late/100%.ndjson"},   // not late/1000.ndjson
		"late/":      {"late/100%.ndjson", "late/1000.ndjson", "late/partX1.ndjson", "late/part_1.ndjson"},
	} {
		rec, files := list(dataset, "transaction_id="+third.String()+"&prefix="+url.QueryEscape(prefix), s.tokenA)
		require.Equal(t, http.StatusOK, rec.Code, prefix)
		assert.Equal(t, want, paths(files), prefix)
	}

	// The filter never falls back to the head.
	rec, _ = list(dataset, "transaction_id="+uuid.NewString(), s.tokenA)
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "transaction not found")
	for name, query := range map[string]string{
		"not a UUID": "transaction_id=not-a-uuid",
		"empty":      "transaction_id=",
		"repeated":   "transaction_id=" + first.String() + "&transaction_id=" + second.String(),
	} {
		rec, _ = list(dataset, query, s.tokenA)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", name, rec.Body.String())
	}
	// A query that names the filter is read strictly: a value with an invalid escape is refused,
	// not dropped as a request without the filter.
	for _, query := range []string{"transaction_id=%zz", "transaction_id=" + first.String() + "&prefix=late/100%",
		"transaction%5Fid=%zz", "transaction_id=" + first.String() + "&transaction_id=%zz"} {
		req := httptest.NewRequest(http.MethodGet, dataset+"/files", nil)
		req.URL.RawQuery = query
		req.Header.Set("Authorization", "Bearer "+s.tokenA)
		rec = httptest.NewRecorder()
		s.srv.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", query, rec.Body.String())
	}
	rec, encoded := list(dataset, "transaction%5Fid="+third.String(), s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Len(t, encoded, 4, "an encoded name with a value that decodes is the filter")
	// Without the filter a pair that does not parse is dropped, as it always was.
	req := httptest.NewRequest(http.MethodGet, dataset+"/files", nil)
	req.URL.RawQuery = "prefix=late/100%"
	req.Header.Set("Authorization", "Bearer "+s.tokenA)
	rec = httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	rec, _ = list(dataset, "transaction_id="+first.String(), s.tokenB)
	assert.Equal(t, http.StatusNotFound, rec.Code, "another tenant: %s", rec.Body.String())
	rec, _ = list(dataset, "branch=missing&transaction_id="+first.String(), s.tokenA)
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())

	// A branch holds one open transaction at a time, so each draft is aborted once it is read.
	transactions := dataset + "/branches/main/transactions"
	for _, state := range []string{"open", "aborted"} {
		rec = s.do(t, http.MethodPost, transactions, "application/json", strings.NewReader(`{"type":"SNAPSHOT"}`), s.tokenA)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		var draft struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &draft))
		if state == "aborted" {
			rec = s.do(t, http.MethodPost, transactions+"/"+draft.ID+":abort", "", nil, s.tokenA)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		}
		rec, _ = list(dataset, "transaction_id="+draft.ID, s.tokenA)
		assert.Equal(t, http.StatusNotFound, rec.Code, "%s transaction: %s", state, rec.Body.String())
		if state == "open" {
			rec = s.do(t, http.MethodPost, transactions+"/"+draft.ID+":abort", "", nil, s.tokenA)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		}
	}

	// A publication to another dataset of the same owner is not a transaction of this one.
	rec = s.do(t, http.MethodPost, "/v1/datasets", "application/json",
		strings.NewReader(`{"name":"wp28-files-other","format":"json"}`), s.tokenA)
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, rec.Code, rec.Body.String())
	var other struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &other))
	otherDataset, foreign := "/v1/datasets/"+other.ID, publish("/v1/datasets/"+other.ID, "wp28-files-other",
		[2]string{"part-00000.ndjson", "{\"n\":8}\n"})
	rec, _ = list(dataset, "transaction_id="+foreign.String(), s.tokenA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "a transaction of another dataset: %s", rec.Body.String())
	rec, own := list(otherDataset, "transaction_id="+foreign.String(), s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, []string{"part-00000.ndjson"}, paths(own))

	// A file that was deleted since, as retention leaves it, is neither listed nor downloaded.
	_, err := s.pool.Exec(ctx, `UPDATE dataset_files SET deleted_at = NOW() WHERE id = $1`, older.ID)
	require.NoError(t, err)
	rec, kept := list(dataset, "transaction_id="+first.String(), s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Empty(t, kept)
	status, _ = downloadLocation(older.ID)
	assert.Equal(t, http.StatusGone, status)
	rec, head = list(dataset, "", s.tokenA)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, newer.ID, fileOf(head, "part-00000.ndjson"))
}
