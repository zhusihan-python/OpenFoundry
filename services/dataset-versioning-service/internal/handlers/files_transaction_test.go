package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type fileCommits struct {
	store   *fakeStore
	handler *handlers.Handlers
	owner   uuid.UUID
	main    uuid.UUID
	// first, second and later are committed on main, in that order. second is its head, so later
	// lies after the head. open and aborted never committed, other is committed on side, a branch
	// created from main, and foreign belongs to another dataset.
	first, second, later, open, aborted, other, foreign uuid.UUID
	// firstFile and secondFile share one logical path. The rest was committed with first.
	firstFile, secondFile                                                             uuid.UUID
	lateFile, underscoreFile, alikeUnderscore, percentFile, alikePercent, expiredFile uuid.UUID
	foreignFile                                                                       uuid.UUID
}

func newFileCommits(t *testing.T) fileCommits {
	t.Helper()
	owner := uuid.New()
	store := newFakeStore(owner)
	dataset, otherDataset := store.datasets[0].ID, uuid.New()
	store.datasets = append(store.datasets, models.Dataset{ID: otherDataset, OwnerID: owner, Name: "other"})
	c := fileCommits{store: store, handler: &handlers.Handlers{Repo: store}, owner: owner, main: uuid.New(),
		first: uuid.New(), second: uuid.New(), later: uuid.New(), open: uuid.New(), aborted: uuid.New(),
		other: uuid.New(), foreign: uuid.New(),
		firstFile: uuid.New(), secondFile: uuid.New(), lateFile: uuid.New(), underscoreFile: uuid.New(),
		alikeUnderscore: uuid.New(), percentFile: uuid.New(), alikePercent: uuid.New(), expiredFile: uuid.New(),
		foreignFile: uuid.New()}
	createdFrom := c.second
	store.branches[dataset] = []models.DatasetBranch{
		{ID: c.main, DatasetID: dataset, Name: "main", HeadTransactionID: &c.second},
		{ID: c.other, DatasetID: dataset, Name: "side", ParentBranchID: &c.main, CreatedFromTransactionID: &createdFrom},
	}
	side, otherBranch := uuid.New(), uuid.New()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	transaction := func(id, datasetID, branchID uuid.UUID, status models.TransactionStatus, at time.Time) {
		txn := models.RuntimeTransaction{ID: id, DatasetID: datasetID, BranchID: branchID,
			Status: status, TxType: models.TransactionTypeSnapshot, StartedAt: at}
		if status == models.TransactionStatusCommitted {
			committedAt := at
			txn.CommittedAt = &committedAt
		}
		store.runtimeTransactions[id] = txn
	}
	transaction(c.first, dataset, c.main, models.TransactionStatusCommitted, base)
	transaction(c.second, dataset, c.main, models.TransactionStatusCommitted, base.Add(time.Minute))
	transaction(c.later, dataset, c.main, models.TransactionStatusCommitted, base.Add(3*time.Minute))
	transaction(c.open, dataset, c.main, models.TransactionStatusOpen, base.Add(4*time.Minute))
	transaction(c.aborted, dataset, c.main, models.TransactionStatusAborted, base.Add(4*time.Minute))
	transaction(c.other, dataset, side, models.TransactionStatusCommitted, base)
	transaction(c.foreign, otherDataset, otherBranch, models.TransactionStatusCommitted, base)
	file := func(id, transactionID uuid.UUID, path string, at time.Time) models.DatasetFile {
		return models.DatasetFile{ID: id, DatasetID: dataset, TransactionID: transactionID, LogicalPath: path,
			SizeBytes: 1, CreatedAt: at, ModifiedAt: at, Status: "active"}
	}
	deletedAt := base
	store.files[dataset] = []models.DatasetFile{file(c.secondFile, c.second, "part-00000.ndjson", base.Add(time.Minute)),
		file(c.firstFile, c.first, "part-00000.ndjson", base),
		file(c.lateFile, c.first, "late/part-00001.ndjson", base),
		file(c.underscoreFile, c.first, "late/part_1.ndjson", base),
		file(c.alikeUnderscore, c.first, "late/partX1.ndjson", base),
		file(c.percentFile, c.first, "late/100%.ndjson", base),
		file(c.alikePercent, c.first, "late/1000.ndjson", base)}
	expired := file(c.expiredFile, c.first, "expired.ndjson", base)
	expired.DeletedAt, expired.Status = &deletedAt, "deleted"
	store.files[dataset] = append(store.files[dataset], expired)
	foreign := file(c.foreignFile, c.foreign, "part-00000.ndjson", base)
	foreign.DatasetID = otherDataset
	store.files[otherDataset] = []models.DatasetFile{foreign}
	return c
}

// list reads the files listing. The caller is the owner unless it names another one.
func (c fileCommits) list(t *testing.T, query string, caller ...uuid.UUID) (*httptest.ResponseRecorder, *models.ListDatasetFilesResponse) {
	t.Helper()
	who := c.owner
	if len(caller) != 0 {
		who = caller[0]
	}
	req := datasetReq("GET", c.store, who, "")
	req.URL.RawQuery = query
	rec := httptest.NewRecorder()
	c.handler.ListFiles(rec, req)
	if rec.Code != http.StatusOK {
		return rec, nil
	}
	var listed models.ListDatasetFilesResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
	return rec, &listed
}

func paths(listed *models.ListDatasetFilesResponse) []string {
	out := []string{}
	for _, file := range listed.Files {
		out = append(out, file.LogicalPath)
	}
	return out
}

func ids(listed *models.ListDatasetFilesResponse) []uuid.UUID {
	out := []uuid.UUID{}
	for _, file := range listed.Files {
		out = append(out, file.ID)
	}
	return out
}

func TestListFilesWithoutATransactionKeepsTheHeadListing(t *testing.T) {
	c := newFileCommits(t)
	for _, query := range []string{"", "branch=main"} {
		rec, head := c.list(t, query)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "main", head.Branch)
		// One file per logical path, the newest of a path that was written twice, in path order.
		assert.Equal(t, []string{"expired.ndjson", "late/100%.ndjson", "late/1000.ndjson", "late/part-00001.ndjson",
			"late/partX1.ndjson", "late/part_1.ndjson", "part-00000.ndjson"}, paths(head))
		assert.Equal(t, []uuid.UUID{c.expiredFile, c.percentFile, c.alikePercent, c.lateFile, c.alikeUnderscore,
			c.underscoreFile, c.secondFile}, ids(head))
	}
	// The store matches the prefix of the head listing in its own way, so only plain characters are
	// asserted here. An underscore and a percent sign are asserted with the transaction filter,
	// which takes its prefix as text.
	for prefix, want := range map[string][]uuid.UUID{"late/part-": {c.lateFile}, "late/10": {c.percentFile, c.alikePercent}} {
		rec, narrowed := c.list(t, "prefix="+prefix)
		require.Equal(t, http.StatusOK, rec.Code, prefix)
		assert.Equal(t, want, ids(narrowed), prefix)
	}
}

func TestListFilesNamesTheFilesOfAHistoricalTransactionAndOfTheHead(t *testing.T) {
	c := newFileCommits(t)
	_, head := c.list(t, "")
	rec, earlier := c.list(t, "transaction_id="+c.first.String())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "main", earlier.Branch)
	// The first publication of the path, which the head listing replaced, and one file of it that
	// was deleted since is left out.
	assert.Equal(t, 6, earlier.Total)
	assert.ElementsMatch(t, []uuid.UUID{c.firstFile, c.lateFile, c.underscoreFile, c.alikeUnderscore,
		c.percentFile, c.alikePercent}, ids(earlier))
	for _, file := range earlier.Files {
		assert.Equal(t, c.first, file.TransactionID)
		assert.Equal(t, "active", file.Status)
	}
	assert.NotContains(t, ids(head), c.firstFile, "the head listing no longer holds the earlier file of the path")
	rec, latest := c.list(t, "branch=main&transaction_id="+c.second.String())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, []uuid.UUID{c.secondFile}, ids(latest))
	assert.Contains(t, ids(head), c.secondFile, "the head transaction holds the file the head listing shows")
}

// Only the transaction filter takes its prefix as text. The head listing keeps the matching it
// always had, which this file does not assert.
func TestListFilesTransactionFilterTakesThePrefixAsText(t *testing.T) {
	c := newFileCommits(t)
	for prefix, want := range map[string][]uuid.UUID{
		"late/part_": {c.underscoreFile}, // not late/partX1.ndjson
		"late/100%":  {c.percentFile},    // not late/1000.ndjson
		"late/part-": {c.lateFile},
		"late/part":  {c.lateFile, c.underscoreFile, c.alikeUnderscore},
		"late/":      {c.lateFile, c.underscoreFile, c.alikeUnderscore, c.percentFile, c.alikePercent},
	} {
		rec, listed := c.list(t, "transaction_id="+c.first.String()+"&prefix="+url.QueryEscape(prefix))
		require.Equal(t, http.StatusOK, rec.Code, prefix)
		assert.ElementsMatch(t, want, ids(listed), prefix)
	}
}

func TestListFilesRefusesAMalformedTransactionFilter(t *testing.T) {
	c := newFileCommits(t)
	for _, refused := range [][2]string{
		{"transaction_id=not-a-uuid", "transaction_id must be a UUID"},
		{"transaction_id=", "transaction_id must be a UUID"},
		// A value with an invalid escape is refused, not dropped as a request without the filter.
		{"transaction_id=%zz", "invalid query encoding"},
		{"transaction_id=" + c.first.String() + "&prefix=late/100%", "invalid query encoding"},
		{"transaction_id=" + c.first.String() + "&transaction_id=" + c.second.String(), "at most once"},
		// The name of the filter may be encoded. URL.Query() drops such a pair whole when its
		// value does not decode; it must not pass as a request without the filter.
		{"transaction%5Fid=%zz", "invalid query encoding"},
		{"transaction_id=" + c.first.String() + "&transaction_id=%zz", "invalid query encoding"},
	} {
		req := datasetReq("GET", c.store, c.owner, "")
		req.URL.RawQuery = refused[0]
		rec := httptest.NewRecorder()
		c.handler.ListFiles(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code, refused[0])
		assert.Contains(t, rec.Body.String(), refused[1], refused[0])
	}
	rec, named := c.list(t, "transaction%5Fid="+c.first.String())
	require.Equal(t, http.StatusOK, rec.Code, "an encoded name with a value that decodes is the filter: %s", rec.Body.String())
	assert.Equal(t, 6, named.Total)
	// Without the filter the request is read as before: a pair that does not parse is dropped
	// before the store sees it. This is how the request is read, not how a prefix is matched.
	rec, head := c.list(t, "prefix=late/100%")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Len(t, head.Files, 7)
}

func TestListFilesTransactionFilterNeverFallsBackToTheHead(t *testing.T) {
	c := newFileCommits(t)
	for name, query := range map[string]string{
		"open transaction":                "transaction_id=" + c.open.String(),
		"aborted transaction":             "transaction_id=" + c.aborted.String(),
		"unknown transaction":             "transaction_id=" + uuid.NewString(),
		"committed after the branch head": "transaction_id=" + c.later.String(),
		"committed on another branch":     "branch=main&transaction_id=" + c.other.String(),
		// side was created from main and reads the file of second. The commit is not its own.
		"a commit of the parent branch":   "branch=side&transaction_id=" + c.second.String(),
		"committed, but not on side":      "branch=side&transaction_id=" + c.first.String(),
		"unknown branch":                  "branch=missing&transaction_id=" + c.first.String(),
		"another dataset":                 "transaction_id=" + c.foreign.String(),
		"a dataset id, not a transaction": "transaction_id=" + c.store.datasets[1].ID.String(),
	} {
		req := datasetReq("GET", c.store, c.owner, "")
		req.URL.RawQuery = query
		rec := httptest.NewRecorder()
		c.handler.ListFiles(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code, "%s: %s", name, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "transaction not found", name)
	}
}

func TestListFilesTransactionFilterFollowsTheHeadOfTheBranch(t *testing.T) {
	c := newFileCommits(t)
	dataset := c.store.datasets[0].ID
	// main is the first branch of the fixture.
	moved := c.first
	branches := c.store.branches[dataset]
	branches[0].HeadTransactionID = &moved
	c.store.branches[dataset] = branches
	// The head moved back to the first publication. The second is still committed on the branch,
	// but after its head, so the filter does not reach it any more.
	rec, _ := c.list(t, "transaction_id="+c.second.String())
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	rec, first := c.list(t, "transaction_id="+c.first.String())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, ids(first), c.firstFile)
}

func TestListFilesTransactionFilterKeepsTheTenantBoundary(t *testing.T) {
	c := newFileCommits(t)
	// The dataset of another owner is not found, so neither is a transaction of it.
	rec, _ := c.list(t, "transaction_id="+c.first.String(), uuid.New())
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}
