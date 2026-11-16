package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDatasetRowsReaderConsumesExcelImportShape pins the contract between the
// P3.1 Excel import and pipeline dataset_input nodes: the rows endpoint's
// response for an imported workbook (real bytes captured from the isolated
// P3.1 E2E run) must decode through DatasetServiceRowReader with columns,
// rows, and the strict (non-truncated) execution semantics intact.
func TestDatasetRowsReaderConsumesExcelImportShape(t *testing.T) {
	const e2eBody = `{"dataset_id":"11111111-1111-1111-1111-111111111111","path":"orders.ndjson","format":"ndjson","source":"file_index","columns":["code","desc","order_no","qty"],"rows":[{"code":"123456789012345678901234567890","desc":"富文本描述内容","order_no":"007","qty":"3"},{"code":"987654321098765432109876543210","desc":null,"order_no":"008","qty":"4"}],"truncated":false,"size_bytes":181}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/datasets/11111111-1111-1111-1111-111111111111/files/rows" {
			if got := r.URL.Query().Get("mode"); got != "execution" {
				t.Errorf("execution read must request mode=execution, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(e2eBody))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	reader := DatasetServiceRowReader{BaseURL: srv.URL, Client: srv.Client()}
	out, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{
		DatasetRID: "11111111-1111-1111-1111-111111111111",
		Limit:      1000,
	})
	if err != nil {
		t.Fatalf("ReadDatasetRows: %v", err)
	}
	if len(out.Columns) != 4 || out.Columns[0] != "code" || out.Columns[3] != "qty" {
		t.Fatalf("columns: %v", out.Columns)
	}
	if len(out.Rows) != 2 {
		t.Fatalf("rows: %d", len(out.Rows))
	}
	if string(out.Rows[0]["order_no"]) != `"007"` {
		t.Fatalf("leading-zero code must stay a string: %s", out.Rows[0]["order_no"])
	}
	if string(out.Rows[1]["desc"]) != "null" {
		t.Fatalf("blank cell must surface as null: %s", out.Rows[1]["desc"])
	}
	if out.Truncated {
		t.Fatal("execution reads must not report truncation for in-limit imports")
	}
}
