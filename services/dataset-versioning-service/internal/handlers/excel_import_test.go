package handlers

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/xlsximp"
)

// The NDJSON output caps must equal the dataset rows reader limits exactly,
// otherwise an import could register a file its own reader rejects.
func TestExcelLimitsMatchRowsReader(t *testing.T) {
	if xlsximp.MaxNDJSONTotalBytes != datasetFileRowsMaxBytes {
		t.Fatalf("total cap %d != rows reader %d", xlsximp.MaxNDJSONTotalBytes, datasetFileRowsMaxBytes)
	}
	if xlsximp.MaxNDJSONLineBytes != datasetFileRowsMaxLineBytes {
		t.Fatalf("line cap %d != rows reader %d", xlsximp.MaxNDJSONLineBytes, datasetFileRowsMaxLineBytes)
	}
}

func TestExcelErrStatusMapping(t *testing.T) {
	cases := map[string]int{
		xlsximp.ErrOversized:   413,
		xlsximp.ErrFormula:     422,
		xlsximp.ErrRowLimit:    422,
		xlsximp.ErrOutputLimit: 422,
		xlsximp.ErrMacro:       422,
		xlsximp.ErrMapping:     400,
		xlsximp.ErrHeader:      400,
	}
	for kind, want := range cases {
		if got := excelErrStatus(kind); got != want {
			t.Fatalf("excelErrStatus(%s) = %d, want %d", kind, got, want)
		}
	}
}

func multipartConfigRequest(t *testing.T, config string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if config != "" {
		if err := mw.WriteField("config", config); err != nil {
			t.Fatalf("write config: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}
	req := httptest.NewRequest("POST", "/v1/datasets/imports/excel/preview", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if err := req.ParseMultipartForm(1 << 20); err != nil {
		t.Fatalf("parse multipart: %v", err)
	}
	return req
}

func TestDecodeExcelConfig(t *testing.T) {
	w := httptest.NewRecorder()
	cfg, ok := decodeExcelConfig(w, multipartConfigRequest(t, ""))
	if ok || w.Code != 400 {
		t.Fatalf("missing config must be rejected, ok=%v code=%d", ok, w.Code)
	}
	w = httptest.NewRecorder()
	cfg, ok = decodeExcelConfig(w, multipartConfigRequest(t, `{"sheet":"数据","header_row":3,"mapping":[{"source_index":0,"target":"a"}]}`))
	if !ok || cfg.Sheet != "数据" || cfg.HeaderRow == nil || *cfg.HeaderRow != 3 || len(cfg.Mapping) != 1 {
		t.Fatalf("valid config must parse: ok=%v cfg=%+v", ok, cfg)
	}
	w = httptest.NewRecorder()
	_, ok = decodeExcelConfig(w, multipartConfigRequest(t, `{not json`))
	if ok || w.Code != 400 || !strings.Contains(w.Body.String(), "excel_config_invalid") {
		t.Fatalf("malformed config must be rejected with 400, ok=%v code=%d body=%s", ok, w.Code, w.Body.String())
	}
}
