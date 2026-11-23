package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestLLMBuildPreservesAllSupportedRows(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(w, http.StatusOK, map[string]string{"reply": "value"})
	}))
	defer server.Close()
	llm := NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: server.URL})
	for _, tc := range []struct {
		name    string
		input   int
		want    int
		preview bool
	}{
		{"standalone", 0, 1, false},
		{"above preview cap", 6, 6, false},
		{"preview", 6, 2, true},
		{"full supported build", 50, 50, false},
		{"over build limit", 51, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]map[string]json.RawMessage, tc.input)
			for i := range rows {
				rows[i] = map[string]json.RawMessage{"id": mustRuntimeJSON(i)}
			}
			before := calls.Load()
			req := LLMTransformRequest{InputRows: rows, Payload: json.RawMessage(`{"prompt":"describe","max_rows":2}`), Preview: tc.preview}
			req.Node.BuildBranch = "preview" // a build can use any branch name
			result, err := llm.RunLLMTransform(t.Context(), req)
			if tc.input > 50 {
				require.ErrorContains(t, err, "llm_row_limit_exceeded")
				require.Empty(t, result.Metadata)
			} else {
				require.NoError(t, err)
				got := rowsFromLLMResult(result)
				require.Len(t, got, tc.want)
				if tc.input > 0 {
					require.Equal(t, rows[tc.want-1]["id"], got[tc.want-1]["id"])
				}
			}
			require.EqualValues(t, tc.want, calls.Load()-before)
		})
	}
}

func TestLLMPreviewLimitOnlyAppliesToPreviewEntryPoint(t *testing.T) {
	nodes := []models.PipelineNode{
		{ID: "input", TransformType: "dataset_input", Config: json.RawMessage(`{"rows":[{"id":"A"},{"id":"B"}]}`)},
		{ID: "model", TransformType: "llm", DependsOn: []string{"input"}, Config: json.RawMessage(`{"prompt":"score","max_rows":1,"output_type":"double"}`)},
		{ID: "out", TransformType: "output_dataset", DependsOn: []string{"model"}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"reply": "4"})
	}))
	defer server.Close()
	ports := ExecutionPorts{LLM: NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: server.URL})}
	preview, err := executeLocalPipelinePreviewWithPorts(t.Context(), uuid.New(), "model", nodes, 3, ports)
	require.NoError(t, err)
	require.Len(t, preview.Rows, 1)
	require.JSONEq(t, "4", string(preview.Rows[0]["llm_output"]))

	body := buildLLMOutput(t, nodes, ports, nil)
	require.Equal(t, "DOUBLE", body.Schema.Fields[1].Type)
	require.Equal(t, "{\"id\":\"A\",\"llm_output\":4}\n{\"id\":\"B\",\"llm_output\":4}\n", string(body.Files[0].ContentBase64))
}

func TestLLMEmptyPreviewAndOutputRetainSchema(t *testing.T) {
	raw := json.RawMessage(`{"prompt":"score","output_column":"score","output_type":"double"}`)
	nodes := []models.PipelineNode{
		{ID: "input", TransformType: "dataset_input", Config: json.RawMessage(`{"rows":[{"id":1}]}`)},
		{ID: "empty", TransformType: "filter", DependsOn: []string{"input"}, Config: json.RawMessage(`{"predicate":"id = 9"}`)},
		{ID: "model", TransformType: "llm", DependsOn: []string{"empty"}, Config: raw},
		{ID: "pick", TransformType: "select", DependsOn: []string{"model"}, Config: json.RawMessage(`{"columns":["score","id"]}`)},
		{ID: "out", TransformType: "output_dataset", DependsOn: []string{"pick"}},
	}
	ir := models.NewPipelineIRFromNodes(nodes)
	ir.Nodes[0].OutputSchema = &models.PipelineIRSchema{Fields: []models.PipelineIRField{{Name: "id", FieldType: "LONG"}}}
	ir.Nodes[2].OutputSchema = &models.PipelineIRSchema{Fields: []models.PipelineIRField{{Name: "score", FieldType: "DOUBLE"}}}
	report, schemas := validatePipelineIRStrictWithSchemas("empty", ir)
	require.Empty(t, report.Errors)
	require.Equal(t, []string{"score", "id"}, schemas["out"].columnNames())

	ports := ExecutionPorts{LLM: NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: "http://unused.invalid"})}
	declarations := declaredTableSchemas(ir)
	for _, target := range []string{"model", "pick", "out"} {
		preview, err := executeLocalPreviewWithSchemas(t.Context(), uuid.New(), target, nodes, 3, ports, declarations)
		require.NoError(t, err)
		require.Empty(t, preview.Rows)
		require.Equal(t, []string{"id", "score"}, preview.Columns, target)
	}
	body := buildLLMOutput(t, nodes, ports, declarations)
	require.Empty(t, body.Files[0].ContentBase64)
	require.Equal(t, []datasetOutputField{{Name: "id", Type: "LONG"}, {Name: "score", Type: "DOUBLE"}}, body.Schema.Fields)
}

func buildLLMOutput(t *testing.T, nodes []models.PipelineNode, ports ExecutionPorts, declarations map[string]*models.PipelineIRSchema) datasetOutputCommitRequest {
	t.Helper()
	runner := newRuntimeNodeRunner(ports)
	var result executor.NodeResult
	for _, n := range nodes {
		node := executor.Node{ID: n.ID, DependsOn: n.DependsOn, Metadata: map[string]any{"transform_type": n.TransformType, "logic_payload": n.Config, runtimeDeclaredSchema: declarations[n.ID]}}
		var err error
		result, err = runner.Run(t.Context(), executor.NodeContext{Node: node})
		require.NoError(t, err)
	}
	body, err := datasetOutputCommitBody(executor.OutputTransaction{FileFormat: "NDJSON"}, result)
	require.NoError(t, err)
	return body
}

func TestLLMOutputTypesAndOverwrite(t *testing.T) {
	for _, tc := range []struct{ kind, reply, want string }{
		{"string", "true", `"true"`}, {"json", `{"a":[1]}`, `"{\"a\":[1]}"`},
		{"double", "3", `3`}, {"boolean", "true", `true`}, {"json", "oops", ""}, {"double", "NaN", ""}, {"boolean", "maybe", ""},
	} {
		cfg := llmNodeConfig{OutputType: tc.kind}
		value, err := cfg.outputValue(tc.reply)
		if tc.want == "" {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.JSONEq(t, tc.want, string(value))
	}
	base := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "value", FieldType: "STRING"}}}
	node := models.PipelineIRNode{ID: "model", TransformType: "llm", Config: json.RawMessage(`{"prompt":"classify","output_column":"value","output_type":"boolean"}`)}
	report := pipelineStrictValidationReport{}
	schema := validateStrictNode(node, []pipelineStrictSchema{base}, &report)
	require.Empty(t, report.Errors)
	require.Len(t, schema.Fields, 1)
	require.Equal(t, "BOOLEAN", schema.Fields[0].FieldType)
	require.Equal(t, "STRING", base.Fields[0].FieldType)
}

func TestComposedLLMPreview(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(w, http.StatusOK, map[string]string{"reply": "translated"})
	}))
	defer server.Close()
	ports := ExecutionPorts{LLM: NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: server.URL})}
	join := models.PipelineNode{ID: "join", TransformType: "join", DependsOn: []string{"model", "right"}, Config: json.RawMessage(`{"_join":{"join_type":"inner","matches":[{"left_column":"id","right_column":"target"}],"auto_select_left":true}}`)}
	sort := models.PipelineNode{ID: "sort", TransformType: "sql", DependsOn: []string{"model"}, Config: json.RawMessage(`{"_stack":{"blocks":[{"kind":"sort","applied":true,"sort_keys":[{"column":"id","direction":"desc"}]}]}}`)}
	project := models.PipelineNode{ID: "out", TransformType: "output_dataset", DependsOn: []string{"model"}, Config: json.RawMessage(`{"_output":{"columns":["id"]}}`)}
	for _, tc := range []struct {
		name                                                   string
		target                                                 models.PipelineNode
		input, sample, budget, wantCalls, wantRows, wantLastID int
		wantErr                                                string
	}{
		{"sixth row joins", join, 6, 1, 0, 6, 1, 6, ""},
		{"sixth row sorts first", sort, 6, 1, 6, 6, 1, 6, ""},
		{"join exceeds explicit budget", join, 6, 1, 5, 0, 0, 0, "llm_preview_budget_exceeded"},
		{"sort exceeds hard limit", sort, 51, 1, 0, 0, 0, 0, "llm_preview_budget_exceeded"},
		{"project six", project, 6, 6, 0, 6, 6, 6, ""},
		{"project one", project, 6, 1, 6, 1, 1, 1, ""},
		{"project budget", project, 6, 6, 2, 2, 2, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]map[string]int, tc.input)
			for i := range rows {
				rows[i] = map[string]int{"id": i + 1}
			}
			nodes := []models.PipelineNode{
				{ID: "source", TransformType: "dataset_input", Config: mustRuntimeJSON(map[string]any{"rows": rows})},
				{ID: "right", TransformType: "dataset_input", Config: json.RawMessage(`{"rows":[{"target":6}]}`)},
				{ID: "model", TransformType: "llm", DependsOn: []string{"source"}, Config: mustRuntimeJSON(map[string]any{"prompt": "translate", "max_rows": tc.budget})},
				tc.target,
			}
			before := calls.Load()
			preview, err := executeLocalPipelinePreviewWithPorts(t.Context(), uuid.New(), tc.target.ID, nodes, tc.sample, ports)
			require.EqualValues(t, tc.wantCalls, calls.Load()-before)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, preview.Rows, tc.wantRows)
			require.Equal(t, mustRuntimeJSON(tc.wantLastID), preview.Rows[len(preview.Rows)-1]["id"])
		})
	}
}

func TestLLMAfterJoinUsesDisplayBudget(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(w, http.StatusOK, map[string]string{"reply": "ok"})
	}))
	defer server.Close()
	nodes := []models.PipelineNode{
		{ID: "left", TransformType: "input", Config: json.RawMessage(`{"rows":[{"id":1},{"id":2},{"id":3}]}`)},
		{ID: "right", TransformType: "input", Config: json.RawMessage(`{"rows":[{"other":4}]}`)},
		{ID: "join", TransformType: "join", DependsOn: []string{"left", "right"}, Config: json.RawMessage(`{"_join":{"join_type":"cross","auto_select_left":true}}`)},
		{ID: "model", TransformType: "llm", DependsOn: []string{"join"}, Config: json.RawMessage(`{"prompt":"classify","max_rows":2}`)},
	}
	ports := ExecutionPorts{LLM: NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: server.URL})}
	preview, err := executeLocalPipelinePreviewWithPorts(t.Context(), uuid.New(), "model", nodes, 1, ports)
	require.NoError(t, err)
	require.Len(t, preview.Rows, 1)
	require.EqualValues(t, 1, calls.Load())
}
