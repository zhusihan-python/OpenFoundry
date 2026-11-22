package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestPipelineTransformCatalogIncludesVersionedFunctions(t *testing.T) {
	registry := &fakePipelineFunctionRegistry{}
	registry.set([]PipelineFunctionDefinition{
		{
			ID:          "fn.trail_effort",
			Name:        "trail_effort",
			DisplayName: "Trail effort",
			Version:     "1.0.0",
			Runtime:     PipelineFunctionRuntimeExpression,
			Parameters: []PipelineFunctionParameter{
				{Name: "distance_miles", Type: "Double", Required: true},
				{Name: "elevation_gain_ft", Type: "Double", Required: true},
			},
			ResultType: "Double",
			Expression: "distance_miles + (elevation_gain_ft / 1000)",
		},
		{Name: "scalar_round", Expression: "round(1.2)"},
		{Name: "scalar_unknown", Expression: "abs(unknown_function(1))"},
		{Name: "scalar_bad_arity", Expression: "abs(1,2)"},
		{Name: "scalar_invalid_syntax", Expression: "abs("},
		{Name: "scalar_empty"},
		{
			ID:         "fn.parse_gpx",
			Name:       "parse_gpx",
			Version:    "0.2.0",
			Runtime:    PipelineFunctionRuntimePython,
			ResultType: "String",
			Source:     "def compute(gpx): return gpx",
		},
	})
	restore := SetExecutionPorts(ExecutionPorts{Functions: registry})
	defer restore()

	rr := httptest.NewRecorder()
	ListPipelineTransformCatalog(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/transforms/catalog", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	var response pipelineTransformCatalogResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))

	static := transformCatalogV1()
	require.Equal(t, static.SchemaVersion, response.SchemaVersion)
	require.Equal(t, static.Categories, response.Categories)
	require.Len(t, response.Transforms, len(static.Transforms)+3)
	require.JSONEq(t, string(mustRuntimeJSON(static.Transforms)), string(mustRuntimeJSON(response.Transforms[:len(static.Transforms)])))

	seen := map[string]pipelineTransformCatalogEntry{}
	for _, entry := range response.Transforms {
		seen[entry.ID] = entry
	}
	require.Contains(t, seen, "function.trail_effort@1.0.0")
	require.Contains(t, seen, "function.parse_gpx@0.2.0")
	require.Contains(t, seen, "function.scalar_round@0.1.0")
	require.NotContains(t, seen, "function.scalar_unknown@0.1.0")
	require.NotContains(t, seen, "function.scalar_bad_arity@0.1.0")
	require.NotContains(t, seen, "function.scalar_invalid_syntax@0.1.0")
	require.NotContains(t, seen, "function.scalar_empty@0.1.0")
	require.Equal(t, "function", seen["function.trail_effort@1.0.0"].TransformType)
	require.Equal(t, "available", seen["function.trail_effort@1.0.0"].ExecutionStatus)
	require.Equal(t, "requires_python_sidecar", seen["function.parse_gpx@0.2.0"].ExecutionStatus)
	require.Equal(t, "trail_effort", seen["function.trail_effort@1.0.0"].Function.Name)
	require.Equal(t, "1.0.0", seen["function.trail_effort@1.0.0"].Function.Version)
}

func TestCatalogFunctionDefaultRunsInPreviewAndBuild(t *testing.T) {
	registry := &fakePipelineFunctionRegistry{}
	pinned := trailEffortFunction("1.0.0", "distance_miles + (elevation_gain_ft / 1000)")
	python := PipelineFunctionDefinition{
		ID: "fn.python_effort", Name: "python_effort", Version: "1.0.0",
		Runtime: PipelineFunctionRuntimePython,
	}
	registry.set([]PipelineFunctionDefinition{pinned, python})
	restore := SetExecutionPorts(ExecutionPorts{Functions: registry})
	defer restore()

	catalogRR := httptest.NewRecorder()
	ListPipelineTransformCatalog(catalogRR, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/transforms/catalog", nil))
	require.Equal(t, http.StatusOK, catalogRR.Code)
	var catalog pipelineTransformCatalogResponse
	require.NoError(t, json.NewDecoder(catalogRR.Body).Decode(&catalog))
	entries := map[string]pipelineTransformCatalogEntry{}
	for _, entry := range catalog.Transforms {
		entries[entry.ID] = entry
	}
	expressionEntry := entries["function.trail_effort@1.0.0"]
	pythonEntry := entries["function.python_effort@1.0.0"]
	require.Equal(t, "1.0.0", expressionEntry.DefaultConfig["function_version"])
	require.Equal(t, false, expressionEntry.DefaultConfig["function_auto_upgrade"])
	require.Equal(t, "python", entries["python_transform"].TransformType)
	require.Equal(t, "function", pythonEntry.TransformType)
	require.Equal(t, "requires_python_sidecar", pythonEntry.ExecutionStatus)

	registry.set([]PipelineFunctionDefinition{
		pinned, trailEffortFunction("2.0.0", "distance_miles * 2"), python,
	})
	unresolved := map[string]any{
		"function_id": pinned.ID, "function_version": "3.0.0", "target_column": pinned.Name,
	}
	for _, tc := range []struct {
		name      string
		config    map[string]any
		functions PipelineFunctionRegistry
		wantError string
	}{
		{name: "catalog default selects pinned version", config: expressionEntry.DefaultConfig, functions: registry},
		{name: "missing registry", config: expressionEntry.DefaultConfig, wantError: "pipeline_function_registry_not_configured"},
		{name: "unresolved pin", config: unresolved, functions: registry, wantError: "pipeline_function_not_found"},
		{name: "Python UDF is declaration only", config: pythonEntry.DefaultConfig, functions: registry, wantError: "python_reusable_function_runtime_not_configured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ports := ExecutionPorts{Functions: tc.functions}
			restore := SetExecutionPorts(ports)
			defer restore()
			pipelineID := uuid.New()
			nodes := []models.PipelineNode{
				{ID: "input", TransformType: "dataset_input", Config: json.RawMessage(`{"rows":[{"distance_miles":5,"elevation_gain_ft":1000}]}`)},
				{ID: "effort", TransformType: expressionEntry.TransformType, DependsOn: []string{"input"}, Config: mustRuntimeJSON(tc.config)},
			}

			runner := newRuntimeNodeRunner(ports)
			var buildErr error
			for _, node := range nodes {
				_, buildErr = runner.Run(t.Context(), executor.NodeContext{
					BuildID: pipelineID, Node: executor.Node{
						ID: node.ID, DependsOn: node.DependsOn,
						Metadata: map[string]any{"transform_type": node.TransformType, "logic_payload": node.Config},
					},
				})
				if node.ID == "input" {
					require.NoError(t, buildErr)
				}
			}

			body := bytes.NewReader(mustRuntimeJSON(map[string]any{"nodes": nodes}))
			route := "/api/v1/pipelines/" + pipelineID.String() + "/nodes/effort/preview"
			request := requestWithPreviewURLParams(http.MethodPost, route, body, pipelineID.String(), "effort")
			previewRR := httptest.NewRecorder()
			PreviewPipelineNode(previewRR, request)

			if tc.wantError != "" {
				require.ErrorContains(t, buildErr, tc.wantError)
				require.Equal(t, http.StatusBadRequest, previewRR.Code)
				require.Contains(t, previewRR.Body.String(), tc.wantError)
				return
			}
			require.NoError(t, buildErr)
			require.Equal(t, http.StatusOK, previewRR.Code, previewRR.Body.String())
			var preview pipelineNodePreviewResponse
			require.NoError(t, json.NewDecoder(previewRR.Body).Decode(&preview))
			require.Len(t, preview.Rows, 1)
			require.JSONEq(t, `6`, string(preview.Rows[0]["trail_effort"]))
			require.Equal(t, rowsToMaps(runner.Table.snapshotRows("effort")), preview.Rows)
		})
	}
}

func TestReusableExpressionFunctionRunsAndAutoUpgradesVersion(t *testing.T) {
	registry := &fakePipelineFunctionRegistry{}
	registry.set([]PipelineFunctionDefinition{trailEffortFunction("1.0.0", "distance_miles + (elevation_gain_ft / 1000)")})
	rt := newLightweightTableRuntime(registry)
	buildID := uuid.New()
	ctx := context.Background()

	_, err := rt.Run(ctx, executor.NodeContext{BuildID: buildID, Node: executor.Node{ID: "input"}}, json.RawMessage(`{
		"rows":[{"trail_id":"mesa","distance_miles":5,"elevation_gain_ft":1000}]
	}`), "dataset_input")
	require.NoError(t, err)

	cfg := json.RawMessage(`{
		"function_name":"trail_effort",
		"function_auto_upgrade":true,
		"target_column":"effort_score"
	}`)
	_, err = rt.Run(ctx, executor.NodeContext{BuildID: buildID, Node: executor.Node{ID: "effort_v1", DependsOn: []string{"input"}}}, cfg, "function")
	require.NoError(t, err)
	rows := rt.snapshotRows("effort_v1")
	require.Len(t, rows, 1)
	require.JSONEq(t, `6`, string(rows[0]["effort_score"]))

	registry.set([]PipelineFunctionDefinition{
		trailEffortFunction("1.0.0", "distance_miles + (elevation_gain_ft / 1000)"),
		trailEffortFunction("2.0.0", "distance_miles * 2"),
	})
	_, err = rt.Run(ctx, executor.NodeContext{BuildID: buildID, Node: executor.Node{ID: "effort_v2", DependsOn: []string{"input"}}}, cfg, "function")
	require.NoError(t, err)
	rows = rt.snapshotRows("effort_v2")
	require.Len(t, rows, 1)
	require.JSONEq(t, `10`, string(rows[0]["effort_score"]))
}

func trailEffortFunction(version, expression string) PipelineFunctionDefinition {
	return PipelineFunctionDefinition{
		ID:      "fn.trail_effort",
		Name:    "trail_effort",
		Version: version,
		Runtime: PipelineFunctionRuntimeExpression,
		Parameters: []PipelineFunctionParameter{
			{Name: "distance_miles", Type: "Integer", Required: true},
			{Name: "elevation_gain_ft", Type: "Integer", Required: true},
		},
		ResultType:  "Integer",
		Expression:  expression,
		Description: "Estimated trail effort from distance and elevation gain.",
	}
}

type fakePipelineFunctionRegistry struct {
	mu   sync.Mutex
	defs []PipelineFunctionDefinition
	err  error
}

func (r *fakePipelineFunctionRegistry) set(defs []PipelineFunctionDefinition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defs = append([]PipelineFunctionDefinition(nil), defs...)
}

func (r *fakePipelineFunctionRegistry) ListPipelineFunctions(context.Context) ([]PipelineFunctionDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	return append([]PipelineFunctionDefinition(nil), r.defs...), nil
}

func (r *fakePipelineFunctionRegistry) ResolvePipelineFunction(_ context.Context, ref PipelineFunctionRef) (PipelineFunctionDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return PipelineFunctionDefinition{}, r.err
	}
	var latest *PipelineFunctionDefinition
	for i := range r.defs {
		def := normalisePipelineFunction(r.defs[i])
		if ref.ID != "" && def.ID != ref.ID {
			continue
		}
		if ref.Name != "" && def.Name != ref.Name {
			continue
		}
		if ref.Version != "" && def.Version != ref.Version {
			continue
		}
		if latest == nil || def.Version > latest.Version {
			copy := def
			latest = &copy
		}
	}
	if latest == nil {
		return PipelineFunctionDefinition{}, errors.New("pipeline_function_not_found")
	}
	return *latest, nil
}
