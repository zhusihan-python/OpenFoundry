package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

const (
	defaultPipelinePreviewSampleSize = 50
	maxPipelinePreviewSampleSize     = 500
)

type previewPipelineNodeRequest struct {
	DAG        json.RawMessage       `json:"dag,omitempty"`
	IR         *models.PipelineIR    `json:"ir,omitempty"`
	Nodes      []models.PipelineNode `json:"nodes,omitempty"`
	SampleSize int                   `json:"sample_size,omitempty"`
}

type pipelineNodePreviewResponse struct {
	PipelineID           string                       `json:"pipeline_id"`
	NodeID               string                       `json:"node_id"`
	Columns              []string                     `json:"columns"`
	Rows                 []map[string]json.RawMessage `json:"rows"`
	SampleSize           int                          `json:"sample_size"`
	GeneratedAt          time.Time                    `json:"generated_at"`
	Seed                 uint64                       `json:"seed"`
	SourceChain          []string                     `json:"source_chain"`
	Fresh                bool                         `json:"fresh"`
	Error                *pipelineNodePreviewError    `json:"error,omitempty"`
	DatasetFileFormat    string                       `json:"dataset_file_format,omitempty"`
	DatasetSchemaVersion string                       `json:"dataset_schema_version,omitempty"`
	DatasetSchema        *datasetRowsSchema           `json:"dataset_schema,omitempty"`
}

type pipelineNodePreviewError struct {
	Kind      string `json:"kind"`
	NodeID    string `json:"node_id,omitempty"`
	Transform string `json:"transform,omitempty"`
	Message   string `json:"message"`
}

type previewExecutionError struct {
	status int
	err    pipelineNodePreviewError
}

func (e previewExecutionError) Error() string { return e.err.Message }

// PreviewPipelineNode executes the selected node's upstream dependency chain
// in the local lightweight table runtime and returns a deterministic row
// sample. POST may carry an unsaved draft DAG from the editor; GET previews the
// persisted pipeline row.
func PreviewPipelineNode(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := pipelineIDFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_pipeline_id", "detail": err.Error()})
		return
	}
	nodeID, err := pipelineNodeIDFromRequest(r)
	if err != nil {
		writePipelinePreviewError(w, pipelineID, "", defaultPipelinePreviewSampleSize, nil, http.StatusBadRequest, pipelineNodePreviewError{Kind: "invalid_node_id", Message: err.Error()})
		return
	}
	body, err := readPreviewPipelineNodeRequest(r)
	if err != nil {
		writePipelinePreviewError(w, pipelineID, nodeID, defaultPipelinePreviewSampleSize, nil, http.StatusBadRequest, pipelineNodePreviewError{Kind: "invalid_json", NodeID: nodeID, Message: err.Error()})
		return
	}
	sampleSize := clampPipelinePreviewSampleSize(firstPositiveInt(body.SampleSize, queryInt(r, "sample_size")))
	ir, err := previewIRForRequest(r.Context(), pipelineID, body)
	if err != nil {
		var execErr previewExecutionError
		if errors.As(err, &execErr) {
			writePipelinePreviewError(w, pipelineID, nodeID, sampleSize, nil, execErr.status, execErr.err)
			return
		}
		writePipelinePreviewError(w, pipelineID, nodeID, sampleSize, nil, http.StatusBadRequest, pipelineNodePreviewError{Kind: "invalid_pipeline_graph", NodeID: nodeID, Message: err.Error()})
		return
	}
	// Dataset-backed input nodes read as the calling user, not as the
	// service account.
	previewCtx := contextWithCallerBearer(r.Context(), callerBearerFromRequest(r))
	nodes := ir.LegacyNodes()
	ports, _ := currentExecutionPorts()
	response, err := executeLocalPreviewWithSchemas(previewCtx, pipelineID, nodeID, nodes, sampleSize, ports, declaredTableSchemas(ir))
	if err != nil {
		var execErr previewExecutionError
		if errors.As(err, &execErr) {
			writePipelinePreviewError(w, pipelineID, nodeID, sampleSize, execErr.errChain(nodes, nodeID), execErr.status, execErr.err)
			return
		}
		writePipelinePreviewError(w, pipelineID, nodeID, sampleSize, nil, http.StatusBadRequest, pipelineNodePreviewError{Kind: "preview_failed", NodeID: nodeID, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func previewIRForRequest(ctx context.Context, pipelineID uuid.UUID, req previewPipelineNodeRequest) (models.PipelineIR, error) {
	if req.IR != nil {
		return req.IR.Normalize(), nil
	}
	if len(req.Nodes) > 0 {
		return models.NewPipelineIRFromNodes(req.Nodes), nil
	}
	if len(bytes.TrimSpace(req.DAG)) > 0 {
		return models.ParsePipelineIR(req.DAG)
	}
	repo, ok := currentPipelineAuthoringRepository()
	if !ok {
		return models.PipelineIR{}, previewExecutionError{status: http.StatusServiceUnavailable, err: pipelineNodePreviewError{Kind: "pipeline_authoring_repository_not_configured", Message: "PreviewPipelineNode requires a draft graph body or a configured pipeline authoring repository"}}
	}
	pipeline, err := repo.GetPipeline(ctx, pipelineID)
	if err != nil {
		return models.PipelineIR{}, err
	}
	if pipeline == nil {
		return models.PipelineIR{}, previewExecutionError{status: http.StatusNotFound, err: pipelineNodePreviewError{Kind: "pipeline_not_found", Message: "pipeline not found"}}
	}
	return pipeline.ParsedIR()
}

func executeLocalPipelinePreview(ctx context.Context, pipelineID uuid.UUID, nodeID string, nodes []models.PipelineNode, sampleSize int) (pipelineNodePreviewResponse, error) {
	ports, _ := currentExecutionPorts()
	return executeLocalPipelinePreviewWithPorts(ctx, pipelineID, nodeID, nodes, sampleSize, ports)
}

func executeLocalPipelinePreviewWithPorts(ctx context.Context, pipelineID uuid.UUID, nodeID string, nodes []models.PipelineNode, sampleSize int, ports ExecutionPorts) (pipelineNodePreviewResponse, error) {
	return executeLocalPreviewWithSchemas(ctx, pipelineID, nodeID, nodes, sampleSize, ports, nil)
}

func executeLocalPreviewWithSchemas(ctx context.Context, pipelineID uuid.UUID, nodeID string, nodes []models.PipelineNode, sampleSize int, ports ExecutionPorts, schemas map[string]*models.PipelineIRSchema) (pipelineNodePreviewResponse, error) {
	chain, err := upstreamExecutionChain(nodes, nodeID)
	if err != nil {
		return pipelineNodePreviewResponse{}, err
	}
	// A publication preview uses the same binding and row projection as a run.
	// Validate it before executing upstream nodes, including external calls.
	target := chain[len(chain)-1]
	_, nested := datasetOutputConfigFields(target.Config)
	var projection []executor.OutputTransaction
	if nested["columns"] != nil {
		projection, _, err = plannedNodeOutputs(pipelineID, pipelineID, target, nodes)
		if err != nil {
			return pipelineNodePreviewResponse{}, previewExecutionError{status: http.StatusBadRequest, err: pipelineNodePreviewError{Kind: "transform_failed", NodeID: nodeID, Transform: target.TransformType, Message: err.Error()}}
		}
	}
	runtime := newLightweightTableRuntime(ports.Functions)
	runtime.datasets = ports.DatasetRows
	// Windows, joins and unions can depend on rows beyond the display
	// sample. Use the bounded build inputs before limiting the final result.
	sampleInputs := len(projection) == 0 && !previewChainRequiresBuildInputs(chain)
	completeRows := completeRowsForLLMPreview(chain)
	runtime.joinExpansionLimit = maxLightweightExpandedRows
	runner := runtimeNodeRunner{Table: runtime, LLM: ports.LLM, Preview: true}
	var targetMetadata map[string]any
	for _, node := range chain {
		transform := strings.TrimSpace(node.TransformType)
		if !runtime.Supports(transform) && !isLLMTransform(transform) {
			return pipelineNodePreviewResponse{}, previewExecutionError{status: http.StatusBadRequest, err: pipelineNodePreviewError{Kind: "unsupported_transform", NodeID: node.ID, Transform: transform, Message: fmt.Sprintf("local preview does not support transform %q", transform)}}
		}
		// Persisted input nodes carry their dataset binding as node-level
		// InputDatasetIDs (the same metadata the executor attaches for
		// builds). Mirror it ONLY when the node pins a dataset snapshot:
		// the pin's resolution relies on the metadata candidate, while
		// unpinned previews must keep their exact legacy behavior
		// (synthetic rows for reference-less nodes included).
		metadata := map[string]any{
			"logic_kind":     "transform",
			"transform_type": transform,
			"logic_payload":  node.Config,
			"label":          node.Label,
		}
		if schema := schemas[node.ID]; schema != nil {
			metadata[runtimeDeclaredSchema] = schema
		}
		// Pinned snapshots are indivisible. Bound the model calls independently
		// of the dataset read, even for a one-row preview.
		sampleNode := sampleInputs && !completeRows[node.ID] && !(completeRows != nil && PayloadDeclaresDatasetSnapshot(node.Config))
		runtime.datasetRowsPreview = sampleNode
		if sampleNode {
			metadata["logic_payload"] = previewConfigWithSampleSize(node.Config, sampleSize)
			metadata["sample_size"] = sampleSize
		}
		if isLLMTransform(transform) {
			metadata["sample_size"] = sampleSize
			runner.CompletePreviewRows = completeRows[node.ID]
		}
		if PayloadDeclaresDatasetSnapshot(node.Config) {
			if ids := uuidStrings(node.InputDatasetIDs); len(ids) > 0 {
				metadata["input_dataset_ids"] = ids
			}
		}
		var outputs []executor.OutputTransaction
		if node.ID == nodeID {
			outputs = projection
		}
		result, err := runner.Run(ctx, executor.NodeContext{
			BuildID:     pipelineID,
			BuildBranch: "preview",
			Attempt:     1,
			Node: executor.Node{
				ID:        node.ID,
				JobID:     uuid.NewSHA1(uuid.NameSpaceOID, []byte(pipelineID.String()+":preview:"+node.ID)),
				DependsOn: node.DependsOn,
				Outputs:   outputs,
				Metadata:  metadata,
			},
		})
		if err != nil {
			return pipelineNodePreviewResponse{}, previewExecutionError{status: http.StatusBadRequest, err: pipelineNodePreviewError{Kind: "transform_failed", NodeID: node.ID, Transform: transform, Message: err.Error()}}
		}
		if node.ID == nodeID {
			targetMetadata = result.Metadata
		}
		if sampleNode {
			runtime.trimRows(node.ID, sampleSize)
		}
	}
	rows := limitRuntimeRows(runtime.snapshotRows(nodeID), sampleSize)
	response := pipelineNodePreviewResponse{
		PipelineID:  pipelineID.String(),
		NodeID:      nodeID,
		Columns:     deriveRuntimeColumns(rows),
		Rows:        rowsToMaps(rows),
		SampleSize:  sampleSize,
		GeneratedAt: time.Now().UTC(),
		Seed:        pipelinePreviewSeed(pipelineID, nodeID),
		SourceChain: chainIDs(chain),
		Fresh:       true,
	}
	if columns := resultColumns(targetMetadata); len(rows) == 0 && len(columns) > 0 {
		response.Columns = columns
	}
	if columns := runtime.positionalColumns(nodeID); columns != nil {
		response.Columns = columns
	}
	if schema, ok := targetMetadata["dataset_schema"].(*datasetRowsSchema); ok && schema != nil {
		response.Columns = schema.columns()
		response.DatasetFileFormat = metadataString(targetMetadata, "dataset_file_format")
		response.DatasetSchemaVersion = metadataString(targetMetadata, "dataset_schema_version")
		response.DatasetSchema = schema
	}
	if len(projection) > 0 {
		response.Columns, response.Rows, err = applyOutputColumnProjection(projection[0], resultColumns(targetMetadata), response.Rows)
		if err == nil && response.DatasetSchema != nil {
			source := response.DatasetSchema
			var fields []datasetOutputField
			fields, err = projectedOutputFields(source.Fields, response.Columns)
			response.DatasetSchema = &datasetRowsSchema{FileFormat: source.FileFormat, Fields: fields}
		}
		if err != nil {
			return pipelineNodePreviewResponse{}, previewExecutionError{status: http.StatusBadRequest, err: pipelineNodePreviewError{Kind: "transform_failed", NodeID: nodeID, Transform: target.TransformType, Message: err.Error()}}
		}
	}
	return response, nil
}

// A downstream join, sort or filter can depend on a model result outside
// the display window. Propagate that need along its actual dependencies.
// A model downstream of such an operator can still use a prefix.
func completeRowsForLLMPreview(chain []models.PipelineNode) map[string]bool {
	if !slices.ContainsFunc(chain, func(node models.PipelineNode) bool { return isLLMTransform(node.TransformType) }) {
		return nil
	}
	complete := make(map[string]bool)
	for i := len(chain) - 1; i >= 0; i-- {
		node := chain[i]
		if complete[node.ID] || previewConsumesWholeRelation(node) {
			for _, dep := range node.DependsOn {
				complete[dep] = true
			}
		}
	}
	return complete
}

func previewConsumesWholeRelation(node models.PipelineNode) bool {
	switch normaliseTableTransform(node.TransformType) {
	case "filter":
		return true
	case "sql":
		cfg, err := parseTableRuntimeConfig(node.Config)
		if err != nil {
			return false // The execution path reports invalid configurations.
		}
		if cfg.Stack == nil {
			return cfg.Join != nil || cfg.GeoJoin != nil || cfg.Union != nil || cfg.Predicate != "" || cfg.Expression != ""
		}
		for _, block := range cfg.Stack.Blocks {
			if block.Applied {
				switch strings.ToLower(strings.TrimSpace(block.Kind)) {
				case "filter", "sort", "aggregate", "conditional_aggregate", "window_aggregate", "explode", "csv_parse":
					return true
				}
			}
		}
	}
	return false
}

func previewChainRequiresBuildInputs(chain []models.PipelineNode) bool {
	var pinned, needsPinnedInput bool
	for _, node := range chain {
		pinned = pinned || PayloadDeclaresDatasetSnapshot(node.Config)
		kind := normaliseTableTransform(node.TransformType)
		if kind == "drop" || kind == "gpx_parse" {
			needsPinnedInput = true
		}
		if kind != "sql" {
			continue
		}
		cfg, err := parseTableRuntimeConfig(node.Config)
		if err != nil {
			continue
		}
		if cfg.Stack == nil {
			// A matching neighbor or a union column may first appear beyond the
			// display window. Limit the final result after reading bounded inputs.
			if cfg.Join != nil || cfg.GeoJoin != nil || cfg.Union != nil {
				return true
			}
			continue
		}
		for _, block := range cfg.Stack.Blocks {
			if !block.Applied {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(block.Kind), "window_aggregate") {
				return true
			}
			if block.shapesColumns() || isHaversineBlockKind(block.Kind) {
				needsPinnedInput = true
			}
		}
	}
	// A pinned read rejects truncation. The preview size limits the display.
	return pinned && needsPinnedInput
}

func isHaversineBlockKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "haversine", "haversine_distance", "geo_distance":
		return true
	}
	return false
}

func upstreamExecutionChain(nodes []models.PipelineNode, target string) ([]models.PipelineNode, error) {
	byID := make(map[string]models.PipelineNode, len(nodes))
	for _, node := range nodes {
		byID[node.ID] = node
	}
	if _, ok := byID[target]; !ok {
		return nil, previewExecutionError{status: http.StatusNotFound, err: pipelineNodePreviewError{Kind: "node_not_found", NodeID: target, Message: "node not found in pipeline graph"}}
	}
	visiting := map[string]bool{}
	visited := map[string]bool{}
	out := []models.PipelineNode{}
	var visit func(string) error
	visit = func(id string) error {
		if visited[id] {
			return nil
		}
		if visiting[id] {
			return previewExecutionError{status: http.StatusBadRequest, err: pipelineNodePreviewError{Kind: "cycle_detected", NodeID: id, Message: "cycle detected in upstream preview chain"}}
		}
		node, ok := byID[id]
		if !ok {
			return previewExecutionError{status: http.StatusBadRequest, err: pipelineNodePreviewError{Kind: "missing_dependency", NodeID: id, Message: "node dependency is missing from pipeline graph"}}
		}
		visiting[id] = true
		for _, dep := range node.DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		out = append(out, node)
		return nil
	}
	if err := visit(target); err != nil {
		return nil, err
	}
	return out, nil
}

func readPreviewPipelineNodeRequest(r *http.Request) (previewPipelineNodeRequest, error) {
	var req previewPipelineNodeRequest
	if r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch {
		return req, nil
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return req, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return req, nil
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, err
	}
	return req, nil
}

func previewConfigWithSampleSize(raw json.RawMessage, sampleSize int) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return mustRuntimeJSON(map[string]any{"sample_size": sampleSize})
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return raw
	}
	if _, ok := cfg["sample_size"]; !ok {
		cfg["sample_size"] = mustRuntimeJSON(sampleSize)
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return raw
	}
	return out
}

func writePipelinePreviewError(w http.ResponseWriter, pipelineID uuid.UUID, nodeID string, sampleSize int, chain []string, status int, err pipelineNodePreviewError) {
	if chain == nil {
		chain = []string{}
	}
	writeJSON(w, status, pipelineNodePreviewResponse{
		PipelineID:  pipelineID.String(),
		NodeID:      nodeID,
		Columns:     []string{},
		Rows:        []map[string]json.RawMessage{},
		SampleSize:  sampleSize,
		GeneratedAt: time.Now().UTC(),
		Seed:        pipelinePreviewSeed(pipelineID, nodeID),
		SourceChain: chain,
		Fresh:       false,
		Error:       &err,
	})
}

func (e previewExecutionError) errChain(nodes []models.PipelineNode, target string) []string {
	chain, err := upstreamExecutionChain(nodes, target)
	if err != nil {
		return nil
	}
	return chainIDs(chain)
}

func pipelineNodeIDFromRequest(r *http.Request) (string, error) {
	for _, key := range []string{"node_id", "nodeId"} {
		if raw := chi.URLParam(r, key); raw != "" {
			decoded, err := url.PathUnescape(raw)
			if err != nil {
				return "", err
			}
			return decoded, nil
		}
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for i, part := range parts {
		if part == "nodes" && i+1 < len(parts) {
			decoded, err := url.PathUnescape(parts[i+1])
			if err != nil {
				return "", err
			}
			return decoded, nil
		}
	}
	return "", errors.New("node id is required")
}

func limitRuntimeRows(rows []pipelineexpression.Row, limit int) []pipelineexpression.Row {
	if limit <= 0 || len(rows) <= limit {
		return cloneRows(rows)
	}
	return cloneRows(rows[:limit])
}

func chainIDs(nodes []models.PipelineNode) []string {
	out := make([]string, len(nodes))
	for i, node := range nodes {
		out[i] = node.ID
	}
	return out
}

func pipelinePreviewSeed(pipelineID uuid.UUID, nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(pipelineID.String()))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(nodeID))
	return h.Sum64()
}

func queryInt(r *http.Request, key string) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return parsed
}

func firstPositiveInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return defaultPipelinePreviewSampleSize
}

func clampPipelinePreviewSampleSize(value int) int {
	if value <= 0 {
		return defaultPipelinePreviewSampleSize
	}
	if value > maxPipelinePreviewSampleSize {
		return maxPipelinePreviewSampleSize
	}
	return value
}
