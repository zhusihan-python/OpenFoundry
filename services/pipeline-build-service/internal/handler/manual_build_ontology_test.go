package handler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func irPipeline(t *testing.T, nodes []models.PipelineIRNode) *models.Pipeline {
	t.Helper()
	ir := models.PipelineIR{Version: "1", Nodes: nodes}
	raw, err := json.Marshal(ir)
	require.NoError(t, err)
	return &models.Pipeline{DAG: json.RawMessage(raw)}
}

func TestOntologyOutputIRNodesClassifiesGraphOutputs(t *testing.T) {
	outID := uuid.New()
	pipeline := irPipeline(t, []models.PipelineIRNode{
		{ID: "src", TransformType: "dataset_input", Config: json.RawMessage(`{}`)},
		{ID: "plain", TransformType: "output_dataset", OutputDatasetID: &outID, Config: json.RawMessage(`{}`)},
		{ID: "obj", TransformType: "output_dataset", OutputDatasetID: &outID,
			Config: json.RawMessage(`{"_output":{"kind":"object_type","primary_key":"orderId"}}`)},
		{ID: "obj_via_transform", TransformType: "output_object_type", OutputDatasetID: &outID, Config: json.RawMessage(`{}`)},
		{ID: "lnk", TransformType: "output_dataset",
			Config: json.RawMessage(`{"_output":{"kind":"link_type","cardinality":"many_to_one"}}`)},
	})
	ir, err := pipeline.RuntimeIR()
	require.NoError(t, err)
	nodes := ontologyOutputIRNodes(ir)
	require.Equal(t, []string{"obj", "obj_via_transform", "lnk"}, nodes)
}

func TestOntologyOutputNodesOfBuildPipelineGatesOnLoader(t *testing.T) {
	pipeline := irPipeline(t, []models.PipelineIRNode{
		{ID: "obj", TransformType: "output_dataset", OutputDatasetID: &uuid.UUID{},
			Config: json.RawMessage(`{"_output":{"kind":"object_type","primary_key":"orderId"}}`)},
	})
	id := uuid.New()
	store := &fakeBuildPipelineLoader{pipeline: pipeline, id: id}
	require.Equal(t, []string{"obj"},
		ontologyOutputNodesOfBuildPipeline(context.Background(), store, "ri.foundry.main.pipeline."+id.String()))
	// Bare UUID references resolve too.
	require.Equal(t, []string{"obj"},
		ontologyOutputNodesOfBuildPipeline(context.Background(), store, id.String()))
	// Unknown or malformed references never claim ontology targets.
	require.Nil(t, ontologyOutputNodesOfBuildPipeline(context.Background(), store, "not-a-rid"))
	require.Nil(t, ontologyOutputNodesOfBuildPipeline(context.Background(), store, ""))
	// Stores that cannot load the pipeline graph keep legacy behavior.
	require.Nil(t, ontologyOutputNodesOfBuildPipeline(context.Background(), struct{}{}, id.String()))
	require.Nil(t, ontologyOutputNodesOfBuildPipeline(context.Background(), nil, id.String()))
}

func TestOntologyOutputNodesOfBuildPipelineHandlesMissingPipeline(t *testing.T) {
	id := uuid.New()
	store := &fakeBuildPipelineLoader{id: id}
	require.Nil(t, ontologyOutputNodesOfBuildPipeline(context.Background(), store, id.String()))
}

type fakeBuildPipelineLoader struct {
	pipeline *models.Pipeline
	id       uuid.UUID
}

func (f *fakeBuildPipelineLoader) LoadPipeline(ctx context.Context, id uuid.UUID) (*models.Pipeline, error) {
	if id != f.id {
		return nil, nil
	}
	return f.pipeline, nil
}
