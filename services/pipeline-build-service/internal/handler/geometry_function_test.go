package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestReusableExpressionFunctionPreservesGeometryArgument(t *testing.T) {
	definition := PipelineFunctionDefinition{
		Name:       "inside_parcel",
		Expression: "geom_within(location,parcel)",
		Parameters: []PipelineFunctionParameter{
			{Name: "location", Type: "Geometry"},
			{Name: "parcel", Type: "Geometry"},
		},
	}
	point := json.RawMessage(`{"type":"Point","coordinates":[1,1]}`)
	polygon := json.RawMessage(`{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}`)
	rows := []pipelineexpression.Row{{"source_point": point, "parcel": polygon}}
	result, err := applyExpressionFunction(rows, definition, runtimeFunctionConfig{
		Args: map[string]string{"location": "source_point"},
	})
	require.NoError(t, err)
	require.Equal(t, json.RawMessage("true"), result[0]["inside_parcel"])
	require.Equal(t, point, result[0]["source_point"])
}
