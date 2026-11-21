package handler

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const workbookSelectionA = `{"path":"a.xlsx","excel":{"sheet":"Orders","mapping":[{"source_index":1,"target":" status "},{"source_index":0,"target":"id"}]}}`
const workbookSelectionB = `{"path":"b.xlt","excel":{"sheet":"Quantities","mapping":[{"source_index":0,"target":"id"},{"source_index":1,"target":"quantity"}]},"long_fields":[" quantity "]}`

func TestExcelSnapshotFilesReconcilesDeclaredSchema(t *testing.T) {
	for _, list := range []string{workbookSelectionB + "," + workbookSelectionA, workbookSelectionA + "," + workbookSelectionB} {
		files, err := parseExcelSnapshotFiles(json.RawMessage("[" + list + "]"))
		require.NoError(t, err)
		require.Equal(t, "a.xlsx", files[0].Path)
		require.Equal(t, "status", files[0].Excel.Mapping[0].Target)
		require.Equal(t, []string{"quantity"}, files[1].LongFields)

		schema, err := workbookSchema(files)
		require.NoError(t, err)
		require.Equal(t, []datasetRowsField{
			{Name: "id", Type: "STRING"},
			{Name: "quantity", Type: "LONG"},
			{Name: "status", Type: "STRING"},
		}, schema.Fields)
	}
}

func TestExcelSnapshotFilesRejectsConflictingTypes(t *testing.T) {
	conflicting := strings.Replace(workbookSelectionB, `[" quantity "]`, `["id"]`, 1)
	files := []string{workbookSelectionA, conflicting}
	for range 2 {
		parsed, err := parseExcelSnapshotFiles(json.RawMessage("[" + strings.Join(files, ",") + "]"))
		require.NoError(t, err)
		_, err = workbookSchema(parsed)
		require.ErrorContains(t, err, `excel_files_type_conflict: target "id"`)
		slices.Reverse(files)
	}
}

func TestExcelSnapshotFilesRejectsInvalidSelections(t *testing.T) {
	for name, other := range map[string]string{
		"duplicate path":   workbookSelectionA,
		"unknown field":    strings.Replace(workbookSelectionB, `"long_fields"`, `"unknown"`, 1),
		"unmapped LONG":    strings.Replace(workbookSelectionB, `" quantity "`, `"missing"`, 1),
		"duplicate target": strings.Replace(workbookSelectionB, `"target":"quantity"`, `"target":" id "`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseExcelSnapshotFiles(json.RawMessage("[" + workbookSelectionA + "," + other + "]"))
			require.Error(t, err)
		})
	}
}
