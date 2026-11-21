package xlsximp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinnedCellRangeFiltersByPhysicalCoordinates(t *testing.T) {
	data := baseWorkbook(t,
		`<row r="1"><c r="C1"><v>10</v></c></row>
		<row r="2"><c r="A2"><v>99</v></c><c r="C2" t="inlineStr"><is><t>007</t></is></c></row>
		<row r="3"><c r="A3"><v>98</v></c></row>
		<row r="4"><c r="C4"><v>9.99</v></c><c r="F4"><f>1+2</f></c></row>
		<row r="5"><c r="C5"><v>50</v></c></row>
		<row r="5000"><c r="C5000"><v>42</v></c></row>`, "", "")
	selection := "C2:E4"
	cfg := Config{Sheet: "说明", Mapping: []ColumnMap{{SourceIndex: 2, Target: "value"}, {SourceIndex: 4, Target: "blank"}}}
	result, err := NormalizeSnapshotRange(data, cfg, "xlsx", &selection)
	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, result.PhysicalRows)
	require.Equal(t, "{\"blank\":null,\"value\":\"007\"}\n{\"blank\":null,\"value\":\"9.99\"}\n", string(result.NDJSON()))

	far := "C5000:C5000"
	result, err = NormalizeSnapshotRange(data, Config{Sheet: cfg.Sheet, Mapping: cfg.Mapping[:1]}, "xlsx", &far)
	require.NoError(t, err)
	require.Equal(t, []int{5000}, result.PhysicalRows)
	require.Equal(t, "{\"value\":\"42\"}\n", string(result.NDJSON()))

	cfg.HeaderRow = intPtr(1)
	_, err = NormalizeSnapshotRange(data, cfg, "xlsx", &selection)
	wantKind(t, err, ErrHeader, "header outside range")
	cfg.HeaderRow = nil
	cfg.Mapping[0].SourceIndex = 0
	_, err = NormalizeSnapshotRange(data, cfg, "xlsx", &selection)
	wantKind(t, err, ErrMapping, "mapping outside range")
	_, err = NormalizeSnapshotRange(data, cfg, "xls", &selection)
	wantKind(t, err, ErrConfig, "unsupported BIFF8 range")
}

func TestPinnedCellRangeValidation(t *testing.T) {
	for _, value := range []string{"", "A1", "C5:B12", "B5:C1", "A1:XFD2", "A1:A1048577", "A+1:B2", "A01:B2", "Sheet1!A1:B2", "$A$1:B2"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseCellRectangle(value, nil)
			wantKind(t, err, ErrConfig, "invalid A1 range")
		})
	}
	_, err := parseCellRectangle("A1:A1001", nil)
	wantKind(t, err, ErrRowLimit, "too many data rows")
	_, err = parseCellRectangle("A1:A1001", intPtr(1))
	require.NoError(t, err)
}
