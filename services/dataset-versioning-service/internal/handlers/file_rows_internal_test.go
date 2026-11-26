package handlers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVCellValuePreservesLeadingZeros(t *testing.T) {
	t.Parallel()
	cases := []struct {
		cell string
		want string
	}{
		{"007", `"007"`}, // SKU keeps leading zeros as a string
		{"0", `0`},       // zero stays numeric
		{"0.5", `0.5`},   // fractional without leading zero
		{"-0.5", `-0.5`}, // negative fraction
		{"42", `42`},     // plain integer
		{"3.14", `3.14`}, // decimal
		{"-17", `-17`},   // negative integer
		{"+5", `"+5"`},   // explicit sign is not a plain literal
		{"1e5", `"1e5"`}, // exponent notation stays a string
		{" 12 ", `12`},   // surrounding whitespace trimmed
		{"", `null`},     // empty cell is null
		{"  ", `null`},   // whitespace-only cell is null
		{"abc", `"abc"`}, // text stays text
		{"0012345678901234567", `"0012345678901234567"`},   // leading zeros always string
		{"12345678901234567890", `"12345678901234567890"`}, // >int64, >15 digits: exact string
		{"1234567890123456", `1234567890123456`},           // 16 digits fits int64 exactly
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, string(csvCellValue(tc.cell)), "cell %q", tc.cell)
	}
}

// The schema a CSV publication declares mirrors this predicate cell for cell,
// so each of these literals must keep its type.
func TestCSVCellValueKeepsTheInferenceMirrorLiterals(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		cell string
		want string
	}{
		{"1e3", `"1e3"`},           // exponent notation
		{"+1", `"+1"`},             // explicit sign
		{"001.00", `"001.00"`},     // leading zeros
		{"NaN", `"NaN"`},           // not a JSON number
		{"Infinity", `"Infinity"`}, // not a JSON number
		{"9223372036854775808", `"9223372036854775808"`}, // one past int64
		{"1234567890123456.5", `"1234567890123456.5"`},   // 16 integer digits
		{"007", `"007"`}, // SKU keeps leading zeros as a string
		{"-0", `-0`},     // negative zero is an int64
		{"9223372036854775807", `9223372036854775807`}, // the largest int64
		{"123456789012345.5", `123456789012345.5`},     // 15 integer digits
		{" 12 ", `12`}, // surrounding whitespace trimmed
		{"  ", `null`}, // whitespace-only cell is null
	} {
		assert.Equal(t, tc.want, string(csvCellValue(tc.cell)), "cell %q", tc.cell)
	}
}

// The Amazon smoke's reports.column_names mirrors this function.
func TestCSVColumnNamesKeepTheInferenceMirrorRule(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		header  []string
		want    []string
		refused string
	}{
		{"padded cell", []string{" \torder postal\r\n"}, []string{"order postal"}, ""},
		{"non-breaking space and NEL", []string{"\u00a0total\u0085"}, []string{"total"}, ""},
		{"em space and ideographic space", []string{"\u2003total\u3000"}, []string{"total"}, ""},
		{"a separator outside the set is kept", []string{"\x1cstring\x1c"}, []string{"\x1cstring\x1c"}, ""},
		{"empty once trimmed, counted from 1", []string{"", " ", "\t\n"}, []string{"column_1", "column_2", "column_3"}, ""},
		{"a generated name met again", []string{"column_2", " "}, nil, `duplicate column "column_2"`},
		{"the first repeat met is named", []string{"a", "b", "b", "c", "c"}, nil, `duplicate column "b"`},
	} {
		columns, err := csvColumnNames(tc.header)
		if tc.refused != "" {
			assert.EqualError(t, err, tc.refused, tc.name)
			continue
		}
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.want, columns, tc.name)
	}

	// The bound is 256 columns, and a repeated name is refused before it.
	assert.Equal(t, 256, datasetFileRowsMaxColumns)
	wide := make([]string, datasetFileRowsMaxColumns+1)
	for i := range wide {
		wide[i] = fmt.Sprintf("c%d", i)
	}
	columns, err := csvColumnNames(wide[:datasetFileRowsMaxColumns])
	assert.NoError(t, err)
	assert.Len(t, columns, datasetFileRowsMaxColumns)
	_, err = csvColumnNames(wide)
	assert.EqualError(t, err, "file has 257 columns, max 256")
	wide[1] = wide[0]
	_, err = csvColumnNames(wide)
	assert.EqualError(t, err, `duplicate column "c0"`)
}

func TestParseDelimitedRows(t *testing.T) {
	t.Parallel()
	data := []byte("sku,qty,price,label\n007,2,1.5,hello\n100, ,3.25,world\n")
	columns, rows, truncated, err := parseDelimitedRows(data, false, 100)
	require.NoError(t, err)
	require.False(t, truncated)
	assert.Equal(t, []string{"sku", "qty", "price", "label"}, columns)
	require.Len(t, rows, 2)
	assert.Equal(t, `"007"`, string(rows[0]["sku"]))
	assert.Equal(t, `2`, string(rows[0]["qty"]))
	assert.Equal(t, `1.5`, string(rows[0]["price"]))
	assert.Equal(t, `"hello"`, string(rows[0]["label"]))
	assert.Equal(t, `100`, string(rows[1]["sku"]))
	assert.Equal(t, `null`, string(rows[1]["qty"])) // whitespace-only cell
	assert.Equal(t, `3.25`, string(rows[1]["price"]))
}

func TestParseDelimitedRowsLimitAndTruncation(t *testing.T) {
	t.Parallel()
	data := []byte("a,b\n1,2\n3,4\n5,6\n")
	columns, rows, truncated, err := parseDelimitedRows(data, false, 2)
	require.NoError(t, err)
	require.True(t, truncated)
	assert.Equal(t, []string{"a", "b"}, columns)
	require.Len(t, rows, 2)
	assert.Equal(t, `1`, string(rows[0]["a"]))
}

func TestParseDelimitedRowsHeaderOnlyAndEmpty(t *testing.T) {
	t.Parallel()
	columns, rows, truncated, err := parseDelimitedRows([]byte("a,b\n"), false, 10)
	require.NoError(t, err)
	require.False(t, truncated)
	assert.Equal(t, []string{"a", "b"}, columns)
	assert.Empty(t, rows)

	columns, rows, truncated, err = parseDelimitedRows([]byte{}, false, 10)
	require.NoError(t, err)
	require.False(t, truncated)
	assert.Empty(t, columns)
	assert.Empty(t, rows)
}

func TestParseDelimitedRowsDuplicateColumn(t *testing.T) {
	t.Parallel()
	_, _, _, err := parseDelimitedRows([]byte("a,a\n1,2\n"), false, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate column "a"`)
}

func TestParseDelimitedRowsTSV(t *testing.T) {
	t.Parallel()
	columns, rows, truncated, err := parseDelimitedRows([]byte("sku\tqty\n008\t3\n"), true, 10)
	require.NoError(t, err)
	require.False(t, truncated)
	assert.Equal(t, []string{"sku", "qty"}, columns)
	require.Len(t, rows, 1)
	assert.Equal(t, `"008"`, string(rows[0]["sku"]))
	assert.Equal(t, `3`, string(rows[0]["qty"]))
}

func TestParseNDJSONRows(t *testing.T) {
	t.Parallel()
	data := []byte("{\"sku\": \"007\", \"qty\": 4}\n\n{\"sku\": 12, \"price\": 2.5}\n{\"sku\": \"007\", \"qty\": 5}\n")
	columns, rows, truncated, err := parseNDJSONRows(data, 2)
	require.NoError(t, err)
	require.True(t, truncated)
	assert.Equal(t, []string{"price", "qty", "sku"}, columns) // sorted: deterministic across reads
	require.Len(t, rows, 2)
	assert.Equal(t, `"007"`, string(rows[0]["sku"]))
	assert.Equal(t, `4`, string(rows[0]["qty"]))
	assert.Equal(t, `2.5`, string(rows[1]["price"]))
}

func TestParseNDJSONRowsRejectsNonObject(t *testing.T) {
	t.Parallel()
	_, _, _, err := parseNDJSONRows([]byte("[1,2]\n"), 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a JSON object")
}
