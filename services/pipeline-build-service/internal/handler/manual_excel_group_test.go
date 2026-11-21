package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateExcelGroupLongPreservesOtherValues(t *testing.T) {
	sheet := manualExcelGroupSheet{RequiredFields: []string{"amount"}, LongFields: []string{"amount", "optional"}}
	cases := []struct {
		name string
		raw  string
		want string
		code string
	}{
		{name: "maximum", raw: `"9223372036854775807"`, want: "9223372036854775807"},
		{name: "minimum", raw: `"-9223372036854775808"`, want: "-9223372036854775808"},
		{name: "normalizes decimal", raw: `" +0007 "`, want: "7"},
		{name: "positive overflow", raw: `"9223372036854775808"`, code: "INVALID_LONG"},
		{name: "negative overflow", raw: `"-9223372036854775809"`, code: "INVALID_LONG"},
		{name: "fraction", raw: `"1.0"`, code: "INVALID_LONG"},
		{name: "exponent", raw: `"1e3"`, code: "INVALID_LONG"},
		{name: "text", raw: `"abc"`, code: "INVALID_LONG"},
		{name: "required missing", raw: "null", code: "REQUIRED_VALUE_MISSING"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := map[string]json.RawMessage{"amount": json.RawMessage(tc.raw), "optional": json.RawMessage("null"), "order": json.RawMessage(`"0007"`)}
			issues := validateExcelGroupRow(row, sheet)
			require.Equal(t, "null", string(row["optional"]))
			require.Equal(t, `"0007"`, string(row["order"]))
			if tc.code != "" {
				require.Equal(t, []excelGroupRowIssue{{field: "amount", code: tc.code}}, issues)
				return
			}
			require.Empty(t, issues)
			require.Equal(t, tc.want, string(row["amount"]))
		})
	}
}
