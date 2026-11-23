package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnCastExactAndTyped(t *testing.T) {
	for _, tc := range []struct{ raw, target, want string }{
		{`"9007199254740993"`, "Long", "9007199254740993"},
		{`9007199254740993`, "String", `"9007199254740993"`},
		{`"9.007199254740993e15"`, "Long", "9007199254740993"},
		{`"9223372036854775807.99"`, "Long", "9223372036854775807"},
		{`"-9223372036854775808.9"`, "Long", "-9223372036854775808"},
		{`"2147483647.99999"`, "Integer", "2147483647"},
		{`"2026-11-23T09:10:11+08:00"`, "Timestamp", `"2026-11-23T01:10:11Z"`},
		{`"2024-02-29"`, "Date", `"2024-02-29"`},
		{`"2147483648"`, "Integer", ""}, {`"9223372036854775808"`, "Long", ""},
		{`"-9223372036854775809"`, "Long", ""}, {`"1e999999"`, "Long", ""},
		{`"NaN"`, "Double", ""}, {`"Inf"`, "Double", ""},
		{`"2026-02-29"`, "Date", ""}, {`"bad instant"`, "Timestamp", ""},
	} {
		t.Run(tc.target+"/"+tc.raw, func(t *testing.T) {
			got, err := columnCastValue(json.RawMessage(tc.raw), tc.target)
			if tc.want == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
		})
	}
}
