package handler

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

const runtimeJoinLeftAnti = "left_anti"

func joinLeftAnti(left, right []pe.Row, matches []runtimeJoinMatch, leftColumns []string) ([]pe.Row, error) {
	leftKeys, rightKeys := joinKeyColumns(matches)
	rightIndex := make(map[string]struct{}, len(right))
	for _, row := range right {
		key, matchable, err := antiJoinKey(row, rightKeys)
		if err != nil {
			return nil, err
		}
		if matchable {
			rightIndex[key] = struct{}{}
		}
	}

	out := make([]pe.Row, 0, len(left))
	for _, row := range left {
		key, matchable, err := antiJoinKey(row, leftKeys)
		if err != nil {
			return nil, err
		}
		if _, matched := rightIndex[key]; matchable && matched {
			continue
		}
		out = append(out, composeRuntimeJoinRow(row, nil, leftColumns, nil, ""))
	}
	return out, nil
}

func antiJoinKey(row pe.Row, columns []string) (string, bool, error) {
	key, matchable, err := runtimeCompositeKey(row, columns, false)
	if err != nil {
		return "", false, fmt.Errorf("left anti join %w", err)
	}
	return key, matchable, nil
}

func runtimeCompositeKey(row pe.Row, columns []string, groupNulls bool) (string, bool, error) {
	var key []byte
	for _, column := range columns {
		raw := bytes.TrimSpace(row[column])
		value := pe.EvalValueFromJSON(raw)
		var text string
		var kind byte
		switch value.Kind {
		case pe.EvalKindNull:
			if len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
				return "", false, fmt.Errorf("key column %q requires a scalar value", column)
			}
			if !groupNulls {
				return "", false, nil
			}
			kind = 'z'
		case pe.EvalKindString:
			text, kind = value.Str, 's'
		case pe.EvalKindBool:
			text, kind = strconv.FormatBool(value.Bool), 'b'
		case pe.EvalKindInteger:
			text, kind = strconv.FormatInt(value.Int, 10), 'n'
		case pe.EvalKindDouble:
			// RatString canonicalizes integral doubles exactly without rounding
			// int64 keys through float64 and collapsing adjacent integers.
			text, kind = new(big.Rat).SetFloat64(value.Double).RatString(), 'n'
		default:
			return "", false, fmt.Errorf("key column %q requires a scalar value", column)
		}
		// Include type and length so distinct composite tuples cannot collide.
		key = append(key, kind)
		key = strconv.AppendInt(key, int64(len(text)), 10)
		key = append(key, ':')
		key = append(key, text...)
	}
	return string(key), true, nil
}
