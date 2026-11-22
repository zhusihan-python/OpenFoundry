package handler

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

type runtimeSortKey struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
}

func normalizeRuntimeSortKeys(raw []runtimeSortKey) ([]runtimeSortKey, error) {
	if len(raw) == 0 {
		return nil, errors.New("sort requires at least one key")
	}
	keys := make([]runtimeSortKey, 0, len(raw))
	seen := make(map[string]bool, len(raw))
	for _, key := range raw {
		if strings.TrimSpace(key.Column) == "" || seen[key.Column] {
			return nil, fmt.Errorf("sort key column %q is empty or repeated", key.Column)
		}
		seen[key.Column] = true
		key.Direction = strings.ToLower(strings.TrimSpace(key.Direction))
		switch key.Direction {
		case "", "asc":
			key.Direction = "asc"
		case "desc":
		default:
			return nil, fmt.Errorf("sort key %q requires asc or desc", key.Column)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

type runtimeSortValue struct {
	value  pe.EvalValue
	number *big.Rat
}

type runtimeSortRow struct {
	row    pe.Row
	values []runtimeSortValue
}

func applyRuntimeSortBlock(rows []pe.Row, rawKeys []runtimeSortKey) ([]pe.Row, error) {
	keys, err := normalizeRuntimeSortKeys(rawKeys)
	if err != nil {
		return nil, err
	}
	families := make([]pe.EvalValueKind, len(keys))
	for index := range families {
		families[index] = pe.EvalKindNull
	}
	ordered := make([]runtimeSortRow, 0, len(rows))
	for _, row := range rows {
		entry := runtimeSortRow{row: row, values: make([]runtimeSortValue, len(keys))}
		for index, key := range keys {
			raw := bytes.TrimSpace(row[key.Column])
			value := pe.EvalValueFromJSON(raw)
			cell := runtimeSortValue{value: value}
			family := value.Kind
			switch value.Kind {
			case pe.EvalKindNull:
				if len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
					return nil, fmt.Errorf("sort key %q has an unsupported value", key.Column)
				}
			case pe.EvalKindInteger:
				family = pe.EvalKindDouble
				cell.number = new(big.Rat).SetInt64(value.Int)
			case pe.EvalKindDouble:
				cell.number = new(big.Rat).SetFloat64(value.Double)
			}
			if family != pe.EvalKindNull {
				if families[index] != pe.EvalKindNull && families[index] != family {
					return nil, fmt.Errorf("sort key %q has mixed scalar types", key.Column)
				}
				families[index] = family
			}
			entry.values[index] = cell
		}
		ordered = append(ordered, entry)
	}
	slices.SortStableFunc(ordered, func(left, right runtimeSortRow) int {
		for index, key := range keys {
			if order := compareRuntimeSortValues(left.values[index], right.values[index], key.Direction == "desc"); order != 0 {
				return order
			}
		}
		return 0
	})
	result := make([]pe.Row, 0, len(rows))
	for _, entry := range ordered {
		result = append(result, entry.row)
	}
	return result, nil
}

func compareRuntimeSortValues(left, right runtimeSortValue, descending bool) int {
	if left.value.Kind == pe.EvalKindNull {
		if right.value.Kind == pe.EvalKindNull {
			return 0
		}
		return 1
	}
	if right.value.Kind == pe.EvalKindNull {
		return -1
	}
	if descending {
		left, right = right, left
	}
	switch left.value.Kind {
	case pe.EvalKindInteger, pe.EvalKindDouble:
		return left.number.Cmp(right.number)
	case pe.EvalKindString:
		return cmp.Compare(left.value.Str, right.value.Str)
	default:
		if left.value.Bool == right.value.Bool {
			return 0
		}
		if left.value.Bool {
			return 1
		}
		return -1
	}
}
