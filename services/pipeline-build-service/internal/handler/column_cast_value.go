package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// columnCastValue converts a non-null scalar. Read the original number token so
// adjacent int64s remain distinct when a whole column is converted or renamed.
func columnCastValue(raw json.RawMessage, target string) (json.RawMessage, error) {
	token := bytes.TrimSpace(raw)
	value := string(token)
	if len(token) > 0 && token[0] == '"' {
		if err := json.Unmarshal(token, &value); err != nil {
			return nil, err
		}
	}
	text := strings.TrimSpace(value)
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "string", "varchar":
		return json.Marshal(value)
	case "integer", "int":
		return castColumnInteger(text, 32)
	case "long", "bigint":
		return castColumnInteger(text, 64)
	case "float", "double":
		number, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, fmt.Errorf("requires a numeric value representable as a finite double")
		}
		return json.Marshal(number)
	case "date", "timestamp":
		value, err := parseColumnTime(text)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(strings.TrimSpace(target), "date") {
			return json.Marshal(value.UTC().Format(time.DateOnly))
		}
		return json.Marshal(value.UTC().Format(time.RFC3339Nano))
	case "boolean", "bool":
		switch strings.ToLower(text) {
		case "true", "1":
			return json.RawMessage("true"), nil
		case "false", "0":
			return json.RawMessage("false"), nil
		}
		return nil, fmt.Errorf("requires a boolean value")
	}
	return nil, fmt.Errorf("unsupported target type %q", target)
}

func castColumnInteger(text string, bits int) (json.RawMessage, error) {
	if i, err := strconv.ParseInt(text, 10, bits); err == nil {
		return json.Marshal(i)
	}
	// Bound the magnitude before constructing a high-precision decimal.
	// Rounding toward zero preserves the integral part through 64 bits,
	// including a fractional input immediately below the next integer.
	approx, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(approx) || math.IsInf(approx, 0) || math.Abs(approx) >= 1<<64 {
		return nil, fmt.Errorf("requires a numeric value within the %d-bit integer range", bits)
	}
	if approx == 0 {
		return json.RawMessage("0"), nil
	}
	number, _, err := big.ParseFloat(text, 10, 128, big.ToZero)
	if err != nil {
		return nil, fmt.Errorf("requires a decimal numeric value")
	}
	i, _ := number.Int(nil)
	if !i.IsInt64() || (bits == 32 && (i.Int64() < math.MinInt32 || i.Int64() > math.MaxInt32)) {
		return nil, fmt.Errorf("value is outside the %d-bit integer range", bits)
	}
	return json.Marshal(i.Int64())
}

func parseColumnTime(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", time.DateOnly} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("requires a valid ISO date or timestamp")
}
