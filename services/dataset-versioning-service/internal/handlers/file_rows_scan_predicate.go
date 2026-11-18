// WP3.3 increment 2: the rows:scan predicate — one clearly supported
// top-level scalar equality, parsed strictly and compared with exact types.
//
// The wire shape is {"column":C,"op":"eq","value":V} passed as the URL
// query parameter `predicate`. Everything is validated up front: unknown or
// duplicate keys, a missing column/op/value, an op other than "eq", a
// non-scalar value, malformed JSON, trailing content and length limits are
// all explicit 400s — there is no lenient fallback.
//
// Comparison semantics: field names match with exact case; string, boolean,
// null and number values only equal values of the same JSON type (no
// coercion — the string "1" is not the number 1); a record without the
// column never matches (a missing field is not null); numbers compare
// exactly through a normalized decimal form (see decimalNumber) with no
// float64 anywhere, so 9007199254740992 and 9007199254740993 stay
// distinguishable while 1, 1.0 and 1e0 are the same value. Only top-level
// record fields participate; a record whose top level repeats the column
// key matches by the LAST occurrence, matching what encoding/json's map
// decoding would observe.

package handlers

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Bounds on the predicate and its pieces. These exist so a request can
// never make the server build large objects from arbitrary-length tokens
// or arbitrary exponents; exceeding any bound is a 400, never a truncation.
const (
	scanPredicateMaxRaw      = 4096  // bytes of the raw query parameter
	scanPredicateMaxColumn   = 256   // bytes of the column name
	scanPredicateMaxString   = 1024  // bytes of a decoded string value
	scanPredicateMaxNumber   = 64    // bytes of a number token
	scanPredicateMaxExponent = 10000 // |explicit exponent| of a predicate number
)

// scanPredicateValueKind tags the JSON type of a predicate value.
type scanPredicateValueKind uint8

const (
	scanPredicateString scanPredicateValueKind = iota + 1
	scanPredicateBool
	scanPredicateNull
	scanPredicateNumber
)

// decimalNumber is the exact representation of a finite JSON decimal: the
// value is neg × digits × 10^exp10 where digits has no leading and no
// trailing zeros. Two decimals are mathematically equal iff their
// normalized triples are equal, so equality is plain string/integer
// comparison — exact at any magnitude, with no float64 and no big-number
// construction. Zero normalizes to {false, "0", 0} for every spelling
// ("0", "-0", "0.0", "0e7", …).
type decimalNumber struct {
	neg    bool
	digits string
	exp10  int64
}

// scanPredicateValue is one typed scalar.
type scanPredicateValue struct {
	kind    scanPredicateValueKind
	str     string        // scanPredicateString
	boolean bool          // scanPredicateBool
	number  decimalNumber // scanPredicateNumber
}

// scanPredicate is the parsed predicate.
type scanPredicate struct {
	column string
	value  scanPredicateValue
}

// parseScanPredicate strictly parses the raw `predicate` query parameter.
func parseScanPredicate(raw string) (*scanPredicate, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("predicate is empty")
	}
	if len(raw) > scanPredicateMaxRaw {
		return nil, fmt.Errorf("predicate exceeds %d bytes", scanPredicateMaxRaw)
	}
	// Invalid UTF-8 must be rejected before the JSON decoder sees it:
	// decoding would silently replace bad bytes with U+FFFD, letting a
	// percent-encoded %FF smuggle a different string than was sent.
	if !utf8.ValidString(raw) {
		return nil, fmt.Errorf("predicate is not valid UTF-8")
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	openTok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("predicate is not a JSON object")
	}
	if delim, ok := openTok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("predicate is not a JSON object")
	}
	seen := map[string]bool{}
	var column, op string
	var value *scanPredicateValue
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("predicate is malformed JSON")
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("predicate is malformed JSON")
		}
		if seen[key] {
			return nil, fmt.Errorf("predicate field %q appears more than once", key)
		}
		seen[key] = true
		switch key {
		case "column":
			tok, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("predicate column must be a string")
			}
			str, ok := tok.(string)
			if !ok {
				return nil, fmt.Errorf("predicate column must be a string")
			}
			if len(str) == 0 || len(str) > scanPredicateMaxColumn {
				return nil, fmt.Errorf("predicate column must be 1-%d bytes", scanPredicateMaxColumn)
			}
			column = str
		case "op":
			tok, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("predicate op must be a string")
			}
			str, ok := tok.(string)
			if !ok {
				return nil, fmt.Errorf("predicate op must be a string")
			}
			op = str
		case "value":
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, fmt.Errorf("predicate value is not valid JSON")
			}
			v, err := parseScanPredicateValue(raw)
			if err != nil {
				return nil, err
			}
			value = &v
		default:
			return nil, fmt.Errorf("predicate field %q is not supported (expected column, op, value)", key)
		}
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return nil, fmt.Errorf("predicate is malformed JSON")
	}
	// After the closing '}' the input must be exhausted. Any outcome other
	// than a clean io.EOF — a second value or trailing garbage — rejects.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("predicate has trailing content")
	}
	if column == "" {
		return nil, fmt.Errorf("predicate column is required")
	}
	if op == "" {
		return nil, fmt.Errorf("predicate op is required")
	}
	if op != "eq" {
		return nil, fmt.Errorf("predicate op must be \"eq\", got %q", op)
	}
	if value == nil {
		return nil, fmt.Errorf("predicate value is required")
	}
	return &scanPredicate{column: column, value: *value}, nil
}

// parseScanPredicateValue classifies one scalar value; objects and arrays
// are rejected as non-scalar.
func parseScanPredicateValue(raw json.RawMessage) (scanPredicateValue, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return scanPredicateValue{}, fmt.Errorf("predicate value is not valid JSON")
	}
	switch trimmed[0] {
	case '"':
		var str string
		if err := json.Unmarshal([]byte(trimmed), &str); err != nil {
			return scanPredicateValue{}, fmt.Errorf("predicate string value is malformed")
		}
		if len(str) > scanPredicateMaxString {
			return scanPredicateValue{}, fmt.Errorf("predicate string value exceeds %d bytes", scanPredicateMaxString)
		}
		return scanPredicateValue{kind: scanPredicateString, str: str}, nil
	case 't', 'f':
		var b bool
		if err := json.Unmarshal([]byte(trimmed), &b); err != nil {
			return scanPredicateValue{}, fmt.Errorf("predicate value is not a scalar")
		}
		return scanPredicateValue{kind: scanPredicateBool, boolean: b}, nil
	case 'n':
		if trimmed != "null" {
			return scanPredicateValue{}, fmt.Errorf("predicate value is not a scalar")
		}
		return scanPredicateValue{kind: scanPredicateNull}, nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if len(trimmed) > scanPredicateMaxNumber {
			return scanPredicateValue{}, fmt.Errorf("predicate number token exceeds %d bytes", scanPredicateMaxNumber)
		}
		num, hasExp, exp, err := parseDecimalToken(trimmed)
		if err != nil {
			return scanPredicateValue{}, err
		}
		if hasExp && (exp > scanPredicateMaxExponent || exp < -scanPredicateMaxExponent) {
			return scanPredicateValue{}, fmt.Errorf("predicate number exponent exceeds |%d|", scanPredicateMaxExponent)
		}
		return scanPredicateValue{kind: scanPredicateNumber, number: num}, nil
	case '{', '[':
		return scanPredicateValue{}, fmt.Errorf("predicate value must be a scalar (string, boolean, null or number)")
	default:
		return scanPredicateValue{}, fmt.Errorf("predicate value is not a scalar")
	}
}

// parseDecimalToken normalizes one JSON number token into its exact
// decimal triple. The explicit exponent is parsed BEFORE the zero
// normalization and always reported (present flag and value) so the
// predicate parser can bound it even for zero mantissas ("0e10001" is
// rejected like "1e10001"); row-side callers ignore it. An exponent that
// does not fit int64 clamps to ±MaxInt64 — the value then differs from
// every bounded predicate number, so equality stays exact. Every
// accumulation into exp10 is overflow-clamped rather than wrapped.
func parseDecimalToken(token string) (num decimalNumber, hasExp bool, exp int64, err error) {
	s := token
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	intPart := ""
	fracPart := ""
	i := 0
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == 0 {
		return decimalNumber{}, false, 0, fmt.Errorf("malformed number token")
	}
	intPart = s[:i]
	if i < len(s) && s[i] == '.' {
		i++
		fracStart := i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		if i == fracStart {
			return decimalNumber{}, false, 0, fmt.Errorf("malformed number token")
		}
		fracPart = s[fracStart:i]
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		expStart := i
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		digitsStart := i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		if i == digitsStart {
			return decimalNumber{}, false, 0, fmt.Errorf("malformed number token")
		}
		parsed, parseErr := strconv.ParseInt(s[expStart:i], 10, 64)
		if parseErr != nil {
			var numErr *strconv.NumError
			if errors.As(parseErr, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
				if parsed > 0 {
					parsed = math.MaxInt64
				} else {
					parsed = math.MinInt64
				}
			} else {
				return decimalNumber{}, false, 0, fmt.Errorf("malformed number token")
			}
		}
		hasExp = true
		exp = parsed
	}
	if i != len(s) {
		return decimalNumber{}, false, 0, fmt.Errorf("malformed number token")
	}

	digits := intPart + fracPart
	// Fold away leading zeros, then trailing zeros (each trailing zero
	// shifts the decimal point up by one, clamped at the int64 ceiling).
	for len(digits) > 1 && digits[0] == '0' {
		digits = digits[1:]
	}
	exp10 := exp
	for len(digits) > 0 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
		if exp10 < math.MaxInt64 {
			exp10++
		}
	}
	// The fraction length shifts the point down; clamp at the floor.
	if fracLen := int64(len(fracPart)); exp10 < math.MinInt64+fracLen {
		exp10 = math.MinInt64
	} else {
		exp10 -= fracLen
	}
	if digits == "" {
		// Every spelling of zero normalizes identically (the exponent is
		// still reported for the predicate bound check above).
		return decimalNumber{neg: false, digits: "0", exp10: 0}, hasExp, exp, nil
	}
	return decimalNumber{neg: neg, digits: digits, exp10: exp10}, hasExp, exp, nil
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// decimalEquals is exact equality of two normalized decimals.
func decimalEquals(a, b decimalNumber) bool {
	return a.neg == b.neg && a.digits == b.digits && a.exp10 == b.exp10
}

// matchesRecord reports whether one NDJSON record satisfies the predicate.
// The record must already be a valid JSON object (validNDJSONRecord).
func (p *scanPredicate) matchesRecord(record []byte) bool {
	value, found := topLevelFieldValue(record, p.column)
	if !found {
		// A missing field is not null and never matches.
		return false
	}
	return scanScalarMatches(value, p.value)
}

// topLevelFieldValue extracts the raw JSON value of the LAST top-level
// occurrence of column in a JSON object record (encoding/json map
// semantics). Non-object input returns not-found.
func topLevelFieldValue(record []byte, column string) (json.RawMessage, bool) {
	dec := json.NewDecoder(strings.NewReader(string(record)))
	openTok, err := dec.Token()
	if err != nil {
		return nil, false
	}
	if delim, ok := openTok.(json.Delim); !ok || delim != '{' {
		return nil, false
	}
	var found json.RawMessage
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, false
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, false
		}
		if key == column {
			found = value
		}
	}
	return found, found != nil
}

// scanScalarMatches compares one raw record value (already extracted) with
// the typed predicate value. Exact types only: no cross-type coercion, and
// non-scalar record values (object/array) never match a scalar predicate.
func scanScalarMatches(raw json.RawMessage, want scanPredicateValue) bool {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return false
	}
	switch trimmed[0] {
	case '"':
		if want.kind != scanPredicateString {
			return false
		}
		var str string
		if err := json.Unmarshal([]byte(trimmed), &str); err != nil {
			return false
		}
		return str == want.str
	case 't', 'f':
		if want.kind != scanPredicateBool {
			return false
		}
		var b bool
		if err := json.Unmarshal([]byte(trimmed), &b); err != nil {
			return false
		}
		return b == want.boolean
	case 'n':
		if trimmed != "null" {
			return false
		}
		return want.kind == scanPredicateNull
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if want.kind != scanPredicateNumber {
			return false
		}
		num, _, _, err := parseDecimalToken(trimmed)
		if err != nil {
			return false
		}
		return decimalEquals(num, want.number)
	default:
		return false
	}
}

// encodeScanPredicate renders the predicate into its canonical business
// form: column (length-prefixed) plus a type tag plus the canonical value
// bytes. Two predicates encode identically iff they mean the same thing
// ("1", "1.0" and "1e0" all produce the same bytes). The cursor does not
// store these bytes — it stores their SHA-256 digest, which is what a
// continuation compares, so equal-value spellings keep paging while any
// semantic change is a mismatch.
func encodeScanPredicate(p *scanPredicate) []byte {
	value := encodeScanPredicateValue(p.value)
	out := make([]byte, 0, 2+len(p.column)+len(value))
	out = append(out, byte(len(p.column)>>8), byte(len(p.column)))
	out = append(out, p.column...)
	return append(out, value...)
}

// scanPredicateDigest is the cursor binding for a predicate: a presence
// flag plus the SHA-256 of the canonical encoding. A zero flag with a zero
// digest marks "no predicate".
func scanPredicateDigest(p *scanPredicate) (flag byte, digest [32]byte) {
	if p == nil {
		return 0, [32]byte{}
	}
	sum := sha256.Sum256(encodeScanPredicate(p))
	return 1, sum
}

func encodeScanPredicateValue(v scanPredicateValue) []byte {
	switch v.kind {
	case scanPredicateString:
		return append([]byte{'S'}, v.str...)
	case scanPredicateBool:
		if v.boolean {
			return []byte{'B', '1'}
		}
		return []byte{'B', '0'}
	case scanPredicateNull:
		return []byte{'N'}
	case scanPredicateNumber:
		num := []byte{'D'}
		if v.number.neg {
			num = append(num, '-')
		}
		num = append(num, v.number.digits...)
		num = append(num, 'e')
		return strconv.AppendInt(num, v.number.exp10, 10)
	default:
		return []byte{'?'}
	}
}
