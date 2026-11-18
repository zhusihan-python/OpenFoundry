package handlers

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
)

// --- predicate parsing ---

func TestParseScanPredicateAccepts(t *testing.T) {
	for name, raw := range map[string]string{
		"string value":     `{"column":"id","op":"eq","value":"abc"}`,
		"bool value":       `{"column":"flag","op":"eq","value":false}`,
		"null value":       `{"column":"note","op":"eq","value":null}`,
		"int value":        `{"column":"n","op":"eq","value":9007199254740993}`,
		"float value":      `{"column":"n","op":"eq","value":1.0}`,
		"exponent value":   `{"column":"n","op":"eq","value":1e2}`,
		"unicode column":   `{"column":"字段","op":"eq","value":"行"}`,
		"escaped string":   `{"column":"s","op":"eq","value":"a\"b"}`,
		"whitespace body":  ` { "column" : "c" , "op" : "eq" , "value" : 1 } `,
		"negative number":  `{"column":"n","op":"eq","value":-0.5}`,
		"negative zero":    `{"column":"n","op":"eq","value":-0}`,
		"empty string":     `{"column":"c","op":"eq","value":""}`,
		"key order swapped": `{"value":1,"op":"eq","column":"c"}`,
	} {
		pred, err := parseScanPredicate(raw)
		require.NoError(t, err, "case %q", name)
		assert.NotEmpty(t, pred.column)
	}
}

func TestParseScanPredicateRejects(t *testing.T) {
	for name, raw := range map[string]string{
		"empty":                   ``,
		"not an object":           `[1]`,
		"bare scalar":             `"x"`,
		"missing column":          `{"op":"eq","value":1}`,
		"missing op":              `{"column":"c","value":1}`,
		"missing value":           `{"column":"c","op":"eq"}`,
		"unknown field":           `{"column":"c","op":"eq","value":1,"extra":2}`,
		"duplicate column":        `{"column":"a","column":"b","op":"eq","value":1}`,
		"duplicate value":         `{"column":"c","op":"eq","value":1,"value":2}`,
		"op neq":                  `{"column":"c","op":"neq","value":1}`,
		"op non-string":           `{"column":"c","op":1,"value":1}`,
		"column non-string":       `{"column":1,"op":"eq","value":1}`,
		"column empty":            `{"column":"","op":"eq","value":1}`,
		"column too long":         `{"column":"` + strings.Repeat("x", 257) + `","op":"eq","value":1}`,
		"object value":            `{"column":"c","op":"eq","value":{"a":1}}`,
		"array value":             `{"column":"c","op":"eq","value":[1]}`,
		"malformed json":          `{"column":`,
		"trailing garbage":        `{"column":"c","op":"eq","value":1}x`,
		"trailing second value":   `{"column":"c","op":"eq","value":1} {"column":"c","op":"eq","value":2}`,
		"trailing whitespace obj": `{"column":"c","op":"eq","value":1} {}`,
		"number token too long":   `{"column":"n","op":"eq","value":` + strings.Repeat("1", 65) + `}`,
		"exponent too large":      `{"column":"n","op":"eq","value":1e10001}`,
		"exponent too small":      `{"column":"n","op":"eq","value":1e-10001}`,
		"zero huge exponent":      `{"column":"n","op":"eq","value":0e10001}`,
		"zero huge neg exponent":  `{"column":"n","op":"eq","value":0e-9223372036854775808}`,
		"string value too long":   `{"column":"s","op":"eq","value":"` + strings.Repeat("x", 1025) + `"}`,
		"raw too long":            `{"column":"c","op":"eq","value":"` + strings.Repeat("x", 4100) + `"}`,
		"invalid utf8":            "{\"column\":\"\xff\",\"op\":\"eq\",\"value\":1}",
	} {
		_, err := parseScanPredicate(raw)
		assert.Error(t, err, "case %q must be rejected", name)
	}
}

// --- decimal normalization: exactness table ---

func TestDecimalTokenNormalization(t *testing.T) {
	triple := func(token string) decimalNumber {
		num, _, _, err := parseDecimalToken(token)
		require.NoError(t, err, "token %s", token)
		return num
	}
	// Equal spellings of the same value.
	for _, pair := range [][2]string{
		{"1", "1.0"}, {"1", "1e0"}, {"1e0", "1.000"}, {"1", "1e-0"},
		{"100", "1e2"}, {"100", "100.0"}, {"0.1", "0.10"}, {"0.1", "1e-1"},
		{"0.001", "1e-3"}, {"-0", "0"}, {"-0.0", "0e7"}, {"0", "0.000"},
		{"-1.5", "-1.50"}, {"-1.5", "-15e-1"}, {"9007199254740992", "9007199254740992.0"},
		{"123456789012345678901234567890", "123456789012345678901234567890e0"},
	} {
		assert.Equal(t, triple(pair[0]), triple(pair[1]), "%s must equal %s", pair[0], pair[1])
		assert.True(t, decimalEquals(triple(pair[0]), triple(pair[1])))
	}
	// Distinct values that a float64 would collapse.
	for _, pair := range [][2]string{
		{"9007199254740992", "9007199254740993"},
		{"0.1", "0.10000000000000000000001"},
		{"1e-400", "0"}, // sub-float representable, but nonzero decimal
		{"123456789012345678901", "123456789012345678902"},
	} {
		assert.False(t, decimalEquals(triple(pair[0]), triple(pair[1])), "%s must differ from %s", pair[0], pair[1])
	}
	// Out-of-int64 exponents on the ROW side clamp but stay unequal to any
	// bounded predicate number; zero stays zero regardless of exponent.
	huge, _, _, err := parseDecimalToken("1e99999999999999999999")
	require.NoError(t, err)
	assert.False(t, decimalEquals(huge, triple("1e10000")))
	zeroHuge, hasExp, exp, err := parseDecimalToken("-0e99999999999999999999")
	require.NoError(t, err)
	assert.True(t, zeroHuge.digits == "0" && !zeroHuge.neg, "zero mantissa normalizes to zero")
	assert.True(t, hasExp && exp > 0, "exponent is still reported for bounded-zero rejection")
}

// --- scalar matching: typed compare, missing != null, case, last-wins ---

func TestScanScalarMatchingTypes(t *testing.T) {
	pred := func(raw string) *scanPredicate {
		p, err := parseScanPredicate(raw)
		require.NoError(t, err)
		return p
	}
	row := `{"id":9007199254740993,"flag":true,"note":null,"s":"1","ID":7,"o":{"z":1}}`
	record := json.RawMessage(row)

	cases := []struct {
		predicate string
		matches   bool
	}{
		{`{"column":"id","op":"eq","value":9007199254740993}`, true},
		{`{"column":"id","op":"eq","value":9007199254740992}`, false},
		{`{"column":"id","op":"eq","value":9007199254740993.0}`, true},
		{`{"column":"id","op":"eq","value":"9007199254740993"}`, false}, // string vs number
		{`{"column":"flag","op":"eq","value":true}`, true},
		{`{"column":"flag","op":"eq","value":"true"}`, false},
		{`{"column":"flag","op":"eq","value":1}`, false},
		{`{"column":"note","op":"eq","value":null}`, true},
		{`{"column":"absent","op":"eq","value":null}`, false}, // missing != null
		{`{"column":"s","op":"eq","value":1}`, false},         // number vs string
		{`{"column":"s","op":"eq","value":"1"}`, true},
		{`{"column":"id","op":"eq","value":7}`, false}, // exact case: "ID" is a different field
		{`{"column":"ID","op":"eq","value":7}`, true},
		{`{"column":"o","op":"eq","value":1}`, false}, // non-scalar record value never matches a scalar
		{`{"column":"o","op":"eq","value":null}`, false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.matches, pred(tc.predicate).matchesRecord(record), "predicate %s", tc.predicate)
	}
}

func TestTopLevelFieldValueLastWins(t *testing.T) {
	// A repeated top-level key matches by its LAST occurrence, mirroring
	// what encoding/json's map decoding would observe.
	value, found := topLevelFieldValue(json.RawMessage(`{"x":1,"y":2,"x":3}`), "x")
	require.True(t, found)
	assert.Equal(t, json.RawMessage("3"), value)

	_, found = topLevelFieldValue(json.RawMessage(`{"x":1}`), "z")
	assert.False(t, found)

	// Nested occurrences are not top-level fields.
	_, found = topLevelFieldValue(json.RawMessage(`{"a":{"z":1}}`), "z")
	assert.False(t, found)
}

func TestPredicateDigestSemantics(t *testing.T) {
	pred := func(raw string) *scanPredicate {
		p, err := parseScanPredicate(raw)
		require.NoError(t, err)
		return p
	}
	digest := func(raw string) (byte, [32]byte) { return scanPredicateDigest(pred(raw)) }
	// Equal business semantics → equal digest (continuation compatible).
	d1flag, d1sum := digest(`{"column":"n","op":"eq","value":1}`)
	d2flag, d2sum := digest(`{ "value" : 1.0 , "op" : "eq" , "column" : "n" }`)
	d3flag, d3sum := digest(`{"column":"n","op":"eq","value":1e0}`)
	assert.Equal(t, d1flag, d2flag)
	assert.Equal(t, d1sum, d2sum)
	assert.Equal(t, d1flag, d3flag)
	assert.Equal(t, d1sum, d3sum)
	// Semantic change → different digest.
	for _, changed := range []string{
		`{"column":"n","op":"eq","value":2}`,
		`{"column":"m","op":"eq","value":1}`,
		`{"column":"n","op":"eq","value":"1"}`,
		`{"column":"n","op":"eq","value":null}`,
	} {
		flag, sum := digest(changed)
		assert.Equal(t, d1flag, flag)
		assert.NotEqual(t, d1sum, sum, "predicate %s must rebind the cursor", changed)
	}
	// Absent predicate is its own binding.
	flag, sum := scanPredicateDigest(nil)
	assert.Equal(t, byte(0), flag)
	assert.Equal(t, [32]byte{}, sum)
}

// --- s2 cursor codec ---

func TestScanCursorV2RoundTrip(t *testing.T) {
	for _, in := range []scanCursorV2{
		{datasetID: uuid.New(), branchID: uuid.New(), version: 3, txnID: uuid.New(), mode: scanCursorV2ModePath, predFlag: 1, predSum: [32]byte{9}, path: "part-00000.ndjson", offset: 42},
		{datasetID: uuid.New(), branchID: uuid.New(), version: 1, txnID: uuid.New(), mode: scanCursorV2ModeAllFiles, file: "b.ndjson", offset: 7},
		{datasetID: uuid.New(), branchID: uuid.New(), version: 1, txnID: uuid.New(), mode: scanCursorV2ModeAllFiles, file: "", offset: 0},
	} {
		out, err := decodeScanCursorV2(encodeScanCursorV2(in))
		require.NoError(t, err)
		assert.Equal(t, in, out)
	}
}

func TestScanCursorV2Rejects(t *testing.T) {
	valid := scanCursorV2{datasetID: uuid.New(), branchID: uuid.New(), version: 2, txnID: uuid.New(), mode: scanCursorV2ModePath, predFlag: 1, predSum: [32]byte{5}, path: "p.ndjson", offset: 3}
	encoded := encodeScanCursorV2(valid)
	first := encoded[0]
	if first == 'A' {
		first = 'B'
	} else {
		first = 'A'
	}

	// Structural rejections.
	for name, raw := range map[string]string{
		"not base64":     "!!!!",
		"wrong magic":    string(first) + encoded[1:],
		"truncated":      encoded[:len(encoded)-2],
		"trailing bytes": encoded + "AA",
	} {
		_, err := decodeScanCursorV2(raw)
		assert.Error(t, err, "cursor case %q", name)
	}

	// Field-level rejections via re-encoded mutations.
	mutate := func(mutateFunc func(*scanCursorV2)) string {
		bad := valid
		mutateFunc(&bad)
		return encodeScanCursorV2(bad)
	}
	for name, raw := range map[string]string{
		"zero version": mutate(func(c *scanCursorV2) { c.version = 0 }),
		"negative offset": mutate(func(c *scanCursorV2) { c.offset = -1 }),
		"unknown mode":  mutate(func(c *scanCursorV2) { c.mode = 9 }),
		"path mode no path": mutate(func(c *scanCursorV2) { c.path = "" }),
		"path too long": mutate(func(c *scanCursorV2) { c.path = strings.Repeat("x", 513) }),
		"allfiles start nonzero": mutate(func(c *scanCursorV2) { c.mode = scanCursorV2ModeAllFiles; c.path = ""; c.file = ""; c.offset = 5 }),
		"allfiles file too long": mutate(func(c *scanCursorV2) { c.mode = scanCursorV2ModeAllFiles; c.path = ""; c.file = strings.Repeat("x", 513) }),
	} {
		_, err := decodeScanCursorV2(raw)
		assert.Error(t, err, "cursor case %q", name)
	}

	// flag=1 with all-zero digest is rejected; flag=0 with a set digest is
	// rejected. Built by patching bytes of a mode-2 encoding (fixed layout
	// before the location).
	zeroDigest := encodeScanCursorV2(scanCursorV2{datasetID: uuid.New(), branchID: uuid.New(), version: 1, txnID: uuid.New(), mode: scanCursorV2ModeAllFiles})
	raw, err := base64.RawURLEncoding.DecodeString(zeroDigest)
	require.NoError(t, err)
	require.Equal(t, byte(0), raw[55])
	raw[55] = 1
	_, err = decodeScanCursorV2(base64.RawURLEncoding.EncodeToString(raw))
	assert.Error(t, err, "flag set with zero digest must be rejected")

	raw2, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	raw2[56] = 1 // set a digest byte while keeping flag=1 is fine; instead test flag=0
	raw2[55] = 0 // flag 0 but digest nonzero
	_, err = decodeScanCursorV2(base64.RawURLEncoding.EncodeToString(raw2))
	assert.Error(t, err, "flag clear with nonzero digest must be rejected")

	// Location length inconsistent with total length.
	raw3, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	binary.LittleEndian.PutUint16(raw3[88:90], 99)
	_, err = decodeScanCursorV2(base64.RawURLEncoding.EncodeToString(raw3))
	assert.Error(t, err, "location length mismatch must be rejected")
}

// --- segment scanner edges (deferral, budgets, EOF honesty) ---

func TestScanSegmentDefersRecordAndSetsEndOffset(t *testing.T) {
	// The first record exactly fills a small byte budget; the second is
	// deferred with endOffset at its start (not 0) so no bytes are re-read.
	content := `{"a":1}` + "\n" + `{"b":2}` + "\n"
	fs, key := scanFS(t, content)
	seg, err := scanNDJSONSegment(fs, key, int64(len(content)), 0, 10, 8, nil)
	require.NoError(t, err)
	require.False(t, seg.fileDone)
	require.Len(t, seg.rows, 1)
	assert.Equal(t, `{"a":1}`, string(seg.rows[0]))
	assert.Equal(t, int64(8), seg.consumed)
	assert.Equal(t, int64(8), seg.endOffset, "deferral must resume at the deferred record")
}

func TestScanSegmentByteBudgetOverflowOnResume(t *testing.T) {
	content := `{"a":1}` + "\n" + `{"b":222222}` + "\n" + `{"c":3}` + "\n"
	fs, key := scanFS(t, content)
	offset := int64(len(`{"a":1}` + "\n"))
	// Resume after record 1 with a budget that fits nothing new.
	seg, err := scanNDJSONSegment(fs, key, int64(len(content)), offset, 10, 1, nil)
	require.NoError(t, err)
	require.False(t, seg.fileDone)
	assert.Empty(t, seg.rows)
	assert.Equal(t, offset, seg.endOffset, "deferral at resume position must not rewind")
}

func TestScanSegmentNoTrailingNewline(t *testing.T) {
	content := `{"a":1}` + "\n" + `{"b":2}` // final record without newline
	fs, key := scanFS(t, content)
	seg, err := scanNDJSONSegment(fs, key, int64(len(content)), 0, 10, datasetScanMaxPageBytes, nil)
	require.NoError(t, err)
	require.True(t, seg.fileDone)
	assert.Len(t, seg.rows, 2)
	assert.Equal(t, int64(len(content)), seg.endOffset)
}

func TestScanSegmentEarlyEOFFailsPage(t *testing.T) {
	// Committed size says 100 bytes but the object is shorter: the upfront
	// stat itself fails the page.
	fs, key := scanFS(t, `{"a":1}`+"\n")
	_, err := scanNDJSONSegment(fs, key, 100, 0, 10, datasetScanMaxPageBytes, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_file_size_mismatch")
}

// shorteningFS simulates an object that SHRANK between the stat and the
// read: Seek(End) still reports the committed size, but Read delivers
// fewer bytes.
type shorteningFS struct {
	inner *storageabstraction.LocalBackingFS
	key   string
	hide  int64
}

func (f *shorteningFS) OpenLocalObject(key string) (io.ReadSeekCloser, error) {
	reader, err := f.inner.OpenLocalObject(key)
	if err != nil || key != f.key {
		return reader, err
	}
	return &shorteningReader{r: reader, remain: f.hide}, nil
}

type shorteningReader struct {
	r      io.ReadSeekCloser
	remain int64
}

func (s *shorteningReader) Read(p []byte) (int, error) {
	if s.remain <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > s.remain {
		p = p[:s.remain]
	}
	n, err := s.r.Read(p)
	s.remain -= int64(n)
	if s.remain <= 0 && err == nil {
		return n, io.EOF
	}
	return n, err
}

func (s *shorteningReader) Seek(off int64, whence int) (int64, error) { return s.r.Seek(off, whence) }
func (s *shorteningReader) Close() error                             { return s.r.Close() }

func TestScanSegmentMidReadShrinkFailsPage(t *testing.T) {
	// Stat passes (size matches the committed record) but the read window
	// ends early: the page must fail explicitly, never complete short.
	content := `{"a":1}` + "\n" + `{"b":2}` + "\n"
	inner, key := scanFS(t, content)
	opener := &shorteningFS{inner: inner, key: key, hide: 8} // only record 1 delivered
	_, err := scanNDJSONSegment(opener, key, int64(len(content)), 0, 10, datasetScanMaxPageBytes, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_file_size_mismatch", "short read must fail the page explicitly")
}

func TestScanSegmentPredicateFilter(t *testing.T) {
	content := `{"id":1}` + "\n" + `{"id":2}` + "\n" + `{"id":1}` + "\n"
	fs, key := scanFS(t, content)
	pred, err := parseScanPredicate(`{"column":"id","op":"eq","value":1}`)
	require.NoError(t, err)
	seg, err := scanNDJSONSegment(fs, key, int64(len(content)), 0, 10, datasetScanMaxPageBytes, pred)
	require.NoError(t, err)
	require.True(t, seg.fileDone)
	require.Len(t, seg.rows, 2)
	assert.Equal(t, `{"id":1}`, string(seg.rows[0]))
	assert.Equal(t, `{"id":1}`, string(seg.rows[1]))
	assert.Equal(t, int64(len(content)), seg.consumed, "skipped rows still count as consumed bytes")
}
