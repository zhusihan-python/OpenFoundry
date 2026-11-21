package xlsximp

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// ── tiny workbook builder ───────────────────────────────────────────────────

const ctXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>`

func workbookXML(sheets string) string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>` + sheets + `</sheets></workbook>`
}

func relsXML(triples ...string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`)
	for i := 0; i+2 < len(triples); i += 3 {
		fmt.Fprintf(&b, `<Relationship Id="%s" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/%s" Target="%s"/>`, triples[i], triples[i+1], triples[i+2])
	}
	b.WriteString(`</Relationships>`)
	return b.String()
}

func buildZip(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
	return buf.Bytes()
}

// baseWorkbook: sheet1 "说明" (worksheet), sheet2 "数据" (worksheet),
// sheet3 "图表" (chartsheet, excluded), sharedStrings sst[0..2].
func baseWorkbook(t *testing.T, sheet1, sheet2, sst string) []byte {
	t.Helper()
	return buildZip(t, map[string]string{
		"[Content_Types].xml": ctXML,
		"xl/workbook.xml": workbookXML(
			`<sheet name="说明" sheetId="1" r:id="rId1"/><sheet name="数据" sheetId="2" r:id="rId2"/><sheet name="图表" sheetId="3" r:id="rId3"/>`),
		"xl/_rels/workbook.xml.rels": relsXML(
			"rId1", "worksheet", "worksheets/sheet1.xml",
			"rId2", "worksheet", "worksheets/sheet2.xml",
			"rId3", "chartsheet", "chartsheets/chart1.xml"),
		"xl/worksheets/sheet1.xml": sheetXML(sheet1),
		"xl/worksheets/sheet2.xml": sheetXML(sheet2),
		"xl/chartsheets/chart1.xml": `<chartsheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"/>`,
		"xl/sharedStrings.xml":     sstXML(sst),
	})
}

func sheetXML(sheetData string) string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>` + sheetData + `</sheetData></worksheet>`
}

func sstXML(items string) string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="3" uniqueCount="3">` + items + `</sst>`
}

func intPtr(v int) *int { return &v }

func wantKind(t *testing.T, err error, kind string, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected %s error, got nil", context, kind)
	}
	if got := KindOf(err); got != kind {
		t.Fatalf("%s: expected kind %s, got %q (%v)", context, kind, got, err)
	}
}

// ── inspect ─────────────────────────────────────────────────────────────────

func TestInspectListsWorksheetsOnly(t *testing.T) {
	data := baseWorkbook(t, `<row r="1"><c r="A1" t="s"><v>0</v></c></row>`,
		`<row r="1"><c r="B1"><v>1</v></c></row>`,
		`<si><t>说明文字</t></si>`)
	out, err := Inspect(data, "report.xlsx")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(out.Sheets) != 2 {
		t.Fatalf("expected 2 worksheets (chart sheet excluded), got %+v", out.Sheets)
	}
	if out.Sheets[0].Name != "说明" || out.Sheets[1].Name != "数据" {
		t.Fatalf("unexpected sheet order: %+v", out.Sheets)
	}
	if len(out.SHA256) != 64 || out.SizeBytes != int64(len(data)) || out.MaxDataRows != MaxDataRows {
		t.Fatalf("unexpected inspect result: %+v", out)
	}
}

// ── normalization: header, mapping, values ──────────────────────────────────

// 数据 sheet: rows 1-2 说明, row 3 header with a duplicate and a blank
// header, rows 4-6 data with blanks, rich text, cached formula, and a
// positional value in column C behind two blank columns.
func normalWorkbook(t *testing.T) []byte {
	sst := `<si><t>名称</t></si>` + // 0
		`<si><r><rPr><b/></rPr><t>富</t></r><r><t>文本</t></r></si>` + // 1 rich
		`<si><t>名称</t></si>` // 2 duplicate of 0
	sheet := strings.Join([]string{
		`<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1"><v>注: 中文工作簿</v></c></row>`,
		`<row r="2"><c r="A2" t="s"><v>2</v></c></row>`,
		// header row 3: 名称, 名称 (dup), blank, 数量
		`<row r="3"><c r="A3" t="s"><v>0</v></c><c r="B3" t="s"><v>2</v></c><c r="C3"/><c r="D3" t="s"><v>0</v></c></row>`,
		// data row 4: name, rich text, positional C4 value, formula cached 42
		`<row r="4"><c r="A4" t="s"><v>0</v></c><c r="B4" t="s"><v>1</v></c><c r="C4"><v>pos-c</v></c><c r="D4"><f>SUM(1,41)</f><v>42</v></c></row>`,
		// data row 5: blank A, cached-empty formula B5, inline string C5
		`<row r="5"><c r="B5"><f>A4&amp;1</f><v></v></c><c r="C5" t="inlineStr"><is><t>内联</t></is></c></row>`,
		// data row 6: shared formula master (D6 with cache) and member (E6 with cache)
		`<row r="6"><c r="D6"><f t="shared" ref="D6:E6" si="0">D4+1</f><v>43</v></c><c r="E6"><f t="shared" si="0"/><v>44</v></c></row>`,
	}, "")
	return baseWorkbook(t, `<row r="1"><c r="A1"><v>1</v></c></row>`, sheet, sst)
}

func TestNormalizeChineseHeaderAtRow3WithDuplicateAndBlankHeaders(t *testing.T) {
	cfg := Config{
		Sheet:     "数据",
		HeaderRow: intPtr(3),
		Mapping: []ColumnMap{
			{SourceIndex: 0, Target: "left_name"},
			{SourceIndex: 1, Target: "right_name"},
			{SourceIndex: 2, Target: "pos"},
			{SourceIndex: 3, Target: "qty"},
		},
	}
	out, err := Normalize(normalWorkbook(t), cfg, 10)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if out.RowCount != 3 {
		t.Fatalf("expected 3 data rows, got %d", out.RowCount)
	}
	if fmt.Sprint(out.Columns) != "[left_name pos qty right_name]" {
		t.Fatalf("columns not sorted: %v", out.Columns)
	}
	// Source columns describe the physical header: duplicate 名称, blank, 名称.
	if out.SourceColumns[0].Header != "名称" || out.SourceColumns[1].Header != "名称" || out.SourceColumns[2].Header != "" {
		t.Fatalf("unexpected source columns: %+v", out.SourceColumns)
	}
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(out.NDJSON())), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad ndjson line %q: %v", line, err)
		}
		rows = append(rows, m)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 ndjson rows, got %d", len(rows))
	}
	if rows[0]["left_name"] != "名称" || rows[0]["right_name"] != "富文本" || rows[0]["pos"] != "pos-c" || rows[0]["qty"] != "42" {
		t.Fatalf("row 4 values: %+v", rows[0])
	}
	if rows[1]["left_name"] != nil {
		t.Fatalf("blank A5 must be null: %+v", rows[1])
	}
	if rows[1]["right_name"] != nil {
		t.Fatalf("cached-empty formula B5 must be null: %+v", rows[1])
	}
	if rows[1]["pos"] != "内联" {
		t.Fatalf("inline string C5: %+v", rows[1])
	}
	if rows[2]["qty"] != "43" {
		t.Fatalf("shared formula master value: %+v", rows[2])
	}
	// E6 is beyond the mapping; its shared formula value is irrelevant.
	if len(out.SampleRows) != 3 {
		t.Fatalf("sample rows: %d", len(out.SampleRows))
	}
}

func TestNormalizeNoHeaderKeepsFirstRowAsData(t *testing.T) {
	cfg := Config{
		Sheet:     "数据",
		HeaderRow: nil,
		Mapping:   []ColumnMap{{SourceIndex: 0, Target: "a"}, {SourceIndex: 3, Target: "d"}},
	}
	out, err := Normalize(normalWorkbook(t), cfg, 5)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if out.RowCount != 6 { // rows 1..6 are all data without a header
		t.Fatalf("expected 6 data rows without header, got %d", out.RowCount)
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(strings.SplitN(string(out.NDJSON()), "\n", 2)[0]), &first); err != nil {
		t.Fatalf("first line: %v", err)
	}
	if first["a"] != "名称" || first["d"] != nil {
		t.Fatalf("first data row (physical row 1): %+v", first)
	}
}

func TestNormalizeSwappedAndSameTargets(t *testing.T) {
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{
		{SourceIndex: 1, Target: "alpha"}, {SourceIndex: 0, Target: "beta"},
	}}
	out, err := Normalize(normalWorkbook(t), cfg, 2)
	if err != nil {
		t.Fatalf("normalize swapped targets: %v", err)
	}
	var first map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(string(out.NDJSON()), "\n", 2)[0]), &first)
	if first["alpha"] != "富文本" || first["beta"] != "名称" {
		t.Fatalf("swapped mapping must follow physical index: %+v", first)
	}
	dup := Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{
		{SourceIndex: 0, Target: "x"}, {SourceIndex: 1, Target: " x "},
	}}
	wantKind(t, func() error { _, err := Normalize(normalWorkbook(t), dup, 1); return err }(), ErrMapping, "duplicate target after trim")
}

// ── formula cache semantics ─────────────────────────────────────────────────

func TestUncachedFormulaRejectedWithCellRef(t *testing.T) {
	sheet := `<row r="1"><c r="A1"><v>h</v></c><c r="B1"><v>h2</v></c></row>` +
		`<row r="2"><c r="A2"><v>1</v></c><c r="B2"><f>A2*2</f></c></row>` + // B2 uncached
		`<row r="3"><c r="A3"><v>2</v></c><c r="C3"><f>A3*2</f></c></row>` // C3 uncached but unmapped
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{
		{SourceIndex: 0, Target: "a"}, {SourceIndex: 1, Target: "b"},
	}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrFormula, "uncached mapped formula")
	if !strings.Contains(err.Error(), "B2") {
		t.Fatalf("error must name the cell, got %v", err)
	}
}

func TestSharedFormulaMemberWithoutCacheRejected(t *testing.T) {
	sheet := `<row r="1"><c r="A1"><v>h</v></c></row>` +
		`<row r="2"><c r="A2"><f t="shared" ref="A2:A3" si="0">1+1</f><v>2</v></c></row>` +
		`<row r="3"><c r="A3"><f t="shared" si="0"/></c></row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	_, err := Normalize(data, cfg, 5)
	wantKind(t, err, ErrFormula, "shared formula member without cache")
}

// ── ST_Xstring escapes ──────────────────────────────────────────────────────

func TestDecodeXString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"a_x000D_b", "a\rb"},
		{"_x005F_x000D_", "_x000D_"}, // escaped underscore keeps the next escape literal
		{"_x005F_plain", "_plain"},
		{"_xD83D__xDE00_", "\U0001F600"}, // surrogate pair combines
		{"_x", "_x"},
		{"_xZZZZ_", "_xZZZZ_"},
	}
	for _, c := range cases {
		got, err := decodeXString(c.in)
		if err != nil {
			t.Fatalf("decode %q: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("decode %q = %q, want %q", c.in, got, c.want)
		}
	}
	if _, err := decodeXString("_xD83D_"); err == nil {
		t.Fatal("unpaired high surrogate must error")
	}
	if _, err := decodeXString("_xDC00_"); err == nil {
		t.Fatal("unpaired low surrogate must error")
	}
}

func TestNormalizeDecodesSharedStringEscapesAndSkipsPhonetics(t *testing.T) {
	sst := `<si><t>_x005F_x000D_</t></si>` + // 0 literal _x000D_
		`<si><t>か</t><rPh><t>カ</t></rPh><r><t>ん</t></r></si>` // 1 phonetic run skipped -> かん
	sheet := `<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>`
	data := baseWorkbook(t, "", sheet, sst)
	cfg := Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{
		{SourceIndex: 0, Target: "esc"}, {SourceIndex: 1, Target: "ph"},
	}}
	out, err := Normalize(data, cfg, 1)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var first map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(string(out.NDJSON()), "\n", 2)[0]), &first)
	if first["esc"] != "_x000D_" {
		t.Fatalf("escape literal: %+v", first)
	}
	if first["ph"] != "かん" {
		t.Fatalf("phonetic run must be skipped: %+v", first)
	}
}

// ── structural strictness ───────────────────────────────────────────────────

func TestStructuralRejections(t *testing.T) {
	makeWB := func(sheet string) []byte { return baseWorkbook(t, "", sheet, "") }
	cfgOf := func() Config {
		return Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	}
	cases := []struct {
		name   string
		sheet  string
		kind   string
	}{
		{"duplicate cell", `<row r="1"><c r="A1"><v>1</v></c><c r="A1"><v>2</v></c></row>`, ErrInvalid},
		{"duplicate row", `<row r="1"><c r="A1"><v>1</v></c></row><row r="1"><c r="A1"><v>2</v></c></row>`, ErrInvalid},
		{"cell row mismatch", `<row r="4"><c r="A9"><v>1</v></c></row>`, ErrInvalid},
		{"invalid cell ref", `<row r="1"><c r="0A"><v>1</v></c></row>`, ErrInvalid},
		{"row attr non-numeric", `<row r="abc"><c r="A1"><v>1</v></c></row>`, ErrInvalid},
		{"sparse content row", `<row r="1"><c r="A1"><v>h</v></c></row><row r="2000"><c r="A2000"><v>x</v></c></row>`, ErrRowLimit},
		{"column beyond max", `<row r="1"><c r="` + columnName(MaxSourceColumns+1) + `1"><v>x</v></c></row>`, ErrRowLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Normalize(makeWB(tc.sheet), cfgOf(), 1)
			wantKind(t, err, tc.kind, tc.name)
		})
	}
}

func TestSequentialCoordinatesInferredOnlyWhenRAbsent(t *testing.T) {
	sheet := `<row><c><v>a</v></c><c><v>b</v></c></row><row><c><v>c</v></c></row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{
		{SourceIndex: 1, Target: "second"},
	}}
	out, err := Normalize(data, cfg, 5)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var first map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(string(out.NDJSON()), "\n", 2)[0]), &first)
	if first["second"] != "b" {
		t.Fatalf("second cell of inferred row must map by position: %+v", first)
	}
}

// ── limits and bombs ────────────────────────────────────────────────────────

func TestMoreThanMaxDataRowsRejected(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<row r="1"><c r="A1"><v>h</v></c></row>`)
	for i := 2; i <= MaxDataRows+2; i++ {
		fmt.Fprintf(&b, `<row r="%d"><c r="A%d"><v>%d</v></c></row>`, i, i, i)
	}
	data := baseWorkbook(t, "", b.String(), "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrRowLimit, "more than 1000 data rows")
}

func TestRowElementFloodRejected(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= maxRowElements+1; i++ {
		fmt.Fprintf(&b, `<row r="%d"/>`, i)
	}
	data := baseWorkbook(t, "", b.String(), "")
	_, err := Normalize(data, Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}, 1)
	wantKind(t, err, ErrRowLimit, "row element flood")
}

func TestZipEntryRejections(t *testing.T) {
	base := func() map[string]string {
		return map[string]string{
			"[Content_Types].xml":       ctXML,
			"xl/workbook.xml":           workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
			"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "worksheets/sheet1.xml"),
			"xl/worksheets/sheet1.xml":  sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
		}
	}
	t.Run("macro entry", func(t *testing.T) {
		e := base()
		e["xl/vbaProject.bin"] = "vba"
		_, err := Inspect(buildZip(t, e), "m.xlsm")
		wantKind(t, err, ErrMacro, "vbaProject.bin")
	})
	t.Run("external links", func(t *testing.T) {
		e := base()
		e["xl/externalLinks/externalLink1.xml"] = "<el/>"
		_, err := Inspect(buildZip(t, e), "x.xlsx")
		wantKind(t, err, ErrExternalLinks, "externalLinks")
	})
	t.Run("encrypted", func(t *testing.T) {
		e := base()
		e["EncryptionInfo"] = "x"
		_, err := Inspect(buildZip(t, e), "x.xlsx")
		wantKind(t, err, ErrEncrypted, "EncryptionInfo")
	})
	t.Run("macro content type", func(t *testing.T) {
		e := base()
		e["[Content_Types].xml"] = strings.Replace(ctXML, "sheet.main+xml\"", "sheet.macroEnabled.main+xml\"", 1)
		_, err := Inspect(buildZip(t, e), "x.xlsm")
		wantKind(t, err, ErrMacro, "macroEnabled content type")
	})
	t.Run("inert vba default mapping accepted", func(t *testing.T) {
		// Real producers (excelize) unconditionally declare a Default for
		// .bin; without an actual xl/vbaProject.bin entry it is inert.
		e := base()
		e["[Content_Types].xml"] = strings.Replace(ctXML, "</Types>",
			`<Default Extension="bin" ContentType="application/vnd.ms-office.vbaproject"/></Types>`, 1)
		if _, err := Inspect(buildZip(t, e), "x.xlsx"); err != nil {
			t.Fatalf("inert vbaproject default must be accepted: %v", err)
		}
	})
	t.Run("real vba part under custom name via override rejected", func(t *testing.T) {
		e := base()
		e["xl/custom.bin"] = "vba-bytes"
		e["[Content_Types].xml"] = strings.Replace(ctXML, "</Types>",
			`<Override PartName="/xl/custom.bin" ContentType="application/vnd.ms-office.vbaProject"/></Types>`, 1)
		_, err := Inspect(buildZip(t, e), "x.xlsx")
		wantKind(t, err, ErrMacro, "actual VBA part with custom name")
	})
	t.Run("real vba part via default extension rejected", func(t *testing.T) {
		// The Default mapping is inert only while no matching entry exists;
		// an actual .bin part makes it effective and rejects the workbook.
		e := base()
		e["xl/payload.bin"] = "vba-bytes"
		e["[Content_Types].xml"] = strings.Replace(ctXML, "</Types>",
			`<Default Extension="bin" ContentType="application/vnd.ms-office.vbaproject"/></Types>`, 1)
		_, err := Inspect(buildZip(t, e), "x.xlsx")
		wantKind(t, err, ErrMacro, "actual .bin part under vba default")
	})
	t.Run("entry count", func(t *testing.T) {
		e := base()
		for i := 0; i < MaxZipEntries; i++ {
			e[fmt.Sprintf("filler/f%04d.txt", i)] = "x"
		}
		_, err := Inspect(buildZip(t, e), "x.xlsx")
		wantKind(t, err, ErrZipUnsafe, "entry count")
	})
	t.Run("missing workbook override", func(t *testing.T) {
		e := base()
		e["[Content_Types].xml"] = `<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/></Types>`
		_, err := Inspect(buildZip(t, e), "x.xlsx")
		wantKind(t, err, ErrInvalid, "missing workbook override")
	})
	t.Run("declared bomb entry", func(t *testing.T) {
		if _, err := Inspect(fakeHugeEntryZip(), "bomb.xlsx"); err == nil {
			t.Fatal("declared-huge entry must be rejected before decompression")
		} else if KindOf(err) != ErrZipUnsafe {
			t.Fatalf("expected zip-unsafe, got %v", err)
		}
	})
}

// fakeHugeEntryZip builds a real one-entry zip and patches the central
// directory to declare a 64MiB uncompressed size, proving the precheck
// reads declarations instead of decompressing.
func fakeHugeEntryZip() []byte {
	buf := buildZipForPatch()
	target := "xl/worksheets/sheet1.xml"
	// Central header: sig(4) verMade(2) verNeed(2) flags(2) method(2)
	// time(2) date(2) crc(4) compSize(4) uncompSize(4) nameLen(2)...
	for p := 0; p+46 <= len(buf); p++ {
		if string(buf[p:p+4]) != "PK\x01\x02" {
			continue
		}
		nameLen := int(buf[p+28]) | int(buf[p+29])<<8
		if p+46+nameLen > len(buf) || string(buf[p+46:p+46+nameLen]) != target {
			continue
		}
		buf[p+20], buf[p+21], buf[p+22], buf[p+23] = 0, 0, 0, 4 // compressed 64MiB
		buf[p+24], buf[p+25], buf[p+26], buf[p+27] = 0, 0, 0, 4 // uncompressed 64MiB
	}
	return buf
}

func buildZipForPatch() []byte {
	entries := map[string]string{
		"[Content_Types].xml":        ctXML,
		"xl/workbook.xml":            workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
		"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "worksheets/sheet1.xml"),
		"xl/worksheets/sheet1.xml":   sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
	}
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	for name, body := range entries {
		w, _ := zw.Create(name)
		_, _ = w.Write([]byte(body))
	}
	_ = zw.Close()
	return out.Bytes()
}

// ── output budget ───────────────────────────────────────────────────────────

func bigCellWorkbook(t *testing.T, cols, cellSize int) []byte {
	t.Helper()
	cell := strings.Repeat("a", cellSize)
	var b strings.Builder
	b.WriteString(`<row r="1">`)
	for i := 0; i < cols; i++ {
		fmt.Fprintf(&b, `<c r="%s1" t="inlineStr"><is><t>%s</t></is></c>`, columnName(i+1), cell)
	}
	b.WriteString(`</row><row r="2">`)
	for i := 0; i < cols; i++ {
		fmt.Fprintf(&b, `<c r="%s2" t="inlineStr"><is><t>%s</t></is></c>`, columnName(i+1), cell)
	}
	b.WriteString(`</row>`)
	return baseWorkbook(t, "", b.String(), "")
}

func TestNDJSONLineBudgetRejectsBeforeAllocation(t *testing.T) {
	// Two mapped columns of 600KB each: no single string is oversized, but
	// the combined row exceeds the 1MiB line budget — rejected before the
	// row is ever materialized.
	data := bigCellWorkbook(t, 2, 600*1024)
	cfg := Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{
		{SourceIndex: 0, Target: "a"}, {SourceIndex: 1, Target: "b"},
	}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrOutputLimit, "combined row over line budget")
}

func TestNDJSONSingleOversizedCellRejected(t *testing.T) {
	data := bigCellWorkbook(t, 1, 1200*1024)
	cfg := Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrOutputLimit, "single oversized cell")
}

func TestEscapedLengthUpperBoundCoversJSON(t *testing.T) {
	samples := []string{"plain", "quote\"back\\slash", "ctrl\u0001\u001f", "html<&>", "中文é\U0001F600", strings.Repeat("x\x01\"", 1000)}
	for _, s := range samples {
		enc, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal %q: %v", s, err)
		}
		// json.Marshal includes the surrounding quotes; the bound excludes them.
		if got := 2 + jsonEscapedLenUpperBound(s); got < len(enc) {
			t.Fatalf("bound %d < actual %d for %q", got, len(enc), s)
		}
	}
}

// ── shared string indices ───────────────────────────────────────────────────

func TestSharedStringIndexOutOfRangeFails(t *testing.T) {
	sheet := `<row r="1"><c r="A1" t="s"><v>99</v></c></row>`
	data := baseWorkbook(t, "", sheet, `<si><t>x</t></si>`)
	cfg := Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrInvalid, "sst index out of range")
	neg := `<row r="1"><c r="A1" t="s"><v>-1</v></c></row>`
	data = baseWorkbook(t, "", neg, `<si><t>x</t></si>`)
	_, err = Normalize(data, cfg, 1)
	wantKind(t, err, ErrInvalid, "negative sst index")
}

// ── configuration validation ────────────────────────────────────────────────

func TestConfigValidation(t *testing.T) {
	data := normalWorkbook(t)
	valid := Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	if _, err := Normalize(data, valid, 1); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	cases := []struct {
		name string
		cfg  Config
		kind string
	}{
		{"missing sheet", Config{Sheet: "", HeaderRow: intPtr(1), Mapping: valid.Mapping}, ErrConfig},
		{"unknown sheet", Config{Sheet: "不存在", HeaderRow: intPtr(1), Mapping: valid.Mapping}, ErrSheet},
		{"chart sheet", Config{Sheet: "图表", HeaderRow: intPtr(1), Mapping: valid.Mapping}, ErrSheet},
		{"header zero", Config{Sheet: "数据", HeaderRow: intPtr(0), Mapping: valid.Mapping}, ErrHeader},
		{"header too deep", Config{Sheet: "数据", HeaderRow: intPtr(MaxHeaderRow + 1), Mapping: valid.Mapping}, ErrHeader},
		{"empty mapping array", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{}}, ErrMapping},
		{"dup source", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{0, "a"}, {0, "b"}}}, ErrMapping},
		{"empty target", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{0, "  "}}}, ErrMapping},
		{"dup target", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{0, "a"}, {1, "a"}}}, ErrMapping},
		{"source out of range", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{MaxSourceColumns, "a"}}}, ErrMapping},
		{"source beyond width", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{9, "a"}}}, ErrMapping},
		{"target too long", Config{Sheet: "数据", HeaderRow: intPtr(3), Mapping: []ColumnMap{{0, strings.Repeat("t", MaxTargetNameLen+1)}}}, ErrMapping},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Normalize(data, tc.cfg, 1)
			wantKind(t, err, tc.kind, tc.name)
		})
	}
}

// ── misc ────────────────────────────────────────────────────────────────────

func TestSuggestedLogicalPath(t *testing.T) {
	cases := map[string]string{
		"report.xlsx":  "report.ndjson",
		"a/b/c.xlsx":   "c.ndjson",
		"":             "workbook.ndjson",
		"weird.XLSX":   "weird.ndjson",
		"noext":        "noext.ndjson",
	}
	for in, want := range cases {
		if got := SuggestedLogicalPath(in); got != want {
			t.Fatalf("SuggestedLogicalPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRelationshipTargetTraversalFails(t *testing.T) {
	e := map[string]string{
		"[Content_Types].xml":        ctXML,
		"xl/workbook.xml":            workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
		"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "../../evil.xml"),
		"xl/worksheets/sheet1.xml":   sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
	}
	_, err := Inspect(buildZip(t, e), "x.xlsx")
	wantKind(t, err, ErrInvalid, "path traversal target")
}

// ── round-3 semantic regressions ────────────────────────────────────────────

func TestDecodeXStringEscapedUnderscoreForms(t *testing.T) {
	cases := []struct{ in, want string }{
		{"_x005F_x000D_", "_x000D_"}, // shared underscore: literal escape text
		{"_x005F__x000D_", "_\r"},    // independent escapes: underscore + CR
		{"_x005F_plain", "_plain"},
	}
	for _, c := range cases {
		got, err := decodeXString(c.in)
		if err != nil {
			t.Fatalf("decode %q: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("decode %q = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCachedFormulaStringDecodesXString(t *testing.T) {
	sheet := `<row r="1"><c r="A1"><v>h</v></c></row>` +
		`<row r="2"><c r="A2" t="str"><f>X1</f><v>_x005F_x000D_</v></c></row>` +
		`<row r="3"><c r="A3" t="str"><f>X2</f><v>a_x000D_b</v></c></row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	out, err := Normalize(data, cfg, 5)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out.NDJSON())), "\n")
	var r2, r3 map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &r2)
	_ = json.Unmarshal([]byte(lines[1]), &r3)
	if r2["a"] != "_x000D_" {
		t.Fatalf("t=str cached literal escape: %+v", r2)
	}
	if r3["a"] != "a\rb" {
		t.Fatalf("t=str cached decoded escape: %+v", r3)
	}
}

func TestInlineRichTextAndPhoneticsExcluded(t *testing.T) {
	sheet := `<row r="1">` +
		`<c r="A1" t="inlineStr"><is><r><rPr><b/></rPr><t>富</t></r><r><t>文</t></r></is></c>` +
		`<c r="B1" t="inlineStr"><is><t>か</t><rPh sb="0" eb="1"><t>カ</t></rPh><r><t>ん</t></r></is></c>` +
		`</row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: nil, Mapping: []ColumnMap{
		{SourceIndex: 0, Target: "rich"}, {SourceIndex: 1, Target: "ph"},
	}}
	out, err := Normalize(data, cfg, 1)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var first map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(string(out.NDJSON()), "\n", 2)[0]), &first)
	if first["rich"] != "富文" {
		t.Fatalf("inline rich runs: %+v", first)
	}
	if first["ph"] != "かん" {
		t.Fatalf("inline phonetic run must be skipped: %+v", first)
	}
}

func TestRowInferenceContinuesAfterExplicitRow(t *testing.T) {
	sheet := `<row r="1"><c r="A1"><v>h</v></c></row>` +
		`<row r="5"><c r="A5"><v>five</v></c></row>` +
		`<row><c r="A6"><v>six</v></c></row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	out, err := Normalize(data, cfg, 5)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if out.RowCount != 2 {
		t.Fatalf("rows 5 and inferred 6 must both be data rows, got %d", out.RowCount)
	}
	lines := strings.Split(strings.TrimSpace(string(out.NDJSON())), "\n")
	var second map[string]any
	_ = json.Unmarshal([]byte(lines[1]), &second)
	if second["a"] != "six" {
		t.Fatalf("inferred row after explicit r=5 must be row 6: %+v", second)
	}
}

func TestEmptyCellOccupiesCoordinate(t *testing.T) {
	sheet := `<row r="1"><c r="A1"/><c r="B1"><v>h</v></c></row>` +
		`<row r="2"><c r="A2"/><c r="A2"><v>x</v></c></row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrInvalid, "valued cell at an already-occupied coordinate")
}

func TestEmptyCellAloneIsBlank(t *testing.T) {
	sheet := `<row r="1"><c r="A1"/><c r="B1"><v>h</v></c></row>` +
		`<row r="2"><c r="A2"/><c r="B2"><v>x</v></c></row>`
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{
		{SourceIndex: 0, Target: "a"}, {SourceIndex: 1, Target: "b"},
	}}
	out, err := Normalize(data, cfg, 5)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var first map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(string(out.NDJSON()), "\n", 2)[0]), &first)
	if first["a"] != nil || first["b"] != "x" {
		t.Fatalf("empty cell must stay null: %+v", first)
	}
}

func TestHeaderOnlySheetWithMappingRejected(t *testing.T) {
	sheet := `<row r="1"><c r="A1"><v>h</v></c></row>` // header present, no data
	data := baseWorkbook(t, "", sheet, "")
	cfg := Config{Sheet: "数据", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "a"}}}
	_, err := Normalize(data, cfg, 1)
	wantKind(t, err, ErrSheet, "header-only sheet must not import")
}

func TestDuplicateRelIDRejected(t *testing.T) {
	e := map[string]string{
		"[Content_Types].xml": ctXML,
		"xl/workbook.xml":     workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
		"xl/_rels/workbook.xml.rels": `<?xml version="1.0"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
</Relationships>`,
		"xl/worksheets/sheet1.xml": sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
	}
	_, err := Inspect(buildZip(t, e), "x.xlsx")
	wantKind(t, err, ErrInvalid, "duplicate relationship id")
}

func TestNonExactWorksheetRelationshipTypeNotSelectable(t *testing.T) {
	e := map[string]string{
		"[Content_Types].xml": ctXML,
		"xl/workbook.xml":     workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
		"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheets", "worksheets/sheet1.xml"), // plural: not the official type
		"xl/worksheets/sheet1.xml":   sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
	}
	out, err := Inspect(buildZip(t, e), "x.xlsx")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(out.Sheets) != 0 {
		t.Fatalf("non-official relationship type must not be selectable: %+v", out.Sheets)
	}
}

func TestCaseDuplicatePartNameRejected(t *testing.T) {
	e := map[string]string{
		"[Content_Types].xml":       ctXML,
		"xl/workbook.xml":           workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
		"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "worksheets/sheet1.xml"),
		"xl/worksheets/sheet1.xml":  sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
		"XL/WORKSHEETS/SHEET1.XML":  sheetXML(`<row r="1"><c r="A1"><v>2</v></c></row>`),
	}
	_, err := Inspect(buildZip(t, e), "x.xlsx")
	wantKind(t, err, ErrInvalid, "case-duplicate part name")
}

func TestWrongRootNamespacesRejected(t *testing.T) {
	mutateWB := func(ns string) map[string]string {
		e := map[string]string{
			"[Content_Types].xml":        ctXML,
			"xl/workbook.xml":            `<wb xmlns="` + ns + `"><sheets><sheet name="S" sheetId="1" r:id="rId1"/></sheets></wb>`,
			"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "worksheets/sheet1.xml"),
			"xl/worksheets/sheet1.xml":   sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
		}
		return e
	}
	if _, err := Inspect(buildZip(t, mutateWB("http://example.com/other")), "x.xlsx"); err == nil {
		t.Fatal("wrong workbook namespace must be rejected")
	} else {
		wantKind(t, err, ErrInvalid, "wrong workbook root namespace")
	}
	e := map[string]string{
		"[Content_Types].xml": ctXML,
		"xl/workbook.xml":     workbookXML(`<sheet name="S" sheetId="1" r:id="rId1"/>`),
		"xl/_rels/workbook.xml.rels": `<?xml version="1.0"?>
<Relationships xmlns="http://example.com/other"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>`,
		"xl/worksheets/sheet1.xml": sheetXML(`<row r="1"><c r="A1"><v>1</v></c></row>`),
	}
	if _, err := Inspect(buildZip(t, e), "x.xlsx"); err == nil {
		t.Fatal("wrong rels namespace must be rejected")
	}
}

func TestNormalizeSnapshotBindsTheWorkbookPartType(t *testing.T) {
	cfg := Config{Sheet: "模板", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "order_no"}}}
	fixture := func(types string) []byte {
		return buildZip(t, map[string]string{
			"[Content_Types].xml":        types,
			"xl/workbook.xml":            workbookXML(`<sheet name="模板" sheetId="1" r:id="rId1"/>`),
			"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "worksheets/sheet1.xml"),
			"xl/worksheets/sheet1.xml": sheetXML(
				`<row r="1"><c r="A1" t="inlineStr"><is><t>编号</t></is></c></row>` +
					`<row r="4"><c r="A4" t="inlineStr"><is><t>007</t></is></c></row>`),
		})
	}
	templateTypes := strings.Replace(ctXML, xlsxWorkbookType, xltxWorkbookType, 1)
	data := fixture(templateTypes)
	result, err := NormalizeSnapshot(data, cfg, "xltx")
	if err != nil {
		t.Fatal(err)
	}
	if string(result.NDJSON()) != "{\"order_no\":\"007\"}\n" ||
		len(result.PhysicalRows) != 1 || result.PhysicalRows[0] != 4 {
		t.Fatalf("unexpected template rows %s at %v", result.NDJSON(), result.PhysicalRows)
	}
	_, err = NormalizeSnapshot(fixture(ctXML), cfg, "xltx")
	wantKind(t, err, ErrInvalid, "XLSX renamed to XLTX")
	_, err = NormalizeSnapshot(data, cfg, "xlsx")
	wantKind(t, err, ErrInvalid, "XLTX renamed to XLSX")
	_, err = Normalize(data, cfg, 0)
	wantKind(t, err, ErrInvalid, "template on standard upload normalization")
	_, err = Inspect(data, "renamed.xlsx")
	wantKind(t, err, ErrInvalid, "template on standard upload inspection")

	extraType := `<Override PartName="/xl/other.xml" ContentType="` + xltxWorkbookType + `"/></Types>`
	wrongPart := strings.Replace(ctXML, "</Types>", extraType, 1)
	_, err = NormalizeSnapshot(fixture(wrongPart), cfg, "xltx")
	wantKind(t, err, ErrInvalid, "template content type attached to unrelated part")

	duplicate := `<Override PartName="/xl/workbook.xml" ContentType="` + xltxWorkbookType + `"/></Types>`
	_, err = NormalizeSnapshot(fixture(strings.Replace(ctXML, "</Types>", duplicate, 1)), cfg, "xltx")
	wantKind(t, err, ErrInvalid, "conflicting workbook part overrides")

	macro := strings.Replace(templateTypes, xltxWorkbookType, "application/vnd.ms-excel.template.macroEnabled.main+xml", 1)
	_, err = NormalizeSnapshot(fixture(macro), cfg, "xltx")
	wantKind(t, err, ErrMacro, "macro-enabled template disguised as XLTX")
}
