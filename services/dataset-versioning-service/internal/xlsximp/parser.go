// Package xlsximp is the bounded XLSX reader for the P3.1 Excel sheet/header
// mapping import. Preview and final import share one validation and
// normalization path (Normalize), so the two can never disagree about what a
// configuration means.
//
// Implementation notes (Root reviews 2026-11-16): pure standard library —
// archive/zip plus a single streaming encoding/xml pass per selected
// worksheet. No library cell APIs that materialize whole worksheets are
// used: worksheet-vs-chart filtering reads only lightweight package
// relationship metadata, and formula/cached-value existence (hasF / hasV,
// including shared and self-closing formulas) is recorded by the same
// bounded scan that collects values. Structural problems fail closed
// (invalid or inconsistent coordinates, duplicate rows/cells, CRC or
// truncation errors, path traversal in relationship targets, macro or
// mismatched workbook content types); sequential coordinates are inferred only when
// the r attribute is entirely absent.
//
// String semantics match OpenXML ST_Xstring: shared and inline text
// concatenates direct <t> and rich <r><t> runs only (phonetic rPh runs are
// excluded), original whitespace is preserved, and _xHHHH_ escapes decode in
// a single pass — _x005F_ marks the following escape as literal text,
// surrogate pairs combine, and unpaired surrogates are rejected.
//
// Semantics fixed for P3.1: cell values are raw stored strings (numbers and
// dates keep their serial form); blank cells become JSON null — a fixed
// behavior for this phase, not the full P3.2 null-value rule set. A mapped
// formula cell with <f> but no <v> has no cached result and is rejected with
// its coordinates; a formula whose cached <v> is present but empty is a
// legal empty result and normalizes to null. Shared-string indices outside
// the table (including negative) fail the whole workbook.
//
// The normalized NDJSON is budgeted before it is materialized: per-row and
// total encoded sizes (targets, quoting, escapes, nulls, separators) are
// accumulated per cell and reject the moment a cap is exceeded, so no
// oversized payload is ever fully allocated. Both preview and import fail
// identically on outputs the rows reader could not read back.
package xlsximp

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
)

// Limits shared by inspect, preview and import.
const (
	// MaxCompressedBytes caps the workbook file itself (matches the
	// UploadData multipart ceiling); the HTTP envelope adds
	// MultipartEnvelopeMargin on top.
	MaxCompressedBytes      = 32 << 20
	MultipartEnvelopeMargin = 512 << 10
	// MaxUncompressedBytes caps total uncompressed zip content.
	MaxUncompressedBytes = 64 << 20
	MaxZipEntries        = 4096
	MaxSheets            = 64
	// MaxTargetColumns matches datasetFileRowsMaxColumns in handlers.
	MaxTargetColumns = 256
	// MaxSourceColumns bounds the physical source row width.
	MaxSourceColumns = 512
	// MaxDataRows matches datasetFileRowsMaxLimit / maxDatasetInputRows.
	MaxDataRows = 1000
	// MaxHeaderRow bounds the selectable 1-based header row.
	MaxHeaderRow = 100
	// DefaultSampleRows / MaxSampleRows bound preview sample output.
	DefaultSampleRows = 25
	MaxSampleRows     = 100
	// MaxTargetNameLen bounds a mapped target column name.
	MaxTargetNameLen = 256

	// Streaming scan caps (per selected worksheet, enforced before anything
	// beyond one row of cells is retained): total <row> elements including
	// blank ones, total <c> elements, and how far beyond the configured
	// header the last content row may sit. Together with the Excel row
	// ceiling these bound sparse-row and styled-blank floods.
	maxRowElements  = 65536
	maxCellElements = 262144
	contentRowSlack = 64
	excelMaxRows    = 1048576

	// NDJSON output caps — must equal the dataset rows reader limits
	// (datasetFileRowsMaxBytes / datasetFileRowsMaxLineBytes in handlers);
	// a handlers test asserts the equality.
	MaxNDJSONTotalBytes = 32 << 20
	MaxNDJSONLineBytes  = 1 << 20

	maxSharedStrings = 2 << 20
	maxMetaTokens    = 262144

	xlsmWorkbookType = "application/vnd.ms-excel.sheet.macroenabled.main+xml"
	xltmWorkbookType = "application/vnd.ms-excel.template.macroenabled.main+xml"
	xltxWorkbookType = "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml"
	xlsxWorkbookType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
	// vbaContentType matches the VBA project part content types
	// (application/vnd.ms-office.vbaProject and its
	// .versionIndependent / .sigStorage variants).
	vbaContentType = "vnd.ms-office.vbaproject"

	// Strict part namespaces: documents rooted anywhere else are rejected
	// rather than half-parsed.
	nsSpreadsheetML       = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
	nsPackageRels         = "http://schemas.openxmlformats.org/package/2006/relationships"
	nsPackageContentTypes = "http://schemas.openxmlformats.org/package/2006/content-types"

	// Exact relationship types (an allowlist, not a suffix match).
	relOffice     = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/"
	relWorksheet  = relOffice + "worksheet"
	relChartsheet = relOffice + "chartsheet"
	relDialog     = relOffice + "dialogSheet"
)

// Config is the validated import configuration. HeaderRow is the 1-based
// physical header row, or nil when the sheet has no header row.
type Config struct {
	Sheet     string      `json:"sheet"`
	HeaderRow *int        `json:"header_row"`
	Mapping   []ColumnMap `json:"mapping"`
}

// ColumnMap binds a 0-based physical source column to a unique non-empty
// target name. Duplicate or blank original headers are expected: identity is
// the physical index, never the header text.
type ColumnMap struct {
	SourceIndex int    `json:"source_index"`
	Target      string `json:"target"`
}

// SourceColumn describes one physical source column for mapping UIs.
type SourceColumn struct {
	Index  int    `json:"index"`
	Header string `json:"header"`
}

// SheetInfo names one selectable worksheet (chart/dialog sheets excluded).
type SheetInfo struct {
	Name  string `json:"name"`
	Index int    `json:"index"`
}

// InspectResult is the workbook summary returned before any config exists.
type InspectResult struct {
	Filename    string      `json:"filename"`
	SizeBytes   int64       `json:"size_bytes"`
	SHA256      string      `json:"sha256"`
	Sheets      []SheetInfo `json:"sheets"`
	MaxDataRows int         `json:"max_data_rows"`
}

// Result is the shared normalization outcome for preview and import.
type Result struct {
	Config        Config           `json:"config"`
	Columns       []string         `json:"columns"`        // sorted target names
	SourceColumns []SourceColumn   `json:"source_columns"` // physical source layout
	SampleRows    []map[string]any `json:"sample_rows"`
	RowCount      int              `json:"row_count"`
	PhysicalRows  []int            `json:"-"`

	rows      []map[string]any
	ndjson    []byte
	ndjsonSHA string
}

// NDJSON returns the verified normalized bytes (one object per data row,
// keys sorted, string values or null). Normalize has already enforced the
// per-line and total caps, so these bytes are always readable by the
// dataset rows reader.
func (r *Result) NDJSON() []byte { return r.ndjson }

// NDJSONSHA256 hashes the exact normalized bytes.
func (r *Result) NDJSONSHA256() string {
	if r.ndjsonSHA == "" {
		sum := sha256.Sum256(r.NDJSON())
		r.ndjsonSHA = hex.EncodeToString(sum[:])
	}
	return r.ndjsonSHA
}

// Error is a typed parser failure; Kind maps to HTTP error codes upstream.
type Error struct {
	Kind string
	Msg  string
}

func (e *Error) Error() string { return e.Kind + ": " + e.Msg }

func errKind(kind, format string, args ...any) *Error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// Error kinds.
const (
	ErrInvalid       = "excel_file_invalid"
	ErrOversized     = "excel_oversized"
	ErrZipUnsafe     = "excel_zip_unsafe"
	ErrMacro         = "excel_macro_forbidden"
	ErrExternalLinks = "excel_external_links_forbidden"
	ErrEncrypted     = "excel_encrypted"
	ErrSheet         = "excel_sheet_invalid"
	ErrConfig        = "excel_config_invalid"
	ErrMapping       = "excel_mapping_invalid"
	ErrHeader        = "excel_header_invalid"
	ErrRowLimit      = "excel_row_limit"
	ErrFormula       = "excel_formula_uncached"
	ErrOutputLimit   = "excel_output_limit"
	ErrParse         = "excel_parse_failed"
)

// KindOf extracts the parser error kind, defaulting to ErrParse.
func KindOf(err error) string {
	if e, ok := err.(*Error); ok {
		return e.Kind
	}
	return ErrParse
}

// ── OpenXML ST_Xstring decoding ─────────────────────────────────────────────

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	default:
		return int(c-'A') + 10
	}
}

// The _xHHHH_ token is 7 bytes: '_', 'x', four hex digits, '_'.
func xEscapeAt(s string, i int) (uint16, bool) {
	if i+7 > len(s) || s[i] != '_' || s[i+1] != 'x' || s[i+6] != '_' {
		return 0, false
	}
	for j := 2; j < 6; j++ {
		if !isHexByte(s[i+j]) {
			return 0, false
		}
	}
	v := uint16(0)
	for j := 2; j < 6; j++ {
		v = v*16 + uint16(hexVal(s[i+j]))
	}
	return v, true
}

// decodeXString resolves _xHHHH_ escapes in one pass. _x005F_ before another
// escape marks that escape as literal text (_x005F_x000D_ stays _x000D_).
// Surrogate pairs combine; an unpaired surrogate is rejected.
func decodeXString(s string) (string, error) {
	if !strings.Contains(s, "_x") {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		v, ok := xEscapeAt(s, i)
		if !ok {
			b.WriteByte(s[i])
			i++
			continue
		}
		if v == 0x005F {
			// Escaped underscore: emit it and advance past the token only. A
			// following independent escape decodes normally
			// (_x005F__x000D_ -> "_\r"); shared-underscore text
			// (_x005F_x000D_) passes through as literal _x000D_ because the
			// remainder no longer starts an escape.
			b.WriteByte('_')
			i += 7
			continue
		}
		if v >= 0xD800 && v <= 0xDBFF {
			low, okLow := xEscapeAt(s, i+7)
			if !okLow || low < 0xDC00 || low > 0xDFFF {
				return "", fmt.Errorf("unpaired high surrogate _x%04X_", v)
			}
			r := (rune(v-0xD800)<<10 | rune(low-0xDC00)) + 0x10000
			b.WriteRune(r)
			i += 14
			continue
		}
		if v >= 0xDC00 && v <= 0xDFFF {
			return "", fmt.Errorf("unpaired low surrogate _x%04X_", v)
		}
		b.WriteRune(rune(v))
		i += 7
	}
	return b.String(), nil
}

// ── package-level metadata ──────────────────────────────────────────────────

type sheetRef struct {
	name      string
	entry     string // zip entry path of the sheet XML
	worksheet bool
}

type workbookMeta struct {
	sheets []sheetRef // document order, all kinds
	byName map[string]sheetRef
}

// forbiddenEntry reports macro parts, external references, and encrypted
// packages by entry name.
func forbiddenEntry(name string) (bool, string) {
	lower := strings.ToLower(name)
	switch {
	case lower == "xl/vbaproject.bin":
		return true, ErrMacro
	case strings.HasPrefix(lower, "xl/externallinks/"):
		return true, ErrExternalLinks
	case lower == "encryptioninfo" || lower == "encryptedpackage" ||
		strings.HasPrefix(lower, "encryptioninfo/") || strings.HasPrefix(lower, "encryptedpackage/"):
		return true, ErrEncrypted
	}
	return false, ""
}

// checkContentTypes verifies the effective type of the part the parser
// reads as the workbook. Only explicitly typed pinned macro formats may
// contain an opaque VBA part.
func checkContentTypes(zr *zip.Reader, workbookType string, macroWorkbook bool) error {
	entry := zipEntry(zr, "[Content_Types].xml")
	if entry == nil {
		return errKind(ErrInvalid, "workbook is missing [Content_Types].xml")
	}
	dec, closer, err := decodeEntryXML(entry)
	if err != nil {
		return err
	}
	defer closer.Close()
	rootChecked := false
	overrides := map[string]string{} // "/xl/..." part name → content type
	defaults := map[string]string{}  // extension (lowercased, no dot) → content type
	tokens := 0
	for {
		tok, terr := dec.Token()
		if terr == io.EOF {
			break
		}
		if terr != nil {
			return errKind(ErrInvalid, "[Content_Types].xml is malformed: %v", terr)
		}
		if tokens++; tokens > maxMetaTokens {
			return errKind(ErrInvalid, "[Content_Types].xml is unexpectedly large")
		}
		if el, ok := tok.(xml.StartElement); ok {
			if !rootChecked {
				rootChecked = true
				if el.Name.Space != nsPackageContentTypes {
					return errKind(ErrInvalid, "[Content_Types].xml has unexpected root namespace %q", el.Name.Space)
				}
			}
			if el.Name.Local == "Override" || el.Name.Local == "Default" {
				partType := ""
				partName := ""
				ext := ""
				for _, a := range el.Attr {
					switch a.Name.Local {
					case "ContentType":
						partType = strings.ToLower(strings.TrimSpace(a.Value))
					case "PartName":
						partName = strings.ToLower(strings.TrimSpace(a.Value))
					case "Extension":
						ext = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(a.Value, ".")))
					}
				}
				if strings.Contains(partType, "macroenabled") &&
					(!macroWorkbook || partType != workbookType || partName != "/xl/workbook.xml" || el.Name.Local != "Override") {
					return errKind(ErrMacro, "macro-enabled content type %q is not allowed", partType)
				}
				if el.Name.Local == "Override" && partName != "" {
					if _, duplicate := overrides[partName]; duplicate && partName == "/xl/workbook.xml" {
						return errKind(ErrInvalid, "workbook has duplicate main part type declarations")
					}
					overrides[partName] = partType
				}
				if el.Name.Local == "Default" && ext != "" {
					defaults[ext] = partType
				}
			}
		}
	}
	matchedWorkbook := false
	// Effective content types decide on the REAL parts: an Override wins;
	// a Default only applies to extensions that actually have an entry.
	// Macro-free formats reject a VBA part however it is named.
	// A lazy Default with no matching entry stays inert (real producers
	// declare one unconditionally).
	for _, f := range zr.File {
		lower := "/" + strings.ToLower(f.Name)
		eff, ok := overrides[lower]
		if !ok {
			if dot := strings.LastIndex(lower, "."); dot >= 0 && dot+1 < len(lower) {
				eff, ok = defaults[lower[dot+1:]]
			}
		}
		if lower == "/xl/workbook.xml" {
			matchedWorkbook = eff == workbookType
		}
		if ok && strings.Contains(eff, vbaContentType) && (!macroWorkbook || !strings.HasSuffix(lower, ".bin")) {
			return errKind(ErrMacro, "part %s has unexpected VBA content type %q", f.Name, eff)
		}
	}
	if !matchedWorkbook {
		return errKind(ErrInvalid, "workbook main part does not match the expected content type %q", workbookType)
	}
	return nil
}

// openZip validates the container and returns a reader whose entries obey
// the count and uncompressed-size caps (zip-bomb precheck via the central
// directory, before any entry is decompressed).
func openZip(data []byte, workbookType string) (*zip.Reader, error) {
	if int64(len(data)) > MaxCompressedBytes {
		return nil, errKind(ErrOversized, "workbook is %d bytes, max %d", len(data), MaxCompressedBytes)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, errKind(ErrInvalid, "workbook is not a readable zip container: %v", err)
	}
	if len(zr.File) > MaxZipEntries {
		return nil, errKind(ErrZipUnsafe, "workbook has %d zip entries, max %d", len(zr.File), MaxZipEntries)
	}
	macroWorkbook := workbookType == xlsmWorkbookType || workbookType == xltmWorkbookType
	total := uint64(0)
	seenNames := map[string]bool{}
	for _, f := range zr.File {
		if bad, kind := forbiddenEntry(f.Name); bad && (!macroWorkbook || kind != ErrMacro) {
			return nil, errKind(kind, "workbook entry %s is not allowed", f.Name)
		}
		lower := strings.ToLower(f.Name)
		if seenNames[lower] {
			return nil, errKind(ErrInvalid, "workbook has duplicate part name %q (part names are case-insensitive)", f.Name)
		}
		seenNames[lower] = true
		total += f.UncompressedSize64
		if f.UncompressedSize64 > MaxUncompressedBytes {
			return nil, errKind(ErrZipUnsafe, "zip entry %s declares %d uncompressed bytes, max %d", f.Name, f.UncompressedSize64, MaxUncompressedBytes)
		}
	}
	if total > MaxUncompressedBytes {
		return nil, errKind(ErrZipUnsafe, "workbook decompresses to %d bytes total, max %d", total, MaxUncompressedBytes)
	}
	if err := checkContentTypes(zr, workbookType, macroWorkbook); err != nil {
		return nil, err
	}
	return zr, nil
}

func zipEntry(zr *zip.Reader, name string) *zip.File {
	for _, f := range zr.File {
		if strings.EqualFold(f.Name, name) {
			return f
		}
	}
	return nil
}

// resolveTarget maps a relationship target to a zip entry path relative to
// the xl/ base where the workbook part lives. Anything that could escape
// the package fails closed.
func resolveTarget(target string) (string, error) {
	t := strings.ReplaceAll(target, "\\", "/")
	if strings.Contains(t, "..") || strings.Contains(t, "://") || strings.Contains(t, "file:") {
		return "", errKind(ErrInvalid, "relationship target %q escapes the package", target)
	}
	t = strings.TrimPrefix(t, "/")
	if t == "" {
		return "", errKind(ErrInvalid, "relationship target is empty")
	}
	if !strings.HasPrefix(t, "xl/") {
		t = "xl/" + t
	}
	if !strings.HasSuffix(t, ".xml") {
		return "", errKind(ErrInvalid, "relationship target %q is not an XML part", target)
	}
	return t, nil
}

// readWorkbookMeta parses only the lightweight parts: xl/workbook.xml (sheet
// names and r:id) and xl/_rels/workbook.xml.rels (id → target and type).
// No worksheet XML is opened here, so inspecting a workbook never loads
// sheet content; chart/dialog sheets are recognized by relationship type.
func readWorkbookMeta(zr *zip.Reader) (*workbookMeta, error) {
	wbEntry := zipEntry(zr, "xl/workbook.xml")
	if wbEntry == nil {
		return nil, errKind(ErrInvalid, "workbook is missing xl/workbook.xml")
	}
	sheets, err := parseWorkbookSheets(wbEntry)
	if err != nil {
		return nil, err
	}

	relsEntry := zipEntry(zr, "xl/_rels/workbook.xml.rels")
	type rel struct{ id, target, relType, mode string }
	rels := map[string]rel{}
	if relsEntry != nil {
		dec, closer, derr := decodeEntryXML(relsEntry)
		if derr != nil {
			return nil, derr
		}
		defer closer.Close()
		rootChecked := false
		tokens := 0
		for {
			tok, terr := dec.Token()
			if terr == io.EOF {
				break
			}
			if terr != nil {
				return nil, errKind(ErrInvalid, "xl/_rels/workbook.xml.rels is malformed: %v", terr)
			}
			if tokens++; tokens > maxMetaTokens {
				return nil, errKind(ErrInvalid, "xl/_rels/workbook.xml.rels is unexpectedly large")
			}
			if el, ok := tok.(xml.StartElement); ok {
				if !rootChecked {
					rootChecked = true
					if el.Name.Space != nsPackageRels {
						return nil, errKind(ErrInvalid, "xl/_rels/workbook.xml.rels has unexpected root namespace %q", el.Name.Space)
					}
				}
				if el.Name.Local == "Relationship" {
					r := rel{}
					for _, a := range el.Attr {
						switch a.Name.Local {
						case "Id":
							r.id = a.Value
						case "Target":
							r.target = a.Value
						case "Type":
							r.relType = a.Value
						case "TargetMode":
							r.mode = a.Value
						}
					}
					if r.id != "" {
						if _, dup := rels[r.id]; dup {
							return nil, errKind(ErrInvalid, "workbook relationships have duplicate Id %q", r.id)
						}
						rels[r.id] = r
					}
				}
			}
		}
	}

	meta := &workbookMeta{byName: map[string]sheetRef{}}
	for _, s := range sheets {
		if s.name == "" {
			return nil, errKind(ErrInvalid, "xl/workbook.xml contains a sheet without a name")
		}
		if _, dup := meta.byName[s.name]; dup {
			return nil, errKind(ErrInvalid, "workbook has duplicate sheet name %q", s.name)
		}
		ref := sheetRef{name: s.name}
		if r, ok := rels[s.rid]; ok && r.mode != "External" {
			// Exact type allowlist: only the official worksheet type makes a
			// sheet selectable; chart/dialog sheets and anything else are not.
			switch r.relType {
			case relWorksheet:
				target, terr := resolveTarget(r.target)
				if terr != nil {
					return nil, terr
				}
				ref.entry = target
				ref.worksheet = true
			case relChartsheet, relDialog:
				// recognized but not selectable
			default:
				// unknown sheet-kind relationship: not selectable
			}
		}
		meta.sheets = append(meta.sheets, ref)
		meta.byName[s.name] = ref
	}
	return meta, nil
}

func parseWorkbookSheets(entry *zip.File) ([]rawSheetOut, error) {
	dec, closer, err := decodeEntryXML(entry)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var out []rawSheetOut
	inSheets := false
	rootChecked := false
	tokens := 0
	for {
		tok, terr := dec.Token()
		if terr == io.EOF {
			break
		}
		if terr != nil {
			return nil, errKind(ErrInvalid, "xl/workbook.xml is malformed: %v", terr)
		}
		if tokens++; tokens > maxMetaTokens {
			return nil, errKind(ErrInvalid, "xl/workbook.xml is unexpectedly large")
		}
		switch el := tok.(type) {
		case xml.StartElement:
			if !rootChecked {
				rootChecked = true
				if el.Name.Space != nsSpreadsheetML {
					return nil, errKind(ErrInvalid, "xl/workbook.xml has unexpected root namespace %q", el.Name.Space)
				}
			}
			if el.Name.Local == "sheets" {
				inSheets = true
			} else if inSheets && el.Name.Local == "sheet" {
				s := rawSheetOut{}
				for _, a := range el.Attr {
					switch a.Name.Local {
					case "name":
						s.name = a.Value
					case "id":
						if a.Name.Space == "http://schemas.openxmlformats.org/officeDocument/2006/relationships" || a.Name.Space == "" {
							s.rid = a.Value
						}
					}
				}
				out = append(out, s)
			}
		case xml.EndElement:
			if el.Name.Local == "sheets" {
				inSheets = false
			}
		}
	}
	return out, nil
}

type rawSheetOut struct{ name, rid string }

func decodeEntryXML(f *zip.File) (*xml.Decoder, io.Closer, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, nil, errKind(ErrInvalid, "zip entry %s cannot be opened: %v", f.Name, err)
	}
	return xml.NewDecoder(rc), rc, nil
}

// worksheets returns the selectable worksheet references in document order.
func (m *workbookMeta) worksheets() []sheetRef {
	out := make([]sheetRef, 0, len(m.sheets))
	for _, s := range m.sheets {
		if s.worksheet {
			out = append(out, s)
		}
	}
	return out
}

// loadSharedStrings resolves t="s" cell values. String items concatenate
// their direct <t> and rich-text <r><t> runs only — phonetic <rPh> runs are
// skipped, other stray character data is ignored, original whitespace is
// preserved, and _xHHHH_ escapes decode per ST_Xstring. Out-of-range or
// negative indices fail the workbook at cell-resolution time.
func loadSharedStrings(zr *zip.Reader) ([]string, error) {
	entry := zipEntry(zr, "xl/sharedStrings.xml")
	if entry == nil {
		return nil, nil
	}
	dec, closer, err := decodeEntryXML(entry)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	out := make([]string, 0, 1024)
	var b strings.Builder
	inItem, inT, inRPh := false, false, false
	rootChecked := false
	tokens := 0
	flush := func() error {
		if !inItem {
			return nil
		}
		decoded, derr := decodeXString(b.String())
		if derr != nil {
			return errKind(ErrInvalid, "xl/sharedStrings.xml has an invalid _xHHHH_ escape: %v", derr)
		}
		out = append(out, decoded)
		b.Reset()
		return nil
	}
	for {
		tok, terr := dec.Token()
		if terr == io.EOF {
			break
		}
		if terr != nil {
			return nil, errKind(ErrInvalid, "xl/sharedStrings.xml is malformed: %v", terr)
		}
		if tokens++; tokens > maxMetaTokens {
			return nil, errKind(ErrZipUnsafe, "xl/sharedStrings.xml is unexpectedly large")
		}
		switch el := tok.(type) {
		case xml.StartElement:
			if !rootChecked {
				rootChecked = true
				if el.Name.Space != nsSpreadsheetML {
					return nil, errKind(ErrInvalid, "xl/sharedStrings.xml has unexpected root namespace %q", el.Name.Space)
				}
			}
			switch el.Name.Local {
			case "si":
				if err := flush(); err != nil {
					return nil, err
				}
				inItem, inT = true, false
				b.Reset()
			case "rPh":
				if inItem {
					inRPh = true
				}
			case "t":
				if inItem && !inRPh {
					inT = true
				}
			}
		case xml.CharData:
			if inT {
				b.Write(el)
			}
		case xml.EndElement:
			switch el.Name.Local {
			case "si":
				if err := flush(); err != nil {
					return nil, err
				}
				inItem = false
			case "rPh":
				inRPh = false
			case "t":
				inT = false
			}
		}
		if len(out) > maxSharedStrings {
			return nil, errKind(ErrZipUnsafe, "workbook has more than %d shared strings", maxSharedStrings)
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// ── worksheet scan ──────────────────────────────────────────────────────────

// cellInfo is one retained cell: its raw resolved value plus the formula and
// cached-result existence flags recorded by the scan. hasF covers normal,
// array, and shared (including self-closing) formulas; hasV distinguishes a
// missing cached value (reject when mapped) from a present-but-empty one
// (legal empty result, null).
type cellInfo struct {
	raw  string
	hasF bool
	hasV bool
}

type scannedRow struct {
	pos   int // physical 1-based row number
	cells map[int]cellInfo
}

// scanWorksheet streams the selected worksheet XML once, enforcing every
// structural bound before retaining anything: row-element and cell-element
// counts, the physical row ceiling, strictly increasing row numbers with
// duplicate rejection, cell/row coordinate agreement, and the content-row
// span relative to the configured header row. Sequential coordinates are
// inferred only when the r attribute is entirely absent. Only the header
// row and in-range data rows survive.
func scanWorksheet(zr *zip.Reader, ref sheetRef, headerAt int, shared []string, selection *cellRectangle) (header map[int]cellInfo, data []scannedRow, maxLen int, err error) {
	entry := zipEntry(zr, ref.entry)
	if entry == nil {
		return nil, nil, 0, errKind(ErrInvalid, "worksheet part %s is missing from the workbook", ref.entry)
	}
	dec, closer, err := decodeEntryXML(entry)
	if err != nil {
		return nil, nil, 0, err
	}
	defer closer.Close()

	const (
		stOutside = iota
		stInSheetData
		stInRow
		stInV
		stInF
		stInIS
		stInIST
		stInISPh
	)
	state := stOutside
	rowElements, cellElements := 0, 0
	currentPos, prevRowPos := 0, 0
	prevCellCol := 0
	var rowCells map[int]cellInfo
	var seenCols map[int]bool
	rowHasContent := false
	sawSheetData := false
	rootChecked := false
	header = nil
	data = make([]scannedRow, 0, 64)
	maxLen = 0

	curCol := 0
	curType := ""
	curHasF, curHasV := false, false
	var vBuf, isBuf strings.Builder

	retainRow := func(rowPos int, cells map[int]cellInfo, hasContent bool) error {
		if !hasContent {
			return nil
		}
		if selection == nil && rowPos > headerAt+MaxDataRows+contentRowSlack {
			return errKind(ErrRowLimit, "sheet %q has content at row %d beyond the configured range (header %d + %d data rows + %d slack)", ref.name, rowPos, headerAt, MaxDataRows, contentRowSlack)
		}
		if headerAt > 0 && rowPos == headerAt {
			header = cells
		} else if rowPos > headerAt {
			if len(data) >= MaxDataRows {
				return errKind(ErrRowLimit, "sheet %q has more than %d data rows under the configured header (first excess at row %d)", ref.name, MaxDataRows, rowPos)
			}
			data = append(data, scannedRow{pos: rowPos, cells: cells})
		}
		for c := range cells {
			if c+1 > maxLen {
				maxLen = c + 1
			}
		}
		return nil
	}

	for {
		tok, terr := dec.Token()
		if terr == io.EOF {
			break
		}
		if terr != nil {
			return nil, nil, 0, errKind(ErrParse, "worksheet %s is malformed or truncated: %v", ref.entry, terr)
		}
		switch el := tok.(type) {
		case xml.StartElement:
			if !rootChecked {
				rootChecked = true
				if el.Name.Space != nsSpreadsheetML {
					return nil, nil, 0, errKind(ErrInvalid, "worksheet %s has unexpected root namespace %q", ref.entry, el.Name.Space)
				}
			}
			switch el.Name.Local {
			case "sheetData":
				state = stInSheetData
			case "row":
				if state != stInSheetData {
					continue
				}
				if rowCells != nil {
					if err := retainRow(currentPos, rowCells, rowHasContent); err != nil {
						return nil, nil, 0, err
					}
				}
				rowElements++
				if rowElements > maxRowElements {
					return nil, nil, 0, errKind(ErrRowLimit, "sheet %q has more than %d row elements", ref.name, maxRowElements)
				}
				rowNo := 0
				rowAttrPresent := false
				for _, a := range el.Attr {
					if a.Name.Local == "r" {
						rowAttrPresent = true
						if n, e := strconv.Atoi(strings.TrimSpace(a.Value)); e == nil {
							rowNo = n
						}
					}
				}
				if rowAttrPresent {
					if rowNo < 1 || rowNo > excelMaxRows {
						return nil, nil, 0, errKind(ErrInvalid, "sheet %q declares invalid row %q", ref.name, strings.TrimSpace(attrByName(el.Attr, "r")))
					}
					if rowNo <= prevRowPos {
						return nil, nil, 0, errKind(ErrInvalid, "sheet %q has a duplicate or out-of-order row %d after row %d", ref.name, rowNo, prevRowPos)
					}
					currentPos = rowNo
					prevRowPos = rowNo
				} else {
					// Absent r continues after the previous physical row.
					currentPos = prevRowPos + 1
					prevRowPos = currentPos
				}
				rowCells = map[int]cellInfo{}
				seenCols = map[int]bool{}
				rowHasContent = false
				prevCellCol = 0
				state = stInRow
			case "c":
				if state != stInRow {
					continue
				}
				cellElements++
				if cellElements > maxCellElements {
					return nil, nil, 0, errKind(ErrRowLimit, "sheet %q has more than %d cell elements", ref.name, maxCellElements)
				}
				curCol = 0
				curType = ""
				curHasF, curHasV = false, false
				vBuf.Reset()
				isBuf.Reset()
				cellRef := ""
				for _, a := range el.Attr {
					switch a.Name.Local {
					case "r":
						cellRef = strings.TrimSpace(a.Value)
					case "t":
						curType = a.Value
					}
				}
				if cellRef != "" {
					col, cellRow, e := parseCellRef(cellRef)
					if e != nil || cellRow != currentPos {
						return nil, nil, 0, errKind(ErrInvalid, "sheet %q row %d has cell ref %q that is invalid or disagrees with the row", ref.name, currentPos, cellRef)
					}
					curCol = col
				} else {
					curCol = prevCellCol + 1
				}
				prevCellCol = curCol
				if curCol < 1 || curCol > MaxSourceColumns {
					return nil, nil, 0, errKind(ErrRowLimit, "sheet %q row %d references column %s beyond the max %d", ref.name, currentPos, columnName(curCol), MaxSourceColumns)
				}
				// Every cell — even an empty one — occupies its coordinate; a
				// later cell at the same coordinate is a duplicate.
				if seenCols[curCol-1] {
					return nil, nil, 0, errKind(ErrInvalid, "sheet %q row %d has a duplicate cell at column %s", ref.name, currentPos, columnName(curCol))
				}
				seenCols[curCol-1] = true
			case "v":
				if state == stInRow {
					state = stInV
				}
			case "f":
				if state == stInRow {
					curHasF = true
					state = stInF
				}
			case "is":
				if state == stInRow {
					state = stInIS
				}
			case "rPh":
				if state == stInIS {
					state = stInISPh
				}
			case "t":
				if state == stInIS {
					state = stInIST
				}
			}
		case xml.CharData:
			switch state {
			case stInV:
				vBuf.Write(el)
			case stInIST:
				isBuf.Write(el)
			}
		case xml.EndElement:
			switch el.Name.Local {
			case "v":
				if state == stInV {
					curHasV = true
					state = stInRow
				}
			case "f":
				if state == stInF {
					state = stInRow
				}
			case "t":
				if state == stInIST {
					state = stInIS
				}
			case "rPh":
				if state == stInISPh {
					state = stInIS
				}
			case "is":
				if state == stInIS {
					state = stInRow
				}
			case "c":
				if state != stInRow {
					continue
				}
				ci := cellInfo{raw: vBuf.String(), hasF: curHasF, hasV: curHasV}
				if curType == "s" && ci.hasV {
					idx, e := strconv.Atoi(strings.TrimSpace(ci.raw))
					if e != nil || idx < 0 || idx >= len(shared) {
						return nil, nil, 0, errKind(ErrInvalid, "sheet %q row %d column %s references shared string %q outside the table", ref.name, currentPos, columnName(curCol), ci.raw)
					}
					ci.raw = shared[idx]
				} else if curType == "inlineStr" {
					decoded, derr := decodeXString(isBuf.String())
					if derr != nil {
						return nil, nil, 0, errKind(ErrInvalid, "sheet %q row %d column %s has an invalid inline string escape: %v", ref.name, currentPos, columnName(curCol), derr)
					}
					ci.raw = decoded
					if isBuf.Len() > 0 {
						ci.hasV = true
					}
				} else if curType == "str" && ci.hasV {
					// Cached formula string results are ST_Xstring too.
					decoded, derr := decodeXString(ci.raw)
					if derr != nil {
						return nil, nil, 0, errKind(ErrInvalid, "sheet %q row %d column %s has an invalid cached formula string: %v", ref.name, currentPos, columnName(curCol), derr)
					}
					ci.raw = decoded
				}
				if selection != nil && !selection.includes(currentPos, curCol) {
					continue
				}
				if ci.raw != "" || ci.hasF || ci.hasV {
					rowCells[curCol-1] = ci
					if ci.raw != "" || ci.hasF {
						rowHasContent = true
					}
				}
			case "row":
				if state == stInRow {
					state = stInSheetData
				}
			case "sheetData":
				state = stOutside
				sawSheetData = true
			}
		}
	}
	if rowCells != nil {
		if err := retainRow(currentPos, rowCells, rowHasContent); err != nil {
			return nil, nil, 0, err
		}
	}
	if !sawSheetData {
		return nil, nil, 0, errKind(ErrInvalid, "worksheet %s has no sheetData element", ref.entry)
	}
	return header, data, maxLen, nil
}

func attrByName(attrs []xml.Attr, name string) string {
	for _, a := range attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// parseCellRef parses "B3" into 1-based column and row.
func parseCellRef(ref string) (col, row int, err error) {
	i := 0
	for i < len(ref) && ref[i] >= 'A' && ref[i] <= 'Z' {
		i++
	}
	if i == 0 || i > 3 || i == len(ref) {
		return 0, 0, fmt.Errorf("bad cell ref %q", ref)
	}
	col = 0
	for _, c := range ref[:i] {
		col = col*26 + int(c-'A') + 1
	}
	row, err = strconv.Atoi(ref[i:])
	if err != nil || row < 1 {
		return 0, 0, fmt.Errorf("bad cell ref %q", ref)
	}
	return col, row, nil
}

// columnName renders a 1-based column index as "A", "B", … "AA".
func columnName(col int) string {
	name := make([]byte, 0, 3)
	for col > 0 {
		col--
		name = append(name, byte('A'+col%26))
		col /= 26
	}
	for i, j := 0, len(name)-1; i < j; i, j = i+1, j-1 {
		name[i], name[j] = name[j], name[i]
	}
	return string(name)
}

// ── NDJSON budgeting ────────────────────────────────────────────────────────

// jsonEscapedLenUpperBound bounds the JSON encoding of a Go string as
// produced by encoding/json (quotes excluded): ordinary bytes stay 1:1,
// quotes and backslashes escape to 2, control characters and the HTML-safe
// trio (<, >, &) escape to 6-byte \uXXXX forms. The bound is exact enough
// for budget rejection before any payload allocation.
func jsonEscapedLenUpperBound(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			n += 2
		case c < 0x20 || c == '<' || c == '>' || c == '&':
			n += 6
		default:
			n++
		}
	}
	return n
}

// ── public API ──────────────────────────────────────────────────────────────

// Inspect summarizes the workbook using package metadata only: worksheets
// (chart/dialog sheets excluded by relationship type), bounded sizes, and
// the source sha256 used for provenance metadata.
func Inspect(data []byte, filename string) (*InspectResult, error) {
	zr, err := openZip(data, xlsxWorkbookType)
	if err != nil {
		return nil, err
	}
	meta, err := readWorkbookMeta(zr)
	if err != nil {
		return nil, err
	}
	names := meta.worksheets()
	if len(names) > MaxSheets {
		return nil, errKind(ErrSheet, "workbook has %d worksheets, max %d", len(names), MaxSheets)
	}
	sheets := make([]SheetInfo, 0, len(names))
	for i, s := range names {
		sheets = append(sheets, SheetInfo{Name: s.name, Index: i})
	}
	sum := sha256.Sum256(data)
	return &InspectResult{
		Filename:    filename,
		SizeBytes:   int64(len(data)),
		SHA256:      hex.EncodeToString(sum[:]),
		Sheets:      sheets,
		MaxDataRows: MaxDataRows,
	}, nil
}

func (c Config) normalizedMapping() ([]ColumnMap, error) {
	if len(c.Mapping) == 0 {
		return nil, errKind(ErrMapping, "mapping is required")
	}
	if len(c.Mapping) > MaxTargetColumns {
		return nil, errKind(ErrMapping, "mapping has %d entries, max %d columns", len(c.Mapping), MaxTargetColumns)
	}
	seenSource := map[int]bool{}
	seenTarget := map[string]bool{}
	out := make([]ColumnMap, 0, len(c.Mapping))
	for _, m := range c.Mapping {
		if m.SourceIndex < 0 || m.SourceIndex >= MaxSourceColumns {
			return nil, errKind(ErrMapping, "source_index %d out of range 0..%d", m.SourceIndex, MaxSourceColumns-1)
		}
		if seenSource[m.SourceIndex] {
			return nil, errKind(ErrMapping, "source_index %d mapped more than once", m.SourceIndex)
		}
		seenSource[m.SourceIndex] = true
		target := strings.TrimSpace(m.Target)
		if target == "" {
			return nil, errKind(ErrMapping, "source_index %d has an empty target name", m.SourceIndex)
		}
		if len([]rune(target)) > MaxTargetNameLen {
			return nil, errKind(ErrMapping, "target %q exceeds %d characters", target, MaxTargetNameLen)
		}
		if seenTarget[target] {
			return nil, errKind(ErrMapping, "target %q mapped more than once", target)
		}
		seenTarget[target] = true
		out = append(out, ColumnMap{SourceIndex: m.SourceIndex, Target: target})
	}
	return out, nil
}

// Normalize validates the configuration against the real workbook bytes and
// produces the canonical result shared by preview and import. The row count
// is always fully counted (over-limit sheets are rejected, never truncated),
// and the NDJSON is budgeted per cell before any row is materialized so
// both preview and import fail identically — and early — on outputs the
// rows reader could not read back. A nil cfg.Mapping yields source columns
// only.
func Normalize(data []byte, cfg Config, sampleRows int) (*Result, error) {
	return normalize(data, cfg, sampleRows, xlsxWorkbookType, nil)
}

func pinnedOOXMLWorkbookType(format string) string {
	switch format {
	case "xlsx":
		return xlsxWorkbookType
	case "xltx":
		return xltxWorkbookType
	case "xlsm":
		return xlsmWorkbookType
	case "xltm":
		return xltmWorkbookType
	default:
		return ""
	}
}

// SupportsSnapshotFormat reports the workbook formats of the pinned endpoint.
func SupportsSnapshotFormat(format string) bool {
	return format == "xls" || format == "xlt" || pinnedOOXMLWorkbookType(format) != ""
}

// NormalizeSnapshot decodes rows from an explicitly typed pinned file.
func NormalizeSnapshot(data []byte, cfg Config, format string) (*Result, error) {
	return NormalizeSnapshotRange(data, cfg, format, nil)
}

// NormalizeSnapshotRange optionally limits a pinned OOXML read to an A1 rectangle.
func NormalizeSnapshotRange(data []byte, cfg Config, format string, cellRange *string) (*Result, error) {
	if workbookType := pinnedOOXMLWorkbookType(format); workbookType != "" {
		var selection *cellRectangle
		if cellRange != nil {
			var err error
			selection, err = parseCellRectangle(*cellRange, cfg.HeaderRow)
			if err != nil {
				return nil, err
			}
		}
		return normalize(data, cfg, 0, workbookType, selection)
	}
	if cellRange != nil {
		return nil, errKind(ErrConfig, "cell_range requires an OOXML workbook")
	}
	if format == "xls" || format == "xlt" {
		return normalizeBIFF8(data, cfg, format)
	}
	return nil, errKind(ErrInvalid, "unsupported workbook format %q", format)
}

func normalize(data []byte, cfg Config, sampleRows int, workbookType string, selection *cellRectangle) (*Result, error) {
	zr, err := openZip(data, workbookType)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Sheet) == "" {
		return nil, errKind(ErrConfig, "sheet is required")
	}
	meta, err := readWorkbookMeta(zr)
	if err != nil {
		return nil, err
	}
	sheets := meta.worksheets()
	if len(sheets) > MaxSheets {
		return nil, errKind(ErrSheet, "workbook has %d worksheets, max %d", len(sheets), MaxSheets)
	}
	ref, ok := meta.byName[cfg.Sheet]
	if !ok || !ref.worksheet {
		return nil, errKind(ErrSheet, "sheet %q is not a selectable worksheet", cfg.Sheet)
	}

	headerAt := 0
	if cfg.HeaderRow != nil {
		if *cfg.HeaderRow < 1 || *cfg.HeaderRow > MaxHeaderRow {
			return nil, errKind(ErrHeader, "header_row %d out of range 1..%d", *cfg.HeaderRow, MaxHeaderRow)
		}
		headerAt = *cfg.HeaderRow
	}

	mappingRequired := true
	var mapping []ColumnMap
	if cfg.Mapping == nil {
		mappingRequired = false
	} else if len(cfg.Mapping) == 0 {
		return nil, errKind(ErrMapping, "mapping must not be empty")
	} else if mapping, err = cfg.normalizedMapping(); err != nil {
		return nil, err
	}

	if selection != nil {
		for _, column := range mapping {
			if column.SourceIndex+1 < selection.firstCol || column.SourceIndex+1 > selection.lastCol {
				return nil, errKind(ErrMapping, "source_index %d is outside cell_range", column.SourceIndex)
			}
		}
	}

	var shared []string
	if shared, err = loadSharedStrings(zr); err != nil {
		return nil, err
	}
	headerCells, dataRows, maxLen, err := scanWorksheet(zr, ref, headerAt, shared, selection)
	if err != nil {
		return nil, err
	}

	if selection != nil {
		maxLen = selection.lastCol
	}

	result := &Result{
		Config:  Config{Sheet: cfg.Sheet, HeaderRow: cfg.HeaderRow, Mapping: mapping},
		Columns: []string{},
	}
	for i := 0; i < maxLen; i++ {
		header := ""
		if ci, ok := headerCells[i]; ok {
			header = ci.raw
		}
		result.SourceColumns = append(result.SourceColumns, SourceColumn{Index: i, Header: header})
	}
	result.RowCount = len(dataRows)

	if mappingRequired && maxLen == 0 {
		return nil, errKind(ErrSheet, "sheet %q has no readable rows to map", cfg.Sheet)
	}
	if mappingRequired && len(dataRows) == 0 {
		// Header-only sheets cannot import: an empty NDJSON would leave the
		// dataset with columns but no rows and an ambiguous schema surface.
		return nil, errKind(ErrSheet, "sheet %q has no data rows under the configured header", cfg.Sheet)
	}
	if mappingRequired {
		for _, m := range mapping {
			if m.SourceIndex >= maxLen {
				return nil, errKind(ErrMapping, "source_index %d is beyond the sheet width %d", m.SourceIndex, maxLen)
			}
		}
	}
	if !mappingRequired {
		result.SampleRows = []map[string]any{}
		return result, nil
	}

	targets := make([]string, 0, len(mapping))
	for _, m := range mapping {
		targets = append(targets, m.Target)
	}
	sort.Strings(targets)
	result.Columns = targets

	// Fixed per-row encoding budget: JSON braces, target names with quotes
	// and colon, one comma between entries, and "null" for null values.
	fixedRow := 2 + len(mapping) - 1
	perTarget := make([]int, len(mapping))
	for i, m := range mapping {
		perTarget[i] = 2 + jsonEscapedLenUpperBound(m.Target) + 1 // "name":
		fixedRow += perTarget[i]
	}

	rows := make([]map[string]any, 0, len(dataRows))
	result.PhysicalRows = make([]int, 0, len(dataRows))
	var ndjson bytes.Buffer
	for _, dr := range dataRows {
		row := make(map[string]any, len(mapping))
		rowBudget := fixedRow
		for i, m := range mapping {
			ci, present := dr.cells[m.SourceIndex]
			var value any
			if present {
				if ci.hasF && !ci.hasV {
					return nil, errKind(ErrFormula, "cell %s%d has a formula without a cached result", columnName(m.SourceIndex+1), dr.pos)
				}
				if ci.raw != "" {
					value = ci.raw
					rowBudget += perTarget[i] + 2 + jsonEscapedLenUpperBound(ci.raw)
				} else {
					rowBudget += perTarget[i] + 4
				}
			} else {
				rowBudget += perTarget[i] + 4
			}
			row[m.Target] = value
			if rowBudget > MaxNDJSONLineBytes {
				return nil, errKind(ErrOutputLimit, "normalized row %d would exceed %d bytes", dr.pos, MaxNDJSONLineBytes)
			}
		}
		encoded, merr := json.Marshal(row)
		if merr != nil {
			return nil, errKind(ErrParse, "row %d cannot be encoded: %v", dr.pos, merr)
		}
		if len(encoded) > MaxNDJSONLineBytes {
			return nil, errKind(ErrOutputLimit, "normalized row %d is %d bytes, max %d per line", dr.pos, len(encoded), MaxNDJSONLineBytes)
		}
		if ndjson.Len()+len(encoded)+1 > MaxNDJSONTotalBytes {
			return nil, errKind(ErrOutputLimit, "normalized output exceeds %d bytes total", MaxNDJSONTotalBytes)
		}
		ndjson.Write(encoded)
		ndjson.WriteByte('\n')
		rows = append(rows, row)
		result.PhysicalRows = append(result.PhysicalRows, dr.pos)
	}
	result.rows = rows
	result.ndjson = ndjson.Bytes()

	if sampleRows < 0 {
		sampleRows = DefaultSampleRows
	}
	if sampleRows > MaxSampleRows {
		sampleRows = MaxSampleRows
	}
	if sampleRows > len(rows) {
		sampleRows = len(rows)
	}
	result.SampleRows = rows[:sampleRows]
	return result, nil
}

// SuggestedLogicalPath derives the default .ndjson logical path from the
// original workbook filename ("report.xlsx" -> "report.ndjson").
func SuggestedLogicalPath(filename string) string {
	base := path.Base(strings.TrimSpace(filename))
	if base == "" || base == "." || base == "/" {
		return "workbook.ndjson"
	}
	if ext := path.Ext(base); ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	if base == "" {
		return "workbook.ndjson"
	}
	return base + ".ndjson"
}
