package xlsximp

import (
	"strings"
	"testing"
)

func TestPinnedMacroWorkbookReadsOnlyWorksheetValues(t *testing.T) {
	cfg := Config{Sheet: "Sheet", HeaderRow: intPtr(1), Mapping: []ColumnMap{{SourceIndex: 0, Target: "value"}}}
	workbook := func(mainType string, extra map[string]string) []byte {
		entries := map[string]string{
			"[Content_Types].xml": strings.Replace(strings.Replace(ctXML, xlsxWorkbookType, mainType, 1), "</Types>",
				`<Default Extension="bin" ContentType="application/vnd.ms-office.vbaProject"/></Types>`, 1),
			"xl/workbook.xml":            workbookXML(`<sheet name="Sheet" sheetId="1" r:id="rId1"/>`),
			"xl/_rels/workbook.xml.rels": relsXML("rId1", "worksheet", "worksheets/sheet1.xml"),
			"xl/worksheets/sheet1.xml": sheetXML(
				`<row r="1"><c r="A1" t="inlineStr"><is><t>Value</t></is></c></row>` +
					`<row r="4"><c r="A4"><f>1+2</f><v>9.99</v></c></row>`),
			// Opaque bytes prove that the worksheet reader does not interpret VBA.
			"xl/vbaProject.bin": "uninterpreted binary",
		}
		for path, content := range extra {
			entries[path] = content
		}
		return buildZip(t, entries)
	}

	for _, tc := range []struct {
		format, mainType string
	}{
		{"xlsm", xlsmWorkbookType},
		{"xltm", xltmWorkbookType},
	} {
		t.Run(tc.format, func(t *testing.T) {
			data := workbook(tc.mainType, nil)
			result, err := NormalizeSnapshot(data, cfg, tc.format)
			if err != nil {
				t.Fatal(err)
			}
			if string(result.NDJSON()) != "{\"value\":\"9.99\"}\n" || len(result.PhysicalRows) != 1 || result.PhysicalRows[0] != 4 {
				t.Fatalf("unexpected cached worksheet result: %s at %v", result.NDJSON(), result.PhysicalRows)
			}
			_, err = Normalize(data, cfg, 0)
			wantKind(t, err, ErrMacro, "normal upload")
			_, err = NormalizeSnapshot(data, cfg, "xlsx")
			wantKind(t, err, ErrMacro, "macro file renamed to XLSX")
			_, err = NormalizeSnapshot(workbook(tc.mainType, map[string]string{"xl/externalLinks/externalLink1.xml": "external"}), cfg, tc.format)
			wantKind(t, err, ErrExternalLinks, "external part")
			_, err = NormalizeSnapshot(workbook(tc.mainType, map[string]string{"EncryptionInfo": "encrypted"}), cfg, tc.format)
			wantKind(t, err, ErrEncrypted, "encryption part")
		})
	}
	_, err := NormalizeSnapshot(workbook(xltmWorkbookType, nil), cfg, "xlsm")
	wantKind(t, err, ErrMacro, "template renamed to macro workbook")

	types := strings.Replace(ctXML, xlsxWorkbookType, xlsmWorkbookType, 1)
	types = strings.Replace(types, "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml", "application/vnd.ms-office.vbaProject", 1)
	_, err = NormalizeSnapshot(workbook(xlsmWorkbookType, map[string]string{"[Content_Types].xml": types}), cfg, "xlsm")
	wantKind(t, err, ErrMacro, "VBA disguised as a worksheet XML part")
}
