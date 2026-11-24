package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	"github.com/google/uuid"
)

// noonOrderLineColumns is the sealed NDJSON profile a row comparison reads
// next to orders.v1: published noon order lines, one JSON object per record
// whose six values are all strings.
var noonOrderLineColumns = [...]string{"store", "market", "order_nr", "item_nr", "unit_price", "currency"}

func isNoonOrderLineKey(key string) bool {
	return slices.Contains(noonOrderLineColumns[:], key)
}

func isOrdersV1Key(key string) bool {
	for _, field := range parquetOrdersSchema().Fields {
		if field.Name == key {
			return true
		}
	}
	return false
}

// scanRowComparePage reads one bounded page of the pinned snapshot through the
// scanner rows:scan and the versioned preview use for that file format.
func (h *Handlers) scanRowComparePage(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, pin rowComparePin,
	target *scanTarget, noon bool, limits rowCompareScanLimits, cursor string) (datasetScanAllFilesResponse, bool) {
	if noon {
		return h.scanNDJSONAllFilesPage(w, r, datasetID, pin.Branch, target.branch, target.txn, pin.Version, limits.pageRows, nil, cursor)
	}
	return h.scanParquetAllFilesPage(w, r, datasetID, pin.Branch, target.branch, target.txn, pin.Version, limits.pageRows, nil, cursor)
}

func rowCompareRows(page datasetScanAllFilesResponse, noon bool) ([]json.RawMessage, error) {
	if noon {
		return noonOrderLineRows(page.Rows)
	}
	_, rows, err := pinnedPreviewRows(page)
	return rows, err
}

// noonOrderLineRows checks each record against the sealed profile and returns
// it as a compact object with sorted keys, the form the strict Parquet scanner
// emits, so two records are equal exactly when their values are.
func noonOrderLineRows(records []json.RawMessage) ([]json.RawMessage, error) {
	rows := make([]json.RawMessage, 0, len(records))
	for _, record := range records {
		// Pointers tell a null from an empty string: unmarshaling null into a
		// string leaves it empty without an error.
		var fields map[string]*string
		if json.Unmarshal(record, &fields) != nil || len(fields) != len(noonOrderLineColumns) {
			return nil, errors.New("noon order-line record is not an object of six string columns")
		}
		line := make(map[string]string, len(fields))
		for _, column := range noonOrderLineColumns {
			if fields[column] == nil {
				return nil, errors.New("noon order-line record is missing a column or holds null")
			}
			line[column] = *fields[column]
		}
		row, err := json.Marshal(line)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
