package handlers

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/google/uuid"
)

const (
	rowCompareCursorMagic = "r1"
	rowCompareCursorSize  = len(rowCompareCursorMagic) + 2*sha256.Size
)

type rowCompareCursor struct {
	scope [sha256.Size]byte
	after [sha256.Size]byte
}

type rowComparePage struct {
	limit  int
	cursor *rowCompareCursor
	scope  [sha256.Size]byte
}

// A cursor carries a request binding and a digest of the last changed key.
// Hashing the key bounds the token even when the selected column holds long
// strings. The cursor only positions a freshly resolved and authorized read.
func (c rowCompareCursor) encode() string {
	raw := make([]byte, 0, rowCompareCursorSize)
	raw = append(raw, rowCompareCursorMagic...)
	raw = append(raw, c.scope[:]...)
	raw = append(raw, c.after[:]...)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func parseRowCompareCursor(raw string) (*rowCompareCursor, error) {
	if len(raw) != base64.RawURLEncoding.EncodedLen(rowCompareCursorSize) {
		return nil, fmt.Errorf("dataset_row_compare_cursor_invalid: invalid cursor length")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	if err != nil || len(decoded) != rowCompareCursorSize || string(decoded[:len(rowCompareCursorMagic)]) != rowCompareCursorMagic {
		return nil, fmt.Errorf("dataset_row_compare_cursor_invalid: unrecognized cursor")
	}
	var cursor rowCompareCursor
	copy(cursor.scope[:], decoded[len(rowCompareCursorMagic):len(rowCompareCursorMagic)+sha256.Size])
	copy(cursor.after[:], decoded[len(rowCompareCursorMagic)+sha256.Size:])
	return &cursor, nil
}

func (q rowCompareRequest) pageScope(datasetID uuid.UUID) [sha256.Size]byte {
	// Quote variable-length strings to preserve the field boundaries.
	s := fmt.Sprintf("%s\n%q\n%d\n%s\n%q\n%d\n%s\n%q\n%d",
		datasetID, q.base.Branch, q.base.Version, q.base.TransactionID,
		q.target.Branch, q.target.Version, q.target.TransactionID, q.key, q.page.limit)
	return sha256.Sum256([]byte(s))
}
