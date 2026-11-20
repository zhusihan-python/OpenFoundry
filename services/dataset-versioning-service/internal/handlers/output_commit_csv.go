package handlers

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// normalizeOutputCSVOptions freezes the options stored with an inline
// publication. The transaction's schema, rather than the active file index,
// retains the options when another version is published.
func normalizeOutputCSVOptions(schema *models.DatasetSchema) error {
	if schema == nil || schema.CustomMetadata == nil || schema.CustomMetadata.CSV == nil {
		return nil
	}
	if schema.FileFormat != models.FileFormatText {
		return errors.New("csv options require a TEXT schema")
	}
	options := schema.CustomMetadata.CSV
	if options.Delimiter != "" {
		r, width := utf8.DecodeRuneInString(options.Delimiter)
		switch {
		case width != len(options.Delimiter), r == utf8.RuneError, r == 0, r == '\n', r == '\r', r == '"':
			return errors.New("csv delimiter must be one valid CSV separator")
		}
	}
	charset := strings.ToUpper(strings.TrimSpace(options.Charset))
	switch charset {
	case "", "UTF8", "UTF-8":
		charset = "UTF-8"
	case "UTF-16LE", "UTF-16BE":
	default:
		return errors.New("csv charset must be UTF-8, UTF-16LE, or UTF-16BE")
	}
	bom := strings.ToLower(strings.TrimSpace(options.BOM))
	switch bom {
	case "", "strip":
		bom = "strip"
	case "require", "forbid":
	default:
		return errors.New("csv bom must be strip, require, or forbid")
	}
	headerRow := 0
	if options.HeaderRow != nil {
		headerRow = *options.HeaderRow
	} else if options.Header {
		headerRow = 1
	}
	if headerRow < 0 {
		return errors.New("csv headerRow must be zero or a positive row number")
	}
	options.Charset = charset
	options.BOM = bom
	options.HeaderRow = &headerRow
	options.Header = headerRow > 0
	return nil
}
