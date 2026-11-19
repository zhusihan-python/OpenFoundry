package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const jsonFlattenMaxDepth = 32

var errJSONFlattenInputFormat = errors.New("dataset_json_flatten_format: nested JSON expansion requires NDJSON input")
var jsonFieldPathEscaper = strings.NewReplacer(`\`, `\\`, ".", `\.`)

func scanSchemaInference(flattenJSON bool) *datasetSchemaInference {
	if flattenJSON {
		return &datasetSchemaInference{format: "json"}
	}
	return &datasetSchemaInference{format: "csv"}
}

func flattenJSONRecord(raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, errors.New("dataset_json_flatten_invalid: input must be a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	var nameBytes int
	if err := expandJSONObject(raw, "", 1, fields, &nameBytes); err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

func expandJSONObject(raw json.RawMessage, prefix string, depth int, fields map[string]json.RawMessage, nameBytes *int) error {
	if depth > jsonFlattenMaxDepth {
		return fmt.Errorf("dataset_json_flatten_depth: objects may be at most %d levels deep", jsonFlattenMaxDepth)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	start, err := dec.Token()
	if err != nil || start != json.Delim('{') {
		return errors.New("dataset_json_flatten_invalid: input must be a JSON object")
	}
	seen := make(map[string]struct{})
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || key == "" {
			return errors.New("dataset_json_flatten_invalid: object keys must be non-empty strings")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("dataset_json_flatten_duplicate_key: duplicate object key %q", key)
		}
		seen[key] = struct{}{}
		path := jsonFieldPathEscaper.Replace(key)
		if prefix != "" {
			path = prefix + "." + path
		}
		if len(path) > schemaInferenceMaxNameBytes {
			return errors.New("dataset_json_schema_names_exceeded: column path exceeds 64 KiB")
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		value = bytes.TrimSpace(value)
		if value[0] == '{' && string(bytes.TrimSpace(value[1:len(value)-1])) != "" {
			if err := expandJSONObject(value, path, depth+1, fields, nameBytes); err != nil {
				return err
			}
			continue
		}
		if len(fields) >= schemaInferenceMaxColumns {
			return errors.New("dataset_json_schema_columns_exceeded: at most 256 expanded columns per row")
		}
		if len(path) > schemaInferenceMaxNameBytes-*nameBytes {
			return errors.New("dataset_json_schema_names_exceeded: expanded column names exceed 64 KiB")
		}
		*nameBytes += len(path)
		fields[path] = value
	}
	return nil
}
