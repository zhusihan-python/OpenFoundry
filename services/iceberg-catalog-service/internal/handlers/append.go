package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/iceberg-catalog-service/internal/models"
)

// AppendBatch validates the OpenFoundry writer contract for both sinks.
// The catalog's metadata store cannot write row data or manifests.
func (h *Handlers) AppendBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSONErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var batch models.AppendBatch
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&batch); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid append body")
		return
	}
	if err := validateAppendSpec(batch.Spec); err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(batch.Rows) == 0 {
		writeJSONErr(w, http.StatusBadRequest, "rows must be non-empty")
		return
	}

	namespace := namespacePath(batch.Spec.Namespace)
	table, err := h.Repo.GetTable(r.Context(), projectRID(r), namespace, batch.Spec.Table)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if table == nil {
		writeJSONErr(w, http.StatusNotFound, "table not found")
		return
	}
	if err := validateAppendContract(table, batch); err != nil {
		writeJSONErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	// A 2xx response allows the sink to commit Kafka offsets. Only a durable
	// table writer can acknowledge this request after its files and snapshot commit.
	writeJSONErr(w, http.StatusServiceUnavailable, "durable Iceberg table writer required")
}

func validateAppendSpec(spec models.TableSpec) error {
	if strings.TrimSpace(spec.Catalog) == "" {
		return fmt.Errorf("catalog is required")
	}
	if strings.TrimSpace(spec.Namespace) == "" || strings.TrimSpace(spec.Table) == "" {
		return fmt.Errorf("namespace and table are required")
	}
	if strings.TrimSpace(spec.PartitionTransform) == "" {
		return fmt.Errorf("partition_transform is required")
	}
	if strings.TrimSpace(spec.SortOrder) == "" {
		return fmt.Errorf("sort_order is required")
	}
	if len(spec.Schema) == 0 {
		return fmt.Errorf("schema is required")
	}
	seenIDs := map[int]struct{}{}
	seenNames := map[string]struct{}{}
	for _, field := range spec.Schema {
		if field.ID <= 0 || strings.TrimSpace(field.Name) == "" || strings.TrimSpace(field.Type) == "" {
			return fmt.Errorf("schema fields require id, name and type")
		}
		if _, ok := seenIDs[field.ID]; ok {
			return fmt.Errorf("duplicate schema field id %d", field.ID)
		}
		seenIDs[field.ID] = struct{}{}
		name := strings.TrimSpace(field.Name)
		if _, ok := seenNames[name]; ok {
			return fmt.Errorf("duplicate schema field name %s", name)
		}
		seenNames[name] = struct{}{}
	}
	return nil
}

func validateAppendContract(table *models.IcebergTable, batch models.AppendBatch) error {
	if got := normalizeSimpleSchema(table.SchemaJSON); len(got) > 0 && !reflect.DeepEqual(got, batch.Spec.Schema) {
		return fmt.Errorf("schema mismatch")
	}
	if !matchesPartition(table.PartitionSpec, batch.Spec.PartitionTransform) {
		return fmt.Errorf("partition metadata mismatch")
	}
	if !matchesSortOrder(table.SortOrder, batch.Spec.SortOrder) {
		return fmt.Errorf("sort metadata mismatch")
	}
	for i, row := range batch.Rows {
		if err := validateAppendRow(batch.Spec.Schema, row); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
	}
	return nil
}

func validateAppendRow(schema []models.FieldSpec, row map[string]any) error {
	allowed := map[string]models.FieldSpec{}
	for _, field := range schema {
		allowed[field.Name] = field
		value, exists := row[field.Name]
		if field.Required && (!exists || value == nil) {
			return fmt.Errorf("required field %s missing", field.Name)
		}
		if exists && value != nil && !valueMatchesFieldType(value, field.Type) {
			return fmt.Errorf("field %s has invalid %s value", field.Name, field.Type)
		}
	}
	for name := range row {
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("unknown field %s", name)
		}
	}
	return nil
}

func valueMatchesFieldType(value any, typ string) bool {
	switch typ {
	case "uuid":
		s, ok := value.(string)
		if !ok {
			return false
		}
		_, err := uuid.Parse(s)
		return err == nil
	case "string":
		_, ok := value.(string)
		return ok
	case "uint32":
		n, ok := jsonNumber(value)
		return ok && n >= 0 && n == float64(uint32(n))
	case "timestamptz":
		if _, ok := jsonNumber(value); ok {
			return true
		}
		_, err := time.Parse(time.RFC3339Nano, fmt.Sprint(value))
		return err == nil
	default:
		return true
	}
}

func jsonNumber(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case json.Number:
		n, err := v.Float64()
		return n, err == nil
	default:
		return 0, false
	}
}

func normalizeSimpleSchema(raw json.RawMessage) []models.FieldSpec {
	var direct []models.FieldSpec
	if err := json.Unmarshal(raw, &direct); err == nil && len(direct) > 0 {
		return direct
	}
	var iceberg struct {
		Fields []models.FieldSpec `json:"fields"`
	}
	if err := json.Unmarshal(raw, &iceberg); err == nil && len(iceberg.Fields) > 0 {
		return iceberg.Fields
	}
	return nil
}

func matchesPartition(raw json.RawMessage, want string) bool {
	if strings.TrimSpace(want) == "" {
		return false
	}
	if jsonStringValue(raw) == want {
		return true
	}
	var spec struct {
		Fields []struct {
			Transform  string `json:"transform"`
			SourceName string `json:"source-name"`
		} `json:"fields"`
	}
	if json.Unmarshal(raw, &spec) != nil || len(spec.Fields) != 1 {
		return len(raw) == 0 || string(raw) == "null" || string(raw) == "{}"
	}
	field := spec.Fields[0]
	return fmt.Sprintf("%s(%s)", field.Transform, field.SourceName) == want
}

func matchesSortOrder(raw json.RawMessage, want string) bool {
	if strings.TrimSpace(want) == "" {
		return false
	}
	if jsonStringValue(raw) == want {
		return true
	}
	var order struct {
		Fields []struct {
			SourceName string `json:"source-name"`
			Direction  string `json:"direction"`
		} `json:"fields"`
	}
	if json.Unmarshal(raw, &order) != nil || len(order.Fields) != 1 {
		return len(raw) == 0 || string(raw) == "null" || string(raw) == "{}"
	}
	field := order.Fields[0]
	return strings.TrimSpace(field.SourceName+" "+strings.ToUpper(field.Direction)) == want
}

func jsonStringValue(raw json.RawMessage) string {
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	return ""
}
