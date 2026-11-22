package handler

import (
	"encoding/json"
	"fmt"
	"strings"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

const runtimeDeclaredSchema = "declared_table_schema"

type lightweightTable struct {
	rows        []pipelineexpression.Row
	node        *models.PipelineIRNode
	schema      pipelineStrictSchema
	positional  bool
	castLineage bool
}

func (draft *runtimeUnionDraft) byPosition() bool {
	return draft != nil && strings.EqualFold(strings.TrimSpace(draft.UnionType), "by_position")
}

func declaredTableSchemas(ir models.PipelineIR) map[string]*models.PipelineIRSchema {
	schemas := map[string]*models.PipelineIRSchema{}
	for _, node := range ir.Nodes {
		if node.OutputSchema != nil {
			schemas[node.ID] = node.OutputSchema
		} else if normaliseTableTransform(node.TransformType) == "input" && node.PreviewSchema != nil {
			schemas[node.ID] = node.PreviewSchema
		}
	}
	return schemas
}

func (rt *lightweightTableRuntime) runtimeTable(node executor.Node, transform, kind string, payload json.RawMessage, cfg tableRuntimeConfig, datasetMeta map[string]any, rows []pipelineexpression.Row) lightweightTable {
	table := lightweightTable{rows: rows}
	// Preserve logical types the JSON encoding alone cannot distinguish.
	// Other pipelines keep their existing lazy schema resolution.
	if cfg.Stack != nil {
		for _, block := range cfg.Stack.Blocks {
			if block.Applied && strings.EqualFold(strings.TrimSpace(block.Kind), "cast") {
				table.castLineage = true
				break
			}
		}
	}
	rt.mu.Lock()
	for _, dep := range node.DependsOn {
		table.castLineage = table.castLineage || rt.tables[dep].castLineage
	}
	rt.mu.Unlock()
	if kind == "sql" && cfg.Stack == nil && cfg.Join == nil && cfg.GeoJoin == nil && cfg.Union.byPosition() {
		// runUnion has already resolved and checked the input layouts.
		table.schema = rt.tableSchema(node.DependsOn[0])
		if table.castLineage {
			deps := make([]pipelineStrictSchema, len(node.DependsOn))
			for i, dep := range node.DependsOn {
				deps[i] = rt.tableSchema(dep)
			}
			table.schema = unionCastSchema(deps, true)
		}
		table.positional = true
		return table
	}
	if (kind == "output" || kind == "passthrough") && len(node.DependsOn) > 0 {
		rt.mu.Lock()
		source := rt.tables[node.DependsOn[0]]
		rt.mu.Unlock()
		if source.positional {
			table.schema, table.positional = source.schema, true
			return table
		}
	}
	irNode := models.PipelineIRNode{ID: node.ID, TransformType: transform, Config: payload, DependsOn: node.DependsOn}
	irNode.OutputSchema, _ = node.Metadata[runtimeDeclaredSchema].(*models.PipelineIRSchema)
	if irNode.OutputSchema == nil {
		if schema, ok := datasetMeta["dataset_schema"].(*datasetRowsSchema); ok {
			irNode.OutputSchema = &models.PipelineIRSchema{}
			for _, field := range schema.Fields {
				irNode.OutputSchema.Fields = append(irNode.OutputSchema.Fields, models.PipelineIRField{
					Name: field.Name, FieldType: field.Type, Nullable: field.Nullable,
				})
			}
		}
	}
	table.node = &irNode
	return table
}

func (rt *lightweightTableRuntime) storeTable(nodeID string, table lightweightTable) {
	table.rows = cloneRows(table.rows)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.tables[nodeID] = table
}

func (rt *lightweightTableRuntime) tableSchema(nodeID string) pipelineStrictSchema {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.resolveTableSchema(nodeID)
}

// Resolve and cache the layout when a join, union or typed publication needs it.
// The graph is already executing in dependency order and mu is held.
func (rt *lightweightTableRuntime) resolveTableSchema(nodeID string) pipelineStrictSchema {
	table, ok := rt.tables[nodeID]
	if !ok || table.schema.Known {
		return table.schema
	}
	rows := make([]map[string]json.RawMessage, len(table.rows))
	for i, row := range table.rows {
		rows[i] = row
	}
	inferred := schemaFromRows(rows)
	if table.node == nil {
		table.schema = inferred
		rt.tables[nodeID] = table
		return table.schema
	}

	var schema pipelineStrictSchema
	if table.node.OutputSchema != nil {
		schema = schemaFromIRSchema(*table.node.OutputSchema)
	}
	if len(table.node.DependsOn) > 0 && (!schema.Known || table.castLineage) {
		deps := make([]pipelineStrictSchema, 0, len(table.node.DependsOn))
		for _, dep := range table.node.DependsOn {
			deps = append(deps, rt.resolveTableSchema(dep))
		}
		derived := validateStrictNode(*table.node, deps, &pipelineStrictValidationReport{})
		if !schema.Known {
			schema = derived
		} else {
			for i, field := range schema.Fields {
				if computed, exists := derived.field(field.Name); exists && computed.castTarget {
					schema.Fields[i] = computed
				}
			}
		}
	}
	if !schema.Known {
		schema = inferred
	} else {
		// Row-dependent blocks can produce fields absent from the static
		// shape. Keep their names, using the same deterministic fallback as
		// an untyped input, so no observed data gets dropped.
		for _, field := range inferred.Fields {
			if _, exists := schema.field(field.Name); !exists {
				schema.Fields = append(schema.Fields, field)
			}
		}
	}
	table.schema = schema
	rt.tables[nodeID] = table
	return schema
}

func (rt *lightweightTableRuntime) positionalColumns(nodeID string) []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	table := rt.tables[nodeID]
	if !table.positional {
		return nil
	}
	return table.schema.columnNames()
}

func (s pipelineStrictSchema) columnNames() []string {
	names := make([]string, 0, len(s.Fields))
	for _, field := range s.Fields {
		names = append(names, field.Name)
	}
	return names
}

func (rt *lightweightTableRuntime) alignPositionalUnion(inputs []string, batches [][]pipelineexpression.Row) ([]pipelineexpression.Row, error) {
	target := rt.tableSchema(inputs[0]).columnNames()
	out := []pipelineexpression.Row{}
	for i, rows := range batches {
		schema := rt.tableSchema(inputs[i])
		if len(rows) == 0 && !schema.Known {
			continue
		}
		source := schema.columnNames()
		if len(source) != len(target) {
			return nil, fmt.Errorf("union_position_arity_mismatch: input %d has %d columns, expected %d", i+1, len(source), len(target))
		}
		seen := make(map[string]bool, len(source))
		for _, name := range source {
			if strings.TrimSpace(name) == "" || seen[name] {
				return nil, fmt.Errorf("union_position_invalid_column: input %d has a blank or duplicate column %q", i+1, name)
			}
			seen[name] = true
		}
		for _, row := range rows {
			next := make(pipelineexpression.Row, len(target))
			for pos, name := range target {
				value := row[source[pos]]
				if len(value) == 0 {
					value = json.RawMessage("null")
				}
				next[name] = value
			}
			out = append(out, next)
		}
	}
	return out, nil
}
