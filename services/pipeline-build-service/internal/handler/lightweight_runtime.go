package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/google/uuid"
	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

type lightweightTableRuntime struct {
	mu        sync.Mutex
	tables    map[string]lightweightTable
	functions PipelineFunctionRegistry
	// datasets reads registered dataset files for input-shaped nodes. When
	// nil, a node that references a dataset fails explicitly instead of
	// falling back to synthetic rows.
	datasets PipelineDatasetRowSource
	// datasetRowsPreview marks preview runs, which may read a truncated
	// sample; formal execution keeps strict no-truncation semantics.
	datasetRowsPreview bool
	// joinExpansionLimit caps the intermediate rows one relational join may
	// materialize (0 = uncapped). Preview runs set the shared
	// maxLightweightExpandedRows bound because they fully materialize the
	// join expansion; builds keep their existing output capability.
	joinExpansionLimit int
}

type tableRuntimeConfig struct {
	Rows           []map[string]json.RawMessage `json:"rows,omitempty"`
	SeedRows       []map[string]json.RawMessage `json:"seed_rows,omitempty"`
	Records        []map[string]json.RawMessage `json:"records,omitempty"`
	Data           []map[string]json.RawMessage `json:"data,omitempty"`
	Predicate      string                       `json:"predicate,omitempty"`
	Expression     string                       `json:"expression,omitempty"`
	Columns        []string                     `json:"columns,omitempty"`
	Select         []string                     `json:"select,omitempty"`
	DropColumns    []string                     `json:"drop_columns,omitempty"`
	Renames        map[string]string            `json:"renames,omitempty"`
	ColumnMappings []struct {
		SourceColumn string `json:"source_column"`
		TargetColumn string `json:"target_column"`
	} `json:"column_mappings,omitempty"`
	DatasetID      string `json:"dataset_id,omitempty"`
	DatasetRID     string `json:"dataset_rid,omitempty"`
	DatasetPath    string `json:"dataset_path,omitempty"`
	DatasetBranch  string `json:"dataset_branch,omitempty"`
	DatasetVersion string `json:"dataset_version,omitempty"`
	// DatasetSnapshot is the raw WP3.4 pin object: when present the node
	// reads an explicit {branch, version, transaction_id} snapshot instead
	// of the branch's current files. Raw on purpose — parse errors must
	// surface as dataset_snapshot_invalid, not a generic config parse.
	DatasetSnapshot     json.RawMessage           `json:"dataset_snapshot,omitempty"`
	InputDatasetIDs     []string                  `json:"input_dataset_ids,omitempty"`
	SQL                 string                    `json:"sql,omitempty"`
	SampleSize          int                       `json:"sample_size,omitempty"`
	FunctionID          string                    `json:"function_id,omitempty"`
	FunctionName        string                    `json:"function_name,omitempty"`
	FunctionVersion     string                    `json:"function_version,omitempty"`
	FunctionAutoUpgrade bool                      `json:"function_auto_upgrade,omitempty"`
	TargetColumn        string                    `json:"target_column,omitempty"`
	ResultType          string                    `json:"result_type,omitempty"`
	Arguments           []runtimeFunctionArgument `json:"arguments,omitempty"`
	Args                map[string]string         `json:"args,omitempty"`
	Stack               *runtimeTransformStack    `json:"_stack,omitempty"`
	Join                *runtimeJoinDraft         `json:"_join,omitempty"`
	GeoJoin             *runtimeGeoJoinDraft      `json:"_geo_join,omitempty"`
	Union               *runtimeUnionDraft        `json:"_union,omitempty"`
	GPX                 string                    `json:"gpx,omitempty"`
	GPXXML              string                    `json:"gpx_xml,omitempty"`
	GPXContent          string                    `json:"gpx_content,omitempty"`
	SourceColumn        string                    `json:"source_column,omitempty"`
	GPXColumn           string                    `json:"gpx_column,omitempty"`
	ContentColumn       string                    `json:"content_column,omitempty"`
	TrailID             string                    `json:"trail_id,omitempty"`
	TrailIDColumn       string                    `json:"trail_id_column,omitempty"`
	TrailName           string                    `json:"trail_name,omitempty"`
	TrailNameColumn     string                    `json:"trail_name_column,omitempty"`
	SourceName          string                    `json:"source_name,omitempty"`
	SourceNameColumn    string                    `json:"source_name_column,omitempty"`
	FileNameColumn      string                    `json:"file_name_column,omitempty"`
}

// Row-expanding blocks store their results in memory before publication.
const maxLightweightExpandedRows = 100_000

type runtimeTransformStack struct {
	Blocks []runtimeTransformBlock `json:"blocks,omitempty"`
}

type runtimeTransformBlock struct {
	Frame                   *runtimeWindowFrame       `json:"frame,omitempty"`
	Expression              string                    `json:"expression,omitempty"`
	GroupBy                 []string                  `json:"group_by,omitempty"`
	Aggregations            []runtimeAggregation      `json:"aggregations,omitempty"`
	CSVDelimiter            string                    `json:"delimiter,omitempty"`
	CSVHasHeader            *bool                     `json:"has_header,omitempty"`
	JSONPath                string                    `json:"json_path,omitempty"`
	SortKeys                []runtimeSortKey          `json:"sort_keys,omitempty"`
	Kind                    string                    `json:"kind,omitempty"`
	Applied                 bool                      `json:"applied,omitempty"`
	Columns                 []string                  `json:"columns,omitempty"`
	Renames                 []runtimeRenameMapping    `json:"renames,omitempty"`
	SourceColumn            string                    `json:"source_column,omitempty"`
	TargetType              string                    `json:"target_type,omitempty"`
	TargetColumn            string                    `json:"target_column,omitempty"`
	StartLatColumn          string                    `json:"start_lat_column,omitempty"`
	StartLonColumn          string                    `json:"start_lon_column,omitempty"`
	EndLatColumn            string                    `json:"end_lat_column,omitempty"`
	EndLonColumn            string                    `json:"end_lon_column,omitempty"`
	Lat1Column              string                    `json:"lat1_column,omitempty"`
	Lon1Column              string                    `json:"lon1_column,omitempty"`
	Lat2Column              string                    `json:"lat2_column,omitempty"`
	Lon2Column              string                    `json:"lon2_column,omitempty"`
	Unit                    string                    `json:"unit,omitempty"`
	SourceUnit              string                    `json:"source_unit,omitempty"`
	TargetUnit              string                    `json:"target_unit,omitempty"`
	Mode                    string                    `json:"mode,omitempty"`
	Match                   string                    `json:"match,omitempty"`
	Conditions              []runtimeFilterCondition  `json:"conditions,omitempty"`
	RemoveSpecialCharacters bool                      `json:"remove_special_characters,omitempty"`
	FunctionID              string                    `json:"function_id,omitempty"`
	FunctionName            string                    `json:"function_name,omitempty"`
	FunctionVersion         string                    `json:"function_version,omitempty"`
	FunctionAutoUpgrade     bool                      `json:"function_auto_upgrade,omitempty"`
	Arguments               []runtimeFunctionArgument `json:"arguments,omitempty"`
	Args                    map[string]string         `json:"args,omitempty"`
	ResultType              string                    `json:"result_type,omitempty"`
}

type runtimeFunctionArgument struct {
	Name       string          `json:"name,omitempty"`
	Expression string          `json:"expression,omitempty"`
	Column     string          `json:"column,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
}

type runtimeRenameMapping struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type runtimeFilterCondition struct {
	Column           string `json:"column,omitempty"`
	Operator         string `json:"operator,omitempty"`
	Value            string `json:"value,omitempty"`
	TreatEmptyAsNull bool   `json:"treat_empty_as_null,omitempty"`
}

type runtimeJoinDraft struct {
	JoinType        string             `json:"join_type,omitempty"`
	Matches         []runtimeJoinMatch `json:"matches,omitempty"`
	LeftColumns     []string           `json:"left_columns,omitempty"`
	RightColumns    []string           `json:"right_columns,omitempty"`
	RightPrefix     string             `json:"right_prefix,omitempty"`
	AutoSelectLeft  bool               `json:"auto_select_left,omitempty"`
	AutoSelectRight bool               `json:"auto_select_right,omitempty"`
}

type runtimeJoinMatch struct {
	LeftColumn  string `json:"left_column,omitempty"`
	RightColumn string `json:"right_column,omitempty"`
}

type runtimeGeoJoinDraft struct {
	Mode                string   `json:"mode,omitempty"`
	JoinType            string   `json:"join_type,omitempty"`
	LeftGeometryColumn  string   `json:"left_geometry_column,omitempty"`
	RightGeometryColumn string   `json:"right_geometry_column,omitempty"`
	LeftLatColumn       string   `json:"left_lat_column,omitempty"`
	LeftLonColumn       string   `json:"left_lon_column,omitempty"`
	RightLatColumn      string   `json:"right_lat_column,omitempty"`
	RightLonColumn      string   `json:"right_lon_column,omitempty"`
	Unit                string   `json:"unit,omitempty"`
	MaxDistance         float64  `json:"max_distance,omitempty"`
	K                   int      `json:"k,omitempty"`
	DistanceColumn      string   `json:"distance_column,omitempty"`
	RankColumn          string   `json:"rank_column,omitempty"`
	LeftColumns         []string `json:"left_columns,omitempty"`
	RightColumns        []string `json:"right_columns,omitempty"`
	RightPrefix         string   `json:"right_prefix,omitempty"`
	AutoSelectLeft      bool     `json:"auto_select_left,omitempty"`
	AutoSelectRight     bool     `json:"auto_select_right,omitempty"`
	MaxLeftRows         int      `json:"max_left_rows,omitempty"`
	MaxRightRows        int      `json:"max_right_rows,omitempty"`
	MaxCandidatePairs   int      `json:"max_candidate_pairs,omitempty"`
}

type runtimeUnionDraft struct {
	UnionType string `json:"union_type,omitempty"`
}

func newRuntimeNodeRunner(ports ExecutionPorts) runtimeNodeRunner {
	distributed := ports.Distributed
	if distributed == nil {
		distributed = NewSparkFlinkDistributedRunner(DistributedRuntimeConfig{})
	}
	table := newLightweightTableRuntime(ports.Functions)
	table.datasets = ports.DatasetRows
	return runtimeNodeRunner{
		JobRunner: ports.JobRunner,
		Python:    ports.Python,
		LLM:       ports.LLM,
		Table:     table,
		Dist:      distributed,
	}
}

func newLightweightTableRuntime(functions ...PipelineFunctionRegistry) *lightweightTableRuntime {
	var registry PipelineFunctionRegistry
	if len(functions) > 0 {
		registry = functions[0]
	}
	return &lightweightTableRuntime{tables: map[string]lightweightTable{}, functions: registry}
}

func (rt *lightweightTableRuntime) Supports(transformType string) bool {
	switch normaliseTableTransform(transformType) {
	case "input", "filter", "select", "drop", "rename", "passthrough", "output", "sql", "function", "gpx_parse":
		return true
	default:
		return false
	}
}

func (rt *lightweightTableRuntime) Run(ctx context.Context, node executor.NodeContext, payload json.RawMessage, transformType string) (executor.NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return executor.NodeResult{}, err
	}
	cfg, err := parseTableRuntimeConfig(payload)
	if err != nil {
		return executor.NodeResult{}, err
	}

	kind := normaliseTableTransform(transformType)
	// A declared dataset_snapshot must actually reach the pinned read:
	// transforms that never read a dataset (or literal inline rows) would
	// silently shadow the pin — reject instead of running something else.
	if len(cfg.DatasetSnapshot) > 0 {
		if kind != "input" && (kind != "passthrough" || len(node.Node.DependsOn) != 0) {
			return executor.NodeResult{}, fmt.Errorf("dataset_snapshot_shadowed: node %q has transform %q, which never reads a dataset; dataset_snapshot is only valid on dataset_input nodes", node.Node.ID, transformType)
		}
		if cfg.inlineRows() != nil {
			return executor.NodeResult{}, fmt.Errorf("dataset_snapshot_shadowed: node %q declares both inline rows and dataset_snapshot; a pinned read must not be shadowed by literal rows", node.Node.ID)
		}
	}
	var rows []pipelineexpression.Row
	var datasetMeta map[string]any
	switch kind {
	case "input":
		rows = cfg.inlineRows()
		if rows == nil {
			rows, datasetMeta, err = rt.datasetInputRows(ctx, node, cfg)
		}
	case "filter":
		rows, err = rt.runFilter(node, cfg)
	case "select":
		rows, err = rt.runSelect(node, cfg)
	case "drop":
		rows, err = rt.runDrop(node, cfg)
	case "rename":
		rows, err = rt.runRename(node, cfg)
	case "passthrough":
		if len(node.Node.DependsOn) == 0 {
			rows = cfg.inlineRows()
			if rows == nil {
				rows, datasetMeta, err = rt.datasetInputRows(ctx, node, cfg)
			}
			break
		}
		rows, err = rt.firstDependencyRows(node)
	case "output":
		rows, err = rt.firstDependencyRows(node)
	case "sql":
		rows, err = rt.runStructuredSQL(ctx, node, cfg)
	case "function":
		rows, err = rt.runFunction(ctx, node, cfg)
	case "gpx_parse":
		rows, err = rt.runGPXParse(node, cfg)
	default:
		err = fmt.Errorf("unsupported lightweight transform type: %s", transformType)
	}
	if err != nil {
		return executor.NodeResult{}, err
	}
	columns := deriveRuntimeColumns(rows)
	table := rt.runtimeTable(node.Node, transformType, kind, payload, cfg, datasetMeta, rows)
	rt.storeTable(node.Node.ID, table)
	if table.positional {
		columns = table.schema.columnNames()
	}

	metaRows := cloneRows(rows)
	if len(metaRows) > 5 {
		metaRows = metaRows[:5]
	}
	meta := map[string]any{
		"runtime":        "lightweight_table",
		"engine":         "pipeline-expression",
		"transform_type": transformType,
		"rows_affected":  len(rows),
		"columns":        columns,
		"sample_rows":    rowsToMaps(metaRows),
		"data_rows":      rowsToMaps(rows),
	}
	rt.applyTypedOutput(node.Node, table, meta, kind == "output" || len(node.Node.Outputs) > 0)
	for key, value := range datasetMeta {
		meta[key] = value
	}
	if kind == "function" {
		meta["function_id"] = cfg.FunctionID
		meta["function_name"] = cfg.FunctionName
		meta["function_version"] = cfg.FunctionVersion
		meta["target_column"] = cfg.TargetColumn
	}
	return executor.NodeResult{
		OutputContentHash: hashRuntimeRows(node.Node.ID, transformType, rows),
		Metadata:          meta,
	}, nil
}

// Carry logical types that row JSON cannot represent, such as the type of
// an empty column or a DOUBLE returned as an integral number.
func (rt *lightweightTableRuntime) applyTypedOutput(node executor.Node, table lightweightTable, meta map[string]any, publish bool) {
	projecting := false
	for _, output := range node.Outputs {
		if len(output.OutputColumns) > 0 {
			projecting = true
			break
		}
	}
	if !projecting && (!table.typedLineage || (!publish && len(table.rows) > 0)) {
		return
	}
	schema := rt.tableSchema(node.ID)
	if !schema.Known {
		return
	}
	if !table.positional {
		columns := schema.columnNames()
		sort.Strings(columns)
		meta["columns"] = columns
	}
	if !publish {
		return
	}
	fields := make(map[string]datasetOutputField)
	for _, field := range schema.Fields {
		if field.typedOutput || (projecting && len(table.rows) == 0) {
			kind := strings.ToUpper(field.FieldType)
			if parsed, ok := parseStrictType(kind); ok {
				kind = strictTypeName(parsed)
			}
			fields[field.Name] = datasetOutputField{Name: field.Name, Type: kind, Nullable: field.Nullable}
		}
	}
	if len(fields) > 0 {
		meta["cast_output_fields"] = fields
	}
}

func parseTableRuntimeConfig(payload json.RawMessage) (tableRuntimeConfig, error) {
	var cfg tableRuntimeConfig
	if len(payload) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return cfg, fmt.Errorf("parse lightweight table config: %w", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return cfg, nil
	}
	nested := envelope["config"]
	if len(nested) == 0 {
		return cfg, nil
	}
	outerConfig := !cfg.empty()
	outerDataset := cfg.DatasetRID != "" || cfg.DatasetID != "" || cfg.DatasetPath != "" ||
		cfg.DatasetBranch != "" || cfg.DatasetVersion != "" || len(cfg.InputDatasetIDs) > 0
	// Keep the legacy precedence for unpinned configs. With a pin, do not
	// discard a dataset reference or legacy version at either level.
	if (outerConfig || outerDataset) && PayloadDeclaresDatasetSnapshot(payload) {
		return cfg, errors.New("dataset_snapshot_shadowed: payload mixes top-level config fields with a nested config object while declaring dataset_snapshot; declare the pin in exactly one config object")
	}
	if outerConfig {
		return cfg, nil
	}
	return parseTableRuntimeConfig(nested)
}

func (cfg tableRuntimeConfig) empty() bool {
	return len(cfg.Rows) == 0 &&
		len(cfg.SeedRows) == 0 &&
		len(cfg.Records) == 0 &&
		len(cfg.Data) == 0 &&
		cfg.Predicate == "" &&
		cfg.Expression == "" &&
		len(cfg.Columns) == 0 &&
		len(cfg.Select) == 0 &&
		len(cfg.DropColumns) == 0 &&
		len(cfg.Renames) == 0 &&
		len(cfg.ColumnMappings) == 0 &&
		len(cfg.DatasetSnapshot) == 0 &&
		cfg.SQL == "" &&
		cfg.FunctionID == "" &&
		cfg.FunctionName == "" &&
		cfg.FunctionVersion == "" &&
		cfg.TargetColumn == "" &&
		cfg.ResultType == "" &&
		len(cfg.Arguments) == 0 &&
		len(cfg.Args) == 0 &&
		cfg.Stack == nil &&
		cfg.Join == nil &&
		cfg.GeoJoin == nil &&
		cfg.Union == nil &&
		cfg.GPX == "" &&
		cfg.GPXXML == "" &&
		cfg.GPXContent == "" &&
		cfg.SourceColumn == "" &&
		cfg.GPXColumn == "" &&
		cfg.ContentColumn == "" &&
		cfg.TrailID == "" &&
		cfg.TrailIDColumn == "" &&
		cfg.TrailName == "" &&
		cfg.TrailNameColumn == "" &&
		cfg.SourceName == "" &&
		cfg.SourceNameColumn == "" &&
		cfg.FileNameColumn == ""
}

func (cfg tableRuntimeConfig) inlineRows() []pipelineexpression.Row {
	for _, raw := range [][]map[string]json.RawMessage{cfg.Rows, cfg.SeedRows, cfg.Records, cfg.Data} {
		if len(raw) > 0 {
			out := make([]pipelineexpression.Row, 0, len(raw))
			for _, row := range raw {
				out = append(out, cloneRow(pipelineexpression.Row(row)))
			}
			return out
		}
	}
	return nil
}

// datasetInputRows loads rows for an input-shaped node that references a
// dataset. Nodes without a dataset reference keep the existing synthetic
// sample behaviour and return (rows, nil, nil). A referenced dataset that
// cannot be loaded is a hard error — the runtime never fabricates rows for
// an explicit dataset reference. Formal execution reads in strict mode
// (no truncation, no implicit file choice); preview runs
// (datasetRowsPreview) may truncate.
func (rt *lightweightTableRuntime) datasetInputRows(ctx context.Context, node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, map[string]any, error) {
	ref, ok, err := datasetInputReferenceFrom(node, cfg)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return syntheticRows(node.Node.ID, cfg.SampleSize), nil, nil
	}
	if ref.Version != "" {
		return nil, nil, fmt.Errorf("dataset_input_version_not_supported: node %q pins dataset version %q; historical dataset versions are not readable yet — clear dataset_version or pin dataset_branch instead of silently reading the latest rows", node.Node.ID, ref.Version)
	}
	if producers, ok := ctx.Value(manualGroupedDatasetProducersKey{}).(map[string]uuid.UUID); ok && ref.Snapshot == nil {
		id, err := canonicalDatasetRIDForComparison(ref.DatasetRID)
		if err != nil {
			return nil, nil, err
		}
		if producer, inGroup := producers[id]; inGroup && producer != node.Node.JobID {
			return nil, nil, fmt.Errorf("manual build cannot read unpublished grouped dataset %s; use a DAG input or a committed snapshot", ref.DatasetRID)
		}
	}
	if rt.datasets == nil {
		return nil, nil, fmt.Errorf("dataset_input_source_not_configured: node %q references dataset %s but no dataset row source is wired (set DATASET_SERVICE_URL)", node.Node.ID, ref.DatasetRID)
	}
	result, err := rt.datasets.ReadDatasetRows(ctx, DatasetRowsRequest{
		DatasetRID:      ref.DatasetRID,
		Path:            ref.Path,
		Branch:          ref.Branch,
		Limit:           clampDatasetInputRows(cfg.SampleSize),
		AllowTruncation: rt.datasetRowsPreview,
		Snapshot:        ref.Snapshot,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dataset_input_load_failed: node %q failed to load dataset %s: %w", node.Node.ID, ref.DatasetRID, err)
	}
	rows := make([]pipelineexpression.Row, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, cloneRow(pipelineexpression.Row(row)))
	}
	meta := map[string]any{
		"dataset_rid":            ref.DatasetRID,
		"dataset_file_path":      result.Path,
		"dataset_file_format":    result.Format,
		"dataset_rows_truncated": result.Truncated,
	}
	if ref.Snapshot != nil {
		// The pin is echoed from the request side: ScanStream proved the
		// served pages bind to exactly this transaction/branch/version.
		meta["dataset_branch"] = ref.Snapshot.Branch
		meta["dataset_version"] = ref.Snapshot.Version
		meta["dataset_transaction_id"] = "ri.foundry.main.transaction." + ref.Snapshot.TransactionID.String()
		if result.Schema != nil {
			meta["columns"] = result.Columns
			meta["dataset_schema_version"] = result.SchemaVersion
			meta["dataset_schema"] = result.Schema
			if len(ref.Snapshot.ExcelFiles) != 0 {
				meta["excel_files_output_schema"] = &datasetOutputSchema{Fields: result.Schema.Fields, FileFormat: result.Schema.FileFormat}
			}
		}
	} else if ref.Branch != "" {
		meta["dataset_branch"] = ref.Branch
	}
	return rows, meta, nil
}

func (rt *lightweightTableRuntime) runFilter(node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, error) {
	rows, err := rt.firstDependencyRows(node)
	if err != nil {
		return nil, err
	}
	predicate := strings.TrimSpace(firstNonEmpty(cfg.Predicate, cfg.Expression))
	if predicate == "" {
		return nil, errors.New("lightweight_filter_missing_predicate")
	}
	parsed, err := pipelineexpression.ParseExpr(predicate)
	if err != nil {
		return nil, fmt.Errorf("lightweight_filter_parse: %w", err)
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		result, err := pipelineexpression.Eval(parsed, row)
		if err != nil {
			return nil, fmt.Errorf("lightweight_filter_eval: %w", err)
		}
		if keep, ok := result.AsBool(); ok && keep {
			out = append(out, cloneRow(row))
		}
	}
	return out, nil
}

func (rt *lightweightTableRuntime) runSelect(node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, error) {
	rows, err := rt.firstDependencyRows(node)
	if err != nil {
		return nil, err
	}
	columns := cfg.Columns
	if len(columns) == 0 {
		columns = cfg.Select
	}
	if len(columns) == 0 {
		return cloneRows(rows), nil
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		next := pipelineexpression.Row{}
		for _, col := range columns {
			if value, ok := row[col]; ok {
				next[col] = append(json.RawMessage(nil), value...)
			}
		}
		out = append(out, next)
	}
	return out, nil
}

func (rt *lightweightTableRuntime) runDrop(node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, error) {
	rows, err := rt.firstDependencyRows(node)
	if err != nil {
		return nil, err
	}
	return applyRuntimeDropBlock(rows, firstStrings(cfg.DropColumns, cfg.Columns), rt.dependencySchema(node).columnNames())
}

func (rt *lightweightTableRuntime) dependencySchema(node executor.NodeContext) pipelineStrictSchema {
	if len(node.Node.DependsOn) == 0 {
		return pipelineStrictSchema{}
	}
	return rt.tableSchema(node.Node.DependsOn[0])
}

func (block runtimeTransformBlock) shapesColumns() bool {
	switch strings.ToLower(strings.TrimSpace(block.Kind)) {
	case "cast", "drop", "normalize":
		return block.Applied
	}
	return false
}

func (rt *lightweightTableRuntime) runRename(node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, error) {
	rows, err := rt.firstDependencyRows(node)
	if err != nil {
		return nil, err
	}
	renames := map[string]string{}
	for source, target := range cfg.Renames {
		renames[source] = target
	}
	for _, mapping := range cfg.ColumnMappings {
		if mapping.SourceColumn != "" && mapping.TargetColumn != "" {
			renames[mapping.SourceColumn] = mapping.TargetColumn
		}
	}
	out := cloneRows(rows)
	for _, row := range out {
		for source, target := range renames {
			if value, ok := row[source]; ok {
				row[target] = append(json.RawMessage(nil), value...)
				delete(row, source)
			}
		}
	}
	return out, nil
}

func (rt *lightweightTableRuntime) runStructuredSQL(ctx context.Context, node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, error) {
	if cfg.Stack != nil {
		return rt.runTransformStack(ctx, node, *cfg.Stack)
	}
	if cfg.Join != nil {
		return rt.runJoin(node, *cfg.Join)
	}
	if cfg.GeoJoin != nil {
		return rt.runGeoJoin(node, *cfg.GeoJoin)
	}
	if cfg.Union != nil {
		return rt.runUnion(node, *cfg.Union)
	}
	if cfg.FunctionID != "" || cfg.FunctionName != "" {
		return rt.runFunction(ctx, node, cfg)
	}
	if cfg.Predicate != "" || cfg.Expression != "" {
		filtered, err := rt.runFilter(node, cfg)
		if err != nil {
			return nil, err
		}
		if len(cfg.Columns) > 0 || len(cfg.Select) > 0 {
			rt.storeRows(node.Node.ID+":sql-filter", filtered)
			filterNode := node
			filterNode.Node.DependsOn = []string{node.Node.ID + ":sql-filter"}
			return rt.runSelect(filterNode, cfg)
		}
		return filtered, nil
	}
	if len(cfg.Columns) > 0 || len(cfg.Select) > 0 {
		return rt.runSelect(node, cfg)
	}
	if rows := cfg.inlineRows(); rows != nil {
		return rows, nil
	}
	if len(node.Node.DependsOn) > 0 {
		return rt.firstDependencyRows(node)
	}
	return nil, errors.New("lightweight_sql_requires_rows_or_structured_config")
}

func (rt *lightweightTableRuntime) runTransformStack(ctx context.Context, node executor.NodeContext, stack runtimeTransformStack) ([]pipelineexpression.Row, error) {
	rows, err := rt.firstDependencyRows(node)
	if err != nil {
		return nil, err
	}
	// Each block sees the shape produced by its predecessors, including
	// declared columns that have no values in this batch.
	var schema pipelineStrictSchema
	for _, block := range stack.Blocks {
		if block.shapesColumns() {
			schema = rt.dependencySchema(node)
			break
		}
	}
	out := cloneRows(rows)
	for _, block := range stack.Blocks {
		if !block.Applied {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(block.Kind)) {
		case "filter":
			out, err = applyRuntimeFilterBlock(out, block)
		case "select":
			out = applyRuntimeSelectBlock(out, block.Columns)
		case "drop":
			out, err = applyRuntimeDropBlock(out, block.Columns, schema.columnNames())
		case "rename":
			out = applyRuntimeRenameBlock(out, block.Renames)
		case "sort":
			out, err = applyRuntimeSortBlock(out, block.SortKeys)
		case "normalize_units":
			out, err = applyRuntimeNormalizeUnitsBlock(out, block)
		case "normalize":
			out, err = applyRuntimeNormalizeBlock(out, block.RemoveSpecialCharacters, schema.columnNames())
		case "formula", "derive_column":
			out, err = applyRuntimeFormulaBlock(out, block)
		case "window_aggregate":
			out, err = applyRuntimeWindowBlock(out, block)
		case "aggregate", "conditional_aggregate":
			out, err = applyRuntimeAggregateBlock(out, block)
		case "csv_parse":
			out, err = applyRuntimeCSVParseBlock(out, block)
		case "explode":
			out, err = applyRuntimeExplodeBlock(out, block)
		case "json_extract":
			out, err = applyRuntimeJSONExtractBlock(out, block)
		case "cast":
			out, err = applyRuntimeCastBlock(out, block, schema.columnNames())
		case "haversine", "haversine_distance", "geo_distance":
			out, err = applyRuntimeHaversineBlock(out, block)
		case "function", "udf", "reusable_function":
			out, err = rt.applyFunction(ctx, out, runtimeFunctionConfig{
				FunctionID:          block.FunctionID,
				FunctionName:        block.FunctionName,
				FunctionVersion:     block.FunctionVersion,
				FunctionAutoUpgrade: block.FunctionAutoUpgrade,
				TargetColumn:        block.TargetColumn,
				ResultType:          block.ResultType,
				Arguments:           block.Arguments,
				Args:                block.Args,
			})
		default:
			err = fmt.Errorf("lightweight_stack_unsupported_block:%s", block.Kind)
		}
		if err != nil {
			return nil, err
		}
		if schema.Known {
			schema = validateTransformStack(node.Node.ID, runtimeTransformStack{Blocks: []runtimeTransformBlock{block}}, schema, &pipelineStrictValidationReport{})
		}
	}
	return out, nil
}

type runtimeFunctionConfig struct {
	FunctionID          string
	FunctionName        string
	FunctionVersion     string
	FunctionAutoUpgrade bool
	TargetColumn        string
	ResultType          string
	Arguments           []runtimeFunctionArgument
	Args                map[string]string
}

func (rt *lightweightTableRuntime) runFunction(ctx context.Context, node executor.NodeContext, cfg tableRuntimeConfig) ([]pipelineexpression.Row, error) {
	rows, err := rt.firstDependencyRows(node)
	if err != nil {
		return nil, err
	}
	return rt.applyFunction(ctx, rows, runtimeFunctionConfig{
		FunctionID:          cfg.FunctionID,
		FunctionName:        cfg.FunctionName,
		FunctionVersion:     cfg.FunctionVersion,
		FunctionAutoUpgrade: cfg.FunctionAutoUpgrade,
		TargetColumn:        cfg.TargetColumn,
		ResultType:          cfg.ResultType,
		Arguments:           cfg.Arguments,
		Args:                cfg.Args,
	})
}

func (rt *lightweightTableRuntime) applyFunction(ctx context.Context, rows []pipelineexpression.Row, cfg runtimeFunctionConfig) ([]pipelineexpression.Row, error) {
	if rt.functions == nil {
		return nil, errors.New("pipeline_function_registry_not_configured")
	}
	def, err := rt.functions.ResolvePipelineFunction(ctx, PipelineFunctionRef{
		ID:          cfg.FunctionID,
		Name:        cfg.FunctionName,
		Version:     cfg.FunctionVersion,
		AutoUpgrade: cfg.FunctionAutoUpgrade,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline_function_resolve_failed: %w", err)
	}
	def = normalisePipelineFunction(def)
	switch def.Runtime {
	case PipelineFunctionRuntimeExpression:
		return applyExpressionFunction(rows, def, cfg)
	case PipelineFunctionRuntimePython:
		return nil, errors.New("python_reusable_function_runtime_not_configured")
	default:
		return nil, fmt.Errorf("unsupported_pipeline_function_runtime:%s", def.Runtime)
	}
}

func applyExpressionFunction(rows []pipelineexpression.Row, def PipelineFunctionDefinition, cfg runtimeFunctionConfig) ([]pipelineexpression.Row, error) {
	if strings.TrimSpace(def.Expression) == "" {
		return nil, fmt.Errorf("pipeline_function_missing_expression:%s", def.Name)
	}
	expr, err := pipelineexpression.ParseExpr(def.Expression)
	if err != nil {
		return nil, fmt.Errorf("pipeline_function_expression_parse:%w", err)
	}
	target := strings.TrimSpace(cfg.TargetColumn)
	if target == "" {
		target = def.Name
	}
	if target == "" {
		return nil, errors.New("pipeline_function_missing_target_column")
	}
	argExprs, err := compileFunctionArguments(def, cfg)
	if err != nil {
		return nil, err
	}
	geometryArguments := make(map[string]bool)
	for _, parameter := range def.Parameters {
		if strings.EqualFold(strings.TrimSpace(parameter.Type), "GEOMETRY") {
			geometryArguments[strings.TrimSpace(parameter.Name)] = true
		}
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		scope := cloneRow(row)
		for name, argExpr := range argExprs {
			// A GEOMETRY column must reach the expression as the original JSON value.
			if geometryArguments[name] && argExpr.Kind == pipelineexpression.ExprColumn {
				if raw, ok := row[argExpr.Name]; ok {
					scope[name] = raw
					continue
				}
			}
			value, err := pipelineexpression.Eval(argExpr, row)
			if err != nil {
				return nil, fmt.Errorf("pipeline_function_argument_eval:%s:%w", name, err)
			}
			scope[name] = value.ToJSON()
		}
		value, err := pipelineexpression.Eval(expr, scope)
		if err != nil {
			return nil, fmt.Errorf("pipeline_function_eval:%s:%w", def.Name, err)
		}
		next := cloneRow(row)
		next[target] = value.ToJSON()
		out = append(out, next)
	}
	return out, nil
}

func compileFunctionArguments(def PipelineFunctionDefinition, cfg runtimeFunctionConfig) (map[string]pipelineexpression.Expr, error) {
	byName := map[string]string{}
	for key, value := range cfg.Args {
		if strings.TrimSpace(key) != "" && strings.TrimSpace(value) != "" {
			byName[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	for _, arg := range cfg.Arguments {
		name := strings.TrimSpace(arg.Name)
		if name == "" {
			continue
		}
		switch {
		case strings.TrimSpace(arg.Expression) != "":
			byName[name] = strings.TrimSpace(arg.Expression)
		case strings.TrimSpace(arg.Column) != "":
			byName[name] = strings.TrimSpace(arg.Column)
		case len(arg.Value) > 0:
			byName[name] = strings.TrimSpace(string(arg.Value))
		}
	}

	compiled := map[string]pipelineexpression.Expr{}
	for _, param := range def.Parameters {
		name := strings.TrimSpace(param.Name)
		if name == "" {
			continue
		}
		exprText := strings.TrimSpace(byName[name])
		if exprText == "" {
			exprText = name
		}
		expr, err := pipelineexpression.ParseExpr(exprText)
		if err != nil {
			return nil, fmt.Errorf("pipeline_function_argument_parse:%s:%w", name, err)
		}
		compiled[name] = expr
	}
	return compiled, nil
}

func (rt *lightweightTableRuntime) runJoin(node executor.NodeContext, draft runtimeJoinDraft) ([]pipelineexpression.Row, error) {
	if len(node.Node.DependsOn) < 2 {
		return nil, errors.New("lightweight_join_requires_two_inputs")
	}
	left, err := rt.dependencyRows(node.Node.DependsOn[0])
	if err != nil {
		return nil, err
	}
	right, err := rt.dependencyRows(node.Node.DependsOn[1])
	if err != nil {
		return nil, err
	}
	joinType := strings.ToLower(strings.TrimSpace(draft.JoinType))
	if joinType == "" {
		joinType = "left"
	}
	matches := validJoinMatches(draft.Matches)
	if joinType != "cross" && len(matches) == 0 {
		return nil, errors.New("lightweight_join_missing_match_conditions")
	}

	leftColumns := rt.selectRuntimeJoinColumns(node.Node.DependsOn[0], draft.LeftColumns, draft.AutoSelectLeft || len(draft.LeftColumns) == 0)
	if joinType == runtimeJoinLeftAnti {
		if len(matches) != len(draft.Matches) {
			return nil, errors.New("left anti join requires both columns in each match condition")
		}
		if err := rejectRuntimeJoinColumnCollision(node.Node.ID, leftColumns, nil, ""); err != nil {
			return nil, err
		}
		return joinLeftAnti(left, right, matches, leftColumns)
	}
	rightColumns := rt.selectRuntimeJoinColumns(node.Node.DependsOn[1], draft.RightColumns, draft.AutoSelectRight)
	if err := rejectRuntimeJoinColumnCollision(node.Node.ID, leftColumns, rightColumns, draft.RightPrefix); err != nil {
		return nil, err
	}
	// A cross join has no key columns, so every row shares the empty tuple.
	if joinType == "cross" {
		matches = nil
	}
	leftKeys, rightKeys := joinKeyColumns(matches)
	rightIndex := make(map[string][]int, len(right))
	for i, row := range right {
		key, matchable, err := runtimeCompositeKey(row, rightKeys, false)
		if err != nil {
			return nil, fmt.Errorf("lightweight join: %w", err)
		}
		if matchable {
			rightIndex[key] = append(rightIndex[key], i)
		}
	}

	out := make([]pipelineexpression.Row, 0)
	appendRow := func(lrow, rrow pipelineexpression.Row) error {
		if rt.joinExpansionLimit > 0 && len(out) >= rt.joinExpansionLimit {
			return fmt.Errorf("%w: node %q exceeds %d joined rows", errJoinRowsExceeded, node.Node.ID, rt.joinExpansionLimit)
		}
		out = append(out, composeRuntimeJoinRow(lrow, rrow, leftColumns, rightColumns, draft.RightPrefix))
		return nil
	}
	rightMatched := make([]bool, len(right))
	for _, lrow := range left {
		key, matchable, err := runtimeCompositeKey(lrow, leftKeys, false)
		if err != nil {
			return nil, fmt.Errorf("lightweight join: %w", err)
		}
		var hits []int
		if matchable {
			hits = rightIndex[key]
		}
		for _, i := range hits {
			rightMatched[i] = true
			if err := appendRow(lrow, right[i]); err != nil {
				return nil, err
			}
		}
		if len(hits) == 0 && (joinType == "left" || joinType == "outer") {
			if err := appendRow(lrow, nil); err != nil {
				return nil, err
			}
		}
	}
	if joinType == "right" || joinType == "outer" {
		for i, rrow := range right {
			if !rightMatched[i] {
				if err := appendRow(nil, rrow); err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

func (rt *lightweightTableRuntime) runUnion(node executor.NodeContext, draft runtimeUnionDraft) ([]pipelineexpression.Row, error) {
	if len(node.Node.DependsOn) < 2 {
		return nil, errors.New("lightweight_union_requires_two_inputs")
	}
	batches := make([][]pipelineexpression.Row, 0, len(node.Node.DependsOn))
	for _, dep := range node.Node.DependsOn {
		rows, err := rt.dependencyRows(dep)
		if err != nil {
			return nil, err
		}
		batches = append(batches, rows)
	}
	if draft.byPosition() {
		return rt.alignPositionalUnion(node.Node.DependsOn, batches)
	}
	columns := []string{}
	seen := map[string]struct{}{}
	for _, batch := range batches {
		for _, col := range deriveRuntimeColumns(batch) {
			if _, ok := seen[col]; ok {
				continue
			}
			seen[col] = struct{}{}
			columns = append(columns, col)
		}
	}
	out := []pipelineexpression.Row{}
	for _, batch := range batches {
		for _, row := range batch {
			next := pipelineexpression.Row{}
			for _, col := range columns {
				if value, ok := row[col]; ok {
					next[col] = append(json.RawMessage(nil), value...)
				} else {
					next[col] = json.RawMessage("null")
				}
			}
			out = append(out, next)
		}
	}
	return out, nil
}

func (rt *lightweightTableRuntime) firstDependencyRows(node executor.NodeContext) ([]pipelineexpression.Row, error) {
	if len(node.Node.DependsOn) == 0 {
		return nil, fmt.Errorf("lightweight_table_missing_input:%s", node.Node.ID)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, dep := range node.Node.DependsOn {
		if table, ok := rt.tables[dep]; ok {
			return cloneRows(table.rows), nil
		}
	}
	return nil, fmt.Errorf("lightweight_table_input_not_ready:%s", strings.Join(node.Node.DependsOn, ","))
}

func (rt *lightweightTableRuntime) dependencyRows(nodeID string) ([]pipelineexpression.Row, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	table, ok := rt.tables[nodeID]
	if !ok {
		return nil, fmt.Errorf("lightweight_table_input_not_ready:%s", nodeID)
	}
	return cloneRows(table.rows), nil
}

func (rt *lightweightTableRuntime) storeRows(nodeID string, rows []pipelineexpression.Row) {
	rt.storeTable(nodeID, lightweightTable{rows: rows})
}

func (rt *lightweightTableRuntime) snapshotRows(nodeID string) []pipelineexpression.Row {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return cloneRows(rt.tables[nodeID].rows)
}

func (rt *lightweightTableRuntime) trimRows(nodeID string, limit int) {
	if limit <= 0 {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if table, ok := rt.tables[nodeID]; ok && len(table.rows) > limit {
		table.rows = cloneRows(table.rows[:limit])
		rt.tables[nodeID] = table
	}
}

func applyRuntimeFilterBlock(rows []pipelineexpression.Row, block runtimeTransformBlock) ([]pipelineexpression.Row, error) {
	if len(block.Conditions) == 0 {
		return nil, errors.New("lightweight_stack_filter_missing_conditions")
	}
	mode := strings.ToLower(strings.TrimSpace(block.Mode))
	if mode == "" {
		mode = "keep"
	}
	match := strings.ToLower(strings.TrimSpace(block.Match))
	if match == "" {
		match = "all"
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		conditionResults := make([]bool, 0, len(block.Conditions))
		for _, condition := range block.Conditions {
			if strings.TrimSpace(condition.Column) == "" {
				return nil, errors.New("lightweight_stack_filter_missing_column")
			}
			conditionResults = append(conditionResults, runtimeConditionMatches(row, condition))
		}
		ok := match == "any" && anyBool(conditionResults)
		if match != "any" {
			ok = allBool(conditionResults)
		}
		if mode == "drop" {
			ok = !ok
		}
		if ok {
			out = append(out, cloneRow(row))
		}
	}
	return out, nil
}

// applyRuntimeDropBlock removes the configured columns. A column that does not
// exist upstream is a hard error — the strict schema validation already reports
// it as missing_column, so a silent no-op would let a preview pass where the
// build rejects (or a renamed-away column silently survive). declared lists the
// dependency's declared schema columns; rows alone may miss sparse ones.
func applyRuntimeDropBlock(rows []pipelineexpression.Row, columns []string, declared []string) ([]pipelineexpression.Row, error) {
	drop := make([]string, 0, len(columns))
	for _, col := range columns {
		if strings.TrimSpace(col) != "" {
			drop = append(drop, col)
		}
	}
	if err := requireRuntimeColumns(availableColumnNames(rows, declared), drop); err != nil {
		return nil, err
	}
	remove := map[string]struct{}{}
	for _, col := range drop {
		remove[col] = struct{}{}
	}
	out := cloneRows(rows)
	for _, row := range out {
		for col := range remove {
			delete(row, col)
		}
	}
	return out, nil
}

// requireRuntimeColumns reports every requested name the oracle does not know,
// using the strict validation's missing_column wording so preview failures and
// build failures read identically.
func requireRuntimeColumns(oracle, requested []string) error {
	known := make(map[string]struct{}, len(oracle))
	for _, col := range oracle {
		known[col] = struct{}{}
	}
	missing := make([]string, 0, len(requested))
	for _, col := range requested {
		if _, ok := known[col]; !ok {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	parts := make([]string, 0, len(missing))
	for _, col := range missing {
		parts = append(parts, fmt.Sprintf("column %q does not exist in upstream schema", col))
	}
	return errors.New(strings.Join(parts, "; "))
}

// availableColumnNames merges the names observed in rows with the dependency's
// declared schema so a declared-but-sparse column stays addressable — the same
// merge the lightweight join uses for its projections.
func availableColumnNames(rows []pipelineexpression.Row, declared []string) []string {
	names := deriveRuntimeColumns(rows)
	if len(declared) == 0 {
		return names
	}
	seen := make(map[string]struct{}, len(names)+len(declared))
	for _, name := range names {
		seen[name] = struct{}{}
	}
	for _, name := range declared {
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func applyRuntimeSelectBlock(rows []pipelineexpression.Row, columns []string) []pipelineexpression.Row {
	if len(columns) == 0 {
		return cloneRows(rows)
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		next := pipelineexpression.Row{}
		for _, col := range columns {
			if strings.TrimSpace(col) == "" {
				continue
			}
			if value, ok := row[col]; ok {
				next[col] = append(json.RawMessage(nil), value...)
			}
		}
		out = append(out, next)
	}
	return out
}

func applyRuntimeRenameBlock(rows []pipelineexpression.Row, renames []runtimeRenameMapping) []pipelineexpression.Row {
	out := cloneRows(rows)
	for _, row := range out {
		for _, mapping := range renames {
			if strings.TrimSpace(mapping.From) == "" || strings.TrimSpace(mapping.To) == "" {
				continue
			}
			if value, ok := row[mapping.From]; ok {
				row[mapping.To] = append(json.RawMessage(nil), value...)
				delete(row, mapping.From)
			}
		}
	}
	return out
}

// applyRuntimeNormalizeBlock renames every column with the shared snake_case
// rule. Two upstream names that normalize to the same target, or a name that
// normalizes to empty, are hard errors: keeping whichever source happened to
// iterate last would drop the other column nondeterministically.
func applyRuntimeNormalizeBlock(rows []pipelineexpression.Row, removeSpecial bool, declared []string) ([]pipelineexpression.Row, error) {
	sources := map[string][]string{}
	for _, name := range availableColumnNames(rows, declared) {
		target := normalizeRuntimeColumnName(name, removeSpecial)
		if target == "" {
			return nil, fmt.Errorf("column %q normalizes to an empty name", name)
		}
		sources[target] = append(sources[target], name)
	}
	targets := make([]string, 0, len(sources))
	for target := range sources {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for _, target := range targets {
		if clash := sources[target]; len(clash) > 1 {
			sort.Strings(clash)
			quoted := make([]string, 0, len(clash))
			for _, name := range clash {
				quoted = append(quoted, fmt.Sprintf("%q", name))
			}
			return nil, fmt.Errorf("duplicate column name %q (from %s)", target, strings.Join(quoted, ", "))
		}
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		next := pipelineexpression.Row{}
		for col, value := range row {
			next[normalizeRuntimeColumnName(col, removeSpecial)] = append(json.RawMessage(nil), value...)
		}
		out = append(out, next)
	}
	return out, nil
}

// applyRuntimeCastBlock converts one column to a supported scalar type. Nulls
// stay null, and a value the target type cannot represent fails the whole
// transform with the column and row — mirroring the unit-conversion block —
// instead of silently publishing the original value under a schema that
// claims the target type.
func applyRuntimeCastBlock(rows []pipelineexpression.Row, block runtimeTransformBlock, declared []string) ([]pipelineexpression.Row, error) {
	source := strings.TrimSpace(block.SourceColumn)
	if source == "" {
		return nil, errors.New("cast block requires source_column")
	}
	if !validRuntimeCastTarget(block.TargetType) {
		return nil, fmt.Errorf("cast target %q is not a supported type", block.TargetType)
	}
	target := strings.TrimSpace(block.TargetColumn)
	if target == "" {
		target = source
	}
	if err := requireRuntimeColumns(availableColumnNames(rows, declared), []string{source}); err != nil {
		return nil, err
	}
	if target != source {
		for _, name := range availableColumnNames(rows, declared) {
			if name == target {
				return nil, fmt.Errorf("duplicate column name %q", target)
			}
		}
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for index, row := range rows {
		next := cloneRow(row)
		raw, ok := row[source]
		if !ok || isRuntimeNullish(raw, false) {
			delete(next, source)
			next[target] = json.RawMessage("null")
			out = append(out, next)
			continue
		}
		value, err := columnCastValue(raw, block.TargetType)
		if err != nil {
			return nil, fmt.Errorf("cast column %q at row %d: %w", source, index+1, err)
		}
		if target != source {
			delete(next, source)
		}
		next[target] = value
		out = append(out, next)
	}
	return out, nil
}

func runtimeConditionMatches(row pipelineexpression.Row, condition runtimeFilterCondition) bool {
	raw, ok := row[condition.Column]
	nullish := !ok || isRuntimeNullish(raw, condition.TreatEmptyAsNull)
	switch strings.ToLower(strings.TrimSpace(condition.Operator)) {
	case "is_null":
		return nullish
	case "is_not_null", "":
		return !nullish
	case "equals":
		return runtimeScalarString(raw) == condition.Value
	case "not_equals":
		return runtimeScalarString(raw) != condition.Value
	case "greater_than":
		left, lok := runtimeScalarFloat(raw)
		right, rok := strconv.ParseFloat(condition.Value, 64)
		return lok && rok == nil && left > right
	case "less_than":
		left, lok := runtimeScalarFloat(raw)
		right, rok := strconv.ParseFloat(condition.Value, 64)
		return lok && rok == nil && left < right
	default:
		return false
	}
}

func isRuntimeNullish(raw json.RawMessage, treatEmptyAsNull bool) bool {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return true
	}
	if !treatEmptyAsNull {
		return false
	}
	return runtimeScalarString(raw) == ""
}

func runtimeScalarString(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		if b {
			return "true"
		}
		return "false"
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return strings.TrimSpace(string(raw))
}

func runtimeScalarFloat(raw json.RawMessage) (float64, bool) {
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return f, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		return parsed, err == nil
	}
	return 0, false
}

// validRuntimeCastTarget reports whether the cast block's target type is one
// the lightweight runtime can materialize (the catalog's scalar set plus the
// historical aliases). Anything else fails instead of passing values through.
func validRuntimeCastTarget(targetType string) bool {
	switch strings.ToLower(strings.TrimSpace(targetType)) {
	case "integer", "long", "int", "bigint", "double", "float", "boolean", "bool",
		"string", "varchar", "date", "timestamp":
		return true
	}
	return false
}

// normalizeRuntimeColumnName maps one column name onto the snake_case style the
// transform catalog promises. An underscore appears where a lowercase letter or
// digit is followed by an uppercase letter (OrderID -> order_id), matching the
// editor's live preview; runs of spaces and hyphens collapse to one underscore
// and surrounding underscores trim away. removeSpecial drops every remaining
// symbol, while unicode letters and digits always survive.
func normalizeRuntimeColumnName(name string, removeSpecial bool) string {
	var b strings.Builder
	var prevUnderscore bool
	var prev rune
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' && ((prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9')) {
			b.WriteRune('_')
			prevUnderscore = true
		}
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(unicode.ToLower(r))
			prevUnderscore = false
		case r == '_' || unicode.IsSpace(r) || r == '-':
			if !prevUnderscore {
				b.WriteRune('_')
				prevUnderscore = true
			}
		case !removeSpecial:
			b.WriteRune(unicode.ToLower(r))
			prevUnderscore = false
		}
		prev = r
	}
	return strings.Trim(b.String(), "_")
}

func validJoinMatches(matches []runtimeJoinMatch) []runtimeJoinMatch {
	out := make([]runtimeJoinMatch, 0, len(matches))
	for _, match := range matches {
		if strings.TrimSpace(match.LeftColumn) != "" && strings.TrimSpace(match.RightColumn) != "" {
			out = append(out, match)
		}
	}
	return out
}

func joinKeyColumns(matches []runtimeJoinMatch) (left, right []string) {
	for _, match := range matches {
		left = append(left, match.LeftColumn)
		right = append(right, match.RightColumn)
	}
	return left, right
}

func selectRuntimeColumns(rows []pipelineexpression.Row, explicit []string, auto bool) []string {
	if !auto {
		return append([]string(nil), explicit...)
	}
	return deriveRuntimeColumns(rows)
}

// selectRuntimeJoinColumns picks one join side's projection. Explicit lists
// win verbatim; auto-selection uses the dependency's table schema (declared
// types merged with the observed rows), so an empty or sparse input keeps
// its declared columns — NULL-filled by composeRuntimeJoinRow and still
// visible to the duplicate-column check — instead of silently dropping them.
// The geospatial join keeps selectRuntimeColumns.
func (rt *lightweightTableRuntime) selectRuntimeJoinColumns(dep string, explicit []string, auto bool) []string {
	if !auto {
		return append([]string(nil), explicit...)
	}
	return rt.tableSchema(dep).columnNames()
}

// errJoinDuplicateColumn marks a lightweight join whose composed output
// names one column twice; the wrapped message is the strict validation's
// own duplicate_column_name text.
var (
	errJoinDuplicateColumn = errors.New("lightweight_join_duplicate_column_name")
	errJoinRowsExceeded    = errors.New("lightweight_join_rows_exceeded")
)

// rejectRuntimeJoinColumnCollision keeps the lightweight join consistent
// with the strict build validation: it feeds the composed output names
// (left selection, then right selection under the same prefixing rule
// composeRuntimeJoinRow applies) through the shared checkDuplicateNames
// rule, so an unprefixed right column colliding with a left column, or any
// duplicate inside an explicit projection — the left anti join's left-only
// projection included — fails with the same duplicate_column_name message
// instead of silently letting one side overwrite the other.
func rejectRuntimeJoinColumnCollision(nodeID string, leftColumns, rightColumns []string, rightPrefix string) error {
	names := make([]string, 0, len(leftColumns)+len(rightColumns))
	names = append(names, leftColumns...)
	prefix := strings.TrimSpace(rightPrefix)
	for _, column := range rightColumns {
		target := column
		if prefix != "" {
			target = prefix + column
		}
		names = append(names, target)
	}
	report := pipelineStrictValidationReport{}
	checkDuplicateNames(nodeID, names, "duplicate_column_name", &report)
	if len(report.Errors) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", errJoinDuplicateColumn, report.Errors[0].Message)
}

func composeRuntimeJoinRow(left, right pipelineexpression.Row, leftColumns, rightColumns []string, rightPrefix string) pipelineexpression.Row {
	out := pipelineexpression.Row{}
	for _, col := range leftColumns {
		if left != nil {
			if value, ok := left[col]; ok {
				out[col] = append(json.RawMessage(nil), value...)
				continue
			}
		}
		out[col] = json.RawMessage("null")
	}
	for _, col := range rightColumns {
		target := col
		if strings.TrimSpace(rightPrefix) != "" {
			target = strings.TrimSpace(rightPrefix) + col
		}
		if right != nil {
			if value, ok := right[col]; ok {
				out[target] = append(json.RawMessage(nil), value...)
				continue
			}
		}
		out[target] = json.RawMessage("null")
	}
	return out
}

func anyBool(values []bool) bool {
	for _, value := range values {
		if value {
			return true
		}
	}
	return false
}

func allBool(values []bool) bool {
	for _, value := range values {
		if !value {
			return false
		}
	}
	return true
}

func normaliseTableTransform(transformType string) string {
	switch strings.ToLower(strings.TrimSpace(transformType)) {
	case "dataset_input", "input", "source", "source_dataset", "table_input", "external":
		return "input"
	case "filter", "row_filter":
		return "filter"
	case "select", "project", "projection":
		return "select"
	case "drop", "drop_columns":
		return "drop"
	case "rename", "rename_columns":
		return "rename"
	case "passthrough", "noop":
		return "passthrough"
	case "output", "dataset_output", "output_dataset", "table_output", "output_object_type", "object_type_output", "output_link_type", "link_type_output":
		return "output"
	case "sql", "structured_sql", "join", "table_join", "union", "union_all", "transform_stack",
		"geo_join", "geospatial_join", "geometry_join",
		"geo_distance_join", "geometry_distance_join",
		"geo_intersection_join", "geometry_intersection_join",
		"geo_nearest_join", "geo_nearest_neighbor_join", "geometry_nearest_neighbor_join", "knn_geo_join":
		return "sql"
	case "function", "function_call", "udf", "reusable_function":
		return "function"
	case "gpx_parse", "parse_gpx", "gpx", "gpx_parser":
		return "gpx_parse"
	default:
		return strings.ToLower(strings.TrimSpace(transformType))
	}
}

func syntheticRows(nodeID string, sampleSize int) []pipelineexpression.Row {
	if sampleSize <= 0 || sampleSize > 100 {
		sampleSize = 10
	}
	out := make([]pipelineexpression.Row, 0, sampleSize)
	for i := 0; i < sampleSize; i++ {
		out = append(out, pipelineexpression.Row{
			"id":          mustRuntimeJSON(i),
			"source_node": mustRuntimeJSON(nodeID),
			"synthetic":   json.RawMessage("true"),
			"value":       mustRuntimeJSON(i * 10),
		})
	}
	return out
}

func cloneRows(rows []pipelineexpression.Row) []pipelineexpression.Row {
	out := make([]pipelineexpression.Row, len(rows))
	for i, row := range rows {
		out[i] = cloneRow(row)
	}
	return out
}

func cloneRow(row pipelineexpression.Row) pipelineexpression.Row {
	out := make(pipelineexpression.Row, len(row))
	for k, v := range row {
		out[k] = append(json.RawMessage(nil), v...)
	}
	return out
}

func rowsToMaps(rows []pipelineexpression.Row) []map[string]json.RawMessage {
	out := make([]map[string]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		next := map[string]json.RawMessage{}
		for k, v := range row {
			next[k] = append(json.RawMessage(nil), v...)
		}
		out = append(out, next)
	}
	return out
}

func deriveRuntimeColumns(rows []pipelineexpression.Row) []string {
	if len(rows) == 0 {
		return []string{}
	}
	seen := map[string]struct{}{}
	for _, row := range rows {
		for col := range row {
			seen[col] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for col := range seen {
		out = append(out, col)
	}
	sort.Strings(out)
	return out
}

func hashRuntimeRows(nodeID, transformType string, rows []pipelineexpression.Row) string {
	payload, _ := json.Marshal(rowsToMaps(rows))
	sum := sha256.Sum256(append([]byte(nodeID+"|"+transformType+"|"), payload...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mustRuntimeJSON(v any) json.RawMessage {
	out, _ := json.Marshal(v)
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
