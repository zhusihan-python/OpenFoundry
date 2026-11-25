import api, { ApiError } from './client';

export interface Dataset {
  id: string;
  rid?: string;
  name: string;
  description: string;
  format: string;
  storage_path: string;
  size_bytes: number;
  row_count: number;
  owner_id: string;
  tags: string[];
  current_version: number;
  active_branch: string;
  metadata?: Record<string, unknown> | null;
  health_status?: string | null;
  current_view_id?: string | null;
  created_at: string;
  updated_at: string;
}

export interface DatasetListResponse {
  data: Dataset[];
  page: number;
  per_page: number;
  total: number;
  total_pages: number;
}

interface DatasetListItemsResponse {
  items: Dataset[];
}

export interface ListDatasetsParams {
  page?: number;
  per_page?: number;
  limit?: number;
  search?: string;
  tag?: string;
  owner_id?: string;
}

export interface CreateDatasetParams {
  name: string;
  description?: string;
  format?: string;
  tags?: string[];
}

export interface UpdateDatasetParams {
  name?: string;
  description?: string;
  owner_id?: string;
  tags?: string[];
}

export interface DatasetVersion {
  id: string;
  dataset_id: string;
  version: number;
  message: string;
  size_bytes: number;
  row_count: number;
  storage_path: string;
  transaction_id?: string | null;
  created_at: string;
}

export interface DatasetSnapshot {
  id: string;
  branch: string;
  version: number;
}

interface DatasetSnapshotBinding {
  branch: string;
  version: number;
  transaction_id: string;
}

export interface DatasetSnapshotSchema extends DatasetSnapshotBinding {
  dataset_id: string;
  fields: Array<{ name: string; field_type: string; nullable: boolean }>;
}

export type DatasetComparedRow = Record<string, string | null>;

export interface DatasetRowComparison {
  dataset_id: string;
  key: string;
  base: DatasetSnapshotBinding;
  target: DatasetSnapshotBinding;
  counts: Record<'added' | 'removed' | 'modified' | 'unchanged', number>;
  changes: Array<{
    key: string;
    kind: 'added' | 'removed' | 'modified';
    before?: DatasetComparedRow;
    after?: DatasetComparedRow;
  }>;
  changes_truncated: boolean;
  next_cursor?: string;
}

export interface DatasetPreviewResponse {
  dataset_id: string;
  version?: number;
  transaction_id?: string;
  schema_version?: string;
  schema?: DatasetSchemaPayload;
  next_cursor?: string | null;
  size_bytes?: number;
  format?: string;
  branch?: string | null;
  storage_path?: string;
  limit?: number;
  offset?: number;
  row_count?: number;
  rows?: Array<Record<string, unknown>>;
  columns?: Array<{
    name: string;
    field_type?: string;
    data_type?: string;
    nullable?: boolean;
  }>;
  total_rows?: number;
  warnings?: string[];
  errors?: string[];
  message?: string;
}

export interface DatasetSchema {
  id: string;
  dataset_id: string;
  fields: unknown;
  created_at: string;
}

// ─── T6.x — view-scoped Foundry schema ───────────────────────────────────────
// Mirrors `services/dataset-versioning-service/src/models/schema.rs`. The
// `type` discriminator on `DatasetField.field_type` matches the JSON layout
// produced by the Rust serde model.

export type DatasetFileFormat = 'PARQUET' | 'AVRO' | 'TEXT';

export type DatasetFieldType =
  | { type: 'BOOLEAN' }
  | { type: 'BYTE' }
  | { type: 'SHORT' }
  | { type: 'INTEGER' }
  | { type: 'LONG' }
  | { type: 'FLOAT' }
  | { type: 'DOUBLE' }
  | { type: 'STRING' }
  | { type: 'BINARY' }
  | { type: 'DATE' }
  | { type: 'TIMESTAMP' }
  | { type: 'DECIMAL'; precision?: number; scale?: number }
  | { type: 'ARRAY'; arraySubType?: DatasetField }
  | { type: 'MAP'; mapKeyType?: DatasetField; mapValueType?: DatasetField }
  | { type: 'STRUCT'; subSchemas?: DatasetField[] };

export interface DatasetField {
  name: string;
  type: DatasetFieldType['type'];
  nullable?: boolean;
  description?: string;
  // Decimal
  precision?: number;
  scale?: number;
  // Array
  arraySubType?: DatasetField;
  // Map
  mapKeyType?: DatasetField;
  mapValueType?: DatasetField;
  // Struct
  subSchemas?: DatasetField[];
}

export interface DatasetCsvOptions {
  delimiter: string;
  quote: string;
  escape: string;
  header: boolean;
  null_value: string;
  date_format?: string;
  timestamp_format?: string;
  charset: string;
}

export interface DatasetCustomMetadata {
  csv?: DatasetCsvOptions;
}

export interface DatasetSchemaPayload {
  fields: DatasetField[];
  file_format: DatasetFileFormat;
  custom_metadata?: DatasetCustomMetadata | null;
}

export interface DatasetSchemaResponse {
  view_id: string;
  dataset_id: string;
  branch?: string | null;
  schema: DatasetSchemaPayload;
  content_hash: string;
  created_at: string;
  unchanged?: boolean;
}

export interface DatasetBranch {
  id: string;
  dataset_id: string;
  name: string;
  version: number;
  base_version?: number;
  description: string;
  is_default: boolean;
  created_at: string;
  updated_at: string;
  // P1 — Foundry-style branch model. Optional so the type stays
  // backwards-compatible with the legacy payload that still surfaces
  // through `handlers::branches`.
  rid?: string;
  dataset_rid?: string;
  parent_branch_id?: string | null;
  head_transaction_id?: string | null;
  created_from_transaction_id?: string | null;
  last_activity_at?: string;
  fallback_chain?: string[];
  labels?: Record<string, string>;
  has_open_transaction?: boolean;
  // P4 — retention + archival columns. Optional so legacy payloads
  // that pre-date the `20260504000030_branch_retention` migration
  // still parse cleanly.
  retention_policy?: 'INHERITED' | 'FOREVER' | 'TTL_DAYS';
  retention_ttl_days?: number | null;
  archived_at?: string | null;
  archive_grace_until?: string | null;
}

/// P3 — wire shape used by the BranchGraph + dashboard.
export interface DatasetBranchAncestor {
  rid: string;
  name: string;
  is_root: boolean;
}

export interface DatasetBranchPreviewDelete {
  branch: string;
  branch_rid: string;
  current_parent: string | null;
  current_parent_rid: string | null;
  children_to_reparent: Array<{
    branch: string;
    branch_rid: string;
    new_parent: string | null;
    new_parent_rid: string | null;
  }>;
  transactions_preserved: boolean;
  head_transaction: { id: string; rid: string } | null;
}

export interface DatasetBranchDeleteResponse {
  branch: string;
  branch_rid: string;
  reparented: Array<{
    child_branch: string;
    child_branch_rid: string;
    new_parent: string | null;
    new_parent_rid: string | null;
  }>;
}

export interface BranchSourceFromBranch { from_branch: string; }
export interface BranchSourceFromTransaction { from_transaction_rid: string; }
export interface BranchSourceAsRoot { as_root: true; }
export type BranchSource =
  | BranchSourceFromBranch
  | BranchSourceFromTransaction
  | BranchSourceAsRoot;

export interface CreateBranchV2Params {
  name: string;
  source: BranchSource;
  fallback_chain?: string[];
  labels?: Record<string, string>;
  description?: string;
}

export interface ReparentBranchParams {
  new_parent_branch?: string | null;
}

export interface DatasetJobSpecStatus {
  has_master_jobspec: boolean;
  branches_with_jobspec: string[];
}

export interface DatasetJobSpecRow {
  id: string;
  rid: string;
  pipeline_rid: string;
  branch_name: string;
  output_dataset_rid: string;
  output_branch: string;
  job_spec_json: unknown;
  inputs: unknown;
  content_hash: string;
  version: number;
  published_by: string;
  published_at: string;
}

export interface DatasetView {
  id: string;
  dataset_id: string;
  name: string;
  description: string;
  sql_text: string;
  source_branch?: string | null;
  source_version?: number | null;
  materialized: boolean;
  refresh_on_source_update: boolean;
  format: string;
  current_version: number;
  storage_path?: string | null;
  row_count: number;
  schema_fields: unknown;
  last_refreshed_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface CreateDatasetViewParams {
  name: string;
  description?: string;
  sql: string;
  source_branch?: string;
  source_version?: number;
  materialized?: boolean;
  refresh_on_source_update?: boolean;
}

export interface DatasetFilesystemEntry {
  entry_type: 'directory' | 'file';
  name: string;
  path: string;
  storage_path?: string;
  size_bytes?: number;
  last_modified?: string;
  content_type?: string | null;
  metadata: Record<string, unknown>;
}

export interface DatasetFilesystemResponse {
  dataset_id: string;
  requested_path: string;
  root: string;
  current_version: number;
  active_branch: string;
  entries: DatasetFilesystemEntry[];
  items: DatasetFilesystemEntry[];
  breadcrumbs: Array<{ name: string; path: string }>;
  sections: {
    versions: number;
    branches: number;
    views: number;
  };
}

export interface DatasetTransaction {
  id: string;
  dataset_id: string;
  view_id?: string | null;
  operation: string;
  branch_id?: string;
  branch_name?: string | null;
  status: string;
  summary: string;
  metadata: Record<string, unknown>;
  created_at: string;
  committed_at?: string | null;
}

export interface CreateDatasetBranchParams {
  name: string;
  source_version?: number;
  description?: string;
}

export interface CatalogTagFacet {
  value: string;
  count: number;
}

export interface CatalogOwnerFacet {
  owner_id: string;
  count: number;
}

export interface DatasetCatalogFacets {
  tags: CatalogTagFacet[];
  owners: CatalogOwnerFacet[];
}

export interface DatasetValueCount {
  value: string;
  count: number;
}

export interface DatasetColumnProfile {
  name: string;
  field_type: string;
  nullable: boolean;
  null_count: number;
  null_rate: number;
  distinct_count: number;
  uniqueness_rate: number;
  sample_values: DatasetValueCount[];
  min_value: string | null;
  max_value: string | null;
  average_value: number | null;
}

export interface DatasetRuleResult {
  rule_id: string;
  name: string;
  rule_type: string;
  severity: string;
  passed: boolean;
  measured_value: string | null;
  message: string;
}

export interface DatasetQualityProfile {
  row_count: number;
  column_count: number;
  duplicate_rows: number;
  completeness_ratio: number;
  uniqueness_ratio: number;
  generated_at: string;
  columns: DatasetColumnProfile[];
  rule_results: DatasetRuleResult[];
}

export interface DatasetQualityRule {
  id: string;
  dataset_id: string;
  name: string;
  rule_type: string;
  severity: string;
  config: Record<string, unknown>;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface DatasetQualityHistoryEntry {
  id: string;
  dataset_id: string;
  score: number;
  passed_rules: number;
  failed_rules: number;
  alerts_count: number;
  created_at: string;
}

export interface DatasetQualityAlert {
  id: string;
  dataset_id: string;
  level: string;
  kind: string;
  message: string;
  status: string;
  details: Record<string, unknown>;
  created_at: string;
  resolved_at: string | null;
}

export interface DatasetQualityResponse {
  profile: DatasetQualityProfile | null;
  score: number | null;
  history: DatasetQualityHistoryEntry[];
  alerts: DatasetQualityAlert[];
  rules: DatasetQualityRule[];
  profiled_at: string | null;
}

export interface DatasetBuildParams {
  branch?: string;
  reason?: string;
}

export interface DatasetBuildResponse {
  id?: string;
  rid?: string;
  build_id?: string;
  status?: string;
  state?: string;
  message?: string;
  [key: string]: unknown;
}

export interface DatasetExportParams {
  format: 'CSV' | 'PARQUET';
  branch?: string;
  version?: number;
  include_schema?: boolean;
}

export interface DatasetExportResponse {
  id?: string;
  rid?: string;
  export_id?: string;
  status?: string;
  state?: string;
  download_url?: string;
  message?: string;
  [key: string]: unknown;
}

export interface DatasetLintSummary {
  resource_posture: string;
  total_findings: number;
  high_severity: number;
  medium_severity: number;
  low_severity: number;
  tracked_versions: number;
  branch_count: number;
  stale_branch_count: number;
  materialized_view_count: number;
  auto_refresh_view_count: number;
  transaction_count: number;
  failed_transaction_count: number;
  pending_transaction_count: number;
  enabled_rule_count: number;
  active_alert_count: number;
  object_count: number;
  small_file_count: number;
  largest_object_bytes: number;
  average_object_size_bytes: number;
  quality_score: number | null;
}

export interface DatasetLintFinding {
  code: string;
  title: string;
  severity: string;
  category: string;
  description: string;
  evidence: string[];
  impact: string;
  recommendation: string;
}

export interface DatasetLintRecommendation {
  code: string;
  priority: string;
  title: string;
  rationale: string;
  actions: string[];
}

export interface DatasetLintResponse {
  dataset_id: string;
  dataset_name: string;
  analyzed_at: string;
  summary: DatasetLintSummary;
  findings: DatasetLintFinding[];
  recommendations: DatasetLintRecommendation[];
}

export interface CreateDatasetQualityRuleParams {
  name: string;
  rule_type: string;
  severity?: string;
  enabled?: boolean;
  config: Record<string, unknown>;
}

export interface UpdateDatasetQualityRuleParams {
  name?: string;
  severity?: string;
  enabled?: boolean;
  config?: Record<string, unknown>;
}

export async function listDatasets(params?: ListDatasetsParams): Promise<DatasetListResponse> {
  const query = new URLSearchParams();
  const page = Math.max(1, params?.page ?? 1);
  const perPage = Math.max(1, params?.per_page ?? params?.limit ?? 100);
  const needsClientFiltering = Boolean(params?.search || params?.tag || params?.owner_id);
  const limit = params?.limit ?? (needsClientFiltering ? Math.max(perPage, 500) : perPage);

  query.set('page', String(page));
  query.set('per_page', String(perPage));
  query.set('limit', String(limit));
  if (params?.search) query.set('search', params.search);
  if (params?.tag) query.set('tag', params.tag);
  if (params?.owner_id) query.set('owner_id', params.owner_id);
  const qs = query.toString();
  const response = await api.get<DatasetListResponse | DatasetListItemsResponse>(`/datasets${qs ? `?${qs}` : ''}`);

  if ('data' in response) return response;

  const search = params?.search?.trim().toLowerCase();
  const tag = params?.tag?.trim().toLowerCase();
  const ownerId = params?.owner_id?.trim().toLowerCase();
  const filtered = response.items.filter((dataset) => {
    const datasetTags = dataset.tags ?? [];
    const searchable = [
      dataset.name,
      dataset.description,
      dataset.id,
      dataset.rid,
    ].filter(Boolean).join(' ').toLowerCase();
    const matchesSearch = !search
      || searchable.includes(search)
      || datasetTags.some((item) => item.toLowerCase().includes(search));
    const matchesTag = !tag || datasetTags.some((item) => item.toLowerCase() === tag);
    const matchesOwner = !ownerId || dataset.owner_id?.toLowerCase() === ownerId;
    return matchesSearch && matchesTag && matchesOwner;
  });
  const start = (page - 1) * perPage;
  const data = filtered.slice(start, start + perPage);

  return {
    data,
    page,
    per_page: perPage,
    total: filtered.length,
    total_pages: Math.max(1, Math.ceil(filtered.length / perPage)),
  };
}

export function getCatalogFacets() {
  return api.get<DatasetCatalogFacets>('/datasets/catalog/facets');
}

export function getDataset(id: string) {
  return api.get<Dataset>(`/datasets/${id}`);
}

// The preview backend now also emits the unified rows-reader shape
// (`columns: string[]` + `rows: JSON[][]`, used by Excel imports and other
// normalized NDJSON datasets) while older surfaces still return object rows
// with `{name, ...}` columns. Normalize the positional shape into the
// object shape every current consumer (VirtualizedPreviewTable, Fusion,
// Contour, lineage, wizards) expects; object-shaped responses pass through.
type DatasetPreviewWireResponse = Omit<DatasetPreviewResponse, 'columns' | 'rows'> & {
  columns?: Array<string | NonNullable<DatasetPreviewResponse['columns']>[number]>;
  rows?: Array<Record<string, unknown> | unknown[]>;
};

function normalizeDatasetPreviewResponse(
  response: DatasetPreviewWireResponse,
): DatasetPreviewResponse {
  if (!Array.isArray(response.columns) || !Array.isArray(response.rows)) {
    return response as DatasetPreviewResponse;
  }
  const columns = response.columns.map((column) =>
    typeof column === 'string' ? { name: column } : column,
  );
  const hasPositionalRows =
    response.rows.length > 0 && Array.isArray(response.rows[0]);
  if (!hasPositionalRows || columns.length === 0) {
    // Positional rows without column names cannot be keyed; leave them as-is
    // rather than inventing names the server never sent.
    return { ...response, columns } as DatasetPreviewResponse;
  }
  const names = columns.map((column) => column.name);
  const rows = response.rows.map((row) =>
    // Object.fromEntries defines own properties, so even a `__proto__` column
    // name survives as data instead of hitting the Object.prototype setter.
    Object.fromEntries(
      names.map((name, position) => [
        name,
        Array.isArray(row) ? row[position] ?? null : row[name],
      ]),
    ) as Record<string, unknown>,
  );
  return { ...response, columns, rows } as DatasetPreviewResponse;
}

export async function previewDataset(datasetId: string, params?: { limit?: number; offset?: number; version?: number; branch?: string; cursor?: string }) {
  const query = new URLSearchParams();
  if (params?.limit) query.set('limit', String(params.limit));
  if (params?.offset) query.set('offset', String(params.offset));
  if (params?.version !== undefined) {
    query.set('version', String(params.version));
    query.set('branch', params.branch ?? (await getDataset(datasetId)).active_branch);
  } else if (params?.branch) {
    query.set('branch', params.branch);
  }
  if (params?.cursor) query.set('cursor', params.cursor);
  const qs = query.toString();
  const response = await api.get<DatasetPreviewWireResponse>(`/datasets/${datasetId}/preview${qs ? `?${qs}` : ''}`);
  return normalizeDatasetPreviewResponse(response);
}

// GET /datasets/{id}/schema: Excel-imported (and other normalized) datasets
// return the wrapped `SchemaResponse` shape `{schema: {fields, file_format},
// dataset_id, ...}` while the legacy route returned flat `{fields}`. Unwrap so
// `schema.fields` keeps working for every existing consumer; flat payloads
// pass through unchanged.
export async function getDatasetSchema(datasetId: string): Promise<DatasetSchema> {
  const response = await api.get<DatasetSchema | DatasetSchemaResponse>(`/datasets/${datasetId}/schema`);
  if ('schema' in response && response.schema !== null && !('fields' in response)) {
    return {
      id: response.view_id ?? response.dataset_id,
      dataset_id: response.dataset_id,
      fields: response.schema.fields,
      created_at: response.created_at,
    };
  }
  return response as DatasetSchema;
}

export function getDatasetSchemaForBranch(datasetId: string, branch: string) {
  const query = new URLSearchParams();
  if (branch) query.set('branch', branch);
  const qs = query.toString();
  return api.get<DatasetSchema | DatasetSchemaResponse>(
    `/datasets/${datasetId}/schema${qs ? `?${qs}` : ''}`,
  );
}

function matchesSnapshot(binding: DatasetSnapshotBinding, snapshot: DatasetSnapshot) {
  return binding?.branch === snapshot.branch
    && binding.version === snapshot.version
    && binding.transaction_id === snapshot.id;
}

export function datasetUuid(datasetId: string) {
  return datasetId.toLowerCase().replace(/^ri\.foundry\.main\.dataset\./, '');
}

export async function getDatasetSnapshotSchema(datasetId: string, snapshot: DatasetSnapshot) {
  const query = new URLSearchParams({ branch: snapshot.branch, version: String(snapshot.version) });
  const schema = await api.get<DatasetSnapshotSchema>(`/datasets/${encodeURIComponent(datasetId)}/schema?${query}`);
  if (schema.dataset_id !== datasetUuid(datasetId) || !matchesSnapshot(schema, snapshot)) {
    throw new Error('Schema did not match the selected version.');
  }
  return schema;
}

// A dataset_schema_format_unsupported answer leaves the snapshot without a
// versioned schema to compare. Its rows can still be read.
export function schemaFormatUnsupported(error: unknown): boolean {
  return error instanceof ApiError && error.message.startsWith('dataset_schema_format_unsupported');
}

export async function compareDatasetSnapshotRows(
  datasetId: string,
  base: DatasetSnapshot,
  target: DatasetSnapshot,
  key: string,
  pagination?: { limit: number; cursor?: string },
) {
  const query = new URLSearchParams({
    base_branch: base.branch,
    base_version: String(base.version),
    base_transaction: base.id,
    target_branch: target.branch,
    target_version: String(target.version),
    target_transaction: target.id,
    key,
  });
  if (pagination) {
    query.set('limit', String(pagination.limit));
    if (pagination.cursor !== undefined) query.set('cursor', pagination.cursor);
  }
  const comparison = await api.get<DatasetRowComparison>(`/datasets/${encodeURIComponent(datasetId)}/compare/rows?${query}`);
  if (
    comparison.dataset_id !== datasetUuid(datasetId)
    || comparison.key !== key
    || !matchesSnapshot(comparison.base, base)
    || !matchesSnapshot(comparison.target, target)
  ) {
    throw new Error('Row comparison did not match the selected versions and key.');
  }
  return comparison;
}

export function getViewSchema(datasetId: string, viewId: string) {
  return api.get<DatasetSchemaResponse>(
    `/datasets/${datasetId}/views/${viewId}/schema`,
  );
}

// ─── P2 — view-scoped Foundry preview ─────────────────────────────────────
// Mirrors `services/dataset-versioning-service/src/storage/preview.rs::PreviewPage`.

export interface ViewPreviewCsvOptions {
  delimiter: string;
  quote: string;
  escape: string;
  header: boolean;
  null_value: string;
  date_format?: string | null;
  timestamp_format?: string | null;
  charset: string;
}

export interface ViewPreviewColumn {
  name: string;
  field_type: string;
  nullable: boolean;
}

export interface ViewPreviewResponse {
  view_id: string;
  dataset_id: string;
  branch?: string | null;
  file_format: string;
  text_sub_format?: string | null;
  limit: number;
  offset: number;
  row_count: number;
  total_rows: number;
  columns: ViewPreviewColumn[];
  rows: Array<Record<string, unknown>>;
  schema_inferred: boolean;
  csv_options?: ViewPreviewCsvOptions | null;
  warnings: string[];
  errors: string[];
}

export interface ViewPreviewParams {
  limit?: number;
  offset?: number;
  format?: 'auto' | 'parquet' | 'avro' | 'text';
  csv_delimiter?: string;
  csv_quote?: string;
  csv_escape?: string;
  csv_header?: boolean;
  csv_null_value?: string;
  csv_charset?: string;
  csv_date_format?: string;
  csv_timestamp_format?: string;
  csv?: boolean;
}

export function previewView(
  datasetId: string,
  viewId: string,
  params: ViewPreviewParams = {},
) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue;
    query.set(key, String(value));
  }
  const qs = query.toString();
  return api.get<ViewPreviewResponse>(
    `/datasets/${datasetId}/views/${viewId}/data${qs ? `?${qs}` : ''}`,
  );
}

// ─── P3 — Foundry "Backing filesystem" Files tab ──────────────────────────
// Mirrors `services/dataset-versioning-service/src/handlers/files.rs`.

export type DatasetBackingFileStatus = 'active' | 'deleted';

export interface DatasetBackingFile {
  id: string;
  dataset_id: string;
  transaction_id: string;
  logical_path: string;
  physical_uri: string;
  size_bytes: number;
  sha256?: string | null;
  created_at: string;
  modified_at: string;
  status: DatasetBackingFileStatus;
}

export interface DatasetFilesResponse {
  view_id?: string | null;
  branch: string;
  total: number;
  files: DatasetBackingFile[];
}

export interface ListDatasetFilesParams {
  branch?: string;
  view_id?: string;
  prefix?: string;
}

export function listDatasetFiles(datasetId: string, params: ListDatasetFilesParams = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    query.set(key, String(value));
  }
  const qs = query.toString();
  return api.get<DatasetFilesResponse>(
    `/datasets/${datasetId}/files${qs ? `?${qs}` : ''}`,
  );
}

/** Return the absolute URL the browser should follow to download a
 *  file. The DVS endpoint returns a 302 to a presigned URL; the browser
 *  follows it transparently when this URL is used as the `href` of an
 *  `<a>` or assigned to `window.location`. */
export function datasetFileDownloadUrl(datasetId: string, fileId: string): string {
  return `/api/v1/datasets/${datasetId}/files/${fileId}/download`;
}

export interface DatasetStorageDetails {
  fs_id: string;
  driver: 'local' | 's3' | 'hdfs';
  base_directory: string;
  presign_ttl_seconds: number;
  total_active_bytes: number;
  total_active_files: number;
  total_deleted_bytes: number;
  total_deleted_files: number;
}

export function getDatasetStorageDetails(datasetId: string) {
  return api.get<DatasetStorageDetails>(`/datasets/${datasetId}/storage-details`);
}

export function putViewSchema(
  datasetId: string,
  viewId: string,
  schema: DatasetSchemaPayload,
) {
  return api.post<DatasetSchemaResponse>(
    `/datasets/${datasetId}/views/${viewId}/schema`,
    { schema },
  );
}

export function createDataset(params: CreateDatasetParams) {
  return api.post<Dataset>('/datasets', params);
}

export function updateDataset(id: string, params: UpdateDatasetParams) {
  return api.patch<Dataset>(`/datasets/${id}`, params);
}

export function deleteDataset(id: string) {
  return api.delete(`/datasets/${id}`);
}

export function startDatasetBuild(datasetId: string, params: DatasetBuildParams = {}) {
  return api.post<DatasetBuildResponse>(`/datasets/${datasetId}/builds`, params);
}

export function exportDataset(datasetId: string, params: DatasetExportParams) {
  return api.post<DatasetExportResponse>(`/datasets/${datasetId}/exports`, params);
}

type DatasetListPage<T> = T[] | { data: T[]; next_cursor?: string | null };

async function listDatasetPages<T>(path: string): Promise<T[]> {
  const entries: T[] = [];
  const seen = new Set<string>();
  let cursor: string | null = null;
  do {
    const page: DatasetListPage<T> = await api.get<DatasetListPage<T>>(
      cursor ? `${path}?cursor=${encodeURIComponent(cursor)}` : path,
    );
    if (Array.isArray(page)) return [...entries, ...page];
    entries.push(...page.data);
    cursor = page.next_cursor ?? null;
    if (cursor && seen.has(cursor)) throw new Error('Dataset listing cursor did not advance.');
    if (cursor) seen.add(cursor);
  } while (cursor);
  return entries;
}

type RuntimeDatasetTransaction = Omit<DatasetTransaction, 'operation' | 'created_at'> & {
  tx_type: string;
  started_at: string;
};

function normalizeDatasetTransaction(transaction: DatasetTransaction | RuntimeDatasetTransaction): DatasetTransaction {
  if ('tx_type' in transaction) {
    return { ...transaction, operation: transaction.tx_type, created_at: transaction.started_at };
  }
  return transaction;
}

export function committedDatasetSnapshots(transactions: DatasetTransaction[], versions: DatasetVersion[]) {
  const versionsByTransaction = new Map<string, number>();
  for (const version of versions) {
    if (version.transaction_id && !versionsByTransaction.has(version.transaction_id)) {
      versionsByTransaction.set(version.transaction_id, version.version);
    }
  }
  const history = new Map<string, DatasetSnapshot>();
  for (const transaction of transactions) {
    const version = versionsByTransaction.get(transaction.id);
    if (transaction.status === 'COMMITTED' && transaction.operation === 'SNAPSHOT' && transaction.branch_name && version !== undefined) {
      history.set(transaction.id, { id: transaction.id, branch: transaction.branch_name, version });
    }
  }
  return Array.from(history.values());
}

export function getVersions(datasetId: string) {
  return listDatasetPages<DatasetVersion>(`/datasets/${datasetId}/versions`);
}

export async function listDatasetTransactions(datasetId: string): Promise<DatasetTransaction[]> {
  const transactions = await listDatasetPages<DatasetTransaction | RuntimeDatasetTransaction>(
    `/datasets/${datasetId}/transactions`,
  );
  return transactions.map(normalizeDatasetTransaction);
}

export function missingVersionTransactionIds(versions: DatasetVersion[], recent: DatasetTransaction[]) {
  const seen = new Set(recent.map((transaction) => transaction.id));
  return versions.flatMap(({ transaction_id: id }) => {
    if (!id || seen.has(id)) return [];
    seen.add(id);
    return [id];
  });
}

function fetchTransactionBatch(datasetId: string, ids: string[]) {
  return api.post<Array<{ id: string; status: number; data?: RuntimeDatasetTransaction }>>(
    `/datasets/${encodeURIComponent(datasetId)}/transactions:batchGet`, { ids },
  );
}

export async function getDatasetTransactions(datasetId: string, transactionIds: string[]): Promise<DatasetTransaction[]> {
  // DVS resolves each ID individually inside a batch. Bound the work per request.
  const batchSize = 100;
  const transactions: DatasetTransaction[] = [];
  for (let start = 0; start < transactionIds.length; start += batchSize) {
    const ids = transactionIds.slice(start, start + batchSize);
    const results = await fetchTransactionBatch(datasetId, ids);
    const pending = new Set(ids);
    for (const { id, status, data } of results) {
      if (!pending.delete(id)) throw new Error('Unexpected transaction in dataset history.');
      if (status === 404) continue;
      if (status !== 200 || data?.id !== id || data.dataset_id !== datasetUuid(datasetId)) {
        throw new Error('Transaction did not match the requested dataset history.');
      }
      transactions.push(normalizeDatasetTransaction(data));
    }
    if (pending.size) throw new Error('Incomplete transaction batch in dataset history.');
  }
  return transactions;
}

export async function getDatasetTransaction(datasetId: string, transactionId: string): Promise<DatasetTransaction | null> {
  const [result] = await fetchTransactionBatch(datasetId, [transactionId]);
  return result?.status === 200 && result.data ? normalizeDatasetTransaction(result.data) : null;
}

export function listBranches(datasetId: string) {
  return api.get<DatasetBranch[]>(`/datasets/${datasetId}/branches`);
}

export function createDatasetBranch(datasetId: string, params: CreateDatasetBranchParams) {
  return api.post<DatasetBranch>(`/datasets/${datasetId}/branches`, params);
}

export function checkoutDatasetBranch(datasetId: string, branchName: string) {
  return api.post<Dataset>(`/datasets/${datasetId}/branches/${encodeURIComponent(branchName)}/checkout`, {});
}

export function listDatasetViews(datasetId: string) {
  return api.get<DatasetView[]>(`/datasets/${datasetId}/views`);
}

export function getDatasetView(datasetId: string, viewId: string) {
  return api.get<DatasetView>(`/datasets/${datasetId}/views/${viewId}`);
}

export function createDatasetView(datasetId: string, params: CreateDatasetViewParams) {
  return api.post<DatasetView>(`/datasets/${datasetId}/views`, params);
}

export function refreshDatasetView(datasetId: string, viewId: string) {
  return api.post<DatasetView>(`/datasets/${datasetId}/views/${viewId}/refresh`, {});
}

export function previewDatasetView(datasetId: string, viewId: string, params?: { limit?: number; offset?: number }) {
  const query = new URLSearchParams();
  if (params?.limit) query.set('limit', String(params.limit));
  if (params?.offset) query.set('offset', String(params.offset));
  const qs = query.toString();
  return api.get<Record<string, unknown>>(`/datasets/${datasetId}/views/${viewId}/preview${qs ? `?${qs}` : ''}`);
}

export function listDatasetFilesystem(datasetId: string, params?: { path?: string }) {
  const query = new URLSearchParams();
  if (params?.path) query.set('path', params.path);
  const qs = query.toString();
  return api.get<DatasetFilesystemResponse>(`/datasets/${datasetId}/files${qs ? `?${qs}` : ''}`);
}

export function getDatasetQuality(datasetId: string) {
  return api.get<DatasetQualityResponse>(`/datasets/${datasetId}/quality`);
}

export function getDatasetLint(datasetId: string) {
  return api.get<DatasetLintResponse>(`/datasets/${datasetId}/lint`);
}

export function refreshDatasetQualityProfile(datasetId: string) {
  return api.post<DatasetQualityResponse>(`/datasets/${datasetId}/quality/profile`, {});
}

export function createDatasetQualityRule(datasetId: string, params: CreateDatasetQualityRuleParams) {
  return api.post<DatasetQualityResponse>(`/datasets/${datasetId}/quality/rules`, params);
}

export function updateDatasetQualityRule(datasetId: string, ruleId: string, params: UpdateDatasetQualityRuleParams) {
  return api.patch<DatasetQualityResponse>(`/datasets/${datasetId}/quality/rules/${ruleId}`, params);
}

export function deleteDatasetQualityRule(datasetId: string, ruleId: string) {
  return api.delete<DatasetQualityResponse>(`/datasets/${datasetId}/quality/rules/${ruleId}`);
}

// ─── P4 — Foundry "View retention policies for a dataset [Beta]" ──────────
// Mirrors `services/retention-policy-service/src/handlers/retention.rs`.

export interface RetentionSelector {
  dataset_rid?: string | null;
  project_id?: string | null;
  marking_id?: string | null;
  all_datasets?: boolean;
}

export interface RetentionCriteria {
  transaction_age_seconds?: number | null;
  transaction_state?: string | null;
  view_age_seconds?: number | null;
  last_accessed_seconds?: number | null;
}

export interface RetentionPolicy {
  id: string;
  name: string;
  scope: string;
  target_kind: 'dataset' | 'transaction' | string;
  retention_days: number;
  legal_hold: boolean;
  purge_mode: string;
  rules: string[];
  is_system: boolean;
  selector: RetentionSelector;
  criteria: RetentionCriteria;
  grace_period_minutes: number;
  last_applied_at?: string | null;
  next_run_at?: string | null;
  created_at: string;
  updated_at: string;
  active: boolean;
}

export interface ApplicablePoliciesResponse {
  dataset_rid: string;
  context: {
    project_id?: string | null;
    marking_id?: string | null;
    space_id?: string | null;
    org_id?: string | null;
  };
  inherited: {
    org: RetentionPolicy[];
    space: RetentionPolicy[];
    project: RetentionPolicy[];
  };
  explicit: RetentionPolicy[];
  effective: RetentionPolicy | null;
  conflicts: Array<{ winner_id: string; loser_id: string; reason: string }>;
}

export interface RetentionPreviewTransaction {
  id: string;
  tx_type: string;
  status: string;
  started_at: string;
  committed_at?: string | null;
  would_delete: boolean;
  policy_id?: string | null;
  policy_name?: string | null;
  reason?: string | null;
}

export interface RetentionPreviewFile {
  id: string;
  transaction_id: string;
  logical_path: string;
  physical_uri: string;
  size_bytes: number;
  policy_id: string;
  policy_name: string;
  reason: string;
}

export interface RetentionPreviewResponse {
  dataset_rid: string;
  as_of_days: number;
  as_of: string;
  effective_policy: RetentionPolicy | null;
  transactions: RetentionPreviewTransaction[];
  files: RetentionPreviewFile[];
  summary: {
    transactions_total: number;
    transactions_would_delete: number;
    files_total: number;
    bytes_total: number;
  };
  warnings: string[];
}

export interface ApplicablePoliciesParams {
  project_id?: string;
  marking_id?: string;
  space_id?: string;
  org_id?: string;
}

export function getApplicablePolicies(
  datasetRid: string,
  params: ApplicablePoliciesParams = {},
) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value) query.set(key, value);
  }
  const qs = query.toString();
  return api.get<ApplicablePoliciesResponse>(
    `/datasets/${datasetRid}/applicable-policies${qs ? `?${qs}` : ''}`,
  );
}

export function getRetentionPreview(
  datasetRid: string,
  asOfDays: number,
  params: ApplicablePoliciesParams = {},
) {
  const query = new URLSearchParams();
  query.set('as_of_days', String(asOfDays));
  for (const [key, value] of Object.entries(params)) {
    if (value) query.set(key, value);
  }
  return api.get<RetentionPreviewResponse>(
    `/datasets/${datasetRid}/retention-preview?${query.toString()}`,
  );
}

export interface CreateRetentionPolicyParams {
  name: string;
  target_kind: 'dataset' | 'transaction';
  retention_days: number;
  purge_mode: string;
  selector: RetentionSelector;
  criteria?: RetentionCriteria;
  grace_period_minutes?: number;
  legal_hold?: boolean;
  active?: boolean;
  scope?: string;
  rules?: string[];
  updated_by: string;
}

export function createRetentionPolicy(params: CreateRetentionPolicyParams) {
  // Routed via the gateway's `/api/v1/retention` prefix so it lands on
  // retention-policy-service (the bare `/policies` namespace is owned
  // by authorization-policy-service for RBAC).
  return api.post<RetentionPolicy>(`/retention/policies`, params);
}

export function updateRetentionPolicy(id: string, params: Partial<CreateRetentionPolicyParams>) {
  return api.put<RetentionPolicy>(`/retention/policies/${id}`, params);
}

export function deleteRetentionPolicy(id: string) {
  return api.delete(`/retention/policies/${id}`);
}

// ─── P6 — Dataset health (Foundry "Data Health" + "Health checks") ────────
// Mirrors `services/dataset-quality-service/src/handlers/health.rs`.

export interface DatasetHealthResponse {
  dataset_rid: string;
  dataset_id?: string | null;
  row_count: number;
  col_count: number;
  null_pct_by_column: Record<string, number>;
  freshness_seconds: number;
  last_commit_at?: string | null;
  txn_failure_rate_24h: number;
  last_build_status: 'success' | 'failed' | 'stale' | 'unknown';
  schema_drift_flag: boolean;
  extras: Record<string, unknown>;
  last_computed_at: string;
}

export function getDatasetHealth(datasetRid: string) {
  return api.get<DatasetHealthResponse>(`/datasets/${datasetRid}/health`);
}

// ─── P5 — Marketplace dataset product (publish + install) ─────────────────
// Mirrors `services/marketplace-service/src/handlers/dataset_product.rs`.

export interface DatasetProductManifest {
  entity: 'dataset';
  version: string;
  schema?: unknown;
  retention?: unknown[];
  branching_policy?: unknown;
  schedules?: string[];
  bootstrap: { mode: 'schema-only' | 'with-snapshot' };
}

export interface DatasetProduct {
  id: string;
  name: string;
  source_dataset_rid: string;
  entity_type: 'dataset';
  version: string;
  project_id?: string | null;
  published_by?: string | null;
  export_includes_data: boolean;
  include_schema: boolean;
  include_branches: boolean;
  include_retention: boolean;
  include_schedules: boolean;
  manifest: DatasetProductManifest;
  bootstrap_mode: 'schema-only' | 'with-snapshot';
  published_at: string;
  created_at: string;
}

export interface PublishDatasetProductRequest {
  name: string;
  version?: string;
  project_id?: string;
  export_includes_data?: boolean;
  include_schema?: boolean;
  include_branches?: boolean;
  include_retention?: boolean;
  include_schedules?: boolean;
  bootstrap_mode?: 'schema-only' | 'with-snapshot';
  schema?: unknown;
  retention?: unknown[];
  branching_policy?: unknown;
  schedules?: string[];
}

export function publishDatasetProduct(
  datasetRid: string,
  payload: PublishDatasetProductRequest,
) {
  return api.post<DatasetProduct>(
    `/marketplace/products/from-dataset/${datasetRid}`,
    payload,
  );
}

export interface DatasetProductInstall {
  id: string;
  product_id: string;
  target_project_id: string;
  target_dataset_rid: string;
  bootstrap_mode: string;
  status: string;
  details: unknown;
  installed_by?: string | null;
  created_at: string;
  completed_at?: string | null;
}

export function installDatasetProduct(
  productId: string,
  payload: {
    target_project_id: string;
    target_dataset_rid: string;
    bootstrap_mode?: 'schema-only' | 'with-snapshot';
  },
) {
  return api.post<DatasetProductInstall>(
    `/marketplace/products/${productId}/install`,
    payload,
  );
}

export async function uploadData(datasetId: string, file: File, options: { logicalPath?: string } = {}) {
  const formData = new FormData();
  formData.append('file', file);
  if (options.logicalPath) {
    formData.append('logical_path', options.logicalPath);
  }
  const headers: Record<string, string> = {};
  const authHeader = api.authorizationHeaders().Authorization;
  if (authHeader) {
    headers.Authorization = authHeader;
  }
  const response = await fetch(`/api/v1/datasets/${datasetId}/upload`, {
    method: 'POST',
    headers,
    body: formData,
  });
  if (!response.ok) throw new Error('Upload failed');
  return response.json();
}

// ────────────────────────────────────────────────────────────────────────────
// P3.1 — Excel (.xlsx) sheet/header mapping import.
// Mirrors `services/dataset-versioning-service/internal/handlers/excel_import.go`
// and `internal/xlsximp/parser.go`: inspect lists worksheets, preview
// validates a config against the real workbook bytes (mapping:null performs
// layout discovery), and import normalizes the sheet to NDJSON on a
// format=json dataset. Multipart only — never set Content-Type manually so
// the browser adds the boundary; errors are `{error: string}` JSON.
// ────────────────────────────────────────────────────────────────────────────

export interface ExcelSheetInfo {
  name: string;
  index: number;
}

export interface ExcelInspectResult {
  filename: string;
  size_bytes: number;
  sha256: string;
  sheets: ExcelSheetInfo[];
  max_data_rows: number;
}

export interface ExcelColumnMap {
  source_index: number;
  target: string;
}

/** `header_row` is the 1-based physical header row, or null when the sheet
 *  has no header (the first row is data). `mapping: null` requests layout
 *  discovery instead of validation. */
export interface ExcelImportConfig {
  sheet: string;
  header_row: number | null;
  mapping: ExcelColumnMap[] | null;
}

export interface ExcelSourceColumn {
  index: number;
  header: string;
}

export interface ExcelPreviewResult {
  config: ExcelImportConfig;
  source_columns: ExcelSourceColumn[];
  columns: string[];
  sample_rows: Array<Record<string, string | null>>;
  row_count: number;
}

export interface ExcelImportResult {
  path: string;
  physical_uri: string;
  size_bytes: number;
  row_count: number;
  columns: string[];
  ndjson_sha256: string;
  files: unknown[];
}

async function postExcelForm<T>(
  path: string,
  fields: {
    file?: File;
    config?: ExcelImportConfig;
    sample_rows?: number;
    logical_path?: string;
  },
  signal?: AbortSignal,
): Promise<T> {
  const formData = new FormData();
  if (fields.file !== undefined) formData.append('file', fields.file);
  if (fields.config !== undefined) formData.append('config', JSON.stringify(fields.config));
  if (fields.sample_rows !== undefined) formData.append('sample_rows', String(fields.sample_rows));
  if (fields.logical_path !== undefined && fields.logical_path !== '') {
    formData.append('logical_path', fields.logical_path);
  }
  const headers: Record<string, string> = {};
  const authHeader = api.authorizationHeaders().Authorization;
  if (authHeader) {
    headers.Authorization = authHeader;
  }
  const response = await fetch(`/api/v1${path}`, {
    method: 'POST',
    headers,
    body: formData,
    signal,
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    const raw = payload && typeof payload === 'object'
      ? (payload as { error?: unknown; message?: unknown }).error ?? (payload as { message?: unknown }).message
      : null;
    const message = typeof raw === 'string' && raw ? raw : `Excel request failed (${response.status})`;
    throw new ApiError(response.status, message);
  }
  return response.json() as Promise<T>;
}

/** POST /datasets/imports/excel/inspect — worksheet list for mapping UIs. */
export function inspectExcelWorkbook(file: File, signal?: AbortSignal) {
  return postExcelForm<ExcelInspectResult>('/datasets/imports/excel/inspect', { file }, signal);
}

/** POST /datasets/imports/excel/preview — layout discovery (mapping:null) or
 *  full config validation with canonical columns + sample rows. */
export function previewExcelWorkbook(
  file: File,
  config: ExcelImportConfig,
  options: { sampleRows?: number; signal?: AbortSignal } = {},
) {
  return postExcelForm<ExcelPreviewResult>(
    '/datasets/imports/excel/preview',
    { file, config, sample_rows: options.sampleRows },
    options.signal,
  );
}

/** POST /datasets/{id}/excel/import — 201 with the normalized NDJSON file
 *  metadata. `logicalPath` must end with .ndjson/.jsonl; the backend derives
 *  `<workbook-stem>.ndjson` when omitted. */
export function importExcelWorkbook(
  datasetId: string,
  file: File,
  config: ExcelImportConfig,
  options: { logicalPath?: string; signal?: AbortSignal } = {},
) {
  return postExcelForm<ExcelImportResult>(
    `/datasets/${encodeURIComponent(datasetId)}/excel/import`,
    { file, config, logical_path: options.logicalPath },
    options.signal,
  );
}

// ────────────────────────────────────────────────────────────────────────────
// P3 — Foundry-style branch + JobSpec endpoints.
// Wired against the routes defined in `services/dataset-versioning-service/src/lib.rs`
// and `services/pipeline-authoring-service/src/main.rs`.
// ────────────────────────────────────────────────────────────────────────────

/// `POST /datasets/{rid}/branches` (P1 v2 shape).
export function createBranchV2(datasetRid: string, params: CreateBranchV2Params) {
  return api.post<DatasetBranch>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches`,
    params,
  );
}

/// `DELETE /datasets/{rid}/branches/{branch}` (P1 reparent-preview).
export function deleteDatasetBranch(datasetRid: string, branchName: string) {
  return api.delete<DatasetBranchDeleteResponse>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}`,
  );
}

/// `GET /datasets/{rid}/branches/{branch}/preview-delete`.
export function previewDeleteBranch(datasetRid: string, branchName: string) {
  return api.get<DatasetBranchPreviewDelete>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}/preview-delete`,
  );
}

/// `POST /datasets/{rid}/branches/{branch}:reparent`.
export function reparentBranch(
  datasetRid: string,
  branchName: string,
  params: ReparentBranchParams,
) {
  return api.post<DatasetBranch>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}:reparent`,
    params,
  );
}

/// `GET /datasets/{rid}/branches/{branch}/ancestry` — child→root walk.
export function listBranchAncestry(datasetRid: string, branchName: string) {
  return api.get<DatasetBranchAncestor[]>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}/ancestry`,
  );
}

/// `POST /datasets/{rid}/branches/{branch}/transactions/{txn}:commit`.
export function commitTransaction(
  datasetRid: string,
  branchName: string,
  transactionId: string,
) {
  return api.post<DatasetTransaction>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(
      branchName,
    )}/transactions/${encodeURIComponent(transactionId)}:commit`,
    {},
  );
}

/// `POST /datasets/{rid}/branches/{branch}/transactions/{txn}:abort`.
export function abortTransaction(
  datasetRid: string,
  branchName: string,
  transactionId: string,
) {
  return api.post<DatasetTransaction>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(
      branchName,
    )}/transactions/${encodeURIComponent(transactionId)}:abort`,
    {},
  );
}

/// `GET /datasets/{rid}/job-specs?on_branch=<branch>` — queries the
/// `pipeline-authoring-service` index. The dataset-versioning surface
/// proxies this under the same `/datasets/...` namespace, so the UI
/// keeps using `api` (which is rooted at `/api/v1`).
export function listDatasetJobSpecs(
  datasetRid: string,
  params?: { on_branch?: string },
) {
  const query = new URLSearchParams();
  if (params?.on_branch) query.set('on_branch', params.on_branch);
  const qs = query.toString();
  return api.get<DatasetJobSpecRow[]>(
    `/datasets/${encodeURIComponent(datasetRid)}/job-specs${qs ? `?${qs}` : ''}`,
  );
}

/// Roll-up "is there a JobSpec on master?" used by the catalog card
/// + lineage colouring. Falls back to `false` when the listing is
/// empty or the call fails (we don't want a transient 404 to flip
/// every dataset to grey).
export async function loadJobSpecStatus(
  datasetRid: string,
): Promise<DatasetJobSpecStatus> {
  try {
    const rows = await listDatasetJobSpecs(datasetRid);
    const branches = Array.from(new Set(rows.map((r) => r.branch_name)));
    return {
      has_master_jobspec: branches.includes('master'),
      branches_with_jobspec: branches.sort(),
    };
  } catch {
    return { has_master_jobspec: false, branches_with_jobspec: [] };
  }
}

// ────────────────────────────────────────────────────────────────────────────
// P4 — branch retention + markings inheritance.
// ────────────────────────────────────────────────────────────────────────────

export interface BranchMarkingsView {
  effective: string[];
  explicit: string[];
  inherited_from_parent: string[];
}

export interface UpdateRetentionParams {
  policy: 'INHERITED' | 'FOREVER' | 'TTL_DAYS';
  ttl_days?: number | null;
}

export function updateBranchRetention(
  datasetRid: string,
  branchName: string,
  params: UpdateRetentionParams,
) {
  return api.patch<{ branch: string; policy: string; ttl_days: number | null }>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}/retention`,
    params,
  );
}

export function restoreBranch(datasetRid: string, branchName: string) {
  return api.post<{ branch: string; restored_at: string; previously_archived_at: string }>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}:restore`,
    {},
  );
}

export function getBranchMarkings(datasetRid: string, branchName: string) {
  return api.get<BranchMarkingsView>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/${encodeURIComponent(branchName)}/markings`,
  );
}

// ────────────────────────────────────────────────────────────────────────────
// P5 — Branch comparison + lifecycle timeline data.
// ────────────────────────────────────────────────────────────────────────────

export interface CompareTransactionSummary {
  transaction_rid: string;
  transaction_id: string;
  branch: string;
  tx_type: string;
  status: string;
  committed_at: string | null;
  files_changed: number;
}

export interface CompareConflictingFile {
  logical_path: string;
  a_transaction_rid: string;
  b_transaction_rid: string;
  content_hash_a: string | null;
  content_hash_b: string | null;
}

export interface BranchCompareResponse {
  base_branch: string;
  compare_branch: string;
  lca_branch_rid: string | null;
  a_only_transactions: CompareTransactionSummary[];
  b_only_transactions: CompareTransactionSummary[];
  conflicting_files: CompareConflictingFile[];
}

export function compareBranches(datasetRid: string, base: string, compare: string) {
  const query = new URLSearchParams({ base, compare });
  return api.get<BranchCompareResponse>(
    `/datasets/${encodeURIComponent(datasetRid)}/branches/compare?${query.toString()}`,
  );
}
