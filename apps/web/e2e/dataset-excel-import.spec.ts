import { expect, test, type Page, type Route } from '@playwright/test';

// P3.1 Excel (.xlsx) import UI state tests. Every backend route is mocked at
// the HTTP boundary (same paths/shapes as the real handlers in
// services/dataset-versioning-service/internal/handlers/excel_import.go) —
// these verify browser state-machine behavior only: sheet/header selection,
// mapping suggestions + duplicate gating, invalidation on edits, late-response
// protection, submit payload, and the detail-page preview/schema adapters.
// Real end-to-end HTTP acceptance against a running backend is coordinated
// separately by Root.

const now = '2026-11-16T00:00:00Z';
const TINY = 'e2e/fixtures/tiny-workbook.xlsx';
const SECOND = 'e2e/fixtures/second-book.xlsx';
const LEGACY = 'e2e/fixtures/legacy-book.xls';

const DATASET = {
  id: 'ds-excel-1',
  rid: 'ri.foundry.main.dataset.ds-excel-1',
  name: 'tiny workbook',
  description: '',
  format: 'json',
  storage_path: 'datasets/ds-excel-1',
  size_bytes: 210,
  row_count: 42,
  owner_id: '00000000-0000-0000-0000-000000000001',
  tags: [],
  current_version: 1,
  active_branch: 'master',
  created_at: now,
  updated_at: now,
};

const INSPECT = {
  filename: 'tiny-workbook.xlsx',
  size_bytes: 17,
  sha256: 'a'.repeat(64),
  // First sheet is a read-me sheet on purpose: the UI must not auto-adopt it.
  sheets: [
    { name: '说明', index: 0 },
    { name: '销售明细', index: 1 },
  ],
  max_data_rows: 1000,
};

interface WireConfig {
  sheet: string;
  header_row: number | null;
  mapping: Array<{ source_index: number; target: string }> | null;
}

function discoveryResult(config: WireConfig) {
  const columns =
    config.sheet === '销售明细'
      ? config.header_row === null
        ? [{ index: 0, header: '' }, { index: 1, header: '' }]
        : [
            { index: 0, header: '日期' },
            { index: 1, header: '金额' },
            { index: 2, header: '' },
          ]
      : [{ index: 0, header: '说明列' }];
  return {
    config: { sheet: config.sheet, header_row: config.header_row, mapping: null },
    source_columns: columns,
    columns: [],
    sample_rows: [],
    row_count: 0,
  };
}

function validateResult(config: WireConfig) {
  const mapping = config.mapping ?? [];
  const targets = mapping.map((entry) => ({ ...entry, target: entry.target.trim() }));
  const sample: Record<string, string | null> = {};
  const values = ['2026-01-05', '12.50', null];
  targets.forEach((entry, position) => {
    sample[entry.target] = values[position] ?? null;
  });
  return {
    config: { sheet: config.sheet, header_row: config.header_row, mapping: targets },
    source_columns: [],
    columns: targets.map((entry) => entry.target).sort(),
    sample_rows: [sample],
    row_count: 42,
  };
}

const discoveryCalls: WireConfig[] = [];
const validateCalls: WireConfig[] = [];
const createdDatasets: Array<Record<string, unknown>> = [];
const importCalls: Array<{ config: WireConfig; logicalPath: string | null }> = [];
let inspectCalls = 0;
let validateDelayMs = 0;
let validateStatus = 200;

// Playwright 1.59's intercepted Request has no formData(); parse the raw
// multipart body ourselves (fields are small text parts — the workbook bytes
// are never inspected here).
async function readFormField(route: Route, name: string): Promise<string | null> {
  const request = route.request();
  const body = (await request.postDataBuffer())?.toString('latin1') ?? '';
  const contentType = request.headers()['content-type'] ?? '';
  const boundaryMatch = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType);
  if (!boundaryMatch) return null;
  const boundary = boundaryMatch[1] ?? boundaryMatch[2];
  for (const part of body.split(`--${boundary}`)) {
    const headerEnd = part.indexOf('\r\n\r\n');
    if (headerEnd === -1) continue;
    const headers = part.slice(0, headerEnd);
    if (!headers.includes(`name="${name}"`)) continue;
    // Body was split as latin1 to keep byte offsets intact; restore UTF-8.
    const raw = part.slice(headerEnd + 4).replace(/\r\n$/, '');
    return Buffer.from(raw, 'latin1').toString('utf8');
  }
  return null;
}

async function mockExcelRoutes(page: Page) {
  discoveryCalls.length = 0;
  validateCalls.length = 0;
  createdDatasets.length = 0;
  importCalls.length = 0;
  inspectCalls = 0;
  validateDelayMs = 0;
  validateStatus = 200;

  await page.addInitScript(() => {
    window.localStorage.setItem('of_access_token', 'e2e-token');
  });
  await page.route('**/api/v1/auth/bootstrap-status', (route) =>
    route.fulfill({ json: { requires_initial_admin: false } }));
  await page.route('**/api/v1/users/me', (route) =>
    route.fulfill({
      json: {
        id: '00000000-0000-0000-0000-000000000001',
        email: 'runner@example.com',
        name: 'Trail Runner',
        is_active: true,
        roles: ['admin'],
        groups: [],
        permissions: ['*'],
        organization_id: null,
        attributes: {},
        mfa_enabled: false,
        mfa_enforced: false,
        auth_source: 'local',
        created_at: now,
      },
    }));
  await page.route('**/api/v1/datasets/imports/excel/inspect', (route) => {
    inspectCalls += 1;
    return route.fulfill({ json: INSPECT });
  });
  await page.route('**/api/v1/datasets/imports/excel/preview', async (route: Route) => {
    const config = JSON.parse(String(await readFormField(route, 'config'))) as WireConfig;
    if (config.mapping === null) {
      discoveryCalls.push(config);
      return route.fulfill({ json: discoveryResult(config) });
    }
    validateCalls.push(config);
    if (validateDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, validateDelayMs));
    }
    if (validateStatus !== 200) {
      return route.fulfill({
        status: validateStatus,
        json: { error: 'excel_header_invalid: header_row 200 out of range 1..100' },
      });
    }
    return route.fulfill({ json: validateResult(config) });
  });
  await page.route('**/api/v1/datasets', async (route) => {
    if (route.request().method() !== 'POST') {
      return route.fulfill({ json: { data: [], page: 1, per_page: 100, total: 0, total_pages: 1 } });
    }
    createdDatasets.push(route.request().postDataJSON() as Record<string, unknown>);
    return route.fulfill({ json: DATASET });
  });
  await page.route('**/api/v1/datasets/ds-excel-1/excel/import', async (route) => {
    importCalls.push({
      config: JSON.parse(String(await readFormField(route, 'config'))) as WireConfig,
      logicalPath: await readFormField(route, 'logical_path'),
    });
    return route.fulfill({
      status: 201,
      json: {
        path: 'tiny-workbook.ndjson',
        physical_uri: 'local://datasets/ds-excel-1/excel/uuid/data.ndjson',
        size_bytes: 210,
        row_count: 42,
        columns: ['金额_元', '日期', '备注'],
        ndjson_sha256: 'b'.repeat(64),
        files: [],
      },
    });
  });
  // Detail page (after successful submit).
  await page.route('**/api/v1/datasets/ds-excel-1', (route) => route.fulfill({ json: DATASET }));
  await page.route('**/api/v1/datasets/ds-excel-1/preview**', (route) =>
    route.fulfill({
      // Unified rows-reader shape: string columns + positional JSON rows.
      json: {
        dataset_id: 'ds-excel-1',
        format: 'json',
        limit: 100,
        offset: 0,
        columns: ['金额_元', '日期', '备注'],
        rows: [['12.50', '2026-01-05', null]],
      },
    }));
  await page.route('**/api/v1/datasets/ds-excel-1/schema', (route) =>
    route.fulfill({
      // Wrapped SchemaResponse shape used for normalized NDJSON datasets.
      json: {
        view_id: '00000000-0000-0000-0000-000000000000',
        dataset_id: 'ds-excel-1',
        branch: 'master',
        schema: {
          fields: [
            { name: '日期', type: 'STRING', nullable: true },
            { name: '金额_元', type: 'STRING', nullable: true },
          ],
          file_format: 'TEXT',
        },
        content_hash: 'excel-import:abc',
        created_at: now,
      },
    }));
  await page.route('**/api/v1/datasets/ds-excel-1/transactions', (route) =>
    route.fulfill({ json: [] }));
}

async function stageWorkbook(page: Page, path = TINY) {
  await page.locator('input[type="file"]').setInputFiles(path);
  await expect(page.getByTestId('excel-import-panel')).toBeVisible();
  // inspect done once the sheet count chip appears
  await expect(page.getByText('2 sheets')).toBeVisible();
}

async function chooseSalesSheet(page: Page) {
  await page.getByLabel('Worksheet').selectOption('销售明细');
  await expect(page.getByTestId('excel-mapping-table')).toBeVisible();
}

test.describe('dataset upload — excel import (mocked HTTP)', () => {
  test('maps a non-first Chinese sheet with header row 3, gates duplicates, and submits the validated config', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');

    await stageWorkbook(page);

    // Not auto-adopting the first sheet: no mapping until the user chooses.
    await expect(page.getByLabel('Worksheet')).toHaveValue('');
    await expect(page.getByTestId('excel-mapping-table')).toHaveCount(0);
    // Excel defaults: format locked to json, normalized .ndjson path.
    await expect(page.getByLabel('File path')).toHaveValue('tiny-workbook.ndjson');
    await expect(page.locator('select[disabled]', { hasText: 'JSON' })).toBeVisible();
    await expect(page.getByLabel('Name')).toHaveValue('tiny workbook');
    // The CSV/Parquet sample-inference aside and the write-mode radios stay
    // out of the Excel flow (server validation is the source of truth).
    await expect(page.getByRole('heading', { name: 'Inference panel' })).toHaveCount(0);
    await expect(page.getByText('Transactionless')).toHaveCount(0);

    await chooseSalesSheet(page);
    await expect(discoveryCalls.at(-1)).toMatchObject({ sheet: '销售明细', header_row: 1, mapping: null });
    // Suggestions: real headers pass through, blank header falls back to column_N.
    await expect(page.locator('input[aria-label="target-0"]')).toHaveValue('日期');
    await expect(page.locator('input[aria-label="target-1"]')).toHaveValue('金额');
    await expect(page.locator('input[aria-label="target-2"]')).toHaveValue('column_2');

    // Header row 3 (commit via Enter) re-discovers with the new layout.
    await page.getByLabel('Header row number').fill('3');
    await page.getByLabel('Header row number').press('Enter');
    await expect(discoveryCalls.at(-1)).toMatchObject({ sheet: '销售明细', header_row: 3, mapping: null });
    await expect(page.getByTestId('excel-mapping-table')).toBeVisible();

    // Rename a target; the previous validation state is gone and submit stays off.
    await page.locator('input[aria-label="target-1"]').fill('金额_元');
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();

    // Duplicate target blocks validation.
    await page.locator('input[aria-label="target-2"]').fill('日期');
    await expect(page.getByText('Duplicate target names: 日期')).toBeVisible();
    await expect(page.getByTestId('excel-validate')).toBeDisabled();

    await page.locator('input[aria-label="target-2"]').fill('备注');
    await expect(page.getByTestId('excel-validate')).toBeEnabled();

    // Server validation gates submit; the preview shows canonical output.
    await page.getByTestId('excel-validate').click();
    await expect(page.getByTestId('excel-validated')).toContainText('42 data rows');
    await expect(page.getByTestId('excel-preview-table')).toContainText('2026-01-05');
    await expect(page.getByTestId('excel-preview-table')).toContainText('12.50');
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeEnabled();

    await page.getByRole('button', { name: 'Upload', exact: true }).click();
    await expect(page).toHaveURL(/\/datasets\/ds-excel-1$/);

    // createDataset ran with format=json; import received the exact validated config + path.
    expect(createdDatasets).toHaveLength(1);
    expect(createdDatasets[0]).toMatchObject({ name: 'tiny workbook', format: 'json' });
    expect(importCalls).toHaveLength(1);
    expect(importCalls[0].logicalPath).toBe('tiny-workbook.ndjson');
    expect(importCalls[0].config).toEqual({
      sheet: '销售明细',
      header_row: 3,
      mapping: [
        { source_index: 0, target: '日期' },
        { source_index: 1, target: '金额_元' },
        { source_index: 2, target: '备注' },
      ],
    });
    expect(page.getByTestId('excel-import-panel')).toHaveCount(0);

    // Detail page: unified preview shape renders as an object-row table…
    await expect(page.getByRole('columnheader', { name: '金额_元' })).toBeVisible();
    await expect(page.getByRole('cell', { name: '12.50' })).toBeVisible();
    // …and the wrapped schema unwraps into the fields table.
    await page.getByRole('tab', { name: 'Schema' }).click();
    await expect(page.getByRole('cell', { name: '日期' }).first()).toBeVisible();
    await expect(page.getByRole('cell', { name: 'STRING' }).first()).toBeVisible();
  });

  test('a late validation response cannot re-enable submit or overwrite state after an edit', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');
    await stageWorkbook(page);
    await chooseSalesSheet(page);

    validateDelayMs = 1200;
    await page.getByTestId('excel-validate').click();
    // Edit a target while the (slow) validation is in flight.
    await page.locator('input[aria-label="target-0"]').fill('日期_v2');
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();

    // Wait past the delayed response: it must be dropped, not applied.
    await page.waitForTimeout(1800);
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByTestId('excel-preview-table')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();

    // Server-side validation errors surface verbatim.
    validateStatus = 422;
    await page.getByTestId('excel-validate').click();
    await expect(page.getByTestId('excel-error')).toContainText('excel_header_invalid');
    await expect(page.getByTestId('excel-error')).toContainText('200 out of range');
  });

  test('no-header keeps the first row, and switching sheet or file clears mapping/preview/validation', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');
    await stageWorkbook(page);
    await chooseSalesSheet(page);

    // No header: suggestions fall back to column_N (first row is kept as data).
    await page.getByLabel('Includes header row').uncheck();
    await expect(discoveryCalls.at(-1)).toMatchObject({ sheet: '销售明细', header_row: null, mapping: null });
    await expect(page.getByTestId('excel-mapping-table')).toBeVisible();
    await expect(page.locator('input[aria-label="target-0"]')).toHaveValue('column_0');
    await expect(page.locator('input[aria-label="target-1"]')).toHaveValue('column_1');

    // Re-enabling the header restores header-derived suggestions.
    await page.getByLabel('Includes header row').check();
    await expect(page.locator('input[aria-label="target-0"]')).toHaveValue('日期');

    // Validate, then switch sheets: discovery, mapping and validation reset.
    await page.getByTestId('excel-validate').click();
    await expect(page.getByTestId('excel-validated')).toBeVisible();
    await page.getByLabel('Worksheet').selectOption('说明');
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByTestId('excel-preview-table')).toHaveCount(0);
    await expect(page.locator('input[aria-label="target-0"]')).toHaveValue('说明列');
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();

    // Replacing the file remounts the panel from scratch (fresh inspect state).
    await page.locator('input[type="file"]').setInputFiles(SECOND);
    await expect(page.getByLabel('Worksheet')).toHaveValue('');
    await expect(page.getByTestId('excel-mapping-table')).toHaveCount(0);
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByLabel('File path')).toHaveValue('second-book.ndjson');
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();
  });

  test('rejects legacy .xls files instead of falling back to the parquet flow', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');

    await page.locator('input[type="file"]').setInputFiles(LEGACY);
    await expect(page.getByText(/Only plain \.xlsx workbooks are supported/)).toBeVisible();
    await expect(page.getByText('No file selected')).toBeVisible();
    await expect(page.getByTestId('excel-import-panel')).toHaveCount(0);
  });

  test('replacing a metadata-identical file remounts state and drops its in-flight validation', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');
    await stageWorkbook(page);
    await chooseSalesSheet(page);
    await page.getByTestId('excel-validate').click();
    await expect(page.getByTestId('excel-validated')).toBeVisible();
    const inspectCount = inspectCalls;

    // Slow validation for the OLD file, then swap in different bytes under
    // the exact same filename/size/mtime profile the old key relied on.
    validateDelayMs = 1200;
    await page.getByTestId('excel-validate').click();
    await page.locator('input[type="file"]').setInputFiles({
      name: 'tiny-workbook.xlsx',
      mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      buffer: Buffer.from('PK\x03\x04OTHER-FIXTURE', 'latin1'),
    });

    // Fresh panel: re-inspected, nothing pre-selected, nothing validated.
    await expect(page.getByLabel('Worksheet')).toHaveValue('');
    await expect(inspectCalls).toBeGreaterThan(inspectCount);
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);

    // The delayed validation response for the old bytes must never land.
    await page.waitForTimeout(1800);
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByTestId('excel-preview-table')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();
    expect(page.url()).toContain('/datasets/upload');
    expect(importCalls).toHaveLength(0);
  });

  test('editing the header draft invalidates validation before blur, and Enter commits without submitting the form', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');
    await stageWorkbook(page);
    await chooseSalesSheet(page);

    await page.getByTestId('excel-validate').click();
    await expect(page.getByTestId('excel-validated')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeEnabled();

    // Typing a new header row (no blur yet) must immediately revoke the old
    // validation and gate the upload.
    await page.getByLabel('Header row number').fill('5');
    await expect(page.getByTestId('excel-validated')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();

    // Enter commits the header (re-discovery) but must not submit the form.
    await page.getByLabel('Header row number').press('Enter');
    await expect(discoveryCalls.at(-1)).toMatchObject({ sheet: '销售明细', header_row: 5, mapping: null });
    await expect(page.getByTestId('excel-mapping-table')).toBeVisible();
    await expect(page).toHaveURL(/\/datasets\/upload$/);
    expect(createdDatasets).toHaveLength(0);
    await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeDisabled();
  });

  test('switching from CSV to XLSX leaves no stale CSV inference in the schema panel', async ({ page }) => {
    await mockExcelRoutes(page);
    await page.goto('/datasets/upload');

    await page.locator('input[type="file"]').setInputFiles('e2e/fixtures/cities.csv');
    await expect(page.locator('aside').getByText('city')).toBeVisible();
    // Non-Excel flow keeps the inference aside and write-mode options.
    await expect(page.getByRole('heading', { name: 'Inference panel' })).toBeVisible();
    await expect(page.getByText('Transactionless')).toBeVisible();

    await stageWorkbook(page);
    await expect(page.getByTestId('excel-import-panel')).toBeVisible();
    await expect(page.locator('aside').getByText('city')).toHaveCount(0);
    await expect(page.getByRole('heading', { name: 'Inference panel' })).toHaveCount(0);
    await expect(page.getByText('Transactionless')).toHaveCount(0);
    await expect(page.getByText('2 sheets')).toBeVisible();
  });
});
