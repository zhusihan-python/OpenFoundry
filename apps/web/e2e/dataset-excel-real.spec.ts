import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { expect, test, type Page } from '@playwright/test';

// Real-backend P3.1 workflow against the isolated p31 stack. This suite is
// distinct from dataset-excel-import.spec.ts (mocked HTTP, browser state
// machine only): here inspect/preview/import, dataset creation, and the
// detail-page preview/schema/rows responses are all REAL gateway traffic via
// the test-only dev server (vite.p31-e2e.config.ts), which stubs only the two
// identity endpoints the isolated stack does not provide.
//
// Opt-in only — a default Playwright run must SKIP this external integration
// (never count it as passed). Enable with P31_REAL_E2E=1; overrides and
// prerequisites are documented in e2e/fixtures/README.md.

const TOKEN_FILE = process.env.P31_TOKEN_FILE ?? '/private/tmp/openfoundry-p31-review/token_admin.txt';
const ORDERS_XLSX = resolve(process.env.P31_FIXTURE ?? 'e2e/fixtures/orders-real.xlsx');

async function signIn(page: Page): Promise<string> {
  // Read at test runtime; the token value never lands in the repo or logs.
  const token = readFileSync(TOKEN_FILE, 'utf8').trim();
  await page.addInitScript((value) => {
    window.localStorage.setItem('of_access_token', value);
  }, token);
  return token;
}

test.describe('dataset upload — excel import (real stack, opt-in)', () => {
  test.skip(
    process.env.P31_REAL_E2E !== '1',
    'opt-in only: set P31_REAL_E2E=1 (requires the isolated p31 stack and token file; see e2e/fixtures/README.md)',
  );

test('real stack: 订单 sheet (header row 3) → code/desc/order_no/qty → validated json dataset import → detail preview & schema', async ({ page }) => {
  test.setTimeout(90_000);
  const token = await signIn(page);
  await page.goto('/datasets/upload');

  await page.locator('input[type="file"]').setInputFiles(ORDERS_XLSX);

  // Real inspect: all three worksheets listed, none auto-adopted.
  await expect(page.getByText('3 sheets')).toBeVisible();
  await expect(page.getByLabel('Worksheet')).toHaveValue('');

  // Non-first sheet, header on physical row 3.
  await page.getByLabel('Worksheet').selectOption('订单');
  await page.getByLabel('Header row number').fill('3');
  await page.getByLabel('Header row number').press('Enter');
  await expect(page.getByTestId('excel-mapping-table')).toBeVisible();

  // Suggestions come from the real header row (订单号/编号/描述/数量).
  await expect(page.locator('input[aria-label="target-0"]')).toHaveValue('订单号');
  await expect(page.locator('input[aria-label="target-1"]')).toHaveValue('编号');
  await expect(page.locator('input[aria-label="target-2"]')).toHaveValue('描述');
  await expect(page.locator('input[aria-label="target-3"]')).toHaveValue('数量');

  // Rename targets (source_index identity unchanged).
  await page.locator('input[aria-label="target-0"]').fill('order_no');
  await page.locator('input[aria-label="target-1"]').fill('code');
  await page.locator('input[aria-label="target-2"]').fill('desc');
  await page.locator('input[aria-label="target-3"]').fill('qty');

  const datasetName = `p31-ui-orders-${Date.now()}`;
  await page.getByLabel('Name').fill(datasetName);
  await expect(page.getByLabel('File path')).toHaveValue('orders-real.ndjson');

  // Server validation over the real workbook bytes.
  await page.getByTestId('excel-validate').click();
  await expect(page.getByTestId('excel-validated')).toContainText('2 data rows');
  // Real sample values: leading-zero order no, 30-digit code, null desc cell.
  await expect(page.getByTestId('excel-preview-table')).toContainText('007');
  await expect(page.getByTestId('excel-preview-table')).toContainText('123456789012345678901234567890');

  await page.getByRole('button', { name: 'Upload', exact: true }).click();
  await expect(page).toHaveURL(/\/datasets\/[0-9a-f-]{36}$/);
  const rid = page.url().split('/').pop() ?? '';

  // Real GET preview (unified positional shape) renders the mapped columns
  // with the same values, including the leading-zero and 30-digit strings.
  await expect(page.getByRole('columnheader', { name: 'order_no' })).toBeVisible();
  await expect(page.getByRole('cell', { name: '007', exact: true })).toBeVisible();
  await expect(page.getByRole('cell', { name: '123456789012345678901234567890' })).toBeVisible();

  // Real GET schema (wrapped SchemaResponse) unwraps into the fields table.
  await page.getByRole('tab', { name: 'Schema' }).click();
  for (const column of ['code', 'desc', 'order_no', 'qty']) {
    await expect(page.getByRole('cell', { name: column, exact: true }).first()).toBeVisible();
  }
  await expect(page.getByRole('cell', { name: 'STRING', exact: true }).first()).toBeVisible();

  // Catalog statistics are updated in the same publish transaction (no
  // client-side faking): 2 rows and the real normalized NDJSON size — 181
  // bytes for this workbook with the standard 4-column mapping — shown in
  // the detail header chips…
  await expect(page.getByText('2 rows').first()).toBeVisible();
  await expect(page.getByText('181 B').first()).toBeVisible();
  // …and verified directly against the catalog API…
  const catalogResponse = await page.request.get(`/api/v1/datasets/${rid}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(catalogResponse.ok()).toBeTruthy();
  const catalog = (await catalogResponse.json()) as { row_count: number; size_bytes: number };
  expect(catalog.row_count).toBe(2);
  expect(catalog.size_bytes).toBe(181);
  // …and they persist across a full page reload (server state, not memory).
  await page.reload();
  await expect(page.getByText('2 rows').first()).toBeVisible();
  await expect(page.getByText('181 B').first()).toBeVisible();
});

test('real stack: no-header sheet 原始 keeps the first row and imports with fallback names', async ({ page }) => {
  test.setTimeout(90_000);
  await signIn(page);
  await page.goto('/datasets/upload');

  await page.locator('input[type="file"]').setInputFiles(ORDERS_XLSX);
  await expect(page.getByText('3 sheets')).toBeVisible();

  await page.getByLabel('Worksheet').selectOption('原始');
  await expect(page.getByTestId('excel-mapping-table')).toBeVisible();
  // Default header row 1 consumed the first data row; untick to keep it.
  await page.getByLabel('Includes header row').uncheck();
  await expect(page.getByTestId('excel-mapping-table')).toBeVisible();

  await expect(page.locator('input[aria-label="target-0"]')).toHaveValue('column_0');
  await expect(page.locator('input[aria-label="target-1"]')).toHaveValue('column_1');

  const datasetName = `p31-ui-raw-${Date.now()}`;
  await page.getByLabel('Name').fill(datasetName);

  await page.getByTestId('excel-validate').click();
  // Real layout of 原始 (verified via the gateway): 2 physical rows × 2
  // columns, all retained as data because there is no header.
  await expect(page.getByTestId('excel-validated')).toContainText('2 data rows');
  await expect(page.getByTestId('excel-validated')).toContainText('2 target columns');

  await page.getByRole('button', { name: 'Upload', exact: true }).click();
  await expect(page).toHaveURL(/\/datasets\/[0-9a-f-]{36}$/);

  await expect(page.getByRole('columnheader', { name: 'column_0' })).toBeVisible();
});
});
