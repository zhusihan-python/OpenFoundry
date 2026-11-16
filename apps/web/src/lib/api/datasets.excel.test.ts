import { afterEach, describe, expect, it, vi } from 'vitest';

import api, { ApiError } from './client';
import {
  getDatasetSchema,
  importExcelWorkbook,
  inspectExcelWorkbook,
  previewDataset,
  previewExcelWorkbook,
  uploadData,
} from './datasets';

// Unit tests for the P3.1 surface: the Excel inspect/preview/import helpers
// (request shape, multipart fields, error contract, AbortSignal plumbing) and
// the previewDataset/getDatasetSchema compatibility adapters for the unified
// rows-reader / wrapped-schema responses. All HTTP is mocked — real backend
// integration is verified separately.

function workbookFile(name = 'sales.xlsx') {
  return new File([new Uint8Array([0x50, 0x4b, 0x03, 0x04, 0x00])], name, {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
}

function stubFetch(body: unknown, status = 200) {
  const calls: Array<{ input: unknown; init?: RequestInit }> = [];
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: unknown, init?: RequestInit) => {
      calls.push({ input, init });
      return new Response(JSON.stringify(body), {
        status,
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
  return calls;
}

describe('excel import api helpers', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    api.setToken(null);
  });

  it('inspect posts multipart to the excel inspect route without a manual content-type', async () => {
    api.setToken('e2e-token');
    const calls = stubFetch({
      filename: 'sales.xlsx',
      size_bytes: 5,
      sha256: 'abc',
      sheets: [{ name: '说明', index: 0 }, { name: '销售明细', index: 1 }],
      max_data_rows: 1000,
    });
    const file = workbookFile();

    const result = await inspectExcelWorkbook(file);

    expect(result.sheets.map((sheet) => sheet.name)).toEqual(['说明', '销售明细']);
    expect(result.max_data_rows).toBe(1000);
    expect(calls).toHaveLength(1);
    expect(calls[0].input).toBe('/api/v1/datasets/imports/excel/inspect');
    expect(calls[0].init?.method).toBe('POST');
    const headers = calls[0].init?.headers as Record<string, string>;
    expect(headers.Authorization).toBe('Bearer e2e-token');
    // The browser must derive the multipart boundary itself.
    expect(Object.keys(headers)).not.toContain('Content-Type');
    expect(calls[0].init?.body).toBeInstanceOf(FormData);
    const form = calls[0].init?.body as FormData;
    expect(form.get('file')).toBe(file);
    expect(form.get('config')).toBeNull();
  });

  it('preview sends the config as a JSON form field plus sample_rows and forwards the abort signal', async () => {
    const calls = stubFetch({
      config: { sheet: '销售明细', header_row: 3, mapping: [] },
      source_columns: [{ index: 0, header: '日期' }],
      columns: [],
      sample_rows: [],
      row_count: 0,
    });
    const file = workbookFile();
    const controller = new AbortController();
    const config = { sheet: '销售明细', header_row: 3, mapping: null };

    await previewExcelWorkbook(file, config, { sampleRows: 0, signal: controller.signal });

    expect(calls[0].input).toBe('/api/v1/datasets/imports/excel/preview');
    const form = calls[0].init?.body as FormData;
    expect(form.get('file')).toBe(file);
    expect(JSON.parse(form.get('config') as string)).toEqual(config);
    expect(form.get('sample_rows')).toBe('0');
    expect(calls[0].init?.signal).toBe(controller.signal);
  });

  it('import targets the dataset-scoped route and appends the logical path', async () => {
    const calls = stubFetch({
      path: 'sales.ndjson',
      physical_uri: 'local://obj',
      size_bytes: 120,
      row_count: 42,
      columns: ['amount', 'date'],
      ndjson_sha256: 'def',
      files: [],
    }, 201);
    const file = workbookFile();
    const config = {
      sheet: '销售明细',
      header_row: null,
      mapping: [{ source_index: 0, target: 'date' }],
    };

    const result = await importExcelWorkbook('ds 1', file, config, { logicalPath: 'sales.ndjson' });

    expect(result.row_count).toBe(42);
    expect(calls[0].input).toBe('/api/v1/datasets/ds%201/excel/import');
    const form = calls[0].init?.body as FormData;
    expect(JSON.parse(form.get('config') as string)).toEqual(config);
    expect(form.get('logical_path')).toBe('sales.ndjson');
  });

  it('import omits the logical_path field when none is supplied', async () => {
    const calls = stubFetch({ path: 'sales.ndjson' }, 201);

    await importExcelWorkbook('ds-1', workbookFile(), {
      sheet: 's',
      header_row: 1,
      mapping: [{ source_index: 0, target: 'a' }],
    });

    expect((calls[0].init?.body as FormData).get('logical_path')).toBeNull();
  });

  it('surfaces server {error} payloads as ApiError with status and message', async () => {
    stubFetch({ error: 'excel_row_limit: sheet "销售明细" has more than 1000 data rows' }, 422);

    const attempt = previewExcelWorkbook(workbookFile(), {
      sheet: '销售明细',
      header_row: 1,
      mapping: [{ source_index: 0, target: 'a' }],
    });

    await expect(attempt).rejects.toSatisfy((cause: unknown) => {
      expect(cause).toBeInstanceOf(ApiError);
      const error = cause as ApiError;
      expect(error.status).toBe(422);
      expect(error.message).toContain('excel_row_limit');
      expect(error.message).toContain('1000');
      return true;
    });
  });

  it('falls back to a status-derived message for non-JSON error bodies', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('boom', { status: 500 })),
    );

    await expect(inspectExcelWorkbook(workbookFile())).rejects.toSatisfy((cause: unknown) => {
      expect(cause).toBeInstanceOf(ApiError);
      expect((cause as ApiError).status).toBe(500);
      expect((cause as ApiError).message).toContain('500');
      return true;
    });
  });

  it('keeps the legacy uploadData request shape untouched', async () => {
    const calls = stubFetch({ ok: true });
    const file = new File(['a,b\n1,2\n'], 'data.csv', { type: 'text/csv' });

    await uploadData('ds-1', file, { logicalPath: 'raw/data.csv' });

    expect(calls[0].input).toBe('/api/v1/datasets/ds-1/upload');
    const form = calls[0].init?.body as FormData;
    expect(form.get('file')).toBe(file);
    expect(form.get('logical_path')).toBe('raw/data.csv');
    const headers = calls[0].init?.headers as Record<string, string>;
    expect(Object.keys(headers)).not.toContain('Content-Type');
  });
});

describe('dataset preview/schema compatibility adapters', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('previewDataset converts positional rows-reader responses into object rows', async () => {
    stubFetch({
      dataset_id: 'ds-1',
      format: 'json',
      limit: 100,
      offset: 0,
      columns: ['日期', 'amount'],
      rows: [['2026-01-05', '12.5'], [null, '3']],
    });

    const response = await previewDataset('ds-1', { limit: 100 });

    expect(response.columns).toEqual([{ name: '日期' }, { name: 'amount' }]);
    expect(response.rows).toEqual([
      { 日期: '2026-01-05', amount: '12.5' },
      { 日期: null, amount: '3' },
    ]);
    expect(response.format).toBe('json');
    expect(response.limit).toBe(100);
  });

  it('previewDataset passes legacy object-shaped responses through unchanged', async () => {
    const legacy = {
      dataset_id: 'ds-1',
      columns: [{ name: 'city', field_type: 'STRING', nullable: true }],
      rows: [{ city: 'Berlin' }],
      total_rows: 1,
      warnings: [],
    };
    stubFetch(legacy);

    const response = await previewDataset('ds-1');

    expect(response).toEqual(legacy);
  });

  it('previewDataset keeps a __proto__ column as own data without prototype pollution', async () => {
    stubFetch({
      columns: ['__proto__', 'x'],
      rows: [['a', '12.50'], [null, null]],
    });

    const response = await previewDataset('ds-1');

    const [first, second] = response.rows ?? [];
    // Own data property, not the Object.prototype accessor.
    expect(Object.prototype.hasOwnProperty.call(first, '__proto__')).toBe(true);
    expect(first['__proto__']).toBe('a');
    expect(first['x']).toBe('12.50');
    expect(second['x']).toBe(null);
    expect(second['__proto__']).toBe(null);
    // No prototype leakage onto unrelated objects.
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    expect(Object.getPrototypeOf(first)).toBe(Object.prototype);
  });

  it('previewDataset keeps object rows when only the columns need naming', async () => {
    stubFetch({ columns: ['a', 'b'], rows: [] });

    const response = await previewDataset('ds-1');

    expect(response.columns).toEqual([{ name: 'a' }, { name: 'b' }]);
    expect(response.rows).toEqual([]);
  });

  it('getDatasetSchema unwraps the wrapped SchemaResponse into the flat legacy shape', async () => {
    stubFetch({
      view_id: '00000000-0000-0000-0000-000000000000',
      dataset_id: 'ds-1',
      branch: 'master',
      schema: {
        fields: [{ name: '日期', type: 'STRING', nullable: true }],
        file_format: 'TEXT',
      },
      content_hash: 'excel-import:abc',
      created_at: '2026-11-16T00:00:00Z',
    });

    const schema = await getDatasetSchema('ds-1');

    expect(schema.dataset_id).toBe('ds-1');
    expect(schema.fields).toEqual([{ name: '日期', type: 'STRING', nullable: true }]);
    expect(schema.created_at).toBe('2026-11-16T00:00:00Z');
  });

  it('getDatasetSchema passes flat legacy responses through unchanged', async () => {
    const flat = {
      id: 'schema-1',
      dataset_id: 'ds-1',
      fields: [{ name: 'city', type: 'STRING' }],
      created_at: '2026-05-01T00:00:00Z',
    };
    stubFetch(flat);

    const schema = await getDatasetSchema('ds-1');

    expect(schema).toEqual(flat);
  });
});
