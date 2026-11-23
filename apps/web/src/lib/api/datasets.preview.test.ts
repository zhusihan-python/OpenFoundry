import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  compareDatasetSnapshotRows,
  getDatasetSnapshotSchema,
  getDatasetTransaction,
  getVersions,
  listDatasetTransactions,
  previewDataset,
} from './datasets';

function stubResponses(...responses: unknown[]) {
  const fetch = vi.fn<typeof globalThis.fetch>().mockImplementation(async () => Response.json(responses.shift()));
  vi.stubGlobal('fetch', fetch);
  return fetch;
}

describe('dataset history and pinned preview', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('normalizes the live transaction page and follows version cursors', async () => {
    const transaction = {
      id: 'tx-old',
      dataset_id: 'ds-1',
      tx_type: 'SNAPSHOT',
      branch_name: 'release',
      status: 'COMMITTED',
      started_at: '2026-01-01T00:00:00Z',
    };
    const fetch = stubResponses(
      { data: [transaction] },
      { data: [{ version: 2, transaction_id: 'tx-new' }], next_cursor: 'versions/page+2' },
      { data: [{ version: 1, transaction_id: 'tx-old' }] },
      [{ status: 200, data: transaction }],
    );
    expect(await listDatasetTransactions('ds-1')).toMatchObject([
      { id: 'tx-old', operation: 'SNAPSHOT', branch_name: 'release', created_at: transaction.started_at },
    ]);
    expect((await getVersions('ds-1')).map((version) => version.transaction_id)).toEqual(['tx-new', 'tx-old']);
    expect(fetch.mock.calls[2]?.[0]).toBe('/api/v1/datasets/ds-1/versions?cursor=versions%2Fpage%2B2');
    expect(await getDatasetTransaction('ds-1', 'tx-old')).toMatchObject({ id: 'tx-old', operation: 'SNAPSHOT' });
    expect(fetch).toHaveBeenLastCalledWith('/api/v1/datasets/ds-1/transactions:batchGet',
      expect.objectContaining({ method: 'POST', body: JSON.stringify({ ids: ['tx-old'] }) }));
  });

  it('preserves opaque cursors and exact integers and supplies the branch for version-only callers', async () => {
    const fetch = stubResponses(
      { active_branch: 'release' },
      { dataset_id: 'ds-1', transaction_id: 'tx-old', branch: 'release', version: 2, format: 'parquet',
        rows: [{ order_id: '9223372036854775807' }], columns: [{ name: 'order_id', field_type: 'LONG' }],
        next_cursor: 'opaque-next' },
    );
    const result = await previewDataset('ds-1', { version: 2, cursor: 'opaque/+ cursor' });

    expect(fetch.mock.calls.map((call) => call[0])).toEqual([
      '/api/v1/datasets/ds-1',
      '/api/v1/datasets/ds-1/preview?version=2&branch=release&cursor=opaque%2F%2B+cursor',
    ]);
    expect(result.rows?.[0]?.order_id).toBe('9223372036854775807');
    expect(result.next_cursor).toBe('opaque-next');
    expect(result.transaction_id).toBe('tx-old');
  });

  const selected = { id: 'tx-old', branch: 'releases/published', version: 2 };
  const storedSchema = {
    dataset_id: 'ds-1',
    transaction_id: selected.id,
    branch: selected.branch,
    version: selected.version,
    fields: [{ name: 'amount', field_type: 'DECIMAL(18,4)', nullable: false }],
  };

  it('reads the schema using the selected snapshot branch and version', async () => {
    const fetch = stubResponses(storedSchema);
    await expect(getDatasetSnapshotSchema('ri.foundry.main.dataset.ds-1', selected)).resolves.toEqual(storedSchema);
    expect(fetch.mock.calls[0]?.[0]).toBe(
      '/api/v1/datasets/ri.foundry.main.dataset.ds-1/schema?branch=releases%2Fpublished&version=2',
    );
  });

  it.each([
    { dataset_id: 'ds-foreign' },
    { branch: 'main' },
    { version: 3 },
    { transaction_id: 'tx-new-head' },
  ])('rejects a schema that belongs to a different selection (%j)', async (override) => {
    stubResponses({ ...storedSchema, ...override });
    await expect(getDatasetSnapshotSchema('ds-1', selected)).rejects.toThrow('Schema did not match the selected version.');
  });
});

describe('dataset snapshot row comparison', () => {
  afterEach(() => vi.unstubAllGlobals());

  const base = { id: 'tx-base', branch: 'releases/old', version: 1 };
  const target = { id: 'tx-target', branch: 'releases/current', version: 2 };
  const comparison = {
    dataset_id: 'ds-1',
    key: 'order_id',
    base: { branch: base.branch, version: base.version, transaction_id: base.id },
    target: { branch: target.branch, version: target.version, transaction_id: target.id },
    counts: { added: 1, removed: 0, modified: 0, unchanged: 0 },
    changes: [{
      kind: 'added', key: '9223372036854775807',
      after: { order_id: '9223372036854775807', amount: '900719925474.0993' },
    }],
    changes_truncated: false,
  };

  it('sends both selected transactions and preserves exact key and row values', async () => {
    const fetch = stubResponses(comparison);
    const result = await compareDatasetSnapshotRows('ri.foundry.main.dataset.ds-1', base, target, 'order_id');
    expect(result).toEqual(comparison);
    expect(fetch.mock.calls[0]?.[0]).toBe(
      '/api/v1/datasets/ri.foundry.main.dataset.ds-1/compare/rows'
      + '?base_branch=releases%2Fold&base_version=1&base_transaction=tx-base'
      + '&target_branch=releases%2Fcurrent&target_version=2&target_transaction=tx-target&key=order_id',
    );
  });

  it('keeps both snapshots and encodes the opaque cursor for the next page', async () => {
    const lastPage = { ...comparison, counts: { ...comparison.counts, added: 2 } };
    const firstPage = {
      ...lastPage,
      changes: [{
        kind: 'added', key: '9223372036854775806',
        after: { order_id: '9223372036854775806', amount: '900719925474.0992' },
      }],
      next_cursor: 'opaque/+ cursor',
    };
    const fetch = stubResponses(firstPage, lastPage);
    const first = await compareDatasetSnapshotRows('ds-1', base, target, 'order_id', { limit: 1 });
    const next = await compareDatasetSnapshotRows('ds-1', base, target, 'order_id', {
      limit: 1,
      cursor: first.next_cursor,
    });

    const endpoint = '/api/v1/datasets/ds-1/compare/rows'
      + '?base_branch=releases%2Fold&base_version=1&base_transaction=tx-base'
      + '&target_branch=releases%2Fcurrent&target_version=2&target_transaction=tx-target'
      + '&key=order_id&limit=1';
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      endpoint,
      endpoint + '&cursor=opaque%2F%2B+cursor',
    ]);
    expect(first).toEqual(firstPage);
    expect(next).toEqual(lastPage);
    expect(next.next_cursor).toBeUndefined();
  });

  it.each([
    { dataset_id: 'ds-foreign' },
    { key: 'note' },
    { base: { ...comparison.base, branch: 'main' } },
    { base: { ...comparison.base, version: 3 } },
    { base: { ...comparison.base, transaction_id: 'tx-other' } },
    { target: { ...comparison.target, branch: 'main' } },
    { target: { ...comparison.target, version: 3 } },
    { target: { ...comparison.target, transaction_id: 'tx-new-head' } },
  ])('rejects a response bound to different inputs (%j)', async (override) => {
    stubResponses({ ...comparison, ...override });
    await expect(compareDatasetSnapshotRows('ds-1', base, target, 'order_id'))
      .rejects.toThrow('Row comparison did not match the selected versions and key.');
  });
});
