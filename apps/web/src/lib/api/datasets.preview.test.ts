import { afterEach, describe, expect, it, vi } from 'vitest';

import { getDatasetTransaction, getVersions, listDatasetTransactions, previewDataset } from './datasets';

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
});
