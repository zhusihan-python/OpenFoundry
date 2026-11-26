import { afterEach, describe, expect, it, vi } from 'vitest';

import api from './client';
import { downloadDatasetFile, listDatasetFiles } from './datasets';

const DATASET = '11111111-1111-1111-1111-111111111111';
const TRANSACTION = '22222222-2222-2222-2222-222222222222';
const FILE = '33333333-3333-3333-3333-333333333333';

describe('files of a committed version', () => {
  afterEach(() => {
    api.setToken(null);
    vi.unstubAllGlobals();
  });

  it('asks the listing for the files of that transaction alone', async () => {
    const fetchMock = stubFetch(new Response(JSON.stringify({ branch: 'main', total: 1, files: [] })));

    const listing = await listDatasetFiles(DATASET, { branch: 'main', transaction_id: TRANSACTION });

    expect(fetchMock.mock.calls[0][0]).toBe(`/api/v1/datasets/${DATASET}/files?branch=main&transaction_id=${TRANSACTION}`);
    expect(listing.total).toBe(1);
  });

  it('reads a listed file with the session credential and returns its bytes', async () => {
    api.setToken('reader-session');
    const fetchMock = stubFetch(new Response('{"total":"59.57"}\n', { status: 200 }));

    const blob = await downloadDatasetFile(DATASET, FILE);

    expect(fetchMock.mock.calls[0][0]).toBe(`/api/v1/datasets/${DATASET}/files/${FILE}/download`);
    expect(fetchMock.mock.calls[0][1]).toEqual({ headers: { Authorization: 'Bearer reader-session' } });
    expect(await blob.text()).toBe('{"total":"59.57"}\n');
  });

  it('sends no credential when the session holds none', async () => {
    const fetchMock = stubFetch(new Response('bytes'));

    await downloadDatasetFile(DATASET, FILE);

    expect(fetchMock.mock.calls[0][1]).toEqual({ headers: {} });
  });

  it('names the service answer when the reader may not read the file', async () => {
    api.setToken('reader-session');
    stubFetch(new Response(JSON.stringify({ error: 'dataset_not_found' }), { status: 404 }));

    await expect(downloadDatasetFile(DATASET, FILE)).rejects.toMatchObject({ name: 'ApiError', status: 404, message: 'dataset_not_found' });
  });

  it('names the message inside the gateway error envelope', async () => {
    stubFetch(new Response(JSON.stringify({ error: { code: 'upstream_unavailable', message: 'upstream unavailable' } }), { status: 502 }));

    await expect(downloadDatasetFile(DATASET, FILE)).rejects.toMatchObject({ status: 502, message: 'upstream unavailable' });
  });

  it('names a message the service answers at its top level', async () => {
    stubFetch(new Response(JSON.stringify({ message: 'dataset_not_found' }), { status: 404 }));

    await expect(downloadDatasetFile(DATASET, FILE)).rejects.toMatchObject({ status: 404, message: 'dataset_not_found' });
  });

  it('falls back when the envelope holds no message and when the answer is not JSON', async () => {
    stubFetch(new Response(JSON.stringify({ error: { code: 'upstream_unavailable' } }), { status: 502 }));
    await expect(downloadDatasetFile(DATASET, FILE)).rejects.toMatchObject({ message: 'File download failed (502)' });
    stubFetch(new Response('<html>bad gateway</html>', { status: 502 }));
    await expect(downloadDatasetFile(DATASET, FILE)).rejects.toMatchObject({ message: 'File download failed (502)' });
  });

  it('reads an error of a JSON answer the same way, as every other call does', async () => {
    stubFetch(new Response(JSON.stringify({ error: { code: 'dataset_not_found', message: 'dataset not found' } }), { status: 404 }));

    await expect(api.get('/datasets')).rejects.toMatchObject({ name: 'ApiError', status: 404, message: 'dataset not found' });
    stubFetch(new Response('not json', { status: 503, statusText: 'Service Unavailable' }));
    await expect(api.get('/datasets')).rejects.toMatchObject({ status: 503, message: 'Service Unavailable' });
  });

  it('names the status when a refusal carries no message', async () => {
    stubFetch(new Response('', { status: 502 }));

    await expect(downloadDatasetFile(DATASET, FILE)).rejects.toMatchObject({ status: 502, message: 'File download failed (502)' });
  });
});

function stubFetch(response: Response) {
  const fetchMock = vi.fn(async (_url: string, _init?: RequestInit) => response);
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}
