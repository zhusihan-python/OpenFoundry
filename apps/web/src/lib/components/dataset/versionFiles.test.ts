import { describe, expect, it } from 'vitest';

import type { DatasetFilesResponse, DatasetSnapshot } from '@/lib/api/datasets';

import {
  downloadFailed,
  downloadFinished,
  downloadStarted,
  versionFiles,
  type VersionFileDownloads,
} from './versionFiles';

const V2: DatasetSnapshot = { id: 'tx-v2', branch: 'main', version: 2 };
const V3: DatasetSnapshot = { id: 'tx-v3', branch: 'main', version: 3 };

function listing(...ids: string[]): DatasetFilesResponse {
  return {
    branch: 'main',
    total: ids.length,
    files: ids.map((id, index) => ({
      id, dataset_id: 'dataset', transaction_id: 'tx-v3', physical_uri: 'local/backing/' + id, logical_path: `part-0000${index}.ndjson`,
      size_bytes: 100 + index, status: 'active', created_at: '', modified_at: '',
    })),
  };
}

// [id, pending, error?]: each file with its own download state, or none.
function shown(files: ReturnType<typeof versionFiles>) {
  return files?.files.map((file): (string | boolean)[] => {
    if (file.download === null) return [file.id, true];
    return typeof file.download === 'string' ? [file.id, false, file.download] : [file.id, false];
  });
}

describe('version files of a Compare side', () => {
  it('has nothing to download while a branch is selected', () => {
    expect(versionFiles(undefined, listing('a'), undefined, {})).toBeNull();
  });

  it('lists the files of the selected version by itself, and keeps another version out', () => {
    const v2 = versionFiles(V2, listing('a', 'b'), undefined, {});
    expect(shown(v2)).toEqual([['a', false], ['b', false]]);
    // v3 was selected before its listing answered: nothing of v2 is shown for it.
    expect(versionFiles(V3, undefined, undefined, { a: null })).toEqual({ status: 'loading', files: [] });
  });

  it('marks every file it is downloading, not only the first', () => {
    let downloads: VersionFileDownloads = {};
    downloads = downloadStarted(downloads, 'a');
    downloads = downloadStarted(downloads, 'b');
    expect(shown(versionFiles(V3, listing('a', 'b'), undefined, downloads))).toEqual([['a', true], ['b', true]]);
    // The same submission again changes nothing: both stay pending by their own id.
    expect(downloadStarted(downloads, 'a')).toEqual(downloads);
  });

  it('keeps a failure on the file that failed', () => {
    let downloads = downloadStarted(downloadStarted({}, 'a'), 'b');
    downloads = downloadFailed(downloads, 'a', 'HTTP 404');
    expect(shown(versionFiles(V3, listing('a', 'b'), undefined, downloads))).toEqual([['a', false, 'HTTP 404'], ['b', true]]);
    // The other file finishing leaves the failure where it is.
    expect(shown(versionFiles(V3, listing('a', 'b'), undefined, downloadFinished(downloads, 'b'))))
      .toEqual([['a', false, 'HTTP 404'], ['b', false]]);
    expect(shown(versionFiles(V3, listing('a', 'b'), undefined, downloadFinished(downloads, 'a'))))
      .toEqual([['a', false], ['b', true]]);
  });

  it('follows a download through fast version switches, and never onto another file', () => {
    let downloads = downloadStarted({}, 'v3-file');
    // v3 is downloading; the reader switches to v2 and starts that file too.
    downloads = downloadStarted(downloads, 'v2-file');
    const v3 = listing('v3-file'), v2 = listing('v2-file');
    expect(shown(versionFiles(V3, v3, undefined, downloads))).toEqual([['v3-file', true]]);
    expect(shown(versionFiles(V2, v2, undefined, downloads))).toEqual([['v2-file', true]]);
    // v3's answer lands while v2 is selected: it touches v3's file only.
    downloads = downloadFailed(downloads, 'v3-file', 'upstream unavailable');
    expect(shown(versionFiles(V2, v2, undefined, downloads))).toEqual([['v2-file', true]]);
    expect(shown(versionFiles(V3, v3, undefined, downloads))).toEqual([['v3-file', false, 'upstream unavailable']]);
    // Back on v3, its failure is retried; v2's file is still pending.
    downloads = downloadStarted(downloads, 'v3-file');
    expect(shown(versionFiles(V3, v3, undefined, downloads))).toEqual([['v3-file', true]]);
    expect(shown(versionFiles(V2, v2, undefined, downloads))).toEqual([['v2-file', true]]);
  });

  it('shows one file the same on both sides that list it', () => {
    const downloads = downloadStarted({}, 'same');
    const both = listing('same');
    expect(shown(versionFiles(V2, both, undefined, downloads))).toEqual(shown(versionFiles(V3, both, undefined, downloads)));
  });

  it('says why the files could not be listed, before any download state', () => {
    expect(versionFiles(V3, listing('a'), 'HTTP 403', { a: null })).toEqual({ status: 'error', files: [], error: 'HTTP 403' });
  });
});
