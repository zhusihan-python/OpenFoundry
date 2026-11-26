import type { DatasetFilesResponse, DatasetSnapshot } from '@/lib/api/datasets';

/** One file of a files listing, as a side of the Compare tab shows it. */
export interface CompareVersionFile {
  id: string;
  path: string;
  size_bytes: number;
  /** null while this file's own download runs, else why it failed; absent when neither. */
  download?: string | null;
}

/** The files of the committed version a side selects, listed by its transaction. */
export interface CompareVersionFiles {
  status: 'loading' | 'error' | 'ready';
  files: CompareVersionFile[];
  error?: string;
}

/** By file id: null while that download runs, else why it failed. A file that
 *  has no entry here is not downloading. */
export type VersionFileDownloads = Record<string, string | null>;

function sameFile(downloads: VersionFileDownloads): (file: CompareVersionFile) => CompareVersionFile {
  return (file) => (file.id in downloads ? { ...file, download: downloads[file.id] } : file);
}

export function downloadStarted(downloads: VersionFileDownloads, fileId: string): VersionFileDownloads {
  return { ...downloads, [fileId]: null };
}

export function downloadFinished(downloads: VersionFileDownloads, fileId: string): VersionFileDownloads {
  return Object.fromEntries(Object.entries(downloads).filter(([id]) => id !== fileId));
}

export function downloadFailed(downloads: VersionFileDownloads, fileId: string, error: string): VersionFileDownloads {
  return { ...downloads, [fileId]: error };
}

/** What a side shows of its version's files. A branch names no transaction to
 *  list them by, so that side shows nothing to download. A download is known by
 *  its file id alone: it follows the file into whichever side or version lists
 *  it, and never shows against another file. */
export function versionFiles(
  snapshot: DatasetSnapshot | undefined,
  listing: DatasetFilesResponse | undefined,
  error: string | undefined,
  downloads: VersionFileDownloads,
): CompareVersionFiles | null {
  if (!snapshot) return null;
  if (error) return { status: 'error', files: [], error };
  if (!listing) return { status: 'loading', files: [] };
  return {
    status: 'ready',
    files: listing.files
      .map((file) => ({ id: file.id, path: file.logical_path, size_bytes: file.size_bytes }))
      .map(sameFile(downloads)),
  };
}
