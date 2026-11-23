import { useMutation } from '@tanstack/react-query';
import { useId, useState, type FormEvent } from 'react';

import { compareDatasetSnapshotRows, type DatasetComparedRow, type DatasetSnapshot } from '@/lib/api/datasets';

interface SnapshotRowComparisonProps {
  datasetId: string;
  base: DatasetSnapshot;
  target: DatasetSnapshot;
}

interface ComparisonPage {
  key: string;
  pageIndex: number;
  cursor?: string;
}

const CHANGE_PAGE_SIZE = 50;

const CHANGE_LABEL = {
  added: 'Added',
  removed: 'Removed',
  modified: 'Modified',
} as const;

function ComparedRow({ row }: { row?: DatasetComparedRow }) {
  if (!row) return '—';
  return <pre style={{ margin: 0, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{JSON.stringify(row, null, 2)}</pre>;
}

export function SnapshotRowComparison({ datasetId, base, target }: SnapshotRowComparisonProps) {
  const headingId = useId();
  const keyInputId = useId();
  const [keyColumn, setKeyColumn] = useState('');
  const [pageCursors, setPageCursors] = useState<(string | undefined)[]>([undefined]);
  const comparison = useMutation({
    mutationFn: ({ key, cursor }: ComparisonPage) => compareDatasetSnapshotRows(datasetId, base, target, key, {
      limit: CHANGE_PAGE_SIZE,
      cursor,
    }),
  });
  const key = keyColumn.trim();
  const result = comparison.data;
  const request = comparison.variables;

  function compare(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!key || comparison.isPending) return;
    setPageCursors([undefined]);
    comparison.mutate({ key, pageIndex: 0 });
  }

  function previousPage() {
    if (!request || request.pageIndex === 0 || comparison.isPending) return;
    const pageIndex = request.pageIndex - 1;
    comparison.mutate({ key: request.key, pageIndex, cursor: pageCursors[pageIndex] });
  }

  function nextPage() {
    const cursor = result?.next_cursor;
    if (!request || !cursor || comparison.isPending) return;
    const pageIndex = request.pageIndex + 1;
    setPageCursors((current) => [...current.slice(0, pageIndex), cursor]);
    comparison.mutate({ key: request.key, pageIndex, cursor });
  }

  return (
    <section aria-labelledby={headingId} className="of-panel" style={{ padding: 16, display: 'grid', gap: 12 }}>
      <div>
        <h2 id={headingId} style={{ margin: 0, fontSize: 18 }}>Compare rows by key</h2>
        <p className="of-text-muted" style={{ margin: '4px 0 0', fontSize: 13 }}>
          Key values must be present and unique in each snapshot. Compare up to 8,192 rows per orders.v1 Parquet snapshot.
        </p>
      </div>
      <form onSubmit={compare} style={{ display: 'flex', alignItems: 'end', flexWrap: 'wrap', gap: 8 }}>
        <div>
          <label htmlFor={keyInputId} className="of-text-muted" style={{ display: 'block', fontSize: 12 }}>Key column</label>
          <input
            id={keyInputId}
            className="of-input"
            value={keyColumn}
            placeholder="order_id"
            disabled={comparison.isPending}
            onChange={(event) => {
              setKeyColumn(event.target.value);
              setPageCursors([undefined]);
              comparison.reset();
            }}
          />
        </div>
        <button type="submit" className="of-button of-button--primary" disabled={!key || comparison.isPending}>
          Compare rows
        </button>
      </form>
      {comparison.isPending && <p role="status" className="of-text-muted">Comparing rows…</p>}
      {comparison.error && (
        <div className="of-status-danger" style={{ padding: 8, display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
          <span role="alert">{comparison.error.message}</span>
          {request && <button type="button" className="of-button" onClick={() => comparison.mutate(request)}>Retry</button>}
        </div>
      )}
      {result && (
        <>
          <p role="status" style={{ fontSize: 13 }}>
            {result.counts.added} added · {result.counts.removed} removed · {result.counts.modified} modified · {result.counts.unchanged} unchanged
          </p>
          {result.changes_truncated && (
            <p className="of-text-muted" style={{ fontSize: 13 }}>Showing {result.changes.length} change details. The counts include all rows.</p>
          )}
        </>
      )}
      {request && (pageCursors.length > 1 || result?.next_cursor) && (
        <nav aria-label="Row change pages" style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
          <button type="button" className="of-button" disabled={request.pageIndex === 0 || comparison.isPending} onClick={previousPage}>
            Previous page
          </button>
          <span>Page {request.pageIndex + 1}</span>
          <button type="button" className="of-button" disabled={!result?.next_cursor || comparison.isPending} onClick={nextPage}>
            Next page
          </button>
        </nav>
      )}
      {result && result.changes.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table aria-label="Row changes" className="of-table" style={{ fontSize: 12 }}>
            <thead>
              <tr>
                <th scope="col">Change</th>
                <th scope="col">Key</th>
                <th scope="col">Before</th>
                <th scope="col">After</th>
              </tr>
            </thead>
            <tbody>
              {result.changes.map((change) => (
                <tr key={change.key}>
                  <td>{CHANGE_LABEL[change.kind]}</td>
                  <td style={{ fontFamily: 'var(--font-mono)' }}>{change.key}</td>
                  <td><ComparedRow row={change.before} /></td>
                  <td><ComparedRow row={change.after} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
