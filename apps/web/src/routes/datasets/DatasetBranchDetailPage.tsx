import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';

import {
  CompareTab,
  type CompareFile,
  type CompareSelector,
  type CompareSide,
  type SchemaField,
} from '@/lib/components/dataset/CompareTab';
import { DeleteBranchDialog } from '@/lib/components/dataset/DeleteBranchDialog';
import { SnapshotRowComparison } from '@/lib/components/dataset/SnapshotRowComparison';
import { Tabs } from '@/lib/components/Tabs';
import {
  downloadFailed,
  downloadFinished,
  downloadStarted,
  versionFiles,
  type CompareVersionFile,
  type VersionFileDownloads,
} from '@/lib/components/dataset/versionFiles';
import {
  committedDatasetSnapshots,
  compareBranches,
  datasetUuid,
  downloadDatasetFile,
  getBranchMarkings,
  getDataset,
  getDatasetSchemaForBranch,
  getDatasetSnapshotSchema,
  getDatasetTransactions,
  getVersions,
  listBranches,
  listDatasetFiles,
  listDatasetTransactions,
  missingVersionTransactionIds,
  restoreBranch,
  schemaFormatUnsupported,
  updateBranchRetention,
  type BranchCompareResponse,
  type BranchMarkingsView,
  type Dataset,
  type DatasetBranch,
  type DatasetFilesResponse,
  type DatasetSchema,
  type DatasetSchemaResponse,
  type DatasetSnapshot,
  type DatasetTransaction,
} from '@/lib/api/datasets';

type Tab = 'overview' | 'compare' | 'history' | 'retention' | 'security';
type Policy = 'INHERITED' | 'FOREVER' | 'TTL_DAYS';

type BranchCompareSelection = CompareSelector & {
  pin?: { datasetId: string; branchId: string; snapshot: DatasetSnapshot };
};

const EMPTY_TRANSACTIONS: DatasetTransaction[] = [];

const EMPTY_MARKINGS: BranchMarkingsView = {
  effective: [],
  explicit: [],
  inherited_from_parent: [],
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function shortId(value?: string | null): string {
  if (!value) return '-';
  if (value.length <= 12) return value;
  return `${value.slice(0, 8)}...`;
}

function formatDate(value?: string | null): string {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function fieldType(value: unknown): string | undefined {
  if (typeof value === 'string') return value;
  if (isRecord(value) && typeof value.type === 'string') return value.type;
  return undefined;
}

function normalizeSchemaFields(raw: unknown): SchemaField[] {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((entry) => {
    if (!isRecord(entry)) return [];
    const name = typeof entry.name === 'string' ? entry.name : '';
    if (!name) return [];
    return [{
      name,
      type: fieldType(entry.type) ?? fieldType(entry.field_type) ?? fieldType(entry.data_type),
      nullable: typeof entry.nullable === 'boolean' ? entry.nullable : undefined,
    }];
  });
}

function schemaFields(response: DatasetSchema | DatasetSchemaResponse): SchemaField[] {
  if ('schema' in response) return normalizeSchemaFields(response.schema.fields);
  return normalizeSchemaFields(response.fields);
}

function compareFiles(response: DatasetFilesResponse): CompareFile[] {
  return response.files.map((file) => ({
    path: file.logical_path,
    size_bytes: file.size_bytes,
  }));
}

function parentName(branch: DatasetBranch | null, branches: DatasetBranch[]): string {
  if (!branch?.parent_branch_id) return '- (root)';
  const parent = branches.find((candidate) => candidate.id === branch.parent_branch_id);
  return parent?.name ?? shortId(branch.parent_branch_id);
}

function defaultSelectors(branches: DatasetBranch[], currentName: string, current: DatasetBranch | null) {
  const parent = current?.parent_branch_id ? branches.find((candidate) => candidate.id === current.parent_branch_id) : null;
  const defaultBranch = branches.find((candidate) => candidate.is_default);
  const fallback = branches.find((candidate) => candidate.name !== currentName);
  return {
    A: { kind: 'branch', value: parent?.name ?? defaultBranch?.name ?? fallback?.name ?? currentName } as CompareSelector,
    B: { kind: 'branch', value: currentName } as CompareSelector,
  };
}

function snapshotForSelector(selector: CompareSelector, snapshots: DatasetSnapshot[]) {
  return selector.kind === 'transaction'
    ? snapshots.find((candidate) => candidate.id === selector.value)
    : undefined;
}

function useCompareSide(datasetId: string, selector: CompareSelector, snapshot: DatasetSnapshot | undefined, enabled: boolean) {
  return useQuery<CompareSide>({
    queryKey: ['dataset-compare-side', datasetId, selector.kind, selector.value, snapshot?.branch, snapshot?.version],
    enabled: enabled && Boolean(selector.value),
    staleTime: 0,
    retry: false,
    queryFn: async () => {
      if (selector.kind === 'transaction') {
        if (!snapshot) throw new Error('The selected transaction has no committed snapshot version.');
        const schema = await getDatasetSnapshotSchema(datasetId, snapshot);
        return {
          schema: schema.fields.map((field) => ({ name: field.name, type: field.field_type, nullable: field.nullable })),
        };
      }
      const [schema, files] = await Promise.all([
        getDatasetSchemaForBranch(datasetId, selector.value),
        listDatasetFiles(datasetId, { branch: selector.value }),
      ]);
      return { schema: schemaFields(schema), files: compareFiles(files) };
    },
  });
}

// The files of the committed version a side selects, listed by the transaction
// that cut it and nothing else: the head's listing would not reach an old version.
function useVersionFiles(datasetId: string, snapshot: DatasetSnapshot | undefined, enabled: boolean) {
  return useQuery({
    queryKey: ['dataset-version-files', datasetId, snapshot?.branch, snapshot?.version, snapshot?.id],
    enabled: enabled && Boolean(snapshot),
    staleTime: 0,
    retry: false,
    queryFn: () => {
      if (!snapshot) throw new Error('No committed version is selected.');
      return listDatasetFiles(datasetId, { branch: snapshot.branch, transaction_id: snapshot.id });
    },
  });
}

export function DatasetBranchDetailPage() {
  const { id: routeId = '', branch: branchName = '' } = useParams<{ id: string; branch: string }>();
  const id = datasetUuid(routeId);
  const navigate = useNavigate();
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [branches, setBranches] = useState<DatasetBranch[]>([]);
  const [branch, setBranch] = useState<DatasetBranch | null>(null);
  const [markings, setMarkings] = useState<BranchMarkingsView>(EMPTY_MARKINGS);
  const [tab, setTab] = useState<Tab>('overview');
  const [policy, setPolicy] = useState<Policy>('INHERITED');
  const [ttlDays, setTtlDays] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [deleteOpen, setDeleteOpen] = useState(false);

  const [selectorA, setSelectorA] = useState<BranchCompareSelection>({ kind: 'branch', value: '' });
  const [selectorB, setSelectorB] = useState<BranchCompareSelection>({ kind: 'branch', value: '' });
  const transactionsQuery = useQuery({
    queryKey: ['dataset-transactions', id],
    queryFn: () => listDatasetTransactions(id),
    enabled: Boolean(id),
  });
  const transactions = transactionsQuery.data ?? EMPTY_TRANSACTIONS;
  const canCompare = dataset?.id === id && branch?.dataset_id === id && branch.name === branchName
    && !loading && tab === 'compare';
  const versionsQuery = useQuery({
    queryKey: ['dataset-versions', id],
    queryFn: () => getVersions(id),
    enabled: canCompare,
  });
  const versions = useMemo(
    () => (versionsQuery.data ?? []).filter((version) => version.dataset_id === id && Number.isInteger(version.version) && version.version > 0),
    [id, versionsQuery.data],
  );
  const recent = useMemo(() => transactions.filter((tx) => tx.dataset_id === id), [id, transactions]);
  const missingIds = useMemo(() => missingVersionTransactionIds(versions, recent), [versions, recent]);
  const older = useQuery({
    queryKey: ['dataset-version-transactions', id, missingIds],
    queryFn: () => getDatasetTransactions(id, missingIds),
    enabled: canCompare && versionsQuery.isSuccess && transactionsQuery.isSuccess && missingIds.length > 0,
  });
  const historyError = transactionsQuery.failureReason ?? versionsQuery.failureReason ?? older.failureReason
    ?? transactionsQuery.error ?? versionsQuery.error ?? older.error;
  const snapshots = useMemo(() => {
    if (historyError) return [];
    const activeBranches = new Map(
      branches.filter((branch) => branch.dataset_id === id && !branch.archived_at).map((branch) => [branch.name, branch.id]),
    );
    const candidates = [...recent, ...(older.data ?? [])].filter((tx) => {
      const branchId = tx.branch_name && activeBranches.get(tx.branch_name);
      return branchId && (!tx.branch_id || tx.branch_id === branchId);
    });
    const resolved = committedDatasetSnapshots(candidates, versions);
    // The recent-transaction window can move while a selected pair is being paged.
    // Keep only the selected pins that the new batch still needs to resolve.
    if (older.isPending) {
      for (const { pin } of [selectorA, selectorB]) {
        if (!pin || pin.datasetId !== id) continue;
        const { snapshot } = pin;
        if (activeBranches.get(snapshot.branch) === pin.branchId && missingIds.includes(snapshot.id)
          && versions.some((v) => v.version === snapshot.version && v.transaction_id === snapshot.id)
          && !resolved.some((s) => s.id === snapshot.id)) {
          resolved.push(snapshot);
        }
      }
    }
    return resolved;
  }, [branches, historyError, id, missingIds, older.data, older.isPending, recent, selectorA, selectorB, versions]);
  const snapshotA = snapshotForSelector(selectorA, snapshots);
  const snapshotB = snapshotForSelector(selectorB, snapshots);
  const sideA = useCompareSide(id, selectorA, snapshotA, canCompare);
  const sideB = useCompareSide(id, selectorB, snapshotB, canCompare);
  const filesOfA = useVersionFiles(id, snapshotA, canCompare);
  const filesOfB = useVersionFiles(id, snapshotB, canCompare);
  const [fileDownloads, setFileDownloads] = useState<VersionFileDownloads>({});
  const sidesLoading = sideA.isFetching || sideB.isFetching;
  // The versioned schema exists for Parquet snapshots only. Another format has
  // no schema to diff, while its versions stay selected and their rows compare.
  const schemaNote = (side: typeof sideA): { error?: string; notice?: string } => !side.isError ? {}
    : schemaFormatUnsupported(side.error)
      ? { notice: 'This file format has no schema preview for the selected version.' }
      : { error: side.error?.message };
  const sideANote = schemaNote(sideA);
  const sideBNote = schemaNote(sideB);
  const twoBranches = selectorA.kind === 'branch' && selectorB.kind === 'branch';
  const showBranchComparison = canCompare && twoBranches && sideA.isSuccess && sideB.isSuccess
    && !sidesLoading && selectorA.value !== selectorB.value;
  const branchCompare = useQuery({
    queryKey: ['dataset-branch-comparison', id, selectorA.value, selectorB.value],
    queryFn: () => compareBranches(id, selectorA.value, selectorB.value),
    enabled: showBranchComparison,
    staleTime: 0,
    retry: false,
  });

  const branchTransactions = useMemo(() => {
    return transactions.filter((tx) => {
      if (tx.branch_name) return tx.branch_name === branchName;
      return branch?.is_default ?? false;
    });
  }, [branch, branchName, transactions]);

  async function load() {
    if (!id || !branchName) return;
    setLoading(true);
    setError('');
    try {
      const [nextDataset, nextBranches] = await Promise.all([
        getDataset(id),
        listBranches(id),
      ]);
      const found = nextBranches.find((candidate) => candidate.name === branchName) ?? null;
      const selectors = defaultSelectors(nextBranches, branchName, found);
      setDataset(nextDataset);
      setBranches(nextBranches);
      setBranch(found);
      setPolicy(found?.retention_policy ?? 'INHERITED');
      setTtlDays(found?.retention_ttl_days ?? null);
      setSelectorA(selectors.A);
      setSelectorB(selectors.B);
      if (!found) {
        setError(`Branch "${branchName}" was not found.`);
        setMarkings(EMPTY_MARKINGS);
      } else {
        try {
          setMarkings(await getBranchMarkings(id, branchName));
        } catch {
          setMarkings(EMPTY_MARKINGS);
        }
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Failed to load branch');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
  }, [id, branchName]);

  function changeSelector(which: 'A' | 'B', selector: CompareSelector) {
    const snapshot = snapshotForSelector(selector, snapshots);
    const selectedBranch = snapshot && branches.find((branch) => branch.dataset_id === id && branch.name === snapshot.branch);
    const selection = snapshot && selectedBranch
      ? { ...selector, pin: { datasetId: id, branchId: selectedBranch.id, snapshot } }
      : selector;
    if (which === 'A') setSelectorA(selection);
    else setSelectorB(selection);
  }

  // The browser saves the bytes this client read with the signed-in user's credential.
  // Each step reads the downloads as they stand then, so one file finishing or
  // failing never clears what another file's download has put there.
  async function downloadVersionFile(file: CompareVersionFile) {
    if (file.download === null) return;
    setFileDownloads((previous) => downloadStarted(previous, file.id));
    try {
      const blob = await downloadDatasetFile(id, file.id);
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = file.path.split('/').pop() || 'download';
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      setFileDownloads((previous) => downloadFinished(previous, file.id));
    } catch (cause) {
      const error = cause instanceof Error ? cause.message : 'Download failed';
      setFileDownloads((previous) => downloadFailed(previous, file.id, error));
    }
  }

  const sideFiles = (snapshot: DatasetSnapshot | undefined, query: { data?: DatasetFilesResponse; error: Error | null }) =>
    versionFiles(snapshot, query.data, query.error ? query.error.message || 'Files could not be listed.' : undefined, fileDownloads);

  async function saveRetention() {
    setBusy(true);
    setError('');
    try {
      await updateBranchRetention(id, branchName, {
        policy,
        ttl_days: policy === 'TTL_DAYS' ? ttlDays : null,
      });
      await load();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Save failed');
    } finally {
      setBusy(false);
    }
  }

  async function restore() {
    setBusy(true);
    setError('');
    try {
      await restoreBranch(id, branchName);
      await load();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Restore failed');
    } finally {
      setBusy(false);
    }
  }

  if (loading) {
    return (
      <section className="of-page" style={{ padding: 24 }}>
        <p className="of-text-muted">Loading...</p>
      </section>
    );
  }

  return (
    <section className="of-page" style={{ display: 'grid', gap: 12 }}>
      <header className="of-panel" style={{ padding: 12, display: 'grid', gap: 10 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12, flexWrap: 'wrap' }}>
          <div style={{ minWidth: 0 }}>
            <Link to={`/datasets/${id}/branches`} style={{ color: 'var(--text-muted)', fontSize: 12 }}>← Branches</Link>
            <h1 className="of-heading-lg" style={{ marginTop: 4 }}>{branchName}</h1>
            <p className="of-text-muted" style={{ marginTop: 2, fontSize: 11 }}>
              {dataset?.name ?? id} · parent {parentName(branch, branches)} · head {shortId(branch?.head_transaction_id)}
            </p>
          </div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
            {branch?.archived_at && (
              <button type="button" onClick={() => void restore()} disabled={busy} className="of-button">
                Restore branch
              </button>
            )}
            <button type="button" onClick={() => setDeleteOpen(true)} disabled={!branch || busy} className="of-button" style={{ color: '#b42318', borderColor: '#e5b8b8' }}>
              Delete branch
            </button>
          </div>
        </div>
        {branch && (
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {branch.is_default && <span className="of-chip of-chip-active">Default</span>}
            <span className="of-chip">Policy {branch.retention_policy ?? 'INHERITED'}</span>
            <span className="of-chip">{branch.has_open_transaction ? 'Open transaction' : 'No open transaction'}</span>
            {branch.archived_at && <span className="of-chip of-status-warning">Archived</span>}
          </div>
        )}
      </header>

      {error && (
        <div className="of-status-danger" style={{ padding: '10px 14px', borderRadius: 'var(--radius-md)', fontSize: 13 }}>
          {error}
        </div>
      )}

      {branch ? (
        <>
          <Tabs
            tabs={[
              { id: 'overview', label: 'Overview' },
              { id: 'compare', label: 'Compare' },
              { id: 'history', label: 'History' },
              { id: 'retention', label: 'Retention' },
              { id: 'security', label: 'Security' },
            ] as const}
            active={tab}
            onChange={setTab}
          />

          {tab === 'overview' && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 360px), 1fr))', gap: 12 }}>
              <section className="of-panel" style={{ padding: 14 }}>
                <p className="of-heading-sm">Branch details</p>
                <dl style={{ marginTop: 10, display: 'grid', gridTemplateColumns: '140px minmax(0, 1fr)', gap: '8px 12px', fontSize: 12 }}>
                  <dt className="of-text-muted">Description</dt><dd>{branch.description || '-'}</dd>
                  <dt className="of-text-muted">Parent</dt><dd style={{ fontFamily: 'var(--font-mono)' }}>{parentName(branch, branches)}</dd>
                  <dt className="of-text-muted">Created from</dt><dd style={{ fontFamily: 'var(--font-mono)' }}>{shortId(branch.created_from_transaction_id)}</dd>
                  <dt className="of-text-muted">Head tx</dt><dd style={{ fontFamily: 'var(--font-mono)' }}>{shortId(branch.head_transaction_id)}</dd>
                  <dt className="of-text-muted">Created</dt><dd>{formatDate(branch.created_at)}</dd>
                  <dt className="of-text-muted">Updated</dt><dd>{formatDate(branch.updated_at)}</dd>
                  <dt className="of-text-muted">Last activity</dt><dd>{formatDate(branch.last_activity_at)}</dd>
                  <dt className="of-text-muted">Archived</dt><dd>{formatDate(branch.archived_at)}</dd>
                </dl>
              </section>

              <section className="of-panel" style={{ padding: 14, display: 'grid', gap: 12 }}>
                <div>
                  <p className="of-heading-sm">Fallback chain</p>
                  <p className="of-text-muted" style={{ marginTop: 6, fontFamily: 'var(--font-mono)', fontSize: 12 }}>
                    {(branch.fallback_chain ?? []).join(' -> ') || '-'}
                  </p>
                </div>
                <div>
                  <p className="of-heading-sm">Labels</p>
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 6 }}>
                    {Object.entries(branch.labels ?? {}).map(([key, value]) => (
                      <span key={key} className="of-chip">{key}={value}</span>
                    ))}
                    {Object.keys(branch.labels ?? {}).length === 0 && <span className="of-text-muted" style={{ fontSize: 12 }}>No labels</span>}
                  </div>
                </div>
                <div>
                  <p className="of-heading-sm">Branch set</p>
                  <p className="of-text-muted" style={{ marginTop: 6, fontSize: 12 }}>
                    {branches.length} branch{branches.length === 1 ? '' : 'es'} in this dataset. {branchTransactions.length} transaction{branchTransactions.length === 1 ? '' : 's'} are associated with this branch.
                  </p>
                </div>
              </section>
            </div>
          )}

          {canCompare && (
            <section style={{ display: 'grid', gap: 12 }}>
              <CompareTab
                snapshots={snapshots}
                branches={branches}
                sideA={sideA.data ?? null}
                sideB={sideB.data ?? null}
                sideAError={sideANote.error}
                sideBError={sideBNote.error}
                sideANotice={sideANote.notice}
                sideBNotice={sideBNote.notice}
                selectorA={selectorA}
                selectorB={selectorB}
                loading={sidesLoading}
                error={historyError
                  ? `Committed versions unavailable. ${historyError.message}`
                  : branchCompare.error?.message}
                filesA={sideFiles(snapshotA, filesOfA)}
                filesB={sideFiles(snapshotB, filesOfB)}
                onChangeSelector={changeSelector}
                onDownloadFile={(file) => void downloadVersionFile(file)}
              />
              {/* Rows compare by key without a schema to diff, which only Parquet snapshots serve. */}
              {snapshotA && snapshotB && !sideANote.error && !sideBNote.error && (
                <SnapshotRowComparison
                  key={JSON.stringify([id, snapshotA, snapshotB])}
                  datasetId={id}
                  base={snapshotA}
                  target={snapshotB}
                />
              )}
              {showBranchComparison && !branchCompare.isFetching && branchCompare.isSuccess && (
                <BranchCompareSummary comparison={branchCompare.data} />
              )}
            </section>
          )}

          {tab === 'history' && (
            <section className="of-panel" style={{ overflow: 'auto' }}>
              <table className="of-table" style={{ fontSize: 12 }}>
                <thead>
                  <tr><th>ID</th><th>Operation</th><th>Status</th><th>Summary</th><th>Created</th><th>Committed</th></tr>
                </thead>
                <tbody>
                  {branchTransactions.map((tx) => (
                    <tr key={tx.id}>
                      <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{shortId(tx.id)}</td>
                      <td>{tx.operation}</td>
                      <td>{tx.status}</td>
                      <td>{tx.summary || '-'}</td>
                      <td>{formatDate(tx.created_at)}</td>
                      <td>{formatDate(tx.committed_at)}</td>
                    </tr>
                  ))}
                  {branchTransactions.length === 0 && (
                    <tr><td colSpan={6} className="of-text-muted">
                      {transactionsQuery.isLoading ? 'Loading transactions…' : 'No transactions on this branch.'}
                    </td></tr>
                  )}
                </tbody>
              </table>
            </section>
          )}

          {tab === 'retention' && (
            <section className="of-panel" style={{ padding: 16, display: 'grid', gap: 8 }}>
              {branch.archived_at && (
                <div style={{ padding: 10, background: '#fef3c7', color: '#92400e', borderRadius: 8, fontSize: 12 }}>
                  Branch archived at <code>{branch.archived_at}</code>.
                  <button type="button" onClick={() => void restore()} disabled={busy} className="of-button" style={{ marginLeft: 8, fontSize: 11 }}>
                    Restore branch
                  </button>
                </div>
              )}
              <fieldset style={{ display: 'grid', gap: 6, padding: 0, border: 0 }}>
                <legend style={{ fontSize: 11, textTransform: 'uppercase', color: 'var(--text-muted)' }}>Policy</legend>
                {(['INHERITED', 'FOREVER', 'TTL_DAYS'] as Policy[]).map((p) => (
                  <label key={p} style={{ display: 'flex', gap: 8, padding: 6, border: '1px solid var(--border-default)', borderRadius: 8 }}>
                    <input type="radio" checked={policy === p} onChange={() => setPolicy(p)} />
                    <span>
                      <strong>{p}</strong>
                      <span style={{ display: 'block', fontSize: 11, color: 'var(--text-muted)' }}>
                        {p === 'INHERITED' && 'Walk up parent_branch chain.'}
                        {p === 'FOREVER' && 'Never archived.'}
                        {p === 'TTL_DAYS' && 'Archive after N days of inactivity.'}
                      </span>
                    </span>
                  </label>
                ))}
              </fieldset>
              {policy === 'TTL_DAYS' && (
                <label style={{ fontSize: 13 }}>
                  TTL (days)
                  <input
                    type="number"
                    min={1}
                    value={ttlDays ?? ''}
                    onChange={(event) => setTtlDays(event.target.value ? Number(event.target.value) : null)}
                    className="of-input"
                    style={{ marginTop: 4, width: 120 }}
                  />
                </label>
              )}
              <div>
                <button type="button" onClick={() => void saveRetention()} disabled={busy} className="of-button of-button--primary">
                  Save retention
                </button>
              </div>
            </section>
          )}

          {tab === 'security' && (
            <section className="of-panel" style={{ padding: 16 }}>
              {markings.effective.length === 0 ? (
                <p className="of-text-muted">No markings on this branch.</p>
              ) : (
                <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
                  {(['effective', 'explicit', 'inherited_from_parent'] as const).map((group) => (
                    <div key={group} style={{ padding: 12, border: '1px solid var(--border-default)', borderRadius: 8 }}>
                      <p style={{ fontSize: 11, textTransform: 'uppercase', color: 'var(--text-muted)' }}>
                        {group === 'effective' && 'Effective'}
                        {group === 'explicit' && 'Explicit on this branch'}
                        {group === 'inherited_from_parent' && 'Inherited from parent'}
                      </p>
                      <ul style={{ display: 'flex', flexWrap: 'wrap', gap: 4, paddingLeft: 0, listStyle: 'none', marginTop: 6 }}>
                        {markings[group].length === 0 ? (
                          <li className="of-text-muted">-</li>
                        ) : (
                          markings[group].map((marking) => (
                            <li key={marking} style={{ fontSize: 11, padding: '2px 8px', background: 'var(--bg-subtle)', borderRadius: 999 }}>
                              <code>{marking}</code>
                            </li>
                          ))
                        )}
                      </ul>
                    </div>
                  ))}
                </div>
              )}
            </section>
          )}
        </>
      ) : (
        <section className="of-panel" style={{ padding: 24 }}>
          <p className="of-text-muted">The branch could not be loaded from the dataset branch listing.</p>
        </section>
      )}

      <DeleteBranchDialog
        datasetRid={id}
        branch={branch}
        open={deleteOpen}
        onClose={() => setDeleteOpen(false)}
        onDeleted={() => navigate(`/datasets/${id}/branches`)}
      />
    </section>
  );
}

function BranchCompareSummary({ comparison }: { comparison: BranchCompareResponse | null }) {
  if (!comparison) return null;
  return (
    <section className="of-panel" style={{ padding: 12, display: 'grid', gap: 10 }}>
      <header>
        <p className="of-eyebrow">Branch divergence</p>
        <p className="of-text-muted" style={{ marginTop: 4, fontSize: 12 }}>
          LCA {comparison.lca_branch_rid ? shortId(comparison.lca_branch_rid) : '-'} · {comparison.conflicting_files.length} conflicting file{comparison.conflicting_files.length === 1 ? '' : 's'}
        </p>
      </header>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 8 }}>
        <TransactionColumn title={`Only on ${comparison.base_branch}`} rows={comparison.a_only_transactions} />
        <TransactionColumn title={`Only on ${comparison.compare_branch}`} rows={comparison.b_only_transactions} />
        <section style={{ padding: 10, background: 'var(--bg-subtle)', borderRadius: 8 }}>
          <p className="of-eyebrow" style={{ fontSize: 10 }}>Conflicts ({comparison.conflicting_files.length})</p>
          <ul style={{ marginTop: 6, paddingLeft: 0, listStyle: 'none', display: 'grid', gap: 4, maxHeight: 260, overflow: 'auto' }}>
            {comparison.conflicting_files.map((file) => (
              <li key={file.logical_path} style={{ fontSize: 11 }}>
                <code>{file.logical_path}</code>
                <p className="of-text-muted" style={{ margin: '2px 0 0', fontSize: 10 }}>
                  A {shortId(file.a_transaction_rid)} · B {shortId(file.b_transaction_rid)}
                </p>
              </li>
            ))}
            {comparison.conflicting_files.length === 0 && <li className="of-text-muted" style={{ fontSize: 11 }}>No conflicts.</li>}
          </ul>
        </section>
      </div>
    </section>
  );
}

function TransactionColumn({ title, rows }: { title: string; rows: BranchCompareResponse['a_only_transactions'] }) {
  return (
    <section style={{ padding: 10, background: 'var(--bg-subtle)', borderRadius: 8 }}>
      <p className="of-eyebrow" style={{ fontSize: 10 }}>{title} ({rows.length})</p>
      <ul style={{ marginTop: 6, paddingLeft: 0, listStyle: 'none', display: 'grid', gap: 4, maxHeight: 260, overflow: 'auto' }}>
        {rows.map((tx) => (
          <li key={tx.transaction_rid} style={{ fontSize: 11, fontFamily: 'var(--font-mono)' }}>
            {tx.tx_type} · {shortId(tx.transaction_rid)} · {formatDate(tx.committed_at)}
          </li>
        ))}
        {rows.length === 0 && <li className="of-text-muted" style={{ fontSize: 11 }}>No branch-only transactions.</li>}
      </ul>
    </section>
  );
}
