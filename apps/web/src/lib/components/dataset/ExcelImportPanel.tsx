import { useEffect, useRef, useState } from 'react';

import {
  inspectExcelWorkbook,
  previewExcelWorkbook,
  type ExcelColumnMap,
  type ExcelImportConfig,
  type ExcelInspectResult,
  type ExcelPreviewResult,
  type ExcelSourceColumn,
} from '@/lib/api/datasets';

export interface ExcelImportPanelState {
  /** True while inspect / discovery / validation preview is in flight. */
  busy: boolean;
  /** The server-validated config to submit with; null until the current
   *  mapping has passed a server preview, and cleared again the moment
   *  sheet / header / targets / file change. */
  validatedConfig: ExcelImportConfig | null;
}

// Every async step (inspect, discovery, validation preview) runs under a
// single generation counter with a shared AbortController: starting a new
// step aborts the previous request, and a response may only touch state when
// it is still the current generation AND the component is mounted. A late
// response can therefore never overwrite rows, busy flags, or re-enable
// submit for a configuration the user has already edited away from.
export function ExcelImportPanel({
  file,
  logicalPath,
  disabled = false,
  onStateChange,
}: {
  file: File;
  logicalPath: string;
  disabled?: boolean;
  onStateChange: (state: ExcelImportPanelState) => void;
}) {
  const [inspect, setInspect] = useState<ExcelInspectResult | null>(null);
  const [sheet, setSheet] = useState('');
  const [hasHeader, setHasHeader] = useState(true);
  const [headerDraft, setHeaderDraft] = useState('1');
  // null = no header row (first row is data); otherwise the committed 1-based
  // header row the current discovery ran with. The draft only becomes
  // committed on blur/Enter so per-keystroke edits don't re-run discovery.
  const [committedHeader, setCommittedHeader] = useState<number | null>(1);
  const [sourceColumns, setSourceColumns] = useState<ExcelSourceColumn[] | null>(null);
  const [mapping, setMapping] = useState<ExcelColumnMap[] | null>(null);
  const [preview, setPreview] = useState<ExcelPreviewResult | null>(null);
  const [validatedConfig, setValidatedConfig] = useState<ExcelImportConfig | null>(null);
  const [busyPhase, setBusyPhase] = useState<'inspect' | 'discover' | 'preview' | null>('inspect');
  const [error, setError] = useState('');

  const generationRef = useRef(0);
  const abortRef = useRef<AbortController | null>(null);
  const aliveRef = useRef(true);

  // Report gate state upward; the parent's canSubmit combines this with its
  // own name/path/busy checks. The callback is kept in a ref so an unstable
  // parent identity cannot re-trigger the report effect in a loop.
  const onStateChangeRef = useRef(onStateChange);
  useEffect(() => {
    onStateChangeRef.current = onStateChange;
  });
  useEffect(() => {
    onStateChangeRef.current({ busy: busyPhase !== null, validatedConfig });
  }, [busyPhase, validatedConfig]);

  useEffect(() => {
    aliveRef.current = true;
    return () => {
      aliveRef.current = false;
      abortRef.current?.abort();
    };
  }, []);

  function startRun() {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    return {
      signal: controller.signal,
      isCurrent: () => aliveRef.current && generation === generationRef.current,
    };
  }

  // Drop any in-flight response (bump the generation it checks against) and
  // forget prior validation success. Called for every mutation that makes the
  // previously validated config stale. Aborting leaves no run in flight, so
  // the busy flag must be cleared here — the aborted run can no longer do it.
  function invalidateValidation() {
    generationRef.current += 1;
    abortRef.current?.abort();
    setBusyPhase(null);
    setPreview(null);
    setValidatedConfig(null);
  }

  function clearMappingState() {
    invalidateValidation();
    setSourceColumns(null);
    setMapping(null);
  }

  async function runInspect() {
    const run = startRun();
    setBusyPhase('inspect');
    setError('');
    try {
      const result = await inspectExcelWorkbook(file, run.signal);
      if (!run.isCurrent()) return;
      setInspect(result);
      setBusyPhase(null);
    } catch (cause) {
      if (!run.isCurrent() || isAbortError(cause)) return;
      setError(errorMessage(cause));
      setBusyPhase(null);
    }
  }

  async function runDiscovery(targetSheet: string, targetHeader: number | null) {
    const run = startRun();
    setBusyPhase('discover');
    setError('');
    setPreview(null);
    setValidatedConfig(null);
    setSourceColumns(null);
    setMapping(null);
    try {
      const result = await previewExcelWorkbook(
        file,
        { sheet: targetSheet, header_row: targetHeader, mapping: null },
        { sampleRows: 0, signal: run.signal },
      );
      if (!run.isCurrent()) return;
      setSourceColumns(result.source_columns);
      setMapping(suggestMapping(result.source_columns));
      setBusyPhase(null);
    } catch (cause) {
      if (!run.isCurrent() || isAbortError(cause)) return;
      setError(errorMessage(cause));
      setBusyPhase(null);
    }
  }

  async function runValidate() {
    if (!sheet || !mapping || !mappingValid) return;
    const run = startRun();
    setBusyPhase('preview');
    setError('');
    setPreview(null);
    setValidatedConfig(null);
    const config: ExcelImportConfig = {
      sheet,
      header_row: committedHeader,
      mapping: mapping.map((entry) => ({ source_index: entry.source_index, target: entry.target.trim() })),
    };
    try {
      const result = await previewExcelWorkbook(file, config, { signal: run.signal });
      if (!run.isCurrent()) return;
      setPreview(result);
      // Submit uses exactly the config the server acknowledged and echoed
      // back, never a client-side reinterpretation of it.
      setValidatedConfig(result.config);
      setBusyPhase(null);
    } catch (cause) {
      if (!run.isCurrent() || isAbortError(cause)) return;
      setError(errorMessage(cause));
      setBusyPhase(null);
    }
  }

  useEffect(() => {
    void runInspect();
    // The parent remounts this panel (keyed by file identity) when the file
    // changes, so inspect only needs to run once per mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!sheet) return;
    void runDiscovery(sheet, committedHeader);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sheet, committedHeader]);

  function selectSheet(next: string) {
    if (!next || next === sheet) return;
    clearMappingState();
    setError('');
    setSheet(next);
  }

  function parseHeaderDraft(): number {
    const parsed = Number.parseInt(headerDraft, 10);
    if (!Number.isFinite(parsed)) return 1;
    return Math.min(100, Math.max(1, parsed));
  }

  function toggleHeader(next: boolean) {
    const nextHeader = next ? parseHeaderDraft() : null;
    setHasHeader(next);
    if (nextHeader !== committedHeader) {
      clearMappingState();
      setCommittedHeader(nextHeader);
    }
  }

  function commitHeaderDraft() {
    if (!hasHeader) return;
    const next = parseHeaderDraft();
    setHeaderDraft(String(next));
    if (next !== committedHeader) {
      clearMappingState();
      setCommittedHeader(next);
    }
  }

  function updateHeaderDraft(next: string) {
    setHeaderDraft(next);
    // A pending header edit already describes a different configuration than
    // anything previously validated: invalidate immediately and gate upload.
    // Discovery only re-runs when the draft is committed (blur/Enter).
    invalidateValidation();
  }

  function updateTarget(sourceIndex: number, target: string) {
    if (!mapping) return;
    setMapping(mapping.map((entry) => (
      entry.source_index === sourceIndex ? { ...entry, target } : entry
    )));
    // Any target edit invalidates validation immediately; a preview response
    // still in flight for the old targets must be dropped.
    invalidateValidation();
  }

  const trimmedTargets = mapping?.map((entry) => entry.target.trim()) ?? [];
  const hasEmptyTarget = trimmedTargets.some((target) => target === '');
  const duplicateTargets = trimmedTargets.filter(
    (target, index) => target !== '' && trimmedTargets.indexOf(target) !== index,
  );
  const mappingValid = Boolean(mapping && mapping.length > 0) && !hasEmptyTarget && duplicateTargets.length === 0;
  const inputsDisabled = disabled || busyPhase !== null;

  return (
    <section className="of-panel" style={{ padding: 12, display: 'grid', gap: 12 }} data-testid="excel-import-panel">
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center' }}>
        <div>
          <p className="of-eyebrow" style={{ margin: 0 }}>XLSX workbook</p>
          <h2 className="of-heading-sm" style={{ margin: 0 }}>Excel import mapping</h2>
        </div>
        <span className="of-chip">{inspect ? `${inspect.sheets.length} sheets` : 'inspecting'}</span>
      </div>

      <div className="of-panel-muted" style={{ padding: 10, display: 'grid', gap: 4, fontSize: 12 }}>
        <p style={{ margin: 0 }}>
          Only plain <code>.xlsx</code> workbooks are supported (<code>.xls</code>/<code>.xlsm</code> are rejected).
          Pick a worksheet and header row, map source columns to unique target names, and validate on the server
          before uploading.
        </p>
        <p className="of-text-muted" style={{ margin: 0 }}>
          The selected range is imported as JSON (NDJSON) and written as <code>{logicalPath || '(path above)'}</code>.
          Limits: {inspect?.max_data_rows ?? 1000} data rows / 256 target columns. Values are imported as text exactly
          as stored by the workbook; blank cells become <code>null</code>. Typing and timezone conversion are not
          applied.
        </p>
      </div>

      {error && (
        <div className="of-status-danger" style={{ padding: '8px 10px', borderRadius: 'var(--radius-md)', fontSize: 12 }} data-testid="excel-error">
          {error}
        </div>
      )}

      {busyPhase === 'inspect' && (
        <div className="of-panel-muted" style={{ padding: 10 }}>
          <p className="of-text-muted" style={{ margin: 0 }}>Reading workbook worksheets...</p>
        </div>
      )}

      {inspect && busyPhase !== 'inspect' && (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 200px), 1fr))', gap: 10 }}>
            <label style={{ display: 'grid', gap: 4, fontSize: 12, color: 'var(--text-strong)', fontWeight: 600 }}>
              Worksheet
              <select
                className="of-input"
                aria-label="Worksheet"
                value={sheet}
                disabled={inputsDisabled}
                onChange={(event) => selectSheet(event.target.value)}
              >
                <option value="" disabled>Choose a worksheet…</option>
                {inspect.sheets.map((entry) => (
                  <option key={entry.name} value={entry.name}>{entry.name} (#{entry.index + 1})</option>
                ))}
              </select>
            </label>
            <div style={{ display: 'grid', gap: 4, fontSize: 12, color: 'var(--text-strong)', fontWeight: 600 }}>
              Header row
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <label style={{ display: 'flex', gap: 6, alignItems: 'center', fontWeight: 400 }}>
                  <input
                    type="checkbox"
                    aria-label="Includes header row"
                    checked={hasHeader}
                    disabled={inputsDisabled}
                    onChange={(event) => toggleHeader(event.target.checked)}
                  />
                  has header
                </label>
                {hasHeader && (
                  <input
                    type="number"
                    className="of-input"
                    aria-label="Header row number"
                    min={1}
                    max={100}
                    value={headerDraft}
                    disabled={inputsDisabled}
                    onChange={(event) => updateHeaderDraft(event.target.value)}
                    onBlur={commitHeaderDraft}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        // Keep Enter from submitting the surrounding upload form.
                        event.preventDefault();
                        commitHeaderDraft();
                      }
                    }}
                    style={{ width: 90 }}
                  />
                )}
              </div>
              <span className="of-text-muted" style={{ fontWeight: 400, fontSize: 11 }}>
                1-based physical row; untick when the first row is data (it is kept).
              </span>
            </div>
          </div>

          {busyPhase === 'discover' && (
            <div className="of-panel-muted" style={{ padding: 10 }}>
              <p className="of-text-muted" style={{ margin: 0 }}>Discovering source columns...</p>
            </div>
          )}

          {!busyPhase && !mapping && sheet && (
            <div className="of-panel-muted" style={{ padding: 10 }}>
              <p className="of-text-muted" style={{ margin: 0 }}>
                No columns discovered for this sheet/header combination yet.
              </p>
            </div>
          )}

          {mapping && sourceColumns && (
            <>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                <p className="of-text-muted" style={{ margin: 0, fontSize: 12 }}>
                  Map each source column to a unique target name. Suggestions come from the header row; empty or
                  duplicate headers get automatic fallback names.
                </p>
                <button
                  type="button"
                  className="of-button of-button--primary"
                  data-testid="excel-validate"
                  disabled={!mappingValid || inputsDisabled}
                  onClick={() => void runValidate()}
                >
                  {busyPhase === 'preview' ? 'Validating...' : 'Validate & preview'}
                </button>
              </div>

              <div style={{ overflow: 'auto', border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)' }}>
                <table className="of-table" style={{ minWidth: 420 }} data-testid="excel-mapping-table">
                  <thead>
                    <tr><th>Source column</th><th>Original header</th><th>Target name</th></tr>
                  </thead>
                  <tbody>
                    {mapping.map((entry) => {
                      const source = sourceColumns.find((column) => column.index === entry.source_index);
                      const trimmed = entry.target.trim();
                      const isDuplicate = trimmed !== '' && trimmedTargets.filter((t) => t === trimmed).length > 1;
                      const rowInvalid = trimmed === '' || isDuplicate;
                      return (
                        <tr key={entry.source_index}>
                          <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>
                            {excelColumnLetter(entry.source_index)} · {entry.source_index + 1}
                          </td>
                          <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {source?.header ? source.header : <span className="of-text-muted">(empty)</span>}
                          </td>
                          <td>
                            <input
                              className="of-input"
                              aria-label={`target-${entry.source_index}`}
                              value={entry.target}
                              disabled={inputsDisabled}
                              onChange={(event) => updateTarget(entry.source_index, event.target.value)}
                              style={{
                                width: '100%',
                                minWidth: 160,
                                borderColor: rowInvalid ? 'var(--status-danger)' : undefined,
                              }}
                            />
                            {trimmed === '' && (
                              <span style={{ color: 'var(--status-danger)', fontSize: 11 }}>target name is required</span>
                            )}
                            {isDuplicate && (
                              <span style={{ color: 'var(--status-danger)', fontSize: 11 }}>duplicate target name</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {(hasEmptyTarget || duplicateTargets.length > 0) && (
                <p style={{ margin: 0, color: 'var(--status-danger)', fontSize: 12 }}>
                  {hasEmptyTarget && 'Target names must not be empty. '}
                  {duplicateTargets.length > 0 && `Duplicate target names: ${Array.from(new Set(duplicateTargets)).join(', ')}.`}
                  Fix them before validating.
                </p>
              )}
            </>
          )}

          {preview && (
            <>
              <div
                className="of-status-success"
                style={{ padding: '8px 10px', borderRadius: 'var(--radius-md)', fontSize: 12 }}
                data-testid="excel-validated"
              >
                Validated on the server: {preview.row_count} data row{preview.row_count === 1 ? '' : 's'} ·{' '}
                {preview.columns.length} target column{preview.columns.length === 1 ? '' : 's'} · imported as{' '}
                <code>{logicalPath || 'workbook.ndjson'}</code>
              </div>
              <div style={{ overflow: 'auto', border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)' }}>
                <table className="of-table" style={{ minWidth: 420 }} data-testid="excel-preview-table">
                  <thead>
                    <tr>
                      {preview.columns.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.sample_rows.map((row, rowIndex) => (
                      <tr key={rowIndex}>
                        {preview.columns.map((column) => (
                          <td key={column}>{row[column] === null || row[column] === undefined ? <span className="of-text-muted">null</span> : row[column]}</td>
                        ))}
                      </tr>
                    ))}
                    {preview.sample_rows.length === 0 && (
                      <tr><td colSpan={preview.columns.length} className="of-text-muted">No sample rows.</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </>
      )}
    </section>
  );
}

// Suggest a unique, non-empty target for every discovered source column:
// the trimmed original header when it is unique so far, `<header>_<index>`
// when the header repeats, and `column_<index>` when the header is blank.
// Identity is always the physical source index, never the header text.
function suggestMapping(sourceColumns: ExcelSourceColumn[]): ExcelColumnMap[] {
  const used = new Set<string>();
  return sourceColumns.map((column) => {
    const header = column.header.trim();
    let target = header !== '' && !used.has(header) ? header : header !== '' ? `${header}_${column.index}` : `column_${column.index}`;
    while (used.has(target)) {
      target = `${target}_${column.index}`;
    }
    used.add(target);
    return { source_index: column.index, target };
  });
}

function isAbortError(cause: unknown) {
  return cause instanceof DOMException && cause.name === 'AbortError';
}

// Excel column letters for the user-facing source position (A, B, … AA); the
// wire format keeps using the 0-based physical source_index.
function excelColumnLetter(index: number): string {
  let letter = '';
  let position = index + 1;
  while (position > 0) {
    const remainder = (position - 1) % 26;
    letter = String.fromCharCode(65 + remainder) + letter;
    position = Math.floor((position - 1) / 26);
  }
  return letter;
}

function errorMessage(cause: unknown) {
  return cause instanceof Error && cause.message ? cause.message : 'Excel request failed';
}
