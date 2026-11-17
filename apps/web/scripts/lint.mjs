import { ESLint } from 'eslint';
import { readFileSync } from 'node:fs';
import { relative } from 'node:path';
import { createHash } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export const BASELINE_SCOPE = {
  pathPrefix: 'src/routes/datasets/',
  ruleId: 'react-hooks/exhaustive-deps',
};

function sourceOf(result) {
  if (typeof result.source === 'string') return result.source;
  // ESLint omits `source` for clean files and some edge results. Fall back to
  // reading from disk; if that fails, hash a deterministic empty context
  // instead of crashing mid-run. Either way the finding cannot silently
  // match an unrelated baseline entry.
  try {
    return readFileSync(result.filePath, 'utf8');
  } catch {
    return '';
  }
}

export function findingKey(result, message, cwd = process.cwd()) {
  const source = sourceOf(result);
  const line = message.line ?? 0;
  const context = source.split('\n').slice(Math.max(0, line - 3), line + 1).join('\n');
  return [
    relative(cwd, result.filePath).replaceAll('\\', '/'),
    message.ruleId,
    message.message,
    createHash('sha256').update(context).digest('hex'),
  ].join('|');
}

// A baseline is a JSON array of { key, count, reason }. Keys are
// `path|ruleId|message|sha256(context)` as produced by findingKey. Only the
// pilot scope (src/routes/datasets + exhaustive-deps) may be baselined, so
// parse errors (ruleId null) and any other rule or directory are rejected
// here — they can never be silenced through lint-baseline.json.
export function validateBaseline(baseline) {
  if (!Array.isArray(baseline)) throw new Error('lint baseline must be a JSON array');
  const seen = new Set();
  baseline.forEach((item, index) => {
    const where = `lint baseline entry ${index}`;
    if (typeof item !== 'object' || item === null) throw new Error(`${where} must be an object`);
    if (typeof item.key !== 'string' || item.key.length === 0) throw new Error(`${where}: key must be a non-empty string`);
    if (typeof item.reason !== 'string' || item.reason.trim().length === 0) throw new Error(`${where}: reason must be a non-empty string`);
    if (!Number.isInteger(item.count) || item.count < 1) throw new Error(`${where}: count must be a positive integer`);
    if (seen.has(item.key)) throw new Error(`${where}: duplicate key (merge counts on a single entry instead)`);
    seen.add(item.key);
    const segments = item.key.split('|');
    const [path, ruleId] = segments;
    const hash = segments[segments.length - 1];
    const message = segments.slice(2, -1).join('|');
    if (segments.length < 4 || !message) throw new Error(`${where}: key must be path|ruleId|message|sha256`);
    if (!path.startsWith(BASELINE_SCOPE.pathPrefix) || path.includes('..')) throw new Error(`${where}: key path must stay under ${BASELINE_SCOPE.pathPrefix}`);
    if (ruleId !== BASELINE_SCOPE.ruleId) throw new Error(`${where}: key ruleId must be ${BASELINE_SCOPE.ruleId}`);
    if (!/^[0-9a-f]{64}$/.test(hash)) throw new Error(`${where}: key hash must be a 64-char hex sha256`);
  });
  return baseline;
}

// Filters `results` in place against `baseline`, recomputes per-file counts,
// and reports whether every baseline entry was consumed. Messages without a
// ruleId (parse errors, config failures) are never eligible for baselining.
// Callers may pass pre-collected results (already linted, safe to mutate);
// the CLI always lints the working tree itself.
export function applyBaseline(results, baseline, cwd = process.cwd()) {
  const remaining = new Map(baseline.map(item => [item.key, item.count]));
  let known = 0;
  for (const result of results) {
    const source = sourceOf(result);
    result.messages = result.messages.filter(message => {
      if (!message.ruleId) return true;
      const key = findingKey({ ...result, source }, message, cwd);
      const count = remaining.get(key) ?? 0;
      if (Number.isInteger(count) && count > 0) { remaining.set(key, count - 1); known++; return false; }
      return true;
    });
    result.errorCount = result.messages.filter(m => m.severity === 2).length;
    result.warningCount = result.messages.filter(m => m.severity === 1).length;
  }
  const stale = [...remaining.values()].some(count => Number.isInteger(count) && count > 0);
  const total = baseline.reduce((n, item) => n + item.count, 0);
  return { results, known, total, stale };
}

export async function lintOnce({ baseline, results } = {}) {
  const eslint = new ESLint();
  const resolvedBaseline = baseline ?? JSON.parse(readFileSync(new URL('./lint-baseline.json', import.meta.url), 'utf8'));
  validateBaseline(resolvedBaseline);
  const resolvedResults = results ?? await eslint.lintFiles(['.']);
  const { results: filtered, known, total, stale } = applyBaseline(resolvedResults, resolvedBaseline);
  const newErrors = filtered.flatMap(result => result.messages
    .filter(message => message.severity === 2)
    .map(message => ({ file: relative(process.cwd(), result.filePath).replaceAll('\\', '/'), ruleId: message.ruleId, message: message.message })));
  console.log(await (await eslint.loadFormatter('stylish')).format(filtered));
  console.log(`Known dataset Hooks debt: ${known}/${total}. New findings are never baselined automatically.`);
  if (stale) console.error('A baseline finding changed or disappeared: review and remove/update its exact entry.');
  return { failed: stale || newErrors.length > 0, stale, known, total, newErrors };
}

async function main() {
  const { failed } = await lintOnce();
  if (failed) process.exitCode = 1;
}

const invokedDirectly = process.argv[1]
  && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
if (invokedDirectly) await main();
