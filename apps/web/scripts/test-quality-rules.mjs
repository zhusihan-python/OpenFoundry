import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ESLint } from 'eslint';
import { applyBaseline, findingKey, lintOnce, validateBaseline, BASELINE_SCOPE } from './lint.mjs';

const eslint = new ESLint();

// ---------------------------------------------------------------------------
// Part 1: the pilot rules actually fire inside src/routes/datasets and stay
// silent elsewhere.
// ---------------------------------------------------------------------------
const options = { filePath: 'src/routes/datasets/QualityFixture.tsx' };
const cases = [
  ['direct fetch', 'export const load = () => fetch("/api/v1/datasets");', 'no-restricted-globals'],
  ['window fetch', 'export const load = () => window.fetch("/api/v1/datasets");', 'no-restricted-properties'],
  ['global fetch', 'export const load = () => globalThis.fetch("/api/v1/datasets");', 'no-restricted-properties'],
  ['conditional hook', 'import {useState} from "react"; export function Fixture({ok}: {ok: boolean}) { if(ok) useState(0); return null; }', 'react-hooks/rules-of-hooks'],
  ['missing dependency', 'import {useEffect} from "react"; export function Fixture({value}: {value: string}) { useEffect(() => { console.log(value); }, []); return null; }', 'react-hooks/exhaustive-deps'],
];
for (const [name, code, rule] of cases) {
  const [result] = await eslint.lintText(code, options);
  assert(result.messages.some(m => m.ruleId === rule && m.severity === 2), `${name}: expected ${rule}`);
}
const [valid] = await eslint.lintText('import {useEffect} from "react"; export function Fixture({value}: {value: string}) { useEffect(() => { console.log(value); }, [value]); return null; }', options);
assert.equal(valid.errorCount, 0, JSON.stringify(valid.messages));
const [api] = await eslint.lintText('export const load = () => fetch("/api/v1/datasets");', {filePath: 'src/lib/api/quality-fixture.ts'});
assert.equal(api.errorCount, 0);
console.log('PASS part 1: five invalid patterns rejected; valid hooks and API fetch allowed');

// ---------------------------------------------------------------------------
// Part 2: the baseline filter cannot be talked into a false green. All
// findings below come from real ESLint output, keys from findingKey itself —
// the key formula is anchored by part 3's run against the committed baseline.
// ---------------------------------------------------------------------------
const fixturePath = 'src/routes/datasets/QualityBaselineFixture.tsx';
const missingDep = dep => [
  "import { useEffect } from 'react';",
  '',
  `export function Fixture({ ${dep} }: { ${dep}: string }) {`,
  '  useEffect(() => {',
  `    console.log(${dep});`,
  '  }, []);',
  '  return null;',
  '}',
  '',
].join('\n');

const [debtResult] = await eslint.lintText(missingDep('value'), { filePath: fixturePath });
const debtMessage = debtResult.messages.find(m => m.ruleId === 'react-hooks/exhaustive-deps');
assert.ok(debtMessage, 'fixture must produce an exhaustive-deps finding');
const entry = count => ({ key: findingKey(debtResult, debtMessage), count, reason: 'test' });

// Baselined finding is filtered and counted; exit-worthy errors drop to zero.
{
  const { results: [out], known, total, stale } = applyBaseline([structuredClone(debtResult)], [entry(1)]);
  assert.equal(known, 1);
  assert.equal(total, 1);
  assert.equal(out.errorCount, 0);
  assert.equal(stale, false);
}

// Same finding with an empty baseline stays red.
{
  const { results: [out] } = applyBaseline([structuredClone(debtResult)], []);
  assert.equal(out.errorCount, 1);
}

// A second identical finding cannot reuse a count-1 entry.
{
  const duplicated = structuredClone(debtResult);
  duplicated.messages.push({ ...debtMessage });
  const { results: [out], known, stale } = applyBaseline([duplicated], [entry(1)]);
  assert.equal(known, 1);
  assert.equal(out.errorCount, 1);
  assert.equal(stale, false);
}

// A non-integer or non-positive count can never absorb a finding, even when
// applyBaseline is called directly with a hand-built baseline.
{
  for (const bad of [0, -1, NaN, 1.5]) {
    const { results: [out], known, stale } = applyBaseline([structuredClone(debtResult)], [entry(bad)]);
    assert.equal(known, 0, `count ${bad} must not absorb findings`);
    assert.equal(out.errorCount, 1);
    assert.equal(stale, false);
  }
}

// Changed message text: the old entry no longer matches (new error + stale).
{
  const changed = structuredClone(debtResult);
  const changedMessage = changed.messages.find(m => m.ruleId === 'react-hooks/exhaustive-deps');
  changedMessage.message = changedMessage.message.replace("'value'", "'other'");
  const { results: [out], known, stale } = applyBaseline([changed], [entry(1)]);
  assert.equal(known, 0);
  assert.equal(out.errorCount, 1);
  assert.equal(stale, true);
}

// Changed path: entry recorded under another file must not absorb findings.
{
  const moved = structuredClone(debtResult);
  moved.filePath = moved.filePath.replace('QualityBaselineFixture', 'OtherFixture');
  const { results: [out], known, stale } = applyBaseline([moved], [entry(1)]);
  assert.equal(known, 0);
  assert.equal(out.errorCount, 1);
  assert.equal(stale, true);
}

// Edited source inside the finding's context window: hash changes, entry goes
// stale (same missing dependency, so the message text itself is unchanged).
{
  const [edited] = await eslint.lintText(missingDep('value').replace('console.log', 'console.debug'), { filePath: fixturePath });
  const { results: [out], known, stale } = applyBaseline([edited], [entry(1)]);
  assert.equal(known, 0);
  assert.equal(out.errorCount, 1);
  assert.equal(stale, true);
}

// Two structurally identical violations share one key; a count-1 entry only
// absorbs one of them (padding keeps both context windows byte-identical).
{
  const block = name => [
    "import { useEffect } from 'react';",
    '',
    `export function Fixture${name}({ value }: { value: string }) {`,
    '    // pad1',
    '    // pad2',
    '    // pad3',
    '    // pad4',
    '    useEffect(() => {',
    '      console.log(value);',
    '    }, []);',
    '    return null;',
    '  }',
    '',
  ].join('\n');
  const source = block('A') + block('B').split('\n').slice(2).join('\n');
  const [twin] = await eslint.lintText(source, { filePath: fixturePath });
  const twins = twin.messages.filter(m => m.ruleId === 'react-hooks/exhaustive-deps');
  assert.equal(twins.length, 2);
  const twinEntry = { key: findingKey(twin, twins[0]), count: 1, reason: 'test' };
  const { results: [out], known } = applyBaseline([structuredClone(twin)], [twinEntry]);
  assert.equal(known, 1);
  assert.equal(out.errorCount, 1);
}

// Parse errors carry no ruleId and are never eligible for baselining, even
// via an entry whose key literally encodes the null ruleId.
{
  const [broken] = await eslint.lintText('export function ( {', { filePath: fixturePath });
  const parseError = broken.messages.find(m => m.fatal);
  assert.ok(parseError && parseError.ruleId === null, 'expected a fatal parse error');
  const hostile = structuredClone(broken);
  const hostileEntry = { key: findingKey({ ...hostile, source: '' }, { ...parseError, line: parseError.line }), count: 1, reason: 'test' };
  const { results: [out], known, stale } = applyBaseline([hostile], [hostileEntry]);
  assert.equal(known, 0);
  assert.equal(out.errorCount, 1);
  assert.equal(stale, true);
}

// Missing/empty source and unreadable files hash deterministically instead of
// crashing the run.
{
  const noSource = { ...structuredClone(debtResult), source: undefined, filePath: '/nonexistent/QualityFixture.tsx' };
  const { results: [out] } = applyBaseline([noSource], []);
  assert.equal(out.errorCount, 1);
  const emptySource = { ...structuredClone(debtResult), source: '' };
  const emptyEntry = { key: findingKey(emptySource, emptySource.messages.find(m => m.ruleId === 'react-hooks/exhaustive-deps')), count: 1, reason: 'test' };
  const rerun = applyBaseline([{ ...structuredClone(emptySource) }], [emptyEntry]);
  assert.equal(rerun.known, 1);
  assert.equal(rerun.results[0].errorCount, 0);
}
console.log('PASS part 2: baseline filter stays red on new/changed/duplicate/stale findings');

// ---------------------------------------------------------------------------
// Part 2b: validateBaseline rejects anything outside the pilot scope or with
// a malformed entry, so lint-baseline.json cannot grow into a lint silencer.
// ---------------------------------------------------------------------------
{
  const validEntry = entry(1);
  const scopeEscape = key => key.replace(`src/routes/datasets/${fixturePath.split('/').pop()}`, 'src/lib/api/OtherFixture.tsx');
  const badBaselines = [
    ['not an array', {}],
    ['item not an object', [null]],
    ['missing key', [{ count: 1, reason: 'test' }]],
    ['empty key', [{ ...validEntry, key: '' }]],
    ['missing reason', [{ key: validEntry.key, count: 1 }]],
    ['blank reason', [{ ...validEntry, reason: '   ' }]],
    ['zero count', [{ ...validEntry, count: 0 }]],
    ['negative count', [{ ...validEntry, count: -1 }]],
    ['NaN count', [{ ...validEntry, count: NaN }]],
    ['fractional count', [{ ...validEntry, count: 1.5 }]],
    ['string count', [{ ...validEntry, count: '1' }]],
    ['duplicate key', [validEntry, { ...validEntry }]],
    ['out-of-scope path', [{ ...validEntry, key: scopeEscape(validEntry.key) }]],
    ['path traversal', [{ ...validEntry, key: validEntry.key.replace(BASELINE_SCOPE.pathPrefix, `../${BASELINE_SCOPE.pathPrefix}`) }]],
    ['wrong rule', [{ ...validEntry, key: validEntry.key.replace(BASELINE_SCOPE.ruleId, 'react-hooks/rules-of-hooks') }]],
    ['parse-error ruleId', [{ ...validEntry, key: validEntry.key.replace(BASELINE_SCOPE.ruleId, 'null') }]],
    ['short hash', [{ ...validEntry, key: validEntry.key.slice(0, -1) }]],
    ['truncated key', [{ ...validEntry, key: 'src/routes/datasets/x.tsx|react-hooks/exhaustive-deps' }]],
  ];
  for (const [name, bad] of badBaselines) {
    assert.throws(() => validateBaseline(bad), Error, `expected rejection: ${name}`);
  }
  assert.doesNotThrow(() => validateBaseline([validEntry]));
  const committed = JSON.parse(readFileSync(new URL('./lint-baseline.json', import.meta.url), 'utf8'));
  assert.doesNotThrow(() => validateBaseline(committed));
  assert.equal(committed.length, 4);
}
console.log('PASS part 2b: baseline schema/scope validation rejects 18 malformed shapes');

// ---------------------------------------------------------------------------
// Part 3: the real CLI and the injected-baseline API stay red on every
// false-green vector. No environment variable can swap the baseline: the
// committed lint-baseline.json is the only input the CLI reads.
// ---------------------------------------------------------------------------
const baselinePath = new URL('./lint-baseline.json', import.meta.url);
const baseline = JSON.parse(readFileSync(baselinePath, 'utf8'));
// Unique per-run name + exclusive create: the test can never clobber or
// delete a file it did not create itself.
const fixtureName = `__quality_rules_${randomUUID().replace(/-/g, '')}__.tsx`;
const fixtureFile = join(process.cwd(), 'src/routes/datasets', fixtureName);
const fixtureCode = [
  "import { useEffect } from 'react';",
  '',
  '// Written and removed by scripts/test-quality-rules.mjs; never commit this file.',
  'export function QualityRulesFixture({ value }: { value: string }) {',
  '  useEffect(() => {',
  '    console.log(value);',
  '  }, []);',
  '  return null;',
  '}',
  '',
].join('\n');
const tmp = mkdtempSync(join(tmpdir(), 'of-lint-baseline-'));
const runCli = env => {
  const childEnv = { ...process.env, OPENFOUNDRY_LINT_BASELINE: env };
  const run = spawnSync('node', ['scripts/lint.mjs'], { encoding: 'utf8', cwd: process.cwd(), env: childEnv });
  assert.equal(typeof run.status, 'number', `lint wrapper crashed: ${run.stderr}`);
  return run;
};

let fixtureCreated = false;
try {
  const clean = runCli(join(tmp, 'ignored.json'));
  assert.equal(clean.status, 0, `expected green on clean tree:\n${clean.stdout}\n${clean.stderr}`);
  assert.match(clean.stdout, /4\/4/);
  assert.equal(clean.stderr, '');

  // A baseline file pointed at by the environment must be ignored entirely:
  // removing an entry via OPENFOUNDRY_LINT_BASELINE cannot resurface or hide
  // anything because the CLI never reads that variable.
  const shrunk = JSON.stringify(baseline.slice(0, -1));
  writeFileSync(join(tmp, 'ignored.json'), shrunk);
  const ignoredEnv = runCli(join(tmp, 'ignored.json'));
  assert.equal(ignoredEnv.status, 0, 'the CLI must ignore OPENFOUNDRY_LINT_BASELINE');
  assert.match(ignoredEnv.stdout, /4\/4/);

  writeFileSync(fixtureFile, fixtureCode, { flag: 'wx' });
  fixtureCreated = true;
  const withViolation = runCli(join(tmp, 'ignored.json'));
  assert.equal(withViolation.status, 1, 'a NEW violation must fail the wrapper');
  assert.match(withViolation.stdout, new RegExp(fixtureName.replaceAll('.', '\\.')));
  assert.match(withViolation.stdout, /missing dependency: 'value'/);
} finally {
  // Remove only what this run created; unknown files are never touched.
  if (fixtureCreated) unlinkSync(fixtureFile);
}

// Injected-baseline scenarios share one real lint pass over the working tree;
// each scenario mutates only its own deep copy of the results.
const rawResults = structuredClone(await eslint.lintFiles(['.']));
{
  const { failed, known, total } = await lintOnce({ baseline, results: structuredClone(rawResults) });
  assert.equal(failed, false);
  assert.equal(known, 4);
  assert.equal(total, 4);
}
{
  const ghost = JSON.parse(JSON.stringify(baseline));
  ghost.push({ key: `${BASELINE_SCOPE.pathPrefix}DoesNotExist.tsx|${BASELINE_SCOPE.ruleId}|ghost finding|${'0'.repeat(64)}`, count: 1, reason: 'test' });
  const { failed, stale } = await lintOnce({ baseline: ghost, results: structuredClone(rawResults) });
  assert.equal(stale, true, 'a baseline entry matching nothing must fail');
  assert.equal(failed, true);
}
{
  const doubled = baseline.map((item, index) => index === 0 ? { ...item, count: item.count + 1 } : item);
  const { failed, stale } = await lintOnce({ baseline: doubled, results: structuredClone(rawResults) });
  assert.equal(stale, true, 'an over-counted baseline entry must fail');
  assert.equal(failed, true);
}
{
  const removed = await lintOnce({ baseline: baseline.slice(0, -1), results: structuredClone(rawResults) });
  assert.equal(removed.stale, false);
  assert.equal(removed.known, 3);
  assert.equal(removed.failed, true, 'deleting a baseline entry must resurface its finding');
  assert.ok(removed.newErrors.some(e => e.file === `${BASELINE_SCOPE.pathPrefix}DatasetsListPage.tsx`));
}
assert.throws(() => validateBaseline(baseline.concat(baseline[0])), /duplicate key/);
rmSync(tmp, { recursive: true, force: true });
console.log('PASS part 3: CLI ignores env overrides; new violation, ghost entry, over-count, and removed entry all fail');
