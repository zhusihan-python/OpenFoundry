// Input is supplied by preview-ui.sh for the run's published noon order lines.
const fs = await import("node:fs/promises");
const assert = (await import("node:assert/strict")).default;
const ready = JSON.parse(await fs.readFile(input.state + "/compare-ready.json", "utf8"));
const token = (await fs.readFile(input.state + "/browser.jwt", "utf8")).trim();
const base = input.origin;
const task = await taskSpace("OpenFoundry noon order line version comparison");
await fs.writeFile(input.state + "/browser-space", String(task.spaceId), { mode: 0o600 });
const page = task.page("p1");
// A cold dev server compiles the page on its first load.
const load = 90000;

const rowKeyInput = 'loc=role:textbox[name="Key column"]';
const compareRows = 'loc=role:button[name="Compare rows"]';

function comparisonSection() {
  return [...document.querySelectorAll("h2")]
    .find((node) => node.textContent.trim() === "Compare rows by key")?.closest("section");
}

async function openCompareTab(url) {
  await page.goto(url, { timeout: load });
  await page.waitForSelector('loc=role:tab[name="Compare"]', { timeout: load });
  await page.click('loc=role:tab[name="Compare"]');
}

async function selectSides(selections) {
  for (const [side, value] of Object.entries(selections)) {
    const locator = `loc=role:combobox[name="Side ${side}"]`;
    await page.waitForSelector(locator, { timeout: load });
    await page.waitForFunction((option) => [...document.querySelectorAll("select option")]
      .some((node) => node.value === option), value, { timeout: load });
    await page.selectOption(locator, value);
  }
}

async function openRowComparison(url, selections) {
  await openCompareTab(url);
  await selectSides(selections);
  await page.waitForSelector(rowKeyInput, { timeout: load });
}

// The page's own rendering and request, independent of its diff calculation.
function readRowComparison(dataset, older, newer, key, limit) {
  return page.evaluate(({ dataset, older, newer, key, limit }) => {
    const section = [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare rows by key")?.closest("section");
    const table = section?.querySelector('table[aria-label="Row changes"]');
    const selectors = Object.fromEntries(["A", "B"].map((side) => {
      const control = [...document.querySelectorAll("select")]
        .find((select) => [...select.labels].some((label) => label.textContent.trim() === `Side ${side}`));
      return [side, control ? {
        value: control.value,
        label: control.selectedOptions[0]?.textContent.trim() ?? null,
        options: [...control.options].map((option) => option.value),
      } : null];
    }));
    const requestPath = `/api/v1/datasets/${dataset}/compare/rows`;
    const params = {
      key, limit: String(limit),
      base_branch: older.branch, base_version: String(older.version), base_transaction: older.transaction_id,
      target_branch: newer.branch, target_version: String(newer.version), target_transaction: newer.transaction_id,
    };
    const request = performance.getEntriesByType("resource").reverse().find((entry) => {
      const address = new URL(entry.name);
      return entry.initiatorType === "fetch" && address.origin === location.origin && address.pathname === requestPath
        && Object.entries(params).every(([name, value]) => address.searchParams.get(name) === value);
    });
    const rows = [...(table?.tBodies[0]?.rows ?? [])].map((row) => {
      const [change, key, before, after] = [...row.cells].map((cell) => cell.textContent.trim());
      return { key, kind: change.toLowerCase(),
        ...(before !== "—" && { before: JSON.parse(before) }),
        ...(after !== "—" && { after: JSON.parse(after) }) };
    });
    return {
      selectors, rows, table: !!table, key_column: section?.querySelector('input[class="of-input"]')?.value ?? null,
      alerts: [...(section?.parentElement?.querySelectorAll('[role="alert"]') ?? [])].map((node) => node.textContent.trim().slice(0, 300)),
      unavailable_labels: [...(section?.parentElement?.querySelectorAll('strong') ?? [])]
        .filter((node) => /unavailable/i.test(node.textContent)).length,
      schema_notices: [...(section?.parentElement?.querySelectorAll('.of-text-muted') ?? [])]
        .filter((node) => /no schema preview/i.test(node.textContent)).length,
      summary: section?.querySelector('[role="status"]')?.textContent.trim() ?? null,
      error: section?.querySelector('[role="alert"]')?.textContent.trim() ?? null,
      navigation_unix_ms: performance.timeOrigin,
      request: request ? {
        path: requestPath, params: Object.fromEntries(new URL(request.name).searchParams), status: request.responseStatus,
        started_unix_ms: performance.timeOrigin + request.startTime,
        completed_unix_ms: performance.timeOrigin + request.responseEnd,
        transferred_bytes: request.transferSize,
      } : null,
    };
  }, { dataset, older, newer, key, limit });
}

// The dev stub answers the profile with a fixed identity; mask its labels.
async function maskIdentity(labels) {
  return page.evaluate((labels) => {
    const masked = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      for (const label of labels) {
        if (node.nodeValue.includes(label)) {
          node.nodeValue = node.nodeValue.split(label).join("Reader (anonymized)");
          masked.push(label);
        }
      }
    }
    return masked;
  }, labels);
}

// The page's own rendering and requests for the schemas of two committed
// versions, independent of its diff calculation.
function readSchemaComparison(dataset, base, target) {
  return page.evaluate(({ dataset, base, target }) => {
    const section = [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare schemas")?.closest("section");
    const panels = [...(section?.querySelectorAll(".of-panel") ?? [])];
    const titled = (title) => panels.find((panel) => panel.querySelector(".of-eyebrow")?.textContent.trim() === title);
    const diff = titled("Schema diff");
    // Every item a Schema diff panel holds, counted one by one: a change carries
    // a sign and a name, the empty notice is one sentence, and anything else is
    // kept as well, so a panel never states two things unnoticed.
    const counted = (panel) => {
      const items = [...(panel?.querySelectorAll("li") ?? [])];
      const rows = [], other = [];
      let empty_notices = 0;
      for (const item of items) {
        const spans = [...item.querySelectorAll("span")].map((span) => span.textContent.trim());
        if (spans.length) {
          const [sign, name, hint] = spans;
          rows.push({ sign, name, ...(hint && { hint }) });
        } else if (item.textContent.trim() === "No schema changes.") empty_notices += 1;
        else other.push(item.textContent.trim());
      }
      return { items: items.length, rows, empty_notices, other };
    };
    // Panels the page must never show, read with the code that reads its own.
    const row = '<li><span>~</span><span>unit_price</span><span>STRING → DOUBLE</span></li>';
    const notice = "<li>No schema changes.</li>";
    const synthetic = (html) => {
      const panel = document.createElement("div");
      panel.innerHTML = `<ul>${html}</ul>`;
      return counted(panel);
    };
    const own = '<li>unit_price is now a number</li>';
    const controls = [synthetic(row + notice), synthetic(row + own), synthetic(row), synthetic(notice), synthetic(notice + own)];
    const selectors = {}, sides = {};
    for (const side of ["A", "B"]) {
      const control = [...(section?.querySelectorAll("select") ?? [])]
        .find((select) => [...select.labels].some((label) => label.textContent.trim() === `Side ${side}`));
      const notes = [...(control?.closest(".of-panel")?.querySelectorAll(".of-text-muted") ?? [])]
        .map((node) => node.textContent.trim());
      const declared = /^(\d+) fields?/.exec(notes.find((note) => /^\d+ fields?/.test(note)) ?? "");
      selectors[side] = control ? {
        value: control.value,
        label: control.selectedOptions[0]?.textContent.trim() ?? null,
        options: [...control.options].map((option) => option.value),
      } : null;
      sides[side] = { fields: declared ? Number(declared[1]) : null };
    }
    const requestPath = `/api/v1/datasets/${dataset}/schema`;
    const request = (pin) => {
      // Resource entries are chronological: the last match is the page's latest request for that version.
      const entry = performance.getEntriesByType("resource").reverse().find((candidate) => {
        const address = new URL(candidate.name);
        return candidate.initiatorType === "fetch" && address.origin === location.origin && address.pathname === requestPath
          && address.searchParams.get("branch") === pin.branch && address.searchParams.get("version") === String(pin.version);
      });
      return entry ? {
        path: requestPath, params: Object.fromEntries(new URL(entry.name).searchParams), status: entry.responseStatus,
        started_unix_ms: performance.timeOrigin + entry.startTime,
        completed_unix_ms: performance.timeOrigin + entry.responseEnd,
        transferred_bytes: entry.transferSize,
      } : null;
    };
    return {
      selectors, sides, panel: !!diff, ...counted(diff), control: controls,
      files_panel: !!titled("Files diff"),
      alerts: [...(section?.querySelectorAll('[role="alert"]') ?? [])].map((node) => node.textContent.trim().slice(0, 300)),
      unavailable_labels: [...(section?.querySelectorAll("strong") ?? [])]
        .filter((node) => /unavailable/i.test(node.textContent)).length,
      schema_notices: [...(section?.querySelectorAll(".of-text-muted") ?? [])]
        .filter((node) => /no schema preview/i.test(node.textContent)).length,
      navigation_unix_ms: performance.timeOrigin,
      read_unix_ms: performance.timeOrigin + performance.now(),
      requests: { A: request(base), B: request(target) },
    };
  }, { dataset, base, target });
}

// The dev stub's identity labels masked, then the page kept as the reader saw it.
async function keepCapture(name, read) {
  const masked = await maskIdentity(ready.anonymize);
  const capture = `${name}.png`;
  await page.screenshot({ path: input.state + "/" + capture, fullPage: true });
  const dom = await read();
  const captured_at_unix_ms = Date.now();
  await fs.writeFile(input.state + `/${name}.dom.html`, [
    "<!doctype html>",
    "<!-- Anonymized capture of the Compare panel. Identity labels replaced: " + masked.length + " -->",
    `<html><head><meta charset="utf-8"><title>${dom.url}</title></head><body>`,
    dom.compare,
    "</body></html>",
  ].join("\n") + "\n", { mode: 0o600 });
  return { capture, dom: `${name}.dom.html`, masked_identity_labels: masked, captured_at_unix_ms,
    captured_at: new Date(captured_at_unix_ms).toISOString() };
}

// The panel states exactly one thing: the expected changes and no empty notice
// or, without any, one empty notice and no change. Nothing else is in it.
function statesChanges(view, expected) {
  const changes = expected.length;
  return view.items === changes + (changes ? 0 : 1) && view.empty_notices === (changes ? 0 : 1)
    && view.rows.length === changes && view.other.length === 0;
}

// One selection of two committed versions of the order lines, in a page session
// that stays open: the last is made while the third version is the head.
async function captureSchemaGroup(selection, ordinal, asked) {
  const { name, pins, expected } = selection;
  const dataset = ready.schema.dataset;
  if (ordinal === 0) await openCompareTab(`${base}/datasets/${dataset}/branches/${pins.A.branch}`);
  const selections = { A: `transaction:${pins.A.transaction_id}`, B: `transaction:${pins.B.transaction_id}` };
  // Stamped before any selection of this step: a request the step causes starts after it.
  const step_started_unix_ms = await page.evaluate(() => performance.timeOrigin + performance.now());
  await selectSides(selections);
  await page.waitForFunction(({ dataset, pins, since }) => {
    // The diff panel and the loading notice replace each other: once the panel
    // is drawn, both sides answered and neither is unavailable.
    const drawn = [...document.querySelectorAll(".of-panel")].some((panel) =>
      panel.querySelector(".of-eyebrow")?.textContent.trim() === "Schema diff" && panel.querySelector("li"));
    const answered = (pin) => performance.getEntriesByType("resource").some((entry) => {
      const address = new URL(entry.name);
      return entry.initiatorType === "fetch" && address.origin === location.origin
        && address.pathname === `/api/v1/datasets/${dataset}/schema`
        && address.searchParams.get("branch") === pin.branch && address.searchParams.get("version") === String(pin.version)
        && performance.timeOrigin + entry.startTime >= since && entry.responseEnd > 0;
    });
    return drawn && pins.every(answered);
  }, { dataset, pins: asked.map((side) => pins[side]), since: step_started_unix_ms }, { timeout: load });
  const view = await readSchemaComparison(dataset, pins.A, pins.B);
  Object.assign(view, { step_started_unix_ms });
  assert.equal(view.panel, true);
  // The case states the changes. The panel is counted item by item against them.
  assert.ok(statesChanges(view, expected));
  assert.deepEqual(view.rows, expected);
  // The same reading must know each statement alone, and must refuse a change
  // next to the empty notice, next to another sentence, or a notice next to one,
  // whichever statement it is held against.
  const [changeWithNotice, changeWithOther, change, notice, noticeWithOther] = view.control;
  assert.ok(statesChanges(change, change.rows));
  assert.ok(statesChanges(notice, []));
  for (const mixed of [changeWithNotice, changeWithOther, noticeWithOther]) {
    for (const stated of [[], change.rows]) assert.equal(statesChanges(mixed, stated), false);
  }
  assert.deepEqual(view.alerts, []);
  assert.equal(view.unavailable_labels, 0);
  assert.equal(view.schema_notices, 0);
  for (const [side, pin] of [["A", pins.A], ["B", pins.B]]) {
    assert.equal(view.selectors[side]?.value, selections[side]);
    assert.equal(view.selectors[side]?.label, `v${pin.version} · ${pin.branch} · ${pin.transaction_id.slice(0, 8)}`);
    assert.ok(view.sides[side].fields > 0);
    assert.equal(view.requests[side]?.status, 200);
    assert.ok(view.requests[side].transferred_bytes > 0);
  }
  const kept = await keepCapture(`compare-schema-${name}`, () => page.evaluate(() => ({
    url: location.href,
    compare: [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare schemas")?.closest("section")?.outerHTML,
  })));
  schema[name] = { ...view, ...kept };
}

await page.goto(base + "/login", { timeout: load });
await page.evaluate((jwt) => localStorage.setItem("of_access_token", jwt), token);

const groups = {};
const schema = {};

async function captureGroup(group) {
  const url = `${base}/datasets/${ready.dataset}/branches/${group.base.branch}`;
  const selections = { A: `transaction:${group.base.transaction_id}`, B: `transaction:${group.target.transaction_id}` };
  await openRowComparison(url, selections);
  await page.fill(rowKeyInput, ready.key);
  await page.click(compareRows);
  await page.waitForFunction(() => {
    const section = [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare rows by key")?.closest("section");
    return section?.querySelector('[role="alert"]')
      || /\d+ added · \d+ removed · \d+ modified · \d+ unchanged/.test(section?.querySelector('[role="status"]')?.textContent ?? "");
  }, undefined, { timeout: load });
  const view = await readRowComparison(ready.dataset, group.base, group.target, ready.key, ready.limit);
  assert.equal(view.key_column, ready.key);
  assert.equal(view.summary, group.summary);
  assert.deepEqual(view.rows, group.rows);
  assert.equal(view.table, group.rows.length > 0);
  assert.equal(view.error, null);
  // A version without a schema preview stays selected and readable: no alert, no "unavailable".
  assert.deepEqual(view.alerts, []);
  assert.equal(view.unavailable_labels, 0);
  assert.equal(view.schema_notices, 2);
  assert.equal(view.request?.status, 200);
  assert.ok(view.request.transferred_bytes > 0);
  for (const [side, pin] of [["A", group.base], ["B", group.target]]) {
    assert.equal(view.selectors[side].value, selections[side]);
    assert.equal(view.selectors[side].label, `v${pin.version} · ${pin.branch} · ${pin.transaction_id.slice(0, 8)}`);
  }
  const masked = await maskIdentity(ready.anonymize);
  const capture = `compare-${group.name}.png`;
  await page.screenshot({ path: input.state + "/" + capture, fullPage: true });
  const dom = await page.evaluate(() => ({
    url: location.href,
    compare: [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare rows by key")?.closest("section")?.parentElement.outerHTML,
  }));
  const captured_at_unix_ms = Date.now();
  await fs.writeFile(input.state + `/compare-${group.name}.dom.html`, [
    "<!doctype html>",
    "<!-- Anonymized capture of the Compare panel. Identity labels replaced: " + masked.length + " -->",
    `<html><head><meta charset="utf-8"><title>${dom.url}</title></head><body>`,
    dom.compare,
    "</body></html>",
  ].join("\n") + "\n", { mode: 0o600 });
  groups[group.name] = { ...view, capture, dom: `compare-${group.name}.dom.html`, masked_identity_labels: masked,
    captured_at_unix_ms, captured_at: new Date(captured_at_unix_ms).toISOString() };
}

// What the page showed and asked when a capture fails: paths and statuses, never headers.
async function noteFailure(name) {
  const seen = await page.evaluate(() => performance.getEntriesByType("resource")
    .filter((entry) => entry.initiatorType === "fetch")
    .map((entry) => ({ path: new URL(entry.name).pathname, status: entry.responseStatus }))
    .filter((entry) => entry.path.startsWith("/api/")));
  const notices = await page.evaluate(() => [...document.querySelectorAll('[role="alert"], [role="status"]')]
    .map((node) => node.textContent.trim().slice(0, 300)));
  await fs.writeFile(input.state + "/preview-failure.json", JSON.stringify({ group: name, notices, seen }, null, 2) + "\n",
    { mode: 0o600 });
}

for (const group of ready.groups) {
  try {
    await captureGroup(group);
  } catch (error) {
    await noteFailure(group.name);
    throw error;
  }
}
// A side selected afresh asks for its schema again: both sides of the first selection.
let earlier = {};
for (const [ordinal, group] of ready.schema.groups.entries()) {
  const pins = { A: group.base, B: group.target };
  try {
    await captureSchemaGroup({ name: group.name, pins, expected: group.expected }, ordinal,
      Object.keys(pins).filter((side) => earlier[side] !== pins[side].transaction_id));
  } catch (error) {
    await noteFailure(group.name);
    throw error;
  }
  earlier = { A: pins.A.transaction_id, B: pins.B.transaction_id };
}
await fs.writeFile(input.state + "/browser.json", JSON.stringify({
  status: "passed", space_id: task.spaceId, groups, schema, browser: await page.evaluate(() => navigator.userAgent),
}, null, 2) + "\n", { mode: 0o600 });
console.log("PASS page compares v1 with v2 and v1 with v3 by item number behind the real gateway");
console.log("PASS page compares the schemas of order line v1 with v2, v2 with v3 and v1 with v2 again behind the real gateway");
