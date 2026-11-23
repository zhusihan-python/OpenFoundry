// Run by ego-browser nodejs with the disposable stack's input prepended.
const fs = await import("node:fs/promises");
const assert = (await import("node:assert/strict")).default;
const { setTimeout: delay } = await import("node:timers/promises");
const token = (await fs.readFile(input.token_file, "utf8")).trim();
const base = "http://127.0.0.1:" + input.web_port;
const task = await taskSpace("OpenFoundry WP8.3 order version comparison");
await fs.writeFile(input.envdir + "/browser-space", String(task.spaceId), { mode: 0o600 });
const page = task.page("p1");
await page.goto(base + "/login");
await page.evaluate((jwt) => localStorage.setItem("of_access_token", jwt), token);

async function writeMarker(name, value) {
  const path = input.envdir + "/" + name;
  await fs.writeFile(path + ".tmp", JSON.stringify(value) + "\n", { mode: 0o600 });
  await fs.rename(path + ".tmp", path);
}

async function waitForMarker(name, timeout) {
  const deadline = Date.now() + timeout;
  do {
    try {
      return JSON.parse(await fs.readFile(input.envdir + "/" + name, "utf8"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    await delay(75);
  } while (Date.now() < deadline);
  throw new Error(`timed out waiting for ${name}`);
}

async function captureScreenshot(capture) {
  await page.screenshot({ path: input.evidence + "/" + capture });
  const captured_at_unix_ms = Date.now();
  return { capture, captured_at_unix_ms, captured_at: new Date(captured_at_unix_ms).toISOString() };
}

// These values are independent of the component's diff calculation.
const expectedSchemaDiff = [
  ["+", "added_field", "BOOLEAN"],
  ["−", "legacy_field", "STRING"],
  ["~", "amount", "LONG → STRING"],
];

async function captureSchemaDiff() {
  const { old, next, observed_unix_ms } = await waitForMarker("schema-diff-ready.json", 45000);
  const url = `${base}/datasets/${old.dataset}/branches/main`;
  const values = { a: `transaction:${old.transaction}`, b: `transaction:${next.transaction}` };
  const readComparison = ({ pins, error }) => {
    const comparison = [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare schemas")?.closest("section");
    if (!comparison) return null;
    const controls = [...comparison.querySelectorAll("select")];
    const selects = Object.fromEntries(["A", "B"].map((side) => {
      const name = `Side ${side}`;
      const control = controls.find((select) => select.getAttribute("aria-label") === name
        || [...select.labels].some((label) => label.textContent.trim() === name));
      return [side.toLowerCase(), control ? {
        value: control.value,
        options: [...control.options].map((option) => option.value),
      } : null];
    }));
    const panel = [...comparison.querySelectorAll("p, h2, h3")]
      .find((node) => node.textContent.trim() === "Schema diff")?.parentElement;
    const rows = panel ? [...panel.querySelectorAll("li")]
      .map((row) => [...row.children].map((span) => span.textContent.trim())) : null;
    const alert = error ? [...comparison.querySelectorAll('[role="alert"]')]
      .find((node) => node.getClientRects().length > 0 && node.textContent.includes("Side A unavailable")
        && node.textContent.includes(error) && !node.textContent.includes("Side B unavailable")) : null;
    const requests = Object.fromEntries(Object.entries(pins).map(([side, pin]) => {
      const path = `/api/v1/datasets/${pin.dataset}/schema`;
      const request = performance.getEntriesByType("resource").reverse().find((entry) => {
        const address = new URL(entry.name);
        return entry.initiatorType === "fetch" && address.origin === location.origin && address.pathname === path
          && address.searchParams.get("version") === String(pin.version)
          && address.searchParams.get("branch") === pin.branch;
      });
      return [side, request ? {
        path, version: pin.version, branch: pin.branch, status: request.responseStatus,
        started_unix_ms: performance.timeOrigin + request.startTime,
        completed_unix_ms: performance.timeOrigin + request.responseEnd,
        transferred_bytes: request.transferSize,
      } : null];
    }));
    return { selects, rows, requests, alert: alert ? alert.textContent.trim() : null,
      navigation_unix_ms: performance.timeOrigin };
  };

  async function capturePhase(name, observed, tip = null, error = null) {
    await page.goto(url);
    await page.click('loc=role:tab[name="Compare"]');
    for (const [side, value] of Object.entries(values)) {
      const label = `Side ${side.toUpperCase()}`;
      const locator = `loc=role:combobox[name="${label}"]`;
      await page.waitForSelector(locator, { timeout: 20000 });
      await page.waitForFunction((option) => [...document.querySelectorAll("select option")]
        .some((node) => node.value === option), value, { timeout: 20000 });
      await page.selectOption(locator, value);
    }
    await page.waitForFunction(({ error, diff }) => {
      const comparison = [...document.querySelectorAll("h2")]
        .find((node) => node.textContent.trim() === "Compare schemas")?.closest("section");
      if (!comparison) return false;
      const panel = [...comparison.querySelectorAll("p, h2, h3")]
        .find((node) => node.textContent.trim() === "Schema diff")?.parentElement;
      if (error) return !panel && comparison.textContent.includes("Side A unavailable")
        && comparison.textContent.includes(error) && !comparison.textContent.includes("Side B unavailable")
        && [...comparison.querySelectorAll(".of-text-muted")].some((node) => node.textContent.trim() === "3 fields");
      const rows = panel && [...panel.querySelectorAll("li")]
        .map((row) => [...row.children].map((span) => span.textContent.trim()));
      return JSON.stringify(rows) === JSON.stringify(diff);
    }, { error, diff: expectedSchemaDiff }, { timeout: 20000 });
    const view = await page.evaluate(readComparison, { pins: { a: old, b: next }, error });
    assert.ok(view.navigation_unix_ms >= observed);
    for (const [side, value] of Object.entries(values)) {
      assert.equal(view.selects[side].value, value);
      assert.ok(view.selects[side].options.includes(values.a) && view.selects[side].options.includes(values.b));
      if (tip) assert.ok(view.selects[side].options.includes(`transaction:${tip.transaction}`));
      assert.equal(view.requests[side]?.status, error && side === "a" ? 422 : 200);
      assert.ok(view.requests[side].transferred_bytes > 0);
    }
    if (error) {
      assert.equal(view.rows, null);
      assert.ok(view.alert.includes("Side A unavailable") && view.alert.includes(error));
    } else {
      assert.deepEqual(view.rows, expectedSchemaDiff);
      assert.equal(view.alert, null);
    }
    return { ...view, ...await captureScreenshot(name) };
  }

  const before = await capturePhase("schema-diff-before.png", observed_unix_ms);
  await writeMarker("schema-diff-compared.json", { captured_at_unix_ms: before.captured_at_unix_ms });
  const head = await waitForMarker("schema-diff-head.json", 45000);
  const after_head = await capturePhase("schema-diff-after-head.png", head.observed_unix_ms, head.tip);
  await writeMarker("schema-diff-rechecked.json", { captured_at_unix_ms: after_head.captured_at_unix_ms });
  const missingFile = await waitForMarker("schema-diff-missing.json", 45000);
  const missing = await capturePhase("schema-diff-unavailable.png", missingFile.observed_unix_ms, head.tip, missingFile.error);
  await writeMarker("schema-diff-observed.json", { captured_at_unix_ms: missing.captured_at_unix_ms });
  const restoredFile = await waitForMarker("schema-diff-restored.json", 45000);
  const restored = await capturePhase("schema-diff-restored.png", restoredFile.observed_unix_ms, head.tip);
  return { before, after_head, missing, restored };
}

const rowKeyInput = 'loc=role:textbox[name="Key column"]';
const compareRows = 'loc=role:button[name="Compare rows"]';
const rowChangesTable = 'loc=role:table[name="Row changes"]';

async function openRowComparison(url, selections) {
  await page.goto(url);
  await page.click('loc=role:tab[name="Compare"]');
  for (const [side, value] of Object.entries(selections)) {
    const locator = `loc=role:combobox[name="Side ${side}"]`;
    await page.waitForSelector(locator, { timeout: 20000 });
    await page.waitForFunction((option) => [...document.querySelectorAll("select option")]
      .some((node) => node.value === option), value, { timeout: 20000 });
    await page.selectOption(locator, value);
  }
  await page.waitForSelector(rowKeyInput, { timeout: 20000 });
}

function comparisonSummary(counts) {
  return ["added", "removed", "modified", "unchanged"]
    .map((kind) => `${counts[kind]} ${kind}`).join(" · ");
}

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
    const navigation = section?.querySelector('nav[aria-label="Row change pages"]');
    const rows = [...(table?.tBodies[0]?.rows ?? [])].map((row) => {
      const [change, key, before, after] = [...row.cells].map((cell) => cell.textContent.trim());
      return { key, kind: change.toLowerCase(),
        ...(before !== "—" && { before: JSON.parse(before) }),
        ...(after !== "—" && { after: JSON.parse(after) }) };
    });
    return {
      selectors, rows, table: !!table,
      page_number: Number(navigation?.textContent.match(/Page (\d+)/)?.[1] ?? 1),
      previous_enabled: [...(navigation?.querySelectorAll("button") ?? [])]
        .some((button) => button.textContent.trim() === "Previous page" && !button.disabled),
      next_enabled: [...(navigation?.querySelectorAll("button") ?? [])]
        .some((button) => button.textContent.trim() === "Next page" && !button.disabled),
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

async function captureRowMatch() {
  const { expected, observed_unix_ms } = await waitForMarker("row-match-ready.json", 45000);
  const { base: older, target: newer, dataset_id: dataset } = expected;
  const url = `${base}/datasets/${dataset}/branches/main`;
  const selections = { A: `transaction:${older.transaction_id}`, B: `transaction:${newer.transaction_id}` };
  const summary = comparisonSummary(expected.counts);

  async function captureValid(name, readyAt, tip = null) {
    await openRowComparison(url, selections);
    await page.fill(rowKeyInput, expected.key);
    await page.click(compareRows);
    await page.waitForSelector(rowChangesTable, { timeout: 20000 });
    const view = await readRowComparison(dataset, older, newer, expected.key, 50);
    assert.equal(view.summary, summary);
    assert.deepEqual(view.rows, expected.changes);
    assert.equal(view.error, null);
    assert.equal(view.request.status, 200);
    assert.ok(view.request.transferred_bytes > 0);
    assert.ok(view.navigation_unix_ms >= readyAt);
    for (const side of ["A", "B"]) {
      assert.equal(view.selectors[side].value, selections[side]);
      if (tip) assert.ok(view.selectors[side].options.includes(`transaction:${tip.transaction_id}`));
    }
    return { ...view, ...await captureScreenshot(name) };
  }

  const before = await captureValid("row-match-before.png", observed_unix_ms);
  await writeMarker("row-match-compared.json", { captured_at_unix_ms: before.captured_at_unix_ms });
  const head = await waitForMarker("row-match-head.json", 45000);
  const after_head = await captureValid("row-match-after-head.png", head.observed_unix_ms, head.tip);
  await writeMarker("row-match-rechecked.json", { captured_at_unix_ms: after_head.captured_at_unix_ms });

  await page.fill(rowKeyInput, "wrong_key");
  await page.waitForFunction(() => !document.querySelector('table[aria-label="Row changes"]'),
    undefined, { timeout: 20000 });
  await page.click(compareRows);
  await page.waitForFunction(() => {
    const section = [...document.querySelectorAll("h2")]
      .find((node) => node.textContent.trim() === "Compare rows by key")?.closest("section");
    const alert = section?.querySelector('[role="alert"]');
    return alert?.textContent.includes("dataset_row_compare_key_invalid");
  }, undefined, { timeout: 20000 });
  const rejected = await readRowComparison(dataset, older, newer, "wrong_key", 50);
  assert.equal(rejected.request.status, 400);
  assert.ok(rejected.request.transferred_bytes > 0);
  assert.equal(rejected.table, false);
  assert.equal(rejected.summary, null);
  assert.deepEqual(rejected.rows, []);
  const invalid_key = { ...rejected, ...await captureScreenshot("row-match-invalid-key.png") };
  return { before, after_head, invalid_key };
}

async function captureRangeDiff() {
  const { expected, limit, observed_unix_ms } = await waitForMarker("range-diff-ready.json", 90000);
  const { base: older, target: newer, dataset_id: dataset, key, changes } = expected;
  const url = `${base}/datasets/${dataset}/branches/main`;
  const selections = { A: `transaction:${older.transaction_id}`, B: `transaction:${newer.transaction_id}` };
  const summary = comparisonSummary(expected.counts);
  const previous = 'loc=role:button[name="Previous page"]';
  const next = 'loc=role:button[name="Next page"]';
  const totalPages = Math.ceil(changes.length / limit);
  await openRowComparison(url, selections);
  await page.fill(rowKeyInput, key);

  async function move(button, number, screenshot = null) {
    const sentAfter = Date.now();
    await page.click(button);
    await page.waitForFunction(({ number, firstKey }) => {
      const table = document.querySelector('table[aria-label="Row changes"]');
      const navigation = document.querySelector('nav[aria-label="Row change pages"]');
      return Number(navigation?.textContent.match(/Page (\d+)/)?.[1]) === number
        && table?.tBodies[0]?.rows[0]?.cells[1]?.textContent.trim() === firstKey;
    }, { number, firstKey: changes[(number - 1) * limit].key }, { timeout: 20000 });
    const view = await readRowComparison(dataset, older, newer, key, limit);
    assert.equal(view.page_number, number);
    assert.equal(view.summary, summary);
    assert.deepEqual(view.rows, changes.slice((number - 1) * limit, number * limit));
    assert.equal(view.error, null);
    assert.equal(view.previous_enabled, number > 1);
    assert.equal(view.next_enabled, number < totalPages);
    assert.equal(view.request.status, 200);
    assert.ok(view.request.transferred_bytes > 0 && view.request.started_unix_ms >= sentAfter);
    assert.equal(Boolean(view.request.params.cursor), number > 1);
    for (const side of ["A", "B"]) assert.equal(view.selectors[side].value, selections[side]);
    if (screenshot) {
      return { ...view, action_unix_ms: sentAfter, ...await captureScreenshot(screenshot) };
    }
    return { ...view, action_unix_ms: sentAfter, captured_at_unix_ms: Date.now() };
  }

  const first = await move(compareRows, 1, "range-diff-first.png");
  assert.ok(first.navigation_unix_ms >= observed_unix_ms);
  await writeMarker("range-diff-first.json", { captured_at_unix_ms: first.captured_at_unix_ms });
  const head = await waitForMarker("range-diff-head.json", 45000);
  assert.equal(head.tip.version, 3);
  const pages = [first];
  for (let number = 2; number <= totalPages; number++) {
    const view = await move(next, number, number === totalPages ? "range-diff-last.png" : null);
    assert.equal(view.navigation_unix_ms, first.navigation_unix_ms);
    assert.ok(view.request.started_unix_ms >= head.observed_unix_ms);
    pages.push(view);
  }
  assert.deepEqual(pages.flatMap((view) => view.rows), changes);
  const previousPage = await move(previous, totalPages - 1, "range-diff-previous.png");
  assert.equal(previousPage.navigation_unix_ms, first.navigation_unix_ms);
  assert.equal(previousPage.request.params.cursor, pages.at(-2).request.params.cursor);
  await writeMarker("range-diff-traversed.json", { captured_at_unix_ms: previousPage.captured_at_unix_ms });
  return { pages, previous: previousPage };
}

const schema_diff = await captureSchemaDiff();
const row_match = await captureRowMatch();
const range_diff = await captureRangeDiff();
await fs.writeFile(input.evidence + "/browser.json", JSON.stringify({
  status: "passed", space_id: task.spaceId, schema_diff, row_match, range_diff,
  browser: await page.evaluate(() => navigator.userAgent),
}, null, 2) + "\n");
console.log("PASS historical Schema Diff before and after head advance, missing and restored Parquet object");
console.log("PASS historical row matching across Parquet files and pages, stable after head advance, and visible unknown-key error");
console.log("PASS historical Range Diff traverses all rows with no reload after head advance and reverses a page");
