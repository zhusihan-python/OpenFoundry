// Input is supplied by preview-ui.sh for the run's published Amazon reports.
const fs = await import("node:fs/promises");
const assert = (await import("node:assert/strict")).default;
const ready = JSON.parse(await fs.readFile(input.state + "/compare-ready.json", "utf8"));
const token = (await fs.readFile(input.state + "/browser.jwt", "utf8")).trim();
const base = input.origin;
const task = await taskSpace("OpenFoundry amazon report version comparison");
await fs.writeFile(input.state + "/browser-space", String(task.spaceId), { mode: 0o600 });
const page = task.page("p1");
// A cold dev server compiles the page on its first load.
const load = 90000;

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

// A function that runs in the Page is called from its source alone: it closes
// over nothing of this script, and takes one JSON value.
const inPage = (fn, argument) => page.evaluate(`(${fn})(${JSON.stringify(argument)})`);

// The page's own rendering and requests for the schemas of two committed
// versions, independent of its diff calculation.
function readSchemaComparison({ dataset, pins, since }) {
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
  const synthetic = (html) => {
    const panel = document.createElement("div");
    panel.innerHTML = `<ul>${html}</ul>`;
    return counted(panel);
  };
  const changeRow = '<li><span>~</span><span>total</span><span>DOUBLE → STRING</span></li>';
  const notice = "<li>No schema changes.</li>";
  const own = "<li>total is now a string</li>";
  const controls = [synthetic(changeRow + notice), synthetic(changeRow + own), synthetic(changeRow), synthetic(notice), synthetic(notice + own)];
  const selectors = {}, sides = {}, requests = {};
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
    // The request of that side's schema, asked since the step began.
    const path = `/api/v1/datasets/${dataset}/schema`;
    const params = { branch: pins[side].branch, version: String(pins[side].version) };
    const entry = performance.getEntriesByType("resource").reverse().find((candidate) => {
      const address = new URL(candidate.name);
      return candidate.initiatorType === "fetch" && address.origin === location.origin && address.pathname === path
        && Object.entries(params).every(([name, value]) => address.searchParams.get(name) === value)
        && performance.timeOrigin + candidate.startTime >= since && candidate.responseEnd > 0;
    });
    requests[side] = entry ? {
      path, params, status: entry.responseStatus,
      started_unix_ms: performance.timeOrigin + entry.startTime,
      completed_unix_ms: performance.timeOrigin + entry.responseEnd,
      transferred_bytes: entry.transferSize,
    } : null;
  }
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
    requests,
  };
}

// The dev stub's identity labels masked, then the page kept as the reader saw it.
async function keepCapture(name, section, read) {
  const masked = await maskIdentity(ready.anonymize);
  const capture = `${name}.png`;
  await page.screenshot({ path: input.state + "/" + capture, fullPage: true });
  const dom = await read();
  const captured_at_unix_ms = Date.now();
  await fs.writeFile(input.state + `/${name}.dom.html`, [
    "<!doctype html>",
    "<!-- Anonymized capture of the Compare panel. Identity labels replaced: " + masked.length + " -->",
    `<html><head><meta charset="utf-8"><title>${dom.url}</title></head><body>`,
    dom[section],
    "</body></html>",
  ].join("\n") + "\n", { mode: 0o600 });
  return { capture, dom: `${name}.dom.html`, masked_identity_labels: masked, captured_at_unix_ms,
    captured_at: new Date(captured_at_unix_ms).toISOString() };
}

const compareSection = (heading) => page.evaluate((heading) => ({
  url: location.href,
  compare: [...document.querySelectorAll("h2")].find((node) => node.textContent.trim() === heading)?.closest("section")?.outerHTML,
}), heading);

// One selection of two committed versions of the report, in a page session
// that stays open: the second is made while v4 is the head.
async function captureSchemaGroup(selection, ordinal, asked) {
  const { name, pins } = selection;
  const dataset = ready.dataset;
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
  const view = await inPage(readSchemaComparison, { dataset, pins, since: step_started_unix_ms });
  Object.assign(view, { step_started_unix_ms });
  // What the panel states is judged once, by the reader of this record: the case holds the
  // page's own panel, and the five panels a page must never show, to the same rule. Here the
  // capture is held to being the page's own, with both sides answered.
  assert.equal(view.panel, true);
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
  const kept = await keepCapture(`compare-schema-${name}`, "compare", () => compareSection("Compare schemas"));
  schema[name] = { ...view, ...kept };
}

// Output v3 located in the version list of its dataset, its file listed by the
// transaction that cut it, and that file downloaded through the page's control.
async function downloadLastGood() {
  const { dataset, branch, version, transaction_id, logical_path, size_bytes, saved_as } = ready.download;
  await openCompareTab(`${base}/datasets/${dataset}/branches/${branch}`);
  const locator = 'loc=role:combobox[name="Side B"]';
  await page.waitForSelector(locator, { timeout: load });
  const value = `transaction:${transaction_id}`;
  const label = `v${version} · ${branch} · ${transaction_id.slice(0, 8)}`;
  // The version is found in the list by its label, and only then read back as the transaction.
  await page.waitForFunction(({ value }) => [...document.querySelectorAll("select option")].some((node) => node.value === value),
    { value }, { timeout: load });
  const listed = await inPage(({ value, version }) => {
    const control = [...document.querySelectorAll("select")].find((select) =>
      [...select.labels].some((node) => node.textContent.trim() === "Side B"));
    const option = [...control.options].find((node) => node.textContent.trim().startsWith(`v${version} ·`));
    return { option: option ? { value: option.value, label: option.textContent.trim() } : null,
      versions: [...control.options].filter((node) => node.value.startsWith("transaction:")).map((node) => node.textContent.trim()) };
  }, { value, version });
  assert.deepEqual(listed.option, { value, label });
  const step_started_unix_ms = await page.evaluate(() => performance.timeOrigin + performance.now());
  await page.selectOption(locator, value);
  // The page lists the files of the selected version by its transaction, and draws them.
  await page.waitForFunction(({ dataset, transaction_id, since }) => {
    const listed = performance.getEntriesByType("resource").some((entry) => {
      const address = new URL(entry.name);
      return entry.initiatorType === "fetch" && address.origin === location.origin
        && address.pathname === `/api/v1/datasets/${dataset}/files`
        && address.searchParams.get("transaction_id") === transaction_id
        && performance.timeOrigin + entry.startTime >= since && entry.responseEnd > 0;
    });
    const panel = [...document.querySelectorAll("select")].find((select) =>
      [...select.labels].some((node) => node.textContent.trim() === "Side B"))?.closest(".of-panel");
    return listed && (panel?.querySelector('ul[aria-label="Side B files"] li') || panel?.querySelector('[role="alert"]')
      || /holds no files/.test(panel?.textContent ?? ""));
  }, { dataset, transaction_id, since: step_started_unix_ms }, { timeout: load });
  const read = () => inPage(({ dataset, transaction_id, since, logical_path, size_bytes }) => {
    const control = [...document.querySelectorAll("select")].find((select) =>
      [...select.labels].some((node) => node.textContent.trim() === "Side B"));
    if (!control) return null;
    const panel = control.closest(".of-panel");
    const files = [...(panel.querySelector('ul[aria-label="Side B files"]')?.querySelectorAll("li") ?? [])].map((item) => {
      const path = item.querySelector("code")?.textContent.trim() ?? null;
      // The page writes a size under its own locale. Its text is held against the same formatting
      // of the size the reader listed, made here in the page; nothing is parsed back from it.
      const size_shown_as = path === logical_path ? `${size_bytes.toLocaleString()} B` : null;
      const size_text = item.querySelector(".of-text-muted")?.textContent.trim() ?? null;
      return {
        path, size_text, size_shown_as, size_matches: size_shown_as === null ? null : size_text === size_shown_as,
        control: item.querySelector("button")?.getAttribute("aria-label") ?? null,
        disabled: item.querySelector("button")?.disabled ?? null,
      };
    });
    return {
      selector: { value: control.value, label: control.selectedOptions[0]?.textContent.trim() ?? null },
      files, controls: files.map((file) => file.control),
      alerts: [...panel.querySelectorAll('[role="alert"]')].map((node) => node.textContent.trim().slice(0, 300)),
      notes: [...panel.querySelectorAll(":scope > .of-text-muted")].map((node) => node.textContent.trim()),
      // The listing the page asked for this version: the files of its transaction, since the selection.
      listing_request: (() => {
        const entry = performance.getEntriesByType("resource").reverse().find((candidate) => {
          const address = new URL(candidate.name);
          return candidate.initiatorType === "fetch" && address.origin === location.origin
            && address.pathname === `/api/v1/datasets/${dataset}/files`
            && address.searchParams.get("transaction_id") === transaction_id
            && performance.timeOrigin + candidate.startTime >= since && candidate.responseEnd > 0;
        });
        if (!entry) return null;
        const address = new URL(entry.name);
        return {
          path: address.pathname, params: Object.fromEntries(address.searchParams), status: entry.responseStatus,
          started_unix_ms: performance.timeOrigin + entry.startTime,
          completed_unix_ms: performance.timeOrigin + entry.responseEnd,
          transferred_bytes: entry.transferSize,
        };
      })(),
    };
  }, { dataset, transaction_id, since: step_started_unix_ms, logical_path, size_bytes });
  const before = await read();
  assert.deepEqual(before.alerts, []);
  assert.deepEqual(before.selector, { value, label });
  // Every file of the version is listed, each under its own control; the case names the one to download.
  assert.ok(before.files.length > 0);
  assert.deepEqual(before.controls, before.files.map((file) => `Download ${file.path}`));
  assert.equal(before.controls.filter((control) => control === `Download ${logical_path}`).length, 1);
  const shown = before.files.find((file) => file.path === logical_path);
  assert.equal(shown.size_matches, true, `the page writes the size ${shown.size_text}, not ${shown.size_shown_as}`);
  const ready_capture = await keepCapture("download-v3-ready", "compare", () => compareSection("Compare schemas"));

  const clicked_unix_ms = await page.evaluate(() => performance.timeOrigin + performance.now());
  const started = page.waitForEvent("download", { timeout: 60_000 });
  await page.click(`loc=role:button[name="Download ${logical_path}"]`, { label: "download output v3 file" });
  const download = await started;
  const event = { url: download.url(), suggested_filename: download.suggestedFilename(), status: "saved" };
  await download.saveAs(`${input.state}/${saved_as}`);
  event.saved_unix_ms = Date.now();
  await page.waitForFunction(() => ![...document.querySelectorAll('ul[aria-label="Side B files"] button')]
    .some((button) => button.disabled), undefined, { timeout: load });
  const after = await read();
  assert.deepEqual(after.alerts, []);
  assert.ok(after.files.every((file) => file.disabled === false));
  // The request of the page's own control: the download route of the dataset, since the click.
  const request = await inPage(({ dataset, since }) => {
    const entry = performance.getEntriesByType("resource").reverse().find((candidate) => {
      const address = new URL(candidate.name);
      return candidate.initiatorType === "fetch" && address.origin === location.origin
        && address.pathname.startsWith(`/api/v1/datasets/${dataset}/files/`) && address.pathname.endsWith("/download")
        && performance.timeOrigin + candidate.startTime >= since && candidate.responseEnd > 0;
    });
    if (!entry) return null;
    const address = new URL(entry.name);
    return {
      path: address.pathname, params: Object.fromEntries(address.searchParams), status: entry.responseStatus,
      started_unix_ms: performance.timeOrigin + entry.startTime,
      completed_unix_ms: performance.timeOrigin + entry.responseEnd,
      transferred_bytes: entry.transferSize,
    };
  }, { dataset, since: clicked_unix_ms });
  const saved_capture = await keepCapture("download-v3-saved", "compare", () => compareSection("Compare schemas"));
  return { saved_as, version, option: listed.option, versions: listed.versions, selector: after.selector, files: after.files,
    controls: after.controls, listing_request: before.listing_request, request, clicked_unix_ms, event,
    captures: { ready: ready_capture, saved: saved_capture } };
}

// What the page showed and asked when a capture fails: paths and statuses, never headers.
async function noteFailure(step) {
  const seen = await page.evaluate(() => performance.getEntriesByType("resource")
    .filter((entry) => entry.initiatorType === "fetch")
    .map((entry) => ({ path: new URL(entry.name).pathname, status: entry.responseStatus }))
    .filter((entry) => entry.path.startsWith("/api/")));
  const notices = await page.evaluate(() => [...document.querySelectorAll('[role="alert"], [role="status"]')]
    .map((node) => node.textContent.trim().slice(0, 300)));
  await fs.writeFile(input.state + "/preview-failure.json", JSON.stringify({ step, notices, seen }, null, 2) + "\n",
    { mode: 0o600 });
}

await page.goto(base + "/login", { timeout: load });
await page.evaluate((jwt) => localStorage.setItem("of_access_token", jwt), token);

const schema = {};
// A side selected afresh asks for its schema again: both sides of the first selection.
let earlier = {};
for (const [ordinal, group] of ready.groups.entries()) {
  const pins = { A: group.base, B: group.target };
  try {
    await captureSchemaGroup({ name: group.name, pins }, ordinal,
      Object.keys(pins).filter((side) => earlier[side] !== pins[side].transaction_id));
  } catch (error) {
    await noteFailure(group.name);
    throw error;
  }
  earlier = { A: pins.A.transaction_id, B: pins.B.transaction_id };
}
let download;
try {
  download = await downloadLastGood();
} catch (error) {
  await noteFailure("download");
  throw error;
}
await fs.writeFile(input.state + "/browser.json", JSON.stringify({
  status: "passed", space_id: task.spaceId, origin: base, schema, download, browser: await page.evaluate(() => navigator.userAgent),
}, null, 2) + "\n", { mode: 0o600 });
console.log("PASS page compares the schemas of report v2 with v3 and v3 with v4 behind the real gateway");
console.log("PASS page locates output v3 in its version list and the browser downloads its file there");
