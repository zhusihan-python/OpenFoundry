// Run by ego-browser nodejs with the disposable stack's input prepended.
const fs = await import("node:fs/promises");
const assert = (await import("node:assert/strict")).default;
const fixture = JSON.parse(await fs.readFile(input.evidence + "/browser-fixture.json", "utf8"));
const token = (await fs.readFile(input.token_file, "utf8")).trim();
const base = "http://127.0.0.1:" + input.web_port;
const task = await taskSpace("OpenFoundry WP3.14 history verification");
await fs.writeFile(input.envdir + "/browser-space", String(task.spaceId), { mode: 0o600 });
const page = task.page("p1");
await page.goto(base + "/login");
await page.evaluate((jwt) => localStorage.setItem("of_access_token", jwt), token);

const selection = 'loc=role:combobox[name="Preview transaction"]';
const next = 'loc=role:button[name="Next page"]';
const readTable = () => {
  const table = [...document.querySelectorAll("table")].find((item) =>
    [...item.querySelectorAll("th > div:first-child")].some((head) => head.textContent.trim() === "order_id"),
  );
  if (!table) return null;
  const headers = [...table.querySelectorAll("th > div:first-child")].map((th) => th.textContent.trim());
  const columnIndex = headers.indexOf("order_id") + 1; // The preview includes a row-number column.
  return {
    headers,
    ids: [...table.querySelectorAll("tbody tr")].flatMap((row) =>
      row.cells.length > columnIndex ? [row.cells[columnIndex].textContent.trim()] : [],
    ),
    transaction: new URL(location.href).searchParams.get("txn"),
  };
};
const waitForOrder = (order) => page.waitForFunction(
  (order) => [...document.querySelectorAll("td")].some((cell) => cell.textContent.trim() === order),
  order, { timeout: 20000 },
);

await page.goto(base + "/datasets/" + fixture.old.dataset);
await page.waitForSelector(selection, { timeout: 20000 });
await page.waitForFunction((id) => [...document.querySelectorAll("select option")].some((option) => option.value === id),
  fixture.old.transaction, { timeout: 20000 });
await page.selectOption(selection, fixture.old.transaction);
await waitForOrder(fixture.first_order);
const first = await page.evaluate(readTable);
assert.equal(first.transaction, fixture.old.transaction);
assert.deepEqual(first.headers, fixture.columns);
assert.equal(first.ids[0], fixture.first_order);
await page.click(next, { label: "Read the next page" });
await waitForOrder(fixture.next_order);
const following = await page.evaluate(readTable);
assert.equal(following.transaction, fixture.old.transaction);
assert.equal(following.ids[0], fixture.next_order);
assert.ok(!following.ids.includes(fixture.first_order));

await page.selectOption(selection, fixture.new_head.transaction);
await waitForOrder("-555555");
await page.selectOption(selection, fixture.old.transaction);
await waitForOrder(fixture.first_order);
assert.equal((await page.evaluate(readTable)).ids[0], fixture.first_order);
await page.screenshot({ path: input.evidence + "/historical-preview.png" });

await page.goto(base + "/datasets/" + fixture.empty.dataset + "?txn=" + fixture.empty.transaction);
await page.waitForFunction(() => [...document.querySelectorAll("th > div:first-child")]
  .some((head) => head.textContent.trim() === "order_id"), undefined, { timeout: 20000 });
const empty = await page.evaluate(readTable);
assert.equal(empty.transaction, fixture.empty.transaction);
assert.deepEqual(empty.headers, fixture.columns);
assert.deepEqual(empty.ids, []);
await fs.writeFile(input.evidence + "/browser.json", JSON.stringify({
  status: "passed", space_id: task.spaceId, first, next: following, empty,
  browser: await page.evaluate(() => navigator.userAgent),
}, null, 2) + "\n");
console.log("PASS historical preview, pagination, latest selector and typed empty");
