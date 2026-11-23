// Input is supplied by catalog-ui.sh for a fresh synthetic tenant.
const fs = await import("node:fs/promises");
const task = await taskSpace("OpenFoundry S3 order acceptance");
const page = task.page("p1");
await fs.writeFile(input.state + "/browser-space", String(task.spaceId));
const token = (await fs.readFile(input.state + "/browser.jwt", "utf8")).trim();
const report = JSON.parse(await fs.readFile(input.state + "/orders-report.json", "utf8"));
await page.goto(input.origin + "/login");
await page.evaluate((jwt) => localStorage.setItem("of_access_token", jwt), token);
await page.goto(input.origin + "/data-connection/new");
const cardsReady = () => {
  const cards = [...document.querySelectorAll("li button")];
  const matching = (name) => cards.filter(el => el.querySelector("strong")?.textContent === name);
  return matching("Amazon S3").some(el => el.textContent.includes("Batch sync")) &&
    matching("PostgreSQL").length > 0 && matching("PostgreSQL").every(el => !el.textContent.includes("Batch sync"));
};
await page.waitForFunction(cardsReady, undefined, { timeout: 30000 });
await page.goto(input.origin + "/data-connection/sources/" + report.source_id);
await page.waitForFunction(() => [...document.querySelectorAll("button")].some(el => el.textContent.trim().toLowerCase() === "capabilities"));
await page.evaluate(() => [...document.querySelectorAll("button")].find(el => el.textContent.trim().toLowerCase() === "capabilities").click());
await page.waitForFunction(() => [...document.querySelectorAll("section span")].some(el => el.textContent === "Batch sync"));
const requests = await page.evaluate(() => performance.getEntriesByType("resource")
  .map(item => new URL(item.name).pathname).filter(path => path.endsWith("/catalog") || path.endsWith("/capabilities")));
if (!requests.some(path => path.includes(report.source_id))) throw new Error("No source-specific capability request");
await fs.writeFile(input.state + "/browser-checks.json", JSON.stringify({
  source: report.source_id, requests, real_connector: true, identity_bootstrap: "Vite fixture",
  gallery_s3_batch: true, gallery_postgres_batch: false, configured_source_batch: true,
}, null, 2));
