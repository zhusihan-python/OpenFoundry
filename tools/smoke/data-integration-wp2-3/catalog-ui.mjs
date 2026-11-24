// Input is supplied by catalog-ui.sh for a fresh synthetic tenant.
const fs = await import("node:fs/promises");
const report = JSON.parse(await fs.readFile(input.state + "/orders-report.json", "utf8"));
const task = await taskSpace("OpenFoundry order acceptance " + report.source_id);
const page = task.page("p1");
await fs.writeFile(input.state + "/browser-space", String(task.spaceId));
const token = (await fs.readFile(input.state + "/browser.jwt", "utf8")).trim();
await page.goto(input.origin + "/login");
await page.evaluate((jwt) => localStorage.setItem("of_access_token", jwt), token);
await page.goto(input.origin + "/data-connection/new");
const postgres = report.connector === "postgresql";
const cardsReady = (connector) => {
  const cards = [...document.querySelectorAll("li button")];
  const matching = (name) => cards.filter(el => el.querySelector("strong")?.textContent === name);
  return matching("Amazon S3").some(el => el.textContent.includes("Batch sync")) &&
    ["PostgreSQL","MySQL"].every(name => matching(name).length > 0 && matching(name).every(el => el.textContent.includes("Batch sync") === (name.toLowerCase() === connector)));
};
await page.waitForFunction(cardsReady, report.connector, { timeout: 30000 });
await page.goto(input.origin + "/data-connection/sources/" + report.source_id);
await page.waitForFunction(() => [...document.querySelectorAll("button")].some(el => el.textContent.trim().toLowerCase() === "capabilities"));
await page.evaluate(() => [...document.querySelectorAll("button")].find(el => el.textContent.trim().toLowerCase() === "capabilities").click());
await page.waitForFunction(() => {
  const badges = [...document.querySelectorAll("section span")].map(el => el.textContent);
  return badges.includes("Batch sync") && !badges.includes("Exploration") && !badges.includes("Virtual table");
});
await page.waitForFunction((source) => performance.getEntriesByType("resource")
  .some(entry => new URL(entry.name).pathname.endsWith("/sources/" + source + "/capabilities")),
  report.source_id, { timeout: 30000 });
const requests = await page.evaluate(() => performance.getEntriesByType("resource")
  .map(item => new URL(item.name).pathname).filter(path => path.endsWith("/catalog") || path.endsWith("/capabilities")));
if (!requests.some(path => path.includes(report.source_id))) throw new Error("No source-specific capability request");
await fs.writeFile(input.state + "/browser-checks.json", JSON.stringify({
  source: report.source_id, requests, real_connector: true, identity_bootstrap: "Vite fixture",
  gallery_s3_batch: true, gallery_postgres_batch: postgres, gallery_mysql_batch: report.connector === "mysql", configured_source_batch: true, configured_source_exploration: false,
}, null, 2));
