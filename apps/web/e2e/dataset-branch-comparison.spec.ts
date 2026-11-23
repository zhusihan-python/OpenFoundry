import { expect, test } from '@playwright/test';

const datasetId = 'abc12345-ffff-4444-9999-123456789abc';
const master = { id: 'master-id', name: 'master', dataset_id: datasetId, is_default: true };
const release = { id: 'release-id', name: 'release', dataset_id: datasetId, parent_branch_id: master.id };
const older = {
  id: 'old-snapshot', dataset_id: datasetId, branch_id: master.id, branch_name: master.name,
  tx_type: 'SNAPSHOT', status: 'COMMITTED',
};

const api = `**/api/v1/datasets/${datasetId}`;

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('of_access_token', 'test-token'));
  await page.route('**/api/v1/auth/bootstrap-status', (route) =>
    route.fulfill({ json: { requires_initial_admin: false } }));
  await page.route('**/api/v1/users/me', (route) =>
    route.fulfill({ json: { id: 'reader', email: 'reader@example.test', roles: [], groups: [], permissions: [], attributes: {} } }));
  await page.route(api, (route) => route.fulfill({ json: { id: datasetId, name: 'Orders' } }));
  await page.route(api + '/branches', (route) => route.fulfill({ json: [master, release] }));
  await page.route(api + '/branches/release/markings', (route) =>
    route.fulfill({ json: { effective: [], explicit: [], inherited_from_parent: [] } }));
});

for (const routeId of [datasetId, datasetId.toUpperCase(), `ri.foundry.main.dataset.${datasetId}`]) {
  test(`branch comparison binds the route and resolves older versions for ${routeId}`, async ({ page }) => {
    const recent = Array.from({ length: 200 }, (_, i) => ({ ...older, id: `recent-${i}`, tx_type: 'APPEND' }));
    await page.route(api + '/transactions', (route) => route.fulfill({ json: { data: recent } }));
    const batch = [
      older, { ...older, id: 'append', tx_type: 'APPEND' }, { ...older, id: 'open', status: 'OPEN' },
      { ...older, id: 'missing-branch', branch_name: '' }, { ...older, id: 'recreated-branch', branch_id: 'retired-id' },
    ];
    await page.route(api + '/versions', (route) => route.fulfill({ json: { data: [
      ...batch.map((tx, i) => ({ version: i + 1, transaction_id: tx.id, dataset_id: datasetId })),
      { version: 6, transaction_id: 'foreign-version', dataset_id: 'other-dataset' },
    ] } }));
    await page.route(api + '/transactions:batchGet', (route) => {
      expect(route.request().postDataJSON().ids).toEqual(batch.map((tx) => tx.id));
      return route.fulfill({ status: 207, json: batch.map((data) => ({ id: data.id, status: 200, data })) });
    });

    let failSide = false;
    await page.route(api + '/schema?**', (route) => {
      const branch = new URL(route.request().url()).searchParams.get('branch');
      if (branch === 'master' && failSide) return route.fulfill({ status: 422, json: { error: 'Schema unreadable' } });
      return route.fulfill({ json: { fields: [] } });
    });
    await page.route(api + '/files?**', (route) => route.fulfill({ json: { files: [] } }));
    await page.route(api + '/branches/compare?**', (route) => route.fulfill({ json: {
      base_branch: 'master', compare_branch: 'release', conflicting_files: [], a_only_transactions: [], b_only_transactions: [],
    } }));

    await page.goto(`/datasets/${routeId}/branches/release`);
    await page.getByRole('tab', { name: 'Compare' }).click();
    await expect(page.getByText('Branch divergence')).toBeVisible();
    const versions = page.getByLabel('Side A').locator('option[value^="transaction:"]');
    await expect(versions).toHaveText(['v1 · master · old-snap']);

    await page.getByRole('tab', { name: 'Overview' }).click();
    failSide = true;
    await page.getByRole('tab', { name: 'Compare' }).click();
    await expect(page.getByRole('alert')).toContainText('Schema unreadable');
    await expect(page.getByText('Branch divergence')).toHaveCount(0);

    await page.route(api + '/branches', (route) => route.fulfill({ json: [
      master, { ...release, dataset_id: 'other-dataset' },
    ] }));
    await page.reload();
    await page.getByRole('tab', { name: 'Compare' }).click();
    await expect(page.getByRole('tab', { name: 'Compare' })).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByLabel('Side A')).toHaveCount(0);
  });
}

for (const initialHead of [201, 202]) {
  test(`keeps page two while reconnect refreshes ${initialHead} versions and drops invalid pins`, async ({ page }) => {
    const transaction = (version: number) => ({ ...older, id: `tx-${version}` });
    let head = initialHead;
    let rejectPin = false;
    let releaseBatch = () => {};
    const batchBarrier = new Promise<void>((resolve) => { releaseBatch = resolve; });
    await page.route(api + '/transactions', (route) => route.fulfill({
      json: { data: Array.from({ length: 200 }, (_, i) => transaction(head - i)) },
    }));
    await page.route(api + '/versions', (route) => route.fulfill({
      json: { data: Array.from({ length: head }, (_, i) => ({
        dataset_id: datasetId, version: head - i, transaction_id: transaction(head - i).id,
      })) },
    }));
    await page.route(api + '/transactions:batchGet', async (route) => {
      if (head > initialHead) await batchBarrier;
      const ids = route.request().postDataJSON().ids as string[];
      if (rejectPin && initialHead === 202) return route.fulfill({ status: 403, json: { error: 'Read denied' } });
      return route.fulfill({ status: 207, json: ids.map((id) => rejectPin && id === 'tx-1'
        ? { id, status: 404 }
        : { id, status: 200, data: { ...older, id } }) });
    });
    await page.route(api + '/schema?**', (route) => {
      const params = new URL(route.request().url()).searchParams;
      const version = Number(params.get('version'));
      return route.fulfill({ json: { dataset_id: datasetId, branch: 'master', version, transaction_id: transaction(version).id, fields: [] } });
    });
    await page.route(api + '/files?**', (route) => route.fulfill({ json: { files: [] } }));
    await page.route(api + '/branches/compare?**', (route) => route.fulfill({ json: {
      base_branch: 'master', compare_branch: 'release', conflicting_files: [], a_only_transactions: [], b_only_transactions: [],
    } }));
    await page.route(api + '/compare/rows?**', (route) => {
      const params = new URL(route.request().url()).searchParams;
      const key = params.has('cursor') ? 'second' : 'first';
      return route.fulfill({ json: {
        dataset_id: datasetId, key: 'order_id',
        base: { branch: 'master', version: 1, transaction_id: 'tx-1' },
        target: { branch: 'master', version: 2, transaction_id: 'tx-2' },
        counts: { added: 2, removed: 0, modified: 0, unchanged: 0 },
        changes: [{ key, kind: 'added', after: { order_id: key } }], changes_truncated: false,
        next_cursor: key === 'first' ? 'page-2' : undefined,
      } });
    });

    let now = Date.now();
    await page.clock.setFixedTime(now);
    await page.goto(`/datasets/${datasetId}/branches/release`);
    await page.getByRole('tab', { name: 'Compare' }).click();
    await page.getByLabel('Side A').selectOption('transaction:tx-1');
    await page.getByLabel('Side B').selectOption('transaction:tx-2');
    await page.getByLabel('Key column').fill('order_id');
    await page.getByRole('button', { name: 'Compare rows', exact: true }).click();
    await page.getByRole('button', { name: 'Next page' }).click();

    async function assertPageTwo() {
      await expect(page.getByText('Page 2', { exact: true })).toBeVisible();
      await expect(page.getByLabel('Key column')).toHaveValue('order_id');
      await expect(page.getByLabel('Side A')).toHaveValue('transaction:tx-1');
      await expect(page.getByLabel('Side B')).toHaveValue('transaction:tx-2');
      await expect(page.getByRole('cell', { name: 'second', exact: true })).toBeVisible();
    }
    async function reconnect() {
      head += 2;
      await page.evaluate(() => window.dispatchEvent(new Event('offline')));
      now += 31_000;
      await page.clock.setFixedTime(now);
      await page.evaluate(() => window.dispatchEvent(new Event('online')));
    }

    await assertPageTwo();
    const refresh = page.waitForRequest((req) => req.url().endsWith('/transactions:batchGet')
      && req.postDataJSON().ids.includes(transaction(initialHead - 198).id));
    await reconnect();
    await refresh;
    // This unselected transaction has just left the recent window. Its absence
    // proves the UI observed the pending batch before we inspect the pinned rows.
    const evicted = page.getByLabel('Side A').locator(`option[value="transaction:tx-${initialHead - 198}"]`);
    try {
      await expect(evicted).toHaveCount(0);
      await assertPageTwo();
    } finally {
      releaseBatch();
    }
    await expect(evicted).toHaveCount(1);
    await assertPageTwo();

    rejectPin = true;
    await reconnect();
    await expect(page.getByRole('table', { name: 'Row changes' })).toHaveCount(0);
    await expect(page.getByLabel('Key column')).toHaveCount(0);
  });
}
