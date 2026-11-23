import { expect, test } from '@playwright/test';

const datasetId = 'abc12345-ffff-4444-9999-123456789abc';
const master = { id: 'master-id', name: 'master', dataset_id: datasetId, is_default: true };
const release = { id: 'release-id', name: 'release', dataset_id: datasetId, parent_branch_id: master.id };
const older = {
  id: 'old-snapshot', dataset_id: datasetId, branch_id: master.id, branch_name: master.name,
  tx_type: 'SNAPSHOT', status: 'COMMITTED',
};

for (const routeId of [datasetId, datasetId.toUpperCase(), `ri.foundry.main.dataset.${datasetId}`]) {
  test(`branch comparison binds the route and resolves older versions for ${routeId}`, async ({ page }) => {
    await page.addInitScript(() => localStorage.setItem('of_access_token', 'test-token'));
    await page.route('**/api/v1/auth/bootstrap-status', (route) =>
      route.fulfill({ json: { requires_initial_admin: false } }));
    await page.route('**/api/v1/users/me', (route) =>
      route.fulfill({ json: { id: 'reader', email: 'reader@example.test', roles: [], groups: [], permissions: [], attributes: {} } }));
    const api = `**/api/v1/datasets/${datasetId}`;
    await page.route(api, (route) => route.fulfill({ json: { id: datasetId, name: 'Orders' } }));
    await page.route(api + '/branches', (route) => route.fulfill({ json: [master, release] }));
    await page.route(api + '/branches/release/markings', (route) =>
      route.fulfill({ json: { effective: [], explicit: [], inherited_from_parent: [] } }));
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
