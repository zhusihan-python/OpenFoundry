import { expect, test, type Page } from '@playwright/test';

const pipelineId = '11111111-1111-1111-1111-111111111111';

const pipeline = {
  id: pipelineId,
  name: 'Trail running demo',
  description: 'Preview selected nodes without saving first.',
  owner_id: '00000000-0000-0000-0000-000000000001',
  dag: [
    {
      id: 'source_trails',
      label: 'Trail source',
      transform_type: 'external',
      config: {
        rows: [
          { trail: 'Anemone', distance: 2.9, gain: 700 },
          { trail: 'Mesa Trail', distance: 6.0, gain: 900 },
        ],
      },
      output_dataset_id: null,
    },
    {
      id: 'select_trails',
      label: 'Select fast trails',
      transform_type: 'select',
      config: { columns: ['trail', 'distance'] },
      depends_on: ['source_trails'],
      output_dataset_id: null,
    },
  ],
  status: 'draft',
  schedule_config: { enabled: false, cron: null },
  retry_policy: { max_attempts: 1, retry_on_failure: false, allow_partial_reexecution: true },
  next_run_at: null,
  created_at: '2026-05-10T00:00:00Z',
  updated_at: '2026-05-10T00:00:00Z',
  pipeline_type: 'BATCH',
};

const preview = {
  pipeline_id: pipelineId,
  node_id: 'select_trails',
  columns: ['trail', 'distance'],
  rows: [{ trail: 'Mesa Trail', distance: 6.0 }],
  sample_size: 50,
  generated_at: '2026-05-10T00:00:00Z',
  seed: 1,
  source_chain: ['source_trails', 'select_trails'],
  fresh: true,
};

async function stubPipelineSession(page: Page, permissions: string[]) {
  await page.addInitScript(() => {
    window.localStorage.setItem('of_access_token', 'e2e-token');
    window.localStorage.setItem('of_refresh_token', 'e2e-refresh');
  });

  await page.route('**/api/v1/auth/bootstrap-status', async (route) => {
    await route.fulfill({ json: { requires_initial_admin: false } });
  });
  await page.route('**/api/v1/users/me', async (route) => {
    await route.fulfill({
      json: {
        id: '00000000-0000-0000-0000-000000000001',
        email: 'runner@example.com',
        name: 'Trail Runner',
        is_active: true,
        roles: ['member'],
        groups: [],
        permissions,
        organization_id: null,
        attributes: {},
        mfa_enabled: false,
        mfa_enforced: false,
        auth_source: 'local',
        created_at: '2026-05-10T00:00:00Z',
      },
    });
  });
  await page.route(`**/api/v1/pipelines/${pipelineId}`, async (route) => {
    await route.fulfill({ json: pipeline });
  });
  await page.route('**/api/v1/pipelines?**', (route) => route.fulfill({ json: { data: [pipeline], total: 1 } }));
  await page.route(`**/api/v1/pipelines/${pipelineId}/runs**`, (route) => route.fulfill({
    json: { data: [{ id: 'run-1', status: 'failed', attempt_number: 1, trigger_type: 'manual', started_at: pipeline.created_at }] },
  }));
  await page.route(`**/api/v1/pipelines/${pipelineId}/versions`, (route) => route.fulfill({
    json: { data: [{ id: 'version-1', version_number: 1, version_kind: 'published', name: pipeline.name, created_at: pipeline.created_at }] },
  }));
}
test('previews a draft DAG when PBS omits empty node arrays', async ({ page }) => {
  await stubPipelineSession(page, ['pipeline.write']);
  await page.route('**/api/v1/pipelines/_validate', async (route) => {
    await route.fulfill({
      json: {
        valid: true,
        errors: [],
        warnings: [],
        next_run_at: null,
        summary: { node_count: 2, edge_count: 1, root_node_ids: ['source_trails'], leaf_node_ids: ['select_trails'] },
      },
    });
  });
  await page.route(`**/api/v1/pipelines/${pipelineId}/nodes/select_trails/preview**`, async (route) => {
    const body = route.request().postDataJSON() as { dag?: unknown; sample_size?: number };
    expect(body.dag).toBeTruthy();
    expect(body.sample_size).toBe(50);
    await route.fulfill({ json: preview });
  });

  await page.goto(`/pipelines/${pipelineId}/edit`);
  const canvas = page.getByRole('img', { name: 'Pipeline DAG' });
  await expect(canvas.getByText('Trail source')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Save', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Publish draft' })).toBeVisible();
  await expect(canvas.locator('path[marker-end]')).toHaveCount(1);
  await canvas.getByText('Select fast trails').click();

  await expect(page.getByText('Preview')).toBeVisible();
  await expect(page.getByText('Mesa Trail')).toBeVisible();
  await expect(page.getByText('distance')).toBeVisible();
  await expect(page.getByText(/chain source_trails/)).toBeVisible();
});

test('a pipeline viewer can inspect the saved graph and runs without write requests', async ({ page }) => {
  await stubPipelineSession(page, ['pipeline.read']);
  const writes: string[] = [];
  page.on('request', (request) => {
    const path = new URL(request.url()).pathname;
    if (path.startsWith('/api/v1/') && !['GET', 'HEAD', 'OPTIONS'].includes(request.method())) {
      writes.push(`${request.method()} ${path}`);
    }
  });
  await page.route(`**/api/v1/pipelines/${pipelineId}/nodes/select_trails/preview**`, async (route) => {
    expect(route.request().method()).toBe('GET');
    expect(route.request().postData()).toBeNull();
    await route.fulfill({ json: preview });
  });

  await page.goto('/pipelines/new');
  await expect(page).toHaveURL(/\/pipelines$/);
  await expect(page.getByRole('button', { name: 'New pipeline', exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: /^(Delete|Run due|Trigger run|Retry)$/i })).toHaveCount(0);
  await page.getByRole('link', { name: pipeline.name, exact: true }).click();

  const canvas = page.getByRole('img', { name: 'Pipeline DAG' });
  await expect(canvas.locator('path[marker-end]')).toHaveCount(1);
  await expect(page.getByRole('tab', { name: /^(Nodes|Config|Validate)$/i })).toHaveCount(0);
  await canvas.getByText('Select fast trails').click();
  await expect(page.getByText('Mesa Trail')).toBeVisible();
  await expect(page.getByRole('button', {
    name: /^(Save|Undo|Redo|Publish draft|Open proposal|Deploy|Run now|Transform|Connect.*|Remove|\+ SQL)$/i,
  })).toHaveCount(0);
  const refresh = page.waitForResponse((response) => response.url().includes('/nodes/select_trails/preview'));
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  expect((await refresh).status()).toBe(200);

  await page.getByRole('tab', { name: 'History' }).click();
  await expect(page.getByText('v1', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /^(Publish draft|Restore.*)$/i })).toHaveCount(0);
  await page.getByRole('tab', { name: 'Runs' }).click();
  await expect(page.getByRole('cell', { name: 'failed' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Retry' })).toHaveCount(0);
  await page.getByRole('link', { name: 'Details', exact: true }).click();
  await expect(page.getByText('Selected run', { exact: true })).toBeVisible();
  expect(writes).toEqual([]);
});
