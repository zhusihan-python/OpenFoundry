import { defineConfig, devices } from '@playwright/test';

// P3.1 Excel import UI specs run on a dedicated Vite port so they never
// contend with a developer's dev server on 5174. Scope is limited to the
// dataset-excel-import spec via testMatch.
export default defineConfig({
  testDir: './e2e',
  testMatch: /dataset-excel-import\.spec\.ts/,
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: 'http://127.0.0.1:5194',
    trace: 'on-first-retry',
  },
  webServer: {
    // No `--` separator: pnpm forwards these flags straight to vite (verified
    // against the installed vite CLI; the separator form made Playwright's
    // readiness probe time out).
    command: 'pnpm dev --host 127.0.0.1 --port 5194 --strictPort',
    url: 'http://127.0.0.1:5194',
    reuseExistingServer: true,
    timeout: 120_000,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
