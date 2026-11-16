import { defineConfig, devices } from '@playwright/test';

// Real-backend P3.1 workflow specs: the webServer runs the test-only
// vite.p31-e2e.config.ts on port 5195 (identity stubbed locally, everything
// else proxied to the p31 gateway). No dataset HTTP is mocked. The suite is
// additionally gated by P31_REAL_E2E=1 inside the spec; see
// e2e/fixtures/README.md for all P31_* overrides (loopback defaults).
const WEB_PORT = process.env.P31_WEB_PORT ?? '5195';

export default defineConfig({
  testDir: './e2e',
  testMatch: /dataset-excel-real\.spec\.ts/,
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: `http://127.0.0.1:${WEB_PORT}`,
    trace: 'on-first-retry',
  },
  webServer: {
    command: 'pnpm exec vite --config vite.p31-e2e.config.ts',
    url: `http://127.0.0.1:${WEB_PORT}`,
    // The server is kept running for Root's ego-browser review; tests reuse it.
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
