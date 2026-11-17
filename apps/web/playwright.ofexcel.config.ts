import { defineConfig } from '@playwright/test';
import base from './playwright.p31-real.config';

// PR3 acceptance-runner overlay (tools/smoke/dataset-excel/run.sh): same
// suite (testMatch from the base config) and same test-only vite server
// (vite.p31-e2e.config.ts, identity-stubbed), but:
//   * reuseExistingServer: false — the runner owns the whole server
//     lifecycle on a fresh random port; a stale externally-started vite
//     can never satisfy (or poison) an acceptance run;
//   * trace retained only on failure and screenshots only on failure —
//     passing runs leave minimal artifacts.
// The manual p31 review config (playwright.p31-real.config.ts) is untouched
// and keeps reuseExistingServer: true for interactive use.
export default defineConfig({
  ...base,
  use: {
    ...base.use,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: {
    ...base.webServer,
    reuseExistingServer: false,
  },
});
