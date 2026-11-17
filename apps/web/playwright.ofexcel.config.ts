import { defineConfig } from '@playwright/test';
import base from './playwright.p31-real.config';

// PR3 acceptance-runner overlay (tools/smoke/dataset-excel/run.sh): same
// suite (testMatch from the base config) and same test-only vite server
// (vite.p31-e2e.config.ts, identity-stubbed), but:
//   * reuseExistingServer: false — the runner owns the whole server
//     lifecycle on a fresh random port; a stale externally-started vite
//     can never satisfy (or poison) an acceptance run;
//   * trace retained only on failure and screenshots only on failure —
//     passing runs leave minimal artifacts;
//   * captureGitInfo.diff: false — on GitHub CI Playwright embeds the
//     ENTIRE PR diff into config.metadata.gitDiff of the JSON report
//     (gitCommitInfoPlugin; verified by local repro in a simulated
//     pull_request env). This PR's own sources legitimately contain
//     credential-shaped literals (the guard's grep pattern, synthetic
//     negative-control JWTs), which would trip the CI upload guard that
//     scans the report. Provenance is carried by the evidence manifest
//     instead (exact commit sha + dirty flag + image/binary hashes).
//     commit stays true (no sensitive content).
// The manual p31 review config (playwright.p31-real.config.ts) is untouched
// and keeps reuseExistingServer: true for interactive use.
export default defineConfig({
  ...base,
  captureGitInfo: {
    commit: true,
    diff: false,
  },
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
