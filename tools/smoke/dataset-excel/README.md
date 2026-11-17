# dataset-excel — real-backend acceptance harness (PR3)

One-shot, clean-environment acceptance for the two REAL-backend tests in
`apps/web/e2e/dataset-excel-real.spec.ts` (Excel import: 订单 header-row-3
mapping → validated json dataset → detail preview/schema/persistence, and
no-header 原始 fallback import). Same anti-greening contract as
`tools/smoke/data-integration-s1-security`: real exit codes + strict
structured verification + negative controls. No new services, no shared
platform — a thin reproducible wrapper around the EXISTING spec, fixtures
and configs.

## Run

```bash
tools/smoke/dataset-excel/run.sh          # full run (~5-15 min; builds 2 Go binaries)
tools/smoke/dataset-excel/run.sh --keep   # keep the disposable stack for debugging
```

Requires: Docker (`golang:1.25` + `postgres:16`, pulled on first use),
python3, pnpm install done (`pnpm --filter @open-foundry/web install
--frozen-lockfile`), and Playwright chromium installed
(`pnpm --filter @open-foundry/web exec playwright install chromium`).

What one command does — nothing manual is left over from previous reviews:

1. **[pre] ensure-images** — inspect `golang:1.25` / `postgres:16`; pull
   only what is missing locally (cold CI runners have neither; existing
   images are never touched or removed).
2. **[0] record-env** — exact toolchain identities (node/pnpm/playwright/go
   `go version`, docker client) + docker image **sha256 IDs** (tags are not
   runtime identities).
2. **[1] build** — `dataset-versioning-service` + `edge-gateway-service`
   compiled from THIS worktree inside `golang:1.25`
   (`go build -buildvcs=false`, same package path as `make build-services`;
   `-buildvcs=false` because worktree `.git` is an absolute-path pointer
   that does not survive the mount). Binary sha256 recorded.
3. **[2] stack** — per-run network + `postgres:16` + DVS + REAL edge
   gateway on a RANDOM free host port (explicit-conflict failure: docker
   `-p` refuses a taken port; vite runs `strictPort`). No dependency on
   the resident `p31-*` containers, port 3931, or
   `/private/tmp/openfoundry-p31-review` leftovers. The gateway performs
   the real `/api/v1/datasets/*` → `/v1/*` rewrite against DVS.
4. **[3] probe** — `GET /healthz` = 200 AND an authenticated
   `GET /api/v1/datasets?page_size=1` = 200 with JSON (proves
   gw→DVS→PG + JWT validation + repo.Migrate boot all live).
5. **[4] live skip control** — the REAL suite runs once with
   `P31_REAL_E2E=0`: playwright must exit 0, the report must genuinely
   contain exactly the 2 required tests SKIPPED, and the strict verifier
   must REJECT that report. A crash or empty report cannot masquerade as
   "control passed".
6. **[5] real run** — `P31_REAL_E2E=1` via the dedicated
   `apps/web/playwright.ofexcel.config.ts` (extends the manual p31 config;
   forces `reuseExistingServer: false` so a stale vite can never satisfy a
   run, `trace: retain-on-failure`, `screenshot: only-on-failure`).
   Structured `json` reporter output is verified:
   both required tests present, each `status: expected`, exactly one
   result `passed` with `retry: 0`, positive duration, `startTime`;
   `stats.expected == 2` and `skipped/unexpected/flaky == 0`; no
   top-level errors; exactly 2 specs total.
7. **[6] evidence + controls** — `evidence.json` (run id, git
   commit/dirty/branch, toolchain + image IDs, binary/fixture/report
   sha256s, probe statuses — artifacts re-hashed by the verifier, fixture
   hash pinned to `1d52be…d745f`), then `negative_controls.sh` mutating
   the REAL run's JSON/manifest, and a final all-exit-codes-zero check.

## Anti-greening contract (how a run "counts")

Every step's REAL exit code — command AND tee — is recorded to
`results/<RUN_ID>/exit-codes.txt`; non-zero aborts. `verify_results.py`
additionally rejects: missing files, any SKIP/failed/flaky/interrupted,
retry traces, zero-duration "passed" results, missing `startTime`, hidden
extra specs, reporter-level errors, wrong run inventories, fixture hash
drift, artifact hash drift, duplicate artifacts/steps, token-value
leakage into evidence, non-200 probes, and missing env identities.
`negative_controls.sh` proves these rejections against mutants of the
REAL run's artifacts (never hand-made shapes) and exercises the real
`run_step`/`verify_step`/`expect_verifier_reject` wrappers in subshells.

## What this acceptance does NOT verify (honest scope)

- **Real login is NOT verified.** `vite.p31-e2e.config.ts` stubs
  `/api/v1/auth/bootstrap-status` and `/api/v1/users/me` locally (the
  disposable stack has no auth service); the browser session is injected
  with a synthetic token. Everything dataset-related IS real gateway
  traffic.
- The synthetic JWT carries the PUBLIC p31 test claims (sub/org/roles from
  `runtime-public.json`) but is signed with a **fresh random secret per
  run** and is valid only against this run's disposable stack — there is
  no real account behind it.

## Security notes

- Token values never enter logs, `results/`, or git; only their sha256 is
  recorded. The per-run token file lives in a 0700 mktemp dir, removed on
  exit (also on INT/TERM).
- Playwright traces/test artifacts captured **on failure MAY contain the
  one-shot token** (localStorage). Keep `results/` local (gitignored);
  never upload raw traces — upload `evidence.json`, `exit-codes.txt`,
  `playwright.json` and logs only.
- The runner removes ONLY what it created: per-run containers (`docker rm
  -fv`, including the postgres anonymous volume), network, and token dir.
  The resident `p31-gw`/`p31-dvs`/`p31-pg` containers, shared cache
  volumes (`openfoundry-p31-gomod`, `of-go-build`), other worktrees and
  the original repo are never touched.

## Evidence (gitignored `results/<RUN_ID>/`)

| File | Content |
| --- | --- |
| `exit-codes.txt` | raw exit code per step (verifier: all zero, required steps present) |
| `versions.json` | toolchain + docker image sha256 identities |
| `build-dvs.log` / `build-gw.log` | full go build output |
| `stack-probe.json` / `.log` | healthz + authenticated catalog probe statuses |
| `playwright-skip.json` / `.log` | live skip-control run (must be verifier-REJECTED) |
| `playwright.json` / `playwright-real.log` | structured results of the real run |
| `evidence.json` | manifest: git state, env ids, artifact sha256s, probes |
| `negative-controls.log` | 19 rejections + 3 acceptances + 5 real-wrapper controls |
| `test-artifacts/` | traces/screenshots on failure (may contain one-shot token) |

## Binding evidence to the final source (clean-SHA rule)

The evidence manifest records the git commit and dirty flag. A run on a
dirty tree (harness development) proves the machinery; the ACCEPTED state
for review is a run with `git.dirty == false` on the exact commit that is
merged — re-run `run.sh` once after committing the harness.

## CI

`.github/workflows/dataset-excel-acceptance.yml` runs the same `run.sh` on
`pull_request` (any base branch) and `workflow_dispatch` when the relevant
paths change: the spec, fixtures, both playwright configs, the vite test
config, dataset upload UI + api/auth client code, web/workspace package
manifests and lockfile, `libs/auth-middleware`, DVS/gateway services, the
harness itself, and go.mod/go.sum. Structured evidence is uploaded on BOTH
success and failure, guarded against credential-shaped strings, via an
explicit per-file whitelist — raw traces, test artifacts and service logs
are never uploaded.
