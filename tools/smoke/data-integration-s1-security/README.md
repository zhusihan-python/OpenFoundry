# data-integration-s1-security — S1 acceptance (WP2.1 + WP2.2)

One-shot acceptance for the S1 security work packages:

- **WP2.1** — webhook `invoke` / `history` tenant isolation: cross-tenant
  callers get `404` with **zero** outbound upstream hits and **zero** new
  `webhook_history` rows; same-tenant owner/admin/grant paths keep working
  and history reads return real seeded content.
- **WP2.2** — independent `CREDENTIAL_ENCRYPTION_KEY` startup gate: the
  production binary fails closed without a valid independent key (verified
  to happen BEFORE any database connect); explicit dev/test keeps the legacy
  JWT-derived fallback.

## Run

```bash
tools/smoke/data-integration-s1-security/run.sh          # full run
tools/smoke/data-integration-s1-security/run.sh --keep   # keep containers for debugging
```

Requires: Docker with the `postgres:16` and `golang:1.25` images (already
part of the project's tooling) and the repo mounted at `/work` inside the
harness container (done automatically).

## Anti-greening contract (how a step "counts")

Every step must satisfy BOTH, in order:

1. **Real exit code 0 — for the command AND for tee.** The step wrappers
   (`record_rc` / `run_step` / `verify_step`) live in `runner_lib.sh` and are
   sourced by both the runner and the negative controls (one implementation,
   no copies). Each docker/go/helper command runs under `set -o pipefail`;
   the pipeline status array is captured once right after the pipeline and
   BOTH the command's rc and tee's rc are recorded separately to
   `results/<RUN_ID>/exit-codes.txt` (`<step>` and `<step>-tee`). Any
   non-zero aborts the run immediately, and a final pass re-checks that
   every recorded code is 0. grep is never treated as evidence of success.
2. **Strict structured verification** (`verify_results.py`): evidence JSONs
   must exist, carry the run's `run_id`, have REAL booleans for
   `all_passed`/`pass` (ints/strings rejected), non-empty typed `cases` with
   `got == want`, the REQUIRED scenario inventory present, rejections with
   zero side-effect deltas, no SKIP/FAIL lines in the test log, and — for the
   binary gate — the runtime-order case (fast, non-timeout refusal on an
   unreachable DSN). Missing or failing JSON fails the run.

`negative_controls.sh` (step 4 of the runner, also standalone) proves the
verifier rejects 16 classes of bad evidence (missing file, `all_passed`
false/string/int, empty cases, missing required scenario, int `pass`,
side-effect deltas, status mismatch, wrong run_id, log without PASS, log
with SKIP, gate runtime-order missing/timeout, non-zero exit codes) and
accepts 3 valid baselines. Four REAL-WRAPPER controls additionally source
`runner_lib.sh` in a sub-shell and drive the production `run_step` /
`verify_step` themselves: a FAIL-text-free failing command aborts with its
own rc; a tee that cannot write the log aborts the step even though the
command exits 0; `verify_step` aborts on bad evidence and passes a valid
baseline — asserting the wrapper's recorded exit codes along the way. All
against `mktemp` fixtures, no docker, no real stack.

## What the runner creates (and removes)

Per run, with unique names — nothing else is touched (no existing services,
DBs, volumes, `.env`):

- docker network `ofs1sec-net-<RUN_ID>`
- `postgres:16` container `ofs1sec-pg-<RUN_ID>` with synthetic credentials
- docker volumes `ofs1sec-modcache` / `ofs1sec-buildcache` (Go module/build
  caches, shared between runs, created on first use)
- `results/<RUN_ID>/` logs + JSON evidence + `exit-codes.txt` (gitignored)

## Evidence

| File | Content |
| --- | --- |
| `results/<RUN_ID>/exit-codes.txt` | raw exit code of every step (all must be 0) |
| `results/<RUN_ID>/wp21-unit.log` | unit + regression `go test` output (handlers/config/domain/server) |
| `results/<RUN_ID>/wp21-http-pg.json` | per-case HTTP matrix against the real server router + JWT verification + PG16 repo: status, upstream Δ, history Δ (rejections pinned to 0/0) |
| `results/<RUN_ID>/wp21-http-pg.log` | `go test -v` log of the HTTP/PG acceptance (required PASS line, no SKIP) |
| `results/<RUN_ID>/wp22-binary-gate.json` | binary startup-gate cases incl. unreachable-DSN runtime-order refusal (real booleans, leak-checked) |
| `results/<RUN_ID>/wp22-binary-gate.log` | full gate log |
| `results/<RUN_ID>/negative-controls.log` | bounded negative-control results |

All secrets in the harness are synthetic per-run values; result JSONs record
assertions only — never key material, JWT secrets or DSNs.

## Entry points in the source tree

- `services/connector-management-service/internal/handlers/webhook_tenant_isolation_test.go` — fakeStore tenant matrix (unit) + real-router alias test with signed JWTs
- `services/connector-management-service/internal/handlers/webhook_tenant_http_pg_test.go` — env-gated (`S1SEC_PG_DSN`) real HTTP+PG acceptance with seeded-content assertions
- `services/connector-management-service/internal/config/config_test.go` — `TestFromEnvCredentialKeyGate` gate matrix + leak assertions
