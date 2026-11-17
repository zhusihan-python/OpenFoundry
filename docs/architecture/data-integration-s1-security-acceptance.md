# S1 Security Acceptance — WP2.1 Webhook Tenant Isolation + WP2.2 Credential-Key Startup Gate

**工程验收通过 2026-11-17**（GLM 编制执行 / Codex review，四轮评审修复后独立复核通过）。
最终证据唯一指向 run `20261117T024942_29906`；更早的
`021720` / `023128` / `024220` 为过程记录（见文末）。

## Final results (run `20261117T024942_29906`, all exit 0)

- **HTTP + PG16: 29/29** — full `server.New` router + signed-JWT `authmw`
  verification + real `repo.Repo` on an isolated disposable `postgres:16`.
  12×404 / 3×401 / 1×400 / 2×403 / 10×200 / 1×429; all 12 cross-tenant &
  unknown-id rejections (owner cross-org, cross-org admin, grant holders,
  same-sub switched org, personal↔org, both history URL aliases) show
  **0 outbound hits + 0 new history rows in PG**; both URL-alias history
  positives are content-verified against seeded marker rows.
- **Binary startup gate: 11/11** — production missing/whitespace/wrong-length/
  legacy-derived key, unset env, `shared`, `staging`,
  `SERVICE_VERSION=dev`-in-production all refuse to boot with sanitized
  errors; production-with-key and dev-fallback reach healthy `/healthz` then
  exit 0; the runtime-order case (`production missing key refuses before DB
  connect (unreachable DSN)`) refused in **0 s** — before any DB dial.
- **11 recorded exit-code lines, all 0** (`exit-codes.txt`: command + tee
  per step, both enforced by the shared `runner_lib.sh` wrapper).
- **Negative controls: 16 rejected / 3 accepted / 4 real-wrapper** — the
  strict verifier (`verify_results.py`) rejects bad evidence and the
  production wrappers themselves (sourced, not copied) abort on failing
  commands (FAIL-text-free), failing `tee` log writes, and failing JSON
  verification.
- Unit/regression: handlers/config/domain/server `ok`; `-race`
  handlers+config `ok`; `go vet` + `gofmt` clean.
- Compose static check: dev compose resolves `development`/empty key by
  default and `production`/passthrough key when overridden
  (`--env-file /dev/null`, no stack restarted, no real `.env` read).

## Scope delivered

| WP | Change | Source |
| --- | --- | --- |
| 2.1 | `InvokeWebhook` + `ListWebhookHistory` (both URL aliases) resolve the connection via `GetConnectionForTenant(ctx, id, tenantFromClaims(claims))`; cross-tenant resources answer `404` before any side effect (outbound, history write, rate-limit consumption); same-tenant owner/admin/grant semantics unchanged (`connections:write`/`webhooks:invoke`, `connections:read`/`webhooks:read`); personal domains stay owner-only; `InvokeWebhook` gained the `Repo == nil` → 503 pending guard | `services/connector-management-service/internal/handlers/registrations.go` |
| 2.2 | `deriveCredentialKey` is fail-closed: an independent `CREDENTIAL_ENCRYPTION_KEY` (base64, exactly 32 bytes) is required for `production`/`prod`, unset, `shared` (Helm default), `staging`, `sovereign` and any unknown environment; only explicit `development`/`dev`/`test`/`testing`/`local` keep the legacy JWT-derived fallback; keys equal to the raw JWT secret or the legacy JWT-derived digest are rejected in every environment; errors are sanitized (`CredentialKeyError`: variable name + policy reason only — previously `InvalidEnvError.Value` carried the raw key into `main`'s fatal log) | `services/connector-management-service/internal/config/config.go` |

Environment signal: `OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT` (injected by
`infra/helm/_shared/templates/_helpers.tpl` from `global.deploymentFabric.environment`;
`values-prod.yaml` sets `production`, `values-dev.yaml` `development`, Helm
default `shared`). `SERVICE_VERSION` (defaults `dev`) and
`OPENFOUNDRY_DEV_AUTH` are deliberately NOT treated as dev signals. The
runtime domain crypto (`internal/domain/credentials.go`, AES-GCM ciphertext
format) is untouched — no re-encryption, no key rotation of existing data.
The development compose (`infra/compose/docker-compose.yml`) pins the
canonical dev signal with a pure passthrough for the key; `.env.example`
documents that its legacy `ENVIRONMENT` entry is not the canonical signal.

## Verification entry points

- `tools/smoke/data-integration-s1-security/run.sh` — one-shot runner
  (isolated PG16 + golang:1.25 harness; strict verifier; exit-code ledger;
  negative controls). Helpers: `runner_lib.sh` (shared wrappers),
  `wp22-binary-gate.sh`, `verify_results.py`, `negative_controls.sh`.
- `services/connector-management-service/internal/handlers/webhook_tenant_isolation_test.go`
  — fakeStore tenant matrix + real-router alias test with signed JWTs.
- `services/connector-management-service/internal/handlers/webhook_tenant_http_pg_test.go`
  — env-gated (`S1SEC_PG_DSN`) real HTTP+PG acceptance, seeded-content
  assertions, all seeds verified non-empty before the rejection matrix.
- `services/connector-management-service/internal/config/config_test.go` —
  `TestFromEnvCredentialKeyGate` (env×key matrix + leak assertions).
- Evidence (final): `tools/smoke/data-integration-s1-security/results/20261117T024942_29906/`
  — `exit-codes.txt` (11 lines), `wp21-unit.log`, `wp21-http-pg.{log,json}`,
  `wp22-binary-gate.{log,json}`, `negative-controls.log`; leak-checked
  (no JWT secret / DSN / key material in any JSON).

## Compatibility boundary (WP2.2)

- Existing non-dev deployments without `CREDENTIAL_ENCRYPTION_KEY` **will now
  fail to start** (intended fail-closed default) — set an independent key
  (`openssl rand -base64 32`) before deploying. Documented in
  `services/connector-management-service/README.md` ("CREDENTIAL_ENCRYPTION_KEY
  startup gate"), including: changing the key does not re-encrypt existing
  ciphertext; rows encrypted with the old (incl. legacy JWT-derived) key
  become undecryptable; no automatic rotation is performed on existing DBs.
- Dev/test environments keep the legacy JWT-derived fallback only via an
  explicit `OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT` value; test baselines now pin
  a synthetic independent key.

## Not verified / out of scope

- No RID (non-UUID) route alias exists for webhook invoke/history — only
  UUID `{id}` / `{source_id}`; non-UUID is rejected 400 (covered above).
- Helm charts themselves were not deployed; the gate reads the documented
  `OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT` values verified in
  `infra/helm/_shared/templates/_helpers.tpl` and `infra/helm/profiles/values-*.yaml`.
- Out of scope throughout: other DI connectors/pipeline features, commits,
  deployment, real credentials.

## Process record (superseded runs)

- `20261117T021720_18605` — first green run (29/29 HTTP+PG, 10/10 gate).
- `20261117T023128_28412` — after Codex round 2 (dev compose, seeded-content
  history assertions, real-router alias test).
- `20261117T024220_15481` — after Codex round 3 (strict JSON verifier, real
  booleans, exit-code ledger, negative controls, unreachable-DSN runtime-order
  case). Codex round 4 residuals (tee enforcement via shared
  `runner_lib.sh`, real-wrapper negative controls, org-A pre-seeding) were
  verified in the final run above.
