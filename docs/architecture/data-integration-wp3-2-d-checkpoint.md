# WP3.2 D API checkpoint — Sol continuation

> 历史阶段快照，当前 WP3.2 验收/归档清理以[验收报告](data-integration-wp3-2-acceptance.md)为准；其中当时保留的临时容器已清理，旧启动命令是历史记录。

2026-11-17. API scope only; does not claim WP3.2 complete. No commit/push/deployment.

## Current implementation

- Five legacy CRUD methods were moved precisely out of shared `scheduling_repo.go`; core owns that file again.
- Removed four unused unsafe legacy entry points. `UpsertGrant` is a thin trusted fixture adapter delegating to `UpsertGrantAs`.
- All grant/definition management transactions use one transaction advisory lock for `(tenant_kind, tenant_id, schedule_key)` before reading rows. This includes empty histories and first definition insert. Latest-owner authorization therefore observes newly committed history after waiting.
- Enable locks grant and existing definition before separate DB clock/expiry and cron computation. Resume locks binding and grant before separate DB clock. HTTP error `errors.As` target is local.
- HTTP actor derives from verified JWT; owner/admin/personal rules and strict request decode already present. Enable/resume bind tenant/key/pipeline and require subject=created_by for API-created grants.

## Verification complete for D scope

Dedicated PG: `ofwp32-d-pg`, PostgreSQL 16.16, port 55433. Never use worker A PG. Dedicated Go image is Go 1.26.3; module remains unchanged.

Raw evidence: `/private/tmp/ofwp32-d-evidence/`.

- `05-api-sol-test.txt`: exit 2, image entrypoint expected unit/integration/guard-probe; no tests ran.
- `06-api-sol-test.txt`: exit 1, compile found GLM test unused variables and invalid multi-return Exec assertions; fixed in source. Also transient core helper vet issue reported to core (helper now exists).
- `07-api-sol-test.txt`: exit 1, remaining fixture grantID variable fixed.
- `08-api-sol-test.txt`: exit 1, pre-existing cipher fixture was not 32 bytes; replaced with exact AES-256 fixture key.
- `09-api-sol-test.txt`: exit 1, admin regrant fixture omitted admin JWT role; fixed helper to accept roles. All four concurrency/expiry tests passed already.
- `10-api-sol-test.txt`: **exit 0**, all 13 real JWT HTTP/PG tests passed, including concurrent first/history creation, queued former owner and both grant/definition row lock waits across expiry.
- `12-unit-build.txt`: **exit 0**, CRUD authorization unit tests passed and `go build -p 1 ./services/pipeline-build-service/...` passed.
- `11-cache-image.txt`: dedicated ephemeral cached image `ofwp32-d-test-cache:local`, created from retained `ofwp32-d-test2` to reuse downloaded dependencies. No global Docker configuration changed.
- Real HTTP/JWT/PG suite covers ownership, domains, binding, invalid cron/key, cron next run and revision lifecycle. Added real PG concurrent initial/history generations, queued former owner after admin regrant, and definition-lock wait across expiry.

PostgreSQL 16 official docs verified transaction advisory lock scope/release and row locking: https://www.postgresql.org/docs/16/explicit-locking.html and https://www.postgresql.org/docs/16/functions-admin.html .

## Resource audit

Privileged `ps` showed no leftover GLM D rsync/retry loop. Only current Sol test process and wrapper were present. `/private/tmp` has D evidence only, no D snapshot directory; no original results or archives deleted.

## Review handoff

D API implementation and scoped checks are ready for root review. No remaining D test failure. Whole WP3.2 remains owned by root and the other workers.

Retained test resources for reproducibility: dedicated PG `ofwp32-d-pg`, stopped container `ofwp32-d-test2`, cached image `ofwp32-d-test-cache:local`. All raw evidence is retained. No repository old results, DB volumes, environment files, or global Docker limits were modified. No final production deployment or commit occurred.

Reproduce HTTP/PG suite using `docker start -a ofwp32-d-test2` (the retained command runs gofmt then the integration suite). The cached image supports focused unit/build checks without downloading the API dependency set again. Continue to coordinate CPU/memory with other workers.
