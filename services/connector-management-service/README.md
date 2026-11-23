# connector-management-service (Go)

Go runtime for the Foundry Data Connection app migration. The Go service now uses the same runtime configuration names and defaults as the Rust service where those defaults exist.

## Compatibility naming

Data Connection public payloads should follow the frozen terminology in
[`docs/reference/foundry-compatibility-glossary.md`](../../docs/reference/foundry-compatibility-glossary.md):
use `source` for Data Connection sources, `source_rid` for stable source
identity, `webhook` for outbound callable definitions, and `output_parameters`
for parsed webhook/action outputs. Legacy `/connections` aliases may remain at
the HTTP edge, but internal and new product docs should prefer `source`.

## S3 snapshot sources

The native sync endpoint reads one NDJSON object and commits its rows to a versioned dataset. Configure a trusted, path-style object store origin with `S3_SYNC_ENDPOINT` and its signing region with `S3_SYNC_REGION` (default `us-east-1`). Leave the endpoint unset to disable native S3 reads. Restrict upstream access using `ALLOWED_EGRESS_HOSTS` and the store's object policy.

Create a source using `POST /api/v1/data-connection/sources`:

```json
{"name":"Daily orders","connector_type":"s3","config":{"bucket":"order-fixture","sync":{"object_key":"orders/daily.ndjson"}}}
```

The source may instead use `url: "s3://order-fixture/orders/"` and `object_key: "daily.ndjson"`. Bind `kind: "aws_keys"` through the source credentials endpoint. Its `value` is a JSON-encoded string containing `access_key_id`, `secret_access_key` and an optional `session_token`. Grant the upstream identity permission to read the specified object. Source JSON stores the bucket and key. The gallery's `batch_sync` capability identifies types served by the native snapshot runtime, and the per-source capabilities response exposes `native_snapshot_sync` for the saved configuration. Other gallery capabilities describe their respective connector features.

Use the standard sync definition and run endpoints. Each successful run provides the output transaction and content hash. A missing object or malformed row fails the run before publication. [Reproducible order pipeline and downstream readback](../../tools/smoke/data-integration-wp2-3/README.md).

## REST API sources

`connector_type: "rest_api"` sources use a normalized config shape for outbound
webhooks and external HTTP reads. The service accepts either `domain` or
`base_url` and stores both, plus auth metadata, secret references, worker/runtime
policy, and source permissions. This lets an Open-Meteo source be configured
without code changes:

```json
{
  "name": "Open-Meteo",
  "connector_type": "rest_api",
  "config": {
    "domain": "api.open-meteo.com",
    "auth": { "type": "none" },
    "runtime": {
      "worker": "foundry",
      "timeout_ms": 10000,
      "retry_count": 0,
      "allowed_methods": ["GET", "POST"]
    },
    "permissions": {
      "discoverable": true,
      "syncable": false,
      "invokable": true,
      "usable_in_code": true,
      "allowed_egress_hosts": ["api.open-meteo.com"]
    },
    "webhook": {
      "path": "/v1/forecast",
      "method": "GET",
      "inputs": [
        { "id": "latitude", "type": "number", "required": true },
        { "id": "longitude", "type": "number", "required": true }
      ],
      "calls": [
        {
          "id": "weather",
          "method": "GET",
          "path": "/v1/forecast",
          "query_params": {
            "latitude": "{{latitude}}",
            "longitude": "{{longitude}}",
            "current": "temperature_2m,wind_speed_10m,relative_humidity_2m"
          }
        }
      ],
      "outputs": [
        { "id": "temperature", "type": "number", "extractor": { "from_call": "weather", "path": "/current/temperature_2m" } },
        { "id": "wind_speed", "type": "number", "extractor": { "from_call": "weather", "path": "/current/wind_speed_10m" } },
        { "id": "humidity", "type": "number", "extractor": { "from_call": "weather", "path": "/current/relative_humidity_2m" } }
      ],
      "timeout_ms": 10000,
      "concurrency_limit": 10,
      "rate_limit": { "max_requests": 60, "per_seconds": 60 },
      "history": { "enabled": true, "retention_days": 30, "store_outputs": true }
    }
  }
}
```

Supported `auth.type` values are `none`, `basic`, `bearer`, `api_key`, and
`custom_header`. Local/dev tests may pass `auth.value` or legacy
`bearer_token`; production configs should prefer `auth.secret_ref` plus the
credential endpoints.

## Tenant boundaries (P1.2)

Sources (connections), their credentials, policy bindings, sync definitions
(`batch_sync_defs`) and sync runs are tenant-scoped:

- The tenant is derived exclusively from the verified JWT `org_id` claim at
  creation time and persisted (`connections.org_id`, `batch_sync_defs.org_id`,
  `sync_runs.org_id`, all nullable). Client-supplied organization headers or
  request-body tenant fields are never consulted, and `?owner_id=` query
  filters can only narrow a list, never widen it.
- Org-context callers only see rows with a matching `org_id` — the same
  subject in Org A and Org B gets two fully isolated domains, and org admins
  cannot cross organizations either (admin grants apply only inside the
  caller's own tenant).
- Legacy rows with `org_id IS NULL` stay reachable only by their original
  owner, and only from personal-domain requests (JWT without `org_id`). Org
  requests never auto-claim them.
- Within the tenant, the existing owner/scope grants still apply (owner,
  `admin` role, `connections:read`/`connections:write` permissions).

Out of the first-slice scope (still owner-scoped only): media-set syncs,
virtual tables, connector agents, webhook invocation/history, inbound
listeners, and the Iceberg catalog emulation surface.

## Snapshot sync (P1.3)

`POST /api/v1/data-connection/syncs/{id}/run` executes a bounded, synchronous
full-snapshot REST pull for `rest_api` sources that carry a `sync` config
block. The block is a first-class field of the normalized source config:

```json
{
  "base_url": "https://api.example.com",
  "auth": { "type": "bearer", "secret_ref": "oauth_token" },
  "sync": {
    "resource_path": "/v1/items",
    "records_path": "data",
    "pagination": { "type": "page", "page_param": "page", "page_size_param": "page_size", "page_size": 100 },
    "limits": { "max_pages": 100, "max_records": 10000, "max_response_bytes": 8388608, "max_total_bytes": 67108864, "request_timeout_ms": 10000 }
  }
}
```

- `pagination.type` is `page` (number-based), `cursor` (requires
  `next_cursor_path`; cursor values are opaque strings echoed back via
  `cursor_param`), or `none` (single request).
- `records_path` is a dot path from the response body to the records array
  (`""` addresses the root array). Records are copied into the NDJSON snapshot
  verbatim, so identifiers like SKUs with leading zeroes survive unchanged.
- Every request URL is rebuilt from `resource_path` on the source host —
  pagination can never be steered to another host — redirects are rejected,
  and egress goes through the shared `http_runtime` policy. A source may carry
  an optional `egress` block (`allow_insecure_http`,
  `allow_private_networks`, `allowed_hosts`, `blocked_hosts`); it survives
  config normalization and is merged as a **narrowing override only**: the
  service-level host allowlist and private-network prohibition are ceilings a
  source cannot widen (`allowed_hosts` intersects, `blocked_hosts` appends,
  `allow_private_networks` may only tighten), and insecure HTTP stays
  rejected by default — `allow_insecure_http: true` on the source is the one
  explicit exception, still subject to the allow/block lists.
- All limits (`max_pages`, `max_records`, `max_response_bytes`,
  `max_total_bytes`, per-request timeout) **fail the run explicitly**;
  snapshots are never truncated. Cursor loops are detected and fail the run.
- External credentials: `auth.secret_ref` addresses a credential kind stored
  via `POST /api/v1/data-connection/sources/{id}/credentials` (AES-GCM
  encrypted at rest). The decrypted material is applied **only** to the
  upstream REST source; missing or undecryptable credentials fail the run.
- Publication: the run posts the exact NDJSON bytes to the dataset service's
  `POST /api/v1/datasets/{rid}/outputs:commit` (format `TEXT`,
  `sync-<run id>.ndjson`, real `size_bytes`) **forwarding the caller's
  platform JWT** — the dataset service enforces target-dataset ownership and
  tenant on that identity. A run only becomes `succeeded` when the response
  carries a committed `transaction.id` (recorded as the run's version
  reference). Zero-row snapshots publish a downloadable empty `.ndjson`.
- `content_hash` is `sha256` over the NDJSON bytes only (no run ids, no
  config envelope). Each snapshot run publishes its own version — there is no
  cross-run no-op reuse (a sync repointed at a different target dataset must
  never reuse the old target's version).
- Upstream HTTP failures, invalid JSON, limit breaches, and publish failures
  all propagate to a `failed` run with the error recorded; nothing is
  published from an incomplete pull.

The former dispatch to ingestion-replication (`HTTPIngestionClient`, which
only created an ingest job and counted the config envelope as payload) was
removed from the sync-run path; runs against connector types without a real
runtime now fail explicitly instead of reporting fake success.

### Target dataset binding checks

`output_dataset_id` bindings are validated against the dataset service under
the **caller's forwarded JWT** via `GET /api/v1/datasets/{id}` (reusing
`HTTPDatasetVersioningClient`):

- on `POST /data-connection/syncs` (create) and on `PATCH /data-connection/syncs/{id}`
  whenever the update is applied — cross-org or missing targets reject the
  request with 404/403 and nothing is created or mutated. A missing dataset
  dependency fails closed (503 unconfigured / 502 upstream 5xx-timeout), and
  cross-tenant updates keep a stable 404 even while the dependency is down
  (tenant visibility and the owner/`connections:write` grant are checked
  before any dependency call).
- on `POST /data-connection/syncs/{id}/run`, after the run row is created and
  **before any upstream pull**: an invalid target fails the run with nothing
  fetched and nothing published. The final `outputs:commit` re-adjudicates on
  the dataset side, so races remain covered.
- 401 (caller JWT rejected upstream), 403 (other tenant's dataset) and
  404 (missing) are classified distinctly; timeouts/5xx are dependency
  failures and never blame the request.

### Credential-safe error handling

Run errors are persisted and returned verbatim, so all transport failures are
sanitized first: URL query strings, fragments and userinfo are stripped
entirely (not per-parameter-name) from every error an upstream call can
produce — `api_key` auth may place the credential behind any `query_param`
name, and `url.Error` embeds the fully-encoded request URL. Timeout errors
carry no URL at all.

### Integration tests (real PostgreSQL 16)

`internal/repo/repo_integration_test.go` and
`internal/repo/lease_integration_test.go` (build tag `integration`) prove the
tenant predicates, migrations, cross-domain CRUD, and the P2.1 lease
semantics against a real database:

- `TestPGMigrateEmptyDatabaseAndRerun`
- `TestPGMigrateLegacyDatabaseGainsOrgColumns` (pre-org schema + rows upgraded)
- `TestPGSourceTenantCRUDMatrix` (+ subtests: list/get/update/delete,
  credentials, sync defs, runs, `UpdateConnectionConfig` org scan)
- `TestPGLeaseEnqueueCommitsAndProvenance` (committed pending row visible
  from a second session; `requested_by`/`owner_id` provenance)
- `TestPGLeaseConcurrentClaimExactlyOneWinner` (8 independent sessions)
- `TestPGLeaseRenewCompleteRejectWrongFencingMaterial` (holder/token/
  generation/tenant mismatches leave the full row untouched)
- `TestPGLeaseRowLockExpiryClockRegression` (blocker holds FOR UPDATE
  without writing — verified via pg_blocking_pids with the DB clock still
  inside the lease at wait entry; expiry behind the lock still fences renew
  AND complete)
- `TestPGLeaseTTLAnchoredToPostLockClock` (a claim/renew blocked longer than
  its own TTL still installs a lease anchored to the post-lock DB clock,
  never born expired)
- `TestPGLeaseExpiryFenceBeforeTakeover` (expired leases cannot renew or
  complete even before any takeover)
- `TestPGLeaseTakeoverFencingAndABA` (generation+token rotation; old tokens
  dead forever, including same-holder ABA; unexpired leases not takeable)
- `TestPGLegacyRowsGuarded` (legacy lease-less rows close exactly once and
  are immune to lease ops/takeover)
- `TestPGLeaseTenantIsolationMatrix` (personal/org domains × foreign
  tenants: enqueue/claim/renew/complete/check all refuse, full row unchanged)
- `TestPGLeaseInvalidArgumentsTouchNothing` (blank holder/token, zero/over-
  max/rounds-to-zero TTLs, generation 0: error out without touching a column)
- `TestPGActiveGuardsBlockSourceAndTargetMutations` (refusals leave the
  source/def/run rows byte-identical)
- `TestPGDefLockVsRetargetDeterministic` (uncommitted pending INSERT holds
  the def's FK KEY SHARE; the retarget genuinely waits, then is refused
  post-lock with rows unchanged)
- `TestPGEnqueueVsDeleteBarrier` (two-session barrier races for both the
  enqueue and the legacy-run trigger paths against guarded deletes)
- `TestPGMigrateLegacyDBPreservesRowsAndLeaseOnRerun` (built on a genuine
  pre-P2.1 schema — lease columns provably absent — the upgrade keeps legacy
  terminal version/hash and never resets a live lease on replay)

Run with `CONNECTOR_DB_DSN` (acceptance runner inside a compose container —
each test gets its own fresh database; no Docker socket needed):

```sh
CONNECTOR_DB_DSN="postgres://postgres:postgres@host:5432/postgres?sslmode=disable" \
  go test -tags integration -run TestPG ./internal/repo/
```

Without the variable the tests fall back to the standard testcontainers
harness.

## Durable tasks and run leases (P2.1)

A manual `POST .../syncs/{id}/run` now executes as a durable task under a
database lease. The HTTP contract is unchanged (still synchronous
pull → publish → persist → `202` carrying the completed run), but the run is
no longer a bare `running` row: every execution is fenced by the database.

Lifecycle and fencing:

- **Enqueue** inserts a `pending` row (`sync_runs`) with trusted provenance:
  `org_id` from the sync def, `owner_id` snapshotting the **source owner**,
  `requested_by` recording the **verified JWT subject** that triggered the
  run — the two identity fields are never conflated, and request bodies or
  headers cannot forge either. Each POST inserts its own row; manual posts
  are never merged or throttled (per-source rate limiting is a later P2
  task). The insert runs in a transaction that first takes `FOR SHARE` on
  the source row, so it serializes against guarded deletes and can never
  surface a foreign-key violation mid-race.
- **Claim** atomically flips `pending` → `running` in a two-phase
  transaction, bumping `attempt`/`generation` and installing an unguessable
  random lease token plus the holder identity (hostname + per-process
  suffix; never any credential, JWT or Authorization material). Multiple
  connections racing on the same pending run are serialized by the row lock
  and re-check `status='pending'` after acquiring it — exactly one wins.
- **Renew / terminal complete** are lease CAS operations guarded by
  run id + tenant + `status='running'` + holder + token + generation +
  an UNEXPIRED lease. Terminal states can never be overwritten afterwards.
  Every lease operation is **two-phase**: `SELECT … FOR UPDATE` on the run
  row first, then — only after the lock is held — a separate UPDATE whose
  `clock_timestamp()` quals and TTL extension are evaluated with the
  post-lock clock. A single-statement CAS is insufficient under READ
  COMMITTED: a blocker that only holds `FOR UPDATE` (no row version change)
  never triggers EvalPlanQual re-evaluation, so the waiting statement would
  keep its pre-wait verdict. `TestPGLeaseRowLockExpiryClockRegression`
  proves both renew and complete are fenced in that exact scenario.
- **Expiry semantics**: once `clock_timestamp() >= lease_expires_at` — even
  before anyone takes over — renew and complete are rejected. `TakeoverExpiredSyncRun`
  exists as storage capability for the P2.2 background worker: it re-claims
  an expired run, rotating generation and token so every older
  (token, generation) pair is permanently fenced (same-holder ABA included).
  Nothing in P2.1 calls it from HTTP or a worker; no REST run is re-executed
  automatically.
- **Legacy rows**: pre-P2.1 `running` rows have NULL lease columns — they are
  never takeover candidates and are never auto re-published. The legacy
  no-token `CompleteSyncRun` can still close them exactly once; its
  predicates (`status='running' AND lease_token IS NULL`) can neither bypass
  a leased run nor rewrite a terminal state nor close a P2.1 pending task.

Execution-time lease enforcement on the synchronous path:

- A bounded heartbeat (default TTL 60s, beat TTL/3) renews the lease while
  the REST pull is in flight; any renew failure is **sticky** — the failure
  and its cause are recorded, the pull's work context is cancelled, and no
  later successful renew on a detached context may "revive" the execution
  into publishing.
- `FetchRESTSnapshot` accepts an optional `BeforePage` gate (nil for all
  other callers); the sync path re-checks the lease against the database
  clock before **every** page request, so expiry or takeover between
  heartbeat ticks is observed at the next page boundary and no further pages
  are fetched.
- Before publishing, heartbeats stop and one final full-TTL renew proves the
  lease; losing it aborts before any publish side effect. The publish
  (dataset `outputs:commit`) and the final terminal write each run under
  their own bounded detached-context timeouts.
- **No fabricated outcomes**: if the claim is lost the API answers `409`; if
  the lease is lost/fenced the API answers `409` and the run stays `running`
  for takeover; if the terminal write fails after a successful publish the
  API answers `503` stating the outcome is uncertain. The pre-P2.1 fallback
  that returned an in-memory "completed" run after a failed persist has been
  removed deliberately.

Honest limits of this guarantee (by design, not an accident):

- The dataset publish is an HTTP call to dataset-versioning-service. It is
  **not** fenced by this database's CAS: if the lease expires while the
  publish is in flight, or the terminal write fails after it, the dataset
  may hold a committed version while the run row stays `running`. P2.1
  proves lease and local-state fencing only — **no exactly-once remote
  publish is claimed**. The pre-commit gate bounds that window to one lease
  TTL; closing it is left to the later-P2 idempotent publishing / commit
  recovery tasks (P2.2 delivers an interruptible worker and does not promise
  exactly-once publishing either).
- Upgrade path: stop the old binary, run migrations (replayed idempotently
  on every start; a live lease survives a replay untouched), then start the
  new binary. Running old and new binaries concurrently against the same
  database is **not** supported and not safe — the old code has no lease
  discipline.
- Active-period policy: while any run of a source is `pending` or
  `running`, source deletion, source config changes (including the
  auto-registration config surface) and sync-def retargeting are refused
  with `409` (`repo.ErrActiveRuns`); name/status-only connection updates and
  same-target def writes stay allowed; after runs reach terminal states the
  pre-P2.1 semantics return (a hard delete cascades the run rows away). The
  guards are enforced under row locks (source `FOR UPDATE` / def `FOR
  UPDATE` vs the enqueue's `FOR SHARE` + FK KEY SHARE), so an enqueue
  committing while a delete waits is actually observed — proven by the
  barrier tests above.

## Interruptible runs and async execution (P2.2)

The default `POST .../syncs/{id}/run` stays the synchronous P2.1 contract
(durable enqueue → lease claim → pull → publish → persisted terminal state →
`202`), and it is now equally **cancellable** and **service-shutdown-managed**.
An explicit opt-in `?async=true` returns `202` with the `pending` run
immediately and executes in the background under the service root context.
Any other `async` spelling is a `400` with zero durable rows.

New per-run endpoints (UUID double-bound to the parent sync def AND the
trusted tenant; read visibility matches the runs list; cancel requires the
same grant as triggering — owner / same-org admin / `connections:write`):

- `GET /syncs/{id}/runs/{run_id}` — the run projection incl. the P2.2 stage
  fields (`control_mode`, `cancel_requested_at`, `cancelled_by`,
  `publish_entered_at`). Lease-internal values (holder/token) never leave the
  repo layer.
- `POST /syncs/{id}/runs/{run_id}/cancel` — machine-readable outcomes:
  `pending` → `200` terminally `aborted`; `running` with the publish gate
  open → `202` with the request durably persisted (the still-valid holder
  interrupts I/O through a ~1s DB poll and confirms `aborted`); already
  terminal → `200` idempotent; gate already entered → `409` `cancel_too_late`
  (the run completes under its real outcome, never reported as aborted);
  pre-P2.2 rows → `409` `cancel_unsupported_legacy` (their NULL stage columns
  prove nothing about remote publishes). Repeats keep the FIRST
  cancel_requested_at/cancelled_by verbatim.

Cancel-vs-publish fencing: `BeginPublishGate` decides the race atomically
under the run row lock (same two-phase protocol as the lease ops). Cancel
first ⇒ the gate refuses and `CommitOutput` is provably never called; gate
first ⇒ later cancels are observably too-late. The terminal write itself is
stage-guarded in SQL: `succeeded` requires the gate entered and no cancel
won; `aborted` requires the gate NOT entered (legacy rows keep the bare P2.1
CAS). Shutdown/cancel helpers therefore cannot mislabel a gate-entered row.

Execution model: ONE coordinator owns both waiting modes — the default
synchronous handler and the async worker share the same bounded admission
semaphore and the same worker pool (default 1 worker ⇒ peak upstream
concurrency ≤ 1 for both), the same executor, and the same service-lifecycle
tracking. Caller Authorization strings live ONLY in a bounded in-memory
registry (queue payloads carry run handles); the registry is actively swept
at expiry and cleared on every exit path; nothing is persisted or logged.
The overall task deadline and the auth-memory TTL are anchored at Reserve
time (queue wait included) and are never reset by later database latency.
Shutdown order is explicit and testable: signal → close admissions → HTTP
drain (own context) → cancel background roots → coordinator Stop (drain
queue with answers to every waiting handler, cancel executions, bounded
join) → join schedulers → pool close. An incomplete Stop is reported as an
error and the pool is deliberately left to process exit — runs stay
honestly unconfirmed (pending/running, queryable, cancellable), never given
fabricated terminal states. On restart the coordinator adopts NOTHING:
pre-existing pending/running rows are never scanned, claimed or replayed.

Honest limits (unchanged in kind from P2.1): the dataset publish is an HTTP
call not fenced by this database's CAS — no exactly-once publishing is
claimed. A cancel request on an expired-lease run stays an unconfirmed,
queryable request; it is never reported as "already aborted". `TakeoverExpiredSyncRun`
remains storage-only; no scheduler, retry, checkpointing or rate limiting is
enabled (the sync scheduler stays a no-op unless explicitly configured).

Environment overrides (unset keeps the defaults; an explicit value is
validated at startup — `0`, negative or over-max fails to boot, never
silently defaults): `CONNECTOR_SYNC_ASYNC_QUEUE` (capacity for queued +
executing, queued + sync + async combined; default 64),
`CONNECTOR_SYNC_ASYNC_WORKERS` (default 1), `CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS`
(default 300), `CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS` (default 300).

## Per-source sync admission (P2.3)

An OPTIONAL typed `sync.admission` block in a source's config bounds the
ADMISSION of new sync runs for that source — shared by every sync def of the
source, every legal actor, the synchronous and `?async=true` paths, and the
legacy `RunSyncJob` trigger:

```json
"sync": {
  "resource_path": "/items",
  "admission": {"max_inflight_runs": 4, "min_interval_ms": 1500}
}
```

- `max_inflight_runs`: optional explicit integer `1..4096` — at most this many
  `pending + running` runs of the source may exist before a new admission is
  refused (`0`, negatives, out-of-range and non-integer types are rejected
  with `400` and change nothing). Missing = the cap dimension is off.
- `min_interval_ms`: optional integer `0..86400000`; `0` turns the dimension
  off, missing is off. It is the minimum interval between SUCCESSFUL
  admissions of the source, decided on the database clock.
- A missing/null/empty block keeps the exact pre-P2.3 behavior. The block is
  validated on every config write surface (typed rest_api create/update, the
  passthrough types, and the raw auto-registration rewrite) and roundtrips
  verbatim. It is not the webhook invocation limiter and not the per-run
  pagination `sync.limits`.

Decision protocol: a lockless pre-read ONLY picks the lock wait mode
(admission-enabled source → `FOR UPDATE NOWAIT`, everything else → plain
`FOR UPDATE`, preserving the enqueue-vs-delete barrier semantics for
no-policy sources). Under the held source lock the policy is re-read
authoritatively from the row, `pending + running` is counted in SQL and the
DB clock is sampled in the same statement; the run INSERT and the
`connections.sync_last_admitted_at` advance (a private column, never exposed
by the API, never reset by migration replays, never backfilled) commit in
ONE short transaction. Even policy-off admissions stamp the clock, so
enabling `min_interval_ms` later interprets real history. Rejected,
failed-insert and rolled-back admissions change nothing — no run row, no
interval consumed. Capacity frees exactly when a run reaches a real terminal
commit (`pending` abort included); `running` runs with a persisted cancel
request, an expired lease or legacy unknown outcome keep occupying until
their honest terminal state. Canceling an admitted run never refunds the
interval.

Refusals answer `429` with a stable machine contract (no credentials, JWTs
or lease material anywhere in the body):

```json
{"error": "source_sync_throttled", "code": "source_sync_throttled",
 "reason": "capacity | min_interval | contention",
 "message": "...", "retry_after_seconds": 3}
```

`Retry-After` always equals `retry_after_seconds`: `min_interval` carries the
ceiling of the post-lock DB remaining time; `capacity` and `contention`
(the NOWAIT lock statement hit SQLSTATE 55P03) carry an advisory 1-second
hint — a suggestion, not a promise. The local execution reservation is
returned on refusal; queue-full/shutting-down stays `503` and ordinary
database errors / unknown commit outcomes keep their honest 5xx semantics.

`POST /api/v1/webhooks/{source_id}/invoke` executes either this `rest_api`
source-backed webhook model or the legacy absolute-URL webhook config. The
webhook is resolved **within the caller's tenant domain** (the JWT org, or the
caller's own NULL-org rows): a webhook outside the caller's tenant answers
`404` — before any outbound call, history write, or rate-limit consumption —
so cross-tenant callers (admins and grant holders included) cannot confirm a
webhook exists. Within the tenant, the handler enforces source
ownership/invoke permissions (`connections:write` / `webhooks:invoke`), source
`invokable`, allowed HTTP methods, egress host allowlists, timeout, max
call/input/response limits, rate limits, and concurrency limits before
returning sanitized diagnostics and typed `output_parameters`.

Webhook history is persisted when the definition has `history.enabled: true`
or uses the default history settings. `store_inputs` controls whether request
inputs are retained; `store_outputs` controls whether parsed
`output_parameters` are retained. History reads apply the same tenant-first
rule; within the tenant, source owners, admins, or callers with
`connections:read` / `webhooks:read` grants (personal domains stay owner-only)
can inspect retained entries with:

- `GET /api/v1/webhooks/{source_id}/history`
- `GET /api/v1/data-connection/sources/{source_id}/webhook-history`

## HTTPS inbound listeners

Sources can also expose an HTTPS listener for external systems that push events
into OpenFoundry. Listener requests are authenticated by the listener config, so
the receive routes are intentionally mounted outside JWT auth:

- `POST /api/v1/listeners/{source_id}/events`
- `POST /api/v1/data-connection/sources/{source_id}/listeners/{listener_id}/events`

Example source config:

```json
{
  "listener": {
    "id": "trail-events",
    "type": "https",
    "enabled": true,
    "auth": {
      "type": "hmac_sha256",
      "header": "X-OpenFoundry-Signature",
      "secret": "dev-only-local-secret"
    },
    "destination": {
      "mode": "object",
      "object_type_id": "00000000-0000-0000-0000-000000000000"
    },
    "limits": {
      "max_payload_bytes": 1048576
    }
  }
}
```

The receiver accepts JSON payloads, validates either `hmac_sha256`,
`shared_secret`, or `none` auth, redacts auth headers before storage, and writes
an `inbound_listener_events` record with the payload plus destination metadata
(`event_log`, `dataset`, or `object`). Source owners, admins, or callers with
listener read permission can inspect received records with:

- `GET /api/v1/listeners/{source_id}/events`
- `GET /api/v1/data-connection/sources/{source_id}/listener-events`

## Required in production

| Variable | Description |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection string. The service fails to start when absent. |
| `JWT_SECRET` or `OPENFOUNDRY_JWT_SECRET` | JWT signing/validation secret. `OPENFOUNDRY_JWT_SECRET` wins when both are set. The service fails to start when neither is set. |
| `CREDENTIAL_ENCRYPTION_KEY` | Base64 of exactly 32 random bytes (`openssl rand -base64 32`), the independent AES-256-GCM key for credential ciphertext at rest. See the startup gate below. |

### CREDENTIAL_ENCRYPTION_KEY startup gate

The service resolves its credential-encryption key at startup and **fails closed** unless an
independent key is configured:

- **Production and every non-dev environment fail closed.** With
  `OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT` set to `production`/`prod` (the Helm `values-prod.yaml`
  profile), any other value (including the Helm default `shared` and the `staging` /
  `sovereign` profiles), or unset entirely, a missing/blank `CREDENTIAL_ENCRYPTION_KEY` is a
  startup error. `SERVICE_VERSION` (which defaults to `dev`) and the `OPENFOUNDRY_DEV_AUTH`
  shim are deliberately **not** treated as dev signals.
- **Explicit local dev/test keeps the legacy fallback.** Only when
  `OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT` is one of `development`, `dev`, `test`, `testing`, or
  `local` may a missing key fall back to the legacy JWT-derived key
  (`sha256("openfoundry/credential-encryption/v1\0" + JWT_SECRET)`). A configured but invalid
  key (bad base64, wrong length) is still a startup error in every environment.
- **The key must be independent of `JWT_SECRET`.** A key equal to the raw JWT secret bytes or
  to the legacy JWT-derived digest is rejected in every environment — moving the same bytes
  into a new variable name does not make them independent.
- **Errors are safe to log.** Gate failures name only the variable and the policy reason;
  they never echo the key material or the JWT secret.

Rotating or replacing the key **does not re-encrypt existing credential ciphertexts**: rows
encrypted with the old key (including legacy JWT-derived ones) become undecryptable after the
key changes, and the service never auto-rotates ciphertext on an existing database. Before
deploying a new independent key, either keep the previous key material available for
migration or plan a re-entry of stored credentials — do not point a new key at a database
full of old-key ciphertext expecting transparent decryption.

## Rust-compatible runtime defaults

| Variable | Default | Description |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | HTTP bind host. |
| `PORT` | `50088` | HTTP bind port, matching Rust. |
| `DATASET_SERVICE_URL` | `http://localhost:50079` | Dataset-versioning service base URL. |
| `PIPELINE_SERVICE_URL` | `http://localhost:50080` | Pipeline service base URL. |
| `ONTOLOGY_SERVICE_URL` | `http://localhost:50103` | Ontology service base URL. |
| `INGESTION_REPLICATION_GRPC_URL` | empty | Reserved. Snapshot sync no longer dispatches to ingestion-replication (Kafka/Flink reconcilers only); this URL does not affect sync runs. |
| `NETWORK_BOUNDARY_SERVICE_URL` | `http://localhost:50119` | Network boundary service base URL. |
| `SYNC_POLL_INTERVAL_SECS` | `2` | Sync scheduler poll interval. |
| `ALLOW_PRIVATE_NETWORK_EGRESS` | `true` | Allows private-network egress during local/dev bring-up. |
| `ALLOWED_EGRESS_HOSTS` | empty list | Comma-separated host allowlist. Wildcards may be interpreted by later egress logic. |
| `AGENT_STALE_AFTER_SECS` | `120` | Connector agent staleness threshold. |
| `MEDIA_SETS_SERVICE_URL` | `http://localhost:50156` | Media sets service base URL used by media-set sync runtime. |
| `OPENFOUNDRY_VENDED_CREDENTIALS_TTL_SECS` | `900` | Vended credential TTL for Iceberg/catalog integrations. |

## Go-only operational knobs

| Variable | Default | Description |
| --- | --- | --- |
| `SERVICE_VERSION` | `dev` | Version reported by health/observability. Build-time version replaces `dev` in `main`. |
| `METRICS_ADDR` | `0.0.0.0:9090` | Reserved metrics bind address for consistency with other Go services; this service exposes `/metrics` on the main router. |
| `SECRET_MANAGER_URL` | empty | Placeholder for future secret-manager integration. |
| `OUTBOX_ENABLED` | `true` | Placeholder switch for future outbox emission. |

## Dev-only

| Variable | Default | Description |
| --- | --- | --- |
| `OPENFOUNDRY_DEV_AUTH` | `false` | Mounts `/api/v1/auth/*` and `/api/v1/users/me` shim routes when true/`1`. Do not enable in production. |
| `OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS` | `0` | Enables future recurring virtual-table auto-registration worker when greater than zero. |
| `CONNECTOR_SYNC_ASYNC_QUEUE` | `64` | P2.2 run-coordinator capacity (queued + executing, sync + async combined). Explicit out-of-range values fail startup. |
| `CONNECTOR_SYNC_ASYNC_WORKERS` | `1` | P2.2 run-coordinator worker concurrency (bounds upstream concurrency for both waiting modes). |
| `CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS` | `300` | Max time a caller Authorization may live in the in-memory registry (anchored at admission, queue wait included). |
| `CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS` | `300` | Overall task deadline from admission (queue wait included). |

## P2.4 observable scheduled scheduling (business-platform exception to ADR-0025)

`batch_sync_defs` (REST snapshot batch syncs) can now fire on a cron schedule.
This is the **explicit, limited business-platform implementation-path exception
to ADR-0025** recorded in `docs/architecture/business-platform-implementation-plan.md`:
a single DB-claimed scheduler for this one Connector surface, reusing the
repository-owned Foundry-parity cron semantics (`libs/scheduling-cron`). It is
NOT a platform scheduler and does not implement or replace the Temporal
migration. Media-set sync scheduling stays dormant.

Contract and design: `tools/smoke/business-platform-p2-4/design-draft.md` and
`implementation-contract.md` (authoritative signatures).

- Cron: 5 fields = unix5, 6 fields = quartz6 (inferred by field count), IANA
  time zone (`UTC` default; `Local` rejected), expression ≤256 bytes. Legacy
  stored `schedule_cron` values migrate DISABLED with a NULL cursor — no
  backfill, no implicit activation; only explicitly supplied new values are
  validated. Enable requires a valid non-empty cron; empty cron clears and
  disables. Any actual change to cron/time zone/enabled/output target/file
  glob bumps `schedule_revision` (invalidating outstanding schedule grants)
  and recomputes the cursor from the locked DB time.
- Claim protocol: one overdue window per short transaction (Source and Def
  rows both `FOR UPDATE NOWAIT`, internal 5s budget), locked DB clock as the
  only time authority, candidate revision+slot CAS, shared P2.3 admission
  gate, tick+run+cursor committed together, admission stamp last. Missed
  windows coalesce to the single claimed slot; the next cursor is strictly in
  the future. Tick history (`sync_schedule_ticks`) is bounded to the most
  recent 500 rows per def.
- Execution authorization: scheduled runs act under an explicitly re-armed
  trusted caller's identity. `POST /api/v1/data-connection/syncs/{id}/schedule:arm`
  stores the caller Authorization in the PROCESS-LOCAL registry only (never in
  the DB, logs or responses; never a background-signed token) with an absolute
  deadline of min(verified JWT exp, arm time + TTL). The DB stores only
  nonsecret grant metadata for cross-instance adjudication. A live remote
  owner is never preempted; with no live owner, claims honestly record
  `auth_required`. **Arming is per-instance**: restart or expiry requires a
  fresh arm; `DELETE .../schedule:arm` revokes. `GET .../schedule/ticks`
  lists the bounded tick history. `enqueued` proves durable admission only —
  dispatch/execution truth stays on the run.

| Variable | Default | Description |
| --- | --- | --- |
| `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS` | `0` (off) | P2.4 scheduler tick loop; explicit values bounded to 1–30 (out-of-range fails startup). With the loop off, due slots stay unclaimed and no ticks are written. |
| `OPENFOUNDRY_SYNC_SCHEDULER_AUTH_TTL_SECS` | `300` | Per-arm grant memory TTL; the effective deadline is further clamped by the verified caller JWT exp. Explicit values bounded to [1,86400]. |

## P2.5 incremental completion checkpoints (cross-run REST incremental batches)

Opt-in via the source's `sync.incremental` block (`enabled`, fixed
`request_param` carried on EVERY page, `completed_checkpoint_path` on the
FINAL response, optional presence-tracked `initial_value`). The completion
checkpoint is an opaque provider token — never trimmed, never
lexicographically compared, never echoed (errors/logs/metadata carry lengths
only; the API exposes length+sha256). Opaque tokens must be nonempty, ≤512
raw bytes, valid UTF-8, control-free, and may not contain U+FFFD (malformed
wire/config bytes are rejected, never silently normalized — both pinned Go
toolchains replace them during JSON decoding).

**Upstream prerequisite (documented, no generic CDC claim):** the source
must provide an exclusive incremental stream (`request_param` semantics =
"only records after this position") with a stable window across pages;
in-run pagination cursors are never saved as data checkpoints. Sources whose
cursor semantics replay the last page yield at-least-once batches.

**Semantics:** per-SyncDef durable single-flight ownership is taken in the
same admission transaction as the pending run (pending counts); manual 409
`sync_incremental_busy`, scheduled truthful `skipped_incremental_busy` tick
with the CRON cursor advancing and the DATA checkpoint untouched. Each
successful batch publishes an INDEPENDENT `SNAPSHOT` version — no cumulative
merge, no UPSERT, and NOT `APPEND` (the dataset APPEND view merge does not
preserve every file-index read surface). The branch's effective view is the
latest batch; history stays readable run→transaction→file.

**Conservative unknown boundary:** the publish-gate ENTRY. Every owner
release asserts `publish_entered_at IS NULL` in the same transaction as the
terminal write; after gate entry, even an undispatched interruption closes
as honest `failed` with the owner RETAINED. A usable publish ACK requires
exact target echo + `COMMITTED` + non-nil transaction id (adapter AND
handler, injected ports included). Success+advance+release is ONE fenced
transaction (all-or-nothing); a lost local COMMIT response is an honest
unknown (all-old or all-new — the owner may already be cleared), never
repaired or blindly repeated. No automatic replay/takeover for captured
work: unresolved owners await P26 (`GET /syncs/{id}/checkpoint` shows the
owner and its blocked reason).

**Fences:** source config/credential writes (including the bare
auto-registration config surface and `SetCredential`), def target/file_glob
changes, deletes and incremental mode-off edits refuse with
`sync_incremental_owner_unresolved` while any unresolved owner exists
(pure name/status/cron/enable edits are unaffected; credential SAME-value
rotation keeps the binding stable via content fingerprints). A stale binding
has no P25 reset API — recovery is documented as newDef-only.

### PostgreSQL table snapshots

Set `POSTGRES_SYNC_ENDPOINT=postgresql://host:5432/database` for the operator-owned source database. The native pgx driver always verifies the server certificate and hostname against the service's system roots. For a private CA, install its trust root in the connector container. Apply the connector egress policy and provision a source login with SELECT on its table and USAGE on the schema. Configure the source server to require password authentication.

Create a `postgresql` source with `{"sync":{"postgres_table":{"schema":"sales","name":"orders"}}}` and bind a source credential of kind `password` whose value is a JSON string containing `username` and `password`. Native reads use a bounded, read-only repeatable-read transaction, order by the table's primary key, and publish NDJSON through the existing batch sync API. The source supports a single table with a primary key, up to 10,000 rows and 8 MiB of output. Both standalone and partitioned tables are supported. Old catalog-only configurations retain their existing behavior. The source URL, SQL and login secrets are excluded from the source configuration.
