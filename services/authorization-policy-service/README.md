# authorization-policy-service (Go port)

Go port of the Rust `services/authorization-policy-service` crate (per
ADR-0027). The Rust binary is currently `fn main() {}` (S8 / B14
consolidation pending), so the Go port is the **canonical
implementation**.

## Port status (2026-05-07)

The Go binary is no longer just the Cedar-policy foundation slice. It
wires the consolidated `/api/v1` authorization surface: Cedar policy
CRUD with strict validation, ABAC policies and `/policy-evaluations`,
RBAC roles/groups/permissions, governance-template applications,
project constraints, structural-security rules, checkpoints/purpose
records, cipher channels/licenses, and network-boundary resources.

The latest stub scan found **no productive `StatusNotImplemented` or
placeholder handler matches** in this service. Test-only matches live in
`*_test.go` and are excluded from the production scan.
## Implemented surface

Cedar policy CRUD over Postgres with strict schema validation via
`libs/authz-cedar-go` before every write. Optional NATS publish on
`authz.policy.changed` so peer services hot-reload. The service also mounts the
Rust top-level authorization surface: tenant-scoped ABAC policies/evaluation and
RBAC roles/groups/permissions.

Endpoints (all under `/api/v1`, JWT-protected):

- `GET    /cedar-policies`             — list (500 most-recent)
- `POST   /cedar-policies`             — create (validates against bundled schema)
- `GET    /cedar-policies/{id}`        — fetch
- `PATCH  /cedar-policies/{id}`        — partial update; bumps `version` on `source` change, re-validates
- `DELETE /cedar-policies/{id}`        — delete
- `GET    /abac-policies` / `POST /abac-policies` / `GET|PATCH|DELETE /abac-policies/{id}` — tenant-scoped ABAC policy catalog
- `POST   /policy-evaluations`         — Cedar/ABAC access decision evaluation
- `GET    /permissions` / `POST /permissions` — authorization permission catalog
- `GET|POST /roles`, `GET|PUT|PATCH|DELETE /roles/{id}` — role CRUD with permission grants in `permission_ids`
- `GET|POST /groups`, `GET|PUT|PATCH|DELETE /groups/{id}` — group CRUD with group→role grants in `role_ids`
- `POST   /users/{id}/roles`, `DELETE /users/{id}/roles/{role_id}` — user-role grants
- `POST   /users/{id}/groups`, `DELETE /users/{id}/groups/{group_id}` — membership

Plus `/healthz` + `/metrics`.

## RBAC and restricted-view ownership decisions

Identity-federation also exposes identity-local RBAC for users, login/session
administration, API keys, and SCIM group provisioning. To avoid duplicating the
wrong source of truth, authorization-policy-service owns only authorization
policy RBAC: tenant-scoped roles, groups, permissions, membership, user-role
grants, and group→role grants that protect this service's policy/evaluation
surface. The route audit test in `internal/server/rbac_routes_test.go` locks that
top-level RBAC surface in this service.

Restricted-view CRUD is consolidated in `identity-federation-service` because
restricted views are CBAC/session-scoping configuration authored alongside
identity claims, SCIM groups, and scoped sessions. This service intentionally does
not expose `/api/v1/restricted-views`; instead, `POST /api/v1/policy-evaluations`
reads enabled `restricted_views` rows and applies their row filters, hidden
columns, allowed organization IDs, markings, guest access, and consumer-mode
settings during ABAC evaluation.

## Configuration

| Env var                                  | Required | Default        |
|------------------------------------------|----------|----------------|
| `DATABASE_URL`                           | yes      | —              |
| `OPENFOUNDRY_JWT_SECRET` / `JWT_SECRET`  | yes      | —              |
| `NATS_URL`                               | no       | unset (no publish) |
| `HOST`                                   | no       | `0.0.0.0`      |
| `PORT`                                   | no       | `50115`        |
| `METRICS_ADDR`                           | no       | `0.0.0.0:9090` |
| `SERVICE_VERSION`                        | no       | `dev`          |

When `NATS_URL` is set, every successful CRUD write publishes to
`authz.policy.changed` (the subject `libs/authz-cedar-go`'s
`PolicyReloadSubscriber` listens on by default).

## Schema

`internal/repo/migrations/0001_cedar_policies.sql` creates the
`cedar_policies` table read by `libs/authz-cedar-go/pg.go` (per
ADR-0027). Column set is locked to that loader's `SELECT` — changing
either side breaks the contract.

```sql
cedar_policies (
    id          TEXT     PRIMARY KEY,
    version     INTEGER  NOT NULL,
    source      TEXT     NOT NULL,
    description TEXT,
    active      BOOLEAN  NOT NULL DEFAULT TRUE,
    created_by  UUID     NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

## Wire format (locked)

`CedarPolicy`:

```json
{
  "id": "permit-cleared-readers",
  "version": 1,
  "source": "permit(principal, action == Action::\"read\", resource is Dataset);",
  "description": "view dataset when clearance covers markings",
  "active": true,
  "created_by": "11111111-1111-1111-1111-111111111111",
  "created_at": "2026-05-06T00:00:00Z",
  "updated_at": "2026-05-06T00:00:00Z"
}
```

List envelope: `{"items": [...]}` (matches the foundation-slice
convention used by organizations + enrollments).

## Validation contract

Every write (POST + PATCH-with-`source`) runs through a fresh in-memory
`PolicyStore` from `libs/authz-cedar-go` before the SQL INSERT/UPDATE.
A bad source therefore fails with `400 Bad Request` and the row is
never persisted. The active validator state is hermetic per request
so concurrent malformed writes can't poison one another.

## Remaining migration notes

Per the [stub audit](../../docs/archive/STUB-AUDIT.md), this service currently has no
productive stub matches in production Go files. Keep future work focused
on conformance depth (Cedar/ABAC/RBAC fixtures and cross-service policy
reload behavior), not on replacing placeholder route handlers.
## Remaining follow-up slices

Per the [INVENTORY](../../docs/archive/INVENTORY-authorization-policy-service.md) (archived), the
remaining gap is broader Cedar conformance coverage. Top-level RBAC, restricted
view evaluation, ABAC evaluation, governance, checkpoint/purpose, cipher, and
network-boundary routes are mounted or intentionally consolidated as documented
above.

## Build / test

```sh
cd openfoundry-go
go build ./services/authorization-policy-service/...
go test -race ./services/authorization-policy-service/...
```

## Smoke

```sh
go run ./services/authorization-policy-service/cmd/authorization-policy-service
# → ERROR config load failed error="required environment variable DATABASE_URL is not set"
```

### Runtime chat purpose decisions

`POST /api/v1/checkpoints/purpose/enforce` evaluates the configured
`ai_chat_completion` policy before a runtime provider call. The service JWT
must carry `auth_methods: ["service"]` and the explicit
`purpose-checkpoint:enforce` permission. Provision this credential only for
runtimes trusted to supply the authenticated actor, operation, and selected
provider's network scope. Migration 0008 registers permission names. The
operator assigns the credential and permissions.

The seeded `ai-private-network` policy requires a selected private, local or
hybrid route and a justification of at least 20 Unicode characters. The
ledger accepts up to 1,024 characters of independent business justification.
The route accepts chat completions and copilot requests. Missing or
unsupported policy configuration and storage failures return 503. A completed
evaluation returns HTTP 200 and a receipt with its approval decision. A
client must honor the decision before making the provider call.

Each evaluated row has a trusted `enforced_by` service subject and a separate
`actor_id`. Only these subjects and callers explicitly granted
`purpose-checkpoint:read` can list those rows. The earlier manual-record API
continues to create rows with a null enforced subject. The service commits
its receipt and `purpose_checkpoint.evaluated` audit outbox event in the same
PostgreSQL transaction. The ledger retains the independent justification.
The event retains the route, reason, length and digest. Chat messages, model
replies and credentials are excluded. Shipping outbox events beyond
PostgreSQL requires a configured CDC consumer.

On an existing Helm `pg-policy` cluster, the outbox owner applies
`infra/helm/infra/postgres-clusters/files/pg-policy-checkpoint-outbox-grant.sql`
to the application database before rollout. New clusters apply the same
grant during initdb. It adds column-level `SELECT (event_id)` to
`svc_checkpoints_purpose`, whose existing schema `USAGE` and table
`INSERT, DELETE` grants support enqueue. The service writer DSN must use
this role or an equivalently scoped producer on the same database.
Migration 0008 uses the platform-owned ADR-0022 table with replica identity
`FULL`. On a fresh standalone database, it creates the outbox.
