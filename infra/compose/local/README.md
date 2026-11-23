# Local Runtime Assets

The root `compose.yaml` includes `infra/compose/docker-compose.yml`. For the dev overlay, use `docker compose -f infra/compose/docker-compose.yml -f infra/compose/docker-compose.dev.yml`.

| Path | Consumed by |
| --- | --- |
| `postgres-init/` | PostgreSQL image entrypoint on the first start of a volume |
| `debezium/checkpoint-audit-outbox.json` | `checkpoint-audit-connect-init` registers the policy audit connector |
| `nginx/` | `nginx` app-profile edge proxy |

## Checkpoint audit CDC

Compose's authorization-policy service writes to `openfoundry_auth_service`. To enable audit delivery locally, inject a strong secret as `OPENFOUNDRY_CHECKPOINT_AUDIT_CDC_PASSWORD` into the Compose environment and retain it for restarts. The PostgreSQL initializer and Connect worker use the same secret. Without it a fresh stack skips this role, publication and connector.

1. Keep client checkpoint traffic paused during the first cutover. Start PostgreSQL with `docker compose up -d --wait postgres`. The default database list includes `openfoundry_auth_service`. For an existing volume the database owner must first ensure that database exists, then run `docker compose exec -T postgres bash /docker-entrypoint-initdb.d/03-checkpoint-audit-outbox.sh`. The same command can be repeated. It preserves the current role password. For a rotation, coordinate the owner-side role password change with the worker's injected secret.
2. Run `docker compose up -d --wait debezium-connect`, then `docker compose run --rm checkpoint-audit-connect-init`. Verify that both connector and task report `RUNNING` at `/connectors/checkpoint-audit-outbox/status` on the Connect endpoint. On the source database verify that `of_checkpoint_audit_slot` is active, uses `pgoutput`, and belongs to `openfoundry_auth_service`. The database owner predefines the insert-only, audit-topic-filtered publication. Connect creates the dedicated replication slot at first connection.
3. Start the policy service and sink. Submit a synthetic authenticated checkpoint, then match its receipt and event IDs through Kafka topic `audit.events.v1` and an independent read of the audit table before admitting regular traffic. The first slot can only capture events from its creation. Earlier checkpoint transactions have already deleted their outbox rows. A restart reuses the existing slot and Connect offsets.

Monitor `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` and `active` in `pg_replication_slots` for `of_checkpoint_audit_slot`. A stopped consumer can retain WAL. For a planned retirement, quiesce the producer, confirm the final checkpoint has reached the table, remove this connector from Connect, and have the owner release its inactive slot and retire its publication and login. Unsetting the opt-in secret only skips the bootstrap step. The original `pg-policy-outbox` uses a separate database and slot.

### Helm deployment

The Helm `outbox-pg-policy` connector is paused in source. Its template connects to `pg-policy-rw.pg-policy.svc/openfoundry`; the CloudNativePG chart bootstraps database `app` in the release namespace. Before a cluster cutover, obtain the deployed policy writer's primary host and database from the service's resolved configuration, verify its actual outbox and capture scope with the owner, and confirm the active Connect secret provider, Kafka topic permissions and payload encoding. Select one relay for these events. Run the same checkpoint-through-table check on that cluster before admitting traffic.
