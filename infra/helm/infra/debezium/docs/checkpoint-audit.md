# Checkpoint audit outbox

Enable one relay for the checkpoint events in the policy service's writable database. Check the source of its writer DSN before configuring this chart. If a general outbox connector already publishes these events, coordinate its publication filter and the cutover to avoid delivering the same checkpoint through both connectors. Keep the slot identity stable across restarts.

1. On PostgreSQL 16 with logical WAL enabled, run the [owner bootstrap](../../../../compose/local/postgres-init/03-checkpoint-audit-outbox.sh) against the policy database. Supply the actual primary in `PGHOST`, the database owner in `POSTGRES_USER`, the database name in `CHECKPOINT_AUDIT_DBNAME` and the replication login password in `CHECKPOINT_AUDIT_CDC_PASSWORD` via the deployment's credential mechanism. The script creates or verifies the insert-only publication `of_checkpoint_audit_pub` for this producer and the dedicated login `checkpoint_audit_cdc`. For an existing login supply its current password.
2. Provision a Secret in namespace `kafka` with that password under the key `password`. Set `checkpointAudit.hostname` to the direct primary service, `checkpointAudit.dbname` to the same database, and `checkpointAudit.passwordSecretName` to that Secret. Enable `checkpointAudit.enabled`. The Connect worker picks up the Secret on rollout and Strimzi reconciles the connector.
3. Before sending a checkpoint, verify the connector and its sole task report `RUNNING` and PostgreSQL reports the slot `of_checkpoint_audit_slot` active in the intended database. Submit a checkpoint and match its event ID in `audit.events.v1` and the audit table. Configure the sink's durable writer separately.

Turning the option off removes the connector through GitOps. Check slot lag during an outage and agree how to resume before dropping replication state. The connector reads committed WAL from its slot and has no historical table snapshot.

To verify the chart locally, run `python3 tools/smoke/audit-delivery/test_helm_connector.py` from the repository root with Helm and PyYAML installed.
