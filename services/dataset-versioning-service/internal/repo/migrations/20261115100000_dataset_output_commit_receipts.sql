-- P2.6 W1 — idempotent dataset output commits.
--
-- One receipt per (dataset_id, idempotency_key) records the first COMMITTED
-- keyed outputs:commit: the exact request fingerprint (SHA256 over the raw
-- HTTP JSON body, computed by this service) and the immutable response bytes
-- returned to the first caller. Replays and GET
-- /datasets/{rid}/outputs/commits/{key} serve these stored bytes verbatim —
-- never a rebuild from head/index/lineage.
--
-- Semantics:
--   * insert-only: no UPDATE path, no DELETE path besides the dataset ON
--     DELETE CASCADE; a receipt is the durable record of an accepted
--     operation and cannot be rewritten;
--   * response_body is TEXT, not JSONB: JSONB reorders keys, which would
--     break the byte-stable replay contract;
--   * request_fingerprint is a 64-char lowercase hex SHA256 computed by the
--     service — caller-supplied hashes are never trusted or stored here;
--   * the row stores no JWT/subject material: a fresh principal with access
--     to the same dataset may legally replay the same key.
--
-- Backfill: none. Every statement is idempotent so startup replays are no-ops.

CREATE TABLE IF NOT EXISTS dataset_output_commit_receipts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id          UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    idempotency_key     TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    branch_name         TEXT NOT NULL,
    transaction_id      UUID NOT NULL,
    transaction         JSONB NOT NULL,
    response_body       TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT dataset_output_commit_receipts_key_len
        CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    CONSTRAINT dataset_output_commit_receipts_fingerprint_hex
        CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT dataset_output_commit_receipts_unique_key
        UNIQUE (dataset_id, idempotency_key)
);

COMMENT ON TABLE dataset_output_commit_receipts IS
    'Immutable receipts for idempotent outputs:commit requests: first-writer-wins per (dataset_id, idempotency_key), stores exact response bytes for byte-stable replay';
COMMENT ON COLUMN dataset_output_commit_receipts.request_fingerprint IS
    'SHA256 hex over the exact raw HTTP JSON body (including the idempotency_key field), computed by the service';
COMMENT ON COLUMN dataset_output_commit_receipts.response_body IS
    'Exact serialized response bytes returned to the first caller; served verbatim on replay and GET (TEXT keeps JSONB from reordering keys)';
