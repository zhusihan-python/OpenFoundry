-- WP3.3 increment 3A — durable descriptors for staged output objects.
--
-- One row per reservation implements the single-artifact staging model
-- (UNIQUE (reservation_id)): at most one in-flight upload (status STAGING
-- guarded by lease_expires_at) and at most one live sealed artifact
-- (status SEALED; CONSUMED is terminal once a staged commit published it).
-- There is deliberately no epoch/supersede chain: a byte-identical
-- re-transmission reuses the sealed descriptor, different content is an
-- explicit conflict and requires a fresh reservation.
--
-- Semantics:
--   * STAGING rows exist only while one upload is streaming (or after its
--     descriptor transaction's COMMIT outcome stayed unknown); an expired
--     lease may be taken over, and takeover is the only path that clears
--     unreferenced staging files for the reservation;
--   * size/sha256/record_count/sealed_path/expires_at are meaningful only
--     once status = SEALED; sha256 is a 64-char lowercase hex digest;
--   * sealed_path is server-private (relative to the private staging root)
--     and is never exposed through any public route;
--   * insert + state machine only: no UPDATE rewrites a SEALED or CONSUMED
--     row's binding columns (dataset/branch/reservation/build/actor/key/
--     logical_path), and there is no DELETE besides the dataset ON DELETE
--     CASCADE and staging-lease takeover of a dead STAGING attempt.
--
-- Backfill: none. Every statement is idempotent so startup replays are no-ops.

CREATE TABLE IF NOT EXISTS dataset_staged_output_objects (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id              UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    branch_name             TEXT NOT NULL,
    reservation_id          UUID NOT NULL,
    build_id                UUID NOT NULL,
    actor_id                UUID NOT NULL,
    idempotency_key         TEXT NOT NULL,
    logical_path            TEXT NOT NULL,
    status                  TEXT NOT NULL,
    lease_expires_at        TIMESTAMPTZ,
    size_bytes              BIGINT NOT NULL DEFAULT 0,
    sha256                  TEXT NOT NULL DEFAULT '',
    record_count            BIGINT NOT NULL DEFAULT 0,
    sealed_path             TEXT NOT NULL DEFAULT '',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sealed_at               TIMESTAMPTZ,
    expires_at              TIMESTAMPTZ,
    consumed_at             TIMESTAMPTZ,
    consumed_by_transaction UUID,
    CONSTRAINT dataset_staged_output_objects_status
        CHECK (status IN ('STAGING','SEALED','CONSUMED')),
    CONSTRAINT dataset_staged_output_objects_staging_lease
        CHECK (status <> 'STAGING' OR lease_expires_at IS NOT NULL),
    CONSTRAINT dataset_staged_output_objects_sealed_shape
        CHECK (status = 'STAGING' OR (sha256 ~ '^[0-9a-f]{64}$'
            AND sealed_path <> '' AND expires_at IS NOT NULL AND sealed_at IS NOT NULL)),
    CONSTRAINT dataset_staged_output_objects_key_len
        CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    CONSTRAINT dataset_staged_output_objects_size_nonneg
        CHECK (size_bytes >= 0 AND record_count >= 0),
    CONSTRAINT dataset_staged_output_objects_one_per_reservation
        UNIQUE (reservation_id)
);

CREATE INDEX IF NOT EXISTS dataset_staged_output_objects_dataset_status
    ON dataset_staged_output_objects (dataset_id, status);

COMMENT ON TABLE dataset_staged_output_objects IS
    'Single-artifact staging descriptors for WP3.3 staged output objects: one in-flight upload and one live sealed object per reservation, sealed digests are the authoritative size/hash for STAGED_ADD commits';
COMMENT ON COLUMN dataset_staged_output_objects.sealed_path IS
    'Server-private path relative to the private staging root; never exposed to callers';
COMMENT ON COLUMN dataset_staged_output_objects.lease_expires_at IS
    'Takeover deadline while status = STAGING; a live sealed artifact is never leased';
