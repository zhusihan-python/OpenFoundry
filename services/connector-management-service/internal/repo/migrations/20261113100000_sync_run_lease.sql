-- P2.1 durable task queue + lease columns on sync_runs.
--
-- A manual POST /run now enqueues a *pending* row (durable task) and the
-- executing request claims it atomically (status pending -> running) with a
-- per-claim fencing generation and an unguessable lease token.
--
-- Every lease operation (claim, renew, terminal complete, expired takeover)
-- runs as a short two-phase transaction: (1) SELECT ... FOR UPDATE locks the
-- run row by id + tenant only; (2) AFTER the lock is acquired a separate
-- UPDATE re-checks status/holder/token/generation/expiry using
-- clock_timestamp() and writes the new lease_expires_at from that same
-- post-lock clock. Two-phase is mandatory: in READ COMMITTED a blocked
-- statement only re-evaluates its quals (EvalPlanQual) when the blocker
-- produced a NEW row version — a blocker that holds FOR UPDATE without
-- updating lets the waiting UPDATE proceed with the qual evaluation from
-- BEFORE the wait, so a single-statement clock_timestamp() CAS could renew an
-- lease that expired while it was blocked on the lock.
--
-- Ownership provenance: owner_id snapshots the Source owner (connections.
-- owner_id) at enqueue time; requested_by records the verified JWT subject
-- that triggered the run. They are distinct concepts and never mixed.
--
-- Idempotent by construction: re-running adds nothing and rewrites nothing,
-- so an in-flight lease survives migration replays (the service replays all
-- SQL on every start; there is no migration ledger).
--
-- Legacy rows (pre-P2.1): terminal states, dataset_version_id and
-- content_hash are untouched; legacy `running` rows have lease_expires_at
-- NULL and are therefore NOT takeover candidates — nothing re-publishes them
-- automatically. They remain closable exactly once through the legacy
-- no-token CompleteSyncRun path, whose predicates now require
-- status='running' AND lease_token IS NULL.

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS requested_by UUID,
    ADD COLUMN IF NOT EXISTS owner_id UUID,
    ADD COLUMN IF NOT EXISTS attempt INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS generation INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lease_holder TEXT,
    ADD COLUMN IF NOT EXISTS lease_token TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_sync_runs_claim ON sync_runs(status, generation);
