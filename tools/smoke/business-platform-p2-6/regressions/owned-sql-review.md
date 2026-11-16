# Root review: exact runtime SQL binding

Implementation: NEW `adapters/w10_owned_sql.py`; no copied source patches.
Every UUID comes from this invocation's actual successful authenticated HTTP
create receipt. Registry records safe method/path/status/id/name/actor/tenant;
Def registration additionally requires same-actor/same-tenant Source and target
created here. Run registration uses an actual authenticated Run list under that
new Def, requiring run parent/org/requested_by equality. Arbitrary UUID, prior
handles, old9/8, and another actor never enter this registry. A scoped spec loader
instruments only NEW copied P1 `http`; it preserves response and original business
assertions. Dynamic JWTs are added to W8 evidence redactor in memory.

| Root enabled template | Exact operation and purpose | Dynamic object evidence | Bounds and cleanup |
|---|---|---|---|
| `p1-new-target-check` | `ALTER TABLE dataset_files DROP CONSTRAINT IF EXISTS p1_smoke_block_publish; ALTER TABLE dataset_files ADD CONSTRAINT p1_smoke_block_publish CHECK (NOT (dataset_id = 'NEW_ID'::uuid AND logical_path LIKE 'sync-%')) NOT VALID;` forces NEW target publish failure | Actual Dataset POST201/200 receipt, actual actor/tenant and new target UUID | Requires `serial_owned_window:true`; before reversible global DDL actual SELECT proves zero pending/running Runs across this invocation's owned Defs; original P1 finally drops only this constraint. Pending cleanup persists if DROP child fails and fails wrapper result |
| `p21-new-run-expiry` | `UPDATE sync_runs SET lease_expires_at = NOW() - INTERVAL '5 seconds' WHERE id = 'NEW_RUN'::uuid;` proves lease-loss fencing | Actual Run list under actual-created Def+Source+target, same org/requested_by | Exactly this new Run ID; no generic UPDATE or old run ID accepted |
| `p21-new-def-terminal-trigger` | Exact copied `CREATE OR REPLACE FUNCTION p21_c_terminal_fault` body matches `NEW.sync_def_id::text = 'NEW_DEF'` and terminal statuses, then exact trigger | Actual Def POST response with same-tenant/source/output creation chain | Requires serial window and actual zero own inflight before install. Global trigger only raises for this NEW Def. Original finally exact DROP trigger/function; failed cleanup remains pending and wrapper fails |
| `p23-owned-source-nowait` | Exact `SELECT id FROM connections WHERE id = 'NEW_SOURCE'::uuid FOR UPDATE NOWAIT;` | Actual Source POST response from this invocation; source actor/tenant retained | Explicit `BEGIN;` + original SELECT + `ROLLBACK;`, outside enforced read-only transaction. No DML. Old Source UUID cannot be registered/adopted |

Only Root release `owned_sql_templates` enables these names. Unmatched SQL uses
Postgres `BEGIN READ ONLY; original SQL; COMMIT;` even if SELECT calls a mutating
function. Optional Root exact argv SHA allowlist uses canonical JSON argv bytes
and is separately explicit; never a generic prefix permission.

Every actual argv is written before spawn. Actual child rc/stdout/stderr remains
original and scrubbed; spawn/timeout has `.rc=unavailable`, metadata
`original_rc:null`, not a fabricated child exit. Code-pure tests evaluate the five
ABC original SQL f-string expressions from the NEW copied source and invoke the
actual copied P1 CHECK generation with a fake psql sink; they prove exact templates,
unknown/foreign UUID denial, cleanup failure retention, and Source NOWAIT bound.

The current zero-inflight read scopes all actual-created Defs from this suite.
Root must additionally schedule these serial windows after other NEW worker
activity has stopped; this boolean is scheduling authority, not claimed live
quiet evidence for external activity. Initial whole-set guard still rejects any
unrostered controlled/CP extra. No old running unknown is cancelled or cleared.

P22 N old-C cancellation is forbidden even if expected409. Adapter now requires
Root `legacy_cancel_probe` matching a typed NEW `legacy-null`/running row and
Run/Def/Source/tenant/actor, then rebinds only NEW module's
`P21_LEGACY_RUNS.C_committed_unconfirmed` before executing original N assertions.
Without this binding a full11 run refuses. Setup can use an extra copied P21
C-only phase (`--only C`) to create NEW committed/unconfirmed sample; it is not
credited as finalABC3. Record its actual full27/protocol/parent/public baseline
into typed ledger before P22. FinalABC3 remains the last business-write batch.
Only the selected NEW sample gets the expected unsupported cancel attempt.

Correction after actual cancel.go read: N unsupported branch requires `control_mode=legacy`, independently of `recovery_protocol=NULL`. P21 C-only creates controlled/NULL and CANNOT supply this probe. Full11 requires actual NEW legacy fixture with real HTTP-created parent chain and a separately Root-authorized NEW-row-only setup; no such setup has run. Typed guard additionally requires Root `legacy_whole_baseline_27` from pre-setup whole-legacy read and compares old+NEW whole legacy set, including shadows. No probe or baseline means full11 refuses.

NEW `adapters/p22_legacy_probe_setup.py` is the minimum INSERT proposal, with no
CLI/live entrypoint. `insert_new_fixture` requires the explicit Root stage
`p22-new-legacy-probe-setup`, `allow_new_legacy_fixture_insert:true`, and a registry
already populated by this invocation's actual Source/target/Def HTTP creates.
It generates a fresh Run UUID and `INSERT ... SELECT` verifies Def→Source→target
and actor/tenant in Postgres, with control_mode legacy, recovery protocol NULL,
all stage/lease fields NULL. No UPDATE, legacy conversion, old C reuse, or cleanup
of unknown rows. Actual full safe27/protocol/parent/public and whole-legacy before/
after collection plus Root acceptance are still required before this sample may
be used by N. Current delivery has not called this setup or any actual SQL.
