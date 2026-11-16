# P2.4 Real-PostgreSQL Regression Test Plan (pg-test-plan.md)

Status: **PG TEST FILE FROZEN — 13 top-level names, no further additions.**
Independent review accepted rounds 044158/044227 (13× exact 1 RUN/1 PASS,
0 SKIP/0 FAIL, go1.26.3/go1.25.14, rc 0, no race, same content hash); the
follow-up rounds carrying the root-directed subtests are equally green:
new-normal `new-normal_20261114T044537Z` rc=0 and new-race
`new-race_20261114T044607Z` rc=0 (each 13/13/0/0; the exact-equality
window_too_short subtest and both lockedDef-drift CAS variants PASS inside
both). schedule.go:511 priority confirmed: same-ID local arm expiry maps to
window_too_short even when the DB expiry is identical — grant_expired stays
the arm-less DB-row truth. The isolated fixture stays UP (never downed);
`unified` (old59+13=72, one fresh log) waits for root's unified release after
the primary's test-only freeze. q3v9/HTTP/swap untouched.

Author: ZCode GLM-5.3 (PG-test track). Exclusive artifacts of this track:
this file + `schedule_integration_test.go`. Everything else in
`tools/smoke/business-platform-p2-4/` (design-draft.md, pins/guards/smoke/
preflight scripts, results/) belongs to the business or tools tracks and is
untouched. Production source, the frozen 59 tests/assertions, and old guards
are never modified by this track.

Authority chain (newest wins on conflicts):
1. P23 completion handoff (2026-11-14) — baseline facts.
2. ROOT review contract `handoffs/2026-11-14-p24/review-contract.md`
   (2026-11-14) — mandatory corrections; IMPLEMENTATION AUTHORIZED.
3. Business `design-draft.md` v2 (read-only pre-review DONE, see §6a).
4. `implementation-contract.md` — PENDING; the exact-signature alignment
   source for this track before any test function is written.

Revision 3 (2026-11-14): matrix rebuilt against the root contract + draft v2
DDL/protocol; v1-draft rejected items are dead (no-epoch grants, budget
`>=300` rule, blanket 23505 swallow, stamp-not-last, "fired" status). Revision
2 carried the time-contract corrections (scan-hint `now`; post-lock DB clock
authoritative) which the root contract then confirmed as correction #1.

Completion criteria (mechanically checkable):
(a) the four frozen guard sources of the 59 mandatory names are named (§1);
(b) every mandated scenario maps to a concrete deterministic design citing
    real helpers and now-concrete schema objects (§4);
(c) the implementation-contract blockers are enumerated (§6b);
(d) this track's only repo changes are this file and
    `schedule_integration_test.go` (git untracked-files check); compile
    evidence with real rc lives under /private/tmp/openfoundry-p24-repair-evidence/pg/.

---

## 1. Frozen baseline: the 59 mandatory names (unchanged, must stay green)

P2.4 must NOT touch any existing test file or guard. The final official P2.4
log must show, in ONE fresh migrate-tests run, every one of the 59 names with
exactly 1 top-level `=== RUN`, 1 `--- PASS`, 0 SKIP, 0 FAIL (distinct
top-level names; child `name/sub` never counts; SKIP > FAIL priority; no
required SKIP tolerated).

Four frozen guard sources (verified 2026-11-14):

| Guard | Count | Source file |
|---|---|---|
| P1 dataset 6 | 6 | `tools/smoke/business-platform-p1/run_tests.sh` `REQUIRED_DATASET` (line 19) |
| P1 pipeline 17 | 17 | `tools/smoke/business-platform-p1/run_tests.sh` `REQUIRED_PIPELINE` (line 20) |
| P1 connector 3 | 3 | `tools/smoke/business-platform-p1/run_tests.sh` `REQUIRED_CONNECTOR` (line 23) |
| P2.1 lease 14 | 14 | `tools/smoke/business-platform-p2-1/run_tests_p21.sh` `REQUIRED_LEASE` (line 14) |
| P2.2 cancel/migrate 12 | 12 | `tools/smoke/business-platform-p2-2/p22-required-names.txt` |
| P2.3 admission 7 | 7 | `tools/smoke/business-platform-p2-3/p23-required-names.txt` |
| **Total** | **59** | |

Cross-check: official P2.3 log `.../2026-11-14-p23-final/05-pg59/migrate-tests-20261114T053759.log`
holds 957 distinct top-level names (whole packages compile+run); the 59 are
the guard-selected subset. The P2.4 exact checker keeps that distinction.

New P2.4 names: draft list in §5 (14 provisional) reported to the user for
the tools track's `p24-required-names.txt`; final count = actually frozen
names; no padding. Honest-at-pinning discipline identical to
`p23-required-names.txt` (no RUN/PASS claims before a real log).

## 2. Contract-pinned facts the tests build on (draft v2 §5/§6, root contract)

- TIME (root correction #1): caller/worker `now` = scan hint ONLY. After
  Source AND Def locks, fresh `clock_timestamp()` (draft: `dbNow`) is the sole
  authority for due / expiry / next-fire / admission. Claim carries the
  candidate's expected revision + scheduled_for; post-lock mismatch ⇒ clean
  stale_candidate rejection. A future-injected hint can never fire early.
- LOCK ORDER (draft §6, pinned): claim tx = Source FOR UPDATE **NOWAIT
  (scheduler side ALWAYS, including policy-off sources)** → Def FOR UPDATE →
  post-lock re-read + `dbNow` → adjudicate → run INSERT → cursor/tick/prune →
  `advanceSyncLastAdmitted` as the LAST statement → COMMIT. arm/revoke tx =
  Def FOR UPDATE only (same side as UpdateSyncJob; no cycle with claim's
  Source-first). 55P03 ⇒ tx aborted, bounded rollback, contention counter,
  NO rows/cursor/stamp. 23505 handling matches constraint
  `sync_schedule_ticks_slot_uniq` ONLY.
- SCHEMA (draft §5, migration `20261115120000_sync_schedule_observability.sql`,
  timestamp per implementation day; idempotent batch replay, no CONCURRENTLY,
  no backfill): `batch_sync_defs` gains schedule_time_zone (UTC default) /
  schedule_enabled (FALSE default) / schedule_revision (1 default) /
  schedule_next_fire_at / schedule_last_fire_at / schedule_last_tick_at /
  schedule_last_tick_outcome; partial index `idx_batch_sync_defs_schedule_due`
  on next_fire_at WHERE enabled; new `sync_schedule_ticks` (UNIQUE
  `sync_schedule_ticks_slot_uniq (sync_def_id, schedule_revision,
  scheduled_for)`, detail ≤256 CHECK, run_id FK, actor, reason); new
  `sync_schedule_grants` (PK sync_def_id, per-arm grant_id, revision, boot_id,
  actor_subject, org_id/owner_subject tenant snapshot, source/output expected
  binding, armed_at, expires_at arm-writable only, last_seen_at). Legacy
  `schedule_cron` rows stay via column defaults ⇒ disabled + NULL cursor, no
  UPDATE, no implicit activation. `sync_runs` untouched.
- TICK OUTCOMES (draft §8): `enqueued` (durable admission, NOT proven
  dispatch; consumes stamp), `dispatch_failed` (post-hoc unique-key patch,
  run_id kept, cursor/stamp untouched), `auth_required` (reasons never_armed /
  expired / revoked / stale_revision / owner_stale / window_too_short; cursor
  advances, stamp NOT consumed), `skipped_admission_capacity` /
  `skipped_admission_min_interval` (P2.3 typed refusals; cursor advances,
  stamp NOT consumed), `error_invalid_schedule` (no run; cursor set NULL,
  halted pending repair). Out-of-row aggregate counters: contention /
  local_full / remote_owner / stale_candidate / not_due / not_visible
  (in-process; NOT PG-observable — PG tests assert zero rows + prompt
  bounded return + later-source progress instead).
- CONFIG SEMANTICS: create revision=1; ACTUAL changes to cron/timezone/
  enabled/output/file_glob bump revision (same-value patches do NOT); disable
  ⇒ cursor NULL; enable/config-change ⇒ future cursor recomputed from locked
  DB time; empty cron explicitly clears+disables (bumps); omitted PATCH
  fields preserve; enable requires valid nonempty cron; explicit new
  cron/timezone validated even when disabled; legacy invalid strings survive
  name-only edits; cron flavor inferred by field count (Unix5/Quartz6),
  length ≤256; UTC default + IANA tz via in-repo `libs/scheduling-cron`.
- IDENTITY: per-arm fresh random grant_id/epoch; independent schedule boot
  UUID (never lease holder/hostname/token); arm captures the verified caller
  tenant snapshot — claim runs under THAT tenant, parent chain re-checked
  post-lock; heartbeat updates only last_seen_at with exact-generation match
  and DB clock, never extends expires_at.
- Dispatch/deadline/registry-memory/Stop/JWT mechanics are PRIMARY-unit
  territory (root contract §40); this file covers only their DB-observable
  residue (grant rows, tick rows, run rows, no-residue guarantees).

## 3. Deterministic-concurrency inventory (frozen files — reused, never edited)

All under `internal/repo/`, build tag `integration`:

| Mechanism | Host | Use |
|---|---|---|
| `bootRepo`/`bootRepoWithDSN`/`freshDSN` | `repo_integration_test.go:32-82` | fresh DB per test (CONNECTOR_DB_DSN or testcontainers) |
| `p23SourceBarrier` (+release/commit/boundedRelease) | `source_admission_integration_test.go:234-284` | connections-row FOR UPDATE barrier, EvalPlanQual-proof |
| `singleConnRepo` / `backendPID` | `lease_integration_test.go:62,76` | one op = one backend PID |
| `waitUntilBlockedBy` / `waitBackendLocksCleared` | `lease_integration_test.go:87` / `migration_concurrency_integration_test.go:301` | lock-queue truth |
| `p23WaitAdvisoryQueued` | `source_admission_integration_test.go:509` | advisory park proof |
| `p23WaitClockPast` / `dbClock` | `:401` / `lease_integration_test.go:111` | bounded DB-clock polls |
| `p23Op[T]` / `mustRecv` / `p23RegisterCleanup` | `:309-342` / `cancel_integration_test.go:141` | release-before-cleanup LIFO, bounded joins |
| BEFORE INSERT marker trigger | `:1760-1790` | deterministic in-tx failure injection |
| `tableRowJSON` / `snapshotSyncRun` / `assertSyncRunPersisted` | `lease_integration_test.go:129` / `migration_concurrency_integration_test.go:353-372` | byte-for-byte preservation |
| `p23ApplyMigrationsExceptAdmission` pattern | `:436` | body-scan pre-P2.x database |
| `p23StripSQLComments` + CONCURRENTLY check | `:478` | serialized-batch form proof |
| `p23Stamp`/`p23MustStamp`/`p23AssertStampUnchanged`/`p23ActiveCount` | `:141-169,534` | stamp/occupancy truth |
| `leaseEnqueue` / `BeginPublishGate` / `RequestCancelSyncRun` / `CompleteLeasedSyncRun` | lease/cancel files | occupancy matrix builders |

New p24 helpers (already in `schedule_integration_test.go`, compile-verified):
`p24RowBarrier` (allowlisted connections/batch_sync_defs FOR UPDATE barrier
with the p23 discipline), `p24ApplyMigrationsExcept(t, pool, marker)`
(generic body-scan; marker = `sync_schedule_ticks` per draft §5),
`p24Go[T]` (generic single repo call under `p23Op`).

## 4. Mandated matrix → deterministic designs (schema now concrete)

File owner (exclusive): `internal/repo/schedule_integration_test.go`.
Techniques per root contract + user mandate: post-lock clock, same-slot real
two-pool races, generation/tenant/output/revision CAS, every-stage rollback,
per-source progress/fairness, migration replay immutability. No sleep-guessing
anywhere: barrier handshakes, lock-queue truth, bounded DB-clock polls,
marker-gated triggers, explicit fixture SQL.

### T1 Locked clock & candidate CAS (root #1)
- Fixture: enabled def, cursor past (`schedule_next_fire_at` set past via
  direct SQL or config API), revision R. Candidate struct fed to claim with
  (R, slot); also stale variants: R−1/R+1, slot±ε, NULL-cursor mismatch
  (`IS DISTINCT FROM` semantics: candidate slot vs actual NULL).
- Barrier proof of post-lock authority: hold Def lock (p24RowBarrier) past
  scan; claim parked (`waitUntilBlockedBy`); while parked, mutate cursor far
  future via a second session; release ⇒ claim's post-lock re-read sees the
  future cursor ⇒ not_due, zero rows. Conversely mutate to past ⇒ fires.
- Future-hint negative: pass a far-future `now` hint with a due row ⇒ still
  fires (hint must not suppress); pass future hint with not-due row ⇒ never
  fires. Deterministic: no waits needed, just the return classification.
- dbNow anchoring of new cursor: new `schedule_next_fire_at` and
  `last_fire_at`/tick `claimed_at` must be ≥ barrier-release DB clock (prove
  write-time clock, mirroring P2.3's insert-barrier stamp anchor test with
  `p23InsertBarrierKey`-style advisory park inside the claim tx if an
  in-tx hook exists; otherwise assert `claimed_at ≥ dbNow sampled at park
  release`).

### T2 Same slot, two real pools ⇒ exactly 1 tick + 1 run (root #1/#3)
- Two `singleConnRepo` sessions claim the same (def, R, scheduled_for).
  Primary: Def FOR UPDATE — loser parks (proven), winner commits tick
  `enqueued` + pending run + cursor; loser re-reads post-lock ⇒ clean no-op
  (stale/not-due by advanced cursor), ZERO rows. Both orders via barrier.
- Last defense: defeat the lock ordering deterministically by pre-inserting
  the tick row for the exact slot (fixture SQL), then claim ⇒ hits
  `sync_schedule_ticks_slot_uniq`; assert the code path treats ONLY that
  constraint as benign duplicate (compare by pgconn constraint name) and
  commits no run; a DIFFERENT unique violation (fixture: second unique index
  or marker trigger raising 23505 with another constraint name) must surface
  as an honest error, never be swallowed.

### T3 Bounded loop & fair progress (root #2)
- Hot head: k defs whose SOURCE rows are barrier-held (NOWAIT ⇒ 55P03 fast);
  one later eligible def. One loop/batch pass must return promptly (elapsed
  bound ≪ lock_timeout; assert < 3s like P2.3's NOWAIT test) having claimed
  the eligible def; no rows/cursor/stamp changes on the hot defs.
- Policy-off hot source: same shape with admission-policy absent — the
  scheduler still uses NOWAIT (contract), so a held row is fast contention,
  NOT a blocking wait (assert elapsed; P2.3's manual blocking semantics are
  separately proven untouched by the frozen 59).
- Fairness/starvation: contended/remote-owner head rows (grants owned by a
  synthetic remote boot, or barrier-held) repeated ≥ batch size (8) ahead of
  an eligible def in (next_fire_at, id) order ⇒ the scan's keyset/round-robin
  must still reach the eligible def within a bounded number of passes
  (deterministic: fixed head set, assert claimed in pass 1 or 2 — exact
  bound from implementation-contract).
- 55P03 aftermath: no idle-in-tx (`p23AssertNoIdleInTx`), same session
  usable for the next candidate.

### T4 Skipped-decision atomicity (root #3)
- Fill source to cap (P2.3 occupancy builders incl. cancel-requested,
  gate-entered, expired-lease, legacy rows) ⇒ scheduled claim commits tick
  `skipped_admission_capacity` + cursor advance, NO run, stamp unchanged
  (`p23AssertStampUnchanged` on `p23MustStamp` precondition); manual
  `EnqueueSyncRun` at the same state still gets the typed 429 — shared gate.
- min_interval analog: stamp inside interval ⇒ `skipped_admission_min_interval`,
  cursor advances, stamp unchanged; interval never refunded.
- auth_required: enabled+due def, NO grant row ⇒ tick `auth_required`
  (reason never_armed), cursor advances, stamp unchanged, no run. Grant row
  EXPIRED (`expires_at` past via fixture) ⇒ reason expired. Grant at OLD
  revision (revision bumped by config change) ⇒ reason stale_revision.
  Grant `last_seen_at` older than freshness (90s) ⇒ owner_stale.
- error_invalid_schedule: corrupt `schedule_cron` out-of-band (direct SQL,
  the only reachable path — every write surface validates) ⇒ tick
  `error_invalid_schedule`, NO run, cursor NULL (halted), repeat claim keeps
  it halted; repair (valid cron PATCH) recomputes a future cursor.

### T5 Success commit & stamp-last ordering (root #3)
- Happy path asserts ONE tx: pending run (requested_by = arm actor, tenant =
  arm snapshot), tick `enqueued` with run link + actor, cursor =
  NextFireAfter(parsed, dbNow) strictly future, last_fire/last_tick/outcome
  set — then stamp advanced (write-time ≥ claimed_at).
- LAST-statement proof (deterministic): marker-gated BEFORE UPDATE trigger on
  `connections` raising when `sync_last_admitted_at` is SET ⇒ the whole claim
  rolls back (tick, run, cursor all absent) ⇒ stamp update is inside the same
  tx and later than them; additionally the trigger body snapshots in-tx
  visibility of the tick row + new cursor into the RAISE message (or a
  fixture table) to directly prove ordering.
- Prune in-tx: fixture 501 ticks ⇒ one claim prunes to ≤500 oldest-first;
  marker BEFORE DELETE trigger raising once ⇒ full rollback (prune is not
  exempt from atomicity).
- Rollback at EVERY stage (user mandate): marker triggers on (a) run INSERT,
  (b) tick INSERT, (c) cursor UPDATE (batch_sync_defs), (d) stamp UPDATE ⇒
  each leaves ZERO durable delta, rows byte-equal (`tableRowJSON`), bounded
  rollback, same session recovers, `p23AssertNoIdleInTx`.

### T6 Honest dispatch residue (root #4)
- Repo-level only: after `enqueued`, the pending run exists with NO execution
  markers; a subsequent claim/loop pass must not adopt/re-touch it (cursor
  already future ⇒ not_due — assert run untouched via
  `snapshotSyncRun`/`assertSyncRunPersisted`).
- `dispatch_failed` post-hoc patch (repo helper per implementation-contract):
  applied by slot unique key to an existing `enqueued` tick ⇒ outcome flips,
  run_id kept, cursor/last_fire/stamp unchanged; applied to a nonexistent
  slot ⇒ clean nil/error per contract.

### T7 Grant identity, arm/revoke CAS vs claim (root #5/#6/#7)
- Fresh grant_id per arm even for same actor/revision/process (two UPSERTs ⇒
  different grant_id, old row replaced — PK def; assert monotonic armed_at,
  cursor/revision untouched by arm (cannot re-fire the slot).
- arm expected-binding CAS: arm tx Def FOR UPDATE with expected revision R /
  output O; concurrent retarget (UpdateSyncJob output) wins the Def lock
  first in one order ⇒ arm answers clean conflict (arm_target_changed /
  409-equivalent repo value), grant row NOT written; arm-first order ⇒
  retarget applies afterwards and grant row keeps OLD binding with OLD
  revision ⇒ next claim sees stale_revision ⇒ auth_required path, never
  silently authorizes the new output.
- revoke: DELETE grant row under Def lock; late revoke of an OLD generation
  (fixture: grant row already replaced by newer grant_id) deletes nothing
  (exact-generation WHERE) — the later successful grant survives.
- arm/revoke vs claim linearization: barrier on Def row; arm parked vs claim
  parked in both orders; each completes under the lock with consistent
  post-lock state (grant presence decides auth path deterministically).
- boot identity: grant rows carry boot_id ≠ lease holder/token values
  (fixture claims a lease first; assert boot_id differs from lease_holder/
  lease_token strings and is a fresh UUID per process — DB-observable subset).
- Tenant snapshot: grant org_id/owner_subject = arm caller snapshot; claim
  executes the run with the ARM tenant (run org/owner fields), parent chain
  re-checked post-lock under that tenant (org def claimed via personal
  tenant candidate ⇒ not_visible).

### T8 Owner liveness heartbeat (root #9, DB residue)
- Heartbeat UPDATE matches exact (grant_id, boot, revision, actor/tenant,
  not-yet-expired, fresh last_seen) ⇒ only `last_seen_at` moves (DB clock:
  new value ≥ dbClock before call), `expires_at` byte-identical.
- Wrong generation (any field mismatch) ⇒ zero rows touched (row snapshot
  equal); expired grant ⇒ zero rows; foreign owner ⇒ zero rows.

### T9 Config semantics & validation zero-write (root §10 line)
- Create ⇒ revision 1. Actual change of cron/tz/enabled/output/file_glob ⇒
  revision+1 (each separately); same-value PATCH ⇒ revision unchanged.
  Disable ⇒ cursor NULL; re-enable ⇒ future cursor from locked DB time
  (assert ≥ barrier-release dbNow — post-lock clock, not fixture time).
  Enable-without/invalid cron ⇒ `invalid_schedule`, zero durable change
  (`tableRowJSON` before==after, no rows). Empty cron explicitly ⇒ cleared+
  disabled+revision bump. Omitted PATCH fields preserve. Legacy INVALID
  `schedule_cron` row: name-only edit passes (no re-validation), stays
  disabled until an explicit cron change validates. Explicit invalid new
  cron/timezone rejected even when disabled. Flavor inference (5⇒Unix5,
  6⇒Quartz6), length >256 rejected. detail ≤256 CHECK asserted directly at
  DB level (257 ⇒ constraint error via fixture insert attempt —
  defense-in-depth; how the repo bounds detail follows the contract).
- Cursor recomputed on config change NEVER in the past even if cron
  expression would match a past instant (NextFireAfter(dbNow)).

### T10 Migration replay & history immutability (root §10 + user mandate)
- `p24ApplyMigrationsExcept` marker `sync_schedule_ticks`: genuine pre-P2.4
  DB with legacy terminal runs (artifacts), live valid lease (claim held),
  admission stamp set, legacy `schedule_cron` rows (valid + invalid
  strings) ⇒ Migrate twice ⇒ legacy schedules disabled + NULL cursor (no
  UPDATE — assert via row defaults), no ticks/grants created, sync_runs
  byte-preserved (`assertSyncRunPersisted`), lease columns intact, stamp
  unchanged, pre-existing batch_sync_defs columns field-preserved; post-
  migration replay #2 byte-equal snapshot of all touched tables.
- New migration file: no CONCURRENTLY (`p23StripSQLComments`), idempotent
  (IF NOT EXISTS forms), plays inside the batch advisory lock (existing
  `MigrationsAdvisoryLockKey` proof still green via frozen 59).

### T11 Independent sources & real per-source progress (user mandate)
- Two sources same org (+cross-tenant variant): source A def barrier-held;
  A's claim parked; B's claim commits fully while A parked (mirror of P2.3
  test 2). No global gate, no cross-source queue. Per-source cursors advance
  independently (fixture both due ⇒ both fire in one pass, each exactly 1).

## 5. Top-level names — AS WRITTEN (DRAFT for p24-required-names; tools track owns the file)

1. `TestPGScheduleLockedClockAndCandidateCAS` — scan-hint-narrows-only, post-lock rewrites decide not_due/stale, CAS field matrix, NULL-cursor never due
2. `TestPGScheduleSameSlotSingleWinnerTwoPools` — sequential loser clean no-op; 4-way same-instant: exactly 1 tick + 1 run, slot identity exact
3. `TestPGScheduleLoopBoundedFairProgress` — NOWAIT source contention fast/rowless (<3s); keyset `after` reaches a later def past a contended head
4. `TestPGScheduleSkipDecisionAtomicity` — auth_required reason matrix (never_armed/grant_expired/owner_stale/stale_revision/window_too_short), skipped_admission_capacity/interval (stamp never consumed, cursor advances, manual 429 shares the gate), error_invalid_schedule halts cursor NULL pending repair
5. `TestPGScheduleSuccessCommitStampLastAndPrune` — real committed TickID (round-1 #4), honest pending/controlled run with arm actor/tenant, future cursor + last-fire bookkeeping, stamp at/after claimed_at, in-tx stamp-order probe (tick+cursor visible at stamp time), 500-retention prune
6. `TestPGScheduleStageFailureFullRollback` — marker triggers at run/tick/cursor/stamp stages: zero durable delta, byte-equal rows, session recovers, no idle-in-tx
7. `TestPGScheduleHonestDispatchResidue` — no adoption/re-touch of the pending run; dispatch_failed patch (bounded detail 256, run link kept, def/stamp untouched), repeat/wrong-slot no-ops
8. `TestPGScheduleGrantGenerationCASAndArmRevoke` — fresh grant_id per arm (cursor/revision untouched), expected revision/output CAS, defID/in.SyncDefID mismatch zero-write cross-def+cross-tenant (round-1 #1), arm waiting past its own expiry persists nothing/Live never true (round-1 #3), revoke returns EXACTLY the deleted generation id (round-1 #8 shape)
9. `TestPGScheduleHeartbeatExactGeneration` — exact-generation update (only last_seen moves), pure-blocker crossing expiry AND freshness ⇒ false + row byte-identical (two-phase lock-then-clock; the P2.1 single-UPDATE-WHERE-clock trap), Def check is MVCC (valid heartbeat succeeds under a held Def lock — no reverse order)
10. `TestPGScheduleGetViewLiveByActualRevision` — revision bump ⇒ Live=false from the ACTUAL joined def revision (arg removed by the round-1 #5 fix), re-arm restores, nil on no-row/invisible
11. `TestPGScheduleConfigSemanticsZeroWrite` — revision bumps on actual change only (cron/tz/enabled/output/file_glob), same-value no-bump, disable⇒NULL/re-enable⇒future cursor, empty-cron clear+bump, invalid cron/tz/enable-without-cron zero-write both surfaces, legacy invalid cron survives name-only edits, Quartz6+UTC default+IANA tz
12. `TestPGScheduleMigrationReplayLegacyDisabled` — pre-P2.4 DB (marker skip set = exactly the P2.4 file), legacy valid+invalid crons ⇒ disabled+NULL cursor+revision 1 (no backfill/activation), old columns byte-preserved, stamp/live lease/terminal artifacts survive double replay, no CONCURRENTLY
13. `TestPGScheduleIndependentSourcesProgress` — hot source A fast-contention never blocks source B; scheduled+manual share the P2.3 gate (capacity skip + typed 429 + stamp discipline)

13 names → final total **59+13=72** if frozen as written. The earlier
provisional `TestPGScheduleTenantSnapshotAndParentChain` merged into 8/13 and
`TestPGScheduleAdmissionSharedWithManual` into 4/13 (subtests).

## 6a. design-draft v2 pre-review (read-only) — API testability verdicts

DONE 2026-11-14, draft v2 (182 lines). Overall: PG-testable and consistent
with the root contract. Findings:

- GOOD: DDL fully specified (constraint names enable the 23505 scoping
  test); outcome vocabulary closed (§8 table maps outcome→run/cursor/stamp —
  directly assertable); lock orders pinned per tx type; legacy migration
  via column defaults (no UPDATE) is cleanly testable; error_invalid_schedule
  OOB-only reachability matches the zero-write validation matrix.
- GAP-1 (blocking): exact repo method signatures absent — claim entry
  (candidate struct fields, return/typed-outcome shape), batch/scan loop
  entry (does `RunDueSyncJobs` survive? workers.SyncSchedulerStore change?),
  arm/revoke/heartbeat/dispatch-failed repo helpers, and the run-insert
  helper shared with EnqueueSyncRun. → implementation-contract.
- GAP-2: `actor` value on non-run ticks (auth_required/skipped_*) unspecified
  (arm.actor? NULL?) — needed for row assertions.
- GAP-3: fairness pass bound ("bounded number of passes") not numeric; T3
  needs the exact bound (or an assertable per-pass contract).
- GAP-4: prune exact policy (keep newest 500? delete WHERE created_at out of
  top-500?) — needed for T5 prune assertions.
- GAP-5: dispatch_failed patch helper semantics on missing slot (nil vs
  error) — T6.
- NOTE-1: out-of-row skip counters are in-process; PG tests assert zero-rows
  + prompt return + progress instead (draft already honest about "无行").
- NOTE-2: arm HTTP flow (JWT claims, output precheck over HTTP) is handler
  territory; PG tests exercise the repo tx helpers directly with synthetic
  snapshots. Registry memory/TTL/Stop/deadline-min are primary-unit scope.

## 6b. Blockers — status after round-1 sync

RESOLVED (contract published + source landed + tests written/adapted):
B1 exact signatures (claim/scan/arm/heartbeat/tick list/dispatch-failure);
B2 GAP-2 actor values (auth_required/skipped ticks carry the arm/grant actor
where one exists — asserted); GAP-5 dispatch-failure missing-slot = false.
Also adapted live to two mid-flight API corrections the primary GLM landed
DURING test writing: `RevokeScheduleGrant → (*uuid.UUID, error)` (round-1 #8
safe-Drop — asserted to equal the deleted generation's id) and
`GetScheduleGrantView` losing the caller defRevision arg (round-1 #5 — the
stale-argument lie is now structurally impossible).

OPEN for the primary/root (not test blockers, but semantics the written
tests pin to the CONTRACT, and a not-yet-synced contract line):
- ROOT RULING APPLIED (PG round-1): the old window_too_short auth_required
  expectation is RETIRED. A local arm expiry differing from the same
  grant_id's immutable DB expiry is an illegal/mismatched snapshot ⇒ fail
  closed (remote_owner-class skip, zero run/tick, no cursor advance). Only a
  healthy same-generation grant whose DB expiry genuinely passed answers
  grant_expired/auth_required. Test 4 encodes exactly this; the contract text
  (§3 step 5b) still carries the old wording and the primary GLM is syncing
  it to the root-review semantics.
- Round-1 #12 (Def lock mechanism) RESOLVED in source: claim's Def lock is
  FOR UPDATE NOWAIT under the 5s scheduleOpBudget; tests assert fast
  contention (never a row wait) and use a table ACCESS EXCLUSIVE barrier for
  the post-lock-clock proof instead of a row park.

## 6c. PG round-1 review — 18-item fix ledger (all applied, vet rc=0)

Definite errors 1–10: dedicated grants row_to_json snapshot helper (frozen
tableRowJSON untouched); PID warmed BEFORE every launch on a MaxConns1 pool
(arm-def wait, arm-grant wait, both HB lock proofs); Def-NOWAIT honored (fast
contention subtest + table-lock clock proof replacing the row park); old-slot
candidate now expects stale_candidate and the future-cursor candidate with a
future hint expects not_due (no early firing); NULL-cursor case expects
stale (CAS precedes due); capacity skip preserves the exact manual run
(count+ID), asserting no ADDITIONAL run; every tick decision read back by the
RETURNED TickID; p24Arm fixes ActorSubject = trusted OwnerSubject (verified
caller sub); enable-without-cron uses a dedicated cronless disabled def
(re-enabling the enabled fixture is a legal same-value patch); legacy glob
case split into same-value patch (no bump) vs actual change (bump, junk cron
survives unvalidated both ways); oversize fixture is a 5-field legal-value
list whose TRIMMED length >256 with a separate surrounding-whitespace
acceptance case.

Proof strengthening 11–18: trusted-input negative matrix (OrgID/Owner/
Actor/Revision/Output, exactly one field each, prior grant/def/cursor/stamp
byte-unchanged, no run/tick) + same-current-grantID renew-with-differing-
field refusal; the SECOND arm wait (pure grant-row FOR UPDATE after Def
acquisition) crossing its own expiry ⇒ ErrScheduleArmExpired with the
pre-existing grant byte-preserved; every expiry/freshness lock proof asserts
validity at genuine-block time before crossing, compares complete rows after
the refusal and proves same-session recovery, with windows inside the 5s op
budget; HB success preserves EVERY column except last_seen_at (jsonb-minus
snapshot) and current-def revision/output retargets refuse with row
preservation; GetView wrong-tenant checked while a live grant exists; the
stamp-order probe seeds the full 500-tick history and requires the probe to
see 500 (prune ran before the stamp, new tick kept); all four rollback-stage
failures start from a REAL prior manual admission (non-NULL stamp + pending
run preserved byte-for-byte, recovery adds the scheduled run); migration
replay compares BOTH seeded legacy runs (terminal AND still-running) against
their pre-upgrade snapshots after the FINAL replays.

## 7. Version-sensitive items → find-docs before use

- Resolved by root: cron parser/evaluator = in-repo `libs/scheduling-cron`
  (verified: `ParseCron` Unix5/Quartz6, `NextFireAfter` DST gap-skip/overlap,
  embedded `time/tzdata`); no external dependency to vet.
- Any NEW `ON CONFLICT`/constraint-name-matched 23505 handling not already
  proven in P2.1–P2.3 (advisory locks, FOR UPDATE/NOWAIT/KEY SHARE,
  EvalPlanQual two-phase CAS, clock_timestamp anchoring are proven).
- PG16.15 + pgx v5.7.4 pairing verified (vet run pulled pgx v5.7.4);
  anything outside that envelope is verified first.

## 7c. Resumed-review-1 round (test + script hardening)

Three test corrections (compile rc=0,
`vet-integration-repo-20261114T1420.{log,rc}`):
- `p24GrantImmutable` uses `(to_jsonb(g) - 'last_seen_at')::text` — key
  subtraction is a jsonb-text operation (PG16 official JSON functions); the
  former `row_to_json(...)::text - key` spelling was invalid SQL.
- The stamp-order probe records a THIRD column `run_ticks` = ticks with
  run_id IS NOT NULL: the 500 seeded ticks are auth_required/no-run, so
  `run_ticks == 1` at stamp time proves THIS claim's enqueued run-tick was
  already inserted — closing the cursor→stamp→tick→prune false green that
  total-500 + cursor alone could not (stamp would have seen 500 OLD ticks).
- The same-grant-id negative reuses ONLY the current grant id with every
  other field fully valid (no simultaneous owner change that would let the
  owner check alone carry the refusal): every arm must mint a fresh id.

Script corrections (syntax rc=0 + usage dry rc=2 archived at
`script-syntax-check-20261114T1415.{raw,rc}`):
- `write_meta` ends with an explicit `if`/`return 0` (the bare
  `[ -n extra ] && printf` returned 1 on empty extra under set -e, aborting
  `up` before readiness).
- `unified` propagates the ORIGINAL docker rc via `exit "$rc"` right after
  its report line — a failure can never reach the formal driver as 0.
- Per-attempt source CONTENT hash (`source_tree_hash`: go.mod/go.sum +
  deterministic shasum of the three services' *.go/*.sql trees and
  libs/scheduling-cron) recorded before AND after each run with a
  `SOURCE_DRIFT_DURING_RUN` flag; the git dirty-LIST sha stays as context
  only. Evidence/secrets are never hashed.
- `mkdir -p` the evidence dir before any redirection; the 0600 secret file
  moved OUT of the evidence dir to `/private/tmp/openfoundry-p24-pg.env`
  (never archived; `down` removes it; values never printed).
- Ownership labels `of-p24-owner=pg-isolated` on the network AND container;
  `up` refuses pre-existing names; `down`/`unified` verify the label before
  touching anything (env overrides can point at foreign names but cleanup
  then refuses) — q3v9/old fixtures are structurally unreachable.
- Version/ports are ASSERTED evidence, not comments: `up` records
  `published_ports=` + `server_version=` lines and DIES unless ports==0 and
  major==16; every Go attempt's raw log opens with the actual `go version`
  (`go version && ...` heads both run_new and the unified wrapper around the
  frozen P1 runner — the frozen script itself is untouched).

## 7b. Isolated-PG execution interface (for root / Tools — PREPARED, not started)

Exclusive driver: `tools/smoke/business-platform-p2-4/run_pg_isolated_p24.sh`
(syntax-checked rc=0 + usage dry-check rc=2 archived at
`/private/tmp/openfoundry-p24-repair-evidence/pg/script-syntax-check-20261114T1330.*`).
It is the ONLY artifact of this section; actual PG start waits for root's
signal. Contract: formal-execution-contract.md "Isolated PG ownership".

- `up` — private network `of-p24-pg-net` + `of-p24-pg` (postgres:16, ZERO
  host ports, verified in the raw log), pg_isready bounded poll, per-`up`
  random password stored ONLY in a 0600 env file under the evidence dir; the
  raw log also pins the exact server version line. Never touches q3v9 (no
  compose, no base PG, no migrate_fresh; nothing outside its own names).
- `new-normal` — golang:1.26.3-alpine3.22, CGO_ENABLED=0,
  `sh -c "go vet -tags=integration ./internal/repo/ && go test
  -tags=integration -count=1 -v -run '^TestPGSchedule' ./internal/repo/"`,
  CONNECTOR_DB_DSN → maintenance DB (repo helper creates fresh per-test DBs).
- `new-race` — golang:1.25 (Go 1.25.14), CGO_ENABLED=1,
  `-race -count=1 -v` (same test filter; separate log; never -count=2).
- `unified` — ONE fresh attempt of the FROZEN P1 runner:
  creates a fresh `ofp24_dataset_<UTC>` database for DATASET_DB_DSN;
  PIPELINE/CONNECTOR DSNs point at the maintenance `postgres` DB; runs
  `sh tools/smoke/business-platform-p1/run_tests.sh integration` at /src with
  the read-only source mount + shared caches + GOPROXY=off. The outer raw log
  IS the unified old59+newN raw (the runner cats its full `go test -v`
  output); the outer .rc is the runner's honest rc (go rc or its 26-name
  guard). It does NOT call P1/run.sh or any formal pg59 wrapper (those target
  the q3 fixture / migrate_fresh).
- `down` / `status` — remove/inspect EXACTLY its own container+network;
  status prints no secrets.
- Evidence discipline per attempt (all modes): `<phase>_<actualUTC>.raw`
  (DIRECT `> file 2>&1` from docker; no pipe/tee/grep anywhere), `.rc`
  (printf on the line immediately after docker returns), `.cmd` (exact
  command, password redacted `***`), `.meta` (actual UTC, git HEAD,
  dirty-tree sha256, image, mounts, caches) — unique actual-clock names,
  failures preserved, never overwritten; drift during a run is diagnosable
  from the recorded source identity. Defaults overridable via P24_* env
  (SRC/EVIDENCE caches/images). Tools' exact checker consumes the unified
  raw + .rc: 59 old + actual-N new names, each exactly 1 top-level RUN + 1
  PASS + 0 SKIP + 0 FAIL, plus original rc 0 (their file, not this track's).

## 8. Execution status & evidence (this track only)

UNIFIED FRESH 72 — GREEN, FINAL CANDIDATE (root rounds applied: cache warm
modwarm-20261114T1610 rc=0 for the two locked new deps under readonly
source; the wrong cursor-must-differ assertion replaced by EXACT new-cron
boundary math with root's minute-49 rationale):
- FINAL triple on hash `eb5757f7…d762` (before==after in every meta):
  new-normal `new-normal_20261114T045711Z` rc=0 (go1.26.3, 13/13/0/0);
  new-race `new-race_20261114T045727Z` rc=0 (go1.25.14, 13/13/0/0,
  0 DATA RACE); unified `unified_20261114T045814Z` rc=0 (frozen P1 runner,
  fresh dataset DB, go1.26.3, version_ok=true) — Tools exact checker:
  **distinct=72 bad=0 go_rc=0**, 5 top SKIP = the known optional five.
- T11 cron-change proof is now: revision bump + strictly-future pre-patch
  anchor + cursor sits exactly ON a UTC */7 minute boundary
  (second/nanosecond zero, minute ≡0 mod 7) + equals one of the two
  candidate boundaries reachable from a locked clock in [pre, commit] —
  a must-differ assertion was wrong because e.g. minute 49 satisfies both
  every-minute and */7 (legal same-instant recompute). Re-enable repeats
  the same boundary math against the stored cron.
- Earlier green intermediate triple on 1cda0894 (045113/045506/045350)
  remains archived; failures 044823Z (fixture timing) and 045135Z (cold
  cache) preserved verbatim.
- PRESERVED failed attempts (fixture-side, fully diagnosed): unified
  `044823Z` rc=1 — my timing-fragile assertion (every-minute→*/7 cursor
  coincidence is a legal recompute; removed) plus post-hoc dbClock
  comparisons (hardened to pre-captured anchors in T4/T11); unified
  `045135Z` rc=1 — five packages [setup failed] because the primary's new
  deps (coder/websocket, pashagolub/pgxmock/v4) were absent from the shared
  cache under GOPROXY=off; fixed by a one-off cache warm
  (`modwarm-20261114T1610` rc=0) without changing the offline attempt
  discipline.
- Fixture still UP (never downed); q3v9/HTTP/swap untouched. The PG test
  file stays frozen at 13 names; the timing hardening was a fixture-only
  change inside the frozen tests (no new names, no old-59/production
  edits).

Root round on 044003 (both findings were fixture-side, kept 044003 rc=1):
- grant_expired vs window_too_short: the contract's reason ORDER maps an
  arm-present expired window (same grant_id, arm.ExpiresAt ≤ dbNow) to
  window_too_short even when the expiry EQUALS the row's; grant_expired is
  the arm-less DB-row truth. The new
  `same_id_exact_expiry_expired_answers_window_too_short` subtest uses ONE
  canonical µs value (read back from the row into arm.ExpiresAt — two host
  clock reads differ at µs precision) so the branch is exercised by an
  exactly-equal snapshot, not a mismatched one.
- CAS: material AND expected both bind the precheck-old values; the locked
  def's ACTUAL state then moves under the parked arm in TWO race variants
  (revision bump and output retarget) — each answers
  ErrScheduleArmTargetChanged with zero writes. The write↔expect
  disagreement cases stay as the documented ErrScheduleArmInput precheck
  layer (no branch hidden).

Final green rounds (fixture still up; unified deferred to source freeze):
- new-normal `new-normal_20261114T044537Z` rc=0 — go1.26.3 linux/arm64,
  version_ok=true, 13 top RUN / 13 PASS / 0 SKIP / 0 FAIL.
- new-race `new-race_20261114T044607Z` rc=0 — go1.25.14 linux/arm64,
  version_ok=true, 13/13/0/0 and 0 DATA RACE lines.
- Both metas: source_content_sha256 before==after (3f19957c…91b2), no drift.

REAL isolated-PG rounds (root-released up→new-normal→new-race; fixture
of-p24-pg PG 16.15 on of-p24-pg-net, 0 host ports asserted; NOT down):
- First new-normal attempt `new-normal_20261114T044003Z` rc=1 PRESERVED:
  11/13 top-level PASS; the 2 failures were TEST-side alignments to the
  source's newer semantics — (a) grant_expired now arrives arm-less (DB
  truth per root ruling; an arm-present claim labels window_too_short),
  (b) the arm drift subtest split: write↔expect disagreement is the NEW
  pre-DB `ErrScheduleArmInput` precheck, while `ErrScheduleArmTargetChanged`
  is now proven by a REAL Def-lock race (consistent arm parks, def bumps
  revision under it, commit, post-lock CAS refuses, zero writes).
- `new-normal_20261114T044158Z` rc=0: go1.26.3 linux/arm64 (raw first line,
  version_ok=true), 13 top RUN / 13 top PASS / 0 SKIP / 0 FAIL.
- `new-race_20261114T044227Z` rc=0: go1.25.14 linux/arm64 (version_ok=true),
  13/13/0/0 and 0 DATA RACE lines.
- Both metas: source_content_sha256 before==after
  (dc46014d…cad1), no SOURCE_DRIFT flag.
- Awaiting root's source/test freeze before the separate fresh `unified`
  (old59+13=72); q3v9/HTTP/swap untouched; fixture stays up.

- `schedule_integration_test.go`: 13 top-level `TestPGSchedule*` functions +
  p24 machinery (row barrier on connections/batch_sync_defs/
  sync_schedule_grants with BOUNDED 5s release/commit/exec contexts,
  stage-failure flag-table triggers for run/tick/cursor/stamp, in-tx
  stamp-order probe table, `p24Go` hard-bounded 20s, fixtures/observers).
  Every background op is `p24Go` + `p23RegisterCleanup`
  (release-before-cleanup); every concurrency point is a barrier handshake or
  lock-queue truth poll; time authority is always the DB clock.
- Compile evidence (container pattern of P23: golang:1.26.3-alpine3.22,
  GOTOOLCHAIN=local, readonly mod, isolated named cache volumes — no PG
  touched): final `go vet -tags integration ./...internal/repo/` ⇒ **rc=0**
  (`vet-integration-repo-20261114T1206.{log,rc}`); the intermediate
  `1121/1126/1132/1140/1148/1155/1200` logs+rcs archive each honest failure
  (my mechanical bugs and the two live API adaptations) and `1036/1042`
  archive the earlier helper-only rounds.
- NOT done by this track, deliberately: starting PG, running any DB test,
  touching production source/old tests/other tracks' files.

## 9. Confidence markers

- High: frozen 59 sources/counts; root contract content; draft v2 DDL/
  protocol facts (§2); helper inventory; time contract; scheduling-cron
  surface; compile rc=0 evidence.
- Medium: T3 fairness bound mechanics (GAP-3); T1 in-tx advisory anchor
  feasibility (depends on claim tx exposing a parkable statement — barrier
  fallback specified); T5 trigger-snapshot ordering proof (technique sound,
  final form follows exact tx shape).
- Low/unresolved: everything blocked on B1–B4 (exact signatures) — no test
  code proceeds on guesses; provisional name list subject to freeze.
