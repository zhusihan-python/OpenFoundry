# tools/smoke/business-platform-p2-4 — P2.4 acceptance tooling (DRAFT)

Status: **DRAFT, awaiting root review** (2026-11-14). Nothing here runs a
stack mutation, test, or switch until root releases it. The business
contract is NOT pinned; contract-dependent phases hard-refuse.

## Ownership

* This directory's acceptance tooling is owned by the P2.4 tools GLM task.
* `design-draft.md` and `pg-test-plan.md` belong to the OTHER GLM task —
  this task does not create or modify them.
* Frozen P1/P21/P22/P23 tools and results are invoked by absolute path
  only, never modified.

## File inventory

| file | role | state |
|---|---|---|
| `derive_pins_p24.py` | regenerate the 4-row pin list from the FROZEN P2.3 final archive; enforces SHA256 + 20-common-field equality + P23-pin cross-check; `--check` = tamper check; validates the generated module via `ast.parse` + literal equality (root round-1 fix) | ran rc=0 |
| `p24_pins.py` | GENERATED pin data (4 rows x 27 fields). Do not hand-edit | generated |
| `p24_live_guard.py` | owned strict live guard: wholesale controlled pending/running query, exact 27-field pins, real-datetime `started < container boot`, no discovery/no age-expiry forgiveness; `RELEASED_FOR_EXECUTION=False` until root flips it; strict `compose ps` (rc0 + exactly 1 cid) and friendly empty-stdout BLOCK (root round-2 fixes) | DRAFT, not executed |
| `selftest_live_guard_p24.py` | pure policy selftest (no docker/db): green/unknown/missing/drift/NULL/duplicate/boot/org-divergence/lease-movement + timestamp trap + ps/inspect adjudication small cases | ran rc=0 (19 ok) |
| `collect_preflight_p24.py` | READ-ONLY live preflight collector (root-authorized 2026-11-14): docker whitelist inspect (connector/fixture), base5 ps, volumes, .env MODE only, SQL: 24safe+3org controlled rows / cron+timing sweep / legacy b6ec49ab B/C (frozen P22 projection), fixture counters HTTP GET; raw stdout + real rc + stderr per artifact, `-rN` collision-safe | ran rc=0 |
| `verify_preflight_p24.py` | pure verifier over 00-preflight artifacts: A 4x27 exact, B real-datetime pre-boot, C fixture unchanged, D base5, E volumes, F env600, G legacy==frozen baseline, H fixture counts==baseline, I no non-empty schedule_cron | ran rc=0 (9/9) |
| `formal_run_p24.sh` | formal phase orchestrator SKELETON (P23 discipline inherited; `P24_CONFIRM` + contract gates; swap precheck = p24 guard; p24-scenarios runs aux two-replica and tears it down; `disable-owned-proof` phase = G7 stop-firing proof) | DRAFT |
| `pin-provenance.md` | provenance + semantics + open questions for root review | DRAFT |
| `smoke_p24.py` | HTTP scenario suite mapped to the 8 gates; review-tools-round1 10 items APPLIED: owned-ID registry + global finally disable/GET-confirm/ID-set window (gates verdict), per-case cleanup, strict 200+bare-array+association lists, G1 COMMITTED/lineage/full expected_records, G2/G7 positive run+tick ID-set proofs, G3 rev0+3, G4 owner-sub/org short JWT, G5 REAL admission cap/interval + zero-effect snapshots + B-terminal, G6 write-scope isolation (parent proof → PG), suffix/attempt collision-safe results (`RESULT_FILE=`); runtime refuses without `--confirm-http` | DRAFT; probes rc=0/0/3 |
| `p24-required-names.txt` | DRAFT 13 top-level PG names from current schedule_integration_test.go; root freezes after stability | draft |
| `run_tests_p24.sh` | owned REQUIRED-name guard (P23 pattern; guard-only/integration/guard-probe; refuses empty/missing list; grep guard NEVER replaces Go rc) | probes ok |
| `contract-pinned.json` | tools-GLM-maintained metadata (root-authorized): review-contract SHA256 + pinned decisions + gates; exact-names section flips when implementation-contract updates | generated |
| `implementation-contract.md` | **primary GLM owns** exact routes/JSON/SQL/API — landed 2026-11-14 10:38 | read-only for tools |
| `readonly_baseline_p24.py` | OWNED wrapper: frozen P2.2 read-only steps re-used unchanged, output under P24 (`readonly-baselines/…`), prints `BASELINE_DIR=`; P22/results/preparation gets NO new dirs (root 2026-11-14) | DRAFT |
| `p24_schedule_env.sh` | renders the P24 scheduler-env override (`OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS` 1..30 on connector+aux; collision-safe; non-secret) — applied only by confirm-gated formal step 07b | DRAFT, not executed |
| `p24-required-names.txt`, `run_tests_p24.sh`, `archive_standalone_p24.py` | to be written when PG actual test names land | pending |

**Guard release status (2026-11-14):** root reviewed the corrected guard
body and fresh 4×27 snapshot and released READ-ONLY execution.
`guard-release-run.out/.rc` in 00-preflight records the single run:
rc=0, boot `2026-11-13T22:05:01.092139798Z`, 4/4 pinned, undrifted,
pre-boot. The swap phase remains gated behind root's formal release
(`P24_CONFIRM`); evidence code now enforces the rc-gate (all bound
artifacts rc=0), binds raw/derived/rc files + SHA256 + attempt, keeps
verdict history with an index, and 07b evidence uses the corrected
`sync.admission.min_interval_ms` path.

## Formal-execution preparation (formal-execution-contract.md)

| file | role | state |
|---|---|---|
| `formal_run_p24.sh` | COMPLETE driver; `full` LOCKED FALLBACK ordered: preflight→baseline→fingerprints→unit→pg-evidence(import-or-exec)→build→swap(**01b-swap-source-gate: fresh 373 sha == pre == PG binding tools SHA + image-ids pinned to the rc0 01-build-final attempt, no ls -t**; gate BEFORE guard4/stop)→p1pre9→p23-adapted(archive-even-on-failure)→p22full(failure-first-archive)→scheduler-window(--with-restart-probe)→pre-auxdown-gates(guard4+quiesce)→aux-down→persist-pre→pre-restart-gates→restart3(stop -t120/start/health/image-verify)→reread→p1post9→**ABC LAST WRITE**→postfacts→persist-post→source-freeze→standalone→finalize; entry rc0→exit0 verified by pure mock | front segment released 2026-11-14 |
| `pg_exact_check_p24.py` | exact checker + `--new-only` mode | rc0/1 verified |
| `run_pg_isolated_p24.sh` | **PG GLM owns**; binding path `05-pg-import` already executed rc0 (binds PG_FINAL_BINDING: 3 rounds sha/rc/go/no-drift + REAL exact checker over imported logs + 5 unverified optional SKIPs + preserved failures; identical-content tests NOT rerun) | bound |
| `check_owned_quiesced_p24.py` | pre-auxdown/pre-restart owned gate (fresh SQL: disabled/NULL/no-grants/all-runs-terminal); env_dir Path-normalized (root fix) | fail-closed rc1 |
| `collect_persist_state_p24.py` | owned persistent-state snapshot (defs config/revision/cursor + tick/run ID sets + grant absence) pre/post final restart; finalize compares EQUAL | fail-closed rc1 |
| `p23_q_guard_adapter_p24.py` | transparent adapter: frozen smoke_p23 Q run with EXACTLY the inner guard argv substituted to the reviewed 4-pin guard; proxy delegates every other subprocess call unchanged; rejects malformed guard argvs; archives original/replacement argv+SHAs+rc/stdout+count; success requires exactly 1 substitution; `RELEASED_FOR_EXECUTION=False` until root review | refuse rc=3 |
| `cohort_gate_p24.py` | SHA-bound cohort handoff gate (late-ABC sequence): approved 2-row record + fixed four must be exactly the controlled set pre-restart; fixed-four pre-boot rule kept, cohort two are the reviewed post-boot exception; NOT a pin modification | refusal paths rc=1 |
| `verify_postfacts_p24.py` | ATTEMPT-BOUND verifier (one fresh dir + attempt-manifest; never stem-latest mixing): manifest sha/rc integrity incl. both result-input bindings; UNCONDITIONAL exact-6 unique (old4 ×27 unchanged + diff EXACTLY 2 — no "if unknown" shortcut); new-two FULL parent chain vs raw SQL (all 9 chain keys, org triple, run_owner/requested==owner per frozen P21 actor); B lease<started by REAL datetime; C bound to logical_path UUID + result COMMITTED/lineage/hash + LIVE target re-proof; old4 pre-boot with new2 reported post-boot; owned FRESH-SQL final state (disabled/NULL/match bindings, zero grants, GLOBAL nonempty-cron == owned cron set); C'-G' facts | synth green rc0 / drop-C rc1 / tamper rc1 |
| `collect_postfacts_p24.py` | post-ABC read-only collector into ONE fresh attempt dir: facts, controlled 27-field, legacy, fixture counters incl. THIS attempt's two ABC keys, ABC raw parent chain (2 Sources/Defs/ALL runs), C target LIVE (txn/lineage/hash, in-memory token), owned final SQL; attempt-manifest LAST binds every file sha256+rc + inputs | DRAFT |
| `pg_exact_check_p24.py` | exact checker: 1 top RUN/1 PASS/0 SKIP/0 FAIL per name over old 4 lists + actual P24 list (computed total, today 72), duplicates/illegal/empty rejected, wrapped Go rc file must read 0 (grep never replaces Go rc) | synth green rc0 / neg rc1 |
| `run_pg_isolated_p24.sh` | **PG GLM owns** (not written here); driver expects `PG_LOG=`/`PG_RC_FILE=` stdout markers, refuses while missing | pending |

## Results layout

`results/2026-11-14-p24-final/00-preflight/` — read-only live baseline
(collection attempts preserved verbatim, `-rN`, `.rc`/`.err` sidecars;
`local-selftest/` selftest attempts; `preflight-verdict.json` latest).

## Invariants this tooling enforces

* q3v9 base5 + fixture same-process (boot pin
  `2026-11-13T18:29:14.176785055Z`) + PG/files/cache volumes + env600.
* The 4 controlled unknown rows stay exactly pinned (27 fields each),
  never cancelled/retriggered/rewritten; org triple is a stricter
  inferred-equality constraint, fresh-verified 2026-11-14.
* Legacy b6ec49ab B/C stay under the frozen P2.2 baseline projection.
* Default `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=0` must not silently
  start legacy cron; all non-empty `schedule_cron`/timing columns are
  inventoried before any scheduling is enabled.
