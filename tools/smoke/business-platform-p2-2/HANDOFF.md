# DURABLE HANDOFF — P2.2 formal acceptance execution (2026-11-14)

## Authorization
ROOT FORMAL RELEASE (2026-11-14, this conversation): this session is the
SOLE q3v9 executor; proceed autonomously through the audited
EXECUTION-PLAN.md; no further confirmations needed. Tool-side fixes are
autonomous; BUSINESS failures get a precise repro + evidence for the
main GLM (do not weaken assertions / no bodyless-K bypass).

## Durable state (all under tools/smoke/business-platform-p2-2/)
- EXECUTION-PLAN.md — the audited step order & rc discipline.
- formal_run.sh <step> — orchestrator; EVERY step appends real
  {step,rc,seconds,cmd} to results/2026-11-14-p22-final/manifest.jsonl
  and writes <step>.log + <step>/ dir. Resume-safe: run steps in order.
- Step order: build → up → fixture-restart → baseline → unit → pg50 →
  p1pre9 → aux-up → contract → p22 → p21abc → restart3 → reread →
  p1post9 → aux-down → finalize.
- readonly_baseline.py / compare_baseline.py — pre/post persisted-handle
  comparison (fixture counters NEVER compared across upgrade — expected
  reset recorded at 03-fixture-restart/expected-counter-reset.json).
- Prior evidence: results/preparation/2026-11-14T021922-readonly-baseline
  (pre-upgrade facts: stack was pre-P22 build; B=running/0 files;
  C=running/1 COMMITTED file sha 7e69fbf3…; P1 old artifact + 3-domain
  Build/Job all GET-200 verified).

## Non-negotiables
- rc discipline: `bash -o pipefail` for run.sh steps (internal tee);
  orchestrator uses direct redirection only. Never treat tee's 0 as ok.
- fixture restarted exactly ONCE (step 03) — never again.
- P21 historical b6ec49ab B/C rows: never cancel/rewrite; compare only.
- PG50 = P1-26 inside runner + explicit P21-14 & P22-10 guards on the
  SAME verbose log (05-pg50/migrate-tests.log).
- P22 must be official (D..N all once); default queue64/worker1/TTL300.
- Order: P22 (incl. J) BEFORE P21 A/B/C (J's live-task guard); final
  three-service restart AFTER P21; I/M stop evidence ≠ final restart.
- Archive everything raw under results/2026-11-14-p22-final/ (keep
  failures, no overwrite); finalize runs the secret scan + manifest.

## Continuation procedure for a successor GLM
1. Read results/2026-11-14-p22-final/manifest.jsonl — the last step with
   rc=0 tells you where to resume; console.log has the running narrative.
2. Continue: bash formal_run.sh <next-step> (see order above). Long steps
   (unit/pg50/p22/p21abc) take minutes — run in background, poll the .log
   tails only (context discipline: target <60%, hard 70%).
3. On tool bugs: fix in this directory autonomously + rerun the step.
   On business failures: capture repro/evidence, continue independent
   steps, and compile the failure section in the final report.
4. finalize → write results/2026-11-14-p22-final/final-report.md with
   facts only (counts, rcs, versions: git head + dirty count, image ids,
   Go build/vet/race results relayed by root: real-tcp-post-boundary-r2
   8PASS/exit0, unit-post-drain-final exit0, race 4pkg exit0, build-vet
   Go1.26.3 0/0 — raws held root-side; record as reported facts).
5. Root-relevant notes: remaining main-side nit = test require-order
   lock fix (no production impact). Overcap(>1MiB)=413, slowbody=408,
   read-err=400 all pre-admission zero-rows; queue-full stays 503.

## Current status (updated as steps complete; see manifest-corrected.jsonl)

### ATTEMPT1 RESULT (raw archived, NOT official): 09-p22-D-N rc=1, 8 PASS / 3 FAIL
- PASS: D,E,F,G,H,K,M,N — M passed under the OLD 2-member cohort code
  (gap since fixed; first-round JSON kept as attempt1 evidence).
- FAIL I: connector2 boot after 3-replica `compose start` died at
  2026-11-13T18:39:08.012Z with
  `migrations: apply 20260424201000_sync_jobs_runtime.sql: relation
  "sync_jobs" does not exist (42P01)` → 90s health fail. ALL stop-side
  evidence green (3 replicas exit_code=0/oom=false whitelisted inspect).
  ROOT-CONFIRMED BUSINESS GAP: Repo.Migrate per-SQL pool.Exec, NO
  migration lock; CREATE→ALTER→DROP chain lets concurrent replays
  destroy each other's tables. Evidence:
  results/2026-11-14-p22-final/attempt1-diagnostics/ (structured JSON +
  exact log excerpt, scrub-checked).
- FAIL J: cascade (deadline-aux guard: connector2 not running).
- FAIL L: tool bug — short identity had a random sub → correct 403
  permission denial (not expiry). FIXED: owner sub/org, TTL=5s only,
  fresh-token read precheck added.
- **HOLD**: no attempt2 / no further stop-start reproductions until the
  main GLM lands minimal Migrate serialization + independent PG-writer
  regression AND root re-authorizes build/up. (The 02:44 local
  reproduction attempt never executed — shell quoting rc=127, nothing
  touched.)

### FINAL CANDIDATE execution — **COMPLETE: FINAL_OK=True (2026-11-14)**
**Archival closure done**: standalone/ holds ALL 57 root-side raws
(connector/pg-cancel/pg-migrate-concurrency incl. classified FAIL/
partial history; 5 files DSN-password-transformed, 52 byte-identical;
per-file SHA256 in standalone/index.json); final-report cites the real
paths + evidence-tier boundaries (I/M runtime-assertion summaries; J
r3 elapsed 7.0s, case 12.8s incl. restore); whole-archive
hash-manifest.json (144 files); extended secret-scan (env values +
JWT/private-key/lease-value/real-DSN patterns; ${VAR} and <redacted>
markers excluded) = 0 leaks; stack-status.txt records base-5-up /
fixture-reloaded-once / aux-down. FINAL_OK=True re-verified.

attempt4 (09-p22-D-N-r3.log, suffix 7aeaf5c9): **P22 OFFICIAL 11/11**
(M=5-member cohort — per-member comparisons are REVIEWED-CODE ASSERTIONS
that PASSED this round; result JSON holds summaries only, see the
evidence-tier note in final-report.md; J terminal_elapsed=7.0s, case
12.8s incl. finally-restore, root-pinned fragment hit; I=real concurrent
49.1s under the new advisory lock). P21 ABC OFFICIAL 3/3 (suffix
96351e3b). Unit 61/0/0,
PG52 (guards rc0; both new migration top-levels RUN+PASS). P1 pre/post
9/0/0. restart compare all-True; upgrade compare True. aux down rc0.
finalize: FINAL_OK=True, 0 secret leaks. Full facts:
results/2026-11-14-p22-final/final-report.md. Attempt layering kept
(8/3 → 10/1 → cancelled → 11/11 official; J-focused partial green).

### FINAL CANDIDATE execution (root re-release 2026-11-14 03:03) — superseded snapshot
- [x] 01-build-final-r1 rc0 — EXPLICIT 5-target build (base 3 + merged
      connector2/3; the override builds have separate images) —
      image-ids.json: connector a7dbe5a18cb0 / connector2 6100360f3ded /
      connector3 c8336a21ad16 / pipeline 94c5865edd4c / dataset dfe34ef37bd7
- [x] 01b-swap-* rc0 — guard: 0 controlled in-flight; graceful stop of
      the 3 old migrators (exit0/oom=false, whitelisted, exits.txt);
      up-base + up-aux --no-deps (CONCURRENT migrate under the new
      advisory lock — no serial-start bypass); health; per-replica
      image-membership + fresh StartedAt VERIFY rc0 (no mixed fleet)
- [x] 04-unit-r1 rc0 — FINAL CANDIDATE unit: 61 ok / 0 FAIL / 0 SKIP
- [x] 05-pg50 r1 rc0 — fresh complete log (guard input
      05-pg50/migrate-tests-20261114T031136.log) + guard-p21 rc0 (14)
      + guard-p22 rc0 (**12** incl. TestPGConcurrentMigrateSerialized and
      TestPGMigrateLockWaitCancellation — both verified RUN+PASS in log)
- [x] 06-p1-pre9-r1 rc0 — 9/0/0, uncovered 0
- [x] 07-aux-up-r1 rc0 (record; dataset-proxy untouched, running)
- [x] 08-contract-r2 rc0 — all 3 aux up, cancel 404, P22 columns
- [ ] 09-p22-D-N attempt2 — IN FLIGHT (background; log will be
      09-p22-D-N-r1.log; full 11 official required, NOT stitched)
- [ ] 10-p21-ABC / 11-restart3(+health) / 12-reread(+compare restart-mode
      at 12-reread-compare-output/compare.json) / 13-p1-post9 /
      99-aux-down / finalize (FINAL_OK must be true) + final report
Constraints re-affirmed: fixture NEVER restarted again; volumes/raws/
historical B/C untouched; attempt1 raws kept; failures honest.

### Tool fixes landed while held (all compile; selftests unaffected)
- M: cohort is now MANDATORY 5 members (completed/cancelled/interrupted
  REQUIRED + P21 B/C first-class members with real run/source/target
  bindings via `_legacy_chain`; full projection/pages/file-set/txn-map/
  runs-set per member; completed boundTxn + 200-then-hash kept).
- L: real-owner short-TTL token + fresh-valid precheck.
- contract-probe: aux startup retry (r1 recorded, green).
- formal_run reread: compare output → 12-reread-compare-output/ (was
  expanding LAST_STEP_DIR too early); upgrade-compare likewise stable.

- [x] 01-build rc0 (root confirmed 3 images Built) / 02-up rc0
- [x] 03-fixture-restart rc0 — counters reset to 0 (expected-reset.json +
      new-boot snapshot archived); fixture NEVER restarts again
- [x] 00-baseline rc0 — **p22-full-13col** (post-up 022957 dir recorded in
      00-baseline-path.txt); C artifact sha 7e69fbf3… intact post-upgrade
- [x] 00b-upgrade-compare rc0 (021922→022957: 10-col common equal,
      control_mode=legacy/cancel=f/gate=f, targets/artifact/buildjob ok)
- [x] 04-unit rc0 — 61 ok / 0 FAIL / 0 SKIP
- [x] 05-pg50 rc0 — migrate + guard-p21(14) + guard-p22(10); P1-26 inside
      runner; guard input archived 05-pg50/migrate-tests-attempt1.log;
      SKIP=5 (external-gated optional, listed in log)
- [x] 06-p1-pre9 rc0 — 9/0/0 uncovered 0
- [x] 07-aux-up rc0; 08-contract rc0 (r1 after proxy-startup retry fix:
      cancel 404, columns landed, 3 aux up)
- [ ] 09-p22-D-N — IN FLIGHT (background exec_21d7e020…; log:
      results/2026-11-14-p22-final/09-p22-D-N.log)
- [ ] 10-p21-ABC / 11-restart3(+health) / 12-reread(+compare restart-mode)
- [ ] 13-p1-post9 / 99-aux-down / finalize (verdict: required rc0 + pre9/
      post9 9/0/0 + p22 official-11 + p21 3 + compares ok + 0 secret leaks)

Rev2+ orchestrator notes: manifest.jsonl lines now valid JSON (2 legacy
lines repaired in manifest-corrected.jsonl only); per-attempt -rN logs;
pg50 guard input per-attempt unique; phase exit = step rc; finalize
requires full verdict set (see REQUIRED list in formal_run.sh).
