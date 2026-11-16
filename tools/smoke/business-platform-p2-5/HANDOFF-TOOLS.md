# P25 Tools GLM — durable handoff (2026-11-14, foundation round)

One-pager for a fresh Tools context. Full design: `tools-design.md` v2
(same directory; v1→v2 changelog §12 maps every root round-1 ruling).
STATE + NEXT only.

## Round history
- Round 0 (design): read-only evidence → tools-design v1 + this file.
- Round 1 (root review-tools-round1): corrections absorbed into
  design v2; foundation coding released (see below). Primary v1
  last-page-request-cursor protocol REJECTED — do not build old
  fixture/HTTP scenarios against it.

## Key corrected facts (supersede round-0 notes)
- Transition anchor = P24 FINAL connector boot
  `2026-11-14T07:11:20.883030213Z` + FULL image
  `sha256:fe3208b3fcbd4d5fe428647e106d6ba6aa5b71fd94e9ba51144a9565258dc4c3`
  from SHA-bound `15-final-base3-images.json` (also carries dataset/
  pipeline full IDs + aux connector2/3/dataset-proxy containers:0 =
  removed fault-trigger evidence). Nov13T22:05 is ONLY old4 historical
  provenance. A blanket all6<current-boot check MUST fail today
  (postboot2 started 07:25 > current boot 07:11) — first-transition vs
  steady are two explicit guard policies.
- Owned-history frozen binding: `13-persist-post-r1/persist-manifest.json`
  owned_history_sha256 `8c18999a723fef3fe819612a33d821fc7731aad16c0634506a5bc0a622a18001`,
  17 owned defs / 32 ticks / 15 terminal runs / 0 grants, p24_result
  sha `3beb1bce…` — P25 preflight re-verifies all fresh.
- 373-file source baseline = archive `01-source-hash-pre.txt`
  (combined `8cd25d4220a6bdae843a693e1c1afa928991f537f5902a31e3e970704db6386b`).
- cohort_gate_p24.py is an UNEXECUTED DRAFT without full27 schema
  enforcement — never reuse as P25 release guard.

## Foundation files (implemented this round; pure selftests only)
p25_cohort_derive.py (SHA-chain + anchors + atomic ast generation +
--check + --emit-frozen-baseline) → p25_cohort6.py (generated) +
p24-frozen-baseline.json + source-p24-baseline.txt (373 current tree
== frozen); p25_cohort_guard.py (DRAFT, RELEASED_FOR_EXECUTION=False,
pure first-transition/steady adjudicators); selftest_p25_cohort.py;
source_hash_p25.py (emit/verify/delta); readonly_preflight_p25.py
(DRAFT collector, --plan only until released); verify_preflight_p25.py
(pure adjudicators); selftest_p25_source_preflight.py;
run_foundation_selftests_p25.sh (honest runner; real cmd/rc/stdout in
results/2026-11-14-p25-foundation/).

## Round 2 (root review-tools-code-round1, 11 must-fix) — DONE
- All 11 items implemented (see tools-design.md §12 round-2 block):
  derive mechanical values (sealed source-baseline combined VERIFIED,
  single-line literals after 2 hand-split corruptions were caught by
  our own hard-refuse, 13 new sealed content-gate files consumed,
  owned-history file SHA-bound, legacy SQL/P21 fault names imported/
  regex-verified); guard full roster validation + damaged-expected
  refusal + steady requires boot AND full image; collector rewritten
  (appended suffixes, cmd-before-exec, timeout/OSError honesty,
  13-step content-gate catalog incl ps -aq aux/volumes/envs/fixture/
  legacy/fullstate/C-live/pg_proc+pg_trigger, NO future-CP queries,
  injectable IO); verifier rewritten (exact per-artifact membership +
  argv whitelist + all content gates); ONE coherent mocked
  collector->verifier happy + missing-artifact + timeout negatives
  (real naming, no fake pre-placed files); runner attempt-dirs with
  --only (no green reruns for bookkeeping).
- Green map (latest per step, all logs preserved under
  results/2026-11-14-p25-foundation/attempt-1..6): py_compile a4,
  derive_real/derive_check/selftest_cohort a3, baseline_install/
  tree_delta/preimpl_gate(rc1 honest, in-flight impl)/guard_refusal
  (rc3)/preflight_plan/baseline_byteequal a2, selftest_src a6 (18/18;
  cohort selftest 47/47 incl new F-layer). Failed attempts 1/4/5 kept.
- Current-tree delta vs frozen 373 (tree-delta.json a2): changed=4
  models/rest_snapshot + added migration (primary track in flight),
  removed=0.

## Round 3 (C-live shape fix + full-tool start) — DONE
- C-live gate now STRICT FLAT RuntimeTransaction per root's source
  read (transactions.go:58-77; models.go:837): route
  GET /api/v1/datasets/{target}/branches/main/transactions/{txn} (frozen
  P21 route table :68/:77), file via files/{fid}/download; verifier
  checks id/dataset_id/status==COMMITTED/metadata.lineage.sync_run_id
  exactly — no tolerant shape mixing. Affected steps green in
  attempt-7 (derive/check/cohort/src/plan).
- FULL-TOOL START (runtime still NOT released; guard DRAFT False):
  * fixtures/rest_source_p25.py — accepted v1.1 protocol: independent
    `since_token` param + terminal-page completed token at
    meta.completed_token; deterministic page keys (sha256, no hash());
    bounded old-token replay; unknown/foreign/control/oversize tokens
    400; fault knobs (fail_mid_pages, fail_terminal_after_rows,
    slow_terminal, candidate_mode missing/non_string/empty/oversize/
    ctrl_chars/echo_request_token, empty tail R14); /admin/src
    old-fixture-compatible counters + /admin/log ledger (token chain
    + zero-dispatch evidence).
  * fixtures/dataset_fault_proxy_p25.py — drop_after_verify
    (forward→verify COMMITTED→record proof→drop response), no_txn_200/
    not_committed/wrong_target/http_500 fabrications WITHOUT forward
    (zero-forward provable), /admin/mode + /admin/log ledger.
  * compose-p25-fixture.yaml (p25fixture 17092) +
    compose-p25-aux.yaml (p25-proxy 17093, p25connector 17094 with
    DATASET_SERVICE_URL→proxy) — ZERO depends_on (root addendum;
    driver must use explicit whitelist + --no-deps), static-checked.
  * smoke_p25.py SCAFFOLD — contract-locked ROUTES/FIELDS/error
    codes, h1–h15 registry w/ gate vocabulary; pure modes only
    (--plan/--contract-probe/--list-gates); no HTTP engine yet.
  * selftest_p25_fixture_proxy.py 29/29 (attempt-8) + contract-probe
    rc0 + compose static rc0.

## Round 4 (review-tools-code-round2, 6 closures) — DONE, all green
1. collect entry loads the GUARD module (round-2 AttributeError fixed);
   pure selftest drives the ACTUAL main/collect entry in-process (flag
   flip + module-level real_run_cli/real_http_get mock swap; no
   listener/network/DB).
2. owned-history SQLs now filter by the EXACT sealed-17 def ids
   (UUID-validated literals from the seal-derived baseline; no caller
   SQL text). GLOBAL controlled-6 projection stays WHOLE-SET. NEW
   global steps: 05b grants-total (expect 0) + 05c global nonempty-cron
   (EXACT sealed id set, derived from 12c artifact). Mock SQL routing
   keys on the REAL query shape: the ANY(ARRAY[..]) filter must be
   present in the SQL text or the mock refuses — semantics drift is
   not maskable.
3. C gate now also binds txn branch_name=='main' and a files-LIST
   ownership step (file entry transaction_id==sealed txn +
   logical_path carries the run id) on top of flat
   id/dataset_id/status/lineage + download bytes/sha.
4. SEAL-ANCHORED FORMAL PATH: verify main takes ONLY --attempt-dir;
   plan/frozen/roster are REDERIVED from the fixed P24 seal
   (SEAL_P24_*_DEFAULT + full hash chain) and the sibling generated
   files must EXACTLY equal the derivation (key sets included).
   guard.load_roster rederives from the seal too — double-editing
   baseline+cohort cannot forge expectations (C4 proves the formal
   main REFUSES the mock C bytes against seal-derived sha). Toy data
   is injectable ONLY into the internal verify_attempt() pure
   function by tests.
5. attempt-summary is a full CONSUMPTION LEDGER (per-artifact
   name/bytes/sha256 + original rc + plan sha256); the verifier
   independently re-hashes every file, checks ledger membership both
   ways, recorded rc==0/200, and plan_sha==canonical. local-sha gate:
   EXACT three keys, MISSING never equals MISSING,
   source-p24-baseline.txt sha256 == sealed combined 8cd25d42… .
6. Counter discipline: request_count/inventory_started/
   inventory_finished must be present NON-BOOL ints; active must be
   the exact int 0 (bool False rejected); started==finished; both
   reads complete + fully equal. Malformed-field negative (False/
   None/string) flows through the ACTUAL content checker (C7).
Green map: selftest_src 22/22 + fxproxy 29/29 + probe + compile
(attempt-11); selftest_cohort 47/47 + derive/check/plan (attempt-9);
src fix rerun (attempt-10). Failed attempts preserved.

## Round 5 (ownership handoff, root coordination 2026-11-14)
- Route PINNED by root: checkpoint view =
  GET /api/v1/data-connection/syncs/{id}/checkpoint (Connector
  server.go:95+130; contract's shorter spelling = doc typo, Primary
  syncing). Scaffold note updated + probe/compile green (attempt-12).
- OWNERSHIP TRANSFER: smoke_p25.py moves to PRIMARY (they continue it
  after their 3 production patches complete and freeze). Tools made
  the FINAL edit (route pin above) and WILL NOT EDIT smoke_p25.py
  again; its pure selftest invocations remain readable. Primary-facing
  API doc for fixture/proxy/compose saved at
  fixtures/FIXTURE-PROXY-API.md (frozen handoff).
- Tools retains: foundation (all rounds above), fixture/proxy/compose,
  formal driver, tail roster/quiesce, collectors, images, finalize,
  standalone, suite adapters.

## Round 6 (review-fixture-proxy-round1, 6 fixes + scoped loopback)
- Foundation round-2 six closures were already green (attempts 9-11)
  BEFORE this round, per root's ordering.
- Fixture rewritten (consumer position = request token ONLY: NULL
  always re-serves window A; T_i⇒window i+1; only latest token maps to
  exhausted empty re-issue; terminal candidate derived from REQUEST
  window — replay-stable; per-request page snapshot). Real counter
  lifecycle: started/active on entry, finished/active-- in finally on
  ALL exits incl. disconnects; delays OUTSIDE the lock (admin/healthz/
  sibling srcs progress while one src blocked). empty_batch knob
  REMOVED (exhausted-stream = the empty case; new always-empty src
  p25empty covers initial-empty).
- Proxy rewritten: EVERY commit dispatch ledgered incl. normal
  passthrough (zero-publish provable); /admin/stats
  started/finished/inflight/modes + true-quiet (bounded thread join);
  fault modes isolate EXACTLY ONE ACK predicate (target from path:
  no_txn_200 = correct target+COMMITTED+no txn id; not_committed =
  correct target+valid txn+OPEN; wrong_target = only target wrong);
  drop_after_verify records full upstream ACK (sha + parsed fields) +
  verified_committed verdict (nonnil txn ∧ COMMITTED ∧ exact target)
  BEFORE dropping; no Authorization/JWT values ever logged.
- NEW scoped loopback selftest (root release): OWN in-process servers
  on 127.0.0.1:0 only (fixture + proxy + FAKE dataset stub), hard
  destination-port guard, finally shutdown + bounded joins, no
  leftovers. L1 nil-retry-A / L2 token-retry stable / L3 stable
  paging / L4 blocked: active>0 + healthz + sibling progress + true
  quiet / L5 error + raw-socket disconnect lifecycles quiet / L6
  normal commit ledger + stats / L7 single-predicate faults over HTTP
  / L8 drop-verify (COMMITTED→verified true; OPEN→false, both
  dropped+recorded).
- Green: pure 33/33 + loopback 27/27 + compile (attempt-13);
  intermediate failures preserved (empty-batch nesting, unpack arity,
  missing sys import — all test-side, fixed).
- FIXTURE-PROXY-API.md updated to r2 (semantic revisions marked);
  smoke_p25.py handoff to Primary stands (Tools still not editing it).

## Round 7 (full tool suite candidate) — DONE, reviewable
New Tools-owned files (all NEW P25; frozen dirs untouched):
- p25_tail_roster.py — hash-frozen PREDECLARED tail rosters; TWO
  independent rosters (nonterminal RUN rows vs CHECKPOINT OWNERS incl.
  terminal-failed run refs, never cleaned/adopted); pure declare/
  verify; vocabulary-locked semantics.
- check_owned_quiesced_p25.py — owner-aware quiesce: owned defs
  disabled/NULL; NEW runs for owner-blocked defs = adoption violation;
  blocked-admission proof consumed from SAVED pre-ABC artifact (POST
  after ABC forbidden).
- collect_persist_state_p25.py — P25 persist snapshot (defs/ticks/
  runs/grants/cp_owners with P24-style manifest+shas; owners marked
  PROTECTED retention).
- collect_postfacts_p25.py / verify_postfacts_p25.py — readonly facts
  builder (input-sha bound) + final-roster adjudication: old6
  unchanged (seal-derived equality) + ABC-round 2 (bound to THIS
  round's archived result, collision-checked vs old6) + declared tail
  k — tail verifier sees ONLY tail rows (no double counting); counters
  x2 stable/quiet/int-discipline; fixture+proxy true-quiet gates.
- check_running_images_p25.py — full-id image equality (seal anchors
  or reviewed P25 target), p25 services present/absent per phase, old
  aux stays absent.
- standalone_p25.py — redaction with logged sites + replay compare
  (unlogged diffs and no-op sites both fail).
- finalize_p25.py — rc/verdict/required-step gates → secret scan 0/0
  → manifest → tools-sha → hash-manifest LAST; FINAL_OK only all-
  green (06b informational gate exempted by name).
- p23_q_guard_shim_p25.py + p25_old_suite_adapter.py — frozen-suite
  rerun adaptation (exact 5-token inner-guard argv → P25 shim; other
  argv untouched; malformed guard argv REJECTED; record + sha chain;
  P23-Q exactly-one enforced; P24 full7 count recorded). Adapter
  RELEASED_FOR_EXECUTION=False.
- formal_run_p25.sh — LOCKED 23-phase order (list/dryrun modes;
  P25_CONFIRM + RELEASED_RUNTIME double gate; write phases refuse rc3
  until runtime release; readonly phases post-ABC ungated): ALL
  switch/restart/aux-removal + P1post9 + artifact/BuildJob reread
  BEFORE p21abc (final q3 write); tail declaration hash-frozen in
  pre_tail_gates BEFORE tail; attempt-safe step logging.
- selftest_p25_tailtools.py 25/25 (attempt-14) + driver dryrun rc0 +
  unconfirmed-phase refusal rc3 verified.
Suite totals now: cohort 47, source/preflight 22, fxproxy 33,
loopback 27, tailtools 25 — all green across attempts 9-14.

## Round 8 (queued small fix + driver phase bodies)
- AUX-ABSENCE FIX (foundation re-review): 03-aux-absent steps no
  longer run `compose ps -aq <svc>` (base compose lacks those services
  → rc1 "no such service" is NOT absence). Now exact docker label
  filters: docker ps -aq --filter label=com.docker.compose.project=
  openfoundry-platform-p1-q3v9 --filter label=com.docker.compose.
  service=<svc> — includes stopped, precise, project-scoped,
  compose-file-independent. Canonical plan/selftest mock (new B4b
  assertion + label route)/ledger all synced; green attempt-15
  (selftest_src 23/23, plan rc0).
- Driver phase bodies completed (attempt-16 dryrun rc0 + refusal rc3):
  fixture_up = explicit whitelist + --no-deps p25fixture + bounded
  health wait (17092/healthz); aux_down = proxy TRUE-quiet proof step
  (inflight==0 ∧ started==finished) BEFORE explicit-whitelist
  rm -sf of the three p25 services; cross-track invocations (unit/
  build/swap/regressions incl. P24 full7 via adapter/reread/p1post9/
  restart/tail/p21abc/final_images) carry REF-PIN markers — exact
  frozen argv pinned at release review, never invented; pre_tail_gates
  now requires root-pinned tail-spec.json before declare.

## Round 9 (review-fixture-proxy-round2, 3 remaining defects) — DONE
1. verify_commit_ack now parses the txn id as a REAL nonzero UUID
   (uuid.UUID + != zero); pure E5 covers positive real-UUID and the
   negatives t-1 / zero-UUID / None / not-a-uuid / empty.
2. drop_after_verify ledger now retains the FULL raw upstream ACK
   body (upstream_ack_raw — commit JSON only; no headers/Auth/
   secrets) alongside the sha; loopback L8b independently RECOMPUTES
   sha256(raw) == recorded sha and parses the raw.
3. Real worker-thread lifecycle: begin_commit registers the current
   thread (+commit_threads_registered counter), finish_commit
   unregisters — join_threads is no longer an empty-set fake green
   (L8e asserts registered>=1; finally joins real set). Disconnect
   test reworked: SINGLE-PAGE src with terminal delay, waits until
   ENTRY is observable (active>0) BEFORE closing the socket, then
   bounded-waits true quiet (L5c). Test-race fixes: L5b bounded
   quiet poll; missing hashlib import.
Green: fxproxy 33/33 (attempt-17/19), loopback 28/28 (attempt-20);
attempts 18/19-partial failures preserved honestly.

## Round 10 (review-tools-code-round3 ALL FOUR closed) — candidate STABLE
- #1 aux absence via exact docker label filters: closed round-8.
- #2 PRE-EXECUTION SEALED BOUNDARY: collect_body strong-compares the
  ENTIRE local baseline == independently sealed-derived
  frozen_baseline_dict BEFORE any runtime IO (rc2 refusal; no
  subprocess constructed) and builds the executable plan from the
  SEALED dict — never the local file. Negative C8: SQL-only tamper
  (cohort sha unchanged) ⇒ rc2 with ZERO run_cli/http calls
  (counted).
- #3 LEDGER STRICTNESS: summary.attempt_dir must equal the dir under
  adjudication; steps_recorded must be the COMPLETE canonical base
  set (exact two-way; c-live multi-stem aware); every .rc/.http_status
  ledger entry must carry original_rc/status EQUAL to the actual file
  content itemwise (missing = failure); counter url/cmd/kind compared
  BYTE-EXACT against canonical plan text; per-src raw .http_status/
  .body required (existence alone proves nothing). Negatives C9a-d.
- #4 COUNTER RAW CHAIN: every counter response retained RAW (per-src
  .url/.http_status/.body on disk + ledger); derived snapshot binds
  bodies_sha256; verifier REPLAYS derivation from raw bodies+statuses
  (derived != replay fails), and each body must self-identify
  name==src (fixture + mock updated). Negative C10: swapped raw body
  caught by replay+sha.
Green: src 29/29 + fxproxy 33/33 + loopback 28/28 (attempt-22);
attempt-21 partial failure preserved. Fixture round2 three closed in
round-9. Candidate stable — ready for root narrow recheck toward the
ONE readonly preflight release.

## Round 11 (review-formal-tools-round1, 10-group batch) — implemented
- Fixture FIRST (Primary unblock): p25h4 now A→B(8) — deterministic
  second NON-EMPTY window for seeded candidate scenarios; nil still
  maps to A; API doc synced; D6 test (fxproxy 34/34, attempt-24).
- #1 driver: aux_down quiet proof failure now ABORTS rm (strict
  non-bool ints; None==None cannot pass); `full` locked-order branch
  (recorded phase chain; import-green env allowance); finalize phase
  note re hash-LAST.
- #2 adapter/shim: frozen suites' main() take NO argv (sys.argv
  swap/restore); shim policy via env P25_SHIM_POLICY=steady needs
  reviewed P25_TARGET_BOOT/IMAGE (first-transition default).
- #3 finalizer: per-step LATEST-attempt rc (historical failures
  preserved, not poisoning selected-PASS); bare `rc` + `.rc`; tool-sha
  covers fixtures/yaml/md; absent verdicts never pass.
- #4 hash LAST: manifest/tools-sha settle INSIDE EV, hash-manifest is
  the final EV write; post-seal EV must be ZERO-WRITE (readback
  bidirectional + mtime guard); FINAL VERDICT returned on stdout to
  the driver OUTSIDE the sealed EV.
- #5 standalone: byte-preserving (only sanctioned redaction lines
  change; binaries exact), per-file original+copy SHAs, independent
  re-READ replay, --reverify mode, missing/empty src rejected.
- #6 images: absence requires EXPLICIT integer containers counts
  (missing keys are NOT absence); unknown services fail; running
  window services need state+full image.
- #7 tail/quiesce: post-exec EXACT identity binding (tag→def/source/
  run UUIDs, UUID-to-UUID, one-run-two-tags refused, owner row
  required); quiesce requires NON-EMPTY expected defs set.
- #8 postfacts: controlled_nonterminal_ids CONVENIENCE FIELD FORBIDDEN;
  ABC ids DERIVED from raw result_p21.json (suffix/pass bound; b/c
  semantics walk; ambiguous ⇒ refuse, exact shape pinned at release).
- #9 persist: REAL per-table collection records ({rows, rc, err,
  cmd_sha256}); missing table/nonzero rc = honest failure (no
  defaulted [], no fabricated rc0).
- #10 negatives for the above added to tailtools (35/35, attempt-27)
  incl. C1b/c, C3b/c/d, D3b/c, E1b/E3, F1b.
Green map this round: attempt-24 fxproxy 34/34; attempt-27 tailtools
35/35 + compile; attempt-28 cross-suite confirm (src/loopback/driver).
Remaining externally-pending facts (honest): REF-PIN frozen argv for
unit/build/swap/regressions/reread/p1post9/restart/p21abc/final_images;
exact frozen result_p21.json field shapes for derive_abc_ids pinning;
tail-spec.json contents (root, pre-tail).

## Round 12 (round3 narrow-recheck residuals #3/#4) — DONE
- #3 residual: counter raws are READ-TAGGED ({sid}.{read1|read2}.{src}
  .url/.http_status/.body) — read2 can never overwrite read1 (54 raw
  files = 9 srcs x 2 reads x 3 artifacts; first-round failure
  originals immutable). Membership/ledger/replay all read-tagged.
- #4 residual: summary.attempt_dir must be PRESENT and EXACTLY equal
  (missing/null never passes). Negatives C9e/C9f; distinct-raw check
  C4c; C10 re-targets read2's tagged file. selftest_src 32/32
  (attempt-29). Foundation candidate FULLY closed for the ONE readonly
  preflight release.

## Round 13 (ONE readonly preflight RELEASED and EXECUTED)
- Root released the collector flag ONLY (guard/adapter stay False).
  Executed against fixed env /private/tmp/openfoundry-platform-p1-q3v9
  with the in-memory service JWT (never printed/archived; no q3
  writes/switch/restart/compose).
- attempt-1 (raw preserved): collect rc0/0 failures; verify rc1 —
  found two TOOL-SHAPE bugs (not sealed expectations): dataset routes
  are /v1/... (frozen P1 CONTRACT has NO /api prefix) and psql session
  SET tags polluted last-line parsing. Fixed both + dead block.
- attempt-2 (final): plan rc0; collect rc0 failures=0; verify rc1 with
  ALL 14 reasons isolated to the 07/C-live gate: HTTP 404
  {"error":"dataset not found"} on the CORRECT /v1 routes — the
  service JWT (fallback personal-domain) authenticates but cannot see
  the ORG-OWNED C dataset; P24's live C re-verification used the
  runtime-minted ABC actor token, which is deliberately not persisted.
  Every other gate GREEN: 01 exact P24-final boot/image, 02/02b, 03
  aux absent, 04 old6 first-transition (27-field exact, anchors,
  groups), 05 owned-17 fullstate exact, 05b/05c global grants=0 +
  nonempty-cron exact, 06 legacy13, 08 fault absent, 09 9-src counters
  x2 stable/quiet (54 raw), 10 volumes, 11 modes 600, 12 fixture
  boot/PID1, 13 shas+baseline==8cd25d42, membership+ledger clean.
- Verdict reported honestly: all_ok=False, sole cause credential
  visibility. Root to decide: accept sealed-P24 C evidence for this
  preflight, authorize another EXISTING credential, or record as
  environment fact. Tools did NOT weaken the gate.
- selftest_src re-green after /v1+SET fixes (attempt-30).

### Round 13 addendum (B6 incident + final state)
- B6 refusal test was written for the DRAFT flag; after the legitimate
  release it subprocess-invoked the released CLI ONCE → an ACCIDENTAL
  read-only collection (attempt-3, 166 artifacts, C-live honestly
  skipped for unset token). Verified impact: ZERO counter drift
  (/admin/src snapshots never increment request_count; live p21b
  still == sealed 2), zero q3 writes. Raw preserved +
  ACCIDENTAL-NOTE.md. B6 rewritten to in-process flag flip with a
  no-IO tripwire (32/32, attempt-31).
- FINAL preflight verdict for root: attempt-2 verify all_ok=False with
  ALL 14 reasons isolated to 07/C-live (HTTP 404 dataset-not-found on
  the CORRECT /v1 routes — service JWT authenticates but cannot see
  the org-owned C dataset; P24's live re-verify used the
  runtime-minted actor token, never persisted). Every other gate
  green. Awaiting root's disposition (accept sealed-P24 C evidence /
  authorize another existing credential / record as env fact).

## Round 14 (PG coordination; no reruns, no new runtime)
- PG three stages INDEPENDENTLY raw-verified by root (normal15/race15/
  unified87, original rc0, meta gates 0, same source a1bff07f…ffd27).
  NOT rerun by Tools. Bindings interface consumed READ-ONLY:
  pg-candidate-bindings-r17.json (candidate source sha, required
  names 15 / unified 87, stage attempt bases + rc/gates, test-file +
  checker shas).
- BUILD ownership: PG session SOLELY writes build_candidates_p25.sh +
  BUILD-HANDOFF.md and performs the ONLY-build (base3 + frozen
  oldaux2 + new p25connector; build/image-inspect only, evidence at
  /private/tmp/openfoundry-p25-repair-evidence/build/). Tools does
  NOT co-write. Driver phase_build now CONSUMES the reviewed PG
  artifact read-only (bindings sha prefix/suffix + evidence
  components + source-gate presence; never builds; deploy still
  separately runtime-gated) — dryrun rc0 (attempt-32).
- Candidate NOT switched; readonly preflight state unchanged
  (attempt-2 verdict stands, 07/C-live awaiting root disposition).
  Formal 10-group corrected version with two readonly agents; REF-PIN
  closure waits for their feedback. Old tools untouched.

## Round 15 (C-live root ruling executed) — PREFLIGHT ALL_OK TRUE
- Root ruled: fresh C verification via the SAME existing test-actor
  identity. Claims spelling read VERBATIM from frozen
  mint_token.py/smoke.mint_token (HS256; sub/jti/iat/exp/email/name/
  roles/permissions/optional org_id). c_live_reverify_p25.py mints a
  10-minute in-memory actor JWT from the existing q3 signing material
  with sub=sealed C run owner / org_id=sealed C org (seal-derived),
  performs EXACTLY 3 read-only GETs; token/secret never printed/
  archived; no DB/permission change.
- c-supplement/attempt-1: txn/file-download/files-list ALL HTTP 200.
- verify_preflight_p25 --c-supplement (new, foundation-owned): 07 gate
  ENFORCED from the fresh GETs (not exempted); base attempt-2 files
  untouched (07 base artifacts stay archived as superseded historical
  facts, explicitly marked in provenance); verdict records supplement
  dir + 9 artifact SHAs + base-attempt binding.
- FINAL VERDICT (attempt-2 + c-supplement): ALL_OK=TRUE,
  membership/ledger/content all true, reasons 0. Every first-
  transition requirement freshly verified: exact P24-final boot/image,
  old6 27-field, owned-17 fullstate, global grants 0 + nonempty-cron,
  legacy13, fault absent, 9-src counters x2 quiet (54 raw), volumes,
  dual env 600, fixture boot/PID1, shas+baseline, AND fresh C
  txn(200, flat, branch main, COMMITTED, exact target, lineage)+
  file bytes 22766/sha 7e69fbf3… + files-list ownership.
- One intermediate verify rc1 was MY relative-path invocation (strict
  attempt_dir equality correctly refused); absolute rerun green —
  honest note, no file changed.
- SCOPE SPLIT ACKNOWLEDGED: Tools now owns ONLY foundation/fixture/
  proxy/compose + readonly execution + own HANDOFF. Handed off to the
  new GLM (do not edit): formal_run_p25.sh, old adapter+shim, images,
  tail roster, quiesce, finalize, standalone, postfacts, persist.
  Business/build helper/smoke untouched as before.

## Round 16 (review-c-supplement-round1 both gaps) — DONE, ALL_OK re-confirmed
- #1 REAL adjudicate_supplement: exact 3 canonical GET members (no
  extras/missing/partial); per-artifact url/http_status/body raw
  bytes+SHA+length vs ITS OWN ledger (tamper-without-ledger-update
  refused — D1); exact schema + attempt_dir identity (resolve-strict);
  supplements_base_attempt must equal the base path AND record the
  BASE SUMMARY sha256 (summary field or verifier-minted immutable
  sibling base-binding.json — minted for the saved supplement with
  real values; originals untouched; different base refused — D2);
  actor sub/org must equal the sealed C run parents (roster-sourced;
  swapped actor refused — D3); canonical URLs must carry the sealed
  target/txn/file refs; supplement UNKNOWN extras refused (D4).
  Rejection short-circuits the verdict (c_supplement_rejected) and
  records the supplement SUMMARY sha in successful verdicts.
- #2 NARROW base-07 supersession: prefix-wide startswith bypass
  REMOVED. Only the EXACT canonical three .http_status entries with
  recorded original_rc == 404 may be superseded (same-target fresh
  GETs); base 500/corrupt/unknown/noncanonical still fail — D5 proves
  the narrow rule fires even for a fully re-bound crafted pair. Base
  07 url/cmd/raw-SHA membership checks preserved.
- Re-adjudicated SAVED base2 + supplement (no q3 recollect, no GET
  redo): ALL_OK=TRUE, reasons 0, supp summary sha 28a13afac39d8464….
  Two earlier verifier bugs found by the REAL saved evidence (bare-vs-
  full stem set compare; ledger canonical key suffix) — fixed.
- selftest_src 38/38 (attempt-36) with CLI negatives D1-D6 (D6 =
  positive control on the saved evidence). Scope unchanged
  (foundation/fixture/proxy/compose + readonly + HANDOFF only).

## Round 17 (attempt-36 narrow: 3 leftovers) — CLOSED, ALL_OK re-confirmed
- Narrow-1: adjudicate_supplement ledger now requires the EXACT 9-key
  url/status/body set (extras/missing/non-dict/missing-fields each a
  distinct reason; deleting 8 of 9 refused — D7).
- Narrow-2: base .url/.cmd canonical comparisons run BEFORE/WITH the
  404-supersession branch — both base and supplement URLs must be
  canonical; a tampered base URL under canonical 404 statuses refused
  (D8). Non-all-404 base (500/corrupt/unknown) falls through to the
  generic checks — supersession impossible (D5 retained).
- Narrow-3: base-binding.json sibling fully validated — exact
  base_attempt path + base_summary_sha256 + supplement_summary_sha256
  (any field tamper refused — D9); the binding FILE sha256 is consumed
  into the final verdict (binding_sha256) for the formal hard-bind.
- Re-adjudicated SAVED originals: ALL_OK=TRUE, reasons 0
  (summary_sha 28a13afac39d8464…, binding_sha a1799498f231b94b…).
  selftest_src 41/41 (attempt-38; attempt-37 partial failure honestly
  kept — the new binding defense fired before the crafted negatives
  reached their rules; D5/D8 now re-bind siblings fully so the narrow
  rules themselves are proven).
- No q3 reread, no GET redo, scope unchanged; Tools now IDLE awaiting
  root (final formal consumes summary_sha + binding_sha + the sealed
  chain).

## Round 18 (RUNTIME round3 — review-runtime-tools-round2 SIX items, new sole-writer session) — IMPLEMENTED + executed where authorized

Ownership (root redistribution): the runtime-GLM now solely owns the 7
runtime files (formal_run_p25.sh, p25_old_suite_adapter.py,
p23_q_guard_shim_p25.py, check_running_images_p25.py,
p25_tail_roster.py, check_owned_quiesced_p25.py + new runtime
selftests/docs). Archive GLM owns collect/verify-postfacts,
collect-persist, finalize, standalone — their NEW interfaces are
consumed read-only, never co-edited. smoke stays Primary's.

1. (#1) formal_run_p25.sh rewritten — every phase has a REAL body,
   REF-PIN gone. argv pinned from the FROZEN P24 driver phase bodies
   (P1 run.sh tests/smoke; P21 smoke_p21 --env-dir; P22 smoke_p22 /
   swap_verify / p22_connector2 up+down / compare_baseline; P23
   adapter pattern "-- --env-dir … --allow-restart"; P24 smoke
   "--confirm-http --official --with-restart-probe"; restart3
   stop/start -t 120 + P1 smoke --wait-only). Fingerprints: sealed 373
   baseline (file sha == 8cd25d42…), CURRENT tree == reviewed
   candidate a1bff07f…ffd27 (NEW runtime-owned
   candidate_source_gate_p25.py reproduces the PG source_tree_hash
   stream BYTE-EXACTLY, importing the foundation ROOTS read-only —
   foundation's source_hash_p25.py itself was touched by mistake,
   caught on self-review and REVERTED byte-identical, foundation
   selftest 41/0 re-green; bindings json + driver-pinned literal
   cross-checked both ways), EXPLICIT
   baseline→current delta recorded (removed must stay 0; today
   373→380, changed 16 / added 7 / removed 0). Nothing demands
   unchanged-old-373 anymore. unit IMPORTS Primary's accepted
   attempt20 (cmd/rc "rc=0"/38 ok/0 FAIL re-verified + shas + re-bound
   to TODAY's tree via the candidate hash; exec mode = frozen P1
   runner, live-gated). pg_isolated IMPORTS bindings-r17 (stage
   artifacts sha-bound; metas re-parsed; the REAL exact checker
   executed over the imported raws --new-only ×2 + unified; test-file
   + checker shas vs today's tree; 15 required names). build consumes
   the pinned 142804Z ONLY-build round → EV selected-build.json
   (exact full sha256 per service incl. p25connector 59472329… and
   cached python:3.12-alpine; source-gate expect==candidate).
   EXECUTED FOR REAL under results/2026-11-14-p25-final/ (P25_CONFIRM
   only; pure phases need no runtime flag): fingerprints rc0, unit
   rc0, pg_isolated rc0, build rc0 (manifest.jsonl + per-step attempt
   dirs). Honest failures preserved: fp-baseline-sealed attempt1
   (caught OUR OWN 63-char hand-typed seal literal — the P24 dropped-
   char lesson repeating, fixed), pg-import attempts 1-2 (import-
   verifier bugs: Path-wrapped pinned argv, go-version prefix),
   build-consume attempt1 (PG meta "(tag …)" suffix, image_sha[ key
   slice off-by-one, cache/source-gate have no before/after). full
   refuses rc1 without RELEASED_RUNTIME; live phases refuse rc3;
   unit/pg EXEC modes refuse rc3. preflight = released collector +
   verify + PRE-WINDOW readonly_baseline_p25.py (frozen P22 step
   functions imported read-only; output ONLY in P25 EV — the frozen
   P24 wrapper writes into the P24 tree so it is never invoked).
2. (#2) adapter/shim: per-invocation guard rc/stdout/stderr + sha256s
   archived; EXACT expected counts enforced (p23=1; p24=2 with
   --with-restart-probe; ZERO substitutions FAIL loudly);
   --deployment-record verified BEFORE the suite loads (schema/
   RFC3339 boot / full-64-hex image / == selected-build connector /
   == candidate source) and the steady shim env is set FROM the
   verified record (record wins over bare env). Driver writes/
   refreshes deployment records (candidate-swap → p23-q-restart →
   p24-g4-restart → restart-regression) so P23Q/P24 legal restarts
   RE-record the new boot (never forever-first-transition); the
   pre-tail steady guard adjudicates against the LATEST record. The
   shim re-verifies the record from env and refuses env/record drift.
3. (#3) images rewritten: exact selected-build full-SHA mapping;
   PRESENT = strict-int containers==EXACTLY 1 + running + image==
   expected + ps_rc 0 + inspect_rc 0 (garbage fails); ABSENT =
   strict-int containers==0 + SUCCESSFUL ps (bool False / missing key
   / failed ps are never absence); unknown services fail; evidence
   without the collector schema refused; python:3.14-slim tag services
   (p25fixture/p25-proxy) must equal the collector-resolved tag id AND
   the driver's first-observation pin (window-images-pin.json at the
   restart check; re-verified at final). New LIVE collect mode refuses
   rc3 until released.
4. (#4) tail roster rewritten around ONE authoritative verify():
   declaration re-derived + sha RECOMPUTED (tamper refuse); exact
   frozen 27-key schema on every run row; counts exact; identity
   binding MANDATORY (UUID-to-UUID def/source/run, parent fields must
   match, one-run-two-tags refused, placeholder/absent handles
   refused); owner EXACT set (unknown owners block — never adopted or
   cleaned); terminal-failed owners validated against TERMINAL
   evidence and NOT required in the nonterminal set (old bug fixed);
   evidence vocabulary locked with the h12 rule — local-pg-rollback
   can NEVER evidence a C-family lost-COMMIT-ACK tag (atomic proof ≠
   lost ACK). CLI verify requires --bound (no bypass helper);
   verify_bound_identities remains ONLY as a strict compatibility
   wrapper for the Archive verifier import (empty bound no longer
   passes). verify_postfacts routed through the single verifier with
   terminal evidence.
5. (#5) quiesce rewritten: EXACT expected-def UUID set (root-pinned
   pre-tail; missing/extra fail); runs_terminal evidence CONSUMED
   (every expected-def run terminal except an owner's OWN held run);
   per-Def/Run PRE-ABC blocked-POST binding (each owner def UUID needs
   its own {http_status 4xx, rc strict-int, body_sha256 hex64, run_id
   UUID == retained owner run}); unknown owner rows retained-and-
   blocking; LIVE readonly collect mode (same SQL projections as the
   frozen P24 persist + Archive postfacts; new_runs_since_decl from
   real started_at) refuses rc3.
6. (#6) NEW selftest_p25_runtime_tools.py 43/43 (selftest_p25_
   tailtools.py now delegates): every negative hits the ACTUAL CLI /
   adapter.main / shim main — A3 two-field toy run rows via CLI, B3
   def_tag-in-UUID-field via CLI, D wrong-candidate-SHA via the real
   candidate CLI (+ D10 real a1bff07f green), G through adapter.main
   (real record verification, real shim subprocess rc3 archived with
   stdout/stderr/shas, P24 zero-substitution BLOCK) and the shim CLI
   itself; H1-H3 driver gating.

Cross-track interface notes (consumed read-only; pinned):
- Archive collect_postfacts --collect --env-dir --out-dir
  --aux-quiet-dir --abc-result: the driver archives RAW quiet proofs
  at EV/aux-quiet-archive/{proxy-quiet-proof,fixture-quiet-proof}
  .stdout.log (the exact names the canonical plan references; proxy
  stats JSON + per-src fixture counters list, strict-int validated
  BEFORE removal). verify_postfacts --attempt-dir --tail-declaration.
- Primary smoke tail contract (PINNED): tail result must expose
  result.bound[tag] = {def_id, source_id, run_id} (real handles; the
  driver's tail-bind refuses anything else) and write
  tail/blocked-proof.json = {ok, phase:"pre-abc", entries:{
  <owner-def-uuid>: {http_status, rc, body_sha256, run_id?}}}.
- Root pins pre-tail: tail-spec.json (v2 schema incl. evidence_kind +
  owner def_id UUIDs) and quiesce-expected-defs.json (exact UUIDs).
- finalize: driver calls mode=finalize --meta-dir <EV>-seal-meta; the
  sealer's stdout/rc land OUTSIDE the sealed EV.

## Round 19 (root coordination: frozen ARCHIVE-INTERFACE v2 consumed; priority import paths EXECUTED)

- ARCHIVE-INTERFACE.md v2 / ARCHIVE-CONTRACT.md §1.1 consumed READ-ONLY
  (Archive files never edited). Driver round4 adaptations: canonical
  22-stage step names (preflight/unit/pg-isolated/build/swap/window/
  regressions-*/reread-artifacts/p1post9/restart-regression/
  pre-tail-gates/tail/aux-down/final-images/p21abc/postfacts/persist/
  source-freeze/standalone); driver-written selected-index.json
  {schema,selections} mapping each stage's LATEST steps/<stage>-
  attemptN (attempt-N glob bug caught and fixed); pg-isolated step dir
  carries the bindings copy (§2); build step dir carries
  build-selected-manifest.json; per-stage *report*/*verdict*/*manifest*
  artifacts copied into each stage dir (p1pre9/p23/p22/p24 reports,
  reread-report, restart/final-images verdicts, pre-tail + source-freeze
  verdicts); aux_down quiet proofs print the RAW proxy stats JSON /
  per-src fixture counters list and the raws are copied INTO the
  aux-down step dir under the §3 canonical names; postfacts consumes
  --aux-quiet-dir <latest aux-down attempt dir> + --abc-result
  $EV_ROOT/abc-result.json (p21abc writes it byte-exact, refuses
  overwrite); persist --out-dir persist-attempts; standalone bundle
  form; finalize = selected-index → prepare → BARE seal → BARE reverify
  (stdout/rc in $EV_ROOT-seal-meta).
- PRIORITY SMALL AUDITABLE PATHS — EXECUTED FOR REAL (EV
  results/2026-11-14-p25-final/, all failures preserved):
  * preflight IMPORT (default; P25_PREFLIGHT_MODE=collect stays
    runtime-gated): 4 REF SHAs verified on disk (base summary
    11d77e36…, supp summary 28a13afa…, base-binding a1799498…,
    final verdict 79af7b28… — binding fields cross-checked), then the
    PURE foundation verifier re-adjudicates the SAVED base2+supp with
    the EXACT accepted invocation shape (--attempt-dir ABSOLUTE — the
    base summary records the absolute string and the verifier demands
    string equality; --c-supplement RELATIVE resolved from cwd=tools
    dir; attempts 1-4 honestly refused wrong shapes, attempt-5 green
    all_ok=TRUE; no new live, no GET, no waiver — attempts 1-4 kept).
  * build IMPORT: canonical build step rc0; selected-build.json exact
    full SHAs (connector 6b556702…97592 etc.).
  * swap PURE GATES: swap-source-gate (tree == a1bff07f…ffd27, real
    candidate_source_gate_p25.py) rc0 + swap-build-binding (selected
    build intact + connector == root-reviewed 6b556702… full SHA)
    rc0, THEN honest rc3 at the live boundary (guard/stop/up/health/
    record/images await RELEASED_RUNTIME + guard flag).
  * unit/pg-isolated/fingerprints re-executed under canonical names
    (unit attempt-1 argv-index bug honestly kept; attempt-2 green).
  * finalize path exercised: selected-index maps the 4 completed
    stages (preflight-attempt5/unit-attempt2/pg-isolated-attempt1/
    build-attempt1) and lists the other 18 as MISSING; finalize
    prepare rc1 honestly refuses with stages_refused=18 and ZERO
    problems on the 4 present stages — both tracks' interfaces are
    mutually consistent.
- selftest_p25_runtime_tools.py 43/43 still green.

## Round 20 (review-first-switch-round1 fixes 1-5 — targeted, NO full rewrite) — DONE + pure-verified

Small-candidate SHA256 (combined, sorted lines) over the six changed
runtime-owned files (formal_run_p25.sh, deploy_record_helper_p25.py,
compose-p25-runtime.yaml, check_running_images_p25.py,
selftest_p25_runtime_tools.py, RUNTIME-INTERFACE.md):
**abeb0fa6302d57677e751260f72fdee1aefb90756d06754f38b508631a6457ce**
- fix1: deploy_record_helper_p25.py (NEW runtime-owned) — only true
  path args are Path; project/pinned-sha/cause stay str; driver
  write_deploy_tool now COPIES it into EV/gen (sha logged) instead of
  embedding a heredoc. record_body(argv8, run_cli) is injectable; the
  selftest drives the REAL body with mocked IO (I1-I5: positive record
  + ambiguous-ps/image-mismatch/source-mismatch refusals; IO surface
  ps/inspect/gate ONLY — structurally no stop/up).
- fix2: swap's OWN mandatory PRE-stop gate consumes the EXACT accepted
  build-candidate-bindings-r18.json (all 6 groups cmd/raw/rc/meta by
  path+sha256+bytes, rc==0, hard-gate source == a1bff07f…ffd27, all 6
  tag→full-image ids, cache python:3.12-alpine full id) AND re-hashes
  TODAY'S inputs (base compose, P22 override, PG-generated aux
  override, service.Dockerfile, helper) — any drift refuses BEFORE
  the runtime boundary; live half adds swap-tag-check (docker image
  inspect <proj>-<svc>:latest → r18 full id) BEFORE stop; no implied
  build/pull at up. Does NOT depend on the build phase; P25_BUILD_R18
  override exists ONLY for the tampered-copy selftest negative (J2:
  flipped raw sha → gate rc1, zero stop/tag attempts, reason recorded).
- fix3: swap-steady-old6 — IMMEDIATE steady guard whole-set 27-field
  re-verify on the ACTUAL new boot + full image after health/images,
  rc0 required BEFORE any healthy HTTP (pre_tail re-checks again).
- fix4: PHASES reordered (fixture_up AFTER swap+old6, BEFORE window;
  swap-time all p25 aux absent ✓). fixture_up now raises ALL THREE p25
  services (--no-deps; fixture healthz + proxy admin + connector
  healthz bounded waits). NEW compose-p25-runtime.yaml (runtime-owned,
  env/image ONLY) stacks over q3 compose + my fixture yaml + the
  PG-GENERATED build/compose-p25-aux.yaml: p25fixture/p25-proxy pinned
  to the r18-cached python:3.12-alpine FULL id (no 3.14 pull),
  p25connector DATASET_SERVICE_URL→p25-proxy + egress +p25fixture +
  OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=1 (base loops stay OFF).
  check_running_images expected map now covers p25fixture/p25-proxy
  with the same cached full id (tag/pin machinery removed).
- fix5: window/tail argv per HTTP-INTERFACE v3.2 — actual CLI bases
  (connector 17094 / dataset 17078 / fixture 17092 / proxy 17093),
  tenant JWT env precheck (OPENFOUNDRY_SMOKE_JWT/_ORG2/_ARM present,
  values never printed), tail carries --tail-declaration +
  --tail-bindings; pre_tail derives the ACTUAL bindings from window
  handles.json (bind_from_handle in the root-pinned tail-spec;
  unresolvable ⇒ refuse, no placeholders); post-tail cross-check
  writes tail-bound.json from the SAME bindings + engine rosters.
  arm readiness understanding recorded: armed && scheduler_loop_running.
- hygiene: the accidental repo-root file named a1bff07f…ffd27 (my
  argv-shift-era PG-import output) moved into
  steps/pg-import-attempt1/ (the actual FAILED attempt) +
  ACCIDENTAL-NOTE.txt; preserved verbatim; repo root clean.
- preflight import now also consumes the external
  preflight-accepted.json (status/candidate + every pinned artifact
  cross-checked + the verifier's OWN file sha c35c64a8… must match
  TODAY's tool — a changed verifier requires a root re-pin, never a
  silent pass).
- Executed on the MAIN EV (new attempts, all failures kept):
  preflight rc0 (a6: accepted-json checks green, re-adjudicated
  all_ok TRUE), fingerprints/unit/pg-isolated/build rc0, swap pure
  gates rc0 → honest rc3 at the live boundary.
- Round20 addendum 2 (first-swap narrow re-review final gap): swap-build-binding section 4 no longer treats selected-build.json as optional — it is REQUIRED pre-stop (schema p25-selected-build-v1 + ok/all_ok true + source == pinned + the COMPLETE 7-image map incl. python-3.12-alpine, every id == r18); missing file / corrupt JSON / missing image key all refuse at the gate with ZERO tag-check/stop attempts (new negatives J4/J5/J6; J1 now runs the pure build import first in its temp EV). One NameError (cache used before definition) caught by J1 and fixed (cache extraction hoisted); selftest 56/56; combined SHA updated.
- Round20 addendum: J2b added — r18 file ENTIRELY MISSING also refuses pre-stop (gate rc1, zero tag-check/stop attempts); selftest now 52/52; combined SHA updated accordingly.
- selftest 51/51 (new I1-I5, J1-J3; D-section updated to the full
  expected-map; H3 unchanged). RUNTIME-INTERFACE.md added (tail
  declaration/bindings/deployment-record/quiesce-proof schemas for
  Archive's independent same-shape adjudication — no WIP imports).

## Round 21 (Sol Runtime takeover; root-reviewed narrow live release)

- Selected-build PRE-stop gate now rejects every non-object JSON value,
  including `null` and `[]`; only dictionaries reach `.get`. New J7/J8
  negatives and explicit no-UP assertions passed. Pre-release pure suite:
  **58/58, rc0**. External evidence (before snapshots/exact diff, full raw
  logs and persistent missing/corrupt/missing-key/null/array driver attempts):
  `handoffs/2026-11-14-p25/runtime-null-gate-20261114T161700Z` under the
  visualization workspace. All five gate rc1; no tag-check/stop/up steps.
- Root explicitly authorized ONLY first candidate swap and the one-line
  cohort-guard release exception plus images-collector release. Both flags
  are now True. First sandbox run refused at compose ps (permission denied
  on Docker socket), preserved as firsttransition attempt1, no mutation.
  Necessary escalation then completed the reviewed swap **rc0**: source /
  build-binding attempt14, firsttransition attempt2, tag/stop/up/health /
  deployment record/images/steady-old6/canonical swap attempt1 all rc0.
  New connector boot `2026-11-14T16:19:08.688601428Z`, image
  `sha256:6b556702b34ba399f4ee434963c798faf86b12af294657ec3b144309c0097592`.
  Immediate steady verdict: exact six-cohort, whole schema undrifted.
  Root accepted the actual record. Do NOT rerun first-transition swap.
- Subsequent root release authorized fixture_up only, then fresh only-h1.
  Runtime override now pins p25connector to r18 image
  `sha256:5947232921a29a40a0bdd7d28def5a7c44ea487e5412072e9d1f5837576a72db`
  and exposes `127.0.0.1:17094:50088`. Up explicitly uses --no-deps,
  --no-build, --pull never (installed Compose v5.3.0 help checked).
  Both health steps now fail closed with ||die. Safe rendered config is
  under main EV/fixture-config-review; all three new services have no
  depends_on; cached Python full IDs and scheduler=1 verified. Main
  scheduler remains unset/default=0. fixture-up/fixture-health/
  fixture-connector-wait attempt1 all rc0. Main EV/fixture-start-execution
  before/after.json prove old fixture/Postgres/base3 IDs, boot, PID and
  images unchanged.
- Fresh only-h1, pinned smoke SHA
  `94a57522818d9ec6f49720fc12ef0f26480dc13d159597daa7f824be00589228`,
  ran ONCE and returned rc1. Main EV `h1-trial/result-attempt1.json`;
  raw `h1-trial/attempt1/p25-healthy-20261114T162306` (32 records,
  6 handles). Three failures: `base_epoch=None want 0/1/2`.
  Three actual runs succeeded, checkpoints advanced 1/2/3, ACK/data/
  ledger/view checks passed; final quiet=true, both new rosters empty.
  Root/HTTP worker notified; no retry/reset/new live after this failure.
  JWT minted in memory with seal-derived accepted actor/org and frozen
  P1 acceptance permissions, never written into evidence.
- Root then authorized three READ-ONLY GETs of those SAME existing Run
  IDs (no new trigger/reset). Saved under
  `h1-trial/attempt1/read-supplement/attempt1`; each has cmd/body/status/
  rc/stderr; all HTTP200/rc0. Actual `incremental.mode=cursor`, base_epoch
  strict ints 0/1/2. Every original POST field, including IDs/status/txn,
  equals GET. summary binds exact three IDs and POST/GET byte SHAs plus
  original result SHA `ac01089227f8700006abf62274f748ffb55aa65dcc45423dd71b31bddfff5810`.
  Supplement summary SHA
  `0e3f4648b14f1ad29aa51c58ffb57b2b5e99801eda9ec7e8f2c690c968d9d263`;
  pure verdict.json supersedes ONLY those three missing-on-POST facts.
  Original rc1/summary preserved; root acceptance pending. HTTP worker
  fixed canonical GET after terminal POST (new smoke SHA
  `757f99755ca90f23394060ac3744e76871cf7b184803a1dd6d16ad4bb50b1172`),
  no HTTP rerun performed.
- Existing selftest G4 expects guard unreleased; after this release it
  must be isolated before future pure runs. Do not blindly run that
  subprocess test against a released live guard. The 58/58 evidence
  above precedes flag changes. Full driver remains unaccepted: pending
  HTTP v4 semantic-before/actual-bindings-after tail, h10 independent
  p25connector crash, postfacts aux-down stage lookup, adapter env/SHA
  checks. Runtime interface's old tail/prebinding text is stale; latest
  HTTP/root contracts take priority. No full/window/old regressions/
  h10/tail/ABC/archive sealing authorized by this round.

## Round 22 (actual baseline/old regressions/P24/healthy; failures preserved)

- Readonly pre-baseline collected ONCE, rc0/all_ok, nine frozen steps:
  main EV `readonly-baselines/2026-11-15T002810-readonly-baseline`.
  `00-baseline-pre-path.txt` selects it; phase_window now validates and
  reuses it. New flat-stage lookup fixes aux-down selection.
- Adapter/shim enforce mandatory deployment record + selected-build
  schema/source/seven full image IDs/exact selected bytes SHA and path.
  Child env merges parent then suite then record overrides. Targeted
  injected-IO checks 27/27 rc0; adapter released by Root. Evidence:
  `results/2026-11-15-p25-runtime-adapter/attempt-1` (initial fixture
  bytecode-cache test failure documented honestly), narrow driver
  render/order mocks under `driver-narrow`.
- P1pre9=9/0, P23=8/0 with 1/1 real substitutions, P22=11/0 all actual
  step rc0. P23 and P22 both refreshed deployment record + immediate
  steady old6 rc0. P22 fixed postverify input to frozen top-level service
  keys, eliminating the old nested-map empty-loop false pass. All seven
  local tags/full IDs and deterministic P22 render verified; original
  helper render SHA before/after = accepted r18 8a9411f7…1236.
  Outer `regressions_pre_p24` process returned **2**, NOT phase green:
  Runtime edited its live Bash entry while it ran, changing parser
  offsets after the last completed step. NO suite rerun. Full evidence
  + launch-SHA reconstruction limitation recorded in main EV
  `regressions-pre-p24-driver-failure/execution-summary.json`.
- P24 was then separately implemented/reviewed/released. Canonical
  driver SHA f3623fdecd8de10bfbef70d472061d9f1c1504e7a9e412bd799d5c074d46cf68
  copied into a read-only SAME-DIRECTORY `.formal_run_p25.exec-<sha>.sh`
  snapshot. Keep executing immutable snapshots; never edit an active
  entry. P24 snapshot outer rc0: loop1/three health + full IDs, official
  7/0 + 2/2 guard substitutions, raw sidecars, owned quiesce, restore
  loop-unset/health, new record+steady6, resume p25connector all rc0.
  Root accepted actual P24 facts. Main EV `driver-executions/p24-attempt1`;
  P24 report `steps/regressions-p24-attempt1/p24-report.json`, suffix
  `0045450a93` (TEN hex chars; do not impose eight). Latest main boot
  after restore `2026-11-14T16:48:07.615197094Z`, same accepted full image.
- Root exception let HTTP raise NEW fixture cap5→30s only. Runtime
  reloaded ONLY p25fixture once, rc0; main EV `fixture-reload/attempt1`.
  Seven source counters/ledgers BEFORE reload saved and bound to old h1
  result SHA; same container/image, boot16:21:58→16:49:54.454522213Z;
  new generation all counters0/ledgers[]. Old fixture/PG/base3 ID/boot/
  PID/image unchanged across reload. Fixture SHA
  8a98e7927f4d8a9e237a6c0a98258c48eb6ab0c0f9d2c5b8df24592820e9788b.
- Root released full healthy ONCE. Snapshot above, smoke SHA
  ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2.
  Main EV `window-result.json`, `window/p25-healthy-20261114T165116`:
  465 records / 90 handles, outer+window rc1. h1/2/3/4/5/6/8/9/12 pass;
  h7/h13/h15 fail. Runtime incorrectly narrowed separate ARM JWT to
  connections:write, losing Dataset publish permissions. h13 actual
  scheduled Run errors are commit_output HTTP403, base_epoch0, post-
  publish retained owners. FUTURE ARM MUST REUSE MAIN publish-capable
  actor JWT; never narrow that identity. No tokens written to evidence.
  h7 saved fences actually HTTP409 error-only (no code); HTTP worker
  handles contract correction. h15 busy source-position check under
  savedraw diagnosis. Original result/rc1 intact, no retry/reset.
- Healthy finally quiet=true/nonterminal-new0, but TWO truthful
  failedhealthy owners persist, both source
  58beb454-e456-4514-9379-733bb17d4fe7, blocked requires_reconciliation:
  def800aeeb5-4e6f-477e-aac3-c26c5e8920f4 / run
  cb26c86e-b736-460d-8de3-db39d26a71e6; def
  eedde5e2-2642-4ee3-9b7e-1b9d03baf3e0 / run
  012327b2-0ace-40ba-8277-09fbd256b361. Never clean/adopt them. Final
  semantic declaration must explicitly label their failedhealthy origin.
  HTTP owns new h10_crash_runner_p25.py; initial SHA32a6…5259 release is
  suspended pending baseline-two-owner protection patch/review. Runtime
  must CALL it, never duplicate/edit its implementation.
- Currently NO live active. Conditional base3 restart/h10/tail were NOT
  executed after unhealthy result. Root is reviewing selected healthy
  corrections; future healthy aggregate must select every case's actual
  PASS raw and retain failedhistory/owners, never clear original failed[].
  Later order: healthy green → base3 restart + boot/CP proofs + steady6 →
  strict-old6 canonical pre-tail gate BEFORE held creation → h10 helper →
  freeze semantic+knownbindings → tail → actual per-owner blocked POST
  proof + owned disable/revoke/quiet → auxdown/finalimages → P1post9 +
  artifact/BuildJob reread → finalABC last write → readonly Archive/seal.
- Compose help verification used INSTALLED v5.3.0 (`up --no-build`,
  `--pull never`, `--no-deps`, config JSON); only tool transcript was
  captured then, no separate raw help file exists. No online lookup or
  replacement version is claimed.

## Round23: actual final-write barrier / readonly closure (2026-11-15)

This section supersedes the Round22 pending state above. Root has accepted
actual selected healthy12, h10 crash, semantic/actual tail, pre-ABC owned
quiesce, and the complete final-write batch. No live process is running.

- Main EV: `results/2026-11-14-p25-final`. Original fullhealthy and tail
  attempts both retain rc1. Pure typed aggregates select actual replacement
  facts without overwriting originals: `window-aggregate.json` +
  `window-union-handles.json`; `tail-aggregate.json` +
  `tail-selected-bindings.json` + `h14-supplement-proof.json`. Root accepted
  the original h14 201 COMMITTED ACK / EOF, independent txn/files/download
  raw proof, and retained earlier wrong ACK-file download404.
- H10 actual once succeeded at `h10/attempt1/result.json` using HTTP-owned
  runner21231530…c4f8. Baseline failedhealthy2 retained; held1 added. Frozen
  declaration `tail-declaration.json` bytes SHA
  `eff0223fe593774db8b5c70fdf32bade8555ee48c6daf20354dcdc27b2a7ddd4`;
  actual selected tail has exact8 CP owners and1 nonterminal held Run.
- Pre-ABC mutation producer `owned-quiesce/attempt1` performed exactly31
  grant DELETE200 +31 schedule-disable PATCH200 +8 busy POST409, with
  complete public CP and Run-ID sets unchanged. Original outerrc1 came
  only from grants NULL wire parsing. Five readonly SQL supplement queries
  at `owned-quiesce/attempt1/read-supplement/attempt1` produce strict []
  grants and accepted closure:31 disabled/null,8 owners,0 grants, exact5
  intentional-tail first Run pairs. NEVER rerun the mutation producer.
- Final batch actual allrc0: aux-down → final-images → p1post9 →
  reread-artifacts → p21abc. Only6 auxiliary containers removed, no
  volumes/base/old fixture touched. Final base3 boot/image equal accepted
  restart; all6 aux explicitly absent. P1post9 all9pass; old four-section
  artifact/BuildJob comparison no differences. ABC official3pass0fail,
  partialfalse. `abc-result.json` has new B Run
  `cd6b699c-f80b-4939-a2b9-ec1889e04cd7` and C Run
  `fc812f16-6e9b-4c7b-83d8-464199f33aeb`.
- **FINAL ABC IS THE LAST q3 WRITE. Permanently readonly from now on.**
  No POST/PATCH/DELETE/cleanup/restart/build/new business experiment.
  Root accepted `driver-executions/final-write-batch-attempt1/results.json`.
- Actual readonly postfacts `postfacts-attempts/attempt-1`, canonical
  `steps/postfacts-attempt1`, verdicttrue/reasons[]: controlled9 (old6 +
  h10 + ABC2), CP owners8. `owned-identity-catalog.json` derives whole
  history from380 original inputs/44 distinct Defs and fixed wholeunion
  raw SELECT. Actual tail arguments are derived aggregate/bindings, not
  original rc1 summary or semantic owner UUID placeholders.
- Actual persist attempt1 rc1 retained (empty grants NULL). Archive sole
  patch COALESCE grants to [] gives current collector SHA
  `b966603d426ee7b1313342092cafe075ca8420c8fff9d5b5e649e7708f3365a2`.
  `persist-attempts/attempt-2` / `steps/persist-attempt2` actualrc0,
  failures[], sealed17 rows unchanged (defs17/ticks32/runs15/grants0).
- Sourcefreeze actualrc0: `14-source-hash-post.txt`,
  `14-source-delta-final.json`; candidate Go/SQL/rootmodsum stays
  a1bff07f…ffd27. No Go/PG/build rerun.
- Canonical selected-index was refreshed to actual21 predecessors, with
  standalone self deferred. Standalone actualattempt1 rc1 is preserved:
  consumer issues pretty-JSON swap record, postfacts concise stdout
  shadowing full verdict, and tuple/list JSON comparison. Archive owns
  the narrow pure fixes. No business stage is being rerun.
- Current formal driver snapshot SHA
  `ee3a45f9723b37010fe6a5c4ed04d3d1c4aaac0de94c8d56798e1bfff6b6b991`
  is used immutable in the same directory. Never edit any in-flight bash
  entry. Its old postfacts body is stale; actual readonly collection used
  frozen direct CLI with mandatory selected tail-summary/tail-bindings/
  identity-catalog. Actual CLI evidence is authoritative.

## NEXT (Root gates, current)

1. Archive fixes only the three concrete offline consumer defects;
   rerun standalone packaging capability CLI with failure history intact.
   Refresh selected-index to all22 actual canonical successful selections.
2. Run **bare prepare only**:
   `python3 -I finalize_p25.py prepare --ev EV --tools-dir HERE`.
   stdout/stderr/rc MUST be outside EV (`EV-seal-meta`); do not call the
   combined formal finalize phase because it automatically seals.
3. Report actual22 prepare result to Root. Actual seal/reverify/
   package-final remain NOT released. After explicit Root authorization,
   bare seal → bare reverify → bare package-final logs/rc stay outside EV.
   Canonical standalone is preseal capability proof; package-final is the
   eventual complete sealed EV bundle, not an extra business phase.
4. Keep all original failures and actual read-only supplement references.
   Root coordinates approvals; never re-ask the user for existing scope.

## Standing rules (inherited + round1 additions)
- Never edit P24-or-older files; import/verify-by-hash only.
- P24 pins stay 4; postboot2 separate P25 group-2; exact-27-key schema
  enforced on expected AND live rows; no auto-discovery/enrollment.
- No restart to age rows; legitimate reviewed switch later switches
  guard to steady policy with parameterized target boot/image.
- Tail rows → scoped transition roster + expanded verifier; owner
  roster separate from run roster; unresolved owner never erased.
- Failures archive-first with real cmd/rc; secret-scan → manifest →
  hash-manifest LAST; env600 never printed/archived.
- Bash for drivers; attempts -rN; honest per-name tables; counters
  compared only within the same controlled src cohort; no silent
  empty-table mapping in any collector (catalog discipline).
- Handoff files live ONLY in tools/smoke/business-platform-p2-5/.
- Context budget: target <60%, hard cap 70% — hand off timely.
