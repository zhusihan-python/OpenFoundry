# P25 ARCHIVE-INTERFACE — actual CLI and evidence shapes

Status: **v2.4 implementation reviewed/frozen by Root at 2026-11-14 17:37 UTC. Runtime-only readonly collectors are enabled strictly after final ABC actual PASS. Actual seal/package-final remain unreleased.**

CURRENT RUNTIME READS **ONLY THIS FILE** on the archive side (root
ruling). Runtime GLM consumes it READ-ONLY and adapts
`formal_run_p25.sh` accordingly (the archive group does NOT edit the
driver; the archive group's five scripts + offline verifier are
archive-exclusive — Runtime must not edit them, including
verify_postfacts_p25.py). Any change to this surface needs a
root-coordinated version bump here first. Runtime's forthcoming
`RUNTIME-INTERFACE.md` (frozen declaration / actual-identity data
schema) MUST stay consistent with §4.5 below — same declaration
schema, same 27-key rows, same exact dual-roster semantics; the
archive verifier adjudicates the FROZEN ARTIFACT independently (no
import of WIP modules) and will not regress to toy counting.

## v2.5 h14 wire supplement (Root-authorized narrow reopen)

The original tail process rc1, raw h14 proxy `verified_committed:false`, raw summary and original bindings remain immutable. Only `EV/tail-aggregate.json` with schema `p25-tail-aggregate-v1` can select this supplementary branch. Archive actually calls `verify_h14_supplement_p25.verify_supplement(path, resolve_path=...)` before using it. The pure helper must rebuild the two known h14 gate replacements from original raw201 nested ACK, local EOF/NULL txn/CP0 retained owner, and the three independently captured readonly remote HTTP responses; all other gates/business fields/8 owner identities/1 held nonterminal/spec are unchanged. A normal-looking summary with flags flipped is rejected by the original recorder comparison.

The helper produces `EV/h14-supplement-proof.json`, derived `EV/tail-aggregate.json`, and NEW `EV/tail-selected-bindings.json`. The actual bindings map/declaration SHA/bindings SHA remain equal to original; only the new summary byte SHA and explicit original provenance change. Normal successful tail-result paths remain supported.

Runtime uses an actual new successful supplement/aggregate CLI step for canonical `tail` (original failed tail attempt stays intact). Existing postfacts CLI flags remain identical, but selected inputs become `--tail-summary EV/tail-aggregate.json --tail-bindings EV/tail-selected-bindings.json`. Both `v_tail` and independent `verify_attempt` call the same raw-verifying selector. Standalone's same canonical callbacks therefore replay the typed proof as well, with logical source metadata preserved and actual reads explicitly rebound into the bundle.

No original declaration, raw tail or binding document is overwritten. This reopening is code/pure only; actual readonly collection and finalization remain Runtime/Root controlled under the existing finalABC and seal authorization boundaries.

Actual supplementary artifacts now exist and Archive pure replay is green: selected aggregate SHA08634838…eb6c /newbinding49bab5dc…8193, original tailrc1 unchanged, canonical new tail-attempt2rc0. Current wire candidate hashes: finalize576b4da2…00dd7 /verify_postfacts84aa60fe…1bcf4; closure attempt-8 contains the3 scoped tests and actual copied-dependency validation. Root narrow acceptance of this wire candidate remains pending; the earlier reviewed collector/finish lifecycle authorization boundaries are unchanged.

## v2.4 authoritative integration notes (override older examples below)

- `verify_postfacts_p25.py --attempt-dir A --tail-declaration D --tail-bindings B --tail-summary S --identity-catalog C --out V` consumes actual **after-execution** HTTP output B/S. HTTP itself receives only the semantic declaration D; it does not receive B/S as execution inputs.
- Returned V includes `attempt_dir`, `tail_declaration`, `tail_bindings`, `tail_summary`, `identity_catalog` absolute paths. Finalizer reruns the pure verifier using these immutable inputs. Declaration schema remains `p25-tail-roster-v1`; canonical SHA is `sha256(json.dumps(spec, sort_keys=True).encode())`. Bindings schema remains `p25-tail-bindings-v1`, field `bindings`; `run/` keys are distinct from owner keys. Binding SHA, declaration SHA, final summary byte SHA and exact separate owner/nonterminal-run sets are mandatory.
- Actual CP owner reason is `requires_reconciliation` for failed or lease-expired running; pending/live running can have `""`, matched to the specific declaration. h10 held-prepublish crash does not claim publish dispatch/unknown commit. No future UUID goes in semantic spec; known existing h10 identities remain outside `spec`.
- Canonical selection requires original cmd/stdout/stderr/rc0. Canonical PASS is produced by its real semantic validator; actual HTTP/frozen-suite result JSON does **not** need an invented generic `ok:true` wrapper.
- Healthy `window-result.json` uses actual `scenarios`/gate objects/`failed`/quiet/recorded scenario-result bytes. Registry excludes driver-coordinated h10 and tail scenarios; h5 attribution-only empty gates remain legitimate. Tail uses actual `tail-result.json` with `scenarios`, `tail_adjudication`, `final_owner_roster`, `final_run_roster`, `tail_bindings_file`.
- Frozen regressions use their actual `results`, exact source-defined `SCENARIOS`, counts, suffix, and (P22/P23/P24) `official:true, partial:false`. No arbitrary passed-name list is accepted.
- `final-images` reads existing EV `final-running.json` and `.raw` ps/inspect original rc/bytes. It replays all nine services: new r18 base3 full IDs/count1/running, all six aux count0. Accepted r18 SHA is fixed a4e577…feef.
- `reread-artifacts` reruns pure P22 `compare_baseline.compare(before, after, "restart")` against the actual saved manifests and compares `12-reread-compare/compare.json` byte-derived JSON facts. No invented artifact-list wrapper.
- persist has archive-owned `collect_persist_state_p25.verify_attempt(A)`: canonical SQL/target, original rc, exact raw membership and SHA, raw JSON versus derived JSON, sealed17 defs/ticks/runs/grants **full row set** equality; source/target/schedule/history fields cannot be replaced while retaining IDs.
- Postfacts canonical plan now also reuses reviewed preflight safety fact commands for global grants, legacy13, fault absence, volumes, env modes, original fixture boot/image/PID1. It does not reuse pre-switch base boot adjudication. Global cron uses `05c` IDs plus independent unfiltered `05d` full rows. The `p25-owned-identity-catalog-v1` input is rederived from every EV P24 report, handles file and observed safe def response, plus ONE original fixed-whole-union def read; unknown global rows fail. Runtime integration remains pending.
- Standalone now copies complete EV (including nested `.pyc` evidence), PG/build raw roots, base+supp preflight, P24 full859/tools dependencies and source hash index. Explicit `path_maps` rebind original references to copied roots. Offline verifier reuses the canonical finalizer validators and allows only the saved-log Python exact checker/source enumeration subprocesses, never Go/Docker/HTTP/SQL. The single standalone receipt is deferred inside the pre-seal package to avoid recursion; Root has approved the two-step lifecycle: `preseal-capability` only validates packaging plus all predecessor stage semantics; the final COMPLETE EV package is built after hashLAST outside EV.
- Runtime agreed to write selected-index before standalone and write prepare stdout/stderr/rc outside EV. Prepare locks validated EV bytes/file set, external raw/bundle bytes, tools full set, and source fingerprint. Seal rechecks fixed old859 seal and exact set. Existing 94/0 synthetic suite is not acceptance of these new shapes; use targeted `selftest_p25_archive_closure.py`, currently 11 tests (10 batch green plus new identity-catalog targeted check green).

Finalization CLI after prepare/lock: bare `seal --ev E --meta-dir M`, bare `reverify --ev E --meta-dir M`, then bare `package-final --ev E --tools-dir T --meta-dir M`. M MUST be outside E and all stdout/stderr/rc stay in M. `package-final` rechecks source seal first, creates M/final-bundle/attempt-N collision-safely, copies COMPLETE sealed E including root hash-manifest, verifies the original/copy SHA mapping against the source seal/tools map and the same 21 nonrecursive semantic validators, retains exact offline cmd/stdout/stderr/rc and receipt in M, and checks E did not change. The final artifact scope is `sealed-final`; preseal capability is never final delivery.

Identity catalog descriptor: `inputs[{path,sha256,kind}]`, where kind is `p24-result`, `p25-handles`, `p25-def-create-response`, `p25-def-read-response`, or `catalog-def-read`. The final kind additionally has cmd_path/cmd_sha256, rc_path/rc_sha256, stderr_path/stderr_sha256. Whole-union SQL comes ONLY from `collect_postfacts_p25.identity_def_sql(ids)` with existing `psql_argv(fixed_env, sql)`. Raw read columns are exactly id/source_id/cron/enabled/cursor_null; all fresh known defs must be disabled/NULL. `defs[{def_id,source_id,cron,derived_from[]}]` is a checked cache, never an allowlist. All actual history must be listed and rehashed, including failures. Final postfacts verifier now requires `--identity-catalog C`.

Remaining concrete integration blockers: Runtime catalog/finalize wiring; base3 restart versus separate h10 actual crash record validation; HTTP's explicit healthy aggregate of original full rc1 + exact3 successful retries. Aggregate must preserve all origin receipts/handles and be independently replayed. No P25 completion claim.

## 0. Authoritative reference values (root coordination 2026-11-14)

- Candidate/PG source SHA256 (THE binding; earlier "379 source" was a
  typo — it is **378 Go/SQL files + root go.mod/go.sum**):
  `a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27`
- PG green already raw-verified by root: normal 15 / race 15 /
  unified 72+15=**87**; interface artifact
  `pg-candidate-bindings-r17.json` + `BUILD-HANDOFF.md`.
- These are IMPORTED as selected proof (§2). Nothing green is rerun.

## 1. EV layout + the CANONICAL selected-index the finalizer enforces

```
$EV_ROOT/
  steps/<stage-id>-attempt<N>/{cmd.txt, stdout.log, stderr.log, rc}
  selected-index.json          # driver-written (schema below)
  manifest.json                # finalize prepare writes
  tools-sha.json               # finalize prepare writes
  hash-manifest.json           # finalize seal writes (LAST EV write)
```

`selected-index.json` — keys must be EXACTLY the 22 canonical stage
ids (EXACT matching; substring `--require` is gone; the canonical set
cannot be shrunk by omitting `--require`):

    preflight unit pg-isolated build swap window
    regressions-p1pre9 regressions-p23 regressions-p22
    regressions-p24 reread-artifacts p1post9 restart-regression
    pre-tail-gates tail aux-down final-images p21abc
    postfacts persist source-freeze standalone

Each selected dir needs `cmd.txt` + `stdout.log` + `stderr.log` +
`rc`==0 + an EXPLICIT true verdict (`*verdict*/*report*/*manifest*`
JSON artifact OR the stdout LAST non-empty line as JSON — every
archive CLI tool already prints that machine verdict). Per-stage
REAL-CONTENT schemas (round3-hardened; see ARCHIVE-CONTRACT §1.1):
- window cases each carry an ACTUAL raw artifact reference (file
  present + non-empty in the step dir); regressions carry raw_files;
- final-images anchors = the r18 CANDIDATE image ids (post-swap;
  the P24 boot anchors are pre-swap history) + a non-empty ps raw;
- source-freeze triggers a FINAL source-fingerprint RECOMPUTE (the
  exact PG/build recipe) that must equal `a1bff07f…ffd27`;
- postfacts/persist/standalone run their INDEPENDENT verifiers at
  prepare time (verify_postfacts.verify_attempt; persist files-ledger
  replay + sealed-17 completeness; the self-contained offline
  verifier as a real subprocess over the bundle incl. its externals
  PG r17 / build r18 / preflight-accepted / P24 seal pair) — stage
  summaries alone are never sufficient;
- the standalone bundle enumerates the EV COMPLETELY (evidence
  trees are never results/build-excluded).
`06b…`/`*informational*` keys are tolerated without rc0 but never
substitute; unknown extra keys are recorded unadjudicated. SEAL
locks: manifest final_ok true + zero input drift (tools-sha entries
re-hashed; frozen P24 imports re-verified).

## 2. Importing already-green evidence (PG / build / unit / preflight)

NEVER rerun green — prepare itself consumes the pinned REAL trees and
re-hashes every imported artifact into `manifest.imported_green`
(NOT just JSON counts):

- **pg-isolated** (r17, per-name semantics): the driver puts a copy
  of `pg-candidate-bindings-r17.json` in the step dir; prepare
  re-hashes all THREE real stage trees (.cmd/.raw/.rc/.meta/
  .source-gate.rc/.version-gate.rc; every rc file == 0), requires
  each of the 15 required names to be a top-level `--- PASS` in EVERY
  stage raw (never SKIP; new stages also top-level PASS == 15; FAIL
  == 0 anywhere), each stage's exact_verdict == the pinned string,
  and the unified raw's `required-test guard status: 0` + names chain
  15 / `old72 + new15 = 87` + source `a1bff07f…ffd27`;
- **build** (r18, root-accepted): consumed via
  `build-candidate-bindings-r18.json` — the EXACT 6 selected green
  groups (`render-aux 142804Z, build-base3 142804Z, build-oldaux2
  142810Z, build-newaux1 142811Z, cache-python 142814Z, source-gate
  142814Z`); the failed 142634Z round is preserved history and NOT
  selected. NO ls -t / latest / wildcard selection: every cmd/raw/
  rc/meta is re-hashed (sha256 AND bytes) at its pinned path, rc
  content == 0 == real_rc, hard gates source rc0 + `a1bff07f…ffd27`;
- **unit**: prepare re-hashes the accepted attempt20 (38 ok / 0 FAIL /
  rc0) and attempt18 race (4 ok / rc0) logs directly;
- **preflight — OFFICIALLY CLOSED (no disposition pending)**: imported
  via the root-issued external `…/handoffs/2026-11-14-p25/
  preflight-accepted.json` (`status=first-transition-preflight-
  accepted`): ALL pinned artifacts re-hashed on disk against their
  recorded sha256s — base2 `verify-csupp.stdout` (verdict sha
  `79af7b28…93a6c6`, must parse all_ok TRUE) + `verify-csupp.rc`
  (0), `collect/attempt-1` summary, `c-supplement/attempt-1` summary
  + `base-binding.json`, verifier `verify_preflight_p25.py` (sha
  `c35c64a8…77a7ba`), `c_live_reverify_p25.py`. The anchor is
  verify-csupp — NOT the historical verify.stdout 404. C is NOT
  exempted (the supplement chain is part of the accepted set); root +
  readonly agent independently 3-fixed / all-raw-verified; actual
  41 pure/0 + saved-data re-adjudication rc0; no live reread.

## 3. aux quiet proofs (aux_down phase MUST produce RAW)

Post-ABC postfacts does NOT contact the removed p25 proxy/fixture; it
consumes these ARCHIVED files from the SELECTED aux_down attempt dir
(`--aux-quiet-dir`):

- `proxy-quiet-proof.stdout.log` — the RAW stats JSON object:
  `{"commit_started": <int>, "commit_finished": <int>,
    "commit_inflight": 0, ...}` — STRICT non-bool ints,
  `commit_started == commit_finished`, `commit_inflight == 0`.
  The CURRENT driver prints only a pass line — change the step to
  print the stats JSON itself (the raw proof, not a boolean).
- `fixture-quiet-proof.stdout.log` — JSON LIST of per-src fixture
  counter bodies: `[{"name": "<src>", "active": 0, ...}, …]` — every
  `active` a strict non-bool int 0 (add this step; GET
  `http://127.0.0.1:17092/admin/src/<name>` per p25 src, print the
  bodies as one JSON list).

Missing/malformed = honest collection FAILURE downstream (by design).

## 4. postfacts phase (after p21abc; strictly readonly)

```bash
# collect (flag currently RELEASED_FOR_EXECUTION=False; root flips):
run_step postfacts-collect python3 -I "$HERE/collect_postfacts_p25.py" \
  --collect --out-dir "$EV_ROOT/postfacts-attempts" \
  --env-dir "$Q3_ENV_DIR" \
  --aux-quiet-dir "$EV_ROOT/steps/aux-down-attempt<N>" \
  --abc-result "$EV_ROOT/abc-result.json"
# verify (rc0 = gate pass; verdict JSON on stdout, LAST line):
run_step postfacts-verify python3 -I "$HERE/verify_postfacts_p25.py" \
  --attempt-dir "$EV_ROOT/postfacts-attempts/attempt-<N>" \
  --tail-declaration "$EV_ROOT/tail-declaration.json" \
  --out "$EV_ROOT/steps/postfacts-verify-verdict.json"
```

- The p21abc phase must place THIS round's raw `result_p21.json` at
  `$EV_ROOT/abc-result.json` (byte-exact copy; the collector re-copies
  it into its attempt with sha binding).
- Collector artifacts land under
  `postfacts-attempts/attempt-N/…` (attempt dirs never overwritten);
  the verifier accepts ONLY `--attempt-dir` (+ hash-bound tail
  declaration) and rederives every expectation from the P24 seal.
- rc: collect 0 = all steps executed clean (missing quiet raw ⇒ 1);
  verify 0 = every gate green.
- BOTH tools print a machine verdict as the LAST stdout line
  (`{"ok": …}`) — with or without `--out`; that last line is the §1
  verdict evidence for the finalize selection.

### 4.5 tail declaration + identity data schema (FROZEN; the artifact
Runtime's RUNTIME-INTERFACE.md must match)

`$EV_ROOT/tail-declaration.json` (hash-frozen pre-tail; the verifier
records its sha256 in the verdict):

```json
{"schema": "p25-tail-roster-v1",
 "ok": true,
 "spec": {
   "run_rows": [
     {"def_tag": "p25-tail-x", "source_tag": "p25h11",
      "semantics": "p25-drop-noack", "count": 1}],
   "owner_rows": [
     {"def_tag": "p25-tail-x", "run_ref": "terminal-failed",
      "blocked_reason": "requires_reconciliation"}],
   "produced_by": "p25-tail"},
 "declaration_sha256": "<sha256 of canonical JSON of spec>"}
```

- `semantics` vocabulary (frozen): `B-lease-loss`,
  `C-committed-unconfirmed`, `p25-drop-noack`,
  `p25-crash-held-prepublish`, `p25-local-commit-unknown`; `count` is
  a strict non-bool int ≥ 1;
- OPTIONAL actual identities (may ALSO be carried in the attempt
  facts as `tail_bound_identities`): `{"<tag>": {"def_id": <uuid>,
  "source_id": <uuid>, "run_id": <uuid>}}` — UUID-to-UUID exact; one
  run never satisfies two tags; each bound run needs an owner row.
  When present they are MANDATORY (malformed = refuse), never
  optional-when-declared;
- the verifier enforces on the artifact, independently of any roster
  module: declared tail count == actual tail rows EXACTLY (extras and
  missing both block); EVERY tail row is the exact frozen 27-key
  controlled projection (guard `expected_schema()`); declared
  terminal-failed owners RETAINED; every owner row carries the P26
  hold reason; final controlled set is EXACTLY old6 + ABC2 (derived
  from the raw P21 result: official/partial/counts 3-0, real 3-case
  keyset, suffix→fixture_src→Source/Def/Run parents walk, C
  cross-checked against `logical_path`) + declared tail k. The
  nonterminal RUN roster and the CHECKPOINT OWNER roster are
  adjudicated SEPARATELY; owner rows may reference terminal failed
  runs (never cleaned/adopted). Owner-row projection is pinned in
  ARCHIVE-CONTRACT §4 (real DDL `sync_def_checkpoints`;
  `blocked_reason` derived from `run_status`).

## 5. persist phase

```bash
run_step persist python3 -I "$HERE/collect_persist_state_p25.py" \
  --collect --out-dir "$EV_ROOT/persist-attempts" \
  --env-dir "$Q3_ENV_DIR"
```

Per-table psql (sealed-17 owned set + REAL `sync_def_checkpoints`
owner snapshot), cmd/rc/stdout/stderr raw per table, `persist-manifest
.json`. The tool prints a machine verdict (`{"ok": …}`) as the LAST
stdout line — that line is the §1 verdict evidence. rc 0 = every
table rc0 + hard checks green.

## 6. standalone phase

```bash
run_step standalone python3 -I "$HERE/standalone_p25.py" \
  bundle --src-ev "$EV_ROOT" --tools-dir "$HERE" \
  --bundle-dir "$EV_ROOT-standalone"
```

rc0 requires full replay + the self-contained offline verifier green.
(`sanitize`/`reverify`/`verify-bundle` subcommands also exist.)

## 7. finalize phase — hash LAST, sealer stdout/rc OUTSIDE the EV

`run_step` writes rc/stdout into the EV AFTER the child exits — a
post-seal write. The sealed interface is two-step:

```bash
# pre-seal (EV still open; run_step fine; the 22-stage canonical set
# is ALWAYS enforced — --require only adds exact-key assertions):
run_step finalize-prepare python3 -I "$HERE/finalize_p25.py" prepare \
  --ev "$EV_ROOT" --tools-dir "$HERE"

# seal — BARE invocation, all output OUTSIDE the EV (do NOT run_step):
python3 -I "$HERE/finalize_p25.py" seal --ev "$EV_ROOT" \
  --meta-dir "$EV_ROOT-seal-meta" \
  > "$EV_ROOT-seal-meta/seal.stdout" \
  2> "$EV_ROOT-seal-meta/seal.stderr"
echo $? > "$EV_ROOT-seal-meta/seal.rc"

# final independent readback (also bare; MUST rc0):
python3 -I "$HERE/finalize_p25.py" reverify --ev "$EV_ROOT" \
  --meta-dir "$EV_ROOT-seal-meta" \
  > "$EV_ROOT-seal-meta/reverify.stdout" \
  2> "$EV_ROOT-seal-meta/reverify.stderr"
echo $? > "$EV_ROOT-seal-meta/reverify.rc"
```

rc semantics: prepare 0/1; seal 0/1 (refuses without prepare, refuses
double-seal, re-scans secrets at seal time); reverify 0/1 — ANY file
added/modified in the EV after the seal is a post-seal write ⇒ rc1.
`--require` names must each match a selected-index key (substring).
A compat single-shot `finalize` mode exists for manual use; the formal
run uses the two-step form above.

## 8. Window/smoke verdict requirement (Primary-facing, via driver)

When the `window` phase is selected, its stdout must END with a JSON
verdict object (`{"ok": true, …}` last non-empty line) or carry a
`*verdict*.json` artifact — absent/null never passes finalize.


## Root-reviewed frozen implementation, 2026-11-14T17:37:41Z

Root has completed static review of the actual PG, old-suite/HTTP schemas, canonical postfacts safety/catalog/raw replay, exact known/dual rosters, prepare→hashLAST and outside final-package implementation. Root accepted independent HTTP reviewer isolation tests over 989 aggregate dependencies and 1358 h10+aggregate dependencies with original directory IO refused, and Archive actual v_window/v_restart both return zero reasons. h10 baseline omission is closed by ordered HTTP raw replay, not caller snapshots/report.ok. It rebuilds before144 calls and after final155 calls for the actual24 baseline defs, anchors old two owners' complete public CP and protected Run fields to ORIGINAL failedhealthy HTTP history, checks before/after records/SHA/body bytes/order/whole handles/known3/origins, and rejects coordinated generation tampering across before+after raw, summaries, ledgers and hashes.

Only two release lines changed on Root explicit authority: collect_postfacts and collect_persist RELEASED_FOR_EXECUTION=True. Original before/after SHA and condition are recorded in results/2026-11-15-p25-archive-closure/attempt-5/root-readonly-release.json; frozen implementation code SHA map is sibling frozen-implementation-sha.json. No Archive worker live IO occurred. Runtime is the only executor, after finalABC actual PASS. All other implementation code is frozen pending an actual concrete issue.

Actual proof evidence: closure attempt-2:12 targeted pure tests rc0; attempt-3 nested-control fullindex relocation test rc0; attempt-4 oldfailedprepare lifecycle + h10 rawrc + catalog negatives rc0; attempt-6 actual h10 source replay/fully coordinated copied-generation tamper and real v_window/v_restart canonical replay rc0. The lifecycle negative intentionally isolates canonical callbacks and the final real offline verifier refuses absent business facts: it is not an all21 actual positive claim. Source root3 prepare controls may legitimately change after preseal capability; only those exact root paths (not nested names) are exempt from predecessor comparison, while the final sealed bundle includes and verifies every final root control/seal/tool map.

Runtime still must finish tail, readonly catalog/postfacts/persist, fresh selected-index from actual attempts, and real22-stage prepare. Root must review before actual seal/package-final release. Current main EV's old attempt-MISSING selected-index is historical and must only be regenerated when the actual preceding stages are complete; failed attempts/old controls remain preserved. No whole P25 completion claim.


Root-accepted persist empty-set wire fix: actual persist a1rc1/blank grants wire remains rejected and preserved. Only grants SELECT now uses COALESCE(json_agg(row_to_json(t)), '[]'::json), guaranteeing explicit [] for empty rows. Strict parser unchanged; other4SQL projections unchanged. Collector SHA b966603d426ee7b1313342092cafe075ca8420c8fff9d5b5e649e7708f3365a2 replaces db2f9f...2bf. Root authorized Runtime readonly persist-a2 only, existing CLI unchanged; no repeated postfacts/HTTP/Go/PG/build/newwrite. See closure attempt-9 exact single-line diff and targeted oldblank-reject/explicitarray-pass evidence.


### Root-authorized real standalone-a1 consumer repairs — frozen for a2

Actual standalone a1 rc1 and EV-standalone destination are preserved. Three true wire/consumer fixes: v_swap parses its complete pretty stdout JSON; v_postfacts prioritizes exact same-step postfacts-verdict.json over its compact stdout receipt; v_reread JSON-roundtrips frozen compare output so tuple counts match the actual JSON-list wire, retaining full equality/source_gates/all mandatory sections. No old P22 or original evidence changes.

Full actual failed-a1 bundle was copied toTMP, only new owned consumer code overlaid, all original mapped directories denied through io.open/builtins.open: swap/reread/fullpostfacts all reasons=[]. Deeper postfacts archive-path alias is adapted only for fixed logical P24 default vs its explicit physical copy, preserving every guard exact27/type/group4+2/fixedIDs/anchors/fullimages/byteSHA/fullsealed-PROVENANCE check. Existing host guard path remains unchanged. Pure plan/replay factories accept only internally supplied already-validated roster to avoid duplicate alias checks; default/CLI/IO/SQL and original a95f8ebd72e88c2f9d7a74a2a62c1f4469e9ade99b4df8fed827055713e5e351 plan remain unchanged.

Fresh a2 selection comes ONLY from selected standalone real cmd --bundle-dir with strict srcEV/toolsDir/actualrc0/receiptmanifestSHA/source meta validation; no glob/canonical-old guess. Same actual bundle content is covered by prepare external lock. A1 stays intact. Runtime may now only rerun standalone a2 then refresh actual22 selected index and bareprepare with stdout/stderr/rc outsideEV. Actual seal/package-final still not authorized. Entire Archive implementation/docs freeze after this entry; only concrete actual issue can reopen. Evidence closure attempt-10/actual-copy-consumer-result.json and frozen-code-sha.json.


### Root-authorized a2 completion-ledger lifecycle repair (freeze for a3)

Actual a2 standalone offline21+package0 passed; bareprepare-r1 failed only because manifest.jsonl necessarily appends its own selected run_step receipt after copy. Root authorized a precise append-only check, not a ledger exemption. Captured old prefix length/SHA must match; suffix must be exactly one JSONL row with the actual6 keys step/attempt/rc/seconds/cmd/dir. step=standalone, strict integer attempt matching selected, strict rc0 and nonnegative integer seconds, exact selecteddir and originalselected cmd without CMD prefix. The selected command's actualrc/receiptmanifestSHA checks remain in force. Unknown append/old byte changes refuse; full final ledger enters prepare lock and complete sealed bundle.

Actual a2 positive passed; TMP old-line mutation and extraappend both refused; originals unchanged. Runtime fresh a3 contains the new tool byte, then actual22 index and bareprepare-r2 EV-external outputs. a1/a2/prepare-r1 all preserved; no business/live rerun or seal authorization. finalize SHA ee90388deef013f9cfb216a4d89465a396f0ecaf7daa5639aceca5ebea66f6f6. All Archive implementation/docs freeze after this entry. Evidence closure attempt-11/actual-ledger-append-check.json.
