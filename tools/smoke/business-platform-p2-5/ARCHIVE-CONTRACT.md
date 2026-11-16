# P25 ARCHIVE CONTRACT (round: review-archive-tools-round2 closure)

Owner: Archive GLM (this file's group: `finalize_p25.py`,
`standalone_p25.py`, `collect_postfacts_p25.py`,
`verify_postfacts_p25.py`, `collect_persist_state_p25.py`, plus NEW
`offline_verify_p25.py`, `selftest_p25_archive.py`,
`run_archive_selftests_p25.sh`, this contract, `HANDOFF-ARCHIVE.md`).
Old-Tools no longer write this group (root handoff 2026-11-14).

## 0. Completion criteria (mechanical; reviewed against actual runs)

`run_archive_selftests_p25.sh` exits 0; its real cmd/rc/stdout for every
step land under `results/2026-11-14-p25-archive/attempt-N/`; inside it:

1. `python3 -I -m py_compile` on all 6 owned/new python files → rc0.
2. `selftest_p25_archive.py` → ALL checks pass, including the six
   round-2 defect negatives:
   - G1 finalizer: selected-index strict schema; absent/null verdict
     REFUSED; historical failed attempt does NOT poison; required-step
     missing REFUSED; tools-sha covers json/txt/yaml/fixtures; root-accepted
     PG green (15/15/87, source `a1bff07f…ffd27`) imported via actual
     bindings-file SHA content, never rerun.
   - G2 seal: after `seal`, ANY EV write is detected by `reverify`
     (negative writes a file post-seal → rc1); seal itself writes ONLY
     `hash-manifest.json` (snapshot proof in the outside meta dir);
     sealer stdout/rc land OUTSIDE the EV (meta dir), per §2 interface.
   - G3 standalone: byte preservation (binaries exact, CRLF/no-trailing-
     newline preserved, undecodable lines verbatim); immutable fresh
     dst; bidirectional full index; exact-sanctioned-transform replay
     rejects unapproved same-line changes; empty/missing src REFUSED;
     bundle (tools + frozen deps + offline verifier) re-sealed and
     independently verified; tamper/delete/empty bundle REFUSED.
   - G4 ABC derivation: REAL frozen `10b-p21-ABC/result_p21.json`
     shape (official/partial/counts + 3-case statuses; keys
     `heartbeat_progress`/`lease_loss_fencing`/`terminal_commit_fault`;
     NO convenience id field); B has NO direct UUID — derived via
     suffix→fixture_src (`p21b-<suffix>`/`p21c-<suffix>`)→Source/Def/
     Run parents from collected rows; C cross-checked against
     `evidence.terminal_commit_fault.logical_path`; old6+ABC2 exact,
     unique, parents-bound.
   - G5 collectors: ACTUAL readonly IO entry (`collect_body` with
     injectable `run_cli`/`http_get`; `--collect` refuses rc3 while
     `RELEASED_FOR_EXECUTION=False` with ZERO subprocess/HTTP
     constructed); cmd saved BEFORE execution; original rc/stdout/
     stderr raw; raw-byte SHA + same-attempt membership ledger; caller
     JSON never accepted as execution proof; missing table/quiet raw
     is honest FAILURE, never defaulted empty/rc0.
   - G6 postfacts: full seal-derived content gates (typed non-bool int
     counters, HTTP 200, name self-identification, inventory shape,
     read1==read2); aux-removed quiet consumed from ARCHIVED
     pre-removal RAW proof bound to the aux attempt (no post-hoc HTTP
     to the removed proxy/fixture, no caller booleans); dual roster
     (nonterminal RUN roster vs CHECKPOINT OWNER roster incl.
     terminal-failed owner refs) adjudicated separately and exactly.
3. No live HTTP/listener/Go/DB/Docker/q3 access happens anywhere in
   the selftests (mocked/injected IO only; real CLI invocations run
   only pure/draft-refusal paths).

## 1. Evidence schemas this group produces/consumes

### 1.1 selected-pass index (finalize INPUT, driver-written)

`<EV>/selected-index.json`:
```json
{"schema": "p25-selected-index-v1",
 "selections": {"<stage-id>": {"attempt_dir": "steps/<stage-id>-attempt2",
                                "attempt": 2, "note": "..."}}}
```

**Canonical stage set (root re-pin, round2 #1/formal #3)**: prepare
enforces a FIXED set of 22 required stages; each maps to an EXACT
selection key (substring matching is gone, and an empty `--require`
cannot shrink the gate — `--require` only ADDS exact-key assertions):

    preflight  unit  pg-isolated  build  swap  window
    regressions-p1pre9(9)  regressions-p23(8)  regressions-p22(11)
    regressions-p24(7)  reread-artifacts  p1post9(9)
    restart-regression  pre-tail-gates  tail  aux-down  final-images
    p21abc  postfacts  persist  source-freeze  standalone

Every selected dir needs: `cmd.txt` (non-empty), `stdout.log`,
`stderr.log`, `rc` == 0, and an EXPLICIT true verdict (`*verdict* /
*report* / *manifest*` JSON artifact or the stdout LAST non-empty line
as JSON; absent/null/false refuse). Per-stage REAL-CONTENT validators
(no same-dir arbitrary JSON can substitute):

- `unit`: accepted connector evidence re-hashed from the pinned
  evidence tree — attempt20 full suite exactly 38 `ok` / 0 FAIL /
  rc0, attempt18 race 4 `ok` / rc0;
- `pg-isolated`: bindings copy + all THREE real stage trees consumed
  (`.cmd/.raw/.rc/.meta/.source-gate.rc/.version-gate.rc` re-hashed,
  every rc file reads 0, raw 0-FAIL/≥1-ok) + names chain (15 active in
  the sibling required-names file == bindings) + unified string
  `old72 + new15 = 87` + source `a1bff07f…ffd27`;
- `build`: consumed via `build-candidate-bindings-r18.json` — the
  EXACT 6 root-accepted green groups at pinned paths, each cmd/raw/
  rc/meta re-hashed (sha256 AND bytes, no latest/wildcard selection),
  rc content 0 == real_rc, hard gates source rc0 + `a1bff07f…ffd27`;
- `preflight`: OFFICIALLY CLOSED — imported via the root-issued
  external `preflight-accepted.json` (status first-transition-
  preflight-accepted): every pinned artifact re-hashed on disk ==
  recorded sha (final verdict `verify-csupp.stdout` sha
  `79af7b28…93a6c6` parses all_ok TRUE; verifier
  `verify_preflight_p25.py` sha `c35c64a8…77a7ba`; anchor is
  verify-csupp, not the historical verify.stdout); C NOT exempted;
- `window`: exact case set h1..h15, unique, all pass, non-empty gate
  lists; `tail`: {h11,h14} pass;
- regressions/p1post9: exact counts with unique passed-name lists;
- `p21abc`: REAL result shape (official/partial/counts 3-0/three real
  case keys pass) + 8-hex suffix NOT in the historical suffix set
  (identity);
- `aux-down`: BOTH archived quiet raws re-validated (strict non-bool
  ints); `final-images`: base3 images == root-pinned full anchors,
  p25+old-aux explicit int 0;
- `postfacts`/`persist`/`source-freeze`/`standalone`: their real
  artifact schemas (roster sizes equal + ABC UUIDs; tables rows with
  defs==17/grants==0; removed==[]; bundle control files present).

PREPARE-INTERNAL MANDATORY (no selection can waive): the old P24
archive re-verified BY RE-HASH (859 files == hash-manifest; 27 tools
== 16-tools-sha-final, missing==[]), the full tools tree SHA, and the
frozen P24 seal inputs. Historical NON-selected failed attempts stay
archived and truthful (no poisoning); `06b`/`informational` keys are
exempt from rc==0 and can never substitute a required stage; unknown
extra keys are recorded as unadjudicated (never green).

### 1.2 finalize artifacts

- `<EV>/manifest.json` (pre-seal): problems/final_ok pre-verdict,
  secret scan 0/0, required steps.
- `<EV>/tools-sha.json`: sha256 of EVERY file under the tools dir
  (recursive; `__pycache__`/`*.pyc`/`results/`/`build/` excluded) —
  json/txt/pins/yaml/fixtures included, not only py/sh — plus
  `frozen_imports` (read-only P24 seal inputs).
- `<EV>/hash-manifest.json` (LAST write into EV): covers every EV file
  except the EV-ROOT `hash-manifest.json` itself; NESTED files that
  happen to share the name ARE covered (and ARE secret-scanned — the
  root-only exclusion is also how the secret scan treats it).
- Outside-EV meta dir (default `<EV>-seal-meta/`):
  `preseal-snapshot.json`, `seal-report.json`, `seal.stdout`,
  `seal.rc`, and reverify reports. NOTHING else may write into EV
  after seal.

## 2. Driver interface for the seal phase (Runtime GLM — do NOT route
the seal through run_step)

`run_step` writes `cmd.txt/stdout.log/stderr.log/rc` INTO the EV after
the child exits — that is a post-seal EV write. The locked interface:

```bash
# 1) pre-seal (may run through run_step — EV is still open):
run_step finalize-prepare python3 -I "$HERE/finalize_p25.py" prepare \
  --ev "$EV_ROOT" --tools-dir "$HERE" \
  --require preflight --require window --require postfacts \
  --require persist --require standalone --require pg_isolated

# 2) seal — BARE invocation, all output OUTSIDE the EV:
python3 -I "$HERE/finalize_p25.py" seal --ev "$EV_ROOT" \
  --meta-dir "$EV_ROOT-seal-meta" > "$EV_ROOT-seal-meta/seal.stdout" \
  2> "$EV_ROOT-seal-meta/seal.stderr"; echo $? > "$EV_ROOT-seal-meta/seal.rc"

# 3) final independent readback (also outside the EV):
python3 -I "$HERE/finalize_p25.py" reverify --ev "$EV_ROOT" \
  --meta-dir "$EV_ROOT-seal-meta" > "$EV_ROOT-seal-meta/reverify.stdout" \
  2> "$EV_ROOT-seal-meta/reverify.stderr"; echo $? > "$EV_ROOT-seal-meta/reverify.rc"
```

`seal` refuses if `manifest.json`/`tools-sha.json` are missing (i.e. it
must run AFTER prepare). `reverify` compares a fresh EV scan against
the pre-seal snapshot: any new file, mtime_ns change, size change or
hash drift after the seal is a POST-SEAL WRITE and fails rc1 — this is
the mechanical guard that the driver really kept the sealer's outer
stdout/rc outside the EV.

## 3. aux quiet RAW consumption contract (postfacts)

The p25 proxy/fixture are REMOVED (`rm -sf`) before ABC; post-ABC
postfacts must NOT contact them. The aux_down selected attempt dir
must contain (Runtime adds the fixture one; driver round-7+ body
already produces the proxy one — note the CURRENT driver prints only a
pass line for proxy; the contract requires the RAW stats JSON):

- `proxy-quiet-proof.stdout.log`: JSON object with `commit_started`,
  `commit_finished`, `commit_inflight` as STRICT non-bool ints,
  `commit_inflight == 0`, `commit_started == commit_finished`;
- `fixture-quiet-proof.stdout.log`: JSON listing every p25-fixture src
  counter body with strict-int `active == 0`.

`collect_postfacts_p25.py --collect --aux-quiet-dir <that attempt dir>`
consumes these files as archived RAW (bytes + sha bound into the
attempt ledger, bound to that attempt's identity). Missing/malformed =
honest collection FAILURE (never a boolean, never post-hoc HTTP).

## 4. checkpoint-owner table (REAL DDL, primary migration
20261116100000)

`sync_def_checkpoints` (`active_run_id` owner, `sync_def_id` PK,
`checkpoint_value`, `confirmed_epoch`, …). Owner roster query (used by
the postfacts collector; `blocked_reason` is DERIVED in the projection
— an unresolved owner on a terminal-failed run IS the P26 hold):

    SELECT json_agg(row_to_json(t)) FROM (
      SELECT c.sync_def_id::text, c.source_id::text,
             c.output_dataset_id::text, c.mode,
             c.confirmed_epoch::text, c.active_run_id::text AS run_id,
             COALESCE(c.active_since::text,'') AS active_since,
             r.status AS run_status,
             CASE WHEN r.status IN ('failed','aborted','error')
                  THEN 'awaiting_p26_resolution' ELSE '' END
                  AS blocked_reason
      FROM sync_def_checkpoints c
      LEFT JOIN sync_runs r ON r.id = c.active_run_id
      WHERE c.active_run_id IS NOT NULL
      ORDER BY c.sync_def_id) t;

An owner may reference a TERMINAL FAILED run — that is valid P25 state
(PROTECTED retention; never cleaned/adopted; P26 resolves). The
nonterminal RUN roster and the CHECKPOINT OWNER roster are adjudicated
separately (tail declaration carries both).

## 5. Ownership pins (this group)

- Archive GLM solely writes: the 5 rewritten scripts + the 4 new
  files above. `selftest_p25_tailtools.py` sections C/E/F test the
  RETIRED v1 pure-assembler interfaces (`build_facts` dumps-JSON,
  boolean `fixture_quiet`, filename-glob `finalize`) and are superseded
  by `selftest_p25_archive.py`; its A/B/D/G sections (tail roster/
  quiesce/images/adapter — not this group's files) remain valid.
- Frozen P24/P22/P21 assets are consumed read-only via the seal chain
  (`p25_cohort_derive.SEALED_*`), by SHA, never edited or rerun.


## 2026-11-15 Root review follow-up (v2.4 WIP)

ARCHIVE-INTERFACE.md v2.4 integration notes supersede older v2.3 closure claims. Targeted pure checks cover saved PG15/15/87, source a1bff, old859 exactset, full persist raw replay, canonical postfacts provenance, exact dual tail binding states, actual HTTP registry/gates, prepare→seal input mutation refusal, and bundle relocation of already-accepted unit/PG/build/preflight/source facts. Current targeted suite has 9 tests; the old 94/0 suite uses obsolete generic synthetic shapes and is not formal acceptance. Final safety catalog, restart/h10 records and final COMPLETE-EV standalone lifecycle remain under active coordination. No live collector, no final seal, no full acceptance claim.
