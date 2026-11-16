# P24 Tools GLM — durable execution handoff (2026-11-14, post-window)

Continuation one-pager for a fresh Tools context. Read the two review
docs (review-finalizer.md, review-final-evidence.md) + formal-execution-contract.md
for full rationale; this file records STATE + NEXT only.

## Ownership (do not cross)
- Tools (me): driver/formal_run_p24.sh, finalize, pg-import, quiesce
  (check_owned_quiesced_p24.py), owned_history_p24.py, images5
  (check_running_images_p24.py), adapter, source_hash, exact-checker,
  run_tests_p24.sh, preflight/verify_preflight, readonly wrapper, archiving.
- Primary: smoke_p24.py ONLY. PG: run_pg_isolated_p24.sh +
  collect_postfacts/verify_postfacts/collect_persist_state (3 files) +
  PG tests. Never edit others' files.

## Verified green (do NOT rerun)
- Front segment all rc0: baseline, source-gate(8cd25d42…),
  guard4, graceful swap, aux-up, health, swap_verify(5 pinned IDs),
  P1pre9 9/9, P23-adapted 8/8 official (suffix a5508ab8, exactly_one),
  P22full 11/11 + post-images. Unit 61 rc0; build rc0 (5 IDs);
  PG bound via 05-pg-import (16 artifacts, rc0).
- P24 window attempt2 rc0: suffix 150041b036, 7/7 pass, official,
  cleanup ok, G4 restart-probe pass; sidecars 6/6; history union=17
  (failed 1448507d89[4 defs kept disabled/NULL/grant-gone/terminal] +
  green 13); loops restored to unset. Attempt1 failure preserved.
- Tail rc0 (2026-11-14 15:1x): pre-auxdown gates / aux-down (3 test
  aux removed, no volumes) / persist-pre(17) / pre-restart gates /
  restart3 stop120-start-health / base3 images verify / reread+compare
  ok / P1post9 9/9 / persist-post(17, state==pre). NOTE: 11a/11c
  quiesce ran at 13 (latest result only — corrected in driver to
  history-union afterwards); compensated read-only by 11d-post-restart
  quiesce-union17 rc0 + persist-pre/post 17-def snapshots. ABC/postfacts/
  source-freeze/standalone/finalize NOT run yet.

## Next released tail (root will authorize; NO ABC yet)
`P24_CONFIRM=yes bash formal_run_p24.sh <phase>` in this order (driver is bash — arrays, ${1//-/_}; earlier runs invoked via sh and only worked because macOS /bin/sh is bash — use bash from now on):
pre-auxdown-gates → aux-down → persist-pre → pre-restart-gates →
restart3 → reread → p1post9. THEN STOP (ABC/postfacts/persist-post/
source-freeze/standalone/finalize await separate release).
Driver `full` already encodes the locked order (ABC last write).

## In flight / coordination
- PG adding: .txt.err artifacts (persist 16-file set now enforced by
  finalize), `--owned-history` flag (driver passes conditionally once
  their files grep-match it), owned_history_sha field (finalize
  requires both pre/post == bound history sha).
- finalize validate_persist NOW requires: all 16 files hashed, rc
  file==manifest rc==0, state==derived==raw reparse, bad/hard empty,
  same result sha, history sha binding, then state equality.

## Standing rules
Never rerun greens for bookkeeping; failures archive-first (no cp||true);
fixed pins stay 4; old4/fixture/volumes untouched; no secret output;
attempt-bound reads via step-log markers (RESULT_FILE=/PERSIST_DIR=/
OWNED_HISTORY=/PG_IMPORT_JSON=/SIDECARS_MANIFEST=/ATTEMPT_DIR=/BASELINE_DIR=);
secret-scan → manifest.json → hash-manifest LAST.


## FINAL STATE (2026-11-14 ~15:3x) — FINAL_OK = True
- finalize rc0: FINAL_OK true, 19 verdicts 0 bad, secret 0/0,
  hash-manifest 855 files LAST. ABC suffix 1020a565 official 3/0 was
  the LAST q3 write; everything after is read-only (postfacts r1
  all_ok 12/12, persist final snapshot, source 8cd25d42 equal,
  final base3 images, standalone, tools-sha 27 files).
- FINAL-NEW-TWO-PROVENANCE.json: full chain of 885d1429(C)/dcca308f(B)
  — source/def/target/actor/fixture + C txn COMMITTED/lineage/hash.
  They did NOT cross the final restart (locked fallback, never claimed).
  Fixed pins stay 4; these two are root-review records, NOT pins.
- MF metadata fix: 11e lines' log/dir normalized to EV-relative;
  original preserved as manifest.jsonl.pre-11epathfix-archive.
- Known glob trap: postfacts has TWO verdict files — the r1 suffix
  file is the FINAL all_ok one (finalize binds it via step-log marker).
- PG pending: owned_history_sha field absent both persist sides
  (content root-verified equal); private PG of-p24-pg kept, not cleaned.
