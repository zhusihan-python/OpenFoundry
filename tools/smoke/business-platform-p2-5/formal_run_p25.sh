#!/bin/bash
# P25 FORMAL DRIVER — COMPLETE REAL CLOSED LOOP (round4, 2026-11-14).
#
# Consumes the FROZEN Archive CLI surface READ-ONLY
# (ARCHIVE-INTERFACE.md v2 + ARCHIVE-CONTRACT.md §1.1): canonical
# 22-stage selected-index.json, archive collector/verifier CLIs,
# two-step seal (stdout/rc outside the EV). Cross-track work already
# genuinely green (readonly preflight base+supplement, Primary
# unit+race, isolated-PG 15/15/87, PG ONLY-build 6) is IMPORTED as
# selected proof with exact SHA binding — nothing green is rerun, no
# new live is taken, nothing is waived.
#
# Every phase body runs REAL commands whose argv is taken from the
# FROZEN predecessor phase bodies (formal_run_p24.sh / P1 run.sh /
# P21-P24 smoke CLIs — read, never edited).
#
# Locked order (root review-tools-round1 + implementation-release):
#   ALL switch/restart/aux-removal + P1post9 + old artifact/BuildJob
#   reread happen BEFORE the final ABC; ABC is the LAST q3 write;
#   afterwards STRICTLY readonly. Owner roster and nonterminal-run
#   roster are adjudicated independently; terminal-failed owners are
#   never cleaned/adopted. The tail roster is hash-frozen (declared)
#   BEFORE tail execution.
#
# Authorization: this driver never asks the user; root coordinates.
# Live (q3-touching / docker / psql / HTTP) steps require
# RELEASED_RUNTIME=yes; pure-evidence steps run on files only. Tools
# with their own RELEASED_FOR_EXECUTION flags (cohort guard, adapter,
# live collectors) independently refuse rc3 until root flips them.
#
# Source fingerprints: the historical 373-file P24 baseline stays
# SEAL-VERIFIED (its own sha256 == 8cd25d42…), the CURRENT reviewed
# candidate (378 Go/SQL files + root go.mod/go.sum) must equal
# a1bff07f…ffd27 (pg-candidate-bindings-r17.json + BUILD-HANDOFF
# source gate), and the baseline→current delta is recorded EXPLICITLY
# (removed must stay 0).
#
# bash only (P24 lesson). Collision-safe attempt logging; failures
# abort the phase and are preserved (no cp||true).
#
# usage:
#   formal_run_p25.sh list | dryrun
#   P25_CONFIRM=yes formal_run_p25.sh <phase>
#   P25_CONFIRM=yes RELEASED_RUNTIME=yes formal_run_p25.sh full

set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
P1="$REPO/tools/smoke/business-platform-p1"
P21="$REPO/tools/smoke/business-platform-p2-1"
P22="$REPO/tools/smoke/business-platform-p2-2"
P24_FINAL="$REPO/tools/smoke/business-platform-p2-4/results/2026-11-14-p24-final"
EV_ROOT="${P25_EV_ROOT:-$HERE/results/2026-11-14-p25-final}"
MANIFEST="$EV_ROOT/manifest.jsonl"
CONFIRM="${P25_CONFIRM:-no}"
RUNTIME="${RELEASED_RUNTIME:-no}"

Q3_ENV_DIR="${P25_Q3_ENV_DIR:-/private/tmp/openfoundry-platform-p1-q3v9}"
Q3_PROJECT="openfoundry-platform-p1-q3v9"
COMPOSE_BASE=(docker compose -f "$Q3_ENV_DIR/compose.yaml" \
  --project-name "$Q3_PROJECT")
# p25 service chain: my fixture override (p25fixture def) + the
# PG-GENERATED build/compose-p25-aux.yaml (p25connector build def,
# read-only use) + the runtime override LAST (cached python:3.12-alpine
# full image pins + fault-proxy env + scheduler interval 1 — env/image
# only, no other track's yaml edited)
COMPOSE_AUX=(-f "$Q3_ENV_DIR/compose.yaml" -f "$HERE/compose-p25-fixture.yaml" \
  -f "$HERE/build/compose-p25-aux.yaml" -f "$HERE/compose-p25-runtime.yaml")

# Reviewed candidate source (ARCHIVE-INTERFACE §0; cross-checked
# against the bindings file at every use).
CANDIDATE_SOURCE_SHA="a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27"
PG_BINDINGS="$HERE/pg-candidate-bindings-r17.json"
# accepted r18 build bindings (P25_BUILD_R18 override exists ONLY
# so pure selftests can inject a TAMPERED copy; the formal default
# is the repo file)
BUILD_R18="${P25_BUILD_R18:-$HERE/build-candidate-bindings-r18.json}"
PG_EV="/private/tmp/openfoundry-p25-repair-evidence/pg"
BUILD_EV="/private/tmp/openfoundry-p25-repair-evidence/build"
UNIT_EV="/private/tmp/openfoundry-p25-repair-evidence/connector"
# Selected ONLY-build round (BUILD-HANDOFF.md §执行状态, 142804Z; the
# earlier 142634Z build-base3 run FAILED per-service image location and
# stays preserved — it is never selected).
BUILD_STAMPS=(
  "build-base3_20261114T142804Z"
  "build-oldaux2_20261114T142810Z"
  "build-newaux1_20261114T142811Z"
  "cache-python312alpine_20261114T142814Z"
  "source-gate_20261114T142814Z"
)
# Primary's accepted full-suite green (root-read originals; complete
# stdout + original Go rc; see $UNIT_EV/INDEX.md attempt20).
UNIT_GREEN="$UNIT_EV/attempt20-round2-build-vet-fullunit"
# ROOT-ACCEPTED readonly preflight chain (base attempt-2 + c-supplement
# attempt-1; rounds 15-17 ALL_OK re-confirmed; independent root/
# readonly-agent verification). REF SHAs — root bumps at the final
# review if the supplement verifier's in-flight binding fixes land.
PREFLIGHT_ROOT="$HERE/results/2026-11-14-p25-preflight"
PREFLIGHT_BASE_DIR="$PREFLIGHT_ROOT/attempt-2"
PREFLIGHT_BASE_ATTEMPT="$PREFLIGHT_BASE_DIR/collect/attempt-1"
PREFLIGHT_SUPP_DIR="$PREFLIGHT_ROOT/c-supplement/attempt-1"
PREFLIGHT_BASE_SUMMARY_SHA="11d77e365ae751357fff378ca9a52f3d9f9e0366caf48f3c61cdec36f7424482"
PREFLIGHT_SUPP_SUMMARY_SHA="28a13afac39d8464667ada974b8a777783bc82ac4339a8fe24dcf9d866bcc717"
PREFLIGHT_BINDING_SHA="a1799498f231b94b19a8f0f9126ae429277892cf9e7c8fa0eaddacc05aa1ff57"
PREFLIGHT_FINAL_VERDICT_SHA="79af7b281013ef1c1f16ee99fe9fac75ae1bf91be5f9bb29a9fd07698d93a6c6"
# root's machine-readable acceptance record (external handoffs dir)
PREFLIGHT_ACCEPTED_JSON="/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-14-p25/preflight-accepted.json"

# ---- locked phase order -------------------------------------------------
PHASES=(
  preflight                 # IMPORT the root-accepted readonly chain
                            # (base attempt-2 + c-supplement, SHA-bound,
                            # pure re-adjudication — no new live, nothing
                            # waived); collect mode = released live
                            # collector, runtime-gated
  fingerprints              # sealed 373 baseline + candidate a1bff07f
                            # + EXPLICIT baseline→current delta
  unit                      # import Primary's accepted attempt20 green
                            # (exec mode = frozen P1 runner, live-gated)
  pg_isolated               # import bindings-r17 three stages + REAL
                            # exact checker over imported logs
  build                     # import PG ONLY-build evidence →
                            # selected-build.json (never builds here)
  swap                      # PURE gates (source + r18 build binding —
                            # the auditable-now path) THEN tag-check,
                            # stop/up/health/images + IMMEDIATE steady
                            # old6 re-verify on the real new boot/full
                            # image (BEFORE any HTTP); deployment
                            # record written; guard → steady from it
  fixture_up                # AFTER swap success + old6 steady check:
                            # up -d --no-deps ALL THREE p25 services
                            # (fixture+proxy+connector — the healthy
                            # window needs proxy+connector for h13);
                            # cached python:3.12-alpine images + the
                            # p25connector scheduler interval 1 come
                            # from compose-p25-runtime.yaml
  window                    # pre-window readonly baseline + Primary
                            # smoke_p25.py healthy scenarios
  regressions               # aux-old-up → p1pre9 → p23(1) → p22(11) →
                            # P24 full7(2) via adapter with
                            # deployment-record-verified steady shim env;
                            # deployment record refreshed after each
                            # restart-bearing suite
  reread_artifacts          # old artifact + Build/Job reread + compare
  p1post9                   # P1 post-9 regression (pre-ABC)
  restart_regression        # controlled base3 restart (h10, pre-tail) +
                            # new boot recorded
  pre_tail_gates            # steady strict6 (latest deployment record) +
                            # tail roster DECLARE (hash-frozen)
  tail                      # aux up + smoke tail scenarios (h11+h14) +
                            # ACTUAL identity binding from the tail result
  aux_down                  # true-quiet RAW proofs → p25 rm -sf →
                            # old aux down (proofs archived IN the
                            # aux-down step dir for postfacts)
  final_images              # base3 on exact candidate SHAs; ALL aux
                            # explicitly absent (successful ps evidence)
  p21abc                    # FINAL q3 write; raw result at
                            # $EV_ROOT/abc-result.json (byte-exact)
  postfacts                 # READONLY: quiesce + Archive collect/verify
  persist                   # readonly persist snapshot (real SQL records)
  source_freeze             # candidate == a1bff07f + delta unchanged +
                            # full manifest emitted
  standalone                # Archive bundle (self-contained + offline
                            # verifier)
  finalize                  # selected-index → prepare → seal → reverify
                            # (hash LAST; sealer stdout/rc OUTSIDE EV)
)
LIVE_PHASES="preflight fixture_up swap window regressions reread_artifacts p1post9 restart_regression pre_tail_gates tail aux_down final_images p21abc postfacts persist"

# canonical 22-stage ids (ARCHIVE-CONTRACT §1.1 / finalize
# CANONICAL_STAGES) — the driver's MAIN step per stage uses exactly
# these names so selected-index.json is a direct latest-attempt map
CANONICAL_STAGES=(
  preflight unit pg-isolated build swap window
  regressions-p1pre9 regressions-p23 regressions-p22
  regressions-p24 reread-artifacts p1post9 restart-regression
  pre-tail-gates tail aux-down final-images p21abc
  postfacts persist source-freeze standalone
)

log() { echo "[p25-formal] $*"; }
die() { echo "[p25-formal] ABORT: $*" >&2; exit 1; }

phase_refuse_runtime() {
  log "phase '$1' requires the runtime release (RELEASED_RUNTIME=yes)"
  return 3
}

run_step() {  # run_step <name> <cmd...> — real rc, attempt-safe artifacts
  local name="$1"; shift
  local dir="$EV_ROOT/steps/$name"
  local n=1
  while [ -e "$dir-attempt$n" ]; do n=$((n+1)); done
  dir="$dir-attempt$n"
  mkdir -p "$dir"
  { echo "CMD: $*"; } > "$dir/cmd.txt"
  local t0 rc
  t0=$(date +%s)
  "$@" > "$dir/stdout.log" 2> "$dir/stderr.log"
  rc=$?
  echo "$rc" > "$dir/rc"
  printf '{"step":"%s","attempt":%d,"rc":%d,"seconds":%d,"cmd":%s,"dir":"%s"}\n' \
    "$name" "$n" "$rc" "$(( $(date +%s) - t0 ))" \
    "$(printf '%s' "$*" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')" \
    "${dir#$EV_ROOT/}" >> "$MANIFEST"
  log "step $name rc=$rc (attempt $n)"
  LAST_STEP_LOG="$dir/stdout.log"
  LAST_STEP_DIR="$dir"
  return $rc
}

capture_step_var() {  # last 'VAR=value' marker from the latest step log
  sed -n "s/^$1=//p" "$LAST_STEP_LOG" | tail -1
}

latest_attempt_dir() {  # highest attempt-N under $1
  python3 - "$1" <<'PY'
import sys
from pathlib import Path
base = Path(sys.argv[1])
cands = [int(p.name.split("-")[1]) for p in base.glob("attempt-*")
         if p.is_dir() and p.name.split("-")[-1].isdigit()]
print(base / f"attempt-{max(cands)}" if cands else "")
PY
}

latest_stage_dir() {  # latest attempt dir of a canonical stage step
  python3 - "$EV_ROOT/steps" "$1" <<'PYLATEST'
import re, sys
from pathlib import Path
base, stage = Path(sys.argv[1]), sys.argv[2]
candidates = [(int(m.group(1)), p) for p in base.glob(stage + "-attempt*")
              if p.is_dir() and (m := re.fullmatch(re.escape(stage) + r"-attempt(\d+)", p.name))]
print(max(candidates)[1] if candidates else "")
PYLATEST
}

latest_deployment_record() {
  [ -f "$EV_ROOT/deployment-record-latest.txt" ] \
    || { echo ""; return 1; }
  local p; p="$(cat "$EV_ROOT/deployment-record-latest.txt")"
  [ -n "$p" ] && [ -f "$p" ] || { echo ""; return 1; }
  printf '%s' "$p"
}

# The deployment-record writer: the standalone runtime-owned helper
# (deploy_record_helper_p25.py — record_body() takes INJECTABLE run_cli
# so the selftest drives the REAL generated body with mocked IO) is
# copied byte-exact into $EV/gen (sha recorded). Live: compose ps +
# docker inspect + the candidate source gate; no mutation; fails
# closed BEFORE writing any record.
write_deploy_tool() {
  mkdir -p "$EV_ROOT/gen"
  if [ -f "$EV_ROOT/gen/deployment_record.py" ]; then
    return 0
  fi
  cp "$HERE/deploy_record_helper_p25.py" \
    "$EV_ROOT/gen/deployment_record.py" \
    || die "deployment-record helper copy failed"
  log "deploy helper sha256=$(shasum -a 256 "$EV_ROOT/gen/deployment_record.py" | cut -d' ' -f1)"
}

record_deployment() {  # record_deployment <cause-tag>
  local cause="$1"
  write_deploy_tool
  run_step "deploy-record-$cause" python3 -I \
    "$EV_ROOT/gen/deployment_record.py" "$HERE" "$REPO" "$Q3_ENV_DIR" \
    "$Q3_PROJECT" "$CANDIDATE_SOURCE_SHA" "$EV_ROOT/selected-build.json" \
    "$cause" "$EV_ROOT" \
    || die "deployment record ($cause) failed — refusing to continue"
}

# bindings cross-check: the reviewed candidate SHA must come from the
# bindings file AND equal the driver-pinned literal (both directions).
bindings_candidate_sha() {
  python3 - "$PG_BINDINGS" "$CANDIDATE_SOURCE_SHA" <<'PY'
import json, sys
b = json.loads(open(sys.argv[1]).read())
sha = b["candidate_source_sha256"]
assert sha == sys.argv[2], f"bindings candidate {sha} != pinned"
print(sha)
PY
}

stage_verdict() {  # stage_verdict <file> <json> — explicit TRUE verdict
  python3 -c 'import json, sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps(json.loads(sys.argv[2]),
                                        indent=1, sort_keys=True) + "\n")' \
    "$1" "$2" || die "verdict write failed: $1"
}

# ---------------------------------------------------------------- phases --

phase_preflight() {
  if [ "${P25_PREFLIGHT_MODE:-import}" = "collect" ]; then
    # LIVE mode (the root-released collector; runtime-gated). The
    # formal default is IMPORT — no new live, nothing waived.
    [ "$RUNTIME" = "yes" ] || { phase_refuse_runtime preflight; return 3; }
    run_step preflight-plan python3 -I "$HERE/readonly_preflight_p25.py" \
      --plan --env-dir "$Q3_ENV_DIR" || die "preflight plan failed"
    run_step preflight-collect python3 -I "$HERE/readonly_preflight_p25.py" \
      --collect --env-dir "$Q3_ENV_DIR" --out-dir "$EV_ROOT/00-preflight" \
      || die "preflight collect failed"
    local att; att="$(latest_attempt_dir "$EV_ROOT/00-preflight")"
    [ -n "$att" ] && [ -d "$att" ] \
      || die "preflight: no attempt dir under 00-preflight"
    run_step preflight-verify python3 -I "$HERE/verify_preflight_p25.py" \
      --attempt-dir "$att" --out "$EV_ROOT/00-preflight-verdict.json" \
      || die "preflight verify failed"
    return 0
  fi
  # IMPORT the root-accepted chain (base attempt-2 + c-supplement
  # attempt-1): four REF SHAs verified on disk, then the PURE verifier
  # re-adjudicates the SAVED artifacts (foundation tool, read-only
  # invoke; no collect, no GET, no waiver) and the final verdict must
  # carry the pinned binding sha.
  run_step preflight python3 -I - \
    "$PREFLIGHT_BASE_DIR" "$PREFLIGHT_BASE_ATTEMPT" \
    "$PREFLIGHT_SUPP_DIR" "$HERE/verify_preflight_p25.py" \
    "$PREFLIGHT_BASE_SUMMARY_SHA" "$PREFLIGHT_SUPP_SUMMARY_SHA" \
    "$PREFLIGHT_BINDING_SHA" "$PREFLIGHT_FINAL_VERDICT_SHA" \
    "$PREFLIGHT_ACCEPTED_JSON" <<'PYA'
import hashlib, json, subprocess, sys
from pathlib import Path
(base_dir, base_att, supp_dir, verifier, base_sum_sha, supp_sum_sha,
 bind_sha, verdict_sha, accepted_json) = sys.argv[1:10]


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


probs, consumed = [], {}
# root acceptance record: status, candidate sha, and the verifier's
# OWN file sha must match TODAY's tool before we run it (a changed
# verifier needs a root re-pin, never a silent pass)
try:
    acc = json.loads(Path(accepted_json).read_text())
except (OSError, ValueError) as exc:
    acc = {}
    probs.append(f"preflight-accepted.json unreadable: {exc}")
if acc.get("status") != "first-transition-preflight-accepted":
    probs.append("preflight-accepted.json status != accepted")
acc_arts = acc.get("artifacts") or {}
verifier_sha = acc_arts.get(str(Path(verifier).resolve()))
if verifier_sha is None:
    probs.append("preflight-accepted.json lacks the verifier entry")
elif sha(Path(verifier)) != verifier_sha:
    probs.append(f"verify_preflight_p25.py {sha(Path(verifier))[:12]}… "
                 f"!= accepted {verifier_sha[:12]}… — root must re-pin")
files = {
    "base_summary": (Path(base_att) / "attempt-summary.json",
                     base_sum_sha),
    "supp_summary": (Path(supp_dir) / "attempt-summary.json",
                     supp_sum_sha),
    "supp_binding": (Path(supp_dir) / "base-binding.json", bind_sha),
    "final_verdict": (Path(base_dir) / "verify-csupp.stdout",
                      verdict_sha)}
for name, (p, want) in files.items():
    if not p.is_file():
        probs.append(f"{name} missing: {p}")
        continue
    got = sha(p)
    consumed[name] = {"path": str(p), "sha256": got}
    if got != want:
        probs.append(f"{name} sha {got[:12]}… != REF {want[:12]}…")
    acc_want = acc_arts.get(str(p.resolve()))
    if acc_want is not None and acc_want != got:
        probs.append(f"{name} sha != preflight-accepted.json entry")
# binding json must bind exactly the two pinned summaries
try:
    b = json.loads(Path(supp_dir, "base-binding.json").read_text())
except (OSError, ValueError):
    b = {}
if b.get("base_summary_sha256") != base_sum_sha \
        or b.get("supplement_summary_sha256") != supp_sum_sha \
        or Path(b.get("base_attempt") or "").name != \
        Path(base_att).name:
    probs.append("base-binding fields != pinned base/supplement")
# PURE re-adjudication of the SAVED chain (no live, no GET), invoked
# EXACTLY as the accepted green run: --attempt-dir ABSOLUTE (the base
# summary records the absolute string; the verifier demands exact
# string equality), --c-supplement RELATIVE (the supplement summary
# records the relative string; resolved from cwd = the tools dir).
# Same manner as the accepted run — not a waiver.
rel_supp = "results/2026-11-14-p25-preflight/c-supplement/attempt-1"
pr = subprocess.run(["python3", "-I", verifier,
                     "--attempt-dir", str(Path(base_att).resolve()),
                     "--c-supplement", rel_supp],
                    capture_output=True, text=True, timeout=300,
                    cwd=str(Path(verifier).resolve().parent))
verdict = None
if pr.returncode != 0:
    probs.append(f"pure re-adjudication rc={pr.returncode}: "
                 f"{pr.stdout[-200:]}{pr.stderr[-200:]}")
else:
    # the verifier prints the WHOLE verdict as one (pretty-printed)
    # JSON document — parse the full stdout, falling back to the last
    # line for compact-output variants
    try:
        verdict = json.loads(pr.stdout)
    except ValueError:
        lines = [l for l in pr.stdout.splitlines() if l.strip()]
        try:
            verdict = json.loads(lines[-1]) if lines else None
        except ValueError:
            verdict = None
    if not isinstance(verdict, dict) or verdict.get("all_ok") is not True:
        probs.append("re-adjudicated verdict all_ok is not true")
    else:
        cs = verdict.get("c_supplement") or {}
        if cs.get("binding_sha256") != bind_sha:
            probs.append(f"verdict binding_sha256 "
                         f"{str(cs.get('binding_sha256'))[:12]}… != REF "
                         f"{bind_sha[:12]}…")
        if cs.get("summary_sha256") != supp_sum_sha:
            probs.append("verdict supplement summary sha != REF")
out = {"schema": "p25-preflight-import-v1",
       "mode": "import (root-accepted base attempt-2 + c-supplement)",
       "consumed": consumed,
       "re_adjudicated": verdict is not None
       and verdict.get("all_ok") is True,
       "problems": probs, "ok": not probs,
       "all_ok": not probs}
print(json.dumps(out, indent=1, sort_keys=True))
sys.exit(0 if not probs else 1)
PYA
  [ $? -eq 0 ] || die "preflight import failed (REF SHAs or re-adjudication)"
  cp "$PREFLIGHT_BASE_DIR/verify-csupp.stdout" \
    "$LAST_STEP_DIR/accepted-final-verdict.json" \
    || die "preflight: accepted verdict archive failed"
}

phase_fingerprints() {
  # 1) the HISTORICAL 373-file baseline stays seal-verified: its own
  #    bytes must hash to the sealed combined value.
  run_step fp-baseline-sealed python3 -I -c "
import hashlib, sys
from pathlib import Path
p = Path('$HERE/source-p24-baseline.txt')
got = hashlib.sha256(p.read_bytes()).hexdigest()
want = '8cd25d4220a6bdae843a693e1c1afa928991f537f5902a31e3e970704db6386b'
print('BASELINE_SHA256=' + got)
print('BASELINE_SEALED=' + ('OK' if got == want else 'FAILED'))
sys.exit(0 if got == want else 1)" || die "sealed 373 baseline drifted"
  # 2) CURRENT tree == reviewed candidate (bindings + pinned literal
  #    cross-checked; PG-gate algorithm reproduced byte-exactly).
  local cand; cand="$(bindings_candidate_sha)" \
    || die "pg-candidate-bindings-r17.json missing/drifted"
  run_step fp-candidate python3 -I "$HERE/candidate_source_gate_p25.py" \
    --repo "$REPO" --expect "$cand" \
    || die "current tree != reviewed candidate a1bff07f…"
  run_step fp-bindings-sha python3 -I -c "
import hashlib
from pathlib import Path
print('BINDINGS_SHA256=' + hashlib.sha256(
    Path('$PG_BINDINGS').read_bytes()).hexdigest())"
  # 3) EXPLICIT baseline→current delta (removed MUST be 0; changed and
  #    added are recorded, not punished — adjudicated again unchanged
  #    at source_freeze).
  run_step fp-delta python3 -I "$HERE/source_hash_p25.py" delta \
    --repo "$REPO" --baseline "$HERE/source-p24-baseline.txt" \
    --out "$EV_ROOT/01-source-delta.json" \
    || die "source delta: REMOVED files are an anomaly (rc1)"
}

phase_unit() {
  if [ "${P25_UNIT_MODE:-import}" != "import" ]; then
    # exec mode = frozen P1 unit runner in its disposable container —
    # live docker/Go, runtime-gated (import is the default and the
    # already-accepted evidence).
    [ "$RUNTIME" = "yes" ] || { phase_refuse_runtime unit; return 3; }
    run_step unit-exec bash -o pipefail "$P1/run.sh" tests \
      || die "unit exec failed"
    return 0
  fi
  # IMPORT Primary's accepted attempt20 (complete stdout + original Go
  # rc0 + cmd sidecar, root-read) — re-verified here, bound to TODAY's
  # tree via the candidate source hash. No rerun, no summary trick.
  run_step unit python3 -I - "$UNIT_GREEN" \
    "$HERE/candidate_source_gate_p25.py" \
    "$REPO" "$CANDIDATE_SOURCE_SHA" "$EV_ROOT/04-unit-import.json" <<'PY'
import hashlib, json, re, subprocess, sys
from pathlib import Path
base = Path(sys.argv[1])
gate = Path(sys.argv[2])
repo, pinned, out_json = sys.argv[3], sys.argv[4], Path(sys.argv[5])
files = {s: Path(f"{base}.{s}") for s in ("cmd", "log", "rc")}
probs = []
for s, p in files.items():
    if not p.is_file():
        probs.append(f"attempt20 .{s} missing: {p}")
if not probs:
    rc_txt = files["rc"].read_text().strip()
    if rc_txt != "rc=0":
        probs.append(f".rc content {rc_txt!r} != 'rc=0'")
    cmd = files["cmd"].read_text()
    for needle in ("golang:1.26.3", "go test -count=1 ./..."):
        if needle not in cmd:
            probs.append(f".cmd lacks {needle!r}")
    log = files["log"].read_text(errors="replace")
    ok_n = len(re.findall(r"^ok\s", log, re.M))
    fail_n = len(re.findall(r"^FAIL", log, re.M))
    if fail_n != 0 or ok_n != 38:
        probs.append(f"log ok={ok_n} FAIL={fail_n} (want 38/0)")
cand = subprocess.run(["python3", "-I", str(gate),
                       "--repo", repo, "--expect", pinned],
                      capture_output=True, text=True, timeout=300)
if cand.returncode != 0:
    probs.append("current tree != pinned candidate source — imported "
                 "green would not bind to today's bytes")
verdict = {"schema": "p25-unit-import-v1",
           "selected": {s: {"path": str(p),
                            "bytes": p.stat().st_size,
                            "sha256": hashlib.sha256(
                                p.read_bytes()).hexdigest()}
                        for s, p in files.items() if p.is_file()},
           "ok_packages": 38 if not probs else None,
           "problems": probs, "ok": not probs,
           "all_ok": not probs}
out_json.write_text(json.dumps(verdict, indent=1, sort_keys=True) + "\n")
print(json.dumps(verdict, indent=1, sort_keys=True))
sys.exit(0 if not probs else 1)
PY
  [ $? -eq 0 ] || die "unit import failed"
  cp "$EV_ROOT/04-unit-import.json" "$LAST_STEP_DIR/unit-verdict.json" \
    || die "unit verdict archive failed"
}

phase_pg_isolated() {
  if [ "${P25_PG_MODE:-import}" != "import" ]; then
    [ "$RUNTIME" = "yes" ] || { phase_refuse_runtime pg_isolated; return 3; }
    for m in new-normal new-race unified; do
      run_step "pg-exec-$m" sh "$HERE/run_pg_isolated_p25.sh" "$m" \
        || die "pg exec $m failed"
    done
    return 0
  fi
  # IMPORT bindings-r17 three stages: every artifact file verified on
  # disk, meta gates re-parsed, and the REAL exact checker executed over
  # the IMPORTED logs (read-only; never a rerun).
  run_step pg-isolated python3 -I - "$PG_BINDINGS" "$PG_EV" "$HERE" "$REPO" \
    "$CANDIDATE_SOURCE_SHA" "$EV_ROOT/05-pg-import.json" \
    "$P1/run_tests.sh" "$P21/run_tests_p21.sh" \
    "$P22/p22-required-names.txt" <<'PY'
import hashlib, json, subprocess, sys
from pathlib import Path
bindings_p, pg_ev, here, repo = map(Path, sys.argv[1:5])
pinned, out_json = sys.argv[5], Path(sys.argv[6])
extra_argv = sys.argv[7:]
for sub, fname in (("business-platform-p2-3", "p23-required-names.txt"),
                   ("business-platform-p2-4", "p24-required-names.txt")):
    extra_argv.append(str(here.parent / sub / fname))
extra_argv.append(str(here / "p25-required-names.txt"))


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


b = json.loads(Path(bindings_p).read_text())
probs, consumed = [], {}
if b["candidate_source_sha256"] != pinned:
    probs.append("bindings candidate sha != pinned")
consumed["bindings-r17.json"] = {"path": str(bindings_p),
                                 "sha256": sha(bindings_p)}
want_go = {"new-normal": "go1.26.3", "new-race": "go1.25.14",
           "unified": "go1.26.3"}
for st in b["stages"]:
    name = st["stage"]
    base = Path(st["attempt_base"])
    arts = {}
    for suf in st["artifacts"]:
        p = Path(f"{base}{suf}")
        if not p.is_file():
            probs.append(f"{name}: missing {p}")
            continue
        arts[suf] = {"path": str(p), "bytes": p.stat().st_size,
                     "sha256": sha(p)}
    consumed[name] = arts
    for rc_key in ("source-gate.rc", "version-gate.rc"):
        a = arts.get(rc_key)
        if a and Path(a["path"]).read_text().strip() != "0":
            probs.append(f"{name}: {rc_key} != 0")
    if arts.get(".rc") and Path(arts[".rc"]["path"]).read_text().strip() != "0":
        probs.append(f"{name}: original .rc != 0")
    meta = {}
    if arts.get(".meta"):
        for line in Path(arts[".meta"]["path"]).read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                meta.setdefault(k, v)
    if meta.get("source_content_sha256_before") != pinned \
            or meta.get("source_content_sha256_after") != pinned:
        probs.append(f"{name}: meta source before/after != candidate")
    if want_go[name] not in meta.get("go_version_actual",
                                     "").split():
        probs.append(f"{name}: go {meta.get('go_version_actual')!r} "
                     f"!= {want_go[name]}")
    if str(st.get("original_rc")) != "0" or str(
            st.get("source_gate_rc")) != "0" or str(
            st.get("version_gate_rc")) != "0":
        probs.append(f"{name}: bindings rc fields not all 0")
    for aux in st.get("aux_attempts", []):
        aux_base = aux.split(" ")[0]
        if not (Path(pg_ev) / f"{aux_base}.rc").is_file():
            probs.append(f"{name}: aux attempt missing: {aux_base}")
    # the REAL exact checker over the imported log (read-only)
    if arts.get(".raw") and arts.get(".rc") and arts.get(".meta"):
        cmd = ["python3", "-I", str(here / "pg_exact_check_p25.py"),
               "--meta", arts[".meta"]["path"],
               "--expect-source-sha", pinned]
        if name in ("new-normal", "new-race"):
            cmd.append("--new-only")
        cmd += [arts[".raw"]["path"], arts[".rc"]["path"]] + extra_argv
        pr = subprocess.run(cmd, capture_output=True, text=True)
        if pr.returncode != 0:
            probs.append(f"{name}: exact checker rc={pr.returncode}: "
                         f"{pr.stdout[-160:]}{pr.stderr[-160:]}")
# test files + checker + required names bound to TODAY's tree
for key, rel in (("repo", "services/connector-management-service/"
                  "internal/repo/checkpoint_integration_test.go"),
                 ("handlers", "services/connector-management-service/"
                  "internal/handlers/checkpoint_integration_test.go")):
    want = b["test_file_sha256"][key]
    got = sha(Path(repo) / rel)
    if got != want:
        probs.append(f"test_file[{key}] {got[:12]}… != bindings "
                     f"{want[:12]}…")
if sha(here / "pg_exact_check_p25.py") != b["checker_sha256"]:
    probs.append("checker sha != bindings")
names = [l.strip() for l in
         (here / "p25-required-names.txt").read_text().splitlines()
         if l.strip() and not l.startswith("#")]
if len(names) != b["required_names_count"]:
    probs.append(f"required names {len(names)} != bindings "
                 f"{b['required_names_count']}")
verdict = {"schema": "p25-pg-import-v1", "consumed": consumed,
           "required_names_count": len(names),
           "unified_total": b["unified_required_total"],
           "problems": probs, "ok": not probs,
           "all_ok": not probs}
out_json.write_text(json.dumps(verdict, indent=1,
                               sort_keys=True) + "\n")
print(json.dumps(verdict, indent=1, sort_keys=True))
sys.exit(0 if not probs else 1)
PY
  [ $? -eq 0 ] || die "pg import failed"
  # ARCHIVE-INTERFACE §2: the step dir carries the bindings copy
  cp "$PG_BINDINGS" "$LAST_STEP_DIR/pg-candidate-bindings-r17.json" \
    || die "pg bindings archive failed"
  cp "$EV_ROOT/05-pg-import.json" \
    "$LAST_STEP_DIR/pg-import-verdict.json" \
    || die "pg verdict archive failed"
}

phase_build() {
  # BUILD IS PG-TRACK-OWNED (BUILD-HANDOFF.md): PG solely wrote
  # build_candidates_p25.sh and performed the ONLY-build. This phase
  # CONSUMES the reviewed selected artifacts read-only (never builds,
  # never edits the helper) and emits the canonical selected-build
  # image mapping every later phase binds to.
  run_step build python3 -I - "$BUILD_EV" "$CANDIDATE_SOURCE_SHA" \
    "$EV_ROOT/selected-build.json" "${BUILD_STAMPS[@]}" <<'PY'
import hashlib, json, sys
from pathlib import Path
(ev, pinned, out_json, *stamps) = sys.argv[1:]
ev = Path(ev)


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


probs, components, images = [], {}, {}
for stamp in stamps:
    meta_p, rc_p, cmd_p, raw_p = (ev / f"{stamp}.meta",
                                  ev / f"{stamp}.rc",
                                  ev / f"{stamp}.cmd",
                                  ev / f"{stamp}.raw")
    for p in (meta_p, rc_p, cmd_p):
        if not p.is_file():
            probs.append(f"{stamp}: missing {p.name}")
    if rc_p.is_file() and rc_p.read_text().strip() != "0":
        probs.append(f"{stamp}: rc != 0")
    meta = {}
    if meta_p.is_file():
        for line in meta_p.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                # PG metas carry a trailing "(tag repo:tag)" comment on
                # image lines — the VALUE ends before it
                v = v.split(" (")[0].strip()
                if k.startswith("image_sha[") and k.endswith("]"):
                    images[k[10:-1]] = v
                elif k == "image_sha":
                    images["python-3.12-alpine"] = v
                else:
                    meta.setdefault(k, v)
    is_build = stamp.startswith("build-")
    if is_build:
        for k in ("source_content_sha256_before",
                  "source_content_sha256_after"):
            if meta.get(k) != pinned:
                probs.append(f"{stamp}: {k} != candidate")
        if meta.get("original_docker_rc") is not None \
                and meta.get("original_docker_rc") != "0":
            probs.append(f"{stamp}: original_docker_rc != 0")
    elif stamp.startswith("source-gate"):
        if meta.get("expect_source_sha") != pinned:
            probs.append(f"{stamp}: expect_source_sha != candidate")
    components[stamp] = {
        "kind": "build" if is_build else stamp.split("_")[0],
        "rc": rc_p.read_text().strip() if rc_p.is_file() else None,
        "meta_sha256": sha(meta_p) if meta_p.is_file() else None,
        "cmd_sha256": sha(cmd_p) if cmd_p.is_file() else None,
        "raw_sha256": sha(raw_p) if raw_p.is_file() else None}
need = {"dataset", "pipeline", "connector", "connector2",
        "connector3", "p25connector", "python-3.12-alpine"}
missing = sorted(need - set(images))
if missing:
    probs.append(f"image map missing services: {missing}")
import re
hex64 = re.compile(r"^[0-9a-f]{64}$")
for svc in sorted(images):
    v = images[svc]
    h = v[7:] if v.startswith("sha256:") else v
    if not hex64.match(h):
        probs.append(f"image[{svc}] not full sha256: {v!r}")
    else:
        images[svc] = f"sha256:{h}"
verdict = {"schema": "p25-selected-build-v1",
           "source_sha256": pinned,
           "stamps": {s.split("_")[0]: s for s in stamps},
           "components": components,
           "images": {k: images[k] for k in sorted(images)},
           "problems": probs, "ok": not probs,
           "all_ok": not probs}
Path(out_json).write_text(json.dumps(verdict, indent=1,
                                     sort_keys=True) + "\n")
print(json.dumps(verdict, indent=1, sort_keys=True))
sys.exit(0 if not probs else 1)
PY
  [ $? -eq 0 ] || die "PG build artifact consumption failed"
  cp "$EV_ROOT/selected-build.json" \
    "$LAST_STEP_DIR/build-selected-manifest.json" \
    || die "build manifest archive failed"
}

phase_fixture_up() {
  # AFTER swap success + the old6 steady post-check (fix 4): raise
  # ALL THREE p25 services (fixture + proxy + connector — the healthy
  # window needs proxy+connector for h13) with the explicit whitelist
  # + --no-deps; images/env come from compose-p25-runtime.yaml (cached
  # python:3.12-alpine full ids; p25connector scheduler interval 1;
  # base loops stay OFF).
  run_step fixture-up docker compose "${COMPOSE_AUX[@]}" \
    --project-name "$Q3_PROJECT" up -d --no-deps --no-build --pull never p25fixture \
    p25-proxy p25connector \
    || die "fixture up failed"
  run_step fixture-health python3 -I -c "
import sys, time, urllib.request
def up(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status == 200
    except Exception:
        return False
deadline = time.time() + 60
while time.time() < deadline:
    if (up('http://127.0.0.1:17092/healthz')
            and up('http://127.0.0.1:17093/admin/mode')):
        print('p25 fixture+proxy healthy'); sys.exit(0)
    time.sleep(2)
print('p25 fixture/proxy never healthy'); sys.exit(1)" \
    || die "fixture/proxy health failed"
  # p25connector readiness (HTTP-INTERFACE §2.1): bounded healthz wait
  run_step fixture-connector-wait python3 -I -c "
import sys, time, urllib.request
deadline = time.time() + 60
while time.time() < deadline:
    try:
        with urllib.request.urlopen('http://127.0.0.1:17094/healthz',
                                    timeout=2) as r:
            if r.status == 200:
                print('p25connector healthy'); sys.exit(0)
    except Exception:
        pass
    time.sleep(2)
print('p25connector never healthy'); sys.exit(1)" \
    || die "p25connector health failed"
}

phase_swap() {
  # ---- PURE gates (auditable NOW; nothing has been stopped) --------
  local cand; cand="$(bindings_candidate_sha)" \
    || die "pg-candidate-bindings-r17.json missing/drifted"
  run_step swap-source-gate python3 -I \
    "$HERE/candidate_source_gate_p25.py" \
    --repo "$REPO" --expect "$cand" \
    || die "swap source gate: tree != reviewed candidate"
  # build-binding gate (fix 2): consume the EXACT accepted r18
  # selected record — all 6 groups' cmd/raw/rc/meta verified by
  # path+sha256+bytes (NO glob/latest), rc files == 0, source hard
  # gate == the pinned candidate, all 6 service tag→full-image ids
  # present, and TODAY'S compose/Dockerfile/helper inputs re-hashed
  # equal (any drift refuses BEFORE anything is stopped). This gate
  # does NOT depend on the build phase having run in this pass.
  run_step swap-build-binding python3 -I - \
    "$BUILD_R18" \
    "$CANDIDATE_SOURCE_SHA" "$EV_ROOT/swap-build-binding.json" \
    "$EV_ROOT/selected-build.json" <<'PY'
import hashlib, json, re, sys
from pathlib import Path
(r18_path, pinned, out_json, sel_path) = sys.argv[1:5]


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


probs, consumed = [], {}
b = json.loads(Path(r18_path).read_text())
consumed["build-candidate-bindings-r18.json"] = sha(r18_path)
if b.get("hard_gates", {}).get("source_content_sha256") != pinned:
    probs.append("r18 hard-gate source != pinned candidate")
# 1) every selected group: exact cmd/raw/rc/meta path+sha+bytes, rc==0
for group, entry in sorted(
        (b.get("selected_green_attempts") or {}).items()):
    for kind in ("cmd", "raw", "rc", "meta"):
        a = (entry or {}).get(kind) or {}
        pth = a.get("path") or ""
        f = Path(pth)
        if not f.is_file():
            probs.append(f"{group}.{kind}: missing {pth}")
            continue
        got = sha(f)
        consumed[f"{group}.{kind}"] = got
        if got != a.get("sha256") or \
                f.stat().st_size != a.get("bytes"):
            probs.append(f"{group}.{kind}: sha/bytes drift {pth}")
    rc_p = Path((entry.get("rc") or {}).get("path") or "missing")
    if rc_p.is_file() and rc_p.read_text().strip() != "0":
        probs.append(f"{group}: rc file != 0")
    if entry.get("real_rc") != 0:
        probs.append(f"{group}: real_rc != 0")
# 2) the 3 base full images (+all 6 service ids) present and full hex
svc_images = b.get("service_images") or {}
hex64 = re.compile(r"^[0-9a-f]{64}$")
for svc, m in sorted(svc_images.items()):
    iid = (m or {}).get("image_id") or ""
    h = iid[7:] if iid.startswith("sha256:") else iid
    if not hex64.match(h):
        probs.append(f"service_images[{svc}] not full sha256")
for svc in ("dataset", "pipeline", "connector"):
    if svc not in svc_images:
        probs.append(f"service_images lacks base service {svc}")
# 3) TODAY'S build inputs re-hashed equal (compose/Dockerfile/helper/
#    generated aux override) — input drift refuses BEFORE stop
for key in ("base_compose", "p22_override", "p25_aux_override_new",
            "service_dockerfile", "helper"):
    a = (b.get("inputs") or {}).get(key) or {}
    pth = a.get("path") or ""
    f = Path(pth)
    if not f.is_file():
        probs.append(f"inputs.{key}: missing {pth}")
        continue
    got = sha(f)
    consumed[f"inputs.{key}"] = got
    if got != a.get("sha256"):
        probs.append(f"inputs.{key}: TODAY drift {pth}")
cache = ((b.get("cache_only_images") or {})
         .get("python:3.12-alpine") or {}).get("image_id", "")
# 4) selected-build.json is REQUIRED pre-stop (narrow-review #2:
#    record_deployment/images need it AFTER stop — a missing/bad file
#    must refuse HERE, never after mutation): strict schema/ok/source
#    + the COMPLETE 7-image map, every id equal to r18
sel = None
try:
    sel = json.loads(Path(sel_path).read_text())
except FileNotFoundError:
    probs.append(f"selected-build.json MISSING ({sel_path}) — run the "
                 "build import before swap; refusing BEFORE stop")
except (OSError, ValueError) as exc:
    probs.append(f"selected-build.json not valid JSON: {exc}")
if not isinstance(sel, dict):
    probs.append("selected-build.json must be an object")
else:
    if sel.get("schema") != "p25-selected-build-v1":
        probs.append("selected-build.json schema != "
                     "p25-selected-build-v1")
    if sel.get("ok") is not True or sel.get("all_ok") is not True:
        probs.append("selected-build.json ok/all_ok not true")
    if sel.get("source_sha256") != pinned:
        probs.append("selected-build.json source != pinned candidate")
    want_images = {k: v["image_id"] for k, v in svc_images.items()}
    want_images["python-3.12-alpine"] = cache
    got_images = sel.get("images") or {}
    if not isinstance(got_images, dict):
        probs.append("selected-build.json images not an object")
    else:
        missing_keys = sorted(set(want_images) - set(got_images))
        extra_keys = sorted(set(got_images) - set(want_images))
        if missing_keys:
            probs.append(f"selected-build.json images missing keys: "
                         f"{missing_keys}")
        if extra_keys:
            probs.append(f"selected-build.json images extra keys: "
                         f"{extra_keys}")
        for svc in sorted(set(want_images) & set(got_images)):
            if got_images[svc] != want_images[svc]:
                probs.append(f"selected-build {svc} "
                             f"{str(got_images[svc])[:12]}… != r18 "
                             f"{want_images[svc][:12]}…")
if not hex64.match(cache[7:] if cache.startswith("sha256:") else cache):
    probs.append("cache python:3.12-alpine full id missing")
verdict = {"schema": "p25-swap-build-binding-v2",
           "r18_sha256": consumed["build-candidate-bindings-r18.json"],
           "service_images": {k: v["image_id"] for k, v in
                              sorted(svc_images.items())},
           "cache_python312": cache,
           "inputs_today_sha256": {k: v for k, v in consumed.items()
                                   if k.startswith("inputs.")},
           "problems": probs, "ok": not probs,
           "all_ok": not probs}
Path(out_json).write_text(json.dumps(verdict, indent=1,
                                     sort_keys=True) + "\n")
print(json.dumps(verdict, indent=1, sort_keys=True))
sys.exit(0 if not probs else 1)
PY
  [ $? -eq 0 ] || die "swap build-binding gate failed"
  cp "$EV_ROOT/swap-build-binding.json" \
    "$LAST_STEP_DIR/swap-build-binding-verdict.json" \
    || die "swap gate verdict archive failed"
  # ---- LIVE candidate switch (argv mirrors frozen P24 swap) -------
  [ "$RUNTIME" = "yes" ] || { phase_refuse_runtime swap; return 3; }
  run_step swap-guard-firsttransition python3 -I \
    "$HERE/p25_cohort_guard.py" --policy first-transition \
    --env-dir "$Q3_ENV_DIR" --service connector \
    || die "swap: first-transition guard refused (stack not at the sealed P24-final state — root must re-adjudicate)"
  # fix 2 (live half): the LOCAL tags the up would use must resolve to
  # the r18 full image ids BEFORE anything is stopped — no implied
  # build/pull at up; reviewed local candidate only.
  run_step swap-tag-check python3 -I - \
    "$BUILD_R18" "$Q3_PROJECT" <<'PYTAG'
import json, subprocess, sys
from pathlib import Path
b = json.loads(Path(sys.argv[1]).read_text())
proj = sys.argv[2]
probs = []
for svc, m in sorted((b.get("service_images") or {}).items()):
    tag = m["tag"]
    pr = subprocess.run(["docker", "image", "inspect", "--format",
                         "{{.Id}}", tag], capture_output=True, text=True,
                        timeout=60)
    if pr.returncode != 0:
        probs.append(f"{svc}: local tag {tag} not resolvable "
                     f"(rc={pr.returncode}) — refusing to up/build")
        continue
    got = pr.stdout.strip()
    if got != m["image_id"]:
        probs.append(f"{svc}: local tag {tag} -> {got[:19]} != r18 "
                     f"{m['image_id'][:19]} (tag drift)")
print(json.dumps({"schema": "p25-swap-tag-check-v1",
                  "problems": probs, "ok": not probs,
                  "all_ok": not probs}, indent=1, sort_keys=True))
sys.exit(0 if not probs else 1)
PYTAG
  [ $? -eq 0 ] || die "swap tag-check FAILED — stop/up NEVER constructed (r18 tag drift)"
  run_step swap-stop "${COMPOSE_BASE[@]}" stop -t 120 \
    dataset pipeline connector || die "swap stop failed"
  run_step swap-up-base "${COMPOSE_BASE[@]}" up -d --no-deps \
    dataset pipeline connector || die "swap up failed"
  run_step swap-health python3 -I "$P1/smoke/smoke.py" \
    --env-dir "$Q3_ENV_DIR" --wait-only || die "swap health failed"
  record_deployment candidate-swap
  run_step swap-images-collect python3 -I "$HERE/check_running_images_p25.py" \
    collect --env-dir "$Q3_ENV_DIR" \
    --out "$EV_ROOT/swap-running.json" || die "swap images collect failed"
  run_step swap-images-verify python3 -I "$HERE/check_running_images_p25.py" \
    verify --running "$EV_ROOT/swap-running.json" --phase swap \
    --selected-build "$EV_ROOT/selected-build.json" \
    --out "$EV_ROOT/swap-images-verdict.json" \
    || die "swap images verify failed"
  # fix 3: IMMEDIATE steady old6 re-verify on the ACTUAL new boot +
  # full image — whole-set 27-field + parents, rc0 required BEFORE any
  # healthy HTTP (not deferred to pre_tail; pre_tail re-checks again).
  local rec0; rec0="$(latest_deployment_record)"
  run_step swap-steady-old6 python3 -I "$HERE/p25_cohort_guard.py" \
    --policy steady --env-dir "$Q3_ENV_DIR" --service connector \
    --target-boot "$(python3 -c \
      'import json,sys;print(json.load(open(sys.argv[1]))["connector_boot"])' "$rec0")" \
    --target-image "$(python3 -c \
      'import json,sys;print(json.load(open(sys.argv[1]))["connector_image"])' "$rec0")" \
    || die "swap: post-switch steady old6 re-verify FAILED — no HTTP until green"
  # canonical swap stage record (v_swap: target_boot/target_image)
  local rec; rec="$(latest_deployment_record)"
  run_step swap python3 -I -c "
import json, sys
from pathlib import Path
rec = json.loads(Path(sys.argv[1]).read_text())
out = {'schema': 'p25-swap-record-v1', 'ok': True, 'all_ok': True,
       'target_boot': rec['connector_boot'],
       'target_image': rec['connector_image'],
       'source_sha256': rec['source_sha256'],
       'selected_build_sha256': rec['selected_build_sha256'],
       'deployment_record': sys.argv[1]}
print(json.dumps(out, indent=1, sort_keys=True))" \
    "$rec" || die "swap record failed"
}

phase_baseline_pre() {
  if [ -f "$EV_ROOT/00-baseline-pre-path.txt" ]; then
    local old; old="$(cat "$EV_ROOT/00-baseline-pre-path.txt")"
    python3 -I -c 'import json,sys;from pathlib import Path
p=Path(sys.argv[1]);m=json.loads((p/"manifest.json").read_text())
assert m["all_ok"] is True and m["steps"] and all(x["ok"] is True for x in m["steps"])
print("reusing successful pre baseline:",p)' "$old" \
      || die "saved baseline pointer is not a successful baseline"
    return 0
  fi
  # PRE-WINDOW readonly baseline = the reread phase's "before" (frozen
  # P22 step functions imported read-only; output lands in P25 EV only)
  run_step baseline-pre python3 -I "$HERE/readonly_baseline_p25.py" \
    --env-dir "$Q3_ENV_DIR" --out-root "$EV_ROOT/readonly-baselines" \
    || die "pre-window baseline failed"
  local d; d="$(capture_step_var BASELINE_DIR)"
  [ -n "$d" ] && [ -f "$d/manifest.json" ] \
    || die "baseline-pre: no BASELINE_DIR"
  printf '%s\n' "$d" > "$EV_ROOT/00-baseline-pre-path.txt"
}

phase_window() {
  phase_baseline_pre || die "pre-window baseline failed"
  run_step window-probe python3 -I "$HERE/smoke_p25.py" \
    --contract-probe || die "smoke interface probe failed"
  # fix 5 (HTTP-INTERFACE §1): inject the three tenant JWTs from the
  # released runner's environment (values NEVER recorded/echoed) and
  # pass the ACTUAL CLI base URLs (dataset base = the real dataset
  # service, engine-side remote proof, not via the proxy).
  local jwt_missing=""
  for v in OPENFOUNDRY_SMOKE_JWT OPENFOUNDRY_SMOKE_JWT_ORG2 \
           OPENFOUNDRY_SMOKE_ARM_JWT; do
    [ -n "${!v:-}" ] || jwt_missing="$jwt_missing $v"
  done
  [ -z "$jwt_missing" ] \
    || die "window: missing tenant JWT env (never printed):$jwt_missing"
  run_step window python3 -I "$HERE/smoke_p25.py" \
    --phase healthy --released \
    --sql-env-dir "$Q3_ENV_DIR" \
    --fixture-src-url http://p25fixture:8090 \
    --pg-proof-file "$PG_BINDINGS" \
    --connector-base http://127.0.0.1:17094 \
    --dataset-base http://127.0.0.1:17078 \
    --fixture-base http://127.0.0.1:17092 \
    --proxy-base http://127.0.0.1:17093 \
    --evidence-dir "$EV_ROOT/window" \
    --result-file "$EV_ROOT/window-result.json" \
    || die "smoke healthy window failed"
}

steady_latest_record() {
  local rec; rec="$(latest_deployment_record)" || die "latest deployment record missing"
  local boot image
  boot="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["connector_boot"])' "$rec")"
  image="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["connector_image"])' "$rec")"
  run_step "$1" python3 -I "$HERE/p25_cohort_guard.py" --policy steady \
    --env-dir "$Q3_ENV_DIR" --service connector --target-boot "$boot" \
    --target-image "$image" || die "post-regression old6 steady refused"
}

phase_regressions() {
  phase_regressions_pre_p24
  phase_regressions_p24
}

phase_regressions_pre_p24() {
  # frozen-suite regression window. The old aux (connector2/3 +
  # dataset-proxy) rises here (frozen P22 helper, P24 07-aux-up argv)
  # and comes back down in aux_down BEFORE ABC. Adapter invocations
  # verify the reviewed deployment record and pass the steady shim
  # env; every restart-bearing suite refreshes the record so later
  # guards adjudicate against the ACTUAL new boot.
  [ -f "$EV_ROOT/selected-build.json" ] \
    || die "regressions: selected-build.json missing (build phase)"
  run_step reg-inputs-pure python3 -I - "$HERE" "$REPO" "$Q3_ENV_DIR" <<'PYREG'
import hashlib, json, sys
from pathlib import Path
here, repo, q3 = map(Path, sys.argv[1:])
b = json.loads((here/"build-candidate-bindings-r18.json").read_text())
a = b["inputs"]["p22_override"]
rendered = (here.parent/"business-platform-p2-2/compose-p22-connector2.yaml.template").read_bytes().replace(b"__OF_ROOT__",str(repo).encode())
actual = q3/"compose-p22-connector2.yaml"
assert str(actual) == a["path"]
assert hashlib.sha256(rendered).hexdigest() == a["sha256"]
assert hashlib.sha256(actual.read_bytes()).hexdigest() == a["sha256"]
print(json.dumps({"ok":True,"all_ok":True,"rendered_sha256":a["sha256"],"current_override_sha256":a["sha256"]}))
PYREG
  [ $? -eq 0 ] || die "old aux deterministic render/input SHA drift"
  run_step reg-local-tags python3 -I - "$BUILD_R18" <<'PYREGTAG'
import json, subprocess, sys
from pathlib import Path
b=json.loads(Path(sys.argv[1]).read_text())
expected={v["tag"]:v["image_id"] for v in b["service_images"].values()}
expected["python:3.12-alpine"]=b["cache_only_images"]["python:3.12-alpine"]["image_id"]
for tag,want in sorted(expected.items()):
 r=subprocess.run(["docker","image","inspect","--format","{{.Id}}",tag],capture_output=True,text=True,timeout=60)
 assert r.returncode==0 and r.stdout.strip()==want, "local tag/image drift: "+tag
print(json.dumps({"ok":True,"all_ok":True,"tags":expected}))
PYREGTAG
  [ $? -eq 0 ] || die "old aux local image gate failed"
  run_step reg-aux-old-up env P22_CONFIRM_CONNECTOR2=yes \
    sh "$P22/p22_connector2.sh" up || die "old aux up failed"
  run_step reg-render-postcheck python3 -I - "$BUILD_R18" <<'PYRENDER'
import hashlib,json,sys
from pathlib import Path
a=json.loads(Path(sys.argv[1]).read_text())["inputs"]["p22_override"]
assert hashlib.sha256(Path(a["path"]).read_bytes()).hexdigest()==a["sha256"]
print(json.dumps({"ok":True,"all_ok":True,"p22_override_sha256":a["sha256"]}))
PYRENDER
  [ $? -eq 0 ] || die "old aux helper rendered changed bytes"
  run_step regressions-p1pre9 bash -o pipefail "$P1/run.sh" smoke \
    || die "p1 pre9 failed"
  cp "$Q3_ENV_DIR/result.json" "$LAST_STEP_DIR/result.json" \
    || die "p1 pre9: result.json missing"
  cp "$Q3_ENV_DIR/result.json" "$LAST_STEP_DIR/p1pre9-report.json" \
    || die "p1 pre9 report archive failed"
  local rec; rec="$(latest_deployment_record)" \
    || die "regressions: no deployment record"
  run_step regressions-p23 python3 -I "$HERE/p25_old_suite_adapter.py" \
    --suite smoke_p23.py --expect-adaptations 1 \
    --deployment-record "$rec" \
    --selected-build "$EV_ROOT/selected-build.json" \
    --record-dir "$EV_ROOT/09-p23-adapted-records" -- \
    --env-dir "$Q3_ENV_DIR" --allow-restart \
    || die "p23 adapted rerun failed"
  if [ -f "$Q3_ENV_DIR/result_p23.json" ]; then
    cp "$Q3_ENV_DIR/result_p23.json" "$LAST_STEP_DIR/" || die "p23 result archive failed"
    cp "$Q3_ENV_DIR/result_p23.json" "$LAST_STEP_DIR/p23-report.json" \
      || die "p23 report archive failed"
    local sfx; sfx="$(python3 -c \
      "import json;print(json.load(open('$Q3_ENV_DIR/result_p23.json'))['suffix'])" \
      2>/dev/null || true)"
    if [ -n "$sfx" ]; then
      printf '%s\n' "$sfx" > "$LAST_STEP_DIR/snapshot-suffix.txt"
      mkdir -p "$LAST_STEP_DIR/snapshots_latest"
      cp -R "$Q3_ENV_DIR/snapshots_p23/$sfx" \
        "$LAST_STEP_DIR/snapshots_latest/" 2>/dev/null \
        || die "p23 snapshots archive failed"
    fi
  else
    die "p23 adapted: no result_p23.json produced"
  fi
  record_deployment p23-q-restart
  steady_latest_record reg-p23-steady-old6
  rec="$(latest_deployment_record)"
  run_step regressions-p22 env P22_CONFIRM_CONNECTOR2=yes \
    python3 -I "$P22/smoke_p22.py" --env-dir "$Q3_ENV_DIR" \
    || die "p22 full failed"
  [ -f "$Q3_ENV_DIR/result_p22.json" ] \
    || die "p22 full: no result_p22.json produced"
  cp "$Q3_ENV_DIR/result_p22.json" "$LAST_STEP_DIR/" \
    || die "p22 result archive failed"
  cp "$Q3_ENV_DIR/result_p22.json" "$LAST_STEP_DIR/p22-report.json" \
    || die "p22 report archive failed"
  run_step reg-p22-ids python3 -I - "$EV_ROOT/selected-build.json" \
    "$EV_ROOT/p22-image-ids.json" <<'PY'
import json, sys
from pathlib import Path
sel = json.loads(Path(sys.argv[1]).read_text())
short = {svc: img[7:19] for svc, img in sel["images"].items()
         if svc in ("connector", "connector2", "connector3")}
Path(sys.argv[2]).write_text(json.dumps(
    {"derived_from": sys.argv[1],
     "full": {k: sel["images"][k] for k in short},
     **short}, indent=1) + "\n")
print("P22_IDS=" + ",".join(f"{k}={v}" for k, v in sorted(short.items())))
PY
  [ $? -eq 0 ] || die "p22 ids derivation failed"
  run_step reg-p22-post-images python3 -I "$P22/swap_verify.py" \
    "$Q3_ENV_DIR" "$EV_ROOT/p22-image-ids.json" \
    || die "p22 post-images verify failed"
  record_deployment p22-restart
  steady_latest_record reg-p22-steady-old6
}

p24_verify_interval() {
  run_step "$1" python3 -I - "$Q3_ENV_DIR" "$2" "$EV_ROOT/selected-build.json" <<'PYINTERVAL'
import json, subprocess, sys, time, urllib.request
from pathlib import Path
q3,want=Path(sys.argv[1]),sys.argv[2]
images=json.loads(Path(sys.argv[3]).read_text())["images"]
base=["docker","compose","-f",str(q3/"compose.yaml"),"-f",str(q3/"compose-p22-connector2.yaml"),"--project-name","openfoundry-platform-p1-q3v9"]
out={}
for service,port in [("connector",17088),("connector2",17089),("connector3",17087)]:
 r=subprocess.run(base+["ps","-q",service],capture_output=True,text=True,timeout=60)
 assert r.returncode==0 and len(r.stdout.split())==1
 cid=r.stdout.strip()
 r=subprocess.run(["docker","inspect","--format","{{.Image}}",cid],capture_output=True,text=True,timeout=60)
 assert r.returncode==0 and r.stdout.strip()==images[service],service+" selected image drift"
 # Full Env stays only in memory; persist ONLY the scheduler field.
 r=subprocess.run(["docker","inspect","--format","{{json .Config.Env}}",cid],capture_output=True,text=True,timeout=60)
 assert r.returncode==0
 vals=[x.split("=",1)[1] for x in json.loads(r.stdout) if x.startswith("OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=")]
 assert vals==([want] if want else []), service+" scheduler interval differs"
 deadline=time.monotonic()+60
 while True:
  try:
   with urllib.request.urlopen("http://127.0.0.1:"+str(port)+"/healthz",timeout=3) as response:
    if response.status==200:break
  except Exception:pass
  assert time.monotonic()<deadline,service+" health timeout"
  time.sleep(1)
 out[service]={"container_id":cid,"image":images[service],"scheduler_interval":want or "unset","health":200}
print(json.dumps({"ok":True,"all_ok":True,"services":out}))
PYINTERVAL
}

phase_regressions_p24() (
  local loop_applied=no p25_stopped=no rf="" ovr="" interval=""
  local p24="$HERE/../business-platform-p2-4"
  local compose_old=(docker compose -f "$Q3_ENV_DIR/compose.yaml" -f "$Q3_ENV_DIR/compose-p22-connector2.yaml" --project-name "$Q3_PROJECT")
  p24_restore() {
    if [ "$loop_applied" = yes ]; then
      run_step p24-restore-loop0 "${compose_old[@]}" up -d --no-deps --no-build --pull never connector connector2 connector3 || return 1
      p24_verify_interval p24-restore-health-interval "" || return 1
      record_deployment p24-loop-restore
      steady_latest_record p24-restore-steady-old6
      loop_applied=no
    fi
    if [ "$p25_stopped" = yes ]; then
      run_step p24-p25-resume docker compose "${COMPOSE_AUX[@]}" --project-name "$Q3_PROJECT" start p25connector || return 1
      run_step p24-p25-health python3 -I -c '
import time,urllib.request
end=time.monotonic()+60
while True:
 try:
  with urllib.request.urlopen("http://127.0.0.1:17094/healthz",timeout=3) as r:
   if r.status==200:print("p25connector healthy");break
 except Exception:pass
 assert time.monotonic()<end,"p25connector health timeout"
 time.sleep(1)' || return 1
      p25_stopped=no
    fi
  }
  trap 'saved_rc=$?; trap - EXIT; p24_restore; cleanup_rc=$?; [ "$saved_rc" -ne 0 ] || saved_rc=$cleanup_rc; exit "$saved_rc"' EXIT
  phase_baseline_pre || die "P24: baseline missing"
  steady_latest_record p24-entry-steady-old6
  # The engine-owned P25 trial is terminal; preserve its container/image.
  run_step p24-p25-pause docker compose "${COMPOSE_AUX[@]}" --project-name "$Q3_PROJECT" stop -t 120 p25connector || die "P24: p25connector stop failed"
  p25_stopped=yes
  run_step p24-schedule-render bash "$p24/p24_schedule_env.sh" render || die "P24 scheduler render failed"
  ovr="$(capture_step_var OVERRIDE_FILE)"; interval="$(capture_step_var OVERRIDE_INTERVAL)"
  [ -n "$ovr" ] && [ -f "$ovr" ] && [ "$interval" = 1 ] || die "P24 schedule override missing"
  printf '%s\n' "$ovr" > "$EV_ROOT/p24-schedule-override-path.txt"
  # Mark before apply: even a partial recreate must restore loop0 finally.
  loop_applied=yes
  run_step p24-loop-apply docker compose -f "$Q3_ENV_DIR/compose.yaml" -f "$Q3_ENV_DIR/compose-p22-connector2.yaml" -f "$ovr" --project-name "$Q3_PROJECT" up -d --no-deps --no-build --pull never connector connector2 connector3 || die "P24 loop enable failed"
  p24_verify_interval p24-loop-health-interval "$interval" || die "P24 loop1/health failed"
  record_deployment p24-loop-enable
  steady_latest_record p24-loop-steady-old6
  local rec; rec="$(latest_deployment_record)" || die "P24 record missing"
  local suite_rc=0 suite_dir=""
  run_step regressions-p24 python3 -I "$HERE/p25_old_suite_adapter.py" \
    --suite smoke_p24.py --expect-adaptations 2 --deployment-record "$rec" \
    --selected-build "$EV_ROOT/selected-build.json" --record-dir "$EV_ROOT/10c-p24-adapted-records" -- \
    --env-dir "$Q3_ENV_DIR" --confirm-http --official --with-restart-probe || suite_rc=$?
  suite_dir="$LAST_STEP_DIR"; rf="$(capture_step_var RESULT_FILE)"
  [ -n "$rf" ] && [ -f "$rf" ] || die "P24 result missing (original suite finally did not finish)"
  cp "$rf" "$suite_dir/p24-report.json" || die "P24 result copy failed"
  # Reuse the exact frozen sidecar collector BODY, never its driver/old EV.
  run_step p24-sidecar-extract python3 -I - "$p24/formal_run_p24.sh" "$EV_ROOT/gen/p24-sidecars.py" <<'PYSIDECAR'
import hashlib,sys
from pathlib import Path
src,dst=map(Path,sys.argv[1:]);text=src.read_text();body=text.split("cat > \"$sidecar\" <<'PY'\n",1)[1].split('\nPY\n',1)[0]
dst.parent.mkdir(parents=True,exist_ok=True);dst.write_text(body+'\n')
print("frozen_driver_sha256="+hashlib.sha256(src.read_bytes()).hexdigest())
print("sidecar_helper_sha256="+hashlib.sha256(dst.read_bytes()).hexdigest())
PYSIDECAR
  [ $? -eq 0 ] || die "P24 sidecar extractor failed"
  run_step p24-sidecars python3 -I "$EV_ROOT/gen/p24-sidecars.py" "$rf" "$suite_dir" || suite_rc=1
  record_deployment p24-g4-restart
  steady_latest_record p24-after-suite-steady-old6
  # Frozen suite always attempts global finally revoke/disable, even on a
  # failed scenario. Independently adjudicate its actual owned result.
  run_step p24-owned-quiesce python3 -I "$p24/check_owned_quiesced_p24.py" --p24-result "$rf" --env-dir "$Q3_ENV_DIR" || suite_rc=1
  p24_restore || die "P24 finally restore failed"
  trap - EXIT
  return "$suite_rc"
)

phase_reread_artifacts() {
  local before; before="$(cat "$EV_ROOT/00-baseline-pre-path.txt" 2>/dev/null)"
  [ -n "$before" ] && [ -f "$before/manifest.json" ] \
    || die "reread: missing pre-window baseline path"
  run_step reread-post python3 -I "$HERE/readonly_baseline_p25.py" \
    --env-dir "$Q3_ENV_DIR" --out-root "$EV_ROOT/readonly-baselines" \
    || die "reread collection failed"
  local after; after="$(capture_step_var BASELINE_DIR)"
  [ -n "$after" ] && [ -f "$after/manifest.json" ] \
    || die "reread: no BASELINE_DIR"
  printf '%s\n' "$after" > "$EV_ROOT/12-reread-path.txt"
  cp "$after/manifest.json" "$EV_ROOT/12-reread-after-manifest.json" \
    || die "reread: after-manifest archive failed"
  mkdir -p "$EV_ROOT/12-reread-compare"
  run_step reread-artifacts python3 -I "$P22/compare_baseline.py" \
    --before "$before/manifest.json" \
    --after "$EV_ROOT/12-reread-after-manifest.json" \
    --out "$EV_ROOT/12-reread-compare/compare.json" --mode restart \
    || die "reread compare failed"
  cp "$EV_ROOT/12-reread-compare/compare.json" \
    "$LAST_STEP_DIR/reread-report.json" \
    || die "reread report archive failed"
}

phase_p1post9() {
  run_step p1post9 bash -o pipefail "$P1/run.sh" smoke \
    || die "p1 post9 failed"
  cp "$Q3_ENV_DIR/result.json" "$LAST_STEP_DIR/result.json" \
    || die "p1 post9: result.json missing"
  cp "$Q3_ENV_DIR/result.json" "$LAST_STEP_DIR/p1post9-report.json" \
    || die "p1 post9 report archive failed"
}

restart_checkpoint_snapshot() {
  run_step "$1" python3 -I - "$EV_ROOT" "$2" <<'PYRESTARTCP'
import hashlib,json,os,sys,urllib.request,urllib.error
from pathlib import Path
ev,out=map(Path,sys.argv[1:]);root=out.with_suffix('.raw');root.mkdir(parents=True,exist_ok=True)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
summaries=[ev/'h1-trial/result-attempt1.json',ev/'window-result.json']
ids=set();inputs=[]
for path in summaries:
 s=json.loads(path.read_text());hp=Path(s['attempts']['handles_file']);hs=json.loads(hp.read_text())
 inputs.extend([{'path':str(path),'sha256':sha(path)},{'path':str(hp),'sha256':sha(hp)}])
 ids.update(h['ref'] for h in hs['handles'] if h.get('ok') is True and h.get('kind')=='def' and h.get('scenario')=='h1')
 if path.name=='window-result.json':ids.update(o['def_id'] for o in s['final_owner_roster'])
assert ids and os.environ.get('OPENFOUNDRY_SMOKE_JWT')
views={};members=[]
for did in sorted(ids):
 url='http://127.0.0.1:17088/api/v1/data-connection/syncs/'+did+'/checkpoint'
 p=root/(did+'.json');(root/(did+'.cmd.json')).write_text(json.dumps({'method':'GET','url':url})+'\n')
 status=0;body=b'';rc=0
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers={'Authorization':'Bearer '+os.environ['OPENFOUNDRY_SMOKE_JWT']}),timeout=15) as r:status=r.status;body=r.read()
 except urllib.error.HTTPError as e:status=e.code;body=e.read();rc=1
 p.write_bytes(body);(root/(did+'.status')).write_text(str(status)+'\n');(root/(did+'.rc')).write_text(str(rc)+'\n')
 assert status==200 and rc==0,'CP GET failed for '+did
 views[did]=json.loads(body);members.append({'path':str(p),'sha256':sha(p),'status':status,'rc':rc})
doc={'schema':'p25-restart-cp-snapshot-v1','inputs':inputs,'views':views,'members':members,'ok':True}
out.write_text(json.dumps(doc,indent=2)+'\n');print(json.dumps({'ok':True,'defs':sorted(views),'out':str(out)}))
PYRESTARTCP
}

phase_restart_regression() {
  # controlled base3 restart — h10 is a separate aux crash runner. New
  # boot is RECORDED (steady guards after this use the fresh record).
  run_step restart-before-images-collect python3 -I "$HERE/check_running_images_p25.py" collect \
    --env-dir "$Q3_ENV_DIR" --out "$EV_ROOT/restart-before-running.json" \
    || die "restart before images collection failed"
  run_step restart-before-images-verify python3 -I "$HERE/check_running_images_p25.py" verify \
    --running "$EV_ROOT/restart-before-running.json" --phase restart \
    --selected-build "$EV_ROOT/selected-build.json" \
    --out "$EV_ROOT/restart-before-images-verdict.json" || die "restart before images refused"
  restart_checkpoint_snapshot restart-cp-before "$EV_ROOT/restart-cp-before.json" || die "restart before CP read failed"
  run_step restart-stop "${COMPOSE_BASE[@]}" stop -t 120 \
    dataset pipeline connector || die "restart stop failed"
  run_step restart-start "${COMPOSE_BASE[@]}" start \
    dataset pipeline connector || die "restart start failed"
  run_step restart-health python3 -I "$P1/smoke/smoke.py" \
    --env-dir "$Q3_ENV_DIR" --wait-only || die "restart health failed"
  record_deployment restart-regression
  run_step restart-images-collect python3 -I \
    "$HERE/check_running_images_p25.py" collect \
    --env-dir "$Q3_ENV_DIR" --out "$EV_ROOT/restart-running.json" \
    || die "restart images collect failed"
  run_step restart-regression python3 -I \
    "$HERE/check_running_images_p25.py" verify \
    --running "$EV_ROOT/restart-running.json" --phase restart \
    --selected-build "$EV_ROOT/selected-build.json" \
    --out "$EV_ROOT/restart-images-verdict.json" \
    || die "restart images verify failed"
  cp "$EV_ROOT/restart-images-verdict.json" \
    "$LAST_STEP_DIR/restart-images-verdict.json" \
    || die "restart verdict archive failed"
  steady_latest_record restart-steady-old6
  restart_checkpoint_snapshot restart-cp-after "$EV_ROOT/restart-cp-after.json" || die "restart after CP read failed"
  run_step restart-boot-cp-verify python3 -I - "$EV_ROOT" <<'PYRESTARTVERIFY'
import json,sys
from pathlib import Path
from datetime import datetime
ev=Path(sys.argv[1]);before=json.loads((ev/'restart-before-running.json').read_text())['services'];after=json.loads((ev/'restart-running.json').read_text())['services']
assert set(before)==set(after)
changes={}
for service,b in before.items():
 a=after[service];assert a['image']==b['image'] and a['containers']==b['containers']==1
 bt,at=b['started_at'],a['started_at']
 if service in ('dataset','pipeline','connector'):assert datetime.fromisoformat(at.replace('Z','+00:00'))>datetime.fromisoformat(bt.replace('Z','+00:00'))
 else:assert at==bt,'aux boot changed during base3 restart: '+service
 changes[service]={'before':bt,'after':at,'image':a['image']}
cb=json.loads((ev/'restart-cp-before.json').read_text());ca=json.loads((ev/'restart-cp-after.json').read_text());assert cb['views'] and cb['views']==ca['views'],'CP body changed or empty across base3 restart'
verdict={'ok':True,'all_ok':True,'base3_advanced_aux_unchanged':True,'checkpoint_views_unchanged':True,'boots':changes,'checkpoint_defs':sorted(cb['views'])}
(ev/'restart-boot-cp-verdict.json').write_text(json.dumps(verdict,indent=2)+'\n');print(json.dumps(verdict,indent=2))
PYRESTARTVERIFY
  [ $? -eq 0 ] || die "restart boot/CP invariants failed"
}

phase_pre_tail_old6() {
  steady_latest_record pre-tail-gates
  local rec; rec="$(latest_deployment_record)"
  run_step pre-tail-record-verdict python3 -I - "$rec" "$EV_ROOT" <<'PYPRETAIL'
import json,sys
from pathlib import Path
r=json.loads(Path(sys.argv[1]).read_text());ev=Path(sys.argv[2]);v={'ok':True,'all_ok':True,'policy':'steady','target_boot':r['connector_boot'],'target_image':r['connector_image'],'deployment_record':sys.argv[1]}
steps=sorted((ev/'steps').glob('pre-tail-gates-attempt*'),key=lambda p:int(p.name.rsplit('attempt',1)[1]));assert steps
(steps[-1]/'pre-tail-verdict.json').write_text(json.dumps(v,indent=2)+'\n');print(json.dumps(v))
PYPRETAIL
  [ $? -eq 0 ] || die "pre-tail strict old6 record failed"
}

phase_pre_tail_gates() {
  # Only BEFORE h10 held creation. Semantic declaration is a separate
  # post-h10 pure phase; never ask the strict6 guard to ignore new work.
  phase_pre_tail_old6
}

phase_tail_declare() {
  local h10dir; h10dir="$(cat "$EV_ROOT/h10-selected-path.txt")" \
    || die "accepted h10 pointer missing"
  run_step tail-known-lease-proof python3 -I - "$h10dir" <<'PYTAILLEASE'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1]);report=json.loads((root/'result.json').read_text());proof=json.loads((root/'lease-observation/attempt1/summary.json').read_text())
assert report['ok'] is True and proof['ok'] is True and proof['known_binding']==report['held']
assert proof['owner']['run_id']==report['held']['run_id'] and proof['owner']['run_status']=='running' and proof['owner']['blocked_reason']=='requires_reconciliation'
for kind in ('checkpoint','run'):
 p=Path(proof[kind+'_path']);assert hashlib.sha256(p.read_bytes()).hexdigest()==proof[kind+'_sha256']
assert proof['checkpoint_data_unchanged'] is True and proof['protected_run_unchanged'] is True
print(json.dumps({'ok':True,'all_ok':True,'held':report['held'],'lease_proof':str(root/'lease-observation/attempt1/summary.json')}))
PYTAILLEASE
  [ $? -eq 0 ] || die "h10 actual reconciliation reason not proven"
  local binding; binding="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["tail_known_binding_file"])' "$h10dir/result.json")"
  run_step tail-declare python3 -I "$HERE/p25_tail_roster.py" declare \
    --h10-binding "$binding" --failed-healthy-summary "$EV_ROOT/window-result.json" \
    --out "$EV_ROOT/tail-declaration.json" || die "semantic tail declaration refused"
}

phase_tail() {
  [ ! -e "$EV_ROOT/tail-result.json" ] || die "tail result exists; no automatic tail replay"
  local h10dir; h10dir="$(cat "$EV_ROOT/h10-selected-path.txt")" || die "h10 pointer missing"
  local merged; merged="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["merged_handles_file"])' "$h10dir/result.json")"
  # Future h11/h14 UUIDs are produced by this execution. Only semantic
  # declaration + actual prior handles are inputs; bindings are OUTPUT.
  run_step tail python3 -I "$HERE/smoke_p25.py" \
    --phase tail --released \
    --connector-base http://127.0.0.1:17094 \
    --dataset-base http://127.0.0.1:17078 \
    --fixture-base http://127.0.0.1:17092 \
    --proxy-base http://127.0.0.1:17093 \
    --fixture-src-url http://p25fixture:8090 \
    --tail-declaration "$EV_ROOT/tail-declaration.json" \
    --owned-handles-file "$EV_ROOT/window-union-handles.json" \
    --owned-handles-file "$merged" \
    --evidence-dir "$EV_ROOT/tail" --result-file "$EV_ROOT/tail-result.json" \
    || die "actual tail failed; retain all partial handles/owners, never automatically retry"
  run_step tail-bind python3 -I "$HERE/p25_tail_roster.py" verify \
    --declaration "$EV_ROOT/tail-declaration.json" --summary "$EV_ROOT/tail-result.json" \
    --out "$EV_ROOT/tail-runtime-verdict.json" || die "tail exact identity replay refused"
  run_step tail-owner-roster python3 -I "$HERE/p25_tail_roster.py" owner-roster \
    --declaration "$EV_ROOT/tail-declaration.json" --summary "$EV_ROOT/tail-result.json" \
    --out "$EV_ROOT/tail-owner-roster.json" || die "actual owner roster derivation refused"
}

phase_aux_down() {
  # TRUE-quiet proofs FIRST; any failed/missing/invalid proof MUST
  # ABORT the removal. Strict non-bool int checks. The step stdout IS
  # the RAW proof (ARCHIVE-INTERFACE §3): the proxy stats JSON object
  # / the per-src fixture counters JSON LIST as the LAST line — the
  # postfacts collector consumes them from the aux-down step dir.
  run_step proxy-quiet-proof python3 -I -c "
import json, sys, urllib.request
try:
    with urllib.request.urlopen(
            'http://127.0.0.1:17093/admin/stats', timeout=5) as r:
        s = json.loads(r.read())
except Exception as exc:
    print(f'quiet proof failed: {exc}'); sys.exit(1)
def si(v):
    return isinstance(v, int) and not isinstance(v, bool)
ok = (si(s.get('commit_inflight')) and s['commit_inflight'] == 0
      and si(s.get('commit_started')) and si(s.get('commit_finished'))
      and s['commit_started'] == s['commit_finished'])
print(json.dumps(s, sort_keys=True))
sys.exit(0 if ok else 1)"
  local proxy_rc=$?
  local proxy_dir="$LAST_STEP_DIR"
  [ $proxy_rc -eq 0 ] || die "proxy true-quiet proof FAILED — aux removal ABORTED"
  run_step fixture-quiet-proof python3 -I -c "
import json, sys, urllib.request
SRCS = ['p25h1', 'p25h2', 'p25h3', 'p25h4', 'p25h6', 'p25h13',
        'p25empty']
bodies = []
for n in SRCS:
    try:
        with urllib.request.urlopen(
                f'http://127.0.0.1:17092/admin/src/{n}',
                timeout=5) as r:
            b = json.loads(r.read())
    except Exception as exc:
        print(f'fixture quiet proof failed for {n}: {exc}'); sys.exit(1)
    bodies.append(b)
def si(v):
    return isinstance(v, int) and not isinstance(v, bool)
bad = [n for n, b in zip(SRCS, bodies)
       if not isinstance(b, dict) or b.get('name') != n
       or not si(b.get('inventory_started'))
       or not si(b.get('inventory_finished'))
       or b.get('inventory_started') != b.get('inventory_finished')
       or not si(b.get('active')) or b.get('active') != 0]
print(json.dumps(bodies, indent=1, sort_keys=True))
if bad:
    print('fixture quiet fields invalid: ' + ','.join(bad), file=sys.stderr)
sys.exit(0 if not bad else 1)"
  local fx_rc=$?
  local fx_dir="$LAST_STEP_DIR"
  [ $fx_rc -eq 0 ] || die "fixture true-quiet proof FAILED — aux removal ABORTED"
  run_step aux-down docker compose "${COMPOSE_AUX[@]}" \
    --project-name "$Q3_PROJECT" rm -sf p25fixture p25-proxy \
    p25connector || die "aux down failed"
  # ARCHIVE-INTERFACE §3: the RAW quiet proofs live IN the selected
  # aux-down step dir under their canonical names
  cp "$proxy_dir/stdout.log" \
    "$LAST_STEP_DIR/proxy-quiet-proof.stdout.log" \
    || die "aux-down: proxy quiet raw archive failed"
  cp "$fx_dir/stdout.log" \
    "$LAST_STEP_DIR/fixture-quiet-proof.stdout.log" \
    || die "aux-down: fixture quiet raw archive failed"
  # old aux (connector2/3 + dataset-proxy) back down BEFORE ABC —
  # frozen P22 helper, P24 99-aux-down argv.
  run_step aux-old-down sh "$P22/p22_connector2.sh" down \
    || die "old aux down failed"
}

phase_final_images() {
  run_step final-images-collect python3 -I \
    "$HERE/check_running_images_p25.py" collect \
    --env-dir "$Q3_ENV_DIR" --out "$EV_ROOT/final-running.json" \
    || die "final images collect failed"
  run_step final-images python3 -I \
    "$HERE/check_running_images_p25.py" verify \
    --running "$EV_ROOT/final-running.json" --phase post-aux \
    --selected-build "$EV_ROOT/selected-build.json" \
    --out "$EV_ROOT/final-images-verdict.json" \
    || die "final images verify failed"
  cp "$EV_ROOT/final-images-verdict.json" \
    "$LAST_STEP_DIR/final-images-verdict.json" \
    || die "final images verdict archive failed"
}

phase_p21abc() {
  run_step p21abc python3 -I "$P21/smoke_p21.py" --env-dir "$Q3_ENV_DIR" \
    || die "p21 ABC failed"
  [ -f "$Q3_ENV_DIR/result_p21.json" ] \
    || die "p21abc: no result_p21.json produced"
  cp "$Q3_ENV_DIR/result_p21.json" "$LAST_STEP_DIR/" \
    || die "p21abc result archive failed"
  # ARCHIVE-INTERFACE §4: THIS round's raw result at the canonical EV
  # path (byte-exact; the collector re-copies it with sha binding).
  [ -e "$EV_ROOT/abc-result.json" ] \
    && die "p21abc: $EV_ROOT/abc-result.json already exists — refusing to overwrite"
  cp "$Q3_ENV_DIR/result_p21.json" "$EV_ROOT/abc-result.json" \
    || die "p21abc: canonical abc-result archive failed"
}

phase_postfacts() {
  # READONLY from here. First the owner-aware quiesce adjudication
  # (exact expected-def UUID set root-pinned pre-tail; blocked-POST
  # proofs consumed ONLY from the SAVED pre-ABC artifact the tail
  # scenarios produced — a POST now would be a write). Then the
  # Archive-track collectors/verifier (frozen CLI).
  [ -f "$HERE/quiesce-expected-defs.json" ] \
    || die "quiesce-expected-defs.json missing — root pins the exact P25-owned def UUID set at the pre-tail review"
  local decl_at; decl_at="$(python3 -c \
    'import json,sys;print(json.load(open(sys.argv[1])).get("declared_at",""))' \
    "$EV_ROOT/tail-declaration.json")"
  run_step quiesce-collect python3 -I \
    "$HERE/check_owned_quiesced_p25.py" collect \
    --env-dir "$Q3_ENV_DIR" \
    --expected-defs "$HERE/quiesce-expected-defs.json" \
    --declared-at "$decl_at" --out "$EV_ROOT/quiesce-state.json" \
    || die "quiesce collect failed"
  run_step quiesce-check python3 -I \
    "$HERE/check_owned_quiesced_p25.py" check \
    --state "$EV_ROOT/quiesce-state.json" \
    --owner-roster "$EV_ROOT/tail-declaration.json" \
    --expected-defs "$HERE/quiesce-expected-defs.json" \
    --blocked-proof "$EV_ROOT/tail/blocked-proof.json" \
    --out "$EV_ROOT/quiesce-verdict.json" \
    || die "quiesce check failed"
  # Archive collectors consume the SELECTED aux-down attempt dir
  local auxdir; auxdir="$(latest_stage_dir aux-down)"
  [ -n "$auxdir" ] && [ -d "$auxdir" ] \
    || die "postfacts: no aux-down attempt dir (quiet raws source)"
  run_step postfacts-collect python3 -I "$HERE/collect_postfacts_p25.py" \
    --collect --env-dir "$Q3_ENV_DIR" \
    --out-dir "$EV_ROOT/postfacts-attempts" \
    --aux-quiet-dir "$auxdir" \
    --abc-result "$EV_ROOT/abc-result.json" \
    || die "postfacts collect failed"
  local att; att="$(latest_attempt_dir "$EV_ROOT/postfacts-attempts")"
  [ -n "$att" ] && [ -d "$att" ] || die "postfacts: no attempt dir"
  run_step postfacts python3 -I "$HERE/verify_postfacts_p25.py" \
    --attempt-dir "$att" \
    --tail-declaration "$EV_ROOT/tail-declaration.json" \
    --out "$EV_ROOT/postfacts-verdict.json" \
    || die "postfacts verify failed"
  cp "$EV_ROOT/postfacts-verdict.json" \
    "$LAST_STEP_DIR/postfacts-verdict.json" \
    || die "postfacts verdict archive failed"
}

phase_persist() {
  run_step persist python3 -I "$HERE/collect_persist_state_p25.py" \
    --collect --env-dir "$Q3_ENV_DIR" \
    --out-dir "$EV_ROOT/persist-attempts" \
    || die "persist collect failed"
}

phase_source_freeze() {
  local cand; cand="$(bindings_candidate_sha)" \
    || die "pg-candidate-bindings-r17.json missing/drifted"
  run_step freeze-candidate python3 -I \
    "$HERE/candidate_source_gate_p25.py" \
    --repo "$REPO" --expect "$cand" \
    || die "source freeze: tree drifted from reviewed candidate"
  run_step freeze-delta python3 -I "$HERE/source_hash_p25.py" delta \
    --repo "$REPO" --baseline "$HERE/source-p24-baseline.txt" \
    --out "$EV_ROOT/14-source-delta-final.json" \
    || die "source freeze delta: REMOVED anomaly"
  run_step freeze-delta-equal python3 -I -c "
import json, sys
from pathlib import Path
a = json.loads(Path('$EV_ROOT/01-source-delta.json').read_text())
b = json.loads(Path('$EV_ROOT/14-source-delta-final.json').read_text())
same = a == b
print(f'DELTA_STABLE={\"OK\" if same else \"DRIFT\"}')
print(json.dumps({'fingerprints': a, 'final': b}) if not same else '')
sys.exit(0 if same else 1)" || die "source delta drifted during the run"
  run_step source-freeze python3 -I "$HERE/source_hash_p25.py" emit \
    "$REPO" "$EV_ROOT/14-source-hash-post.txt" \
    || die "source emit failed"
  stage_verdict "$LAST_STEP_DIR/source-freeze-verdict.json" \
    "{\"ok\": true, \"all_ok\": true, \"candidate_sha256\": \"$cand\", \"delta_stable\": true}"
}

phase_standalone() {
  write_selected_index_tool
  local stages_tmp="$EV_ROOT/gen/canonical-stages.txt"
  printf '%s\n' "${CANONICAL_STAGES[@]}" > "$stages_tmp"
  run_step selected-index-pre-standalone python3 -I "$EV_ROOT/gen/selected_index.py" \
    "$EV_ROOT" "$stages_tmp" || die "pre-standalone selected-index failed"
  # Archive-track bundle (frozen CLI §6): tools + frozen deps + the
  # offline verifier, self-contained + independently verified.
  run_step standalone python3 -I "$HERE/standalone_p25.py" \
    bundle --src-ev "$EV_ROOT" --tools-dir "$HERE" \
    --bundle-dir "$EV_ROOT-standalone" \
    || die "standalone bundle failed"
}

write_selected_index_tool() {
  mkdir -p "$EV_ROOT/gen"
  [ -f "$EV_ROOT/gen/selected_index.py" ] && return 0
  cat > "$EV_ROOT/gen/selected_index.py" <<'PY'
import json, sys
from pathlib import Path
(ev, stages_file) = sys.argv[1], Path(sys.argv[2])
stages = [l.strip() for l in stages_file.read_text().splitlines()
          if l.strip()]
ev = Path(ev)
steps = ev / "steps"


def attempts_of(stage):
    # run_step layout: steps/<stage>-attempt<N> (stage names contain
    # hyphens themselves; match the -attempt suffix exactly)
    out = []
    prefix = f"{stage}-attempt"
    if steps.is_dir():
        for p in steps.iterdir():
            if p.is_dir() and p.name.startswith(prefix) \
                    and p.name[len(prefix):].isdigit():
                out.append((int(p.name[len(prefix):]), p))
    return sorted(out)


sel = {}
for stage in stages:
    cands = attempts_of(stage)
    if cands:
        n, p = cands[-1]
        sel[stage] = {"attempt_dir": str(p.relative_to(ev)),
                      "attempt": n}
    else:
        sel[stage] = {"attempt_dir": f"steps/{stage}-attempt-MISSING",
                      "note": "stage not executed yet — finalize "
                              "refuses (honest)"}
out = {"schema": "p25-selected-index-v1", "selections": sel}
(ev / "selected-index.json").write_text(
    json.dumps(out, indent=1, sort_keys=True) + "\n")
missing = [s for s in stages if "MISSING" in sel[s]["attempt_dir"]]
print("SELECTED_INDEX_STAGES=" + str(len(sel)))
print("SELECTED_INDEX_MISSING=" + (",".join(missing) or "-"))
PY
}

phase_finalize() {
  # ARCHIVE-INTERFACE §7 (frozen): selected-index (driver-written) →
  # prepare (EV still open; canonical 22 enforced) → seal (BARE, all
  # output OUTSIDE the EV) → reverify (bare, MUST rc0).
  write_selected_index_tool
  local stages_tmp="$EV_ROOT/gen/canonical-stages.txt"
  printf '%s\n' "${CANONICAL_STAGES[@]}" > "$stages_tmp"
  run_step selected-index python3 -I "$EV_ROOT/gen/selected_index.py" \
    "$EV_ROOT" "$stages_tmp" \
    || die "selected-index generation failed"
  local meta="$EV_ROOT-seal-meta"
  mkdir -p "$meta"
  local n=0
  while [ -e "$meta/prepare-stdout-r$((n+1)).log" ] \
     || [ -e "$meta/seal-stdout-r$((n+1)).log" ]; do n=$((n+1)); done
  local k=$((n+1))
  # Prepare locks the EV inputs. Its own output/rc must already be
  # outside EV; run_step would mutate stdout/rc/manifest after locking.
  python3 -I "$HERE/finalize_p25.py" prepare --ev "$EV_ROOT" \
    --tools-dir "$HERE" > "$meta/prepare-stdout-r$k.log" \
    2> "$meta/prepare-stderr-r$k.log"
  local prc=$?
  echo "$prc" > "$meta/prepare-r$k.rc"
  [ $prc -eq 0 ] || die "finalize prepare failed (external attempt $k)"
  python3 -I "$HERE/finalize_p25.py" seal --ev "$EV_ROOT" \
    --meta-dir "$meta" \
    > "$meta/seal-stdout-r$k.log" 2> "$meta/seal-stderr-r$k.log"
  local src=$?
  echo "$src" > "$meta/seal-r$k.rc"
  log "seal rc=$src (attempt $k; stdout/rc outside the sealed EV)"
  [ $src -eq 0 ] || die "seal failed (rc=$src) — evidence preserved"
  python3 -I "$HERE/finalize_p25.py" reverify --ev "$EV_ROOT" \
    --meta-dir "$meta" \
    > "$meta/reverify-stdout-r$k.log" 2> "$meta/reverify-stderr-r$k.log"
  local rrc=$?
  echo "$rrc" > "$meta/reverify-r$k.rc"
  log "reverify rc=$rrc"
  [ $rrc -eq 0 ] || die "reverify failed (rc=$rrc) — post-seal EV write suspected"
  # The canonical standalone stage was pre-seal capability proof.
  # Deliver the complete sealed EV only now; all receipts remain external.
  python3 -I "$HERE/finalize_p25.py" package-final --ev "$EV_ROOT" \
    --tools-dir "$HERE" --meta-dir "$meta" \
    > "$meta/package-final-stdout-r$k.log" \
    2> "$meta/package-final-stderr-r$k.log"
  local frc=$?
  echo "$frc" > "$meta/package-final-r$k.rc"
  [ $frc -eq 0 ] || die "final complete sealed-EV package failed (rc=$frc)"
}

full_run() {
  # LOCKED ORDER end-to-end: every phase in sequence; a phase failure
  # aborts (its attempt evidence is already archived). Import phases
  # bind the accepted greens; live phases run the real commands.
  for ph in "${PHASES[@]}"; do
    if [ "$ph" = "p21abc" ]; then
      log "full: ABC is the FINAL q3 write — readonly phases follow"
    fi
    "$0" "$ph"
    rc=$?
    [ $rc -eq 0 ] || die "full aborted at phase $ph (rc=$rc)"
  done
  log "full: complete"
}

case "${1:-}" in
  full)
    [ "$CONFIRM" = "yes" ] && [ "$RUNTIME" = "yes" ] \
      || die "full requires P25_CONFIRM=yes and RELEASED_RUNTIME=yes"
    mkdir -p "$EV_ROOT"
    full_run
    exit 0 ;;
  list)
    printf '%s\n' "${PHASES[@]}"
    exit 0 ;;
  dryrun)
    log "locked order (dryrun; CONFIRM=$CONFIRM RUNTIME=$RUNTIME):"
    printf '  %s\n' "${PHASES[@]}"
    log "canonical selected-index stages:"
    printf '  %s\n' "${CANONICAL_STAGES[@]}"
    log "readonly-after: p21abc (inclusive); tail roster is" \
        "declared (hash-frozen) BEFORE tail; ABC is the FINAL q3 write"
    exit 0 ;;
esac

PH="$1"
valid=""
for p in "${PHASES[@]}"; do [ "$p" = "$PH" ] && valid=1; done
case "$PH" in baseline_pre|regressions_pre_p24|regressions_p24|pre_tail_old6|tail_declare) valid=1 ;; esac
[ -n "$valid" ] || die "unknown phase '$PH' (see: list)"

if [ "$CONFIRM" != "yes" ]; then
  log "phase '$PH' NOT confirmed (set P25_CONFIRM=yes)"
  exit 3
fi
# pure-evidence phases/paths run on files only; live steps need the
# release (swap/preflight enforce this INSIDE the phase after their
# pure gates so the auditable gate path always runs)
case "$PH" in
  fingerprints|unit|pg_isolated|build|source_freeze|standalone|finalize|tail_declare) : ;;
  preflight) if [ "${P25_PREFLIGHT_MODE:-import}" != "import" ]; then
               [ "$RUNTIME" = "yes" ] || { phase_refuse_runtime "$PH"; exit 3; }
             fi ;;
  swap) : ;;
  *) [ "$RUNTIME" = "yes" ] || { phase_refuse_runtime "$PH"; exit 3; } ;;
esac

mkdir -p "$EV_ROOT"
"phase_$PH"
rc=$?
log "phase $PH rc=$rc"
exit $rc
