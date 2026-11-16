#!/bin/bash
# P2.4 FORMAL acceptance orchestrator — COMPLETE DRIVER (2026-11-14).
# Prepared per handoffs/2026-11-24-p24/formal-execution-contract.md:
# NO skeleton PASS remains; every phase runs real steps or refuses
# honestly on missing external inputs (PG runner owned by PG GLM,
# adapter/cohort-gate release flags owned by root).
#
# Release gates (all must be true before ANY mutation phase runs):
#   P24_CONFIRM=yes                          (this driver's interlock)
#   p23_q_guard_adapter_p24.RELEASED…=True   (root flips after review)
#   cohort-approved.json + .sha256 in $EV    (only for the ABC-last
#       cohort sequence; absent ⇒ documented fallback ordering)
#   smoke_p24.py --confirm-http              (HTTP release marker)
#
# Discipline inherited verbatim from the audited P23 formal runner:
# real rc via direct redirection (no tee), collision-safe -rN artifacts,
# FAILED/partial attempts preserved verbatim, manifest.jsonl records
# {step,rc,seconds,cmd,log,dir}, fixture NEVER restarted, PG/file/cache
# volumes + .env(600) untouched, controlled rows never cancelled or
# rewritten, hash manifest LAST. No `cp || true`, no stale globs.
set -uo pipefail

ROOT="/Volumes/ExternalSD/projects/OpenFoundry"
P1="$ROOT/tools/smoke/business-platform-p1"
P21="$ROOT/tools/smoke/business-platform-p2-1"
P22="$ROOT/tools/smoke/business-platform-p2-2"
P23="$ROOT/tools/smoke/business-platform-p2-3"
P24="$ROOT/tools/smoke/business-platform-p2-4"
TMP="${OF_P1_TMP:-/private/tmp/openfoundry-platform-p1-q3v9}"
PROJ="openfoundry-platform-p1-q3v9"
EV="$P24/results/$(date +%F)-p24-final"
MF="$EV/manifest.jsonl"
PG_RUNNER="$P24/run_pg_isolated_p24.sh"     # PG GLM owns this file
COHORT_REC="$EV/cohort-approved.json"
COHORT_SHA="$EV/cohort-approved.sha256"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
mkdir -p "$EV"
cd "$ROOT"

compose() { docker compose -f "$TMP/compose.yaml" --project-name "$PROJ" "$@"; }
compose3() {  # base + frozen P22 aux override (+ optional P24 override)
  local ovr="$1"; shift
  local fs=(-f "$TMP/compose.yaml" -f "$TMP/compose-p22-connector2.yaml")
  [ -n "$ovr" ] && fs+=(-f "$ovr")
  docker compose "${fs[@]}" --project-name "$PROJ" "$@"
}

require_confirm() {
  if [ "${P24_CONFIRM:-}" != "yes" ]; then
    echo "[p24-formal] refusing mutation phase '$1' without P24_CONFIRM=yes" >&2
    exit 1
  fi
}
require_contract() {
  [ -f "$P24/contract-pinned.json" ] && [ -f "$P24/implementation-contract.md" ] || {
    echo "[p24-formal] '$1' refused: contract metadata missing" >&2; exit 3; }
}

step() { # step <name> <cmd...> — real rc, collision-safe artifacts
  local name="$1"; shift
  local log="$EV/$name.log" dir="$EV/$name" n=0
  while [ -e "$log" ]; do n=$((n + 1)); log="$EV/$name-r$n.log"; dir="$EV/$name-r$n"; done
  mkdir -p "$dir"
  local t0 rc dur
  t0=$(date +%s)
  echo "[p24-formal] START $name: $*" | tee -a "$EV/console.log" >&2
  ( "$@" ) >"$log" 2>&1 < /dev/null
  rc=$?
  dur=$(( $(date +%s) - t0 ))
  printf '{"step":"%s","rc":%d,"seconds":%d,"cmd":%s,"log":"%s","dir":"%s/"}\n' \
    "$name" "$rc" "$dur" \
    "$(printf '%s' "$*" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')" \
    "${log#$EV/}" "${dir#$EV/}" >> "$MF"
  echo "[p24-formal] END $name rc=$rc ${dur}s log=${log#$EV/}" | tee -a "$EV/console.log" >&2
  LAST_STEP_LOG="$log"; LAST_STEP_DIR="$dir"
  return $rc
}

capture_var() { sed -n "s/^$1=//p" "$LAST_STEP_LOG" | tail -1; }

# ---------------------------------------------------------------- phases --
phase_preflight() {
  step 00-preflight-collect python3 -I "$P24/collect_preflight_p24.py" "$TMP" || return
  step 00-preflight-verify python3 -I "$P24/verify_preflight_p24.py"
}

phase_baseline() {
  step 00-baseline python3 -I "$P24/readonly_baseline_p24.py" --env-dir "$TMP" || return
  local d; d=$(capture_var BASELINE_DIR)
  [ -n "$d" ] && [ -f "$d/manifest.json" ] || { echo "[p24-formal] baseline: no BASELINE_DIR" >&2; return 3; }
  printf '%s\n' "$d" > "$EV/00-baseline-path.txt"
  cp "$d/manifest.json" "$EV/00-baseline-manifest.json"
}

phase_fingerprints() {
  # candidate source CONTENT hash BEFORE build (driver review fix:
  # candidate == final must be provable by content, not git status)
  step 01-source-hash-pre python3 -I "$P24/source_hash_p24.py" \
    "$ROOT" "$EV/01-source-hash-pre.txt" || return
  sed -n 's/^SOURCE_HASH=//p' "$LAST_STEP_LOG" | tail -1 \
    > "$EV/01-source-hash-pre.sha"
  [ -s "$EV/01-source-hash-pre.sha" ] || {
    echo "[p24-formal] source-hash-pre: SOURCE_HASH not captured" >&2
    return 3; }
  local fps="$EV/01-fingerprints.py"
  cat > "$fps" <<'PY'
import hashlib, json, subprocess, sys
from pathlib import Path
ev, root = Path(sys.argv[1]), Path(sys.argv[2])
p24 = root / "tools/smoke/business-platform-p2-4"
files = ["implementation-contract.md", "contract-pinned.json",
         "p24_live_guard.py", "p24_pins.py", "p23_q_guard_adapter_p24.py",
         "cohort_gate_p24.py", "verify_postfacts_p24.py",
         "p24-required-names.txt", "run_tests_p24.sh",
         "pg_exact_check_p24.py", "smoke_p24.py"]
out = {"client_date": "2026-11-14",
       "git_head": subprocess.run(["git", "-C", str(root), "rev-parse",
                                   "HEAD"], capture_output=True,
                                  text=True).stdout.strip(),
       "branch": subprocess.run(["git", "-C", str(root), "rev-parse",
                                 "--abbrev-ref", "HEAD"],
                                capture_output=True,
                                text=True).stdout.strip(),
       "files": {f: hashlib.sha256((p24 / f).read_bytes()).hexdigest()
                 for f in files}}
path = ev / "01-fingerprints.json"
n = 0
while path.exists():
    n += 1
    path = ev / f"01-fingerprints-r{n}.json"
path.write_text(json.dumps(out, indent=1) + "\n")
print("FINGERPRINTS=" + path.name)
PY
  step 01-fingerprints python3 -I "$fps" "$EV" "$ROOT"
}

pg_runner_step() { # <step-name> <mode…>: refuses until PG GLM lands it
  local name="$1"; shift
  [ -f "$PG_RUNNER" ] || {
    echo "[p24-formal] $name refused: $PG_RUNNER not landed (PG GLM owns it)" >&2
    return 3; }
  step "$name" sh "$PG_RUNNER" "$@" || return
  local lg rc_
  lg=$(capture_var PG_LOG); rc_=$(capture_var PG_RC_FILE)
  [ -n "$lg" ] && [ -f "$lg" ] && [ -n "$rc_" ] && [ -f "$rc_" ] || {
    echo "[p24-formal] $name: PG_LOG/PG_RC_FILE not captured — refusing" >&2
    return 3; }
  printf '%s\n%s\n' "$lg" "$rc_" > "$EV/${name}-paths.txt"
}

pg_exact_new() { # <step-name> <paths-file>: new-names-only exact check
  local name="$1" paths="$2" lg rc_
  lg=$(sed -n 1p "$paths"); rc_=$(sed -n 2p "$paths")
  step "$name" python3 -I "$P24/pg_exact_check_p24.py" --new-only \
    "$lg" "$rc_" \
    "$P1/run_tests.sh" "$P21/run_tests_p21.sh" \
    "$P22/p22-required-names.txt" "$P23/p23-required-names.txt" \
    "$P24/p24-required-names.txt"
}

phase_pg_import() {
  # BIND the already-green isolated-PG evidence (no identical-content
  # rerun). Verifies EVERY artifact object carrying path+sha256 in the
  # binding (16 today: script/test/names + per-round raw/rc/meta + the
  # tools source per-file manifest), re-checks go versions + original
  # rcs + no source drift, recomputes the tools manifest ACTUAL hash,
  # parses the optional-5 SKIP names from the unified RAW, and runs the
  # real exact checker over the imported logs. Output is per-attempt
  # (collision-safe) and its path is printed for attempt-bound binding.
  local B="${P24_PG_BINDING:-/private/tmp/openfoundry-p24-repair-evidence/pg/PG_FINAL_BINDING.json}"
  [ -f "$B" ] || { echo "[p24-formal] pg-import: binding missing: $B" >&2; return 3; }
  require_confirm pg-import
  local pgimp="$EV/05-pg-import.py"
  cat > "$pgimp" <<'PY'
import hashlib, json, re, subprocess, sys
from pathlib import Path
(B, checker, pre_sha_file, out_json) = map(Path, sys.argv[1:5])
b = json.loads(B.read_text())
probs, checked = [], []
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def walk_arts(o, where=""):
    if isinstance(o, dict):
        if isinstance(o.get("path"), str) and isinstance(o.get("sha256"), str) \
                and re.fullmatch(r"[0-9a-f]{64}", o["sha256"]):
            p = Path(o["path"])
            ok = p.is_file() and sha(p) == o["sha256"]
            if o.get("bytes") is not None and p.is_file() \
                    and p.stat().st_size != o["bytes"]:
                ok = False
            checked.append(where)
            if not ok:
                probs.append(f"{where}: artifact missing/sha/bytes drift")
        for k, v in o.items():
            walk_arts(v, f"{where}/{k}")
    elif isinstance(o, list):
        for i2, v in enumerate(o):
            walk_arts(v, f"{where}[{i2}]")
walk_arts(b.get("artifacts"), "artifacts")
for mode in ("new-normal", "new-race", "unified"):
    walk_arts(b["final_rounds"][mode], f"final_rounds/{mode}")
# the tools per-file source manifest is the 16th verified artifact
walk_arts((b.get("source_hash_tools_algorithm") or {})
          .get("per_file_manifest"), "source_hash_tools_algorithm")
rounds_ok = {}
for mode, want_go in (("new-normal", "go1.26.3"),
                      ("new-race", "go1.25.14"),
                      ("unified", "go1.26.3")):
    r = b["final_rounds"][mode]
    ok = not [x for x in probs if x.startswith(f"final_rounds/{mode}")]
    if str(r.get("original_rc")) != "0":
        probs.append(f"{mode}: original_rc != 0"); ok = False
    if not str(r.get("go_version", "")).startswith(want_go):
        probs.append(f"{mode}: go version {r.get('go_version')}"); ok = False
    if r.get("source_drift_during_run"):
        probs.append(f"{mode}: source drift during run"); ok = False
    if r.get("source_content_sha256_pgshell_before") != \
            r.get("source_content_sha256_pgshell_after"):
        probs.append(f"{mode}: pgshell before/after differ"); ok = False
    rounds_ok[mode] = ok
# tools source manifest: ACTUAL hash must equal its recorded sha AND the
# driver's pre-freeze anchor
t_alg = b.get("source_hash_tools_algorithm") or {}
pm = t_alg.get("per_file_manifest") or {}
pre_sha = Path(pre_sha_file).read_text().strip() \
    if Path(pre_sha_file).is_file() else ""
if not (pm.get("path") and Path(pm["path"]).is_file()
        and sha(Path(pm["path"])) == pm.get("sha256")
        == t_alg.get("combined_sha256") == pre_sha):
    probs.append("tools source manifest actual hash != recorded/pre "
                 f"({pre_sha[:12]}…)")
# optional-5 SKIP names parsed from the UNIFIED RAW (not the prose field)
uraw = Path(b["final_rounds"]["unified"]["raw"]["path"])
skips_raw = sorted(set(re.findall(r"^--- SKIP:\s+(\S+)", 
                        uraw.read_text(errors="replace"), re.M)))
skips_field = b["final_rounds"]["unified"].get(
    "optional_skip_unverified") or []
if skips_raw != sorted(skips_field):
    probs.append(f"optional SKIPs raw={skips_raw} != field={skips_field}")
# real exact checker over the IMPORTED logs (never a fake rerun)
for mode, extra in (("new-normal", ["--new-only"]),
                    ("new-race", ["--new-only"]), ("unified", [])):
    r = b["final_rounds"][mode]
    cmd = ["python3", "-I", str(checker)] + extra + [
        r["raw"]["path"], r["rc"]["path"], sys.argv[5], sys.argv[6],
        sys.argv[7], sys.argv[8], sys.argv[9]]
    pr = subprocess.run(cmd, capture_output=True, text=True)
    if pr.returncode != 0:
        probs.append(f"{mode}: exact checker rc={pr.returncode}: "
                     f"{pr.stdout[-140:]}")
verdict = {"binding": str(B), "binding_sha256": sha(B),
           "artifacts_checked": checked, "artifacts_count": len(checked),
           "rounds": rounds_ok,
           "tools_manifest_actual_ok": not any(
               "tools source manifest" in x for x in probs),
           "optional_skip_unverified": skips_raw,
           "failures_preserved": b.get("failures_preserved"),
           "probs": probs, "ok": not probs and len(checked) >= 16}
out_json.write_text(json.dumps(verdict, indent=1) + "\n")
print(f"PG_IMPORT_JSON={out_json}")
print(f"[pg-import] artifacts={len(checked)} rounds={rounds_ok} "
      f"probs={probs[:4]}")
raise SystemExit(0 if verdict["ok"] else 1)
PY
  local imp_out="$EV/05-pg-import-$(date +%Y%m%dT%H%M%S).json"
  step 05-pg-import python3 -I "$pgimp" "$B" "$P24/pg_exact_check_p24.py" \
    "$EV/01-source-hash-pre.sha" "$imp_out" \
    "$P1/run_tests.sh" "$P21/run_tests_p21.sh" \
    "$P22/p22-required-names.txt" "$P23/p23-required-names.txt" \
    "$P24/p24-required-names.txt"
}

phase_pg_evidence() {  # bind green PG evidence if present; else execute
  local B="${P24_PG_BINDING:-/private/tmp/openfoundry-p24-repair-evidence/pg/PG_FINAL_BINDING.json}"
  if [ -f "$B" ]; then
    phase_pg_import
  else
    phase_pg_new && phase_pg_unified
  fi
}

phase_unit() {
  # frozen P1 unit runner in its independent Go test container (compose
  # service unit-tests, source bind-mounted READ-ONLY; no postgres) —
  # REAL log + rc via pipefail. PG can never substitute for unit.
  step 04-unit bash -o pipefail "$P1/run.sh" tests; local rc=$?
  local ok fail skips
  ok=$(grep -c '^ok ' "$LAST_STEP_LOG" || true)
  fail=$(grep -c '^FAIL' "$LAST_STEP_LOG" || true)
  skips=$(grep -c '^--- SKIP' "$LAST_STEP_LOG" || true)
  printf '{"step":"04-unit-summary","ok_packages":%s,"FAIL_lines":%s,"SKIP_lines":%s}\n' \
    "$ok" "$fail" "$skips" >> "$MF"
  return $rc
}

phase_pg_new() {
  # PG runner modes are new-normal / new-race (review: no bare "new"):
  # two separate steps, each with its own real rc + version gate (the
  # runner returns 2 on toolchain mismatch) + exact new-names check on
  # ITS OWN log (never -count=2 in an exact-one log).
  pg_runner_step 05-pg-new-normal new-normal || return
  pg_exact_new 05-pg-new-normal-exact "$EV/05-pg-new-normal-paths.txt" || return
  pg_runner_step 05-pg-new-race new-race || return
  pg_exact_new 05-pg-new-race-exact "$EV/05-pg-new-race-paths.txt"
}
phase_pg_unified() {
  pg_runner_step 05-pg-unified unified || return
  local paths lg rc_
  paths=$(cat "$EV/05-pg-unified-paths.txt")
  lg=$(printf '%s\n' "$paths" | sed -n 1p)
  rc_=$(printf '%s\n' "$paths" | sed -n 2p)
  step 05-pg-exact python3 -I "$P24/pg_exact_check_p24.py" \
    "$lg" "$rc_" \
    "$P1/run_tests.sh" "$P21/run_tests_p21.sh" \
    "$P22/p22-required-names.txt" "$P23/p23-required-names.txt" \
    "$P24/p24-required-names.txt"
}

phase_build() {
  require_confirm build-final
  require_contract build-final
  local bdir="$EV/01-build-final"; local n=0
  while [ -e "$bdir" ]; do n=$((n+1)); bdir="$EV/01-build-final-r$n"; done
  mkdir -p "$bdir"
  local rc=0
  { echo "## cmd: compose build dataset pipeline connector"
    compose build dataset pipeline connector 2>&1; echo "rc=$?"; } > "$bdir/build-base3.log"
  [ "$(sed -n 's/^rc=//p' "$bdir/build-base3.log" | tail -1)" = "0" ] || rc=1
  bash "$P22/p22_connector2.sh" render || { echo "[p24-formal] render failed" >&2; printf '{"step":"01-build-final","rc":6}\n' >> "$MF"; return 6; }
  { echo "## cmd: compose base+override build connector2 connector3"
    compose3 "" build connector2 connector3 2>&1; echo "rc=$?"; } > "$bdir/build-aux2.log"
  [ "$(sed -n 's/^rc=//p' "$bdir/build-aux2.log" | tail -1)" = "0" ] || rc=1
  docker images --format '{{.Repository}} {{.ID}}' | grep "^$PROJ-" > "$bdir/docker-images.txt"
  python3 - "$bdir" <<'PY'
import json, sys
out = sys.argv[1]
repos = {"dataset", "pipeline", "connector", "connector2", "connector3"}
ids = {}
for line in open(f"{out}/docker-images.txt"):
    repo, iid = line.split()
    svc = repo.replace("openfoundry-platform-p1-q3v9-", "")
    if svc in repos: ids[svc] = iid
missing = sorted(repos - set(ids))
open(f"{out}/image-ids.json", "w").write(json.dumps(ids, indent=2) + "\n")
print("[p24-build] image ids:", ids, "| missing:", missing)
raise SystemExit(1 if missing else 0)
PY
  local brc=$?
  printf '{"step":"01-build-final","rc":%d}\n' "$(( rc | brc ))" >> "$MF"
  git -C "$ROOT" rev-parse HEAD > "$bdir/git-head.txt"
  return $(( rc | brc ))
}

phase_swap() {
  require_confirm swap
  # ---- SOURCE-PINNED HARD GATE (root): BEFORE guard/stop --------------
  # fresh 373-file content manifest must equal BOTH the reviewed
  # 01-source-hash-pre.sha AND the PG binding tools SHA (read from the
  # binding file, never a baked literal); image-ids pinned to THE rc0
  # 01-build-final manifest attempt (recorded dir+sha, no ls -t glob).
  # Any failure aborts BEFORE anything is stopped.
  local gate="$EV/01b-swap-source-gate.py"
  cat > "$gate" <<'PY'
import glob, hashlib, json, re, subprocess, sys
from pathlib import Path
(repo, manifest_out, pre_sha_path, binding_path, ev) = map(Path, sys.argv[1:6])
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
probs = []
r = subprocess.run(["python3", "-I",
                    str(Path(__file__).resolve().parents[2] / "source_hash_p24.py"),
                    str(repo), str(manifest_out)],
                   capture_output=True, text=True)
if r.returncode != 0:
    print("[swap-gate] BLOCK: source hash run failed", file=sys.stderr)
    sys.exit(1)
m = re.search(r"^SOURCE_HASH=([0-9a-f]{64})$", r.stdout, re.M)
fm = re.search(r"^SOURCE_FILES=(\d+)$", r.stdout, re.M)
fresh = m.group(1) if m else ""
pre = pre_sha_path.read_text().strip() if pre_sha_path.is_file() else ""
try:
    binding = json.loads(binding_path.read_text())
except Exception:
    binding = {}
combined = (binding.get("source_hash_tools_algorithm") or {}
            ).get("combined_sha256", "")
if not (fresh and fresh == pre == combined):
    probs.append(f"source fresh={fresh[:12]}… pre={pre[:12]}… "
                 f"binding={combined[:12]}…")
if not (fm and int(fm.group(1)) == 373):
    probs.append(f"SOURCE_FILES={fm.group(1) if fm else '?'} != 373")
steps = []
for l in open(ev / "manifest.jsonl"):
    try:
        steps.append(json.loads(l))
    except ValueError:
        pass
blds = [s for s in steps if s.get("step") == "01-build-final"
        and s.get("rc") == 0]
bdir = None
if not blds:
    probs.append("no rc0 01-build-final attempt in manifest")
else:
    bdir = (blds[-1].get("dir") or "").strip("/")
    if not bdir:  # legacy printf line without dir: rc0 dir must be UNIQUE
        cands = []
        for d in sorted(glob.glob(str(ev / "01-build-final*"))):
            dp = Path(d)
            if not (dp / "image-ids.json").is_file():
                continue
            rcs = [next(iter(re.findall(r"^rc=(\d+)$",
                          lg.read_text(errors="replace"), re.M)), "x")
                   for lg in (dp / "build-base3.log",
                              dp / "build-aux2.log") if lg.is_file()]
            if rcs and all(x == "0" for x in rcs):
                cands.append(dp)
        if len(cands) == 1:
            bdir = cands[0].name
        else:
            probs.append(f"build dir not recorded and not unique: "
                         f"{[c.name for c in cands]}")
ids_path = ev / bdir / "image-ids.json" if bdir else None
ids = json.loads(ids_path.read_text()) if ids_path and ids_path.is_file() else {}
if not (ids_path and ids_path.is_file()
        and set(ids) == {"dataset", "pipeline", "connector",
                         "connector2", "connector3"}):
    probs.append("image-ids not the 5-service set from the pinned "
                 "rc0 build attempt")
verdict = {"fresh_source_sha256": fresh, "pre_sha256": pre,
           "binding_tools_sha256": combined,
           "pinned_build_attempt_dir": bdir,
           "pinned_image_ids": ids,
           "pinned_image_ids_path": str(ids_path) if ids_path else None,
           "pinned_image_ids_sha256": (sha(ids_path)
                                       if ids_path and ids_path.is_file()
                                       else None),
           "probs": probs, "ok": not probs}
(ev / "01b-swap-source-gate.json").write_text(
    json.dumps(verdict, indent=1) + "\n")
print("[swap-gate] fresh==pre==binding:",
      bool(fresh) and fresh == pre == combined, "| pinned:", bdir)
for p in probs:
    print(f"[swap-gate] BLOCK: {p}", file=sys.stderr)
sys.exit(0 if not probs else 1)
PY
  step 01b-swap-source-gate python3 -I "$gate" "$ROOT" \
    "$EV/01b-swap-source-gate-fresh.txt" "$EV/01-source-hash-pre.sha" \
    "/private/tmp/openfoundry-p24-repair-evidence/pg/PG_FINAL_BINDING.json" \
    "$EV" || return
  local bf_ids
  bf_ids="$EV/$(python3 -c "
import json
d = json.load(open('$EV/01b-swap-source-gate.json'))
print(((d.get('pinned_build_attempt_dir') or '') + '/image-ids.json'))")"
  [ -n "$bf_ids" ] && [ -f "$bf_ids" ] || {
    echo "[p24-formal] swap: pinned image-ids missing" >&2; return 3; }
  step 01b-swap-guard python3 -I "$P24/p24_live_guard.py" "$TMP" connector || return
  local present=""
  for svc in connector connector2 connector3; do
    [ -n "$(compose3 "" ps -a -q "$svc" 2>/dev/null | head -1)" ] && present="$present $svc"
  done
  [ -n "$present" ] || { echo "[p24-formal] swap: no connector replica present" >&2; return 4; }
  # shellcheck disable=SC2086
  step 01b-swap-stop compose3 "" stop -t 120 $present || return
  local exits="$LAST_STEP_DIR/exits.txt"; : > "$exits"; local n_present=0
  for svc in connector connector2 connector3; do
    local cid; cid=$(compose3 "" ps -a -q "$svc" | head -1)
    if [ -z "$cid" ]; then echo "$svc absent(aux-down-safe-state)" >> "$exits"; continue; fi
    docker inspect --format "$svc {{.State.Running}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.FinishedAt}}" "$cid" >> "$exits" || return 4
    n_present=$((n_present + 1))
  done
  for svc in connector connector2 connector3; do
    case " $present " in *" $svc "*) ;; *) echo "$svc absent(aux-down-safe-state)" >> "$exits";; esac
  done
  local graceful; graceful=$(grep -c "|0|false|" "$exits" || true)
  [ "$graceful" -eq "$n_present" ] && [ "$n_present" -ge 1 ] || {
    echo "[p24-formal] swap: non-graceful old exit (graceful=$graceful present=$n_present)" >&2; cat "$exits" >&2; return 5; }
  step 01b-swap-up-base compose up -d --no-deps dataset pipeline connector || return
  # swap_verify needs the candidate aux replicas RUNNING — bring the
  # frozen P22 aux/proxy up EXPLICITLY here (old aux was removed after
  # P23), with the P22 interlock passed in the command env, never
  # relying on an inherited shell variable
  step 07-aux-up-swap env P22_CONFIRM_CONNECTOR2=yes sh "$P22/p22_connector2.sh" up || return
  step 01b-swap-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only || return
  step 01b-swap-verify python3 -I "$P22/swap_verify.py" "$TMP" "$bf_ids"
}

phase_p1pre9() {
  step 06-p1-pre9 bash -o pipefail "$P1/run.sh" smoke || return
  cp "$TMP/result.json" "$LAST_STEP_DIR/result.json"
}

phase_p23_adapted() {
  require_confirm p23-adapted
  step 09-p23-adapted-full python3 -I "$P24/p23_q_guard_adapter_p24.py" \
    --record-dir "$EV/09-p23-adapted" -- \
    --env-dir "$TMP" --allow-restart
  local rc=$?
  # archive the suffix result AND the P23 snapshot pair regardless of
  # the run's rc (failures archive first, never cp||true) — the attempt
  # dir binding keeps this run's artifacts separate from older greens
  if [ -f "$TMP/result_p23.json" ]; then
    cp "$TMP/result_p23.json" "$LAST_STEP_DIR/" || rc=$((rc|4))
    local sfx; sfx=$(python3 -c "import json;print(json.load(open('$TMP/result_p23.json'))['suffix'])" 2>/dev/null || true)
    if [ -n "$sfx" ]; then
      printf '%s\n' "$sfx" > "$LAST_STEP_DIR/snapshot-suffix.txt"
      mkdir -p "$LAST_STEP_DIR/snapshots_latest"
      cp -R "$TMP/snapshots_p23/$sfx" "$LAST_STEP_DIR/snapshots_latest/" 2>/dev/null \
        || { echo "[p24-formal] p23-adapted: snapshots_latest archive FAILED" >&2; rc=$((rc|4)); }
      cp -R "$TMP/snapshots_p23" "$LAST_STEP_DIR/snapshots_history" 2>/dev/null \
        || { echo "[p24-formal] p23-adapted: snapshots_history archive FAILED" >&2; rc=$((rc|4)); }
    fi
  else
    echo "[p24-formal] p23-adapted: no result_p23.json produced" >&2
    rc=$((rc|4))
  fi
  return $rc
}

phase_p22full() {
  require_confirm p22full
  # explicit P22 interlock in the command env (never an implicit shell
  # variable); the attempt result is archived BEFORE returning even on
  # step failure — failures archive first, never cp||true
  step 10-p22-full env P22_CONFIRM_CONNECTOR2=yes python3 -I "$P22/smoke_p22.py" --env-dir "$TMP"
  local rc=$?
  if [ -f "$TMP/result_p22.json" ]; then
    cp "$TMP/result_p22.json" "$LAST_STEP_DIR/" || rc=$((rc|4))
  else
    echo "[p24-formal] p22full: no result_p22.json produced" >&2
    rc=$((rc|4))
  fi
  [ $rc -ne 0 ] && return $rc
  local bf_ids; bf_ids=$(ls -t "$EV"/01-build-final*/image-ids.json 2>/dev/null | head -1)
  step 10c-post-p22-images python3 -I "$P22/swap_verify.py" "$TMP" "$bf_ids"
}

phase_scheduler_window() {
  require_confirm scheduler-window
  require_contract scheduler-window
  # ---- pre-window gates (review-final-evidence): fresh strict guard4;
  # five RUNNING services on the PINNED image IDs (incl dataset/pipeline
  # — swap_verify only covers the 3 Connectors); known-owned terminal
  # check whenever a PRIOR p24 attempt exists
  step 07-pre-window-guard4 python3 -I "$P24/p24_live_guard.py" "$TMP" connector || return
  step 07-pre-window-images python3 -I "$P24/check_running_images_p24.py" \
    --gate-json "$EV/01b-swap-source-gate.json" \
    --out "$EV/07-pre-window-images.json" || return
  if prior=$(latest_p24_result); then
    step 07-pre-window-quiesce python3 -I "$P24/check_owned_quiesced_p24.py" \
      --p24-result "$prior" --env-dir "$TMP" || return
  else
    echo "[p24-formal] pre-window: no prior p24 attempt — no known owned set yet" >&2
  fi
  # aux/proxy ensured up BEFORE any override exists (idempotent after
  # the swap-phase bring-up); explicit P22 interlock in the command env
  step 07-aux-up env P22_CONFIRM_CONNECTOR2=yes sh "$P22/p22_connector2.sh" up || return
  step 07a-p24-schedule-env sh "$P24/p24_schedule_env.sh" render || return
  # review bug 2: machine interface — the renderer prints
  # OVERRIDE_FILE=/OVERRIDE_INTERVAL= lines; never regex human prose
  local ovr interval
  ovr=$(capture_var OVERRIDE_FILE)
  interval=$(capture_var OVERRIDE_INTERVAL)
  [ -n "$ovr" ] && [ -f "$ovr" ] && [ -n "$interval" ] || {
    echo "[p24-formal] schedule-env: OVERRIDE_FILE/OVERRIDE_INTERVAL not captured — refusing" >&2
    return 3; }
  printf '%s\n' "$ovr" > "$EV/07-p24-override-path.txt"
  require_confirm schedule-env-apply
  step 07b-schedule-env-apply compose3 "$ovr" up -d --no-deps connector connector2 connector3 || return
  step 07c-schedule-env-verify sh -c '
    want="'"$interval"'"; bad=0
    for svc in connector connector2 connector3; do
      cid=$(docker compose -f "'"$TMP"'/compose.yaml" -f "'"$TMP"'/compose-p22-connector2.yaml" --project-name "'"$PROJ"'" ps -q "$svc" | head -1)
      got=$(docker inspect --format "{{range .Config.Env}}{{println .}}{{end}}" "$cid" 2>/dev/null | sed -n "s/^OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=//p")
      echo "'"$PROJ"'-$svc interval=$got (want=$want)"
      [ "$got" = "$want" ] || bad=1
    done
    exit $bad' || return
  # 07d: BOUNDED wait until ALL THREE connector /healthz answer 200 —
  # never let the scenarios race a recreating replica (root release)
  step 07d-healthz-wait sh -c '
    deadline=$(( $(date +%s) + 60 )); bad=1
    while [ $(date +%s) -lt $deadline ]; do
      ok=0
      for u in http://127.0.0.1:17088/healthz http://127.0.0.1:17089/healthz http://127.0.0.1:17087/healthz; do
        curl -fsS -m 3 "$u" >/dev/null 2>&1 && ok=$((ok+1))
      done
      echo "healthz ok=$ok/3"
      [ $ok -eq 3 ] && { bad=0; break; }
      sleep 2
    done
    exit $bad' || return
  # corrected P24 HTTP: scenario bodies + owned-cleanup quiet proof run
  # while loops are STILL ON; --with-restart-probe makes the G4 restart
  # probe REQUIRED for official acceptance (root: default-skip is
  # honest but insufficient for final acceptance)
  step 10c-p24-scenarios python3 -I "$P24/smoke_p24.py" --env-dir "$TMP" --confirm-http --official --with-restart-probe
  local rc=$?
  local rf; rf=$(capture_var RESULT_FILE)
  if [ -n "$rf" ] && [ -f "$rf" ]; then cp "$rf" "$LAST_STEP_DIR/" || rc=$((rc|1));
  else echo "[p24-formal] RESULT_FILE not captured" >&2; rc=$((rc|1)); fi
  [ $rc -ne 0 ] && return $rc
  # G4 probe sidecars: archive the EXACT files this attempt's result
  # references under /private/tmp (raw guard/stop/state/start outputs)
  # into THIS attempt's step dir with sha256 — result JSON alone with
  # /private/tmp paths is not a durable archive. Only referenced files
  # are copied (never the whole TMP, which holds env/PG volumes).
  local sidecar="$EV/10c-p24-sidecars.py"
  cat > "$sidecar" <<'PY'
import hashlib, json, re, sys
from pathlib import Path
(result_path, dest_dir) = sys.argv[1], Path(sys.argv[2])
dest_dir.mkdir(parents=True, exist_ok=True)
refs = []
def walk(o):
    if isinstance(o, dict):
        for v in o.values(): walk(v)
    elif isinstance(o, list):
        for v in o: walk(v)
    elif isinstance(o, str) and o.startswith("/private/tmp/") \
            and re.search(r"\.(log|out|txt|json|rc|err)$", o):
        refs.append(o)
walk(json.loads(Path(result_path).read_text()))
copied, missing = [], []
for src in sorted(set(refs)):
    sp = Path(src)
    if not sp.is_file():
        missing.append(src)
        continue
    name = sp.name
    n = 0
    while (dest_dir / "sidecars" / name).exists():
        n += 1
        name = re.sub(r"(\.\w+)$", rf"-{n}\1", sp.name)
    dstd = dest_dir / "sidecars" / name
    dstd.parent.mkdir(parents=True, exist_ok=True)
    dstd.write_bytes(sp.read_bytes())
    copied.append({"src": src, "dst": str(dstd),
                   "sha256": hashlib.sha256(sp.read_bytes()).hexdigest(),
                   "bytes": sp.stat().st_size})
manifest = {"result": str(result_path),
            "result_sha256": hashlib.sha256(
                Path(result_path).read_bytes()).hexdigest(),
            "referenced": sorted(set(refs)),
            "copied": copied, "missing": missing,
            "ok": bool(copied) and not missing}
mpath = dest_dir / "sidecars-manifest.json"
mpath.write_text(json.dumps(manifest, indent=1) + "\n")
print(f"SIDECARS_MANIFEST={mpath.resolve()}")
print(f"[sidecars] referenced={len(set(refs))} copied={len(copied)} "
      f"missing={len(missing)}")
for m_ in missing:
    print(f"[sidecars] BLOCK: referenced file missing: {m_}",
          file=sys.stderr)
sys.exit(0 if manifest["ok"] else 1)
PY
  step 10c-p24-sidecars python3 -I "$sidecar" "$rf" "$LAST_STEP_DIR" || return
  # collision-safe owned-history UNION across ALL 10c attempts (incl.
  # failures) — the final global-cron roster scope; PG collectors may
  # consume via --owned-history
  step 10c-owned-history python3 -I "$P24/owned_history_p24.py" \
    --ev "$EV" || return
  # loops STILL ON: independently confirm owned disable/revoke/terminal
  # (fresh SQL; smoke's own cleanup record already gates its verdict)
  local hist=""
  hist=$(latest_owned_history) || hist=""
  local qargs=(--p24-result "$rf" --env-dir "$TMP")
  [ -n "$hist" ] && [ -f "$hist" ] && qargs+=(--owned-history "$hist")
  step 10d-window-quiesce python3 -I "$P24/check_owned_quiesced_p24.py" \
    "${qargs[@]}" || return
  # loops still ON: strict guard4 must still hold (4 fixed rows untouched)
  step 10d-window-guard4 python3 -I "$P24/p24_live_guard.py" "$TMP" connector || return
  # restore loop0: recreate the three WITHOUT the P24 override
  require_confirm restore-loop0
  step 10e-restore-loop0 compose3 "" up -d --no-deps connector connector2 connector3 || return
  step 10g-restore-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only || return
  step 10f-loop0-verify sh -c '
    bad=0
    for svc in connector connector2 connector3; do
      cid=$(docker compose -f "'"$TMP"'/compose.yaml" -f "'"$TMP"'/compose-p22-connector2.yaml" --project-name "'"$PROJ"'" ps -q "$svc" | head -1)
      got=$(docker inspect --format "{{range .Config.Env}}{{println .}}{{end}}" "$cid" 2>/dev/null | sed -n "s/^OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=//p")
      echo "'"$PROJ"'-$svc interval=${got:-<unset>} (want unset)"
      [ -z "$got" ] || bad=1
    done
    exit $bad'
}

phase_abc_pre() {   # cohort path only: guard4 + fresh snapshot before ABC
  require_confirm abc-pre
  step 11-pre-abc-guard4 python3 -I "$P24/p24_live_guard.py" "$TMP" connector || return
  step 11-pre-abc-collect python3 -I "$P24/collect_preflight_p24.py" "$TMP" || return
  step 11-pre-abc-verify python3 -I "$P24/verify_preflight_p24.py"
}

phase_p21abc() {
  step 10b-p21-ABC python3 -I "$P21/smoke_p21.py" --env-dir "$TMP"
  local rc=$?
  # archive the attempt result BEFORE returning even on failure —
  # failures archive first, never cp||true
  if [ -f "$TMP/result_p21.json" ]; then
    cp "$TMP/result_p21.json" "$LAST_STEP_DIR/" || rc=$((rc|4))
  else
    echo "[p24-formal] p21abc: no result_p21.json produced" >&2
    rc=$((rc|4))
  fi
  return $rc
}

phase_cohort_gate() {
  [ -f "$COHORT_REC" ] && [ -f "$COHORT_SHA" ] || { echo "[p24-formal] cohort-gate: approved record missing" >&2; return 3; }
  require_confirm cohort-gate
  step 11b-cohort-gate python3 -I "$P24/cohort_gate_p24.py" \
    --record "$COHORT_REC" --record-sha256 "$(cat "$COHORT_SHA")" \
    --env-dir "$TMP"
}

latest_p24_result() {
  local d r
  d=$(python3 - "$EV" <<'PY'
import json, sys
from pathlib import Path
ev = Path(sys.argv[1])
best = None
for l in open(ev / "manifest.jsonl"):
    try: s = json.loads(l)
    except ValueError: continue
    if s.get("step") == "10c-p24-scenarios":
        best = s
print(best["dir"] if best else "")
PY
)
  [ -n "$d" ] || return 1
  r=$(ls -t "$EV/$d"/result_p24-*.json 2>/dev/null | head -1)
  [ -n "$r" ] || return 1
  printf '%s' "$r"
}

latest_owned_history() {  # newest OWNED_HISTORY= marker, same file the
                          # window/history steps actually produced
  local best
  best=$(sed -n 's/^OWNED_HISTORY=//p' "$EV/10c-owned-history.log" 2>/dev/null | tail -1)
  [ -n "$best" ] && [ -f "$best" ] || best=$(ls -t "$EV"/owned-history-*.json 2>/dev/null | head -1)
  [ -n "$best" ] && [ -f "$best" ] || return 1
  printf '%s' "$best"
}

phase_pre_auxdown_gates() {  # strict guard4 + owned quiesce BEFORE aux-down
  require_confirm pre-auxdown-gates
  local pr; pr=$(latest_p24_result) || { echo "[p24-formal] pre-auxdown: no p24 result" >&2; return 3; }
  local hist=""
  hist=$(latest_owned_history) || hist=""
  local q=(--p24-result "$pr" --env-dir "$TMP")
  [ -n "$hist" ] && q+=(--owned-history "$hist")
  step 11a-pre-auxdown-guard4 python3 -I "$P24/p24_live_guard.py" "$TMP" connector || return
  step 11a-pre-auxdown-quiesce python3 -I "$P24/check_owned_quiesced_p24.py" "${q[@]}"
}

phase_pre_restart_gates() {  # same gates immediately BEFORE final restart
  require_confirm pre-restart-gates
  local pr; pr=$(latest_p24_result) || { echo "[p24-formal] pre-restart: no p24 result" >&2; return 3; }
  local hist=""
  hist=$(latest_owned_history) || hist=""
  local q=(--p24-result "$pr" --env-dir "$TMP")
  [ -n "$hist" ] && q+=(--owned-history "$hist")
  step 11c-pre-restart-guard4 python3 -I "$P24/p24_live_guard.py" "$TMP" connector || return
  step 11c-pre-restart-quiesce python3 -I "$P24/check_owned_quiesced_p24.py" "${q[@]}"
}

phase_persist_pre() {   # owned persistent state BEFORE final restart
  require_confirm persist-pre
  local pr; pr=$(latest_p24_result) || return 3
  local d="$EV/11-persist-pre"; local n=0
  while [ -e "$d" ]; do n=$((n+1)); d="$EV/11-persist-pre-r$n"; done
  local h1=""
  h1=$(latest_owned_history) || h1=""
  local pargs=(--p24-result "$pr" --out-dir "$d" --env-dir "$TMP")
  if [ -n "$h1" ] && [ -f "$h1" ] && grep -q -- '--owned-history' "$P24/collect_persist_state_p24.py"; then
    pargs+=(--owned-history "$h1")
  fi
  step 11b-persist-pre python3 -I "$P24/collect_persist_state_p24.py" \
    "${pargs[@]}"
}

phase_persist_post() {  # same snapshot AFTER final restart (compare in finalize)
  require_confirm persist-post
  local pr; pr=$(latest_p24_result) || return 3
  local d="$EV/13-persist-post"; local n=0
  while [ -e "$d" ]; do n=$((n+1)); d="$EV/13-persist-post-r$n"; done
  local h2=""
  h2=$(latest_owned_history) || h2=""
  local pargs2=(--p24-result "$pr" --out-dir "$d" --env-dir "$TMP")
  if [ -n "$h2" ] && [ -f "$h2" ] && grep -q -- '--owned-history' "$P24/collect_persist_state_p24.py"; then
    pargs2+=(--owned-history "$h2")
  fi
  step 13b-persist-post python3 -I "$P24/collect_persist_state_p24.py" \
    "${pargs2[@]}"
}

phase_restart3() {
  require_confirm restart3
  # explicit graceful 120s stop, then start, health, and ACTUAL image
  # verification of the restarted base three
  step 11-restart3-stop compose stop -t 120 dataset pipeline connector || return
  step 11-restart3-start compose start dataset pipeline connector || return
  step 11-restart3-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only || return
  # base3 verify: ONLY the original three services on the PINNED gate
  # IDs (frozen swap_verify would demand aux running — aux is REMOVED
  # by design here; same step name, no ls -t, gate-pinned)
  step 11-restart3-verify python3 -I "$P24/check_running_images_p24.py" \
    --gate-json "$EV/01b-swap-source-gate.json" --base3 \
    --out "$EV/11-restart3-base3-images.json"
}

phase_reread() {
  local before; before=$(cat "$EV/00-baseline-path.txt" 2>/dev/null) || true
  [ -n "$before" ] && [ -f "$before/manifest.json" ] || { echo "[p24-formal] reread: missing baseline path" >&2; return 3; }
  step 12-reread python3 -I "$P24/readonly_baseline_p24.py" --env-dir "$TMP" || return
  local after; after=$(capture_var BASELINE_DIR)
  [ -n "$after" ] && [ -f "$after/manifest.json" ] || { echo "[p24-formal] reread: no BASELINE_DIR" >&2; return 3; }
  printf '%s\n' "$after" > "$EV/12-reread-path.txt"
  # freeze the ACTUAL after-manifest path BEFORE any later step can
  # move $LAST_STEP_DIR (review fix: never let the logdir drift)
  local after_json="$EV/12-reread-after-manifest.json"
  cp "$after/manifest.json" "$after_json" || return 4
  local cmp_dir="$EV/12-reread-compare-output"; local n=0
  while [ -e "$cmp_dir" ]; do n=$((n+1)); cmp_dir="$EV/12-reread-compare-output-r$n"; done
  mkdir -p "$cmp_dir"
  step 12-reread-compare python3 -I "$P22/compare_baseline.py" \
    --before "$before/manifest.json" --after "$after_json" \
    --out "$cmp_dir/compare.json" --mode restart || return
  printf '%s\n' "$cmp_dir" > "$EV/12-reread-compare-path.txt"
}

phase_postfacts() { # ONE fresh attempt dir + attempt-bound verifier
  require_confirm postfacts
  local abc pr attempt n=0
  abc=$(ls -t "$EV"/10b-p21-ABC*/result_p21.json 2>/dev/null | head -1)
  pr=$(ls -t "$EV"/10c-p24-scenarios*/result_p24-*.json 2>/dev/null | head -1)
  [ -n "$abc" ] && [ -f "$abc" ] && [ -n "$pr" ] && [ -f "$pr" ] || {
    echo "[p24-formal] postfacts: ABC result and P24 result are BOTH required" >&2
    return 3; }
  attempt="$EV/13-postfacts-attempt-$(date +%Y%m%dT%H%M%S)"
  while [ -e "$attempt" ]; do n=$((n+1)); attempt="$EV/13-postfacts-attempt-$(date +%Y%m%dT%H%M%S)-r$n"; done
  # collection failures are archived in the attempt dir BEFORE we return
  local h3=""
  h3=$(latest_owned_history) || h3=""
  local cargs=(--env-dir "$TMP" --abc-result "$abc" --p24-result "$pr" \
    --out-dir "$attempt")
  if [ -n "$h3" ] && [ -f "$h3" ] && grep -q -- '--owned-history' "$P24/collect_postfacts_p24.py"; then
    cargs+=(--owned-history "$h3")
  fi
  step 13-postfacts-collect python3 -I "$P24/collect_postfacts_p24.py" \
    "${cargs[@]}" || return
  # verify must receive the SAME history binding the collector got
  # (PG contract: flag/manifest sha symmetry + union-owned roster)
  local vargs=(--attempt-dir "$attempt" --abc-result "$abc" --p24-result "$pr")
  if [ -n "$h3" ] && [ -f "$h3" ] && grep -q -- '--owned-history' "$P24/verify_postfacts_p24.py"; then
    vargs+=(--owned-history "$h3")
  fi
  step 13-postfacts-verify python3 -I "$P24/verify_postfacts_p24.py" \
    "${vargs[@]}"
}

phase_p1post9() {
  step 13-p1-post9 bash -o pipefail "$P1/run.sh" smoke || return
  cp "$TMP/result.json" "$LAST_STEP_DIR/result.json"
}

phase_source_freeze() {
  # post-acceptance candidate freeze (review fix): source CONTENT hash
  # must equal the pre-build hash, and the ACTUAL 5 image IDs still
  # running must equal the built candidate set
  require_confirm source-freeze
  step 14-source-hash-post python3 -I "$P24/source_hash_p24.py" \
    "$ROOT" "$EV/14-source-hash-post.txt" || return
  local post_sha; post_sha=$(sed -n 's/^SOURCE_HASH=//p' "$LAST_STEP_LOG" | tail -1)
  local pre_sha; pre_sha=$(cat "$EV/01-source-hash-pre.sha")
  [ -n "$post_sha" ] && [ "$post_sha" = "$pre_sha" ] || {
    echo "[p24-formal] source-freeze: candidate content hash DRIFT pre=$pre_sha post=$post_sha — diagnostics only" >&2
    return 5; }
  local bf_ids; bf_ids=$(ls -t "$EV"/01-build-final*/image-ids.json 2>/dev/null | head -1)
  [ -n "$bf_ids" ] || { echo "[p24-formal] source-freeze: no build image-ids.json" >&2; return 3; }
  local imgck="$EV/14-image-ids-check.py"
  cat > "$imgck" <<'PY'
import json, subprocess, sys
want = json.load(open(sys.argv[1]))
cur = subprocess.run(["docker", "images", "--format",
                      "{{.Repository}} {{.ID}}"],
                     capture_output=True, text=True).stdout
ids = {repo.replace("openfoundry-platform-p1-q3v9-", ""): iid
       for repo, iid in (l.split() for l in cur.splitlines()
                         if l.startswith("openfoundry-platform-p1-q3v9-"))}
drift = {svc: (ids.get(svc), want.get(svc))
         for svc in want if ids.get(svc) != want.get(svc)}
print(f"[image-freeze] built={want} current={ {k: ids.get(k) for k in want} }")
if drift:
    print(f"[image-freeze] DRIFT {drift}", file=sys.stderr)
    sys.exit(5)
PY
  step 14-image-ids-check python3 -I "$imgck" "$bf_ids"
}

phase_aux_down() {
  require_confirm aux-down
  step 99-aux-down sh "$P22/p22_connector2.sh" down
}

phase_standalone() {
  require_confirm standalone
  local roots="${P24_STANDALONE_ROOTS:?export P24_STANDALONE_ROOTS=dir[:dir2...]}" args=""
  local IFS=':'; for r in $roots; do args="$args --src-root $r"; done; unset IFS
  # frozen P23 archiver reused read-only via args; index lands in $EV
  step 90-standalone python3 -I "$P23/archive_standalone_p23.py" \
    $args --dst "$EV/standalone"
}

phase_finalize() {
  # FINALIZER (attempt-bound): every verdict reads the LATEST manifest
  # attempt's step dir (never glob-first mixing of older greens); the
  # ABC gate checks the REAL manifest order; PG evidence may be bound
  # from the green PG_FINAL_BINDING.json (no identical-content rerun)
  # or from executed steps; secret scan + hash manifest stay LAST.
  python3 - "$EV" <<'PY'
import glob, hashlib, json, os, pathlib, re, sys
ev = pathlib.Path(sys.argv[1])  # EV-only entry point

def sha(p):
    return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()

steps = []          # chronological (manifest order) step records
for l in open(ev / "manifest.jsonl"):
    l = l.strip()
    if not l:
        continue
    try:
        steps.append(json.loads(l))
    except ValueError:
        # review: a corrupt manifest line is FATAL, never skipped
        raise SystemExit(f"finalize: unparsable manifest.jsonl line: "
                         f"{l[:80]!r}")

def attempts(name):
    return [s for s in steps if s.get("step") == name and "rc" in s]

def latest(name):
    a = attempts(name)
    return a[-1] if a else None

def dir_of(name):
    s = latest(name)
    return (ev / s["dir"].rstrip("/")) if s and s.get("dir") else None

def log_of(name):
    s = latest(name)
    return (ev / s["log"]) if s and s.get("log") else None

def path_from_log(name, marker):
    lg = log_of(name)
    if not lg or not lg.is_file():
        return None
    for ln in reversed(lg.read_text(errors="replace").splitlines()):
        if marker in ln:
            tail = ln.split(marker, 1)[1].strip().split()[0] \
                if marker.endswith("=") else ln.rsplit("-> ", 1)[-1].strip()
            return tail
    return None

PG_EXEC = ["05-pg-new-normal", "05-pg-new-normal-exact",
           "05-pg-new-race", "05-pg-new-race-exact",
           "05-pg-unified", "05-pg-exact"]
FIXED = ["00-preflight-verify", "00-baseline", "01-source-hash-pre",
         "01-fingerprints", "04-unit", "01-build-final",
         "01b-swap-guard", "01b-swap-stop", "01b-swap-up-base",
         "07-aux-up-swap", "01b-swap-health", "01b-swap-verify",
         "06-p1-pre9", "09-p23-adapted-full", "10-p22-full",
         "10c-post-p22-images", "07-aux-up", "07a-p24-schedule-env",
         "07b-schedule-env-apply", "07c-schedule-env-verify",
         "10c-p24-scenarios", "10c-p24-sidecars",
         "10c-owned-history", "10d-window-quiesce",
         "10d-window-guard4",
         "10e-restore-loop0", "10g-restore-health",
         "10f-loop0-verify", "07-pre-window-guard4",
         "07-pre-window-images", "07d-healthz-wait",
         "11a-pre-auxdown-guard4",
         "11a-pre-auxdown-quiesce", "99-aux-down", "11b-persist-pre",
         "11c-pre-restart-guard4", "11c-pre-restart-quiesce",
         "11-restart3-stop", "11-restart3-start", "11-restart3-health",
         "11-restart3-verify", "12-reread", "12-reread-compare",
         "13-p1-post9", "10b-p21-ABC", "13-postfacts-collect",
         "13-postfacts-verify", "13b-persist-post",
         "14-source-hash-post", "14-image-ids-check", "90-standalone"]
pg_mode = "import" if latest("05-pg-import") else "exec"
REQUIRED = FIXED + (["05-pg-import"] if pg_mode == "import" else PG_EXEC)
step_rc, missing = {}, []
for name in REQUIRED:
    s = latest(name)
    if s is None:
        missing.append(name)
    else:
        step_rc[name] = s["rc"]
verdicts = {}

def read(p):
    try:
        return json.load(open(p))
    except Exception:
        return None

def result_in(step, fname_glob):
    d = dir_of(step)
    if not d:
        return None
    hits = sorted(glob.glob(str(d / fname_glob)))
    return read(hits[-1]) if hits else None

def jv(label, d, check):
    verdicts[label] = {"ok": bool(d is not None and check(d)),
                       "detail": None if d is None else "attempt-bound"}

jv("p1_pre9_9", result_in("06-p1-pre9", "result.json"),
   lambda d: d.get("counts", {}).get("pass") == 9
   and d.get("counts", {}).get("fail") == 0)
jv("p1_post9_9", result_in("13-p1-post9", "result.json"),
   lambda d: d.get("counts", {}).get("pass") == 9
   and d.get("counts", {}).get("fail") == 0)
jv("p23_adapted_8_official",
   result_in("09-p23-adapted-full", "result_p23.json"),
   lambda d: d.get("official") is True and d.get("partial") is False
   and d.get("counts", {}).get("pass") == 8)
qa_path = path_from_log("09-p23-adapted-full", "-> ")
qad = read(qa_path) if qa_path else None
verdicts["q_adaptation_exactly_one"] = {
    "ok": bool(qad and qad.get("exactly_one") is True
               and qad.get("smoke_rc") == 0),
    "detail": qa_path}
jv("p22_full_11", result_in("10-p22-full", "result_p22.json"),
   lambda d: d.get("official") is True
   and d.get("counts", {}).get("pass") == 11)
jv("p21_abc_3", result_in("10b-p21-ABC", "result_p21.json"),
   lambda d: d.get("official") is True
   and d.get("counts", {}).get("pass") == 3)
p24d = result_in("10c-p24-scenarios", "result_p24-*.json")
verdicts["p24_official_cleanup_ok"] = {
    "ok": bool(p24d and p24d.get("official") is True
               and (p24d.get("cleanup") or {}).get("ok") is True),
    "detail": "attempt-bound"}
pf_path = path_from_log("13-postfacts-verify", "-> ")
if pf_path and not pf_path.startswith("/"):
    cand = ev / pf_path                     # PG verifier prints the
    pf_path = str(cand) if cand.is_file() else pf_path  # basename
pf_sha = sha(pf_path) if pf_path and pathlib.Path(pf_path).is_file() \
    else None
pfd = read(pf_path) if pf_path else None
verdicts["postfacts_all_ok"] = {"ok": bool(pfd and pfd.get("all_ok") is True),
                                "detail": {"path": pf_path,
                                           "sha256": pf_sha}}

# PG evidence: bound import (binding json + its own verdict) or executed
if pg_mode == "import":
    gi_path = path_from_log("05-pg-import", "PG_IMPORT_JSON=")
    gid = read(gi_path) if gi_path else None
    verdicts["pg_evidence"] = {
        "ok": bool(step_rc.get("05-pg-import") == 0
                   and gid and gid.get("ok") is True
                   and gid.get("artifacts_count", 0) >= 16
                   and gid.get("tools_manifest_actual_ok") is True),
        "detail": {"path": gi_path,
                   "artifacts": (gid or {}).get("artifacts_count"),
                   "optional_skip_unverified":
                       (gid or {}).get("optional_skip_unverified")}}
else:
    verdicts["pg_evidence"] = {
        "ok": all(step_rc.get(n) == 0 for n in PG_EXEC),
        "detail": "executed isolated-PG rounds"}

# persist: INDEPENDENTLY validate each pre/post attempt's raw/derived/
# rc/sha/bad-steps + same result binding + non-empty exact owned set
# (disabled/NULL, grants [], runs terminal) BEFORE state equality
def validate_persist(name):
    d = None
    pd_ = path_from_log(name, "PERSIST_DIR=")
    if pd_:
        d = pathlib.Path(pd_)
    if d is None or not (d / "persist-manifest.json").is_file():
        d2 = dir_of(name)
        if d2 and (d2 / "persist-manifest.json").is_file():
            d = d2
    if d is None:
        return None, f"{name}: no persist dir (log marker/step dir)"
    pm = read(d / "persist-manifest.json")
    if not pm:
        return None, f"{name}: persist-manifest missing"
    probs = []
    if pm.get("bad"):
        probs.append(f"bad steps {pm.get('bad')}")
    for fname, meta in sorted((pm.get("files") or {}).items()):
        fp = d / fname
        if not fp.is_file() or sha(fp) != meta.get("sha256"):
            probs.append(f"{fname}: missing/sha drift")
    # ALL 16 artifacts required: 4 names x (txt, txt.rc, txt.err,
    # json) — every one listed in the manifest, present, hash-equal
    names4 = ("defs", "ticks", "runs", "grants")
    expect = {f"{k}{ext}" for k in names4
              for ext in (".txt", ".txt.rc", ".txt.err", ".json")}
    listed = set((pm.get("files") or {}).keys())
    for miss in sorted(expect - listed):
        probs.append(f"artifact not in manifest: {miss}")
    for fname, meta in sorted((pm.get("files") or {}).items()):
        fp = d / fname
        if not fp.is_file() or sha(fp) != meta.get("sha256"):
            probs.append(f"{fname}: missing/sha drift")
    for k in names4:
        rcp = d / f"{k}.txt.rc"
        rc_manifest = (pm.get("rc") or {}).get(k)
        rc_text = rcp.read_text().strip() if rcp.is_file() else None
        if not (rc_text is not None and rc_text == "0"
                and str(rc_manifest) == "0"):
            probs.append(f"{k}: rc file={rc_text!r} manifest="
                         f"{rc_manifest!r} (want both 0)")
        dj, rj = d / f"{k}.json", d / f"{k}.txt"
        state_k = (pm.get("state") or {}).get(k)
        if dj.is_file() and rj.is_file():
            lines = [l for l in rj.read_text().strip().splitlines()
                     if l]
            try:
                derived = json.loads(dj.read_text())
                raw_k = json.loads(lines[-1] if lines else "null")
                raw_k = [] if raw_k is None else raw_k
                if not (state_k == derived == raw_k):
                    probs.append(f"{k}: state!=derived!=raw reparse")
            except ValueError:
                probs.append(f"{k}: raw reparse invalid")
        else:
            probs.append(f"{k}: raw/derived file absent")
    if pm.get("bad"):
        probs.append(f"bad steps {pm.get('bad')}")
    if pm.get("hard_problems"):
        probs.append(f"hard problems {pm.get('hard_problems')}")
    state = pm.get("state") or {}
    defs_rows = state.get("defs") or []
    owned = set(pm.get("owned_ids") or [])
    if not owned or {r.get("id") for r in defs_rows} != owned:
        probs.append("defs != exact non-empty owned set")
    for r in defs_rows:
        if not (r.get("enabled") == "false" and r.get("cursor_null")
                == "true"):
            probs.append(f"{r.get('id', '?')[:8]}: not disabled/NULL")
    if state.get("grants"):
        probs.append("grant rows remain")
    runs = state.get("runs") or []
    if not runs or any(r.get("status") not in
                       ("succeeded", "failed", "aborted") for r in runs):
        probs.append("runs not all-terminal/empty")
    return pm, probs

ppre, pre_probs = validate_persist("11b-persist-pre")
ppost, post_probs = validate_persist("13b-persist-post")
hist_sha_needed = None
ohp = path_from_log("10c-owned-history", "OWNED_HISTORY=")
if ohp and pathlib.Path(ohp).is_file():
    hist_sha_needed = sha(ohp)
# STRICT (root final review): the PG field is owned_history_sha256;
# require NON-EMPTY and pre == post == sha256(actual bound history
# file). No permissive fallback.
hist_bind_note = "history-bound"
pre_h = (ppre or {}).get("owned_history_sha256")
post_h = (ppost or {}).get("owned_history_sha256")
if not hist_sha_needed:
    pre_probs.append("no bound history file (10c marker unresolved)")
    hist_bind_note = "unbound"
elif not (pre_h and post_h and pre_h == post_h == hist_sha_needed):
    pre_probs.append(f"owned_history_sha256 pre={pre_h!r} post={post_h!r}"
                     f" bound={hist_sha_needed!r}")
    hist_bind_note = "mismatch"
else:
    hist_bind_note = f"both-bound=={hist_sha_needed[:12]}…"
persist_ok = bool(ppre and ppost and not pre_probs and not post_probs
                  and ppre.get("p24_result_sha256")
                  == ppost.get("p24_result_sha256")
                  and ppre.get("owned_ids") == ppost.get("owned_ids")
                  and ppre.get("state") == ppost.get("state"))
verdicts["owned_persistence_pre_eq_post"] = {
    "ok": persist_ok,
    "detail": {"pre_problems": pre_probs[:4],
               "post_problems": post_probs[:4],
               "same_result_sha": bool(
                   ppre and ppost and ppre.get("p24_result_sha256")
                   == ppost.get("p24_result_sha256")),
               "state_equal": bool(
                   ppre and ppost
                   and ppre.get("state") == ppost.get("state")),
               "history_binding": hist_bind_note}}

# source freeze: pre == post == PG binding tools hash (when bound)
pre_sha = (ev / "01-source-hash-pre.sha").read_text().strip() \
    if (ev / "01-source-hash-pre.sha").is_file() else ""
post_sha = ""
plg = log_of("14-source-hash-post")
if plg and plg.is_file():
    m = re.search(r"^SOURCE_HASH=([0-9a-f]{64})$",
                  plg.read_text(errors="replace"), re.M)
    post_sha = m.group(1) if m else ""
binding_sha = ""
if pg_mode == "import":
    gi_path2 = path_from_log("05-pg-import", "PG_IMPORT_JSON=")
    gid2 = read(gi_path2) if gi_path2 else None
    binding_sha = (gid2 or {}).get("binding_sha256", "")
binding_combined = (read("/private/tmp/"
                     "openfoundry-p24-repair-evidence/pg/"
                     "PG_FINAL_BINDING.json") or {}
                    ).get("source_hash_tools_algorithm", {}
                          ).get("combined_sha256", "")
verdicts["source_freeze_equal"] = {
    "ok": bool(pre_sha and post_sha and pre_sha == post_sha
               and (pg_mode != "import"
                    or (binding_combined
                        and pre_sha == binding_combined))),
    "detail": {"pre": pre_sha, "post": post_sha,
               "binding_combined": binding_combined,
               "binding_file_sha": binding_sha}}

# ABC real-order gate (manifest sequence, not prose)
seq = [s.get("step") for s in steps]
def first_idx(n):
    return seq.index(n) if n in seq else None
abc_i = next((i for i, n in enumerate(seq) if n == "10b-p21-ABC"), None)
order_bad = []
if abc_i is None:
    order_bad.append("ABC step absent")
else:
    must_precede = ["01b-swap-stop", "01b-swap-up-base",
                    "07-pre-window-images", "07b-schedule-env-apply",
                    "10e-restore-loop0", "99-aux-down",
                    "11-restart3-stop", "12-reread", "13-p1-post9"]
    for n in must_precede:
        i = first_idx(n)
        if i is None or i > abc_i:
            order_bad.append(f"{n} not before ABC (idx {i} vs {abc_i})")
    allowed_after = ("13-postfacts-", "13b-persist-post", "14-",
                     "15-final-", "16-tools-sha-final", "90-standalone")
    for i2, n in enumerate(seq[abc_i + 1:], abc_i + 1):
        if not n.startswith(allowed_after):
            order_bad.append(f"write-ish step {n} after ABC (idx {i2})")
verdicts["abc_real_order_gate"] = {
    "ok": not order_bad,
    "detail": order_bad[:4] or
              "all q3 mutation before ABC; only read-only after"}

# restart compare (fresh sidecar)
cmp_ok = False
sidecar = ev / "12-reread-compare-path.txt"
if sidecar.is_file():
    d = read(os.path.join(open(sidecar).read().strip(), "compare.json"))
    cmp_ok = bool(d and d.get("ok") is True)
verdicts["restart_compare_ok"] = {"ok": cmp_ok,
                                  "detail": "fresh sidecar"}

# standalone index (actual roots/each-file hash/transform/missing)
st_step = latest("90-standalone") or {}
st_argv = st_step.get("cmd", "")
want_roots = set(re.findall(r"--src-root (\S+)", st_argv))
idx = read(ev / "standalone/index.json")
cov_roots = {e.get("src_root") for e in (idx or [])
             if isinstance(e, dict) and e.get("src_root")}
if want_roots and cov_roots and want_roots != cov_roots:
    filebad.append(f"index roots {sorted(cov_roots)} != step argv "
                   f"{sorted(want_roots)}")
entries = (idx or []) if isinstance(idx, list) else (idx or {}).get("files", [])
miss = [e for e in entries if isinstance(e, dict) and e.get("missing")]
skipped_envlike = [e for e in entries if isinstance(e, dict)
                   and e.get("skipped")]
filebad, transformed_n = [], 0
for e in entries:
    if not isinstance(e, dict) or e.get("missing") or e.get("skipped"):
        continue
    dst = e.get("dst")
    sd_, ss_ = e.get("sha256_dst"), e.get("sha256_src")
    if not (dst and sd_ and ss_):
        filebad.append(f"entry missing dst/sha fields: {str(e)[:80]}")
        continue
    dp = pathlib.Path(dst)
    if not dp.is_file() or sha(dp) != sd_:
        filebad.append(f"dst sha drift: {dst}")
    if e.get("transformed"):
        transformed_n += 1
        if not e.get("redactions"):
            filebad.append(f"transformed without redactions: {dst}")
    elif ss_ != sd_:
        filebad.append(f"clean rule violated (src!=dst): {dst}")
verdicts["standalone_index_verified"] = {
    "ok": bool(entries) and not miss and not filebad,
    "detail": {"entries": len(entries), "missing": len(miss),
               "env_like_skipped": len(skipped_envlike),
               "transformed": transformed_n,
               "file_problems": filebad[:4]}}

# optional external SKIPs: from the bound PG import (unverified), else
# from the actual latest unit log
skips = []
if pg_mode == "import":
    gi_path3 = path_from_log("05-pg-import", "PG_IMPORT_JSON=")
    skips = (read(gi_path3) if gi_path3 else {}
             ).get("optional_skip_unverified") or []
else:
    ul = latest("04-unit")
    if ul:
        log = (ev / ul["log"]).read_text(errors="replace")
        skips = sorted(set(re.findall(r"^--- SKIP:\s+(\S+)", log, re.M)))
verdicts["optional_skips_recorded_unverified"] = {
    "ok": True, "detail": skips}

# owned-history union must be COVERED by the postfacts owned roster —
# old failed-attempt defs may never fall out of scope as "unknown"
oh_path = path_from_log("10c-owned-history", "OWNED_HISTORY=")
oh = read(oh_path) if oh_path else None
if oh and oh.get("schema") != "p24-owned-history-v1":
    oh = None
union_ids = set((oh or {}).get("owned_sync_ids") or {})
pf_collect_dir = None
pf_paths = path_from_log("13-postfacts-collect", "ATTEMPT_DIR=")
if pf_paths:
    pf_collect_dir = pathlib.Path(pf_paths)
roster = set()
if pf_collect_dir and (pf_collect_dir / "12a-owned-defs.json").is_file():
    roster = {r.get("id") for r in
              read(pf_collect_dir / "12a-owned-defs.json") or []}
uncovered = sorted(union_ids - roster) if roster else sorted(union_ids)
verdicts["owned_history_union_covered"] = {
    "ok": bool(oh and oh.get("all_attempts_valid")
               and union_ids and not uncovered),
    "detail": {"history": oh_path, "union": len(union_ids),
               "roster": len(roster), "uncovered": uncovered[:5]}}

imgv = read(ev / "07-pre-window-images.json")
verdicts["five_running_images_pinned"] = {
    "ok": bool(imgv and imgv.get("ok") is True),
    "detail": {"bad": (imgv or {}).get("bad", [])[:3]}}
sd_path = path_from_log("10c-p24-sidecars", "SIDECARS_MANIFEST=")
sd = read(sd_path) if sd_path else None
sd_bad = []
p24r_path_ref = None
if sd:
    p24r_path_ref = sd.get("result")
    p24r_sha_ref = sd.get("result_sha256")
    if not (p24r_path_ref and pathlib.Path(p24r_path_ref).is_file()
            and sha(p24r_path_ref) == (p24r_sha_ref or "")):
        sd_bad.append("manifest result binding/sha mismatch")
    for c in sd.get("copied") or []:
        dp = pathlib.Path(c.get("dst", ""))
        if not dp.is_file() or sha(dp) != c.get("sha256") \
                or dp.stat().st_size != c.get("bytes"):
            sd_bad.append(f"copied drift: {c.get('dst')}")
    if not (sd.get("copied") and not sd.get("missing")):
        sd_bad.append("no copied sidecars or missing references")
    # exact-set binding: copied sources == referenced minus missing
    copied_srcs = {c.get("src") for c in sd.get("copied") or []}
    want_srcs = set(sd.get("referenced") or []) - set(
        sd.get("missing") or [])
    if copied_srcs != want_srcs:
        sd_bad.append(f"copied set != referenced set: "
                      f"missing_from_copy={sorted(want_srcs - copied_srcs)}"
                      f" extra={sorted(copied_srcs - want_srcs)}")
verdicts["g4_sidecars_archived"] = {
    "ok": bool(sd and not sd_bad),
    "detail": {"manifest": sd_path,
               "copied": len((sd or {}).get("copied") or []),
               "missing": (sd or {}).get("missing"),
               "problems": sd_bad[:4],
               "bound_result": p24r_path_ref}}

verdicts["abc_sequence"] = {
    "ok": True,
    "detail": "LOCKED SAFE FALLBACK: all q3 mutation/P1post9/old "
              "artifact+BuildJob reread BEFORE ABC; ABC last q3 write; "
              "post-ABC read-only only; new two NOT claimed to cross "
              "the final restart; fixed pins stay exactly four"}

# secret scan (env VALUES + value patterns; placeholders excluded)
envf = "/private/tmp/openfoundry-platform-p1-q3v9/.env"
vals = [l.split("=", 1)[1].strip() for l in open(envf)
        if "=" in l and l.split("=", 1)[1].strip()]
_pat = {"jwt": re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}"),
        "private_key": re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
        "lease_value": re.compile(r"(lease_token|lease_holder)[\"\'\s:=]+[A-Za-z0-9._\-]{8,}"),
        "dsn_password": re.compile(r"postgres(?:ql)?://[^:/\s]+:(?!\$\{|<redacted>)[^@\s]+@")}
leaks, hits = [], []
for path in glob.glob(str(ev / "**/*"), recursive=True):
    if not os.path.isfile(path):
        continue
    try:
        blob = open(path, errors="replace").read()
    except Exception:
        continue
    for v in vals:
        if v and v in blob:
            leaks.append(os.path.relpath(path, ev))
    for kind, rx in _pat.items():
        n = len(rx.findall(blob))
        if n:
            hits.append({"file": os.path.relpath(path, ev),
                         "kind": kind, "count": n})
json.dump({"secret_scan_leaks": leaks,
           "value_pattern_hits": hits},
          open(ev / "secret-scan.json", "w"), indent=2)

all_rc0 = (not missing and all(v == 0 for v in step_rc.values()))
final_ok = (all_rc0 and all(v["ok"] for v in verdicts.values())
            and not leaks and not hits)
json.dump({"client_date": "2026-11-14", "required_step_rc": step_rc,
           "missing_steps": missing, "pg_mode": pg_mode,
           "content_verdicts": verdicts,
           "secret_scan": {"leaks": len(leaks), "hits": len(hits)},
           "all_required_rc0": all_rc0, "FINAL_OK": final_ok},
          open(ev / "manifest.json", "w"), indent=2)
manifest_hashes = {}
for path in sorted(glob.glob(str(ev / "**/*"), recursive=True)):
    if os.path.isfile(path) and not path.endswith("hash-manifest.json"):
        manifest_hashes[os.path.relpath(path, ev)] = \
            hashlib.sha256(open(path, "rb").read()).hexdigest()
json.dump({"client_date": "2026-11-14", "files": manifest_hashes},
          open(ev / "hash-manifest.json", "w"), indent=2)
print("finalize: FINAL_OK =", final_ok, "| pg_mode:", pg_mode,
      "| missing:", missing,
      "| rc!=0:", [k for k, v in step_rc.items() if v != 0],
      "| verdicts bad:", [k for k, v in verdicts.items() if not v["ok"]],
      "| secret leaks:", len(leaks), "| hits:", len(hits))
sys.exit(0 if final_ok else 1)
PY
}

# ---------------------------------------------------------- orchestration --
run_phase() {  # hyphen→underscore INSIDE (review bug 1a: callers pass
               # pg-new etc. verbatim)
  local fn="phase_${1//-/_}"
  "$fn"
  return $?
}

full_abort() {  # review bug 1b: echo would clobber $? — rc arrives as $1
  local rc="$1" ph="$2"
  echo "[p24-formal] full: aborted at $ph rc=$rc (failures already archived)" >&2 | tee -a "$EV/console.log" >&2
  exit "$rc"
}

case "${1:-}" in
  preflight|baseline|fingerprints|unit|pg-new|pg-unified|pg-import|\
  pg-evidence|build|swap|\
  p1pre9|p23-adapted|p22full|scheduler-window|abc-pre|p21abc|\
  cohort-gate|pre-auxdown-gates|aux-down|pre-restart-gates|restart3|\
  reread|postfacts|p1post9|persist-pre|persist-post|source-freeze|\
  standalone|finalize)
    run_phase "${1}"
    rc=$?
    if [ $rc -ne 0 ]; then full_abort "$rc" "${1}"; fi
    exit 0
    ;;
  full)
    # LOCKED SAFE FALLBACK (root, 2026-11-14 formal-execution-contract
    # final selection): ALL q3 restarts/recreates/aux-removal AND the
    # P1post9 + old artifact/BuildJob readback happen BEFORE P21ABC;
    # ABC is the LAST q3 WRITE; afterwards ONLY read-only snapshot,
    # hash and finalize. The cohort-gate sequence is NOT auto-selected
    # in this round (phase_cohort_gate stays standalone for a future,
    # separately-reviewed round). Fixed pins stay EXACTLY four.
    for ph in preflight baseline fingerprints unit pg-evidence \
              build swap p1pre9 p23-adapted p22full scheduler-window \
              pre-auxdown-gates aux-down persist-pre pre-restart-gates \
              restart3 reread p1post9; do
      run_phase "$ph"; rc=$?
      if [ $rc -ne 0 ]; then full_abort "$rc" "$ph"; fi
    done
    echo "[p24-formal] full: LOCKED FALLBACK — ABC is the LAST q3 write; its new two will NOT undergo the final restart (reported, never claimed otherwise)" >&2 | tee -a "$EV/console.log"
    for ph in p21abc postfacts persist-post source-freeze standalone finalize; do
      run_phase "$ph"; rc=$?
      if [ $rc -ne 0 ]; then full_abort "$rc" "$ph"; fi
    done
    exit 0
    ;;
  list) grep -o 'phase_[a-z0-9_]*' "$SCRIPT_PATH" | sort -u | sed 's/^phase_//' | tr '\n' ' '; echo ;;
  *) sed -n '2,20p' "$SCRIPT_PATH" >&2; echo "phases: $(grep -o 'phase_[a-z0-9_]*' "$SCRIPT_PATH" | sort -u | sed 's/^phase_//' | tr '\n' ' ')" >&2; exit 2 ;;
esac
