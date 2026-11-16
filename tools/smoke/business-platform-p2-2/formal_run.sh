#!/bin/bash
# P2.2 FORMAL acceptance orchestrator (root release 2026-11-14; rev2).
# Every step captures the REAL rc (direct redirection, no tee), writes a
# COLLISION-SAFE log/dir (reruns append -rN suffixes, history is never
# overwritten), appends valid-JSON {step,rc,seconds,cmd,log,dir} lines to
# manifest.jsonl, and each phase EXITS WITH THE STEP'S REAL RC so a
# failed step can never look green. Long phases run minutes; resume with
# the same step name after fixing.
set -uo pipefail

ROOT="/Volumes/ExternalSD/projects/OpenFoundry"
P1="$ROOT/tools/smoke/business-platform-p1"
P22="$ROOT/tools/smoke/business-platform-p2-2"
TMP="${OF_P1_TMP:-/private/tmp/openfoundry-platform-p1-q3v9}"
PROJ="openfoundry-platform-p1-q3v9"
EV="$P22/results/2026-11-14-p22-final"
MF="$EV/manifest.jsonl"
mkdir -p "$EV"
export P22_CONFIRM_CONNECTOR2=yes
cd "$ROOT"

compose() { docker compose -f "$TMP/compose.yaml" --project-name "$PROJ" "$@"; }

step() { # step <name> <cmd...> — real rc, collision-safe artifacts
  local name="$1"; shift
  local log="$EV/$name.log" dir="$EV/$name" n=0
  while [ -e "$log" ]; do          # NEVER overwrite a previous attempt
    n=$((n + 1))
    log="$EV/$name-r$n.log"
    dir="$EV/$name-r$n"
  done
  mkdir -p "$dir"
  local t0 rc dur
  t0=$(date +%s)
  echo "[formal] START $name: $*" | tee -a "$EV/console.log" >&2
  ( "$@" ) >"$log" 2>&1 < /dev/null
  rc=$?
  dur=$(( $(date +%s) - t0 ))
  printf '{"step":"%s","rc":%d,"seconds":%d,"cmd":%s,"log":"%s","dir":"%s/"}\n' \
    "$name" "$rc" "$dur" \
    "$(printf '%s' "$*" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')" \
    "${log#$EV/}" "${dir#$EV/}" >> "$MF"
  echo "[formal] END $name rc=$rc ${dur}s log=${log#$EV/}" | tee -a "$EV/console.log" >&2
  LAST_STEP_LOG="$log"; LAST_STEP_DIR="$dir"
  return $rc
}

latest_baseline_dir() { # newest readonly-baseline dir under preparation
  ls -d "$P22"/results/preparation/*-readonly-baseline 2>/dev/null | tail -1
}

case "${1:-}" in
  build-final)
    # FINAL-candidate 5-target build (see header). Collision-safe dir.
    bdir="$EV/01-build-final"; n=0
    while [ -e "$bdir" ]; do n=$((n+1)); bdir="$EV/01-build-final-r$n"; done
    mkdir -p "$bdir"
    rc=0
    {
      echo "## cmd: compose build dataset pipeline connector"
      compose build dataset pipeline connector 2>&1
      echo "rc=$?"
    } > "$bdir/build-base3.log"
    [ "$(sed -n 's/^rc=//p' "$bdir/build-base3.log" | tail -1)" = "0" ] || rc=1
    {
      echo "## cmd: compose -f base -f override build connector2 connector3"
      docker compose -f "$TMP/compose.yaml" -f "$TMP/compose-p22-connector2.yaml" \
        --project-name "$PROJ" build connector2 connector3 2>&1
      echo "rc=$?"
    } > "$bdir/build-aux2.log"
    [ "$(sed -n 's/^rc=//p' "$bdir/build-aux2.log" | tail -1)" = "0" ] || rc=1
    # image IDs from the docker registry view by repo tag — robust to
    # dangling digests on stopped containers (compose images is not)
    docker images --format '{{.Repository}} {{.ID}}' \
      | grep '^openfoundry-platform-p1-q3v9-' > "$bdir/docker-images.txt"
    python3 - "$bdir" <<'PY'
import json, sys
out = sys.argv[1]
repos = {"dataset", "pipeline", "connector", "connector2", "connector3"}
ids = {}
for line in open(f"{out}/docker-images.txt"):
            repo, iid = line.split()
            svc = repo.replace("openfoundry-platform-p1-q3v9-", "")
            if svc in repos:
                ids[svc] = iid
missing = sorted(repos - set(ids))
open(f"{out}/image-ids.json", "w").write(json.dumps(ids, indent=2) + "\n")
print("[build-final] image ids:", ids, "| missing:", missing)
raise SystemExit(1 if missing else 0)
PY
    brc=$?
    printf '{"step":"01-build-final","rc":%d,"images":"%s/image-ids.json"}\n' \
      "$(( rc | brc ))" "${bdir#$EV/}" >> "$MF"
    git -C "$ROOT" rev-parse HEAD > "$bdir/git-head.txt"
    echo "[formal] build-final rc=$(( rc | brc )) dir=${bdir#$EV/}" >&2
    exit $(( rc | brc ))
    ;;
  swap)
    # Replace ALL old migrators with the fresh binaries — never a mixed
    # fleet: (1) refuse while any controlled task is still in flight;
    # (2) stop the three connectors with a generous grace and record the
    # whitelisted exits; (3) recreate base 3 + aux 2 (explicit services,
    # --no-deps, postgres/fixture health is the read-only precondition);
    # (4) verify every running replica mounts an image from THIS build
    # round and shows a NEW StartedAt. Concurrent migrate under the new
    # advisory lock is the point — no serial-start bypass.
    bf_ids=$(ls -t "$EV"/01-build-final*/image-ids.json 2>/dev/null | head -1)
    if [ -z "$bf_ids" ] || [ ! -f "$bf_ids" ]; then
      echo "[formal] swap: no 01-build-final*/image-ids.json — run build-final first" >&2
      exit 3
    fi
    step 01b-swap-guard python3 -I "$P22/swap_guard.py" "$TMP"
    rc=$?; [ $rc -ne 0 ] && { echo "[formal] swap: in-flight controlled tasks — abort" >&2; exit $rc; }
    step 01b-swap-stop docker compose -f "$TMP/compose.yaml" \
      -f "$TMP/compose-p22-connector2.yaml" --project-name "$PROJ" \
      stop -t 120 connector connector2 connector3
    rc=$?; [ $rc -ne 0 ] && exit $rc
    mkdir -p "$EV/01b-swap-stop-output"
    for svc in connector connector2 connector3; do
      cid=$(docker compose -f "$TMP/compose.yaml" -f "$TMP/compose-p22-connector2.yaml" \
        --project-name "$PROJ" ps -a -q "$svc" | head -1)
      [ -n "$cid" ] || { echo "[formal] swap: no cid for $svc" >&2; exit 4; }
      docker inspect --format "$svc {{.State.Running}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.FinishedAt}}" \
        "$cid" >> "$EV/01b-swap-stop-output/exits.txt" || exit 4
    done
    grep -c "|0|false|" "$EV/01b-swap-stop-output/exits.txt" | grep -q "^3$" || {
      echo "[formal] swap: non-graceful old-migrator exit" >&2
      cat "$EV/01b-swap-stop-output/exits.txt" >&2; exit 5; }
    # recreate: base three, then the two aux replicas (both --no-deps;
    # postgres/fixture untouched — fixture NEVER restarts this round)
    step 01b-swap-up-base compose up -d --no-deps dataset pipeline connector
    rc=$?; [ $rc -ne 0 ] && exit $rc
    step 01b-swap-up-aux docker compose -f "$TMP/compose.yaml" \
      -f "$TMP/compose-p22-connector2.yaml" --project-name "$PROJ" \
      up -d --no-deps connector2 connector3
    rc=$?; [ $rc -ne 0 ] && exit $rc
    step 01b-swap-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only
    rc=$?; [ $rc -ne 0 ] && exit $rc
    # per-replica image membership + fresh StartedAt verification
    step 01b-swap-verify python3 -I "$P22/swap_verify.py" \
      "$TMP" "$bf_ids"
    exit $?
    ;;
  build)
    step 01-build compose build dataset pipeline connector; rc=$?
    compose images --format json dataset pipeline connector \
      > "$LAST_STEP_DIR/images.json" 2>/dev/null || true
    git -C "$ROOT" rev-parse HEAD > "$LAST_STEP_DIR/git-head.txt"
    git -C "$ROOT" status --porcelain | wc -l > "$LAST_STEP_DIR/git-dirty-count.txt"
    exit $rc
    ;;
  up)
    OF_P1_SKIP_BUILD=1 step 02-up bash -o pipefail "$P1/run.sh" up
    exit $?
    ;;
  fixture-restart)
    step 03-fixture-restart compose restart fixture; rc=$?
    sleep 3
    python3 - "$LAST_STEP_DIR" <<'PY'
import json, sys, time, urllib.request
out = sys.argv[1]
pre = {"note": "in-memory fixture counters from the pre-upgrade process are "
               "HISTORICAL (archived in the 2026-11-14T021922-readonly-baseline); "
               "they RESET at this restart BY DESIGN and are never compared "
               "across the upgrade", "client_date": time.strftime("%Y-%m-%d")}
open(f"{out}/expected-counter-reset.json", "w").write(json.dumps(pre, indent=2) + "\n")
snap = {}
for name in ("p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab"):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:17090/admin/src/{name}",
                                    timeout=5) as r:
            snap[name] = json.loads(r.read())
    except Exception as exc:
        snap[name] = {"error": repr(exc)[:120]}
open(f"{out}/new-boot-initial-snapshot.json", "w").write(
    json.dumps({"taken_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "srcs": snap},
               indent=2) + "\n")
print("[formal] new-boot snapshot:",
      json.dumps({k: (v.get("request_count") if isinstance(v, dict) else None)
                  for k, v in snap.items()}))
PY
    exit $rc
    ;;
  baseline)
    local_before=$(latest_baseline_dir)
    step 00-baseline python3 -I "$P22/readonly_baseline.py" --env-dir "$TMP"; rc=$?
    # record THIS run's baseline dir explicitly (no ls-guessing later)
    local_after=$(latest_baseline_dir)
    if [ -n "$local_after" ] && [ "$local_after" != "$local_before" ]; then
      printf '%s\n' "$local_after" > "$EV/00-baseline-path.txt"
      cp "$local_after/manifest.json" "$EV/00-baseline-manifest.json" 2>/dev/null || true
    elif [ -n "$local_after" ]; then
      printf '%s\n' "$local_after" > "$EV/00-baseline-path.txt"
      cp "$local_after/manifest.json" "$EV/00-baseline-manifest.json" 2>/dev/null || true
    fi
    grep -m1 '"variant"' "$EV/00-baseline-manifest.json" \
      | tee -a "$EV/console.log" >&2 || true
    exit $rc
    ;;
  upgrade-compare)
    # FIXED pre-upgrade baseline (never ls-guessed) vs the recorded
    # post-up baseline path: persisted objects only, strict keys.
    local_pre="$P22/results/preparation/2026-11-14T021922-readonly-baseline"
    local_post=$(cat "$EV/00-baseline-path.txt" 2>/dev/null || true)
    if [ ! -f "$local_pre/manifest.json" ] || [ -z "$local_post" ] \
       || [ ! -f "$local_post/manifest.json" ]; then
      echo "[formal] upgrade-compare: fixed pre/post baseline missing" >&2
      exit 3
    fi
    mkdir -p "$EV/00b-upgrade-compare-output"
    step 00b-upgrade-compare python3 -I "$P22/compare_baseline.py" \
      --mode upgrade \
      --before "$local_pre/manifest.json" \
      --after "$local_post/manifest.json" \
      --out "$EV/00b-upgrade-compare-output/compare-upgrade.json"
    exit $?
    ;;
  unit)
    step 04-unit bash -o pipefail "$P1/run.sh" tests; rc=$?
    ok=$(grep -c '^ok ' "$LAST_STEP_LOG" || true)
    fail=$(grep -c '^FAIL' "$LAST_STEP_LOG" || true)
    skips=$(grep -c '^--- SKIP' "$LAST_STEP_LOG" || true)
    printf '{"step":"04-unit-summary","ok_packages":%s,"FAIL_lines":%s,"SKIP_lines":%s}\n' \
      "$ok" "$fail" "$skips" >> "$MF"
    echo "[formal] unit ok=$ok FAIL=$fail SKIP=$skips (step rc=$rc)" >&2
    exit $rc
    ;;
  pg50)
    mkdir -p "$EV/05-pg50"                    # guard input lives HERE
    step 05-pg50-migrate bash -o pipefail "$P1/run.sh" migrate-tests; rc=$?
    # per-attempt UNIQUE guard input (never overwrite a previous attempt;
    # both guards below consume exactly THIS copy)
    guard_input="$EV/05-pg50/migrate-tests-$(date +%Y%m%dT%H%M%S).log"
    cp "$TMP/logs/migrate-tests.log" "$guard_input"
    if [ $rc -ne 0 ]; then echo "[formal] pg50: migrate rc=$rc, guards skipped" >&2; exit $rc; fi
    step 05-pg50-guard-p21 sh "$ROOT/tools/smoke/business-platform-p2-1/run_tests_p21.sh" \
      integration "$guard_input" || rc=$?
    step 05-pg50-guard-p22 sh "$P22/run_tests_p22.sh" \
      integration "$guard_input" || rc=$?
    run50=$(grep -c '^=== RUN' "$guard_input" || true)
    pass50=$(grep -c '^--- PASS' "$guard_input" || true)
    skip50=$(grep -c '^--- SKIP' "$guard_input" || true)
    printf '{"step":"05-pg50-summary","mandatory":"26+14+12=52 via three guards (P22 file: 12 names, incl. 2 migration-concurrency pinned)","guard_input":"%s","top_RUN":%s,"top_PASS":%s,"SKIP":%s}\n' \
      "${guard_input#$EV/}" "$run50" "$pass50" "$skip50" >> "$MF"
    echo "[formal] pg50 RUN=$run50 PASS=$pass50 SKIP=$skip50 (phase rc=$rc)" >&2
    exit $rc
    ;;
  p1pre9)
    step 06-p1-pre9 bash -o pipefail "$P1/run.sh" smoke; rc=$?
    cp "$TMP/result.json" "$LAST_STEP_DIR/result.json"
    python3 -c "import json;d=json.load(open('$LAST_STEP_DIR/result.json'));print('pre9',d['counts'],'uncovered',len(d.get('uncovered',[])))" >&2 || true
    exit $rc
    ;;
  aux-up)
    step 07-aux-up sh "$P22/p22_connector2.sh" up
    exit $?
    ;;
  contract)
    step 08-contract python3 -I "$P22/smoke_p22.py" --env-dir "$TMP" --contract-probe
    exit $?
    ;;
  p22)
    step 09-p22-D-N python3 -I "$P22/smoke_p22.py" --env-dir "$TMP"; rc=$?
    cp "$TMP/result_p22.json" "$LAST_STEP_DIR/" 2>/dev/null || true
    exit $rc
    ;;
  p21abc)
    step 10-p21-ABC python3 -I "$ROOT/tools/smoke/business-platform-p2-1/smoke_p21.py" \
      --env-dir "$TMP"; rc=$?
    cp "$TMP/result_p21.json" "$LAST_STEP_DIR/" 2>/dev/null || true
    exit $rc
    ;;
  restart3)
    step 11-restart3 compose restart dataset pipeline connector; rc=$?
    [ $rc -ne 0 ] && exit $rc
    step 11-restart3-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only
    exit $?
    ;;
  reread)
    before=$(cat "$EV/00-baseline-path.txt" 2>/dev/null) || true
    if [ -z "$before" ] || [ ! -f "$before/manifest.json" ]; then
      echo "[formal] reread: missing 00-baseline-path.txt — run baseline first" >&2
      exit 3
    fi
    step 12-reread python3 -I "$P22/readonly_baseline.py" --env-dir "$TMP"; rc=$?
    after=$(latest_baseline_dir)
    printf '%s\n' "$after" > "$EV/12-reread-path.txt"
    cp "$after/manifest.json" "$EV/12-reread/baseline-after.json" 2>/dev/null \
      || cp "$after/manifest.json" "$LAST_STEP_DIR/baseline-after.json"
    [ $rc -ne 0 ] && exit $rc
    # FIXED output location: $LAST_STEP_DIR expands too early (before the
    # step creates its dir), so the compare result goes to a stable dir
    # the finalizer reads — matching the manifest-recorded reality.
    mkdir -p "$EV/12-reread-compare-output"
    step 12-reread-compare python3 -I "$P22/compare_baseline.py" \
      --before "$before/manifest.json" \
      --after "$EV/12-reread/baseline-after.json" \
      --out "$EV/12-reread-compare-output/compare.json"
    exit $?
    ;;
  p1post9)
    step 13-p1-post9 bash -o pipefail "$P1/run.sh" smoke; rc=$?
    cp "$TMP/result.json" "$LAST_STEP_DIR/result.json"
    python3 -c "import json;d=json.load(open('$LAST_STEP_DIR/result.json'));print('post9',d['counts'],'uncovered',len(d.get('uncovered',[])))" >&2 || true
    exit $rc
    ;;
  aux-down)
    step 99-aux-down sh "$P22/p22_connector2.sh" down
    exit $?
    ;;
  finalize)
    python3 - "$EV" <<'PY'
import json, os, glob, sys
ev = sys.argv[1]
raw = f"{ev}/manifest.jsonl"
# raw jsonl: the first TWO legacy lines were written by the rev1 printf
# bug with a trailing '}}' — the raw file stays byte-identical; this
# DERIVED index repairs parseability and is what tooling consumes.
fixed, bad = [], 0
for line in open(raw):
    line = line.strip()
    if not line:
        continue
    try:
        fixed.append(json.loads(line))
    except ValueError:
        fixed.append(json.loads(line[:-1]))  # drop the single extra '}'
        bad += 1
with open(f"{ev}/manifest-corrected.jsonl", "w") as fh:
    for rec in fixed:
        fh.write(json.dumps(rec) + "\n")

def last_attempt(name):  # latest attempt of a step (collision -rN aware)
    hits = [s for s in fixed if s.get("step") == name and "rc" in s]
    return hits[-1] if hits else None

REQUIRED = ["02-up", "03-fixture-restart", "00-baseline",
            "00b-upgrade-compare", "01-build-final", "01b-swap-guard",
            "01b-swap-stop", "01b-swap-up-base", "01b-swap-up-aux",
            "01b-swap-health", "01b-swap-verify", "04-unit",
            "05-pg50-migrate", "05-pg50-guard-p21", "05-pg50-guard-p22",
            "06-p1-pre9", "07-aux-up", "08-contract", "09-p22-D-N",
            "10-p21-ABC", "11-restart3", "11-restart3-health", "12-reread",
            "12-reread-compare", "13-p1-post9", "99-aux-down"]
step_rc = {}
missing = []
for name in REQUIRED:
    s = last_attempt(name)
    if s is None:
        missing.append(name)
    else:
        step_rc[name] = s["rc"]

def read_json(path):
    try:
        return json.load(open(path))
    except Exception:
        return None

def step_file(name, fname):
    s = last_attempt(name)
    if not s:
        return None
    return os.path.join(ev, s.get("dir", ""), fname)

verdicts = {}
for label, name, fname, check in (
        ("p1_pre9_full", "06-p1-pre9", "result.json",
         lambda d: d and d.get("counts", {}).get("pass") == 9
         and d.get("counts", {}).get("fail") == 0
         and not d.get("uncovered")),
        ("p1_post9_full", "13-p1-post9", "result.json",
         lambda d: d and d.get("counts", {}).get("pass") == 9
         and d.get("counts", {}).get("fail") == 0
         and not d.get("uncovered")),
        ("p22_official_11", "09-p22-D-N", "result_p22.json",
         lambda d: d and d.get("official") is True
         and d.get("partial") is False
         and d.get("counts", {}).get("fail") == 0
         and d.get("counts", {}).get("pass") == 11),
        ("p21_abc_full", "10-p21-ABC", "result_p21.json",
         lambda d: d and d.get("official") is True
         and d.get("counts", {}).get("fail") == 0
         and d.get("counts", {}).get("pass") == 3)):
    d = read_json(step_file(name, fname)) if step_file(name, fname) else None
    verdicts[label] = {"ok": bool(check(d)),
                       "detail": (None if d is None else
                                  {"counts": d.get("counts"),
                                   "official": d.get("official"),
                                   "uncovered": len(d.get("uncovered", [])
                                                    or [])})}
for label, path in (
        ("upgrade_compare_ok",
         f"{ev}/00b-upgrade-compare-output/compare-upgrade.json"),
        ("restart_compare_ok",
         f"{ev}/12-reread-compare-output/compare.json")):
    # BOTH consumers read the FIXED output locations the producer steps
    # actually write (reread's --out goes to 12-reread-compare-output/;
    # never resolve through the recorded step dir).
    d = read_json(path)
    verdicts[label] = {"ok": bool(d and d.get("ok") is True)}

# secret scan: (1) no .env VALUE anywhere; (2) VALUE-level patterns —
# raw JWTs, private-key blocks, lease token/holder VALUES, DSN
# passwords — across the whole archive incl. standalone/ (field NAMES
# in test-expected contexts are allowed; VALUES are never). Counts by
# kind+file only; values are never echoed.
import re as _re
envf = os.path.join(os.environ.get(
    "OF_P1_TMP", "/private/tmp/openfoundry-platform-p1-q3v9"), ".env")
vals = [l.split("=", 1)[1].strip() for l in open(envf)
        if "=" in l and l.split("=", 1)[1].strip()]
_pat = {
    "jwt": _re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}"),
    "private_key": _re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "lease_value": _re.compile(r"(lease_token|lease_holder)[\"'\s:=]+[A-Za-z0-9._\-]{8,}"),
    # real DSN passwords only — ${VAR} placeholders AND our own
    # <redacted> scrub marker excluded
    "dsn_password": _re.compile(
        r"postgres(?:ql)?://[^:/\s]+:(?!\$\{|<redacted>)[^@\s]+@"),
}
leaks = []
pattern_hits = []
for path in glob.glob(f"{ev}/**/*", recursive=True):
    if not os.path.isfile(path):
        continue
    try:
        blob = open(path, errors="replace").read()
    except Exception:
        continue
    for v in vals:
        if v and v in blob:
            leaks.append({"file": os.path.relpath(path, ev),
                          "env_key_value_len": len(v)})
    for kind, rx in _pat.items():
        n = len(rx.findall(blob))
        if n:
            pattern_hits.append({"file": os.path.relpath(path, ev),
                                 "kind": kind, "count": n})
json.dump({"secret_scan_leaks": leaks,
           "value_pattern_hits": pattern_hits},
          open(f"{ev}/secret-scan.json", "w"), indent=2)

all_steps_rc0 = (not missing and all(v == 0 for v in step_rc.values()))
all_verdicts = all(v["ok"] for v in verdicts.values())
# value-level pattern hits GATE the verdict exactly like env leaks
final_ok = all_steps_rc0 and all_verdicts and not leaks and not pattern_hits
json.dump({"client_date": "2026-11-14",
           "note": "steps from manifest-corrected.jsonl; raw manifest.jsonl "
                   "kept byte-identical (rev1 legacy lines had one extra '}')",
           "steps": fixed,
           "required_step_rc": step_rc,
           "missing_steps": missing,
           "content_verdicts": verdicts,
           "secret_scan": {"leaks": len(leaks),
                           "value_pattern_hits": len(pattern_hits)},
           "all_required_rc0": all_steps_rc0,
           "all_content_verdicts_ok": all_verdicts,
           "FINAL_OK": final_ok,
           "legacy_broken_lines_repaired": bad},
          open(f"{ev}/manifest.json", "w"), indent=2)

# whole-archive hash manifest — generated LAST, only after secret-scan
# and manifest.json are fully final (no stale-digest luck involved)
manifest_hashes = {}
for path in sorted(glob.glob(f"{ev}/**/*", recursive=True)):
    if os.path.isfile(path) and not path.endswith("hash-manifest.json"):
        manifest_hashes[os.path.relpath(path, ev)] = \
            __import__("hashlib").sha256(open(path, "rb").read()).hexdigest()
json.dump({"client_date": "2026-11-14", "files": manifest_hashes},
          open(f"{ev}/hash-manifest.json", "w"), indent=2)
print("finalize: FINAL_OK =", final_ok,
      "| missing:", missing,
      "| rc!=0:", [k for k, v in step_rc.items() if v != 0],
      "| verdicts bad:", [k for k, v in verdicts.items() if not v["ok"]],
      "| secret leaks:", len(leaks),
      "| value pattern hits:", len(pattern_hits))
sys.exit(0 if final_ok else 1)
PY
    ;;
  *)
    sed -n '2,12p' "$0" >&2
    grep -o '^  [a-z0-9-]*)' "$0" | tr -d ' )' >&2
    exit 2 ;;
esac
