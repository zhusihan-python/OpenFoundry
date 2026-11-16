#!/bin/bash
# P2.3 FORMAL acceptance orchestrator (skeleton 2026-11-14; NOT released
# for execution — root formal release required first; mutation phases are
# interlocked behind P23_CONFIRM=yes).
#
# Discipline (inherited from the audited P2.2 formal_run.sh):
#   * every step captures the REAL rc via direct redirection (no tee);
#     steps that wrap run.sh (which tees internally) use bash -o pipefail
#   * collision-safe logs/dirs: reruns append -rN, history never overwritten
#   * each phase EXITS WITH THE STEP'S REAL RC; manifest.jsonl records
#     {step,rc,seconds,cmd,log,dir}
#   * phase JSON artifacts and per-run suffixes stay separate; historical
#     FAIL/partial attempts are preserved verbatim; finalize selects the
#     LATEST attempt of each required step and refuses partial results
#   * the whole-archive hash manifest is generated LAST, after the secret
#     scan and the final manifest are complete
#   * the fixture is NEVER restarted; PG/file volumes and .env(600) are
#     never touched; P21 legacy rows are never cancelled/rewritten
set -uo pipefail

ROOT="/Volumes/ExternalSD/projects/OpenFoundry"
P1="$ROOT/tools/smoke/business-platform-p1"
P21="$ROOT/tools/smoke/business-platform-p2-1"
P22="$ROOT/tools/smoke/business-platform-p2-2"
P23="$ROOT/tools/smoke/business-platform-p2-3"
TMP="${OF_P1_TMP:-/private/tmp/openfoundry-platform-p1-q3v9}"
PROJ="openfoundry-platform-p1-q3v9"
EV="$P23/results/$(date +%F)-p23-final"
MF="$EV/manifest.jsonl"
mkdir -p "$EV"
export P22_CONFIRM_CONNECTOR2=yes   # frozen P2.2 aux driver interlock
cd "$ROOT"

compose() { docker compose -f "$TMP/compose.yaml" --project-name "$PROJ" "$@"; }
compose_aux() { docker compose -f "$TMP/compose.yaml" \
  -f "$TMP/compose-p22-connector2.yaml" --project-name "$PROJ" "$@"; }

require_confirm() {
  if [ "${P23_CONFIRM:-}" != "yes" ]; then
    echo "[p23-formal] refusing mutation phase '$1' without " >&2
    echo "P23_CONFIRM=yes — formal execution needs the root release." >&2
    exit 1
  fi
}

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
  echo "[p23-formal] START $name: $*" | tee -a "$EV/console.log" >&2
  ( "$@" ) >"$log" 2>&1 < /dev/null
  rc=$?
  dur=$(( $(date +%s) - t0 ))
  printf '{"step":"%s","rc":%d,"seconds":%d,"cmd":%s,"log":"%s","dir":"%s/"}\n' \
    "$name" "$rc" "$dur" \
    "$(printf '%s' "$*" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')" \
    "${log#$EV/}" "${dir#$EV/}" >> "$MF"
  echo "[p23-formal] END $name rc=$rc ${dur}s log=${log#$EV/}" | tee -a "$EV/console.log" >&2
  LAST_STEP_LOG="$log"; LAST_STEP_DIR="$dir"
  return $rc
}

latest_baseline_dir() {
  ls -d "$P22"/results/preparation/*-readonly-baseline 2>/dev/null | tail -1
}

# Freshness rule (root batch-2): a "latest glob" may ONLY be used when
# THIS step demonstrably produced a NEW output dir — a stale dir from an
# earlier failed/partial attempt must never be mistaken for this run's
# result. Prints the single new dir on stdout, empty on failure.
new_baseline_dir() { # <pre-list-file> <post-list-file>
  comm -13 "$1" "$2" | tail -1
}

case "${1:-}" in
  local-selftest)
    # SAFE NOW: local syntax + helper selftests only (no stack, no docker)
    step 00-local-selftest bash "$P23/selftest_all_p23.sh"
    exit $?
    ;;
  baseline)
    # freshness: only a dir THIS step created may become the baseline
    ls -d "$P22"/results/preparation/*-readonly-baseline 2>/dev/null | sort \
      > "$TMP/.p23-baseline-pre.txt" || true
    step 00-baseline python3 -I "$P22/readonly_baseline.py" --env-dir "$TMP"; rc=$?
    ls -d "$P22"/results/preparation/*-readonly-baseline 2>/dev/null | sort \
      > "$TMP/.p23-baseline-post.txt" || true
    new_dir=$(new_baseline_dir "$TMP/.p23-baseline-pre.txt" "$TMP/.p23-baseline-post.txt")
    if [ -z "$new_dir" ] || [ ! -f "$new_dir/manifest.json" ]; then
      echo "[p23-formal] baseline: no NEW readonly-baseline dir produced by THIS step — refusing to fall back to a stale glob" >&2
      exit 3
    fi
    printf '%s\n' "$new_dir" > "$EV/00-baseline-path.txt"   # fresh truncate
    cp "$new_dir/manifest.json" "$EV/00-baseline-manifest.json"
    exit $rc
    ;;
  build-final)
    require_confirm build-final
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
    # closeout-3: render must REALLY succeed and produce the override —
    # a failed render must never fall through to a stale override file
    # from an earlier round
    if ! bash "$P22/p22_connector2.sh" render; then
      echo "[p23-formal] build-final: override render FAILED — refusing a stale override" >&2
      printf '{"step":"01-build-final","rc":6,"error":"override render failed"}\n' >> "$MF"
      exit 6
    fi
    [ -f "$TMP/compose-p22-connector2.yaml" ] || {
      echo "[p23-formal] build-final: override file missing after render" >&2
      printf '{"step":"01-build-final","rc":6,"error":"override file missing after render"}\n' >> "$MF"
      exit 6
    }
    {
      echo "## cmd: compose -f base -f override build connector2 connector3"
      docker compose -f "$TMP/compose.yaml" -f "$TMP/compose-p22-connector2.yaml" \
        --project-name "$PROJ" build connector2 connector3 2>&1
      echo "rc=$?"
    } > "$bdir/build-aux2.log"
    [ "$(sed -n 's/^rc=//p' "$bdir/build-aux2.log" | tail -1)" = "0" ] || rc=1
    docker images --format '{{.Repository}} {{.ID}}' \
      | grep "^$PROJ-" > "$bdir/docker-images.txt"
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
print("[p23-build] image ids:", ids, "| missing:", missing)
raise SystemExit(1 if missing else 0)
PY
    brc=$?
    printf '{"step":"01-build-final","rc":%d,"images":"%s/image-ids.json"}\n' \
      "$(( rc | brc ))" "${bdir#$EV/}" >> "$MF"
    git -C "$ROOT" rev-parse HEAD > "$bdir/git-head.txt"
    echo "[p23-formal] build-final rc=$(( rc | brc )) dir=${bdir#$EV/}" >&2
    exit $(( rc | brc ))
    ;;
  swap)
    require_confirm swap
    bf_ids=$(ls -t "$EV"/01-build-final*/image-ids.json 2>/dev/null | head -1)
    if [ -z "$bf_ids" ] || [ ! -f "$bf_ids" ]; then
      echo "[p23-formal] swap: no 01-build-final*/image-ids.json — run build-final first" >&2
      exit 3
    fi
    step 01b-swap-guard python3 -I "$P23/p23_live_guard.py" "$TMP" connector
    rc=$?; [ $rc -ne 0 ] && { echo "[p23-formal] swap: live-guard refused (unaccounted/drifted controlled rows) — abort; raw provenance: 01b-swap-guard/investigation/" >&2; exit $rc; }
    # Aux tolerance (root batch-2): the P22 close-out left the aux
    # replicas DOWN/removed — an absent aux service is a recorded safe
    # state, not an error; only PRESENT containers must stop gracefully.
    present_svcs=""
    for svc in connector connector2 connector3; do
      cid=$(compose_aux ps -a -q "$svc" 2>/dev/null | head -1)
      [ -n "$cid" ] && present_svcs="$present_svcs $svc"
    done
    [ -n "$present_svcs" ] || {
      echo "[p23-formal] swap: no connector replica present at all" >&2
      exit 4; }
    # shellcheck disable=SC2086
    step 01b-swap-stop compose_aux stop -t 120 $present_svcs
    rc=$?; [ $rc -ne 0 ] && exit $rc
    exits="$LAST_STEP_DIR/exits.txt"      # per-attempt dir: no append reuse
    : > "$exits"
    n_present=0
    for svc in $present_svcs; do
      cid=$(compose_aux ps -a -q "$svc" | head -1)
      if [ -z "$cid" ]; then
        echo "$svc vanished-between-stop-and-inspect" >> "$exits"
        continue
      fi
      docker inspect --format "$svc {{.State.Running}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.FinishedAt}}" \
        "$cid" >> "$exits" || exit 4
      n_present=$((n_present + 1))
    done
    for svc in connector connector2 connector3; do
      case " $present_svcs " in *" $svc "*) ;; *)
        echo "$svc absent(aux-down-safe-state)" >> "$exits" ;; esac
    done
    graceful=$(grep -c "|0|false|" "$exits" || true)
    [ "$graceful" -eq "$n_present" ] && [ "$n_present" -ge 1 ] || {
      echo "[p23-formal] swap: non-graceful old exit (graceful=$graceful present=$n_present)" >&2
      cat "$exits" >&2; exit 5; }
    # recreate: base three then the two aux replicas (both --no-deps; the
    # fixture and postgres are NEVER restarted by this flow)
    step 01b-swap-up-base compose up -d --no-deps dataset pipeline connector
    rc=$?; [ $rc -ne 0 ] && exit $rc
    step 01b-swap-up-aux compose_aux up -d --no-deps connector2 connector3
    rc=$?; [ $rc -ne 0 ] && exit $rc
    step 01b-swap-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only
    rc=$?; [ $rc -ne 0 ] && exit $rc
    step 01b-swap-verify python3 -I "$P22/swap_verify.py" "$TMP" "$bf_ids"
    exit $?
    ;;
  unit)
    step 04-unit bash -o pipefail "$P1/run.sh" tests; rc=$?
    ok=$(grep -c '^ok ' "$LAST_STEP_LOG" || true)
    fail=$(grep -c '^FAIL' "$LAST_STEP_LOG" || true)
    skips=$(grep -c '^--- SKIP' "$LAST_STEP_LOG" || true)
    printf '{"step":"04-unit-summary","ok_packages":%s,"FAIL_lines":%s,"SKIP_lines":%s}\n' \
      "$ok" "$fail" "$skips" >> "$MF"
    echo "[p23-formal] unit ok=$ok FAIL=$fail SKIP=$skips (actual counts; 61 is the P22 baseline, not a hard pin)" >&2
    exit $rc
    ;;
  pg59)
    require_confirm pg59
    mkdir -p "$EV/05-pg59"
    step 05-pg59-migrate bash -o pipefail "$P1/run.sh" migrate-tests; rc=$?
    guard_input="$EV/05-pg59/migrate-tests-$(date +%Y%m%dT%H%M%S).log"
    cp "$TMP/logs/migrate-tests.log" "$guard_input"
    if [ $rc -ne 0 ]; then echo "[p23-formal] pg59: migrate rc=$rc, guards skipped" >&2; exit $rc; fi
    step 05-pg59-guard-p21 sh "$P21/run_tests_p21.sh" integration "$guard_input" || rc=$?
    step 05-pg59-guard-p22 sh "$P22/run_tests_p22.sh" integration "$guard_input" || rc=$?
    step 05-pg59-guard-p23 sh "$P23/run_tests_p23.sh" integration "$guard_input" || rc=$?
    # closeout-2: EXACT per-name statistics over the four guard name
    # lists — each of the 59 distinct top-level names must appear with
    # exactly 1 top-level RUN, 1 top-level PASS and 0 SKIP. The raw
    # `^=== RUN` line count includes SUBTESTS and is recorded ONLY as
    # run_lines_total (never claimed as top-level). The checker is
    # written to a FILE first: step() redirects stdin to /dev/null, so a
    # heredoc can never reach the command through it.
    pgx_script="$EV/05-pg59/pg59_exact-$(date +%Y%m%dT%H%M%S).py"
    cat > "$pgx_script" <<'PY'
import re, sys
log_path = sys.argv[1]
text = open(log_path, errors="replace").read()
names = []
for path, var in ((sys.argv[2], "REQUIRED_DATASET"),
                  (sys.argv[2], "REQUIRED_PIPELINE"),
                  (sys.argv[2], "REQUIRED_CONNECTOR"),
                  (sys.argv[3], "REQUIRED_LEASE")):
    m = re.search(rf'^{var}="([^"]+)"', open(path).read(), re.M)
    if not m:
        raise SystemExit(f"closeout-2: {var} not found in {path}")
    names.extend(m.group(1).split())
for path in (sys.argv[4], sys.argv[5]):
    for ln in open(path):
        ln = ln.split("#")[0].strip()
        if ln:
            names.append(ln)
bad, seen = [], set()
for n in names:
    if n in seen:
        bad.append(f"duplicate name {n}")
        continue
    seen.add(n)
    run = len(re.findall(rf"^=== RUN\s+{re.escape(n)}(\s|$)", text, re.M))
    passed = len(re.findall(rf"^--- PASS:\s+{re.escape(n)}(\s|\(|$)", text, re.M))
    skipped = len(re.findall(rf"^--- SKIP:\s+{re.escape(n)}(\s|\(|$)", text, re.M))
    if run != 1 or passed != 1 or skipped != 0:
        bad.append(f"{n}: RUN={run} PASS={passed} SKIP={skipped}")
print(f"[pg59-exact] distinct={len(seen)} bad={len(bad)}")
for b in bad[:10]:
    print("  ", b)
raise SystemExit(1 if bad or len(seen) != 59 else 0)
PY
    step 05-pg59-exact python3 -I "$pgx_script" "$guard_input" \
      "$P1/run_tests.sh" "$P21/run_tests_p21.sh" \
      "$P22/p22-required-names.txt" "$P23/p23-required-names.txt"
    pgx_rc=$?
    [ $pgx_rc -ne 0 ] && rc=$pgx_rc
    [ $pgx_rc -ne 0 ] && rc=$pgx_rc
    run_total=$(grep -c '^=== RUN' "$guard_input" || true)
    pass_total=$(grep -c '^--- PASS' "$guard_input" || true)
    skip_total=$(grep -c '^--- SKIP' "$guard_input" || true)
    printf '{"step":"05-pg59-summary","mandatory":"26+14+12+7=59, four guard lists, per-name exact 1RUN/1PASS/0SKIP enforced by 05-pg59-exact","guard_input":"%s","run_lines_total_note":"includes subtest lines, NOT a top-level count","run_lines_total":%s,"pass_lines_total":%s,"skip_lines_total":%s}\n' \
      "${guard_input#$EV/}" "$run_total" "$pass_total" "$skip_total" >> "$MF"
    echo "[p23-formal] pg59 exact-59 rc=$pgx_rc (raw RUN lines=$run_total incl subtests)" >&2
    exit $rc
    ;;
  p1pre9)
    step 06-p1-pre9 bash -o pipefail "$P1/run.sh" smoke; rc=$?
    cp "$TMP/result.json" "$LAST_STEP_DIR/result.json"
    exit $rc
    ;;
  aux-up)
    require_confirm aux-up
    step 07-aux-up sh "$P22/p22_connector2.sh" up
    exit $?
    ;;
  contract)
    step 08-contract python3 -I "$P23/smoke_p23.py" --env-dir "$TMP" --contract-probe
    exit $?
    ;;
  p23)
    require_confirm p23
    step 09-p23-scenarios python3 -I "$P23/smoke_p23.py" \
      --env-dir "$TMP" --allow-restart; rc=$?
    [ $rc -ne 0 ] && exit $rc
    # closeout-1: snapshot copy/verification binds to THIS run's
    # result_p23.json suffix — only snapshots_latest/<suffix> is the
    # gating set; the WHOLE TMP history is archived separately and can
    # neither press an old bad pair into the verdict nor pad an old
    # green pair into it
    cp "$TMP/result_p23.json" "$LAST_STEP_DIR/" || exit 4
    suffix=$(python3 -c "import json;print(json.load(open('$TMP/result_p23.json'))['suffix'])") || exit 4
    [ -n "$suffix" ] || { echo "[p23-formal] p23: result_p23.json carries no suffix" >&2; exit 4; }
    printf '%s\n' "$suffix" > "$LAST_STEP_DIR/snapshot-suffix.txt"
    mkdir -p "$LAST_STEP_DIR/snapshots_latest"
    cp -R "$TMP/snapshots_p23/$suffix" "$LAST_STEP_DIR/snapshots_latest/" || exit 4
    cp -R "$TMP/snapshots_p23" "$LAST_STEP_DIR/snapshots_history" || exit 4
    exit $rc
    ;;
  p22full)
    require_confirm p22full
    step 10-p22-full python3 -I "$P22/smoke_p22.py" --env-dir "$TMP"; rc=$?
    cp "$TMP/result_p22.json" "$LAST_STEP_DIR/" 2>/dev/null || true
    exit $rc
    ;;
  p21abc)
    step 10b-p21-ABC python3 -I "$P21/smoke_p21.py" --env-dir "$TMP"; rc=$?
    cp "$TMP/result_p21.json" "$LAST_STEP_DIR/" 2>/dev/null || true
    exit $rc
    ;;
  restart3)
    require_confirm restart3
    step 11-restart3 compose restart dataset pipeline connector; rc=$?
    [ $rc -ne 0 ] && exit $rc
    step 11-restart3-health python3 -I "$P1/smoke/smoke.py" --env-dir "$TMP" --wait-only
    exit $?
    ;;
  reread)
    before=$(cat "$EV/00-baseline-path.txt" 2>/dev/null) || true
    if [ -z "$before" ] || [ ! -f "$before/manifest.json" ]; then
      echo "[p23-formal] reread: missing 00-baseline-path.txt — run baseline first" >&2
      exit 3
    fi
    ls -d "$P22"/results/preparation/*-readonly-baseline 2>/dev/null | sort \
      > "$TMP/.p23-reread-pre.txt" || true
    step 12-reread python3 -I "$P22/readonly_baseline.py" --env-dir "$TMP"; rc=$?
    ls -d "$P22"/results/preparation/*-readonly-baseline 2>/dev/null | sort \
      > "$TMP/.p23-reread-post.txt" || true
    after=$(new_baseline_dir "$TMP/.p23-reread-pre.txt" "$TMP/.p23-reread-post.txt")
    if [ -z "$after" ] || [ ! -f "$after/manifest.json" ]; then
      echo "[p23-formal] reread: no NEW baseline dir produced by THIS step — refusing stale glob" >&2
      exit 3
    fi
    printf '%s\n' "$after" > "$EV/12-reread-path.txt"
    # per-attempt dir (collision-safe -rN): a failed cp aborts — never a
    # stale file from an earlier attempt at a fixed path
    cp "$after/manifest.json" "$LAST_STEP_DIR/baseline-after.json" || exit 4
    [ $rc -ne 0 ] && exit $rc
    cmp_dir="$EV/12-reread-compare-output"; n=0
    while [ -e "$cmp_dir" ]; do n=$((n+1)); cmp_dir="$EV/12-reread-compare-output-r$n"; done
    mkdir -p "$cmp_dir"
    step 12-reread-compare python3 -I "$P22/compare_baseline.py" \
      --before "$before/manifest.json" \
      --after "$LAST_STEP_DIR/baseline-after.json" \
      --out "$cmp_dir/compare.json" \
      --mode restart; rc=$?
    printf '%s\n' "$cmp_dir" > "$EV/12-reread-compare-path.txt"  # fresh truncate
    exit $rc
    ;;
  post-images)
    # root closeout-6 wording: the frozen J's up-deadline/restore only
    # does `compose up` with different env — a RESHAPE, not a rebuild
    # (no --build). Run AFTER 10-p22-full so the FINAL image ownership
    # of every connector replica is verified here (swap_verify.py).
    require_confirm post-images
    bf_ids=$(ls -t "$EV"/01-build-final*/image-ids.json 2>/dev/null | head -1)
    if [ -z "$bf_ids" ] || [ ! -f "$bf_ids" ]; then
      echo "[p23-formal] post-images: no 01-build-final*/image-ids.json" >&2
      exit 3
    fi
    step 10c-post-p22-images python3 -I "$P22/swap_verify.py" "$TMP" "$bf_ids"
    exit $?
    ;;
  standalone)
    # new round of root-side raws gets its OWN index (the P2.2 standalone
    # archive stays read-only). P23_STANDALONE_ROOTS: colon-separated dirs.
    require_confirm standalone
    roots="${P23_STANDALONE_ROOTS:?export P23_STANDALONE_ROOTS=dir[:dir2...]}"
    args=""
    IFS=':' ; for r in $roots; do args="$args --src-root $r"; done; unset IFS
    step 90-standalone python3 -I "$P23/archive_standalone_p23.py" \
      $args --dst "$EV/standalone"
    exit $?
    ;;
  p1post9)
    step 13-p1-post9 bash -o pipefail "$P1/run.sh" smoke; rc=$?
    cp "$TMP/result.json" "$LAST_STEP_DIR/result.json"
    exit $rc
    ;;
  aux-down)
    require_confirm aux-down
    step 99-aux-down sh "$P22/p22_connector2.sh" down
    exit $?
    ;;
  finalize)
    python3 - "$EV" <<'PY'
import glob, json, os, re, sys
ev = sys.argv[1]

raw = f"{ev}/manifest.jsonl"
fixed = []
for line in open(raw):
    line = line.strip()
    if not line:
        continue
    try:
        fixed.append(json.loads(line))
    except ValueError:
        raise SystemExit(f"finalize: unparsable manifest line (no repair "
                         f"loop in P2.3 — the writer is single-format): "
                         f"{line[:80]}")

def last_attempt(name):
    hits = [s for s in fixed if s.get("step") == name and "rc" in s]
    return hits[-1] if hits else None

REQUIRED = ["00-baseline", "01-build-final", "01b-swap-guard",
            "01b-swap-stop", "01b-swap-up-base", "01b-swap-up-aux",
            "01b-swap-health", "01b-swap-verify", "04-unit",
            "05-pg59-migrate", "05-pg59-guard-p21", "05-pg59-guard-p22",
            "05-pg59-guard-p23", "05-pg59-exact", "06-p1-pre9",
            "07-aux-up", "08-contract", "09-p23-scenarios", "10-p22-full",
            "10c-post-p22-images", "10b-p21-ABC", "11-restart3",
            "11-restart3-health", "12-reread", "12-reread-compare",
            "13-p1-post9", "90-standalone", "99-aux-down"]
step_rc, missing = {}, []
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
    return os.path.join(ev, s.get("dir", ""), fname) if s else None

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
        ("p23_official_8", "09-p23-scenarios", "result_p23.json",
         lambda d: d and d.get("official") is True
         and d.get("partial") is False
         and d.get("counts", {}).get("fail") == 0
         and d.get("counts", {}).get("pass") == 8),
        ("p22_full_official_11", "10-p22-full", "result_p22.json",
         lambda d: d and d.get("official") is True
         and d.get("partial") is False
         and d.get("counts", {}).get("fail") == 0
         and d.get("counts", {}).get("pass") == 11),
        ("p21_abc_full", "10b-p21-ABC", "result_p21.json",
         lambda d: d and d.get("official") is True
         and d.get("counts", {}).get("fail") == 0
         and d.get("counts", {}).get("pass") == 3)):
    path = step_file(name, fname)
    d = read_json(path) if path else None
    verdicts[label] = {"ok": bool(check(d)),
                       "detail": (None if d is None else
                                  {"counts": d.get("counts"),
                                   "official": d.get("official"),
                                   "partial": d.get("partial")})}

d = None
sidecar = f"{ev}/12-reread-compare-path.txt"
if os.path.isfile(sidecar):
    cmp_dir = open(sidecar).read().strip()
    if cmp_dir:
        d = read_json(os.path.join(cmp_dir, "compare.json"))
verdicts["restart_compare_ok"] = {
    "ok": bool(d and d.get("ok") is True),
    "detail": "compare.json located via the fresh sidecar written by the "
              "reread step (never a stale fixed path)"}

# value-level before/after pairing — bound to THIS attempt's
# result_p23.json suffix under snapshots_latest/<suffix>/ only
# (closeout-1: the whole-TMP history lives in snapshots_history and can
# neither press an old bad pair red nor pad an old green pair in). The
# REQUIRED pair is base "o-reject" (before/after/compare) and its
# before-side handles must carry the full expected O universe.
snap_problems = []
pairs = 0
green_bases = set()
handles_ok = False
snap_root = None
latest_p23 = last_attempt("09-p23-scenarios")
if latest_p23:
    base_dir = os.path.join(ev, latest_p23["dir"])
    res_p23 = read_json(os.path.join(base_dir, "result_p23.json"))
    suffix = (res_p23 or {}).get("suffix")
    if not suffix:
        snap_problems.append("result_p23.json carries no suffix")
    else:
        snap_root = os.path.join(base_dir, "snapshots_latest", suffix)
        if not os.path.isdir(snap_root):
            snap_problems.append(f"snapshots_latest/{suffix} missing")
        else:
            names = set(os.listdir(snap_root))
            befores = {n[len("before-"):-len(".json")] for n in names
                       if n.startswith("before-") and n.endswith(".json")}
            afters = {n[len("after-"):-len(".json")] for n in names
                      if n.startswith("after-") and n.endswith(".json")}
            for tag in sorted(befores | afters):
                if tag not in befores:
                    snap_problems.append(f"{tag}: after without before")
                    continue
                if tag not in afters:
                    snap_problems.append(f"{tag}: before without after")
                    continue
                cmpj = read_json(os.path.join(snap_root,
                                              f"compare-{tag}.json"))
                if not cmpj or cmpj.get("ok") is not True:
                    snap_problems.append(f"{tag}: compare missing/not ok")
                else:
                    pairs += 1
                    green_bases.add(tag)
            # required pair + expected handle universe for the O sweep
            if "o-reject" not in green_bases:
                snap_problems.append(
                    "required pair o-reject missing or not green")
            else:
                h = (read_json(os.path.join(snap_root,
                                            "before-o-reject.json"))
                     or {}).get("handles") or {}
                handles_ok = (len(h.get("sources", [])) == 1
                              and len(h.get("syncs", [])) == 3
                              and len(h.get("targets", [])) == 3
                              and len(h.get("runs", [])) == 2)
                if not handles_ok:
                    snap_problems.append(
                        "o-reject handles universe != expected "
                        "(1 source / 3 syncs / 3 targets / 2 runs)")
verdicts["snapshot_pairs_green"] = {
    "ok": (pairs > 0 and not snap_problems and "o-reject" in green_bases
           and handles_ok),
    "detail": {"pairs": pairs, "problems": snap_problems[:10],
               "green_bases": sorted(green_bases),
               "handles_universe_ok": handles_ok,
               "snap_root": snap_root}}

# standalone is REQUIRED (root batch-2): real rc + index content, and
# no missing roots (archive_standalone_p23 already fails on a missing
# root; finalize double-checks the content).
std_step = last_attempt("90-standalone")
std_index = read_json(f"{ev}/standalone/index.json") \
    if os.path.isfile(f"{ev}/standalone/index.json") else None
std_files = (std_index or {}).get("files", [])
std_missing = [f for f in std_files if f.get("missing")]
verdicts["standalone_required"] = {
    "ok": bool(std_step and std_step.get("rc") == 0
               and std_files and not std_missing),
    "detail": {"rc": (std_step or {}).get("rc"),
               "entries": len(std_files),
               "missing_roots": len(std_missing)}}

# secret scan: .env VALUES plus value-level patterns across the whole
# archive (identical policy to the P2.2 finalize; placeholders excluded)
envf = os.path.join(os.environ.get(
    "OF_P1_TMP", "/private/tmp/openfoundry-platform-p1-q3v9"), ".env")
vals = [l.split("=", 1)[1].strip() for l in open(envf)
        if "=" in l and l.split("=", 1)[1].strip()]
_pat = {
    "jwt": re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}"),
    "private_key": re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "lease_value": re.compile(r"(lease_token|lease_holder)[\"'\s:=]+[A-Za-z0-9._\-]{8,}"),
    "dsn_password": re.compile(
        r"postgres(?:ql)?://[^:/\s]+:(?!\$\{|<redacted>)[^@\s]+@"),
}
leaks, pattern_hits = [], []
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
final_ok = all_steps_rc0 and all_verdicts and not leaks and not pattern_hits
json.dump({"client_date": "2026-11-14",
           "steps": fixed,
           "required_step_rc": step_rc,
           "missing_steps": missing,
           "content_verdicts": verdicts,
           "secret_scan": {"leaks": len(leaks),
                           "value_pattern_hits": len(pattern_hits)},
           "all_required_rc0": all_steps_rc0,
           "all_content_verdicts_ok": all_verdicts,
           "FINAL_OK": final_ok},
          open(f"{ev}/manifest.json", "w"), indent=2)

# whole-archive hash manifest — generated LAST (after secret scan and
# manifest.json are final)
import hashlib
manifest_hashes = {}
for path in sorted(glob.glob(f"{ev}/**/*", recursive=True)):
    if os.path.isfile(path) and not path.endswith("hash-manifest.json"):
        manifest_hashes[os.path.relpath(path, ev)] = \
            hashlib.sha256(open(path, "rb").read()).hexdigest()
json.dump({"client_date": "2026-11-14", "files": manifest_hashes},
          open(f"{ev}/hash-manifest.json", "w"), indent=2)
print("finalize: FINAL_OK =", final_ok,
      "| missing:", missing,
      "| rc!=0:", [k for k, v in step_rc.items() if v != 0],
      "| verdicts bad:", [k for k, v in verdicts.items() if not v["ok"]],
      "| snap problems:", snap_problems[:5],
      "| secret leaks:", len(leaks),
      "| value pattern hits:", len(pattern_hits))
sys.exit(0 if final_ok else 1)
PY
    ;;
  *)
    sed -n '2,14p' "$0" >&2
    grep -o '^  [a-z0-9-]*)' "$0" | tr -d ' )' >&2
    exit 2 ;;
esac
