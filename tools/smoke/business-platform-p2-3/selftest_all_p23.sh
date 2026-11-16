#!/bin/sh
#
# P2.3 preparation self-test driver — LOCAL ONLY: syntax checks and the
# guard/snapshot/archive helper selftests. No stack, no services, no
# docker, no q3v9 access of any kind. Raw outputs plus real exit codes
# are archived under results/preparation/ (originals only, nothing
# reconstructed). A failing check fails the overall run.
#
# Usage: sh selftest_all_p23.sh   (exit 0 iff every check passed)
#
set -eu
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"   # root batch-G: safe to invoke from anywhere (e.g. $ROOT)
STAMP="$(date +%Y-%m-%dT%H%M%S)"
OUT="$SCRIPT_DIR/results/preparation/${STAMP}-p23-local-selftest"
mkdir -p "$OUT"

overall=0

# run <name> <command...>: capture RAW stdout+stderr, the exit code, and
# the exact command line into the archive.
run() {
  name="$1"; shift
  log="$OUT/$name.log"
  printf 'cmd: %s\n' "$*" > "$OUT/$name.cmd"
  set +e
  "$@" > "$log" 2>&1
  rc=$?
  set -e
  printf '%s' "$rc" > "$OUT/$name.rc"
  if [ "$rc" -ne 0 ]; then
    overall=1
    printf 'FAIL %s (rc=%s) -> %s\n' "$name" "$rc" "$log"
  else
    printf 'ok   %s (rc=0)\n' "$name"
  fi
}

run sh_n_guard          sh -n run_tests_p23.sh
run sh_n_guard_selftest sh -n selftest_guard_p23.sh
run bash_n_formal       bash -n formal_run_p23.sh   # bash script (batch-G)
run py_compile_smoke    python3 -m py_compile smoke_p23.py
run py_compile_snap     python3 -m py_compile admission_snapshots.py
run py_compile_archive  python3 -m py_compile archive_standalone_p23.py
run py_compile_snapsel  python3 -m py_compile selftest_snapshots.py
run py_compile_locksel  python3 -m py_compile selftest_lock_holder.py
run py_compile_liveguard python3 -m py_compile p23_live_guard.py
run py_compile_livesel  python3 -m py_compile selftest_live_guard.py
run guard_selftest      sh selftest_guard_p23.sh
run snapshots_selftest  python3 -I selftest_snapshots.py
run lock_holder_selftest python3 -I selftest_lock_holder.py
run live_guard_selftest python3 -I selftest_live_guard.py
run archive_selftest    python3 -I archive_standalone_p23.py --selftest
run guard_only_dryrun   sh run_tests_p23.sh guard-only

rm -rf "$SCRIPT_DIR/__pycache__"

# manifest: client-local date, commands, exit codes, per-log sha256 —
# written from the REAL files only.
CLIENT_DATE="$(date +%F)"
{
  printf '{\n'
  printf '  "client_date": "%s",\n' "$CLIENT_DATE"
  printf '  "note": "local-only preparation selftest (syntax + helper logic); no stack, no docker, no q3v9; raw logs only",\n'
  printf '  "overall_rc": %s,\n' "$overall"
  printf '  "checks": [\n'
  first=1
  for rcfile in "$OUT"/*.rc; do
    name="$(basename "$rcfile" .rc)"
    rc="$(cat "$rcfile")"
    cmd="$(cat "$OUT/$name.cmd" 2>/dev/null | sed 's/^cmd: //')"
    sha="$(shasum -a 256 "$OUT/$name.log" | cut -d" " -f1)"
    if [ "$first" -eq 1 ]; then first=0; else printf ',\n'; fi
    printf '    {"name": "%s", "rc": %s, "cmd": "%s", "log_sha256": "%s"}' \
      "$name" "$rc" "$cmd" "$sha"
  done
  printf '\n  ]\n}\n'
} > "$OUT/manifest.json"

printf '\n[p23-selftest-all] overall=%s evidence=%s\n' "$overall" "$OUT"
exit "$overall"
