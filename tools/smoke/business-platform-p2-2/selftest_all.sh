#!/bin/sh
#
# P2.2 preparation self-test driver — runs every local (no-stack) check
# and archives the RAW outputs plus command exit codes under
# results/preparation/. Only original logs are archived: nothing is
# filtered, summarised into existence, or reconstructed after the fact.
# A failing command is recorded as-is and fails the overall run.
#
# Usage: sh selftest_all.sh   (exit 0 iff every check passed)
#
set -eu
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# LOCAL timestamp per the root instruction: evidence is dated by the
# client environment (2026-11-14), not UTC.
STAMP="$(date +%Y-%m-%dT%H%M%S)"
OUT="$SCRIPT_DIR/results/preparation/${STAMP}-selftests"
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

run sh_n_guard        sh -n run_tests_p22.sh
run sh_n_selftest     sh -n selftest_guard.sh
run sh_n_driver       sh -n p22_connector2.sh
run py_compile_smoke  python3 -m py_compile smoke_p22.py
run py_compile_proxy  python3 -m py_compile dataset_proxy.py
run guard_selftest    sh selftest_guard.sh
run fixture_selftest  python3 -I selftest_fixture.py
run proxy_selftest    python3 -I selftest_proxy.py

rm -rf "$SCRIPT_DIR/__pycache__"

# manifest: date (client local date per root instruction), commands,
# exit codes, and per-log sha256 — written from the REAL files.
CLIENT_DATE="$(date +%Y-%m-%d)"
{
  printf '{\n'
  printf '  "client_date": "%s",\n' "$CLIENT_DATE"
  printf '  "note": "raw preparation self-test evidence; original logs only, no reconstruction",\n'
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

printf '\n[p22-selftest-all] overall=%s evidence=%s\n' "$overall" "$OUT"
exit "$overall"
