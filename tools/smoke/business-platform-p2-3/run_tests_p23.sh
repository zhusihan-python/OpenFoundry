#!/bin/sh
#
# OpenFoundry P2.3 source-admission — REQUIRED-name guard (standalone; the
# P1 26-name, P2.1 14-name, and P2.2 12-name guards stay frozen/untouched).
#
# The required P2.3 PG names come from p23-required-names.txt —
# ROOT-PINNED 2026-11-14 at 7 distinct top-level names from the dedicated
# writer source_admission_integration_test.go. At pinning time NONE of
# them had RUN/PASSed in any real log: the guard's job is exactly to
# refuse blessing until a real migrate-tests log shows each name with a
# top-level `=== RUN <name>` AND `--- PASS: <name>`.
#
# Final mandatory total = 26 + 14 + 12 + 7 = 59 once genuinely green.
#
# Counting rule: DISTINCT real names (duplicates in the file collapse),
# each needing a TOP-LEVEL `=== RUN <name>` AND `--- PASS: <name>`; a
# child-only `=== RUN <name>/sub` does not count; SKIP > FAIL priority;
# no required SKIP tolerated. A missing/empty names file still FAILS
# CLOSED — an empty guard must never be able to bless a run.
#
set -eu
MODE="${1:-guard-only}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NAMES_FILE="${P23_NAMES_FILE:-$SCRIPT_DIR/p23-required-names.txt}"

log() { printf '[p23-guard] %s\n' "$*"; }

# Print the effective required names, one per line (comments/blanks
# stripped, duplicates collapsed to the FIRST occurrence), or return
# non-zero when the file is missing.
load_names() {
  [ -f "$NAMES_FILE" ] || return 1
  sed -e 's/#.*//' "$NAMES_FILE" | grep -v '^[[:space:]]*$' \
    | awk '!seen[$0]++' || true
}

status=0
LOG="${LOG:-/tmp/p23-integration.log}"

check_required() {
  name="$1"
  ran=$(grep -Ec "^=== RUN[[:space:]]+${name}([[:space:]]|$)" "$LOG" || true)
  if grep -Eq "^--- SKIP:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    log "FAIL: required admission test ${name} was SKIPped"; status=1
  elif grep -Eq "^--- FAIL:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    log "FAIL: required admission test ${name} FAILed"; status=1
  elif grep -Eq "^--- PASS:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    if [ "${ran:-0}" -ge 1 ]; then
      log "ok: required admission test ${name} PASSed (RUN+PASS)"
    else
      # a PASS line without its own top-level RUN is not a real execution
      # (child-only RUN + parent PASS does not count either)
      log "FAIL: required admission test ${name} PASS line without a top-level RUN"; status=1
    fi
  elif [ "${ran:-0}" -ge 1 ]; then
    log "FAIL: required admission test ${name} ran without a top-level PASS"; status=1
  else
    log "FAIL: required admission test ${name} did not run"; status=1
  fi
}

case "$MODE" in
  guard-only)
    if names=$(load_names); then
      count=$(printf '%s\n' "$names" | grep -c . || true)
      log "admission list: ${count:-0} distinct names loaded from $NAMES_FILE"
      log "total required = P1 26 + P2.1 14 + P2.2 12 + ${count:-0} = $((52 + ${count:-0})) (three old guards frozen)"
      log "honest status: pinned 2026-11-14 with NO real RUN+PASS yet — integration refuses until a real log shows them"
    else
      log "names file missing: $NAMES_FILE (PENDING root finalization — 0 names)"
    fi
    ;;
  integration)
    LOG="${2:?usage: run_tests_p23.sh integration <log>}"
    names=$(load_names) || {
      log "FAIL: $NAMES_FILE missing — P2.3 required names not finalized by root; refusing to bless any run"
      exit 1
    }
    [ -n "$names" ] || {
      log "FAIL: $NAMES_FILE carries no names — refusing to bless any run"
      exit 1
    }
    status=0
    for name in $names; do check_required "$name"; done
    log "admission guard status: $status (distinct names checked: $(printf '%s\n' "$names" | grep -c . || true))"
    exit $status
    ;;
  guard-probe)
    # Local self-test hook (never used by compose): sh run_tests_p23.sh
    # guard-probe <synthetic-log> <name> — echoes GUARD_STATUS=<0|1>.
    cp "$2" /tmp/p23-integration.log
    LOG=/tmp/p23-integration.log
    status=0
    check_required "$3"
    echo "GUARD_STATUS=$status"
    exit 0
    ;;
  *)
    echo "unknown mode: $MODE (expected guard-only|integration|guard-probe)" >&2
    exit 2
    ;;
esac
