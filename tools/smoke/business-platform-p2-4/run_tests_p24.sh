#!/bin/sh
#
# OpenFoundry P2.4 schedule observability — REQUIRED-name guard (owned
# tools copy of the frozen run_tests_p23.sh pattern; P1/P21/P22/P23
# guards stay frozen/untouched).
#
# The required P2.4 PG names come from p24-required-names.txt —
# DRAFT 2026-11-14: 13 top-level names read from the CURRENT
# internal/repo/schedule_integration_test.go while the PG GLM applies
# the 18-item review. Root FREEZES the final list only after
# source+tests are stable and real-PG green. This guard never blesses a
# run whose log lacks a top-level RUN+PASS for every listed name.
#
# Discipline (item 10): this grep guard REFUSES; it never replaces the
# wrapped Go/migrate rc. The formal pg59 phase uses the Go rc FIRST and
# this guard SECOND — both must be green.
#
# Counting rule: DISTINCT real names (duplicates collapse), each needing
# a TOP-LEVEL `=== RUN <name>` AND `--- PASS: <name>`; child-only
# `=== RUN <name>/sub` does not count; SKIP > FAIL priority; no required
# SKIP tolerated. Missing/empty names file FAILS CLOSED.
#
set -eu
MODE="${1:-guard-only}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NAMES_FILE="${P24_NAMES_FILE:-$SCRIPT_DIR/p24-required-names.txt}"

log() { printf '[p24-guard] %s\n' "$*"; }

load_names() {
  [ -f "$NAMES_FILE" ] || return 1
  sed -e 's/#.*//' "$NAMES_FILE" | grep -v '^[[:space:]]*$' \
    | awk '!seen[$0]++' || true
}

status=0
LOG="${LOG:-/tmp/p24-integration.log}"

check_required() {
  name="$1"
  ran=$(grep -Ec "^=== RUN[[:space:]]+${name}([[:space:]]|$)" "$LOG" || true)
  if grep -Eq "^--- SKIP:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    log "FAIL: required schedule test ${name} was SKIPped"; status=1
  elif grep -Eq "^--- FAIL:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    log "FAIL: required schedule test ${name} FAILed"; status=1
  elif grep -Eq "^--- PASS:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    if [ "${ran:-0}" -ge 1 ]; then
      log "ok: required schedule test ${name} PASSed (RUN+PASS)"
    else
      log "FAIL: required schedule test ${name} PASS line without a top-level RUN"; status=1
    fi
  elif [ "${ran:-0}" -ge 1 ]; then
    log "FAIL: required schedule test ${name} ran without a top-level PASS"; status=1
  else
    log "FAIL: required schedule test ${name} did not run"; status=1
  fi
}

case "$MODE" in
  guard-only)
    if names=$(load_names); then
      count=$(printf '%s\n' "$names" | grep -c . || true)
      log "schedule list: ${count:-0} distinct names loaded from $NAMES_FILE"
      log "total required = P1 26 + P2.1 14 + P2.2 12 + P2.3 7 + ${count:-0} = $((59 + ${count:-0}))"
      log "honest status: DRAFT names (not root-frozen); integration refuses until a real log shows each RUN+PASS"
    else
      log "names file missing: $NAMES_FILE (PENDING root finalization — 0 names)"
    fi
    ;;
  integration)
    LOG="${2:?usage: run_tests_p24.sh integration <log>}"
    names=$(load_names) || {
      log "FAIL: $NAMES_FILE missing — P2.4 required names not finalized; refusing to bless any run"
      exit 1
    }
    [ -n "$names" ] || {
      log "FAIL: $NAMES_FILE carries no names — refusing to bless any run"
      exit 1
    }
    status=0
    for name in $names; do check_required "$name"; done
    log "schedule guard status: $status (distinct names checked: $(printf '%s\n' "$names" | grep -c . || true))"
    exit $status
    ;;
  guard-probe)
    cp "$2" /tmp/p24-integration.log
    LOG=/tmp/p24-integration.log
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
