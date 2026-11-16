#!/bin/sh
#
# OpenFoundry P2.1 lease integration — REQUIRED-name guard (standalone;
# the P1 26-name guard stays frozen and untouched).
#
# The list below is the CANDIDATE set read from
# services/connector-management-service/internal/repo/lease_integration_test.go.
# ACCEPTED at 14 names (2026-11-13 acceptance run, evidence
# results/2026-11-13-p21-final/logs/p21-required-guard.log).
#
set -eu
MODE="${1:-guard-only}"

REQUIRED_LEASE="TestPGLeaseEnqueueCommitsAndProvenance TestPGLeaseConcurrentClaimExactlyOneWinner TestPGLeaseRenewCompleteRejectWrongFencingMaterial TestPGLeaseRowLockExpiryClockRegression TestPGLeaseExpiryFenceBeforeTakeover TestPGLeaseTakeoverFencingAndABA TestPGLegacyRowsGuarded TestPGLeaseInvalidArgumentsTouchNothing TestPGActiveGuardsBlockSourceAndTargetMutations TestPGDefLockVsRetargetDeterministic TestPGEnqueueVsDeleteBarrier TestPGMigrateLegacyDBPreservesRowsAndLeaseOnRerun TestPGLeaseTTLAnchoredToPostLockClock TestPGLeaseTenantIsolationMatrix"

log() { printf '[p21-guard] %s\n' "$*"; }

status=0
LOG="${LOG:-/tmp/p21-integration.log}"
check_required() {
  name="$1"
  ran=$(grep -Ec "^=== RUN[[:space:]]+${name}([[:space:]]|$)" "$LOG" || true)
  if grep -Eq "^--- SKIP:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    log "FAIL: required lease test ${name} was SKIPped"; status=1
  elif grep -Eq "^--- FAIL:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    log "FAIL: required lease test ${name} FAILed"; status=1
  elif grep -Eq "^--- PASS:[[:space:]]+${name}([[:space:]]|\(|$)" "$LOG"; then
    if [ "${ran:-0}" -ge 1 ]; then
      log "ok: required lease test ${name} PASSed (RUN+PASS)"
    else
      # a PASS line without its own top-level RUN is not a real execution
      log "FAIL: required lease test ${name} PASS line without a top-level RUN"; status=1
    fi
  elif [ "${ran:-0}" -ge 1 ]; then
    log "FAIL: required lease test ${name} ran without a top-level PASS"; status=1
  else
    log "FAIL: required lease test ${name} did not run"; status=1
  fi
}

case "$MODE" in
  guard-only)
    log "lease list: $(echo "$REQUIRED_LEASE" | wc -w | tr -d ' ') names (ACCEPTED 2026-11-13, 14/14 RUN+PASS)"
    ;;
  integration)
    log "integration mode runs via the P1 runner (../business-platform-p1/run.sh migrate-tests)"
    log "this guard post-checks its log: pass the log path as $2"
    LOG="${2:?usage: run_tests_p21.sh integration <log>}"
    status=0
    for name in $REQUIRED_LEASE; do check_required "$name"; done
    log "lease guard status: $status"
    exit $status
    ;;
  guard-probe)
    # Local self-test hook: sh run_tests_p21.sh guard-probe <log> <name>
    cp "$2" /tmp/p21-integration.log
    LOG=/tmp/p21-integration.log
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
