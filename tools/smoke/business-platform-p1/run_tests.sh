#!/bin/sh
#
# OpenFoundry P1 acceptance — in-container test runner.
#
# Invoked by compose (ENTRYPOINT) with the mode as first argument:
#   unit        full `go test` for the three P1 services
#   integration `-tags=integration` suites against the compose Postgres
#               via DATASET_DB_DSN / PIPELINE_DB_DSN / CONNECTOR_DB_DSN
#
# Required-test guard: every name in REQUIRED_* must actually RUN and
# must not be SKIPped. A missing or skipped required test fails the run
# loudly — silent skips are never acceptable here.
#
set -eu

MODE="${1:-unit}"
SERVICES="./services/dataset-versioning-service/... ./services/pipeline-build-service/... ./services/connector-management-service/..."

REQUIRED_DATASET="TestDatasetOrgTenancyMigrationAndVisibility TestMigrationsFreshDBRerunAndRepeatedSnapshots TestCommitDatasetOutputPublishFaultInjection TestSchemaBranchHTTPRoundTrip TestSchemaOwnershipCannotBeClaimed TestSchemaColumnsRepairLegacyDatabase"
REQUIRED_PIPELINE="TestTenantIsolationMigrationsAndBoundary TestTenantMigrationOnPopulatedLegacyDB TestTenantRepairRegressionBundle TestBuildFlowCancelledContextFinalizesState TestBuildFlowPublishDedupRebindsMapping TestBuildFlowLedgerUpgradeAppliesNewMigrations TestBuildFlowAbortTransactionTerminalSemantics TestBuildFlowPendingRetryClosure TestBuildFlowSecondRecordFailureCompensatesEverything TestBuildFlowSecondOpenFailureCompensatesFirst TestBuildFlowReleaseFailureKeepsReference TestBuildFlowCancelAfterOpenCompensatesOnDetachedContext TestBuildFlowJobSpecPublishOwnership TestBuildFlowForeignInputSpecRejectedZeroSideEffects TestBuildFlowMultiOutputPGStateAssertions TestBuildFlowRecordAndAbortBothFailKeepsRecoverableHandle TestBuildFlowEmitJobLogTenantGating"
# Connector repo integration tests honour CONNECTOR_DB_DSN and create
# their own fresh databases. Append further must-run cases as they land.
REQUIRED_CONNECTOR="TestPGMigrateEmptyDatabaseAndRerun TestPGMigrateLegacyDatabaseGainsOrgColumns TestPGSourceTenantCRUDMatrix"

log() { printf '[test-runner] %s\n' "$*"; }

status=0
# Go's -v output ends every top-level result line with the test name
# followed by a SPACE and the duration ("--- PASS: TestName (0.00s)"),
# so the boundary class is (whitespace | "(" | end-of-line). A required
# test is ok ONLY with a top-level --- PASS line; SKIP, FAIL, a RUN
# without a terminal PASS, or a missing RUN all fail the guard.
check_required() {
  service="$1"; name="$2"
  if grep -Eq "^--- PASS:[[:space:]]+${name}([[:space:]]|\(|$)" /tmp/integration.log; then
    if grep -Eq "^--- SKIP:[[:space:]]+${name}([[:space:]]|\(|$)" /tmp/integration.log; then
      log "FAIL: required ${service} test ${name} was SKIPped"
      status=1
    else
      log "ok: required ${service} test ${name} PASSed"
    fi
  elif grep -Eq "^--- SKIP:[[:space:]]+${name}([[:space:]]|\(|$)" /tmp/integration.log; then
    log "FAIL: required ${service} test ${name} was SKIPped"
    status=1
  elif grep -Eq "^--- FAIL:[[:space:]]+${name}([[:space:]]|\(|$)" /tmp/integration.log; then
    log "FAIL: required ${service} test ${name} FAILed"
    status=1
  elif grep -Eq "^=== RUN[[:space:]]+${name}(/|$|[[:space:]])" /tmp/integration.log; then
    log "FAIL: required ${service} test ${name} ran but has no top-level PASS"
    status=1
  else
    log "FAIL: required ${service} test ${name} did not run (missing?)"
    status=1
  fi
}

case "$MODE" in
  unit)
    log "unit tests: $SERVICES"
    go test -count=1 $SERVICES
    ;;
  integration)
    log "integration tests (tag=integration): $SERVICES"
    log "DSNs: DATASET_DB_DSN=${DATASET_DB_DSN:+set} PIPELINE_DB_DSN=${PIPELINE_DB_DSN:+set} CONNECTOR_DB_DSN=${CONNECTOR_DB_DSN:+set}"
    go_rc=0
    go test -tags=integration -count=1 -v $SERVICES > /tmp/integration.log 2>&1 || go_rc=$?
    cat /tmp/integration.log
    if [ "$go_rc" -ne 0 ]; then
      log "go test exited non-zero: $go_rc"
      exit "$go_rc"
    fi
    for name in $REQUIRED_DATASET; do check_required dataset "$name"; done
    for name in $REQUIRED_PIPELINE; do check_required pipeline "$name"; done
    for name in $REQUIRED_CONNECTOR; do check_required connector "$name"; done
    log "required-test guard status: $status"
    exit $status
    ;;
  guard-probe)
    # Local self-test hook (never used by compose): run the SHIPPED guard
    # against a synthetic log. Usage: sh run_tests.sh guard-probe <log> <name>
    cp "$2" /tmp/integration.log
    check_required probe "$3"
    echo "GUARD_STATUS=$status"
    exit 0  # probe always exits 0; callers assert on the echoed status
    ;;
  *)
    echo "unknown mode: $MODE (expected unit|integration|guard-probe)" >&2
    exit 2
    ;;
esac
