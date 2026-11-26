#!/usr/bin/env bash
# S1 security acceptance runner (WP2.1 webhook tenant isolation + WP2.2
# independent credential-key startup gate).
#
# Anti-greening contract (review round 3):
#   * Every docker/go/helper step's REAL exit code is captured (pipefail +
#     PIPESTATUS-aware) and recorded to results/<RUN_ID>/exit-codes.txt; a
#     non-zero rc fails the run immediately. grep is display-only.
#   * Evidence JSONs are strictly validated by verify_results.py (typed
#     fields, run_id, required scenario inventory, real booleans, no SKIP);
#     a missing or failing JSON fails the run even if the command exited 0.
#   * Step 4 runs bounded negative controls proving the runner rejects bad
#     evidence (see negative_controls.sh).
#
# Creates ONLY disposable, uniquely-named resources per run (docker network,
# postgres:16 container with synthetic credentials, golang:1.26.11 harness with
# cached modules). Never touches existing services/DBs/volumes/.env; removes
# exactly what it created on exit.
#
# Usage: ./run.sh [--keep]
#   --keep  keep containers/network/logs for debugging (cleanup skipped)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%S)_$RANDOM"
RESULTS_DIR="$HERE/results/$RUN_ID"
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

NET="ofs1sec-net-$RUN_ID"
PG="ofs1sec-pg-$RUN_ID"
PGUSER="ofs1sec"
PGDB="ofs1sec"
PGPASS="s1sec-synthetic-pg-pass-$RUN_ID" # synthetic per-run credential
DSN="postgres://$PGUSER:$PGPASS@$PG:5432/$PGDB"

mkdir -p "$RESULTS_DIR"
EXIT_CODES="$RESULTS_DIR/exit-codes.txt"
: >"$EXIT_CODES"



cleanup() {
  if [[ $KEEP -eq 1 ]]; then
    echo "[cleanup] --keep: leaving $PG / $NET / $RESULTS_DIR in place"
    return
  fi
  docker rm -f "$PG" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  echo "[cleanup] removed $PG and $NET (kept: $RESULTS_DIR)"
}
trap cleanup EXIT

echo "== S1 security acceptance run $RUN_ID =="
docker network create "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" \
  -e POSTGRES_USER="$PGUSER" -e POSTGRES_PASSWORD="$PGPASS" -e POSTGRES_DB="$PGDB" \
  postgres:16 >/dev/null
echo "[pg] started isolated postgres:16 as $PG (network $NET)"

for i in $(seq 1 60); do
  if docker exec "$PG" pg_isready -U "$PGUSER" -d "$PGDB" >/dev/null 2>&1; then
    echo "[pg] ready after ${i}s"
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then echo "[pg] FAILED to become ready" >&2; exit 1; fi
done

GO_MOUNTS=(-v "$REPO_ROOT:/work"
  -v ofs1sec-modcache:/go/pkg/mod
  -v ofs1sec-buildcache:/root/.cache/go-build)

# Shared step wrappers (record_rc / run_step / verify_step) live in
# runner_lib.sh, sourced by this runner AND by the negative controls so the
# controls exercise the real wrapper logic, never a copy.
source "$HERE/runner_lib.sh"

echo
echo "== [1/4] WP2.1 unit + tenant-matrix regression =="
run_step "wp21-unit" "$RESULTS_DIR/wp21-unit.log" \
  docker run --rm "${GO_MOUNTS[@]}" -w "/work/services/connector-management-service" \
  -e GOFLAGS=-trimpath golang:1.26.11 \
  go test ./internal/handlers/ ./internal/config/ ./internal/domain/ ./internal/server/ -count=1

echo
echo "== [2/4] WP2.1 real HTTP + PG16 acceptance =="
run_step "wp21-http-pg" "$RESULTS_DIR/wp21-http-pg.log" \
  docker run --rm "${GO_MOUNTS[@]}" --network "$NET" \
  -w "/work/services/connector-management-service" \
  -e GOFLAGS=-trimpath -e S1SEC_PG_DSN="$DSN" -e S1SEC_RUN_ID="$RUN_ID" \
  -e S1SEC_RESULTS_JSON="/work/tools/smoke/data-integration-s1-security/results/$RUN_ID/wp21-http-pg.json" \
  golang:1.26.11 \
  go test ./internal/handlers/ -count=1 -run TestS1SecWebhookTenantIsolationHTTPAgainstPG -v
verify_step "wp21-http-pg-verify" http \
  "$RESULTS_DIR/wp21-http-pg.json" "$RESULTS_DIR/wp21-http-pg.log" "$RUN_ID"

echo
echo "== [3/4] WP2.2 binary startup-gate acceptance =="
run_step "wp22-binary-gate" "$RESULTS_DIR/wp22-binary-gate.log" \
  docker run --rm "${GO_MOUNTS[@]}" --network "$NET" \
  -e S1SEC_PG_DSN="$DSN" -e S1SEC_RUN_ID="$RUN_ID" \
  -e S1SEC_RESULTS_JSON="/work/tools/smoke/data-integration-s1-security/results/$RUN_ID/wp22-binary-gate.json" \
  golang:1.26.11 bash /work/tools/smoke/data-integration-s1-security/wp22-binary-gate.sh
verify_step "wp22-binary-gate-verify" gate \
  "$RESULTS_DIR/wp22-binary-gate.json" "$RUN_ID"

echo
echo "== [4/4] bounded negative controls (runner must reject bad evidence) =="
run_step "negative-controls" "$RESULTS_DIR/negative-controls.log" \
  bash "$HERE/negative_controls.sh" "$HERE/verify_results.py"

verify_step "exit-codes-all-zero" exitcodes "$EXIT_CODES"

echo
echo "== run $RUN_ID PASSED (all steps rc=0, evidence verified) =="
echo "results: $RESULTS_DIR"
ls -1 "$RESULTS_DIR"
