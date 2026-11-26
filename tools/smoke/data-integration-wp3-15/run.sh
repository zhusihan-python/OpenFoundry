#!/usr/bin/env bash
# WP3.15 disposable format benchmark. PBS talks directly to DVS.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$(cd "$HERE/../data-integration-wp3-3" && pwd)"
export WP315_REPO="$(cd "$HERE/../../.." && pwd)"
RUN_ID="${1:?usage: WP315_DUCKDB_PYTHON=/absolute/path/to/python run.sh RUN_ID FULL_EXPECTED_SHA}"
export WP315_EXPECTED_SHA="${2:?pass the expected full source SHA}"
PYTHON="${WP315_DUCKDB_PYTHON:?set WP315_DUCKDB_PYTHON to a Python with DuckDB 1.4.4}"
[[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,35}[a-z0-9])?$ ]] || { echo "invalid run ID"; exit 2; }
[[ "$WP315_EXPECTED_SHA" =~ ^[a-f0-9]{40}$ ]] || { echo "pass a full lowercase commit SHA"; exit 2; }
[[ "$PYTHON" = /* && -x "$PYTHON" ]] || { echo "provide an absolute Python executable"; exit 2; }
"$PYTHON" -I -c 'import duckdb,sys; sys.exit(0 if duckdb.__version__ == "1.4.4" else "DuckDB 1.4.4 is required")'
"$PYTHON" -I "$HERE/evidence.py" source >/dev/null

export OF33C_RUN_ID="wp315-$RUN_ID"
export WP33C_PROJECT="of-wp33c-$OF33C_RUN_ID"
export COMPOSE_PROJECT_NAME="$WP33C_PROJECT"
export OF33C_ENVDIR="/private/tmp/$WP33C_PROJECT"
STACK_RESULTS="$STACK/results/3c-$OF33C_RUN_ID"
export WP315_BIN="$STACK_RESULTS/bin"
export WP33C_EVIDENCE="$STACK_RESULTS/wp315"
export WP33C_DVS=http://127.0.0.1:17178
export WP33C_PBS=http://127.0.0.1:17181
export WP315_SCAN_PAGE_ROWS=500
export WP315_STAGE_TIMEOUT_SECONDS=180
unset OF33C_PG_PASSWORD OF33C_JWT_SECRET WP33C_TOKEN WP315_FOREIGN_TOKEN
OVERLAY="$OF33C_ENVDIR/wp315-compose.yaml"
[[ ! -e "$OF33C_ENVDIR" && ! -e "$STACK_RESULTS" ]] || { echo "use a fresh run ID"; exit 2; }

ensure_project_empty() {
  local label="label=com.docker.compose.project=$WP33C_PROJECT"
  local containers volumes networks
  containers="$(docker ps -aq --filter "$label")"
  volumes="$(docker volume ls -q --filter "$label")"
  networks="$(docker network ls -q --filter "$label")"
  [[ -z "$containers$volumes$networks" ]] || { echo "owned project exists or was incompletely removed"; return 1; }
}
ensure_project_empty
docker image inspect golang:1.26.11 alpine:3.22 postgres:16-alpine >/dev/null
mkdir -p "$STACK/results"
mkdir "$STACK_RESULTS"
mkdir "$WP33C_EVIDENCE"

compose() {
  docker compose --project-directory "$OF33C_ENVDIR" -f "$OF33C_ENVDIR/compose.yaml" -f "$OVERLAY" "$@"
}
cleanup() {
  local incoming=$?
  if [[ -f "$OVERLAY" ]]; then
    if [[ "$incoming" -ne 0 ]]; then
      compose ps -a > "$WP33C_EVIDENCE/failing-services.log" 2>&1 || true
      compose logs --tail=80 --no-color postgres dvs pbs 2>&1 \
        | "$PYTHON" -I "$HERE/evidence.py" redact-logs > "$WP33C_EVIDENCE/failing-service-logs.log" || true
    fi
    compose down --remove-orphans -v 2>&1 | tee -a "$WP33C_EVIDENCE/stack-down.log"
    ensure_project_empty
  fi
}
trap cleanup EXIT

"$PYTHON" -I "$HERE/evidence.py" source > "$WP33C_EVIDENCE/source-before.json"
"$STACK/run3c.sh" build
cat > "$OVERLAY" <<COMPOSE
services:
  pbs:
    environment:
      BATCHED_SCAN_PAGE_ROWS: "$WP315_SCAN_PAGE_ROWS"
      BATCHED_STAGE_TIMEOUT_SECONDS: "$WP315_STAGE_TIMEOUT_SECONDS"
      SCHEDULER_CONSUMER_ENABLED: "false"
COMPOSE
compose up -d --pull never postgres dvs pbs 2>&1 | tee "$WP33C_EVIDENCE/stack-up.log"

owner="$(sed -n 's/^WP33C_SUB=//p' "$OF33C_ENVDIR/.env")"
secret="$(sed -n 's/^OF33C_JWT_SECRET=//p' "$OF33C_ENVDIR/.env")"
owner_org="$(uuidgen | tr 'A-Z' 'a-z')"
foreign_org="$(uuidgen | tr 'A-Z' 'a-z')"
[[ "$owner_org" != "$foreign_org" ]]
permissions="dataset.read,dataset:read,dataset.write,dataset:write,pipeline.write,pipeline:write"
mint() {
  "$PYTHON" -I "$WP315_REPO/tools/smoke/business-platform-p1/mint_token.py" \
    --secret "$secret" --sub "$owner" --org "$1" --permissions "$permissions"
}
WP33C_TOKEN="$(mint "$owner_org")"
WP315_FOREIGN_TOKEN="$(mint "$foreign_org")"
export WP33C_TOKEN WP315_FOREIGN_TOKEN
unset secret

"$PYTHON" -I "$HERE/evidence.py" runtime > "$WP33C_EVIDENCE/runtime-before.json"
"$PYTHON" -I "$HERE/benchmark.py" 2>&1 | tee "$WP33C_EVIDENCE/benchmark.log"
"$PYTHON" -I "$HERE/evidence.py" runtime > "$WP33C_EVIDENCE/runtime-after.json"
"$PYTHON" -I "$HERE/evidence.py" source > "$WP33C_EVIDENCE/source-after.json"
cleanup
trap - EXIT
printf '{"project":"%s","containers":0,"volumes":0,"networks":0}\n' "$WP33C_PROJECT" > "$WP33C_EVIDENCE/teardown.json"
"$PYTHON" -I "$HERE/evidence.py" verify
echo "PASS: $WP33C_EVIDENCE/verdict.json"
