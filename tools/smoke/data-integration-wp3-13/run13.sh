#!/usr/bin/env bash
# Disposable WP3.13 acceptance against the WP3.3 real-service stack.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../data-integration-wp3-3"
REPO="$(cd "$HERE/../../.." && pwd)"
RUN_ID="${1:?usage: WP313_DUCKDB_PYTHON=/path/to/python run13.sh RUN_ID}"
PYTHON="${WP313_DUCKDB_PYTHON:?set WP313_DUCKDB_PYTHON to a Python with DuckDB 1.4.4}"
[[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,39}[a-z0-9])?$ ]] || { echo "invalid run ID"; exit 2; }
[[ "$PYTHON" = /* && -x "$PYTHON" ]] || { echo "provide an absolute Python executable"; exit 2; }
"$PYTHON" -I -c 'import duckdb,sys; sys.exit(0 if duckdb.__version__ == "1.4.4" else "DuckDB 1.4.4 is required")'

export OF33C_RUN_ID="wp313-$RUN_ID"
export WP33C_PROJECT="of-wp33c-$OF33C_RUN_ID"
export COMPOSE_PROJECT_NAME="$WP33C_PROJECT"
unset OF33C_PG_PASSWORD OF33C_JWT_SECRET
export OF33C_ENVDIR="/private/tmp/$WP33C_PROJECT"
STACK_RESULTS="$STACK/results/3c-$OF33C_RUN_ID"
export WP33C_EVIDENCE="$STACK_RESULTS/wp313"
[[ ! -e "$OF33C_ENVDIR" && ! -e "$STACK_RESULTS" ]] || { echo "use a fresh run ID"; exit 2; }

# Check for remnants before the cleanup trap takes ownership of this project.
label="label=com.docker.compose.project=$WP33C_PROJECT"
containers="$(docker ps -aq --filter "$label")"
volumes="$(docker volume ls -q --filter "$label")"
networks="$(docker network ls -q --filter "$label")"
[[ -z "$containers$volumes$networks" ]] || { echo "Compose project already exists, use a fresh run ID"; exit 2; }
mkdir -p "$STACK/results"
mkdir "$STACK_RESULTS"
mkdir "$WP33C_EVIDENCE"
cleanup() {
  if [[ -f "$OF33C_ENVDIR/compose.yaml" ]]; then
    "$STACK/run3c.sh" down-volumes
  fi
}
trap cleanup EXIT

"$PYTHON" -I "$HERE/provenance.py" snapshot > "$WP33C_EVIDENCE/source-before.json"
"$STACK/run3c.sh" build
"$STACK/run3c.sh" proxy-selftest 2>&1 | tee "$WP33C_EVIDENCE/proxy-selftest.log"
mkdir -p "$STACK_RESULTS/fault-proxy"
docker compose --project-directory "$OF33C_ENVDIR" \
  -f "$OF33C_ENVDIR/compose.yaml" -f "$OF33C_ENVDIR/compose-3c-fault.yaml" up -d
export WP33C_DVS=http://127.0.0.1:17178
export WP33C_PBS=http://127.0.0.1:17181
export WP33C_PROXY_CTL=http://127.0.0.1:17179
unset WP33C_TOKEN
owner="$(sed -n 's/^WP33C_SUB=//p' "$OF33C_ENVDIR/.env")"
secret="$(sed -n 's/^OF33C_JWT_SECRET=//p' "$OF33C_ENVDIR/.env")"
WP33C_TOKEN="$("$PYTHON" -I "$REPO/tools/smoke/business-platform-p1/mint_token.py" \
  --secret "$secret" --sub "$owner" --permissions dataset.write,dataset:write,pipeline.write,pipeline:write)" \
  "$PYTHON" -I "$HERE/acceptance.py"
unset secret

"$PYTHON" -I "$HERE/provenance.py" snapshot > "$WP33C_EVIDENCE/source-after.json"
cleanup
trap - EXIT
"$PYTHON" -I "$HERE/provenance.py" verify
echo "PASS: $WP33C_EVIDENCE/verdict.json"
