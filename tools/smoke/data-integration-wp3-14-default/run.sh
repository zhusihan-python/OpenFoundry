#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../data-integration-wp3-3"
export WP314_REPO="$(cd "$HERE/../../.." && pwd)"
RUN_ID="${1:?usage: WP314_DUCKDB_PYTHON=/absolute/path/to/python run.sh RUN_ID}"
PYTHON="${WP314_DUCKDB_PYTHON:?set WP314_DUCKDB_PYTHON to a Python with DuckDB 1.4.4}"
[[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,28}[a-z0-9])?$ ]] || { echo "invalid run ID"; exit 2; }
[[ "$PYTHON" = /* && -x "$PYTHON" ]] || { echo "provide an absolute Python executable"; exit 2; }
"$PYTHON" -I -c 'import duckdb,sys; sys.exit(0 if duckdb.__version__ == "1.4.4" else "DuckDB 1.4.4 is required")'

export OF33C_RUN_ID="wp314d-$RUN_ID"
export WP33C_PROJECT="of-wp33c-$OF33C_RUN_ID"
export COMPOSE_PROJECT_NAME="$WP33C_PROJECT"
export OF33C_ENVDIR="/private/tmp/$WP33C_PROJECT"
unset OF33C_PG_PASSWORD OF33C_JWT_SECRET
STACK_RESULTS="$STACK/results/3c-$OF33C_RUN_ID"
export WP314_BIN="$STACK_RESULTS/bin"
export WP33C_EVIDENCE="$STACK_RESULTS/wp314-default"
export WP314_OVERLAY="$HERE/compose.yaml"
export WP33C_DVS=http://127.0.0.1:17178
export WP33C_PBS=http://127.0.0.1:17181
export WP33C_PROXY_CTL=http://127.0.0.1:17179
export WP314_CIPHER_KEY="$(openssl rand -base64 32)"
[[ ! -e "$OF33C_ENVDIR" && ! -e "$STACK_RESULTS" ]] || { echo "use a fresh run ID"; exit 2; }
label="label=com.docker.compose.project=$WP33C_PROJECT"
containers="$(docker ps -aq --filter "$label")"
volumes="$(docker volume ls -q --filter "$label")"
networks="$(docker network ls -q --filter "$label")"
[[ -z "$containers$volumes$networks" ]] || { echo "Compose project exists, use a fresh run ID"; exit 2; }
mkdir -p "$STACK/results"
mkdir "$STACK_RESULTS"
mkdir -p "$WP33C_EVIDENCE/fault-proxy"
compose() {
  docker compose --project-directory "$OF33C_ENVDIR" -f "$OF33C_ENVDIR/compose.yaml" -f "$WP314_OVERLAY" "$@"
}
cleanup() {
  if [[ -f "$OF33C_ENVDIR/compose.yaml" ]]; then compose down --remove-orphans -v; fi
}
trap cleanup EXIT

"$PYTHON" -I "$HERE/evidence.py" snapshot > "$WP33C_EVIDENCE/source-before.json"
"$STACK/run3c.sh" build
docker run --rm -v "$WP314_REPO":/src:ro -v /private/tmp/of-gomodcache:/go/pkg/mod \
  -v /private/tmp/of-gocache:/root/.cache/go-build -v "$WP314_BIN":/out -w /src \
  -e GOTOOLCHAIN=local -e GOPROXY=off -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 \
  golang:1.26.11 go build -trimpath -buildvcs=false -o /out/schedules-tick ./libs/event-scheduler/cmd/schedules-tick
shasum -a 256 "$WP314_BIN/schedules-tick" > "$WP33C_EVIDENCE/tick-build-sha256.txt"
python3 "$HERE/../data-integration-wp3-2/test_fault_proxy.py" 2>&1 | tee "$WP33C_EVIDENCE/proxy-selftest.log"
compose up -d postgres dvs kafka registry faultproxy
compose run --rm --no-deps schemas 2>&1 | tee "$WP33C_EVIDENCE/schema-registration.log"
# Both the consumer and connector depend on healthy Kafka.
compose up -d pbs connect

unset WP33C_TOKEN
owner="$(sed -n 's/^WP33C_SUB=//p' "$OF33C_ENVDIR/.env")"
secret="$(sed -n 's/^OF33C_JWT_SECRET=//p' "$OF33C_ENVDIR/.env")"
export WP314_ORG_ID="$(uuidgen | tr 'A-Z' 'a-z')"
export WP33C_TOKEN="$("$PYTHON" -I "$WP314_REPO/tools/smoke/business-platform-p1/mint_token.py" \
  --secret "$secret" --sub "$owner" --org "$WP314_ORG_ID" \
  --permissions dataset.write,dataset:write,pipeline.write,pipeline:write)"
unset secret

"$PYTHON" -I "$HERE/acceptance.py"
"$PYTHON" -I "$HERE/manual_graph.py"
"$PYTHON" -I "$HERE/evidence.py" snapshot > "$WP33C_EVIDENCE/source-after.json"
"$PYTHON" -I "$HERE/evidence.py" verify
cleanup
trap - EXIT
mv "$WP33C_EVIDENCE/.verified.json" "$WP33C_EVIDENCE/verdict.json"
echo "PASS: $WP33C_EVIDENCE/verdict.json"
