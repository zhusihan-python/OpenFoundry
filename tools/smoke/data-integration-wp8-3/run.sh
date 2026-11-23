#!/usr/bin/env bash
# Real-service WP8.3 order-version comparison acceptance for a clean checkout.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STACK="$HERE/../data-integration-wp3-3"
REPO="$(cd "$HERE/../../.." && pwd)"
RUN_ID="${1:?usage: WP83_DUCKDB_PYTHON=/absolute/path/to/python run.sh RUN_ID}"
PYTHON="${WP83_DUCKDB_PYTHON:?set WP83_DUCKDB_PYTHON to a Python with DuckDB 1.4.4}"
[[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,31}[a-z0-9])?$ ]] || { echo "invalid run ID"; exit 2; }
[[ "$PYTHON" = /* && -x "$PYTHON" ]] || { echo "provide an absolute Python executable"; exit 2; }
"$PYTHON" -I -c 'import duckdb; assert duckdb.__version__ == "1.4.4"'
command -v ego-browser >/dev/null
test -f "$REPO/apps/web/node_modules/vite/bin/vite.js"
cd "$REPO"
test -z "$(git status --porcelain)"
REVISION="$(git rev-parse HEAD)"
TREE="$(git rev-parse HEAD^{tree})"

export OF33C_RUN_ID="wp83-$RUN_ID"
export WP33C_PROJECT="of-wp33c-$OF33C_RUN_ID"
export WP33C_DVS=http://127.0.0.1:17178
export WP33C_PBS=http://127.0.0.1:17181
export OF33C_ENVDIR="/private/tmp/$WP33C_PROJECT"
export WP33C_EVIDENCE="$HERE/results/$RUN_ID"
STACK_RESULTS="$STACK/results/3c-$OF33C_RUN_ID"
BIN="$STACK_RESULTS/bin"
GATEWAY="$WP33C_PROJECT-gateway"
WEB_PID=""
SCHEMA_DIFF_PID=""
ROW_MATCH_PID=""
RANGE_DIFF_PID=""
BROWSER_FINISHED=""
[[ ! -e "$OF33C_ENVDIR" && ! -e "$WP33C_EVIDENCE" && ! -e "$STACK_RESULTS" ]] || { echo "use a fresh run ID"; exit 2; }
label="label=com.docker.compose.project=$WP33C_PROJECT"
containers="$(docker ps -aq --filter "$label")"
volumes="$(docker volume ls -q --filter "$label")"
networks="$(docker network ls -q --filter "$label")"
[[ -z "$containers$volumes$networks" ]] || { echo "Compose project already exists"; exit 2; }
mkdir -p "$HERE/results"
mkdir "$WP33C_EVIDENCE"

finish_browser() {
  local space_id
  if [[ -n "$BROWSER_FINISHED" || ! -f "$OF33C_ENVDIR/browser-space" ]]; then return 0; fi
  space_id="$(cat "$OF33C_ENVDIR/browser-space")"
  [[ "$space_id" =~ ^[0-9]+$ ]] || return 1
  ego-browser nodejs -e "await (await taskSpace($space_id)).finish({ keep: [] });" \
    >"$WP33C_EVIDENCE/browser-finish.log" 2>&1 || return 1
  BROWSER_FINISHED=1
}

driver_running() {
  [[ "$(jobs -pr)" =~ (^|[[:space:]])$1($|[[:space:]]) ]]
}

stop_driver() {
  local pid="$1" name="$2" grace_seconds="$3" deadline status=0
  if [[ -z "$pid" ]]; then return 0; fi
  if driver_running "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
    deadline=$((SECONDS + grace_seconds))
    while ((SECONDS < deadline)) && driver_running "$pid"; do sleep 0.2; done
    if driver_running "$pid"; then
      echo "$name did not stop after TERM" >&2
      kill -KILL "$pid" 2>/dev/null || true
      status=1
    fi
  fi
  wait "$pid" 2>/dev/null || true
  return "$status"
}

cleanup() {
  local status=0
  stop_driver "$SCHEMA_DIFF_PID" schema-diff 25 || status=1
  stop_driver "$ROW_MATCH_PID" row-match 25 || status=1
  stop_driver "$RANGE_DIFF_PID" range-diff 25 || status=1
  finish_browser || status=1
  if [[ -n "$WEB_PID" ]]; then
    kill "$WEB_PID" 2>/dev/null || true
    wait "$WEB_PID" 2>/dev/null || true
  fi
  if [[ -f "$OF33C_ENVDIR/gateway-id" ]]; then
    docker rm -f "$(cat "$OF33C_ENVDIR/gateway-id")" >/dev/null || status=1
  fi
  if [[ -f "$OF33C_ENVDIR/compose.yaml" ]]; then
    "$STACK/run3c.sh" down-volumes || status=1
  fi
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$STACK/run3c.sh" build 2>&1 | tee "$WP33C_EVIDENCE/services-build.log"
cat >"$OF33C_ENVDIR/compose-read.yaml" <<'YAML'
services:
  pbs:
    environment:
      BATCHED_SCAN_PAGE_ROWS: "2"
YAML
docker compose --project-name "$WP33C_PROJECT" --project-directory "$OF33C_ENVDIR" \
  -f "$OF33C_ENVDIR/compose.yaml" -f "$OF33C_ENVDIR/compose-read.yaml" up -d --wait
secret="$(sed -n 's/^OF33C_JWT_SECRET=//p' "$OF33C_ENVDIR/.env")"
owner="$(sed -n 's/^WP33C_SUB=//p' "$OF33C_ENVDIR/.env")"
token_file="$OF33C_ENVDIR/browser-token"
minter="$REPO/tools/smoke/business-platform-p1/mint_token.py"
permissions="dataset.read,dataset:read,dataset.write,dataset:write,pipeline.write,pipeline:write"
(umask 077; "$PYTHON" -I "$minter" --secret "$secret" --sub "$owner" --permissions "$permissions" >"$token_file")

GO_IMAGE=golang:1.26.3
docker run --rm -v "$REPO:/work:ro" -w /work -v "$BIN:/out" \
  -v /private/tmp/of-gomodcache:/go/pkg/mod -v /private/tmp/of-gocache:/root/.cache/go-build \
  -e GOTOOLCHAIN=local -e GOPROXY=off -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 "$GO_IMAGE" \
  bash -eu -c 'go version; go build -trimpath -buildvcs=false -o /out/edge-gateway-service ./services/edge-gateway-service/cmd/edge-gateway-service' \
  >"$WP33C_EVIDENCE/gateway-build.log" 2>&1
read -r gw_port web_port < <("$PYTHON" -I - <<'PY'
import socket
with socket.socket() as gw, socket.socket() as web:
    for listener in (gw, web):
        listener.bind(("127.0.0.1", 0))
    print(gw.getsockname()[1], web.getsockname()[1])
PY
)
docker run -d --name "$GATEWAY" --network "${WP33C_PROJECT}_default" \
  --label "$label" -v "$BIN:/binaries:ro" -p "127.0.0.1:$gw_port:8080" \
  -e OF_SERVER__PORT=8080 -e OF_JWT__SECRET="$secret" \
  -e OF_UPSTREAM__DATASET_VERSIONING_SERVICE_URL=http://dvs:50078 \
  --entrypoint /binaries/edge-gateway-service "$GO_IMAGE" >"$OF33C_ENVDIR/gateway-id"
unset secret
wait_http() {
  local try
  for ((try=0; try<45; try++)); do
    if curl --max-time 2 -fsS "$1" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  echo "service unreachable: $1" >&2
  return 1
}
wait_http "http://127.0.0.1:$gw_port/healthz"
(
  cd "$REPO/apps/web"
  P31_WEB_PORT="$web_port" P31_GW_URL="http://127.0.0.1:$gw_port" \
    exec node node_modules/vite/bin/vite.js --config vite.p31-e2e.config.ts
) >"$WP33C_EVIDENCE/web.log" 2>&1 &
WEB_PID=$!
wait_http "http://127.0.0.1:$web_port"

WP33C_TOKEN="$(cat "$token_file")" "$PYTHON" -I "$HERE/schema_diff.py" \
  >"$WP33C_EVIDENCE/schema-diff.log" 2>&1 &
SCHEMA_DIFF_PID=$!
WP33C_TOKEN="$(cat "$token_file")" "$PYTHON" -I "$HERE/row_match.py" \
  >"$WP33C_EVIDENCE/row-match.log" 2>&1 &
ROW_MATCH_PID=$!
WP33C_TOKEN="$(cat "$token_file")" "$PYTHON" -I "$HERE/range_diff.py" \
  >"$WP33C_EVIDENCE/range-diff.log" 2>&1 &
RANGE_DIFF_PID=$!

{
  "$PYTHON" -I - "$web_port" "$token_file" "$WP33C_EVIDENCE" "$OF33C_ENVDIR" <<'PY'
import json
import sys
keys = ("web_port", "token_file", "evidence", "envdir")
print("const input = " + json.dumps(dict(zip(keys, sys.argv[1:], strict=True))) + ";")
PY
  cat "$HERE/browser.mjs"
} | ego-browser nodejs >"$WP33C_EVIDENCE/browser.log" 2>&1
schema_diff_status=0
wait "$SCHEMA_DIFF_PID" || schema_diff_status=$?
SCHEMA_DIFF_PID=""
row_match_status=0
wait "$ROW_MATCH_PID" || row_match_status=$?
ROW_MATCH_PID=""
range_diff_status=0
wait "$RANGE_DIFF_PID" || range_diff_status=$?
RANGE_DIFF_PID=""
[[ "$schema_diff_status" -eq 0 && "$row_match_status" -eq 0 && "$range_diff_status" -eq 0 ]]
test -f "$OF33C_ENVDIR/browser-space"
finish_browser

"$PYTHON" -I "$HERE/verify.py" "$WP33C_EVIDENCE" "$STACK_RESULTS" "$OF33C_ENVDIR" \
  "$WP33C_PROJECT" "$GATEWAY" "$REVISION" "$TREE"
cleanup >"$WP33C_EVIDENCE/cleanup.log" 2>&1
trap - EXIT
containers="$(docker ps -aq --filter "$label")"
volumes="$(docker volume ls -q --filter "$label")"
networks="$(docker network ls -q --filter "$label")"
[[ -z "$containers$volumes$networks" ]]
test "$(git rev-parse HEAD)" = "$REVISION"
test -z "$(git status --porcelain)"
mv "$OF33C_ENVDIR/verdict.json" "$WP33C_EVIDENCE/verdict.json"
echo "PASS: $WP33C_EVIDENCE/verdict.json"
