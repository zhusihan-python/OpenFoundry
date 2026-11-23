#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
state=$1
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
(
  cd "$here/../../../apps/web"
  P31_WEB_PORT="$port" P31_GW_URL="$WP23_CONNECTOR" exec node node_modules/vite/bin/vite.js --config vite.p31-e2e.config.ts
) >"$state/vite.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT
origin="http://127.0.0.1:$port"
curl -fsS --retry 30 --retry-all-errors --retry-delay 1 "$origin" >/dev/null
{
  python3 -c 'import json,sys; print("const input = " + json.dumps({"origin":sys.argv[1],"state":sys.argv[2]}) + ";")' "$origin" "$state"
  cat "$here/catalog-ui.mjs"
} | ego-browser nodejs
space=$(cat "$state/browser-space")
[[ "$space" =~ ^[0-9]+$ ]]
ego-browser nodejs -e "await (await taskSpace($space)).finish({ keep: [] });"
