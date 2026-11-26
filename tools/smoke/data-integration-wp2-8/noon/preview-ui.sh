#!/usr/bin/env bash
# The real frontend page of a case, behind the run's real gateway: the browser script of the
# directory named second, or of this one (the published noon order lines).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
state=$1
pages=${2:-$here}
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
server=""
finish_browser() {
  if [[ -f "$state/browser-space" ]]; then
    ego-browser nodejs -e "await (await taskSpace($(cat "$state/browser-space"))).finish({ keep: [] });" \
      >>"$state/preview-browser.log" 2>&1
  fi
}
cleanup() {
  local status=$?
  finish_browser || status=1
  if [[ -n "$server" ]]; then
    kill "$server" 2>/dev/null || true
    wait "$server" 2>/dev/null || true
  fi
  return "$status"
}
trap cleanup EXIT
(
  cd "$root/apps/web"
  P31_WEB_PORT="$port" P31_GW_URL="$WP23_GATEWAY" exec node node_modules/vite/bin/vite.js --config vite.p31-e2e.config.ts
) >"$state/preview-web.log" 2>&1 &
server=$!
origin="http://127.0.0.1:$port"
curl -fsS --retry 30 --retry-all-errors --retry-delay 1 "$origin" >/dev/null
{
  python3 -I -c 'import json,sys; keys=("origin","state"); print("const input = " + json.dumps(dict(zip(keys, sys.argv[1:], strict=True))) + ";")' "$origin" "$state"
  cat "$pages/preview-browser.mjs"
} | ego-browser nodejs >"$state/preview-browser.log" 2>&1
