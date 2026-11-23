#!/usr/bin/env bash
# Each invocation owns a fresh Compose project and keeps only its evidence.
set -euo pipefail
umask 077
here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
revision=$(git -C "$root" rev-parse HEAD)
if [[ -n "$(git -C "$root" status --porcelain)" ]]; then
  echo "Run acceptance from a clean checkout of the candidate commit." >&2
  exit 1
fi
state=$(mktemp -d "${TMPDIR:-/tmp}/wp23-orders.XXXXXXXX")
export WP23_DVS_PORT
WP23_DVS_PORT=${WP23_DVS_PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')}
export BIN_DIR="$state/bin"
mkdir -p "$BIN_DIR"
project="wp23-$(openssl rand -hex 6)"
export DB_PASSWORD JWT_SECRET CREDENTIAL_KEY STORE_ACCESS STORE_SECRET
DB_PASSWORD=$(openssl rand -hex 20)
JWT_SECRET=$(openssl rand -hex 32)
CREDENTIAL_KEY=$(openssl rand -base64 32)
STORE_ACCESS=$(openssl rand -hex 10)
STORE_SECRET=$(openssl rand -hex 24)
compose=(docker compose -p "$project" -f "$here/compose.yaml")
echo "Evidence directory: $state"
cleanup() {
  result=$?
  if (( result != 0 )); then
    "${compose[@]}" ps -a
    # Keep logs private in the run directory, separate from the handoff.
    "${compose[@]}" logs --no-color >"$state/services.log" 2>&1
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null
}
trap cleanup EXIT
docker run --rm -v "$root:/src:ro" -v "$BIN_DIR:/out" \
  -v "${WP23_GO_MOD_CACHE:-wp23-go-mod}:/go/pkg/mod" \
  -v "${WP23_GO_BUILD_CACHE:-wp23-go-build}:/root/.cache/go-build" \
  -w /src -e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 golang:1.26.3 \
  go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build -ldflags '-s -w -buildid=' -o /out/ \
  ./services/{dataset-versioning-service,pipeline-build-service,connector-management-service}/cmd/...
"${compose[@]}" up -d
"${compose[@]}" images --format json >"$state/images.json"
(cd "$BIN_DIR" && shasum -a 256 *-service) >"$state/services.sha256"
endpoint() { printf 'http://%s' "$("${compose[@]}" port "$1" "$2")"; }
export WP23_DVS WP23_PBS WP23_CONNECTOR WP23_STORE
WP23_DVS=$(endpoint dataset 50078)
WP23_PBS=$(endpoint pipeline 50081)
WP23_CONNECTOR=$(endpoint connector 50088)
WP23_STORE=$(endpoint store 9000)
for url in "$WP23_DVS/healthz" "$WP23_PBS/healthz" "$WP23_CONNECTOR/healthz" "$WP23_STORE/health"; do
  curl -fsS --retry 30 --retry-all-errors --retry-delay 1 --max-time 3 "$url" >/dev/null
done
python3 -I "$here/orders.py" "$state"
test "$(git -C "$root" rev-parse HEAD)" = "$revision"
test -z "$(git -C "$root" status --porcelain)"
