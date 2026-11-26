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
source_type=${1:-s3}
orders_script="$here/orders.py"
table_fixture=""
gateway=0
page_downloads=0
case "$source_type" in
  s3) ;;
  noon) orders_script="$here/../data-integration-wp2-8/noon/smoke.py" ;;
  noon-preview) orders_script="$here/../data-integration-wp2-8/noon/preview.py"; gateway=1 ;;
  amazon) orders_script="$here/../data-integration-wp2-8/amazon/smoke.py" ;;
  amazon-preview) orders_script="$here/../data-integration-wp2-8/amazon/preview.py"; gateway=1; page_downloads=1 ;;
  lingxing) orders_script="$here/../data-integration-wp2-8/smoke.py" ;;
  postgresql) table_fixture="$here/../data-integration-wp2-5" ;;
  mysql) table_fixture="$here/../data-integration-wp2-4" ;;
  *) echo "usage: $0 [s3|postgresql|mysql|lingxing|noon|amazon|noon-preview|amazon-preview]" >&2; exit 2 ;;
esac
if (( gateway )); then
  command -v ego-browser >/dev/null || { echo "$source_type needs ego-browser on PATH" >&2; exit 2; }
  [[ -f "$root/apps/web/node_modules/vite/bin/vite.js" ]] ||
    { echo "$source_type needs the installed web workspace: pnpm install --frozen-lockfile" >&2; exit 2; }
fi
state=$(mktemp -d "${TMPDIR:-/tmp}/wp23-orders.XXXXXXXX")
export WP23_DVS_PORT
WP23_DVS_PORT=${WP23_DVS_PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')}
export BIN_DIR="$state/bin"
mkdir -p "$BIN_DIR"
project="wp23-$(openssl rand -hex 6)"
export DB_PASSWORD JWT_SECRET CREDENTIAL_KEY STORE_ACCESS STORE_SECRET WP23_GATEWAY_CONTAINER WP23_FORWARDER_CONTAINER
DB_PASSWORD=$(openssl rand -hex 20)
JWT_SECRET=$(openssl rand -hex 32)
CREDENTIAL_KEY=$(openssl rand -base64 32)
STORE_ACCESS=$(openssl rand -hex 10)
STORE_SECRET=$(openssl rand -hex 24)
WP23_GATEWAY_CONTAINER="$project-gateway"
WP23_FORWARDER_CONTAINER="$project-forwarder"
compose=(docker compose -p "$project" -f "$here/compose.yaml")
if [[ -n "$table_fixture" ]]; then
  export WP25_ADMIN_PASSWORD WP25_READER_PASSWORD WP25_OTHER_PASSWORD WP25_TLS
  WP25_ADMIN_PASSWORD=$(openssl rand -hex 24)
  WP25_READER_PASSWORD=$(openssl rand -hex 24)
  WP25_OTHER_PASSWORD=$(openssl rand -hex 24)
  WP25_TLS=$(mktemp -d)
  compose+=(-f "$table_fixture/compose.yaml")
  orders_script="$table_fixture/orders.py"
fi
echo "Evidence directory: $state"
cleanup() {
  result=$?
  if (( result != 0 )); then
    "${compose[@]}" ps -a
    # Keep logs private in the run directory, separate from the handoff.
    "${compose[@]}" logs --no-color >"$state/services.log" 2>&1
  fi
  if (( page_downloads )); then docker rm -f "$WP23_FORWARDER_CONTAINER" >/dev/null 2>&1 || true; fi
  if (( gateway )); then docker rm -f "$WP23_GATEWAY_CONTAINER" >/dev/null 2>&1 || true; fi
  "${compose[@]}" down -v --remove-orphans >/dev/null
  if [[ -n "$table_fixture" ]]; then
    rm -f "$WP25_TLS/server.key" "$WP25_TLS/server.crt" "$WP25_TLS/openssl.cnf"
    rmdir "$WP25_TLS"
  fi
}
trap cleanup EXIT
if [[ -n "$table_fixture" ]]; then
  cat > "$WP25_TLS/openssl.cnf" <<'CERT'
[req]
prompt=no
distinguished_name=dn
x509_extensions=extensions
[dn]
CN=orders-db
[extensions]
subjectAltName=DNS:orders-db
basicConstraints=critical,CA:TRUE
extendedKeyUsage=serverAuth
CERT
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -config "$WP25_TLS/openssl.cnf" \
    -keyout "$WP25_TLS/server.key" -out "$WP25_TLS/server.crt" 2>"$state/cert-create.log"
  chmod 755 "$WP25_TLS"
  chmod 644 "$WP25_TLS/server.crt"
fi
build=(./services/{dataset-versioning-service,pipeline-build-service,connector-management-service}/cmd/...)
if (( gateway )); then build+=(./services/edge-gateway-service/cmd/edge-gateway-service); fi
if (( page_downloads )); then build+=(./tools/smoke/data-integration-wp2-3/loopback-forwarder); fi
docker run --rm -v "$root:/src:ro" -v "$BIN_DIR:/out" \
  -v "${WP23_GO_MOD_CACHE:-wp23-go-mod}:/go/pkg/mod" \
  -v "${WP23_GO_BUILD_CACHE:-wp23-go-build}:/root/.cache/go-build" \
  -w /src -e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 golang:1.26.11 \
  go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build -ldflags '-s -w -buildid=' -o /out/ "${build[@]}"
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
if [[ -n "$table_fixture" ]]; then
  export WP25_DB_CONTAINER
  WP25_DB_CONTAINER=$("${compose[@]}" ps -q orders-db)
fi
if (( gateway )); then
  # The real edge gateway of the web page, forwarding /api to this stack's dataset service.
  export WP23_GATEWAY WP23_GATEWAY_PORT
  WP23_GATEWAY_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  docker run -d --name "$WP23_GATEWAY_CONTAINER" --network "${project}_default" \
    -v "$BIN_DIR:/binaries:ro" -p "127.0.0.1:$WP23_GATEWAY_PORT:8080" \
    -e OF_SERVER__PORT=8080 -e OF_JWT__SECRET="$JWT_SECRET" \
    -e OF_UPSTREAM__DATASET_VERSIONING_SERVICE_URL=http://dataset:50078 \
    --entrypoint /binaries/edge-gateway-service golang:1.26.11 >/dev/null
  WP23_GATEWAY="http://127.0.0.1:$WP23_GATEWAY_PORT"
  curl -fsS --retry 30 --retry-all-errors --retry-delay 1 --max-time 3 "$WP23_GATEWAY/healthz" >/dev/null
fi
if (( page_downloads )); then
  # A page download goes through the gateway, which follows the dataset service's redirect to the
  # signed address of the file: the files base URL, on the host's loopback, out of reach from the
  # gateway's container. The forwarder answers that address in the gateway's own network namespace
  # and connects each caller to the dataset service. It binds the loopback alone, so nothing off
  # the container reaches it, and the signed address keeps a host other than the service's own,
  # where a client leaves a bearer credential behind on a redirect.
  docker run -d --name "$WP23_FORWARDER_CONTAINER" --network "container:$WP23_GATEWAY_CONTAINER" \
    -v "$BIN_DIR:/binaries:ro" \
    --entrypoint /binaries/loopback-forwarder golang:1.26.11 \
    -listen-port "$WP23_DVS_PORT" -target dataset:50078 >/dev/null
fi
python3 -I "$orders_script" "$state"
test "$(git -C "$root" rev-parse HEAD)" = "$revision"
test -z "$(git -C "$root" status --porcelain)"
