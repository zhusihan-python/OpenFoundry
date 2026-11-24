#!/usr/bin/env bash
set -euo pipefail
umask 077
here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
state=$(mktemp -d "${TMPDIR:-/tmp}/wp15-orders.XXXXXXXX")
echo "Evidence directory: $state"
export BIN_DIR="$state/bin" CDC_SECRETS
CDC_SECRETS=$(mktemp -d)
mkdir -p "$BIN_DIR"
export COMPOSE_PROJECT_NAME="wp15-$(openssl rand -hex 6)" COMPOSE_FILE="$here/compose.yaml"
export SOURCE_PASSWORD DB_PASSWORD WORKER_PASSWORD READER_PASSWORD JWT_SECRET WP23_DVS_PORT
SOURCE_PASSWORD=$(openssl rand -hex 24)
DB_PASSWORD=$(openssl rand -hex 24)
WORKER_PASSWORD=$(openssl rand -hex 24)
READER_PASSWORD=$(openssl rand -hex 24)
JWT_SECRET=$(openssl rand -hex 32)
WP23_DVS_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
cleanup(){
  rc=$?
  if ((rc!=0)); then docker compose logs --no-color worker >"$state/worker.log" 2>&1 || true; fi
  docker compose down -v --remove-orphans >/dev/null
  rm -f "$CDC_SECRETS/writer"
  rmdir "$CDC_SECRETS"
}
trap cleanup EXIT
docker run --rm -v "$root:/src:ro" -v "$BIN_DIR:/out" -v "${WP23_GO_MOD_CACHE:-wp23-go-mod}:/go/pkg/mod" -v "${WP23_GO_BUILD_CACHE:-wp23-go-build}:/root/.cache/go-build" -w /src -e CGO_ENABLED=0 -e GOTOOLCHAIN=local golang:1.25.14 go build -mod=readonly -trimpath -buildvcs=false -o /out/ ./services/ingestion-replication-service/cmd/orders-cdc ./services/dataset-versioning-service/cmd/dataset-versioning-service
docker compose up -d --wait source postgres kafka connect dataset
docker compose images --format json >"$state/images.json"
python3 -I "$here/orders.py" "$state"
