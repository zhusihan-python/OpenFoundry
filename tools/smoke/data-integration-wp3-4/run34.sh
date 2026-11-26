#!/usr/bin/env bash
# WP3.4 pinned-input smoke orchestrator — real PBS+DVS+PG16 end-to-end.
#
# Only this directory (tools/smoke/data-integration-wp3-4/) and its ignored
# results/ are touched. The WP3.3 suite, service sources beyond the WP3.4
# product paths, tests baselines and the rest of the working tree stay as
# they are.
#
# Subcommands: setup build up down down-volumes restart-pbs status logs full
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
RUN_ID="${OF34_RUN_ID:?set OF34_RUN_ID (e.g. 'r1'): lowercase alnum + hyphen; names project of-wp34-<id> and evidence results/wp34-<id>}"
PROJECT="of-wp34-${RUN_ID}"
RESULTS="$HERE/results"
EVIDENCE="$RESULTS/wp34-${RUN_ID}"
BIN="$EVIDENCE/bin"
ENVDIR="/private/tmp/${PROJECT}"
DVS_URL="http://127.0.0.1:17278"
PBS_URL="http://127.0.0.1:17281"
GO_IMAGE="golang:1.26.11"
GOMODCACHE="/private/tmp/of-gomodcache"
GOCACHE="/private/tmp/of-gocache"

compose() { docker compose --project-name "$PROJECT" --project-directory "$ENVDIR" -f "$ENVDIR/compose.yaml" "$@"; }

die() { echo "FAIL: $*" >&2; exit 2; }

validate_names() {
  [[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,46}[a-z0-9])?$ ]] \
    || die "OF34_RUN_ID '$RUN_ID' must be lowercase alnum + hyphen (edges alphanumeric, ≤48)"
  [[ ! -L "$ENVDIR" ]] || die "environment directory must not be a symlink: $ENVDIR"
}

wait_healthy() {
  local url="$1" name="$2" i
  for ((i = 0; i < 90; i++)); do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "FAIL: $name not healthy at $url" >&2; return 1
}

cmd_setup() {
  validate_names
  mkdir -p "$ENVDIR"; chmod 700 "$ENVDIR"
  local own_ports; own_ports="$(docker ps --filter "name=$PROJECT-" --format '{{.Names}} {{.Ports}}' | tr '\n' ';' || true)"
  for p in 17278 17281; do
    if [[ "$own_ports" == *":$p->"* ]]; then continue; fi
    if python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',$p));s.close()" 2>/dev/null; then :; else
      echo "FAIL: host port $p is occupied by another environment:" >&2
      docker ps --format '{{.Names}} {{.Ports}}' | grep ":$p->" >&2 || true
      die "tear that environment down before starting run '$RUN_ID'"
    fi
  done
  if [[ ! -f "$ENVDIR/.env" ]]; then
    (umask 077; {
      echo "OF34_PG_PASSWORD=$(openssl rand -hex 24)"
      echo "OF34_JWT_SECRET=$(openssl rand -hex 32)"
      echo "WP34_SUB=$(uuidgen | tr 'A-Z' 'a-z')"
    } > "$ENVDIR/.env")
  fi
  sed -e "s|__OF_ROOT__|$REPO|g" -e "s|__OF34_PROJECT__|$PROJECT|g" \
      -e "s|__RUN_BIN__|$BIN|g" "$HERE/compose-wp34.yaml.template" > "$ENVDIR/compose.yaml"
  echo "envdir: $ENVDIR (project $PROJECT)"
}

mint_token() {
  local secret sub
  secret="$(grep '^OF34_JWT_SECRET=' "$ENVDIR/.env" | cut -d= -f2-)"
  sub="$(grep '^WP34_SUB=' "$ENVDIR/.env" | cut -d= -f2-)"
  python3 -I "$REPO/tools/smoke/business-platform-p1/mint_token.py" \
    --secret "$secret" --sub "$sub" --permissions "dataset.write,dataset:write,pipeline.write,pipeline:write"
}

harness() {
  local token; token="$(mint_token)"
  WP34_PROJECT="$PROJECT" WP34_DVS="$DVS_URL" WP34_PBS="$PBS_URL" \
  WP34_TOKEN="$token" WP34_EVIDENCE="$EVIDENCE" \
    python3 -I "$HERE/harness34.py" "$@"
}

cmd_build() {
  cmd_setup
  mkdir -p "$BIN" "$EVIDENCE"
  local build_log="$EVIDENCE/build.log"
  docker run --rm \
    -v "$REPO":/src:ro -v "$GOMODCACHE":/go/pkg/mod -v "$GOCACHE":/root/.cache/go-build \
    -v "$BIN":/out -w /src -e GOTOOLCHAIN=local -e GOPROXY=off \
    -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 \
    "$GO_IMAGE" sh -c '
      set -eux
      go version
      go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build \
        -ldflags "-s -w -buildid=" -o /out/dataset-versioning-service \
        ./services/dataset-versioning-service/cmd/dataset-versioning-service
      go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build \
        -ldflags "-s -w -buildid=" -o /out/pipeline-build-service \
        ./services/pipeline-build-service/cmd/pipeline-build-service' \
    | tee "$build_log"
  chmod +x "$BIN"/*
  shasum -a 256 "$BIN"/* | tee "$EVIDENCE/binary-sha256.txt"
}

cmd_up() {
  [[ -x "$BIN/pipeline-build-service" ]] || { echo "run '$0 build' first"; exit 1; }
  compose up -d postgres
  compose up -d dvs
  wait_healthy "$DVS_URL/healthz" dvs
  compose up -d pbs
  wait_healthy "$PBS_URL/healthz" pbs
  echo "up: dvs $DVS_URL pbs $PBS_URL"
}

cmd_restart_pbs() {
  local before; before="$(docker inspect --format '{{.State.StartedAt}}' "${PROJECT}-pbs" 2>/dev/null || true)"
  compose restart pbs
  wait_healthy "$PBS_URL/healthz" pbs
  local after; after="$(docker inspect --format '{{.State.StartedAt}} restarts={{.RestartCount}}' "${PROJECT}-pbs")"
  [[ -n "$before" && -n "$after" && "$before" != "$after" ]] || die "PBS start time did not change"
  if [[ -d "$EVIDENCE" && -n "$after" ]]; then
    printf '%s -> %s\n' "${before:-unknown}" "$after" >> "$EVIDENCE/pbs-restarts.log"
  fi
  echo "restarted pbs (fresh process, same volumes/DBs): $after"
}

fingerprint() {
  python3 -I "$REPO/tools/smoke/data-integration-wp3-3/fingerprint3c.py" "$REPO" "$EVIDENCE" --mode "$1"
}

check_fingerprint() {
  python3 -I - "$EVIDENCE" <<'PY'
import json
from pathlib import Path
import sys

evidence = Path(sys.argv[1])
start = json.loads((evidence / "fingerprint.json").read_text())
end = json.loads((evidence / "fingerprint_end.json").read_text())
if start["working_tree_fingerprint"] != end["working_tree_fingerprint"]:
    raise SystemExit("FAIL: source changed during the WP3.4 smoke run")
print(f"source fingerprint verified at {start['head']}")
PY
  shasum -a 256 -c "$EVIDENCE/binary-sha256.txt"
}

cmd_down() { validate_names; compose down --remove-orphans; }
cmd_down_volumes() { validate_names; compose down -v --remove-orphans; }
cmd_status() { compose ps; }
cmd_logs() { compose logs --tail "${2:-80}" "${1:-dvs}"; }

cmd_full() {
  validate_names
  if [[ -e "$EVIDENCE/handles" ]]; then
    die "run dir already has handles: $EVIDENCE — pick a fresh OF34_RUN_ID or remove it yourself:
  rm -rf '$EVIDENCE'"
  fi
  mkdir -p "$EVIDENCE"
  fingerprint start
  cmd_build
  cmd_up

  harness seed --rows 12 --tag main
  harness graph-pin --tag main | tee "$EVIDENCE/graph-a.log"
  harness poison --tag main | tee "$EVIDENCE/poison.log"
  cmd_restart_pbs
  harness graph-pin-b --tag main | tee "$EVIDENCE/graph-b.log"
  harness canonical-pin --tag main | tee "$EVIDENCE/canonical.log"
  cmd_restart_pbs
  harness canonical-verify --tag main | tee "$EVIDENCE/canonical-verify.log"
  harness bad-txn --tag main | tee "$EVIDENCE/bad-txn.log"

  fingerprint end
  check_fingerprint
  set +e
  harness report | tee "$EVIDENCE/report.log"
  local report_rc=$?
  set -e
  echo "verdict: report_rc=$report_rc"
  [[ "$report_rc" -eq 0 ]]
}

case "${1:-}" in
  setup|build|up|down|down-volumes|restart-pbs|status|logs|full) "cmd_${1//-/_}" "$@" ;;
  phase) shift; harness "$@" ;;
  *) echo "usage: $0 {setup|build|up|down|down-volumes|restart-pbs|status|logs|phase|full}" >&2; exit 2 ;;
esac
