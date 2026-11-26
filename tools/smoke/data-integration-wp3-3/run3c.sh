#!/usr/bin/env bash
# WP3.3 3C smoke orchestrator — real PBS+DVS+PG16 end-to-end memory/receipt.
#
# Only this directory (tools/smoke/data-integration-wp3-3/) and its ignored
# results/ are touched. Services, shared libs, tests baselines and the rest
# of the working tree stay exactly as the parallel 3A/3B sessions left them.
#
# Subcommands: setup build up down down-volumes restart-svcs status logs
#              proxy-selftest dry-run full
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
# One run = one disposable project (review): the run id is REQUIRED and
# validated (lowercase alnum + hyphen, edges alphanumeric, no '..', ≤48).
# It derives the compose project, the env dir under /private/tmp, and the
# evidence dir — nothing is shared, pre-cleaned, or rm -rf'd across runs.
RUN_ID="${OF33C_RUN_ID:?set OF33C_RUN_ID (e.g. 'final'): lowercase alnum + hyphen; names project of-wp33c-<id> and evidence results/3c-<id>}"
PROJECT="of-wp33c-${RUN_ID}"
RESULTS="$HERE/results"
EVIDENCE="$RESULTS/3c-${RUN_ID}"
BIN="$EVIDENCE/bin"
ENVDIR="${OF33C_ENVDIR:-/private/tmp/${PROJECT}}"
DVS_URL="http://127.0.0.1:17178"
PBS_URL="http://127.0.0.1:17181"
PROXY_CTL="http://127.0.0.1:17179"
GO_IMAGE="golang:1.26.11"
GOMODCACHE="/private/tmp/of-gomodcache"
GOCACHE="/private/tmp/of-gocache"

compose() { docker compose --project-directory "$ENVDIR" -f "$ENVDIR/compose.yaml" "$@"; }

die() { echo "FAIL: $*" >&2; exit 2; }

# Safety gate (review): run id charset-checked; env dir must resolve (after
# symlink resolution) under the host temp roots and contain no '..' component.
validate_names() {
  [[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,46}[a-z0-9])?$ ]] \
    || die "OF33C_RUN_ID '$RUN_ID' must be lowercase alnum + hyphen (edges alphanumeric, ≤48)"
  local resolved comp
  resolved="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$ENVDIR")" \
    || die "cannot resolve OF33C_ENVDIR '$ENVDIR'"
  case "$resolved" in
    /private/tmp/*|/tmp/*) : ;;
    *) die "OF33C_ENVDIR '$ENVDIR' resolves to '$resolved' — must stay under /private/tmp or /tmp (symlink escapes are rejected)" ;;
  esac
  local IFS=/
  for comp in $ENVDIR; do
    [[ "$comp" != ".." ]] || die "OF33C_ENVDIR '$ENVDIR' must not contain '..'"
  done
}

# Every required phase must have left its check file behind, and the
# recorded binary hashes must still match the files (no silent swaps).
check_phase_completeness() {
  local missing=0 f
  for f in seed-scale1500 seed-scale15000 seed-full1500 seed-full15000 \
           seed-fstage seed-ftrunc seed-fack seed-vpin \
           memory-run-scale1500 memory-run-scale15000 \
           memory-run-full1500 memory-run-full15000 \
           version-pin-vpin fault-stage-fstage \
           fault-truncate-ftrunc fault-ack-fack; do
    [[ -s "$EVIDENCE/checks/$f.json" ]] || { echo "missing phase evidence: checks/$f.json" >&2; missing=1; }
  done
  for f in fingerprint.json fingerprint_end.json binary-sha256.txt; do
    [[ -s "$EVIDENCE/$f" ]] || { echo "missing evidence file: $f" >&2; missing=1; }
  done
  if [[ -s "$EVIDENCE/binary-sha256.txt" ]]; then
    (cd "$BIN" && shasum -a 256 -c "$EVIDENCE/binary-sha256.txt" >/dev/null 2>&1) \
      || { echo "binary sha256 mismatch vs recorded build hashes" >&2; missing=1; }
  fi
  return "$missing"
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
  # Ports are fixed: this suite runs alone. A port may only be held by THIS
  # run's own project (mid-run restarts); any other holder is a hard stop
  # naming the offender — never a silent pre-clean of someone else's env.
  local own_ports; own_ports="$(docker ps --filter "name=$PROJECT-" --format '{{.Names}} {{.Ports}}' | tr '\n' ';' || true)"
  for p in 17178 17181 17179; do
    if [[ "$own_ports" == *":$p->"* ]]; then continue; fi  # held by THIS run's own project
    if python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',$p));s.close()" 2>/dev/null; then :; else
      echo "FAIL: host port $p is occupied by another environment:" >&2
      docker ps --format '{{.Names}} {{.Ports}}' | grep ":$p->" >&2 || true
      die "tear that environment down (or change this suite's port plan) before starting run '$RUN_ID'"
    fi
  done
  if [[ ! -f "$ENVDIR/.env" ]]; then
    (umask 077; {
      echo "OF33C_PG_PASSWORD=$(openssl rand -hex 24)"
      echo "OF33C_JWT_SECRET=$(openssl rand -hex 32)"
      echo "WP33C_SUB=$(uuidgen | tr 'A-Z' 'a-z')"
    } > "$ENVDIR/.env")
  fi
  sed -e "s|__OF_ROOT__|$REPO|g" -e "s|__OF33C_PROJECT__|$PROJECT|g" \
      -e "s|__RUN_BIN__|$BIN|g" "$HERE/compose-3c.yaml.template" > "$ENVDIR/compose.yaml"
  sed -e "s|__OF_ROOT__|$REPO|g" -e "s|__OF33C_PROJECT__|$PROJECT|g" \
      -e "s|__RUN_BIN__|$BIN|g" -e "s|__FAULT_EVIDENCE__|$EVIDENCE/fault-proxy|g" \
      "$HERE/compose-3c-fault.yaml.template" > "$ENVDIR/compose-3c-fault.yaml"
  echo "envdir: $ENVDIR (project $PROJECT)"
}

mint_token() {
  local secret sub
  secret="$(grep '^OF33C_JWT_SECRET=' "$ENVDIR/.env" | cut -d= -f2-)"
  sub="$(grep '^WP33C_SUB=' "$ENVDIR/.env" | cut -d= -f2-)"
  python3 -I "$REPO/tools/smoke/business-platform-p1/mint_token.py" \
    --secret "$secret" --sub "$sub" --permissions "dataset.write,dataset:write,pipeline.write,pipeline:write"
}

harness() {  # harness phase + args, with env wired
  local token; token="$(mint_token)"
  WP33C_PROJECT="$PROJECT" WP33C_DVS="$DVS_URL" WP33C_PBS="$PBS_URL" \
  WP33C_TOKEN="$token" WP33C_EVIDENCE="$EVIDENCE" WP33C_PROXY_CTL="http://127.0.0.1:17179" \
    python3 -I "$HERE/harness3c.py" "$@"
}

cmd_build() {
  cmd_setup
  mkdir -p "$BIN" "$EVIDENCE"
  local arch; arch="$(uname -m)"; [[ "$arch" == "arm64" ]] || { echo "unexpected arch $arch"; exit 1; }
  python3 -I "$HERE/fingerprint3c.py" "$REPO" "$EVIDENCE"
  local build_log="$EVIDENCE/build.log"
  local pbs_required="${1:-required}"
  local rc_dvs=0 rc_pbs=0
  docker run --rm \
    -v "$REPO":/src:ro -v "$GOMODCACHE":/go/pkg/mod -v "$GOCACHE":/root/.cache/go-build \
    -v "$BIN":/out -w /src -e GOTOOLCHAIN=local -e GOPROXY=off \
    -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 \
    "$GO_IMAGE" sh -c '
      set -ux
      go version
      go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build \
        -ldflags "-s -w -buildid=" -o /out/dataset-versioning-service \
        ./services/dataset-versioning-service/cmd/dataset-versioning-service' \
    | tee "$build_log" || rc_dvs=$?
  if [[ $rc_dvs -ne 0 ]]; then
    docker run --rm -v "$REPO":/src:ro -v "$GOMODCACHE":/go/pkg/mod -v "$GOCACHE":/root/.cache/go-build \
      -v "$BIN":/out -w /src -e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly \
      -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 "$GO_IMAGE" sh -c '
        go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build \
          -ldflags "-s -w -buildid=" -o /out/dataset-versioning-service \
          ./services/dataset-versioning-service/cmd/dataset-versioning-service' \
      | tee -a "$build_log" || rc_dvs=$?
  fi
  [[ $rc_dvs -eq 0 ]] || { echo "FAIL: DVS binary did not build"; exit 1; }
  docker run --rm \
    -v "$REPO":/src:ro -v "$GOMODCACHE":/go/pkg/mod -v "$GOCACHE":/root/.cache/go-build \
    -v "$BIN":/out -w /src -e GOTOOLCHAIN=local -e GOPROXY=off \
    -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 \
    "$GO_IMAGE" sh -c '
      go build -trimpath -buildvcs=false -tags osusergo,netgo,static_build \
        -ldflags "-s -w -buildid=" -o /out/pipeline-build-service \
        ./services/pipeline-build-service/cmd/pipeline-build-service' \
    | tee -a "$build_log" || rc_pbs=$?
  if [[ $rc_pbs -ne 0 ]]; then
    echo "NOTE: PBS build failed (3B in flight?) — pbs_required=$pbs_required" | tee -a "$build_log"
    if [[ "$pbs_required" == "required" ]]; then exit 1; fi
  fi
  chmod +x "$BIN"/*
  shasum -a 256 "$BIN"/* | tee "$EVIDENCE/binary-sha256.txt"
}

cmd_up() {
  [[ -x "$BIN/dataset-versioning-service" ]] || { echo "run '$0 build' first"; exit 1; }
  compose up -d postgres
  compose up -d dvs
  wait_healthy "$DVS_URL/healthz" dvs
  if [[ -x "$BIN/pipeline-build-service" ]]; then
    compose up -d pbs
    wait_healthy "$PBS_URL/healthz" pbs
    echo "up: dvs $DVS_URL pbs $PBS_URL"
  else
    echo "up: dvs $DVS_URL (pbs binary absent — DVS-only mode)"
  fi
}

cmd_restart_svcs() {  # fresh processes (fresh /proc windows), same volumes/DBs
  compose restart dvs pbs
  wait_healthy "$DVS_URL/healthz" dvs
  wait_healthy "$PBS_URL/healthz" pbs
  echo "restarted dvs+pbs (fresh processes)"
}

cmd_fault_route() {  # PBS -> faultproxy -> DVS (ACK-fault scenario only)
  docker compose --project-directory "$ENVDIR" \
    -f "$ENVDIR/compose.yaml" -f "$ENVDIR/compose-3c-fault.yaml" up -d faultproxy
  docker compose --project-directory "$ENVDIR" \
    -f "$ENVDIR/compose.yaml" -f "$ENVDIR/compose-3c-fault.yaml" up -d pbs
  wait_healthy "http://127.0.0.1:17179/healthz" faultproxy-control
  wait_healthy "$PBS_URL/healthz" pbs
  echo "pbs now routed through faultproxy"
}

cmd_down() { validate_names; compose down --remove-orphans; }
cmd_down_volumes() { validate_names; compose down -v --remove-orphans; }
cmd_status() { compose ps; }
cmd_logs() { compose logs --tail "${2:-80}" "${1:-dvs}"; }

cmd_proxy_selftest() { python3 -I "$HERE/test_fault_proxy_3c.py"; }

cmd_dry_run() {
  cmd_build tolerate-pbs
  cmd_up
  cmd_proxy_selftest
  mkdir -p "$EVIDENCE"
  harness seed --rows 300 --tag dryrun
  harness dvs-chain
  echo "dry-run complete (evidence: $EVIDENCE)"
}

fingerprint_end() {  # review: freeze the source manifest again at suite END
  python3 -I "$HERE/fingerprint3c.py" "$REPO" "$EVIDENCE" --mode end
}

cmd_full() {
  validate_names
  # Review: never rm -rf a path the environment can point anywhere. A full
  # run REQUIRES a fresh run dir; reuse is refused with the exact manual
  # command so destruction stays a deliberate human action. There is also
  # NO pre-`down -v`: each run has its OWN project (fresh empty volumes by
  # construction); if the ports are held by anyone, setup names them and
  # stops — a new run never cleans a same-named/foreign old database.
  if [[ -e "$EVIDENCE" ]]; then
    die "run dir already exists: $EVIDENCE — pick a fresh OF33C_RUN_ID, or remove it yourself:
  rm -rf '$EVIDENCE'"
  fi
  cmd_build required
  cmd_up
  cmd_proxy_selftest | tee "$EVIDENCE/proxy-selftest.log"

  harness seed --rows 1500  --tag scale1500
  harness seed --rows 1500  --tag fstage
  harness seed --rows 1500  --tag fack
  harness seed --rows 1500  --tag ftrunc
  harness seed --rows 1500  --tag vpin
  harness seed --rows 15000 --tag scale15000
  # Review pair: no-filter full copy — the OUTPUT crosses the 24 MiB delta
  # gate (5.9 MiB -> 58.6 MiB), so whole-output buffering cannot pass.
  harness seed --rows 1500  --tag full1500
  harness seed --rows 15000 --tag full15000

  cmd_restart_svcs
  harness memory-run --rows 1500  --tag scale1500 | tee "$EVIDENCE/mem-scale1500.log"
  cmd_restart_svcs
  harness memory-run --rows 15000 --tag scale15000 | tee "$EVIDENCE/mem-scale15000.log"
  cmd_restart_svcs
  harness memory-run --rows 1500  --tag full1500 --mode full | tee "$EVIDENCE/mem-full1500.log"
  cmd_restart_svcs
  harness memory-run --rows 15000 --tag full15000 --mode full | tee "$EVIDENCE/mem-full15000.log"

  # version pin: poison head AFTER CreateBuild, outside memory windows
  harness version-pin --tag vpin | tee "$EVIDENCE/version-pin.log"
  # supplementary negative: invalid predicate (scan-side staging failure)
  harness fault-stage --tag fstage | tee "$EVIDENCE/fault-stage.log"

  # fault phase through the chunked-capable proxy
  cmd_fault_route
  harness fault-truncate --tag ftrunc | tee "$EVIDENCE/fault-truncate.log"
  harness fault-ack --tag fack | tee "$EVIDENCE/fault-ack.log"

  fingerprint_end
  # Review: exit-code integrity. pipefail stays ON; under `set +e` the `$?`
  # after each FULL pipeline reflects ANY failing element — harness OR tee
  # — so nothing can swallow a failure into a green exit. Phase
  # completeness and the recorded binary hashes are verified after, and the
  # final exit is the truthful AND of all three.
  local report_rc=0 hygiene_rc=0 phase_rc=0
  set +e
  harness report 2>&1 | tee "$EVIDENCE/report.log"
  report_rc=$?
  hygiene_scan 2>&1 | tee "$EVIDENCE/hygiene.log"
  hygiene_rc=$?
  check_phase_completeness 2>&1 | tee "$EVIDENCE/phase-completeness.log"
  phase_rc=$?
  set -e
  echo "verdict: report_rc=$report_rc hygiene_rc=$hygiene_rc phase_rc=$phase_rc"
  [[ "$report_rc" -eq 0 && "$hygiene_rc" -eq 0 && "$phase_rc" -eq 0 ]]
}

hygiene_scan() {  # no secret/JWT values in anything this suite wrote
  python3 - "$ENVDIR/.env" "$HERE" "$EVIDENCE" <<'PYEOF'
import os, re, sys
envfile, here, ev = sys.argv[1:4]
secrets = {}
for line in open(envfile):
    if "=" in line:
        k, v = line.strip().split("=", 1)
        if k != "WP33C_SUB" and len(v) > 8:
            secrets[k] = v
bad = []
for root in (here, ev):
    for r, _, files in os.walk(root):
        for fn in files:
            p = os.path.join(r, fn)
            try:
                data = open(p, "rb").read()
            except OSError:
                continue
            for k, v in secrets.items():
                if v.encode() in data:
                    bad.append((p, k))
            if re.search(rb"eyJ[A-Za-z0-9_-]{20,}", data):  # JWT-shaped blobs
                bad.append((p, "JWT-shaped"))
print("hygiene:", ("FAIL " + repr(bad)) if bad else "PASS (no secret values, no JWT-shaped blobs)")
sys.exit(1 if bad else 0)
PYEOF
}

cmd_wait_3b_then_full() {
  # Quiescence gate for the parallel 3B session: PBS must compile AND no
  # PBS/dataset Go file may have been modified in the last 180s. Then one
  # final full run (its fingerprint pins whatever tree it measured).
  local deadline=$(( $(date +%s) + 2700 )) last=""
  while [[ $(date +%s) -lt $deadline ]]; do
    if docker run --rm -v "$REPO":/src:ro -v "$GOMODCACHE":/go/pkg/mod \
        -v "$GOCACHE":/root/.cache/go-build -w /src -e GOTOOLCHAIN=local \
        -e GOPROXY=off -e GOFLAGS=-mod=readonly -e CGO_ENABLED=0 \
        golang:1.26.11 go build -o /dev/null ./services/pipeline-build-service/... 2>/dev/null; then
      last="$(find services/pipeline-build-service libs -name '*.go' -newermt '-180 seconds' 2>/dev/null | head -1)"
      if [[ -z "$last" ]]; then
        echo "3B quiescent (compiles, no edits for 180s) — starting full run"
        cmd_full
        return
      fi
      echo "compiles but recent edits (${last##*/}) — waiting"
    else
      echo "PBS does not compile (3B mid-edit) — waiting"
    fi
    sleep 60
  done
  echo "FAIL: 3B never quiesced within 45min"; exit 1
}

case "${1:-}" in
  setup|build|up|down|down-volumes|restart-svcs|status|proxy-selftest|dry-run|full|wait-3b-then-full) "cmd_${1//-/_}" "${@:2}" ;;
  logs) cmd_logs "${@:2}" ;;
  *) echo "usage: $0 {setup|build|up|down|down-volumes|restart-svcs|status|logs|proxy-selftest|dry-run|full|wait-3b-then-full}"; exit 2 ;;
esac
