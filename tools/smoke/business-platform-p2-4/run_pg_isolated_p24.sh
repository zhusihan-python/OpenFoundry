#!/bin/sh
#
# OpenFoundry P2.4 — ISOLATED PostgreSQL 16 test driver (PG-track exclusive).
#
# Preparation contract: handoffs/2026-11-14-p24/formal-execution-contract.md
# ("Isolated PG development/full test ownership") + review-pg-round1.md.
# Pattern follows the P23 isolated-PG evidence
# (/private/tmp/openfoundry-p2-3-evidence/pg-source-admission/08_commands.txt):
# a private docker network, a postgres:16 fixture with NO host ports, the
# repo mounted READ-ONLY, the existing shared Go cache volumes, and every Go
# invocation inside the network. It NEVER touches q3v9: no compose, no base
# PG, no migrate_fresh, no P1/run.sh, no formal pg59 wrapper (those point at
# the q3 fixture and would drop/recreate its databases).
#
# This script is PREPARED, not yet authorized to run: starting PG / running
# DB tests waits for root's explicit signal after the corrected test source
# review. Syntax checks (`sh -n`) and dry listing are safe.
#
# Modes:
#   up           create the private network + of-p24-pg (postgres:16, both
#                carrying the ownership label of-p24-owner=pg-isolated),
#                wait READY, ASSERT zero published host ports and server
#                major 16 as recorded evidence, and write the 0600 secret
#                file (password + maintenance DSN) into PRIVATE TMP — never
#                into the evidence directory, so no archive can sweep it.
#                The password is generated per `up` and NEVER echoed.
#   new-normal   NEW tests only: golang:1.26.3-alpine3.22, CGO_ENABLED=0,
#                `go vet -tags=integration && go test -count=1 -v
#                -run '^TestPGSchedule'` on the connector repo package.
#   new-race     NEW tests only: golang:1.25 (Go 1.25.14), CGO_ENABLED=1,
#                `-race -count=1 -v`, separate log (never -count=2).
#   unified      ONE fresh run of the frozen P1 runner
#                `tools/smoke/business-platform-p1/run_tests.sh integration`
#                inside the same isolated PG: a FRESH dataset database is
#                created for DATASET_DB_DSN; PIPELINE_DB_DSN/CONNECTOR_DB_DSN
#                point at the maintenance `postgres` database (their helpers
#                create fresh per-test databases themselves). The raw log is
#                the unified old59+newN evidence for the Tools exact checker.
#   down         remove of-p24-pg and of-p24-pg-net (exact names only).
#   status       print container/network presence + evidence listing. No
#                secrets are ever printed.
#
# Evidence discipline (every attempt):
#   <phase>_<UTC>.raw   the docker stdout+stderr via DIRECT redirection
#                       (`> file 2>&1`, no pipes/tee/grep anywhere)
#   <phase>_<UTC>.rc    the REAL docker exit code, printf'd on the line
#                       right after the command returns (failures preserved,
#                       never overwritten — unique UTC names, collision
#                       suffix); a failure does NOT block later attempts
#   <phase>_<UTC>.cmd   the exact command with the DSN password redacted
#   <phase>_<UTC>.meta  actual UTC timestamp, git HEAD, dirty-tree sha256,
#                       image tag, phase, mounts
# The timestamps come from the actual clock (`date -u`), never from a manual
# label. Source changes DURING a run are diagnosable via the recorded
# HEAD/dirty hash (the mount is read-only, so in-flight host edits are the
# only source of drift and are flagged in the meta for root's review).

set -eu

SRC="${P24_SRC:-/Volumes/ExternalSD/projects/OpenFoundry}"
EV="${P24_EVIDENCE:-/private/tmp/openfoundry-p24-repair-evidence/pg}"
NET="${P24_NET:-of-p24-pg-net}"
PG="${P24_PG:-of-p24-pg}"
PGUSER="${P24_PGUSER:-ofp24}"
GOMOD_CACHE="${P24_GOMOD_CACHE:-openfoundry-gomod}"   # existing P23 cache
GOBUILD_CACHE="${P24_GOBUILD_CACHE:-openfoundry-gobuild}"
IMG_NORMAL="${P24_IMG_NORMAL:-golang:1.26.3-alpine3.22}"
IMG_RACE="${P24_IMG_RACE:-golang:1.25}"               # Go 1.25.14
OWNER_LABEL="of-p24-owner=pg-isolated"
# The secret env file lives OUTSIDE the evidence directory so no standalone
# or final archive can ever sweep it. 0600, private TMP only, values never
# printed; removed by `down`.
SECRET_FILE="${P24_SECRET_FILE:-/private/tmp/openfoundry-p24-pg.env}"

# Evidence dir must exist BEFORE anything redirects into it.
mkdir -p "$EV"

die() { printf '[p24-pg] ERROR: %s\n' "$*" >&2; exit 2; }

now_utc() { date -u +%Y%m%dT%H%M%SZ; }

# source_tree_hash prints a DETERMINISTIC content hash of everything the Go
# runs compile from: the three business services' Go/SQL trees, the in-repo
# cron library, and the root module graph. A dirty-path LIST cannot detect a
# file whose content changed while its path stayed dirty — this can.
# Evidence/secrets are never part of the hashed set.
source_tree_hash() {
  {
    printf 'go.mod\n'
    cat "$SRC/go.mod"
    printf 'go.sum\n'
    cat "$SRC/go.sum"
    printf 'content-hashes\n'
    find "$SRC/services/connector-management-service" \
         "$SRC/services/dataset-versioning-service" \
         "$SRC/services/pipeline-build-service" \
         "$SRC/libs/scheduling-cron" \
         -type f \( -name '*.go' -o -name '*.sql' \) -print0 \
      | sort -z | xargs -0 shasum -a 256
  } | shasum -a 256 | cut -d' ' -f1
}

# record_source_meta appends before/after content hashes + drift flag to the
# attempt meta (call with "before" first, then "after" the docker attempt).
record_source_meta() {
  phase_kind="$1" # before|after
  hash="$(source_tree_hash)"
  printf 'source_content_sha256_%s=%s\n' "$phase_kind" "$hash" >> "$META"
  if [ "$phase_kind" = after ] && [ "$hash" != "$HASH_BEFORE" ]; then
    printf 'SOURCE_DRIFT_DURING_RUN=1\n' >> "$META"
  fi
  [ "$phase_kind" = before ] && HASH_BEFORE="$hash" || true
  return 0
}

# fresh_attempt <phase> -> sets RAW/RCF/CMD/META with a unique actual-clock
# name; existing files are NEVER overwritten (collision => -2, -3 suffix).
fresh_attempt() {
  phase="$1"
  base="$EV/${phase}_$(now_utc)"
  n=1
  while [ -e "${base}.raw" ]; do
    n=$((n + 1))
    base="$EV/${phase}_$(now_utc)-$n"
  done
  RAW="${base}.raw"; RCF="${base}.rc"; CMD="${base}.cmd"; META="${base}.meta"
}

# write_meta records the frozen source identity of the attempt. The trailing
# extra line uses an explicit if — a bare `[ -n ] && printf` would return 1
# on an empty extra under set -e and abort the mode.
write_meta() {
  phase="$1"; image="$2"; extra="${3:-}"
  head=$(git -C "$SRC" rev-parse HEAD 2>/dev/null || printf 'unknown')
  dirty=$(git -C "$SRC" status --porcelain 2>/dev/null | shasum -a 256 | cut -d' ' -f1)
  {
    printf 'phase=%s\n' "$phase"
    printf 'utc=%s\n' "$(now_utc)"
    printf 'source_head=%s\n' "$head"
    printf 'source_dirty_list_sha256=%s\n' "${dirty:-unknown}"
    printf 'image=%s\n' "$image"
    printf 'src_mount=%s:/src:ro\n' "$SRC"
    printf 'caches=%s,%s\n' "$GOMOD_CACHE" "$GOBUILD_CACHE"
    printf 'network=%s container=%s owner_label=%s\n' "$NET" "$PG" "$OWNER_LABEL"
    if [ -n "$extra" ]; then
      printf '%s\n' "$extra"
    fi
  } > "$META"
  return 0
}

# redact a command line for the .cmd evidence file (password never lands).
redact() { printf '%s\n' "$1" | sed -e "s#$P24_PG_PASSWORD#***#g"; }

load_env() {
  [ -f "$SECRET_FILE" ] || die "secret env file missing — run '$0 up' first ($SECRET_FILE)"
  . "$SECRET_FILE"
  [ -n "${P24_PG_PASSWORD:-}" ] || die "secret env file carries no password"
}

# require_owned <name> <kind net|container>: refuse to operate on anything
# that does not carry this script's ownership label — env overrides can point
# NET/PG at other names, but cleanup can then never touch foreign resources.
require_owned() {
  name="$1"; kind="$2"
  if [ "$kind" = net ]; then
    label=$(docker network inspect "$name" --format '{{index .Labels "of-p24-owner"}}' 2>/dev/null || true)
  else
    label=$(docker inspect "$name" --format '{{index .Config.Labels "of-p24-owner"}}' 2>/dev/null || true)
  fi
  [ "$label" = "pg-isolated" ] || die "$kind $name is not owned by this script (missing label) — refusing to touch it"
}

# run_new <phase> <image> <cgoflag> <testflags> <expected_go_version>: one
# NEW-tests docker attempt with DIRECT redirect into $RAW and the real rc
# saved on the immediately following line. No pipe, no tee, no trailing grep.
# The actual `go version` is the FIRST line of the raw log and is ASSERTED
# against the expected toolchain (image tags can drift; the version is the
# contract). PG_LOG/PG_RC_FILE are printed for the tools driver BEFORE the
# return, on failure too.
run_new() {
  phase="$1"; image="$2"; cgo="$3"; testflags="$4"; want_go="$5"
  load_env
  require_owned "$NET" net
  require_owned "$PG" container
  running=$(docker inspect "$PG" --format '{{.State.Running}}' 2>/dev/null || true)
  [ "$running" = "true" ] || die "container $PG is not in State.Running=true"
  fresh_attempt "$phase"
  dsn="postgres://$PGUSER:$P24_PG_PASSWORD@$PG:5432/postgres"
  write_meta "$phase" "$image" "runner=go-version+vet&&new-tests $testflags expected_go=$want_go"
  record_source_meta before
  redact "docker run --rm --network $NET -v $SRC:/src:ro \
-v $GOMOD_CACHE:/go/pkg/mod -v $GOBUILD_CACHE:/root/.cache/go-build \
-w /src/services/connector-management-service \
-e CONNECTOR_DB_DSN=postgres://$PGUSER:***@$PG:5432/postgres \
-e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly -e GOPROXY=off -e CGO_ENABLED=$cgo \
$image sh -c \"go version && go vet -tags=integration ./internal/repo/ && go test -tags=integration $testflags ./internal/repo/\"" > "$CMD"
  rc=0
  docker run --rm --network "$NET" \
    -v "$SRC":/src:ro \
    -v "$GOMOD_CACHE":/go/pkg/mod \
    -v "$GOBUILD_CACHE":/root/.cache/go-build \
    -w /src/services/connector-management-service \
    -e CONNECTOR_DB_DSN="$dsn" \
    -e GOTOOLCHAIN=local \
    -e GOFLAGS=-mod=readonly \
    -e GOPROXY=off \
    -e CGO_ENABLED="$cgo" \
    "$image" \
    sh -c "go version && go vet -tags=integration ./internal/repo/ && go test -tags=integration $testflags ./internal/repo/" \
    > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  record_source_meta after
  gov=$(head -n 1 "$RAW" 2>/dev/null || true)
  printf 'go_version_actual=%s\n' "$gov" >> "$META"
  version_ok=true
  case "$gov" in
    "go version $want_go "*|"go version $want_go") ;;
    *) version_ok=false ;;
  esac
  printf 'version_ok=%s\n' "$version_ok" >> "$META"
  printf '[p24-pg] %s: docker rc=%s (raw: %s)\n' "$phase" "$rc" "$RAW"
  printf 'PG_LOG=%s\nPG_RC_FILE=%s\n' "$RAW" "$RCF"
  if [ "$version_ok" = false ]; then
    # GATE: the ORIGINAL docker rc stays truthfully in $RCF; the script
    # surfaces 2 so a wrong toolchain can never ride a passing rc.
    printf '[p24-pg] %s: toolchain GATE FAILED — want %s, got: %s (docker rc %s preserved)\n' \
      "$phase" "$want_go" "$gov" "$rc" >&2
    return 2
  fi
  return "$rc"
}

case "${1:-}" in

up)
  [ "$(docker network ls --format '{{.Name}}' | grep -cx "$NET" || true)" -eq 0 ] \
    || die "network $NET already exists — '$0 down' first (never reuse state)"
  docker inspect "$PG" >/dev/null 2>&1 \
    && die "container name $PG already exists — '$0 down' first (never reuse state)"
  docker network create --label "$OWNER_LABEL" "$NET" > "$EV/net-create.$(now_utc).raw" 2>&1
  docker network inspect "$NET" > /dev/null
  P24_PG_PASSWORD="$(openssl rand -hex 24)"
  fresh_attempt "up"
  rc=0
  docker run -d --name "$PG" --network "$NET" \
    --label "$OWNER_LABEL" \
    -e POSTGRES_USER="$PGUSER" \
    -e POSTGRES_PASSWORD="$P24_PG_PASSWORD" \
    -e POSTGRES_DB=postgres \
    postgres:16 > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  [ "$rc" -eq 0 ] || die "postgres:16 container failed to start (rc=$rc, see $RAW)"
  write_meta "up" "postgres:16"
  redact "docker run -d --name $PG --network $NET --label $OWNER_LABEL \
-e POSTGRES_USER=$PGUSER -e POSTGRES_PASSWORD=*** -e POSTGRES_DB=postgres postgres:16" > "$CMD"
  # readiness: bounded poll on the LOCAL socket (no password needed inside).
  i=0
  until docker exec "$PG" pg_isready -U "$PGUSER" -d postgres >/dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 60 ] || die "PG never became ready"
    sleep 1
  done
  ports=$(docker port "$PG" | wc -l | tr -d '[:space:]')
  pgver=$(docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc 'SHOW server_version' | tr -d '[:space:]')
  {
    printf '=== published host ports (ASSERTED to be 0) ===\n'
    printf 'published_ports=%s\n' "$ports"
    printf '=== server version (ASSERTED major 16) ===\n'
    printf 'server_version=%s\n' "$pgver"
    docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc 'SELECT version()'
    printf '=== network labels ===\n'
    docker network inspect "$NET" --format '{{json .Labels}}'
    printf '=== container labels ===\n'
    docker inspect "$PG" --format '{{json .Config.Labels}}'
  } >> "$RAW" 2>&1
  [ "$ports" = "0" ] || die "ASSERTION FAILED: $PG published $ports host port(s) — refusing to continue"
  case "$pgver" in
    16.*) ;;
    *) die "ASSERTION FAILED: server_version is '$pgver' (expected major 16)" ;;
  esac
  umask 077
  {
    printf "P24_PG_PASSWORD=%s\n" "$P24_PG_PASSWORD"
    printf "P24_DSN_MAINT=postgres://%s:%s@%s:5432/postgres\n" "$PGUSER" "$P24_PG_PASSWORD" "$PG"
  } > "$SECRET_FILE"
  umask 022
  printf '[p24-pg] up: %s ready on %s (asserted 0 host ports, server %s); secret 0600 at %s (never archived)\n' \
    "$PG" "$NET" "$pgver" "$SECRET_FILE"
  ;;

new-normal)
  run_new "new-normal" "$IMG_NORMAL" 0 "-count=1 -v -run '^TestPGSchedule'" "go1.26.3"
  ;;

new-race)
  run_new "new-race" "$IMG_RACE" 1 "-race -count=1 -v -run '^TestPGSchedule'" "go1.25.14"
  ;;

unified)
  load_env
  require_owned "$NET" net
  require_owned "$PG" container
  running=$(docker inspect "$PG" --format '{{.State.Running}}' 2>/dev/null || true)
  [ "$running" = "true" ] || die "container $PG is not in State.Running=true"
  # PostgreSQL folds unquoted identifiers to lowercase: keep the database
  # name ALL-lowercase (the UTC stamp carries uppercase T/Z) so the created
  # database and the DSN always agree without identifier quoting.
  db=$(printf 'ofp24_dataset_%s' "$(now_utc)" | tr '[:upper:]' '[:lower:]')
  createdb_base="$EV/unified-createdb.$(now_utc)"
  rc=0
  docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc "CREATE DATABASE $db" \
    > "${createdb_base}.raw" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "${createdb_base}.rc"
  docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc 'SHOW server_version' >> "${createdb_base}.raw" 2>&1
  [ "$rc" -eq 0 ] || die "fresh dataset database creation failed (rc=$rc, see ${createdb_base}.raw)"
  dataset_dsn="postgres://$PGUSER:$P24_PG_PASSWORD@$PG:5432/$db"
  maint_dsn="postgres://$PGUSER:$P24_PG_PASSWORD@$PG:5432/postgres"
  fresh_attempt "unified"
  write_meta "unified" "$IMG_NORMAL" "dataset_db=$db runner=go-version + frozen P1 run_tests.sh integration"
  record_source_meta before
  redact "docker run --rm --network $NET -v $SRC:/src:ro \
-v $GOMOD_CACHE:/go/pkg/mod -v $GOBUILD_CACHE:/root/.cache/go-build -w /src \
-e DATASET_DB_DSN=postgres://$PGUSER:***@$PG:5432/$db \
-e PIPELINE_DB_DSN=postgres://$PGUSER:***@$PG:5432/postgres \
-e CONNECTOR_DB_DSN=postgres://$PGUSER:***@$PG:5432/postgres \
-e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly -e GOPROXY=off $IMG_NORMAL \
sh -c \"go version && sh tools/smoke/business-platform-p1/run_tests.sh integration\"" > "$CMD"
  rc=0
  docker run --rm --network "$NET" \
    -v "$SRC":/src:ro \
    -v "$GOMOD_CACHE":/go/pkg/mod \
    -v "$GOBUILD_CACHE":/root/.cache/go-build \
    -w /src \
    -e DATASET_DB_DSN="$dataset_dsn" \
    -e PIPELINE_DB_DSN="$maint_dsn" \
    -e CONNECTOR_DB_DSN="$maint_dsn" \
    -e GOTOOLCHAIN=local \
    -e GOFLAGS=-mod=readonly \
    -e GOPROXY=off \
    "$IMG_NORMAL" \
    sh -c "go version && sh tools/smoke/business-platform-p1/run_tests.sh integration" \
    > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  record_source_meta after
  gov=$(head -n 1 "$RAW" 2>/dev/null || true)
  printf 'go_version_actual=%s\n' "$gov" >> "$META"
  version_ok=true
  case "$gov" in
    "go version go1.26.3 "*|"go version go1.26.3") ;;
    *) version_ok=false ;;
  esac
  printf 'version_ok=%s\n' "$version_ok" >> "$META"
  printf '[p24-pg] unified: runner rc=%s (raw: %s; dataset db: %s)\n' "$rc" "$RAW" "$db"
  printf 'PG_LOG=%s\nPG_RC_FILE=%s\n' "$RAW" "$RCF"
  if [ "$version_ok" = false ]; then
    # GATE: original docker rc preserved in $RCF; exit 2 blocks a drifting
    # toolchain from masquerading as a pass.
    printf '[p24-pg] unified: toolchain GATE FAILED — want go1.26.3, got: %s (docker rc %s preserved)\n' \
      "$gov" "$rc" >&2
    exit 2
  fi
  # Propagate the ORIGINAL docker rc to the formal driver — the report lines
  # above must never turn a failure into a 0 exit.
  exit "$rc"
  ;;

down)
  # Only EXACTLY our labeled, uniquely-named resources — a foreign object
  # under an overridden name is refused, never force-removed. q3v9 and every
  # old fixture are structurally out of reach (different names, no label).
  if docker inspect "$PG" >/dev/null 2>&1; then
    require_owned "$PG" container
    docker rm -f "$PG" > "$EV/down-container.$(now_utc).raw" 2>&1
  else
    printf 'container %s absent\n' "$PG" > "$EV/down-container.$(now_utc).raw"
  fi
  if docker network inspect "$NET" >/dev/null 2>&1; then
    require_owned "$NET" net
    docker network rm "$NET" > "$EV/down-net.$(now_utc).raw" 2>&1
  else
    printf 'network %s absent\n' "$NET" > "$EV/down-net.$(now_utc).raw"
  fi
  rm -f "$SECRET_FILE"
  printf '[p24-pg] down: removed %s and %s (labeled isolated artifacts only; secret file removed)\n' "$PG" "$NET"
  ;;

status)
  printf 'network %s: ' "$NET"
  docker network inspect "$NET" >/dev/null 2>&1 && printf 'present\n' || printf 'absent\n'
  printf 'container %s: ' "$PG"
  docker inspect "$PG" >/dev/null 2>&1 && printf 'present\n' || printf 'absent\n'
  if docker inspect "$PG" >/dev/null 2>&1; then
    printf 'published host ports: '
    docker port "$PG" | wc -l
  fi
  printf 'evidence dir: %s\n' "$EV"
  ls -l "$EV" | tail -n +2 || true
  ;;

*)
  sed -n '2,40p' "$0" | grep -E '^#( |$)' || true
  die "usage: $0 up|new-normal|new-race|unified|down|status"
  ;;
esac
