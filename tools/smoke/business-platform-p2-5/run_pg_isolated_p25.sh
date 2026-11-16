#!/bin/sh
#
# OpenFoundry P2.5 — ISOLATED PostgreSQL 16 test driver (PG-track exclusive).
#
# Contract: handoffs/2026-11-14-p25/implementation-release.md (§File
# ownership, §Execution authority NOW) + review-pg-plan-round1.md.
# Pattern follows the P24 isolated-PG driver
# (tools/smoke/business-platform-p2-4/run_pg_isolated_p24.sh): private
# docker network, postgres:16 with NO host ports, repo mounted READ-ONLY,
# existing shared Go cache volumes, direct-redirect raw logs with the REAL
# rc on the next line. It NEVER touches q3v9, of-p24-pg, P24 tools, or any
# predecessor archive (distinct names + ownership label make foreign
# resources structurally unreachable).
#
# CURRENT PHASE (implementation-release): COMPILE-ONLY is authorized.
# Starting real PG / running DB tests waits for root's review of the P25
# candidate — the up/new-*/unified modes below are PREPARED, not released.
#
# Modes:
#   compile-normal  AUTHORIZED NOW: golang:1.26.3-alpine3.22, CGO_ENABLED=0,
#                   `go vet -tags=integration ./internal/repo/` plus
#                   `go test -tags=integration -c -o /dev/null` (compile,
#                   never run). No PG, no network needed.
#   compile-race    AUTHORIZED NOW: golang:1.25 (Go 1.25.14), CGO_ENABLED=1,
#                   `go test -race -tags=integration -c -o /dev/null`.
#   up              [NOT YET RELEASED] create of-p25-pg-net + of-p25-pg
#                   (postgres:16, label of-p25-owner=pg-isolated), assert
#                   zero host ports + major 16, write the 0600 secret env
#                   into PRIVATE TMP (never evidence, never printed).
#   new-normal      [NOT YET RELEASED] NEW tests only, go1.26.3, CGO off,
#                   vet + `go test -count=1 -v -run '^TestPGCheckpoint'`.
#   new-race        [NOT YET RELEASED] NEW tests only, go1.25.14, CGO on,
#                   `-race -count=1 -v` (separate log, never -count=2).
#   unified         [NOT YET RELEASED] ONE fresh frozen P1 runner
#                   `tools/smoke/business-platform-p1/run_tests.sh
#                   integration` inside the same isolated PG — the
#                   unified old72+newN evidence for pg_exact_check_p25.py.
#   down            remove of-p25-pg and of-p25-pg-net (exact names only).
#   status          print presence + evidence listing. No secrets printed.
#
# Evidence discipline (identical to P24): <phase>_<UTC>.raw (direct
# `> file 2>&1`, no pipes/tee/grep), .rc (REAL docker exit code), .cmd
# (password redacted), .meta (UTC, HEAD, dirty-tree hash, source content
# hash before/after + drift flag, image, version gate result). Unique
# actual-clock names; failures preserved, never overwritten.

set -eu

SRC="${P25_SRC:-/Volumes/ExternalSD/projects/OpenFoundry}"
EV="${P25_EVIDENCE:-/private/tmp/openfoundry-p25-repair-evidence/pg}"
NET="${P25_NET:-of-p25-pg-net}"
PG="${P25_PG:-of-p25-pg}"
PGUSER="${P25_PGUSER:-ofp25}"
GOMOD_CACHE="${P25_GOMOD_CACHE:-openfoundry-gomod}"     # existing shared cache
GOBUILD_CACHE="${P25_GOBUILD_CACHE:-openfoundry-gobuild}"
IMG_NORMAL="${P25_IMG_NORMAL:-golang:1.26.3-alpine3.22}"
IMG_RACE="${P25_IMG_RACE:-golang:1.25}"                 # Go 1.25.14
OWNER_LABEL="of-p25-owner=pg-isolated"
SECRET_FILE="${P25_SECRET_FILE:-/private/tmp/openfoundry-p25-pg.env}"

mkdir -p "$EV"

die() { printf '[p25-pg] ERROR: %s\n' "$*" >&2; exit 2; }

now_utc() { date -u +%Y%m%dT%H%M%SZ; }

# Deterministic content hash of everything the Go runs compile from (same
# set as the P24 driver: three business services + in-repo cron lib +
# module graph). Evidence/secrets never in the hashed set.
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

# record_source_meta records the attempt's source content hash (before and
# after). It NEVER alters the original Go/Docker rc; drift is captured in
# the SOURCE_DRIFT global + a meta line, and the HARD wrapper gates below
# consume it (review-pg-code-round2 #9: a drift flag alone must not let
# run_new/unified continue as success).
record_source_meta() {
  phase_kind="$1" # before|after
  hash="$(source_tree_hash)"
  printf 'source_content_sha256_%s=%s\n' "$phase_kind" "$hash" >> "$META"
  if [ "$phase_kind" = before ]; then
    SOURCE_HASH_BEFORE="$hash"
    SOURCE_DRIFT=0
  else
    SOURCE_HASH_AFTER="$hash"
    SOURCE_DRIFT=0
    if [ "$hash" != "$SOURCE_HASH_BEFORE" ]; then
      SOURCE_DRIFT=1
      printf 'SOURCE_DRIFT_DURING_RUN=1\n' >> "$META"
    fi
  fi
  return 0
}

# p25_record_gates <original_rc>: writes the INDEPENDENT gate rc files
# (source-gate = content hash before==after; version-gate = toolchain
# match) plus original/gate/wrapper rc meta lines, and echoes the WRAPPER
# rc (2 on any gate violation, otherwise the original rc). The .rc file
# always keeps the ORIGINAL docker exit code — never tampered.
p25_record_gates() {
  orig_rc="$1"
  src_gate=0
  [ "$SOURCE_DRIFT" = "0" ] || src_gate=1
  ver_gate=0
  [ "$version_ok" = "true" ] || ver_gate=1
  printf '%s\n' "$src_gate" > "${RAW%.raw}.source-gate.rc"
  printf '%s\n' "$ver_gate" > "${RAW%.raw}.version-gate.rc"
  wrapper_rc="$orig_rc"
  if [ "$src_gate" -ne 0 ] || [ "$ver_gate" -ne 0 ]; then
    wrapper_rc=2
  fi
  {
    printf 'original_docker_rc=%s\n' "$orig_rc"
    printf 'source_gate_rc=%s\n' "$src_gate"
    printf 'version_gate_rc=%s\n' "$ver_gate"
    printf 'wrapper_rc=%s\n' "$wrapper_rc"
  } >> "$META"
  WRAPPER_RC="$wrapper_rc"
}

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

redact() { printf '%s\n' "$1" | sed -e "s#$P25_PG_PASSWORD#***#g"; }

load_env() {
  [ -f "$SECRET_FILE" ] || die "secret env file missing — run '$0 up' first ($SECRET_FILE)"
  . "$SECRET_FILE"
  [ -n "${P25_PG_PASSWORD:-}" ] || die "secret env file carries no password"
}

require_owned() {
  name="$1"; kind="$2"
  if [ "$kind" = net ]; then
    label=$(docker network inspect "$name" --format '{{index .Labels "of-p25-owner"}}' 2>/dev/null || true)
  else
    label=$(docker inspect "$name" --format '{{index .Config.Labels "of-p25-owner"}}' 2>/dev/null || true)
  fi
  [ "$label" = "pg-isolated" ] || die "$kind $name is not owned by this script (missing label) — refusing to touch it"
}

# run_compile <phase> <image> <cgo> <want_go>: AUTHORIZED compile-only
# attempt — vet + test -c (never executes a test), no PG/network needed.
# The actual `go version` is the first raw line and is GATED against the
# expected toolchain (tags can drift; the version is the contract).
run_compile() {
  phase="$1"; image="$2"; cgo="$3"; want_go="$4"
  extraflags=""
  [ "$phase" = compile-race ] && extraflags=" -race"
  fresh_attempt "$phase"
  write_meta "$phase" "$image" "runner=go-version+vet+test-c$extraflags expected_go=$want_go requires_pg=no"
  record_source_meta before
  printf 'docker run --rm -v %s:/src:ro -v %s:/go/pkg/mod -v %s:/root/.cache/go-build \
-w /src/services/connector-management-service -e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly \
-e GOPROXY=off -e CGO_ENABLED=%s %s sh -c "go version && go vet -tags=integration \
./internal/repo/ ./internal/handlers/ && go test%s -tags=integration -c -o /dev/null \
./internal/repo/ && go test%s -tags=integration -c -o /dev/null ./internal/handlers/"\n' \
    "$SRC" "$GOMOD_CACHE" "$GOBUILD_CACHE" "$cgo" "$image" "$extraflags" "$extraflags" > "$CMD"
  rc=0
  docker run --rm \
    -v "$SRC":/src:ro \
    -v "$GOMOD_CACHE":/go/pkg/mod \
    -v "$GOBUILD_CACHE":/root/.cache/go-build \
    -w /src/services/connector-management-service \
    -e GOTOOLCHAIN=local \
    -e GOFLAGS=-mod=readonly \
    -e GOPROXY=off \
    -e CGO_ENABLED="$cgo" \
    "$image" \
    sh -c "go version && go vet -tags=integration ./internal/repo/ ./internal/handlers/ && go test${extraflags} -tags=integration -c -o /dev/null ./internal/repo/ && go test${extraflags} -tags=integration -c -o /dev/null ./internal/handlers/" \
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
  printf '[p25-pg] %s: docker rc=%s (raw: %s)\n' "$phase" "$rc" "$RAW"
  if [ "$version_ok" = false ]; then
    printf '[p25-pg] %s: toolchain GATE FAILED — want %s, got: %s (docker rc %s preserved)\n' \
      "$phase" "$want_go" "$gov" "$rc" >&2
    return 2
  fi
  return "$rc"
}

# run_new <phase> <image> <cgoflag> <testflags> <want_go>: PREPARED, NOT
# RELEASED — new-tests docker attempt against the isolated PG fixture.
run_new() {
  phase="$1"; image="$2"; cgo="$3"; testflags="$4"; want_go="$5"
  load_env
  require_owned "$NET" net
  require_owned "$PG" container
  running=$(docker inspect "$PG" --format '{{.State.Running}}' 2>/dev/null || true)
  [ "$running" = "true" ] || die "container $PG is not in State.Running=true"
  fresh_attempt "$phase"
  dsn="postgres://$PGUSER:$P25_PG_PASSWORD@$PG:5432/postgres"
  write_meta "$phase" "$image" "runner=go-version+vet&&new-tests $testflags expected_go=$want_go"
  record_source_meta before
  redact "docker run --rm --network $NET -v $SRC:/src:ro \
-v $GOMOD_CACHE:/go/pkg/mod -v $GOBUILD_CACHE:/root/.cache/go-build \
-w /src/services/connector-management-service \
-e CONNECTOR_DB_DSN=postgres://$PGUSER:***@$PG:5432/postgres \
-e GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly -e GOPROXY=off -e CGO_ENABLED=$cgo \
$image sh -c \"go version && go vet -tags=integration ./internal/repo/ ./internal/handlers/ && go test -tags=integration $testflags ./internal/repo/ ./internal/handlers/\"" > "$CMD"
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
    sh -c "go version && go vet -tags=integration ./internal/repo/ ./internal/handlers/ && go test -tags=integration $testflags ./internal/repo/ ./internal/handlers/" \
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
  printf '[p25-pg] %s: docker rc=%s (raw: %s)\n' "$phase" "$rc" "$RAW"
  printf 'PG_LOG=%s\nPG_RC_FILE=%s\n' "$RAW" "$RCF"
  # review-pg-code-round2 #9: HARD gates — original docker rc stays in $RCF;
  # source-gate/version-gate get their own rc files; the wrapper rc is the
  # consumable verdict (non-zero on any violation).
  p25_record_gates "$rc"
  wrapper="$WRAPPER_RC"
  if [ "$wrapper" -ne "$rc" ]; then
    printf '[p25-pg] %s: GATE violation — wrapper rc=%s (source_gate/version_gate rc files beside raw; original docker rc %s preserved in %s)\n' \
      "$phase" "$wrapper" "$rc" "$RCF" >&2
  fi
  return "$wrapper"
}

case "${1:-}" in

compile-normal)
  run_compile "compile-normal" "$IMG_NORMAL" 0 "go1.26.3"
  ;;

compile-race)
  run_compile "compile-race" "$IMG_RACE" 1 "go1.25.14"
  ;;

# ---------------------------------------------------------------------------
# RELEASED PHASE: `up` only (root release 2026-14, preparation stage).
# Evidence-first discipline (root up-release conditions):
#   - every component writes its REDACTED .cmd and .meta BEFORE the command
#     runs, then the REAL component rc — a docker rc0 can never mask a
#     ready/version/ports gate failure (each gate saves its own rc file);
#   - exact names of-p25-pg / of-p25-pg-net, owner label, ZERO host ports;
#   - the cached official postgres:16 image is used as-is; the ACTUAL image
#     ID and full server version are recorded (no dependency upgrades);
#   - if a same-name network/container/secret ALREADY EXISTS it is PRESERVED
#     untouched: only a read-only status report is written and up exits 3
#     (never "down first", never delete/recreate);
#   - the secret env (0600, /private/tmp/openfoundry-p25-pg.env) lives
#     OUTSIDE the evidence dir; its VALUES / Config.Env / DSN are never
#     printed or archived — only its path, mode and presence;
#   - of-p24-pg, q3 and every old volume are structurally out of reach
#     (distinct names + label).
# ---------------------------------------------------------------------------
up)
  up_rc=0

  # --- component 0: pre-flight — same-name resources are PRESERVED -------
  fresh_attempt "up-preflight"
  write_meta "up-preflight" "-" "purpose=existence-check non-destructive"
  printf 'docker network inspect %s (existence only) ; docker inspect %s (existence only) ; [ -e %s ] (existence only)\n' \
    "$NET" "$PG" "$SECRET_FILE" > "$CMD"
  {
    printf '=== preflight %s ===\n' "$(now_utc)"
    printf 'network_exists=%s\n' "$(docker network ls --format '{{.Name}}' | grep -cx "$NET" || true)"
    printf 'container_exists=%s\n' "$(docker inspect "$PG" >/dev/null 2>&1 && printf yes || printf no)"
    printf 'secret_exists=%s\n' "$([ -e "$SECRET_FILE" ] && printf yes || printf no)"
  } > "$RAW" 2>&1
  printf '0\n' > "$RCF"
  if docker network ls --format '{{.Name}}' | grep -qx "$NET"; then
    {
      printf '=== PRESERVED pre-existing network (read-only report) ===\n'
      docker network inspect "$NET" --format 'name={{.Name}} labels={{json .Labels}}'
    } >> "$RAW" 2>&1
    printf '[p25-pg] up: network %s ALREADY EXISTS — preserved untouched (status recorded); no recreation\n' "$NET"
    up_rc=3
  fi
  if docker inspect "$PG" >/dev/null 2>&1; then
    {
      printf '=== PRESERVED pre-existing container (read-only report; Config.Env NEVER read) ===\n'
      docker inspect "$PG" --format 'name={{.Name}} running={{.State.Running}} labels={{json .Config.Labels}} image={{.Config.Image}}'
      printf 'published_ports=%s\n' "$(docker port "$PG" | wc -l | tr -d '[:space:]')"
    } >> "$RAW" 2>&1
    printf '[p25-pg] up: container %s ALREADY EXISTS — preserved untouched (status recorded); no recreation\n' "$PG"
    up_rc=3
  fi
  if [ -e "$SECRET_FILE" ]; then
    {
      printf '=== PRESERVED pre-existing secret file (mode only, values NEVER read) ===\n'
      stat -f 'path=%N mode=%Lp' "$SECRET_FILE" 2>/dev/null || stat -c 'path=%n mode=%a' "$SECRET_FILE"
    } >> "$RAW" 2>&1
    printf '[p25-pg] up: secret %s ALREADY EXISTS — preserved untouched (mode recorded only)\n' "$SECRET_FILE"
    up_rc=3
  fi
  [ "$up_rc" -eq 0 ] || exit "$up_rc"

  # --- component 1: image record (cached official postgres:16, as-is) ----
  fresh_attempt "up-image"
  write_meta "up-image" "postgres:16" "purpose=record actual image id (no pull/upgrade attempted)"
  printf 'docker image inspect postgres:16 --format {{.Id}} {{join .RepoTags ","}}\n' > "$CMD"
  rc=0
  docker image inspect postgres:16 --format '{{.Id}} {{join .RepoTags ","}}' > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  [ "$rc" -eq 0 ] || die "postgres:16 image not inspectable locally (rc=$rc) — refusing any implicit upgrade"
  PG_IMAGE_ID=$(docker image inspect postgres:16 --format '{{.Id}}')
  printf 'image_id=%s\n' "$PG_IMAGE_ID" >> "$META"

  # --- component 2: network create (label-owned, exact name) ------------
  fresh_attempt "up-net-create"
  write_meta "up-net-create" "-" "network=$NET label=$OWNER_LABEL"
  printf 'docker network create --label %s %s\n' "$OWNER_LABEL" "$NET" > "$CMD"
  rc=0
  docker network create --label "$OWNER_LABEL" "$NET" > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  [ "$rc" -eq 0 ] || die "network create failed (rc=$rc, see $RAW)"
  docker network inspect "$NET" --format 'name={{.Name}} labels={{json .Labels}}' >> "$RAW" 2>&1

  # --- component 3: container run (secret only in memory + 0600 later) --
  P25_PG_PASSWORD="$(openssl rand -hex 24)"
  fresh_attempt "up-container"
  write_meta "up-container" "postgres:16" "container=$PG network=$NET label=$OWNER_LABEL image_id=$PG_IMAGE_ID"
  redact "docker run -d --name $PG --network $NET --label $OWNER_LABEL \
-e POSTGRES_USER=$PGUSER -e POSTGRES_PASSWORD=*** -e POSTGRES_DB=postgres postgres:16" > "$CMD"
  rc=0
  docker run -d --name "$PG" --network "$NET" \
    --label "$OWNER_LABEL" \
    -e POSTGRES_USER="$PGUSER" \
    -e POSTGRES_PASSWORD="$P25_PG_PASSWORD" \
    -e POSTGRES_DB=postgres \
    postgres:16 > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  [ "$rc" -eq 0 ] || die "postgres:16 container failed to start (rc=$rc, see $RAW)"
  printf 'container_id=%s\n' "$(docker inspect "$PG" --format '{{.Id}}')" >> "$META"
  printf 'actual_image_id=%s\n' "$(docker inspect "$PG" --format '{{.Image}}')" >> "$META"

  # --- component 4: readiness poll (its OWN rc file; never masked) ------
  fresh_attempt "up-ready"
  write_meta "up-ready" "-" "probe=docker exec $PG pg_isready bounded=60x1s"
  printf 'docker exec %s pg_isready -U %s -d postgres (bounded 60x1s poll)\n' "$PG" "$PGUSER" > "$CMD"
  rc=1
  i=0
  until docker exec "$PG" pg_isready -U "$PGUSER" -d postgres >> "$RAW" 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then rc=1; break; fi
    sleep 1
  done
  [ "$i" -lt 60 ] && rc=0
  printf '%s\n' "$rc" > "$RCF"
  printf 'ready_poll_attempts=%s\n' "$i" >> "$META"
  [ "$rc" -eq 0 ] || die "PG never became ready (ready rc=$rc, see $RAW)"

  # --- component 5: gates — ports==0, major 16, labels (each own rc) ----
  fresh_attempt "up-gates"
  write_meta "up-gates" "-" "gates=ports==0,server_major=16,owner_labels"
  printf 'gates: docker port %s | wc -l == 0 ; docker exec %s psql SHOW server_version /^16/ ; label inspections\n' "$PG" "$PG" > "$CMD"
  gate_fail=0
  ports=$(docker port "$PG" | wc -l | tr -d '[:space:]')
  pgver=$(docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc 'SHOW server_version' | tr -d '[:space:]')
  pgver_full=$(docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc 'SELECT version()' | head -n 1)
  net_label=$(docker network inspect "$NET" --format '{{index .Labels "of-p25-owner"}}')
  ctr_label=$(docker inspect "$PG" --format '{{index .Config.Labels "of-p25-owner"}}')
  {
    printf '=== gate inputs %s ===\n' "$(now_utc)"
    printf 'published_ports=%s\n' "$ports"
    printf 'server_version=%s\n' "$pgver"
    printf 'server_version_full=%s\n' "$pgver_full"
    printf 'image_id=%s\n' "$PG_IMAGE_ID"
    printf 'net_owner_label=%s\n' "$net_label"
    printf 'container_owner_label=%s\n' "$ctr_label"
    printf '=== gate results (each gate has its own rc line) ===\n'
  } > "$RAW" 2>&1
  if [ "$ports" = "0" ]; then printf 'gate_ports_rc=0\n' >> "$RAW"; else printf 'gate_ports_rc=1\n' >> "$RAW"; gate_fail=1; fi
  case "$pgver" in
    16.*) printf 'gate_version_rc=0\n' >> "$RAW" ;;
    *) printf 'gate_version_rc=1\n' >> "$RAW"; gate_fail=1 ;;
  esac
  if [ "$net_label" = "pg-isolated" ] && [ "$ctr_label" = "pg-isolated" ]; then
    printf 'gate_labels_rc=0\n' >> "$RAW"
  else
    printf 'gate_labels_rc=1\n' >> "$RAW"; gate_fail=1
  fi
  printf '%s\n' "$gate_fail" > "$RCF"
  [ "$gate_fail" -eq 0 ] || die "up GATES FAILED (ports/version/labels — per-gate rc lines in $RAW; nothing recreated)"

  # --- component 6: secret write (0600, outside evidence, values silent) -
  fresh_attempt "up-env-write"
  write_meta "up-env-write" "-" "path=$SECRET_FILE mode=0600 values_never_printed"
  printf 'umask 077 ; { printf "P25_PG_PASSWORD=***\\nP25_DSN_MAINT=postgres://%s:***@%s:5432/postgres\\n" } > %s\n' \
    "$PGUSER" "$PG" "$SECRET_FILE" > "$CMD"
  rc=0
  umask 077
  {
    printf "P25_PG_PASSWORD=%s\n" "$P25_PG_PASSWORD"
    printf "P25_DSN_MAINT=postgres://%s:%s@%s:5432/postgres\n" "$PGUSER" "$P25_PG_PASSWORD" "$PG"
  } > "$SECRET_FILE" || rc=$?
  umask 022
  printf '%s\n' "$rc" > "$RCF"
  printf 'secret_mode=%s\n' "$(stat -f '%Lp' "$SECRET_FILE" 2>/dev/null || stat -c '%a' "$SECRET_FILE")" >> "$META"
  [ "$rc" -eq 0 ] || die "secret env write failed (rc=$rc)"

  printf '[p25-pg] up: %s ready on %s — ports=0 server=%s image=%s labels=of-p25-owner=pg-isolated; secret 0600 at %s (values never printed/archived)\n' \
    "$PG" "$NET" "$pgver" "$PG_IMAGE_ID" "$SECRET_FILE"
  ;;

new-normal)
  run_new "new-normal" "$IMG_NORMAL" 0 "-count=1 -v -run '^TestPGCheckpoint'" "go1.26.3"
  ;;

new-race)
  run_new "new-race" "$IMG_RACE" 1 "-race -count=1 -v -run '^TestPGCheckpoint'" "go1.25.14"
  ;;

unified)
  load_env
  require_owned "$NET" net
  require_owned "$PG" container
  running=$(docker inspect "$PG" --format '{{.State.Running}}' 2>/dev/null || true)
  [ "$running" = "true" ] || die "container $PG is not in State.Running=true"
  # ALL-lowercase db name (PG folds unquoted identifiers).
  db=$(printf 'ofp25_dataset_%s' "$(now_utc)" | tr '[:upper:]' '[:lower:]')
  # review-pg-code-round2 #8: the CREATE DATABASE component records its
  # redacted .cmd/.meta BEFORE execution and keeps its OWN real rc; the
  # SHOW-version probe is a SEPARATE attempt with its own rc + gate so a
  # failed version read can never ride a successful createdb rc0.
  fresh_attempt "unified-createdb"
  write_meta "unified-createdb" "-" "database=$db"
  printf 'docker exec %s psql -U %s -d postgres -tAc "CREATE DATABASE %s"\n' \
    "$PG" "$PGUSER" "$db" > "$CMD"
  rc=0
  docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc "CREATE DATABASE $db" \
    > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  [ "$rc" -eq 0 ] || die "fresh dataset database creation failed (rc=$rc, see $RAW)"

  fresh_attempt "unified-db-version"
  write_meta "unified-db-version" "-" "gate=server_major_16"
  printf 'docker exec %s psql -U %s -d postgres -tAc "SHOW server_version"\n' \
    "$PG" "$PGUSER" > "$CMD"
  # The docker exec writes DIRECTLY into its own raw file; its REAL rc goes
  # straight into .rc (narrow recheck #8: a $(... | tr) pipeline would
  # capture tr's rc and lose the true SHOW rc).
  rc=0
  docker exec "$PG" psql -U "$PGUSER" -d postgres -tAc 'SHOW server_version' \
    > "$RAW" 2>&1 || rc=$?
  printf '%s\n' "$rc" > "$RCF"
  printf 'original_docker_rc=%s\n' "$rc" >> "$META"
  # Parse in a SEPARATE step, FROM the saved raw; a FAILED SHOW is refused
  # even if its partial output happens to look like a 16.x string.
  ver=$(tr -d '[:space:]' < "$RAW" | head -n 1)
  ver_gate=0
  if [ "$rc" -ne 0 ]; then
    ver_gate=1
  else
    case "$ver" in
      16.*) ;;
      *) ver_gate=1 ;;
    esac
  fi
  printf 'server_version=%s\nversion_gate_rc=%s\n' "$ver" "$ver_gate" >> "$META"
  printf '%s\n' "$ver_gate" > "${RAW%.raw}.version-gate.rc"
  [ "$ver_gate" -eq 0 ] || die "unified db-version GATE FAILED (show rc=$rc, server '$ver', see $RAW)"
  dataset_dsn="postgres://$PGUSER:$P25_PG_PASSWORD@$PG:5432/$db"
  maint_dsn="postgres://$PGUSER:$P25_PG_PASSWORD@$PG:5432/postgres"
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
  printf '[p25-pg] unified: runner rc=%s (raw: %s; dataset db: %s)\n' "$rc" "$RAW" "$db"
  printf 'PG_LOG=%s\nPG_RC_FILE=%s\n' "$RAW" "$RCF"
  # review-pg-code-round2 #9: same HARD gates as run_new — the original
  # runner rc stays in $RCF; gate rc files + wrapper rc are independent.
  p25_record_gates "$rc"
  wrapper="$WRAPPER_RC"
  if [ "$wrapper" -ne "$rc" ]; then
    printf '[p25-pg] unified: GATE violation — wrapper rc=%s (source_gate/version_gate rc files beside raw; original runner rc %s preserved in %s)\n' \
      "$wrapper" "$rc" "$RCF" >&2
  fi
  exit "$wrapper"
  ;;

down)
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
  printf '[p25-pg] down: removed %s and %s (labeled isolated artifacts only; secret file removed)\n' "$PG" "$NET"
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
  sed -n '2,42p' "$0" | grep -E '^#( |$)' || true
  die "usage: $0 compile-normal|compile-race|up|new-normal|new-race|unified|down|status"
  ;;
esac
