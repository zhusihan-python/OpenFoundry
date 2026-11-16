#!/bin/sh
#
# P2.2 optional-replica driver — renders and manages the ADDITIVE P2.2
# services on top of the frozen P1 isolation stack:
#
#   connector2    (127.0.0.1:17089) cross-process cancel entry
#   dataset-proxy (127.0.0.1:17091) controlled Dataset response barrier
#   connector3    (127.0.0.1:17087) connector via dataset-proxy
#
# AUX-ONLY INVARIANT (review fix 1): `up` starts EXACTLY these three
# services with `--no-deps` — compose would otherwise include the base
# postgres/dataset dependencies in startup/creation/reconciliation,
# which is outside the aux scope. The service list is a hard-coded
# whitelist (not env-derived, never empty -> no degenerate full-stack
# up), and the base stack must already be healthy: `up` verifies that
# READ-ONLY first and refuses otherwise. `down` removes only the aux
# services (no -v, base untouched).
#
# `up` additionally refuses without P22_CONFIRM_CONNECTOR2=yes — a
# deliberate interlock until the root session green-lights formal P2.2
# execution. render / status / down are safe.
#
# Subcommands:
#   render       write $OF_P1_TMP/compose-p22-connector2.yaml (no stack change)
#   up           verify base health (read-only) + up -d --no-deps the three
#                aux services only (interlocked)
#   up-deadline  <ttl_s> rebuild connector2 ONLY with a short task TTL
#                (interlocked + base health + live-task guard; J scenario)
#   restore      rebuild connector2 with the standard aux env
#   down         stop+rm ONLY the aux services (never touches the base stack)
#   status       compose ps (aux) + health probes
#
set -eu
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TMP_DIR="${OF_P1_TMP:-/private/tmp/openfoundry-platform-p1-q3v9}"
PROJECT_NAME="openfoundry-platform-p1-q3v9"
BASE_COMPOSE="$TMP_DIR/compose.yaml"
OVERRIDE="$TMP_DIR/compose-p22-connector2.yaml"

# The aux whitelist (connector2 dataset-proxy connector3) appears ONLY as
# hard-coded literals in up/down/status below — deliberately not
# env-derived so no caller can smuggle a base service name in or empty
# the list into a degenerate full-stack up.

compose_base() {
  docker compose -f "$BASE_COMPOSE" --project-name "$PROJECT_NAME" "$@"
}

compose_with_override() {
  docker compose -f "$BASE_COMPOSE" -f "$OVERRIDE" \
    --project-name "$PROJECT_NAME" "$@"
}

require_setup() {
  if [ ! -f "$BASE_COMPOSE" ] || [ ! -f "$TMP_DIR/.env" ]; then
    echo "error: $TMP_DIR not set up; run ../business-platform-p1/run.sh setup first" >&2
    exit 1
  fi
}

require_interlock() {
  if [ "${P22_CONFIRM_CONNECTOR2:-}" != "yes" ]; then
    echo "refusing: export P22_CONFIRM_CONNECTOR2=yes first — the shared" >&2
    echo "isolation stack must only gain/reshape P2.2 replicas after the" >&2
    echo "root review green-lights P2.2 execution (see README.md)." >&2
    exit 1
  fi
}

require_no_live_tasks() {
  # Review fix A: the P21 legacy B/C rows are permanently 'running' BY
  # DESIGN and must never be cancelled or rewritten — so a whole-DB
  # pending/running count can never pass. Scope the guard to tasks the
  # aux2 CURRENT PROCESS could own: the executor does not scan old DB
  # tasks, so any controlled task connector2 can own has
  # started_at >= this boot's container StartedAt (conservative: may
  # over-count other instances started in the same window; can never
  # miss this instance's). Whitelisted columns only — NEVER raw
  # holder/token values. The psql rc is checked separately and stdout
  # must be STRICTLY numeric before the zero-compare (a failed psql
  # piped through tr must never default to 0 and pass).
  [ -f "$OVERRIDE" ] || cmd_render
  cid2=$(compose_with_override ps -q connector2 2>/dev/null | head -1)
  if [ -z "$cid2" ]; then
    echo "error: connector2 is not running — start the aux set first" >&2
    exit 1
  fi
  curl -fsS -m 3 http://127.0.0.1:17089/healthz >/dev/null 2>&1 || {
    echo "error: connector2 healthz not answering on 17089" >&2
    exit 1
  }
  started_at=$(docker inspect --format '{{.State.StartedAt}}' "$cid2" 2>/dev/null)
  case "$started_at" in
    *T*Z*) ;; # RFC3339 shape e.g. 2026-11-14T01:23:45.123456789Z
    *) echo "error: connector2 StartedAt unreadable (whitelist inspect)" >&2
       exit 1 ;;
  esac
  psql_rc=0
  live=$(compose_base exec -T postgres psql -U openfoundry \
    -d connectors -t -A -c \
    "SELECT count(*) FROM sync_runs WHERE control_mode = 'controlled' \
       AND status IN ('pending','running') \
       AND started_at >= '${started_at}'::timestamptz;" 2>/dev/null) \
    || psql_rc=$?
  if [ "$psql_rc" -ne 0 ]; then
    echo "error: live-task guard query failed (psql rc=$psql_rc) — refusing" >&2
    exit 1
  fi
  live=$(printf '%s' "$live" | tr -d '[:space:]')
  case "$live" in
    ''|*[!0-9]*)
      echo "error: live-task guard stdout not strictly numeric ('$live')" >&2
      exit 1 ;;
  esac
  if [ "$live" -ne 0 ]; then
    echo "error: $live controlled task(s) started since this connector2 boot" >&2
    echo "are still pending/running — let them finish or cancel before" >&2
    echo "reshaping connector2 (P21 legacy rows are intentionally excluded)" >&2
    exit 1
  fi
}

base_health_readonly() {
  # READ-ONLY base check: the aux services declare postgres/dataset as
  # dependencies; `up --no-deps` will NOT create or start them, so they
  # must already be up (postgres healthy, dataset running).
  if ! compose_base ps --format json postgres 2>/dev/null \
        | grep -q '"Health":"healthy"'; then
    echo "error: base postgres is not healthy — aux up refuses to touch the" >&2
    echo "base stack; start it via ../business-platform-p1/run.sh up first" >&2
    return 1
  fi
  if ! compose_base ps --format json dataset 2>/dev/null \
        | grep -q '"State":"running"'; then
    echo "error: base dataset is not running — aux up refuses to touch the" >&2
    echo "base stack; start it via ../business-platform-p1/run.sh up first" >&2
    return 1
  fi
}

up_aux_only() {
  # --no-deps (compose v5 up.go: noDeps -> IgnoreDependencies): compose
  # reconciles EXACTLY the named aux services and never the base ones.
  compose_with_override up -d --no-deps connector2 dataset-proxy connector3
}

cmd_render() {
  require_setup
  sed -e "s|__OF_ROOT__|$REPO_ROOT|g" \
    "$SCRIPT_DIR/compose-p22-connector2.yaml.template" > "$OVERRIDE"
  echo "rendered $OVERRIDE (repo root: $REPO_ROOT)"
}

case "${1:-}" in
  render) cmd_render ;;
  up)
    require_interlock
    cmd_render
    base_health_readonly
    up_aux_only
    echo "aux replicas up (--no-deps, base untouched):" \
         "connector2=:17089 dataset-proxy=:17091 connector3=:17087"
    ;;
  up-deadline)
    # Review fix 4 (J): rebuild connector2 ONLY with a short task TTL so
    # the deadline fires well before any source HTTP timeout. Whitelist
    # aux-only (--no-deps), interlocked, base-health-checked, and refused
    # while any run is live. No prod/test endpoint is invented — just the
    # real CONNECTOR_SYNC_ASYNC_* env on an aux replica.
    ttl="${2:?usage: p22_connector2.sh up-deadline <task_ttl_seconds>}"
    case "$ttl" in *[!0-9]*|'') echo "error: ttl must be a positive integer" >&2; exit 1;; esac
    [ "$ttl" -ge 1 ] || { echo "error: ttl must be >= 1" >&2; exit 1; }
    require_interlock
    require_setup
    cmd_render
    base_health_readonly
    require_no_live_tasks
    DEADLINE_OVERRIDE="$TMP_DIR/compose-p22-deadline.yaml"
    {
      echo "# P2.2 deadline aux (scenario J): connector2 ONLY, short task TTL."
      echo "# Non-sensitive values only (two integers). Keep as evidence."
      echo "services:"
      echo "  connector2:"
      echo "    environment:"
      echo "      CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS: \"$ttl\""
      echo "      CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS: \"300\""
    } > "$DEADLINE_OVERRIDE"
    docker compose -f "$BASE_COMPOSE" -f "$OVERRIDE" -f "$DEADLINE_OVERRIDE" \
      --project-name "$PROJECT_NAME" up -d --no-deps connector2
    echo "connector2 reshaped with TASK_TTL=${ttl}s (auth 300s) — effective env:"
    docker compose -f "$BASE_COMPOSE" -f "$OVERRIDE" -f "$DEADLINE_OVERRIDE" \
      --project-name "$PROJECT_NAME" config 2>/dev/null \
      | grep -E 'CONNECTOR_SYNC_ASYNC_(TASK|AUTH)_TTL_SECS' || true
    ;;
  restore)
    # restore connector2 to the standard aux env (deadline override file
    # is kept on disk as raw evidence)
    require_interlock
    require_setup
    [ -f "$OVERRIDE" ] || cmd_render
    compose_with_override up -d --no-deps connector2
    echo "connector2 restored to default aux env"
    ;;
  down)
    require_setup
    [ -f "$OVERRIDE" ] || cmd_render
    # rm -sf ONLY the aux services; no -v, no base service is addressed.
    compose_with_override rm -sf connector2 dataset-proxy connector3
    echo "aux replicas removed (base stack untouched)"
    ;;
  status)
    require_setup
    [ -f "$OVERRIDE" ] || cmd_render
    compose_with_override ps connector2 dataset-proxy connector3 || true
    for url in http://127.0.0.1:17089/healthz http://127.0.0.1:17087/healthz \
               http://127.0.0.1:17091/p22proxy/healthz; do
      curl -fsS -m 3 "$url" >/dev/null 2>&1 \
        && echo "healthy: $url" || echo "not answering: $url"
    done
    ;;
  *)
    sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
