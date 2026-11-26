#!/usr/bin/env bash
# WP2.2 acceptance: the PRODUCTION BINARY must fail closed at startup without
# an independent CREDENTIAL_ENCRYPTION_KEY, and must start when one is
# provided (or in explicit dev fallback). Runs inside the golang:1.26.11 harness
# container against the isolated PG16 instance from run.sh.
#
# The result JSON uses REAL booleans (true/false) for pass/all_passed; the
# runner (run.sh → verify_results.py) type-checks them strictly. All secrets
# here are synthetic per-run values; the JSON records assertions only —
# never key or secret material.
set -euo pipefail

SERVICE_DIR="/work/services/connector-management-service"
OUT="${S1SEC_RESULTS_JSON:-/tmp/wp22-binary-gate.json}"
RUN_ID="${S1SEC_RUN_ID:-manual}"
DSN="${S1SEC_PG_DSN:?S1SEC_PG_DSN required}"
JWT="s1sec-synthetic-bin-jwt-secret"
# Syntactically valid but UNREACHABLE DSN (TCP port 1 on loopback refuses
# immediately) — used to prove the key gate fires before any DB dial.
UNREACHABLE_DSN="postgres://ofs1sec:s1sec-unreachable@127.0.0.1:1/ofs1sec?sslmode=disable"
GOOD_KEY="$(head -c 32 /dev/urandom | base64)"                     # fresh independent 32B key
LEGACY_KEY="$(printf 'openfoundry/credential-encryption/v1\x00%s' "$JWT" | openssl dgst -sha256 -binary | base64)"
BAD_LEN_KEY="$(head -c 16 /dev/urandom | base64)"

cd "$SERVICE_DIR"
mkdir -p /tmp/s1sec-bin
go build -o /tmp/s1sec-bin/connector-management-service ./cmd/connector-management-service
echo "[gate] binary built"

BASE_ENV=(DATABASE_URL="$DSN" JWT_SECRET="$JWT" HOST=127.0.0.1 METRICS_ADDR=127.0.0.1:0
  OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS=0 OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=0)

boolof() { [[ "$1" == 1 ]] && echo true || echo false; }

# expect_refusal <env...> : run binary, assert non-zero exit, sanitized error
expect_refusal() {
  local name="$1"; shift
  set +e
  env -u CREDENTIAL_ENCRYPTION_KEY "${BASE_ENV[@]}" "$@" \
    /tmp/s1sec-bin/connector-management-service >/tmp/s1sec-bin/out.log 2>&1
  local rc=$?
  set -e
  local out; out="$(cat /tmp/s1sec-bin/out.log)"
  local pass=1 reason=""
  [[ $rc -eq 0 ]] && { pass=0; reason="exited 0, expected refusal"; }
  grep -q "CREDENTIAL_ENCRYPTION_KEY" <<<"$out" || { pass=0; reason="${reason:+$reason; }error does not name CREDENTIAL_ENCRYPTION_KEY"; }
  # The error must never echo key material or the JWT secret itself.
  grep -qF "$JWT" <<<"$out" && { pass=0; reason="${reason:+$reason; }LEAK: JWT secret in output"; }
  [[ -n "${PROBE_KEY:-}" ]] && grep -qF "$PROBE_KEY" <<<"$out" && { pass=0; reason="${reason:+$reason; }LEAK: key material in output"; }
  echo "[case] $name rc=$rc pass=$pass ${reason:+reason=$reason}"
  CASES+=("$(printf '{ "name": "%s", "want": "refusal", "exit_code": %d, "pass": %s, "leak_free_checked": true%s }' \
    "$name" "$rc" "$(boolof $pass)" "${reason:+", \"reason\": \"$reason\""}")")
}

# expect_refusal_before_db <env...> : same as expect_refusal but with an
# UNREACHABLE database — proves the refusal comes from the credential-key
# gate BEFORE any DB dial: the run must finish fast on its own (timeout kill,
# DB-connect errors and hangs all fail the case).
expect_refusal_before_db() {
  local name="$1"; shift
  local started; started=$(date +%s)
  set +e
  timeout 10 env -u CREDENTIAL_ENCRYPTION_KEY "${BASE_ENV[@]/DATABASE_URL=*/DATABASE_URL=$UNREACHABLE_DSN}" "$@" \
    /tmp/s1sec-bin/connector-management-service >/tmp/s1sec-bin/out.log 2>&1
  local rc=$?
  set -e
  local elapsed=$(( $(date +%s) - started ))
  local out; out="$(cat /tmp/s1sec-bin/out.log)"
  local pass=1 reason=""
  [[ $rc -eq 124 ]] && { pass=0; reason="timed out (killed by timeout) — gate did not fail fast"; }
  [[ $rc -eq 0 ]] && { pass=0; reason="exited 0, expected refusal"; }
  grep -q "CREDENTIAL_ENCRYPTION_KEY" <<<"$out" || { pass=0; reason="${reason:+$reason; }error does not name CREDENTIAL_ENCRYPTION_KEY"; }
  grep -qE "dial tcp|connection refused|pgx|pgxpool" <<<"$out" && { pass=0; reason="${reason:+$reason; }reached DB connect before the key gate"; }
  grep -qF "$JWT" <<<"$out" && { pass=0; reason="${reason:+$reason; }LEAK: JWT secret in output"; }
  echo "[case] $name rc=$rc elapsed=${elapsed}s pass=$pass ${reason:+reason=$reason}"
  CASES+=("$(printf '{ "name": "%s", "want": "refusal_before_db", "exit_code": %d, "elapsed_s": %d, "pass": %s%s }' \
    "$name" "$rc" "$elapsed" "$(boolof $pass)" "${reason:+", \"reason\": \"$reason\""}")")
}

# expect_healthy <env...> : run binary, poll /healthz until 200, then TERM
expect_healthy() {
  local name="$1" port="$2"; shift 2
  env -u CREDENTIAL_ENCRYPTION_KEY "${BASE_ENV[@]}" PORT="$port" "$@" \
    /tmp/s1sec-bin/connector-management-service >/tmp/s1sec-bin/out.log 2>&1 &
  local pid=$!
  local healthy=0
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then healthy=1; break; fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  kill -TERM "$pid" 2>/dev/null || true
  local rc=0
  wait "$pid" || rc=$?
  local out; out="$(cat /tmp/s1sec-bin/out.log)"
  local pass=1 reason=""
  [[ $healthy -eq 1 ]] || { pass=0; reason="never reached healthy /healthz"; }
  grep -qF "$JWT" <<<"$out" && { pass=0; reason="${reason:+$reason; }LEAK: JWT secret in output"; }
  echo "[case] $name healthy=$healthy exit=$rc pass=$pass ${reason:+reason=$reason}"
  CASES+=("$(printf '{ "name": "%s", "want": "healthy_start", "healthy": %s, "exit_code": %d, "pass": %s%s }' \
    "$name" "$(boolof $healthy)" "$rc" "$(boolof $pass)" "${reason:+", \"reason\": \"$reason\""}")")
}

CASES=()

# Fail-closed cases: production and unknown/unset environments refuse to boot.
expect_refusal "production missing key" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production
PROBE_KEY="$LEGACY_KEY"
expect_refusal "production legacy JWT-derived key refused" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production CREDENTIAL_ENCRYPTION_KEY="$LEGACY_KEY"
PROBE_KEY="$BAD_LEN_KEY"
expect_refusal "production wrong-length key refused" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production CREDENTIAL_ENCRYPTION_KEY="$BAD_LEN_KEY"
PROBE_KEY=""
expect_refusal "production whitespace key" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production CREDENTIAL_ENCRYPTION_KEY="   "
expect_refusal "unset environment missing key" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=
expect_refusal "helm-default shared missing key" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=shared
expect_refusal "staging missing key" OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=staging
expect_refusal "SERVICE_VERSION=dev is not a dev signal" SERVICE_VERSION=dev OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production

# RUNTIME-ORDER case (review D): even with a syntactically valid but
# UNREACHABLE database, the missing-key refusal in production must arrive
# fast and name the key — never a DB error, hang, or timeout kill.
expect_refusal_before_db "production missing key refuses before DB connect (unreachable DSN)" \
  OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production

# Boot cases: independent key in production; explicit dev fallback.
expect_healthy "production with independent key boots" 28101 \
  OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=production CREDENTIAL_ENCRYPTION_KEY="$GOOD_KEY"
expect_healthy "development fallback boots" 28102 \
  OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=development

all=1
for c in "${CASES[@]}"; do grep -q '"pass": false' <<<"$c" && all=0; done
cat >"$OUT" <<EOF
{
  "run_id": "$RUN_ID",
  "cases": [
$(printf '%s,\n' "${CASES[@]}" | sed '$ s/,$//')
  ],
  "all_passed": $(boolof $all)
}
EOF
echo "[gate] results written to $OUT"
cat "$OUT"
[[ $all -eq 1 ]]
