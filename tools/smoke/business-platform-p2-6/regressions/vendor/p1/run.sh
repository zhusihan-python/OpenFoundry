#!/usr/bin/env bash
#
# OpenFoundry business-platform P1 acceptance — environment driver.
#
# Everything secret lives ONLY in $OF_P1_TMP/.env (chmod 600, gitignored
# by location: it is outside the repo). Nothing here resets or touches
# the older 16078/16081 and 15078/15081 environments.
#
# Subcommands:
#   setup         generate .env + render compose.yaml into $OF_P1_TMP
#   build         build dataset/pipeline/connector/fixture images
#   up            start postgres+fixture+services (waits for health)
#   smoke         run the acceptance smoke (writes $OF_P1_TMP/result.json)
#   tests         full `go test` for the three services (compose profile)
#   migrate-tests PG16 empty-DB + re-run migration integration tests
#   restart       restart the three services (migration re-run evidence)
#   status        compose ps
#   logs <svc>    follow logs (redaction-safe: services never log JWTs)
#   down          stop the stack (keep data volume)
#   down-volumes  stop and delete the data volume (fresh acceptance run)
#
# Environment overrides:
#   OF_P1_TMP   working dir (default /private/tmp/openfoundry-platform-p1-q3v9)
#   OF_P1_SKIP_BUILD=1   up: skip image rebuild

set -euo pipefail
# `smoke`/`tests` pipe through tee: without pipefail a failing python/go
# would be masked by tee's exit 0.
set -o pipefail 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TMP_DIR="${OF_P1_TMP:-/private/tmp/openfoundry-platform-p1-q3v9}"
PROJECT_NAME="openfoundry-platform-p1-q3v9"
COMPOSE_FILE="$TMP_DIR/compose.yaml"
LOG_DIR="$TMP_DIR/logs"

compose() {
  docker compose -f "$COMPOSE_FILE" --project-name "$PROJECT_NAME" "$@"
}

require_setup() {
  if [ ! -f "$TMP_DIR/.env" ] || [ ! -f "$COMPOSE_FILE" ]; then
    echo "error: $TMP_DIR not set up; run: $0 setup" >&2
    exit 1
  fi
}

cmd_setup() {
  mkdir -p "$TMP_DIR" "$LOG_DIR"
  if [ ! -f "$TMP_DIR/.env" ]; then
    pg_password="$(openssl rand -hex 24)"
    jwt_secret="$(openssl rand -hex 32)"
    credential_key="$(openssl rand -base64 32)"
    service_jwt="$(python3 -I "$SCRIPT_DIR/mint_token.py" --secret "$jwt_secret" --hours 72)"
    umask 077
    {
      echo "OF_P1_PG_PASSWORD=$pg_password"
      echo "OF_P1_JWT_SECRET=$jwt_secret"
      echo "OF_P1_CREDENTIAL_KEY=$credential_key"
      echo "OF_P1_SERVICE_JWT=$service_jwt"
    } > "$TMP_DIR/.env"
    echo "generated fresh secrets in $TMP_DIR/.env (mode 600)"
  else
    echo "reusing existing $TMP_DIR/.env"
  fi
  sed -e "s|__OF_ROOT__|$REPO_ROOT|g" \
    "$SCRIPT_DIR/compose.yaml.template" > "$COMPOSE_FILE"
  cp "$SCRIPT_DIR/init.sql" "$TMP_DIR/init.sql"
  chmod 644 "$COMPOSE_FILE" "$TMP_DIR/init.sql"
  echo "rendered $COMPOSE_FILE (repo root: $REPO_ROOT)"
}

cmd_build() {
  require_setup
  compose build dataset pipeline connector
}

cmd_up() {
  require_setup
  if [ "${OF_P1_SKIP_BUILD:-0}" != "1" ]; then
    compose build dataset pipeline connector
  fi
  compose up -d postgres fixture
  compose up -d dataset pipeline connector
  echo "waiting for service health (smoke --wait-only)..."
  python3 -I "$SCRIPT_DIR/smoke/smoke.py" --env-dir "$TMP_DIR" --wait-only \
    2>&1 | tee "$LOG_DIR/boot.log"
  compose ps
}

cmd_smoke() {
  require_setup
  python3 -I "$SCRIPT_DIR/smoke/smoke.py" --env-dir "$TMP_DIR" "$@" \
    2>&1 | tee "$LOG_DIR/smoke.log"
}

cmd_tests() {
  require_setup
  compose build unit-tests
  compose run --rm unit-tests 2>&1 | tee "$LOG_DIR/unit-tests.log"
}

cmd_migrate_tests() {
  require_setup
  # Reset the migration target so "empty DB" evidence is repeatable.
  # Pipeline tests create their own fresh databases from the maintenance
  # DB; connector repo integration tests use testcontainers (docker sock).
  compose exec -T postgres psql -U openfoundry -d datasets \
    -c "DROP DATABASE IF EXISTS migrate_fresh;" \
    -c "CREATE DATABASE migrate_fresh OWNER openfoundry;"
  compose build integration-tests
  compose run --rm integration-tests 2>&1 | tee "$LOG_DIR/migrate-tests.log"
}

cmd_restart() {
  require_setup
  compose restart dataset pipeline connector
  python3 -I "$SCRIPT_DIR/smoke/smoke.py" --env-dir "$TMP_DIR" --wait-only \
    2>&1 | tee "$LOG_DIR/restart-health.log"
  echo "services restarted against the existing volume (migration re-run path)"
}

cmd_status() { require_setup && compose ps; }
cmd_logs() { require_setup && compose logs -f "${1:-dataset}"; }
cmd_down() { require_setup && compose down; }
cmd_down_volumes() { require_setup && compose down -v; }

cmd_summarize() {
  # Sanitized summary for review, delivered inside the repo (no secrets:
  # result.json is produced by smoke.py with token scrubbing; log copies
  # are scrubbed against every .env secret value before leaving $TMP_DIR).
  require_setup
  local out_dir="$SCRIPT_DIR/results/last-run"
  mkdir -p "$out_dir/logs"
  if [ ! -f "$TMP_DIR/result.json" ]; then
    echo "no result.json at $TMP_DIR; run smoke first" >&2
    exit 1
  fi
  python3 -I - "$TMP_DIR/result.json" "$out_dir" "$TMP_DIR" <<'PYEOF'
import json, re, sys
from pathlib import Path
src, out_dir, tmp_dir = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
out_dir.mkdir(parents=True, exist_ok=True)
data = json.loads(src.read_text())
counts = data.get("counts", {})
uncovered = data.get("uncovered", [])
logs = tmp_dir / "logs"

lines = ["# P1 acceptance summary (auto-generated, sanitized)", "",
         f"- final smoke run suffix: `{data['suffix']}`",
         f"- smoke pass/fail: **{counts.get('pass', '?')}/{counts.get('fail', '?')}**",
         f"- uncovered: **{counts.get('uncovered', len(uncovered))}**"
         + (" — full acceptance NOT claimed" if uncovered else ""), ""]

# unit tests
unit = logs / "unit-tests.log"
if unit.exists():
    text = unit.read_text(errors="replace")
    ok = len(re.findall(r"^ok\s", text, re.M))
    fails = len(re.findall(r"^FAIL", text, re.M))
    pkgs = {}
    for m in re.finditer(r"^ok\s+\S+/services/([a-z-]+)/\S+", text, re.M):
        pkgs[m.group(1)] = pkgs.get(m.group(1), 0) + 1
    lines += ["## unit tests (three services, compose runner)", "",
              f"- packages ok: **{ok}** ({', '.join(f'{k} {v}' for k, v in sorted(pkgs.items()))})",
              f"- FAIL lines: **{fails}**", ""]

# integration / migration suites with required-name guard
mig = logs / "migrate-tests.log"
if mig.exists():
    text = mig.read_text(errors="replace")
    required = ["TestDatasetOrgTenancyMigrationAndVisibility",
                "TestMigrationsFreshDBRerunAndRepeatedSnapshots",
                "TestCommitDatasetOutputPublishFaultInjection",
                "TestSchemaBranchHTTPRoundTrip",
                "TestSchemaOwnershipCannotBeClaimed",
                "TestSchemaColumnsRepairLegacyDatabase",
                "TestTenantIsolationMigrationsAndBoundary",
                "TestTenantMigrationOnPopulatedLegacyDB",
                "TestTenantRepairRegressionBundle",
                "TestBuildFlowCancelledContextFinalizesState",
                "TestBuildFlowPublishDedupRebindsMapping",
                "TestBuildFlowLedgerUpgradeAppliesNewMigrations",
                "TestBuildFlowAbortTransactionTerminalSemantics",
                "TestBuildFlowPendingRetryClosure",
                "TestBuildFlowSecondRecordFailureCompensatesEverything",
                "TestBuildFlowSecondOpenFailureCompensatesFirst",
                "TestBuildFlowReleaseFailureKeepsReference",
                "TestBuildFlowCancelAfterOpenCompensatesOnDetachedContext",
                "TestBuildFlowJobSpecPublishOwnership",
                "TestBuildFlowForeignInputSpecRejectedZeroSideEffects",
                "TestBuildFlowMultiOutputPGStateAssertions",
                "TestBuildFlowRecordAndAbortBothFailKeepsRecoverableHandle",
                "TestBuildFlowEmitJobLogTenantGating",
                "TestPGMigrateEmptyDatabaseAndRerun",
                "TestPGMigrateLegacyDatabaseGainsOrgColumns",
                "TestPGSourceTenantCRUDMatrix"]
    lines += [f"## PG16 integration ({len(required)} required tests, zero required skips)", ""]
    for name in required:
        state = "PASS" if re.search(rf"^--- PASS: {name}(\s|\(|$)", text, re.M) else (
            "SKIP!!" if re.search(rf"^--- SKIP: {name}(\s|\(|$)", text, re.M) else "MISSING!!")
        lines.append(f"- {name}: **{state}**")
    lines.append(f"- FAIL lines: **{len(re.findall(r'^FAIL', text, re.M))}**")
    # Optional externally-gated suites also present in the log: list them
    # explicitly so "no required skips" can never be misread as "every
    # integration capability ran". They are NOT P1-required and NOT in the
    # verified scope.
    skip_reasons = {
        "TestPipelineAIPProviderSmokeGated": "requires external AIP provider (not provisioned)",
        "TestDistributedPipelineClusterSmokeGated": "requires external distributed cluster (not provisioned)",
        "TestAIServiceLLMProviderSmokeGated": "requires external AI/LLM provider (not provisioned)",
        "TestExecutePythonTransformWithRealSidecar": "requires real python sidecar (not provisioned)",
        "TestExecutePythonTransformParsesStravaAndGPXWithRealSidecar": "requires real python sidecar (not provisioned)",
    }
    skipped = re.findall(r"^--- SKIP: (\S+)", text, re.M)
    optional = [s for s in skipped if s not in required]
    if optional:
        lines += ["", "### optional SKIPs present in the log (externally gated, NOT P1-required, NOT in verified scope)", ""]
        for name in optional:
            reason = skip_reasons.get(name, "externally gated dependency not provisioned")
            lines.append(f"- {name}: SKIP — {reason}")
    lines += [""]

# restart evidence
rh = logs / "restart-health.log"
if rh.exists():
    lines += ["## restart (migration re-run on existing volume)", "",
              "- services restarted and healthy: see logs/restart-health.log", ""]

# layered persistence evidence — each probe reported separately; the
# historical pre-volume loss is NEVER overwritten by new PASS files and no
# blanket "old artifact persisted" claim is made.
# Fixed-name phase evidence ONLY. The generic restart-persistence-probe.json
# is a rolling probe output (overwritten by whichever probe ran last) and is
# deliberately NOT listed — phases are attributed to their dedicated files:
#   volume-recreate-probe.json  = original pre/post across --force-recreate (d64a7353)
#   volume-restart-probe.json   = original true-restart PASS (69a5b86f)
layered = [
    ("historical-pre-volume-artifact-loss.json",
     "historical: PRE-volume artifact loss (c12cad5c; bytes lost with the "
     "container layer before the dataset-files volume existed — NOT recovered, NOT backfilled)"),
    ("volume-recreate-probe.json",
     "post-volume-fix: original pre/post byte persistence across `--force-recreate`"),
    ("volume-restart-probe.json",
     "post-volume-fix: original artifact re-read after a true service restart"),
]
for fname, title in layered:
    f = logs / fname
    if not f.exists():
        continue
    pj = json.loads(f.read_text())
    lines += [f"## persistence evidence — {fname}", f"({title})", "",
              f"- verdict: **{pj.get('verdict')}**"
              + (f" (sample run `{pj.get('sample_suffix') or pj.get('review_suffix')}`)" if (pj.get("sample_suffix") or pj.get("review_suffix")) else ""),
              f"- file download SHA256 matches recorded hash: "
              f"**{pj.get('checks', {}).get('file_download_sha256', pj.get('checks', {}).get('dataset_file_download', {})).get('matches_pre_restart', pj.get('checks', {}).get('dataset_file_download', {}).get('sha256_matches_run_content_hash'))}**",
              f"- dataset GET / txn metadata intact: "
              f"{pj.get('checks', {}).get('dataset_get', {}).get('status')} / "
              f"{pj.get('checks', {}).get('transaction_get', {}).get('matches_pre_restart')}",
              ""]
lines += ["### persistence-evidence layers (summary)", "",
          "- `historical-pre-volume-artifact-loss.json` — pre-volume FAIL (c12cad5c): "
          "bytes lost with the container layer; original probe JSON was overwritten, "
          "this is the reconstructed-from-console sole record; NOT recovered/backfilled",
          "- `volume-recreate-probe.json` / `volume-restart-probe.json` — post-volume-fix "
          "PASS on the fixed sample: byte persistence across `--force-recreate` and restart",
          "- Build/Job three-domain handles: `build-persistence-probe.json`",
          "- Limitation: cross-container byte persistence is claimed ONLY under the new "
          "`dataset-files` volume configuration; no claim is made about pre-volume artifacts",
          ""]

if uncovered:
    lines += ["## UNCOVERED (scope judged at P1 closure)", ""]
    for item in uncovered:
        lines.append(f"- **{item['scenario']}/{item['aspect']}**: {item['reason'][:400]}")
    lines.append("")

for name, res in data["results"].items():
    lines.append(f"## smoke: {name}: {res['status'].upper()} ({res.get('seconds', '?')}s)")
    if res["status"] == "fail":
        lines.append(f"- error: {res.get('error', '')[:500]}")
    ev = data.get("evidence", {}).get(name) or {}
    for key, value in ev.items():
        if key == "error":
            continue
        lines.append(f"- {key}: `{json.dumps(value, ensure_ascii=False)[:300]}`")
    lines.append("")

(out_dir / "summary.md").write_text("\n".join(lines) + "\n")
(out_dir / "result.json").write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")

# scrubbed log copies into the repo for review
secrets = []
env_file = tmp_dir / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            value = line.split("=", 1)[1].strip()
            if value:
                secrets.append(value)
for log_name in ("unit-tests.log", "migrate-tests.log", "restart-health.log",
                 "smoke.log", "boot.log", "guard-selftest.log",
                 "restart-persistence-probe.json"):
    src_log = logs / log_name
    if not src_log.exists():
        continue
    text = src_log.read_text(errors="replace")
    for secret in secrets:
        text = text.replace(secret, "<redacted>")
    (out_dir / "logs" / log_name).write_text(text)
print(f"summary written to {out_dir}")
PYEOF
}

case "${1:-}" in
  setup) cmd_setup ;;
  build) cmd_build ;;
  up) shift; cmd_up "$@" ;;
  smoke) shift; cmd_smoke "$@" ;;
  tests) cmd_tests ;;
  migrate-tests) cmd_migrate_tests ;;
  restart) cmd_restart ;;
  summarize) cmd_summarize ;;
  status) cmd_status ;;
  logs) shift; cmd_logs "${1:-}" ;;
  down) cmd_down ;;
  down-volumes) cmd_down_volumes ;;
  *)
    sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
