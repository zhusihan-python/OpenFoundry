#!/usr/bin/env bash
# PR3 — dataset-excel real-backend acceptance runner (one-shot, clean env).
#
# Makes the two REAL-backend tests in apps/web/e2e/dataset-excel-real.spec.ts
# reproducibly acceptable with one command, in a fully disposable stack:
#
#   * builds dataset-versioning-service + edge-gateway-service from THIS
#     worktree's sources inside golang:1.25 (go build -buildvcs=false, same
#     package path as `make build-services`; binary sha256 recorded),
#   * starts a per-run docker network + postgres:16 + DVS + real edge
#     gateway (rewrites /api/v1/datasets/* -> /v1/*) on a RANDOM free host
#     port — no dependency on the resident p31 containers, port 3931, or
#     /private/tmp/openfoundry-p31-review leftovers,
#   * mints a per-round synthetic admin JWT (random secret, public test
#     claims from runtime-public.json) via tools/smoke/business-platform-p1/
#     mint_token.py — the token value never enters logs, results or git,
#   * runs Playwright via the EXISTING playwright.p31-real.config.ts +
#     vite.p31-e2e.config.ts (identity endpoints stubbed locally by that
#     vite config — real login is NOT verified by this acceptance; see
#     README), with structured json results,
#   * enforces the anti-greening contract: real exit codes for every step
#     (command AND tee), strict structured verification (verify_results.py),
#     a LIVE skip control (suite run without P31_REAL_E2E must be REJECTED),
#     and bounded negative controls mutating the REAL run's JSON.
#
# Never touches: product behavior, the resident p31 containers (p31-gw /
# p31-dvs / p31-pg), any existing DB/volume/.env, other worktrees.
# Removes exactly what it created (per-run containers/network + token file);
# results/<RUN_ID>/ stays on disk (gitignored) as evidence.
#
# Usage: ./run.sh [--keep]     (--keep: keep containers/network for debug)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
WEB_DIR="$REPO_ROOT/apps/web"
MINT_TOKEN="$REPO_ROOT/tools/smoke/business-platform-p1/mint_token.py"
DOCKER="${OFEXCEL_DOCKER:-docker}"
command -v "$DOCKER" >/dev/null 2>&1 || {
  for cand in /Applications/Docker.app/Contents/Resources/bin/docker \
              /usr/local/bin/docker /opt/homebrew/bin/docker; do
    [[ -x "$cand" ]] && DOCKER="$cand" && break
  done
  command -v "$DOCKER" >/dev/null 2>&1 || { echo "docker not found" >&2; exit 1; }
}

RUN_ID="$(date -u +%Y%m%dT%H%M%S)_$RANDOM"
RESULTS_DIR="$HERE/results/$RUN_ID"
BIN_DIR="$RESULTS_DIR/bin"
FIXTURE="$WEB_DIR/e2e/fixtures/orders-real.xlsx"
GO_IMAGE="${OFEXCEL_GO_IMAGE:-golang:1.25}"
PG_IMAGE="${OFEXCEL_PG_IMAGE:-postgres:16}"
# Reusable (never deleted) module/build caches; created on first use.
MODCACHE="${OFEXCEL_MODCACHE:-openfoundry-p31-gomod}"
BUILDCACHE="${OFEXCEL_BUILDCACHE:-of-go-build}"

NET="ofexcel-net-$RUN_ID"
PG="ofexcel-pg-$RUN_ID"
DVS="ofexcel-dvs-$RUN_ID"
GW="ofexcel-gw-$RUN_ID"
# Public TEST identity values from the p31 handoff (runtime-public.json) —
# these claims are documented public test fixtures, not secrets. The JWT
# SECRET is freshly random per run and never persisted.
TOKEN_SUB="1a5554ba-d8be-485c-923b-234d6f2629a6"
TOKEN_ORG="dd3f286c-9c17-4a0e-932f-286968d8a8a7"

KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

mkdir -p "$BIN_DIR"
EXIT_CODES="$RESULTS_DIR/exit-codes.txt"
: >"$EXIT_CODES"

# Per-round synthetic token material: live in a private mktemp dir, removed
# on exit. Only their sha256 (hash, not value) enters the evidence.
TOKEN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ofexcel-token.XXXXXX")"
chmod 700 "$TOKEN_DIR"
TOKEN_FILE="$TOKEN_DIR/token_admin.txt"

cleanup() {
  # Archive service logs first (always, pass or fail — diagnostics evidence).
  "$DOCKER" logs "$DVS" >"$RESULTS_DIR/dvs.log" 2>&1 || true
  "$DOCKER" logs "$GW" >"$RESULTS_DIR/gw.log" 2>&1 || true
  if [[ $KEEP -eq 1 ]]; then
    echo "[cleanup] --keep: leaving $PG $DVS $GW / $NET in place"
  else
    # -v: postgres keeps its data in an anonymous volume; without -v the
    # per-run PG data would leak. Only ever touches THIS run's containers.
    "$DOCKER" rm -fv "$PG" "$DVS" "$GW" >/dev/null 2>&1 || true
    "$DOCKER" network rm "$NET" >/dev/null 2>&1 || true
    echo "[cleanup] removed $PG $DVS $GW (with anonymous volumes) and $NET (kept: $RESULTS_DIR)"
  fi
  rm -rf "$TOKEN_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT   # EXIT trap still runs cleanup on Ctrl-C / signals
trap 'exit 143' TERM

# ------------------------------------------------------------ preconditions --
[[ -f "$FIXTURE" ]] || { echo "fixture missing: $FIXTURE" >&2; exit 1; }
[[ -f "$MINT_TOKEN" ]] || { echo "mint_token.py missing: $MINT_TOKEN" >&2; exit 1; }
[[ -x "$WEB_DIR/node_modules/.bin/playwright" ]] || {
  echo "playwright not installed (pnpm --filter @open-foundry/web install --frozen-lockfile)" >&2; exit 1; }
"$DOCKER" info >/dev/null 2>&1 || { echo "docker unusable" >&2; exit 1; }
for vol in "$MODCACHE" "$BUILDCACHE"; do
  "$DOCKER" volume inspect "$vol" >/dev/null 2>&1 || "$DOCKER" volume create "$vol" >/dev/null
done

# Random free host ports (explicit-conflict failure downstream: docker -p
# refuses a taken port, vite runs strictPort).
read -r GW_HOST_PORT WEB_PORT <<<"$(python3 - <<'PY'
import socket
ports = []
for _ in range(2):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    ports.append(s.getsockname()[1])
    s.close()
print(*ports)
PY
)"

# Per-round JWT secret + synthetic admin token (public test claims).
JWT_SECRET="$(openssl rand -hex 32)"
python3 -I "$MINT_TOKEN" --secret "$JWT_SECRET" \
  --sub "$TOKEN_SUB" --org "$TOKEN_ORG" \
  --roles admin --permissions dataset.write,dataset:write,dataset.read \
  --hours 2 >"$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

source "$HERE/runner_lib.sh"

echo "== dataset-excel real-backend acceptance run $RUN_ID =="
echo "    gw host port : $GW_HOST_PORT"
echo "    web port     : $WEB_PORT"
echo "    results      : $RESULTS_DIR"

# ------------------------------------------------- [pre] ensure base images --
# A cold CI runner has NEITHER golang nor postgres pre-pulled; record-env's
# image inspect (and the later docker runs) would fail there. Inspect first
# and only pull what is missing — never touches/removes existing images.
echo
echo "== [pre] ensure base images present (inspect, pull only if missing) =="
run_step "ensure-images" "$RESULTS_DIR/ensure-images.log" \
  bash -c '
    set -e
    for ref in "$1" "$2"; do
      if '"$DOCKER"' image inspect "$ref" >/dev/null 2>&1; then
        echo "[images] $ref present locally"
      else
        echo "[images] $ref missing — pulling"
        '"$DOCKER"' pull "$ref"
      fi
    done
  ' _ "$GO_IMAGE" "$PG_IMAGE"

# --------------------------------------------------- [0] record environment --
echo
echo "== [0/6] record exact toolchain + image identities (tags are not versions) =="
run_step "record-env" "$RESULTS_DIR/record-env.log" \
  python3 - "$RESULTS_DIR/versions.json" "$WEB_DIR" "$DOCKER" "$GO_IMAGE" "$PG_IMAGE" <<'PY'
import json, subprocess, sys

out, web_dir, docker, go_img, pg_img = sys.argv[1:6]

def run(cmd, cwd=None):
    return subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)

def image_info(ref):
    r = run([docker, "image", "inspect", ref])
    if r.returncode != 0:
        return {"error": r.stderr.strip()[:200]}
    doc = json.loads(r.stdout)[0]
    return {"id": doc.get("Id", ""),
            "repo_digests": doc.get("RepoDigests", []) or []}

env = {
    "node": run(["node", "--version"]).stdout.strip(),
    "pnpm": run(["pnpm", "--version"]).stdout.strip(),
    "playwright": run(["./node_modules/.bin/playwright", "--version"],
                      cwd=web_dir).stdout.strip(),
    "go": run([docker, "run", "--rm", go_img, "go", "version"]).stdout.strip(),
    "docker_client": run([docker, "--version"]).stdout.strip(),
    "images": {go_img: image_info(go_img), pg_img: image_info(pg_img)},
}
for key in ("node", "pnpm", "playwright", "go"):
    if not env[key]:
        raise SystemExit(f"could not determine {key} version")
if not env["images"][go_img].get("id") or not env["images"][pg_img].get("id"):
    raise SystemExit("could not determine image identities (missing images?)")
with open(out, "w", encoding="utf-8") as fh:
    json.dump(env, fh, indent=2)
print(f"[excel] toolchain: node={env['node']} pnpm={env['pnpm']} "
      f"playwright={env['playwright'].split()[-1]} go={env['go'].split()[-2]}")
PY

# ------------------------------------------------------- [1] build binaries --
echo
echo "== [1/6] build DVS + edge-gateway from this worktree (golang:1.25) =="
run_step "build-dvs" "$RESULTS_DIR/build-dvs.log" \
  "$DOCKER" run --rm \
    -v "$REPO_ROOT:/work:ro" -v "$BIN_DIR:/out" \
    -v "$MODCACHE:/go/pkg/mod" -v "$BUILDCACHE:/root/.cache/go-build" \
    -w /work -e GOFLAGS=-buildvcs=false "$GO_IMAGE" \
    go build -buildvcs=false -o /out/dataset-versioning-service \
    ./services/dataset-versioning-service/cmd/dataset-versioning-service

run_step "build-gw" "$RESULTS_DIR/build-gw.log" \
  "$DOCKER" run --rm \
    -v "$REPO_ROOT:/work:ro" -v "$BIN_DIR:/out" \
    -v "$MODCACHE:/go/pkg/mod" -v "$BUILDCACHE:/root/.cache/go-build" \
    -w /work -e GOFLAGS=-buildvcs=false "$GO_IMAGE" \
    go build -buildvcs=false -o /out/edge-gateway-service \
    ./services/edge-gateway-service/cmd/edge-gateway-service

# ------------------------------------------------------- [2] disposable stack --
echo
echo "== [2/6] disposable stack: pg16 + dvs + real edge gateway =="
"$DOCKER" network create "$NET" >/dev/null
"$DOCKER" run -d --name "$PG" --network "$NET" \
  -e POSTGRES_USER=ofexcel -e POSTGRES_PASSWORD="ofexcel-pg-$RUN_ID" \
  -e POSTGRES_DB=ofexcel "$PG_IMAGE" >/dev/null
for i in $(seq 1 60); do
  "$DOCKER" exec "$PG" pg_isready -U ofexcel -d ofexcel >/dev/null 2>&1 && break
  sleep 1
  [[ $i -eq 60 ]] && { echo "[pg] FAILED to become ready" >&2; exit 1; }
done
echo "[stack] pg ready: $PG"

"$DOCKER" run -d --name "$DVS" --network "$NET" \
  -v "$BIN_DIR:/binaries:ro" --tmpfs /data \
  -e DATABASE_URL="postgres://ofexcel:ofexcel-pg-$RUN_ID@$PG:5432/ofexcel" \
  -e JWT_SECRET="$JWT_SECRET" -e HOST=0.0.0.0 -e PORT=8080 \
  -e DATASET_FILES_BASE_DIR=/data \
  --entrypoint /binaries/dataset-versioning-service "$GO_IMAGE" >/dev/null

"$DOCKER" run -d --name "$GW" --network "$NET" \
  -v "$BIN_DIR:/binaries:ro" \
  -p "127.0.0.1:$GW_HOST_PORT:8080" \
  -e OF_SERVER__PORT=8080 \
  -e OF_JWT__SECRET="$JWT_SECRET" \
  -e OF_UPSTREAM__DATASET_VERSIONING_SERVICE_URL="http://$DVS:8080" \
  --entrypoint /binaries/edge-gateway-service "$GO_IMAGE" >/dev/null
echo "[stack] started $DVS (repo.Migrate on boot) and $GW (publishes 127.0.0.1:$GW_HOST_PORT)"

# ------------------------------------------- [3] health + end-to-end probe --
echo
echo "== [3/6] health check + authenticated catalog probe (gw->dvs->pg) =="
run_step "stack-probe" "$RESULTS_DIR/stack-probe.log" \
  python3 "$HERE/probe_stack.py" "$GW_HOST_PORT" "$TOKEN_FILE" \
    "$RESULTS_DIR/stack-probe.json"

# ------------------------------------- [4] live negative control: SKIP gate --
echo
echo "== [4/6] live skip control: P31_REAL_E2E=0 must yield 2 real skips AND be rejected =="
# The opt-in gate is the spec's own test.skip(...). Playwright exits 0 on an
# all-skipped run — the classic false green. Three-gate control:
#   (a) the REAL suite runs with the gate explicitly closed and playwright
#       itself must exit 0 (a crash/missing-env is NOT a valid skip control);
#   (b) the report must genuinely show exactly the 2 required tests skipped
#       (machine-checked shape, not a grep);
#   (c) the strict verifier must REJECT that report (skips never count).
run_step "skip-control-playwright" "$RESULTS_DIR/playwright-skip.log" \
  env P31_REAL_E2E=0 P31_TOKEN_FILE="$TOKEN_FILE" P31_FIXTURE="$FIXTURE" \
      P31_WEB_PORT="$WEB_PORT" P31_GW_URL="http://127.0.0.1:$GW_HOST_PORT" \
      PLAYWRIGHT_JSON_OUTPUT_NAME="$RESULTS_DIR/playwright-skip.json" \
      bash -c "cd '$WEB_DIR' && pnpm exec playwright test \
        --config=playwright.ofexcel.config.ts --reporter=list,json \
        --output='$RESULTS_DIR/test-artifacts-skip'"

run_step "skip-control-shape" "$RESULTS_DIR/skip-control-shape.log" \
  python3 -c "
import json, sys
doc = json.load(open('$RESULTS_DIR/playwright-skip.json', encoding='utf-8'))
stats = doc['stats']
assert stats['expected'] == 0, f\"expected={stats['expected']} (a test PASSED in the gated run?)\"
assert stats['skipped'] == 2, f\"skipped={stats['skipped']}, want the 2 required tests skipped\"
assert stats['unexpected'] == 0 and stats['flaky'] == 0
print(f'[negctl] skip-run shape ok: {stats}')
"
expect_verifier_reject "skip-control" playwright "$RESULTS_DIR/playwright-skip.json"

# --------------------------------------------------- [5] the REAL acceptance --
echo
echo "== [5/6] REAL acceptance: P31_REAL_E2E=1 against the disposable stack =="
run_step "playwright-real" "$RESULTS_DIR/playwright-real.log" \
  env P31_REAL_E2E=1 P31_TOKEN_FILE="$TOKEN_FILE" P31_FIXTURE="$FIXTURE" \
      P31_WEB_PORT="$WEB_PORT" P31_GW_URL="http://127.0.0.1:$GW_HOST_PORT" \
      PLAYWRIGHT_JSON_OUTPUT_NAME="$RESULTS_DIR/playwright.json" \
      bash -c "cd '$WEB_DIR' && pnpm exec playwright test \
        --config=playwright.ofexcel.config.ts --reporter=list,json \
        --output='$RESULTS_DIR/test-artifacts'"
verify_step "playwright-real-verify" playwright "$RESULTS_DIR/playwright.json"

# ------------------------------------------- [6] evidence + negative controls --
echo
echo "== [6/6] evidence manifest + negative controls =="
set +e
python3 - "$RESULTS_DIR" "$REPO_ROOT" "$RUN_ID" "$GW_HOST_PORT" "$WEB_PORT" \
  "$NET" "$GO_IMAGE" "$PG_IMAGE" "$PG" "$DVS" "$GW" "$TOKEN_FILE" \
  "$TOKEN_SUB" "$FIXTURE" <<'PY'
import hashlib, json, subprocess, sys

(results, repo, run_id, gw_port, web_port, net, go_img, pg_img, pg, dvs, gw,
 token_file, sub, fixture) = sys.argv[1:15]

def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def artifact(name, path):
    return {"name": name, "path": path, "sha256": sha(path)}

def git_out(*args):
    return subprocess.run(["git", *args], cwd=repo, capture_output=True,
                          text=True, check=True).stdout.strip()

git = {
    "commit": git_out("rev-parse", "HEAD"),
    "dirty": bool(git_out("status", "--porcelain")),
    "branch": git_out("rev-parse", "--abbrev-ref", "HEAD"),
}
probe = json.load(open(f"{results}/stack-probe.json", encoding="utf-8"))
env_info = json.load(open(f"{results}/versions.json", encoding="utf-8"))
doc = {
    "run_id": run_id,
    "git": git,
    "env": env_info,
    "artifacts": [
        artifact("binary-dvs", f"{results}/bin/dataset-versioning-service"),
        artifact("binary-gw", f"{results}/bin/edge-gateway-service"),
        artifact("fixture-orders-real-xlsx", fixture),
        artifact("playwright-results", f"{results}/playwright.json"),
    ],
    "probe": {"gw_healthz_status": probe["gw_healthz_status"],
              "catalog_status": probe["catalog_status"]},
    "token": {"sub": sub, "sha256": sha(token_file),
              "note": "value not recorded here; per-run random secret; "
                      "playwright traces/test artifacts MAY contain the "
                      "token (localStorage) — keep results/ local and "
                      "gitignored, never upload raw traces; the JWT is "
                      "valid only against this run's disposable stack"},
    "stack": {"gw_host_port": int(gw_port), "web_port": int(web_port),
              "network": net, "go_image": go_img, "pg_image": pg_img,
              "containers": [pg, dvs, gw]},
}
with open(f"{results}/evidence.json", "w", encoding="utf-8") as fh:
    json.dump(doc, fh, indent=2)
print("[excel] evidence manifest written")
PY
MANIFEST_RC=$?
set -e
record_rc "update-manifest" "$MANIFEST_RC"

verify_step "verify-manifest" manifest "$RESULTS_DIR/evidence.json"

run_step "negative-controls" "$RESULTS_DIR/negative-controls.log" \
  bash "$HERE/negative_controls.sh" "$RESULTS_DIR/playwright.json" \
    "$RESULTS_DIR/evidence.json"

verify_step "exit-codes-all-zero" exitcodes "$EXIT_CODES"

echo
echo "== run $RUN_ID PASSED (2/2 real-backend tests executed & passed, evidence verified) =="
echo "results: $RESULTS_DIR"
ls -1 "$RESULTS_DIR"
