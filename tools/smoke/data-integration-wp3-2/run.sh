#!/usr/bin/env bash
# WP3.2 worker-B acceptance runner (producer/infra/CDC base chain).
#
# Anti-greening contract (persist-then-assert wrappers in runner_lib.sh):
#   * every command's raw cmd_rc AND the tee rc land in exit-codes.txt
#     before any assertion — no catch-alls anywhere;
#   * Kafka evidence = explicit topic/partition end-offsets (kafka-get-
#     offsets.sh) + a deterministic --max-messages consume; a missing
#     offsets file FAILS the run (absence is never "empty");
#   * the CDC-gate negative control persists the pod JSON, the real
#     terminated.exitCode and the fail-closed reason before cleanup;
#   * source files are sha256-pinned pre/post (no full-repo archive);
#   * the tick image is traced docker→node→pod by immutable image ID.
#
# RESULT LAYERING: this runner proves the PRODUCER-SIDE BASE CHAIN on a
# real kind cluster (real CronJob automatic cycles → tick gates →
# transaction ① → Debezium CDC → Kafka envelope). It does NOT claim
# WP3.1 §6 V1–V8 — the consumer side waits for worker A.
#
# Isolation: namespace ofwp32 in a dedicated kind cluster with its OWN
# kubeconfig; synthetic credentials only; existing stacks untouched.
#
# Usage: ./run.sh [--keep]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%S)_$RANDOM"
RESULTS_DIR="$HERE/results/$RUN_ID"
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

KIND_BIN="/private/tmp/ofwp32-tools/bin/kind-new"
KUBECONFIG_OFWP32="/private/tmp/ofwp32-acc/kubeconfig"
KIND_CONFIG="/private/tmp/ofwp32-acc/kind-config.yaml"
NODE_IMAGE="kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed"
CLUSTER="ofwp32-acc"
NS="ofwp32"
PG_POD=""

PG_IMAGE="postgres:16-alpine"
KAFKA_IMAGE="apache/kafka:3.7.1"
CONNECT_IMAGE="quay.io/debezium/connect:2.7.2.Final"
APICURIO_IMAGE="apicurio/apicurio-registry-mem:2.6.13.Final"
CURL_IMAGE="curlimages/curl:8.10.1"
TICK_IMAGE="ofwp32/schedules-tick:acc"
PG_DSN="postgres://ofwp32:ofwp32-synthetic-acc@ofwp32-pg:5432/ofwp32?sslmode=disable"

TRIGGER_SHA="cd18c6212d2d9d35b0c092669daee58049c36e15d8a3174ca173e95e63eea92f"
OUTCOME_SHA="aef7a88509a097fe41c8ec50d484ca75d75910c2bc1881c3104138a56fc06a12"

mkdir -p "$RESULTS_DIR"
EXIT_CODES="$RESULTS_DIR/exit-codes.txt"
: >"$EXIT_CODES"

source "$HERE/runner_lib.sh"

# Source files pinned by sha256 — the exact inputs this run compiled
# and deployed. Post-run hashes must match pre-run (no full archive).
source_sha_list() { # source_sha_list <outfile>
  {
    cd "$REPO_ROOT" || exit 1
    find libs/event-scheduler -name '*.go' -o -name '*.sql' -o -name 'Dockerfile' | sort
    find libs/schedule-events/schema -name '*.json' | sort
    find tools/smoke/data-integration-wp3-2 -maxdepth 2 \
      \( -name 'run.sh' -o -name 'runner_lib.sh' -o -name 'verify_results.py' \
         -o -name 'negative_controls.sh' -o -name 'status_check.py' -o -path '*/k8s/*' \) | sort
    echo infra/helm/infra/debezium/templates/kafka-connector-outbox-pipeline.yaml
    echo infra/helm/infra/debezium/values.yaml
    echo infra/helm/apps/of-data-engine/templates/schedules-tick-cronjob.yaml
    echo infra/helm/apps/of-data-engine/values.yaml
    echo infra/helm/infra/kafka-cluster/values.yaml
    echo infra/helm/infra/kafka-cluster/values-apicurio-2-6.yaml
    echo infra/helm/infra/kafka-cluster/templates/apicurio-schemas-schedule-events-job.yaml
    find infra/helm/infra/kafka-cluster/files/schemas -name 'pipeline.schedule.*.json' | sort
    echo libs/schedule-events/events.go
    echo go.mod
    echo go.sum
  } | xargs shasum -a 256 >"$1"
}

cleanup() {
  if [[ $KEEP -eq 1 ]]; then
    echo "[cleanup] --keep: leaving namespace $NS, cluster $CLUSTER and $RESULTS_DIR in place"
    return
  fi
  kc delete namespace "$NS" --ignore-not-found >/dev/null 2>&1 || true
  echo "[cleanup] removed namespace $NS (kept: kind cluster $CLUSTER, $RESULTS_DIR)"
}
trap cleanup EXIT

echo "== WP3.2 producer-chain acceptance run $RUN_ID =="

# ─── Phase 0: preflight, budget, images, versions, source pin ───────
echo "[phase] 0 preflight"
run_step "source-sha-pre" "$RESULTS_DIR/source-sha-pre.log" source_sha_list "$RESULTS_DIR/source-sha-pre.txt"
[[ -x "$KIND_BIN" ]] || { echo "kind binary missing at $KIND_BIN" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker missing" >&2; exit 1; }
command -v kubectl >/dev/null || { echo "kubectl missing" >&2; exit 1; }
export KUBECONFIG_OFWP32

if ! kc get nodes >/dev/null 2>&1; then
  run_step "kind-create-cluster" "$RESULTS_DIR/kind-create.log" \
    "$KIND_BIN" create cluster --config "$KIND_CONFIG" --kubeconfig "$KUBECONFIG_OFWP32" --image "$NODE_IMAGE" --wait 300s
  docker update --memory 6g --memory-swap 6g "${CLUSTER}-control-plane" >>"$RESULTS_DIR/kind-create.log" 2>&1 || true
fi
run_step "preflight-cluster" "$RESULTS_DIR/preflight-cluster.log" bash -c '
  set -e
  kubectl --kubeconfig "'"$KUBECONFIG_OFWP32"'" get nodes -o wide
  kubectl --kubeconfig "'"$KUBECONFIG_OFWP32"'" version -o json
'
docker stats --no-stream >"$RESULTS_DIR/docker-stats-before.txt"

# Image load: docker save | node ctr import (kind load's --all-platforms
# trips over the docker-desktop containerd image store; the manual pipe
# imports the single-platform tar deterministically).
for img in "$PG_IMAGE" "$KAFKA_IMAGE" "$CONNECT_IMAGE" "$APICURIO_IMAGE" "$CURL_IMAGE"; do
  # Cached images are independently inspected, never a swallowed failed pull.
  if docker image inspect "$img" >/dev/null 2>&1; then
    run_step "inspect-cached-${img##*/}" "$RESULTS_DIR/inspect-cached-${img##*/}.log" \
      docker image inspect "$img"
  else
    run_step "pull-${img##*/}" "$RESULTS_DIR/pull-${img##*/}.log" docker pull "$img"
  fi

  run_step "load-${img##*/}" "$RESULTS_DIR/load-${img##*/}.log" bash -c '
    set -o pipefail
    docker save "'"$img"'" | docker exec --privileged -i '"$CLUSTER"'-control-plane \
      ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -
  '
done

{
  echo "docker_server=$(docker version --format '{{.Server.Version}}')"
  echo "kubectl_client=$(kubectl version --client --output=json 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["clientVersion"]["gitVersion"])' 2>/dev/null || echo unknown)"
  echo "kubectl_server=$(kc version -o json 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["serverVersion"]["gitVersion"])' 2>/dev/null || echo unknown)"
  echo "kind=$("$KIND_BIN" --version)"
  for img in "$PG_IMAGE" "$KAFKA_IMAGE" "$CONNECT_IMAGE" "$APICURIO_IMAGE" "$CURL_IMAGE"; do
    echo "image $img = $(docker inspect --format '{{index .RepoDigests 0}}' "$img" 2>/dev/null || docker inspect --format '{{.Id}}' "$img")"
  done
} >"$RESULTS_DIR/versions.txt"
run_step "record-versions" "$RESULTS_DIR/record-versions.log" cat "$RESULTS_DIR/versions.txt"

# Stabilize the shared node (idempotent, this cluster only): the
# default kind coredns memory limit (170Mi) OOMKilled both DNS pods
# under JVM bootstrap load and took cluster DNS (and the Connect task)
# down; the node cap also needs headroom for control-plane + 3 JVMs.
run_step "stabilize-node" "$RESULTS_DIR/stabilize-node.log" bash -c '
  set -e
  docker update --memory 6500m --memory-swap 6500m '"$CLUSTER"'-control-plane
  kubectl --kubeconfig "'"$KUBECONFIG_OFWP32"'" -n kube-system set resources deployment/coredns \
    --limits=memory=512Mi --requests=memory=170Mi >/dev/null
  kubectl --kubeconfig "'"$KUBECONFIG_OFWP32"'" -n kube-system rollout status deployment/coredns --timeout=180s
'

# ─── Phase 1: build + load the schedules-tick image ─────────────────
echo "[phase] 1 build tick image"
# Stage a MINIMAL build context (go.mod + go.sum + libs/, ~5MB): the
# repo root carries ~8GB of historical smoke archives on a slow
# external volume — transferring them as Docker context times out the
# BuildKit session (4.18GB@541s observed). The binary only needs the
# module graph + libs/**; no global .dockerignore is touched (other
# workers own their build layouts).
BUILDCTX="$RESULTS_DIR/buildctx"
mkdir -p "$BUILDCTX"
cp "$REPO_ROOT/go.mod" "$REPO_ROOT/go.sum" "$BUILDCTX/"
cp -R "$REPO_ROOT/libs" "$BUILDCTX/libs"
run_step "docker-build-tick" "$RESULTS_DIR/build-tick.log" \
  docker build -f "$REPO_ROOT/libs/event-scheduler/Dockerfile" \
    --build-arg VERSION="ofwp32-$RUN_ID" \
    --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 \
    -t "$TICK_IMAGE" "$BUILDCTX"
docker image inspect "$TICK_IMAGE" >"$RESULTS_DIR/tick-image-docker-inspect.json"
run_step "export-tick" "$RESULTS_DIR/export-tick.log" docker save -o "$RESULTS_DIR/tick-image.tar" "$TICK_IMAGE"
python3 - "$RESULTS_DIR" <<'PYCONFIG'
import hashlib, json, pathlib, sys, tarfile
r = pathlib.Path(sys.argv[1])
with tarfile.open(r / "tick-image.tar") as archive:
    manifests = json.load(archive.extractfile("manifest.json"))
    assert len(manifests) == 1, "expected single saved runtime image"
    manifest = manifests[0]
    config = archive.extractfile(manifest["Config"]).read()
    config_id = "sha256:" + hashlib.sha256(config).hexdigest()
    (r / "tick-image-docker-id.txt").write_text(config_id + "\n")
    (r / "tick-image-save-manifest.json").write_text(json.dumps(manifest, indent=2))
PYCONFIG
run_step "load-tick" "$RESULTS_DIR/load-tick.log" bash -c '
  set -o pipefail
  cat "'"$RESULTS_DIR"'/tick-image.tar" | docker exec --privileged -i '"$CLUSTER"'-control-plane \
    ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -
'

# ─── Phase 2: base chain ────────────────────────────────────────────
echo "[phase] 2 base chain"
run_step "apply-pg" "$RESULTS_DIR/apply-pg.log" kc apply -f "$HERE/k8s/10-pg.yaml"
run_step "wait-pg" "$RESULTS_DIR/wait-pg.log" bash -c '
  set -e
  kubectl --kubeconfig "'"$KUBECONFIG_OFWP32"'" -n '"$NS"' wait --for=condition=ready pod -l app=ofwp32-pg --timeout=300s
'
PG_POD="$(kc -n "$NS" get pods -l app=ofwp32-pg -o name | head -1 | sed 's|pod/||')"
echo "[evidence] PG_POD=$PG_POD"

run_step "pg-migrations" "$RESULTS_DIR/pg-migrations.log" bash -c '
  set -e
  POD='"$PG_POD"'
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  $K exec -i "$POD" -- psql -v ON_ERROR_STOP=1 -U ofwp32 -d ofwp32 < "'"$REPO_ROOT"'/libs/event-scheduler/migrations/0001_schedules_definitions.sql"
  $K exec -i "$POD" -- psql -v ON_ERROR_STOP=1 -U ofwp32 -d ofwp32 < "'"$REPO_ROOT"'/libs/event-scheduler/migrations/0002_schedules_outbox_dispatch.sql"
  $K exec -i "$POD" -- psql -v ON_ERROR_STOP=1 -U ofwp32 -d ofwp32 -c "SELECT count(*) FROM pg_tables WHERE schemaname IN ('"'"'schedules'"'"','"'"'outbox'"'"','"'"'scheduling'"'"')"
'

run_step "apply-kafka" "$RESULTS_DIR/apply-kafka.log" bash -c '
  set -e
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  $K apply -f "'"$HERE"'/k8s/20-kafka.yaml"
  $K wait --for=condition=ready pod -l app=ofwp32-kafka --timeout=600s
  KPOD=$($K get pods -l app=ofwp32-kafka -o name | head -1 | sed "s|pod/||")
  for t in pipeline.builds.v1 pipeline.schedule.outcomes.v1 __dlq.pipeline.schedule.outcomes.v1 __dlq.ofwp32.outbox-pipeline; do
    $K exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
  done
  $K exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
'

run_step "apply-connect-apicurio" "$RESULTS_DIR/apply-connect-apicurio.log" bash -c '
  set -e
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  $K apply -f "'"$HERE"'/k8s/30-connect.yaml" -f "'"$HERE"'/k8s/40-apicurio.yaml"
  $K wait --for=condition=ready pod -l app=ofwp32-connect --timeout=600s
  $K wait --for=condition=ready pod -l app=ofwp32-apicurio --timeout=600s
'

run_step "register-schemas" "$RESULTS_DIR/register-schemas.log" bash -c '
  set -e
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  SRC="'"$REPO_ROOT"'/libs/schedule-events/schema"
  $K create configmap ofwp32-schemas \
    --from-file="$SRC/pipeline.schedule.triggered.v1.json" \
    --from-file="$SRC/pipeline.schedule.outcome.v1.json" \
    --dry-run=client -o yaml | $K apply -f -
  $K create configmap ofwp32-register-script \
    --from-file=register.sh="'"$HERE"'/k8s/50-register.sh" \
    --dry-run=client -o yaml | $K apply -f -
  $K delete pod ofwp32-register --ignore-not-found >/dev/null
  $K run ofwp32-register --restart=Never --image='"$CURL_IMAGE"' \
    --overrides "{\"spec\":{\"containers\":[{\"name\":\"reg\",\"image\":\"'"$CURL_IMAGE"'\",\"env\":[{\"name\":\"TRIGGER_SHA256\",\"value\":\"'"$TRIGGER_SHA"'\"},{\"name\":\"OUTCOME_SHA256\",\"value\":\"'"$OUTCOME_SHA"'\"},{\"name\":\"APICURIO_BASE_URL\",\"value\":\"http://ofwp32-apicurio:8080\"},{\"name\":\"GROUP_ID\",\"value\":\"default\"}],\"command\":[\"/bin/sh\",\"/scripts/register.sh\"],\"resources\":{\"requests\":{\"cpu\":\"50m\",\"memory\":\"64Mi\"},\"limits\":{\"cpu\":\"500m\",\"memory\":\"128Mi\"}},\"volumeMounts\":[{\"name\":\"schemas\",\"mountPath\":\"/etc/schemas\",\"readOnly\":true},{\"name\":\"scripts\",\"mountPath\":\"/scripts\",\"readOnly\":true}]}],\"volumes\":[{\"name\":\"schemas\",\"configMap\":{\"name\":\"ofwp32-schemas\"}},{\"name\":\"scripts\",\"configMap\":{\"name\":\"ofwp32-register-script\"}}],\"restartPolicy\":\"Never\"}}" >/dev/null
  $K wait --for=jsonpath={.status.phase}=Succeeded pod/ofwp32-register --timeout=300s
  $K logs ofwp32-register
'

run_step "register-connector" "$RESULTS_DIR/register-connector.log" bash -c '
  set -eo pipefail
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  R="'"$RESULTS_DIR"'"
  # Root review round 5: the earlier pod-side `grep -c` counted LINES
  # (status body is one compact JSON line) so the all-tasks gate could
  # never pass — the ">600s task spawn" attribution is RETRACTED. All
  # parsing now happens on the HOST over the RAW body via
  # status_check.py; kubectl exec -i forwards stdin for the PUT.
  # Wait for the Connect REST listener first (pod Running/Ready is not
  # HTTP evidence).
  for i in $(seq 1 60); do
    if $K exec deploy/ofwp32-connect -- curl -sfo /dev/null http://localhost:8083/; then break; fi
    echo "waiting for connect REST (${i}/60)…"
    sleep 5
  done
  # Idempotent config PUT via stdin (kubectl exec -i is a verified
  # stdin-forwarding primitive, unlike kubectl run without -i).
  $K exec -i deploy/ofwp32-connect -- curl -sfS -X PUT \
    -H "Content-Type: application/json" --data-binary @- \
    http://localhost:8083/connectors/ofwp32-outbox-pipeline/config \
    < "'"$HERE"'/k8s/60-connector.json" > "$R/connector-put-response.json"
  echo "connector config PUT ok"
  for i in $(seq 1 120); do
    if $K exec deploy/ofwp32-connect -- \
      curl -sf http://localhost:8083/connectors/ofwp32-outbox-pipeline/status \
      > "$R/connector-status-raw.json" 2>/dev/null; then
      set +e
      python3 "'"$HERE"'/status_check.py" "$R/connector-status-raw.json" | tee /dev/stderr
      rc=${PIPESTATUS[0]}
      set -e
      [ "$rc" -eq 0 ] && exit 0
    else
      echo "status fetch failed (attempt ${i})"
    fi
    sleep 5
  done
  echo "connector never reached RUNNING (connector + all tasks)" >&2
  exit 1
'

run_step "verify-cdc-slot" "$RESULTS_DIR/verify-cdc-slot.log" bash -c '
  set -e
  POD='"$PG_POD"'
  kubectl --kubeconfig "'"$KUBECONFIG_OFWP32"'" -n '"$NS"' exec "$POD" -- \
    psql -U ofwp32 -d ofwp32 -Atc \
    "SELECT slot_name,database,plugin,active FROM pg_replication_slots WHERE slot_name='"'"'ofwp32_outbox_pipeline'"'"'" \
    | tee /dev/stderr | grep -qx "ofwp32_outbox_pipeline|ofwp32|pgoutput|t"
'

# ─── Phase 3: producer scenarios (base-chain layer) ─────────────────
echo "[phase] 3 producer scenarios (base chain only, NOT V1-V8)"
run_step "apply-cronjob" "$RESULTS_DIR/apply-cronjob.log" kc apply -f "$HERE/k8s/70-cronjob.yaml"

run_step "seed-real" "$RESULTS_DIR/seed-real.log" bash -c '
  set -e
  POD='"$PG_POD"'
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  $K exec -i "$POD" -- psql -v ON_ERROR_STOP=1 -U ofwp32 -d ofwp32 <<"SQL"
WITH g AS (
  INSERT INTO scheduling.grants
    (id, tenant_kind, tenant_id, org_id, subject, schedule_key, pipeline_rid,
     generation, state, expires_at, token_ciphertext, created_by)
  VALUES ('"'"'11111111-aaaa-4111-8111-000000000001'"'"','"'"'org'"'"',
          '"'"'11111111-bbbb-4111-8111-000000000002'"'"','"'"'11111111-bbbb-4111-8111-000000000002'"'"',
          '"'"'11111111-cccc-4111-8111-000000000003'"'"','"'"'nightly-build'"'"',
          '"'"'ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111'"'"',
          1,'"'"'active'"'"', now()+interval '"'"'1 day'"'"','"'"'\\x00'"'"'::bytea,
          '"'"'11111111-cccc-4111-8111-000000000003'"'"')
  RETURNING id, tenant_id
), good AS (
  INSERT INTO schedules.definitions
    (id,name,cron_expr,cron_flavor,time_zone,enabled,topic,payload_template,next_run_at,
     dispatch_mode,tenant_kind,tenant_id,schedule_key,definition_revision,pipeline_rid,build_branch,grant_ref)
  SELECT '"'"'22222222-aaaa-4111-8111-000000000011'"'"','"'"'wp32-good'"'"','"'"'*/1 * * * *'"'"','"'"'unix5'"'"','"'"'UTC'"'"',true,
         '"'"'pipeline.builds.v1'"'"','"'"'{"k":"v"}'"'"'::jsonb, now(),
         '"'"'pipeline_outbox_v1'"'"','"'"'org'"'"',g.tenant_id,'"'"'nightly-build'"'"',1,
         '"'"'ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111'"'"','"'"'main'"'"',g.id FROM g
  RETURNING id
), bad AS (
  INSERT INTO schedules.definitions
    (id,name,cron_expr,cron_flavor,time_zone,enabled,topic,payload_template,next_run_at,
     dispatch_mode,tenant_kind,tenant_id,schedule_key,definition_revision,pipeline_rid,build_branch,grant_ref)
  SELECT '"'"'22222222-bbbb-4111-8111-000000000012'"'"','"'"'wp32-badcron'"'"','"'"'not-a-cron'"'"','"'"'unix5'"'"','"'"'UTC'"'"',true,
         '"'"'pipeline.builds.v1'"'"','"'"'{"k":"v"}'"'"'::jsonb, now(),
         '"'"'pipeline_outbox_v1'"'"','"'"'org'"'"',g.tenant_id,'"'"'broken-one'"'"',1,
         '"'"'ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111'"'"','"'"'main'"'"',g.id FROM g
  RETURNING id
)
INSERT INTO schedules.definitions
  (id,name,cron_expr,cron_flavor,time_zone,enabled,topic,payload_template,next_run_at)
VALUES ('"'"'22222222-cccc-4111-8111-000000000013'"'"','"'"'wp32-legacy'"'"','"'"'* * * * *'"'"','"'"'unix5'"'"','"'"'UTC'"'"',true,
        '"'"'pipeline.builds.v1'"'"','"'"'{"k":"v"}'"'"'::jsonb, now());
SQL
  # Seed timestamp is written ONLY after the seed transaction committed.
  date -u +%Y-%m-%dT%H:%M:%SZ > "'"$RESULTS_DIR"'/seed-time.txt"
  $K exec "$POD" -- psql -U ofwp32 -d ofwp32 -Atc \
    "SELECT count(*) FROM schedules.definitions WHERE dispatch_mode='"'"'pipeline_outbox_v1'"'"'" | tee /dev/stderr | grep -qx 2
'

echo "[gate] waiting for >= 2 AUTOMATIC CronJob cycles created AFTER the seed (max 300s)…"
run_step "wait-auto-cycles" "$RESULTS_DIR/wait-auto-cycles.log" bash -c '
  set -e
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  SEED_TS=$(cat "'"$RESULTS_DIR"'/seed-time.txt")
  for i in $(seq 1 60); do
    done_n=$($K get jobs -o json | SEED_TS="$SEED_TS" python3 -c "
import json,sys,os
d=json.load(sys.stdin)
seed=os.environ[\"SEED_TS\"]
n=0
for j in d[\"items\"]:
    created=j[\"metadata\"].get(\"creationTimestamp\",\"\")
    if created <= seed:
        continue  # pre-seed (empty) ticks are not cycle evidence
    if any(c[\"type\"]==\"Complete\" and c[\"status\"]==\"True\" for c in j.get(\"status\",{}).get(\"conditions\",[])):
        n+=1
print(n)")
    total=$($K get jobs -o name | wc -l)
    echo "tick ${i}: jobs_total=$total completed_after_seed=$done_n (seed=$SEED_TS)"
    [ "$done_n" -ge 2 ] && exit 0
    sleep 5
  done
  echo "fewer than 2 post-seed automatic cycles completed" >&2
  exit 1
'

run_step "collect-evidence" "$RESULTS_DIR/collect-evidence.log" bash -c '
  set -eo pipefail
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  POD='"$PG_POD"'
  KPOD=$($K get pods -l app=ofwp32-kafka -o name | head -1 | sed "s|pod/||")
  R="'"$RESULTS_DIR"'"
  # 1) Explicit topic/partition end-offset evidence (raw, no catch-all):
  #    a MISSING offsets file fails the verifier — absence != empty.
  $K exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic pipeline.builds.v1 | tee "$R/kafka-offsets-pipeline-builds.txt"
  $K exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic __dlq.ofwp32.outbox-pipeline | tee "$R/kafka-offsets-dlq.txt"
  total=$(awk -F: "{s+=\$NF} END{print s+0}" "$R/kafka-offsets-pipeline-builds.txt")
  echo "pipeline.builds.v1 total messages (end offsets): $total"
  # 2) Deterministic consume of exactly the known message count.
  if [ "$total" -gt 0 ]; then
    $K exec "$KPOD" -- /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic pipeline.builds.v1 \
      --from-beginning --max-messages "$total" --timeout-ms 30000 \
      > "$R/kafka-pipeline-builds.ndjson"
  else
    : > "$R/kafka-pipeline-builds.ndjson"
  fi
  wc -l "$R/kafka-pipeline-builds.ndjson"
  # 3) DB state snapshot.
  $K exec "$POD" -- psql -U ofwp32 -d ofwp32 -Atc \
    "SELECT json_build_object('"'"'audit'"'"',(SELECT coalesce(json_agg(row_to_json(a)),json_build_array()) FROM scheduling.dispatch_audit a),
                               '"'"'defs'"'"',(SELECT json_agg(row_to_json(d)) FROM schedules.definitions d),
                               '"'"'outbox_rows'"'"',(SELECT count(*) FROM outbox.events))" \
    > "$R/db-state.json"
  # 4) CronJob job history + pod imageID chain + tick logs.
  $K get jobs -o json > "$R/tick-jobs.json"
  # Node-level structured evidence (read-only) + per-job completed pods.
  docker inspect '"$CLUSTER"'-control-plane > "$R/node-inspect.json"
  docker exec '"$CLUSTER"'-control-plane crictl images -o json > "$R/node-images.json"
  for j in $($K get jobs -o name | sed "s|.*/||"); do
    p=$($K get pods -l job-name="$j" -o name 2>/dev/null | head -1 | sed "s|pod/||" || true)
    if [ -n "${p:-}" ]; then
      $K get pod "$p" -o json > "$R/tick-pod-$j.json" 2>/dev/null || true
      $K logs "$p" > "$R/tick-log-$j.txt" 2>&1 || true
    fi
  done
  echo collected
'

run_step "image-chain" "$RESULTS_DIR/image-chain.log" bash -c '
  set -eo pipefail
  R="'"$RESULTS_DIR"'"
  python3 - "$R" <<PY
import hashlib, json, os, sys
r = sys.argv[1]
docker_id = open(os.path.join(r, "tick-image-docker-id.txt")).read().strip()
node_images = json.load(open(os.path.join(r, "node-images.json")))
node_id = ""
for img in node_images.get("images", []):
    for tag in img.get("repoTags", []) or []:
        if "schedules-tick" in tag:
            node_id = img.get("id", "")
jobs = json.load(open(os.path.join(r, "tick-jobs.json")))
pods = {}
for f in sorted(os.listdir(r)):
    if not f.startswith("tick-pod-") or not f.endswith(".json"):
        continue
    job = f[len("tick-pod-"):-len(".json")]
    d = json.load(open(os.path.join(r, f)))
    for cs in d.get("status", {}).get("containerStatuses", []):
        pods[job] = {"imageID": cs.get("imageID", ""), "image": cs.get("image", ""),
                     "phase": d.get("status", {}).get("phase", "")}
json.dump({"docker_id": docker_id, "node_image_id": node_id, "pods": pods},
          open(os.path.join(r, "image-chain.json"), "w"), indent=1, sort_keys=True)
print(json.dumps({"docker_id": docker_id, "node_image_id": node_id,
                  "pod_count": len(pods)}, sort_keys=True))
PY
'

# Negative control: wrong CDC slot must fail closed. The pod JSON, the
# REAL terminated exitCode and the fail-closed reason are persisted
# BEFORE the pod is deleted.
run_step "negctl-cdc-gate" "$RESULTS_DIR/negctl-cdc-gate.log" bash -c '
  set -e
  K="kubectl --kubeconfig '"$KUBECONFIG_OFWP32"' -n '"$NS"'"
  R="'"$RESULTS_DIR"'"
  $K delete pod ofwp32-negctl-gate --ignore-not-found >/dev/null
  $K run ofwp32-negctl-gate --restart=Never --image='"$TICK_IMAGE"' \
    --override-type=json --overrides="[{\"op\":\"add\",\"path\":\"/spec/containers/0/resources\",\"value\":{\"requests\":{\"cpu\":\"50m\",\"memory\":\"64Mi\"},\"limits\":{\"cpu\":\"500m\",\"memory\":\"128Mi\"}}}]" \
    --env="SCHEDULES_DISPATCH_MODE=pipeline_outbox_v1" \
    --env="DATABASE_URL='"$PG_DSN"'" \
    --env="SCHEDULES_REGISTRY_URL=http://ofwp32-apicurio:8080" \
    --env="SCHEDULES_CDC_SLOT=ofwp32_wrong_slot" >/dev/null
  $K wait --for=jsonpath={.status.phase}=Failed pod/ofwp32-negctl-gate --timeout=180s
  $K get pod ofwp32-negctl-gate -o json > "$R/negctl-cdc-gate-pod.json"
  $K logs ofwp32-negctl-gate > "$R/negctl-cdc-gate-pod.log"
  $K delete pod ofwp32-negctl-gate >/dev/null
  python3 - "$R" <<PY
import json,sys,os
r=sys.argv[1]
d=json.load(open(os.path.join(r,"negctl-cdc-gate-pod.json")))
cs=d["status"]["containerStatuses"][0]
term=cs.get("state",{}).get("terminated",{})
code=term.get("exitCode")
log=open(os.path.join(r,"negctl-cdc-gate-pod.log")).read()
json.dump({"exit_code": code, "terminated_reason": term.get("reason"),
           "cdc_not_ready_in_log": "cdc not ready" in log,
           "slot_in_log": "ofwp32_wrong_slot" in log},
          open(os.path.join(r,"negctl-cdc-gate.json"),"w"), indent=1)
assert isinstance(code,int) and code != 0, f"exitCode must be non-zero int, got {code!r}"
assert "cdc not ready" in log, "log must contain the fail-closed reason"
print("negctl evidence persisted")
PY
'

# ─── Phase 4: strict verification + negative controls ───────────────
echo "[phase] 4 verify"
run_step "source-sha-post" "$RESULTS_DIR/source-sha-post.log" source_sha_list "$RESULTS_DIR/source-sha-post.txt"
verify_step "verify-basechain" basechain "$RESULTS_DIR" "$RUN_ID"
run_step "negative-controls" "$RESULTS_DIR/negative-controls.log" \
  bash "$HERE/negative_controls.sh" "$HERE/verify_results.py" "$RESULTS_DIR"
verify_step "exit-codes-all-zero" exitcodes "$EXIT_CODES"

echo "== run $RUN_ID PASSED (base chain; NOT V1–V8) =="
echo "results: $RESULTS_DIR"
ls -1 "$RESULTS_DIR"
