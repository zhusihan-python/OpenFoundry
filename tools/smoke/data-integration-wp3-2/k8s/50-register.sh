#!/bin/sh
# ofwp32 isolated acceptance — register + byte-verify the two frozen
# schedule-event schemas against the pinned Apicurio 2.6.13.Final v2
# registry. Runs INSIDE the curl pod (schemas mounted read-only at
# /etc/schemas). Byte-exact verification (root review round 2):
#   * GET bodies go to files; curl's own rc is enforced by -f — never
#     hash through a pipe or a shell variable.
#   * mounted bytes, registered bytes and the pinned sha256 must all
#     agree, trailing LF included.
set -eu
B="${APICURIO_BASE_URL:-http://ofwp32-apicurio:8080}/apis/registry/v2"
G="${GROUP_ID:-default}"
TMP="$(mktemp -d)"

# Bounded wait for the registry (the JVM can take minutes to listen
# under the acceptance-cluster memory cap; pod Ready without an HTTP
# probe only proves the process runs) — and VERSION CHECK: the running
# registry must report the pinned 2.6.13.Final, not merely answer.
BASE="${APICURIO_BASE_URL:-http://ofwp32-apicurio:8080}"
i=0
until curl -sfo /tmp/sysinfo.json "$BASE/apis/registry/v2/system/info" \
      && grep -q '2\.6\.13\.Final' /tmp/sysinfo.json; do
  i=$((i+1))
  [ "$i" -le 60 ] || { echo "apicurio not reachable/version-mismatched after 60 tries:" >&2; cat /tmp/sysinfo.json >&2 2>/dev/null || true; exit 1; }
  echo "waiting for apicurio 2.6.13.Final (${i}/60)…"
  sleep 5
done
echo "apicurio version ok: $(cat /tmp/sysinfo.json)"

reg_and_verify() {
  art="$1"; pinned="$2"
  # Registry artifact id has NO .json suffix; the mounted file DOES
  # (ConfigMap keys are the source file names) — two different names
  # for the same bytes (root review round 3).
  local_file="/etc/schemas/${art}.json"
  # ifExists enum is UPPERCASE in the 2.6.13.Final v2 API
  # (FAIL|UPDATE|RETURN|RETURN_OR_UPDATE — live-verified; the lowercase
  # 'update' I first used 404s on enum parse). RETURN_OR_UPDATE = create
  # if absent, new version if content changed, dedup return if same.
  curl -sfS -X POST \
    -H "Content-Type: application/json" \
    -H "X-Registry-ArtifactType: JSON" \
    -H "X-Registry-ArtifactId: ${art}" \
    --data-binary "@${local_file}" \
    "${B}/groups/${G}/artifacts?ifExists=RETURN_OR_UPDATE" >/dev/null
  curl -sfS -H "Accept: application/json" \
    -o "${TMP}/${art}.registered" \
    "${B}/groups/${G}/artifacts/${art}"
  mounted_sha="$(sha256sum "${local_file}" | cut -d' ' -f1)"
  reg_sha="$(sha256sum "${TMP}/${art}.registered" | cut -d' ' -f1)"
  [ "${mounted_sha}" = "${pinned}" ] || { echo "mounted bytes drift for ${art}: ${mounted_sha} != ${pinned}" >&2; exit 1; }
  [ "${reg_sha}" = "${pinned}" ] || { echo "registered content drift for ${art}: ${reg_sha} != ${pinned}" >&2; exit 1; }
  echo "registered+verified ${art} ${pinned}"
}

reg_and_verify pipeline.schedule.triggered.v1 "${TRIGGER_SHA256:?}"
reg_and_verify pipeline.schedule.outcome.v1 "${OUTCOME_SHA256:?}"
echo done
