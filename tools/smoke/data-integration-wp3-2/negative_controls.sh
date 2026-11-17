#!/usr/bin/env bash
# Bounded negative controls for the WP3.2 runner (root review round 3):
# every control must fail for the TARGETED reason, never "any error".
#   C1 failing command under run_step     -> abort, with the exact step
#      recorded in the child's exit-codes AND "RUNNER FAILED at step"
#   C2 failing tee under run_step         -> abort with tee_rc:1 record
#   C3 expected-fail cmd that exits 0     -> abort with the specific
#      "expected non-zero command exit" message
#   C3b expected-fail cmd non-zero        -> passes AND log kept
#   C4 forged event_id                    -> the FULL positive fixture
#      must FIRST pass basechain verbatim; then ONLY event_id is
#      tampered and the verifier must reject with the event_id
#      mismatch reason.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFIER="${1:?verifier path}"
RESULTS_DIR="${2:?results dir}"
CTRL="$RESULTS_DIR/negctl"
mkdir -p "$CTRL"
PASS=0; FAILED=0

note_fail() { echo "[negctl] $1: FAIL — $2"; FAILED=$((FAILED+1)); }
note_pass() { echo "[negctl] $1: OK ($2)"; PASS=$((PASS+1)); }

PREAMBLE='
set -uo pipefail
HERE="'"$HERE"'"
CTRL="'"$CTRL"'"
EXIT_CODES="$CTRL/child-exit-codes.txt"; : >"$EXIT_CODES"
source "$HERE/runner_lib.sh"
'

# C1: failing command aborts run_step, with the exact step recorded.
bash -c "$PREAMBLE"'
  run_step c1-fail "$CTRL/c1.log" false
  echo SHOULD_NOT_REACH
' >"$CTRL/c1.out" 2>&1
rc=$?
if [[ $rc -ne 0 ]] && grep -q "RUNNER FAILED at step 'c1-fail'" "$CTRL/c1.out" \
   && grep -q "^c1-fail = cmd_rc:1 tee_rc:0$" "$CTRL/child-exit-codes.txt" \
   && ! grep -q "SHOULD_NOT_REACH" "$CTRL/c1.out"; then
  note_pass "C1 failing cmd aborts run_step" "rc=$rc, step recorded"
else
  note_fail "C1 failing cmd aborts run_step" "rc=$rc (want abort with targeted record)"
fi

# C2: failing tee aborts run_step even though the command exits 0.
bash -c "$PREAMBLE"'
  mkdir -p "$CTRL/c2dir" && chmod 500 "$CTRL/c2dir"
  run_step c2-failtee "$CTRL/c2dir/nope.log" true
  chmod 700 "$CTRL/c2dir"
' >"$CTRL/c2.out" 2>&1
rc=$?
chmod 700 "$CTRL/c2dir" 2>/dev/null || true
if [[ $rc -ne 0 ]] && grep -q "RUNNER FAILED at step 'c2-failtee'" "$CTRL/c2.out" \
   && grep -q "^c2-failtee = cmd_rc:0 tee_rc:1$" "$CTRL/child-exit-codes.txt"; then
  note_pass "C2 failing tee aborts run_step" "rc=$rc, tee_rc:1 recorded"
else
  note_fail "C2 failing tee aborts run_step" "rc=$rc (want abort with tee_rc:1 record)"
fi

# C3: expect_fail_step whose command exits 0 aborts with the specific
# message (an "any error" pass is not accepted).
bash -c "$PREAMBLE"'
  expect_fail_step c3-unexpected-zero "$CTRL/c3.log" true
' >"$CTRL/c3.out" 2>&1
rc=$?
if [[ $rc -ne 0 ]] && { grep -q "expected non-zero command exit, got 0" "$CTRL/c3.log" || grep -q "expected non-zero command exit, got 0" "$CTRL/c3.out"; } \
   && grep -q "^c3-unexpected-zero = cmd_rc:0 tee_rc:0$" "$CTRL/child-exit-codes.txt"; then
  note_pass "C3 expected-fail cmd exiting 0 aborts" "rc=$rc, targeted message"
else
  note_fail "C3 expected-fail cmd exiting 0 aborts" "rc=$rc (want targeted abort)"
fi

# C3b: a genuinely failing command passes expect_fail_step, run
# continues, and the command's output is preserved in the log.
bash -c "$PREAMBLE"'
  expect_fail_step c3b-real-fail "$CTRL/c3b.log" sh -c "echo rejected-by-design >&2; exit 3"
' >"$CTRL/c3b.out" 2>&1
rc=$?
if [[ $rc -eq 0 ]] && grep -q "rejected-by-design" "$CTRL/c3b.log" \
   && grep -q "^c3b-real-fail = cmd_rc:3 tee_rc:0$" "$CTRL/child-exit-codes.txt"; then
  note_pass "C3b expected-fail cmd non-zero passes" "rc=0, output preserved"
else
  note_fail "C3b expected-fail cmd non-zero passes" "rc=$rc (want 0 with preserved log)"
fi

# C4: full positive fixture MUST pass basechain verbatim; then ONLY the
# event_id is forged and the verifier must reject citing event_id.
python3 - "$CTRL" <<'PY'
import datetime as dt, importlib.util, json, os, sys, uuid

ctrl = sys.argv[1]
NS = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
TENANT = "11111111-bbbb-4111-8111-000000000002"
GRANT = "11111111-aaaa-4111-8111-000000000001"
RID = "ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111"
GOOD_DEF = "22222222-aaaa-4111-8111-000000000011"
BAD_DEF = "22222222-bbbb-4111-8111-000000000012"
LEGACY_DEF = "22222222-cccc-4111-8111-000000000013"
KEY = "nightly-build"

def envelope(scheduled_for):
    payload = {
        "tenant_kind": "org", "tenant_id": TENANT, "schedule_key": KEY,
        "definition_revision": 1, "scheduled_for": scheduled_for,
        "pipeline_rid": RID, "build_branch": "main",
        "grant_ref": GRANT, "grant_generation": 1,
    }
    d = dt.datetime.fromisoformat(scheduled_for.replace("Z", "+00:00"))
    nanos = d.microsecond * 1000
    fmt = d.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"
    agg = f"org:{TENANT}/{KEY}"
    version = f"1@{fmt}"
    eid = uuid.uuid5(NS, "\x1f".join(["pipeline_schedule", agg, version, "pipeline.schedule.triggered.v1"]))
    return {
        "event_id": str(eid), "event_type": "pipeline.schedule.triggered.v1",
        "aggregate_id": agg, "aggregate_type": "pipeline_schedule",
        "occurred_at": scheduled_for, "correlation_id": f"schedule:{GOOD_DEF}",
        "causation_id": "tick:00000000-0000-4000-8000-000000000001",
        "payload": payload,
    }

slots = ["2026-11-17T06:00:00Z", "2026-11-17T06:01:00Z"]
envs = [envelope(s) for s in slots]
import hashlib, importlib.util
def fake_sha(path):
    return hashlib.sha256(("ofwp32-fixture:" + path).encode()).hexdigest()
# REQUIRED set imported FROM the verifier itself — the fixture can
# never drift from what basechain demands.
spec = importlib.util.spec_from_file_location(
    "verify_results", os.path.join(os.path.dirname(os.path.abspath(
        "tools/smoke/data-integration-wp3-2/verify_results.py")),
        "verify_results.py"))
vr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vr)
REQUIRED = vr.REQUIRED_SOURCE_PATHS
sha_lines = "".join(f"{fake_sha(p)}  {p}\n" for p in sorted(REQUIRED))

files = {
    "versions.txt": "docker_server=27.0\nkubectl_client=v1.36.1\nkubectl_server=v1.36.4\nkind kind version 0.33.0\nimage x = sha256:aa\n",
    "source-sha-pre.txt": sha_lines,
    "source-sha-post.txt": sha_lines,
    "seed-time.txt": "2026-11-17T05:59:30Z\n",
    "tick-jobs.json": json.dumps({"items": [
        {"metadata": {"name": f"ofwp32-schedules-tick-{i}",
                      "creationTimestamp": "2026-11-17T06:00:%02dZ" % (30 + i),
                      "ownerReferences": [{"kind": "CronJob", "name": "ofwp32-schedules-tick"}]},
         "status": {"conditions": [{"type": "Complete", "status": "True"}]}}
        for i in range(2)
    ]}),
    "kafka-offsets-pipeline-builds.txt": "pipeline.builds.v1:0:2\n",
    "kafka-offsets-dlq.txt": "__dlq.ofwp32.outbox-pipeline:0:0\n",
    "kafka-pipeline-builds.ndjson": "".join(json.dumps(e) + "\n" for e in envs),
    "db-state.json": json.dumps({
        "audit": [{"event_id": e["event_id"], "definition_id": GOOD_DEF,
                   "tenant_kind": "org", "tenant_id": TENANT, "schedule_key": KEY,
                   "definition_revision": 1, "scheduled_for": s,
                   "fired_at": "2026-11-17T06:00:31Z"} for e, s in zip(envs, slots)],
        "defs": [
            {"id": GOOD_DEF, "name": "wp32-good", "enabled": True,
             "last_run_at": "2026-11-17T06:01:05Z", "quarantined_at": None},
            {"id": BAD_DEF, "name": "wp32-badcron", "enabled": False,
             "quarantined_at": "2026-11-17T06:00:31Z",
             "quarantine_reason": "cron recompute failed: junk"},
            {"id": LEGACY_DEF, "name": "wp32-legacy", "enabled": True, "last_run_at": None},
        ],
        "outbox_rows": 0,
    }),
    "node-images.json": json.dumps({"images": [{"id": "sha256:" + "c" * 64,
        "repoDigests": ["fixture@sha256:" + "d" * 64]}]}),
    "image-chain.json": json.dumps({
        "docker_id": ("sha256:" + "c" * 64), "node_image_id": ("sha256:" + "c" * 64),
        "pods": {f"ofwp32-schedules-tick-{i}": {"imageID": ("fixture@sha256:" + "d" * 64),
                 "image": "ofwp32/schedules-tick:acc", "phase": "Succeeded"}
                 for i in range(2)}}),
    "negctl-cdc-gate.json": json.dumps({"exit_code": 1, "terminated_reason": "Error",
                                        "cdc_not_ready_in_log": True, "slot_in_log": True}),
    "tick-log-ofwp32-schedules-tick-28572139.txt": 'time=... level=INFO msg="schedules-tick completed" mode=pipeline_outbox_v1 fired=1\n',
}
json.dump({"files": files}, open(os.path.join(ctrl, "positive-fixture.json"), "w"))

# Tampered copy: ONLY the first event_id is forged (deterministic wrong
# value — not the derived one).
forged = dict(files)
bad_envs = [dict(envs[0]), envs[1]]
bad_envs[0]["event_id"] = str(uuid.uuid5(NS, "forged-not-derived"))
forged["kafka-pipeline-builds.ndjson"] = "".join(json.dumps(e) + "\n" for e in bad_envs)
json.dump({"files": forged}, open(os.path.join(ctrl, "tampered-evidence.json"), "w"))
print("fixtures built")
PY

# Positive pre-check: the untampered fixture must PASS basechain.
python3 "$VERIFIER" expect_reject /dev/null basecode_never 2>/dev/null >/dev/null; true
GOOD_DIR="$CTRL/positive"
python3 - "$CTRL/positive-fixture.json" "$GOOD_DIR" <<'PY'
import json, os, sys
os.makedirs(sys.argv[2], exist_ok=True)
for name, content in json.load(open(sys.argv[1]))["files"].items():
    open(os.path.join(sys.argv[2], name), "w").write(content)
PY
if python3 "$VERIFIER" basechain "$GOOD_DIR" pos >/dev/null 2>&1; then
  note_pass "C4-positive full fixture passes basechain" "verifier accepted the canonical shape"
else
  python3 "$VERIFIER" basechain "$GOOD_DIR" pos 2>&1 | head -3 | sed 's/^/    /'
  note_fail "C4-positive full fixture passes basechain" "fixture invalid — control cannot certify the verifier"
fi

# C5: delete exactly ONE critical input line from BOTH pre and post
# (pre==post stays true, so the targeted failure is the missing-input
# check, not the equality check).
python3 - "$CTRL" <<'PYC5'
import json, os, sys
ctrl = sys.argv[1]
files = json.load(open(os.path.join(ctrl, "positive-fixture.json")))["files"]
victim = "libs/event-scheduler/outbox_scheduler.go"
def drop_line(text, path):
    return "".join(l + "\n" for l in text.splitlines() if not l.endswith(" " + path))
for key in ("source-sha-pre.txt", "source-sha-post.txt"):
    files[key] = drop_line(files[key], victim)
assert victim not in files["source-sha-pre.txt"]
json.dump({"files": files}, open(os.path.join(ctrl, "deleted-input-fixture.json"), "w"))
PYC5

# Tampered: ONLY event_id changed; rejection must cite event_id + the
# derived value (targeted reason, not any error).
OUT=$(python3 "$VERIFIER" expect_reject "$CTRL/tampered-evidence.json" basechain 2>&1)
rc=$?
if [[ $rc -eq 0 ]] && echo "$OUT" | grep -q "event_id .* != derived" ; then
  note_pass "C4 forged event_id rejected" "$(echo "$OUT" | head -1 | cut -c1-120)"
else
  note_fail "C4 forged event_id rejected" "rc=$rc; rejection detail: $(echo "$OUT" | head -1)"
fi

OUT5=$(python3 "$VERIFIER" expect_reject "$CTRL/deleted-input-fixture.json" basechain 2>&1)
rc=$?
if [[ $rc -eq 0 ]] && echo "$OUT5" | grep -q "missing required input"; then
  note_pass "C5 deleted critical input rejected" "$(echo "$OUT5" | head -1 | cut -c1-120)"
else
  note_fail "C5 deleted critical input rejected" "rc=$rc; detail: $(echo "$OUT5" | head -1)"
fi

# C6: strict status parser (root review round 5) — the old grep -c
# counted lines on a one-line JSON body and could never distinguish
# RUNNING-with-tasks from anything else. Positive: connector+task both
# RUNNING on ONE compact line. Negatives: task FAILED, empty tasks.
SC="$HERE/status_check.py"
mk_case() { printf '%s' "$2" > "$CTRL/c6-$1.json"; }
mk_case ok '{"name":"x","connector":{"state":"RUNNING","worker_id":"w"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"w"}],"type":"source"}'
mk_case taskfailed '{"name":"x","connector":{"state":"RUNNING","worker_id":"w"},"tasks":[{"id":0,"state":"FAILED","worker_id":"w"}],"type":"source"}'
mk_case emptytasks '{"name":"x","connector":{"state":"RUNNING","worker_id":"w"},"tasks":[],"type":"source"}'
if python3 "$SC" "$CTRL/c6-ok.json" >/dev/null 2>&1; then
  note_pass "C6 parser accepts single-line dual RUNNING" "compact JSON, both states RUNNING"
else
  note_fail "C6 parser accepts single-line dual RUNNING" "positive case rejected"
fi
for case in taskfailed emptytasks; do
  if python3 "$SC" "$CTRL/c6-$case.json" >/dev/null 2>&1; then
    note_fail "C6 parser rejects $case" "verdict must be NOT-READY"
  else
    note_pass "C6 parser rejects $case" "non-zero as expected"
  fi
done

echo "[negctl] negative controls: pass=$PASS fail=$FAILED"
[[ $FAILED -eq 0 ]] || exit 1
exit 0
