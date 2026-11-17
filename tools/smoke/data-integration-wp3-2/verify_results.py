#!/usr/bin/env python3
"""Strict evidence verifier for the WP3.2 producer-chain runner.

Modes:
  basechain <results_dir> <run_id>
  exitcodes <exit-codes.txt>
  expect_reject <payload-file> <mode> [args...]   (negative controls)

stdlib only. Every check fails closed: a missing artifact is a FAIL,
never a pass. Type checks exclude bool from int (type(x) is int).
"""
import datetime as dt
import json
import os
import sys
import uuid

OID_NAMESPACE = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
AGGREGATE_TYPE = "pipeline_schedule"
EVENT_TYPE_TRIGGERED = "pipeline.schedule.triggered.v1"
CRONJOB_NAME = "ofwp32-schedules-tick"

# Frozen seed identities (run.sh seed-real) — evidence must match them
# exactly; unknown identities are rejected.
SEED_TENANT_ID = "11111111-bbbb-4111-8111-000000000002"
SEED_GRANT_REF = "11111111-aaaa-4111-8111-000000000001"
SEED_PIPELINE_RID = "ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111"
SEED_GOOD_DEF_ID = "22222222-aaaa-4111-8111-000000000011"
SEED_BAD_DEF_ID = "22222222-bbbb-4111-8111-000000000012"
SEED_LEGACY_DEF_ID = "22222222-cccc-4111-8111-000000000013"
SEED_SCHEDULE_KEY = "nightly-build"

REQUIRED_STEPS = [
    "source-sha-pre", "preflight-cluster", "record-versions",
    "docker-build-tick", "export-tick", "load-tick",
    "apply-pg", "wait-pg", "pg-migrations", "apply-kafka",
    "apply-connect-apicurio", "register-schemas", "register-connector",
    "verify-cdc-slot", "apply-cronjob", "seed-real", "wait-auto-cycles",
    "collect-evidence", "image-chain", "negctl-cdc-gate",
    "source-sha-post", "verify-basechain", "negative-controls",
    # NOTE: "exit-codes-all-zero" is deliberately NOT required here —
    # its own record is appended only after this check runs; its rc is
    # enforced by the runner exiting non-zero.
]

# The exact critical input set source_sha_list enumerates for THIS
# round's producer + runner (B-owned; NOT the whole repo). Deleting any
# one of these must fail verification (root independent review).
REQUIRED_SOURCE_PATHS = [
    "libs/event-scheduler/Dockerfile",
    "libs/event-scheduler/cmd/schedules-tick/main.go",
    "libs/event-scheduler/definition.go",
    "libs/event-scheduler/doc.go",
    "libs/event-scheduler/errors.go",
    "libs/event-scheduler/outbox_scheduler.go",
    "libs/event-scheduler/registry.go",
    "libs/event-scheduler/scheduler.go",
    "libs/event-scheduler/migrations/0001_schedules_definitions.sql",
    "libs/event-scheduler/migrations/0002_schedules_outbox_dispatch.sql",
    "libs/schedule-events/events.go",
    "libs/schedule-events/schema/pipeline.schedule.outcome.v1.json",
    "libs/schedule-events/schema/pipeline.schedule.triggered.v1.json",
    "tools/smoke/data-integration-wp3-2/negative_controls.sh",
    "tools/smoke/data-integration-wp3-2/run.sh",
    "tools/smoke/data-integration-wp3-2/runner_lib.sh",
    "tools/smoke/data-integration-wp3-2/verify_results.py",
    "tools/smoke/data-integration-wp3-2/k8s/10-pg.yaml",
    "tools/smoke/data-integration-wp3-2/k8s/20-kafka.yaml",
    "tools/smoke/data-integration-wp3-2/k8s/30-connect.yaml",
    "tools/smoke/data-integration-wp3-2/k8s/40-apicurio.yaml",
    "tools/smoke/data-integration-wp3-2/k8s/50-register.sh",
    "tools/smoke/data-integration-wp3-2/k8s/60-connector.json",
    "tools/smoke/data-integration-wp3-2/status_check.py",
    "tools/smoke/data-integration-wp3-2/k8s/70-cronjob.yaml",
    "infra/helm/infra/debezium/templates/kafka-connector-outbox-pipeline.yaml",
    "infra/helm/infra/debezium/values.yaml",
    "infra/helm/apps/of-data-engine/templates/schedules-tick-cronjob.yaml",
    "infra/helm/apps/of-data-engine/values.yaml",
    "infra/helm/infra/kafka-cluster/values.yaml",
    "infra/helm/infra/kafka-cluster/values-apicurio-2-6.yaml",
    "infra/helm/infra/kafka-cluster/templates/apicurio-schemas-schedule-events-job.yaml",
    "infra/helm/infra/kafka-cluster/files/schemas/pipeline.schedule.triggered.v1.json",
    "infra/helm/infra/kafka-cluster/files/schemas/pipeline.schedule.outcome.v1.json",
    "go.mod",
    "go.sum",
]

ENVELOPE_FIELDS = {
    "event_id": str, "event_type": str, "aggregate_id": str,
    "aggregate_type": str, "occurred_at": str, "correlation_id": str,
    "causation_id": str, "payload": dict,
}
TRIGGER_PAYLOAD_FIELDS = {
    "tenant_kind": str, "tenant_id": str, "schedule_key": str,
    "definition_revision": int, "scheduled_for": str, "pipeline_rid": str,
    "build_branch": str, "grant_ref": str, "grant_generation": int,
}


def fail(msg: str) -> None:
    print(f"verify: FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def is_int(x) -> bool:
    return type(x) is int


def read_file(results_dir: str, name: str) -> str:
    path = os.path.join(results_dir, name)
    if not os.path.isfile(path):
        fail(f"{name} missing — absence is never evidence")
    return open(path).read()


def fmt_scheduled_for(ts: str) -> str:
    t = ts.replace("Z", "+00:00")
    d = dt.datetime.fromisoformat(t).astimezone(dt.timezone.utc)
    nanos = d.microsecond * 1000
    return d.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def derive_event_id(payload: dict) -> uuid.UUID:
    aggregate_id = f"{payload['tenant_kind']}:{payload['tenant_id']}/{payload['schedule_key']}"
    version = f"{payload['definition_revision']}@{fmt_scheduled_for(payload['scheduled_for'])}"
    joined = "\x1f".join([AGGREGATE_TYPE, aggregate_id, version, EVENT_TYPE_TRIGGERED])
    return uuid.uuid5(OID_NAMESPACE, joined)


def validate_trigger_message(raw_line: str, where: str) -> dict:
    try:
        env = json.loads(raw_line)
    except json.JSONDecodeError as e:
        fail(f"{where}: not valid JSON: {e}")
    for field, typ in ENVELOPE_FIELDS.items():
        if field not in env:
            fail(f"{where}: envelope missing '{field}'")
        if typ is int:
            if not is_int(env[field]):
                fail(f"{where}: envelope '{field}' must be int (not bool/float)")
        elif not isinstance(env[field], typ):
            fail(f"{where}: envelope '{field}' wrong type")
    if env["event_type"] != EVENT_TYPE_TRIGGERED:
        fail(f"{where}: event_type {env['event_type']!r}")
    if env["aggregate_type"] != AGGREGATE_TYPE:
        fail(f"{where}: aggregate_type {env['aggregate_type']!r}")
    payload = env["payload"]
    for field, typ in TRIGGER_PAYLOAD_FIELDS.items():
        if field not in payload:
            fail(f"{where}: payload missing '{field}'")
        if typ is int:
            if not is_int(payload[field]):
                fail(f"{where}: payload '{field}' must be int (not bool/float)")
        elif not isinstance(payload[field], typ):
            fail(f"{where}: payload '{field}' wrong type")
    # Known seed identities only.
    if payload["tenant_kind"] != "org" or payload["tenant_id"] != SEED_TENANT_ID:
        fail(f"{where}: unknown tenant identity {payload['tenant_kind']}:{payload['tenant_id']}")
    if payload["schedule_key"] != SEED_SCHEDULE_KEY:
        fail(f"{where}: unknown schedule_key {payload['schedule_key']!r}")
    if payload["grant_ref"] != SEED_GRANT_REF:
        fail(f"{where}: unknown grant_ref {payload['grant_ref']!r}")
    if payload["pipeline_rid"] != SEED_PIPELINE_RID:
        fail(f"{where}: unknown pipeline_rid {payload['pipeline_rid']!r}")
    if payload["definition_revision"] < 1 or payload["grant_generation"] < 1:
        fail(f"{where}: revision/generation below 1")
    expected = derive_event_id(payload)
    if env["event_id"] != str(expected):
        fail(f"{where}: event_id {env['event_id']} != derived {expected} (forged or mismatched tuple)")
    expected_agg = f"{payload['tenant_kind']}:{payload['tenant_id']}/{payload['schedule_key']}"
    if env["aggregate_id"] != expected_agg:
        fail(f"{where}: aggregate_id {env['aggregate_id']!r} != {expected_agg!r}")
    return env


def parse_offsets(text: str, name: str) -> dict:
    """kafka-get-offsets output: 'topic:partition:offset' per line."""
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) != 3:
            fail(f"{name}: unparseable offset line {line!r}")
        topic, partition, offset = parts
        if not partition.isdigit() or not offset.isdigit():
            fail(f"{name}: non-numeric partition/offset in {line!r}")
        out[(topic, int(partition))] = int(offset)
    if not out:
        fail(f"{name}: no offsets recorded (topic missing?)")
    return out


def mode_basechain(results_dir: str, run_id: str) -> None:
    if not run_id or "/" in run_id:
        fail("run_id must be a plain token")

    # 1. versions.
    vtext = read_file(results_dir, "versions.txt")
    for needle in ("docker_server=", "kubectl_server=", "kind ", "image "):
        if needle not in vtext:
            fail(f"versions.txt missing '{needle}' line")
    if "v1.36.4" not in vtext:
        fail("versions.txt does not record the pinned server v1.36.4")

    # 2. source pins: every line a real shasum record (64-hex + path),
    #    every required input present, pre == post byte-exact.
    pre = read_file(results_dir, "source-sha-pre.txt")
    post = read_file(results_dir, "source-sha-post.txt")
    if not pre.strip():
        fail("source-sha-pre.txt empty")
    if pre != post:
        fail("source files changed between pre and post — evidence not reproducible")
    import re as _re
    paths = set()
    for line in pre.splitlines():
        line = line.strip()
        if not line:
            continue
        m = _re.match(r"^([0-9a-f]{64})[ \t]+(\S+)$", line)
        if not m:
            fail(f"source-sha line not in shasum format (64-hex + path): {line!r}")
        paths.add(m.group(2))
    for required in REQUIRED_SOURCE_PATHS:
        if required not in paths:
            fail(f"source-sha missing required input {required}")

    # 3. seed time + automatic cycles from the LOCKED CronJob.
    seed_ts = read_file(results_dir, "seed-time.txt").strip()
    if not seed_ts:
        fail("seed-time.txt empty")
    jobs = json.loads(read_file(results_dir, "tick-jobs.json"))
    completed_post_seed = 0
    for j in jobs.get("items", []):
        meta = j.get("metadata", {})
        owners = meta.get("ownerReferences", [])
        if not owners or owners[0].get("kind") != "CronJob" or owners[0].get("name") != CRONJOB_NAME:
            continue
        if meta.get("creationTimestamp", "") <= seed_ts:
            continue
        if any(c.get("type") == "Complete" and c.get("status") == "True"
               for c in j.get("status", {}).get("conditions", [])):
            completed_post_seed += 1
    if completed_post_seed < 2:
        fail(f"need >=2 completed AUTOMATIC cycles from CronJob {CRONJOB_NAME} created after seed {seed_ts}, got {completed_post_seed}")

    # 4. Kafka offsets: explicit topic/partition evidence; DLQ must be
    #    PROVEN empty (all end-offsets zero), not assumed.
    trig_offsets = parse_offsets(read_file(results_dir, "kafka-offsets-pipeline-builds.txt"),
                                 "kafka-offsets-pipeline-builds.txt")
    if not any(t == "pipeline.builds.v1" for (t, _p) in trig_offsets):
        fail("no pipeline.builds.v1 partition in offsets evidence")
    total_messages = sum(v for (t, _p), v in trig_offsets.items() if t == "pipeline.builds.v1")
    dlq_offsets = parse_offsets(read_file(results_dir, "kafka-offsets-dlq.txt"),
                                "kafka-offsets-dlq.txt")
    for (t, p), v in dlq_offsets.items():
        if t != "__dlq.ofwp32.outbox-pipeline":
            fail(f"dlq offsets contain foreign topic {t}")
        if v != 0:
            fail(f"connector DLQ partition {p} has {v} messages — must be zero")

    # 5. Messages: deterministic count + strict envelopes + >= 2
    #    DISTINCT scheduled_for slots.
    raw = read_file(results_dir, "kafka-pipeline-builds.ndjson")
    lines = [l for l in raw.splitlines() if l.strip()]
    if len(lines) != total_messages:
        fail(f"consumed {len(lines)} messages != end-offset total {total_messages}")
    if total_messages < 2:
        fail(f"need >=2 trigger messages, got {total_messages}")
    seen_ids, slots = set(), set()
    for i, line in enumerate(lines):
        env = validate_trigger_message(line, f"kafka[{i}]")
        if env["event_id"] in seen_ids:
            fail(f"kafka[{i}]: duplicate event_id {env['event_id']}")
        seen_ids.add(env["event_id"])
        slots.add(fmt_scheduled_for(env["payload"]["scheduled_for"]))
    if len(slots) < 2:
        fail(f"need >=2 DISTINCT scheduled_for slots, got {sorted(slots)}")

    # 6. DB state: audit<->kafka 1:1 (event_id AND scheduled_for), seed
    #    identities, quarantine, legacy untouched, outbox empty.
    db = json.loads(read_file(results_dir, "db-state.json"))
    for key in ("audit", "defs", "outbox_rows"):
        if key not in db:
            fail(f"db-state.json missing '{key}'")
    if not is_int(db["outbox_rows"]) or db["outbox_rows"] != 0:
        fail(f"outbox_rows must be int 0, got {db['outbox_rows']!r}")
    audit = db["audit"]
    audit_by_id = {}
    for row in audit:
        eid = row.get("event_id")
        if eid in audit_by_id:
            fail(f"duplicate audit row {eid}")
        audit_by_id[eid] = row
    if set(audit_by_id) != seen_ids:
        fail(f"audit<->kafka mismatch: audit_only={set(audit_by_id)-seen_ids} kafka_only={seen_ids-set(audit_by_id)}")
    kafka_by_id = {}
    for line in lines:
        env = json.loads(line)
        kafka_by_id[env["event_id"]] = env["payload"]
    for eid, row in audit_by_id.items():
        if row.get("tenant_kind") != "org" or row.get("tenant_id") != SEED_TENANT_ID:
            fail(f"audit {eid}: unknown tenant identity")
        if row.get("schedule_key") != SEED_SCHEDULE_KEY:
            fail(f"audit {eid}: unknown schedule_key")
        if row.get("definition_id") != SEED_GOOD_DEF_ID:
            fail(f"audit {eid}: unexpected definition_id {row.get('definition_id')}")
        if not is_int(row.get("definition_revision")) or row["definition_revision"] != 1:
            fail(f"audit {eid}: revision must be int 1")
        # FULL opportunity tuple (root review): audit and Kafka payload
        # must agree on scheduled_for too, not only on event_id.
        payload = kafka_by_id.get(eid)
        if payload is None:
            fail(f"audit {eid}: no matching Kafka event")
        try:
            audit_sf = fmt_scheduled_for(row.get("scheduled_for", ""))
        except Exception as e:
            fail(f"audit {eid}: unparseable scheduled_for {row.get('scheduled_for')!r}: {e}")
        if audit_sf != fmt_scheduled_for(payload["scheduled_for"]):
            fail(f"audit {eid}: scheduled_for {row.get('scheduled_for')!r} != payload {payload['scheduled_for']!r}")
        if payload["definition_revision"] != row["definition_revision"] \
                or payload["tenant_kind"] != row["tenant_kind"] \
                or payload["tenant_id"] != row["tenant_id"] \
                or payload["schedule_key"] != row["schedule_key"]:
            fail(f"audit {eid}: opportunity tuple mismatch beyond event_id")
    defs = {d["name"]: d for d in db["defs"]}
    good = defs.get("wp32-good")
    if good is None:
        fail("wp32-good definition missing")
    if good.get("id") != SEED_GOOD_DEF_ID:
        fail("wp32-good definition id drift")
    if good.get("enabled") is not True:
        fail("wp32-good must stay enabled")
    if not good.get("last_run_at"):
        fail("wp32-good must have fired")
    if good.get("quarantined_at"):
        fail("wp32-good must not be quarantined")
    bad = defs.get("wp32-badcron")
    if bad is None:
        fail("wp32-badcron definition missing")
    if bad.get("id") != SEED_BAD_DEF_ID:
        fail("wp32-badcron definition id drift")
    if bad.get("enabled") is not False or not bad.get("quarantined_at"):
        fail("wp32-badcron must be disabled+quarantined")
    reason = bad.get("quarantine_reason") or ""
    if not reason.startswith("cron"):
        fail(f"wp32-badcron quarantine_reason must start with 'cron', got {reason!r}")
    if len(reason) > 512 or "\n" in reason:
        fail("quarantine_reason violates sanitization bounds")
    legacy = defs.get("wp32-legacy")
    if legacy is None:
        fail("wp32-legacy definition missing")
    if legacy.get("id") != SEED_LEGACY_DEF_ID:
        fail("wp32-legacy definition id drift")
    if legacy.get("last_run_at"):
        fail("wp32-legacy (direct mode) must never be fired by the outbox tick")

    # 7. Image chain (structured): docker ID == node image ID, and
    #    EVERY completed post-seed automatic job's OWN pod ran exactly
    #    that image (one pod is never accepted as representative).
    chain = json.loads(read_file(results_dir, "image-chain.json"))
    docker_id = chain.get("docker_id", "")
    if not _re.fullmatch(r"sha256:[0-9a-f]{64}", str(docker_id)):
        fail(f"image-chain docker_id not an immutable ID: {docker_id!r}")
    if chain.get("node_image_id") != docker_id:
        fail(f"node image id {chain.get('node_image_id')!r} != built docker id {docker_id}")
    pods = chain.get("pods", {})
    # completed post-seed job names re-derived from tick-jobs evidence
    seed_ts2 = read_file(results_dir, "seed-time.txt").strip()
    completed_jobs = []
    for j in jobs.get("items", []):
        meta = j.get("metadata", {})
        owners = meta.get("ownerReferences", [])
        if not owners or owners[0].get("kind") != "CronJob" or owners[0].get("name") != CRONJOB_NAME:
            continue
        if meta.get("creationTimestamp", "") <= seed_ts2:
            continue
        if any(c.get("type") == "Complete" and c.get("status") == "True"
               for c in j.get("status", {}).get("conditions", [])):
            completed_jobs.append(meta["name"])
    for job in completed_jobs:
        entry = pods.get(job)
        if not entry:
            fail(f"image-chain: no pod evidence for completed job {job}")
        # Kubelet reports a repo manifest digest; CRI images.id is the
        # config digest. Link through the SAME structured CRI image entry.
        raw_images = json.loads(read_file(results_dir, "node-images.json"))
        pod_image_id = entry.get("imageID", "")
        pod_digest = pod_image_id.rsplit("@", 1)[-1]
        matches = [image for image in raw_images.get("images", [])
                   if any(d.rsplit("@", 1)[-1] == pod_digest
                          for d in image.get("repoDigests", []) or [])]
        if not matches or any(image.get("id") != docker_id for image in matches):
            fail(f"job {job} pod imageID {pod_image_id!r} has no unique matching built config {docker_id}")
        if entry.get("phase") != "Succeeded":
            fail(f"job {job} pod phase {entry.get('phase')!r} != Succeeded")

    # 8. CDC-gate negative control: persisted JSON with the REAL
    #    terminated exitCode and the fail-closed reason.
    neg = json.loads(read_file(results_dir, "negctl-cdc-gate.json"))
    if not is_int(neg.get("exit_code")) or neg["exit_code"] == 0:
        fail(f"negctl exit_code must be a non-zero int, got {neg.get('exit_code')!r}")
    if neg.get("cdc_not_ready_in_log") is not True:
        fail("negctl log must contain the cdc-not-ready reason")
    if neg.get("slot_in_log") is not True:
        fail("negctl log must name the wrong slot")

    # 9. At least one real tick log shows a completed outbox tick.
    tick_logs = [n for n in os.listdir(results_dir) if n.startswith("tick-log-")]
    if not tick_logs:
        fail("no tick pod logs collected")
    if not any("schedules-tick completed" in open(os.path.join(results_dir, n)).read()
               for n in tick_logs):
        fail("no tick log shows a completed run")

    print(f"verify: basechain OK (auto_cycles={completed_post_seed}, kafka_events={len(lines)}, "
          f"distinct_slots={len(slots)}, audit_rows={len(audit_by_id)}) — base chain ONLY, not V1–V8")


def mode_exitcodes(path: str) -> None:
    text = read_file(os.path.dirname(path) or ".", os.path.basename(path))
    if not text.strip():
        fail("exit-codes file empty")
    seen = set()
    bad = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Garbage line (no record shape) is itself a failure.
        if "=" not in line:
            bad.append(f"garbage line: {line!r}")
            continue
        step, status = line.split("=", 1)
        step = step.strip()
        status = status.strip()
        ok_shape = (step and
                    status.startswith("cmd_rc:") and
                    " tee_rc:" in status and
                    status.replace("cmd_rc:", "").replace(" tee_rc:", "").isdigit())
        if not ok_shape:
            bad.append(f"malformed record: {line!r}")
            continue
        seen.add(step)
        if status != "cmd_rc:0 tee_rc:0":
            bad.append(line)
    for required in REQUIRED_STEPS:
        if required not in seen:
            bad.append(f"missing required step: {required}")
    if bad:
        fail("exit-codes violations:\n  " + "\n  ".join(bad))
    print(f"verify: exit codes all zero, required steps present ({len(REQUIRED_STEPS)})")


def mode_expect_reject(argv: list[str]) -> None:
    if len(argv) < 2:
        print("usage: expect_reject <payload-file> <mode> [args...]", file=sys.stderr)
        sys.exit(2)
    payload_file, mode = argv[0], argv[1]
    import tempfile

    if mode == "basechain":
        tmp = tempfile.mkdtemp(prefix="negctl-")
        data = json.load(open(payload_file))
        for name, content in data.get("files", {}).items():
            with open(os.path.join(tmp, name), "w") as fh:
                fh.write(content)
        code, output = _run_quiet(mode_basechain, [tmp, "negctl"])
    elif mode == "exitcodes":
        code, output = _run_quiet(mode_exitcodes, [payload_file])
    else:
        print(f"expect_reject: unknown mode {mode}", file=sys.stderr)
        sys.exit(2)
    if code == 0:
        print("expect_reject: verifier ACCEPTED bad evidence — control FAILS", file=sys.stderr)
        sys.exit(1)
    # Surface the verifier's rejection detail so controls can assert
    # the SPECIFIC failure, not just any failure.
    print(output.strip())


def _run_quiet(fn, argv):
    import io
    import contextlib

    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            fn(*argv)
        return 0, buf.getvalue()
    except SystemExit as e:
        return int(e.code or 0), buf.getvalue()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    mode = sys.argv[1]
    if mode == "basechain":
        if len(sys.argv) != 4:
            print("usage: basechain <results_dir> <run_id>", file=sys.stderr)
            sys.exit(2)
        mode_basechain(sys.argv[2], sys.argv[3])
    elif mode == "exitcodes":
        mode_exitcodes(sys.argv[2])
    elif mode == "expect_reject":
        mode_expect_reject(sys.argv[2:])
    else:
        print(f"unknown mode {mode}", file=sys.stderr)
        sys.exit(2)
