"""Scheduled canonical default Parquet publication on the isolated live stack."""

import base64
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import UUID

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.with_name("data-integration-wp3-13")))
import acceptance as writer

stack = writer.stack
expect = writer.expect
EVIDENCE = writer.EVIDENCE
ORG = str(UUID(os.environ["WP314_ORG_ID"]))
PARQUET = {"output_schema_version": writer.SCHEMA}
NDJSON_MIME = "application/x-ndjson"
TOPIC = "pipeline.builds.v1"


def compose(*args, **kwargs):
    envdir = Path(os.environ["OF33C_ENVDIR"])
    return stack.docker("compose", "--project-directory", str(envdir), "-f", str(envdir / "compose.yaml"),
                        "-f", os.environ["WP314_OVERLAY"], *args, **kwargs).stdout


def pg(sql):
    row, = stack.pg("wp33c_pbs", sql)
    return json.loads(row)


def call(method, base, path, body):
    status, response = stack.http(method, base + path, body)
    if not 200 <= status <= 299:
        raise AssertionError(f"{method} {path} failed with status {status}")
    return response


def poll(label, check, timeout=90):
    expiry = time.monotonic() + timeout
    while True:
        result = check()
        if result:
            return result
        if time.monotonic() >= expiry:
            raise AssertionError(f"timed out waiting for {label}")
        time.sleep(0.35)


def prepare_cdc():
    writer.wait_for_health(stack.DVS)
    writer.wait_for_health(stack.PBS)
    for topic in (TOPIC, "pipeline.schedule.outcomes.v1", "__dlq.ofwp32.outbox-pipeline"):
        compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092",
                "--create", "--if-not-exists", "--topic", topic, "--partitions", "1", "--replication-factor", "1")
    endpoint = "http://localhost:8083"
    client = ("exec", "-T", "connect", "curl", "--max-time", "8", "-fsS")

    def http_ready():
        probe = subprocess_call(*client, endpoint + "/")
        return json.loads(probe.stdout) if probe.returncode == 0 else None

    connect_version = poll("Kafka Connect HTTP", http_ready)
    envfile = Path(os.environ["OF33C_ENVDIR"]) / ".env"
    password, = [line.split("=", 1)[1] for line in envfile.read_text().splitlines()
                 if line.startswith("OF33C_PG_PASSWORD=")]
    conf = json.loads((HERE.with_name("data-integration-wp3-2") / "k8s/60-connector.json").read_text())
    conf.update({"database.hostname": "postgres", "database.user": "openfoundry",
                 "database.password": password, "database.dbname": "wp33c_pbs"})
    config_url = endpoint + "/connectors/wp314-default-outbox"
    # The PUT response echoes the fixture DB password. Keep both request and response in memory.
    result = subprocess_call(*client, "-X", "PUT", "-H", "Content-Type: application/json",
                             "--data-binary", "@-", config_url + "/config", input=json.dumps(conf))
    expect("connector config registered", result.returncode, 0)

    def task_ready():
        probe = subprocess_call(*client, config_url + "/status")
        if probe.returncode != 0:
            return None
        report = json.loads(probe.stdout)
        return report if (report["connector"]["state"] == "RUNNING" and report["tasks"]
                          and all(task["state"] == "RUNNING" for task in report["tasks"])) else None

    connector = poll("running Debezium connector and tasks", task_ready)
    slot = poll("active logical replication slot", lambda: pg(
        "SELECT COALESCE((SELECT row_to_json(s) FROM "
        "(SELECT slot_name, plugin, database, active FROM pg_replication_slots "
        "WHERE slot_name='ofwp32_outbox_pipeline' AND active) s), 'null'::json)"))
    expect("CDC on the scheduling database", (slot["database"], slot["plugin"]), ("wp33c_pbs", "pgoutput"))
    writer.write_json("cdc", {"connect_version": connect_version, "connector": connector, "slot": slot})
    return connect_version


def subprocess_call(*args, **kwargs):
    envdir = Path(os.environ["OF33C_ENVDIR"])
    return subprocess.run(
        ["docker", "compose", "--project-directory", str(envdir), "-f", str(envdir / "compose.yaml"),
         "-f", os.environ["WP314_OVERLAY"], *args], capture_output=True, text=True, check=False, **kwargs,
    )


def schedule(case, source, output, profile):
    source_rid, version, txn = source
    pipeline = stack.create_pipeline(f"wp314d-{case}", source_rid, output)
    spec = stack.publish_spec(pipeline, {
        "transform_type": "dataset_input", "dataset_rid": source_rid,
        "dataset_snapshot": {"branch": "main", "version": version, "transaction_id": txn},
        **profile,
    }, [{"dataset_rid": source_rid}], [output])
    key = f"wp314d-{case}"
    grant = call("POST", stack.PBS, "/api/v1/scheduling/grants",
                 {"schedule_key": key, "pipeline_rid": pipeline, "token": stack.TOKEN})
    definition = call("POST", stack.PBS, "/api/v1/scheduling/definitions",
                      {"schedule_key": key, "pipeline_rid": pipeline, "build_branch": "main",
                       "cron": "0 0 1 1 *", "grant_id": grant["id"], "grant_generation": grant["generation"]})
    expect("real definition enabled", definition["status"], "enabled")
    definition_id = pg("SELECT to_json(id) FROM schedules.definitions "
                       f"WHERE schedule_key='{key}' AND tenant_kind='org' AND tenant_id='{ORG}'")
    latest = max(stack.list_versions(source_rid), key=lambda item: item["version"])
    return {"schedule_key": key, "spec": spec, "pipeline": pipeline, "grant_id": grant["id"],
            "grant_generation": grant["generation"], "definition_id": definition_id,
            "source": {"dataset": source_rid, "version": version, "transaction": txn},
            "source_head_at_trigger_setup": latest, "profile": profile}


def fire(case, setup):
    definition = str(UUID(setup["definition_id"]))
    # Make precisely one real annual opportunity overdue so no wall-clock wait is needed.
    due = pg("WITH changed AS (UPDATE schedules.definitions "
             "SET next_run_at=date_trunc('year', now())-interval '1 year' "
             f"WHERE id='{definition}' AND tenant_id='{ORG}' AND enabled RETURNING next_run_at) "
             "SELECT to_json(next_run_at) FROM changed")
    raw = compose("run", "--rm", "--no-deps", "tick")
    (EVIDENCE / f"{case}-tick.log").write_text(raw)
    fired, = [line.split()[1] for line in raw.splitlines() if line.startswith("fired ")]
    event = str(UUID(fired))
    audit = pg("SELECT row_to_json(a) FROM scheduling.dispatch_audit a " f"WHERE event_id='{event}'")
    expect("single producer audit binds the exact due slot",
           (audit["definition_id"], audit["schedule_key"], audit["tenant_id"], audit["scheduled_for"]),
           (definition, setup["schedule_key"], ORG, due))
    advanced = pg(f"SELECT to_json(next_run_at>now()) FROM schedules.definitions WHERE id='{definition}'")
    expect("producer advanced next due into the future", advanced, True)
    return audit


def dispatch(event):
    return pg("SELECT COALESCE((SELECT row_to_json(d) FROM scheduling.dispatches d "
              f"WHERE event_id='{str(UUID(event))}'), 'null'::json)")


def wait_terminal(audit, start, expected):
    def ready():
        row = dispatch(audit["event_id"])
        if row and row["state"] in ("failed", "succeeded", "skipped", "blocked"):
            expect("dispatch outcome", row["state"], expected)
            return row
        return None

    row = poll(f"{start['schedule_key']} dispatch terminal", ready, timeout=120)
    expect("consumer admitted the exact tick opportunity",
           (row["event_id"], row["schedule_key"], row["scheduled_for"], row["definition_revision"], row["tenant_id"]),
           (audit["event_id"], audit["schedule_key"], audit["scheduled_for"], audit["definition_revision"], ORG))
    expect("dispatch bound to the real grant", (row["grant_ref"], row["grant_generation"]),
           (start["grant_id"], start["grant_generation"]))
    expect("no outstanding PBS references", stack.build_refs(row["build_id"]), (0, 0))
    return row


def receipts(event):
    return pg("SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) "
              f"FROM scheduling.publish_receipts r WHERE dispatch_event_id='{str(UUID(event))}'")


def proxy_events():
    path = EVIDENCE / "fault-proxy/events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.is_file() else []


def published(case, rid, old_file, before):
    version = writer.new_version(rid, before)
    txn = version["transaction_id"]
    file, = [f for f in writer.file_list(rid) if f["transaction_id"] == txn]
    index, = [f for f in writer.get(writer.dvs(rid, "files/index")) if f["path"] == file["logical_path"]]
    raw = writer.download(rid, file)
    expect("old file SHA unchanged", sha256(writer.download(rid, old_file)).hexdigest(), before["old_file_sha256"])
    expect("actual signed-download size", (file["size_bytes"], index["size_bytes"]), (len(raw), len(raw)))
    expect("actual signed-download storage URI", index["storage_path"], file["physical_uri"])
    path = EVIDENCE / f"{case}{Path(file['logical_path']).suffix}"
    path.write_bytes(raw)
    return version, file, index, raw, path


def verify_receipt(row, file, raw, typed):
    txn_id = file["transaction_id"]
    receipt, = receipts(row["event_id"])
    expect("PBS receipt final committed", receipt["state"], "committed")
    expect("PBS receipt names exact DVS txn", receipt["receipt"]["transaction"]["id"], txn_id)
    expect("PBS job outputs name committed DVS txn",
           [(writer.id_tail(txn), committed) for txn, committed in stack.job_outputs_of(row["build_id"])],
           [(txn_id, "t")])
    expect("PBS build terminal", stack.build_state(row["build_id"]), ["BUILD_COMPLETED"])
    persisted = bytes.fromhex(receipt["request_body"][2:])
    expect("PBS persisted exact request hash", sha256(persisted).hexdigest(), receipt["request_sha256"])
    intent = json.loads(persisted)
    expect("wire idempotency key is the persisted key", intent["idempotency_key"], receipt["key"])
    expect("wire transaction type", intent["transaction_type"], "SNAPSHOT")
    expect("wire output profile", intent.get("output_schema_version"), writer.SCHEMA if typed else None)
    item, = intent["files"]
    expect("scheduled path sends actual inline ADD", item["operation"], "ADD")
    expect("wire bytes exactly match signed product bytes", base64.b64decode(item["content_base64"], validate=True), raw)
    expect("wire logical path", item["logical_path"], file["logical_path"])
    return {"key": receipt["key"], "request_sha256": receipt["request_sha256"],
            "transaction": txn_id, "request_size": len(persisted)}


def verify_parquet(conn, case, output, old_file, before, row, expected):
    version, file, index, raw, path = published(case, output, old_file, before)
    expect("default filename", file["logical_path"], "part-00000.parquet")
    expect("canonical Parquet media type", index["content_type"], writer.MIME)
    metadata = index["metadata"]
    expect("server-stamped typed file index",
           (metadata["format"], metadata["schema_version"], metadata["record_count"], metadata["sha256"]),
           ("parquet", writer.SCHEMA, len(expected), sha256(raw).hexdigest()))
    txn = writer.get(writer.transaction(output, version["transaction_id"]))
    expect("transaction committed", txn["status"], "COMMITTED")
    expect("server-stamped typed transaction",
           (txn["metadata"]["format"], txn["metadata"]["schema_version"], txn["metadata"]["row_count"]),
           ("parquet", writer.SCHEMA, len(expected)))
    parquet = writer.verify_parquet(conn, path, expected, int(bool(expected)))
    return {"output_dataset": output, "version": version, "parquet": parquet,
            "indexed": index, "receipt": verify_receipt(row, file, raw, typed=True)}


def normal_parquet(conn, case, source, expected):
    output, old_file, before = writer.seed_output(case)
    setup = schedule(case, source, output, PARQUET)
    if case == "default-parquet":
        expect("NDJSON input pin is historical",
               setup["source_head_at_trigger_setup"]["version"] > source[1], True)
    audit = fire(case, setup)
    row = wait_terminal(audit, setup, "succeeded")
    return {**setup, **verify_parquet(conn, case, output, old_file, before, row, expected),
            "producer_audit": audit, "dispatch": row}


def ack_replay(conn, source, expected):
    case = "replayed-parquet"
    output, old_file, before = writer.seed_output(case)
    setup = schedule(case, source, output, PARQUET)
    expect("Parquet input pin is historical", setup["source_head_at_trigger_setup"]["version"] > source[1], True)
    path = f"/v1/datasets/{stack.canon(output)}/outputs:commit"
    call("POST", stack.PROXY_CTL, "/__faults/drop-next", {"method": "POST", "path": path, "hold_replay": True})
    audit = fire(case, setup)

    def lost_ack():
        events = [e for e in proxy_events() if e.get("path") == path]
        dropped = [e for e in events if e["action"] == "ack_dropped"]
        held = [e for e in events if e["action"] == "hold_started"]
        if dropped and held:
            drop, = dropped
            hold, = held
            return drop, hold
        return None

    drop, hold = poll("COMMITTED-only lost ACK and replay held upstream", lost_ack)
    expect("first replay identical as it enters hold",
           (hold["key"], hold["request_sha256"]), (drop["key"], drop["request_sha256"]))
    pending, = receipts(audit["event_id"])
    expect("durable pending or unknown before restart", pending["state"] in ("pending", "unknown"), True)
    original = bytes.fromhex(pending["request_body"][2:])
    expect("durable request hash before restart", sha256(original).hexdigest(), drop["request_sha256"])
    expect("proxy first wire request exactly equals persisted bytes",
           (EVIDENCE / "fault-proxy" / drop["request_file"]).read_bytes(), original)
    committed = writer.new_version(output, before)
    expect("DVS durably published before restart", committed["transaction_id"], drop["committed_transaction_id"])
    pre_restart = dispatch(audit["event_id"])
    expect("PBS has not finalized before restart", pre_restart["state"] != "succeeded", True)
    stack.docker("kill", f"{stack.PROJECT}-pbs")
    call("POST", stack.PROXY_CTL, "/__faults/release", {})

    def old_request_forwarded():
        return next((e for e in proxy_events() if e.get("path") == path and e["action"] == "forwarded"
                     and e["sequence"] > hold["sequence"]), None)

    # Let the released old HTTP handler finish first so a later forward is
    # attributable to the new PBS process rather than the held connection.
    old_forward = poll("released old request drained before restart", old_request_forwarded)
    compose("start", "pbs")
    writer.wait_for_health(stack.PBS)
    row = wait_terminal(audit, setup, "succeeded")
    result = verify_parquet(conn, case, output, old_file, before, row, expected)
    expect("only one DVS version across ACK-loss recovery", result["version"], committed)
    replayed = [e for e in proxy_events() if e.get("path") == path and e["action"] == "forwarded"
                and e["sequence"] > old_forward["sequence"]]
    expect("restarted PBS really sent another commit", bool(replayed), True)
    for event in replayed:
        expect("same replay key, original bytes and DVS transaction",
               (event["key"], event["request_sha256"], event["committed_transaction_id"]),
               (drop["key"], drop["request_sha256"], drop["committed_transaction_id"]))
        expect("actual new connection request bytes are identical",
               (EVIDENCE / "fault-proxy" / event["request_file"]).read_bytes(), original)
    after, = receipts(audit["event_id"])
    expect("original persistent bytes unchanged", after["request_body"], pending["request_body"])
    expect("new PBS instance recovered the durable receipt", row["fence_token"] > pre_restart["fence_token"], True)
    receipt_sha, err = stack.receipt_raw_sha(output, after["key"])
    expect("real DVS receipt accessible by original key", err, None)
    return {**setup, **result, "producer_audit": audit, "dispatch": row, "ack_drop": drop,
            "old_handler_drained": old_forward, "restarted_replay": replayed, "dvs_receipt_sha256": receipt_sha}


def legacy(case, source, expected, profile, logical):
    output, old_file, before = writer.seed_output(case)
    setup = schedule(case, source, output, profile)
    audit = fire(case, setup)
    row = wait_terminal(audit, setup, "succeeded")
    version, file, index, raw, artifact = published(case, output, old_file, before)
    expect("NDJSON output logical path", file["logical_path"], logical)
    expect("NDJSON MIME", index["content_type"], NDJSON_MIME)
    expect("exact NDJSON source objects", [json.loads(line) for line in raw.splitlines()], expected)
    return {**setup, "producer_audit": audit, "dispatch": row, "output_dataset": output, "version": version,
            "artifact": artifact.name, "sha256": sha256(raw).hexdigest(),
            "receipt": verify_receipt(row, file, raw, typed=False)}


def invalid_row(source):
    case = "invalid-row"
    output, old_file, before = writer.seed_output(case)
    setup = schedule(case, source, output, PARQUET)
    audit = fire(case, setup)
    row = wait_terminal(audit, setup, "failed")
    detail = row.get("error") or ""
    expect("typed Parquet encoder rejected invalid decimal",
           "dataset_output_parquet_invalid" in detail and "DECIMAL(18,4) amount" in detail, True)
    expect("no durable publish intent after encode failure", receipts(audit["event_id"]), [])
    expect("invalid source preserved prior output exactly", writer.output_state(output, old_file), before)
    commits = [e for e in proxy_events() if e.get("path") ==
               f"/v1/datasets/{stack.canon(output)}/outputs:commit" and e["method"] == "POST"]
    expect("invalid rows never attempted DVS commit", commits, [])
    return {**setup, "producer_audit": audit, "dispatch": row, "output_dataset": output,
            "prior_head": before["head"], "prior_content_sha256": before["old_file_sha256"]}


def direct_rejection(valid):
    case = "dvs-rejection"
    output, old_file, before = writer.seed_output(case)
    endpoint = writer.dvs(output, "outputs:commit")
    results = {}
    options = {
        "corrupt-footer": (valid[:-4] + b"nope", writer.MIME, writer.SCHEMA),
        "wrong-mime": (valid, NDJSON_MIME, writer.SCHEMA),
        "wrong-profile": (valid, writer.MIME, "orders.v2"),
    }
    for name, (content, mime, profile) in options.items():
        key = f"wp314d-{name}"
        status, problem = stack.http("POST", endpoint, {
            "idempotency_key": key, "branch": "main", "transaction_type": "SNAPSHOT",
            "output_schema_version": profile, "files": [{
                "operation": "ADD", "logical_path": "part-00000.parquet",
                "size_bytes": len(content), "content_type": mime,
                "content_base64": base64.b64encode(content).decode(),
            }],
        }, allow=(400,))
        expect(f"{name} validation rejected request", status, 400)
        expect(f"{name} left no commit receipt", stack.receipt_get(output, key)[0], 404)
        expect(f"{name} preserved head, index and actual file", writer.output_state(output, old_file), before)
        results[name] = {"status": status, "error": problem["error"]}
    return {"output_dataset": output, "prior_head": before["head"], "prior_content_sha256": before["old_file_sha256"],
            "negative_probes": results}


def runtime(conn, connect_version):
    services = {}
    for service in ("postgres", "dvs", "pbs", "kafka", "connect", "registry", "faultproxy"):
        container = compose("ps", "-q", service).strip()
        value = stack.docker("inspect", "--format", "{{.Id}}|{{.Image}}|{{.Config.Image}}|{{.State.StartedAt}}", container)
        services[service] = dict(zip(("id", "image_id", "image", "started_at"),
                                     value.stdout.strip().split("|"), strict=True))
    hashes = {service: compose("exec", "-T", service, "sha256sum", "/service").split()[0]
              for service in ("dvs", "pbs")}
    hashes["tick"] = compose("run", "--rm", "--no-deps", "--entrypoint", "sha256sum", "tick", "/tick").split()[0]
    version, source_id, codename = conn.execute("PRAGMA version").fetchone()
    return {"services": services, "binary_sha256": hashes, "postgres": stack.pg("wp33c_pbs", "SHOW server_version")[0],
            "kafka": compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--version").strip(),
            "connect": connect_version, "docker": stack.docker("version", "--format", "{{.Server.Version}}").stdout.strip(),
            "reader": {"name": "DuckDB", "version": version, "source_id": source_id, "codename": codename},
            "trigger_source": "schedules-tick -> PostgreSQL WAL -> Debezium -> Kafka",
            "test_controls": ["one overdue annual opportunity per HTTP-created schedule",
                              "committed-only ACK loss and replay hold through WP3.2 HTTP proxy"]}


def confirm_kafka(results):
    desired = [result["producer_audit"] for result in results if "producer_audit" in result]
    raw = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092",
                  "--topic", TOPIC, "--partition", "0", "--offset", "earliest",
                  "--max-messages", str(len(desired)), "--timeout-ms", "10000")
    events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    expect("actual FIFO Kafka events equal tick audit identities",
           [event["event_id"] for event in events], [audit["event_id"] for audit in desired])
    writer.write_json("kafka-events", events)


def save(case, result):
    passed = {"status": "passed", **result}
    writer.write_json(case, passed)
    print(f"PASS {case}", flush=True)
    return passed


def main():
    connect_version = prepare_cdc()
    orders = json.loads((writer.WP312 / "orders.json").read_text())["orders"]
    expected = writer.reference.EXPECTED_ORDERS
    ndjson = writer.seed_input("wp314d-orders", [("orders.ndjson", orders)])
    # The input pin must read historical bytes even though a different head exists.
    stack.inline_commit(ndjson[0], "wp314d-advance-input", [
        ("orders.ndjson", (json.dumps({**orders[0], "order_id": -123456}) + "\n").encode()),
    ], "Advance pinned source head")
    invalid = writer.seed_input("wp314d-invalid", [("orders.ndjson", [{**orders[0], "amount": "1.00001"}])])
    empty = writer.seed_input("wp314d-empty", [("orders.ndjson", [])])

    with duckdb.connect() as conn:
        writer.write_json("runtime-before", runtime(conn, connect_version))
        default = save("default-parquet", normal_parquet(conn, "default-parquet", ndjson, expected))
        pin = (default["output_dataset"], default["version"]["version"], default["version"]["transaction_id"])
        # Point the scheduled replay fixture at the now-historical typed Parquet version.
        stack.inline_commit(pin[0], "wp314d-advance-parquet-input", [("new-head.ndjson", b'{"new_head":true}\n')],
                            "Advance Parquet source head")
        replay = save("replayed-parquet", ack_replay(conn, pin, expected))
        empty_result = save("empty-parquet", normal_parquet(conn, "empty-parquet", empty, []))
        explicit = save("explicit-ndjson", legacy("explicit-ndjson", ndjson, orders,
                        {**PARQUET, "file_format": "NDJSON", "logical_path": "explicit.ndjson"}, "explicit.ndjson"))
        unprofiled = save("legacy-ndjson", legacy("legacy-ndjson", ndjson, orders, {}, "part-00000.ndjson"))
        invalid_result = save("invalid-row", invalid_row(invalid))
        save("dvs-rejection", direct_rejection((EVIDENCE / default["parquet"]["path"]).read_bytes()))
        confirm_kafka([default, replay, empty_result, explicit, unprofiled, invalid_result])
        writer.write_json("runtime-after", runtime(conn, connect_version))


if __name__ == "__main__":
    main()
