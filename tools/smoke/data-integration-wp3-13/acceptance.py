"""Exercise the product Parquet publish path and fault handling on real services."""

from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import platform
import sys
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen
from uuid import UUID, uuid4

import duckdb

HERE = Path(__file__).resolve().parent
WP33 = HERE.with_name("data-integration-wp3-3")
WP312 = HERE.with_name("data-integration-wp3-12")
sys.path.extend(map(str, (HERE, WP33, WP312)))
import harness3c as stack
import verify as reference
from provenance import BINARIES, CASES, expect

EVIDENCE = Path(stack.EVIDENCE)
MIME = "application/vnd.apache.parquet"
SCHEMA = reference.SCHEMA_VERSION
LOGICAL = "part-00000.parquet"
OLD_BYTES = b'{"existing":"published before the build"}\n'


def write_json(name, data):
    (EVIDENCE / f"{name}.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def dvs(rid, endpoint):
    return f"{stack.DVS}/v1/datasets/{rid}/{endpoint}"


def get(url):
    status, body = stack.http("GET", url, allow=(200,))
    expect("HTTP GET", status, 200)
    return body


def id_tail(rid):
    return str(UUID(stack.rid_tail(rid)))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def download(rid, file):
    request = Request(dvs(rid, f"files/{file['id']}/download"),
                      headers={"Authorization": f"Bearer {stack.TOKEN}"})
    try:
        with build_opener(NoRedirect()).open(request, timeout=30):
            raise AssertionError("authenticated file route did not issue a signed redirect")
    except HTTPError as response:
        with response:
            expect("file download redirect", response.code, 302)
            location = response.headers["Location"]
    signed = urlsplit(location)
    service = urlsplit(stack.DVS)
    expect("signed download origin", (signed.scheme, signed.netloc), (service.scheme, service.netloc))
    expect("signed local download path", signed.path.startswith("/v1/_internal/local-fs/"), True)
    # The caller bearer is sent to the authorization route only.
    with urlopen(location, timeout=30) as response:
        expect("signed download HTTP status", response.status, 200)
        return response.read()


def backing_files(rid):
    # Read-only filesystem inventory catches abandoned installs invisible to HTTP.
    entries = stack.docker("exec", f"{stack.PROJECT}-dvs", "find", "/var/of/backing",
                           "-type", "f", "-path", f"*/datasets/{id_tail(rid)}/*").stdout
    return sorted(entries.splitlines())


def stage_rows(reservation):
    return stack.pg("wp33c_dvs", "SELECT status FROM dataset_staged_output_objects "
                                f"WHERE reservation_id='{id_tail(reservation)}'")


def stage_files(reservation):
    return stack.docker("exec", f"{stack.PROJECT}-dvs", "find", "/var/of/staging",
                        "-type", "f", "-path", f"*/{id_tail(reservation)}/*").stdout.splitlines()


def transaction(rid, txn):
    return dvs(rid, f"branches/main/transactions/{id_tail(txn)}")


def no_stage_residue(reservation):
    # The proxy resets PBS before DVS has necessarily observed EOF.
    deadline = time.monotonic() + 10
    while True:
        rows, files = stage_rows(reservation), stage_files(reservation)
        if not rows and not files:
            return {"descriptor_states": rows, "private_staging_files": files}
        if time.monotonic() >= deadline:
            raise AssertionError(f"reservation {reservation} leaked staging state: {rows}, {files}")
        time.sleep(0.2)


def file_list(rid):
    return get(dvs(rid, "files?branch=main"))["files"]


def output_state(rid, old_file):
    return {
        "head": get(dvs(rid, "branches/main"))["head_transaction_id"],
        "versions": stack.list_versions(rid),
        "files": file_list(rid),
        "file_index": get(dvs(rid, "files/index")),
        "backing_files": backing_files(rid),
        "old_file_sha256": sha256(download(rid, old_file)).hexdigest(),
    }


def seed_output(case):
    rid = stack.create_dataset(f"wp313-output-{case}")
    stack.ensure_main_branch(rid)
    stack.inline_commit(rid, f"wp313-seed-{case}", [("old-head.ndjson", OLD_BYTES)], "WP3.13 old head")
    old_file, = file_list(rid)
    before = output_state(rid, old_file)
    old_version, = before["versions"]
    expect("seed version binds to head", old_version["transaction_id"], before["head"])
    expect("seed file readback", before["old_file_sha256"], sha256(OLD_BYTES).hexdigest())
    expect("one real seed backing file", len(before["backing_files"]), 1)
    return rid, old_file, before


def seed_input(tag, files):
    rid = stack.create_dataset(f"wp313-input-{tag}")
    stack.ensure_main_branch(rid)
    ndjson = [(path, b"".join(json.dumps(row, separators=(",", ":")).encode() + b"\n" for row in rows))
              for path, rows in files]
    result = stack.inline_commit(rid, f"wp313-input-{tag}", ndjson, "WP3.13 pinned input")
    txn = result["transaction"]["id"]
    return rid, stack.version_by_txn(rid, txn), txn


def setup_build(case, source, output, predicate=None):
    rid, version, txn = source
    pipeline = stack.create_pipeline(f"wp313-{case}", rid, output)
    input_spec = {"version": version, "branch": "main", "all_files": True}
    if predicate is not None:
        input_spec["predicate"] = predicate
    spec = stack.publish_spec(pipeline, {
        "transform_type": "batched_dataset_copy",
        "input": input_spec,
        "output": {"logical_path": LOGICAL, "format": "parquet", "schema_version": SCHEMA},
    }, [{"dataset_rid": stack.canon(rid)}], [stack.canon(output)])
    build, _ = stack.create_build(pipeline, output)
    reservation, = stack.pg("wp33c_pbs", "SELECT transaction_rid FROM build_pending_output_txns "
                                        f"WHERE build_id='{id_tail(build)}'")
    expect("open DVS reservation", get(transaction(output, reservation))["status"], "OPEN")
    expect("references held before execute", stack.build_refs(build), (1, 1))
    pinned = stack.pg("wp33c_pbs", "SELECT input_dataset_rid, input_branch, input_version, logical_path "
                                 f"FROM build_batched_configs WHERE build_id='{id_tail(build)}'")
    expect("frozen source pin and Parquet path", pinned, [f"{stack.canon(rid)}|main|{version}|{LOGICAL}"])
    return {"build": build, "reservation": id_tail(reservation), "job_spec": spec,
            "source": {"dataset": rid, "version": version, "transaction": txn}}


def verify_parquet(conn, path, expected, row_groups):
    def query(sql):
        return conn.execute(sql, [str(path)]).fetchall()

    schema = [(name, kind) for name, kind, *_ in
              query("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)")]
    expect("SQL types", schema, reference.SQL_SCHEMA)
    physical_schema = query("""
        SELECT name, type, type_length, repetition_type, converted_type, precision, scale, logical_type
        FROM parquet_schema(?) WHERE num_children IS NULL
    """)
    expect("Parquet physical and logical types", physical_schema, reference.PARQUET_SCHEMA)
    footer, = query("SELECT created_by, num_rows, num_row_groups, format_version FROM parquet_file_metadata(?)")
    expect("Parquet footer", footer, ("parquet-go version 18.6.0", len(expected), row_groups, 2))
    metadata = dict(query("SELECT key, value FROM parquet_kv_metadata(?)"))
    expect("embedded schema version", metadata.get(b"openfoundry.schema_version"), SCHEMA.encode())
    expect("embedded Arrow schema present", bool(metadata.get(b"ARROW:schema")), True)
    expect("row-group writer compression and encodings",
           query("SELECT DISTINCT compression, encodings FROM parquet_metadata(?)"),
           [("SNAPPY", "PLAIN, RLE")] if expected else [])
    scanned = query("""
        SELECT order_id, amount, order_date, epoch_us(ordered_at), source_time_zone, note
        FROM read_parquet(?, hive_partitioning = false) ORDER BY order_id
    """)
    rows = [
        (order_id, format(amount, "f"), day.isoformat(),
         (reference.UTC_EPOCH + timedelta(microseconds=micros)).isoformat(timespec="microseconds"),
         zone, note)
        for order_id, amount, day, micros, zone, note in scanned
    ]
    expect("readback row count", len(rows), len(expected))
    for i, (actual, golden) in enumerate(zip(rows, expected, strict=True)):
        expect(f"ordered row {i + 1}", actual, golden)
    return {"path": path.name, "sha256": sha256(path.read_bytes()).hexdigest(),
            "footer": footer, "sql_schema": schema, "physical_schema": physical_schema,
            "schema_version": SCHEMA,
            "exact_orders": [(str(row[0]), *row[1:]) for row in rows if row in reference.EXPECTED_ORDERS]}


def new_version(rid, before):
    versions = stack.list_versions(rid)
    current, = [version for version in versions if version not in before["versions"]]
    expect("exactly one additional committed version", len(versions), len(before["versions"]) + 1)
    expect("previous version retained", all(v in versions for v in before["versions"]), True)
    expect("next version number", current["version"], before["versions"][0]["version"] + 1)
    expect("new head transaction", get(dvs(rid, "branches/main"))["head_transaction_id"],
           current["transaction_id"])
    return current


def verify_publish(conn, case, build, before, rid, old_file, execute, expected, row_groups, replay=False):
    status, response = execute
    expect("execute status", status, 200)
    expect("execute build identity", response["build_id"], build["build"])
    expect("execute build state", response["state"], "BUILD_COMPLETED")
    detail = response["batched"]
    expect("execute replay flag", detail["replay"], replay)
    expect("execute output", (detail["dataset_rid"], detail["branch"]), (stack.canon(rid), "main"))
    version = new_version(rid, before)
    txn = version["transaction_id"]
    expect("PBS points to version transaction", id_tail(detail["published_transaction_rid"]), txn)
    expect("PBS build completed in Postgres", stack.build_state(build["build"]), ["BUILD_COMPLETED"])
    expect("PBS committed job output", [(id_tail(row[0]), row[1]) for row in stack.job_outputs_of(build["build"])],
           [(txn, "t")])
    expect("PBS references released", stack.build_refs(build["build"]), (0, 0))
    expect("reservation finalized", get(transaction(rid, build["reservation"]))["status"], "ABORTED")
    descriptors = stack.pg("wp33c_dvs", "SELECT status, consumed_by_transaction FROM dataset_staged_output_objects "
                                       f"WHERE reservation_id='{build['reservation']}'")
    expect("DVS descriptor consumed by publication", descriptors, [f"CONSUMED|{txn}"])
    file, = [item for item in file_list(rid) if item["transaction_id"] == txn]
    expect("published filename", file["logical_path"], LOGICAL)
    index, = [item for item in get(dvs(rid, "files/index")) if item["path"] == LOGICAL]
    expect("DVS indexed MIME", index["content_type"], MIME)
    expect("DVS file index stored URI", index["storage_path"], file["physical_uri"])
    raw = download(rid, file)
    path = EVIDENCE / f"{case}.parquet"
    path.write_bytes(raw)
    expect("old file still has identical bytes", sha256(download(rid, old_file)).hexdigest(),
           before["old_file_sha256"])
    digest = sha256(raw).hexdigest()
    # The legacy file projection makes sha256 optional. The file index
    # carries the mandatory DVS digest, checked with the other index fields below.
    expect("PBS stage SHA-256", detail["sha256"], digest)
    if file.get("sha256") is not None:
        expect("projected file SHA-256", file["sha256"], digest)
    expect("published file and stage size", (file["size_bytes"], index["size_bytes"], detail["size_bytes"]),
           (len(raw),) * 3)
    metadata = index["metadata"]
    subset = {
        "format": "parquet", "schema_version": SCHEMA, "sha256": digest, "record_count": len(expected),
        "dataset_id": id_tail(rid), "transaction_id": txn, "build_id": build["build"],
        "staged_object_id": detail["staged_object_id"],
    }
    expect("DVS indexed publish metadata", {key: metadata[key] for key in subset}, subset)
    expect("PBS sealed row count", detail["record_count"], len(expected))
    parquet = verify_parquet(conn, path, expected, row_groups)
    return {"status": "passed", **build, "output_dataset": rid, "previous_head": before["head"],
            "version": version, "file_id": file["id"], "file_index": index, "parquet": parquet,
            "execute_detail": detail, "references_released": True}


def publish_case(conn, case, source, expected, row_groups, predicate=None):
    rid, old_file, before = seed_output(case)
    build = setup_build(case, source, rid, predicate)
    return verify_publish(conn, case, build, before, rid, old_file, stack.execute(build["build"]),
                          expected, row_groups)


def build_failure(case, source):
    rid, old_file, before = seed_output(case)
    build = setup_build(case, source, rid)
    start = len(stack.proxy_events())
    if case == "truncated-stage":
        _, arm = stack.http("POST", stack.PROXY_CTL + "/truncate-stage",
                            {"after_bytes": 128, "count": 30})
        expect("truncate rule armed", arm["armed"], True)
    try:
        status, response = stack.execute(build["build"], timeout=120)
        expect("failed execute status", status, 400)
        expect("failed execute error", response["error"], "batched_stage_failed")
        if case == "invalid-row":
            expect("typed invalid decimal caused failure",
                   "batched_record_invalid" in response["detail"]
                   and "DECIMAL(18,4) amount" in response["detail"], True)
        events = stack.proxy_events()[start:]
        if case == "truncated-stage":
            faults = [e for e in events if e["action"] == "stage_truncated"]
            expect("DVS transport truncation observed", bool(faults), True)
            expect("truncations sent partial request upstream",
                   {e["bytes_forwarded"] for e in faults}, {128})
        expect("failed build made no commit attempt",
               [e for e in events if e["path"].endswith("/outputs:commit")], [])
        expect("PBS failed terminally", stack.build_state(build["build"]), ["BUILD_FAILED"])
        expect("failed build released PBS references", stack.build_refs(build["build"]), (0, 0))
        expect("failed build reservation aborted",
               get(transaction(rid, build["reservation"]))["status"], "ABORTED")
        residue = no_stage_residue(build["reservation"])
        after = output_state(rid, old_file)
        expect("failure preserved head, versions, index and old bytes", after, before)
        return {"status": "passed", **build, "output_dataset": rid, "http_status": status,
                "error": response, "before": before, "after": after, "proxy_events": events,
                "references_released": True, "stage_residue": residue}
    finally:
        stack.http("POST", stack.PROXY_CTL + "/reset", {})


def wait_for_health(base):
    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen(base + "/healthz", timeout=2) as response:
                expect("health status", response.status, 200)
                return
        except OSError as exc:
            if time.monotonic() >= deadline:
                raise AssertionError(f"service did not become healthy at {base}") from exc
            time.sleep(0.25)


def replay_case(conn, source):
    case = "ack-replay"
    rid, old_file, before = seed_output(case)
    build = setup_build(case, source, rid)
    start = len(stack.proxy_events())
    path = f"/v1/datasets/{stack.canon(rid)}/outputs:commit"
    _, armed = stack.http("POST", stack.PROXY_CTL + "/drop-next",
                          {"method": "POST", "path": path, "key": ""})
    expect("drop rule armed", armed["armed"], True)
    first_status, first = stack.execute(build["build"])
    expect("lost ACK execute status", first_status, 503)
    expect("lost ACK error", first["error"], "batched_publish_outcome_unknown")
    events = stack.proxy_events()[start:]
    drop, = [e for e in events if e["action"] == "ack_dropped"]
    expect("fault on exact dataset commit path", drop["path"], path)
    expect("proxy dropped a successful DVS reply", drop["upstream_status"] in (200, 201), True)
    txn = new_version(rid, before)
    expect("DVS committed before restart", drop["transaction_id"], txn["transaction_id"])
    key, request_sha = drop["key"], drop["request_sha256"]

    receipt_sha, error = stack.receipt_raw_sha(rid, quote(key, safe=""))
    expect("DVS receipt available before restart", error, None)
    receipt, = stack.pg("wp33c_dvs", "SELECT request_fingerprint, "
                         "encode(sha256(convert_to(response_body,'UTF8')),'hex') "
                         "FROM dataset_output_commit_receipts "
                         f"WHERE dataset_id='{id_tail(rid)}' AND idempotency_key="
                         f"(SELECT idempotency_key FROM dataset_staged_output_objects "
                         f"WHERE reservation_id='{build['reservation']}')")
    expect("DVS durable exact request and response", receipt, f"{request_sha}|{receipt_sha}")
    durable, = stack.pg("wp33c_pbs", "SELECT stage, octet_length(commit_request_body), "
                        "encode(sha256(commit_request_body),'hex') FROM build_batched_configs "
                        f"WHERE build_id='{id_tail(build['build'])}'")
    stage, body_len, saved_sha = durable.split("|")
    expect("PBS retained publish intent", stage, "OUTCOME_UNKNOWN")
    expect("PBS persisted commit bytes with original hash", (int(body_len) > 0, saved_sha), (True, request_sha))
    expect("PBS references retained before replay", stack.build_refs(build["build"]), (1, 1))

    stack.docker("restart", f"{stack.PROJECT}-pbs")
    wait_for_health(stack.PBS)
    execution = stack.execute(build["build"])
    after_drop = stack.proxy_events()[start + events.index(drop) + 1:]
    replay, = [event for event in after_drop if event["action"] == "commit_passthrough"]
    expect("commit replay exact body and transaction",
           (replay["path"], replay["key"], replay["request_sha256"], replay["transaction_id"]),
           (path, key, request_sha, txn["transaction_id"]))
    expect("no source reread or restaging after ACK loss",
           [e for e in after_drop if e["path"].endswith(("/rows:scan", "/outputs:stage"))], [])
    expect("unchanged raw DVS receipt", stack.receipt_raw_sha(rid, quote(key, safe="")), (receipt_sha, None))
    saved_receipt, = stack.pg("wp33c_pbs", "SELECT encode(sha256(commit_response_body),'hex') "
                              f"FROM build_batched_configs WHERE build_id='{id_tail(build['build'])}'")
    expect("PBS persisted the exact DVS receipt", saved_receipt, receipt_sha)
    result = verify_publish(conn, case, build, before, rid, old_file, execution,
                            reference.EXPECTED_ORDERS, 1, replay=True)
    expect("replay reused the version", result["version"], txn)
    return {**result, "first_status": first_status, "first_error": first, "ack_drop": drop, "replay": replay,
            "persisted_intent_bytes": int(body_len), "receipt_sha256": receipt_sha,
            "post_drop_events": after_drop}


def direct_dvs_rejection(expected_rows):
    case = "dvs-rejection"
    rid, old_file, before = seed_output(case)
    build_id = str(uuid4())
    key = f"wp313-{build_id}"
    _, txn = stack.http("POST", dvs(rid, "branches/main/transactions"), {
        "type": "UPDATE",
        "providence": {"origin": "pipeline-build-service",
                       "purpose": "wp33-staged-output", "build_id": build_id},
    })
    reservation = id_tail(txn["id"])
    expect("direct DVS test reservation opened", txn["status"], "OPEN")
    query = urlencode({"branch": "main", "reservation_id": reservation, "build_id": build_id,
                       "idempotency_key": key, "logical_path": LOGICAL, "format": "parquet"})
    stage = dvs(rid, f"outputs:stage?{query}")
    valid = (EVIDENCE / "ack-replay.parquet").read_bytes()
    corrupt = valid[:-12] + b"broken footer"
    bad_status, bad_body = stack.http("POST", stage, corrupt, ctype=MIME, allow=(400,))
    expect("malformed Parquet status and code", (bad_status, bad_body["code"]), (400, "STAGED_PARQUET_INVALID"))
    malformed_cleanup = no_stage_residue(reservation)
    mime_status, mime_body = stack.http("POST", stage, valid, ctype="application/x-ndjson", allow=(400,))
    expect("reject wrong stage MIME", (mime_status, f"Content-Type {MIME}" in mime_body["error"]), (400, True))
    mismatch_cleanup = no_stage_residue(reservation)
    stage_status, sealed = stack.http("POST", stage, valid, ctype=MIME, allow=(201,))
    expect("same reservation accepts valid bytes",
           (stage_status, sealed["format"], sealed["record_count"], sealed["size_bytes"], sealed["sha256"]),
           (201, "parquet", expected_rows, len(valid), sha256(valid).hexdigest()))
    commit_status, commit_body = stack.http("POST", dvs(rid, "outputs:commit"), {
        "idempotency_key": key, "branch": "main", "transaction_type": "SNAPSHOT",
        "reservation_id": reservation, "build_id": build_id,
        "files": [{"logical_path": LOGICAL, "operation": "STAGED_ADD",
                   "staged_object_id": sealed["staged_object_id"], "size_bytes": sealed["size_bytes"],
                   "sha256": sealed["sha256"], "content_type": "application/x-ndjson",
                   "metadata": {"schema_version": SCHEMA}}],
    }, allow=(400,))
    expect("Parquet seal cannot commit under NDJSON MIME",
           (commit_status, f"files[].content_type must be {MIME}" in commit_body["error"]), (400, True))
    expect("wrong MIME neither consumed nor installed the valid seal", stage_rows(reservation), ["SEALED"])
    expect("wrong MIME left no commit receipt", stack.receipt_get(rid, key)[0], 404)
    expect("wrong MIME left reservation open", get(transaction(rid, reservation))["status"], "OPEN")
    _, aborted = stack.http("POST", transaction(rid, reservation) + ":abort", {})
    expect("direct probe reservation explicitly aborted", aborted["status"], "ABORTED")
    after = output_state(rid, old_file)
    expect("DVS rejection preserved head, versions, index and old bytes", after, before)
    return {"status": "passed", "output_dataset": rid, "reservation": reservation,
            "corrupted_footer": {"status": bad_status, "error": bad_body, **malformed_cleanup},
            "stage_mime_mismatch": {"status": mime_status, "error": mime_body, **mismatch_cleanup},
            "commit_mime_mismatch": {"status": commit_status, "error": commit_body},
            "unpublished_seal": sealed["staged_object_id"], "aborted_reservation": True,
            "before": before, "after": after}


def runtime(conn):
    version, source_id, codename = conn.execute("PRAGMA version").fetchone()
    containers = {}
    for service in ("dvs", "pbs", "postgres", "faultproxy"):
        name = f"{stack.PROJECT}-{service}"
        identity = stack.docker("inspect", "--format",
                                "{{.Id}}|{{.Image}}|{{.Config.Image}}|{{.State.StartedAt}}", name).stdout.strip()
        container = dict(zip(("id", "image_id", "image", "started_at"), identity.split("|"), strict=True))
        if service in BINARIES:
            container["binary_sha256"] = stack.docker("exec", name, "sha256sum", "/service").stdout.split()[0]
        containers[service] = container
    return {"project": stack.PROJECT,
            "docker_server_version": stack.docker("version", "--format", "{{.Server.Version}}").stdout.strip(),
            "postgres_version": stack.pg("wp33c_dvs", "SHOW server_version")[0],
            "reader": {"name": "DuckDB", "version": version, "source_id": source_id, "codename": codename,
                       "python": platform.python_version(), "platform": platform.platform()},
            "stubs": ["WP3.3 HTTP transport proxy for the two injected faults"],
            "containers": containers}


def main():
    expect("DuckDB package", duckdb.__version__, reference.DUCKDB_VERSION)
    for service in (stack.DVS, stack.PBS, stack.PROXY_CTL):
        wait_for_health(service)
    orders = json.loads((WP312 / "orders.json").read_text())["orders"]
    # The three WP3.12 decimal/time/NULL controls plus enough exact, unique
    # orders to force a second Arrow batch and Parquet row group.
    extra = [{**orders[0], "order_id": i, "note": f"batch row {i}"} for i in range(1, 1023)]
    golden = [(row["order_id"], *reference.EXPECTED_ORDERS[-1][1:5], row["note"]) for row in extra]
    golden.extend(reference.EXPECTED_ORDERS)
    rows = seed_input("all", [("part-00000.ndjson", orders), ("part-00001.ndjson", extra)])
    small = seed_input("exact", [("part-00000.ndjson", orders)])
    invalid = seed_input("invalid", [("part-00000.ndjson", orders),
                                    ("part-00001.ndjson", [{**orders[0], "amount": "1.00001"}])])
    with duckdb.connect() as conn:
        write_json("runtime-before", runtime(conn))
        cases = {
            "publish": lambda: publish_case(conn, "publish", rows, golden, 2),
            "empty": lambda: publish_case(conn, "empty", small, [], 0,
                                          '{"column":"source_time_zone","op":"eq","value":"Etc/UTC"}'),
            "invalid-row": lambda: build_failure("invalid-row", invalid),
            "truncated-stage": lambda: build_failure("truncated-stage", rows),
            "ack-replay": lambda: replay_case(conn, small),
            "dvs-rejection": lambda: direct_dvs_rejection(len(orders)),
        }
        expect("mandatory smoke case roster", tuple(cases), CASES)
        for name, run in cases.items():
            try:
                write_json(name, run())
            except Exception as exc:
                write_json(name, {"status": "failed", "error_type": type(exc).__name__})
                raise
            print(f"PASS {name}", flush=True)
        write_json("runtime-after", runtime(conn))


if __name__ == "__main__":
    main()
