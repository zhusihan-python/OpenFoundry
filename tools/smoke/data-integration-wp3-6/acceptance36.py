"""WP3.6 live acceptance: an incompatible pinned input schema is rejected before
any transform or output write, on the real WP3.3 PBS, DVS and PostgreSQL stack."""

import base64
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlencode

import duckdb

HERE = Path(__file__).resolve().parent
WP312 = HERE.with_name("data-integration-wp3-12")
sys.path.extend(map(str, (HERE, WP312, HERE.with_name("data-integration-wp3-13"),
                          HERE.with_name("data-integration-wp3-3"))))
import acceptance as publisher  # WP3.13 driver: seeds, download, output_state, verify_publish
import harness3c as stack
import reader36
import verify as reference  # WP3.12 golden orders.v1
from provenance36 import expect

EVIDENCE = Path(stack.EVIDENCE)
FIXTURE = EVIDENCE / "fixture" / "orders-amount-string.parquet"
MISMATCH = "dataset_schema_content_hash_mismatch"
PAIRING = "dataset_schema_transaction_mismatch"
CASES = ("historical-schema", "bound-publish", "switched-input")


BUSINESS = ("schema", "scan", "stage", "commit")
# The proxy logs an ordinary relay as "forward" and a commit request under
# its own action names; none is armed here, so a commit shows up as a passthrough.
RELAYED = ("forward", "commit_passthrough", "ack_dropped", "commit_response_oversize",
           "stage_truncated", "stage_truncate_error")


def forwards(events):
    """(route, upstream status) of every request the proxy relayed to DVS.

    The proxy records a request once DVS has answered it, so the list follows
    response order, not the order in which requests were opened.
    """
    return [(event["path"].rsplit("/", 1)[-1].rsplit(":", 1)[-1], event.get("upstream_status"))
            for event in events if event.get("action") in RELAYED]


def business(events):
    """The schema gate, scan, upload and commit requests among the forwards."""
    return [event for event in forwards(events) if event[0] in BUSINESS]


def schema_answers(events):
    """Upstream statuses of the schema route, sorted.

    CreateBuild's resolver reads the input's current schema over the same
    route as the schema precondition, and the proxy record carries no query.
    The suite measures that legacy traffic on the unbound build and expects
    a bound build to add exactly one schema answer.
    """
    return sorted(status for route, status in business(events) if route == "schema")


def added_schema_answer(events, status, legacy):
    """A bound CreateBuild answers the schema route once more than the unbound
    build's current-schema lookups; that one added answer is the precondition's.
    A refused CreateBuild stops at the gate, so it runs fewer legacy lookups."""
    answers = schema_answers(events)
    expect(f"exactly one schema precondition answered {status}", answers.count(status), 1)
    answers.remove(status)
    expect("the other schema reads are the resolver's current-schema lookups", answers, legacy[:len(answers)])


def read_schema(dataset, version, status=200, **params):
    query = urlencode({"branch": "main", "version": version, **params})
    code, body = stack.http("GET", publisher.dvs(dataset, f"schema?{query}"), timeout=5, allow=(status,))
    expect("historical schema HTTP status", code, status)
    return body


def publish(pipeline, dataset, version, expected_hash, output):
    """One TRANSFORM spec: the only varying field is the pinned input version."""
    return stack.publish_spec(pipeline, {
        "transform_type": "batched_dataset_copy",
        "input": {"version": version, "branch": "main", "all_files": True,
                  "expected_schema_hash": expected_hash},
        "output": {"logical_path": publisher.LOGICAL, "format": "parquet", "schema_version": publisher.SCHEMA},
    }, [{"dataset_rid": stack.canon(dataset)}], [stack.canon(output)])


def create_build(pipeline, output, status):
    code, response = stack.http("POST", stack.PBS + "/v1/builds", {
        "pipeline_rid": pipeline, "build_branch": "main", "trigger_kind": "MANUAL",
        "output_dataset_rids": [stack.canon(output)],
    }, allow=(status,))
    expect("CreateBuild HTTP status", code, status)
    return response


def independent_read(dataset, version, name):
    """A separate process locates the version, downloads the file and reads it."""
    env = {**os.environ, "WP36_DVS": stack.DVS, "WP36_TOKEN": stack.TOKEN}
    run = subprocess.run([sys.executable, "-I", str(HERE / "reader36.py"), "--dataset", dataset,
                          "--version", str(version), "--out", str(EVIDENCE / f"{name}.parquet")],
                         env=env, capture_output=True, text=True, check=False)
    if run.returncode != 0:
        raise AssertionError(f"independent reader {name} failed: {run.stderr[-800:]}")
    verdict = json.loads(run.stdout)
    publisher.write_json(f"{name}-reader", verdict)
    return verdict


def pbs_counts():
    """Build rows, frozen configurations, reservations and input locks now in PBS."""
    return {table: stack.pg("wp33c_pbs", f"SELECT count(*) FROM {table}")[0]
            for table in ("builds", "build_batched_configs", "build_pending_output_txns", "build_input_locks")}


def seed_orders(conn):
    """The existing orders sample: an unbound build publishes orders.v1 Parquet."""
    orders = json.loads((WP312 / "orders.json").read_text())["orders"]
    source = publisher.seed_input("wp36-orders", [("part-00000.ndjson", orders)])
    dataset, old_file, before = publisher.seed_output("wp36-input")
    start = len(stack.proxy_events())
    build = publisher.setup_build("wp36-orders", source, dataset)
    create_events = stack.proxy_events()[start:]
    start = len(stack.proxy_events())
    published = publisher.verify_publish(conn, "wp36-input", build, before, dataset, old_file,
                                         stack.execute(build["build"]), reference.EXPECTED_ORDERS, 1)
    execute_events = stack.proxy_events()[start:]
    expect("the unbound build made no schema read while executing", schema_answers(execute_events), [])
    routes = [route for route, _ in business(execute_events)]
    expect("the unbound build scanned, uploaded and committed", [route in routes for route in ("scan", "stage", "commit")],
           [True, True, True])
    publisher.write_json("unbound-orders-events", {"create": forwards(create_events), "execute": forwards(execute_events)})
    return {"dataset": dataset, "orders": {"version": published["version"]["version"],
                                           "transaction": published["version"]["transaction_id"],
                                           "unbound_build": build["build"]},
            "orders_file": reader36.describe(EVIDENCE / "wp36-input.parquet"),
            "legacy_schema_answers": schema_answers(create_events)}


def commit_incompatible(dataset):
    """Commit the generated fixture: same orders rows, amount as a string."""
    raw = FIXTURE.read_bytes()
    _, receipt = stack.http("POST", publisher.dvs(dataset, "outputs:commit"), {
        "idempotency_key": "wp36-amount-string", "branch": "main", "transaction_type": "SNAPSHOT",
        "summary": "WP3.6 orders rows with a string amount",
        "files": [{"logical_path": FIXTURE.name, "operation": "ADD", "size_bytes": len(raw),
                   "content_type": publisher.MIME, "content_base64": base64.b64encode(raw).decode()}],
    }, allow=(201,))
    transaction = receipt["transaction"]["id"]
    expect("incompatible sample committed as a real snapshot",
           (receipt["transaction"]["status"], receipt["transaction"]["tx_type"]), ("COMMITTED", "SNAPSHOT"))
    registered, = [item for item in publisher.file_list(dataset) if item["transaction_id"] == transaction]
    expect("committed bytes equal the generated fixture", publisher.download(dataset, registered), raw)
    return {"version": stack.version_by_txn(dataset, transaction), "transaction": transaction,
            "file_sha256": sha256(raw).hexdigest()}


def historical_schema(state):
    """1) The versioned schema is bound by version and transaction."""
    dataset, good, bad = state["dataset"], state["orders"], state["incompatible"]
    plain = read_schema(dataset, good["version"])
    expect("schema is bound to the transaction that cut the version",
           plain["transaction_id"], good["transaction"])
    expect("orders.v1 fields equal an independent Parquet read", plain["fields"], state["orders_file"]["fields"])
    expect("content hash equals an independent computation",
           plain["content_hash"], state["orders_file"]["content_hash"])
    incompatible = read_schema(dataset, bad["version"])
    string_amount = [{**field, "field_type": "STRING"} if field["name"] == "amount" else field
                     for field in plain["fields"]]
    expect("incompatible schema is readable and bound to its own transaction",
           (incompatible["transaction_id"], incompatible["fields"]), (bad["transaction"], string_amount))
    expect("incompatible fields equal an independent read",
           incompatible["fields"], reader36.public_fields(state["incompatible_file"]["schema"]))

    expect("a correct version/transaction pair is accepted",
           read_schema(dataset, good["version"], transaction_id=good["transaction"]), plain)
    expect("the incompatible version pairs with its own transaction",
           read_schema(dataset, bad["version"], transaction_id=bad["transaction"]), incompatible)
    wrong = read_schema(dataset, bad["version"], transaction_id=good["transaction"], status=409)
    reverse = read_schema(dataset, good["version"], transaction_id=bad["transaction"], status=409)
    expect("another version's transaction is refused with a stable code",
           [answer["code"] for answer in (wrong, reverse)], [PAIRING, PAIRING])
    expect("the refusal names the transaction that cut the version", wrong["error"],
           f"{PAIRING}: version {bad['version']} was cut by transaction {bad['transaction']}, not {good['transaction']}")

    expect("the matching hash returns the same schema",
           read_schema(dataset, good["version"], expected_content_hash=plain["content_hash"]), plain)
    expect("the correct hash of the incompatible sample is accepted",
           read_schema(dataset, bad["version"], expected_content_hash=incompatible["content_hash"]), incompatible)
    refused = read_schema(dataset, bad["version"], expected_content_hash=plain["content_hash"], status=412)
    expect("the orders.v1 expectation refuses the string-amount version", refused,
           {"error": MISMATCH + ": the committed schema does not match expected_content_hash", "code": MISMATCH})
    expect("the pair is checked before the hash",
           read_schema(dataset, bad["version"], transaction_id=good["transaction"],
                       expected_content_hash=plain["content_hash"], status=409), wrong)
    expect("an unbound request keeps its shape and gains only content_hash", sorted(plain),
           ["branch", "content_hash", "dataset_id", "fields", "transaction_id", "version"])
    return {"orders_version": plain, "incompatible_version": incompatible,
            "wrong_pair": wrong, "reverse_pair": reverse, "hash_refused": refused}


def bound_publish(conn, state):
    """2) A legitimate pinned input with its expected schema runs the pipeline."""
    dataset, good = state["dataset"], state["orders"]
    output, old_file, before = publisher.seed_output("wp36-output")
    pipeline = stack.create_pipeline("wp36-pinned", dataset, output)
    expected = state["orders_file"]["content_hash"]
    start = len(stack.proxy_events())
    publish(pipeline, dataset, good["version"], expected, output)
    build = create_build(pipeline, output, 202)["build_id"]
    create_events = stack.proxy_events()[start:]
    added_schema_answer(create_events, 200, state["legacy_schema_answers"])
    expect("CreateBuild touched no scan, upload or commit route",
           [route for route, _ in business(create_events) if route != "schema"], [])
    build_id = publisher.id_tail(build)
    reservation, = stack.pg("wp33c_pbs", "SELECT transaction_rid FROM build_pending_output_txns "
                                          f"WHERE build_id='{build_id}'")
    frozen, = stack.pg("wp33c_pbs", "SELECT input_version || '|' || expected_input_schema_hash "
                                    f"FROM build_batched_configs WHERE build_id='{build_id}'")
    expect("frozen input version and schema hash", frozen, f"{good['version']}|{expected}")
    start = len(stack.proxy_events())
    executed = stack.execute(build)
    published = publisher.verify_publish(conn, "wp36-bound", {"build": build, "reservation": publisher.id_tail(reservation)},
                                         before, output, old_file, executed, reference.EXPECTED_ORDERS, 1)
    execute_events = stack.proxy_events()[start:]
    expect("execution proved the schema before any scan or upload", business(execute_events)[0], ("schema", 200))
    routes = [route for route, _ in business(execute_events)]
    expect("the bound build scanned, uploaded and committed", [route in routes for route in ("scan", "stage", "commit")],
           [True, True, True])
    version = published["version"]["version"]
    verdict = independent_read(output, version, "wp36-bound-reader")
    expect("independent reader rows equal the pinned input rows", verdict["rows"], state["orders_file"]["rows"])
    expect("independent reader bytes equal the published file", verdict["sha256"], published["parquet"]["sha256"])
    expect("independent reader size equals the published size", verdict["size_bytes"], published["file_index"]["size_bytes"])
    state.update(output=output, old_file=old_file, pipeline=pipeline, expected=expected, version=version,
                 sha256=verdict["sha256"], size=verdict["size_bytes"],
                 output_transaction=published["version"]["transaction_id"],
                 before=publisher.output_state(output, old_file))
    return {"build": build, "pipeline": pipeline, "expected_schema_hash": expected,
            "input": {"version": good["version"], "transaction": good["transaction"], "fields": state["orders_file"]["fields"]},
            "published": published, "create_events": forwards(create_events), "execute_events": forwards(execute_events),
            "reader": {key: verdict[key] for key in ("version", "transaction_id", "sha256", "size_bytes", "row_count")}}


def switched_input(state):
    """3) The same expectation on the string-amount version is refused before any work."""
    dataset, bad, output, old_file = state["dataset"], state["incompatible"], state["output"], state["old_file"]
    counts = pbs_counts()
    start = len(stack.proxy_events())
    publish(state["pipeline"], dataset, bad["version"], state["expected"], output)
    refused = create_build(state["pipeline"], output, 412)
    events = stack.proxy_events()[start:]
    expect("CreateBuild reports the schema mismatch at the top level", refused["error"], "batched_input_schema_mismatch")
    expect("the rejection carries the dataset code, the input version and the expectation",
           [part in refused["detail"] for part in (MISMATCH, f"input version {bad['version']}", state["expected"])],
           [True, True, True])
    # The gate: the one added schema read is the refusal; no scan, upload or commit followed.
    added_schema_answer(events, 412, state["legacy_schema_answers"])
    expect("no scan, upload or commit followed the refusal",
           [route for route, _ in business(events) if route != "schema"], [])
    expect("no build row, configuration, reservation or lock was written", pbs_counts(), counts)
    expect("the rejection preserved head, versions, files, index and old bytes",
           publisher.output_state(output, old_file), state["before"])
    verdict = independent_read(output, state["version"], "wp36-after-rejection")
    expect("downstream re-download of the same version has identical bytes",
           (verdict["sha256"], verdict["size_bytes"], verdict["transaction_id"]),
           (state["sha256"], state["size"], state["output_transaction"]))
    expect("the re-download still holds the same rows", verdict["rows"], state["orders_file"]["rows"])
    # Recorded in the shape of the PBS response body: the top-level code, then the detail.
    return {"http_status": 412, "error": refused["error"], "detail": refused["detail"],
            "input": {"version": bad["version"], "transaction": bad["transaction"]},
            "expected_schema_hash": state["expected"], "business_events": business(events),
            "schema_answers": schema_answers(events), "legacy_schema_answers": state["legacy_schema_answers"],
            "proxy_events_raw": forwards(events), "output_unchanged": True,
            "redownload": {key: verdict[key] for key in ("version", "transaction_id", "sha256", "size_bytes", "row_count")}}


def main():
    expect("DuckDB package", duckdb.__version__, reference.DUCKDB_VERSION)
    for service in (stack.DVS, stack.PBS, stack.PROXY_CTL):
        publisher.wait_for_health(service)
    with duckdb.connect() as conn:
        runtime = publisher.runtime(conn)
        # No fault is armed: the proxy only records the requests it forwards.
        runtime["stubs"] = ["WP3.3 HTTP transport proxy in passthrough mode (request log only)"]
        publisher.write_json("runtime-before", runtime)
        state = seed_orders(conn)
        state["incompatible"] = commit_incompatible(state["dataset"])
        state["incompatible_file"] = reader36.describe(FIXTURE)
        runs = {"historical-schema": lambda: historical_schema(state),
                "bound-publish": lambda: bound_publish(conn, state),
                "switched-input": lambda: switched_input(state)}
        expect("mandatory smoke case roster", tuple(runs), CASES)
        results = {}
        for name, run in runs.items():
            try:
                results[name] = run()
                publisher.write_json(name, {"status": "passed", **results[name]})
            except Exception as exc:
                publisher.write_json(name, {"status": "failed", "error_type": type(exc).__name__, "error": str(exc)})
                raise
            print(f"PASS {name}", flush=True)
        publisher.write_json("runtime-after", publisher.runtime(conn))
        publisher.write_json("wp36-acceptance", {
            "status": "passed", "cases": list(CASES), "input_dataset": state["dataset"], "output_dataset": state["output"],
            "orders": state["orders"], "incompatible": state["incompatible"],
            "legacy_schema_answers": state["legacy_schema_answers"],
            "expected_schema_hash": state["expected"], "historical_schema": results["historical-schema"],
            "bound_publish": results["bound-publish"], "switched_input": results["switched-input"]})


if __name__ == "__main__":
    main()
