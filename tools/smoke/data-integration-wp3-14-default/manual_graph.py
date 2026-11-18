"""Read and publish via the real graph /runs route on the disposable stack."""

from hashlib import sha256
import json
from pathlib import Path
import sys

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.with_name("data-integration-wp3-13")))
import acceptance as write_fixture

stack = write_fixture.stack
expect = write_fixture.expect
SCHEMA = write_fixture.SCHEMA
PROFILE = {"output_schema_version": SCHEMA}


def trigger_graph(label, source, target, config, status):
    dataset, version, txn = source
    pipeline = stack.http("POST", stack.PBS + "/api/v1/pipelines", {
        "name": "wp314-default-graph-" + label,
        "pipeline_type": "FASTER",
        "nodes": [
            {"id": "source", "label": "Read pinned orders", "transform_type": "dataset_input",
             "input_dataset_ids": [dataset], "config": {
                 "source_kind": "dataset", "dataset_id": stack.canon(dataset),
                 "dataset_snapshot": {"branch": "main", "version": version, "transaction_id": txn},
             }},
            {"id": "output", "label": "Publish orders", "transform_type": "output_dataset",
             "depends_on": ["source"], "output_dataset_id": target, "config": config},
        ],
    }, allow=(201,))[1]
    run = stack.http("POST", f"{stack.PBS}/api/v1/pipelines/{pipeline['id']}/runs", {}, allow=(201,))[1]
    expect(f"graph run {label}", run["status"], status)
    result = {"id": run["id"], "pipeline": pipeline["id"], "state": status}
    if status == "failed":
        output, = (node for node in run["node_results"] if node["node_id"] == "output")
        expect("typed amount violation failed publication", "invalid DECIMAL(18,4) amount" in output["error"], True)
        result["output_error"] = output["error"]
    return result


def verify_file(target, before, path, mime):
    version = write_fixture.new_version(target, before)
    file, = [item for item in write_fixture.file_list(target) if item["transaction_id"] == version["transaction_id"]]
    expect("published logical path", file["logical_path"], path)
    indexed, = [item for item in write_fixture.get(write_fixture.dvs(target, "files/index")) if item["path"] == path]
    expect("real committed MIME", indexed["content_type"], mime)
    content = write_fixture.download(target, file)
    digest = sha256(content).hexdigest()
    expect("file SHA-256 and size", (indexed["metadata"]["sha256"], indexed["size_bytes"]), (digest, len(content)))
    return version, file, indexed["metadata"], content


def verify_parquet(conn, label, target, before, rows):
    version, file, metadata, content = verify_file(target, before, "part-00000.parquet", write_fixture.MIME)
    expect("server accepted schema and count", (metadata["schema_version"], metadata["record_count"]), (SCHEMA, len(rows)))
    path = write_fixture.EVIDENCE / f"manual-{label}.parquet"
    path.write_bytes(content)
    parquet = write_fixture.verify_parquet(conn, path, rows, int(bool(rows)))
    return {"version": version, "file": file, "parquet": parquet}


def main():
    expect("DuckDB version", duckdb.__version__, write_fixture.reference.DUCKDB_VERSION)
    orders = json.loads((write_fixture.WP312 / "orders.json").read_text())["orders"]
    good = write_fixture.seed_input("wp314-default-manual", [("orders.ndjson", orders)])
    stack.inline_commit(good[0], "wp314d-graph-advance-input", [
        ("orders.ndjson", (json.dumps({**orders[0], "order_id": -123456}) + "\n").encode()),
    ], "Advance graph input after pinning")
    empty = write_fixture.seed_input("wp314-default-manual-empty", [("orders.ndjson", [])])
    bad = write_fixture.seed_input("wp314-default-manual-invalid", [
        ("orders.ndjson", [{**orders[0], "amount": "1.00001"}]),
    ])

    with duckdb.connect() as conn:
        rid, _, before = write_fixture.seed_output("wp314-default-manual-orders")
        default = trigger_graph("default", good, rid, PROFILE, "succeeded")
        default.update(verify_parquet(conn, "default", rid, before, write_fixture.reference.EXPECTED_ORDERS))
        current_file = default["file"]
        old_state = write_fixture.output_state(rid, current_file)
        negative = trigger_graph("bad-row", bad, rid, PROFILE, "failed")
        expect("invalid row preserves version, index and previous byte digest",
               write_fixture.output_state(rid, current_file), old_state)
        negative["old_file_sha256"] = old_state["old_file_sha256"]

        empty_rid, _, empty_before = write_fixture.seed_output("wp314-default-manual-typed-empty")
        empty_case = trigger_graph("empty", empty, empty_rid, PROFILE, "succeeded")
        empty_case.update(verify_parquet(conn, "empty", empty_rid, empty_before, []))

        ndjson_rid, _, ndjson_before = write_fixture.seed_output("wp314-default-manual-explicit-ndjson")
        explicit = trigger_graph("ndjson", good, ndjson_rid, {**PROFILE, "file_format": "NDJSON"}, "succeeded")
        version, _, metadata, raw = verify_file(ndjson_rid, ndjson_before, "part-00000.ndjson", "application/x-ndjson")
        expect("explicit NDJSON rows", [json.loads(line) for line in raw.splitlines()], orders)
        explicit.update({"version": version, "sha256": metadata["sha256"]})

    write_fixture.write_json("manual-graph", {
        "status": "passed", "entrypoint": "POST /api/v1/pipelines/{id}/runs",
        "default": default, "typed_empty": empty_case, "explicit_ndjson": explicit, "invalid_row": negative,
    })
    print("PASS real graph default Parquet, typed empty, explicit NDJSON and invalid input retention", flush=True)


if __name__ == "__main__":
    main()
