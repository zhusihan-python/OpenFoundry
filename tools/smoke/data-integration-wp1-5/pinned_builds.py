"""Build three order projections from versioned CDC inputs using a single pipeline."""
import hashlib
import json


def build(state, fixture, p1, graph, handoff, owner, reader, source, sample):
    raw = handoff["input_dataset"]
    out = handoff["output_dataset"]
    nodes = fixture.order_amount_nodes(graph, raw, out, source)
    before = {v["transaction_id"] for v in graph.list_versions(out)}
    graph.TOKEN = owner
    try:
        if "pipeline_id" in handoff:
            pid = handoff["pipeline_id"]
            graph.http("PATCH", f"{graph.PBS}/api/v1/pipelines/{pid}", {"nodes": nodes})
        else:
            pid = graph.create_pipeline("cdc-order-amounts", nodes)
            handoff["pipeline_id"] = pid

        _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pid}")
        config = next(n["config"] for n in saved["dag"]["nodes"] if n["id"] == "input")
        assert config["dataset_id"] == raw and config["dataset_snapshot"] == nodes[0]["config"]["dataset_snapshot"]
        run = graph.run_pipeline(pid)
        assert run["status"] == "succeeded", run.get("error_message")
    finally:
        graph.TOKEN = reader

    assert run["pipeline_id"] == pid and run["id"] == run["run_id"]
    observed = next(n["output"] for n in run["node_results"] if n["node_id"] == "input")
    input_dataset = observed["dataset_rid"].removeprefix("ri.foundry.main.dataset.")
    input_transaction = observed["dataset_transaction_id"].removeprefix("ri.foundry.main.transaction.")
    assert input_dataset == raw and observed["dataset_branch"] == "main"
    assert observed["dataset_version"] == source["version"] and input_transaction == source["transaction"]
    output_node = next(n for n in run["node_results"] if n["node_id"] == "output")
    resource, = output_node["output_resources"]
    assert resource["status"] == "committed" and resource["rid"].removeprefix("ri.foundry.main.dataset.") == out
    produced, = [v for v in graph.list_versions(out) if v["transaction_id"] not in before]
    txn = produced["transaction_id"]
    metadata = graph.get_output_transaction(out, txn)["metadata"]
    assert metadata["pipeline_rid"] == pid and metadata["transaction_rid"] == resource["transaction_rid"]

    expected = [{"order_id": r["order_id"], "amount_cents": r["amount_cents"]} for r in sample]
    scanned = graph.scan_all(out, produced["version"])["rows"]
    file, = [f for f in p1.list_files(reader, out) if f.get("transaction_id") == txn]
    download = p1.download_file(reader, out, file["id"])
    assert download.status == 200
    rows = [json.loads(line) for line in download.body.splitlines()]
    assert rows == scanned == expected
    assert all(type(row["amount_cents"]) is int for result in (rows, scanned) for row in result)
    artifact = f"build-{len(handoff['builds'])+1}-input-v{source['version']}.ndjson"
    (state / artifact).write_bytes(download.body)
    handoff["builds"].append({
        "pipeline_run_id": run["id"], "input_branch": observed["dataset_branch"],
        "input_dataset": input_dataset, "input_version": observed["dataset_version"],
        "input_transaction": input_transaction, "output_version": produced["version"],
        "output_transaction": txn, "pipeline_output_transaction_rid": resource["transaction_rid"],
        "output_file_id": file["id"], "artifact": artifact, "rows": rows,
        "sha256": hashlib.sha256(download.body).hexdigest(),
    })
    print(f"PASS build {run['id']} consumed CDC v{source['version']} and published {artifact}", flush=True)


def verify_replay(state, p1, graph, reader, handoff, revision):
    builds = handoff["builds"]
    first, latest, replay = builds
    assert [b["input_version"] for b in builds] == [1, 4, 1]
    assert first["input_transaction"] == replay["input_transaction"] != latest["input_transaction"]
    for field in ("pipeline_run_id", "output_version", "output_transaction", "output_file_id"):
        assert len({b[field] for b in builds}) == 3, field
    assert first["sha256"] == replay["sha256"] != latest["sha256"]
    assert (state / first["artifact"]).read_bytes() == (state / replay["artifact"]).read_bytes()
    out = handoff["output_dataset"]
    versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(out)}
    for b in builds:
        assert versions[b["output_version"]] == b["output_transaction"]
        assert graph.scan_all(out, b["output_version"])["rows"] == b["rows"]
        download = p1.download_file(reader, out, b["output_file_id"])
        assert download.status == 200 and download.body == (state / b["artifact"]).read_bytes()
    handoff["revision"] = revision
    (state / "pipeline-report.json").write_text(json.dumps(handoff, indent=2) + "\n")
    print("PASS independent v1 replay matched the original bytes after v4", flush=True)
