"""Offline Lingxing product batches through real Connector, Dataset Versioning and a pinned Pipeline."""
import hashlib
import importlib.util
import json
import os
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
import uuid

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("s3_fixture", HERE.parent / "data-integration-wp2-3/orders.py")
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)
products = s3.module("wp28_products", HERE / "products.py")
BUCKET = "lingxing-export"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def example(product_id, sku, name, price):
    return {"id": product_id, "sku": sku, "product_name": name, "cg_price": price}


FIRST = [example(11, "0007", "样品甲", "0.00"), example(12, "W-2", "样品乙", "9.25"),
         example(13, "D3", "样品丙", "17.40")]
SECOND = [example(11, "0007", "样品甲", "2.50"), example(14, "N4", "样品丁", "10.75")]
COSTS = ["sku", "cg_price"]
# SHA256 of the NDJSON a downstream consumer downloads for the costs of each product version.
COST_SHA256 = {1: "846c2b4dbd3314a84f67c36031a06547f7c60341970fde512aa6afd975c0d6e8",
               2: "62a109d34eb2d53871b72ab6eee8e63a60a12a2ac543f9c9a783500bf5630420"}


def costs(version):
    # Independent downstream oracle for these two snapshots: each line is one SKU and its cost, both strings.
    rows = {1: [("0007", "0.00"), ("W-2", "9.25"), ("D3", "17.40")], 2: [("0007", "2.50"), ("N4", "10.75")]}
    return [dict(sku=sku, cg_price=cost) for sku, cost in rows[version]]


def value_diffs(data, expected):
    """Name each line and value where downloaded NDJSON differs from the expected rows."""
    lines = data.splitlines()
    diffs = [] if len(lines) == len(expected) else [f"{len(lines)} lines, expected {len(expected)}"]
    for number, (line, row) in enumerate(zip(lines, expected), 1):
        try:
            value = json.loads(line)
        except ValueError:
            diffs.append(f"line {number}: not JSON")
            continue
        if not isinstance(value, dict):
            diffs.append(f"line {number}: not an object")
            continue
        for column in sorted(set(value) | set(row)):
            if column not in value or column not in row:
                diffs.append(f"line {number} {column}: present on one side only")
            elif value[column] != row[column]:
                diffs.append(f"line {number} {column}: {value[column]!r}, expected {row[column]!r}")
    return diffs


def cost_nodes(graph, source, out, published):
    nodes = graph.graph_nodes(source, published["output_version"], published["output_transaction"], out)
    nodes.insert(1, {"id": "project-costs", "label": "Publish product costs", "depends_on": ["input"],
                     "transform_type": "select", "config": {"columns": COSTS}})
    nodes[-1]["depends_on"] = ["project-costs"]
    return nodes


def pages(rows, *, fail_at=None):
    calls = []
    def fetch(offset, length):
        calls.append(offset)
        if offset == fail_at:
            raise OSError("fixture source page unavailable")
        # Deserialize the simulated wire response just as a decimal-preserving reader would.
        body = json.dumps({"code": 0, "total": len(rows), "data": rows[offset:offset + length]})
        return json.loads(body, parse_float=Decimal)
    return fetch, calls


def main(state):
    p1 = s3.module("wp28_p1", HERE.parent / "business-platform-p1/smoke/smoke.py")
    p1.BASES.update(dataset=os.environ["WP23_DVS"], connector=os.environ["WP23_CONNECTOR"])
    p1.SECRET_BYTES = os.environ["JWT_SECRET"].encode()
    ids = p1.build_identities()
    owner = ids["user_a"].token
    reader = p1.mint_token(org=ids["org_a"], permissions=[])
    foreign = ids["user_b"].token
    os.environ.update(WP34_PROJECT="wp28", WP34_EVIDENCE=str(state), WP34_TOKEN=reader,
                      WP34_DVS=p1.BASES["dataset"], WP34_PBS=os.environ["WP23_PBS"])
    graph = s3.module("wp28_graph", HERE.parent / "data-integration-wp3-4/harness34.py")
    raw = p1.create_dataset(owner, "Lingxing received exports", "json")["id"]
    target = p1.create_dataset(owner, "Lingxing product snapshots", "json")["id"]
    report = {"revision": subprocess.check_output(["git", "-C", str(HERE), "rev-parse", "HEAD"], text=True).strip(),
              "source_mode": "offline synthetic pages", "organization": ids["org_a"],
              "source_dataset": raw, "product_dataset": target, "checks": [], "batches": []}

    def check(label, condition):
        if not condition:
            raise AssertionError(label)
        print("PASS", label, flush=True)
        report["checks"].append(label)

    def receive(batch, fetch):
        # No file or object is made visible until every page validates.
        rows = products.collect(fetch, 2)
        if not rows:
            raise ValueError("empty snapshot publication awaits product confirmation")
        path = f"products/{batch}.ndjson"
        export = state / f"{batch}.ndjson"
        export.write_bytes(products.ndjson(rows))
        writer = s3.scoped_credentials(f"arn:aws:s3:::{BUCKET}/{path}", "s3:PutObject")
        check("upload completed export " + batch,
              s3.s3_request("PUT", f"{BUCKET}/{path}", export, credentials=writer) == 200)
        result = p1.http("connector", s3.DC + "/sources", method="POST", token=owner,
                         payload={"name": f"products-{batch}", "connector_type": "s3",
                                  "config": {"bucket": BUCKET, "sync": {"object_key": path}}})
        assert result.status in (200, 201), result.status
        source_id = result.body["id"]
        credentials = s3.scoped_credentials(f"arn:aws:s3:::{BUCKET}/{path}", "s3:GetObject")
        bound = p1.http("connector", f"{s3.DC}/sources/{source_id}/credentials", method="POST",
                        token=owner, payload={"kind": "aws_keys", "value": json.dumps(credentials)})
        assert bound.status in (200, 201, 204), bound.status
        sync = p1.create_sync(owner, source_id, raw)
        assert sync.status in (200, 201), sync.status
        sync_id = sync.body["id"]
        run = p1.run_sync(owner, sync_id)
        check("connector committed export " + batch, run["status"] == "succeeded")
        return {"source_batch": batch, "source_id": source_id, "sync_id": sync_id, "sync_run": run["id"],
                "raw_transaction": run["dataset_version_id"], "export_sha256": sha(export.read_bytes()),
                "row_count": len(rows)}

    def deliver(batch):
        response = p1.api("connector", "sync_run_get", token=owner,
                          path_args={"id": batch["sync_id"], "run_id": batch["sync_run"]})
        assert response.status == 200 and response.body["status"] == "succeeded"
        assert response.body["id"] == batch["sync_run"] and response.body["sync_def_id"] == batch["sync_id"]
        assert response.body["dataset_version_id"] == batch["raw_transaction"]
        source_txn = graph.get_output_transaction(raw, batch["raw_transaction"])
        assert source_txn["metadata"]["lineage"] == {
            "source_id": batch["source_id"], "sync_def_id": batch["sync_id"], "sync_run_id": batch["sync_run"]}
        rows, _, data = p1.download_run_artifact(reader, raw, response.body)
        assert len(rows) == batch["row_count"] and sha(data) == batch["export_sha256"]
        body = products.commit_body(raw, batch["source_batch"], data)
        wire = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        result = p1.http("dataset", f"/api/v1/datasets/{target}/outputs:commit", token=owner, payload=wire)
        if result.status in (200, 201):
            receipt = result.body
            assert (receipt["dataset_id"], receipt["branch"], receipt["idempotency_key"],
                    receipt["request_fingerprint"], receipt["transaction"]["status"]) == (
                        target, "main", body["idempotency_key"], sha(wire), "COMMITTED")
        return result

    check("create private export bucket", s3.s3_request("PUT", BUCKET) in (200, 201))
    for i, expected in enumerate((FIRST, SECOND), 1):
        fetch, offsets = pages(expected)
        batch = receive(str(uuid.uuid4()), fetch)
        check(f"v{i} page offsets", offsets == ([0, 2] if i == 1 else [0]))
        result = deliver(batch)
        check(f"v{i} first publish", result.status == 201)
        txn = result.body["transaction"]["id"]
        version = graph.version_by_txn(target, txn)
        file, = [f for f in p1.list_files(reader, target) if f["transaction_id"] == txn]
        batch.update(output_version=version, output_transaction=txn, output_file_id=file["id"], offsets=offsets)
        report["batches"].append(batch)
        check(f"v{i} version number and scan", version == i and graph.scan_all(target, version)["rows"] == expected)

    first, second = report["batches"]
    before = graph.list_versions(target)
    before_raw = graph.list_versions(raw)
    replay = deliver(first)
    check("v1 redelivery after v2 returns original receipt", replay.status == 200 and
          replay.body["transaction"]["id"] == first["output_transaction"])
    conflict = deliver(dict(second, source_batch=first["source_batch"]))
    check("changed content under existing batch rejected", conflict.status == 409)
    report["replay_transaction"] = replay.body["transaction"]["id"]
    failed_batch = str(uuid.uuid4())
    fetch, offsets = pages(FIRST, fail_at=2)
    try:
        receive(failed_batch, fetch)
    except OSError as exc:
        assert str(exc) == "fixture source page unavailable"
    else:
        raise AssertionError("incomplete export accepted")
    check("page 2 failure left no export", offsets == [0, 2] and
          s3.s3_request("GET", f"{BUCKET}/products/{failed_batch}.ndjson") == 404)
    check("remove source to exercise connector failure",
          s3.s3_request("DELETE", f"{BUCKET}/products/{second['source_batch']}.ndjson") == 204)
    failed = p1.run_sync(owner, second["sync_id"])
    check("connector read failure has no receipt", failed["status"] == "failed" and not failed.get("dataset_version_id"))
    report["failed_connector_run"] = failed["id"]
    check("replay and failures preserve both version histories",
          graph.list_versions(target) == before and graph.list_versions(raw) == before_raw)
    head = p1.api("dataset", "ds_branch_get", token=reader, path_args={"id": target, "branch": "main"})
    check("reader sees v2 as branch head after v1 replay and failures",
          head.status == 200 and head.body["head_transaction_id"] == second["output_transaction"])
    report["final_head_transaction"] = head.body["head_transaction_id"]
    for batch, expected in zip(report["batches"], (FIRST, SECOND)):
        version = batch["output_version"]
        data = p1.download_file(reader, target, batch["output_file_id"])
        check(f"independent reader resolves v{version} after replay", data.status == 200 and
              sha(data.body) == batch["export_sha256"] and data.body == products.ndjson(expected) and
              graph.scan_all(target, version)["rows"] == expected)
        (state / f"published-v{version}.ndjson").write_bytes(data.body)
        check(f"foreign tenant denied v{version} download",
              p1.download_file(foreign, target, batch["output_file_id"]).status in (403, 404))
    denied = p1.http("dataset", f"/v1/datasets/{target}/files/rows:scan?branch=main&version=2&all_files=true",
                     token=foreign)
    check("foreign tenant denied version scan", denied.status in (403, 404))

    # One pipeline reads the published product snapshots, not the raw batches: v1, then v2, then v1 again.
    def head_of(dataset):
        result = p1.api("dataset", "ds_branch_get", token=reader, path_args={"id": dataset, "branch": "main"})
        assert result.status == 200, result.status
        return result.body["head_transaction_id"]

    down = p1.create_dataset(owner, "Lingxing product costs", "json")["id"]
    # Two different identifiers name one published output. Every build records both.
    pipeline = report["pipeline"] = dict(
        pipeline_id=None, output_dataset=down, columns=COSTS, expected_sha256=COST_SHA256, builds=[],
        transaction_ids={
            "input_transaction": "Dataset Versioning transaction id of the product snapshot version the run read",
            "output_transaction": "Dataset Versioning transaction id of the output version, as the reader lists it",
            "pipeline_output_transaction_rid": "transaction_rid of the committed resource in the Pipeline run receipt, "
                                               "the Pipeline's own name. The Dataset transaction repeats it in "
                                               "metadata.transaction_rid, which links the run to its output version"})
    known = {v["transaction_id"] for v in graph.list_versions(down)}
    input_versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(target)}
    for ordinal, batch in enumerate((first, second, first), 1):
        version = batch["output_version"]
        pin = {"branch": "main", "version": version,
               "transaction_id": "ri.foundry.main.transaction." + batch["output_transaction"]}
        heads = dict(input=head_of(target), output=head_of(down) if pipeline["builds"] else None)
        nodes = cost_nodes(graph, target, down, batch)
        graph.TOKEN = owner
        try:
            pid = pipeline["pipeline_id"]
            if pid is None:
                pid = pipeline["pipeline_id"] = graph.create_pipeline("lingxing-product-costs", nodes)
            else:
                graph.http("PATCH", f"{graph.PBS}/api/v1/pipelines/{pid}", {"nodes": nodes})
            _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pid}")
            run = graph.run_pipeline(pid)
        finally:
            graph.TOKEN = reader
        s3.write_json(state / f"pipeline-{ordinal}-graph.json", saved)
        s3.write_json(state / f"pipeline-{ordinal}-run.json", run)
        check(f"pipeline run {ordinal} succeeded", run["status"] == "succeeded")
        saved_nodes = {n["id"]: n for n in saved["dag"]["nodes"]}
        config = saved_nodes["input"]["config"]
        check(f"pipeline run {ordinal} saved the graph pinned to product v{version} and its transaction",
              config["dataset_id"] == target and config["dataset_snapshot"] == pin
              and saved_nodes["project-costs"]["config"] == {"columns": COSTS}
              and saved_nodes["output"]["output_dataset_id"] == down)
        observed = next(n["output"] for n in run["node_results"] if n["node_id"] == "input")
        check(f"pipeline run {ordinal} input node receipt read product v{version} on main through the pinned transaction",
              run["pipeline_id"] == pid and run["id"] == run["run_id"]
              and observed["dataset_rid"].removeprefix("ri.foundry.main.dataset.") == target
              and observed["dataset_branch"] == pin["branch"] and observed["dataset_version"] == pin["version"]
              and observed["dataset_transaction_id"] == pin["transaction_id"]
              and observed["rows_affected"] == batch["row_count"] and not observed["dataset_rows_truncated"]
              and observed["sample_rows"] == next(r for b, r in ((first, FIRST), (second, SECOND)) if b is batch))
        produced, = [v for v in graph.list_versions(down) if v["transaction_id"] not in known]
        known.add(produced["transaction_id"])
        output_node = next(n for n in run["node_results"] if n["node_id"] == "output")
        resource, = output_node["output_resources"]
        transaction = graph.get_output_transaction(down, produced["transaction_id"])
        s3.write_json(state / f"pipeline-{ordinal}-transaction.json", transaction)
        metadata = transaction["metadata"]
        # The run's output resource carries the Pipeline's own transaction name. The Dataset
        # transaction of the new version must name the same handle and this run, not another one.
        handle = f"pipeline-run:{run['id']}:output"
        check(f"pipeline run {ordinal} committed one resource as output v{produced['version']}, a transaction of this run",
              resource["status"] == "committed" and resource["kind"] == "dataset"
              and resource["rid"].removeprefix("ri.foundry.main.dataset.") == down
              and resource["transaction_rid"] == handle and transaction["status"] == "COMMITTED"
              and transaction["branch_name"] == "main" and transaction["dataset_id"] == down
              and metadata["pipeline_output"] is True and metadata["pipeline_rid"] == pid
              and metadata["output_node_id"] == "output" and metadata["transaction_rid"] == handle)
        rows = costs(version)
        versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(down)}
        file, = [f for f in p1.list_files(reader, down) if f.get("transaction_id") == produced["transaction_id"]]
        response = p1.download_file(reader, down, file["id"])
        artifact = f"pipeline-{ordinal}-output-v{produced['version']}.ndjson"
        (state / artifact).write_bytes(response.body)
        diffs = value_diffs(response.body, rows) if response.status == 200 else [f"download status {response.status}"]
        check(f"independent reader maps output v{produced['version']} to its transaction and downloads "
              f"the {len(rows)} costs of product v{version}" + ("" if not diffs else ": " + "; ".join(diffs)),
              response.status == 200 and versions[produced["version"]] == produced["transaction_id"] and not diffs
              and response.body == products.ndjson(rows) and sha(response.body) == COST_SHA256[version]
              and graph.scan_all(down, produced["version"])["rows"] == rows)
        check(f"dataset transaction of output v{produced['version']} carries the run's output hash, "
              f"the {len(rows)} expected values and the bytes the reader downloaded",
              metadata["output_content_hash"] == output_node["output"]["content_hash"]
              and [dict(zip(metadata["preview_columns"], values)) for values in metadata["preview_rows"]] == rows
              and metadata["row_count"] == len(rows) and metadata["size_bytes"] == len(response.body))
        pipeline["builds"].append(dict(ordinal=ordinal, pipeline_run_id=run["id"], input_dataset=target,
                                       input_branch=observed["dataset_branch"], input_version=version,
                                       input_transaction=batch["output_transaction"], input_head=heads["input"],
                                       output_dataset=down, output_version=produced["version"],
                                       output_transaction=produced["transaction_id"], output_head=heads["output"],
                                       pipeline_output_transaction_rid=handle,
                                       pipeline_content_hash=metadata.get("output_content_hash"),
                                       output_file_id=file["id"], artifact=artifact, rows=rows,
                                       row_count=len(rows), size_bytes=len(response.body),
                                       sha256=sha(response.body)))

    first_build, latest, replay = pipeline["builds"]
    output_versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(down)}
    builds = pipeline["builds"]
    check("three runs of one pipeline read product v1, v2 and v1 and published output v1, v2 and v3 in that order",
          [b["input_version"] for b in builds] == [1, 2, 1] and [b["output_version"] for b in builds] == [1, 2, 3]
          and sorted(output_versions) == [1, 2, 3]
          and {b["output_version"]: b["output_transaction"] for b in builds} == output_versions
          and all(len({b[field] for b in builds}) == 3
                  for field in ("pipeline_run_id", "output_transaction", "output_file_id"))
          and {v["version"]: v["transaction_id"] for v in graph.list_versions(target)} == input_versions
          == {b["input_version"]: b["input_transaction"] for b in builds} and len(input_versions) == 2)
    check("product v2 stayed the head of the input while every run read its pinned version",
          head_of(target) == second["output_transaction"]
          and all(b["input_head"] == second["output_transaction"] for b in builds))
    check("v1 replay read the first v1 transaction again and published the first v1 output bytes and hash, not v2",
          replay["input_transaction"] == first_build["input_transaction"] != latest["input_transaction"]
          and replay["output_head"] == latest["output_transaction"] and head_of(down) == replay["output_transaction"]
          and replay["sha256"] == first_build["sha256"] == COST_SHA256[1] != latest["sha256"] == COST_SHA256[2]
          and (state / replay["artifact"]).read_bytes() == (state / first_build["artifact"]).read_bytes()
          != (state / latest["artifact"]).read_bytes())
    s3.write_json(state / "product-report.json", report)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
