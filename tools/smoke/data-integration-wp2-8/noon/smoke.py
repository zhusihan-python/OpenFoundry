"""Single-store noon order_live replay through S3 Connector, versioned Dataset APIs and a pinned Pipeline."""
import base64
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
SMOKE = HERE.parents[1]
spec = importlib.util.spec_from_file_location("wp23_s3", SMOKE / "data-integration-wp2-3/orders.py")
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)
STORE = "RPL01-NAE"
BUCKET = "noon-order-live"
KEY = "exports/order-lines.ndjson"
COLUMNS = ["store", "market", "order_nr", "item_nr", "unit_price", "currency"]


def encode(rows):
    return b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n" for row in rows)


def materialize(previous, delta):
    """Overlay this committed batch on its pinned predecessor within the single fixture store."""
    items = {row["item_nr"]: row for row in previous}
    seen = set()
    for row in delta:
        item = row["item_nr"]
        parts = re.fullmatch(r"([A-Z0-9]+)-[0-9]+", item)
        if not parts or item in seen or not row["sku"]:
            raise ValueError("invalid or duplicate replay order line")
        if row["country_code"] != "AE" or row["currency_code"] != "AED":
            raise ValueError("unexpected market or currency for AE fixture")
        price = Decimal(str(row["offer_price"]))
        if not price.is_finite() or price < 0:
            raise ValueError("invalid order line unit price")
        seen.add(item)
        items[item] = dict(store=STORE, market="AE", order_nr=parts[1], item_nr=item,
                          sku=row["sku"], unit_price=str(price), currency=row["currency_code"])
    return [items[item] for item in sorted(items)]


def expected(version):
    # Independent downstream oracle for these two batches.
    keys = ["001", "002", "REJ", "TMO", "MIS"]
    prices = ["49" if version == 1 else "49.25", "120", "30", "25", "60"]
    return [dict(store=STORE, market="AE", order_nr=f"NRPL{i:04}", item_nr=f"NRPL{i:04}-1",
                 sku="N-RPL-SKU-" + keys[i-1], unit_price=prices[i-1], currency="AED")
            for i in range(1, (3 if version == 1 else 5) + 1)]


def projection(version):
    # Downstream order lines keep their key, currency and unit price.
    return [{column: row[column] for column in COLUMNS} for row in expected(version)]


def line_price_nodes(graph, source, out, published):
    nodes = graph.graph_nodes(source, published["version"], published["transaction"], out)
    nodes.insert(1, {"id": "project-lines", "label": "Publish order line prices", "depends_on": ["input"],
                     "transform_type": "select", "config": {"columns": COLUMNS}})
    nodes[-1]["depends_on"] = ["project-lines"]
    return nodes


def main(state):
    p1 = s3.module("noon_p1", SMOKE / "business-platform-p1/smoke/smoke.py")
    p1.BASES.update(dataset=os.environ["WP23_DVS"], pipeline=os.environ["WP23_PBS"], connector=os.environ["WP23_CONNECTOR"])
    p1.SECRET_BYTES = os.environ["JWT_SECRET"].encode()
    ids = p1.build_identities()
    owner = ids["user_a"].token
    reader = p1.mint_token(org=ids["org_a"], permissions=[])
    os.environ.update(WP34_TOKEN=reader, WP34_DVS=p1.BASES["dataset"], WP34_PBS=p1.BASES["pipeline"],
                      WP34_EVIDENCE=str(state), WP34_PROJECT="noon")
    graph = s3.module("noon_graph", SMOKE / "data-integration-wp3-4/harness34.py")
    raw = p1.create_dataset(owner, "noon incoming batches", "json")["id"]
    target = p1.create_dataset(owner, "noon AE order lines", "json")["id"]
    report = dict(revision=subprocess.check_output(["git", "-C", str(HERE), "rev-parse", "HEAD"], text=True).strip(),
                  source="synthetic order_live QA replay + constructed second batch", source_dataset=raw,
                  dataset=target, store=STORE, upstream_revision="6f3103e82248bcd33cd375be5338c207298ae04b",
                  batches=[], checks=[])

    def check(label, ok):
        if not ok:
            raise AssertionError(label)
        report["checks"].append(label)
        print("PASS", label, flush=True)

    def connector(path, payload):
        result = p1.http("connector", s3.DC + path, method="POST", token=owner, payload=payload)
        assert result.status in (200, 201, 204), result.status
        return result.body

    check("create export bucket", s3.s3_request("PUT", BUCKET) in (200, 201))
    resource = f"arn:aws:s3:::{BUCKET}/{KEY}"
    writer = s3.scoped_credentials(resource, "s3:PutObject")
    source = connector("/sources", {"name":"noon replay", "connector_type":"s3",
                       "config":{"bucket":BUCKET, "sync":{"object_key":KEY}}})["id"]
    connector(f"/sources/{source}/credentials", {"kind":"aws_keys", "value":json.dumps(s3.scoped_credentials(resource, "s3:GetObject"))})
    sync = p1.create_sync(owner, source, raw)
    assert sync.status in (200,201)
    sync_id = sync.body["id"]

    def commit(batch, parent, rows):
        data = encode(rows)
        payload = dict(idempotency_key=f"noon:{raw}:{batch}", branch="main", transaction_type="SNAPSHOT", format="json",
                       metadata=dict(source_dataset=raw, source_batch=batch, parent_version=str(parent)),
                       files=[dict(logical_path="order-lines.ndjson", operation="ADD", content_type="application/x-ndjson",
                                   size_bytes=len(data), content_base64=base64.b64encode(data).decode())])
        return p1.http("dataset", f"/api/v1/datasets/{target}/outputs:commit", token=owner, payload=payload)

    def deliver(run, parent):
        delta, _, _ = p1.download_run_artifact(reader, raw, run)
        previous = graph.scan_all(target, parent)["rows"] if parent else []
        return commit(run["dataset_version_id"], parent, materialize(previous, delta))

    seed = [json.loads(line) for line in (HERE / "replay.ndjson").read_bytes().splitlines()]
    revised = dict(seed[0], offer_price=49.25, base_price=49.25, gmv_lcy=49.25)
    runs = []
    for version, delta in enumerate((seed[:3], [revised, *seed[3:]]), 1):
        path = state / f"delta-v{version}.ndjson"
        path.write_bytes(encode(delta))
        if version == 2:
            assert s3.s3_request("DELETE", f"{BUCKET}/{KEY}") == 204
            prior = graph.list_versions(raw)
            failed = p1.run_sync(owner, sync_id)
            check("unavailable second batch preserves first raw and downstream versions",
                  failed["status"] == "failed" and not failed.get("dataset_version_id") and graph.list_versions(raw) == prior
                  and len(graph.list_versions(target)) == 1 and graph.scan_all(target, 1)["rows"] == expected(1))
        assert s3.s3_request("PUT", f"{BUCKET}/{KEY}", path, credentials=writer) == 200
        run = p1.run_sync(owner, sync_id)
        assert run["status"] == "succeeded"
        downloaded, _, body = p1.download_run_artifact(reader, raw, run)
        check(f"v{version} committed source matches original delta", downloaded == delta and body == path.read_bytes())
        runs.append(run)
        result = deliver(run, version-1)
        assert result.status == 201
        txn = result.body["transaction"]["id"]
        file, = [f for f in p1.list_files(reader, target) if f["transaction_id"] == txn]
        check(f"v{version} published expected order lines", graph.version_by_txn(target, txn) == version
              and graph.scan_all(target, version)["rows"] == expected(version))
        report["batches"].append(dict(version=version, parent_version=version-1, raw_transaction=run["dataset_version_id"],
                                      sync_run=run["id"], transaction=txn, file=file["id"], row_count=len(expected(version))))

    before = graph.list_versions(target)
    replay = deliver(runs[0], 0)
    check("replay first committed batch returns original transaction",
          replay.status == 200 and replay.body["transaction"]["id"] == report["batches"][0]["transaction"])
    conflict = commit(runs[0]["dataset_version_id"], 0, expected(2))
    check("conflicting content rejected without new version", conflict.status == 409 and graph.list_versions(target) == before)
    head = p1.api("dataset", "ds_branch_get", token=reader, path_args={"id": target, "branch": "main"})
    check("v2 remains branch head after v1 replay and conflict", head.status == 200
          and head.body["head_transaction_id"] == report["batches"][1]["transaction"])
    for batch in report["batches"]:
        v = batch["version"]
        wanted = expected(v)
        response = p1.download_file(reader, target, batch["file"])
        check(f"independent reader scans and downloads exact v{v}", response.status == 200
              and response.body == encode(wanted) and graph.scan_all(target, v)["rows"] == wanted)
        (state / f"published-v{v}.ndjson").write_bytes(response.body)
        batch["sha256"] = hashlib.sha256(response.body).hexdigest()
        check(f"other tenant cannot download v{v}", p1.download_file(ids["user_b"].token, target, batch["file"]).status in (403,404))

    # One pipeline reads the published order lines, not the raw batches: v1, then v2, then v1 again.
    down = p1.create_dataset(owner, "noon AE order line prices", "json")["id"]
    pipeline = report["pipeline"] = dict(pipeline_id=None, output_dataset=down, columns=COLUMNS, builds=[])
    known = {v["transaction_id"] for v in graph.list_versions(down)}
    for ordinal, batch in enumerate((report["batches"][0], report["batches"][1], report["batches"][0]), 1):
        rows = projection(batch["version"])
        nodes = line_price_nodes(graph, target, down, batch)
        graph.TOKEN = owner
        try:
            if pipeline["pipeline_id"] is None:
                pipeline["pipeline_id"] = graph.create_pipeline("noon-line-prices", nodes)
            else:
                graph.http("PATCH", f"{graph.PBS}/api/v1/pipelines/{pipeline['pipeline_id']}", {"nodes": nodes})
            _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pipeline['pipeline_id']}")
            config = next(n["config"] for n in saved["dag"]["nodes"] if n["id"] == "input")
            run = graph.run_pipeline(pipeline["pipeline_id"])
        finally:
            graph.TOKEN = reader
        if run["status"] != "succeeded":
            print(json.dumps(run, indent=2), file=sys.stderr)
        pid = pipeline["pipeline_id"]
        check(f"pipeline run {ordinal} succeeded", run["status"] == "succeeded")
        observed = next(n["output"] for n in run["node_results"] if n["node_id"] == "input")
        check(f"pipeline run {ordinal} bound published v{batch['version']} in its saved input and node result",
              run["pipeline_id"] == pid and run["id"] == run["run_id"]
              and config["dataset_id"] == target and config["dataset_snapshot"] == nodes[0]["config"]["dataset_snapshot"]
              and observed["dataset_rid"].removeprefix("ri.foundry.main.dataset.") == target
              and observed["dataset_branch"] == "main"
              and observed["dataset_version"] == batch["version"]
              and observed["dataset_transaction_id"] == "ri.foundry.main.transaction." + batch["transaction"])
        produced, = [v for v in graph.list_versions(down) if v["transaction_id"] not in known]
        known.add(produced["transaction_id"])
        resource, = next(n for n in run["node_results"] if n["node_id"] == "output")["output_resources"]
        metadata = graph.get_output_transaction(down, produced["transaction_id"])["metadata"]
        check(f"pipeline run {ordinal} committed the projection as output v{produced['version']}",
              resource["status"] == "committed" and resource["rid"].removeprefix("ri.foundry.main.dataset.") == down
              and metadata["pipeline_rid"] == pid and metadata["transaction_rid"] == resource["transaction_rid"])
        file, = [f for f in p1.list_files(reader, down) if f.get("transaction_id") == produced["transaction_id"]]
        response = p1.download_file(reader, down, file["id"])
        check(f"pipeline run {ordinal} published the {len(rows)} expected order lines of published v{batch['version']}",
              response.status == 200 and response.body == encode(rows)
              and graph.scan_all(down, produced["version"])["rows"] == rows)
        artifact = f"pipeline-{ordinal}-input-v{batch['version']}.ndjson"
        (state / artifact).write_bytes(response.body)
        pipeline["builds"].append(dict(pipeline_run_id=run["id"], input_branch=observed["dataset_branch"],
                                       input_dataset=target, input_version=observed["dataset_version"],
                                       input_transaction=batch["transaction"], output_version=produced["version"],
                                       output_transaction=produced["transaction_id"],
                                       pipeline_output_transaction_rid=resource["transaction_rid"],
                                       output_file_id=file["id"], artifact=artifact, rows=rows,
                                       row_count=len(rows), sha256=hashlib.sha256(response.body).hexdigest()))

    first, latest, replay = pipeline["builds"]
    versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(down)}
    for build in pipeline["builds"]:
        wanted = projection(build["input_version"])
        response = p1.download_file(reader, down, build["output_file_id"])
        check(f"independent reader scans and downloads exact pipeline output v{build['output_version']}",
              versions[build["output_version"]] == build["output_transaction"] and response.status == 200
              and response.body == encode(wanted) and graph.scan_all(down, build["output_version"])["rows"] == wanted)
    check("three pipeline runs published distinct output versions and transactions",
          [b["input_version"] for b in pipeline["builds"]] == [1, 2, 1] and sorted(versions) == [1, 2, 3]
          and all(len({b[field] for b in pipeline["builds"]}) == 3
                  for field in ("pipeline_run_id", "output_version", "output_transaction", "output_file_id")))
    check("v1 replay matches the first v1 bytes and differs from v2",
          first["input_transaction"] == replay["input_transaction"] != latest["input_transaction"]
          and first["sha256"] == replay["sha256"] != latest["sha256"]
          and (state / first["artifact"]).read_bytes() == (state / replay["artifact"]).read_bytes()
          and (state / first["artifact"]).read_bytes() != (state / latest["artifact"]).read_bytes())
    s3.write_json(state / "noon-report.json", report)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
