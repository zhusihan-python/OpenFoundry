"""Offline Lingxing product batches through real Connector, Dataset Versioning and a pinned Pipeline."""
import hashlib
import importlib.util
import json
import os
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlencode
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
# The product contract as the versioned schema states it, and the SHA256 of
# that field list: the content hash every product version published under it
# answers, whatever its rows.
CONTRACT = [{"name": field["name"], "field_type": field["type"], "nullable": field["nullable"]}
            for field in products.PRODUCT_SCHEMA]
PRODUCT_SCHEMA_SHA256 = hashlib.sha256(json.dumps(CONTRACT, separators=(",", ":")).encode()).hexdigest()


def contract_of(data):
    """The ordered typed columns an independent reader finds in published NDJSON bytes."""
    rows = [json.loads(line) for line in data.splitlines()]
    names = list(rows[0])
    if any(list(row) != names for row in rows) or set(names) != products.PRODUCT_COLUMNS:
        raise ValueError("published products have other columns or a different column order")
    if not all(type(row["id"]) is int and isinstance(row["sku"], str) and isinstance(row["product_name"], str)
               and isinstance(row["cg_price"], str) for row in rows):
        raise ValueError("published products do not keep the id an integer and the SKU, name and cost strings")
    return [{"name": name, "field_type": "LONG" if name == "id" else "STRING", "nullable": False} for name in names]


def schema_route(dataset, version, **params):
    """The formal versioned schema request for one dataset version on main."""
    return f"/v1/datasets/{dataset}/schema?" + urlencode({"branch": "main", "version": version, **params})


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


def commit_request(batch, dataset, data, rows):
    """The Dataset request for one delivery of a batch, in the shape its first publication used.

    The Dataset service fingerprints the exact request, so a redelivery has to
    rebuild the request of the first publication. A batch carries the contract
    it was first published under: one published with it declares the schema of
    its rows, and one published before it is sent as it was then, without one.
    """
    schema = products.published_schema(rows) if batch.get("contract") else None
    return products.commit_body(dataset, batch["source_batch"], data, schema=schema)


def cost_nodes(graph, source, out, published, expected_schema_hash=None):
    nodes = graph.graph_nodes(source, published["output_version"], published["output_transaction"], out)
    if expected_schema_hash is not None:
        # The pin also requires the committed schema of that version to
        # have this content hash. The pipeline service proves it against
        # the dataset service as the caller before it reads any row.
        nodes[0]["config"]["dataset_snapshot"]["expected_schema_hash"] = expected_schema_hash
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

    def deliver(batch, dataset=target):
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
        body = commit_request(batch, raw, data, rows)
        wire = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        result = p1.http("dataset", f"/api/v1/datasets/{dataset}/outputs:commit", token=owner, payload=wire)
        if result.status in (200, 201):
            receipt = result.body
            assert (receipt["dataset_id"], receipt["branch"], receipt["idempotency_key"],
                    receipt["request_fingerprint"], receipt["transaction"]["status"]) == (
                        dataset, "main", body["idempotency_key"], sha(wire), "COMMITTED")
        return result

    def head_of(dataset):
        result = p1.api("dataset", "ds_branch_get", token=reader, path_args={"id": dataset, "branch": "main"})
        assert result.status == 200, result.status
        return result.body["head_transaction_id"]

    check("create private export bucket", s3.s3_request("PUT", BUCKET) in (200, 201))
    for i, expected in enumerate((FIRST, SECOND), 1):
        fetch, offsets = pages(expected)
        batch = receive(str(uuid.uuid4()), fetch)
        check(f"v{i} page offsets", offsets == ([0, 2] if i == 1 else [0]))
        # Published under the input contract: every later delivery of this batch rebuilds that request.
        batch["contract"] = True
        result = deliver(batch)
        check(f"v{i} first publish", result.status == 201)
        txn = result.body["transaction"]["id"]
        batch["request_fingerprint"] = result.body["request_fingerprint"]
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
    check("v1 redelivery rebuilds the request of its first publication, the schema included",
          replay.status == 200 and replay.body["request_fingerprint"] == first["request_fingerprint"])
    conflict = deliver(dict(second, source_batch=first["source_batch"]))
    check("changed content under existing batch rejected", conflict.status == 409)
    report["replay_transaction"] = replay.body["transaction"]["id"]

    # Compatibility boundary of the input contract. A batch published before it
    # existed was sent without a schema, and that request is not rewritten: its
    # bytes still replay to the original receipt, while the same batch key sent
    # with the contract schema is another request, which the key refuses. Such a
    # version keeps its rows readable by version and answers no versioned schema.
    legacy = report["legacy"] = dict(
        dataset=p1.create_dataset(owner, "Lingxing product snapshots before the input contract", "json")["id"])
    before_contract = dict(first, contract=False)
    old = deliver(before_contract, dataset=legacy["dataset"])
    check("a batch published as before the contract commits without a schema", old.status == 201)
    old_txn = legacy["transaction"] = old.body["transaction"]["id"]
    old_versions = graph.list_versions(legacy["dataset"])
    same = deliver(before_contract, dataset=legacy["dataset"])
    check("the request of a batch published before the contract still replays to its original receipt",
          same.status == 200 and same.body["transaction"]["id"] == old_txn
          and same.body["request_fingerprint"] == old.body["request_fingerprint"])
    rewritten = deliver(first, dataset=legacy["dataset"])
    s3.write_json(state / "legacy-rewritten-replay.json",
                  dict(status=rewritten.status, body=rewritten.body, versions_after=graph.list_versions(legacy["dataset"])))
    legacy.update(rewritten_replay_status=rewritten.status, rewritten_replay_code=rewritten.body.get("code"),
                  request_fingerprint=old.body["request_fingerprint"])
    check("the same batch key sent with the contract schema is refused and creates no version",
          rewritten.status == 409 and rewritten.body.get("code") == "IDEMPOTENCY_KEY_CONFLICT"
          and graph.list_versions(legacy["dataset"]) == old_versions and head_of(legacy["dataset"]) == old_txn)
    unsupported = p1.http("dataset", schema_route(legacy["dataset"], 1, transaction_id=old_txn), token=reader, raw=True)
    (state / "schema-legacy-unsupported.json").write_bytes(unsupported.body)
    legacy["schema_status"] = unsupported.status
    check("the version published before the contract answers no versioned schema and keeps its rows readable",
          unsupported.status == 422
          and json.loads(unsupported.body).get("error", "").startswith("dataset_schema_format_unsupported")
          and b'"fields"' not in unsupported.body and graph.scan_all(legacy["dataset"], 1)["rows"] == FIRST)
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

    # An independent reader holds no write permission. It asks the formal versioned
    # schema of each published version, bound to the transaction that cut it, and
    # verifies the hash itself.
    schema = report["schema"] = dict(dataset=target, fields=CONTRACT, content_hash=PRODUCT_SCHEMA_SHA256, reads=[], answers=[])
    answered = {}

    def schema_answer(version, name, **params):
        result = p1.http("dataset", schema_route(target, version, **params), token=reader, raw=True)
        (state / f"schema-{name}.json").write_bytes(result.body)
        schema["answers"].append(dict(name=name, version=version, **params, status=result.status, artifact=f"schema-{name}.json"))
        return result

    for batch in report["batches"]:
        version, txn = batch["output_version"], batch["output_transaction"]
        read = schema_answer(version, f"v{version}-bound", transaction_id=txn, expected_content_hash=PRODUCT_SCHEMA_SHA256)
        answered[version] = read.body
        body = json.loads(read.body) if read.status == 200 else {}
        check(f"independent reader reads the contract of product v{version} bound to its transaction and hash",
              read.status == 200
              and (body.get("dataset_id"), body.get("branch"), body.get("version"), body.get("transaction_id")) == (
                  target, "main", version, txn)
              and body.get("fields") == CONTRACT
              and body.get("content_hash") == hashlib.sha256(
                  json.dumps(body["fields"], separators=(",", ":")).encode()).hexdigest() == PRODUCT_SCHEMA_SHA256)
        check(f"the contract declares the SKU and the cost of product v{version} as strings",
              {field["name"]: field["field_type"] for field in body.get("fields", [])}
              == {"cg_price": "STRING", "id": "LONG", "product_name": "STRING", "sku": "STRING"})
        check(f"the contract a reader derives from the bytes of product v{version} is the schema it was answered",
              contract_of((state / f"published-v{version}.ndjson").read_bytes()) == body.get("fields"))
        schema["reads"].append(dict(version=version, transaction=txn, content_hash=body.get("content_hash")))
    again = schema_answer(1, "v1-again")
    check("the schema of v1 answers as before while v2 is the head", again.status == 200 and again.body == answered[1])
    for version, other in ((1, second), (2, first)):
        crossed = schema_answer(version, f"v{version}-with-v{other['output_version']}-transaction",
                                transaction_id=other["output_transaction"], expected_content_hash=PRODUCT_SCHEMA_SHA256)
        check(f"the schema of v{version} refuses the transaction of v{other['output_version']}", crossed.status == 409
              and json.loads(crossed.body).get("code") == "dataset_schema_transaction_mismatch"
              and b'"fields"' not in crossed.body)
    stale = schema_answer(2, "v2-other-hash", transaction_id=second["output_transaction"],
                          expected_content_hash=hashlib.sha256(b"[]").hexdigest())
    check("the schema of v2 refuses another content hash", stale.status == 412
          and json.loads(stale.body).get("code") == "dataset_schema_content_hash_mismatch"
          and b'"fields"' not in stale.body)
    foreign_schema = p1.http("dataset", schema_route(target, 1, transaction_id=first["output_transaction"]),
                             token=foreign, raw=True)
    schema["other_tenant_status"] = foreign_schema.status
    check("the same user in another tenant cannot read the product schema", foreign_schema.status in (403, 404))

    # One pipeline reads the published product snapshots, not the raw batches: v1, then v2, then v1 again.
    down = p1.create_dataset(owner, "Lingxing product costs", "json")["id"]
    # Two different identifiers name one published output. Every build records both.
    pipeline = report["pipeline"] = dict(
        pipeline_id=None, output_dataset=down, columns=COSTS, expected_sha256=COST_SHA256,
        expected_schema_hash=PRODUCT_SCHEMA_SHA256, builds=[],
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
               "transaction_id": "ri.foundry.main.transaction." + batch["output_transaction"],
               "expected_schema_hash": PRODUCT_SCHEMA_SHA256}
        heads = dict(input=head_of(target), output=head_of(down) if pipeline["builds"] else None)
        nodes = cost_nodes(graph, target, down, batch, expected_schema_hash=PRODUCT_SCHEMA_SHA256)
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
        check(f"pipeline run {ordinal} binds its input to the schema hash the reader verified for product v{version}",
              config["dataset_snapshot"].get("expected_schema_hash") == PRODUCT_SCHEMA_SHA256
              and next(r for r in schema["reads"] if r["version"] == version) == dict(
                  version=version, transaction=batch["output_transaction"], content_hash=PRODUCT_SCHEMA_SHA256))
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
                                       input_schema_hash=config["dataset_snapshot"].get("expected_schema_hash"),
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
    # Negative controls of the bound input. Each saves and runs a Pipeline of
    # its own, so the graph of the three runs keeps its last good pin, and each
    # must fail at the input before a row is read and publish no version.
    refusals = pipeline["refusals"] = {}
    outputs_before, inputs_before = graph.list_versions(down), graph.list_versions(target)

    def control(tag, nodes, token=owner, out=None):
        """Save and run one graph as a token. Return the saved input, the run and the output versions."""
        graph.TOKEN = token
        try:
            control_id = graph.create_pipeline(f"lingxing-product-costs-{tag}", nodes)
            _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{control_id}")
            run = graph.run_pipeline(control_id)
            versions = graph.list_versions(out) if out else None
        finally:
            graph.TOKEN = reader
        s3.write_json(state / f"refused-{tag}-graph.json", saved)
        s3.write_json(state / f"refused-{tag}-run.json", run)
        results = run.get("node_results") or []
        return dict(pipeline_id=control_id, status=run["status"],
                    saved_input=next(n["config"] for n in saved["dag"]["nodes"] if n["id"] == "input"),
                    nodes=[dict(node_id=n["node_id"], status=n["status"], error=n.get("error")) for n in results],
                    committed=[resource for n in results for resource in n.get("output_resources") or []
                               if resource["status"] == "committed"],
                    output_versions=versions)

    def refused_at_input(outcome, *codes):
        """The input node failed with these codes; the nodes after it never ran and nothing was committed."""
        states = {n["node_id"]: n["status"] for n in outcome["nodes"]}
        error = next((n["error"] or "" for n in outcome["nodes"] if n["node_id"] == "input"), "")
        return (outcome["status"] == "failed"
                and states == {"input": "failed", "project-costs": "cancelled", "output": "cancelled"}
                and all(code in error for code in ("dataset_input_load_failed", "dataset_input_schema_rejected", *codes))
                and not outcome["committed"])

    # The pin names v1 together with the transaction that cut v2.
    crossed = refusals["crossed_transaction"] = control(
        "crossed-transaction", cost_nodes(graph, target, down, dict(second, output_version=first["output_version"]),
                                          expected_schema_hash=PRODUCT_SCHEMA_SHA256))
    check("the pin that names v1 with the transaction of v2 is saved as declared",
          crossed["saved_input"]["dataset_id"] == target and crossed["saved_input"]["dataset_snapshot"] == {
              "branch": "main", "version": first["output_version"],
              "transaction_id": "ri.foundry.main.transaction." + second["output_transaction"],
              "expected_schema_hash": PRODUCT_SCHEMA_SHA256})
    check("the run bound to v1 with the transaction of v2 is refused at the input and publishes nothing",
          refused_at_input(crossed, "dataset_schema_transaction_mismatch")
          and graph.list_versions(down) == outputs_before and graph.list_versions(target) == inputs_before)

    # The same user in another organization saves the same graph over an output dataset of its own.
    other_down = p1.create_dataset(foreign, "Lingxing product costs of another tenant", "json")["id"]
    other = refusals["other_tenant"] = control(
        "other-tenant", cost_nodes(graph, target, other_down, second, expected_schema_hash=PRODUCT_SCHEMA_SHA256),
        token=foreign, out=other_down)
    other["output_dataset"] = other_down
    # The Dataset service answers the other organization with a denial, not with the dataset.
    other_error = next((n["error"] or "" for n in other["nodes"] if n["node_id"] == "input"), "")
    check("the same user in another organization is refused at the input of this product dataset and publishes nothing",
          other["saved_input"]["dataset_id"] == target and other["saved_input"]["dataset_snapshot"] == {
              "branch": "main", "version": second["output_version"],
              "transaction_id": "ri.foundry.main.transaction." + second["output_transaction"],
              "expected_schema_hash": PRODUCT_SCHEMA_SHA256}
          and ("batched_dataset_403" in other_error or "batched_dataset_404" in other_error)
          and refused_at_input(other) and other["output_versions"] == []
          and graph.list_versions(down) == outputs_before and graph.list_versions(target) == inputs_before)
    pipeline["versions_around_refusals"] = dict(
        output_before=outputs_before, output_after=graph.list_versions(down),
        input_before=inputs_before, input_after=graph.list_versions(target))
    s3.write_json(state / "product-report.json", report)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
