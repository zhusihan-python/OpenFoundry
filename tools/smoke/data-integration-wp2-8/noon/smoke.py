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
from urllib.parse import urlencode

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


# Order lines are published under one fixed contract: these seven columns as
# JSON strings, the unit price included. encode() writes sorted keys, which is
# this order, so the versioned schema answers it as declared.
ORDER_LINE_COLUMNS = ["currency", "item_nr", "market", "order_nr", "sku", "store", "unit_price"]
ORDER_LINE_SCHEMA = [{"name": name, "field_type": "STRING", "nullable": False} for name in ORDER_LINE_COLUMNS]


def published_schema(rows):
    """The typed column contract every order-line snapshot is published under.

    Each row must have exactly the contract columns, all strings, so an empty
    snapshot declares the same columns as any other.
    """
    for row in rows:
        if set(row) != set(ORDER_LINE_COLUMNS) or not all(isinstance(value, str) for value in row.values()):
            raise ValueError("published order lines must have exactly the contract columns, all strings")
    return {"file_format": "TEXT", "fields": [{"name": name, "type": "STRING", "nullable": False} for name in ORDER_LINE_COLUMNS]}


def columns_of(data):
    """The ordered typed columns an independent reader finds in published NDJSON bytes."""
    rows = [json.loads(line) for line in data.splitlines()]
    names = list(rows[0])
    nullable = {name for row in rows for name, value in row.items() if value is None}
    if any(list(row) != names for row in rows):
        raise ValueError("published order lines have different column orders")
    if not all(isinstance(value, str) for row in rows for value in row.values()):
        raise ValueError("published order lines are not all strings")
    return [{"name": name, "field_type": "STRING", "nullable": name in nullable} for name in names]


def numeric_unit_price(version):
    """The same order lines with the unit price as a JSON number instead of a string."""
    return [dict(row, unit_price=float(row["unit_price"])) for row in expected(version)]


def numeric_unit_price_schema(rows):
    """The publish contract with one legitimate change: the unit price is a
    number in the declaration and in every row, the other columns as before.
    """
    for row in rows:
        if set(row) != set(ORDER_LINE_COLUMNS) or not all(
                type(row[name]) in (int, float) if name == "unit_price" else isinstance(row[name], str) for name in row):
            raise ValueError("numeric order lines must have the contract columns, the unit price a number and the rest strings")
    return {"file_format": "TEXT",
            "fields": [{"name": name, "type": "DOUBLE" if name == "unit_price" else "STRING", "nullable": False}
                       for name in ORDER_LINE_COLUMNS]}


def schema_route(dataset, version, **params):
    """The formal versioned schema request for one dataset version on main."""
    return f"/v1/datasets/{dataset}/schema?" + urlencode({"branch": "main", "version": version, **params})


def line_price_nodes(graph, source, out, published, expected_schema_hash=None):
    nodes = graph.graph_nodes(source, published["version"], published["transaction"], out)
    if expected_schema_hash is not None:
        # The pin also requires the committed schema of that version to
        # have this content hash. The pipeline service proves it against
        # the dataset service as the caller before it reads any row.
        nodes[0]["config"]["dataset_snapshot"]["expected_schema_hash"] = expected_schema_hash
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

    def commit(batch, parent, rows, declare=published_schema):
        data = encode(rows)
        payload = dict(idempotency_key=f"noon:{raw}:{batch}", branch="main", transaction_type="SNAPSHOT", format="json",
                       metadata=dict(source_dataset=raw, source_batch=batch, parent_version=str(parent)),
                       files=[dict(logical_path="order-lines.ndjson", operation="ADD", content_type="application/x-ndjson",
                                   size_bytes=len(data), content_base64=base64.b64encode(data).decode())],
                       schema=declare(rows))
        return p1.http("dataset", f"/api/v1/datasets/{target}/outputs:commit", token=owner, payload=payload)

    def deliver(run, parent):
        delta, _, _ = p1.download_run_artifact(reader, raw, run)
        previous = graph.scan_all(target, parent)["rows"] if parent else []
        return commit(run["dataset_version_id"], parent, materialize(previous, delta))

    seed = [json.loads(line) for line in (HERE / "replay.ndjson").read_bytes().splitlines()]
    revised = dict(seed[0], offer_price=49.25, base_price=49.25, gmv_lcy=49.25)
    # The reader holds no write permission. It asks the formal versioned schema
    # route of the order lines and checks each answer against an independent
    # expectation, never against the publisher's declaration.
    schema = report["schema"] = dict(dataset=target, columns=ORDER_LINE_COLUMNS, reads=[], answers=[])
    declared = {}

    def schema_answer(version, name, **params):
        result = p1.http("dataset", schema_route(target, version, **params), token=reader, raw=True)
        (state / f"schema-{name}.json").write_bytes(result.body)
        schema["answers"].append(dict(name=name, version=version, **params, status=result.status, artifact=f"schema-{name}.json"))
        return result

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
        # The first iteration reads v1 before the second publication, the second reads v2 after it.
        read = schema_answer(version, f"v{version}-first")
        body = json.loads(read.body) if read.status == 200 else {}
        check(f"independent reader reads the declared schema of order line v{version}", read.status == 200
              and (body.get("dataset_id"), body.get("branch"), body.get("version"), body.get("transaction_id")) == (target, "main", version, txn)
              and body.get("fields") == ORDER_LINE_SCHEMA
              and body.get("content_hash") == hashlib.sha256(json.dumps(body["fields"], separators=(",", ":")).encode()).hexdigest())
        declared[version] = read.body
        schema["reads"].append(dict(version=version, transaction=txn, content_hash=body["content_hash"]))

    first_batch, second_batch = report["batches"][0], report["batches"][1]
    again = schema_answer(1, "v1-again")
    check("order line schema of v1 answers as before while v2 is the head", again.status == 200 and again.body == declared[1])
    check("v2 changed row values and kept the declared columns and schema hash",
          encode(expected(1)) != encode(expected(2))
          and [json.loads(declared[v])["fields"] for v in (1, 2)] == [ORDER_LINE_SCHEMA] * 2
          and json.loads(declared[1])["content_hash"] == json.loads(declared[2])["content_hash"])
    for name, version, other in (("v1-with-v2-transaction", 1, second_batch), ("v2-with-v1-transaction", 2, first_batch)):
        crossed = schema_answer(version, name, transaction_id=other["transaction"])
        check(f"order line schema of v{version} refuses the transaction of the other version", crossed.status == 409
              and json.loads(crossed.body).get("code") == "dataset_schema_transaction_mismatch" and b'"fields"' not in crossed.body)
    digest = json.loads(declared[2])["content_hash"]
    bound = schema_answer(2, "v2-bound", transaction_id=second_batch["transaction"], expected_content_hash=digest)
    check("order line schema of v2 accepts its own transaction and content hash", bound.status == 200 and bound.body == declared[2])
    stale = schema_answer(2, "v2-other-hash", expected_content_hash=hashlib.sha256(b"[]").hexdigest())
    check("order line schema of v2 refuses another content hash", stale.status == 412
          and json.loads(stale.body).get("code") == "dataset_schema_content_hash_mismatch" and b'"fields"' not in stale.body)
    foreign = p1.http("dataset", schema_route(target, 1), token=ids["user_b"].token, raw=True)
    check("other tenant cannot read the order line schema", foreign.status in (403, 404))

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
        check(f"declared schema of order line v{v} matches the columns of the rows the reader downloaded",
              columns_of(response.body) == ORDER_LINE_SCHEMA == json.loads(declared[v])["fields"])
        batch["sha256"] = hashlib.sha256(response.body).hexdigest()
        check(f"other tenant cannot download v{v}", p1.download_file(ids["user_b"].token, target, batch["file"]).status in (403,404))

    # One pipeline reads the published order lines, not the raw batches: v1, then v2, then v1 again.
    down = p1.create_dataset(owner, "noon AE order line prices", "json")["id"]
    pipeline = report["pipeline"] = dict(pipeline_id=None, output_dataset=down, columns=COLUMNS, expected_schema_hash=digest, builds=[])
    known = {v["transaction_id"] for v in graph.list_versions(down)}
    for ordinal, batch in enumerate((report["batches"][0], report["batches"][1], report["batches"][0]), 1):
        rows = projection(batch["version"])
        nodes = line_price_nodes(graph, target, down, batch, digest)
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
        check(f"pipeline run {ordinal} pins the seven-column schema hash the reader read for published v{batch['version']}",
              config["dataset_snapshot"].get("expected_schema_hash") == digest
              and json.loads(declared[batch["version"]])["content_hash"] == digest)
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
                                       input_transaction=batch["transaction"], input_schema_hash=config["dataset_snapshot"].get("expected_schema_hash"),
                                       output_version=produced["version"],
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

    # The connector's raw batch and the six-column projection are NDJSON
    # without a schema in a commit receipt. Both keep their unsupported answer.
    for name, dataset, version in (("raw-batch", raw, 1), ("projection", down, first["output_version"])):
        result = p1.http("dataset", schema_route(dataset, version), token=reader, raw=True)
        (state / f"schema-unsupported-{name}.json").write_bytes(result.body)
        check(f"versioned schema of the {name} without a schema in a commit receipt stays unsupported", result.status == 422
              and json.loads(result.body).get("error", "").startswith("dataset_schema_format_unsupported")
              and b'"fields"' not in result.body)
        schema["answers"].append(dict(name=f"unsupported-{name}", dataset=dataset, version=version, status=result.status,
                                      artifact=f"schema-unsupported-{name}.json"))
    # The pin carries the schema precondition. A third publication of the
    # order lines legitimately changes the contract. The same graph keeps
    # the original hash when it is bound to that version.
    gate = report["schema_gate"] = dict(dataset=target, pipeline_id=pid, output_dataset=down, expected_schema_hash=digest)
    latest = pipeline["builds"][-1]

    def last_good():
        """The reader maps the last good output and its input version to their
        transactions, downloads both files and asks the schema of the input
        bound to its transaction and the pinned hash.
        """
        output = p1.download_file(reader, down, latest["output_file_id"])
        source = p1.download_file(reader, target, second_batch["file"])
        answer = p1.http("dataset", schema_route(target, 2, transaction_id=second_batch["transaction"], expected_content_hash=digest),
                         token=reader, raw=True)
        return dict(output_versions={v["version"]: v["transaction_id"] for v in graph.list_versions(down)},
                    input_versions={v["version"]: v["transaction_id"] for v in graph.list_versions(target)},
                    output_status=output.status, output_sha256=hashlib.sha256(output.body).hexdigest(),
                    input_status=source.status, input_sha256=hashlib.sha256(source.body).hexdigest(),
                    input_schema_status=answer.status, input_schema_sha256=hashlib.sha256(answer.body).hexdigest())

    # A legitimate third publication: the unit price is a number in the
    # declaration and in every stored row. The service accepts it, and the
    # schema hash of that version differs from the one the graph pins.
    numeric = numeric_unit_price(2)
    third = commit("numeric-unit-price", 2, numeric, declare=numeric_unit_price_schema)
    check("order line v3 with a numeric unit price publishes as a legitimate version", third.status == 201)
    third_txn = third.body["transaction"]["id"]
    third_file, = [f for f in p1.list_files(reader, target) if f["transaction_id"] == third_txn]
    v3 = schema_answer(3, "v3-numeric", transaction_id=third_txn)
    answer = json.loads(v3.body) if v3.status == 200 else {}
    response = p1.download_file(reader, target, third_file["id"])
    (state / "published-v3-numeric.ndjson").write_bytes(response.body)
    gate["third"] = dict(version=3, transaction=third_txn, file=third_file["id"], rows=len(numeric),
                         sha256=hashlib.sha256(response.body).hexdigest(), schema_status=v3.status, content_hash=answer.get("content_hash"))
    check("order line v3 declares the unit price as DOUBLE and stores it as a number, so its schema hash differs",
          v3.status == 200 and graph.version_by_txn(target, third_txn) == 3
          and (answer.get("dataset_id"), answer.get("branch"), answer.get("version"), answer.get("transaction_id")) == (target, "main", 3, third_txn)
          and answer.get("fields") == [dict(name=f["name"], field_type=f["type"], nullable=f["nullable"])
                                       for f in numeric_unit_price_schema(numeric)["fields"]]
          and answer["content_hash"] == hashlib.sha256(json.dumps(answer["fields"], separators=(",", ":")).encode()).hexdigest()
          and answer["content_hash"] != digest and response.status == 200 and response.body == encode(numeric)
          and all(type(json.loads(line)["unit_price"]) is float for line in response.body.splitlines()))

    # The two snapshots straddle the refused run only: v3 is already the head,
    # and v2 is still the last version that answers the pinned hash.
    before = last_good()
    gate["before"] = before
    check("before the refusal the reader maps the last good output v3 and input v2 to their transactions and downloads both",
          before["output_versions"] == versions
          and before["output_versions"][latest["output_version"]] == latest["output_transaction"]
          and before["input_versions"] == {**{b["version"]: b["transaction"] for b in report["batches"]}, 3: third_txn}
          and before["output_status"] == before["input_status"] == before["input_schema_status"] == 200
          and before["output_sha256"] == latest["sha256"] and before["input_sha256"] == second_batch["sha256"]
          and before["input_schema_sha256"] == hashlib.sha256(declared[2]).hexdigest())

    # The same graph, the same hash, now bound to v3. The pipeline service
    # must refuse the input for the schema before it reads a row.
    nodes = line_price_nodes(graph, target, down, dict(version=3, transaction=third_txn), digest)
    before_outputs, before_inputs = graph.list_versions(down), graph.list_versions(target)
    graph.TOKEN = owner
    try:
        graph.http("PATCH", f"{graph.PBS}/api/v1/pipelines/{pid}", {"nodes": nodes})
        _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pid}")
        saved_input = next(n["config"] for n in saved["dag"]["nodes"] if n["id"] == "input")
        refused = graph.run_pipeline(pid)
    finally:
        graph.TOKEN = reader
    (state / "schema-gate-refused-run.json").write_bytes(json.dumps(refused, indent=2).encode())
    results = refused.get("node_results") or []
    input_result = next((n for n in results if n["node_id"] == "input"), {})
    input_error = input_result.get("error") or ""
    gate["refused"] = dict(run_id=refused.get("run_id"), status=refused["status"], saved_input=saved_input,
                           nodes=[dict(node_id=n["node_id"], status=n["status"], error=n.get("error"),
                                       output_resources=[r["status"] for r in n.get("output_resources") or []]) for n in results])
    check("the same graph keeps the original schema hash when it is bound to v3",
          saved_input["dataset_id"] == target and saved_input["dataset_snapshot"] == nodes[0]["config"]["dataset_snapshot"]
          and saved_input["dataset_snapshot"]["expected_schema_hash"] == digest)
    # Both codes must be in the input node's own error: the run only reports "failed".
    check("the run bound to v3 is refused for the schema hash and publishes nothing",
          refused["status"] == "failed"
          and {n["node_id"]: n["status"] for n in results} == {"input": "failed", "project-lines": "cancelled", "output": "cancelled"}
          and "dataset_input_schema_rejected" in input_error and "dataset_schema_content_hash_mismatch" in input_error
          and not [r for n in results for r in n.get("output_resources") or [] if r["status"] == "committed"]
          and graph.list_versions(down) == before_outputs and graph.list_versions(target) == before_inputs)
    after = last_good()
    gate["after"] = after
    check("after the refusal the reader maps and downloads the same last good output and input",
          after == before and graph.list_versions(down) == before_outputs)
    head = p1.api("dataset", "ds_branch_get", token=reader, path_args={"id": target, "branch": "main"})
    check("the refused run leaves the pinned versions readable and v3 the head",
          head.status == 200 and head.body["head_transaction_id"] == third_txn
          and graph.scan_all(target, 1)["rows"] == expected(1) and graph.scan_all(target, 2)["rows"] == expected(2)
          and graph.scan_all(target, 3)["rows"] == numeric)

    s3.write_json(state / "noon-report.json", report)
    return dict(reader=reader, other=ids["user_b"].token, p1=p1, graph=graph)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
