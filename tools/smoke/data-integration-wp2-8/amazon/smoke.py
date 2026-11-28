"""Simulated Amazon monthly transaction_view handoff through S3 Connector and versioned Dataset APIs."""
import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
from urllib.parse import urlencode

HERE = Path(__file__).resolve().parent
SMOKE = HERE.parents[1]
spec = importlib.util.spec_from_file_location("wp23_s3", SMOKE / "data-integration-wp2-3/orders.py")
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)
reports = s3.module("amazon_reports", HERE / "reports.py")
transactions = s3.module("amazon_transactions", HERE / "transactions.py")
BUCKET = "amazon-monthly"
# (period, fixture): October holds two files, as one upstream row can.
FILES = (("2026-09", "2026-09"), ("2026-10", "2026-10"), ("2026-10", "2026-10-2"))
# The negative report of the schema gate, derived at the end from the second October file. It has its own
# fileId like any file, and is not one of FILES.
NEGATIVE = "2026-10-text-total"
# The fileIds of the simulated upload receipts. One month is not one file.
FILE_IDS = {name: str(uuid.uuid5(uuid.NAMESPACE_URL, f"openfoundry-smoke/amazon/{name}"))
            for _, name in (*FILES, ("2026-10", NEGATIVE))}


def main(state):
    p1 = s3.module("wp28_p1", SMOKE / "business-platform-p1/smoke/smoke.py")
    p1.BASES.update(dataset=os.environ["WP23_DVS"], connector=os.environ["WP23_CONNECTOR"])
    p1.SECRET_BYTES = os.environ["JWT_SECRET"].encode()
    ids = p1.build_identities()
    owner = ids["user_a"].token
    reader = p1.mint_token(org=ids["org_a"], permissions=[])
    foreign = ids["user_b"].token
    received = p1.create_dataset(owner, "Amazon received monthly reports", "json")["id"]
    files_ds = p1.create_dataset(owner, "Amazon US transaction views", "csv")["id"]
    status = p1.create_dataset(owner, "Amazon US monthly report status", "json")["id"]
    report = {"revision": subprocess.check_output(["git", "-C", str(HERE), "rev-parse", "HEAD"], text=True).strip(),
              "source_mode": "synthetic CSV files, registrations and receipts", "upstream_revision": "6f3103e",
              "organization": ids["org_a"], "store": reports.STORE, "marketplace": "amazon-us",
              "received_dataset": received, "report_dataset": files_ds, "status_dataset": status,
              "checks": [], "files": [], "refused": []}
    originals = {name: (HERE / "fixtures" / f"transaction_view-{name}.csv").read_bytes() for _, name in FILES}
    reported = dict(storeId=reports.STORE, marketplace="amazon-us", reportKind="transaction_view", period="2026-08",
                    checkedAt=datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))

    def check(label, condition):
        if not condition:
            raise AssertionError(label)
        print("PASS", label, flush=True)
        report["checks"].append(label)

    def post(path, payload):
        result = p1.http("connector", s3.DC + path, method="POST", token=owner, payload=payload)
        assert result.status in (200, 201, 204), result.status
        return result.body

    def versions(dataset):
        listed = p1.http("dataset", f"/v1/datasets/{dataset}/versions?limit=1000", token=reader)
        assert listed.status == 200, listed.status
        return [(v["version"], v["transaction_id"]) for v in listed.body.get("data") or []]

    def head(dataset):
        branch = p1.api("dataset", "ds_branch_get", path_args={"id": dataset, "branch": "main"}, token=reader)
        return branch.body["head_transaction_id"]

    def index(dataset):
        """(path, content type) of the files at the head. Downloads themselves are octet-stream."""
        listed = p1.http("dataset", f"/v1/datasets/{dataset}/files/index", token=reader)
        assert listed.status == 200, listed.status
        return [(entry["path"], entry["content_type"]) for entry in listed.body]

    def collect(period, delivery):
        """A real S3 Connector reads one key with a credential that can read that key only."""
        key = reports.object_key(period, delivery)
        source = post("/sources", {"name": f"amazon {period} {delivery}", "connector_type": "s3",
                                   "config": {"bucket": BUCKET, "sync": {"object_key": key}}})["id"]
        post(f"/sources/{source}/credentials", {"kind": "aws_keys", "value": json.dumps(
            s3.scoped_credentials(f"arn:aws:s3:::{BUCKET}/{key}", "s3:GetObject"))})
        return p1.run_sync(owner, post("/syncs", {"source_id": source, "output_dataset_id": received})["id"])

    def deliver(period, delivery, body):
        """Stage one envelope under its own key, then let the connector commit it."""
        key = reports.object_key(period, delivery)
        path = state / f"envelope-{period}-{re.sub(r'[^a-z0-9]+', '-', delivery.lower())}.ndjson"
        path.write_bytes(body)
        writer = s3.scoped_credentials(f"arn:aws:s3:::{BUCKET}/{key}", "s3:PutObject")
        assert s3.s3_request("PUT", f"{BUCKET}/{key}", path, credentials=writer) == 200
        run = collect(period, delivery)
        assert run["status"] == "succeeded", run
        return run

    def uploaded(name):
        return {"status": "uploaded", "fileId": FILE_IDS[name]}

    def file_envelope(period, data, receipt, registered=None, **changes):
        """A file envelope as delivered: its registration and the receipt the sender kept."""
        record = dict(reports.registration(data if registered is None else registered, period), **changes)
        return reports.envelope("report", registration=record, content_base64=base64.b64encode(data).decode(),
                                receipt=receipt)

    def commit(dataset, body, token=owner, headers=None):
        wire = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        result = p1.http("dataset", f"/api/v1/datasets/{dataset}/outputs:commit", token=token, payload=wire,
                         headers=headers)
        if result.status in (200, 201):
            receipt = result.body
            assert (receipt["idempotency_key"], receipt["request_fingerprint"], receipt["transaction"]["status"]) == (
                body["idempotency_key"], reports.sha256(wire), "COMMITTED")
        return result

    def handoff(run, period):
        """Read the committed envelope. Its receipt and registration decide what may be published."""
        _, _, body = p1.download_run_artifact(owner, received, run)
        record, file_id, data = reports.unpack(body, period)
        if file_id is None:
            return status, reports.no_data_commit(record)
        return files_ds, reports.file_commit(record, file_id, data)

    def register(run, period, **options):
        dataset, body = handoff(run, period)
        return commit(dataset, body, **options)

    def read(dataset, txn):
        """What a reader sees of one transaction, knowing neither the staged file nor the receipt.
        The file is the listing entry bound to the transaction; its id is the one downloads accept."""
        listed = [v for v, t in versions(dataset) if t == txn]
        meta = p1.api("dataset", "ds_txn_get", path_args={"id": dataset, "branch": "main", "txn": txn}, token=reader)
        bound = [f for f in p1.list_files(reader, dataset) if f["transaction_id"] == txn]
        assert meta.status == 200 and len(bound) == 1, (listed, meta.status, len(bound))
        got = p1.download_file(reader, dataset, bound[0]["id"])
        assert got.status == 200, got.status
        return dict(version=listed, metadata=meta.body.get("metadata") or {}, file=bound[0], body=got.body)

    def confirm(entry, data, when):
        seen = read(files_ds, entry["transaction"])
        record = reports.registration(data, entry["period"])
        check(f"reader resolves {entry['file']} {when}: version {entry['version']}, its scope and fileId",
              seen["version"] == [entry["version"]]
              and seen["file"]["logical_path"] == f"amazon-us/transaction_view/{entry['period']}/{entry['file_id']}.csv"
              and {k: seen["metadata"].get(k) for k in ("registration", "file_id")} == {"registration": record, "file_id": entry["file_id"]})
        check(f"reader confirms {entry['file']} {when}: original CSV bytes, sha256 and length",
              seen["body"] == data and reports.sha256(seen["body"]) == record["sha256"]
              and seen["file"]["size_bytes"] == record["byteSize"] == len(data))
        (state / f"published-{entry['file']}.csv").write_bytes(seen["body"])
        return seen["file"]["id"]

    check("create private report bucket", s3.s3_request("PUT", BUCKET) in (200, 201))
    for period, name in FILES:
        data, run = originals[name], deliver(period, name, file_envelope(period, originals[name], uploaded(name)))
        check(f"connector committed the envelope of {name} unchanged",
              run["content_hash"] == reports.sha256((state / f"envelope-{period}-{name}.ndjson").read_bytes()))
        result = register(run, period)
        check(f"{name} registered as the original CSV under its own fileId", result.status == 201)
        entry = dict(file=name, period=period, file_id=FILE_IDS[name], sha256=reports.sha256(data), byte_size=len(data),
                     transaction=result.body["transaction"]["id"], received_transaction=run["dataset_version_id"],
                     sync_run=run["id"], version=len(report["files"]) + 1)
        check(f"{name} is the head of the report dataset, published as text/csv",
              index(files_ds) == [(f"amazon-us/transaction_view/{period}/{FILE_IDS[name]}.csv", "text/csv")])
        entry["dataset_file"] = confirm(entry, data, "when registered")
        report["files"].append(entry)
    _, second, third = report["files"]

    # A redelivery is recognised by its scope and fileId.
    before = versions(files_ds)
    replay = register(deliver("2026-10", "again", file_envelope("2026-10", originals["2026-10"], uploaded("2026-10"))), "2026-10")
    check("the same registration redelivered replays its first receipt and publishes nothing",
          replay.status == 200 and replay.body["transaction"]["id"] == second["transaction"] and versions(files_ds) == before)
    changed = file_envelope("2026-10", originals["2026-10"].replace(b"Demo City", b"Demo Town"), uploaded("2026-10"))
    conflict = register(deliver("2026-10", "changed", changed), "2026-10")
    check("a changed file under the same fileId is a conflict and changes nothing",
          conflict.status == 409 and versions(files_ds) == before)
    report.update(replayed_transaction=replay.body["transaction"]["id"], conflict_status=conflict.status)

    # The connector commits every envelope below. The handoff must publish none of them.
    september = originals["2026-09"]

    def refused(label, period, body):
        run = deliver(period, label, body)
        try:
            register(run, period)
        except reports.RegistrationError as exc:
            reason = str(exc)
        else:
            raise AssertionError(label)
        check(f"refused {label}: {reason}", versions(files_ds) == before and not versions(status))
        report["refused"].append(dict(case=label, period=period, reason=reason, sync_run=run["id"],
                                      received_transaction=run["dataset_version_id"]))

    refused("a file shorter than its registration", "2026-07", file_envelope("2026-07", september[:-40], uploaded("2026-09"), registered=september))
    refused("a file that is not the one that was hashed", "2026-07", file_envelope("2026-07", september.replace(b"Demo City", b"Demo Town"), uploaded("2026-09"), registered=september))
    refused("coverage outside the period", "2026-06", file_envelope("2026-06", september, uploaded("2026-09"), coverageStart="2026-05-01"))
    refused("a file whose upload was not confirmed", "2026-05", file_envelope("2026-05", september, {"status": "failed", "note": "core returned HTTP 500"}))
    for label, receipt in (("failed", {"status": "failed", "note": "core returned HTTP 500"}),
                           ("skipped", {"status": "skipped", "note": "store not bound"}),
                           ("never sent", {})):
        refused(f"a no-data month whose report was {label}", "2026-04",
                reports.envelope("no_data", report=dict(reported, period="2026-04"), receipt=receipt))
    wrong_length = reports.file_commit(reports.registration(september, "2026-03"), str(uuid.uuid4()), september)
    wrong_length["files"][0]["size_bytes"] += 1
    check("a declared length that differs from the bytes is refused by the Dataset service",
          commit(files_ds, wrong_length).status == 400 and versions(files_ds) == before)

    # A reported no-data month is state. It is kept apart from the report files.
    report_head = head(files_ds)
    check("the report dataset head is the latest registered file", report_head == third["transaction"])
    record = dict(reported, requirementCode="transaction_view")
    body = reports.envelope("no_data", report=reported, receipt={"status": "reported", "requirementCode": "transaction_view"})
    # The record claims the other tenant in headers. Only the signed caller's tenant may see it.
    result = register(deliver("2026-08", "no-data", body), "2026-08", headers=dict.fromkeys(p1.FORGED_HEADERS, ids["org_b"]))
    no_data = result.body["transaction"]["id"]
    check("a reported no-data month is recorded in the status dataset only",
          result.status == 201 and versions(status) == [(1, no_data)] and versions(files_ds) == before
          and head(files_ds) == report_head)
    seen = read(status, no_data)
    check("the reader resolves the no-data record by its checkedAt; the dataset holds the record only",
          seen["metadata"].get("no_data") == record and json.loads(seen["body"]) == record
          and seen["file"]["logical_path"] == "amazon-us/transaction_view/2026-08.no-data.json"
          and index(status) == [("amazon-us/transaction_view/2026-08.no-data.json", "application/json")])
    (state / "no-data-2026-08.json").write_bytes(seen["body"])
    report["no_data"] = dict(record, version=1, transaction=no_data)
    for entry in report["files"]:
        confirm(entry, originals[entry["file"]], "after the no-data record")

    # A month whose envelope cannot be fetched is a failure, not a month without data.
    received_before = versions(received)
    missing = collect("2026-09", "missing")
    check("an envelope that cannot be fetched fails the connector run and confirms no month",
          missing["status"] == "failed" and not missing.get("dataset_version_id") and "HTTP 404" in str(missing.get("error"))
          and versions(received) == received_before and versions(status) == [(1, no_data)] and versions(files_ds) == before)
    report["failed_sync_run"] = missing["id"]
    check("only the reported month is confirmed without data",
          [read(status, t)["metadata"]["no_data"]["period"] for _, t in versions(status)] == ["2026-08"])
    for entry in report["files"]:
        confirm(entry, originals[entry["file"]], "after the failed fetch")
    check("the report dataset head is still the latest registered file", head(files_ds) == report_head)

    # The tenant is the signed caller's, for the files, the record and the staged envelope.
    check("the report and status datasets belong to the signed caller's tenant",
          [p1.api("dataset", "ds_get", path_args={"id": d}, token=reader).body["org_id"] for d in (files_ds, status)]
          == [ids["org_a"]] * 2)
    for label, dataset, txn, file_id in (("report file", files_ds, third["transaction"], third["dataset_file"]),
                                         ("no-data record", status, no_data, seen["file"]["id"])):
        for what, response in (("dataset", p1.api("dataset", "ds_get", path_args={"id": dataset}, token=foreign)),
                               ("transaction", p1.api("dataset", "ds_txn_get", path_args={"id": dataset, "branch": "main", "txn": txn}, token=foreign)),
                               ("download", p1.download_file(foreign, dataset, file_id))):
            check(f"the other tenant is denied the {label} {what} (HTTP {response.status})", response.status in p1.DENY)
    third_body = reports.file_commit(reports.registration(originals["2026-10-2"], "2026-10"), FILE_IDS["2026-10-2"], originals["2026-10-2"])
    check("the other tenant is denied the registration and the no-data record",
          commit(files_ds, third_body, token=foreign).status in p1.DENY
          and commit(status, reports.no_data_commit(record), token=foreign).status in p1.DENY
          and versions(files_ds) == before and versions(status) == [(1, no_data)])
    check("the other tenant is denied a staged envelope",
          s3.s3_request("GET", f"{BUCKET}/{reports.object_key('2026-09', '2026-09')}", credentials=s3.scoped_credentials(f"arn:aws:s3:::{BUCKET}/other-org/*", "s3:GetObject")) == 403)
    # One pipeline reads the two published October reports, not the staged envelopes: the first, then the second, then the first again.
    os.environ.update(WP34_PROJECT="wp28-amazon", WP34_EVIDENCE=str(state), WP34_TOKEN=reader,
                      WP34_DVS=p1.BASES["dataset"], WP34_PBS=os.environ["WP23_PBS"])
    graph = s3.module("wp28_amazon_graph", SMOKE / "data-integration-wp3-4/harness34.py")
    first, second = report["files"][1:]
    # A build of one October file must hold none of the transactions of the other.
    other = {first["file"]: second["file"], second["file"]: first["file"]}
    # What the reader asked of the schema of each report version: the route's answers, by version.
    schemas = {}

    def schema_route(version, **params):
        """The formal versioned schema request for one report version on main."""
        return f"/v1/datasets/{files_ds}/schema?" + urlencode({"branch": "main", "version": version, **params})

    def schema_answer(name, version, **params):
        """The reader's versioned schema request for one report version. The answer is kept as it came."""
        result = p1.http("dataset", schema_route(version, **params), token=reader, raw=True)
        (state / f"schema-{name}.json").write_bytes(result.body)
        return result

    def pin_of(entry, version, schema):
        """What a run is pinned to: the version and transaction the reader listed, and the schema it expects of them."""
        return {"branch": "main", "version": version,
                "transaction_id": "ri.foundry.main.transaction." + entry["transaction"],
                "expected_schema_hash": schema["content_hash"], "expected_schema": schema["fields"]}

    for entry in (first, second):
        data = originals[entry["file"]]
        # One read, as the reader makes it: the version of the transaction, the file bound to it and its bytes.
        seen = read(files_ds, entry["transaction"])
        record = reports.registration(data, entry["period"])
        published = seen["metadata"].get("schema") or {}
        options = (published.get("custom_metadata") or {}).get("csv") or {}
        entry["logical_path"] = seen["file"]["logical_path"]
        check(f"reader resolves {entry['file']} as the input of the Pipeline: version {entry['version']}, its scope and "
              f"fileId, the original CSV bytes, sha256 and length",
              seen["version"] == [entry["version"]]
              and seen["file"]["logical_path"] == f"amazon-us/transaction_view/{entry['period']}/{entry['file_id']}.csv"
              and {k: seen["metadata"].get(k) for k in ("registration", "file_id")} == {"registration": record, "file_id": entry["file_id"]}
              and seen["body"] == data and reports.sha256(seen["body"]) == record["sha256"]
              and seen["file"]["size_bytes"] == record["byteSize"] == len(data))
        check(f"{entry['file']} is published with its header on record {reports.HEADER_ROW}, after the amounts preamble",
              published.get("file_format") == "TEXT" and published.get("fields") == reports.csv_schema(data)
              and {name: options.get(name) for name in ("headerRow", "charset", "bom")}
              == {"headerRow": reports.HEADER_ROW, "charset": "UTF-8", "bom": "strip"})
        # The reader holds no write permission. It asks the formal versioned schema route for the version it lists for
        # the transaction, not for the number the handoff counted, and holds the answer against the schema the
        # publication declared, which it reads from the transaction metadata. The hash it takes is the one the route
        # answers: it encodes nothing to get one.
        version = graph.version_by_txn(files_ds, entry["transaction"])
        result = schema_answer(f"v{version}", version, transaction_id=entry["transaction"])
        answer = json.loads(result.body) if result.status == 200 else {}
        # The route names the type of a field field_type; the schema kept with the commit names it type.
        declared = [dict(name=field["name"], field_type=field["type"], nullable=field["nullable"])
                    for field in seen["metadata"]["schema"]["fields"]]
        check(f"independent reader reads the schema of report v{version}: bound to its version and transaction, "
              f"total DOUBLE and the {len(declared)} fields the publication declared",
              result.status == 200
              and (answer.get("dataset_id"), answer.get("branch"), answer.get("version"), answer.get("transaction_id"))
              == (files_ds, "main", version, entry["transaction"])
              and next((field["field_type"] for field in answer.get("fields") or [] if field["name"] == "total"), None)
              == "DOUBLE"
              and answer.get("fields") == declared)
        schemas[version] = answer
        own = schema_answer(f"v{version}-own-hash", version, transaction_id=entry["transaction"],
                            expected_content_hash=answer.get("content_hash"))
        check(f"the schema of report v{version} answers 200 under its own content hash",
              own.status == 200 and own.body == result.body)
        # Another hash is refused. It is a fixed digest, not a schema the test encodes.
        other_hash = schema_answer(f"v{version}-other-hash", version, transaction_id=entry["transaction"],
                                   expected_content_hash=hashlib.sha256(b"another schema").hexdigest())
        check(f"the schema of report v{version} refuses another content hash "
              f"(HTTP {other_hash.status}, dataset_schema_content_hash_mismatch)",
              other_hash.status == 412
              and json.loads(other_hash.body).get("code") == "dataset_schema_content_hash_mismatch")
    check(f"report v{first['version']} and v{second['version']} answer the same fields and the same schema hash",
          schemas[first["version"]]["fields"] == schemas[second["version"]]["fields"]
          and schemas[first["version"]]["content_hash"] == schemas[second["version"]]["content_hash"])

    def head_of(dataset):
        result = p1.api("dataset", "ds_branch_get", token=reader, path_args={"id": dataset, "branch": "main"})
        assert result.status == 200, result.status
        return result.body["head_transaction_id"]

    def files_of(dataset, txn):
        """The files one committed transaction holds, as the reader lists them for that transaction alone."""
        listed = p1.http("dataset", f"/v1/datasets/{dataset}/files?transaction_id={txn}", token=reader)
        assert listed.status == 200, (listed.status, listed.body)
        return listed.body["files"]

    down = p1.create_dataset(owner, "Amazon US transaction amounts", "json")["id"]
    # Two different identifiers name one published output. Every build records both.
    pipeline = report["pipeline"] = dict(
        pipeline_id=None, output_dataset=down, columns=transactions.COLUMNS, expected_sha256=transactions.OUTPUT_SHA256,
        builds=[],
        transaction_ids={
            "input_transaction": "Dataset Versioning transaction id of the report version the run read",
            "output_transaction": "Dataset Versioning transaction id of the output version, as the reader lists it",
            "pipeline_output_transaction_rid": "transaction_rid of the committed resource in the Pipeline run receipt, "
                                               "the Pipeline's own name. The Dataset transaction repeats it in "
                                               "metadata.transaction_rid, which links the run to its output version"})
    input_before = head_of(files_ds)
    known = {v["transaction_id"] for v in graph.list_versions(down)}
    input_versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(files_ds)}
    for ordinal, entry in enumerate((first, second, first), 1):
        name = entry["file"]
        # The pin follows the version the reader lists for the transaction, not the number the handoff counted.
        version = graph.version_by_txn(files_ds, entry["transaction"])
        report_rows = transactions.expected(originals[name])
        wanted = transactions.expected(originals[name], transactions.COLUMNS)
        # The pin also requires the schema the reader asked of that version: its hash and its fields.
        schema = schemas[version]
        pin = pin_of(entry, version, schema)
        nodes = transactions.nodes(graph, files_ds, down, dict(entry, version=version), schema)
        graph.TOKEN = owner
        try:
            pid = pipeline["pipeline_id"]
            if pid is None:
                pid = pipeline["pipeline_id"] = graph.create_pipeline("amazon-transaction-amounts", nodes)
            else:
                graph.http("PATCH", f"{graph.PBS}/api/v1/pipelines/{pid}", {"nodes": nodes})
            run = graph.run_pipeline(pid)
            # The graph as it stands after the run: the pin the run was saved with.
            _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pid}")
        finally:
            graph.TOKEN = reader
        s3.write_json(state / f"pipeline-{ordinal}-graph.json", saved)
        s3.write_json(state / f"pipeline-{ordinal}-run.json", run)
        check(f"pipeline run {ordinal} succeeded", run["status"] == "succeeded")
        # The reader reads the head of the input once the run is over: no run moved it off the version it read.
        input_head = head_of(files_ds)
        check(f"report v3 was still the head of the input after pipeline run {ordinal}", input_head == second["transaction"])
        saved_nodes = {n["id"]: n for n in saved["dag"]["nodes"]}
        config = saved_nodes["input"]["config"]
        check(f"pipeline run {ordinal} saved the graph pinned to report v{version} and its transaction",
              config["dataset_id"] == files_ds and config["dataset_snapshot"] == pin
              and saved_nodes["project-transactions"]["config"] == {"columns": transactions.COLUMNS}
              and saved_nodes["output"]["output_dataset_id"] == down)
        observed = next(n["output"] for n in run["node_results"] if n["node_id"] == "input")
        # The receipt holds the rows as a JSON reader parsed them. Their amounts are read back as Decimals.
        read_diffs = transactions.value_diffs(transactions.lines(observed["sample_rows"]), report_rows)
        check(f"pipeline run {ordinal} input node receipt read report v{version} on main through the pinned transaction: "
              f"{len(report_rows)} transactions under their header columns" + ("" if not read_diffs else "; " + "; ".join(read_diffs)),
              run["pipeline_id"] == pid and run["id"] == run["run_id"]
              and observed["dataset_rid"].removeprefix("ri.foundry.main.dataset.") == files_ds
              and observed["dataset_branch"] == pin["branch"] and observed["dataset_version"] == pin["version"]
              and observed["dataset_transaction_id"] == pin["transaction_id"]
              and observed["dataset_file_format"] == "csv" and observed["rows_affected"] == len(report_rows)
              and not observed["dataset_rows_truncated"] and not read_diffs)
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
        produced_version = produced["version"]
        check(f"pipeline run {ordinal} committed one resource as output v{produced_version}, a transaction of this run",
              resource["status"] == "committed" and resource["kind"] == "dataset"
              and resource["rid"].removeprefix("ri.foundry.main.dataset.") == down
              and resource["transaction_rid"] == handle and transaction["status"] == "COMMITTED"
              and transaction["branch_name"] == "main" and transaction["dataset_id"] == down
              and metadata["pipeline_output"] is True and metadata["pipeline_rid"] == pid
              and metadata["output_node_id"] == "output" and metadata["transaction_rid"] == handle)
        output_txns = {v["version"]: v["transaction_id"] for v in graph.list_versions(down)}
        bound, = [f for f in p1.list_files(reader, down) if f.get("transaction_id") == produced["transaction_id"]]
        response = p1.download_file(reader, down, bound["id"])
        artifact = f"pipeline-{ordinal}-output-v{produced_version}.ndjson"
        (state / artifact).write_bytes(response.body)
        diffs = transactions.value_diffs(response.body, wanted) if response.status == 200 else [f"download status {response.status}"]
        check(f"independent reader maps output v{produced_version} to its transaction and downloads "
              f"the {len(wanted)} transactions of report v{version}" + ("" if not diffs else ": " + "; ".join(diffs)),
              response.status == 200 and output_txns[produced_version] == produced["transaction_id"] and not diffs
              and reports.sha256(response.body) == transactions.OUTPUT_SHA256[name])
        # The amounts are summed as decimals on both sides: in what was downloaded, and in the report file itself.
        net = transactions.net(transactions.read(response.body))
        check(f"the {len(wanted)} transactions of report v{version} sum to total {net} in the download "
              f"and in the report file",
              net == transactions.net(transactions.expected(originals[name])) == transactions.NET_TOTAL[name])
        # The preview is compared only under unique column names and rows as wide as the columns.
        preview = transactions.table(metadata["preview_columns"], metadata["preview_rows"])
        check(f"dataset transaction of output v{produced_version} carries the run's output hash, "
              f"the {len(wanted)} expected values and the bytes the reader downloaded",
              metadata["output_content_hash"] == output_node["output"]["content_hash"]
              and sorted(metadata["preview_columns"]) == sorted(transactions.COLUMNS) and preview is not None
              and not transactions.value_diffs(transactions.lines(preview), wanted)
              and metadata["row_count"] == len(wanted) and metadata["size_bytes"] == len(response.body))
        held = transactions.read(response.body) if response.status == 200 and not diffs else []
        check(f"output v{produced_version} holds only the {len(wanted)} transactions of {name}, in file order, "
              f"and none of the other October file",
              transactions.holds(held, wanted, transactions.expected(originals[other[name]], ("settlement id",))))
        negatives = transactions.negative_amounts(held)
        check(f"output v{produced_version} keeps the {transactions.NEGATIVE_AMOUNTS[name]} negative amounts of {name} "
              f"where they are, sign included",
              negatives == transactions.negative_amounts(wanted)
              and sum(map(len, negatives)) == transactions.NEGATIVE_AMOUNTS[name])
        pipeline["builds"].append(dict(ordinal=ordinal, pipeline_run_id=run["id"], input_dataset=files_ds,
                                       input_file=name, input_file_id=entry["file_id"], input_logical_path=entry["logical_path"],
                                       input_sha256=entry["sha256"], input_branch=observed["dataset_branch"],
                                       input_version=version, input_transaction=entry["transaction"], input_head=input_head,
                                       output_dataset=down, output_version=produced_version,
                                       output_transaction=produced["transaction_id"],
                                       pipeline_output_transaction_rid=handle,
                                       pipeline_content_hash=metadata.get("output_content_hash"),
                                       settlement_ids=[row["settlement id"] for row in held],
                                       negative_amounts=sum(map(len, negatives)), net_total=str(net),
                                       output_file_id=bound["id"], artifact=artifact, row_count=len(wanted),
                                       size_bytes=len(response.body), sha256=reports.sha256(response.body)))

    first_build, latest, replay = pipeline["builds"]
    output_versions = {v["version"]: v["transaction_id"] for v in graph.list_versions(down)}
    builds = pipeline["builds"]
    pipeline["heads"] = dict(input_before=input_before, output_after=head_of(down))
    check("three runs of one pipeline read report v2, v3 and v2 and published output v1, v2 and v3 in that order",
          [b["input_version"] for b in builds] == [2, 3, 2] and [b["output_version"] for b in builds] == [1, 2, 3]
          and sorted(output_versions) == [1, 2, 3]
          and {b["output_version"]: b["output_transaction"] for b in builds} == output_versions
          and all(len({b[field] for b in builds}) == 3
                  for field in ("pipeline_run_id", "output_transaction", "output_file_id"))
          and {v["version"]: v["transaction_id"] for v in graph.list_versions(files_ds)} == input_versions
          == {**{1: report["files"][0]["transaction"]}, **{b["input_version"]: b["input_transaction"] for b in builds}}
          and len(input_versions) == 3)
    heads = pipeline["heads"]
    check("report v3 was the head of the input before the runs and after every run read its pinned version, "
          "and the output head after them is the replay",
          heads["input_before"] == second["transaction"]
          and all(b["input_head"] == second["transaction"] for b in builds)
          and heads["output_after"] == replay["output_transaction"])
    check("v2 replay read the first report v2 transaction again and published the first output bytes and hash, not v3's",
          replay["input_transaction"] == first_build["input_transaction"] != latest["input_transaction"]
          and replay["sha256"] == first_build["sha256"] == transactions.OUTPUT_SHA256[first["file"]]
          != latest["sha256"] == transactions.OUTPUT_SHA256[second["file"]]
          and (state / replay["artifact"]).read_bytes() == (state / first_build["artifact"]).read_bytes()
          != (state / latest["artifact"]).read_bytes())

    # Every output is history once v3 is the head. The reader starts from each version number alone: the
    # transaction that cut it, the files that transaction holds, and the download of that file. It also scans
    # the version by number. Nothing here uses what a build recorded while its output was the head.
    history = pipeline["history"] = []
    listed = {v["version"]: v["transaction_id"] for v in graph.list_versions(down)}
    for build, entry in zip((first_build, latest, replay), (first, second, first)):
        version = build["output_version"]
        transaction = graph.get_output_transaction(down, listed[version])
        wanted = transactions.expected(originals[entry["file"]], transactions.COLUMNS)
        files = files_of(down, listed[version])
        response = p1.download_file(reader, down, files[0]["id"]) if len(files) == 1 else None
        artifact = f"pipeline-history-output-v{version}.ndjson"
        if response is not None:
            (state / artifact).write_bytes(response.body)
        scan = graph.scan_all(down, version)
        diffs = transactions.value_diffs(response.body, wanted) if response is not None and response.status == 200 else [
            f"{len(files)} files listed for the transaction"]
        scan_diffs = transactions.value_diffs(transactions.lines(scan["rows"]), wanted)
        history.append(dict(version=version, transaction=listed[version], pipeline_run_id=build["pipeline_run_id"],
                            pipeline_output_transaction_rid=transaction["metadata"].get("transaction_rid"),
                            listed_files=[dict(file_id=f["id"], logical_path=f["logical_path"], status=f["status"],
                                               transaction_id=f["transaction_id"]) for f in files],
                            download_status=None if response is None else response.status, artifact=artifact,
                            row_count=build["row_count"], size_bytes=None if response is None else len(response.body),
                            download_sha256=None if response is None else reports.sha256(response.body),
                            scan_rows=len(scan["rows"]), scan_sha256=scan["rendered_sha256"]))
        seen = history[-1]
        check(f"after v3 is the head, the read-only reader takes output v{version} to the transaction of build "
              f"{build['ordinal']}, lists its one file and downloads the {len(wanted)} transactions of report "
              f"v{build['input_version']} with the bytes and hash that build published"
              + ("" if not diffs else ": " + "; ".join(diffs)),
              listed[version] == build["output_transaction"] and transaction["status"] == "COMMITTED"
              and transaction["metadata"].get("transaction_rid") == build["pipeline_output_transaction_rid"]
              and transaction["metadata"].get("pipeline_rid") == pipeline["pipeline_id"]
              and len(files) == 1 and files[0]["transaction_id"] == listed[version] and files[0]["status"] == "active"
              and files[0]["id"] == build["output_file_id"] and response is not None and response.status == 200
              and not diffs and seen["download_sha256"] == build["sha256"] == transactions.OUTPUT_SHA256[entry["file"]]
              and response.body == (state / build["artifact"]).read_bytes())
        check(f"a reader that knows only output v{version} scans that version and reads the same "
              f"{len(wanted)} transactions of report v{build['input_version']}"
              + ("" if not scan_diffs else ": " + "; ".join(scan_diffs)),
              not scan_diffs and len(scan["rows"]) == len(wanted))
    first_seen, latest_seen, replay_seen = history
    check("the history a reader takes after v3 is the head replays: v1 and v3 are the first publication and v2 the second file, "
          "by download and by scan",
          [h["version"] for h in history] == [1, 2, 3]
          and [h["download_sha256"] for h in history] == [transactions.OUTPUT_SHA256[first["file"]],
                                                          transactions.OUTPUT_SHA256[second["file"]],
                                                          transactions.OUTPUT_SHA256[first["file"]]]
          and first_seen["scan_sha256"] == replay_seen["scan_sha256"] != latest_seen["scan_sha256"]
          and (state / replay_seen["artifact"]).read_bytes() == (state / first_seen["artifact"]).read_bytes()
          != (state / latest_seen["artifact"]).read_bytes()
          and len({h["transaction"] for h in history}) == 3 and len({h["listed_files"][0]["file_id"] for h in history}) == 3)

    # Two refusals of the bound input, after the three runs and before the schema gate below. Each saves and runs a
    # Pipeline of its own, so the Pipeline of the three runs keeps the pin of its last good run. Each must fail at its
    # input node before a row is read and publish nothing. Before each refusal and after it, the read-only reader of the
    # first organization lists both datasets, takes the last good output v3 to the transaction that cut it and to its
    # one file, downloads the file and scans the version. Nothing of that view is taken from what a build recorded.
    refusals = pipeline["refusals"] = {}

    def input_of(saved):
        """The input config of a saved graph: the dataset and the pin."""
        return next(n["config"] for n in saved["dag"]["nodes"] if n["id"] == "input")

    def org_of(dataset):
        """The tenant of a dataset, as the reader sees it."""
        result = p1.api("dataset", "ds_get", path_args={"id": dataset}, token=reader)
        assert result.status == 200, result.status
        return result.body["org_id"]

    def view():
        """The reader's view of both datasets and of the last good output, and the Pipeline as its owner saved it."""
        # Versions are held by number, so the view does not depend on the order a listing answers them in.
        listed_outputs, listed_reports = dict(versions(down)), dict(versions(files_ds))
        version, transaction = max(listed_outputs.items())
        cut = graph.get_output_transaction(down, transaction)
        files = files_of(down, transaction)
        response = p1.download_file(reader, down, files[0]["id"]) if len(files) == 1 else None
        graph.TOKEN = owner
        try:
            _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pid}")
        finally:
            graph.TOKEN = reader
        return dict(output_versions=listed_outputs, output_head=head_of(down), output_org=org_of(down),
                    report_versions=listed_reports, report_head=head_of(files_ds), report_org=org_of(files_ds),
                    pipeline=dict(tenant=saved["tenant_id"], owner=saved["owner_id"], input=input_of(saved)),
                    version=version, transaction=transaction, status=cut["status"], dataset=cut["dataset_id"],
                    branch=cut["branch_name"], run=cut["metadata"].get("transaction_rid"),
                    file=None if len(files) != 1 else files[0]["id"],
                    download=None if response is None else response.status,
                    scan=graph.scan_all(down, version)["rendered_sha256"],
                    sha256=None if response is None or response.status != 200 else reports.sha256(response.body),
                    bytes=None if response is None else response.body)

    def recorded(seen):
        """The reader's view for the report, the downloaded bytes aside."""
        return {key: value for key, value in seen.items() if key != "bytes"}

    def control(tag, nodes, token, out=None):
        """Save and run one graph as a token, on a Pipeline of its own. Return the saved input, the run and the versions
        of the output dataset `out` as that token lists them."""
        graph.TOKEN = token
        try:
            own = graph.create_pipeline(f"amazon-transaction-amounts-{tag}", nodes)
            _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{own}")
            run = graph.run_pipeline(own)
            published = None if out is None else {v["version"]: v["transaction_id"] for v in graph.list_versions(out)}
        finally:
            graph.TOKEN = reader
        s3.write_json(state / f"refused-{tag}-graph.json", saved)
        s3.write_json(state / f"refused-{tag}-run.json", run)
        results = run.get("node_results") or []
        return dict(pipeline_id=own, status=run["status"], tenant=saved["tenant_id"], owner=saved["owner_id"],
                    saved_input=input_of(saved), published=published,
                    error=next((n.get("error") or "" for n in results if n["node_id"] == "input"), ""),
                    nodes=[dict(node_id=n["node_id"], status=n["status"], error=n.get("error"),
                                output_resources=[r["status"] for r in n.get("output_resources") or []]) for n in results])

    # What the three runs left, as they recorded it. The reader's view is held against this, never against itself.
    replay_graph = json.loads((state / "pipeline-3-graph.json").read_text())
    wanted = dict(output_versions={b["output_version"]: b["output_transaction"] for b in builds},
                  output_head=heads["output_after"], output_org=ids["org_a"],
                  report_versions=input_versions,
                  report_head=second["transaction"], report_org=ids["org_a"],
                  pipeline=dict(tenant=ids["org_a"], owner=ids["dual_sub"], input=input_of(replay_graph)),
                  version=replay["output_version"], transaction=replay["output_transaction"], status="COMMITTED",
                  dataset=down, branch="main", run=replay["pipeline_output_transaction_rid"], file=replay["output_file_id"],
                  download=200, scan=replay_seen["scan_sha256"], sha256=replay["sha256"],
                  bytes=(state / replay["artifact"]).read_bytes())

    def refuse(tag, why, nodes, holds, what, token=owner, out=None):
        """The reader's view, one run of a Pipeline of its own saved with `nodes`, and the reader's view again.

        `holds(outcome)` says whether the run was refused as this case expects.
        """
        baseline = view()
        check(f"before the run refused for {why} the read-only reader takes output v3 to the transaction of the third run, "
              "its one file, and the bytes and the hash that run published",
              baseline == wanted)
        outcome = control(tag, nodes, token, out)
        check(f"the run refused for {why} {what}", holds(outcome))
        after = view()
        outcome.update(baseline=recorded(baseline), after=recorded(after))
        check(f"after the run refused for {why} the read-only reader takes output v3 to the same transaction, file, bytes "
              "and hash, and the versions, the heads, the tenants and the saved pin of the Pipeline are as before",
              after == baseline)
        (state / f"refused-{tag}-readback-v3.ndjson").write_bytes(after["bytes"])
        return outcome

    # The pin names report v2 together with the transaction that cut v3.
    v2_schema = schemas[first_build["input_version"]]
    crossed = refusals["crossed_transaction"] = refuse(
        "crossed-transaction", "the transaction",
        transactions.nodes(graph, files_ds, down, dict(second, version=first_build["input_version"]), v2_schema),
        lambda outcome: transactions.refused_at_input(outcome, "batched_dataset_409_dataset_schema_transaction_mismatch"),
        "is refused on its input node by an explicit 409 dataset_schema_transaction_mismatch: project-transactions and "
        "output are cancelled, and no resource is committed")
    check("the pin that names report v2 with the transaction of report v3 is saved as declared, on a Pipeline of the "
          "first organization",
          crossed["saved_input"]["dataset_id"] == files_ds
          and crossed["saved_input"]["dataset_snapshot"] == pin_of(second, first_build["input_version"], v2_schema)
          and (crossed["tenant"], crossed["owner"]) == (ids["org_a"], ids["dual_sub"]))

    # The same user in another organization saves the pin of the second run, which succeeded here, over an output dataset
    # of its own. The Dataset service answers that organization with a denial, not with the dataset.
    other_down = p1.create_dataset(foreign, "Amazon US transaction amounts of another organization", "json")["id"]
    latest_graph = json.loads((state / "pipeline-2-graph.json").read_text())
    foreign_run = refusals["other_organization"] = refuse(
        "other-organization", "the organization",
        transactions.nodes(graph, files_ds, other_down, dict(second, version=latest["input_version"]),
                           schemas[latest["input_version"]]),
        lambda outcome: transactions.refused_at_input(outcome)
        and any(f"batched_dataset_{status}" in outcome["error"] for status in p1.DENY)
        and outcome["published"] == {},
        "is refused on its input node by 403 or 404: project-transactions and output are cancelled, no resource is "
        "committed, and its output dataset holds no version",
        token=foreign, out=other_down)
    foreign_run["output_dataset"] = other_down
    check("the same user in another organization saves the pin of the second run, over an output dataset of its own, on a "
          "Pipeline of that organization",
          foreign_run["saved_input"] == input_of(latest_graph)
          and (foreign_run["tenant"], foreign_run["owner"]) == (ids["org_b"], ids["dual_sub"]))

    # A report whose total holds text in one row must not pass the Pipeline. It is the second October file with that
    # one cell as N/A, and it goes the way of every other file: envelope, connector commit, handoff, registration and
    # publication. Nothing marks it. The schema the Pipeline expects is what refuses it.
    name, period = NEGATIVE, "2026-10"
    bad = transactions.text_total(originals["2026-10-2"])
    (state / "negative-transaction_view-2026-10.csv").write_bytes(bad)
    run = deliver(period, "text-total", file_envelope(period, bad, uploaded(name)))
    result = register(run, period)
    check(f"{name} is registered under its own fileId, as every other file", result.status == 201)
    negative = dict(file=name, period=period, file_id=FILE_IDS[name], sha256=reports.sha256(bad), byte_size=len(bad),
                    transaction=result.body["transaction"]["id"], received_transaction=run["dataset_version_id"],
                    sync_run=run["id"], version=len(report["files"]) + 1)
    negative["dataset_file"] = confirm(negative, bad, "when registered")

    # The reader asks the schema of that report as it did of v2 and v3, for the version it lists. Its total is text
    # now, so the column is a string, and its hash is not the one the Pipeline expects. The route says so to a caller
    # that names that hash. The expected schema is the reader's own answer for the version of the second October file.
    version = graph.version_by_txn(files_ds, negative["transaction"])
    good = graph.version_by_txn(files_ds, second["transaction"])
    expected = schemas[good]
    digest = expected["content_hash"]
    asked = schema_answer(f"v{version}", version, transaction_id=negative["transaction"])
    committed = json.loads(asked.body) if asked.status == 200 else {}
    check(f"the schema of report v{version} answers total as STRING and every other field as v{good}, "
          "under another hash",
          asked.status == 200
          and (committed.get("dataset_id"), committed.get("branch"), committed.get("version"), committed.get("transaction_id"))
          == (files_ds, "main", version, negative["transaction"])
          and committed.get("fields") == [{**field, "field_type": "STRING"} if field["name"] == "total" else field
                                          for field in expected["fields"]]
          and bool(committed.get("content_hash")) and committed["content_hash"] != digest)
    stale = schema_answer(f"v{version}-expected-hash", version, transaction_id=negative["transaction"],
                          expected_content_hash=digest)
    check(f"the schema of report v{version} refuses the content hash the Pipeline expects "
          f"(HTTP {stale.status}, dataset_schema_content_hash_mismatch)",
          stale.status == 412 and json.loads(stale.body).get("code") == "dataset_schema_content_hash_mismatch")

    # What the reader lists before the refused run.
    before = dict(outputs=graph.list_versions(down), head=head_of(down), reports=graph.list_versions(files_ds))

    # The same Pipeline, pinned to that report with the same expected schema as the three runs before it. It is the
    # fourth run of this Pipeline.
    pin = pin_of(negative, version, expected)
    nodes = transactions.nodes(graph, files_ds, down, dict(negative, version=version), expected)
    graph.TOKEN = owner
    try:
        graph.http("PATCH", f"{graph.PBS}/api/v1/pipelines/{pid}", {"nodes": nodes})
        refusal = graph.run_pipeline(pid)
        # The graph as it stands after the run: the pin the run was saved with.
        _, saved = graph.http("GET", f"{graph.PBS}/api/v1/pipelines/{pid}")
    finally:
        graph.TOKEN = reader
    s3.write_json(state / "pipeline-4-graph.json", saved)
    s3.write_json(state / "pipeline-4-run.json", refusal)
    saved_pin = {n["id"]: n for n in saved["dag"]["nodes"]}["input"]["config"]
    results = refusal.get("node_results") or []
    statuses = {n["node_id"]: n["status"] for n in results}
    # The run only says failed. The reason is in the input node's own error, and the Pipeline service writes it.
    error = next((n.get("error") or "" for n in results if n["node_id"] == "input"), "")
    reason = 'field "total" is STRING in the committed schema and DOUBLE in the expected schema'
    after = dict(outputs=graph.list_versions(down), head=head_of(down))
    check(f"the same graph, bound to report v{version}, keeps the schema hash and the fields of the runs before it",
          saved_pin["dataset_id"] == files_ds and saved_pin["dataset_snapshot"] == pin)
    check(f"the run bound to report v{version} is refused on its input node, and project-transactions and output "
          "are cancelled",
          refusal["status"] == "failed"
          and statuses == {"input": "failed", "project-transactions": "cancelled", "output": "cancelled"})
    check(f"the input node names the refusal: dataset_input_schema_rejected, dataset_schema_content_hash_mismatch "
          f"and {reason}",
          "dataset_input_schema_rejected" in error and "dataset_schema_content_hash_mismatch" in error and reason in error)
    check("the refused run commits no resource and leaves the output versions, the output head and the report versions "
          "as they were",
          not [resource for n in results for resource in n.get("output_resources") or [] if resource["status"] == "committed"]
          and after["outputs"] == before["outputs"] and after["head"] == before["head"]
          and graph.list_versions(files_ds) == before["reports"])

    # The last good output is still there for a reader that starts from the version list it lists after the refusal:
    # output v3 is resolved there, then the transaction that cut it, the files of that transaction and its one file.
    # What the forward build recorded is read only after the download, to cross-check what the reader resolved.
    last_good = next(v for v in after["outputs"] if v["version"] == 3)
    transaction = graph.get_output_transaction(down, last_good["transaction_id"])
    files = files_of(down, last_good["transaction_id"])
    response = p1.download_file(reader, down, files[0]["id"]) if len(files) == 1 else None
    downloaded = None if response is None or response.status != 200 else reports.sha256(response.body)
    check(f"after the refusal the read-only reader resolves output v{last_good['version']} from the version list it "
          f"lists: the transaction that cut it, its one file, and the bytes and the hash that forward build "
          f"{replay['ordinal']} published",
          transaction["status"] == "COMMITTED" and len(files) == 1
          and files[0]["transaction_id"] == last_good["transaction_id"]
          and last_good["transaction_id"] == replay["output_transaction"] and files[0]["id"] == replay["output_file_id"]
          and downloaded == replay["sha256"] and response.body == (state / replay["artifact"]).read_bytes())
    report["schema_gate"] = dict(
        negative=negative, expected_schema_hash=digest, committed_schema_hash=committed.get("content_hash"),
        refused_run_id=refusal["id"],
        nodes=[dict(node_id=n["node_id"], status=n["status"], error=n.get("error")) for n in results],
        outputs=dict(before=before["outputs"], after=after["outputs"]),
        output_head=dict(before=before["head"], after=after["head"]),
        last_good_output=dict(version=last_good["version"], transaction_id=last_good["transaction_id"],
                              file_id=None if len(files) != 1 else files[0]["id"], sha256=downloaded))
    s3.write_json(state / "amazon-report.json", report)
    return dict(reader=reader, other=foreign, p1=p1)


def downstream(state, handles):
    """This round's v3, located from its run receipt by the read-only reader and delivered to a Cassandra store.

    Only the command of ``run.sh amazon`` runs this, after the handoff and while its Dataset
    service is up. ``main`` alone is the entry of ``amazon-preview`` and needs no object service.
    The three settlement lines are delivered twice, by two processes running the same entry, and
    three other sources are refused by the same assertion; the summary goes under
    ``downstream`` in amazon-report.json. An AssertionError is raised when a check failed, after
    the evidence was written.
    """
    published = s3.module("amazon_published", SMOKE / "data-integration-wp2-8/ontology_sync_published.py")
    report = json.loads((state / "amazon-report.json").read_text())
    report["downstream"] = published.run(state, report, dataset=os.environ["WP23_DVS"], reader=handles["reader"],
                                         bin_dir=os.environ["BIN_DIR"], type_id="SettlementLine")
    s3.write_json(state / "amazon-report.json", report)


if __name__ == "__main__":
    state = Path(sys.argv[1])
    downstream(state, main(state))
