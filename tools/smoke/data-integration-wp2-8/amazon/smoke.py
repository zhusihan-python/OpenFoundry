"""Simulated Amazon monthly transaction_view handoff through S3 Connector and versioned Dataset APIs."""
import base64
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

HERE = Path(__file__).resolve().parent
SMOKE = HERE.parents[1]
spec = importlib.util.spec_from_file_location("wp23_s3", SMOKE / "data-integration-wp2-3/orders.py")
s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s3)
reports = s3.module("amazon_reports", HERE / "reports.py")
BUCKET = "amazon-monthly"
# (period, fixture): October holds two files, as one upstream row can.
FILES = (("2026-09", "2026-09"), ("2026-10", "2026-10"), ("2026-10", "2026-10-2"))
# The fileIds of the simulated upload receipts. One month is not one file.
FILE_IDS = {name: str(uuid.uuid5(uuid.NAMESPACE_URL, f"openfoundry-smoke/amazon/{name}")) for _, name in FILES}


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
    s3.write_json(state / "amazon-report.json", report)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
