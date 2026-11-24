"""Offline Lingxing product batches through real Connector and Dataset Versioning."""
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
                      WP34_DVS=p1.BASES["dataset"])
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
    s3.write_json(state / "product-report.json", report)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
