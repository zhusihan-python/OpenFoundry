"""Exercise the public S3 sync, pinned FASTER pipeline and versioned downloads."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import quote, urlencode
from xml.etree import ElementTree

HERE = Path(__file__).resolve().parent
DC = "/api/v1/data-connection"
BUCKET = "order-fixture"
KEY = "orders/门店 01+daily.ndjson"
SAMPLES = [
    [{"order_id": "0007", "amount_cents": 1255, "batch": "first"},
     {"order_id": "0011", "amount_cents": 0, "batch": "first"}],
    [{"order_id": "0007", "amount_cents": 1742, "batch": "second"},
     {"order_id": "0023", "amount_cents": 9321, "batch": "second"}],
]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def scoped_credentials(resource, action):
    policy = {"Version": "2012-10-17",
              "Statement": [{"Effect": "Allow", "Action": [action], "Resource": [resource]}]}
    form = urlencode({"Action": "AssumeRole", "Version": "2011-06-15", "DurationSeconds": 900,
                      "Policy": json.dumps(policy)})
    config = f'user = "{os.environ["STORE_ACCESS"]}:{os.environ["STORE_SECRET"]}"\n'
    response = subprocess.run(["curl", "-q", "--fail", "--silent", "--show-error",
                               "--max-time", "10", "--config", "-", "--aws-sigv4", "aws:amz:us-east-1:s3",
                               "--data", form, os.environ["WP23_STORE"] + "/"],
                              input=config.encode(), stdout=subprocess.PIPE, check=True)
    root = ElementTree.fromstring(response.stdout)
    return {key: root.findtext(".//{*}" + tag) for key, tag in
            [("access_key_id", "AccessKeyId"), ("secret_access_key", "SecretAccessKey"),
             ("session_token", "SessionToken")]}


def s3_request(method, path, body=None, signed=True, credentials=None):
    cmd = ["curl", "-q", "--silent", "--show-error", "--max-time", "10",
           "--output", os.devnull, "--write-out", "%{http_code}", "--request", method]
    config = ""
    if signed:
        if credentials is None:
            credentials = {"access_key_id": os.environ["STORE_ACCESS"], "secret_access_key": os.environ["STORE_SECRET"]}
        config = f'user = "{credentials["access_key_id"]}:{credentials["secret_access_key"]}"\n'
        if credentials.get("session_token"):
            config += f'header = "X-Amz-Security-Token: {credentials["session_token"]}"\n'
        cmd.extend(["--config", "-", "--aws-sigv4", "aws:amz:us-east-1:s3"])
    payload = body.read_bytes() if body else b""
    cmd.extend(["--header", "x-amz-content-sha256: " + hashlib.sha256(payload).hexdigest()])
    if body:
        cmd.extend(["--upload-file", str(body)])
    cmd.append(os.environ["WP23_STORE"] + "/" + quote(path, safe="/"))
    return int(subprocess.check_output(cmd, input=config.encode(), timeout=15))


def main(state):
    p1 = module("wp23_p1", HERE.parent / "business-platform-p1/smoke/smoke.py")
    p1.BASES.update(dataset=os.environ["WP23_DVS"], pipeline=os.environ["WP23_PBS"],
                    connector=os.environ["WP23_CONNECTOR"])
    p1.SECRET_BYTES = os.environ["JWT_SECRET"].encode()
    identities = p1.build_identities()
    owner = identities["user_a"].token
    downstream = p1.mint_token(org=identities["org_a"], permissions=[])
    other = identities["user_b"].token
    os.environ.update(WP34_PROJECT="wp23", WP34_EVIDENCE=str(state),
                      WP34_DVS=p1.BASES["dataset"], WP34_PBS=p1.BASES["pipeline"],
                      WP34_TOKEN=owner)
    graph = module("wp23_graph", HERE.parent / "data-integration-wp3-4/harness34.py")

    revision = subprocess.check_output(["git", "-C", str(HERE), "rev-parse", "HEAD"], text=True).strip()
    report = {"revision": revision, "branch": "main", "organization": identities["org_a"],
              "source_object": f"s3://{BUCKET}/{KEY}", "snapshots": [], "checks": []}

    def check(label, condition):
        report["checks"].append({"name": label, "passed": bool(condition)})
        write_json(state / "orders-report.json", report)
        if not condition:
            raise AssertionError(label)
        print("PASS", label, flush=True)

    def connector(method, path, token=owner, body=None):
        return p1.http("connector", DC + path, method=method, token=token, payload=body)

    check("create private fixture bucket", s3_request("PUT", BUCKET) in (200, 201))
    raw = p1.create_dataset(owner, "orders-raw", "json")["id"]
    out = p1.create_dataset(owner, "orders-for-downstream", "json")["id"]
    report.update(input_dataset=raw, output_dataset=out, input_rid="ri.foundry.main.dataset." + raw,
                  output_rid="ri.foundry.main.dataset." + out)
    src = connector("POST", "/sources", body={"name": "daily-orders", "connector_type": "s3",
                    "config": {"bucket": BUCKET, "sync": {"object_key": KEY}}})
    check(f"create S3 source (HTTP {src.status})", src.status in (200, 201))
    src_id = src.body["id"]
    gallery = connector("GET", "/catalog")
    entries = {item["type"]: item for item in gallery.body["connectors"]}
    check("catalog advertises executable REST and S3 snapshot sync only",
          {kind for kind, item in entries.items() if "batch_sync" in item["capabilities"]} == {"rest_api", "s3"})
    caps = connector("GET", f"/sources/{src_id}/capabilities")
    check("configured S3 source reports native snapshot capability",
          caps.status == 200 and caps.body["capabilities"]["native_snapshot_sync"]
          and caps.body["capabilities"]["modes"] == ["batch"])

    reader = scoped_credentials(f"arn:aws:s3:::{BUCKET}/{KEY}", "s3:GetObject")
    check("issued object-scoped reader credentials", all(reader.values()))
    writer = scoped_credentials(f"arn:aws:s3:::{BUCKET}/{KEY}", "s3:PutObject")
    credential = json.dumps(reader)
    result = connector("POST", f"/sources/{src_id}/credentials",
                       body={"kind": "aws_keys", "value": credential})
    check(f"bind source credential (HTTP {result.status})", result.status in (200, 201, 204))
    sync = p1.create_sync(owner, src_id, raw)
    check("create sync through public API", sync.status in (200, 201))
    sync_id = sync.body["id"]
    report.update(source_id=src_id, sync_id=sync_id)
    for i, rows in enumerate(SAMPLES, 1):
        path = state / f"orders-v{i}.ndjson"
        path.write_bytes(b"".join(json.dumps(row).encode() + b"\n" for row in rows))
        check(f"put source revision {i}", s3_request("PUT", f"{BUCKET}/{KEY}", path, credentials=writer) == 200)
        if i == 1:
            denied = s3_request("PUT", f"{BUCKET}/other-org/daily.ndjson", path, credentials=writer)
            check("tenant writer cannot write the other organization prefix", denied == 403)
            tested = connector("POST", f"/sources/{src_id}/test-connection")
            check("test-connection reads the actual object with the source identity",
                  tested.status == 200 and tested.body["success"] is True)
            check("signed object reader allowed", s3_request("GET", f"{BUCKET}/{KEY}", credentials=reader) == 200)
            check("read-only identity cannot overwrite the object",
                  s3_request("PUT", f"{BUCKET}/{KEY}", path, credentials=reader) == 403)
            foreign_reader = scoped_credentials(f"arn:aws:s3:::{BUCKET}/other-org/*", "s3:GetObject")
            check("other valid storage identity denied order object",
                  s3_request("GET", f"{BUCKET}/{KEY}", credentials=foreign_reader) == 403)
            check("anonymous read refused by object store",
                  s3_request("GET", f"{BUCKET}/{KEY}", signed=False) == 403)
        run = p1.run_sync(owner, sync_id)
        check(f"sync revision {i} ({run.get('status')})",
              run.get("status") == "succeeded")
        txn = run["dataset_version_id"]
        version = graph.version_by_txn(raw, txn)
        values, file, data = p1.download_run_artifact(downstream, raw, run)
        check(f"independent reader downloaded source revision {i}", values == rows)
        (state / f"downloaded-input-v{i}.ndjson").write_bytes(data)
        report["snapshots"].append(dict(version=version, transaction=txn, sync_run=run["id"],
                                        file_id=file["id"], sha256=hashlib.sha256(data).hexdigest()))

    first, second = report["snapshots"]
    check("two distinct published versions", first["version"] < second["version"])
    foreign_source = connector("POST", "/sources", other, {"name": "other-org-orders", "connector_type": "s3",
                               "config": {"bucket": BUCKET, "sync": {"object_key": KEY}}})
    check("second organization has its own valid source", foreign_source.status in (200, 201))
    foreign_id = foreign_source.body["id"]
    bound = connector("POST", f"/sources/{foreign_id}/credentials", other,
                      {"kind": "aws_keys", "value": json.dumps(foreign_reader)})
    check("second organization binds its own restricted identity", bound.status in (200, 201))
    foreign_ds = p1.create_dataset(other, "other-org-output", "json")["id"]
    foreign_sync = p1.create_sync(other, foreign_id, foreign_ds)
    check("second organization owns its destination", foreign_sync.status in (200, 201))
    graph.TOKEN = other
    foreign_versions = graph.list_versions(foreign_ds)
    foreign_run = p1.run_sync(other, foreign_sync.body["id"])
    check("other organization's signed sync cannot read orders or publish a version",
          foreign_run.get("status") == "failed" and not foreign_run.get("dataset_version_id")
          and "HTTP 403" in str(foreign_run.get("error", ""))
          and graph.list_versions(foreign_ds) == foreign_versions and not p1.list_files(other, foreign_ds))
    graph.TOKEN = owner

    # Construct the pinned pipeline AFTER v2 exists. Reading the head would give wrong amounts.
    nodes = graph.graph_nodes(raw, first["version"], first["transaction"], out)
    nodes.insert(1, {"id": "project-orders", "label": "Publish order amounts", "depends_on": ["input"],
                     "transform_type": "select", "config": {"columns": ["order_id", "amount_cents"]}})
    nodes[-1]["depends_on"] = ["project-orders"]
    pid = graph.create_pipeline("order-amounts", nodes)
    built = graph.run_pipeline(pid)
    check(f"pinned pipeline succeeded ({built.get('status')} / {built.get('error_message') or ''})",
          built.get("status") == "succeeded")
    versions = graph.list_versions(out)
    check("one pipeline output version", len(versions) == 1)
    produced = versions[0]
    graph.TOKEN = downstream
    actual = graph.scan_all(out, produced["version"])["rows"]
    expected = [{"order_id": row["order_id"], "amount_cents": row["amount_cents"]} for row in SAMPLES[0]]
    check("downstream read pinned v1 order IDs and integer amounts after v2",
          actual == expected and all(type(r["amount_cents"]) is int for r in actual))
    files = [f for f in p1.list_files(downstream, out) if f.get("transaction_id") == produced["transaction_id"]]
    check("output file bound to pipeline transaction", len(files) == 1)
    file_id = files[0]["id"]
    response = p1.download_file(downstream, out, file_id)
    check("consumer downloaded committed output file", response.status == 200 and bool(response.body))
    downloaded_rows = [json.loads(line) for line in response.body.splitlines() if line.strip()]
    check("downloaded bytes decode to the expected projection", downloaded_rows == expected)
    (state / "published-orders.ndjson").write_bytes(response.body)
    report.update(pipeline_id=pid, pipeline_run=built["id"], build_id=built["id"], output_version=produced["version"],
                  output_transaction=produced["transaction_id"], output_file_id=file_id,
                  output_sha256=hashlib.sha256(response.body).hexdigest(), expected_rows=expected)
    for i, row in enumerate(report["snapshots"]):
        check(f"historical input version {row['version']} independently readable",
              graph.scan_all(raw, row["version"])["rows"] == SAMPLES[i])
    before = graph.list_versions(raw)
    malformed = state / "malformed.ndjson"
    malformed.write_text('{"order_id":"partial"}\ntruncated\n')
    check("stage corrupt object", s3_request("PUT", f"{BUCKET}/{KEY}", malformed, credentials=writer) == 200)
    rejected = p1.run_sync(owner, sync_id)
    check("invalid second row fails without publishing the first row",
          rejected.get("status") == "failed" and "NDJSON" in str(rejected.get("error", ""))
          and not rejected.get("dataset_version_id") and graph.list_versions(raw) == before)
    check("remove synthetic source object", s3_request("DELETE", f"{BUCKET}/{KEY}") == 204)
    failed = p1.run_sync(owner, sync_id)
    check("missing source fails without a new version", failed.get("status") == "failed"
          and not failed.get("dataset_version_id") and graph.list_versions(raw) == before)
    report["failed_sync_run"] = failed["id"]
    check("previous published input remains readable", graph.scan_all(raw, second["version"])["rows"] == SAMPLES[1])
    check("previous output remains readable", graph.scan_all(out, produced["version"])["rows"] == expected)
    check("download after failure has the same bytes",
          p1.download_file(downstream, out, file_id).body == response.body)
    for label, response in [
        ("source", connector("GET", f"/sources/{src_id}", other)),
        ("sync trigger", connector("POST", f"/syncs/{sync_id}/run", other)),
        ("output download", p1.download_file(other, out, file_id)),
    ]:
        check(f"other tenant denied {label} (HTTP {response.status})", response.status in (403, 404))

    if os.getenv("WP23_VERIFY_UI") == "1":
        token_file = state / "browser.jwt"
        try:
            token_file.write_text(owner)
            subprocess.run(["bash", str(HERE / "catalog-ui.sh"), str(state)], check=True)
        finally:
            token_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
