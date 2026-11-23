"""PostgreSQL order snapshots using the same version-pinned consumer as WP2.3."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("order_fixture", HERE.parent / "data-integration-wp2-3/orders.py")
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def sql(statement, user="postgres", *, records=None, denied=False, sslmode="verify-full"):
    passwords = {"postgres": "WP25_ADMIN_PASSWORD", "orders_reader": "WP25_READER_PASSWORD",
                 "other_reader": "WP25_OTHER_PASSWORD"}
    env = dict(os.environ, PGPASSWORD=os.environ[passwords[user]])
    cmd = ["docker", "exec", "-i", "-e", "PGPASSWORD", "-e", "PGSSLMODE=" + sslmode,
           "-e", "PGSSLROOTCERT=/fixture-tls/server.crt", os.environ["WP25_DB_CONTAINER"],
           "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-h", "orders-db", "-U", user, "-d", "orders"]
    if records is not None:
        cmd.extend(["-v", "records=" + json.dumps(records)])
    result = subprocess.run(cmd, input=statement, capture_output=True, text=True, timeout=30, env=env)
    if sslmode == "disable":
        return result.returncode != 0 and "rejects connection" in result.stderr
    if denied:
        return result.returncode != 0 and "permission denied" in result.stderr
    result.check_returncode()
    return result.stdout.strip()


def main(state):
    p1 = fixture.module("wp25_p1", HERE.parent / "business-platform-p1/smoke/smoke.py")
    p1.BASES.update(dataset=os.environ["WP23_DVS"], pipeline=os.environ["WP23_PBS"],
                    connector=os.environ["WP23_CONNECTOR"])
    p1.SECRET_BYTES = os.environ["JWT_SECRET"].encode()
    identities = p1.build_identities()
    owner, other = identities["user_a"].token, identities["user_b"].token
    reader = p1.mint_token(org=identities["org_a"], permissions=[])
    os.environ.update(WP34_PROJECT="wp25", WP34_EVIDENCE=str(state), WP34_DVS=p1.BASES["dataset"],
                      WP34_PBS=p1.BASES["pipeline"], WP34_TOKEN=owner)
    graph = fixture.module("wp25_graph", HERE.parent / "data-integration-wp3-4/harness34.py")
    revision = subprocess.check_output(["git", "-C", str(HERE), "rev-parse", "HEAD"], text=True).strip()
    raw = p1.create_dataset(owner, "postgres-orders", "json")["id"]
    out = p1.create_dataset(owner, "postgres-order-amounts", "json")["id"]
    report = dict(revision=revision, connector="postgresql", branch="main", organization=identities["org_a"],
                  table="sales.orders", input_dataset=raw, output_dataset=out, input_rid="ri.foundry.main.dataset."+raw,
                  output_rid="ri.foundry.main.dataset."+out, snapshots=[], checks=[])

    def check(label, condition):
        report["checks"].append({"name": label, "passed": bool(condition)})
        fixture.write_json(state / "orders-report.json", report)
        if not condition:
            raise AssertionError(label)
        print("PASS", label, flush=True)

    def api(method, path, token=owner, body=None):
        return p1.http("connector", fixture.DC + path, method=method, token=token, payload=body)

    def new_sync(token, role, password, dataset):
        source = api("POST", "/sources", token, {"connector_type": "postgresql", "name": "orders",
                     "config": {"sync": {"postgres_table": {"schema": "sales", "name": "orders"}}}})
        check("created PostgreSQL source for "+role, source.status in (200,201))
        source_id = source.body["id"]
        credential = api("POST", f"/sources/{source_id}/credentials", token,
                         {"kind": "password", "value": json.dumps({"username":role,"password":password})})
        check("bound source-scoped credential for "+role, credential.status in (200,201,204))
        sync = p1.create_sync(token, source_id, dataset)
        check("created batch sync for "+role, sync.status in (200,201))
        return source_id, sync.body["id"]

    check("fixture authenticates read role over verified TLS",
          sql("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()", "orders_reader") == "t")
    check("source rejects plaintext logins", sql("SELECT 1", "orders_reader", sslmode="disable"))
    check("read role cannot modify orders", sql("DELETE FROM sales.orders", "orders_reader", denied=True))
    check("other database role cannot read orders", sql("SELECT * FROM sales.orders", "other_reader", denied=True))
    src, sync = new_sync(owner, "orders_reader", os.environ["WP25_READER_PASSWORD"], raw)
    report.update(source_id=src, sync_id=sync)
    entries = api("GET","/catalog").body["connectors"]
    check("PostgreSQL gallery advertises batch", any(c["type"]=="postgresql" and "batch_sync" in c["capabilities"] for c in entries))
    cap = api("GET",f"/sources/{src}/capabilities").body["capabilities"]
    check("source reports only native batch", cap["native_snapshot_sync"] and cap["modes"]==["batch"] and
          not any(cap[k] for k in ("supports_incremental","supports_cdc","supports_zero_copy","supports_discovery")))
    check("connection test reads the actual empty table", api("POST", f"/sources/{src}/test-connection").body["success"])

    for i, records in enumerate(fixture.SAMPLES,1):
        sql("BEGIN; DELETE FROM sales.orders; INSERT INTO sales.orders SELECT * FROM json_populate_recordset(NULL::sales.orders, :'records'); COMMIT;", records=list(reversed(records)))
        run = p1.run_sync(owner,sync)
        check(f"snapshot v{i} succeeded",run["status"]=="succeeded")
        txn = run["dataset_version_id"]
        version = graph.version_by_txn(raw,txn)
        values,file,data = p1.download_run_artifact(reader,raw,run)
        check(f"independent reader downloaded v{i} in primary-key order",values==records and all(type(r["amount_cents"]) is int for r in values))
        (state/f"downloaded-input-v{i}.ndjson").write_bytes(data)
        (state/f"orders-v{i}.ndjson").write_text("".join(json.dumps(r)+"\n" for r in records))
        report["snapshots"].append(dict(version=version,transaction=txn,sync_run=run["id"],file_id=file["id"],sha256=hashlib.sha256(data).hexdigest()))
    first,second = report["snapshots"]
    check("two distinct versions published",first["version"]<second["version"])
    produced,file_id,response,expected=fixture.verify_pinned_output(state,p1,graph,reader,raw,out,first,check,report)
    foreign=p1.create_dataset(other,"other-orders","json")["id"]
    foreign_src,foreign_sync=new_sync(other,"other_reader",os.environ["WP25_OTHER_PASSWORD"],foreign)
    check("other source connection test denied the actual table",api("POST",f"/sources/{foreign_src}/test-connection",other).body["success"] is False)
    graph.TOKEN=other
    prior=graph.list_versions(foreign)
    denied=p1.run_sync(other,foreign_sync)
    check("foreign database role cannot publish",denied["status"]=="failed" and not denied.get("dataset_version_id") and graph.list_versions(foreign)==prior and not p1.list_files(other,foreign))
    graph.TOKEN=reader
    before=graph.list_versions(raw)
    for name,statement in [
        ("row limit","INSERT INTO sales.orders SELECT 'bulk-'||n,1,'bulk' FROM generate_series(1,10001) n"),
        ("oversized row","TRUNCATE sales.orders; INSERT INTO sales.orders VALUES ('too-large',1,repeat('x',8388608))"),
        ("missing table","DROP TABLE sales.orders")
    ]:
        sql(statement)
        run=p1.run_sync(owner,sync)
        check(name+" preserves published versions",run["status"]=="failed" and not run.get("dataset_version_id") and graph.list_versions(raw)==before)
    check("v2 readable after source failure",graph.scan_all(raw,second["version"])["rows"]==fixture.SAMPLES[1])
    check("pinned output readable after source failure",graph.scan_all(out,produced["version"])["rows"]==expected and p1.download_file(reader,out,file_id).body==response.body)
    for label,res in [("source",api("GET",f"/sources/{src}",other)),("sync",api("POST",f"/syncs/{sync}/run",other)),("download",p1.download_file(other,out,file_id))]:
        check("other platform tenant denied "+label,res.status in (403,404))
    fixture.verify_browser(state,owner)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
