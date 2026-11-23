"""Exercise two deployable audit writer containers against the same Iceberg table."""
import argparse
import base64
import hashlib
import json
import socket
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import iceberg_table as iceberg
from run import Fixture, GROUP, HERE, REPO, TOPIC, check, run, until


def request(base, method, path, body=None, headers=None):
    url = urlsplit(base)
    connection = HTTPConnection(url.hostname, url.port, timeout=50)
    try:
        connection.request(method, path, body, headers or {})
        response = connection.getresponse()
        return response.status, response.read(), response.getheader("Content-Type")
    finally:
        connection.close()


@contextmanager
def proxy(forward):
    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            # Lakekeeper advertises its REST URI using the incoming Host.
            headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in {"connection", "accept-encoding", "content-length"}}
            try:
                result = forward(self.command, self.path, data, headers)
            except OSError:
                # Propagate the real upstream disconnect (including catalog shutdown).
                self.close_connection = True
                return
            if result is None:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            status, body, content_type = result
            self.send_response(status)
            if content_type:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        do_GET = do_POST = do_HEAD = handle_request

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://host.docker.internal:{server.server_port}"
        finally:
            server.shutdown()
            thread.join()


class CatalogRace:
    """Hold two real commits on the same parent, then release both to Lakekeeper."""
    def __init__(self):
        self.address = None
        self.barrier = None
        self.lock = threading.Lock()
        self.commits = []
        self.drop_commit_response = False
        self.dropped = False

    def begin(self):
        self.commits = []
        self.barrier = threading.Barrier(2, timeout=20)

    def forward(self, method, path, data, headers):
        observation = None
        if method == "POST" and path.endswith("/namespaces/of_audit/tables/events"):
            with self.lock:
                barrier = self.barrier
                if barrier and len(self.commits) < 2:
                    requirements = json.loads(data)["requirements"]
                    parents = [x.get("snapshot-id") for x in requirements if
                               x["type"] == "assert-ref-snapshot-id" and x.get("ref") == "main"]
                    observation = {"parents": parents}
                    self.commits.append(observation)
                    if len(self.commits) == 2:
                        self.barrier = None
            if observation is not None:
                barrier.wait()
        result = request(self.address, method, path, data, headers)
        if observation is not None:
            observation["http"] = result[0]
        if (self.drop_commit_response and method == "POST" and path.endswith("/namespaces/of_audit/tables/events")
                and result[0] == 200):
            self.drop_commit_response = False
            self.dropped = True
            return None
        return result


def row():
    return {"event_id": str(uuid4()), "at": 1700000000000000, "kind": "synthetic",
            "payload": '{"accepted":true}', "correlation_id": None}


def batch(rows):
    fields = [{"id": f.field_id, "name": f.name, "type": str(f.field_type), "required": f.required}
              for f in iceberg.SCHEMA.fields]
    spec = {"catalog": "lakekeeper", "namespace": "of_audit", "table": "events", "warehouse": "audit-fixture",
            "partition_transform": "day(at)", "sort_order": "at ASC", "schema": fields}
    return json.dumps({"spec": spec, "rows": rows}).encode()


def read(f, expected):
    table = json.loads(run(f.python, HERE / "iceberg_table.py", "read", "--root", f.table, env=f.env))
    rows = table.pop("rows")
    key = lambda value: value["event_id"]
    check(sorted(rows, key=key) == sorted(expected, key=key), "independent S3 row readback mismatch")
    check(all(p.startswith("s3://audit-fixture/") for p in [table["metadata_location"], *table["data_files"]]),
          "unexpected storage location")
    required = {"write.metadata.delete-after-commit.enabled": "false",
                "write.metadata.previous-versions-max": "999999",
                "history.expire.max-snapshot-age-ms": "9223372036854775807",
                "history.expire.min-snapshots-to-keep": "2147483647"}
    check(required.items() <= table["properties"].items(), "audit retention is not configured")
    return {**table, "row_count": len(rows)}


def check_races(f, race, endpoints):
    expected = []
    results = {}
    same, overlap, left, right, key, extra_a, extra_b = [row() for _ in range(7)]
    cases = [("identical", [same], [same], [204, 204], 1),
             ("overlap", [left, overlap], [overlap, right], [204, 204], 3),
             ("conflict", [key, extra_a], [dict(key, payload='{"accepted":false}'), extra_b], [204, 409], 4)]
    for label, a, b, codes, snapshots in cases:
        race.begin()
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(request, dest, "POST", iceberg.APPEND_PATH, batch(rows),
                                {"Content-Type": "application/json"}) for dest, rows in zip(endpoints, (a, b))]
            actual = [job.result()[0] for job in jobs]
        check(sorted(actual) == codes, f"{label}: writer responses {actual}, catalog commits {race.commits}")
        commits = race.commits
        check(len(commits) == 2 and len(commits[0]["parents"]) == 1 and commits[0]["parents"] == commits[1]["parents"]
              and sorted(c["http"] for c in commits) == [200, 409], f"{label}: no observed catalog race {commits}")
        accepted = {r["event_id"]: r for result, rows in zip(actual, (a, b)) if result == 204 for r in rows}
        expected.extend(dict(r, payload=json.loads(r["payload"])) for r in accepted.values())
        state = read(f, expected)
        check(state["snapshot_count"] == snapshots, f"{label}: unexpected snapshot count")
        results[label] = {"writer_http": actual, "catalog_commits": commits, "table": state}
    return expected, results


def check_lost_ack(f, endpoints, expected):
    requests = []
    lose_response = False
    target = endpoints[0]
    def forward(method, path, data, headers):
        nonlocal lose_response
        result = request(target, method, path, data, headers)
        requests.append((hashlib.sha256(data).hexdigest(), result[0]))
        if lose_response and result[0] == 204:
            lose_response = False
            return None
        return result
    with proxy(forward) as url:
        f.env["FIXTURE_TABLE_URL"] = url
        f.compose("up", "-d", "--no-deps", "--force-recreate", "sink")
        f.start_connector()
        def checkpoint(approved, offset):
            event, receipt = f.observe(f.submit(approved), offset)
            expected.append({k: event.get(k) for k in ("event_id", "at", "kind", "payload", "correlation_id")})
            return receipt
        first = checkpoint(True, 0)
        until("allow offset", lambda: f.offset() == 1)
        read(f, expected)
        lose_response = True
        second = checkpoint(False, 1)
        code = f.wait_failed_sink()
        observed_offset = f.offset()
        check(not lose_response, f"proxy never dropped a 204; writer statuses {[status for _, status in requests]}")
        check(observed_offset == 1, f"offset after losing a committed append response: {observed_offset!r}")
        committed = read(f, expected)
        target = endpoints[1]
        f.compose("start", "sink")
        until("replay acknowledged by second writer", lambda: f.offset() == 2)
        check(read(f, expected) == committed, "replay changed persisted state")
        check(len(requests) == 3 and all(x[1] == 204 for x in requests)
              and requests[1] == requests[2], "retry did not repeat the acknowledged request")
        check(f.kafka("get-offsets", "--topic", TOPIC, "--time", "latest") == TOPIC + ":0:2", "extra Kafka records")
        return {"events": [first, second], "sink_exit": code, "group": GROUP, "offsets": [1, 2], "kafka_end": 2,
                "writer_requests": requests, "table_after_commit_and_after_replay": committed}


def check_catalog_failures(f, race, endpoints, expected):
    headers = {"Content-Type": "application/json"}
    new = row()
    data = batch([new])
    before = read(f, expected)
    f.compose("stop", "lakekeeper")
    status = request(endpoints[0], "POST", iceberg.APPEND_PATH, data, headers)[0]
    check(status == 503, "writer acknowledged a catalog outage")
    f.compose("start", "lakekeeper")
    until("catalog recovery", lambda: request(race.address, "GET", "/health")[0] == 200)
    check(read(f, expected) == before, "outage changed the committed table")
    race.drop_commit_response = True
    first = request(endpoints[0], "POST", iceberg.APPEND_PATH, data, headers)[0]
    check(race.dropped and first in (204, 503), "successful catalog commit response was not dropped")
    expected.append(dict(new, payload=json.loads(new["payload"])))
    committed = read(f, expected)
    check(committed["snapshot_count"] == before["snapshot_count"] + 1, "unknown commit outcome added extra snapshots")
    second = request(endpoints[1], "POST", iceberg.APPEND_PATH, data, headers)[0]
    check(second == 204 and read(f, expected) == committed, "replaying the unknown commit changed the table")
    return {"outage_http": status, "response_lost_http": first, "replay_http": second, "table": committed}


def object_store_request(f, method, target, payload=None):
    # Credentials come from the disposable fixture, never from the user's AWS config.
    args = ["curl", "--silent", "--show-error", "--config", "-", "--aws-sigv4", "aws:amz:us-east-1:s3",
            "--output", "/dev/null", "--write-out", "%{http_code}", "--request", method]
    if payload is not None:
        path = f.root / "bucket-policy-body"
        path.write_bytes(payload)
        md5 = base64.b64encode(hashlib.md5(payload, usedforsecurity=False).digest()).decode()
        args.extend(["--header", "Content-MD5: " + md5, "--data-binary", "@" + str(path)])
    auth = 'user = "' + f.env["FIXTURE_S3_ACCESS"] + ":" + f.env["FIXTURE_S3_SECRET"] + '"'
    url = "http://127.0.0.1:" + f.env["FIXTURE_S3_PORT"] + "/audit-fixture/" + target
    return int(run(*args, url, data=auth + "\n"))


def check_storage_failure(f, race, endpoints, expected):
    # Deny only Parquet writes in this disposable bucket. Metadata and already
    # committed files remain readable throughout, including by the catalog.
    policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*",
              "Action": "s3:PutObject", "Resource": "arn:aws:s3:::audit-fixture/*.parquet"}]}
    before = read(f, expected)
    new = row()
    body, headers = batch([new]), {"Content-Type": "application/json"}
    check(object_store_request(f, "PUT", "?policy", json.dumps(policy).encode()) in (200, 204),
          "could not install fixture bucket policy")
    try:
        denial = object_store_request(f, "PUT", "denial-probe.parquet", b"probe")
        check(denial == 403, f"expected S3 write denial, got {denial}")
        health = request(race.address, "GET", "/health")[0]
        status = request(endpoints[0], "POST", iceberg.APPEND_PATH, body, headers)[0]
        check(health == 200 and status == 503, f"unexpected catalog/writer statuses: {health}/{status}")
        check(read(f, expected) == before, "S3 denial changed the audit table")
    finally:
        check(object_store_request(f, "DELETE", "?policy") in (200, 204), "could not restore fixture bucket")
    replay = request(endpoints[1], "POST", iceberg.APPEND_PATH, body, headers)[0]
    expected.append(dict(new, payload=json.loads(new["payload"])))
    after = read(f, expected)
    check(replay == 204 and after["snapshot_count"] == before["snapshot_count"] + 1, "S3 recovery mismatch")
    return {"s3_put_http": denial, "catalog_http": health, "writer_http": status,
            "replay_http": replay, "previous_snapshot": before["snapshot_id"], "table": after}


def check_retention_guard(f, writer, endpoint, expected):
    before = read(f, expected)
    key = "history.expire.max-snapshot-age-ms"

    def set_age(value):
        f.compose("exec", "-T", writer, "python", "-c",
                  "import sys, writer; t = writer.catalog().load_table(writer.TABLE); "
                  "t.transaction().set_properties({sys.argv[1]:sys.argv[2]}).commit_transaction()", key, value)

    set_age("60000")
    try:
        status = request(endpoint, "POST", iceberg.APPEND_PATH, batch([row()]), {"Content-Type": "application/json"})[0]
        check(status == 409, f"finite retention was accepted: HTTP {status}")
    finally:
        set_age(before["properties"][key])
    after = read(f, expected)
    check(after["snapshot_id"] == before["snapshot_id"] and after["data_files"] == before["data_files"],
          "refusing finite retention changed the data snapshot")
    return {"finite_retention_http": status, "snapshot_unchanged": True}


def verify(f, tag):
    race = CatalogRace()
    with proxy(race.forward) as catalog_url:
        ports = (18081, 18082)
        services = {"object-store": {"ports": [f"127.0.0.1::{port}" for port in ports]}}
        writers = []
        for port in ports:
            name = f"writer-{port}"
            writers.append(name)
            services[name] = {"image": tag, "network_mode": "service:object-store", "environment": {
                "PORT": str(port), "CATALOG_URI": catalog_url + "/catalog", "ICEBERG_WAREHOUSE": "audit-fixture",
                "S3_ENDPOINT": "http://127.0.0.1:" + f.env["FIXTURE_S3_PORT"], "AWS_REGION": "us-east-1",
                "AWS_ACCESS_KEY_ID": f.env["FIXTURE_S3_ACCESS"], "AWS_SECRET_ACCESS_KEY": f.env["FIXTURE_S3_SECRET"]}}
        extra = f.root / "writers.json"
        extra.write_text(json.dumps({"services": services}))
        f.compose_base.extend(["-f", extra])
        def start_writers():
            race.address = f.env["FIXTURE_REST_URL"]
            f.compose("run", "--rm", "--no-deps", writers[0], "python", "writer.py", "create-table")
            f.compose("up", "-d", "--no-deps", *writers)
            return "http://object-store:" + str(ports[0])

        f.start(start_writers)
        endpoints = [f.address("object-store", port) for port in ports]
        for endpoint in endpoints:
            until("writer health", lambda: request(endpoint, "GET", "/healthz")[0] == 200)
        f.stage = "competing catalog commits"
        expected, races = check_races(f, race, endpoints)
        f.stage = "Kafka redelivery to second writer"
        lost_ack = check_lost_ack(f, endpoints, expected)
        f.stage = "catalog outage and lost catalog acknowledgement"
        failures = check_catalog_failures(f, race, endpoints, expected)
        f.stage = "healthy catalog with S3 write denied"
        storage_failure = check_storage_failure(f, race, endpoints, expected)
        f.stage = "reject finite audit retention"
        retention_guard = check_retention_guard(f, writers[0], endpoints[0], expected)
        return {"races": races, "lost_ack": lost_ack, "retention_guard": retention_guard,
                "catalog_failures": failures, "storage_failure": storage_failure,
                "container_ids": [f.compose("ps", "-q", name) for name in writers]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="local image built from services/audit-sink/table-writer")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    check(not output.is_relative_to(REPO), "evidence must be outside the checkout")
    report = {"revision": run("git", "rev-parse", "HEAD"), "dirty": bool(run("git", "status", "--porcelain")),
              "writer_image": run("docker", "image", "inspect", "-f", "{{.Id}}", args.image)}
    with output.open("x") as out, tempfile.TemporaryDirectory(prefix="of-writer-") as directory:
        f = Fixture(Path(directory), Path(sys.executable), lakekeeper=True)
        try:
            f.build()
            f.render()
            report.update(verify(f, args.image))
            report["result"] = "passed"
        except BaseException as exc:
            report.update(result="failed", failed_stage=f.stage, error_type=type(exc).__name__)
            raise
        finally:
            try:
                f.close()
                report["cleaned_up"] = True
            except Exception as exc:
                report.update(result="failed", cleaned_up=False, cleanup_error=type(exc).__name__)
                raise
            finally:
                json.dump(report, out, indent=2)
                out.write("\n")


if __name__ == "__main__":
    main()
