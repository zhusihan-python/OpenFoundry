"""Replay one archived v3 transaction through the sync client against a real Cassandra store.

Regression for the duplicate ProductCost objects of the generic create route: the
same tenant, source transaction and SKUs delivered twice through
``POST /api/v1/ontology/types/ProductCost/objects/`` grew three objects into six.
Every scenario below runs against the object service unchanged. Business callers
sync the transaction once with ``python3 ontology_sync.py ENDPOINT``; this script
is the regression and runs every scenario. ``--type OrderLinePrice`` replays the
archived noon transaction instead; its scenarios are in ``ontology_sync_noon.py``
and use the same stack.

    python3 ontology_sync_replay.py --output FILE [--bin-dir DIR]
    python3 ontology_sync_replay.py --output FILE --evidence-root DIR
    python3 ontology_sync_replay.py --output FILE --endpoint URL

Without ``--endpoint`` the script starts ``ontology_sync.compose.yaml`` with the
``object-database-service`` binary found in ``--bin-dir`` (default ``<repo>/bin``)
and stops it before exiting. ``--endpoint`` replays against a stack that already
runs; its store must hold no ProductCost object for the archived tenant. The
images are not pulled unless ``--pull`` says so.

The evidence JSON is written in every case. The exit code is 0 only when every
scenario passed.
"""

import argparse
import concurrent.futures
import contextlib
import copy
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import urllib.request
import uuid
from pathlib import Path

from ontology_sync import (PRODUCT_COST_KEY, SOURCE_ATTRIBUTES, InvalidSourceError, OntologySyncClient,
                             SyncConflictError, archived_transaction, rows_of, stable_object_id,
                             sync_product_costs)

HERE = Path(__file__).resolve().parent
COMPOSE = HERE / "ontology_sync.compose.yaml"
ROUTE = "/api/v1/ontology/types/ProductCost/objects/"


class ReplayError(Exception):
    """The replay could not reach a store to test against."""


def call(endpoint, method, path, tenant, body=None):
    headers = {"Content-Type": "application/json", "x-of-tenant": tenant}
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(endpoint + path, headers=headers, method=method, data=data)
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def snapshot(endpoint, tenant):
    """The ProductCost objects of one tenant grouped by SKU, each one read back by id."""
    listed = call(endpoint, "GET", ROUTE + "?per_page=50", tenant)
    assert listed["total"] == len(listed["data"])
    groups = {}
    for item in listed["data"]:
        detail = call(endpoint, "GET", ROUTE + item["id"], tenant)
        assert (detail["id"], detail["object_type_id"]) == (item["id"], "ProductCost")
        properties = detail["properties"]
        groups.setdefault(properties["sku"], []).append({"id": detail["id"], "cg_price": properties["cg_price"]})
    return {sku: sorted(items, key=lambda item: item["id"]) for sku, items in groups.items()}


def counted(groups):
    return sum(map(len, groups.values()))


def client_for(endpoint, tenant, **options):
    return OntologySyncClient(endpoint, tenant, key_properties=PRODUCT_COST_KEY, **options)


def stable_id(tenant, row):
    return stable_object_id(tenant, "ProductCost", row["_source"]["transaction"],
                            {name: row[name] for name in PRODUCT_COST_KEY})


def backend_is_cassandra(endpoint, tenant):
    """The service stores objects in a ready Cassandra store."""
    status = call(endpoint, "GET", "/api/v1/object-database/status", tenant)
    assert status["backend"] == "cassandra" and status["ready"] is True, status
    return {"passed": True, "status": status}


def store_starts_empty(endpoint, tenant):
    """The archived tenant holds no ProductCost object before the replay."""
    held = snapshot(endpoint, tenant)
    assert not held, f"tenant {tenant} already holds {counted(held)} ProductCost objects; use an empty store"
    return {"passed": True, "tenant": tenant, "count_before": 0}


def replay_twice(endpoint, tenant, rows, deliveries=2):
    """The same transaction delivered again through sync_product_costs returns the first objects."""
    runs = []
    for delivery in range(1, deliveries + 1):
        results = sync_product_costs(endpoint, tenant, rows)
        by_sku = snapshot(endpoint, tenant)
        runs.append({"delivery": delivery, "count": counted(by_sku), "by_sku": by_sku,
                     "results": [{"sku": row["sku"], "object_id": result.object_id, "created": result.created}
                                 for row, result in zip(rows, results)]})
    first, *again = runs
    assert first["count"] == len(rows) and all(item["created"] for item in first["results"]), first
    assert {sku: [item["cg_price"] for item in items] for sku, items in first["by_sku"].items()} == \
        {row["sku"]: [row["cg_price"]] for row in rows}
    for run in again:
        assert run["count"] == len(rows) and run["by_sku"] == first["by_sku"], run
        assert not any(item["created"] for item in run["results"]), run
    return {"passed": True, "tenant": tenant, "deliveries": runs}


def other_content_is_rejected(endpoint, tenant, rows):
    """A row that carries the key of a stored row with any other content is rejected."""
    client = client_for(endpoint, tenant)
    row = rows[0]
    before = snapshot(endpoint, tenant)
    other = {"version": 4, "dataset": "another-dataset", "file": "another-file", "sha256": "0" * 64,
             "line": row["_source"]["line"] + 1}
    attempts = [("cg_price", {**row, "cg_price": "9.99"})]
    for name in SOURCE_ATTRIBUTES:
        if name != "transaction":  # the transaction is part of the key
            attempts.append(("_source." + name, {**row, "_source": {**row["_source"], name: other[name]}}))
    rejected = []
    for changed, properties in attempts:
        try:
            client.sync("ProductCost", properties)
        except SyncConflictError as error:
            rejected.append({"changed": changed, "error": str(error)})
        else:
            raise AssertionError(f"a row with another {changed} was accepted")
    assert snapshot(endpoint, tenant) == before
    return {"passed": True, "tenant": tenant, "rejected": rejected, "count_after": counted(before)}


def incomplete_source_is_refused(endpoint, tenant, row):
    """A row whose _source lacks an attribute never reaches the store."""
    client = client_for(endpoint, tenant)
    refused = []
    for name in SOURCE_ATTRIBUTES:
        properties = copy.deepcopy(row)
        del properties["_source"][name]
        try:
            client.sync("ProductCost", properties)
        except InvalidSourceError as error:
            refused.append({"missing": name, "error": str(error)})
        else:
            raise AssertionError(f"a row without _source.{name} was accepted")
    return {"passed": True, "refused": refused}


def concurrent_delivery(endpoint, rows, workers, rounds):
    """Workers racing on one transaction leave one object per row and one creator."""
    runs = []
    for _ in range(rounds):
        tenant = str(uuid.uuid4())
        start = threading.Barrier(workers)

        def deliver(_):
            client = client_for(endpoint, tenant)
            client.require_cassandra_backend()
            start.wait(timeout=60)
            return [client.sync("ProductCost", row) for row in rows]

        with concurrent.futures.ThreadPoolExecutor(workers) as pool:
            outcomes = list(pool.map(deliver, range(workers)))
        by_sku = snapshot(endpoint, tenant)
        assert counted(by_sku) == len(rows), by_sku
        held = []
        for index, row in enumerate(rows):
            ids = sorted({outcome[index].object_id for outcome in outcomes})
            creators = sum(outcome[index].created for outcome in outcomes)
            assert ids == [stable_id(tenant, row)] == [item["id"] for item in by_sku[row["sku"]]], (row["sku"], ids)
            assert creators == 1, (row["sku"], creators)
            held.append({"sku": row["sku"], "object_id": ids[0], "created_by": creators})
        runs.append({"tenant": tenant, "count": counted(by_sku), "rows": held})
    return {"passed": True, "workers": workers, "rounds": runs}


def lost_response_is_retried(endpoint, rows):
    """A put that was applied but never answered is retried into the stored object."""
    tenant = str(uuid.uuid4())
    dropped = set()

    def losing(request, timeout):
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
        if request.get_method() == "PUT" and request.full_url not in dropped:
            dropped.add(request.full_url)
            raise ConnectionResetError("response lost after the put was applied")
        return body

    client = client_for(endpoint, tenant, opener=losing)
    results = [client.sync("ProductCost", row) for row in rows]
    assert not any(result.created for result in results)
    assert len(dropped) == len(rows)
    by_sku = snapshot(endpoint, tenant)
    assert counted(by_sku) == len(rows)
    for row, result in zip(rows, results):
        assert [item["id"] for item in by_sku[row["sku"]]] == [result.object_id] == [stable_id(tenant, row)]
    return {"passed": True, "tenant": tenant, "lost_responses": len(dropped), "count": counted(by_sku),
            "results": [{"sku": row["sku"], "object_id": result.object_id} for row, result in zip(rows, results)]}


def ordinary_create_keeps_minting_ids(endpoint, rows, deliveries=2):
    """The generic create route is not the sync entry and still mints an id per request."""
    tenant = str(uuid.uuid4())
    ids = []
    for _ in range(deliveries):
        ids += [call(endpoint, "POST", ROUTE, tenant, {"properties": row})["id"] for row in rows]
    by_sku = snapshot(endpoint, tenant)
    assert len(set(ids)) == len(ids) == len(rows) * deliveries
    assert counted(by_sku) == len(ids)
    assert {stable_id(tenant, row) for row in rows}.isdisjoint(ids)
    return {"passed": True, "tenant": tenant, "returned_ids": ids, "count": counted(by_sku)}


class Stack:
    """The compose stack of ontology_sync.compose.yaml, stopped again on exit."""

    def __init__(self, args):
        self.args = args
        self.binary = Path(args.bin_dir).resolve() / "object-database-service"
        self.endpoint = None

    def compose(self, *words, environment=None):
        return subprocess.run(["docker", "compose", "-f", str(COMPOSE), "-p", self.args.project, *words],
                              capture_output=True, text=True, env=environment or dict(os.environ))

    def __enter__(self):
        if not self.binary.is_file():
            raise ReplayError(f"{self.binary} is missing; build object-database-service into --bin-dir first")
        try:
            started = self.compose("up", "-d", "--pull", self.args.pull,
                                   environment={**os.environ, "OBJECT_BIN_DIR": str(self.binary.parent)})
            if started.returncode:
                raise ReplayError(f"compose up failed:\n{started.stderr.strip()}")
            deadline = time.monotonic() + self.args.startup_timeout
            while time.monotonic() < deadline:
                port = self.compose("port", "objects", "50125")
                if port.returncode == 0 and port.stdout.strip():
                    self.endpoint = "http://" + port.stdout.strip().splitlines()[0]
                    try:
                        call(self.endpoint, "GET", "/api/v1/object-database/status", "probe")
                        return self
                    except OSError:
                        pass
                time.sleep(2)
            logs = self.compose("logs", "--no-color", "--tail", "15", "objects").stdout
            raise ReplayError(f"object service did not answer within {self.args.startup_timeout}s:\n{logs}")
        except BaseException:
            self.stop()
            raise

    def __exit__(self, *exception):
        self.stop()
        return False

    def stop(self):
        self.compose("down", "-v", "--remove-orphans")

    def describe(self):
        release = self.compose("exec", "-T", "db", "cqlsh", "-e", "SELECT release_version FROM system.local").stdout
        found = re.search(r"\b(\d+\.\d+\.\d+)\b", release)
        return {"binary": str(self.binary), "binary_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                "cassandra_release": found.group(1) if found else None, "compose": COMPOSE.name}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--evidence-root", type=Path,
                        help="directory holding manifest.json and the report of the archived transaction's source")
    parser.add_argument("--output", type=Path, required=True, help="file to write the JSON evidence to")
    parser.add_argument("--endpoint", help="object service of a stack that is already running")
    parser.add_argument("--bin-dir", default=str(HERE.parents[2] / "bin"), help="directory holding object-database-service")
    parser.add_argument("--project", default=f"of-ontology-sync-{os.getpid()}")
    parser.add_argument("--pull", choices=("never", "missing", "always"), default="never")
    parser.add_argument("--startup-timeout", type=int, default=240)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--type", choices=("ProductCost", "OrderLinePrice"), default="ProductCost",
                        help="archived transaction to replay; default: ProductCost")
    args = parser.parse_args()
    if args.type == "OrderLinePrice":
        from ontology_sync_noon import replay  # imported here: that module imports this one
        return replay(args)

    revision, tenant, source, raw = archived_transaction(args.evidence_root)
    rows = rows_of(raw, source)
    evidence = {"revision": revision, "tenant": tenant, "source": source, "rows": rows,
                "client_sha256": hashlib.sha256((HERE / "ontology_sync.py").read_bytes()).hexdigest(), "scenarios": {}}
    try:
        with contextlib.ExitStack() as scope:
            if args.endpoint:
                endpoint = args.endpoint.rstrip("/")
            else:
                stack = scope.enter_context(Stack(args))
                endpoint, evidence["stack"] = stack.endpoint, stack.describe()
            evidence["endpoint"] = endpoint
            scenarios = [
                ("backend_is_cassandra", lambda: backend_is_cassandra(endpoint, tenant)),
                ("store_starts_empty", lambda: store_starts_empty(endpoint, tenant)),
                ("replay_twice", lambda: replay_twice(endpoint, tenant, rows)),
                ("other_content_is_rejected", lambda: other_content_is_rejected(endpoint, tenant, rows)),
                ("incomplete_source_is_refused", lambda: incomplete_source_is_refused(endpoint, tenant, rows[0])),
                ("concurrent_delivery", lambda: concurrent_delivery(endpoint, rows, args.workers, args.rounds)),
                ("lost_response_is_retried", lambda: lost_response_is_retried(endpoint, rows)),
                ("ordinary_create_keeps_minting_ids", lambda: ordinary_create_keeps_minting_ids(endpoint, rows)),
            ]
            for name, run in scenarios:
                try:
                    evidence["scenarios"][name] = run()
                except Exception as error:  # recorded, so that one failure does not hide the others
                    evidence["scenarios"][name] = {"passed": False, "error": f"{type(error).__name__}: {error}"}
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    evidence["passed"] = "error" not in evidence and all(s["passed"] for s in evidence["scenarios"].values())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    if "error" in evidence:
        print(f"FAIL replay: {evidence['error']}")
    for name, scenario in evidence["scenarios"].items():
        print(f"{'PASS' if scenario['passed'] else 'FAIL'} {name}" + ("" if scenario["passed"] else ": " + scenario["error"]))
    print(f"{'PASS' if evidence['passed'] else 'FAIL'} evidence {args.output}")
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
