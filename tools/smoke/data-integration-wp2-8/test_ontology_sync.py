import contextlib
import copy
import hashlib
import io
import json
import re
import tempfile
import threading
import unittest
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ontology_sync import (
    ARCHIVED, SOURCE_ATTRIBUTES, InvalidSourceError, OntologySyncClient, SyncBackendError, SyncConflictError,
    archived_transaction, main, rows_of, stable_object_id, sync_product_costs,
)

OBJECT = re.compile(r"^/api/v1/object-database/objects/([^/]+)/([^/]+)$")
TRANSACTION = "56ea4fbc-99e4-4a07-98b8-de12f1f272c7"


def row(sku="0007", cost="0.00"):
    return {"cg_price": cost, "sku": sku, "_source": {
        "version": 3, "dataset": "5a2150df-b480-47b7-939b-288025692fea", "transaction": TRANSACTION,
        "file": "96bdec4a-2fa1-4be3-b0fc-ce79e762c593",
        "sha256": "846c2b4dbd3314a84f67c36031a06547f7c60341970fde512aa6afd975c0d6e8", "line": 1}}


def object_id_of(sku="0007"):
    return stable_object_id("tenant-a", "ProductCost", TRANSACTION, {"sku": sku})


class Handler(BaseHTTPRequestHandler):
    """The three routes the sync client uses, with Cassandra's INSERT ... IF NOT EXISTS semantics."""

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.requests.append(("GET", self.path))
        if self.path == "/api/v1/object-database/status":
            return self.reply({"service": "object-database-service", "ready": True, "backend": self.server.backend})
        match = OBJECT.match(self.path)
        if not match:
            return self.reply({"error": "not found"}, 404)
        with self.server.lock:
            found = self.server.objects.get(tuple(match.groups()))
        self.reply(found, 200 if found else 404)

    def do_PUT(self):
        self.server.requests.append(("PUT", self.path))
        match = OBJECT.match(self.path)
        if not match:
            return self.reply({"error": "not found"}, 404)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with self.server.lock:
            if self.server.fail_puts:
                return self.reply({"error": "unavailable"}, self.server.fail_puts.pop(0))
            tenant, object_id = match.groups()
            if (tenant, object_id) in self.server.objects or self.server.unreadable:
                return self.reply({"outcome": "version_conflict", "expected_version": 0, "actual_version": 1})
            self.server.objects[(tenant, object_id)] = {
                "tenant": tenant, "id": object_id, "type_id": body["type_id"], "version": 1,
                "payload": body["payload"], "owner": body["owner"], "created_at_ms": 1, "updated_at_ms": 1}
            self.reply({"outcome": "inserted"})

    def reply(self, value, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(value).encode())


class FakeObjectService(ThreadingHTTPServer):
    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.backend = "cassandra"
        self.objects = {}
        self.fail_puts = []
        self.unreadable = False
        self.requests = []
        self.lock = threading.Lock()
        threading.Thread(target=self.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True).start()


class SyncClientTests(unittest.TestCase):
    def setUp(self):
        self.service = FakeObjectService()

    def tearDown(self):
        self.service.shutdown()
        self.service.server_close()

    def client_with(self, **options):
        return OntologySyncClient(f"http://127.0.0.1:{self.service.server_port}", "tenant-a",
                                  key_properties=("sku",), backoff=0, **options)

    def sync(self, properties=None, **options):
        return self.client_with(**options).sync("ProductCost", row() if properties is None else properties)

    def methods(self):
        return [method for method, _ in self.service.requests]

    def store(self, payload, **changes):
        object_id = object_id_of(payload["sku"])
        stored = {"tenant": "tenant-a", "id": object_id, "type_id": "ProductCost", "version": 1, "payload": payload}
        self.service.objects[("tenant-a", object_id)] = {**stored, **changes}

    def test_the_stable_id_is_a_legal_timeuuid_of_the_source_key(self):
        first = object_id_of()
        self.assertEqual(first, object_id_of())
        self.assertEqual((uuid.UUID(first).version, uuid.UUID(first).variant), (1, uuid.RFC_4122))
        others = {object_id_of("W-2"),
                  stable_object_id("tenant-b", "ProductCost", TRANSACTION, {"sku": "0007"}),
                  stable_object_id("tenant-a", "OtherCost", TRANSACTION, {"sku": "0007"}),
                  stable_object_id("tenant-a", "ProductCost", "another-transaction", {"sku": "0007"})}
        self.assertEqual(len(others), 4)
        self.assertNotIn(first, others)

    def test_key_property_order_does_not_change_the_stable_id(self):
        forward = stable_object_id("tenant-a", "Price", TRANSACTION, {"sku": "0007", "market": "DE"})
        backward = stable_object_id("tenant-a", "Price", TRANSACTION, {"market": "DE", "sku": "0007"})
        self.assertEqual(forward, backward)

    def test_the_same_row_syncs_once_and_is_confirmed_afterwards(self):
        first, second = self.sync(), self.sync()
        self.assertEqual((first.created, second.created), (True, False))
        self.assertEqual(first.object_id, second.object_id)
        self.assertEqual(list(self.service.objects), [("tenant-a", object_id_of())])
        self.assertEqual(self.service.requests[0], ("GET", "/api/v1/object-database/status"))
        self.assertEqual(set(self.methods()), {"GET", "PUT"})

    def test_rows_of_one_transaction_keep_their_own_objects(self):
        results = [self.sync(row(sku)) for sku in ("0007", "W-2", "D3")]
        self.assertEqual(len({result.object_id for result in results}), 3)
        self.assertEqual([result.created for result in results], [True, True, True])

    def test_the_same_key_with_another_cost_is_rejected_and_nothing_changes(self):
        self.sync()
        before = copy.deepcopy(self.service.objects)
        with self.assertRaisesRegex(SyncConflictError, "different properties"):
            self.sync(row(cost="9.99"))
        self.assertEqual(self.service.objects, before)

    def test_every_source_attribute_of_the_stored_object_is_checked(self):
        other = {"version": 4, "dataset": "other-dataset", "transaction": "other-transaction", "file": "other-file",
                 "sha256": "0" * 64, "line": 2}
        for name in SOURCE_ATTRIBUTES:
            with self.subTest(source=name):
                self.service.objects.clear()
                stored = row()
                stored["_source"][name] = other[name]
                self.store(stored)
                with self.assertRaisesRegex(SyncConflictError, "_source." + name):
                    self.sync()

    def test_an_object_of_another_type_or_tenant_is_rejected(self):
        for changes in ({"type_id": "OtherCost"}, {"tenant": "tenant-b"}):
            with self.subTest(changes=changes):
                self.store(row(), **changes)
                with self.assertRaisesRegex(SyncConflictError, "belongs to tenant"):
                    self.sync()

    def test_an_incomplete_source_is_refused_before_anything_is_sent(self):
        for name in SOURCE_ATTRIBUTES:
            with self.subTest(source=name):
                properties = row()
                del properties["_source"][name]
                with self.assertRaisesRegex(InvalidSourceError, name):
                    self.sync(properties)
        for broken in ({"version": 3}, "line.txt", None):
            with self.subTest(source=broken):
                properties = row()
                properties["_source"] = broken
                with self.assertRaises(InvalidSourceError):
                    self.sync(properties)
        self.assertEqual(self.service.requests, [])

    def test_a_row_without_a_key_property_is_refused(self):
        properties = row()
        del properties["sku"]
        with self.assertRaisesRegex(InvalidSourceError, "sku"):
            self.sync(properties)
        self.assertEqual(self.service.requests, [])

    def test_a_lost_put_response_is_retried_and_converges_on_the_stored_object(self):
        lost = []

        def losing(request, timeout):
            with urllib.request.urlopen(request, timeout=timeout) as response:
                applied = response.read()
            if request.get_method() == "PUT" and not lost:
                lost.append(request.full_url)
                raise ConnectionResetError("response lost after the put was applied")
            return applied

        result = self.sync(opener=losing)
        self.assertEqual((result.created, result.object_id), (False, object_id_of()))
        self.assertEqual(len(self.service.objects), 1)
        self.assertEqual(self.methods(), ["GET", "PUT", "PUT", "GET"])

    def test_a_failing_put_is_retried(self):
        self.service.fail_puts = [503, 429]
        result = self.sync()
        self.assertEqual((result.created, len(self.service.objects)), (True, 1))
        self.assertEqual(self.methods().count("PUT"), 3)

    def test_a_rejected_put_is_not_retried(self):
        self.service.fail_puts = [400]
        with self.assertRaisesRegex(SyncBackendError, "returned 400"):
            self.sync()
        self.assertEqual(self.methods().count("PUT"), 1)
        self.assertEqual(self.service.objects, {})

    def test_a_conflict_without_a_readable_object_is_an_error(self):
        self.service.unreadable = True
        with self.assertRaisesRegex(SyncBackendError, "returned 404"):
            self.sync()
        self.assertEqual(self.methods(), ["GET", "PUT", "GET"])
        self.assertEqual(self.service.objects, {})

    def test_a_non_cassandra_backend_is_refused(self):
        self.service.backend = "in-memory"
        with self.assertRaisesRegex(SyncBackendError, "'in-memory'"):
            self.sync()
        self.assertEqual(self.service.requests, [("GET", "/api/v1/object-database/status")])
        self.assertEqual(self.service.objects, {})

    def test_the_archived_transaction_syncs_once_through_the_product_cost_entry(self):
        endpoint = f"http://127.0.0.1:{self.service.server_port}"
        revision, tenant, source, raw = archived_transaction()
        rows = rows_of(raw, source)
        self.assertEqual((revision, tenant, source), (ARCHIVED["revision"], ARCHIVED["tenant"], ARCHIVED["source"]))
        self.assertEqual([row["sku"] for row in rows], ["0007", "W-2", "D3"])
        first = sync_product_costs(endpoint, tenant, rows)
        again = sync_product_costs(endpoint, tenant, rows)
        self.assertEqual([result.created for result in first], [True, True, True])
        self.assertEqual([result.created for result in again], [False, False, False])
        self.assertEqual([result.object_id for result in again], [result.object_id for result in first])
        self.assertEqual(len(self.service.objects), 3)

    def test_the_archive_is_also_read_from_an_evidence_directory(self):
        artifact = b'{"cg_price":"1.00","sku":"A"}\n{"cg_price":"2.00","sku":"B"}\n'
        sha256 = hashlib.sha256(artifact).hexdigest()
        build = {"output_version": 3, "artifact": "v3.ndjson", "output_transaction": "tx-1",
                 "output_file_id": "file-1", "sha256": sha256}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "lingxing").mkdir()
            (root / "lingxing" / "v3.ndjson").write_bytes(artifact)
            (root / "lingxing" / "product-report.json").write_text(json.dumps(
                {"revision": "r1", "organization": "tenant-b", "pipeline": {"output_dataset": "dataset-1"}}))
            (root / "manifest.json").write_text(json.dumps(
                {"revision": "r1", "sources": {"lingxing": {"builds": [{"output_version": 2}, build]}}}))
            revision, tenant, source, raw = archived_transaction(root)
            self.assertEqual((revision, tenant, raw), ("r1", "tenant-b", artifact))
            self.assertEqual(source, {"version": 3, "dataset": "dataset-1", "transaction": "tx-1",
                                      "file": "file-1", "sha256": sha256})
            (root / "manifest.json").write_text(json.dumps({"revision": "r2", "sources": {}}))
            with self.assertRaisesRegex(InvalidSourceError, "differs"):
                archived_transaction(root)

    def test_an_artifact_that_does_not_hash_to_its_source_is_refused(self):
        _, _, source, raw = archived_transaction()
        with self.assertRaisesRegex(InvalidSourceError, "hashes to"):
            rows_of(raw.replace(b"9.25", b"9.35"), source)

    def test_the_command_syncs_once_and_reports_each_row(self):
        endpoint = f"http://127.0.0.1:{self.service.server_port}"

        def run(*arguments):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main([endpoint, *arguments])
            return code, json.loads(out.getvalue())

        code, first = run()
        again_code, again = run("--tenant", ARCHIVED["tenant"])
        self.assertEqual((code, again_code), (0, 0))
        self.assertEqual((first["created"], first["existing"]), (3, 0))
        self.assertEqual((again["created"], again["existing"]), (0, 3))
        self.assertEqual([item["object_id"] for item in again["results"]], [item["object_id"] for item in first["results"]])
        self.assertEqual([(item["line"], item["sku"]) for item in first["results"]], [(1, "0007"), (2, "W-2"), (3, "D3")])
        self.assertEqual(len(self.service.objects), 3)

    def test_the_command_stops_with_exit_code_1_on_a_conflict(self):
        endpoint = f"http://127.0.0.1:{self.service.server_port}"
        tenant, source = ARCHIVED["tenant"], ARCHIVED["source"]
        object_id = stable_object_id(tenant, "ProductCost", source["transaction"], {"sku": "0007"})
        self.service.objects[(tenant, object_id)] = {"tenant": tenant, "id": object_id, "type_id": "ProductCost",
                                                     "version": 1, "payload": row(cost="9.99")}
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main([endpoint])
        self.assertEqual((code, out.getvalue()), (1, ""))
        self.assertIn("SyncConflictError", err.getvalue())
        self.assertEqual(len(self.service.objects), 1)


if __name__ == "__main__":
    unittest.main()
