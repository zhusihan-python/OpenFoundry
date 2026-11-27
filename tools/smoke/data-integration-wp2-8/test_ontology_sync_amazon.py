import contextlib
import copy
import hashlib
import io
import json
import re
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from ontology_sync import (
    ARCHIVED_SETTLEMENT_LINES, SETTLEMENT_LINE_KEY, OntologySyncClient, SyncConflictError, archived_transaction, main,
    row_key, rows_of, stable_object_id, sync_settlement_lines,
)
from ontology_sync_amazon import (
    EXPECTED_LINES, archived_lines, changed_total_is_rejected, held_lines, readback, replay_twice, verify_archive,
)
from test_ontology_sync import FakeObjectService, Handler

TRANSACTION = "18b8eb56-d952-4feb-9a61-d4d06f79f296"
TENANT = ARCHIVED_SETTLEMENT_LINES["tenant"]
SOURCE = ARCHIVED_SETTLEMENT_LINES["source"]
# The object ids of the three archived lines: tenant, type, transaction, _source.file and _source.line.
OBJECT_IDS = ["e22bd0a8-ea05-14eb-b49f-6e8ffe1bfff0", "7c73f492-ecfc-1569-a820-73c76f4c974d",
              "3a77ce9d-8b6a-12b5-92e7-f8f5b2316616"]
LIST = "/api/v1/ontology/types/SettlementLine/objects/?per_page=50"
DETAIL = re.compile(r"^/api/v1/ontology/types/SettlementLine/objects/([^/?]+)$")
LINES = [(1, "Order", "000-0000000-0000003", "DEMO-SKU-C3", "59.57"),
         (2, "Order", "000-0000000-0000004", "DEMO-SKU-D4", "7.44"),
         (3, "Refund", "000-0000000-0000004", "DEMO-SKU-D4", "-7.44")]


def archived():
    """(source, rows, archived) of the embedded Amazon transaction: floats for the sync, exact decimals for the checks."""
    _, _, source, raw = archived_transaction(None, "SettlementLine")
    return source, rows_of(raw, source), archived_lines(raw, source)


def object_id_of(line, file=SOURCE["file"], transaction=TRANSACTION, tenant=TENANT):
    return stable_object_id(tenant, "SettlementLine", transaction, {"_source.file": file, "_source.line": line})


def through_service(value):
    """A payload as the object service answers it: Go reads every JSON number as a float64 and
    writes an integral one without a fraction."""
    if isinstance(value, dict):
        return {name: through_service(item) for name, item in value.items()}
    if isinstance(value, list):
        return [through_service(item) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        number = float(value)
        return int(number) if number.is_integer() else number
    return value


def ontology_object(stored):
    """The wire shape of objects_bridge.go for a stored object."""
    return {"id": stored["id"], "object_type_id": stored["type_id"], "properties": through_service(stored["payload"]),
            "created_by": "system", "created_at": "1970-01-01T00:00:00.001Z", "updated_at": "1970-01-01T00:00:00.001Z"}


class OntologyHandler(Handler):
    """Handler plus the list and detail routes of the SettlementLine type, answered from the same store."""

    def do_GET(self):
        tenant = self.headers.get("x-of-tenant")
        listed, detail = self.path == LIST, DETAIL.match(self.path)
        if not listed and not detail:
            return super().do_GET()
        self.server.requests.append(("GET", self.path))
        with self.server.lock:
            held = [copy.deepcopy(obj) for (owner, _), obj in self.server.objects.items()
                    if owner == tenant and obj["type_id"] == "SettlementLine"]
        if detail:
            found = [obj for obj in held if obj["id"] == detail.group(1)]
            if not found:
                return self.reply({"error": "not found"}, 404)
            return self.reply(ontology_object(found[0]))
        self.reply({"data": [ontology_object(obj) for obj in held], "total": len(held), "page": 1, "per_page": 50})


class FakeOntologyService(FakeObjectService):
    def __init__(self):
        super().__init__()
        self.RequestHandlerClass = OntologyHandler  # read per request, so it can be replaced after the start


class SettlementLineTests(unittest.TestCase):
    def setUp(self):
        self.service = FakeOntologyService()

    def tearDown(self):
        self.service.shutdown()
        self.service.server_close()

    def endpoint(self):
        return f"http://127.0.0.1:{self.service.server_port}"

    def paths(self):
        return [path for method, path in self.service.requests]

    def store(self, tenant, object_id, payload):
        self.service.objects[(tenant, object_id)] = {
            "tenant": tenant, "id": object_id, "type_id": "SettlementLine", "version": 1, "payload": payload,
            "owner": "00000000-0000-1000-8000-000000000001", "created_at_ms": 1, "updated_at_ms": 1}

    def test_the_embedded_amazon_transaction_holds_its_three_settlement_lines(self):
        revision, tenant, source, raw = archived_transaction(None, "SettlementLine")
        self.assertEqual((revision, tenant), ("c48a709f91408a1182b4d72cac03d21cdfa01a0b", TENANT))
        self.assertEqual(source, {"version": 3, "dataset": "e5cd197b-4337-4d42-b8d2-410b3174e41e",
                                  "transaction": TRANSACTION, "file": "8bfb76ec-9f57-4e49-9ba5-f2f3cffdfdf3",
                                  "sha256": "2adc3f19f70041f150364fa81f33d275c19667204e68efabd28389551e2ce464"})
        self.assertEqual((len(raw), raw.count(b"\n")), (1640, 3))
        rows = rows_of(raw, source)  # refuses an artifact that does not hash to _source.sha256
        archived = archived_lines(raw, source)
        self.assertEqual([row["_source"]["line"] for row in archived], [1, 2, 3])
        for row, (line, kind, order, sku, total) in zip(archived, LINES):
            with self.subTest(line=line):
                self.assertEqual((row["type"], row["order id"], row["sku"], str(row["total"])), (kind, order, sku, total))
                self.assertIsInstance(row["total"], Decimal)
                self.assertEqual(row["_source"], {**source, "line": line})
        self.assertEqual([{**{name: row[name] for name in EXPECTED_LINES[0]}, "total": str(row["total"])}
                          for row in archived], EXPECTED_LINES)
        self.assertEqual([row["total"] for row in rows], [59.57, 7.44, -7.44])  # the sync sends floats

    def test_a_key_property_of_the_source_is_read_from_the_source(self):
        _, rows, _ = archived()
        third = rows[2]
        self.assertEqual(row_key(third, third["_source"], SETTLEMENT_LINE_KEY),
                         {"_source.file": SOURCE["file"], "_source.line": 3})
        # a property of the row that is named like a source attribute is not consulted
        decoy = {**third, "_source.file": "not-the-file"}
        self.assertEqual(row_key(decoy, decoy["_source"], SETTLEMENT_LINE_KEY)["_source.file"], SOURCE["file"])

    def test_the_amazon_transaction_syncs_once_through_the_settlement_line_entry(self):
        _, rows, _ = archived()
        first = sync_settlement_lines(self.endpoint(), TENANT, rows)
        again = sync_settlement_lines(self.endpoint(), TENANT, rows)
        self.assertEqual([result.created for result in first], [True, True, True])
        self.assertEqual([result.created for result in again], [False, False, False])
        self.assertEqual([result.object_id for result in again], [result.object_id for result in first])
        self.assertEqual([result.object_id for result in first], OBJECT_IDS)
        self.assertEqual({(tenant, obj["type_id"]) for (tenant, _), obj in self.service.objects.items()},
                         {(TENANT, "SettlementLine")})
        self.assertEqual(len(self.service.objects), 3)

    def test_two_lines_of_one_order_and_sku_are_two_objects(self):
        _, rows, _ = archived()
        self.assertEqual((rows[1]["order id"], rows[1]["sku"]), (rows[2]["order id"], rows[2]["sku"]))
        self.assertNotEqual(OBJECT_IDS[1], OBJECT_IDS[2])
        for line, key in ((1, "tenant"), (1, "type"), (3, "file"), (3, "transaction")):
            with self.subTest(line=line, changed=key):
                self.assertNotEqual(object_id_of(line), {
                    "tenant": lambda: object_id_of(line, tenant="another-tenant"),
                    "type": lambda: stable_object_id(TENANT, "ProductCost", TRANSACTION,
                                                      {"_source.file": SOURCE["file"], "_source.line": line}),
                    "file": lambda: object_id_of(line, file="another-file"),
                    "transaction": lambda: object_id_of(line, transaction="another-transaction"),
                }[key]())

    def test_a_line_of_another_file_or_transaction_is_another_object(self):
        _, rows, _ = archived()
        client = OntologySyncClient(self.endpoint(), TENANT, key_properties=SETTLEMENT_LINE_KEY, backoff=0)
        other_file = {**rows[2], "_source": {**rows[2]["_source"], "file": "another-file"}}
        other_transaction = {**rows[2], "_source": {**rows[2]["_source"], "transaction": "another-transaction"}}
        results = [client.sync("SettlementLine", row) for row in (rows[2], other_file, other_transaction)]
        self.assertEqual([result.created for result in results], [True, True, True])
        self.assertEqual(len({result.object_id for result in results}), 3)
        self.assertEqual(len(self.service.objects), 3)

    def test_the_third_line_with_another_total_is_rejected_and_nothing_changes(self):
        _, rows, _ = archived()
        sync_settlement_lines(self.endpoint(), TENANT, rows)
        before = copy.deepcopy(self.service.objects)
        client = OntologySyncClient(self.endpoint(), TENANT, key_properties=SETTLEMENT_LINE_KEY, backoff=0)
        changed = {**rows[2], "total": 7.44}
        self.assertEqual(changed["_source"], rows[2]["_source"])
        with self.assertRaisesRegex(SyncConflictError, "different properties"):
            client.sync("SettlementLine", changed)
        self.assertEqual(self.service.objects, before)

    def test_the_amazon_archive_is_also_read_from_an_evidence_directory(self):
        _, _, embedded, raw = archived_transaction(None, "SettlementLine")
        sha256 = hashlib.sha256(raw).hexdigest()
        names = ["amazon/v3.ndjson", "amazon/amazon-report.json", "manifest.json"]

        def write_sums(root):
            (root / "SHA256SUMS").write_text("".join(
                f"{hashlib.sha256((root / name).read_bytes()).hexdigest()}  {name}\n" for name in names))

        # As in the archived manifest, the v1 build shares the artifact and the hash of the v3 build.
        first = {"output_version": 1, "artifact": "v3.ndjson", "output_transaction": "tx-0", "output_file_id": "file-0",
                 "sha256": sha256}
        build = {"output_version": 3, "artifact": "v3.ndjson", "output_transaction": "tx-1",
                 "output_file_id": "file-1", "sha256": sha256, "bytes": len(raw)}
        manifest = {"revision": "r1", "sources": {"amazon": {"builds": [first, build]}}}
        report = {"revision": "r1", "organization": "tenant-b", "pipeline": {
            "output_dataset": "dataset-1",
            # the report's own build of v3 and its download of that version
            "builds": [{"output_version": 3, "output_transaction": "tx-1", "output_file_id": "file-1",
                        "artifact": "v3.ndjson", "sha256": sha256}],
            "history": [{"version": 3, "transaction": "tx-1", "download_sha256": sha256, "size_bytes": len(raw),
                         "row_count": 3, "listed_files": [{"file_id": "file-1", "transaction_id": "tx-1"}]}]}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "amazon").mkdir()
            (root / "amazon" / "v3.ndjson").write_bytes(raw)
            (root / "amazon" / "amazon-report.json").write_text(json.dumps(report))
            (root / "manifest.json").write_text(json.dumps(manifest))
            write_sums(root)
            revision, tenant, source, raw = archived_transaction(root, "SettlementLine")
            self.assertEqual((revision, tenant, raw), ("r1", "tenant-b", raw))
            self.assertEqual(source, {"version": 3, "dataset": "dataset-1", "transaction": "tx-1",
                                      "file": "file-1", "sha256": embedded["sha256"]})
            checked = verify_archive(root, source, raw)
            self.assertEqual(sorted(checked["checksums"]), sorted(names))
            self.assertEqual((checked["bytes"], checked["rows"], checked["output"]["transaction"]), (len(raw), 3, "tx-1"))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main([self.endpoint(), "--type", "SettlementLine", "--evidence-root", directory])
            self.assertEqual((code, json.loads(out.getvalue())["source"]), (0, source))
            (root / "SHA256SUMS").write_text(f"{'0' * 64}  amazon/amazon-report.json\n")
            with self.assertRaisesRegex(AssertionError, "recorded"):
                verify_archive(root, source, raw)
            build["bytes"] += 1
            (root / "manifest.json").write_text(json.dumps(manifest))
            write_sums(root)
            with self.assertRaises(AssertionError):
                verify_archive(root, source, raw)
            build["bytes"] -= 1
            report["pipeline"]["history"][0]["download_sha256"] = "0" * 64  # the report downloaded other bytes
            (root / "amazon" / "amazon-report.json").write_text(json.dumps(report))
            (root / "manifest.json").write_text(json.dumps(manifest))
            write_sums(root)
            with self.assertRaises(AssertionError):
                verify_archive(root, source, raw)

    def test_the_command_syncs_the_amazon_transaction_once_with_the_explicit_type(self):
        def run(*arguments):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main([self.endpoint(), *arguments])
            return code, json.loads(out.getvalue())

        code, first = run("--type", "SettlementLine")
        again_code, again = run("--type", "SettlementLine", "--tenant", TENANT)
        self.assertEqual((code, again_code), (0, 0))
        self.assertEqual(list(first), ["revision", "tenant", "source", "results", "created", "existing"])
        self.assertEqual((first["created"], first["existing"]), (3, 0))
        self.assertEqual((again["created"], again["existing"]), (0, 3))
        self.assertEqual(list(first["results"][0]), ["line", "settlement id", "object_id", "created"])
        self.assertEqual([(item["line"], item["settlement id"]) for item in first["results"]],
                         [(1, 1000000010), (2, 1000000011), (3, 1000000012)])
        self.assertEqual([item["object_id"] for item in again["results"]], [item["object_id"] for item in first["results"]])
        self.assertEqual((first["revision"], first["tenant"], first["source"]),
                         (ARCHIVED_SETTLEMENT_LINES["revision"], TENANT, SOURCE))
        self.assertEqual(len(self.service.objects), 3)

    def test_the_command_stops_with_exit_code_1_on_an_amazon_conflict(self):
        _, rows, _ = archived()
        self.store(TENANT, OBJECT_IDS[2], {**rows[2], "total": 7.44})
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main([self.endpoint(), "--type", "SettlementLine"])
        self.assertEqual((code, out.getvalue()), (1, ""))
        self.assertIn("SyncConflictError", err.getvalue())
        self.assertEqual(self.service.objects[(TENANT, OBJECT_IDS[2])]["payload"]["total"], 7.44)
        self.assertEqual(sorted(key[1] for key in self.service.objects), sorted(OBJECT_IDS))

    def test_each_delivery_is_read_back_on_its_own_through_the_list_and_the_detail_route(self):
        source, rows, archived_rows = archived()
        evidence = replay_twice(self.endpoint(), TENANT, rows, archived_rows)
        first, second = evidence["deliveries"]
        self.assertEqual((evidence["passed"], first["count"], first["created"], second["count"], second["created"]),
                         (True, 3, 3, 3, 0))
        for line, (number, kind, order, sku, total) in zip(first["lines"], LINES):
            with self.subTest(line=number):
                self.assertEqual((line["line"], line["type"], line["order id"], line["sku"], line["total"]),
                                 (number, kind, order, sku, total))
                self.assertEqual(line["object_id"], OBJECT_IDS[number - 1])
        self.assertEqual((first["total"], second["total"]), ("59.57", "59.57"))
        self.assertEqual(second["lines"], first["lines"])
        self.assertEqual([(item["object_id"], item["created"]) for item in second["results"]],
                         [(object_id, False) for object_id in OBJECT_IDS])
        self.assertEqual(self.paths().count(LIST), 2)
        self.assertEqual(sum(bool(DETAIL.match(path)) for path in self.paths()), 6)
        self.assertEqual(first["readback"]["details"][OBJECT_IDS[2]]["properties"]["total"], "-7.44")

    def test_a_line_held_twice_fails_the_readback(self):
        _, rows, archived_rows = archived()
        sync_settlement_lines(self.endpoint(), TENANT, rows)
        self.store(TENANT, "00000000-0000-1000-8000-00000000dead", rows[2])  # what a second create would leave
        with self.assertRaisesRegex(AssertionError, "held twice"):
            replay_twice(self.endpoint(), TENANT, rows, archived_rows)

    def test_an_amount_that_differs_from_the_archive_fails_the_readback(self):
        _, rows, archived_rows = archived()
        self.store(TENANT, OBJECT_IDS[2], {**rows[2], "total": -7.45})
        with self.assertRaises(AssertionError):
            held_lines(readback(self.endpoint(), TENANT), archived_rows)

    def test_the_changed_total_is_rejected_and_the_store_is_read_again(self):
        _, rows, archived_rows = archived()
        sync_settlement_lines(self.endpoint(), TENANT, rows)
        before = copy.deepcopy(self.service.objects)
        evidence = changed_total_is_rejected(self.endpoint(), TENANT, rows, archived_rows)
        self.assertEqual((evidence["passed"], evidence["count_after"], evidence["total"]), (True, 3, "59.57"))
        self.assertEqual((evidence["rejected"]["line"], evidence["rejected"]["changed"], evidence["rejected"]["total"]),
                         (3, "total", "7.44"))
        self.assertEqual(evidence["rejected"]["exception"], "SyncConflictError")
        self.assertIn("different properties", evidence["rejected"]["error"])
        self.assertEqual([line["object_id"] for line in evidence["lines"]], OBJECT_IDS)
        self.assertEqual([line["total"] for line in evidence["lines"]], ["59.57", "7.44", "-7.44"])
        self.assertEqual(self.service.objects, before)

    def test_the_changed_total_scenario_fails_when_the_store_does_not_hold_the_line(self):
        _, rows, archived_rows = archived()
        sync_settlement_lines(self.endpoint(), TENANT, rows[:2])
        with self.assertRaisesRegex(AssertionError, "was accepted"):
            changed_total_is_rejected(self.endpoint(), TENANT, rows, archived_rows)


if __name__ == "__main__":
    unittest.main()
