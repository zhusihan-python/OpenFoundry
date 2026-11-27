import contextlib
import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from ontology_sync import (
    ARCHIVED_ORDER_LINE_PRICES, InvalidSourceError, OntologySyncClient, SyncConflictError,
    archived_transaction, main, rows_of, stable_object_id, sync_order_line_prices,
)
from ontology_sync_noon import EXPECTED_VALUES
from test_ontology_sync import FakeObjectService

TRANSACTION = "1f600730-d3f1-4ffe-8cd0-c4058d27ced0"
TENANT = ARCHIVED_ORDER_LINE_PRICES["tenant"]
VALUES = {"NRPL0001-1": ["AED", "49"], "NRPL0002-1": ["AED", "120"], "NRPL0003-1": ["AED", "30"]}


def row(item_nr="NRPL0001-1", unit_price="49"):
    return {"currency": "AED", "item_nr": item_nr, "market": "AE", "order_nr": item_nr.split("-")[0],
            "store": "RPL01-NAE", "unit_price": unit_price,
            "_source": {**ARCHIVED_ORDER_LINE_PRICES["source"], "line": 1}}


def object_id_of(item_nr="NRPL0001-1"):
    return stable_object_id(TENANT, "OrderLinePrice", TRANSACTION, {"item_nr": item_nr})


class OrderLinePriceTests(unittest.TestCase):
    def setUp(self):
        self.service = FakeObjectService()

    def tearDown(self):
        self.service.shutdown()
        self.service.server_close()

    def endpoint(self):
        return f"http://127.0.0.1:{self.service.server_port}"

    def sync(self, properties=None):
        client = OntologySyncClient(self.endpoint(), TENANT, key_properties=("item_nr",), backoff=0)
        return client.sync("OrderLinePrice", row() if properties is None else properties)

    def test_the_embedded_noon_transaction_holds_its_three_order_lines(self):
        revision, tenant, source, raw = archived_transaction(None, "OrderLinePrice")
        self.assertEqual((revision, tenant), ("c48a709f91408a1182b4d72cac03d21cdfa01a0b", TENANT))
        self.assertEqual(source, {"version": 3, "dataset": "016dbd77-fe2e-4d60-a434-f34647e95e28",
                                  "transaction": TRANSACTION, "file": "a155352f-77b2-4e7c-af98-8e988f9e1b9b",
                                  "sha256": "2ce181d593a204b707822233a69835875ac0aa74acce5e9176393b18e43529fd"})
        rows = rows_of(raw, source)  # refuses an artifact that does not hash to _source.sha256
        self.assertEqual([item["item_nr"] for item in rows], list(VALUES))
        self.assertEqual({item["item_nr"]: [item["currency"], item["unit_price"]] for item in rows}, VALUES)
        self.assertEqual([item["_source"]["line"] for item in rows], [1, 2, 3])
        self.assertEqual({item["item_nr"]: [item["currency"], item["unit_price"]] for item in rows}, EXPECTED_VALUES)

    def test_the_noon_transaction_syncs_once_through_the_order_line_price_entry(self):
        _, tenant, source, raw = archived_transaction(None, "OrderLinePrice")
        rows = rows_of(raw, source)
        first = sync_order_line_prices(self.endpoint(), tenant, rows)
        again = sync_order_line_prices(self.endpoint(), tenant, rows)
        self.assertEqual([result.created for result in first], [True, True, True])
        self.assertEqual([result.created for result in again], [False, False, False])
        self.assertEqual([result.object_id for result in again], [result.object_id for result in first])
        self.assertEqual([result.object_id for result in first], [object_id_of(item_nr) for item_nr in VALUES])
        self.assertEqual({stored["type_id"] for stored in self.service.objects.values()}, {"OrderLinePrice"})
        self.assertNotIn(object_id_of("NRPL0001-1"),
                         {stable_object_id(TENANT, "ProductCost", TRANSACTION, {"item_nr": "NRPL0001-1"})})
        self.assertEqual({key[0] for key in self.service.objects}, {tenant})
        self.assertEqual(len(self.service.objects), 3)

    def test_the_same_order_line_with_another_price_is_rejected_and_nothing_changes(self):
        self.sync()
        before = copy.deepcopy(self.service.objects)
        with self.assertRaisesRegex(SyncConflictError, "different properties"):
            self.sync(row(unit_price="9.99"))
        self.assertEqual(self.service.objects, before)

    def test_the_noon_archive_is_also_read_from_an_evidence_directory(self):
        artifact = b'{"currency":"SAR","item_nr":"A-1","market":"SA","order_nr":"A","store":"RPL01-NAE","unit_price":"1"}\n'
        sha256 = hashlib.sha256(artifact).hexdigest()
        # As in the archived manifest, the v1 build shares the artifact and the hash of the v3 build.
        first = {"output_version": 1, "artifact": "v3.ndjson", "output_transaction": "tx-0",
                 "output_file_id": "file-0", "sha256": sha256}
        build = {"output_version": 3, "artifact": "v3.ndjson", "output_transaction": "tx-1",
                 "output_file_id": "file-1", "sha256": sha256}
        report = {"revision": "r1", "pipeline": {"output_dataset": "dataset-1",
                                                 "refusals": {"crossed_transaction": {"tenant": "tenant-b"}}}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "noon").mkdir()
            (root / "noon" / "v3.ndjson").write_bytes(artifact)
            (root / "noon" / "noon-report.json").write_text(json.dumps(report))
            (root / "manifest.json").write_text(json.dumps(
                {"revision": "r1", "sources": {"noon": {"builds": [first, build]}}}))
            revision, tenant, source, raw = archived_transaction(root, "OrderLinePrice")
            self.assertEqual((revision, tenant, raw), ("r1", "tenant-b", artifact))
            self.assertEqual(source, {"version": 3, "dataset": "dataset-1", "transaction": "tx-1",
                                      "file": "file-1", "sha256": sha256})
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main([self.endpoint(), "--type", "OrderLinePrice", "--evidence-root", directory])
            self.assertEqual((code, json.loads(out.getvalue())["source"]), (0, source))
            (root / "manifest.json").write_text(json.dumps({"revision": "r2", "sources": {}}))
            with self.assertRaisesRegex(InvalidSourceError, "differs"):
                archived_transaction(root, "OrderLinePrice")

    def test_the_command_syncs_the_noon_transaction_once_with_the_explicit_type(self):
        def run(*arguments):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main([self.endpoint(), *arguments])
            return code, json.loads(out.getvalue())

        code, first = run("--type", "OrderLinePrice")
        again_code, again = run("--type", "OrderLinePrice", "--tenant", TENANT)
        self.assertEqual((code, again_code), (0, 0))
        self.assertEqual(list(first), ["revision", "tenant", "source", "results", "created", "existing"])
        self.assertEqual((first["created"], first["existing"]), (3, 0))
        self.assertEqual((again["created"], again["existing"]), (0, 3))
        self.assertEqual(list(first["results"][0]), ["line", "item_nr", "object_id", "created"])
        self.assertEqual([(item["line"], item["item_nr"]) for item in first["results"]],
                         [(1, "NRPL0001-1"), (2, "NRPL0002-1"), (3, "NRPL0003-1")])
        self.assertEqual([item["object_id"] for item in again["results"]],
                         [item["object_id"] for item in first["results"]])
        self.assertEqual((first["revision"], first["tenant"], first["source"]),
                         (ARCHIVED_ORDER_LINE_PRICES["revision"], TENANT, ARCHIVED_ORDER_LINE_PRICES["source"]))
        self.assertEqual(len(self.service.objects), 3)

    def test_the_command_stops_with_exit_code_1_on_a_noon_conflict(self):
        object_id = object_id_of()
        self.service.objects[(TENANT, object_id)] = {"tenant": TENANT, "id": object_id, "type_id": "OrderLinePrice",
                                                     "version": 1, "payload": row(unit_price="9.99")}
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main([self.endpoint(), "--type", "OrderLinePrice"])
        self.assertEqual((code, out.getvalue()), (1, ""))
        self.assertIn("SyncConflictError", err.getvalue())
        self.assertEqual(len(self.service.objects), 1)

    def test_a_type_that_is_not_synced_is_refused(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as stopped:
            main([self.endpoint(), "--type", "SettlementReport"])
        self.assertEqual(stopped.exception.code, 2)
        self.assertEqual(self.service.requests, [])


if __name__ == "__main__":
    unittest.main()
