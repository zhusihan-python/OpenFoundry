"""The noon OrderLinePrice rows through the delivery entry of ``ontology_sync_published.py``.

The entry is the one #95 wrote for the lingxing ProductCost rows; ``--type OrderLinePrice``
selects the noon kind, as ``--type`` selects the noon replay in ``ontology_sync_replay.py``.
The order line number is the key of a row, its currency and unit price its values, and the
report is the one of the noon handoff. What the two types share is tested in
``test_ontology_sync_published.py``.
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

import ontology_sync_published as entry
from ontology_sync import (ARCHIVED_ORDER_LINE_PRICES, SOURCE_ATTRIBUTES, archived_transaction, rows_of,
                           stable_object_id, sync_order_line_prices, sync_product_costs)
from ontology_sync_noon import EXPECTED_VALUES, ROUTE
from ontology_sync_published import (KINDS, ORDER_LINE_PRICE, PRODUCT_COST, DatasetReader, differences,
                                     lines_and_values, locate, main, source_of)
from ontology_sync_replay import ROUTE as LINGXING_ROUTE
from test_ontology_sync_published import BASE, FakeDataset, FakeStore, named, published, reader_of, stored

HERE = Path(__file__).resolve().parent
# The noon smoke module is loaded by path: at this directory ``smoke`` is the lingxing one.
_spec = importlib.util.spec_from_file_location("noon_handoff", HERE / "noon/smoke.py")
noon = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(noon)
# The v1 and the v3 of one round: the same bytes and the same hash. V2 is another snapshot.
V1 = V3 = noon.encode(noon.projection(1))
V2 = noon.encode(noon.projection(2))


def order_lines(tenant, raw, source):
    """The readback of a tenant that holds one OrderLinePrice object per order line of ``raw``."""
    return stored(tenant, raw, source, ORDER_LINE_PRICE)


def values(report):
    """The [currency, unit price] each order line number holds in a readback."""
    return {obj["properties"]["item_nr"]: [obj["properties"]["currency"], obj["properties"]["unit_price"]]
            for obj in report["store"]["objects"]}


def located(*pairs):
    """What ``locate`` returns for each (receipt, output) pair of the noon kind, from one Dataset service."""
    reader, _ = reader_of(*(output for _, output in pairs))
    return [locate(reader, receipt, ORDER_LINE_PRICE)[0] for receipt, _ in pairs]


class Kinds(unittest.TestCase):
    """What differs between the two types is held by the kind; ``--type`` chooses one of two."""

    def test_the_noon_kind_names_its_key_its_entry_its_route_and_its_files(self):
        self.assertEqual((ORDER_LINE_PRICE.key, ORDER_LINE_PRICE.sync, ORDER_LINE_PRICE.expected),
                         ("item_nr", sync_order_line_prices, EXPECTED_VALUES))
        self.assertEqual(ORDER_LINE_PRICE.route, ROUTE)  # the route of the noon replay
        self.assertEqual((ORDER_LINE_PRICE.report, ORDER_LINE_PRICE.source_file, ORDER_LINE_PRICE.downloaded_file,
                          ORDER_LINE_PRICE.downstream_file),
                         ("noon-report.json", "order-line-price-source.json", "order-line-price-v3.ndjson",
                          "order-line-price-downstream.json"))

    def test_the_lingxing_kind_keeps_the_names_of_95(self):
        self.assertEqual((PRODUCT_COST.key, PRODUCT_COST.sync, PRODUCT_COST.expected, PRODUCT_COST.value_name,
                          PRODUCT_COST.route), ("sku", sync_product_costs, entry.EXPECTED_COSTS, "cg_price", LINGXING_ROUTE))
        self.assertEqual((PRODUCT_COST.report, PRODUCT_COST.source_file, PRODUCT_COST.downloaded_file,
                          PRODUCT_COST.downstream_file),
                         ("product-report.json", "product-cost-source.json", "product-cost-v3.ndjson",
                          "product-cost-downstream.json"))
        self.assertEqual((KINDS, entry.DEFAULT), ({"ProductCost": PRODUCT_COST, "OrderLinePrice": ORDER_LINE_PRICE},
                                                  "ProductCost"))

    def test_a_type_that_is_not_synced_is_refused(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as stopped:
            main(["--state", ".", "--endpoint", "http://objects.test", "--type", "SettlementLine"])
        self.assertEqual((stopped.exception.code, out.getvalue()), (2, ""))


class LocateTests(unittest.TestCase):
    def test_the_receipt_leads_to_one_transaction_its_version_and_its_file_and_the_bytes_are_hashed(self):
        first = published(V1, version=1)
        second = published(V2, version=2, dataset=first[1]["transaction"]["dataset_id"])
        third = published(V3, version=3, dataset=first[1]["transaction"]["dataset_id"])
        reader, _ = reader_of(first[1], second[1], third[1])
        binding, raw = locate(reader, third[0], ORDER_LINE_PRICE)
        transaction, file = third[1]["transaction"], third[1]["file"]
        self.assertEqual(raw, V3)
        self.assertEqual(source_of(binding), {"version": 3, "dataset": transaction["dataset_id"],
                                              "transaction": transaction["id"], "file": file["id"],
                                              "sha256": hashlib.sha256(V3).hexdigest()})
        self.assertEqual(binding["lines"], {"NRPL0001-1": 1, "NRPL0002-1": 2, "NRPL0003-1": 3})
        self.assertEqual((binding["row_count"], binding["size_bytes"]), (3, len(V3)))
        self.assertEqual(binding["run_id"], third[0]["id"])

    def test_v1_and_v3_of_one_dataset_are_told_apart_by_their_receipt_not_by_bytes_or_hash(self):
        first = published(V1, version=1)
        third = published(V3, version=3, dataset=first[1]["transaction"]["dataset_id"])
        one, three = located(first, third)
        self.assertEqual([name for name in source_of(one) if one[name] != three[name]], ["version", "transaction", "file"])
        self.assertEqual((one["content_hash"], one["sha256"]), (three["content_hash"], three["sha256"]))

    def test_the_v3_this_round_publishes_has_the_bytes_and_hash_of_the_archived_transaction(self):
        # What lets the archived source be refused for its identity alone: it shares every value and
        # the hash of this round and differs in dataset, transaction and file. V2 holds other rows.
        _, _, source, raw = archived_transaction(None, "OrderLinePrice")
        self.assertEqual(raw, ARCHIVED_ORDER_LINE_PRICES["artifact"])
        self.assertEqual((V3, hashlib.sha256(V3).hexdigest()), (raw, source["sha256"]))
        self.assertEqual(lines_and_values([json.loads(line) for line in V3.splitlines()], ORDER_LINE_PRICE),
                         ({"NRPL0001-1": 1, "NRPL0002-1": 2, "NRPL0003-1": 3}, EXPECTED_VALUES))
        self.assertNotEqual(hashlib.sha256(V2).hexdigest(), source["sha256"])

    def test_a_transaction_that_does_not_repeat_the_handle_names_no_source(self):
        receipt, output = published(V3)
        output["transaction"]["metadata"]["transaction_rid"] = f"pipeline-run:{uuid.uuid4()}:output"
        reader, _ = reader_of(output)
        with self.assertRaisesRegex(entry.ReceiptError, "0 transactions of dataset .* repeat 'pipeline-run:"):
            locate(reader, receipt, ORDER_LINE_PRICE)

    def test_a_file_whose_order_line_lacks_its_price_does_not_parse(self):
        receipt, output = published(b'{"currency":"AED","item_nr":"NRPL0001-1"}\n')
        reader, _ = reader_of(output)
        with self.assertRaisesRegex(entry.ReceiptError, "does not parse: KeyError"):
            locate(reader, receipt, ORDER_LINE_PRICE)


class AcceptanceTests(unittest.TestCase):
    """``differences`` of the noon kind is the one acceptance. It is fed the readback of a tenant."""

    def setUp(self):
        self.tenant = "tenant-round"
        self.receipt, self.output = published(V3)
        reader, _ = reader_of(self.output)
        self.binding, _ = locate(reader, self.receipt, ORDER_LINE_PRICE)
        self.source = source_of(self.binding)

    def problems(self, store):
        return differences(self.binding, self.tenant, store, ORDER_LINE_PRICE)

    def with_first_object(self, store, **properties):
        """The store with the properties of its first object changed or added."""
        first = {**store["objects"][0], "properties": {**store["objects"][0]["properties"], **properties}}
        return {**store, "objects": [first, *store["objects"][1:]]}

    def test_the_three_order_lines_of_this_round_are_accepted(self):
        self.assertEqual(self.problems(order_lines(self.tenant, V3, self.source)), [])

    def test_the_id_of_each_order_line_is_derived_from_its_item_nr(self):
        three = order_lines(self.tenant, V3, self.source)
        ids = {obj["properties"]["item_nr"]: obj["id"] for obj in three["objects"]}
        self.assertEqual(len(set(ids.values())), 3)
        self.assertEqual(ids, {item_nr: stable_object_id(self.tenant, "OrderLinePrice", self.source["transaction"],
                                                        {"item_nr": item_nr}) for item_nr in EXPECTED_VALUES})
        # Not the id of a ProductCost row keyed by the same name.
        self.assertTrue(set(ids.values()).isdisjoint(
            stable_object_id(self.tenant, "ProductCost", self.source["transaction"], {"sku": item_nr})
            for item_nr in EXPECTED_VALUES))

    def test_the_archived_transaction_is_refused_though_it_shares_values_and_hash(self):
        _, _, source, raw = archived_transaction(None, "OrderLinePrice")
        self.assertEqual((source["sha256"], raw), (self.source["sha256"], V3))
        problems = self.problems(order_lines(self.tenant, raw, source))
        self.assertEqual(named(problems, " _source."), {"dataset", "transaction", "file"})
        # Beside the id set, the id of each of the three order lines is that of another transaction.
        others = [problem for problem in problems if " _source." not in problem]
        self.assertEqual(len(others), 4, problems)
        self.assertIn("is not", others[0])
        self.assertEqual([problem.rsplit("item_nr ", 1)[1] for problem in others[1:]],
                         ["'NRPL0001-1'", "'NRPL0002-1'", "'NRPL0003-1'"])
        self.assertFalse(any("currency and unit_price" in problem or "is not one of" in problem for problem in problems),
                         problems)
        self.assertEqual(len([problem for problem in problems if " _source." in problem]), 9)

    def test_the_v1_file_of_this_round_is_refused_though_it_shares_dataset_bytes_and_hash(self):
        receipt, output = published(V1, version=1, dataset=self.source["dataset"])
        one, = located((receipt, output))
        self.assertEqual([name for name in ("dataset", "sha256") if one[name] != self.source[name]], [])
        problems = self.problems(order_lines(self.tenant, V1, source_of(one)))
        self.assertEqual(named(problems, " _source."), {"version", "transaction", "file"})
        self.assertFalse(any("currency and unit_price" in problem for problem in problems))

    def test_the_v2_file_of_this_round_is_refused_by_identity_and_values(self):
        receipt, output = published(V2, version=2, dataset=self.source["dataset"])
        two, = located((receipt, output))
        problems = self.problems(order_lines(self.tenant, V2, source_of(two)))
        self.assertEqual(named(problems, " _source."), {"version", "transaction", "file", "sha256"})
        self.assertTrue(any("item_nr 'NRPL0004-1' is not one of" in problem for problem in problems), problems)
        self.assertTrue(any("currency and unit_price ['AED', '49.25'], expected ['AED', '49']" in problem
                            for problem in problems), problems)

    def test_another_count_or_another_type_is_refused(self):
        three = order_lines(self.tenant, V3, self.source)
        self.assertTrue(any("4 listed and 4 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "total": 4, "objects": three["objects"] + [three["objects"][0]]})))
        self.assertTrue(any("3 listed and 2 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "objects": three["objects"][:2]})))
        other = {**three, "objects": [{**three["objects"][0], "object_type_id": "ProductCost"}, *three["objects"][1:]]}
        self.assertTrue(any("is a ProductCost" in problem for problem in self.problems(other)), self.problems(other))

    def test_two_objects_that_swapped_their_complete_properties_are_refused(self):
        # The id set is still the right one and each row agrees with the order line it names, so
        # only the id each object holds for that order line can tell that the two rows changed places.
        three = order_lines(self.tenant, V3, self.source)
        first, second, third = three["objects"]
        swapped = {**three, "objects": [{**first, "properties": second["properties"]},
                                        {**second, "properties": first["properties"]}, third]}
        problems = self.problems(swapped)
        self.assertEqual(len(problems), 2, problems)
        self.assertEqual([problem.split(": ")[1].split(",")[0] for problem in problems],
                         ["is the id of another row", "is the id of another row"])
        self.assertEqual({problem.rsplit("item_nr ", 1)[1] for problem in problems}, {"'NRPL0001-1'", "'NRPL0002-1'"})

    def test_another_currency_or_price_a_wrong_line_or_a_boolean_line_is_refused(self):
        three = order_lines(self.tenant, V3, self.source)
        for change, problem in (({"unit_price": "9.99"}, "currency and unit_price ['AED', '9.99'], expected ['AED', '49']"),
                                ({"currency": "SAR"}, "currency and unit_price ['SAR', '49'], expected ['AED', '49']"),
                                ({"_source": {**self.source, "line": 2}}, "_source.line: 2, its file reads 1"),
                                ({"_source": {**self.source, "line": True}}, "_source.line: True, its file reads 1")):
            with self.subTest(change=change):
                problems = self.problems(self.with_first_object(three, **change))
                self.assertEqual(len(problems), 1, problems)
                self.assertTrue(problems[0].endswith(problem), problems[0])

    def test_a_missing_source_is_refused(self):
        three = order_lines(self.tenant, V3, self.source)
        problems = self.problems(self.with_first_object(three, _source=None))
        self.assertEqual(named(problems, " _source."), set(SOURCE_ATTRIBUTES))

    def test_what_a_readback_holds_is_summarized_by_item_nr(self):
        held = entry._held(order_lines(self.tenant, V3, self.source), ORDER_LINE_PRICE)
        self.assertEqual((held["count"], held["source"], held["values"]), (3, self.source, EXPECTED_VALUES))
        self.assertEqual(entry._held({"total": 0, "objects": []}, ORDER_LINE_PRICE), {"count": 0, "source": None, "values": {}})


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.addCleanup(self.store.server_close)
        self.addCleanup(self.store.shutdown)
        self.endpoint = f"http://127.0.0.1:{self.store.server_port}"
        self.receipt, self.output = published(V3)
        self.dataset = FakeDataset(self.output)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = Path(directory.name)
        self.tenant = self.write_round(self.receipt)
        for patcher in (mock.patch.object(entry, "DatasetReader",
                                          lambda base, token: DatasetReader(base, token, opener=self.dataset)),
                        mock.patch.dict(os.environ, {"WP34_TOKEN": "reader-bearer"})):
            patcher.start()
            self.addCleanup(patcher.stop)

    def write_round(self, receipt):
        """The report of a noon round. Its ``organization`` is the publisher of the round, the
        tenant the entry delivers under."""
        tenant = str(uuid.uuid4())
        (self.state / "noon-report.json").write_text(json.dumps(
            {"revision": "r1", "organization": tenant,
             "pipeline": {"pipeline_id": receipt["pipeline_id"], "builds": [{"ordinal": 3, "output_version": 3}]}}))
        (self.state / "pipeline-3-run.json").write_text(json.dumps(receipt))
        return tenant

    def command(self, *arguments):
        """Run the noon entry once. Return its exit code and the report it printed."""
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["--state", str(self.state), "--dataset-base", BASE, "--endpoint", self.endpoint,
                         "--type", "OrderLinePrice", *arguments])
        return code, json.loads(out.getvalue()) if out.getvalue() else None, err.getvalue()

    def source(self):
        return json.loads((self.state / ORDER_LINE_PRICE.source_file).read_text())

    def test_the_entry_locates_this_round_s_v3_delivers_it_once_and_reads_three_order_lines_back(self):
        code, report, err = self.command()
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(report["tenant"], self.tenant)
        self.assertEqual(report["source"], source_of(self.source()))
        self.assertEqual((report["source"]["sha256"], report["lines"]),
                         (hashlib.sha256(V3).hexdigest(), {"NRPL0001-1": 1, "NRPL0002-1": 2, "NRPL0003-1": 3}))
        self.assertEqual((self.state / ORDER_LINE_PRICE.downloaded_file).read_bytes(), V3)
        self.assertEqual(report["delivered"]["created"], 3)
        self.assertEqual([(item["line"], item["item_nr"], item["created"]) for item in report["delivered"]["results"]],
                         [(1, "NRPL0001-1", True), (2, "NRPL0002-1", True), (3, "NRPL0003-1", True)])
        ids = {item["item_nr"]: item["object_id"] for item in report["delivered"]["results"]}
        self.assertEqual(len(set(ids.values())), 3)
        self.assertEqual(ids, {item_nr: stable_object_id(self.tenant, "OrderLinePrice", report["source"]["transaction"],
                                                        {"item_nr": item_nr}) for item_nr in EXPECTED_VALUES})
        # The list, then each listed object by its id: the store lists its objects by id.
        route = ORDER_LINE_PRICE.route
        self.assertEqual([path for method, path in self.store.requests if method == "GET"][-4:],
                         [route + "?per_page=50", *(route + object_id for object_id in sorted(ids.values()))])
        self.assertEqual((report["store"]["total"], len(report["store"]["objects"])), (3, 3))
        by_line = {obj["properties"]["_source"]["line"]: obj for obj in report["store"]["objects"]}
        self.assertEqual(sorted(by_line), [1, 2, 3])
        for number, line in enumerate(V3.splitlines(), 1):
            row = json.loads(line)
            obj = by_line[number]
            self.assertEqual((obj["id"], obj["object_type_id"]), (ids[row["item_nr"]], "OrderLinePrice"))
            self.assertEqual(obj["properties"], {**row, "_source": {**report["source"], "line": number}})
            self.assertEqual(set(obj["properties"]["_source"]), set(SOURCE_ATTRIBUTES))
        self.assertEqual(values(report), EXPECTED_VALUES)
        self.assertEqual((report["problems"], report["accepted"]), ([], True))
        self.assertEqual(len(self.store.objects), 3)

    def test_the_same_command_run_again_exits_0_creates_nothing_and_reads_the_same_order_lines_back(self):
        _, first, _ = self.command()
        code, again, err = self.command()
        self.assertEqual((code, err), (0, ""))
        self.assertEqual((again["delivered"]["created"], [item["created"] for item in again["delivered"]["results"]]),
                         (0, [False] * 3))
        self.assertEqual((again["store"], len(self.store.objects)), (first["store"], 3))

    def test_an_order_line_of_the_same_key_with_another_price_stops_the_delivery_with_exit_1(self):
        self.assertEqual(self.command()[0], 0)
        source = self.source()
        object_id = stable_object_id(self.tenant, "OrderLinePrice", source["transaction"], {"item_nr": "NRPL0001-1"})
        stored = self.store.objects[(self.tenant, object_id)]
        self.store.objects[(self.tenant, object_id)] = {**stored, "payload": {**stored["payload"], "unit_price": "9.99"}}
        code, report, err = self.command()
        self.assertEqual((code, report), (1, None))
        self.assertIn("SyncConflictError", err)
        self.assertEqual(self.store.objects[(self.tenant, object_id)]["payload"]["unit_price"], "9.99")

    def test_a_tenant_that_received_the_archived_transaction_is_refused_on_readback_with_exit_1(self):
        _, first, _ = self.command()
        _, tenant, source, raw = archived_transaction(None, "OrderLinePrice")
        sync_order_line_prices(self.endpoint, tenant, rows_of(raw, source))
        code, report, err = self.command("--tenant", tenant, "--verify-only")
        self.assertEqual((code, err), (1, ""))
        self.assertEqual(report["delivered"], None)
        self.assertEqual(source["sha256"], first["source"]["sha256"])
        self.assertEqual(values(report), EXPECTED_VALUES)
        self.assertEqual(named(report["problems"], " _source."), {"dataset", "transaction", "file"})
        self.assertIn("is not", [problem for problem in report["problems"] if " _source." not in problem][0])
        self.assertFalse(report["accepted"])
        # This round's tenant is read back again and still holds the same three order lines.
        code, again, _ = self.command("--verify-only")
        self.assertEqual((code, again["store"]), (0, first["store"]))

    def test_a_tenant_that_received_this_round_s_v1_file_is_refused_on_readback_with_exit_1(self):
        self.command()
        receipt, output = published(V1, version=1, dataset=self.source()["dataset"])
        self.dataset.publish(output)
        reader = DatasetReader(BASE, "reader-bearer", opener=self.dataset)
        one, raw = locate(reader, receipt, ORDER_LINE_PRICE)
        sync_order_line_prices(self.endpoint, "tenant-first", rows_of(raw, source_of(one)))
        code, report, _ = self.command("--tenant", "tenant-first", "--verify-only")
        self.assertEqual(code, 1)
        self.assertEqual(named(report["problems"], " _source."), {"version", "transaction", "file"})

    def test_without_type_the_entry_is_the_lingxing_one_and_reads_no_noon_report(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["--state", str(self.state), "--dataset-base", BASE, "--endpoint", self.endpoint])
        self.assertEqual((code, out.getvalue()), (1, ""))
        self.assertIn("FileNotFoundError", err.getvalue())
        self.assertEqual(self.store.objects, {})

    def test_a_run_whose_output_is_not_located_syncs_nothing(self):
        self.dataset.outputs[0]["transaction"]["metadata"]["transaction_rid"] = f"pipeline-run:{uuid.uuid4()}:output"
        code, report, err = self.command()
        self.assertEqual((code, report), (1, None))
        self.assertIn("ReceiptError", err)
        self.assertEqual(self.store.objects, {})


if __name__ == "__main__":
    unittest.main()
