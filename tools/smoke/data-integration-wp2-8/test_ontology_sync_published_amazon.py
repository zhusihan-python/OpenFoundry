"""The Amazon SettlementLine rows through the delivery entry of ``ontology_sync_published.py``.

The entry is the one #95 wrote for the lingxing ProductCost rows; ``--type SettlementLine``
selects the amazon kind, as ``--type`` selects the Amazon replay in ``ontology_sync_replay.py``.
A settlement line has no key property of its own: as #92 keys it, an object is identified by
``_source.file`` and ``_source.line`` next to the tenant, the type and the transaction, so the
order and the refund of one order id and one SKU hold two objects. The file is the one of the
source the receipt names, which holds no line; the line is the row's own. What the types share
is tested in ``test_ontology_sync_published.py``.
"""

import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
import uuid
from decimal import Decimal
from pathlib import Path
from unittest import mock

import ontology_sync_amazon as amazon
import ontology_sync_published as entry
from ontology_sync import (ARCHIVED_SETTLEMENT_LINES, SETTLEMENT_LINE_KEY, SOURCE_ATTRIBUTES, archived_transaction,
                           rows_of, stable_object_id, sync_settlement_lines)
from ontology_sync_amazon import EXPECTED_LINES, EXPECTED_TOTAL, ROUTE
from ontology_sync_published import (KINDS, ORDER_LINE_PRICE, PRODUCT_COST, SETTLEMENT_LINE, DatasetReader,
                                     differences, lines_and_values, locate, main, source_of)
from test_ontology_sync_published import (BASE, V3 as LINGXING_V3, FakeDataset, FakeStore, named, published, reader_of,
                                          stored)

# The v1 and the v3 of one round: the bytes and the hash of the archived transaction. V2 is the other October
# file, two lines: a fee that names no order, and an order.
V1 = V3 = ARCHIVED_SETTLEMENT_LINES["artifact"]
V2 = (b'{"date/time":"Nov 2, 2026 06:00:00 AM PST","description":"FBA inventory storage fee","order id":null,'
      b'"product sales":0,"quantity":null,"settlement id":1000000013,"sku":null,"total":-9.85,'
      b'"type":"FBA Inventory Fee"}\n'
      b'{"date/time":"Nov 14, 2026 09:30:00 AM PST","description":"Sample item E","order id":"000-0000000-0000005",'
      b'"product sales":11.99,"quantity":1,"settlement id":1000000014,"sku":"DEMO-SKU-E5","total":8.49,'
      b'"type":"Order"}\n')
# What a read-back object may hold in the place of its ``_source`` that is no object.
NO_SOURCE = ("oops", [1], 7, None, True)
NAMES = ("settlement id", "type", "order id", "sku", "total")


def rows_of_file(raw):
    """The rows of one file as the amazon kind reads them: every amount an exact decimal."""
    return [SETTLEMENT_LINE.row(line) for line in raw.splitlines()]


def settlement_lines(tenant, raw, source):
    """The readback of a tenant that holds one SettlementLine object per line of ``raw``, keyed as #92 keys it."""
    return stored(tenant, raw, source, SETTLEMENT_LINE)


def expected_ids(tenant, source):
    """The ids of the lines of one file, by line: the file of the source and the line of the row."""
    return {number: stable_object_id(tenant, "SettlementLine", source["transaction"],
                                     {"_source.file": source["file"], "_source.line": number}) for number in (1, 2, 3)}


def lines_of(store):
    """The objects of a readback by the line each claims."""
    return {obj["properties"]["_source"]["line"]: obj for obj in store["objects"]}


def located(*pairs):
    """What ``locate`` returns for each (receipt, output) pair of the amazon kind, from one Dataset service."""
    reader, _ = reader_of(*(output for _, output in pairs))
    return [locate(reader, receipt, SETTLEMENT_LINE)[0] for receipt, _ in pairs]


def replaced(store, number, **properties):
    """The store with properties of the object of one line replaced; ``_source`` among them."""
    objects = list(store["objects"])
    held = lines_of(store)[number]
    objects[objects.index(held)] = {**held, "properties": {**held["properties"], **properties}}
    return {**store, "objects": objects}


def swapped(store, left, right):
    """The store with the complete properties of the objects of two lines exchanged, their ids staying."""
    objects = list(store["objects"])
    objects[left], objects[right] = ({**objects[left], "properties": objects[right]["properties"]},
                                     {**objects[right], "properties": objects[left]["properties"]})
    return {**store, "objects": objects}


def values_of(obj):
    """What a read-back object holds of the five names of a settlement line, as its JSON report keeps them."""
    return [obj["properties"][name] for name in NAMES]


class Kinds(unittest.TestCase):
    """What differs for the amazon kind is held by the kind; ``--type`` chooses one of three."""

    def test_the_amazon_kind_is_keyed_by_file_and_line_and_reads_amounts_as_exact_decimals(self):
        self.assertEqual((SETTLEMENT_LINE.key, SETTLEMENT_LINE.sync, SETTLEMENT_LINE.key_properties, SETTLEMENT_LINE.keys),
                         ("settlement id", sync_settlement_lines, SETTLEMENT_LINE_KEY, SETTLEMENT_LINE_KEY))
        self.assertEqual((PRODUCT_COST.by_line, ORDER_LINE_PRICE.by_line, SETTLEMENT_LINE.by_line), (False, False, True))
        self.assertEqual((SETTLEMENT_LINE.name_property, PRODUCT_COST.name_property, ORDER_LINE_PRICE.name_property),
                         ("line", "sku", "item_nr"))
        self.assertEqual((SETTLEMENT_LINE.row('{"total":59.57}')["total"], SETTLEMENT_LINE.row('{"t":1.5}')["t"]),
                         (Decimal("59.57"), Decimal("1.5")))
        self.assertIs(type(PRODUCT_COST.row('{"cg_price":1.5}')["cg_price"]), float)
        self.assertIs(SETTLEMENT_LINE.get, amazon.read)

    def test_the_file_of_the_key_comes_from_the_source_and_the_line_from_the_row(self):
        # The source of a file names no line; the line is the name of the row.
        source = {"version": 3, "dataset": "d", "transaction": "t", "file": "f", "sha256": "s"}
        self.assertEqual(SETTLEMENT_LINE.key_of(source, 2), {"_source.file": "f", "_source.line": 2})
        self.assertEqual(PRODUCT_COST.key_of(source, "0007"), {"sku": "0007"})
        self.assertEqual(ORDER_LINE_PRICE.key_of(source, "NRPL0001-1"), {"item_nr": "NRPL0001-1"})
        # The id derived this way is the one #92 derives for the row of that source at that line.
        archived = ARCHIVED_SETTLEMENT_LINES["source"]
        row = {**json.loads(V3.splitlines()[1]), "_source": {**archived, "line": 2}}
        self.assertEqual(stable_object_id("t", "SettlementLine", archived["transaction"], SETTLEMENT_LINE.key_of(archived, 2)),
                         amazon.stable_id("t", row))

    def test_the_kind_names_its_route_its_report_and_its_files(self):
        self.assertEqual(SETTLEMENT_LINE.route, ROUTE)  # the route of the Amazon replay
        self.assertEqual((SETTLEMENT_LINE.report, SETTLEMENT_LINE.source_file, SETTLEMENT_LINE.downloaded_file,
                          SETTLEMENT_LINE.downstream_file),
                         ("amazon-report.json", "settlement-line-source.json", "settlement-line-v3.ndjson",
                          "settlement-line-downstream.json"))
        self.assertEqual(set(KINDS), {"ProductCost", "OrderLinePrice", "SettlementLine"})

    def test_what_this_round_s_v3_is_expected_to_hold_is_expected_lines_by_line(self):
        self.assertEqual(SETTLEMENT_LINE.expected,
                         {number: [*(line[name] for name in NAMES[:-1]), Decimal(line["total"])]
                          for number, line in enumerate(EXPECTED_LINES, 1)})
        self.assertEqual([values[-1] for values in SETTLEMENT_LINE.expected.values()],
                         [Decimal("59.57"), Decimal("7.44"), Decimal("-7.44")])


class LocateTests(unittest.TestCase):
    def test_the_receipt_leads_to_one_transaction_its_version_and_its_file_and_the_bytes_are_hashed(self):
        first = published(V1, version=1)
        second = published(V2, version=2, dataset=first[1]["transaction"]["dataset_id"])
        third = published(V3, version=3, dataset=first[1]["transaction"]["dataset_id"])
        reader, _ = reader_of(first[1], second[1], third[1])
        binding, raw = locate(reader, third[0], SETTLEMENT_LINE)
        transaction, file = third[1]["transaction"], third[1]["file"]
        self.assertEqual(raw, V3)
        self.assertEqual(source_of(binding), {"version": 3, "dataset": transaction["dataset_id"],
                                              "transaction": transaction["id"], "file": file["id"],
                                              "sha256": hashlib.sha256(V3).hexdigest()})
        self.assertEqual(binding["lines"], {1: 1, 2: 2, 3: 3})
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
        _, _, source, raw = archived_transaction(None, "SettlementLine")
        self.assertEqual(raw, V3)
        self.assertEqual((V3, hashlib.sha256(V3).hexdigest()), (raw, source["sha256"]))
        # What the file holds, read as exact decimals, is what EXPECTED_LINES states of it.
        self.assertEqual(lines_and_values(rows_of_file(V3), SETTLEMENT_LINE), ({1: 1, 2: 2, 3: 3}, SETTLEMENT_LINE.expected))
        self.assertNotEqual(hashlib.sha256(V2).hexdigest(), source["sha256"])

    def test_a_transaction_that_does_not_repeat_the_handle_names_no_source(self):
        receipt, output = published(V3)
        output["transaction"]["metadata"]["transaction_rid"] = f"pipeline-run:{uuid.uuid4()}:output"
        reader, _ = reader_of(output)
        with self.assertRaisesRegex(entry.ReceiptError, "0 transactions of dataset .* repeat 'pipeline-run:"):
            locate(reader, receipt, SETTLEMENT_LINE)

    def test_a_file_whose_settlement_line_lacks_its_total_does_not_parse(self):
        receipt, output = published(b'{"order id":"000-0000000-0000003","settlement id":1000000010,"type":"Order"}\n')
        reader, _ = reader_of(output)
        with self.assertRaisesRegex(entry.ReceiptError, "does not parse: KeyError"):
            locate(reader, receipt, SETTLEMENT_LINE)


class AcceptanceTests(unittest.TestCase):
    """``differences`` of the amazon kind is the one acceptance. It is fed the readback of a tenant."""

    def setUp(self):
        self.tenant = "tenant-round"
        self.receipt, self.output = published(V3)
        reader, _ = reader_of(self.output)
        self.binding, _ = locate(reader, self.receipt, SETTLEMENT_LINE)
        self.source = source_of(self.binding)

    def problems(self, store, binding=None):
        return differences(binding or self.binding, self.tenant, store, SETTLEMENT_LINE)

    def test_the_three_settlement_lines_of_this_round_are_accepted(self):
        self.assertEqual(self.problems(settlement_lines(self.tenant, V3, self.source)), [])

    def test_the_id_of_each_settlement_line_is_derived_from_the_file_of_this_round_and_its_own_line(self):
        three = settlement_lines(self.tenant, V3, self.source)
        held = lines_of(three)
        self.assertEqual(sorted(held), [1, 2, 3])
        self.assertEqual({number: held[number]["id"] for number in held}, expected_ids(self.tenant, self.source))
        self.assertEqual(len({obj["id"] for obj in three["objects"]}), 3)
        # Lines 2 and 3 are an order and the refund of one order id and one SKU: two objects.
        second, third = held[2]["properties"], held[3]["properties"]
        self.assertEqual([(row["order id"], row["sku"]) for row in (second, third)],
                         [(EXPECTED_LINES[1]["order id"], EXPECTED_LINES[1]["sku"])] * 2)
        self.assertEqual((second["type"], third["type"]), ("Order", "Refund"))
        self.assertNotEqual(held[2]["id"], held[3]["id"])
        # Not the id of a row keyed by the property that names it in the report.
        self.assertTrue({obj["id"] for obj in three["objects"]}.isdisjoint(
            stable_object_id(self.tenant, "SettlementLine", self.source["transaction"], {"settlement id": line["settlement id"]})
            for line in EXPECTED_LINES))

    def test_two_objects_that_swapped_their_complete_properties_are_refused(self):
        # The id set is still the right one and each row agrees with the line it claims, so only the id
        # each object holds for that line can tell that the two rows changed places. The order and the
        # refund of one order and SKU are the pair that matters: they swap like any other two.
        three = settlement_lines(self.tenant, V3, self.source)
        for left, right in ((1, 2), (2, 3), (1, 3)):
            with self.subTest(lines=(left, right)):
                problems = self.problems(swapped(three, left - 1, right - 1))
                self.assertEqual(len(problems), 2, problems)
                self.assertTrue(all(problem.split(": ")[1].startswith("is the id of another row") for problem in problems),
                               problems)
                self.assertEqual([problem.rsplit("line ", 1)[1] for problem in problems], [str(right), str(left)])

    def test_the_same_rows_under_swapped_lines_are_refused_by_their_values(self):
        # Delivered again with the lines of rows 1 and 2 exchanged: each id is the one of the line its row
        # claims and its _source is this round's, so what refuses the tenant is the values of each line.
        other = "tenant-swapped"
        rows = rows_of_file(V3)
        store = {"total": 3, "objects": [
            {"id": expected_ids(other, self.source)[number], "object_type_id": "SettlementLine",
             "properties": {**rows[row - 1], "_source": {**self.source, "line": number}}}
            for row, number in ((1, 2), (2, 1), (3, 3))]}
        problems = entry.row_differences(self.source, self.binding["lines"], SETTLEMENT_LINE.expected, other, store,
                                         SETTLEMENT_LINE)
        self.assertEqual((len(problems), named(problems, " _source."),
                          [problem for problem in problems if "is the id of another row" in problem]),
                         (2, set(), []), problems)
        self.assertEqual(problems[0], f"object {expected_ids(other, self.source)[2]}: {SETTLEMENT_LINE.value_name} "
                        "[1000000010, 'Order', '000-0000000-0000003', 'DEMO-SKU-C3', Decimal('59.57')], expected "
                        "[1000000011, 'Order', '000-0000000-0000004', 'DEMO-SKU-D4', Decimal('7.44')]")

    def test_the_archived_transaction_is_refused_though_it_shares_values_and_hash(self):
        _, _, source, raw = archived_transaction(None, "SettlementLine")
        self.assertEqual((source["sha256"], raw), (self.source["sha256"], V3))
        problems = self.problems(settlement_lines(self.tenant, raw, source))
        self.assertEqual(named(problems, " _source."), {"dataset", "transaction", "file"})
        # Beside the id set, the id of each of the three lines is that of another transaction.
        others = [problem for problem in problems if " _source." not in problem]
        self.assertEqual(len(others), 4, problems)
        self.assertIn("is not", others[0])
        self.assertEqual([problem.rsplit("line ", 1)[1] for problem in others[1:]], ["1", "2", "3"])
        self.assertFalse(any("settlement id, type" in problem or "is not one of" in problem for problem in problems),
                         problems)
        self.assertEqual(len([problem for problem in problems if " _source." in problem]), 9)

    def test_the_v1_file_of_this_round_is_refused_though_it_shares_dataset_bytes_and_hash(self):
        receipt, output = published(V1, version=1, dataset=self.source["dataset"])
        one, = located((receipt, output))
        self.assertEqual([name for name in ("dataset", "sha256") if one[name] != self.source[name]], [])
        problems = self.problems(settlement_lines(self.tenant, V1, source_of(one)))
        self.assertEqual(named(problems, " _source."), {"version", "transaction", "file"})
        self.assertFalse(any("settlement id, type" in problem for problem in problems), problems)

    def test_the_v2_file_of_this_round_is_refused_by_identity_and_values(self):
        receipt, output = published(V2, version=2, dataset=self.source["dataset"])
        two, = located((receipt, output))
        problems = self.problems(settlement_lines(self.tenant, V2, source_of(two)))
        self.assertEqual(named(problems, " _source."), {"version", "transaction", "file", "sha256"})
        self.assertTrue(problems[1].endswith(": is the id of another row, expected the id of line 1"), problems[1])
        self.assertIn("[1000000013, 'FBA Inventory Fee', None, None, Decimal('-9.85')], expected [1000000010, 'Order', "
                      "'000-0000000-0000003', 'DEMO-SKU-C3', Decimal('59.57')]", problems[2])
        self.assertIn("2 listed and 2 read back, expected the 3 objects", problems[0])

    def test_another_count_or_another_type_is_refused(self):
        three = settlement_lines(self.tenant, V3, self.source)
        self.assertTrue(any("4 listed and 4 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "total": 4, "objects": three["objects"] + [three["objects"][0]]})))
        self.assertTrue(any("3 listed and 2 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "objects": three["objects"][:2]})))
        other = {**three, "objects": [{**three["objects"][0], "object_type_id": "ProductCost"}, *three["objects"][1:]]}
        self.assertTrue(any("is a ProductCost" in problem for problem in self.problems(other)), self.problems(other))

    def test_another_total_type_order_or_source_attribute_is_refused(self):
        three = settlement_lines(self.tenant, V3, self.source)
        first = three["objects"][0]
        where, name = f"object {first['id']}", SETTLEMENT_LINE.value_name
        wanted = "[1000000010, 'Order', '000-0000000-0000003', 'DEMO-SKU-C3', Decimal('59.57')]"
        for change, held in (({"total": "59.58"}, "[1000000010, 'Order', '000-0000000-0000003', 'DEMO-SKU-C3', Decimal('59.58')]"),
                             ({"type": "Refund"}, "[1000000010, 'Refund', '000-0000000-0000003', 'DEMO-SKU-C3', Decimal('59.57')]"),
                             ({"order id": "another"}, "[1000000010, 'Order', 'another', 'DEMO-SKU-C3', Decimal('59.57')]"),
                             ({"sku": None}, "[1000000010, 'Order', '000-0000000-0000003', None, Decimal('59.57')]"),
                             ({"settlement id": 1000000011}, "[1000000011, 'Order', '000-0000000-0000003', 'DEMO-SKU-C3', Decimal('59.57')]")):
            with self.subTest(change=change):
                problems = self.problems(replaced(three, 1, **change))
                self.assertEqual(problems, [f"{where}: {name} {held}, expected {wanted}"], problems)
        # The file of the key is the one of this round, so an object that claims another file keeps its id.
        for change, attribute in (({"sha256": "another"}, "sha256"), ({"file": "another"}, "file"), ({"line": 4}, None)):
            with self.subTest(change=change):
                problems = self.problems(replaced(three, 1, _source={**self.source, "line": 1, **change}))
                if attribute:
                    self.assertEqual(problems, [f"{where} _source.{attribute}: 'another', its file reads {self.source[attribute]!r}"])
                else:
                    self.assertEqual([problem.split(": ")[1].split(" ")[0] for problem in problems], ["line"], problems)

    def test_a_line_that_is_a_boolean_a_float_or_text_is_no_line_and_another_line_moves_the_row(self):
        three = settlement_lines(self.tenant, V3, self.source)
        for line in (True, 1.0, "1"):
            with self.subTest(line=line):
                problems = self.problems(replaced(three, 1, _source={**self.source, "line": line}))
                self.assertEqual([problem.split(": ")[1] for problem in problems], [f"line {line!r} is not one of [1, 2, 3]"])
        # The first row claiming line 2: its id is that of line 1 and its values those of line 1, not of line 2.
        problems = self.problems(replaced(three, 1, _source={**self.source, "line": 2}))
        self.assertEqual((len(problems), problems[0].split(": ")[1]), (2, "is the id of another row, expected the id of line 2"))
        self.assertTrue(problems[1].endswith("Decimal('59.57')], expected [1000000011, 'Order', '000-0000000-0000004', "
                                             "'DEMO-SKU-D4', Decimal('7.44')]"), problems[1])

    def test_a_source_that_is_no_object_is_refused_for_that(self):
        three = settlement_lines(self.tenant, V3, self.source)
        first = three["objects"][0]
        for source in NO_SOURCE:
            with self.subTest(source=source):
                # One problem names the _source; nothing else of the object is read after it.
                self.assertEqual(self.problems(replaced(three, 1, _source=source)),
                                 [f"object {first['id']} _source: {source!r}, expected an object naming its line"])
        # A type named by its key property refuses the same _source by its six attributes, as before.
        costs = stored(self.tenant, LINGXING_V3, self.source, PRODUCT_COST)
        for source in NO_SOURCE:
            with self.subTest(source=source, type="ProductCost"):
                cost = {**costs, "objects": [{**costs["objects"][0], "properties": {**costs["objects"][0]["properties"], "_source": source}},
                                            *costs["objects"][1:]]}
                problems = entry.row_differences(self.source, {"0007": 1, "W-2": 2, "D3": 3}, PRODUCT_COST.expected, self.tenant,
                                                 cost, PRODUCT_COST)
                self.assertEqual(named(problems, " _source."), set(SOURCE_ATTRIBUTES))
                self.assertEqual(entry._held(cost, PRODUCT_COST)["count"], 3)

    def test_an_amount_read_as_a_float_is_refused(self):
        # The same rows read with every fraction a float: no float is the exact decimal of the file.
        three = settlement_lines(self.tenant, V3, self.source)
        floats = {**three, "objects": [{**obj, "properties": {**json.loads(line), "_source": obj["properties"]["_source"]}}
                                       for obj, line in zip(three["objects"], V3.splitlines())]}
        problems = self.problems(floats)
        self.assertEqual(len(problems), 3, problems)
        for problem, obj in zip(problems, floats["objects"]):
            held, expected = problem.split(", expected ")
            self.assertTrue(held.startswith(f"object {obj['id']}: {SETTLEMENT_LINE.value_name} ["), problem)
            self.assertNotIn("Decimal(", held)
            self.assertIn("Decimal(", expected)

    def test_a_readback_kept_as_json_evidence_is_judged_the_same(self):
        # --verify-only judges the binding and the readback as it reads them back from JSON: the amounts as
        # their digits and the lines named by text.
        three = settlement_lines(self.tenant, V3, self.source)
        kept = json.loads(json.dumps(entry.plain(three)))
        self.assertEqual([values_of(obj)[-1] for obj in kept["objects"]], ["59.57", "7.44", "-7.44"])
        binding = json.loads(json.dumps(self.binding))
        self.assertEqual((binding["lines"], self.problems(kept, binding)), ({"1": 1, "2": 2, "3": 3}, []))
        moved = self.problems(replaced(kept, 2, _source={**self.source, "line": 3}), binding)
        self.assertEqual((named(moved, " _source."), moved[0].split(": ")[1]), (set(), "is the id of another row, expected the id of line 3"))

    def test_what_a_readback_holds_is_summarized_by_line(self):
        three = settlement_lines(self.tenant, V3, self.source)
        held = entry._held(three, SETTLEMENT_LINE)
        self.assertEqual((held["count"], held["source"], held["values"]), (3, self.source, SETTLEMENT_LINE.expected))
        self.assertEqual(entry._held({"total": 0, "objects": []}, SETTLEMENT_LINE), {"count": 0, "source": None, "values": {}})
        # An object whose _source is no object holds no attribute of a source and claims no line.
        for source in NO_SOURCE:
            with self.subTest(source=source):
                held = entry._held(replaced(three, 2, _source=source), SETTLEMENT_LINE)
                self.assertEqual((held["count"], held["source"], sorted(held["values"], key=str)), (3, None, [1, 3, None]))
                self.assertEqual(held["values"][1], SETTLEMENT_LINE.expected[1])
                self.assertEqual(held["values"][None], SETTLEMENT_LINE.expected[2])

    def test_the_lines_are_checked_as_a_whole_by_the_checker_of_92(self):
        sentence, judge = SETTLEMENT_LINE.whole
        three = settlement_lines(self.tenant, V3, self.source)
        self.assertEqual(judge(json.loads(json.dumps(entry.plain(three)))), EXPECTED_TOTAL)
        self.assertEqual(sum((Decimal(line["total"]) for line in EXPECTED_LINES), Decimal(0)), Decimal(EXPECTED_TOTAL))
        self.assertIn(f"adding up to {EXPECTED_TOTAL}", sentence)
        # Another total, and one id for the order and the refund of one order and SKU.
        with self.assertRaises(AssertionError):
            judge(json.loads(json.dumps(entry.plain(replaced(three, 1, total="59.58")))))
        second, third = lines_of(three)[2], lines_of(three)[3]
        one_id = {**three, "objects": [{**third, "id": second["id"]} if obj is third else obj for obj in three["objects"]]}
        with self.assertRaises(AssertionError):
            judge(json.loads(json.dumps(entry.plain(one_id))))


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
        """The report of an amazon round. Its ``organization`` is the publisher of the round, the
        tenant the entry delivers under."""
        tenant = str(uuid.uuid4())
        (self.state / "amazon-report.json").write_text(json.dumps(
            {"revision": "r1", "organization": tenant,
             "pipeline": {"pipeline_id": receipt["pipeline_id"], "builds": [{"ordinal": 3, "output_version": 3}]}}))
        (self.state / "pipeline-3-run.json").write_text(json.dumps(receipt))
        return tenant

    def command(self, *arguments):
        """Run the amazon entry once. Return its exit code, the report it printed and what it printed on stderr."""
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["--state", str(self.state), "--dataset-base", BASE, "--endpoint", self.endpoint,
                         "--type", "SettlementLine", *arguments])
        return code, json.loads(out.getvalue()) if out.getvalue() else None, err.getvalue()

    def source(self):
        return json.loads((self.state / SETTLEMENT_LINE.source_file).read_text())

    def object_id(self, number):
        source = self.source()
        return stable_object_id(self.tenant, "SettlementLine", source["transaction"],
                                {"_source.file": source["file"], "_source.line": number})

    def test_the_entry_locates_this_round_s_v3_delivers_it_once_and_reads_three_settlement_lines_back(self):
        code, report, err = self.command()
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(report["tenant"], self.tenant)
        self.assertEqual(report["source"], source_of(self.source()))
        self.assertEqual((report["source"]["sha256"], report["lines"]),
                         (hashlib.sha256(V3).hexdigest(), {"1": 1, "2": 2, "3": 3}))
        self.assertEqual((self.state / SETTLEMENT_LINE.downloaded_file).read_bytes(), V3)
        self.assertEqual(report["delivered"]["created"], 3)
        self.assertEqual([(item["line"], item["settlement id"], item["created"]) for item in report["delivered"]["results"]],
                         [(1, 1000000010, True), (2, 1000000011, True), (3, 1000000012, True)])
        ids = {item["line"]: item["object_id"] for item in report["delivered"]["results"]}
        self.assertEqual(len(set(ids.values())), 3)
        self.assertEqual(ids, expected_ids(self.tenant, report["source"]))
        # The list, then each listed object by its id: the store lists its objects by id.
        route = SETTLEMENT_LINE.route
        self.assertEqual([path for method, path in self.store.requests if method == "GET"][-4:],
                         [route + "?per_page=50", *(route + object_id for object_id in sorted(ids.values()))])
        self.assertEqual((report["store"]["total"], len(report["store"]["objects"])), (3, 3))
        held = lines_of(report["store"])
        self.assertEqual(sorted(held), [1, 2, 3])
        for number, row in enumerate(amazon.archived_lines(V3, report["source"]), 1):
            obj = held[number]
            self.assertEqual((obj["id"], obj["object_type_id"]), (ids[number], "SettlementLine"))
            amounts = {name: value for name, value in row.items() if isinstance(value, Decimal)}
            self.assertEqual({name: Decimal(obj["properties"][name]) for name in amounts}, amounts)
            self.assertEqual({name: obj["properties"][name] for name in row if name not in amounts},
                             {name: row[name] for name in row if name not in amounts})
            self.assertEqual(set(obj["properties"]["_source"]), set(SOURCE_ATTRIBUTES))
        # The report keeps every amount as the digits of its exact decimal, or as the integer the file holds: never a float.
        self.assertEqual([values_of(held[number]) for number in (1, 2, 3)],
                         [[line[name] for name in NAMES] for line in EXPECTED_LINES])
        columns = ("product sales", "selling fees", "gift wrap credits", "total")
        amounts = [obj["properties"][name] for obj in report["store"]["objects"] for name in columns]
        self.assertTrue(all(isinstance(value, (int, str)) and not isinstance(value, bool) for value in amounts), amounts)
        self.assertEqual(sorted(Decimal(value) for value in amounts),
                         sorted(row[name] for row in rows_of_file(V3) for name in columns))
        self.assertEqual((report["problems"], report["accepted"]), ([], True))
        self.assertEqual(len(self.store.objects), 3)

    def test_the_same_command_run_again_exits_0_creates_nothing_and_reads_the_same_settlement_lines_back(self):
        _, first, _ = self.command()
        code, again, err = self.command()
        self.assertEqual((code, err), (0, ""))
        self.assertEqual((again["delivered"]["created"], [item["created"] for item in again["delivered"]["results"]]),
                         (0, [False] * 3))
        self.assertEqual((again["store"], len(self.store.objects)), (first["store"], 3))

    def test_a_settlement_line_of_the_same_line_with_another_total_stops_the_delivery_with_exit_1(self):
        self.assertEqual(self.command()[0], 0)
        object_id = self.object_id(1)
        stored = self.store.objects[(self.tenant, object_id)]
        self.store.objects[(self.tenant, object_id)] = {**stored, "payload": {**stored["payload"], "total": 59.58}}
        code, report, err = self.command()
        self.assertEqual((code, report), (1, None))
        self.assertIn("SyncConflictError", err)
        self.assertEqual(self.store.objects[(self.tenant, object_id)]["payload"]["total"], 59.58)

    def test_a_tenant_that_received_the_archived_transaction_is_refused_on_readback_with_exit_1(self):
        _, first, _ = self.command()
        _, tenant, source, raw = archived_transaction(None, "SettlementLine")
        sync_settlement_lines(self.endpoint, tenant, rows_of(raw, source))
        code, report, err = self.command("--tenant", tenant, "--verify-only")
        self.assertEqual((code, err), (1, ""))
        self.assertEqual(report["delivered"], None)
        self.assertEqual(source["sha256"], first["source"]["sha256"])
        held = lines_of(report["store"])
        self.assertEqual([values_of(held[number]) for number in (1, 2, 3)], [[line[name] for name in NAMES] for line in EXPECTED_LINES])
        self.assertEqual(named(report["problems"], " _source."), {"dataset", "transaction", "file"})
        self.assertIn("is not", [problem for problem in report["problems"] if " _source." not in problem][0])
        self.assertFalse(report["accepted"])
        # This round's tenant is read back again and still holds the same three settlement lines.
        code, again, _ = self.command("--verify-only")
        self.assertEqual((code, again["store"]), (0, first["store"]))

    def test_a_tenant_that_received_this_round_s_v1_file_is_refused_on_readback_with_exit_1(self):
        self.command()
        receipt, output = published(V1, version=1, dataset=self.source()["dataset"])
        self.dataset.publish(output)
        reader = DatasetReader(BASE, "reader-bearer", opener=self.dataset)
        one, raw = locate(reader, receipt, SETTLEMENT_LINE)
        sync_settlement_lines(self.endpoint, "tenant-first", rows_of(raw, source_of(one)))
        code, report, _ = self.command("--tenant", "tenant-first", "--verify-only")
        self.assertEqual(code, 1)
        self.assertEqual(named(report["problems"], " _source."), {"version", "transaction", "file"})

    def test_verify_only_refuses_an_object_whose_source_is_no_object_without_failing(self):
        _, first, _ = self.command()
        object_id = self.object_id(2)
        stored = self.store.objects[(self.tenant, object_id)]
        for source in NO_SOURCE:
            with self.subTest(source=source):
                self.store.objects[(self.tenant, object_id)] = {**stored, "payload": {**stored["payload"], "_source": source}}
                code, report, err = self.command("--verify-only")
                self.store.objects[(self.tenant, object_id)] = stored
                # One report with one problem that names the _source, exit code 1 and nothing on stderr.
                self.assertEqual((code, err, report["accepted"]), (1, "", False))
                self.assertEqual(report["problems"], [f"object {object_id} _source: {source!r}, expected an object naming its line"])
                kept = {obj["id"]: obj for obj in report["store"]["objects"]}
                self.assertEqual(kept[object_id]["properties"]["_source"], source)
                self.assertEqual([obj for obj in report["store"]["objects"] if obj["id"] != object_id],
                                 [obj for obj in first["store"]["objects"] if obj["id"] != object_id])
        # With the object restored the tenant is accepted again.
        code, again, _ = self.command("--verify-only")
        self.assertEqual((code, again["store"]), (0, first["store"]))

    def test_without_type_the_entry_is_the_lingxing_one_and_reads_no_amazon_report(self):
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
