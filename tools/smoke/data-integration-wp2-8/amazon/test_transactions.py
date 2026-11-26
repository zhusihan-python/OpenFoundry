import csv
from decimal import Decimal
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import unittest
from unittest import mock

from transactions import (AMOUNTS, COLUMNS, COUNTS, IDENTITY, NEGATIVE_AMOUNTS, NET_TOTAL, OUTPUT_SHA256, agrees, expected,
                          holds, lines, negative_amounts, net, nodes, read, refused_at_input, table, text_total,
                          value_diffs, with_total)

HERE = Path(__file__).resolve().parent
DESCRIPTIVE = {"marketplace", "account type", "fulfillment", "order city", "order state", "order postal",
               "tax collection model"}
# Five columns of the first October file, as a download may hold them.
KEYS = ("settlement id", "order id", "quantity", "selling fees", "total")
FIRST = dict(zip(KEYS, (1000000010, "000-0000000-0000003", 1, -9.00, 59.57)))
REST = [dict(zip(KEYS, (1000000011, "000-0000000-0000004", 1, -2.25, 7.44))),
        dict(zip(KEYS, (1000000012, "000-0000000-0000004", -1, 2.25, -7.44)))]


def fixture(name):
    return (HERE / "fixtures" / f"transaction_view-{name}.csv").read_bytes()


def file(*rows):
    """A download: one JSON object per line."""
    return "".join(json.dumps(row) + "\n" for row in rows).encode()


def changed(**changes):
    """The download with its first line changed."""
    return file({**FIRST, **changes}, *REST)


def records(data):
    """The records of a report as a CSV reader gives them."""
    return [record for record in csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""))]


class Expectation(unittest.TestCase):
    def test_every_report_is_read_from_its_header_and_keeps_every_column(self):
        for name, count in (("2026-09", 4), ("2026-10", 3), ("2026-10-2", 2)):
            with self.subTest(file=name):
                rows = expected(fixture(name))
                self.assertEqual(len(rows), count)
                self.assertEqual(list(rows[0])[:2] + list(rows[0])[-2:], ["date/time", "settlement id", "other", "total"])
                self.assertEqual((set(rows[0]) - set(COLUMNS), set(COLUMNS) - set(rows[0])), (DESCRIPTIVE, set()))

    def test_values_follow_their_business_field(self):
        first, café, refund = expected(fixture("2026-10"))
        self.assertEqual({name: first[name] for name in IDENTITY},
                         {"date/time": "Oct 2, 2026 08:05:00 AM PDT", "settlement id": 1000000010, "type": "Order",
                          "order id": "000-0000000-0000003", "sku": "DEMO-SKU-C3", "description": "Sample item C",
                          "quantity": 1})
        self.assertEqual((first["product sales"], first["selling fees"], first["total"]),
                         (Decimal("59.99"), Decimal("-9.00"), Decimal("59.57")))
        self.assertEqual((café["description"], café["product sales"], refund["description"], refund["quantity"]),
                         ("Café sampler à la carte", Decimal("14.99"), "Café sampler à la carte", -1))
        fee, order = expected(fixture("2026-10-2"))
        self.assertEqual((fee["type"], fee["order id"], fee["sku"], fee["quantity"], fee["fba fees"], fee["total"]),
                         ("FBA Inventory Fee", None, None, None, Decimal("-9.85"), Decimal("-9.85")))
        self.assertEqual((order["order id"], order["selling fees"], order["total"]),
                         ("000-0000000-0000005", Decimal("-1.50"), Decimal("8.49")))

    def test_each_value_is_of_its_kind_and_never_a_bool(self):
        for name in ("2026-09", "2026-10", "2026-10-2"):
            for row in expected(fixture(name)):
                for column, value in row.items():
                    with self.subTest(file=name, column=column):
                        if value is None:
                            continue
                        kind = Decimal if column in AMOUNTS else int if column in ("settlement id", "quantity") else str
                        self.assertIs(type(value), kind)

    def test_projection_keeps_the_identity_and_every_amount_in_header_order(self):
        for name in ("2026-10", "2026-10-2"):
            every = expected(fixture(name))
            self.assertEqual(expected(fixture(name), COLUMNS), [{c: row[c] for c in COLUMNS} for row in every])
            self.assertEqual(list(expected(fixture(name), COLUMNS)[0]), COLUMNS)
        self.assertEqual((COLUMNS[:7], COLUMNS[7:]), (IDENTITY, AMOUNTS))

    def test_negative_amounts_are_six_and_three_and_the_refund_reversal_stays_positive(self):
        first = negative_amounts(expected(fixture("2026-10"), COLUMNS))
        second = negative_amounts(expected(fixture("2026-10-2"), COLUMNS))
        self.assertEqual(first, [{"selling fees": Decimal("-9.00")},
                                 {"marketplace withheld tax": Decimal("-1.20"), "selling fees": Decimal("-2.25"),
                                  "fba fees": Decimal("-4.10")},
                                 {"product sales": Decimal("-14.99"), "total": Decimal("-7.44")}])
        self.assertEqual(second, [{"fba fees": Decimal("-9.85"), "total": Decimal("-9.85")}, {"selling fees": Decimal("-1.50")}])
        for name, negatives in (("2026-10", first), ("2026-10-2", second)):
            self.assertEqual(sum(map(len, negatives)), NEGATIVE_AMOUNTS[name])
        refund = expected(fixture("2026-10"))[2]
        self.assertEqual({name: refund[name] for name in ("marketplace withheld tax", "selling fees", "fba fees")},
                         {"marketplace withheld tax": Decimal("1.20"), "selling fees": Decimal("2.25"),
                          "fba fees": Decimal("4.10")})

    def test_the_two_files_of_one_month_share_no_transaction(self):
        first, second = (expected(fixture(name), ("settlement id", "order id", "sku")) for name in NEGATIVE_AMOUNTS)
        self.assertEqual([row["settlement id"] for row in first], [1000000010, 1000000011, 1000000012])
        self.assertEqual([row["settlement id"] for row in second], [1000000013, 1000000014])
        for column in KEYS[:1] + ("order id", "sku"):
            self.assertTrue({row[column] for row in first}.isdisjoint(row[column] for row in second), column)

    def test_downloaded_rows_are_the_expected_transactions_and_none_of_the_other_file(self):
        first, second = (expected(fixture(name), COLUMNS) for name in ("2026-10", "2026-10-2"))
        self.assertTrue(holds(first, first, second) and holds(second, second, first))
        # The rows of the same file are not strangers, a shorter or reordered download is not the file, nor is nothing.
        for rows, wanted, strangers in ((first, first, first), (second, second, second), (first[:2], first, second),
                                        (first[::-1], first, second), (second + first[:1], second, first), ([], first, second)):
            with self.subTest(rows=len(rows), wanted=len(wanted)):
                self.assertFalse(holds(rows, wanted, strangers))

    def test_a_report_without_a_usable_header_is_refused(self):
        for data, reason in ((b"a,b\n1,2\n", "no header record"), (b"date/time,total\n1\n", "does not match its header")):
            with self.subTest(data=data), self.assertRaisesRegex(ValueError, reason):
                expected(data)
        with self.assertRaisesRegex(ValueError, "no column 'no such column'"):
            expected(fixture("2026-10"), ("no such column",))

    def test_sample_output_hashes_are_pinned_for_the_two_october_files(self):
        self.assertEqual(sorted(OUTPUT_SHA256), sorted(NEGATIVE_AMOUNTS))
        self.assertEqual(len(set(OUTPUT_SHA256.values())), 2)
        self.assertTrue(all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in OUTPUT_SHA256.values()))


class Agreement(unittest.TestCase):
    def test_a_number_is_a_number_and_not_a_bool_or_a_text(self):
        for actual, expected_value, agrees_ in ((1, 1, True), (Decimal("1.0"), 1, True), (Decimal("-9"), Decimal("-9.00"), True),
                                                (True, 1, False), (False, 0, False), ("1", 1, False), (1, "1", False),
                                                (59.99, Decimal("59.99"), False), ("x", None, False), (False, None, False),
                                                (0, None, False), (None, None, True), ("a", "a", True), ("a", "b", False)):
            with self.subTest(actual=actual, expected=expected_value):
                self.assertEqual(agrees(actual, expected_value), agrees_)


class Reconciliation(unittest.TestCase):
    rows = staticmethod(lambda: expected(fixture("2026-10"), KEYS))

    def test_a_matching_download_has_no_diffs(self):
        self.assertEqual(value_diffs(file(FIRST, *REST), self.rows()), [])

    def test_changed_sign_value_or_text_is_named(self):
        rows = self.rows()
        for changes, diff in (({"selling fees": 9.00}, "line 1 selling fees: Decimal('9.0'), expected Decimal('-9.00')"),
                              ({"order id": 3}, "line 1 order id: 3, expected '000-0000000-0000003'"),
                              ({"total": "59.57"}, "line 1 total: '59.57', expected Decimal('59.57')"),
                              ({"settlement id": 1000000013}, "line 1 settlement id: 1000000013, expected 1000000010")):
            with self.subTest(changes=changes):
                self.assertEqual(value_diffs(changed(**changes), rows), [diff])

    def test_a_bool_is_not_a_number(self):
        rows = self.rows()
        self.assertEqual(value_diffs(changed(quantity=True), rows), ["line 1 quantity: True, expected 1"])
        self.assertEqual(value_diffs(changed(**{"selling fees": False}), rows),
                         ["line 1 selling fees: False, expected Decimal('-9.00')"])

    def test_line_count_null_and_a_column_on_one_side_only(self):
        rows = self.rows()
        # Lines are compared with the expected rows of their own number. A missing last line leaves no line to name.
        self.assertEqual(value_diffs(file(FIRST, REST[0]), rows), ["2 lines, expected 3"])
        self.assertEqual(value_diffs(file(FIRST, {**REST[0], "total": 0}), rows),
                         ["2 lines, expected 3", "line 2 total: 0, expected Decimal('7.44')"])
        self.assertEqual(value_diffs(file({**FIRST, "selling fees": None}, *REST), rows),
                         ["line 1 selling fees: None, expected Decimal('-9.00')"])
        self.assertEqual(value_diffs(file({key: value for key, value in FIRST.items() if key != "quantity"}, *REST), rows),
                         ["line 1 quantity: present on one side only"])
        self.assertEqual(value_diffs(file({**FIRST, "sku": "DEMO-SKU-C3"}, *REST), rows),
                         ["line 1 sku: present on one side only"])

    def test_a_line_that_is_no_object_is_named(self):
        self.assertEqual(value_diffs(b"not json\n[1]\n" + file(REST[1]), self.rows()),
                         ["line 1: not JSON", "line 2: not an object"])

    def test_one_value_in_another_number_form_still_agrees(self):
        self.assertEqual(value_diffs(changed(quantity=1.0, total=5.957e1), self.rows()), [])
        self.assertEqual(value_diffs(changed(quantity=1.5), self.rows()), ["line 1 quantity: Decimal('1.5'), expected 1"])


class ParsedRows(unittest.TestCase):
    def test_preview_rows_are_read_only_under_unique_columns_of_one_width(self):
        self.assertEqual(table(("a", "b"), [[1, None], [Decimal("-9.00"), "x"]]),
                         [{"a": 1, "b": None}, {"a": Decimal("-9.00"), "b": "x"}])
        self.assertEqual(table([], []), [])
        for columns, rows in ((("a", "a"), [[1, 2]]), (("a", "b"), [[1]]), (("a", "b"), [[1, 2, 3]])):
            with self.subTest(columns=columns, rows=rows):
                self.assertIsNone(table(columns, rows))

    def test_rows_a_json_reader_parsed_are_read_as_the_decimals_they_came_as(self):
        self.assertEqual(read(lines([{"selling fees": -9.0, "total": 59.99, "description": "Café", "order id": None},
                                     {"settlement id": 1000000010}])),
                         [{"selling fees": Decimal("-9.0"), "total": Decimal("59.99"), "description": "Café", "order id": None},
                          {"settlement id": 1000000010}])


class NegativeReport(unittest.TestCase):
    def test_with_total_changes_the_total_of_the_last_data_row_and_no_other_cell(self):
        original, derived = fixture("2026-10-2"), text_total(fixture("2026-10-2"))
        before, after = records(original), records(derived)
        self.assertEqual((len(before), len(after), before[1][-1]), (4, 4, "total"))
        self.assertEqual([len(record) for record in after], [len(record) for record in before])
        differences = [(at, cell) for at, (x, y) in enumerate(zip(before, after))
                       for cell, (a, b) in enumerate(zip(x, y)) if a != b]
        # One cell: the last column of the last record.
        self.assertEqual(differences, [(3, 29)])
        self.assertEqual((before[3][29], after[3][29]), ("8.49", "N/A"))

    def test_with_total_keeps_the_bom_the_line_endings_and_the_other_rows_byte_for_byte(self):
        original, derived = fixture("2026-10-2"), text_total(fixture("2026-10-2"))
        # All that differs is the one cell. The BOM is in the shared start and the last line ending in the shared end.
        start = os.path.commonprefix([original, derived])
        end = os.path.commonprefix([original[::-1], derived[::-1]])
        self.assertEqual((original[len(start):len(original) - len(end)], derived[len(start):len(derived) - len(end)]),
                         (b"8.49", b"N/A"))
        self.assertEqual((start[:3], start[-1:], end), (b"\xef\xbb\xbf", b",", b"\n"))
        self.assertEqual(original.split(b"\n")[:-2], derived.split(b"\n")[:-2])

    def test_with_total_keeps_the_line_endings_of_a_report_with_crlf_or_without_a_last_newline(self):
        lf = fixture("2026-10-2")
        crlf, bare = lf.replace(b"\n", b"\r\n"), lf.rstrip(b"\n")
        # Putting the cell back gives the same bytes, whatever the line endings.
        self.assertEqual([with_total(data, "8.49") for data in (lf, crlf, bare)], [lf, crlf, bare])
        derived = [with_total(data, "N/A") for data in (lf, crlf, bare)]
        self.assertEqual([data.endswith(ending) for data, ending in zip(derived, (b",N/A\n", b",N/A\r\n", b",N/A"))],
                         [True] * 3)
        # Every line ending of the CRLF report is still CRLF, and the bare report still has no last newline.
        self.assertEqual([data.count(b"\n") for data in derived], [4, 4, 3])
        self.assertEqual(with_total(crlf, "N/A").replace(b"\r\n", b"").count(b"\n"), 0)

    def test_with_total_refuses_a_last_data_row_that_does_not_end_with_a_number(self):
        for data in (text_total(fixture("2026-10-2")), b"", b"\n", b"date/time,total\n1\n", b"date/time,total\n1,8.49\n1\n",
                     b'date/time,total\n1,"8.49"\n', b"date/time,total\n1,8.49 \n", b"date/time,total\n1,1e3\n",
                     b"date/time,total\n1,007\n"):
            with self.subTest(data=data), self.assertRaisesRegex(ValueError, "does not end with a number"):
                with_total(data, "N/A")


class NetTotal(unittest.TestCase):
    def test_the_net_of_each_october_report_is_pinned_and_the_download_sums_to_it(self):
        for name, total in (("2026-10", Decimal("59.57")), ("2026-10-2", Decimal("-1.36"))):
            with self.subTest(file=name):
                # What a reader downloads: one JSON object per line, an amount a number.
                download = "".join(json.dumps({"total": float(row["total"])}) + "\n" for row in expected(fixture(name))).encode()
                self.assertEqual((NET_TOTAL[name], net(expected(fixture(name))), net(read(download))), (total, total, total))

    def test_the_net_skips_a_row_whose_total_is_empty_or_text(self):
        rows = expected(fixture("2026-10-2"))
        rows[-1]["total"] = "N/A"
        self.assertEqual(net(rows), expected(fixture("2026-10-2"))[0]["total"])
        self.assertEqual(net(read(b'{"total": -9.85}\n{"total": "N/A"}\n')), Decimal("-9.85"))
        self.assertEqual(net([{"total": 1}, {"total": Decimal("0.5")}, {"total": None}, {"total": "1"}, {"total": True}, {}]),
                         Decimal("1.5"))
        self.assertEqual(net([]), Decimal(0))


class Graph:
    """The node builder of the real WP3.4 harness, which reads its environment at import."""

    def __init__(self):
        spec = importlib.util.spec_from_file_location(
            "wp28_amazon_test_harness", HERE.parents[1] / "data-integration-wp3-4/harness34.py")
        harness = importlib.util.module_from_spec(spec)
        with mock.patch.dict(os.environ, WP34_PROJECT="test", WP34_EVIDENCE=".", WP34_TOKEN="test"):
            spec.loader.exec_module(harness)
        self.graph_nodes = harness.graph_nodes


class TransactionPipeline(unittest.TestCase):
    # The answer of the versioned schema route for one report version, as the reader holds it.
    SCHEMA = {"content_hash": "0" * 63 + "1",
              "fields": [{"name": "date/time", "field_type": "STRING", "nullable": True},
                         {"name": "total", "field_type": "DOUBLE", "nullable": True}]}

    def test_the_pipeline_pins_the_published_version_and_projects_the_columns(self):
        built = nodes(Graph(), "reports", "amounts", dict(version=3, transaction="txn-3"))
        self.assertEqual([node["id"] for node in built], ["input", "project-transactions", "output"])
        self.assertEqual(built[0]["config"], {
            "source_kind": "dataset", "dataset_id": "reports",
            "dataset_snapshot": {"branch": "main", "version": 3, "transaction_id": "ri.foundry.main.transaction.txn-3"}})
        self.assertEqual((built[1]["transform_type"], built[1]["config"], built[1]["depends_on"]),
                         ("select", {"columns": COLUMNS}, ["input"]))
        self.assertEqual((built[2]["transform_type"], built[2]["depends_on"], built[2]["output_dataset_id"]),
                         ("output_dataset", ["project-transactions"], "amounts"))
        self.assertEqual((built[0]["transform_type"], built[0]["input_dataset_ids"]), ("dataset_input", ["reports"]))
        self.assertNotIn("amounts", json.dumps(built[0]))

    def test_each_published_version_pins_its_own_transaction(self):
        pins = [nodes(Graph(), "reports", "amounts", dict(version=v, transaction=f"txn-{v}"))[0]["config"]["dataset_snapshot"]
                for v in (2, 3, 2)]
        self.assertEqual(pins, [{"branch": "main", "version": 2, "transaction_id": "ri.foundry.main.transaction.txn-2"},
                                {"branch": "main", "version": 3, "transaction_id": "ri.foundry.main.transaction.txn-3"},
                                pins[0]])

    def test_the_pin_also_carries_the_schema_the_reader_asked_of_the_version_and_its_hash(self):
        built = nodes(Graph(), "reports", "amounts", dict(version=4, transaction="txn-4"), self.SCHEMA)
        self.assertEqual([node["id"] for node in built], ["input", "project-transactions", "output"])
        pin = built[0]["config"]["dataset_snapshot"]
        self.assertEqual({name: value for name, value in pin.items() if not name.startswith("expected_schema")},
                         {"branch": "main", "version": 4, "transaction_id": "ri.foundry.main.transaction.txn-4"})
        self.assertEqual((pin["expected_schema_hash"], pin["expected_schema"]),
                         (self.SCHEMA["content_hash"], self.SCHEMA["fields"]))
        # The pin holds a copy of the answer. A change to one does not reach the other.
        self.assertIsNot(pin["expected_schema"], self.SCHEMA["fields"])
        pin["expected_schema"][1]["field_type"] = "STRING"
        self.assertEqual(self.SCHEMA["fields"][1]["field_type"], "DOUBLE")

    def test_a_pin_without_an_expected_schema_names_none(self):
        for built in (nodes(Graph(), "reports", "amounts", dict(version=2, transaction="txn-2")),
                      nodes(Graph(), "reports", "amounts", dict(version=2, transaction="txn-2"), None)):
            self.assertEqual(sorted(built[0]["config"]["dataset_snapshot"]), ["branch", "transaction_id", "version"])


CROSSED = "batched_dataset_409_dataset_schema_transaction_mismatch"
REFUSED = ('dataset_input_load_failed: node "input" failed to load dataset ri.foundry.main.dataset.0: '
           'dataset_input_schema_rejected: ' + CROSSED + ': not the transaction of v2')


def outcome(status="failed", error=REFUSED, later=("cancelled", "cancelled"), resources=()):
    """One saved run as the report summarizes it: its status and its nodes."""
    return dict(status=status, nodes=[
        dict(node_id="input", status="failed", error=error, output_resources=list(resources)),
        dict(node_id="project-transactions", status=later[0], output_resources=[]),
        dict(node_id="output", status=later[1], output_resources=[])])


class RefusedAtInput(unittest.TestCase):
    def test_accepts_a_run_refused_at_its_input_with_the_named_code(self):
        self.assertTrue(refused_at_input(outcome(), CROSSED))
        # Without a named code the refusal of another organization needs no status of its own.
        self.assertTrue(refused_at_input(outcome(error=REFUSED.replace(CROSSED, "batched_dataset_404: no such dataset"))))

    def test_requires_the_explicit_status_next_to_the_code(self):
        for code in ("batched_dataset_404_dataset_schema_transaction_mismatch", "dataset_schema_transaction_mismatch"):
            with self.subTest(code=code):
                self.assertFalse(refused_at_input(outcome(error=REFUSED.replace(CROSSED, code)), CROSSED))

    def test_requires_the_refusal_in_the_error_of_the_input_node(self):
        for name, before, after in (("not the load", "dataset_input_load_failed", "dataset_input_pipeline_failed"),
                                    ("not the schema", "dataset_input_schema_rejected", "dataset_input_rows_invalid")):
            with self.subTest(name=name):
                self.assertFalse(refused_at_input(outcome(error=REFUSED.replace(before, after)), CROSSED))

    def test_requires_the_two_nodes_after_the_input_by_name(self):
        # The order-line Pipeline of noon cuts its input by project-lines, which is not a node here.
        other = dict(status="failed", nodes=[outcome()["nodes"][0],
                                             dict(node_id="project-lines", status="cancelled", output_resources=[]),
                                             outcome()["nodes"][2]])
        self.assertFalse(refused_at_input(other, CROSSED))

    def test_refuses_a_run_that_committed_an_output_or_whose_later_node_ran(self):
        for name, run in (("committed", outcome(resources=("committed",))),
                          ("later node succeeded", outcome(later=("cancelled", "succeeded"))),
                          ("later node failed", outcome(later=("failed", "cancelled"))),
                          ("run succeeded", outcome(status="succeeded")),
                          ("no node result", dict(status="failed", nodes=[]))):
            with self.subTest(run=name):
                self.assertFalse(refused_at_input(run, CROSSED))


if __name__ == "__main__":
    unittest.main()
