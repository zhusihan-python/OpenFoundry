import json
import unittest

from smoke import (COLUMNS, HERE, ORDER_LINE_COLUMNS, ORDER_LINE_SCHEMA, columns_of, encode, expected, line_price_nodes,
                   materialize, numeric_unit_price, numeric_unit_price_schema, projection, published_schema)


class OrderLiveReplay(unittest.TestCase):
    def setUp(self):
        self.rows = [json.loads(line) for line in (HERE / "replay.ndjson").read_bytes().splitlines()]

    def test_first_batch(self):
        self.assertEqual(materialize([], self.rows[:3]), expected(1))

    def test_pinned_parent_and_partial_delta(self):
        self.assertEqual(materialize(expected(1), [dict(self.rows[0], offer_price="49.25"), *self.rows[3:]]), expected(2))

    def test_two_items_in_one_order(self):
        second = dict(self.rows[0], item_nr="NRPL0001-2")
        result = materialize([], [self.rows[0], second])
        self.assertEqual([row["order_nr"] for row in result], ["NRPL0001", "NRPL0001"])
        self.assertEqual(len({row["item_nr"] for row in result}), 2)

    def test_projection(self):
        for version, count, price in ((1, 3, "49"), (2, 5, "49.25")):
            rows = projection(version)
            self.assertEqual(list(rows[0]), COLUMNS)
            self.assertEqual(len({(row["store"], row["market"], row["item_nr"]) for row in rows}), count)
            self.assertEqual((rows[0]["item_nr"], rows[0]["currency"], rows[0]["unit_price"]),
                             ("NRPL0001-1", "AED", price))
            self.assertTrue(all(isinstance(row["unit_price"], str) for row in rows))

    def test_invalid_batch(self):
        for delta in ([self.rows[0], self.rows[0]], [dict(self.rows[0], currency_code="")],
                      [dict(self.rows[0], country_code="SA")], [dict(self.rows[0], offer_price="NaN")]):
            with self.subTest(delta=delta), self.assertRaises(ValueError):
                materialize([], delta)


CONTRACT = {"file_format": "TEXT", "fields": [
    {"name": column, "type": "STRING", "nullable": False} for column in ORDER_LINE_COLUMNS]}


class OrderLineSchema(unittest.TestCase):
    def test_contract_is_fixed_and_matches_the_published_rows(self):
        for version in (1, 2):
            rows = [json.loads(line) for line in encode(expected(version)).splitlines()]
            # The published key order is the declared order, and every value is a string.
            self.assertEqual([list(row) for row in rows], [ORDER_LINE_COLUMNS] * len(rows))
            self.assertEqual(published_schema(expected(version)), CONTRACT)
            self.assertEqual(columns_of(encode(expected(version))), ORDER_LINE_SCHEMA)

    def test_empty_snapshot_declares_the_contract_columns(self):
        self.assertEqual(published_schema([]), CONTRACT)

    def test_unit_price_stays_a_string_when_it_changes(self):
        prices = [[row["unit_price"] for row in map(json.loads, encode(expected(v)).splitlines())] for v in (1, 2)]
        self.assertEqual(prices, [["49", "120", "30"], ["49.25", "120", "30", "25", "60"]])
        self.assertTrue(all(isinstance(price, str) for batch in prices for price in batch))

    def test_row_key_order_does_not_change_the_contract(self):
        reordered = [dict(reversed(list(row.items()))) for row in expected(1)]
        self.assertEqual(published_schema(reordered), CONTRACT)

    def test_rejects_missing_extra_and_mistyped_columns(self):
        missing = [{key: value for key, value in row.items() if key != "sku"} for row in expected(1)]
        extra = [dict(row, extra="x") for row in expected(1)]
        mistyped = [dict(expected(1)[0], unit_price=49.25)]
        for name, rows in (("missing", missing), ("extra", extra), ("mistyped", mistyped)):
            with self.subTest(rows=name), self.assertRaises(ValueError):
                published_schema(rows)
        # The independent reader derives what the bytes hold, so its answer
        # differs from the contract for such rows.
        for name, rows in (("missing", missing), ("extra", extra)):
            self.assertNotEqual(columns_of(encode(rows)), ORDER_LINE_SCHEMA, name)
        with self.assertRaises(ValueError):
            columns_of(encode(mistyped))


DIGEST = "a" * 64


class Graph:
    @staticmethod
    def graph_nodes(source, version, txn, out):
        return [{"id": "input", "config": {"dataset_id": source, "dataset_snapshot": {"branch": "main", "version": version, "transaction_id": txn}}},
                {"id": "output", "depends_on": ["input"]}]


class SchemaPin(unittest.TestCase):
    def test_pin_without_a_hash_declares_no_key(self):
        nodes = line_price_nodes(Graph(), "source", "out", {"version": 2, "transaction": "txn"})
        self.assertEqual(nodes[0]["config"]["dataset_snapshot"], {"branch": "main", "version": 2, "transaction_id": "txn"})

    def test_pin_with_a_hash_adds_the_key_to_the_input_snapshot_only(self):
        nodes = line_price_nodes(Graph(), "source", "out", {"version": 2, "transaction": "txn"}, DIGEST)
        self.assertEqual(nodes[0]["config"]["dataset_snapshot"],
                         {"branch": "main", "version": 2, "transaction_id": "txn", "expected_schema_hash": DIGEST})
        self.assertNotIn("expected_schema_hash", json.dumps(nodes[1:]))


NUMERIC = {"file_format": "TEXT", "fields": [
    {"name": column, "type": "DOUBLE" if column == "unit_price" else "STRING", "nullable": False} for column in ORDER_LINE_COLUMNS]}


class NumericUnitPrice(unittest.TestCase):
    def test_only_the_unit_price_changes_to_a_number(self):
        rows = numeric_unit_price(2)
        self.assertEqual([{key: value for key, value in row.items() if key != "unit_price"} for row in rows],
                         [{key: value for key, value in row.items() if key != "unit_price"} for row in expected(2)])
        self.assertEqual([row["unit_price"] for row in rows], [49.25, 120.0, 30.0, 25.0, 60.0])
        self.assertTrue(all(type(row["unit_price"]) is float for row in rows))
        # The published key order is the declared order, as for the string contract.
        self.assertEqual([list(row) for row in map(json.loads, encode(rows).splitlines())], [ORDER_LINE_COLUMNS] * len(rows))

    def test_declares_the_unit_price_as_double_and_the_rest_as_before(self):
        self.assertEqual(numeric_unit_price_schema(numeric_unit_price(2)), NUMERIC)

    def test_each_contract_refuses_the_rows_of_the_other(self):
        for name, declare, rows in (("string price", numeric_unit_price_schema, expected(2)),
                                    ("numeric price", published_schema, numeric_unit_price(2)),
                                    ("boolean price", numeric_unit_price_schema, [dict(row, unit_price=True) for row in numeric_unit_price(2)]),
                                    ("missing column", numeric_unit_price_schema,
                                     [{key: value for key, value in row.items() if key != "sku"} for row in numeric_unit_price(2)])):
            with self.subTest(rows=name), self.assertRaises(ValueError):
                declare(rows)
