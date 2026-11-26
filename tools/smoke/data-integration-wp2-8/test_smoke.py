import hashlib
import json
import os
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import smoke
from smoke import (CONTRACT, COSTS, COST_SHA256, PRODUCT_SCHEMA_SHA256, commit_request, contract_of, cost_nodes, costs,
                   schema_route, value_diffs)

V1 = b'{"cg_price":"0.00","sku":"0007"}\n{"cg_price":"9.25","sku":"W-2"}\n{"cg_price":"17.40","sku":"D3"}\n'
V2 = b'{"cg_price":"2.50","sku":"0007"}\n{"cg_price":"10.75","sku":"N4"}\n'


def wire(body):
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


class CostOracle(unittest.TestCase):
    def test_first_snapshot_keeps_three_string_costs(self):
        self.assertEqual(costs(1), [dict(sku="0007", cg_price="0.00"), dict(sku="W-2", cg_price="9.25"),
                                    dict(sku="D3", cg_price="17.40")])

    def test_second_snapshot_replaces_the_whole_batch(self):
        self.assertEqual(costs(2), [dict(sku="0007", cg_price="2.50"), dict(sku="N4", cg_price="10.75")])
        self.assertTrue({"W-2", "D3"}.isdisjoint(row["sku"] for row in costs(2)))

    def test_cost_bytes_and_hashes_are_pinned(self):
        for version, data in ((1, V1), (2, V2)):
            with self.subTest(version=version):
                self.assertEqual(smoke.products.ndjson(costs(version)), data)
                self.assertEqual(hashlib.sha256(data).hexdigest(), COST_SHA256[version])
        self.assertNotEqual(COST_SHA256[1], COST_SHA256[2])

    def test_no_oracle_for_another_snapshot(self):
        with self.assertRaises(KeyError):
            costs(3)


class ValueReconciliation(unittest.TestCase):
    def test_matching_lines_and_values(self):
        self.assertEqual(value_diffs(V1, costs(1)), [])
        self.assertEqual(value_diffs(V2, costs(2)), [])

    def test_sku_or_cost_as_a_number_is_not_the_stored_string(self):
        data = b'{"cg_price":0.0,"sku":7}\n{"cg_price":"9.25","sku":"W-2"}\n{"cg_price":"17.40","sku":"D3"}\n'
        self.assertEqual(value_diffs(data, costs(1)),
                         ["line 1 cg_price: 0.0, expected '0.00'", "line 1 sku: 7, expected '0007'"])

    def test_line_count_and_the_shared_lines_are_both_named(self):
        self.assertEqual(value_diffs(V2, costs(1)),
                         ["2 lines, expected 3", "line 1 cg_price: '2.50', expected '0.00'",
                          "line 2 cg_price: '10.75', expected '9.25'", "line 2 sku: 'N4', expected 'W-2'"])
        self.assertEqual(value_diffs(V1, costs(2)),
                         ["3 lines, expected 2", "line 1 cg_price: '0.00', expected '2.50'",
                          "line 2 cg_price: '9.25', expected '10.75'", "line 2 sku: 'W-2', expected 'N4'"])

    def test_each_line_is_reconciled_up_to_the_shorter_side(self):
        self.assertEqual(value_diffs(V2, costs(1)[:2]),
                         ["line 1 cg_price: '2.50', expected '0.00'", "line 2 cg_price: '10.75', expected '9.25'",
                          "line 2 sku: 'N4', expected 'W-2'"])

    def test_column_on_one_side_and_broken_lines(self):
        self.assertEqual(value_diffs(b'{"sku":"0007"}\nnot json\n[1]\n', costs(1)),
                         ["line 1 cg_price: present on one side only", "line 2: not JSON", "line 3: not an object"])
        self.assertEqual(value_diffs(b'{"cg_price":"0.00","id":11,"sku":"0007"}\n', costs(1)[:1]),
                         ["line 1 id: present on one side only"])


class Graph:
    """The node builder of the real WP3.4 harness, which reads its environment at import."""

    def __init__(self):
        with mock.patch.dict(os.environ, WP34_PROJECT="test", WP34_EVIDENCE=".", WP34_TOKEN="test"):
            harness = smoke.s3.module("wp28_test_harness", smoke.HERE.parent / "data-integration-wp3-4/harness34.py")
        self.graph_nodes = harness.graph_nodes


class CostPipeline(unittest.TestCase):
    def test_costs_pipeline_pins_the_published_version_and_writes_another_dataset(self):
        nodes = cost_nodes(Graph(), "products", "costs", dict(output_version=2, output_transaction="txn-2"))
        self.assertEqual([node["id"] for node in nodes], ["input", "project-costs", "output"])
        self.assertEqual(nodes[0]["config"], {
            "source_kind": "dataset", "dataset_id": "products",
            "dataset_snapshot": {"branch": "main", "version": 2,
                                 "transaction_id": "ri.foundry.main.transaction.txn-2"}})
        self.assertEqual((nodes[1]["transform_type"], nodes[1]["config"], nodes[1]["depends_on"]),
                         ("select", {"columns": COSTS}, ["input"]))
        self.assertEqual((nodes[2]["transform_type"], nodes[2]["depends_on"], nodes[2]["output_dataset_id"]),
                         ("output_dataset", ["project-costs"], "costs"))
        self.assertEqual((nodes[0]["transform_type"], nodes[0]["input_dataset_ids"]),
                         ("dataset_input", ["products"]))
        self.assertNotIn("costs", json.dumps(nodes[0]))

    def test_each_published_version_pins_its_own_transaction(self):
        pins = [cost_nodes(Graph(), "products", "costs", dict(output_version=v, output_transaction=f"txn-{v}"))[0]
                ["config"]["dataset_snapshot"] for v in (1, 2, 1)]
        self.assertEqual(pins, [{"branch": "main", "version": 1, "transaction_id": "ri.foundry.main.transaction.txn-1"},
                                {"branch": "main", "version": 2, "transaction_id": "ri.foundry.main.transaction.txn-2"},
                                pins[0]])

    def test_the_input_is_bound_to_the_schema_hash_only_when_asked(self):
        bound = cost_nodes(Graph(), "products", "costs", dict(output_version=2, output_transaction="txn-2"),
                           expected_schema_hash=PRODUCT_SCHEMA_SHA256)[0]["config"]["dataset_snapshot"]
        self.assertEqual(bound, {"branch": "main", "version": 2, "transaction_id": "ri.foundry.main.transaction.txn-2",
                                 "expected_schema_hash": PRODUCT_SCHEMA_SHA256})
        loose = cost_nodes(Graph(), "products", "costs", dict(output_version=2, output_transaction="txn-2"))
        self.assertNotIn("expected_schema_hash", loose[0]["config"]["dataset_snapshot"])


class InputContract(unittest.TestCase):
    """The product contract a downstream reader verifies by version and transaction."""

    FIRST = smoke.products.ndjson(smoke.FIRST)
    SECOND = smoke.products.ndjson(smoke.SECOND)

    def test_the_contract_hash_is_the_hash_of_the_stated_fields(self):
        self.assertEqual(hashlib.sha256(json.dumps(CONTRACT, separators=(",", ":")).encode()).hexdigest(),
                         PRODUCT_SCHEMA_SHA256)
        self.assertEqual(PRODUCT_SCHEMA_SHA256,
                         "90fc93dc51e09666772df81377d9c2642d5e444008a7fc65c089193efb6aee4b")
        self.assertEqual(CONTRACT, [
            {"name": "cg_price", "field_type": "STRING", "nullable": False}, {"name": "id", "field_type": "LONG", "nullable": False},
            {"name": "product_name", "field_type": "STRING", "nullable": False}, {"name": "sku", "field_type": "STRING", "nullable": False}])

    def test_a_reader_derives_the_same_contract_from_the_bytes_of_each_snapshot(self):
        self.assertEqual(contract_of(self.FIRST), CONTRACT)
        self.assertEqual(contract_of(self.SECOND), CONTRACT)

    def test_bytes_that_do_not_hold_the_contract_are_not_a_contract(self):
        numeric = b'{"cg_price":0.0,"id":11,"product_name":"x","sku":7}\n'
        for data in (numeric, b'{"id":11,"sku":"0007"}\n', b'{"cg_price":"0.00","id":11,"product_name":"x","sku":"0007","x":1}\n',
                     b'{"cg_price":"0.00","id":11,"product_name":"x","sku":"0007"}\n{"sku":"W-2","id":12,"product_name":"y","cg_price":"9.25"}\n'):
            with self.subTest(data=data), self.assertRaises(ValueError):
                contract_of(data)

    def test_the_schema_request_names_branch_version_transaction_and_hash(self):
        route = schema_route("products", 2, transaction_id="txn-2", expected_content_hash=PRODUCT_SCHEMA_SHA256)
        self.assertEqual(urlsplit(route).path, "/v1/datasets/products/schema")
        self.assertEqual(parse_qs(urlsplit(route).query), {"branch": ["main"], "version": ["2"], "transaction_id": ["txn-2"],
                                                           "expected_content_hash": [PRODUCT_SCHEMA_SHA256]})
        self.assertEqual(parse_qs(urlsplit(schema_route("products", 1)).query), {"branch": ["main"], "version": ["1"]})


class Redelivery(unittest.TestCase):
    """The request of a delivery follows the publication of its batch, not the call that redelivers it."""

    DATA = smoke.products.ndjson(smoke.FIRST)
    ROWS = [json.loads(line) for line in DATA.splitlines()]

    def test_a_batch_published_under_the_contract_is_redelivered_with_its_schema(self):
        batch = dict(source_batch="batch-1", contract=True)
        first = wire(commit_request(batch, "raw", self.DATA, self.ROWS))
        again = wire(commit_request(batch, "raw", self.DATA, self.ROWS))
        self.assertEqual(again, first)
        self.assertEqual(hashlib.sha256(first).hexdigest(), hashlib.sha256(again).hexdigest())
        self.assertEqual(json.loads(first)["schema"], {"file_format": "TEXT", "fields": smoke.products.PRODUCT_SCHEMA})
        self.assertNotEqual(first, wire(smoke.products.commit_body("raw", "batch-1", self.DATA)))

    def test_a_batch_published_before_the_contract_is_redelivered_as_it_was_sent(self):
        batch = dict(source_batch="batch-1")
        sent = wire(commit_request(batch, "raw", self.DATA, self.ROWS))
        self.assertEqual(sent, wire(commit_request(batch, "raw", self.DATA, self.ROWS)))
        self.assertEqual(sent, wire(smoke.products.commit_body("raw", "batch-1", self.DATA)))
        self.assertNotIn("schema", json.loads(sent))
        # Under the same batch key the contract is another request.
        self.assertNotEqual(sent, wire(commit_request(dict(batch, contract=True), "raw", self.DATA, self.ROWS)))


if __name__ == "__main__":
    unittest.main()
