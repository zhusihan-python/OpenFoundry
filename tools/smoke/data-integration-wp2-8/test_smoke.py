import hashlib
import json
import os
import unittest
from unittest import mock

import smoke
from smoke import COSTS, COST_SHA256, cost_nodes, costs, value_diffs

V1 = b'{"cg_price":"0.00","sku":"0007"}\n{"cg_price":"9.25","sku":"W-2"}\n{"cg_price":"17.40","sku":"D3"}\n'
V2 = b'{"cg_price":"2.50","sku":"0007"}\n{"cg_price":"10.75","sku":"N4"}\n'


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


if __name__ == "__main__":
    unittest.main()
