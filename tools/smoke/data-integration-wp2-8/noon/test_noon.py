import json
import unittest

from smoke import HERE, expected, materialize


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

    def test_invalid_batch(self):
        for delta in ([self.rows[0], self.rows[0]], [dict(self.rows[0], currency_code="")],
                      [dict(self.rows[0], country_code="SA")], [dict(self.rows[0], offer_price="NaN")]):
            with self.subTest(delta=delta), self.assertRaises(ValueError):
                materialize([], delta)
