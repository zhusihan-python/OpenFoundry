import unittest
from decimal import Decimal

from products import collect, commit_body


def product(i, cost="0.00"):
    return {"id": i, "sku": f"{i:04}", "product_name": f"商品 {i}", "cg_price": cost}


def page(rows, total):
    return {"code": 0, "data": rows, "total": total}


class ProductPages(unittest.TestCase):
    def test_offset_uses_returned_rows(self):
        calls = []
        def read(offset, length):
            calls.append((offset, length))
            return {0: page([product(1), product(2, Decimal("5.27"))], 3),
                    2: page([product(3)], 3)}[offset]
        rows = collect(read, 2)
        self.assertEqual([(0, 2), (2, 2)], calls)
        self.assertEqual([(r["id"], r["sku"], r["cg_price"]) for r in rows],
                         [(1, "0001", "0.00"), (2, "0002", "5.27"), (3, "0003", "0.00")])

    def test_incomplete_or_inconsistent_page(self):
        for bad in (page([], 3), page([product(2)], 3), page([product(3)], 4),
                    page([{"id": 3}], 3), page([product(3), product(4)], 3),
                    {"code": 500, "data": [], "total": 3}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                collect(lambda offset, length: bad if offset else page([product(1), product(2)], 3), 2)

    def test_missing_required_field_on_second_page(self):
        for field in ("id", "sku", "product_name", "cg_price"):
            incomplete = product(3)
            del incomplete[field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                collect(lambda offset, length: page([incomplete], 3) if offset else
                        page([product(1), product(2)], 3), 2)

    def test_read_error_propagates(self):
        def read(offset, length):
            if offset:
                raise OSError("second page unavailable")
            return page([product(1)], 2)
        with self.assertRaises(OSError):
            collect(read, 1)

    def test_empty_source_kept_separate_from_publication(self):
        self.assertEqual(collect(lambda *_: page([], 0), 2), [])
        with self.assertRaises(ValueError):
            commit_body("source", "batch", b"")

    def test_bad_total_or_cost(self):
        for value in (None, True, -1, 1.5):
            with self.subTest(total=value), self.assertRaises(ValueError):
                collect(lambda *_: page([], value), 2)
        for cost in (None, False, "", "NaN", "Infinity"):
            with self.subTest(cost=cost), self.assertRaises(ValueError):
                collect(lambda *_: page([product(1, cost)], 1), 2)


if __name__ == "__main__":
    unittest.main()
