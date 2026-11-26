import hashlib
import json
import unittest
from decimal import Decimal

from products import PRODUCT_SCHEMA, collect, commit_body, ndjson, project, published_schema


def product(i, cost="0.00"):
    return {"id": i, "sku": f"{i:04}", "product_name": f"商品 {i}", "cg_price": cost}


def page(rows, total):
    return {"code": 0, "data": rows, "total": total}


def wire(body):
    """The bytes the fixture sends: one key order and spacing, so the bytes decide the fingerprint."""
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


# The request of one fixed delivery as it was sent before the input contract
# existed, with no schema. It is byte for byte what revision 39e1622ad built.
ONE = ndjson([{"id": 11, "sku": "0007", "product_name": "sample", "cg_price": "0.00"}])
HISTORICAL_REQUEST = (
    '{"branch":"main","files":[{"content_base64":"eyJjZ19wcmljZSI6IjAuMDAiLCJpZCI6MTEsInByb2R1Y3RfbmFtZSI6InNhbXBsZSIsInNrdSI6IjAwMDcifQo=",'
    '"content_type":"application/x-ndjson","logical_path":"products.ndjson","operation":"ADD","size_bytes":65}],"format":"json",'
    '"idempotency_key":"lingxing-product:src-dataset:batch-0001","metadata":{"content_hash":'
    '"0a2cc9c16e07ed1d261d006c85e0fc92cfc1d9f1f156ef4b6736b0dd40210310","source_batch":"batch-0001","source_dataset":"src-dataset"},'
    '"transaction_type":"SNAPSHOT"}').encode()


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


class ProductContract(unittest.TestCase):
    ROWS = [project(product(11, "0.00")), project(product(12, "9.25"))]

    def test_the_contract_has_its_own_order_whatever_order_the_rows_come_in(self):
        # The rows of a downloaded file carry the sorted keys ndjson() wrote; a page delivers id first.
        downloaded = [json.loads(line) for line in ndjson(self.ROWS).splitlines()]
        self.assertEqual(list(downloaded[0]), ["cg_price", "id", "product_name", "sku"])
        self.assertEqual(published_schema(self.ROWS), published_schema(downloaded))
        self.assertEqual(published_schema(downloaded), {"file_format": "TEXT", "fields": PRODUCT_SCHEMA})
        self.assertEqual([(field["name"], field["type"], field["nullable"]) for field in PRODUCT_SCHEMA],
                         [("cg_price", "STRING", False), ("id", "LONG", False),
                          ("product_name", "STRING", False), ("sku", "STRING", False)])

    def test_each_row_holds_the_contract_columns_with_the_contract_types(self):
        for bad in ({"id": 11}, dict(self.ROWS[0], extra="x"), dict(self.ROWS[0], id=True), dict(self.ROWS[0], id="11"),
                    dict(self.ROWS[0], sku=7), dict(self.ROWS[0], cg_price=Decimal("0.00")),
                    dict(self.ROWS[0], product_name=None)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                published_schema(self.ROWS[:1] + [bad])
        self.assertEqual([field["name"] for field in published_schema([])["fields"]],
                         ["cg_price", "id", "product_name", "sku"])

    def test_every_snapshot_answers_one_contract(self):
        first, second = published_schema(self.ROWS), published_schema(self.ROWS[:1])
        self.assertEqual(first, second)
        first["fields"][0]["type"] = "DOUBLE"
        self.assertEqual(second, published_schema(self.ROWS))

    def test_a_request_without_a_schema_is_the_one_sent_before_the_contract(self):
        self.assertEqual(wire(commit_body("src-dataset", "batch-0001", ONE)), HISTORICAL_REQUEST)
        self.assertEqual(hashlib.sha256(HISTORICAL_REQUEST).hexdigest(),
                         "b9e1e17714120b3e3b9ecd82987176e8eeaa1ba7ead78fa1c1838f0bcd026f49")
        self.assertNotIn("schema", commit_body("src-dataset", "batch-0001", ONE))

    def test_the_contract_adds_the_schema_and_is_another_request_under_the_same_key(self):
        rows = [json.loads(line) for line in ONE.splitlines()]
        under = wire(commit_body("src-dataset", "batch-0001", ONE, schema=published_schema(rows)))
        self.assertEqual(under, wire(commit_body("src-dataset", "batch-0001", ONE, schema=published_schema(rows))))
        self.assertNotEqual(under, HISTORICAL_REQUEST)
        with_schema = json.loads(under)
        self.assertEqual(with_schema.pop("schema"), {"file_format": "TEXT", "fields": PRODUCT_SCHEMA})
        self.assertEqual(wire(with_schema), HISTORICAL_REQUEST)


if __name__ == "__main__":
    unittest.main()
