import hashlib
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock
from urllib.error import URLError

import preview
from smoke import expected, numeric_unit_price, projection


class LocalOnlyRedirects(unittest.TestCase):
    """Two loopback services standing in for the gateway and the dataset service."""

    def setUp(self):
        seen = self.seen = []
        signed = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append((self.server.server_port, self.headers.get("Authorization")))
                if self.path == "/download":
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{signed[0]}/signed")
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        self.gateway, service = HTTPServer(("127.0.0.1", 0), Handler), HTTPServer(("127.0.0.1", 0), Handler)
        signed.append(service.server_port)
        self.servers = (self.gateway, service)
        for server in self.servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()

    def tearDown(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()

    def endpoints(self, dataset_port):
        return {"WP23_GATEWAY": f"http://127.0.0.1:{self.gateway.server_port}",
                "WP23_DVS": f"http://127.0.0.1:{dataset_port}"}

    def test_credential_stays_with_the_service_it_was_presented_to(self):
        with mock.patch.dict(os.environ, self.endpoints(self.servers[1].server_port)):
            reply = preview.request("GET", "/download", "reader")
        self.assertEqual((reply["status"], reply["body"]), (200, b"{}"))
        self.assertEqual(reply["redirects"], [(302, f"127.0.0.1:{self.servers[1].server_port}")])
        self.assertEqual(self.seen, [(self.gateway.server_port, "Bearer reader"), (self.servers[1].server_port, None)])

    def test_redirect_leaves_the_run_endpoints_unfollowed(self):
        with mock.patch.dict(os.environ, self.endpoints(1)):
            with self.assertRaises(URLError):
                preview.request("GET", "/download", "reader")
        self.assertEqual(self.seen, [(self.gateway.server_port, "Bearer reader")])


class VersionComparison(unittest.TestCase):
    def test_second_batch(self):
        oracle = preview.compare_records(projection(1), projection(2))
        self.assertEqual(preview.stated(oracle), (preview.EXPECTED["v1-v2"]["counts"], preview.EXPECTED["v1-v2"]["changes"]))
        modified, *added = oracle["changes"]
        self.assertEqual((modified["before"]["unit_price"], modified["after"]["unit_price"]), ("49", "49.25"))
        self.assertEqual([(row["key"], row["kind"]) for row in added],
                         [("NRPL0004-1", "added"), ("NRPL0005-1", "added")])
        self.assertTrue(all("before" not in row for row in added))

    def test_replayed_first_batch(self):
        oracle = preview.compare_records(projection(1), projection(1))
        self.assertEqual(preview.stated(oracle), (preview.EXPECTED["v1-v3"]["counts"], preview.EXPECTED["v1-v3"]["changes"]))
        self.assertEqual(oracle["changes"], [])

    def test_summary(self):
        self.assertEqual(preview.summary(preview.EXPECTED["v1-v2"]["counts"]),
                         "2 added · 0 removed · 1 modified · 2 unchanged")
        self.assertEqual(preview.summary(preview.EXPECTED["v1-v3"]["counts"]),
                         "0 added · 0 removed · 0 modified · 3 unchanged")

    def test_duplicate_key(self):
        with self.assertRaises(ValueError):
            preview.compare_records(projection(1), projection(1) + projection(1))


class CommittedSchema(unittest.TestCase):
    """Only a committed declaration bound to its version is a schema to compare."""

    BINDING = ("dataset", "main", 1, "transaction")
    FIELDS = [{"name": "item_nr", "field_type": "STRING", "nullable": False}]

    def answer(self, **changes):
        body = dict(dataset_id="dataset", branch="main", version=1, transaction_id="transaction",
                    fields=[dict(field) for field in self.FIELDS], content_hash=preview.schema_hash(self.FIELDS))
        return dict(body, **changes)

    def test_declared_fields(self):
        self.assertEqual(preview.committed_schema(self.answer(), self.BINDING), self.FIELDS)

    def test_content_hash_is_the_hash_of_the_compact_fields(self):
        fields = [dict(name="unit_price", field_type="DOUBLE", nullable=False)]
        self.assertEqual(preview.schema_hash(fields), hashlib.sha256(
            b'[{"name":"unit_price","field_type":"DOUBLE","nullable":false}]').hexdigest())

    def test_no_schema_is_never_a_schema_to_compare(self):
        tampered = self.answer()
        tampered["fields"][0]["field_type"] = "DOUBLE"
        without_fields = {name: value for name, value in self.answer().items() if name != "fields"}
        # A missing, empty or changed-after-hashing declaration, another hash,
        # another version, another transaction.
        refused = ((self.answer(fields=None), self.BINDING), (without_fields, self.BINDING),
                   (self.answer(fields=[], content_hash=preview.schema_hash([])), self.BINDING),
                   (tampered, self.BINDING), (self.answer(content_hash="0" * 64), self.BINDING),
                   (self.answer(), ("dataset", "main", 2, "transaction")),
                   (self.answer(), ("dataset", "main", 1, "another transaction")))
        for body, binding in refused:
            with self.subTest(body=body, binding=binding), self.assertRaises(ValueError):
                preview.committed_schema(body, binding)


class StatedSchema(unittest.TestCase):
    """The Schema diff panel states exactly one thing. A mixed panel never passes."""

    ROW = dict(sign="~", name="unit_price", hint="STRING → DOUBLE")

    def view(self, **changes):
        return dict(dict(items=1, rows=[], empty_notices=1, other=[]), **changes)

    def test_one_empty_notice_states_no_change(self):
        self.assertTrue(preview.states_schema(self.view(), []))

    def test_one_row_states_the_change(self):
        self.assertTrue(preview.states_schema(self.view(items=1, rows=[self.ROW], empty_notices=0), [self.ROW]))

    def test_a_mixed_or_blank_panel_never_passes(self):
        # The change next to the empty notice or next to a sentence of its own, a
        # second change, a missing or a doubled notice, a look-alike sentence, an
        # empty panel: whichever statement each is held against.
        mixed = (dict(items=2, rows=[self.ROW], empty_notices=1),
                 dict(items=2, rows=[self.ROW], other=["unit_price is now a number"]),
                 dict(items=2, rows=[self.ROW, dict(sign="+", name="sku", hint="STRING")], empty_notices=0),
                 dict(items=1, rows=[self.ROW], empty_notices=1),
                 dict(items=2, rows=[self.ROW], empty_notices=0, other=["No schema changes"]),
                 dict(items=2, rows=[], empty_notices=1, other=["unit_price is now a number"]),
                 dict(items=2, rows=[], empty_notices=2),
                 dict(items=1, rows=[], empty_notices=0, other=["No schema changes"]),
                 dict(items=0, rows=[], empty_notices=0, other=[]))
        for view in mixed:
            for expected in ([], [self.ROW]):
                with self.subTest(view=view, expected=expected):
                    self.assertFalse(preview.states_schema(view, expected))


class OrderLineRows(unittest.TestCase):
    def test_second_batch_changed_rows_and_a_price(self):
        self.assertTrue(preview.rows_and_price_changed(expected(1), expected(2)))
        self.assertFalse(preview.rows_and_price_changed(expected(1), expected(1)))

    def test_numeric_unit_price_changed_the_unit_price_alone(self):
        self.assertTrue(preview.only_unit_price_changed(expected(2), numeric_unit_price(2)))
        self.assertFalse(preview.only_unit_price_changed(expected(1), expected(2)))
        self.assertFalse(preview.only_unit_price_changed(expected(2), expected(2)))

    def test_every_selection_states_its_schema_changes(self):
        self.assertEqual([name for name, _, _ in preview.SCHEMA_GROUPS], list(preview.EXPECTED_SCHEMA))
        self.assertEqual([name for name, first, _ in preview.SCHEMA_GROUPS if first == 2], ["v2-v3"])


if __name__ == "__main__":
    unittest.main()
