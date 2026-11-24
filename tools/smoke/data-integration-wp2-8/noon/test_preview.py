import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock
from urllib.error import URLError

import preview
from smoke import projection


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


if __name__ == "__main__":
    unittest.main()
