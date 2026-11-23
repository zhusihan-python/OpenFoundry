"""Check the audit-sink HTTP contract with a real worker and an unavailable catalog."""
import http.client
import json
import os
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from uuid import uuid4

SERVICE = Path(__file__).resolve().parents[1]
PATH = "/openfoundry/iceberg/v1/append"
WAREHOUSE = "wp65-writer-tests"
FIELDS = [
    {"id": 1, "name": "event_id", "type": "uuid", "required": True},
    {"id": 2, "name": "at", "type": "timestamptz", "required": True},
    {"id": 3, "name": "correlation_id", "type": "string", "required": False},
    {"id": 4, "name": "kind", "type": "string", "required": True},
    {"id": 5, "name": "payload", "type": "string", "required": True},
]


def batch(rows, **changes):
    spec = {"catalog": "lakekeeper", "warehouse": WAREHOUSE, "namespace": "of_audit", "table": "events",
            "partition_transform": "day(at)", "sort_order": "at ASC", "schema": FIELDS}
    return json.dumps({"spec": spec | changes, "rows": rows}).encode()


def event(**changes):
    return {"event_id": str(uuid4()), "at": 1_763_900_000_000_000, "correlation_id": None,
            "kind": "synthetic", "payload": '{"ok":true}'} | changes


class WriterHTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reserve the catalog port without listening so a valid append gets a deterministic refusal.
        closed_catalog = socket.socket()
        cls.addClassCleanup(closed_catalog.close)
        closed_catalog.bind(("127.0.0.1", 0))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            cls.port = probe.getsockname()[1]
        environment = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
        environment.update(PORT=str(cls.port), ICEBERG_WAREHOUSE=WAREHOUSE,
                           CATALOG_URI=f"http://127.0.0.1:{closed_catalog.getsockname()[1]}/catalog")
        cls.process = subprocess.Popen([sys.executable, "writer.py"], cwd=SERVICE, env=environment)
        cls.addClassCleanup(cls.stop)
        deadline = time.monotonic() + 20
        while cls.process.poll() is None and time.monotonic() < deadline:
            try:
                if cls.request("GET", "/healthz")[0] == 200:
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise AssertionError("writer failed to become healthy")

    @classmethod
    def stop(cls):
        cls.process.terminate()
        try:
            cls.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            cls.process.kill()
            cls.process.wait()

    @classmethod
    def request(cls, method, path, body=None, **headers):
        connection = http.client.HTTPConnection("127.0.0.1", cls.port, timeout=15)
        try:
            connection.request(method, path, body, {"Content-Type": "application/json"} | headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def test_routes_and_content_type(self):
        self.assertEqual(self.request("GET", "/healthz"), (200, b"ok"))
        status, metrics = self.request("GET", "/metrics")
        self.assertEqual(status, 200)
        self.assertIn(b"audit_table_writer_max_batch_bytes 67108864", metrics)
        self.assertEqual(self.request("POST", "/elsewhere")[0], 404)
        self.assertEqual(self.request("GET", PATH)[0], 405)
        self.assertEqual(self.request("POST", PATH, b"{}", **{"Content-Type": "text/plain"})[0], 415)

    def test_wrong_table_or_layout(self):
        for changed, status in [({"namespace": "other"}, 404), ({"table": "other"}, 404),
                                ({"warehouse": "other"}, 422), ({"catalog": "sqlite"}, 422),
                                ({"schema": FIELDS[:-1]}, 422), ({"sort_order": "at DESC"}, 422),
                                ({"partition_transform": "month(at)"}, 422)]:
            with self.subTest(changed=changed):
                self.assertEqual(self.request("POST", PATH, batch([event()], **changed))[0], status)

    def test_invalid_rows_and_batch(self):
        self.assertEqual(self.request("POST", PATH, b"{")[0], 400)
        for rows in ([], ["row"], [event(event_id="invalid")], [event(at=True)], [event(payload="invalid JSON")],
                     [event(correlation_id=7)], [event(extra=1)], [event(kind=None)]):
            with self.subTest(rows=rows):
                self.assertEqual(self.request("POST", PATH, batch(rows))[0], 422)

    def test_limits(self):
        headers = {"Content-Length": str(64 * 1024 * 1024 + 1)}
        self.assertEqual(self.request("POST", PATH, b"", **headers)[0], 413)
        self.assertEqual(self.request("POST", PATH, batch([None] * 100_001))[0], 422)

    def test_in_batch_conflict_precedes_catalog_access(self):
        first = event()
        for changes in ({"at": first["at"] + 1}, {"kind": "different"}, {"payload": "false"}):
            with self.subTest(changes=changes):
                self.assertEqual(self.request("POST", PATH, batch([first, first | changes]))[0], 409)
        self.assertEqual(self.request("POST", PATH, batch([first, first]))[0], 503)

    def test_unavailable_catalog_is_retryable_and_health_stays_up(self):
        row = event(payload='"synthetic-private-value"')
        status, body = self.request("POST", PATH, batch([row]))
        self.assertEqual(status, 503)
        self.assertNotIn(row["payload"].encode(), body)
        self.assertEqual(self.request("GET", "/healthz")[0], 200)


if __name__ == "__main__":
    unittest.main()
