"""Regression coverage for the local single-writer adapter."""
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import iceberg_table as iceberg
from run import until


class ReplayTest(unittest.TestCase):
    def test_mixed_replay_and_conflict_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            @contextmanager
            def writer():
                (root / "port").unlink(missing_ok=True)
                process = subprocess.Popen([sys.executable, iceberg.__file__, "serve", "--root", directory])
                try:
                    until("writer ready", lambda: (root / "port").exists(), seconds=20)
                    yield
                finally:
                    process.terminate()
                    process.wait(timeout=10)

            def post(rows):
                fields = [{"id": f.field_id, "name": f.name, "type": str(f.field_type), "required": f.required}
                          for f in iceberg.SCHEMA.fields]
                spec = dict(catalog="lakekeeper", namespace="of_audit", table="events",
                            partition_transform="day(at)", sort_order="at ASC", schema=fields)
                url = "http://127.0.0.1:" + (root / "port").read_text() + iceberg.APPEND_PATH
                request = Request(url, data=json.dumps({"spec": spec, "rows": rows}).encode(),
                                  headers={"Content-Type": "application/json"})
                with urlopen(request, timeout=10) as response:
                    self.assertEqual(response.status, 204)

            first = dict(event_id=str(uuid4()), at=100, kind="synthetic", payload='{"ok":true}')
            second = dict(first, event_id=str(uuid4()))
            with writer():
                post([first, first])

            with writer():
                post([first, second, second])
                post([second, first])
                with self.assertRaises(HTTPError) as failure:
                    post([dict(first, event_id=str(uuid4())), dict(first, payload='{"ok":false}')])
                self.assertEqual(failure.exception.code, 409)
                failure.exception.close()
                table = iceberg.catalog(root).load_table(iceberg.TABLE)
                self.assertCountEqual([str(row["event_id"]) for row in table.scan().to_arrow().to_pylist()],
                                      [first["event_id"], second["event_id"]])
                self.assertEqual(len(table.snapshots()), 2)


if __name__ == "__main__":
    unittest.main()
