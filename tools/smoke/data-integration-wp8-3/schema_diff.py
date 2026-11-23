"""Publish and independently verify the real historical Schema Diff fixture."""

from base64 import b64encode
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import re
import signal
import time
from urllib.parse import urlencode

import duckdb


spec = spec_from_file_location("wp83_harness", Path(__file__).with_name("harness.py"))
storage = module_from_spec(spec)
spec.loader.exec_module(storage)

history = storage
writer = storage.writer
stack = storage.stack
expect = storage.expect
FIXTURES = (
    ("a.parquet", "SELECT 1::BIGINT AS order_id, 17::BIGINT AS amount, 'old'::VARCHAR AS legacy_field"),
    ("b.parquet", "SELECT 1::BIGINT AS order_id, '17.25'::VARCHAR AS amount, true AS added_field"),
    ("c.parquet", "SELECT 1::BIGINT AS order_id, 17.25::DOUBLE AS amount, 'v3'::VARCHAR AS head_only"),
)
SQL_TYPES = {"BIGINT": "LONG", "VARCHAR": "STRING", "BOOLEAN": "BOOLEAN", "DOUBLE": "DOUBLE"}


def publish(dataset, ordinal):
    name, sql = FIXTURES[ordinal - 1]
    artifact = writer.EVIDENCE / f"schema-diff-v{ordinal}.parquet"
    with duckdb.connect() as conn:
        conn.sql(sql).write_parquet(str(artifact))
        columns = conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [str(artifact)],
        ).fetchall()
        physical = conn.execute(
            "SELECT name, repetition_type FROM parquet_schema(?) WHERE num_children IS NULL", [str(artifact)],
        ).fetchall()
    expect("DuckDB reports the same physical and logical columns",
           [field[0] for field in physical], [field[0] for field in columns])
    expect("fixture uses known Parquet repetition types",
           all(repetition in {"OPTIONAL", "REQUIRED"} for _, repetition in physical), True)
    fields = [{"name": name, "field_type": SQL_TYPES[kind], "nullable": repetition == "OPTIONAL"}
              for (name, kind, *_), (_, repetition) in zip(columns, physical, strict=True)]
    raw = artifact.read_bytes()
    status, receipt = stack.http("POST", writer.dvs(dataset, "outputs:commit"), {
        "idempotency_key": f"wp83-schema-diff-v{ordinal}",
        "branch": "main",
        "transaction_type": "SNAPSHOT",
        "summary": f"Schema diff fixture v{ordinal}",
        "files": [{"logical_path": name, "operation": "ADD", "size_bytes": len(raw),
                   "content_type": writer.MIME, "content_base64": b64encode(raw).decode()}],
    }, allow=(201,))
    txn = receipt["transaction"]
    expect("fixture is a real main snapshot",
           (status, txn["status"], txn["branch_name"], txn["dataset_id"], txn["tx_type"]),
           (201, "COMMITTED", "main", dataset, "SNAPSHOT"))
    expect("fixture version was assigned by DVS", stack.version_by_txn(dataset, txn["id"]), ordinal)
    registered, = [item for item in writer.file_list(dataset)
                   if (item["transaction_id"], item["logical_path"]) == (txn["id"], name)]
    expect("signed file bytes equal independently inspected Parquet",
           writer.download(dataset, registered), raw)
    return {
        "dataset": dataset, "branch": "main", "version": ordinal, "transaction": txn["id"], "fields": fields,
        "file": {"id": registered["id"], "transaction": txn["id"], "path": name,
                 "size_bytes": len(raw), "sha256": sha256(raw).hexdigest(), "artifact": artifact.name},
    }


def read_schema(pin, status=200):
    query = urlencode({"branch": pin["branch"], "version": pin["version"]})
    actual, response = stack.http("GET", writer.dvs(pin["dataset"], f"schema?{query}"), timeout=5, allow=(status,))
    expect("pinned schema HTTP status", actual, status)
    expected = {"error": storage.MISSING} if status == 422 else {
        "dataset_id": pin["dataset"], "branch": pin["branch"], "version": pin["version"],
        "transaction_id": pin["transaction"], "fields": pin["fields"],
    }
    expect("schema response is tied to the exact requested snapshot", response, expected)
    return {"http_status": actual, **response}


def main():
    envdir = Path(os.environ["OF33C_ENVDIR"])
    marker = lambda name: envdir / f"schema-diff-{name}.json"
    expected_markers = ("ready", "compared", "head", "rechecked", "missing", "observed", "restored")
    expect("browser and driver markers start clean", any(marker(name).exists() for name in expected_markers), False)

    stop_requested = False
    changing_storage = False

    def check_running():
        if stop_requested:
            raise SystemExit("historical schema diff fixture interrupted")

    def request_stop(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True
        if not changing_storage:
            check_running()

    def capture_time(name, since):
        def capture():
            check_running()
            if not marker(name).is_file():
                return None
            at = json.loads(marker(name).read_text())["captured_at_unix_ms"]
            expect("browser capture followed the matching backend observation",
                   type(at) is int and since <= at <= history.unix_ms(), True)
            return at
        return history.wait_for(f"browser schema diff {name}", capture, time.monotonic() + 180)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    container = storage.owned_dvs()
    dataset = writer.id_tail(stack.create_dataset("wp83-historical-schema-diff"))
    stack.ensure_main_branch(dataset)
    old, next_pin = (publish(dataset, ordinal) for ordinal in (1, 2))
    before = {"a": read_schema(old), "b": read_schema(next_pin)}
    ready_at = history.unix_ms()
    history.atomic_json(marker("ready"), {"old": old, "next": next_pin, "observed_unix_ms": ready_at})
    compared_at = capture_time("compared", ready_at)

    tip = publish(dataset, 3)
    head, = [item for item in stack.list_versions(dataset) if item["version"] == tip["version"]]
    expect("the new snapshot becomes the branch head",
           history.get(writer.dvs(dataset, "branches/main"))["head_transaction_id"], head["transaction_id"])
    after_head = {"a": read_schema(old), "b": read_schema(next_pin), "head": read_schema(tip)}
    expect("both historical schemas survive the new head", {key: after_head[key] for key in before}, before)
    head_at = history.unix_ms()
    history.atomic_json(marker("head"), {"tip": tip, "observed_unix_ms": head_at})
    rechecked_at = capture_time("rechecked", head_at)

    original_files = writer.file_list(dataset)
    old_file, = [file for file in original_files if file["id"] == old["file"]["id"]]
    source, target = storage.scoped_object(old, {
        "files": [old_file], "signed_files": [old["file"]], "backing_files": writer.backing_files(dataset),
    })
    directory = None
    changing_storage = True
    try:
        check_running()
        candidate = storage.docker("exec", container, "mktemp", "-d", "/var/of/wp83-missing.XXXXXX")
        expect("private scratch directory is on the owned volume",
               bool(re.fullmatch(r"/var/of/wp83-missing\.[a-zA-Z0-9]{6}", candidate)), True)
        directory = candidate
        storage.move_to_quarantine(container, source, directory)
        changing_storage = False
        check_running()
        missing = {"a": read_schema(old, 422), "b": read_schema(next_pin)}
        expect("other comparison side remains readable", missing["b"], before["b"])
        expect("the registered file index remains intact during quarantine", writer.file_list(dataset), original_files)
        missing_at = history.unix_ms()
        history.atomic_json(marker("missing"), {"error": missing["a"]["error"], "observed_unix_ms": missing_at})
        unavailable_at = capture_time("observed", missing_at)
        expect("v1 remains unavailable through the browser capture", read_schema(old, 422), missing["a"])
    finally:
        changing_storage = True
        if directory is not None:
            storage.restore_from_quarantine(container, source, directory)
        changing_storage = False
    check_running()

    restored = {"a": read_schema(old), "b": read_schema(next_pin)}
    expect("restored historical schemas match their first reads", restored, before)
    restored_file_sha256 = sha256(writer.download(dataset, old_file)).hexdigest()
    expect("the restored object preserves its signed bytes", restored_file_sha256, target["sha256"])
    restored_at = history.unix_ms()
    writer.write_json("schema-diff", {
        "status": "passed", "old": old, "next": next_pin, "tip": tip, "before": before,
        "after_head": after_head, "missing": missing, "restored": restored,
        "restored_file_sha256": restored_file_sha256,
        "service_observed_unix_ms": {"before": ready_at, "after_head": head_at, "missing": missing_at,
                                     "restored": restored_at},
        "browser_observed_unix_ms": {"before": compared_at, "after_head": rechecked_at, "missing": unavailable_at},
    })
    history.atomic_json(marker("restored"), {"observed_unix_ms": restored_at})
    print("PASS actual historical Parquet schemas before and after a new head, quarantine and restoration", flush=True)


if __name__ == "__main__":
    main()
