"""Compare complete committed Parquet snapshots with an independent DuckDB oracle."""

from base64 import b64encode
from datetime import datetime, timedelta
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import time
from urllib.parse import urlencode

import duckdb


spec = spec_from_file_location("wp83_harness", Path(__file__).with_name("harness.py"))
history = module_from_spec(spec)
spec.loader.exec_module(history)

writer, stack, expect = history.writer, history.stack, history.expect
KEY = "order_id"
ADDED, MODIFIED, REMOVED = 9007199254740992, 9007199254740993, 9007199254740995
COMMON_ROWS = 129


def source_profile(row):
    return (
        row["order_id"], row["amount"], row["order_date"],
        datetime.fromisoformat(row["ordered_at"]).astimezone(writer.reference.UTC_EPOCH.tzinfo).isoformat(timespec="microseconds"),
        row["source_time_zone"], row["note"],
    )


def publish_parquet(conn, tag, rows, *, expected_rows=None, row_groups=1, predicate=None):
    source = writer.seed_input(tag, [("orders.ndjson", rows)])
    expected = rows if expected_rows is None else expected_rows
    output = writer.publish_case(conn, tag, source, sorted(map(source_profile, expected)), row_groups, predicate)
    return output["parquet"]["path"]


def commit_snapshot(dataset, version, artifacts):
    specs = []
    for path, artifact in artifacts:
        raw = (writer.EVIDENCE / artifact).read_bytes()
        specs.append({"logical_path": path, "operation": "ADD", "size_bytes": len(raw),
                      "content_type": writer.MIME, "content_base64": b64encode(raw).decode()})
    status, reply = stack.http("POST", writer.dvs(dataset, "outputs:commit"), {
        "idempotency_key": f"wp83-match-v{version}",
        "branch": "main", "transaction_type": "SNAPSHOT", "summary": f"Row match fixture v{version}",
        "files": specs,
    }, allow=(201,))
    txn = reply["transaction"]
    expect("row fixture committed on its exact branch",
           (status, txn["status"], txn["dataset_id"], txn["branch_name"], txn["tx_type"]),
           (201, "COMMITTED", dataset, "main", "SNAPSHOT"))
    expect("actual assigned dataset version", stack.version_by_txn(dataset, txn["id"]), version)
    expect("just-published version is the head",
           history.get(writer.dvs(dataset, "branches/main"))["head_transaction_id"], txn["id"])
    registered = {item["logical_path"]: item for item in writer.file_list(dataset)
                  if item["transaction_id"] == txn["id"]}
    expect("commit registered exactly the fixture files", sorted(registered), sorted(path for path, _ in artifacts))
    evidence = []
    for path, artifact in artifacts:
        raw = (writer.EVIDENCE / artifact).read_bytes()
        backing = writer.download(dataset, registered[path])
        expect("signed published bytes are the independently verified Parquet", backing, raw)
        evidence.append({"path": path, "artifact": artifact, "transaction": txn["id"],
                         "size_bytes": len(backing), "sha256": sha256(backing).hexdigest()})
    return {"branch": "main", "version": version, "transaction_id": txn["id"]}, evidence


def read_rows(conn, files):
    rows = conn.execute("""
        SELECT order_id::VARCHAR, amount::VARCHAR, order_date::VARCHAR, epoch_us(ordered_at),
               source_time_zone, note
        FROM read_parquet(?, hive_partitioning = false)
    """, [[str(writer.EVIDENCE / file["artifact"]) for file in files]]).fetchall()
    by_key = {}
    for key, amount, order_date, micros, zone, note in rows:
        timestamp = writer.reference.UTC_EPOCH + timedelta(microseconds=micros)
        by_key[key] = {
            KEY: key, "amount": amount, "order_date": order_date,
            "ordered_at": timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "source_time_zone": zone, "note": note,
        }
    expect("fixture keys are unique in the union of files", len(by_key), len(rows))
    return by_key


def expected_comparison(conn, dataset, base, target, files):
    before = read_rows(conn, files["base"])
    after = read_rows(conn, files["target"])
    changes = [
        {"key": str(ADDED), "kind": "added", "after": after[str(ADDED)]},
        {"key": str(MODIFIED), "kind": "modified", "before": before[str(MODIFIED)], "after": after[str(MODIFIED)]},
        {"key": str(REMOVED), "kind": "removed", "before": before[str(REMOVED)]},
    ]
    unchanged = {key for key in before.keys() & after.keys() if before[key] == after[key]}
    expect("DuckDB fixture contains only the intended changes",
           (after.keys() - before.keys(), before.keys() - after.keys(), (before.keys() & after.keys()) - unchanged),
           ({str(ADDED)}, {str(REMOVED)}, {str(MODIFIED)}))
    expect("unchanged rows span a complete scan page and both input files", len(unchanged), COMMON_ROWS + 1)
    return {"dataset_id": dataset, "key": KEY, "base": base, "target": target,
            "counts": {"added": 1, "removed": 1, "modified": 1, "unchanged": len(unchanged)},
            "changes": changes, "changes_truncated": False}


def compare(dataset, base, target, key=KEY, *, status=200):
    query = urlencode({
        "key": key,
        "base_branch": base["branch"], "base_version": base["version"], "base_transaction": base["transaction_id"],
        "target_branch": target["branch"], "target_version": target["version"],
        "target_transaction": target["transaction_id"],
    })
    return stack.http("GET", writer.dvs(dataset, f"compare/rows?{query}"), timeout=15, allow=(status,))


def reject(dataset, base, target, code, status, key=KEY):
    actual, reply = compare(dataset, base, target, key, status=status)
    expect("failed comparison HTTP status", actual, status)
    expect("explicit machine-readable rejection", reply["error"].split(":", 1)[0], code)
    expect("no partial diff in an error", {"changes", "counts"} & reply.keys(), set())
    return {"http_status": actual, "error": reply["error"]}


def create_dataset(name):
    rid = writer.id_tail(stack.create_dataset(name))
    stack.ensure_main_branch(rid)
    return rid


def main():
    envdir = Path(os.environ["OF33C_ENVDIR"])
    marker = lambda name: envdir / f"row-match-{name}.json"
    markers = ("ready", "compared", "head", "rechecked")
    expect("raw comparison markers start clean", any(marker(name).exists() for name in markers), False)

    orders = {row[KEY]: row for row in json.loads((writer.WP312 / "orders.json").read_text())["orders"]}
    unchanged = orders[9223372036854775807]
    target_rows = [
        {**orders[MODIFIED], "amount": "12345678901234.5001"},
        {**orders[REMOVED], KEY: ADDED},
        unchanged,
    ]
    shared_rows = [{**unchanged, KEY: key, "note": f"shared order {key}"}
                   for key in range(1000, 1000 + COMMON_ROWS)]
    dataset = create_dataset("wp83-historical-row-match")
    with duckdb.connect() as conn:
        common = publish_parquet(conn, "wp83-match-common", shared_rows)
        target_artifact = publish_parquet(conn, "wp83-match-target", target_rows)
        orders_parquet = publish_parquet(conn, "wp83-match-orders", list(orders.values()))
        empty_parquet = publish_parquet(conn, "wp83-match-empty", list(orders.values()), expected_rows=[],
                                        row_groups=0,
                                        predicate='{"column":"source_time_zone","op":"eq","value":"Etc/UTC"}')
        base, base_files = commit_snapshot(dataset, 1, [("a.parquet", common), ("b.parquet", orders_parquet)])
        target, target_files = commit_snapshot(dataset, 2, [("a.parquet", common), ("b.parquet", target_artifact)])
        files = {"base": base_files, "target": target_files}
        expected = expected_comparison(conn, dataset, base, target, files)

    before_status, before = compare(dataset, base, target)
    expect("actual comparison matches independent DuckDB read of both complete snapshots", before, expected)
    ready_at = history.unix_ms()
    history.atomic_json(marker("ready"), {"expected": expected, "observed_unix_ms": ready_at})

    def wait_capture(name, since):
        def read():
            if not marker(name).is_file():
                return None
            captured = json.loads(marker(name).read_text())["captured_at_unix_ms"]
            expect("browser captured after backend comparison", since <= captured <= history.unix_ms(), True)
            return captured
        return history.wait_for(f"browser row-key comparison {name}", read, time.monotonic() + 180)

    compared_at = wait_capture("compared", ready_at)
    tip, tip_files = commit_snapshot(dataset, 3, [("head.parquet", empty_parquet)])
    after_status, after = compare(dataset, base, target)
    expect("advancing the head preserves both pinned inputs and the row diff", after, before)
    head_at = history.unix_ms()
    history.atomic_json(marker("head"), {"tip": tip, "observed_unix_ms": head_at})
    rechecked_at = wait_capture("rechecked", head_at)

    duplicate_dataset = create_dataset("wp83-historical-row-match-duplicate")
    duplicate, duplicate_files = commit_snapshot(duplicate_dataset, 1, [("a.parquet", common), ("b.parquet", common)])
    duplicate_target, _ = commit_snapshot(duplicate_dataset, 2, [("b.parquet", target_artifact)])
    rejected = {
        "unknown_key": reject(dataset, base, target, "dataset_row_compare_key_invalid", 400, "wrong_key"),
        "transaction_mismatch": reject(dataset, {**base, "transaction_id": target["transaction_id"]},
                                       target, "dataset_row_compare_transaction_mismatch", 422),
        "duplicate": reject(duplicate_dataset, duplicate, duplicate_target, "dataset_row_compare_duplicate_key", 422),
    }
    writer.write_json("row-match", {
        "status": "passed", "base": base, "target": target, "tip": tip, "duplicate": duplicate,
        "files": {**files, "tip": tip_files, "duplicate": duplicate_files},
        "expected": expected, "before": {"http_status": before_status, **before},
        "after": {"http_status": after_status, **after}, "rejections": rejected,
        "service_observed_unix_ms": {"before": ready_at, "after_head": head_at},
        "browser_observed_unix_ms": {"before": compared_at, "after_head": rechecked_at},
    })
    print("PASS historical row matching across Parquet files and pages, new head and rejected inputs", flush=True)


if __name__ == "__main__":
    main()
