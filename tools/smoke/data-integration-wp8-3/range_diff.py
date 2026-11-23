"""Verify every page of a real Parquet Range Diff against DuckDB."""

from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import time
from urllib.parse import urlencode

import duckdb


HERE = Path(__file__).resolve().parent


def load_driver(name):
    spec = spec_from_file_location(name, HERE / f"{name}.py")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


row_match = load_driver("row_match")
history = row_match.history
writer, stack, expect = history.writer, history.stack, history.expect
KEY = row_match.KEY
LIMIT = 50
ROW_COUNT = 1300
KIND_COUNT = 60
BROWSER_TIMEOUT_SECONDS = 180


def fixtures():
    prototype = next(row for row in json.loads((writer.WP312 / "orders.json").read_text())["orders"]
                     if row[KEY] == row_match.MODIFIED)
    ids = list(range(10000, 10000 + ROW_COUNT - 1)) + [row_match.MODIFIED]
    base = [{**prototype, KEY: key, "note": f"range order {key}"} for key in ids]
    target = [
        {**row, "amount": "12345678901234.5001"} if offset < 2 * KIND_COUNT - 1 or row[KEY] == row_match.MODIFIED else row
        for offset, row in enumerate(base[KIND_COUNT:], KIND_COUNT)
    ]
    new_ids = list(range(20000, 20000 + KIND_COUNT - 1)) + [row_match.ADDED]
    target.extend({**prototype, KEY: key, "note": f"new range order {key}"} for key in new_ids)
    expect("both fixture sides have their requested row count", (len(base), len(target)), (ROW_COUNT, ROW_COUNT))
    # Each snapshot contains two interleaved key ranges. Target rows also
    # switch file positions so sorting or pairing only within a file fails.
    return (base[::2], base[1::2]), (target[1::2][::-1], target[::2][::-1])


def create_fixture(conn):
    dataset = row_match.create_dataset("wp83-historical-range-diff")
    pins, files, by_side = {}, {}, {}
    for version, (side, parts) in enumerate(zip(("base", "target"), fixtures(), strict=True), start=1):
        artifacts = [
            (logical, row_match.publish_parquet(conn, f"wp83-range-{side}-{ordinal}", rows))
            for ordinal, (logical, rows) in enumerate(zip(("a.parquet", "b.parquet"), parts, strict=True), start=1)
        ]
        pins[side], files[side] = row_match.commit_snapshot(dataset, version, artifacts)
        by_side[side] = row_match.read_rows(conn, files[side])
        expect(f"{side} oracle covers the two committed files", len(by_side[side]), ROW_COUNT)

    base_rows, target_rows = by_side["base"], by_side["target"]
    counts = dict.fromkeys(("added", "removed", "modified", "unchanged"), 0)
    changes = []
    for key in sorted(base_rows.keys() | target_rows.keys()):
        before, after = base_rows.get(key), target_rows.get(key)
        if before is None:
            kind = "added"
        elif after is None:
            kind = "removed"
        elif before != after:
            kind = "modified"
        else:
            counts["unchanged"] += 1
            continue
        counts[kind] += 1
        detail = {"key": key, "kind": kind}
        if before is not None:
            detail["before"] = before
        if after is not None:
            detail["after"] = after
        changes.append(detail)
    expect("independent fixture classification", counts,
           {"added": KIND_COUNT, "removed": KIND_COUNT, "modified": KIND_COUNT,
            "unchanged": ROW_COUNT - 2 * KIND_COUNT})
    result = {"dataset_id": dataset, "key": KEY, **pins, "counts": counts, "changes": changes,
              "changes_truncated": False}
    return result, files


def request_page(expected, *, cursor=None, limit=LIMIT, status=200, **overrides):
    query = {
        "key": expected["key"],
        **{f"{side}_{wire}": expected[side][field]
           for side in ("base", "target")
           for wire, field in (("branch", "branch"), ("version", "version"), ("transaction", "transaction_id"))},
        "limit": limit,
    }
    if cursor is not None:
        query["cursor"] = cursor
    query.update(overrides)
    start = history.unix_ms()
    actual, reply = stack.http("GET", writer.dvs(expected["dataset_id"], f"compare/rows?{urlencode(query)}"),
                               timeout=15, allow=(status,))
    expect("range comparison HTTP status", actual, status)
    return {"status": actual, "started_unix_ms": start, "completed_unix_ms": history.unix_ms(),
            "query": query, "body": reply}


def check_page(expected, page, offset, seen):
    reply = page["body"]
    changes = expected["changes"]
    next_offset = offset + min(LIMIT, len(changes) - offset)
    want = {**expected, "changes": changes[offset:next_offset]}
    expect("page payload matches the complete independent oracle at its offset",
           {key: value for key, value in reply.items() if key != "next_cursor"}, want)
    cursor = reply.get("next_cursor")
    expect("cursor presence matches whether any oracle changes remain", bool(cursor), next_offset < len(changes))
    if cursor:
        expect("cursor advances exactly once", cursor in seen, False)
        seen.add(cursor)
    else:
        expect("terminal response omits its continuation", "next_cursor" in reply, False)
    return cursor


def traverse(expected, initial=None):
    cursor, seen, pages = None, set(), []
    for offset in range(0, len(expected["changes"]), LIMIT):
        page = initial if offset == 0 and initial else request_page(expected, cursor=cursor)
        cursor = check_page(expected, page, offset, seen)
        pages.append(page)
    expect("whole traversal ends at oracle cardinality",
           [row for page in pages for row in page["body"]["changes"]], expected["changes"])
    return pages


def rejection(expected, code, **query):
    page = request_page(expected, status=400, **query)
    reply = page["body"]
    expect("rejected request exposes an error without partial comparison", set(reply), {"error"})
    expect("explicit cursor rejection", reply["error"].split(":", 1)[0], code)
    return page


def process_snapshot(container):
    name = f"{stack.PROJECT}-dvs"
    identity, start, restarts, oom = stack.docker(
        "inspect", "--format", "{{.Id}}|{{.State.StartedAt}}|{{.RestartCount}}|{{.State.OOMKilled}}", container,
    ).stdout.strip().split("|")
    expect("memory belongs to the owned DVS container ID", identity, container)
    expect("memory belongs to the service executable", stack.docker("exec", name, "readlink", "/proc/1/exe").stdout.strip(),
           "/service")
    expect("measured service did not run out of memory", oom, "false")
    proc = stack.proc_snapshot("dvs")
    expect("service is PID 1", proc.get("pid"), "1")
    expect("valid resident memory and PID start ticks",
           all(proc.get(key, 0) > 0 for key in ("vmrss_kib", "vmhwm_kib", "starttime_ticks")), True)
    return {**proc, "container_id": identity, "started_at": start, "restart_count": int(restarts), "oom_killed": False}


def measure_traversal(expected):
    # Prior fixture traffic and browser activity have ended. Restart only the
    # checked, disposable DVS container after fixture publication to separate
    # its process high-water mark from staging and the earlier smoke cases.
    container = history.owned_dvs()
    prior = process_snapshot(container)
    stack.docker("restart", "--time", "5", container)
    writer.wait_for_health(stack.DVS)
    before = process_snapshot(container)
    expect("isolated DVS restart started a fresh PID1 process",
           before["started_at"] != prior["started_at"] and before["starttime_ticks"] != prior["starttime_ticks"], True)
    started_at = history.unix_ms()
    start_ns = time.perf_counter_ns()
    pages = traverse(expected)
    duration_ns = time.perf_counter_ns() - start_ns
    completed_at = history.unix_ms()
    after = process_snapshot(container)
    for key in ("container_id", "pid", "started_at", "starttime_ticks", "restart_count"):
        expect(f"{key} remained stable across the timed comparison", before[key], after[key])
    expect("one-process VmHWM cannot fall", before["vmhwm_kib"] <= after["vmhwm_kib"], True)
    return {
        "scope": "DVS PID 1 process high-water since isolated restart immediately before a full Range Diff traversal",
        "prior": prior, "before": before, "after": after, "duration_ns": duration_ns,
        "started_unix_ms": started_at, "completed_unix_ms": completed_at, "page_count": len(pages),
        "payload_sha256": sha256(json.dumps(
            [row for page in pages for row in page["body"]["changes"]], sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest(),
    }


def main():
    envdir = Path(os.environ["OF33C_ENVDIR"])

    def marker(name):
        return envdir / f"range-diff-{name}.json"

    def capture(name, since):
        def read():
            if not marker(name).is_file():
                return None
            at = json.loads(marker(name).read_text())["captured_at_unix_ms"]
            expect("browser marker follows its service checkpoint", since <= at <= history.unix_ms(), True)
            return at
        return history.wait_for(f"browser range diff {name}", read, time.monotonic() + BROWSER_TIMEOUT_SECONDS)

    marks = ("ready", "first", "head", "traversed")
    expect("range diff marker paths are fresh", any(marker(name).exists() for name in marks), False)
    # Wait for the existing browser cases before publishing these fixtures.
    history.wait_for("existing WP8.3 row match driver to finish", lambda: (writer.EVIDENCE / "row-match.json").is_file(),
                     time.monotonic() + BROWSER_TIMEOUT_SECONDS)
    with duckdb.connect() as conn:
        expected, files = create_fixture(conn)
        empty_parquet = row_match.publish_parquet(
            conn, "wp83-range-empty", list(json.loads((writer.WP312 / "orders.json").read_text())["orders"]),
            expected_rows=[], row_groups=0,
            predicate='{"column":"source_time_zone","op":"eq","value":"Etc/UTC"}')
    initial = request_page(expected)
    first_cursor = check_page(expected, initial, 0, set())
    ready_at = history.unix_ms()
    history.atomic_json(marker("ready"), {"expected": expected, "limit": LIMIT, "observed_unix_ms": ready_at})
    first_capture = capture("first", ready_at)

    tip, files["tip"] = row_match.commit_snapshot(
        expected["dataset_id"], 3, [("head.parquet", empty_parquet)],
    )
    head_at = history.unix_ms()
    history.atomic_json(marker("head"), {"tip": tip, "observed_unix_ms": head_at})
    pages = traverse(expected, initial)
    rejected = {
        "malformed": rejection(expected, "dataset_row_compare_cursor_invalid", cursor="not/a/valid/cursor"),
        "key": rejection(expected, "dataset_row_compare_cursor_mismatch", cursor=first_cursor, key="amount"),
        "limit": rejection(expected, "dataset_row_compare_cursor_mismatch", cursor=first_cursor, limit=LIMIT - 1),
        "version": rejection(expected, "dataset_row_compare_cursor_mismatch", cursor=first_cursor,
                             target_version=tip["version"], target_transaction=tip["transaction_id"]),
    }
    last_capture = capture("traversed", head_at)
    memory = measure_traversal(expected)
    writer.write_json("range-diff", {
        "status": "passed", "expected": expected, "tip": tip, "files": files, "limit": LIMIT, "rows_per_side": ROW_COUNT,
        "pages": pages, "rejections": rejected, "memory": memory, "ready_unix_ms": ready_at, "head_unix_ms": head_at,
        "browser_first_unix_ms": first_capture, "browser_traversed_unix_ms": last_capture,
    })
    print("PASS historical Range Diff beyond 1024 rows, exact multi-file pages, cursor errors, and DVS peak memory",
          flush=True)


if __name__ == "__main__":
    main()
