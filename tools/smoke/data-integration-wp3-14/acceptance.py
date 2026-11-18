"""Verify committed scans, historical preview and real PBS pinned reads."""

import base64
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlencode

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.with_name("data-integration-wp3-13")))
import acceptance as writer

stack = writer.stack
expect = writer.expect
MIME = writer.MIME
SCHEMA = writer.SCHEMA
COLUMNS = ("order_id", "amount", "order_date", "ordered_at", "source_time_zone", "note")


def evidence(name, data):
    writer.write_json(name, {"status": "passed", **data})
    print(f"PASS {name}", flush=True)


def profile(row):
    order_id = row["order_id"]
    if type(order_id) is not int:
        raise AssertionError("machine scan must use a JSON integer for order_id")
    return (
        order_id, row["amount"], row["order_date"],
        datetime.fromisoformat(row["ordered_at"]).astimezone(UTC).isoformat(timespec="microseconds"),
        row["source_time_zone"], row["note"],
    )


def read_duckdb(conn, ref):
    artifact = writer.EVIDENCE / ref["parquet"]["path"]
    rows = conn.execute("""
       SELECT order_id, amount, order_date, epoch_us(ordered_at), source_time_zone, note
       FROM read_parquet(?, file_row_number = true, hive_partitioning = false)
       ORDER BY file_row_number
    """, [str(artifact)]).fetchall()
    return [
        (order_id, format(amount, "f"), day.isoformat(),
         (writer.reference.UTC_EPOCH + timedelta(microseconds=micros)).isoformat(timespec="microseconds"), zone, note)
        for order_id, amount, day, micros, zone, note in rows
    ]


def commit_files(rid, key, specs):
    status, response = stack.http("POST", writer.dvs(rid, "outputs:commit"), {
        "idempotency_key": key, "branch": "main", "transaction_type": "SNAPSHOT",
        "summary": key, "files": [
            {"logical_path": logical, "operation": "ADD", "size_bytes": len(raw),
             "content_type": mime, "content_base64": base64.b64encode(raw).decode()}
            for logical, raw, mime in specs
        ],
    })
    expect("file transaction committed", response["transaction"]["status"], "COMMITTED")
    expect("file commit HTTP", status, 201)
    txn = response["transaction"]["id"]
    return {"dataset": rid, "version": stack.version_by_txn(rid, txn), "transaction": txn}


def get_page(pin, endpoint, **params):
    query = urlencode({"branch": "main", "version": pin["version"], **params})
    return writer.dvs(pin["dataset"], f"{endpoint}?{query}")


def assert_pin(pin, reply):
    for name, value in (("dataset_id", pin["dataset"]), ("version", pin["version"]),
                        ("transaction_id", pin["transaction"]), ("branch", "main")):
        expect(f"page binding {name}", reply[name], value)


def scan(pin, limit, path=None, predicate=None, after_first=None, data_format="parquet"):
    params = {"limit": limit}
    params["path" if path else "all_files"] = path if path else "true"
    if predicate is not None:
        params["predicate"] = json.dumps(predicate, separators=(",", ":"))
    rows, pages = [], []
    cursors = set()
    for _ in range(80):
        response = writer.get(get_page(pin, "files/rows:scan", **params))
        assert_pin(pin, response)
        expect("file format", response["format"], data_format)
        if data_format == "parquet":
            expect("Parquet schema version", response["schema_version"], SCHEMA)
        values = response["rows"]
        expect("page row count", response["row_count"], len(values))
        if len(values) > limit:
            raise AssertionError("scan exceeded requested page row limit")
        rows.extend(values)
        cursor = response.get("next_cursor")
        pages.append({"count": len(values), "page_bytes": response["page_bytes"], "has_more": cursor is not None})
        if len(pages) == 1 and after_first is not None:
            after_first(cursor)
        if cursor is None:
            return rows, pages
        if not cursor or cursor in cursors:
            raise AssertionError("scan cursor stalled")
        cursors.add(cursor)
        params["cursor"] = cursor
    raise AssertionError("scan exceeded acceptance fixture page count")


def rejections(pinned, latest, old_cursor, parquet):
    rid = stack.create_dataset("wp314-negative-files")
    stack.ensure_main_branch(rid)
    bad = commit_files(rid, "wp314-cut-footer", [("bad.parquet", parquet[:-3], MIME)])
    mixed = commit_files(rid, "wp314-mixed", [
        ("a.parquet", parquet, MIME), ("b.ndjson", b'{"str":"mixed"}\n', "application/x-ndjson"),
    ])
    cases = {
        "truncated": (get_page(bad, "files/rows:scan", all_files="true"), 422),
        "mixed": (get_page(mixed, "files/rows:scan", all_files="true"), 422),
        "cursor_other_version": (get_page(latest, "files/rows:scan", all_files="true", cursor=old_cursor), 400),
        "cursor_malformed": (get_page(pinned, "files/rows:scan", all_files="true", cursor="!invalid"), 400),
        "preview_cursor_malformed": (get_page(pinned, "preview", cursor="!invalid"), 400),
    }
    outcomes = {}
    for name, (url, want) in cases.items():
        status, body = stack.http("GET", url, allow=(want,))
        expect(name, status, want)
        outcomes[name] = {"status": status, "code": body.get("code"), "error": body.get("error")}
    foreign = os.environ["WP314_FOREIGN_TOKEN"]
    owner = stack.TOKEN
    try:
        stack.TOKEN = foreign
        for endpoint in ("files/rows:scan", "preview"):
            params = {"all_files": "true"} if endpoint == "files/rows:scan" else {}
            status, _ = stack.http("GET", get_page(pinned, endpoint, **params), allow=(404,))
            outcomes["foreign_" + endpoint] = {"status": status}
    finally:
        stack.TOKEN = owner
    return outcomes


def checked_preview(pin, limit, **params):
    reply = writer.get(get_page(pin, "preview", limit=limit, **params))
    assert_pin(pin, reply)
    expect("preview returns actual format", reply["format"], "parquet")
    expect("preview schema version", reply["schema_version"], SCHEMA)
    expect("preview typed columns", tuple(column["name"] for column in reply["columns"]), COLUMNS)
    expect("preview row count", reply["row_count"], len(reply["rows"]))
    return reply


def compare_preview(response, expected):
    values = []
    for row in response["rows"]:
        copy = dict(row)
        if type(copy["order_id"]) is not str:
            raise AssertionError("browser-facing order_id must be decimal text")
        copy["order_id"] = int(copy["order_id"])
        values.append(profile(copy))
    expect("preview exact rows", values, expected)


def pinned_input_request(pipe, pin, size=25):
    return stack.http("POST", f"{stack.PBS}/api/v1/pipelines/{pipe}/nodes/source/preview", {
        "sample_size": size, "nodes": [{
            "id": "source", "label": "Pinned orders", "transform_type": "dataset_input",
            "input_dataset_ids": [pin["dataset"]],
            "config": {"source_kind": "dataset", "dataset_id": stack.canon(pin["dataset"]),
                       "dataset_snapshot": {"branch": "main", "version": pin["version"],
                                            "transaction_id": pin["transaction"]}},
        }],
    }, allow=(200,))[1]


def copy_build(pipe, output, expected):
    build, _ = stack.create_build(pipe, output)
    status, response = stack.execute(build)
    expect("real PBS pinned copy HTTP", status, 200)
    expect("real PBS pinned copy build", response["state"], "BUILD_COMPLETED")
    txn = writer.id_tail(response["batched"]["published_transaction_rid"])
    file, = [item for item in writer.file_list(output) if item["transaction_id"] == txn]
    raw = writer.download(output, file)
    expect("pinned copy exact rows", [profile(json.loads(line)) for line in raw.splitlines()], expected)
    digest = sha256(raw).hexdigest()
    expect("pinned copy digest", response["batched"]["sha256"], digest)
    return {"build": build, "transaction": txn, "sha256": digest}


def pbs_replay(pinned, empty, expected):
    out, _, initial = writer.seed_output("wp314-pbs-read-replay")
    pipe = stack.create_pipeline("wp314-copy-old", pinned["dataset"], out)
    stack.publish_spec(pipe, {
        "transform_type": "batched_dataset_copy",
        "input": {"branch": "main", "version": pinned["version"], "all_files": True},
        "output": {"logical_path": "copy.ndjson"},
    }, [{"dataset_rid": stack.canon(pinned["dataset"])}], [stack.canon(out)])
    old_read = pinned_input_request(pipe, pinned)
    first = copy_build(pipe, out, expected)
    new_tip = commit_files(pinned["dataset"], "wp314-changed-pbs-tip", [
        ("current.ndjson", b'{"order_id":-555555}\n', "application/x-ndjson"),
    ])
    expect("PBS source new head", new_tip["version"], pinned["version"] + 1)
    replay = copy_build(pipe, out, expected)
    expect("same bytes after source head update", first["sha256"], replay["sha256"])
    if first["transaction"] == replay["transaction"]:
        raise AssertionError("fresh builds must publish distinct output transactions")
    expect("copy executions each published one version", len(stack.list_versions(out)), len(initial["versions"]) + 2)
    for binding, read, want in (
        (pinned, old_read, expected),
        (pinned, pinned_input_request(pipe, pinned), expected),
        (empty, pinned_input_request(pipe, empty), []),
    ):
        expect("PBS preview file format", read["dataset_file_format"], "parquet")
        expect("PBS preview schema version", read["dataset_schema_version"], SCHEMA)
        expect("PBS preview columns", tuple(read["columns"]), COLUMNS)
        expect(f"PBS real pinned input rows at version {binding['version']}", [profile(row) for row in read["rows"]], want)
    return {"pin": pinned, "new_head": new_tip, "before": first, "after": replay, "exact_rows": len(expected),
            "typed_empty": empty}


def pin(ref):
    info = ref["version"]
    return {"dataset": ref["output_dataset"], "version": info["version"], "transaction": info["transaction_id"]}


def main():
    expect("DuckDB package version", duckdb.__version__, writer.reference.DUCKDB_VERSION)
    orders = json.loads((writer.WP312 / "orders.json").read_text())["orders"]
    extra = [{**orders[0], "order_id": index, "note": f"page row {index}"} for index in range(1, 1023)]
    main_expected, small_expected = list(map(profile, orders + extra)), list(map(profile, orders))
    source_main = writer.seed_input("wp314-large", [("orders.ndjson", orders + extra)])
    source_small = writer.seed_input("wp314-small", [("orders.ndjson", orders)])
    with duckdb.connect() as conn:
        large = writer.publish_case(conn, "wp314-large", source_main, sorted(main_expected), 2)
        small = writer.publish_case(conn, "wp314-small", source_small, sorted(small_expected), 1)
        empty = writer.publish_case(conn, "wp314-empty", source_small, [], 0,
                                    '{"column":"source_time_zone","op":"eq","value":"Etc/UTC"}')
        main_values, small_values = read_duckdb(conn, large), read_duckdb(conn, small)
        expect("independent DuckDB large order", main_values, main_expected)
        expect("independent DuckDB small order", small_values, small_expected)
    main_bytes, small_bytes = ((writer.EVIDENCE / item["parquet"]["path"]).read_bytes() for item in (large, small))
    rid = stack.create_dataset("wp314-order-history")
    stack.ensure_main_branch(rid)
    pinned = commit_files(rid, "wp314-two-parquet-parts", [
        ("b.parquet", small_bytes, MIME), ("a.parquet", main_bytes, MIME),
    ])
    changed = {}

    def new_head(cursor):
        if cursor is None:
            raise AssertionError("initial Parquet page has no continuation")
        changed["cursor"] = cursor
        changed["new"] = commit_files(rid, "wp314-replace-branch-head", [
            ("a.ndjson", b'{"order_id":-555555,"latest":true}\n', "application/x-ndjson"),
        ])
        expect("new version published", changed["new"]["version"], pinned["version"] + 1)

    values, pages = scan(pinned, 131, after_first=new_head)
    expected = main_values + small_values
    expect("real committed scan across row groups and files after head changes", list(map(profile, values)), expected)
    single, _ = scan(pinned, 151, path="a.parquet")
    expect("explicit Parquet file scan", list(map(profile, single)), main_values)
    only, _ = scan(pinned, 113, path="a.parquet",
                   predicate={"column": "order_id", "op": "eq", "value": 9007199254740993})
    expect("exact int64 predicate", list(map(profile, only)),
           [row for row in main_values if row[0] == 9007199254740993])
    new_values, _ = scan(changed["new"], 100, data_format="ndjson")
    expect("new head remains NDJSON", new_values, [{"order_id": -555555, "latest": True}])
    evidence("committed-pages", {"pinned": pinned, "new_head": changed["new"], "pages": pages,
                                "rows": len(values), "parquet_sizes": [len(main_bytes), len(small_bytes)],
                                "exact_values": [str(item[0]) for item in main_values[:3]]})

    first = checked_preview(pinned, 100)
    compare_preview(first, expected[:100])
    second = checked_preview(pinned, 100, cursor=first["next_cursor"])
    compare_preview(second, expected[100:200])
    empty_pin = pin(empty)
    empty_rows, empty_pages = scan(empty_pin, 100)
    expect("typed empty scan", (empty_rows, len(empty_pages)), ([], 1))
    preview = checked_preview(empty_pin, 100)
    expect("schema-bearing empty preview", (preview["rows"], preview["next_cursor"]), ([], None))
    evidence("preview-and-empty", {"pinned": pinned, "empty": empty_pin,
                                   "first_order": first["rows"][0]["order_id"],
                                   "next_order": second["rows"][0]["order_id"], "columns": COLUMNS})
    evidence("rejections", {"cases": rejections(pinned, changed["new"], changed["cursor"], small_bytes)})
    evidence("pbs-replay", pbs_replay(pin(small), empty_pin, small_values))
    writer.write_json("browser-fixture", {"old": pinned, "new_head": changed["new"], "empty": empty_pin,
                                        "first_order": first["rows"][0]["order_id"],
                                        "next_order": second["rows"][0]["order_id"], "columns": COLUMNS})


if __name__ == "__main__":
    main()
