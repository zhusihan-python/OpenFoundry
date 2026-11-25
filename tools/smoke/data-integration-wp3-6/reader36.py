"""WP3.6 independent reader: locate a published file by dataset version, download
it, and read its schema and rows back with DuckDB.

Standalone on purpose. It shares no code and no state with the acceptance
driver and is given only a dataset RID, a version and an output path.
"""

import argparse
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import duckdb

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def call(url):
    """Authenticated GET; returns (status, body)."""
    request = Request(url, headers={"Authorization": "Bearer " + os.environ["WP36_TOKEN"]})
    try:
        with urlopen(request, timeout=30) as response:
            return response.status, response.read()
    except HTTPError as error:
        with error:
            return error.code, error.read()


def locate(dataset, version):
    """Bind the version to its transaction, then find that transaction's file."""
    base = f"{os.environ['WP36_DVS'].rstrip('/')}/v1/datasets/{dataset}"
    status, raw = call(f"{base}/versions?limit=1000")
    if status != 200:
        raise SystemExit(f"the versions of {dataset} are not readable: HTTP {status} {raw[:200]!r}")
    matches = [item for item in json.loads(raw).get("data", []) if item["version"] == version]
    if len(matches) != 1 or not matches[0].get("transaction_id"):
        raise SystemExit(f"version {version} did not bind to exactly one transaction: {matches}")
    status, raw = call(f"{base}/files?branch=main")
    if status != 200:
        raise SystemExit(f"file list of {dataset} is not readable: HTTP {status} {raw[:200]!r}")
    files = [item for item in json.loads(raw)["files"] if item["transaction_id"] == matches[0]["transaction_id"]]
    if len(files) != 1:
        raise SystemExit(f"version {version} should hold exactly one file, found {len(files)}")
    return base, matches[0]["transaction_id"], files[0]


def download(base, file):
    """Follow the authenticated download route to its signed URL and fetch the bytes."""
    request = Request(f"{base}/files/{file['id']}/download",
                      headers={"Authorization": "Bearer " + os.environ["WP36_TOKEN"]})
    try:
        with build_opener(NoRedirect()).open(request, timeout=30):
            raise SystemExit("the file route did not issue a signed redirect")
    except HTTPError as redirect:
        with redirect:
            if redirect.code != 302:
                raise SystemExit(f"the file route answered HTTP {redirect.code}")
            location = redirect.headers["Location"]
    service, signed = urlsplit(os.environ["WP36_DVS"]), urlsplit(location)
    if (signed.scheme, signed.netloc) != (service.scheme, service.netloc) or not signed.path.startswith("/v1/_internal/local-fs/"):
        raise SystemExit(f"unexpected signed download location {location}")
    # The signed URL is fetched on its own: the caller bearer is not sent to it.
    with urlopen(location, timeout=30) as response:
        return response.read()


def inspect(path):
    """Physical columns and exact row values, read by DuckDB."""
    with duckdb.connect() as conn:
        schema = conn.execute(
            "SELECT name, type, type_length, repetition_type, converted_type, precision, scale, logical_type "
            "FROM parquet_schema(?) WHERE num_children IS NULL", [str(path)]).fetchall()
        rows = conn.execute("""
            SELECT order_id, amount, order_date, epoch_us(ordered_at), source_time_zone, note
            FROM read_parquet(?, file_row_number = true, hive_partitioning = false)
            ORDER BY file_row_number
        """, [str(path)]).fetchall()
    values = []
    for order_id, amount, day, micros, zone, note in rows:
        values.append([order_id, amount if isinstance(amount, str) else format(amount, "f"), day.isoformat(),
                       (EPOCH + timedelta(microseconds=micros)).isoformat(timespec="microseconds"), zone, note])
    return [list(column) for column in schema], values


def public_fields(schema):
    """The dataset service's public field list, derived from physical Parquet columns."""
    fields = []
    for name, kind, _length, repetition, converted, precision, scale, logical in schema:
        if kind == "INT64" and converted == "INT_64":
            field_type = "LONG"
        elif kind == "BYTE_ARRAY" and converted == "UTF8":
            field_type = "STRING"
        elif kind == "FIXED_LEN_BYTE_ARRAY" and converted == "DECIMAL":
            field_type = f"DECIMAL({precision},{scale})"
        elif kind == "INT32" and converted == "DATE":
            field_type = "DATE"
        elif kind == "INT64" and converted == "TIMESTAMP_MICROS":
            field_type = "TIMESTAMP(MICROS," + ("UTC" if "isAdjustedToUTC=1" in logical else "LOCAL") + ")"
        else:
            raise ValueError(f"unsupported physical column {name}: {kind} {converted} {logical}")
        fields.append({"name": name, "field_type": field_type, "nullable": repetition == "OPTIONAL"})
    return fields


def content_hash(fields):
    """SHA-256 of the compact JSON field list in column order."""
    ordered = [{key: field[key] for key in ("name", "field_type", "nullable")} for field in fields]
    return sha256(json.dumps(ordered, separators=(",", ":")).encode()).hexdigest()


def describe(path):
    """Everything an independent reader can state about one Parquet file."""
    schema, rows = inspect(path)
    fields = public_fields(schema)
    return {"schema": schema, "fields": fields, "content_hash": content_hash(fields), "rows": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    base, transaction, file = locate(args.dataset, args.version)
    content = download(base, file)
    args.out.write_bytes(content)
    description = describe(args.out)
    print(json.dumps({
        "status": "passed", "dataset": args.dataset, "version": args.version, "transaction_id": transaction,
        "file_id": file["id"], "logical_path": file["logical_path"], "size_bytes": len(content),
        "registered_size_bytes": file["size_bytes"], "sha256": sha256(content).hexdigest(),
        "row_count": len(description["rows"]), **description,
    }, sort_keys=True))


if __name__ == "__main__":
    sys.exit(main())
