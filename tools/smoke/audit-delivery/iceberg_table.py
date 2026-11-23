"""Local audit-delivery smoke writer and independent Iceberg reader."""

import argparse
import json
import os
import time
from urllib.request import Request, urlopen
from urllib.error import URLError
from urllib.parse import urlsplit

from requests.exceptions import ConnectionError as CatalogConnectionError
from pyiceberg.serializers import FromInputFile
import sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as parquet
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType, UUIDType

TABLE = ("of_audit", "events")
SCHEMA = Schema(
    NestedField(1, "event_id", UUIDType(), required=True),
    NestedField(2, "at", TimestamptzType(), required=True),
    NestedField(3, "correlation_id", StringType(), required=False),
    NestedField(4, "kind", StringType(), required=True),
    NestedField(5, "payload", StringType(), required=True),
)
PARTITION = PartitionSpec(PartitionField(2, 1000, DayTransform(), "at_day"))
ORDER = SortOrder(SortField(2, IdentityTransform(), SortDirection.ASC, NullOrder.NULLS_FIRST))
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
APPEND_PATH = "/openfoundry/iceberg/v1/append"


def catalog(root):
    if "FIXTURE_REST_URL" in os.environ:
        from pyiceberg.catalog.rest import RestCatalog
        return RestCatalog("audit", uri=os.environ["FIXTURE_REST_URL"] + "/catalog",
                           warehouse="audit-fixture", **{
                               "s3.endpoint": s3_url(), "s3.region": "us-east-1",
                               "s3.access-key-id": os.environ["FIXTURE_S3_ACCESS"],
                               "s3.secret-access-key": os.environ["FIXTURE_S3_SECRET"],
                               "header.X-Iceberg-Access-Delegation": "client-managed",
                           })
    return SqlCatalog("smoke", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=(root / "warehouse").as_uri())


def s3_url():
    return "http://127.0.0.1:" + os.environ["FIXTURE_S3_PORT"]


def bootstrap():
    from pyarrow.fs import S3FileSystem
    access, secret = os.environ["FIXTURE_S3_ACCESS"], os.environ["FIXTURE_S3_SECRET"]
    fs = S3FileSystem(access_key=access, secret_key=secret, region="us-east-1",
                      endpoint_override=s3_url(), scheme="http", allow_bucket_creation=True)
    base = os.environ["FIXTURE_REST_URL"]
    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen(base + "/health", timeout=2):
                fs.create_dir("audit-fixture")
            break
        except (URLError, OSError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)

    def post(path, body):
        request = Request(base + "/management/v1/" + path, data=json.dumps(body).encode(),
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=30):
            pass

    post("bootstrap", {"accept-terms-of-use": True})
    post("warehouse", {"warehouse-name": "audit-fixture",
                       "project-id": "00000000-0000-0000-0000-000000000000",
                       "storage-profile": {"type": "s3", "bucket": "audit-fixture", "endpoint": s3_url(),
                                           "region": "us-east-1", "path-style-access": True, "sts-enabled": False},
                       "storage-credential": {"type": "s3", "credential-type": "access-key",
                                              "access-key-id": access, "secret-access-key": secret}})


def batch_rows(request):
    spec = request["spec"]
    if (spec["namespace"], spec["table"]) != TABLE:
        raise LookupError("unknown table")
    fields = [
        {"id": field.field_id, "name": field.name, "type": str(field.field_type), "required": field.required}
        for field in SCHEMA.fields
    ]
    if (spec["catalog"], spec["partition_transform"], spec["sort_order"], spec["schema"]) != (
        "lakekeeper", "day(at)", "at ASC", fields
    ):
        raise ValueError("table contract mismatch")

    rows = request["rows"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("expected nonempty batch")
    for row in rows:
        expected = {"event_id", "at", "kind", "payload"}
        if not isinstance(row, dict) or not expected <= row.keys() <= expected | {"correlation_id"}:
            raise ValueError("invalid audit fields")
        if type(row["at"]) is not int or not all(isinstance(row[k], str) for k in ("event_id", "kind", "payload")):
            raise ValueError("invalid required audit field")
        if row.get("correlation_id") is not None and not isinstance(row["correlation_id"], str):
            raise ValueError("invalid correlation ID")
        json.loads(row["payload"])
        row["event_id"] = UUID(row["event_id"]).bytes
        row["at"] = EPOCH + timedelta(microseconds=row["at"])
    return pa.Table.from_pylist(rows, schema=schema_to_pyarrow(SCHEMA)).sort_by("at")


class EventConflict(Exception):
    """An existing event ID has different audit fields."""


def append_events(table, rows):
    # The smoke owns a small table and serializes all writes in one HTTPServer.
    seen = {row["event_id"]: row for row in table.scan().to_arrow().to_pylist()}
    new_indices = []
    for index, row in enumerate(rows.to_pylist()):
        event_id = row["event_id"]
        if event_id in seen:
            if seen[event_id] != row:
                raise EventConflict
        else:
            seen[event_id] = row
            new_indices.append(index)
    if new_indices:
        table.append(rows.take(new_indices))


def serve(args):
    root = args.root
    root.mkdir(parents=True, exist_ok=True)
    db = catalog(root)
    db.create_namespace_if_not_exists(TABLE[0])
    table = db.create_table_if_not_exists(TABLE, SCHEMA, partition_spec=PARTITION, sort_order=ORDER)

    def record_batch(name, rows):
        marker = root / name
        if not marker.exists():
            pending = marker.with_suffix(".tmp")
            pending.write_text(json.dumps([str(value) for value in rows["event_id"].to_pylist()]))
            pending.replace(marker)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            nonlocal table
            if self.path != APPEND_PATH:
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16 * 1024 * 1024:
                    self.send_error(400, "invalid request length")
                    return
                rows = batch_rows(json.loads(self.rfile.read(length)))
            except KeyError:
                self.send_error(422, "missing required field")
                return
            except LookupError:
                self.send_error(404, "unknown table")
                return
            except (ValueError, TypeError, OverflowError):
                self.send_error(422, "invalid audit batch")
                return
            if (root / "reject-append").exists():
                record_batch("append-rejected", rows)
                self.send_error(503, "append disabled for recovery check")
                return
            try:
                if table is None:
                    table = db.load_table(TABLE)
                append_events(table, rows)
            except EventConflict:
                self.send_error(409, "event ID already has different audit fields")
                return
            except Exception as exc:
                # A failed request can leave the catalog commit outcome unknown.
                table = None
                if isinstance(exc, CatalogConnectionError) and exc.request is not None:
                    request = exc.request
                    url = urlsplit(request.url)
                    if (request.method == "POST" and url.netloc == urlsplit(os.environ.get("FIXTURE_REST_URL", "")).netloc
                            and url.path.endswith("/namespaces/of_audit/tables/events")):
                        record_batch("append-rejected", rows)
                print(f"Iceberg append failed: {type(exc).__name__}", file=sys.stderr, flush=True)
                self.send_error(503, "Iceberg append failed")
                return
            drop = root / "drop-append-response"
            if drop.exists():
                drop.unlink()
                record_batch("append-response-dropped", rows)
                self.close_connection = True
                return
            self.send_response(204)
            self.end_headers()

    with HTTPServer((args.host, args.port), Handler) as server:
        port_file = root / "port.tmp"
        port_file.write_text(str(server.server_port))
        port_file.replace(root / "port")
        server.serve_forever()


def read(root):
    table = catalog(root).load_table(TABLE)
    if table.schema() != SCHEMA or table.spec() != PARTITION or table.sort_order() != ORDER:
        raise ValueError("persisted Iceberg table contract mismatch")
    snapshot = table.current_snapshot()
    if "FIXTURE_REST_URL" in os.environ:
        metadata = FromInputFile.table_metadata(table.io.new_input(table.metadata_location))
        if metadata.current_snapshot_id != snapshot.snapshot_id:
            raise ValueError("S3 metadata and REST snapshot disagree")
    files = sorted(task.file.file_path for task in table.scan().plan_files())
    for path in files:
        with table.io.new_input(path).open() as source:
            event_id_field = parquet.ParquetFile(source).schema.column(0)
            if event_id_field.name != "event_id" or event_id_field.logical_type.type != "UUID":
                raise ValueError("missing Parquet UUID annotation on event_id")
    rows = table.scan().to_arrow().to_pylist()
    for row in rows:
        row["event_id"] = str(row["event_id"])
        row["at"] = (row["at"] - EPOCH) // timedelta(microseconds=1)
        row["payload"] = json.loads(row["payload"])
    print(json.dumps({
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "snapshot_count": len(table.snapshots()),
        "metadata_location": table.metadata_location,
        "data_files": files,
        "rows": rows,
    }))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    server = sub.add_parser("serve")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=0)
    server.add_argument("--root", type=Path, required=True)
    sub.add_parser("read").add_argument("--root", type=Path, required=True)
    sub.add_parser("bootstrap").add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    args.root = args.root.resolve()
    if args.command == "serve":
        serve(args)
    elif args.command == "bootstrap":
        bootstrap()
    else:
        read(args.root)


if __name__ == "__main__":
    main()
