"""Local audit-delivery smoke writer and independent Iceberg reader."""

import argparse
import json
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
    return SqlCatalog("smoke", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=(root / "warehouse").as_uri())


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


def serve(args):
    root = args.root
    root.mkdir(parents=True, exist_ok=True)
    db = catalog(root)
    db.create_namespace_if_not_exists(TABLE[0])
    table = db.create_table_if_not_exists(TABLE, SCHEMA, partition_spec=PARTITION, sort_order=ORDER)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
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
                (root / "append-rejected").touch()
                self.send_error(503, "append disabled for recovery check")
                return
            try:
                table.append(rows)
            except Exception as exc:
                print(f"Iceberg append failed: {type(exc).__name__}", file=sys.stderr, flush=True)
                self.send_error(503, "Iceberg append failed")
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
    args = parser.parse_args()
    args.root = args.root.resolve()
    if args.command == "serve":
        serve(args)
    else:
        read(args.root)


if __name__ == "__main__":
    main()
