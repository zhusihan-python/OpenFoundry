"""Durable checkpoint audit appends against a configured Iceberg REST catalog."""
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from functools import cache
from http import HTTPStatus
from uuid import UUID

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import CommitFailedException, NoSuchNamespaceError, NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, In
from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow
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
FIELDS = [dict(id=f.field_id, name=f.name, type=str(f.field_type), required=f.required) for f in SCHEMA.fields]
REQUIRED = {"event_id", "at", "kind", "payload"}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 100_000
MAX_ATTEMPTS = 5
APPEND_PATH = "/openfoundry/iceberg/v1/append"


class Conflict(ValueError):
    pass


class WrongTable(ValueError):
    pass


def reject_nonfinite(_):
    raise ValueError("invalid JSON number")


def decode_batch(body, warehouse):
    if not isinstance(body, dict) or not isinstance(body.get("spec"), dict):
        raise ValueError("expected table specification")
    spec = body["spec"]
    if (spec.get("namespace"), spec.get("table")) != TABLE:
        raise WrongTable("unknown table")
    if (spec.get("warehouse", warehouse) != warehouse or spec.get("catalog") != "lakekeeper"
            or spec.get("partition_transform") != "day(at)" or spec.get("sort_order") != "at ASC"
            or spec.get("schema") != FIELDS
            or any(type(f["id"]) is not int or type(f["required"]) is not bool for f in spec["schema"])):
        raise ValueError("unsupported table specification")
    rows = body.get("rows")
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise ValueError("invalid batch size")
    events = {}
    for row in rows:
        if (not isinstance(row, dict) or not REQUIRED <= row.keys() <= REQUIRED | {"correlation_id"}
                or not all(isinstance(row[k], str) for k in ("event_id", "kind", "payload"))
                or type(row["at"]) is not int
                or row.get("correlation_id") is not None and not isinstance(row["correlation_id"], str)):
            raise ValueError("invalid audit fields")
        key = UUID(row["event_id"])
        json.loads(row["payload"], parse_constant=reject_nonfinite)
        for name in ("kind", "payload", "correlation_id"):
            if row.get(name) is not None:
                row[name].encode("utf-8")
        try:
            at = EPOCH + timedelta(microseconds=row["at"])
        except OverflowError as exc:
            raise ValueError("timestamp out of range") from exc
        event = dict(row, event_id=key, at=at, correlation_id=row.get("correlation_id"))
        if key in events and events[key] != event:
            raise Conflict("conflicting event ID in batch")
        events[key] = event
    return events


@cache
def catalog():
    # Reuse each sync worker's HTTP session. The table itself is loaded on every attempt.
    props = {"header.X-Iceberg-Access-Delegation": "client-managed"}
    for variable, key in (("S3_ENDPOINT", "s3.endpoint"), ("AWS_REGION", "s3.region"),
                          ("AWS_ACCESS_KEY_ID", "s3.access-key-id"), ("AWS_SECRET_ACCESS_KEY", "s3.secret-access-key"),
                          ("AWS_SESSION_TOKEN", "s3.session-token"), ("CATALOG_TOKEN", "token")):
        if value := os.environ.get(variable):
            props[key] = value
    return RestCatalog("audit", uri=os.environ["CATALOG_URI"], warehouse=os.environ["ICEBERG_WAREHOUSE"], **props)


def check_layout(table):
    if table.schema() != SCHEMA or table.spec() != PARTITION or table.sort_order() != ORDER:
        raise Conflict("incompatible audit table layout")


def missing_events(table, events):
    # Iceberg prunes files by UUID bounds. Arrow 21 has no equality or is_in
    # kernel for its UUID extension type, so filter candidate batches on the
    # underlying fixed-size binary values. Keep Iceberg's normal delete handling.
    files = table.scan(row_filter=In("event_id", events.keys())).plan_files()
    scan = ArrowScan(table.metadata, table.io, table.schema(), AlwaysTrue())
    keys = pa.array([k.bytes for k in events], type=pa.binary(16))
    seen = set()
    for chunk in scan.to_record_batches(files):
        ids = chunk.column("event_id").storage
        for row in chunk.filter(pc.is_in(ids, value_set=keys)).to_pylist():
            key = row["event_id"]
            if key in seen or row != events[key]:
                raise Conflict("event ID conflicts with committed audit data")
            seen.add(key)
    return [dict(row, event_id=key.bytes) for key, row in events.items() if key not in seen]


def append(events, db):
    for attempt in range(MAX_ATTEMPTS):
        table = db.load_table(TABLE)
        check_layout(table)
        pending = missing_events(table, events)
        if not pending:
            return
        data = pa.Table.from_pylist(pending, schema=schema_to_pyarrow(SCHEMA)).sort_by("at")
        try:
            # PyIceberg 0.10.0 appends against the main ref of this loaded
            # table, including the no-snapshot case. A losing writer must
            # reload AND recheck event IDs before trying another append.
            table.append(data)
            return
        except CommitFailedException:
            if attempt + 1 == MAX_ATTEMPTS:
                raise
            time.sleep(random.uniform(0.01, 0.04) * (2 ** attempt))


def handle(env):
    path = env["PATH_INFO"]
    if env["REQUEST_METHOD"] == "GET" and path == "/healthz":
        return HTTPStatus.OK, b"ok"
    if path != APPEND_PATH:
        return HTTPStatus.NOT_FOUND, b"unknown route"
    if env["REQUEST_METHOD"] != "POST":
        return HTTPStatus.METHOD_NOT_ALLOWED, b"POST required"
    if env.get("CONTENT_TYPE", "").split(";", 1)[0].strip().lower() != "application/json":
        return HTTPStatus.UNSUPPORTED_MEDIA_TYPE, b"expected application/json"
    length = env.get("CONTENT_LENGTH", "")
    if not length:
        return HTTPStatus.LENGTH_REQUIRED, b"Content-Length required"
    if not length.isdecimal():
        return HTTPStatus.BAD_REQUEST, b"invalid length"
    if int(length) > MAX_BYTES:
        return HTTPStatus.REQUEST_ENTITY_TOO_LARGE, b"batch too large"
    raw = env["wsgi.input"].read(int(length))
    if len(raw) != int(length):
        return HTTPStatus.BAD_REQUEST, b"incomplete batch"
    try:
        body = json.loads(raw, parse_constant=reject_nonfinite)
    except (ValueError, RecursionError):
        return HTTPStatus.BAD_REQUEST, b"invalid JSON"
    try:
        events = decode_batch(body, os.environ["ICEBERG_WAREHOUSE"])
    except WrongTable:
        return HTTPStatus.NOT_FOUND, b"unknown table"
    except Conflict:
        return HTTPStatus.CONFLICT, b"conflicting event ID"
    except (ValueError, RecursionError):
        return HTTPStatus.UNPROCESSABLE_ENTITY, b"invalid audit batch"
    try:
        append(events, catalog())
    except (NoSuchTableError, NoSuchNamespaceError):
        return HTTPStatus.NOT_FOUND, b"audit table missing"
    except Conflict:
        return HTTPStatus.CONFLICT, b"conflicting audit data or layout"
    except Exception as exc:
        # Storage errors can include URLs, credentials or rows. Expose only
        # the class. Unknown commit outcomes can be safely retried by the sink.
        logging.error("audit append unavailable (%s)", type(exc).__name__)
        return HTTPStatus.SERVICE_UNAVAILABLE, b"audit append unavailable"
    return HTTPStatus.NO_CONTENT, b""


def app(env, start_response):
    status, body = handle(env)
    headers = [] if status == HTTPStatus.NO_CONTENT else [
        ("Content-Type", "text/plain"), ("Content-Length", str(len(body)))]
    start_response(f"{status.value} {status.phrase}", headers)
    return [body]


if __name__ == "__main__":
    if sys.argv[1:] == ["create-table"]:
        try:
            db = catalog()
            db.create_namespace_if_not_exists(TABLE[0])
            check_layout(db.create_table_if_not_exists(TABLE, SCHEMA, partition_spec=PARTITION, sort_order=ORDER))
        except Exception as exc:
            sys.exit(f"audit table initialization failed ({type(exc).__name__})")
    elif len(sys.argv) == 1:
        for required in ("CATALOG_URI", "ICEBERG_WAREHOUSE"):
            if not os.environ.get(required):
                sys.exit(f"{required} must be configured")
        os.execv(sys.executable, [sys.executable, "-m", "gunicorn", "--bind", "0.0.0.0:" + os.getenv("PORT", "8080"),
                                  "--no-control-socket", "--timeout", "45", "writer:app"])
    else:
        sys.exit("usage: writer.py [create-table]")
