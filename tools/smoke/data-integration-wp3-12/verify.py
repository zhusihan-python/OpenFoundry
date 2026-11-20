"""Read the WP3.12 fixtures with DuckDB and emit verification evidence."""

import argparse
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import platform
import sys

import duckdb

DUCKDB_VERSION = "1.4.4"
SCHEMA_VERSION = "orders.v1"
FIXTURE_COMMIT = "034f432a-f112-441b-8726-8295288f733e"
INPUT_PIN = {
    "dataset_id": "fixture/orders",
    "branch": "main",
    "version": 3,
    "transaction_id": "fe91b669-3da2-49aa-a487-ea8fd2991fc3",
}
SQL_SCHEMA = [
    ("order_id", "BIGINT"),
    ("amount", "DECIMAL(18,4)"),
    ("order_date", "DATE"),
    ("ordered_at", "TIMESTAMP WITH TIME ZONE"),
    ("source_time_zone", "VARCHAR"),
    ("note", "VARCHAR"),
]
PARQUET_SCHEMA = [
    ("order_id", "INT64", None, "REQUIRED", "INT_64", None, None,
     "IntType(bitWidth=@, isSigned=1)"),
    ("amount", "FIXED_LEN_BYTE_ARRAY", "8", "REQUIRED", "DECIMAL", 18, 4,
     "DecimalType(scale=4, precision=18)"),
    ("order_date", "INT32", None, "REQUIRED", "DATE", None, None, "DateType()"),
    ("ordered_at", "INT64", None, "REQUIRED", "TIMESTAMP_MICROS", None, None,
     "TimestampType(isAdjustedToUTC=1, unit=TimeUnit(MILLIS=<null>, MICROS=MicroSeconds(), NANOS=<null>))"),
    ("source_time_zone", "BYTE_ARRAY", None, "REQUIRED", "UTF8", None, None, "StringType()"),
    ("note", "BYTE_ARRAY", None, "OPTIONAL", "UTF8", None, None, "StringType()"),
]
# Independent expectations, sorted by order_id. The source contains local times
# with offsets. These literals specify the corresponding UTC instants.
EXPECTED_ORDERS = [
    (9007199254740993, "12345678901234.5000", "2024-02-29",
     "2024-02-28T18:20:00.123456+00:00", "Asia/Kathmandu", None),
    (9007199254740995, "-7.1000", "2024-11-03",
     "2024-11-03T05:30:00.000001+00:00", "America/New_York", ""),
    (9223372036854775807, "0.0000", "2024-11-03",
     "2024-11-03T06:30:00.000001+00:00", "America/New_York", "held for review"),
]
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class VerificationError(ValueError):
    pass


def expect(label, actual, expected):
    if actual != expected:
        raise VerificationError(f"{label}: expected {expected!r}, got {actual!r}")


def verify_file(conn, path, expected_orders, input_hash):
    def query(sql):
        return conn.execute(sql, [str(path)]).fetchall()

    columns = query("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)")
    sql_schema = [(name, sql_type) for name, sql_type, *_ in columns]
    expect("DuckDB schema", sql_schema, SQL_SCHEMA)

    parquet_schema = query("""
        SELECT name, type, type_length, repetition_type, converted_type, precision, scale, logical_type
        FROM parquet_schema(?) WHERE num_children IS NULL
    """)
    expect("Parquet schema", parquet_schema, PARQUET_SCHEMA)

    created_by, num_rows, row_groups, version = query("""
        SELECT created_by, num_rows, num_row_groups, format_version FROM parquet_file_metadata(?)
    """)[0]
    expect("Parquet footer", (created_by, num_rows, row_groups, version),
           ("parquet-go version 18.6.0", len(expected_orders), int(bool(expected_orders)), 2))
    chunks = query("""
        SELECT row_group_id, column_id, path_in_schema, compression, encodings
        FROM parquet_metadata(?) ORDER BY row_group_id, column_id
    """)
    expected_chunks = [
        (0, index, name, "SNAPPY", "PLAIN, RLE")
        for index, (name, _) in enumerate(sql_schema)
    ] if expected_orders else []
    expect("Parquet column chunks", chunks, expected_chunks)

    metadata = dict(query("SELECT key, value FROM parquet_kv_metadata(?)"))
    expect("embedded schema version", metadata.get(b"openfoundry.fixture.schema_version"),
           SCHEMA_VERSION.encode())
    expect("embedded input hash", metadata.get(b"openfoundry.fixture.input_sha256"),
           input_hash.encode())
    expect("Arrow schema metadata present", bool(metadata.get(b"ARROW:schema")), True)

    # epoch_us preserves the instant and full microsecond precision without
    # requiring a Python timezone adapter to fetch DuckDB TIMESTAMPTZ values.
    rows = query("""
        SELECT order_id, amount, order_date, epoch_us(ordered_at), source_time_zone, note
        FROM read_parquet(?, hive_partitioning = false) ORDER BY order_id
    """)
    expect("scanned row count", len(rows), len(expected_orders))
    reported_rows = []
    for row, expected in zip(rows, expected_orders, strict=True):
        order_id, amount, order_date, epoch_us, source_time_zone, note = row
        normalized = (
            order_id, format(amount, "f"), order_date.isoformat(),
            (UTC_EPOCH + timedelta(microseconds=epoch_us)).isoformat(timespec="microseconds"),
            source_time_zone, note,
        )
        for (name, _), actual, expected_value in zip(sql_schema, normalized, expected, strict=True):
            expect(f"order {expected[0]} column {name}", actual, expected_value)
        # Quoting the large IDs and decimals keeps the evidence JSON exact in JavaScript readers.
        reported_rows.append(dict(zip(
            (name for name, _ in sql_schema), (str(order_id), *normalized[1:]), strict=True,
        )))

    raw = path.read_bytes()
    return {
        "path": str(path),
        "sha256": sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "format": "parquet",
        "format_version": version,
        "schema_version": SCHEMA_VERSION,
        "created_by": created_by,
        "row_count": num_rows,
        "row_groups": row_groups,
        "compression": sorted({chunk[3] for chunk in chunks}),
        "columns": [
            {
                "name": name, "duckdb_type": sql_type, "physical_type": physical,
                "type_length_bytes": int(length) if length else None,
                "nullable": repetition == "OPTIONAL", "logical_type": logical,
                "converted_type": converted, "precision": precision, "scale": scale,
            }
            for (name, sql_type), (_, physical, length, repetition, converted, precision, scale, logical)
            in zip(sql_schema, parquet_schema, strict=True)
        ],
        "rows": reported_rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json", type=Path)
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("--code-revision", required=True, help="Git SHA of the fixture source")
    parser.add_argument("--dirty", action="store_true", help="mark a run using working-tree edits")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    raw_input = args.input_json.read_bytes()
    input_hash = sha256(raw_input).hexdigest()
    input_pin = json.loads(raw_input)["input"]
    expect("input pin", input_pin, INPUT_PIN)
    expect("DuckDB version", duckdb.__version__, DUCKDB_VERSION)
    with duckdb.connect() as conn:
        version, source_id, codename = conn.execute("PRAGMA version").fetchone()
        artifacts = [
            verify_file(conn, args.output_directory / "orders.parquet", EXPECTED_ORDERS, input_hash),
            verify_file(conn, args.output_directory / "orders-empty.parquet", [], input_hash),
        ]

    print(json.dumps({
        "evidence_version": "wp3.12.parquet-fixture.v1",
        "fixture_commit_id": FIXTURE_COMMIT,
        "run_id": args.run_id,
        "code_revision": args.code_revision,
        "source_dirty": args.dirty,
        "input": {**input_pin, "path": str(args.input_json), "sha256": input_hash},
        "reader": {"name": "DuckDB", "version": version, "source_id": source_id,
                   "codename": codename, "python": platform.python_version()},
        "comparison_key": "order_id",
        "artifacts": artifacts,
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except VerificationError as exc:
        sys.exit(str(exc))
