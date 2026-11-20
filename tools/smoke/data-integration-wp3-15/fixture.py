"""Deterministic 100k order input and independent DuckDB acceptance."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from statistics import median
from time import perf_counter_ns

import duckdb


ROWS = 100_000
FILES = 4
FORMATS = ("ndjson", "parquet")
ORDER_ID_START = 9_007_199_254_740_993
STORES = 20
COST_BASE = 10_000       # Units of 0.0001
COST_PER_STORE = 1_375
UNITS = Decimal(10_000)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MICROSECOND = timedelta(microseconds=1)
ZONES = (
    ("UTC", timezone.utc),
    ("Asia/Shanghai", timezone(timedelta(hours=8))),
    ("Asia/Kolkata", timezone(timedelta(minutes=330))),
    ("Etc/GMT+5", timezone(timedelta(hours=-5))),
)
SQL_TYPES = [
    ("order_id", "BIGINT"),
    ("amount", "DECIMAL(18,4)"),
    ("order_date", "DATE"),
    ("ordered_at", "TIMESTAMP WITH TIME ZONE"),
    ("source_time_zone", "VARCHAR"),
    ("note", "VARCHAR"),
]
READERS = {
    "ndjson": """read_ndjson(?, columns={
        order_id:'BIGINT', amount:'DECIMAL(18,4)', order_date:'DATE',
        ordered_at:'TIMESTAMPTZ', source_time_zone:'VARCHAR', note:'VARCHAR'
    }, hive_partitioning=false)""",
    "parquet": "read_parquet(?, hive_partitioning=false)",
}


@dataclass
class Golden:
    rows: list
    store_profits: list


def make_input():
    files = []
    expected = []
    sales = [0] * STORES
    counts = [0] * STORES
    for shard in range(FILES):
        part = bytearray()
        start = shard * (ROWS // FILES)
        for i in range(start, start + ROWS // FILES):
            order_id = ORDER_ID_START + i
            scaled = ((i * 1717) % 1_000_019) - (20_000 if i % 13 == 0 else 0)
            if i % 4999 == 0:
                scaled = 900_719_925_474_099_999
            amount = (Decimal(scaled) / UNITS).quantize(Decimal("0.0001"))
            day = date(2024, 1, 1) + timedelta(days=i % 366)
            zone, tz = ZONES[i % len(ZONES)]
            recorded = datetime(day.year, day.month, day.day, i % 24, i % 60, (i // 60) % 60,
                                (i * 7919) % 1_000_000, tzinfo=tz)
            note = None if i % 7 == 0 else f"订单批次 {i % 997:03d}\n渠道 {i % 11}"
            order = {
                "order_id": order_id,
                "amount": str(amount),
                "order_date": day.isoformat(),
                "ordered_at": recorded.isoformat(timespec="microseconds"),
                "source_time_zone": zone,
                "note": note,
            }
            part.extend(json.dumps(order, separators=(",", ":"), ensure_ascii=False).encode() + b"\n")
            expected.append((order_id, amount, day, (recorded - EPOCH) // MICROSECOND, zone, note))
            counts[i % STORES] += 1
            sales[i % STORES] += scaled
        files.append((f"orders/part-{shard:05d}.ndjson", bytes(part)))

    profits = []
    for i, (count, gross) in enumerate(zip(counts, sales, strict=True), start=1):
        cost = count * (COST_BASE + COST_PER_STORE * i)
        profits.append((i, count, Decimal(gross) / UNITS, Decimal(cost) / UNITS, Decimal(gross - cost) / UNITS))
    return files, Golden(expected, profits)


def require(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, found {actual!r}")


def reader_connection():
    require("DuckDB version", duckdb.__version__, "1.4.4")
    return duckdb.connect(config={"threads": 1})


def profit_query(fmt):
    return f"""
        WITH sales AS (
            SELECT ((order_id - {ORDER_ID_START}) % {STORES}) + 1 AS store,
                   amount
            FROM {READERS[fmt]}
        ), priced AS (
            SELECT store, amount,
                   CAST({COST_BASE} + store * {COST_PER_STORE} AS DECIMAL(18,0)) *
                       CAST(0.0001 AS DECIMAL(5,4)) AS cost
            FROM sales
        )
        SELECT store, count(*), sum(amount), sum(cost), sum(amount - cost)
        FROM priced
        GROUP BY store ORDER BY store
    """


def validate_artifact(path, fmt, golden):
    digest = sha256()
    with reader_connection() as conn:
        schema = conn.execute(f"DESCRIBE SELECT * FROM {READERS[fmt]}", [str(path)]).fetchall()
        require("DuckDB column names and SQL types", [(name, kind) for name, kind, *_ in schema], SQL_TYPES)
        footer = None
        if fmt == "parquet":
            footer, = conn.execute("""
                SELECT created_by, num_rows, num_row_groups, format_version
                FROM parquet_file_metadata(?)
            """, [str(path)]).fetchall()
            require("committed Parquet footer", footer, ("parquet-go version 18.6.0", ROWS, (ROWS + 1023) // 1024, 2))
            compression = conn.execute("SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]).fetchall()
            require("committed Parquet compression", compression, [("SNAPPY",)])
        cursor = conn.execute(f"""
            SELECT order_id, amount, order_date, epoch_us(ordered_at), source_time_zone, note
            FROM {READERS[fmt]} ORDER BY order_id
        """, [str(path)])
        count = 0
        while rows := cursor.fetchmany(4096):
            for row in rows:
                if count >= len(golden.rows):
                    raise AssertionError("output included unexpected additional orders")
                require(f"order {count + 1}, all six values", row, golden.rows[count])
                order_id, amount, day, timestamp, zone, note = row
                line = (order_id, str(amount), day.isoformat(), timestamp, zone, note)
                digest.update(json.dumps(line, ensure_ascii=False, separators=(",", ":")).encode() + b"\n")
                count += 1
        require("exact output row count", count, ROWS)
        require("exact store profits", conn.execute(profit_query(fmt), [str(path)]).fetchall(), golden.store_profits)
    return {"row_count": count, "rows_sha256": digest.hexdigest(), "parquet_footer": footer,
            "store_count": len(golden.store_profits)}


def compare_queries(paths_by_fmt, golden):
    samples = {fmt: [] for fmt in FORMATS}
    order = [list(FORMATS if i % 2 == 0 else FORMATS[::-1]) for i in range(10)]
    with reader_connection() as conn:
        version, = conn.execute("SELECT version()").fetchone()
        for fmt in FORMATS:
            require("warm-up store profits",
                    conn.execute(profit_query(fmt), [str(paths_by_fmt[fmt])]).fetchall(), golden.store_profits)
        for pair in order:
            for fmt in pair:
                sql = profit_query(fmt)
                params = [str(paths_by_fmt[fmt])]
                start = perf_counter_ns()
                profits = conn.execute(sql, params).fetchall()
                elapsed_ns = perf_counter_ns() - start
                require("timed store profits", profits, golden.store_profits)
                samples[fmt].append(elapsed_ns / 1_000_000_000)
    return {
        "reader": {"duckdb": version, "threads": 1},
        "warmups_per_format": 1,
        "sample_order": order,
        "samples_seconds": samples,
        "median_seconds": {fmt: median(values) for fmt, values in samples.items()},
        "store_profits": [(store, count, str(gross), str(cost), str(profit))
                          for store, count, gross, cost, profit in golden.store_profits],
    }
