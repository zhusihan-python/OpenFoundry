# WP3.12 Parquet order fixture

The fixture gives WP3.5 (artifact manifest) and WP3.6 (schema versions) concrete Parquet files and a reproducible verification record. The Go writer and standalone DuckDB reader run without OpenFoundry services. Dataset, transaction and commit identifiers belong to the synthetic fixture.

## Writer profile

The writer uses `pqarrow.FileWriter` from the repository's Apache Arrow Go v18.6.0 dependency. It selects Arrow's Parquet writer compatibility setting 2.6, data pages v1, Snappy compression and a row-group limit of 1,024 records. The fixture disables dictionary encoding and decimal-as-integer storage. It uses a UTC-adjusted `TIMESTAMP(MICROS)` annotation on `INT64`, with INT96 disabled. Input timestamps must align to microseconds. The writer stores serialized Arrow schema metadata and adds the fixture schema version and input JSON SHA-256 to each Parquet footer. The footer version is the integer `2` for the 2.6 writer profile. [Apache Arrow Go v18.6.0 Parquet source](https://github.com/apache/arrow-go/tree/v18.6.0/parquet)

| Column | Arrow | Parquet physical and logical type | DuckDB |
| --- | --- | --- | --- |
| `order_id` | Int64 | INT64, signed 64-bit integer | BIGINT |
| `amount` | Decimal128(18,4) | FIXED_LEN_BYTE_ARRAY(8), DECIMAL(18,4) | DECIMAL(18,4) |
| `order_date` | Date32 | INT32, DATE | DATE |
| `ordered_at` | Timestamp(us, UTC) | INT64, TIMESTAMP(MICROS, adjusted to UTC) | TIMESTAMP WITH TIME ZONE |
| `source_time_zone` | String | BYTE_ARRAY, STRING | VARCHAR |
| `note` | Nullable String | Optional BYTE_ARRAY, STRING | VARCHAR |

The amount has four fractional digits and at most 18 digits overall. The writer parses it as a scaled integer to retain its full precision. The order date is the source business date. The timestamp stores an instant, and the separate string preserves the source IANA zone. The two New York rows use the repeated 01:30 local time with offsets an hour apart. The Kathmandu instant falls on the previous UTC day. [DuckDB 1.4 timestamp types](https://duckdb.org/docs/1.4/sql/data_types/timestamp)

| Order ID | Amount | Order date | Ordered at (UTC) | Source zone | Note |
| --- | --- | --- | --- | --- | --- |
| 9007199254740993 | 12345678901234.5000 | 2024-02-29 | 2024-02-28 18:20:00.123456 | Asia/Kathmandu | NULL |
| 9007199254740995 | -7.1000 | 2024-11-03 | 2024-11-03 05:30:00.000001 | America/New_York | Empty string |
| 9223372036854775807 | 0.0000 | 2024-11-03 | 2024-11-03 06:30:00.000001 | America/New_York | held for review |

The verifier uses `ORDER BY order_id`. It checks the ordered SQL and Parquet schemas, required and optional fields, footer counts, Snappy chunks, embedded metadata and each row value. It reads timestamps through `epoch_us` to compare instants at full microsecond precision. The empty projection carries the identical schema and pins with zero rows and zero row groups. [DuckDB 1.4 Parquet metadata](https://duckdb.org/docs/1.4/data/parquet/metadata) · [DuckDB 1.4 timestamp functions](https://duckdb.org/docs/1.4/sql/functions/timestamptz#epoch-ustimestamptz)

## Reproduce

Run from the repository root with the Go toolchain from `go.mod` and Python 3.10 or later.

```sh
set -eu
fixture=tools/smoke/data-integration-wp3-12
work=.openfoundry/wp3-12
output="$work/positive"

python3 -m venv "$work/duckdb-venv"
reader="$work/duckdb-venv/bin/python"
"$reader" -m pip install 'duckdb==1.4.4'
go version
go run "./$fixture" "$fixture/orders.json" "$output"

revision="$(git rev-parse HEAD)"
run_id="wp3-12-$(date -u +%Y%m%dT%H%M%SZ)"
set --  # Add --dirty to the evidence when running from a modified worktree.
if [ -n "$(git status --porcelain)" ]; then
  set -- --dirty
fi
"$reader" "$fixture/verify.py" "$fixture/orders.json" "$output" \
  --code-revision "$revision" --run-id "$run_id" "$@" > "$output/evidence.json"
```

`evidence.json` binds the artifact paths and SHA-256 hashes to the ordered columns, schema version `orders.v1`, pinned input dataset `fixture/orders` on `main` at version 3 and its transaction UUID, fixture commit UUID, run ID and tested Git revision. Each file embeds the input hash and schema version that the reader independently checks. Order IDs and amounts are strings in the evidence JSON so readers can retain their exact digits. The record is a fixture for WP3.5 and WP3.6.

## Field-value negative control

Continue with the variables above. Change one amount by one scale unit and regenerate through the Go writer. DuckDB must reject the amount after the schema, row count and embedded hash of the mutated input have passed verification.

```sh
negative="$work/negative"
mkdir -p "$negative"
"$reader" - "$fixture/orders.json" "$negative/orders.json" <<'PY'
import json
from pathlib import Path
import sys

source = json.loads(Path(sys.argv[1]).read_bytes())
order = next(order for order in source["orders"] if order["order_id"] == 9007199254740995)
order["amount"] = "-7.1001"
Path(sys.argv[2]).write_text(json.dumps(source), encoding="utf-8")
PY
go run "./$fixture" "$negative/orders.json" "$negative"
verify_exit=0
"$reader" "$fixture/verify.py" "$negative/orders.json" "$negative" \
  --code-revision "$revision" --run-id "$run_id-negative" "$@" \
  > "$negative/evidence.json" 2> "$negative/rejection.txt" || verify_exit=$?
test "$verify_exit" -eq 1
test ! -s "$negative/evidence.json"
grep -Fx "order 9007199254740995 column amount: expected '-7.1000', got '-7.1001'" \
  "$negative/rejection.txt"
```
