# WP3.6 pinned input schema smoke

From the repository root on an arm64 host with Docker Compose and a Python 3.10+ environment containing DuckDB 1.4.4, run against a clean checkout:

```sh
WP36_DUCKDB_PYTHON=/absolute/path/to/python \
  tools/smoke/data-integration-wp3-6/run36.sh unique-run-id
```

The suite uses the WP3.3 Go build, PostgreSQL 16, PBS and DVS Compose stack, signed local filesystem reads and the transport proxy in passthrough mode, which only records the requests PBS forwards to DVS. It owns a fresh project and uses loopback ports 17178, 17179 and 17181. The project containers and volumes are removed when the run exits. The temporary environment and the ignored results directory remain available for diagnostics.

The input dataset holds the WP3.12 orders sample. An unbound Build first publishes the `orders.v1` Parquet file. A second version then commits the same rows with `amount` as a string, written by `main.go` so that it differs from `orders.v1` in that field alone. The scan contract stays sealed to `orders.v1`: that version is committed and its schema is readable, but it is not a scannable input.

| Case | Required observations |
| --- | --- |
| Historical schema | The versioned schema is bound to the transaction that cut each version and matches an independent DuckDB read, including the content hash. A correct version and transaction pair is accepted. Another version's transaction returns HTTP 409, and the orders.v1 hash on the string-amount version returns HTTP 412 |
| Bound publish | The same pipeline pins `orders.v1` with its content hash. CreateBuild proves the schema and touches no scan, upload or commit route, then freezes the version and hash. Execution proves the schema again before any scan or upload and publishes. A separate reader process locates the new output version, downloads it and reads the same rows and bytes |
| Switched input | The same pipeline and the same hash pin the string-amount version. CreateBuild answers HTTP 412 with `batched_input_schema_mismatch`. The proxy recorded that schema request refused with 412 and no scan, upload or commit, PBS wrote no build row or reservation, and the output dataset is unchanged. A second read of the output version returns identical bytes |

A passing verdict requires all three cases, an unchanged clean revision and matching binaries in the running services. After teardown, `provenance36.py` repeats the claims from the downloaded files alone and records them in `verdict.json`.

The proxy records a relayed request only once DVS has answered it, so its log follows response order. It logs an ordinary relay as `forward` and a commit request under its own action names; both count as business routes here. CreateBuild's resolver also reads the input's current schema over the same `/schema` route as the schema precondition, and the record carries no query string. The suite measures those legacy lookups on the unbound build and expects a bound CreateBuild to answer the route once more than that baseline, and only once: 200 when the hash matches, 412 when it does not. No scan or upload may appear in either CreateBuild, a refused one is followed by no scan or upload at all, and execution must answer the schema proof before any scan or upload shows up. Other forwarded requests are kept as recorded.

Look for the acceptance summary in `tools/smoke/data-integration-wp3-3/results/3c-wp36-<run-id>/wp36/wp36-acceptance.json`. The same directory contains the downloaded Parquet files, the reader verdicts and the runtime evidence.
