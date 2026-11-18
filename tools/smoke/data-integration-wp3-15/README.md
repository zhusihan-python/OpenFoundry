# WP3.15 format benchmark

This smoke compares the same production batched build path with a Snappy Parquet output and an uncompressed NDJSON output. PBS reads one committed 100,000-row snapshot across four NDJSON files. The input branch advances to different content before execution, so every trial must still read the pinned version. Each build publishes one file through DVS staging and commit.

From a clean checkout on the local Docker Compose host used by WP3.3:

```bash
WP315_DUCKDB_PYTHON=/absolute/path/to/duckdb-1.4.4/bin/python \
  tools/smoke/data-integration-wp3-15/run.sh RUN_ID "$(git rev-parse HEAD)"
```

Use a fresh run ID with ports 17178 and 17181 free. The runner builds PBS and DVS, starts a disposable PostgreSQL and filesystem stack, and verifies the exact revision and published file bytes. The run uses scoped synthetic identities. The results directory under `tools/smoke/data-integration-wp3-3/results/3c-wp315-RUN_ID/wp315/` contains trial data, files, and hashes.

Four write pairs alternate which format runs first. Both service processes restart before each trial. The PostgreSQL and filesystem caches carry over between trials. Write elapsed time starts at the PBS Execute request and ends at its successful response. It includes reading, encoding, staging, and committing. Input preparation, build creation, and signed downloads are outside that interval. Linux `/proc/1` supplies the baseline and final high-water RSS for each service. High-water RSS includes process startup. The before and after samples show how much that peak grew.

DuckDB 1.4.4 checks all six values of each downloaded order, including amounts, exact 64-bit order IDs, offsets normalized to microseconds, and nullable notes. The fixture assigns 20 stores in round-robin order from the first order ID and gives each order a synthetic cost of `1.0000 + 0.1375 × store`. The query verifies each store's count, gross sales, cost, and profit against exact values computed during fixture generation. Ten timed queries per format alternate order after warm-up on the first pair of files with one DuckDB thread. Each timing includes fetching the aggregate. These are warm local-file query measurements with host filesystem cache available.

The runtime checks that valid credentials from a second organization can access their own dataset and are denied access to the tested published artifacts. The result describes this local workload and its configuration. Ordinary canonical manual publication follows the separate F15 review and real-backend acceptance.

DuckDB reader options were checked on the pinned runtime and against the project documentation. [NDJSON reader](https://duckdb.org/docs/lts/data/json/loading_json) · [Parquet reader](https://duckdb.org/docs/lts/data/parquet/overview)
