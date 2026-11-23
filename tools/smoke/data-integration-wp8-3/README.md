# WP8.3 order-version comparison smoke

Runs on an arm64 Linux-container Docker host with Docker Compose, Node, installed
frontend dependencies, ego-browser and a Python environment with DuckDB 1.4.4. Use
a clean detached checkout and a fresh run ID.

```sh
pnpm install --frozen-lockfile
WP83_DUCKDB_PYTHON=/absolute/path/to/python \
  tools/smoke/data-integration-wp8-3/run.sh unique-run-id
```

The test runs the repository's WP3.3 PBS, DVS and PostgreSQL stack, with a real
gateway and the existing isolated UI dev configuration. Only identity bootstrap
and profile calls use the dev stub. The stack owns loopback ports 17178 and 17181
and chooses local gateway and UI ports. The browser space and all runtime
resources belong to this run.

| Case | Required observations |
| --- | --- |
| Historical range diff | Two real multi-file Parquet snapshots of 1,300 interleaved rows each contain 60 added, 60 removed and 60 modified keys with file positions swapped between sides. The browser pages through every change without reloading after a third version is published, and reverses one page with the exact same cursor. Cursor rejections for malformed tokens and changed key, limit or version return machine-readable errors. An isolated DVS restart bounds the service's peak memory during a full traversal |
| Historical row match | Commit two real multi-file Parquet snapshots verified by independent DuckDB. Compare v1 and v2 by `order_id` in the browser: one added row, one removed row, one modified amount and 130 unchanged rows spanning both files and a complete scan page. Publish a third snapshot and verify the pinned comparison is unchanged. An unknown key shows the real HTTP 400 rejection without a partial table. Duplicate keys and transaction mismatches are rejected machine-readably |
| Historical schema diff | Commit two real Parquet snapshots with independent DuckDB footer and type checks. Compare v1 and v2 in the browser and verify an added field, a removed field and a type change. Publish a third snapshot, then reselect the same historical pair and verify identical results. In the disposable DVS volume, temporarily move v1's exact registered file. Side A shows the real HTTP 422 error, side B retains v2 and no difference panel is rendered. Restore v1 and verify the original signed SHA-256 and rendered difference |

A passing verdict requires every case, a clean unchanged revision and matching
binaries in the running services, plus cleanup. Evidence is in
`tools/smoke/data-integration-wp8-3/results/<run-id>/`, including browser captures.
Build binary metadata is in the corresponding WP3.3 stack results. Reproduction
uses disposable synthetic users and order rows.
