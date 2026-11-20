# WP3.14 live read-side smoke

Run on an arm64 Linux-container Docker host with Docker Compose, Node, installed frontend dependencies, ego-browser and a Python environment with DuckDB 1.4.4. Use a clean detached checkout and fresh run ID.

```sh
pnpm install --frozen-lockfile
WP314_DUCKDB_PYTHON=/absolute/path/to/python \
  tools/smoke/data-integration-wp3-14/run14.sh unique-run-id
```

The test runs the repository's WP3.3 PBS, DVS and PostgreSQL stack, with a real gateway and the existing isolated UI dev configuration. Only identity bootstrap and profile calls use the dev stub. The stack owns loopback ports 17178 and 17181 and chooses local gateway and UI ports. The browser space and all runtime resources belong to this run.

| Case | Required observations |
| --- | --- |
| Committed pages | Real WP3.13 output files are registered in one SNAPSHOT and read in path and row order. A new NDJSON head is committed between pages. The 1,028 historical rows match independent DuckDB, and an exact long-integer predicate returns its expected row |
| Historical preview | DVS preview carries the selected transaction, quoted full-precision long integers and typed columns. Its cursor returns the next rows. Typed empty scan and preview expose the schema |
| Rejections | Real DVS rejects a corrupted footer, mixed formats, invalid or cross-version cursors and a different tenant |
| PBS replay | A real PBS preview reads pinned input through multiple DVS pages. Two real Builds using the same pinned source produce the same exact bytes before and after the head update. An empty input preview preserves its column schema |
| Browser | The real dataset page selects history, advances one page, switches to the changed head and back, and displays columns for a typed empty version |

A passing verdict requires all five cases, a clean unchanged revision and matching binaries in the running services, plus cleanup. Evidence is in `tools/smoke/data-integration-wp3-14/results/<run-id>/`, and build binary metadata is in the corresponding WP3.3 stack results. Reproduction uses disposable synthetic users and order rows.
