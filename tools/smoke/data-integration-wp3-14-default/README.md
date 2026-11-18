# WP3.14 default Parquet write acceptance

From an arm64 host with Docker Compose and an absolute Python 3.11+ executable with DuckDB 1.4.4, run the disposable suite:

```sh
WP314_DUCKDB_PYTHON=/absolute/path/to/python \
  tools/smoke/data-integration-wp3-14-default/run.sh fresh-run-id
```

The runner builds fresh DVS, PBS, and `schedules-tick` Linux binaries using the WP3.3 build environment. It uses PostgreSQL 16, Kafka 3.7.1, Debezium 2.7.2.Final, and Apicurio 2.6.13.Final on an isolated Compose project. Host ports 17178, 17179 and 17181 must be available. Docker volumes and containers for the run are removed at exit. The ignored results directory is retained, along with the temporary Compose environment needed for diagnostics.

Each schedule is registered and enabled via the real PBS scheduling API. The fixture moves precisely one annual definition's due time into the past and starts the real one-shot `schedules-tick` process. It then compares the producer audit IDs, actual Kafka messages delivered through Debezium logical replication, and durable PBS dispatch IDs. The test controls the due time. Kubernetes CronJob timing is outside this run.

| Case | Required observations |
| --- | --- |
| Default Parquet | A frozen `orders.v1` profile with no explicit format produces one typed Parquet version from the selected historical NDJSON source, even after its head advances. |
| Lost ACK | Historical Parquet input is republished. The WP3.2 proxy drops a DVS response only after a confirmed commit and holds PBS's first retry until the process is stopped. A new PBS process sends the same persisted request and key and links the original DVS transaction. Exactly one version is added. |
| Empty Parquet | A scheduled empty source produces a schema-bearing file with no rows and no row groups. |
| Explicit and unprofiled NDJSON | The same historically pinned source retains exact values and NDJSON media type when the JobSpec explicitly chooses NDJSON or omits the typed profile. |
| Invalid typed row | The encoder rejects an invalid decimal, the dispatch fails, and the old head, indexed file, and signed-download bytes stay intact. |
| DVS rejection | Direct typed inline commits with a damaged footer, wrong MIME, or unknown profile add no receipt, version, or file. |
| Manual graph | The independent graph runs endpoint is exercised by `manual_graph.py`. Ordinary canonical manual publication has a separate review prerequisite. |

DuckDB validates the exact values and types of files downloaded through the actual authorized DVS route and its signed local filesystem URL. Receipt checks compare SHA-256, original request bytes, and published transaction identity across DVS, the proxy, and PBS. The suite uses fixture-only due time and HTTP transport controls.

The verdict and case artifacts are under `tools/smoke/data-integration-wp3-3/results/3c-wp314d-<run-id>/wp314-default/`. It requires every case and binds the source fingerprint, deployed binary hashes, runtime identities and downloaded artifact hashes. A failed or incomplete run leaves diagnostic files without a passing verdict.
