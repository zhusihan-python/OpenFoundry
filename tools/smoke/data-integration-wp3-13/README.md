# WP3.13 live Parquet publication smoke

From the repository root on an arm64 host with Docker Compose and a Python 3.10+ environment containing DuckDB 1.4.4, run:

```sh
WP313_DUCKDB_PYTHON=/absolute/path/to/python \
  tools/smoke/data-integration-wp3-13/run13.sh unique-run-id
```

The suite uses the WP3.3 Go build, PostgreSQL 16, PBS and DVS Compose stack, signed local filesystem reads, and transport fault proxy. It owns a fresh project and uses loopback ports 17178, 17179 and 17181. The project containers and volumes are removed when the run exits. The temporary environment and the ignored results directory remain available for diagnostics.

| Case | Required observations |
| --- | --- |
| Publish | One new version in DVS, the same published transaction in PBS, exactly 1,025 rows across two Parquet row groups, the WP3.12 exact control values, and matching DVS and PBS file digests, sizes and counts |
| Empty | A successful Build and a schema-bearing zero-row Parquet file |
| Invalid row | A typed decimal error aborts publication and clears the reservation and PBS references |
| Truncated stage | The proxy forwards part of the stage request to DVS before cutting the connection. The failed Build leaves no sealed descriptor, private staging file or new committed output |
| Lost ACK | After dropping a successful DVS commit response, the restarted PBS submits the original persisted request bytes. DVS returns the original receipt without a second publication or stage request |
| DVS rejection | Real DVS rejects a corrupted footer and a mismatched stage MIME. A valid seal is then rejected by the commit route when the MIME differs. The probe aborts its reservation after checking that the seal was never published |

The negative cases compare the previous version list, head, file index and old file SHA-256 over an authenticated download before and after each case. Read-only container inspection checks for private staging or unpublished backing files that the HTTP APIs cannot expose. DuckDB verifies the Parquet copies downloaded through the real DVS file and signed URL endpoints. The WP3.3 smoke suite separately verifies the default NDJSON path and its memory bounds.

Look for `verdict.json` in `tools/smoke/data-integration-wp3-3/results/3c-wp313-<run-id>/wp313/`. A passing verdict requires all six cases, the proxy self-test, unchanged source fingerprints, matching deployed binary hashes, and DuckDB 1.4.4. The same directory contains per-case evidence and the downloaded Parquet files. Service images, PostgreSQL and DuckDB versions, and binary hashes are recorded in the runtime evidence.
