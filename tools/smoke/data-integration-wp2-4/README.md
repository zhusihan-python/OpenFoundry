# MySQL order snapshots

From a clean checkout run `tools/smoke/data-integration-wp2-3/run.sh mysql` with Docker Compose, OpenSSL, Bash and Python 3. The runner provisions MySQL 8.4 with a fixture CA and two database users, alongside the platform database, dataset service, connector and pipeline. Set `WP23_VERIFY_UI=1` with web dependencies and ego-browser installed to check the gallery and per-source capabilities in a browser.

The fixture writes the same two order revisions used by the PostgreSQL and S3 suites. A downstream reader pins the first input version, scans the pipeline result, and downloads its file. Order 0007 must have 1255 cents and 0011 must have zero. The printed evidence directory includes the downloaded files and an orders-report.json with dataset versions, sync runs, transactions, build and file IDs. The suite also checks TLS, per-table grants, tenant isolation, composite primary keys, exact decimals, and continued access after a failed source read.
