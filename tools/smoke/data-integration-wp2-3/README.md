# S3 order snapshot to a pinned pipeline output

Run from a clean checkout of the candidate revision with Docker Compose, Python 3, OpenSSL, and curl with AWS SigV4 support.

```sh
tools/smoke/data-integration-wp2-3/run.sh
```

Each run compiles the connector, dataset and pipeline services from the checkout and starts a fresh Postgres 16 and RustFS 1.0.6. Host ports bind to loopback. The script prints the evidence directory and removes its containers and volumes on exit. Set `WP23_GO_MOD_CACHE` and `WP23_GO_BUILD_CACHE` to existing Docker volumes or cache directories to reuse dependencies.

The fixture signs its own local JWTs. Two organizations share a user identity and a separate user in the first organization reads the published versions without write permissions. For object storage, the fixture provisions separate temporary writer and reader credentials scoped to one order key. It verifies the writer cannot create an object under another tenant’s prefix. The connector receives only that reader credential. The test also attempts access with a second identity scoped to another prefix.

The public APIs create a source, bind its credential, create a sync and run it twice after replacing the object. Once both versions exist, a FASTER pipeline pins version 1, selects order ID and amount in cents, and publishes a new dataset. Its executor uses the pipeline run ID as the build ID. The consumer downloads and parses the transaction-bound file and reads the same version through the row scan. A malformed second line and a deleted object exercise two source failures. Both must preserve the preceding versions.

The evidence directory contains synthetic source files, downloaded snapshots, the published output and `orders-report.json`. Use the report to look up source, sync run, dataset, build, branch, version, transaction, file ID and SHA-256. Re-running the command creates new IDs. The expected order amounts stay 1255 and 0 cents even though the source head has changed. The scope is one NDJSON object per snapshot.

[Fixture storage identity and policy model](https://docs.rustfs.com/en/security-compliance/iam/sts)

`lingxing`, `noon` and `amazon` run the three source handoffs of `tools/smoke/data-integration-wp2-8` through this entry, and `lingxing-gateway`, `noon-gateway` and `amazon-gateway` the same rounds with their objects also accepted through the edge gateway; the [README of that directory](../data-integration-wp2-8/README.md) holds both.

Set `WP23_VERIFY_UI=1` to verify the gallery and the configured source in ego-browser with the installed web workspace dependencies. The Vite bootstrap and profile responses are fixtures; catalog and per-source capabilities come from the real connector service.
