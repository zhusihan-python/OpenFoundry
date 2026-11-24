# Lingxing local-product snapshot acceptance

From a clean checkout with Docker Compose, Bash, Python 3, curl and OpenSSL, run:

```sh
python3 -m unittest discover -s tools/smoke/data-integration-wp2-8
bash tools/smoke/data-integration-wp2-3/run.sh lingxing
```

The simulated product-list responses use the public offset, length, total and product fields. The fixture exercises a two-page export, preserves decimal costs and leading zeroes in SKUs, and completes all pages before creating an export object. Empty results are recognized; publishing an empty replacement awaits a business decision. [Lingxing local product list](https://apidoc.lingxing.com/new/api-docs/Product/ProductLists/)

An upstream batch ID identifies one completed export. The fixture supplies two synthetic batches as distinct NDJSON objects. A real S3 Connector reads each with scoped credentials and commits it into a raw dataset. The test passes the batch ID, original Connector run, raw transaction, expected row count and checksum to the handoff. The handoff re-reads the committed file, verifies its checksum and submits the original bytes to the dedicated product dataset using a stable Dataset commit key. Redeliver the first batch after the second is visible to observe the original receipt. Reusing its ID with different bytes must return HTTP 409. Retrying a manual Connector extraction creates a separate run; the replay in this test starts at the committed export handoff.

The evidence directory contains product-report.json, both source batch files, two downloaded product files, binary checksums and Compose image IDs. The Docker project and its temporary identities are isolated. The report records each upstream batch and both Dataset transactions. A real approved Lingxing export is still needed to validate source ownership and production field mapping.
