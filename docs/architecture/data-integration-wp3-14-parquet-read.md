# WP3.14 version-pinned Parquet reads

WP3.14 adds reading of the [WP3.13 published `orders.v1` files](data-integration-wp3-13-parquet-publish.md). The reader follows the version's committed SNAPSHOT transaction and its registered ADD files. Each page verifies dataset access, then resolves the branch, version and transaction again. A file must live at its registered transaction-owned object key. The current dataset file index can move with the head, so the pinned reader looks up the transaction files.

## Scan and preview

- `GET /v1/datasets/{rid}/files/rows:scan` accepts the existing branch and version with either `path` or `all_files=true`. A Parquet page carries `format: "parquet"`, `schema_version: "orders.v1"`, the typed schema and an opaque cursor when more work remains. The cursor binds the dataset, branch, version, transaction, scan mode, predicate and file position. A scan of all files visits their logical paths in order. Parquet and other files in the same all-files request produce an explicit rejection. An explicit path can select a supported file from that transaction.
- `GET /v1/datasets/{rid}/preview?version=N&branch=main` uses the same committed scan. Request the next page with the returned `next_cursor`. The Dataset detail page binds its history selection and next page to the transaction and version in the URL. Versioned Parquet preview exposes the full column list for an empty file and represents `order_id` as decimal text so the browser can display every digit. Machine scans and PBS preserve the JSON integer. Amounts remain scale-four strings. Timestamps are UTC with microseconds and carry the source time zone in its own column.
- The versioned reader supports the bounded `orders.v1` V1 pages from WP3.13. It checks footer metadata, the exact schema, chunk bounds, Snappy blocks, definition levels and PLAIN values in the row groups it reads. A page has row and returned-byte limits and a work allowance across files and selected row groups. Existing NDJSON pages keep their byte-offset cursors. [Apache Parquet page encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/) · [Parquet levels](https://parquet.apache.org/docs/file-format/nestedencoding/)

## Pinned pipeline inputs

The existing PBS `dataset_input` snapshot checks the exact branch, dataset, version and transaction on each page. Parquet reads validate the reported schema and carry its ordered columns, including for empty datasets. The real PBS preview exposes the source schema and original JSON values. A batched Build can copy a committed Parquet input through the same scanner after the input branch advances.

This increment covers reads, historical preview and fixed-input reruns. The follow-up WP3.14 publication increment covers default Parquet output for ordinary manual and scheduled Builds. [Live read-side checks](../../tools/smoke/data-integration-wp3-14/README.md)
