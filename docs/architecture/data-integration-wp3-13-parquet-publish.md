# WP3.13 staged Parquet publication

WP3.13 adds an explicit Parquet choice to the manual `batched_dataset_copy` build. The build scans the pinned NDJSON input, encodes Arrow record batches, and publishes one Parquet artifact to the output dataset's active branch. The writer uses the [WP3.12 order schema and Parquet profile](data-integration-wp3-12-parquet.md).

## Select Parquet

In the `logic_payload` of the published JobSpec, specify the output as shown below. The input dataset RID belongs in `inputs[].dataset_rid` on the JobSpec. `logic_payload.input` pins the branch, version, and single file or `all_files` selection.

```json
{
  "transform_type": "batched_dataset_copy",
  "input": {"branch": "main", "version": 3, "all_files": true},
  "output": {"format": "parquet", "schema_version": "orders.v1", "logical_path": "part-00000.parquet"}
}
```

The input supplies all six `orders.v1` fields. Use an exact JSON integer for `order_id`, a scale-four decimal string for `amount`, `YYYY-MM-DD` for `order_date`, a microsecond-aligned RFC 3339 timestamp for `ordered_at`, a source zone string for `source_time_zone`, and a string or `null` for `note`. PBS preserves the source zone string as data. It writes at most 1,024 rows and 4 MiB of input record bytes per Arrow batch, with at most 4,096 row groups. A valid empty input produces a Parquet file with the same column schema and zero rows. Omitting the output format selects the established NDJSON path.

## Stage and commit

- `outputs:stage` accepts `format=parquet`, a `.parquet` logical path and `Content-Type: application/vnd.apache.parquet` together. The existing reservation, Build, branch, caller and idempotency key identify the upload. Before sealing, DVS applies the configured upload and record limits, then checks the bounded footer, column schema, row group and page counts, byte ranges, and Snappy blocks. DVS computes the digest over the exact bytes. PBS validates source values before writing, and the independent DuckDB check decodes the published column values. [IANA media type registration](https://www.iana.org/assignments/media-types/application/vnd.apache.parquet)
- `outputs:commit` uses the existing `STAGED_ADD` operation with the same logical path, staged object ID, size, SHA-256 and Parquet content type. DVS compares the request against the sealed descriptor and rehashes the installed bytes, then publishes the snapshot and stores the immutable keyed receipt in the existing transaction. The file index carries the actual content type and the output records the `orders.v1` schema identifier.
- PBS remembers the exact commit request before sending it. If DVS publishes and the acknowledgement is lost, a renewed request by the Build creator replays those bytes and reconciles the receipt. A rejected or interrupted upload leaves the previous committed version readable.

WP3.14 covers default publication and reading by committed version. To exercise WP3.13 with real PBS, DVS, PostgreSQL and DuckDB 1.4.4, follow the [live publication smoke instructions](../../tools/smoke/data-integration-wp3-13/README.md).
