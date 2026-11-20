# WP3.14 default Parquet publication

A pipeline SNAPSHOT output can declare `output_schema_version: "orders.v1"` in its frozen JobSpec logic payload or in a saved output node's configuration. The schema declaration selects the [WP3.13 Parquet profile](data-integration-wp3-13-parquet-publish.md) by default. Set `file_format: "NDJSON"` or an explicit `.ndjson` output path to keep that output in NDJSON. If an output uses an `_output` object, keep the schema version, path, format and write mode together inside it. Existing configs without a declared schema use their established behavior. The supported typed schema for this increment is `orders.v1`.

```json
{
  "transform_type": "dataset_input",
  "dataset_rid": "ri.foundry.main.dataset.<uuid>",
  "dataset_snapshot": {
    "branch": "main",
    "version": 3,
    "transaction_id": "<committed transaction UUID>"
  },
  "output_schema_version": "orders.v1"
}
```

The writer accepts complete materialized result rows and uses the same bounded Arrow batches as the staged path. It writes a schema-bearing empty file for a complete zero-row result. A malformed typed row fails publication before creating a new head. Inline outputs have a 64 MiB file cap and a 128 MiB validation budget measured as the encoded file plus its uncompressed row groups. DVS still enforces its configured record limit. Large streaming copies continue to use the staged build path.

## Publication and recovery

DVS receives one content-bearing `ADD` inside the existing `outputs:commit` request, with `output_schema_version` as the typed publication marker. It confirms the schema, file and decoded values, then stamps file MIME, SHA-256, record count and schema from the verified bytes and publishes in the existing transaction. Imports and connectors can continue using their existing generic output request. The [pinned reader](data-integration-wp3-14-parquet-read.md) follows the committed SNAPSHOT to the actual file.

Scheduled execution encodes before handing off its reservation and persists the exact keyed publication bytes before sending them. The existing receipt recovery replays that request when DVS committed and its response was lost. The graph run route calls the shared writer. Ordinary canonical manual publication follows the F15 reservation and receipt contract and retains its independent review prerequisite.

Use the disposable [default publication smoke](../../tools/smoke/data-integration-wp3-14-default/README.md) to verify real publishing, precise readback and recovery.
