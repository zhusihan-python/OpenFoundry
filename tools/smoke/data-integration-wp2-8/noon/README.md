# noon AE order-line handoff

Run from a clean checkout on Docker Desktop:

```sh
python3 -B -m unittest discover -s tools/smoke/data-integration-wp2-8/noon -v
bash tools/smoke/data-integration-wp2-3/run.sh noon
```

The five synthetic sales-dashboard hits in replay.ndjson come from the in-memory
[upstream QA replay](https://github.com/UNILUME-AI/unilume/blob/6f3103e82248bcd33cd375be5338c207298ae04b/packages/sync/src/connector/noon-replay.ts#L346),
with to_date 2026-11-20 and pages of two. The downstream projection uses the
[order_live mapping](https://github.com/UNILUME-AI/unilume/blob/6f3103e82248bcd33cd375be5338c207298ae04b/packages/sync/pipelines/noon/noon_order_live.json).
The item number identifies one line in the fixture store and market. Its numeric suffix identifies the item within the order.
The amount is the offered unit price in the stated currency.

The test stages two controlled deltas for one QA store. The first contains three items.
The second adds items four and five and changes the first item's unit price from 49 to 49.25 AED.
The batch split and price change are local scenarios. A real S3 Connector ingests each staged file.
The harness reads its committed raw transaction, overlays that delta on the selected prior output
version, and publishes a complete snapshot. Redelivery starts at the same committed raw transaction
and prior version. The output report records the store, input and output identifiers, checksums and tests.
This run exercises the handoff using local Docker services. Live noon extraction and production scheduling are outside its scope.
