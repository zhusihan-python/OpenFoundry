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

One pipeline then builds a downstream projection of the published order lines.
Its input node pins a published version of that dataset, a select node keeps the store, market, order number, item number, unit price and currency,
and its output node publishes to a new dataset. The same pipeline runs three times: v1, then v2, then v1 again while v2 is still the head.
Before each run the harness reads the saved input binding. The input node result must report the same dataset, branch, version and transaction.
After the last run the independent reader maps each output version to its transaction, rescans it and downloads the file bound to that transaction.
The replay of v1 must match the original bytes and differ from v2. The report's `pipeline` section records the pipeline ID,
the three run IDs, input and output transactions, versions, file IDs, rows and checksums.
The `pipeline-*-input-v*.ndjson` files are the downloaded outputs.
This run exercises the handoff using local Docker services. Live noon extraction and production scheduling are outside its scope.

## Version comparison preview

The optional `noon-preview` case runs the checks above in the same stack, then compares the three published output versions of that one Pipeline dataset by `item_nr`:

```sh
pnpm install --frozen-lockfile
WP23_GO_MOD_CACHE=of-go-mod WP23_GO_BUILD_CACHE=of-go-build bash tools/smoke/data-integration-wp2-3/run.sh noon-preview
```

It needs the installed web workspace and ego-browser. The runner starts the real edge gateway in front of the stack's dataset service.
The independent reader of the first organization, the identity that downloads every published version above, downloads v1, v2 and v3 and calls the row comparison API through the gateway.
The same reader opens the dataset's Compare tab in ego-browser behind that gateway; only identity bootstrap and profile use the dev stub.
A key match computed by the case over the downloaded NDJSON, the gateway API and the page must agree:

| Pair | Added | Removed | Modified | Unchanged |
| --- | --- | --- | --- | --- |
| v1 → v2 | NRPL0004-1, NRPL0005-1 | none | NRPL0001-1, unit price 49 → 49.25 AED | 2 |
| v1 → v3 | none | none | none | 3 |

`preview-report.json` records all three results and the hash, version, transaction and run of each input and output.
`compare-*.png`, `compare-*.dom.html` and `compare-*.api.json` hold the two captures, with identity labels masked.
The case fails if a token or secret reaches its directory.
The gateway does not serve this stack's downloads: it follows the dataset service's redirect to a signed loopback URL that its container cannot reach.
The NDJSON therefore comes from the dataset service's download route, the entry the reader checks above use. The report records the gateway's answer to that request without asserting it.
The page comparison and the API comparison go through the gateway. On the page, a version of a format without a schema preview stays selected with a note and is never marked unavailable.
