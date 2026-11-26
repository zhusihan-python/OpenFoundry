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

## Versioned schema of the order lines

Each order-line publication is a keyed inline `outputs:commit` that declares one fixed publish contract in its formal `schema` field:
`currency, item_nr, market, order_nr, sku, store, unit_price`, each `STRING` and not nullable.
The publisher builds the field list from that constant. It rejects rows with a missing or extra column or a value that is not a string, and an empty snapshot declares the same columns.
The dataset service seals the declaration in the immutable receipt of the transaction that cut the version. Its versioned schema route answers from that receipt, matched by dataset, transaction and branch. The service derives nothing from the file and never reads the transaction's free-form `metadata`, so the names, order, types and nullability are the publisher's statement and the content hash follows it.

The reader of the first organization holds no write permission. It calls `GET /v1/datasets/{id}/schema` on the order-line dataset with its own token:
after v1 is published and before v2 exists, and again after v2 is published.

- Each answer is bound to the version and transaction that cut it and lists the seven contract fields. The hash equals SHA-256 of the compact JSON `fields` array.
- v2 changes row values, including the unit price `49` → `49.25`, and answers the same fields and hash as v1. A second read of v1 while v2 is the head returns the earlier bytes.
- The other version's transaction returns HTTP 409 `dataset_schema_transaction_mismatch`; the version's own transaction and hash return HTTP 200; another hash returns HTTP 412 `dataset_schema_content_hash_mismatch`.
- The fields match the ordered keys and string values of the NDJSON the reader downloads for that version. Another tenant cannot read the schema.

The connector's raw batch and the six-column Pipeline projection are NDJSON without a schema in a commit receipt. Their versioned schema stays HTTP 422 `dataset_schema_format_unsupported` without `fields`. The projection has its schema in transaction metadata only, because the pipeline commits without a key, and the route does not read that metadata.

`schema-*.json` hold each answer, and the report's `schema` section records the dataset, columns, hashes and answers.

## Schema precondition on the Pipeline input

The input node's `dataset_snapshot` also carries `expected_schema_hash`, the hash the reader read for the pinned version.
All three runs name the same seven-column hash. The Pipeline service proves it as the caller before it reads a row,
so v1 and v2 are each read by their version and transaction only after the committed schema of that version answered the hash.

After those runs the test publishes a third version of the order lines. It is a valid publication:
the declaration states `unit_price` as `DOUBLE` and every stored row holds a number, so its schema hash differs from the first two versions.
The same Pipeline, still naming the original hash, is then bound to v3.
The run fails on the input node, whose own error carries `dataset_input_schema_rejected` and `dataset_schema_content_hash_mismatch`.
The two nodes after it are cancelled, and the output dataset keeps its three versions.

Before and after the refused run the independent reader maps the last good output v3 and the input v2 to their transactions,
downloads both files and asks the schema of v2 bound to its transaction and the pinned hash. Both answers are the same.
`schema-gate-refused-run.json` holds the refused run, `published-v3-numeric.ndjson` the third version,
and the report's `schema_gate` section records the two snapshots.

This stack has no recorder between the Pipeline service and the dataset service, so the run shows the refusal and its reason.
That a refused schema reads no row is proven by the unit tests of the Pipeline service.

## Refused inputs of the Pipeline

After those runs the same graph is refused twice at its input node, each time on a Pipeline of its own,
so the Pipeline of the three runs keeps the pin of its last good run. Saving the graph accepts both.
Each run fails at its input node with the Dataset service's reason in that node's own error: the two nodes after the input are cancelled,
no output resource is committed, and the output dataset and the order lines keep their versions.

- The pin names v1 together with the transaction that cut v2. The Pipeline service asks the schema of that binding as the caller,
  and the Dataset service refuses it with HTTP 409 `dataset_schema_transaction_mismatch`.
  The input node's own error names that status and code, `batched_dataset_409_dataset_schema_transaction_mismatch`.
- The same user in another organization saves the pin of the second run, which succeeded for the first organization,
  on a Pipeline of that organization over an output dataset of its own. The Dataset service answers that organization
  with a denial, HTTP 403 or 404, and not with the dataset. That Pipeline publishes nothing, and its output dataset keeps no version.

Before the first refusal and after each one, the independent reader of the first organization lists the versions and heads of both datasets,
maps the last good output v3 to its transaction and file, rescans the version and downloads the file. The three views are the same.
The bytes equal the v1 replay downloaded above and the SHA256 is the one recorded for that run.
`refused-crossed-transaction-*.json` and `refused-other-organization-*.json` hold the saved graph and the run of each refusal.
The report's `pipeline.refusals` section records the saved input, tenant and owner, node states and errors of both, and the reader's three views.

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

## Schema comparison of the order lines

`noon-preview` then compares the published order lines themselves, the input dataset of the runs above, on the same Compare tab in one page session of the real browser:
v1 with v2, v2 with v3, and v1 with v2 again while v3 is the head.
The reader asks the schema of each version through the gateway, as the page does, and of the dataset service bound to the version and transaction.
Both answer the bytes the handoff read when that version was published. The page must state these changes:

| Pair | Schema diff |
| --- | --- |
| v1 → v2 | none: the same seven columns and hash, while rows and a price changed |
| v2 → v3 | `unit_price` `STRING` → `DOUBLE`; nothing added, removed or otherwise changed |

v3 is the input of the refused run. The report records that run's ID, its saved input binding and the input node's error,
and the page's selected transaction must be the one that run was bound to.
An answer that is not a committed schema bound to the selected version and transaction, an empty one included, never counts as "no changes".
The page's Schema diff panel is counted item by item: with a change it holds that row and no empty notice, without any exactly one `No schema changes.` and no row.
A change next to the empty notice, or next to another sentence, never passes. Each capture also reads such mixed panels with the code that reads the page's own, and must refuse them.
`compare-schema-*.png` and `compare-schema-*.dom.html` hold the three captures;
`compare-schema-v*.gateway.json` and `compare-schema-v*.dvs.json` hold both answers of each version.
The report's `schema_diff` section records them.
