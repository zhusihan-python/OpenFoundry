# Amazon monthly transaction_view handoff (simulated)

Run from a clean checkout on Docker Desktop:

```sh
python3 -B -m unittest discover -s tools/smoke/data-integration-wp2-8/amazon -v
bash tools/smoke/data-integration-wp2-3/run.sh amazon
```

This is the first case of an Amazon monthly report that arrives through the ziniao bridge.
Nothing here contacts Amazon or ziniao and no account is read. The CSV files, the registrations
and the receipts are simulated inputs that the test submits to the public connector and dataset
services, the same services the other `run.sh` cases use.

## Contract

The field names are read from upstream `UNILUME-AI/unilume` at `6f3103e`,
`desktop/packs/ziniao-bridge/src/report-upload.ts` (lines 113-124, 155-177 and 278-292).

- A file registration carries `storeId`, `marketplace`, `reportKind`, `period`, `coverageStart`,
  `coverageEnd`, `completeness`, `sha256`, `byteSize` and `contentType`. Its receipt is
  `status: uploaded` and a `fileId`.
- A no-data report carries `storeId`, `marketplace`, `reportKind`, `period` and `checkedAt`, and
  has no file. `checkedAt` is only the time the report was sent. The month counts as no-data only
  when the receipt says `status: reported`; `requirementCode` is optional.
- Neither names a tenant. The tenant is the signed caller's.

## Flow

1. The test stages one NDJSON envelope per delivery in S3. The S3 Connector accepts NDJSON lines
   only, so the CSV travels inside the line as base64 and nothing of it is lost.
2. A real S3 Connector commits the envelope to the received dataset. Its content hash equals the
   staged bytes.
3. The handoff reads the committed envelope and checks it: the receipt, the scope, the coverage,
   the length and the hash of the file.
4. A registered file is published as the original CSV bytes through `outputs:commit`. The
   idempotency key is the scope and the `fileId`, so one month can hold several files. The
   registration and the `fileId` are kept in the transaction metadata. The commit's `schema`
   says where the header is: on the second record, after the `All amounts in USD` preamble.
   It uses the CSV options the dataset service keeps with a publication. The bytes are not
   changed.
   The `schema` also declares one field per header column, read from the committed bytes by
   the publication (`csv_schema` in `reports.py`). The dataset service derives nothing from a
   CSV file. The business role of a column names the type it may hold, and the cells of that
   file decide whether it holds:

   - an amount column is `DOUBLE`, and `settlement id` and `quantity` are `LONG`, while every
     cell that is not empty is a number as the Dataset CSV reader gives one;
   - any other column is `STRING`, and so is an amount or a count that holds text in that file;
   - every field is nullable, because an empty cell is null in any column.

   A number is what `csvCellValue` in
   `services/dataset-versioning-service/internal/handlers/file_rows.go` makes one. The
   publication mirrors that rule in `reader_number`, so `1e3`, `+1`, `001.00`, `NaN`,
   `Infinity` and `007` are text in a `total` column, although `Decimal` would parse some of
   them. `order postal` stays `STRING`: a code with leading zeroes is text.
   A column is named as the same reader names it (`csvColumnNames` in the same file, mirrored
   by `column_names`): each header cell is trimmed, a cell that is empty once trimmed is
   `column_<position>` counted from 1, and a name that comes twice, or a header of more than 256
   columns, is refused.
5. A reported no-data month is written as one JSON record to a separate status dataset. It never
   touches the report dataset.
6. An independent reader, who has no knowledge of the staged file or of the receipt, resolves each
   registration by version and transaction and confirms the bytes, the SHA-256 and the length.
7. One Pipeline then reads the two published October reports, not the staged envelopes. Its graph
   is a pinned dataset input, a `select` of the transaction identity and every amount column, and
   an output to another dataset. The same Pipeline runs three times, pinned by dataset, branch,
   version and transaction to v2, v3 and v2 again while v3 is the branch head. Each version holds
   one file only, so a pin names one published report. `transactions.py` holds what a reader
   expects of the transactions; it is not part of the publication.
   The pin also carries the schema a reader asked of that version through the versioned schema
   route: the answer's `content_hash` as `expected_schema_hash` and its `fields` as
   `expected_schema`. The Pipeline service refuses a run whose input version is committed under
   another schema.
8. A report that must be refused is the second October file with the `total` of its last data row
   as `N/A` (`text_total` in `transactions.py`, a test input). It has its own `fileId` and goes
   the way of every other file, so nothing marks it. Its `total` column is `STRING` and its
   schema hash is another one. It is published as v4, and the same Pipeline is pinned to it with
   the same expected schema.

## What is real, what is assembled, what is pending

- Real, read from the pinned source: the field names above and the offline transaction_view rules
  in `report-check.ts` that the CSV files follow (required columns, an `All amounts in USD` line,
  equal column counts, amount columns that sum to `total`, dates inside the month,
  marketplace `amazon.com`). The unmodified upstream `checkReport` accepts all three files.
- Assembled by this test: the three CSV files, the negative report derived from the second
  October file, every identifier in them, the store UUID, the `fileId` values, the envelope
  layout, the object keys, the handoff rules and the schema rule of step 4.
- Pending from the source side: a real Transaction View file and its column layout, the core
  endpoints `/api/report-files` and `/api/report-files/no-data`, the values of `requirementCode`,
  the path from the bridge to S3 (upstream PUTs to a presigned URL), consent gates, scheduling,
  other stores, marketplaces and report kinds.

## What the test asserts

- Three files register, two of them for one month, and the reader confirms each one.
- The same registration delivered again replays the first receipt (HTTP 200) and publishes nothing.
  A different file under the same `fileId` is a conflict (HTTP 409).
- The handoff refuses, with nothing published: a file shorter than or different from its
  registration, coverage outside the period, an upload whose receipt is not `uploaded`, and a
  no-data month whose report failed, was skipped or was never sent. A declared length that differs
  from the bytes is refused by the dataset service (HTTP 400).
- A reported no-data month is recorded in the status dataset only: the report dataset keeps its
  head and every registered file stays readable with the same bytes.
- An envelope that cannot be fetched fails the connector run and confirms no month.
- The other tenant is denied the datasets, transactions, downloads and commits, and a staged
  envelope. A record that claims the other tenant in headers stays with the signed caller.
- Each October report is published with its header on record 2 and with one nullable field per
  header column, in header order: the 16 amounts `DOUBLE`, `settlement id` and `quantity` `LONG`,
  every other column `STRING`. The CSV options are `headerRow: 2`, `charset: UTF-8`, `bom: strip`.
- A read-only reader asks the versioned schema route of each October report,
  `GET /v1/datasets/{id}/schema?branch=main&version=N&transaction_id=T`, for the version it lists
  for the transaction, not for a number the handoff counted. The answer is 200, bound to the
  dataset, the branch, the version and the transaction. `total` is `field_type: DOUBLE`. The
  `fields` equal the ones the publication declared, which the reader reads from the transaction
  metadata: the route names the type of a field `field_type`, the kept schema says `type`. With
  the answer's own `content_hash` as `expected_content_hash` the route answers 200, with another
  hash it answers 412 `dataset_schema_content_hash_mismatch`. v2 and v3 answer the same fields
  and the same hash. The hash is the one the route answers: the test encodes no JSON to compute
  one.
- Each Pipeline run is checked against its saved graph and the receipt of its input node: the
  pinned dataset, branch, version and transaction, the schema the reader expects of that version
  (`expected_schema_hash` and `expected_schema` in the pin), the CSV format, the row count and
  the rows under their header columns. The `transaction_rid` of the run's output resource,
  `pipeline-run:<run id>:output`, is repeated in the `metadata.transaction_rid` of the Dataset
  transaction that cut the output version. That links a run to its output.
- A read-only reader maps each output version to its transaction and downloads its file. The
  rows are reconciled line by line and value by value with the report file read from its own
  bytes: three transactions for v2, two for v3, and a replay equal to the first output. Amounts
  are compared as decimals, so `-9.00` and the reversal fees `1.20`, `2.25` and `4.10` of the
  refund keep their sign. A `settlement id` is a number, an `order id` is the string with its
  leading zeroes, an empty cell is null and a bool never passes for a number. The two files of
  one month do not mix: an output holds only the `settlement id` values of its own file, in file
  order. The bytes and SHA256 of the first output and of the replay are equal and differ from
  those of the second.
- The sum of `total` is compared as decimals on two sides, in each downloaded output and in the
  report file read from its own bytes: `59.57` for the first October file, `-1.36` for the
  second, and `59.57` again for the replay. Each build records it as `net_total`.
- After the third run, v1 and v2 are history. The reader starts from each version number alone.
  It takes the version to the transaction that cut it, lists the files of that transaction with
  `transaction_id` on the files listing, and downloads the one file it holds. It also scans the
  version by number. v1, v2 and v3 give the bytes and the hash each build published, by download
  and by scan. Nothing of that step uses what a build recorded while its output was the head.
- The preview of an output transaction is compared only under unique column names and rows as
  wide as the columns.
- A report whose `total` holds text is refused for its schema. It is the second October file with
  the `total` of its last data row as `N/A`; a unit test proves that exactly that cell differs
  and that the BOM, the line endings and every other byte stay. It is registered under its own
  `fileId` and published as v4 the way every other file is. The reader's schema answer for v4
  says `total` `STRING` and every other field as v3, under another hash, and the route answers
  412 `dataset_schema_content_hash_mismatch` to the hash the Pipeline expects.
- The same Pipeline is then pinned to v4 with the same `expected_schema_hash` and
  `expected_schema` as the three runs before it, and the saved graph shows it. The run is
  `failed`; the node statuses are `input` `failed`, `project-transactions` and `output`
  `cancelled`. The input node's own error, as the run API returns it, carries
  `dataset_input_schema_rejected`, `dataset_schema_content_hash_mismatch` and
  `field "total" is STRING in the committed schema and DOUBLE in the expected schema`. The
  reason is written by the Pipeline service, not by this test. No output resource is committed.
- Before and after the refused run the read-only reader lists the output dataset and the report
  dataset. The versions, each with its transaction, and the output head are equal item by item.
- After the refusal the reader starts from the version list it lists: it finds output v3 there,
  takes the `transaction_id` of that entry to its transaction, lists the files of that
  transaction and downloads its one file. Only then is what it resolved compared with what the
  third run recorded: the transaction, the file id, the bytes and the SHA-256. They are also the
  ones of the first run.

The evidence directory holds `amazon-report.json`, every staged envelope, the three published CSV
files, the no-data record, the saved graph, run and Dataset transaction of each Pipeline run, the
three downloaded outputs and their three downloads from history. It also holds the schema answers
of the reader (`schema-v2.json`, `schema-v3.json`, `schema-v4.json` and the conditional reads next
to them), the negative report `negative-transaction_view-2026-10.csv` and its publication, the
saved graph and the run of the refused fourth run (`pipeline-4-graph.json`,
`pipeline-4-run.json`), and the `schema_gate` section of `amazon-report.json`.

## Known limits

- The dataset service refused a wrong declared length in this run (HTTP 400). It does not check
  the hash of inline content: its commit handler compares `sha256` for staged files only. That is
  read from the handler and is not asserted here. The handoff refuses a hash mismatch and the
  reader confirms the hash.
- Downloads are served as `application/octet-stream`. The content type is read from the head file
  index, which is why it is asserted when a registration is the head.
- The file id in a commit receipt and in the file index is not accepted by downloads. Downloads
  use the id of the listing entry that is bound to the transaction.
- Each registration is its own snapshot version of the report dataset, so the head holds the
  latest registered file only. Reading a whole month means reading its versions. The negative
  report is the last file registered, so the head is v4 once the run is over, and the last good
  file is v3.
- The versioned schema route answers what the publication declared. The dataset service derives
  nothing from a CSV file and does not check the declaration against it. The declaration holds
  because the publication reads the cells by the rule of `csvCellValue`: a change to that Go
  rule must reach `reader_number`, or a column is typed by a rule the reader no longer follows.
- The three Pipeline outputs share one logical path, `part-00000.ndjson`. The file listing
  without `transaction_id` and the file index keep the newest file of a path. A version that is
  not the head is reached through `transaction_id` on the files listing, which names the files
  of the transaction that cut it. The file index does not reach it.
