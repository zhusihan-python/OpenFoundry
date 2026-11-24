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
   registration and the `fileId` are kept in the transaction metadata.
5. A reported no-data month is written as one JSON record to a separate status dataset. It never
   touches the report dataset.
6. An independent reader, who has no knowledge of the staged file or of the receipt, resolves each
   registration by version and transaction and confirms the bytes, the SHA-256 and the length.

## What is real, what is assembled, what is pending

- Real, read from the pinned source: the field names above and the offline transaction_view rules
  in `report-check.ts` that the CSV files follow (required columns, an `All amounts in USD` line,
  equal column counts, amount columns that sum to `total`, dates inside the month,
  marketplace `amazon.com`). The unmodified upstream `checkReport` accepts all three files.
- Assembled by this test: the three CSV files and every identifier in them, the store UUID, the
  `fileId` values, the envelope layout, the object keys and the handoff rules.
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

The evidence directory holds `amazon-report.json`, every staged envelope, the three published CSV
files and the no-data record.

## Known limits

- The dataset service checks the length of inline content but not its hash. The handoff refuses a
  hash mismatch and the reader confirms the hash; the service alone would not.
- Downloads are served as `application/octet-stream`. The content type is read from the head file
  index, which is why it is asserted when a registration is the head.
- The file id in a commit receipt and in the file index is not accepted by downloads. Downloads
  use the id of the listing entry that is bound to the transaction.
- Each registration is its own snapshot version of the report dataset, so the head holds the
  latest registered file only. Reading a whole month means reading its versions.
