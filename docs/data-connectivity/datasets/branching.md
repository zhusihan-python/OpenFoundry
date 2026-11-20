# Datasets — Branching

Branches are isolated views of a dataset's history. They share storage (file paths), but each branch keeps its own pointer to a `head_transaction_id`; commits on one branch never alter another's view.

## Default branch

Every dataset is created with a single branch named **`master`**. Subsequent branches inherit from a parent (defaulting to `master`) and start with the parent's HEAD as their initial pointer.

## Creating a branch

```http
POST /v1/datasets/{rid}/branches
Content-Type: application/json

{
  "name": "feature/new-schema",
  "parent_branch": "master",
  "description": "trial new column types"
}
```

Two parent-selection strategies are mutually exclusive:

* **`parent_branch`** — the new branch tracks the parent's current HEAD.
* **`from_transaction`** — pin the new branch to a specific historical transaction (its branch becomes the parent).

Omit both for a **root branch** (HEAD = NULL until the first commit).

## Reparenting

```http
POST /v1/datasets/{rid}/branches/{branch}:reparent
Content-Type: application/json

{ "new_parent_branch": "master" }
```

Pass `null` (or omit the field) to make the branch a root. The branch may not become its own parent.

## Soft-deletion

```http
DELETE /v1/datasets/{rid}/branches/{branch}
```

Children are re-parented to the branch's grandparent (which may be NULL → they become roots) before the branch is marked `deleted_at`. The unique-name slot on the dataset is freed via the partial unique index `uq_dataset_branches_dataset_id_name_active`, so the same name can be reused immediately.

## Fallback chains

Each branch may declare an ordered list of fallback branches; readers walk the chain when the branch itself is missing data for a path.

```http
PUT /v1/datasets/{rid}/branches/{branch}/fallbacks
Content-Type: application/json

{ "chain": ["staging", "master"] }
```

A fallback resolution emits `dataset_branch_fallback_resolutions_total{outcome="hit|miss"}`.

## Store CSV options with a published version

Inline `outputs:commit` accepts CSV options in `schema.custom_metadata.csv`
for a `TEXT` schema. The publication retains the effective options in the
committed transaction's schema metadata. Later publications have their own
configuration. You can inspect the stored configuration through the transaction
endpoint for the version.

| Option | Meaning |
| --- | --- |
| `delimiter` | A single CSV separator. Omission preserves the format default (comma for CSV, tab for TSV). |
| `headerRow` | One-based record number for the header. Zero denotes no header. If omitted, `header: true` resolves to one and `header: false` resolves to zero. |
| `charset` | `UTF-8` (default), `UTF-16LE`, or `UTF-16BE`. |
| `bom` | `strip` (default), `require`, or `forbid`. |

Version- and timestamp-pinned preview read the options from the selected
transaction. CSV and TSV snapshots without saved options use a first-record
header, UTF-8 and optional matching BOM. A header record is counted by CSV
records, including quoted multiline fields as one record. A headerless file
uses `column_1`, `column_2`, and so on. The preview uses standard CSV
double quoting and the existing cell conversion rules.

The preview opens one local CSV or TSV ADD file per page, up to 32 MiB per
file. Each continuation resumes at a logical record of that same snapshot.
Malformed encoding, an incompatible BOM, and a missing file produce an error
for the selected version. A CSV preview rejects snapshots containing another
file format. Fixed-version scans at `files/rows:scan` use the same
decoder and support either `all_files=true` or an explicit CSV/TSV `path`,
with the existing equality predicate. Pipeline execution reads the decoded
rows through this endpoint.

For pinned CSV input, the pipeline node preview reports the inferred schema.
Batched copy to NDJSON applies the same inference to the executed rows and
saves the schema with the output transaction. Use the output version's
transaction endpoint to read `metadata.schema`. Integers and decimals in one
column infer `DOUBLE`. Other type conflicts infer `STRING`. A missing or null
value makes its field nullable. A field with only a header or nulls infers
nullable `STRING`. Columns are sorted by name. The inference retains statistics
for at most 256 columns and 64 KiB of column names across all files.

## Read a pipeline artifact manifest

A successful single-output batched build stores its manifest in
`metadata.artifact_manifest` on the output transaction. Read the dataset's
version list to find the version's `transaction_id`, then read that
transaction. The manifest's own `version` identifies the manifest format.

The input entry records the dataset, branch, version and transaction scanned
by Pipeline Build Service. It includes the selected path when present, and a
SHA-256 of the configured predicate text when used. The job spec identity and
content hash come from the build's frozen configuration. Dataset Versioning
Service verifies the source pin in the caller's tenant and supplies the
output transaction, path, format, record count, byte size and digest from
the sealed publication. The manifest is published in the same transaction as
the output file. A retry of the same commit uses its original receipt.

## Flatten nested JSON in a pinned pipeline

Set `dataset_snapshot.flatten_json` to `true` in a pinned dataset input node
to preview the expanded rows and inferred schema. For a published batched
job, use `logic_payload.transform_type: "batched_json_flatten"` with a pinned
NDJSON input and an `.ndjson` output. The output transaction stores the
inferred schema in `metadata.schema`.

Non-empty objects expand into dot-separated paths. A literal dot or
backslash in a field name is escaped with a backslash, so `customer.name`
and the literal key `customer.name` in the original record produce
distinct columns (`customer.name` and `customer\.name`). Arrays keep their
values and infer `ARRAY`. Empty objects keep their values and infer
`STRUCT`. The scalar inference, missing or null values, column ordering
and schema budgets follow the rules above. Out-of-range numbers keep their
JSON values and infer `STRING`. Expansion rejects empty or
duplicate keys in expanded objects and limits object nesting to 32 levels.

## Preview a snapshot at a timestamp

Use the dataset preview API with a branch and an RFC 3339 timestamp.

```http
GET /v1/datasets/{rid}/preview?branch=main&ts=2026-11-19T08:20:00Z&limit=100
```

The service selects the latest committed dataset version on that active branch
at or before the timestamp, then reads its pinned transaction. When commits
share a timestamp, the higher version wins. The response includes the resolved
`version` and `transaction_id`. Use the same timestamp or the returned version
with `next_cursor` for the next page. The cursor remains bound to the resolved
version and its transaction.

The timestamp and `version` selectors are mutually exclusive. A timestamp
before the first published version returns HTTP 404. Open and aborted
transactions do not participate. An unreadable selected version returns an
error from the pinned reader. Preview supports the same formats and limits
as other version-pinned reads.

## Authorization

All four mutating endpoints (`create_branch`, `delete_branch`, `:reparent`, `PUT /fallbacks`) require `dataset.write`. Read endpoints (`GET .../branches`, `GET .../branches/{branch}`) are open to any authenticated caller.

Denials are counted in `dataset_rbac_denials_total{operation=…}` with operation labels:

* `branch.create`
* `branch.delete`
* `branch.reparent`
* `branch.fallbacks.update`

## Audit

Each successful mutation emits a structured audit record under `target = "audit"`:

| action                      | extra fields                                       |
| --------------------------- | -------------------------------------------------- |
| `branch.create`             | `branch`, `parent_branch_id`, `initial_head`       |
| `branch.delete`             | `branch`, `branch_id`                              |
| `branch.reparent`           | `branch`, `new_parent_id`                          |
| `branch.fallbacks.update`   | `branch`, `chain`                                  |

## See also

* [transactions.md](./transactions.md)
* [retention.md](./retention.md)

## Publish a manual output group

DVS accepts `POST /v1/datasets/outputs:commit-group` for manual canonical
builds with existing output reservations. The request contains a `members`
array of 2 to 16 elements. Each element supplies a `dataset_rid` and an
`output` object in the keyed inline snapshot commit format. Use a distinct
dataset per member and reservations from the same build and caller. The inline
content limit applies to the entire request.

DVS commits the group as one PostgreSQL transaction and returns a
`receipts` array in request order. A fresh commit returns HTTP 201. An
exact-byte replay returns HTTP 200 with the original receipts. An existing
key bound to other bytes or an incomplete receipt set returns HTTP 409.
A rejected member leaves both committed versions and indexes unchanged.
The service removes files created by the request on a known rollback.
If the database commit outcome is unknown, DVS returns HTTP 503 and retains
the files. Retry with the exact request bytes.

This endpoint uses the local backing filesystem. PBS orchestration must
prepare every output before calling it.

## Read an XLSX snapshot in a pipeline

A manual build can read one registered XLSX file from a committed snapshot and
publish normalized NDJSON. On a `dataset_input` node, set `dataset_path` to the
workbook's logical path. In `dataset_snapshot`, supply `branch`, `version`,
`transaction_id`, and an `excel` object. The object selects one worksheet,
the physical header row and the source columns to keep and rename.
Pipeline Build uses the internal Dataset Versioning endpoint for this read.

```json
{
  "dataset_path": "orders.xlsx",
  "dataset_snapshot": {
    "branch": "main",
    "version": 1,
    "transaction_id": "11111111-1111-1111-1111-111111111111",
    "excel": {
      "sheet": "订单",
      "header_row": 3,
      "mapping": [{"source_index": 0, "target": "order_no"}]
    }
  }
}
```

Source column indexes are zero-based. An omitted or null `header_row` reads
the sheet without a header. Normalization retains strings, including numeric
text, and represents blank cells as null. A mapped formula requires a cached
value. The parser accepts plain XLSX, bounds compressed and decompressed
workbook sizes, and rejects sheets with more than 1,000 data rows. For pinned
reads, a smaller node row limit also rejects the input. Workbook reads do not
change the source dataset or create an import. The published output uses the
ordinary single-output manual commit path.
