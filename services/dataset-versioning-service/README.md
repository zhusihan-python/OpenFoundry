# dataset-versioning-service (Go)

This Go service is the incremental port of the Rust `dataset-versioning-service`.

## Port status

Implemented end-to-end verticals:

- Dataset CRUD (`/api/v1/datasets`).
- Dataset versions (`/api/v1/datasets/{id}/versions`).
- Dataset branches (`/api/v1/datasets/{id}/branches`).
- Files API + backing filesystem presigning:
  - `GET /api/v1/datasets/{id}/files` lists the persisted logical path to physical URI mappings from `dataset_files`, with branch cutoff and prefix filtering.
    Without `transaction_id` it keeps one file per logical path, the newest, and the request is read as before. With `transaction_id` it lists the active files that one committed transaction of the branch holds, in logical path order, so a version that is no longer the head stays downloadable by the transaction that cut it. `prefix` is then text: `_` and `%` in it are those characters.
    The transaction must be committed on that branch of that dataset and before the head of the branch, as the head listing ends at the head. A commit of a parent branch is not one of a branch created from it, as for `rows:scan`: the branch inherits the files to read, not the commit. A query that names the filter, with its name as written or encoded, is read strictly and a malformed value is `400`. Anything else is `404` and never falls back to the head.
  - `GET /api/v1/datasets/{id}/files/{file_id}/download` returns a temporary backing-filesystem URL for active files and rejects soft-deleted files with `410 Gone`.
  - `POST /api/v1/datasets/{id}/transactions/{txn}/files` validates an OPEN transaction and returns a transaction-scoped presigned upload URL plus the canonical `physical_uri`.
  - The default production wiring uses the local backing filesystem presigner in `libs/storage-abstraction`; set `DATASET_FILES_BASE_URL` and `DATASET_FILES_BASE_DIR` to control generated URLs.

Still pending after this slice:

- Dataset quality persistence/API.
- Views API and view-specific preview/schema endpoints.
- Retention worker physical purge loop.
- Full Iceberg writer/catalog integration beyond local backing-file presigning.
