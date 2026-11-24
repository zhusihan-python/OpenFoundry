# Data connectivity & integration

This section covers how OpenFoundry connects to source systems, ingests data, and moves it into operational platform flows.

## OpenFoundry mapping

- `services/data-connector`
- `services/dataset-service`
- `services/pipeline-service`
- `services/streaming-service`
- `proto/data_integration/*`
- `proto/dataset/*`
- `proto/pipeline/*`
- `proto/streaming/*`

## Key concerns

- connection management
- sync orchestration
- dataset creation and versioning
- batch and streaming ingestion
- storage and archival integration

## S3 order snapshots

Create an S3 source, bind its read credential, and run a snapshot sync to publish a dataset version. Downstream pipelines can pin that version and independently download the result after the source changes. [Source configuration](../../services/connector-management-service/README.md#s3-snapshot-sources) · [Docker acceptance and sample orders](../../tools/smoke/data-integration-wp2-3/README.md)

## MySQL order snapshots

A MySQL 8.4 source can publish successive InnoDB order snapshots. A downstream reader can pin the first version, download its pipeline output and verify its rows after the source changes. [Reproducible MySQL acceptance](../../tools/smoke/data-integration-wp2-4/README.md)

## PostgreSQL order snapshots

PostgreSQL table snapshots can publish two versions of the same order table and feed a version-pinned downstream pipeline. The connector reads one primary-keyed table using a source-scoped login and verified TLS. [Reproducible PostgreSQL acceptance](../../tools/smoke/data-integration-wp2-5/README.md)

## PostgreSQL order CDC

A single-partition Debezium feed can publish an initial order snapshot and subsequent insert, update and delete versions through the dataset service. A reader can pin any published version, compare its order rows and download the transaction's file. [Docker acceptance and worker scope](../../tools/smoke/data-integration-wp1-5/README.md)
