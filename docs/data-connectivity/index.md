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
