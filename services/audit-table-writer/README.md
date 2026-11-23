# Audit table writer

This HTTP service accepts Go audit-sink batches for `of_audit.events` and writes Parquet through the Iceberg REST catalog. Run it on a private network reachable by audit-sink. The append route accepts audit data from its caller. Restrict ingress to the sink using the deployment's service identity and network controls.

Build from the repository root:

```sh
docker build -f services/audit-table-writer/Dockerfile -t openfoundry/audit-table-writer:local .
```

Configure `CATALOG_URI` with the Iceberg REST base (for example `http://lakekeeper:8181/catalog`), `ICEBERG_WAREHOUSE` with the target warehouse, and `S3_ENDPOINT`, `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for the S3-compatible store. Inject credentials through the runtime environment. Every replica must address the same table. The warehouse and object-store address must be reachable from the containers. For an existing warehouse, initialize the fixed [audit table schema](../../docs/migration/audit-sink-iceberg.md) once with the owner credentials. The command also checks an existing table's schema, partition spec and sort order.

```sh
docker run --rm --network openfoundry --env-file "$WRITER_ENV_FILE" \
  openfoundry/audit-table-writer:local python writer.py create-table
```

Start the service with its own catalog and object-store write credentials. Set `CATALOG_TOKEN` when the catalog requires a bearer token.

```sh
docker run --name audit-table-writer --network openfoundry --env-file "$WRITER_ENV_FILE" \
  openfoundry/audit-table-writer:local
```

Set the sink's `ICEBERG_CATALOG_URL` to `http://audit-table-writer:8080` on that network and set its `ICEBERG_WAREHOUSE` to the writer's warehouse. The sink appends at `POST /openfoundry/iceberg/v1/append`. Only a successful response permits it to advance the Kafka offset. To verify the image with two instances, run the [concurrent audit delivery smoke](../../tools/smoke/audit-delivery/README.md#two-docker-writers).

The request limit is 100,000 rows and 64 MiB. Size the sink batches accordingly. Both exact retries and mixed new/repeated batches are supported. A reused ID with different stored fields rejects the entire batch. Writers prune candidate files by UUID bounds and check matching committed rows on the same table snapshot they append against. A competing snapshot commit triggers a bounded reload and a fresh comparison. An unknown commit outcome or storage unavailability returns 503, leaving the caller free to retry. Failed commits can leave unreferenced data files; retain the warehouse's normal orphan-file maintenance schedule.

Run the HTTP contract tests from the repository root against the built image:

```sh
docker run --rm --network none \
  -v "$PWD/services/audit-table-writer/tests:/opt/writer/tests:ro" \
  openfoundry/audit-table-writer:local python -m unittest discover -s tests -v
```
