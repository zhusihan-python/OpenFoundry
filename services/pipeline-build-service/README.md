# `pipeline-build-service` (Go)

Build / execution side of Pipeline Builder. The Go service now mounts the
Rust route surface under `/api/v1/data-integration`, `/api/v1/pipeline/builds`
and `/v1`, while keeping the older `/api/v1/*` compatibility aliases for
callers that already moved to the Go namespace.

## Compatibility naming

Pipeline Builder public payloads should follow the frozen terminology in
[`docs/reference/foundry-compatibility-glossary.md`](../../docs/reference/foundry-compatibility-glossary.md):
`pipeline`, `pipeline_node`, `transform`, `build`, `job`, `dataset_output`,
`object_output`, and `link_output`. In short: use `id` for internal UUIDs,
`rid` for stable external resources, `transform_type` for node behavior, and
`output_dataset_id`/`output_dataset_rid` according to the API surface.

## Pipeline IR

Authoring CRUD now persists a stable `pipeline_ir.v1` graph under the existing
`pipelines.dag` JSONB column. The IR models nodes, ports, edges, resources,
inputs, outputs, transform config, output schema, preview schema, validation
state, and version metadata. Legacy DAG rows stored as `[]PipelineNode` remain
readable: `Pipeline.ParsedIR()` normalizes both shapes, while `ParsedNodes()`
keeps execution adapters isolated from the new authoring envelope.

New create/update payloads may send either `ir`, `dag`, or compatibility
`nodes`; handlers normalize them to the canonical IR before persistence and
reject duplicate node IDs, missing dependencies, invalid edge ports, cycles, and
unsupported IR versions.

## Port status

| Component | Status |
|---|---|
| Health (`/health`, `/healthz`) + Prometheus (`/metrics`) | ✅ |
| Rust route shape | ✅ route-audit reports no `missing`, `501`, or `empty-envelope` states for `pipeline-build-service` |
| Build resolution (`CreateBuild`, `DryRunResolve`) | ✅ wired through injected JobSpec, dataset-versioning, lock and build persistence ports |
| `/api/v1/data-integration` runs/build queue | ✅ backed by the production run repository when `DATABASE_URL` is configured; otherwise explicit `503` configuration errors |
| `/v1/builds`, `/v1/jobs`, `/v1/job-specs` | ✅ mounted and backed by build/job/log repositories where data access is required |
| SparkApplication `/api/v1/pipeline/builds/run` + status | ✅ mounted, Kubernetes-dispatched, and persisted in `pipeline_run_submissions` when `DATABASE_URL` is configured |
| Spark run list compatibility alias | 🟡 config-gated; returns recent `pipeline_run_submissions` rows instead of an empty envelope once the repository is configured |
| DAG executor / runtime dispatch | ✅ executor path accepts persisted plans or inline nodes; lightweight table transforms run through the existing `pipeline-expression` runtime, while Python, Spark, and job-runner dispatch remain injectable runtime ports |
| Lightweight / Faster pipeline type | ✅ `pipeline_type=FASTER` persists through authoring CRUD, accepts `LIGHTWEIGHT` aliases, tags execution plans with `preferred_runtime=lightweight_table`, and uses the existing OpenFoundry local table/expression runtime without DuckDB/DataFusion |
| Spark / Flink distributed pipeline type | ✅ `pipeline_type=DISTRIBUTED` persists with `distributed_config`; unchanged graph nodes dispatch through the `DistributedTransformRunner` port, Spark/PySpark submit SparkApplication CRs through existing Kubernetes wiring, and Flink remains an injectable runtime adapter with an explicit config-gated error until supplied |
| Python transform node | ✅ sidecar-backed `python` nodes receive upstream rows as `prepared_inputs`, enforce optional package allowlists, clamp timeouts, capture stdout/stderr, and publish `result_rows` for downstream output commits |
| LLM node and AIP generation | ✅ `llm` nodes dispatch through the configured AI service, publish generated values as normal table columns, and `/api/v1/pipelines/{id}/aip/generate` appends provider-generated transform nodes to the graph with preview feedback |
| Reusable functions / UDF nodes | ✅ versioned registry functions appear in the transform catalog; expression UDF nodes execute through the lightweight runtime with optional `function_auto_upgrade`; Python function catalog entries remain sidecar-gated separately from Python transform nodes |
| Pipeline authoring lifecycle | ✅ draft DAGs, published DAGs, branch names, proposal state, version history, publish, and restore are persisted through `pipeline_versions`; builds prefer the published graph when present |
| Build orchestration lifecycle | ✅ pipeline runs persist `queued`, `running`, `succeeded`, `failed`, and `cancelled`; each run stores rich `node_results` with transition events, attempts, row counts, schema deltas, output resources, and log RIDs for the run detail UI |
| Dataset output commits | ✅ non-Iceberg dataset outputs POST successful runtime rows to `dataset-versioning` with inferred schema, preview rows, file metadata, and lineage before marking `job_outputs` committed |
| Ontology object/link outputs | ✅ object outputs commit a backing dataset, create/update ontology-definition object types and properties, and materialize rows into object-database bridge objects; link outputs deploy/update link types and write link rows through object-database |
| Logs | ✅ list/SSE/emit/ws surfaces are mounted; history/emit/ws paths are config-gated on live log store/subscriber wiring |
| Iceberg output client (ADR-0041) | 🟡 config-gated by `FOUNDRY_ICEBERG_CATALOG_URL`; boot warning remains intentional |
| Legacy pipeline authoring CRUD aliases | ✅ mounted through the repository-backed authoring lifecycle when `DATABASE_URL` is configured; otherwise explicit `503` configuration errors |

LLM node previews honor `max_rows`. A build processes the complete input up to 50 rows per node. Larger inputs fail before the first model request. Empty inputs retain their columns and produce zero rows. An independent prompt with no upstream node generates one row. The `boolean` and `double` output types write typed cells. The `json` output type writes validated JSON text to a string column that can feed a JSON extraction step.

AIP generation proposes new nodes for a draft or saved graph. The model connects transforms to existing graph node IDs through `depends_on`. Any explicit dataset ID must already be present in the graph in the same input or output role. PBS validates the proposal and returns a preview or a runtime preview diagnostic. The author chooses whether to apply the nodes.

Use `openfoundry-go/docs/migration/route-parity-audit.md` as the generated
source of truth for route-shape parity. Productive handlers either execute real
repository/runtime work or return a machine-readable configuration error that
names the missing adapter.

## Manual build outputs

A successful MANUAL or FORCE build confirms publication of its selected datasets.
To publish ontology objects or links, run the graph with
`POST /api/v1/pipelines/{id}/runs`. During resolution the service checks the
requested datasets and every output of their selected JobSpecs against the
pipeline graph. A mixed graph can build its ordinary dataset targets independently.

Resolution checks the declared dataset output projection before reserving outputs.
For an older resolved build whose saved projection is invalid, execute returns
409 with `dataset_output_projection_invalid`. Abort that build to release its
reservations, publish the corrected JobSpec, and create a new build.

Execution uses the published JobSpec RID and output reservations saved with the
build. This also applies to older pending builds. A historical JobSpec that
described only a dataset cannot establish whether an earlier graph declared
additional ontology work. Its completion receipt covers the dataset publication.

## Build & run

```sh
go build -o bin/pipeline-build-service ./services/pipeline-build-service/cmd/pipeline-build-service
go test ./services/pipeline-build-service/...
```

## Configuration

| Variable | Default |
|---|---|
| `HOST` | `0.0.0.0` |
| `PORT` | `50081` |
| `JWT_SECRET` | (required for authenticated route groups) |
| `DATABASE_URL` | unset; repository-backed handlers return explicit `503` until configured |
| `DATA_DIR` | `/var/lib/openfoundry/pipeline-build` |
| `DATASET_SERVICE_URL` | `http://localhost:50079` |
| `DATASET_SERVICE_BEARER` | unset; forwarded as `Authorization: Bearer` for dataset output commits when configured |
| `ONTOLOGY_DEFINITION_SERVICE_URL` | `http://localhost:50103` |
| `ONTOLOGY_DEFINITION_SERVICE_BEARER` | unset; forwarded as `Authorization: Bearer` for ontology object output deploys when configured |
| `OBJECT_DATABASE_SERVICE_URL` | `http://localhost:50104` |
| `OBJECT_DATABASE_SERVICE_BEARER` | unset; forwarded as `Authorization: Bearer` for object output materialization when configured |
| `WORKFLOW_SERVICE_URL` | `http://localhost:50080` |
| `AI_SERVICE_URL` | `http://localhost:50127` |
| `PIPELINE_LLM_PROTOCOL` | `ai-service` by default. Set to `agent-runtime` for LLM nodes and AIP-assisted generation calling the authenticated chat route of the Go agent-runtime-service at `AI_SERVICE_URL`. |
| `AI_SERVICE_BEARER` | unset; forwarded as `Authorization: Bearer` for LLM nodes and AIP-assisted generation when configured |
| `PIPELINE_LLM_PURPOSE_JUSTIFICATION` | Explicit business purpose for calls made by this deployment’s PBS service identity to agent-runtime, including generation and LLM rows. Set a purpose appropriate for the workload. The seeded policy requires at least 20 characters and an eligible provider route. Empty by default. |
| `STORAGE_BACKEND` | `local` |
| `STORAGE_BUCKET` | unset |
| `S3_*` | unset |
| `LOCAL_STORAGE_ROOT` | unset |
| `DISTRIBUTED_PIPELINE_WORKERS` | `4` |
| `DISTRIBUTED_COMPUTE_POLL_INTERVAL_MS` | `1000` |
| `DISTRIBUTED_COMPUTE_TIMEOUT_SECS` | `1800` |
| `SPARK_NAMESPACE` | `openfoundry-spark` |
| `PIPELINE_RUNNER_IMAGE` | `openfoundry/pipeline-runner:dev` |
| `KUBERNETES_API_URL` / in-cluster service env | unset; SparkApplication handlers return explicit `503` until kube wiring is available |
| `KUBERNETES_BEARER_TOKEN` | unset |
| `FOUNDRY_ICEBERG_CATALOG_URL` | unset (boot-time warning matches Rust) |
| `FOUNDRY_ICEBERG_CATALOG_BEARER` | unset |

## Tenant isolation (P1.1) — capability boundaries

- Pipelines, versions, runs, builds, jobs, job logs and job specs are
  tenant-isolated from the verified JWT (OrgID → organization domain;
  no OrgID → personal domain with owner isolation). Lists, id-based
  lookups (UUID and RID), subresources and live log streams all enforce
  the boundary in SQL.
- **SparkApplication submission is NOT supported for tenant-scoped
  callers** (`POST /api/v1/data-integration/spark-runs`,
  `POST /api/v1/pipeline/builds/run`): the runner does not yet carry a
  constrained tenant identity, so scoped submissions fail closed with
  503 `spark_submission_tenant_unsupported` before any Kubernetes side
  effect. Status/list queries stay tenant-mapped. Distributed execution
  under tenants lands with P2/P4.
- Build resolution helpers that read the co-located dataset schema
  (branch list / transaction open / view schema) fail closed for scoped
  callers until the dataset schema exposes its org columns; route
  through the dataset service HTTP API in split-database deployments.
