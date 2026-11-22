# Pipeline Transform Catalog and Reusable Functions

The Pipeline Build Service (PBS) serves `pipeline_transform_catalog.v1` at
`GET /api/v1/pipelines/transforms/catalog`. It starts with the static transform
catalog and appends eligible definitions from the injected
`PipelineFunctionRegistry` under **Reusable functions**. The dynamic entries
vary with the registry and carry a function version. The static entries keep
their identity, order, and configuration. The product mapping counts static
transforms separately from registered function versions.
[Transform inventory](data-integration-wp6-1-transform-inventory.md) ·
[Catalog implementation](../../services/pipeline-build-service/internal/handler/transform_catalog.go)

| Entry | Catalog declaration | Execution requirement |
| --- | --- | --- |
| Static transform | Fixed entry with a transform type, editor configuration, and execution status. | The node type, configuration, and active runtime determine execution. |
| Expression UDF | `function.<name>@<version>` with `transform_type: function` and status `available`. Admission requires a parseable expression whose scalar calls and supported arities are recognized. | Lightweight build and local preview resolve the function in the injected registry. Evaluation still depends on the argument expressions, input columns, and row values. |
| Python UDF | Versioned function metadata with status `requires_python_sidecar`. | Reusable Python function execution currently returns `python_reusable_function_runtime_not_configured`, including when the sidecar is configured for static Python nodes. |
| Static Python transform | Fixed `python_transform` entry with `transform_type: python` and status `requires_python_sidecar`. | The build runner dispatches node source and inputs to the configured Python sidecar. |

[Lightweight function execution](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go) ·
[Build dispatch](../../services/pipeline-build-service/internal/handler/execution.go) ·
[Local preview](../../services/pipeline-build-service/internal/handler/preview.go)

The generated function config includes the ID, name, explicit version, argument
expressions, and target column. It defaults `function_auto_upgrade` to `false`.
The resolver owns version selection and receives the requested version and
auto-upgrade flag. Expression functions evaluate per input row and add or
replace the target column. Schema validation describes the output shape from
the node config. Registry resolution and evaluation happen at runtime.

The shipped PBS entrypoint currently leaves `ExecutionPorts.Functions` unset.
In that configuration the catalog serves the static list, and invoking a
reusable function returns `pipeline_function_registry_not_configured`. A registry
listing error returns `pipeline_function_catalog_failed` from the catalog
endpoint. A resolution error returns `pipeline_function_resolve_failed` from
execution. Production registry integration and Spark execution of these
function references remain unverified.
[Registry interface](../../services/pipeline-build-service/internal/handler/function_registry.go) ·
[PBS entrypoint](../../services/pipeline-build-service/cmd/pipeline-build-service/main.go)

## Regression checks

The static JSON snapshot detects changes to the standalone catalog. The dynamic
catalog test verifies that injecting a registry preserves the static list and
that definitions with missing or invalid expressions, unknown scalar calls,
or unsupported arities are excluded. The dispatch test takes the pinned config
from the catalog response and checks it through the build runner and HTTP
preview with a test registry. It also checks missing registry, unresolved
version, and Python UDF errors.
[Static catalog tests](../../services/pipeline-build-service/internal/handler/transform_catalog_test.go) ·
[Function catalog and dispatch tests](../../services/pipeline-build-service/internal/handler/pipeline_function_test.go)
