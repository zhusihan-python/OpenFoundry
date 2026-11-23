# agent-runtime-service

Hosts the agent runtime API and OpenAI-compatible chat/copilot endpoints backed by `libs/ai-kernel-go` domain runtimes.

## Environment

| Variable | Required | Description |
| --- | --- | --- |
| `DATABASE_URL` | yes | PostgreSQL connection used for agent runtime state and migrations. |
| `JWT_SECRET` | yes | Shared JWT signing secret for authenticated `/api/v1/agent-runtime/*` routes. |
| `LLM_PROVIDER_BASE_URL` and `LLM_PROVIDER_MODEL` | optional pair | OpenAI-compatible inference base URL (including its API version prefix) and served model for chat and copilot. |
| `LLM_PROVIDER_TOKEN` | optional | Bearer token sent to the configured inference endpoint. |
| `KAFKA_BOOTSTRAP_SERVERS` | no | Kafka bootstrap servers for the future `ai.events.v1` producer wiring. |
| `AUTHORIZATION_POLICY_SERVICE_URL` | yes, with a real provider | Internal base URL for the authorization-policy service. |
| `PURPOSE_CHECKPOINT_URL` | no | Fallback base URL for the same service. |
| `PURPOSE_CHECKPOINT_BEARER` | yes, with a real provider | Dedicated service JWT for the runtime to submit purpose decisions. Grant only `purpose-checkpoint:enforce` and identify it as a service credential. |
| `LLM_PROVIDER_NETWORK_SCOPE` | no | Operator-declared network scope for the actual inference endpoint. Defaults to `public`. Configure `local`, `private`, or `hybrid` only when the deployment meets that scope. |

Chat and copilot obtain a recorded purpose decision before invoking the configured model. The audit actor is the authenticated caller of agent-runtime. For Pipeline Builder this is its configured service identity. Configure a workload-specific purpose on PBS. The seeded chat policy requires an eligible private-network route and a purpose justification. A denial returns 403. An unavailable checkpoint or invalid approval returns 503 before inference. Configure the internal checkpoint endpoint and provision its JWT through the deployment's secret store.
