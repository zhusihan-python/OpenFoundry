# WP3.2 isolated canonical runtime acceptance

These are the 2026-11-17 round's actual HTTP/PG/Kafka acceptance helpers. The
producer `run.sh` and its PASS remain a separate layer. The canonical helpers
use the dedicated kind kubeconfig `/private/tmp/ofwp32-acc/kubeconfig`, namespace
`ofwp32`; they never use the current kubectl context. `OFWP32_EVIDENCE` selects
the evidence directory (default `/private/tmp/ofwp32-runtime-sol-evidence`).

## Runtime

`k8s/80-canonical-services.json` deploys fresh DVS/PBS services and task-owned
PVCs. DVS uses a separate `ofwp32_dvs` database on this fixture's PostgreSQL;
PBS uses `ofwp32`, the same database as tick/CDC. PBS's Dataset adapter uses
`http://ofwp32-fault-proxy:8080`, which forwards real authenticated HTTP to
DVS. `k8s/81-fault-proxy.json` is the test-only proxy, with independent evidence
PVC. All keys in these manifests are synthetic fixture values. Test leases
are 15 seconds, heartbeat 5 seconds; production defaults are unchanged.

Runtime images contain newly compiled linux/arm64 binaries. The final PBS
binary is core v3, SHA256
`c384b635c151a9cc130b85a41d39f18cd65a6e8af219c5c74f518fc4aed3d4a9`.
The runtime image Dockerfile is a distroless nonroot image with `COPY service
/service`, `WORKDIR /home/nonroot`, and `ENTRYPOINT ["/service"]`. The evidence
contains build/load logs, docker-save tar, and the binary/config/Pod identity
chain. DVS was compiled from unchanged HEAD source with Go1.26.3, one CPU,
1GiB memory, `GOMAXPROCS=1`, `CGO_ENABLED=0`, `go build -mod=readonly
-buildvcs=false -p 1`. Only go.mod/sum and source are inputs; no .env is read.

The Kafka fixture stores logs in its existing container, without a PVC.
Broker fault tests preserve its Pod/container and use exact STOP/CONT;
deleting or scaling down that Kafka Pod would destroy test history.

## Fresh HTTP seed

Mint fresh test org/personal JWTs with the existing P1 `mint_token.py`, using
the synthetic manifest JWT key. `canonical_seed.py` takes `PBS`, `DVS`, `TOKEN`
environment variables and creates datasets, branches, real CSV upload/schema
commit, pipeline, and formally published TRANSFORM JobSpec. Its canonical
`logic_payload` uses `transform_type:dataset_input` and `dataset_rid`; the
expected output is the exact two rows `A,2` and `B,5`. HTTP status/body evidence
is stderr; stdout is the seed handle JSON. No Authorization header is logged.

Scheduling grant/definition creation is through the real scheduling API.
The initial minute schedule ran automatically on v3 at 09:02/09:03 UTC and
again after actual PBS restart at 09:04 UTC. The round's precise seed/token
files are under the evidence directory. Tokens remain untracked local test
credentials; evidence reports do not print them.

## Case helpers and evidence

Fault cases use enabled annual definitions and inject matching contract events
into the real Kafka topic. Their scheduled opportunity, deterministic event ID,
tenant, revision and grant reference/generation are recorded. This prevents
unrelated automatic ticks during a fault case; these injections are consumer
fault acceptance, not evidence of extra automatic CronJob periods. The counted
minute-CronJob periods and concurrent real tick Jobs are verified separately.

- `canonical_acceptance.py duplicate`: two concurrent exact successful-event
  replays through Kafka. Requires zero consumer lag and unchanged PG counts
  and real DVS versions.
- `revocation_acceptance.py`: org/personal old-ref and stale-generation
  rejection, no Build or product, plus same-UUID cross-kind HTTP isolation.
- `failure_acceptance.py`: actual scheduled unsupported-transform failure,
  unchanged versions and exact old product rows, valid spec restored in
  `finally`.
- `lost_ack_acceptance.py`: gzip-aware proxy drops a real COMMITTED ACK and
  holds replay; actual PBS stop/restart must recover the original bytes/key/
  transaction with no extra version. See `FAULT_PROXY.md` for controls.
- `hold_acceptance.py` (default Forbid, `revoke` for org/personal): requires
  actual `hold_started`, executing dispatch, live lease/activity, release,
  and normal upstream rows HTTP200. Revocation must fail at an authorization
  gate, with unchanged versions; timeout/transport failure is not accepted.
- `admission_crash_acceptance.py`: real PG advisory barriers, actual Pod kill,
  redelivery/recovery in admission before-commit and after-commit windows.
- `tick_coalesce_acceptance.py`: overdue next_run_at is an explicit test seed;
  audit trigger holds the first real tick after acquiring its definition
  lock. A second actual Job must finish with parsed JSON `fired:0`; first
  finishes `fired:1` after release. Exactly one old opportunity/audit/product
  and a future next due prove coalescing. `finally` releases the holder before
  dropping its trigger and pauses only its test definition.
- `poison_acceptance.py`: isolated malformed Kafka messages reach a durable
  five-attempt budget. A test SQL barrier forces a DLQ transaction failure,
  or holds the DLQ transition while the existing broker process is stopped.
  The ACK-fault fixture uses only the Kafka Pod network namespace and a rule
  for the old PBS IP/9092 flow, installed before barrier release; it keeps
  that rule until the replacement PBS re-reads the original offset. The
  broker Pod is preserved. Each case must prove stable poison identity,
  one logical DLQ, no sixth application attempt, actual PBS restart, and a
  succeeding healthy event/product in the same partition. `finally` restores
  the broker and removes only its own SQL/network controls. Failed fault
  attempts remain failed evidence, not proof of the final window.
- `cycle_products_acceptance.py`: read-only linkage of the counted v3 UTC
  opportunities, deterministic Build, persisted receipt, real DVS COMMITTED
  transaction/version/view and exact historical file bytes. Runtime output
  files are served over the existing signed local-FS HTTP protocol using the
  synthetic fixture signing key; legacy `files/{id}/download` does not index
  these runtime transaction file IDs. Each 40-byte NDJSON file must hash to
  its receipt SHA and equal original request `content_base64` bytes.

Each helper exits nonzero on a failed business assertion. Keep raw failed
logs; create distinct case keys/artifact prefixes for a corrected run. These
are explicit round fixtures, including fixed counted timestamps and handle
files, rather than a generic environment-independent test suite. While a
case uses actual PBS/Kafka faults, grant it the exclusive shared fault slot;
other workers may only inspect evidence until it releases the slot.
