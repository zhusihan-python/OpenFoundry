# Checkpoint audit delivery

Run from the repository root on Docker Desktop with Docker Compose v2 and Python 3.10 or later. Containers must be able to reach the host's loopback server through `host.docker.internal`. The fixture publishes the policy and Connect endpoints on random loopback ports.

```sh
python3 -m venv /tmp/of-audit-venv
/tmp/of-audit-venv/bin/python -m pip install -r tools/smoke/audit-delivery/requirements.txt
/tmp/of-audit-venv/bin/python tools/smoke/audit-delivery/run.py --output /tmp/of-checkpoint-delivery.json
```

Choose a fresh evidence filename for each run. The build uses the checked-out source with Go 1.25.14 in Docker. To reuse existing Linux Go caches, set `AUDIT_GO_MOD_CACHE` and `AUDIT_GO_BUILD_CACHE` to their host directories.

The runner starts a unique Compose project. It renders the development foundation profile and uses its PostgreSQL owner bootstrap, Kafka Connect worker, and checkpoint connector registration container. It verifies that the authorization service and connector address the same database. The smoke uses a one-partition Kafka topic and random credentials. After the slot becomes active, it submits authenticated allow and deny requests, matches each receipt and its outbox changes in committed WAL, and reads each event at its native Kafka offset.

The Go audit sink writes through its HTTP writer to a PyIceberg SQL catalog backed by temporary SQLite and Parquet files. An independent process opens the table after each phase. The second event receives an injected HTTP 503 before append. The runner checks the actual sink exit status, unchanged snapshot and committed offset, then restarts the sink and verifies the second event, its original ID, both row payloads and the final consumer position.

The JSON report includes the source revision and dirty state, component versions, event IDs, offsets, snapshot IDs and cleanup status. The temporary catalog and the project's Docker resources are removed at exit. A passing run covers this local writer endpoint and the pre-append failure. A deployed writer, its catalog and its credentials require their own environment acceptance.
