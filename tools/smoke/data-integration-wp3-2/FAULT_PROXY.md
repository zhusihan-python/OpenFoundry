# Test-only DVS fault proxy

This tool is for isolated WP3.2 smoke fixtures. It adds no production endpoints or dependencies. Bind its control port only in the isolated test namespace.

```sh
python3 fault_proxy.py --listen 0.0.0.0 --port 8080 \
  --upstream http://ofwp32-dvs:50078 --evidence-dir /evidence --timeout 30
```

Use a Python 3 image (for example the test environment's existing Python image), mount this script read-only and a run-specific writable `/evidence`. Expose service `ofwp32-fault-proxy:8080` only to the test namespace. Set test PBS `DATASET_SERVICE_URL=http://ofwp32-fault-proxy:8080`. Preserve existing DVS/PBS credentials: the proxy forwards the original Authorization header without logging it.

Arm one lost ACK with `POST /__faults/drop-next` and JSON:

```json
{"method":"POST","path":"/v1/datasets/ri.foundry.main.dataset.UUID/outputs:commit","key":"sch:EXACT-KEY"}
```

Path and method match exactly. Omit `key` (or use an empty string) for the next matching commit. An existing armed rule returns 409. The proxy first forwards the request and reads the entire upstream response. Only a 2xx JSON response whose transaction has `status=COMMITTED` and a nonempty `id` consumes the rule; then it logs the transaction and closes the downstream connection without sending any response headers or ACK bytes. Upstream errors and non-COMMITTED responses pass through and leave the rule armed. The next matching request passes through normally. Request bodies are forwarded byte-for-byte, with original Authorization; redirects are not followed.

For an execution hold, `POST /__faults/hold-next` with:

```json
{"method":"GET","path":"/v1/datasets/EXACT-DATASET/EXACT-ROWS-PATH"}
```

Use the actual DVS read path observed in the fixture. The next exact match waits before contacting DVS, logs `hold_started`, then `POST /__faults/release` with `{}` releases it. Waits are bounded by `--timeout`; an unreleased hold responds 504. Other requests continue normally. This permits a second tick to encounter Forbid while the first run retains its activity slot. Controls are test-only, unauthenticated, and must remain isolated.

`events.jsonl` contains method, path, key, request SHA256, upstream status, confirmed transaction ID, action and timestamp. Numbered `.request.bin` files preserve original request body bytes for replay comparisons; they contain dataset content, never request headers. Protect these test artifacts as dataset evidence. No bearer/header dumps or upstream error bodies appear in logs.

Run the independent real-HTTP regression:

```sh
python3 tools/smoke/data-integration-wp3-2/test_fault_proxy.py
```

It proves upstream commit before the first downstream disconnect, a successful second request with identical bytes/key/bearer and only one logical committed key, token-free event logs, and a read held before upstream contact until explicit release. Runtime must additionally connect this proxy to real PBS/DVS for V6; this local regression alone is not product acceptance evidence.

Gzip ACKs are supported: the proxy forwards the caller's `Accept-Encoding` unchanged. It decompresses a gzip response only for COMMITTED classification, then forwards the original compressed bytes and Content-Encoding on non-dropped responses. The regression includes a gzip-producing upstream and validates both the first ACK disconnect and subsequent compressed replay response. This matches real DVS compression without modifying PBS transport settings.

With a reused evidence volume, startup resumes numbering after the greatest existing raw-file or event-log sequence. Raw files use exclusive creation and cannot overwrite a prior capture. The restart regression verifies old bytes survive and the next request receives a new filename. Use one proxy replica per evidence directory.

For a deterministic crash between lost ACK and reconciliation, include `"hold_replay":true` in `drop-next`. On a confirmed commit the proxy atomically arms a one-shot hold for the next identical method/path/key before disconnecting the original caller. Wait for both `ack_dropped` and `hold_started` evidence, crash PBS, then release the replay hold with the existing control. This closes the otherwise millisecond-wide same-run reconciliation race; it changes only the fixture, not the production protocol. The gzip regression proves replay is held before any second upstream request.
