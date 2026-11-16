# Shared fixture delay extension (authorized P1-shared-tool change)

`../business-platform-p1/fixtures/rest_source.py` gains, per `src` key:
- `delay_ms` (default 0) and `delay_from_page` (default 2), controlled by
  the existing `POST /admin/src/{name}/set`; validation rejects negative,
  non-int, and out-of-range values (delay_ms int in [0,60000],
  delay_from_page int in [1,10000]).
- The delay is applied only when delay_ms > 0 and page >= delay_from_page,
  AFTER auth/page resolution and BEFORE records are sliced — record
  content, envelopes, and pagination are unchanged. Defaults keep every
  existing scenario latency-free.
- `request_count` still increments at request ENTRY (pre-wait), so an
  in-flight slow page is observable via `GET /admin/src/{name}` while it
  waits — usable for P2 heartbeat/lease-loss observation once the service
  contract lands. No lease HTTP endpoint is implied or added.

Local stdlib self-tests (no stack started): default no-delay <0.3s; 151
records value-identical; page1 fast / page2 slow (400ms -> observed
0.405s); rejects for -1/60001/"300"/0/1000000 with error strings; accepts
boundary 0/60000/1/10000; zero-delay fast after reset; snapshot exposes
new fields; request_count observed incremented DURING the in-flight wait
(before=0 during=1, request elapsed 0.81s vs 800ms configured).
