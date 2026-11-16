#!/usr/bin/env python3
"""Controlled reverse proxy for the P2.2 isolation stack (TOOL only —
this lives next to the acceptance scripts and is never part of any
service image or prod surface).

Purpose (root-directed 2026-11-14): give the interruptible-worker
acceptance a REAL-Dataset response barrier — the upstream Dataset
service actually processes (and COMMITS) the request, while the proxy
holds the RESPONSE back from the connector until released (or a bounded
max hold). This makes the "publish gate already won" window observable
and injectable WITHOUT inventing any fault-entry endpoint on the
services themselves:

  * gate-wins too_late: hold the outputs:commit response, observe the
    gate/publish phase in PG, cancel => 409 too_late, release => the
    connector receives the REAL committed response and the run succeeds.
  * publish lost-response: hold across a shutdown; the Dataset stays
    COMMITTED, the connector never confirms, nothing is faked as aborted
    and nothing auto re-dispatches.

Guarantees:
  * The proxy NEVER fabricates or alters upstream bytes — a held request
    is answered with the exact status/headers/body the upstream sent.
  * Every hold is bounded (max_hold_s); a held thread always finishes.
  * Reinstalling a hold id while requests are captured on the existing
    hold is REJECTED (409 busy) — an orphaned old hold could otherwise
    no longer be released or observed for up to max_hold_s.
  * held_now/matched prove the upstream RESPONSE was received — they say
    NOTHING about the upstream business outcome (e.g. Dataset COMMITTED);
    acceptance scenarios must verify the real transaction/file/hash.
  * The control plane carries plain config values only. Authorization
    and every other request header are forwarded upstream and NEVER
    stored, logged, or echoed by the admin surface.
  * log_message prints the request line only.

Run (trusted code, stdlib only):
    python3 -I dataset_proxy.py [bind_host] [port] [upstream_base]
Control plane (same listener):
    GET  /p22proxy/healthz
    POST /p22proxy/hold      {"id","method","path_suffix"[,"dataset_id"],
                              "max_hold_s" 1..900}   (default max 300)
    POST /p22proxy/release   {"id"} | {"all": true}
    GET  /p22proxy/snapshot  {"holds":[{...,"matched","held_now"}],"inflight":N}
"""

import http.client
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

ADMIN_PREFIX = "/p22proxy/"
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate",
              "proxy-authorization", "te", "trailers",
              "transfer-encoding", "upgrade", "host", "content-length"}

DEFAULT_MAX_HOLD_S = 300


class Hold:
    """One installed hold spec + its live counters (all ints — nothing
    that could carry header/token material)."""

    def __init__(self, hid, method, path_suffix, dataset_id, max_hold_s):
        self.id = hid
        self.method = method.upper()
        self.path_suffix = path_suffix
        self.dataset_id = dataset_id
        self.max_hold_s = max_hold_s
        self.matched = 0     # requests that matched and were held
        self.held_now = 0    # currently parked (response received, withheld)
        self.inflight = 0    # captured on THIS hold: matched but not yet answered
        self.released_gen = 0  # bumped by every release; waiters compare
        self.cond = threading.Condition()

    def matches(self, method, path):
        if self.method != method.upper():
            return False
        if not path.endswith(self.path_suffix):
            return False
        if self.dataset_id and self.dataset_id not in path:
            return False
        return True

    def spec(self):
        return {"id": self.id, "method": self.method,
                "path_suffix": self.path_suffix,
                "dataset_id": self.dataset_id,
                "max_hold_s": self.max_hold_s,
                "matched": self.matched, "held_now": self.held_now,
                "inflight": self.inflight}


class ProxyState:
    def __init__(self):
        self.holds = {}
        self.inflight = 0
        self.lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            return {"holds": [h.spec() for h in self.holds.values()],
                    "inflight": self.inflight}


STATE = ProxyState()


class Handler(BaseHTTPRequestHandler):
    server_version = "OpenFoundryP22DatasetProxy/1.0"
    protocol_version = "HTTP/1.1"
    upstream_base = "http://dataset:50078"  # replaced in main()

    def log_message(self, fmt, *args):  # request line ONLY — no headers/bodies
        print(f"[p22proxy] {self.address_string()} {fmt % args}", flush=True)

    def send_json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------- admin
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == ADMIN_PREFIX + "healthz":
            self.send_json(200, {"status": "ok"})
            return
        if path == ADMIN_PREFIX + "snapshot":
            self.send_json(200, STATE.snapshot())
            return
        if path.startswith(ADMIN_PREFIX):
            self.send_json(404, {"error": "not found"})
            return
        self._forward("GET")

    def do_POST(self):
        path = urlsplit(self.path).path
        if path not in (ADMIN_PREFIX + "hold", ADMIN_PREFIX + "release"):
            self._forward("POST")
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            patch = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            self.send_json(400, {"error": "invalid json"})
            return
        if path.endswith("hold"):
            self._install_hold(patch)
        else:
            self._release(patch)

    def do_PUT(self):
        self._forward("PUT")

    def do_PATCH(self):
        self._forward("PATCH")

    def do_DELETE(self):
        self._forward("DELETE")

    def _install_hold(self, patch):
        hid = patch.get("id")
        method = patch.get("method")
        suffix = patch.get("path_suffix")
        dataset_id = patch.get("dataset_id")
        max_hold_s = patch.get("max_hold_s", DEFAULT_MAX_HOLD_S)
        errors = []
        if not isinstance(hid, str) or not hid or len(hid) > 64:
            errors.append("id must be a short string")
        if not isinstance(method, str) or method.upper() not in \
                ("GET", "POST", "PUT", "PATCH", "DELETE"):
            errors.append("method must be an HTTP verb")
        if not isinstance(suffix, str) or not suffix.startswith("/") \
                or len(suffix) < 2:
            errors.append("path_suffix must start with '/'")
        if dataset_id is not None and not isinstance(dataset_id, str):
            errors.append("dataset_id must be a string or null")
        if not isinstance(max_hold_s, int) or isinstance(max_hold_s, bool) \
                or max_hold_s < 1 or max_hold_s > 900:
            errors.append("max_hold_s must be an int in [1, 900]")
        if errors:
            self.send_json(400, {"errors": errors})
            return
        hold = Hold(hid, method, suffix, dataset_id, max_hold_s)
        with STATE.lock:
            existing = STATE.holds.get(hid)
            if existing is not None and existing.inflight > 0:
                # Reinstalling while requests are captured on the old hold
                # would orphan them: release-by-id only reaches the NEW
                # object, leaving the old waiters unreleasable and
                # unobservable for up to max_hold_s. Reject instead.
                self.send_json(409, {"errors": [
                    f"hold id '{hid}' is busy: {existing.inflight} in-flight "
                    "request(s) captured on the existing hold; release them "
                    "or let them drain before reinstalling"]})
                return
            STATE.holds[hid] = hold  # idle reinstall replaces atomically
        self.send_json(200, {"state": hold.spec()})

    def _release(self, patch):
        with STATE.lock:
            if patch.get("all") is True:
                holds = list(STATE.holds.values())
            else:
                hid = patch.get("id")
                hold = STATE.holds.get(hid) if isinstance(hid, str) else None
                holds = [hold] if hold else []
            for hold in holds:
                with hold.cond:
                    hold.released_gen += 1
                    hold.cond.notify_all()
        self.send_json(200, {"released": len(holds)})

    # ---------------------------------------------------------- forwarding
    def _forward(self, method_override=None):
        method = method_override or self.command
        url = urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None

        # matching happens BEFORE the upstream call so the hold spec can
        # only ever see method/path/dataset-id — never header material.
        with STATE.lock:
            matched = [h for h in STATE.holds.values()
                       if h.matches(method, url.path)]
            STATE.inflight += 1
            hold = matched[0] if matched else None
            if hold is not None:
                hold.inflight += 1  # capture: busy-reject guards reinstall
                # capture the release generation BEFORE the upstream call
                # so a release racing with this request can never be lost
                gen = hold.released_gen
        try:
            try:
                upstream = http.client.HTTPConnection(
                    self.upstream_host, self.upstream_port, timeout=60)
                headers = {k: v for k, v in self.headers.items()
                           if k.lower() not in HOP_BY_HOP}
                upstream.request(method, self.path, body=body, headers=headers)
                resp = upstream.getresponse()
                resp_body = resp.read()
                resp_headers = [(k, v) for k, v in resp.getheaders()
                                if k.lower() not in HOP_BY_HOP]
                status = resp.status
                upstream.close()
            except Exception as exc:
                self.send_json(502, {"error": "upstream unreachable",
                                     "detail": str(exc)[:120]})
                return

            if hold is not None:
                # precision: a request that was ALREADY released while its
                # upstream call was in flight (generation bumped before the
                # park point) never withholds anything — it must not count
                # as matched or held at all. `hold` itself stays bound so
                # the outer finally still drops its inflight capture.
                park = True
                with hold.cond:
                    if hold.released_gen != gen:
                        park = False  # released mid-flight: answer at once
                if park:
                    with hold.cond:
                        hold.matched += 1
                        hold.held_now += 1
                    try:
                        deadline = time.monotonic() + hold.max_hold_s
                        with hold.cond:
                            # park until released (generation bump) or the
                            # bounded deadline — either way the client then
                            # receives the REAL upstream response; nothing
                            # is fabricated
                            while hold.released_gen == gen \
                                    and time.monotonic() < deadline:
                                hold.cond.wait(deadline - time.monotonic())
                    finally:
                        with hold.cond:
                            hold.held_now -= 1

            # the EXACT upstream answer — never fabricated, never altered
            self.send_response(status)
            for key, value in resp_headers:
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)
        finally:
            with STATE.lock:
                STATE.inflight -= 1
                if hold is not None:
                    hold.inflight -= 1


def main():
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8091
    Handler.upstream_base = (sys.argv[3] if len(sys.argv) > 3
                             else "http://dataset:50078")
    parts = urlsplit(Handler.upstream_base)
    Handler.upstream_host = parts.hostname
    Handler.upstream_port = parts.port or 80
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"[p22proxy] listening on {host}:{port} -> "
          f"{Handler.upstream_base}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
