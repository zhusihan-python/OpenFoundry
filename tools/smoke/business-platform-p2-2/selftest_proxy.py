#!/usr/bin/env python3
"""Local stdlib self-test for dataset_proxy.py (P2.2 isolation-stack
tool). Spins a stub upstream + the proxy in-process on ephemeral ports;
localhost only, no stack, no docker.

Proves:
  * passthrough is byte-faithful (status/body/headers) and forwards
    Authorization upstream;
  * a hold parks the RESPONSE only — the upstream request is PROVABLY
    processed while the client is still blocked (matched/held_now live
    counters);
  * release delivers the REAL upstream response (no fabrication);
  * bounded expiry answers with the real response without any release;
  * non-matching paths are never held;
  * admin validation rejects malformed holds;
  * the admin/snapshot surface never carries Authorization material.
"""

import importlib.util
import json
from pathlib import Path
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.error
import urllib.request

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("p22_proxy_under_test",
                                              HERE / "dataset_proxy.py")
proxy_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proxy_mod)

PASS = 0
FAIL = 0
SECRET = "Bearer SECRET-JWT-VALUE"


def ok(cond, label, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"ok   {label}")
    else:
        FAIL += 1
        print(f"FAIL {label} {detail}")


class StubUpstream(BaseHTTPRequestHandler):
    server_version = "StubUpstream/1.0"
    protocol_version = "HTTP/1.1"
    seen = []  # (method, path, auth_ok, body) — auth_ok is EXACT-match
    lock = threading.Lock()

    def log_message(self, fmt, *args):
        pass

    def _record(self, body=b""):
        # Record ONLY whether the Authorization header equals THIS case's
        # secret — the token value itself is never stored or echoed
        # (presence-only recording would let any bearer pass the check).
        with StubUpstream.lock:
            StubUpstream.seen.append((self.command, self.path,
                                      self.headers.get("Authorization") == SECRET,
                                      body.decode(errors="replace")))

    def do_GET(self):
        self._record()
        payload = json.dumps({"upstream": "get", "path": self.path}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self._record(body)
        if self.path.endswith("/slow-commit"):
            time.sleep(0.6)  # keeps the proxy inside its upstream call
        payload = json.dumps({"upstream": "committed",
                              "path": self.path}).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Upstream-Marker", "real-commit")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


stub = ThreadingHTTPServer(("127.0.0.1", 0), StubUpstream)
STUB_PORT = stub.server_address[1]
threading.Thread(target=stub.serve_forever, daemon=True).start()

proxy_mod.Handler.upstream_host = "127.0.0.1"
proxy_mod.Handler.upstream_port = STUB_PORT
proxy = ThreadingHTTPServer(("127.0.0.1", 0), proxy_mod.Handler)
PROXY_PORT = proxy.server_address[1]
threading.Thread(target=proxy.serve_forever, daemon=True).start()


def req(port, path, *, method="GET", payload=None, headers=None, timeout=10):
    data = json.dumps(payload).encode() if payload is not None else None
    hdrs = dict(headers or {})
    if data is not None:
        hdrs["Content-Type"] = "application/json"
    r = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                               headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def admin(path, payload=None):
    if payload is None:
        return req(PROXY_PORT, "/p22proxy/" + path)
    return req(PROXY_PORT, "/p22proxy/" + path, method="POST", payload=payload)


# --- 1. healthz ----------------------------------------------------------
ok(admin("healthz")[0] == 200, "healthz")

# --- 2. passthrough is byte-faithful, forwards Authorization -------------
direct = req(STUB_PORT, "/v1/datasets", headers={"Authorization": SECRET})
via = req(PROXY_PORT, "/v1/datasets", headers={"Authorization": SECRET})
ok(via[0] == direct[0] == 200 and via[2] == direct[2],
   "passthrough status+body byte-equal")
ok(via[1].get("Content-Type") == direct[1].get("Content-Type"),
   "passthrough content-type preserved")
with StubUpstream.lock:
    ok(StubUpstream.seen[-1][2] is True,
       "upstream saw the EXACT Authorization (not mere presence)")
# negative control: a wrong bearer must record False (exact-match proof)
req(PROXY_PORT, "/v1/datasets", headers={"Authorization": "Bearer WRONG"})
with StubUpstream.lock:
    ok(StubUpstream.seen[-1][2] is False,
       "exact-match recording: wrong bearer recorded as False")

# --- 3. hold validation ----------------------------------------------------
st, _, body = admin("hold", {"id": "", "method": "POST", "path_suffix": "/x"})
ok(st == 400, "hold rejects empty id")
st, _, _ = admin("hold", {"id": "h", "method": "TRACE", "path_suffix": "/x"})
ok(st == 400, "hold rejects bad method")
st, _, _ = admin("hold", {"id": "h", "method": "POST", "path_suffix": "x"})
ok(st == 400, "hold rejects suffix without '/'")
st, _, _ = admin("hold", {"id": "h", "method": "POST", "path_suffix": "/x",
                          "max_hold_s": 901})
ok(st == 400, "hold rejects max_hold_s>900")

# --- 4. hold parks RESPONSE only; release delivers REAL response ----------
admin("hold", {"id": "gate", "method": "POST",
               "path_suffix": "/outputs:commit", "dataset_id": "d-123",
               "max_hold_s": 60})
box = {}
th = threading.Thread(
    target=lambda: box.update(zip(("st", "hd", "body"),
                                  req(PROXY_PORT, "/v1/datasets/d-123/outputs:commit",
                                      method="POST", payload={"x": 1},
                                      headers={"Authorization": SECRET},
                                      timeout=30))), daemon=True)
th.start()
deadline = time.monotonic() + 5
snap = None
while time.monotonic() < deadline:
    snap = json.loads(admin("snapshot")[2])
    hold = next((h for h in snap["holds"] if h["id"] == "gate"), None)
    if hold and hold["matched"] == 1 and hold["held_now"] == 1:
        break
    time.sleep(0.05)
ok(hold and hold["matched"] == 1 and hold["held_now"] == 1,
   "held request observable (matched=1, held_now=1)", str(snap))
with StubUpstream.lock:
    ok(any(m == "POST" and p == "/v1/datasets/d-123/outputs:commit"
           and auth for m, p, auth, _b in StubUpstream.seen),
   "upstream PROCESSED the commit while the client is still parked")
th_join_started = time.monotonic()
time.sleep(0.5)
ok(th.is_alive(), "client still blocked before release")
st_rel = admin("release", {"id": "gate"})
ok(json.loads(st_rel[2])["released"] == 1, "release acknowledged")
th.join(timeout=5)
ok(not th.is_alive(), "client unblocked after release")
direct_commit = req(STUB_PORT, "/v1/datasets/d-123/outputs:commit",
                    method="POST", payload={"x": 1},
                    headers={"Authorization": SECRET})
ok(box.get("st") == direct_commit[0] == 201
   and box.get("body") == direct_commit[2]
   and box.get("hd", {}).get("X-Upstream-Marker") == "real-commit",
   "released response is the REAL upstream answer (status+body+marker)",
   str(box))
snap = json.loads(admin("snapshot")[2])
hold = next(h for h in snap["holds"] if h["id"] == "gate")
ok(hold["held_now"] == 0, "held_now settles to 0", str(snap))

# --- 5. bounded expiry answers the real response without release ----------
admin("hold", {"id": "expire", "method": "GET", "path_suffix": "/datasets",
               "max_hold_s": 1})
box2 = {}
th2 = threading.Thread(
    target=lambda: box2.update(zip(("st", "hd", "body"),
                                   req(PROXY_PORT, "/v1/datasets",
                                       headers={"Authorization": SECRET},
                                       timeout=30))), daemon=True)
t0 = time.monotonic()
th2.start()
th2.join(timeout=10)
elapsed = time.monotonic() - t0
ok(box2.get("st") == 200 and 0.9 <= elapsed < 5,
   "bounded hold expiry delivers real response", f"{elapsed:.2f}s")
snap = json.loads(admin("snapshot")[2])
ok(next(h for h in snap["holds"] if h["id"] == "expire")["held_now"] == 0,
   "expiry cleans up held_now")

# --- 6. non-matching path never held ---------------------------------------
admin("hold", {"id": "narrow", "method": "POST",
               "path_suffix": "/outputs:commit", "dataset_id": "d-other"})
st3, _, _ = req(PROXY_PORT, "/v1/datasets/d-123/files", method="POST",
                payload={}, timeout=10)
ok(st3 == 201, "non-matching path passes through (not held)")
snap = json.loads(admin("snapshot")[2])
ok(next(h for h in snap["holds"] if h["id"] == "narrow")["matched"] == 0,
   "narrow hold matched nothing")

# --- 6b. busy-reject (review fix 2), DETERMINISTIC ORDER: install h ->
# request captured/held -> reinstall h must be REJECTED (old waiters would
# be orphaned) -> release h delivers the real response -> inflight drains
# -> reinstall now succeeds. ----------------------------------------------
admin("hold", {"id": "busy", "method": "POST",
               "path_suffix": "/outputs:commit", "max_hold_s": 60})
box_b = {}
th_b = threading.Thread(
    target=lambda: box_b.update(zip(("st", "hd", "body"),
                                    req(PROXY_PORT, "/v1/datasets/db-1/outputs:commit",
                                        method="POST", payload={},
                                        timeout=30))), daemon=True)
th_b.start()
b_deadline = time.monotonic() + 5
hold_b = None
while time.monotonic() < b_deadline:
    hold_b = next((h for h in json.loads(admin("snapshot")[2])["holds"]
                   if h["id"] == "busy"), None)
    if hold_b and hold_b["matched"] == 1 and hold_b["held_now"] == 1:
        break
    time.sleep(0.05)
ok(hold_b and hold_b["inflight"] == 1,
   "busy: request captured on hold (inflight=1, held_now=1)", str(hold_b))
st_reinstall = admin("hold", {"id": "busy", "method": "POST",
                              "path_suffix": "/outputs:commit",
                              "max_hold_s": 60})
ok(st_reinstall[0] == 409,
   "busy: reinstall with an in-flight request is rejected (409)",
   str(st_reinstall[0]))
ok(th_b.is_alive(), "busy: the held request is still parked after the "
   "rejected reinstall")
admin("release", {"id": "busy"})
th_b.join(timeout=5)
ok(box_b.get("st") == 201, "busy: release still reaches the OLD waiter",
   str(box_b))
drain_deadline = time.monotonic() + 5
while time.monotonic() < drain_deadline:
    hold_b = next((h for h in json.loads(admin("snapshot")[2])["holds"]
                   if h["id"] == "busy"), None)
    if hold_b and hold_b["inflight"] == 0:
        break
    time.sleep(0.05)
ok(hold_b and hold_b["inflight"] == 0, "busy: inflight drains after answer")
st_again = admin("hold", {"id": "busy", "method": "POST",
                          "path_suffix": "/outputs:commit", "max_hold_s": 60})
ok(st_again[0] == 200, "busy: idle reinstall allowed once drained")
admin("release", {"all": True})

# --- 6c. idle reinstall (no in-flight) is allowed ---------------------------
st_idle1 = admin("hold", {"id": "idle", "method": "GET",
                          "path_suffix": "/datasets", "max_hold_s": 30})
st_idle2 = admin("hold", {"id": "idle", "method": "GET",
                          "path_suffix": "/datasets", "max_hold_s": 45})
ok(st_idle1[0] == 200 and st_idle2[0] == 200,
   "idle reinstall allowed with zero in-flight")
snap_idle = json.loads(admin("snapshot")[2])
ok(next(h for h in snap_idle["holds"] if h["id"] == "idle")["max_hold_s"] == 45,
   "idle reinstall replaced the spec")

# --- 6d. release DURING upstream processing: the generation was bumped
# before the park point, so the request answers immediately and must NOT
# count as matched/held (review fix A ordering). --------------------------
admin("hold", {"id": "midflight", "method": "POST",
               "path_suffix": "/slow-commit", "max_hold_s": 60})
box_s = {}
t0 = time.monotonic()
th_s = threading.Thread(
    target=lambda: box_s.update(zip(("st", "hd", "body"),
                                    req(PROXY_PORT, "/v1/datasets/d-slow/slow-commit",
                                        method="POST", payload={}, timeout=30))),
    daemon=True)
th_s.start()
s_deadline = time.monotonic() + 5
seen_slow = False
while time.monotonic() < s_deadline:
    with StubUpstream.lock:
        seen_slow = any(m == "POST" and p.endswith("/slow-commit")
                        for m, p, _a, _b in StubUpstream.seen)
    if seen_slow:
        break
    time.sleep(0.02)
ok(seen_slow, "midflight: upstream is processing the request")
admin("release", {"id": "midflight"})  # BEFORE the upstream returns
th_s.join(timeout=10)
elapsed_s = time.monotonic() - t0
ok(box_s.get("st") == 201 and elapsed_s < 2.5,
   "midflight: released-during-upstream answers without parking",
   f"{elapsed_s:.2f}s")
hold_s = next((h for h in json.loads(admin("snapshot")[2])["holds"]
               if h["id"] == "midflight"), None)
ok(hold_s and hold_s["matched"] == 0 and hold_s["inflight"] == 0,
   "midflight: never counted as matched/held; inflight drained",
   str(hold_s))

# --- 7. admin/snapshot surfaces carry no header material -------------------
blob = admin("snapshot")[2].decode() + admin("healthz")[2].decode()
ok("SECRET-JWT-VALUE" not in blob and "Authorization" not in blob,
   "admin surfaces carry no Authorization material")
st404, _, _ = admin("nothing")
ok(st404 == 404, "unknown admin path 404s")

stub.shutdown()
proxy.shutdown()
print(f"\n[p22-proxy-selftest] pass={PASS} fail={FAIL}")
sys.exit(0 if FAIL == 0 else 1)
