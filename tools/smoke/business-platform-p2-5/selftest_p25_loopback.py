#!/usr/bin/env python3
"""P2.5 LOOPBACK selftest — scoped release (root, review-fixture-proxy-
round1 §Newly allowed execution).

Runs OWN in-process servers bound to 127.0.0.1:0 (OS-random ports):
the P25 fixture handler, the P25 fault-proxy handler, and a FAKE
Dataset stub. A hard guard asserts every request destination is one of
the EXACT bound stub ports — no real q3 env/JWT/DSN is read, no fixed
ports, no Docker, no background services beyond this process, and the
finally block shuts everything down with bounded joins (no leftovers).

Covers what pure-core tests cannot (real Handler/forward lifecycle):
  L1 nil-retry-A over HTTP; L2 token retry stability; L3 stable paging
  window; L4 blocked request: admin sees active>0, healthz + SIBLING
  src still progress (delay outside the lock), then true quiet;
  L5 error + client-disconnect lifecycles end quiet; L6 NORMAL commit
  passthrough IS ledgered (zero-publish provable) + stats quiet;
  L7 each fault mode over HTTP isolates EXACTLY ONE ACK predicate;
  L8 drop_after_verify records the full real ACK + verdict and drops
  the caller only after observing it (COMMITTED and non-COMMITTED
  upstream variants).
"""

import hashlib
import importlib.util
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
PASS, FAIL = [], []
BOUND_PORTS = set()          # hard destination guard


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  ok: {name}")
    else:
        FAIL.append(f"{name} {detail}")
        print(f"  FAIL: {name} {detail}")


def loadmod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def url(port, path):
    assert port in BOUND_PORTS, f"destination guard: {port} not bound"
    return f"http://127.0.0.1:{port}{path}"


def get_json(port, path, timeout=5):
    with urllib.request.urlopen(url(port, path),
                                timeout=timeout) as resp:
        return resp.status, json.loads(resp.read())


def post_json(port, path, payload, timeout=8):
    req = urllib.request.Request(
        url(port, path), data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read())


# --- FAKE dataset stub -------------------------------------------------

class FakeDatasetCore:
    def __init__(self):
        self.commit_mode = "committed"    # committed | not_committed
        self.requests = []


class FakeDatasetHandler(BaseHTTPRequestHandler):
    core: FakeDatasetCore = None

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.core.requests.append({"path": self.path,
                                   "body_len": len(body)})
        m = self.path.rsplit("/datasets/", 1)
        target = m[1].split("/")[0] if len(m) == 2 else "unknown"
        if self.path.endswith("/outputs:commit"):
            if self.core.commit_mode == "committed":
                return self._send(200, {
                    "dataset_id": target,
                    "transaction": {"id":
                                    "22222222-2222-4222-8222-222222222222",
                                    "status": "COMMITTED"}})
            return self._send(200, {
                "dataset_id": target,
                "transaction": {"id":
                                "33333333-3333-4333-8333-333333333333",
                                "status": "OPEN"}})
        self._send(404, {"error": "not found"})

    def do_GET(self):
        self.core.requests.append({"path": self.path})
        self._send(200, {"ok": True})


def start_server(handler_cls):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    srv.daemon_threads = True
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    BOUND_PORTS.add(srv.server_address[1])
    return srv, t


def http_walk(fx_port, src, token=None, since_param="since_token"):
    rows, pages = [], 0
    params = [f"src={src}"]
    if token is not None:
        params.append(f"{since_param}={token}")
    cursor = None
    while True:
        q = "&".join(params + ([f"cursor={cursor}"] if cursor else []))
        st, body = get_json(fx_port, f"/data?{q}")
        pages += 1
        if st != 200:
            return st, body, rows, pages
        rows.extend(body.get("records", []))
        if not body.get("next_cursor"):
            return st, body, rows, pages
        cursor = body["next_cursor"]


def main():
    fx = loadmod(HERE / "fixtures" / "rest_source_p25.py", "lb_fx")
    px = loadmod(HERE / "fixtures" / "dataset_fault_proxy_p25.py",
                 "lb_px")

    fake_core = FakeDatasetCore()
    FakeDatasetHandler.core = fake_core

    app = fx.IncrementalFixture()
    fx.Handler.app = app
    proxy_core = px.ProxyCore()
    px.Handler.core = proxy_core

    servers = []
    try:
        stub_srv, stub_t = start_server(FakeDatasetHandler)
        fx_srv, fx_t = start_server(fx.Handler)
        px.Handler.upstream = f"http://127.0.0.1:{stub_srv.server_address[1]}"
        px_srv, px_t = start_server(px.Handler)
        servers = [(stub_srv, stub_t), (fx_srv, fx_t), (px_srv, px_t)]
        fxp = fx_srv.server_address[1]
        pxp = px_srv.server_address[1]
        print(f"[loopback] fixture=:{fxp} proxy=:{pxp} "
              f"stub=:{stub_srv.server_address[1]} (all 127.0.0.1:0)")

        print("[L1-L3] consumer position over real HTTP")
        st, body, rows, _ = http_walk(fxp, "p25h1")
        check("L1a first pull serves A and completes tok-0",
              st == 200 and len(rows) == 24
              and body["meta"]["completed_token"] == "p25h1-tok-0")
        tok0 = body["meta"]["completed_token"]
        # simulate publish failure: consumer checkpoint stays NULL ⇒ a
        # NULL retry MUST see window A again (not B)
        st, body, rows, _ = http_walk(fxp, "p25h1")
        check("L1b NULL retry re-serves A (no implicit progress)",
              st == 200 and rows[0].startswith("a-")
              and body["meta"]["completed_token"] == tok0)
        st, body, rows, _ = http_walk(fxp, "p25h1",
                                      token=tok0)
        check("L2a tok-0 pull serves B and completes tok-1",
              st == 200 and rows[0].startswith("b-")
              and body["meta"]["completed_token"] == "p25h1-tok-1")
        tok1 = body["meta"]["completed_token"]
        st, b2, rows2, _ = http_walk(fxp, "p25h1", token=tok0)
        check("L2b repeated tok-0 ⇒ identical window/token",
              rows2 == rows and b2["meta"]["completed_token"] == tok1)
        st, body, rows, pages = http_walk(fxp, "p25h1", token=tok1)
        check("L3 stable paging window (C: 1 page, 30 rows, 2 pages "
              "total for A)", rows[0].startswith("c-")
              and len(rows) == 30 and pages == 1)

        print("[L4] blocked request: admin/healthz/sibling progress")
        app.set_knob("p25h1", {"slow_terminal_ms": 1200})
        result = {}

        def blocked():
            try:
                http_walk(fxp, "p25h1")
                result["done"] = True
            except Exception as exc:                   # noqa: BLE001
                result["err"] = str(exc)[:80]

        th = threading.Thread(target=blocked, daemon=True)
        th.start()
        saw_active = False
        for _ in range(60):
            snap = app.snapshot("p25h1")
            if snap and snap["active"] > 0:
                saw_active = True
                break
            time.sleep(0.02)
        check("L4a admin observes active>0 while blocked", saw_active)
        t0 = time.time()
        st, h = get_json(fxp, "/healthz")
        check("L4b healthz progresses during block",
              st == 200 and h.get("ok") is True
              and time.time() - t0 < 0.5)
        t0 = time.time()
        st, body, rows, _ = http_walk(fxp, "p25h3")
        check("L4c SIBLING src progresses during block (lock-free "
              "delay)", st == 200 and time.time() - t0 < 0.5
              and rows[0].startswith("a-"))
        th.join(timeout=8)
        check("L4d blocked request completes; true quiet after",
              result.get("done") is True and app.quiet()
              and app.snapshot("p25h1")["active"] == 0)

        print("[L5] error + disconnect lifecycles end quiet")
        app.set_knob("p25h4", {"fail_mid_pages": True})
        st, b1 = get_json(fxp, "/data?src=p25h4")
        st2 = 0
        try:
            with urllib.request.urlopen(
                    url(fxp, f"/data?src=p25h4&cursor="
                             f"{b1['next_cursor']}"), timeout=5) as r:
                st2 = r.status
        except urllib.error.HTTPError as exc:
            st2 = exc.code
        check("L5a injected 500 delivered", st2 == 500)
        deadline = time.time() + 2
        quiet_err = False
        while time.time() < deadline:
            if app.quiet():
                quiet_err = True
                break
            time.sleep(0.02)
        check("L5b quiet after error exit", quiet_err)
        # client disconnect mid-response: SINGLE-PAGE src with the
        # delay ON the terminal page; wait until ENTRY is observable
        # (active>0) BEFORE closing, so the in-flight request is real
        app.set_knob("p25h6", {"slow_terminal_ms": 900})
        s = socket.create_connection(("127.0.0.1", fxp), timeout=3)
        s.sendall(b"GET /data?src=p25h6 HTTP/1.1\r\nHost: x\r\n"
                  b"Connection: close\r\n\r\n")
        entered = False
        deadline = time.time() + 3
        while time.time() < deadline:
            if app.snapshot("p25h6")["active"] > 0:
                entered = True
                break
            time.sleep(0.01)
        s.close()                       # server write will hit EPIPE
        deadline = time.time() + 5
        quiet = False
        while time.time() < deadline:
            if app.quiet():
                quiet = True
                break
            time.sleep(0.05)
        check("L5c disconnect after observable entry finishes quiet",
              entered and quiet)
        app.set_knob("p25h6", {"slow_terminal_ms": 0})

        print("[L6] normal commit passthrough IS ledgered")
        tgt = "44444444-4444-4444-8444-444444444444"
        proxy_core.set_mode("passthrough")
        st, body = post_json(pxp, f"/api/v1/datasets/{tgt}/"
                                  f"outputs:commit", {"files": []})
        led = proxy_core.ledger[-1] if proxy_core.ledger else {}
        check("L6a passthrough relays the real stub ACK",
              st == 200 and body["transaction"]["status"] == "COMMITTED"
              and body["dataset_id"] == tgt)
        check("L6b normal commit LEDGERED with parsed fields",
              led.get("mode") == "passthrough"
              and led.get("forwarded") is True
              and led.get("request_target") == tgt
              and led.get("ack_txn_status") == "COMMITTED")
        check("L6c stats quiet after passthrough",
              proxy_core.quiet()
              and proxy_core.stats["commit_inflight"] == 0)

        print("[L7] fault modes isolate exactly ONE predicate")
        for mode, pred in (
                ("no_txn_200", "txn-id-absent"),
                ("not_committed", "status-open"),
                ("wrong_target", "target-changed")):
            proxy_core.set_mode(mode)
            st, body = post_json(pxp, f"/api/v1/datasets/{tgt}/"
                                      f"outputs:commit", {"files": []})
            led = proxy_core.ledger[-1]
            ok = (led.get("forwarded") is False
                  and led.get("request_target") == tgt)
            if mode == "no_txn_200":
                ok = ok and body["dataset_id"] == tgt \
                    and body["transaction"]["status"] == "COMMITTED" \
                    and "id" not in body["transaction"]
            elif mode == "not_committed":
                ok = ok and body["dataset_id"] == tgt \
                    and body["transaction"]["id"] == px.FAKE_TXN_ID \
                    and body["transaction"]["status"] == "OPEN"
            else:
                ok = ok and body["dataset_id"] != tgt \
                    and body["transaction"]["status"] == "COMMITTED" \
                    and body["transaction"]["id"] == px.FAKE_TXN_ID
            check(f"L7 {mode} isolates {pred} (HTTP)", ok,
                  str(body)[:80])

        print("[L8] drop_after_verify: full ACK recorded, then drop")
        fake_core.commit_mode = "committed"
        proxy_core.set_mode("drop_after_verify")
        err = None
        try:
            post_json(pxp, f"/api/v1/datasets/{tgt}/outputs:commit",
                      {"files": []}, timeout=6)
        except Exception as exc:                        # noqa: BLE001
            err = exc
        led = proxy_core.ledger[-1]
        check("L8a caller got NO usable ACK (drop/timeout)",
              err is not None)
        raw = led.get("upstream_ack_raw")
        check("L8b verified_committed ONLY on full match "
              "(real COMMITTED stub) + raw ACK recompute",
              led.get("verified_committed") is True
              and led.get("forwarded") is True
              and led.get("ack_txn_status") == "COMMITTED"
              and led.get("ack_dataset_id") == tgt
              and len(led.get("upstream_ack_sha256", "")) == 64
              and isinstance(raw, str)
              and hashlib.sha256(raw.encode()).hexdigest()
              == led["upstream_ack_sha256"]
              and json.loads(raw)["transaction"]["status"]
              == "COMMITTED")
        fake_core.commit_mode = "not_committed"
        err2 = None
        try:
            post_json(pxp, f"/api/v1/datasets/{tgt}/outputs:commit",
                      {"files": []}, timeout=6)
        except Exception as exc:                        # noqa: BLE001
            err2 = exc
        led2 = proxy_core.ledger[-1]
        check("L8c non-COMMITTED upstream: verified_committed FALSE, "
              "still dropped+recorded",
              err2 is not None
              and led2.get("verified_committed") is False
              and led2.get("ack_txn_status") == "OPEN")
        check("L8d proxy quiet after drops", proxy_core.quiet())
        reg = proxy_core.stats.get("commit_threads_registered", 0)
        check("L8e real worker threads were registered (join is not "
              "an empty-set fake green)", reg >= 1)

    finally:
        for srv, t in servers:
            srv.shutdown()
            srv.server_close()
            t.join(timeout=10)
            check(f"server :{srv.server_address[1]} joined clean",
                  not t.is_alive())
        ok_join = proxy_core.join_threads(timeout=10)
        check("proxy worker threads finalized (bounded join)", ok_join)
        leftovers = [p for p in BOUND_PORTS
                     if _port_still_listening(p)]
        check("no listening leftovers on bound ports",
              not leftovers, str(leftovers))

    print(f"\n[selftest-p25-loopback] pass={len(PASS)} fail={len(FAIL)}")
    for f in FAIL:
        print(f"  FAILED: {f}")
    return 1 if FAIL else 0


def _port_still_listening(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


if __name__ == "__main__":
    sys.exit(main())
