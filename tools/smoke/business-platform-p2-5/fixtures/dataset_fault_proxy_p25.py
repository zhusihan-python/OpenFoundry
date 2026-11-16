#!/usr/bin/env python3
"""P25 dataset fault PROXY (DRAFT, not deployed).

Sits between the aux connector (DATASET_SERVICE_URL=http://p25-proxy)
and the real dataset-versioning-service. Fixture-proxy-round-1 fixes:

  * EVERY outputs:commit dispatch is ledgered — including NORMAL
    passthrough (item 3). An empty log can now PROVE zero publishes.
  * Real request lifecycle counters: started / finished / inflight /
    quiet with thread finalization (bounded join at shutdown; /admin/
    stats exposes them; aux removal may require TRUE quiet, never a
    fixed zero). No Authorization/DSN/JWT VALUES are ever recorded —
    method/path/status and safely parsed ACK fields only (test fakes).
  * Fault modes isolate EXACTLY ONE ACK predicate (item 4): the
    requested target is derived from the commit path; wrong_target
    changes ONLY dataset_id; not_committed keeps a valid txn id and
    the correct target but sets status OPEN; no_txn_200 keeps the
    correct target and status COMMITTED but omits the transaction id.
  * drop_after_verify (item 5): forwards to the OWNED upstream, then
    records the actual full ACK body + request target + parsed txn
    id/status/dataset + a verification verdict that claims
    verified_committed ONLY on nonnil txn + COMMITTED + exact target
    match — and only then drops the response. The smoke layer still
    independently rereads the Dataset service; the proxy ledger alone
    is never treated as commit proof.

Modes: passthrough | drop_after_verify | no_txn_200 | not_committed |
wrong_target | http_500. Non-commit traffic always passes through.

Admin: GET /admin/mode, POST /admin/mode {"mode":…}, GET /admin/log,
GET /admin/stats.

Usage (when released): python3 -I dataset_fault_proxy_p25.py \
    0.0.0.0 80 http://dataset:50078
"""

import json
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODES = ("passthrough", "drop_after_verify", "no_txn_200",
         "not_committed", "wrong_target", "http_500")

FAKE_TARGET_ID = "00000000-0000-4000-8000-0000000000dd"
FAKE_TXN_ID = "00000000-0000-4000-8000-0000000000ff"

COMMIT_PATH_RE = re.compile(r"^/api/v1/datasets/"
                            r"([0-9a-fA-F-]{36})/outputs:commit$")


class ProxyCore:
    """State + ledger + lifecycle stats (no sockets)."""

    def __init__(self):
        self.mode = "passthrough"
        self.ledger = []
        self.lock = threading.Lock()
        self.stats = {"commit_started": 0, "commit_finished": 0,
                      "commit_inflight": 0, "modes": {}}
        self._threads = set()

    # ---- lifecycle --------------------------------------------------
    def begin_commit(self):
        with self.lock:
            self.stats["commit_started"] += 1
            self.stats["commit_inflight"] += 1
            self.stats["commit_threads_registered"] = \
                self.stats.get("commit_threads_registered", 0) + 1
            self._threads.add(threading.current_thread())

    def finish_commit(self):
        with self.lock:
            self.stats["commit_finished"] += 1
            self.stats["commit_inflight"] = max(
                0, self.stats["commit_inflight"] - 1)
            self._threads.discard(threading.current_thread())

    def note_mode(self, mode):
        with self.lock:
            self.stats["modes"][mode] = self.stats["modes"].get(mode,
                                                                0) + 1

    def quiet(self):
        with self.lock:
            return (self.stats["commit_inflight"] == 0
                    and self.stats["commit_started"]
                    == self.stats["commit_finished"])

    def register_thread(self, t):
        with self.lock:
            self._threads.add(t)

    def join_threads(self, timeout=10.0):
        """Bounded finalization for shutdown (no leftovers)."""
        with self.lock:
            threads = list(self._threads)
        for t in threads:
            t.join(timeout=timeout)
        with self.lock:
            self._threads.difference_update(threads)
        return all(not t.is_alive() for t in threads)

    # ---- ledger -----------------------------------------------------
    def record(self, **kv):
        with self.lock:
            kv["ts"] = round(time.time(), 6)
            self.ledger.append(kv)
            if len(self.ledger) > 1000:
                self.ledger = self.ledger[-1000:]

    def set_mode(self, mode):
        if mode not in MODES:
            return False
        self.mode = mode
        return True

    def is_commit_post(self, method, path):
        return method == "POST" and COMMIT_PATH_RE.match(path) is not None

    @staticmethod
    def requested_target(path):
        m = COMMIT_PATH_RE.match(path)
        return m.group(1).lower() if m else None


def fabricate(mode, requested_target):
    """(status, body) — each fault mode corrupts EXACTLY ONE predicate
    (review item 4). requested_target comes from the commit path."""
    if mode == "no_txn_200":
        # correct target + COMMITTED status, NO transaction id
        return 200, {"dataset_id": requested_target,
                     "transaction": {"status": "COMMITTED"}}
    if mode == "not_committed":
        # correct target + VALID txn id, status not COMMITTED
        return 200, {"dataset_id": requested_target,
                     "transaction": {"id": FAKE_TXN_ID, "status": "OPEN"}}
    if mode == "wrong_target":
        # ONLY the target differs; status COMMITTED + valid txn kept
        return 200, {"dataset_id": FAKE_TARGET_ID,
                     "transaction": {"id": FAKE_TXN_ID,
                                     "status": "COMMITTED"}}
    if mode == "http_500":
        return 502, {"error": "p25 fault proxy: injected 5xx"}
    return None, None


def parse_ack(body_bytes):
    """Safely parsed ACK fields (no headers/secrets)."""
    try:
        parsed = json.loads(body_bytes)
    except (ValueError, TypeError, UnicodeDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    txn = parsed.get("transaction") or {}
    return {"ack_dataset_id": parsed.get("dataset_id"),
            "ack_txn_id": txn.get("id") if isinstance(txn, dict)
            else None,
            "ack_txn_status": txn.get("status") if isinstance(txn, dict)
            else None}


def verify_commit_ack(parsed, requested_target):
    """The drop_after_verify verdict: verified_committed ONLY on a REAL
    nonzero UUID txn id (parsed as UUID, not truthiness — round-2 fix:
    bool() wrongly accepted 't-1'/zero-UUID) + COMMITTED + exact
    requested target."""
    import uuid as _uuid
    try:
        txn_ok = str(parsed.get("ack_txn_id", "")).strip() != "" \
            and _uuid.UUID(str(parsed.get("ack_txn_id"))) \
            != _uuid.UUID(int=0)
    except (ValueError, AttributeError, TypeError):
        txn_ok = False
    return {"verified_committed":
            txn_ok
            and parsed.get("ack_txn_status") == "COMMITTED"
            and str(parsed.get("ack_dataset_id", "")).lower()
            == str(requested_target).lower()}


class Handler(BaseHTTPRequestHandler):
    core: ProxyCore = None
    upstream: str = None

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, payload, ctype="application/json"):
        body = payload if isinstance(payload, bytes) else \
            json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _admin(self, method):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/admin/mode" and method == "GET":
            return self._send(200, {"mode": self.core.mode})
        if u.path == "/admin/mode" and method == "POST":
            try:
                body = json.loads(self.rfile.read(
                    int(self.headers.get("Content-Length", "0")))
                    or b"{}")
            except ValueError:
                return self._send(400, {"error": "bad json"})
            ok = self.core.set_mode(body.get("mode", ""))
            return self._send(200 if ok else 400,
                              {"ok": ok, "mode": self.core.mode})
        if u.path == "/admin/log" and method == "GET":
            with self.core.lock:
                return self._send(200, list(self.core.ledger))
        if u.path == "/admin/stats" and method == "GET":
            with self.core.lock:
                return self._send(200, dict(self.core.stats))
        return self._send(404, {"error": "not found"})

    def _upstream(self, method, path, body):
        url = f"{self.upstream}{path}"
        req = urllib.request.Request(url, data=body if body else None,
                                     method=method)
        for h in ("Content-Type", "Authorization"):
            v = self.headers.get(h)
            if v:
                req.add_header(h, v)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, resp.read(), None
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read(), None
        except Exception as exc:                    # honest ledger entry
            return None, b"", exc

    def do_GET(self):
        if self.path.startswith("/admin/"):
            return self._admin("GET")
        status, resp, err = self._upstream("GET", self.path, None)
        self.core.record(mode="passthrough(non-commit)", method="GET",
                         path=self.path, forwarded=True,
                         upstream_status=status,
                         error=str(err) if err else None)
        if err is not None:
            return self._send(502, {"error":
                                    f"upstream unreachable: {err}"})
        self._send(status, resp,
                   ctype=self.headers.get("Content-Type",
                                          "application/json"))

    def do_POST(self):
        if self.path.startswith("/admin/"):
            return self._admin("POST")
        core = self.core
        mode = core.mode
        length = int(self.headers.get("Content-Length", "0"))
        req_body = self.rfile.read(length) if length else b""
        if not core.is_commit_post("POST", self.path):
            status, resp, err = self._upstream("POST", self.path,
                                               req_body)
            core.record(mode="passthrough(non-commit)", method="POST",
                        path=self.path, forwarded=True,
                        upstream_status=status,
                        error=str(err) if err else None)
            if err is not None:
                return self._send(502, {"error":
                                        f"upstream unreachable: {err}"})
            return self._send(status, resp)

        # outputs:commit — full lifecycle + ledger on EVERY dispatch
        target = core.requested_target(self.path)
        core.begin_commit()
        core.note_mode(mode)
        try:
            if mode == "passthrough":
                status, resp, err = self._upstream("POST", self.path,
                                                   req_body)
                parsed = parse_ack(resp) if err is None and 200 <= status < 300 else {}
                core.record(mode=mode, method="POST", path=self.path,
                            forwarded=True, request_target=target,
                            upstream_status=status,
                            error=str(err) if err else None, **parsed)
                if err is not None:
                    return self._send(502, {"error":
                                            f"upstream unreachable: "
                                            f"{err}"})
                return self._send(status, resp)
            if mode == "drop_after_verify":
                status, resp, err = self._upstream("POST", self.path,
                                                   req_body)
                parsed = parse_ack(resp) if err is None and 200 <= status < 300 else {}
                verdict = verify_commit_ack(parsed, target)
                raw_text = (resp or b"").decode("utf-8", "replace") \
                    if err is None else None
                core.record(mode=mode, method="POST", path=self.path,
                            forwarded=True, request_target=target,
                            upstream_status=status,
                            upstream_ack_sha256=_sha(resp),
                            # FULL raw ACK body (commit JSON only — no
                            # headers/Auth/secrets) so any reviewer can
                            # independently recompute the sha256 above
                            upstream_ack_raw=raw_text,
                            error=str(err) if err else None,
                            **parsed, **verdict)
                # Drop ONLY after a usable real 2xx ACK. Invalid ACKs and
                # upstream failures remain ordinary forwarded failures.
                if err is not None:
                    return self._send(502, {"error": f"upstream unreachable: {err}"})
                if not verdict["verified_committed"]:
                    return self._send(status, resp)
                try:
                    self.connection.close()
                except OSError:
                    pass
                return
            # no-forward fabrication modes (each ONE predicate off)
            status, body = fabricate(mode, target)
            core.record(mode=mode, method="POST", path=self.path,
                        forwarded=False, request_target=target,
                        fabricated_status=status)
            return self._send(status, body)
        finally:
            core.finish_commit()          # disconnect-safe lifecycle


def _sha(b):
    import hashlib
    return hashlib.sha256(b or b"").hexdigest()


def main():
    host, port, upstream = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    Handler.core = ProxyCore()
    Handler.upstream = upstream.rstrip("/")
    srv = ThreadingHTTPServer((host, port), Handler)
    srv.daemon_threads = True
    print(f"[p25-proxy] serving {host}:{port} -> {Handler.upstream}",
          flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
