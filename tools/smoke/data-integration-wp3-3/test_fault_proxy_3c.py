#!/usr/bin/env python3
"""Self-contained regression for fault_proxy_3c.py — no DVS needed.

Proves the two properties the 3C ACK-fault scenario depends on:

1. CHUNKED TRANSPARENCY: a request sent with Transfer-Encoding: chunked
   (the framing PBS 3B's io.Pipe stage upload produces) crosses the proxy
   byte-for-byte — the upstream sees the exact bytes — and the response
   streams back intact. This is the gap that made reusing the WP3.2 proxy
   impossible (it reads bodies strictly by Content-Length).
2. DROP-ONLY-AFTER-COMMITTED: with a rule armed, a 2xx COMMITTED
   outputs:commit gets its full response read upstream, then the caller's
   socket is hard-reset; a non-COMMITTED upstream answer passes through
   and leaves the rule armed. A byte-identical replay passes through and
   both crossings log the same request SHA256.

Run: python3 -I tools/smoke/data-integration-wp3-3/test_fault_proxy_3c.py
"""

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
PROXY = os.path.join(HERE, "fault_proxy_3c.py")


def read_body(rfile, headers):
    if "chunked" in (headers.get("Transfer-Encoding") or "").lower():
        out = bytearray()
        while True:
            n = int(rfile.readline(1024).strip().split(b";")[0], 16)
            if n == 0:
                rfile.readline(1024)
                break
            remaining = n
            while remaining:
                c = rfile.read(min(remaining, 65536))
                out += c
                remaining -= len(c)
            rfile.read(2)
        return bytes(out)
    n = int(headers.get("Content-Length") or 0)
    return rfile.read(n) if n else b""


class Upstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen = {}
    lock = threading.Lock()

    def log_message(self, fmt, *args):
        pass

    def _finish(self, status, obj):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        body = read_body(self.rfile, self.headers)
        with Upstream.lock:
            Upstream.seen.setdefault(self.path, []).append(
                hashlib.sha256(body).hexdigest())
        if self.path.endswith("/outputs:commit"):
            ok = b'"idempotency_key":"k1"' in body or b'"idempotency_key": "k1"' in body
            if ok:
                self._finish(200, {"transaction": {"status": "COMMITTED", "id": "txn-1"}})
            else:
                self._finish(500, {"error": "boom"})
        else:
            self._finish(200, {"echo_sha256": hashlib.sha256(body).hexdigest(),
                               "echo_len": len(body)})

    def do_GET(self):
        with Upstream.lock:
            Upstream.seen.setdefault(self.path, []).append("GET")
        body = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def raw_request(port, method, path, body, chunked):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    if chunked:
        framing = [
            f"{method} {path} HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n".encode()
        ]
        # two uneven chunks to prove framing, not size luck
        mid = max(1, len(body) * 2 // 3)
        for part in (body[:mid], body[mid:]):
            framing.append(b"%x\r\n" % len(part) + part + b"\r\n")
        framing.append(b"0\r\n\r\n")
        payload = b"".join(framing)
    else:
        payload = (f"{method} {path} HTTP/1.1\r\nHost: x\r\n"
                   f"Content-Length: {len(body)}\r\n\r\n").encode() + body
    s.sendall(payload)
    s.settimeout(10)
    try:
        data = s.recv(65536)
        more = True
        while more:
            try:
                chunk = s.recv(65536)
                if not chunk:
                    more = False
                else:
                    data += chunk
            except socket.timeout:
                more = False
    except ConnectionResetError:
        data = b"<RST>"
    s.close()
    return data


def ctl(port, method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                 data=json.dumps(body).encode() if body else None,
                                 method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def main():
    up = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    threading.Thread(target=up.serve_forever, daemon=True).start()
    up_port = up.server_address[1]

    evd = tempfile.mkdtemp(prefix="wp33c-proxy-test-")
    proc = subprocess.Popen(
        [sys.executable, "-I", PROXY, "--listen", "127.0.0.1", "--port", "0",
         "--control", "0", "--upstream", f"http://127.0.0.1:{up_port}",
         "--evidence-dir", evd, "--timeout", "10"],
        stdout=subprocess.PIPE, text=True)
    line = proc.stdout.readline()  # startup banner carries the chosen ports
    px_port = int(line.split("listening on 127.0.0.1:")[1].split(" ")[0])
    ctl_port = int(line.split("(control :")[1].split(")")[0])

    failures = []

    def check(name, ok, detail=""):
        print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
        if not ok:
            failures.append(name)

    try:
        # 1. chunked transparency
        body = b"x" * 200_000 + b"END"
        resp = raw_request(px_port, "POST", "/v1/datasets/d/files/rows:scan", body, chunked=True)
        ok = (b"200" in resp.split(b"\r\n", 1)[0]
              and hashlib.sha256(body).hexdigest().encode() in resp)
        seen = Upstream.seen.get("/v1/datasets/d/files/rows:scan", [])
        check("chunked request crosses byte-exact", ok and seen == [hashlib.sha256(body).hexdigest()],
              f"upstream_shas={len(seen)}")

        # plain GET passthrough
        resp = raw_request(px_port, "GET", "/healthz", b"", chunked=False)
        check("GET passthrough", b"200" in resp.split(b"\r\n", 1)[0])

        # 2. non-COMMITTED upstream passes through and keeps rule armed
        ctl(ctl_port, "POST", "/drop-next",
            {"method": "POST", "path": "/v1/datasets/d/outputs:commit", "key": ""})
        resp = raw_request(px_port, "POST", "/v1/datasets/d/outputs:commit",
                           b'{"idempotency_key":"other"}', chunked=False)
        check("non-COMMITTED passes through, rule kept", b"500" in resp.split(b"\r\n", 1)[0])

        # 3. COMMITTED -> full response read, then RST to caller
        commit_body = b'{"idempotency_key":"k1","files":[]}'
        sha = hashlib.sha256(commit_body).hexdigest()
        resp = raw_request(px_port, "POST", "/v1/datasets/d/outputs:commit",
                           commit_body, chunked=False)
        check("COMMITTED ACK hard-dropped", resp == b"<RST>" or b"200" not in resp.split(b"\r\n", 1)[0],
              f"first bytes={resp[:20]!r}")

        events = ctl(ctl_port, "GET", "/events")["events"]
        drops = [e for e in events if e["action"] == "ack_dropped"]
        check("ack_dropped logged with digest + txn",
              len(drops) == 1 and drops[0]["request_sha256"] == sha
              and drops[0]["transaction_id"] == "txn-1" and drops[0]["upstream_status"] == 200)

        # 4. byte-identical replay passes through with the same digest
        resp = raw_request(px_port, "POST", "/v1/datasets/d/outputs:commit",
                           commit_body, chunked=False)
        check("replay passes through 200", b"200" in resp.split(b"\r\n", 1)[0])
        events = ctl(ctl_port, "GET", "/events")["events"]
        passes = [e for e in events if e["action"] == "commit_passthrough"
                  and e["request_sha256"] == sha]
        check("replay digest == dropped digest", len(passes) == 1)

        # no Authorization material in events
        blob = json.dumps(events)
        check("events contain no auth material", "Bearer" not in blob and "Authorization" not in blob)

        # 5. mid-stage truncation: upstream receives >0 bytes then EOF
        ctl(ctl_port, "POST", "/reset", {})
        ctl(ctl_port, "POST", "/truncate-stage",
            {"path_suffix": "/outputs:stage", "after_bytes": 70000, "count": 2})
        stage_body = b"y" * 200_000
        resp = raw_request(px_port, "POST", "/v1/datasets/d/outputs:stage",
                           stage_body, chunked=True)
        truncated_events = ctl(ctl_port, "GET", "/events")["events"]
        st_events = [e for e in truncated_events if e["action"] == "stage_truncated"]
        check("stage truncated mid-body after >0 bytes",
              resp in (b"", b"<RST>") or not resp.startswith(b"HTTP/1.1 200"),
              f"first={resp[:16]!r}")
        check("truncation event logged", len(st_events) == 1
              and st_events[0]["bytes_forwarded"] == 70000
              and st_events[0]["remaining_count"] == 1)
        # second matching upload is truncated too, then the rule is spent
        raw_request(px_port, "POST", "/v1/datasets/d/outputs:stage",
                    stage_body, chunked=True)
        truncated_events = ctl(ctl_port, "GET", "/events")["events"]
        st_events = [e for e in truncated_events if e["action"] == "stage_truncated"]
        check("truncate count decrements to zero", len(st_events) == 2
              and st_events[1]["remaining_count"] == 0)
    finally:
        proc.terminate()
        up.shutdown()

    print("RESULT:", "PASS" if not failures else f"FAIL {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
