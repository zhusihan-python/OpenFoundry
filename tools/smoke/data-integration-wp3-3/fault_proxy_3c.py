#!/usr/bin/env python3
"""WP3.3 3C test-only DVS fault proxy — transparent, chunked-capable.

Why a new fixture instead of reusing tools/smoke/data-integration-wp3-2/
fault_proxy.py: that proxy reads request bodies strictly by Content-Length
(fault_proxy.py handle_request: `int(self.headers.get('Content-Length',
'0'))` + `self.rfile.read(length)`), so a `Transfer-Encoding: chunked`
request body — which PBS 3B produces for the streaming outputs:stage
upload through io.Pipe — is misread as a 0-byte body and the remaining
chunked bytes corrupt the next request on the connection. That file is a
WP3.2 deliverable and stays untouched; this 3C fixture is the corrected
transport used only by the WP3.3 3C fault scenarios.

Behavior:

- Every non-commit request is forwarded chunk-by-chunk in BOTH directions
  (64 KiB read granularity): chunked and Content-Length request bodies are
  re-emitted upstream as chunked; responses stream back re-framed as
  chunked. The streaming stage body is never buffered in memory.
- `outputs:commit` requests (short JSON) are buffered with a 1 MiB bound,
  forwarded byte-identical, and answered with the real upstream response.
  When a drop rule is armed and matches (method + exact path, optionally
  idempotency key parsed from the body), the proxy first reads the FULL
  upstream response and only drops the connection downstream when it can
  prove DVS committed: HTTP 2xx whose JSON transaction has
  status=="COMMITTED" and a non-empty id. Only then does the PBS-side
  socket get a hard reset (SO_LINGER 0), so PBS observes a pure transport
  error AFTER durability. Non-COMMITTED upstream responses pass through
  and leave the rule armed.
- Every outputs:commit crossing the proxy is logged to events.jsonl with
  the exact request-body SHA256 (key, method, path, upstream status,
  transaction id, action) so the driver can prove byte-identical replay.
  Non-commit forwards are logged as one `forward` line each (method,
  path, upstream status) — transparency evidence. Authorization headers
  and JWT material are never logged.

Control plane (separate port, isolated network only):
  POST /drop-next {"method":"POST","path":"/v1/datasets/<rid>/outputs:commit",
                   "key":"optional-exact-idempotency-key"}
  POST /truncate-stage {"path_suffix":"/outputs:stage","after_bytes":1048576,
                        "count":20} — truncate the next N matching streaming
                  uploads mid-body (upstream gets >0 bytes, then EOF; caller
                  gets a hard reset). Used by the 3C mid-stage failure
                  negative control.
  POST /reset      {} — clear armed rules
  GET  /healthz    {}
  GET  /events     — full events.jsonl as JSON lines
"""

import argparse
import datetime
import gzip
import hashlib
import http.client
import json
import os
import socket
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Hop-by-hop / framing headers stripped when re-emitting. Content-Length and
# Transfer-Encoding are deliberately dropped in both directions: this proxy
# re-frames (requests -> chunked upstream for streamed bodies / exact length
# for the buffered commit body; responses -> chunked downstream).
HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade",
    "host", "content-length",
}
MAX_COMMIT_BODY = 1 << 20
MAX_DROP_RESPONSE = 8 << 20
STREAM_SLICE = 64 * 1024


def iso_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def read_chunked(rfile):
    """Yield a chunked request body chunk by chunk (trailing CRLF consumed)."""
    while True:
        line = rfile.readline(1024)
        if not line:
            raise ConnectionError("eof inside chunked body")
        try:
            size = int(line.strip().split(b";")[0], 16)
        except ValueError:
            raise ConnectionError("bad chunk size line")
        if size == 0:
            while True:  # trailers until blank line
                t = rfile.readline(1024)
                if t in (b"\r\n", b"\n", b""):
                    break
            return
        remaining = size
        while remaining > 0:
            chunk = rfile.read(min(remaining, STREAM_SLICE))
            if not chunk:
                raise ConnectionError("eof inside chunk")
            yield chunk
            remaining -= len(chunk)
        crlf = rfile.read(2)
        if crlf not in (b"\r\n", b"\n", b""):
            raise ConnectionError("bad chunk terminator")


def read_by_length(rfile, total):
    remaining = total
    while remaining > 0:
        chunk = rfile.read(min(remaining, STREAM_SLICE))
        if not chunk:
            raise ConnectionError("eof inside body")
        yield chunk
        remaining -= len(chunk)


class DropRule:
    def __init__(self, method, path, key):
        self.method = method.upper()
        self.path = path
        self.key = key or ""


class TruncateRule:
    """One-shot repeated mid-body truncation for the FIRST N matching
    streaming uploads: after `after_bytes` request bytes have been read from
    the caller, the upstream socket is closed mid-chunked-body (no
    terminator) and the caller's socket is hard-reset. The dataset service
    has demonstrably received >0 bytes when the failure hits."""

    def __init__(self, path_suffix, after_bytes, count):
        self.path_suffix = path_suffix
        self.after_bytes = after_bytes
        self.count = count


class ProxyState:
    def __init__(self, upstream, evidence_dir, timeout):
        self.upstream = upstream
        self.evidence_dir = evidence_dir
        self.timeout = timeout
        self.lock = threading.Lock()
        self.rule = None
        self.truncate = None
        self.events_path = os.path.join(evidence_dir, "events.jsonl")
        os.makedirs(evidence_dir, exist_ok=True)

    def arm(self, method, path, key):
        with self.lock:
            if self.rule is not None:
                return False
            self.rule = DropRule(method, path, key)
            return True

    def arm_truncate(self, path_suffix, after_bytes, count):
        with self.lock:
            if self.truncate is not None:
                return False
            self.truncate = TruncateRule(path_suffix, after_bytes, count)
            return True

    def take_truncate(self, path_no_query):
        """Return the active truncate rule if this path matches (count
        decremented atomically); None otherwise."""
        with self.lock:
            t = self.truncate
            if t is None or not path_no_query.endswith(t.path_suffix):
                return None
            if t.count <= 0:
                return None
            t.count -= 1
            return t

    def truncate_done(self):
        with self.lock:
            return self.truncate is not None and self.truncate.count <= 0

    def reset(self):
        with self.lock:
            self.rule = None
            self.truncate = None
            return True

    def take_if_match(self, method, path_no_query, key):
        """Consume the armed rule when it matches this commit; keep it armed
        otherwise (including when the upstream response later turns out
        non-COMMITTED — matching is re-checked before the drop)."""
        with self.lock:
            if self.rule is None:
                return None
            r = self.rule
            if r.method != method.upper():
                return None
            if r.path and r.path != path_no_query:
                return None
            if r.key and r.key != (key or ""):
                return None
            return r

    def clear_armed(self, rule):
        with self.lock:
            if self.rule is not None and self.rule is rule:
                self.rule = None

    def record(self, event):
        line = json.dumps(event, sort_keys=True)
        with self.lock:
            with open(self.events_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def events(self):
        with self.lock:
            if not os.path.exists(self.events_path):
                return []
            with open(self.events_path, "r", encoding="utf-8") as f:
                return [json.loads(l) for l in f if l.strip()]


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "wp33c-fault-proxy/1"

    def log_message(self, fmt, *args):  # events.jsonl is the only log
        pass

    def handle(self):
        try:
            BaseHTTPRequestHandler.handle(self)
        except (ConnectionError, BrokenPipeError, socket.timeout, OSError):
            self.close_connection = True

    def finish(self):
        # After a hard reset the socket is gone; flushing would raise into
        # socketserver's error handler for no diagnostic value.
        try:
            BaseHTTPRequestHandler.finish(self)
        except (ConnectionError, BrokenPipeError, socket.timeout, OSError):
            pass

    def do_GET(self):
        self.proxy()

    def do_POST(self):
        self.proxy()

    def do_PUT(self):
        self.proxy()

    def do_PATCH(self):
        self.proxy()

    def do_DELETE(self):
        self.proxy()

    def do_HEAD(self):
        self.proxy()

    # ---- request body -----------------------------------------------------

    def request_body_iter(self):
        te = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in te:
            return "chunked", read_chunked(self.rfile)
        cl = self.headers.get("Content-Length")
        if cl:
            n = int(cl)
            if n < 0:
                raise ValueError("negative content-length")
            if n == 0:
                return "none", iter(())
            return "length", read_by_length(self.rfile, n)
        return "none", iter(())

    def forward_headers(self):
        out = {}
        for k, v in self.headers.items():
            if k.lower() in HOP:
                continue
            out[k] = v
        return out

    # ---- responses --------------------------------------------------------

    def simple(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def stream_response(self, resp):
        """Re-serve an upstream response, streaming, re-framed as chunked."""
        bodyless = resp.status in (204, 304) or self.command == "HEAD"
        self.send_response(resp.status, resp.reason)
        for k, v in resp.getheaders():
            if k.lower() in HOP:
                continue
            self.send_header(k, v)
        chunked = False
        if not bodyless:
            self.send_header("Transfer-Encoding", "chunked")
            chunked = True
        self.send_header("Connection", "close")
        self.end_headers()
        if bodyless:
            return
        while True:
            b = resp.read(STREAM_SLICE)
            if not b:
                break
            self.wfile.write(b"%x\r\n" % len(b))
            self.wfile.write(b)
            self.wfile.write(b"\r\n")
        if chunked:
            self.wfile.write(b"0\r\n\r\n")
        self.close_connection = True

    def buffered_response(self, resp, body):
        self.send_response(resp.status, resp.reason)
        for k, v in resp.getheaders():
            if k.lower() in HOP:
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def hard_reset(self):
        """Close the downstream socket with an RST (not a clean FIN) so the
        caller records an explicit transport error."""
        try:
            self.connection.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
        except OSError:
            pass
        self.close_connection = True
        try:
            self.connection.close()
        except OSError:
            pass

    # ---- core -------------------------------------------------------------

    def proxy(self):
        st = self.server.state
        try:
            framing, body_iter = self.request_body_iter()
        except (ConnectionError, ValueError):
            self.simple(400, {"error": "bad_request_framing"})
            return

        path = self.path
        no_query = path.split("?", 1)[0]
        is_commit = (
            self.command == "POST" and no_query.endswith("/outputs:commit")
        )
        if not is_commit:
            trunc = st.take_truncate(no_query)
            if trunc is not None and framing != "none":
                self.truncate_stage(st, path, body_iter, trunc)
                return

        if is_commit:
            self.proxy_commit(st, path, no_query, body_iter)
            return

        hdrs = self.forward_headers()
        body = body_iter if framing != "none" else None
        conn = http.client.HTTPConnection(
            st.upstream[0], st.upstream[1], timeout=st.timeout
        )
        try:
            conn.request(self.command, path, body=body, headers=hdrs)
            resp = conn.getresponse()
            st.record({
                "action": "forward", "method": self.command,
                "path": no_query, "upstream_status": resp.status,
                "ts": iso_now(),
            })
            self.stream_response(resp)
        except (ConnectionError, OSError, http.client.HTTPException):
            try:
                self.simple(502, {"error": "upstream_unavailable"})
            except OSError:
                self.close_connection = True
        finally:
            conn.close()

    def truncate_stage(self, st, path, body_iter, rule):
        """Forward exactly rule.after_bytes of the streaming upload, then
        close the UPSTREAM socket mid-chunked-body (terminator withheld) and
        hard-reset the caller. The dataset service has received a partial
        (>0 byte) body when its read errors out — a real mid-stage failure,
        not a pre-flight rejection."""
        buf = bytearray()
        try:
            for chunk in body_iter:
                buf += chunk
                if len(buf) >= rule.after_bytes:
                    break
        except (ConnectionError, OSError):
            pass  # caller gave up early; still proceed with what we hold
        partial = bytes(buf[:rule.after_bytes])
        hdrs = self.forward_headers()
        conn = http.client.HTTPConnection(
            st.upstream[0], st.upstream[1], timeout=st.timeout
        )
        try:
            conn.putrequest(self.command, path, skip_host=True, skip_accept_encoding=True)
            conn.putheader("Host", f"{st.upstream[0]}:{st.upstream[1]}")
            for k, v in hdrs.items():
                conn.putheader(k, v)
            conn.putheader("Transfer-Encoding", "chunked")
            conn.endheaders()
            # write the partial chunks WITHOUT the 0-terminator, then kill
            # the socket: the upstream reader must hit EOF mid-body.
            conn.sock.sendall(b"%x\r\n" % len(partial) + partial + b"\r\n")
            conn.sock.close()
            st.record({
                "action": "stage_truncated", "method": self.command,
                "path": path.split("?", 1)[0], "bytes_forwarded": len(partial),
                "remaining_count": rule.count, "ts": iso_now(),
            })
        except (ConnectionError, OSError, http.client.HTTPException) as e:
            st.record({
                "action": "stage_truncate_error", "method": self.command,
                "path": path.split("?", 1)[0], "error": type(e).__name__,
                "remaining_count": rule.count, "ts": iso_now(),
            })
        finally:
            try:
                conn.close()
            except OSError:
                pass
        self.hard_reset()

    def proxy_commit(self, st, path, no_query, body_iter):
        buf = bytearray()
        for chunk in body_iter:
            buf += chunk
            if len(buf) > MAX_COMMIT_BODY:
                self.simple(413, {"error": "commit_body_too_large"})
                return
        body = bytes(buf)
        req_sha = hashlib.sha256(body).hexdigest()
        try:
            key = json.loads(body or b"{}").get("idempotency_key", "")
        except (ValueError, AttributeError):
            key = ""

        rule = st.take_if_match(self.command, no_query, key)

        hdrs = self.forward_headers()
        conn = http.client.HTTPConnection(
            st.upstream[0], st.upstream[1], timeout=st.timeout
        )
        try:
            conn.request(self.command, path, body=body, headers=hdrs)
            resp = conn.getresponse()
            resp_bytes = resp.read(MAX_DROP_RESPONSE + 1)
            if len(resp_bytes) > MAX_DROP_RESPONSE:
                st.record({
                    "action": "commit_response_oversize", "method": "POST",
                    "path": no_query, "key": key, "request_sha256": req_sha,
                    "upstream_status": resp.status, "ts": iso_now(),
                })
                self.simple(502, {"error": "commit_response_oversize"})
                return
            committed, txn = self.classify(resp, resp_bytes)
            if rule is not None and committed:
                # DVS has returned a FULL 2xx COMMITTED response: the commit
                # is durable upstream. Only now may the caller be cut off.
                st.clear_armed(rule)
                st.record({
                    "action": "ack_dropped", "method": "POST",
                    "path": no_query, "key": key, "request_sha256": req_sha,
                    "upstream_status": resp.status, "transaction_id": txn,
                    "ts": iso_now(),
                })
                self.hard_reset()
                return
            st.record({
                "action": "commit_passthrough", "method": "POST",
                "path": no_query, "key": key, "request_sha256": req_sha,
                "upstream_status": resp.status, "transaction_id": txn,
                "dropped_rule_kept": rule is not None, "ts": iso_now(),
            })
            self.buffered_response(resp, resp_bytes)
        except (ConnectionError, OSError, http.client.HTTPException):
            try:
                self.simple(502, {"error": "upstream_unavailable"})
            except OSError:
                self.close_connection = True
        finally:
            conn.close()

    @staticmethod
    def classify(resp, resp_bytes):
        if not (200 <= resp.status < 300):
            return False, ""
        payload = resp_bytes
        if (resp.getheader("Content-Encoding") or "").lower() == "gzip":
            try:
                payload = gzip.decompress(resp_bytes)
            except (OSError, ValueError):
                return False, ""
        try:
            obj = json.loads(payload)
            txn = (obj.get("transaction") or {})
        except ValueError:
            return False, ""
        ok = txn.get("status") == "COMMITTED" and bool(txn.get("id"))
        return ok, str(txn.get("id") or "")


class ControlHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "wp33c-fault-proxy-control/1"

    def log_message(self, fmt, *args):
        pass

    def _reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        st = self.server.state
        if self.path == "/healthz":
            self._reply(200, {"ok": True})
        elif self.path == "/events":
            self._reply(200, {"events": st.events()})
        else:
            self._reply(404, {"error": "not_found"})

    def do_POST(self):
        st = self.server.state
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            self._reply(400, {"error": "invalid_json"})
            return
        if self.path == "/drop-next":
            ok = st.arm(
                body.get("method", "POST"),
                body.get("path", ""),
                body.get("key", ""),
            )
            self._reply(200 if ok else 409, {"armed": ok})
        elif self.path == "/truncate-stage":
            ok = st.arm_truncate(
                body.get("path_suffix", "/outputs:stage"),
                int(body.get("after_bytes", 1 << 20)),
                int(body.get("count", 20)),
            )
            self._reply(200 if ok else 409, {"armed": ok})
        elif self.path == "/reset":
            st.reset()
            self._reply(200, {"reset": True})
        else:
            self._reply(404, {"error": "not_found"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--control", type=int, default=8081)
    ap.add_argument("--upstream", required=True,
                    help="e.g. http://dvs:50078")
    ap.add_argument("--evidence-dir", required=True)
    ap.add_argument("--timeout", type=float, default=300.0)
    args = ap.parse_args()

    u = args.upstream.rstrip("/")
    if not u.startswith("http://"):
        raise SystemExit("only http:// upstream is supported")
    host, _, port = u[len("http://"):].partition(":")
    state = ProxyState((host, int(port or 80)), args.evidence_dir, args.timeout)

    proxy = ThreadingHTTPServer((args.listen, args.port), ProxyHandler)
    proxy.state = state
    proxy.daemon_threads = True
    control = ThreadingHTTPServer((args.listen, args.control), ControlHandler)
    control.state = state
    control.daemon_threads = True
    threading.Thread(target=control.serve_forever, daemon=True).start()
    print(f"wp33c-fault-proxy listening on {args.listen}:{proxy.server_address[1]} "
          f"(control :{control.server_address[1]}) upstream {args.upstream} "
          f"evidence {args.evidence_dir}", flush=True)
    proxy.serve_forever()


if __name__ == "__main__":
    main()
