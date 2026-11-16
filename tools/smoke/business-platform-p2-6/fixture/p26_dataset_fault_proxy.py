#!/usr/bin/env python3
"""P26 W7 dataset fault PROXY (NEW, owned by W7).

Sits between an aux connector's DATASET_SERVICE_URL and the real
dataset-versioning-service (in the live phase; not bound now). Forwards
the EXACT request bytes + Authorization to ONE fixed configured upstream
— never to any other host — and records only safe ledger facts:

  * request body sha256 + size + idempotency key hash (key_hash);
    raw request bodies are NEVER stored in any ledger/log;
  * Authorization header values are forwarded but never stored, parsed,
    echoed, or logged;
  * upstream ACK safe parsed fields only (dataset/txn/status/key/
    fingerprint/content_hash/lineage-run) + ack sha256; raw ACK bytes
    are never stored;
  * verified_committed is a STRICT verdict: nonzero-UUID transaction id
    + status COMMITTED + exact dataset target + key echo match +
    64-hex fingerprint. The proxy ledger is NEVER treated as commit
    proof — the smoke layer independently proves commits against the
    real Dataset service (GET receipt / SQL).

Controlled modes (POST /admin/mode; exactly one behavior per mode):
  passthrough          — forward everything, full commit ledger.
  drop_ack_once        — forward; if the upstream returns a REAL first
                         commit (201 + strict verdict), DROP the response
                         to the client ONCE (connection closed, client
                         sees timeout/unknown) and auto-revert to
                         passthrough. Non-201/non-verified responses
                         pass through untouched (one-shot not consumed).
  fail_before_forward  — respond 502 immediately, forwarded=false
                         (provable zero upstream arrival).
  hold_request         — hold the NEXT commit dispatch at a BOUNDED
                         barrier BEFORE forwarding (real in-flight
                         blocker for receipt-404-while-commit-inflight);
                         release via /admin/barrier/release or auto at
                         the deadline.
  hold_response        — forward, then hold the upstream response at a
                         BOUNDED barrier before returning it.

Barriers are bounded (<=30s), auto-release on timeout, released by
admin, and released by core.close() — a failed assertion can never wedge
a proxy thread. Receipt GETs (…/outputs/commits/{key}) always forward
(ledgered separately) so query budgets are provable. Lifecycle counters
are unambiguous: started / finished / inflight on EVERY dispatch
including fabricated ones.

Pure core (P26ProxyCore) is socket-free and testable; main() refuses to
bind unless the Root release file lists `http-proxy-serve`.

Usage (only when released):
  python3 -I p26_dataset_fault_proxy.py serve 0.0.0.0 8091 \
      http://dataset:50078
"""

import json
import re
import sys
import threading
import time
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.dont_write_bytecode = True
sys.path.insert(0, str(__file__).rsplit("/", 1)[0] + "/../http")

from p26_http_lib import (MAX_BARRIER_MS, RELEASE_PHASES, BoundedEvent,
                          key_hash, refuse_serve, sha256_hex, utcnow_iso)

MODES = ("passthrough", "drop_ack_once", "fail_before_forward",
         "hold_request_once", "hold_response", "hold_drop_once",
         "hold_receipt_once", "fail_receipt_once",
         "drop_then_fail_rest")

COMMIT_PATH_RE = re.compile(
    r"^/api/v1/datasets/([0-9a-fA-F-]{36})/outputs:commit$")
RECEIPT_PATH_RE = re.compile(
    r"^/api/v1/datasets/([0-9a-fA-F-]{36})/outputs/commits/"
    r"([A-Za-z0-9._~:=-]{1,200})$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
LEDGER_CAP = 1000
FORWARD_HEADERS = ("Content-Type", "Authorization")
# The local admin credential must NEVER ride a business outward hop,
# even if a caller smuggles it into per-request headers (defense).
STRIP_OUTBOUND_HEADERS = ("X-P26-Admin-Token",)
MAX_BODY_BYTES = (64 << 20) + 1          # contract cap; 413 beyond

# Old substrate denylist for the ONE fixed upstream (serve boundary):
# the P26 proxy must NEVER point at the old q3 fixture or removed P25 aux.
OLD_UPSTREAM_MARKERS = ("p25fixture", "p25proxy", "p25connector",
                        "fixture-1", "aux1709", "17092", "17093", "17094",
                        "17090")


def validate_upstream(base):
    """Returns (ok, reason). Enforced at the serve boundary only."""
    if not base or not re.match(r"^https?://", base):
        return False, "upstream base must be http(s)"
    low = base.lower()
    for marker in OLD_UPSTREAM_MARKERS:
        if marker in low:
            return False, f"upstream matches old substrate marker " \
                          f"{marker!r}"
    return True, "ok"


def parse_commit_body(body):
    """Safe extraction: idempotency key + explicit branch. The body
    itself is never stored anywhere."""
    try:
        doc = json.loads(body or b"")
    except (ValueError, TypeError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict):
        return None
    return {"idempotency_key": doc.get("idempotency_key"),
            "branch": doc.get("branch")}


def parse_ack(body_bytes):
    """Safe parsed ACK fields (W1 response shape). No raw retention."""
    try:
        doc = json.loads(body_bytes or b"")
    except (ValueError, TypeError, UnicodeDecodeError):
        return {}
    if not isinstance(doc, dict):
        return {}
    txn = doc.get("transaction")
    txn = txn if isinstance(txn, dict) else {}
    meta = txn.get("metadata")
    meta = meta if isinstance(meta, dict) else {}
    lineage = meta.get("lineage")
    lineage = lineage if isinstance(lineage, dict) else {}
    return {
        "ack_dataset_id": doc.get("dataset_id"),
        "ack_branch": doc.get("branch"),
        "ack_txn_id": txn.get("id"),
        "ack_txn_status": txn.get("status"),
        "ack_key": doc.get("idempotency_key"),
        "ack_fingerprint": doc.get("request_fingerprint"),
        "ack_content_hash": meta.get("content_hash"),
        "ack_lineage_run_id": lineage.get("sync_run_id"),
    }


def _nonzero_uuid(value):
    try:
        return uuid.UUID(str(value)) != uuid.UUID(int=0)
    except (ValueError, AttributeError, TypeError):
        return False


def verify_commit_ack(parsed, requested_target, request_key,
                      request_branch, forwarded_body_sha256):
    """STRICT verdict — every predicate must hold, INCLUDING the W1 byte
    contract: the ACK fingerprint must equal sha256 of the EXACT raw
    bytes this proxy forwarded (never trust a well-formed but foreign
    fingerprint)."""
    checks = {
        "txn_nonzero_uuid": _nonzero_uuid(parsed.get("ack_txn_id")),
        "status_committed": parsed.get("ack_txn_status") == "COMMITTED",
        "dataset_exact": str(parsed.get("ack_dataset_id", "")).lower()
        == str(requested_target or "").lower(),
        "key_echo_match": bool(request_key) and
        parsed.get("ack_key") == request_key,
        "fingerprint_hex64": bool(
            HEX64.fullmatch(str(parsed.get("ack_fingerprint") or ""))),
        "fingerprint_matches_body": bool(forwarded_body_sha256) and
        parsed.get("ack_fingerprint") == forwarded_body_sha256,
        "branch_echo_match": (request_branch is None)
        or (parsed.get("ack_branch") == request_branch),
        "content_hash_hex64": bool(
            HEX64.fullmatch(str(parsed.get("ack_content_hash") or ""))),
    }
    return {"verified_committed": all(checks.values()), **checks}


class P26ProxyCore:
    """Socket-free core. transport is a callable
    request(method, url, headers, body, timeout_s) -> HTTPResponse."""

    def __init__(self, upstream_base, boot_id=None,
                 clock=time.monotonic, barrier_max_ms=MAX_BARRIER_MS,
                 admin_token=None):
        self.upstream_base = (upstream_base or "").rstrip("/")
        self.boot_id = boot_id or str(uuid.uuid4())
        self.started_at = utcnow_iso()
        self.mode = "passthrough"
        self.admin_token = admin_token   # None => admin DENIED (fail closed)
        self.barrier_max_ms = max(0, min(int(barrier_max_ms),
                                         MAX_BARRIER_MS))
        self.lock = threading.Lock()
        self.ledger = []
        self.stats = {"commit_started": 0, "commit_finished": 0,
                      "commit_inflight": 0, "receipt_queries": 0,
                      "non_commit_forwarded": 0, "drops": 0,
                      "modes": {}}
        self._barrier = None                    # single active BoundedEvent
        self._clock = clock

    # ---- lifecycle -------------------------------------------------------
    def _begin(self):
        with self.lock:
            self.stats["commit_started"] += 1
            self.stats["commit_inflight"] += 1
            self.stats["modes"][self.mode] = \
                self.stats["modes"].get(self.mode, 0) + 1
        done = []

        def finish():
            if done:
                return
            done.append(1)
            with self.lock:
                self.stats["commit_finished"] += 1
                self.stats["commit_inflight"] = max(
                    0, self.stats["commit_inflight"] - 1)
        return finish

    def _record(self, **kv):
        with self.lock:
            kv["ts"] = utcnow_iso()
            kv["boot_id"] = self.boot_id   # bind every observation to THIS boot
            self.ledger.append(kv)
            if len(self.ledger) > LEDGER_CAP:
                del self.ledger[:len(self.ledger) - LEDGER_CAP]

    # ---- barriers ----------------------------------------------------------
    def _take_barrier(self, max_ms=None):
        if max_ms is None:
            max_ms = self.barrier_max_ms
        with self.lock:
            if self._barrier is None:
                self._barrier = BoundedEvent(max_ms)
            return self._barrier

    def barrier_state(self):
        with self.lock:
            return self._barrier.state() if self._barrier else None

    def release_barrier(self):
        """Idempotent success semantics: releasing when nothing is held
        is a NO-OP success (nothing remains unreleased) — cleanup checks
        must not misread an idle proxy as a failed release."""
        with self.lock:
            if self._barrier is not None:
                self._barrier.release(by="admin")
        return True

    # ---- forwarding ----------------------------------------------------------
    def _forward(self, transport, method, path, headers, body,
                 timeout_s=60.0):
        """Forward to the ONE fixed upstream. Authorization is passed
        through verbatim and never stored (only header NAMES travel in
        the ledger). The local admin credential is STRIPPED — it never
        reaches the business upstream even by accident."""
        fwd_headers = {h: headers.get(h) for h in FORWARD_HEADERS
                       if headers.get(h) is not None}
        for h in STRIP_OUTBOUND_HEADERS:
            fwd_headers.pop(h, None)
        return transport.request(method, self.upstream_base + path,
                                 headers=fwd_headers, body=body,
                                 timeout_s=timeout_s)

    # ---- commit dispatch ------------------------------------------------
    def handle_commit(self, path, headers, body, transport,
                      timeout_s=60.0):
        """One outputs:commit dispatch. Returns an action dict for the
        HTTP layer / tests:
          {"action": "respond", "status": int, "body": bytes}
          {"action": "drop"}                        # close, no response
        """
        finish = self._begin()
        try:
            m = COMMIT_PATH_RE.match(path)
            target = m.group(1).lower()
            mode = self.mode
            parsed_req = parse_commit_body(body) or {}
            req_key = parsed_req.get("idempotency_key")
            req_branch = parsed_req.get("branch")
            body_sha = sha256_hex(body)
            common = {"kind": "commit", "mode": mode, "path": path,
                      "dataset_target": target,
                      "request_body_sha256": body_sha,
                      "request_body_bytes": len(body or b""),
                      "request_branch": req_branch,
                      "request_key_hash": key_hash(req_key)
                      if req_key else None}

            if mode == "fail_before_forward":
                self._record(**common, forwarded=False,
                             fabricated_status=502)
                return {"action": "respond", "status": 502,
                        "body": json.dumps(
                            {"error": "p26 proxy: fail_before_forward"}
                        ).encode()}

            held_ms = None
            released_by = None
            if mode == "hold_request_once":
                # ONE-SHOT in-flight hold (r3 memo): exactly the FIRST
                # dispatch is parked BEFORE the DVS forward and the mode
                # is atomically disarmed under the same lock — a second
                # same-body POST can never be held again by this mode.
                with self.lock:
                    won_hold = self.mode == "hold_request_once"
                    if won_hold:
                        self.mode = "passthrough"
                        barrier = BoundedEvent(self.barrier_max_ms)
                        self._barrier = barrier
                if won_hold:
                    self._record(**common,
                                 forwarded=False,
                                 held_before_forward=True,
                                 held_phase=True)
                    released, held_ms = barrier.wait()   # bounded
                    released_by = barrier.released_by
                    with self.lock:
                        self._barrier = None
                    common["held_ms"] = held_ms
                    common["held_released_by"] = released_by

            resp = self._forward(transport, "POST", path, headers,
                                 body, timeout_s)
            ack = parse_ack(resp.body) if resp.err is None and \
                200 <= (resp.status or 0) < 300 else {}
            verdict = verify_commit_ack(ack, target, req_key, req_branch,
                                        body_sha)

            if resp.err is not None:
                self._record(**common, forwarded=True,
                             upstream_error=type(resp.err).__name__,
                             **ack)
                return {"action": "respond", "status": 502,
                        "body": json.dumps({"error":
                                            "p26 proxy: upstream "
                                            "unreachable"}).encode()}

            if mode in ("drop_ack_once", "hold_drop_once",
                        "drop_then_fail_rest") \
                    and resp.status == 201 \
                    and verdict["verified_committed"]:
                # ATOMIC one-shot consumption under the lock: among
                # concurrent dispatches exactly ONE may drop. The budget
                # combo additionally transitions to fail_before_forward
                # IN THE SAME LOCK — W3's immediate retry deterministically
                # hits refusal with zero external poll window (r3 memo).
                with self.lock:
                    won = self.mode == mode
                    if won:
                        self.mode = "fail_before_forward" \
                            if mode == "drop_then_fail_rest" \
                            else "passthrough"
                        self.stats["drops"] += 1
                if won:
                    if mode == "hold_drop_once":
                        # DETERMINISTIC KILL WINDOW (R2 item 2): the real
                        # 201 is committed upstream and the response sits
                        # at a BOUNDED barrier. The harness can observe
                        # this entry, stop/kill the owned connector (the
                        # client is provably still blocked in its request
                        # — it cannot have retried), then release: the
                        # ACK is dropped, one-shot consumed. No timing.
                        self._record(**common, forwarded=True,
                                     upstream_status=resp.status,
                                     ack_sha256=sha256_hex(resp.body),
                                     dropped=False, held_for_stop=True,
                                     held_phase=True,
                                     **ack, **verdict)
                        barrier = self._take_barrier()
                        barrier.wait()
                        with self.lock:
                            self._barrier = None
                    self._record(**common, forwarded=True,
                                 upstream_status=resp.status,
                                 ack_sha256=sha256_hex(resp.body),
                                 dropped=True,
                                 held_for_stop=(mode == "hold_drop_once"),
                                 **ack, **verdict)
                    return {"action": "drop"}
                # lost the race: behave as passthrough (recorded below)

            held_resp_ms = None
            if mode == "hold_response":
                barrier = self._take_barrier()
                released, held_resp_ms = barrier.wait()
                with self.lock:
                    self._barrier = None

            rec = dict(common, forwarded=True,
                       upstream_status=resp.status,
                       ack_sha256=sha256_hex(resp.body), **ack,
                       **verdict)
            if held_ms is not None:
                rec["held_ms"] = held_ms
                rec["held_released_by"] = released_by
            if held_resp_ms is not None:
                rec["held_response_ms"] = held_resp_ms
            self._record(**rec, dropped=False)
            return {"action": "respond", "status": resp.status,
                    "body": resp.body}
        finally:
            finish()                              # every exit path

    # ---- receipt query passthrough ---------------------------------------
    def handle_receipt_get(self, path, headers, transport,
                           timeout_s=30.0):
        m = RECEIPT_PATH_RE.match(path)
        key = m.group(2)
        mode = self.mode
        with self.lock:
            self.stats["receipt_queries"] += 1

        if mode == "fail_receipt_once":
            # ONE-SHOT dependency failure on the receipt query (r3
            # memo): the client sees an actual dependency-error code
            # (502), zero upstream arrival; run stays unknown. Disarmed
            # atomically under the lock.
            with self.lock:
                won = self.mode == "fail_receipt_once"
                if won:
                    self.mode = "passthrough"
            if won:
                self._record(kind="receipt_error", path=path,
                             key_hash=key_hash(key), mode=mode,
                             forwarded=False, fabricated_status=502,
                             one_shot=True)
                return {"action": "respond", "status": 502,
                        "body": json.dumps({"error":
                                            "p26 proxy: receipt "
                                            "dependency failure"}
                                           ).encode()}

        resp = self._forward(transport, "GET", path, headers,
                             None, timeout_s)

        if mode == "hold_receipt_once":
            # ONE-SHOT held receipt response (r3 memo, two-instance
            # claim observability): the FIRST receipt GET's response is
            # parked at a bounded barrier (one-shot disarm under the
            # lock — later queries pass through); release via admin.
            # This branch records THE one ledger entry for the query.
            with self.lock:
                won = self.mode == "hold_receipt_once"
                if won:
                    self.mode = "passthrough"
                    barrier = BoundedEvent(self.barrier_max_ms)
                    self._barrier = barrier
            if won:
                self._record(kind="receipt_query", path=path,
                             key_hash=key_hash(key), mode=mode,
                             forwarded=True,
                             upstream_status=resp.status,
                             held_response=True, held_phase=True)
                barrier.wait()
                with self.lock:
                    self._barrier = None
                return {"action": "respond", "status": resp.status,
                        "body": resp.body}

        self._record(kind="receipt_query", path=path,
                     key_hash=key_hash(key), mode=mode,
                     forwarded=resp.err is None,
                     upstream_status=resp.status,
                     upstream_error=type(resp.err).__name__
                     if resp.err is not None else None)
        if resp.err is not None:
            return {"action": "respond", "status": 502,
                    "body": json.dumps({"error":
                                        "p26 proxy: upstream "
                                        "unreachable"}).encode()}
        return {"action": "respond", "status": resp.status,
                "body": resp.body}

    # ---- generic passthrough (non-commit, non-receipt) -------------------
    def handle_other(self, method, path, headers, body, transport,
                     timeout_s=30.0):
        with self.lock:
            self.stats["non_commit_forwarded"] += 1
        resp = self._forward(transport, method, path, headers, body,
                             timeout_s)
        self._record(kind="passthrough(non-commit)", method=method,
                     path=path, forwarded=resp.err is None,
                     upstream_status=resp.status,
                     upstream_error=type(resp.err).__name__
                     if resp.err is not None else None)
        if resp.err is not None:
            return {"action": "respond", "status": 502,
                    "body": json.dumps({"error":
                                        "p26 proxy: upstream "
                                        "unreachable"}).encode()}
        return {"action": "respond", "status": resp.status,
                "body": resp.body}

    # ---- admin ------------------------------------------------------------
    def set_mode(self, mode):
        if mode not in MODES:
            return False
        with self.lock:
            self.mode = mode
        return True

    def snapshot_stats(self):
        with self.lock:
            snap = dict(self.stats)
            snap["mode"] = self.mode
            snap["inflight_quiet"] = (
                snap["commit_inflight"] == 0
                and snap["commit_started"] == snap["commit_finished"])
            return snap

    def boot_info(self):
        return {"component": "p26-dataset-fault-proxy",
                "boot_id": self.boot_id, "started_at": self.started_at,
                "upstream": self.upstream_base,
                "release_phase": RELEASE_PHASES["proxy"]}

    def close(self):
        with self.lock:
            barrier = self._barrier
            self._barrier = None
        if barrier is not None:
            barrier.release(by="core-close")


# --------------------------------------------------------------------------
# HTTP layer (binds ONLY when released)
# --------------------------------------------------------------------------

class _BodyTooLarge(Exception):
    pass


class Handler(BaseHTTPRequestHandler):
    core: P26ProxyCore = None
    transport = None                             # live urllib transport
    timeout = 60                                 # read deadline per request

    def log_message(self, fmt, *args):
        pass                                    # never log headers/auth

    def _send(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > MAX_BODY_BYTES:
            raise _BodyTooLarge()
        return self.rfile.read(length) if length > 0 else b""

    def _admin_ok(self):
        """FAIL CLOSED: proxy admin requires an exact token match."""
        if self.core.admin_token is None:
            return False
        return self.headers.get("X-P26-Admin-Token") == self.core.admin_token

    def _dispatch(self, method):
        core = self.core
        path = urllib.parse.urlparse(self.path).path
        if method == "POST" and COMMIT_PATH_RE.match(path):
            result = core.handle_commit(path, dict(self.headers),
                                        self._read_body(), self.transport)
        elif method == "GET" and RECEIPT_PATH_RE.match(path):
            result = core.handle_receipt_get(path, dict(self.headers),
                                             self.transport)
        else:
            result = core.handle_other(method, path, dict(self.headers),
                                       self._read_body(),
                                       self.transport)
        if result["action"] == "drop":
            try:
                self.connection.close()
            except OSError:
                pass
            return
        self._send(result["status"], result["body"] or b"")

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/healthz":
            return self._send(200, json.dumps(
                {"ok": True, "boot_id": self.core.boot_id}).encode())
        if u.path.startswith("/admin/") and not self._admin_ok():
            return self._send(403, b'{"error": "admin token required"}')
        if u.path == "/admin/boot":
            return self._send(200, json.dumps(
                self.core.boot_info()).encode())
        if u.path == "/admin/mode":
            return self._send(200, json.dumps(
                {"mode": self.core.mode}).encode())
        if u.path == "/admin/log":
            with self.core.lock:
                return self._send(200, json.dumps(
                    list(self.core.ledger)).encode())
        if u.path == "/admin/stats":
            return self._send(200, json.dumps(
                self.core.snapshot_stats()).encode())
        if u.path == "/admin/barrier":
            return self._send(200, json.dumps(
                {"barrier": self.core.barrier_state()}).encode())
        return self._dispatch("GET")

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        if u.path.startswith("/admin/"):
            if not self._admin_ok():
                return self._send(403,
                                  b'{"error": "admin token required"}')
            if u.path == "/admin/mode":
                try:
                    doc = json.loads(self._read_body() or b"{}")
                except ValueError:
                    return self._send(400, b'{"error": "bad json"}')
                except _BodyTooLarge:
                    return self._send(413, b'{"error": "body too large"}')
                ok = self.core.set_mode(doc.get("mode", ""))
                return self._send(200 if ok else 400, json.dumps(
                    {"ok": ok, "mode": self.core.mode}).encode())
            if u.path == "/admin/barrier/release":
                return self._send(
                    200 if self.core.release_barrier() else 404,
                    b'{"ok": true}')
            return self._send(404, b'{"error": "not found"}')
        try:
            return self._dispatch("POST")
        except _BodyTooLarge:
            return self._send(413, b'{"error": "commit body too large"}')


def main(argv):
    if len(argv) < 5 or argv[1] != "serve":
        print("usage: p26_dataset_fault_proxy.py serve <host> <port> "
              "<upstream-base> --admin-token-env NAME", file=sys.stderr)
        return 2
    if not __import__("p26_http_lib").release_allows(
            RELEASE_PHASES["proxy"])[0]:
        return refuse_serve("p26-proxy", RELEASE_PHASES["proxy"], argv)
    ok, why = validate_upstream(argv[4])
    if not ok:
        print(f"[p26-proxy] REFUSING upstream {argv[4]}: {why}",
              file=sys.stderr)
        return 3
    port = int(argv[3])
    if 17090 <= port <= 17094:
        print(f"[p26-proxy] REFUSING port {port}: old P25/q3 substrate",
              file=sys.stderr)
        return 3
    admin_env = None
    if "--admin-token-env" in argv:
        i = argv.index("--admin-token-env")
        admin_env = argv[i + 1] if i + 1 < len(argv) else None
    if not admin_env:
        print("[p26-proxy] REFUSING: --admin-token-env is REQUIRED "
              "(admin fails closed)", file=sys.stderr)
        return 3
    admin_token = __import__("os").environ.get(admin_env)
    if not admin_token:
        print(f"[p26-proxy] REFUSING: env {admin_env} empty/missing "
              "(admin fails closed)", file=sys.stderr)
        return 3

    from p26_http_lib import UrllibTransport
    core = P26ProxyCore(argv[4], admin_token=admin_token)
    Handler.core = core
    Handler.transport = UrllibTransport()   # no-redirect, auth never moved
    srv = ThreadingHTTPServer((argv[2], port), Handler)
    srv.daemon_threads = True
    print(f"[p26-proxy] boot_id={core.boot_id} serving "
          f"{argv[2]}:{port} -> {core.upstream_base}", flush=True)
    try:
        srv.serve_forever()
    finally:
        core.close()
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
