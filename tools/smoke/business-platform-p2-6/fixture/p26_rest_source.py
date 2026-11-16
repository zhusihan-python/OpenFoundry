#!/usr/bin/env python3
"""P26 W7 synthetic REST source fixture (NEW, owned by W7).

Serves the connector's configurable REST sync protocol (records_path /
cursor pagination / since_token incremental), same protocol family the
P25 fixture spoke — but with NEW owned src names, NEW deterministic
data, and a NEW boot identity. It does NOT reuse or restart old
fixture17090 or any removed p25 aux (17092-94).

Pure core (P26FixtureCore) is fully testable without sockets; the HTTP
main() refuses to bind unless the P26 release file (Root-issued) lists
the `http-fixture-serve` phase.

Fixture invariants (validated by selftests):
  * deterministic multi-batch data — every boot serves identical bytes;
  * opaque non-sequential page cursors (content-derived `pg-<sha12>`);
  * consumer position derives ONLY from the request token:
      absent/NULL since_token => ALWAYS window 0;
      T_i => window i+1; the LATEST token => exhausted (empty batch,
      same token re-issued); unknown/foreign token => 400;
  * terminal pages carry meta.completed_token derived from the request
    window (stable under replay; old-token replay moves no position);
  * per-src real request lifecycle counters (started/finished/active)
    and a request ledger that NEVER records Authorization or any header
    values — only method/path/token/cursor/window/rows/timing;
  * bounded barriers: `hold_next_ms` (<=30s) blocks the NEXT /data for
    a src outside any lock; release via POST /admin/release/<src> or
    auto-release at the deadline; core.close() releases everything;
  * /admin/expected/<src>?window=N returns the deterministic expected
    records + NDJSON sha256/rows/bytes so the harness can assert
    downloaded bytes/hash/rows against the fixture without duplicating
    data logic.

Usage (only when released): python3 -I p26_rest_source.py serve 0.0.0.0 8090
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

from p26_http_lib import (RELEASE_PHASES, BoundedEvent, opaque_digest,
                          refuse_serve, sha256_hex, utcnow_iso)

SINCE_PARAM = "since_token"
COMPLETED_PATH = "meta.completed_token"
LEDGER_CAP = 1000
MAX_BODY_BYTES = 1 << 20            # 1 MiB request-body cap (413 beyond)


def token_digest(tok):
    """Observability-safe digest for opaque positional values — the SAME
    format as p26_http_lib.opaque_digest so scenario expectations and
    fixture admin surfaces agree (raw tokens/cursors NEVER leave the
    core through admin/ledger surfaces)."""
    return opaque_digest(tok)


def records_ndjson(records):
    """EXACT bytes the real connector materialises: each REST record is
    copied VERBATIM as raw JSON (a JSON string record keeps its quotes)
    followed by '\n' (rest_snapshot.go:239-247). Fixture records are
    ASCII-only (asserted in selftest) so json.dumps matches the wire
    serialization byte-for-byte."""
    out = []
    for rec in records:
        out.append(json.dumps(rec))
        out.append("\n")
    return "".join(out).encode("utf-8")

# --- deterministic NEW owned data (distinct from every P25 batch) -----------

def _rec(prefix, i):
    return f"{prefix}-{i:04d}|p26-{prefix}|qty-{i * 7 % 97}"


_PAGES_SNAP1 = [
    [_rec("s1a", i) for i in range(1, 13)],
    [_rec("s1a", i) for i in range(13, 25)],
    [_rec("s1a", i) for i in range(25, 33)],
]
_PAGES_INC1_A = [
    [_rec("i1a", i) for i in range(1, 9)],
    [_rec("i1a", i) for i in range(9, 17)],
]
_PAGES_INC1_B = [
    [_rec("i1b", i) for i in range(1, 7)],
]
_PAGES_LEGACY = [
    [_rec("lg1", i) for i in range(1, 6)],
]

SRC_LAYOUT = {
    # snapshot healthy: one window, 3 pages, 32 rows
    "p26snap1": [_PAGES_SNAP1],
    # incremental healthy: window A (8+8) then window B (6)
    "p26inc1": [_PAGES_INC1_A, _PAGES_INC1_B],
    # legacy opt-out control: one page, 5 rows, old path (no recovery)
    "p26legacy": [_PAGES_LEGACY],
}


def page_key(page):
    return "pg-" + sha256_hex("\n".join(page).encode())[:12]


class SourceState:
    """Per-src immutable data + mutable counters/knobs/holds."""

    def __init__(self, name, windows):
        self.name = name
        self.windows = [list(w) for w in windows]   # immutable content
        self.knobs = {"fail_mid_pages": False,
                      "fail_terminal_after_rows": False,
                      "slow_terminal_ms": 0,
                      "hold_next_ms": 0}
        self.counters = {"request_count": 0, "started": 0,
                         "finished": 0, "active": 0,
                         "total_rows": sum(len(p) for w in self.windows
                                           for p in w),
                         "windows": len(self.windows)}
        self.tokens_seen = []                       # since_token ledger
        self.ledger = []
        self._pending_hold = None                   # armed, not yet hit
        self._active_hold = None                    # being waited on now

    # ---- position semantics (request-derived only) --------------------
    def token_for(self, window):
        return f"{self.name}-tok-{window}"

    def window_for(self, token):
        if token is None:
            return 0                                # NULL => ALWAYS A
        if token == self.token_for(len(self.windows) - 1):
            return len(self.windows)                # exhausted position
        m = re.fullmatch(re.escape(self.name) + r"-tok-(\d+)", token)
        if not m:
            return None
        i = int(m.group(1))
        if i >= len(self.windows) - 1:
            return None                             # only LAST maps forward
        return i + 1

    def expected(self, window):
        if window is None or window < 0 \
                or window >= len(self.windows):
            return None
        pages = self.windows[window]
        records = [r for p in pages for r in p]
        ndjson = records_ndjson(records)
        return {"src": self.name, "window": window,
                "records": records,
                "rows": len(records), "bytes": len(ndjson),
                "ndjson_sha256": sha256_hex(ndjson),
                "pages": len(pages),
                "terminal_token": self.token_for(window),
                "terminal_token_digest": token_digest(
                    self.token_for(window))}


class P26FixtureCore:
    """Pure core: no sockets. HTTP layer and selftests both call this."""

    def __init__(self, boot_id=None, clock=time.monotonic):
        self.boot_id = boot_id or str(uuid.uuid4())
        self.started_at = utcnow_iso()
        self.sources = {n: SourceState(n, w) for n, w in SRC_LAYOUT.items()}
        self.lock = threading.Lock()
        self.admin_token = None                     # optional restriction
        self._clock = clock
        self._closed = False

    # ---- request lifecycle ---------------------------------------------
    def begin(self, src):
        with self.lock:
            st = self.sources.get(src)
            if st is None:
                return None
            st.counters["request_count"] += 1
            st.counters["started"] += 1
            st.counters["active"] += 1
        done = []

        def finish():
            if done:
                return
            done.append(1)
            with self.lock:
                st2 = self.sources.get(src)
                if st2 is not None:
                    st2.counters["finished"] += 1
                    st2.counters["active"] = max(
                        0, st2.counters["active"] - 1)
        return finish

    # ---- /data -----------------------------------------------------------
    def data(self, src, params):
        """Returns (status, body). Barriers sleep OUTSIDE the lock; the
        caller's finish() hook runs on EVERY exit path."""
        st = self.sources.get(src)
        if st is None:
            return 404, {"error": "unknown src"}
        since = params.get(SINCE_PARAM)
        cursor = params.get("cursor", "")
        try:
            page_size = int(params.get("page_size", "50"))
        except ValueError:
            return 400, {"error": "bad page_size"}
        if page_size <= 0:
            return 400, {"error": "bad page_size"}
        if since is not None and (not since or len(since.encode()) > 512
                                  or any(c < " " for c in since)):
            return 400, {"error": "invalid since token"}

        hold = None
        with self.lock:
            window = st.window_for(since)
            st.tokens_seen.append(None if since is None
                                  else token_digest(since))
            st.ledger.append({
                "ts": utcnow_iso(),
                "token": None if since is None else token_digest(since),
                "cursor": token_digest(cursor) if cursor else None,
                "page_size": page_size,
                "window": window if window is not None else -1})
            if len(st.ledger) > LEDGER_CAP:
                del st.ledger[:len(st.ledger) - LEDGER_CAP]
            if st._pending_hold is not None:
                hold = st._pending_hold
                st._pending_hold = None
                st._active_hold = hold
        if window is None:
            return 400, {"error": "unknown since token"}
        if hold is not None:
            released, waited_ms = hold.wait()        # bounded, lock-free
            with self.lock:
                if st._active_hold is hold:
                    st._active_hold = None
            self._note_hold(src, waited_ms, released)

        if window >= len(st.windows):
            # exhausted position: empty batch, SAME token re-issued
            body = {"records": [], "next_cursor": ""}
            body_set(body, COMPLETED_PATH, since)
            return 200, body

        pages = st.windows[window]
        keys = [page_key(p) for p in pages]
        if cursor:
            if cursor not in keys:
                return 400, {"error": "unknown cursor"}
            idx = keys.index(cursor)
        else:
            idx = 0
        if st.knobs.get("fail_mid_pages") and idx == 1 and cursor:
            return 500, {"error": "injected mid-page failure"}
        rows = pages[idx][:page_size]
        next_cursor = keys[idx + 1] if idx + 1 < len(pages) else ""
        body = {"records": rows, "next_cursor": next_cursor}
        if next_cursor == "":
            # terminating page
            if st.knobs.get("fail_terminal_after_rows"):
                return 500, {"error":
                             "injected terminal failure after rows"}
            if st.knobs.get("slow_terminal_ms"):
                time.sleep(min(int(st.knobs["slow_terminal_ms"]) / 1000.0,
                               30.0))
            body_set(body, COMPLETED_PATH, st.token_for(window))
        return 200, body

    def _note_hold(self, src, waited_ms, released):
        with self.lock:
            st = self.sources.get(src)
            if st is not None:
                st.ledger.append({"ts": utcnow_iso(), "hold_waited_ms":
                                  waited_ms,
                                  "hold_released_early": released})

    # ---- admin ------------------------------------------------------------
    def snapshot(self, src):
        with self.lock:
            st = self.sources.get(src)
            if st is None:
                return None
            snap = dict(st.counters)
            snap["name"] = src
            snap["tokens_seen"] = list(st.tokens_seen[-64:])
            snap["knobs"] = dict(st.knobs)
            snap["pending_hold"] = (st._pending_hold.state()
                                    if st._pending_hold else None)
            snap["active_hold"] = (st._active_hold.state()
                                   if st._active_hold else None)
            return snap

    def ledger(self, src):
        with self.lock:
            st = self.sources.get(src)
            return list(st.ledger) if st else None

    def expected(self, src, window):
        st = self.sources.get(src)
        if st is None:
            return None
        try:
            w = int(window)
        except (TypeError, ValueError):
            return None
        return st.expected(w)

    def set_knob(self, src, knob):
        with self.lock:
            st = self.sources.get(src)
            if st is None or not isinstance(knob, dict):
                return False, "unknown src"
            for k, v in knob.items():
                if k not in st.knobs:
                    return False, f"unknown knob {k}"
            for k, v in knob.items():
                if k == "hold_next_ms":
                    if not isinstance(v, int) or v < 0:
                        return False, "hold_next_ms must be int>=0"
                    st._pending_hold = BoundedEvent(min(v, 30_000))
                else:
                    st.knobs[k] = v
            return True, "ok"

    def release_hold(self, src):
        """Release the hold a request is CURRENTLY waiting on (or the
        armed one if it has not been hit yet)."""
        with self.lock:
            st = self.sources.get(src)
            if st is None:
                return False
            if st._active_hold is not None:
                st._active_hold.release(by="admin")
                return True
            if st._pending_hold is not None:
                st._pending_hold.release(by="admin")
                return True
        return False

    def quiet(self):
        with self.lock:
            return all(s.counters["active"] == 0
                       and s.counters["started"] == s.counters["finished"]
                       and s._pending_hold is None
                       for s in self.sources.values())

    def boot_info(self):
        return {"component": "p26-rest-source-fixture",
                "boot_id": self.boot_id, "started_at": self.started_at,
                "srcs": sorted(self.sources),
                "release_phase": RELEASE_PHASES["fixture"]}

    def close(self):
        """Release every pending/active hold so no thread can stay wedged."""
        with self.lock:
            self._closed = True
            holds = [(h, n) for n, s in self.sources.items()
                     for h in (s._pending_hold, s._active_hold)
                     if h is not None]
        for hold, _name in holds:
            hold.release(by="core-close")


class _BodyTooLarge(Exception):
    pass


def body_set(body, path, value):
    cur = body
    segs = path.split(".")
    for s in segs[:-1]:
        cur = cur.setdefault(s, {})
    cur[segs[-1]] = value


# --------------------------------------------------------------------------
# HTTP layer (binds ONLY when released)
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    core: P26FixtureCore = None
    timeout = 60                                  # per-connection read deadline

    def log_message(self, fmt, *args):
        pass                                    # never log headers/auth

    def _send(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
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
        """FAIL CLOSED: admin requires an exact token match. A core
        without a configured token (pure tests default) denies admin
        outright; live boots MUST pass one via env."""
        if self.core.admin_token is None:
            return False
        return self.headers.get("X-P26-Admin-Token") == self.core.admin_token

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = {k: v[0] for k, v in
             urllib.parse.parse_qs(u.query, keep_blank_values=True).items()}
        if u.path == "/healthz":
            return self._send(200, {"ok": True,
                                    "boot_id": self.core.boot_id})
        if u.path.startswith("/admin/") and not self._admin_ok():
            return self._send(403, {"error": "admin token required"})
        if u.path == "/admin/boot":
            return self._send(200, self.core.boot_info())
        if u.path == "/admin/quiet":
            return self._send(200, {"quiet": self.core.quiet()})
        m = re.match(r"^/admin/src/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            snap = self.core.snapshot(m.group(1))
            return self._send(200, snap) if snap else \
                self._send(404, {"error": "unknown src"})
        m = re.match(r"^/admin/log/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            led = self.core.ledger(m.group(1))
            return self._send(200, led) if led is not None else \
                self._send(404, {"error": "unknown src"})
        m = re.match(r"^/admin/expected/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            exp = self.core.expected(m.group(1), q.get("window"))
            return self._send(200, exp) if exp else \
                self._send(404, {"error": "unknown src/window"})
        if u.path == "/data":
            src = q.pop("src", None)
            if not src:
                return self._send(400, {"error": "src required"})
            finish = self.core.begin(src)
            if finish is None:
                return self._send(404, {"error": "unknown src"})
            try:
                status, body = self.core.data(src, q)
                return self._send(status, body)
            finally:
                finish()                        # disconnect-safe
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        if u.path.startswith("/admin/") and not self._admin_ok():
            return self._send(403, {"error": "admin token required"})
        m = re.match(r"^/admin/knob/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            try:
                knob = json.loads(self._read_body() or b"{}")
            except ValueError:
                return self._send(400, {"error": "bad json"})
            except _BodyTooLarge:
                return self._send(413, {"error": "body too large"})
            ok, why = self.core.set_knob(m.group(1), knob)
            return self._send(200 if ok else 400,
                              {"ok": ok, "detail": why})
        m = re.match(r"^/admin/release/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            return self._send(200 if self.core.release_hold(m.group(1))
                              else 404, {"src": m.group(1)})
        return self._send(404, {"error": "not found"})


def main(argv):
    if len(argv) < 4 or argv[1] != "serve":
        print("usage: p26_rest_source.py serve <host> <port> "
              "--admin-token-env NAME", file=sys.stderr)
        return 2
    if not __import__("p26_http_lib").release_allows(
            RELEASE_PHASES["fixture"])[0]:
        return refuse_serve("p26-fixture", RELEASE_PHASES["fixture"], argv)
    host, port = argv[2], int(argv[3])
    if 17090 <= port <= 17094:
        # old q3 fixture(17090) + removed P25 aux(17092-94): never ours
        print(f"[p26-fixture] REFUSING port {port}: old P25/q3 substrate",
              file=sys.stderr)
        return 3
    admin_env = None
    if "--admin-token-env" in argv:
        i = argv.index("--admin-token-env")
        admin_env = argv[i + 1] if i + 1 < len(argv) else None
    if not admin_env:
        print("[p26-fixture] REFUSING: --admin-token-env is REQUIRED "
              "(admin fails closed)", file=sys.stderr)
        return 3
    admin_token = __import__("os").environ.get(admin_env)
    if not admin_token:
        print(f"[p26-fixture] REFUSING: env {admin_env} empty/missing "
              "(admin fails closed)", file=sys.stderr)
        return 3
    core = P26FixtureCore()
    core.admin_token = admin_token
    Handler.core = core
    srv = ThreadingHTTPServer((host, port), Handler)
    srv.daemon_threads = True
    print(f"[p26-fixture] boot_id={core.boot_id} serving {host}:{port}",
          flush=True)
    try:
        srv.serve_forever()
    finally:
        core.close()
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
