#!/usr/bin/env python3
"""P25 INCREMENTAL fixture — upstream REST source (DRAFT, not deployed).

Implements the ACCEPTED v1.1 protocol (implementation-contract.md §2.1/
2.2) with the fixture-proxy-round-1 review fixes:

  * CONSUMER POSITION IS NOT SERVER DELIVERY HISTORY (review item 1):
    a NULL (absent) since-token ALWAYS maps to the INITIAL window; the
    next window is chosen ONLY by the explicit checkpoint token in the
    request (T_i ⇒ window i+1). Delivery of a terminal response never
    advances any server-side consumer position — a publish failure
    leaves the consumer at NULL and a NULL retry re-serves window A.
    The terminal candidate token is derived from the REQUEST window
    index (stable; replay of an old token cannot move anyone's
    position). The page list of the chosen window is snapshotted per
    request so paging sees a stable window.
  * REAL COUNTER LIFECYCLE (item 2): every /data request does
    started++/active++ at entry and finished++/active-- in a finally
    on ALL exits (success, injected error, client disconnect). State
    mutations hold the lock only briefly; delays/barriers happen
    OUTSIDE the lock so admin and sibling srcs keep progressing while
    one request is paused. /admin/src therefore observes active>0
    during a blocked request and a true quiet 0 afterwards.
  * EMPTY semantics (item 6): the empty_batch knob is REMOVED. Empty
    results are the documented exhausted-stream case (token at the
    latest position ⇒ records [] and the SAME position token
    re-issued); a dedicated always-empty src `p25empty` covers the
    initial-source-empty shape (first terminal immediately, 0 rows,
    token tok-0) without any implicit consumer progress.

Protocol (unchanged): independent fixed data param `since_token`
(per-src configurable), terminal-page completed checkpoint at a
configurable dot path (default `meta.completed_token`), opaque ≤512B
UTF-8 no-control tokens, unknown/foreign tokens 400, bounded old-token
replay (an old token re-serves the window AFTER its position, at
least once, deterministically).

Fault knobs (POST /admin/knob/<name>): fail_mid_pages,
fail_terminal_after_rows, slow_terminal_ms, candidate_mode
(missing|non_string|empty|oversize|ctrl_chars|echo_request_token).

Admin: GET /healthz; GET /admin/src/<n> (old-fixture-shaped counters
+ per-request lifecycle); GET /admin/log/<n> (request ledger incl.
served window, request token, candidate, terminal flag).

Usage (when released): python3 -I rest_source_p25.py 0.0.0.0 8090
"""

import hashlib
import json
import re
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_SINCE_PARAM = "since_token"
DEFAULT_COMPLETED_PATH = "meta.completed_token"

BATCHES = {
    "A": [[f"a-{i:03d}|name-a{i}|qty-{i}" for i in range(1, 13)],
          [f"a-{i:03d}|name-a{i}|qty-{i}" for i in range(13, 25)]],
    "B": [[f"b-{i:03d}|name-b{i}|qty-{i}" for i in range(1, 9)],
          [f"b-{i:03d}|name-b{i}|qty-{i}" for i in range(9, 17)],
          [f"b-{i:03d}|name-b{i}|qty-{i}" for i in range(17, 21)]],
    "C": [[f"c-{i:03d}|name-c{i}|qty-{i}" for i in range(1, 31)]],
}
EMPTY_BATCH = [[[]]]      # ONE batch containing ONE empty page:
                         # batches = [batch], batch = [page], page = []


class SourceState:
    """One logical src. Positions are DERIVED from request tokens only;
    served_batches is delivery history for the LEDGER, never used to
    pick windows (review item 1)."""

    def __init__(self, name, batches, since_param=None,
                 completed_path=None):
        self.name = name
        self.batches = [list(p) for p in batches]   # immutable content
        self.since_param = since_param or DEFAULT_SINCE_PARAM
        self.completed_path = completed_path or DEFAULT_COMPLETED_PATH
        self.knobs = {"fail_mid_pages": False,
                      "fail_terminal_after_rows": False,
                      "slow_terminal_ms": 0,
                      "candidate_mode": None}
        self.counters = {"request_count": 0, "inventory_started": 0,
                         "inventory_finished": 0, "active": 0,
                         "total": sum(len(p) for p in self.batches),
                         "mode": "ok"}
        self.ledger = []

    def token_for(self, window):
        return f"{self.name}-tok-{window}"

    def window_for(self, token):
        """NULL ⇒ ALWAYS window 0 (the initial window), regardless of
        delivery history. T_i ⇒ window i+1. Unknown ⇒ None (400)."""
        if token is None:
            return 0
        if token == self.token_for(len(self.batches) - 1):
            return len(self.batches)          # exhausted position
        m = re.fullmatch(re.escape(self.name) + r"-tok-(\d+)", token)
        if not m:
            return None
        i = int(m.group(1))
        if i >= len(self.batches) - 1:
            return None                       # only the LAST token maps
        return i + 1

    def snapshot(self, request, window):
        self.ledger.append({**request, "window": window})
        if len(self.ledger) > 500:
            self.ledger = self.ledger[-500:]


def _valid_token(tok):
    b = tok.encode("utf-8", "strict")
    if not b or len(b) > 512:
        return False
    return not any(c < 0x20 or c == 0x7F for c in b)


class IncrementalFixture:
    """Pure core + per-request lifecycle. data() must be called via
    begin()/finish() pairs (the HTTP layer does; tests may too)."""

    def __init__(self):
        self.sources = {
            "p25h1": SourceState("p25h1",
                                 [BATCHES["A"], BATCHES["B"],
                                  BATCHES["C"]]),
            "p25h2": SourceState("p25h2", [[BATCHES["A"][0],
                                            BATCHES["A"][1]],
                                           [BATCHES["B"][2]]]),
            "p25h3": SourceState("p25h3", [BATCHES["A"]]),
            # p25h4: A then a deterministic B window (root request:
            # second NON-EMPTY window so a seeded h4 run can exercise
            # candidate faults before exhaustion; nil still maps to A)
            "p25h4": SourceState("p25h4",
                                 [BATCHES["A"], [BATCHES["B"][0]]]),
            "p25h6": SourceState("p25h6", [[BATCHES["A"][0]],
                                           [BATCHES["B"][0]],
                                           [BATCHES["C"][0]]]),
            "p25h13": SourceState("p25h13", [[BATCHES["A"][0]],
                                             [BATCHES["B"][0]]]),
            "p25empty": SourceState("p25empty", EMPTY_BATCH),
        }
        self.lock = threading.Lock()

    # ---- request lifecycle (review item 2) ---------------------------
    def begin(self, src):
        """Entry bookkeeping. Returns finish() callable; MUST be called
        on every exit path (success/error/disconnect)."""
        with self.lock:
            st = self.sources.get(src)
            if st is None:
                return None
            st.counters["request_count"] += 1
            st.counters["inventory_started"] += 1
            st.counters["active"] += 1
        done = []

        def finish():
            if done:
                return
            done.append(1)
            with self.lock:
                st2 = self.sources.get(src)
                if st2 is not None:
                    st2.counters["inventory_finished"] += 1
                    st2.counters["active"] = max(
                        0, st2.counters["active"] - 1)
        return finish

    def data(self, src, params):
        """Compute one /data response. Delays happen OUTSIDE the lock;
        the caller's finish hook runs even when this returns an error
        status (the HTTP layer keeps it in its own finally for socket
        disconnects)."""
        st = self.sources.get(src)
        if st is None:
            return 404, {"error": "unknown src"}
        since = params.get(st.since_param)
        cursor = params.get("cursor", "")
        try:
            page_size = int(params.get("page_size", "50"))
        except ValueError:
            return 400, {"error": "bad page_size"}
        if since is not None and not _valid_token(since):
            return 400, {"error": "invalid since token bytes"}
        window = st.window_for(since)
        with self.lock:                       # short state mutation only
            st.snapshot({"since_param": st.since_param, "token": since,
                         "cursor": cursor, "page_size": page_size,
                         "ts": round(time.time(), 6)}, window
                        if window is not None else -1)
        if window is None:
            return 400, {"error": "unknown since token"}
        if window >= len(st.batches):
            # EXHAUSTED position: zero rows; re-issue the REQUEST token's
            # position (candidate derives from the request, not mutable
            # state). No implicit consumer progress.
            body = {"records": [], "next_cursor": ""}
            _set_path(body, st.completed_path, since)
            return 200, body
        pages = [list(p) for p in st.batches[window]]   # stable snapshot
        keys = [_page_key(p) for p in pages]
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
            # TERMINATING page. Any configured delay happens here — the
            # fixture lock is NOT held (admin + sibling srcs progress).
            if st.knobs.get("fail_terminal_after_rows"):
                return 500, {"error":
                             "injected terminal failure after rows"}
            if st.knobs.get("slow_terminal_ms"):
                time.sleep(min(int(st.knobs["slow_terminal_ms"]) / 1000.0,
                               30))
            _apply_candidate(st, body, window,
                             st.knobs.get("candidate_mode"), since)
        return 200, body

    # ---- admin ------------------------------------------------------
    def snapshot(self, src):
        with self.lock:
            st = self.sources.get(src)
            if st is None:
                return None
            snap = dict(st.counters)
            snap["name"] = src      # body self-identifies its src (#4)
            return snap

    def ledger(self, src):
        with self.lock:
            st = self.sources.get(src)
            return list(st.ledger) if st else None

    def set_knob(self, src, knob):
        with self.lock:
            st = self.sources.get(src)
            if st is None or not isinstance(knob, dict):
                return False
            for k, v in knob.items():
                if k not in st.knobs:
                    return False
                st.knobs[k] = v
            return True

    def quiet(self):
        with self.lock:
            return all(s.counters["active"] == 0
                       and s.counters["inventory_started"]
                       == s.counters["inventory_finished"]
                       for s in self.sources.values())


def _page_key(page):
    h = hashlib.sha256("\n".join(page).encode()).hexdigest()[:12]
    return f"pg-{h}"


def _apply_candidate(st, body, window, mode, request_token):
    """Terminal completed-checkpoint derived from the REQUEST window
    (review item 1): stable under replay, never from mutable state."""
    good = st.token_for(window)
    if mode == "missing":
        return
    if mode == "non_string":
        _set_path(body, st.completed_path, {"not": "a string"})
        return
    if mode == "empty":
        _set_path(body, st.completed_path, "")
        return
    if mode == "oversize":
        _set_path(body, st.completed_path, "x" * 513)
        return
    if mode == "ctrl_chars":
        _set_path(body, st.completed_path, "bad\x01token")
        return
    if mode == "echo_request_token" and request_token is not None:
        # rows>0 with candidate == the REQUEST token ⇒ the connector
        # must reject with ErrIncrementalNoProgress
        _set_path(body, st.completed_path, request_token)
        return
    _set_path(body, st.completed_path, good)


def _set_path(body, path, value):
    cur = body
    segs = path.split(".")
    for s in segs[:-1]:
        cur = cur.setdefault(s, {})
    cur[segs[-1]] = value


class Handler(BaseHTTPRequestHandler):
    app: IncrementalFixture = None

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, payload, raw=None):
        body = raw if raw is not None else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type",
                         "application/json" if raw is None
                         else "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query, keep_blank_values=True)
        flat = {k: v[0] for k, v in q.items()}
        if u.path == "/healthz":
            return self._send(200, {"ok": True})
        m = re.match(r"^/admin/src/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            snap = self.app.snapshot(m.group(1))
            return self._send(200, snap) if snap else self._send(404, {
                "error": "unknown src"})
        m = re.match(r"^/admin/log/([A-Za-z0-9_.-]+)$", u.path)
        if m:
            led = self.app.ledger(m.group(1))
            return self._send(200, led) if led is not None else \
                self._send(404, {"error": "unknown src"})
        if u.path == "/data":
            src = flat.pop("src", None)
            if not src:
                return self._send(400, {"error": "src required"})
            finish = self.app.begin(src)
            if finish is None:
                return self._send(404, {"error": "unknown src"})
            try:
                status, body = self.app.data(src, flat)
                return self._send(status, body)
            except (BrokenPipeError, ConnectionResetError):
                raise                    # finally below still runs
            finally:
                finish()                 # disconnect-safe lifecycle
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        m = re.match(r"^/admin/knob/([A-Za-z0-9_.-]+)$", u.path)
        if not m:
            return self._send(404, {"error": "not found"})
        try:
            knob = json.loads(self.rfile.read(
                int(self.headers.get("Content-Length", "0"))) or b"{}")
        except ValueError:
            return self._send(400, {"error": "bad json"})
        ok = self.app.set_knob(m.group(1), knob)
        return self._send(200 if ok else 404, {"ok": ok})


def main():
    host, port = sys.argv[1], int(sys.argv[2])
    app = IncrementalFixture()
    Handler.app = app
    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"[p25-fixture] serving {host}:{port}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
