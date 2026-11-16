#!/usr/bin/env python3
"""Synthetic paginated inventory REST source for OpenFoundry P1 acceptance.

Design constraints (from docs/architecture/business-platform-implementation-plan.md P1.3):

* 151 deterministic inventory records served across at least 3 pages
  (default page_size 50 -> 4 pages).
* SKU strings carry leading zeros ("000001".."000151") so any layer that
  coerces them to numbers is caught by the value-by-value download check.
* Pagination style is negotiable at request time (page number, offset,
  or opaque cursor; wrapped envelope, "data" wrapper, or bare array) so
  the final adapter contract chosen by the Source/Sync implementation
  can be exercised without changing this fixture.
* Behaviour is keyed by `src=<name>` so several Sources in the platform
  can point at independent datasets/failure modes of the same fixture.
  An admin endpoint mutates a key at runtime (content flip, failure
  modes) WITHOUT touching the Source configuration — that is how the
  "same config, changed source content -> new hash" acceptance works.
* Everything is deterministic: no randomness, fixed timestamps.

Run (trusted code, stdlib only):

    python3 -I rest_source.py [bind_host] [port]

Endpoints:

    GET  /healthz
    GET  /v1/inventory?src=<name>&page=N&page_size=50[&wrap=items|data|array]
    GET  /v1/inventory?src=<name>&offset=N&limit=50[&wrap=...]
    GET  /v1/inventory?src=<name>&cursor=<token>[&wrap=...]
    POST /admin/src/<name>/set        {"mode": ..., "content": 1|2,
                                       "page_size": n, "total": n,
                                       "require_api_key": str|null,
                                       "barrier_pages": [n,...]|"all",
                                       "barrier_wait_s": 1..600,
                                       "barrier_release": [n,...]|"all"}
    GET  /admin/src/<name>            (state dump, secrets excluded)

P2.2 extension (root-authorized 2026-11-14, minimal increment to this one
shared file): a per-src/page RELEASABLE barrier plus started/finished/
active safe counts, for interruptible-worker acceptance. Defaults keep
every P1/P2.1 scenario byte-identical: the barrier is OFF unless
barrier_pages is set, parked requests coordinate on a per-src condition
(no global lock is held while waiting), every wait is bounded
(barrier_wait_s, observable 504 on expiry), and install resets release
marks. The admin surface only ever carries plain config values — request
headers, platform JWTs, and other sensitive material are never stored,
logged, or echoed.
"""

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

DEFAULT_TOTAL = 151
DEFAULT_PAGE_SIZE = 50
CONTENT_VERSIONS = (1, 2)

# Failure/edge modes a source key can be switched to.
#   ok          — normal multi-page serving
#   fail_page2  — HTTP 500 from page 2 onwards (upstream failure)
#   invalid_json— page 2 serves 200 + broken JSON body (parse failure)
#   loop        — page 3 advertises page 1 as its successor (cycle)
#   many_pages  — page_size forced to 1: 151 pages (page-limit overrun)
MODES = ("ok", "fail_page2", "invalid_json", "loop", "many_pages")


def record(index, content):
    """Deterministic inventory record; index is 1-based."""
    if content == 1:
        return {
            "sku": f"{index:06d}",  # leading zeros must survive the pipeline
            "qty": index,
            "name": f"Widget {index:03d}",
            "unit_price_cents": 900 + index,
            "category": ("A", "B", "C")[index % 3],
            "is_active": index % 7 != 0,
            "updated_at": "2026-11-13T00:00:00Z",
        }
    if content == 2:
        return {
            "sku": f"{index:06d}",
            "qty": index + 1000,
            "name": f"Widget X {index:03d}",
            "unit_price_cents": 1900 + index,
            "category": ("A", "B", "C")[index % 3],
            "is_active": index % 7 == 0,
            "updated_at": "2026-11-13T00:00:00Z",
        }
    raise ValueError(f"unknown content version {content!r}")


def records(total, content):
    return [record(i, content) for i in range(1, total + 1)]


DEFAULT_BARRIER_WAIT_S = 120


class _BarrierGate:
    """ONE installation generation of the P2.2 releasable barrier.

    Requests capture the gate OBJECT under state.lock at entry, so a
    later reinstall (a NEW gate) can never change the rules for an
    already-arrived request — neither its page set, its release marks,
    nor its wait bound. This closes the snapshot/wait gap by
    construction: release-then-reinstall between a request's snapshot
    and its park still leaves the request on ITS generation with ITS
    marks. Releases address the CURRENT gate only; retired gates stay
    releasable-by-nobody but bounded by their own wait_s."""

    __slots__ = ("pages", "all_pages", "wait_s", "released",
                 "all_released", "waiters", "cond")

    def __init__(self, pages, all_pages, wait_s):
        self.pages = pages          # frozenset[int]
        self.all_pages = all_pages  # bool ("all" mode)
        self.wait_s = wait_s
        self.released = set()       # int pages released on THIS gate
        self.all_released = False
        self.waiters = 0
        self.cond = threading.Condition()

    def matches(self, page):
        return self.all_pages or page in self.pages


class SourceState:
    """Per-`src`-key mutable state (defaults reproduce the plain flow)."""

    def __init__(self, name):
        self.name = name
        self.mode = "ok"
        self.content = 1
        self.total = DEFAULT_TOTAL
        self.page_size = DEFAULT_PAGE_SIZE
        self.require_api_key = None  # None -> auth not enforced
        self.request_count = 0
        # Bounded per-request delay for P2 heartbeat/lease-loss
        # observation: applied only when delay_ms > 0 and the requested
        # page >= delay_from_page. Defaults keep every existing scenario
        # latency-free; record content and pagination are unchanged.
        self.delay_ms = 0
        self.delay_from_page = 2
        # P2.2 releasable barrier (root-authorized 2026-11-14; default OFF
        # -> zero behavior change for every P1/P2.1 scenario). Installing
        # barrier_pages creates a NEW generation gate; parked requests
        # coordinate on their OWN gate's condition (never the global
        # sources lock, never the state lock) and are bounded by the
        # gate's wait_s. barrier_waiters counts ALL gates including
        # retired ones (guarded by a dedicated small lock).
        self.barrier = None  # current _BarrierGate or None (OFF)
        self.barrier_default_wait_s = DEFAULT_BARRIER_WAIT_S
        self.barrier_waiters = 0
        # P2.2 safe counters (per src): inventory requests entered vs fully
        # served; active = started - finished includes barrier-parked ones.
        self.inventory_started = 0
        self.inventory_finished = 0
        self.lock = threading.Lock()
        self.waiters_lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            gate = self.barrier
            snap = {
                "name": self.name,
                "mode": self.mode,
                "content": self.content,
                "total": self.total,
                "page_size": self.page_size,
                "require_api_key": self.require_api_key,
                "request_count": self.request_count,
                "delay_ms": self.delay_ms,
                "delay_from_page": self.delay_from_page,
                "inventory_started": self.inventory_started,
                "inventory_finished": self.inventory_finished,
                "active": self.inventory_started - self.inventory_finished,
                "barrier_all": bool(gate and gate.all_pages),
                "barrier_pages": sorted(gate.pages) if gate else [],
                "barrier_wait_s": gate.wait_s if gate else DEFAULT_BARRIER_WAIT_S,
                "barrier_released": sorted(gate.released) if gate else [],
                "barrier_all_released": bool(gate and gate.all_released),
            }
        with self.waiters_lock:
            snap["barrier_waiters"] = self.barrier_waiters
        return snap


class FixtureHandler(BaseHTTPRequestHandler):
    server_version = "OpenFoundryP1Fixture/1.0"
    protocol_version = "HTTP/1.1"
    sources = {}  # name -> SourceState, populated via get_state()
    sources_lock = threading.Lock()

    # ---------------------------------------------------------------- helpers
    @classmethod
    def get_state(cls, name):
        with cls.sources_lock:
            state = cls.sources.get(name)
            if state is None:
                state = SourceState(name)
                cls.sources[name] = state
            return state

    def log_message(self, fmt, *args):  # keep fixture logs one-line, no bodies
        print(f"[fixture] {self.address_string()} {fmt % args}", flush=True)

    def send_json(self, status, payload, extra_headers=None):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------ routes
    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/healthz":
            self.send_json(200, {"status": "ok"})
            return
        if url.path == "/v1/inventory":
            self.serve_inventory(url)
            return
        if url.path.startswith("/admin/src/"):
            parts = url.path.rstrip("/").split("/")
            if len(parts) == 4 and parts[3]:
                self.send_json(200, self.get_state(parts[3]).snapshot())
                return
        self.send_json(404, {"error": "not found"})

    def do_POST(self):
        url = urlsplit(self.path)
        parts = url.path.rstrip("/").split("/")
        # /admin/src/<name>/set
        if len(parts) == 5 and parts[1] == "admin" and parts[2] == "src" and parts[3] and parts[4] == "set":
            length = int(self.headers.get("Content-Length") or 0)
            try:
                patch = json.loads(self.rfile.read(length) or b"{}")
            except ValueError:
                self.send_json(400, {"error": "invalid json"})
                return
            self.apply_admin(parts[3], patch)
            return
        self.send_json(404, {"error": "not found"})

    def apply_admin(self, name, patch):
        state = self.get_state(name)
        errors = []
        with state.lock:
            if "mode" in patch:
                if patch["mode"] not in MODES:
                    errors.append(f"mode must be one of {MODES}")
                else:
                    state.mode = patch["mode"]
            if "content" in patch:
                if patch["content"] not in CONTENT_VERSIONS:
                    errors.append("content must be 1 or 2")
                else:
                    state.content = patch["content"]
            if "total" in patch:
                # 0 is legal: an empty source whose first page is an empty
                # array (page-mode adapters terminate immediately).
                if not isinstance(patch["total"], int) or patch["total"] < 0:
                    errors.append("total must be a non-negative int")
                else:
                    state.total = patch["total"]
            if "page_size" in patch:
                if not isinstance(patch["page_size"], int) or patch["page_size"] < 1:
                    errors.append("page_size must be a positive int")
                else:
                    state.page_size = patch["page_size"]
            if "delay_ms" in patch:
                value = patch["delay_ms"]
                if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 60000:
                    errors.append("delay_ms must be an int in [0, 60000]")
                else:
                    state.delay_ms = value
            if "delay_from_page" in patch:
                value = patch["delay_from_page"]
                if not isinstance(value, int) or isinstance(value, bool) or value < 1 or value > 10000:
                    errors.append("delay_from_page must be an int in [1, 10000]")
                else:
                    state.delay_from_page = value
            if "require_api_key" in patch:
                value = patch["require_api_key"]
                if value is not None and not isinstance(value, str):
                    errors.append("require_api_key must be a string or null")
                else:
                    state.require_api_key = value
            # --- P2.2 barrier group (root-authorized 2026-11-14; validated
            # ATOMICALLY: any error leaves ALL barrier state untouched).
            # Only ints / "all" are accepted — the admin surface never
            # carries, stores, or echoes request headers, platform JWTs,
            # or any other sensitive material.
            barrier_errors = []
            new_pages = new_wait = release = None
            if "barrier_pages" in patch:
                value = patch["barrier_pages"]
                if value == "all":
                    new_pages = "all"
                elif isinstance(value, list) and all(
                        isinstance(p, int) and not isinstance(p, bool)
                        and 1 <= p <= 10000 for p in value):
                    new_pages = sorted(set(value))
                else:
                    barrier_errors.append(
                        'barrier_pages must be "all" or a list of ints in [1, 10000]')
            if "barrier_wait_s" in patch:
                value = patch["barrier_wait_s"]
                if not isinstance(value, int) or isinstance(value, bool) \
                        or value < 1 or value > 600:
                    barrier_errors.append("barrier_wait_s must be an int in [1, 600]")
                else:
                    new_wait = value
            if "barrier_release" in patch:
                value = patch["barrier_release"]
                if value == "all":
                    release = "all"
                elif isinstance(value, list) and all(
                        isinstance(p, int) and not isinstance(p, bool)
                        and 1 <= p <= 10000 for p in value):
                    release = sorted(set(value))
                else:
                    barrier_errors.append(
                        'barrier_release must be "all" or a list of ints in [1, 10000]')
            errors.extend(barrier_errors)
            if not barrier_errors:
                wait_s_for_install = (new_wait if new_wait is not None
                                      else state.barrier_default_wait_s)
                if new_pages is not None:
                    # installing pages installs a NEW GENERATION gate: the
                    # release marks start empty BY CONSTRUCTION (fresh
                    # object), and any request that already captured an
                    # older gate keeps waiting on THAT gate's marks — a
                    # reinstall can never re-block or unblock an
                    # already-arrived request.
                    if new_pages == "all":
                        state.barrier = _BarrierGate(frozenset(), True,
                                                     wait_s_for_install)
                    else:
                        state.barrier = _BarrierGate(frozenset(new_pages), False,
                                                     wait_s_for_install)
                if new_wait is not None:
                    if state.barrier is not None:
                        state.barrier.wait_s = new_wait
                    else:
                        state.barrier_default_wait_s = new_wait
                if release is not None and state.barrier is not None:
                    if release == "all":
                        state.barrier.all_released = True
                    else:
                        state.barrier.released.update(release)
                    with state.barrier.cond:
                        state.barrier.cond.notify_all()
            snapshot = {
                "name": state.name, "mode": state.mode, "content": state.content,
                "total": state.total, "page_size": state.page_size,
                "require_api_key": state.require_api_key,
                "request_count": state.request_count,
                "delay_ms": state.delay_ms,
                "delay_from_page": state.delay_from_page,
                "inventory_started": state.inventory_started,
                "inventory_finished": state.inventory_finished,
                "active": state.inventory_started - state.inventory_finished,
                "barrier_all": bool(state.barrier and state.barrier.all_pages),
                "barrier_pages": sorted(state.barrier.pages) if state.barrier else [],
                "barrier_wait_s": (state.barrier.wait_s if state.barrier
                                   else state.barrier_default_wait_s),
                "barrier_released": sorted(state.barrier.released) if state.barrier else [],
                "barrier_all_released": bool(state.barrier and state.barrier.all_released),
            }
        with state.waiters_lock:
            snapshot["barrier_waiters"] = state.barrier_waiters
        self.send_json(400 if errors else 200, {"errors": errors, "state": snapshot})

    # -------------------------------------------------------------- pagination
    def encode_cursor(self, page):
        return base64.urlsafe_b64encode(f"p{page}".encode()).decode().rstrip("=")

    @staticmethod
    def decode_cursor(token):
        pad = "=" * (-len(token) % 4)
        try:
            value = base64.urlsafe_b64decode(token + pad).decode()
        except Exception:
            return None
        return int(value[1:]) if value.startswith("p") and value[1:].isdigit() else None

    @staticmethod
    def _barrier_wait(state, gate, page):
        """P2.2: park on THIS request's captured generation gate until
        `page` (or everything) is released on that gate, for at most the
        gate's wait_s; False on bounded-wait expiry. The gate's own
        condition is held only in tiny critical sections (wait() releases
        it), so neither the sources lock, the state lock, nor any other
        src's traffic can be blocked by a parked request. The state-level
        waiter counter (all gates) is maintained under a separate small
        lock and cleaned up in finally."""
        with state.waiters_lock:
            state.barrier_waiters += 1
        with gate.cond:
            gate.waiters += 1
            try:
                deadline = time.monotonic() + gate.wait_s
                while not (gate.all_released or page in gate.released):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    gate.cond.wait(remaining)
                return True
            finally:
                gate.waiters -= 1
                with state.waiters_lock:
                    state.barrier_waiters -= 1

    def serve_inventory(self, url):
        # P2.2: inventory_finished counts a request once its response is
        # fully served (any status), so active = started - finished is
        # the true in-flight count including barrier-parked requests.
        try:
            self._serve_inventory_inner(url)
        finally:
            query = parse_qs(url.query)
            src = (query.get("src") or ["default"])[0]
            state = self.get_state(src)
            with state.lock:
                state.inventory_finished += 1

    def _serve_inventory_inner(self, url):
        query = parse_qs(url.query)
        src = (query.get("src") or ["default"])[0]
        state = self.get_state(src)
        with state.lock:
            state.request_count += 1
            state.inventory_started += 1
            mode, content, total = state.mode, state.content, state.total
            page_size = 1 if mode == "many_pages" else state.page_size
            require_api_key = state.require_api_key
            state_delay_ms, state_delay_from_page = state.delay_ms, state.delay_from_page
            # capture the CURRENT generation gate under the same lock that
            # installs gates: from here on, reinstallation cannot change
            # this request's barrier rules (snapshot/wait gap closed)
            state_barrier = state.barrier

        if require_api_key is not None and self.headers.get("X-API-Key") != require_api_key:
            self.send_json(401, {"error": "missing or invalid X-API-Key"})
            return

        wrap = (query.get("wrap") or ["items"])[0]
        if wrap not in ("items", "data", "array"):
            self.send_json(400, {"error": "wrap must be items|data|array"})
            return

        # Resolve the requested window into (page, start, size).
        if "cursor" in query:
            page = self.decode_cursor(query["cursor"][0])
            if page is None:
                self.send_json(400, {"error": "invalid cursor"})
                return
            start = (page - 1) * page_size
        elif "page" in query:
            try:
                page = int(query["page"][0])
            except ValueError:
                self.send_json(400, {"error": "page must be an integer"})
                return
            start = (page - 1) * page_size
        elif "offset" in query:
            try:
                start = int(query["offset"][0])
                page = start // page_size + 1
            except ValueError:
                self.send_json(400, {"error": "offset must be an integer"})
                return
        else:
            page, start = 1, 0
        if "limit" in query and mode != "many_pages":
            try:
                page_size = max(1, int(query["limit"][0]))
            except ValueError:
                self.send_json(400, {"error": "limit must be an integer"})
                return
        if "page_size" in query and mode != "many_pages":
            try:
                page_size = max(1, int(query["page_size"][0]))
            except ValueError:
                self.send_json(400, {"error": "page_size must be an integer"})
                return
        if page < 1:
            self.send_json(400, {"error": "page must be >= 1"})
            return

        # P2.2 releasable barrier (default OFF: no gate installed -> this
        # block is skipped entirely and every P1/P2.1 scenario behaves
        # exactly as before). Parked AFTER auth/page resolution and BEFORE
        # the delay/records slicing, on the request's OWN captured gate; a
        # bounded-wait expiry is an OBSERVABLE 504 so a forgotten release
        # can never masquerade as a slow-but-successful page.
        if state_barrier is not None and state_barrier.matches(page) \
                and not self._barrier_wait(state, state_barrier, page):
            self.send_json(504, {"error": "barrier wait expired", "page": page})
            return

        # request_count was already incremented at entry (pre-wait), so a
        # slow in-flight page is observable via /admin/src/{name} while it
        # waits. Delay never alters records, envelopes, or pagination.
        if state_delay_ms > 0 and page >= state_delay_from_page:
            import time as _time
            _time.sleep(state_delay_ms / 1000.0)

        # Failure modes trigger from page 2 onwards.
        if page >= 2 and mode == "fail_page2":
            self.send_json(500, {"error": "upstream exploded", "page": page})
            return
        if page >= 2 and mode == "invalid_json":
            body = '{"items": [{"sku": "000042", "qty": '  # truncated mid-object
            raw = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return

        # Out-of-range windows return an EMPTY page (HTTP 200) with no next
        # marker — page-mode adapters terminate on the empty array.
        window = records(total, content)[start:start + page_size] if start < total else []
        # loop mode: after page 3 point back at page 1 forever.
        next_page = page + 1 if start + page_size < total else None
        if mode == "loop" and page >= 3:
            next_page = 1
        next_params = {"src": src}
        if "cursor" in query:
            next_params["cursor"] = self.encode_cursor(next_page) if next_page else None
        elif "offset" in query:
            next_params["offset"] = start + page_size if next_page else None
        elif "limit" in query or "page_size" in query:
            next_params["page"] = next_page
            next_params["page_size"] = page_size
        else:
            next_params["page"] = next_page
            next_params["page_size"] = page_size
        next_url = None
        if next_page:
            next_params = {k: v for k, v in next_params.items() if v is not None}
            next_url = url.path + "?" + urlencode(next_params)

        headers = {}
        if next_url:
            headers["Link"] = f'<{next_url}>; rel="next"'
            headers["X-Next-Page"] = str(next_page)

        if wrap == "array":
            self.send_json(200, window, headers)
            return
        key = "items" if wrap == "items" else "data"
        payload = {
            key: window,
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": (total + page_size - 1) // page_size,
        }
        if next_page:
            payload["next_page"] = next_page
            payload["next_cursor"] = self.encode_cursor(next_page)
            payload["next_offset"] = start + page_size
            if next_url:
                payload["next_url"] = next_url
        self.send_json(200, payload, headers)


def main():
    import sys

    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8090
    server = ThreadingHTTPServer((host, port), FixtureHandler)
    print(f"[fixture] synthetic inventory source listening on {host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
