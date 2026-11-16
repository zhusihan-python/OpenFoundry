#!/usr/bin/env python3
"""Local stdlib self-test for the P2.2 barrier extension of the SHARED
fixture (tools/smoke/business-platform-p1/fixtures/rest_source.py).

Runs the fixture in-process on an ephemeral port; touches nothing but
localhost. Verifies, per the root authorization (2026-11-14):

  * default OFF -> P1/P2.1 behavior byte-identical (bodies compared),
    zero added latency, new snapshot fields present but inert;
  * per-src/page releasable barrier: park, live waiter/active counts,
    release of one page / "all", install-resets-release;
  * bounded wait: expiry is an observable 504, waiters cleaned up;
  * no global blocking: a parked src never delays another src;
  * admin validation is atomic for the barrier group (bad input leaves
    state untouched; mixed valid+invalid applies nothing);
  * started/finished/active counts are exact across sequential and
    concurrent traffic;
  * the admin/state surface never stores or echoes Authorization or any
    other request header.

Usage: python3 -I selftest_fixture.py   (exit 0 iff all cases pass)
"""

import importlib.util
import json
from pathlib import Path
import sys
import threading
import time
from http.server import ThreadingHTTPServer
import urllib.error
import urllib.request

FIXTURE = (Path(__file__).resolve().parent.parent
           / "business-platform-p1" / "fixtures" / "rest_source.py")

spec = importlib.util.spec_from_file_location("p22_fixture_under_test", FIXTURE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

PASS = 0
FAIL = 0


def ok(cond, label, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"ok   {label}")
    else:
        FAIL += 1
        print(f"FAIL {label} {detail}")


def get(path, timeout=10, headers=None):
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (PORT, path),
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def admin_set(src, patch):
    body = json.dumps(patch).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}/admin/src/{src}/set", data=body,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def raw_get(path, timeout=10):
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (PORT, path))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def snap(src):
    return get(f"/admin/src/{src}")[1]


def parked_get(path, box, timeout=10):
    """Fetch in a thread; store (status, body) or the exception."""
    def run():
        try:
            box["res"] = get(path, timeout=timeout)
        except Exception as exc:  # timeout/socket errors surface as failures
            box["err"] = repr(exc)
    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th


server = ThreadingHTTPServer(("127.0.0.1", 0), mod.FixtureHandler)
PORT = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()

# --- 1. default OFF: byte-identical bodies + inert new fields ----------
t0 = time.monotonic()
s1, b1 = get("/v1/inventory?src=parity-a&page=1&page_size=50")
s1b, b1b = get("/v1/inventory?src=parity-a&page=2&page_size=50")
elapsed = time.monotonic() - t0
s2, b2 = get("/v1/inventory?src=parity-b&page=1&page_size=50")
ok(s1 == s1b == s2 == 200, "default pages 200")
# RAW byte capture for the within-src parity proof below — a parsed-JSON
# comparison must never be labelled "byte-identical" (review fix A).
raw_b_before = raw_get("/v1/inventory?src=parity-b&page=1&page_size=50")[1]
# next_url embeds the src name, so cross-src parity compares the DATA
# only (an honest data-level claim, never a byte-level one).
ok(b1["items"] == b2["items"] and b1b["items"][0]["sku"] == "000051",
   "default record data identical across srcs")
ok(elapsed < 0.3, "default latency-free", f"{elapsed:.3f}s")
sa = snap("parity-a")
for key in ("inventory_started", "inventory_finished", "active",
            "barrier_all", "barrier_pages", "barrier_wait_s",
            "barrier_released", "barrier_all_released", "barrier_waiters"):
    ok(key in sa, f"snapshot exposes {key}")
ok(sa["inventory_started"] == 2 and sa["inventory_finished"] == 2
   and sa["active"] == 0, "default counts exact", str(sa))
ok(sa["barrier_pages"] == [] and sa["barrier_all"] is False
   and sa["barrier_waiters"] == 0, "default barrier inert")

# explicit disable must equal the untouched default — compared on RAW
# bytes (review fix A): parsed-JSON equality is not byte identity.
admin_set("parity-b", {"barrier_pages": [], "barrier_wait_s": 120})
ok(raw_get("/v1/inventory?src=parity-b&page=1&page_size=50")[1] == raw_b_before,
   "barrier_pages=[] keeps RAW response bytes identical")

# --- 2. park + live counts + release one page --------------------------
admin_set("bar-a", {"barrier_pages": [1]})
box = {}
th = parked_get("/v1/inventory?src=bar-a&page=1&page_size=5", box)
time.sleep(0.4)
s = snap("bar-a")
ok(s["barrier_waiters"] == 1 and s["active"] == 1
   and s["inventory_started"] == 1 and s["inventory_finished"] == 0,
   "parked request observable (waiters/active, not finished)", str(s))
# no global blocking: another src stays fast while bar-a is parked
t0 = time.monotonic()
get("/v1/inventory?src=bar-other&page=1&page_size=5")
ok(time.monotonic() - t0 < 0.3, "parked src does not block other srcs")
st, body = admin_set("bar-a", {"barrier_release": [1]})
th.join(timeout=5)
ok(box.get("res", (0,))[0] == 200
   and len(box.get("res", (None, None))[1]["items"]) == 5,
   "released page served with real records", str(box))
s = snap("bar-a")
ok(s["barrier_waiters"] == 0 and s["active"] == 0
   and s["inventory_finished"] == 1, "counts settle after release", str(s))

# --- 3. barrier "all" + release "all" + concurrent waiters --------------
admin_set("bar-all", {"barrier_pages": "all"})
boxes = [{} for _ in range(4)]
ths = [parked_get(f"/v1/inventory?src=bar-all&page={p}&page_size=5", b)
       for p, b in zip((1, 2, 3, 1), boxes)]
time.sleep(0.4)
s = snap("bar-all")
ok(s["barrier_waiters"] == 4, "four waiters parked", str(s))
admin_set("bar-all", {"barrier_release": "all"})
for th in ths:
    th.join(timeout=5)
ok(all(b.get("res", (0,))[0] == 200 for b in boxes),
   "release all wakes every waiter", str(boxes))
s = snap("bar-all")
ok(s["inventory_finished"] == 4 and s["active"] == 0,
   "concurrent counts exact", str(s))

# --- 4. bounded wait: expiry is an observable 504 ------------------------
admin_set("bar-exp", {"barrier_pages": [1], "barrier_wait_s": 1})
box = {}
th = parked_get("/v1/inventory?src=bar-exp&page=1&page_size=5", box)
th.join(timeout=6)
status, body = box.get("res", (None, None))
ok(status == 504 and body.get("error") == "barrier wait expired",
   "bounded expiry answers 504 with marker", str(box))
s = snap("bar-exp")
ok(s["barrier_waiters"] == 0 and s["active"] == 0,
   "waiter cleaned up after expiry", str(s))

# --- 5. install resets release marks -------------------------------------
admin_set("bar-reset", {"barrier_pages": [1]})
admin_set("bar-reset", {"barrier_release": [1]})
ok(get("/v1/inventory?src=bar-reset&page=1&page_size=5")[0] == 200,
   "released page passes immediately")
admin_set("bar-reset", {"barrier_pages": [1]})
box = {}
th = parked_get("/v1/inventory?src=bar-reset&page=1&page_size=5", box)
time.sleep(0.3)
ok(snap("bar-reset")["barrier_waiters"] == 1,
   "reinstalling barrier_pages parks again (release marks reset)")
admin_set("bar-reset", {"barrier_release": "all"})
th.join(timeout=5)
ok(box.get("res", (0,))[0] == 200, "second install releasable normally")

# --- 6. validation is atomic for the barrier group -----------------------
admin_set("bar-val", {"barrier_pages": [2]})
st, body = admin_set("bar-val", {"barrier_pages": [0], "barrier_wait_s": 0})
ok(st == 400 and len(body["errors"]) == 2, "invalid barrier input -> 400", str(body))
sv = snap("bar-val")
ok(sv["barrier_pages"] == [2] and sv["barrier_wait_s"] == 120,
   "rejected patch leaves barrier state untouched", str(sv))
st, body = admin_set("bar-val", {"barrier_pages": [1], "barrier_release": ["x"]})
ok(st == 400, "mixed valid+invalid patch rejected")
ok(snap("bar-val")["barrier_pages"] == [2],
   "mixed patch applied NOTHING (atomic group)")
for bad in ([10001], ["1"], [True], 5, "allx"):
    st, _ = admin_set("bar-val", {"barrier_pages": bad})
    ok(st == 400, f"barrier_pages {bad!r} rejected")
for bad in (0, 601, "5", True):
    st, _ = admin_set("bar-val", {"barrier_wait_s": bad})
    ok(st == 400, f"barrier_wait_s {bad!r} rejected")

# --- 7. P21 delay still works next to the barrier ------------------------
admin_set("bar-delay", {"delay_ms": 300, "delay_from_page": 2})
t0 = time.monotonic()
get("/v1/inventory?src=bar-delay&page=1&page_size=50")
fast = time.monotonic() - t0
t0 = time.monotonic()
get("/v1/inventory?src=bar-delay&page=2&page_size=50")
slow = time.monotonic() - t0
ok(fast < 0.15 and 0.28 <= slow < 1.0, "delay_ms behaviour unchanged",
   f"fast={fast:.3f}s slow={slow:.3f}s")

# --- 7b. generation gap counter-example (review fix 3): a parked request
# must complete on ITS generation's release marks even when the barrier
# is REINSTATED immediately after — deterministic order enforced by
# observing waiters>=1 before the release/reinstall pair. ---------------
admin_set("bar-gen", {"barrier_pages": [1]})
box_g1 = {}
th_g1 = parked_get("/v1/inventory?src=bar-gen&page=1&page_size=5", box_g1)
gen_deadline = time.monotonic() + 5
while time.monotonic() < gen_deadline \
        and snap("bar-gen").get("barrier_waiters", 0) < 1:
    time.sleep(0.05)
ok(snap("bar-gen")["barrier_waiters"] == 1, "gen: R1 parked on gate g1")
admin_set("bar-gen", {"barrier_release": [1]})       # release ON g1
admin_set("bar-gen", {"barrier_pages": [1]})         # reinstall as NEW g2
th_g1.join(timeout=5)
ok(box_g1.get("res", (0,))[0] == 200,
   "gen: R1 completed on g1's marks despite immediate reinstall", str(box_g1))
box_g2 = {}
th_g2 = parked_get("/v1/inventory?src=bar-gen&page=1&page_size=5", box_g2)
gen_deadline = time.monotonic() + 5
while time.monotonic() < gen_deadline \
        and snap("bar-gen").get("barrier_waiters", 0) < 1:
    time.sleep(0.05)
ok(snap("bar-gen")["barrier_waiters"] == 1,
   "gen: a NEW request parks on the fresh gate g2")
ok(snap("bar-gen")["barrier_released"] == [],
   "gen: g2 carries no stale release marks from g1")
admin_set("bar-gen", {"barrier_release": "all"})
th_g2.join(timeout=5)
ok(box_g2.get("res", (0,))[0] == 200, "gen: g2 releasable normally")

# --- 7c. release with no barrier installed is a safe no-op ---------------
st_noop, body_noop = admin_set("bar-noop", {"barrier_release": "all"})
ok(st_noop == 200 and body_noop["errors"] == [],
   "release without an installed barrier is a no-op")

# --- 8. no sensitive header storage/echo ---------------------------------
get("/v1/inventory?src=bar-a&page=1&page_size=5",
    headers={"Authorization": "Bearer SECRETVALUE", "X-Obscure": "hush"})
blob = json.dumps(snap("bar-a")) + json.dumps(admin_set("bar-a", {})[1])
ok("SECRETVALUE" not in blob and "Authorization" not in blob
   and "X-Obscure" not in blob, "state/admin surfaces carry no headers")

server.shutdown()
print(f"\n[p22-fixture-selftest] pass={PASS} fail={FAIL}")
sys.exit(0 if FAIL == 0 else 1)
