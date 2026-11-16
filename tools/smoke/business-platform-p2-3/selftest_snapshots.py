#!/usr/bin/env python3
"""Local (no-stack) selftest of admission_snapshots STRICTNESS — compare
rules, strict envelope parsers, and full-set txn traversal.

Pure synthetic data: no PG, no HTTP, no docker. Proves the comparator
refuses exactly the fake-green shapes root banned (review batches 1+3):
empty snapshot pairs, missing handle universes, missing sources/keys,
null==null agreements (except the legal explicit null stamp), skipped
targets, un-allowed changes, and ghost transactions. Also proves the
strict files/txns envelope parsers reject malformed 200-bodies, and the
cursor-following traversal collects the FULL set without truncation.

Run: python3 -I selftest_snapshots.py (exit 0 iff all pass).
"""

import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "p23snap", HERE / "admission_snapshots.py")
SNAP = importlib.util.module_from_spec(spec)
spec.loader.exec_module(SNAP)

PASS = 0
FAIL = 0


def check(label, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"ok   {label} (ok={got})")
    else:
        FAIL += 1
        print(f"FAIL {label} ok={got} want={want}")


def check_raises(label, fn):
    global PASS, FAIL
    try:
        fn()
    except ValueError as exc:
        PASS += 1
        print(f"ok   {label} (raises: {str(exc)[:70]})")
        return
    except Exception as exc:  # noqa: BLE001 — wrong exception type = fail
        FAIL += 1
        print(f"FAIL {label} raised {type(exc).__name__} not ValueError")
        return
    FAIL += 1
    print(f"FAIL {label} did not raise")


HANDLES = {
    "sources": [{"id": "s1", "src": "src1"}],
    "syncs": [{"id": "y1", "source_id": "s1", "target_id": "d1"}],
    "runs": ["r1"],
    "targets": [{"dataset_id": "d1", "token": "SECRET-TOKEN"}],
}

SANITIZED = {"sources": [{"id": "s1", "src": "src1"}],
             "syncs": [{"id": "y1", "source_id": "s1", "target_id": "d1"}],
             "runs": ["r1"], "targets": ["d1"]}


def snap(**over):
    base = {
        "taken_at": "t",
        "handles": json.loads(json.dumps(SANITIZED)),
        "collection_failures": [],
        "sources": {"s1": {"stamp": "2026-11-14 01:00:00+00",
                           "inflight": 1, "run_ids": ["r1"]}},
        "runs": {"r1": dict(zip(SNAP.RUN_PROJECTION_KEYS,
                                ["running", "t", "h", "e", "t", "f", "f",
                                 "controlled", "1", "1", "f", "f", ""]))},
        "syncs": {"y1": ["r1"]},
        "fixture": {"src1": {"request_count": 2, "inventory_started": 2,
                             "inventory_finished": 1, "active": 1,
                             "barrier_waiters": 1}},
        "targets": {"d1": {"file_ids": [], "txn_status": {}}},
    }
    base.update(over)
    return base


# ---- compare: universe + presence + fake-green traps ------------------
check("empty-pair-fails", SNAP.compare({}, {})["ok"], False)
check("empty-pair-reasons",
      any("handles" in v for v in SNAP.compare({}, {})["violations"]), True)

check("invariant-identical", SNAP.compare(snap(), snap())["ok"], True)

b = snap()
a = snap()
a["sources"]["s1"]["inflight"] = 2
check("unallowed-change", SNAP.compare(b, a)["ok"], False)
check("allowed-change",
      SNAP.compare(b, a, allowed=["sources.s1.inflight"])["ok"], True)

a = snap()
del a["sources"]["s1"]
check("missing-source-entry", SNAP.compare(snap(), a)["ok"], False)

a = snap()
a["sources"]["s-ghost"] = {"stamp": None, "inflight": 0, "run_ids": []}
check("extra-source-entry", SNAP.compare(snap(), a)["ok"], False)

a = snap()
del a["sources"]["s1"]["stamp"]
check("missing-stamp-key", SNAP.compare(snap(), a)["ok"], False)

a = snap()
del a["runs"]["r1"]["status"]
check("missing-projection-key", SNAP.compare(snap(), a)["ok"], False)

# explicit null stamp is the ONE legal null (never-admitted source)
b = snap()
a = snap()
b["sources"]["s1"]["stamp"] = None
a["sources"]["s1"]["stamp"] = None
check("null-stamp-both-sides-legal", SNAP.compare(b, a)["ok"], True)

b = snap()
a = snap()
b["sources"]["s1"]["stamp"] = None
check("null-vs-value-stamp-is-change",
      SNAP.compare(b, a)["ok"], False)
check("null-vs-value-stamp-allowed",
      SNAP.compare(b, a, allowed=["sources.s1.stamp"])["ok"], True)

a = snap()
a["targets"]["d1"] = "skipped:no-token"
check("skipped-target-violation", SNAP.compare(snap(), a)["ok"], False)

a = snap()
a["syncs"]["y1"] = ["r1", "r-ghost"]
check("binding-leak-detected", SNAP.compare(snap(), a)["ok"], False)

a = snap()
a["syncs"]["y1"] = ["r1", "r2"]
a["sources"]["s1"]["run_ids"] = ["r1", "r2"]
res = SNAP.compare(snap(), a)
check("run-set-change-unallowed", res["ok"], False)

b = snap(collection_failures=[{"where": "sources.s1", "detail": "x"}])
check("collection-failure-vetoes", SNAP.compare(b, snap())["ok"], False)

# ghost transaction (no file) is visible through the FULL txn set
a = snap()
a["targets"]["d1"]["txn_status"] = {"t9": "OPEN"}
check("ghost-txn-detected", SNAP.compare(snap(), a)["ok"], False)
check("ghost-txn-allowed-recorded",
      "targets.d1.txn_status" in
      SNAP.compare(snap(), a, allowed=["targets.d1.txn_status"])["changes"],
      True)

a = snap()
a["fixture"]["src1"]["request_count"] = 3
check("fixture-counter-change", SNAP.compare(snap(), a)["ok"], False)

# heartbeat may legitimately advance — but ONLY via an explicit allow
a = snap()
a["runs"]["r1"]["heartbeat"] = "h-later"
check("heartbeat-needs-explicit-allow",
      SNAP.compare(snap(), a)["ok"], False)
check("heartbeat-explicit-allow",
      SNAP.compare(snap(), a, allowed=["runs.r1.heartbeat"])["ok"], True)

# ---- batch-A: universe rules + identical-wrong-type bypass ----------
def snap_with_handles(h, **over):
    s = snap(**over)
    s["handles"] = h
    return s


EMPTY_H = {"sources": [], "syncs": [], "runs": [], "targets": []}
check("empty-universe-fails",
      SNAP.compare(snap_with_handles(EMPTY_H),
                   snap_with_handles(EMPTY_H))["ok"], False)

h_no_srcs = json.loads(json.dumps(SANITIZED)); h_no_srcs["sources"] = []
check("empty-sources-universe-fails",
      SNAP.compare(snap_with_handles(h_no_srcs),
                   snap_with_handles(h_no_srcs))["ok"], False)

h_no_syncs = json.loads(json.dumps(SANITIZED)); h_no_syncs["syncs"] = []
check("empty-syncs-universe-fails",
      SNAP.compare(snap_with_handles(h_no_syncs),
                   snap_with_handles(h_no_syncs))["ok"], False)

h_no_targets = json.loads(json.dumps(SANITIZED)); h_no_targets["targets"] = []
check("empty-targets-universe-fails",
      SNAP.compare(snap_with_handles(h_no_targets),
                   snap_with_handles(h_no_targets))["ok"], False)

# runs MAY be empty (legal) — all other sections intact
h_no_runs = json.loads(json.dumps(SANITIZED)); h_no_runs["runs"] = []
b = snap_with_handles(h_no_runs); b["runs"] = {}
a = snap_with_handles(h_no_runs); a["runs"] = {}
check("empty-runs-universe-legal", SNAP.compare(b, a)["ok"], True)

h_no_srcname = json.loads(json.dumps(SANITIZED))
h_no_srcname["sources"][0]["src"] = None
check("source-handle-missing-src-fails",
      SNAP.compare(snap_with_handles(h_no_srcname),
                   snap_with_handles(h_no_srcname))["ok"], False)

# identical WRONG types must never pass via equality (the bypass trap)
b = snap(); a = snap()
b["sources"]["s1"] = "oops"; a["sources"]["s1"] = "oops"
check("same-string-source-bypass-fails",
      SNAP.compare(b, a)["ok"], False)

b = snap(); a = snap()
b["syncs"]["y1"] = "oops"; a["syncs"]["y1"] = "oops"
check("same-string-sync-bypass-fails",
      SNAP.compare(b, a)["ok"], False)

b = snap(); a = snap()
b["syncs"]["y1"] = ["r1", 5]; a["syncs"]["y1"] = ["r1", 5]
check("sync-list-non-str-item-fails",
      SNAP.compare(b, a)["ok"], False)

b = snap(); a = snap()
b["targets"]["d1"] = "oops"; a["targets"]["d1"] = "oops"
check("same-string-target-bypass-fails",
      SNAP.compare(b, a)["ok"], False)

# closeout-4: a syncs entry as a dict ({} on BOTH sides) must fail —
# the old dict-branch bypass that skipped sync typing and binding
b = snap(); a = snap()
b["syncs"]["y1"] = {}; a["syncs"]["y1"] = {}
check("same-empty-dict-sync-bypass-fails",
      SNAP.compare(b, a)["ok"], False)

b = snap(); a = snap()
b["syncs"]["y1"] = {"r1": 1}; a["syncs"]["y1"] = {"r1": 1}
check("same-dict-sync-bypass-fails",
      SNAP.compare(b, a)["ok"], False)

# ---- strict files envelope -------------------------------------------
good_files = {"files": [{"id": "f1"}, {"id": "f2"}]}
check("files-good", [f["id"] for f in
                     SNAP.parse_files_envelope(good_files)], ["f1", "f2"])
check_raises("files-missing-key", lambda: SNAP.parse_files_envelope({}))
check_raises("files-empty-dict-value",
             lambda: SNAP.parse_files_envelope({"files": {}}))
check_raises("files-empty-string-value",
             lambda: SNAP.parse_files_envelope({"files": ""}))
check_raises("files-item-not-object",
             lambda: SNAP.parse_files_envelope({"files": ["f1"]}))
check_raises("files-item-no-id",
             lambda: SNAP.parse_files_envelope({"files": [{"x": 1}]}))
check_raises("files-duplicate-ids",
             lambda: SNAP.parse_files_envelope(
                 {"files": [{"id": "f1"}, {"id": "f1"}]}))

# ---- strict txns page envelope ----------------------------------------
check("txns-null-data-legal",
      SNAP.parse_txns_page({"data": None, "has_more": False}),
      ([], None, False))
check("txns-good-page",
      SNAP.parse_txns_page({"data": [{"id": "t1", "status": "OPEN"}],
                            "has_more": False}),
      ([{"id": "t1", "status": "OPEN"}], None, False))
check_raises("txns-missing-has_more",
             lambda: SNAP.parse_txns_page({"data": []}))
check_raises("txns-has-more-no-cursor",
             lambda: SNAP.parse_txns_page({"data": [], "has_more": True}))
check_raises("txns-item-no-id",
             lambda: SNAP.parse_txns_page({"data": [{}], "has_more": False}))
check_raises("txns-item-id-not-string",
             lambda: SNAP.parse_txns_page({"data": [{"id": 7}],
                                           "has_more": False}))
check_raises("txns-data-not-list",
             lambda: SNAP.parse_txns_page({"data": "x", "has_more": False}))

# ---- full-set traversal with cursor following -------------------------
PAGES = {
    None: (200, {"data": [{"id": "t1", "status": "OPEN"},
                          {"id": "t2", "status": "COMMITTED"}],
                 "next_cursor": "c2", "has_more": True}),
    "c2": (200, {"data": None, "has_more": False}),   # legal nil tail page
}


def fetch(cursor):
    return PAGES[cursor]


check("txns-traversal-full-set",
      SNAP.enumerate_target_txns(fetch),
      {"t1": "OPEN", "t2": "COMMITTED"})


def endless(cursor):
    return (200, {"data": [{"id": "t", "status": "OPEN"}],
                  "next_cursor": "again", "has_more": True})


check_raises("txns-traversal-bounded",
             lambda: SNAP.enumerate_target_txns(endless, max_pages=3))


def bad_status(cursor):
    return (500, {"error": "x"})


check_raises("txns-traversal-non-200",
             lambda: SNAP.enumerate_target_txns(bad_status))


# batch-A: duplicate ids across pages FAIL (never silently overwrite)
def dup_pages(cursor):
    pages = {
        None: (200, {"data": [{"id": "t1", "status": "OPEN"}],
                     "next_cursor": "c2", "has_more": True}),
        "c2": (200, {"data": [{"id": "t1", "status": "COMMITTED"}],
                     "has_more": False}),
    }
    return pages[cursor]


check_raises("txns-duplicate-id-across-pages",
             lambda: SNAP.enumerate_target_txns(dup_pages))


# batch-A: a status outside the pinned vocabulary FAILS (never defaults)
def vocab_pages(cursor):
    return (200, {"data": [{"id": "t1", "status": "weird-status"}],
                  "has_more": False})


check_raises("txns-illegal-status-vocabulary",
             lambda: SNAP.enumerate_target_txns(vocab_pages))


def empty_status_pages(cursor):
    return (200, {"data": [{"id": "t1", "status": ""}],
                  "has_more": False})


check_raises("txns-empty-status",
             lambda: SNAP.enumerate_target_txns(empty_status_pages))


# ---- sanitized handles never carry tokens -----------------------------
h = {"sources": [{"id": "s1", "src": "src1"}],
     "syncs": [{"id": "y1", "source_id": "s1", "target_id": "d1"}],
     "runs": ["r1"],
     "targets": [{"dataset_id": "d1", "token": "SECRET-TOKEN"}]}
san = SNAP.sanitize_handles(h)
check("handles-token-stripped",
      "SECRET-TOKEN" in json.dumps(san), False)
check("handles-shape", san == SANITIZED, True)

print(f"\n[p23-snapshots-selftest] pass={PASS} fail={FAIL}")
sys.exit(1 if FAIL else 0)
