#!/usr/bin/env python3
"""P2.5 fixture + proxy PURE selftest (no sockets, no runtime).

Covers the fixture-proxy-round-1 semantics: NULL always re-serves the
initial window (delivery history is NOT consumer position), token-
driven windows with stable replay, candidate derived from the request
window, per-request counter lifecycle via begin/finish (including
error paths), removed empty-batch knob, isolated single-predicate ACK
fabrications, and the drop_after_verify verdict function. The REAL
HTTP handler paths (blocking, disconnects, admin progress) are covered
by selftest_p25_loopback.py under the scoped loopback release.
"""

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PASS, FAIL = [], []


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


def walk(app, src, params):
    st, body = app.data(src, dict(params))
    rows = len(body.get("records", [])) if st == 200 else 0
    while st == 200 and body.get("next_cursor"):
        p = dict(params)
        p["cursor"] = body["next_cursor"]
        st, body = app.data(src, p)
        rows += len(body.get("records", [])) if st == 200 else 0
    return st, body, rows


def tok_of(body):
    return body.get("meta", {}).get("completed_token")


def main():
    fx = loadmod(HERE / "fixtures" / "rest_source_p25.py", "fx")
    px = loadmod(HERE / "fixtures" / "dataset_fault_proxy_p25.py", "px")

    print("[A] baseline A→B→C with real pagination")
    app = fx.IncrementalFixture()
    st, body = app.data("p25h1", {})
    check("A1 run1 page1 rows + cursor", st == 200
          and len(body["records"]) == 12 and body["next_cursor"])
    st, body, _ = walk(app, "p25h1", {})
    check("A2 run1 terminal token tok-0", st == 200
          and tok_of(body) == "p25h1-tok-0" and body["next_cursor"] == "")
    tok0 = tok_of(body)
    st, body, rows = walk(app, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok0})
    check("A3 run2 (tok-0) serves batch B and completes tok-1",
          st == 200 and rows == 20 and tok_of(body) == "p25h1-tok-1"
          and body["records"][0].startswith("b-"))
    tok1 = tok_of(body)
    st, body, rows = walk(app, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok1})
    check("A4 run3 (tok-1) serves batch C and completes tok-2",
          st == 200 and rows == 30 and tok_of(body) == "p25h1-tok-2")
    tok2 = tok_of(body)

    print("[B] consumer-position semantics (review item 1)")
    st, body, rows = walk(app, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok2})
    check("B1 exhausted position: empty rows, re-issues REQUEST token",
          st == 200 and rows == 0 and tok_of(body) == tok2)
    st, body, rows = walk(app, "p25h1", {})
    check("B2 NULL retry ALWAYS re-serves the initial window A "
          "(delivery history is not consumer position)",
          st == 200 and rows == 24 and tok_of(body) == tok0
          and body["records"][0].startswith("a-"))
    st, b1, r1 = walk(app, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok0})
    st2, b2, r2 = walk(app, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok0})
    check("B3 repeated same token ⇒ identical stable window + token",
          (r1, tok_of(b1)) == (r2, tok_of(b2)) == (20, tok1))
    st, b, _ = walk(app, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok0})
    check("B4 old-token replay does NOT move anyone's position "
          "(still derives tok-1 from the REQUEST window)",
          tok_of(b) == tok1)
    st, body = app.data("p25h1", {fx.DEFAULT_SINCE_PARAM: "foreign-x"})
    check("B5 unknown/foreign token 400", st == 400)
    check("B6 control-char/oversize token 400",
          app.data("p25h1", {fx.DEFAULT_SINCE_PARAM:
                             "bad\x01tok"})[0] == 400
          and app.data("p25h1", {fx.DEFAULT_SINCE_PARAM:
                                 "x" * 513})[0] == 400)
    st, body, _ = walk(app, "p25empty", {})
    check("B7 initial-empty src: first terminal 0 rows + tok-0",
          st == 200 and body["records"] == []
          and tok_of(body) == "p25empty-tok-0")

    print("[C] fault knobs")
    app2 = fx.IncrementalFixture()
    app2.set_knob("p25h4", {"fail_mid_pages": True})
    st, b1 = app2.data("p25h4", {})
    st, b2 = app2.data("p25h4", {"cursor": b1["next_cursor"]})
    check("C1 mid-page 500 knob", st == 500)
    app3 = fx.IncrementalFixture()
    app3.set_knob("p25h4", {"fail_terminal_after_rows": True})
    st, b1, served = walk(app3, "p25h4", {})
    check("C2 terminal-after-rows 500 (position not consumed)",
          st == 500 and served == 12)
    app3.set_knob("p25h4", {"fail_terminal_after_rows": False})
    st, b1, total = walk(app3, "p25h4", {})
    check("C3 NULL retry after terminal failure re-serves batch A "
          "intact", st == 200 and tok_of(b1) == "p25h4-tok-0"
          and total == 24)
    for mode, expect in (
            ("missing", None),
            ("non_string", {"not": "a string"}),
            ("empty", ""),
            ("oversize", "x" * 513),
            ("ctrl_chars", "bad\x01token")):
        appm = fx.IncrementalFixture()
        appm.set_knob("p25h4", {"candidate_mode": mode})
        st, bm, _ = walk(appm, "p25h4", {})
        got = bm.get("meta", {}).get("completed_token", "__MISSING__")
        want = expect if expect is not None else "__MISSING__"
        check(f"C4 candidate_mode={mode}", got == want, f"got {got!r}")
    appe = fx.IncrementalFixture()
    st, b1, _ = walk(appe, "p25h1", {})
    tok = tok_of(b1)
    appe.set_knob("p25h1", {"candidate_mode": "echo_request_token"})
    st, b2, rows2 = walk(appe, "p25h1", {fx.DEFAULT_SINCE_PARAM: tok})
    check("C5 echo_request_token: rows>0 + candidate==REQUEST token",
          st == 200 and rows2 > 0 and tok_of(b2) == tok)

    print("[D] lifecycle + counters + knobs registry")
    appl = fx.IncrementalFixture()
    fin = appl.begin("p25h3")
    check("D1 begin→finish lifecycle pair", fin is not None)
    appl.data("p25h3", {})
    fin()
    snap = appl.snapshot("p25h3")
    check("D2 success path quiet", snap["active"] == 0
          and snap["inventory_started"] == snap["inventory_finished"]
          == 1 and appl.quiet())
    appl.set_knob("p25h3", {"fail_mid_pages": True})
    fin = appl.begin("p25h3")
    st, b1 = appl.data("p25h3", {})
    st2, b2 = appl.data("p25h3", {"cursor": b1["next_cursor"]})
    fin()                                     # error exits still finish
    snap = appl.snapshot("p25h3")
    check("D3 error path quiet (finish on injected 500)",
          st2 == 500 and snap["active"] == 0
          and snap["inventory_started"] == snap["inventory_finished"])
    check("D4 removed empty_batch knob now REJECTED",
          not appl.set_knob("p25h3", {"empty_batch": True})
          and not appl.set_knob("no-such-src",
                                {"fail_mid_pages": True}))
    app_x1 = fx.IncrementalFixture()
    app_x2 = fx.IncrementalFixture()
    _, bx1 = app_x1.data("p25h3", {})
    _, bx2 = app_x2.data("p25h3", {})
    check("D5 page keys deterministic across instances",
          bx1["next_cursor"] == bx2["next_cursor"])
    appb = fx.IncrementalFixture()
    st, b1, _ = walk(appb, "p25h4", {})
    t0 = tok_of(b1)
    st, b2, rows_b = walk(appb, "p25h4", {fx.DEFAULT_SINCE_PARAM: t0})
    check("D6 p25h4 second NON-EMPTY window B after seed (nil still A)",
          rows_b == 8 and b2["records"][0].startswith("b-")
          and tok_of(b2) == "p25h4-tok-1")

    print("[E] proxy core (isolated predicates + verdict + stats)")
    core = px.ProxyCore()
    tgt = "11111111-1111-4111-8111-111111111111"
    check("E1 requested_target parsed from path",
          core.requested_target(f"/api/v1/datasets/{tgt}/outputs:commit")
          == tgt and core.requested_target("/api/v1/datasets/x") is None)
    s, b = px.fabricate("no_txn_200", tgt)
    check("E2 no_txn_200: correct target+COMMITTED, txn id ABSENT",
          s == 200 and b["dataset_id"] == tgt
          and b["transaction"]["status"] == "COMMITTED"
          and "id" not in b["transaction"])
    s, b = px.fabricate("not_committed", tgt)
    check("E3 not_committed: correct target+valid txn, status OPEN",
          s == 200 and b["dataset_id"] == tgt
          and b["transaction"]["id"] == px.FAKE_TXN_ID
          and b["transaction"]["status"] == "OPEN")
    s, b = px.fabricate("wrong_target", tgt)
    check("E4 wrong_target: ONLY target differs",
          s == 200 and b["dataset_id"] != tgt
          and b["transaction"]["id"] == px.FAKE_TXN_ID
          and b["transaction"]["status"] == "COMMITTED")
    real_txn = "55555555-5555-4555-8555-555555555555"
    good = {"ack_dataset_id": tgt, "ack_txn_id": real_txn,
            "ack_txn_status": "COMMITTED"}
    neg = []
    for bad_id in ("t-1",
                   "00000000-0000-0000-0000-000000000000",
                   None, "not-a-uuid", ""):
        neg.append(px.verify_commit_ack(
            dict(good, ack_txn_id=bad_id), tgt)
            ["verified_committed"])
    check("E5 verify verdict: real-UUID positive; zero-UUID/junk/"
          "None/empty all negative",
          px.verify_commit_ack(good, tgt)["verified_committed"]
          and not any(neg)
          and not px.verify_commit_ack(
              dict(good, ack_txn_status="OPEN"), tgt)
              ["verified_committed"]
          and not px.verify_commit_ack(
              dict(good, ack_dataset_id="other"), tgt)
              ["verified_committed"])
    core.begin_commit()
    check("E6 inflight tracked; quiet only after finish",
          not core.quiet())
    core.finish_commit()
    check("E7 quiet after finish", core.quiet())
    check("E8 is_commit_post precise",
          core.is_commit_post("POST",
                              f"/api/v1/datasets/{tgt}/outputs:commit")
          and not core.is_commit_post("POST", f"/api/v1/datasets/{tgt}")
          and not core.is_commit_post(
              "GET", f"/api/v1/datasets/{tgt}/outputs:commit"))

    print(f"\n[selftest-p25-fixture-proxy] pass={len(PASS)} "
          f"fail={len(FAIL)}")
    for f in FAIL:
        print(f"  FAILED: {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
