#!/usr/bin/env python3
"""Local selftest for the P2.4 live-guard PURE policy (DRAFT 2026-11-14).

No docker, no database, no HTTP — only account()/_parse_ts with the real
generated pins plus synthetic mutations. Run (AFTER root release) as:
    python3 -I selftest_live_guard_p24.py
Exit 0 iff every case behaves exactly as the policy demands.
"""

import copy
import importlib.util
import sys
from pathlib import Path


def load_guard():
    spec = importlib.util.spec_from_file_location(
        "p24_live_guard", Path(__file__).resolve().parent / "p24_live_guard.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    g = load_guard()
    pins = g.PINNED_EXCEPTIONS
    failures = []

    def case(name, rows, pinned, boot, want_ok):
        got_ok, reasons, _ = g.account(rows, copy.deepcopy(pinned), boot)
        status = "ok" if got_ok == want_ok else "FAIL"
        if got_ok != want_ok:
            failures.append(f"{name}: want_ok={want_ok} got_ok={got_ok} "
                            f"reasons={reasons}")
        print(f"[selftest] {status:4} {name}"
              + ("" if got_ok == want_ok else f" — {reasons}"))

    # rows exactly as pinned; boot strictly after every started_at
    boot = "2026-11-13T22:15:00Z"   # restart3-era boot; all pins predate it
    green_rows = [dict(p) for p in pins]
    case("green: 4 pinned rows pre-boot", green_rows, pins, boot, True)

    case("unknown extra row blocks",
         green_rows + [dict(green_rows[0], id="00000000-0000-0000-0000-000000000000",
                            started_at="2026-11-13 19:00:00+00")],
         pins, boot, False)

    missing = [dict(p) for p in pins if not p["id"].startswith("74d62557")]
    case("pinned row missing blocks", missing, pins, boot, False)

    drifted = [dict(p) for p in pins]
    drifted[0]["status"] = "pending"
    case("single-field drift blocks", drifted, pins, boot, False)

    null_started = [dict(p) for p in pins]
    null_started[2]["started_at"] = ""     # NULL rendered empty
    case("NULL started_at on pinned row blocks", null_started, pins, boot, False)

    case("duplicate row id blocks", green_rows + [dict(green_rows[1])],
         pins, boot, False)

    late_boot = "2026-11-13T21:09:27Z"     # BEFORE the two 528455c2 starts
    case("started_at not before boot blocks (real-datetime compare)",
         green_rows, pins, late_boot, False)

    org_drift = [dict(p) for p in pins]
    org_drift[3]["run_org_id"] = "11111111-1111-1111-1111-111111111111"
    case("org triple divergence blocks", org_drift, pins, boot, False)

    lease_renewed = [dict(p) for p in pins]
    lease_renewed[0]["lease_expires_at"] = "2026-11-13 23:00:00+00"
    case("lease-expiry movement blocks (no age/expiry forgiveness)",
         lease_renewed, pins, boot, False)

    # the P23 false-pass trap: raw-string compare would rank every
    # '2026-11-13 22:…+00' BEFORE '2026-11-13T21:09:27Z' (' ' < 'T');
    # _parse_ts must compare real datetimes and block instead.
    trap = g._parse_ts("2026-11-13 22:02:50.746532+00")
    ref = g._parse_ts("2026-11-13T21:09:27Z")
    ok_trap = trap > ref
    print(f"[selftest] {'ok' if ok_trap else 'FAIL':4} "
          "psql-format later-than-RFC3339 parses as later")
    if not ok_trap:
        failures.append("_parse_ts raw-string trap regressed")

    # pins sanity: 4 rows, 27 fields each, org triple consistent
    ok_sane = (len(pins) == 4 and all(len(p) == 27 for p in pins)
               and all(p["conn_org_id"] == p["def_org_id"] == p["run_org_id"]
                       == p["org_id"] for p in pins))
    print(f"[selftest] {'ok' if ok_sane else 'FAIL':4} "
          "pins: 4 rows x 27 fields, org triple consistent")
    if not ok_sane:
        failures.append("pin structure sanity failed")

    # root round-2: strict compose-ps adjudication + inspect parsing
    def expect(name, cond):
        print(f"[selftest] {'ok' if cond else 'FAIL':4} {name}")
        if not cond:
            failures.append(name)

    cid, e = g.pick_container("abc123\n", 0)
    expect("pick_container: rc0 single cid ok", cid == "abc123" and e is None)
    expect("pick_container: ps rc!=0 blocks",
           g.pick_container("abc", 1) == (None, "compose ps rc=1"))
    cid0, e0 = g.pick_container("\n", 0)
    expect("pick_container: empty stdout blocks",
          cid0 is None and "empty stdout" in e0)
    cidm, em = g.pick_container("aaa\nbbb\n", 0)
    expect("pick_container: 2 cids block (no blind [0])",
           cidm is None and "ambiguous" in em)
    boot, img, ei = g.parse_inspect_boot(
        "2026-11-13T22:05:01.09Z|sha256:cafe", 0)
    expect("parse_inspect_boot: valid ok",
           boot == "2026-11-13T22:05:01.09Z" and img == "sha256:cafe"
           and ei is None)
    b1, i1, e1 = g.parse_inspect_boot("", 0)
    expect("parse_inspect_boot: empty stdout friendly-blocks",
           b1 is None and "empty stdout" in e1)
    b2, i2, e2 = g.parse_inspect_boot("nope|sha256:cafe", 0)
    expect("parse_inspect_boot: non-RFC3339 blocks",
           b2 is None and "RFC3339" in e2)
    b3, i3, e3 = g.parse_inspect_boot("x", 1)
    expect("parse_inspect_boot: inspect rc!=0 blocks", b3 is None)

    if failures:
        for f in failures:
            print(f"[selftest] FAIL: {f}", file=sys.stderr)
        return 1
    print("[selftest] all cases behaved as pinned")
    return 0


if __name__ == "__main__":
    sys.exit(main())
