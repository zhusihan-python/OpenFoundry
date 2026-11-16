#!/usr/bin/env python3
"""Local (no-stack, no-docker) selftest of p23_live_guard.account —
the fixed-exception policy core. Synthetic rows only.

Covers the root-pinned semantics:
  * exactly-two fixed rows with exact projections pass (and only them)
  * an unknown row — old, new, or started_at NULL — BLOCKS
  * a pinned row that drifted (status/heartbeat/lease/binding) BLOCKS
  * a pinned row that disappeared (terminalized/deleted) BLOCKS
  * a pinned row started AT/AFTER the container boot BLOCKS
  * cross-format timestamps parse (psql '+00' text vs RFC3339 'Z')
Run: python3 -I selftest_live_guard.py
"""

import copy
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "p23live", HERE / "p23_live_guard.py")
G = importlib.util.module_from_spec(spec)
spec.loader.exec_module(G)

PASS = 0
FAIL = 0


def check(label, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"ok   {label}")
    else:
        FAIL += 1
        print(f"FAIL {label} got={got!r} want={want!r}")


BOOT = "2026-11-13T19:22:48.735540501Z"


def rows_current():
    """The two pinned rows EXACTLY as the live projection returns them."""
    return [copy.deepcopy(p) for p in G.PINNED_EXCEPTIONS]


ok, reasons, _ = G.account(rows_current(), G.PINNED_EXCEPTIONS, BOOT)
check("exact-two-undrifted-preboot-pass", ok, True)

# unknown OLD row blocks (no blanket started_at<boot forgiveness)
rows = rows_current() + [{"id": "11111111-2222-3333-4444-555555555555",
                          "status": "running", "started_at":
                          "2026-11-13 19:20:00+00"}]
ok, reasons, _ = G.account(rows, G.PINNED_EXCEPTIONS, BOOT)
check("unknown-old-row-blocks", ok, False)
check("unknown-old-row-reason-names-it",
      any("UNACCOUNTED" in r for r in reasons), True)

# unknown NULL-started row blocks
rows = rows_current() + [{"id": "11111111-2222-3333-4444-555555555555",
                          "status": "pending", "started_at": ""}]
ok, reasons, _ = G.account(rows, G.PINNED_EXCEPTIONS, BOOT)
check("null-started-row-blocks", ok, False)

# pinned drift: status rewritten / heartbeat advanced / lease moved
for field, value in (("status", "aborted"),
                     ("heartbeat_at", "2026-11-14 09:00:00+00"),
                     ("lease_expires_at", "2030-01-01 00:00:00+00"),
                     ("source_name", "someone-else")):
    rows = rows_current()
    rows[0][field] = value
    ok, reasons, _ = G.account(rows, G.PINNED_EXCEPTIONS, BOOT)
    check(f"drift-{field}-blocks", ok, False)
    check(f"drift-{field}-reason-cites-drift",
          any("DRIFT" in r for r in reasons), True)

# pinned row missing (terminalized/deleted) blocks
rows = rows_current()[1:]
ok, reasons, _ = G.account(rows, G.PINNED_EXCEPTIONS, BOOT)
check("pinned-missing-blocks", ok, False)
check("pinned-missing-reason-cites-missing",
      any("MISSING" in r for r in reasons), True)

# pinned row started AT/AFTER the boot blocks (process maybe alive)
rows = rows_current()
rows[1]["started_at"] = "2026-11-13 19:23:00+00"
ok, reasons, _ = G.account(rows, G.PINNED_EXCEPTIONS, BOOT)
check("pinned-postboot-blocks", ok, False)

# cross-format parse: psql text vs RFC3339
check("parse-psql-text", str(G._parse_ts("2026-11-13 19:22:06.812306+00")),
      "2026-11-13 19:22:06.812306+00:00")
check("parse-rfc3339", str(G._parse_ts("2026-11-13T19:22:48.735Z")),
      "2026-11-13 19:22:48.735000+00:00")
check("preboot-crossformat-compares-true",
      G._parse_ts("2026-11-13 19:22:06+00")
      < G._parse_ts("2026-11-13T19:22:48Z"), True)

# empty rows (nothing live at all) is a CLEAN pass
ok, reasons, _ = G.account([], G.PINNED_EXCEPTIONS, BOOT)
check("no-rows-but-pinned-missing-still-blocks", ok, False)

print(f"\n[p23-live-guard-selftest] pass={PASS} fail={FAIL}")
sys.exit(1 if FAIL else 0)
