#!/usr/bin/env python3
"""P24-owned COHORT HANDOFF GATE (formal-execution-contract.md §3).

DRAFT — a dedicated known-quiescent-cohort exception used ONLY in the
planned late-P21ABC sequence, SEPARATE from p24_live_guard.py: the
fixed P24 pins stay EXACTLY FOUR and are never modified by this file.

What root approves (before gate execution): an immutable cohort record
JSON produced from the ACTUAL post-ABC raw cohort (2 deliberate
controlled unknowns B/C). Root reviews the real IDs/full raws and pins
the record's sha256; the gate refuses any record whose bytes hash
differently. No auto-discovery, no age/expiry exemption, no automatic
pin expansion, no writes.

Gate check (read-only, immediately before ONE planned base3 restart):
  * ALL controlled pending/running rows (including started_at NULL)
    must be EXACTLY: fixed old four (p24_pins, every field unchanged)
    + approved cohort new two (every record field unchanged).
  * unknown / missing / duplicate / any drift BLOCKS.
  * boot rule: the old four must predate the current connector boot
    (real-datetime, strict); the new two are a KNOWN post-boot cohort —
    that is precisely the reviewed exception — and their started_at
    values must match the record exactly.

Record schema (root-approved file, e.g. cohort-approved.json):
  {"cohort_date": "...", "abc_suffix": "...",
   "rows": [ {same 27-field safe projection as p24_pins rows}, ...2 ],
   "evidence": {"b_provenance": "strong parent chain ...",
                "c_provenance": "logical_path UUID + COMMITTED ..."}}

Usage:
  python3 -I cohort_gate_p24.py --record <cohort-approved.json> \
      --record-sha256 <sha> [--env-dir <tmp>] [--service connector]
Exit 0 iff the exact approved cohort + fixed old four are unchanged.
"""

import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = "openfoundry-platform-p1-q3v9"
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


GUARD = load_module(HERE / "p24_live_guard.py", "p24_guard")   # ROW_SQL reuse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--record", required=True)
    ap.add_argument("--record-sha256", required=True)
    ap.add_argument("--env-dir",
                    default="/private/tmp/openfoundry-platform-p1-q3v9")
    ap.add_argument("--service", default="connector")
    args = ap.parse_args()

    record_path = Path(args.record)
    if not record_path.is_file():
        print(f"[cohort-gate] BLOCK: record missing {record_path}",
              file=sys.stderr)
        return 1
    got_sha = hashlib.sha256(record_path.read_bytes()).hexdigest()
    if got_sha != args.record_sha256:
        print(f"[cohort-gate] BLOCK: record sha256 {got_sha} != approved "
              f"{args.record_sha256} — refusing to run against an "
              "unapproved/modified cohort record", file=sys.stderr)
        return 1
    record = json.loads(record_path.read_text())
    cohort_rows = record.get("rows") or []
    if len(cohort_rows) != 2:
        print(f"[cohort-gate] BLOCK: cohort record must carry exactly 2 "
              f"rows, got {len(cohort_rows)}", file=sys.stderr)
        return 1

    pins = GUARD.PINNED_EXCEPTIONS            # fixed four, NEVER expanded
    expected = {p["id"]: p for p in pins}
    for row in cohort_rows:
        if row.get("id") in expected:
            print("[cohort-gate] BLOCK: cohort row collides with fixed "
                  "pin id", file=sys.stderr)
            return 1
        expected[row["id"]] = row

    env_dir = Path(args.env_dir)
    files = ["-f", str(env_dir / "compose.yaml")]
    override = env_dir / "compose-p22-connector2.yaml"
    if override.exists():
        files += ["-f", str(override)]
    ps = subprocess.run(["docker", "compose", *files,
                         "--project-name", PROJECT, "ps", "-q",
                         args.service], capture_output=True, text=True,
                        timeout=120)
    cid, err = GUARD.pick_container(ps.stdout, ps.returncode)
    if err:
        print(f"[cohort-gate] BLOCK: {err}", file=sys.stderr)
        return 1
    ins = subprocess.run(["docker", "inspect", "--format",
                          "{{.State.StartedAt}}|{{.Image}}", cid],
                         capture_output=True, text=True, timeout=60)
    boot, _img, err = GUARD.parse_inspect_boot(ins.stdout, ins.returncode)
    if err:
        print(f"[cohort-gate] BLOCK: {err}", file=sys.stderr)
        return 1
    print(f"[cohort-gate] {args.service} boot={boot}")

    psql = subprocess.run(["docker", "compose", *files,
                           "--project-name", PROJECT, "exec", "-T",
                           "postgres", "psql", "-U", "openfoundry", "-d",
                           "connectors", "-t", "-A", "-v",
                           "ON_ERROR_STOP=1", "-c", GUARD.SESSION_SQL,
                           "-c", GUARD.ROW_SQL],
                          capture_output=True, text=True, timeout=120)
    if psql.returncode != 0:
        print(f"[cohort-gate] BLOCK: projection rc={psql.returncode}",
              file=sys.stderr)
        return 1
    try:
        rows = json.loads(
            psql.stdout.strip().splitlines()[-1] or "null") or []
    except ValueError:
        print("[cohort-gate] BLOCK: projection not JSON", file=sys.stderr)
        return 1
    print(f"[cohort-gate] controlled pending/running rows: {len(rows)}")

    cohort_ids = {r["id"] for r in cohort_rows}
    reasons = []
    rows_by_id = {}
    for row in rows:
        rid = row.get("id")
        if rid in rows_by_id:
            reasons.append(f"duplicate row id {rid}")
            continue
        rows_by_id[rid] = row
    for rid in sorted(rows_by_id):
        if rid not in expected:
            reasons.append(f"UNACCOUNTED controlled row {rid} — not "
                           "fixed-four, not approved cohort")
    for rid, want in sorted(expected.items()):
        row = rows_by_id.get(rid)
        if row is None:
            reasons.append(f"expected row {rid} MISSING (deleted or "
                           "terminalized — must never be touched)")
            continue
        for key, value in sorted(want.items()):
            if row.get(key) != value:
                reasons.append(f"row {rid} DRIFT on {key}: "
                               f"got={row.get(key)!r} want={value!r}")
        started = row.get("started_at") or ""
        if rid in cohort_ids:
            continue          # reviewed known post-boot exception
        if not started:
            reasons.append(f"fixed row {rid} started_at NULL")
        else:
            try:
                if not (GUARD._parse_ts(started)
                        < GUARD._parse_ts(boot)):
                    reasons.append(f"fixed row {rid} started_at {started} "
                                   f"NOT before boot {boot}")
            except ValueError as exc:
                reasons.append(f"fixed row {rid} started_at unparseable: "
                               f"{exc}")
    if reasons:
        for r in reasons:
            print(f"[cohort-gate] BLOCK: {r}", file=sys.stderr)
        return 1
    print("[cohort-gate] ok: exact approved cohort (2 known post-boot, "
          f"root-reviewed record {got_sha[:12]}…) + fixed four "
          "undrifted/pre-boot "
          f"({len(rows_by_id)}/6)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
