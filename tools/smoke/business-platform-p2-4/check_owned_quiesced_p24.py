#!/usr/bin/env python3
"""P24 owned-quiesce gate (immediately BEFORE aux-down and BEFORE the
final restart — root: both must be preceded by strict guard4 + owned
terminal/disabledNULL).

Read-only: guard4 runs separately; THIS checks the owned set by FRESH
SQL: every owned def disabled + NULL cursor + no grant row + every run
terminal. Exit 0 iff all hold.

Usage: python3 -I check_owned_quiesced_p24.py \
         --p24-result <result_p24-*.json> [--env-dir <tmp>]
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

PROJECT = "openfoundry-platform-p1-q3v9"
TERMINAL = ("succeeded", "failed", "aborted")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p24-result", required=True)
    ap.add_argument("--owned-history",
                    help="p24-owned-history-v1 manifest; when given, the "
                         "expected owned set is its UNION (failed "
                         "attempts included) instead of only the p24 "
                         "result's owned ids")
    ap.add_argument("--env-dir",
                    default="/private/tmp/openfoundry-platform-p1-q3v9")
    args = ap.parse_args()
    # root fix: argparse hands env_dir as str; normalize ONCE here —
    # `str / str` below would TypeError (and the RuntimeError handler
    # would not catch it)
    env_dir = Path(args.env_dir)
    owned = sorted(json.loads(
        Path(args.p24_result).read_text()).get("owned_sync_ids") or {})
    if args.owned_history:
        hist = json.loads(Path(args.owned_history).read_text())
        if hist.get("schema") != "p24-owned-history-v1":
            print("[quiesce] BLOCK: bad history schema", file=sys.stderr)
            return 1
        if not hist.get("all_attempts_valid"):
            print("[quiesce] BLOCK: history not all-valid", file=sys.stderr)
            return 1
        owned = sorted(hist.get("owned_sync_ids") or {})
    if not owned:
        print("[quiesce] BLOCK: no owned ids (result/history)",
              file=sys.stderr)
        return 1
    in_ids = ", ".join(f"'{i}'::uuid" for i in owned)

    def psql(sql):
        proc = subprocess.run(
            ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
             "--project-name", PROJECT, "exec", "-T", "postgres", "psql",
             "-U", "openfoundry", "-d", "connectors", "-t", "-A", "-v",
             "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True, text=True, timeout=90)
        if proc.returncode != 0:
            raise RuntimeError(f"psql rc={proc.returncode}")
        return [l for l in proc.stdout.strip().splitlines() if l]

    bad = []
    runs = []
    try:
        rows = psql(
            "SELECT d.id::text || '|' || (d.schedule_enabled)::text || '|' "
            "|| (d.schedule_next_fire_at IS NULL)::text FROM"
            f" batch_sync_defs d WHERE d.id IN ({in_ids}) ORDER BY d.id;")
        if len(rows) != len(owned):
            bad.append(f"owned defs count {len(rows)} != {len(owned)}")
        for ln in rows:
            i, en, nul = ln.split("|")
            if not (en == "false" and nul == "true"):
                bad.append(f"{i[:8]}: enabled={en} cursor_null={nul}")
        grants = psql("SELECT g.sync_def_id::text FROM"
                      " sync_schedule_grants g WHERE g.sync_def_id IN"
                      f" ({in_ids}) ORDER BY 1;")
        if grants:
            bad.append(f"grant rows remain: {grants}")
        runs = psql(
            "SELECT r.id::text || '|' || r.status FROM sync_runs r WHERE"
            f" r.sync_def_id IN ({in_ids}) ORDER BY r.id;")
        unres = [ln for ln in runs if ln.split("|")[1] not in TERMINAL]
        if unres:
            bad.append(f"non-terminal runs: {unres[:5]}")
    except RuntimeError as exc:
        print(f"[quiesce] BLOCK: {exc}", file=sys.stderr)
        return 1
    if bad:
        for b in bad:
            print(f"[quiesce] BLOCK: {b}", file=sys.stderr)
        return 1
    print(f"[quiesce] ok: {len(owned)} owned defs disabled/NULL, grants "
          f"none, all runs terminal ({len(runs)} runs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
