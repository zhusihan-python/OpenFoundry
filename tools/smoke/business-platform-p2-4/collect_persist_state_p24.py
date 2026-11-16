#!/usr/bin/env python3
"""P24 OWNED persistent-state snapshot (pre/post final restart).

Read-only SQL over THIS round's owned Sync IDs (from the p24 result):
per owned def — schedule config/revision/cursor/last-fire/last-tick,
its tick-ID set, its run-ID set, and grant absence. The driver
snapshots BEFORE the final restart and again AFTER; the finalizer
compares them EQUAL (persistence proven by data, never by legacy
artifact reread or cleanup booleans). No secret columns.

Usage: python3 -I collect_persist_state_p24.py \
         --p24-result <result_p24-*.json> --out-dir <fresh dir> \
         [--owned-history <p24-owned-history-v1.json>] [--env-dir <tmp>]

--owned-history (optional, retry-evidence support): the Tools-generated
exact global owned roster (schema=p24-owned-history-v1; attempts=[{result_
path, result_sha256, suffix}]; owned_sync_ids=SAFE-binding exact union
INCLUDING failed attempts). Validated before any runtime work: every
input's actual sha256/suffix/bindings re-checked, union rebuilt == the
declared one, latest success present. The union drives the snapshot's
owned set and hard binding checks; the manifest keeps p24_result_sha256
and adds owned_history_sha256.

Exit 0 iff every query ran (content equality is the finalizer's job).
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT = "openfoundry-platform-p1-q3v9"


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def load_owned_history(hist_path, latest_result_path):
    """Validate a p24-owned-history-v1 roster (same contract as
    collect_postfacts_p24.load_owned_history); returns (union, sha, errs).
    Duplicated inline so this collector stays a standalone -I script."""
    errs = []
    try:
        hist = json.loads(Path(hist_path).read_text())
    except Exception as exc:  # noqa: BLE001
        return None, None, [f"history unreadable: {exc}"]
    if hist.get("schema") != "p24-owned-history-v1":
        errs.append("schema != p24-owned-history-v1")
    attempts = hist.get("attempts")
    if not (isinstance(attempts, list) and attempts):
        errs.append("attempts missing/empty")
        attempts = []
    latest_abs = Path(latest_result_path).resolve()
    latest_sha = sha(latest_result_path)
    latest_in = False
    union = {}
    for i, a in enumerate(attempts):
        rp = (a or {}).get("result_path")
        if not (isinstance(rp, str) and os.path.isabs(rp)):
            errs.append(f"attempts[{i}].result_path not absolute")
            continue
        p = Path(rp)
        if not p.is_file():
            errs.append(f"attempts[{i}] result missing: {rp}")
            continue
        try:
            actual = sha(p)
        except OSError as exc:
            errs.append(f"attempts[{i}] unreadable: {exc}")
            continue
        if actual != (a or {}).get("result_sha256"):
            errs.append(f"attempts[{i}] sha mismatch")
            continue
        try:
            res = json.loads(p.read_text())
        except ValueError:
            errs.append(f"attempts[{i}] result not JSON")
            continue
        if res.get("suffix") != (a or {}).get("suffix"):
            errs.append(f"attempts[{i}] suffix mismatch")
        ids = res.get("owned_sync_ids") or {}
        if not isinstance(ids, dict):
            errs.append(f"attempts[{i}] owned_sync_ids not a map")
            continue
        for k, v in ids.items():
            if not (isinstance(v, dict)
                    and isinstance(v.get("source_id"), str) and v["source_id"]
                    and isinstance(v.get("target_id"), str) and v["target_id"]):
                errs.append(f"attempts[{i}] binding {k[:8]} not safe")
                continue
            if k in union and union[k] != v:
                errs.append(f"binding {k[:8]} conflicts across attempts")
            union[k] = v
        if p.resolve() == latest_abs and actual == latest_sha:
            latest_in = True
    if not latest_in:
        errs.append("latest success (--p24-result) not among attempts")
    declared = hist.get("owned_sync_ids")
    if not (isinstance(declared, dict) and declared):
        errs.append("declared owned_sync_ids missing/empty")
    elif declared != union:
        errs.append(f"declared union != rebuilt ({len(declared)} vs "
                    f"{len(union)})")
    if errs:
        return None, None, errs
    return union, sha(hist_path), []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p24-result", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--owned-history", default=None)
    ap.add_argument("--env-dir",
                    default="/private/tmp/openfoundry-platform-p1-q3v9")
    args = ap.parse_args()
    env_dir = Path(args.env_dir)
    out = Path(args.out_dir)
    if out.exists() and any(out.iterdir()):
        print(f"[persist] BLOCK: out-dir not fresh: {out}", file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)

    p24 = json.loads(Path(args.p24_result).read_text())
    owned_map = p24.get("owned_sync_ids") or {}
    # Optional exact global roster (retry-evidence): validated BEFORE any
    # runtime work; the union (failed attempts included) drives the owned
    # set and the hard binding checks below.
    hist_sha = None
    if args.owned_history:
        union, hist_sha, h_errs = load_owned_history(
            args.owned_history, args.p24_result)
        if h_errs:
            print("[persist] BLOCK: owned-history invalid: "
                  + "; ".join(h_errs[:5]), file=sys.stderr)
            return 1
        owned_map = union
    owned = sorted(owned_map)
    if not owned:
        print("[persist] BLOCK: p24 result has no owned ids",
              file=sys.stderr)
        return 1
    in_ids = ", ".join(f"'{i}'::uuid" for i in owned)

    def psql(sql):
        return subprocess.run(
            ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
             "--project-name", PROJECT, "exec", "-T", "postgres", "psql",
             "-U", "openfoundry", "-d", "connectors", "-t", "-A", "-v",
             "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True, text=True, timeout=90)

    state, files, rcs = {}, {}, {}

    def save(name, proc, rows):
        (out / f"{name}.txt").write_text(proc.stdout)
        (out / f"{name}.txt.err").write_text(proc.stderr)
        (out / f"{name}.txt.rc").write_text(f"{proc.returncode}\n")
        if rows is not None:
            (out / f"{name}.json").write_text(
                json.dumps(rows, indent=1, sort_keys=True) + "\n")
        rcs[name] = proc.returncode
        # Original .rc/.err sidecars are hashed alongside the raw and
        # derived files (review item4: the finalizer independently
        # revalidates raw/derived/original rc/hash — empty==empty can
        # never green this manifest).
        for ext in ("txt", "txt.err", "txt.rc", "json"):
            p = out / f"{name}.{ext}"
            if p.exists():
                files[f"{name}.{ext}"] = {"sha256": sha(p)}
        return rows

    def rows_of(proc, required_last_json=True):
        if proc.returncode != 0:
            return None
        lines = [l for l in proc.stdout.strip().splitlines() if l]
        try:
            v = json.loads(lines[-1]) if lines else []
            return [] if v is None else v
        except ValueError:
            return None

    q_defs = ("SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text,"
              " d.source_id::text, d.output_dataset_id::text,"
              " COALESCE(d.schedule_cron,'') cron,"
              " COALESCE(d.schedule_time_zone,'') tz,"
              " (d.schedule_enabled)::text enabled,"
              " d.schedule_revision,"
              " (d.schedule_next_fire_at IS NULL)::text cursor_null,"
              " COALESCE(d.schedule_last_fire_at::text,'') last_fire,"
              " COALESCE(d.schedule_last_tick_at::text,'') last_tick,"
              " COALESCE(d.schedule_last_tick_outcome,'') last_outcome"
              f" FROM batch_sync_defs d WHERE d.id IN ({in_ids})"
              " ORDER BY d.id) t;")
    q_ticks = ("SELECT json_agg(row_to_json(t)) FROM (SELECT"
               " k.sync_def_id::text, k.id::text tick_id"
               " FROM sync_schedule_ticks k WHERE k.sync_def_id IN"
               f" ({in_ids}) ORDER BY k.sync_def_id, k.created_at,"
               " k.id) t;")
    q_runs = ("SELECT json_agg(row_to_json(t)) FROM (SELECT"
              " r.sync_def_id::text, r.id::text run_id, r.status"
              " FROM sync_runs r WHERE r.sync_def_id IN"
              f" ({in_ids}) ORDER BY r.sync_def_id, r.id) t;")
    q_grants = ("SELECT json_agg(row_to_json(t)) FROM (SELECT"
                " g.sync_def_id::text FROM sync_schedule_grants g"
                f" WHERE g.sync_def_id IN ({in_ids}) ORDER BY 1) t;")

    bad = []
    for name, sql in (("defs", q_defs), ("ticks", q_ticks),
                      ("runs", q_runs), ("grants", q_grants)):
        proc = psql(sql)
        rows = rows_of(proc)
        if rows is None:
            bad.append(name)
        save(name, proc, rows)
        state[name] = rows

    # HARD structural checks (review item4): a NULL/empty projection is []
    # in psql, and bare pre==post state equality over two empties would be
    # a false green. The collector itself must refuse to bless that: exact
    # non-empty owned defs with the expected Source/target bindings, all
    # disabled with NULL cursor, ZERO grant rows, and a run structure whose
    # every run is terminal. Persistence is proven by DATA (finalizer
    # compares pre==post STATE), never by legacy artifact reread alone.
    hard = []
    TERMINAL = ("succeeded", "failed", "aborted")
    if state["defs"] is not None:
        got_ids = sorted(r["id"] for r in state["defs"])
        if not owned or got_ids != owned:
            hard.append(f"defs set {got_ids} != owned {owned}")
        for r in state["defs"] or []:
            b = owned_map.get(r["id"]) or {}
            if not (r.get("source_id") == b.get("source_id")
                    and r.get("output_dataset_id") == b.get("target_id")):
                hard.append(f"{r['id'][:8]}: source/target != binding")
            if not (r.get("enabled") == "false"
                    and r.get("cursor_null") == "true"):
                hard.append(f"{r['id'][:8]}: not disabled/NULL-cursor")
    if state["grants"] is not None and state["grants"] != []:
        hard.append(f"grant rows remain: {state['grants']}")
    if state["runs"] is not None:
        # Structure-only over the runs that ACTUALLY exist: every run's
        # parent must be inside the exact owned set, and every existing run
        # must be terminal. NO must-have-run requirement — legitimately
        # runless owned defs exist (G3/G6 isolation/disabled fixtures and
        # first-round failed defs); nothing here may invent runs or delete
        # defs to pass. The non-empty hard gates live on defs/owned above.
        runs_by_def = {}
        for r in state["runs"] or []:
            runs_by_def.setdefault(r["sync_def_id"], []).append(r)
        for sid, rs in sorted(runs_by_def.items()):
            if sid not in owned_map:
                hard.append(f"run parent {sid[:8]} outside exact owned set")
            for r in rs:
                if r.get("status") not in TERMINAL:
                    hard.append(f"{sid[:8]}: run {r['run_id'][:8]} status "
                                f"{r.get('status')!r} not terminal")
    if hard:
        bad.append("hard:" + "; ".join(hard[:4]))

    manifest = {"client_date": time.strftime("%Y-%m-%d"),
                "p24_result_sha256": sha(args.p24_result),
                "owned_history_sha256": hist_sha,
                "owned_ids": owned,
                "files": dict(sorted(files.items())),
                "rc": dict(sorted(rcs.items())),
                "state": state,
                "bad": bad,
                "hard_checks": hard,
                "terminal_states": list(TERMINAL),
                "note": "read-only owned persistent-state snapshot; no "
                        "secret columns"}
    (out / "persist-manifest.json").write_text(
        json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    print(f"[persist] owned={len(owned)} bad={bad} -> {out}")
    print(f"PERSIST_DIR={out}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
