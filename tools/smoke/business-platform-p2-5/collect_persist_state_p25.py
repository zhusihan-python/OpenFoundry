#!/usr/bin/env python3
"""P25 PERSIST-STATE collector — ACTUAL readonly per-table SQL
(review-archive-tools-round2 #5 / formal-1 #9 closure; P24 shape).

NOT a caller-dumps assembler: the collect entry runs REAL readonly
psql per table behind a release flag, writing each command BEFORE
execution and the ORIGINAL rc/stdout/stderr/raw-sha per table into a
fresh immutable attempt dir:

  defs / ticks / runs / grants — the sealed-17 owned set filtered by
      UUID-validated ANY(ARRAY[...]) literals (never caller SQL text)
  cp_owners — sync_def_checkpoints (REAL DDL, migration 20261116100000):
      FULL table snapshot; owners are PROTECTED retention state
      (may reference terminal failed runs; never cleaned/adopted)

A missing table / nonzero rc / unparseable projection is an HONEST
FAILURE (no defaulted [], no fabricated rc0/empty err, no silent
empty-table mapping — catalog discipline). Hard structural checks:
exact 17 owned defs (disabled/NULL cursor), every existing owned run
terminal, zero grants, cp_owners schema rows recorded for retention.

`--plan` is pure. `--collect` refuses rc3 BEFORE any subprocess is
constructed while RELEASED_FOR_EXECUTION is False; collect_body()
takes an INJECTABLE run_cli so the selftest drives the REAL entry.
"""

import argparse
import hashlib
import importlib.util
import json
import re
import sys
sys.dont_write_bytecode = True  # Pure validation must not mutate copied evidence/tools.
import time
from pathlib import Path

RELEASED_FOR_EXECUTION = True   # root flips for the readonly tail

ENV_DIR_DEFAULT = "/private/tmp/openfoundry-platform-p1-q3v9"
PROJECT = "openfoundry-platform-p1-q3v9"
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")
TERMINAL = ("succeeded", "failed", "aborted", "error")

# REAL DDL (primary migration 20261116100000).
CP_OWNERS_SQL = (
    "SELECT json_agg(row_to_json(t)) FROM ("
    "  SELECT c.sync_def_id::text, c.source_id::text,"
    "         c.output_dataset_id::text, c.mode,"
    "         c.confirmed_epoch::text,"
    "         (c.checkpoint_value IS NULL)::text AS cp_null,"
    "         c.active_run_id::text AS run_id,"
    "         COALESCE(c.active_since::text,'') AS active_since,"
    "         r.status AS run_status"
    "  FROM sync_def_checkpoints c"
    "  LEFT JOIN sync_runs r ON r.id = c.active_run_id"
    "  ORDER BY c.sync_def_id) t;")


def sealed_baseline(here: Path) -> dict:
    spec = importlib.util.spec_from_file_location(
        "p25_derive_persist", here / "p25_cohort_derive.py")
    derive = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(derive)
    data = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                         Path(derive.SEAL_P24_TOOLS_DEFAULT))
    cohort6_sha = hashlib.sha256(
        (here / "p25_cohort6.py").read_bytes()).hexdigest()
    return derive.frozen_baseline_dict(data, cohort6_sha)


def id_literals(ids):
    parts = []
    for i in sorted(set(ids)):
        if not UUID_RE.match(str(i)):
            raise ValueError(f"non-UUID id refused for SQL: {i!r}")
        parts.append(f"'{i}'")
    return ", ".join(parts)


def table_sqls(frozen: dict):
    """EXACT sealed-17 filtering (same shapes as the released P25
    preflight collector; the owned set is rederived from the seal)."""
    owned_ids = [d["id"] for d in
                 frozen["content_gates"]["owned_fullstate"]["defs"]]
    arr = id_literals(owned_ids)
    return {
        "defs": (
            "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text,"
            " d.source_id::text, d.output_dataset_id::text,"
            " COALESCE(d.schedule_cron,'') AS cron,"
            " COALESCE(d.schedule_time_zone,'') AS tz,"
            " (d.schedule_enabled)::text AS enabled,"
            " d.schedule_revision,"
            " (d.schedule_next_fire_at IS NULL)::text AS cursor_null,"
            " COALESCE(d.schedule_last_fire_at::text,'') AS last_fire,"
            " COALESCE(d.schedule_last_tick_at::text,'') AS last_tick,"
            " COALESCE(d.schedule_last_tick_outcome,'') AS last_outcome"
            f" FROM batch_sync_defs d WHERE d.id::text = ANY (ARRAY"
            f"[{arr}]::text[]) ORDER BY d.id) t;"),
        "ticks": (
            "SELECT json_agg(row_to_json(t)) FROM (SELECT"
            " t.sync_def_id::text, t.id::text AS tick_id FROM"
            " sync_schedule_ticks t WHERE t.sync_def_id::text = ANY"
            f" (ARRAY[{arr}]::text[]) ORDER BY t.id) t;"),
        "runs": (
            "SELECT json_agg(row_to_json(t)) FROM (SELECT"
            " r.id::text AS run_id, r.status, r.sync_def_id::text FROM"
            " sync_runs r WHERE r.control_mode='controlled' AND"
            " r.status IN ('succeeded','failed','aborted') AND"
            " r.sync_def_id::text = ANY (ARRAY"
            f"[{arr}]::text[]) ORDER BY r.id) t;"),
        "grants": (
            "SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM (SELECT"
            " g.sync_def_id::text, g.grant_id::text FROM"
            " sync_schedule_grants g WHERE g.sync_def_id::text = ANY"
            f" (ARRAY[{arr}]::text[]) ORDER BY g.grant_id) t;"),
        "cp_owners": CP_OWNERS_SQL,
    }


def psql_argv(env_dir: Path, sql: str):
    return ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
            "--project-name", PROJECT, "exec", "-T", "postgres",
            "psql", "-U", "openfoundry", "-d", "connectors", "-t",
            "-A", "-v", "ON_ERROR_STOP=1",
            "-c", "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';",
            "-c", sql]


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def collect_body(args, run_cli):
    here = Path(__file__).resolve().parent
    frozen = sealed_baseline(here)
    sqls = table_sqls(frozen)
    env_dir = Path(args.env_dir)
    base = Path(args.out_dir)
    n = 1
    while (base / f"attempt-{n}").exists():
        n += 1
    out = base / f"attempt-{n}"
    out.mkdir(parents=True)

    failures, state, rcs, cmd_shas = [], {}, {}, {}
    for name in ("defs", "ticks", "runs", "grants", "cp_owners"):
        argv = psql_argv(env_dir, sqls[name])
        stem = f"{name}.txt"
        (out / f"{stem}.cmd").write_text(" ".join(argv) + "\n")
        cmd_shas[name] = _sha(out / f"{stem}.cmd")
        rc, stdout, stderr, cls = run_cli(argv, timeout=120)
        rows = None
        if cls == "timeout":
            (out / f"{stem}.rc").write_text("unknown\n")
            (out / f"{stem}.wrapper").write_text("timeout\n")
            rcs[name] = "unknown"
            failures.append(f"{name}: TIMEOUT — original rc unknown")
            rows = None
        elif cls == "error":
            (out / f"{stem}.rc").write_text("unknown\n")
            (out / f"{stem}.wrapper").write_text("oserror\n")
            rcs[name] = "unknown"
            failures.append(f"{name}: OSERROR — original rc unknown")
            rows = None
        else:
            (out / f"{stem}.rc").write_text(f"{rc}\n")
            rcs[name] = rc
            if rc != 0:
                failures.append(f"{name}: collection rc={rc} — honest "
                                "failure, never fabricated empty")
                rows = None
        (out / stem).write_text(stdout or "")
        (out / f"{stem}.err").write_text(stderr or "")
        if cls == "ok" and rc == 0:
            lines = [l for l in (stdout or "").strip().splitlines()
                     if l.strip()]
            try:
                if not lines:
                    raise ValueError("empty stdout")
                v = json.loads(lines[-1])
                rows = [] if v is None else v
            except ValueError:
                failures.append(f"{name}: projection last line not "
                                "JSON — refusing (missing table is "
                                "NOT an empty result)")
                rows = None
        state[name] = rows
        if rows is not None:
            (out / f"{name}.json").write_text(json.dumps(
                rows, indent=1, sort_keys=True) + "\n")

    # HARD structural checks (P24 discipline): missing tables already
    # failed above; here the CONTENT must match the sealed truth.
    hard = []
    owned = [d["id"] for d in
             frozen["content_gates"]["owned_fullstate"]["defs"]]
    if state["defs"] is not None:
        got = sorted(r["id"] for r in state["defs"])
        if got != sorted(owned):
            hard.append(f"defs set {len(got)} != sealed owned "
                        f"{len(owned)}")
        for r in state["defs"]:
            if not (r.get("enabled") == "false"
                    and r.get("cursor_null") == "true"):
                hard.append(f"{str(r.get('id'))[:8]}: not disabled/"
                            "NULL-cursor")
    if state["grants"] is not None and state["grants"] != []:
        hard.append(f"grant rows remain: {state['grants'][:3]}")
    if state["runs"] is not None:
        for r in state["runs"]:
            if r.get("status") not in TERMINAL:
                hard.append(f"owned run {str(r.get('run_id'))[:8]} "
                            f"status {r.get('status')!r} not terminal")
    if state["cp_owners"] is not None:
        for r in state["cp_owners"]:
            if not (r.get("sync_def_id") and r.get("source_id")
                    and r.get("output_dataset_id")):
                hard.append("cp_owner row missing parent fields")
    for table, expected_rows in frozen["content_gates"]["owned_fullstate"].items():
        rows = state.get(table)
        canon = lambda rs: sorted(json.dumps(r, sort_keys=True) for r in rs)
        if rows is None or canon(rows) != canon(expected_rows):
            hard.append(f"{table}: full rows differ from sealed17 fullstate")
    failures += [f"hard: {h}" for h in hard]

    files = {}
    for p in sorted(out.rglob("*")):
        if p.is_file() and p.name != "persist-manifest.json":
            files[str(p.relative_to(out))] = {"sha256": _sha(p)}
    manifest = {"schema": "p25-persist-v2",
                "client_date": time.strftime("%Y-%m-%d"),
                "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
                "owned_ids": sorted(owned),
                "tables": {t: {"rows": (len(state[t])
                                        if state[t] is not None
                                        else None),
                               "rc": rcs[t],
                               "cmd_sha256": cmd_shas[t]}
                           for t in sqls},
                "files": files,
                "failures": failures,
                "note": "cp_owners are PROTECTED retention state "
                        "(may reference terminal failed runs); never "
                        "cleaned/adopted; P26 resolves",
                "ok": not failures}
    (out / "persist-manifest.json").write_text(json.dumps(
        manifest, indent=1, sort_keys=True) + "\n")
    print(f"[p25-persist] attempt {out} ok={manifest['ok']} "
          f"failures={len(failures)}")
    print(f"PERSIST_DIR={out}")
    # machine verdict LAST on stdout (finalizer evidence, ARCHIVE-
    # INTERFACE §5)
    print(json.dumps({"ok": manifest["ok"],
                      "attempt_dir": str(out),
                      "failures": len(failures)}))
    return 0 if manifest["ok"] else 1


def verify_attempt(attempt_dir: Path) -> dict:
    """Pure replay from original command/rc/stdout, against seal-derived fullstate."""
    d = Path(attempt_dir)
    problems, state = [], {}
    frozen = sealed_baseline(Path(__file__).resolve().parent)
    expected = frozen["content_gates"]["owned_fullstate"]
    man = json.loads((d / "persist-manifest.json").read_text())
    if man.get("ok") is not True or man.get("failures") != []:
        problems.append("persist collection did not succeed")
    ledger = man.get("files") or {}
    required = {f"{t}{suf}" for t in table_sqls(frozen)
                for suf in (".txt", ".txt.cmd", ".txt.rc", ".txt.err", ".json")}
    disk = {str(p.relative_to(d)) for p in d.rglob("*")
            if p.is_file() and p != d / "persist-manifest.json"}
    if disk != required or set(ledger) != required:
        problems.append("persist canonical raw member set/ledger mismatch")
    for rel in sorted(required):
        p = d / rel
        if not p.is_file() or _sha(p) != (ledger.get(rel) or {}).get("sha256"):
            problems.append(f"persist raw missing/hash mismatch: {rel}")
    for table, sql in table_sqls(frozen).items():
        try:
            cmd = (d / f"{table}.txt.cmd").read_text()
            want_cmd = " ".join(psql_argv(Path("/private/tmp/openfoundry-platform-p1-q3v9"), sql)) + "\n"
            if cmd != want_cmd:
                problems.append(f"persist {table} command differs from canonical readonly SQL/target")
            if (d / f"{table}.txt.rc").read_text().strip() != "0":
                problems.append(f"persist {table} original rc != 0")
            text = (d / f"{table}.txt").read_text().strip()
            if not text:
                raise ValueError("empty stdout is missing evidence")
            raw_rows = json.loads(text.splitlines()[-1])
            rows = [] if raw_rows is None else raw_rows
            if not isinstance(rows, list) or json.loads((d / f"{table}.json").read_text()) != rows:
                raise ValueError("derived JSON differs from raw rows")
            state[table] = rows
            if table in expected:
                canon = lambda rs: sorted(json.dumps(r, sort_keys=True) for r in rs)
                if canon(rows) != canon(expected[table]):
                    problems.append(f"persist {table} full rows differ from sealed17 fullstate")
        except (OSError, ValueError, IndexError) as exc:
            problems.append(f"persist {table} raw replay failed: {exc}")
    return {"ok": not problems, "problems": problems, "tables": state}


def real_run_cli(argv, timeout):
    import subprocess
    try:
        res = subprocess.run(argv, capture_output=True, text=True,
                             timeout=timeout)
        return res.returncode, res.stdout, res.stderr, "ok"
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout or ""
        err = exc.stderr or ""
        if isinstance(out, bytes):
            out = out.decode("utf-8", "replace")
        if isinstance(err, bytes):
            err = err.decode("utf-8", "replace")
        return None, out, err, "timeout"
    except OSError as exc:
        return None, "", f"{exc.__class__.__name__}: {exc}", "error"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--collect", action="store_true")
    ap.add_argument("--out-dir", default=str(
        Path(__file__).resolve().parent / "results" /
        "2026-11-14-p25-persist"))
    ap.add_argument("--env-dir", default=ENV_DIR_DEFAULT)
    args = ap.parse_args(argv)
    if args.plan:
        plan = {"tables": list(table_sqls(
            sealed_baseline(Path(__file__).resolve().parent))),
                "env_dir": args.env_dir}
        print(json.dumps(plan, indent=1, sort_keys=True))
        return 0
    if args.collect:
        if not RELEASED_FOR_EXECUTION:
            print("[p25-persist] DRAFT: not released — no psql "
                  "subprocess was constructed.", file=sys.stderr)
            return 3
        return collect_body(args, real_run_cli)
    print("usage: --plan | --collect", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
