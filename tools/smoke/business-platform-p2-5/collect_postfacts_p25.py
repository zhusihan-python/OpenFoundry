#!/usr/bin/env python3
"""P25 POSTFACTS collector — ACTUAL readonly IO entry (ABC-strictly-
after; review-archive-tools-round2 #5/#6).

NOT a dumps-assembler: the collect entry performs REAL readonly IO
behind a release flag, recording each command BEFORE execution and the
ORIGINAL rc/stdout/stderr/raw bytes + sha per artifact into an
immutable attempt dir with a full consumption ledger:

  01 controlled full-27 rows      psql (frozen guard ROW_SQL)
  02 checkpoint-owner rows        psql (sync_def_checkpoints DDL)
  03/04 protected counters        HTTP x2 reads (read-tagged raws;
                                   bodies never overwritten)
  05 archived proxy-quiet RAW     consumed from the aux_down attempt
                                   (the proxy is REMOVED before ABC —
                                   no post-hoc HTTP, never a boolean)
  06 archived fixture-quiet RAW   consumed from the aux_down attempt
  07 abc-result raw copy          bytes+sha bound into the attempt

`--plan` is pure. `--collect` refuses rc3 BEFORE any subprocess/HTTP
is constructed while RELEASED_FOR_EXECUTION is False; collect_body()
takes INJECTABLE run_cli/http_get so the selftest drives the REAL
entry with mocked IO (no listener/network/DB). A missing archived
quiet raw or a failed query is an HONEST FAILURE recorded in the
attempt — never defaulted, never fabricated.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import sys
sys.dont_write_bytecode = True  # Pure validation must not mutate copied evidence/tools.
import time
from pathlib import Path

RELEASED_FOR_EXECUTION = True   # root flips for the ONE readonly tail

ENV_DIR_DEFAULT = "/private/tmp/openfoundry-platform-p1-q3v9"
PROJECT = "openfoundry-platform-p1-q3v9"
# The OLD P1 fixture (protected cohorts live there; it is NOT removed
# with the p25 aux services):
FIXTURE_BASE = "http://127.0.0.1:17090"

# REAL DDL (primary migration 20261116100000): sync_def_checkpoints.
# blocked_reason is DERIVED in the projection: an unresolved owner on a
# terminal-failed run IS the P26 hold (tail-roster vocabulary); a
# nonterminal owner carries '' (adjudicated separately).
OWNER_SQL = (
    "SELECT json_agg(row_to_json(t)) FROM ("
    "  SELECT c.sync_def_id::text, c.source_id::text,"
    "         c.output_dataset_id::text, c.mode,"
    "         c.confirmed_epoch::text,"
    "         c.active_run_id::text AS run_id,"
    "         COALESCE(c.active_since::text,'') AS active_since,"
    "         r.status AS run_status,"
    "         CASE WHEN r.status='failed' OR (r.status='running'"
    "              AND NOT (r.lease_expires_at IS NOT NULL AND"
    "              r.lease_expires_at > clock_timestamp()))"
    "              THEN 'requires_reconciliation' ELSE '' END"
    "              AS blocked_reason"
    "  FROM sync_def_checkpoints c"
    "  LEFT JOIN sync_runs r ON r.id = c.active_run_id"
    "  WHERE c.active_run_id IS NOT NULL"
    "  ORDER BY c.sync_def_id) t;")


def load_guard(here: Path):
    spec = importlib.util.spec_from_file_location(
        "p25_guard_pf", here / "p25_cohort_guard.py")
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    return guard


def psql_argv(env_dir: Path, sql: str):
    return ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
            "--project-name", PROJECT, "exec", "-T", "postgres",
            "psql", "-U", "openfoundry", "-d", "connectors", "-t",
            "-A", "-v", "ON_ERROR_STOP=1",
            "-c", "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';",
            "-c", sql]


def sealed_baseline(here: Path) -> dict:
    spec = importlib.util.spec_from_file_location(
        "p25_derive_pfcol", here / "p25_cohort_derive.py")
    derive = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(derive)
    data = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                         Path(derive.SEAL_P24_TOOLS_DEFAULT))
    cohort6_sha = hashlib.sha256(
        (here / "p25_cohort6.py").read_bytes()).hexdigest()
    return derive.frozen_baseline_dict(data, cohort6_sha)


def identity_def_sql(ids):
    """Canonical fixed-whole-union readonly projection for Runtime catalog."""
    import re
    values = sorted(set(ids))
    if not values or any(not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", str(v)) for v in values):
        raise ValueError("identity catalog requires a nonempty exact UUID set")
    literals = ", ".join("'" + v + "'" for v in values)
    return ("SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text, "
            "d.source_id::text, COALESCE(d.schedule_cron,'') AS cron, "
            "d.schedule_enabled::text AS enabled, "
            "(d.schedule_next_fire_at IS NULL)::text AS cursor_null "
            "FROM batch_sync_defs d WHERE d.id::text = ANY (ARRAY[" + literals + "]::text[]) ORDER BY d.id) t;")


GLOBAL_SCHEDULES_SQL = (
    "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text, "
    "d.source_id::text, COALESCE(d.schedule_cron,'') AS cron, "
    "d.schedule_enabled::text AS enabled, "
    "(d.schedule_next_fire_at IS NULL)::text AS cursor_null "
    "FROM batch_sync_defs d WHERE COALESCE(d.schedule_cron,'') <> '' ORDER BY d.id) t;")


def canonical_plan_dict(frozen: dict, env_dir: Path = None, roster=None):
    """The canonical catalog (pure; PARAMETER-FREE apart from the
    FIXED env dir/base so collector and verifier hashes match). File
    locations for archived raws resolve at execution time from CLI
    args; the plan records logical sources only."""
    env_dir = Path(env_dir or ENV_DIR_DEFAULT)
    guard = load_guard(Path(__file__).resolve().parent)
    protected = sorted(
        frozen["content_gates"]["protected_counters"]["expected"])
    steps = [
        {"id": "01-controlled-rows", "kind": "psql",
         "argv": psql_argv(env_dir, guard.ROW_SQL.strip()),
         "artifacts": ["rows.json"]},
        {"id": "02-cp-owners", "kind": "psql",
         "argv": psql_argv(env_dir, OWNER_SQL),
         "artifacts": ["owners.json"]},
        {"id": "03-counters", "kind": "http_get", "read": "read1",
         "urls": [f"{FIXTURE_BASE}/admin/src/{s}" for s in protected]},
        {"id": "04-counters", "kind": "http_get", "read": "read2",
         "urls": [f"{FIXTURE_BASE}/admin/src/{s}" for s in protected]},
        {"id": "05-archived-proxy-quiet", "kind": "archived_raw",
         "src": "aux:proxy-quiet-proof.stdout.log",
         "artifacts": ["stats.archived"]},
        {"id": "06-archived-fixture-quiet", "kind": "archived_raw",
         "src": "aux:fixture-quiet-proof.stdout.log",
         "artifacts": ["counters.archived"]},
        {"id": "07-abc-result", "kind": "archived_raw",
         "src": "ev:abc-result.json",
         "artifacts": ["abc-result.raw.json"]},
    ]
    # Reuse the reviewed preflight FACT commands only. No old-base boot
    # adjudication or owned17 duplicate query is imported here.
    spec = importlib.util.spec_from_file_location("postfacts_safety_plan", Path(__file__).resolve().parent / "readonly_preflight_p25.py")
    preflight = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(preflight)
    roster = roster if roster is not None else guard.load_roster()
    safety_ids = {"05b-global-grants", "05c-global-nonempty-cron", "06-legacy-bc", "08-fault-absent", "10-volumes", "11-env-modes", "12-fixture"}
    steps.extend(st for st in preflight.build_plan(env_dir, roster, frozen, "OF_P25_C_TOKEN") if st["id"] in safety_ids)
    steps.append({"id": "05d-global-schedules", "kind": "psql",
                  "argv": psql_argv(env_dir, GLOBAL_SCHEDULES_SQL), "artifacts": ["rows.json"]})
    return {"env_dir": str(env_dir), "fixture_base": FIXTURE_BASE,
            "steps": steps}


def plan_sha256(plan: dict) -> str:
    return hashlib.sha256(json.dumps(
        plan, indent=1, sort_keys=False).encode()).hexdigest()


class Attempt:
    """Collision-free immutable attempt dir + consumption ledger
    (cmd written BEFORE execution; original rc/stdout/stderr)."""

    def __init__(self, base: Path):
        n = 1
        while (base / f"attempt-{n}").exists():
            n += 1
        self.dir = base / f"attempt-{n}"
        self.dir.mkdir(parents=True)
        self.failures = []
        self.steps = []
        self.ledger = {}

    def record(self, name, original_rc=None):
        p = self.dir / name
        entry = {"bytes": p.stat().st_size,
                 "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
        if original_rc is not None:
            entry["original_rc"] = original_rc
        self.ledger[name] = entry


def _write_cmd(attempt: Attempt, base: str, argv):
    (attempt.dir / f"{base}.cmd").write_text(
        " ".join(argv) + "\n")               # cmd BEFORE execution
    attempt.record(f"{base}.cmd")


def execute_plan(steps, attempt: Attempt, run_cli, http_get,
                 aux_quiet_dir: Path, abc_result: Path, plan: dict,
                 plan_sha: str):
    for step in steps:
        sid = step["id"]
        if step["kind"] in ("psql", "cli", "compose_ps", "inspect"):
            argv = list(step["argv"])
            if step.get("needs_cid_from"):
                cid_path = attempt.dir / (step["needs_cid_from"] + ".stdout")
                cids = cid_path.read_text().splitlines() if cid_path.is_file() else []
                if len(cids) != 1 or not cids[0].strip():
                    attempt.failures.append(f"{sid}: original ps did not yield one fixture CID")
                    continue
                argv[argv.index("<CID>")] = cids[0].strip()
            for stem in step["artifacts"]:
                base = f"{sid}.{stem}"
                _write_cmd(attempt, base, argv)
                rc, out, err, cls = run_cli(argv, timeout=120)
                if cls == "timeout":
                    (attempt.dir / f"{base}.rc").write_text("unknown\n")
                    (attempt.dir / f"{base}.wrapper").write_text(
                        "timeout\n")
                    attempt.failures.append(
                        f"{base}: TIMEOUT — original rc unknown; "
                        "partial output retained")
                    rc_label = "unknown"
                elif cls == "error":
                    (attempt.dir / f"{base}.rc").write_text("unknown\n")
                    (attempt.dir / f"{base}.wrapper").write_text(
                        "oserror\n")
                    attempt.failures.append(
                        f"{base}: OSERROR — original rc unknown")
                    rc_label = "unknown"
                else:
                    (attempt.dir / f"{base}.rc").write_text(f"{rc}\n")
                    if rc != 0:
                        attempt.failures.append(
                            f"{base}: rc={rc} (honest failure — never "
                            "an empty success)")
                    rc_label = str(rc)
                (attempt.dir / f"{base}.stdout").write_text(out or "")
                (attempt.dir / f"{base}.stderr").write_text(err or "")
                attempt.steps.append(base)
                for suf in (".rc", ".stdout", ".stderr", ".wrapper"):
                    p = attempt.dir / f"{base}{suf}"
                    if p.is_file():
                        attempt.record(p.name, rc_label
                                       if suf == ".rc" else None)
        elif step["kind"] == "http_get":
            read = step["read"]
            snap = {"bodies_sha256": {}}
            for url in step["urls"]:
                src = url.rsplit("/", 1)[-1]
                base = f"{sid}.{read}.{src}"
                (attempt.dir / f"{base}.url").write_text(f"GET {url}\n")
                status, body, err = http_get(url, "")
                (attempt.dir / f"{base}.http_status").write_text(
                    f"{status if err is None else 'error'}\n")
                (attempt.dir / f"{base}.body").write_bytes(body or b"")
                if err is not None:
                    (attempt.dir / f"{base}.err").write_text(
                        str(err)[:300] + "\n")
                    attempt.failures.append(
                        f"{base}: HTTP error {err}")
                else:
                    if status != 200:
                        attempt.failures.append(
                            f"{base}: HTTP {status} != 200")
                    try:
                        json.loads(body or b"")
                    except ValueError:
                        attempt.failures.append(
                            f"{base}: body not JSON")
                snap[src] = {"http_status":
                             status if err is None else "error"}
                snap["bodies_sha256"][src] = hashlib.sha256(
                    body or b"").hexdigest()
                for suf in (".url", ".http_status", ".body", ".err"):
                    p = attempt.dir / f"{base}{suf}"
                    if p.is_file():
                        attempt.record(p.name)
            (attempt.dir / f"{sid}.{read}.derived").write_text(
                json.dumps(snap, indent=1, sort_keys=True) + "\n")
            attempt.steps.append(f"{sid}.{read}")
            attempt.record(f"{sid}.{read}.derived")
        elif step["kind"] == "archived_raw":
            logical = step["src"]
            if logical.startswith("aux:"):
                src = aux_quiet_dir / logical[4:]
            else:
                src = abc_result
            for stem in step["artifacts"]:
                base = f"{sid}.{stem}"
                (attempt.dir / f"{base}.cmd").write_text(
                    f"ARCHIVED-RAW {logical} <- {src}\n")
                attempt.record(f"{base}.cmd")
                if not src.is_file():
                    attempt.failures.append(
                        f"{base}: archived raw MISSING ({src}) — the "
                        "pre-removal proof must exist in the aux "
                        "attempt; refusing to fabricate")
                    (attempt.dir / f"{base}.rc").write_text("missing\n")
                    attempt.record(f"{base}.rc", "missing")
                    attempt.steps.append(base)
                    continue
                raw = src.read_bytes()
                (attempt.dir / base).write_bytes(raw)
                (attempt.dir / f"{base}.rc").write_text("0\n")
                attempt.steps.append(base)
                attempt.record(base, 0)
                attempt.record(f"{base}.rc", 0)
        else:
            attempt.failures.append(f"{sid}: unknown kind "
                                    f"{step['kind']}")
    summary = {"attempt_dir": str(attempt.dir),
               "client_time": time.strftime("%Y-%m-%d %H:%M:%S"),
               "steps_recorded": attempt.steps,
               "failures": attempt.failures,
               "plan_sha256": plan_sha,
               "artifacts": attempt.ledger,
               "ok": not attempt.failures}
    (attempt.dir / "plan.json").write_text(json.dumps(
        plan, indent=1) + "\n")
    attempt.record("plan.json")
    (attempt.dir / "attempt-summary.json").write_text(json.dumps(
        summary, indent=1, sort_keys=True) + "\n")
    return summary


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


def real_http_get(url, token):
    import urllib.request
    import urllib.error
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, resp.read(), None
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), None
    except Exception as exc:            # noqa: BLE001 — honest error
        return None, b"", exc


def collect_body(args, run_cli, http_get):
    """Actual collection path (release flag already checked). The
    sealed baseline is rederived BEFORE any runtime IO; the plan is
    built from the SEALED dict."""
    here = Path(__file__).resolve().parent
    sealed = sealed_baseline(here)
    plan = canonical_plan_dict(sealed, Path(args.env_dir))
    plan_sha = plan_sha256(canonical_plan_dict(sealed))
    attempt = Attempt(Path(args.out_dir))
    summary = execute_plan(plan["steps"], attempt, run_cli, http_get,
                           Path(args.aux_quiet_dir),
                           Path(args.abc_result), plan, plan_sha)
    print(f"[p25-postfacts] attempt {attempt.dir} ok={summary['ok']} "
          f"failures={len(summary['failures'])}")
    # machine verdict LAST on stdout (finalizer evidence, ARCHIVE-
    # INTERFACE §4)
    print(json.dumps({"ok": summary["ok"],
                      "attempt_dir": str(attempt.dir),
                      "failures": len(summary["failures"])}))
    return 0 if summary["ok"] else 1


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--collect", action="store_true")
    ap.add_argument("--out-dir", default=str(
        Path(__file__).resolve().parent / "results" /
        "2026-11-14-p25-postfacts"))
    ap.add_argument("--env-dir", default=ENV_DIR_DEFAULT)
    ap.add_argument("--aux-quiet-dir", required=False,
                    help="the SELECTED aux_down attempt dir carrying "
                         "the archived pre-removal quiet raws")
    ap.add_argument("--abc-result", required=False,
                    help="this round's raw result_p21.json (copied "
                         "byte-exact into the attempt)")
    args = ap.parse_args(argv)
    if args.plan:
        plan = canonical_plan_dict(
            sealed_baseline(Path(__file__).resolve().parent))
        print(json.dumps(plan, indent=1, sort_keys=False))
        return 0
    if args.collect:
        if not RELEASED_FOR_EXECUTION:
            print("[p25-postfacts] DRAFT: not released — root must "
                  "review the archive code/selftests and flip "
                  "RELEASED_FOR_EXECUTION. No docker/psql/http "
                  "subprocess was constructed.", file=sys.stderr)
            return 3
        missing = [k for k in ("aux_quiet_dir", "abc_result")
                   if not getattr(args, k)]
        if missing:
            print(f"[p25-postfacts] --collect requires --{missing[0]}"
                  " (archived quiet raws + raw abc result)",
                  file=sys.stderr)
            return 2
        return collect_body(args, real_run_cli, real_http_get)
    print("usage: --plan | --collect", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
