#!/usr/bin/env python3
"""P2.5 readonly preflight COLLECTOR (foundation; DRAFT, not released).

Round-2 closures implemented here (review-tools-code-round2):
  1. the ACTUAL collect/main entry loads the GUARD module (which owns
     load_roster) — the round-2 AttributeError at the collect entry is
     fixed, and the pure selftest drives THIS entry with mocked
     subprocess/HTTP (no listener/network/DB).
  2. owned-history SQLs filter by the EXACT sealed 17 def ids
     (UUID-validated literals injected from the seal-derived baseline —
     never caller-editable SQL text); the GLOBAL controlled-6
     projection stays a WHOLE-SET query; separate GLOBAL grants-total
     and global nonempty-cron steps preserve the sealed-roster
     protection.
  3. C endpoints: GET /datasets/{t}/branches/main/transactions/{txn}
     (FLAT RuntimeTransaction), GET /datasets/{t}/files/{fid}/download
     (raw bytes) and GET /datasets/{t}/files (list, for the file's
     transaction-ownership + logical_path lineage binding).
  4. the canonical PLAN is derived from the seal chain and recorded in
     the attempt (plan.json + its sha in the summary); the FORMAL
     verifier rederives everything from the fixed P24 seal and accepts
     NO caller-provided plan/baseline on its CLI.
  5. attempt-summary is a full CONSUMPTION LEDGER: every artifact's
     exact name/bytes/sha256 + per-step original rc + step ids +
     canonical plan sha256 (+ env). .cmd files are written BEFORE
     execution; timeout/OSError keep partial output with honest
     "unknown" original rc and a separate wrapper label.
  6. counter reads record full bodies; typing discipline is enforced
     by the verifier (non-bool ints, exact int 0, all three counts).

`--plan` stays pure. `--collect` refuses (rc 3) until root flips
RELEASED_FOR_EXECUTION. `collect_body()` takes INJECTABLE run_cli/
http_get callables so the pure selftest exercises the real entry.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

RELEASED_FOR_EXECUTION = True    # ROOT RELEASED 2026-11-15: ONE readonly first-transition preflight (this flag ONLY; guard/adapter/runtime stay unreleased)

ENV_DIR_DEFAULT = "/private/tmp/openfoundry-platform-p1-q3v9"
PROJECT = "openfoundry-platform-p1-q3v9"
PG_ENV_600 = "/private/tmp/openfoundry-p24-pg.env"   # MODE only
DATASET_BASE = "http://127.0.0.1:17078"
FIXTURE_BASE = "http://127.0.0.1:17090"

FAULT_FUNCTION = "p21_c_terminal_fault"
FAULT_TRIGGER = "p21_c_terminal_fault_trg"

AUX_SERVICES = ("connector2", "connector3", "dataset-proxy")

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")


def id_literals(ids):
    """Validate UUIDs and render a safe SQL array literal. The ids come
    from the seal-derived baseline only; anything non-UUID aborts (no
    caller-editable text ever reaches SQL)."""
    parts = []
    for i in sorted(set(ids)):
        if not UUID_RE.match(str(i)):
            raise ValueError(f"non-UUID id refused for SQL: {i!r}")
        parts.append(f"'{i}'")
    return ", ".join(parts)


def compose_argv(env_dir: Path, *a):
    return ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
            "--project-name", PROJECT, *a]


def psql_argv(env_dir: Path, sql: str):
    return compose_argv(env_dir, "exec", "-T", "postgres", "psql",
                        "-U", "openfoundry", "-d", "connectors",
                        "-t", "-A", "-v", "ON_ERROR_STOP=1",
                        "-c", "SET TimeZone='UTC'; "
                              "SET DateStyle='ISO, MDY';", "-c", sql)


def owned_sqls(frozen: dict):
    """Round-2: EXACT sealed-17 filtering (UUID-validated literals)."""
    owned_ids = [d["id"] for d in frozen["content_gates"]
                 ["owned_fullstate"]["defs"]]
    arr = id_literals(owned_ids)
    return {
        "owned-defs.json":
            "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text, "
            "d.source_id::text, d.output_dataset_id::text, "
            "COALESCE(d.schedule_cron,'') AS cron, "
            "d.schedule_time_zone AS tz, d.schedule_enabled::text AS "
            "enabled, (d.schedule_next_fire_at IS NULL)::text AS "
            "cursor_null, COALESCE(d.schedule_revision::text,'') AS "
            "schedule_revision, COALESCE(d.schedule_last_fire_at::text,"
            " '') AS last_fire, COALESCE(d.schedule_last_tick_outcome,"
            "'') AS last_outcome, COALESCE(d.schedule_last_tick_at::"
            "text,'') AS last_tick FROM batch_sync_defs d WHERE "
            f"d.id::text = ANY (ARRAY[{arr}]::text[]) ORDER BY d.id) t;",
        "owned-ticks.json":
            "SELECT json_agg(row_to_json(t)) FROM (SELECT t.sync_def_id"
            "::text, t.id::text AS tick_id FROM sync_schedule_ticks t "
            f"WHERE t.sync_def_id::text = ANY (ARRAY[{arr}]::text[]) "
            "ORDER BY t.id) t;",
        "owned-runs.json":
            "SELECT json_agg(row_to_json(t)) FROM (SELECT r.id::text AS"
            " run_id, r.status, r.sync_def_id::text FROM sync_runs r "
            "WHERE r.control_mode='controlled' AND r.status IN "
            "('succeeded','failed','aborted') AND r.sync_def_id::text ="
            f" ANY (ARRAY[{arr}]::text[]) ORDER BY r.id) t;",
        "owned-grants.json":
            "SELECT json_agg(row_to_json(t)) FROM (SELECT g.sync_def_id"
            "::text, g.grant_id::text FROM sync_schedule_grants g WHERE"
            f" g.sync_def_id::text = ANY (ARRAY[{arr}]::text[]) ORDER "
            "BY g.grant_id) t;",
    }


GLOBAL_GRANTS_SQL = (
    "SELECT count(*)::text FROM sync_schedule_grants;")
GLOBAL_NONEMPTY_CRON_SQL = (
    "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text FROM "
    "batch_sync_defs d WHERE COALESCE(d.schedule_cron,'') <> '' "
    "ORDER BY d.id) t;")

FAULT_FUNCTION_SQL = (
    "SELECT count(*)::text FROM pg_proc p JOIN pg_namespace n ON "
    "n.oid = p.pronamespace WHERE n.nspname='public' AND "
    f"p.proname='{FAULT_FUNCTION}';")
FAULT_TRIGGER_SQL = (
    "SELECT count(*)::text FROM pg_trigger tg JOIN pg_class c ON "
    "c.oid = tg.tgrelid WHERE c.relname='sync_runs' AND "
    f"tg.tgname='{FAULT_TRIGGER}';")


def load_guard(here: Path):
    """The GUARD module (owns load_roster + adjudicators)."""
    spec = importlib.util.spec_from_file_location(
        "p25_guard", here / "p25_cohort_guard.py")
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    return guard


def build_plan(env_dir: Path, roster, frozen: dict, token_env: str):
    """The EXACT canonical catalog (pure). Rendered into --plan and into
    every attempt; the FORMAL verifier rederives it from the seal."""
    cg = frozen["content_gates"]
    protected = sorted(cg["protected_counters"]["expected"])
    c_live = cg["c_live_sealed"]
    legacy = cg["legacy"]
    guard = load_guard(Path(__file__).resolve().parent)
    steps = []

    def svc_step(sid, svc, ps_flag, ins_fmt, ins_stem):
        steps.append({
            "id": sid, "kind": "compose_ps", "service": svc,
            "argv": compose_argv(env_dir, "ps", ps_flag, svc),
            "artifacts": ["ps.out"]})
        steps.append({
            "id": sid, "kind": "inspect", "service": svc,
            "needs_cid_from": f"{sid}.ps.out",
            "argv": ["docker", "inspect", "--format", ins_fmt, "<CID>"],
            "artifacts": [ins_stem]})

    svc_step("01-connector-boot-image", "connector", "-q",
             "{{.State.StartedAt}}|{{.Image}}", "inspect.out")
    svc_step("02-dataset-boot-image", "dataset", "-q",
             "{{.State.StartedAt}}|{{.Image}}", "inspect.out")
    svc_step("02b-pipeline-boot-image", "pipeline", "-q",
             "{{.State.StartedAt}}|{{.Image}}", "inspect.out")

    for svc in AUX_SERVICES:
        # ROOT FIX (foundation re-review): `compose ps -aq <svc>` with
        # only the base file loaded fails rc1 "no such service" — the
        # three aux services live in the FROZEN override, and an rc1 is
        # NOT an absence proof. Use docker ps -aq with EXACT compose
        # project + service LABEL filters instead: includes stopped
        # containers, precise per-service match, never crosses the q3
        # project, and never depends on override files. Empty stdout is
        # then a true absence (all states) proof.
        steps.append({
            "id": "03-aux-absent", "kind": "cli", "service": svc,
            "argv": ["docker", "ps", "-aq", "--filter",
                     f"label=com.docker.compose.project={PROJECT}",
                     "--filter",
                     f"label=com.docker.compose.service={svc}"],
            "artifacts": [f"{svc}.out"], "expect_empty": True})

    steps.append({"id": "04-controlled-rows", "kind": "psql",
                  "argv": psql_argv(env_dir, guard.ROW_SQL.strip()),
                  "artifacts": ["rows.json"]})

    for stem, sql in sorted(owned_sqls(frozen).items()):
        steps.append({"id": "05-owned-fullstate", "kind": "psql",
                      "argv": psql_argv(env_dir, sql),
                      "artifacts": [stem]})
    steps.append({"id": "05b-global-grants", "kind": "psql",
                  "argv": psql_argv(env_dir, GLOBAL_GRANTS_SQL),
                  "artifacts": ["count.txt"]})
    steps.append({"id": "05c-global-nonempty-cron", "kind": "psql",
                  "argv": psql_argv(env_dir, GLOBAL_NONEMPTY_CRON_SQL),
                  "artifacts": ["ids.json"]})

    for label, rid in sorted(legacy["runs"].items()):
        sql = ("SET TimeZone='UTC'; SET DateStyle='ISO, MDY';\n"
               + legacy["projection_sql"].format(rid=rid))
        steps.append({"id": "06-legacy-bc", "kind": "psql",
                      "argv": compose_argv(env_dir, "exec", "-T",
                                           "postgres", "psql", "-U",
                                           "openfoundry", "-d",
                                           "connectors", "-t", "-A",
                                           "-v", "ON_ERROR_STOP=1",
                                           "-c", sql),
                      "artifacts": [f"{label}.txt"]})

    steps.append({
        "id": "07-c-live", "kind": "http_get",
        "urls": [f"{DATASET_BASE}/v1/datasets/{c_live['target']}"
                 f"/branches/main/transactions/{c_live['txn_id']}",
                 f"{DATASET_BASE}/v1/datasets/{c_live['target']}"
                 f"/files/{c_live['bound_files'][0]}/download",
                 f"{DATASET_BASE}/v1/datasets/{c_live['target']}"
                 f"/files"],
        "token_env": token_env,
        "artifacts": ["txn.json", "file.bin", "files.json"]})

    steps.append({"id": "08-fault-absent", "kind": "psql",
                  "argv": psql_argv(env_dir, FAULT_FUNCTION_SQL),
                  "artifacts": ["function-count.txt"]})
    steps.append({"id": "08-fault-absent", "kind": "psql",
                  "argv": psql_argv(env_dir, FAULT_TRIGGER_SQL),
                  "artifacts": ["trigger-count.txt"]})

    for read in ("read1", "read2"):
        steps.append({
            "id": "09-protected-counters", "kind": "http_get",
            "urls": [f"{FIXTURE_BASE}/admin/src/{s}" for s in protected],
            "artifacts": [f"{read}.json"],
            "expect_status": 200})

    steps.append({"id": "10-volumes", "kind": "cli",
                  "argv": ["docker", "volume", "ls", "--format",
                           "{{.Name}}"],
                  "artifacts": ["names.txt"]})
    steps.append({"id": "11-env-modes", "kind": "cli",
                  "argv": ["stat", "-f", "%Lp", str(env_dir / ".env")],
                  "artifacts": ["q3-env-mode.txt"]})
    steps.append({"id": "11-env-modes", "kind": "cli",
                  "argv": ["stat", "-f", "%Lp", PG_ENV_600],
                  "artifacts": ["pg-env-mode.txt"]})

    steps.append({"id": "12-fixture", "kind": "compose_ps",
                  "service": "fixture",
                  "argv": compose_argv(env_dir, "ps", "-q", "fixture"),
                  "artifacts": ["ps.out"]})
    steps.append({"id": "12-fixture", "kind": "inspect",
                  "needs_cid_from": "12-fixture.ps.out",
                  "argv": ["docker", "inspect", "--format",
                           "{{.State.StartedAt}}|{{.State.Running}}|"
                           "{{.Image}}|{{.HostConfig.RestartPolicy.Name}}",
                           "<CID>"],
                  "artifacts": ["state.txt"]})
    steps.append({"id": "12-fixture", "kind": "inspect",
                  "needs_cid_from": "12-fixture.ps.out",
                  "argv": ["docker", "inspect", "--format",
                           "{{json .Config.Entrypoint}}|{{json .Config.Cmd}}",
                           "<CID>"],
                  "artifacts": ["pid1.txt"]})

    steps.append({"id": "13-local-shas", "kind": "local_sha",
                  "files": ["p24-frozen-baseline.json", "p25_cohort6.py",
                            "source-p24-baseline.txt"],
                  "artifacts": ["derived.json"]})
    return steps


def canonical_plan_dict(env_dir: Path, roster, frozen):
    """The canonical plan content — deliberately WITHOUT any mutable
    release flag so the collect-time and seal-rederived hashes match
    byte-for-byte."""
    steps = build_plan(env_dir, roster, frozen, "OF_P25_C_TOKEN")
    return {"env_dir": str(env_dir),
            "dataset_base": DATASET_BASE,
            "fixture_base": FIXTURE_BASE,
            "steps": steps}


def plan_json(env_dir: Path) -> str:
    here = Path(__file__).resolve().parent
    guard = load_guard(here)
    roster = guard.load_roster()
    frozen = json.loads((here / "p24-frozen-baseline.json").read_text())
    plan = canonical_plan_dict(env_dir, roster, frozen)
    plan["released"] = RELEASED_FOR_EXECUTION   # display-only
    return json.dumps(plan, indent=1, sort_keys=False) + "\n"


class Attempt:
    """Artifact writer: appended suffixes, collision-free attempt dir,
    full consumption ledger (round-2 item 5)."""

    def __init__(self, base: Path):
        n = 1
        while (base / f"attempt-{n}").exists():
            n += 1
        self.dir = base / f"attempt-{n}"
        self.dir.mkdir(parents=True)
        self.failures = []
        self.steps = []
        self.rcs = {}
        self.ledger = {}

    def raw(self, name):
        return self.dir / name

    def record_artifact(self, name):
        """Ledger entry: exact bytes + sha256 of what is on disk NOW
        (post-write), plus the recorded original rc when known."""
        p = self.dir / name
        entry = {"bytes": p.stat().st_size,
                 "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
        if name in self.rcs:
            entry["original_rc"] = self.rcs[name]
        self.ledger[name] = entry


def execute_plan(steps, attempt: Attempt, run_cli, http_get, roster,
                 frozen, plan_sha):
    """Orchestration with INJECTED IO callables (selftest drives the
    real entry). run_cli(argv, timeout) -> (rc, out, err, cls) with cls
    in {ok, timeout, error}; http_get(url, token) -> (status, bytes,
    err)."""
    for step in steps:
        sid = step["id"]
        if step["kind"] in ("compose_ps", "inspect", "psql", "cli"):
            argv = list(step["argv"])
            if step.get("needs_cid_from"):
                stem = step["needs_cid_from"]
                ps_out = attempt.raw(stem + ".stdout")
                if not ps_out.is_file():
                    attempt.failures.append(f"{sid}: missing cid source "
                                            f"{stem}")
                    continue
                cids = [l for l in ps_out.read_text().splitlines()
                        if l.strip()]
                if len(cids) != 1:
                    attempt.failures.append(
                        f"{sid}: {stem} did not yield exactly one cid")
                    continue
                argv[argv.index("<CID>")] = cids[0]
            for stem in step["artifacts"]:
                base = f"{sid}.{stem}"
                attempt.raw(base + ".cmd").write_text(
                    " ".join(argv) + "\n")          # cmd BEFORE execution
                rc, out, err, cls = run_cli(argv, timeout=120)
                if cls == "timeout":
                    attempt.raw(base + ".rc").write_text("unknown\n")
                    attempt.raw(base + ".wrapper").write_text("timeout\n")
                    attempt.rcs[base + ".rc"] = "unknown"
                    attempt.failures.append(f"{base}: TIMEOUT — original "
                                            "rc unknown; partial output "
                                            "retained")
                elif cls == "error":
                    attempt.raw(base + ".rc").write_text("unknown\n")
                    attempt.raw(base + ".wrapper").write_text("oserror\n")
                    attempt.rcs[base + ".rc"] = "unknown"
                    attempt.failures.append(f"{base}: OSERROR — original "
                                            "rc unknown")
                else:
                    attempt.raw(base + ".rc").write_text(f"{rc}\n")
                    attempt.rcs[base + ".rc"] = str(rc)
                    if rc != 0:
                        attempt.failures.append(
                            f"{base}: rc={rc} (error artifact — must be "
                            "adjudicated, never an empty success)")
                attempt.raw(base + ".stdout").write_text(out or "")
                attempt.raw(base + ".stderr").write_text(err or "")
                attempt.steps.append(base)
                for suffix in (".cmd", ".rc", ".stdout", ".stderr",
                               ".wrapper"):
                    p = attempt.raw(base + suffix)
                    if p.is_file():
                        attempt.record_artifact(p.name)
        elif step["kind"] == "http_get":
            token = os.environ.get(step.get("token_env", ""), "")
            if step.get("token_env") and not token:
                attempt.failures.append(
                    f"{sid}: token env {step['token_env']} unset — step "
                    "cannot run; record honestly, never fake")
                continue
            if step["id"] == "09-protected-counters":
                # (#4 + round3-residual) every counter response is
                # retained RAW under a READ-TAGGED filename:
                # {sid}.{read}.{src}.url/.http_status/.body — read2
                # can never overwrite read1's raw evidence (18 raw
                # sets, first-round failure originals immutable).
                read = step["artifacts"][0].split(".")[0]  # read1|read2
                snap = {"bodies_sha256": {}}
                for url in step["urls"]:
                    name = url.rsplit("/", 1)[-1]
                    base = f"{sid}.{read}.{name}"
                    attempt.raw(base + ".url").write_text(
                        f"GET {url}\n")
                    status, body, err = http_get(url, "")
                    attempt.raw(base + ".http_status").write_text(
                        f"{status if err is None else 'error'}\n")
                    attempt.rcs[base + ".http_status"] = \
                        str(status) if err is None else "error"
                    attempt.raw(base + ".body").write_bytes(body or b"")
                    if err is not None:
                        attempt.raw(base + ".err").write_text(
                            str(err)[:300] + "\n")
                        attempt.failures.append(
                            f"{base}: HTTP error {err}")
                    entry = ({"http_status": status,
                              "body": json.loads(body) if body
                              else None}
                             if err is None
                             else {"error": str(err)[:150]})
                    snap[name] = entry
                    snap["bodies_sha256"][name] = hashlib.sha256(
                        body or b"").hexdigest()
                    for suffix in (".url", ".http_status", ".body",
                                   ".err"):
                        pf = attempt.raw(base + suffix)
                        if pf.is_file():
                            attempt.record_artifact(pf.name)
                stem = step["artifacts"][0]
                attempt.raw(f"{sid}.{stem}.cmd").write_text(
                    "GET (http) " + ", ".join(step["urls"]) + "\n")
                attempt.raw(f"{sid}.{stem}.derived").write_text(
                    json.dumps(snap, indent=1, sort_keys=True) + "\n")
                attempt.raw(f"{sid}.{stem}.kind").write_text("http\n")
                attempt.steps.append(f"{sid}.{stem}")
                for suffix in (".cmd", ".derived", ".kind"):
                    attempt.record_artifact(f"{sid}.{stem}{suffix}")
            else:
                for stem, url in zip(step["artifacts"], step["urls"]):
                    attempt.raw(f"{sid}.{stem}.url").write_text(
                        f"GET {url}\n")
                    status, body, err = http_get(url, token)
                    attempt.raw(f"{sid}.{stem}.http_status").write_text(
                        f"{status if err is None else 'error'}\n")
                    attempt.rcs[f"{sid}.{stem}.http_status"] = \
                        str(status) if err is None else "error"
                    if err is not None:
                        attempt.raw(f"{sid}.{stem}.err").write_text(
                            str(err)[:300] + "\n")
                        attempt.failures.append(
                            f"{sid}.{stem}: HTTP error {err}")
                    attempt.raw(f"{sid}.{stem}.body").write_bytes(
                        body or b"")
                    attempt.steps.append(f"{sid}.{stem}")
                    for suffix in (".url", ".http_status", ".body",
                                   ".err"):
                        p = attempt.raw(f"{sid}.{stem}{suffix}")
                        if p.is_file():
                            attempt.record_artifact(p.name)
        elif step["kind"] == "local_sha":
            here = Path(__file__).resolve().parent
            derived, exc = {}, None
            for f in step["files"]:
                p = here / f
                try:
                    derived[f] = hashlib.sha256(
                        p.read_bytes()).hexdigest() if p.is_file() \
                        else "MISSING"
                except OSError as e:
                    derived[f] = f"ERROR:{e.__class__.__name__}"
                    exc = str(e)[:150]
            stem = step["artifacts"][0]
            base = f"{sid}.{stem}"
            attempt.raw(base + ".kind").write_text("local\n")
            attempt.raw(base + ".derived").write_text(
                json.dumps(derived, indent=1, sort_keys=True) + "\n")
            if exc is not None:
                attempt.raw(base + ".exception").write_text(exc + "\n")
                attempt.failures.append(f"{base}: local derivation error")
            attempt.steps.append(base)
            for suffix in (".kind", ".derived", ".exception"):
                p = attempt.raw(base + suffix)
                if p.is_file():
                    attempt.record_artifact(p.name)
        else:
            attempt.failures.append(f"{sid}: unknown kind {step['kind']}")

    # the canonical plan sha (passed in, derived from the seal chain by
    # collect_body) is bound into the summary; the verifier rederives
    # it independently and refuses any drift (round-2 item 4/5).
    summary = {
        "attempt_dir": str(attempt.dir),
        "client_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "steps_recorded": attempt.steps,
        "failures": attempt.failures,
        "plan_sha256": plan_sha,
        "artifacts": attempt.ledger,
        "ok": not attempt.failures,
    }
    attempt.raw("attempt-summary.json").write_text(
        json.dumps(summary, indent=1, sort_keys=True) + "\n")
    return summary


def real_run_cli(argv, timeout):
    """Real subprocess adapter: honest timeout/OSError classification
    with partial output retention (code-review items 5/11)."""
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
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, resp.read(), None
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), None
    except Exception as exc:            # honest error, never a fake 200
        return None, b"", exc


def sealed_baseline(here: Path) -> dict:
    """Independently rederive the FULL frozen baseline from the fixed
    P24 seal (round3 #2): content-hash-anchored archive + tool chain.
    This is the ONLY trusted source for anything that reaches real
    psql/HTTP argv."""
    spec = importlib.util.spec_from_file_location(
        "p25_derive_seal", here / "p25_cohort_derive.py")
    derive = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(derive)
    data = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                         Path(derive.SEAL_P24_TOOLS_DEFAULT))
    cohort6_sha = hashlib.sha256(
        (here / "p25_cohort6.py").read_bytes()).hexdigest()
    return derive.frozen_baseline_dict(data, cohort6_sha)


def read_local_baseline(here: Path) -> dict:
    """Local sibling read — indirection kept separate so the negative
    selftest can simulate a tampered LOCAL file (sealed source stays
    the sealed_baseline() derivation)."""
    return json.loads(
        (here / "p24-frozen-baseline.json").read_text())


def collect_body(args, run_cli, http_get):
    """The actual collection path (flag already checked by caller).

    PRE-EXECUTION BOUNDARY (review-tools-code-round3 #2): BEFORE any
    runtime IO (no subprocess/HTTP is constructed), the ENTIRE local
    baseline must EXACTLY equal the independently sealed-derived
    frozen_baseline_dict — a tamper of only legacy.projection_sql (or
    any other field) with the cohort SHA unchanged is refused HERE,
    not post-hoc by the verifier; the executable plan is then built
    from the SEALED dict, never from the local file."""
    here = Path(__file__).resolve().parent
    sealed = sealed_baseline(here)
    local = read_local_baseline(here)
    if local != sealed:
        print("[p25-preflight] REFUSE: local p24-frozen-baseline.json "
              "!= independently sealed derivation — possible "
              "expectation tamper (e.g. SQL); refusing BEFORE any "
              "runtime IO", file=sys.stderr)
        return 2
    guard = load_guard(here)
    roster = guard.load_roster()
    frozen = sealed                       # sealed-derived, not local
    env_dir = Path(args.env_dir)
    plan = canonical_plan_dict(env_dir, roster, frozen)
    plan_sha = hashlib.sha256(
        json.dumps(plan, indent=1, sort_keys=False).encode()).hexdigest()
    attempt = Attempt(Path(args.out_dir))
    summary = execute_plan(plan["steps"], attempt, run_cli, http_get,
                           roster, frozen, plan_sha)
    print(f"[p25-preflight] attempt {attempt.dir} ok={summary['ok']} "
          f"failures={len(summary['failures'])}")
    return 0 if summary["ok"] else 1


def collect(args):
    if not RELEASED_FOR_EXECUTION:
        print("[p25-preflight] DRAFT: not released — root must review "
              "--plan + the foundation code/selftests and flip "
              "RELEASED_FOR_EXECUTION. No docker/psql/http subprocess "
              "was constructed.", file=sys.stderr)
        return 3
    return collect_body(args, real_run_cli, real_http_get)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--collect", action="store_true")
    ap.add_argument("--out-dir",
                    default=str(Path(__file__).resolve().parent /
                                "results" / "2026-11-14-p25-preflight"))
    ap.add_argument("--env-dir", default=ENV_DIR_DEFAULT)
    args = ap.parse_args(argv)
    if args.plan:
        print(plan_json(Path(args.env_dir)), end="")
        return 0
    if args.collect:
        return collect(args)
    print("usage: --plan | --collect", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
