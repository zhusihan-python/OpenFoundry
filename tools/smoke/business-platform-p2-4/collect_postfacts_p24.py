#!/usr/bin/env python3
"""P24 POST-ABC read-only collector — ONE attempt, ONE directory.

Review closure (2026-11-14): the final-state evidence must be bound to
THIS attempt — raw + derived + .rc + sha256 in a fresh dedicated dir
with an attempt manifest; the verifier never mixes per-stem "latest"
files from older attempts. Runs ONLY after P21 ABC (the last q3 write
in the locked fallback); everything here is read-only:
  docker ps/inspect/volume-ls, stat .env MODE only, SELECT-only psql,
  fixture counter GETs, Dataset-API GETs as the C run's real owner
  (token minted IN MEMORY from the loaded env — never printed/saved).

Collected (all for this attempt):
  01-05  stack/connector/fixture/volumes/env-mode facts
  06     ALL controlled pending/running (guard ROW_SQL, 27 fields)
  08     legacy b6ec49ab B/C (frozen P22 projection)
  09     fixture counters: legacy trio + this attempt's two ABC keys
  10a-c  ABC raw parent chain: the two attempt Sources, their Defs,
         and EVERY run of those Defs (27-field projection)
  11     C target live: file bound to the C run, txn status, lineage,
         content hash vs sha256(downloaded bytes)
  12a-c  owned final state by FRESH SQL: this round's exact owned Sync
         IDs (disabled/NULL cursor/cron), their grants (must be none),
         and the GLOBAL non-empty schedule_cron set
  attempt-manifest.json  LAST: every file's sha256 + rc + the input
         result files' sha256 (attempt binding)

Usage: python3 -I collect_postfacts_p24.py --abc-result <result_p21.json>
           --p24-result <result_p24-*.json> --out-dir <fresh dir>
           [--owned-history <p24-owned-history-v1.json>] [--env-dir <tmp>]

--owned-history (optional, retry-evidence support): the Tools-generated
exact global owned roster. schema=p24-owned-history-v1; attempts=[{result_
path:absolute archived step result, result_sha256, suffix}]; owned_sync_ids
= the SAFE-binding exact union INCLUDING failed attempts (a failed round's
disabled/NULL/terminal defs stay owned — never deleted to fit a global
set). The collector validates EVERY input file's actual sha256/suffix/
owned bindings, rebuilds the union and requires it to equal the declared
one, and requires the LATEST SUCCESS (this --p24-result, whose own
resultSHA stays the manifest's p24_result_sha256) to be among the attempts.
The union then drives 12a/12b; the manifest gains owned_history_sha256.
Nothing here relaxes the unknown/global-cron gates.

Exit 0 iff every collection step succeeded (verdicts are
verify_postfacts_p24.py's job). No secret ever reaches the outputs.
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
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = "openfoundry-platform-p1-q3v9"
P1_SMOKE = HERE.parent / "business-platform-p1" / "smoke" / "smoke.py"
LEGACY_FIXTURES = ["p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab"]


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_owned_history(hist_path, latest_result_path):
    """Validate a p24-owned-history-v1 roster and return (union, sha256).

    Fails closed: every attempt entry must name an ABSOLUTE archived result
    file whose actual bytes hash to result_sha256, whose parsed suffix
    matches, and whose owned_sync_ids are safe bindings; the rebuilt union
    must EQUAL the declared one; and the latest SUCCESS (the --p24-result
    passed alongside, byte-identical by path+sha) must be among the
    attempts. Failed attempts' defs are part of the union by design.
    """
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
    ap.add_argument("--abc-result", required=True)
    ap.add_argument("--p24-result", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--owned-history", default=None)
    ap.add_argument("--env-dir",
                    default="/private/tmp/openfoundry-platform-p1-q3v9")
    args = ap.parse_args()
    env_dir = Path(args.env_dir)
    out = Path(args.out_dir)
    if out.exists() and any(out.iterdir()):
        print(f"[postfacts-collect] BLOCK: out-dir not fresh: {out}",
              file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)

    abc = json.loads(Path(args.abc_result).read_text())
    p24 = json.loads(Path(args.p24_result).read_text())
    suffix = abc.get("suffix")
    if not suffix or not re.fullmatch(r"[0-9a-f]{6,16}", str(suffix)):
        print("[postfacts-collect] BLOCK: ABC result has no valid suffix",
              file=sys.stderr)
        return 1
    names = [f"p21-p21b-{suffix}", f"p21-c-{suffix}"]
    fx_keys = [f"p21b-{suffix}", f"p21c-{suffix}"]
    owned = p24.get("owned_sync_ids") or {}
    # Optional exact global roster (retry-evidence): validated BEFORE any
    # runtime work; on any inconsistency the collection refuses to start.
    hist_sha = None
    if args.owned_history:
        union, hist_sha, h_errs = load_owned_history(
            args.owned_history, args.p24_result)
        if h_errs:
            print("[postfacts-collect] BLOCK: owned-history invalid: "
                  + "; ".join(h_errs[:5]), file=sys.stderr)
            return 1
        owned = union
    owned_ids = sorted(owned)

    guard = load_module(HERE / "p24_live_guard.py", "p24_guard")
    base = load_module(HERE.parent / "business-platform-p2-2" /
                       "readonly_baseline.py", "p22_baseline")
    files = {}        # name -> {"sha256", "rc"}
    steps_bad = []

    def record(name, rc):
        p = out / name
        files[name] = {"sha256": sha(p) if p.exists() else None, "rc": rc}
        if rc != 0:
            steps_bad.append(name)

    def save(name, proc):
        (out / f"{name}.txt").write_text(proc.stdout)
        (out / f"{name}.txt.err").write_text(proc.stderr)
        (out / f"{name}.txt.rc").write_text(f"{proc.returncode}\n")
        record(f"{name}.txt", proc.returncode)
        # The ORIGINAL .err/.rc sidecars are manifest-bound too (review
        # item2): every consumed raw/rc/err file of THIS attempt carries its
        # own sha256, and the .rc file's bytes must restate the step rc so
        # the verifier can check original-rc==manifest-rc without trusting
        # either alone.
        record(f"{name}.txt.err", proc.returncode)
        record(f"{name}.txt.rc", 0 if f"{proc.returncode}\n" ==
               (out / f"{name}.txt.rc").read_text() else 1)
        return proc.returncode

    def run(cmd, timeout=120):
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout)

    def compose(*a):
        return run(["docker", "compose", "-f", str(env_dir / "compose.yaml"),
                    "--project-name", PROJECT, *a])

    def psql(name, sql):
        proc = compose("exec", "-T", "postgres", "psql", "-U",
                       "openfoundry", "-d", "connectors", "-t", "-A",
                       "-v", "ON_ERROR_STOP=1", "-c", sql)
        rc = save(name, proc)
        rows = None
        if rc == 0:
            lines = [l for l in proc.stdout.strip().splitlines() if l]
            try:
                rows = json.loads(lines[-1]) if lines else []
                if rows is None:
                    rows = []
            except ValueError:
                rows = None
        if rows is not None:
            (out / f"{name}.json").write_text(
                json.dumps(rows, indent=1, sort_keys=True) + "\n")
            record(f"{name}.json", 0)
        else:
            steps_bad.append(f"{name}.json(unparsed)")
        return rows

    # 01-05 facts ---------------------------------------------------------
    save("01-docker-ps", compose("ps", "--all"))
    for svc, tag in (("connector", "02-inspect-connector"),):
        ps = compose("ps", "-q", svc)
        cid, err = guard.pick_container(ps.stdout, ps.returncode)
        if err:
            print(f"[postfacts-collect] BLOCK: {svc} {err}", file=sys.stderr)
            return 1
        save(tag, run(["docker", "inspect", "--format",
                       "{{.State.StartedAt}}|{{.Image}}|{{.State.Running}}",
                       cid]))
    ps = compose("ps", "-q", "fixture")
    fid, err = guard.pick_container(ps.stdout, ps.returncode)
    if err:
        print(f"[postfacts-collect] BLOCK: fixture {err}", file=sys.stderr)
        return 1
    save("03a-inspect-fixture-state",
         run(["docker", "inspect", "--format",
              "{{.State.StartedAt}}|{{.State.Running}}|{{.Image}}|"
              "{{.HostConfig.RestartPolicy.Name}}", fid]))
    save("03b-inspect-fixture-pid1",
         run(["docker", "inspect", "--format",
              "{{json .Config.Entrypoint}}|{{json .Config.Cmd}}", fid]))
    save("04-volumes", run(["docker", "volume", "ls", "--format",
                            "{{.Name}}"]))
    save("05-env-mode", run(["stat", "-f", "%Lp", str(env_dir / ".env")]))

    # 06 controlled projection ------------------------------------------
    psql("06-controlled", guard.SESSION_SQL + guard.ROW_SQL)

    # 08 legacy B/C -------------------------------------------------------
    for label, rid in base.P21_LEGACY_RUNS.items():
        proc = compose("exec", "-T", "postgres", "psql", "-U",
                       "openfoundry", "-d", "connectors", "-t", "-A",
                       "-v", "ON_ERROR_STOP=1", "-c",
                       "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';\n"
                       + base.PROJECTION_SQL.format(rid=rid))
        save(f"08-legacy-{label}", proc)

    # 10 ABC raw parent chain ---------------------------------------------
    in_names = ", ".join(f"'{n}'" for n in names)
    psql("10a-abc-connections",
         "SELECT json_agg(row_to_json(t)) FROM (SELECT c.id::text, c.name,"
         " c.owner_id::text, c.org_id::text FROM connections c"
         f" WHERE c.name IN ({in_names}) ORDER BY c.name) t;")
    psql("10b-abc-defs",
         "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text,"
         " d.source_id::text, d.output_dataset_id::text, d.org_id::text"
         " FROM batch_sync_defs d JOIN connections c ON c.id = d.source_id"
         f" WHERE c.name IN ({in_names}) ORDER BY d.id) t;")
    tail = ("WHERE r.control_mode = 'controlled'\n"
            "    AND r.status IN ('pending','running')) t;")
    if tail not in guard.ROW_SQL:
        print("[postfacts-collect] BLOCK: guard ROW_SQL tail changed",
              file=sys.stderr)
        return 1
    runs_sql = guard.ROW_SQL.replace(
        tail, f"WHERE c.name IN ({in_names})) t;")
    abc_runs = psql("10c-abc-runs", guard.SESSION_SQL + runs_sql) or []
    conns = json.loads((out / "10a-abc-connections.json").read_text()) \
        if (out / "10a-abc-connections.json").exists() else []
    defs = json.loads((out / "10b-abc-defs.json").read_text()) \
        if (out / "10b-abc-defs.json").exists() else []

    # 09 fixture counters (legacy trio + this attempt's two keys) ----------
    compose_txt = (env_dir / "compose.yaml").read_text()
    m = re.search(r"fixture:.*?ports:\s*\[?\s*[\"']?([0-9.]+:\d+):8090",
                  compose_txt, re.S)
    fx_base = os.environ.get("OF_P1_FIXTURE_BASE") or (
        f"http://{m.group(1)}" if m else "http://127.0.0.1:17090")
    counts, fx_ok = {}, True
    for key in LEGACY_FIXTURES + fx_keys:
        try:
            with urllib.request.urlopen(f"{fx_base}/admin/src/{key}",
                                        timeout=10) as resp:
                snap = json.loads(resp.read())
            counts[key] = {k: snap.get(k) for k in
                           ("request_count", "inventory_started",
                            "inventory_finished", "active", "total",
                            "mode")}
        except Exception as exc:
            fx_ok = False
            counts[key] = {"error": str(exc)[:150]}
    (out / "09-fixture-counts.json").write_text(
        json.dumps({"fixture_base": fx_base, "attempt_keys": fx_keys,
                    "counts": counts}, indent=1, sort_keys=True) + "\n")
    record("09-fixture-counts.json", 0 if fx_ok else 1)

    # 11 C target live (GET-only, in-memory token) --------------------------
    c_live = {"ok": False}
    try:
        c_conn = next((c for c in conns if c["name"] == names[1]), None)
        c_def = next((d for d in defs
                      if c_conn and d["source_id"] == c_conn["id"]), None)
        c_run = next((r for r in abc_runs
                      if c_def and r["sync_def_id"] == c_def["id"]), None)
        if not (c_conn and c_def and c_run):
            c_live["error"] = "C parent chain incomplete"
        else:
            M = load_module(P1_SMOKE, "p1smoke")
            M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
            M.load_env(env_dir)               # in-memory only
            tok = M.mint_token(sub=c_conn["owner_id"],
                               org=c_conn.get("org_id") or None)
            target = c_def["output_dataset_id"]
            flist = M.list_files(tok, target)
            bound = [f for f in flist if c_run["id"] in json.dumps(f)]
            c_live["run_id"] = c_run["id"]
            c_live["target"] = target
            c_live["files_total"] = len(flist)
            c_live["bound_files"] = [f.get("id") for f in bound]
            if len(bound) == 1:
                txn_id = bound[0].get("transaction_id")
                txn = M.api("dataset", "ds_txn_get",
                            path_args={"id": target, "branch": "main",
                                       "txn": txn_id}, token=tok)
                meta = (txn.body or {}).get("metadata") or {}
                dl = M.download_file(tok, target, bound[0]["id"])
                digest = hashlib.sha256(dl.body).hexdigest() \
                    if dl.status == 200 else None
                c_live.update({
                    "txn_id": txn_id, "txn_http": txn.status,
                    "txn_status": (txn.body or {}).get("status"),
                    "lineage_run": (meta.get("lineage") or {}).get(
                        "sync_run_id"),
                    "content_hash": meta.get("content_hash"),
                    "download_http": dl.status, "sha256": digest,
                    "bytes": len(dl.body) if dl.status == 200 else None})
                c_live["ok"] = True
            else:
                c_live["error"] = f"bound files != 1 ({len(bound)})"
    except Exception as exc:                          # noqa: BLE001
        c_live["error"] = str(exc)[:200]
    (out / "11-c-target-live.json").write_text(
        json.dumps(c_live, indent=1, sort_keys=True) + "\n")
    record("11-c-target-live.json", 0 if c_live.get("ok") else 1)

    # 12 owned final state by FRESH SQL --------------------------------------
    if owned_ids:
        in_ids = ", ".join(f"'{i}'::uuid" for i in owned_ids)
        psql("12a-owned-defs",
             "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text,"
             " d.source_id::text, d.output_dataset_id::text,"
             " (d.schedule_enabled)::text AS enabled,"
             " (d.schedule_next_fire_at IS NULL)::text AS cursor_null,"
             " COALESCE(d.schedule_cron, '') AS cron"
             f" FROM batch_sync_defs d WHERE d.id IN ({in_ids})"
             " ORDER BY d.id) t;")
        psql("12b-owned-grants",
             "SELECT json_agg(row_to_json(t)) FROM (SELECT"
             " g.sync_def_id::text FROM sync_schedule_grants g"
             f" WHERE g.sync_def_id IN ({in_ids}) ORDER BY 1) t;")
    else:
        steps_bad.append("12a-owned-defs(no owned ids in p24 result)")
    psql("12c-global-nonempty-cron",
         "SELECT json_agg(row_to_json(t)) FROM (SELECT d.id::text"
         " FROM batch_sync_defs d WHERE d.schedule_cron IS NOT NULL"
         " AND d.schedule_cron <> '' ORDER BY d.id) t;")

    # Exact per-file whitelist the verifier consumes (review item2): every
    # raw .txt with its .err/.rc sidecars, every psql-derived .json, and the
    # two direct-observation JSONs. Built EXPLICITLY — a comprehension over
    # uniform extensions would list .json files steps like 01-05 never
    # produce.
    txt_steps = ["01-docker-ps", "02-inspect-connector",
                 "03a-inspect-fixture-state", "03b-inspect-fixture-pid1",
                 "04-volumes", "05-env-mode", "06-controlled",
                 "10a-abc-connections", "10b-abc-defs", "10c-abc-runs"]
    txt_steps += [f"08-legacy-{lbl}" for lbl in base.P21_LEGACY_RUNS]
    if owned_ids:
        txt_steps += ["12a-owned-defs", "12b-owned-grants",
                      "12c-global-nonempty-cron"]
    else:
        txt_steps += ["12c-global-nonempty-cron"]
    consumed = sorted(
        f"{n}{ext}" for n in txt_steps
        for ext in (".txt", ".txt.err", ".txt.rc")
    ) + sorted(
        f"{n}.json" for n in txt_steps
        if n not in ("01-docker-ps", "02-inspect-connector",
                     "03a-inspect-fixture-state",
                     "03b-inspect-fixture-pid1", "04-volumes",
                     "05-env-mode")
        and not n.startswith("08-legacy-")
    ) + ["09-fixture-counts.json", "11-c-target-live.json"]

    manifest = {"client_date": time.strftime("%Y-%m-%d"),
                "attempt_dir": out.name,
                "abc_suffix": suffix,
                "abc_result_sha256": sha(args.abc_result),
                "p24_result_sha256": sha(args.p24_result),
                "owned_history_sha256": hist_sha,
                "owned_ids": owned_ids,
                "files": files,
                "bad_steps": steps_bad,
                "consumed_by_verifier": consumed,
                "note": "read-only post-ABC collection; no token/secret "
                        "written anywhere"}
    (out / "attempt-manifest.json").write_text(
        json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    print(f"[postfacts-collect] files={len(files)} bad={steps_bad}")
    print(f"ATTEMPT_DIR={out}")
    return 1 if steps_bad else 0


if __name__ == "__main__":
    sys.exit(main())
