#!/usr/bin/env python3
"""P2.4 READ-ONLY preflight collector (root-authorized 2026-11-14).

Collects the live runtime baseline into
  results/2026-11-14-p24-final/00-preflight/
WITHOUT running any test, switch, restart, build or DB write.

Whitelist (root message 2026-11-14) — nothing else executes:
  docker compose ps | docker inspect (connector/fixture facts) |
  docker volume ls | stat .env MODE ONLY |
  compose exec -T postgres psql SELECT-only |
  fixture HTTP GET /admin/src/<name> (counters)
Never printed anywhere: .env values, lease holder/token VALUES (presence
booleans only), JWTs.

Every command's raw stdout is preserved with its REAL rc; reruns append
-rN (history never overwritten). Reuses, never modifies, frozen tools:
  * guard ROW_SQL imported from p24_live_guard.py (exact same projection
    the guard will run at switch time)
  * P21_LEGACY_RUNS / PROJECTION_SQL / PROJECTION_KEYS imported from the
    frozen P2.2 readonly_baseline.py (byte-identical rendering)

Usage: python3 -I collect_preflight_p24.py [<env_dir>]
Exit 0 iff every collection step itself succeeded (content verdicts are
verify_preflight_p24.py's job).
"""

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
OUT_ROOT = HERE / "results" / "2026-11-14-p24-final" / "00-preflight"
PROJECT = "openfoundry-platform-p1-q3v9"
FIXTURE_BOOT_PIN = "2026-11-13T18:29:14.176785055Z"   # frozen final-fixture-facts.txt
EXPECTED_VOLUMES = {
    "openfoundry-platform-p1-q3v9_dataset-files",
    "openfoundry-platform-p1-q3v9_go-build-cache",
    "openfoundry-platform-p1-q3v9_postgres-data",
}
FIXTURE_SRCS = [  # all fixture src families the rounds created
    "p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab",
    "p21b-96351e3b", "p21c-96351e3b",
    "p21b-528455c2", "p21c-528455c2",
]


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def artifact(name):
    """Collision-safe output path: never overwrite a previous attempt."""
    p = OUT_ROOT / name
    n = 0
    while p.exists():
        n += 1
        p = OUT_ROOT / re.sub(r"(\.\w+)?$", rf"-r{n}\1", name, count=1)
    return p


def save_raw(name, proc, note=""):
    p = artifact(name)
    p.write_text(proc.stdout)
    (OUT_ROOT / f"{p.name}.rc").write_text(f"{proc.returncode}\n")
    (OUT_ROOT / f"{p.name}.err").write_text(proc.stderr)   # never discard
    if note:
        print(f"[preflight] {p.name} rc={proc.returncode} {note}")
    return proc.returncode, p


def run(cmd, timeout=120):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def main():
    env_dir = Path(sys.argv[1] if len(sys.argv) > 1 else
                   "/private/tmp/openfoundry-platform-p1-q3v9")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    manifest = {"client_date": time.strftime("%Y-%m-%d"),
                "env_dir": str(env_dir),
                "note": "read-only preflight; no start/stop/restart/build/"
                        "DB-write; no env values, tokens or holder values "
                        "anywhere in outputs",
                "steps": []}

    # --only <tag>: re-collect JUST the matching step (root 2026-11-14:
    # selective SELECT-only re-runs keep green history untouched)
    only = None
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1]

    def want(tag):
        return only is None or only in tag

    guard = load_module(HERE / "p24_live_guard.py", "p24_guard")
    base = load_module(HERE.parent / "business-platform-p2-2" /
                       "readonly_baseline.py", "p22_baseline")

    def compose(*args, timeout=120):
        return run(["docker", "compose", "-f", str(env_dir / "compose.yaml"),
                    "--project-name", PROJECT, *args], timeout)

    # 01 stack ps (base5 state) ------------------------------------------
    if want("01-"):
        rc, _ = save_raw("01-docker-ps.txt", compose("ps", "--all"))
        manifest["steps"].append({"step": "docker-ps", "rc": rc})

    # 02 connector facts ---------------------------------------------------
    if want("02-"):
        ps_c = compose("ps", "-q", "connector")
        cid, err = guard.pick_container(ps_c.stdout, ps_c.returncode)
        if err:
            print(f"[preflight] BLOCK: connector {err}", file=sys.stderr)
            return 1
        insp = run(["docker", "inspect", "--format",
                    "{{.State.StartedAt}}|{{.Image}}|{{.State.Running}}",
                    cid])
        rc, p = save_raw("02-inspect-connector.txt", insp,
                         note=insp.stdout.strip()[:100])
        manifest["steps"].append({"step": "inspect-connector", "rc": rc,
                                  "container": cid})   # FULL cid (root fix ①)

    # 03 fixture facts (boot/running/image/policy + PID1 entrypoint/cmd;
    # this fixture container has NO .Config.Path — Entrypoint/Cmd only)
    if want("03"):
        ps_f = compose("ps", "-q", "fixture")
        fid, err_f = guard.pick_container(ps_f.stdout, ps_f.returncode)
        if err_f:
            print(f"[preflight] BLOCK: fixture {err_f}", file=sys.stderr)
            return 1
        insp_fa = run(["docker", "inspect", "--format",
                       "{{.State.StartedAt}}|{{.State.Running}}|{{.Image}}|"
                       "{{.HostConfig.RestartPolicy.Name}}", fid])
        rc_a, _ = save_raw("03a-inspect-fixture-state.txt", insp_fa,
                           note=f"pin expects boot={FIXTURE_BOOT_PIN}")
        insp_fb = run(["docker", "inspect", "--format",
                       "{{json .Config.Entrypoint}}|{{json .Config.Cmd}}",
                       fid])
        rc_b, _ = save_raw("03b-inspect-fixture-pid1.txt", insp_fb,
                           note="expect Cmd [python3,-I,/srv/rest_source.py,…]")
        manifest["steps"].append({"step": "inspect-fixture",
                                  "rc": rc_a | rc_b,
                                  "pin_boot": FIXTURE_BOOT_PIN})

    # 04 volumes (names only) ----------------------------------------------
    if want("04-"):
        vols = run(["docker", "volume", "ls", "--format", "{{.Name}}"])
        rc, _ = save_raw("04-volumes.txt", vols,
                         note=f"expect>={len(EXPECTED_VOLUMES)} named volumes")
        manifest["steps"].append({"step": "volume-ls", "rc": rc})

    # 05 .env MODE only (no values) ----------------------------------------
    if want("05-"):
        mode = run(["stat", "-f", "%Lp", str(env_dir / ".env")])
        rc, _ = save_raw("05-env-mode.txt", mode,
                         note="600 expected; no values read")
        manifest["steps"].append({"step": "env-mode", "rc": rc})

    def psql_json(name, sql, note=""):
        proc = compose("exec", "-T", "postgres", "psql", "-U", "openfoundry",
                       "-d", "connectors", "-t", "-A", "-v",
                       "ON_ERROR_STOP=1", "-c", sql)
        rc, p = save_raw(name + ".txt", proc, note=note)
        lines = [l for l in proc.stdout.strip().splitlines() if l]
        rows = None
        try:
            rows = json.loads(lines[-1]) if lines else []
        except ValueError:
            pass
        if rows is not None:
            (OUT_ROOT / f"{p.stem}.json").write_text(
                json.dumps(rows, indent=1, sort_keys=True) + "\n")
        return rc, rows, p

    # 06 controlled pending/running — guard ROW_SQL verbatim ----------------
    if want("06-"):
        rc, rows, _ = psql_json(
            "06-controlled-24safe3org",
            guard.SESSION_SQL + guard.ROW_SQL,
            note="expect exactly the 4 pinned rows")
        manifest["steps"].append({"step": "controlled-24safe3org", "rc": rc,
                                  "rows": len(rows) if rows is not None
                                  else None})

    # 07 cron / timing sweep --------------------------------------------------
    sweep_sql = (
        "SELECT json_agg(row_to_json(t)) FROM ("
        " SELECT table_name, column_name FROM information_schema.columns"
        " WHERE table_schema='public' AND (column_name='schedule_cron'"
        "   OR column_name LIKE '%interval%'"
        "   OR column_name LIKE 'next_run%' OR column_name LIKE 'last_run%')"
        " ORDER BY table_name, column_name) t;")
    cols = []
    if want("07a"):
        rc, cols, _ = psql_json("07a-cron-column-sweep", sweep_sql,
                                note="catalog sweep")
        manifest["steps"].append({"step": "cron-column-sweep", "rc": rc})
    cron_nonempty = {}
    if cols and want("07-nonempty"):
        # one catalog probe for the identity columns each table ACTUALLY has
        tbl_cols = {}
        for c in cols:
            tbl = c["table_name"]
            if tbl not in tbl_cols:
                probe = ("SELECT json_agg(column_name) FROM "
                         "information_schema.columns WHERE table_schema='public'"
                         f" AND table_name='{tbl}'")
                rc_p, present, _ = psql_json(f"07c-columns-{tbl}", probe)
                tbl_cols[tbl] = set(present or [])
        for c in cols:
            tbl, col = c["table_name"], c["column_name"]
            have = tbl_cols.get(tbl, set())
            ident = "id::text id" if "id" in have else (
                "source_id::text source_id" if "source_id" in have
                else "ctid::text ctid")
            extra = ", org_id::text org_id" if "org_id" in have else ""
            if col == "schedule_cron":
                cond = "schedule_cron IS NOT NULL AND schedule_cron <> ''"
            else:
                cond = f'"{col}" IS NOT NULL'
            sql = (f'SELECT json_agg(row_to_json(t)) FROM (SELECT {ident}'
                   f'{extra}, "{col}" FROM "{tbl}" WHERE {cond}) t;')
            rc2, rows2, _ = psql_json(f"07-nonempty-{tbl}-{col}", sql)
            cron_nonempty[f"{tbl}.{col}"] = rows2 if rows2 is not None else "unparsed"
            manifest["steps"].append({"step": f"nonempty-{tbl}-{col}", "rc": rc2})
    # P23 admission interval (non-secret numeric). Root fix ③: the P23
    # contract stores it at sync.ADMISSION.min_interval_ms — the earlier
    # sync.min_interval_ms path was wrong and its outputs are not evidence.
    if want("07b"):
        rc, adm_rows, _ = psql_json(
            "07b-sync-admission-min-interval",
            "SELECT json_agg(row_to_json(t)) FROM (SELECT c.id::text, c.name,"
            " c.config->'sync'->'admission'->>'min_interval_ms'"
            " AS min_interval_ms FROM connections c"
            " WHERE c.config->'sync'->'admission'->>'min_interval_ms'"
            " IS NOT NULL) t;")
        manifest["steps"].append({"step": "sync-admission-min-interval",
                                  "rc": rc,
                                  "rows": len(adm_rows) if adm_rows else 0})

    # 08 legacy B/C — frozen P22 projection verbatim ---------------------------
    if want("08-"):
        for label, rid in base.P21_LEGACY_RUNS.items():
            rc, _, _ = psql_json(
                f"08-legacy-{label}",
                "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';\n"
                + base.PROJECTION_SQL.format(rid=rid),
                note="13-field frozen P22 projection")
            manifest["steps"].append({"step": f"legacy-{label}", "rc": rc})

    # 09 fixture counters (read-only GET) ---------------------------------------
    if not want("09-"):
        counts, ok_all = {}, True
    else:
        fx_base = os.environ.get("OF_P1_FIXTURE_BASE", "")
        if not fx_base:
            compose_txt = (env_dir / "compose.yaml").read_text()
            m = re.search(r"fixture:.*?ports:\s*\[?\s*[\"']?([0-9.]+:\d+):8090",
                          compose_txt, re.S)
            host = m.group(1) if m else "127.0.0.1:17090"
            fx_base = f"http://{host}"
        counts = {}
        ok_all = True
        for name in FIXTURE_SRCS:
            try:
                with urllib.request.urlopen(f"{fx_base}/admin/src/{name}",
                                            timeout=10) as resp:
                    snap = json.loads(resp.read())
                counts[name] = {k: snap.get(k) for k in
                                ("request_count", "inventory_started",
                                 "inventory_finished", "active", "total",
                                 "mode")}
            except Exception as exc:
                ok_all = False
                counts[name] = {"error": str(exc)[:150]}
        p = artifact("09-fixture-counts.json")
        p.write_text(json.dumps({"fixture_base": fx_base, "counts": counts},
                                indent=1, sort_keys=True) + "\n")
        (OUT_ROOT / f"{p.name}.rc").write_text("0\n" if ok_all else "1\n")
        print(f"[preflight] 09-fixture-counts rc={'0' if ok_all else '1'} "
              f"base={fx_base}")
        manifest["steps"].append({"step": "fixture-counts",
                                  "rc": 0 if ok_all else 1})

    # collection manifest: collision-safe (root ②: never overwrite history)
    mp = artifact("collection-manifest.json")
    mp.write_text(json.dumps(manifest, indent=1) + "\n")
    bad = [s for s in manifest["steps"] if s.get("rc") not in (0, None)]
    print(f"[preflight] steps={len(manifest['steps'])} bad_rc="
          f"{[s['step'] for s in bad]} manifest={mp.name}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
