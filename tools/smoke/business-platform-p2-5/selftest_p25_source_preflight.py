#!/usr/bin/env python3
"""P2.5 foundation SELFTEST — source-hash + collector→verifier (pure).

Round-2 closures exercised here:
  * C1 drives the ACTUAL collector main/collect ENTRY (in-process flag
    flip + module-level real_run_cli/real_http_get replacement with
    mocks — no listener/network/DB);
  * C2 verifies the attempt through the INTERNAL verify_attempt with
    REAL canonical plan + toy-only c-file expectation (the sole
    off-runtime softening, injected into the pure function, never the
    CLI);
  * C3 proves the FORMAL verifier main is seal-anchored: run against
    the mocked attempt it derives expectations from the fixed P24 seal
    and REFUSES the mock C bytes (rc1 naming the sha gate);
  * C4 missing single artifact; C5 timeout honesty (actual entry);
  * C6 malformed counter fields actually flow through the content
    checker (active=False / request_count=None / missing inventory).
Mock SQL routing keys on REAL query semantics (the sealed-17 ANY
filter must be PRESENT in the SQL text or the mock refuses — the mock
cannot mask query-semantics drift).
"""

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
PASS, FAIL = [], []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  ok: {name}")
    else:
        FAIL.append(f"{name} {detail}")
        print(f"  FAIL: {name} {detail}")


def loadmod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def synth_tree(tmp: Path):
    root = tmp / "repo"
    for rel, files in {
        "services/connector-management-service": ["a.go", "b.sql",
                                                   "x_test.go"],
        "services/dataset-versioning-service": ["c.go"],
        "services/pipeline-build-service": ["d.sql"],
        "libs/scheduling-cron": ["e.go", "ignore.txt"],
    }.items():
        d = root / rel
        d.mkdir(parents=True)
        for f in files:
            (d / f).write_text(f"// {f}\n")
    (root / "go.mod").write_text("module t\n")
    (root / "go.sum").write_text("t h1:x\n")
    return root


def sh_source(mode_args):
    return subprocess.run(
        [sys.executable, "-I", str(HERE / "source_hash_p25.py"),
         *mode_args], capture_output=True, text=True, timeout=120)


def make_mocks(frozen, roster, mock_file_blob, counters_override=None):
    cg = frozen["content_gates"]
    owned_ids = sorted(d["id"] for d in cg["owned_fullstate"]["defs"])

    def run_cli(argv, timeout=120):
        j = " ".join(argv)
        if "compose" in j and " ps -q " in j:
            svc = argv[argv.index("-q") + 1]
            return 0, f"cid-{svc}\n", "", "ok"
        if "compose" in j and " ps -aq " in j:
            return 0, "", "", "ok"            # absent incl. stopped
        if argv[0] == "docker" and argv[1] == "ps" and "-aq" in argv \
                and any("label=com.docker.compose.service=" in a
                        for a in argv):
            return 0, "", "", "ok"            # label-filtered absence
        if argv[0] == "docker" and argv[1] == "inspect":
            fmt = argv[argv.index("--format") + 1]
            cid = argv[-1]
            if cid == "cid-fixture":
                fx = cg["fixture"]
                if "RestartPolicy" in fmt:
                    return 0, (f"{fx['boot']}|{fx['running']}|"
                               f"{fx['image']}|{fx['restart_policy']}\n"
                               ), "", "ok"
                return 0, f"null|{fx['pid1']}\n", "", "ok"
            if "StartedAt" in fmt:
                if "cid-connector" in cid:
                    return 0, (f"{roster.FINAL_ANCHOR}|"
                               f"{roster.FINAL_IMAGE}\n"), "", "ok"
                if "cid-dataset" in cid:
                    return 0, ("2026-11-14T07:11:20.303516546Z|"
                               f"{roster.DATASET_IMAGE}\n"), "", "ok"
                return 0, ("2026-11-14T07:11:20.883892088Z|"
                           f"{roster.PIPELINE_IMAGE}\n"), "", "ok"
        if argv[0] == "docker" and argv[1] == "volume":
            names = cg["volumes_expected_names"] + ["extra-1"]
            return 0, "\n".join(names) + "\n", "", "ok"
        if argv[0] == "stat":
            return 0, "600\n", "", "ok"
        if "psql" in j:
            sql = argv[-1]
            # semantics-honest routing: the SEALED-17 filter must be IN
            # the SQL text, or the mock refuses (cannot mask drift)
            has_any = "ANY (ARRAY[" in sql
            if "batch_sync_defs d WHERE" in sql and has_any:
                for i in owned_ids:
                    assert f"'{i}'" in sql, "owned filter id missing"
                return 0, json.dumps(
                    cg["owned_fullstate"]["defs"]) + "\n\n", "", "ok"
            if "sync_schedule_ticks" in sql and has_any:
                return 0, json.dumps(
                    cg["owned_fullstate"]["ticks"]) + "\n\n", "", "ok"
            if "succeeded','failed','aborted" in sql and has_any:
                return 0, json.dumps(
                    cg["owned_fullstate"]["runs"]) + "\n\n", "", "ok"
            if "sync_schedule_grants" in sql and has_any:
                return 0, "\n", "", "ok"
            if "FROM sync_schedule_grants" in sql and not has_any:
                return 0, "0\n\n", "", "ok"     # GLOBAL count (05b)
            if "schedule_cron,'') <> ''" in sql:
                return 0, json.dumps(
                    [{"id": i} for i in
                     cg["global_catalog"]["nonempty_cron_ids"]]) + \
                    "\n\n", "", "ok"
            if "FROM sync_runs r" in sql and "pending','running" in sql:
                rows = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
                return 0, json.dumps(rows) + "\n\n", "", "ok"
            if "pg_proc" in sql or "pg_trigger" in sql:
                return 0, "0\n\n", "", "ok"
            if "sync_runs WHERE id" in sql:
                lbl = "B" if "6d6dc56f" in sql else "C"
                line = cg["legacy"]["expected_lines"][
                    "B_lease_lost_unpublished" if lbl == "B"
                    else "C_committed_unconfirmed"]
                return 0, "SET\nSET\n" + line + "\n", "", "ok"
            return 1, "", f"unexpected sql {sql[:60]}", "ok"
        return 1, "", f"unexpected argv {j[:60]}", "ok"

    def http_get(url, token):
        if "/admin/src/" in url:
            name = url.rsplit("/", 1)[-1]
            body = dict(cg["protected_counters"]["expected"][name])
            body["name"] = name          # body self-identifies (#4)
            if counters_override and name in counters_override:
                body.update(counters_override[name])
            return 200, json.dumps(body).encode(), None
        if "/branches/main/transactions/" in url:
            cl = cg["c_live_sealed"]
            return 200, json.dumps({
                "id": cl["txn_id"], "dataset_id": cl["target"],
                "branch_name": "main", "status": "COMMITTED",
                "metadata": {"lineage": {
                    "sync_run_id": cl.get("lineage_run")}}}).encode(), \
                None
        if "/download" in url:
            return 200, mock_file_blob, None
        if url.endswith("/files"):
            cl = cg["c_live_sealed"]
            entry = {"id": cl["bound_files"][0],
                     "transaction_id": cl["txn_id"],
                     "logical_path": f"sync-{cl['lineage_run']}.ndjson"}
            return 200, json.dumps(
                {"files": [entry]}).encode(), None
        return 404, b"", None

    return run_cli, http_get


def main():
    print("[A] source_hash_p25 synthetic battery")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        root = synth_tree(tmp)
        base = tmp / "base.txt"
        r = sh_source(["emit", str(root), str(base)])
        check("A1 emit rc0", r.returncode == 0, r.stderr)
        check("A2 SOURCE_FILES=8", "SOURCE_FILES=8" in r.stdout, r.stdout)
        combined = [l for l in r.stdout.splitlines()
                    if l.startswith("SOURCE_HASH=")][0].split("=")[1]
        r = sh_source(["verify", "--repo", str(root), "--baseline",
                       str(base), "--expect-combined", combined])
        check("A3 verify identical rc0", r.returncode == 0
              and "VERIFY=OK" in r.stdout, r.stdout)
        (root / "services" / "dataset-versioning-service" / "c.go")\
            .write_text("// changed\n")
        r = sh_source(["verify", "--repo", str(root), "--baseline",
                       str(base)])
        check("A4 changed surfaced rc1", r.returncode == 1
              and "CHANGED" in r.stderr)
    sh = loadmod(HERE / "source_hash_p25.py", "sh_m")
    p24 = loadmod(HERE.parent / "business-platform-p2-4" /
                  "source_hash_p24.py", "sh24")
    check("A5 ROOTS/EXTRA == frozen P24", sh.ROOTS == p24.ROOTS
          and sh.EXTRA == p24.EXTRA)

    print("[B] plan shape + DRAFT gates")
    r = subprocess.run([sys.executable, "-I",
                        str(HERE / "readonly_preflight_p25.py"),
                        "--plan"], capture_output=True, text=True,
                       timeout=120)
    check("B1 --plan rc0", r.returncode == 0, r.stderr)
    plan_disp = json.loads(r.stdout)
    ids = {s["id"] for s in plan_disp["steps"]}
    check("B2 step ids incl global catalog",
          {"05-owned-fullstate", "05b-global-grants",
           "05c-global-nonempty-cron", "07-c-live",
           "09-protected-counters", "13-local-shas"} <= ids)
    check("B3 owned SQLs carry the sealed-17 ANY filter",
          all("ANY (ARRAY[" in json.dumps(s)
              for s in plan_disp["steps"]
              if s["id"] == "05-owned-fullstate"))
    check("B4 global controlled-6 stays WHOLE-SET",
          all("ANY" not in json.dumps(s)
              for s in plan_disp["steps"]
              if s["id"] == "04-controlled-rows"))
    aux = [s for s in plan_disp["steps"]
           if s["id"] == "03-aux-absent"]
    check("B4b aux absence uses exact project+service label filters "
          "(docker ps -aq; compose-file-independent)",
          len(aux) == 3 and all(
              s["argv"][0:3] == ["docker", "ps", "-aq"]
              and "label=com.docker.compose.project="
              "openfoundry-platform-p1-q3v9" in s["argv"]
              and any("label=com.docker.compose.service=" in a
                      for a in s["argv"])
              for s in aux))
    check("B5 no future CP table in any SQL",
          all("sync_def_checkpoints" not in json.dumps(s)
              for s in plan_disp["steps"]))
    # B6 (rewritten after the accidental-collection incident): the
    # collector flag is NOW legitimately released in production code —
    # refusal tests flip it IN-PROCESS and NEVER subprocess or invoke
    # real IO; a real collection must never run from a selftest.
    coll_b6 = loadmod(HERE / "readonly_preflight_p25.py", "coll_b6")
    was = coll_b6.RELEASED_FOR_EXECUTION
    calls = {"n": 0}

    def no_io(*a, **k):
        calls["n"] += 1
        raise AssertionError("real IO must never run in B6")
    coll_b6.RELEASED_FOR_EXECUTION = False
    ns = type("A", (), {"env_dir": coll_b6.ENV_DIR_DEFAULT,
                        "out_dir": "/nonexistent-b6"})()
    rc = coll_b6.collect(ns)          # gate refuses BEFORE any IO
    coll_b6.RELEASED_FOR_EXECUTION = was
    check("B6 unreleased gate refuses rc3 with ZERO IO constructed",
          rc == 3 and calls["n"] == 0)

    print("[C] ACTUAL entry + seal-anchored verification")
    coll = loadmod(HERE / "readonly_preflight_p25.py", "coll_m")
    ver = loadmod(HERE / "verify_preflight_p25.py", "ver_m")
    guard = loadmod(HERE / "p25_cohort_guard.py", "g_c")
    roster = guard.load_roster()
    frozen_real = json.loads(
        (HERE / "p24-frozen-baseline.json").read_text())

    mock_blob = (b"p25-mock-artifact\n" * 1600)[:22766]
    mock_sha = hashlib.sha256(mock_blob).hexdigest()
    frozen_toy = json.loads(json.dumps(frozen_real))
    frozen_toy["content_gates"]["c_live_sealed"]["bytes"] = 22766
    frozen_toy["content_gates"]["c_live_sealed"]["sha256"] = mock_sha

    run_cli, http_get = make_mocks(frozen_toy, roster, mock_blob)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        os.environ["OF_P25_C_TOKEN"] = "mock-token"
        # C1: the ACTUAL main/collect entry, flag flipped in-process,
        # module-level IO adapters replaced by mocks
        coll.RELEASED_FOR_EXECUTION = True
        coll.real_run_cli, coll.real_http_get = run_cli, http_get
        rc = coll.main(["--collect", "--out-dir", str(tmp / "att")])
        coll.RELEASED_FOR_EXECUTION = False
        att_dirs = sorted((tmp / "att").glob("attempt-*"))
        check("C1 actual --collect entry rc0", rc == 0 and att_dirs,
              f"rc={rc}")
        att = att_dirs[-1]
        summary = json.loads((att / "attempt-summary.json").read_text())
        check("C2a attempt-summary consumption ledger populated",
              isinstance(summary.get("artifacts"), dict)
              and len(summary["artifacts"]) > 30
              and all({"sha256", "bytes"} <= set(v)
                      for v in summary["artifacts"].values()))
        check("C2b canonical plan sha recorded",
              isinstance(summary.get("plan_sha256"), str)
              and len(summary["plan_sha256"]) == 64)
        sample = "01-connector-boot-image.inspect.out.stdout"
        disk_sha = hashlib.sha256(
            (att / sample).read_bytes()).hexdigest()
        check("C2c ledger sha matches disk",
              summary["artifacts"][sample]["sha256"] == disk_sha)

        # canonical plan: same derivation the formal verifier uses
        plan = coll.canonical_plan_dict(Path(coll.ENV_DIR_DEFAULT),
                                        roster, frozen_real)
        plan_sha = hashlib.sha256(json.dumps(
            plan, indent=1, sort_keys=False).encode()).hexdigest()
        check("C3a recorded plan sha == canonical",
              summary["plan_sha256"] == plan_sha)

        # C3b: INTERNAL verify_attempt happy (toy ONLY in the c-file
        # expectation inside the pure function — never the CLI)
        verdict = ver.verify_attempt(att, plan, frozen_toy, guard,
                                     roster, plan_sha)
        check("C3b internal verify_attempt all_ok",
              verdict["all_ok"], str(verdict["reasons"][:4]))

        # C4: FORMAL main is seal-anchored — the mock C bytes CANNOT
        # satisfy the seal-derived expectation → rc1 naming the gate
        r = subprocess.run([sys.executable, "-I",
                            str(HERE / "verify_preflight_p25.py"),
                            "--attempt-dir", str(att)],
                           capture_output=True, text=True, timeout=120)
        check("C4 formal main refuses mock C bytes (seal anchor)",
              r.returncode == 1
              and "C file sha256" in r.stdout, r.stdout[-300:])

        # C5: missing single artifact negative via the internal path
        import shutil
        copy = tmp / "att-missing"
        shutil.copytree(att, copy)
        (copy / "05-owned-fullstate.owned-ticks.json.stdout").unlink()
        verdict = ver.verify_attempt(copy, plan, frozen_toy, guard,
                                     roster, plan_sha)
        check("C5 missing artifact fails and is named",
              (not verdict["all_ok"])
              and any("owned-ticks.json.stdout MISSING" in x
                      for x in verdict["reasons"]),
              str(verdict["reasons"][:3]))

        # C6: timeout honesty through the ACTUAL entry
        def cli_timeout(argv, timeout=120):
            if argv[0] == "docker" and argv[1] == "volume":
                return None, "partial-stdout-line\n", "", "timeout"
            return run_cli(argv, timeout)
        coll.RELEASED_FOR_EXECUTION = True
        coll.real_run_cli = cli_timeout
        rc = coll.main(["--collect", "--out-dir", str(tmp / "att2")])
        coll.RELEASED_FOR_EXECUTION = False
        att2 = sorted((tmp / "att2").glob("attempt-*"))[-1]
        rc_f = att2 / "10-volumes.names.txt.rc"
        wrap_f = att2 / "10-volumes.names.txt.wrapper"
        out_f = att2 / "10-volumes.names.txt.stdout"
        check("C6a timeout attempt rc1 + evidence retained",
              rc == 1 and rc_f.read_text().strip() == "unknown"
              and wrap_f.read_text().strip() == "timeout"
              and "partial-stdout-line" in out_f.read_text())
        verdict = ver.verify_attempt(att2, plan, frozen_toy, guard,
                                     roster, plan_sha)
        check("C6b verifier refuses timeout attempt",
              (not verdict["all_ok"])
              and any("10-volumes" in x for x in verdict["reasons"]))

        # C7: malformed counter fields actually hit the content checker
        bad = {"p21b-1020a565": {"active": False},
               "p21c-1020a565": {"request_count": None},
               "p21b-528455c2": {"inventory_finished": "2"}}
        run_cli2, http_get2 = make_mocks(frozen_toy, roster, mock_blob,
                                         counters_override=bad)
        coll.RELEASED_FOR_EXECUTION = True
        coll.real_run_cli, coll.real_http_get = run_cli2, http_get2
        rc = coll.main(["--collect", "--out-dir", str(tmp / "att3")])
        coll.RELEASED_FOR_EXECUTION = False
        att3 = sorted((tmp / "att3").glob("attempt-*"))[-1]
        verdict = ver.verify_attempt(att3, plan, frozen_toy, guard,
                                     roster, plan_sha)
        reasons = " | ".join(verdict["reasons"])
        check("C7 malformed counters rejected by content checker",
              (not verdict["all_ok"])
              and "active" in reasons
              and "request_count" in reasons
              and "inventory_finished" in reasons,
              reasons[:300])

        # C8: pre-execution boundary — ONLY the SQL tampered, cohort
        # sha unchanged ⇒ refusal with ZERO runtime IO calls
        calls = {"cli": 0, "http": 0}

        def cli_count(argv, timeout=120):
            calls["cli"] += 1
            return run_cli(argv, timeout)

        def http_count(url, token):
            calls["http"] += 1
            return http_get(url, token)
        tampered = json.loads(json.dumps(frozen_real))
        tampered["content_gates"]["legacy"]["projection_sql"] = \
            "SELECT evil FROM sync_runs; -- {rid}"
        orig_reader = coll.read_local_baseline
        coll.read_local_baseline = lambda here: tampered
        rc = coll.collect_body(
            type("A", (), {"env_dir": coll.ENV_DIR_DEFAULT,
                           "out_dir": str(tmp / "att-tamper")})(),
            cli_count, http_count)
        coll.read_local_baseline = orig_reader
        check("C8 SQL-only baseline tamper refused pre-IO with ZERO "
              "runtime calls", rc == 2 and calls == {"cli": 0,
                                                     "http": 0})

        # C9 (#3): ledger must catch rc/status/steps/cmd tampering
        import copy as _copy
        base_dir = att
        # a) .rc file altered after collection
        c9a = tmp / "att-c9a"
        shutil.copytree(base_dir, c9a)
        (c9a / "10-volumes.names.txt.rc").write_text("0\n\n")
        v = ver.verify_attempt(c9a, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C9a altered .rc caught (ledger original_rc mismatch)",
              (not v["all_ok"]) and any(
                  "original_rc" in x or "actual .rc" in x
                  for x in v["reasons"]), str(v["reasons"][:2]))
        # b) steps_recorded dropped one canonical base
        c9b = tmp / "att-c9b"
        shutil.copytree(base_dir, c9b)
        summ = json.loads((c9b / "attempt-summary.json").read_text())
        summ["steps_recorded"] = summ["steps_recorded"][:-1]
        (c9b / "attempt-summary.json").write_text(json.dumps(
            summ, indent=1, sort_keys=True))
        v = ver.verify_attempt(c9b, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C9b incomplete steps_recorded caught",
              (not v["all_ok"]) and any(
                  "steps_recorded" in x for x in v["reasons"]))
        # c) counter cmd text tampered
        c9c = tmp / "att-c9c"
        shutil.copytree(base_dir, c9c)
        (c9c / "09-protected-counters.read1.json.cmd").write_text(
            "GET (http) something-else\n")
        v = ver.verify_attempt(c9c, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C9c non-canonical counter cmd caught",
              (not v["all_ok"]) and any("cmd" in x
                                        for x in v["reasons"]))
        # d) attempt_dir mismatch
        c9d = tmp / "att-c9d"
        shutil.copytree(base_dir, c9d)
        summ = json.loads((c9d / "attempt-summary.json").read_text())
        summ["attempt_dir"] = "/not/this/dir"
        (c9d / "attempt-summary.json").write_text(json.dumps(
            summ, indent=1, sort_keys=True))
        v = ver.verify_attempt(c9d, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C9d attempt_dir mismatch caught",
              (not v["all_ok"]) and any("attempt_dir" in x
                                        for x in v["reasons"]))

        # C9e: attempt_dir missing / null must FAIL (round3 residual)
        c9e = tmp / "att-c9e"
        shutil.copytree(base_dir, c9e)
        summ = json.loads((c9e / "attempt-summary.json").read_text())
        summ.pop("attempt_dir", None)
        (c9e / "attempt-summary.json").write_text(json.dumps(
            summ, indent=1, sort_keys=True))
        v = ver.verify_attempt(c9e, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C9e missing attempt_dir caught",
              (not v["all_ok"]) and any("attempt_dir" in x
                                        for x in v["reasons"]))
        c9f = tmp / "att-c9f"
        shutil.copytree(base_dir, c9f)
        summ = json.loads((c9f / "attempt-summary.json").read_text())
        summ["attempt_dir"] = None
        (c9f / "attempt-summary.json").write_text(json.dumps(
            summ, indent=1, sort_keys=True))
        v = ver.verify_attempt(c9f, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C9f null attempt_dir caught", (not v["all_ok"]))

        # C4c: BOTH reads' raw evidence distinct and present (18 raw
        # sets: 9 srcs x 2 reads x url/status/body)
        n_raw = sum(1 for f in base_dir.iterdir()
                    if f.name.startswith("09-protected-counters.")
                    and f.name.endswith((".url", ".http_status",
                                         ".body")))
        check("C4c 54 read-tagged raw files (2x9x3), read1 intact "
              "after read2", n_raw == 54
              and (base_dir /
                   "09-protected-counters.read1.p21b-1020a565.body")
                  .is_file()
              and (base_dir /
                   "09-protected-counters.read2.p21b-1020a565.body")
                  .is_file())

        # C10 (#4): raw counter chain replay catches a swapped body
        c10 = tmp / "att-c10"
        shutil.copytree(base_dir, c10)
        swap = ("p21b-1020a565")
        bad = dict(frozen_toy["content_gates"]
                   ["protected_counters"]["expected"][swap])
        bad["request_count"] = 999
        bad["name"] = swap
        (c10 / "09-protected-counters.read2."
              f"{swap}.body").write_text(json.dumps(bad))
        v = ver.verify_attempt(c10, plan, frozen_toy, guard, roster,
                               plan_sha)
        check("C10 swapped raw counter body caught by replay+sha",
              (not v["all_ok"]) and any(
                  "bodies_sha256" in x or "replay" in x
                  for x in v["reasons"]), str(v["reasons"][:2]))

        # D-layer: supplement adjudicator negatives (SAVED real
        # evidence copied to tmp and TAMPERED — CLI-level, no live)
        REAL_SUPP = Path("/Volumes/ExternalSD/projects/OpenFoundry/"
                         "tools/smoke/business-platform-p2-5/results/"
                         "2026-11-14-p25-preflight/c-supplement/"
                         "attempt-1")
        REAL_BASE = Path("/Volumes/ExternalSD/projects/OpenFoundry/"
                         "tools/smoke/business-platform-p2-5/results/"
                         "2026-11-14-p25-preflight/attempt-2/collect/"
                         "attempt-1")
        if REAL_SUPP.is_dir() and REAL_BASE.is_dir():
            import shutil as _sh
            supp_plan = plan

            def vfy(supp_dir, base_dir):
                r = subprocess.run(
                    [sys.executable, "-I",
                     str(HERE / "verify_preflight_p25.py"),
                     "--attempt-dir", str(base_dir),
                     "--c-supplement", str(supp_dir)],
                    capture_output=True, text=True, timeout=180)
                try:
                    return r.returncode, json.loads(r.stdout)
                except ValueError:
                    return r.returncode, {}

            with tempfile.TemporaryDirectory() as td2:
                t2 = Path(td2)
                # d1: body tampered without ledger update
                d1 = t2 / "supp-tamper"
                _sh.copytree(REAL_SUPP, d1)
                (d1 / "07-c-live.txn.json.body").write_text(
                    '{"transaction": {"status": "COMMITTED"}}')
                rc, v = vfy(d1, REAL_BASE)
                check("D1 raw body tamper (no ledger update) refused",
                      rc == 1 and any("ledger bytes/sha != raw" in x
                                      for x in v.get("reasons", [])),
                      str(v.get("reasons", [])[:2]))
                # d2: different base binding
                d2 = t2 / "supp-otherbase"
                _sh.copytree(REAL_SUPP, d2)
                summ = json.loads((d2 / "attempt-summary.json")
                                  .read_text())
                summ["supplements_base_attempt"] = "/not/the/base"
                (d2 / "attempt-summary.json").write_text(json.dumps(
                    summ, indent=1, sort_keys=True))
                rc, v = vfy(d2, REAL_BASE)
                check("D2 different-base binding refused", rc == 1
                      and any("binds base" in x
                              for x in v.get("reasons", [])))
                # d3: actor swapped
                d3 = t2 / "supp-actor"
                _sh.copytree(REAL_SUPP, d3)
                summ = json.loads((d3 / "attempt-summary.json")
                                  .read_text())
                summ["actor_identity"] = {
                    "sub": "00000000-0000-4000-8000-0000000000ff",
                    "org_id": "00000000-0000-4000-8000-0000000000ee"}
                (d3 / "attempt-summary.json").write_text(json.dumps(
                    summ, indent=1, sort_keys=True))
                rc, v = vfy(d3, REAL_BASE)
                check("D3 non-sealed actor refused", rc == 1 and any(
                    "actor" in x.lower() for x in v.get("reasons", [])))
                # d4: unknown extra file in supplement
                d4 = t2 / "supp-extra"
                _sh.copytree(REAL_SUPP, d4)
                (d4 / "zz-stray.txt").write_text("x\n")
                rc, v = vfy(d4, REAL_BASE)
                check("D4 supplement unknown extra refused", rc == 1
                      and any("UNKNOWN file" in x
                              for x in v.get("reasons", [])))
                # d5: base 07 with 500 (not the canonical 404) must
                # NOT be supersededable
                d5b = t2 / "base-500"
                _sh.copytree(REAL_BASE, d5b)
                (d5b / "07-c-live.txn.json.http_status").write_text(
                    "500\n")
                summ = json.loads((d5b / "attempt-summary.json")
                                  .read_text())
                summ["artifacts"]["07-c-live.txn.json.http_status"][
                    "original_rc"] = "500"
                (d5b / "attempt-summary.json").write_text(json.dumps(
                    summ, indent=1, sort_keys=True))
                # rebind a supplement COPY to the tampered base copy
                # (path + fresh summary sha) so the narrow 500 rule is
                # actually reached — the binding defenses alone must
                # not be the only thing that refuses
                d5s = t2 / "supp-500rebind"
                _sh.copytree(REAL_SUPP, d5s)
                bsha = hashlib.sha256(
                    (d5b / "attempt-summary.json").read_bytes()
                ).hexdigest()
                summ5 = json.loads((d5s / "attempt-summary.json")
                                   .read_text())
                summ5["supplements_base_attempt"] = str(d5b)
                summ5["attempt_dir"] = str(d5s)
                (d5s / "attempt-summary.json").write_text(json.dumps(
                    summ5, indent=1, sort_keys=True))
                (d5s / "base-binding.json").write_text(json.dumps(
                    {"base_attempt": str(d5b),
                     "base_summary_sha256": bsha,
                     "supplement_summary_sha256": hashlib.sha256(
                         (d5s / "attempt-summary.json").read_bytes()
                     ).hexdigest()}, indent=1))
                # recompute ledger sha for the tampered base .rc? the
                # base ledger self-consistency: its summary now records
                # original_rc 500 matching file 500 → ledger passes to
                # the narrow rule
                rc, v = vfy(d5s, d5b)
                check("D5 base 500 NOT supersededable (narrow rule)",
                      rc == 1 and any("http status" in x
                                      for x in v.get("reasons", [])),
                      str(v.get("reasons", [])[:2]))
                # d7 (narrow-1): ledger entries deleted (8 of 9)
                d7 = t2 / "supp-ledgercut"
                _sh.copytree(REAL_SUPP, d7)
                summ = json.loads((d7 / "attempt-summary.json")
                                  .read_text())
                keep = "07-c-live.txn.json.body"
                summ["artifacts"] = {keep: summ["artifacts"][keep]}
                (d7 / "attempt-summary.json").write_text(json.dumps(
                    summ, indent=1, sort_keys=True))
                rc, v = vfy(d7, REAL_BASE)
                check("D7 8-of-9 ledger deletion refused (exact 9 "
                      "keyset)", rc == 1 and any(
                          "ledger entry MISSING" in x
                          for x in v.get("reasons", [])),
                      str(v.get("reasons", [])[:2]))
                # d8 (narrow-2): base URL tampered while statuses are
                # the canonical 404s — base URL check must still fire
                d8b = t2 / "base-urltamper"
                _sh.copytree(REAL_BASE, d8b)
                (d8b / "07-c-live.txn.json.url").write_text(
                    "GET http://evil.example/txn\n")
                summ = json.loads((d8b / "attempt-summary.json")
                                  .read_text())
                summ["attempt_dir"] = str(d8b)
                art = summ["artifacts"]["07-c-live.txn.json.url"]
                import hashlib as _hl
                art["sha256"] = _hl.sha256(
                    (d8b / "07-c-live.txn.json.url").read_bytes()
                ).hexdigest()
                (d8b / "attempt-summary.json").write_text(json.dumps(
                    summ, indent=1, sort_keys=True))
                d8s = t2 / "supp-urlrebind"
                _sh.copytree(REAL_SUPP, d8s)
                summ = json.loads((d8s / "attempt-summary.json")
                                  .read_text())
                summ["attempt_dir"] = str(d8s)
                summ["supplements_base_attempt"] = str(d8b)
                (d8s / "attempt-summary.json").write_text(json.dumps(
                    summ, indent=1, sort_keys=True))
                (d8s / "base-binding.json").write_text(json.dumps(
                    {"base_attempt": str(d8b),
                     "base_summary_sha256": _hl.sha256(
                         (d8b / "attempt-summary.json")
                         .read_bytes()).hexdigest(),
                     "supplement_summary_sha256": _hl.sha256(
                         (d8s / "attempt-summary.json")
                         .read_bytes()).hexdigest()},
                    indent=1))
                rc, v = vfy(d8s, d8b)
                check("D8 base URL tamper refused even under 404 "
                      "supersession", rc == 1 and any(
                          "url(base)" in x or "canonical" in x
                          for x in v.get("reasons", [])),
                      str(v.get("reasons", [])[:3]))
                # d9 (narrow-3): sibling binding tampered (supplement
                # sha field) — full-sibling validation must refuse
                d9 = t2 / "supp-bindtamper"
                _sh.copytree(REAL_SUPP, d9)
                bind = json.loads((d9 / "base-binding.json").read_text())
                bind["supplement_summary_sha256"] = "f" * 64
                (d9 / "base-binding.json").write_text(json.dumps(
                    bind, indent=1, sort_keys=True))
                rc, v = vfy(d9, REAL_BASE)
                check("D9 binding sibling supplement-sha tamper "
                      "refused", rc == 1 and any(
                          "supplement summary sha mismatch" in x
                          for x in v.get("reasons", [])),
                      str(v.get("reasons", [])[:2]))
                # d6: honest positive control — untouched copies pass
                rc, v = vfy(REAL_SUPP, REAL_BASE)
                check("D6 saved evidence still ALL_OK (positive)",
                      rc == 0 and v.get("all_ok") is True
                      and v.get("c_supplement", {})
                      .get("binding_sha256"))
        else:
            print("  skip: D-layer (saved preflight evidence not "
                  "present)")

    print(f"\n[selftest-p25-source-preflight] pass={len(PASS)} "
          f"fail={len(FAIL)}")
    for f in FAIL:
        print(f"  FAILED: {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
