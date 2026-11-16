#!/usr/bin/env python3
"""P25 RUNTIME-TOOLS pure/CLI selftest (round3,
review-runtime-tools-round2 #6) — covers ONLY Tools-owned files:
tail roster, owned-quiesce, running-images, source candidate hash,
adapter/shim, driver gating. Every negative hits the ACTUAL CLI /
adapter.main / shim path (subprocess or in-process main with module
flags flipped — never a hand-fed internal toy). No runtime, no
listener, no docker/psql/HTTP side effects (live entries refuse rc3).
"""

import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
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


def run_cli(argv):
    return subprocess.run([sys.executable, "-I"] + argv,
                          capture_output=True, text=True, timeout=180)


def U(h, v="4"):
    return f"{h * 8}-{h * 4}-{v}{h * 3}-8{h * 3}-{h * 12}"


def full27(rid, def_id, source_id, gate="false", status="pending"):
    keys = loadmod(HERE / "p25_cohort_guard.py",
                   "g_schema").expected_schema()
    row = {k: "" for k in keys}
    row.update({"id": rid, "sync_def_id": def_id,
                "source_id": source_id, "status": status,
                "control_mode": "controlled", "gate_entered": gate,
                "has_holder": "false", "has_token": "false",
                "started_at": "2026-11-14 08:00:00",
                "attempt": "1", "generation": "1",
                "has_hash": "false", "has_version": "false"})
    return row


TAIL_SPEC = {
    "run_rows": [
        {"tag": "p25-tail-x", "semantics": "p25-drop-noack",
         "count": 1, "evidence_kind": "commit-dispatched-noack"},
        {"tag": "p25-tail-y", "semantics": "B-lease-loss",
         "count": 1, "evidence_kind": "lease-expired"}],
    "owner_rows": [
        {"tag": "own-x", "def_id": U("1"),
         "run_ref": "terminal-failed",
         "blocked_reason": "awaiting_p26_resolution"}],
    "produced_by": "p25-tail"}


def tail_fixture(tmp):
    """declare via CLI into tmp; return (decl_path, runs, owners,
    terminal, bound)."""
    spec_path = tmp / "spec.json"
    spec_path.write_text(json.dumps(TAIL_SPEC))
    decl_path = tmp / "decl.json"
    r = run_cli([str(HERE / "p25_tail_roster.py"), "declare",
                 "--spec", str(spec_path), "--out", str(decl_path)])
    return r, decl_path


def write_tail_evidence(tmp, gate_y="false"):
    d1, s1 = U("1"), U("2")
    d2, s2 = U("3"), U("4")
    t0, t1 = U("a"), U("b")
    runs = [full27(t0, d1, s1, gate="true"),
            full27(t1, d2, s2, gate=gate_y)]
    owners = [{"def_id": d1, "run_id": U("c"),
               "run_status": "failed",
               "blocked_reason": "awaiting_p26_resolution"}]
    terminal = [{"id": U("c"), "sync_def_id": d1, "status": "failed"}]
    bound = {"p25-tail-x": {"def_id": d1, "source_id": s1,
                            "run_id": t0},
             "p25-tail-y": {"def_id": d2, "source_id": s2,
                            "run_id": t1}}
    (tmp / "runs.json").write_text(json.dumps(runs))
    (tmp / "owners.json").write_text(json.dumps(owners))
    (tmp / "terminal.json").write_text(json.dumps(terminal))
    (tmp / "bound.json").write_text(json.dumps(bound))
    return runs, owners, terminal, bound


def tail_verify_cli(tmp, decl_path, runs="runs.json",
                    owners="owners.json", terminal="terminal.json",
                    bound="bound.json"):
    argv = [str(HERE / "p25_tail_roster.py"), "verify",
            "--declaration", str(decl_path)]
    for flag, name in (("--runs", runs), ("--owners", owners),
                       ("--terminal-runs", terminal),
                       ("--bound", bound)):
        if name:
            argv += [flag, str(tmp / name)]
    return run_cli(argv)


def main():
    tr = loadmod(HERE / "p25_tail_roster.py", "tr")
    qz = loadmod(HERE / "check_owned_quiesced_p25.py", "qz")
    im = loadmod(HERE / "check_running_images_p25.py", "im")
    ad = loadmod(HERE / "p25_old_suite_adapter.py", "ad")
    sh = loadmod(HERE / "p23_q_guard_shim_p25.py", "sh")

    print("[A] tail roster — declare + the ONE authoritative verifier "
          "(actual CLI)")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        r, decl_path = tail_fixture(tmp)
        check("A1 declare ok + sha-frozen (CLI rc0)", r.returncode == 0
              and json.loads(decl_path.read_text())["ok"]
              and len(json.loads(decl_path.read_text())
                      ["declaration_sha256"]) == 64)
        bad_spec = dict(TAIL_SPEC, run_rows=[
            {"tag": "x", "semantics": "weird", "count": 1}])
        (tmp / "bad-spec.json").write_text(json.dumps(bad_spec))
        r = run_cli([str(HERE / "p25_tail_roster.py"), "declare",
                     "--spec", str(tmp / "bad-spec.json"),
                     "--out", str(tmp / "bad-decl.json")])
        check("A2 unknown semantics refused", r.returncode != 0)
        rb_spec = json.loads(json.dumps(TAIL_SPEC))
        rb_spec["run_rows"][0]["evidence_kind"] = "local-pg-rollback"
        (tmp / "rb-spec.json").write_text(json.dumps(rb_spec))
        r = run_cli([str(HERE / "p25_tail_roster.py"), "declare",
                     "--spec", str(tmp / "rb-spec.json"),
                     "--out", str(tmp / "rb-decl.json")])
        rb_reasons = ""
        if (tmp / "rb-decl.json").is_file():
            rb_reasons = " ".join(json.loads(
                (tmp / "rb-decl.json").read_text())["reasons"])
        check("A2b h12 local-PG-rollback evidence CANNOT serve a "
              "C-family lost-ACK tag (atomic proof ≠ lost COMMIT ACK)",
              r.returncode != 0
              and "evidence_kind" in rb_reasons)
        write_tail_evidence(tmp)
        r = tail_verify_cli(tmp, decl_path)
        check("A4 full positive via CLI (27-key rows, terminal-failed "
              "owner in TERMINAL evidence, not the nonterminal set)",
              r.returncode == 0, r.stdout[-200:] + r.stderr[-200:])
        # A3: 2-field toy run rows through the ACTUAL CLI
        (tmp / "toy-runs.json").write_text(json.dumps(
            [{"id": U("a"), "status": "pending"},
             {"id": U("b"), "status": "pending"}]))
        r = tail_verify_cli(tmp, decl_path, runs="toy-runs.json")
        check("A3 2-field toy run rows REFUSED by the actual CLI "
              "(27-key schema + binding mandatory)",
              r.returncode != 0 and "27-key" in (r.stdout + r.stderr))
        # B3: a def_tag STRING placed in a UUID field
        bad_bound = json.loads((tmp / "bound.json").read_text())
        bad_bound["p25-tail-y"]["def_id"] = "p25-tail-y"
        (tmp / "tag-bound.json").write_text(json.dumps(bad_bound))
        r = tail_verify_cli(tmp, decl_path, bound="tag-bound.json")
        check("B3 tag string in a UUID field REFUSED via actual CLI",
              r.returncode != 0 and "not a UUID" in (r.stdout
                                                     + r.stderr))
        # empty bound map cannot pass
        (tmp / "empty-bound.json").write_text("{}")
        r = tail_verify_cli(tmp, decl_path, bound="empty-bound.json")
        check("A5 empty bound map fails (binding MANDATORY)",
              r.returncode != 0)
        # one run satisfying two tags
        two = json.loads((tmp / "bound.json").read_text())
        two["p25-tail-y"]["run_id"] = two["p25-tail-x"]["run_id"]
        (tmp / "two-bound.json").write_text(json.dumps(two))
        r = tail_verify_cli(tmp, decl_path, bound="two-bound.json")
        check("A6 one run satisfying two tags refused",
              r.returncode != 0 and "two tags" in (r.stdout
                                                   + r.stderr))
        # tampered declaration sha
        tampered = json.loads(decl_path.read_text())
        tampered["spec"]["run_rows"][0]["count"] = 5
        (tmp / "tampered-decl.json").write_text(json.dumps(tampered))
        r = tail_verify_cli(tmp, tmp / "tampered-decl.json")
        check("A8 tampered frozen declaration refused (sha recompute)",
              r.returncode != 0 and "tampered" in (r.stdout
                                                   + r.stderr))
        # terminal-failed owner WITHOUT terminal evidence
        r = tail_verify_cli(tmp, decl_path, terminal=None)
        check("A9 missing terminal evidence for terminal-failed owner "
              "fails closed", r.returncode != 0)
        # stray extra nonterminal run
        runs = json.loads((tmp / "runs.json").read_text())
        runs.append(full27(U("e"), U("9"), U("8")))
        (tmp / "stray-runs.json").write_text(json.dumps(runs))
        r = tail_verify_cli(tmp, decl_path, runs="stray-runs.json")
        check("A10 undeclared stray nonterminal run blocks",
              r.returncode != 0 and "stray" in (r.stdout + r.stderr))
        # unknown owner rows never adopted silently
        owners = json.loads((tmp / "owners.json").read_text())
        owners.append({"def_id": U("f"), "run_id": U("d"),
                       "run_status": "failed",
                       "blocked_reason": "awaiting_p26_resolution"})
        (tmp / "extra-owners.json").write_text(json.dumps(owners))
        r = tail_verify_cli(tmp, decl_path, owners="extra-owners.json")
        check("A11 owner EXACT set — unknown extra owner blocks",
              r.returncode != 0 and "owner def set" in (r.stdout
                                                        + r.stderr))
        # compatibility wrapper is strict now
        decl = json.loads(decl_path.read_text())
        bound = json.loads((tmp / "bound.json").read_text())
        rows = json.loads((tmp / "runs.json").read_text())
        biv = tr.verify_bound_identities(decl, bound, rows, [])
        check("A12 verify_bound_identities wrapper keeps binding "
              "mandatory (ok case)", biv["ok"], str(biv["reasons"]))
        biv = tr.verify_bound_identities(decl, {}, rows, [])
        check("A12b wrapper refuses an empty bound map",
              not biv["ok"])

    print("[B] owned-quiesce — actual CLI")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        d1 = U("1")
        spec_path = tmp / "spec.json"
        spec_path.write_text(json.dumps(TAIL_SPEC))
        decl_path = tmp / "decl.json"
        run_cli([str(HERE / "p25_tail_roster.py"), "declare",
                 "--spec", str(spec_path), "--out", str(decl_path)])
        state = {"schema": "p25-quiesce-state-v1",
                 "defs": [{"id": d1, "schedule_enabled": False,
                           "cursor_null": True}],
                 "runs_terminal": [{"sync_def_id": d1, "id": U("c"),
                                    "status": "failed"}],
                 "new_runs_since_decl": [],
                 "owner_rows": [{"def_id": d1, "run_id": U("c"),
                                 "run_status": "failed",
                                 "blocked_reason":
                                 "awaiting_p26_resolution"}]}
        proof = {"ok": True, "phase": "pre-abc",
                 "entries": {d1: {"http_status": 409, "rc": 0,
                                  "body_sha256": "a" * 64,
                                  "run_id": U("c")}}}
        (tmp / "state.json").write_text(json.dumps(state))
        (tmp / "expected.json").write_text(json.dumps([d1]))
        (tmp / "proof.json").write_text(json.dumps(proof))
        r = run_cli([str(HERE / "check_owned_quiesced_p25.py"), "check",
                     "--state", str(tmp / "state.json"),
                     "--owner-roster", str(decl_path),
                     "--expected-defs", str(tmp / "expected.json"),
                     "--blocked-proof", str(tmp / "proof.json")])
        check("B1 quiet owned state passes via CLI", r.returncode == 0,
              r.stdout[-300:] + r.stderr[-200:])
        wrong = {"defs": [], "runs_terminal": [], "new_runs_since_decl":
                 [], "owner_rows": []}
        (tmp / "wrong.json").write_text(json.dumps(wrong))
        r = run_cli([str(HERE / "check_owned_quiesced_p25.py"), "check",
                     "--state", str(tmp / "wrong.json"),
                     "--owner-roster", str(decl_path),
                     "--expected-defs", str(tmp / "expected.json"),
                     "--blocked-proof", str(tmp / "proof.json")])
        check("B2 missing expected def blocks (exact UUID set)",
              r.returncode != 0)
        adopt = dict(state, new_runs_since_decl=[
            {"id": U("9"), "sync_def_id": d1}])
        (tmp / "adopt.json").write_text(json.dumps(adopt))
        r = run_cli([str(HERE / "check_owned_quiesced_p25.py"), "check",
                     "--state", str(tmp / "adopt.json"),
                     "--owner-roster", str(decl_path),
                     "--expected-defs", str(tmp / "expected.json"),
                     "--blocked-proof", str(tmp / "proof.json")])
        check("B3 new run for owner-blocked def blocks (no adoption)",
              r.returncode != 0 and "adoption forbidden" in r.stdout)
        noproof = {"ok": True, "phase": "pre-abc", "entries": {}}
        (tmp / "noproof.json").write_text(json.dumps(noproof))
        r = run_cli([str(HERE / "check_owned_quiesced_p25.py"), "check",
                     "--state", str(tmp / "state.json"),
                     "--owner-roster", str(decl_path),
                     "--expected-defs", str(tmp / "expected.json"),
                     "--blocked-proof", str(tmp / "noproof.json")])
        check("B4 missing PER-DEF pre-ABC blocked entry fails closed",
              r.returncode != 0 and "NO pre-ABC" in r.stdout)
        (tmp / "tagdef.json").write_text(json.dumps(["p25-tail-x"]))
        r = run_cli([str(HERE / "check_owned_quiesced_p25.py"), "check",
                     "--state", str(tmp / "state.json"),
                     "--owner-roster", str(decl_path),
                     "--expected-defs", str(tmp / "tagdef.json"),
                     "--blocked-proof", str(tmp / "proof.json")])
        check("B6 def_tag string as expected UUID refused "
              "(UUID-to-UUID)", r.returncode != 0)
        r = run_cli([str(HERE / "check_owned_quiesced_p25.py"),
                     "collect", "--expected-defs",
                     str(tmp / "expected.json"),
                     "--out", str(tmp / "s.json")])
        check("B7 live collect refuses rc3 until released",
              r.returncode == 3)

    print("[D] running-images + candidate source — actual CLI")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        sel = {"schema": "p25-selected-build-v1",
               "images": {"connector": "sha256:" + "6" * 64,
                          "dataset": "sha256:" + "d" * 64,
                          "pipeline": "sha256:" + "5" * 64,
                          "connector2": "sha256:" + "2" * 64,
                          "connector3": "sha256:" + "3" * 64,
                          "p25connector": "sha256:" + "a" * 64,
                          "python-3.12-alpine": "sha256:" + "c" * 64}}
        (tmp / "sel.json").write_text(json.dumps(sel))
        tag_img = "sha256:" + "e" * 64

        def evidence(mutate=None, phase="swap"):
            svc = {}
            for s in ("connector", "dataset", "pipeline"):
                svc[s] = {"containers": 1, "running": True,
                          "image": sel["images"][s],
                          "started_at": "2026-11-14T08:00:00Z",
                          "ps_rc": 0, "inspect_rc": 0}
            absent = ("connector2", "connector3", "dataset-proxy",
                      "p25fixture", "p25-proxy", "p25connector")
            for s in absent:
                svc[s] = {"containers": 0, "running": False,
                          "ps_rc": 0, "ps_raw": ""}
            if phase == "restart":
                for s, img in (("connector2", sel["images"]
                                ["connector2"]),
                               ("connector3", sel["images"]
                                ["connector3"]),
                               ("dataset-proxy", sel["images"]
                                ["python-3.12-alpine"]),
                               ("p25fixture", sel["images"]
                                ["python-3.12-alpine"]),
                               ("p25-proxy", sel["images"]
                                ["python-3.12-alpine"]),
                               ("p25connector", sel["images"]
                                ["p25connector"])):
                    svc[s] = {"containers": 1, "running": True,
                              "image": img, "started_at": "x",
                              "ps_rc": 0, "inspect_rc": 0}
            if mutate:
                mutate(svc)
            return {"schema": "p25-running-evidence-v1",
                    "env_dir": "/tmp", "project": "p",
                    "services": svc}

        def verify(ev, phase="swap"):
            (tmp / "run.json").write_text(json.dumps(ev))
            argv = [str(HERE / "check_running_images_p25.py"),
                    "verify", "--running", str(tmp / "run.json"),
                    "--phase", phase,
                    "--selected-build", str(tmp / "sel.json")]
            return run_cli(argv)

        r = verify(evidence())
        check("D1 swap-phase evidence passes (exact build mapping)",
              r.returncode == 0, r.stdout[-200:])
        r = verify(evidence(phase="restart"), phase="restart")
        check("D1b restart-phase evidence passes (old aux + ALL p25 "
              "aux legitimately up on exact selected images)",
              r.returncode == 0, r.stdout[-200:])
        ev = evidence(phase="restart")
        ev["services"]["p25fixture"] = {
            "containers": 1, "running": True, "image": tag_img,
            "started_at": "x", "ps_rc": 0, "inspect_rc": 0}
        r = verify(ev, phase="restart")
        check("D1c p25fixture on a NON-selected (wrong) python image "
              "refuses", r.returncode != 0
              and "selected-build" in r.stdout)

        def bool_count(svc):
            svc["p25-proxy"]["containers"] = False
        r = verify(evidence(mutate=bool_count))
        check("D2 bool False containers is NEVER 0 (non-bool int "
              "required)", r.returncode != 0
              and "strict int" in r.stdout)

        def garbage(svc):
            svc["connector"]["image"] = "sha256:deadbeef"
        r = verify(evidence(mutate=garbage))
        check("D3 garbage/garbled image refuses (full-SHA equality to "
              "the selected build)", r.returncode != 0)

        def drop(svc):
            del svc["p25-proxy"]
        r = verify(evidence(mutate=drop))
        check("D4 missing-key absence is NOT absence", r.returncode != 0
              and "no evidence entry" in r.stdout)

        def failed_ps(svc):
            svc["p25-proxy"]["ps_rc"] = 1
        r = verify(evidence(mutate=failed_ps))
        check("D4b absent service needs a SUCCESSFUL ps (rc0)",
              r.returncode != 0 and "SUCCESSFUL ps" in r.stdout)

        def two(svc):
            svc["connector"]["containers"] = 2
        r = verify(evidence(mutate=two))
        check("D5 containers must be EXACTLY 1 for present services",
              r.returncode != 0 and "EXACTLY 1" in r.stdout)
        ev = evidence(phase="restart")
        ev["services"]["p25-proxy"]["image"] = "sha256:" + "f" * 64
        r = verify(ev, phase="restart")
        check("D6 p25-proxy image drift vs the cached selected image "
              "refuses", r.returncode != 0)

        def unknown(svc):
            svc["stray"] = {"containers": 0, "ps_rc": 0}
        r = verify(evidence(mutate=unknown))
        check("D7 unknown service evidence refused", r.returncode != 0
              and "unknown services" in r.stdout)
        (tmp / "hand.json").write_text(json.dumps(
            {"connector": {"image": sel["images"]["connector"],
                           "running": True}}))
        r = run_cli([str(HERE / "check_running_images_p25.py"),
                     "verify", "--running", str(tmp / "hand.json"),
                     "--phase", "swap",
                     "--selected-build", str(tmp / "sel.json")])
        check("D7b hand-written evidence without the collector schema "
              "refused", r.returncode != 0)
        r = run_cli([str(HERE / "check_running_images_p25.py"),
                     "collect", "--out", str(tmp / "x.json")])
        check("D8 live collect refuses rc3 until released",
              r.returncode == 3)
        # D9/D10: the candidate source gate through its REAL CLI
        r = run_cli([str(HERE / "candidate_source_gate_p25.py"),
                     "--repo",
                     str(HERE.parents[2]),
                     "--expect", "0" * 64])
        check("D9 wrong candidate SHA refused via actual CLI",
              r.returncode == 1)
        r = run_cli([str(HERE / "candidate_source_gate_p25.py"),
                     "--repo", str(HERE.parents[2]),
                     "--expect",
                     "a1bff07f3760fce7b5661b169ba26a165712c950035d"
                     "4e4ec9948639d51ffd27"])
        check("D10 reviewed candidate SHA verified via actual CLI "
              "(PG-gate algorithm)", r.returncode == 0)

    print("[G] adapter.main + shim MAIN paths (not just "
          "GuardSubstitute)")
    r = run_cli([str(HERE / "p23_q_guard_shim_p25.py")])
    check("G1 shim CLI usage error rc2", r.returncode == 2)
    r = run_cli([str(HERE / "p23_q_guard_shim_p25.py"),
                 "/tmp/q3dir", "connector"])
    check("G2 shim CLI refuses rc3 while the guard is unreleased "
          "(REAL main path)", r.returncode == 3
          and "not released" in r.stderr)
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        # adapter.main DRAFT refusal (module flag as shipped)
        sys.argv = ["p25_old_suite_adapter.py", "--suite",
                    "smoke_p23.py", "--", "--env-dir", "/tmp/x"]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), \
                contextlib.redirect_stderr(io.StringIO()):
            rc = ad.main()
        check("G3 adapter.main DRAFT refusal rc3", rc == 3)
        # full happy path with a FAKE frozen suite driving ONE real
        # guard argv; the real shim subprocess answers rc3 (guard
        # unreleased) and the record archives rc/stdout/stderr/shas
        ad.RELEASED_FOR_EXECUTION = True
        suite_dir = tmp / "suites"
        suite_dir.mkdir()
        (suite_dir / "smoke_p23.py").write_text(
            "import subprocess, sys\n"
            f"GUARD = {str(ad.P23_GUARD)!r}\n"
            "def main():\n"
            "    p = subprocess.run(\n"
            "        ['python3', '-I', GUARD, '/tmp/q3dir',\n"
            "         'connector'], capture_output=True, text=True,\n"
            "        timeout=120)\n"
            "    print('suite saw guard rc', p.returncode)\n"
            "    return 0\n")
        rec = {"schema": "p25-deployment-record-v1",
               "connector_boot": "2026-11-14T08:00:00Z",
               "connector_image": sel["images"]["connector"],
               "source_sha256": ad.CANDIDATE_SOURCE_SHA}
        (tmp / "rec.json").write_text(json.dumps(rec))
        (tmp / "selb.json").write_text(json.dumps(sel))
        sys.argv = ["p25_old_suite_adapter.py",
                    "--suite", "smoke_p23.py",
                    "--suite-dir", str(suite_dir),
                    "--deployment-record", str(tmp / "rec.json"),
                    "--selected-build", str(tmp / "selb.json"),
                    "--record-dir", str(tmp / "records"),
                    "--", "--env-dir", "/tmp/q3dir"]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), \
                contextlib.redirect_stderr(io.StringIO()):
            rc = ad.main()
        recs = sorted((tmp / "records").glob("adapter-*.json"))
        entry = json.loads(recs[-1].read_text()) if recs else {}
        inv = (entry.get("adaptations") or [{}])[0]
        check("G4 adapter.main real path: 1 substitution, REAL guard "
              "rc/stdout/stderr + shas archived, steady env from the "
              "verified record",
              rc == 0 and entry.get("count") == 1
              and inv.get("rc") == 3
              and isinstance(inv.get("stdout"), str)
              and len(inv.get("stdout_sha256", "")) == 64
              and entry["guard_env_policy"]["P25_SHIM_POLICY"]
              == "steady", str(entry.get("adaptations"))[:200])
        # P24 shape: zero substitutions must FAIL
        (suite_dir / "smoke_p24.py").write_text(
            "def main():\n    print('no guard call at all')\n"
            "    return 0\n")
        sys.argv = ["p25_old_suite_adapter.py",
                    "--suite", "smoke_p24.py",
                    "--suite-dir", str(suite_dir),
                    "--deployment-record", str(tmp / "rec.json"),
                    "--selected-build", str(tmp / "selb.json"),
                    "--record-dir", str(tmp / "records2"),
                    "--", "--env-dir", "/tmp/q3dir"]
        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            rc = ad.main()
        check("G5 P24 ZERO substitutions BLOCK (expected count "
              "enforced)", rc == 1)
        # unverified deployment record: refuse BEFORE the suite loads
        bad_rec = dict(rec, connector_image="sha256:" + "b" * 64)
        (tmp / "bad-rec.json").write_text(json.dumps(bad_rec))
        sys.argv = ["p25_old_suite_adapter.py",
                    "--suite", "smoke_p23.py",
                    "--suite-dir", str(suite_dir / "nonexistent"),
                    "--deployment-record", str(tmp / "bad-rec.json"),
                    "--selected-build", str(tmp / "selb.json"),
                    "--record-dir", str(tmp / "records3"),
                    "--", "--env-dir", "/tmp/q3dir"]
        raised = None
        buf_err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(buf_err):
            try:
                ad.main()
            except SystemExit as exc:               # noqa: PERF203
                raised = exc
        check("G6 unverified deployment record refuses before any "
              "suite side effect", raised is not None
              and "unverified deployment record" in str(raised)
              and "selected-build connector" in buf_err.getvalue())
        ad.RELEASED_FOR_EXECUTION = False

    print("[H] driver gating (actual CLI)")
    drv = str(HERE / "formal_run_p25.sh")
    r = subprocess.run(["bash", drv, "dryrun"], capture_output=True,
                       text=True)
    check("H1 driver dryrun rc0 + locked order", r.returncode == 0
          and "p21abc" in r.stdout)
    r = subprocess.run(["bash", drv, "swap"], capture_output=True,
                       text=True, env={**__import__("os").environ,
                                       "P25_CONFIRM": "no"})
    check("H2 unconfirmed live phase refuses rc3", r.returncode == 3)
    r = subprocess.run(["bash", drv, "swap"], capture_output=True,
                       text=True, env={**__import__("os").environ,
                                       "P25_CONFIRM": "yes"})
    check("H3 confirmed-but-unreleased live phase refuses rc3",
          r.returncode == 3 and "RELEASED_RUNTIME" in r.stdout
          + r.stderr)

    print("[I] deploy-record helper — REAL record_body with mocked IO")
    import importlib
    dh = importlib.import_module("deploy_record_helper_p25") \
        if "deploy_record_helper_p25" in sys.modules \
        else loadmod(HERE / "deploy_record_helper_p25.py", "dh")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        (tmp / "sel.json").write_text(json.dumps(
            {"images": {"connector": "sha256:" + "6" * 64}}))
        calls = []

        def mk(ps_rc=0, ids=("cid1",), boot="2026-11-14T08:00:00Z",
               image="sha256:" + "6" * 64, gate_sha="a" * 64):
            def run_cli(argv, timeout):
                calls.append(argv[0])
                if argv[0] == "docker" and argv[1] == "compose":
                    return ps_rc, "\n".join(ids) + "\n", ""
                if argv[0] == "docker" and argv[1] == "inspect":
                    return 0, f"{boot}|{image}\n", ""
                if argv[0] == "python3":
                    return (0 if gate_sha else 1,
                            f"CANDIDATE_SOURCE_SHA256={gate_sha}\n", "")
                raise AssertionError(f"unexpected IO {argv[:2]}")
            return run_cli

        argv8 = [str(tmp), str(tmp), str(tmp), "proj1", "a" * 64,
                 str(tmp / "sel.json"), "unit-test",
                 str(tmp / "ev")]
        (tmp / "ev").mkdir()
        rc, rec = dh.record_body(argv8, mk())
        check("I1 positive: record written, boot/image bound, "
              "str-pinned sha formatting never TypeErrors",
              rc == 0 and rec is not None
              and rec["connector_image"] == "sha256:" + "6" * 64
              and rec["cause"] == "unit-test"
              and (tmp / "ev" / "deployment-record-latest.txt")
              .is_file())
        rc, rec = dh.record_body(argv8, mk(ids=("a", "b")))
        check("I2 two containers refuse (ambiguous ps)", rc == 1
              and rec is None)
        rc, rec = dh.record_body(
            argv8, mk(image="sha256:" + "7" * 64))
        check("I3 running image != selected-build connector refuses",
              rc == 1 and rec is None)
        rc, rec = dh.record_body(argv8, mk(gate_sha="b" * 64))
        check("I4 candidate-source mismatch refuses (str compare, no "
              "Path coercion)", rc == 1)
        check("I5 IO surface is ps/inspect/gate ONLY (never stop/up)",
              set(calls) == {"docker", "python3"})

    print("[J] driver swap small-loop — REAL gates via temp EV")
    import os
    with tempfile.TemporaryDirectory() as td:
        tev = Path(td) / "ev"
        env = {**os.environ, "P25_CONFIRM": "yes",
               "P25_EV_ROOT": str(tev)}
        # produce the (pure) selected-build.json via the build import
        # first — the swap gate now REQUIRES it pre-stop
        rb = subprocess.run(
            ["bash", str(HERE / "formal_run_p25.sh"), "build"],
            capture_output=True, text=True, env=env, timeout=600)
        check("J1a build import in temp EV rc0 (selected-build "
              "produced)", rb.returncode == 0
              and (tev / "selected-build.json").is_file())
        r = subprocess.run(
            ["bash", str(HERE / "formal_run_p25.sh"), "swap"],
            capture_output=True, text=True, env=env, timeout=600)
        gate_ok = (tev / "steps" / "swap-build-binding-attempt1" /
                   "rc").read_text().strip() == "0"
        no_tag_check = not list(
            (tev / "steps").glob("swap-tag-check-attempt*"))
        check("J1 gates green then honest rc3 at the live boundary "
              "(r18 verified, ZERO live IO)",
              r.returncode == 3 and gate_ok
              and "runtime release" in r.stdout + r.stderr
              and no_tag_check)
        # J4-J8: selected-build.json negatives — each must refuse
        # BEFORE the runtime boundary (zero tag-check/stop attempts)
        def selbuild_neg(name, mutate):
            evn = Path(td) / f"ev-{name}"
            evn.mkdir(parents=True, exist_ok=True)
            (evn / "steps").mkdir(exist_ok=True)
            sb = json.loads((tev / "selected-build.json").read_text())
            mutate(evn, sb)
            envn = {**os.environ, "P25_CONFIRM": "yes",
                    "P25_EV_ROOT": str(evn)}
            rn = subprocess.run(
                ["bash", str(HERE / "formal_run_p25.sh"), "swap"],
                capture_output=True, text=True, env=envn, timeout=600)
            glog = ""
            gdir = evn / "steps" / "swap-build-binding-attempt1"
            if (gdir / "stdout.log").is_file():
                glog = (gdir / "stdout.log").read_text()
            grc = (gdir / "rc").read_text().strip() \
                if (gdir / "rc").is_file() else "?"
            return name, rn, grc, glog, evn

        def no_stop(evn):
            return (not list((evn / "steps")
                             .glob("swap-tag-check-attempt*"))
                    and not list((evn / "steps")
                                 .glob("swap-stop-attempt*"))
                    and not list((evn / "steps")
                                 .glob("swap-up-*-attempt*")))

        # J4: selected-build.json entirely MISSING
        n, rn, grc, glog, evn = selbuild_neg(
            "missing-sb", lambda evn, sb: None)
        check("J4 missing selected-build.json refuses pre-stop (zero "
              "STOP/UP)", rn.returncode != 0 and grc == "1"
              and "selected-build.json MISSING" in glog and no_stop(evn))
        # J5: corrupt JSON
        n, rn, grc, glog, evn = selbuild_neg(
            "bad-json",
            lambda evn, sb: (evn / "selected-build.json")
            .write_text("{corrupt!!"))
        check("J5 corrupt selected-build.json refuses pre-stop (zero "
              "STOP/UP)", rn.returncode != 0 and grc == "1"
              and "not valid JSON" in glog and no_stop(evn))
        # J6: missing image key
        def drop_key(evn, sb):
            sb["images"].pop("python-3.12-alpine")
            (evn / "selected-build.json").write_text(json.dumps(sb))
        n, rn, grc, glog, evn = selbuild_neg("missing-key", drop_key)
        check("J6 selected-build.json missing image key refuses "
              "pre-stop (zero STOP/UP)", rn.returncode != 0
              and grc == "1"
              and "missing keys" in glog and no_stop(evn))
        # Valid JSON still MUST be an object. In particular, null
        # must not bypass validation, and arrays must not reach .get.
        for case, value, label in (("null-sb", None, "J7"),
                                   ("array-sb", [], "J8")):
            n, rn, grc, glog, evn = selbuild_neg(
                case, lambda evn, sb, value=value:
                (evn / "selected-build.json").write_text(
                    json.dumps(value)))
            check(f"{label} selected-build.json {value!r} refuses "
                  "pre-stop (zero STOP/UP)", rn.returncode != 0
                  and grc == "1"
                  and "must be an object" in glog and no_stop(evn))
        # tampered r18 copy -> gate refuses BEFORE the runtime boundary
        r18 = json.loads((HERE / "build-candidate-bindings-r18.json")
                         .read_text())
        r18["selected_green_attempts"]["build-base3"]["raw"][
            "sha256"] = "0" * 64
        bad = Path(td) / "bad-r18.json"
        bad.write_text(json.dumps(r18))
        tev2 = Path(td) / "ev2"
        env2 = {**os.environ, "P25_CONFIRM": "yes",
                "P25_EV_ROOT": str(tev2),
                "P25_BUILD_R18": str(bad)}
        r = subprocess.run(
            ["bash", str(HERE / "formal_run_p25.sh"), "swap"],
            capture_output=True, text=True, env=env2, timeout=600)
        brc = (tev2 / "steps" / "swap-build-binding-attempt1" /
               "rc").read_text().strip()
        blog = (tev2 / "steps" / "swap-build-binding-attempt1" /
                "stdout.log").read_text()
        check("J2 tampered r18 raw sha refuses BEFORE any stop/up "
              "(gate rc1 + recorded drift reason, no tag-check/stop "
              "attempts)",
              r.returncode != 0 and brc == "1"
              and "sha/bytes drift" in blog
              and not list((tev2 / "steps").glob("swap-tag-check*"))
              and not list((tev2 / "steps").glob("swap-stop*")))
        # J2b: r18 file ENTIRELY MISSING -> same pre-stop refusal
        tev3 = Path(td) / "ev3"
        env3 = {**os.environ, "P25_CONFIRM": "yes",
                "P25_EV_ROOT": str(tev3),
                "P25_BUILD_R18": str(Path(td) / "no-such-r18.json")}
        r = subprocess.run(
            ["bash", str(HERE / "formal_run_p25.sh"), "swap"],
            capture_output=True, text=True, env=env3, timeout=600)
        grc = (tev3 / "steps" / "swap-build-binding-attempt1" /
               "rc").read_text().strip() if (tev3 / "steps" /
                                             "swap-build-binding-attempt1"
                                             / "rc").is_file() else "?"
        check("J2b missing r18 file refuses BEFORE any stop/up "
              "(gate rc1, zero tag-check/stop attempts)",
              r.returncode != 0 and grc == "1"
              and not list((tev3 / "steps").glob("swap-tag-check*"))
              and not list((tev3 / "steps").glob("swap-stop*")))
    order = subprocess.run(["bash", str(HERE / "formal_run_p25.sh"),
                            "list"], capture_output=True,
                           text=True).stdout.split()
    check("J3 fixture_up ordered AFTER swap and BEFORE window",
          order.index("swap") < order.index("fixture_up")
          < order.index("window"))

    print(f"\n[selftest-p25-runtime-tools] pass={len(PASS)} "
          f"fail={len(FAIL)}")
    for f in FAIL:
        print(f"  FAILED: {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
