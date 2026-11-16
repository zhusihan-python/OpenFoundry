#!/usr/bin/env python3
"""P25 ARCHIVE pure selftest — the six review-archive-tools-round2
groups over the ACTUAL tools (injected IO only; no network/DB/docker/
listener). See ARCHIVE-CONTRACT.md §0 for the mechanical completion
criteria these checks implement."""

import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import uuid as _uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
P24_FINAL = Path("/Volumes/ExternalSD/projects/OpenFoundry/"
                 "tools/smoke/business-platform-p2-4/results/"
                 "2026-11-14-p24-final")
PASS, FAIL = [], []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  ok: {name}")
    else:
        FAIL.append(f"{name} :: {detail}")
        print(f"  FAIL: {name} :: {detail}")


def loadmod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


PG_BINDINGS_REAL = None            # loaded lazily from the real file


def _real_bindings():
    global PG_BINDINGS_REAL
    if PG_BINDINGS_REAL is None:
        PG_BINDINGS_REAL = json.loads(
            (HERE / "pg-candidate-bindings-r17.json").read_text())
    return PG_BINDINGS_REAL


_TAIL_SPEC = {
    "run_rows": [
        {"def_tag": "p25-tail-x", "source_tag": "p25h11",
         "semantics": "p25-drop-noack", "count": 1},
        {"def_tag": "p25-tail-y", "source_tag": "p25h14",
         "semantics": "p25-lease-lost-dispatched", "count": 1}],
    "owner_rows": [
        {"def_tag": "p25-tail-x", "run_ref": "terminal-failed",
         "blocked_reason": "awaiting_p26_resolution"}],
    "produced_by": "p25-tail"}


def _frozen_decl(spec=None):
    import hashlib as _h
    spec = spec or _TAIL_SPEC
    return {"schema": "p25-tail-roster-v1", "ok": True, "spec": spec,
            "declaration_sha256": _h.sha256(json.dumps(
                spec, sort_keys=True).encode()).hexdigest()}


def _green_postfacts(td: Path, verify=True):
    """Build a REAL (mocked-IO) green postfacts attempt + frozen tail
    declaration under td; returns (attempt_dir, decl_path)."""
    coll = loadmod(HERE / "collect_postfacts_p25.py", "coll_green")
    pf = loadmod(HERE / "verify_postfacts_p25.py", "pf_green")
    guard = loadmod(HERE / "p25_cohort_guard.py", "g_green")
    roster6 = guard.load_roster()
    frozen = json.loads((HERE / "p24-frozen-baseline.json").read_text())
    protected = frozen["content_gates"]["protected_counters"][
        "expected"]
    raw, rows, owners, abc, tail = _synthetic_world(
        pf, coll, roster6, frozen)
    tmp = td / "pfw"
    aux = tmp / "aux"
    aux.mkdir(parents=True)
    (aux / "proxy-quiet-proof.stdout.log").write_text(json.dumps(
        {"commit_started": 3, "commit_finished": 3,
         "commit_inflight": 0}) + "\n")
    (aux / "fixture-quiet-proof.stdout.log").write_text(json.dumps(
        [{"name": "p25h11-src", "active": 0}]) + "\n")
    abc_path = tmp / "abc-result.json"
    abc_path.write_text(json.dumps(raw, indent=1) + "\n")

    def run_cli(argv, timeout=120):
        sql = argv[-1]
        cg = frozen["content_gates"]
        if argv[0] == "stat":
            return 0, "600\n", "", "ok"
        if argv[:3] == ["docker", "volume", "ls"]:
            return 0, "\n".join(cg["volumes_expected_names"]) + "\n", "", "ok"
        if argv[:2] == ["docker", "inspect"]:
            fx = cg["fixture"]
            value = f"null|{fx['pid1']}" if "Entrypoint" in argv[3] else f"{fx['boot']}|{fx['running']}|{fx['image']}|{fx['restart_policy']}"
            return 0, value + "\n", "", "ok"
        if "ps" in argv and argv[-1] == "fixture":
            return 0, "a" * 64 + "\n", "", "ok"
        if "count(*)" in sql:
            return 0, "0\n", "", "ok"
        for label, rid in cg["legacy"]["runs"].items():
            if rid in sql:
                return 0, cg["legacy"]["expected_lines"][label] + "\n", "", "ok"
        if "COALESCE(d.schedule_cron,'') <> ''" in sql:
            return 0, json.dumps([{"id": rid} for rid in cg["global_catalog"]["nonempty_cron_ids"]]) + "\n", "", "ok"
        if "sync_def_checkpoints" in sql:
            return (0, json.dumps(owners) + "\n", "", "ok")
        return (0, json.dumps(rows) + "\n", "", "ok")

    def http_get(url, token=""):
        import json as _j
        name = url.rsplit("/", 1)[-1]
        want = protected[name]
        return 200, _j.dumps({
            "name": name, "mode": want["mode"],
            "total": want["total"],
            "request_count": want["request_count"],
            "inventory_started": want["inventory_started"],
            "inventory_finished": want["inventory_finished"],
            "active": 0}).encode(), None

    class A:
        pass
    a = A()
    a.out_dir = str(tmp / "attempts")
    a.env_dir = coll.ENV_DIR_DEFAULT
    a.aux_quiet_dir = str(aux)
    a.abc_result = str(abc_path)
    coll.RELEASED_FOR_EXECUTION = True
    try:
        rc = coll.collect_body(a, run_cli, http_get)
    finally:
        coll.RELEASED_FOR_EXECUTION = False
    assert rc == 0, "green postfacts world must collect clean"
    att = tmp / "attempts" / "attempt-1"
    decl_path = td / "tail-declaration.json"
    decl_path.write_text(json.dumps(_frozen_decl()) + "\n")
    if not verify:
        return att, decl_path
    v = pf.verify_attempt(att, decl_path)
    assert v["ok"], f"green world must verify: {v['reasons'][:3]}"
    return att, decl_path


def _persist_attempt(td: Path) -> Path:
    """Faithful mini persist attempt dir (sealed-17 completeness +
    files ledger with real shas)."""
    frozen = json.loads((HERE / "p24-frozen-baseline.json").read_text())
    owned = frozen["content_gates"]["owned_fullstate"]["defs"]
    adir = td / "persist-attempt"
    adir.mkdir(parents=True)
    defs = [dict(id=d["id"], source_id=_mk_uuid(0xD000 + i),
                 output_dataset_id=_mk_uuid(0xE000 + i), cron="",
                 tz="UTC", enabled="false", cursor_null="true")
            for i, d in enumerate(owned)]
    ticks = [{"sync_def_id": owned[0]["id"],
              "tick_id": _mk_uuid(0xB1)}]
    runs = [{"run_id": _mk_uuid(0xB6), "status": "succeeded",
             "sync_def_id": owned[0]["id"]}]
    data = {"defs": defs, "ticks": ticks, "runs": runs,
            "grants": [],
            "cp_owners": [{"sync_def_id": _mk_uuid(0xB2),
                           "source_id": _mk_uuid(0xB3),
                           "output_dataset_id": _mk_uuid(0xB4),
                           "mode": "cursor", "confirmed_epoch": "1",
                           "cp_null": "false",
                           "run_id": _mk_uuid(0xB5),
                           "active_since": "",
                           "run_status": "failed"}]}
    files = {}
    for t, rows in data.items():
        for suffix, content in (
                (".txt", json.dumps(rows) + "\n"),
                (f".json", json.dumps(rows, indent=1,
                                      sort_keys=True) + "\n")):
            p = adir / f"{t}{suffix}"
            p.write_text(content)
        (adir / f"{t}.txt.rc").write_text("0\n")
        (adir / f"{t}.txt.err").write_text("")
        (adir / f"{t}.txt.cmd").write_text(f"psql … {t}\n")
    for p in sorted(adir.rglob("*")):
        if p.is_file():
            files[str(p.relative_to(adir))] = {
                "sha256": __import__("hashlib").sha256(
                    p.read_bytes()).hexdigest()}
    manifest = {
        "ok": True,
        "attempt_dir": str(adir),
        "tables": {"defs": {"rows": len(defs)},
                   "ticks": {"rows": len(ticks)},
                   "runs": {"rows": len(runs)},
                   "grants": {"rows": 0},
                   "cp_owners": {"rows": 1}},
        "files": files}
    (adir / "persist-manifest.json").write_text(json.dumps(
        manifest, indent=1, sort_keys=True) + "\n")
    return adir


def make_stage(ev: Path, key, stdout_obj=None, files=None, rc=0):
    d = ev / "steps" / f"{key}-attempt1"
    d.mkdir(parents=True, exist_ok=True)
    (d / "cmd.txt").write_text(f"python3 -I tool_{key}.py --real\n")
    (d / "stderr.log").write_text("")
    (d / "rc").write_text(f"{rc}\n")
    obj = {"ok": True} if stdout_obj is None else stdout_obj
    (d / "stdout.log").write_text(
        "progress\n" + json.dumps(obj) + "\n")
    for name, content in (files or {}).items():
        p = d / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content if isinstance(content, str)
                     else json.dumps(content, indent=1) + "\n")
    return d


def build_full_ev(td: Path) -> Path:
    """Schema-true EV for ALL 22 canonical stages. postfacts/persist
    are REAL (mocked-IO collector attempt + faithful persist attempt);
    standalone is a REAL bundle (offline-verifiable); images anchor to
    r18 candidates; window/regression cases carry actual raw files."""
    import hashlib as _h
    ev = td / "ev"
    ev.mkdir(parents=True, exist_ok=True)
    real = json.dumps(_real_bindings())
    att, decl = _green_postfacts(td)
    padir = _persist_attempt(td)
    r18 = json.loads(
        (HERE / "build-candidate-bindings-r18.json").read_text())
    r18img = r18["service_images"]
    abc = {"suffix": "c0ffee01", "official": True, "partial": False,
           "counts": {"pass": 3, "fail": 0},
           "results": {c: {"status": "pass", "seconds": 1.0} for c in
                       ("heartbeat_progress", "lease_loss_fencing",
                        "terminal_commit_fault")},
           "evidence": {"terminal_commit_fault": {"logical_path":
                         f"sync-{_mk_uuid(0xA25C)}.ndjson"}}}
    h_raw_files = {f"h{i}-raw.log": f"h{i} request/response ledger\n"
                   for i in range(1, 16)}
    suite_raw = {n: f"{n} raw\n" for n in
                 ("p1pre", "p23", "p22", "p24", "p1post")}
    stages = {
        "preflight": (None, None),
        "unit": (None, None),
        "pg-isolated": (None, {"pg-candidate-bindings-r17.json":
                               real + "\n"}),
        "build": (None, None),
        "swap": ({"ok": True, "target_boot": "2026-11-14T08:00:00Z",
                  "target_image": "sha256:" + "a" * 64}, None),
        "window": ({"ok": True, "cases": [
            {"id": f"h{i}", "status": "pass", "gates": ["g1"],
             "raw": f"h{i}-raw.log"} for i in range(1, 16)]},
            h_raw_files),
        "regressions-p1pre9": ({"ok": True, "passed":
                                [f"p1pre{i}" for i in range(9)],
                                "raw_files": ["p1pre.raw"]},
                               {"p1pre.raw": suite_raw["p1pre"]}),
        "regressions-p23": ({"ok": True, "passed":
                             [f"p23_{i}" for i in range(8)],
                             "raw_files": ["p23.raw"]},
                            {"p23.raw": suite_raw["p23"]}),
        "regressions-p22": ({"ok": True, "passed":
                             [f"p22_{i}" for i in range(11)],
                             "raw_files": ["p22.raw"]},
                            {"p22.raw": suite_raw["p22"]}),
        "regressions-p24": ({"ok": True, "passed":
                             [f"p24_{i}" for i in range(7)],
                             "raw_files": ["p24.raw"]},
                            {"p24.raw": suite_raw["p24"]}),
        "reread-artifacts": ({"ok": True, "artifacts":
                              ["art-1", "job-2"]}, None),
        "p1post9": ({"ok": True, "passed":
                     [f"p1post{i}" for i in range(9)],
                     "raw_files": ["p1post.raw"]},
                    {"p1post.raw": suite_raw["p1post"]}),
        "restart-regression": (
            {"ok": True, "boot_before": "2026-11-14T07:11:20Z",
             "boot_after": "2026-11-14T09:00:00Z"}, None),
        "pre-tail-gates": (None, {
            "tail-declaration.json": _frozen_decl()}),
        "tail": ({"ok": True, "cases": [
            {"id": "h11", "status": "pass", "gates": ["g"]},
            {"id": "h14", "status": "pass", "gates": ["g"]}]}, None),
        "aux-down": (None, {
            "proxy-quiet-proof.stdout.log":
                {"commit_started": 3, "commit_finished": 3,
                 "commit_inflight": 0},
            "fixture-quiet-proof.stdout.log":
                [{"name": "p25h11-src", "active": 0}]}),
        "final-images": ({"ok": True, "services": {
            **{svc: {"image": r18img[svc]["image_id"],
                     "running": "true"}
               for svc in ("connector", "dataset", "pipeline")},
            **{n: {"containers": 0} for n in
               ("p25fixture", "p25-proxy", "p25connector",
                "connector2", "connector3", "dataset-proxy")}}},
            {"images-ps.txt": "CONTAINER ID   IMAGE\n"}),
        "p21abc": ({"ok": True}, {"abc-result.json": abc}),
        "postfacts": ({"ok": True, "final_roster_size": 10,
                       "expected_roster_size": 10,
                       "abc": {"b_id": _mk_uuid(0xA25B),
                               "c_id": _mk_uuid(0xA25C)},
                       "attempt_dir": str(att),
                       "tail_declaration": str(decl)}, None),
        "persist": (None, {
            "persist-manifest.json": {
                "ok": True, "attempt_dir": str(padir),
                "tables": {"defs": {"rows": 17}, "ticks": {"rows": 1},
                           "runs": {"rows": 1},
                           "grants": {"rows": 0},
                           "cp_owners": {"rows": 1}}}}),
        "source-freeze": ({"ok": True, "removed": [],
                           "changed": ["models/models.go"],
                           "added": ["repo/migrations/x.sql"]}, None),
        "standalone": (None, None),
    }
    for key, (obj, files) in stages.items():
        make_stage(ev, key, obj, files)
    selections = {k: {"attempt_dir": f"steps/{k}-attempt1",
                      "attempt": 1} for k in stages}
    selections["06b-preimpl-gate-recheck"] = {
        "attempt_dir": "steps/06b-preimpl-gate-recheck-attempt1",
        "attempt": 1}
    make_stage(ev, "06b-preimpl-gate-recheck", None, rc=1)
    (ev / "selected-index.json").write_text(json.dumps(
        {"schema": "p25-selected-index-v1", "selections": selections},
        indent=1) + "\n")
    sa = loadmod(HERE / "standalone_p25.py", "sa_bfe")
    sa.build_bundle(ev, HERE, td / "ev-standalone")
    return ev

def g1_g2_finalize(fz):
    print("[G1/G2] finalize: CANONICAL stage set + real content "
          "gates + hash-LAST two-step seal")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        ev = build_full_ev(tmp)
        m = fz.prepare(ev, HERE, [])
        check("G1-1 prepare ok over ALL 22 canonical stages with REAL "
              "imported green (unit/pg/r18-build/accepted-preflight) "
              "+ old-859/27 re-hash",
              m["final_ok"] is True
              and all(v == "ok" for v in m["stage_status"].values())
              and len(m["stage_status"]) == 22
              and m["old_archive_reverify"]["archive_files"] == 859
              and m["old_archive_reverify"]["tools"] == 27
              and m["old_archive_reverify"]["archive_drift"] == 0
              and any(k.startswith("unit:") for k in
                      m["imported_green"])
              and any(k.startswith("pg:") for k in m["imported_green"])
              and "build:source-gate:rc" in m["imported_green"]
              and "build:render-aux:raw" in m["imported_green"]
              and any(k.endswith("verify-csupp.stdout")
                      for k in m["imported_green"])
              and sum(1 for k in m["imported_green"]
                      if k.endswith("attempt-summary.json")) == 2
              and m["imported_green"].get(
                  "source-freeze:recomputed") == fz.PG_GREEN_SOURCE
              and m["imported_green"].get(
                  "standalone:offline-rc") == "0"
              and "postfacts:verifier-run" in m["imported_green"]
              and "persist:ledger-replay" in m["imported_green"]
              and any(k.startswith("window:raw:") for k in
                      m["imported_green"]),
              str(m["problems"][:4]))
        s = fz.seal(ev, tmp / "meta")
        check("G2-1 seal ok; wrote ONLY hash-manifest",
              s.get("ok") is True and s.get("zero_extra_writes") is
              True, str(s.get("problems")))
        r = fz.reverify(ev, tmp / "meta")
        check("G2-2 reverify clean", r["ok"] is True,
              str(r["problems"][:3]))
        ts = json.loads((ev / "tools-sha.json").read_text())["files"]
        check("G1-2 tools-sha covers json/txt/yaml/fixtures",
              all(k in ts for k in (
                  "p25-required-names.txt", "compose-p25-fixture.yaml",
                  "pg-candidate-bindings-r17.json",
                  "ARCHIVE-CONTRACT.md")), str(sorted(ts)[:4]))

        def fresh(sub, mutate=None):
            root = tmp / sub
            e = build_full_ev(root)
            if mutate:
                mutate(e)
            return e

        # --- canonical-set negatives: drop ANY single stage ---
        dropped_refused = 0
        for stage in sorted(fz.CANONICAL_STAGES):
            def drop(e, s=stage):
                idx = json.loads((e / "selected-index.json")
                                 .read_text())
                idx["selections"].pop(s)
                (e / "selected-index.json").write_text(
                    json.dumps(idx))
            m2 = fz.prepare(fresh(f"d-{stage}", drop), HERE, [])
            if m2["final_ok"] is False and any(
                    f"'{stage}'" in p for p in m2["problems"]):
                dropped_refused += 1
            else:
                check(f"G1-15 drop '{stage}' must refuse", False,
                      str(m2["problems"][:2]))
        check("G1-15 MISSING any single canonical stage refuses "
              "(all 22 faces, exact-name reason)",
              dropped_refused == 22, f"{dropped_refused}/22")

        # --- verdict layer ---
        def plain_stdout(e):
            d = e / "steps" / "restart-regression-attempt1"
            (d / "stdout.log").write_text("plain output\n")
        m2 = fz.prepare(fresh("nv", plain_stdout), HERE, [])
        check("G1-4 absent explicit verdict REFUSED (stdout-only "
              "stage, no verdict file)", m2["final_ok"] is False and
              any("NO explicit TRUE verdict" in p
                  for p in m2["problems"]), str(m2["problems"][:2]))
        def null_verdict(e):
            make_stage(e, "restart-regression", {
                "ok": None, "boot_before": "2026-11-14T07:11:20Z",
                "boot_after": "2026-11-14T09:00:00Z"})
        m2 = fz.prepare(fresh("nullv", null_verdict), HERE, [])
        check("G1-5 NULL verdict REFUSED (content ok, verdict null)",
              m2["final_ok"] is False and any(
                  "NO explicit TRUE verdict" in p
                  for p in m2["problems"]),
              str(m2["problems"][:2]))
        def bad_rc(e):
            (e / "steps" / "persist-attempt1" / "rc").write_text("1\n")
        m2 = fz.prepare(fresh("rcnz", bad_rc), HERE, [])
        check("G1-6 selected rc!=0 REFUSED", m2["final_ok"] is False
              and any("rc=" in p for p in m2["problems"]),
              str(m2["problems"][:2]))
        # empty --require cannot shrink the gate (already proven by
        # G1-15); a bogus extra require must refuse:
        m2 = fz.prepare(ev, HERE, ["nonexistent-step"])
        check("G1-7a bogus --require exact-key refusal",
              m2["final_ok"] is False and any(
                  "nonexistent-step" in p for p in m2["problems"]),
              str(m2["problems"][:2]))
        # G1-7 (required missing) superseded by the 22-face battery.

        def idx_extra(e):
            idx = json.loads((e / "selected-index.json").read_text())
            idx["extra_key"] = 1
            (e / "selected-index.json").write_text(json.dumps(idx))
        m2 = fz.prepare(fresh("schema", idx_extra), HERE, [])
        check("G1-8 selected-index extra key REFUSED",
              m2["final_ok"] is False, str(m2["problems"][:1]))
        def esc(e):
            idx = json.loads((e / "selected-index.json").read_text())
            idx["selections"]["unit"] = {
                "attempt_dir": "../../etc"}
            (e / "selected-index.json").write_text(json.dumps(idx))
        m2 = fz.prepare(fresh("esc", esc), HERE, [])
        check("G1-9 attempt_dir escaping EV REFUSED",
              m2["final_ok"] is False and any("escapes" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:1]))

        # --- PG import faces ---
        def pg_bindings(mutate):
            def f(e):
                d = e / "steps" / "pg-isolated-attempt1"
                b = json.loads(
                    (d / "pg-candidate-bindings-r17.json").read_text())
                mutate(b)
                (d / "pg-candidate-bindings-r17.json").write_text(
                    json.dumps(b) + "\n")
            return f
        m2 = fz.prepare(fresh("pgbad", pg_bindings(
            lambda b: b.__setitem__(
                "candidate_source_sha256", "f" * 64))), HERE, [])
        check("G1-10 bindings wrong source sha REFUSED",
              m2["final_ok"] is False and any("ffd27" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        m2 = fz.prepare(fresh("pguni", pg_bindings(
            lambda b: b.__setitem__(
                "unified_required_total", "87"))), HERE, [])
        check("G1-10b unified total string tampered REFUSED",
              m2["final_ok"] is False and any("old72" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        def pg_nobind(e):
            (e / "steps" / "pg-isolated-attempt1" /
             "pg-candidate-bindings-r17.json").unlink()
        m2 = fz.prepare(fresh("pgnone", pg_nobind), HERE, [])
        check("G1-12 pg selection without bindings REFUSED",
              m2["final_ok"] is False, str(m2["problems"][:1]))

        # --- doctored REAL evidence trees (copy + flip) ---
        import shutil as _sh

        def with_doctored_tree(sub, doctor, name, substr):
            root = tmp / sub
            tree = root / "evidence"
            _sh.copytree(fz.EVIDENCE_ROOT, tree)
            doctor(tree)
            old_root = fz.EVIDENCE_ROOT
            fz.EVIDENCE_ROOT = tree
            try:
                e = build_full_ev(root / "evroot")
                m2 = fz.prepare(e, HERE, [])
                check(name, m2["final_ok"] is False and any(
                    substr in p for p in m2["problems"]),
                    str(m2["problems"][:3]))
            finally:
                fz.EVIDENCE_ROOT = old_root

        def drop_one_unit_ok(t):
            p = t / ("connector/attempt20-round2-build-vet-"
                     "fullunit.log")
            lines = p.read_text().splitlines()
            dropped = False
            out = []
            for line in lines:
                if not dropped and line.startswith("ok "):
                    dropped = True        # remove exactly one ok line
                    continue
                out.append(line)
            p.write_text("\n".join(out) + "\n")
        with_doctored_tree("d-unit", drop_one_unit_ok,
                           "G1-16 doctored unit evidence (38→37 ok) "
                           "REFUSED", "38/0")

        def flip_pg_rc(t):
            sorted((t / "pg").glob("new-normal_*.rc"))[0].write_text(
                "1\n")
        with_doctored_tree("d-pg", flip_pg_rc,
                           "G1-17 doctored PG stage rc (0→1) REFUSED "
                           "by real tree consumption", "new-normal")

        def flip_build_rc(t):
            sorted((t / "build").glob("source-gate_*.rc"))[-1] \
                .write_text("2\n")
        with_doctored_tree("d-build", flip_build_rc,
                           "G1-18 doctored build component rc "
                           "REFUSED", "source-gate")

        # preflight: doctored accepted.json (root-issued anchor)
        def doctor_accepted(mutate):
            root = tmp / "d-pre"
            root.mkdir(parents=True, exist_ok=True)
            acc = json.loads(fz.PREFLIGHT_ACCEPTED.read_text())
            mutate(acc)
            p = root / "preflight-accepted.json"
            p.write_text(json.dumps(acc, indent=1) + "\n")
            return p
        old_acc = fz.PREFLIGHT_ACCEPTED
        fz.PREFLIGHT_ACCEPTED = doctor_accepted(
            lambda a: a["artifacts"].__setitem__(
                next(iter(a["artifacts"])), "f" * 64))
        try:
            e = build_full_ev(tmp / "d-pre" / "evroot")
            m2 = fz.prepare(e, HERE, [])
            check("G1-19 doctored accepted.json sha REFUSED (import "
                  "integrity)",
                  m2["final_ok"] is False and any("sha" in p
                                                  for p in
                                                  m2["problems"]),
                  str(m2["problems"][:2]))
        finally:
            fz.PREFLIGHT_ACCEPTED = old_acc
        fz.PREFLIGHT_ACCEPTED = doctor_accepted(
            lambda a: a.__setitem__("status", "pending"))
        try:
            e = build_full_ev(tmp / "d-pre2" / "evroot")
            m2 = fz.prepare(e, HERE, [])
            check("G1-19b accepted.json status != accepted REFUSED",
                  m2["final_ok"] is False and any("status" in p
                                                 for p in
                                                 m2["problems"]),
                  str(m2["problems"][:2]))
        finally:
            fz.PREFLIGHT_ACCEPTED = old_acc

        # build: doctored r18 bindings copy in the step dir
        def r18_doctor(mutate):
            def f(e):
                d = e / "steps" / "build-attempt1"
                b = json.loads(
                    (HERE / "build-candidate-bindings-r18.json")
                    .read_text())
                mutate(b)
                (d / "build-candidate-bindings-r18.json").write_text(
                    json.dumps(b) + "\n")
            return f
        m2 = fz.prepare(fresh("r18bad", r18_doctor(
            lambda b: b["selected_green_attempts"]["source-gate"]
            ["rc"].__setitem__("sha256", "e" * 64))), HERE, [])
        check("G1-18b r18 binding sha tampered REFUSED (re-hash vs "
              "binding)", m2["final_ok"] is False and any(
                  "source-gate" in p for p in m2["problems"]),
              str(m2["problems"][:2]))
        m2 = fz.prepare(fresh("r18drop", r18_doctor(
            lambda b: b["selected_green_attempts"].pop(
                "cache-python"))), HERE, [])
        check("G1-18c r18 dropped group REFUSED (exact 6)",
              m2["final_ok"] is False and any("6" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))

        # --- content-schema negatives ---
        def window_bad(e):
            make_stage(e, "window", {"ok": True, "cases": [
                {"id": f"h{i}", "status": "pass", "gates": ["g"],
                 "raw": f"h{i}-raw.log"}
                for i in range(1, 16)
                if i != 7] + [
                {"id": "h1", "status": "pass", "gates": ["g"],
                 "raw": "h1-raw.log"}]},
                {f"h{i}-raw.log": "raw\n" for i in range(1, 16)})
        def window_noraw(e):
            make_stage(e, "window", {"ok": True, "cases": [
                {"id": f"h{i}", "status": "pass", "gates": ["g"]}
                for i in range(1, 16)]})
        m2 = fz.prepare(fresh("wbad", window_bad), HERE, [])
        check("G1-20 window missing h7 + duplicate id REFUSED",
              m2["final_ok"] is False and any("exact" in p or
                                              "unique" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        m2 = fz.prepare(fresh("wnr", window_noraw), HERE, [])
        check("G1-20b window case WITHOUT actual raw REFUSED",
              m2["final_ok"] is False and any("raw" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        def abc_hist(e):
            d = e / "steps" / "p21abc-attempt1"
            raw = json.loads((d / "abc-result.json").read_text())
            raw["suffix"] = "1020a565"
            (d / "abc-result.json").write_text(json.dumps(raw))
        m2 = fz.prepare(fresh("abch", abc_hist), HERE, [])
        check("G1-21 p21abc HISTORICAL suffix collision REFUSED "
              "(identity)",
              m2["final_ok"] is False and any("historical" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        def aux_bool(e):
            d = e / "steps" / "aux-down-attempt1"
            (d / "proxy-quiet-proof.stdout.log").write_text(
                json.dumps({"commit_started": True,
                            "commit_finished": True,
                            "commit_inflight": 0}))
        m2 = fz.prepare(fresh("auxb", aux_bool), HERE, [])
        check("G1-22 aux-down BOOLEAN quiet REFUSED",
              m2["final_ok"] is False and any("non-bool" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        def img_short(e):
            make_stage(e, "final-images", {"ok": True, "services": {
                "connector": {"image": "fe3208b3",
                              "running": "true"}, "dataset":
                {"image": "sha256:" + "1" * 64, "running": "true"},
                "pipeline": {"image": "sha256:" + "2" * 64,
                             "running": "true"},
                **{n: {"containers": 0} for n in
                   ("p25fixture", "p25-proxy", "p25connector",
                    "connector2", "connector3", "dataset-proxy")}}},
                {"images-ps.txt": "CONTAINER ID\n"})
        m2 = fz.prepare(fresh("imgs", img_short), HERE, [])
        check("G1-23 final-images SHORT image REFUSED (r18 candidate "
              "anchor)", m2["final_ok"] is False and any("r18" in p
                                                         for p in
                                                         m2["problems"]),
              str(m2["problems"][:2]))
        def pf_mismatch(e):
            root = e.parent
            make_stage(e, "postfacts", {
                "ok": True, "final_roster_size": 10,
                "expected_roster_size": 9,
                "abc": {"b_id": _mk_uuid(0xA25B), "c_id":
                        _mk_uuid(0xA25C)},
                "attempt_dir": str(root / "pfw" / "attempts" /
                                   "attempt-1"),
                "tail_declaration": str(root / "tail-declaration.json")})
        m2 = fz.prepare(fresh("pfm", pf_mismatch), HERE, [])
        check("G1-24 postfacts roster size mismatch REFUSED (real "
              "verifier ran ok; summary mismatch caught)",
              m2["final_ok"] is False and any("roster sizes" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        def pf_tamper(e):
            root = e.parent
            body = (root / "pfw" / "attempts" / "attempt-1" /
                    "03-counters.read1.p21a-b6ec49ab.body")
            body.write_bytes(b'{"name":"p21a-b6ec49ab","active":0}')
        m2 = fz.prepare(fresh("pft", pf_tamper), HERE, [])
        check("G1-24b TAMPERED postfacts attempt REFUSED by the "
              "independent verifier (ledger re-hash)",
              m2["final_ok"] is False, str(m2["problems"][:2]))
        def persist_grants(e):
            mm = json.loads((e.parent / "persist-attempt" /
                             "persist-manifest.json").read_text())
            mm["tables"]["grants"]["rows"] = 2
            (e.parent / "persist-attempt" /
             "persist-manifest.json").write_text(json.dumps(mm))
        m2 = fz.prepare(fresh("pg2", persist_grants), HERE, [])
        check("G1-25 persist grants rows != 0 REFUSED",
              m2["final_ok"] is False and any("grants" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))
        def src_rm(e):
            make_stage(e, "source-freeze", {
                "ok": True, "removed": ["services/x.go"],
                "changed": [], "added": []})
        m2 = fz.prepare(fresh("srcr", src_rm), HERE, [])
        check("G1-26 source-freeze removed != [] REFUSED",
              m2["final_ok"] is False, str(m2["problems"][:2]))
        e = fresh("sbx")
        _sh.rmtree(tmp / "sbx" / "ev-standalone", ignore_errors=True)
        m2 = fz.prepare(e, HERE, [])
        check("G1-27 standalone missing bundle REFUSED",
              m2["final_ok"] is False and any("bundle" in p
                                              for p in
                                              m2["problems"]),
              str(m2["problems"][:2]))

        def persist_drop_def(e):
            root = e.parent
            d = root / "persist-attempt"
            defs = json.loads((d / "defs.json").read_text())[:-1]
            (d / "defs.json").write_text(json.dumps(
                defs, indent=1, sort_keys=True) + "\n")
            man = json.loads((d / "persist-manifest.json").read_text())
            import hashlib as _h2
            man["files"]["defs.json"] = {
                "sha256": _h2.sha256((d / "defs.json").read_bytes())
                .hexdigest()}
            (d / "persist-manifest.json").write_text(json.dumps(
                man, indent=1, sort_keys=True) + "\n")
        m2 = fz.prepare(fresh("pdrop", persist_drop_def), HERE, [])
        check("G1-25b persist defs NOT the sealed 17 REFUSED "
              "(completeness)", m2["final_ok"] is False and any(
                  "sealed 17" in p for p in m2["problems"]),
              str(m2["problems"][:2]))

        e = fresh("sbt")
        with open(tmp / "sbt" / "ev-standalone" / "ev" /
                  "selected-index.json", "a") as fh:
            fh.write(" ")
        m2 = fz.prepare(e, HERE, [])
        check("G1-27b standalone bundle TAMPERED REFUSED by the "
              "offline re-verification", m2["final_ok"] is False and
              any("offline" in p for p in m2["problems"]),
              str(m2["problems"][:2]))

        import shutil as _sh2
        rroot = tmp / "repo"
        if not rroot.exists():
            rroot.mkdir()
            for d in fz.SOURCE_DIRS:
                _sh2.copytree(Path(fz.REPO_ROOT) / d,
                              rroot / d,
                              ignore=_sh2.ignore_patterns(
                                  "*_test.go"))
            for m in ("go.mod", "go.sum"):
                _sh2.copy(Path(fz.REPO_ROOT) / m, rroot / m)
        go_file = sorted((rroot / fz.SOURCE_DIRS[0]).rglob("*.go"))[0]
        go_file.write_text(go_file.read_text() + "\n// drift\n")
        old_repo = fz.REPO_ROOT
        fz.REPO_ROOT = rroot
        try:
            m2 = fz.prepare(fresh("srcd", None), HERE, [])
            check("G1-26b source recompute DRIFT REFUSED (final "
                  "fingerprint != a1bff…ffd27)",
                  m2["final_ok"] is False and any("fingerprint" in p
                                                  for p in
                                                  m2["problems"]),
                  str(m2["problems"][:2]))
        finally:
            fz.REPO_ROOT = old_repo

        eD2 = build_full_ev(tmp / "drift")
        fz.prepare(eD2, HERE, [])
        tsj = json.loads((eD2 / "tools-sha.json").read_text())
        first = sorted(tsj["files"])[0]
        tsj["files"][first] = "0" * 64
        (eD2 / "tools-sha.json").write_text(json.dumps(
            tsj, indent=1, sort_keys=True) + "\n")
        r = fz.seal(eD2, tmp / "metaD2")
        check("G1-28 seal INPUT DRIFT (tools changed after prepare) "
              "REFUSED", r["ok"] is False and any("input drift" in p
                                                  for p in
                                                  r["problems"]),
              str(r.get("problems", [])[:2]))

        eF = build_full_ev(tmp / "failprep")
        def drop_one(e):
            idx = json.loads((e / "selected-index.json").read_text())
            idx["selections"].pop("swap")
            (e / "selected-index.json").write_text(json.dumps(idx))
        drop_one(eF)
        fz.prepare(eF, HERE, [])            # fails (final_ok false)
        (eF / "tools-sha.json").write_text(json.dumps(
            {"tools_dir": str(HERE), "files": {},
             "frozen_imports": fz.frozen_import_shas()},
            indent=1, sort_keys=True) + "\n")
        r = fz.seal(eF, tmp / "metaF")
        check("G1-29 FAILED prepare NOT sealable (final_ok lock)",
              r["ok"] is False and any("final_ok" in p
                                       for p in r["problems"]),
              str(r.get("problems", [])[:2]))

        # --- nested secret + seal negatives (unchanged semantics) ---
        e2 = build_full_ev(tmp / "more")
        (e2 / "nested").mkdir()
        (e2 / "nested" / "hash-manifest.json").write_text(
            '{"x":"postgres://u:pw@h/db"}\n')
        m2 = fz.prepare(e2, HERE, [])
        check("G1-3 NESTED hash-manifest secret IS scanned",
              m2["final_ok"] is False and m2["secret_hits"] >= 1,
              str(m2["problems"][:1]))
        e3 = build_full_ev(tmp / "psw")
        fz.prepare(e3, HERE, [])
        fz.seal(e3, tmp / "meta3")
        (e3 / "steps" / "rogue.log").write_text("leak\n")
        r = fz.reverify(e3, tmp / "meta3")
        check("G2-4 post-seal EV write DETECTED", r["ok"] is False
              and any("post-seal" in p for p in r["problems"]),
              str(r["problems"][:2]))
        eL = build_full_ev(tmp / "late")
        fz.prepare(eL, HERE, [])
        (eL / "late.log").write_text("postgres://u:pw@h/db\n")
        r = fz.seal(eL, tmp / "metaL")
        check("G2-7 secret after prepare REFUSED at seal time",
              r["ok"] is False and any("seal-time" in p
                                       for p in r["problems"]),
              str(r.get("problems", [])[:1]))
        eD = build_full_ev(tmp / "dbl")
        fz.prepare(eD, HERE, [])
        fz.seal(eD, tmp / "metaD")
        r = fz.seal(eD, tmp / "metaD")
        check("G2-6 double seal REFUSED", r["ok"] is False)
        eS = build_full_ev(tmp / "sealpre")
        r = fz.seal(eS, tmp / "metaS")
        check("G2-5 seal without prepare REFUSED", r["ok"] is False,
              str(r.get("problems")))

def g3_standalone(sa, ov):
    print("[G3] standalone: byte preservation + replay + sealed "
          "bundle + offline verifier")
    check("G3-0 redaction pattern sets identical (standalone vs "
          "offline verifier)",
          [p for _n, p in sa.PATTERNS] == [p for _n, p in ov.PATTERNS])
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        src = tmp / "ev"
        src.mkdir()
        (src / "a.log").write_bytes(
            b"clean line\r\nDSN postgres://u:pw@h/db tail\r\nlast-no-"
            b"newline")
        (src / "b.json").write_bytes(b'{"x": 1}\n')
        (src / "c.bin").write_bytes(b"\x00\x01\xff\xfe\x02")
        meta = sa.sanitize_dir(src, tmp / "copy")
        rc = sa.replay_compare(src, tmp / "copy", meta)
        a = (tmp / "copy" / "a.log").read_bytes()
        check("G3-1 bytes preserved: CRLF + no-trailing-newline + "
              "non-secret lines identical; only the DSN span changed",
              rc["ok"] and a.startswith(b"clean line\r\n")
              and b"REDACTED:dsn" in a
              and a.endswith(b"last-no-newline")
              and (tmp / "copy" / "c.bin").read_bytes()
              == b"\x00\x01\xff\xfe\x02"
              and (tmp / "copy" / "b.json").read_bytes()
              == b'{"x": 1}\n', str(rc["reasons"][:2]))
        # undecodable line: the ASCII secret span IS redacted at BYTE
        # level; the invalid \xff\xfe prefix survives verbatim and NO
        # UTF-8 replacement bytes are ever introduced
        (src / "d.log").write_bytes(
            b"\xff\xfe postgres://u:pw@h/db\n")
        meta = sa.sanitize_dir(src, tmp / "copy2")
        rc = sa.replay_compare(src, tmp / "copy2", meta)
        d = (tmp / "copy2" / "d.log").read_bytes()
        check("G3-2 undecodable line: secret span redacted at BYTE "
              "level; invalid bytes preserved; zero replacement chars",
              rc["ok"] and d.startswith(b"\xff\xfe ")
              and b"REDACTED:dsn" in d
              and b"postgres://" not in d
              and b"\xef\xbf\xbd" not in d, str(rc["reasons"][:2]))
        try:
            sa.sanitize_dir(src, tmp / "copy2")
            check("G3-3 non-fresh dst REFUSED (immutable)", False,
                  "no raise")
        except ValueError:
            check("G3-3 non-fresh dst REFUSED (immutable)", True)
        try:
            sa.sanitize_dir(tmp / "nope", tmp / "copy3")
            check("G3-4 missing src REFUSED", False, "no raise")
        except ValueError:
            check("G3-4 missing src REFUSED", True)
        empty = tmp / "empty"
        empty.mkdir()
        try:
            sa.sanitize_dir(empty, tmp / "copy4")
            check("G3-4b empty src REFUSED", False, "no raise")
        except ValueError:
            check("G3-4b empty src REFUSED", True)
        # unapproved same-line change
        meta = sa.sanitize_dir(src, tmp / "copy5")
        p5 = tmp / "copy5" / "b.json"
        p5.write_bytes(b'{"x": 2}\n')            # unapproved change
        rc = sa.replay_compare(src, tmp / "copy5", meta)
        check("G3-5 unapproved same-line change REJECTED by replay",
              not rc["ok"] and any("sanctioned" in r
                                   for r in rc["reasons"]),
              str(rc["reasons"][:2]))
        meta6 = sa.sanitize_dir(src, tmp / "copy6")
        meta6["files"]["a.log"]["sites"] = []    # forged: site vanished
        rc = sa.replay_compare(src, tmp / "copy6", meta6)
        check("G3-6 vanished/forged site REJECTED", not rc["ok"],
              str(rc["reasons"][:2]))
        meta7 = json.loads(json.dumps(
            meta if isinstance(meta, dict) else meta))
        meta7 = sa.sanitize_dir(src, tmp / "copy7")
        (tmp / "copy7" / "b.json").unlink()
        rc = sa.replay_compare(src, tmp / "copy7", meta7)
        check("G3-7 fullindex bidirectional (deleted copy file "
              "REJECTED)", not rc["ok"] and any("bidirectional" in r
                                                for r in
                                                rc["reasons"]),
              str(rc["reasons"][:2]))
        # bundle: full tools + frozen deps + offline verifier + NEW seal
        bdir = tmp / "bundle"
        bmeta = sa.build_bundle(src, HERE, bdir)
        v = sa.verify_bundle(bdir, src_ev=src)
        check("G3-8 bundle: tools+deps+offline verifier + new seal; "
              "full replay + offline verify rc0",
              v["ok"] and (bdir / "bundle-hash-manifest.json").is_file()
              and (bdir / "offline_verify_p25.py").is_file()
              and (bdir / "tools" / "finalize_p25.py").is_file()
              and len(bmeta["files"]) > 20, str(v["reasons"][:2]))
        tampered = bdir / "b.json" if (bdir / "b.json").is_file() \
            else bdir / "ev" / "b.json"
        raw = tampered.read_bytes()
        tampered.write_bytes(raw.replace(b"1", b"2"))
        v = sa.verify_bundle(bdir)
        check("G3-9 one-byte bundle tamper REJECTED (new seal "
              "recompute)", not v["ok"], str(v["reasons"][:2]))
        tampered.write_bytes(raw)
        victim = bdir / "ev" / "a.log"
        victim.unlink()
        v = sa.verify_bundle(bdir)
        check("G3-10 deleted bundle file REJECTED", not v["ok"],
              str(v["reasons"][:2]))
        emptyb = tmp / "emptyb"
        emptyb.mkdir()
        proc = subprocess.run(
            [sys.executable, "-I", str(ov.__file__), "--bundle",
             str(emptyb)], capture_output=True, text=True)
        check("G3-11 empty bundle REJECTED by offline verifier CLI "
              f"(rc={proc.returncode})", proc.returncode != 0,
              proc.stdout[:120])
        # surviving secret inside a sealed bundle
        victim2 = bdir / "ev" / "leak.txt"
        victim2.write_text("postgres://u:pw@h/db\n")
        v = sa.verify_bundle(bdir)
        check("G3-12 unredacted secret injected into the bundle "
              "REJECTED (SURVIVING SECRET + seal mismatch)",
              not v["ok"] and any("SURVIVING" in r or "seal" in r
                                  for r in v["reasons"]),
              str(v["reasons"][:2]))


def _frozen_rows():
    prov = json.loads((P24_FINAL / "FINAL-NEW-TWO-PROVENANCE.json")
                      .read_text())
    return [dict(v) for v in prov["rows"].values()]


def g4_abc(pf):
    print("[G4] ABC derivation from the REAL frozen P21 result shape")
    raw = json.loads((P24_FINAL / "10b-p21-ABC" / "result_p21.json")
                     .read_text())
    rows = _frozen_rows()
    d = pf.derive_abc_ids(raw, rows)
    check("G4-1 REAL frozen result: official/partial/counts/3-case "
          "statuses + suffix→fixture_src→parents walk; B has no "
          "direct UUID",
          d["ok"] and d["b_id"] == "dcca308f-8d74-48b6-ae27-"
          "7f17fa2f30e2" and d["c_id"] == "885d1429-53ea-43f5-a5a7-"
          "03d2ae0b4b8a" and d["suffix"] == "1020a565",
          str(d["reasons"][:3]))
    prov = json.loads((P24_FINAL / "FINAL-NEW-TWO-PROVENANCE.json")
                      .read_text())
    check("G4-2 C cross-checks against the sealed c_target_live "
          "run binding",
          prov["c_target_live"]["run_id"] == d["c_id"])
    bad = dict(raw, partial=True)
    check("G4-3 partial result REFUSED",
          not pf.derive_abc_ids(bad, rows)["ok"])
    bad = json.loads(json.dumps(raw))
    bad["counts"]["pass"] = 2
    check("G4-3b counts != 3/0 REFUSED",
          not pf.derive_abc_ids(bad, rows)["ok"])
    bad = json.loads(json.dumps(raw))
    bad["results"]["lease_loss_fencing"]["status"] = "fail"
    check("G4-3c case status != pass REFUSED",
          not pf.derive_abc_ids(bad, rows)["ok"])
    bad = json.loads(json.dumps(raw))
    bad["results"]["scenarios_b"] = bad["results"].pop(
        "lease_loss_fencing")
    check("G4-3d fictional keyset REFUSED (real keys only)",
          not pf.derive_abc_ids(bad, rows)["ok"])
    bad = json.loads(json.dumps(raw))
    bad["controlled_nonterminal_ids"] = ["x", "y"]
    check("G4-3e convenience id field REFUSED",
          not pf.derive_abc_ids(bad, rows)["ok"])
    bad = json.loads(json.dumps(raw))
    bad["evidence"]["terminal_commit_fault"]["logical_path"] = \
        "sync-notauuid.ndjson"
    check("G4-4 malformed C logical_path REFUSED",
          not pf.derive_abc_ids(bad, rows)["ok"])
    dup = rows + [dict(rows[0])]            # second identical src row
    check("G4-5 ambiguous fixture_src walk REFUSED (needs EXACTLY "
          "one row per src)", not pf.derive_abc_ids(raw, dup)["ok"])
    mixed = []
    for r in rows:
        if r["fixture_src"].startswith("p21c-"):
            r = dict(r, id="12345678-1234-4123-8123-123456789012")
        mixed.append(dict(r))
    check("G4-6 C row != logical_path uuid REFUSED (crosscheck)",
          not pf.derive_abc_ids(raw, mixed)["ok"])


def _mk_uuid(seed: int):
    return str(_uuid.UUID(int=seed, version=4))


def _synthetic_world(pf, coll, roster6, frozen, suffix="ab12cd34"):
    """Synthetic-but-REAL-shaped world: sealed old6 rows + ABC rows for
    a fresh suffix + declared tail rows; raw abc result built from the
    FROZEN template."""
    raw = json.loads((P24_FINAL / "10b-p21-ABC" / "result_p21.json")
                     .read_text())
    b_id, c_id = _mk_uuid(0xA25B), _mk_uuid(0xA25C)
    d_b, d_c = _mk_uuid(0xA25D), _mk_uuid(0xA25E)
    s_b, s_c = _mk_uuid(0xA25F), _mk_uuid(0xA260)
    o_b, o_c = _mk_uuid(0xA261), _mk_uuid(0xA262)
    raw = dict(raw, suffix=suffix)
    raw["evidence"] = json.loads(json.dumps(raw["evidence"]))
    raw["evidence"]["terminal_commit_fault"]["logical_path"] = \
        f"sync-{c_id}.ndjson"
    six = [dict(r) for r in roster6.GROUP1 + roster6.GROUP2]

    def abc_row(base, rid, did, sid, oid, src):
        r = dict(base)
        r.update(id=rid, sync_def_id=did, source_id=sid,
                 owner_id=oid, run_owner_id=oid, requested_by=oid,
                 fixture_src=src, source_name=f"p21-c-{src}",
                 status="running")
        return r

    rows_b = _frozen_rows()
    abc = [abc_row(rows_b[0], b_id, d_b, s_b, o_b,
                   f"p21b-{suffix}"),
           abc_row(rows_b[1], c_id, d_c, s_c, o_c,
                   f"p21c-{suffix}")]
    t0, t1 = _mk_uuid(0xA270), _mk_uuid(0xA271)
    dt0, dt1 = _mk_uuid(0xA272), _mk_uuid(0xA273)
    st0, st1 = _mk_uuid(0xA274), _mk_uuid(0xA275)
    tail = [abc_row(rows_b[0], t0, dt0, st0, _mk_uuid(0xA276),
                    "p25h11-src"),
            abc_row(rows_b[1], t1, dt1, st1, _mk_uuid(0xA277),
                    "p25h14-src")]
    owners = [{"sync_def_id": dt0, "source_id": st0,
               "output_dataset_id": _mk_uuid(0xA278), "mode": "cursor",
               "confirmed_epoch": "1", "run_id": _mk_uuid(0xA279),
               "active_since": "", "run_status": "failed",
               "blocked_reason": "awaiting_p26_resolution"}]
    return raw, six + abc + tail, owners, abc, tail


def _run_collect(coll, tmp, rows, owners, raw, proxy_stats,
                 fixture_stats, protected, mutations=None):
    """Drive the ACTUAL collect entry in-process with injected IO."""
    mutations = mutations or {}
    aux = tmp / "aux"
    aux.mkdir(parents=True, exist_ok=True)
    if proxy_stats is not None:
        (aux / "proxy-quiet-proof.stdout.log").write_text(
            json.dumps(proxy_stats) + "\n")
    if fixture_stats is not None:
        (aux / "fixture-quiet-proof.stdout.log").write_text(
            json.dumps(fixture_stats) + "\n")
    abc_path = tmp / "abc-result.json"
    abc_path.write_text(json.dumps(raw, indent=1) + "\n")

    calls = {"cli": 0, "http": 0}

    def run_cli(argv, timeout=120):
        calls["cli"] += 1
        sql = argv[-1]
        if "sync_def_checkpoints" in sql:
            return (0, json.dumps(
                mutations.get("owners", owners)) + "\n", "", "ok")
        if "sync_runs r" in sql and "batch_sync_defs" in sql:
            return (0, json.dumps(mutations.get("rows", rows))
                    + "\n", "", "ok")
        return (1, "", "unexpected SQL", "ok")

    def http_get(url, token=""):
        calls["http"] += 1
        name = url.rsplit("/", 1)[-1]
        if name in mutations.get("drop", set()):
            return 404, b'{"error":"gone"}', None
        body = mutations.get("bodies", {}).get(name)
        if body is None:
            want = protected[name]
            body = {"name": name, "mode": want["mode"],
                    "total": want["total"],
                    "request_count": want["request_count"],
                    "inventory_started": want["inventory_started"],
                    "inventory_finished":
                        want["inventory_finished"],
                    "active": 0}
        if name in mutations.get("unstable", set()) \
                and calls["http"] % 2 == 0:
            body = dict(body, request_count=body["request_count"] + 1)
        import json as _j
        return 200, _j.dumps(body).encode(), None

    class A:
        pass
    a = A()
    a.out_dir = str(tmp / "pf-attempts")
    a.env_dir = coll.ENV_DIR_DEFAULT
    a.aux_quiet_dir = str(aux)
    a.abc_result = str(abc_path)
    coll.RELEASED_FOR_EXECUTION = True
    try:
        rc = coll.collect_body(a, run_cli, http_get)
    finally:
        coll.RELEASED_FOR_EXECUTION = False
    n = 1
    while (tmp / "pf-attempts" / f"attempt-{n+1}").exists():
        n += 1
    return rc, tmp / "pf-attempts" / f"attempt-{n}", calls


def g5_g6(pf, coll):
    print("[G5/G6] collectors: actual readonly IO entry + postfacts "
          "full gates")
    guard = loadmod(HERE / "p25_cohort_guard.py", "g_arch")
    roster6 = guard.load_roster()
    frozen = json.loads((HERE / "p24-frozen-baseline.json").read_text())
    protected = frozen["content_gates"]["protected_counters"][
        "expected"]
    # The tail declaration is a FROZEN ARTIFACT (Runtime owns the
    # roster module, mid-upgrade in parallel); this group adjudicates
    # the artifact itself — built here exactly as the driver freezes
    # it (p25-tail-roster-v1).
    _spec = {
        "run_rows": [
            {"def_tag": "p25-tail-x", "source_tag": "p25h11",
             "semantics": "p25-drop-noack", "count": 1},
            {"def_tag": "p25-tail-y", "source_tag": "p25h14",
             "semantics": "p25-lease-lost-dispatched", "count": 1}],
        "owner_rows": [
            {"def_tag": "p25-tail-x", "run_ref": "terminal-failed",
             "blocked_reason": "awaiting_p26_resolution"}],
        "produced_by": "p25-tail"}

    def _declare(spec):
        return {"schema": "p25-tail-roster-v1", "ok": True,
                "spec": spec,
                "declaration_sha256": hashlib.sha256(
                    json.dumps(spec, sort_keys=True).encode()
                ).hexdigest()}

    decl = _declare(_spec)
    check("G6-0 tail declaration artifact valid (hash-frozen)",
          decl["declaration_sha256"] is not None)

    # schema-vocabulary negatives on the ARTIFACT gate
    bad_decl = _declare(dict(
        _spec, run_rows=[{"def_tag": "x", "semantics": "weird",
                          "count": 0}]))
    vbad = pf.adjudicate_postfacts(
        {"schema": "p25-postfacts-facts-v2", "controlled_rows": [],
         "owner_rows": [], "counters": {}, "proxy_quiet_raw": {},
         "fixture_quiet_raw": []},
        frozen, guard, roster6, bad_decl, {})
    check("G6-0b invalid declared count refused by the tail gate",
          not vbad["ok"] and any("count" in r for r in vbad["reasons"]),
          str(vbad["reasons"][:2]))

    # draft refusal FIRST, via the REAL CLI, with a tripwire: no IO
    outd = tempfile.mkdtemp()
    proc = subprocess.run(
        [sys.executable, "-I", str(HERE / "collect_postfacts_p25.py"),
         "--collect", "--out-dir", outd], capture_output=True,
        text=True)
    check("G5-1 postfacts --collect draft refusal rc3, ZERO "
          "subprocess/HTTP constructed",
          proc.returncode == 3 and not any(
              Path(outd).rglob("attempt-*")),
          proc.stderr[:120])
    proc = subprocess.run(
        [sys.executable, "-I",
         str(HERE / "collect_persist_state_p25.py"), "--collect",
         "--out-dir", outd], capture_output=True, text=True)
    check("G5-1b persist --collect draft refusal rc3",
          proc.returncode == 3 and not any(
              Path(outd).rglob("attempt-*")), proc.stderr[:120])
    shutil.rmtree(outd, ignore_errors=True)

    raw, rows, owners, abc, tail = _synthetic_world(
        pf, coll, roster6, frozen)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        (tmp / "tail-declaration.json").write_text(json.dumps(decl))
        proxy = {"commit_started": 3, "commit_finished": 3,
                 "commit_inflight": 0}
        fixture = [{"name": "p25h11-src", "active": 0},
                   {"name": "p25h14-src", "active": 0}]
        rc, attempt, calls = _run_collect(
            coll, tmp, rows, owners, raw, proxy, fixture, protected)
        check("G5-2 ACTUAL collect entry (injected IO): cmd-before-"
              "exec + real rc/stdout/stderr raws; ok",
              rc == 0 and (attempt / "attempt-summary.json").is_file()
              and (attempt / "01-controlled-rows.rows.json.cmd")
              .is_file(), str((attempt / "attempt-summary.json")
                              .read_text()[:200] if attempt.exists()
                              else "no attempt"))
        v = pf.verify_attempt(attempt, tmp / "tail-declaration.json")
        check("G6-1 verifier (seal-anchored, RAW replay): old6 exact "
              "+ ABC2 derived + tail k + dual roster + counters + "
              "quiet raws → ok, final size 6+2+2",
              v["ok"] and v["final_roster_size"] == 10
              and v["abc"]["b_id"] and v["abc"]["c_id"]
              and v["owner_roster_size"] == 1,
              str(v["reasons"][:4]))
        # identity binding: one run satisfying two tags REFUSED (pure)
        schema27 = set(guard.expected_schema())
        owners_t0 = owners + [
            {"run_id": t["id"],
             "blocked_reason": "awaiting_p26_resolution",
             "run_status": "failed"} for t in tail]
        bound_ok = {tag: {
            "def_id": t["sync_def_id"],
            "source_id": t["source_id"],
            "run_id": t["id"]}
            for tag, t in zip(("p25-tail-x", "p25-tail-y"), tail)}
        tg = pf._tail_gate(decl, tail, owners_t0, bound_ok, schema27)
        check("G6-1c identity binding UUID-to-UUID ok (owner row "
              "present)", not tg, str(tg[:2]))
        bound_two = dict(bound_ok, **{"p25-tail-y": {
            "def_id": tail[1]["sync_def_id"],
            "source_id": tail[1]["source_id"],
            "run_id": tail[0]["id"]}})
        tg = pf._tail_gate(decl, tail, owners_t0, bound_two, schema27)
        check("G6-1d ONE run satisfying TWO tags REFUSED",
              any("already bound" in r for r in tg), str(tg[:2]))
        # caller JSON refused
        facts_v1 = {"schema": "p25-postfacts-facts-v1",
                    "controlled_rows": rows}
        vd = pf.adjudicate_postfacts(
            facts_v1, frozen, guard, roster6, decl, raw)
        check("G5-3 caller-dumps facts v1 REFUSED (not execution "
              "evidence)", not vd["ok"] and any("v2" in r
                                                for r in
                                                vd["reasons"]),
              str(vd["reasons"][:2]))

        _KEEP = object()          # sentinel: absent ≠ explicitly None

        def neg_case(name, mutations, proxy2=_KEEP, fixture2=_KEEP,
                     raw2=None, want_substr=""):
            with tempfile.TemporaryDirectory() as td2:
                t2 = Path(td2)
                (t2 / "tail-declaration.json").write_text(
                    json.dumps(decl))
                rc2, att2, _ = _run_collect(
                    coll, t2,
                    mutations.get("rows", rows),
                    mutations.get("owners", owners),
                    raw2 if raw2 is not None else raw,
                    proxy if proxy2 is _KEEP else proxy2,
                    fixture if fixture2 is _KEEP else fixture2,
                    protected, mutations)
                v2 = pf.verify_attempt(
                    att2, t2 / "tail-declaration.json")
                hit = want_substr and any(want_substr in r
                                          for r in v2["reasons"])
                check(name, not v2["ok"] and hit,
                      str(v2["reasons"][:3]))

        # 27-key schema violation on one tail row (full pipeline)
        rows_bad27 = [dict(r) for r in rows]
        for r in rows_bad27:
            if r.get("fixture_src") == "p25h11-src":
                r.pop("heartbeat_at", None)
        neg_case("G6-1b tail row violating the exact 27-key schema "
                 "REFUSED", {"rows": rows_bad27},
                 want_substr="27-key")
        neg_case("G6-2 proxy quiet BOOLEAN refused (strict non-bool "
                 "ints)", {}, proxy2={"commit_started": True,
                                      "commit_finished": True,
                                      "commit_inflight": 0},
                 want_substr="non-bool")
        neg_case("G6-2b proxy not true-quiet (started!=finished) "
                 "refused", {}, proxy2={"commit_started": 3,
                                        "commit_finished": 2,
                                        "commit_inflight": 0},
                 want_substr="true-quiet")
        neg_case("G6-3 fixture quiet raw EMPTY refused", {},
                 fixture2=[], want_substr="fixture quiet raw")
        neg_case("G6-4 counter active BOOLEAN refused",
                 {"bodies": {"p21b-1020a565": {
                     "name": "p21b-1020a565", "mode": "ok",
                     "total": 151, "request_count": 2,
                     "inventory_started": 2,
                     "inventory_finished": 2, "active": False}}},
                 want_substr="non-bool")
        neg_case("G6-5 counter UNSTABLE across reads refused",
                 {"unstable": {"p21a-b6ec49ab"}},
                 want_substr="UNSTABLE")
        neg_case("G6-6 counter HTTP 404 refused",
                 {"drop": {"p21c-b6ec49ab"}}, want_substr="!= 200")
        neg_case("G6-7 counter request_count DRIFT vs sealed refused",
                 {"bodies": {"p21b-1020a565": {
                     "name": "p21b-1020a565", "mode": "ok",
                     "total": 151, "request_count": 9,
                     "inventory_started": 2,
                     "inventory_finished": 2, "active": 0}}},
                 want_substr="sealed")
        neg_case("G6-8 owner roster dropped (declared terminal-failed "
                 "owner not retained) refused", {"owners": []},
                 want_substr="owner")
        neg_case("G6-9 EXTRA tail row refused (exact k)",
                 {"rows": rows + [dict(tail[0],
                                       id=_mk_uuid(0xA290))]},
                 want_substr="!=")
        neg_case("G6-10 archived proxy quiet raw MISSING = honest "
                 "collection failure; verifier refuses",
                 {}, proxy2=None,
                 want_substr="proxy quiet raw")
        raw2 = json.loads(json.dumps(raw))
        raw2["partial"] = True
        neg_case("G6-11 abc raw partial REFUSED", {}, raw2=raw2,
                 want_substr="partial")
        raw3 = json.loads(json.dumps(raw))
        raw3["controlled_nonterminal_ids"] = ["x"]
        neg_case("G6-12 abc convenience field REFUSED", {},
                 raw2=raw3, want_substr="FORBIDDEN")
        raw4 = json.loads(json.dumps(raw))
        raw4["evidence"]["terminal_commit_fault"][
            "logical_path"] = f"sync-{_mk_uuid(0xA29F)}.ndjson"
        neg_case("G6-13 abc C crosscheck mismatch REFUSED", {},
                 raw2=raw4, want_substr="crosscheck")
        # TRUE collision: the p21c row itself IS an old6 id
        old6_c = roster6.GROUP2[1]["id"]
        rows_col = [dict(r) for r in rows]
        for r in rows_col:
            if r.get("fixture_src") == f"p21c-{raw['suffix']}":
                r["id"] = old6_c
        raw5 = json.loads(json.dumps(raw))
        raw5["evidence"]["terminal_commit_fault"]["logical_path"] = \
            f"sync-{old6_c}.ndjson"
        neg_case("G6-14 ABC id collides with old6 REFUSED (derive "
                 "passes; adjudicator blocks)",
                 {"rows": rows_col}, raw2=raw5, want_substr="COLLIDES")

    # ---- persist collector ----
    print("[G5b] persist: real per-table collection")
    ps = loadmod(HERE / "collect_persist_state_p25.py", "ps_arch")
    owned = frozen["content_gates"]["owned_fullstate"]["defs"]

    def persist_world(mutations=None):
        mutations = mutations or {}
        defs = [dict(d, enabled="false", cursor_null="true")
                for d in owned]

        def run_cli(argv, timeout=120):
            sql = argv[-1]
            if "batch_sync_defs d WHERE" in sql:
                return (0, json.dumps(mutations.get(
                    "defs", defs)) + "\n", "", "ok")
            if "sync_schedule_ticks" in sql:
                return (0, json.dumps([{"sync_def_id": owned[0]["id"],
                                        "tick_id": _mk_uuid(0xB1)}])
                        + "\n", "", "ok")
            if "sync_schedule_grants" in sql:
                if "grants_rc" in mutations:
                    return (mutations["grants_rc"], "",
                            "relation missing", "ok")
                return (0, json.dumps([]) + "\n", "", "ok")
            if "sync_def_checkpoints" in sql:
                return (0, json.dumps([{
                    "sync_def_id": _mk_uuid(0xB2),
                    "source_id": _mk_uuid(0xB3),
                    "output_dataset_id": _mk_uuid(0xB4),
                    "mode": "cursor", "confirmed_epoch": "1",
                    "cp_null": "false", "run_id": _mk_uuid(0xB5),
                    "active_since": "", "run_status": "failed"}])
                    + "\n", "", "ok")
            if "sync_runs r" in sql:
                if mutations.get("runs") is not None:
                    return (0, json.dumps(mutations["runs"])
                            + "\n", "", "ok")
                return (0, json.dumps([
                    {"run_id": _mk_uuid(0xB6), "status": "succeeded",
                     "sync_def_id": owned[0]["id"]}]) + "\n", "",
                    "ok")
            return (1, "", "unexpected", "ok")

        class A:
            pass
        a = A()
        base = mutations.get("base")
        a.out_dir = str(base) if base else None
        a.env_dir = ps.ENV_DIR_DEFAULT
        return a, run_cli

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        a, run_cli = persist_world()
        a.out_dir = str(tmp / "persist")
        ps.RELEASED_FOR_EXECUTION = True
        try:
            rc = ps.collect_body(a, run_cli)
        finally:
            ps.RELEASED_FOR_EXECUTION = False
        att1 = tmp / "persist" / "attempt-1"
        m = json.loads((att1 / "persist-manifest.json").read_text())
        check("G5b-1 persist real per-table shape: cmd (ANY-filter) + "
              "rc + raw + shas; 17 owned + PROTECTED cp_owners",
              rc == 0 and m["ok"] and m["tables"]["defs"]["rows"] == 17
              and m["tables"]["cp_owners"]["rows"] == 1
              and "ANY (ARRAY[" in (att1 / "defs.txt.cmd")
              .read_text(), str(m.get("failures", [])[:3]))
        # immutable attempts: run again -> attempt-2, attempt-1 intact
        a2, run_cli2 = persist_world({"base": tmp / "persist"})
        ps.RELEASED_FOR_EXECUTION = True
        try:
            rc2 = ps.collect_body(a2, run_cli2)
        finally:
            ps.RELEASED_FOR_EXECUTION = False
        check("G5b-2 attempts immutable (second run → attempt-2; "
              "attempt-1 intact)",
              rc2 == 0 and (tmp / "persist" / "attempt-2").is_dir()
              and json.loads((att1 / "persist-manifest.json")
                             .read_text())["ok"])
        # missing table = honest failure, no defaulted []
        a3, run_cli3 = persist_world({"grants_rc": 1})
        a3.out_dir = str(tmp / "p3")
        ps.RELEASED_FOR_EXECUTION = True
        try:
            rc3 = ps.collect_body(a3, run_cli3)
        finally:
            ps.RELEASED_FOR_EXECUTION = False
        m3 = json.loads((tmp / "p3" / "attempt-1" /
                         "persist-manifest.json").read_text())
        check("G5b-3 missing/nonzero table = honest FAILURE (no "
              "defaulted [], no fabricated rc0)",
              rc3 != 0 and not m3["ok"]
              and m3["tables"]["grants"]["rows"] is None
              and m3["tables"]["grants"]["rc"] == 1,
              str(m3["failures"][:2]))
        # non-JSON projection refused
        def bad_cli(argv, timeout=120):
            sql = argv[-1]
            if "batch_sync_defs d WHERE" in sql:
                return (0, "NOT JSON AT ALL\n", "", "ok")
            return run_cli3(argv, timeout)
        a4 = type("A", (), {})()
        a4.out_dir = str(tmp / "p4")
        a4.env_dir = ps.ENV_DIR_DEFAULT
        ps.RELEASED_FOR_EXECUTION = True
        try:
            rc4 = ps.collect_body(a4, bad_cli)
        finally:
            ps.RELEASED_FOR_EXECUTION = False
        m4 = json.loads((tmp / "p4" / "attempt-1" /
                         "persist-manifest.json").read_text())
        check("G5b-4 unparseable projection REFUSED (missing table is "
              "NOT an empty result)", rc4 != 0 and not m4["ok"]
              and m4["tables"]["defs"]["rows"] is None,
              str(m4["failures"][:2]))
        # nonterminal owned run fails hard
        a5, run_cli5 = persist_world({
            "runs": [{"run_id": _mk_uuid(0xB7), "status": "running",
                      "sync_def_id": owned[0]["id"]}]})
        a5.out_dir = str(tmp / "p5")
        ps.RELEASED_FOR_EXECUTION = True
        try:
            rc5 = ps.collect_body(a5, run_cli5)
        finally:
            ps.RELEASED_FOR_EXECUTION = False
        check("G5b-5 nonterminal owned run fails hard checks",
              rc5 != 0)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--make-ev":
        build_full_ev(Path(sys.argv[2]))
        print(f"[make-ev] canonical 22-stage synthetic EV at "
              f"{sys.argv[2]}")
        return 0
    fz = loadmod(HERE / "finalize_p25.py", "fz_arch")
    sa = loadmod(HERE / "standalone_p25.py", "sa_arch")
    ov = loadmod(HERE / "offline_verify_p25.py", "ov_arch")
    pf = loadmod(HERE / "verify_postfacts_p25.py", "pf_arch")
    coll = loadmod(HERE / "collect_postfacts_p25.py", "coll_arch")
    g1_g2_finalize(fz)
    g3_standalone(sa, ov)
    g4_abc(pf)
    g5_g6(pf, coll)
    print(f"\n[selftest-archive] pass={len(PASS)} fail={len(FAIL)}")
    for f in FAIL:
        print(f"  FAILED: {f}")
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
