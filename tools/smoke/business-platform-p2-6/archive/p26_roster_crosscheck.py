#!/usr/bin/env python3
"""P2.6 W9 — formal roster crosscheck against Root's accepted JSONs.

Binds the DRAFT W9 roster to the Root external acceptance files and (in
workspace mode) to the referenced evidence directories. This is the
"正式 roster 核验" step: the roster may only carry `accepted` items whose
facts are actually present in the accepted JSONs, and every still-pending
flag in those JSONs must be covered by a roster `pending` item — a roster
that silently claims completion, or goes stale after a Root update, is
REFUSED. Plans/static-only acceptances never satisfy anything (they are
reported, not promoted).

Small by design: no package assembly, no seal, no generic framework.
Read-only: opens only the files given on the command line (plus evidence
existence stats when --repo is passed). Never imports/executes old P25
tools; never fabricates a pass.

Usage:
  python3 -I -B p26_roster_crosscheck.py check \
      --roster <p26_formal_roster.json> \
      --pg-accepted <S-first-PG-all-accepted.json> \
      --build-accepted <S-build-two-composite-accepted.json> \
      --unit-accepted <final-unit-S-accepted.json> \
      [--extra-accepted <file>]... [--repo <project root>]
  python3 -I -B p26_roster_crosscheck.py selftest

check rc: 0 CROSSCHECK-OK; 1 refused (problems listed). selftest rc: 0
iff every tamper control refuses and the happy control passes.
"""

import argparse
import json
import os
import re
import sys
import tempfile

HEX64 = re.compile(r"^[0-9a-f]{64}$")


def hex64(value):
    """Accept bare 64-hex or docker-style 'sha256:<64-hex>'."""
    v = str(value or "")
    if v.startswith("sha256:"):
        v = v[len("sha256:"):]
    return bool(HEX64.match(v))

# roster pending id  <-  accepted-json pending flag that must stay covered
PENDING_COVER = [
    ("http-live-full-suite", ("pg", "HTTP_build_deploy_final_pending")),
    ("q3-deploy-actual", ("build", "actual_q3_deploy_pending")),
]


def load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def flag(doc, dotted):
    cur = doc
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def check(roster, pg, build, unit, extras, repo):
    probs, bound = [], {}

    def want(cond, msg):
        if not cond:
            probs.append(msg)
        return cond

    want(roster.get("roster_version", "").startswith("p26-w9-draft"),
         f"roster_version {roster.get('roster_version')!r} not a W9 draft")
    items = {i.get("id"): i.get("status") for i in roster.get("items", [])}
    want(bool(items), "roster has no items")

    # --- S equality across roster + the three accepted files
    s_vals = {
        "roster": roster.get("source", {}).get("go_sql_sha256"),
        "pg": pg.get("source"),
        "build": build.get("source"),
        "unit": unit.get("source"),
    }
    want(all(HEX64.match(v or "") for v in s_vals.values()),
         f"source values not all 64-hex: { {k: v for k, v in s_vals.items()} }")
    want(len(set(s_vals.values())) == 1,
         f"source mismatch across roster/accepted files: {s_vals}")
    bound["source"] = s_vals["roster"]

    # --- PG accepted facts
    want(pg.get("new_normal") == 27, f"pg new_normal={pg.get('new_normal')} != 27")
    want(pg.get("new_race") == 27, f"pg new_race={pg.get('new_race')} != 27")
    want(pg.get("unified") == 114, f"pg unified={pg.get('unified')} != 114")
    chains = pg.get("seven_actual_chains_all0", [])
    want(len(chains) == 7, f"expected 7 actual chains, got {len(chains)}")
    new_req = sum(c.get("required", 0) for c in chains if "new" in str(c.get("outer", "")))
    uni_req = sum(c.get("required", 0) for c in chains if "unified" in str(c.get("outer", "")))
    want(new_req == 27 + 27, f"new-chain required sum {new_req} != 54 (27 normal + 27 race)")
    want(uni_req == 114, f"unified-chain required sum {uni_req} != 114")
    for c in chains:
        cid = os.path.basename(str(c.get("outer", "?")))
        want(c.get("process_rc") == 0, f"chain {cid}: process_rc={c.get('process_rc')}")
        want(c.get("wrapper_rc") == 0, f"chain {cid}: wrapper_rc={c.get('wrapper_rc')}")
        want(c.get("allgates0") is True, f"chain {cid}: allgates0={c.get('allgates0')}")
        want(c.get("source") == s_vals["roster"], f"chain {cid}: source differs from S")
        if repo:
            outer = str(c.get("outer", ""))
            want(os.path.isdir(os.path.join(repo, outer)), f"chain {cid}: outer dir missing: {outer}")
            inner = str(c.get("inner", ""))
            ip = inner if os.path.isabs(inner) else os.path.join(repo, inner)
            want(os.path.isdir(ip), f"chain {cid}: inner dir missing: {inner}")
    bound["pg_chains"] = len(chains)

    # --- composite build accepted facts
    want(build.get("composite_not_original_pass") is True,
         "build acceptance is not composite-flagged")
    cands = build.get("candidates", [])
    want(len(cands) == 2 and {c.get("service") for c in cands} == {"dataset", "connector"},
         f"build candidates wrong: {[c.get('service') for c in cands]}")
    for c in cands:
        svc = c.get("service", "?")
        want(hex64(c.get("image")), f"build {svc}: image not 64-hex")
        want(hex64(c.get("binary_sha256")), f"build {svc}: binary not 64-hex")
        want(c.get("original_failed_verify_preserved") is True,
             f"build {svc}: original failure not preserved")
        want(c.get("resume_allrc0") is True, f"build {svc}: resume not all rc0")
        want(hex64(c.get("stopped_build_cid_cleaned")),
             f"build {svc}: cleaned CID not 64-hex")
        if repo:
            want(os.path.isdir(os.path.join(repo, str(c.get("original_build", "")))),
                 f"build {svc}: original dir missing")
            want(os.path.isdir(os.path.join(repo, str(c.get("resume", "")))),
                 f"build {svc}: resume dir missing")
    bound["build_candidates"] = len(cands)

    # --- unit accepted facts
    want(unit.get("all_six_original_steps_rc0") is True, "unit: not all six steps rc0")
    want(unit.get("source_before_after_same_S") is True, "unit: source before/after differ")
    for svc, n in (("dataset", 8), ("pipeline", 15), ("connector", 39)):
        got = unit.get(svc, {}).get("tested_packages_ok")
        want(got == n, f"unit {svc}: tested_packages_ok={got} != {n}")
    opt = str(unit.get("optional_external_provider_cluster_sidecar", ""))
    want("not verified" in opt.lower(),
         "unit: optional-external caveat missing (must never claim 0-skip/verified optionals)")
    if repo:
        want(os.path.isdir(os.path.join(repo, str(unit.get("evidence", "")))),
             "unit: evidence dir missing")
    bound["unit_packages"] = "8/15/39"

    # --- pending coherence (roster must neither over-claim nor go stale)
    docs = {"pg": pg, "build": build}
    for rid, (doc_key, dotted) in PENDING_COVER:
        val = flag(docs[doc_key], dotted)
        status = items.get(rid)
        if val is True:
            want(status == "pending",
                 f"accepted json still says pending ({doc_key}.{dotted}) but roster item {rid!r} is {status!r}")
        elif val is False:
            want(status not in (None, "pending"),
                 f"accepted json closed {doc_key}.{dotted} but roster item {rid!r} is {status!r} — roster stale")
        else:
            probs.append(f"pending flag {doc_key}.{dotted} missing from accepted json")
    # roster accepted ids must exist in the bound facts at all
    known_accepted = {"pg-new-27-normal-27-race", "pg-unified-114",
                      "unit-3services-full-untagged", "build-two-composite"}
    for rid, status in items.items():
        if status == "accepted" and rid not in known_accepted:
            probs.append(f"roster marks {rid!r} accepted but no accepted json backs it")

    # Original process files and business semantics, independent of acceptance booleans.
    if repo:
        import importlib.util
        spec = importlib.util.spec_from_file_location("p26_original_proofs", os.path.join(os.path.dirname(__file__), "p26_original_proofs.py"))
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        read_files, original_results = {}, []
        for item in roster.get("items", []):
            if item.get("status") != "accepted":
                continue
            want(bool(item.get("satisfiers")), "original proof: nonempty satisfiers required")
            for sid in item.get("satisfiers", []):
                contract = item.get("contracts", {}).get(sid, {})
                try:
                    reader = module.Reader(repo)
                    reader.rc(contract.get("rc_artifact", ""), contract.get("rc"))
                    result, read = module.validate(repo, contract.get("proof", {}), s_vals["roster"])
                    read_files.update(reader.files); read_files.update(read)
                    original_results.append(dict(id=sid, result=result))
                except (ValueError, OSError, KeyError, TypeError) as e:
                    probs.append(f"original proof {sid}: {e}")
        bound["original_results"] = original_results
        bound["original_files"] = read_files
    else:
        bound["original_proofs"] = "NOT CHECKED (facts-only mode; never formal verification)"

    bound["extra_accepted_files"] = len(extras)
    for i, ex in enumerate(extras):
        want(isinstance(ex, dict) and bool(ex), f"extra accepted file #{i + 1} empty/unparseable")
    bound["roster_items"] = {rid: status for rid, status in sorted(items.items())}
    return probs, bound


# ----------------------------------------------------------------- selftest
def _write(d, name, obj):
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(obj, fh)
    return p


def selftest():
    S = "b" * 64
    base_roster = {
        "roster_version": "p26-w9-draft-9",
        "source": {"go_sql_sha256": S},
        "items": [
            {"id": "pg-new-27-normal-27-race", "status": "accepted"},
            {"id": "pg-unified-114", "status": "accepted"},
            {"id": "unit-3services-full-untagged", "status": "accepted"},
            {"id": "build-two-composite", "status": "accepted"},
            {"id": "http-live-full-suite", "status": "pending"},
            {"id": "q3-deploy-actual", "status": "pending"},
        ],
    }
    img, cid = "f" * 64, "0" * 64

    def chain(outer, req):
        return {"outer": f"results/w6/pg/{outer}", "inner": f"results/w6/pg/{outer}",
                "required": req, "process_rc": 0, "wrapper_rc": 0,
                "allgates0": True, "source": S}

    def pg_doc(**over):
        d = {"source": S, "new_normal": 27, "new_race": 27, "unified": 114,
             "seven_actual_chains_all0": [
                 chain("real-S-new-connector-normal-attempt1", 18),
                 chain("real-S-new-connector-race-attempt1", 18),
                 chain("real-S-new-dataset-normal-attempt1", 9),
                 chain("real-S-new-dataset-race-attempt1", 9),
                 chain("real-S-unified-connector-normal-attempt1", 82),
                 chain("real-S-unified-dataset-normal-attempt1", 15),
                 chain("real-S-unified-pipeline-normal-attempt1", 17)],
             "HTTP_build_deploy_final_pending": True}
        d.update(over)
        return d

    def build_doc(**over):
        d = {"source": S, "composite_not_original_pass": True, "actual_q3_deploy_pending": True,
             "candidates": [
                 {"service": "dataset", "image": img, "binary_sha256": img,
                  "original_build": "results/w6/build/bd", "resume": "results/w6/build/rd",
                  "original_failed_verify_preserved": True, "resume_allrc0": True,
                  "stopped_build_cid_cleaned": cid},
                 {"service": "connector", "image": img, "binary_sha256": img,
                  "original_build": "results/w6/build/bc", "resume": "results/w6/build/rc",
                  "original_failed_verify_preserved": True, "resume_allrc0": True,
                  "stopped_build_cid_cleaned": cid}]}
        d.update(over)
        return d

    def unit_doc(**over):
        d = {"source": S, "all_six_original_steps_rc0": True, "source_before_after_same_S": True,
             "dataset": {"tested_packages_ok": 8}, "pipeline": {"tested_packages_ok": 15},
             "connector": {"tested_packages_ok": 39},
             "evidence": "results/w1b/final-unit-x",
             "optional_external_provider_cluster_sidecar": "not verified; caveat kept"}
        d.update(over)
        return d

    tmp = tempfile.mkdtemp(prefix="p26_crosscheck_selftest_")
    results = []

    def run(name, roster, pg, build, unit, repo_dirs, expect_ok, substr=""):
        rdir = None
        if repo_dirs:
            rdir = tempfile.mkdtemp(prefix="p26_crosscheck_repo_")
            for rel in repo_dirs:
                os.makedirs(os.path.join(rdir, rel.strip("/")), exist_ok=True)
        probs, _ = check(roster, pg, build, unit, [], rdir)
        joined = " | ".join(probs)
        okk = (not probs) == expect_ok and (not substr or substr in joined)
        results.append((name, okk, f"probs={joined[:150]}"))

    dirs = [f"results/w6/pg/real-S-new-connector-normal-attempt1", "results/w6/pg/real-S-new-connector-race-attempt1",
            "results/w6/pg/real-S-new-dataset-normal-attempt1", "results/w6/pg/real-S-new-dataset-race-attempt1",
            "results/w6/pg/real-S-unified-connector-normal-attempt1", "results/w6/pg/real-S-unified-dataset-normal-attempt1",
            "results/w6/pg/real-S-unified-pipeline-normal-attempt1",
            "results/w6/build/bd", "results/w6/build/rd", "results/w6/build/bc", "results/w6/build/rc",
            "results/w1b/final-unit-x"]
    # inner dirs are /abs/<name> which we do NOT create; use no-repo happy path
    run("happy", base_roster, pg_doc(), build_doc(), unit_doc(), None, True)
    run("empty-dirs-not-original-proof", base_roster, pg_doc(), build_doc(), unit_doc(), dirs, False, "original proof")

    run("source-mismatch", base_roster, pg_doc(source="c" * 64), build_doc(), unit_doc(), None, False, "source mismatch")
    run("wrong-pg-counts", base_roster, pg_doc(new_normal=26), build_doc(), unit_doc(), None, False, "new_normal")
    bad_chain = pg_doc()
    bad_chain["seven_actual_chains_all0"][2]["process_rc"] = 1
    run("chain-rc-nonzero", base_roster, bad_chain, build_doc(), unit_doc(), None, False, "process_rc")
    bad_build = build_doc()
    bad_build["candidates"][0]["original_failed_verify_preserved"] = False
    run("composite-preservation-broken", base_roster, pg_doc(), bad_build, unit_doc(), None, False, "not preserved")
    run("unit-caveat-dropped", base_roster, pg_doc(), build_doc(),
        unit_doc(optional_external_provider_cluster_sidecar="all fine"), None, False, "caveat missing")
    stale = json.loads(json.dumps(base_roster))
    stale["items"] = [i for i in stale["items"] if i["id"] != "http-live-full-suite"]
    run("roster-missing-pending-item", stale, pg_doc(), build_doc(), unit_doc(), None, False, "http-live-full-suite")
    closed = json.loads(json.dumps(base_roster))
    for i in closed["items"]:
        if i["id"] == "http-live-full-suite":
            i["status"] = "accepted"
    run("roster-overclaims-pending", closed, pg_doc(), build_doc(), unit_doc(), None, False, "still says pending")
    stale2 = json.loads(json.dumps(base_roster))
    run("accepted-json-closed-but-roster-stale", stale2,
        pg_doc(HTTP_build_deploy_final_pending=False), build_doc(), unit_doc(), None, False, "stale")
    run("missing-evidence-dir", base_roster, pg_doc(), build_doc(), unit_doc(), dirs[:-1], False, "missing")

    failed = [r for r in results if not r[1]]
    for name, passed, note in results:
        print(f"[selftest:crosscheck] {'PASS' if passed else 'FAIL'} {name} — {note}")
    print(f"[selftest:crosscheck] controls={len(results)} failed={len(failed)}")
    return 1 if failed else 0


def main():
    sys.dont_write_bytecode = True
    ap = argparse.ArgumentParser(prog="p26_roster_crosscheck")
    sub = ap.add_subparsers(dest="cmd", required=True)
    cp = sub.add_parser("check")
    cp.add_argument("--roster", required=True)
    cp.add_argument("--pg-accepted", required=True)
    cp.add_argument("--build-accepted", required=True)
    cp.add_argument("--unit-accepted", required=True)
    cp.add_argument("--extra-accepted", action="append", default=[])
    cp.add_argument("--repo")
    sub.add_parser("selftest")
    args = ap.parse_args()
    if args.cmd == "selftest":
        return selftest()
    extras = [load(p) for p in args.extra_accepted]
    probs, bound = check(load(args.roster), load(args.pg_accepted),
                         load(args.build_accepted), load(args.unit_accepted),
                         extras, args.repo)
    verdict = "CROSSCHECK-OK" if not probs else "CROSSCHECK-REFUSED"
    print(json.dumps({"verdict": verdict, "problems": probs, "bound": bound}, indent=1))
    return 0 if not probs else 1


if __name__ == "__main__":
    sys.exit(main())
