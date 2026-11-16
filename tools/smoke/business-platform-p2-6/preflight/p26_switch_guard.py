#!/usr/bin/env python3
"""P2.6 W6 candidate-switch baseline guard + new-resource deploy plan.

guard       (executable read-only, real seal by default): BEFORE any
            candidate switch the FULL baseline must be green —
            (a) verify_seal FULL over all 5016 sealed files (sample mode
                can never pass here), (b) derive_protected: exact 9 Run
            rows with full 27 fields + parent chains, whole owner-8 with
            FULL CP originals (complete public checkpoint objects +
            matched public runs). Reuses my own cohort CLI functions
            (same codebase); anything less refuses.
deploy-plan  typed ledger + EXACT commands for the new P26 HTTP fixture
            resources (W7-owned code; W6 owns deployment plan + guards):
            p26fixture:17095 / p26proxy:17096 / p26connectora:17097 /
            p26connectorb:17098 (per W7-HTTP-INTERFACE §ports). Old
            q3 fixture-1 (17090) and P25 aux (17092-94) are NEVER
            restarted/reused; lsof empty-check precedes any bind; binds
            themselves wait for Root release.

No binds/restarts/builds/SQL happen here — plans + guards only.
"""

import argparse
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402
from cohort import p26_cohort_cli as C  # noqa: E402

TOOL = "p26_switch_guard"

# W7-HTTP-INTERFACE §ports (read-only reference; W7 owns the code)
NEW_RESOURCES = [
    {"kind": "http-fixture", "service": "p26fixture", "host_port": 17095,
     "container_port": 8090, "owner": "W7 code / W6 deploy plan"},
    {"kind": "http-proxy", "service": "p26proxy", "host_port": 17096,
     "container_port": 8090, "owner": "W7 code / W6 deploy plan"},
    {"kind": "owned-aux-connector", "service": "p26connectora",
     "host_port": 17097, "container_port": 50088,
     "owner": "W7 harness / W6 deploy plan"},
    {"kind": "owned-aux-connector", "service": "p26connectorb",
     "host_port": 17098, "container_port": 50088,
     "owner": "W7 harness / W6 deploy plan"},
]
FORBIDDEN_OLD_PORTS = (17090, 17091, 17092, 17093, 17094)
FORBIDDEN_OLD_NAMES = ("openfoundry-platform-p1-q3v9-fixture-1",)


def cmd_guard(args):
    final_dir = Path(args.final_dir).resolve()
    results = {}
    ok = True
    try:
        seal = C.verify_seal(final_dir, args.seal_sha, args.files_count)
        results["verify_seal_full"] = {k: seal[k] for k in
                                       ("ok", "files_total",
                                        "files_checked", "partial")}
        ok = ok and seal["ok"] and not seal["partial"]
        if not ok:
            results["verify_seal_full"]["problems"] = seal["problems"][:8]
        derived = C.derive_protected(final_dir, args.seal_sha,
                                     args.files_count)
        p9, cp8 = derived["protected9"], derived["cp8"]
        fields_ok = all(len(r["public_projection_27"]) == 27 for r in p9)
        owner8_full = all(
            {"cp_owner_row_full", "quiesce_checkpoint_full",
             "matched_public_run"} <= set(e) for e in cp8)
        results["derive_protected"] = {
            "protected9": len(p9), "cp8": len(cp8),
            "all_27_fields": fields_ok,
            "owner8_full_cp_originals": owner8_full,
            "h10_run_id": derived.get("h10_run_id")}
        ok = ok and len(p9) == 9 and len(cp8) == 8 and fields_ok and \
            owner8_full
    except C.CohortRefusal as e:
        ok = False
        results["refused"] = str(e)
    out = {"utc": G.now_utc(), "ok": ok, "checks": results,
           "scope": "historical_baseline_only",
           "policy": "THIS GUARD VERIFIES THE HISTORICAL SEALED BASELINE "
                     "ONLY (full seal + 9x27fields/parents + whole "
                     "owner-8 full CP). It does NOT read current q3 "
                     "state — it is NOT a current-preservation precheck "
                     "and its rc0 must never be reported as one. Fresh "
                     "current 9/8 + image/volume/fixture/safe-HTTP "
                     "collection is W8's p26_current_preflight scope."}
    G.write_json(Path(args.out), out)
    print(f"[{TOOL}] guard ok={ok} -> {args.out}")
    return 0 if ok else 1


def cmd_deploy_plan(args):
    problems = []
    for r in NEW_RESOURCES:
        if r["host_port"] in FORBIDDEN_OLD_PORTS:
            problems.append(f"{r['service']} reuses forbidden old port "
                            f"{r['host_port']}")
        if r["service"] in FORBIDDEN_OLD_NAMES:
            problems.append(f"{r['service']} collides with old fixture")
    if problems:
        G.write_json(Path(args.out), {"ok": False, "problems": problems})
        print(f"[{TOOL}] deploy-plan REFUSED: {problems}", file=sys.stderr)
        return 1
    plan = {
        "utc": G.now_utc(),
        "preconditions": [
            "HISTORICAL guard green (this tool) AND W8 current "
            "preflight green — both required before any switch",
            "lsof -nP -iTCP:17095-17098 -sTCP:LISTEN  # MUST be empty",
            "read-only ps: old q3 fixture-1(17090)/base(17078/17081/17088)"
            " present & untouched; old P25 aux(17092-94) absent",
            "W7 http/fixture code released by Root; binds only under "
            "Root release"],
        "typed_ledger": NEW_RESOURCES,
        "exact_commands_after_release": [
            ["docker", "run", "-d", "--pull=never", "--name", r["service"],
             "--network", G.P26_NET, "--label", G.P26_OWNER_LABEL,
             "-p", f"127.0.0.1:{r['host_port']}:{r['container_port']}",
             "<W7-RELEASED-IMAGE-FULL-ID>"]
            for r in NEW_RESOURCES],
        "never": ["restart/reuse openfoundry-platform-p1-q3v9-fixture-1 "
                  "(17090)", "bind 17090-17094", "touch old 9 runs / "
                  "owner-8 CP", "q3 base restart"],
        "facts": {
            "aux_image_reuse": "connectorA/B share ONE new Connector "
                               "candidate FULL image ID (no duplicate "
                               "build; root build-round1 #7/#13)",
            "build_scope": "candidates = Dataset + Connector only; "
                           "Pipeline unchanged baseline (no P26 delta)",
            "q3_dataset_port": "explicit 50078 (actual; NOT Go default "
                               "50117) — W7 proxy upstream uses 50078",
            "w7_layout_requirements": [
                "aux fixture/proxy containers preserve NEW P26 root "
                "layout via READ-ONLY mounts (/srv/p26/fixture/... + "
                "/srv/p26/http/...); no P25-style one-script flatten",
                "root release JSON mounted read-only into container + "
                "P26_RELEASE_FILE env so the serve gate works",
                "admin secret lives in memory/private env only"],
            "future_main_switch_shape": "Dataset -> health + readonly "
                "preservation -> Connector; image-only NEW override keeps "
                "old q3 dataset-files:/home/nonroot mount and all "
                "bindings; compose up flags --no-deps --no-build "
                "--pull=never with FULL image IDs bound (no mutable "
                "tags); post-switch: read health of old outputs, loops 0, "
                "current grants none"},
    }
    G.write_json(Path(args.out), plan)
    print(f"[{TOOL}] deploy-plan -> {args.out} (4 typed resources, "
          "17095-17098; binds wait for Root)")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["guard", "deploy-plan"])
    ap.add_argument("--final-dir", default=str(G.P25_FINAL_DIR))
    ap.add_argument("--seal-sha", default=G.EXPECTED_SEAL_SHA256)
    ap.add_argument("--files-count", type=int,
                    default=G.EXPECTED_SEALED_FILES)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    if args.mode == "guard":
        return cmd_guard(args)
    return cmd_deploy_plan(args)


if __name__ == "__main__":
    sys.exit(main())
