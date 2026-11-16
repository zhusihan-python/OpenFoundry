#!/usr/bin/env python3
"""P2.6 W7 HTTP acceptance harness CLI (pure phase).

Subcommands:
  --list-scenarios          print the scenario registry
  --plan-live NAME          emit the exact future live argv + resource
                            ledger (PROPOSAL ONLY — nothing binds/starts)
  --selftest                run the pure selftest suite as an isolated
                            subprocess; real rc/stdout/stderr recorded
                            under results/w7/selftest-<UTC>/
  --run-live NAME           REFUSED unless the Root release file lists
                            `http-live-scenarios` (rc 3, zero HTTP)

This CLI never binds a port, never runs docker, and never touches any
service; live execution is Root-gated exactly like the serve mains.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from p26_http_lib import (RELEASE_PHASES, RESULTS_W7, read_release,
                          redact, release_allows, resolve_bases,
                          utcstamp)

SELFTEST = HERE / "selftest_p26_http.py"

# ---------------------------------------------------------------------------
# proposed live substrate (Root reviews BEFORE any bind/start)
# ---------------------------------------------------------------------------

LIVE_PLAN = {
    "phase": "proposal-only-2026-11-15",
    "note": "Nothing below has been executed. Host ports are PROPOSALS "
            "pending a free-port inspection step; container/service "
            "names are NEW P26-owned and must never collide with the "
            "old q3 fixture17090 or the removed P25 aux "
            "(p25fixture/p25proxy/p25connector, ports 17092-17094).",
    "never_reuse": {
        "old_q3_fixture": "fixture-1 (17090) — STILL RUNNING in q3; "
                          "read-only",
        "removed_p25": ["p25fixture (17092)", "p25 proxy (17093)",
                        "p25connector (17094)"],
    },
    "proposed_services": [
        {"name": "p26fixture", "host_port": 17095,
         "cmd": ["python3", "-I", "/srv/p26_rest_source.py", "serve",
                 "0.0.0.0", "8090",
                 "--admin-token-env", "P26_HTTP_ADMIN_TOKEN"],
         "release_phase": RELEASE_PHASES["fixture"],
         "compose": "override file http/compose-p26-http.yaml (to be "
                    "reviewed; up -d --no-deps p26fixture only)"},
        {"name": "p26proxy", "host_port": 17096,
         "cmd": ["python3", "-I", "/srv/p26_dataset_fault_proxy.py",
                 "serve", "0.0.0.0", "8091",
                 "http://dataset:50078",
                 "--admin-token-env", "P26_HTTP_ADMIN_TOKEN"],
         "release_phase": RELEASE_PHASES["proxy"],
         "upstream": "dataset-versioning-service in-network base "
                     "(exact value verified at live preflight; old "
                     "P25/q3 markers structurally refused)"},
        {"name": "p26connectora", "host_port": 17097,
         "image": "<W6 candidate image sha256 — placeholder, must come "
                  "from W6 build track facts>",
         "env": {"DATASET_SERVICE_URL": "http://p26proxy:8091",
                 "ALLOWED_EGRESS_HOSTS": "p26fixture"}},
        {"name": "p26connectorb", "host_port": 17098,
         "image": "<same candidate image>",
         "env": {"DATASET_SERVICE_URL": "http://p26proxy:8091",
                 "ALLOWED_EGRESS_HOSTS": "p26fixture"},
         "purpose": "two-instance recovery scenarios (exactly-one-claim)"},
    ],
    "preflight_steps_before_any_bind": [
        {"step": "free-port inspection",
         "argv": ["lsof", "-nP", "-iTCP:17095-17098", "-sTCP:LISTEN"],
         "expect": "empty output (all four free) — else pick new ports "
                   "and update this ledger BEFORE any proposal"},
        {"step": "docker ps whitelist snapshot",
         "argv": ["docker", "ps", "--format",
                  "{{.Names}}\t{{.Image}}\t{{.Status}}"],
         "expect": "q3 base3+fixture-1 unchanged; no p25 aux names; "
                   "no p26 names yet"},
        {"step": "candidate image identity",
         "argv": ["docker", "image", "inspect", "<candidate>",
                  "--format", "{{.Id}}"],
         "expect": "full sha256 from W6 build track; recorded in "
                   "resource ledger before connectors start"},
    ],
    "release_file_schema": {
        "path": "/private/tmp/openfoundry-p26-release.json",
        "released_for_must_include": [RELEASE_PHASES["fixture"],
                                      RELEASE_PHASES["proxy"],
                                      RELEASE_PHASES["live"]],
        "expires_utc": "YYYYMMDDTHHMMSSZ",
        "w7_bases": "R2 binding: the release must carry the four "
                    "Root-approved origins {fixture, proxy_admin, dvs, "
                    "connector} (from W6 deployment facts), plus the "
                    "OPTIONAL dvs_files origin (the Dataset FILES base "
                    "for the presigned download two-step, e.g. q3 "
                    "http://127.0.0.1:17078; defaults to the dvs "
                    "origin). CLI flags must EQUAL the approved origin "
                    "exactly and can never widen a credential "
                    "destination. userinfo/query/fragment and "
                    "old-substrate ports are structurally rejected.",
    },
    "port_facts_r2": {
        "dataset_go_default": 50117,        # Go default (NOT used in q3)
        "q3_dataset_port": 50078,           # q3 compose PORT — proxy
        # upstream http://dataset:50078 is CORRECT; do not change to the
        # Go default (Root verified against q3 compose).
        "q3_files_base": "http://127.0.0.1:17078",   # DATASET_FILES_BASE_URL
        "download_protocol": "DownloadFile = 302 Location presigned "
                             "local-fs (/v1/_internal/local-fs/…); the "
                             "harness follows ONE validated anonymous "
                             "hop against the approved files origin "
                             "(w7_bases.dvs_files, falling back to dvs)",
        "versions_protocol": "ListVersions = Page{data,next_cursor,"
                             "has_more}; bounded pagination walk with a "
                             "hard termination assertion",
    },
    "auth_handling": "TWO separated credentials, both memory-only: "
                     "(1) platform org JWT (OPENFOUNDRY_SMOKE_JWT) is "
                     "attached ONLY to dvs/connector clients; "
                     "(2) a random local admin credential "
                     "(P26_HTTP_ADMIN_TOKEN, shared by fixture/proxy "
                     "boots and the runner) rides X-P26-Admin-Token and "
                     "authenticates ONLY the fixture/proxy admin faces "
                     "(both fail closed without it). Neither value ever "
                     "appears in cmd/raw/rc/meta evidence.",
    "live_runner_argv_template": [
        "python3", "-I", "run_p26_http.py", "--run-live", "<scenario>",
        "--fixture-base", "http://127.0.0.1:17095",
        "--proxy-admin-base", "http://127.0.0.1:17096",
        "--dvs-base", "<q3 dataset in-network or forwarded base>",
        "--connector-base", "http://127.0.0.1:17097",
    ],
    "commit_proof_policy": "Proxy ledger is NEVER commit proof; final "
                           "proofs are DIRECT Dataset GET receipt + "
                           "files/download sha256 (and later W6-protected "
                           "SQL views).",
}


def plan_for(scenario_name):
    from p26_scenarios import SCENARIOS
    entry = SCENARIOS.get(scenario_name)
    if entry is None:
        return None
    plan = dict(LIVE_PLAN)
    plan["scenario"] = scenario_name
    plan["scenario_summary"] = entry["summary"]
    plan["scenario_requires"] = entry["requires"]
    plan["live_release_phase"] = RELEASE_PHASES["live"]
    return plan


def cmd_list_scenarios():
    from p26_scenarios import SCENARIOS
    for name, entry in SCENARIOS.items():
        print(f"{name}: {entry['summary']}")
    return 0


def cmd_plan_live(name, out_path):
    plan = plan_for(name)
    if plan is None:
        print(f"unknown scenario {name!r}", file=sys.stderr)
        return 2
    text = json.dumps(plan, indent=1, sort_keys=True) + "\n"
    if out_path:
        Path(out_path).write_text(text, encoding="utf-8")
        print(f"plan written: {out_path}")
    else:
        print(text)
    return 0


def cmd_selftest():
    ts = utcstamp()
    out_dir = RESULTS_W7 / f"selftest-{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {"started_utc": ts, "selftests": [], "all_ok": True}
    cp = subprocess.run([sys.executable, "-I", "-B", str(SELFTEST)],
                        capture_output=True, text=True, timeout=600)
    (out_dir / "selftest_p26_http.stdout").write_text(cp.stdout or "")
    (out_dir / "selftest_p26_http.stderr").write_text(cp.stderr or "")
    (out_dir / "selftest_p26_http.rc").write_text(f"{cp.returncode}\n")
    tail = (cp.stdout or "").strip().splitlines()
    verdict = tail[-1] if tail else "(no output)"
    summary["selftests"].append({"name": "selftest_p26_http.py",
                                 "rc": cp.returncode,
                                 "verdict_line": verdict})
    summary["all_ok"] = cp.returncode == 0
    (out_dir / "SUMMARY.json").write_text(
        json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(f"[w7-selftest] rc={cp.returncode} — {verdict}")
    print(f"[w7-selftest] transcript: {out_dir}")
    if cp.returncode != 0:
        print((cp.stderr or "").strip()[-3000:])
    return cp.returncode


def live_exit_code(facts):
    """R2 item 1: rc 0 IFF the scenario outcome is pass AND the cleanup
    was verified. A pass with failed/unverified cleanup (fail-cleanup)
    or any failure keeps the CLI nonzero — no fake green."""
    if not isinstance(facts, dict):
        return 1
    if facts.get("outcome") != "pass":
        return 1
    if facts.get("cleanup_verified") is not True:
        return 1
    return 0


def cmd_run_live(name, flag_bases):
    ok, why = release_allows(RELEASE_PHASES["live"])
    if not ok:
        print(f"[w7-live] REFUSING to run live scenario: {why}",
              file=sys.stderr)
        print("[w7-live] zero HTTP calls performed", file=sys.stderr)
        return 3
    release_doc = read_release()
    bases, err = resolve_bases(flag_bases, release_doc)
    if err:
        print(f"[w7-live] REFUSING: {err}", file=sys.stderr)
        return 3
    from p26_http_lib import UrllibTransport, redact_json
    from p26_scenarios import SCENARIOS, LiveCtx, run_scenario
    if name not in SCENARIOS:
        print(f"unknown scenario {name!r}", file=sys.stderr)
        return 2
    if "w7_cases" in release_doc and name not in release_doc["w7_cases"]:
        print("[w7-live] REFUSING: scenario outside Root w7_cases", file=sys.stderr)
        return 3
    entry = SCENARIOS[name]
    requires = entry.get("requires", [])

    # r4 safe wiring: B-requiring scenarios need an approved DISTINCT
    # connector_b origin — refuse BEFORE anything is created.
    if "connector_b" in requires and "connector_b" not in bases:
        print("[w7-live] REFUSING: scenario requires a second connector "
              "instance but w7_bases does not approve a DISTINCT "
              "connector_b origin", file=sys.stderr)
        return 3

    # r4 safe wiring: ops-requiring scenarios construct the owned
    # adapter ONLY from the Root release's exact resource facts; any
    # missing fact or credential env refuses with ZERO attempts.
    ops = None
    if "connector_ops" in requires:
        import p26_owned_ops as owned_ops_mod
        from p26_owned_ops import OwnedAuxOps, validate_release_aux
        aux, aux_err = validate_release_aux(release_doc)
        if aux_err:
            print(f"[w7-live] REFUSING: {aux_err}", file=sys.stderr)
            return 3
        missing = [owned_ops_mod.CREDENTIAL_ENV[kind]
                   for kind in entry.get("credential_kinds", ())
                   if kind in owned_ops_mod.CREDENTIAL_ENV
                   and not os.environ.get(
                       owned_ops_mod.CREDENTIAL_ENV[kind])]
        if missing:
            print("[w7-live] REFUSING: private credential envs missing: "
                  + ", ".join(sorted(set(missing)))
                  + " (names only; values never printed)",
                  file=sys.stderr)
            return 3
        ok, why = release_allows(owned_ops_mod.OWNED_OPS_PHASE)
        if not ok:
            print("[w7-live] REFUSING: " + why, file=sys.stderr)
            return 3
        try:
            owned_ops_mod.validate_pg_observer(release_doc.get("w7_pg_observer"))
            ops = OwnedAuxOps(aux, auth_identity=release_doc.get("w7_auth_identity"))
            for kind in entry.get("credential_kinds", ()):
                ops.credential(kind)  # shape checks; values remain memory-only
        except owned_ops_mod.OwnedOpsError as exc:
            print("[w7-live] REFUSING: " + str(exc), file=sys.stderr)
            return 3

    platform_auth = os.environ.get("OPENFOUNDRY_SMOKE_JWT")
    admin_token = os.environ.get("P26_HTTP_ADMIN_TOKEN")
    if not platform_auth:
        print("[w7-live] OPENFOUNDRY_SMOKE_JWT missing", file=sys.stderr)
        return 3
    if not admin_token:
        print("[w7-live] P26_HTTP_ADMIN_TOKEN missing (admin faces fail "
              "closed)", file=sys.stderr)
        return 3
    if release_doc.get("w7_aux_cohort") == 2:
        from p26_owned_ops import validate_auth_identity, OwnedOpsError
        try:
            validate_auth_identity(release_doc.get("w7_auth_identity"),
                [platform_auth, os.environ.get("P26_W7_JWT_WRITER")])
        except OwnedOpsError as exc:
            print("[w7-live] REFUSING: " + str(exc), file=sys.stderr)
            return 3
    ctx = LiveCtx(UrllibTransport(), bases, platform_auth, admin_token,
                  RESULTS_W7 / ("S2-attempts" if release_doc.get("w7_aux_cohort") == 2 else "attempts"), name, ops=ops,
                  approved_files_origin=bases.get("dvs_files"))
    try:
        facts = run_scenario(name, ctx)
    except Exception as exc:                    # noqa: BLE001
        (ctx.evidence.path / "RESULT.json").write_text(
            json.dumps({"result": redact_json(ctx.facts),
                        "error": str(exc)}, indent=1, sort_keys=True,
                       default=str) + "\n", encoding="utf-8")
        print(f"[w7-live] {name}: FAIL — {exc}", file=sys.stderr)
        return 1
    rc = live_exit_code(facts)
    (ctx.evidence.path / "RESULT.json").write_text(
        json.dumps(redact_json(facts), indent=1, sort_keys=True,
                   default=str) + "\n", encoding="utf-8")
    if rc == 0:
        print(f"[w7-live] {name}: PASS (outcome=pass, cleanup verified)")
    else:
        print(f"[w7-live] {name}: FAIL — outcome={facts.get('outcome')!r}"
              f" cleanup_verified={facts.get('cleanup_verified')!r}",
              file=sys.stderr)
    return rc


def main(argv):
    if "--list-scenarios" in argv:
        return cmd_list_scenarios()
    if "--selftest" in argv:
        return cmd_selftest()
    if "--plan-live" in argv:
        idx = argv.index("--plan-live")
        name = argv[idx + 1] if len(argv) > idx + 1 else None
        if not name:
            print("--plan-live requires a scenario name", file=sys.stderr)
            return 2
        out = None
        if "--out" in argv:
            out = argv[argv.index("--out") + 1]
        return cmd_plan_live(name, out)
    if "--run-live" in argv:
        idx = argv.index("--run-live")
        name = argv[idx + 1] if len(argv) > idx + 1 else None
        if not name:
            print("--run-live requires a scenario name", file=sys.stderr)
            return 2
        flags = {"fixture": "--fixture-base",
                 "proxy_admin": "--proxy-admin-base",
                 "dvs": "--dvs-base",
                 "connector": "--connector-base"}
        flag_bases = {}
        for key, flag in flags.items():
            if flag in argv:
                i = argv.index(flag)
                flag_bases[key] = argv[i + 1] if i + 1 < len(argv) \
                    else None
        return cmd_run_live(name, flag_bases)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
