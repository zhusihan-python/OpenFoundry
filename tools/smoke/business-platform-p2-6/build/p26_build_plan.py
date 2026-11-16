#!/usr/bin/env python3
"""P2.6 W6 candidate build / preservation swap / fixture-HTTP plan CLI.

PLAN-ONLY. Real execution additionally needs --execute + a Root release
token listing the phase + every argv gate green. Negative contracts: a
binding missing the old running identity, a bad candidate SHA, or a URL
that hits the REMOVED P25 fixture/proxy is refused with ZERO docker/HTTP
calls (the executor is unreachable before those gates pass).

Modes:
  plan-build        base3 (dataset/pipeline/connector) + P26 aux override
                    generated INTO this track dir; cached images only (no
                    --pull); source content-hash before/after hard gate
  plan-swap         preservation swap: requires (a) the current running
                    old identity (whitelist docker ps/inspect facts), (b)
                    full-64-hex candidate image sha, (c) the protected
                    roster JSON produced by p26_cohort_cli derive-protected
  plan-fixture-http new-P26-fixture-only HTTP acceptance argv; removed P25
                    fixture/proxy URLs are refused outright
"""

import argparse
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402

TOOL = "p26_build_plan"
Q3_COMPOSE = "/private/tmp/openfoundry-platform-p1-q3v9/compose.yaml"
SOURCE_HASH_ARGV = ["python3", "-I", "-B",
                    str(G.ROOT_P26 / "common" / "p26_source_hash.py"),
                    "--repo", G.P26_SRC]
AUX_COMPOSE = str(G.ROOT_P26 / "build" / "compose-p26-aux.yaml")


def plan_build(expect_source_sha: str = None):
    steps = [
        G.step("b-src-before", "build",
               "source content hash BEFORE (P25-parity recipe via "
               "p26_source_hash.py)",
               SOURCE_HASH_ARGV),
        G.step("b-build-base3", "build",
               "q3 base compose BUILD only (no up/down/restart)",
               ["docker", "compose", "-f", Q3_COMPOSE,
                "--project-name", G.Q3_PROJECT, "build", "dataset",
                "pipeline", "connector"],
               write_class="p26-private",
               gates=["q3 .env (mode 600) sourced ONLY for interpolation; "
                      "values never printed/archived",
                      "no --pull; base images must already be cached"]),
        G.step("b-build-aux", "build",
               "P26 aux service build with THIS track's override file",
               ["docker", "compose", "-f", Q3_COMPOSE, "-f", AUX_COMPOSE,
                "--project-name", G.Q3_PROJECT, "build", "p26connector"],
               write_class="p26-private",
               gates=["compose-p26-aux.yaml is generated only inside "
                      "business-platform-p2-6/build — frozen yamls stay "
                      "untouched"]),
        G.step("b-image-ids", "build",
               "record FULL sha256 image ids (never short id, never a "
               "running container's image field)",
               ["docker", "image", "inspect",
                f"{G.Q3_PROJECT}-connector", "--format", "{{.Id}}"],
               gates=["repeat per built service image"]),
        G.step("b-src-after", "build", "source content hash AFTER "
               "(P25-parity recipe)",
               SOURCE_HASH_ARGV),
    ]
    return {"header": G.plan_header(TOOL, "build"), "steps": steps,
            "hard_gates": {
                "source-gate": "before == after" + (
                    f" == baseline {expect_source_sha}"
                    if expect_source_sha else
                    " (baseline sha optional input: P26 export-baseline "
                    "value or Root pin)"),
                "wrapper": "wrapper rc 2 on any gate violation; original "
                           "docker rcs preserved untouched"}}


def _load_binding(path: Path):
    """Swap binding input schema (all required, no defaults):
    {candidate_service: str,
     candidate_image_sha256: 64-hex,
     old_running: [{name, image}],           # current identity being kept
     protected_roster: path to derive-protected output JSON}
    """
    b = json.loads(path.read_text())
    for k in ("candidate_service", "candidate_image_sha256", "old_running",
              "protected_roster"):
        if k not in b:
            raise G.GateRefusal(f"binding missing key {k!r}")
    if not G.HEX64.match(str(b["candidate_image_sha256"])):
        raise G.GateRefusal("candidate_image_sha256 is not full 64-hex — "
                            "refusing (never a short id / tag)")
    if not isinstance(b["old_running"], list) or not b["old_running"]:
        raise G.GateRefusal("old_running identity missing — swap without "
                            "the OLD running identity is refused")
    for e in b["old_running"]:
        if not (isinstance(e, dict) and e.get("name") and e.get("image")):
            raise G.GateRefusal(f"old_running entry malformed: {e!r}")
    roster_path = Path(b["protected_roster"])
    if not roster_path.is_file():
        raise G.GateRefusal(f"protected_roster missing: {roster_path}")
    roster = json.loads(roster_path.read_text())
    if not (roster.get("protected9") and roster.get("cp8")
            and roster.get("seal", {}).get("seal_sha256")
            == G.EXPECTED_SEAL_SHA256):
        raise G.GateRefusal("protected roster is not a valid "
                            "derive-protected product (seal/protected9/cp8)")
    return b


def plan_swap(binding_path: Path):
    try:
        b = _load_binding(binding_path)
    except (G.GateRefusal, ValueError) as e:
        return {"header": G.plan_header(TOOL, "swap"),
                "refused": str(e), "steps": [], "runner_calls_possible": 0}
    verify_reads = [["docker", "ps", "--format",
                     "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}"]]
    for e in b["old_running"]:
        verify_reads.append(["docker", "inspect", e["name"], "--format",
                             "{{.Config.Image}} {{.State.Running}}"])
    steps = [
        G.step("s-verify-old", "swap",
               "OLD running identity must match binding exactly",
               verify_reads[0], gates=[f"expected: {b['old_running']}",
                                       "mismatch => refuse, zero writes"]),
        G.step("s-verify-candidate-image", "swap",
               "candidate image must exist locally with the bound full sha",
               ["docker", "image", "inspect",
                b["candidate_image_sha256"], "--format", "{{.Id}}"],
               gates=["must equal the binding sha; no pull"]),
        G.step("s-up-candidate", "swap",
               "swap = up -d --no-deps of the bound service only",
               ["docker", "compose", "-f", Q3_COMPOSE,
                "--project-name", G.Q3_PROJECT, "up", "-d", "--no-deps",
                b["candidate_service"]],
               write_class="p26-private",
               gates=["only after s-verify-old + s-verify-candidate-image "
                      "rc 0; base containers untouched",
                      "protected9/8CP owners must remain untouched — post "
                      "check reuses the sealed read-only SELECTs"]),
        G.step("s-post-protected-check", "swap",
               "post-swap read-only protected-9/8CP re-verify (released "
               "SQL; verbatim sealed SELECTs)",
               ["docker", "compose", "-f", Q3_COMPOSE,
                "--project-name", G.Q3_PROJECT, "exec", "-T", "postgres",
                "psql", "-U", "openfoundry", "-d", "connectors", "-t", "-A",
                "-v", "ON_ERROR_STOP=1", "-c", "<sealed 27-col SELECT>",
                "-c", "<sealed cp-owners SELECT>"],
               gates=["exact argv from p26_cohort_cli "
                      "plan-oldpg-readonly; Root-released only",
                      "any drift vs protected roster => rollback decision "
                      "belongs to Root, not this tool"]),
    ]
    return {"header": G.plan_header(TOOL, "swap"),
            "binding": {"candidate_service": b["candidate_service"],
                        "candidate_image_sha256":
                            b["candidate_image_sha256"],
                        "old_running": b["old_running"]},
            "steps": steps,
            "negative_contracts": [
                "missing old identity -> refused with ZERO docker writes",
                "bad/short candidate sha -> refused",
                "invalid protected roster -> refused",
                "source drift during swap -> wrapper rc 2"]}


def plan_fixture_http(spec_path: Path):
    """Spec schema: {base_url, candidate_paths: [..],
    removed_p25_patterns: [..]} — removed P25 fixture/proxy URLs refused."""
    spec = json.loads(spec_path.read_text())
    base = spec.get("base_url", "")
    removed = spec.get("removed_p25_patterns") or []
    steps, refused = [], []
    for p in spec.get("candidate_paths") or []:
        url = f"{base}{p}"
        if any(pat in url for pat in removed):
            refused.append({"url": url,
                            "reason": "removed P25 fixture/proxy — cannot "
                                      "be current-quiet evidence"})
            continue
        steps.append(G.step(f"http-{p}", "fixture-http",
                            "new-P26-fixture acceptance GET",
                            ["curl", "-sS", "-o", "/dev/null", "-w",
                             "%{http_code}", url],
                            write_class="mutating",
                            gates=["2xx/4xx recorded verbatim; never "
                                   "retried into green"]))
    return {"header": G.plan_header(TOOL, "fixture-http"),
            "steps": steps, "refused_urls": refused,
            "rule": "removed P25 fixtures/proxies are refused before any "
                    "curl is planned or run"}


def main(argv=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["plan-build", "plan-swap",
                                     "plan-fixture-http"])
    ap.add_argument("--binding", help="plan-swap: binding JSON path")
    ap.add_argument("--spec", help="plan-fixture-http: spec JSON path")
    ap.add_argument("--expect-source-sha", default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args(argv)

    if args.mode == "plan-build":
        plan, phase = plan_build(args.expect_source_sha), "build"
    elif args.mode == "plan-swap":
        if not args.binding:
            print("[p26-build] plan-swap requires --binding", file=sys.stderr)
            return 2
        plan, phase = plan_swap(Path(args.binding)), "swap"
    else:
        if not args.spec:
            print("[p26-build] plan-fixture-http requires --spec",
                  file=sys.stderr)
            return 2
        plan, phase = plan_fixture_http(Path(args.spec)), "fixture-http"

    out = Path(args.out).resolve()
    if G.ROOT_P25 in out.parents or out == G.ROOT_P25:
        print("[p26-build] REFUSED: output inside frozen P25 tree",
              file=sys.stderr)
        return 2
    G.write_json(out, plan)
    print(f"[p26-build] {args.mode}: steps={len(plan.get('steps', []))} "
          f"refused={len(plan.get('refused_urls', []))} -> {out}")

    if args.execute and args.mode == "plan-swap":
        print("[p26-build] plan-swap: the old swap SKELETON must not "
              "execute (root build-round1 #8); accurate small override-"
              "bind swap waits for its own review+release", file=sys.stderr)
        return 2
    if args.execute:
        release = G.load_release()
        steps = [dict(s, kind="argv") for s in plan.get("steps", [])]
        res = G.run_phase(steps, phase, G.real_runner, release=release,
                          execute=True)
        print(json.dumps({k: v for k, v in res.items() if k != "steps"},
                         indent=1))
        if res["verdict"] != "pass":
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
