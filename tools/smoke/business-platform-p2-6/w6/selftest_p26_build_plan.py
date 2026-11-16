#!/usr/bin/env python3
"""PURE self-test for build/p26_build_plan.py (plans + refusals only)."""

import json
import sys
import tempfile
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402
from build import p26_build_plan as B  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[build] {'PASS' if cond else 'FAIL'} {name}"
          + (f" — {detail}" if detail and not cond else ""))


class FakeRunner:
    def __init__(self):
        self.calls = []

    def __call__(self, argv):
        self.calls.append(argv)
        return 0, ""


def valid_roster(td):
    p = Path(td) / "roster.json"
    p.write_text(json.dumps({
        "seal": {"seal_sha256": G.EXPECTED_SEAL_SHA256},
        "protected9": [{"run_id": str(uuid.uuid4())} for _ in range(9)],
        "cp8": [{"run_id": str(uuid.uuid4())} for _ in range(8)]}))
    return p


def main():
    w6dir = Path(__file__).resolve().parents[1] / "results" / "w6"
    w6dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=str(w6dir)) as td:
        # plan-build
        pb = B.plan_build("b" * 64)
        check("build-steps", len(pb["steps"]) == 5)
        check("build-gates-clean",
              all(G.check_step_gates(s) == [] for s in pb["steps"]))
        check("build-source-gate-pinned",
              "b" * 64 in pb["hard_gates"]["source-gate"])

        # swap: missing old identity -> refused with zero steps
        binding = {"candidate_service": "p26connector",
                   "candidate_image_sha256": "c" * 64,
                   "old_running": [],
                   "protected_roster": str(valid_roster(td))}
        bp = Path(td) / "b1.json"
        bp.write_text(json.dumps(binding))
        sw = B.plan_swap(bp)
        check("swap-missing-old-identity-refused",
              sw.get("refused") and sw.get("steps") == [])
        fr = FakeRunner()
        rel = {"released_for": ["swap"], "released_by": "root",
               "expires_utc": "29991231T000000Z"}
        check("swap-refused-has-no-steps-to-run",
              sw["steps"] == [] and fr.calls == [])

        # swap: bad (short) candidate sha -> refused
        b2 = dict(binding, old_running=[{"name": "c", "image": "i"}],
                  candidate_image_sha256="abc123")
        bp2 = Path(td) / "b2.json"
        bp2.write_text(json.dumps(b2))
        sw2 = B.plan_swap(bp2)
        check("swap-short-sha-refused",
              "64-hex" in sw2.get("refused", ""))

        # swap: invalid protected roster -> refused
        bad_rp = Path(td) / "badroster.json"
        bad_rp.write_text(json.dumps({"protected9": [], "cp8": []}))
        b3 = dict(binding, old_running=[{"name": "c", "image": "i"}],
                  protected_roster=str(bad_rp))
        bp3 = Path(td) / "b3.json"
        bp3.write_text(json.dumps(b3))
        sw3 = B.plan_swap(bp3)
        check("swap-invalid-roster-refused",
              "derive-protected" in sw3.get("refused", ""))

        # swap: valid -> 4 steps, up step gated clean, no release -> 0 calls
        b4 = dict(binding, old_running=[{
            "name": f"{G.Q3_PROJECT}-connector-1",
            "image": f"{G.Q3_PROJECT}-connector"}],
            protected_roster=str(valid_roster(td)))
        bp4 = Path(td) / "b4.json"
        bp4.write_text(json.dumps(b4))
        sw4 = B.plan_swap(bp4)
        check("swap-valid-steps", len(sw4.get("steps", [])) == 4)
        check("swap-valid-gates-clean",
              all(G.check_step_gates(s) == [] for s in sw4["steps"]))
        fr4 = FakeRunner()
        steps4 = [dict(s, kind="argv") for s in sw4["steps"]]
        res4 = G.run_phase(steps4, "swap", fr4, release=None, execute=True)
        check("swap-no-release-zero-calls",
              res4["verdict"] == "refused" and fr4.calls == [])

        # fixture-http: removed P25 fixture URLs refused pre-plan
        spec = Path(td) / "http.json"
        spec.write_text(json.dumps({
            "base_url": "http://127.0.0.1",
            "candidate_paths": [":18101/stats", ":18200/health"],
            "removed_p25_patterns": [":18101"]}))
        fh = B.plan_fixture_http(spec)
        urls = [s["argv"][-1] for s in fh["steps"]]
        check("fixture-http-removed-refused",
              len(fh["refused_urls"]) == 1 and
              "removed P25" in fh["refused_urls"][0]["reason"])
        check("fixture-http-clean-planned",
              urls == ["http://127.0.0.1:18200/health"])
        fr5 = FakeRunner()
        steps5 = [dict(s, kind="argv") for s in fh["steps"]]
        res5 = G.run_phase(steps5, "fixture-http", fr5, release=None,
                           execute=True)
        check("fixture-http-no-release-zero-calls",
              res5["verdict"] == "refused" and fr5.calls == [])

        # CLI output refusal into frozen tree
        rc = B.main(["plan-build", "--out",
                     str(G.ROOT_P25 / "x" / "p.json")])
        check("cli-out-in-p25-refused", rc == 2)

    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"[build] {npass}/{len(RESULTS)} PASS")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
