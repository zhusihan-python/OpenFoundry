#!/usr/bin/env python3
"""P25 RUNNING-IMAGES checker (round3, review-runtime-tools-round2 #3).

Expected images come from the SELECTED BUILD mapping (the accepted
r18/PG-ONLY-build evidence consumed by the driver — exact full sha256
ids, never short ids, never "an image key exists"): dataset/pipeline/
connector (base3), connector2/connector3 (old aux), p25connector (new
aux), python:3.12-alpine (dataset-proxy AND p25fixture/p25-proxy —
compose-p25-runtime.yaml pins the cached full id, no floating tags,
no pulls).

Strictness:
  * a PRESENT service needs: non-bool int containers == EXACTLY 1,
    running == true, full-64-hex image == expected, ps rc 0,
    inspect rc 0 — present garbage images fail;
  * an ABSENT service needs: non-bool int containers == 0 AND a
    SUCCESSFUL ps (rc 0) in the evidence — a missing key, a bool
    False, or a failed ps is NEVER absence;
  * unknown services in the evidence fail.

`collect` is LIVE (docker compose ps/inspect per service) and refuses
rc3 until RELEASED_FOR_EXECUTION is flipped by root; IO is injectable
for pure selftests. `verify` is pure.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
HEX64 = re.compile(r"^[0-9a-f]{64}$")
RELEASED_FOR_EXECUTION = True   # root flips for live collection

BASE3 = ("connector", "dataset", "pipeline")
OLD_AUX = ("connector2", "connector3", "dataset-proxy")
P25_AUX = ("p25fixture", "p25-proxy", "p25connector")

# per-phase service state: present (on the expected image) or absent.
# p25fixture/p25-proxy run the r18-CACHED python:3.12-alpine image
# (compose-p25-runtime.yaml pins the full id — never a floating tag),
# so EVERY present service maps to an exact selected-build full id.
PHASE_STATES = {
    "swap": {**{s: "present" for s in BASE3},
             **{s: "absent" for s in OLD_AUX},
             **{s: "absent" for s in P25_AUX}},
    "restart": {**{s: "present" for s in BASE3},
                **{s: "present" for s in OLD_AUX},
                **{s: "present" for s in P25_AUX}},
    "p25-window": {**{s: "present" for s in BASE3},
                   **{s: "absent" for s in OLD_AUX},
                   **{s: "present" for s in P25_AUX}},
    "post-aux": {**{s: "present" for s in BASE3},
                 **{s: "absent" for s in OLD_AUX},
                 **{s: "absent" for s in P25_AUX}},
}


def norm(image: str) -> str:
    v = (image or "").strip()
    if v.startswith("sha256:"):
        v = v[len("sha256:"):]
    if not HEX64.match(v):
        raise ValueError(f"not a full image id: {image!r}")
    return v


def _strict_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def expected_map(selected_build: dict) -> dict:
    images = selected_build["images"]
    want = {"connector": images["connector"],
            "dataset": images["dataset"],
            "pipeline": images["pipeline"],
            "connector2": images["connector2"],
            "connector3": images["connector3"],
            "p25connector": images["p25connector"],
            "dataset-proxy": images["python-3.12-alpine"],
            "p25fixture": images["python-3.12-alpine"],
            "p25-proxy": images["python-3.12-alpine"]}
    return {k: norm(v) for k, v in want.items()}


def adjudicate_images(running: dict, selected_build: dict,
                      phase: str) -> dict:
    """running: collector-schema evidence {schema, services: {svc:
    {containers, running, image, started_at, ps_rc, inspect_rc,
    resolved_image?}}}. phase in PHASE_STATES."""
    reasons = []
    if phase not in PHASE_STATES:
        return {"ok": False, "reasons": [f"unknown phase {phase!r}"]}
    if not isinstance(running, dict) \
            or running.get("schema") != "p25-running-evidence-v1":
        reasons.append("evidence lacks the collector schema — ad-hoc "
                       "hand-written json is not accepted")
        running = {"services": {}}
    services = running.get("services") or {}
    states = PHASE_STATES[phase]
    want = expected_map(selected_build)
    unknown = sorted(set(services) - set(states))
    if unknown:
        reasons.append(f"unknown services in evidence: {unknown}")
    for svc, state in sorted(states.items()):
        got = services.get(svc)
        if not isinstance(got, dict):
            reasons.append(f"{svc}: no evidence entry")
            continue
        # successful ps is REQUIRED either way (explicit absence proof)
        if got.get("ps_rc") != 0:
            reasons.append(f"{svc}: ps rc={got.get('ps_rc')!r} — "
                           "absence/presence claims need a SUCCESSFUL "
                           "ps")
        containers = got.get("containers")
        if not _strict_int(containers):
            reasons.append(f"{svc}: containers not a strict int: "
                           f"{containers!r} (bool/missing never counts)")
            continue
        if state == "present":
            if containers != 1:
                reasons.append(f"{svc}: containers={containers} != "
                               "EXACTLY 1")
            if got.get("running") is not True:
                reasons.append(f"{svc}: not running")
            if got.get("inspect_rc") != 0:
                reasons.append(f"{svc}: inspect rc="
                               f"{got.get('inspect_rc')!r}")
            try:
                got_img = norm(got.get("image") or "")
            except ValueError as exc:
                reasons.append(f"{svc}: {exc}")
                got_img = None
            if got_img is not None and got_img != want[svc]:
                reasons.append(f"{svc}: image {got_img[:12]}… != "
                               f"selected-build {want[svc][:12]}…")
        else:                                   # absent
            if containers != 0:
                reasons.append(f"{svc}: containers={containers} must "
                               f"be 0 in phase {phase}")
            if got.get("running") is True:
                reasons.append(f"{svc}: claims absent but running=true")
    return {"ok": not reasons, "reasons": reasons,
            "phase": phase}


# ------------------------------------------------------------------ live --

def collect_body(args, run_cli):
    """Live evidence gathering. run_cli(argv_list) -> (rc, stdout,
    stderr) is injectable for pure selftests."""
    env_dir = Path(args.env_dir)
    files = ["-f", str(env_dir / "compose.yaml")]
    ovr = env_dir / "compose-p22-connector2.yaml"
    if ovr.exists():
        files += ["-f", str(ovr)]
    files += ["-f", str(HERE / "compose-p25-fixture.yaml"),
              "-f", str(HERE / "compose-p25-aux.yaml")]
    all_svcs = list(BASE3) + list(OLD_AUX) + list(P25_AUX)
    services = {}
    evidence_dir = Path(str(args.out) + ".raw")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    problems = []
    for svc in all_svcs:
        entry = {}
        ps = run_cli(["docker", "compose", *files, "--project-name",
                      args.project, "ps", "-aq", svc])
        entry["ps_rc"] = ps[0]
        entry["ps_raw"] = ps[1]
        (evidence_dir / f"{svc}.ps-aq.raw").write_text(ps[1] or "")
        (evidence_dir / f"{svc}.ps-aq.rc").write_text(f"{ps[0]}\n")
        ids = [l for l in (ps[1] or "").strip().splitlines() if l.strip()]
        entry["containers"] = len(ids)
        entry["running"] = False
        if ids:
            runq = run_cli(["docker", "compose", *files,
                            "--project-name", args.project, "ps", "-q",
                            svc])
            entry["running"] = len(
                [l for l in (runq[1] or "").strip().splitlines()
                 if l.strip()]) == len(ids)
            ins = run_cli(["docker", "inspect", "--format",
                           "{{.State.StartedAt}}|{{.Image}}|"
                           "{{.State.Running}}", ids[0]])
            entry["inspect_rc"] = ins[0]
            (evidence_dir / f"{svc}.inspect.raw").write_text(ins[1] or "")
            (evidence_dir / f"{svc}.inspect.rc").write_text(f"{ins[0]}\n")
            if ins[0] == 0:
                parts = ins[1].strip().split("|")
                if len(parts) == 3:
                    entry["started_at"] = parts[0]
                    entry["image"] = parts[1]
                    entry["running"] = parts[2] == "true"
        services[svc] = entry
    out = {"schema": "p25-running-evidence-v1",
           "env_dir": str(env_dir), "project": args.project,
           "services": services}
    Path(args.out).write_text(json.dumps(out, indent=1, sort_keys=True)
                              + "\n")
    print(f"IMAGES_EVIDENCE={args.out} services={len(services)} "
          f"problems={problems}")
    return 0 if not problems else 1


def real_run_cli(argv):
    proc = subprocess.run(argv, capture_output=True, text=True,
                          timeout=120)
    return proc.returncode, proc.stdout, proc.stderr


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--env-dir",
                   default="/private/tmp/openfoundry-platform-p1-q3v9")
    c.add_argument("--project", default="openfoundry-platform-p1-q3v9")
    c.add_argument("--out", required=True)
    v = sub.add_parser("verify")
    v.add_argument("--running", required=True,
                   help="collector-schema evidence json")
    v.add_argument("--phase", required=True,
                   choices=sorted(PHASE_STATES))
    v.add_argument("--selected-build", required=True)
    v.add_argument("--out")
    args = ap.parse_args()
    if args.mode == "collect":
        if not RELEASED_FOR_EXECUTION:
            print("[p25-images] collect is LIVE (docker ps/inspect) and "
                  "not released — root flips "
                  "RELEASED_FOR_EXECUTION", file=sys.stderr)
            return 3
        return collect_body(args, real_run_cli)
    running = json.loads(Path(args.running).read_text())
    sel = json.loads(Path(args.selected_build).read_text())
    verdict = adjudicate_images(running, sel, args.phase)
    text = json.dumps(verdict, indent=1, sort_keys=True) + "\n"
    if args.out:
        Path(args.out).write_text(text)
    else:
        print(text, end="")
    return 0 if verdict["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
