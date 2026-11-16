#!/usr/bin/env python3
"""P24 RUNNING-image check (driver-owned; before P24 env apply/recreate
and reused as finalizer content evidence).

Frozen swap_verify covers only the 3 Connector replicas; THIS reads
the ACTUAL five running services (dataset, pipeline, connector,
connector2, connector3) — Image ID, Running, StartedAt ONLY (never
Config.Env, never full docker inspect output) — and requires every
Image ID to equal the PINNED built set (from 01b-swap-source-gate.json,
which itself pins the rc0 01-build-final attempt; no ls -t drift).

--base3 mode (root, post-auxdown): check ONLY dataset/pipeline/
connector running images against the SAME pinned five, AND prove the
three test-aux services (connector2, connector3, dataset-proxy) are
REMOVED — the old frozen swap_verify would wrongly demand aux running;
this mode must NEVER be satisfied by re-opening aux.

Usage: python3 -I check_running_images_p24.py \
         --gate-json <01b-swap-source-gate.json> [--env-dir <tmp>] \
         [--out <json>]
Exit 0 iff all five are Running on the pinned image IDs.
"""

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT = "openfoundry-platform-p1-q3v9"
SERVICES = ["dataset", "pipeline", "connector", "connector2",
            "connector3"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gate-json", required=True)
    ap.add_argument("--env-dir",
                    default="/private/tmp/openfoundry-platform-p1-q3v9")
    ap.add_argument("--out")
    ap.add_argument("--base3", action="store_true",
                    help="post-auxdown: verify only dataset/pipeline/"
                         "connector on the pinned IDs and REQUIRE "
                         "connector2/connector3/dataset-proxy absent")
    args = ap.parse_args()
    env_dir = Path(args.env_dir)
    gate = json.loads(Path(args.gate_json).read_text())
    want = gate.get("pinned_image_ids") or {}
    if set(want) != set(SERVICES):
        print(f"[images5] BLOCK: gate pinned set != five services: "
              f"{sorted(want)}", file=sys.stderr)
        return 1

    files = ["-f", str(env_dir / "compose.yaml")]
    override = env_dir / "compose-p22-connector2.yaml"
    if override.exists():
        files += ["-f", str(override)]

    def compose(*a):
        return subprocess.run(
            ["docker", "compose", *files, "--project-name", PROJECT, *a],
            capture_output=True, text=True, timeout=90)

    check_services = ["dataset", "pipeline", "connector"] \
        if args.base3 else list(SERVICES)
    absent_required = ["connector2", "connector3", "dataset-proxy"] \
        if args.base3 else []
    rows, bad = {}, []
    for svc in absent_required:
        ps = compose("ps", "-a", "-q", svc)
        ids = [l for l in ps.stdout.strip().splitlines() if l.strip()]
        rows[f"{svc}(absent)"] = {"containers": len(ids)}
        if ps.returncode != 0 or ids:
            bad.append(f"{svc}: still present ({len(ids)} containers) "
                       "— aux must be REMOVED in base3 mode")
    for svc in check_services:
        ps = compose("ps", "-q", svc)
        lines = [l for l in ps.stdout.strip().splitlines() if l.strip()]
        if ps.returncode != 0 or len(lines) != 1:
            bad.append(f"{svc}: ps rc={ps.returncode} ids={len(lines)}")
            continue
        ins = subprocess.run(
            ["docker", "inspect", "--format",
             "{{.Image}}|{{.State.Running}}|{{.State.StartedAt}}",
             lines[0]], capture_output=True, text=True, timeout=60)
        if ins.returncode != 0:
            bad.append(f"{svc}: inspect rc={ins.returncode}")
            continue
        image, running, started = (
            ins.stdout.strip().split("|") + ["", ""])[:3]
        image = image.replace("sha256:", "")
        rows[svc] = {"image": image[:64], "running": running,
                     "started_at": started}
        if running != "true":
            bad.append(f"{svc}: not Running")
        if image[:12] != want[svc][:12]:
            bad.append(f"{svc}: image {image[:12]} != pinned "
                       f"{want[svc][:12]}")

    verdict = {"client_date": time.strftime("%Y-%m-%d"),
               "mode": "base3" if args.base3 else "full5",
               "pinned_from": str(args.gate_json),
               "pinned": want, "running": rows, "bad": bad,
               "ok": not bad,
               "note": "Image/Running/StartedAt only; Config.Env and "
                       "full inspect output are never read"}
    out = Path(args.out) if args.out else Path(
        "check-running-images-p24.json")
    out.write_text(json.dumps(verdict, indent=1, sort_keys=True) + "\n")
    for svc, r in sorted(rows.items()):
        if "running" in r:
            print(f"[images5] {svc} running={r['running']} "
                  f"image={r['image'][:12]} started={r['started_at']}")
        else:
            print(f"[images5] {svc} absent-check containers="
                  f"{r.get('containers')}")
    for b in bad:
        print(f"[images5] BLOCK: {b}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
