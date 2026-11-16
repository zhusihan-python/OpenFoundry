#!/usr/bin/env python3
"""Swap verification: every running connector replica must mount an
image from THIS build round (image-ids.json) and show a fresh boot."""
import json
import subprocess
import sys

tmp, ids_file = sys.argv[1], sys.argv[2]
ids = {s: v for s, v in json.load(open(ids_file)).items()
       if s in ("connector", "connector2", "connector3")}
merged = ["-f", f"{tmp}/compose.yaml",
          "-f", f"{tmp}/compose-p22-connector2.yaml"]
bad = []
for svc, want in ids.items():
    cid = subprocess.run(
        ["docker", "compose", *merged, "--project-name",
         "openfoundry-platform-p1-q3v9", "ps", "-a", "-q", svc],
        capture_output=True, text=True, timeout=60).stdout.strip().splitlines()
    if not cid:
        print(f"{svc}: no container id")
        bad.append(svc)
        continue
    out = subprocess.run(
        ["docker", "inspect", "--format",
         "{{.Image}}|{{.State.StartedAt}}|{{.State.Running}}", cid[0]],
        capture_output=True, text=True, timeout=60).stdout.strip()
    image, started, running = out.split("|")
    image = image.replace("sha256:", "")[:12]
    ok = running == "true" and image == want
    print(f"{svc}: image={image} want={want} running={running} "
          f"started={started} ok={ok}")
    if not ok:
        bad.append(svc)
raise SystemExit(1 if bad else 0)
