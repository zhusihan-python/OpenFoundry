#!/usr/bin/env python3
"""P25 deployment-record helper (runtime-track owned; round5 fix 1).

The driver copies THIS file byte-exact into $EV/gen/deployment_record.py
(sha recorded) and invokes it after every legitimate (re)start so the
ACTUAL new boot + full image become the steady guard's reviewed target.
record_body() is PURE over injectable run_cli — the selftest drives the
REAL generated body with mocked IO (never string-marker inspection).
Only genuine path arguments are Paths; project/pinned-sha/cause stay
strings (the earlier all-Path unpack made str!=Path comparisons fail
and TypeError'd the [:12] error formatting AFTER stop/up — that bug
class is now structurally impossible + test-pinned).

Reads ONLY (compose ps -q, docker inspect, candidate source gate);
performs no mutation; fails closed before writing any record.
"""

import hashlib
import json
import re
import sys
import time
from pathlib import Path

RFC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
HEX = re.compile(r"^[0-9a-f]{64}$")


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def record_body(argv8, run_cli):
    """argv8: [here, repo, env_dir, project, pinned_sha,
    sel_build_path, cause, ev_root] — paths are 1,2,3,6,8; project/
    pinned_sha/cause stay strings. run_cli(argv_list, timeout) ->
    (rc, stdout, stderr). Returns (exit_rc, record_dict_or_None)."""
    here, repo, env_dir, proj, sel_build_path, ev_root = map(
        Path, (argv8[0], argv8[1], argv8[2], argv8[3], argv8[5],
               argv8[7]))
    pinned_sha, cause = str(argv8[4]), str(argv8[6])

    def block(probs):
        for p in probs:
            print(f"DEPLOY_RECORD_BLOCK: {p}", file=sys.stderr)
        return 1, None

    files = ["-f", str(env_dir / "compose.yaml")]
    ovr = env_dir / "compose-p22-connector2.yaml"
    if ovr.exists():
        files += ["-f", str(ovr)]
    rc, out, err = run_cli(["docker", "compose", *files,
                            "--project-name", proj, "ps", "-q",
                            "connector"], 120)
    ids = [l for l in (out or "").strip().splitlines() if l.strip()]
    if rc != 0 or len(ids) != 1:
        return block([f"compose ps rc={rc} ids={ids}"])
    irc, iout, ierr = run_cli(["docker", "inspect", "--format",
                               "{{.State.StartedAt}}|{{.Image}}",
                               ids[0]], 60)
    if irc != 0:
        return block([f"inspect rc={irc}"])
    try:
        boot, image = (iout or "").strip().split("|", 1)
    except ValueError:
        return block([f"inspect output malformed: {iout!r}"])
    img_hex = image[7:] if image.startswith("sha256:") else image
    try:
        sel = json.loads(Path(sel_build_path).read_text())
    except (OSError, ValueError) as exc:
        return block([f"selected-build unreadable: {exc}"])
    want = (sel.get("images") or {}).get("connector", "")
    want_hex = want[7:] if want.startswith("sha256:") else want
    probs = []
    if not RFC.match(boot):
        probs.append(f"boot not RFC3339: {boot!r}")
    if not HEX.match(img_hex):
        probs.append(f"image not full sha256 hex: {image!r}")
    if img_hex != want_hex:
        probs.append(f"running image {img_hex[:12]}… != selected-build "
                     f"connector {want_hex[:12]}…")
    grc, gout, gerr = run_cli(
        ["python3", "-I", str(here / "candidate_source_gate_p25.py"),
         "--repo", str(repo)], 300)
    got_sha = ""
    for l in (gout or "").splitlines():
        if l.startswith("CANDIDATE_SOURCE_SHA256="):
            got_sha = l.split("=", 1)[1]
    if grc != 0 or got_sha != pinned_sha:
        probs.append(f"candidate source {got_sha[:12]}… != pinned "
                     f"{pinned_sha[:12]}…")
    if probs:
        return block(probs)
    rec = {"schema": "p25-deployment-record-v1",
           "cause": cause,
           "recorded_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                            time.gmtime()),
           "connector_boot": boot,
           "connector_image": f"sha256:{img_hex}",
           "source_sha256": got_sha,
           "selected_build_sha256": sha(sel_build_path),
           "selected_build_path": str(sel_build_path)}
    n = 1
    out_json = ev_root / "deployment-record.json"
    while out_json.exists():
        n += 1
        out_json = ev_root / f"deployment-record-r{n}.json"
    out_json.write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n")
    (ev_root / "deployment-record-latest.txt").write_text(
        str(out_json) + "\n")
    print(f"DEPLOYMENT_RECORD={out_json}")
    print(f"DEPLOYMENT_BOOT={boot}")
    print(f"DEPLOYMENT_IMAGE=sha256:{img_hex}")
    return 0, rec


def real_run_cli(argv, timeout):
    import subprocess
    res = subprocess.run(argv, capture_output=True, text=True,
                         timeout=timeout)
    return res.returncode, res.stdout, res.stderr


def main():
    if len(sys.argv) != 9:
        print("usage: deployment_record.py <here> <repo> <env_dir> "
              "<project> <pinned_sha> <sel_build> <cause> <ev_root>",
              file=sys.stderr)
        return 2
    rc, _ = record_body(sys.argv[1:9], real_run_cli)
    return rc


if __name__ == "__main__":
    sys.exit(main())
