#!/usr/bin/env python3
"""One exact Root-released S2 case; no implicit all-cases or resource creation.

CLI: --source-s ROOT_SHA --case EXACT_NAME --release-file ROOT_JSON
Requires reviewed_s2_tool_shas, reviewed_s2_wrapper_sha256, cohort2 aux,
and the ordinary live/owned-ops release grants. Private env is loaded only
after all static gates pass. Existing scenario assertions are unchanged.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO = ROOT.parents[2]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "common"))
import p26_gates as G
from p26_http_lib import release_allows, resolve_bases
from p26_owned_ops import validate_release_aux, validate_auth_identity
from p26_scenarios import SCENARIOS

TOOLS = tuple(sorted(["http/" + p.name for p in HERE.glob("*.py")
                      if p.name != Path(__file__).name]
                     + ["fixture/" + p.name for p in (ROOT / "fixture").glob("*.py")]))
SECRETS = ("P26_HTTP_ADMIN_TOKEN", "OPENFOUNDRY_SMOKE_JWT",
           "P26_W7_JWT_WRITER", "P26_W7_JWT_READONLY", "P26_W7_JWT_FOREIGN")


def hashes():
    return {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in TOOLS}


def validate(doc, source_s, case, release_file):
    if not re.fullmatch(r"[0-9a-f]{64}", source_s):
        raise ValueError("--source-s requires Root full source sha256")
    if doc.get("frozen_source_sha256") != source_s:
        raise ValueError("Root source S differs from CLI")
    if case not in SCENARIOS or case not in doc.get("w7_cases", []):
        raise ValueError("exact case outside Root w7_cases")
    if doc.get("w7_aux_cohort") != 2:
        raise ValueError("S2 wrapper requires fixed cohort2")
    validate_auth_identity(doc.get("w7_auth_identity"))
    _, error = validate_release_aux(doc)
    if error:
        raise ValueError(error)
    bases, error = resolve_bases({}, doc)
    if error:
        raise ValueError(error)
    if (bases["connector"] != "http://127.0.0.1:17101"
            or bases.get("connector_b") != "http://127.0.0.1:17102"):
        raise ValueError("S2 requires dedicated aux ports 17101/17102")
    phases = ["http-live-scenarios"]
    if "connector_ops" in SCENARIOS[case].get("requires", []):
        phases.append("http-owned-ops")
    for phase in phases:
        ok, why = release_allows(phase, release_file)
        if not ok:
            raise ValueError(why)
    if doc.get("reviewed_s2_tool_shas") != hashes():
        raise ValueError("Root reviewed S2 tool pins differ")
    wrapper_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if doc.get("reviewed_s2_wrapper_sha256") != wrapper_sha:
        raise ValueError("Root reviewed S2 wrapper pin differs")
    return bases


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-s", required=True)
    ap.add_argument("--case", required=True, choices=tuple(SCENARIOS))
    ap.add_argument("--release-file", required=True)
    args = ap.parse_args()
    release_path = Path(args.release_file).resolve()
    doc = json.loads(release_path.read_text())
    validate(doc, args.source_s, args.case, str(release_path))
    before_source = G.candidate_hash(REPO)
    if before_source != args.source_s:
        raise ValueError("actual candidate S drift before execution")
    before_tools = hashes()
    env = os.environ.copy()
    env["P26_RELEASE_FILE"] = str(release_path)
    for path in ("/private/tmp/openfoundry-p26-http-admin.env",
                 "/private/tmp/openfoundry-p26-http-smoke.env"):
        pp = Path(path)
        if pp.is_symlink() or stat.S_IMODE(pp.stat().st_mode) != 0o600:
            raise ValueError("private env must be nonsymlink mode 600")
        for line in pp.read_text().splitlines():
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                if key not in SECRETS:
                    raise ValueError("unexpected private env key")
                env[key] = value
    validate_auth_identity(doc.get("w7_auth_identity"),
                           [env.get("OPENFOUNDRY_SMOKE_JWT"), env.get("P26_W7_JWT_WRITER")])
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "results/w7" / ("S2-outer-" + args.case + "-" + stamp)
    out.mkdir()
    cmd = [sys.executable, "-I", "-B", str(HERE / "run_p26_http.py"),
           "--run-live", args.case]
    (out / "case.cmd").write_text(json.dumps(cmd) + "\n")
    pins = {"source_S": args.source_s, "actual_candidate_hash_before": before_source,
            "candidate_images": doc["expected_candidate_images"], "aux": doc["w7_aux"],
            "bases": doc["w7_bases"], "release_id": doc["release_id"],
            "w7_auth_identity": doc["w7_auth_identity"],
            "frozen_dependencies_sha256": before_tools,
            "wrapper_sha256": doc["reviewed_s2_wrapper_sha256"]}
    (out / "pins.json").write_text(json.dumps(pins, indent=2) + "\n")
    attempts = ROOT / "results/w7/S2-attempts"
    before = set(attempts.glob("*"))
    print("outer=" + str(out), flush=True)
    try:
        cp = subprocess.run(cmd, capture_output=True, env=env, cwd=REPO, timeout=240)
        rc, stdout, stderr = cp.returncode, cp.stdout, cp.stderr
    except subprocess.TimeoutExpired as exc:
        rc, stdout, stderr = None, exc.stdout or b"", exc.stderr or b""
    for key in SECRETS:
        secret = env.get(key, "").encode()
        if secret:
            stdout, stderr = stdout.replace(secret, b"<redacted>"), stderr.replace(secret, b"<redacted>")
    for name, data in (("case.stdout.raw", stdout), ("case.stderr.raw", stderr),
                       ("case.raw", stdout + stderr)):
        (out / name).write_bytes(data)
    (out / "case.rc").write_text(str(rc) if rc is not None else "unavailable")
    after_source, after_tools = G.candidate_hash(REPO), hashes()
    meta = {"original_unix_rc": rc, "timeout_s": 240,
            "inner_attempts": sorted(str(p) for p in set(attempts.glob("*")) - before),
            "actual_candidate_hash_after": after_source,
            "tool_shas_after": after_tools,
            "source_unchanged": before_source == after_source == args.source_s,
            "tools_unchanged": before_tools == after_tools,
            "http_rc_note": "HTTP .rc is normalized_harness; .meta original_http_status is actual HTTP; case.rc is original Unix child rc"}
    (out / "case.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(stdout.decode(errors="replace")); print(stderr.decode(errors="replace"))
    print("original_unix_rc=" + str(rc))
    return rc if (rc is not None and rc >= 0 and meta["source_unchanged"] and meta["tools_unchanged"]) else 1


if __name__ == "__main__":
    sys.exit(main())
