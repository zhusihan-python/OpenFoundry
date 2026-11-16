#!/usr/bin/env python3
"""Post-restart persistence probe for Build/Job handles.

Reads the PRE-restart smoke result (three-domain build_positive handles)
plus the uuid-only identity map, mints matching test JWTs in memory, and
re-reads each domain's Build (by UUID and RID) and Job through the normal
HTTP surface. Asserts: HTTP 200, identity fields intact, build state still
in the aborted family. No tokens/secrets are printed; nothing is modified.

Usage:
  python3 -I probe_build_persistence.py --env-dir <tmp> \
      --result <tmp>/logs/result-pre-restart.json \
      --identities <tmp>/logs/identities-pre-restart.json
"""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import uuid

ARGS = None
SECRET_BYTES = b""
PIPELINE_BASE = ""


def mint_token(secret, *, sub, org, ttl=3600):
    import base64
    import hmac
    import time
    b64 = lambda raw: base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    now = int(time.time())
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = {"sub": sub, "jti": str(uuid.uuid4()), "iat": now, "exp": now + ttl,
               "email": "build-probe@openfoundry.local", "name": "Build Persistence Probe",
               "roles": [], "permissions": ["dataset.write", "dataset:write"]}
    if org:
        payload["org_id"] = org
    message = f"{header}.{b64(json.dumps(payload).encode())}"
    sig = b64(hmac.new(secret, message.encode(), hashlib.sha256).digest())
    return f"{message}.{sig}"


def http_get(base, path, token):
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen
    req = Request(base + path, headers={"Authorization": "Bearer " + token})
    try:
        with urlopen(req, timeout=15) as resp:
            return resp.status, resp.read()
    except HTTPError as exc:
        return exc.code, exc.read(500)


def main():
    global ARGS, SECRET_BYTES, PIPELINE_BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--identities", required=True)
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)
    env = dict(line.split("=", 1) for line in (env_dir / ".env").read_text().splitlines() if "=" in line)
    SECRET_BYTES = env["OF_P1_JWT_SECRET"].encode()
    PIPELINE_BASE = env.get("OF_P1_PIPELINE_BASE", "http://127.0.0.1:17081")

    result = json.loads(Path(ARGS.result).read_text())
    identities = json.loads(Path(ARGS.identities).read_text())
    handles = (result.get("evidence", {}).get("build_positive") or {})
    id_key = {"org_a": "user_a", "org_b": "user_b", "legacy": "legacy"}

    out = {"smoke_suffix": result.get("suffix"), "domains": {}, "verdict": "PASS"}
    failed = []
    for dom in ("org_a", "org_b", "legacy"):
        h = handles.get(dom)
        ident = identities.get(id_key[dom])
        if not h or not ident:
            failed.append(f"{dom}: missing handles/identity")
            continue
        token = mint_token(SECRET_BYTES, sub=ident["sub"], org=ident["org"])
        checks = {}
        rid = h.get("build_rid")
        s1, b1 = http_get(PIPELINE_BASE, f"/api/v1/builds/{h['build_id']}", token)
        body1 = json.loads(b1) if s1 == 200 else {}
        checks["build_get_uuid"] = {
            "status": s1,
            "state": str(body1.get("state", "")).upper(),
            "id_matches": body1.get("id") == h.get("build_id"),
            "rid_matches": body1.get("rid") == rid,
        }
        s2, b2 = http_get(PIPELINE_BASE, f"/v1/builds/{rid}", token) if rid else (0, b"")
        body2 = json.loads(b2) if s2 == 200 else {}
        checks["build_get_rid"] = {"status": s2, "rid_matches": body2.get("rid") == rid}
        s3, b3 = http_get(PIPELINE_BASE, f"/api/v1/jobs/{h['job_rid']}", token)
        body3 = json.loads(b3) if s3 == 200 else {}
        checks["job_get_rid"] = {"status": s3,
                                 "job_spec_rid_matches": body3.get("job_spec_rid") == h.get("spec_rid"),
                                 "build_id_matches": body3.get("build_id") == h.get("build_id")}
        ok = (s1 == 200
              and checks["build_get_uuid"]["state"] in ("BUILD_ABORTING", "BUILD_ABORTED")
              and checks["build_get_uuid"]["id_matches"]
              and checks["build_get_uuid"]["rid_matches"]
              and s2 == 200 and checks["build_get_rid"]["rid_matches"]
              and s3 == 200
              and checks["job_get_rid"]["job_spec_rid_matches"]
              and checks["job_get_rid"]["build_id_matches"])
        checks["ok"] = ok
        out["domains"][dom] = checks
        if not ok:
            failed.append(f"{dom}: {checks}")

    out["failures"] = failed
    out["verdict"] = "PASS" if not failed else "FAIL"
    out_log = env_dir / "logs" / "build-persistence-probe.json"
    out_log.write_text(json.dumps(out, indent=2) + "\n")
    domain_bits = ",".join(
        f"{dom}:{data['build_get_uuid']['state']}"
        for dom, data in out["domains"].items())
    print(f"[build-probe] verdict={out['verdict']} domains={domain_bits} -> {out_log}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
