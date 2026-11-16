#!/usr/bin/env python3
"""Restart persistence probe for the P1 acceptance environment.

Reads the PRE-restart evidence (results/2026-11-13-http-review, smoke run
suffix c12cad5c) and re-verifies, AFTER the service restart, through the
NORMAL dataset HTTP surface:

  * GET /v1/datasets/{id}                       -> 200 (target still exists)
  * GET /v1/datasets/{id}/files/{file_id}/download
                                                -> 200, SHA256(bytes) == the
                                                   pre-restart content_hash
  * GET /v1/datasets/{id}/branches/main/transactions/{txn} -> 200

The original test tokens were never persisted, so the probe reads the
synthetic dataset's owner_id/org_id from THIS isolation environment's
datasets Postgres (SELECT only) and mints a matching test JWT in memory
from the .env secret. No token or secret is ever printed; no data is
modified. Cross-tenant authorization is NOT re-tested here — it is
covered by the standing isolation scenarios.

Usage:
  python3 -I probe_restart_persistence.py --env-dir <tmp> \
      --review-result <repo results/2026-11-13-http-review/result.json>
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import uuid

ARGS = None
ENV = {}
SECRET_BYTES = b""


def load_env(env_dir: Path):
    global ENV, SECRET_BYTES
    ENV = dict(line.split("=", 1) for line in (env_dir / ".env").read_text().splitlines() if "=" in line)
    SECRET_BYTES = ENV["OF_P1_JWT_SECRET"].encode()
    return {
        "dataset": ENV.get("OF_P1_DATASET_BASE", "http://127.0.0.1:17078"),
    }


def mint_token(secret, *, sub, org, ttl=3600):
    import base64
    import hmac
    import time
    b64 = lambda raw: base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    now = int(time.time())
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = {"sub": sub, "jti": str(uuid.uuid4()), "iat": now, "exp": now + ttl,
               "email": "persistence-probe@openfoundry.local", "name": "P1 Persistence Probe",
               "roles": [], "permissions": ["dataset.write", "dataset:write"]}
    if org:
        payload["org_id"] = org
    message = f"{header}.{b64(json.dumps(payload).encode())}"
    sig = b64(hmac.new(secret, message.encode(), hashlib.sha256).digest())
    return f"{message}.{sig}"


def psql_datasets(sql, env_dir):
    cmd = ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
           "--project-name", "openfoundry-platform-p1-q3v9",
           "exec", "-T", "postgres", "psql", "-U", "openfoundry", "-d", "datasets",
           "-v", "ON_ERROR_STOP=1", "-q", "-A", "-t", "-c", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed: {proc.stderr[:300]}")
    return proc.stdout.strip()


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
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", required=True)
    parser.add_argument("--review-result", required=True)
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)
    bases = load_env(env_dir)

    review = json.loads(Path(ARGS.review_result).read_text())
    if review.get("suffix") != "c12cad5c":
        print(f"WARN: review suffix is {review.get('suffix')}, expected c12cad5c", flush=True)
    ev = review["evidence"]["sync_real_rest"]
    dataset_id = ev["target_dataset_id"]
    txn_id = ev["artifact_bound"]["transaction_id"]
    file_id = ev["artifact_bound"]["file_id"]
    old_hash = ev["content_hash"]
    for value in (dataset_id, txn_id, file_id):
        uuid.UUID(value)  # format check; values come from our own prior run

    result = {"review_suffix": review.get("suffix"), "dataset_id": dataset_id,
              "transaction_id": txn_id, "file_id": file_id,
              "pre_restart_content_hash": old_hash, "checks": {}}
    failed = []

    # Read-only lookup of the synthetic dataset's ownership in THIS env.
    row = psql_datasets(
        f"SELECT owner_id::text, org_id::text FROM datasets WHERE id = '{uuid.UUID(dataset_id)}';",
        env_dir)
    parts = row.split("|")
    if len(parts) != 2 or not parts[0]:
        failed.append(f"owner/org lookup unexpected: {row[:80]!r}")
        result["checks"]["owner_lookup"] = {"ok": False}
    else:
        owner_id, org_id = parts[0].strip(), (parts[1].strip() or None)
        result["checks"]["owner_lookup"] = {"ok": True,
                                            "org_domain": "org" if org_id else "personal"}
        token = mint_token(SECRET_BYTES, sub=owner_id, org=org_id)

        status, body = http_get(bases["dataset"], f"/v1/datasets/{dataset_id}", token)
        ok = status == 200
        result["checks"]["dataset_get"] = {"status": status, "ok": ok}
        if not ok:
            failed.append(f"GET dataset -> {status}")

        status, body = http_get(
            bases["dataset"], f"/v1/datasets/{dataset_id}/files/{file_id}/download", token)
        digest = hashlib.sha256(body).hexdigest() if status == 200 else None
        ok = status == 200 and digest == old_hash
        result["checks"]["file_download_sha256"] = {
            "status": status, "bytes": len(body) if status == 200 else None,
            "sha256": digest, "matches_pre_restart": digest == old_hash, "ok": ok}
        if not ok:
            failed.append(f"download/hash mismatch: status={status} sha256={digest}")

        status, body = http_get(
            bases["dataset"],
            f"/v1/datasets/{dataset_id}/branches/main/transactions/{txn_id}", token)
        metadata_hash = None
        if status == 200:
            try:
                metadata_hash = (json.loads(body).get("metadata") or {}).get("content_hash")
            except ValueError:
                metadata_hash = None
        # Strict equality: a missing or WRONG metadata hash must fail the
        # probe, not merely be recorded.
        ok = status == 200 and metadata_hash == old_hash
        result["checks"]["transaction_get"] = {
            "status": status, "metadata_content_hash": metadata_hash,
            "matches_pre_restart": metadata_hash == old_hash, "ok": ok}
        if not ok:
            failed.append(f"GET transaction -> {status} metadata_hash={metadata_hash} "
                          f"(want {old_hash})")

    result["verdict"] = "PASS" if not failed else "FAIL"
    result["failures"] = failed
    out_log = env_dir / "logs" / "restart-persistence-probe.json"
    out_log.parent.mkdir(parents=True, exist_ok=True)
    out_log.write_text(json.dumps(result, indent=2) + "\n")
    print(f"[persistence-probe] verdict={result['verdict']} "
          f"(dataset_get={result['checks'].get('dataset_get', {}).get('status')}, "
          f"download_sha256_match={result['checks'].get('file_download_sha256', {}).get('matches_pre_restart')}, "
          f"txn_metadata_match={result['checks'].get('transaction_get', {}).get('matches_pre_restart')}) "
          f"-> {out_log}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
