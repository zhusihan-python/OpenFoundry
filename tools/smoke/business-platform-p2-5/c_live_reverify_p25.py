#!/usr/bin/env python3
"""P25 C-live FRESH re-verification runner (root-ruled, one-shot).

Root ruling (2026-11-15): the C gate must be freshly verified — using
the SAME existing test-actor identity as the frozen P21/P24 ABC helper
(exact owner/org from the sealed C source/Def/Run original parents),
minting an IN-MEMORY short-lived same-scope actor JWT from the EXISTING
q3 signing material. Exactly THREE read-only GETs; no DB/permission
change; no admin cross-domain; the token/secret is never printed,
archived, or written anywhere.

Claims spelling is taken verbatim from the frozen helper
(business-platform-p1/mint_token.py + smoke.mint_token): HS256 with
{sub, jti, iat, exp, email, name, roles, permissions} and optional
org_id — nothing invented.

Artifacts (NEW attempt dir; nothing from earlier attempts is touched
or overwritten): 07-c-live.txn.json / 07-c-live.file.bin /
07-c-live.files.json each with .url (canonical /v1 route, no token),
.http_status, .body; plus attempt-summary.json binding every artifact
sha256 + the BASE attempt it supplements (explicit SHA association for
verify_preflight_p25.py --c-supplement).

Usage (env provides OF_P1_JWT_SECRET in-memory; never echoed):
  OF_P1_JWT_SECRET=<...> python3 -I c_live_reverify_p25.py \
      --out-dir <dir> [--base-attempt <dir>]
"""

import argparse
import base64
import hashlib
import hmac
import importlib.util
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATASET_BASE = "http://127.0.0.1:17078"


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def mint_actor_jwt(secret: str, sub: str, org: str, minutes: int = 10):
    """Frozen-helper-compatible HS256 actor JWT (same claims spelling);
    exists only in the caller's memory."""
    now = int(time.time())
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = {
        "sub": sub,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + minutes * 60,
        "email": "p25-c-live-reverify@openfoundry.local",
        "name": "P25 C-live reverify (same-scope test actor)",
        "roles": [],
        "permissions": [],
        "org_id": org,
    }
    message = f"{header}.{b64(json.dumps(payload).encode())}"
    sig = b64(hmac.new(secret.encode(), message.encode(),
                       hashlib.sha256).digest())
    return f"{message}.{sig}"


def sha(p: Path):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--base-attempt")
    args = ap.parse_args()

    secret = os.environ.get("OF_P1_JWT_SECRET", "")
    if not secret:
        print("[c-reverify] OF_P1_JWT_SECRET absent — refusing",
              file=sys.stderr)
        return 2

    # sealed C identity + artifact values via the SEAL derivation only
    spec = importlib.util.spec_from_file_location(
        "p25_derive_c", HERE / "p25_cohort_derive.py")
    derive = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(derive)
    data = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                         Path(derive.SEAL_P24_TOOLS_DEFAULT))
    cl = data["CONTENT_GATES"]["c_live_sealed"]
    c_row = next(r for r in data["GROUP2"] if r["id"] == cl["run_id"])
    sub = c_row["run_owner_id"]          # sealed C run owner (actor)
    org = c_row["run_org_id"]            # sealed C org (scope)
    token = mint_actor_jwt(secret, sub, org)   # in-memory only

    urls = {
        "07-c-live.txn.json":
            f"{DATASET_BASE}/v1/datasets/{cl['target']}"
            f"/branches/main/transactions/{cl['txn_id']}",
        "07-c-live.file.bin":
            f"{DATASET_BASE}/v1/datasets/{cl['target']}"
            f"/files/{cl['bound_files'][0]}/download",
        "07-c-live.files.json":
            f"{DATASET_BASE}/v1/datasets/{cl['target']}/files",
    }

    out = Path(args.out_dir)
    n = 1
    while (out / f"attempt-{n}").exists():
        n += 1
    att = out / f"attempt-{n}"
    att.mkdir(parents=True)
    ledger = {}
    failures = []
    for stem, url in urls.items():
        (att / f"{stem}.url").write_text(f"GET {url}\n")
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                status, body = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, body = exc.code, exc.read()
        except Exception as exc:                       # noqa: BLE001
            status, body = "error", b""
            failures.append(f"{stem}: {exc}")
        (att / f"{stem}.http_status").write_text(f"{status}\n")
        (att / f"{stem}.body").write_bytes(body)
        for suffix in (".url", ".http_status", ".body"):
            p = att / f"{stem}{suffix}"
            ledger[p.name] = {"bytes": p.stat().st_size,
                              "sha256": sha(p),
                              "original_rc": str(status)
                              if suffix == ".http_status" else None}
    summary = {
        "attempt_dir": str(att),
        "schema": "p25-c-supplement-v1",
        "supplements_base_attempt": args.base_attempt or None,
        "base_attempt_note": ("verify_preflight_p25 --c-supplement "
                              "consumes this dir for the 07 gate; the "
                              "base attempt files are never modified"),
        "actor_identity": {"sub": sub, "org_id": org,
                           "source": "sealed C run parents "
                                     "(seal-derived)"},
        "token_handling": "in-memory only; never printed/archived",
        "steps_recorded": sorted(urls),
        "artifacts": ledger,
        "failures": failures,
        "ok": not failures,
    }
    (att / "attempt-summary.json").write_text(json.dumps(
        summary, indent=1, sort_keys=True) + "\n")
    statuses = [v.get("original_rc") for k, v in sorted(ledger.items())
                if k.endswith(".http_status")]
    print(f"[c-reverify] attempt {att} ok={summary['ok']} "
          f"statuses={statuses}")
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
