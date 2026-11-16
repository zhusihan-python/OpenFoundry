#!/usr/bin/env python3
"""Mint an HS256 JWT for the P1 acceptance environment (trusted helper).

Used by run.sh to generate the pipeline service fallback bearer
(OF_P1_SERVICE_JWT). Claims match libs/auth-middleware.Claims:
sub/jti/iat/exp/email/name/roles and an OPTIONAL org_id. The service
token deliberately carries NO org claim (personal service domain).

Usage:
  python3 -I mint_token.py --secret <jwt-secret> [--sub <uuid>]
         [--org <uuid>] [--roles admin,viewer]
         [--permissions dataset.write,dataset:write] [--hours 24]

Prints only the token on stdout. Never logs the secret.
"""

import argparse
import base64
import hashlib
import hmac
import json
import time
import uuid


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret", required=True)
    parser.add_argument("--sub", default=str(uuid.uuid4()))
    parser.add_argument("--org", default=None)
    parser.add_argument("--roles", default="")
    parser.add_argument("--permissions", default="dataset.write,dataset:write")
    parser.add_argument("--hours", type=float, default=24.0)
    args = parser.parse_args()

    now = int(time.time())
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = {
        "sub": args.sub,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + int(args.hours * 3600),
        "email": "p1-service@openfoundry.local",
        "name": "P1 Service Token",
        "roles": [r for r in args.roles.split(",") if r],
        "permissions": [p for p in args.permissions.split(",") if p],
    }
    if args.org:
        payload["org_id"] = args.org
    message = f"{header}.{b64(json.dumps(payload).encode())}"
    signature = b64(hmac.new(args.secret.encode(), message.encode(), hashlib.sha256).digest())
    print(f"{message}.{signature}")


if __name__ == "__main__":
    main()
