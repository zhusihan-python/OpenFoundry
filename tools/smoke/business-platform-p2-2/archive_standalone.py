#!/usr/bin/env python3
"""Archive the root's standalone evidence into the final directory.

Copies /private/tmp/openfoundry-p2-2-evidence/{connector,pg-cancel,
pg-migrate-concurrency} — ALL real raws incl. classified historical
FAIL/partial — into results/2026-11-14-p22-final/standalone/. The temp
originals are never modified. .env files are never copied. Value-level
secrets are scrubbed WHEN PRESENT (passwords/JWTs/private keys/lease
token-holder values/DSN passwords); a scrubbed file is marked
transformed with per-kind counts and its ARCHIVE sha — never claimed
byte-identical. Clean files are byte-identical copies (src sha == dst
sha). Output: standalone/index.json (per-file src/dst/sha/transform).
"""

import hashlib
import json
import re
from pathlib import Path
import shutil
import sys

SRC_ROOT = Path("/private/tmp/openfoundry-p2-2-evidence")
DST_ROOT = Path(__file__).resolve().parent / "results" / "2026-11-14-p22-final" / "standalone"
ROOTS = ("connector", "pg-cancel", "pg-migrate-concurrency")

JWT_RE = re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}")
PRIVKEY_RE = re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")
LEASE_RE = re.compile(r"(lease_token|lease_holder)([\"'\s:=]+)([A-Za-z0-9._\-]{8,})")
DSN_RE = re.compile(r"(postgres(?:ql)?://[^:/\s]+:)[^@\s]+(@)")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def scrub(text, env_values):
    counts = {"env_value": 0, "jwt": 0, "private_key": 0,
              "lease_value": 0, "dsn_password": 0}
    for v in env_values:
        if v and v in text:
            counts["env_value"] += text.count(v)
            text = text.replace(v, "<redacted>")
    text, n = JWT_RE.subn("<redacted-jwt>", text)
    counts["jwt"] = n
    text, n = PRIVKEY_RE.subn("-----BEGIN PRIVATE KEY-----[redacted]", text)
    counts["private_key"] = n
    text, n = LEASE_RE.subn(r"\1\2<redacted>", text)
    counts["lease_value"] = n
    text, n = DSN_RE.subn(r"\1<redacted>\2", text)
    counts["dsn_password"] = n
    return text, counts


def main():
    env_file = Path("/private/tmp/openfoundry-platform-p1-q3v9/.env")
    env_values = [l.split("=", 1)[1].strip() for l in env_file.read_text().splitlines()
                  if "=" in l and not l.strip().startswith("#")
                  and l.split("=", 1)[1].strip()]
    index = []
    for root in ROOTS:
        src_dir = SRC_ROOT / root
        if not src_dir.is_dir():
            print(f"[standalone] MISSING root {src_dir}")
            continue
        for src in sorted(src_dir.rglob("*")):
            if not src.is_file():
                continue
            rel = src.relative_to(SRC_ROOT)
            if src.name.startswith(".env"):
                index.append({"src": str(rel), "skipped": ".env-like name"})
                continue
            dst = DST_ROOT / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            raw = src.read_text(errors="replace")
            cleaned, counts = scrub(raw, env_values)
            transformed = any(counts.values())
            if transformed:
                dst.write_text(cleaned)
            else:
                shutil.copyfile(src, dst)
            index.append({
                "src": str(rel),
                "dst": str(dst.relative_to(DST_ROOT.parent)),
                "sha256_src": sha256(src),
                "sha256_dst": sha256(dst),
                "transformed": transformed,
                "redactions": {k: v for k, v in counts.items() if v},
            })
    DST_ROOT.mkdir(parents=True, exist_ok=True)
    (DST_ROOT / "index.json").write_text(json.dumps(
        {"client_date": "2026-11-14",
         "policy": "value-level secrets scrubbed (transformed files are "
                   "NOT byte-identical; clean files are). Historical "
                   "FAIL/partial raws preserved verbatim apart from "
                   "value redaction. Temp originals untouched.",
         "files": index}, indent=2) + "\n")
    n_t = sum(1 for f in index if f.get("transformed"))
    print(f"[standalone] archived {len(index)} entries, transformed={n_t}")
    for f in index:
        if f.get("transformed"):
            print("  transformed:", f["src"], f["redactions"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
