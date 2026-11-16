#!/usr/bin/env python3
"""Standalone evidence archiver for the P2.3 round (2026-11-14).

Parameterized variant of the frozen P2.2 archive_standalone.py: the P2.2
file hardcodes its source roots and destination, and frozen files are
never edited — so THIS round gets its own archiver with the same scrub
semantics and its OWN per-round index.json (root: "the new round of the
57 raw materials has its own index").

Semantics (identical to the accepted P2.2 archiver):
  * copies every file under each --src-root into --dst (subpaths kept)
  * .env-like file NAMES are skipped, never copied
  * value-level secrets are scrubbed WHEN PRESENT (env values, JWTs,
    private keys, lease token/holder values, DSN passwords); a scrubbed
    file is marked transformed with per-kind counts and its archive sha
    and is NEVER claimed byte-identical; clean files are byte-identical
    (src sha == dst sha)
  * output: <dst>/index.json with per-file src/dst/sha256/transform
  * historical FAIL/partial raws are preserved verbatim apart from the
    value redaction; temp originals are never modified

--selftest runs a fully local check (temp dir, synthetic non-secret
fixtures that merely match the scrub patterns) and exits 0/1.

Usage:
  python3 -I archive_standalone_p23.py --src-root <dir> [--src-root <dir2>]
      --dst <dir> [--env-file <env>] [--client-date 2026-11-14]
  python3 -I archive_standalone_p23.py --selftest
"""

import argparse
import hashlib
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path

JWT_RE = re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}")
PRIVKEY_RE = re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")
LEASE_RE = re.compile(r"(lease_token|lease_holder)([\"'\s:=]+)([A-Za-z0-9._\-]{8,})")
DSN_RE = re.compile(r"(postgres(?:ql)?://[^:/\s]+:)[^@\s]+(@)")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def env_values_of(env_file: Path):
    if not env_file or not env_file.exists():
        return []
    return [line.split("=", 1)[1].strip()
            for line in env_file.read_text().splitlines()
            if "=" in line and not line.strip().startswith("#")
            and line.split("=", 1)[1].strip()]


def scrub(text: str, env_vals):
    counts = {"env_value": 0, "jwt": 0, "private_key": 0,
              "lease_value": 0, "dsn_password": 0}
    for v in env_vals:
        if v and v in text:
            counts["env_value"] += text.count(v)
            text = text.replace(v, "<redacted>")
    text, n = JWT_RE.subn("<redacted-jwt>", text)
    counts["jwt"] = n
    text, n = PRIVKEY_RE.subn(
        "-----BEGIN PRIVATE KEY-----[redacted]", text)
    counts["private_key"] = n
    text, n = LEASE_RE.subn(r"\1\2<redacted>", text)
    counts["lease_value"] = n
    text, n = DSN_RE.subn(r"\1<redacted>\2", text)
    counts["dsn_password"] = n
    return text, counts


def archive(src_roots, dst: Path, env_file: Path, client_date: str):
    """Returns 1 when ANY required src-root is missing (fail-closed; the
    index still records the missing roots and any clean copies made)."""
    env_vals = env_values_of(env_file)
    dst.mkdir(parents=True, exist_ok=True)
    index = []
    failed = False
    for root in src_roots:
        root = Path(root)
        if not root.is_dir():
            index.append({"src_root": str(root), "missing": True})
            failed = True
            continue
        for src in sorted(root.rglob("*")):
            if not src.is_file():
                continue
            rel = src.relative_to(root)
            display = f"{root.name}/{rel}"
            if src.name.startswith(".env"):
                index.append({"src": display, "skipped": ".env-like name"})
                continue
            target = dst / root.name / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            raw = src.read_text(errors="replace")
            cleaned, counts = scrub(raw, env_vals)
            transformed = any(counts.values())
            if transformed:
                target.write_text(cleaned)
            else:
                shutil.copyfile(src, target)
            index.append({
                "src": display,
                "dst": str(target),
                "sha256_src": sha256(src),
                "sha256_dst": sha256(target),
                "transformed": transformed,
                "redactions": {k: v for k, v in counts.items() if v},
            })
    (dst / "index.json").write_text(json.dumps(
        {"client_date": client_date,
         "policy": "value-level secrets scrubbed (transformed files are "
                   "NOT byte-identical; clean files are). Historical "
                   "FAIL/partial raws preserved verbatim apart from value "
                   "redaction. Source originals untouched. This round's "
                   "own index; the P2.2 archive is read-only.",
         "files": index}, indent=2) + "\n")
    n_t = sum(1 for f in index if f.get("transformed"))
    print(f"[p23-standalone] archived {len(index)} entries, "
          f"transformed={n_t}, missing_roots="
          f"{sum(1 for f in index if f.get('missing'))} -> {dst}")
    for f in index:
        if f.get("transformed"):
            print("  transformed:", f["src"], f["redactions"])
        if f.get("missing"):
            print("  MISSING ROOT (fail-closed):", f["src_root"], file=sys.stderr)
    return 1 if failed else 0


def selftest():
    tmp = Path(tempfile.mkdtemp(prefix="p23-standalone-selftest."))
    try:
        src = tmp / "evidence"
        (src / "sub").mkdir(parents=True)
        # clean file -> byte-identical copy expected
        (src / "clean.log").write_text("=== RUN TestX\n--- PASS: TestX\n")
        # synthetic (NON-secret) fixtures that merely match the patterns
        (src / "dsn.log").write_text(
            "connect postgres://u:fakepw123@h:5432/db\n")
        (src / "jwt.log").write_text(
            "token eyJaaaaaaaaaaaaaaaaaa.bbbbbbbbbbbbbbbbbb.cccccccccc\n")
        (src / "envval.log").write_text("secret=superfakevalue99 end\n")
        (src / "sub" / ".env-like").write_text("X=1\n")
        envf = tmp / "env"
        envf.write_text("OF_FAKE_SECRET=superfakevalue99\n")
        dst = tmp / "dst"
        rc = archive([src], dst, envf, "2026-11-14")
        idx = json.loads((dst / "index.json").read_text())
        by = {e.get("src"): e for e in idx["files"]}
        checks = [
            ("rc", rc == 0),
            ("clean-byte-identical",
             by["evidence/clean.log"]["sha256_src"]
             == by["evidence/clean.log"]["sha256_dst"]
             and not by["evidence/clean.log"]["transformed"]),
            ("dsn-transformed",
             by["evidence/dsn.log"]["transformed"]
             and "dsn_password" in by["evidence/dsn.log"]["redactions"]),
            ("jwt-transformed", by["evidence/jwt.log"]["transformed"]),
            ("env-value-transformed",
             by["evidence/envval.log"]["transformed"]
             and "<redacted>" in (dst / "evidence" / "envval.log").read_text()),
            ("env-like-skipped",
             by["evidence/sub/.env-like"]["skipped"] == ".env-like name"),
            ("index-exists", (dst / "index.json").exists()),
        ]
        # negative case: a REQUIRED root that does not exist must FAIL
        # the archive (nonzero rc) while the index keeps the record and
        # the good root's files are still archived.
        neg_dst = tmp / "dst-negative"
        neg_rc = archive([src, tmp / "does-not-exist"], neg_dst, envf,
                         "2026-11-14")
        neg_idx = json.loads((neg_dst / "index.json").read_text())
        neg_missing = [e for e in neg_idx["files"] if e.get("missing")]
        checks += [
            ("missing-root-fails", neg_rc == 1),
            ("missing-root-recorded",
             len(neg_missing) == 1
             and neg_missing[0]["src_root"].endswith("does-not-exist")),
            ("missing-root-still-copies-clean",
             (neg_dst / "evidence" / "clean.log").exists()),
        ]
        failed = [label for label, ok in checks if not ok]
        for label, ok in checks:
            print(f"{'ok  ' if ok else 'FAIL'} {label}")
        print(f"\n[p23-standalone-selftest] "
              f"pass={len(checks) - len(failed)} fail={len(failed)}")
        return 1 if failed else 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-root", action="append", default=[])
    parser.add_argument("--dst")
    parser.add_argument("--env-file", type=Path,
                        default=Path("/private/tmp/openfoundry-platform-p1-q3v9/.env"))
    parser.add_argument("--client-date", default="2026-11-14")
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()
    if args.selftest:
        return selftest()
    if not args.src_root or not args.dst:
        parser.error("--src-root (>=1) and --dst are required "
                     "(or use --selftest)")
    return archive(args.src_root, Path(args.dst), args.env_file,
                   args.client_date)


if __name__ == "__main__":
    sys.exit(main())
