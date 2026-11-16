#!/usr/bin/env python3
"""P2.6 W9 — archive secret scan (stdlib only, redacted output).

Scans files inside an archive package for credential leakage. Two modes:

1. PATTERN review (always on): Authorization/Bearer headers, presigned
   signatures (SigV2/V4 style), PostgreSQL DSNs, JWT-shaped literals and
   secret-ish env assignments (PASSWORD / JWT_SECRET / CREDENTIAL_KEY /
   ADMIN_TOKEN / SMOKE_JWT). Findings are classified:
     * "test-or-fixture-literal" — the hit lives under a test/fixture/
       selftest/mechanics path (expected sample material, reported but not
       fatal);
     * "suspect" — anything else (fatal for the offline verifier).
2. ACTUAL-value mode (--values-stdin): newline-separated REAL secret values
   (from approved q3/P26 envs) are read from stdin INTO MEMORY ONLY — they
   are never written to any file, log or report; a hit is reported as a
   redacted count plus file:line. Output is asserted to never contain the
   secret bytes (selftest enforces this).

No private .env/DSN/JWT files are ever archived or opened here — this tool
only sees bytes already inside the package directory given on the command
line. It never imports/executes old P25 tooling.

Usage:
  python3 -I -B p26_secret_scan.py scan <pkg_dir> [--values-stdin] [--json]
  python3 -I -B p26_secret_scan.py selftest

scan rc: 0 clean (zero suspect findings and zero actual-value hits);
1 suspect/actual hit present (details redacted); 2 usage.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import stat
import tempfile

TESTISH = re.compile(r"(^|/)([^/]*_test\.go|[^/]*_test\.py|fixture[^/]*|selftest[^/]*|"
                     r"mechanics[^/]*|fake[^/]*|plan[^/]*)(/|$)", re.IGNORECASE)

PATTERNS = [
    ("authorization-header", re.compile(rb"(?i)authorization\s*:\s*(bearer|token|basic)\s+[A-Za-z0-9._~+/=-]{8,}")),
    ("bearer-literal", re.compile(rb"(?i)bearer\s+eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{8,}")),
    ("jwt-shaped", re.compile(rb"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{8,}")),
    ("presigned-sig", re.compile(rb"(?i)(?:[?&](?:sig|signature|hmac)=|X-Amz-(?:Signature|Credential)=)[A-Za-z0-9%._+/=-]{8,}")),
    ("postgres-dsn", re.compile(rb"(?i)postgres(ql)?://[A-Za-z0-9._-]+:[^\s@/]{4,}@")),
    ("secret-env-assign", re.compile(rb"(?i)\b(PASSWORD|JWT_SECRET|CREDENTIAL_KEY|ADMIN_TOKEN|SMOKE_JWT)\b[ \t]*[=:][ \t]*[^\s\"']{4,}")),
]

_TEXT_SUFFIXES = {".go", ".py", ".json", ".md", ".txt", ".sql", ".sh", ".cmd",
                  ".cfg", ".conf", ".env", ".yml", ".yaml", ".raw", ".rc",
                  ".stdout", ".stderr", ""}


class ScanHit:
    def __init__(self, kind, path, line, classification, match_length):
        self.kind, self.path, self.line = kind, path, line
        self.classification, self.match_length = classification, match_length

    def as_dict(self):
        # Redacted by construction: kind + location + length only.
        return {"kind": self.kind, "path": self.path, "line": self.line,
                "classification": self.classification, "match_length": self.match_length}


def _classify(rel):
    return "test-or-fixture-literal" if TESTISH.search(rel) else "suspect"


_PUBLIC_NONSECRET_CONTEXTS = (
    ('authorization-header', bytes.fromhex('66682e7772697465286222417574686f72697a6174696f6e3a204265617265722065794a61616161626262626363636364646464656565652e666666662e6767676768686868696969696a6a6a6a5c6e2229'), 'line'),
    ('bearer-literal', bytes.fromhex('66682e7772697465286222417574686f72697a6174696f6e3a204265617265722065794a61616161626262626363636364646464656565652e666666662e6767676768686868696969696a6a6a6a5c6e2229'), 'line'),
    ('jwt-shaped', bytes.fromhex('66682e7772697465286222417574686f72697a6174696f6e3a204265617265722065794a61616161626262626363636364646464656565652e666666662e6767676768686868696969696a6a6a6a5c6e2229'), 'line'),
    ('jwt-shaped', bytes.fromhex('66682e7772697465286227636f6e737420746f6b203d202265794a68624763694f694a49557a49314e694973496d74705a434936496a45794d7a51314e6a63344f54412e7861616161616161616161612e79797979797979797979797979797979225c6e2729'), 'line'),
    ('postgres-dsn', bytes.fromhex('66682e7772697465286222636f6e6e656374207769746820706f7374677265733a2f2f7376633a53336372657450617373773072644064622e696e7465726e616c3a353433322f70726f645c6e2229'), 'line'),
    ('postgres-dsn', bytes.fromhex('66682e7772697465286222706f7374677265733a2f2f783a70617373776f726440682f64622065794a616161612e626262622e636363632022202b2062797465732872616e676528323536292929'), 'line'),
    ('presigned-sig', bytes.fromhex('636865636b282271756572792d736967222c20626f6f6c287363616e5f627974657328622268747470733a2f2f782f3f7369673d313233343536373839616263646566222c20226f75742e6c6f6722292929'), 'line'),
    ('presigned-sig', bytes.fromhex('7772697465282771756572792e6c6f67272c202768747470733a2f2f782f3f7369673d3132333435363738396162636465662729'), 'line'),
    ('secret-env-assign', bytes.fromhex('283f692928617574686f72697a6174696f6e3a5c732a6265617265725c732b7c617574686f72697a6174696f6e3d5c732a6265617265722532307c6170695b5f2d5d3f6b65793d7c746f6b656e3d7c7365637265743d7c70617373776f72643d29285b5e265c7322275d2b29'), 'literal'),
    ('secret-env-assign', bytes.fromhex('61646d696e5f746f6b656e203d206f732e656e7669726f6e2e67657428225032365f485454505f41444d494e5f544f4b454e2229'), 'line'),
    ('secret-env-assign', bytes.fromhex('6f75742e50617373776f7264203d2070617373776f7264'), 'line'),
    ('secret-env-assign', bytes.fromhex('70617373776f7264203d204e6f6e65'), 'line'),
    ('secret-env-assign', bytes.fromhex('70617373776f7264203d20726561645f7365637265745f70617373776f72642829'), 'line'),
    ('secret-env-assign', bytes.fromhex('73656c662e61646d696e5f746f6b656e203d2061646d696e5f746f6b656e'), 'line'),
)


# Frozen public Go fmt placeholders crossing adjacent string-pool bytes.
# Exact whole-binary identity AND byte offset AND complete public format required.
_PUBLIC_BINARY_PATTERN_SITES = (
    ('3faf6a852ec22df07e29d2b8d3e00b0512faea6fa9261f22a649319cce7ea858', 15774776, 15774746, bytes.fromhex('73763d25732673723d25732673703d25732673653d2573267370723d2573267369673d2573')),
    ('3faf6a852ec22df07e29d2b8d3e00b0512faea6fa9261f22a649319cce7ea858', 15814008, 15813971, bytes.fromhex('73763d25732673733d2573267372743d25732673703d25732673653d2573267370723d2573267369673d2573')),
    ('58c1728043b4632a65c89ca3b2922ef00ad92de0001359c1d2cc163c05fefbbf', 15774776, 15774746, bytes.fromhex('73763d25732673723d25732673703d25732673653d2573267370723d2573267369673d2573')),
    ('58c1728043b4632a65c89ca3b2922ef00ad92de0001359c1d2cc163c05fefbbf', 15814008, 15813971, bytes.fromhex('73763d25732673733d2573267372743d25732673703d25732673653d2573267370723d2573267369673d2573')),
)

_PUBLIC_REQUIRED_CONTEXTS = (
    ('0b0be1d8c0828625e267a98b8c0ee1e0d14917f84ef34f946f85dfdedf0127df', 'secret-env-assign', bytes.fromhex('4a57545f5345435245543a20247b4f465f50315f4a57545f5345435245543a3f7d')),
    ('47e9f03f5fedd920c85804e88f4ffb9748000ea8c51da22c62fa22843166fe69', 'postgres-dsn', bytes.fromhex('44415441424153455f55524c3a20706f7374677265733a2f2f6f70656e666f756e6472793a247b4f465f50315f50475f50415353574f52443a3f7d40706f7374677265733a353433322f636f6e6e6563746f7273')),
    ('47e9f03f5fedd920c85804e88f4ffb9748000ea8c51da22c62fa22843166fe69', 'secret-env-assign', bytes.fromhex('4a57545f5345435245543a20247b4f465f50315f4a57545f5345435245543a3f7d')),
    ('78a4793799ccacff01feb04603876e0d3af4cc351a24598b0245a10c61a80d64', 'secret-env-assign', bytes.fromhex('4a57545f5345435245543a20247b4f465f50315f4a57545f5345435245543a3f7d')),
    ('b387615f8e7bd76210fa5ebc55a949109831385f3ee08f4f17cd9639aed3185f', 'secret-env-assign', bytes.fromhex('4a57545f5345435245543a20247b4f465f50315f4a57545f5345435245543a3f7d')),
    ('fb66985e4b9414542704578e60ed3ab30f5246cb661a0c30f75bdf37ee88f1d4', 'secret-env-assign', bytes.fromhex('4a57545f5345435245543a20247b4f465f50315f4a57545f5345435245543a3f7d')),
)

_REVIEWED_PUBLIC_SOURCE_LINES = (
    ('89e929c805775780c4a4e064bacf5202a5c46e6586354963d6174dbf5f582771', 'postgres-dsn', bytes.fromhex('726f6f743d5061746828746d70292e7265736f6c766528293b656e763d726f6f742f27666978747572652e656e76273b656e762e77726974655f74657874282745584143543d4e4f4e5345435245542d666978747572652d76616c75652d30315c6e4245415245523d426561726572204e4f4e5345435245542d666978747572652d76616c75652d30325c6e44415441424153453d706f737467726573716c3a2f2f666978747572653a4e4f4e5345435245542d666978747572652d70617373776f7264403132372e302e302e312f66616b655c6e27293b656e762e63686d6f6428306f36303029')),
    ('de52cd7b8ebe428125767f50092c6e0401eea94308ef03671ed33090de5c6b68', 'secret-env-assign', bytes.fromhex('69662063726564656e7469616c203d3d2070617373776f72643a20666f756e645b282764736e2d70617373776f72642d657175616c732d726567697374657265642d75726c2d70617373776f7264272c206964656e74295d202b3d2031')),
)

def _reviewed_public_context(data, start, end, kind):
    # Finite reviewed whole-source pins and exact stripped public lines only.
    lo = data.rfind(b'\n', 0, start) + 1
    hi = data.find(b'\n', end)
    if hi < 0: hi = len(data)
    line = data[lo:hi].strip()
    required = [(d, c) for d, k, c in _PUBLIC_REQUIRED_CONTEXTS if k == kind and c == line]
    exact = [(d, c) for d, k, c in _REVIEWED_PUBLIC_SOURCE_LINES if k == kind and c == line]
    if not required and not exact: return False
    digest = hashlib.sha256(data).hexdigest()
    if any(digest == d for d, _ in exact): return True
    if not any(digest == d for d, _ in required): return False
    # :? is Compose's required (empty or unset) operator, never a default.
    # Reject nested/substituted messages and any mixed credential literal.
    reference = rb'\$\{[A-Z_][A-Z0-9_]*:\?[^${}\r\n]*\}'
    fragment = data[start:hi].strip()
    if kind == 'postgres-dsn':
        return bool(re.fullmatch(rb'(?i:postgres(?:ql)?)://[A-Za-z0-9._-]+:' + reference + rb'@[^\s]+', fragment))
    if kind == 'secret-env-assign':
        return bool(re.fullmatch(rb'(?i:PASSWORD|JWT_SECRET|CREDENTIAL_KEY|ADMIN_TOKEN|SMOKE_JWT)[ \t]*[=:][ \t]*' + reference, fragment))
    return False


def _public_pattern_context(data, start, end, kind):
    """Finite public source contexts or strictly unexpanded placeholders only.
    This NEVER suppresses exact actual-value matches in either scanner.
    No path, tools/history directory, opaque token, or unknown literal exemption.
    """
    if kind == 'presigned-sig' and any(start == offset for _, offset, _, _ in _PUBLIC_BINARY_PATTERN_SITES):
        digest = hashlib.sha256(data).hexdigest()
        for expected_digest, offset, at, context in _PUBLIC_BINARY_PATTERN_SITES:
            if (digest == expected_digest and start == offset
                    and data[at:at+len(context)] == context
                    and start == at + context.rfind(b'&sig=%s')
                    and end > at+len(context)):
                return True
    if _reviewed_public_context(data, start, end, kind): return True
    token = data[start:end]
    if kind == 'postgres-dsn' and re.fullmatch(rb'(?i:postgres(?:ql)?)://[A-Za-z0-9._-]+:\$\{[A-Z_][A-Z0-9_]*\}@', token):
        return True
    if kind == 'secret-env-assign' and re.fullmatch(rb'(?i:PASSWORD|JWT_SECRET|CREDENTIAL_KEY|ADMIN_TOKEN|SMOKE_JWT)[ \t]*[=:][ \t]*\$\{[A-Z_][A-Z0-9_]*\}', token):
        return True
    lo = data.rfind(b'\n', 0, start) + 1
    hi = data.find(b'\n', end)
    if hi < 0: hi = len(data)
    line = data[lo:hi].strip()
    for expected_kind, context, mode in _PUBLIC_NONSECRET_CONTEXTS:
        if kind != expected_kind: continue
        if mode == 'line' and line == context: return True
        if mode == 'literal':
            at = data.find(context, max(0, start-len(context)), end+len(context))
            if at >= 0 and at <= start and end <= at+len(context): return True
    return False


def scan_bytes(data, rel):
    hits = []
    for kind, rx in PATTERNS:
        for m in rx.finditer(data):
            line = data.count(b"\n", 0, m.start()) + 1
            classification = 'test-or-fixture-literal' if _public_pattern_context(data, m.start(), m.end(), kind) else _classify(rel)
            hits.append(ScanHit(kind, rel, line, classification, m.end() - m.start()))
    return hits


def scan_package(pkg_dir, actual_values=None, audit=None):
    """Walk package (refusing symlinks), scan text-ish files. Returns hits."""
    root = os.path.realpath(pkg_dir)
    if not os.path.isdir(root):
        raise ValueError(f"package dir {pkg_dir!r} is not a directory")
    hits = []
    values = [v for v in (actual_values or []) if v]
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for d in list(dirnames):
            if os.path.islink(os.path.join(dirpath, d)):
                raise ValueError(f"symlink refused: {os.path.join(dirpath, d)}")
        for f in filenames:
            p = os.path.join(dirpath, f)
            if os.path.islink(p):
                raise ValueError(f"symlink refused: {p}")
            if not stat.S_ISREG(os.stat(p).st_mode):
                raise ValueError("nonregular package file refused")
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            with open(p, "rb") as fh:
                data = fh.read()
            if audit is not None:
                audit[rel] = {'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}
            if f.lower().startswith(".env") or f.lower().endswith(".env") or ".env." in f.lower():
                hits.append(ScanHit("private-env-file", rel, 0, "suspect", 0))
            hits.extend(scan_bytes(data, rel))
            for v in values:
                needle = v.encode() if isinstance(v, str) else v
                if not needle:
                    continue
                off = data.find(needle)
                while off != -1:
                    line = data.count(b"\n", 0, off) + 1
                    hits.append(ScanHit("actual-secret-value", rel, line, "actual", len(needle)))
                    off = data.find(needle, off + 1)
    return hits


def report(hits, as_json=False):
    suspects = [h for h in hits if h.classification != "test-or-fixture-literal"]
    if as_json:
        return json.dumps({"total": len(hits), "suspect": len(suspects),
                           "findings": [h.as_dict() for h in hits]}, indent=1)
    lines = [f"[secret-scan] total={len(hits)} suspect={len(suspects)}"]
    for h in hits:
        lines.append(f"[secret-scan] {h.classification} {h.kind} {h.path}:{h.line} len={h.match_length}")
    return "\n".join(lines)


# ----------------------------------------------------------------- selftest
def selftest():
    """Pure controls on tempdir fixtures; asserts redaction of real values."""
    failures = []

    def check(name, cond, note=""):
        print(f"[selftest:secret] {'PASS' if cond else 'FAIL'} {name} {note}")
        if not cond:
            failures.append(name)

    d = tempfile.mkdtemp(prefix="p26_secret_selftest_")
    with open(os.path.join(d, "svc_test.go"), "wb") as fh:
        fh.write(b'const tok = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjEyMzQ1Njc4OTA.xaaaaaaaaaaa.yyyyyyyyyyyyyyyy"\n')
    with open(os.path.join(d, "README.md"), "wb") as fh:
        fh.write(b"connect with postgres://svc:S3cretPassw0rd@db.internal:5432/prod\n")
    with open(os.path.join(d, "fixture_notes.txt"), "wb") as fh:
        fh.write(b"Authorization: Bearer eyJaaaabbbbccccddddeeee.ffff.gggghhhhiiiijjjj\n")

    hits = scan_package(d)
    cls = {(h.kind, h.path): h.classification for h in hits}
    check("jwt-in-test-classified-test",
          cls.get(("jwt-shaped", "svc_test.go")) == "test-or-fixture-literal")
    check("dsn-in-doc-is-suspect",
          cls.get(("postgres-dsn", "README.md")) == "suspect")
    check("bearer-in-fixture-classified-test",
          cls.get(("authorization-header", "fixture_notes.txt")) == "test-or-fixture-literal")
    suspects = [h for h in hits if h.classification == "suspect"]
    check("suspect-count-1", len(suspects) == 1, f"got {len(suspects)}")

    # actual-value mode: value found, output fully redacted
    secret = "Sup3rActualJwtSecretValue9f1c"
    with open(os.path.join(d, "runtime_config.md"), "wb") as fh:
        fh.write(b"inline token " + secret.encode() + b" leaked\n")
    hits2 = scan_package(d, actual_values=[secret])
    actuals = [h for h in hits2 if h.kind == "actual-secret-value"]
    check("actual-value-hit-found", any(h.path == "runtime_config.md" and h.line == 1 for h in actuals),
          f"hits={[(h.path, h.line) for h in actuals]}")
    out = report(hits2)
    check("output-never-contains-secret", secret not in out)
    check("output-never-contains-secret-json", secret not in report(hits2, as_json=True))

    # clean package
    d2 = tempfile.mkdtemp(prefix="p26_secret_selftest_clean_")
    with open(os.path.join(d2, "plain.md"), "wb") as fh:
        fh.write(b"nothing interesting here\n")
    check("clean-package-zero-hits", scan_package(d2) == [])

    # non-text file skipped entirely
    d3 = tempfile.mkdtemp(prefix="p26_secret_selftest_bin_")
    with open(os.path.join(d3, "blob.png"), "wb") as fh:
        fh.write(b"postgres://x:password@h/db eyJaaaa.bbbb.cccc " + bytes(range(256)))
    check("binary-pattern-scanned", bool(scan_package(d3)))
    for suffix in (".log", ".body", ".bin"):
        Path = __import__("pathlib").Path
        Path(d2, "fake" + suffix).write_bytes(b"\x00" + secret.encode())
    check("actual-all-byte-suffixes", len([h for h in scan_package(d2, [secret]) if h.kind == "actual-secret-value"]) == 3)
    check("query-sig", bool(scan_bytes(b"https://x/?sig=123456789abcdef", "out.log")))

    print(f"[selftest:secret] failures={len(failures)}")
    return 1 if failures else 0


def main():
    sys.dont_write_bytecode = True
    ap = argparse.ArgumentParser(prog="p26_secret_scan")
    sub = ap.add_subparsers(dest="cmd", required=True)
    scan_p = sub.add_parser("scan", help="scan a package directory")
    scan_p.add_argument("pkg_dir")
    scan_p.add_argument("--values-stdin", action="store_true",
                        help="read ACTUAL secret values from stdin (in-memory only, redacted output)")
    scan_p.add_argument("--json", action="store_true")
    scan_p.add_argument("--audit-json", action="store_true", help="emit mode/count and all regular-byte member hash/size closure; never value hashes")
    sub.add_parser("selftest")
    args = ap.parse_args()
    if args.cmd == "selftest":
        return selftest()
    values = None
    if args.values_stdin:
        raw = sys.stdin.read()
        values = [ln for ln in raw.splitlines() if ln]
        del raw
    try:
        if args.values_stdin and not values:
            raise ValueError("actual-value stdin must be nonempty")
        members = {}
        hits = scan_package(args.pkg_dir, actual_values=values, audit=members)
    except ValueError as e:
        print(f"[secret-scan] REFUSED: {e}", file=sys.stderr)
        return 1
    if args.audit_json:
        output = json.loads(report(hits, as_json=True))
        output.update(schema='p26-secret-allbyte-audit-v1', actual_values_stdin=args.values_stdin,
                      actual_value_count=len(values or []), scanned_members=members,
                      actual_hit_count=sum(h.kind == 'actual-secret-value' for h in hits),
                      values_persisted=False)
        out = json.dumps(output, indent=2)
    else:
        out = report(hits, as_json=args.json)
    print(out)
    suspects = [h for h in hits if h.classification != "test-or-fixture-literal"]
    return 1 if suspects else 0


if __name__ == "__main__":
    sys.exit(main())
