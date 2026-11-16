#!/usr/bin/env python3
"""P2.6 W9 — standalone offline archive verifier (stdlib only, fail-closed).

Verifies a P26 archive PACKAGE directory with zero dependency on Docker,
PostgreSQL, HTTP, the original workspace or any private env: every
artifact is resolved via relative paths INSIDE the package. The original
absolute project path is accepted ONLY as a canonical LABEL for the
Go+SQL source-hash recipe — it is never opened.

The Go+SQL source hash is byte-equal to the accepted P26 recipe
(common/p26_gates.py candidate_stream: go.mod/go.sum literal framing,
"content-hashes" marker, sorted "<sha256>  <absolute-label-path>\\n"
lines over services/*/ and libs/scheduling-cron with .go/.sql suffixes)
but is computed from SNAPSHOT bytes under <pkg>/source/ with the original
root path used purely as the label.

Fail-closed semantics: the verdict is NOTREADY with explicit reasons
unless EVERY accepted roster item has all satisfier evidence steps
actually satisfied AND every pending roster id is closed by an explicit
Root final acceptance file AND the manifest, source hash and secret scan
are all clean. The verifier NEVER prints PASS/SEALED — at best
"VERIFIED-NOT-SEALED"; the actual seal is Root's phase (W10 ordering).

Evidence step rules (see W9-ARCHIVE-INTERFACE.md):
  * classification must be "actual" — "fake"/"plan"/"synthetic"/
    "mechanics-fake" are history-only and never satisfy a requirement;
  * rc_kind "unix": satisfied iff rc == 0;
  * rc_kind "http-status": rc IS an HTTP status (100..599) and must equal
    expected_status, AND outer_rc (the outer CLI Unix exit, recorded
    separately) must be 0 — an HTTP step's .rc is a STATUS, not a Unix rc,
    and the two are never conflated;
  * artifact paths must be package-relative, non-escaping, non-symlink.

Usage:
  python3 -I -B p26_archive_verifier.py verify <pkg_dir> \
      --canonical-root-label /Volumes/ExternalSD/projects/OpenFoundry \
      [--roster <roster.json>] [--emit-report-skeleton <out.json>] \
      [--values-stdin]
  python3 -I -B p26_archive_verifier.py selftest

verify rc: 0 VERIFIED-NOT-SEALED; 1 NOTREADY/refused (reasons printed).
"""

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile

SOURCE_ROOTS = (
    ("services/connector-management-service", (".go", ".sql")),
    ("services/dataset-versioning-service", (".go", ".sql")),
    ("services/pipeline-build-service", (".go", ".sql")),
    ("libs/scheduling-cron", (".go",)),
)
MANIFEST_FORMAT = "p26-archive-manifest-v1"
_REL_OK = re.compile(r"^[A-Za-z0-9._][A-Za-z0-9._+-]*(?:/[A-Za-z0-9._+-]+)*$")
_HISTORY_ONLY = {"fake", "plan", "synthetic", "mechanics-fake", "history-only"}


class Refusal(Exception):
    pass


def problems_init():
    return []


def add(probs, msg):
    probs.append(msg)
    return probs


# ------------------------------------------------------------- path guards
def safe_rel(rel):
    if not rel or rel.startswith("/") or "\\" in rel or "\x00" in rel or not _REL_OK.match(rel):
        raise Refusal(f"illegal package-relative path {rel!r}")
    if any(p in ("", ".", "..") for p in rel.split("/")):
        raise Refusal(f"illegal package-relative path {rel!r}")
    return rel


def pkg_file(pkg_root, rel, must_exist=True):
    safe_rel(rel)
    p = os.path.join(pkg_root, *rel.split("/"))
    real_root, real_p = os.path.realpath(pkg_root), os.path.realpath(p)
    if not real_p.startswith(real_root + os.sep):
        raise Refusal(f"artifact escapes package root: {rel!r}")
    if any(os.path.islink(os.path.join(pkg_root, *rel.split("/")[:n])) for n in range(1, len(rel.split("/")) + 1)):
        raise Refusal(f"symlink refused: {rel!r}")
    if must_exist and not os.path.isfile(p):
        raise Refusal(f"artifact missing inside package: {rel!r}")
    return p


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ------------------------------------------------- S recipe from snapshot
def candidate_hash_from_snapshot(pkg_root, canonical_label):
    """Byte-equal P26 S recipe over snapshot bytes; label-only root path."""
    if not canonical_label or not canonical_label.startswith("/"):
        raise Refusal("canonical root label must be an absolute path string (label only)")
    snap = os.path.join(pkg_root, "source")
    if not os.path.isdir(snap):
        raise Refusal("package lacks source/ snapshot")
    for base in ("go.mod", "go.sum"):
        p = os.path.join(snap, base)
        if not os.path.isfile(p) or os.path.islink(p):
            raise Refusal(f"source snapshot missing {base}")
    parts = [b"go.mod\n", open(os.path.join(snap, "go.mod"), "rb").read(),
             b"go.sum\n", open(os.path.join(snap, "go.sum"), "rb").read(),
             b"content-hashes\n"]
    label_paths = []
    for rel_root, exts in SOURCE_ROOTS:
        base = os.path.join(snap, *rel_root.split("/"))
        if not os.path.isdir(base):
            raise Refusal(f"source snapshot missing dir {rel_root}")
        for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
            for d in list(dirnames):
                if os.path.islink(os.path.join(dirpath, d)):
                    raise Refusal(f"symlink refused in snapshot: {d}")
            for f in filenames:
                p = os.path.join(dirpath, f)
                if os.path.islink(p):
                    raise Refusal(f"symlink refused in snapshot: {p}")
                if os.path.splitext(f)[1] in exts:
                    inner = os.path.relpath(p, snap).replace(os.sep, "/")
                    label_paths.append((f"{canonical_label}/{inner}", p))
    for label, p in sorted(label_paths, key=lambda t: t[0]):
        parts.append(f"{sha256_file(p)}  {label}\n".encode())
    return hashlib.sha256(b"".join(parts)).hexdigest()


# ------------------------------------------------------------ membership
def compute_manifest_entries(pkg_root):
    root = os.path.realpath(pkg_root)
    entries = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for d in list(dirnames):
            if os.path.islink(os.path.join(dirpath, d)):
                raise Refusal(f"symlink directory refused: {os.path.join(dirpath, d)}")
        for f in filenames:
            p = os.path.join(dirpath, f)
            if not os.path.isfile(p):
                raise Refusal("nonregular member refused")
            if os.path.islink(p):
                raise Refusal(f"symlink refused: {p}")
            if not os.path.realpath(p).startswith(root + os.sep):
                raise Refusal(f"path escapes package root: {p}")
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            safe_rel(rel)
            if rel == "manifest.json":  # root-level manifest self-exclusion
                continue
            if rel in entries:
                raise Refusal(f"duplicate relpath {rel!r}")
            entries[rel] = {"sha256": sha256_file(p), "size": os.path.getsize(p)}
    return entries


def verify_membership(pkg_root):
    """If manifest.json exists, exact membership + content must match."""
    mpath = pkg_file(pkg_root, "manifest.json")
    if not os.path.isfile(mpath):
        raise Refusal("required manifest.json missing")
    with open(mpath, "r", encoding="utf-8") as fh:
        man = json.load(fh)
    if man.get("format") != MANIFEST_FORMAT:
        raise Refusal(f"unknown manifest format {man.get('format')!r}")
    disk = compute_manifest_entries(pkg_root)
    listed = {}
    for e in man.get("files", []):
        safe_rel(e["path"])
        if e["path"] in listed:
            raise Refusal(f"duplicate relpath in manifest: {e['path']}")
        listed[e["path"]] = e
    missing = sorted(set(disk) - set(listed))
    extra = sorted(set(listed) - set(disk))
    if missing or extra:
        raise Refusal(f"manifest membership mismatch: missing-from-manifest={missing} extra-in-manifest={extra}")
    bad = [r for r, e in listed.items()
           if e.get("sha256") != disk[r]["sha256"] or e.get("size") != disk[r]["size"]]
    if bad:
        raise Refusal(f"manifest content mismatch: {sorted(bad)}")
    blob = "".join(f"{listed[r]['sha256']}  {listed[r]['size']}  {r}\n" for r in sorted(listed)).encode()
    total = hashlib.sha256(blob).hexdigest()
    if total != man.get("manifest_sha256") or man.get("file_count") != len(disk):
        raise Refusal("manifest_sha256/file_count mismatch")
    return {"file_count": len(disk), "manifest_sha256": total}


# ---------------------------------------------------------- secret scan
_SECRET_PATTERNS = [
    ("postgres-dsn", re.compile(rb"(?i)postgres(ql)?://[A-Za-z0-9._-]+:[^\s@/]{4,}@")),
    ("jwt-shaped", re.compile(rb"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{8,}")),
    ("authorization-header", re.compile(rb"(?i)authorization\s*:\s*(bearer|token|basic)\s+[A-Za-z0-9._~+/=-]{8,}")),
    ("presigned-sig", re.compile(rb"(?i)(?:[?&](?:sig|signature|hmac)=|X-Amz-(?:Signature|Credential)=)[A-Za-z0-9%._+/=-]{8,}")),
    ("secret-env-assign", re.compile(rb"(?i)\b(PASSWORD|JWT_SECRET|CREDENTIAL_KEY|ADMIN_TOKEN|SMOKE_JWT)\b[ \t]*[=:][ \t]*[^\s\"']{4,}")),
]
_TESTISH = re.compile(r"(^|/)([^/]*_test\.go|[^/]*_test\.py|fixture[^/]*|selftest[^/]*|"
                      r"mechanics[^/]*|fake[^/]*|plan[^/]*)(/|$)", re.IGNORECASE)
_TEXT = {".go", ".py", ".json", ".md", ".txt", ".sql", ".sh", ".cmd", ".cfg",
         ".conf", ".env", ".yml", ".yaml", ".raw", ".rc", ".stdout", ".stderr", ""}


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


def secret_suspects(pkg_root, actual_values=None):
    root = os.path.realpath(pkg_root)
    suspects, actual_hits = [], 0
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for d in list(dirnames):
            if os.path.islink(os.path.join(dirpath, d)):
                raise Refusal(f"symlink refused: {os.path.join(dirpath, d)}")
        for f in filenames:
            p = os.path.join(dirpath, f)
            if not os.path.isfile(p):
                raise Refusal("nonregular member refused")
            if os.path.islink(p):
                raise Refusal("symlink refused")
            if not os.path.isfile(p):
                raise Refusal("nonregular file refused")
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            if f.lower().startswith(".env") or f.lower().endswith(".env") or ".env." in f.lower():
                suspects.append(f"private-env-file {rel}")
            data = open(p, "rb").read()
            for kind, rx in _SECRET_PATTERNS:
                for m in rx.finditer(data):
                    if _TESTISH.search(rel) or _public_pattern_context(data, m.start(), m.end(), kind):
                        continue  # test/fixture literals: reported upstream, not fatal here
                    suspects.append(f"{kind} {rel}:{data.count(b'\n', 0, m.start()) + 1}")
            for v in (actual_values or []):
                needle = v.encode() if isinstance(v, str) else v
                if needle and needle in data:
                    actual_hits += 1
    return suspects, actual_hits


# --------------------------------------------------------- evidence rules
def step_satisfied(step):
    """None if satisfied, else a reason string. Never raises on bad shape."""
    if step.get("classification") != "actual":
        cls = step.get("classification", "<missing>")
        if cls in _HISTORY_ONLY:
            return f"classification={cls} is history-only, never satisfies a formal requirement"
        return f"classification={cls!r} is not 'actual'"
    kind = step.get("rc_kind")
    rc = step.get("rc")
    if kind == "unix":
        if not isinstance(rc, int) or isinstance(rc, bool):
            return "unix rc must be an integer"
        return None if rc == 0 else f"unix rc={rc} (nonzero without a preserved-history declaration)"
    if kind == "http-status":
        if type(rc) is not int or not (100 <= rc <= 599):
            return f"http-status rc={rc!r} is not an HTTP status"
        if rc != step.get("expected_status"):
            return f"http status {rc} != expected {step.get('expected_status')!r}"
        outer = step.get("outer_rc")
        if type(outer) is not int or outer != 0:
            return f"http step outer CLI rc={outer!r} must be Unix 0 (separate from the status)"
        return None
    return f"unknown rc_kind {kind!r}"


def load_evidence(pkg_root):
    p = pkg_file(pkg_root, "evidence/evidence.json")
    with open(p, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    steps = doc.get("steps", [])
    return formal_core().index_steps(doc)


def formal_core():
    import importlib.util
    spec=importlib.util.spec_from_file_location('p26_formal_core',os.path.join(os.path.dirname(__file__),'p26_formal_core.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def proof_reader_factory(root):
    return lambda: formal_core().original_module().Reader(root)


def verify_artifact_nonlocal_refusal(pkg_root, step):
    rel = step.get("artifact", "")
    safe_rel(rel)
    pkg_file(pkg_root, rel)  # raises Refusal on absolute/escaping/missing/symlink


def verify_original(pkg_root, step, source):
    try:return formal_core().original(step,source,proof_reader_factory(pkg_root))
    except (ValueError,OSError) as e:raise Refusal(str(e)) from e


# ---------------------------------------------------------------- verify
def verify(pkg_root, canonical_label, roster_path=None, emit=None, actual_values=None):
    probs = problems_init()
    if not os.path.isdir(pkg_root):
        return ["NOTREADY"], [f"package dir {pkg_root!r} not found"], None
    try:
        membership = verify_membership(pkg_root)
        rel = roster_path or "roster/p26_formal_roster.json"
        # CLI roster is always package-relative; absolute paths are rejected before I/O.
        roster_file = pkg_file(pkg_root, rel)
        with open(roster_file, "r", encoding="utf-8") as fh:
            roster = json.load(fh)
    except (Refusal, ValueError, OSError) as e:
        return "NOTREADY", [f"manifest/roster: {e}"], None

    required_ids = set(formal_core().KINDS)-{'final_restart_and_receipt','h12-targeted'}
    ids = [i.get("id") for i in roster.get("items", [])]
    if set(ids) != required_ids or len(ids) != len(set(ids)):
        add(probs, "roster required scope missing/extra/duplicate IDs")
    try:
        allow=formal_core().source_allowlist(pkg_file(pkg_root,'tools/p26_source402_allowlist.json'))
        actual=[]
        for d,_,names in os.walk(os.path.join(pkg_root,'source')):
            actual.extend(os.path.relpath(os.path.join(d,n),os.path.join(pkg_root,'source')).replace(os.sep,'/') for n in names)
        if set(actual)!=set(allow) or len(actual)!=len(allow):raise Refusal('exact frozen source allowlist mismatch')
        pins=formal_core().source_pins(pkg_file(pkg_root,'tools/p26_source402_allowlist.json'))
        for rel,pin in pins.items():
            source_path=pkg_file(pkg_root,'source/'+rel)
            if sha256_file(source_path)!=pin['sha256'] or os.path.getsize(source_path)!=pin['size']:raise Refusal('source frozen per-file hash/size mismatch')
    except (ValueError,OSError,KeyError,Refusal) as e:add(probs,'source membership: '+str(e))
    src = roster.get("source", {})
    want_s = src.get("go_sql_sha256")
    if not want_s:
        add(probs, "roster: source.go_sql_sha256 missing")
    else:
        try:
            got_s = candidate_hash_from_snapshot(pkg_root, canonical_label)
            if got_s != want_s:
                add(probs, f"source: snapshot Go+SQL hash {got_s} != roster {want_s} "
                           f"(recipe over source/ bytes with canonical labels)")
        except (Refusal, OSError, ValueError, KeyError, TypeError) as e:
            add(probs, f"source: {e}")

    try:
        by_id = load_evidence(pkg_root)
    except (Refusal, OSError, ValueError, KeyError, TypeError) as e:
        add(probs, f"evidence: {e}")
        by_id = None

    items_out = []
    try:
        suspects, actual_hits = secret_suspects(pkg_root, actual_values)
        if suspects:
            add(probs, f"secret-scan: {len(suspects)} suspect finding(s) (redacted): " +
                "; ".join(suspects[:5]) + (" ..." if len(suspects) > 5 else ""))
        if actual_hits:
            add(probs, f"secret-scan: {actual_hits} ACTUAL registered secret value hit(s) — never archivable")
    except (Refusal, OSError, ValueError, KeyError, TypeError) as e:
        add(probs, f"secret-scan: {e}")
        suspects, actual_hits = [], 0

    for item in roster.get("items", []):
        iid, status = item.get("id", "<no-id>"), item.get("status", "<no-status>")
        entry = {"id": iid, "status": status, "satisfied": False, "reasons": []}
        try:
            formal_core().validate_item(item,by_id or {},want_s,proof_reader_factory(pkg_root))
            entry["satisfied"]=True
        except (ValueError,OSError,KeyError,TypeError) as e:
            entry["reasons"].append(str(e));add(probs,f"roster item {iid} not satisfied: {e}")
        items_out.append(entry)

    pending_ids = [i["id"] for i in roster.get("items", []) if i.get("status") == "pending"]
    acceptance = {"closed": [], "file": None}
    acc_path = os.path.join(pkg_root, "root-acceptance.json")
    if pending_ids:
        if not os.path.isfile(acc_path):
            add(probs, "Root final acceptance file absent while roster has pending items: " +
                ", ".join(pending_ids))
        else:
            with open(acc_path, "r", encoding="utf-8") as fh:
                acc = json.load(fh)
            acceptance["file"] = "root-acceptance.json"
            raw_accepted = acc.get("accepted", [])
            accepted = set(x.get("id") for x in raw_accepted if isinstance(x, dict) and x.get("satisfiers") and any(i.get("id") == x.get("id") and i.get("satisfiers") == x["satisfiers"] for i in roster.get("items", [])))
            validated = {x["id"] for x in items_out if x["satisfied"]}
            acceptance["closed"] = sorted(accepted & set(pending_ids) & validated)
            accepted &= validated
            not_closed = [p for p in pending_ids if p not in accepted]
            if not_closed:
                add(probs, "Root final acceptance does not close pending items: " + ", ".join(not_closed))
            if not acc.get("roster_complete"):
                add(probs, "Root final acceptance lacks roster_complete=true")

    try:
        restart=roster.get('pre_seal_requirements',{}).get('final_restart_and_receipt',{})
        formal_core().validate_item(restart,by_id or {},want_s,proof_reader_factory(pkg_root),'final_restart_and_receipt')
    except (ValueError,OSError,KeyError,TypeError) as e:add(probs,'final restart/readability/replay: '+str(e))

    try:
        with open(pkg_file(pkg_root,'root-acceptance.json'),encoding='utf-8') as f:final_acceptance=json.load(f)
        formal_core().validate_all(roster,{'steps':list((by_id or {}).values())},final_acceptance,want_s,proof_reader_factory(pkg_root))
    except (ValueError,OSError,KeyError,TypeError,Refusal) as e:
        add(probs,'shared formal/restart/history core: '+str(e))


    report = {
        "roster_version": roster.get("roster_version"),
        "verdict": "VERIFIED-NOT-SEALED" if not probs else "NOTREADY",
        "seal": "NOT-SEALED (seal is Root/W10 phase; this verifier never seals)",
        "source_go_sql_sha256": want_s,
        "manifest": membership,
        "secret_scan": {"suspects": len(suspects), "actual_value_hits": actual_hits},
        "items": items_out,
        "pending_closed_by_root": acceptance,
        "problems": probs,
    }
    if emit and os.path.realpath(emit).startswith(os.path.realpath(pkg_root) + os.sep):
        raise Refusal("report must be outside immutable package")
    if emit:
        with open(emit, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=1, sort_keys=True)
            fh.write("\n")
    return report["verdict"], probs, report


# ----------------------------------------------------------------- selftest
def _mini_pkg(tmp, files):
    d = tempfile.mkdtemp(prefix="p26_verify_selftest_", dir=tmp)
    for rel, content in files.items():
        p = os.path.join(d, *rel.split("/"))
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as fh:
            fh.write(content if isinstance(content, bytes) else content.encode())
    return d


def _expected_stream(files, label):
    """Independent in-test literal expectation of the S recipe."""
    parts = [b"go.mod\n", files["source/go.mod"].encode(),
             b"go.sum\n", files["source/go.sum"].encode(), b"content-hashes\n"]
    for rel in sorted(r for r in files if r.startswith(("source/services/", "source/libs/"))):
        parts.append(f"{hashlib.sha256(files[rel].encode()).hexdigest()}  {label}/{rel[len('source/'):]}\n".encode())
    return hashlib.sha256(b"".join(parts)).hexdigest()


def hardened_selftest():
    checks = []
    def check(name, condition):
        checks.append((name, bool(condition)))
    with tempfile.TemporaryDirectory() as tmp:
        files = {"source/go.mod": "module x", "source/go.sum": "sum",
                 **{"source/" + r + "/x" + exts[0]: "code" for r, exts in SOURCE_ROOTS}}
        label = "/nonexistent/canonical-label"
        d = _mini_pkg(tmp, files)
        roster = {"source": {"go_sql_sha256": candidate_hash_from_snapshot(d, label)},
                  "items": [{"id": "one", "status": "accepted", "satisfiers": []}]}
        def write(rel, value):
            p = os.path.join(d, rel); os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "w") as f: f.write(value)
        write("roster/p26_formal_roster.json", json.dumps(roster))
        write("evidence/evidence.json", '{"steps": []}')
        def manifest():
            entries = compute_manifest_entries(d)
            blob = "".join(f"{e['sha256']}  {e['size']}  {r}\n" for r,e in sorted(entries.items())).encode()
            write("manifest.json", json.dumps({"format": MANIFEST_FORMAT, "file_count": len(entries), "manifest_sha256": hashlib.sha256(blob).hexdigest(), "files": [dict(path=r, **e) for r,e in sorted(entries.items())]}))
        check("missing-manifest", verify(d,label)[0] == "NOTREADY")
        manifest()
        check("empty-satisfiers", any("nonempty" in x for x in verify(d,label)[1]))
        check("external-roster", verify(d,label, "/etc/passwd")[0] == "NOTREADY")
        roster['items'][0]['status'] = 'pending'
        write("roster/p26_formal_roster.json", json.dumps(roster))
        write("root-acceptance.json", '{"roster_complete": true, "accepted": ["one"]}')
        manifest()
        check("pending-ID-only", any("does not close" in x for x in verify(d,label)[1]))
        write("original.rc", "1")
        try: verify_original(d, {"artifact":"original.rc", "rc_artifact":"original.rc", "rc":0}, "x")
        except Refusal: check("lying-rc-original", True)
        else: check("lying-rc-original", False)
        for suffix in ('.log','.body','.bin'):
            write('fake' + suffix, 'CONTROLregisteredvalue')
        check("actual-values-all-bytes", secret_suspects(d, ['CONTROLregisteredvalue'])[1] == 3)
        write('query.log', 'https://x/?sig=123456789abcdef')
        check("query-sig", any('presigned-sig' in x for x in secret_suspects(d)[0]))
        write('.env.private', 'example')
        check("private-env", any('private-env-file' in x for x in secret_suspects(d)[0]))
        os.remove(os.path.join(d,'.env.private'))
        manifest(); write('extra.txt','extra')
        check("manifest-extra", verify(d,label)[0] == 'NOTREADY')
        os.remove(os.path.join(d,'extra.txt')); manifest(); write('original.rc','2')
        check("manifest-content-change", verify(d,label)[0] == 'NOTREADY')
        os.symlink('/etc/passwd', os.path.join(d,'escape'))
        check("manifest-symlink", verify(d,label)[0] == 'NOTREADY')
    for n, ok in checks: print(f"[selftest:verifier] {'PASS' if ok else 'FAIL'} {n}")
    print(f"[selftest:verifier] controls={len(checks)} failed={sum(not ok for _,ok in checks)}")
    return int(any(not ok for _,ok in checks))


def selftest():
    return hardened_selftest()


def main():
    sys.dont_write_bytecode = True
    ap = argparse.ArgumentParser(prog="p26_archive_verifier")
    sub = ap.add_subparsers(dest="cmd", required=True)
    vp = sub.add_parser("verify")
    vp.add_argument("pkg_dir")
    vp.add_argument("--canonical-root-label", required=True)
    vp.add_argument("--roster")
    vp.add_argument("--emit-report-skeleton")
    vp.add_argument("--values-stdin", action="store_true")
    sub.add_parser("selftest")
    args = ap.parse_args()
    if args.cmd == "selftest":
        return selftest()
    values = None
    if args.values_stdin:
        values = [ln for ln in sys.stdin.read().splitlines() if ln]
    if not os.path.realpath(__file__).startswith(os.path.realpath(args.pkg_dir) + os.sep):
        print(json.dumps({"verdict": "NOTREADY", "problems": ["verifier must execute from copied package"]}))
        return 1
    verdict, probs, report = verify(args.pkg_dir, args.canonical_root_label,
                                    roster_path=args.roster,
                                    emit=args.emit_report_skeleton,
                                    actual_values=values)
    print(json.dumps({"verdict": verdict, "problems": probs}, indent=1))
    return 0 if verdict == "VERIFIED-NOT-SEALED" else 1


if __name__ == "__main__":
    sys.exit(main())
