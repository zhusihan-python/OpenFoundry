#!/usr/bin/env python3
"""P25 STANDALONE sanitizer (review-archive-tools-round2 #3 closure).

Byte-preserving sanitized copy + independent replay + sealed bundle:

  * BYTES are preserved unless an EXPLICIT sanctioned redaction span
    changes them: binaries exact; text lines that decode as UTF-8 are
    redacted per pattern and re-encoded; undecodable lines are copied
    VERBATIM (never a UTF-8 replacement byte); CRLF / missing trailing
    newline survive (split/join on b"\\n" is identity for unchanged
    bytes).
  * dst is an IMMUTABLE FRESH destination — an existing non-empty dst
    is refused, never overwritten. Missing/empty src is refused.
  * full index is BIDIRECTIONAL: the set of copied files must equal
    the src file set both ways; every file records original+copy sha.
  * replay RE-READS both sides and independently recomputes the
    sanctioned transform from the ORIGINAL bytes, comparing EXACTLY to
    the copy — any unapproved same-line change, vanished site, forged
    meta sha or side mismatch fails.
  * bundle: sanitized EV copy + tools + frozen dependencies + the
    self-contained offline verifier, sealed with a NEW bundle hash
    manifest (the source-tree seal does not survive changed bytes).
    `verify-bundle` runs offline_verify_p25.py against the bundle.

Offline verification (no original present) lives in
offline_verify_p25.py: bundle seal bidirectional readback, meta
consistency, redaction tokens present at logged sites, and a
NO-SURVIVING-SECRETS scan of the whole bundle.
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
sys.dont_write_bytecode = True  # Pure validation must not mutate copied evidence/tools.
import time
from pathlib import Path

PATTERNS = [
    ("dsn", r"postgres(?:ql)?://[^\s\"']+"),
    ("bearer", r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}"),
    ("jwt", r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"
            r"\.[A-Za-z0-9_\-]{10,}"),
    ("env-secret-assign",
     r"(?i)^(OF_P1_PG_PASSWORD|OF_P1_JWT_SECRET|OF_P1_CREDENTIAL_KEY"
     r"|OF_P1_SERVICE_JWT)\s*=.*$"),
]

import re                                          # noqa: E402

TEXT_SUFFIXES = (".json", ".log", ".txt", ".md", ".rc", ".err",
                 ".cmd", ".stdout", ".stderr", ".url", ".kind",
                 ".wrapper", ".derived", ".py", ".sh", ".yaml", ".yml")

TOOLS_EXCLUDE_DIRS = {"__pycache__", "results", "build"}


def _compile_patterns():
    return [(name, re.compile(pat)) for name, pat in PATTERNS]


def redact_text_line(line: str, compiled=None):
    """One sanctioned line (text-mode helper kept for readability/
    tests): returns (new_line, [reasons])."""
    compiled = compiled or _compile_patterns()
    reasons = []
    out = line
    for name, pat in compiled:
        def repl(_m, _n=name):
            reasons.append(_n)
            return f"<REDACTED:{_n}>"
        out = pat.sub(repl, out)
    return out, reasons


def _compile_byte_patterns():
    """ASCII patterns compiled on BYTES so redaction works line-by-
    line WITHOUT decoding — an undecodable line's ASCII secret span is
    still redacted while every other byte (invalid sequences, CRLF,
    missing trailing newline) survives verbatim. UTF-8 replacement
    bytes are NEVER introduced."""
    return [(name, re.compile(pat.encode("ascii")))
            for name, pat in PATTERNS]


def redact_bytes(raw: bytes):
    """Byte-preserving redaction. Only matched spans change; every
    other byte — including invalid UTF-8 sequences, CRLF and a
    missing trailing newline — is byte-identical."""
    compiled = _compile_byte_patterns()
    sites = []
    lines = raw.split(b"\n")
    for i, line in enumerate(lines):
        new_line, reasons = redact_byte_line(line, compiled)
        if reasons:
            sites.extend([{"line": i + 1, "reason": r} for r in reasons])
            lines[i] = new_line
    return b"\n".join(lines), sites


def redact_byte_line(line: bytes, compiled=None):
    compiled = compiled or _compile_byte_patterns()
    reasons = []
    out = line
    for name, pat in compiled:
        def repl(_m, _n=name):
            reasons.append(_n)
            return f"<REDACTED:{_n}>".encode("ascii")
        out = pat.sub(repl, out)
    return out, reasons


def _require_fresh(dst: Path):
    if dst.exists() and any(dst.iterdir()):
        raise ValueError(f"standalone dst not fresh (exists non-empty): "
                         f"{dst} — immutable destination, never "
                         "overwritten")


def _require_nonempty_src(src: Path):
    if not src.is_dir() or not any(p.is_file() for p in src.rglob("*")):
        raise ValueError(f"standalone src missing/empty: {src}")


def sanitize_tree(src: Path, dst: Path, prefix: str = "",
                  full: bool = False) -> dict:
    """Shared copy engine. Tools tree (`full=False`): caches/results/
    build excluded. EV tree (`full=True`): COMPLETE enumeration —
    nothing excluded except __pycache__/*.pyc (an EV must not silently
    drop evidence dirs). Returns per-file record map."""
    recs = {}
    for p in sorted(src.rglob("*")):
        if p.is_dir():
            continue
        rel = p.relative_to(src)
        if not full and p.suffix == ".pyc":
            continue
        if not full and rel.parts and \
                rel.parts[0] in TOOLS_EXCLUDE_DIRS:
            continue
        raw = p.read_bytes()
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw.decode("utf-8")
            text = True
        except UnicodeDecodeError:
            text = False
        if text:
            new_raw, sites = redact_bytes(raw)
        else:
            new_raw, sites = raw, []
        target.write_bytes(new_raw)
        recs[f"{prefix}{rel}"] = {
            "original_bytes": len(raw), "copy_bytes": len(new_raw),
            "original": hashlib.sha256(raw).hexdigest(),
            "copy": hashlib.sha256(new_raw).hexdigest(),
            "changed": new_raw != raw, "text": text, "sites": sites}
    return recs


def sanitize_dir(src: Path, dst: Path) -> dict:
    """Byte-preserving sanitized copy of an evidence dir (v2)."""
    src, dst = Path(src), Path(dst)
    _require_nonempty_src(src)
    _require_fresh(dst)
    dst.mkdir(parents=True, exist_ok=True)
    recs = sanitize_tree(src, dst)
    src_index = sorted(r for r in recs)
    meta = {"schema": "p25-standalone-v2",
            "src_root": str(src),
            "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "files": recs,
            "redaction_sites": [
                {"file": rel, **s}
                for rel, r in sorted(recs.items())
                for s in r["sites"]],
            "fullindex": {"src": src_index,
                          "copy": sorted(r for r in recs)}}
    return meta


def replay_compare(src: Path, dst: Path, standalone_meta: dict) -> dict:
    """Independent EXACT replay: re-READ original+copy bytes, recompute
    the sanctioned transform from the original, and require the copy to
    equal it byte-for-byte. Unapproved same-line changes, vanished
    sites, forged shas and missing sides all fail."""
    reasons = []
    src, dst = Path(src), Path(dst)
    if not src.is_dir() or not any(p.is_file() for p in src.rglob("*")):
        return {"ok": False, "reasons": [f"replay src missing/empty: "
                                         f"{src}"]}
    files = standalone_meta.get("files") or {}
    if not files:
        return {"ok": False, "reasons": ["meta has an EMPTY file index "
                                         "— empty evidence never passes"]}
    # bidirectional full index vs what is actually on disk
    meta_set = set(files)
    source_set = {str(p.relative_to(src)) for p in src.rglob("*") if p.is_file()}
    if source_set != meta_set:
        reasons.append("fullindex source enumeration differs from meta (source omission)")
    copy_set = {str(p.relative_to(dst)) for p in dst.rglob("*") if p.is_file()}
    copy_set -= {name for name in ("standalone-meta.json", "bundle-hash-manifest.json") if name not in meta_set}
    if copy_set != meta_set:
        only_copy = sorted(copy_set - meta_set)[:5]
        only_meta = sorted(meta_set - copy_set)[:5]
        reasons.append(f"fullindex NOT bidirectional (copy-only "
                       f"{only_copy}, meta-only {only_meta})")
    for rel, rec in sorted(files.items()):
        o, c = src / rel, dst / rel
        if not o.is_file() or not c.is_file():
            reasons.append(f"{rel}: side missing for replay")
            continue
        o_raw, c_raw = o.read_bytes(), c.read_bytes()
        if hashlib.sha256(o_raw).hexdigest() != rec["original"]:
            reasons.append(f"{rel}: ORIGINAL sha drift vs meta (source "
                           "changed after sanitize)")
        if hashlib.sha256(c_raw).hexdigest() != rec["copy"]:
            reasons.append(f"{rel}: copy sha drift vs meta")
        if rec["text"]:
            expect_raw, expect_sites = redact_bytes(o_raw)
            if c_raw != expect_raw:
                reasons.append(f"{rel}: copy != EXACT sanctioned "
                               "transform of the original (unapproved "
                               "change or redaction drift)")
            got_sites = [{"line": s["line"], "reason": s["reason"]}
                         for s in rec["sites"]]
            if got_sites != [{"line": s["line"], "reason": s["reason"]}
                             for s in expect_sites]:
                reasons.append(f"{rel}: logged sites != recomputed "
                               "sites")
        else:
            if c_raw != o_raw:
                reasons.append(f"{rel}: binary copy is not byte-exact")
            if rec["sites"]:
                reasons.append(f"{rel}: binary file claims redaction "
                               "sites")
    return {"ok": not reasons, "reasons": reasons}


def build_bundle(src_ev: Path, tools_dir: Path, bundle_dir: Path) -> dict:
    """Sanitized EV (COMPLETE enumeration — no results/build exclusion
    on evidence) + tools + frozen deps + offline verifier + EXTERNAL
    dependencies (PG r17 / build r18 / preflight-accepted / P24 seal
    pair, read-only copies) under one NEW seal (bundle-hash-manifest
    LAST inside the bundle)."""
    src_ev, tools_dir, bundle_dir = (Path(src_ev), Path(tools_dir),
                                     Path(bundle_dir))
    _require_nonempty_src(src_ev)
    _require_nonempty_src(tools_dir)
    _require_fresh(bundle_dir)
    (bundle_dir / "ev").mkdir(parents=True)
    (bundle_dir / "tools").mkdir(parents=True)
    (bundle_dir / "externals").mkdir(parents=True)
    ev_recs = sanitize_tree(src_ev, bundle_dir / "ev", prefix="ev/",
                            full=True)
    tools_recs = sanitize_tree(tools_dir, bundle_dir / "tools",
                               prefix="tools/")
    verifier_src = Path(__file__).resolve().parent / \
        "offline_verify_p25.py"
    if not verifier_src.is_file():
        raise ValueError("offline_verify_p25.py missing next to "
                         "standalone_p25.py")
    v_raw = verifier_src.read_bytes()
    (bundle_dir / "offline_verify_p25.py").write_bytes(v_raw)
    here = Path(__file__).resolve().parent
    ext_files = {}
    for src, name in (
            (here / "pg-candidate-bindings-r17.json",
             "pg-candidate-bindings-r17.json"),
            (here / "build-candidate-bindings-r18.json",
             "build-candidate-bindings-r18.json"),
            (Path("/Users/allen/.codex/visualizations/2026/11/13/"
                  "01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/"
                  "2026-11-14-p25/preflight-accepted.json"),
             "preflight-accepted.json"),
            (here.parents[0] / "business-platform-p2-4" / "results" /
             "2026-11-14-p24-final" / "hash-manifest.json",
             "p24-hash-manifest.json"),
            (here.parents[0] / "business-platform-p2-4" / "results" /
             "2026-11-14-p24-final" / "16-tools-sha-final.json",
             "p24-16-tools-sha-final.json")):
        if not src.is_file():
            raise ValueError(f"bundle external dependency missing: "
                             f"{src}")
        raw = src.read_bytes()
        (bundle_dir / "externals" / name).write_bytes(raw)
        ext_files[f"externals/{name}"] = {
            "original_bytes": len(raw), "copy_bytes": len(raw),
            "original": hashlib.sha256(raw).hexdigest(),
            "copy": hashlib.sha256(raw).hexdigest(),
            "changed": False, "text": True, "sites": []}
    # Explicit original-prefix -> copy-prefix bindings; retained evidence JSON
    # is byte-preserved apart from logged redactions, never silently rewritten.
    repo = here.parents[2]
    dependencies = {}
    path_maps = {str(src_ev.resolve()): "ev", str(here): "dependencies/repo/tools/smoke/business-platform-p2-5",
                 str(repo): "dependencies/repo"}
    roots = [(Path("/private/tmp/openfoundry-p25-repair-evidence"), "dependencies/evidence"),
             (here / "results/2026-11-14-p25-preflight", "dependencies/preflight")]
    for source, rel in roots:
        if not source.is_dir():
            raise ValueError(f"bundle dependency root missing: {source}")
        target = bundle_dir / rel
        target.mkdir(parents=True)
        dependencies.update(sanitize_tree(source, target, prefix=rel + "/", full=True))
        path_maps[str(source)] = rel
    accepted_root = Path("/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-14-p25")
    accepted_copy = bundle_dir / "dependencies/accepted"
    accepted_copy.mkdir(parents=True)
    # Acceptance is data; no secrets/env files are copied.
    for name in ("preflight-accepted.json", "build-accepted.json"):
        source = accepted_root / name
        target = accepted_copy / name
        target.write_bytes(source.read_bytes())
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        dependencies["dependencies/accepted/" + name] = {"original": digest, "copy": digest, "changed": False, "text": True, "sites": []}
    path_maps[str(accepted_root)] = "dependencies/accepted"
    repo_copy = bundle_dir / "dependencies/repo"
    repo_copy.mkdir(parents=True)
    local_tools = repo_copy / "tools/smoke/business-platform-p2-5"
    local_tools.mkdir(parents=True)
    for rel, rec in tools_recs.items():
        dest = local_tools / rel[len("tools/"):]
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes((bundle_dir / rel).read_bytes())
        dependencies[str(dest.relative_to(bundle_dir))] = dict(rec)
    for name in ("go.mod", "go.sum"):
        source, target = repo / name, repo_copy / name
        target.write_bytes(source.read_bytes())
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        dependencies["dependencies/repo/" + name] = {"original": digest, "copy": digest, "changed": False, "text": True, "sites": []}
    for rel in ("services/connector-management-service", "services/dataset-versioning-service", "services/pipeline-build-service", "libs/scheduling-cron"):
        source, target = repo / rel, repo_copy / rel
        target.mkdir(parents=True)
        for q in sorted(source.rglob("*")):
            if not q.is_file() or q.suffix not in (".go", ".sql"):
                continue
            dest = target / q.relative_to(source)
            dest.parent.mkdir(parents=True, exist_ok=True)
            raw = q.read_bytes()
            sanitized, sites = redact_bytes(raw)
            dest.write_bytes(sanitized)
            dependencies[str(dest.relative_to(bundle_dir))] = {"original": hashlib.sha256(raw).hexdigest(), "copy": hashlib.sha256(sanitized).hexdigest(), "original_bytes": len(raw), "copy_bytes": len(sanitized), "changed": raw != sanitized, "text": True, "sites": sites}
    for name in ("business-platform-p1", "business-platform-p2-1", "business-platform-p2-2", "business-platform-p2-3", "business-platform-p2-4"):
        source, target = here.parent / name, repo_copy / "tools/smoke" / name
        target.mkdir(parents=True)
        dependencies.update(sanitize_tree(source, target, prefix=str(target.relative_to(bundle_dir)) + "/"))
    old_final = here.parent / "business-platform-p2-4/results/2026-11-14-p24-final"
    target = repo_copy / old_final.relative_to(repo)
    target.mkdir(parents=True)
    dependencies.update(sanitize_tree(old_final, target, prefix=str(target.relative_to(bundle_dir)) + "/", full=True))
    files = dict(ev_recs)
    files.update(dependencies)
    for k, v in tools_recs.items():
        if k in files:
            raise ValueError(f"bundle collision on {k}")
        files[k] = v
    files.update(ext_files)
    files["offline_verify_p25.py"] = {
        "original": hashlib.sha256(v_raw).hexdigest(),
        "copy": hashlib.sha256(v_raw).hexdigest(),
        "changed": False, "text": True, "sites": []}
    meta = {"schema": "p25-standalone-bundle-v1",
            "src_ev": str(src_ev), "tools_dir": str(tools_dir),
            "scope": "sealed-final" if (src_ev / "hash-manifest.json").is_file() else "preseal-capability",
            "path_maps": path_maps, "source_repo": str(repo),
            "ev_source_files": sorted(str(p.relative_to(src_ev)) for p in src_ev.rglob("*") if p.is_file()),
            "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "files": files,
            "redaction_sites": [
                {"file": rel, **s} for rel, r in sorted(files.items())
                for s in r["sites"]],
            "fullindex": {"src": sorted(files),
                          "copy": sorted(files)}}
    (bundle_dir / "standalone-meta.json").write_text(json.dumps(
        meta, indent=1, sort_keys=True) + "\n")
    # NEW seal over the sanitized bundle (the source seal does not
    # survive changed bytes): everything except the manifest itself.
    manifest_files = {}
    for p in sorted(bundle_dir.rglob("*")):
        if p.is_file() and p != bundle_dir / "bundle-hash-manifest.json":
            manifest_files[str(p.relative_to(bundle_dir))] = \
                hashlib.sha256(p.read_bytes()).hexdigest()
    (bundle_dir / "bundle-hash-manifest.json").write_text(json.dumps(
        {"client_date": time.strftime("%Y-%m-%d"), "files":
         manifest_files}, indent=1, sort_keys=True) + "\n")
    return meta


def verify_bundle(bundle_dir: Path, src_ev: Path = None) -> dict:
    """Full replay (when the original EV is still available) + the
    self-contained offline verifier run as a REAL CLI subprocess."""
    bundle_dir = Path(bundle_dir)
    reasons = []
    if src_ev is not None:
        full_meta = json.loads((bundle_dir / "standalone-meta.json")
                               .read_text())
        ev_meta = {"files": {k[len("ev/"):]: v for k, v in
                             full_meta.get("files", {}).items()
                             if k.startswith("ev/")}}
        r = replay_compare(Path(src_ev), bundle_dir / "ev", ev_meta)
        if not r["ok"]:
            reasons += [f"replay: {x}" for x in r["reasons"]]
    verifier = bundle_dir / "offline_verify_p25.py"
    if not verifier.is_file():
        return {"ok": False, "reasons": ["offline verifier missing from "
                                         "bundle"]}
    proc = subprocess.run(
        [sys.executable, "-I", str(verifier), "--bundle",
         str(bundle_dir)], capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        reasons.append(f"offline verifier rc={proc.returncode}: "
                       f"{proc.stdout.strip()[:300]} "
                       f"{proc.stderr.strip()[:300]}")
    return {"ok": not reasons, "reasons": reasons,
            "offline_stdout": proc.stdout.strip()[:400]}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    p_s = sub.add_parser("sanitize")
    p_s.add_argument("--src", required=True)
    p_s.add_argument("--dst", required=True)
    p_r = sub.add_parser("reverify")
    p_r.add_argument("--src", required=True)
    p_r.add_argument("--dst", required=True)
    p_b = sub.add_parser("bundle")
    p_b.add_argument("--src-ev", required=True)
    p_b.add_argument("--tools-dir", required=True)
    p_b.add_argument("--bundle-dir", required=True)
    p_v = sub.add_parser("verify-bundle")
    p_v.add_argument("--bundle-dir", required=True)
    p_v.add_argument("--src-ev")
    args = ap.parse_args()
    if args.mode == "sanitize":
        src, dst = Path(args.src), Path(args.dst)
        meta = sanitize_dir(src, dst)
        (dst / "standalone-meta.json").write_text(json.dumps(
            meta, indent=1, sort_keys=True) + "\n")
        r = replay_compare(src, dst, meta)
        print(json.dumps({"ok": r["ok"], "files": len(meta["files"]),
                          "sites": len(meta["redaction_sites"]),
                          "problems": r["reasons"][:3]}))
        return 0 if r["ok"] else 1
    if args.mode == "reverify":
        meta = json.loads((Path(args.dst) / "standalone-meta.json")
                          .read_text())
        r = replay_compare(Path(args.src), Path(args.dst), meta)
        print(json.dumps(r, indent=1, sort_keys=True))
        return 0 if r["ok"] else 1
    if args.mode == "bundle":
        meta = build_bundle(Path(args.src_ev), Path(args.tools_dir),
                            Path(args.bundle_dir))
        r = verify_bundle(Path(args.bundle_dir), Path(args.src_ev))
        print(json.dumps({"ok": r["ok"], "scope": meta["scope"], "files": len(meta["files"]),
                          "bundle_manifest_sha256": hashlib.sha256((Path(args.bundle_dir) / "bundle-hash-manifest.json").read_bytes()).hexdigest(),
                          "problems": r["reasons"][:3]}))
        return 0 if r["ok"] else 1
    r = verify_bundle(Path(args.bundle_dir),
                      Path(args.src_ev) if args.src_ev else None)
    print(json.dumps(r, indent=1, sort_keys=True))
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
