#!/usr/bin/env python3
"""OFFLINE VERIFIER for a P25 standalone bundle (self-contained; runs
with `python3 -I offline_verify_p25.py --bundle DIR` from ANYWHERE —
no sibling imports, no network, no DB).

Verifies, using ONLY the bundle's own bytes:
  1. the bundle is non-empty and carries BOTH required control files
     (standalone-meta.json + bundle-hash-manifest.json) — missing or
     empty bundles are REJECTED;
  2. bundle-hash-manifest.json covers EVERY other bundle file
     bidirectionally and every recorded sha256 matches a fresh read
     (the bundle's NEW seal — recomputed here, not trusted);
  3. standalone-meta.json internal consistency: per-file copy sha ==
     actual bytes; fullindex == on-disk set; the offline verifier
     itself is byte-exact (never redacted);
  4. every logged redaction site's line actually CONTAINS its
     <REDACTED:reason> token in the copy (the sanctioned transform is
     visible exactly where it was logged);
  5. NO SURVIVING SECRETS: none of the frozen secret patterns matches
     anywhere in the bundle (excluding the two control files' hash
     lines — patterns are compiled from the same literals as
     standalone_p25.py; selftest cross-checks them for drift).

rc 0 iff every gate holds. Tampered/deleted/empty bundles fail.
"""

import argparse
import hashlib
import json
import re
import sys
sys.dont_write_bytecode = True  # Pure validation must not mutate copied evidence/tools.
from pathlib import Path

# MUST stay identical to standalone_p25.py PATTERNS (the bundle
# selftest asserts equality — drift is a build error, not a pass).
PATTERNS = [
    ("dsn", r"postgres(?:ql)?://[^\s\"']+"),
    ("bearer", r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}"),
    ("jwt", r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"
            r"\.[A-Za-z0-9_\-]{10,}"),
    ("env-secret-assign",
     r"(?i)^(OF_P1_PG_PASSWORD|OF_P1_JWT_SECRET|OF_P1_CREDENTIAL_KEY"
     r"|OF_P1_SERVICE_JWT)\s*=.*$"),
]
_COMPILED = [(n, re.compile(p)) for n, p in PATTERNS]
CONTROL = ("standalone-meta.json", "bundle-hash-manifest.json")


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def verify_semantics(bundle: Path, meta: dict) -> dict:
    """Reuse canonical finalizer validators with explicit in-bundle path rebinding.

    No evidence JSON is edited. Original/copy SHA bindings account for the
    already logged sanctioned redactions. Subprocesses are restricted to
    the pure saved-log checker and source file enumeration.
    """
    import importlib.util
    import pathlib
    import subprocess
    native_path, native_run = pathlib.Path, subprocess.run
    bundle = native_path(bundle).resolve()
    mapping = meta.get("path_maps") or {}
    problems = []
    if not mapping or not meta.get("source_repo"):
        return {"ok": False, "problems": ["semantic dependency path maps missing"]}
    for original, relative in mapping.items():
        dest = (bundle / relative).resolve()
        if not native_path(original).is_absolute() or not dest.is_relative_to(bundle) or not dest.is_dir():
            return {"ok": False, "problems": ["invalid/missing semantic dependency root"]}
    mappings = sorted(mapping.items(), key=lambda x: len(x[0]), reverse=True)

    def rebound(value):
        text = str(value)
        if native_path(text).is_absolute():
            if native_path(text).is_relative_to(bundle):
                return text
            for original, relative in mappings:
                if text == original or text.startswith(original + "/"):
                    return str(bundle / relative) + text[len(original):]
            # This is only a logical command argument, never opened by validators.
            if text == "/private/tmp/openfoundry-platform-p1-q3v9" or text.startswith("/private/tmp/openfoundry-platform-p1-q3v9/"):
                return text
            raise ValueError(f"offline read outside bound dependency roots: {text}")
        return text

    class BundlePath:
        def __new__(cls, *parts):
            return native_path(rebound(native_path(*parts)))

    def pure_run(argv, *args, **kwargs):
        argv = list(argv)
        allowed_checker = len(argv) > 2 and argv[1] == "-I" and native_path(argv[2]).name == "pg_exact_check_p25.py"
        allowed_find = argv and argv[0] == "find"
        if not (allowed_checker or allowed_find):
            raise ValueError(f"offline subprocess is not a pure allowed verifier: {argv[0]}")
        argv = [rebound(a) if i > 0 and isinstance(a, str) and a.startswith("/") else a for i, a in enumerate(argv)]
        return native_run(argv, *args, **kwargs)

    original_records = {str(bundle / rel): rec for rel, rec in meta["files"].items()}
    try:
        pathlib.Path = BundlePath
        subprocess.run = pure_run
        fp = bundle / "tools/finalize_p25.py"
        spec = importlib.util.spec_from_file_location("p25_bundle_finalizer", fp)
        finalizer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(finalizer)
        finalizer.REPO_ROOT = native_path(rebound(meta["source_repo"]))
        finalizer.HERE = finalizer.REPO_ROOT / "tools/smoke/business-platform-p2-5"
        finalizer.PREFLIGHT_BASE = native_path(rebound(meta["source_repo"] + "/tools/smoke/business-platform-p2-5/results/2026-11-14-p25-preflight/attempt-2"))
        finalizer.PREFLIGHT_ACCEPTED = native_path(rebound("/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-14-p25/preflight-accepted.json"))
        original_sha = finalizer.sha
        def consumed_sha(p):
            actual = original_sha(p)
            rec = original_records.get(str(p))
            return rec["original"] if rec and rec.get("copy") == actual else actual
        finalizer.sha = consumed_sha
        def source_hash(root):
            parts = []
            for name in ("go.mod", "go.sum"):
                parts += [name.encode() + b"\n", (root / name).read_bytes()]
            parts.append(b"content-hashes\n")
            files = sorted(q for rel in finalizer.SOURCE_DIRS for q in (root / rel).rglob("*") if q.is_file() and q.suffix in (".go", ".sql"))
            for q in files:
                recorded_path = meta["source_repo"].rstrip("/") + "/" + str(q.relative_to(root))
                parts.append(f"{consumed_sha(q)}  {recorded_path}\n".encode())
            return hashlib.sha256(b"".join(parts)).hexdigest()
        finalizer.source_tree_hash = source_hash
        ev = bundle / "ev"
        selected, errors = finalizer.load_selected_index(ev, [], deferred_stages=("standalone",) if meta.get("scope") == "preseal-capability" else ())
        problems.extend(errors)
        stage_status = {}
        ctx = {"ev": ev, "evidence_root": finalizer.EVIDENCE_ROOT,
               "preflight_base": finalizer.PREFLIGHT_BASE,
               "preflight_accepted": finalizer.PREFLIGHT_ACCEPTED,
               "repo_root": finalizer.REPO_ROOT,
               "historical_suffixes": finalizer._historical_suffixes(),
               "original_records": original_records, "logical_path_class": native_path}
        if selected is not None:
            for stage, validator in finalizer.CANONICAL_STAGES.items():
                if stage == "standalone":
                    # The enclosing verifier already checks this bundle's seal,
                    # copy index/redaction ledger and no-secret rules. Recursing
                    # into another standalone verifier would add no evidence.
                    stage_status[stage] = "bundle-structure-verified"
                    continue
                if stage not in selected:
                    problems.append(f"semantic stage missing: {stage}")
                    continue
                try:
                    reasons, _ = validator(ev / selected[stage]["attempt_dir"], ctx)
                except Exception as exc:
                    reasons = [f"{type(exc).__name__}: {exc}"]
                stage_status[stage] = "refused" if reasons else "ok"
                problems.extend(f"{stage}: {r}" for r in reasons)
        else:
            problems.append("semantic selected index unavailable")
        if meta.get("scope") == "sealed-final":
            hm = ev / "hash-manifest.json"
            try:
                seal = json.loads(hm.read_text())
                disk = {str(q.relative_to(ev)) for q in ev.rglob("*") if q.is_file() and q != hm}
                if set(seal.get("files") or {}) != disk:
                    problems.append("final EV root seal not bidirectional")
                tools_record = json.loads((ev / "tools-sha.json").read_text())
                if set(finalizer.tools_tree(finalizer.HERE)) != set(tools_record.get("files") or {}):
                    problems.append("final source tools map not bidirectional")
                for rel, digest in (tools_record.get("files") or {}).items():
                    if consumed_sha(finalizer.HERE / rel) != digest:
                        problems.append(f"final source tool original/copy SHA mismatch: {rel}")
                for rel, digest in (seal.get("files") or {}).items():
                    if consumed_sha(ev / rel) != digest:
                        problems.append(f"final EV root seal original/copy mapping mismatch: {rel}")
            except (OSError, ValueError) as exc:
                problems.append(f"final EV root seal unavailable: {exc}")
        elif meta.get("scope") != "preseal-capability":
            problems.append("standalone lifecycle scope missing/invalid")
        old_errors, _ = finalizer.old_archive_reverify()
        problems.extend(old_errors)
        return {"ok": not problems, "problems": problems, "stage_status": stage_status}
    except Exception as exc:
        return {"ok": False, "problems": [f"semantic replay failed: {type(exc).__name__}: {exc}"]}
    finally:
        pathlib.Path, subprocess.run = native_path, native_run


def verify_bundle(bundle: Path) -> dict:
    problems = []
    if not bundle.is_dir():
        return {"ok": False, "problems": [f"bundle dir missing: "
                                          f"{bundle}"]}
    disk_files = {str(p.relative_to(bundle))
                  for p in bundle.rglob("*") if p.is_file()}
    if not disk_files:
        return {"ok": False, "problems": ["bundle is EMPTY"]}
    for c in CONTROL:
        if c not in disk_files:
            problems.append(f"required control file missing: {c}")
    if problems:
        return {"ok": False, "problems": problems}
    # control files must be non-empty JSON
    for c in CONTROL:
        try:
            json.loads((bundle / c).read_text())
        except (ValueError, OSError) as exc:
            problems.append(f"{c} unreadable/empty: {exc}")

    # 2) NEW seal recomputation — bidirectional + fresh hashing
    try:
        manifest = json.loads(
            (bundle / "bundle-hash-manifest.json").read_text())
    except ValueError:
        return {"ok": False, "problems": problems + [
            "bundle-hash-manifest.json not JSON"]}
    recorded = set(manifest.get("files") or {})
    expect = disk_files - {"bundle-hash-manifest.json"}
    if recorded != expect:
        problems.append(f"bundle seal NOT bidirectional (manifest-only "
                        f"{sorted(recorded - expect)[:4]}, disk-only "
                        f"{sorted(expect - recorded)[:4]})")
    for rel in sorted(recorded & expect):
        if sha(bundle / rel) != manifest["files"][rel]:
            problems.append(f"bundle seal sha mismatch: {rel}")

    # 3) meta consistency
    try:
        meta = json.loads((bundle / "standalone-meta.json").read_text())
    except ValueError:
        return {"ok": False, "problems": problems + [
            "standalone-meta.json not JSON"]}
    files = meta.get("files") or {}
    if not files:
        problems.append("standalone-meta file index EMPTY — empty "
                        "evidence never passes")
    copy_disk = disk_files - set(CONTROL)
    ev_disk = {r[len("ev/"):] for r in disk_files if r.startswith("ev/")}
    if set(meta.get("ev_source_files") or []) != ev_disk:
        problems.append("independent EV source enumeration differs from bundle")
    meta_only = sorted(set(files) - copy_disk)[:4]
    disk_only = sorted(copy_disk - set(files))[:4]
    if set(files) != copy_disk:
        problems.append("meta fullindex != bundle contents "
                        f"(meta-only {meta_only}, disk-only "
                        f"{disk_only})")
    for rel, rec in sorted(files.items()):
        p = bundle / rel
        if not p.is_file():
            continue
        if sha(p) != rec.get("copy"):
            problems.append(f"meta copy sha drift: {rel}")
    v = "offline_verify_p25.py"
    if v in files and (files[v].get("changed") is True
                       or files[v].get("sites")):
        problems.append("offline verifier itself was modified by "
                        "redaction — bundle is not self-verifying")

    # 4) logged sites carry their token in the copy
    for site in meta.get("redaction_sites") or []:
        rel, ln = site.get("file"), site.get("line")
        p = bundle / str(rel)
        if not p.is_file():
            problems.append(f"site file missing: {rel}")
            continue
        lines = p.read_text(errors="replace").split("\n")
        if not isinstance(ln, int) or ln < 1 or ln > len(lines):
            problems.append(f"site {rel}:{ln} out of range")
            continue
        if f"<REDACTED:{site.get('reason')}>" not in lines[ln - 1]:
            problems.append(f"site {rel}:{ln} lacks its "
                            f"<REDACTED:{site.get('reason')}> token")

    # 5) no surviving secrets anywhere (control files carry only
    #    hashes/meta; scan them too EXCEPT pattern-source lines are
    #    none — they contain shas and paths only)
    for rel in sorted(disk_files):
        p = bundle / rel
        try:
            text = p.read_text(errors="ignore")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            for name, pat in _COMPILED:
                if pat.search(line):
                    problems.append(f"SURVIVING SECRET ({name}) at "
                                    f"{rel}:{i}")
    semantic = verify_semantics(bundle, meta) if not problems else {"ok": False, "problems": []}
    problems += semantic["problems"]
    return {"ok": not problems, "problems": problems,
            "semantic_stage_status": semantic.get("stage_status", {}),
            "files": len(disk_files),
            "redaction_sites": len(meta.get("redaction_sites") or [])}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", required=True)
    args = ap.parse_args()
    r = verify_bundle(Path(args.bundle))
    print(json.dumps(r, indent=1, sort_keys=True))
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
