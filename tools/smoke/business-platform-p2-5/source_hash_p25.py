#!/usr/bin/env python3
"""P2.5 candidate business SOURCE CONTENT hash + baseline tools.

Same deterministic algorithm as the frozen source_hash_p24.py (sha256
over the CONTENT of every *.go/*.sql under the three business services
+ libs/scheduling-cron, plus root go.mod/go.sum; sorted by path) — the
file-set semantics are asserted identical in the selftests.

Modes:
  emit  <repo-root> <out-file>
        Write one "sha256  repo-relative-path" line per file (sorted),
        print SOURCE_FILES= / SOURCE_HASH= (combined = sha256 of the
        sorted line list, P24-compatible).
  verify --repo <root> --baseline <txt> [--expect-combined <sha>]
        rc0 iff the current tree's line-set EXACTLY equals the baseline
        (no missing/changed/extra files) and, when provided, the
        combined hash equals --expect-combined. Any difference lists
        missing/changed/extra explicitly — never silently empty.
  delta  --repo <root> --baseline <txt> --out <json>
        Write {changed, added, removed, counts} vs the baseline. A
        non-empty `removed` set exits 1 (a file disappearing from the
        candidate source is an anomaly to adjudicate, per design §7.2).

Usage: python3 -I source_hash_p25.py <mode> …
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

# Identical to frozen source_hash_p24.py (asserted in selftests).
ROOTS = [("services/connector-management-service", (".go", ".sql")),
         ("services/dataset-versioning-service", (".go", ".sql")),
         ("services/pipeline-build-service", (".go", ".sql")),
         ("libs/scheduling-cron", (".go",))]
EXTRA = ["go.mod", "go.sum"]


def collect(root: Path):
    files = []
    for rel, exts in ROOTS:
        base = root / rel
        if not base.is_dir():
            print(f"source-hash: missing dir {rel}", file=sys.stderr)
            sys.exit(1)
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in exts:
                files.append(p)
    for rel in EXTRA:
        p = root / rel
        if not p.is_file():
            print(f"source-hash: missing {rel}", file=sys.stderr)
            sys.exit(1)
        files.append(p)
    lines = []
    for p in sorted(files):
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        lines.append(f"{digest}  {p.relative_to(root)}")
    return lines


def combined_of(lines) -> str:
    return hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest()


def emit(args):
    root = Path(args.repo).resolve()
    lines = collect(root)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n")
    print(f"SOURCE_FILES={len(lines)}")
    print(f"SOURCE_HASH={combined_of(lines)}")
    return 0


def load_baseline(path: Path):
    entries = {}
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        sha, _, rel = raw.partition("  ")
        if len(sha) != 64 or not rel:
            print(f"source-hash: malformed baseline line: {raw[:80]!r}",
                  file=sys.stderr)
            sys.exit(1)
        entries[rel] = sha
    return entries


def diff_against_baseline(root: Path, baseline: Path):
    base = load_baseline(baseline)
    cur = {}
    for line in collect(root):
        sha, _, rel = line.partition("  ")
        cur[rel] = sha
    changed = sorted(r for r in base if r in cur and base[r] != cur[r])
    missing = sorted(r for r in base if r not in cur)   # deleted/moved away
    added = sorted(r for r in cur if r not in base)
    return {"changed": changed, "removed": missing, "added": added,
            "baseline_files": len(base), "current_files": len(cur)}


def verify(args):
    root = Path(args.repo).resolve()
    d = diff_against_baseline(root, Path(args.baseline))
    ok = True
    if d["changed"]:
        ok = False
        print(f"source-hash verify: {len(d['changed'])} CHANGED file(s): "
              f"{d['changed'][:5]}…", file=sys.stderr)
    if d["removed"]:
        ok = False
        print(f"source-hash verify: {len(d['removed'])} REMOVED file(s): "
              f"{d['removed'][:5]}…", file=sys.stderr)
    if d["added"]:
        ok = False
        print(f"source-hash verify: {len(d['added'])} ADDED file(s): "
              f"{d['added'][:5]}…", file=sys.stderr)
    combined = combined_of(collect(root))
    if args.expect_combined and combined != args.expect_combined:
        ok = False
        print(f"source-hash verify: combined {combined} != expected "
              f"{args.expect_combined}", file=sys.stderr)
    print(f"SOURCE_FILES={d['current_files']} "
          f"BASELINE_FILES={d['baseline_files']} "
          f"CHANGED={len(d['changed'])} REMOVED={len(d['removed'])} "
          f"ADDED={len(d['added'])}")
    print(f"SOURCE_HASH={combined}")
    print(f"VERIFY={'OK' if ok else 'FAILED'}")
    return 0 if ok else 1


def delta(args):
    root = Path(args.repo).resolve()
    d = diff_against_baseline(root, Path(args.baseline))
    Path(args.out).write_text(json.dumps(d, indent=1, sort_keys=False) + "\n")
    print(f"DELTA changed={len(d['changed'])} removed={len(d['removed'])} "
          f"added={len(d['added'])}")
    return 1 if d["removed"] else 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    p = sub.add_parser("emit")
    p.add_argument("repo")
    p.add_argument("out")
    p.set_defaults(fn=emit)
    p = sub.add_parser("verify")
    p.add_argument("--repo", required=True)
    p.add_argument("--baseline", required=True)
    p.add_argument("--expect-combined")
    p.set_defaults(fn=verify)
    p = sub.add_parser("delta")
    p.add_argument("--repo", required=True)
    p.add_argument("--baseline", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=delta)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
