#!/usr/bin/env python3
"""P25 CANDIDATE SOURCE GATE (runtime-track owned, round3).

Reproduces BYTE-EXACTLY the PG-track source_tree_hash stream
(run_pg_isolated_p25.sh): "go.mod\\n"+bytes, "go.sum\\n"+bytes, the
"content-hashes\\n" marker, then one shasum-style
"<sha256>  <absolute-path>" line per *.go/*.sql under the four roots
(sorted by path). The reviewed candidate value a1bff07f…ffd27 comes
from pg-candidate-bindings-r17.json + the PG build source gate and is
re-verified by the driver's fingerprints / swap / deployment-record /
source_freeze phases through THIS tool.

The file-set semantics are IMPORTED read-only from the foundation
source_hash_p25.py (ROOTS) — never duplicated, never co-edited
(foundation belongs to its own track).

Usage:
  python3 -I candidate_source_gate_p25.py --repo <root> [--expect <sha>]
Prints CANDIDATE_SOURCE_SHA256=…; with --expect rc0 iff equal (the
formal gate; a wrong expect is the D-family negative through the REAL
CLI).
"""

import argparse
import hashlib
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _roots():
    spec = importlib.util.spec_from_file_location(
        "foundation_source_hash", HERE / "source_hash_p25.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.ROOTS


def candidate_stream(root: Path) -> bytes:
    root = Path(root).absolute()
    parts = [b"go.mod\n", (root / "go.mod").read_bytes(),
             b"go.sum\n", (root / "go.sum").read_bytes(),
             b"content-hashes\n"]
    paths = []
    for rel, exts in _roots():
        base = root / rel
        if not base.is_dir():
            print(f"candidate-gate: missing dir {rel}", file=sys.stderr)
            sys.exit(1)
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in exts:
                paths.append(str(p))
    for p in sorted(paths):
        digest = hashlib.sha256(Path(p).read_bytes()).hexdigest()
        parts.append(f"{digest}  {p}\n".encode())
    return b"".join(parts)


def candidate_hash(root: Path) -> str:
    return hashlib.sha256(candidate_stream(root)).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--expect",
                    help="reviewed candidate sha256; rc0 iff equal")
    args = ap.parse_args()
    got = candidate_hash(Path(args.repo))
    print(f"CANDIDATE_SOURCE_SHA256={got}")
    if args.expect:
        ok = got == args.expect
        print(f"CANDIDATE_VERIFY={'OK' if ok else 'FAILED'} "
              f"(expect {args.expect})")
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
