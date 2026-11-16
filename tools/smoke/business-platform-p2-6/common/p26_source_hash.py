#!/usr/bin/env python3
"""Thin CLI for the P25-parity source content hash (W6).

Byte-equal recipe to candidate_source_gate_p25.py:candidate_stream
(proven by selftest parity against the actual P25 module and by the
read-only real-repo comparison recorded in results/w6/).

Usage:
  python3 -I -B p26_source_hash.py --repo <root> [--expect <sha>]
Prints CANDIDATE_SOURCE_SHA256=…; with --expect rc0 iff equal.
"""

import argparse
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

import p26_gates as G  # noqa: E402


def main():
    ap = argparse.ArgumentParser(prog="p26_source_hash")
    ap.add_argument("--repo", required=True)
    ap.add_argument("--expect")
    args = ap.parse_args()
    try:
        got = G.candidate_hash(Path(args.repo))
    except G.GateRefusal as e:
        print(f"[p26-source-hash] REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"CANDIDATE_SOURCE_SHA256={got}")
    if args.expect:
        ok = got == args.expect
        print(f"CANDIDATE_VERIFY={'OK' if ok else 'FAILED'} "
              f"(expect {args.expect})")
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
