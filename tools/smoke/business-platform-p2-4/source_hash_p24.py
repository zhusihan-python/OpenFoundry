#!/usr/bin/env python3
"""P24 candidate business SOURCE CONTENT hash (driver review fix).

Deterministic sha256 over the ACTUAL CONTENT of the candidate business
source — not `git status` text (a dirty path list can stay identical
while a file changes): every *.go / *.sql under the three business
services + the repository-owned cron library, plus root go.mod/go.sum.
Never hashes evidence dirs, secrets or tool outputs.

Outputs:
  <out>            one "sha256  repo-relative-path" line per file,
                   sorted by path
  stdout           SOURCE_FILES=<n>
                   SOURCE_HASH=<sha256 of the sorted line list>
The driver records it BEFORE build and AFTER acceptance; the two must
be equal (candidate == final), otherwise the run is diagnostics only.

Usage: python3 -I source_hash_p24.py <repo-root> <out-file>
"""

import hashlib
import sys
from pathlib import Path

ROOTS = [("services/connector-management-service", (".go", ".sql")),
         ("services/dataset-versioning-service", (".go", ".sql")),
         ("services/pipeline-build-service", (".go", ".sql")),
         ("libs/scheduling-cron", (".go",))]
EXTRA = ["go.mod", "go.sum"]


def main():
    root = Path(sys.argv[1]).resolve()
    out = Path(sys.argv[2])
    files = []
    for rel, exts in ROOTS:
        base = root / rel
        if not base.is_dir():
            print(f"source-hash: missing dir {rel}", file=sys.stderr)
            return 1
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in exts:
                files.append(p)
    for rel in EXTRA:
        p = root / rel
        if not p.is_file():
            print(f"source-hash: missing {rel}", file=sys.stderr)
            return 1
        files.append(p)
    lines = []
    for p in sorted(files):
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        lines.append(f"{digest}  {p.relative_to(root)}")
    text = "\n".join(lines) + "\n"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    combined = hashlib.sha256(text.encode()).hexdigest()
    print(f"SOURCE_FILES={len(lines)}")
    print(f"SOURCE_HASH={combined}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
