#!/usr/bin/env python3
"""Hash HEAD, every changed path, and every harness source at start and end."""

import hashlib
import json
import os
import subprocess
import sys


HARNESS_DIR = "tools/smoke/data-integration-wp3-3"


def main():
    repo, ev = sys.argv[1], sys.argv[2]
    mode = "start"
    if sys.argv[3:4] == ["--mode"]:
        mode = sys.argv[4] if len(sys.argv) > 4 else "start"
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=repo, text=True)

    head = git("rev-parse", "HEAD").strip()
    branch = git("rev-parse", "--abbrev-ref", "HEAD").strip()
    # Clean, tracked harness sources must be hashed even when Git status is empty.
    statuses = {path: "" for path in git("ls-files", "--cached", "-z", "--", HARNESS_DIR).split("\0") if path}
    porcelain = git("status", "--porcelain", "-z", "--untracked-files=all", "--no-renames")
    for token in porcelain.split("\0"):
        if token:
            statuses[token[3:]] = token[:2].strip()

    entries = []
    for path, status in sorted(statuses.items()):
        full = os.path.join(repo, path)
        digest = None
        if os.path.isfile(full):
            with open(full, "rb") as source:
                digest = hashlib.sha256(source.read()).hexdigest()
        entries.append({"status": status, "path": path, "sha256": digest})
    nulls = [e["path"] for e in entries if e["sha256"] is None]
    if nulls:
        raise SystemExit(f"fingerprint[{mode}]: null hashes for {nulls[:5]} "
                         f"({len(nulls)} total) — untracked dirs must be expanded")
    # Self-check (review): every source file of THIS harness directory (the
    # suite's own deliverables, excluding the ignored results/ archive) must
    # appear in the manifest with an individual hash.
    here = os.path.join(repo, HARNESS_DIR)
    manifest_paths = {e["path"] for e in entries}
    on_disk = set()
    for root, dirs, files in os.walk(here):
        dirs[:] = [d for d in dirs if d != "results"]
        for fn in files:
            if fn.endswith(".pyc") or "__pycache__" in root:
                continue
            on_disk.add(os.path.relpath(os.path.join(root, fn), repo))
    missing = sorted(on_disk - manifest_paths)
    if missing:
        raise SystemExit(f"fingerprint[{mode}]: harness sources missing from "
                         f"manifest: {missing}")
    manifest = json.dumps({"head": head, "branch": branch, "entries": entries},
                          sort_keys=True)
    fp = {
        "head": head, "branch": branch, "mode": mode, "paths": len(entries),
        "working_tree_fingerprint": hashlib.sha256(manifest.encode()).hexdigest(),
        "entries": entries,
    }
    out = os.path.join(ev, "fingerprint.json" if mode == "start" else "fingerprint_end.json")
    json.dump(fp, open(out, "w"), indent=1, sort_keys=True)
    print(f"fingerprint[{mode}]: head={head[:12]} paths={len(entries)} "
          f"tree_sha={fp['working_tree_fingerprint'][:16]}… "
          f"(harness sources individually hashed: {len(on_disk)})")


if __name__ == "__main__":
    main()
