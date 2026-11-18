#!/usr/bin/env python3
"""Working-tree fingerprint: HEAD + per-path blob hashes of every changed
or untracked file. Used at BUILD START (default) and at SUITE END (--mode
end) — the review requires both to agree, otherwise the round is invalidated
(parallel sessions moved the tree mid-run)."""

import hashlib
import json
import os
import subprocess
import sys


def main():
    repo, ev = sys.argv[1], sys.argv[2]
    mode = "start"
    if sys.argv[3:4] == ["--mode"]:
        mode = sys.argv[4] if len(sys.argv) > 4 else "start"
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo,
                          capture_output=True, text=True).stdout.strip()
    branch = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo,
                            capture_output=True, text=True).stdout.strip()
    # --untracked-files=all: a whole untracked directory would otherwise
    # collapse into ONE entry whose hash is null (the "path" is a dir).
    # --no-renames: keeps every entry a single path, so the NUL parse below
    # never has to deal with rename's two-path records.
    porc = subprocess.run(
        ["git", "status", "--porcelain", "-z", "--untracked-files=all", "--no-renames"],
        cwd=repo, capture_output=True, text=True).stdout
    entries = []
    for token in porc.split("\0"):
        if not token:
            continue
        status, path = token[:2], token[3:]
        full = os.path.join(repo, path)
        digest = None
        if os.path.isfile(full):
            digest = hashlib.sha256(open(full, "rb").read()).hexdigest()
        entries.append({"status": status.strip(), "path": path, "sha256": digest})
    nulls = [e["path"] for e in entries if e["sha256"] is None]
    if nulls:
        raise SystemExit(f"fingerprint[{mode}]: null hashes for {nulls[:5]} "
                         f"({len(nulls)} total) — untracked dirs must be expanded")
    # Self-check (review): every source file of THIS harness directory (the
    # suite's own deliverables, excluding the ignored results/ archive) must
    # appear in the manifest with an individual hash.
    here = os.path.join(repo, "tools/smoke/data-integration-wp3-3")
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
