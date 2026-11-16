#!/usr/bin/env python3
"""P2.6 W9 — deterministic archive manifest builder/verifier (stdlib only).

Standalone tool for the NEW P26 archive package. It never reads anything
outside the package directory given on the command line, never touches the
original workspace, and never imports/executes any old (P25 or earlier)
tooling — the manifest recipe is a fresh, byte-documented format owned by
W9 (see W9-ARCHIVE-INTERFACE.md).

Format `p26-archive-manifest-v1`: one canonical line per file
    "<sha256>  <size>  <relpath>\\n"
sorted by relpath (code-point order); manifest_sha256 is the SHA-256 of
the concatenated lines. The JSON file additionally carries the file count.
Membership is EXACT when an expected list is supplied: missing and extra
files both fail. Security invariants enforced on build AND verify:
  * relpaths are relative, POSIX-style, no empty/".", "..", "//", leading
    "/" or backslash components — path traversal is refused;
  * symlinks anywhere in the package tree (files or directories) are
    refused — no symlink escape;
  * duplicate relpaths are refused;
  * everything is resolved with os.path.realpath containment checks
    against the package root.

Usage:
  python3 -I -B p26_archive_manifest.py build   <pkg_dir> [manifest.json] [--expect-list <file>]
  python3 -I -B p26_archive_manifest.py verify  <pkg_dir> <manifest.json> [--expect-list <file>]
  python3 -I -B p26_archive_manifest.py selftest

verify rc: 0 ok; 1 mismatch/refusal (with precise reason). build rc: 0 ok;
1 refusal; 2 usage. selftest rc: 0 iff every failure control refuses and
every happy control passes (pure: fixtures under tempfile.mkdtemp only).
"""

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile

FORMAT = "p26-archive-manifest-v1"
_RELPATH_OK = re.compile(r"^[A-Za-z0-9._][A-Za-z0-9._+-]*(?:/[A-Za-z0-9._+-]+)*$")


class Refusal(Exception):
    pass


def check_relpath(rel):
    if not rel or rel.startswith("/") or "\\" in rel or "\x00" in rel:
        raise Refusal(f"illegal relpath {rel!r}")
    if not _RELPATH_OK.match(rel):
        raise Refusal(f"illegal relpath {rel!r}")
    parts = rel.split("/")
    if any(p in ("", ".", "..") for p in parts) or rel.endswith("/"):
        raise Refusal(f"illegal relpath {rel!r}")
    return rel


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def walk_package(pkg_dir):
    """Yield sorted relative file paths, refusing symlinks anywhere.

    The package-root manifest.json itself is never part of membership
    (self-exclusion: it cannot hash itself)."""
    root = os.path.realpath(pkg_dir)
    if not os.path.isdir(root):
        raise Refusal(f"package dir {pkg_dir!r} is not a directory")
    rels = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for d in dirnames:
            if os.path.islink(os.path.join(dirpath, d)):
                raise Refusal(f"symlink directory refused: {os.path.join(dirpath, d)}")
        for f in filenames:
            p = os.path.join(dirpath, f)
            if os.path.islink(p):
                raise Refusal(f"symlink refused: {p}")
            real = os.path.realpath(p)
            if not real.startswith(root + os.sep):
                raise Refusal(f"path escapes package root: {p}")
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            check_relpath(rel)
            if rel != "manifest.json":  # root-level manifest self-exclusion
                rels.append(rel)
    seen = {}
    for rel in rels:
        if rel in seen:
            raise Refusal(f"duplicate relpath {rel!r}")
        seen[rel] = True
    return sorted(rels)


def build_manifest(pkg_dir, expected=None):
    root = os.path.realpath(pkg_dir)
    rels = walk_package(pkg_dir)
    if expected is not None:
        for e in expected:
            check_relpath(e)
        exp = set(expected)
        got = set(rels)
        missing, extra = sorted(exp - got), sorted(got - exp)
        if missing or extra:
            raise Refusal(f"membership mismatch: missing={missing} extra={extra}")
    lines, entries = [], []
    for rel in rels:
        p = os.path.join(root, *rel.split("/"))
        digest, size = sha256_file(p), os.path.getsize(p)
        entries.append({"path": rel, "sha256": digest, "size": size})
        lines.append(f"{digest}  {size}  {rel}\n")
    blob = "".join(lines).encode()
    return {
        "format": FORMAT,
        "file_count": len(entries),
        "manifest_sha256": hashlib.sha256(blob).hexdigest(),
        "files": entries,
    }


def verify_manifest(pkg_dir, manifest_path, expected=None):
    root = os.path.realpath(pkg_dir)
    with open(manifest_path, "r", encoding="utf-8") as fh:
        man = json.load(fh)
    if man.get("format") != FORMAT:
        raise Refusal(f"unknown manifest format {man.get('format')!r}")
    listed = [e["path"] for e in man.get("files", [])]
    if len(listed) != len(set(listed)):
        raise Refusal("duplicate relpath in manifest")
    for rel in listed:
        check_relpath(rel)
    disk = walk_package(pkg_dir)
    if sorted(listed) != disk:
        missing = sorted(set(disk) - set(listed))
        extra = sorted(set(listed) - set(disk))
        raise Refusal(f"membership mismatch vs disk: missing-from-manifest={missing} extra-in-manifest={extra}")
    if expected is not None:
        for e in expected:
            check_relpath(e)
        exp = set(expected)
        got = set(disk)
        if exp != got:
            raise Refusal(f"expected-list mismatch: missing={sorted(exp - got)} extra={sorted(got - exp)}")
    lines, problems = [], []
    for e in sorted(man["files"], key=lambda e: e["path"]):
        p = os.path.join(root, *e["path"].split("/"))
        digest, size = sha256_file(p), os.path.getsize(p)
        if digest != e["sha256"] or size != e["size"]:
            problems.append(e["path"])
        lines.append(f"{e['sha256']}  {e['size']}  {e['path']}\n")
    if problems:
        raise Refusal(f"content mismatch (sha256/size) for: {sorted(problems)}")
    total = hashlib.sha256("".join(lines).encode()).hexdigest()
    if total != man.get("manifest_sha256"):
        raise Refusal(f"manifest_sha256 mismatch: manifest={man.get('manifest_sha256')} recomputed={total}")
    if man.get("file_count") != len(disk):
        raise Refusal("file_count mismatch")
    return {"file_count": len(disk), "manifest_sha256": total}


def load_expect_list(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [ln.strip() for ln in fh if ln.strip()]


# ----------------------------------------------------------------- selftest
def selftest():
    """Pure failure controls on tempdir fixtures; no repo/network access."""
    ok = []

    def control(name, fn, expect_refusal, refusal_contains=None):
        try:
            fn()
        except Refusal as e:
            if not expect_refusal:
                ok.append((name, False, f"unexpected refusal: {e}"))
            elif refusal_contains and refusal_contains not in str(e):
                ok.append((name, False, f"refusal lacks {refusal_contains!r}: {e}"))
            else:
                ok.append((name, True, ""))
        except Exception as e:  # noqa: BLE001 — control must catch everything
            ok.append((name, False, f"unexpected exception {type(e).__name__}: {e}"))
        else:
            ok.append((name, not expect_refusal,
                       "" if not expect_refusal else "expected refusal but passed"))

    def make_pkg(files):
        d = tempfile.mkdtemp(prefix="p26_manifest_selftest_")
        for rel, content in files.items():
            p = os.path.join(d, *rel.split("/"))
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "wb") as fh:
                fh.write(content if isinstance(content, bytes) else content.encode())
        return d

    # happy build + verify roundtrip, deterministic across rebuilds
    pkg = make_pkg({"a.txt": "alpha\n", "d/b.bin": b"\x00\x01\x02", "z.md": "# z"})
    m1 = build_manifest(pkg)
    m2 = build_manifest(pkg)
    control("deterministic-rebuild", lambda: (_ for _ in ()).throw(AssertionError())
            if (m1["manifest_sha256"] != m2["manifest_sha256"] or m1["file_count"] != 3
                or [e["path"] for e in m1["files"]] != ["a.txt", "d/b.bin", "z.md"])
            else None, expect_refusal=False)
    mpath = os.path.join(pkg, "manifest.json")
    with open(mpath, "w", encoding="utf-8") as fh:
        json.dump(m1, fh)
    # root-level manifest.json self-excludes from membership: verify sees the
    # three payload files exactly.
    control("verify-happy", lambda: verify_manifest(pkg, mpath, expected=["a.txt", "d/b.bin", "z.md"]),
            expect_refusal=False)

    # extra file on disk vs manifest
    with open(os.path.join(pkg, "extra.txt"), "w", encoding="utf-8") as fh:
        fh.write("extra")
    control("extra-file", lambda: verify_manifest(pkg, mpath), expect_refusal=True,
            refusal_contains="missing-from-manifest")

    # tampered content
    pkg2 = make_pkg({"a.txt": "alpha\n", "d/b.bin": b"\x00\x01\x02", "z.md": "# z tampered"})
    with open(os.path.join(pkg2, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(build_manifest(make_pkg({"a.txt": "alpha\n", "d/b.bin": b"\x00\x01\x02", "z.md": "# z"})), fh)
    control("tampered-content", lambda: verify_manifest(pkg2, os.path.join(pkg2, "manifest.json")),
            expect_refusal=True, refusal_contains="content mismatch")

    # missing required file
    pkg3 = make_pkg({"a.txt": "alpha\n"})
    control("missing-required", lambda: build_manifest(pkg3, expected=["a.txt", "gone.txt"]),
            expect_refusal=True, refusal_contains="missing")

    # symlink refused
    pkg4 = make_pkg({"real.txt": "r"})
    os.symlink(os.path.join(pkg4, "real.txt"), os.path.join(pkg4, "link.txt"))
    control("symlink-refused", lambda: build_manifest(pkg4), expect_refusal=True, refusal_contains="symlink")

    # traversal relpath in expected list refused
    control("traversal-refused", lambda: build_manifest(pkg3, expected=["../escape.txt"]),
            expect_refusal=True, refusal_contains="illegal relpath")

    # absolute relpath in manifest refused
    pkg5 = make_pkg({"a.txt": "x"})
    with open(os.path.join(pkg5, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"format": FORMAT, "file_count": 1, "manifest_sha256": "0" * 64,
                   "files": [{"path": "/etc/passwd", "sha256": "0" * 64, "size": 0}]}, fh)
    control("absolute-refused", lambda: verify_manifest(pkg5, os.path.join(pkg5, "manifest.json")),
            expect_refusal=True, refusal_contains="illegal relpath")

    # duplicate entries in manifest refused
    pkg6 = make_pkg({"a.txt": "x"})
    e = {"path": "a.txt", "sha256": sha256_file(os.path.join(pkg6, "a.txt")), "size": 1}
    with open(os.path.join(pkg6, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"format": FORMAT, "file_count": 2, "manifest_sha256": "0" * 64, "files": [e, dict(e)]}, fh)
    control("duplicate-refused", lambda: verify_manifest(pkg6, os.path.join(pkg6, "manifest.json")),
            expect_refusal=True, refusal_contains="duplicate")

    failed = [r for r in ok if not r[1]]
    for name, passed, note in ok:
        print(f"[selftest:manifest] {'PASS' if passed else 'FAIL'} {name} {note}")
    print(f"[selftest:manifest] controls={len(ok)} failed={len(failed)}")
    return 1 if failed else 0


def main():
    sys.dont_write_bytecode = True
    ap = argparse.ArgumentParser(prog="p26_archive_manifest")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("pkg_dir")
    b.add_argument("manifest_out", nargs="?")
    b.add_argument("--expect-list")
    v = sub.add_parser("verify")
    v.add_argument("pkg_dir")
    v.add_argument("manifest_json")
    v.add_argument("--expect-list")
    sub.add_parser("selftest")
    args = ap.parse_args()
    try:
        if args.cmd == "selftest":
            return selftest()
        expected = load_expect_list(args.expect_list) if args.expect_list else None
        if args.cmd == "build":
            man = build_manifest(args.pkg_dir, expected=expected)
            out = args.manifest_out or os.path.join(args.pkg_dir, "manifest.json")
            with open(out, "w", encoding="utf-8") as fh:
                json.dump(man, fh, indent=1, sort_keys=True)
                fh.write("\n")
            print(json.dumps({"manifest_sha256": man["manifest_sha256"], "file_count": man["file_count"], "out": out}))
            return 0
        res = verify_manifest(args.pkg_dir, args.manifest_json, expected=expected)
        print(json.dumps({"verified": True, **res}))
        return 0
    except Refusal as e:
        print(f"[p26-archive-manifest] REFUSED: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
