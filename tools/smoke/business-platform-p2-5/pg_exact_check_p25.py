#!/usr/bin/env python3
"""P25 PG exact-checker (implementation-release.md §File ownership).

PG-GLM-owned. Complete rewrite after the quota-interrupted partial edit
(failed version preserved by SHA 5f488247…dbd17 + evidence; this file
restores the FULL exact-name logic and adds the strict positive meta
contract — it is not a git reset).

Verdict contract:
  1. --meta is REQUIRED for the formal verdict (--allow-no-meta only for
     ad-hoc debugging; the real test release carries --meta explicitly).
  2. The attempt .meta must carry the COMPLETE required key set, each key
     EXACTLY once and non-empty:
         source_content_sha256_before / _after   (64-hex; before == after;
             with --expect-source-sha also == the expected candidate sha)
         version_ok == true
         original_docker_rc / source_gate_rc / version_gate_rc /
             wrapper_rc == 0
     An empty meta, missing keys, duplicates, empty values, drift flags or
     any violation REFUSES the verdict (fail-closed, never bad-line-only).
  3. The wrapped Go command's REAL rc file must read 0 (a grep can never
     replace the real rc).
  4. Every required name in ONE fresh raw log shows EXACTLY:
         1 top-level `=== RUN <name>` AND 1 `--- PASS: <name>`
         AND 0 `--- SKIP: <name>` AND 0 `--- FAIL: <name>`
     Subtests (`=== RUN <name>/sub`) never count. Duplicates/illegal/
     empty name lists refuse. --new-only adjudicates the P25 list only
     (new-normal / new-race logs; the list carries ONLY active names);
     the unified mode is old72+new15=87.

Usage:
  python3 -I pg_exact_check_p25.py [--new-only] [--allow-no-meta] \
      [--meta <attempt.meta>] [--expect-source-sha <64hex>] \
      <log> <go.rc.file> <p1/run_tests.sh> <run_tests_p21.sh> \
      <p22-required-names.txt> <p23-required-names.txt> \
      <p24-required-names.txt> <p25-required-names.txt>

Exit 0 iff every check above holds.
"""

import re
import sys

ILLEGAL = re.compile(r"[^A-Za-z0-9_]")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

REQUIRED_META_KEYS = (
    "source_content_sha256_before",
    "source_content_sha256_after",
    "version_ok",
    "original_docker_rc",
    "source_gate_rc",
    "version_gate_rc",
    "wrapper_rc",
)


def load_old_names(p1_sh, p21_sh):
    names = []
    for path, var in ((p1_sh, "REQUIRED_DATASET"),
                      (p1_sh, "REQUIRED_PIPELINE"),
                      (p1_sh, "REQUIRED_CONNECTOR"),
                      (p21_sh, "REQUIRED_LEASE")):
        m = re.search(rf'^{var}="([^"]+)"', open(path).read(), re.M)
        if not m:
            raise SystemExit(f"exact-p25: {var} not found in {path}")
        names.extend(m.group(1).split())
    return names


def load_list_file(path):
    out = []
    for ln in open(path):
        ln = ln.split("#")[0].strip()
        if ln:
            out.append(ln)
    return out


def check_meta(meta_path, expect_sha):
    try:
        meta_lines = [l.strip() for l in open(meta_path, errors="replace") if l.strip()]
    except OSError:
        print(f"exact-p25: FAIL — meta unreadable: {meta_path}")
        return False
    values = {k: [] for k in REQUIRED_META_KEYS}
    drift = False
    for line in meta_lines:
        if line == "SOURCE_DRIFT_DURING_RUN=1":
            drift = True
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k in values:
            values[k].append(v.strip())
    bad = []
    if not meta_lines:
        bad.append("meta is EMPTY")
    if drift:
        bad.append("SOURCE_DRIFT_DURING_RUN=1 present")
    for k in REQUIRED_META_KEYS:
        if len(values[k]) == 0:
            bad.append(f"missing required key {k}")
        elif len(values[k]) > 1:
            bad.append(f"duplicate key {k} ({len(values[k])}x)")
        elif not values[k][0]:
            bad.append(f"empty value for {k}")
    if bad:
        print(f"exact-p25: FAIL — meta contract violated ({meta_path}):")
        for b in bad:
            print("   ", b)
        return False
    m = {k: values[k][0] for k in REQUIRED_META_KEYS}
    for k in ("source_content_sha256_before", "source_content_sha256_after"):
        if not SHA_RE.match(m[k]):
            print(f"exact-p25: FAIL — {k} is not a 64-hex sha: {m[k]!r}")
            return False
    if m["source_content_sha256_before"] != m["source_content_sha256_after"]:
        print("exact-p25: FAIL — source sha before != after")
        return False
    if expect_sha is not None and m["source_content_sha256_before"] != expect_sha:
        print(f"exact-p25: FAIL — source sha {m['source_content_sha256_before']} "
              f"!= expected {expect_sha}")
        return False
    if m["version_ok"] != "true":
        print(f"exact-p25: FAIL — version_ok={m['version_ok']!r} (want true)")
        return False
    for k in ("original_docker_rc", "source_gate_rc", "version_gate_rc", "wrapper_rc"):
        if m[k] != "0":
            print(f"exact-p25: FAIL — {k}={m[k]!r} (want 0)")
            return False
    print(f"[exact-p25] meta contract OK ({meta_path}"
          + (", sha pinned)" if expect_sha is not None else ")"))
    return True


def main():
    args = sys.argv[1:]
    new_only = "--new-only" in args
    if new_only:
        args = [a for a in args if a != "--new-only"]
    allow_no_meta = "--allow-no-meta" in args
    if allow_no_meta:
        args = [a for a in args if a != "--allow-no-meta"]
    meta_path = None
    expect_sha = None
    for flag in ("--meta", "--expect-source-sha"):
        if flag in args:
            i = args.index(flag)
            try:
                value = args[i + 1]
            except IndexError:
                print(f"exact-p25: {flag} needs a value", file=sys.stderr)
                return 2
            if flag == "--meta":
                meta_path = value
            else:
                expect_sha = value.strip().lower()
                if not SHA_RE.match(expect_sha):
                    print("exact-p25: --expect-source-sha must be 64-hex",
                          file=sys.stderr)
                    return 2
            args = args[:i] + args[i + 2:]
    if args and args[0] == "--":
        args = args[1:]

    if meta_path is None and not allow_no_meta:
        print("exact-p25: FAIL --meta is REQUIRED for the formal verdict "
              "(pass the attempt's .meta; --allow-no-meta only for ad-hoc)")
        return 2

    if len(args) != 8:
        print("exact-p25: want 8 path args: <log> <go.rc> <p1/run_tests.sh> "
              "<run_tests_p21.sh> <p22> <p23> <p24> <p25>", file=sys.stderr)
        return 2
    (log_path, rc_path, p1_sh, p21_sh, p22_f, p23_f, p24_f, p25_f) = args[:8]

    if meta_path is not None and not check_meta(meta_path, expect_sha):
        return 1

    try:
        go_rc = int(open(rc_path).read().strip())
    except (OSError, ValueError):
        print(f"exact-p25: FAIL — Go rc file unreadable: {rc_path}")
        return 1
    if go_rc != 0:
        print(f"exact-p25: FAIL — wrapped Go rc={go_rc} (a grep can "
              "never replace the real rc)")
        return 1

    if new_only:
        names = load_list_file(p25_f)
    else:
        names = (load_old_names(p1_sh, p21_sh)
                 + load_list_file(p22_f) + load_list_file(p23_f)
                 + load_list_file(p24_f) + load_list_file(p25_f))
    bad = []
    seen = set()
    for n in names:
        if n in seen:
            bad.append(f"duplicate required name {n}")
            continue
        if ILLEGAL.search(n):
            bad.append(f"illegal name {n!r}")
            continue
        seen.add(n)
    if not names or not load_list_file(p25_f):
        bad.append("empty required list(s) — refusing to bless")

    text = open(log_path, errors="replace").read()
    for n in sorted(seen):
        e = re.escape(n)
        run = len(re.findall(rf"^=== RUN\s+{e}(\s|$)", text, re.M))
        passed = len(re.findall(rf"^--- PASS:\s+{e}(\s|\(|$)", text, re.M))
        skip = len(re.findall(rf"^--- SKIP:\s+{e}(\s|\(|$)", text, re.M))
        fail = len(re.findall(rf"^--- FAIL:\s+{e}(\s|\(|$)", text, re.M))
        if (run, passed, skip, fail) != (1, 1, 0, 0):
            bad.append(f"{n}: RUN={run} PASS={passed} SKIP={skip} "
                       f"FAIL={fail} (want 1/1/0/0)")

    mode = "new-only" if new_only else "unified"
    print(f"[exact-p25:{mode}] distinct={len(seen)} bad={len(bad)} "
          f"go_rc={go_rc}")
    for b in bad[:12]:
        print("   ", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
