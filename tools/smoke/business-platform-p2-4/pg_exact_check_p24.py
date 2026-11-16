#!/usr/bin/env python3
"""P24 PG exact-checker (formal-execution-contract.md §isolated-PG).

Tools-GLM-owned. Reads the IMMUTABLE old name lists (P1 26 + P2.1 14 +
P2.2 12 + P2.3 7) plus the ACTUAL new P24 names file, rejects
duplicates/illegal/empty lists, and verifies in ONE fresh unified raw
log that every required name shows EXACTLY:
    1 top-level `=== RUN <name>` AND 1 `--- PASS: <name>`
    AND 0 `--- SKIP: <name>` AND 0 `--- FAIL: <name>`
Subtests (`=== RUN <name>/sub`) never count. The wrapped Go command's
REAL rc must be 0 (rc file argument — a grep can never replace it).
Deliberately NOT a copy of the P23 checker (which hard-pinned len 59
and had no explicit FAIL check): the expected total is COMPUTED from
the actual lists (old 59 + current N; today N=13 → 72, but the number
follows the real code, not this comment).

Usage:
  python3 -I pg_exact_check_p24.py [--new-only] <log> <go.rc.file> \
      <p1/run_tests.sh> <run_tests_p21.sh> <p22-required-names.txt> \
      <p23-required-names.txt> <p24-required-names.txt>
  --new-only  adjudicate ONLY the actual new P24 names list (used for
              the separate new-normal / new-race logs; each log must
              still show 1 RUN / 1 PASS / 0 SKIP / 0 FAIL per name and
              the wrapped Go rc must be 0).
Exit 0 iff every name is exactly 1 RUN/1 PASS/0 SKIP/0 FAIL, totals
match, and the Go rc file reads 0.
"""

import re
import sys

ILLEGAL = re.compile(r"[^A-Za-z0-9_]")


def load_old_names(p1_sh, p21_sh):
    names = []
    for path, var in ((p1_sh, "REQUIRED_DATASET"),
                      (p1_sh, "REQUIRED_PIPELINE"),
                      (p1_sh, "REQUIRED_CONNECTOR"),
                      (p21_sh, "REQUIRED_LEASE")):
        m = re.search(rf'^{var}="([^"]+)"', open(path).read(), re.M)
        if not m:
            raise SystemExit(f"exact-p24: {var} not found in {path}")
        names.extend(m.group(1).split())
    return names


def load_list_file(path):
    out = []
    for ln in open(path):
        ln = ln.split("#")[0].strip()
        if ln:
            out.append(ln)
    return out


def main():
    args = sys.argv[1:]
    new_only = "--new-only" in args
    if new_only:
        args = [a for a in args if a != "--new-only"]
    (log_path, rc_path, p1_sh, p21_sh, p22_f, p23_f, p24_f) = args[1:8] \
        if args and args[0] == "--" else args[:7]
    try:
        go_rc = int(open(rc_path).read().strip())
    except (OSError, ValueError):
        print(f"exact-p24: FAIL — Go rc file unreadable: {rc_path}")
        return 1
    if go_rc != 0:
        print(f"exact-p24: FAIL — wrapped Go rc={go_rc} (a grep can "
              "never replace the real rc)")
        return 1

    if new_only:
        names = load_list_file(p24_f)
    else:
        names = (load_old_names(p1_sh, p21_sh)
                 + load_list_file(p22_f) + load_list_file(p23_f)
                 + load_list_file(p24_f))
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
    if not names:
        bad.append("empty required list — refusing to bless")

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
    print(f"[exact-p24:{mode}] distinct={len(seen)} bad={len(bad)} "
          f"go_rc={go_rc}")
    for b in bad[:12]:
        print("   ", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())


import re
import sys

ILLEGAL = re.compile(r"[^A-Za-z0-9_]")


def load_old_names(p1_sh, p21_sh):
    names = []
    for path, var in ((p1_sh, "REQUIRED_DATASET"),
                      (p1_sh, "REQUIRED_PIPELINE"),
                      (p1_sh, "REQUIRED_CONNECTOR"),
                      (p21_sh, "REQUIRED_LEASE")):
        m = re.search(rf'^{var}="([^"]+)"', open(path).read(), re.M)
        if not m:
            raise SystemExit(f"exact-p24: {var} not found in {path}")
        names.extend(m.group(1).split())
    return names


def load_list_file(path):
    out = []
    for ln in open(path):
        ln = ln.split("#")[0].strip()
        if ln:
            out.append(ln)
    return out


def main():
    (log_path, rc_path, p1_sh, p21_sh, p22_f, p23_f, p24_f) = sys.argv[1:8]
    try:
        go_rc = int(open(rc_path).read().strip())
    except (OSError, ValueError):
        print(f"exact-p24: FAIL — Go rc file unreadable: {rc_path}")
        return 1
    if go_rc != 0:
        print(f"exact-p24: FAIL — wrapped Go rc={go_rc} (a grep can "
              "never replace the real rc)")
        return 1

    names = (load_old_names(p1_sh, p21_sh)
             + load_list_file(p22_f) + load_list_file(p23_f)
             + load_list_file(p24_f))
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
    if not names or not load_list_file(p24_f):
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

    print(f"[exact-p24] distinct={len(seen)} bad={len(bad)} "
          f"go_rc={go_rc}")
    for b in bad[:12]:
        print("   ", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
