#!/usr/bin/env python3
"""Strict structured verifier for the S1 security acceptance results.

The runner (run.sh) calls this after each evidence-producing step; a step
"counts" only if BOTH the underlying command exited 0 AND this verifier
accepts its JSON. It is deliberately paranoid — the review's whole point was
that grep-ing logs for "FAIL" cannot prove success:

  wp21-http-pg.json:   file exists, run_id matches, all_passed is real True,
                       cases is a non-empty list, every case has typed fields
                       and pass is True, the REQUIRED scenario set is present,
                       and no case is skipped.
  wp21-http-pg.log:    must contain the required PASS line and no SKIP/FAIL.
  wp22-binary-gate.json: same strict typing, its own required scenario set
                       (including the unreachable-DSN runtime-order case),
                       real booleans only (0/1 ints are rejected).
  exit-codes file:     every recorded raw exit code must be 0.

Usage: verify_results.py (http|gate|exitcodes) <args...>
  http <json> <log> <run_id>
  gate <json> <run_id>
  exitcodes <file>
Exit 0 iff every check passes; non-zero with a reason on stderr otherwise.
"""
import json
import re
import sys

# Scenario names the matrix MUST contain (spot-inventory, not an allowlist —
# extra cases are fine, missing ones are a failure).
HTTP_REQUIRED = [
    "cross-org admin invoke",
    "cross-org both-grants invoke",
    "same sub switched to org B",
    "org admin against personal webhook",
    "personal cross-owner invoke",
    "cross-org admin history",
    "cross-org history alias route",
    "invalid jwt invoke",
    "missing jwt invoke",
    "invalid jwt history",
    "non-uuid id rejected before lookup",
    "unknown uuid is a plain 404",
    "rate-limited owner invoke still allowed after flood",
    "rate-limited second invoke throttled",
    "org owner invoke",
    "org admin invoke",
    "org member connections:write invoke",
    "org member webhooks:invoke",
    "personal owner invoke",
    "org member without grants invoke",
    "org member without grants history",
    "org owner history",
    "org member webhooks:read history",
    "org owner history alias",
    "personal owner history",
]
GATE_REQUIRED = [
    "production missing key",
    "production legacy JWT-derived key refused",
    "production wrong-length key refused",
    "production whitespace key",
    "unset environment missing key",
    "helm-default shared missing key",
    "staging missing key",
    "SERVICE_VERSION=dev is not a dev signal",
    "production missing key refuses before DB connect (unreachable DSN)",
    "production with independent key boots",
    "development fallback boots",
]


def fail(msg: str) -> None:
    print(f"verify: REJECT — {msg}", file=sys.stderr)
    sys.exit(1)


def load(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        fail(f"{path} does not exist (evidence step did not produce it)")
    except json.JSONDecodeError as exc:
        fail(f"{path} is not valid JSON: {exc}")
    raise AssertionError  # unreachable


def check_common(doc: dict, path: str, run_id: str, required: list[str], with_status: bool) -> None:
    if not isinstance(doc, dict):
        fail(f"{path}: top level must be an object")
    if doc.get("run_id") != run_id:
        fail(f"{path}: run_id {doc.get('run_id')!r} != expected {run_id!r}")
    ap = doc.get("all_passed")
    if not isinstance(ap, bool):
        fail(f"{path}: all_passed must be a real boolean, got {type(ap).__name__} {ap!r}")
    if ap is not True:
        fail(f"{path}: all_passed is not true")
    cases = doc.get("cases")
    if not isinstance(cases, list) or not cases:
        fail(f"{path}: cases must be a non-empty list")
    names = set()
    for i, case in enumerate(cases):
        if not isinstance(case, dict) or not isinstance(case.get("name"), str):
            fail(f"{path}: case[{i}] has no string name")
        name = case["name"]
        names.add(name)
        if with_status:
            got, want = case.get("got_status"), case.get("want_status")
            for field, value in (("got_status", got), ("want_status", want)):
                if not isinstance(value, int) or isinstance(value, bool):
                    fail(f"{path}: case {name!r} {field} must be int, got {value!r}")
            if got != want:
                fail(f"{path}: case {name!r} got_status {got} != want_status {want}")
        else:
            if not isinstance(case.get("want"), str) or not case["want"]:
                fail(f"{path}: case {name!r} has no string 'want' expectation")
        p = case.get("pass")
        if not isinstance(p, bool):
            fail(f"{path}: case {name!r} pass must be a real boolean, got {type(p).__name__} {p!r}")
        if p is not True:
            fail(f"{path}: case {name!r} pass=false")
    missing = [r for r in required if r not in names]
    if missing:
        fail(f"{path}: required scenarios missing: {missing}")


def verify_http(json_path: str, log_path: str, run_id: str) -> None:
    doc = load(json_path)
    check_common(doc, json_path, run_id, HTTP_REQUIRED, with_status=True)
    # Cross-tenant rejections must show zero outbound + zero history delta.
    for case in doc["cases"]:
        if case["want_status"] in (401, 403, 404, 400):
            if case.get("got_upstream") != 0 or case.get("got_history") != 0:
                fail(f"{json_path}: rejection {case['name']!r} has non-zero side-effect deltas")
    try:
        log = open(log_path, "r", encoding="utf-8").read()
    except FileNotFoundError:
        fail(f"{log_path} does not exist")
    if "--- PASS: TestS1SecWebhookTenantIsolationHTTPAgainstPG" not in log:
        fail(f"{log_path}: required PASS line for the HTTP/PG acceptance test is missing")
    if re.search(r"^(--- FAIL|FAIL\s|\s+--- SKIP)", log, re.M):
        fail(f"{log_path}: contains FAIL/Skip lines")
    print(f"verify: ACCEPT {json_path} ({len(doc['cases'])} cases) + {log_path}")


def verify_gate(json_path: str, run_id: str) -> None:
    doc = load(json_path)
    check_common(doc, json_path, run_id, GATE_REQUIRED, with_status=False)
    # The runtime-order case must show a fast, non-timeout refusal.
    for case in doc["cases"]:
        if case["name"].startswith("production missing key refuses before DB"):
            if case.get("exit_code") == 0 or case.get("exit_code") == 124:
                fail(f"{json_path}: runtime-order case exited {case.get('exit_code')} (0=booted, 124=timeout kill)")
            if not isinstance(case.get("elapsed_s"), int) or case["elapsed_s"] >= 10:
                fail(f"{json_path}: runtime-order case not fast (elapsed_s={case.get('elapsed_s')!r})")
    print(f"verify: ACCEPT {json_path} ({len(doc['cases'])} cases)")


def verify_exitcodes(path: str) -> None:
    try:
        raw = open(path, "r", encoding="utf-8").read()
    except FileNotFoundError:
        fail(f"{path} does not exist")
    codes = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([A-Za-z0-9_.-]+)\s*=\s*(\d+)$", line)
        if not m:
            fail(f"{path}: unparseable line {line!r}")
        codes[m.group(1)] = int(m.group(2))
    if not codes:
        fail(f"{path}: no exit codes recorded")
    bad = {k: v for k, v in codes.items() if v != 0}
    if bad:
        fail(f"{path}: non-zero raw exit codes {bad}")
    print(f"verify: ACCEPT {path} (all of {len(codes)} steps rc=0)")


def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    mode = sys.argv[1]
    if mode == "http":
        verify_http(sys.argv[2], sys.argv[3], sys.argv[4])
    elif mode == "gate":
        verify_gate(sys.argv[2], sys.argv[3])
    elif mode == "exitcodes":
        verify_exitcodes(sys.argv[2])
    else:
        fail(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
