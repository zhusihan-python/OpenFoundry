#!/usr/bin/env python3
"""Strict structured verifier for the dataset-excel real-backend acceptance.

A step "counts" only if BOTH the underlying command exited 0 AND this
verifier accepts its evidence. Deliberately paranoid (same anti-greening
contract as tools/smoke/data-integration-s1-security): grep-ing logs for
"passed" proves nothing.

The Playwright JSON contract below was pinned against REAL output from the
repo-pinned @playwright/test 1.59.1 json reporter (suite.suites[].specs[]
carry the titles; spec.tests[] are per-project entries; stats has exactly
expected/skipped/unexpected/flaky). The negative controls mutate a REAL
run's JSON — never a hand-made shape — to prove each rejection class.

Modes and what they enforce:

  playwright <json>
      Both REQUIRED real-backend specs present; every spec exactly one
      per-project test with status "expected" + expectedStatus "passed";
      exactly one result with status "passed", retry 0, positive duration,
      startTime present. stats.expected==2 and skipped/unexpected/flaky==0;
      top-level errors empty; total spec count exactly 2 (no hidden specs).
      SKIP / failed / flaky / interrupted / missing / empty / retry traces
      are all REJECTED.

  manifest <json>
      Evidence manifest: typed fields, git commit (40 hex) + real bool
      dirty; artifacts re-hashed on disk vs recorded sha256 (duplicate
      artifact names rejected); orders-real.xlsx hash must equal the
      PINNED value; probes 200; token block must NOT contain value/secret
      material.

  exitcodes <file>
      Every recorded raw exit code must be 0; duplicate step names are
      REJECTED; all REQUIRED steps must be present.

Usage: verify_results.py (playwright|manifest|exitcodes) <file>
Exit 0 iff every check passes; non-zero with a reason on stderr otherwise.
"""
import hashlib
import json
import re
import sys

# The two real-backend tests that MUST actually run and pass. Titles are
# pinned from apps/web/e2e/dataset-excel-real.spec.ts (testMatch of
# playwright.p31-real.config.ts selects exactly this file).
REQUIRED_TESTS = [
    "real stack: 订单 sheet (header row 3) → code/desc/order_no/qty → "
    "validated json dataset import → detail preview & schema",
    "real stack: no-header sheet 原始 keeps the first row and imports with "
    "fallback names",
]
# Exactly two tests live in the spec; a run reporting any other count is
# suspect (hidden skips, stray specs, truncated JSON).
EXPECTED_SPEC_COUNT = 2
# One project (chromium) in playwright.p31-real.config.ts → one per-project
# test entry per spec.
EXPECTED_PROJECT_TESTS_PER_SPEC = 1

# sha256 of apps/web/e2e/fixtures/orders-real.xlsx as pinned in
# apps/web/e2e/fixtures/README.md (byte-identical to the p31 handoff
# fixture). Fixture drift must fail acceptance.
FIXTURE_PINNED_SHA256 = (
    "1d52beb7a870225116357011720553462f991095a67a2e3122159f4c512d745f"
)
FIXTURE_ARTIFACT_NAME = "fixture-orders-real-xlsx"
REQUIRED_ARTIFACTS = ["binary-dvs", "binary-gw", FIXTURE_ARTIFACT_NAME,
                      "playwright-results"]
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")

# Steps the runner MUST have recorded (see run.sh). A run missing any of
# these did not execute the full acceptance path. (negative_controls.sh
# self-asserts under the run_step "negative-controls" wrapper; the
# skip-control is the live opt-in-gate control recorded via
# expect_verifier_reject.)
REQUIRED_STEPS = [
    "ensure-images", "record-env", "build-dvs", "build-gw", "stack-probe",
    "skip-control-playwright", "skip-control-shape",
    "skip-control-expected-reject", "playwright-real",
    "playwright-real-verify", "negative-controls", "verify-manifest",
    "evidence-hygiene",
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


def require_dict(value, what: str) -> dict:
    if not isinstance(value, dict):
        fail(f"{what} must be an object, got {type(value).__name__}")
    return value


def require_bool(value, what: str) -> None:
    if not isinstance(value, bool):
        fail(f"{what} must be a real boolean, got {type(value).__name__} "
             f"{value!r}")


def require_int(value, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"{what} must be an int, got {type(value).__name__} {value!r}")
    return value


def collect_specs(node: dict, out: list, path: str) -> None:
    """Recursively collect (spec.title, spec, where) from a Playwright JSON
    tree. Suites nest via .suites; leaf suites carry .specs (titles live on
    specs, never on the per-project test entries)."""
    suites = node.get("suites")
    if isinstance(suites, list):
        for i, sub in enumerate(suites):
            collect_specs(require_dict(sub, f"{path}.suites[{i}]"), out,
                          f"{path}.suites[{i}]")
    specs = node.get("specs")
    if isinstance(specs, list):
        for i, sp in enumerate(specs):
            sp = require_dict(sp, f"{path}.specs[{i}]")
            title = sp.get("title")
            if not isinstance(title, str) or not title:
                fail(f"{path}.specs[{i}]: no string title")
            out.append((title, sp, f"{path}.specs[{i}]"))


def verify_playwright(json_path: str) -> None:
    doc = require_dict(load(json_path), json_path)
    stats = require_dict(doc.get("stats"), f"{json_path}: stats")
    # Playwright 1.59 stats: expected / skipped / unexpected / flaky
    # (didNotRun only appears in newer versions — reject non-zero if the
    # field exists at all).
    for field, want in (("expected", EXPECTED_SPEC_COUNT),
                        ("unexpected", 0), ("flaky", 0), ("skipped", 0)):
        got = require_int(stats.get(field), f"{json_path}: stats.{field}")
        if got != want:
            fail(f"{json_path}: stats.{field}={got}, want {want} "
                 f"(SKIP/failed/flaky/missing never count as pass)")
    if "didNotRun" in stats \
            and require_int(stats["didNotRun"], "stats.didNotRun") != 0:
        fail(f"{json_path}: stats.didNotRun={stats['didNotRun']} != 0")
    errors = doc.get("errors")
    if not isinstance(errors, list):
        fail(f"{json_path}: top-level errors must be a list")
    if errors:
        fail(f"{json_path}: top-level errors present: {errors[:1]}")

    all_specs: list = []
    collect_specs(doc, all_specs, json_path)
    if len(all_specs) != EXPECTED_SPEC_COUNT:
        fail(f"{json_path}: collected {len(all_specs)} specs, want exactly "
             f"{EXPECTED_SPEC_COUNT}")
    by_title: dict = {}
    for title, sp, where in all_specs:
        if title in by_title:
            fail(f"{json_path}: duplicate spec title {title!r}")
        by_title[title] = (sp, where)
        if sp.get("ok") is not True:
            fail(f"{json_path}: {where} ({title!r}) spec.ok is "
                 f"{sp.get('ok')!r}, want true")
    missing = [t for t in REQUIRED_TESTS if t not in by_title]
    if missing:
        fail(f"{json_path}: required tests missing: {missing}")
    for title in REQUIRED_TESTS:
        sp, where = by_title[title]
        tests = sp.get("tests")
        if not isinstance(tests, list) \
                or len(tests) != EXPECTED_PROJECT_TESTS_PER_SPEC:
            fail(f"{json_path}: {where} ({title!r}) must have exactly "
                 f"{EXPECTED_PROJECT_TESTS_PER_SPEC} per-project test "
                 f"entries, got {tests!r}")
        t = require_dict(tests[0], f"{where}.tests[0]")
        if t.get("status") != "expected":
            fail(f"{json_path}: {where} ({title!r}) test status "
                 f"{t.get('status')!r} != 'expected'")
        if t.get("expectedStatus") != "passed":
            fail(f"{json_path}: {where} ({title!r}) expectedStatus "
                 f"{t.get('expectedStatus')!r} != 'passed'")
        results = t.get("results")
        if not isinstance(results, list) or len(results) != 1:
            got = len(results) if isinstance(results, list) else results
            fail(f"{json_path}: {where} ({title!r}) must have exactly one "
                 f"result, got {got!r} (retries leave traces)")
        r = require_dict(results[0], f"{where}.tests[0].results[0]")
        if r.get("status") != "passed":
            fail(f"{json_path}: {where} ({title!r}) result status "
                 f"{r.get('status')!r} != 'passed'")
        if require_int(r.get("retry"), f"{where} result.retry") != 0:
            fail(f"{json_path}: {where} ({title!r}) result.retry != 0 "
                 f"(flaky retry traces never count)")
        if not isinstance(r.get("startTime"), str) or not r["startTime"]:
            fail(f"{json_path}: {where} ({title!r}) result has no "
                 f"startTime — no real execution trace")
        duration = r.get("duration")
        if isinstance(duration, bool) or not isinstance(duration, int) \
                or duration < 1:
            fail(f"{json_path}: {where} ({title!r}) result duration "
                 f"{duration!r} is not a positive int (skips are 0)")
    print(f"verify: ACCEPT {json_path} "
          f"(2/2 required real-backend tests executed and passed)")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    except FileNotFoundError:
        fail(f"manifest artifact {path} does not exist on disk")
    return h.hexdigest()


def verify_manifest(json_path: str) -> None:
    doc = require_dict(load(json_path), json_path)
    if not isinstance(doc.get("run_id"), str) or not doc["run_id"]:
        fail(f"{json_path}: run_id must be a non-empty string")
    git = require_dict(doc.get("git"), f"{json_path}: git")
    commit = git.get("commit")
    if not isinstance(commit, str) or not COMMIT_RE.match(commit):
        fail(f"{json_path}: git.commit must be a 40-hex sha, got {commit!r}")
    require_bool(git.get("dirty"), f"{json_path}: git.dirty")
    if not isinstance(git.get("branch"), str) or not git["branch"]:
        fail(f"{json_path}: git.branch must be a non-empty string")

    artifacts = doc.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        fail(f"{json_path}: artifacts must be a non-empty list")
    names = set()
    for i, a in enumerate(artifacts):
        a = require_dict(a, f"{json_path}: artifacts[{i}]")
        name = a.get("name")
        path = a.get("path")
        sha = a.get("sha256")
        if not isinstance(name, str) or not name:
            fail(f"{json_path}: artifacts[{i}].name must be a string")
        if name in names:
            fail(f"{json_path}: duplicate artifact name {name!r}")
        if not isinstance(path, str) or not path:
            fail(f"{json_path}: artifacts[{name!r}].path must be a string")
        if not isinstance(sha, str) or not SHA256_RE.match(sha):
            fail(f"{json_path}: artifacts[{name!r}].sha256 must be 64-hex")
        actual = sha256_file(path)
        if actual != sha:
            fail(f"{json_path}: artifact {name!r} hash drift: manifest "
                 f"{sha} != on-disk {actual}")
        names.add(name)
    missing = [n for n in REQUIRED_ARTIFACTS if n not in names]
    if missing:
        fail(f"{json_path}: required artifacts missing: {missing}")
    fixture_sha = next(a["sha256"] for a in artifacts
                       if a["name"] == FIXTURE_ARTIFACT_NAME)
    if fixture_sha != FIXTURE_PINNED_SHA256:
        fail(f"{json_path}: fixture hash {fixture_sha} != pinned "
             f"{FIXTURE_PINNED_SHA256} (orders-real.xlsx drifted)")

    probe = require_dict(doc.get("probe"), f"{json_path}: probe")
    for field in ("gw_healthz_status", "catalog_status"):
        if require_int(probe.get(field), f"{json_path}: probe.{field}") != 200:
            fail(f"{json_path}: probe.{field} is {probe.get(field)!r}, "
                 f"want 200 (stack was not healthy end-to-end)")

    env = require_dict(doc.get("env"), f"{json_path}: env")
    for field in ("node", "pnpm", "playwright", "go", "docker_client"):
        if not isinstance(env.get(field), str) or not env[field]:
            fail(f"{json_path}: env.{field} must be a non-empty version "
                 f"string (tags alone are not runtime identities)")
    images = require_dict(env.get("images"), f"{json_path}: env.images")
    if not images:
        fail(f"{json_path}: env.images must record image identities")
    for ref, info in images.items():
        info = require_dict(info, f"{json_path}: env.images[{ref!r}]")
        if not isinstance(info.get("id"), str) \
                or not info["id"].startswith("sha256:"):
            fail(f"{json_path}: env.images[{ref!r}].id must be a sha256 "
                 f"image identity")

    token = require_dict(doc.get("token"), f"{json_path}: token")
    for banned in ("value", "jwt", "secret", "bearer"):
        if banned in token:
            fail(f"{json_path}: token block must never contain "
                 f"{banned!r} material")
    if not isinstance(token.get("sub"), str) or not token["sub"]:
        fail(f"{json_path}: token.sub must be a non-empty string")
    if not isinstance(token.get("sha256"), str) \
            or not SHA256_RE.match(token["sha256"]):
        fail(f"{json_path}: token.sha256 must be 64-hex")

    stack = require_dict(doc.get("stack"), f"{json_path}: stack")
    for field in ("gw_host_port", "web_port"):
        if require_int(stack.get(field), f"{json_path}: stack.{field}") <= 0:
            fail(f"{json_path}: stack.{field} must be a positive port")
    for field in ("network", "go_image", "pg_image"):
        if not isinstance(stack.get(field), str) or not stack[field]:
            fail(f"{json_path}: stack.{field} must be a non-empty string")
    containers = stack.get("containers")
    if not isinstance(containers, list) or not containers \
            or not all(isinstance(c, str) and c for c in containers):
        fail(f"{json_path}: stack.containers must be a list of names")
    print(f"verify: ACCEPT {json_path} (manifest typed, artifacts re-hashed, "
          f"fixture pinned, probes 200)")


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
        step, rc = m.group(1), int(m.group(2))
        if step in codes:
            fail(f"{path}: duplicate step {step!r} "
                 f"(would silently overwrite {codes[step]})")
        codes[step] = rc
    if not codes:
        fail(f"{path}: no exit codes recorded")
    missing = [s for s in REQUIRED_STEPS if s not in codes]
    if missing:
        fail(f"{path}: required steps missing: {missing}")
    bad = {k: v for k, v in codes.items() if v != 0}
    if bad:
        fail(f"{path}: non-zero raw exit codes {bad}")
    print(f"verify: ACCEPT {path} (all of {len(codes)} steps rc=0, "
          f"required steps present)")


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    mode, arg = sys.argv[1], sys.argv[2]
    if mode == "playwright":
        verify_playwright(arg)
    elif mode == "manifest":
        verify_manifest(arg)
    elif mode == "exitcodes":
        verify_exitcodes(arg)
    else:
        fail(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
