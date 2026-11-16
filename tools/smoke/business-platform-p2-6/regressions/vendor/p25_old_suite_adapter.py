#!/usr/bin/env python3
"""P25 TRANSPARENT adapter for rerunning frozen P23/P24 HTTP suites in
SIX-state (DRAFT — NOT RELEASED until root review).

Mirrors the root-approved p23_q_guard_adapter_p24.py mechanism and adds
the round3 (review-runtime-tools-round2 #2) requirements:
  * loads the FROZEN suite module unmodified and runs ITS main() with
    forwarded argv (frozen mains accept NO argv → scoped sys.argv swap
    + restore);
  * substitutes EXACTLY the inner-guard subprocess argv
      [python3, -I, <frozen>/p23_live_guard.py|p24_live_guard.py,
       <dir>, connector]
    into
      [python3, -I, <p25>/p23_q_guard_shim_p25.py, <dir>, connector]
    (same kwargs; the REAL CompletedProcess/rc/stdout/stderr come back);
  * every other subprocess call is delegated UNCHANGED; a guard-looking
    argv that does not match the exact 5-token shape is REJECTED loudly;
  * the REVIEWED DEPLOYMENT RECORD (--deployment-record, written by the
    driver after the candidate swap and refreshed after every
    restart-bearing suite) is VERIFIED before the suite loads: schema,
    RFC3339 boot, full-64-hex image, image == selected-build connector
    image, source_sha256 == the reviewed candidate. The steady shim env
    (P25_SHIM_POLICY/P25_TARGET_BOOT/P25_TARGET_IMAGE) is set FROM the
    verified record and injected into every guard child process — the
    adapter never trusts bare env strings;
  * archives the adaptation record: original/replacement argv, shas of
    BOTH guard files + the shim, the REAL guard rc/stdout/stderr +
    their sha256s, invocation count;
  * EXACT expected invocation counts are ENFORCED: p23 Q = exactly 1;
    p24 full7 (with --with-restart-probe: guard_pre + pre-stop) =
    exactly 2 — ZERO substitutions (or any other count) FAIL the run.
"""

import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SHIM = HERE / "p23_q_guard_shim_p25.py"
RELEASED_FOR_EXECUTION = True      # root approved narrow adapter + 27 pure checks

P23_GUARD = HERE.parent / "business-platform-p2-3" / "p23_live_guard.py"
P24_GUARD = HERE.parent / "business-platform-p2-4" / \
    "p24_live_guard.py"
# frozen suites and their EXACT inner-guard invocation counts (read
# from the frozen sources: smoke_p23.py Q = 1; smoke_p24.py g4 restart
# probe = 2 — _guard4 at guard_pre + immediately pre-stop)
SUITE_BASES = ("business-platform-p2-3", "business-platform-p2-4")
DEFAULT_EXPECT = {"smoke_p23.py": 1, "smoke_p24.py": 2}

RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
                     r"(\.\d+)?Z$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
CANDIDATE_SOURCE_SHA = "a1bff07f3760fce7b5661b169ba26a165712c950035d" \
                       "4e4ec9948639d51ffd27"


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_deployment_record(rec_path, sel_build_path):
    """Verify record + exact selected-build bytes before suite/guard IO."""
    problems = []
    if not rec_path or not sel_build_path:
        raise SystemExit("[p25-adapter] deployment record and selected-build "
                         "are mandatory")
    try:
        rec = json.loads(Path(rec_path).read_text())
        sel = json.loads(Path(sel_build_path).read_text())
    except (OSError, ValueError, TypeError) as exc:
        raise SystemExit(f"[p25-adapter] deployment inputs unreadable ({exc})")
    if not isinstance(rec, dict) or not isinstance(sel, dict):
        raise SystemExit("[p25-adapter] deployment inputs must be objects")
    if rec.get("schema") != "p25-deployment-record-v1":
        problems.append("record schema != p25-deployment-record-v1")
    boot = rec.get("connector_boot")
    if not isinstance(boot, str) or not RFC3339.fullmatch(boot):
        problems.append("connector_boot not RFC3339")
    image = rec.get("connector_image")
    if not isinstance(image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        problems.append("connector_image not full sha256")
    if rec.get("source_sha256") != CANDIDATE_SOURCE_SHA:
        problems.append("record source_sha256 != reviewed candidate")
    if sel.get("schema") != "p25-selected-build-v1" or \
            sel.get("ok") is not True or sel.get("all_ok") is not True:
        problems.append("selected-build schema/ok/all_ok invalid")
    if sel.get("source_sha256") != CANDIDATE_SOURCE_SHA:
        problems.append("selected-build source != reviewed candidate")
    images = sel.get("images")
    required = {"connector", "dataset", "pipeline", "connector2",
                "connector3", "p25connector", "python-3.12-alpine"}
    if not isinstance(images, dict) or set(images) != required:
        problems.append("selected-build images must be exact full seven-image map")
    else:
        if any(not isinstance(v, str) or not re.fullmatch(
                r"sha256:[0-9a-f]{64}", v) for v in images.values()):
            problems.append("selected-build image not full sha256")
        if image != images["connector"]:
            problems.append("record image != selected-build connector")
    if rec.get("selected_build_sha256") != sha256(sel_build_path):
        problems.append("record selected_build_sha256 != actual selected-build bytes")
    selected_path = rec.get("selected_build_path")
    if not isinstance(selected_path, str) or \
            Path(selected_path).resolve() != Path(sel_build_path).resolve():
        problems.append("record selected_build_path != supplied selected-build")
    if problems:
        for problem in problems:
            print(f"[p25-adapter] deployment record BLOCK: {problem}", file=sys.stderr)
        raise SystemExit("[p25-adapter] refusing to run the suite with "
                         "an unverified deployment record")
    return rec


class GuardSubstitute:
    """subprocess-module facade adapting ONLY the exact guard argv.

    Every guard child is spawned with the steady env derived from the
    VERIFIED deployment record (merged over any env the frozen suite
    itself passed — the record wins, it is the reviewed truth)."""

    def __init__(self, real, state, guard_env):
        self._real = real
        self._state = state
        self._guard_env = dict(guard_env)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def run(self, cmd, *args, **kwargs):
        if isinstance(cmd, (list, tuple)) and len(cmd) == 5 \
                and cmd[0] == "python3" and cmd[1] == "-I" \
                and str(cmd[2]) in (str(P23_GUARD), str(P24_GUARD)) \
                and cmd[4] == "connector":
            repl = ["python3", "-I", str(SHIM), str(cmd[3]), cmd[4]]
            kwargs = dict(kwargs)
            suite_env = kwargs.get("env")
            if suite_env is not None and not isinstance(suite_env, dict):
                raise TypeError("guard env must be a dictionary or None")
            kwargs["env"] = {**os.environ, **(suite_env or {}),
                             **self._guard_env}
            proc = self._real.run(repl, *args, **kwargs)
            entry = {"original_argv": [str(x) for x in cmd],
                     "replacement_argv": repl,
                     "guard_sha256": sha256(cmd[2]),
                     "shim_sha256": sha256(SHIM),
                     "rc": proc.returncode,
                     "stdout": proc.stdout,
                     "stderr": proc.stderr,
                     "stdout_sha256": hashlib.sha256(
                         (proc.stdout or "").encode()).hexdigest(),
                     "stderr_sha256": hashlib.sha256(
                         (proc.stderr or "").encode()).hexdigest(),
                     "ts": round(time.time(), 6)}
            self._state.append(entry)
            print(f"[p25-adapter] guard substitution #{len(self._state)}"
                  f" rc={proc.returncode} (steady, record-bound)")
            return proc
        if isinstance(cmd, (list, tuple)) \
                and any(str(g) in " ".join(map(str, cmd))
                        for g in (P23_GUARD, P24_GUARD)) \
                and "live_guard" in " ".join(map(str, cmd)):
            raise RuntimeError(
                f"guard-looking argv not the exact 5-token shape — "
                f"refusing to silently adapt or pass: {cmd}")
        return self._real.run(cmd, *args, **kwargs)


def resolve_suite(suite, suite_dir=None):
    if suite_dir is not None:
        cand = Path(suite_dir) / suite
        return cand if cand.is_file() else None
    for base in SUITE_BASES:
        cand = HERE.parent / base / suite
        if cand.is_file():
            return cand
    return None


def main():
    if not RELEASED_FOR_EXECUTION:
        print("[p25-adapter] DRAFT: not released — root must review "
              "adapter + shim before any suite rerun", file=sys.stderr)
        return 3
    if "--" not in sys.argv:
        print("usage: p25_old_suite_adapter.py [--suite smoke_pXX.py] "
              "[--expect-adaptations N] [--deployment-record <json>] "
              "[--selected-build <json>] [--record-dir <dir>] "
              "[--suite-dir <dir>] -- <frozen suite args>", file=sys.stderr)
        return 2
    argv = sys.argv[1:]
    sep = argv.index("--")
    own, suite_argv = argv[:sep], argv[sep + 1:]

    def opt(name, default=None):
        return Path(own[own.index(name) + 1]) \
            if name in own else default

    suite = opt("--suite", "smoke_p23.py")
    expect = DEFAULT_EXPECT.get(str(suite))
    if "--expect-adaptations" in own:
        expect = int(own[own.index("--expect-adaptations") + 1])
    if expect is None:
        print(f"[p25-adapter] no expected guard-invocation count for "
              f"suite {suite} — pass --expect-adaptations explicitly",
              file=sys.stderr)
        return 2
    suite_dir = opt("--suite-dir")
    rec_path = opt("--deployment-record")
    sel_path = opt("--selected-build")
    record_dir = opt("--record-dir",
                     HERE / "results" / "adapter-records")

    # verify the deployment record (steady truth) BEFORE loading the
    # frozen suite — fail closed, no suite side effects on bad records
    rec = verify_deployment_record(rec_path, sel_path)
    guard_env = {"P25_SHIM_POLICY": "steady",
                 "P25_TARGET_BOOT": rec["connector_boot"],
                 "P25_TARGET_IMAGE": rec["connector_image"],
                 "P25_DEPLOYMENT_RECORD": str(rec_path),
                 "P25_SELECTED_BUILD": str(sel_path)}

    suite_path = resolve_suite(str(suite), suite_dir)
    if suite_path is None:
        print(f"[p25-adapter] frozen suite not found: {suite}",
              file=sys.stderr)
        return 1
    spec = importlib.util.spec_from_file_location("frozen_suite",
                                                  suite_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    state = []
    real_sub = subprocess
    mod.subprocess = GuardSubstitute(real_sub, state, guard_env)
    rc = raised = None
    old_argv = sys.argv          # frozen P23/P24 mains accept NO argv
    sys.argv = ["frozen-suite"] + list(suite_argv)
    try:
        rc = mod.main()
    except SystemExit as exc:            # frozen mains often sys.exit
        rc = exc.code if isinstance(exc.code, int) else 1
    except Exception as exc:                       # noqa: BLE001
        raised = f"{exc.__class__.__name__}: {exc}"[:200]
    finally:
        sys.argv = old_argv
        mod.subprocess = real_sub
    record = {
        "schema": "p25-adapter-v2", "suite": str(suite_path),
        "suite_sha256": sha256(suite_path),
        "deployment_record": str(rec_path) if rec_path else None,
        "selected_build_sha256": sha256(sel_path),
        "deployment_record_sha256": sha256(rec_path)
        if rec_path and rec_path.is_file() else None,
        "guard_env_policy": guard_env,
        "expect_adaptations": expect,
        "adaptations": state, "count": len(state),
        "suite_rc": rc, "raised": raised,
        "labeled": "ADAPTED six-state rerun (inner guard → P25 steady "
                   "shim bound to the reviewed deployment record); "
                   "frozen code byte-unchanged"}
    record_dir.mkdir(parents=True, exist_ok=True)
    p = record_dir / f"adapter-{int(time.time())}.json"
    n = 1
    while p.exists():
        p = record_dir / f"adapter-{int(time.time())}-r{n}.json"
        n += 1
    p.write_text(json.dumps(record, indent=1, sort_keys=True) + "\n")
    print(f"[p25-adapter] rc={rc} adaptations={len(state)}/{expect} "
          f"record={p}")
    if len(state) != expect:
        print(f"[p25-adapter] BLOCK: expected EXACTLY {expect} guard "
              f"substitution(s), got {len(state)} — a zero/wrong count "
              "means the frozen guard path did not really run",
              file=sys.stderr)
        return 1
    if "p23" in str(suite) and len(state) != 1:
        print("[p25-adapter] P23 Q must adapt EXACTLY ONE guard call",
              file=sys.stderr)
        return 1
    return rc if isinstance(rc, int) else 1


if __name__ == "__main__":
    sys.exit(main())
