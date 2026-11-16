#!/usr/bin/env python3
"""P24-owned TRANSPARENT adapter for the frozen P23 full-HTTP Q run
(formal-execution-contract.md §"Frozen P23 Q guard adaptation").

DRAFT — NOT RELEASED. Root reviews adapter+source before any execution
(RELEASED_FOR_EXECUTION below stays False until then).

What it does — and NOTHING else:
  * Loads the FROZEN tools/smoke/business-platform-p2-3/smoke_p23.py
    module unmodified and runs ITS main() with forwarded argv.
  * Substitutes EXACTLY ONE behavioral thing: the Q scenario's inner
    subprocess guard argv
        [python3, -I, <frozen>/p23_live_guard.py, <q3v9dir>, connector]
    is executed as
        [python3, -I, <p24>/p24_live_guard.py, <same dir>, connector]
    (root-reviewed 4-pin guard) — same kwargs (capture_output/text/
    timeout), REAL CompletedProcess/rc/stdout/stderr returned.
  * Any other subprocess call is delegated UNCHANGED to the original
    subprocess module. An argv that references p23_live_guard.py but
    does not match the exact 5-token shape is REJECTED loudly (never
    silently adapted, never silently passed).
  * Archives the adaptation record: original argv, replacement argv,
    sha256 of BOTH guard files, the real guard rc/stdout/stderr,
    invocation count, and the wrapper smoke rc. A SUCCESSFUL adapted
    full-Q run must contain EXACTLY ONE adaptation (checked on exit).
  * The patched module attribute is restored on exit (try/finally);
    no other globals/HERE/DB filters/results are touched. The result
    is labeled: frozen 8-scenario code full run with P24-only
    guard-command adaptation — NOT byte-unmodified direct execution.

Usage:
  python3 -I p23_q_guard_adapter_p24.py [--record-dir <dir>] -- \
      <smoke_p23.py args, e.g. --env-dir <tmp> --allow-restart>
Exit rc = smoke rc when exactly one adaptation happened, else 1.
"""

import hashlib
import importlib.util
import json
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
P23_TOOLS = HERE.parent / "business-platform-p2-3"
P23_SMOKE = P23_TOOLS / "smoke_p23.py"
P23_GUARD = P23_TOOLS / "p23_live_guard.py"
P24_GUARD = HERE / "p24_live_guard.py"

# ROOT-APPROVED 2026-11-14 (tools-execution-next.md: "Adapter current
# exactargv/dir/fullJSON stdout+stderr+SHA/exceptionrecord is APPROVED.
# GLM may set RELEASED_FOR_EXECUTION=True. This approval is only the
# guard adapter substitution, not permission to run q3 yet.") — the
# q3 Q run itself still needs the separate runtime release.
RELEASED_FOR_EXECUTION = True


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class AdapterReject(Exception):
    pass


class _SubprocessProxy:
    """Delegates EVERYTHING to the real subprocess module except .run,
    which applies the single approved argv substitution.

    Review bug 3: the match is the EXACT whole argv
        [python3, -I, <frozen p23_live_guard.py>, <exact q3v9 dir>,
         connector]
    including cmd[3] == the env dir the frozen module actually runs
    with; ANY other argv that merely REFERENCES the old guard (any
    position, any shape) is REJECTED — never silently adapted, never
    silently passed through."""

    def __init__(self, real, state, expected_dir_of):
        self._real = real
        self._state = state
        self._expected_dir_of = expected_dir_of

    def __getattr__(self, name):
        return getattr(self._real, name)

    def run(self, cmd, *args, **kwargs):
        references_old = (isinstance(cmd, (list, tuple))
                          and any(str(x) == str(P23_GUARD) for x in cmd))
        if references_old:
            expected_dir = self._expected_dir_of()
            exact = (len(cmd) == 5 and cmd[0] == "python3"
                     and cmd[1] == "-I"
                     and str(cmd[2]) == str(P23_GUARD)
                     and expected_dir is not None
                     and str(cmd[3]) == expected_dir
                     and cmd[4] == "connector")
            if not exact:
                self._state["rejections"].append(
                    {"argv": [str(x) for x in cmd],
                     "expected_dir": expected_dir,
                     "reason": "argv references p23_live_guard.py but is "
                               "not the EXACT frozen Q invocation"})
                raise AdapterReject(
                    f"unexpected p23_live_guard argv form: {cmd}")
            replacement = [cmd[0], cmd[1], str(P24_GUARD), cmd[3], cmd[4]]
            entry = {"original_argv": [str(x) for x in cmd],
                     "replacement_argv": replacement}
            self._state["invocations"].append(entry)
            proc = self._real.run(replacement, *args, **kwargs)
            entry.update({"rc": proc.returncode,
                          "stdout": proc.stdout,
                          "stderr": proc.stderr,
                          "stdout_sha256": hashlib.sha256(
                              (proc.stdout or "").encode()).hexdigest(),
                          "stderr_sha256": hashlib.sha256(
                              (proc.stderr or "").encode()).hexdigest()})
            return proc
        return self._real.run(cmd, *args, **kwargs)


def main():
    if not RELEASED_FOR_EXECUTION:
        print("[q-adapter] DRAFT: not released — root must review this "
              "adapter and set RELEASED_FOR_EXECUTION=True",
              file=sys.stderr)
        return 3
    argv = sys.argv[1:]
    record_dir = HERE / "results" / "2026-11-14-p24-final" / "q-adaptation"
    if "--record-dir" in argv:
        i = argv.index("--record-dir")
        record_dir = Path(argv[i + 1])
        del argv[i:i + 2]
    if argv and argv[0] == "--":
        argv = argv[1:]
    record_dir.mkdir(parents=True, exist_ok=True)

    spec = importlib.util.spec_from_file_location("smoke_p23", P23_SMOKE)
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)

    state = {"invocations": [], "rejections": []}

    def expected_dir_of():
        # the exact q3v9 dir the frozen module is actually running with
        args = getattr(smoke, "ARGS", None)
        d = getattr(args, "env_dir", None) if args else None
        return str(d) if d else None

    proxy = _SubprocessProxy(subprocess, state, expected_dir_of)
    smoke.subprocess = proxy                      # the ONLY patch
    rc = None
    raised = None
    record_path = [None]
    saved_argv = sys.argv

    def write_record(rc_value, raised_value):
        record = {"client_date": time.strftime("%Y-%m-%d"),
                  "frozen_smoke": str(P23_SMOKE),
                  "frozen_smoke_sha256": sha256(P23_SMOKE),
                  "old_guard": {"path": str(P23_GUARD),
                                "sha256": sha256(P23_GUARD)},
                  "new_guard": {"path": str(P24_GUARD),
                                "sha256": sha256(P24_GUARD)},
                  "label": "frozen 8-scenario P23 code with P24-only "
                           "guard-command adaptation (not "
                           "byte-unmodified)",
                  "forwarded_argv": argv,
                  "smoke_rc": rc_value,
                  "smoke_exception": raised_value,
                  "invocation_count": len(state["invocations"]),
                  "invocations": state["invocations"],
                  "rejections": state["rejections"],
                  "exactly_one": len(state["invocations"]) == 1
                                 and not state["rejections"]}
        name = f"q-adaptation-{time.strftime('%Y%m%dT%H%M%S')}.json"
        out = record_dir / name
        n = 0
        while out.exists():
            n += 1
            out = record_dir / re.sub(r"(\.\w+)?$",
                                      rf"-r{n}\1", name, 1)
        out.write_text(json.dumps(record, indent=1) + "\n")
        record_path[0] = out
        print(f"[q-adapter] smoke_rc={rc_value} invocations="
              f"{record['invocation_count']} -> {out}")

    try:
        sys.argv = ["smoke_p23.py"] + argv        # forwarded arguments
        rc = smoke.main() if hasattr(smoke, "main") else 1
    except BaseException as exc:                  # noqa: BLE001
        # review bug 3c: even an exception must leave a failure record
        raised = f"{type(exc).__name__}: {exc}"[:300]
        write_record(None, raised)
        raise
    finally:
        sys.argv = saved_argv
        smoke.subprocess = subprocess             # restore, always
        if record_path[0] is None:
            write_record(rc, raised)
    if record_path[0] is None:
        write_record(rc, raised)
    if not (len(state["invocations"]) == 1 and not state["rejections"]):
        print("[q-adapter] BLOCK: a successful adapted full-Q requires "
              "EXACTLY ONE substitution (and zero rejections)",
              file=sys.stderr)
        return 1
    return rc if isinstance(rc, int) else 1


if __name__ == "__main__":
    sys.exit(main())
