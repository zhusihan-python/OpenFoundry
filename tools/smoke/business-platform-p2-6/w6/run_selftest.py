#!/usr/bin/env python3
"""W6 aggregate self-test runner (pure: python3 -I -B, no docker/HTTP/Go).

Runs each selftest as an ISOLATED subprocess (python3 -I -B), records the
real rc + output into results/w6/selftest-<UTC>/, and exits 0 only when
every selftest exits 0.
"""

import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

HERE = Path(__file__).resolve().parent
RESULTS_ROOT = HERE.parent / "results" / "w6"

SELFTESTS = [
    "selftest_p26_gates.py",
    "selftest_p26_pg_runner.py",
    "selftest_p26_cohort_cli.py",
    "selftest_p26_build_plan.py",
    "selftest_p26_acceptance_tools.py",
    "selftest_p26_deploy_prep.py",
]


def main():
    import datetime
    ts = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ")
    out_dir = RESULTS_ROOT / f"selftest-{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {"started_utc": ts, "selftests": []}
    all_ok = True
    for name in SELFTESTS:
        cp = subprocess.run(
            [sys.executable, "-I", "-B", str(HERE / name)],
            capture_output=True, text=True)
        (out_dir / f"{name}.stdout").write_text(cp.stdout or "")
        (out_dir / f"{name}.stderr").write_text(cp.stderr or "")
        (out_dir / f"{name}.rc").write_text(f"{cp.returncode}\n")
        tail = (cp.stdout or "").strip().splitlines()
        verdict = tail[-1] if tail else "(no output)"
        summary["selftests"].append(
            {"name": name, "rc": cp.returncode,
             "verdict_line": verdict})
        print(f"[w6-selftest] {name}: rc={cp.returncode} — {verdict}")
        if cp.returncode != 0:
            all_ok = False
            print((cp.stderr or "").strip()[-2000:])
    summary["all_ok"] = all_ok
    (out_dir / "SUMMARY.json").write_text(
        __import__("json").dumps(summary, indent=1) + "\n")
    n_ok = sum(1 for s in summary["selftests"] if s["rc"] == 0)
    print(f"[w6-selftest] {n_ok}/{len(SELFTESTS)} selftest files PASS — "
          f"transcript: {out_dir}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
