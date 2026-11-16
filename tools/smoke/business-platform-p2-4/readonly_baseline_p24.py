#!/usr/bin/env python3
"""P2.4 OWNED wrapper around the FROZEN P2.2 readonly baseline (2026-11-14).

Root requirement: baseline/reread must NEVER create new dirs under
P22/results/preparation again. This wrapper reuses the frozen
readonly_baseline.py STEP FUNCTIONS unchanged (imported read-only — the
file itself is never modified) but writes the manifest into a P24-owned
directory. Flow mirrors the frozen main() exactly; only the output
location and collision-safety differ.

Output: <p24-results>/<stamp>-readonly-baseline/manifest.json
(collision-safe -rN; prints the absolute dir on the LAST stdout line so
the formal runner can capture it without glob freshness tricks).

Usage: python3 -I readonly_baseline_p24.py [--env-dir <tmp>]
Exit 0 iff every recorded step is ok (failures still archived).
"""

import argparse
import importlib.util
import json
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
P22 = HERE.parent / "business-platform-p2-2"
OUT_ROOT = HERE / "results" / "2026-11-14-p24-final" / "readonly-baselines"


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir",
                        default="/private/tmp/openfoundry-platform-p1-q3v9")
    args = parser.parse_args()
    env_dir = Path(args.env_dir)

    base = load_module(P22 / "readonly_baseline.py", "p22_baseline")
    base.STEPS.clear()

    # same P1-smoke bootstrap the frozen main() performs (in-memory only)
    m1 = load_module(base.P1_SMOKE, "p1smoke")
    m1.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    m1.load_env(env_dir)
    base.ARGS = args
    base.M = m1

    stamp = time.strftime("%Y-%m-%dT%H%M%S")
    out_dir = OUT_ROOT / f"{stamp}-readonly-baseline"
    n = 0
    while out_dir.exists():                       # collision-safe
        n += 1
        out_dir = OUT_ROOT / f"{stamp}-readonly-baseline-r{n}"
    out_dir.mkdir(parents=True)

    # flow below mirrors the frozen main() 1:1 (steps are ITS functions)
    services = base.step_stack_ps(env_dir)
    expected = ("postgres", "dataset", "pipeline", "fixture")
    up = {svc: any(name.endswith(f"-{svc}-1") and info.get("state") == "running"
                   for name, info in services.items())
          for svc in expected}
    base.record("service_up_map", bool(up) and all(up.values()),
                {"expected": list(expected), **up})
    base.step_env_guard(env_dir)
    if up["postgres"]:
        base.step_legacy_projections()
        chain = base.step_legacy_chain()
    else:
        base.record("p21_bc_projection", False,
                    {"skipped": "postgres not running"})
        base.record("p21_bc_chain", False, {"skipped": "postgres not running"})
        chain = {}
    base.step_fixture_counts(up["fixture"])
    base.step_legacy_targets_live(chain, up["dataset"])
    base.step_p1_old_artifact(up["dataset"])
    base.step_p1_old_buildjob(up["pipeline"])

    manifest = {"client_date": time.strftime("%Y-%m-%d"),
                "note": "P2.4 owned wrapper: frozen P2.2 read-only steps, "
                        "P24-owned output dir; no service was started/"
                        "stopped/restarted/built by this collector",
                "wrapper": "readonly_baseline_p24.py",
                "frozen_source": str(P22 / "readonly_baseline.py"),
                "steps": base.STEPS,
                "all_ok": all(s["ok"] for s in base.STEPS)}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2)
                                           + "\n")
    print(f"\n[p24-baseline] all_ok={manifest['all_ok']}")
    print(f"BASELINE_DIR={out_dir}")
    return 0 if manifest["all_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
