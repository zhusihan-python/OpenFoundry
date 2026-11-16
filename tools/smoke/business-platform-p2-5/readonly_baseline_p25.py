#!/usr/bin/env python3
"""P2.5 OWNED readonly-baseline wrapper (2026-11-14).

Same discipline as the frozen P24 wrapper (readonly_baseline_p24.py),
which we do NOT invoke — it writes into the frozen P24 results tree.
This wrapper imports the FROZEN P2.2 readonly_baseline.py STEP
FUNCTIONS read-only (the file is never modified) and writes the
manifest into a P25-OWNED directory only. Flow mirrors the frozen
main() 1:1; only the output location and collision-safety differ.

Output: <out-root>/<stamp>-readonly-baseline/manifest.json
(collision-safe -rN; prints BASELINE_DIR= on the last stdout line).

Usage: python3 -I readonly_baseline_p25.py --env-dir <tmp> \
          [--out-root <p25-ev-subdir>]
Exit 0 iff every recorded step is ok (failures still archived).
"""

import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
P22 = HERE.parent / "business-platform-p2-2"
OUT_DEFAULT = HERE / "results" / "2026-11-14-p25-final" / \
    "readonly-baselines"


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_baseline(env_dir: Path, out_root: Path) -> int:
    base = load_module(P22 / "readonly_baseline.py", "p22_baseline")
    base.STEPS.clear()

    # same P1-smoke bootstrap the frozen main() performs (in-memory only)
    m1 = load_module(base.P1_SMOKE, "p1smoke")
    m1.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    m1.load_env(env_dir)
    base.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    base.M = m1

    stamp = time.strftime("%Y-%m-%dT%H%M%S")
    out_dir = out_root / f"{stamp}-readonly-baseline"
    n = 0
    while out_dir.exists():                       # collision-safe
        n += 1
        out_dir = out_root / f"{stamp}-readonly-baseline-r{n}"
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
        base.record("p21_bc_chain", False,
                    {"skipped": "postgres not running"})
        chain = {}
    base.step_fixture_counts(up["fixture"])
    base.step_legacy_targets_live(chain, up["dataset"])
    base.step_p1_old_artifact(up["dataset"])
    base.step_p1_old_buildjob(up["pipeline"])

    manifest = {"client_date": time.strftime("%Y-%m-%d"),
                "note": "P2.5 owned wrapper: frozen P2.2 read-only "
                        "steps, P25-owned output dir; no service was "
                        "started/stopped/restarted/built by this "
                        "collector",
                "wrapper": "readonly_baseline_p25.py",
                "frozen_source": str(P22 / "readonly_baseline.py"),
                "steps": base.STEPS,
                "all_ok": all(s["ok"] for s in base.STEPS)}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2)
                                           + "\n")
    print(f"\n[p25-baseline] all_ok={manifest['all_ok']}")
    print(f"BASELINE_DIR={out_dir}")
    return 0 if manifest["all_ok"] else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir",
                        default="/private/tmp/openfoundry-platform-p1-q3v9")
    parser.add_argument("--out-root", default=str(OUT_DEFAULT))
    args = parser.parse_args()
    return run_baseline(Path(args.env_dir), Path(args.out_root))


if __name__ == "__main__":
    sys.exit(main())
