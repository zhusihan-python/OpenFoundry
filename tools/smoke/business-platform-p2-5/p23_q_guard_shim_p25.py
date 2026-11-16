#!/usr/bin/env python3
"""P25 positional-CLI SHIM for old-suite inner guards (DRAFT).

The frozen P23/P24 suites invoke their inner guard with the positional
argv [python3, -I, <guard.py>, <q3v9dir>, connector]. In SIX-state that
guard (2-pin/4-pin) is no longer the release gate; the P25 reviewed
equivalent is p25_cohort_guard. This shim keeps the OLD argv contract
and forwards to the P25 guard — refusing unless the P25 guard is
released.

Steady policy (round3, review-runtime-tools-round2 #2): the reviewed
deployment record is the ONLY trusted source of the target boot/image.
When P25_DEPLOYMENT_RECORD is set (the adapter always sets it after
verifying the record), the shim RE-VERIFIES the record here (schema,
RFC3339 boot, full-64-hex image, reviewed candidate source binding)
and derives --target-boot/--target-image from IT; bare
P25_TARGET_BOOT/P25_TARGET_IMAGE env values that disagree with the
record are refused. Without a record env, steady still accepts explicit
P25_TARGET_BOOT/P25_TARGET_IMAGE (guard's own two-argument discipline),
and first-transition (no target args) remains the default.

Read-only; never writes; exits with guard semantics (0 ok / 1 block /
2 bad usage / 3 not-released).
"""

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
                     r"(\.\d+)?Z$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
CANDIDATE_SOURCE_SHA = "a1bff07f3760fce7b5661b169ba26a165712c950035d" \
                       "4e4ec9948639d51ffd27"


def fail(msg, rc):
    print(f"[p25-shim] {msg}", file=sys.stderr)
    return rc


def verify_record_env(env):
    """Revalidate the exact adapter-selected record/build bytes in child."""
    path = env.get("P25_DEPLOYMENT_RECORD")
    selected = env.get("P25_SELECTED_BUILD")
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "p25_adapter_record_check", HERE / "p25_old_suite_adapter.py")
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    try:
        rec = adapter.verify_deployment_record(path, selected)
    except SystemExit as exc:
        fail(str(exc), 2)
        return None, None
    boot, image = rec["connector_boot"], rec["connector_image"]
    if env.get("P25_TARGET_BOOT") != boot or \
            env.get("P25_TARGET_IMAGE") != image:
        fail("target boot/image must exactly equal verified record", 2)
        return None, None
    return boot, image


def main(guard=None, env=None):
    if len(sys.argv) != 3:
        print("usage: p23_q_guard_shim_p25.py <env_dir> <service>",
              file=sys.stderr)
        return 2
    if guard is None:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "p25_guard_shim", HERE / "p25_cohort_guard.py")
        guard = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(guard)
    if not guard.RELEASED_FOR_EXECUTION:
        print("[p25-shim] P25 guard not released — refusing (rc3)",
              file=sys.stderr)
        return 3
    import os
    env = os.environ if env is None else env
    if env.get("P25_SHIM_POLICY") != "steady":
        return fail("old-suite shim requires record-bound steady policy", 2)
    boot, image = verify_record_env(env)
    if boot is None:
        return 2
    argv = ["p25_cohort_guard.py", "--policy", "steady",
            "--env-dir", sys.argv[1], "--service", sys.argv[2],
            "--target-boot", boot, "--target-image", image]
    old_argv = sys.argv
    try:
        sys.argv = argv
        return guard.main()
    finally:
        sys.argv = old_argv


if __name__ == "__main__":
    sys.exit(main())
