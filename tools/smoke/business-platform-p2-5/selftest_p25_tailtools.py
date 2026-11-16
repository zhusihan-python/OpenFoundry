#!/usr/bin/env python3
"""DEPRECATED (round3): superseded by selftest_p25_runtime_tools.py,
which drives the ACTUAL CLI / adapter.main / shim paths for the
Tools-owned runtime files. This stub delegates so historical runner
references keep working; the old in-process toy tests were removed
because the runtime-tools round3 review requires real-path negatives
(A3/B3/D/G) and the Archive-track files changed shape."""

import runpy
import sys
from pathlib import Path

sys.exit(runpy.run_path(
    str(Path(__file__).resolve().parent /
        "selftest_p25_runtime_tools.py"),
    run_name="__main__"))
