#!/usr/bin/env python3
"""Swap precondition: refuse while ANY controlled task is still
pending/running (strict-numeric count; read-only)."""
import subprocess
import sys

tmp = sys.argv[1]
sql = ("SELECT count(*) FROM sync_runs WHERE control_mode = 'controlled' "
       "AND status IN ('pending','running');")
proc = subprocess.run(
    ["docker", "compose", "-f", f"{tmp}/compose.yaml",
     "--project-name", "openfoundry-platform-p1-q3v9", "exec", "-T",
     "postgres", "psql", "-U", "openfoundry", "-d", "connectors",
     "-t", "-A", "-c", sql], capture_output=True, text=True, timeout=60)
if proc.returncode != 0:
    print("swap-guard psql rc", proc.returncode)
    raise SystemExit(1)
n = proc.stdout.strip()
if not n.isdigit():
    print("swap-guard stdout not numeric:", repr(n))
    raise SystemExit(1)
print("controlled in-flight:", n)
raise SystemExit(0 if n == "0" else 1)
