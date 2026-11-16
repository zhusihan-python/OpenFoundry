#!/usr/bin/env python3
"""Local (no-stack, no-docker) selftest of smoke_p23._SourceRowLock's
I/O discipline — the root T finding: a default BufferedReader.readline
PREFETCHES the following pid line into the Python-side buffer, the raw
fd then looks empty to select() and the next wait spuriously times out.

The fake child writes BOTH lines (uuid row + pid) in ONE os.write and
keeps stdin open until the parent closes it — exactly the trap shape.
The selftest proves acquire() finds the pid and release() terminates
with rc 0 (plus idempotence). Run: python3 -I selftest_lock_holder.py.
"""

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("p23smoke",
                                              HERE / "smoke_p23.py")
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)

FAKE_CHILD = r"""
import os, sys
payload = b"11111111-1111-1111-1111-111111111111\n4242\n"
os.write(1, payload)          # BOTH lines in one write (prefetch trap)
for line in sys.stdin.buffer:  # hold stdin open until the parent closes
    pass
sys.exit(0)
"""

PASS = 0
FAIL = 0


def check(label, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"ok   {label} ({got!r})")
    else:
        FAIL += 1
        print(f"FAIL {label} got={got!r} want={want!r}")


def fake_cmd():
    return [sys.executable, "-c", FAKE_CHILD]


holder = S._SourceRowLock("sid", cmd_factory=fake_cmd)
pid = holder.acquire(timeout=10)
check("acquire-finds-pid-after-multi-line-write", pid, 4242)
check("leftover-buffer-empty", holder._buf, b"")

rc = holder.release(timeout=10)
check("release-terminates-rc0", rc, 0)
check("release-idempotent", holder.release(), None)

# a child that dies WITHOUT ever writing a numeric line must make
# acquire() FAIL (bounded), not hang
DEAD_CHILD = "import sys; sys.stdout.buffer.write(b'nope\\n'); sys.exit(7)"
holder2 = S._SourceRowLock("sid",
                           cmd_factory=lambda: [sys.executable, "-c",
                                                DEAD_CHILD])
try:
    holder2.acquire(timeout=6)
    check("dead-child-acquire-fails", "returned", "AssertionError")
except AssertionError as exc:
    check("dead-child-acquire-fails", "AssertionError" in type(exc).__name__,
          True)
rc2 = holder2.release(timeout=10)
check("dead-child-release-still-reaps", isinstance(rc2, int), True)

print(f"\n[p23-lock-holder-selftest] pass={PASS} fail={FAIL}")
sys.exit(1 if FAIL else 0)
