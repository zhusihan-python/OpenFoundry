#!/bin/sh
#
# P2.2 guard self-test — synthetic go-test logs only. No stack, no
# services, no docker: this validates the SHIPPED classifier logic of
# run_tests_p22.sh (guard-probe + fail-closed integration) before the
# root session green-lights any real execution.
#
# Usage: sh selftest_guard.sh   (exit 0 iff every case classifies right)
#
set -eu
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GUARD="$SCRIPT_DIR/run_tests_p22.sh"
WORK="$(mktemp -d /tmp/p22-guard-selftest.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

pass=0
fail=0

# expect_guard <want_status 0|1> <label> <log-file> <name>
expect_guard() {
  want="$1"; label="$2"; logf="$3"; name="$4"
  out=$(sh "$GUARD" guard-probe "$logf" "$name" || true)
  got=$(printf '%s\n' "$out" | sed -n 's/^GUARD_STATUS=//p')
  if [ "$got" = "$want" ]; then
    pass=$((pass + 1))
    printf 'ok   %s -> GUARD_STATUS=%s\n' "$label" "$got"
  else
    fail=$((fail + 1))
    printf 'FAIL %s -> GUARD_STATUS=%s (want %s)\n' "$label" "$got" "$want"
  fi
}

# expect_rc <want_rc> <label> <command...>
expect_rc() {
  want="$1"; label="$2"; shift 2
  if "$@" >/dev/null 2>&1; then got=0; else got=$?; fi
  if [ "$got" = "$want" ]; then
    pass=$((pass + 1))
    printf 'ok   %s -> rc=%s\n' "$label" "$got"
  else
    fail=$((fail + 1))
    printf 'FAIL %s -> rc=%s (want %s)\n' "$label" "$got" "$want"
  fi
}

N="TestPGWorkerSample"

# 1. top-level RUN + PASS -> ok
cat > "$WORK/pass.log" <<EOF
=== RUN   $N
--- PASS: $N (0.12s)
EOF
expect_guard 0 "run+pass" "$WORK/pass.log" "$N"

# 2. SKIP takes priority over everything -> FAIL
cat > "$WORK/skip.log" <<EOF
=== RUN   $N
--- SKIP: $N (0.12s)
EOF
expect_guard 1 "skip-priority" "$WORK/skip.log" "$N"

# 3. FAIL -> FAIL
cat > "$WORK/fail.log" <<EOF
=== RUN   $N
--- FAIL: $N (0.12s)
EOF
expect_guard 1 "fail" "$WORK/fail.log" "$N"

# 4. RUN without terminal PASS (panic/timeout) -> FAIL
cat > "$WORK/panic.log" <<EOF
=== RUN   $N
panic: simulated worker panic [recovered]
EOF
expect_guard 1 "run-no-pass" "$WORK/panic.log" "$N"

# 5. name entirely missing from the log -> FAIL
cat > "$WORK/other.log" <<EOF
=== RUN   TestPGUnrelated
--- PASS: TestPGUnrelated (0.05s)
EOF
expect_guard 1 "missing" "$WORK/other.log" "$N"

# 6. child-only RUN + parent PASS (no top-level RUN) -> FAIL
cat > "$WORK/childonly.log" <<EOF
=== RUN   $N/sub_case
--- PASS: $N (0.20s)
EOF
expect_guard 1 "child-run-parent-pass" "$WORK/childonly.log" "$N"

# 7. PASS line without any RUN -> FAIL
cat > "$WORK/passnorun.log" <<EOF
--- PASS: $N (0.10s)
EOF
expect_guard 1 "pass-without-run" "$WORK/passnorun.log" "$N"

# 8. prefix trap: TestPGWorkerSampleExtra must NOT satisfy TestPGWorkerSample
cat > "$WORK/prefix.log" <<EOF
=== RUN   ${N}Extra
--- PASS: ${N}Extra (0.10s)
EOF
expect_guard 1 "prefix-trap" "$WORK/prefix.log" "$N"

# 9. sub-SKIP under a genuinely passing parent (top-level RUN present) -> ok
cat > "$WORK/subskip.log" <<EOF
=== RUN   $N
=== RUN   $N/gated_external
    worker_matrix_test.go:97: external dependency not provisioned
--- SKIP: $N/gated_external (0.00s)
--- PASS: $N (0.31s)
EOF
expect_guard 0 "sub-skip-under-passing-parent" "$WORK/subskip.log" "$N"

# 10. integration mode fails CLOSED without a names file
rm -f "$WORK/absent-names.txt"
P22_NAMES_FILE="$WORK/absent-names.txt" \
  expect_rc 1 "integration-no-names-file" \
  sh "$GUARD" integration "$WORK/pass.log"

# 11. integration mode fails CLOSED on an empty (comments-only) names file
cat > "$WORK/empty-names.txt" <<EOF
# root finalization pending — no real names yet
EOF
P22_NAMES_FILE="$WORK/empty-names.txt" \
  expect_rc 1 "integration-empty-names" \
  sh "$GUARD" integration "$WORK/pass.log"

# 12. integration mode end-to-end: one real name over a good log -> rc 0
printf '%s\n' "$N" > "$WORK/one-name.txt"
P22_NAMES_FILE="$WORK/one-name.txt" \
  expect_rc 0 "integration-good-log" \
  sh "$GUARD" integration "$WORK/pass.log"

# 13. integration mode end-to-end: required name SKIPped in the log -> rc 1
P22_NAMES_FILE="$WORK/one-name.txt" \
  expect_rc 1 "integration-skipped-name" \
  sh "$GUARD" integration "$WORK/skip.log"

# 14. the SHIPPED root-pinned names file, end-to-end (no override):
# a synthetic log carrying EVERY listed name RUN+PASS must bless rc 0.
# (unset first: `VAR=x func` assignments PERSIST after a function call
# under /bin/sh posix mode, so cases 12/13's override would leak here)
unset P22_NAMES_FILE
REAL_NAMES="$(sed -e 's/#.*//' "$SCRIPT_DIR/p22-required-names.txt" \
  | grep -v '^[[:space:]]*$')"
NAMES_COUNT="$(printf '%s\n' "$REAL_NAMES" | grep -c .)"
{
  for name in $REAL_NAMES; do
    printf '=== RUN   %s\n--- PASS: %s (0.21s)\n' "$name" "$name"
  done
} > "$WORK/all-shipped.log"
expect_rc 0 "shipped-names-all-pass (n=${NAMES_COUNT})" \
  sh "$GUARD" integration "$WORK/all-shipped.log"

# 15. same shipped file, log MISSING one name -> rc 1 (no silent pass)
first_name=$(printf '%s\n' "$REAL_NAMES" | head -1)
grep -v "^=== RUN   ${first_name}\$\|^--- PASS: ${first_name} " "$WORK/all-shipped.log" \
  > "$WORK/minus-one.log"
expect_rc 1 "shipped-names-missing-one" sh "$GUARD" integration "$WORK/minus-one.log"

# 16. duplicate lines in a names file collapse to distinct (count rule)
printf '%s\n%s\n' "$N" "$N" > "$WORK/dup-names.txt"
P22_NAMES_FILE="$WORK/dup-names.txt" \
  expect_rc 0 "duplicate-names-collapse" \
  sh "$GUARD" integration "$WORK/pass.log"

printf '\n[p22-guard-selftest] pass=%d fail=%d\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
