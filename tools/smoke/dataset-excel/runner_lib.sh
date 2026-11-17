#!/usr/bin/env bash
# Shared runner helpers for the dataset-excel acceptance suite. SOURCED
# (never executed) by run.sh and negative_controls.sh so the negative
# controls exercise the exact production wrapper logic, never a copy.
# Pattern adopted from tools/smoke/data-integration-s1-security/runner_lib.sh.
#
# Caller contract: set HERE (this directory) and EXIT_CODES (writable file
# path) before calling any function below. NEGCTL_ERR (writable scratch file
# outside the source tree) is derived from EXIT_CODES if unset.

negctl_err_file() {
  echo "${NEGCTL_ERR:-$(dirname "$EXIT_CODES")/.negctl-last.err}"
}

record_rc() { # record_rc <step> <rc>: append raw rc; non-zero aborts the runner
  local step="$1" rc="$2"
  echo "$step = $rc" >>"$EXIT_CODES"
  echo "[rc] $step -> $rc"
  if [[ $rc -ne 0 ]]; then
    echo "RUNNER FAILED at step '$step' (raw exit code $rc); raw codes so far:"
    cat "$EXIT_CODES" >&2
    exit "$rc"
  fi
}

# run_step <step> <logfile> <cmd...>: full log via tee; the trailing grep is
# display-only (guarded || true INSIDE the display layer). The pipeline
# status array is captured ONCE, immediately after the pipeline, and BOTH the
# command's rc ([0]) and tee's rc ([1]) are recorded and enforced — a failed
# log write can never be swallowed. Element [2] (display grep) is
# intentionally unchecked: no-match is fine for a display filter.
run_step() {
  local step="$1" logf="$2"; shift 2
  set +e
  "$@" 2>&1 | tee "$logf" | { grep -E '^(ok|FAIL|---|\[rc\]|verify:|\[negctl\]|\[excel\]|\[stack\]|Running |PASSED|failed)' || true; }
  local -a st=("${PIPESTATUS[@]}")
  set -e
  record_rc "$step" "${st[0]}"
  record_rc "$step-tee" "${st[1]}"
}

# verify_step <step> <verify_args...>: strict verifier with its raw rc
# captured into exit-codes before failing (set -e suspended around the call).
verify_step() {
  local step="$1"; shift
  set +e
  python3 "$HERE/verify_results.py" "$@"
  local rc=$?
  set -e
  record_rc "$step" "$rc"
}

# expect_verifier_reject <step> <verify_args...>: NEGATIVE-control wrapper.
# Runs the strict verifier and REQUIRES it to exit non-zero (bad evidence
# must be rejected). Records 0 iff the rejection happened as expected; a
# verifier ACCEPT of bad evidence aborts the runner (anti-greening).
expect_verifier_reject() {
  local step="$1"; shift
  local errf; errf="$(negctl_err_file)"
  set +e
  python3 "$HERE/verify_results.py" "$@" >/dev/null 2>"$errf"
  local rc=$?
  set -e
  if [[ $rc -eq 0 ]]; then
    echo "[negctl] FAILED: verifier ACCEPTED bad evidence ($step)" >&2
    record_rc "$step-expected-reject" 1
  fi
  echo "[negctl] reject ok: $step — $(head -1 "$errf" 2>/dev/null)"
  record_rc "$step-expected-reject" 0
}
