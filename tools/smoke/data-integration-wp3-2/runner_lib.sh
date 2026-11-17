#!/usr/bin/env bash
# Shared runner helpers for the WP3.2 acceptance suite (B worker).
# SOURCED (never executed) by run.sh and negative_controls.sh so the
# negative controls exercise the exact production wrapper logic instead
# of a re-implementation.
#
# Wrapper model (root review round 2 — persist-then-assert):
#   1. UNCONDITIONALLY persist BOTH raw statuses (command rc and tee
#      rc) to $EXIT_CODES and the step logfile, even when the command
#      already failed or tee died — a swallowed tee rc can never hide a
#      failed log write, and a failed command can never hide the tee
#      status.
#   2. ONLY THEN apply the assertion:
#        run_step        — both rcs must be 0;
#        expect_fail_step— command rc must be NON-zero AND tee rc 0
#                          (the log write itself must still succeed);
#                          a zero command rc fails the control.
#
# Caller contract: set HERE (this directory) and EXIT_CODES (writable
# file path) before calling any function below.

# persist_raw <step> <cmd_rc> <tee_rc>: append both raw statuses.
persist_raw() {
  echo "$1 = cmd_rc:$2 tee_rc:$3" >>"$EXIT_CODES"
  echo "[rc] $1 cmd_rc:$2 tee_rc:$3"
}

# fail_run <step> <rc>: dump what we have and exit non-zero — even
# when the triggering raw status was the tee rc while the command rc
# was 0 (root review round 2 control C2).
fail_run() {
  local rc="${2:-1}"
  [[ $rc -ne 0 ]] || rc=1
  echo "RUNNER FAILED at step '$1'; raw codes so far:" >&2
  cat "$EXIT_CODES" >&2
  exit "$rc"
}

# run_step <step> <logfile> <cmd...>: full log via tee; the trailing
# grep is display-only (guarded || true INSIDE the display layer).
run_step() {
  local step="$1" logf="$2"; shift 2
  set +e
  "$@" 2>&1 | tee "$logf" | { grep -E '^(ok|FAIL|---|\[gate\]|\[case\]|\[rc\]|verify:|\[negctl\]|\[phase\]|\[evidence\])' || true; }
  local -a st=("${PIPESTATUS[@]}")
  set -e
  persist_raw "$step" "${st[0]}" "${st[1]}"
  if [[ ${st[0]} -ne 0 || ${st[1]} -ne 0 ]]; then
    fail_run "$step" "${st[0]}"
  fi
}

# expect_fail_step <step> <logfile> <cmd...>: negative-control wrapper.
# The command MUST exit non-zero while tee still succeeds at writing
# the log (tee_rc 0). Both raw statuses are persisted first.
expect_fail_step() {
  local step="$1" logf="$2"; shift 2
  set +e
  "$@" 2>&1 | tee "$logf" >/dev/null
  local -a st=("${PIPESTATUS[@]}")
  set -e
  persist_raw "$step" "${st[0]}" "${st[1]}"
  if [[ ${st[1]} -ne 0 ]]; then
    echo "[negctl] $step: tee itself failed (rc=${st[1]}) — evidence not captured" | tee -a "$logf" >/dev/null
    fail_run "$step-tee" "${st[1]}"
  fi
  if [[ ${st[0]} -eq 0 ]]; then
    echo "[negctl] $step: expected non-zero command exit, got 0" | tee -a "$logf" >/dev/null
    fail_run "$step" 1
  fi
  echo "[negctl] $step: rejected as expected (cmd_rc=${st[0]}, tee_rc=${st[1]})" | tee -a "$logf" >/dev/null
}

# verify_step <step> <verify_args...>: strict verifier; its raw rc is
# persisted before failing.
verify_step() {
  local step="$1"; shift
  set +e
  python3 "$HERE/verify_results.py" "$@"
  local rc=$?
  set -e
  persist_raw "$step" "$rc" 0
  if [[ $rc -ne 0 ]]; then
    fail_run "$step" "$rc"
  fi
}

# kc: kubectl pinned to the ofwp32 isolated kubeconfig — never the
# user's default context.
kc() {
  kubectl --kubeconfig "$KUBECONFIG_OFWP32" "$@"
}
