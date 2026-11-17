#!/usr/bin/env bash
# Bounded negative controls for the S1 acceptance runner (review round 3C).
# Proves the STRICT verifier (verify_results.py) rejects the exact classes of
# bad evidence that used to make run.sh lie green, and that the runner's
# pipeline shape propagates a helper's non-zero exit. Pure-local: fixtures in
# a mktemp dir, no docker, no real stack, nothing archived.
#
# Usage: negative_controls.sh <path-to-verify_results.py>
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFY="$HERE/verify_results.py"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/s1sec-negctl.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# ---------------------------------------------------------------- fixtures --
# One python program builds a MINIMAL VALID baseline for both evidence kinds
# (importing the REQUIRED inventories from verify_results.py itself, so the
# baseline can never drift from the verifier) plus every mutant.
python3 - "$VERIFY" "$TMP" <<'PYEOF'
import json
import re
import sys

sys.path.insert(0, re.sub(r"/[^/]+$", "", sys.argv[1]))
import verify_results as vr  # noqa: E402

tmp = sys.argv[2]

http_cases = []
for name in vr.HTTP_REQUIRED:
    rejection = any(
        k in name for k in
        ("cross-org", "cross-owner", "jwt", "non-uuid", "unknown uuid",
         "switched to org", "against personal", "without grants", "throttled"))
    http_cases.append({
        "name": name,
        "want_status": 403 if rejection and "jwt" not in name else (404 if rejection else 200),
        "got_status": 403 if rejection and "jwt" not in name else (404 if rejection else 200),
        "want_upstream": 0 if rejection else 1,
        "got_upstream": 0 if rejection else 1,
        "want_history": 0 if rejection else 1,
        "got_history": 0 if rejection else 1,
        "route": "POST /api/v1/webhooks/x/invoke",
        "auth": "jwt",
        "pass": True,
    })
http_base = {"run_id": "negctl-run", "all_passed": True, "cases": http_cases}
http_log_ok = "=== RUN   Test\n--- PASS: TestS1SecWebhookTenantIsolationHTTPAgainstPG (0.1s)\nok  pkg 0.1s\n"

gate_cases = []
for name in vr.GATE_REQUIRED:
    if "unreachable DSN" in name:
        gate_cases.append({"name": name, "want": "refusal_before_db",
                           "exit_code": 1, "elapsed_s": 0, "pass": True})
    elif "boots" in name:
        gate_cases.append({"name": name, "want": "healthy_start",
                           "healthy": True, "exit_code": 0, "pass": True})
    else:
        gate_cases.append({"name": name, "want": "refusal",
                           "exit_code": 1, "pass": True})
gate_base = {"run_id": "negctl-run", "all_passed": True, "cases": gate_cases}

def dump(name, doc):
    with open(f"{tmp}/{name}", "w") as fh:
        json.dump(doc, fh)

def clone(doc):
    return json.loads(json.dumps(doc))

dump("http-ok.json", http_base)
with open(f"{tmp}/http-ok.log", "w") as fh:
    fh.write(http_log_ok)
dump("gate-ok.json", gate_base)
with open(f"{tmp}/exitcodes-ok.txt", "w") as fh:
    fh.write("wp21-unit = 0\nwp21-http-pg = 0\nwp22-binary-gate = 0\n")

d = clone(http_base); d["all_passed"] = False; dump("http-allfalse.json", d)
d = clone(http_base); d["all_passed"] = "true"; dump("http-allstr.json", d)
d = clone(http_base); d["cases"] = []; dump("http-emptycases.json", d)
d = clone(http_base); d["cases"] = [c for c in d["cases"] if c["name"] != "cross-org admin invoke"]; dump("http-missing-scenario.json", d)
d = clone(http_base); d["cases"][0]["pass"] = 1; dump("http-intpass.json", d)
d = clone(http_base)
for c in d["cases"]:
    if c["name"] == "cross-org admin invoke":
        c["got_upstream"] = 1
dump("http-reject-side-effect.json", d)
d = clone(http_base); d["cases"][0]["got_status"] = 500; dump("http-status-mismatch.json", d)
d = clone(http_base); d["run_id"] = "other-run"; dump("http-wrong-runid.json", d)
with open(f"{tmp}/http-nopass.log", "w") as fh:
    fh.write("=== RUN   Test\nok  pkg 0.1s\n")            # no --- PASS line
with open(f"{tmp}/http-skip.log", "w") as fh:
    fh.write("--- PASS: TestS1SecWebhookTenantIsolationHTTPAgainstPG (0.1s)\n    --- SKIP: sub (0.0s)\n")

d = clone(gate_base); d["all_passed"] = 1; dump("gate-intall.json", d)
d = clone(gate_base); d["cases"] = [c for c in d["cases"] if "unreachable DSN" not in c["name"]]; dump("gate-missing-order-case.json", d)
d = clone(gate_base)
for c in d["cases"]:
    if "unreachable DSN" in c["name"]:
        c["exit_code"] = 124
dump("gate-order-timeout.json", d)
d = clone(gate_base); d["all_passed"] = False; dump("gate-allfalse.json", d)
with open(f"{tmp}/exitcodes-bad.txt", "w") as fh:
    fh.write("wp21-unit = 0\nwp22-binary-gate = 1\n")
print("[negctl] fixtures built")
PYEOF

# ------------------------------------------------------------- assertions --
must_reject() { # must_reject <label> <args...>
  local label="$1"; shift
  if python3 "$VERIFY" "$@" >/dev/null 2>"$TMP/last-err.txt"; then
    echo "NEGCTL FAIL: verifier ACCEPTED bad evidence ($label)" >&2
    exit 1
  fi
  echo "[negctl] reject ok: $label"
}
must_accept() { # must_accept <label> <args...>
  local label="$1"; shift
  if ! python3 "$VERIFY" "$@" >/dev/null 2>"$TMP/last-err.txt"; then
    echo "NEGCTL FAIL: verifier REJECTED good evidence ($label): $(cat "$TMP/last-err.txt")" >&2
    exit 1
  fi
  echo "[negctl] accept ok: $label"
}

must_reject "http: missing json file"        http "$TMP/does-not-exist.json" "$TMP/http-ok.log" negctl-run
must_reject "http: all_passed=false"          http "$TMP/http-allfalse.json" "$TMP/http-ok.log" negctl-run
must_reject "http: all_passed string 'true'"  http "$TMP/http-allstr.json" "$TMP/http-ok.log" negctl-run
must_reject "http: empty cases"               http "$TMP/http-emptycases.json" "$TMP/http-ok.log" negctl-run
must_reject "http: required scenario missing" http "$TMP/http-missing-scenario.json" "$TMP/http-ok.log" negctl-run
must_reject "http: pass as int 1"             http "$TMP/http-intpass.json" "$TMP/http-ok.log" negctl-run
must_reject "http: rejection with side effects" http "$TMP/http-reject-side-effect.json" "$TMP/http-ok.log" negctl-run
must_reject "http: status mismatch"           http "$TMP/http-status-mismatch.json" "$TMP/http-ok.log" negctl-run
must_reject "http: wrong run_id"              http "$TMP/http-wrong-runid.json" "$TMP/http-ok.log" negctl-run
must_reject "http: log without PASS line"     http "$TMP/http-ok.json" "$TMP/http-nopass.log" negctl-run
must_reject "http: log containing SKIP"       http "$TMP/http-ok.json" "$TMP/http-skip.log" negctl-run
must_reject "gate: all_passed as int 1"       gate "$TMP/gate-intall.json" negctl-run
must_reject "gate: runtime-order case missing" gate "$TMP/gate-missing-order-case.json" negctl-run
must_reject "gate: runtime-order case timed out (124)" gate "$TMP/gate-order-timeout.json" negctl-run
must_reject "gate: all_passed=false"          gate "$TMP/gate-allfalse.json" negctl-run
must_reject "exitcodes: non-zero raw rc"      exitcodes "$TMP/exitcodes-bad.txt"

must_accept "http: valid baseline"            http "$TMP/http-ok.json" "$TMP/http-ok.log" negctl-run
must_accept "gate: valid baseline"            gate "$TMP/gate-ok.json" negctl-run
must_accept "exitcodes: all zero"             exitcodes "$TMP/exitcodes-ok.txt"

# ------------------------------------------------- real-wrapper controls --
# These controls exercise the PRODUCTION wrapper (runner_lib.sh's
# run_step/verify_step, sourced by run.sh) inside a sub-shell, so a bug in
# the wrapper itself — not just in the verifier — fails here.
wrap() { # wrap <label> <want-rc> <EXIT_CODES-file> <runner-lib statements...>
  local label="$1" want="$2" codes="$3"; shift 3
  set +e
  bash -c "
    set -euo pipefail
    HERE='$HERE'
    EXIT_CODES='$codes'
    source '$HERE/runner_lib.sh'
    $*
  " >/dev/null 2>&1
  local rc=$?
  set -e
  if [[ $rc -ne "$want" ]]; then
    echo "NEGCTL FAIL: real wrapper ($label) exited $rc, want $want" >&2
    exit 1
  fi
  echo "[negctl] real-wrapper ok: $label -> rc=$rc (want $want)"
}

: >"$TMP/ctl-cmd.txt"
# A failing command whose output contains NO 'FAIL' text must still abort the
# real run_step wrapper via its raw exit code (rc=3; display grep can't save it).
wrap "run_step surfaces cmd rc=3 with FAIL-free output" 3 "$TMP/ctl-cmd.txt" \
  "run_step ctl-cmd '$TMP/ctl-cmd.log' bash -c 'echo plain output without the f-word; exit 3'"
# record_rc aborts on the first non-zero element, so exactly the failing
# element's code is recorded before the wrapper exits.
grep -q '^ctl-cmd = 3$' "$TMP/ctl-cmd.txt" || { echo "NEGCTL FAIL: cmd rc not recorded" >&2; exit 1; }

# tee failing to write the log (target is a directory) must abort the wrapper
# even though the command itself exited 0 — the swallowed-tee hole from the
# review.
mkdir -p "$TMP/tee-target-is-dir"
: >"$TMP/ctl-tee.txt"
wrap "run_step aborts when tee cannot write the log" 1 "$TMP/ctl-tee.txt" \
  "run_step ctl-tee '$TMP/tee-target-is-dir' bash -c 'echo x'"
grep -q '^ctl-tee = 0$' "$TMP/ctl-tee.txt" || { echo "NEGCTL FAIL: cmd rc not recorded" >&2; exit 1; }
grep -q '^ctl-tee-tee = [1-9][0-9]*$' "$TMP/ctl-tee.txt" || { echo "NEGCTL FAIL: tee rc not recorded as non-zero" >&2; exit 1; }

# The real verify_step must abort on bad evidence JSON...
: >"$TMP/ctl-verify.txt"
wrap "verify_step aborts on all_passed=false evidence" 1 "$TMP/ctl-verify.txt" \
  "verify_step ctl-verify http '$TMP/http-allfalse.json' '$TMP/http-ok.log' negctl-run"

# ...and pass (rc=0) on the valid baseline, recording zero.
: >"$TMP/ctl-verify-ok.txt"
wrap "verify_step accepts valid baseline" 0 "$TMP/ctl-verify-ok.txt" \
  "verify_step ctl-verify-ok http '$TMP/http-ok.json' '$TMP/http-ok.log' negctl-run"
grep -q '^ctl-verify-ok = 0$' "$TMP/ctl-verify-ok.txt" || { echo "NEGCTL FAIL: ok rc not recorded" >&2; exit 1; }

echo "[negctl] ALL NEGATIVE CONTROLS PASSED (16 rejected, 3 accepted, 4 real-wrapper)"
