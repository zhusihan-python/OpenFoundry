#!/usr/bin/env bash
# Bounded negative controls for the dataset-excel acceptance runner.
#
# Proves the STRICT verifier (verify_results.py) rejects the exact classes
# of bad evidence that could otherwise fake a green acceptance — SKIP
# results, missing/failed/flaky tests, retry traces, zero durations, hidden
# extra specs, manifest drift (fixture hash, duplicate artifacts, token
# leakage, non-200 probes), and duplicated/missing/non-zero exit codes —
# and accepts the REAL run's evidence.
#
# Baselines are the REAL artifacts of the current run (per review: never
# hand-made shapes the verifier could be tuned to self-satisfy): mutants
# are derived by mutating the real Playwright JSON / evidence manifest.
# Plus REAL-WRAPPER controls sourcing runner_lib.sh, like the s1 harness.
#
# Usage: negative_controls.sh <real playwright.json> <real evidence.json>
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFY="$HERE/verify_results.py"
PW_JSON="${1:?usage: negative_controls.sh <playwright.json> <evidence.json>}"
MANIFEST="${2:?usage: negative_controls.sh <playwright.json> <evidence.json>}"
[[ -f "$PW_JSON" ]] || { echo "baseline playwright json missing: $PW_JSON" >&2; exit 1; }
[[ -f "$MANIFEST" ]] || { echo "baseline manifest missing: $MANIFEST" >&2; exit 1; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ofexcel-negctl.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# ------------------------------------------------------------------ mutants --
# One python program derives every mutant from the REAL baselines, so the
# controls can never drift away from actual reporter output shapes.
python3 - "$PW_JSON" "$MANIFEST" "$VERIFY" "$TMP" <<'PYEOF'
import copy, hashlib, json, sys

pw_path, manifest_path, verify_path, tmp = sys.argv[1:5]
pw = json.load(open(pw_path, encoding="utf-8"))
man = json.load(open(manifest_path, encoding="utf-8"))

sys.path.insert(0, verify_path.rsplit("/", 1)[0])
import verify_results as vr  # noqa: E402  (REQUIRED_TESTS live here)

def specs(doc):
    """(container, idx) pairs for every spec, mirroring the traversal."""
    out = []
    def walk(node):
        for s in node.get("suites", []) or []:
            walk(s)
        for i in range(len(node.get("specs", []) or [])):
            out.append((node["specs"], i))
    walk(doc)
    return out

def dump(name, doc):
    with open(f"{tmp}/{name}", "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False)

def clone(doc):
    return copy.deepcopy(doc)

# --- playwright mutants -----------------------------------------------------
# 1. everything skipped (the spec's own opt-in gate closed): statuses,
#    stats and zero-duration skip results shaped like the real reporter's.
d = clone(pw)
d["stats"].update(expected=0, skipped=vr.EXPECTED_SPEC_COUNT,
                  unexpected=0, flaky=0)
for specs_l, i in specs(d):
    sp = specs_l[i]
    t = sp["tests"][0]
    t["status"] = "skipped"
    t["expectedStatus"] = "skipped"
    t["results"] = [{"status": "skipped", "duration": 0, "retry": 0,
                     "startTime": t["results"][0]["startTime"],
                     "errors": [], "annotations": []}]
    sp["ok"] = False
dump("pw-skip-all.json", d)

# 2. one required spec missing (stats consistent — count catches it too).
d = clone(pw)
t2 = vr.REQUIRED_TESTS[1]
for specs_l, i in specs(d):
    if specs_l[i]["title"] == t2:
        del specs_l[i]
        break
d["stats"]["expected"] = vr.EXPECTED_SPEC_COUNT - 1
dump("pw-missing-required.json", d)

# 3. one required spec failed.
d = clone(pw)
for specs_l, i in specs(d):
    if specs_l[i]["title"] == t2:
        t = specs_l[i]["tests"][0]
        t["status"] = "unexpected"
        t["results"] = [{"status": "failed", "duration": 5, "retry": 0,
                         "startTime": t["results"][0]["startTime"],
                         "errors": [{"message": "boom"}]}]
        specs_l[i]["ok"] = False
        break
d["stats"].update(expected=1, unexpected=1)
dump("pw-required-failed.json", d)

# 4. flaky: passed only after a retry (retry trace + two results).
d = clone(pw)
for specs_l, i in specs(d):
    if specs_l[i]["title"] == t2:
        t = specs_l[i]["tests"][0]
        first = copy.deepcopy(t["results"][0])
        t["results"] = [dict(first, status="failed", duration=3),
                        dict(first, status="passed", retry=1)]
        t["status"] = "flaky"
        break
d["stats"].update(expected=1, flaky=1)
dump("pw-flaky-retry.json", d)

# 5. hidden extra spec (third spec present, stats inflated to match).
d = clone(pw)
first_pair = specs(d)[0]
extra = copy.deepcopy(first_pair[0][first_pair[1]])
extra["title"] = "hidden third spec"
extra["id"] = "extra-id"
d["stats"]["expected"] += 1
holder = d["suites"][0]["suites"][0] if d["suites"][0].get("suites") else d["suites"][0]
holder.setdefault("specs", []).append(extra)
dump("pw-extra-spec.json", d)

# 6. passed but zero duration (skip-shaped result wearing passed status).
d = clone(pw)
for specs_l, i in specs(d):
    if specs_l[i]["title"] == vr.REQUIRED_TESTS[0]:
        specs_l[i]["tests"][0]["results"][0]["duration"] = 0
        break
dump("pw-zero-duration.json", d)

# 7. result missing startTime (no real execution trace).
d = clone(pw)
for specs_l, i in specs(d):
    if specs_l[i]["title"] == vr.REQUIRED_TESTS[0]:
        del specs_l[i]["tests"][0]["results"][0]["startTime"]
        break
dump("pw-no-starttime.json", d)

# 8. top-level reporter errors present (global failure outside tests).
d = clone(pw)
d["errors"] = [{"message": "global error"}]
dump("pw-global-errors.json", d)

# --- manifest mutants --------------------------------------------------------
def dumpm(name, doc):
    with open(f"{tmp}/{name}", "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)

# 9. token value leaked into evidence.
d = clone(man)
d["token"]["value"] = "eyJhbGciOiJIUzI1NiJ9.leaked"
dumpm("man-token-leak.json", d)

# 10. fixture drift: real file on disk, real recorded hash, but NOT the
#     pinned orders-real.xlsx (point the fixture artifact at the manifest
#     itself and record its true hash — passes re-hash, fails the pin).
d = clone(man)
self_hash = hashlib.sha256(open(manifest_path, "rb").read()).hexdigest()
for a in d["artifacts"]:
    if a["name"] == vr.FIXTURE_ARTIFACT_NAME:
        a["path"] = manifest_path
        a["sha256"] = self_hash
dumpm("man-fixture-drift.json", d)

# 11. duplicate artifact names.
d = clone(man)
first = copy.deepcopy(d["artifacts"][0])
d["artifacts"].append(first)
dumpm("man-dup-artifact.json", d)

# 12. recorded artifact hash drift (manifest sha != on-disk).
d = clone(man)
d["artifacts"][0]["sha256"] = "0" * 64
dumpm("man-hash-drift.json", d)

# 13. git.dirty as int (not a real boolean).
d = clone(man)
d["git"]["dirty"] = 0
dumpm("man-dirty-int.json", d)

# 14. probe shows an unhealthy stack.
d = clone(man)
d["probe"]["catalog_status"] = 500
dumpm("man-probe-500.json", d)

# 15. env/toolchain identities missing (tags alone are not runtime ids).
d = clone(man)
del d["env"]
dumpm("man-no-env.json", d)

# --- exit-codes mutants ------------------------------------------------------
with open(f"{tmp}/codes-ok.txt", "w") as fh:
    fh.write("ensure-images = 0\nrecord-env = 0\nbuild-dvs = 0\nbuild-gw = 0\n"
             "stack-probe = 0\nskip-control-playwright = 0\n"
             "skip-control-shape = 0\nskip-control-expected-reject = 0\n"
             "playwright-real = 0\nplaywright-real-verify = 0\n"
             "negative-controls = 0\nverify-manifest = 0\n")
with open(f"{tmp}/codes-dup.txt", "w") as fh:
    fh.write("build-dvs = 0\nbuild-dvs = 1\n")
with open(f"{tmp}/codes-missing-step.txt", "w") as fh:
    fh.write("build-dvs = 0\nplaywright-real = 0\n")
with open(f"{tmp}/codes-nonzero.txt", "w") as fh:
    fh.write("ensure-images = 0\nrecord-env = 0\nbuild-dvs = 0\nbuild-gw = 0\n"
             "stack-probe = 0\nskip-control-playwright = 0\n"
             "skip-control-shape = 0\nskip-control-expected-reject = 0\n"
             "playwright-real = 0\nplaywright-real-verify = 0\n"
             "negative-controls = 0\nverify-manifest = 2\n")
print("[negctl] mutants built from real baselines")
PYEOF

# ------------------------------------------------------------- assertions --
must_reject() { # must_reject <label> <mode> <file>
  local label="$1" mode="$2" file="$3"
  if python3 "$VERIFY" "$mode" "$file" >/dev/null 2>"$TMP/last-err.txt"; then
    echo "NEGCTL FAIL: verifier ACCEPTED bad evidence ($label)" >&2
    exit 1
  fi
  echo "[negctl] reject ok: $label — $(head -1 "$TMP/last-err.txt")"
}
must_accept() { # must_accept <label> <mode> <file>
  local label="$1" mode="$2" file="$3"
  if ! python3 "$VERIFY" "$mode" "$file" >/dev/null 2>"$TMP/last-err.txt"; then
    echo "NEGCTL FAIL: verifier REJECTED good evidence ($label): $(cat "$TMP/last-err.txt")" >&2
    exit 1
  fi
  echo "[negctl] accept ok: $label"
}

must_reject "pw: file missing"                 playwright "$TMP/does-not-exist.json"
must_reject "pw: all tests skipped"            playwright "$TMP/pw-skip-all.json"
must_reject "pw: required spec missing"        playwright "$TMP/pw-missing-required.json"
must_reject "pw: required spec failed"         playwright "$TMP/pw-required-failed.json"
must_reject "pw: flaky retry trace"            playwright "$TMP/pw-flaky-retry.json"
must_reject "pw: hidden extra spec"            playwright "$TMP/pw-extra-spec.json"
must_reject "pw: passed with zero duration"    playwright "$TMP/pw-zero-duration.json"
must_reject "pw: result without startTime"     playwright "$TMP/pw-no-starttime.json"
must_reject "pw: top-level reporter errors"    playwright "$TMP/pw-global-errors.json"
must_reject "manifest: token value leaked"     manifest   "$TMP/man-token-leak.json"
must_reject "manifest: fixture hash not pinned" manifest  "$TMP/man-fixture-drift.json"
must_reject "manifest: duplicate artifact"     manifest   "$TMP/man-dup-artifact.json"
must_reject "manifest: artifact hash drift"    manifest   "$TMP/man-hash-drift.json"
must_reject "manifest: git.dirty as int"       manifest   "$TMP/man-dirty-int.json"
must_reject "manifest: probe catalog 500"      manifest   "$TMP/man-probe-500.json"
must_reject "manifest: env identities missing" manifest   "$TMP/man-no-env.json"
must_reject "exitcodes: duplicate step"        exitcodes  "$TMP/codes-dup.txt"
must_reject "exitcodes: required step missing" exitcodes  "$TMP/codes-missing-step.txt"
must_reject "exitcodes: non-zero rc"           exitcodes  "$TMP/codes-nonzero.txt"

must_accept "pw: REAL run baseline"            playwright "$PW_JSON"
must_accept "manifest: REAL run baseline"      manifest   "$MANIFEST"
must_accept "exitcodes: all zero, steps present" exitcodes "$TMP/codes-ok.txt"

# ------------------------------------------------- real-wrapper controls --
# These exercise the PRODUCTION wrapper (runner_lib.sh's run_step /
# verify_step / expect_verifier_reject) inside a sub-shell, so a bug in the
# wrapper itself — not just in the verifier — fails here.
source "$HERE/runner_lib.sh"
wrap() { # wrap <label> <want-rc> <EXIT_CODES-file> <runner-lib statements...>
  local label="$1" want="$2" codes="$3"; shift 3
  set +e
  NEGCTL_ERR="$TMP/.wrap.err" bash -c "
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
# A failing command whose output contains NO 'FAIL' text must still abort
# the real run_step wrapper via its raw exit code (rc=3).
wrap "run_step surfaces cmd rc=3 with FAIL-free output" 3 "$TMP/ctl-cmd.txt" \
  "run_step ctl-cmd '$TMP/ctl-cmd.log' bash -c 'echo plain output; exit 3'"
grep -q '^ctl-cmd = 3$' "$TMP/ctl-cmd.txt" || { echo "NEGCTL FAIL: cmd rc not recorded" >&2; exit 1; }

# tee failing to write the log (target is a directory) must abort the step
# even though the command exited 0 — the swallowed-tee hole.
mkdir -p "$TMP/tee-target-is-dir"
: >"$TMP/ctl-tee.txt"
wrap "run_step aborts when tee cannot write the log" 1 "$TMP/ctl-tee.txt" \
  "run_step ctl-tee '$TMP/tee-target-is-dir' bash -c 'echo x'"
grep -q '^ctl-tee = 0$' "$TMP/ctl-tee.txt" || { echo "NEGCTL FAIL: cmd rc not recorded" >&2; exit 1; }
grep -q '^ctl-tee-tee = [1-9][0-9]*$' "$TMP/ctl-tee.txt" || { echo "NEGCTL FAIL: tee rc not recorded non-zero" >&2; exit 1; }

# verify_step aborts on bad evidence...
: >"$TMP/ctl-verify.txt"
wrap "verify_step aborts on skipped-evidence" 1 "$TMP/ctl-verify.txt" \
  "verify_step ctl-verify playwright '$TMP/pw-skip-all.json'"
grep -q '^ctl-verify = [1-9][0-9]*$' "$TMP/ctl-verify.txt" || { echo "NEGCTL FAIL: verify rc not recorded non-zero" >&2; exit 1; }

# ...and passes (rc=0) on the real baseline, recording zero.
: >"$TMP/ctl-verify-ok.txt"
wrap "verify_step accepts REAL baseline" 0 "$TMP/ctl-verify-ok.txt" \
  "verify_step ctl-verify-ok playwright '$PW_JSON'"
grep -q '^ctl-verify-ok = 0$' "$TMP/ctl-verify-ok.txt" || { echo "NEGCTL FAIL: ok rc not recorded" >&2; exit 1; }

# expect_verifier_reject must abort when the verifier ACCEPTS bad evidence
# (anti-greening: an accept of a skip-run is itself a failure).
: >"$TMP/ctl-accept-gate.txt"
wrap "expect_verifier_reject aborts on ACCEPT of bad evidence" 1 "$TMP/ctl-accept-gate.txt" \
  "expect_verifier_reject ctl-gate playwright '$PW_JSON'"
grep -q '^ctl-gate-expected-reject = 1$' "$TMP/ctl-accept-gate.txt" || { echo "NEGCTL FAIL: gate accept not recorded as rc=1" >&2; exit 1; }

echo "[negctl] ALL NEGATIVE CONTROLS PASSED (19 rejected, 3 accepted, 5 real-wrapper)"
