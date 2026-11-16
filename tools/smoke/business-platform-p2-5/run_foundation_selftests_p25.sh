#!/bin/bash
# P2.5 foundation honest runner (round 2: attempt dirs, no overwrite).
# NO runtime: zero docker/psql/http/go. Each invocation writes a NEW
# attempt dir; earlier attempts (including failures) are never touched.
# bash only (P24 lesson: never sh).
#
# usage: run_foundation_selftests_p25.sh [--only step1,step2,…]
#   steps: py_compile derive_real derive_check selftest_cohort
#          baseline_install tree_delta preimpl_gate selftest_src
#          guard_refusal preflight_plan baseline_byteequal
#          selftest_fxproxy smoke_probe compose_static selftest_loopback
#          selftest_tailtools driver_dryrun driver_refusal
# Do NOT rerun unchanged green steps for bookkeeping (root rule);
# use --only for the steps you actually changed.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
P24_FINAL="$REPO/tools/smoke/business-platform-p2-4/results/2026-11-14-p24-final"
P24_TOOLS="$REPO/tools/smoke/business-platform-p2-4"
ROOT_EV="$HERE/results/2026-11-14-p25-foundation"
N=1
while [ -e "$ROOT_EV/attempt-$N" ]; do N=$((N+1)); done
EV="$ROOT_EV/attempt-$N"
mkdir -p "$EV"
echo "[runner] attempt dir: $EV"

ONLY=""
if [ "${1:-}" = "--only" ]; then ONLY="$2"; fi
want() { [ -z "$ONLY" ] || case ",$ONLY," in *",$1,"*) return 0;; *) return 1;; esac; }

fail=0
step() {  # step <name> <expected-rc: 0|nonzero> <cmd...>
  local name="$1" want_rc="$2"; shift 2
  local log="$EV/$name.log" rcf="$EV/$name.rc"
  echo "CMD: $*" > "$log"
  "$@" >> "$log" 2>&1
  local rc=$?
  echo "$rc" > "$rcf"
  if [ "$want_rc" = "0" ]; then
    [ $rc -eq 0 ] || fail=1
  else
    [ $rc -ne 0 ] || { echo "[$name] expected nonzero, got 0" >&2; fail=1; }
  fi
  echo "[$name] rc=$rc (want $want_rc)"
}

want py_compile && step py_compile 0 python3 -I -m py_compile \
  "$HERE/p25_cohort_derive.py" "$HERE/p25_cohort_guard.py" \
  "$HERE/source_hash_p25.py" "$HERE/readonly_preflight_p25.py" \
  "$HERE/verify_preflight_p25.py" "$HERE/selftest_p25_cohort.py" \
  "$HERE/selftest_p25_source_preflight.py"

# NOTE: derive regenerates p25_cohort6.py + p24-frozen-baseline.json only
# when their derivation inputs change; the generated files are content-
# identical across reruns (deterministic, no timestamps).
want derive_real && step derive_real 0 python3 -I \
  "$HERE/p25_cohort_derive.py" \
  --p24-final-dir "$P24_FINAL" --p24-tools-dir "$P24_TOOLS" \
  --out "$HERE/p25_cohort6.py" \
  --emit-frozen-baseline "$HERE/p24-frozen-baseline.json"

want derive_check && step derive_check 0 python3 -I \
  "$HERE/p25_cohort_derive.py" \
  --p24-final-dir "$P24_FINAL" --p24-tools-dir "$P24_TOOLS" \
  --out "$HERE/p25_cohort6.py" --check

want selftest_cohort && step selftest_cohort 0 python3 -I \
  "$HERE/selftest_p25_cohort.py"

want baseline_install && {
  step baseline_install 0 cp "$P24_FINAL/01-source-hash-pre.txt" \
    "$HERE/source-p24-baseline.txt"
  cat >> "$EV/baseline_install.log" <<'NOTE'
sibling source-p24-baseline.txt = byte-copy of the frozen P24 373-file
baseline (combined 8cd25d42…). Current-tree delta is adjudicated by the
tree_delta step (primary track's P25 implementation is in flight).
NOTE
}

want tree_delta && step tree_delta 0 python3 -I \
  "$HERE/source_hash_p25.py" delta --repo "$REPO" \
  --baseline "$HERE/source-p24-baseline.txt" \
  --out "$EV/tree-delta.json"

# strict gate: rc1 EXPECTED while the parallel implementation is in
# flight; the nonzero rc is honest evidence, not a tool failure.
if want preimpl_gate; then
  step preimpl_gate nonzero python3 -I "$HERE/source_hash_p25.py" \
    verify --repo "$REPO" --baseline "$HERE/source-p24-baseline.txt" \
    --expect-combined \
    8cd25d4220a6bdae843a693e1c1afa928991f537f5902a31e3e970704db6386b
fi

want selftest_src && step selftest_src 0 python3 -I \
  "$HERE/selftest_p25_source_preflight.py"

want selftest_fxproxy && step selftest_fxproxy 0 python3 -I \
  "$HERE/selftest_p25_fixture_proxy.py"

# scoped loopback release (review-fixture-proxy-round1): OWN in-process
# servers on 127.0.0.1:0 only; finally shuts down with bounded joins.
want selftest_loopback && step selftest_loopback 0 python3 -I \
  "$HERE/selftest_p25_loopback.py"

want selftest_tailtools && step selftest_tailtools 0 python3 -I \
  "$HERE/selftest_p25_tailtools.py"

want driver_dryrun && step driver_dryrun 0 bash "$HERE/formal_run_p25.sh" \
  dryrun

if want driver_refusal; then
  # unconfirmed write phase must refuse rc3 (no runtime flag present)
  step driver_refusal nonzero env P25_CONFIRM=no RELEASED_RUNTIME=no \
    bash "$HERE/formal_run_p25.sh" preflight
  rc=$(cat "$EV/driver_refusal.rc")
  [ "$rc" = "3" ] || { echo "[driver_refusal] rc=$rc want 3" >&2; fail=1; }
fi

want smoke_probe && step smoke_probe 0 python3 -I \
  "$HERE/smoke_p25.py" --contract-probe

if want compose_static; then
  step compose_static 0 python3 -I -c "
import re
for f in ('compose-p25-fixture.yaml', 'compose-p25-aux.yaml'):
    body = chr(10).join(l for l in open('$HERE/'+f).read().splitlines()
                        if not l.lstrip().startswith('#'))
    assert 'depends_on' not in body, f
    ports = re.findall(r'\"127.0.0.1:(\d+):', body)
    assert all(17092 <= int(p) <= 17094 for p in ports), (f, ports)
    print(f, 'depends_on-free, ports', ports)
print('compose static checks OK')"
fi

if want guard_refusal; then
  step guard_refusal nonzero python3 -I "$HERE/p25_cohort_guard.py" \
    --policy first-transition
  rc=$(cat "$EV/guard_refusal.rc")
  [ "$rc" = "3" ] || { echo "[guard_refusal] rc=$rc want 3" >&2; fail=1; }
fi

want preflight_plan && step preflight_plan 0 python3 -I \
  "$HERE/readonly_preflight_p25.py" --plan

if want baseline_byteequal; then
  if cmp -s "$HERE/source-p24-baseline.txt" \
    "$P24_FINAL/01-source-hash-pre.txt"; then
    echo 0 > "$EV/baseline_byteequal.rc"
    echo "CMD: cmp -s source-p24-baseline.txt 01-source-hash-pre.txt" \
      > "$EV/baseline_byteequal.log"
  else
    echo 1 > "$EV/baseline_byteequal.rc"; fail=1
  fi
  echo "[baseline_byteequal] rc=$(cat "$EV/baseline_byteequal.rc")"
fi

echo "----------------------------------------"
echo "foundation-runner attempt-$N fail=$fail (logs: $EV)"
exit $fail
