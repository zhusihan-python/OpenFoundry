#!/bin/bash
# P25 ARCHIVE selftest runner (honest: real cmd + rc + stdout/stderr
# per step, archived under results/2026-11-14-p25-archive/attempt-N).
# Pure/selftest only — NO runtime, NO docker/DB/HTTP/listener; the
# only real CLIs run are the archive tools themselves over SYNTHETIC
# dirs (injected fake IO), including draft-refusal paths.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="$HERE/results/2026-11-14-p25-archive"
N=1
while [ -e "$BASE/attempt-$N" ]; do N=$((N+1)); done
OUT="$BASE/attempt-$N"
mkdir -p "$OUT"

step() {  # step <name> <cmd...>
  local name="$1"; shift
  {
    echo "CMD: $*"
  } > "$OUT/$name.log"
  "$@" > "$OUT/$name.stdout" 2> "$OUT/$name.stderr"
  local rc=$?
  echo "$rc" > "$OUT/$name.rc"
  echo "[archive-selftest] $name rc=$rc"
  return $rc
}

fail=0
run() {  # run <name> <expect-rc> <cmd...>
  local name="$1" expect="$2"; shift 2
  step "$name" "$@" || true
  local got; got=$(cat "$OUT/$name.rc")
  if [ "$got" != "$expect" ]; then
    echo "[archive-selftest] UNEXPECTED rc for $name: got=$got want=$expect" | tee -a "$OUT/UNEXPECTED.log"
    fail=1
  fi
}

echo "[archive-selftest] attempt dir: $OUT"

# 1) compile all archive-group files
run 01-py_compile 0 python3 -I -m py_compile \
  "$HERE/finalize_p25.py" "$HERE/standalone_p25.py" \
  "$HERE/offline_verify_p25.py" "$HERE/collect_postfacts_p25.py" \
  "$HERE/verify_postfacts_p25.py" "$HERE/collect_persist_state_p25.py" \
  "$HERE/selftest_p25_archive.py"

# 2) full pure selftest (six groups incl. every defect negative)
run 02-selftest-archive 0 python3 -I "$HERE/selftest_p25_archive.py"

# 3) REAL CLI finalize round on the canonical synthetic EV (built by
#    the selftest builder): prepare → BARE seal (stdout/rc OUTSIDE the
#    EV per ARCHIVE-CONTRACT §2) → reverify
SYN="$OUT/synthetic-ev"
EV="$SYN/ev"
run 03-synthetic-ev-build 0 python3 -I "$HERE/selftest_p25_archive.py" \
  --make-ev "$SYN"
run 04-finalize-prepare 0 python3 -I "$HERE/finalize_p25.py" prepare \
  --ev "$EV" --tools-dir "$HERE"
# BARE seal call — everything lands in the OUTSIDE meta dir:
python3 -I "$HERE/finalize_p25.py" seal --ev "$EV" \
  --meta-dir "$OUT/seal-meta" \
  > "$OUT/05-finalize-seal.stdout" 2> "$OUT/05-finalize-seal.stderr"
echo "$?" > "$OUT/05-finalize-seal.rc"
echo "CMD: python3 -I finalize_p25.py seal --ev $EV --meta-dir $OUT/seal-meta (bare; outside EV)" > "$OUT/05-finalize-seal.log"
if [ "$(cat "$OUT/05-finalize-seal.rc")" != "0" ]; then
  echo "[archive-selftest] seal rc!=0" | tee -a "$OUT/UNEXPECTED.log"; fail=1
fi
python3 -I "$HERE/finalize_p25.py" reverify --ev "$EV" \
  --meta-dir "$OUT/seal-meta" \
  > "$OUT/06-finalize-reverify.stdout" 2> "$OUT/06-finalize-reverify.stderr"
echo "$?" > "$OUT/06-finalize-reverify.rc"
echo "CMD: python3 -I finalize_p25.py reverify --ev $EV --meta-dir $OUT/seal-meta" > "$OUT/06-finalize-reverify.log"
if [ "$(cat "$OUT/06-finalize-reverify.rc")" != "0" ]; then
  echo "[archive-selftest] reverify rc!=0" | tee -a "$OUT/UNEXPECTED.log"; fail=1
fi
# post-seal write MUST be caught by reverify (rc1)
echo "post-seal" > "$EV/steps/rogue.log"
python3 -I "$HERE/finalize_p25.py" reverify --ev "$EV" \
  --meta-dir "$OUT/seal-meta" \
  > "$OUT/07-reverify-postseal.stdout" 2> "$OUT/07-reverify-postseal.stderr"
echo "$?" > "$OUT/07-reverify-postseal.rc"
echo "CMD: reverify after deliberate post-seal write (expect rc1)" > "$OUT/07-reverify-postseal.log"
if [ "$(cat "$OUT/07-reverify-postseal.rc")" != "1" ]; then
  echo "[archive-selftest] post-seal write NOT detected" | tee -a "$OUT/UNEXPECTED.log"; fail=1
fi

# 4) REAL CLI standalone round: sanitize → reverify → bundle →
#    verify-bundle → offline verifier; then tamper (expect rc1)
SEV="$OUT/sa-src"
mkdir -p "$SEV"
printf 'ok line\r\nDSN postgres://u:pw@h/db tail\nplain end' > "$SEV/a.log"
printf '{"x": 1}\n' > "$SEV/b.json"
printf '\x00\x01\x02\x03' > "$SEV/c.bin"
run 08-standalone-sanitize 0 python3 -I "$HERE/standalone_p25.py" \
  sanitize --src "$SEV" --dst "$OUT/sa-copy"
run 09-standalone-reverify 0 python3 -I "$HERE/standalone_p25.py" \
  reverify --src "$SEV" --dst "$OUT/sa-copy"
run 10-standalone-bundle 0 python3 -I "$HERE/standalone_p25.py" \
  bundle --src-ev "$SEV" --tools-dir "$HERE" \
  --bundle-dir "$OUT/sa-bundle"
run 11-offline-verifier 0 python3 -I "$HERE/offline_verify_p25.py" \
  --bundle "$OUT/sa-bundle"
printf 'X' >> "$OUT/sa-bundle/ev/b.json"
run 12-offline-verifier-tamper 1 python3 -I \
  "$HERE/offline_verify_p25.py" --bundle "$OUT/sa-bundle"

# 5) collector draft refusals (rc3, zero IO constructed)
run 13-postfacts-draft-refusal 3 python3 -I \
  "$HERE/collect_postfacts_p25.py" --collect --out-dir "$OUT/no-io"
run 14-persist-draft-refusal 3 python3 -I \
  "$HERE/collect_persist_state_p25.py" --collect --out-dir "$OUT/no-io"
run 15-postfacts-plan 0 python3 -I "$HERE/collect_postfacts_p25.py" \
  --plan
run 16-persist-plan 0 python3 -I \
  "$HERE/collect_persist_state_p25.py" --plan

if [ -e "$OUT/no-io" ] && [ -n "$(ls -A "$OUT/no-io" 2>/dev/null)" ]; then
  echo "[archive-selftest] draft refusal created attempt dirs!" | tee -a "$OUT/UNEXPECTED.log"
  fail=1
fi

echo "[archive-selftest] attempt $N complete fail=$fail"
exit $fail
