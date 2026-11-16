# Preserved full-window failure and exact replacement aggregation

HTTP worker owns aggregate_healthy_p25.py, collect_http_quiet_p25.py, verify_h13_ledger_p25.py and their pure tests. Runtime executes all live calls; these tools do not rewrite original window-result.json, raw rc1, or retained owners.

Three failures were tooling/actor wiring: h7 mutation fences serialize `error` rather than trigger-style `code`; h15 preadmission exact fresh404 represents an uninitialized data CP at epoch0 rather than None; h13 ARM token had only connections:write while scheduled dispatch forwards that token to dataset commit, which requires dataset.write. h13's two dispatched403 Runs remain failed and retain owners. Smoke de6 fixes only h7/h15 interpretation/fakes; Runtime uses the same already-authorized main actor permissions for the ARM JWT. Only h7/h15/h13 were actually rerun; all three rc0. The original nine green cases remain source facts from the original rc1 attempt.

Root reviewed the actual emitted gate-name contract: h2 requires G_SNAPSHOT_ONLY (its gates contain COMMITTED/SNAPSHOT/lineage/hash, exact A/B values and historical file assertions); h3 requires G_EMPTY_BATCH + G_LEDGER (its gates contain empty values/hash, CP unchanged, ledger assertions); h13 requires G_CAS_ATOMIC + G_OWNER_DURABLE, with its missing ledger coverage supplied by a separate real readonly GET proof below. Other cases require their registry gate names. Every selected actual gate must remain true; no summary or recorder result is edited to invent a pass.

Runtime already collected one final global quiet proof using all four actual handles inputs, plus one GET /admin/log/p25h13. The ledger body has six retained entries: two old403 fetch entries, then four successful fetch entries. The pure ledger verifier derives exactly four succeeded Runs from the successful h13 raw HTTP records; proves two defs on one real source, captured base0/1 per def, three scheduled Run links + one real async manual Run link; partitions only nonoverlapping started_at..publish_entered_at intervals; requires exact token/window/cursor/page_size and page counts from SRC_WINDOWS. It excludes the two historical rows explicitly, never clears them.

After Root reviews the tool candidate, Runtime can run this entirely pure aggregation (EV_ROOT is the actual current round's evidence root):

```sh
python3 -I tools/smoke/business-platform-p2-5/aggregate_healthy_p25.py \
  --original-summary "$EV_ROOT/window-result.json" \
  --original-step "$EV_ROOT/steps/window-attempt1" \
  --replacement h7 "$EV_ROOT/healthy-retry/h7/result-attempt1.json" "$EV_ROOT/healthy-retry/h7/attempt1" \
  --replacement h15 "$EV_ROOT/healthy-retry/h15/result-attempt1.json" "$EV_ROOT/healthy-retry/h15/attempt1" \
  --replacement h13 "$EV_ROOT/healthy-retry/h13/result-attempt1.json" "$EV_ROOT/healthy-retry/h13/attempt1" \
  --final-quiet-proof "$EV_ROOT/window-final-quiet/attempt1/proof.json" \
  --final-quiet-step "$EV_ROOT/steps/window-final-quiet-attempt1" \
  --h13-ledger-step "$EV_ROOT/steps/window-h13-ledger-attempt1" \
  --handles-output "$EV_ROOT/window-union-handles.json" \
  --output "$EV_ROOT/window-aggregate.json"
```

Outputs must be fresh. Original actual cmd.txt and retry actual cmd.json argv are both supported, requiring exactly one captured command artifact per step, actual stdout/stderr, original rc, linked summary and full recorder files. Replacement set is exactly original failed3; final h13 must have imported every original/replacement actual def. Final owner roster must equal the original failedhealthy2 exactly, and nonterminal roster must remain empty, with raw source/run/CP parent proofs. Global quiet is replayed from actual GET response status/body bytes/hash/name and strict integer counters for the exact owned source union and proxy. The known failedhealthy owners are explicitly identified by original summary path/SHA.

Output schema is `p25-healthy-aggregate-v1`: actual selected `scenarios`, `case_sources`, `sources` (all original summaries/rc/cmd/stdout/stderr/handles/full recorder SHA links), final live rosters, replayed quiet, h13 ledger supplement verdict, `retained_owner_origins`, `original_failure_preserved`, and `attempts.handles_file/handles_sha256`. Union handles preserve each original row's source summary and handles SHA/index in `origins`.

Archive public pure API: `verify_aggregate(path, resolve_path=None) -> {ok, problems}`. It recomputes every source, mandatory case/gate set, exact owners/parents, quiet and ledger proof, plus byte-exact union handles. An offline resolver may map original absolute paths into a sealed bundle; metadata paths themselves are not rewritten. No live IO is reachable through this API.

Validation: selftest_p25_window_repairs.py 3 methods passed; selftest_aggregate_healthy_p25.py 7 methods with tamper/missing-case/missing-gate/rehash-bad-source-name/token/base/parent negatives passed; original successful actual h13 ledger pure verification passed; actual four-source aggregate build in memory returned12 selected cases,2 preserved owners,0 nonterminal. These offline checks do not claim new live executions.
