# h14 original lost-201-ACK supplement — review candidate

No new Run, proxy restart, owner clearing, adoption, or replay is needed. The original tail completed one real forwarded dataset commit (HTTP201 with a valid nested ACK), then the old proxy unconditionally closed the connector connection. Local Run failed with EOF after publish entry, no local dataset transaction ID, checkpoint0 and retained failed owner. The original tail rc1, raw `verified_committed:false`, bindings and both failed gates remain unchanged.

The proxy source bug is its `status == 200` ACK parse guard (both passthrough and drop branches), not nested JSON parsing. Root's sole ownership exception allowed changing only that proxy tool: accept real successful2xx, strictly verify usable ACK, and close only when verified; invalid ACK/upstream failures are forwarded with their actual status/body. This source patch is prospective and was not loaded into the running proxy. Old source bytes were saved before editing at `results/h14-repair-tools-sol/attempt1/dataset_fault_proxy_p25.before.py`, SHA a6a4aa06535c6b5eafdb9b49243b30a67eb9c4cdfc52befa5086f1e937a557f3; patched source SHA c0e3bbfe240c000b0d3170da6854098a8f728f2264db185579f0e0c0d06961da.

The independently authorized reads are complete. Direct flat transaction GET is COMMITTED/main/SNAPSHOT with exact source/def/Run lineage, rows24/bytes558/content hash. Direct files GET has one transaction-bound Filelist entry with id a6fef0ca-5709-405d-a82a-a14524f73415. ACK resource file id852fde85-9f3a-4c97-8853-0d92791a5287 was not a valid download index ID: its original404/rc1 artifacts are preserved; a separately authorized download using the real Filelist ID returned200/rc0 with exact558-byte A batch/hash. The supplement never treats the ACK's resource file ID as independent file-index proof.

After Root review, Runtime copies the saved old proxy bytes into this EV (preserving SHA) and runs one PURE derivation:

```sh
python3 -I tools/smoke/business-platform-p2-5/verify_h14_supplement_p25.py \
  --original-tail "$EV_ROOT/tail-result.json" \
  --original-step "$EV_ROOT/steps/tail-attempt1" \
  --remote-summary "$EV_ROOT/tail-h14-supplement/attempt2/summary.json" \
  --old-proxy "$EV_ROOT/h14-proxy-original.py" \
  --output "$EV_ROOT/tail-aggregate.json" \
  --bindings-output "$EV_ROOT/tail-selected-bindings.json" \
  --proof-output "$EV_ROOT/h14-supplement-proof.json"
```

All three outputs must be fresh and distinct. `build(arguments, resolve_path=None)` is pure. Public Archive/offline API `verify_supplement(path, resolve_path=None) -> {ok,problems}` recomputes all evidence and byte-compares the derived aggregate, selected bindings and proof. Metadata keeps its original logical absolute paths; the resolver maps only reads into a sealed copy. The engine and h10 runner remain frozen.

The verifier pins original tail SHA c340abeb0b74da9b02dbeee25781187a8688b9fb003714ebbfbfc9989646ee34, exact original ledger record SHA c2d283f8aa4b0227a75f3636af541efd62add567ab7b5fa5e424f6d0fa341e3f and raw ACK SHA a04489f83f1a6410cbf4817dac8376bb185446e7653f03e9f8219e6c516b5027. It requires exactly one real forwarded target201 dispatch in the before/after ledger, usable ACK reparse, original local EOF/CP0/owner facts, immutable declaration plus original exact8-owner/held1-run replay, and three direct remote GET commands with original status/rc/body SHA. It reads actual response bytes, not caller `ok` flags. Original404 artifacts and previous-attempt summary SHA are retained in the input chain.

`tail-aggregate.json` is a flat, typed `p25-tail-aggregate-v1` derived summary. It preserves original phase, h11 and all other gate facts, quiet, rosters, declaration adjudication and exact identity map. Only h14 gate indices0 and2 are replaced with explicit supplement-backed ACK gates, h14.failed becomes false, and top failed becomes empty. `supplement` identifies the original rc1 summary/bindings hashes, original failed gate objects, proof file/SHA and exact source arguments. This is new interpreted evidence, not a rewritten original execution result.

`h14-supplement-proof.json` is built first. Aggregate references its SHA, then `tail-selected-bindings.json` copies the original actual bindings and unchanged binding-map hash while binding `summary_sha256` to final aggregate bytes. No circular SHA references exist. Archive must invoke the pure verifier before selecting these derived artifacts, then apply its normal declaration/dual-roster/final-summary checks.

Tests: `python3 -I selftest_h14_supplement_p25.py`. Four methods include the real201 ACK positive control; wrong target/OPEN/zero or malformed txn/non-JSON/non2xx/upstream-error negative controls; actual saved evidence derivation; exact two-gate replacement and unchanged identities; and rehashed remote lineage/transaction/content/command or local-success/original-summary/old-source tampering rejected. Socket/subprocess/HTTP tripwires protect pure tests. Actual savedraw in-memory build passed; no HTTP/Docker/DB/Go was executed by this worker.
