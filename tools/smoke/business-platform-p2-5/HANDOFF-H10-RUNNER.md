# h10 auxiliary crash runner — Root review candidate

`h10_crash_runner_p25.py` is the sole added live runner. HTTP worker owns it and `selftest_h10_crash_runner_p25.py`; Runtime remains the only live executor. No changes to frozen smoke, fixture, Runtime, Archive, Go, or SQL. Root must review and release this runner before any live invocation.

```sh
python3 -I tools/smoke/business-platform-p2-5/h10_crash_runner_p25.py \
  --env-dir "$Q3_ENV_DIR" \
  --selected-build "$EV_ROOT/selected-build.json" \
  --healthy-summary "$EV_ROOT/window-result.json" \
  --evidence-root "$EV_ROOT/h10/attempt1" \
  --connector-base http://127.0.0.1:17094 \
  --dataset-base http://127.0.0.1:17078 \
  --fixture-base http://127.0.0.1:17092 \
  --proxy-base http://127.0.0.1:17093 \
  --fixture-src-url http://p25fixture:8090 --released
```

`P25_CONFIRM=yes` and all three `OPENFOUNDRY_SMOKE_JWT`, `_ORG2`, `_ARM_JWT` variables must already exist in the executor environment; values never enter argv or records. No gate means rc3 before IO or evidence-directory creation. The evidence root must not already exist. Optional `--healthy-handles FILE` must have byte-identical SHA to the healthy summary's actual `attempts.handles_file`; otherwise the summary path is used directly.

The runner validates frozen engine SHA, accepted selected-build schema/ok/all_ok/source SHA/full p25connector image, and a complete successful fullhealthy summary (h1 trial alone rejected). It discovers exactly one running container by fixed q3 project + p25connector Compose labels, requests full IDs, and reads only Id/Image/Compose labels/StartedAt/Running with Docker's inspect format. No Env output is collected. Image must match selected-build.images.p25connector.

Before the held trigger it imports every actual healthy source/def handle, selects a genuinely succeeded and quiet existing def with a different fixture src from p25h13, creates one new held target/source/def using only http://p25fixture:8090 and request_timeout_ms=30000, prepares pre/post argv, and sets slow_terminal_ms=15000. All new handles, including failed attempts, are flushed immediately by frozen engine helpers.

The trigger starts the monotonic budget. It must prove actual running/controlled/prepublish Run, same CP owner, and fixture active>0 before 3 seconds. It invokes the actual frozen engine pre CLI (healthy/only h10, imported handles, accepted image pin), then rechecks held state and the five-second kill deadline. Only `docker kill -s KILL <same-full64-CID>` and `docker start <same-full64-CID>` are allowed. No graceful restart, base3 restart, retry, adoption, cancellation, owner deletion, or retrigger exists.

Each subprocess has cmd/argv saved before execution, original rc saved separately, raw stdout/stderr, and individual SHA links. Failures/timeouts retain command evidence. Start is followed by actual minimal inspect and bounded health polling (45 seconds). The crash record contains actual labels/identity/full accepted image, both StartedAt values, prior SHA, held identities, ordered actual event times, raw kill/start/health artifacts and original rc. The frozen crash validator runs before post. Actual engine post checks protected CP/owner/Run state and waits for genuine quiet (its fixture polling maximum30 seconds). Post process timeout is90 seconds.

Successful outputs under the fresh evidence root:

- `result.json`: `ok:true`, input source/build/healthy/handles/runner SHA map, command records, quiet/held actual UUIDs, kill elapsed time, and output paths.
- `prior.json`, `pre-result.json`, `crash.json`, `post-result.json`: real frozen-engine/artifact contracts, plus command-specific raw directories.
- `tail-known-binding.json`: `known_bindings.h10-held = {def_id,source_id,run_id}`, semantics `p25-crash-held-prepublish`, and post summary SHA.
- `result.merged_handles_file`: the recorder's actual `handles.json`, containing imported fullhealthy parents plus new held target/source/def/Run; all source configs and actual IDs retained.

Runtime uses `result.merged_handles_file` as a tail `--owned-handles-file` input and inserts the known_bindings map into the hash-frozen semantic declaration. Tail reason is not guessed: after post, readonly-poll actual held CP until `requires_reconciliation` before freezing that expected value. The binding file's prepublish semantic does not claim any COMMIT dispatch or unknown COMMIT result.

On failure rc1, `result.ok:false`, `error`, every partial created handle, and every completed raw command remain; no tail binding is emitted. No recovery action is attempted. Stop and let Root inspect the preserved state. Existing evidence roots are refused rather than overwritten.

Pure validation: `python3 -I selftest_h10_crash_runner_p25.py`. Injected FakeIO runs the real frozen engine pre/post and real crash validator while socket/subprocess/urlopen tripwires prohibit all live IO. Cases cover full success, duplicate/wrong-label/wrong-image container refusal, pre failure/delayed pre/no active/held proof lost before kill, kill/start/health/post failure, actual protected generation takeover, incomplete healthy/unaccepted build, both release gates, and retained handles/raw artifact hashes. No Docker/HTTP/DB/Go was executed by the HTTP worker.

Docker CLI syntax checked against official [inspect reference](https://docs.docker.com/reference/cli/docker/inspect/), [container ls reference](https://docs.docker.com/reference/cli/docker/container/ls/), and [Go-template formatting](https://docs.docker.com/engine/cli/formatting/). `--no-trunc --quiet`, label filters, and `--format` with `json`/`index` are documented. No Docker command was invoked for the lookup.

## Preserved failedhealthy baseline — current candidate overrides old empty-owner assumption

Runner SHA `21231530d7582a66f91acfafda56f6dec5ed454d85f7deb0d9c693467de6c4f8`; frozen engine is now de6eb2bcbc6be34b57f6f7ab4da3be535fec9b140c2646bcbfcc9214395259a1. Prior 32a runner is superseded and must not execute. `--healthy-summary` now points to `EV/window-aggregate.json`; the runner invokes the actual pure `verify_aggregate` before trusting it. Its union handles path supplies all original/replacement source and def parents with origin SHAs.

Before creating the new held run, the runner performs real readonly enumeration of every imported owned def: exact CP-owner roster and nonterminal run roster, plus full public CP and protected Run snapshots and real source parents. They must match the aggregate's original two failed terminal owners and zero nonterminal runs exactly. `baseline.json` binds every raw HTTP record SHA. Post must have exactly baseline2 + held1 owners, only held1 nonterminal, and every prior def's complete protected snapshot must remain identical. `baseline-after.json` binds the actual after facts and raw SHAs; all owned fixture sources are also checked quiet. Unknown extras or changes in old owners/runs fail; no ignore rule or cleanup exists.

`tail-known-binding.json.known_bindings` contains `h10-held` and both existing actual owners under `failedhealthy-h13-<defUUID>`. Its `retained_owner_rows` contains their exact terminal-failed reason plus `origin=failedhealthy`, original source summary path and SHA. These are failed full-window history, not newly produced tail results. Tail must include these two rows and known bindings alongside h10 and the future h11/h14 rows. Six injectable pure tests cover original zero-owner success/refusals and real aggregate-provenance baseline2 success plus mutation of an old failed Run generation (rejected), running actual frozen engine pre/post and crash validator under socket/process/HTTP tripwires.
