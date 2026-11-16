# Executable final preservation interface

`python3 -I -B ABS/regressions/adapters/w10_final_guard.py --release-file ROOT --output EXCLUSIVE`

Root release schema `p26-w10-release-v1`, `root_accepted:true`, stage
`final-typed-preservation`, suite `p26-final`, release_id, future expires_utc,
actual frozen candidate_source, evidence_root under NEW results/w10, exact
final_output, final_tools_sha256 equal to the seven paths in `w10_final_guard.TOOLS`.
Inputs with exact SHA: deployment_record, typed_roster, final_aux_ledger.
The candidate_source is read from Root; this guard contains no fixed business S.

The deployment record includes all five original q3 names, full CID/image/boot,
source_sha256 and current Root-accepted main boots. Optional NEW aux pins may
carry `expected_running:false` plus their Root-pinned `ownership` or `labels`
containing of-p26-owner=p26-http/of-p26-stage=p2-6. Main5/oldfixture cannot be
expected stopped. Current identity must match all fields and the exact state;
missing containers are not accepted as quiet. Default expected state is true.

The typed roster is actual `p26-w10-typed-v1`: every retained NEW nonterminal
controlled Run and every NEW checkpoint owner, including failed terminal owners,
plus the explicit N legacy probe. Full27 original evidence path/SHA/pointer,
Def/Source/output/tenant/actor, independent recovery_protocol (legacy-null maps
SQL NULL), full CP owner if retained, safe full parents, and actual full public
checkpoint/Run baselines are required. The N entry retains the accepted historic
BC2 whole-legacy baseline. Terminal NEW Runs without retained CP owners need not
be added merely for historical existence; every Run of a retained Def remains
in its complete public baseline. Final ABC B/C unknowns are retained exactly;
no terminal state is invented and no unknown is cancelled/adopted/cleared.

`final_aux_ledger` is Root-accepted JSON:

```
{schema:"p26-final-owned-aux-v1",source_sha256:ACTUAL_SOURCE,
 containers:{ACTUAL_NEW_NAME:{cid:FULL64,image:FULL_SHA256,started_at:ACTUAL_UTC,
 running:false,ownership:{"of-p26-owner":"p26-http","of-p26-stage":"p2-6",...}}}}
```

All three NEW connector names p26connectora/b/c must be present and stopped.
Include every other NEW p26-http/p2-6 owned container (fixture/proxies etc.) with
its exact expected state. A WHOLE labelled Docker inventory must equal this
ledger plus any explicitly pinned main container carrying that same label;
unknown/extra names/CIDs refuse. No removed 17092–94 resource is used as quiet
proof. The full final guard is before container removal; a future removal phase
needs separate Root authority and receipts, not a missing-CID exemption here.

The executable first calls W10's actual whole controlled=old9+typed NEW and
whole CP=old8+typed NEW comparator, exact parents/protocol/full safe27/public
baselines. It never calls old-only 9/8 comparators on the extended population.
Then it executes the other original NEW W8 readonly specs and predicates:
main5 Docker inventory/whitelisted identity/mounts/ports/health/network/env names,
base3 service health, original grant query, oldfixture fixed boot and every fixed
src body SHA, old committed transaction and full file bytes/hash through the
real302 then one anonymous same-origin HMAC-safe follow. Original base3 images
must still inspect by full image ID. An additional WHOLE schedule-grants table
SELECT must return exactly[]; no unknown grant is hidden by owned-ID filtering.
Connector loop env NAMES must preserve accepted absence/defaults=OFF. Stopped
NEW connector actual Running=false is the executor-zero proof; live held Run
counts are not mislabeled as active executors.

Every actual command/request has a BEFORE receipt, original Unix child rc,
redacted raw stdout/stderr, unavailable/null rc for spawn/timeout or HTTP (HTTP
status separate), immutable full safe JSON and actual file bytes. Final source
and seven-tool SHA map must remain unchanged. No mutation is implemented.

# Main3 restart bridge for the same whole guard

Minimal guard CLI remains `python3 -I -B ABS/adapters/w10_guard_shim.py ROOT`.
To permit precisely Root-authorized same-CID main boots in that guard, Root adds:

```
main_restart_output: ABS_NEW_RESULTS_OUTPUT,
main_boot_transition:{executor_tool:ABS_NEW_EXECUTOR,
 executor_tool_sha256:ACTUAL_TOOL_SHA,guard_extension_sha256:ACTUAL_HELPER_SHA,
 ledger_dir:ABS_UNDER_OUTPUT,allowed_service_order:["dataset","pipeline","connector"],
 restart_commands:{dataset:EXACT_ARGV,pipeline:EXACT_ARGV,connector:EXACT_ARGV}}
```

Helper: `adapters/w10_boot_transition.py`. Commands are exactly docker restart
one full initial Root CID, with no option or --time120/--timeout120. Header
`ledger_dir/00-anchor.json` equals schema p26-main-boot-transitions-v1,
release_id, executor_tool_sha256, base_deployment_record_sha256, allowed_service_order.
No invented Root release is written. Immutable contiguous prefix files only:
01-dataset.json, 02-pipeline.json, 03-connector.json. Each contains schema
p26-main-boot-transition-v1, release_id, service, previous/current dictionaries
with exactly cid/image/started_at, before/after dictionaries with four receipt
prefixes identity/mounts/ports/env_names, and restart_step receipt prefix.

Every receipt has .cmd.json (argv list or only {argv:list}), .raw, .rc0,
.meta.json original_rc0 or original_child_rc0. Before/after inspect targets the
exact CID and helper IDENTITY/MOUNTS/PORTS/ENVNAMES format constants. Original
identity must be previous/current respectively; same CID/image, changed UTC boot,
mounts/ports/env names unchanged; Connector loops absent. The helper hashes all
original bytes and derives only main3 StartedAt. W10 still actually inspects all
current pins, whole SQL and full public baselines, and verifies receipt bytes
and ledger file set unchanged at completion. Unknown services, skipped steps,
changed CID/image/ports/mounts/env, nonzero/missing rc or writer/helper SHA drift
refuse. The extension never adopts by name or changes oldfixture/PG/aux pins.
