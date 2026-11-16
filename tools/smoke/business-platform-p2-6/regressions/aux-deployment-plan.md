# W10/W11 executable configuration (Sol revision; supersedes draft pair map)

Code/pure only. No resource in this document has been created or restarted.
Source is `b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c`.
The NEW copied tree preserves original relative imports. Closure and origin SHA
are in `vendor/closure-ledger.json`; copied shells/templates are reference bytes,
**never invoked directly**. The facade intercepts the deadline driver and compose
mutations; original guard scripts are loaded only by pure import tests.

| Stage | NEW resource/config requirements | Actual suite tail after wrapper `--` |
|---|---|---|
| P1pre9 | Existing candidate base3, old fixture read/write only on fresh suffix; loops OFF. envdir connector base17088 | `--env-dir ENV` |
| P22full11 | Three independent C/A/B; C17100 and A17097 direct Dataset; B17098 via P22 proxy17099. workers1, queue64, taskTTL300, authTTL300; scheduler/recovery OFF | `--env-dir ENV --only D,E,F,G,H,I,J,K,L,M,N --connector2-base http://127.0.0.1:17097 --connector3-base http://127.0.0.1:17098 --dataset-proxy-base http://127.0.0.1:17099` |
| P23full8 | Same C/A/B/proxy. `--allow-restart` mandatory for Q. One inner guard substitution | `--env-dir ENV --only O,P,Q,R,S,T,U,V --connector2-base http://127.0.0.1:17097 --connector3-base http://127.0.0.1:17098 --dataset-proxy-base http://127.0.0.1:17099 --allow-restart` |
| P24full7 | C only scheduler ON; interval1s; A/B scheduler OFF. C owns fresh arms. Two inner guards; C stop/release/start; retain original grant/config readback assertions | `--env-dir ENV --confirm-http --official --with-restart-probe` |
| P25 healthy/tail | NEW copied P25 sidecars, exact commands below; stage replacement17095/96 only after W7 complete and deploy-owned identity phase. No `--env-dir` accepted | See below |
| P1post9 | Base3 real restart/readability first; connector base17088; loops OFF | `--env-dir ENV` |
| P21ABCfinal3 | LAST business write; candidate connector base17088. Afterward only whole-set/read-only/seal | `--env-dir ENV` |

17100(C)/17099(P22 proxy) are proposed NEW free ports pending Root/deploy inventory
review. A/B remain17097/98. Do not infer availability, start them, or map two
logical connectors to one CID. Do not use removed17092–94 as quiet evidence.
Old fixture17090 never restarts. All NEW connectors permit both `fixture` and
`p26fixture`; P25 stage additionally permits its NEW `p25fixture` alias.

P22 proxy exact executable is NEW
`vendor/tree/business-platform-p2-2/dataset_proxy.py`, command
`python3 -I /srv/dataset_proxy.py 0.0.0.0 8091 http://dataset:50078`.
Mount NEW copy read-only; host17099→8091; B Dataset URL is its actual NEW DNS alias
port8091. `/p22proxy/healthz` and `/p22proxy/*` barriers must answer. P26 proxy API
cannot substitute for this implementation.

For P22/P23/P24, deploy writes a private `.env` in ENV retaining required P1 keys
`OF_P1_PG_PASSWORD`, `OF_P1_JWT_SECRET`, `OF_P1_SERVICE_JWT`; never copy values to
results. Override `OF_P1_CONNECTOR_BASE=http://127.0.0.1:17100`. Dataset17078,
Pipeline17081, fixture17090 remain actual q3 endpoints. `ENV/compose.yaml` used
for postgres exec still resolves q3 Postgres. The wrapper's `aux_compose` and
`aux_project` route every old `connector/connector2/connector3` ps/start/stop/restart
onto NEW C/A/B service names, never q3 main. The wrapper forwards explicit URLs;
there is no substring port rewriting.

P22 J exact deploy-driver binding required in Root release:
`driver_commands["up-deadline:7"]` and `driver_commands["restore"]`, each
`{tool: absolute_NEW_executor, tool_sha256: actual_SHA, argv: exact_argv}`.
Executor checks phase, current NEW A CID/label and zero active current-owned tasks;
up-deadline reshapes ONLY A with
`CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS=7`, `CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS=300`.
It writes `ENV/compose-p22-deadline.yaml` using logical service `connector2` with
those exact two quoted keys (the legacy suite reads this file itself), preserving
raw config readback. J Source HTTP timeout is60000ms. Finally restore must recreate
ONLY A at taskTTL300/authTTL300 and provide actual changed boot; the legacy suite
checks boot and effective env. This is an interface requirement, not a delivered
or executed deployment driver. All baseline replicas set
`CONNECTOR_SYNC_ASYNC_WORKERS=1`, `CONNECTOR_SYNC_ASYNC_QUEUE=64`,
`CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS=300`, `CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS=300`.

P24 wrapper environment `OF_P24_CONNECTOR2_BASE=http://127.0.0.1:17097` is mandatory
(the original P24 helper reads this env key, never a CLI flag).
P24 C `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=1`. No recovery or registration
loops; fresh arms on its own NEW defs only. Actual returned `loop_running`,
arm/revision/readback and Source lock assertions remain in the unchanged suite.
Base MAIN scheduler remains OFF; original fixture unchanged. Stopping C must never
stop Postgres, A/B, main or fixture. Restart phase refreshes deployment pins before
next suite (guard pins never auto-adopt new boot).

P25 exact NEW sidecar entrypoints:
`vendor/tree/business-platform-p2-5/fixtures/rest_source_p25.py` and
`.../dataset_fault_proxy_p25.py`. Use copied compose only as reference and construct
NEW-owned mount/DNS/label/CID binding; no old path mounts/builds. `p25fixture:8090`
inside network host17095; proxy port8091 host17096, upstream actual Dataset50078.
Deploy worker owns any stage replacement; suite does not launch sidecars.

P25 real arguments (all via `--suite p25`, guard-count0 and exact Root suite_argv):

```
--phase healthy --released --only CASE
--connector-base http://127.0.0.1:17100 --dataset-base http://127.0.0.1:17078
--fixture-base http://127.0.0.1:17095 --proxy-base http://127.0.0.1:17096
--fixture-src-url http://p25fixture:8090 --evidence-dir NEW_EVIDENCE
--pg-proof-file ROOT_ACCEPTED_PG_JSON --pg-proof-name TestPGCheckpointAdvanceAllOrNothingRollback
```

Run one invocation per CASE in `h1 h2 h3 h4 h5 h6 h7 h8 h9 h12 h13 h15`;
`--only` accepts ONE exact ID, never comma lists. The copied rollback constant is
`TestPGCheckpointAdvanceAllOrNothingRollback`; accepted NEW PG proof must satisfy the original h12 schema/S assertions.
No retroactive A1→S editing of historical proof. P25 h13 specifically requires NEW C scheduler interval1s and real fresh arm;
A/B and main remain loops OFF. Its scheduler_loop_running assertion must pass.
P25 needs in-memory
`P25_CONFIRM=yes`, `OPENFOUNDRY_SMOKE_JWT`, `OPENFOUNDRY_SMOKE_JWT_ORG2`, optionally
`OPENFOUNDRY_SMOKE_ARM_JWT`; mint correct actual tenant/scopes privately.

h10 uses `--phase healthy --only h10 --h10-stage pre --h10-defs IDS
--h10-held-def NEW_HELD --h10-prior NEW_PRIOR` then deploy-owned real kill/same-image
start and actual record, followed by same tail plus `--h10-stage post
--h10-expected-image FULL_SHA256 --h10-restart-record NEW_RECORD`. Base flags above
still apply. h10 held data is NEW; none of old9/8 may be used as held Def.

Tail uses `--phase tail --released` (no `--only`, runs h11/h14 together) plus base flags,
`--sql-env-dir ENV --owned-handles-file NEW_PRIOR_HANDLES` (repeat as applicable),
`--tail-declaration NEW_HASH_FROZEN_SEMANTIC_DECLARATION`. Declaration must satisfy
original `p25-tail-roster-v1`; runtime bindings must be actual, no future UUIDs.
h13 busy proof uses original read-only SQL fact reader. Accepted h10/h12/h13/tail
supplements must be individually enumerated before run; aggregate count alone is
not acceptance. P25 complete accepted supplement copy/argv is still a follow-up
review item; this document does not claim those supplements already executable.

Final order: P26 HTTP + old regression writes; quiesce ONLY NEW owned; actual
base3 restart/readability; P1post9 + old artifact reads; P21ABC3 LAST write;
whole old+typed-NEW guard + loops/grants/fixture/images read-only; seal/offline.

P22 N additional mandatory setup: Root `legacy_cancel_probe` must reference an
actual NEW typed legacy-null/running sample (Run/Def/Source/tenant/actor), created
by a copied P21 C-only setup if needed, then frozen by actual safe27/protocol/
parent/public evidence. Original N otherwise targets old C; wrapper refuses that
path. Full11 cannot start without the NEW probe binding. C-only setup is an extra
setup batch, never a substitute for finalABC3. The finalABC3 remains last writes.

P25 stage C must additionally switch its actual `DATASET_SERVICE_URL` from direct
Dataset to the NEW P25 fault proxy's network alias port8091, retaining scheduler
interval1s and p25fixture egress. P22 uses C/A direct and B via P22 proxy; P24 C is
direct Dataset with scheduler1; P25 C via P25 proxy with scheduler1 is a separate
deploy-owned stage configuration, not an implicit inherited mapping. h10 kills
only that NEW owned connector by SIGKILL then starts the identical image; graceful
restart does not satisfy the old crash assertion. Parent/base3 remain untouched
until their separately released final restart phase.

Correction after actual cancel.go read: N unsupported branch requires `control_mode=legacy`, independently of `recovery_protocol=NULL`. P21 C-only creates controlled/NULL and CANNOT supply this probe. Full11 requires actual NEW legacy fixture with real HTTP-created parent chain and a separately Root-authorized NEW-row-only setup; no such setup has run. Typed guard additionally requires Root `legacy_whole_baseline_27` from pre-setup whole-legacy read and compares old+NEW whole legacy set, including shadows. No probe or baseline means full11 refuses.

Lifecycle clarification: P22 I stop/start precedes J and changes A/B/C boots with
same CIDs; M later restarts them again. Facade verifies actual same-CID/image
transition and retains its new boot in a local chain. For J, Root can pin
`driver_predecessor_file` under NEW results/w10 plus current adapter tool SHA;
the pre-J A start receipt binds release, previous/current identity, original
start argv/rc and actual after-inspect. Deployment driver must consume this
known-CID boot transition before J, without name adoption or silently ignoring
boot. After J accepted recreation, later M transitions do not overwrite that
predecessor evidence. Root refreshes phase records before the next suite.

h12 current Root-accepted proof is absolute NEW
`/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w6/pg/h12-rollback-compat-proof/proof.json`.
Normal/race missing rollback singleton supplements are accepted, unified reused;
raw/rc bytes preserve original artifacts, KVmeta conversion is explicit. Earlier
NOTREADY audit remains history. This supersedes the previous h12 gap note.
