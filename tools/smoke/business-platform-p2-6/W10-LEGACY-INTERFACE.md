# W10 — Legacy HTTP regression adaptation interface (P26, code/pure stage)

Owner: W3（独家 NEW `regressions/` + 本文件 + `results/w10/`）。Go/SQL 冻结 S 不动。
依据：`legacy-regression-preparation-authorized.md`（2026-11-15）。**本阶段 code/pure only**：
无 actual HTTP/Docker/PG/service config——live 执行等 Root phase gate（adapter 已内置拒绝）。

## 1. 精确必需集（READ-ONLY 派生，逐套引用旧接受证据）

| 套件 | 数量 | 精确 case 名（源自引用报告） | 证据路径（tools/smoke/business-platform-p2-5/results/2026-11-14-p25-final-standalone-attempt3/ev/steps/） |
|---|---|---|---|
| P1 pre9 | 9 | build_isolation, build_positive, connector_isolation, dataset_isolation, hash_change, legacy_regression, pipeline_isolation, sync_failures, sync_real_rest | regressions-p1pre9-attempt1/{cmd.txt,p1pre9-report.json} |
| P1 post9 | 9 | （同上 9 名） | p1post9-attempt1/{cmd.txt,p1post9-report.json} |
| P2.2 full11 | 11 | async_admission_and_completion, running_cancel_cross_process, queue_pending_cancel_and_queuefull, gate_wins_too_late_via_proxy, tenant_and_binding_denials, sigterm_shutdown_no_redispatch, deadline_bounds, sync_disconnect_compat, jwt_expiry_no_fallback, restart_state_preserved, legacy_cancel_unsupported | regressions-p22-attempt1/{cmd.txt,result_p22.json} |
| P2.3 full8 | 8 | admission_capacity_shared, admission_release_matrix, admission_min_interval, denial_precede_throttle, admission_config_validation, contention_real_db_lock, public_private_field_sweep, concurrent_inflight_invariant | regressions-p23-attempt1/{cmd.txt,result_p23.json} |
| P2.4 full7 | 7 | g1..g7 | regressions-p24-attempt1/{cmd.txt,p24-report.json} |
| P2.5 current15 | 15 | h1..h15（aggregate 替换语义照 accepted WINDOW-AGGREGATE 契约） | window-attempt2 + HANDOFF-WINDOW-AGGREGATE.md |
| P2.1 ABC final3 | 3 | heartbeat_progress, lease_loss_fencing, terminal_commit_fault | p21abc-attempt1/{cmd.txt,result_p21.json} |

Base3 restart/readability side-proof（Dataset/Pipeline/Connector，post-restart 只读）随
P1post9 阶段执行；旧 optional 5（provider/cluster/sidecar）**明示 out of scope**。

## 2. Vendor 闭包（byte-identical；执行仅 NEW 副本）

`regressions/vendor/`：p1/{run.sh,mint_token.py,init.sql,compose.yaml.template,README.md,
fixtures/**}；smoke_p21/p22/p23/p24/p25.py；p23/p24_live_guard.py；p25_old_suite_adapter.py
（机制参考）；p25_fixtures/{dataset_fault_proxy_p25.py,rest_source_p25.py}。共 17 文件，
ORIGIN-COPY-LEDGER.md 逐文件 origin/copy sha256 + identical=True。**旧 P25 tools85 与
archives 一律不改不执行不 import**。

## 3. 窄适配清单（唯一允许的替换；adapter manifest 打印同一计划）

1. candidate source 校验：旧 adapter 硬编码 A1 `a1bff07f…` → 参数化 `--candidate-source
   <frozen-S 全 sha256>`（phase 时由 W6 ledger 供给；本文件不发明 S 中段字节）；历史 A1
   证据保持原值有效。
2. suite 加载：沿用旧机制（frozen module 不改、scoped sys.argv、main 转发）——针对 vendor 副本。
3. guard argv：`[python3,-I,<guard>,dir,connector]` 精确形状 → W10 shim；形状不符拒绝；
   计数强制 p23=1、p24=2；original/replacement/SHA + 真实 rc/stdout/stderr 留档。
4. aux：旧 17092/93/94（已删，不得作为 quiet proof）→ **仅新 A=17097、B=17098**（W6 所有）。
5. env-dir/URL：旧 q3v9 → W6 候选部署 env-dir（base URL/端口仅来自该 env）。
6. p1 闭包路径绑定 vendor/p1；JWT 用 vendor mint_token + 私有 env 真实 secret（不造假 Bearer）。
7. 镜像：DVS=4217 候选、Connector=f23f 候选、Pipeline 沿用旧镜像。
8. 守卫面：旧受控 9 + owner 8 + parents/outputs **不碰**；typed NEW owned guards 只围新 9
   HTTP + 旧 ABC；roster 全集相等（无全局 UUID 过滤）；shadow unknown 一律失败。
9. main cfg：cron/sweeper/recovery 保持 OFF；旧 legacy NULL protocol 永不 adopt；
   fixture 17090 永不 restart（boot 2026-11-13T18:29:14.176785055Z）。

业务断言零弱化：不替换为假协议/弱检查、不发明字段、不重写 harness。

## 4. W6 需提供的精确部署计划（本阶段请求；无 secret 值）

- base3（Dataset 4217 / Connector f23f / Pipeline 旧镜像）候选部署 + deployment-record
  （含 frozen-S 全 sha256、boot RFC3339、image full64hex）与 selected-build。
- aux A/B（17097/98）容器 config 全量：workers/capacity/timeouts/cron/sweeper flags/
  ports/DNS/egress + **同时服务 fixture-17090 内部 host 与 p26 fixture host**（避免配置
  漂移）；CID/env ledger（真实值 live 阶段内存供给）。
- 若 P24 需 scheduler loop/arm：仅 NEW aux stage cfg 启用，不开旧 Source 全局 adoption；
  P24 live-arm 谓词天然保护旧 grants（无则显式说明）。
- base3 restart 时序（final 阶段）与 quiesce 协调（见 §5）。

## 5. Final ordering plan（准备，不执行；Root 可按旧 accepted 契约微调）

1. 新 P26 HTTP（W7 场景）与旧回归**全部业务写**完成。
2. 静默（quiesce）**仅新 owned** SourceDefs/ticks/grants/aux workers；unknown owners 保留。
3. base3 实际 restart（Dataset/Pipeline/Connector）→ 只读 side-proof。
4. P1 post9 + 必需旧 artifacts 只读复验。
5. **P2.1 ABC final3 = 最后业务写批次**（新 B/C unknown 行走 typed new protected projection，
   无隐式 adopt）。
6. 终态整集捕获：old controlled 9 + new owned 全集相等；owner 8 + new owned 全集相等、
   无 extra；source/image loops=0、grants none、旧 fixture 计数静默、旧 artifact 读证明。
7. Root archive worker 最终 seal/offline bundle（secretScan 后）。

## 6. 本阶段验证（results/w10/attempt1-code-pure，原件+rc）

py_compile（vendor 全 .py + adapter）rc0；`bash -n vendor/p1/run.sh` rc0；
adapter manifest（ledger 17/17 identical + 7 套 roster 计数全对 + 9 项窄适配计划打印）
rc0；`--root-released` 拒绝 rc1（live gate 生效）。未加载任何 suite 模块、零子进程、零网络。

## 7. Sol executable revision (supersedes §2–4 and draft execution claims)

NEW adapter `regressions/adapters/w10_suite_adapter.py` now parses real required
`--suite {p1,p21,p22,p23,p24,p25} --release-file ROOT_JSON
--candidate-source-file ACCEPTED_S_JSON --env-dir ENV --expect-guard-calls N
--record EXCLUSIVE_NEW_JSON -- SUITE_ARGS`. Missing or mismatched Root exact
phase/suite/argv/S/count is rejected before suite import/runner. The old text
release token and draft arbitrary substring port rewriting are removed.

The active closure is `regressions/vendor/tree/`, preserving old sibling layout;
`vendor/closure-ledger.json` records 17 actual original paths/origin SHA/copy SHA,
all byte-identical and no patches. Flat `vendor/*.py` are historical preparation
copies and never execution targets. New adapter/guard/test code is NEW code,
not counted as a copy from itself. No original old script is imported/executed.
All originals remain read-only; `.pyc` writes are disabled.

Real imports of P1/P21/P22/P23/P24/P25 + snapshots + guard pins pass. Pure tests
actually enter an imported suite's patched main with scoped argv, fake subprocess
port, rc17 guard return, exact guard count, owned compose mutation route, argv/run
restoration; phase mismatch and old fixture mutation prove zero runner calls.
`selftest_w10_guard.py` uses the verified sealed filesystem baseline and actual
current source hash, injecting all Docker/SQL/HTTP. Positive and wrong CID,
whole-SQL shadow row and rc9 negative checks pass. This is pure evidence, **not
an HTTP acceptance result**. Both scripts record actual call count zero.

The guard calls NEW W8 helpers only. It checks actual current source equals full
S; actual Docker CID/full `sha256:` image/boot/running vs Root-pinned deployment
record; immutable old fixture boot; whole controlled27 and CP-owner rows equal
old sealed + typed NEW expected rows; scoped parent union and safe public
checkpoint/Run projections. SQL shadow extras are compared before any old-only
parent provenance subset. Typed rows bind Run/Def/Source/tenant/actor/control_mode/
status/gate_entered to actual full27 baseline and original JSON evidence hash+
JSON pointer. The typed ledger is Root-pinned, never treated as live proof.

Root release schema:

```
{
  "schema": "p26-w10-release-v1", "root_accepted": true,
  "suite": "p23", "suite_argv": ["--env-dir", "ABS_ENV", "...exact real args"],
  "candidate_source": "b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c",
  "expect_guard_calls": 1,
  "deployment_record": "ABS_NEW_JSON", "deployment_record_sha256": "ACTUAL_SHA",
  "typed_roster": "ABS_NEW_JSON", "typed_roster_sha256": "ACTUAL_SHA",
  "aux_compose": "ABS_NEW_AUX_COMPOSE", "aux_project": "ACTUAL_NEW_PROJECT",
  "service_map": {"connector": "ACTUAL_NEW_C", "connector2": "ACTUAL_NEW_A", "connector3": "ACTUAL_NEW_B"},
  "driver_commands": {"up-deadline:7": {"tool": "ABS_NEW_DRIVER", "tool_sha256": "ACTUAL_SHA", "argv": []}, "restore": {"tool": "ABS_NEW_DRIVER", "tool_sha256": "ACTUAL_SHA", "argv": []}}
}
```

Placeholders are documentation, never runnable release input. Deployment record
`{source_sha256:S, containers:{actual_name:{cid:full64hex,image:fullsha256,
started_at:actualUTC}}}` must include base3+old fixture and current NEW aux used.
Pin current actual boot per phase; refresh after restart, never auto-adopt.
Typed empty first-slice baseline is exactly
`{"schema":"p26-w10-typed-v1","runs":[],"public_views":[]}`; it fails if actual
whole SQL includes NEW held/unknown rows. Each typed run requires `run_id,def_id,
source_id,tenant,actor,protocol,purpose,expected_state,pubentered,safe_projection_27,
parent,evidence:{path,sha256,json_pointer}`; CP owner rows optionally add actual
`cp_owner_row_full`. Public views must cover exactly typed Def union, each with
`def_id,actor,tenant,checkpoint_status,baseline_cp,baseline_runs`; checkpoint404
is explicit and retains exact public Run whole-set comparison. Existing old
public views always use sealed checkpoint200 and full Run baselines.

Detailed exact CLI/config/ports and pending deployment driver requirements are
in `regressions/aux-deployment-plan.md`. Three independent NEW C/A/B plus NEW
P22 proxy are required; proposed C17100/P22proxy17099 await Root/deploy inventory
review. The previous two-aux plan was incorrect. No actual resources were started.

Remaining before full old live release: deploy-owned J deadline/restore executor
and refreshed CID/boot phase ledger; Root accepted typed roster; P25 accepted
h10/h12/h13/tail supplemental closure and declaration review. P1/P22/P23 facade
is executable once their concrete release/deploy inputs exist. No HTTP run is
claimed by this delivery.

### Review hardening follow-up (authoritative over §7 protocol example)

CLI also requires `--stage STAGE`. Root release requires `stage`, `suite`, and
future UTC `expires_utc`; all subprocess routes and guard reads recheck it, and
changes to release bytes during a suite refuse. `evidence_root` stays under NEW
p2-6. W8 `Attempt` is reused: command receipts exist BEFORE execution, scrubbed
stdout/stderr and original rc survive failure; public checkpoint/Run safe raw
bodies + status are retained. Each compose mutation additionally verifies actual
source and actual NEW target CID/full image/boot/Running against the pinned
record. Full Config.Env inspection and unpinned Docker targets are refused.

Typed `protocol` means actual **recovery_protocol**, e.g. `p26-v1` or explicit
`legacy-null` for SQL NULL. It is queried independently with safe SELECT and
never compared to control_mode. Separate typed `control_mode` remains
`controlled` for these rows. Historic safe27 schema is unchanged. Positive typed
p26-v1/controlled and wrong-protocol negative tests now exercise the entire guard.

All SQL commands default to Postgres `BEGIN READ ONLY`/`COMMIT`, including SELECT
calling mutating functions; Root can release exact original argv SHA256 via
`sql_write_argv_sha256` (canonical `json.dumps(argv,separators=(',',':'))` bytes).
P1 publish-failure constraint injection, P21 ABC fault expiry/trigger writes, and
P23 T's Source NOWAIT lock still need narrow actual-owned bindings before those
live slices can pass; these required business assertions are retained, not
silently skipped. This revision does not claim full legacy live readiness.

### Owned SQL and deadline integration revision

`regressions/owned-sql-review.md` is the exact Root review checklist. NEW runtime
HTTP observation now binds P1 CHECK target, ABC expiry Run/trigger Def, and P23
NOWAIT Source to this invocation's actual creation identities/tenant/parent.
`owned_sql_templates` explicitly enables those four named templates; DDL also
requires `serial_owned_window:true` plus actual zero own inflight query. Final
cleanup pending after child failure gates the wrapper verdict. No copied file
bytes changed; runtime spec instrumentation is limited to the exact NEW P1 path.

P22 N now has an additional required `legacy_cancel_probe` binding to typed NEW
legacy-null/running sample. Original N's old-C cancel path is prohibited. A NEW
C-only setup may create the sample before full11; it never counts as finalABC3.
Full11 refuses absent probe. FinalABC3 still runs last.

Deadline integration accepts only Root-pinned NEW driver's exact SHA+argv, then
requires original hashed proof `previous→current`, matching release/action,
unchanged image, changed CID/boot, exact four-key config, and actual current
CID/image/boot/running + of-p26/compose ownership labels. Only that A identity
is added to an in-memory dynamic chain; Root original record is unchanged.
`compose config` is a narrow actual A inspect of four safe integer env keys,
with raw receipt retained and logical `connector2` projection returned to the
unchanged J assertions. Full Config.Env dump is never read. Deployment owns driver
and stage compose/config hashes; see its `legacy-stage-checkpoint.json`.

Spawn/timeout failure has `.rc=unavailable` and metadata `original_rc:null`.
Actual returned child exit codes remain unchanged. Output paths are exclusively
NEW `results/w10`; existing suite record paths refuse before a call.

### Holder, cleanup, frozen B and N type review fixes

`w10_holder_seam.py` wraps the original copied long-lived `_SourceRowLock`
methods. Actual Popen, exact BEGIN/Source FOR UPDATE/PG backend pid stdin,
select/read loop, ROLLBACK and kill/reap still run; only registered actual-created
Source IDs and the original command factory are allowed. Stream/PID/child exit/
cleanup receipts are retained. `selftest_w10_cleanup_holder.py` proves original
copied acquire/release with injected child I/O, and expiry cannot block ROLLBACK.

Outer `Facade.cleanup` always runs before restoring runtime hooks, with independent
bounded cleanup authority for exact this-invocation temporary CHECK/trigger/
function commands and tracked holder processes. It handles ABC trigger creation
failure before the original try. Child failures remain original; cleanup failure
stays pending in the final result. Global objects must be absent before install,
so a foreign existing object is never adopted or cleared.

`w10_abc_held.py` observes successful completion of original B (thread join,
HTTP409, no target publish, unchanged counters over original27s window), then
collects exact actual new Run safe27/parents/tenant and DB-clock-expired lease/
NULL protocol. Before C DDL, the exact frozen B is re-read unchanged and expired,
then only that ID is excluded from ACTIVE executor count. B stays running and is
never adopted/cancelled/cleared/terminalized. Other pending/running owned rows
still block DDL. `selftest_w10_abc_held.py` rejects fresh lease or safe27 drift.

N actual unsupported contract is `control_mode=legacy`, independently of recovery
protocol NULL. P21 C-only produces controlled/NULL and cannot satisfy it. The
binding is scoped only around `_legacy_owner_identity` (restored in finally), so
I/M's original old B/C read-only snapshot assertions keep their original targets.
The NEW legacy INSERT proposal/collector is in `p22_legacy_probe_setup.py`, with
no CLI; Root must explicitly release NEW fixture setup. It verifies actual-created
parents, a new UUID absent before INSERT, safe27/protocol/parent before/after and
whole legacy running set equality. Public checkpoint/Run snapshot and Root typed
acceptance still precede N. No actual setup has run.

Typed guard now reads actual full27 for every typed Run even if it falls outside
controlled/live predicate; when NEW legacy rows exist it additionally compares
`legacy_whole_baseline_27 + NEW legacy` to the actual whole pending/running legacy
population. A separate legacy shadow negative test passes. No returned extras
are filtered from either whole controlled or whole legacy comparison.

h12 earlier audit NOTREADY is retained as history. Root has now accepted the
missing same-S exact rollback normal/race supplemental runs; current proof is
`results/w6/pg/h12-rollback-compat-proof/proof.json`, with original-chains.json and
raw/rc byte-equal copies plus explicitly derived KVmeta. Original unified evidence
is reused; no original A1 evidence changed. Root accepted
`h12-rollback-same-S-accepted.json`; unchanged NEW copied P25 h12 pure consumption
was independently accepted. Use that proof path for actual h12; no PG rerun.
