# P2.6 W9 — 封存准备/独立离线verifier 接口（DRAFT，未封存）

状态：W9 新增 archive 工具与纯测（本阶段 code/pure only）。**实际最终 seal 必须等
Root 全部里程碑（HTTP、旧回归、保全、清理）完成后另行 phase——本工具绝不产出
placeholder PASS；对当前状态一律 fail-closed `NOTREADY`。** 依据
`handoffs/2026-11-15-p26/archive-preparation-authorized.md`。

## 1. Ownership（W9 独占新增）

| 文件 | 说明 |
|---|---|
| `archive/p26_archive_manifest.py` | 确定性 manifest build/verify + 9 项失败控制 selftest |
| `archive/p26_secret_scan.py` | 模式审查 + in-memory ACTUAL 值扫描（redacted）+ 9 项控制 selftest |
| `archive/p26_archive_verifier.py` | 独立离线verifier（S标签recipe/manifest/证据规则/secret/fail-closed）+ 13 项控制 selftest |
| `archive/p26_roster_crosscheck.py` | 正式 roster 核验：绑定 Root 外部 accepted JSONs + 工作区证据存在性 + pending 一致性（over-claim/stale 双向拒绝）+ 11 项控制 selftest |
| `archive/p26_formal_roster.json` | DRAFT-2 正式 roster（4 accepted + 6 pending + prepare/W8静态plan注记：plan永不满足） |
| `archive/PACKAGE-README.template.md` | 未来包 README 模板（离线验证命令/证据口径/边界） |
| `W9-ARCHIVE-INTERFACE.md` | 本文件 |
| `results/w9/` | 纯测/校验原始 cmd/rc/stdout/stderr 存档 |

旧 Go/SQL、W1b ack interface、既有 results、W6/W7/W8 文件零改动。旧 P25 工具与
sealed archives 仅作只读概念参考，绝不 import/执行/修改；W9 全部为 fresh stdlib
Python。无新框架/依赖。

## 2. 工具 CLI（均 `python3 -I -B`，stdlib only）

```bash
# 确定性 manifest（根级 manifest.json 自排除；symlink/穿越/重复/多余/缺失全拒绝）
python3 -I -B archive/p26_archive_manifest.py build  <pkg_dir> [out.json] [--expect-list <file>]
python3 -I -B archive/p26_archive_manifest.py verify <pkg_dir> <manifest.json> [--expect-list <file>]
python3 -I -B archive/p26_archive_manifest.py selftest

# secret 扫描（模式：Authorization/Bearer/JWT形/预签名sig/PG DSN/敏感env赋值；
# test/fixture 模式字面量分类非 fatal；ACTUAL 值扫描所有 regular bytes且永不豁免，经 --values-stdin 进内存，
# 报告仅 kind+file:line+长度，绝不含值本身）
python3 -I -B archive/p26_secret_scan.py scan <pkg_dir> [--values-stdin] [--json]
python3 -I -B archive/p26_secret_scan.py selftest

# 独立离线验证（不读原workspace/无私env/无Docker/PG/HTTP）
python3 -I -B archive/p26_archive_verifier.py verify <pkg_dir> \
    --canonical-root-label /Volumes/ExternalSD/projects/OpenFoundry \
    [--roster <包内相对roster.json>] [--emit-report-skeleton out.json] [--values-stdin]
python3 -I -B archive/p26_archive_verifier.py selftest
```

## 3. 离线验证契约（"offline verification" 定义）

- 包内**一切**工件用相对路径解析；原项目绝对路径仅作 canonical LABEL 出现在
  Go+SQL source-hash recipe 的行文本中，**从不打开**（代码构造上无 open 调用）。
- S recipe 与已验收 `common/p26_gates.py candidate_stream` **逐字节等价**，但由
  `source/` 快照字节计算；已在真实树副本上证明
  `offline_recipe_sha == S(b94e6b2f…14c)`（results/w9 attempt1 step 04）。
- manifest：精确成员（缺失/多余双向失败）、逐文件 sha256/size、确定性总 SHA、
  根级 manifest 自排除、symlink（文件与目录）与路径穿越/绝对路径/重复一律拒绝。
- 证据规则：`classification` 必须为 `actual`（`fake/plan/synthetic/mechanics-fake/
  history-only` 只作历史，永不满足）；`rc_kind=unix` ⇔ rc==0；
  `rc_kind=http-status` ⇔ rc 是 100..599 的 HTTP 状态、等于 expected_status、且
  outer_rc（外层 CLI Unix exit）单独记录为 0——**HTTP 步的 .rc 是状态不是 Unix
  rc，二者绝不混淆**；工件路径必须包内相对、不逃逸、非 symlink。
- Root final acceptance：包根 `root-acceptance.json`
  `{roster_complete:true, accepted:[{id:<pending id>,satisfiers:[<实际证据id>]}]}`；缺席/不全 ⇒ `NOTREADY`。
  Root 明示 roster 权威，任何计数或 "PASS" glob 不替代。
- 判定：最好情形 `VERIFIED-NOT-SEALED`；seal/发布是 Root/W10 阶段，本 verifier
  永不 seal、永不对 pending 范围打印 PASS。

## 4. 包布局（封存阶段装配时）

```
<pkg>/
  manifest.json                 # p26-archive-manifest-v1（自排除）
  source/{go.mod,go.sum,services/*,libs/scheduling-cron}   # S recipe 输入
  roster/p26_formal_roster.json # 本 roster（Root 定稿版替换）
  evidence/evidence.json        # steps[]: id/artifact/rc/rc_kind/classification/...
  evidence/w6-pg/... evidence/w1b-final-unit/... evidence/w6-build/...
  root-acceptance.json          # Root 终局（现在不存在 ⇒ NOTREADY）
  # verifier报告输出到包外（不能修改已manifest冻结成员）
```

```bash
# 正式 roster 核验（工作区只读模式；缺项/超claim/过期roster全拒绝）
python3 -I -B archive/p26_roster_crosscheck.py check \
    --roster archive/p26_formal_roster.json \
    --pg-accepted <S-first-PG-all-accepted.json> \
    --build-accepted <S-build-two-composite-accepted.json> \
    --unit-accepted <final-unit-S-accepted.json> \
    [--extra-accepted <prepare/W8静态json>]... \
    [--repo /Volumes/ExternalSD/projects/OpenFoundry]
python3 -I -B archive/p26_roster_crosscheck.py selftest
```

## 5. 本阶段已执行（results/w9/2026-11-15-w9-attempt1/ 与 attempt2/）

| step | 内容 | rc |
|---|---|---|
| 01–03 | 三工具 selftest（manifest 9 控制 / secret 9 / verifier 13，含 fail-closed、HTTP rc 混淆、plan 分类、非本地工件、secret suspect、manifest 多余等反例） | 0/0/0 |
| 04 | S-parity：真实树只读副本上 `candidate_hash_from_snapshot` == S | 0（matches_S=true） |
| — | 工具/roster SHA256 + python 版本存档 | — |

attempt2（results/w9/2026-11-15-w9-attempt2/）：

| step | 内容 | rc |
|---|---|---|
| 01 | crosscheck selftest 11 控制（含 roster 超claim/过期双向拒绝） | 0 |
| 02 | **真实核验**：draft-2 roster × 真实 5 个 Root accepted JSONs × 真实 repo 证据目录存在性 → `CROSSCHECK-OK`（S一致、7链54+114、composite保留、unit 8/15/39+caveat、pending覆盖、extra=prepare+W8静态plan） | 0 |
| — | 首跑因 image `sha256:` 前缀格式差异 REFUSED（原件 `02-crosscheck-real.first-run.REFUSED.stdout` 保留），工具规范化 `sha256:<64hex>` 后重跑 rc0 | — |

纯测只在 tempfile 夹具上运行，不读 repo、无网络/DB；"纯测证明的是工具
mechanics，不证明任何真实 PG/HTTP 执行"。

## 6. 边界与不做

- 不执行/导入旧 P25 tools；不复制其大框架；不复用其 sealed archives 字节。
- 不做实际 seal、不装配最终包、不宣称 roster 定稿——等 Root 全部里程碑与
  `root-acceptance.json`。
- composite build 语义在 roster 中显式保留（原失败历史原样、resume rc0 才是
  satisfier；historical builder sha 3a546d… 与当前 null-safe 变体分别标注）。
- optional external provider/cluster/sidecar 未验证、非 verbose skip 不可枚举——
  roster 与 verifier 均不宣称。

## 7. Sol draft-3 窄修与原件 checkpoint（覆盖 draft-1/2 弱合同）

2026-11-15 attempt3 仅 code/pure/读取已存在非秘密原件；未装配最终包，未读取私env
秘密值，未 seal。上述 attempt1/2 保持原样，其“存在性核验”不作为正式业务证明。

- 正式 verify 首先要求 manifest exact fileset/hash/size/nonlink，再读取包内 roster；
  外部 --roster、空/重复 satisfiers、缺省/重复/额外必需范围 ID 均拒绝。CLI必须
  从复制包内工具执行。canonical source 根只用于标签，不打开原路径。
- 每个 satisfier 的 Root roster contracts 与 evidence step 逐字段相等，并通过
  p26_original_proofs.py 复读原件；original_files 提供该链实际读取原件的精确
  SHA/size 集合，未知/缺失/修改均拒绝。draft-3 pins 待 Root 审阅，非终局 roster。
- PG 逐名 1 RUN/1 PASS/0 SKIP/FAIL，原始 process/wrapper/各gate rc0，Go版本、
  cmd/meta、source before/after S、outer/inner路径绑定；七链168必需名已复核。
- unit 六原始 rc0，before/after S，原始 Go版本与 8/15/39 唯一 Go OK 包输出；
  optional scope 与非verbose per-test skip仍不作保证。
- build 原始 stage04–07 cmd SHA 链、binary字节 SHA、commit ID、08verify rc1、
  原始outer rc2、resume outer0、原始resume步骤/cleanup/config/source都复核。
  历史旧 builder source blob 未恢复；不能用当前 null-safe 脚本冒充旧版。
- pending 关闭必须为 {id,satisfiers}，相等 roster 且逐证据复核成功；单纯ID列表
  永不关闭。HTTP/legacy/typed保全等尚无本读取器的语义实现，保持NOTREADY；
  不用泛化 rc0 代替对应业务断言。Unix rc与HTTP status区分规则保留。
- ACTUAL 值扫描所有regular文件字节（.log/.body/bin/fake路径也fatal），
  模式扫描包含 ?sig=/&sig=/signature/hmac；.env/.env.*/* .env类私文件拒绝。
  仅输出path/kind/count/长度，不输出匹配片段或值，真实值只用内存/匿名管道。
- 报告必须在包外；最终独立副本与原workspace/privateenv拒读证明尚未执行。

attempt3: manifest9、secret11、verifier11、original-proof8、roster11，合计50纯控制
全rc0；实际原件12satisfiers复核共278唯一原件，crosscheck actual rc0。首轮
original crosscheck因 .gate.rc 被算作同名前缀普通rc造成选择器拒绝；REFUSED输出
和rc1保留，修复仅排除gate后缀，未改任何原件。无Go/SQL/业务重跑。

## 8. Supplementary first3/h12/deployment checkpoint — attempt4

新增 `p26_live_proofs.py` 小型 stdlib reader，经 original_proofs 显式派发三种 proof：
`http-first3`、`h12`、`deployment-slice`；`p26_slice_crosscheck.py ROOT CONTRACT
[--negative-controls]` 读取 `p26_slice_proof_bindings.draft.json` 的独立补充合同。
此合同不是最终 roster，不改变六项 pending 或 unified114 数字。

- HTTP first3：只把原 `case.rc` 当 Unix rc；逐步 `.rc` 保留 normalized_harness
  语义（2xx→0，302/404 等保留状态），`meta.original_http_status` 与 raw 首行绑定，
  safe_response 与 raw JSON 投影相等。source actual before/after、固定安全身份/boots、
  下载 bytes/SHA/NDJSON rows、receipt transaction metadata/Run/version 关联重读；
  incremental初始404与1→2epoch/最后CP关联，dropack 201/200两条same-body/ACK/txn、
  9→12→12源请求与drops+1逐原件比较，legacy无keyed receipt404及cleanup quiet。
- 历史 first3八个精确工具blob独立重算，七项还与原checkpoint一致；wrapper为
  9eb95c2bb92d58b269c216c283072fd2490e2354c25a7435bce33de5e9ef8d5c。
  历史hash与本次当前archive工具SHA分别标注，绝不混用。
- h12 两新 normal/race + 已验收 unified原件复用：逐名1RUN/1PASS/0SKIPFAIL，
  original-chains所有SHA、raw/rc字节复制、KV original_meta_sha与S字段导出均比较。
  KV是适配材料，source来自原JSON，不改旧业务source，不增加unified114计数。
- Dataset/Connector切换：原safe inspect中另外四个identity完全相等，本服务新CID/image
  与原mount/ports绑定；new4四个安全identity精确固定。lsof collision原rc1与空输出
  明确期望1，不改0。W8 mid/post/new4-post每个131原rc0、59gate0；28份受保护SQL/
  旧9/CP8/parent/view/counter完整raw与oldpre实际baseline逐字节一致。
- 原始依赖closure共1995唯一文件，显式SHA/size固定，原件不被重写。九项内存变异
  负控移除hash快捷拦截后依然逐语义拒绝；无实际finalpackage复制/封存。

正式 verifier 明确禁止将 `http-first3` 用来关闭 `http-live-full-suite`，禁止将
`deployment-slice` 用来关闭最终ABC/全legacy。完整HTTP与最终typed whole-set仍
NOTREADY，等待Root后续合同。attempt4首次读取器把302误当2xx、以及误期待KV包含
DERIVED字样而拒绝，原REFUSED stdout/rc1保留；修正为真实normalized规则与KV
adapter/originalmeta字段校验，未改原件。

## 9. HTTP10 original stage — attempt5 (DRAFT, not Root final acceptance)

新增 `archive/p26_http10_proofs.py ROOT archive/p26_http10_contract.draft.json
[--negative-controls]`。`http10` aggregate 精确要求十个场景，保留 first3既有reader与
后七case-specific原输入合同。formal verifier仅允许 `http10` 作为全HTTP未来satisfier，
单一http-case/first3不能关闭全suite。此阶段未改最终roster六pending。

5034原件closure；5记录步骤process rc0，9内存negative全拒。新reader逐步normalized
harness rc/HTTPstatus/raw-safe投影、owned子rc/SQLstdout/自然DBclock、固定代数send/query/
generation、receipt/version/download、wholeRow/CP/head等关系复核；详见attempt5 NOTES。
AB-A双文件各用自身meta/receipt，crossscope CURRENT独立链接原RESULTSHA+S+typed tuple，
补证含2管理cleanupPOST诚实记录；502仅dependency，inflight仅proxy-beforeDVS。
9source+wrapper历史快照分别复算并固定，绝不拿当前archive工具SHA冒充。

未做actual final package、真实private values扫描、独立副本拒读原workspace验证或seal。
Root final roster/acceptance及其余pending仍为后续phase。

## 10. Final review candidate / explicit ledger / offline plan — attempt6

- `p26_final_roster.candidate.json` + `p26_evidence.candidate.json` are NEW review inputs;
  pinned draft remains unchanged. HTTP10 and q3 oldpre+Dataset+Connector+new4 show
  accepted-evidence-ready; this intentionally cannot pass formal verifier as final acceptance.
- `p26_closure_inventory.draft.json`: 402source+go.mod/sum,7156 original members,
  7575 planned members total at preparation. No actual copy. Final explicit failed-history
  roots/Root acceptance/unknown final schemas still missing => NOTREADY.
- `p26_package_assembler.py selftest` (11 controls) / `plan LEDGER --workspace ROOT
  --root-review APPROVED_HANDOFF_DIR`. Ledger format p26-copy-ledger-v1 with source_sha256,
  canonical_root_label, history_roots_explicit=true, history_required_paths,
  formal_required_paths, members[{origin:workspace|root-review,path,destination,kind,
  sha256,size}]. Kinds/source/helper/exactclosure/path/byte checks mandatory.
  Actual assembly CLI is intentionally not exposed until Root releases final actual phase.
- `p26_final_proof_interfaces.json`: legacy/finalrestart+receipt/typedABC/cleanup/
  actualsecret/independentoffline must receive original proof schemas, never fake ID closure.
- `p26_offline_isolation_plan.py CONFIG.json` prints profile+argv only; config fields
  package_copy,python_realpath,runtime_read_roots,forbidden_roots,profile_path,probe_config.
  Probe config stays inside copied package; no overlap of allowed/forbidden trees.
  Installed sandbox-exec -f verified, but grammar is private/deprecated and viability untested.
- Future Root phase command: `/usr/bin/sandbox-exec -f PROFILE PYTHON_REALPATH -I -B
  INDEPENDENT_PACKAGE/tools/p26_offline_probe.py --config PACKAGE/probe-config.json`.
  Caller must reset env, close extraFDs, captureoriginalrc/stdout/stderr and hashes outside
  package; deny sentinels pre-created as NONSECRET markers under forbidden roots; no private
  env reads. Required probes: original/private PermissionError, network PermissionError,
  external-exec PermissionError, package verifier best verdict. No Docker/PG/HTTP calls.

attempt6: 14pure controls rc0; accepted oldpre additional original read rc0; inventory read0;
strict incomplete ledger plan rc1 is expected NOTREADY. No actual assembly/sandbox/offline/seal.

## 11. Execution paths implemented — attempt7 (current phase still code/pure only)

This supersedes attempt6's temporary absence of execution CLIs; no actual package/sandbox
execution was performed in attempt7.

- `p26_package_assembler.py assemble LEDGER --workspace ROOT --root-review HANDOFF_ROOT
  --destination NEW_PACKAGE --root-phase PHASE.json` now performs actual exclusive copies.
  Root phase fields: phase=p26-final-assembly, approved=true, source_sha256=S,
  ledger_sha256=SHA256(canonical sorted compact JSON ledger), destination=exact absolute
  NEW_PACKAGE. Before writes: full ledger/S/exact402 membership/hash/helper checks,
  exact formal scope IDs, concrete nonempty satisfiers/contracts/original semantic proofs,
  pending bindings, and explicit accepted finalrestart/readability/replay proof. Unsupported
  final schemas refuse before any copy. It then creates destination once, verifies every
  copied byte, writes bounded copy-ledger+manifest, and runs copied formal verifier.
  Failure leaves the new directory untouched as failed history; no overwrite/resume/seal.
- `p26_offline_runner.py execute --config CONFIG --root-phase PHASE.json` has an actual
  sandbox-exec execution path. Phase fields: phase=p26-independent-offline, approved=true,
  config_sha256=SHA256(canonical JSON CONFIG), manifest_sha256, original_workspace,
  private_forbidden_roots. Config includes source_package/package_copy/capture_dir plus
  isolation plan fields. It verifies manifest before/after independent copy, checks safe
  sentinel existence before isolation, captures cmd/profileSHA/manifest/observed sentinels,
  starts cleanenv+close_fds sandbox with timeout300, saves actual stdout/stderr/rc externally,
  then requires deny probes and copied verifier result/manifest match. Timeout records
  originalrc unavailable, never invents rc. No actual sandbox invocation this phase.
- P1 reader `legacy-p1-slice` supports explicit P1pre9/P1post9 schema. Current pre9:
  original launcher rc(no suffix), original three source/childrc,9exactstdout names/report,
  business inputs,28tools before/after/exact immutable snapshot ledger/blob SHA, initial
  raw SQL9/8/8 +8old public CP/Run hashes and original commands, owned CHECK install/catalog/
  drop and cleanup originalrc/fields. Snapshot origins are labels only; no old source imported.
  P1post remains actual-pending; other legacy slices unsupported => NOTREADY.
- `p26_failed_history.supplement.json` explicitly preserves both foundation legacy-private-env
  failed roots (outer originalrc1 beforewrite); no satisfier. Full final failed-history roster
  is still a Root input, never automatically limited to green attempts.

attempt7 six recorded process commands allrc0: P1 actual+8negative, assembler13pure,
offlinerunner1pure(unreleased-refusal-before-I/O), isolation3pure, verifier11pure,
original8pure. No privatevalue read, actual package copy, sandbox/offline execute or seal.

## 12. Review blockers enforced — attempt8

`p26_formal_core.py` is shared by pre-copy and copied verifier; no independent weaker
preflight loop. Exact item IDs/satisfiers/kinds/fullscope,duplicate evidence,actual
classification,original artifact/rc/HTTPouterrc and nonempty original_files are mandatory.
Read closure is checked against actual ledger origin/path/destination/hash/size beforecopy.
Copied verifier also validates finalrestart original interface, not just presence of fields.
Finallegacy/ABC/cleanup/registeredsecret/restart and wholefailedhistory actual schemas are
explicitly unsupported: valid PG/P1 or Root flags cannot close them. Execution paths remain
implemented, but final readiness is intentionally blocked pending actual schemas.

Frozen source404 allowlist (402GoSQL+go.mod/sum) is SHA-anchored in shared core; package must
copy both core and p26_source402_allowlist.json under tools/. Prefix-based extra.pem/bin
is refused. Offline source/copy/capture/profile are resolved canonical paths, no symlink
aliases or bidirectional nested/equal overlaps. Sentinels use true-path root coverage.

attempt8:33 actual mutation/refusal controls plus36 existing controls,6 process records rc0.
Original PG/P1 valid baselines and sibling offline preflight baseline precede mutation tests.
No actual package/private secret/sandbox/offline/seal; attempt7 evidence is preserved.

## 13. S2 frozen source and original PG/unit/build — attempt10

Root froze S2 ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e.
New S2 allowlist404 +filehash/size hardpin; old allowlist/history retained. Source snapshot
6f3b492f…108c96 and absolute-LABEL stream independently replayed; two changed filehashes,
modules unchanged. ACTIVE_FINAL_SOURCE now S2; no oldS formal satisfaction permitted.

`p26_s2_proofs.py` supplies s2-pg/s2-unit/s2-build-original via shared semantic core:
9PG (h12two separate,114unchanged),units8/15/39 (fullConnector supplement),two original
17rc0 builds with realbinary verification. New S2 roster/evidence candidate441 original
members; build scope build-two-original; no runtime upgrade claim. Original internal38
partial and ACTUAL14 kept immutable,21history member pins; old017d tool snapshot retained,
new exactd39 code asset under archive/historical-tool-blobs/ binds fullConnector39.

9 new memory tamper controls +prior33+36 controls all0; nine recorded commands processrc0.
Three Rootapproved envexclusions onlylstat mode/size/no readhashvalues. No actual package,
privatevalues, sandbox or runtime calls. All later final scopes pending, final Root acceptance
not issued. See attempt10 NOTES/CHECKPOINT and current SOURCES-SHA256.
