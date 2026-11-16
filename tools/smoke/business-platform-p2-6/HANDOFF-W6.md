# P2.6 W6 HANDOFF — PG构建与保全验收（2026-11-15 r16：部署 plan 五处修正）

状态（r16）：Root 14:17 审计发现的部署 plan **五处实际问题全部修正**（零
手打，全部 ID 从原 rc0 raw 程序化注入 + sha64 正则校验）：

1. **rollback 旧 ID 拼接尾巴**：`"sha256:dc4…59264"\n"f57779264",`（相
   邻字符串字面量拼接产生 >64hex）→ 整块以原 01-old-tag.raw 值注入
   （dataset `dc4eca36…577c9264`、connector `6b556702…0097592`），正则
   消除所有 `sha256:64hex\n[0-9a-f]+` 拼接模式。
2. **fixture/proxy 路径**：`/srv/p26_rest_source.py` → **`/srv/p26/
   fixture/p26_rest_source.py`**（P26 整 root 只读挂载于 `/srv/p26`，所
   以入口在 `/srv/p26/fixture/` 子目录下）。同 proxy。
3. **Docker argv 顺序**：重构 `_run_argv` 为 OPTIONS→IMAGE→CMD（原
   `extra + [image]` 把命令字放在了 image 之前）。
4. **aux env 猜测项**：删除无效 `P26_LOOPS`/`P26_AUTO_RECOVERY` → 改用
   **生产 config.go 实名** `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=0`
   （config.go:133，0=off）；`ALLOWED_EGRESS_HOSTS` 从仅 `p26fixture` 改
   为 **`fixture,p26fixture`**（兼容旧回归名+新名）。
5. **JWT metadata**：删除错误"重启后旧 JWT 失效"说法 → 改为"JWTs are
   stateless — valid within expiry regardless of restarts"。

plan JSON 已重发（五件）；自测 6 套 **205/205 rc 0**。

## r16 SHA
deploy_prep **3b286c6d0ef8c8f6…**、deploy 自测 94fb0253…。

（r15 及更早历史节保留在下，未改写。）

## r15 摘要（保留）

- **原始 raw 三处自洽**（01==02==05）且 **== 当前 q3 running/tag**（dataset
  `dc4eca36…577c9264`、connector `6b556702…97592`、pipeline 同）→ 原事实
  链有效，未重建、未 retag/pull/rm。
- executor `OLD_FULL_IDS`、`RESUME_BOUND`（committed/cid/binsha 64）全部
  ==原 raw ✓。
- **唯一病灶**：deploy_prep `candidates().old_full_ids_untouched` 是两个相
  邻字符串字面量拼接（前次前缀正则只改字面量内未删拼接）→ 71/84 字符垃圾。
  **oneoff 窄修**：整块以原 raw 程序化注入干净值；失败审计原件保留于
  `results/w6/build/pin-audit-programmatic.json`（含 oneoff_fix 记录）。
- 全文件 64hex 扫描（deploy_prep/override yaml/executor）：未知值 **0**；
  pipeline 当前==pin ✓。官方 index 模板已于上轮 patch 前双镜像实证。
- plan JSON 已重发；自测 6 套 **205/205 rc 0**；P25 新写 0。

## r15 SHA
deploy_prep **753f43ac74135636…**（oneoff 后）；executor df667072…（未动）。

（r14 及更早历史节保留在下，未改写。）

## r14 摘要（保留）
- **build-resume-dataset / -connector**（各 8/8 门）：官方
  `{{json (index .Config "Cmd")}}` 模板先在双镜像实证（`{{if}}` 形式被否
  定——仍缺键）；exact fullID（4217c402…/f23f6916…）+ safe-config 与旧运
  行时**逐字段相等**（nonroot:nonroot|/home/nonroot|["/service"]|null）+
  原始 stage0（compile/create/cp/commit）按 SHA 绑定 rc0 + **仅精确
  doc-bound CID** 清理 rc0（无 name/force/volumes；label+Running=false
  预检）+ source 前后==S + 旧 q3 运行 ID 不变。原 08 rc1/wrapper2 失败史
  保留未冒称。dataset resume attempt1 一次 AttributeError 崩溃（变更前）
  原件保留，attempt2 修复后绿。
- **unit-dataset / unit-pipeline / unit-connector**（新增 runner `run-unit`
  模式）：离线（--network none、无 DSN、无 integration tag）全模块单测，
  rc0 + version + source(S) 三门全 0。
- deploy candidates 已标 **RESUME-VERIFIED**（含证据路径）；switch 计划
  剩余前置仅 W8 current_core_preservation + Root phase。
- 自测 **6 套 205/205 rc 0**。

## r14 SHA
build-executor **df667072216be3c2…**、runner **017d3305386b40bc…**（+
run-unit）、deploy-prep f0e07141…、deploy 自测 5027b045…。

（r13 及更早历史节保留在下，未改写。）

## r13 事实与修正（诚实记录）
- q3 实网=**openfoundry-platform-p1-q3v9_p1net**（inspect 实证，非
  of-p26-pg-net）；W7 plan-live 的实际 serve argv 已采用（fixture
  /srv/p26_rest_source.py serve 0.0.0.0 8090；proxy …8091 上游
  http://dataset:50078）；python:3.12-alpine fullID=c5ba3dee…dd89。
- 候选镜像 PENDING verify（08 bug）→ switch 计划把"verify 修复重放行绿"
  列为**前置**；W8 current_core_preservation 绿是**第一前置**（非我的
  historical guard）。
- 修复过程中发现 pg-runner phase_dir 秒级同名问题 → 已改 `_phase_dir`
  独占目录（UTC+token+exist_ok=False，镜像 build 模式）；自测 fake EV 根
  改全生命周期作用域（此前断言曾回落真实目录 96 文件——已修，6 连跑稳）。
- env 三件 0600 计划只含 metadata（路径/mode/键名），值永不打印/归档；
  admin 与 platform JWT 分离已写入 env-plan。

## r13 SHA
runner **05014e5ade124dfd…**（独占 phase dir）、deploy-prep aefe4717…、
override yaml 3f84509a…、pg 自测 28b22373…、deploy 自测 8abbf92f…。
build executor 仍 3a546d36…（未动）。

## 待 Root（下一 phase 所需）
- build 08-verify 一行 null-safe 模板修 + 重放行（两候选 4217c402…/
  f23f6916… 完成验证）。
- W8 current_core_preservation 放行与绿证据。
- 之后按 switch-plan/http-plan 的 phase（switch-dataset/switch-connector/
  http-fixture-serve/http-proxy-serve/http-aux-start）逐个放行。

（r12 及更早历史节保留在下，未改写。）

## r12 SHA（供 Root release）
runner **348a1e5a62b78d54…**、common b504a9f8…、build-executor 3a546d36…、
dataset-names 7dd7adde…、**connector-names 07bbdcd3…**（18 名）、
unified-cohorts **f936e63f…**（+三个 unified 名单件 78b00952/9c51c8d1/
6b1f2b84）、roster e5fa0b3d…。

## 放行批次（checkpoint JSON 内含全命令；均带 --expect-source-sha S）
1. new：dataset 9 名 normal→race；connector 18 名 normal→race
   （phase：test-<svc>-<mode>）。
2. unified 114 normal×3（test-unified-<svc>-normal；fresh-db
   ofp26_unified_<svc>_<ts>_<hex>）。
3. build×2（dataset/connector，r8 配方+r10 三窄门；phase build-<svc>）。
规则：可按私有 PG 服务分批并行；首错停该链、原件保留；unified 禁旧绿
拼贴（结构保证）；不碰 q3/旧 PG/HTTP。

（r11 及更早历史节保留在下，未改写。）

## r9 变更
1. **Dataset 9 名**：`p26-dataset-pg-names.txt` += 
   `TestPGP26DatasetWireCommitAckLossReplaysReceipt`（落地源 
   output_commit_ackloss_integration_test.go:217 三向核实；W1b attempt3 
   Root 静态接受）。
2. **roster `emit-names`**：按服务产出 `pg/unified/p26-unified-<svc>-
   names.txt` + `p26-unified-cohorts.json`（names 文件+packages 从实际
   extraction 的 package_now 派生：dataset=repo、pipeline=internal/
   postgres、connector=repo+**handlers**；DSN env DATASET/PIPELINE/
   CONNECTOR_DB_DSN；统一镜像 golang:1.26.3 fullID 2d6c8022…+openfoundry-*
   卷——P25 unified 先例）；分组数字为报告事实非硬编码（6/17/64 实证相符）。
3. **runner `--suite unified`**：phase `test-unified-<svc>-<mode>`；每 sweep
   fresh db `ofp26_unified_<ts>`（CREATE+SHOW major16 各自原件/门，P25 先例）；
   unified 全部 113 名**必须当前 S 真跑**（names gate 拒绝拼旧绿——自测里
   --names 覆盖钩子曾因 cohorts 重载失效而暴露真名单，即反拼贴证明）；
   `--expect-source-sha S` 三方门（before==after==S；不符 pre 阶段零运行拒绝）。
4. **switch_guard 重标**：guard 输出 `scope=historical_baseline_only`+policy
   声明不读 current；deploy-plan 前置改为 historical guard **且** W8 current
   preflight 双绿。真实 json 已再生成。
5. W8 carve 遵守：preflight/p26_current_preflight.py、W8-PREFLIGHT-
   INTERFACE.md、results/w8/ 零写入；旧 swap/deploy 骨架仍禁执行。

## r9 验证
- `w6/run_selftest.py` rc 0 **186 项**（68/**39**/43/14/22）；
  selftest-20261115T052122Z。
- 真实 emit/emit-names：dataset=9/connector=17/frozen87=87；unified 113。
- 工具 SHA：runner **9c76f0b0…**、roster 9311bb5b…、dataset-names 
  7dd7adde…、switch_guard 47c3bfc3…、cohorts json daf64299…。

## r9 统一执行命令（供 Root 冻结 S 后放行；normal 即可，需 new 27 名
normal+race 全 0 先行）
```
python3 -I -B tools/smoke/business-platform-p2-6/pg/p26_pg_runner.py run-tests \
  --execute --suite unified --service dataset   --test-mode normal \
  --expect-source-sha <S> --release-file /private/tmp/openfoundry-p26-release.json
# 同式 --service pipeline / --service connector
# release 需列: test-unified-dataset-normal / test-unified-pipeline-normal /
#              test-unified-connector-normal
```

（r8 及更早历史节保留在下，未改写。）

## r8 诚实记录
- 我手抄 dataset 旧镜像 pin 又错一位（第三次同类教训）；全部 pin 已改为
  程序化捕获。`.Config.Image` 实为 compose tag 串（非 sha）——facts 已按
  实形记录并只对 `.Image`/old-tag `.Id` 施加 pin 等值。
- 一次自测在 EV_DIR 修复前又把少量合成 attempt 写入真实 build 目录；
  SYNTHETIC-EVIDENCE-NOTE 已追记，不删原件。EV_DIR 现为调用期解析。

## r8 验证
- `build/p26_build_executor.py facts`（真实只读）rc **0**（旧栈零变化台账
  + go 缓存 + 端口事实 50078/50088）。
- `w6/run_selftest.py` rc 0 **180 项**（68/33/43/14/**22**）；
  selftest-20261115T050522Z。
- executor SHA **02e34207d6d70525…**（全值 shasum 可复现）；
  acceptance 自测 82e68749…。

（r7 及更早历史节保留在下，未改写。）

## build-round1 逐项落点

| review 项 | 落点 |
| --- | --- |
| source gate 实际比较+expect pin 必填 | `run` 要求 `--expect-source-sha <64hex>`（缺即拒）；before==after==**expect** 三方一致才绿 |
| 逐 cmd/raw/rc/fullID/版本/源码 meta、attempt 不覆盖 | 沿用 PG runner 证据模式（Attempt per 组件）；构后 `02-image-id` full ID |
| compose build 无 `--pull`（布尔语义） | plan argv 无 --pull；offline 保证=FROM 基镜像逐一本地 inspect 缺即拒 + 旧 reviewed 离线配方 |
| q3 .env 显式私有输入 | `--env-file /private/tmp/openfoundry-platform-p1-q3v9/.env`（不 shell source、不打印值） |
| 只 build Dataset+Connector；Pipeline 无 delta | `BUILDABLE=("dataset","connector")`；pipeline run 拒（"unchanged baseline"；facts 记其白名单身份） |
| Connector 一镜像复用 aux×2 | deploy-plan facts 注明 connectorA/B 共享同一候选 fullID |
| 旧镜像保回滚、tag 只加链 | 候选按 fullID 引用；rollback= `docker tag <old-full-id> <tag>`（仅列出不执行）；facts 台账 append-only |
| 旧 swap 骨架不可执行 | `plan-swap --execute` → rc2 硬拒（等独立 review+release） |
| Dataset PORT 50078 实际值 | facts `q3_service_port_facts` 记录（显式 50078 非 50117） |
| W7 布局要求 | deploy-plan facts：aux 保持 /srv/p26/ 只读挂载布局、root release JSON 只读挂载+P26_RELEASE_FILE、admin secret 仅内存/私有 env；未来主切换形态（image-only override 保留 dataset-files:/home/nonroot、`--no-deps --no-build --pull=never`+fullID）已记 |

## r7 验证记录
- `w6/run_selftest.py` rc 0 **170 项**（68/33/43/14/**12**）；selftest-20261115T044031Z。
- `build/p26_build_executor.py facts`（真实只读）rc 0：ledger==pins、端口事实、build 范围。
- `preflight/p26_switch_guard.py deploy-plan` rc 0（facts 增补）。
- `real-connector-normal-attempt1/RESULT.json` 现在 `json.loads` 通过（更正注明）。
- 零 actual build/swap/up/restart；P25 新写 0。

## r7 工具 SHA（供 Root）
build-executor `4e1e8e0b…`、build-plan(swap 硬拒) `d7952ce2…`、
switch-guard `b24468a9…`、acceptance 自测 `fcbbb77b…`。

（r6 及更早历史浓缩节保留在下，未改写。）

## r6 三件套（全部最小可执行 + 纯自测，SHA 见下）

1. **`pg/p26_unified_roster.py`**（emit）：逐名统一 roster——P25 冻结 87 名
   用 pg_exact_check_p25 同款**文件语义**只读提取（p1 三变量+p21 lease+
   p22/23/24/25 列表正则，零 import/执行旧工具）；当前 Dataset 8 +
   Connector 17（要求仍在落地源）；wire-2 预留槽。每名：service/package/
   cohort 镜像与版本/run_group/**sealed_green 1/1/0/0**（解析 bundle 哈希
   验证过的 unified raw）。真实输出 `results/w6/unified-roster.json`：
   87/87 sealed-green、87/87 仍在源码；count 只是副产品，逐名列出为准。
   自测负控：篡改 sealed raw 拒、单名失 PASS 逐名暴露（非 count）。
2. **`build/p26_build_executor.py`**（facts/plan/run）：facts 已真实只读
   执行 rc0（回滚台账：connector 6b556702…/dataset dc4eca36…/pipeline
   d50858d8… 程序化 pin、ledger==pins True；Dockerfile FROM 清单）；run 为
   可执行路径（--execute+release `build-<svc>`）：FROM 基镜像逐一本地
   inspect、缺失即拒（杜绝隐式 pull）→ compose build（精确 q3 project、
   无 --pull）→ full raw/原 rc/meta（全源码 before/after）→ 构后 full
   image id；旧镜像不删不 retag，台账 append-only 供回滚。自测：缺基镜像
   → 零 compose 调用。
3. **`preflight/p26_switch_guard.py`**（guard/deploy-plan）：guard=候选切
   换硬前置——全量 seal 5016 逐文件 + derive（9×27 字段/父链 + owner-8
   全量 CP 原值），任一不绿即拒；**真实执行 rc0**
   （`results/w6/p26-switch-guard-real.json`）。deploy-plan=新资源 typed
   ledger（p26fixture:17095/p26proxy:17096/p26connectora:17097/
   p26connectorb:17098，来自 W7 接口）+ 放行后精确 docker run argv +
   前置（lsof 17095-98 空、旧 17090/17078/81/88 不动、guard 绿）+ 禁止项
   （不复用/重启旧 fixture、不绑 17090-94）；bind 本身等 Root。
   自测：synthetic seal 绿/篡改拒、旧端口冲突拒。

## r6 验证记录

| 命令 | rc | 结果 |
| --- | --- | --- |
| `w6/run_selftest.py` | 0 | **168 项**（68/33/43/14/10）；selftest-20261115T043558Z |
| `pg/p26_unified_roster.py emit`（真实只读） | 0 | frozen87 sealed-green 87/87、in-source 87/87 |
| `build/p26_build_executor.py facts`（真实只读） | 0 | 回滚台账==pins |
| `preflight/p26_switch_guard.py guard`（真实只读） | 0 | seal 全量+9/8 全绿 |
| `preflight/p26_switch_guard.py deploy-plan` | 0 | typed ledger 17095-98 |
| P25 新写 / p26 残留 | — | 0 / 0 |

## r6 工具 SHA（供 Root review）
roster `52fdffba…`、build-executor `b0f816e1…`、switch-guard `aee5cd1f…`、
acceptance 自测 `3fadc2e4…`（全值 `shasum -a 256` 可复现）。

## 历史（浓缩，原件见 results/w6 与此前 HANDOFF 版本转录）
prepare attempt1 rc2（fail-closed 白名单形式缺口，一行 patch+重放行）→
attempt2 rc0（of-p26-pg 16.15、零 host port、env600）；Dataset normal
attempt1 15/17→W1 修 603 行→attempt2 双模式 8/8 全绿；Connector 17 名
15/17（ClaimRecoveryContention 2 子测锁证明 + Budgets 逆序子测，
recovery_integration_test.go:487/566/828，归 W2）。

## 接下来 / 担忧
- W2 两 fixture 窄修后：connector normal 重跑（SHA/命令不变）→ 全绿再 race。
- 统一最终回归（old72+15 lineage + 当前 25 名 + wire-2）在 Root 稳定窗口。
- 担忧（低）：frozen87 的 p25 名单中含 P25-only 名（如 handlers 系），
  package_now 已逐名标注；统一回归时按 P25 pinned 命令跑，不混 cohort。

## round2-final 4 窄 patch（本轮，无框架重写）

1. **TimeoutExpired 双流归一**：`_to_text`/`_timeout_output` 每路独立
   bytes/str/None→text 再拼接（且读 `exc.output`——Python 无 `.stdout` 属
   性，原实现会丢 stdout）；rc124+全 partial+标记。纯测：bytes+None、
   str+bytes、bytes+bytes、None+None 四组合 + 本地 sleep 实测。
2. **readiness 截止语义**：每次探针**前**查 remaining；探针 timeout=
   min(15, remaining)；sleep 计入界内（min(3, remaining)）；截止后不再发
   探针/睡眠，raw 如实记录每探针 `probe_timeout_s`。自测：慢钟模型下
   attempts=3、第三探针 `probe_timeout_s=10`（被 remaining 封顶）、
   "BEFORE next probe" 留痕。
3. **post-validate 每探针独立证据**：06a-label…06f-docker-port 六个探针各
   自真实 .cmd/.raw/.rc（原始 rc），聚合 predicate 移入
   06-post-validate-summary + 独立 .gate.rc（合成文件明标非 docker 证据）。
4. **not-found 严格分类**：仅"预期非零 rc + Docker 明确消息含**精确资源
   名**"（`no such object: of-p26-pg` / `no such network: of-p26-pg-net`）
   判不存在；空输出、daemon/socket 错、错名 no-such、任意 not-found 一律
   fail-closed（secret/network 创建**之前**）。自测：空输出拒、错名拒、
   daemon 错拒、真名通过。

## 边界遵守（新增）

- **自测不再 import/执行封存 P25 模块**：parity 改为独立 fixture（测试内
  显式重构 recipe 流字节并比对）；历史尝试（曾只读 import）如实保留在旧
  转录，不改旧树。
- 诚实披露：修复期间一个自测 bug 曾在真实 `/private/tmp` 创建过我的
  `openfoundry-p26-pg.env`（direct-call 未套 env 覆盖）——已删除该自有文件
  并修正自测（env 包裹 + Abort 捕获）；`/private/tmp` 现为 0 残留、P25 树
  0 修改。

## 验证记录（终）

| 命令 | rc | 结果 |
| --- | --- | --- |
| `python3 -I -B w6/run_selftest.py` | 0 | **156 项 PASS**（gates 68 / pg-runner 31 / cohort 43 / build 14）；转录 `results/w6/selftest-20261115T030857Z/` |
| prepare-release-request.json | — | SHA 刷新（pg runner=064aeda311c63e08…；命令/token schema 不变，仅 prepare 范围） |
| `/private/tmp` p26 残留 / P25 新写 | — | 0 / 0 |

## 接下来 / 担忧

- 接下来：Root 内部放行 `prepare` → 真实空 PG；随后 Dataset PG 独立先跑。
- 担忧（中）：W2 autoseam 落地后 connector P26 名单将超 14——**实际 Connector
  PG 前必须从最新 W2 接口/源重冻结**（Root 已明示；prepare 不需要）。
- 担忧（低）：dataset race 首跑无 P25 先例（W1 handlers race 同镜像是弱先例）。

## round-1 保全附录（本轮新落实，均有自测）

1. **8CP whole-set 无豁免**：roster 恰 8 唯一 run + 8 唯一 def；
   `set(by_run)==roster_runs`（extra owner 无论身份一律拒，移除 protected9
   豁免）；cp-owners def 集也须恰等。负控：protected9 身份的 extra owner 拒、
   roster 重复 def 拒。
2. **8CP 完整原值**：join 封存 before.json（实测 8/8 def 全在）——每条 cp8
   保全 roster 全项 + **完整 owner 行** + **完整公共 checkpoint**
   （initialized/mode/confirmed_epoch/owner 对象）+ **matched public Run**；
   def/source/output 父链比对；缺 def/缺 matched run 即拒。注：checkpoint
   owner 与 run owner 在封存数据中是不同概念、合法不等——我一度自加等值断言
   被真实数据拒，已按附录本意改为如实保全两值（已在报告披露）。
3. **verify_seal 真文件集**：磁盘实际文件集必须 == manifest ∪
   hash-manifest.json（多余文件即拒，自测覆盖）；member 路径禁绝对/`..`/
   符号链接逃逸；`--limit<=0` 拒；**抽样永远 partial=True 且 ok=False/rc1**
   （不再以 sample 称完整绿）。真实全量 5016/5016 全对（1.9s，只读）→
   `results/w6/real-verify-seal-FULL.json`（替代早期 limit50 抽样件）。
4. **export_baseline 收口**：bundle member 同路径安全检查；每个目的文件
   resolve 包含于 out_dir；**拒绝覆盖已存在目标**（旧 baseline-a1 为旧规则
   产物保留为历史，再导出须用新目录）；a1 仅历史 pin。

## round-2 六点（上一轮已修，保持）

1. **EXPOSE≠host publish**：`parse_ports_mapping` 把 `NetworkSettings.Ports`
   当映射解析（`{"5432/tcp": null}`=正常未发布；任何 HostIp/HostPort 非空即
   拒）；独立仍要求 `docker port` 空 + `HostConfig.PortBindings` 空。纯夹具：
   空/暴露未发布/空绑定对象=过，真绑定/hostIp=拒（gates 自测 8 项）。
2. **CLI 字段**：phase/race 用 `args.test_mode`；自测经**真实 argparse 入口**
   双模式断言 `-race`/`CGO_ENABLED=1`/exact phase 名。
3. **全部 subprocess 有界**：`real_runner(argv, timeout_s)`（超时→rc124+标记
   留痕，自测用本地 sleep 证实）；readiness=每探针 15s 超时 + **monotonic 90s
   墙钟上限**（非 20×无界）+ **每次尝试 rc/raw 全保留**（自测：2 次即触顶、
   attempt=1/2 都在 raw）；go test 带 `-timeout 20m` + 外层 1500s。
4. **预检白名单+真证据**：存在性探针改 `--format {{.Id}}`（绝不读
   Config.Env）；每探针独立 .cmd/.raw/.rc **原始 rc**（not-found 如实记 1）；
   合成 summary 明确标注非 docker 证据，聚合判定在独立 `.gate.rc`；单探针
   失败立即停（rc2）。ready 后经自有容器 `psql -t -A -c "SHOW
   server_version_num"` 验 major=16（17→拒，自测覆盖）。
5. **测试 env 文件**：唯一名独占创建（O_CREAT|O_EXCL 0600，拒已存在/符号链
   接，绝不 O_TRUNC 覆盖未知文件），finally 仅清理自建文件；自测：两次运行
   路径不同、结束后不存在。主 secret 校验模式+`^[0-9a-f]{48}$` 格式。
6. **严格 pin**：postgres/golang 实际 full ID 必须等于已审 pin，漂移→拒并报
   Root（自测：漂移→rc2 零创建/零 run）；toolchain 另要求 RepoTags 含精确
   `golang:1.25.14`/`golang:1.26.3`；无 pull 后备。三镜像 pin 由白名单
   inspect **程序化捕获**（杜绝手抄；上轮两处手抄错误均已被门当场抓出）。

## 已完成 / 验证记录

| 命令 | rc | 结果 |
| --- | --- | --- |
| `python3 -I -B w6/run_selftest.py`（终跑） | 0 | **147 项 PASS**（gates 63 / pg-runner 27 / cohort 43 / build 14）；转录 `results/w6/selftest-20261115T025935Z/` |
| `python3 -I -B pg/p26_pg_runner.py facts`（真实白名单） | 0 | `results/w6/pg/facts_*/`（postgres:16=e17e…3dd3d5、golang:1.25=6993…dc80、golang:1.26.3=2d6c…1b1a 全缓存） |
| `cohort/p26_cohort_cli.py verify-seal`（**全量**，真实树） | 0 | 5016/5016 全对、partial=False、ok=True → `results/w6/real-verify-seal-FULL.json` |
| `cohort/p26_cohort_cli.py derive-protected`（新规则，真实树） | 0 | protected9=6+1+2；cp8=8×全对象（owner 行全量+checkpoint 全量+matched run）→ `results/w6/real-derive-protected-v2.json` |
| 真实 repo source-recipe parity（r2 已证） | — | 相等 `f9b162d2…`（`results/w6/source-recipe-parity-real.json`） |
| prepare-release-request.json SHA 复核 | — | 8 文件零漂移（收口后重验） |
| P25 零写入 / `/private/tmp` p26 残留 | — | 0 / 0 |

## Root 内部放行所需交付件（已备）

`results/w6/prepare-release-request.json`：实际命令
`python3 -I -B tools/smoke/business-platform-p2-6/pg/p26_pg_runner.py
prepare --execute --release-file /private/tmp/openfoundry-p26-release.json`
+ 8 个工具文件 SHA256 + release token schema（`released_for:["prepare"]`）
+ 范围声明（仅 prepare；不 run-tests/down/HTTP/旧PG SQL）。

## 接下来 / 担忧

- 接下来：Root 窄放行 `prepare` → 真实 up → **Dataset PG 可独立先跑**（W1
  静态复核已过、八名冻结；Root 管源码前后无漂移）→ connector 侧等 W2 seam
  落定。
- 担忧（中）：W2 正在加 auto-recovery seam（ListRecoveryCandidates/
  ClaimAutomaticRecovery），**connector P26 名单将超过 10**——test 放行前须
  从最新 W2 落地源重冻结 connector names（Root 已提醒）。
- 担忧（中）：dataset race 首跑（golang:1.26.3+-race 无 P25 先例，W1 自己的
  handlers race 是同镜像先例）。
- 担忧（低）：go vet 在同一 argv 内——dataset 侧并行工位代码若临时破损会如实
  留痕失败（证据纪律如此，不掩盖）。

## 授权边界遵守声明

本回合真实执行仅白名单 docker 只读与本地哈希/自测；无 PG/容器/网络/卷/HTTP/
build/deploy；未读任何 secret 值；P25 与 W1-W3、W7 路径零写入。
