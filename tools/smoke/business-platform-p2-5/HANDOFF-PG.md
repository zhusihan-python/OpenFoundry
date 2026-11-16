# OpenFoundry P2.5 — PG-TEST 轨道 HANDOFF（HANDOFF-PG.md）

2026-11-15（r17 **三阶段全正式绿**），ZCode GLM-5.3（PG-test 独立轨道）。
root 接受新候选并放行：new-normal 141338Z（15/15）+ new-race 141638Z
（go1.25.14，15/15）+ unified 141737Z（**87/87 old72+new15**）——每阶段
checker --meta 同 attempt --expect-source-sha **a1bff07f…ffd27** formal
rc=0，docker/wrapper/source/version gate rc 全 0，三 attempt 源哈希同一。
h12 接口：pg-candidate-bindings-r17.json（固定 attempt/工件/SHA 引用）。
逐名表与工件路径见 pg-test-plan §10 r17 及最终报告。生产/旧72/q3/
of-p24-pg 未动；失败 attempt 原件保留。

## 目标与终态

- 目标：P2.5 独立真实 PG 测试契约 + 分阶段实施；当前 **compile-only 已
  放行并已执行**（不起真实 PG/DB/Docker 栈/HTTP/q3）。
- 本轮已达成（r6 增量）：
  - **`services/connector-management-service/internal/handlers/checkpoint_integration_test.go`**
    （PG 独占，integration tag，root 2026-11-14 补充裁定）：**T10
    `TestPGCheckpointNoDBLockAcrossPublish`** 已写并双工具链编译绿——
    真实 `executeSnapshotSync`（公共入口 EnqueueSyncRun→ClaimSyncRun 构造
    捕获态）+ 注入 `Handlers.DatasetVersioning` parked port + 进程内
    httptest 增量源；两个子测试：(a) park 期间第二事务对
    Source→Def→Checkpoint→Run 逐行 `FOR UPDATE NOWAIT` 扫描全部成功
    （零行锁持有证明，root round-1 #4 手法，非 pg_locks 计数），释放以
    usable ACK（exact target+COMMITTED+非 nil txn）⇒ run succeeded +
    checkpoint epoch+1/value/owner 释放原子完成；(b) dispatch 后超时 ACK ⇒
    run failed + owner 保留（P26）+ 数据位置不动。
  - `run_pg_isolated_p25.sh`：compile/new 模式扩至 `./internal/repo/` +
    `./internal/handlers/` 双包；`p25-required-names.txt` 11 活跃（T10
    取消注释）+ 3 待主轨面；`pg-test-plan.md` **r6**（§5 状态、§5b 归属
    含 handler 文件、**§5c 致 Primary 依赖通知**）。
- 前轮（r5）已达成：
  - `pg-test-plan.md` r4→r5（释放令 5 纠正 + busy 次序 + 实施轮状态）。
  - `services/connector-management-service/internal/repo/checkpoint_integration_test.go`
    （本轨道独占）：**10 个顶层名已写并双工具链编译验证**——
    `TestPGCheckpointLegacyMutationFenceBaseline`（legacy 围栏基线，两段式
    证据第 1 段）+ T1 迁移（空/旧/重跑/octet/PK）+ T2 admission（FK 序/
    捕获固化/busy 零 run 零 stamp/binding_stale 不静默重置/55P03 竞争）+
    T3 同 Def 双池（Source 行因果屏障 + 双 PID 阻塞证明，恰一 owner）+
    T4 兄弟 Def 独立（durable owner 无锁态 + 真实异租户负例/正控制）+
    T5 推进全有或全无（marker RAISE 整体回滚/CAS miss 零写禁单边
    succeeded/成功路径 epoch+value+forensics+释放 + GetCheckpointView 安全
    投影 len/sha）+ T6 纯锁自然过期（不改行、先证阻塞时刻有效、锁后
    fresh clock 拒、全行字节不变）+ T7 围栏矩阵（错 token/错 gen/owner
    miss/epoch miss/fingerprint drift→BindingStale/普通 complete 拒写 P25
    succeeded——ErrIncrementalRequiresAdvance）+ T8 prepublish 取消/失败
    （pending 取消同事务释放；running 取消保留 owner；gate 前失败释放；
    **gate 已入未 dispatch ⇒ failed+owner 保留** + awaiting_p26_resolution
    可查）+ T12 advance 侧候选校验（空/513B/控制字符零写拒绝；空批次
    同 token epoch+1 的 R14 转移）。
  - `run_pg_isolated_p25.sh`：compile-normal/compile-race **已执行双绿**；
    up/new-normal/new-race/unified 备好未释放；P24 同款证据纪律（真实
    rc/raw/cmd/meta + 源码内容哈希漂移门 + 版本门 + of-p25-pg* 专属
    label 与 of-p24-pg/q3 结构隔离）。
  - `p25-required-names.txt`：10 活跃（编译验证过）+ 4 注释待主轨面；
  - `pg_exact_check_p25.py`：8 参数干净单实现（--new-only/unified）。
- 下一终态：主轨 T9（repo.go 围栏——已在落地中）/T10（handler 接线）/
  T11（schedule.go）面齐 → 补写 4 名 + 视 EnqueueSyncRun 集成情况把
  T2/T3/T4 的 p25Admit 内联构造换/加测真实公共入口 → 全 14 名编译绿 →
  root 候选 review → 释放 isolated real PG（up → new-normal/new-race →
  unified → exact checker 双判定）。

## 授权与禁止（当前阶段）

- 允许：compile-only（隔离 Go 容器 vet + `go test -c`）；写本轨道 6 文件；
  证据落 `/private/tmp/openfoundry-p25-repair-evidence/pg/`。
- 禁止：真实 PG 启动/运行、q3、of-p24-pg、HTTP、生产代码修改（含主轨
  文件）、旧 72 断言、Tools 文件、commit/reset/stash/清理、P24 归档。
- 签名同步纪律（本轮两次实战）：主轨中途改 `AcquireCheckpointForAdmission`
  增 `models.IncrementalBinding` 参数 → 本轨道 2 处调用点同步；主轨
  repo.go 一度缺 `"bytes"` import 破损整包编译（其围栏改动）→ 本轨道
  **未改生产**，等其自修后复验。后续主轨变更照此办理：同步测试、不改
  生产、如实记录。

## 工作区状态

- HEAD `d7c40cf5`（P0–P24 dirty 保留）。本轨道 repo 内新增仅
  `internal/repo/checkpoint_integration_test.go`（untracked）。
- `tools/smoke/business-platform-p2-5/`：本轨道 6 文件 = pg-test-plan.md
  （r5）、HANDOFF-PG.md（本文件）、run_pg_isolated_p25.sh、
  p25-required-names.txt、pg_exact_check_p25.py；主轨 design-draft.md/
  implementation-contract.md v1.1（446 行，冻结签名）/HANDOFF-PRIMARY.md、
  tools 轨道其余文件与 results/，均非本会话所写。
- 主轨落地状态（2026-11-14 夜核实）：checkpoint.go（1214 行，核心全量+
  takeover 防后门 lease.go:524）、Claim/Cancel/Gate/Complete 公共入口路由、
  migration 20261116100000；**进行中**：repo.go 围栏（UpdateConnection 已
  接 lockSourceDefsForProtection/assertCheckpointOwnersFreeTx）、
  EnqueueSyncRun 增量分支、schedule.go 调度路径、handler 接线。

## 已执行命令与验证结果（真实记录）

-2. r8 轮（review 子例）：读全 review-source-round1/review-repo-round1 →
   加 9 子例（映射见 pg-test-plan §10 r8）→ **compile-normal_
   20261114T124347Z rc=0 / compile-race_20261114T124355Z rc=0**
   （go1.26.3 / go1.25.14，双包 vet + test -c）。注意：这些子例断言
   **契约修后语义**——在 Primary 修复前运行会如实 FAIL（Def 锁等待捕获/
   当前绑定漂移拒推进/retain 分支 owner 守卫/{} 配置回归/循环 token
   不泄漏/requires_reconciliation 措辞），这正是候选评审要看的证明；
   不以 Skip/骨架交付。names 保持 14，证明由子例承载。
-1. r7 轮（全量断言补齐）：T9/T11/T13 写入后 compile 经历——
   主轨中途符号未定 ×3（`compile-normal_20261114T122802Z`=
   sourceIncrementalEnabled/ErrIncrementalRequiresControlled 未定义、
   `_122958Z`=incrementalAdmissionBinding 未定义、`_123500Z`=主轨又改
   完后暴露本轨道文件问题）；本轨道自错修正 ×4（T9 并发子测试 dsn
   错位、documented_recovery 解构错位、T11 unused run、fixture 元数）
   后：**`compile-normal_20261114T123633Z` rc=0（go1.26.3）、
   `compile-race_20261114T123644Z` rc=0（go1.25.14）**——含 13 repo 名
   + 1 handlers 名全部编译，raw 内 RUN/PASS/FAIL=0 行（未执行）。
0. r6 轮：`sh -n` runner rc=0（双包扩展后）；compile 双绿复验：
   `compile-normal_20261114T122224Z` rc=0、
   `compile-race_20261114T122230Z` rc=0——含 handler 文件。
1. r5 轮：compile-normal 首轮 rc=0（仅基线文件）；写入 stage-2 后 rc=1 ×2：
   - `compile-normal_20261114T121148Z`：**主轨 repo.go:285 缺 `"bytes"`
     import**（非本轨道文件）——等待 45s+120s 后主轨自修（import 已在
     repo.go:6）。
   - `compile-normal_20261114T121518Z` / `compile-race_20261114T121519Z`：
     **本轨道文件** `AcquireCheckpointForAdmission` 参数不符（主轨中途加
     binding 参数）→ 同步 2 处调用点。
2. 同步后：`compile-normal_20261114T121546Z` **rc=0**（go1.26.3，
   version_ok=true）；`compile-race_20261114T121552Z` **rc=0**（go1.25.14，
   version_ok=true）。raw 中 RUN/PASS/FAIL 行数 0（未执行任何测试）。
3. `sh -n` runner rc=0（曾修一处 `while…fi`→`done` 笔误）；checker ast
   解析 ok、无参用法 rc=2（预期拒绝）、参数数修正为 8。
4. 未运行：真实 PG/DB/HTTP/q3（未获释放）；未修改任何生产代码。
- 全部证据：`/private/tmp/openfoundry-p25-repair-evidence/pg/`（每次尝试
  .raw/.rc/.cmd/.meta，真实时钟命名，含源码内容哈希 before/after）。

## 证据事实 vs 假设

事实：
- 10 个顶层名在 go1.26.3（CGO off）与 go1.25.14（CGO on，race 编译）下
  **编译通过**（vet + `go test -c`，rc=0，版本门过）；**从未执行**，无
  任何 RUN/PASS 断言可引用。
- 测试断言基于契约 v1.1 与已落地源码逐行核对（checkpoint.go 全文 1214
  行、lease/cancel 路由点、迁移 DDL、models 增量形状、
  `sync_schedule_ticks.outcome` 无枚举 CHECK）。
- T1 旧库子测试特意把 claim 放在迁移**后**：路由化 claim 会 pre-read
  `r.cp_mode`，列在迁移前不存在（运行期事实，非猜测）。

假设/未验证：
- 10 名的**运行时**行为（fixture config 能否过
  normalizeRESTAPISourceConfig、屏障时序、触发器语义）未经真实 PG 验证，
  以首轮 new-normal/new-race 真实日志为准；预期可能有 fixture 级迭代
  （如实记录，不算伪造）。
- T9/T10/T11/T13 尚无测试（对应主轨面未齐），names 文件中保持注释。

## 本轨道独立发现（累计）

- F-1..F-5（前版，均闭环或入计划）。
- F-6：主轨围栏落地于 UpdateConnection 使用
  `bytes.Equal(current.Config, body.Config)` 判"实际变更"——原始 JSON
  字节相等而非解析后有效子集（契约 §3.9 写的是"解析后有效子集变化"）；
  当前实现键序差异会导致假"变更"（保守方向，多触发围栏），或注释不同
  空白等价输入漏判（中性）。已登记不实现，待主轨确认意图（T9 落地时
  断言口径随之定）。**已写入 §5c 致 Primary 通知。**
- F-7：~~公共 EnqueueSyncRun 尚未接增量~~ **已落地**（lease.go:237 调
  AcquireCheckpointForAdmission）——T10 已用公共入口构造捕获态；
  T2/T3/T4 的 p25Admit 内联构造与公共入口断言的合并为后续小步（不阻塞）。

## 下一步（谁、从哪开始）

0. **状态：14/14 名全量行为断言已写、编译双绿、真实 PG 未运行。**
   等待 root 候选评审 → 释放 isolated real PG（`up` → `new-normal`/
   `new-race`（`^TestPGCheckpoint` 双包）→ `unified` →
   `pg_exact_check_p25.py --new-only`/unified 双判定）。运行期若主轨
   再改签名，先同步测试再跑。
1. Primary 已全线落地（EnqueueSyncRun 增量/schedule busy/五处围栏/
   takeover 守卫/Store 接口）——T2/T3/T4 可在运行绿后按需加公共入口
   对照子测试（p25Admit 内联保留为锁序对照）。
2. F-6 已按"两种口径都成立"的探针断言（resource_path 变更），对
   Primary 最终口径不敏感。
3. **致 Primary 的通知已就位**：pg-test-plan §5c（注入缝依赖/签名同步
   纪律/F-6 口径）。Primary 读本目录文件即可对齐。
2. 全 14 名绿后通报 root 候选 review → 释放后 `up` → `new-normal`/
   `new-race`（`^TestPGCheckpoint`）→ `unified` →
   `pg_exact_check_p25.py --new-only` 与 unified 双判定（1 RUN/1 PASS/
   0 SKIP/0 FAIL + rc=0）。
3. F-6/F-7 报主轨确认；发现新缺陷只登记不实现。
4. 运行保全零触碰维持（q3/六队列/of-p24-pg/env600/归档859/工具27）。

## 附件清单

- 本轨道 6 文件；证据（路径引用）：
  `/private/tmp/openfoundry-p25-repair-evidence/pg/compile-*_20261114T*`
  （含两次失败与最终双绿的完整 .raw/.rc/.cmd/.meta）。
- 引用已核实存在：root 六份 p25 handoff 文档、主轨 implementation-
  contract.md v1.1、checkpoint.go/迁移/models、冻结 helper 源、P24
  runner/checker/names。

## 秘密与安全

本目录与测试文件无凭据/DSN/JWT/env600 内容；`/private/tmp/openfoundry-p25-pg.env`
仅在 up 模式生成（当前不存在），永不输出/归档其值。绑定哈希不含明文
auth；候选 token 公开面只出长度/sha256（测试断言同口径）。
