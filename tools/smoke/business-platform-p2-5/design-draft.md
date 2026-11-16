# P2.5 发布后推进断点（跨 run 增量续读）— 契约设计 v2.1

- 日期：2026-11-14。作者：ZCode GLM-5.3（Primary 轨道）。状态：**已放行实施**（root `implementation-release.md`；实现以 `implementation-contract.md` v1.1 为准）。
- **v2.1 变更（implementation-release 五组强制修正，优先于下文 v2 措辞）**：
  1. 保守 unknown 边界＝**publish gate ENTRY**：一切释放 DB 断言 `publish_entered_at IS NULL`；gate 后即使未真正 dispatch 也只写诚实 failed（租约有效时）＋retain owner；**删除 v2 §4.5 的内存 sticky 释放证明与 R5 释放分支**。
  2. 权威 mode＝`run.cp_mode`（Source 锁后重读权威 config 选/升 Def 锁；执行/取消/终态永不看现行 Source config；公开 Repo 入口在 Run-first 锁前路由 P25 work；全部内部 cancel 调用点共享路由）。
  3. 绑定完整性：canonical hash 加入 file_glob＋完整有效 Auth 身份（type、inline token/basic 值、scheme/header/query 放置、secret-ref 名＋所引凭据指纹）；bare `UpdateConnectionConfig` 入围栏；多 Def 写者按 Def ID 排序、先锁 Def 再改受保护 input/credential 行。
  4. 类型化细则：InitialValue `*string` presence（显式空拒绝）、Source-auth query 冲突 Source 级校验、DB 界＝`octet_length` 原始字节、dot path 段校验、Rows/Bytes 独立 json tag。
  5. R9 本地 COMMIT 丢响应＝全旧或全新（**含 owner 可能已被已提交成功清除**），返回 uncertain，不猜测修复、不自动重放。
- 交付物：本文（设计理由/决策/矩阵）＋ **`implementation-contract.md`（精确 schema/签名/转移矩阵/锁序/API/归一化/兼容/文件归属）**。两者冲突时以 implementation-contract 为准。
- 已纳入的 root 输入：
  - R-A 设计期即时结论（独立 SNAPSHOT 批次；占用先于发布；锁序；usable ACK；新 fixture）。
  - R-B `handoffs/2026-11-14-p25/review-initial-contract.md`（源协议钉死、/files 非证明、credential 绑定、unresolved owner 围栏、pending cancel 锁序、六任务保全）。
  - R-C `handoffs/2026-11-14-p25/review-round1.md`（九条阻断修正：**完成检查点协议**、owner FK 顺序、禁止 succeeded 单边提交、禁止编辑副作用静默重置、全路径锁序覆盖、ACK status 契约、Q1–Q4 裁决、旧工具不可伪称未改）。R-C 条目在本文以 §n[m] 标注来源。
- v1→v2 变更日志（R-C 驱动）：
  1. 续读协议整体替换：废弃"最后承载页请求 cursor"方案（其描述的是页起始位置而非消费后位置，排他起始也会重读同页；单页首批会存 NULL 从头再来）。改为**独立完成检查点协议**（§2）。
  2. owner 设置移到 Run INSERT 之后（v1 的 upsert 先置 owner 指向未 INSERT 的 run 会立即 FK 失败）；Def 强锁替代 KEY SHARE（opt-in 路径）。
  3. 删除"cpCAS 失败仍提交 succeeded"分支：成功+推进+释放全有或全无。
  4. 删除"config/target 变更同事务删 checkpoint"：改为**围栏拒绝＋显式恢复路径（newDef-only，P25 无 reset API）**；SetCredential 入锁与围栏。
  5. pending cancel 与 pre-publish 释放改全链 Source→Def→Checkpoint→Run；Takeover 增加 checkpoint-owner 反连接守卫，不得成为 P25 未决 work 的重发布后门。
  6. ACK：`DatasetCommitResult` 加 status；P25 handler 侧强制 COMMITTED+exact target（含注入 port）；非增量路径兼容不动。
  7. Q4 裁决落地：任何 API 不回显 checkpoint 原值，只给长度/sha256/epoch/owner/last-confirmed。
- 依赖不变更：Go 1.26.3 / race Go 1.25.14 / PostgreSQL 16.15 / pgx v5；不改 `go.mod`/`go.sum`。

## 0. 完成标准

- 本阶段（设计+契约）：本文＋implementation-contract 覆盖全部下发问题与 R-A/R-B/R-C 全部裁决，除本目录 Primary 三文件外零 git 改动。
- 编码阶段：§8 矩阵逐项机械判定（新必需 PG 名与旧 72 分列、normal/race 分开日志+真实 rc、HTTP 真值断言、旧套件适配显式计数归档）。

## 1. 现状调用链核查（v1 已核实，此处保留结论与关键行号）

- 主链：`handlers.go:1362` RunSyncJob → `lease.go:168` EnqueueSyncRun（Source→Def(KEY SHARE)→INSERT run→stamp）→ `coordinator.go:627` executeTask → `lease.go:236` ClaimSyncRun → `handlers.go:1697` executeSnapshotSync（心跳/取消轮询 1762-1792、每页 gate 1804、拉取 1881、发布门 1928、`CommitOutput` 1963、终态写 2019/2095）。
- 拉取：`rest_snapshot.go:157`；cursor 循环 316-342（`seenCursors` 防环、空 `next` 终止）；`extractStringPath:390`。
- 发布：`sync.go:113` CommitOutput，SNAPSHOT 缺省(127)；ACK 现仅查 txnUUID 非空(170-183)，status 解码未用、dataset 未比对。
- 租约：`lease.go:116` withRunLock（Run-first）／236 Claim／280 Renew／330 Complete(CAS+stage guard)／397 TakeoverExpired；`cancel.go:93` RequestCancel（Run-first，pending 直写 aborted）；`cancel.go:259` BeginPublishGate。
- 调度：`schedule_loop.go:126` RunOnce（`scanAfter` 进程内）；`schedule.go:352` ClaimDueScheduleSlot（Source NOWAIT→Def NOWAIT→clock→tick 去重→INSERT run→cron 游标→stamp）。
- 变更/凭据：`repo.go:593` UpdateSyncJob（target 守卫 618 仅数 pending/running）；`:239/:1085` Update/DeleteConnection（265/328/1301 同类）；`:377` SetCredential＝**无锁单语句 UPSERT**，`source_credentials.fingerprint`＝明文 sha256（handlers.go:439）。
- 迁移：`repo.go:40-55` 整批 advisory-lock 重放，幂等无 ledger。
- 三种游标切割：分页 next（run 内）／P24 cron 游标（时点）／P2.5 完成检查点（数据流位置）——三者互不混用。旧 fixture cursor 是单快照页号、`updated_at` 静态 → 不可证跨 run 增量。

## 2. 完成检查点协议（R-C §1，整体替换 v1 §2.3）

### 2.1 配置（`sync.incremental`）

```json
{
  "enabled": true,
  "request_param": "since",
  "completed_checkpoint_path": "meta.completed_checkpoint",
  "initial_value": "可选，非空，≤512B"
}
```

- `enabled` 要求 `pagination.type=="cursor"` 且 `next_cursor_path` 非空（保持 cursor-only 范围，但**数据检查点与页内续页完全分离**）。
- **键冲突校验**（双侧）：`request_param` 必须不同于 `page_param`/`page_size_param`/`cursor_param`/`query_params` 全部键/凭据 query 键（api_key 的 `query_param`）；冲突 → 400，绝不静默覆写配置参数。
- `completed_checkpoint_path`：非空 dot path。
- `initial_value`：可选项；出现则非空、≤512B、UTF-8、无控制字符。**opaque 值一律不做 trim 归一化**（配置键名/路径可 trim，token 值不可）。

### 2.2 拉取协议

- **每页请求都携带固定的 `request_param=<当前检查点 token>`**；run 内分页 cursor 仍从空起、仅用于翻页（`rest_snapshot.go:316-342` 循环不动其语义）。首轮（无存量 token）：`initial_value` 非空则携带之，否则**不携带该参数**。
- 分页终止（空 `next`）的那个**最终响应**上提取 `completed_checkpoint_path` → 候选完成检查点。**绝不保存任何页的请求 cursor 或 next 分页 cursor 当作数据检查点**。
- 候选校验（在发布前，任一不过 → run 失败）：
  - 必须存在且为 string；非空；原始字节 ≤512；UTF-8；无控制字符；错误/日志/metadata **永不回显原值**（只记长度）；
  - **非空批次且候选==输入 token → 安全失败**（源给了记录却没给新位置＝无法主张正进步，绝不发布）；
  - 空批次且候选==输入 → 合法（"已消费至当前点"）：确认 epoch+1、token 不变；
  - 空批次且候选≠输入 → 合法：token 与 epoch 同步前进。
- **上游前提（显式写入契约文档）**：源须保证排他式增量流连续性（`request_param` 语义为"只要该位置之后的记录"）且窗口跨页稳定；不满足的源不在 P25 支持范围；不做通用 CDC 主张。
- 逐页输入固定不变式：除分页 cursor 外，一切请求参数（含 `request_param` 值）逐页相同——由构造保证并测试锁定。
- 任何失败（page2/解析/limits/egress/候选校验）不消费数据位置。

### 2.3 语义效果（真实 A→B→C）

首轮读 A 全量并取得 completion token T1；追加 B 后第二轮携带 `since=T1` **只读 B**；再追加 C 第三轮只读 C。排他续读由协议保证，不靠 at-least-once 兜底（v1 的免责声明不足以支撑验收目标——R-C 明示）。若某源在空批次时也会推进 token，同样正确处理（§2.2 末条）。

## 3. Dataset 输出语义（R-A/R-B）

- 每批次＝独立 SNAPSHOT 版本（沿用 `transaction_type="SNAPSHOT"`、`sync-<run_id>.ndjson`）；分支 current view＝最新批次；历史按 run→txn→file 可读。
- 禁止累计 merge/UPSERT/改 APPEND 伪累计（APPEND view merge 不保全 file-index 读取面——R-B 原文）。
- **"latest 仅本批次"的证明**：`/files` 计数不是证明；必须按分支 HEAD 版本下载/preview 逐值==本批次，且旧/新批次 bytes/hash/txn/lineage 分别独立核对（§8 H2/H3）。

## 4. Durable owner、绑定与事务

### 4.1 数据模型（精确 DDL 见 implementation-contract §1）

- 新表 `sync_def_checkpoints`：per-Def 一行。字段：绑定三元组（source/output/fingerprint）、`checkpoint_value`（已确认完成 token，NULL=从未确认）、`confirmed_epoch`（单调确认计数）、`active_run_id/active_since`（单飞 owner）、`advanced_by_run/advanced_at/last_batch_*`（最近确认批次 forensics，含 txn）。
- `sync_runs` 新增**内部列**（不进公开投影）：`cp_mode/cp_base_epoch/cp_base_value/cp_binding_fingerprint/cp_output_dataset_id` —— admission 时刻冻结的不可变基线（R-C §4：不依赖 Claim 后内存值做 CAS）。不序列化到任何公开 JSON（R-C §8：captured input 可能是敏感厂商续读材料）。
- 迁移：纯 `CREATE TABLE IF NOT EXISTS`＋`ADD COLUMN IF NOT EXISTS`，空库/旧库/重跑/三副本并发安全；legacy 4+2 零接触；**不为 old6 或任何旧任务回填/收养**。

### 4.2 占用协议（R-C §2 顺序，单一 admission 事务）

入队事务（manual `EnqueueSyncRun` 与 scheduled `ClaimDueScheduleSlot` 共用插入段）在 incremental def 上按序：

1. 既有资格检查与 Source 锁照旧（manual 沿 P2.3 策略锁型；scheduled 恒 NOWAIT）；
2. **Def 强锁（FOR UPDATE）**替代该路径的 KEY SHARE（R-C：KEY SHARE 不足以串行化同 Def 的初始化/绑定写者）；非增量路径保持现语义；
3. Checkpoint 行：存在→`SELECT ... FOR UPDATE NOWAIT`（55P03 → 整事务回滚：manual＝typed contention 拒绝，scheduled＝contention skip，**不建 run 不打 stamp 不产 tick**——不得在已 abort 事务上 finishNoRun）；不存在→INSERT 空位（owner NULL，epoch 0）——Def 强锁已排除并发初始化竞态；
4. 锁后核验：owner 必须 NULL 且 `binding_fingerprint` 匹配当前锁下重算值（成分见 §4.3）；owner 非空＝**逻辑 busy**（manual 409 `sync_incremental_busy`，不向不可见父泄漏 owner；scheduled 在活事务上 `finishNoRun(skipped_incremental_busy)`：真实 skipped tick＋**cron 游标照常推进**＋不建 run＋不打成功 stamp＋不碰数据检查点）；指纹失配＝`sync_incremental_binding_stale`（不建 run；恢复路径见 §4.4）；
5. **INSERT pending Run**（携带 §4.1 冻结基线内部列）；
6. **UPDATE checkpoint SET active_run_id=新run**（FK 此时才可满足——R-C 纠正 v1 的立即 FK 失败）；
7. Source 准入 stamp 恒为事务最后一条（现语义）。

### 4.3 绑定指纹（锁下重算，双侧使用）

`sha256(canonical_json{source_id, base_url, resource_path, records_path, pagination{type,cursor_param,next_cursor_path}, incremental{request_param,completed_checkpoint_path,initial_value}, query_params(sorted), headers(sorted), output_dataset_id, credentials:[(kind,fingerprint)…sorted]})`

- credentials＝源 auth 引用的各 kind 的现有明文 sha256 `fingerprint`（repo.go:377/handlers.go:439）——内容级身份，**同值重写不失效**（精确 same-value 语义；credential rotation policy 显式：换值→指纹变→§4.4 围栏与 stale 拒绝，绝不静默重置）。
- 排除：`limits`、cron/enable/`schedule_revision`、name/status、`connections.updated_at`（R-B：后者 name/status 也会动，不可作绑定 epoch）。纯 cron/name/status 变更零数据位置影响。

### 4.4 变更围栏与恢复路径（R-C §4：无编辑副作用重置）

- `UpdateSyncJob`/`UpdateConnection`/`DeleteConnection`/`DeleteSyncJob`/`SetCredential`/incremental 开关切换（mode-off patch）：在自身锁事务内（§4.6 锁序）断言受影响 def 的 `active_run_id IS NULL`；非空 → 409 `sync_incremental_owner_unresolved`（附 owner run 与 active_since），**无论 run 是 pending/running/failed-unknown/expired**；含级联删除与 takeover（§4.7）。
- `SetCredential`：由无锁单语句改为 Source 锁事务：围栏→UPSERT→（值变化→指纹变化，但**不删行**，只让后续 admission/publish 门重算失配→`binding_stale`）。
- **恢复路径（显式二选一，P25 取前者）**：P25 不提供 reset/rebind API；binding stale 或需要全新位置时，**文档化恢复＝新建 SyncDef（newDef-only reset）**。显式 reset/rebind API 列为后续候选交 root。绝不静默 reset/delete，绝不在 owner 未决时 reset/release（含 mode-off patch、删除、cascade、takeover）。

### 4.5 成功/失败转移（R-C §3/§5；完整矩阵见 implementation-contract §4）

- **成功（usable ACK 后）**：单一 fenced 事务（Def→Checkpoint→Run，锁后另条 DB clock）：run 成功 CAS＋checkpoint `epoch+1/value=候选/owner=NULL/forensics` **全有或全无**。任一谓词失配（owner/epoch/fingerprint/租约/阶段）→ typed fenced/uncertain，**零部分写、零释放**（v1 的 `succeeded+checkpoint_not_advanced` 分支已删除）。SQL 故障→双向回滚。本地 COMMIT 丢响应→诚实 unknown（全旧或全新），不盲重试、不宣称 DB 物理未动。
- **可证零 dispatch 的失败/取消**（有效 owner：pull/解析/limits/候选校验/egress 失败、gate 前取消、gate 复验失败——一切 `publish_entered_at IS NULL` 状态）：`FailIncrementalSyncRun`（Def→Checkpoint→Run）单事务写 failed/aborted＋owner 释放，epoch/value 不动；**释放谓词以 DB 断言 `publish_entered_at IS NULL` 为准**。**保守 unknown 边界＝publish gate ENTRY（v2.1）**：gate 已入后即使 CommitOutput 从未发出，也只写诚实 failed（若租约仍有效）＋retain owner；`publish_entered_at` 只是保守持久状态，不证明请求到达服务器（R-B），也不从缺 ACK 反推无远端发布；内存 sticky 证明已删除。
- **dispatch 后无 usable ACK**（超时/断连/5xx/ACK 不合格/解码失败）：Run-only CAS 写 failed（不改 checkpoint、不释放——现有 `completeFailedAfterCommitAttempt` 路径即此语义），owner 保留 → P26。无 4xx 例外（Q2 裁决）。
- **失租**：P2.1 语义无本地终态；owner 保留（未决）——即使失租发生在 dispatch 前（有效 owner 才能写终态+释放；失租者写不了）。
- **空批次确认**：成功路径，epoch+1；token 按 §2.2 规则。

### 4.6 锁序（R-B/R-C；细节见 implementation-contract §5）

- 全局偏序 **Source→Def→Checkpoint→Run**；触碰 checkpoint 的一切路径取全链或合规前缀；`withRunLock` 家族保持 Run-only 且**永不后取父锁**。
- 新 ordered 路径：admission（现序+Def 强锁）、`ClaimIncrementalSyncRun`（Def→Checkpoint→Run）、`BeginIncrementalPublishGate`（Def→Checkpoint→Run）、成功 finalize（同）、**pending cancel 与 pre-publish 释放（Source→Def→Checkpoint→Run 全链）**、变更围栏（各自编辑事务）。
- 锁前 lockless 可信预读 IDs，锁后重读再断言；**全部可能等待的锁拿齐后另条 `SELECT clock_timestamp()`** 做一切时敏判定；**任何 DB 锁不跨 REST/Dataset HTTP**；context/rollback/lock-wait 有界（Q3 裁决：现存 checkpoint 行用 `SELECT…FOR UPDATE NOWAIT`，无 `UPDATE…NOWAIT`、无事务中途 SET LOCAL lock_timeout；Def 强锁已消常规初始化竞态；scheduled 55P03＝contention skip 不产 tick）。
- 并发区分：**逻辑 owner-busy**（活事务，可提交真实 skipped tick＋推进 cron 游标）vs **锁争用**（事务已 abort，回滚＋计数 skip）；两者都不建 run、不碰数据检查点、不打成功 stamp。

### 4.7 Takeover 防后门（R-C §5）

`TakeoverExpiredSyncRun`（lease.go:397）加反连接谓词：run 被任何 `sync_def_checkpoints.active_run_id` 引用即**不具接管资格**（返回 nil）。P25 未决 work 无任何自动接管/重执行/重发布路径；owner 不因 run 终态或租约到期而清除。

## 5. ACK 契约（R-C §6）

- `DatasetCommitResult` 增加 `Status string`（生产 adapter 解码 `transaction.status`——字段已在解码结构中，v1 弃用未查）。
- **P25（增量 run）handler 侧强制**（注入 port 同契约）：`TransactionID != Nil && Status=="COMMITTED" && DatasetID==请求 target`。2xx/任意 txnUUID 不构成 usable ACK。
- 兼容规则（明确且不弱化 P25）：**非增量 run 保持现校验**（仅 txnUUID 非空）——旧测试 fixture/替身零改动；增量路径的测试替身必须返回 Status。
- 解码/status/target 错误＝dispatch 后未知（非"未提交"证明）；H8 类断言不得写"无新版本引用＝零发布"，正确断言集见 §8 H9。

## 6. 可观察性与暴露（Q4 裁决，R-C §8）

- 任何 API **不回显 checkpoint 原值**：只给 `initialized`、长度、sha256、confirmed_epoch、owner（run id/状态/since/blocked reason）、last-confirmed（run/txn/rows/bytes/hash/at）。全部仅父链可信租户可见。
- run 捕获的输入（`cp_base_value` 等）永不公开序列化；metadata/日志/URL 错误只含长度/hash。测试用隔离 fixture 记录或受控 SQL/raw 证据证明实际请求输入，不开生产原值 API。
- 未决 owner 与"无自动恢复"必须可查询（409+checkpoint 视图），不得伪装成新任务成功。

## 7. fixture 与工具边界（R-C §9）

- 新独立增量 fixture 按 §2 协议实现（固定 request_param、最终响应 completed path、排他续读、A→B→C 追加、非法/重复 cursor 400、先提交后丢响应页可开关）；旧 fixture 一字不动。
- 旧回归套件：旧 72 PG 断言冻结；P24 smoke7 用的 guard4 在现状 6 条下**不可能原样通过**——一切旧套件 guard 适配必须是**owned wrapper 替换**：逐个计数、归档、标注 adapted，绝不修改冻结的 P24/P23 工具，绝不伪称逐字未改。正式 runtime 计划另行 root review。
- q3 收尾：可加 oldABC2＋P25 有意未决行，**终集 roster 必须显式**，ABC 后零 q3 变更；旧六条/P24 owned17（17 defs/32 ticks/15 runs/0 grants）与 archive859 不可变。

## 8. 验收矩阵（契约级；PG/Tools 轨道按 implementation-contract 对齐细化）

### 8.1 真实 PG16（新必需名单列；normal/race 分日志+真实 rc；防伪清单按 R-B）

| # | 用例 | 判定 |
| --- | --- | --- |
| PG1 | 空库/旧库/重跑/三副本并发迁移 | schema 一致；legacy 4+2 的 27 字段投影不变；无回填 |
| PG2 | 双池并发 admission 同 def（含行不存在与已存在两态） | 恰一 owner；NOWAIT 争用路径回滚零副作用（无 run/stamp/tick） |
| PG3 | 逻辑 busy vs 锁争用分叉 | busy：skipped tick+cron 推进+零数据推进；争用：零 tick 零 run |
| PG4 | 兄弟 def 并行／租户/父链/目标负集 | 独立推进；409 不泄 owner 给不可见父；隔离全负 |
| PG5 | 成功原子性：run 更新成功+checkpoint 语句失败→全回滚；谓词失配（owner/epoch/fingerprint/旧 gen/token）→零写零释放 | 行（含 value 原值）逐字节不变 |
| PG6 | 锁等待中租约到期→锁后钟拒绝完成 | 完成被拒、行不变（真两阶段断言） |
| PG7 | pending cancel／pre-publish 失败／gate 复验失败释放 | 终态+owner=NULL 同 commit；epoch/value 不动 |
| PG8 | 未决 owner 跨读/重启/变更/删除/重置/mode-off/takeover 尝试保持 | 全部 typed 拒绝或资格否定；行不变 |
| PG9 | source config/credential（SetCredential）变更竞态与围栏 | 无死锁；同值重写零影响；换值→stale 不静默重置 |
| PG10 | 迁移重放不清 owner/断点 | 重放前后行一致 |

### 8.2 真实 HTTP（隔离栈+新 fixture）

| # | 场景 | 判定 |
| --- | --- | --- |
| H1 | 首轮 A | V1 逐值==A；COMMITTED/exact target/lineage；checkpoint 建立（value 存在、epoch 1）；admission 冻结基线经受控 SQL/fixture 记录证明被实际使用 |
| H2 | `cp` 追加 B 第二轮 | **只读 B**（逐值无跳漏）；分支 HEAD 下载/preview==B；V1 按版本重读 bytes/hash 不变；**files_total 不作证明** |
| H3 | 再追加 C | V3==C 跨页尾；三版本 lineage/run 对应 |
| H4 | 占用中触发（manual+scheduled） | manual 409 不泄 owner；scheduled skipped tick+cron 前进+数据 checkpoint 不动（两游标分离断言） |
| H5 | 配置负集：page 型/query 键冲突/控制字符/超长/类型错候选/非空批次候选未变 | 400 或 run 失败；全部 pre-publish 释放、零推进 |
| H6 | 旧 snapshot def | 与 P1 逐字节一致；P1 九场景全绿（未适配或显式 adapted 计数） |
| H7 | midpage 故障/limits 触顶/gate 前取消 | 不推进；释放可再触发且从原 token 续读；**gate 后中断（含未 dispatch）＝failed＋owner 保留**（v2.1 边界，并入 H9 类未决断言） |
| H8 | 同 def 并发双触发 | 第二个在 admission 拒绝；**仅一次远端发布** |
| H9 | **dropped ACK（fixture 先提交后丢响应）** | 独立证明远端已 COMMITTED 的 txn/bytes/hash；本地 run failed、checkpoint 不动、owner 保持、再触发 409；**绝不宣称零发布/无新版本** |
| H10 | 空批次 | 成功空版本+epoch+1+token 不变；（fixture 若提供新 token 亦验证前进分支） |
| H11 | sync/async/scheduled 三模式同 def 串行 | 同一断点链连续推进 |
| H12 | 双组织/跨域/个人域/伪造身份 | 端点与 409 隔离；伪造不改断点 |
| H13 | 围栏（owner 未决时 config/credential/target/删除/mode-off） | 409 `sync_incremental_owner_unresolved`；纯 name/cron 变更不重置不拒绝 |

### 8.3 回归与保全

同 R-B/R-C §7/§9：旧 72＋P1 9＋P2.1 ABC＋P2.2 11＋P2.3 8＋P2.4 7 原样（或显式 adapted 计数归档）；六任务保全（old4 全 27 字段+preboot；B/C 仅 SHA-pinned provenance+counters 2/5 stable active0+removedC 故障函数；无年龄例外）；owned17 与 archive859 不可变；ABC 终集 roster 显式；证据按 P24 规格（源码/镜像绑定、失败独立保留、standalone 脱敏、secret scan 0、全档 hash LAST）。

## 9. 遗留开放项（R-C 已裁决 Q1–Q4；仅列未决）

| # | 项 | 建议 | 置信度 |
| --- | --- | --- | --- |
| Q5' | binding_stale 的显式 reset/rebind API（P25 为 newDef-only 文档化恢复） | P26+ 候选；P25 不做 | 高 |
| Q6' | 空批次时源提供新 token 的前进分支 | 按 §2.2 支持（fixture 可选覆盖） | 高 |
| Q7' | 增量 def 的 run `sync-<run_id>.ndjson` 文件名是否需区分批次序号 | 不需要：run id 天然唯一，历史按 run 可读 | 高 |

## 10. 初见审查（多疑工程师视角）

- **"为什么最终响应才有完成 token，中间页给的不行？"** 中间页的 next 分页 token 是页内续页位置，语义上不承诺"其后无数据"；只有终止响应（源声明排空）携带的完成 token 才代表"已消费至"的位置。混用即 v1 错误。
- **"owner 放 run 行上，run 删除了怎么办？"** owner 未决时删除被围栏拒绝；owner 空闲时 def 删除→cascade 删 checkpoint（无未决状态可丢）；run 从不删除。
- **"gate entry 只是保守标记，凭什么它之后一律不释放？"** 这是 v2.1 的保守边界选择：gate 后本执行器可能已发出 CommitOutput，而持久层无法区分"已发出/未发出"；宁可将该窗口全部归入 unknown（failed＋retain，P26 对账），也不引入第二个易错的 dispatch 标记。崩溃即未决（owner 保留），方向保守正确。
- **"成功事务全有或全无，会不会把远端已 ACK 的成功吞成未决？"** 会——谓词失配或本地故障时 run 停留未终态、owner 保留，诚实暴露为未决待 P26 对账；绝不出现"succeeded 但断点没走"的单边状态（R-C §3 的要义）。
- **"SetCredential 改锁事务会不会挡住普通源？"** 只在存在 incremental def 的 source 上加围栏段；纯旧源路径不变。
