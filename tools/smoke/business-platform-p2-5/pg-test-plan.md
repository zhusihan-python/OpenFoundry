# P2.5 发布后推进断点 — 真实 PostgreSQL 回归测试计划（pg-test-plan.md）

状态：**DRAFT r9 — 全 15 名实施完成（compile-only 阶段）。** 按 root
澄清与 Stage2 测试 review（review-pg-code-round1 六项全数修复，§10 r9）：
15/15 名（repo 14 + handlers 1，§5）已写并双工具链编译验证，全部为
全量行为断言（无 Skip/TODO/骨架，负例带类别断言与有效对照）；真实 PG
启动/运行仍待 root 释放。

历史：r3 按 root 三份审查全面重写（review-initial-contract / review-round1
/ review-pg-plan-round1）；r4 并入释放令 5 纠正；主轨 design-draft v1 冲突
部分已死。

作者：ZCode GLM-5.3（PG-test 轨道，P2.5）。本轨道独占产物：本文件 +
`HANDOFF-PG.md`（`tools/smoke/business-platform-p2-5/`）。编码释放后拟按
P24 先例独占一个新集成测试文件（拟名 `checkpoint_integration_test.go`，
build tag `integration`；文件名/归属以 root 指令为准）。

权威链（新者胜）：
1. P24 completion（2026-11-14）— 72 必需名冻结、归档/工具/6 条受控
   unknown/fixture/卷全保留、PG16.15/go1.26.3/go1.25.14-race/pgx v5.7.4
   已验证组合。
2. P25 handoff + `review-initial-contract.md` — 稳定设计要求（含 PG 矩阵
   与五条防伪纪律）。
3. `review-round1.md` + `review-pg-plan-round1.md` — 当前最高优先级纠正。
4. `implementation-release.md`（root，2026-11-14）— 实施放行 + 5 条纠正
   + 文件归属（PG 轨道独占：`internal/repo/checkpoint_integration_test.go`、
   `run_pg_isolated_p25.sh`、`p25-required-names.txt`、`pg_exact_check_p25.py`、
   本计划与 HANDOFF-PG.md；不改生产/旧 72/Tools 文件）。
5. 主轨修订版 implementation-contract（同步中）— P25-API 精确签名以此为准
   回填 `[PC-*]`；例行一致性修复随同步通知 PG/Tools，语义变更须 review。

完成标准（机械判定；针对本轨道最终交付，非本设计稿）：
(a) §4 每个场景有确定性设计，只引现行真实调用链（§0 行号）或显式
    `[PC-*]` 占位；
(b) root 两条审查的每条纠正都映射到具体测试设计（§7 对照表）；
(c) ALGN-* 全枚举（§6b）；
(d) 编码阶段产物仅：本目录两文件 + root 指派的测试文件；最终日志中每个
    新必需名 exact 1 顶层 `=== RUN` / 1 `--- PASS` / 0 SKIP / 0 FAIL，
    normal（go1.26.3）与 race（go1.25.14/CGO）**各自独立真实日志、真实
    cmd/Go rc=0**，unified（旧 72+新名）单独一轮 fresh migrate-tests 日志；
    新必需名清单单独成文件跟踪（拟 `p25-required-names.txt`，tools 轨道
    所有），与旧 72 清单不混写；诚实钉选：无真实日志前不写任何
    RUN/PASS 断言。
(e) 本设计稿阶段：除本目录两文件外 git 零新增改动。

---

## 0. 现行调用链锚点（2026-11-14 核实，HEAD d7c40cf5）

与本计划直接相关的**已存在**代码（行号当日核实；其余见 P24 pg-test-plan
§0/§3 与主轨 design-draft §1）：

- 手动入队：`EnqueueSyncRun`（`internal/repo/lease.go:168`）= 池上锁less
  预检（`defBoundToSourceVisible` admission.go:162 /
  `sourceAdmissionNowait` admission.go:204）→ 事务内 Source 锁
  （admission.go:147，policy-on ⇒ NOWAIT，否则阻塞）→ Def `FOR KEY SHARE`
  （admission.go:184）→ `admissionGate`（admission.go:240；policy 未启用
  在 :252-253 提前 return nil —— **位置陷阱，见 T2**）→
  `insertPendingSyncRun`（schedule.go:190）→ `advanceSyncLastAdmitted`
  （admission.go:291，最后语句）→ commit。
- 调度入队：`ClaimDueScheduleSlot`（schedule.go:352）= Source NOWAIT →
  Def NOWAIT → 锁后 fresh clock → 候选 CAS+due → cron 解析 → grant 裁决
  → `admissionGate` → `insertPendingSyncRun` → tick → 游标 → prune →
  stamp 最后 → commit。55P03 ⇒ 事务 abort，bounded rollback，无 tick 无
  run 无游标（schedule.go:382-389）。
- 执行器：`executeSnapshotSync`（handlers.go:1697）：claim → 心跳/取消
  轮询（cancel.go:335 / lease.go:280）→ `FetchRESTSnapshot`
  （rest_snapshot.go:157；分页 cursor 仅 run 内、每次从空开始 :317）→
  `BeginPublishGate`（cancel.go:259，run 行锁、cancel-vs-publish 终决、
  全 TTL 续租、`publish_entered_at`）→ `CommitOutput`（handlers.go:1963；
  runtime/sync.go:113-184：**ACK 现仅 `TransactionID != uuid.Nil`，无
  COMMITTED/exact-target 校验** — 已确认缺口）→ `completeLeased`
  （handlers.go:2101）→ `CompleteLeasedSyncRun`（lease.go:330，租约 CAS
  + stage guard，`withRunLock` :116 = Run-first，仅锁 run 行）。
- 取消：`RequestCancelSyncRun`（cancel.go:93，Run-first 事务）。
- 变更面：`UpdateSyncJob`（repo.go:593，Def FOR UPDATE；改 target 时锁后
  `txDefHasActiveRuns` :618 **只查 pending/running**）；`UpdateConnection`
  （repo.go:239，config 变更锁后 `txSourceHasActiveRuns` :265 同限）；
  `SetCredential`（repo.go:377，**独立 UPSERT，无 Source 锁、无活跃检查、
  无 epoch/version** — root 指认缺口）；`DeleteConnection`
  （repo.go:308）；`TakeoverExpiredSyncRun`（lease.go:397，storage-only）。
- 迁移：`Migrate`（repo.go:55，单事务 + advisory xact lock 整批重放）；
  现存 19 文件；`sync_runs`/`batch_sync_defs` 无任何 checkpoint/owner 列；
  `sync_schedule_ticks.outcome` 为 TEXT **无枚举 CHECK**（新 outcome 无需
  DDL，20261115120000 已核）。
- 三种"游标"严格区分：run 内分页 cursor / P24 cron 游标
  （schedule_next_fire_at）/ P2.5 data checkpoint —— 三者在测试中**各自
  独立断言**，禁止互推（review-initial-contract §Scheduled 行）。

## 1. 冻结基线：72 必需名（不可触碰）

同 P24：59 旧 + 13 P24 名与断言全冻结；P25 不改任何既有测试文件/guard/
归档。最终 unified 日志一轮 fresh migrate-tests 中 72+新名各自 exact
1 RUN/1 PASS/0 SKIP/0 FAIL、真实 rc。旧套件若需适配（如 P24 smoke7 的
guard4 在当前 exact6 状态不可原样通过），只能由 tools 轨道做**owned
wrapper 替换并计数、归档、标注 adapted**（review-round1 #9）；本轨道不碰。

## 2. 契约锚点（按 root 两审查，替代 root 初拟稿与 draft v1 冲突部分）

- PC-A **独立 completion checkpoint 协议**（review-round1 #1，判死
  last-consumed-page 协议）：`incremental` spec（enabled、request_param、
  completed_checkpoint_path、可选有界非空 initial_value）；
  **checkpoint 请求参数每页固定**且与一切分页键/源认证 query 键不同、
  不得静默覆盖 query_params；run 内分页 cursor 依旧从空开始、仅分页用；
  **只有分页终止后的最终响应**给出必填 opaque completion checkpoint。
  候选原始字节 ≤512、非空、UTF8 无控制字符；错误/日志/元数据永不回显；
  不做 trim 归一化。候选缺失/类型错/超长/非法 ⇒ **publish 前失败**、数据
  位置不变、可证 prepublish 占用者释放。空批次候选==输入合法（epoch 前进、
  token 不变）；非空批次候选未变 ⇒ 安全失败（不得宣称正推进）。源须保证
  exclusive 增量流连续性与跨页稳定窗口（前置条件明示；不宣称通用 CDC）。
- PC-B 批次语义：每成功批次独立 SNAPSHOT version；branch 当前视图 =
  最新批次；历史按 run→transaction/file 可读；不累计/不 UPSERT/不改
  APPEND；`/files` 可列历史 ⇒ **file count 不可作 latest 证据**；若声称
  branch 生效视图须实测 preview，且新旧批次 bytes/hash/txn/lineage 分别
  核对。
- PC-C 同事务推进（review-round1 #3/#6 + release #5）：usable ACK（**exact
  请求 Dataset ID + status COMMITTED + 非 nil txn**，adapter 与 P25
  handler 双侧校验；`DatasetCommitResult` 增显式 status/confirmed 字段；
  非增量注入 port 可保留既有 handler 兼容——返回坏 mock 的旧 adapter 测试
  若其本意是成功契约，须把 fixture 修成合法响应而非弱化新校验）之后，run
  succeeded + checkpoint 值/revision/confirmed version/hash + owner 释放
  为**同一 fenced 本地事务**；**任何 CAS miss 不得单边提交 succeeded**
  （review-round1 #3：成功+推进+释放全有或全无；mismatch ⇒ typed
  fenced/uncertain，零部分写、零释放）。decode/status/target 错误 =
  dispatch 后未知，不是"未提交"证据。
- PC-D durable owner（+ **release #2 权威模式**）：per-Def data checkpoint
  owner 在与 pending run **同一 admission 事务**内取得（pending 即
  owner）；两个同 Def 调用者不可能都从同一 base input 到达远程发布（最终
  versionCAS 单独不足）；不同 Def 在既有 per-Source policy 下独立。
  admission 的锁less Source config 读只是**hint**；**Source 锁后重读权威
  config** 并选择/升级 Def 锁后再 INSERT run；捕获的 `run.cp_mode` 是
  Claim/execute/cancel/finalize 的**唯一权威**（绝不读当前 Source config；
  不收编 cp_mode NULL 旧工作）；LeaseClaim（或等价 json 字段）带内部捕获
  + 安全模式分发器；公共 Repo 入口
  ClaimSyncRun/BeginPublishGate/CompleteLeasedSyncRun/RequestCancelSyncRun
  必须在取 Run-first 锁**之前**路由捕获的 P25 工作，或显式拒绝不兼容的
  直接操作——**普通 Complete 不得在无 checkpoint 原子推进下写 P25
  succeeded**；一切内部 pending cancel 站点（coordinator closeNow/TTL/
  Stop/missing auth、dispatch failure cleanup、HTTP cancel）共享 CP-aware
  路由（只修 HTTP handler 不算修）。Run-only 的 renew/check/门后 failed
  可保留（前提：之后不再取父/CP 锁）。input/config/target/checkpoint
  base version 与 durable owner 在 **admission 时捕获并固化到 run**（内部
  字段，不公开序列化原文），publish 前与完成时核验。owner 不得指向未插入
  的 run（FK 顺序：资格核验 → checkpoint 行锁/初始化 → INSERT pending
  run（带捕获绑定）→ owner 指向该 run → stamp 最后，review-round1 #2）。
  增量 opt-in 路径的 Def 锁必须**强锁**（KEY SHARE 不足），legacy 未
  opt-in 语义不变。
- PC-J 类型化校验细节（release #4）：`InitialValue` 用 `*string`（或等价）
  表达**存在性**——显式空串拒绝、省略/null 放行；Source-auth query 参数
  冲突不能只靠 `NormalizeRESTAPISyncConfig`，需 **Source 级校验**并同时挂
  到 typed roundtrip 与权威 Repo 写/admission；completed checkpoint 的
  dot path 段需校验；DB 侧 checkpoint 上限是**原始字节**：用
  `octet_length BETWEEN 1 AND 512`（非 char_length；应用层校验 UTF8/无
  控制字符/不 trim/原始长度）；opaque 候选与 run 内分页 cursor 依旧分离；
  Rows/Bytes 拆成独立标记 JSON 字段；可为 run 查询/scanner 加安全
  mode/base-epoch 投影，但 raw cp_base_value/fingerprint 输入永不序列化。
- PC-K busy 次序（用户/root 钉死，销 ALGN-15）：Busy 与 P2.3 capacity
  同时出现时**沿既定次序**——Source 容量/间隔 gate 先判（typed 429），
  仅通过后才判 CP busy（409）；二者各自独立负例 + 组合优先级断言。
- PC-E 统一锁序：一切触 checkpoint 的 opt-in 路径恒
  **Source→Def→Checkpoint→Run**，含 admission、成功/失败/aborted
  finalize、pending cancel、delete/reset/config/**credential** 变更
  （review-initial-contract §Locks；review-pg-plan-round1 #5 对应）；
  禁止从 `withRunLock`/`RequestCancel` 的 Run 锁反向取父/checkpoint；
  纯 Run-only 的 heartbeat/check 可保留（前提：之后不再取父锁）。锁后
  重读租户/父链/target/捕获绑定；**全部可能等待的锁拿齐之后另发一条
  fresh `clock_timestamp()`** 判定 holder/token/generation/lease 过期；
  不跨 REST/Dataset HTTP 持任何 DB 锁；rollback/lock 等待/上下文全部
  有界。多 Def 的 Source 变更/reset 检查用稳定 Def-ID 排序加锁。
- PC-F 释放/保留二分（review-round1 #5 + **release #1 裁定**）：**publish
  gate ENTRY 即 P25 的保守未知边界**——一切
  `ReleaseIncrementalOwnerTerminal`/pending-abort 释放谓词都要求
  `publish_entered_at IS NULL`（DB 级 stage guard 即可独立执法，无需新增
  dispatch 标记/事务；sticky-dispatch-as-release-proof 已删除）。gate 已
  入但 workCtx 在真正 CommitOutput 前死掉 ⇒ 若租约仍有效只写诚实
  **failed** 并**保留** owner，绝不算 aborted、绝不凭内存 sticky=false
  释放；gate entry 不证明远端已发布，只标记不确定。gate 前 BindingStale
  且租约/owner 仍有效 ⇒ 可释放；fenced/过期/错 owner/DB 未知 ⇒ 零写保留。
  dispatch 后无 usable ACK / 失租 / 本地 finalize 未知 ⇒ **保留 durable
  owner**（run 即使 failed、租约即使过期也不清）；P25 无自动 owner
  超时/replay/takeover/resolve（P26 接续）；ACK 已知但本地 commit 响应
  丢失 ⇒ 仅承诺本地全旧或全新（**含 owner 可能已被成功的提交清掉**——
  不得凭猜测恢复/释放 owner，不承诺其物理保留，release #5）；返回不确定、
  不盲重发。`TakeoverExpiredSyncRun` 不得成为未决 P25 工作的重发布后门
  （legacy takeover 冻结测试不回归）。gate 失败分类：
  old-token/old-generation/过期租约/owner 不匹配/DB 未知 ⇒ 零写保留；
  仍有效 owner + 可证 prepublish 失败/取消（含 BindingStale）⇒ ordered
  finalize+释放不推进；**不得把一切 gate 错误路由成 failed+释放**。
- PC-G 绑定与变更围栏（review-round1 #4 + **release #3 补全**）：Source
  `updated_at`（name/status）与 `schedule_revision` 均**不是**数据绑定；
  规范哈希须包含 **file_glob 与完整生效 Source Auth 身份/配置**（type、
  inline token/basic username/password、scheme/header/query 放置、
  secret-ref 名与所引 credential 指纹）；**永不持久/发布明文 auth**，只存
  最终绑定哈希；既有 credential 内容指纹若为权威服务端计算/守卫，可作新
  epoch 的等价物。围栏面 = `UpdateConnection` **与裸
  `UpdateConnectionConfig` helper**、`SetCredential`、Def 编辑/删除、
  Source 删除——**retained（failed/未知）owner 必须拒绝**全部（绑定失效+
  重置不可作替代；核验精确 owner+run+checkpoint 值）。**纯
  name/status/cron/enable 编辑不得一刀切拒绝、不得重置数据状态**；只有
  有意义的 input/target/fileglob/mode 变更或 delete/credential 写路径走
  围栏；same-value 规则须成文。多 Def 写者锁按 **Def-ID 排序**，且在改动
  受保护 input/credential 行**之前**先取 Def 锁（使 Def→CP→Run 完成路径
  可安全重读 Source/auth 绑定而无反向 Source 锁）。无 mode-off 或 failed
  终态绕过未决 owner。old6 unknown 与旧普通任务**不回填不收编**；credential
  轮换保守"写即失效"策略显式化。
- PC-H 调度 busy 二分（review-round1 #7）：**逻辑 owner-busy**（行已锁、
  owner 判定不匹配）⇒ 可提交真实 skipped tick 新 outcome + cron 游标照
  P24 策略推进；**不建 run、不 stamp Source 准入、不推进 data
  checkpoint**。**物理锁竞争**（55P03/NOWAIT 失败）⇒ 事务 abort、无
  claimed run，**不得在已 abort 事务上 finishNoRun**（保持 P24 现行为）。
  手动 busy ⇒ 409（Q1 裁决），无新 run、无 stamp；不可见/错父链先走既有
  404/403 再谈配额。Q3 裁决：避免不存在的 `UPDATE...NOWAIT` 与事务中
  SET LOCAL lock_timeout 的歧义；既有 checkpoint 行用
  `SELECT ... FOR UPDATE NOWAIT`，Def 强锁序防初始化竞争。
- PC-I 可观测性（review-round1 #8）：list/detail 默认不回 raw checkpoint；
  暴露 initialized/长度/sha256、epoch、last confirmed run/txn/hash、
  owner ID/status/blocked reason（受信父链访问）。run 捕获输入为供应商
  continuation 材料，公开面只出长度/哈希；测试用隔离 fixture 记录或受控
  SQL/raw 证据证明真实请求输入，**不依赖生产 raw-value API**。

## 3. 确定性并发库存（冻结复用 + 拟新增）

冻结 helper（`internal/repo/`，build tag `integration`，P24 §3 清单全量
沿用不编辑）：`bootRepo`/`freshDSN`、`p23SourceBarrier`、`singleConnRepo`/
`backendPID`、`waitUntilBlockedBy`/`waitBackendLocksCleared`、
`p23WaitAdvisoryQueued`/`p23WaitClockPast`/`dbClock`、`p23Op[T]`/
`mustRecv`/`p23RegisterCleanup`、marker BEFORE 触发器模式、`tableRowJSON`/
`snapshotSyncRun`/`assertSyncRunPersisted`、`p24ApplyMigrationsExcept`、
`p23Stamp` 族、`p24RowBarrier`、`p24Go[T]`。

拟新增（编码释放后，`p25` 前缀，只进新测试文件）：
- `p25CpJSON(t, pool, defID)`：checkpoint 行**整行** `to_jsonb` 快照，
  **不减任何键、含全部时间戳**（round1 #1）；对照基线含 `updated_at`。
- `p25Owner(defID)`：owner/epoch/绑定真相读（直接 SQL，只读，不走被测
  方法自证）。
- `p25DefBarrier`/`p25CpBarrier`：Def/Checkpoint 行纯 FOR UPDATE 屏障
  （p23SourceBarrier 纪律；**只锁行、不改行**——round1 #2 的自然过期
  纪律同样适用于屏障）。
- `p25SeedOldDB`：P24 形状旧库种子（terminal run、live lease、legacy
  无租约 running、stamp、enabled schedule、grant、old6 unknown 形状
  复刻——不触真实 fixture/实例）。
- `p25BlockingPids(t, pool, blocked, waiters)`：以已知 backend PID +
  `pg_blocking_pids` 做分级锁序/阻塞证据（round1 #4；**不使用**
  pg_locks 获取时间戳排序推断）。
- `p25ApplyMigrationsExcept`：委托 `p24ApplyMigrationsExcept`（marker =
  `[PC-DDL]` 特征物，待契约）。

## 4. 命令矩阵 → 确定性设计（T1–T13）

映射 review-initial-contract §Verification requirements 的 PG 矩阵逐项。

### T1 迁移：空库 / 真实旧库 / 重跑 / 并发迁移 [PG 矩阵第 1 项]
拟名 `TestPGCheckpointMigrationEmptyOldRerunConcurrent`
- 空库：Migrate ⇒ `[PC-DDL]` 对象存在、默认值正确、旧 19 文件不受扰、
  rc=0。
- 真实旧库：`p25SeedOldDB` + `p24ApplyMigrationsExcept(marker)` ⇒ Migrate
  ⇒ **无回填/无收编**（old6 unknown 形状与普通旧任务均不产生 checkpoint
  行/owner/epoch）；既有触表逐字节保持（`tableRowJSON`/`snapshotSyncRun`
  全行含时间戳）；live lease、stamp、P24 游标/grant/tick 不动。
- 重跑：第二次 Migrate ⇒ 全部触表快照逐字节相等；无 CONCURRENTLY
  （`p23StripSQLComments`）；幂等形式。
- 并发迁移：两真实会话同时 Migrate（advisory xact lock 串行化）⇒ 两者
  rc=0、schema 一致、无交叉 DDL 残留（P24 冻结测试覆盖通用机制，此处
  按矩阵要求对新表显式复证一轮）。
- DB 层约束直证：`[PC-DDL]` 的候选长度上限是**原始字节**
  （`octet_length(resume_cursor) BETWEEN 1 AND 512`，release #4——多字节
  UTF8 串字符数合规但字节超限须拒）；PK 单行性按约束名精确断言（双插同
  def ⇒ 23505 且约束名匹配，防吞 discipline 同 P24 T2）。

### T2 admission 事务内 owner：FK 顺序与捕获固化 [PG 矩阵 sameDef 前置；PC-D]
拟名 `TestPGCheckpointAdmissionOwnerInTxOrder`
- 正确顺序按 review-round1 #2 钉死（签名待 `[PC-RESERVE]`）：锁less可信
  预读取 ID → Source→Def（**强锁**）→ Checkpoint 行锁/初始化（既有行
  `SELECT ... FOR UPDATE NOWAIT`，Q3 裁决）→ owner/绑定核验 → INSERT
  pending run（**捕获 base epoch/input/绑定到 run 内部字段**）→ owner
  UPDATE 指向已插入 run → stamp 最后 → commit。断言：run 行携带捕获值、
  owner==run.id、epoch 初值、fingerprint 落值。
- **FK 顺序反证**：任何"先 owner 后 run"的写法在 UPDATE owner 时即 FK
  失败（若契约无 deferred FK——review-round1 明示不得依赖 unstated
  deferred FK；测试以 `[PC-DDL]` 实际 FK 形状为准断言立时约束）。
- 捕获固化：admission 后用第二会话改 helper 输入/源配置（name/status/
  cron 类无害项除外）⇒ run 捕获值不变（publish/完成时按捕获值核验，
  见 T7/T9）；**捕获不被后续读取替换**。
- busy 拒绝：同 Def 第二次手动 Enqueue ⇒ 409 型 typed 拒绝（`[PC-BUSY]`
  签名待契约），**零新 run、零 stamp**（`p23AssertStampUnchanged`）；
  fingerprint 不匹配的残留 owner 同拒且行全行不变（round1 #1 全行对照）。
- **busy×capacity 次序（PC-K，销 ALGN-15）**：cap 占满或 min-interval
  未到时同 Def 入队 ⇒ **capacity/interval typed 429 先行**，不触 CP
  busy；容量/间隔放行且 owner 被占 ⇒ 才 409 CP busy；组合场景断言拒因
  类型与次序 + 各自独立负例。
- **权威模式（release #2）**：锁less config 读只是 hint；Source 锁后重读
  权威 config 再定增量资格与 Def 锁强度；`run.cp_mode` 捕获后即为
  Claim/execute/cancel/finalize 唯一权威（绝不读当前 Source config、不
  收编 cp_mode NULL 旧工作）。测试：Source 锁屏障下改 config 制造
  hint/权威分歧 ⇒ 以锁后读为准。
- 非增量 def 完全跳过：不建行、不取 checkpoint 锁、行为逐字节不变
  （legacy KEY SHARE 路径保持——T13 复证）。
- **位置陷阱**：`admissionGate` policy-off 提前 return（admission.go:252）
  ——占用/owner 逻辑不得位于 policy 分支内；policy-off Source 同 Def
  第二 run 仍拒（强测试）。
- 逐阶段回滚：marker 触发器打在 run INSERT 与 owner UPDATE ⇒ 各自零持久
  增量（run 行与 owner **同时**在或同时不在）、字节级回滚、会话复用、
  `p23AssertNoIdleInTx`。

### T3 同 Def 双池：owner 唯一性 [PG 矩阵 sameDef 2pool；round1 #3 修正]
拟名 `TestPGCheckpointSameDefTwoPoolOwnerUniqueness`
- **正确因果屏障（round1 #3）**：同 Source 的第一个 Enqueue 先持 Source
  再等 Def ⇒ 第二个不可能也停在 Def。因此：用**纯 Source 行锁**屏障 +
  两个 backend PID 等待（`p25BlockingPids`/`waitUntilBlockedBy`）构造
  同 Def admission 竞争：会话 B 停在 Source 锁队列；释放 ⇒ 序列化后
  **恰好 1** 个 pending run + 1 个 owner + stamp 恰一次；输家 typed busy、
  零 run 行。两种释放次序各一遍。
- 调度 vs 手动同槽：armed `ClaimDueScheduleSlot` 与手动 Enqueue 竞争 ⇒
  恰好 1 run；输家按 T11 分类断言（逻辑 busy tick 或 contention 回滚，
  不得混）。
- **不先 POST 的结构证明**：输家无 `publish_entered_at`、无 lease、无
  ACK 佐证 —— 远程发布的一切本地前置从未建立。

### T4 兄弟 Def 独立（无锁占用态）[PG 矩阵 siblingDef；round1 #3 修正]
拟名 `TestPGCheckpointSiblingDefIndependentNoLockHeld`
- **正确构造（round1 #3）**：Def A 先取得 **durable owner 且不持任何 DB
  锁**（执行器停在**上游 HTTP** 拉取——repo 层以"claim+gate 未入、owner
  已立"的中间态等价构造，或编码释放后用 handler 层 parked fetch 替身）
  ⇒ 同 Source 的 Def B Enqueue **成功**、独立 owner/行。
- **不得**期待 B 绕过 A 的 Source 短事务锁：若 A 停在 admission 事务内
  （Source 锁持有中），B 同 Source 必然排队/拒绝——该负例单独断言（与
  上面的无锁占用态正例区分，二者不可混写）。
- tenant/parent 活负例（PG 矩阵）：异租户/错父链对真实存在的 Def/
  checkpoint 的 not_visible/403 断言 + 正确租户正控制（§7-E）；
  invisible/mismatched parent 先 404/403、后配额（review-round1 #7）。

### T5 推进全有或全无 [PG 矩阵 run+cp SQL-trigger rollback；PC-C；round1 #7]
拟名 `TestPGCheckpointAdvanceAllOrNothingRollback`
- 成功路径（`[PC-ADVANCE]` 签名待契约）：live 租约 + usable ACK 参数
  （exact target + COMMITTED + 非 nil txn）⇒ 单事务内 run succeeded +
  checkpoint 值/revision/confirmed version/hash + owner=NULL 全 commit；
  锁序 Def→Checkpoint→Run（或 Source→Def→Checkpoint→Run，按契约），
  全锁后另条 fresh clock 判租约。
- **两侧触发回滚（PG 矩阵原文）**：marker RAISE 分别注在 run UPDATE 与
  checkpoint UPDATE ⇒ 各自整体回滚：run 仍 running、checkpoint/owner
  全行不变（`p25CpJSON` 全行含时间戳）。
- **CAS miss 禁单边 succeeded（round1 #7）**：构造 epoch/owner/fingerprint
  任一 miss ⇒ typed fenced/uncertain 返回，**零部分写、零 owner 释放**
  （run 不得 succeeded 单独落库——draft v1 该分支已判死，本测试即防复活）。
- **证据类型诚实（round1 #8 + release #5）**：marker RAISE 只证**本地
  事务回滚**；不将其表述为"模拟 commit 丢响应"。commit 丢失=远端已提交+
  本地无引用，属 HTTP 轨道（H 矩阵）；本地全旧/全新**含 owner 可能已被
  成功的提交清掉**——未知结果下不得凭猜测恢复/释放 owner、不写任何
  "物理未推进/未清"断言。

### T6 自然过期：锁等待中租约越界 [PG 矩阵 lease expires while waiting；round1 #2 修正]
拟名 `TestPGCheckpointLeaseExpiryUnderLockWaitNatural`
- **正确手法（round1 #2 原文）**：纯 FOR UPDATE 分别 park 在 Def /
  Checkpoint / Run 行上（三个子测试，屏障**只锁不改行**）；worker 在租约
  **真实仍有效**时启动（先证真阻塞：backend PID 阻塞证据）；等 DB 时钟
  **自然越过** `lease_expires_at`（`p23WaitClockPast`，不 UPDATE 该行）；
  释放屏障（行未被改）⇒ 被测方法获锁后另条 fresh clock 判定过期 ⇒ 拒绝
  （分类：过期租约 ⇒ 零写保留，PC-F）；**全部受保护行全行不变**。
- **禁止**：停车期间 UPDATE `lease_expires_at` 制造过期（改行会刷新求值
  并漏掉 stale transaction-clock 缺陷——这正是本测试要抓的 EvalPlanQual
  族 bug 的自然态版本）。
- takeover 交互：过期被 `TakeoverExpiredSyncRun` 换代后 owner 仍指该 run
  （run 仍 running，owner 随 run 不随租约）；P25 捕获工作**不得**经
  takeover 重执行/收编（PC-F；旧 legacy takeover 冻结测试不回归，T13）。

### T7 围栏矩阵：零写保留 [PG 矩阵 oldgen/token/cpversion/binding zero change；PC-F]
拟名 `TestPGCheckpointFencingMatrixZeroWrite`
- `[PC-ADVANCE]`/`[PC-GATE]` 拒绝矩阵（全部：typed 分类返回 + run 与
  checkpoint **全行不变** + owner 保留）：旧 generation（takeover 后旧
  token+gen）、错 token、租约过期（衔接 T6 自然手法）、已 terminal、
  owner 不匹配（他人 run）、cpversion/base epoch 不匹配、binding
  fingerprint 不匹配、DB 不确定类（注入的临时故障按契约分类）。
- gate 失败分类学（PC-F）：上述零写类 vs "仍有效 owner + 可证 prepublish
  失败/取消"⇒ ordered finalize+释放不推进（正路径在 T8）；**不得把一切
  gate 错误路由成 failed+释放**——以注入 DB 故障断言其落"零写保留"而非
  释放（防 draft v1 式一刀切路由复活）。
- stage guard 不回归：succeeded 需 gate 已入且无 cancel（lease.go:364
  谓词族）在 P25 推进事务中同样成立。
- **公共入口路由/拒绝矩阵（release #2）**：对捕获的 P25 工作，
  `ClaimSyncRun`/`BeginPublishGate`/`CompleteLeasedSyncRun`/
  `RequestCancelSyncRun` 的直接旧式调用必须**在取 Run-first 锁之前**路由
  到 CP-aware 路径或显式拒绝（零写）；普通 `CompleteLeasedSyncRun` 不得
  在无 checkpoint 原子推进下写 P25 succeeded——逐入口负例断言。

### T8 prepublish 取消/失败：有序释放 [PG 矩阵 pending cancel/prepublish release；PC-E/PC-F；release #1]
拟名 `TestPGCheckpointPrepublishCancelReleaseOrdered`
- 释放谓词 DB 级证据：一切 `ReleaseIncrementalOwnerTerminal`/pending-abort
  释放 SQL 的 WHERE 必含 `publish_entered_at IS NULL`（以 marker 探针/读
  回 SQL 语义断言；gate-entry 即保守未知边界，**无独立 dispatch 标记**，
  销 ALGN-16）。
- pending cancel（`[PC-CANCEL25]`，签名待契约；锁序
  Source→Def→Checkpoint→Run，**不得**从现行 Run-first `RequestCancel`
  反向取父）：pending run 取消 ⇒ aborted + 同事务 owner=NULL + 数据位置
  不变（epoch/resume/confirmed 原值）；全行对照。
- prepublish 失败释放：gate 前的 fetch 失败/界限超限/候选非法（衔接
  T12）/gate 复验失败/BindingStale（租约与 owner 仍有效）⇒ failed/aborted
  + 同事务释放 + 不推进。
- **gate 已入但未真 dispatch（release #1）**：gate entry 后 workCtx 死/
  注入 port 未被调用 ⇒ 若租约仍有效只写诚实 **failed + 保留 owner**，
  绝不 aborted、绝不凭内存 sticky=false 释放；stage guard（lease.go:364
  aborted 需 gate 未入）是该语义的 DB 级独立执法。
- 取消竞态两序：cancel 先 ⇒ gate 拒、零发布可证、释放；gate 先 ⇒
  cancel too_late、run 走真实结果（PC-F 语义保持）。

### T9 未知 owner 围栏：跨变更/删除/重置保留 [PG 矩阵 unknown owner retained across mutation/delete/reset；PC-G；round1 #5]
拟名 `TestPGCheckpointUnknownOwnerRetainedAcrossMutations`
- 构造 retained 态：dispatch 后无 usable ACK（注入超时/非 COMMITTED/
  target 不匹配）⇒ run failed + **owner 保留**（PG 矩阵"包括 failed Run
  with retained owner"）。read 稳定性：多次读 owner/run/checkpoint 全行
  不变（restart 维度归 HTTP/回归轨道，登记 §6b）。
- **retained owner 必拒面（round1 #5 + release #3 扩面）**：Source config
  变更（`UpdateConnection` **与裸 `UpdateConnectionConfig` helper**）、
  **`SetCredential`**、target/fileglob/checkpoint mode 变更
  （`UpdateSyncJob`）、delete（`DeleteConnection`/删 Def）、reset
  （`[PC-RESET]`）⇒ 全部拒绝；核验**精确 owner+run+checkpoint 值**（不是
  "绑定失效+重置"替代）；**绑定失效+重置不可作替代**（draft v1 已判死）。
- **纯编辑不围栏**：name/status-only（`UpdateConnection`）、cron/enable
  （`UpdateSyncJob`，revision 照 bump、调度 grant 行为照旧）在有 retained
  owner 时也**不得一刀切拒绝、不得重置数据位置**——与必拒面对照断言。
- 现行守卫空窗**基线已落盘**：`TestPGCheckpointLegacyMutationFenceBaseline`
  （本轨道 checkpoint_integration_test.go，对现有代码编译通过）两段式
  证据的第 1 段——`txDefHasActiveRuns`/`txSourceHasActiveRuns` 只查
  pending/running（repo.go:618/:265）⇒ failed 终态直接穿过 config/retarget/
  SetCredential；`[PC-FENCE]` 落地后同场景对 P25 捕获工作改证拒绝（第 2
  段），legacy def 保持基线真值。
- 绑定哈希覆盖（release #3）：fixture 改 inline token/basic/放置/
  secret-ref 名/credential 指纹/file_glob 各一 ⇒ 指纹变（数据绑定变）；
  改 name/status/cron ⇒ 指纹不变。多 Def 写者以 **Def-ID 排序**加锁、
  先 Def 后改受保护行——反序并发变更两 Def ⇒ 无死锁、可判定。
- `SetCredential` 现状（repo.go:377 独立 UPSERT 无锁无保护）同样两段式：
  现状穿透证据（已含在基线测试）+ `[PC-CRED-EPOCH]`（Source 锁 + 绑定 +
  未决 owner 保护）后拒绝；既有 credential 内容指纹若权威服务端计算可作
  epoch 等价物（契约确认）。credential 轮换保守"写即失效"策略显式断言。
- 无害变更不重置：纯 name/status（`UpdateConnection`）/cron/enable
  （`UpdateSyncJob`，revision 照 bump）⇒ checkpoint 数据位置不变（PC-G；
  与上面必拒面对照，两类变更不可混淆）。
- 多 Def 稳定排序：同 Source 多 Def 的 fence 检查以稳定 Def-ID 序加锁
  （两 Def 反序并发变更 ⇒ 无死锁、两请求可判定，PG 矩阵 lock 序回归）。

### T10 不跨 HTTP 持锁 [PG 矩阵 no DB lock across HTTP；round1 #4 修正]
拟名 `TestPGCheckpointNoDBLockAcrossPublish`
- **正确手法（round1 #4 原文）**：发布 handler 停在 parked port/fetch
  （编码释放后以注入替身；repo 层等价中间态）期间，另一**有界**事务对
  Source→Def→Checkpoint→Run 依次 `SELECT ... FOR UPDATE NOWAIT` **全部
  成功**后 rollback ⇒ 证明 park 期间无任何行锁被持；**不得**用
  pg_locks count==0 推断。
- 分级锁序证明：已知 backend PID + `pg_blocking_pids` 屏障法：分别 park
  Source/Def/Checkpoint/Run，观察被测事务的等待关系呈单向链
  （Source→Def→Checkpoint→Run），无反向等待（**不使用** pg_locks 时间戳
  排序虚构——round1 #4 明令）。
- 短事务结构：gate 事务 commit 后（publish_entered_at 已置）与 advance
  事务开启前，run/Def/checkpoint 行均可立即被第二会话 FOR UPDATE（衔接
  T4 的无锁占用态）。

### T11 调度 busy：逻辑 skip vs 物理竞争 [PC-H；review-round1 #7]
拟名 `TestPGCheckpointScheduledBusyLogicalVsContention`
- **逻辑 owner-busy**（行已锁、owner 判定不匹配）：tick 落真实新 outcome
  （`[PC-TICK-OUTCOME]` 名待契约；outcome 列无枚举 CHECK 已核）+ **cron
  游标照 P24 策略推进** + 无 run + **不 stamp Source 准入** + **data
  checkpoint 不变**。三游标语义独立断言：cron 游标动了、data checkpoint
  没动、run 内分页 cursor 无关（不落库）。
- **物理锁竞争**：checkpoint/Def 行被他持 + NOWAIT ⇒ 55P03 ⇒ 事务
  abort、bounded rollback、**无 tick 无 run 无游标无 stamp**；断言**不**
  在已 abort 事务上 finishNoRun（P24 现行为保持；防 draft v1 的
  busy-tick-在任何情形下都记 的复活）。
- 手动 busy：409 型 typed（Q1 裁决）+ 零 run 零 stamp；invisible/mismatch
  parent 先 404/403。

### T12 候选校验转移（PG 可观测部分）[PC-A/PC-J；review-round1 #1 + release #4]
拟名 `TestPGCheckpointCandidateValidationTransitions`
- PG 层只测**转移**（校验细节归主轨单测）：候选缺失/类型错/超长（字节）/
  非法 ⇒ **publish 前**失败 ⇒ prepublish 释放（T8 路径）+ 数据位置不变
  + 不建 confirmed version 引用。
- `InitialValue` 存在性语义（release #4）：`*string`——显式空串拒绝、
  省略/null 放行（首启从空读起）；`request_param` 与分页键/Source auth
  query 键冲突 ⇒ **Source 级校验**（typed roundtrip 与权威 Repo 写/
  admission 双挂）拒绝、零写；completed checkpoint dot path 段非法 ⇒
  拒绝。
- 空批次：候选==输入 ⇒ epoch+1、token 不变、owner 释放（成功路径）。
- 非空批次候选未变 ⇒ 安全失败：不推进、不宣称正推进、释放（可证
  prepublish）。
- 候选回显禁令：`[PC-DDL]` CHECK（octet_length ≤512）DB 直证（T1 已
  含）+ 错误路径日志断言归主轨；PG 侧不依赖生产 raw-value API（PC-I）。

### T13 兼容：legacy 快照与 old6 [计划#1/#7；PC-D 尾]
拟名 `TestPGCheckpointLegacySnapshotAndOld6Compat`
- 未 opt-in def 全路径（enqueue→claim→gate→complete）行为与 P24 逐字节
  一致：无 checkpoint 行、无 owner、`withRunLock` Run-first 语义保留、
  legacy `CompleteSyncRun`（repo.go:881）只关 legacy 行。
- old6 unknown 形状（种子复刻）迁移前后全行不变、不被 P25 逻辑收编/
  推进/释放（与 T1 互补：T1 证迁移不造行，T13 证运行时逻辑不碰）。
- `TakeoverExpiredSyncRun` 对 legacy 行照旧可用（冻结语义）；对 P25 捕获
  工作不得重执行（T6 已断言 owner 保持，此处断言无重发布路径接入）。

## 5. 顶层命名矩阵（15/15 已写并双工具链编译验证）

| # | 名 | 状态 | 覆盖 |
|---|---|---|---|
| 0 | `TestPGCheckpointLegacyMutationFenceBaseline` | 已写+编译绿（legacy 围栏永久真值） | T9 前置/T13 |
| 1 | `TestPGCheckpointMigrationEmptyOldRerunConcurrent` | 已写+编译绿 | T1 |
| 2 | `TestPGCheckpointAdmissionOwnerInTxOrder` | 已写+编译绿（p25Admit=契约锁序内联） | T2 |
| 3 | `TestPGCheckpointSameDefTwoPoolOwnerUniqueness` | 已写+编译绿 | T3 |
| 4 | `TestPGCheckpointSiblingDefIndependentNoLockHeld` | 已写+编译绿 | T4 |
| 5 | `TestPGCheckpointAdvanceAllOrNothingRollback` | 已写+编译绿 | T5 |
| 6 | `TestPGCheckpointLeaseExpiryUnderLockWaitNatural` | 已写+编译绿 | T6 |
| 6b | `TestPGCheckpointAdvanceExpiryUnderLockWaitNatural` | 已写+编译绿（r9 新增：成功推进路径的自然越期专项，review-pg-code-round1 #6） | T6 成功孪生 |
| 7 | `TestPGCheckpointFencingMatrixZeroWrite` | 已写+编译绿 | T7 |
| 8 | `TestPGCheckpointPrepublishCancelReleaseOrdered` | 已写+编译绿 | T8 |
| 9 | `TestPGCheckpointUnknownOwnerRetainedAcrossMutations` | 已写+编译绿（全围栏面矩阵：UpdateConnection/UpdateConnectionConfig/SetCredential/retarget/file_glob/Delete；纯编辑放行不重置；takeover 防后门；并发双写者无死锁；newDef 恢复路径） | T9 |
| 10 | `TestPGCheckpointNoDBLockAcrossPublish` | 已写+编译绿（PG 独占 `internal/handlers/checkpoint_integration_test.go`） | T10 |
| 11 | `TestPGCheckpointScheduledBusyLogicalVsContention` | 已写+编译绿（逻辑 busy 真实 tick+cron 游标前进+数据冻结+不 stamp；stale 独立 outcome；物理竞争零持久副作用无 tick） | T11 |
| 12 | `TestPGCheckpointCandidateValidationTransitions` | 已写+编译绿 | T12 |
| 13 | `TestPGCheckpointLegacySnapshotAndOld6Compat` | 已写+编译绿（old6 六形种子：不收编/重放字节同/不接管；legacy 全生命周期零 checkpoint 态+legacy 并发+legacy takeover 保留） | T13 |

注（r5）：admission 集成（EnqueueSyncRun 增量分支）落地后，T2/T3/T4 的
p25Admit 内联构造应改为（或加测）真实公共入口；围栏已见主轨落地进行中
（UpdateConnection 已接 lockSourceDefsForProtection/assertCheckpointOwnersFreeTx，
2026-11-14 曾短暂缺 `bytes` import 破损编译，主轨已自修）。

冻结条件与运行纪律：新必需名单单独文件（`p25-required-names.txt`，
DRAFT，诚实钉选）；normal（go1.26.3）与 race（go1.25.14/CGO）**各自
独立**真实日志 + 真实 cmd/Go rc；unified（72+新）单独一轮 fresh 日志；
exact 检查器 `pg_exact_check_p25.py` 沿用 P24 语义（1 RUN/1 PASS/0 SKIP/
0 FAIL，顶层名去重，子测试不计数，rc 文件为真）。

### 5b. 本轨道文件归属与当前执行阶段（release §File ownership + root 2026-11-14 补充）

- PG 轨道独占：`internal/repo/checkpoint_integration_test.go`、
  **`internal/handlers/checkpoint_integration_test.go`（root 补充裁定：repo
  包集成测试 import handlers 构成 import cycle，real-handler+PG 用例独占
  此新文件，integration tag，不改 Primary 非 PG 测试）**、
  `tools/smoke/business-platform-p2-5/run_pg_isolated_p25.sh`、
  `p25-required-names.txt`、`pg_exact_check_p25.py`、`pg-test-plan.md`、
  `HANDOFF-PG.md`。不改生产代码/旧 72 断言/Tools 文件/主轨文件。
- 新必需名总清单保持精确单源：两文件的名统一进
  `p25-required-names.txt`；runner 的 compile/new 模式已同时覆盖
  `./internal/repo/` 与 `./internal/handlers/` 两个包。
- 当前阶段：**compile-only**——`run_pg_isolated_p25.sh` 的
  `compile-normal`/`compile-race`（隔离 Go 容器 vet + `go test -c`，
  GOPROXY=off、既有共享 Go 缓存卷、repo 只读挂载）；真实 PG
  `up/new-normal/new-race/unified` 模式**已备好但未获释放**（root 审过
  候选后才放行 isolated real PG）。证据落
  `/private/tmp/openfoundry-p25-repair-evidence/pg/`，真实 cmd/rc/raw。

### 5c. 致 Primary 的方法依赖通知（2026-11-14，随 r6）

- T10 已按注入缝 `Handlers.DatasetVersioning`（cmruntime.DatasetVersioningPort）
  与公共入口 EnqueueSyncRun/ClaimSyncRun 驱动真实
  `executeSnapshotSync`；上游源用进程内 httptest（127.0.0.1 + egress
  allow_insecure_http + Config.AllowPrivateNetworkEgress/AllowedEgressHosts）。
  依赖保持：executor 增量分支继续从 `lease.Run.CPMode` 取权威、
  post-dispatch failed 走 routed `FailIncrementalSyncRun`（gate 已入 ⇒
  retain）。若签名再变（尤其 Store 接口/executeSnapshotSync 参数），
  请同步本轨道（上轮 AcquireCheckpointForAdmission 加 binding 参数已
  同步一次）。
- F-6（已按现状断言）：UpdateConnection 围栏以 `bytes.Equal` 原始字节判
  "实际变更"；T9 的 fence 探针用 **binding 相关的 resource_path 变更**
  （原始字节与有效子集两种口径下都是真实输入变更），对 F-6 的最终口径
  不敏感；若 Primary 改为解析后子集比较，T9 无需改动。
- r7 补充：EnqueueSyncRun 增量分支、schedule.go busy/stale 分支、五处
  变更围栏（UpdateConnection/UpdateConnectionConfig/SetCredential/
  UpdateSyncJob 输入变更/DeleteConnection）与 takeover 守卫均已由
  Primary 落地，T9/T11/T13 因此直接以真实公共面断言（不再仅靠
  p25Admit 内联）。

## 6a. 主轨修订版契约发布前的缺口（预登记）

`[PC-DDL]`（修订版表形状/FK/约束名——注意 owner FK 指向 sync_runs 且
写入序为 run 先 owner 后）、`[PC-RESERVE]`（admission 占用语句族）、
`[PC-ADVANCE]`（成功推进事务）、`[PC-GATE]`（checkpoint 感知的
BeginPublish 复验）、`[PC-CANCEL25]`（有序 pending cancel）、
`[PC-FENCE]`（retained-owner 变更围栏谓词）、`[PC-CRED-EPOCH]`
（credential identity epoch 与 SetCredential 保护）、`[PC-RESET]`
（显式重置/rebind 或 documented newDef-only）、`[PC-TICK-OUTCOME]`
（调度逻辑 busy 的 tick outcome 名）、`[PC-DISPATCH-MARKER]`（安全
sticky dispatch 边界标记）、`[PC-BUSY]`（手动 409 typed 形状）、ACK
status/confirmed 字段名（`DatasetCommitResult` 扩展）。

## 6b. 对齐清单（ALGN-*，契约发布后逐条销账）

- ALGN-1 `[PC-DDL]` 全文与迁移文件名/marker 特征物。
- ALGN-2 不回填不收编（root 已裁；测试按此直写，契约只需确认无 deferred
  FK 等隐藏例外）。
- ALGN-3 admission 语句序（已由 review-round1 #2 钉：资格→cp 锁/初始化→
  INSERT run（捕获）→owner→stamp 最后）；待契约给精确语句族与强锁模式
  （Def FOR UPDATE vs 其它）。
- ALGN-4 手动 busy typed 返回与 409 映射（Q1 已裁 409；精确类型名待）。
- ALGN-5 busy 拒绝不消耗 stamp（root 已裁；契约确认 scheduled 逻辑 busy
  tick 与 stamp 的语句相对序）。
- ALGN-6 `[PC-ADVANCE]` 精确签名与放置。
- ALGN-7 usable ACK 参数形状（status/confirmed 字段名、exact target 的
  传参）。
- ALGN-8 owner 随 run 不随租约（root 已裁）；契约确认 takeover 分支无
  owner 改写。
- ALGN-9 ACK 双侧校验（adapter+handler/port）边界；PG 层参数化拒收矩阵。
- ALGN-10 失租/dispatch 未知 ⇒ owner 保留（root 已裁；无例外确认）。
- ALGN-11 retained owner 必拒全部变更面（root 已裁，draft v1 自动重置
  已死）；契约给 `[PC-FENCE]` 精确谓词与 SetCredential 参与 way。
- ALGN-12 `[PC-RESET]` 显式重置语义（root 已裁方向：显式或 newDef-only，
  无 silent reset）。
- ALGN-13 T10 parked-port 层次证明归属（repo 等价中间态 vs handler 替身）。
- ALGN-14 执行环境（of-p24-pg 沿用 vs 新 p25 实例）+ fresh readonly
  baseline + 六队列 guard —— root 另行审定；本阶段零运行时。
- ALGN-15 ~~busy×capacity 次序~~ **已裁**（PC-K）：容量/间隔 gate 先判
  429，通过后才判 CP busy 409；独立负例 + 组合优先级断言已入 T2。
- ALGN-17 空 batch epoch 前进 + token 不变（root 已裁）；候选不变的
  非空批次失败分类（failed+释放 or typed）待契约。
- ALGN-18 restart 维度（owner 跨重启保留）归 HTTP/回归轨道的证据归属。
- ALGN-19 三种游标独立断言中"run 内分页 cursor 不落库"的负证据形式
  （无表可查 ⇒ 以代码事实+T13 行为断言，契约确认无隐藏持久化）。

## 7. 两条 root 审查纠正 ↔ 测试映射 + 五条防伪纪律

review-pg-plan-round1 八条 → 落点：
1 全行零改（含 updated_at）→ `p25CpJSON` 定义 + T2/T5/T6/T7/T8/T9 全部
  对照；2 自然过期（不改行）→ T6；3 因果屏障修正 → T3/T4；4 禁 pg_locks
  时序虚构 → T10/`p25BlockingPids`；5 retained 必拒（含 SetCredential）
  → T9；6 gate 拒绝分类 + takeover 不收编 → T7/T6/T13；7 FK 序/捕获固化/
  禁单边 succeeded → T2/T5；8 证据类型诚实（RAISE≠丢 ACK）→ T5/T7。

review-initial-contract 五条防伪（P24 缺陷）→ 纪律：
- A **NOWAIT 不是 wait**：每个锁断言先钉真实子句（调度 Source/Def 恒
  NOWAIT schedule.go:368/410；手动 policy-on NOWAIT admission.go:135-140；
  `[PC-*]` 的 checkpoint 行锁 = SELECT FOR UPDATE NOWAIT，Q3 已裁）；
  NOWAIT ⇒ 快速零行零等待断言；阻塞 ⇒ `waitUntilBlockedBy` 真停车；
  逻辑 busy 与物理竞争分开断言（T11）。
- B **事务 now ≠ 锁后时钟**：一切时效判定用全部锁拿齐后另发的 fresh
  `clock_timestamp()`；"过期拒"先证"真阻塞时刻有效"（T6）；不 UPDATE
  时间列制造过期。
- C **同点重合**：cron 下一点可与旧点相等（严格 > 断言只按 P24 契约
  NextFireAfter 语义，不假设不等）；checkpoint 推进以 epoch/全行对照断言，
  不做 opaque token 词典比较（PC-A）。
- D **单连接池 PID 死锁**：MaxConns1 池上 backendPID 必须在发起占用连接
  的操作之前预热（P24 6c 纪律，全测试适用）。
- E **异租户伪阳性**：每个 not_visible/403 负例必须打在**真实存在的异
  租户对象**上 + 正确租户正控制（T4/T9/T11）。

## 8. 版本敏感项 → find-docs 先行

- 复用已验证组合（PG16.15/pgx v5.7.4/go1.26.3/go1.25.14-race），不改
  go.mod/go.sum。
- 已证 SQL 形式沿用：advisory xact lock、FOR UPDATE/NOWAIT/KEY SHARE、
  ON CONFLICT ON CONSTRAINT、`to_jsonb` 整行、`pg_blocking_pids`。
- 契约修订版若引入新构造（EXCLUDE、新 JSONB 操作符、deferred FK 等）
  → find-docs 查 PG16 官方文档后再落测试；特别地：若 `[PC-DDL]` 依赖
  deferred FK，须先证伪 review-round1 #2 的"不得依赖 unstated deferred
  FK"禁令边界（显式声明则可，隐式不可）。

## 9. 执行环境与边界（本阶段零运行时）

不运行 Go/真实 PG/Docker/HTTP；q3v9、of-p24-pg、6 条受控 unknown、P24
归档与 27 工具、卷、env600 全不触碰。P25 运行时释放（fresh baseline、
六队列 guard、最终名册 old6+ABC2=8 等）按 review-initial-contract
§Runtime 由 root 另行审定；本轨道只引用不实现。发现业务缺陷只登记
（§6a/6b 或报告 root），不实现。旧 72 断言冻结。

## 10. 修订

- r1（2026-11-14）：初稿（[CP-*] 占位矩阵，10 名）。
- r2（2026-11-14）：对齐主轨 design-draft v1（部分）——**随后被 root
  round-1 审查判定多处不可成立**，已废。
- r3（2026-11-14）：按三份 root 审查全面重写（协议/FK 序/禁单边
  succeeded/禁自动重置/统一锁序/自然过期手法/因果屏障/pg_blocking_pids/
  调度 busy 二分/证据诚实；13 名 + ALGN 19 条）。
- r4（2026-11-14）：按 `implementation-release.md` 对齐——5 条编码前纠正
  并入 PC（gate-entry 保守边界+IS NULL 释放谓词销 ALGN-16、权威
  cp_mode+公共入口路由、绑定哈希扩 file_glob/Auth 身份+UpdateConnectionConfig
  围栏+Def-ID 排序、octet_length+InitialValue 存在性+Source 级冲突校验、
  R9 owner 可已被清）；busy×capacity 次序裁死（销 ALGN-15）；矩阵 14 名；
  §5b 文件归属与 compile-only 阶段。真实 PG 运行仍待 root 对候选的 review。
- r5（2026-11-14，本文）：契约 v1.1 + 主轨 repo 层落地后的**实施轮**——
  checkpoint_integration_test.go 扩至 10 名（基线+T1..T8/T12），对已落地
  签名编译验证双绿（go1.26.3 normal + go1.25.14 race，vet+test -c rc=0；
  期间同步主轨两次中途变更：AcquireCheckpointForAdmission 增
  models.IncrementalBinding 参数；主轨 repo.go 一度缺 `bytes` import 自修）。
  T9/T10/T11/T13 留待主轨面（repo.go 围栏落地中/schedule.go/handler）。
  §5 状态表更新。
- r6（2026-11-14，本文）：root 补充裁定落地——新增 PG 独占
  `internal/handlers/checkpoint_integration_test.go`（T10：真实
  executeSnapshotSync + 注入 parked port + httptest 源；NOWAIT 扫描
  Source→Def→Checkpoint→Run 证 park 期间零行锁；usable ACK 推进/超时
  ACK failed+retain 两子测试）；runner compile/new 扩至双包；names 11
  活跃；§5b 归属更新 + §5c 致 Primary 依赖通知（EnqueueSyncRun 集成已
  落地，F-7 的公共入口断言并入 T10/T2 后续）。双工具链编译复验 rc=0。
- r7（2026-11-14，本文）：按 root 澄清补齐 **T9/T11/T13 全量行为断言**
  （无 Skip/骨架）：T9 围栏全矩阵（六面拒 + 纯编辑放行不重置 + 过期
  owned run 不可 takeover + 并发双写者无死锁 + newDef 恢复路径）、
  T11 调度二分（busy 真实 tick/cron 前进/数据冻结/stamp 不动；stale
  独立 outcome；55P03 竞争零 tick 零游标零 stamp）、T13 old6 六形种子
  （不收编/重放字节同/legacy takeover 保留）+ legacy 全生命周期零
  checkpoint 态。names 14/14 活跃。编译经历主轨三次中途符号未定
  （sourceIncrementalEnabled/ErrIncrementalRequiresControlled/
  incrementalAdmissionBinding——等待自修）与本轨道四处自错修正后：
  **compile-normal_20261114T123633Z rc=0、compile-race_20261114T123644Z
  rc=0**。真实 PG run 仍待 root 释放。
- r8（2026-11-14，本文）：按 root 首轮实际源 review
  （review-source-round1 + review-repo-round1）加 9 个契约真实预期子例
  （不改生产；源未修处测试如实红，即其价值）：
  T2+`def_lock_wait_capture_is_authoritative_post_lock`（repo #1：Def 纯锁
  屏障下 admission 真等待，持锁事务内 retarget 后提交 ⇒ 捕获必须是锁后
  新 target/重算 fp，无 stale pending）；T2+
  `configured_initial_value_is_durably_captured`（repo #5：无 confirmed
  值时 configured InitialValue 持久冻入 run.cp_base_value，cp 确认仍
  NULL/epoch0；未配置 ⇒ NULL）；T5+
  `current_binding_drift_refuses_advance`（repo #4：锁下重读当前全绑定
  对比捕获——越权 SQL 绕过围栏制造 config 漂移 ⇒ Complete 必拒，
  typed+零写）；T8 修正 `requires_reconciliation` 措辞（repo #8 API
  澄清）+`post_gate_fail_wrong_owner_is_zero_write`（repo #3：retain
  分支同受 owner/epoch/binding 守卫）+`
  cancel_wrong_parent_zero_writes_before_any_state_change`（repo #2：
  Source→Def(验 source_id)→CP→Run，错父/未知 run ⇒ NotVisible 零写）+`
  live_second_batch_owner_not_mislabeled_by_last_advance`（repo #8：可
  观测性随当前 owner run，不误用历史 advanced_by_run）；T12+
  `bad_txn_counts_hash_refused_before_locks`（repo #4：nil txn/负
  Rows/Bytes/非 64-hex ⇒ 锁前拒）；T13+
  `legacy_runsyncjob_cannot_bypass_checkpoint_owner`（source #4：opt-in
  def 上 RunSyncJob ⇒ ErrIncrementalRequiresControlled 零 run 零 stamp；
  legacy {}-config source 的 Enqueue/RunSyncJob 行为逐字不变——钉死
  repo #9 的 {} 配置回归）；T10+`
  repeated_cursor_error_never_leaks_the_raw_token`（source #2：循环
  next_cursor 错误永不回显 raw token 入 Run.error，pre-gate 失败 ⇒
  释放 owner）。names 保持 14（子例承载证明，不凑数）。compile 双绿：
  compile-normal_20261114T124347Z rc=0 / compile-race_20261114T124355Z
  rc=0。真实 PG run 仍待 root 释放。
- r9（2026-11-14，本文）：按 root Stage2 测试 review
  （review-pg-code-round1，6 项）全数修复：
  #1 T2 binding_stale 先经真实公共 pending cancel 释放 owner（保留
  cp/fp）再探 stale——分支真正可达，无假绿；
  #2 全部非候选参数合法化（hash-1/hx/hy/hz/h/empty → 真 SHA256，
  空批次=sha256("")；"not-a-hash" 是唯一非法 hash 负例）；marker 回滚
  断言升级为 *pgconn.PgError P0001 + 精确消息 'p25 forced checkpoint
  advance failure'（证命中 SQL）；候选/bad-txn 负例各自断言类别关键词
  （candidate/txn/rows/bytes/hash）+ 有效对照（同参合法调用成功）；
  #3 p25Admit 收 ctx 并全程传播（39 调用点）；三处 goroutine DB op
  （T2 Def锁等待/T3 双池/T6×2）注册 p23RegisterCleanup
  release→cancel→bounded-join；
  #4 T7 拆独立变体：wrong-epoch（owner 正确仅 epoch 漂）typed 零写；
  wrong-token/gen 子例追加 Complete 有效 adv 的成功 CAS 围栏零写；
  T8 post-gate 先 aborted（stage guard 拒+Run/CP 精确不变）再 failed；
  #5 T2 新增公共入口子例 public_enqueue_captures_and_serializes
  （happy 捕获 + typed busy + 零 stamp），Def 锁等待 retarget 竞态改经
  **生产 EnqueueSyncRun** 驱动（p25Admit 保留为 repo 层契约对照，不再
  冒充公共入口证明）；
  #6 新顶层名 TestPGCheckpointAdvanceExpiryUnderLockWaitNatural：合法
  gate+合法 adv、纯 Def 锁 park、阻塞时刻有效证明、自然越期、
  CompleteIncrementalSyncRun 拒（ErrLeaseLost）+ Run/CP 全行不变——
  补齐成功推进路径的 stale-clock 专项覆盖（names 14→15）。
  compile 双绿：compile-normal_20261114T125507Z rc=0 /
  compile-race_20261114T125531Z rc=0（过程失败轮 4 次全归档）。
  真实 PG run 仍待 root 释放。
- r10（2026-11-14，本文）：对齐 review-source-round2（3 项生产补丁对应的
  PG 覆盖 + 2 项 PG 自有修正）与 handler/缺例补全：
  #1 tenant pred $3 —— T8 cancel_wrong_parent 增跨 ORG 租户负例（真实
  run/def 对上 NotVisible）+ 同租户正控（字节断言移至正控前）；
  #2 双 UUID FK bug —— T11 新增 enqueued_positive_triple_uuid_identity：
  真 scheduled 增量正向（非 busy/stale/contention），断言
  tick.run_id == res.Run.ID == cp.active_run_id 三 UUID 同一 + 捕获冻结 +
  stamp 前进；
  #3 legacy 正控 —— T13 补 ran.CPMode Empty 断言（ControlMode=='legacy'
  与 requires_reconciliation 措辞在 r8/r9 已就位，本轮核实无残留）；
  补 T10 context_death_during_dispatch_fails_and_retains_owner（park 中
  取消 ctx ⇒ dispatch 未知 ⇒ failed+retain+requires_reconciliation，
  handler 级 R5）；补 T2 source_lock_wait_config_flip_routes_by_
  authoritative_read（legacy→incremental 翻转在 Source 锁等待中落地 ⇒
  公共 enqueue 以锁后权威读路由增量并捕获——hint 仅供锁型）。
  compile 双绿：compile-normal_20261114T130122Z rc=0 /
  compile-race_20261114T130132Z rc=0。15 名不变（证明入子例）。
  真实 PG run 仍待 root 释放。
- r11（2026-11-14，本文）：root 放行**仅隔离 PG up**（准备阶段，与业务
  最后 3 补丁并行）——`up` 模式按六条件重写并执行：
  (1) 证据先行：每组件先写 redacted .cmd/.meta 再执行；(2) 七组件
  （preflight/image/net-create/container/ready/gates/env-write）各自
  独立 attempt + 真实 rc，ports/version/labels 三门各有自身 rc 行，
  docker rc0 无法掩盖门失败；(3) 精确 of-p25-pg/of-p25-pg-net + 双侧
  owner label + **实测 0 host ports**；(4) 沿用本地缓存官方 postgres:16
  （image sha256:e17e8606…），实录完整 server 版本
  `PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) aarch64`，零依赖变更；
  (5) 同名 network/container/secret 已存在 ⇒ **保留不动**，只写只读
  status 报告并以 rc=3 退出（复跑 up 实测：三者全 preserved，容器
  created 时间未变、仍 running、仍 0 ports）；(6) secret 0600 于
  /private/tmp/openfoundry-p25-pg.env（证据外），仅记路径/模式/存在性，
  值与 Config.Env/DSN 永不输出。首轮 up rc=0（up-*_20261114T1304* 七组
  证据）；of-p24-pg 仍 running 未触碰。new-normal/new-race/unified/
  handler HTTP 仍禁止，待 root 完成 Source 3 补丁冻结与 PG 完整候选
  评审后放行。
- r12（2026-11-14，本文）：root 只读候选审查两项必修完成——
  (1) source_lock_wait_config_flip 跨库屏障修复：bootRepo+os.Getenv(baseDSN)
  改为 bootRepoWithDSN 返回的同一 freshDSN（Migrate/cleanup 语义不变，
  os import 移除）；(2) 全部屏障释放有界化：新增 p25BoundedRollback
  （独立 5s 预算、detached ctx），替换 5 处 cleanup 释放回调、2 处热路径
  释放（require 错误保留）、4 处 defer 屏障回滚——旧 72 文件零触碰。
  双版本 compile-only 复验：compile-normal_20261114T133516Z rc=0
  （go1.26.3）/ compile-race_20261114T133523Z rc=0（go1.25.14）。
  候选 SHA（本修订时点）：repo 测试 0c467e05…、handlers 测试
  5c09717e…、runner 24b2bcb3…、names ba2f0175…、checker ea5082a7…。
  实际 PG 仍未放行（up 已接受，勿再 up/down）。
- r13（2026-11-14，本文）：review-pg-code-round2 八条全闭（#1/#2 r12 已
  修；本轮 #3–#8）：
  #3 调度正向 stamp 改 nullable 断言（先 NULL 后非 NULL，不再
  p23MustStamp 先败）；stale 子例 run 计数基线改 1（已存在 aborted 行，
  断言"无新增"）；
  #4 wrong-token/gen Complete 重排：先以正确材料真实入 gate（Entered），
  快照后置，两变体各自独立 typed 零写（ErrLeaseLost + run 仍 running +
  双表字节不变）——gate NULL 不再掩盖成功 CAS 围栏；
  #5 bad_txn 类别断言对齐业务冻结文案（nil txn→"transaction id"、
  负 Rows/Bytes→"negative batch facts"、非法 hash→"content hash"，
  checkpoint.go:855/858/861 原文核对）；
  #6 old6 重塑为六条全 running 代表形（B 失租×2 / C 存活租×2 /
  gate-entered×2），attempt/generation 种子=1 ⇒ takeover 断言恰为
  attempt 2 / generation 2；terminal 兼容不再冒充 old6 覆盖（由基线与
  legacy 生命周期另行证明）；
  #7 names 明确 15 活跃 + **unified 总数 old72+new15=87** 陈述入文件头；
  #8 unified-createdb 拆独立组件（redacted .cmd/.meta 先行 + 自身真实
  rc），SHOW server_version 独立 attempt + 独立 version gate rc——版本
  读失败不再搭载 createdb rc0。
  本轮一次性 dual compile：compile-normal_20261114T134000Z rc=0
  （go1.26.3）/ compile-race_20261114T134012Z rc=0（go1.25.14）。
  候选 SHA（本轮终点）：repo 测试 43a635a4…3d6519、handlers 测试
  5c09717e…4a3473、runner 00ade9c0…ef1b3、names 480fc1ff…32255。
  业务源未动；真实 PG 测试执行仍未放行。
- r14（2026-11-14，本文）：review-pg-code-round2 追加 #9 修复——
  record_source_meta 不再只是记录 SOURCE_DRIFT 标志：捕获
  SOURCE_HASH_BEFORE/AFTER 与 SOURCE_DRIFT 全局；新增 p25_record_gates
  硬门（source_gate=内容哈希 before==after；version_gate=工具链匹配），
  run_new 与 unified 在原 docker/runner rc **不篡改**（.rc 保存原值）的
  前提下：各自独立落 `<attempt>.source-gate.rc` / `.version-gate.rc`，
  meta 记 original_docker_rc/source_gate_rc/version_gate_rc/wrapper_rc，
  wrapper 违规即非 0（rc=2）；compile 模式维持现状（#9 范围=三个 run
  模式）。exact checker 增 `--meta <path>`：final acceptance 消费 meta，
  SOURCE_DRIFT=1/source_gate=1/version_gate=1/wrapper_rc≠0 任一即拒
  （三分支负例实测：drift→FAIL、wrapper_rc=2→FAIL、干净→放行）。
  本轮集中 dual compile：compile-normal_20261114T134351Z rc=0（go1.26.3）/
  compile-race_20261114T134416Z rc=0（go1.25.14）。runner SHA 更新见
  HANDOFF；未 up/down；真实 PG 执行仍待 root 稳定窄复核放行。
- r15（2026-11-14，本文）：窄复核 #8 尾项 + meta 强制消费——
  (a) unified-db-version 重写：docker exec 直写自身 .raw，真实 SHOW rc
  立即落 .rc（不再经 $()/tr 管道冒充）；版本解析为独立第二步（从已存
  raw 读+归一），失败 SHOW（rc≠0）即使部分输出形似 16 也拒；version
  gate 写独立 `<attempt>.version-gate.rc` + meta（original_docker_rc/
  server_version/version_gate_rc），违规则 die；
  (b) checker `--meta` 升级为正式判定**强制**项：缺 --meta 即拒绝 bless
  （rc=2，仅显式 --allow-no-meta 供 ad-hoc）——真实 test release 将明确
  携带（root 指示）。纯负控实测：无 meta→REFUSED；drift=1→FAIL；
  version_gate_rc=1→FAIL；干净 meta→放行至参数校验。
  Go 测试未变，按指示不再 dual compile；sh -n rc=0 + checker ast ok。
  runner SHA d4946664…→**8c78e33c…80f2c**、checker f53f6056…→
  **7bf7deb3…c3a5e**。真实 PG 执行仍待 root 放行。
- r16（2026-11-14/15，本文）：**首轮真实 isolated new-normal 执行**（root
  放行，of-p25-pg 未动）：
  (a) checker 完整重写（配额中断的半编辑版留证 SHA 5f488247…dbd17）：
  恢复全部 exact-name 逻辑 + 严格 positive meta 契约（7 键唯一非空、
  before==after==--expect-source-sha、version_ok=true、四 rc 全 0、
  fail-closed）；CLI 控制矩阵 1 正 + 7 负全过（缺meta/空/缺键/重复/
  错SHA/缺名+skip/nonzero rc）；checker 现 SHA 36277d28…a332。
  (b) attempt1 `new-normal_20261114T141052Z` docker rc=1：恰 2 个顶层
  FAIL，均为 PG 自有 fixture 级（非生产）：T1 旧库子例跨迁移全行字节比
  较过严（合法新增 cp_* NULL 列）；T10 循环源子例误期执行器"诚实
  failed-run + nil err"返回。原件完整保留。
  (c) 修复（不弱化断言）：新增 p25RowJSONWithoutCp（两侧归一化剥离
  cp_* 后相等 + 显式断言五列全 NULL）；循环源接受 failed-run 形态、
  DB 级证明（Run.error 无 secret/owner 释放/epoch 0）不变。
  (d) attempt2 `new-normal_20261114T141338Z` **docker rc=0 / wrapper
  rc=0 / source/version gate rc=0 / version_ok=true**；exact 判定
  （--new-only --meta 同 attempt，诊断性无 pin）：**distinct=15 bad=0
  go_rc=0**——15/15 真实 RUN/PASS/0 SKIP/0 FAIL。
  (e) fail-closed 生效实证：携 root 旧 pin（65f6ce3c…，r13 修复前候选）
  判定被拒——fixture 修复改变了源码树，attempt 前后一致新源
  **a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27**；
  正式接受待 root 重 pin。新测试 SHA：repo f5391f32…b2fa0、
  handlers 414c2015…8d66d7。外层证据 newnormal-outer_20261114T141042Z
  .{cmd,meta,rc}。new-race/unified 待 root 审本轮 raw 后放行。
- r17（2026-11-15，本文）：root 接受新候选（a1bff07f…）并放行三阶段，
  全部正式绿（每阶段 checker --meta 同 attempt --expect-source-sha
  a1bff07f…ffd27）：
  ① new-normal（已跑不重发）= attempt 141338Z：formal rc=0，15/15；
  ② new-race `new-race_20261114T141638Z`：docker/wrapper/gates rc 全 0，
  go1.25.14 真实，formal rc=0，15/15；
  ③ unified `unified_20261114T141737Z`（fresh dataset db + 独立
  createdb/db-version 组件证据）：docker/wrapper/gates rc 全 0，
  go1.26.3，**formal rc=0，distinct=87（old72+new15）bad=0**。
  三 attempt source hash 逐一并等于 pin（meta 强制）。unified 首判因
  我调用处相对路径错（../../ 应为 ../，--new-only 不读旧名单故前两轮
  未暴露）FileNotFoundError——绝对路径重判过，调用处问题非 checker。
  h12 绑定接口：pg-candidate-bindings-r17.json（引用固定 attempt
  名/raw/rc/meta/SHA 与冻结 names 文件，不自选名单）。生产/旧72/q3/
  of-p24-pg 未动；失败 attempt 141052Z 原件保留。
