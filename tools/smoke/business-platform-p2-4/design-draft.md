# P2.4 可观察定时调度 — 业务设计草案 v2（design-draft.md）

- 状态：v2 DRAFT，已吸收 root review contract（`~/.codex/visualizations/.../handoffs/2026-11-14-p24/review-contract.md`，2026-11-14）全部强制修正与默认值；实施方向已批准。
- v1→v2 变更摘要（root 裁决逐条落实）：
  1. cron 双 flavor：5 字段推断 Unix5、6 字段推断 Quartz6，均支持；表达式长度上限 256 字节；UTC 默认 + IANA 时区；零新依赖。
  2. 旧存量 schedule_cron 迁移后 disabled + NULL 游标，无回填无隐式激活；显式提供的新非空 cron/tz 即使 disabled 也要校验；空 cron 显式清除并停用；PATCH 省略字段保持原值；enable 必须有合法非空 cron。
  3. revision 触发面扩为 cron/tz/enabled/output target/**file_glob** 的**实际变更**（同值 PATCH 不 bump）；create revision=1；disable 置游标 NULL；重新 enable/配置变更在锁后 DB 时间重算未来游标；arm 不改游标/revision（不可重触同槽）。
  4. 调度间隔默认 0=off，显式启用限 1–30s；grant 内存 TTL 默认 **300s**（范围 1–86400，再被已验证 JWT exp 封顶）；registry 1024 / batch 8 / 保留 500 / owner 新鲜度 90s 不变；保留 500 = 1 tick/min 约 **8h20m**（v1"八个月"表述错误，修正）。
  5. claim 契约：调用方 `now` 仅扫描提示；Source+Def 锁后 fresh `clock_timestamp()` 是 due/过期/next/准入的唯一权威；claim 携带候选 expected revision + scheduled_for，锁后失配干净拒绝；未来注入时钟不可提前触发。
  6. 公平性与非阻塞：调度侧 Source/Def 获取**始终非阻塞/紧有界**（含 policy-off Source，一律 NOWAIT），不得让整个轮询循环停在一个热点 Source 后；手工/公开路径阻塞语义不变；候选扫描用 keyset/轮转游标（固定最旧 LIMIT 8 会饿死后续 Source，废除）；心跳独立于 due 工作/Source 等待/本地队列满。
  7. 原子性与时间锚：cron 解析/计算与锁后 schedule 校验先于任何 run 创建；typed 容量/间隔拒绝可同 tx 记 skip+推进游标（不推准入 stamp）；成功路径 tick+run+游标同 commit，`advanceSyncLastAdmitted` 保持成功事务**最后一条 DB 语句**（在游标/tick/prune 之后）；55P03 事务绝不再用；唯一键冲突只匹配显式 slot 唯一约束。
  8. 诚实派发状态：tick 成功结果改名 **`enqueued`**（不称已派发/执行）；commit 与 Dispatch 之间崩溃 → pending Run 派发未确认，不自动收养/重放；已知 Dispatch 失败事后按唯一键补记真实有界关闭 aftermath；Run 仍是执行/终态事实源。
  9. 独立 schedule boot UUID：独立随机生成，不复用/不暴露 leaseHolder、hostname、lease token。
  10. 每 arm 一次全新随机 grant_id/epoch；DB 行持久并比较 grant_id+boot+revision+actor+可信 tenant 快照+期望 Source/Sync/output 绑定+绝对过期；本地旧快照绝不以他人 DB grant 的 actor/凭据执行；执行 tenant 用 arm 时捕获的已验证 caller tenant（绝不仅凭当前 def 制造更宽域）；配额/run 前复检 Source/Def 父链可见性；arm 的输出预检以锁定 expected revision/output CAS 绑定，目标/配置竞态 → 干净重试/冲突，绝不静默授权新目标。
  11. registry 并发与停机：DB arm 前先预留本机有界容量；并发本地 arm 变更串行化或 generation 安全 stage/activate CAS，失败/迟到操作只清理自己的 generation；DB commit 先于 JWT 激活使用；Stop 先在同 gate 下关闭 arm 准入/激活，再清凭据、有界 join 维护；Stop 后返回的 DB arm 不得重填 JWT 或报 armed=true；arm/revoke 与 claim 在 Def 锁下线性化；撤销/配置变更只影响未来准入，已准入 Run 走既有取消 API 与 P22 发布门语义。
  12. 绝对授权截止：**删除 v1 的 `剩余≥max(AuthMemoryTTL,TaskDeadline)+1s` 规则**（使短有效期 JWT 不可用且不正确解决延迟）；给调度派生准入/派发加可选 deadline cap：授权内存过期与执行/任务截止 = min(既有 Reserve 锚定界, grant 绝对过期)；手工路径默认不变；排队等待消耗同一预算；主动过期 sweeper 在调度关闭/无 due 行时也清理引用，Stop 清理；过期后不得新起 pull/预检/发布门，已进发布门保持 P22 有界诚实清理/不确定提交契约（不承诺远端撤回提交）。
  13. owner 存活：心跳只更新精确 (grant_id, boot, revision, actor/tenant, expiry) generation，用 DB 时钟且要求既有 expiry/新鲜度仍有效；绝不延长 expires_at、不复活 stale、不刷新他人；0 行 ⇒ 丢弃该本地 generation；dead/stale owner 在新鲜度过期后可产生 auth_required，恢复需 fresh caller arm；活远端 owner 绝不被无 JWT 副本抢占；GET 状态诚实区分 DB 最近授权/存活元数据与本机可用授权。
  14. 可观察性：tick API 租户/Source 权限保护，默认 50/最大 200，无私有字段 DTO；持久 scheduled_for、claimed_at、decision/reason、可选 Run 链接；detail 有界（≤256）且不得含 JWT/DSN/URL query；循环错误记录不吞；contention/local full/remote owner/not due 聚合计数；error_invalid_schedule 不建 Run 且游标明确挂起待修；无预留的本地容量/Stop 跳过可仅 log/counter（如实记录）。
  15. arm 响应必须携带调度循环运行状态，循环关闭时不得虚假承诺执行。
- 精确 Go 类型/方法签名/schema/API/JSON/config 见同目录 `implementation-contract.md`（对 PG/工具轨道的权威对齐文件）。
- 基线不变：branch `codex/dataset-pipeline-file-io`，HEAD `d7c40cf5`；P0–P2.3 dirty/untracked 全保留；冻结 59 测试名/断言不删不弱化。

## 1. 范围与非目标（v2 重申）

范围内：batch_sync_defs 表风格 TABLE_SYNC 的 REST snapshot 批同步定时调度全链（cron 双 flavor + 时区 + 启停 + 游标 + tick 可观察 + 多实例单火 + 错峰 coalesce + 授权可观察 + P2.1/P2.2/P2.3 语义复用）。

非目标：Temporal 迁移（ADR-0025 例外由 root 记录到架构计划，GLM 在 Connector README 记录实际限制）；media-set 调度保持休眠；不逐历史槽补发；不自动接管/重放；不持久化 JWT；不用 JWTSecret 后台代签；Source 凭据≠发布身份；不改 Dataset/Pipeline/go.mod/go.sum/冻结工具。

## 2. 现状事实

同 v1 §2（F1–F16，全部当日实读带 file:line，未变）；补充：
- F17 `libs/scheduling-cron` 同时实现 Unix5 与 Quartz6（`CronFlavor` 二值，`schedule.go:23-28`），按字段数推断 flavor 由调用方完成。
- F18 保留 500 条的正确时长换算：1 tick/min ≈ 8h20m；1 tick/s ≈ 8m20s（root 修正 v1 错误）。

## 3. ADR-0025 适用范围（v2 措辞与 root 契约一致）

本实现是**明确的、有限的** business-platform 实施路径对 ADR-0025 的例外：仅 Connector 当前 REST snapshot 批同步路径由本 DB 认领调度器执行；不实现也不替代平台 Temporal 迁移。root 负责把例外记入架构计划；GLM 在 Connector README 记录实际限制。

## 4. 契约总览（状态机不变，两维正交）

```
schedule.enabled（持久）: disabled ⇄ enabled；实际变更 bump revision；
  disable ⇒ 游标 NULL；enable/变更 ⇒ 锁后 DB 时间重算未来游标。
authorization（内存+非秘密DB行）: absent → armed(grant_id/epoch) →
  expired | revoked | stale(revision/owner失配或新鲜度过期)。
  每 arm 全新 grant_id；恢复 stale 必须 fresh caller arm。
tick 认领（单候选短 tx，始终 NOWAIT）:
  锁后 dbNow 权威 → 候选 revision+slot 失配干净拒绝 →
  无活 owner ⇒ tick(auth_required)+推进 / 活 owner=本机匹配 ⇒ 准入 →
  typed 拒绝 ⇒ tick(skipped_*)+推进(不推 stamp) / 通过 ⇒
  run INSERT → 游标/tick/prune → advanceSyncLastAdmitted(最后语句) → COMMIT
  ⇒ 派发(带 grant 绝对截止 cap)；tick 结果=enqueued（仅证明持久准入）。
```

## 5. 数据模型（v2 DDL）

迁移（唯一新文件）：`internal/repo/migrations/20261115120000_sync_schedule_observability.sql`（时间戳以实施日为准）。幂等、批处理重放、无 CONCURRENTLY、无回填。

```sql
ALTER TABLE batch_sync_defs
    ADD COLUMN IF NOT EXISTS schedule_time_zone         TEXT        NOT NULL DEFAULT 'UTC',
    ADD COLUMN IF NOT EXISTS schedule_enabled           BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS schedule_revision          INT         NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS schedule_next_fire_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS schedule_last_fire_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS schedule_last_tick_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS schedule_last_tick_outcome TEXT;

CREATE INDEX IF NOT EXISTS idx_batch_sync_defs_schedule_due
    ON batch_sync_defs (schedule_next_fire_at)
    WHERE schedule_enabled;

CREATE TABLE IF NOT EXISTS sync_schedule_ticks (
    id                UUID PRIMARY KEY,
    sync_def_id       UUID NOT NULL REFERENCES batch_sync_defs(id) ON DELETE CASCADE,
    schedule_revision INT  NOT NULL,
    scheduled_for     TIMESTAMPTZ NOT NULL,
    claimed_at        TIMESTAMPTZ NOT NULL,
    outcome           TEXT NOT NULL,
    run_id            UUID REFERENCES sync_runs(id),
    actor             UUID,
    reason            TEXT,
    detail            TEXT,                    -- 有界 ≤256，脱敏
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT sync_schedule_ticks_slot_uniq
        UNIQUE (sync_def_id, schedule_revision, scheduled_for),
    CONSTRAINT sync_schedule_ticks_detail_bound CHECK (detail IS NULL OR char_length(detail) <= 256)
);
CREATE INDEX IF NOT EXISTS idx_sync_schedule_ticks_def_time
    ON sync_schedule_ticks (sync_def_id, created_at DESC);

CREATE TABLE IF NOT EXISTS sync_schedule_grants (
    sync_def_id       UUID PRIMARY KEY REFERENCES batch_sync_defs(id) ON DELETE CASCADE,
    grant_id          UUID NOT NULL,           -- 每 arm 全新随机
    schedule_revision INT  NOT NULL,
    boot_id           UUID NOT NULL,           -- 独立 schedule boot UUID（非 leaseHolder）
    actor_subject     UUID NOT NULL,
    org_id            UUID,                    -- 可信 tenant 快照（NULL=个人域）
    owner_subject     UUID NOT NULL,
    source_id         UUID NOT NULL,           -- 期望绑定
    output_dataset_id UUID NOT NULL,           -- 期望绑定（预检 CAS 对象）
    armed_at          TIMESTAMPTZ NOT NULL,
    expires_at        TIMESTAMPTZ NOT NULL,    -- min(已验证 JWT exp, armed_at+TTL)；仅 arm 可写
    last_seen_at      TIMESTAMPTZ NOT NULL
);
```

`sync_runs` 仍零改动。存量 `schedule_cron` 行 → disabled + NULL 游标（列默认即达成，不 UPDATE）。

## 6. Claim 协议（v2 关键修正点加粗）

锁序 pin（增量于 P2.3，无逆序）：
```
claim tx: Source FOR UPDATE **NOWAIT（调度侧一律，含 policy-off）**
          → Def FOR UPDATE（锁后重读 schedule/revision/游标 + fresh clock_timestamp()）
          → [准入/裁定] → run INSERT → 游标/tick/prune
          → advanceSyncLastAdmitted（成功路径最后一条 DB 语句）
arm tx:   Def FOR UPDATE →（校验 expected revision+output 未变）→ grant UPSERT（不改游标/revision）
revoke tx:Def FOR UPDATE → DELETE grant 行
心跳:     仅 UPDATE last_seen_at（generation 精确匹配 + DB 时钟 + 既有 expiry/新鲜度仍有效）
```
无环论证同 v1（arm/revoke 与 UpdateSyncJob 同为 Def-only；claim 先 Source）。

单候选短 tx 步骤：
1. 无锁候选扫描（keyset/轮转：进程内游标 `(next_fire_at, id)` 续读，短页回卷——公平推进；`now` 仅提示）。
2. 每候选：本地 grant 查询（generation 快照）→ coordinator Reserve（nil ⇒ local_full 计数跳过，无 DB 写）→ claim：
   a. Source NOWAIT：ErrNoRows ⇒ not_visible 干净跳过；**55P03 ⇒ 事务已 abort，有界回滚，contention 计数，无任何行/游标/stamp**。
   b. Def FOR UPDATE：无行 ⇒ not_visible；有行 ⇒ 锁后重读 revision/游标/enabled/cron/tz。
   c. `dbNow := clock_timestamp()`（锁后，权威）。
   d. **候选 CAS**：`d.schedule_revision != cand.Revision` 或 `d.schedule_next_fire_at IS DISTINCT FROM cand.ScheduledFor` ⇒ stale_candidate 干净拒绝（回滚，无行）；再判 due：`enabled ∧ cursor 非空 ∧ cursor <= dbNow`，否则 not_due 干净拒绝——**注入的未来 now 不可能提前触发**（dbNow 才算数）。
   e. arm 裁决（锁后读 grant 行，dbNow）：
      `liveOwner := 行存在 ∧ 行.revision = d.revision ∧ 行.expires_at > dbNow ∧ 行.last_seen_at > dbNow-90s`
      - 本地 arm 存在且与 DB 行 **全 generation 匹配**（grant_id/boot/revision/actor/tenant/source/output）⇒ armed 路径 f；
      - ¬liveOwner ⇒ 任何实例记 `auth_required` tick（reason 细分 never_armed|expired|revoked|stale_revision|owner_stale|window_too_short）+ 推进游标，无 run；本地陈旧副本同时丢弃；
      - liveOwner 属他机/本机无匹配副本 ⇒ remote_owner 计数跳过（不抢槽）。
   f. **cron 解析/校验先于 run**：锁后以 DB 行的 cron/tz 解析（5 字段 Unix5 / 6 字段 Quartz6，长度 ≤256）；失败 ⇒ `error_invalid_schedule` tick + 游标置 NULL 挂起待修（无 run）。
   g. `admissionGate` 复用：typed capacity/interval ⇒ `skipped_admission_capacity` / `skipped_admission_min_interval` tick + 游标推进 + COMMIT（**不调 advanceSyncLastAdmitted**）。
   h. 通过 ⇒ `insertPendingSyncRun`（与 EnqueueSyncRun 共享助手；requested_by=arm.actor；**tenant=arm 捕获的可信 caller tenant**，父链在锁后以该 tenant 复检过）→ `next := NextFireAfter(解析结果, dbNow)` → UPDATE 游标列 + last_fire/last_tick/outcome → INSERT tick(`enqueued`, run_id, actor) → prune（>500）→ **`advanceSyncLastAdmitted` 最后** → COMMIT。tick 唯一键冲突仅匹配 `sync_schedule_ticks_slot_uniq`，命中 ⇒ 回滚跳过（防御纵深）。
3. 提交后（仅 enqueued）：`DispatchScheduled(..., deadlineCap=arm.expiresAt)`；失败（停机竞态）⇒ 有界 `RequestCancelSyncRun` 关闭 + 独立小 tx 按唯一键补记 tick=`dispatch_failed`（detail=真实 aftermath，≤256）。
4. 非争用跳过原因（contention/local_full/remote_owner/stale_candidate/not_due/not_visible）只计数+日志，无行。

维护循环（独立于调度开关与 due 工作）：每 30s 清扫过期本地凭据 + 对每个本地活跃 generation 做精确匹配心跳（0 行 ⇒ 丢弃本地该 generation）。

## 7. 授权模型 v2

- 本机 registry 条目 = `grant_id/boot/revision/actor/tenant/source/output/authorization(秘密)/expiresAt(绝对)`；上限 1024，**DB arm 前先占本机容量**（stage），DB commit 后 activate（activate 前复核 DB 行 grant_id 仍为己方——迟到旧 arm 不得覆盖新成功者；失败/迟到只清自己的 generation）。
- arm HTTP 流：claims → def 可见(tenant) → Source 可见+canWriteSource → 读 (revision, output) → `precheckOutputDataset(output, auth)`（HTTP，无锁）→ arm tx：Def FOR UPDATE 复核 revision+output 与预检所见一致（不一致 ⇒ 409 `arm_target_changed` 干净重试）→ UPSERT grant 行（新 grant_id、绝对 expiresAt=min(JWT exp, arm+TTL)、保留 def 游标/revision 不动）→ commit → activate → 200（含 grant_id、expires_at、`scheduler_loop_running`——循环关闭时明示不承诺执行）。
- Stop 语义：先关 arm 准入/激活 gate → 清全部凭据 → 有界 join 维护；Stop 后到达的 DB arm 结果不得重填 JWT 或报 armed=true。
- 撤销/配置变更只影响未来 tick；已入列 Run 走既有 cancel/publish-gate API。
- 派生任务截止：`min(Reserve 锚定 AuthMemoryTTL/TaskDeadline, grant.expiresAt)`（coordinator 新增可选 deadline cap；排队消耗同一预算；手工路径不变）。过期后不新起 pull/预检/发布门；已进门按 P22 契约收尾。
- 安全不变量同 v1 §7.3（JWT 仅本机内存、不代签、不持久化、发布身份=actor JWT、revision 失效、重启后 auth_required）+ v2 独立 boot UUID、generation 匹配。

## 8. Tick 结果与可观察性 v2

| outcome | 含义 | run_id | 游标 | 准入 stamp |
|---|---|---|---|---|
| `enqueued` | run 已持久入列（**不证明已派发**） | ✓ | 推进 | 消耗 |
| `dispatch_failed` | 派发已知失败，已按真实 aftermath 补记 | ✓ | 已推进（补记不改） | 已消耗 |
| `auth_required` | 无活 owner（reason 细分） | 空 | 推进 | 不消耗 |
| `skipped_admission_capacity` / `skipped_admission_min_interval` | P2.3 typed 拒绝 | 空 | 推进 | 不消耗 |
| `error_invalid_schedule` | 锁后解析失败（仅 OOB 可达） | 空 | **置 NULL 挂起待修** | — |

行外原因聚合计数：contention / local_full / remote_owner / stale_candidate / not_due / not_visible。tick DTO 无私有字段；detail ≤256 且禁 JWT/DSN/URL query。

## 9. API v2（要点）

- 路由族保持 v1（POST/DELETE `schedule:arm`、GET `schedule/ticks`、syncs CRUD 扩字段），与相邻 Sync 路由同租户/父链/权限；稳定错误码 `invalid_schedule`。
- 校验：显式提供的非空 cron/tz 一律校验（含 disabled）；空 cron 显式清除并停用（bump revision）；省略保持；enable 需合法非空 cron；flavor 按字段数推断；长度 ≤256。
- GET 投影：`schedule_authorization: {db_grant: {grant_id, actor_subject, boot_id, armed_at, expires_at, last_seen_at, live}, locally_executable: bool}`——诚实区分 DB 元数据（含他机 owner）与本机可执行性；重启后未经证实的 owner 不标为可执行。
- arm 200 响应含 `scheduler_loop_running`。

## 10. 配置 v2

| 项 | 值 |
|---|---|
| `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS`（既有） | 默认 0=off；显式启用限 **1–30**（越界=启动错误） |
| `OPENFOUNDRY_SYNC_SCHEDULER_AUTH_TTL_SECS`（新） | 默认 **300**；显式范围 1–86400（越界=启动错误）；实际截止再与已验证 JWT exp 取 min |
| 常量 | registry 1024 / batch 8 / tick 保留 500（≈8h20m @1/min）/ 新鲜度 90s / 维护间隔 30s / cron 长度 256 / detail 256 |

## 11–16

Q1–Q9 回答、验收矩阵、责任文件、阻塞项、风险、确信度同 v1 结构，以本 v2 修正为准；实现级精确签名见 `implementation-contract.md`（唯一对齐源）。阻塞项：无。开放项仅剩 root 已授权默认值的落地核对。
