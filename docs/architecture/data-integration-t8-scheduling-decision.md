# T8 调度实现工程合同（WP3.2 实施基线）— v2

- **裁决记录**：用户 2026-11-17 明确同意「以 ADR-0037 的 CronJob→Kafka 为基线补齐接线」，授权覆盖
  本模块（DI-03 管道调度）WP3.2 实施。**范围限定**：本文件是 WP3.1
  [评估报告](data-integration-wp3-1-t8-scheduling-assessment.md) §4-A 方案的本模块工程合同；
  不构成全平台 Automate 统一方案或任何产品批准，不改动 ADR 正文。
- **编制**：GLM-5.3（WP3.2 主 worker），2026-11-17；v2 按 Codex 共享合同首轮 review 8 项修正
  （grant 代次严格==、BuildID/publish key 含 revision、活动占位不可被新机会抢占、receipts 存原始
  bytes、>=5 终止+毒消息身份+DLQ outbox、结果独立 topic、迁移所有权归主 worker、准入事务内原子
  建 build）；**v2.1 按闭环追加 review 3 项**：⑨ outcome 事件一次性语义（仅确认终态发一次，event_id
  含真实 definition_revision）+ publish key 定长 `"sch:"+UUIDv5`（满足 DVS `^[A-Za-z0-9._~:=-]{1,200}$`
  且不拼原始长字段）；⑩ canonical 预开 reservation txn 与 keyed commit 的 **branch 单 OPEN 冲突
  衔接协议**（§4）；⑪ 恢复成功判定不得单凭 receipts 集合（§4）；**⑫ 租户域贯穿（S1 兼容）**：
  `tenant_kind enum(org,personal)` 贯穿 grants/definitions/dispatches/activity/audit 的键与事件
  payload/aggregateID/eventID（`org:<uuid>/<schedule>` 或 `personal:<uuid>/<schedule>`，同 UUID 不同
  kind 互不混域），API 从已验证 JWT 推导域、请求体不得选域；blocked 定稿为 Forbid 语义终态跳过
  （不自动重放）。接口变更须同步本文件。
  **`libs/schedule-events` 公开 API 已冻结供 producer worker（B）编译开工**。
  工具链声明：本仓 `go.mod` 声明 `go 1.25.0`；本机无 go，容器内
  `openfoundry-platform-p1-q3v9-unit-tests` 镜像 toolchain 为 **go1.26.3 linux/arm64**（向后兼容编译
  go1.25 module），**不升级 go.mod 与依赖版本**。
- **上游决策**：ADR-0037/0038（Accepted）；WP3.1 评估 v3（Codex 最终核对通过）。

## 1. 架构与所有权

```
[K8s CronJob 每分钟]                       （B：CronJob 模板/部署，隔离临时集群验证）
   └─ schedules-tick CLI（单次）            （B：libs/event-scheduler tick 改造）
        PG 事务①（pipeline 库）：认领 schedules.definitions（dispatch_mode='pipeline_outbox_v1'）
          + advance next_run_at + INSERT scheduling.dispatch_audit
          + outbox.Enqueue(BuildTriggerEnvelope(...))  —— 同一事务
        [Debezium: outbox.events → Kafka]   （B：同源 CDC 接线；TopicTriggers=pipeline.builds.v1）
   └─ pipeline-build-service 调度消费方      （A：pbs 核心，本文 §4）
        PG 事务②（准入，单一 Tx）：dispatch 唯一 + grant 严格核验 + 活动 slot
          + OpenBuildTx（Tx 内原子建 build 行，不复用 pool 级 OpenBuild）
        事务外执行：Preflight→ResolveBuild→PersistResolvedBuild→BUILD_RUNNING
          →Plans.LoadPlan→executor.Execute（解密 caller token 注入 context）
        PG 事务③（收尾）：build 终态 + dispatch 终态 + outbox.Enqueue(outcome)
          → TopicOutcomes=pipeline.schedule.outcomes.v1（B 负责预置该 topic+DLQ+schema 注册）
```

| 资产 | 所有权 |
| --- | --- |
| 本文件、`libs/schedule-events`（冻结 API）、**`services/pipeline-build-service/**` 全部（含全部 migrations：definitions 绑定列、dispatch_audit、outbox DDL、scheduling.* 、定义 CRUD/登记 API、grant API、消费方核心、测试）** | **A = GLM 主 worker（本 worker）** |
| `libs/event-scheduler/**`（tick/advance/envelope 构造、坏定义隔离）+ `schedules-tick` CLI + `infra/**`（CronJob、Debezium、topic/DLQ 预置与 schema 注册）+ 验收脚本 | **B = producer/infra worker** |
| 代码 review 与验收 | Codex |

冲突规则：B 不改 `services/`、不改 `libs/schedule-events`；A 不改 `libs/event-scheduler`、`infra/**`。
两 worker 不得同时在 pipeline migrations 目录写文件（全部归 A）。

## 2. 事件合同（`libs/schedule-events`，A 所有，**已冻结**）

- **Topic 拆分**（防自消费循环）：`TopicTriggers = "pipeline.builds.v1"`（已预置）；
  `TopicOutcomes = "pipeline.schedule.outcomes.v1"`（**B 预置** topic+`__dlq.pipeline.schedule.outcomes.v1`
  并注册两 schema 工件，工件由本包 embed 提供）。常量与 schema 文件为冻结 wire 契约。
- **事件类型**：`pipeline.schedule.triggered.v1`（触发）/`pipeline.schedule.outcome.v1`（结果）。
- **Envelope**：ADR-0038 八字段；`aggregate_type="pipeline_schedule"`；
  `aggregate_id="<tenant_kind>:<tenant_uuid>/<schedule_key>"`（`org:<uuid>/…` 或 `personal:<uuid>/…`，
  **同 UUID 不同 kind 是两个不同租户，aggregate/eventID/buildID/pubKey 全链隔离**）；
  `version`（event_id 输入）= `<definition_revision>@<scheduled_for>`，时间一律 **显式 UTC 固定纳秒格式**
  `2006-01-02T15:04:05.000000000Z`（包内 `formatScheduledFor`，不用 RFC3339Nano 去零变体）。
- **`event_id` = UUIDv5(OID, `\x1f`-join(aggregate_type, aggregate_id, version, event_type))**——
  producer/consumer 同源复算，consumer 拒不匹配（防伪造）。与库内 OpenLineage run_id 输入域不相交。
- **租户域类型（冻结）**：`TenantKind`（`org`/`personal`）与 `TenantRef{Kind, ID}`；
  `AggregateID(tenant TenantRef, scheduleKey string)`；`ParseTenantRef("kind:uuid")`。
- **TriggerPayload**：`tenant_kind`、`tenant_id`、`schedule_key`、`definition_revision`、`scheduled_for`、
  `pipeline_rid`、`build_branch`、`grant_ref UUID`、`grant_generation int`。
  **`grant_generation` 是严格相等核对项（==）**：consumer 以 DB 中 grant 的当前 generation 精确比对，
  重授（generation+1）即令一切携带旧代次的排队/重放事件失效。事件声明不被信任——consumer 以
  DB 定义绑定与授权行核 tenant_kind/tenant_id/pipeline/subject/revision，并重验所存 JWT
  （签名/exp/**域匹配**：org 域事件必须由带 OrgID 票的 token 执行、personal 域事件必须无 Org 票；
  同 sub 切换组织不能复用个人授权）。
- **派生身份（防修订/跨域碰撞，均已冻结）**：
  `DeriveEventID(tenant TenantRef, scheduleKey string, revision int, scheduledFor time.Time, eventType string) uuid.UUID`；
  `DeriveBuildID(triggerEventID uuid.UUID) uuid.UUID`（**从 trigger event_id 派生**，revision+kind 自动内含）；
  `DerivePublishKey(triggerEventID uuid.UUID, nodeID string) string`——**定长** `"sch:"+UUIDv5(eventID,nodeID)`
  （≤40 字符，恒满足 DVS keyed key pattern；revision/kind 经 eventID 内含；不拼接原始长 schedule_key/node_id；
  重试必须逐字节复用同 key）。
- **Outcome 事件一次性语义（冻结）**：每个机会**仅在到达确认终态
  （succeeded/failed/skipped/blocked）时发布恰好一次**结果事件；`unknown_publish`/`reconcile` 是
  **内部可观察 dispatch 状态，不发事件**——恢复后的成功不得复用旧 event_id 配不同 payload。
  `OutcomePayload.definition_revision` 必填，outcome event_id 用**真实 revision** 派生（不同修订
  机会的结果 ID 不碰撞）。
- **公开 API（冻结）**：上述常量与三个 Derive 函数；`TriggerPayload`/`OutcomePayload`/`Envelope`；
  `BuildTriggerEnvelope(payload TriggerPayload, occurredAt time.Time, correlationID, causationID string)`；
  `ParseAndValidateTrigger(raw []byte) (*Envelope, *TriggerPayload, error)`（schema+event_id 重算+tuple 一致）；
  `BuildOutcomeEnvelope(...)`。Schema 校验双侧强制：embed JSON Schema（draft 2020-12，本包
  `schema/*.json`），producer 发布前、consumer 处理前各校验一次；**静态预填 envelope 不得跨机会重发**。

## 3. 共享 DB 结构（全部迁移文件归 A；pipeline 库，PG）

**B 依赖的契约表**（B 只读写 tick 相关行，不建迁移）：

`ALTER schedules.definitions`（A 的新迁移新增绑定列，全部可空默认以兼容旧行）：

| 列 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `dispatch_mode` | `TEXT` | `NULL` | `'pipeline_outbox_v1'`（新链）/ 其余（NULL、`'legacy_direct'`）= 旧语义 |
| `tenant_kind` | `TEXT` | NULL | `'org'`/`'personal'`，新链必填 |
| `tenant_id` | `UUID` | NULL | org=OrgID / personal=Sub，新链必填 |
| `schedule_key` | `TEXT` | NULL | tenant(kind,id) 内稳定键，新链必填 |
| `definition_revision` | `INT` | NULL | ≥1，每次定义修改 +1，新链必填 |
| `pipeline_rid` / `build_branch` | `TEXT` | NULL | 新链必填 |
| `grant_ref` | `UUID` | NULL | 指向 `scheduling.grants.id`，新链必填 |

唯一键含 kind：`schedules_definitions_mode_key_uidx (tenant_kind, tenant_id, schedule_key) WHERE
dispatch_mode='pipeline_outbox_v1'`；grants 活动唯一 `(tenant_kind, tenant_id, schedule_key) WHERE
state='active'`；activity 主键 `(tenant_kind, tenant_id, pipeline_rid)`；dispatches/dispatch_audit/
pipeline 索引均含 `tenant_kind`（迁移
[20261117100000](../../services/pipeline-build-service/migrations/20261117100000_scheduling_consumer.sql)）。

**grants 域结构（冻结）**：`tenant_kind`+`tenant_id`+`org_id UUID NULL`（org 域 `org_id=tenant_id`、
personal 域 `org_id IS NULL AND tenant_id=subject`，表级 CHECK 强制）+`subject UUID`（零 sub 拒绝入库）。
**授权域推导（冻结）**：API 从**已验证 JWT** 推导——claims 有 `OrgID` → `(org, OrgID)`；无 →
`(personal, Sub)`；**请求体不得选域**。执行上下文映射沿用
[tenancy.go:18-34](../../services/pipeline-build-service/internal/postgres/tenancy.go)：
org → `TenantScope{TenantID:&org, Subject:sub}`；personal → `TenantScope{TenantID:nil, Subject:sub}`
（**不得把 personal Sub 填入 TenantID 指针当 org**）。个人域即便 admin 票也仅允许 owner==本人；
组织域 owner/admin 先限定本资源域。

**双发排除规则（冻结）**：旧 Tick（B 仓库现行为）`WHERE dispatch_mode IS DISTINCT FROM
'pipeline_outbox_v1'`；新 Tick 仅 `WHERE dispatch_mode = 'pipeline_outbox_v1' AND enabled AND
next_run_at <= $1`。两 CLI 不同时启用同一行；升级窗口内旧/新 CronJob 并存也互不重复发。

**坏定义隔离（冻结，`schedules.definitions.quarantined_at/quarantine_reason` 两列由 A 的迁移新增、
B 的 tick 写入）**：仅**永久性**坏行（cron 表达式坏/payload 不过本地 schema）→ B 在**同一事务**内置
`enabled=false`+`quarantined_at=now()`+**脱敏** `quarantine_reason`，tick 继续处理其余 due 行；
**临时性**失败（Registry 不可达/认证失败/版本未就绪）→ 保留 due 重试，**不 quarantine**。
定义经校验更新或显式恢复操作清除两列。B 不改迁移文件（列由本合同 A 侧提供）。

`scheduling.dispatch_audit`（B 的事务①写入的永久机会记录；outbox 行即删，此表为审计源）：
`event_id UUID PK`、`definition_id UUID`、`tenant_id`、`schedule_key`、`definition_revision INT`、
`scheduled_for TIMESTAMPTZ NOT NULL`、`fired_at TIMESTAMPTZ NOT NULL DEFAULT now()`。

`outbox.events`（标准 DDL，与 libs/outbox 依赖一致，A 的迁移负责 `IF NOT EXISTS`）：B 事务①与
A 事务③共用；`outbox.Enqueue` 语义=同事务 INSERT+DELETE（WAL/CDC 转发），**不得轮询该表、
不得以其 PK 做永久去重**。

**A 的消费方表**（`20261117100000_scheduling_consumer.sql`，A 独家写入）：

- `scheduling.dispatches`（每机会准入，`event_id UUID PK`）：`tenant_id UUID NOT NULL`、
  `schedule_key TEXT NOT NULL`、`definition_revision INT NOT NULL`、`scheduled_for TIMESTAMPTZ NOT NULL`、
  `pipeline_rid TEXT NOT NULL`、`build_branch TEXT NOT NULL`、`grant_ref UUID NOT NULL`、
  `grant_generation INT NOT NULL`、`build_id UUID NOT NULL UNIQUE`、
  `state TEXT NOT NULL CHECK IN ('admitted','executing','succeeded','failed','skipped','blocked','unknown_publish','reconcile')`、
  `attempts INT NOT NULL DEFAULT 0`（**跨重启持久的事件处理失败计数**）、
  `lease_owner TEXT NULL`、`fence_token BIGINT NOT NULL DEFAULT 0`、`lease_expires_at TIMESTAMPTZ NULL`、
  `error TEXT NULL`、`created_at/updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`。
  部分唯一索引 `ON dispatches (event_id)`（PK 即每机会唯一）；
  `(tenant_kind, tenant_id, pipeline_rid, scheduled_for)`
  普通索引供对账。
- `scheduling.pipeline_activity`（跨周期不重叠）：`(tenant_kind, tenant_id, pipeline_rid)` 复合 PK、
  `active_dispatch_id UUID NOT NULL`、`fence_token BIGINT NOT NULL`、`heartbeat_at TIMESTAMPTZ`、
  `blocked_reason TEXT NULL`。**接管规则（冻结）**：行存在且 active dispatch 非终态时，新机会按
  **Forbid 语义终态跳过**（dispatch=`blocked`，一次性 outcome 事件、**后续不自动重放**——blocked
  是确认终态而非可恢复状态）；lease 过期**只允许原 dispatch 的恢复流程接管**
  （fence 校验后继续/收尾），不得让新机会顶替占位；`unknown_publish`/`reconcile`/存在未确认远端
  操作时**必须保留活动占位且不发终态 outcome**；仅当 active dispatch 终态且外部发布已确认
  （receipts 全 committed/安全终止）才删除占位行。
- `scheduling.grants`：`id UUID PK`、`tenant_kind TEXT NOT NULL CHECK('org','personal')`、
  `tenant_id UUID NOT NULL`（org=OrgID/personal=Sub）、`org_id UUID NULL`（org 域=tenant_id，
  personal 域 NULL 且 tenant_id=subject，表级 CHECK 强制）、`subject UUID NOT NULL`（零 sub 拒绝）、
  `schedule_key TEXT NOT NULL`、
  `pipeline_rid TEXT NOT NULL`、`generation INT NOT NULL`（新 grant=1，重授=旧行置 revoked+新行+1）、
  `state TEXT NOT NULL CHECK IN ('active','suspended','revoked','expired')`、
  `expires_at TIMESTAMPTZ NOT NULL`（=所存 JWT 的 exp，**无 exp 的 JWT 拒绝入库**）、
  `token_ciphertext BYTEA NOT NULL`（AES-256-GCM；密钥=独立 `SCHEDULER_CREDENTIAL_ENCRYPTION_KEY`
  32B，仅消费方启用时要求，禁止 JWT secret 派生，错误脱敏）、活动唯一
  `(tenant_kind, tenant_id, schedule_key) WHERE state='active'`（部分唯一索引）、审计列。
  **入库校验（冻结）**：JWT 解析后 sub 非空、exp
  存在且未过期、org/tenant 匹配，否则拒绝授予。
- `scheduling.publish_receipts`：`key TEXT PK`（=`DerivePublishKey`）、`dispatch_event_id UUID`、
  `node_id TEXT`、`dataset_rid TEXT`、`request_body BYTEA NOT NULL`（**首次请求原始字节**）、
  `request_sha256 TEXT NOT NULL`（fingerprint）、`state TEXT CHECK IN ('pending','committed','unknown')`、
  `receipt JSONB NULL`（usableACK：`Status=="COMMITTED"` 且 `DatasetID` 匹配，参照 Connector
  runtime/sync.go:103-113 语义）、时间戳。**pending 行必须在首次 POST 之前持久**（先写库再发请求）；
  DVS 回执 404 **不证明无在飞操作**——只允许同 key 原字节重放或对账，禁止换 key 重跑节点。
- `scheduling.poison_messages`：`topic TEXT, partition INT, partition_offset BIGINT` 复合 PK、
  `first_seen_at`、`last_attempt_at`、`attempts INT NOT NULL DEFAULT 0`、
  `last_error TEXT`、`state TEXT CHECK IN ('counting','dlq_enqueued','dlq_published')`——
  **毒消息身份按 topic/partition/offset 稳定记数**（坏 JSON/非法 event_id 同样计入），
  `attempts >= 5` 即终止并走 DLQ。

**遗留表不混用**：Rust 时代 `schedules`/`schedule_runs`/`schedule_scope`/`service_principals`
（20260504000080-082）保持遗留只读；`schedules.definitions` 仅按上表新增列。

## 4. 消费方行为合同（A 实现，pbs）

- **准入（单一 PG 事务）**：`INSERT dispatches ON CONFLICT(event_id) DO NOTHING` → 已存在则按
  state 分流（终态=skip；`unknown_publish`/`reconcile`=进恢复，不盲重放）；grant 核验=以 DB 当前
  行（state=active、`generation == 事件 grant_generation` **严格相等**、未过期、
  **tenant_kind+tenant_id**/pipeline/subject/definition_revision 逐项匹配）+ 解密 JWT 重验
  （签名/exp/**域匹配**：`org` 域事件须由带匹配 OrgID 票的 token 执行、`personal` 域事件必须
  **无** Org 票且 sub==tenant_id——同 sub 切组织不能复用个人授权，同 UUID 不同 kind 互不认）；
  任一不过 → dispatch `skipped`+原因
  （**不执行**，撤权后排队/重放事件不凭旧代次执行）。活动约束按 §3 接管规则（冲突→`blocked`
  **Forbid 语义终态跳过，不自动重放**）。
  **build 行创建用事务内 `OpenBuildTx(ctx, tx, …)`**（确定性 `DeriveBuildID(triggerEventID)`、
  `trigger_kind='SCHEDULED'`、显式 branch、`ON CONFLICT DO NOTHING`）——不得在准入事务内调用持
  pool 另起事务的旧 `Repository.OpenBuild`。事务提交后该事件才算消费成功（与 processed 语义合一，
  无独立 record-before-process）。
- **执行（事务外）**：`resolver.PreflightBuild → OpenBuild(幂等重入) → resolver.ResolveBuild →
  PersistResolvedBuild`（canonical 函数族进程内直调）→ `BUILD_RUNNING`+`started_at` →
  `Plans.LoadPlan` → `executor.Execute`（context 注入解密 caller bearer）。Preflight/Resolve 打开
  的远端输出事务的 crash 恢复**必须走持久意图**：沿用既有
  [build_pending_output_txns](../../services/pipeline-build-service/migrations/20261113110001_build_pending_output_txns.sql)
  + `PendingTxnRecorder`/`CleanupOpenedTransactions` 机制对账清收，不得盲开新事务或重建重放。
  绝不复活 run-due、不写 `pipeline_runs`。
- **租约/心跳/恢复**：执行前 claim（`fence_token+1`、lease TTL）；心跳 `UPDATE … WHERE fence_token=$`；
  **恢复成功判定（⑪，冻结）**：`executing` 且 lease 过期时，「receipts 全 committed」**单独不构成
  成功**——必须同时满足：全部必需 job/node 执行成功（对照 `builds`/`jobs` 真实状态与
  `job_outputs` 完整性）、持久结果完整、无未决 pending/unknown receipt（**空 receipts 集对未执行
  build 不成立**，因 LoadPlan 从 Waiting 重建、不能以 receipt 空集推成功）；任一不满足或有已执行
  副作用/未知结果 → `unknown_publish` 留 reconcile，**不盲重跑**。失败保旧按 S1 单 Dataset 原子性
  口径，**不宣称跨 Dataset 多输出原子发布**（WP3.8 另行）；`admitted` 未执行按 lease 恢复续跑。
- **canonical 预开事务与 keyed commit 的衔接协议（⑩，冻结；不改 DVS 公共协议）**：
  [resolver.go:503-565](../../services/pipeline-build-service/internal/domain/resolver/resolver.go)
  在 Resolve 期为每个输出 `OpenTransaction`（reservation txn，OPEN 状态）+`RecordPendingTxn`+
  `locks.TryAcquire`；DVS keyed commit（[output_commit.go:421](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）
  会再 `StartTransaction`，而 DVS 同 branch 仅允许一个 OPEN
  （[20260501000001_versioning_init.sql:128-130](../../services/dataset-versioning-service/internal/repo/migrations/20260501000001_versioning_init.sql)
  `uq_dataset_transactions_one_open_per_branch`）——直接串接首个输出即冲突。调度路径的
  scheduled-only 衔接：
  1. 节点首个 keyed commit **之前**，先对该 dataset/branch 的 reservation txn 做**安全移交**：
     请求 abort 并取得**终态确认**（abort 成功、明确的 already-aborted，或 NotFound 且我方持有该
     txn 的 `build_pending_output_txns` 持久记录）；移交期间 pipeline 输出锁（TryAcquire 绑定）与
     调度活动占位**保持不释放**；
  2. abort **未获确认**（失败/超时/结果不明）→ 不得继续发布，dispatch 置 `unknown_publish` 留
     reconcile（同 branch 单 OPEN 亦会阻止后续 keyed commit，天然 fail-closed）；
  3. 重复 handoff 必须幂等（再次移交已终态 txn 按 1 的确认集合处理，无新副作用）；
  4. 成功路径：`job_outputs.transaction_rid` 记录 keyed commit 返回的**真实 COMMITTED txn**
     （usableACK 校验对象），原 reservation txn RID 另存 provenance（receipt/dispatch 元数据）；
  5. Resolver 重进**不得再开新输出 txn/锁**（对账已持久 pending 意图）；恢复一律用已持久
     意图/引用。手动路径不经此协议（无 key、无移交）。
- **重试/DLQ**：事件处理失败（含坏 JSON/非法 event_id）计入 `dispatches.attempts`（跨重启）与
  `poison_messages`（按 topic/partition/offset）；**`attempts >= 5` 即终止**（第 5 次失败后不再
  重试）；DLQ 发布作为 **outbox 意图**（事务③同源）落库、意图提交成功后才 ack 该 offset；
  offset 用 `CommitMessages` 显式提交，失败不提交、不越过同分区未解决失败的 offset（`CommitOffsets`
  是 no-op 不用）。业务失败（executor outcome failed/aborted）只落终态+outcome 事件，不自动重跑。
- **发布（事实修正 + scheduled-only 增强）**：`git show f4c6194c` 核实——pbs
  [dataset_output_committer.go](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)
  **当前没有 keyed 请求侧**（仅 body→Marshal→POST+2xx；keyed 校验在 DVS 服务端
  [output_commit.go:25-30](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。
  A 的新增：调度路径输出提交带 `idempotency_key=DerivePublishKey(...)`（DVS keyed 协议）、
  `publish_receipts` 先持久原始请求再 POST、回执 usableACK 校验、unknown→reconcile fail-closed；
  **手动路径行为不变**（不加 key、不写 receipts）。
- **结果事件**：事务③内 `outbox.Enqueue(BuildOutcomeEnvelope(...))` → `TopicOutcomes`；终态+outbox
  不覆盖 DVS 外部提交，外部确认以 receipts 为准。
- **配置（默认全关）**：`SCHEDULER_CONSUMER_ENABLED=false`（启用时强制
  `SCHEDULER_CREDENTIAL_ENCRYPTION_KEY` 32B、`SCHEDULER_KAFKA_BOOTSTRAP_SERVERS`）、
  `SCHEDULER_GROUP_ID`（默认 `pipeline-build-service-scheduler`）、`SCHEDULER_LEASE_TTL`/
  `SCHEDULER_HEARTBEAT_INTERVAL`/`SCHEDULER_MAX_EVENT_ATTEMPTS=5`。
- **定义登记/授权 API（A 实现，消除「无人登记」）**：同事务写调度源行+definitions 绑定列+授权
  修订——`POST /api/v1/scheduling/definitions`（启用调度：pipeline+branch+cron+grant → 写绑定列、
  `dispatch_mode='pipeline_outbox_v1'`、`definition_revision+1`）、
  `PATCH /api/v1/scheduling/definitions/{key}`（暂停/恢复/改 cron → enabled/revision）、
  `DELETE /api/v1/scheduling/definitions/{key}`（停用：`dispatch_mode=NULL` 或删行，按裁决）；
  `POST/DELETE /api/v1/scheduling/grants`（授予/撤销；撤销=state+新代次）。
  **授权域一律从已验证 JWT 推导**（claims 有 OrgID→`(org, OrgID)`，无→`(personal, Sub)`，
  **请求体不得选域**）；组织域 owner/admin 限本资源域；**个人域即便 admin 票也仅允许
  owner==本人**。执行上下文映射按 §3 tenancy.go 规则（personal 不得把 Sub 填 TenantID 指针）。
  所有路径 JWT+TenantScope 保护，权限模型与既有 builds 路由一致。

## 5. 验收与边界

- 验收口径=WP3.1 评估 §6 V1–V8；代码+单测通过**不等于**真实验收（需真实 PG+Kafka/CDC+JWT+隔离
  临时集群，资源前缀 `ofwp32-*`，B 负责；不可用如实上报，不手工 run-due 冒充）。
- 跨周期不重叠与每机会去重**分别**测试（合同 §3 两个独立机制）；V2 按裁定漏期策略恢复。
- 租户域贯穿测试（⑫）：相同 UUID 不同 kind 全链隔离（aggregate/eventID/buildID/grant 唯一键）；
  同 sub 切组织后个人授权不可复用（org 事件必须由带 OrgID 票的 token 执行）；个人域跨 owner
  拒绝（即便 admin 票）。M1 已落 libs 层身份隔离测试（`TestTenantKindDomainIsolation` 等），
  grant/consumer 层测试随实现补。
- 生产 K8s/DB/卷不动；真实 `.env` 不读不改；新功能默认关闭；本包不提交不推送（按指令）。
- 不回退已交付的 4 份 WP3.1 文档。

## 6. 冻结配置面（M3，供 B/C 直接接线；变更须过本文件）

**Env（A 所有 `internal/config`，单位=秒，启用时非法数字 fail-closed）**：

| Env | 默认 | 冻结语义 |
| --- | --- | --- |
| `SCHEDULER_CONSUMER_ENABLED` | `false` | 仅字面 `true` 启用；关闭时一切调度入口惰性（grant 创建 503） |
| `SCHEDULER_CREDENTIAL_ENCRYPTION_KEY` | —（必填） | **base64 解码后恰 32 字节**（`openssl rand -base64 32`，沿 Connector 约定）；**拒绝等于 raw JWT_SECRET 或 legacy JWT-derived digest；无任何 JWT 派生/dev fallback**；错误不含 secret |
| `SCHEDULER_KAFKA_BOOTSTRAP_SERVERS` | —（必填） | 逗号分隔 broker 列表 |
| `SCHEDULER_GROUP_ID` | `pipeline-build-service-scheduler` | consumer group |
| `SCHEDULER_LEASE_TTL` | `120` | 执行租约 TTL（秒） |
| `SCHEDULER_HEARTBEAT_INTERVAL` | `30` | 心跳间隔（秒），必须 < LEASE_TTL |
| `SCHEDULER_MAX_EVENT_ATTEMPTS` | `5` | **1..5**（ADR-0038 消费重试预算） |
| `SCHEDULER_KAFKA_SECURITY_PROTOCOL` | **`SSL`** | `SSL`（mTLS，生产 9093 listener，C 挂载三文件）/`SASL_SSL`/`SASL_PLAINTEXT`/`PLAINTEXT`（显式 dev）；**未知协议拒绝启动，绝不降明文**；SSL 要求三文件齐、SASL_* 要求凭据成套 |
| `SCHEDULER_KAFKA_TLS_CA_FILE` | —（SSL 必填） | CA 文件路径（C：cluster CA） |
| `SCHEDULER_KAFKA_TLS_CERT_FILE` | —（SSL 必填） | 客户端证书（C：scoped KafkaUser `user.crt`） |
| `SCHEDULER_KAFKA_TLS_KEY_FILE` | —（SSL 必填） | 客户端私钥（C：`user.key`） |
| `SCHEDULER_KAFKA_SASL_USERNAME` / `_PASSWORD` | 空 | SASL 可选模式凭据（成套校验） |
| `SCHEDULER_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | `SCRAM-SHA-512`/`SCRAM-SHA-256`/`PLAIN`（枚举校验） |

**HTTP API（JWT+TenantScope 保护；域一律由已验证 JWT 推导，body 永不选域；个人域仅 owner 本人）**：

- `POST /api/v1/scheduling/grants` — body `{"schedule_key","pipeline_rid","token"}`（token=将作为后台执行身份的调用者 JWT；服务端用与 HTTP 同一验证器验签、与调用者域/主体比对后 AES-256-GCM 加密存储）→ `201 {"id","generation","expires_at"}`
- `DELETE /api/v1/scheduling/grants/{key}` → `{"status":"revoked"}`（重授=新 POST，generation+1）
- `POST /api/v1/scheduling/definitions` — body `{"schedule_key","pipeline_rid","build_branch","cron","grant_id","grant_generation"}` → `201 {"status":"enabled"}`（同事务：grant FOR SHARE 当前沿验+绑定列+`definition_revision`+1+清 quarantine）
- `PATCH /api/v1/scheduling/definitions/{key}/pause|resume` → `{"status":"paused|resumed"}`
- `DELETE /api/v1/scheduling/definitions/{key}` → `{"status":"disabled"}`（`dispatch_mode=NULL`）

**Consumer 行为冻结（M3 落地）**：offset 仅在「准入决定落库 / DLQ outbox 意图提交」后 `CommitMessages` 提交（`CommitOffsets` 为 no-op 不用）；毒消息按 `(topic,partition,offset)` 稳定身份计数（坏 JSON/非法 event_id 同计），`>=maxAttempts` → `outbox.Enqueue(__dlq.<topic>)`+poison 行 `dlq_enqueued` 同事务后才 ack；准入设施错误（DB 故障）不提交等重投；恢复扫描（`ExpireStaleExecutions`）覆盖 admitted-未claim/executing-lease过期/unknown_publish/reconcile，恢复只走原 dispatch（`ClaimDispatchForExecution`/`ReconcileDispatch`），绝不新开机会。

WP3.2 implementation correction: dataset output publish keys use `DeriveOutputPublishKey(eventID,nodeID,datasetRID)`: `sch:` plus domain-separated UUIDv5 over a JSON tuple. Receipt node_id remains the JobSpec RID. Retries preserve the key and original bytes. This supports several datasets per node with per-dataset atomicity only. The legacy DerivePublishKey API and event/schema bytes remain unchanged.

WP3.2 canonical implementation: admission creates deterministic Build in its fenced admission transaction. Runner performs Preflight → durable resolve intent → Resolve → fenced persist+seal+RUNNING → LoadPlan → Execute. It does not repeat OpenBuild. Confirmed terminal Build state, output lock release, activity and outcome changes are owned by fenced FinishDispatch; no preliminary pool MarkBuildFailed.
