# WP3.1 / T8：管道调度方案与接线差距评估（决策支持文档）

- **基线**：`f4c6194ca93d2899162887bbde10ff4126fdf618`（2026-11-17 11:23:47 +0800，已提交推送，工作区干净）
- **编制**：GLM-5.3（WP3.1 文档 worker），2026-11-17；**状态：v3——Codex 首轮 A–H 事实修正复核通过、二轮 6 项局部合同收紧已落盘；工程评估完成登记 2026-11-17（GLM 编制，Codex 两轮 review+最终局部条件复核），Codex 最终核对通过（2026-11-17）。机制/产品批准仍待裁、接线未验收（§0 三态）**。
- **责任边界**：本文只做源码盘点与方案比较，**不替产品/架构裁决 T8**，不开 WP3.2，不改 ADR，不实现任何代码。

> **状态注记（2026-11-17，裁决后追加；本文正文保留为历史评估快照，不再更新）**：
> 用户 2026-11-17 已选定 **§4-A（ADR-0037 CronJob→Kafka）为本模块（DI-03 管道调度）
> WP3.2 工程基线并授权实施**——就本模块而言，§0 三态中的第 2 态（机制获批）**已按用户
> 工程授权口径发生**（裁决记录见 [T8 决策记录](data-integration-t8-scheduling-decision.md)
> §裁决记录；§1〔仓外源〕的 Automate 统一方案产品边界约束原样保留，本文未代签产品/架构
> 扩展）。**WP3.2 工程实现与真实隔离验收完成（2026-11-17，本轮纳入分支提交；提交与远端状态以 Git 记录为准）**：用户已选 ADR-0037 CronJob→Kafka 本模块基线；GLM-5.3 初始实现，额度耗尽后 GPT-6.1 Sol 继续，Codex review。API/producer/CDC/Apicurio/canonical 产品及 V1–V8 均通过，见[隔离验收报告](data-integration-wp3-2-acceptance.md)、[决策记录](data-integration-t8-scheduling-decision.md)、[部署 profile](data-integration-wp3-2-deployment-profile.md)。非生产部署、默认 Off；生产 authorizer/DSN/Secret 等前置仍须满足。DI-03/B2 不整体关闭、manual F15 不全局关闭；T2 及其他 T 项、其它 WP 状态不变，WP3.3 不自动启动。历史 P0–P3.1 与原未决措辞保留为登记快照；T0 314–555 与各 WP 冻结估算、源版本均不变。
> §2/§3 中「T8 未裁决/待裁」等表述均为本文登记时点的历史快照；T2 及其他 T 项状态不变。

## 0. 三态声明与证据口径（防止状态混淆）

**三态声明**——本文交付物只对应第 1 态，严禁混同：

1. **评估工程完成** = 本文档通过 Codex review：证据盘点、方案比较、待裁决清单齐备且行号可核对。
   **不代表下述任何一种。**
2. **机制获批** = T8 裁决方（架构组+数据与接入组+平台运行时，Automate/产品范围边界处产品参与，
   见 §5-G）正式选定机制并满足批准门槛（§4 各方案「批准前置」）。截至今日 **T8 未裁决**。
3. **接线真实验收完成** = WP3.2 按裁决机制实施并通过 §6 验收（真实周期/重启/并发/身份/失败保旧）。
   **截至今日未发生，任何 README/注释的「完成」表述均不构成证据。**

**证据等级**：

- 〔源码〕= OpenFoundry 基线源码/迁移/helm 静态读证，行号由编制者独立核对（非转抄）。
- 〔ADR〕= 已批准决策文本原文。
- 〔计划〕= 本仓规划文档断言（以「行 N」标注行号）。
- 〔仓外源〕= 相邻仓固定 commit 的原文，已按 commit 读取核对。

**未执行声明**：本文未运行任何测试/服务/迁移，**未连接 DB/Kafka/集群**——因此凡涉及库表，只断言
「迁移定义存在」，**实际部署状态与存量数据未核**；任何接线切换前须先盘点实际库。不把 README
「完成」表述或表存在当接线证据；未触碰真实 `.env`、旧 results、DB、卷。

**T8 阻塞范围**：仅阻塞 B2 管道调度实现（WP3.2），不阻塞其他 WP、连接器或安全修复
（〔计划〕[实施计划 行504](business-platform-implementation-plan.md)、
[矩阵 行365](data-integration-requirements-coverage-matrix.md)）。
B2 其余开工门槛（Dataset 身份/版本/Schema/发布/权限契约冻结）已由 S1 工程契约 v1 满足
（[S1 契约 行3/行251](data-integration-s1-dataset-contract.md)）——T8 是 WP3.2 唯一剩余前置。

## 1. 现行约束（已批准决策框架，评估不得越界）

| 决策 | 状态 | 对管道调度的约束 |
| --- | --- | --- |
| [ADR-0037](adr/ADR-0037-foundry-pattern-orchestration.md) | Accepted 2026-05-04 | 时间触发=K8s `CronJob`→Kafka 事件（:70 对照表、:110-111）；**把 ADR-0025 的替代方向改为 CronJob→Kafka 而非 Temporal `ScheduleClient`**（:23-25）；outbox+Debezium+Kafka 是唯一跨服务协调总线（:117-119）；`event_id` UUID v5 + 消费方 `processed_events` 去重强制（:120-123）；Temporal/`workers-go` 移除（:132-135）。 |
| [ADR-0021](adr/ADR-0021-temporal-on-cassandra-go-workers.md) | **Superseded**（:10，被 ADR-0037 取代） | 不能再作为引入 Temporal 的现行依据；重引须新架构决策（§4-C）。 |
| [ADR-0025](adr/ADR-0025-eliminate-custom-scheduler.md) | Accepted 2026-05-02 | 删除自研 `pipeline-schedule-service`，当时定向 Temporal Schedules 为唯一调度机制（:102-110）；其替代机制已被 ADR-0037 改写。 |
| [ADR-0038](adr/ADR-0038-event-contract-and-idempotency.md) | Accepted 2026-05-04 | **每个事件在进入 outbox 或由任何非 outbox 生产者发布之前，MUST 经 Avro/JSON Schema 校验**（:43-45）；统一 envelope 8 字段（:49-58）；`event_id` 必须为确定性 UUID v5（:77 起）；消费方 `processed_events` 表（:94 起）；消费侧重试预算 5 次、耗尽进 `__dlq.<original-topic>`（:108-118）——该预算作用于**事件消费**，不自动等同管道业务失败重跑次数（§3 待定）。 |
| [ADR-0022](adr/ADR-0022-transactional-outbox-postgres-debezium.md) | Accepted | outbox→Debezium→Kafka 发布路径；ADR-0037 将其推广为编排基座（ADR-0037 关联声明 :16-19）。 |
| P2.4 限定例外 | 〔计划〕[矩阵 行60](data-integration-requirements-coverage-matrix.md) | 仅覆盖 Connector REST snapshot 同步链的 cron 调度，是 ADR-0025 的**限定例外**，明确不是通用调度框架，**不覆盖管道**；不能作为管道沿用进程内调度的先例。 |

**协作边界（Automate 源要求，〔仓外源〕已核）**：相邻仓
[../../../palantir-research/knowledge-base/_index/裁剪标签.json](../../../palantir-research/knowledge-base/_index/裁剪标签.json)
@ commit `c8588e03c089ba248f2d273dd3311d23ec5d8b2b`，条目 `units["app-building::Automate"]`：
`status: do / priority: P0 / focus: true / by: 产品 / confirmed: true`，note 明言
「要改我们已有的做法，改之前先别按旧做法加功能……要收拢成同一套『条件加效果』，
**等架构方案、产品批了再改**」（updatedAt 2026-11-16）。对本模块的含义：定时触发的收拢方向
**适用**于本评估的协作边界，但统一 Automate 方案未提出、未批——本文不能据未批源默认独立接线
已获批，也不能认定任何统一方案已存在；Automate 边界在 T8 内裁决（本仓侧佐证
[矩阵 行129](data-integration-requirements-coverage-matrix.md)「不等于长期后台身份或统一 Automate」）。

## 2. 两条真实链路：现有与缺失分开列

### 2.1 A 链（ADR-0037 基线）：机制存在、业务接线缺失

**现有**〔源码〕：

- 库 [libs/event-scheduler](../../libs/event-scheduler/doc.go)：单行 `FOR UPDATE SKIP LOCKED`
  认领到期定义（[scheduler.go:69-78](../../libs/event-scheduler/scheduler.go)）；
  `scheduledFor = next_run_at`（:108）；**晚到 tick 把漏掉的多期合并为一次事件**、从下一未来槽恢复
  （:110-117 注释自述为 cron/K8s `Forbid` 类语义；机制上这是本库 `ComputeNextFire(def, now)`
  （:120、:216-242）按 tick 时刻重算实现的**本库合并策略**，非 K8s CronJob `Forbid` 机制赋予，
  不逐期补跑）；cron 重算失败在发布前中止 tick
  （:118-124）；publish 失败回滚、行保持 due 供下次重试（:126-140）；发布后提交（:142-153）。
- 单次 CLI [cmd/schedules-tick/main.go](../../libs/event-scheduler/cmd/schedules-tick/main.go)：
  单发 tick、90s 超时（:53），实例化 Scheduler 并仅调一次 `Tick`（:74-77）；
  Kafka 身份无 SASL 时回落 PLAINTEXT dev principal（:105-119）。
- 表：库自带迁移 [0001_schedules_definitions.sql](../../libs/event-scheduler/migrations/0001_schedules_definitions.sql)
  + pipeline 库镜像迁移 [20260504000083](../../services/pipeline-build-service/migrations/20260504000083_schedules_definitions.sql)
  ——**迁移定义存在；实际部署与存量数据未核**（本文未连库）。
- 测试：[scheduler_test.go:29-168](../../libs/event-scheduler/scheduler_test.go) 共 9 个测试函数，
  全部为 cron 解析/OpenLineage 纯单测，**无 Tick 的 PG/Kafka 集成验收**。

**缺失（接线差距，均经全仓检索独立核实）**〔源码〕：

1. **无登记入口**：迁移注释指向的 Rust 登记方 `schedules_v2.rs`/`cron_registrar` 在仓库中已不存在
   （find 无结果）；Go 侧无任何 `INSERT INTO schedules.definitions`（全仓 grep 仅库内 SELECT/UPDATE）；
   库文档自述「Operators populate」手工填行（[doc.go:19-22](../../libs/event-scheduler/doc.go)）；
   管道 CRUD 不写 `schedules.definitions`（见 §2.2 缺口 2 的 CRUD 证据）。
   实际库中是否有手工/历史存量行**未核**（未连库），切换前须盘点。
2. **无 CronJob 部署模板**：`infra/helm` 无 schedules-tick CronJob，仅
   [of-data-engine/services.yaml:8-9](../../infra/helm/apps/of-data-engine/templates/services.yaml) 与
   [of-platform/README.md:13-15](../../infra/helm/apps/of-platform/README.md) 注释性提及。
3. **pipeline-build-service 无任何 Kafka 代码**（全服务 grep kafka 零命中）：无消费方、无 producer、
   无相关 config；`pipeline.builds.v1` topic 与 `__dlq.pipeline.builds.v1` 在 Kafka values 中预置
   （[values.yaml:161](../../infra/helm/infra/kafka-cluster/values.yaml)、:189）但无代码读写
   （实际 topic 是否已在集群创建未核）。
4. **事件语义不满足 ADR-0038 发布前置**：ADR-0038 :43-45 要求事件在发布前 MUST 经 schema 校验、
   :49-58 要求 8 字段 envelope；tick 现状直接 `Publish` 原文 `payload_template`
   （[scheduler.go:131-132](../../libs/event-scheduler/scheduler.go)），**两者均无**。
   待裁的是**生产者侧** envelope 的具体构造位置与字段映射（tick 内构造 / 登记侧仅存**模板**——
   每次发布时 producer 必须填入本机会 `scheduled_for`/定义 revision、构造合规 `event_id`、经
   schema 校验后发布 / 引入 outbox），不存在「消费方侧补 envelope」的合规选项，
   **静态预填的 envelope/`event_id` 不得跨周期原样重发**（不同机会必须得到不同 `event_id`）。
   现有 OpenLineage `run_id =
   v5(name|scheduled_for)`（:171-180）是**lineage 标识，不是 ADR-0038 业务 `event_id`**，两者严格
   区分；合规 `event_id` 须映射 ADR-0038 的 `aggregate_type/aggregate_id/version/event_type` 语义
   （候选输入：tenant、schedule 稳定 ID、定义 revision、机会 `scheduled_for`——避免不同定义修订或
   不同周期碰撞）。任何偏离此合同的构造须另获批准；**不宣称现 `run_id` 已合规**。
5. **投递语义为至少一次**：publish 成功后 commit 失败会以同一 `scheduledFor` 重发
   （[doc.go:36-44](../../libs/event-scheduler/doc.go) 自述 at-least-once；代码序 :132→:153 佐证）——
   恰好一次不存在，重复抑制依赖消费方幂等（准入事务见 §5）。
6. **〔本文独立发现，确信度高〕坏定义可持续阻塞 tick**：认领按 `ORDER BY next_run_at LIMIT 1`
   （:74-75），坏行重算失败回滚后 `next_run_at` 不变。**串行 tick 下**（CronJob 单实例顺序执行），
   若坏行是最早 due，每个 tick 都在它之前中止，**其后所有到期定义持续得不到处理**；
   并发/重叠 tick 下 `SKIP LOCKED` 只防重复认领，不保证其它 pod 能绕开坏行——**无公平性保证**。
   无 per-row 隔离、无 DLQ（对照 §6-V7 必测）。
7. **定义表无 tenant/owner 列**：[0001 迁移 :15-42](../../libs/event-scheduler/migrations/0001_schedules_definitions.sql)
   全列清单（id/name/cron_expr/cron_flavor/time_zone/enabled/topic/payload_template/next_run_at/
   last_run_at/created_at/updated_at）无租户字段——多租户语义只能藏进 `payload_template`，
   与管道租户模型未接。

### 2.2 B 链（legacy run-due）：端点存在、无触发方、语义缺口多

**现有**〔源码〕：

- HTTP 入口 `POST /api/v1/data-integration/pipelines/_scheduler/run-due`，挂在 JWT+TenantScope
  中间件组内（[server.go:104-106](../../services/pipeline-build-service/internal/server/server.go)、
  :154、:162）——**run-due 必有 caller JWT**，其 bearer 会转发给下游数据面读取
  （[data_integration.go:265-268](../../services/pipeline-build-service/internal/handler/data_integration.go)）。
- 处理器 [RunDueScheduledPipelines](../../services/pipeline-build-service/internal/handler/data_integration.go)
  （:199-222）：查 due（:204）→ 逐条同步执行（:214，沿**请求 context**，无独立周期 worker）→
  **`startPipelineRun` 返回 nil error 后即推进 next_run**（:214-218，按执行后 `time.Now()` 算下次
  :217——涵盖业务终态 failed/cancelled 的情形，见缺口 3）→ 整体返回 200（:221）。
- 执行体（legacy 入口）[startPipelineRun](../../services/pipeline-build-service/internal/handler/data_integration.go)
  （:238-286）：验证 → `OpenPipelineRunWithOptions`（:247，写 `pipeline_runs` 表）→
  `planFromPipeline`（:251）→ 同步 `executor.Execute`（:271）→ 持久终态（:271-280）。
  注意 plan 构造（[execution.go:428](../../services/pipeline-build-service/internal/handler/execution.go)：
  `BuildID = runID`、`BuildBranch` 硬编码 `"master"`；:459 JobID 由 runID+node 派生）**不写
  `builds`/`jobs`/`job_outputs` 实体表**——legacy 调度未进 canonical 构建链（见缺口 10）。
- next_run 重算 [nextrun.go](../../services/pipeline-build-service/internal/domain/schedule/nextrun.go)：
  文件头自称 legacy、1:1 port；复用 `libs/scheduling-cron`（与 A 链同一解析器）；
  paused/disabled/无 cron/解析失败返回 nil（:29-47）。
- repo 三件（[repository.go](../../services/pipeline-build-service/internal/postgres/repository.go)）：
  `ListDuePipelines` :995（纯 SELECT，无 FOR UPDATE/认领；:998-1003 **按调用者租户 scope 过滤**）；
  `OpenPipelineRunWithOptions` :856（:857 新 UUID 直接 INSERT 'queued'，无 slot 唯一键）；
  `UpdatePipelineNextRun` :1020（普通 UPDATE）。
- 单测：cron 重算 next run（〔计划〕[矩阵 行121](data-integration-requirements-coverage-matrix.md)
  登记 data_integration_test.go:133）。

**缺失/语义缺口**〔源码〕：

1. **无周期触发方**：仓库内无 CronJob/进程内 ticker 调 run-due（矩阵 行121 同结论，本轮独立复核）。
2. **`next_run_at` 缺明确初始化路径**：CRUD 的 INSERT/UPDATE 只写 `schedule_config`
   （[pipelines.go:280](../../services/pipeline-build-service/internal/postgres/pipelines.go)、:468、:474、:489），
   该文件不写 `next_run_at`（仅 :20 SELECT 列清单含它）；`UpdatePipelineNextRun`（repository.go:1020）
   是全仓唯一写入点且仅由 run-due 成功路径调用。而 ListDuePipelines 要求
   `next_run_at IS NOT NULL`（:996）——**NULL 的管道永远不会被 run-due 选中，也无法靠 run-due
   自举**（它根本看不到该管道）：启用调度后首轮触发没有发生路径。
3. **「返回 nil error 即推进」涵盖业务失败终态**：`pipelineRunStatus(outcome)`
   （[execution.go:959-970](../../services/pipeline-build-service/internal/handler/execution.go)
   三分支 succeeded/cancelled/failed）落库后 `return fresh, nil`；即便 `GetPipelineRun` 回读失败或
   为空，:281-285 仍 `return run, nil` → 外层照样推进。真正不推进的只有
   validation/plan/Execute 基础设施错误（:244-245、:253-254、:272-274）与
   **Finish 失败**（:278-279 return error）；Get 失败不构成不推进。
4. **失败重试无上限**：不推进的失败使 `next_run_at` 保持过期 → 每次 run-due 重复选中同一管道，
   无退避、无次数上限、无隔离（ADR-0038 的 5 次预算是事件消费侧重试口径，是否映射为管道业务
   重跑预算属 §3 待定，不能默认等同）。
5. **错误处理松散**：逐管道错误被吞（循环内不中断，:210-220）；`UpdatePipelineNextRun`
   错误被 `_ =` 显式忽略（:218）——推进失败即下一周期重复触发。
6. **并发无防护**：两个并发 run-due 各自 SELECT 到同一批 due（无认领）并各开新 run
   （:857 新 UUID 无唯一约束）→ 同期重复副作用可见。
7. **租户视野被调用者绑定 + 后台身份不闭环**：due 列表按调用者 JWT 的租户过滤（:998-1003）——
   后台调度需要跨租户身份或按租户逐个调用，当前无此身份。且**不能泛称「后台已有 service token
   闭环」**：run-due 在 JWT HTTP 组内保留 callerBearer 转发（:265-268）；仅当下游链路无 caller
   context 时数据读取/输出提交才回落静态 service token（[dataset_input_loader.go:101](../../services/pipeline-build-service/internal/handler/dataset_input_loader.go)
   调用、:159-167 `datasetReadBearerToken` 实现；
   [dataset_output_committer.go:57](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)
   调用同一 helper），而数据面 HTTP 版本化客户端无 caller 直接拒绝
   （[dataset_http_versioning.go:117-121](../../services/pipeline-build-service/internal/handler/dataset_http_versioning.go)
   `ErrDatasetCallerRequired`）——真正的后台身份缺口仍开放。
8. **漏期语义**：一次调用每管道至多触发一次 run（推进后不再 due）——同样不逐期补跑；
   与 A 链的差别在推进条件绑定执行返回值（缺口 3/4 组合=失败无限重试）。
9. **不能援引 P2.4 的验收**：P2.4 的租约/身份/恢复语义属 Connector 同步链限定例外
   （〔计划〕矩阵 行60）；B 链自身无租约、无恢复保证。
10. **执行入口不在 canonical 构建链**：legacy 入口只写 `pipeline_runs`（:247/:865）；
    canonical 链是 `PreflightBuild → OpenBuild → ResolveBuild → PersistResolvedBuild`
    （[build_resolution.go:305/:325/:340/:352](../../services/pipeline-build-service/internal/handler/build_resolution.go)），
    由 [repository.go:456/:492/:500](../../services/pipeline-build-service/internal/postgres/repository.go)
    写 `builds`/`jobs`/`job_outputs` 实体。调度事件应接哪个入口、`PipelineRun/Build/Job` 身份如何
    映射、definition/branch 依据（legacy 硬编码 `"master"`；S1 契约新调用显式 branch）、验收查
    哪组表/API——均为 T8 待裁项（§5）。**不把轻量 runtime 已验收（矩阵 行120）等同为 legacy
    调度已进 canonical 链。**

### 2.3 平台基础可复用范围（非全无，但须按缺口修正使用）

〔源码〕以下均已存在，A/B 任一方案均可复用（复用时注意各自注明的缺口）：

- 消息设施：`libs/event-bus-data`（Publisher 与 KafkaSubscriber consumer-group API，
  [publisher.go:15](../../libs/event-bus-data/publisher.go)、
  [subscriber.go:54-95](../../libs/event-bus-data/subscriber.go)）。
  **注意**：`CommitOffsets` 是 no-op（[subscriber.go:136-139](../../libs/event-bus-data/subscriber.go)），
  持久提交必须显式 `CommitMessages`（:119-134），且**同分区不得越过尚未解决的失败 offset 提交**。
- 支撑库：`libs/outbox`、`libs/idempotency`、`libs/state-machine`、`libs/scheduling-cron` 目录齐备。
  **注意**：`libs/idempotency` 是 record-before-process 语义——`CheckAndRecord` 在副作用 closure
  之前独立记账（[idempotency.go:19-25](../../libs/idempotency/idempotency.go) 语义注释、
  :84-93 先记账后执行；closure 失败 eventID 仍标记已处理 :77）——**消费中崩溃/失败后重投会
  skip，可能永久丢一次调度**，仅适合配合 outbox/saga 使用，不能直接当调度准入的完整方案。
- `processed_events` 表先例：[workflow-automation 20260504200000:78](../../services/workflow-automation-service/internal/repo/migrations/20260504200000_outbox_and_idempotency.sql)、
  reindex-coordinator 0001（迁移定义存在；部署未核）。
- **消费方结构参考（非金样）**：[conditionconsumer](../../services/workflow-automation-service/internal/domain/conditionconsumer/consumer.go)
  展示了去重→状态机→outbox 同事务→offset 提交的接线形状（包注释 :1-28；去重调用 :90 即上述
  record-before-process 语义），**照搬其去重时机即继承「崩溃后重投 skip 可能永久丢」的缺口**；
  毒消息不提交 offset 的处理可参考。
- CronJob 部署样板：[approvals-timeout-sweep-cronjob.yaml:37-41](../../infra/helm/apps/of-apps-ops/templates/approvals-timeout-sweep-cronjob.yaml)
  （`schedule */5`、`concurrencyPolicy: Forbid`、`startingDeadlineSeconds: 60`；本仓样板
  `apiVersion: batch/v1`，未检出目标集群版本锁）。按官方 **1.34 行为说明**（参考，实际部署
  集群版本未核，WP3.2 实施前锁定并复核）：`Forbid` 只约束**同一 CronJob 创建的 jobs** 不重叠
  （[concurrency-policy](https://v1-34.docs.kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#concurrency-policy)），
  错过的调度可能不建 job（[job-creation](https://v1-34.docs.kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#job-creation)）——
  即 tick 可能多建/漏建，**下游必须幂等**；且 `Forbid` 不约束管道执行级重叠（后者需持久
  run 级策略，§3 待定）。
- 结构事实：Rust `pipeline-schedule-service`、`workers-go/`、Temporal 相关目录均已删除
  （目录检索无结果；ADR-0037 :132-135 的移除已落实）——方案 C 无现成基础可援引。

## 3. 差距表（维度 × 现状 × 待定；证据行号见 §2 对应条）

| 维度 | 源码现状 | 未闭环语义 | 建议归属/协作 | 实施前待定合同 | WP3.2 验收对应 |
| --- | --- | --- | --- | --- | --- |
| 定义生命周期 | A：无登记入口、operators 手工填行（§2.1-1）；B：CRUD 只存 `schedule_config`、`next_run_at` 无初始化路径（§2.2-2） | 管道 CRUD↔调度定义的创建/暂停/删除/变更传播 | 数据与接入组+架构组 | 定义归属表（`schedules.definitions` vs `pipelines` 谁是源）及 upsert/删除断言 | V1/V7 |
| tenant/principal | A：定义表无租户列（§2.1-7）；B：due 按调用者租户过滤、后台身份不闭环（§2.2-7） | 多租户定义可见性、后台执行身份与租户归属、授权可撤销与代次核验 | 架构组+平台运行时+安全 | 后台 principal 形态与可撤销授权引用/代次核验、租户传播 | V4 |
| 事件契约/可靠投递 | A：无 schema 校验、无 envelope、直接发 `payload_template`、至少一次（§2.1-4/5） | ADR-0038 发布前置由生产者侧满足；envelope 构造位置与字段映射 | 架构组 | envelope 构造位置（tick/登记侧/outbox）与 `event_id` 输入映射（§2.1-4） | V6/V8 |
| 机会去重与准入 | A：SKIP LOCKED 单 tick 内互斥（§2.1）；B：无认领、并发重复 run 可见（§2.2-6）；现有 idempotency 库 record-before-process 有丢调度缺口（§2.3） | 跨 tick 重复（publish 后 commit 失败重发）的准入语义；崩溃后可恢复 | 数据与接入组+架构组 | `processed_event` + 持久 run/slot **同一 DB 事务准入**；终态与结果 outbox 的事务边界另列 | V3 |
| 执行入口与身份映射 | legacy 只写 `pipeline_runs`；canonical 写 `builds/jobs/job_outputs`；branch 硬编码 master（§2.2-10） | 调度接哪个入口；PipelineRun/Build/Job 身份映射与 branch 依据 | 数据与接入组+架构组 | 入口选择、身份映射、definition/branch 规则（S1 新调用显式 branch）、验收查证表组/API | V1/V6 |
| 消费确认 | pipeline-build-service 无 Kafka 代码（§2.1-3）；`CommitOffsets` no-op（§2.3） | 消费→Build→产物发布的确认点与 offset 语义（不得越过未解决失败的 offset） | 数据与接入组 | 处理成功判据（Build 创建?终态?发布?）与提交时机 | V1/V6 |
| 后台身份与撤权 | 无任何长期后台身份；P2.4 arm 重启/过期需重授权（§1/§2.2-7） | 长期授权、可撤销、失效可观察 | 安全+架构组 | 撤权信号路径与失效时调度行为（跳过+告警?暂停?） | V4 |
| 持久执行与恢复 | A：tick 无状态、重发可去重（§2.1-5）；B：请求 context 同步执行、崩溃即丢（§2.2） | 服务重启/消费崩溃后 due 语义与恢复 | 数据与接入组 | 准入事务内未完成 run 的恢复语义（重投/续跑/登记） | V2/V6 |
| 漏期/重叠/失败策略 | A：漏期合并一次（§2.1）；B：失败不推进无限重试、无上限（§2.2-4/8）；CronJob `Forbid`≠管道执行不重叠（§2.3） | 补跑策略；tick 重叠、**每机会去重与跨机会不重叠**分别声明；ADR-0038 的 5 次消费预算是否映射业务重跑预算 | 架构组 | 漏期/重叠（tick 级、每机会、跨机会）/失败（重试上限、退避、DLQ）显式声明 | V3/V7/V8 |
| 结果未知及发布保旧 | 发布/提交/崩溃窗口未定义（A 链 publish→commit §2.1-5；B 链 Finish 失败=终态丢失 §2.2-3） | 结果未知时不得发布半成品、旧产物保持可用 | 数据与接入组（对照 P2.5 语义） | 发布原子性与保旧断言 | V5/V6 |
| 部署/监控/DLQ | topic+DLQ 在 values 预置无人读写（部署未核）、无 CronJob 模板、无调度告警（§2.1-2/3） | 部署形态、指标、DLQ 消费与告警责任 | 平台运行时 | CronJob 参数（频次/超时/Forbid）、SLO 与告警清单 | V2/V8 |
| Automate 边界 | Automate 域已有条件消费链（§2.3 结构参考），但非管道调度方案；统一方案须架构方案+产品批（§1〔仓外源〕） | 管道调度是否并入 Automate 的裁决与时机 | 架构组+产品（限此边界） | 共享边界或明确不共享的声明 | — |

## 4. 四方案比较

**候选可比较；优先级与批准门槛以〔计划〕[矩阵 行381](data-integration-requirements-coverage-matrix.md) 为准——
A 是优先基线，本文推荐不等于批准。**

### 4-A 按 ADR-0037 补接线（优先基线）

- **定位**：现行 Accepted 方向的落地（ADR-0037 :110-111），裁的是接线细节而非机制本身。
- **复用**：§2.1 全部 + §2.3（消息设施、消费方结构参考、CronJob 样板、topic/DLQ 预置）。
- **新增**：登记 API/事务、合规 envelope 生产者侧构造（§2.1-4）、pipeline 事件消费方（含准入事务）、
  CronJob 部署模板、执行入口接线（§3「执行入口与身份映射」）、§6 验收。
- **主要风险**：坏定义持续阻塞（§2.1-6）需库级或部署级对策；envelope 构造是合规前置；
  多租户定义模型需新建（§2.1-7）。
- **批准前置**：T8 常规裁决即可（机制已 Accepted）。
- **依赖/迁移**：§3 待定合同逐项签署；源码层面无登记方在写（§2.1-1），但**实际库存量未核**，
  切换前须盘点存量 `schedules.definitions` 行。
- **估算快照**：WP3.2 = 8–14 人日（〔计划〕[T0 行119](data-integration-openfoundry-t0-reestimate.md)）。

### 4-B legacy run-due 补触发

- **定位**：偏离 ADR-0037。**须先获限定例外或替代 ADR（范围/退出/迁移）**——不能当作不需要
  批准的平级实现选项（矩阵 行381 明文；对照 P2.4 当年即以限定例外方式偏离 ADR-0025）。
- **复用**：§2.2 全部（HTTP 端点、legacy 执行体、next_run 重算）；仍建议补用 §2.3 设施
  （幂等/监控，注意 §2.3 注明的缺口）。
- **新增**：周期触发方、跨租户后台身份、认领/去重、重试上限、`next_run_at` 初始化路径、
  canonical 入口接线（若要求，§3）、§6 验收。
- **主要风险**：治理成本（例外理由+退出计划）；§2.2 的 10 项缺口需逐一修；HTTP 触发面暴露给
  周期调用方带来安全面扩大。
- **批准前置**：限定例外或替代 ADR + 退出/迁移计划。
- **依赖/迁移**：同 §3 合同 + 例外批准；若后续仍转 A，触发方/身份改造大多作废。
- **估算快照**：WP3.11 = 5–8 人日（〔计划〕[T0 行128/行344](data-integration-openfoundry-t0-reestimate.md)，
  替代 WP3.2 非叠加）。

### 4-C 重引 Temporal

- **定位**：须**新架构决策**；不能把 ADR-0025 当作仍要求 Temporal 的现行依据——ADR-0021 已
  Superseded（:10），ADR-0037 :132-135 已下令移除且移除已落实（§2.3 结构事实）。
- **复用**：几乎无（Temporal 集群、客户端、worker 目录均已删除）。
- **新增**：集群+SDK+调度注册+全部接线+运维面（SPOF 类第二集群，ADR-0037 :79-84 否决理由重现）。
- **批准前置**：新 ADR 推翻 ADR-0037 相关部分。
- **估算**：T0 未列——属新不确定量，若认真考虑须先触发重估，不得沿用任何旧区间。

### 4-D 等待统一 Automate

- **定位**：**是等待/依赖策略，不是已存在的第四执行引擎**。Automate 域现有条件消费链（§2.3）
  属 Automate 自身闭环；源要求（§1〔仓外源〕）明言「等架构方案、产品批了再改」——统一方案
  未提出、未批，今日不存在已批方案。
- **复用/新增**：等待期无新增；若未来并入则复用 §2.3 Automate 设施。
- **主要风险**：管道调度无限期缺失（DI-03 P0 收尾悬置）；外部时程不可控。
- **批准前置**：架构方案提出 + 产品批准（Automate/产品范围边界）。
- **估算**：等待期不可估，T0 未列。

### 4-E 小结与新登记不确定量

| 项 | A | B | C | D |
| --- | --- | --- | --- | --- |
| 批准门槛 | 常规 T8 裁决 | 限定例外/替代 ADR | 新 ADR | 架构方案+产品批准 |
| 复用面 | 最大（§2.1+§2.3） | 中（§2.2） | 无 | 未来或大 |
| 语义缺口修复量 | 中（envelope/租户/坏定义/入口） | 大（§2.2 十项） | 全新 | 不适用 |
| 估算快照（T0 原始，非实耗） | WP3.2 8–14 | WP3.11 5–8 | 未列 | 未列 |

**新登记不确定量（仅登记重估触发，不伪精算、不当实耗）**：① A 链 envelope 构造+登记事务+
执行入口接线的裁定复杂度可能使 WP3.2 贴近区间上限；② B 链若要求修齐 §2.2 全部语义
（身份/认领/重试上限/初始化/入口），5–8 区间可能不足——建议 T8 裁决后按实际范围触发 T0 重估；
③ 坏定义阻塞若要求库级修复，属 WP3.2 内新增量。

## 5. 分阶段实施顺序建议与待裁决清单

**推荐顺序（工程建议，均待 T8 裁决；签署前不开 WP3.2）**：

1. **先裁机制**：A 为推荐基线（唯一 Accepted 方向、复用面最大）；若选 B，
   例外批准先行（对照 P2.4 先例）；C/D 不推荐作为本期路径（理由见 §4-C/4-D）。
2. **再裁合同细节**（**集中进一次 T8 决策记录**，不逐项新增审批轮次；按依赖序）：
   定义归属 → tenant/principal → 事件身份+准入事务 → 执行入口与身份映射 →
   漏期/重叠/失败策略 → 手动接口/默认开关/退出 → Automate 共享边界。
3. **后实施**：WP3.2（或裁决 legacy 后的 WP3.11）二选一实施，按 §6 验收。

**裁决与审批层级**：T8 机制与合同由**架构组+数据与接入组+平台运行时**裁决；**产品参与限于
源要求划定的 Automate/产品范围边界**（§1〔仓外源〕）——技术合同细节不逐项升级为产品签字，
产品边界只在「是否并入 Automate、产品范围是否覆盖管道调度」处介入。

**逐项推荐（待 T8 决策记录签署）**：

| 待裁决项 | 本文推荐（待签署） | 理由（证据） | 责任方 |
| --- | --- | --- | --- |
| 定义归属 | A 链：`schedules.definitions` 为运行时定义，管道 CRUD 启用调度时 upsert（name 按 tenant+pipeline 派生）；B 链：`pipelines` 行即定义并补 `next_run_at` 初始化 | 消除「手工填行」与「无初始化路径」断点（§2.1-1、§2.2-2） | 数据与接入组+架构组 |
| tenant/principal | 专用 scheduler 身份 + 定义携带**可撤销的授权引用与代次**（token/bearer 不得入事件；tenant/principal 快照最多作审计）；消费准入与执行前核验**当前** tenant/授权有效性，撤销即拒绝执行 | A 表无租户列、B due 被调用者绑定且后台身份不闭环（§3）；「快照执行」无法表达撤权；arm 模式不适合常驻调度（矩阵 行129）；具体授权机制留 T8 安全协作裁定，本文不声称现有已实现 | 架构组+平台运行时+安全 |
| 事件身份+准入事务 | 生产者侧构造合规 envelope；`event_id = v5` 按 ADR-0038 aggregate 语义映射（tenant、schedule 稳定 ID、定义 revision、`scheduled_for`）；消费方 **`processed_event` + 持久 run/slot 在同一 DB 事务内准入，未完成可恢复**；终态落库与结果 outbox 的事务边界在决策记录中另列 | ADR-0038 :43-58 发布前置；`libs/idempotency` record-before-process 有崩溃丢调度缺口，不能当完整方案（§2.3） | 架构组+数据与接入组 |
| 执行入口与身份映射 | 待裁：接 legacy `pipeline_runs` 链还是 canonical `Preflight→OpenBuild→Resolve→Persist` 链；PipelineRun/Build/Job 身份映射、definition/branch 依据（对齐 S1 显式 branch）；验收查证表组/API 随之确定 | 两链实体不同（§2.2-10）；轻量 runtime 已验收≠legacy 调度已进 canonical 链 | 数据与接入组+架构组 |
| 漏期/重叠/失败 | 漏期合并一次（维持 A 现语义）；tick 重叠用 CronJob `Forbid`；**每机会去重**（同 `scheduled_for` 不重复创建 run）与**跨机会不重叠**分列——若要求不重叠，用按 tenant+pipeline 的活动运行约束/有 fencing 的租约或等效持久策略（同机会 slot 唯一防不了不同 `scheduled_for` 并发）；事件消费重试按 ADR-0038 5 次+DLQ，**管道业务失败重跑预算单独声明** | 与 A 现行为（§2.1）及 ADR-0038 :108-118 一致；`Forbid`/每机会 slot 唯一/跨机会活动运行约束是三层不同机制（§2.3）；5 次预算原义是消费侧重试（§1 表）；持久策略具体形态留 T8 | 架构组 |
| 手动接口/默认开关/退出 | run-due 保留为调试端点+管理员限定；新调度默认关闭、逐租户开启；支持一键全停 | 灰度/回退成本；run-due 现语义缺口多（§2.2），不宜作生产触发面 | 数据与接入组+安全 |
| Automate 共享边界 | 本期不共享，管道调度独立接线；统一 Automate 另按源要求走「架构方案+产品批」 | D 是等待策略且未批（§4-D）；不共享不阻塞（T8 只阻塞 B2 调度） | 架构组+产品（限此边界） |

## 6. WP3.2 计划验收（**计划口径：本轮未执行、未运行任何集群**）

按〔计划〕[矩阵 行132](data-integration-requirements-coverage-matrix.md) 口径细化；
全部要求**周期自动触发**证据，不以人工重复调用 run-due 冒充周期调度：

- **V1 真实周期**：至少两个自动到期周期 → Build（按 §5 裁定的入口与身份映射）→ 产物发布可核对
  （查证表组/API 按 §5 裁定；产物身份/行数/hash）。
- **V2 重启恢复**：调度组件与消费服务重启后**按裁定的漏期策略恢复**（若裁定漏期合并，则不承诺
  补齐停机期间每一期——不得一边合并漏期一边承诺所有期不漏）；恢复后不重复执行已确认的机会。
- **V3 并发/重复**：并发 tick 与重复事件（含 publish→commit 窗口重发）不产生重复可见副作用；
  **同 slot 重复**（同 `scheduled_for`）与**跨 slot 重叠**（不同 `scheduled_for` 是否允许并发，
  按裁定）分别有用例。
- **V4 身份/租户**：后台身份撤权后行为可观察（跳过+告警或暂停，按 §3 裁决）；
  **撤权后已在队列/重放的事件不得凭旧授权执行**（准入与执行前核验当前授权代次）；跨租户读写隔离。
- **V5 失败保旧**：执行失败不破坏旧产物、不发布半成品。
- **V6 提交/崩溃窗口**（至少覆盖三个窗口并各有登记语义，不产生孤儿副作用）：
  准入事务提交前崩溃（事件重投须能续跑/重入，不因先记账而 skip 丢失）；准入事务提交后、执行开始前
  崩溃（未完成 run 可恢复）；执行完成但结果确认/offset 提交前崩溃。**边界**：同事务准入幂等键只
  解决「不重复创建持久 run」，**不构成对外部发布的恰好一次保证**——执行已产生外部产物而本地
  终态/回执未落库时，须有稳定发布幂等键/回执核对与 unknown-result 恢复合同（不盲目重新执行）；
  现状 Pipeline 输出仅 2xx ACK、无回执校验（S1 GAP F15，
  [S1 行89/行242](data-integration-s1-dataset-contract.md)），keyed 幂等/unknown-result 恢复仅在
  Connector 链验收（P2.6，S1 CR-6.2/CR-6.4，行155-169）——**登记为 WP3.2 设计/重估风险，本轮
  未关闭**；「终态落库+outbox」亦不自动覆盖 DVS 外部提交的确认。同分区不得越过未解决失败的
  offset 提交。
- **V7 漏期/重叠/坏定义**：漏期按裁决策略（合并/补跑）；tick 与 run 级重叠均不双跑；坏定义被隔离
  ——**串行 tick 下不得持续阻塞其它到期定义**（针对 §2.1-6，必须有用例）。
- **V8 DLQ/毒消息**：事件消费重试预算耗尽进 `__dlq.pipeline.builds.v1` 可观察、要求 on-call review
  （ADR-0038 :114）；毒消息不阻塞分区。
- **证据形态**：验收记录须含周期触发时间线（tick 时刻 vs due 时刻）与产物核对，登记进矩阵。

**WP3.1 自身完成标准（本文档口径）**：证据盘点（§2，行号可点）、方案比较（§4）、可裁决清单（§5）、
验收草案（§6）齐备且经 Codex review 通过——**不要求本轮真实集群/周期验收**。

## 7. 待 T8 裁决与实施前核实项

1. envelope 生产者侧构造位置（tick 内构造 / 登记侧存**模板**、发布时填入本机会
   `scheduled_for`/revision 并构造 `event_id` / 引入 outbox）与 `event_id` 输入映射的最终选择
   ——ADR-0038 只定合同不定实现；静态预填 envelope 不得跨周期原样重发（§2.1-4、§5）。
2. ADR-0038 的 5 次事件消费重试预算是否/如何映射为管道业务失败重跑预算——两层语义需在
   决策记录中分别声明（§1 表、§5）。
3. CronJob `Forbid` 之外的 **run 级**重叠与漏期策略（持久 slot/租约形态）——需架构组定（§5）。
4. 执行入口（legacy vs canonical）与 PipelineRun/Build/Job 身份映射、branch 依据——随 S1 显式
   branch 契约对齐（§2.2-10、§5）。
5. WP3.11 的 5–8 区间是否覆盖 §2.2 十项缺口修复——建议 T8 裁决后触发 T0 重估（§4-E）。
6. 坏定义阻塞是否要求 `libs/event-scheduler` 库级修复进 WP3.2 范围（§2.1-6、V7）。
7. 实际库中 `schedules.definitions`/`pipelines.next_run_at` 的存量状态（本文未连库未核）——
   切换前盘点项（§0、§4-A）。
