# WP3.2 A-Worker Checkpoint（进度保存 / A2 交接基线）

> 历史阶段快照，当前 WP3.2 验收/归档清理以[验收报告](data-integration-wp3-2-acceptance.md)为准；其中当时保留的临时容器已清理，旧启动命令是历史记录。

- **编制**：GLM-5.3（WP3.2 A worker / 主 service worker），2026-11-17。
- **用途**：A worker 在稳定模块边界交接给 A2（同 GLM 系新会话）；root 据此换 worker。本文为 A 独占文件。
- **HEAD/未提交**：HEAD=`f4c6194ca93d2899162887bbde10ff4126fdf618`（2026-11-17 11:23:47 +0800）；**全部 WP3.2 变更未提交未推送**（`git status` 见下清单）。B/C worker 的并行文件（`libs/event-scheduler/**`、`infra/**`、`tools/smoke/**`、`data-integration-wp3-2-deployment-profile.md`）同工作区共存，A 未触碰。

## 1. 授权与所有权（累计有效）

- 用户 2026-11-17 授权：ADR-0037 基线补齐接线，WP3.2 实施授权（不再停留评估）；「继续自主实现，不需再审批」。
- **A（本 worker/A2）独占**：`services/pipeline-build-service/**` 全部（含全部 migrations、`internal/scheduling`、`internal/schedulingapi`、`internal/config`、`internal/server`、`internal/postgres/scheduling_*`）、`libs/schedule-events/**`、T8 决策文档（`data-integration-t8-scheduling-decision.md`）、WP3.1 评估文档、本 checkpoint。
- **B（producer worker）独占**：`libs/event-scheduler/**`、`tools/smoke/**`。**C（infra worker）独占**：`infra/**`、deployment-profile 文档（生产 DSN/source/role/provider/ACL）。
- 禁止：跨域改动；root `go build ./...`（扫旧 results blob——**只跑受影响包**）；提交/推送；动真实 `.env`/旧 results/DB/卷；生产部署。

## 2. 已 review 通过的模块（root 逐轮复核记录）

| 模块 | 通过的 review | 测试证据（可复跑命令与路径） |
| --- | --- | --- |
| M1 共享事件合同 `libs/schedule-events` | ⑨⑫ 修正后通过；**API 已冻结供 B 编译** | `go test -count=1 ./libs/schedule-events/` → ok；schema 指纹见 decision §2 |
| M2 持久化状态机 `internal/postgres/scheduling_repo.go` + 迁移 `20261117100000` | post-lock 统一核验/sealed 全称/nullable 收口——「生产持久化逻辑定向 review **通过**」 | 真 PG：`PIPELINE_DB_DSN=... go test -tags integration ./services/pipeline-build-service/internal/postgres/` → **ok（~20s，13+ 场景**：接管链/两连接屏障×3（精确 `pg_blocking_pids`+固定连接）/Unsealed/Partial/UnrelatedJob/SealedNil/ClaimWallClock/FencedWriteExpiry/ReconcileGate）。容器：`openfoundry-platform-p1-q3v9-unit-tests`（go1.26.3）；PG：`ofwp32-pg-1`（postgres:16-alpine，55432） |
| M3 消费循环/配置/API/mTLS | 本轮 P1 四项修正后全绿（root 未终审——见 §5 剩余项） | `go test -count=1 ./services/pipeline-build-service/internal/scheduling/ ./services/pipeline-build-service/internal/config/` → ok；`go build ./services/... ./libs/...` → BUILD_OK |

## 3. 尚未完成（**不得宣称为完成**）

1. **Canonical runner**（`scheduling.Runner` 是冻结的注入接口，无实现）：resolver 函数族直调、`BUILD_RUNNING`、decision §4 五步 handoff（reservation txn 安全移交）、keyed publish+receipts（`RecordPendingReceipt/MarkReceipt*` 已冻结）、`FinishDispatch` 接线、expected sets 由 resolver 封存。
2. **消费方生命周期接线**：`server` 仅挂了 `/api/v1/scheduling/*` 路由（默认关；**API 本体归 D**）；`NewDialer`+`Consumer.Run`+`ExecutionWorker` 的启动/停止未接进 server 生命周期。
3. **V1–V8 真实验收全部未跑**（需真实 Kafka/CDC/JWT/两个自动周期；Kafka/mTLS/ACL 生产连线归 C，验收脚本归 B）——「代码+单测绿≠真实验收」。**canonical runner/真实 HTTP/V1–V8 均未完成，本 checkpoint 不构成 WP3.2 完成交接**；由 root 新开 GLM A2 接续。
4. 真 PG 集成测试当前**不含** definitions 绑定核验的专项目用例（fixture 已含绑定行；mismatch/unbound 分支仅由 mock 覆盖）。

## 4. 已冻结接口（B/C 按文件读，勿凭记忆）

- **Env/协议/API 路径与 JSON shape**：decision **§6**（含 mTLS 三文件、SSL 默认、枚举校验、秒单位、fail-closed、`/api/v1/scheduling/*` 六端点）。
- **Go API（`libs/schedule-events`）**：`TopicTriggers/TopicOutcomes`、`TenantKind/TenantRef/ParseTenantRef`、`DeriveEventID/DeriveBuildID/DerivePublishKey`、`BuildTriggerEnvelope/ParseAndValidateTrigger/BuildOutcomeEnvelope`、payload 类型（grant_ref+generation 严格==；outcome 仅确认终态一次）。
- **Repo 面（A2 内部）**：`AdmitDispatch(p, OutcomeHook)`（含定义绑定五项闭环）、`Claim/Reconcile/Heartbeat/Finish/FencedWrite`（post-lock DB clock）、`RecordPendingReceipt/MarkReceipt*`（fenced、key 冲突四元组核验、committed 不降级）、`EnqueueDLQViaOutbox`、`Enable/Set/DisableSchedulingDefinition`、`UpsertGrant/SetGrantState`、`GetDispatch/GetGrantByRef`。
- **迁移**：`20261117100000_scheduling_consumer.sql`（scheduling.* 五表+quarantine 两列+outbox REPLICA IDENTITY FULL+receipts 唯一绑定+expected sets 双列）。

## 5. 剩余 review 项与下一命令

- **已收口（本轮 root 复核意见）**：DLQ 幂等（稳定 `(topic,partition,offset)` 派生 event id+同 TX 锁 poison 行+`dlq_event_id` 持久化；缺行拒绝）；处理预算覆盖 valid-admission 错误（持久 attempts，耗尽即停应用处理）；**持久 phase 读取**（`GetPoisonState`——重启/DLQ-ACK 失败后不再重入 admission，`dlq_enqueued` 只 ACK）；`GetDispatch` 的 `pgx.ErrNoRows`=不存在（其余含 Canceled 持有）；`RecordPoisonAttempt` INSERT 显式 `state` 列（修 NOT NULL 无 DEFAULT 的 23502 必现）。真 PG 回归 `TestPG_PoisonCountAndIdempotentDLQ`（首次计数→双次 DLQ 幂等→无行拒绝）。fake store 改持真实 ErrNoRows 形状。
- **API 移交（root 决定）**：`/api/v1/scheduling/*` 四端点与其 CRUD 持久方法（`Enable/Set/DisableSchedulingDefinition`、`UpsertGrant/SetGrantState`）移交 **D worker**（API 文件+独立 CRUD repo 文件）；A/A2 **不再修改** `internal/schedulingapi/**` 与上述 CRUD 方法。
- **待 root review**：本轮 consumer 最终四项（上述已收口项的终审）。
- **API 独立 review Open 项（root 2026-11-17 复核结论，交接 A2/D 修；A 已停更 API 面）**：
  - **P1 越权**：同 org 普通用户 B 可 DELETE/重授覆盖 A 的 grant、pause/resume/delete A 的 definition——现仅按 tenant+key 匹配，无 owner/admin 判定。修法：同 TX 锁资源行并比对 subject/created_by 或可信 admin 票（个人域仅 owner 本人）。
  - **P2 grant-绑定漏匹配**：Enable 的 grant 查询缺 `pipeline_rid`/`subject` 匹配——grant(P1) 可绑定 definition(P2) 返回 201 后才被准入 skip；resume 未重验当前精确绑定（revision/pipeline/branch/grant_ref）与 grant 有效代次。
  - **P2 首跑时间**：登记/更新固定 `NOW()+1min`——中午登记午夜 cron 会在 1 分钟后误跑；应复用 `ComputeNextFire`（UTC+DB 当前时刻，`internal/domain/schedule/nextrun.go` / `libs/scheduling-cron`），且启用前拒绝非法 cron/schema/key；pause/resume/disable 需 `definition_revision+1`（POST 的 ON CONFLICT 自增已原子，无需重写；disable `enabled=false` 不落 legacy tick 已确认）。
  - **测试缺失**：CRUD HTTP/PG 测试为 0——须覆盖 JWT+同 org owner/非 owner/admin、跨 org、personal admin（仅 owner 本人）、真实首轮触发时间与修订递增。
  - **D worker 并行文件**：`internal/postgres/scheduling_crud_repo.go`（D 所有）落地时含一处机械编译错（`return &b, nil, nil`，两返回值函数）——A 做了**一行 unblock**（不改语义，`return &b, nil`），所有权与后续 review 修正仍归 D/A2。
- **A2 下一命令（建议序）**：
  1. `docker run --rm -v $PWD:/w -w /w --network host -e PIPELINE_DB_DSN='postgres://postgres:ofwp32@127.0.0.1:55432/postgres?sslmode=disable' --entrypoint sh openfoundry-platform-p1-q3v9-unit-tests:latest -c "cd /w && go build ./services/... ./libs/... && go test -count=1 -tags integration ./services/pipeline-build-service/internal/postgres/"`（确认基线绿）。
  2. 实现 canonical runner（决策 §4 顺序：先 expected-sets 封存点设计 review，再 handoff 协议实现——`resolver.PreflightBuild/ResolveBuild/PersistResolvedBuild`+`Plans.LoadPlan`+`executor.Execute`，进程内直调，参考 [build_resolution.go:305-352] 与 [execution.go:343-366]）。
  3. server 消费方生命周期（`NewDialer(cfg.Scheduler)`→reader→`Consumer`/`ExecutionWorker`，`cfg.Scheduler.Enabled` 门控）。
- **whole PBS build 证据（root 要求的实际编译）**：`go build ./services/pipeline-build-service/...` → **WHOLE_PBS_BUILD_OK**（server wiring 此前已修：类型名 `*schedulingapi.SchedulingAPI`、wiring 移至 jwt 定义之后——root 所指 :72 旧错不存在于当前工作区）；`go test ./internal/server/` → ok。
- **测试环境备忘**：PG 容器 `ofwp32-pg-1`（端口 55432，密码 ofwp32）；若不存在先 `docker run -d --name ofwp32-pg-1 -e POSTGRES_PASSWORD=ofwp32 -p 55432:5432 postgres:16-alpine`；集成 tag=`integration`。
