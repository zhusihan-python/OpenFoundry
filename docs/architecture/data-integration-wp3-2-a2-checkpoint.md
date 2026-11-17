# WP3.2 A2-Worker Checkpoint（canonical Runner + 消费方生命周期）

> 历史阶段快照，当前 WP3.2 验收/归档清理以[验收报告](data-integration-wp3-2-acceptance.md)为准；其中当时保留的临时容器已清理，旧启动命令是历史记录。

- **编制**：GLM-5.3（WP3.2 worker A2），2026-11-17。A 独占文件（A checkpoint §1 授权延续）。
- **范围**：`scheduling.Runner` 的 canonical 实现、server Kafka 消费生命周期接线、必要的
  新持久执行 repo 方法与测试。consumer.go / scheduling_repo.go / 其测试 / 20261117100000
  迁移仍归 A1 收口，A2 未触碰（root 明确 A1 停前）。D 独占 schedulingapi/CRUD，B 独占
  event-scheduler/smoke，C infra 已冻结。
- **上游**：T8 决策 v2.1 §4（冻结）；A checkpoint §3-4 冻结接口；root baseline review 四项
  陷阱修正（2026-11-17，见 §2）。

## 1. 相位模型（durable phase，无新列）

相位从**已持久状态推导**（dispatch.state + builds.state + jobs + job_outputs +
publish_receipts + build_pending_output_txns），不引入新 migration：

| 相位 | 判定 | 动作 |
| --- | --- | --- |
| **P0-unresolved-clean** | 无 jobs 行、无 pending txn、无 execution intent | 完整 canonical 链（Preflight→持久resolve intent→ResolveBuild→同 TX persist+seal+RUNNING→LoadPlan→Execute） |
| **P0-unresolved-dangling** | 无 jobs 行 但 有 pending txn 记录 | 不得再 Resolve（会重复开 txn）：CleanupOpenedTransactions 对账清收（确认 abort+释放锁+清记录）；全部确认 → FinishDispatch(failed,"resolve_crash_recovered")；任一未确认 → FinishDispatch(unknown_publish) |
| **P1-resolved-fresh** | 有 jobs 行 且 所有 job=WAITING 且 无 receipt 行 且 无 job_outputs.committed 且 无 job 非 WAITING | 唯一允许 Execute 的恢复相位（零副作用证据）。若 expected sets 未封存（历史缝）→ unknown park（不猜集合） |
| **P2-executed** | 任何 job 非 WAITING 或存在 receipt/committed 证据 | **绝不 Execute**（root 陷阱 2）：先 reconcile 未决 publish（仅同 key+库内原 bytes 重放），再终局评估（§4） |
| **P-queued** | ResolveBuild 返回 BuildQueued（无 Jobs） | 不 persist 不 seal；FinishDispatch(reconcile,"build_queued_upstream")，扫描重claim后重 Resolve（幂等读，无副作用） |

phase 判定数据源：新 repo 方法 `LoadDispatchExecutionState`（一次快照读全部证据）。

## 2. Root baseline 四陷阱与本设计对策

1. **PersistResolvedBuild 自开 TX 与 seal 分离有 crash 缝** → 新方法
   `PersistResolvedAndSeal`（A2 新文件 `postgres/scheduling_exec_repo.go`，同包可复用未导出
   `persistResolvedBuild`）：**一个 FencedWrite TX** 内 persist jobs/job_outputs/deps +
   `UPDATE dispatches SET expected_outputs/expected_nodes`（封存）+ `UPDATE builds SET
   state='BUILD_RUNNING', started_at=NOW()`。BuildQueued 无 Jobs → 不进此方法（P-queued），
   空/未封存集合不可能被当成功（FinishDispatch 全称校验照旧）。
2. **LoadPlan 不恢复 job.state、Execute 全 WAITING** → 仅 P1 才 Execute；P2 一律
   assessment/unknown park（满足 V2 crash/V6）；已持久 jobs 永不二次 Resolve。
3. **executor 吞 audit 错误、repo.Record CAS 0 行 nil** → 失租拦截不靠 audit：
   - 发布硬闸：`RecordPendingReceipt` 本身是 FencedWrite —— POST 前必须成功写入 pending
     receipt，失租即拒绝发布；handoff 前另有 `AssertDispatchFence`（空 FencedWrite）。
   - fenced audit sink 仅做观测对齐 + 失租时 cancel run ctx（节点随后因 ctx 取消停止）。
   - 终态由 FinishDispatch 同 TX 独立核真实状态（receipt gate + 全称完整性，M2 冻结）。
   - 手动路径语义不变（不加 key、不写 receipt、pool audit 照旧）。
4. **普通 committer/repo.Commit 会把未知发布当普通失败补偿清走** → scheduled 专用
   `ScheduledOutputCommitter`/`ScheduledTransactionManager`（`handler/scheduled_committer.go`）：
   - 未知结果（transport 错/非 2xx/2xx 但 Status≠COMMITTED 或 DatasetID 不匹配）→
     `MarkReceiptUnknown`（fenced）+ 返回哨兵 `ErrScheduledPublishUnknown`；runner 据此将
     dispatch 置 unknown_publish（保留 activity/lease 语义），**不走** failed 补偿。
   - `ScheduledTransactionManager.Abort` 拒绝 abort 任何已有 receipt 行（pending/unknown/
     committed）的输出 —— executor 的自动 rollback 无法清走未决发布；仅对无 receipt 的
     reservation 做远端 abort + job_outputs 标记（手动 repo.Abort 只标记不远端 abort，A2 版补齐）。

## 3. canonical 链与五步 handoff 调用路径（P0-clean / P1）

```
claim(fresh owner/fence) → fresh dispatch + CURRENT grant
 → 执行前再核（runner 层，非 repo）：
     grant: id==d.GrantRef && generation==d.GrantGeneration(严格) && state=active &&
            未过期 && pipeline/tenant(kind+id) 匹配 && subject 非空
     JWT: TokenCipher 解密 → ValidateGrantToken(jwt,cfg,now)（签名/exp/sub）→
          derived tenant == dispatch tenant && sub == grant.Subject（org 票/personal 无票域匹配）
     definition: LoadDefinitionBinding → enabled/rev/pipeline/branch/grant_ref 五项 == dispatch
     任一不过 → FinishDispatch(failed,"<reason>")（已 admit 的机会无法执行，如实 failed；
     skipped/blocked 保留给 admission，且 failed 才释放 activity slot）
 → ctx = ContextWithTenantScope(org:{&org,sub}|personal:{nil,sub}) + ContextWithSchedulerBearer(解密JWT)
 → resolver.PreflightBuild（确定性错误 → MarkBuildFailed + FinishDispatch(failed)）
 → admission 已同TX建Build；执行不再调用OpenBuild
 → fenced resolve intent（remote RPC前）
 → resolver.ResolveBuild(args{BuildID=d.BuildID,...}, recorder=repo)
     err → MarkBuildFailed + FinishDispatch(failed)（resolver 内部已做本次开txn的补偿）
     BuildQueued → P-queued park
 → PersistResolvedAndSeal（单一 FencedWrite：jobs+job_outputs+deps+expected sets+BUILD_RUNNING）
 → repo.LoadPlan(ctx) → runner 内补齐每个 output 的 OutputNodeID=node.ID(job_spec_rid)、
     Branch=d.BuildBranch、PipelineRID=d.PipelineRID、CreateIfMissing=false
 → executor.Execute(runCtx, plan, nodeRunner, scheduledTxManager, scheduledCommitter, fencedAudit)
 → 终局映射（§4）
```

**五步 handoff（decision §4-⑩，scheduled-only；节点首个 keyed commit 前）**：

1. `AssertDispatchFence`（失租即止）→ 对该 dataset/branch 的 reservation txn 请求 abort；
   确认集合 = abort 成功(nil，含 already-aborted 幂等) | NotFound(404) 且我方持有
   build_pending_output_txns 持久记录。**不用 CleanupOpenedTransactions 做 handoff**（它会
   释放锁）；pipeline 输出锁与活动占位保持。
2. abort 未获确认（含 409-COMMITTED 异常/传输错/超时）→ 不发布：该节点失败，runner 将
   dispatch 置 unknown_publish（同 branch 单 OPEN 亦 fail-closed）。
3. 重复 handoff 幂等（同确认集合处理）。
4. usableACK（2xx 且 `Transaction.Status=="COMMITTED"` 且 `DatasetID` 匹配）→
   `CompleteScheduledPublish`（单一 FencedWrite：receipt→committed + job_outputs.
   transaction_rid=**真实 COMMITTED txn** + committed=TRUE + 清该 reservation 的 pending 记录；
   原 reservation RID 留在 receipt/provenance）。
5. 重放/恢复只允许同 key + 库内**原始 bytes** 重 POST（P2 reconcile 路径），禁止新 key、
   禁止重跑节点；Resolver 重进不再开新输出 txn/锁（P0-dangling 走既有 pending 清收）。

发布前序（每次 keyed commit）：body = `datasetOutputCommitBody(tx,result)` +
`idempotency_key=DerivePublishKey(d.EventID,nodeID)`（同包嵌入结构体，单次 marshal 定字节）
→ `RecordPendingReceipt`（fenced，先持久后 POST）→ POST → ACK 分类（§2-4）。

## 4. 终局映射（FinishDispatch 前的 receipt 扫描优先于 executor outcome）

按 dispatch 当前证据（`LoadDispatchExecutionState`）：

- 任一 receipt pending/unknown（且本轮 reconcile 后仍未确认）→ **unknown_publish**（不发
  outcome，slot 保留）。
- expected sets 未封存 或 jobs 空 → unknown_publish（不可能 vacuous 成功）。
- 全部 job COMPLETED + 全部 expected（node,dataset）receipt committed → 释放各 dataset
  锁（build 作用域 ReleaseLock；失败仅记日志，锁残留会以 LockHeld 显性阻塞，无静默腐败）
  → FinishDispatch(succeeded, extra=outcome enqueue)。
- 存在 job FAILED/ABORTED 终态且无未决 receipt → 先对**无 receipt** 的剩余 reservation 跑
  CleanupOpenedTransactions（确认 abort+释放锁）；全部确认 → FinishDispatch(failed, reason,
  extra=outcome)；任一未确认 → unknown_publish。
- jobs 不完整（WAITING/僵 RUNNING）且无未决 receipt（root 陷阱 2 口径）→ unknown_publish
  park（"execution_incomplete"），不判 failed、不续跑。
- FinishDispatch(succeeded) 返回 ErrFinishIncomplete → 降级 unknown_publish（不放宽）。

outcome 事件：仅 succeeded/failed/skipped/blocked 终态、经 FinishDispatch extra 同 TX
`outbox.Enqueue(BuildOutcomeEnvelope(...))` 一次；unknown_publish/reconcile 永不发。

## 5. P2 reconcile 流程（unknown_publish/reconcile 或 executing-过期被 ReconcileDispatch/Claim 接管）

1. 快照 → 先对每个 pending/unknown receipt：同 key + `request_body` 原字节重 POST（bearer=
   解密 JWT；handoff 早已确认或 receipt 本身证明已进入发布）；usableACK →
   CompleteScheduledPublish；否则保持 unknown。
2. 对仍 OPEN 的 pending txn 记录（handoff 后未清的）以 abort-幂等确认清收（不动锁）。
3. 回到 §4 终局映射。

## 6. 生命周期接线（server/main，默认 Off）

- `cfg.Scheduler.Enabled=true` 时：`NewDialer(cfg.Scheduler)`（mTLS/SSL，未知协议已由 config
  拒绝）→ `kafka.NewReader`（Topic=TopicTriggers，GroupID，**CommitInterval=0 仅显式提交**）
  → `scheduling.KafkaSource`（FetchMessage/CommitMessages；**nil value tombstone 返回非 nil
  空字节** → 按 bad JSON 进毒消息计数/DLQ，不当作"无消息"）→ `NewConsumer(...)` +
  `ExecutionWorker` + canonical runner。
- **init 任一步失败 → 进程退出（exit 1），不允许"日志报错仍健康"**（config Validate 已
  fail-closed，接线层同口径）。
- shutdown：ctx cancel → 等 consumer/worker goroutine 退出 → `reader.Close()`（释放
  partition/连接）；专用 scheduling pool 与 reader 均在 runtime Stop 清理，不泄漏。
- owner = `pipeline-build-service-scheduler@<hostname-uuid>`，Consumer 与 Runner 同一 owner
  实例（fence 一致）。

## 7. 归属与不碰清单（A2 期间）

- A2 新文件：`postgres/scheduling_exec_repo.go`、`handler/scheduled_committer.go`、
  `scheduling/runner.go`、`scheduling/kafka_source.go`、`scheduling/runtime.go`、对应测试、
  `main.go` 最小接线 diff、本文档。
- 不碰：consumer.go、grant.go、kafka_adapter.go、scheduling_repo.go、其测试、全部既有
  migration、schedulingapi/**、libs/event-scheduler/**、infra/**、libs/schedule-events/**
  （决策文档如需更新只补 A2 实现接口段）。

## 8. 状态（诚实声明）

- 实现/测试状态见 §9（随进度更新）；V1–V8 真实验收仍归 B（真实 Kafka/CDC/JWT/两周期），
  A2 只提供构建配置/种子 API/测试范围故障注入。
- 已知未完成项在 §9 列出，不宣称 WP3.2 完成。

## 9. 进度日志

- [ ] scheduling_exec_repo.go（同 TX persist+seal+RUNNING、fenced audit、快照、
      CompleteScheduledPublish、AssertDispatchFence、LoadDefinitionBinding）
- [ ] scheduled_committer.go（keyed committer/txn-manager/哨兵/NewSchedulerNodeRunner/
      ContextWithSchedulerBearer）
- [ ] runner.go（相位机 + 五步 handoff + 终局映射 + outcome extra）
- [ ] kafka_source.go / runtime.go / main.go 接线
- [ ] 单测 + 独立 PG integration（ofwp32-a2-* 前缀，证据 /private/tmp/ofwp32-a2-evidence）
- [ ] 全相关包 build + 既有测试不回退

WP3.2 implementation correction: dataset output publish keys use `DeriveOutputPublishKey(eventID,nodeID,datasetRID)`: `sch:` plus domain-separated UUIDv5 over a JSON tuple. Receipt node_id remains the JobSpec RID. Retries preserve the key and original bytes. This supports several datasets per node with per-dataset atomicity only. The legacy DerivePublishKey API and event/schema bytes remain unchanged.

### Sol continuation evidence (2026-11-17)

- Targeted scheduling/handler/postgres package tests passed; HTTP replay test exercises a real httptest server returning 503 then COMMITTED, checks original bytes and caller bearer, and confirms once.
- `go test -tags integration ./services/pipeline-build-service/internal/postgres -run TestPG_ -count=1` passed against isolated PostgreSQL 16 (`ofwp32-a2-pg`), including RUNNING-node receipt recovery with persisted computation evidence. Two runs: 20.791s and 19.084s.
- Whole PBS `go build -p 1 ./services/pipeline-build-service/...` passed with pinned dependencies. No go.mod/go.sum change.
- Resolver pre-RPC durable intent now parks unresolved recovery; Resolve errors conservatively retain unknown activity because an open ACK may have been lost. Node entry/result gates verify fence and fresh authorization. Executor context derives from tenant/bearer execution context.
- Publish result evidence and pending bytes share a fenced transaction; reconciliation can recover RUNNING/FAILED jobs with computation evidence after all node output receipts confirm. Transaction RIDs use canonical RID form and update both job_outputs and jobs arrays.
- Remaining: end-to-end canonical Runner/real DVS lost-openACK and crash-window tests; one JobSpec→two Dataset actual commit regression; V1–V8 Kafka/CDC/product evidence. These are not implied by the package/PG checks above.

Further corrections: scheduled executor compensation is deferred to canonical durable finalization (no stale initial receipt-map decision, no pool-bound Metadata.Abort); handoff cache is synchronized. HTTP regression now commits two independent dataset outputs concurrently under one JobSpec and checks independent keys. Explicit upstream BuildQueued clears resolve intent under the fence; arbitrary empty plans and failed Resolve retain it.

Final local checkpoint before runtime canonical integration:
- `TestPG_*` full PostgreSQL16 suite passed 19.378s with fresh current-authorization rejection, two-output RUNNING receipt recovery, and stale-owner pending/lock writes rejected.
- Scheduled resolver bookkeeping uses `ScheduledBuildState`: lock acquisition and pending record/clear run in actual FencedWrite transactions. Cleanup retains output locks; confirmed terminal FinishDispatch releases build locks in the same transaction as outcome enqueue.
- Actual executor first-publish unknown regression observes zero remote/metadata compensation calls. Concurrent two-dataset commits and exact-byte/caller-bearer HTTP replay pass. Scheduled handler race tests pass 1.021s after task-container-only compiler installation.
- Explicit upstream BuildQueued clears intent; unexpected zero-job plans and Resolve errors retain unknown intent. Runtime integration binary exported to `/private/tmp/ofwp32-a2-evidence/pipeline-build-service` (linux/arm64); source frozen pending runtime V1–V8 evidence. Complete canonical product-level evidence remains outstanding, coordinated with runtime worker.

Frozen v2 binary SHA256: `a48ead17bdcb1f035322ec215c359a86c6c466c4355ae6a2f87240ec2eb10513`; execution no longer repeats OpenBuild or pool MarkBuildFailed: admission creates the Build, fenced FinishDispatch owns terminal Build updates.

Reservation provenance is retained in the exact pending request bytes: `datasetOutputCommitBody` sets `provenance.transaction_rid` to the reservation transaction RID. Inspect `convert_from(request_body,'UTF8')::jsonb #>> '{provenance,transaction_rid}'` on `scheduling.publish_receipts` to recover it after jobs/job_outputs switch to the COMMITTED RID. No additional provenance schema is required.

v3 freeze: `/private/tmp/ofwp32-a2-evidence/pipeline-build-service-v3`, SHA256 `c384b635c151a9cc130b85a41d39f18cd65a6e8af219c5c74f518fc4aed3d4a9`. Consumer grant read failures stop the claimed iteration; Runner independently fetches the current grant and uses that exact verified object for tenant/subject context. Actual Run regression covers transient read failure then nil-initial-grant recovery with the correct subject. Scheduling tests passed 7.728s; binary build passed. Runtime verified both publish tables agree; canonical payload requires `logic_payload.transform_type`, not a `kind` alias. Identity product fixture uses `dataset_input`; filter requires a dependency node.
