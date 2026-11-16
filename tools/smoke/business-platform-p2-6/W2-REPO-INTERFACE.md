# W2 → 执行器/恢复工位（W3/W4）冻结仓库接口（v2.1 + review-round1 修复）

状态：**W2 已实现并纯单测/race 验证；真实 PG 集成未执行**（14 组 required names 见文末，W6 运行新隔离 PG）。本文件是 Connector `internal/repo` 对 W3/W4 的唯一稳定对接面；变更需回 W2。依据 `root-decisions-v2.md` v2/v2.1 + `review-w2-round1.md` items 1–7（已全部修复落地）。

包：`repo`。锁序恒定：Source→Def→(CP，仅增量)→Run→Intent。

## 0. 旧入口对 v1 全部封死（review-round1 item 3，W3 不得调用）

- `BeginPublishGate` / `BeginIncrementalPublishGate` 对 p26-v1 Run 返回 **`ErrRecoveryRequiresRecoverableGate`**（零写）；`CompleteIncrementalSyncRun` 对 v1 返回 **`ErrRecoveryRequiresFinalize`**（裸 txn UUID 不是 receipt 证据）。legacy（protocol NULL）路径原语义不变。
- 普通 `TakeoverExpiredSyncRun` 已拒一切 v1；v1 的接管只能走 `ClaimRecovery`。
- `ClaimSyncRun`/`ClaimRecovery`/gate 的全部 RETURNING 与公开 run 投影**都携带 `RecoveryProtocol`**（`models.SyncRun.RecoveryProtocol`，`json:"-"` 不上 wire）——W3 以 `claim.Run.RecoveryProtocol == models.RunRecoveryProtocolP26V1` 分派，永不为空串误判。

## 1. 协议冻结（admission 自动完成，W3 不参与）

- source config `sync.recovery.enabled=true`（typed，rest_api；**不要求 incremental**）→ admission 冻结 `sync_runs.recovery_protocol='p26-v1'` + `recovery_source_id` / `recovery_output_dataset_id` / `recovery_binding_fingerprint`（增量另冻结 cp_* 不变）。旧 Run 永远 NULL，不回填。
- 增量 v1 与 snapshot v1（cp_mode 空）都走本接口；mode 由 run 冻结列路由，W3 不得用当前配置判断。

## 2. 执行器（W3）调用序列 — 正常路径

```go
// 拉取完成后、发送前（一次序列化，永不重排）：
mat := repo.FrozenPublishMaterial{
    SourceID: connID, SyncDefID: job.ID,
    Branch: "main" /* 或 Def 冻结分支 */, LogicalPath: "sync-<run>.ndjson",
    ExactBody: bodyBytes,               // 含 idempotency_key=DerivePublishKey(runID)
    CompletedCheckpoint: &candidate,    // 增量必填；snapshot 必须 nil
    ContentHash: sha256hex(ndjson), Rows: r, Bytes: len(ndjson),
}
// gate：intent 与 publish_entered_at 原子同生；任何 zero-write outcome 不发送
res, err := h.Repo.BeginRecoverablePublishGate(ctx, lease, tenant, mat)  // 通用入口
// res.Outcome: entered(发送) / cancel_requested(aborted) / binding_stale / fenced / owner_lost
// 增量等价 wrapper: BeginRecoverableIncrementalPublishGate(ctx, lease, t, mat)

// 每次发送（含 manual）之前，必须先消费预算（上限 3，跨重启持久，manual 不豁免）：
b, err := h.Repo.ConsumePublishSendBudget(ctx, lease, tenant)
// b.Allowed=false（ErrIntentNotMutable）⇒ 绝不再发送；只能查证/finalize
// 发送使用 intent.ExactBody 原字节（GetPublishIntentMaterial）。多次 30s 发送间必须
// RenewSyncRunLease 续租（失租 sticky stop）。

// 可用 ACK（200/201 均 OK）后：
fin, err := h.Repo.FinalizeRecoveredRun(ctx, lease, tenant, receipt)
// fin.Outcome: finalized_committed | already_resolved（重复幂等零写）
// 增量等价 wrapper: FinalizeRecoveredIncrementalRun
```

## 3. 发送失败 / 未知 — W3 不得终态化

v1 Run 门后 unknown **保持 running**。`CompleteLeasedSyncRun`/`FailIncrementalSyncRun` 对 v1 门后 failed/aborted/success 一律拒绝（`ErrRecoveryPostGateLocked`/`ErrRecoveryRequiresFinalize`，零写）。W3 只能：

```go
// 依赖错误（可重试发送）：
h.Repo.RecordRecoveryWait(ctx, lease, tenant, repo.RecoveryWaitSendDeferred, backoff)  // 不耗预算
// 下轮先 ConsumePublishSendBudget 再原字节重放
// 缺 fresh auth / 远端不可达 / receipt OPEN：
h.Repo.RecordRecoveryWait(ctx, lease, tenant, repo.RecoveryWaitAuthRequired|VerifyUnavailable|ReceiptOpen, d) // 零预算消耗
```

**独立预算纪律（review-round1 item 7，已落地）**：send（总 3）与 auto query（总 5）是**两个独立计数**——一个耗尽绝不阻塞另一个；各自仅因 `finalized` 或**自身**计数达上限而拒绝（`ErrIntentNotMutable`）；计数跨 claim/wait/重启持久永不重置；manual query（`ConsumeRecoveryQueryAttemptManual`，每显式请求至多一次）在 auto 上限之上递增可观测计数、绝不绕过 send 上限；`RecordRecoveryWait` 仅被 `finalized` 拒绝（exhausted 摘要保留、计数为权威）；**真实 committed 的 finalize 在任何耗尽状态下仍成功**。W3/W4 直接把 `BudgetResult{Allowed,Attempt,Remaining}` 与 view 计数透出即可。

## 4. 恢复（W4 手动入口；自动 worker 默认关）

```go
res, err := h.Repo.ClaimRecovery(ctx, runID, tenant, holder, token, ttl)
// res.Outcome: granted | busy_live_holder | binding_stale | not_eligible | not_visible
//            | already_resolved   ← review-round1 item 6 新形状
// 覆盖：pending v1 未 claim（fresh auth 重执行）；running v1 旧租约过期（有/无 intent 均可，
//       IntentExists 区分门后[只查证/同字节重放]与门前[允许重拉]）
// res.Claim 是标准 LeaseClaim（RenewSyncRunLease 续租）；status 不被改动（unknown 保持 running）

// already_resolved（W3/W4 映射）：Run 已 succeeded 且 intent 已 finalized——只读、零写、无 claim；
//   它在当前 CP/owner/binding 状态判定【之前】识别（后续批次已推进/换了 owner 也不阻塞）。
//   W3 处理：直接以 res.Intent 的 txn/hash/计数回答幂等结果（HTTP 200 语义），绝不再 claim/finalize。
// 其余终态（failed/aborted、succeeded 未 finalized）= not_eligible 零写；ClaimRecovery 永不返回 (nil,nil)。

// fresh auth 下查证（DVS GetCommitStatus by key / by txn）：
//   自动路径每次查证: ConsumeRecoveryQueryAttempt(ctx, lease, t, reason)  // 自身上限 5
//   manual 每个显式请求至多一次: ConsumeRecoveryQueryAttemptManual(...)   // 超限计数可观测，不复位
//   ⚠ 缺 fresh auth：RecordRecoveryWait(AuthRequired)，不消费任何 attempt、不发远端
// receipt 与 intent 双层核对（纯函数，W4 先本地核对再进 finalize）：
if err := repo.ReceiptMatchesIntent(receipt, *material); err != nil { /* ErrReceiptMismatch，零写 */ }
fin, err := h.Repo.FinalizeRecoveredRun(ctx, claim, tenant, receipt)  // 预算耗尽状态仍允许真实 committed 收敛
```

## 4b. 自动恢复 seam（root 授权 auto-recovery-seam.md；W3 loop 默认关）

```go
type RecoveryScanKey struct{ ReadyAt time.Time; RunID uuid.UUID }
type RecoveryCandidate struct{ RunID, SyncDefID, SourceID uuid.UUID; ReadyAt time.Time }
r.ListRecoveryCandidates(ctx, hint, after *RecoveryScanKey, limit)  // lockless keyset (ReadyAt,RunID)，页≤100
r.ClaimAutomaticRecovery(ctx, runID, arm *repo.ScheduleArmClaim, holder, token, ttl) (*RecoveryClaimResult, error)
```

- 扫描仅返回 ids+ReadyAt（无 body/key/candidate/token/org/凭据）；hint 只缩页；含 pending v1 与 running-过期（`ReadyAt=max(next_action_at, lease_expires_at)`）；排除 terminal/finalized/未到期 next_action_at/auto-query 耗尽；**send 耗尽但 query 有余仍候选**；无 cron/schedule_enabled 过滤。
- `ClaimAutomaticRecovery` 是自动采纳**唯一**入口：P24 grant 谓词（`matchesArm`/`boundToLockedDef`/`grantDomainMatches`+90s liveness+DB 时钟）在**同一** Source→Def→(CP)→Run 事务内、共享 `recoveryClaimDecision` 核心之前裁定；额外要求 `arm.ExpiresAt == DB ExpiresAt` 且未过、run 原始 `requested_by == arm.Actor`、冻结 parents/target 匹配 arm。**绝不**先验 grant 再裸调 ClaimRecovery；绝不建 run/tick/cursor/admission 写。
- 拒绝均为 typed 零写：`auth_required`（无 grant/错 boot/revision/domain/actor/本地或 DB 过期/心跳陈旧/被 revoke）与 `not_due`（锁后 next_action_at 未到或 auto-query 耗尽）；无 grant 材料外泄。缺 fresh arm 时 worker 只聚合 skip——零远端、零预算消费、零 Run 写。

## 5. 安全投影（HTTP 响应用）

- `GetPublishIntentView(ctx, runID, t) → *PublishIntentView{Protocol,State,LastWaitReason,NextActionAt,SendAttempts,SendBudget,QueryAttempts,QueryBudget,BodyBytes,CheckpointLen,TransactionID,ResolvedAt}` — 无 body/candidate/binding/key。
- `GetPublishIntentMaterial` 仅 repo 内部与 W3 发送（不得序列化进 JSON/日志/错误）。
- `DerivePublishKey(runID)`、`RecoveryBackoff(n)`、`RequestFingerprintOf(body)`、`ValidateFrozenPublishMaterial(runID, mat)`（实际 base64 解码+sha256+size+lineage 交叉核验）、`ValidatePublishCandidateForMode(cpMode, cand)`（增量非空合法 / snapshot NULL）。

## 6. W4 coordinator 约束（Root v2 item 9 / v2.1）

- recover 用独立 ticket，本地同 Run 原子去重；排队失败/过期/Stop/claim 失败只释放自身 ticket/slot，**不得**调用 `RequestCancelSyncRun` 取消 v1 Run（无 generation 的取消被 v2.1 禁止；pending v1 须可后续 fresh auth 恢复）。
- 普通 `TakeoverExpiredSyncRun` 已拒 v1（W2 已加谓词）；v1 的接管只能走 `ClaimRecovery`。

## 7. 关键类型（frozen）

```go
type FrozenPublishMaterial struct {
    SourceID, SyncDefID  uuid.UUID
    Branch, LogicalPath  string
    ExactBody            []byte      // ≤ MaxPublishIntentBodyBytes(64MiB exact JSON body 上限)
    CompletedCheckpoint  *string     // 增量非空；snapshot nil
    ContentHash          string      // sha256(NDJSON)
    Rows, Bytes          int64       // Bytes == len(NDJSON) == len(base64 解码后)
    CPMode               string      // gate 从冻结 run 行填充，executor 不设置
}
type PublishReceipt struct {
    IdempotencyKey, RequestFingerprint, Branch, Status, MetadataContentHash string
    DatasetID, TransactionID, MetadataSyncRunID, MetadataSourceID, MetadataSyncDefID uuid.UUID
}
type BudgetResult struct{ Allowed bool; Attempt, Remaining int }
type RecoveryClaimResult struct{ Outcome RecoveryClaimOutcome; Claim *LeaseClaim; Intent *PublishIntentMaterial; IntentExists, CancelRequested bool }
type RecoveryFinalizeResult struct{ Outcome RecoveryFinalizeOutcome; Run *models.SyncRun }
```

**Receipt 身份（Root 纠正后的最终方向，W3 必须遵守）**：`PublishReceipt` 的**全部 10 个字段一律映射自实际 DVS runtime receipt 的返回值**——`IdempotencyKey=r.IdempotencyKey`、`RequestFingerprint=r.Fingerprint`、`DatasetID=r.DatasetID`、`Branch=r.Branch`、`MetadataContentHash=r.ContentHash`、`MetadataSourceID/SyncDefID/RunID=r.Lineage.SourceID/DefID/RunID`、`TransactionID=r.TxnID`、`Status=r.Status`（COMMITTED 才可 finalize）。**绝不从 intent 补齐任何缺失或不同的 receipt 字段**——拿期望伪装回执会掩盖真实错 ACK。intent 只在 repo 侧参与：`ReceiptMatchesIntent` 拿实际 receipt 与冻结 intent **独立比较**，任一字段错配 → `ErrReceiptMismatch` 零写。DVS receipt 缺字段（如旧版本未回 branch/lineage）就是错配/不可 finalize，不得修补。

**Gate 双亲锚定（review-round1 item 5）**：gate 在任何 INSERT 前用可信 pre-read 校验 `mat.SourceID/SyncDefID == run 冻结双亲`，错配返回 `ErrRecoveryMaterialParentMismatch`（零写）——W3 构造 material 时直接用 run 行冻结字段即可，不得取自当前配置。

错误（typed，零写语义）：`ErrIntentNotMutable`（finalized 或**自身**预算达上限）、`ErrRecoveryIntentMissing`、`ErrReceiptMismatch`、`ErrLeaseLost`、`ErrRecoveryRequiresFinalize`（v1 success 必须走 finalize；`CompleteIncrementalSyncRun` 对 v1 全拒）、`ErrRecoveryRequiresRecoverableGate`（旧 gate 入口对 v1 全拒）、`ErrRecoveryPostGateLocked`（v1 门后 failed/aborted 拒绝）、`ErrRecoveryMaterialParentMismatch`、`ErrRecoveryRunsActive`（SetCredential/file_glob 有效变更 fence）。

## 8. W6 required PG 测试名（18 组，`internal/repo/{recovery,auto_recovery,recovery_commit_ackloss}_integration_test.go`，`-tags=integration`）

1. `TestPGP26AdmissionFreezesBothModes`
2. `TestPGP26GateIntentAtomicRollback`
3. `TestPGP26GateRefusalLeavesNoIntent`
4. `TestPGP26ClaimRecoveryContentionAndShapes`
5. `TestPGP26TakeoverAndCompleteBypassRefused`
6. `TestPGP26BudgetsAcrossClaimsTotalCaps`
7. `TestPGP26ReceiptMismatchZeroWrite`
8. `TestPGP26FinalizeAtomicRollback`
9. `TestPGP26LaterBatchDuplicateFinalizeZeroWrite`
10. `TestPGP26CredentialFileGlobFenceAndLegacy`
11. `TestPGP26MigrationEmptyLegacyReplay`
12. `TestPGP26OldPublicEntriesRefuseV1`
13. `TestPGP26ClaimTerminalShapes`
14. `TestPGP26GateMaterialParentMismatchZeroWrite`
15. `TestPGP26AutoRecoveryScanKeysetAndFilters`（auto seam）
16. `TestPGP26ClaimAutomaticRecoveryGrantMatrix`（auto seam；含 foreign-tenant→not_visible 独立断言）
17. `TestPGP26AutoRecoveryBlockedThenRevoke`（auto seam）
18. `TestPGP26FinalizeWireCommitAckLossOnce`（wire ACK-loss：本地 PG COMMIT 到达服务器而 ACK 丢失；incremental+snapshot；机制纯测试 `TestAckLoss*` 无 tag 常规跑）

首轮真实 PG（of-p26-pg 16.15）：**15/17 绿**（Go1/wrapper2/source0 漂移；race 按条款未跑）。两处失败为测试 fixture 问题已窄修（Contention 双子例改 application_name 发现+递归阻塞链匹配、Budgets reverse 独立 fixture），断言未弱化；失败原件 `results/w6/pg/test-connector-normal_20261115T042550Z/01-go-test.raw` 保留。#18 为新增 wire 证明，待 W6 纳入 cohort 后执行。

源码 SHA 清单（W2 v2.3）：`results/w2/SOURCE-MANIFEST.txt`（v1 冻结副本 `SOURCE-MANIFEST.frozen-v1.txt`）。
