# P2.5 Implementation Contract — 精确实现契约 v1.2.1

- 日期：2026-11-14。作者：ZCode GLM-5.3（Primary 轨道）。状态：**实施中**（root `implementation-release.md` 放行；v1.2 纳入 source/repo round1 九条修正）。
- **v1.2.1 追加（review-source-round2 三个生产补丁＋测试微修，已落码并随 attempt20 冻结 BUSINESS 候选）**：
  1. **F1（cancel 租户占位符）**：`RequestCancelIncrementalSyncRun` 的 Def 锁 SQL 已绑定 `$1=def/$2=source`，租户谓词占位符改为从 **$3** 起（原 argPos 2 会把 sourceUUID 绑成租户值——真实 PG 才暴露，unit mock 不可见）。
  2. **F2（scheduled tick 真实 run id）**：`ClaimDueScheduleSlot` 的 enqueued tick 引用 Acquire 实际返回的 `run.ID`（Acquire 内部自铸 uuid，旧外层预铸 id 会 FK 23503 回滚整个增量调度事务）；不变式＝tick.run_id==checkpoint owner==returned Run.ID（两路径统一）。
  3. **F3（probe 逐层 RawMessage）**：`sourceIncrementalEnabled` 改 map[string]json.RawMessage 逐层探测——非对象 root/sync、缺增量块、非布尔 enabled 一律 legacy off（false,nil 永不报错）；仅显式布尔 `true` 激活全量 REST 校验（已启用但畸形仍 fail closed）。冻结 P23 用例的 `{"sync":"x"}` 旧源准入保持不变。
  4. **测试微修**：bearer 正控改**同名 key**（`query_param==request_param=="since"` 须放行；api_key 同名仍拒）——旧无条件碰撞 bug 在此正控下才会真失败。
  5. PG 侧待补（PG 轨道）：pending-cancel 跨父/跨组织＋释放回归；增量 scheduled **enqueued 正向**用例（现仅 busy/stale/contention）；legacy 正控 ControlMode==legacy、CPMode 为空的断言修正（PG 归属，Primary 不改 PG 文件）。
- **v1.2 变更（review-source-round1 + review-repo-round1 + docs-verification，全部已落码）**：
  1. **有损 token 解码拒绝（S1）**：json.Unmarshal 对畸形 raw UTF-8 与非法 UTF-16 代理对静默替换为 U+FFFD（两代钉定工具链均验证）——`ValidateOpaqueCheckpointValue` 拒绝含 U+FFFD 的 opaque token（窄义不支持规则，文档化）；真实 wire 畸形用例（lone surrogate escape、raw 0xff 字节）入单测。
  2. **循环错误脱敏（S2）**：增量路径的分页循环错误不再回显重复 cursor（可能等于/包含已确认 checkpoint 材料）；旧快照措辞逐字保留；脱敏用例入单测。
  3. **api_key 真实门控（S3）**：Source 级 query 冲突校验仅对 `auth.type==api_key`（镜像 `applyToQuery` 实际行为）；bearer 残留 query_param 不再误拒。
  4. **legacy 旁路防御（S4）**：`Repo.RunSyncJob`（legacy 无租约路径）在 Source 锁下探测到启用增量即 typed 拒绝 `ErrIncrementalRequiresControlled`（零 run/零 stamp）；`CompleteSyncRun` WHERE 增加 `r.cp_mode IS NULL`。
  5. **轻量 probe（S9）**：`sourceIncrementalEnabled`（Source 锁下、无全量 REST 解析）决定模式——`{}` 配置与非 rest_api 源返回 false，P21/P23/P24 旧行为逐字节不变；仅 probed-true 源进入 `readIncrementalBinding` 全量校验（显式启用但非法→fail closed）。
  6. **Def 锁后算指纹（R1）**：manual admission 探测只选锁型；全量绑定在 Def 强锁**之后**计算（消除 target/fileglob 写者竞态）；scheduled 同序复核。
  7. **CP cancel 补 Def 锁（R2）**：`RequestCancelIncrementalSyncRun` 实际锁序＝Source→Def(FOR UPDATE, 校验 source 绑定与租户)→Checkpoint→Run；错父链零写 NotVisible。
  8. **Fail 全守卫（R3）**：`FailIncrementalSyncRun` 在任何终态写（含 post-gate retain 分支）之前验证 owner/baseepoch/binding/target 四重；失配＝fenced 零写。
  9. **Complete 前置校验＋当前绑定（R4）**：锁前拒绝 nil TxnID/负计数/非 64-hex hash；锁下重读当前全量绑定并对比 captured run 与 checkpoint 双方（绝不只比 old fp==old fp）。
  10. **Claim 溯源（R5）**：claim 验证 owner+当前绑定==captured==stored+base epoch+output target；实际输入 token＝冻结的 `run.cp_base_value`（无确认值时 admission 已持久化 InitialValue 进该列；CP 保持 NULL/epoch0；""＝absent sentinel），绝不从现行 config 重推导。
  11. **run-def 断言＋基线锁后重读（R6）**：`withDefCheckpointRunLock` 的 run SELECT 断言 `sync_def_id` 匹配锁定 Def 并重读 cp 基线与 pre-read 比对（`errCapturedBaselineMismatch` 零写围栏）。
  12. **有界回滚（R7）**：新 checkpoint 事务（含 SetCredential）一律 `rollbackCheckpointTx`（5s 独立预算），不再无界 `WithoutCancel`。
  13. **ownerView 当前状态（R8）**：`GetCheckpointView` 的 owner 观测基于当前 owner run 状态＋SQL 侧租约存活（failed 或 running+租约过期→`requires_reconciliation`；pending/活 running→""；**对外为业务状态词，内部阶段名（P26 等）不出现在任何公开 JSON/error**——措辞裁决见 review-repo-round1 附录），废弃 historical advanced_by_run 推断；owner run 读错误 fail-closed 传播。
  14. **证据纪律（S5）**：attempt15 起 .cmd sidecar 先于执行落盘；attempt1-14 的命令出处以 INDEX.md 如实标注（会话工具历史重建，非当时产物）。
- v1.1 变更（纳入 implementation-release 五组强制修正，取代 v1 冲突表述）：
  1. **保守 unknown 边界＝publish gate ENTRY**：一切释放谓词要求 `publish_entered_at IS NULL`（DB 断言）；gate 后即使未真正 dispatch 也只写诚实 failed（租约仍有效时）＋retain owner；**删除内存 sticky-dispatch 释放证明与 R5 释放分支**。
  2. **权威 mode 与入口路由**：admission 的 lockless Source config 读取只是 hint；Source 锁后重读权威 config 再选/升 Def 锁；`run.cp_mode` 是 Claim/执行/取消/终态的唯一权威（永不看现行 Source config）；公开 Repo 入口（ClaimSyncRun/BeginPublishGate/CompleteLeasedSyncRun/RequestCancelSyncRun）在取 Run-first 锁之前路由捕获的 P25 work；所有内部 pending-cancel 调用点（coordinator closeNow/TTL/Stop/缺 auth、dispatch-failure 清理、HTTP cancel）经同一 CP-aware 路由。
  3. **完整数据/身份绑定**：canonical hash 加入 file_glob 与完整有效 Source Auth 身份/配置（type、inline token/basic username/password、scheme/header/query 放置、secret-ref 名与所引凭据指纹）；明文 auth 永不持久化/发布，只存最终 hash；bare `UpdateConnectionConfig` 一并入围栏；纯 name/status/cron/enable 不拒绝不重置；多 Def 写者按 Def ID 排序加锁，**先锁 Def 再改受保护的 input/credential 行**，使 Def→CP→Run 终态可无反向 Source 锁安全重读绑定。
  4. **类型化校验细则**：InitialValue 用 `*string`（显式空串拒绝、省略/null 允许）；Source-auth query 冲突在 Source 级校验（typed roundtrip＋权威 Repo 写/admission 都调用）；completed checkpoint dot path 段校验；DB 界＝`octet_length` 1..512 原始字节；Rows/Bytes 独立 json tag。
  5. **usable ACK 与本地 COMMIT 未知**：adapter＋P25 handler 校验 exact target/COMMITTED/nonnil txn；非增量注入 port 保持兼容；adapter 单测的畸形 mock 按成功契约改为合法 fixture（不弱化新检查）；R9＝全旧或全新（**含 owner 可能已被已提交的成功清除**），返回 uncertain，不猜测修复、不自动重放。
- 其余依据：`design-draft.md` v2＋R-A/R-B/R-C。本文为唯一实现依据。

## 1. 数据库 DDL（唯一新迁移）

`internal/repo/migrations/20261116100000_sync_incremental_checkpoints.sql`：

```sql
-- P2.5 per-def incremental completion checkpoint + durable single-flight owner.
-- Additive only; replay-safe under the whole-batch advisory-lock replay.
-- No backfill/adoption for any pre-P25 row.

CREATE TABLE IF NOT EXISTS sync_def_checkpoints (
    sync_def_id         UUID PRIMARY KEY REFERENCES batch_sync_defs(id) ON DELETE CASCADE,
    source_id           UUID NOT NULL,
    output_dataset_id   UUID NOT NULL,
    mode                TEXT NOT NULL DEFAULT 'cursor',
    -- sha256 hex over canonical binding JSON (§6). Never a cursor value; never
    -- plaintext auth — only the final hash is stored.
    binding_fingerprint TEXT NOT NULL,
    -- Confirmed completion checkpoint (opaque provider token). NULL = never
    -- confirmed. Bound is RAW BYTES.
    checkpoint_value    TEXT CHECK (checkpoint_value IS NULL
                                     OR octet_length(checkpoint_value) BETWEEN 1 AND 512),
    -- Monotonic confirmed-batch counter; the ONLY ordering authority.
    confirmed_epoch     INT NOT NULL DEFAULT 0,
    -- Durable single-flight owner. NULL = free. Set only after the owned Run
    -- row exists; cleared only by §4 terminal transitions.
    active_run_id       UUID REFERENCES sync_runs(id),
    active_since        TIMESTAMPTZ,
    advanced_by_run     UUID,
    advanced_at         TIMESTAMPTZ,
    last_batch_rows     BIGINT,
    last_batch_bytes    BIGINT,
    last_batch_hash     TEXT,
    last_batch_txn      UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_def_checkpoints_owner
    ON sync_def_checkpoints (active_run_id) WHERE active_run_id IS NOT NULL;

-- Admission-frozen immutable baseline (internal; never in public projections).
ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS cp_mode                TEXT,
    ADD COLUMN IF NOT EXISTS cp_base_epoch          INT,
    ADD COLUMN IF NOT EXISTS cp_base_value          TEXT,
    ADD COLUMN IF NOT EXISTS cp_binding_fingerprint TEXT,
    ADD COLUMN IF NOT EXISTS cp_output_dataset_id   UUID;
```

## 2. 模型与运行时类型（冻结签名）

### 2.1 `internal/models/rest_api_sync.go`

```go
type RESTAPIIncrementalConfig struct {
    Enabled                 bool    `json:"enabled"`
    RequestParam            string  `json:"request_param"`
    CompletedCheckpointPath string  `json:"completed_checkpoint_path"`
    // InitialValue: presence-tracked. Omitted/null => absent (allowed).
    // Explicit "" is rejected. Opaque: never trimmed; <=512 raw bytes,
    // UTF-8, no control chars when non-nil.
    InitialValue            *string `json:"initial_value,omitempty"`
}
```

`RESTAPISyncConfig` 增加 `Incremental *RESTAPIIncrementalConfig \`json:"incremental,omitempty"\``。

`NormalizeRESTAPISyncConfig` 追加（仅 `Incremental != nil && Enabled`）：
1. `pagination.Type=="cursor"` 且 `NextCursorPath!=""`；
2. `RequestParam` trim 后非空；`CompletedCheckpointPath` trim 后非空且 dot path 各段非空（与 records_path 同规则）；
3. `RequestParam` ∉ {`PageParam`,`PageSizeParam`,`CursorParam`} ∪ `QueryParams` 键（Source-auth/凭据 query 冲突在此不可见 → §2.1b）；
4. `InitialValue != nil`：非空、≤512 原始字节、UTF-8、无控制字符、不 trim。

**§2.1b Source 级校验**（`models/rest_api_source.go`）：

```go
// ValidateRESTAPISourceIncremental checks cross-block conflicts only visible
// at Source level: the incremental request_param must not collide with the
// source auth query placement (api_key query_param) when incremental is
// enabled. Called from normalizeRESTAPISourceConfig (typed roundtrip) AND from
// authoritative Repo writes/admission after the Source lock.
func ValidateRESTAPISourceIncremental(source *RESTAPISourceConfig) error
```

绑定 canonical 输入（repo 在锁下重算；纯函数在 models，可单测）：

```go
type CredFingerprint struct{ Kind, Fingerprint string }
// IncrementalBinding is the complete canonical input set (release correction 3):
// source identity + effective AUTH identity/config + sync-relevant config +
// def-level file_glob and output target.
type IncrementalBinding struct {
    SourceID, OutputDatasetID  uuid.UUID
    FileGlob                   *string // def file_glob, nil-preserving
    BaseURL, ResourcePath, RecordsPath string
    PaginationType, CursorParam, NextCursorPath string
    Incremental                RESTAPIIncrementalConfig
    QueryParams, Headers       map[string]string
    // Complete effective auth identity: type, scheme, placements, inline
    // values, secret-ref names. Credential VALUES enter only as their stored
    // content fingerprints; plaintext never persists.
    Auth                       RESTAPIAuthConfig
    Credentials                []CredFingerprint // referenced kinds, sorted by Kind
}
func IncrementalBindingFingerprint(b IncrementalBinding) string // sha256 hex of canonical JSON
```

canonical JSON：map 键排序、`Credentials` 按 Kind 排序、`FileGlob` nil/非空区分、`InitialValue` presence 区分、无多余空白。`Auth` 序列化用显式字段序（struct 直Marshal即可，map 仅 QueryParams/Headers 需排序——用中间 struct + sort）。

### 2.2 `internal/runtime/rest_snapshot.go`

```go
type IncrementalFetchSpec struct {
    RequestParam            string
    RequestValue            string // current confirmed token or InitialValue
    HasRequestValue         bool   // false => omit the param entirely
    CompletedCheckpointPath string
}
// RESTSnapshotOptions += Incremental *IncrementalFetchSpec
// SnapshotResult    += CompletedCheckpoint string; CompletedCheckpointLen int

var (
    ErrIncrementalCandidateInvalid = errors.New("rest sync: completed checkpoint candidate is missing or invalid")
    ErrIncrementalNoProgress       = errors.New("rest sync: records returned but completed checkpoint did not advance")
)
```

行为：每页 query 在 `HasRequestValue` 时 `Set(RequestParam, RequestValue)`（逐页同值）；分页 cursor 从空起（现状不动）；**终止分页的最终响应体**提取候选并校验（缺失/非 string/空/`octet>512`/非 UTF-8/含控制字符→`ErrIncrementalCandidateInvalid`；`rows>0 && candidate==RequestValue`→`ErrIncrementalNoProgress`；不 trim、不回显原值）。`Incremental==nil` 行为与现状逐字节一致。

### 2.3 `internal/runtime/sync.go`

```go
type DatasetCommitResult struct {
    DatasetID     uuid.UUID
    TransactionID uuid.UUID
    Status        string
}
```

adapter `CommitOutput` 解码后：`Transaction.Status != "COMMITTED"` → `DependencyError{Op:"commit_output", Detail:"transaction status is not COMMITTED"}`；`decoded.DatasetID != req.DatasetID` → `...Detail:"response dataset does not match the requested target"`；再依现有非空 txnUUID 检查。旧 adapter 单测的畸形 mock 响应按成功契约改为合法 fixture（补 status COMMITTED＋精确 dataset_id），不弱化新检查。

## 3. Repo 层

### 3.1 路由与内部投影（release correction 2）

```go
// runInternalRow is the lockless trusted pre-read used for CP-aware routing:
// cp fields + sync_def_id (+ source_id via def). Never serialized.
type runInternalRow struct {
    SyncDefID, SourceID       uuid.UUID
    CPMode                    string // "" = non-incremental / legacy
    CPBaseEpoch               int
    CPBaseValue               string
    CPBindingFingerprint      string
    CPOutputDatasetID         uuid.UUID
}
// runInternalPreRead: pool-level SELECT (run JOIN def) by id+tenant; returns
// (nil, nil) when not visible.
func (r *Repo) runInternalPreRead(ctx context.Context, runID uuid.UUID, t Tenant) (*runInternalRow, error)
```

公开入口路由（取任何 Run-first 锁**之前**）：
- `ClaimSyncRun`：pre-read `cp_mode != ""` → `claimIncrementalSyncRun`（内部实现）；
- `BeginPublishGate`：同 → `BeginIncrementalPublishGate`；
- `CompleteLeasedSyncRun`：同 → status=="succeeded" 时返回 `ErrIncrementalRequiresAdvance`（普通 complete 不得写 P25 succeeded）；"failed"/"aborted" → `FailIncrementalSyncRun`（条件释放，见 §3.6）；
- `RequestCancelSyncRun`：同 → `RequestCancelIncrementalSyncRun`。
- 由此覆盖**全部**内部 cancel 调用点（coordinator closeNow/TTL/Stop/缺 auth、`closeAfterFailedDispatch`、HTTP cancel）与 executor 终态路径，无需逐点修改。

```go
var ErrIncrementalRequiresAdvance = errors.New("incremental run success must go through CompleteIncrementalSyncRun")
```

`LeaseClaim`/内部扫描携带 cp 字段（`leasedRunColumns` 追加 `r.cp_mode, r.cp_base_epoch, r.cp_base_value, r.cp_binding_fingerprint, r.cp_output_dataset_id`；`models.SyncRun` 增加对应 `json:"-"` 字段——公开投影不变，永不序列化）。执行体用 `lease.Run.CPMode` 判定，**永不**重读现行 Source config。

### 3.2 类型与错误

```go
type IncrementalRejectionKind string
const (
    IncrementalBusy         IncrementalRejectionKind = "busy"
    IncrementalBindingStale IncrementalRejectionKind = "binding_stale"
)
type IncrementalRejection struct {
    Kind        IncrementalRejectionKind
    ActiveRunID uuid.UUID
    ActiveSince time.Time
}
var (
    ErrIncrementalOwnerUnresolved = errors.New("sync def has an unresolved incremental checkpoint owner")
    ErrIncrementalCheckpointCAS   = errors.New("incremental checkpoint CAS mismatch; nothing written")
)
```

### 3.3 Admission 段（manual/scheduled 共用）

```go
// AcquireCheckpointForAdmission runs INSIDE an open admission transaction that
// holds the Source lock and — after rereading the AUTHORITATIVE config — the
// Def FOR UPDATE strong lock. Steps: lock-or-init checkpoint row (existing:
// SELECT..FOR UPDATE NOWAIT; absent: INSERT placeholder) -> owner/binding
// verify -> INSERT pending run with frozen base -> UPDATE owner=run -> return.
func (r *Repo) AcquireCheckpointForAdmission(ctx context.Context, tx pgx.Tx,
    syncID, sourceID uuid.UUID, t Tenant, requestedBy uuid.UUID,
    binding models.IncrementalBinding) (*models.SyncRun, *IncrementalRejection, error)
```

- **manual（`EnqueueSyncRun`）**：lockless conn config 读取仅作 hint 选 Def 锁型（hint 增量→`FOR UPDATE`；hint 非增量→沿用 `FOR KEY SHARE`）；Source 锁后重读权威 config：与 hint 不符（翻转为增量）→ typed 可重试拒绝（`IncrementalRejection{}`→manual 映射 contention 429 语义／scheduled contention skip），避免锁升级死锁；权威为增量→（已是强锁或重试后强锁）`ValidateRESTAPISourceIncremental`＋重算绑定→`admissionGate`（配额先判）→`AcquireCheckpointForAdmission`→stamp 最后。
- **scheduled（`ClaimDueScheduleSlot`）**：现 Source/Def 均 `FOR UPDATE NOWAIT`（已强锁）；锁后重读权威 config 路由增量/非增量；增量 busy→活事务 `finishNoRun(TickOutcomeSkippedIncrementalBusy,"incremental_busy",next,"",&arm.Actor)`；stale→`finishNoRun(TickOutcomeErrorBindingStale,"incremental_binding_stale",next,"",&arm.Actor)`（均：真实 tick＋cron 游标前进＋不建 run＋不打成功 stamp＋不碰数据 checkpoint）；55P03（Checkpoint 行 NOWAIT）→事务已 abort→回滚＋contention skip（无 tick 无 run）。
- 新 tick outcome 常量（repo/schedule.go）：`TickOutcomeSkippedIncrementalBusy = "skipped_incremental_busy"`、`TickOutcomeErrorBindingStale = "error_binding_stale"`。

### 3.4 Claim / 发布门（checkpoint 感知；内部函数，公开入口路由进入）

```go
func (r *Repo) claimIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t Tenant,
    holder, token string, ttl time.Duration, pre *runInternalRow) (*LeaseClaim, error)
// Def(FOR UPDATE)→Checkpoint(FOR UPDATE)→Run(FOR UPDATE)＋锁后另条 DB clock；
// 验 owner==runID 且 checkpoint.binding==run.cp_binding_fingerprint==锁下重算值；
// 再执行与 ClaimSyncRun 相同的 pending→running CAS。验证失配→(nil,nil)（run 留 pending）。

type IncrementalPublishGateOutcome string
const (
    IncrementalPublishGateEntered      IncrementalPublishGateOutcome = "entered"
    IncrementalPublishGateCancelled    IncrementalPublishGateOutcome = "cancelled"
    IncrementalPublishGateFenced       IncrementalPublishGateOutcome = "fenced"
    IncrementalPublishGateBindingStale IncrementalPublishGateOutcome = "binding_stale"
    IncrementalPublishGateOwnerLost    IncrementalPublishGateOutcome = "owner_lost"
)
func (r *Repo) BeginIncrementalPublishGate(ctx context.Context, runID uuid.UUID, t Tenant,
    holder, token string, generation int, ttl time.Duration) (*IncrementalPublishGateResult, error)
// Def(FOR UPDATE)→Checkpoint(FOR UPDATE)→Run(FOR UPDATE)＋锁后 clock；
// 验证序：binding（锁下重读 Source/Auth/Def vs run 基线）→owner/epoch→lease CAS
// （BeginPublishGate 谓词逐字）。fenced/owner_lost→零写保留；cancelled/binding_stale
// →可证零 dispatch（gate 未入）→ FailIncrementalSyncRun(aborted/failed)。
```

绑定重读说明（release correction 3）：终态链持 Def 锁重读 connections/source_credentials；一切受保护写者（UpdateConnection/UpdateConnectionConfig/SetCredential）先持同 Def 锁（多 Def 按 Def ID 排序）再改行，故无反向 Source 锁亦无竞态。

### 3.5 成功 finalize（全有或全无）

```go
type IncrementalAdvance struct {
    CompletedCheckpoint string
    Rows                int64
    Bytes               int64
    ContentHash         string
    TxnID               uuid.UUID
}
func (r *Repo) CompleteIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t Tenant,
    holder, token string, generation int, adv IncrementalAdvance) (*models.SyncRun, error)
// Def→Checkpoint→Run＋锁后 clock，单事务：
//  (a) run 成功 CAS（CompleteLeasedSyncRun 谓词逐字）；
//  (b) checkpoint 推进+释放（WHERE def=? AND active_run_id=runID AND
//      confirmed_epoch=run.cp_base_epoch AND binding_fingerprint=run.cp_binding_fingerprint）。
// 任一失配→全回滚：(ErrLeaseLost / ErrIncrementalCheckpointCAS)，零部分写零释放。
// COMMIT 响应丢失→uncertain（全旧或全新，含 owner 可能已清除）；不猜测修复，不自动重放。
```

### 3.6 失败终态（条件释放；DB 断言 gate 边界）

```go
// FailIncrementalSyncRun: ordered Def(FOR UPDATE)→Checkpoint(FOR UPDATE)
// →Run(FOR UPDATE)＋锁后 clock。写 failed/aborted（lease CAS；aborted 沿用
// stage guard 要求 publish_entered_at IS NULL）。同一事务内读 run 行的
// publish_entered_at：**release 谓词恒要求 publish_entered_at IS NULL**
// （DB 断言）——pre-gate 失败→failed/aborted＋owner 释放（epoch/value 不动）；
// gate 已入（发布门失败后的取消太晚、workCtx 死亡等）→仅写诚实 failed＋owner 保留。
func (r *Repo) FailIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t Tenant,
    holder, token string, generation int, status string,
    runErr error, contentHash *string) (*models.SyncRun, error)
```

（取代 v1 的 `ReleaseIncrementalOwnerTerminal`；内存 sticky-dispatch 证明已删除——gate entry 即保守 unknown 边界。）

### 3.7 取消（checkpoint 感知）

```go
func (r *Repo) RequestCancelIncrementalSyncRun(ctx context.Context, runID, syncID, sourceID uuid.UUID,
    t Tenant, cancelledBy *uuid.UUID, cause string) (*CancelResult, error)
// Source→Def→Checkpoint→Run＋锁后 clock。
// pending→aborted＋释放（同事务；pending 天然 publish_entered_at IS NULL，仍显式断言）；
// running→仅持久化 cancel_requested_at/cancelled_by（不释放；由执行器的
// FailIncrementalSyncRun 或 post-dispatch Run-only failed 决定后续）；
// too_late/legacy/not-visible 沿用 RequestCancelSyncRun 语义。
```

### 3.8 Takeover 防后门

`TakeoverExpiredSyncRun` WHERE 追加 `AND NOT EXISTS (SELECT 1 FROM sync_def_checkpoints cp WHERE cp.active_run_id = r.id)`。

### 3.9 变更围栏（release correction 3 完整覆盖）

```go
// assertCheckpointOwnersFreeTx: defIDs 非空时在持有对应 Def 锁的事务内断言
// active_run_id 全 NULL；否则 ErrIncrementalOwnerUnresolved（列出 def/run/since；无 token）。
func assertCheckpointOwnersFreeTx(ctx context.Context, tx pgx.Tx, defIDs []uuid.UUID) error

// lockSourceDefsForProtection: 在已持 Source 锁的事务内，按 Def ID 升序
// FOR UPDATE 锁定该 source 的全部（或给定）batch_sync_defs 行，返回 defIDs。
// 受保护写者必须先持这些 Def 锁，再 UPDATE connections / UPSERT credentials。
func lockSourceDefsForProtection(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID, t Tenant) ([]uuid.UUID, error)
```

| 路径 | 围栏触发 | 备注 |
| --- | --- | --- |
| `UpdateSyncJob`（repo.go:593） | output_dataset_id 或 file_glob 实际变更 | 断言后通过；**不删不改进 checkpoint 行**（绑定下次重算失配→stale）。cron/enable/name 不触发 |
| `UpdateConnection`（repo.go:239） | config 实际变更（解析后有效子集变化） | Source 锁→lockSourceDefsForProtection（排序）→断言→UPDATE config；name/status-only 不触发 |
| `UpdateConnectionConfig`（repo.go:1280，bare helper） | 任何 config 写 | 同上（该面整写 config，必然触发） |
| `DeleteConnection`（repo.go:1085） | 任何删除 | Source 锁→锁 defs→断言→删除（owner 空闲才可删；checkpoint 随 CASCADE） |
| `SetCredential`（repo.go:377） | 任何写入 | 改 Source 锁事务：锁→lockSourceDefs→断言→UPSERT（语句照旧）；值变化→指纹变化→后续 stale；**同值重写指纹不变、零影响** |

恢复路径（文档化）：binding_stale→新建 SyncDef（newDef-only reset）；P25 无 reset API。

### 3.10 视图

```go
func (r *Repo) GetCheckpointView(ctx context.Context, defID uuid.UUID, t Tenant) (*CheckpointView, error)
```

## 4. 执行路径与转移矩阵

### 4.1 `executeSnapshotSync` 增量分支

1. claim 后从 `lease.Run` 内部字段取基线（cp_mode 为权威；非增量 run 走原路径不变）。
2. 心跳/取消轮询/beforePage 不变。
3. `FetchRESTSnapshot(Incremental spec)`：`ErrIncrementalCandidateInvalid/NoProgress`→pre-gate 失败→`FailIncrementalSyncRun(failed)`。
4. 发布前：stopHeartbeat→sticky 检查（cancelled→`FailIncrementalSyncRun(aborted)`；lease-lost→零写）→`BeginIncrementalPublishGate`：entered→继续；cancelled→abort+释放（§3.6）；binding_stale→failed+释放；fenced/owner_lost→零写保留。**gate 后、CommitOutput 前的 workCtx 死亡**：不释放——`FailIncrementalSyncRun(failed)`（release 谓词因 gate 已入而拒绝释放，owner 保留；租约失效则零写）。
5. `CommitOutput` 后 handler 校验 usable ACK：`TransactionID!=Nil && Status=="COMMITTED" && DatasetID==job.OutputDatasetID`（注入 port 同契约；不满足→Run-only failed（现 completeFailedAfterCommitAttempt），owner 保留）。非增量路径校验保持现状。
6. usable ACK→`CompleteIncrementalSyncRun`；typed 失败映射 §4.3。
7. metadata 增量块（仅长度/计数）：`"incremental":{"mode":"cursor","base_epoch":E,"input_checkpoint_len":L1,"completed_checkpoint_len":L2}`。

### 4.2 转移矩阵（增量 run，有效 owner）

| # | 场景 | run 终态写 | checkpoint | owner | 路径 |
| --- | --- | --- | --- | --- | --- |
| R1 | pull/解析/limits/egress/候选校验失败（gate 前） | failed | 不动 | 释放 | §3.6 |
| R2 | gate 前取消（pending/running） | aborted | 不动 | 释放 | §3.7/§3.6 |
| R3 | gate 复验 binding_stale | failed | 不动 | 释放 | §3.4→§3.6 |
| R4 | gate cancelled | aborted | 不动 | 释放 | §3.4→§3.6 |
| R5 | **gate 后未 dispatch 中断** | failed（若租约有效） | 不动 | **保留** | §3.6（release 谓词 DB 拒绝） |
| R6 | dispatch 后错误/ACK 不合格 | failed（Run-only） | 不动 | 保留→P26 | 现 completeFailedAfterCommitAttempt |
| R7 | usable ACK＋全谓词匹配 | succeeded | epoch+1/value/forensics | 释放 | §3.5 单事务 |
| R8 | usable ACK＋checkpoint 谓词失配 | 零写（run 留 running） | 不动 | 保留→P26 | §3.5 typed |
| R9 | 本地 COMMIT 丢响应 | 未知（全旧或全新，含 owner 可能已清除） | — | 不猜测修复 | §3.5 err |
| R10 | 失租（任意阶段） | 零写 | 不动 | 保留 | fencing |
| R11 | 旧 generation/token 晚到 | 零写 | 不动 | 保留 | fencing |
| R12 | 同 def 二次 admission | 不建 run | 不动 | 不变 | §3.3 拒绝 |
| R13 | 未决 owner 下变更/删除/takeover | 拒绝/无资格 | 不动 | 保留 | §3.8/3.9 |
| R14 | 空批次确认 | succeeded | epoch+1（value 按 §2.2） | 释放 | §3.5 |

### 4.3 handler 错误映射（增量新增）

| 来源 | HTTP |
| --- | --- |
| `IncrementalRejection{busy}`（manual） | 409 `sync_incremental_busy`（active_run_id 仅父链可见时附） |
| `IncrementalRejection{binding_stale}` | 409 `sync_incremental_binding_stale` |
| `ErrIncrementalOwnerUnresolved` | 409 `sync_incremental_owner_unresolved` |
| Checkpoint NOWAIT 争用（manual） | P2.3 既有 429 contention（零副作用） |
| `ErrIncrementalCheckpointCAS`/finalize 存储错 | 503 uncertain |

## 5. 锁序总表

| 路径 | 锁序列 |
| --- | --- |
| manual admission（增量） | Source(现策略)→[权威 config 重读]→Def FOR UPDATE→Checkpoint(NOWAIT/INSERT)→run INSERT→owner UPDATE→stamp |
| scheduled admission（增量） | Source NOWAIT→Def FOR UPDATE NOWAIT→[权威 config 重读]→Checkpoint(同上)→run→owner→tick→cron 游标→prune→stamp |
| claim / gate / 成功 finalize / FailIncrementalSyncRun | Def FOR UPDATE→Checkpoint FOR UPDATE→Run FOR UPDATE→clock→谓词/写 |
| pending cancel | Source→Def→Checkpoint→Run→clock |
| post-dispatch failed（R6） | Run-only（现 completeLeased） |
| renew/check/非增量全部现路径 | Run-only / lock-free（永不后取父锁） |
| 受保护写者（§3.9） | Source→Defs(按 Def ID 排序 FOR UPDATE)→[断言]→UPDATE/UPSERT 受保护行 |

- 时敏判定＝全部可能等待的锁拿齐后另条 `SELECT clock_timestamp()`；任何行锁不跨 HTTP。

## 6. 归一化规则

- canonical JSON：键排序、无多余空白、Credentials 按 Kind 排序、FileGlob/InitialValue presence 区分。
- opaque 值（checkpoint/initial/候选）：永不 trim；原始字节 ≤512；UTF-8；禁 C0/C1/DEL；错误/日志/metadata 只出现长度；公开 API 只出现长度＋sha256。
- 配置键名/路径（request_param、completed_checkpoint_path、pagination 参数）：trim 后使用。
- 双侧校验：typed roundtrip（`normalizeRESTAPISourceConfig`→`NormalizeRESTAPISyncConfig`＋`ValidateRESTAPISourceIncremental`）与权威 Repo 写/admission 锁下重读后校验。

## 7. API 形状（增量新增）

```
GET /api/v1/data-connection/syncs/{id}/checkpoint → 200 CheckpointView | 404/403

（root 钉正 2026-11-14：实际挂载＝Connector server.go:95+130 的 `/api/v1/data-connection` 前缀族；此前省略前缀的 `/api/v1/syncs/…` 拼法为笔误，已同步，无需 live 试错。）
```

```go
type CheckpointView struct {
    Mode             string                     `json:"mode"`
    Initialized      bool                       `json:"initialized"`
    CheckpointLen    int                        `json:"checkpoint_len,omitempty"`
    CheckpointSHA256 string                     `json:"checkpoint_sha256,omitempty"`
    ConfirmedEpoch   int                        `json:"confirmed_epoch"`
    Owner            *CheckpointOwnerView       `json:"owner,omitempty"`
    LastConfirmed    *CheckpointLastConfirmedView `json:"last_confirmed,omitempty"`
}
type CheckpointOwnerView struct {
    RunID        uuid.UUID `json:"run_id"`
    RunStatus    string    `json:"run_status"`
    Since        time.Time `json:"since"`
    BlockedReason string   `json:"blocked_reason"` // "requires_reconciliation" | "" (业务状态词)
}
type CheckpointLastConfirmedView struct {
    RunID       uuid.UUID `json:"run_id"`
    TxnID       uuid.UUID `json:"txn_id"`
    Rows        int64     `json:"rows"`
    Bytes       int64     `json:"bytes"`
    ContentHash string    `json:"content_hash"`
    At          time.Time `json:"at"`
}
```

- run 公开 JSON 追加 `incremental *RunIncrementalView \`json:"incremental,omitempty"\``：`{mode string, base_epoch int}`（由 `json:"-"` 内部字段投影，非原值）。
- scheduled tick outcome 新值见 §3.3。

## 8. 兼容性规则

1. 非增量 def/源/运行：与 P1–P24 逐字节一致；无 checkpoint 行、无 cp_* 值、无新代码路径。
2. adapter 新校验对真实 Dataset 行为等价；仅把"2xx 但非 COMMITTED/目标不符"从误当成功改为 unknown-after-dispatch 失败；旧 adapter 单测畸形 mock 改为合法 fixture。
3. handler：非增量 run 保持仅 txnUUID 非空（旧注入 port 零改动）；增量 run 强制全契约。
4. 迁移幂等可并发重放；旧 72 必需 PG 名断言冻结。
5. 旧 HTTP 套件 guard 适配＝owned wrapper（计数/归档/标注 adapted）；P24/P23 工具与 archive859 一字不动。
6. 公开 JSON 只增不改。

## 9. 文件归属（implementation-release 权威版）

- **Primary**：Connector 业务/迁移/models/runtime/repo/handlers/server/coordinator、README、非 PG 单测/race；`design-draft.md`/`implementation-contract.md`/`HANDOFF-PRIMARY.md`。
- **PG GLM**：`internal/repo/checkpoint_integration_test.go`（或另立新 P25 集成文件）、`run_pg_isolated_p25.sh`、`p25-required-names.txt`、`pg_exact_check_p25.py`、`pg-test-plan.md`/`HANDOFF-PG.md`。
- **Tools GLM**：其余新 P25 工具/fixture/proxy/smoke/driver/guard/归档脚本＋`tools-design.md`/`HANDOFF-TOOLS.md`。
- 无人可改：P1–P24 工具与归档、q3、go.mod/go.sum、旧 72 断言。

## 10. 实施顺序与验证

1. models（含单测）→runtime（含单测）→迁移→repo/checkpoint.go＋插段→handlers/server 接线→README。
2. 靶向验证（真实 cmd+rc，独立 attempt，证据存 `/private/tmp/openfoundry-p25-repair-evidence/connector/`）：`go build ./...`、`go vet ./...`（Connector 内）、非 PG `go test ./...`（排除 integration tag）、相关 `go test -race`、`go vet -tags integration ./...`（compile-only）。不碰 q3/真实 PG/HTTP。

## 11. smoke_p25.py 驱动接口（Primary 实现，2026-11-14）

- 纯检查（现在可跑）：`--plan` / `--contract-probe` / `--list-gates` / `--ast-check` / `--selftest-mocked`（进程内 fakes，无 socket）。
- 真实 HTTP（root release 后）：`--phase healthy|tail [--only hN] --released`＋env `P25_CONFIRM=yes`（双门，缺一 rc3 零网络调用）；`--evidence-dir/--result-file/--pg-proof-file`（h12 绑定 PG 真实日志，不伪造）；`--connector-base/--dataset-base/--fixture-base/--proxy-base/--fixture-src-url` 由 driver 注入（默认全部场景走 aux connector p25connector:17094，fixture http://p25fixture:8090，proxy admin :17093）。
- 输出契约：每场景 gates[]＋owner_roster/run_roster 分账；汇总含 failed 列表、finally 全源 quiet（fixture active0/started==finished＋proxy inflight0/started==finished）、attempts 汇总；attempt 记录不可覆盖（-rN）、无 Authorization/JWT 值。
