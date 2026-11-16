# P2.6 W1 — Dataset `outputs:commit` 幂等发布接口（冻结版）

状态：W1 候选实现接口说明。依据 `root-decisions-v2.md`（2026-11-15）Dataset W1 冻结决定；
覆盖旧 contract 初稿中的冲突建议。本文件只描述 Dataset 侧（dataset-versioning-service），
Connector 侧行为由主线 worker 的 contract v2 定义。

## 1. HTTP 接口

### 1.1 POST `/api/v1/datasets/{rid}/outputs:commit`（`/v1` 镜像同）

#### 1.1.1 请求

沿用现有 `CommitDatasetOutputRequest` 形状（inline `content_base64` 文件、零字节 inline
`""`、metadata-only、受控 `storage_path` 均维持现有校验，不扩展到通用可变 storage
reference 幂等），新增可选字段：

```json
{
  "idempotency_key": "of-p26-pub-v1:<runUUID>",
  "branch": "main",
  "create_if_missing": false,
  "transaction_type": "SNAPSHOT",
  "summary": "…",
  "provenance": { },
  "files": [ { "logical_path": "part-00000.ndjson", "content_base64": "", "size_bytes": 0 } ],
  "lineage_links": [ ],
  "metadata": { }
}
```

- `idempotency_key`：可选。缺省（字段缺失或空串）→ 完全走旧无 key 行为，响应中两个新
  字段不出现（`omitempty`）。
- key 格式：`^[A-Za-z0-9._~:=-]{1,200}$`（URL-path 安全、有界）；不合法 → `400`。
- keyed 请求硬性约束（`400`，事务开始前判定，纯 body 函数故 replay 判定不受影响）：
  - `branch` 必须显式非空；不做 `active_branch`/`main` 静默 fallback（active branch 陈旧
    变化不改变 keyed explicit branch）。
  - `create_if_missing` 必须为 `false`。
- 请求体大小沿用现有 `MaxOutputCommitContentBytes` 上界；raw body 有界读入。
- fingerprint 由 DVS 计算：`request_fingerprint = hex(SHA256(exact raw HTTP JSON body))`，
  **包括 key 字段在内的全部原始字节**。DVS 自行计算，不采信 caller 提供的任何 hash
  （包括 `metadata.content_hash`）；inline 对象仍按现有约束校验实际
  `size_bytes == len(content_base64)` 与 content 总量上限。语义相同但字节不同（重排
  JSON、空白变化）→ fingerprint 不同 → `409`，这是明确的 byte contract。

#### 1.1.2 响应

成功响应沿用 `CommitDatasetOutputResponse`，新增两个 `omitempty` 字段（仅 keyed 成功
时出现）：

```json
{
  "dataset_id": "…",
  "dataset_rid": "ri.foundry.main.dataset.…",
  "branch": "main",
  "transaction": { "id": "…", "status": "COMMITTED", "metadata": { } },
  "schema": { },
  "files": [ ],
  "preview": { },
  "lineage_links": [ ],
  "idempotency_key": "of-p26-pub-v1:<runUUID>",
  "request_fingerprint": "<64 hex chars>"
}
```

- first（该 `(dataset_id, idempotency_key)` 首个成功提交）→ `201`，body 为一次性序列化
  并与 receipt 同事务保存的 immutable bytes。
- replay（同 key 且 fingerprint 一致）→ `200`，**逐字节**返回保存的原始 response body，
  绝不从当前 head/index/lineage 重建，不重放任何写路径（branch/backing/lineage 存在性
  一概不查，head/index/lineage 不动）。
- 同 key 且 fingerprint 不同 → `409`，零副作用：

```json
{ "error": "idempotency key is bound to a different request body", "code": "IDEMPOTENCY_KEY_CONFLICT" }
```

- keyed 失败（校验失败、branch 不存在、open-txn 冲突、publish 步失败、ctx 取消、进程
  死）：**整体回滚**，不留 OPEN 事务、不留 staged rows、不留 receipt、不写 head/index；
  仅物理 inline 对象可能以本次 attempt 的全新 random `txnUUID` 命名空间孤儿存在（无
  引用）。重试视为新 attempt（新 `txnUUID`）。失败 attempt 不是 durable accepted
  operation；key 绑定的是首次**已 committed** 的 body。
- COMMIT ACK 丢失（unknown）→ `503`：

```json
{ "error": "output commit outcome unknown; retry with the same idempotency key", "code": "OUTCOME_UNKNOWN", "retry_after": 1 }
```

  不猜测回滚；重试同 key 同 body 或先 GET receipt。

#### 1.1.3 keyed 错误码汇总

| 场景 | 状态 | 说明 |
|---|---|---|
| dataset 不在可信 tenant scope | 404 | 与不存在不可区分 |
| 显式 branch 不存在 | 404 | 禁止 fallback 到 main/active |
| lineage 目标越 tenant | 404 | 沿用现有规则 |
| key 格式非法 / branch 缺失 / `create_if_missing=true` | 400 | 事务前判定 |
| 同 key 不同 fingerprint | 409 | `IDEMPOTENCY_KEY_CONFLICT` |
| branch 已有 OPEN 事务（无 key 路径持有） | 409 | 沿用现有冲突语义 |
| content 超限 | 413 | 沿用 |
| backing 不可用 | 503 | 沿用 `local_backing_filesystem_unavailable` |
| COMMIT ACK 丢失 | 503 | `OUTCOME_UNKNOWN`，可同 key 重试 |
| 其他 | 400/409/500 | 与旧路径同一映射 |

鉴权：可信 JWT claims（`dataset.write`）+ tenant predicate；key 与 dataset 永不跨域查询，
receipt 不存 JWT/subject —— 同 org 内对现有 dataset 有权限的 fresh 主体可合法重放。

### 1.2 GET `/api/v1/datasets/{rid}/outputs/commits/{key}`（`/v1` 镜像同）

- 返回该 `(dataset, key)` 已 committed receipt 保存的同一份 immutable response bytes，
  `200`；读取按同 tenant predicate + dataset 解析控制。
- `404` 只表示"当前未见 committed receipt"，**不能**证明没有在飞（未 COMMIT 的）提交
  操作；不区分"key 未知"与"dataset 不可见"。
- key 格式非法 → `400`。

## 2. 存储（migration `20261115100000_dataset_output_commit_receipts.sql`）

```sql
CREATE TABLE IF NOT EXISTS dataset_output_commit_receipts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id          UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    idempotency_key     TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    branch_name         TEXT NOT NULL,
    transaction_id      UUID NOT NULL,
    transaction         JSONB NOT NULL,
    response_body       TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    UNIQUE (dataset_id, idempotency_key)
);
```

- `response_body` 用 TEXT 存**精确原始序列化字节**（不用 JSONB：JSONB 会重排 key 顺序，
  破坏 byte-stable replay）。
- receipt 至少含 `dataset_id`、`branch_name`、`transaction`（`{id,status,metadata}` 快
  照）、`idempotency_key`、`request_fingerprint`；`transaction.metadata` 携带请求
  `provenance`/metadata 合并结果（含 Connector 写入的 content_hash / lineage
  Source/Def/Run），由 Connector 两层验证与 intent 一致。
- 表 insert-only：无 UPDATE/DELETE 路径；唯一键 `(dataset_id, idempotency_key)`。

## 3. Go 接口（支撑 Connector worker 并行对接）

### 3.1 repo（dataset-versioning-service）

```go
// OutputCommitTxStore 是 keyed 提交 build 回调拿到的事务内仓储视图；
// *Repo 绑定打开的 pgx.Tx 后满足该接口，测试替身用自身内存视图满足。
type OutputCommitTxStore interface {
    EnsureDefaultBranch(ctx context.Context, dataset *models.Dataset) error
    GetRuntimeBranch(ctx context.Context, datasetID uuid.UUID, branch string) (*models.RuntimeBranch, error)
    ResolveDatasetForTenant(ctx context.Context, raw string, scope TenantScope) (*models.Dataset, error)
    GetTransactionStatus(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID) (string, bool, error)
    StartTransaction(ctx context.Context, datasetID uuid.UUID, branchID uuid.UUID, branchName string, txType models.TransactionType, summary string, providence models.JSONValue, startedBy uuid.UUID) (*models.RuntimeTransaction, error)
    StageTransactionFiles(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, files []models.StageTransactionFile) error
    MergeTransactionMetadata(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, metadata models.JSONValue) error
    CommitTransactionWithPublish(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID, files []models.PutDatasetFileIndexEntry, links []models.PutDatasetLineageLink) (*PublishSnapshot, error)
}

var (
    ErrIdempotencyKeyConflict = errors.New("idempotency key is bound to a different request body")
    ErrCommitOutcomeUnknown   = errors.New("output commit outcome is unknown")
)

const DefaultOutputCommitLockTimeout = 10 * time.Second

func (r *Repo) WithIdempotentOutputCommit(
    ctx context.Context,
    scope TenantScope,
    datasetID uuid.UUID,
    key string,
    fingerprint string,
    build func(ctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error),
) (receipt *models.OutputCommitReceipt, replayed bool, err error)

func (r *Repo) GetOutputCommitReceipt(ctx context.Context, scope TenantScope, datasetID uuid.UUID, key string) (*models.OutputCommitReceipt, error)
```

`WithIdempotentOutputCommit` 内部顺序（单外层事务，首条数据语句前显式
`SET TRANSACTION ISOLATION LEVEL READ COMMITTED` —— 不依赖 server 默认；DB 接口保持
`Pool.Begin` 不变，txRepo 仍拿到 pgx.Tx）：

1. `SET LOCAL lock_timeout`（`Repo.OutputCommitLockTimeout`，零值 → 10s 默认；亚毫秒
  正值钳制为 1ms，绝不格式化成 `0ms`——PG 语义 0 = 无限等待）。
2. 语句一：可信 tenant predicate 的 `SELECT … FROM datasets WHERE id=$1 AND <tenant>
   FOR UPDATE` —— 行锁先于任何新 txn INSERT/FK 锁；跨 tenant/不存在统一
   `ErrNotFound`。
3. 语句二（独立 SELECT，READ COMMITTED 每语句新 snapshot，不与语句一合 CTE）：按
   `(dataset_id, idempotency_key)` 读 receipt。
   - 命中且 fingerprint 一致 → 返回保存 receipt（`replayed=true`），事务无写入、自行
     rollback 释放锁；**不**再查 branch/backing/lineage、不写 head。
   - 命中但 fingerprint 不同 → `ErrIdempotencyKeyConflict`（零副作用）。
4. 未命中 → `txRepo := &Repo{Pool: tx}` 执行 `build`：Start/Stage/Merge/Publish 与
   branch/lineage 读取全部经 `txRepo`（`CommitTransactionWithPublish` 在其上的
   Begin/Commit 是 pgx SAVEPOINT/RELEASE）。失败即整外层回滚。
5. 成功 → 回填 `idempotency_key`/`request_fingerprint`，**一次性** `json.Marshal`
   response，receipt INSERT 与全部发布写入同一外层事务，唯一真实 `COMMIT`。
   - `COMMIT` 返回错误 → `ErrCommitOutcomeUnknown`（不猜回滚；重试查 receipt 或原
     body 重放）。
   - 清理用独立有界 context rollback 自己的 tx；绝不调用全局 `AbortTransaction`，绝不
     abort 他人事务；等待行锁可被 ctx 独立取消。

receipt 读取 SQL 全列以 `r.` 限定；`GetOutputCommitReceipt` 的 JOIN 使用
`JOIN datasets d` 且 tenant predicate 显式 `d.org_id/d.owner_id` 限定（两表均有
`id`/`created_at`，未限定会 PG 42702）。

### 3.2 models

```go
// CommitDatasetOutputRequest 新增
IdempotencyKey string `json:"idempotency_key,omitempty"`

// CommitDatasetOutputResponse 新增（omitempty：无 key 路径不出现）
IdempotencyKey     string `json:"idempotency_key,omitempty"`
RequestFingerprint string `json:"request_fingerprint,omitempty"`

// 新增
type OutputCommitReceipt struct {
    DatasetID          uuid.UUID          `json:"dataset_id"`
    IdempotencyKey     string             `json:"idempotency_key"`
    RequestFingerprint string             `json:"request_fingerprint"`
    Branch             string             `json:"branch"`
    Transaction        RuntimeTransaction `json:"transaction"`
    ResponseBody       json.RawMessage    `json:"response_body"`
    CreatedAt          time.Time          `json:"created_at"`
}
```

### 3.3 handlers.Store 接口新增（fakeStore 同步实现）

```go
WithIdempotentOutputCommit(ctx context.Context, scope repo.TenantScope, datasetID uuid.UUID, key, fingerprint string,
    build func(ctx context.Context, tx repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error),
) (*models.OutputCommitReceipt, bool, error)
GetOutputCommitReceipt(ctx context.Context, scope repo.TenantScope, datasetID uuid.UUID, key string) (*models.OutputCommitReceipt, error)
```

### 3.4 共享锁序（round-1 修正后）

keyed 与无 key 两条发布路径现在共享同一锁序：`commitTransactionSteps`（被
`CommitTransaction` 与 `CommitTransactionWithPublish` 共用）在入口、`loadTxnForUpdate`
之前先 `SELECT id FROM datasets WHERE id=$1 FOR UPDATE`。keyed 提交早已在
`WithIdempotentOutputCommit` 持有同一行锁（同外层事务内重入无害），因此任何
dataset 上的 keyed/plain 发布并发都按 `datasets 行 → txn 行 → branch/version → …`
单一顺序获取锁。（round-1 前文档所述 "legacy versions→datasets 反序" 与实际代码不
符，已更正；修正是预防性一致化，并非证明死锁曾发生——真实交错验收由 W6 的
`TestW1KeyedLegacySharedLockOrderNoDeadlock` 执行。）

## 4. 验证边界

- 本阶段（W1 round-2）：gofmt、本服务 build、受影响 target 单测（handlers/repo，
  fakeStore/httptest，不连 PG/q3）+ `-race`；真实 PG 并发验收测试源码就位
  （`//go:build integration`）但**不执行**。
- 无 key 请求体读取保留原 Decoder 流语义（首 JSON value 即解码、不要求读完尾部）；
  keyed 请求在**同一 decoder** 上做第二次 `Decode` 且严格要求 `err == io.EOF`：
  尾部多余 value、`]`/`}`、任何垃圾或残缺 JSON → 400，超限（`*http.MaxBytesError`）→
  413，仅尾随空白合法且计入 fingerprint（覆盖全部原始字节）。
- 后续单独 review 的命令：真实 PG 容器迁移、`go test -tags integration`、HTTP 集成、
  构建部署。本阶段不启动/重启任何服务。

## 5. W6 执行清单（required names 与准确命令）

**以下八个测试名为冻结固定清单**（W6 首跑前 pin 此清单，无需 prior-PASS 前提）；Dataset
PG cohort 可独立先跑，不等 W3 大接线。交错/回滚验收依赖的确定性由 build-barrier +
`pg_blocking_pids` 对持锁 backend PID 的精确阻塞证明保证（无关锁等待者无法满足
barrier）；barrier release 为 pre-registered `sync.Once` cleanup（LIFO 先于 pool.Close），
所有 receive 有界。这些测试提交 metadata-only 文件，证明事务/投影不变量；可下载
bytes/hash 的验收在后续 HTTP 阶段，本源码不声称覆盖。真实 PG（W6 统一执行，W1 只交付
源码）必须出现且全部通过：

```
go test -tags integration -count=1 -race -v \
  -run 'W1|WithIdempotentOutputCommit' \
  ./services/dataset-versioning-service/internal/repo/
```

Required names（源文件
`internal/repo/output_commit_receipts_integration_test.go` 与
`internal/repo/output_commit_receipts_w1_integration_test.go`）：

| 测试 | 验收点 |
|---|---|
| `TestW1KeyedLockInterleavesWithReplay` | A 持 datasets 锁（build barrier，捕获 A backend PID）→ B 被 `pg_blocking_pids` 证明阻塞于 A → A COMMIT 后 B `replayed=true` 且 build 未运行、bytes 一致 |
| `TestW1KeyedLockWaitCancelZeroWrite` | 等锁者（PID 证明阻塞于 A）ctx 取消 → 零写入、不打扰持锁者；随后新请求稳定 replay 原 bytes |
| `TestW1KeyedReceiptInsertFailureRollsBackPublish` | receipt INSERT 以 23514 失败于具名约束 `dataset_output_commit_receipts_fingerprint_hex`，且 `published` 标记（仅在 `CommitTransactionWithPublish` 成功后置位）为真 → 外层全回滚：receipt/txn/staged/version/head/index/dataset_files/lineage（含本例真实 lineage）全零残留；key 未绑定，合法重试即真正 first |
| `TestW1KeyedCommitAThenBReplayStable` | A→B 后 replay A / GET A 字节稳定；branch head、`current_version`、版本计数、**完整 lineage 集合**、完整 index 集合于 B 后/A replay 后/GET A 后三点快照精确相等（保持 B） |
| `TestW1KeyedLegacySharedLockOrderNoDeadlock` | keyed(main) 持锁 parked、legacy commit(dev) 被 `pg_blocking_pids` 证明阻塞于 keyed 的 backend PID；无 40P01、各 cut 一版本、无 OPEN 残留 |
| `TestW1GetOutputCommitReceiptOwnForeignUnknown` | JOIN 读路径：own 返回保存 payload；foreign org 与 unknown key 均 (nil,nil) |
| `TestWithIdempotentOutputCommitConcurrentTwoInstances` | 两 pool 同 key 同 body 并发：恰一 first + 一 replay，字节一致、一版本、一 receipt、无 OPEN；随后同 key 异 body → typed 409 |
| `TestWithIdempotentOutputCommitConcurrentFingerprintDivergence` | 两 pool 同 key 异 body 并发：恰一胜者 + 一 typed 409，恰一 receipt |
