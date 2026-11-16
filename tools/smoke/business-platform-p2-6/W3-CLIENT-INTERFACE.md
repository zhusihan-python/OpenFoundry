# W3 — Connector runtime 可恢复发布客户端接口（P2.6, v1 冻结）

Owner: W3（`services/connector-management-service/internal/runtime`，新文件 `dataset_recovery.go`）。
依据 root-decisions-v2 + v2.1 范围澄清（2026-11-15）+ Dataset W1 冻结协议。本文件是给执行器
/handlers 工位与 W1 的**稳定接口契约**：签名与 JSON 形状冻结，实现细节可后续演进。本接口本身
**通用覆盖 snapshot 与 incremental**（prepared body 不含 candidate；candidate 由执行器按 run
mode 单独持久——v2.1：incremental 必填非空、snapshot 恒 NULL）。

> W2 边界注记：`internal/repo` 的 intent/gate/finalize API **仍在通用化扩展中，当前 WIP 不构成
> 稳定承诺**；§6 的 repo 侧名字以 W2 冻结版为准。W3 只依赖 v2/v2.1 已冻结的字段语义。

## 1. Go API（package `…/internal/runtime`）

```go
// 新端口（旧 DatasetVersioningPort / CommitOutput / RegisterLegacyAppend 一字未动）。
type DatasetRecoveryPort interface {
    // 发出已持久化的 exact body（原字节），strict 校验回执后才算成功。
    CommitPreparedOutput(ctx context.Context, prepared PreparedCommit, authorization string) (*DatasetCommitReceipt, error)
    // 按 key 查询已 committed 的 immutable 回执；404 => ErrCommitReceiptNotFound（不假成功）。
    GetCommitReceipt(ctx context.Context, datasetID uuid.UUID, key string, authorization string) (*DatasetCommitReceipt, error)
}

// 单次序列化纯函数（无 I/O）。key 由调用方传入，v1 约定 repo.DerivePublishKey(runID)
// = "of-p26-pub-v1:<runUUID>"；runtime 不依赖 repo，只校验 key 形状。
func PrepareRecoverableCommit(req DatasetCommitRequest, idempotencyKey string) (*PreparedCommit, error)

type PreparedCommit struct {
    DatasetID   uuid.UUID   // 目标数据集（入 URL）
    Key         string      // run-scoped idempotency key（入 body + 查询路径）
    Branch      string      // 显式非空 branch；空串在 Prepare 即拒绝，绝不静默解 active/main
    LogicalPath string      // 唯一 inline 文件逻辑路径
    Body        []byte      // 唯一一次 json.Marshal 的精确字节；重放必须原字节复用
    Fingerprint string      // sha256 hex(Body)，与 DVS 服务端对 raw body 自算一致
    ContentHash string      // sha256 hex(Content)，必须与 metadata.content_hash 相等
    Rows, Bytes int64       // counts（Bytes == len(Content) == files[0].size_bytes）
    Lineage     DatasetLineageIdentity // 从 req.Metadata.lineage 提取的回执比对期望
}

type DatasetLineageIdentity struct {
    SourceID  uuid.UUID `json:"source_id"`
    SyncDefID uuid.UUID `json:"sync_def_id"`
    SyncRunID uuid.UUID `json:"sync_run_id"`
}

type DatasetCommitReceipt struct {
    DatasetID          uuid.UUID
    Branch             string
    IdempotencyKey     string
    RequestFingerprint string
    TransactionID      uuid.UUID
    Status             string // 恒 "COMMITTED"（结构校验已拒其他）
    ContentHash        string // transaction.metadata.content_hash
    Lineage            DatasetLineageIdentity
    Replay             bool   // 仅 CommitPreparedOutput 填：HTTP 200 重放 vs 201 首发提交
}

// strict 回执匹配（客户端在 commit 路径内部使用；执行器在 GET 回执后二次使用）。
// 逐项：dataset/branch/key/fingerprint/txn!=nil/COMMITTED/content_hash/lineage 三元。
func (p *PreparedCommit) ValidateReceipt(r *DatasetCommitReceipt) error

// 旧 GetDataset/DatasetInfo 追加（签名与既有 fakes 兼容，字段级扩展）：
// DatasetInfo.ActiveBranch string —— DVS 既有 active_branch 的解析
// （无服务端 active/main fallback；缺省/空白 → ""）。仅用于 P26 首次
// precheck 冻结显式 branch；空值执行器 fail closed，恢复期绝不重查改文。
```

`HTTPDatasetVersioningClient` 同时实现两个端口（编译期断言在 `dataset_recovery.go`）。

## 2. HTTP 协议（DVS W1 冻结）

- `POST {base}/api/v1/datasets/{uuid}/outputs:commit`，请求体 = §3 JSON（含
  `idempotency_key`），字节即指纹输入。首发 `201`、同 key 同字节重放 `200`。
- `GET {base}/api/v1/datasets/{uuid}/outputs/commits/{key}`，返回当初保存的同一
  immutable 响应体；`404` 只表示“暂未见 committed 回执”，不是零发布证明。
- keyed 请求恒为：显式 branch、`create_if_missing=false`、inline 单文件输出。

## 3. 请求 JSON（PrepareRecoverableCommit 唯一一次序列化的形状）

```json
{
  "create_if_missing": false,
  "format": "TEXT",
  "branch": "main",
  "transaction_type": "SNAPSHOT",
  "summary": "Connector snapshot sync <defUUID>",
  "files": [{
    "logical_path": "sync-<runUUID>.ndjson",
    "size_bytes": 123,
    "content_type": "application/x-ndjson",
    "operation": "ADD",
    "content_base64": "<base64 NDJSON；零行输出为 \"\">"
  }],
  "metadata": {
    "bytes": 123, "content_hash": "<sha64hex>", "pages": 3, "rows": 7,
    "lineage": {"source_id": "<uuid>", "sync_def_id": "<uuid>", "sync_run_id": "<uuid>"}
  },
  "idempotency_key": "of-p26-pub-v1:<runUUID>"
}
```

不变量：body 内**没有** raw candidate checkpoint、没有 Authorization/凭证材料；
top-level 字段序固定（struct 序列化），metadata 为调用方原样嵌入（map 序）。
`content_type` 缺省 `application/x-ndjson`、`transaction_type` 缺省 `SNAPSHOT`（与旧
CommitOutput 归一化一致）。Prepare 纯函数级拒绝：空 branch、`create_if_missing=true`、
key 非法（空白/带 `/ ? # %`/控制符/长度>128）、ContentHash 非 64hex 或 != sha256(Content)、
metadata 缺 `content_hash`(须==req.ContentHash)/`rows`(须==req.RowCount)/`bytes`(须==len
(Content))/`lineage`（三个 uuid 可解析）、LogicalPath 空。**64 MiB 上限作用于序列化后的 exact JSON body
（含 base64 膨胀），按实际序列化长度拒绝、绝不截断——64 MiB NDJSON 经 base64 后不一定
装得下（v2.1）**；与 repo.MaxPublishIntentBodyBytes 同值。

## 4. 响应 JSON（两类端点同一保存体；runtime 只解码需要的字段）

```json
{
  "dataset_id": "<uuid>", "dataset_rid": "ri.foundry.main.dataset.<uuid>", "branch": "main",
  "transaction": {
    "id": "<uuid>", "status": "COMMITTED",
    "metadata": {"content_hash": "<sha64hex>",
                 "lineage": {"source_id": "<uuid>", "sync_def_id": "<uuid>", "sync_run_id": "<uuid>"}, "…": "…"}
  },
  "files": ["…"], "preview": {"…": "…"},
  "idempotency_key": "of-p26-pub-v1:<runUUID>",
  "request_fingerprint": "<sha256hex(exact request body)>"
}
```

结构校验（不过即 `*DependencyError` invalid receipt，绝不判成功）：`dataset_id`/`branch`/
`idempotency_key`/`request_fingerprint` 非空、`transaction.id` 非 nil、status==
`"COMMITTED"`、`metadata.content_hash` 与 `metadata.lineage` 三元齐备；GET 另要求
`dataset_id` 回显与请求一致、`idempotency_key` 与所查 key 一致。响应读取上限 8 MiB。

## 5. 错误分类（typed；error 文本永不回显 raw body/token/candidate/URL query）

| 情形 | 返回 |
| --- | --- |
| 传输失败/ctx 超时或取消 | `*DependencyError{Op,Timeout:true}`（Op=`commit_prepared_output`/`get_commit_receipt`） |
| 5xx / 3xx / 202 等未知状态 | `*DependencyError{Op,Status}` |
| 401 / 403 | `ErrDatasetUnauthorized` / `ErrDatasetForbidden`（沿用既有 sentinel） |
| POST 404 | `ErrDatasetNotFound` |
| POST 409（有界解析 typed code） | **仅** `{"code":"IDEMPOTENCY_KEY_CONFLICT"}`（W1 ErrorEnvelope，≤4KiB 有界读）→ `ErrCommitIdempotencyConflict`（永久冲突）；无 code/其他 code/不可解析的 409 → `*DependencyError{Status:409}`（依赖忙，unknown 不终态化），409 error 文本不回显 |
| GET 404 | `ErrCommitReceiptNotFound`（暂未见 committed，非零发布证明） |
| 2xx 但回执结构坏/超限/非 COMMITTED | `*DependencyError{Op,Detail:"invalid receipt…"}`（unknown-after-dispatch） |
| 2xx 但与 prepared 期望不符 | `*ReceiptMismatchError{Check}`（Check ∈ dataset/branch/idempotency_key/request_fingerprint/transaction_id/status/content_hash/lineage） |

客户端**不重试**（无界或有界都不做）：预算/心跳/fencing/可重试判断全部留给执行器
（send 预算总量含 manual 发送、上限 3 且重启不重置；自动 query 预算 5——v2.1，均由
W2 repo API 落账）。Authorization 只进请求头，不进 body/日志/错误。发出前双重校验：
`sha256(Body)==Fingerprint`（发送字节恒等于被指纹化的字节，且入口先做**私有拷贝去别名**——
验证与发送同一数组，调用方/传输层在 hash 校验后改原数组无法改变 wire 字节），以及
**body↔material 实际字节深核**（v2.1：key/branch/logical_path、files[0] 解码后长度==Bytes 且
sha==ContentHash、metadata rows/bytes/content_hash/lineage 一致——**只信实际字节，不信
metadata 声明**）；任一不符即拒绝发送、零网络请求。

## 6. 执行器接线映射（后续 handlers/coordinator 工位；repo 侧名以 W2 冻结版为准）

- **首次 target precheck（P26）**：`GetDataset` 现返回 `DatasetInfo.ActiveBranch`（解析 DVS 既有
  `active_branch`，**不带** DVS commit 路径的 active/main 静默 fallback；缺省/空白 → 空串）。
  执行器取该名作为**首次 intent 冻结的显式 branch** 放进 prepared body；**空/无效即 fail
  closed，绝不放行空名进 keyed commit**（Prepare 层同样拒绝空 branch 兜底）。
- **有 intent 的恢复**：只用 intent 持久 branch 原字节重放，**绝不重查 ActiveBranch 改正文**
  ——每 Run 首次意图冻结后，目标 branch 漂移只能以 ReceiptMismatch(branch) 显式暴露，
  不能被"追认"。
- 门时：`prepared := runtime.PrepareRecoverableCommit(req, repo.DerivePublishKey(runID))` →
  存 intent 材料 `{Branch, LogicalPath, ExactBody: prepared.Body, ContentHash, Rows, Bytes,
  Candidate: incremental ? snapshot.CompletedCheckpoint : NULL}`（v2.1：incremental 非空、
  snapshot NULL；Prepare 已保证全部 body↔material 交叉项通过 repo 侧校验）。
- 发送/重放：由 intent 重建
  `PreparedCommit{DatasetID: m.OutputDatasetID, Key: m.PublishKey, Branch: m.Branch,
  LogicalPath: m.LogicalPath, Body: m.ExactBody, Fingerprint: m.RequestFingerprint,
  ContentHash: m.ContentHash, Rows: m.Rows, Bytes: m.Bytes,
  Lineage: DatasetLineageIdentity{m.SourceID, m.SyncDefID, m.RunID}}` 后调
  `CommitPreparedOutput`（intent 存在后禁止再拉 Source，只查证/同字节重放；重建字段与
  body 不一致会被发送前深核零副作用拒绝）。
- 回执落库：`repo.PublishReceipt{IdempotencyKey, RequestFingerprint, DatasetID,
  TransactionID, Status, MetadataContentHash: r.ContentHash,
  MetadataSyncRunID: r.Lineage.SyncRunID}` → 通用 finalize（v2.1：按 run mode 路由的
  `FinalizeRecoveredRun` 方向；现名 `FinalizeRecoveredIncrementalRun` 将保留为 wrapper）
  ——repo 层会再次独立核验，transaction UUID 本身仍不是 usable ACK。

## 7. 验证与未验项

验证证据（cmd/stdout/stderr/rc 原件与汇总）：
`tools/smoke/business-platform-p2-6/results/w3/attempt1/`（MANIFEST.md）。真实 rc：build 0 /
vet 0 / test-normal 0（go1.26.3, CGO=0, 全包含旧 API 既有测试）/ test-race 0（go1.25.14,
CGO=1）/ gofmt（两新文件）clean。执行环境：Root 豁免的 P25 已缓存 golang 工具镜像，
`--pull=never --network=none GOPROXY=off`，源树只读挂载，gofmt -w 仅窄写挂载本工位新文件。
未验项：真实 PG/DVS/q3/部署（W1 keyed 端点落地前无真服务可打）；409 typed 错误码细化与
GET 路由真实行为待 W1 集成；handlers/coordinator 接线待 Root 后续授权。
