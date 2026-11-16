# P2.6 幂等发布与提交恢复 实施契约（v2.1）

状态：**v2.1 设计 + Connector W2 已实现（models/repo 层，含 snapshot 泛化）；仅纯单测/race 验证，PG/HTTP/部署未执行**。依据 `root-decisions-v2.md` v2+v2.1（2026-11-15，用户已授权整个 P2.6——幂等重试与恢复，不限于 incremental）。**注意**：`a1bff07f…ffd27` 是不变的**历史 P25 基线指纹**；自 W2 起工作树 Go/SQL 已含 P26 改动，它不再是当前源码指纹。q3 旧 9 Run / 8 owner 零接触。

- Connector = `services/connector-management-service`；DVS(Dataset) = `services/dataset-versioning-service`。
- 本轮 W2 写权：Connector `internal/models`、`internal/repo`（新迁移+纯单测）+ 本文档；结果原件 `tools/smoke/business-platform-p2-6/results/w2/`。
- P26 工具根：`tools/smoke/business-platform-p2-6`（P25 tools 85 文件与全部 results/seal 冻结）。

## 1. v1→v2 被 Root 推翻/修正的关键点

| # | v1 初稿（作废） | v2 裁决 |
| --- | --- | --- |
| 1 | key 含 generation/contentHash | key 固定 run-scoped `of-p26-pub-v1:<runUUID>`；目标/branch/正文用独立 request fingerprint 绑定，不能换 key 逃避冲突 |
| 2 | 全量增量 Run 直接走新路径 | 新 source `sync.recovery.enabled` typed opt-in 默认 false；admission 冻结 run `recovery_protocol='p26-v1'`；旧 row NULL 不回填；gate↔intent IFF 仅限 protocol v1 |
| 3 | 不持久正文、恢复绝不重发 | intent 持久 **exact 有界 serialized publish body（含 inline NDJSON）**+candidate+branch+key+fingerprint+hash/counts+冻结 baseline；intent 存在与 `publish_entered_at` 在既有 Def→CP→Run 有序事务同生同灭；**门前无 intent 可在原 baseline 重拉；一旦 intent 存在禁止再拉 Source**，只能查证或同 key+同字节重放 |
| 4 | failed→succeeded 对账转换、404/ABORTED 证据性释放 | **门后 unknown 保持 running**（recovery_state/reason/backoff），不放宽 P25 终态规则；404/ABORTED/OPEN/5xx/错 ACK/预算耗尽都**不释放门后 owner**；owner 仅门前可证未发布失败/取消，或确认 committed 后原子 success+epoch+owner+intent finalize |
| 5 | 无 lease 的 MarkDispatching/Acked | 删除无 fencing 状态迁移；每次持久变化/发送前预算消费/延期/最终成功都完整 lease fencing；查询与发送预算分开、跨重启持久；缺 fresh auth 不消耗发送预算；多次发送期间维持有界 heartbeat 逐次 renew；失租/auth 到期 sticky stop |
| 6 | finalize 前先验 CP/binding | duplicate finalized 成功**先于** CP/binding 校验：锁下 Run succeeded 且 intent finalized 且 txn/hash/count 匹配 → already_resolved 零写 |
| 7 | 恢复端点 `:recover` 后缀 | 统一 `POST /api/v1/data-connection/syncs/{id}/runs/{run_id}/recover`；既有 connection 写权限+租户+完整父链；fresh caller token 由 DVS 自行验证 |
| 8 | boot 自动恢复可用 DB grant | 自动恢复默认关闭；P24 rawJWT 仅本进程 localArm 内存，重启后 DB grant≠可用 auth；须 fresh 本地 grant 且同 boot/revision/identity/TTL 验证；无 auth 不发远端不消费 attempt；**手动 fresh auth 是重启后主入口** |
| 9 | （未提） | Coordinator authReg/active 按 runID 假定新 Run 唯一；recover 用独立 ticket/本地同 Run 去重，不得覆盖他人 auth/slot；排队失败/过期/Stop/claim 失败只释放自身 ticket，**绝不经普通 cleanup 调 RequestCancelSyncRun 取消原 Run** |

Dataset W1 冻结（另工位实现）：单外层 READ COMMITTED 事务（datasets 行 FOR UPDATE 于新 txn INSERT/FK 前；独立 SELECT 读 receipt）；同 key 已 committed receipt 且完整 request fingerprint 一致 → 返回保存的原始 response bytes（不再查 branch/backing/lineage、不重写 head/index），不同 → typed 409 零副作用；无 receipt 时 txRepo 单外层 tx 内 Start/Stage/Merge/Publish + immutable receipt INSERT，唯一真实 COMMIT；ctx 错误/进程死整体回滚不留 OPEN/staged/receipt；key 绑定首次已 committed body；keyed 请求要求明确 branch、create_if_missing=false，fingerprint=SHA256(exact HTTP JSON body 含 key)、DVS 自算、byte contract（重排 JSON 亦 409）；first 201/replay 200；`GET /api/v1/datasets/{rid}/outputs/commits/{key}` 返回 immutable response（404 只代表当前无 committed receipt，不证明无在飞操作）；无 key 走旧行为。

## 2. 协议与数据结构（W2 已落地，Connector 侧，v2.1 泛化覆盖双 mode）

### 2.1 opt-in 与冻结

- `models.RESTAPIRecoveryConfig{Enabled bool}` 挂 `sync.recovery`（`rest_api_sync.go`）。**v2.1：recovery.enabled 不要求 incremental.enabled**——protocol v1 同时覆盖增量与纯 snapshot（cp_mode 空）发布；缺省/关闭不深验，legacy 形状零影响。
- admission 逐层探测 `sourceRecoveryEnabled`（raw JSON `sync.recovery.enabled==true`，legacy `{}`/scalar 不受影响）；冻结 `sync_runs.recovery_protocol='p26-v1'`（写一次，NULL 不回填）。`models.SyncRun.RecoveryProtocol` 带 `json:"-"`。
- **v2.1 身份三元组**：v1 admission 同时冻结 `recovery_source_id`/`recovery_output_dataset_id`/`recovery_binding_fingerprint`。snapshot 用新的 `models.SnapshotRecoveryBinding` 指纹（读取配置+完整 pagination+auth+引用 credential 指纹+target+file_glob；**独立 canonical 形状，不改 P25 IncrementalBinding 算法**）；增量镜像 cp_* 值。snapshot admission 升级为 Source→Def 强锁（与增量同法）；无 CP 行、无 epoch、无 owner，last-commit-wins 语义不变。
- 协议选择不进任何绑定指纹（协议以 run 冻结列为准）。

### 2.2 迁移 `20261117100000_sync_recovery_intents.sql`（v2.1 修订）

- `sync_runs` 新增 `recovery_protocol` + 三元组三列（均不回填）；partial index `(sync_def_id) WHERE recovery_protocol IS NOT NULL AND status IN ('pending','running')` 供 P26 fence。
- `sync_run_publish_intents`：run PK、def/source/org、protocol、`publish_key`（UNIQUE）、`request_fingerprint`（64hex）、`publish_body bytea`、目标 dataset/branch/logical_path、`content_hash`/rows/bytes、`completed_checkpoint`（**NULL=snapshot，非 NULL=增量，≤512 CHECK**）、冻结 baseline、`recovery_state`、wait 字段、`send_attempts`/`query_attempts`（跨重启持久）、`txn_id`、`resolved_at`。partial index `(next_action_at) WHERE recovery_state='waiting'`。
- **Authorization 永不入 intent**；rawBody/candidate/binding/key 不进公开 JSON/错误/日志。

### 2.3 publish key 与 fingerprint（冻结）

- `key = "of-p26-pub-v1:" + runUUID`。**不含** generation/attempt/contentHash。
- `request_fingerprint = sha256hex(exact HTTP JSON body bytes)`（body 内含 `idempotency_key`）；Connector 一次序列化→持久→原字节重放（byte contract，重排 JSON 即 409）。
- **64MiB 是 exact JSON body（序列化后）上限**，不是 NDJSON 上限：client 与 repo 都按实际 serialized 长度拒绝（`ValidateFrozenPublishMaterial`），不截断。
- 材料↔body 交叉验证**不信任 metadata 声明**：`content_base64` 实际解码→len==Bytes==declared size、sha256==ContentHash==metadata 声明、lineage source/def/run 三元等于冻结身份、branch/key 一致、candidate 满足 mode 配对（增量非空合法/snapshot NULL）。

### 2.4 状态机与预算（v2.1 修订）

```
gate(与 publish_entered_at 同 tx) → intent(armed)          [增量 Def→CP→Run→Intent；snapshot Def→Run→Intent]
armed → waiting(RecordRecoveryWait: reason+next_action_at；不耗预算)
任何发送（含 manual）前 ConsumePublishSendBudget(send_attempts+1, 总上限 3, 持久, manual 不豁免)
自动查证失败 ConsumeRecoveryQueryAttempt(上限 5, backoff 1s→16s 封顶, 持久, sticky)
manual 显式 fresh 请求至多一次 ConsumeRecoveryQueryAttemptManual（计数可观测，超 5 仍计，不重置 sticky）
超限 → *_budget_exhausted（sticky；owner/run 不动；**真实 committed 的 finalize 仍允许**）
send 耗尽后 fresh 手动 recover：只允许一次只读查证→committed 则 finalize；不得再发送、不得重置预算
DVS 确认 committed → FinalizeRecoveredRun 单 tx: success(+增量 epoch/owner 释放)+intent finalized
```

- 门后任何未知（EOF/5xx/OPEN/404/ABORTED/错 ACK/预算尽）**不终态化、不释放 owner**，Run 保持 running（v1 Run 的 failed/aborted 被 `FailIncrementalSyncRun`/`CompleteRecoverableSnapshotRun` 在门后拒绝，typed `ErrRecoveryPostGateLocked`）。
- 门前（无 intent）v1 Run：fail/abort 沿原 CAS（快照无 CP 副作用；增量沿 P25 DB 断言释放）。
- 缺 fresh auth：`RecordRecoveryWait(auth_required)`，零远端零 attempt。

## 3. W2 交付的稳定 Go API（对接面详见 `tools/smoke/business-platform-p2-6/W2-REPO-INTERFACE.md`，授权路径）

要点（v2.1 + review-round1 已落地）：通用入口 `BeginRecoverablePublishGate`/`FinalizeRecoveredRun`/`ClaimRecovery` 按 run 冻结 mode 路由（`...Incremental` 保留为 wrapper）；全部 leased/公开 run 投影携带 `RecoveryProtocol`（item 2/4）；旧入口 `BeginPublishGate`/`BeginIncrementalPublishGate`/`CompleteIncrementalSyncRun` 对 v1 typed 拒绝——`ErrRecoveryRequiresRecoverableGate`/`ErrRecoveryRequiresFinalize`（item 3）；gate 双亲锚定 `ErrRecoveryMaterialParentMismatch`（item 5）；`ClaimRecovery` 新只读 outcome `already_resolved`（succeeded+finalized，先于 CP/owner/binding 资格，其余终态零写 not_eligible，永不 `(nil,nil)`，item 6）；**独立预算**（send 3 / auto query 5 互不阻塞、仅自身 cap 或 finalized 拒、manual 超限可观测不复位、wait 仅 finalized 拒、exhausted 摘要保留计数权威、真实 committed finalize 在耗尽态仍成功，item 7）；`PublishReceipt` 10 字段（含 branch + lineage 三元）repo 独立全量核验。gate 拒入一律强制回滚（sentinel，item 1）——intent⟺gate IFF 在所有分支成立。

## 3b. v2.1 通用化与旁路封堵（已实现）

- **snapshot protocol v1**：admission 冻结三元组（§2.1）；`TakeoverExpiredSyncRun` 加谓词拒绝一切 v1 Run（裸接管重拉被堵）；v1 恢复只能走 `ClaimRecovery`。
- **claim 覆盖三形态**：pending v1 未 claim（fresh auth 重执行，IntentExists=false 门前重拉合法）；running v1 旧租约过期且无 intent（门前重执行）；running 过期有 intent（门后：只查证/同字节重放/finalize）。terminal+intent（历史遗留 post-gate failed）→ verify-only claim。iff 破坏（intent 有无 ≠ gate）→ not_eligible 零写。
- **旁路封堵**：`CompleteLeasedSyncRun` 对 v1 快照路由至 `CompleteRecoverableSnapshotRun`（success→`ErrRecoveryRequiresFinalize`；门后 failed/aborted→`ErrRecoveryPostGateLocked`，门前沿原 CAS）；增量 `FailIncrementalSyncRun` 同样门后拒绝；v1 增量 success 拒绝走 `ErrRecoveryRequiresFinalize`。
- **P26-only fence**：`SetCredential` 有效变更（指纹变化；同值轮换允许）与 `UpdateSyncJob` 有效 def-input 变更（file_glob/target；target 另有既有 ErrActiveRuns）在存在 v1 pending/running 时拒绝（`ErrRecoveryRunsActive`）；非 v1 行为零变化。
- **snapshot 语义**：同 Def 多 Run 继续 last-commit-wins，不加全 snapshot 单飞；已 COMMITTED 旧 key replay 纯返回 receipt 不回拨 head；尚未首次提交的旧 intent 恢复后首次 commit 可成为新 head（原语义，记录在案）。

## 4. 原子边界与锁序（冻结；intent 随 Run 叶写）

| 边界 | 内容 |
| --- | --- |
| B2' | v1 gate（增量 Def→CP→Run / 快照 Def→Run）：`publish_entered_at` CAS + intent INSERT 同 tx；材料/配对/fingerprint 全验，冲突整 tx 回滚 |
| B5' | 恢复 claim（Source→Def→[CP 仅增量]→Run）：§3b 三形态；attempt/generation+1、token 轮换、不动 status；活租约→busy、binding 漂→stale、iff 破坏→not_eligible，均零写 |
| B8 | wait/预算：lease CAS + intent 行锁 + state 谓词（finalized/exhausted 拒自动迁移；manual 查询只计不重置） |
| B6' | finalize（通用路由）：already_resolved 先于一切 owner/epoch 检查；随后 success CAS（+增量 epoch 推进/owner 释放，P25 谓词逐字）+ intent finalized 单 tx；不重算当前 binding（owner fence 下不可达，receipt 是更强证据） |

## 5. 不变量（v2.1）

1. I1'' gate↔intent IFF 仅 protocol v1（双 mode）。
2. I2'' key 固定 run-scoped；fingerprint 绑定 exact body；换 key/换字节都被 DVS 409。
3. I3''（DVS W1）同 key 至多一个 committed receipt；replay 原 bytes；无 key 旧行为不变。
4. I4'' epoch 至多推进一次/Run；重复 finalize=already_resolved 零写。
5. I5'' 门后 owner/run 只被"确认 committed 的 B6'"收敛；一切未知保持 running+owner 保留。
6. I6'' intent 存在后禁止拉 Source；门前（无 intent）可原 baseline 重拉。
7. I7'' 一切持久变化完整 lease fencing；失租/auth 到期 sticky stop；发送预算（3）对 manual 无豁免。
8. I8'' legacy（protocol NULL）Run/owner 永不被新路径扫描/迁移/猜测；普通 takeover/complete 旁路对 v1 封堵。
9. I9'' fresh auth：手动=调用者 JWT；自动=本进程 fresh localArm grant；Authorization 永不持久。
10. I10'' recover 独立 ticket；失败只释放自身，绝不无 generation 地 RequestCancel 原 v1 Run（pending v1 可后续 fresh auth 恢复）。
11. I11''（v2.1）材料校验以**实际 inline 字节**为准（解码长度/sha256/lineage/branch/key），metadata 声明不可单独取信；64MiB 界限作用于序列化 JSON body。

## 6. 验收矩阵（v2.1 增补 A2/D2/K2；PG/HTTP 执行待后续单独授权命令）

v2 矩阵（A–L）全部保留，另增：
- A2 snapshot v1：admission 冻结三元组；gate/intent 同生同灭；无 CP 行/epoch/owner 变化。
- D2 门后未知（快照）：run 保持 running（不被 complete 路径写 failed/aborted），typed 拒绝可见。
- K2 门前崩溃 v1：ClaimRecovery granted+IntentExists=false→原 baseline 重拉执行；TakeoverExpiredSyncRun 对同 run 空写。
- M2 SetCredential 有效变更/file_glob 有效变更在 v1 pending/running 期间 409；同值轮换与非 v1 行为不变。
- N2 manual 发送预算：3 次总限耗尽后 manual 不得再发送；一次只读查证+committed finalize 仍可；query 计数跨 manual 累计可观测。

## 7. 工位边界（v2.1）

| 工位 | 范围 | 状态 |
| --- | --- | --- |
| W1 Dataset | 单外层 tx 幂等 commit + immutable receipt + GET status | 另工位 |
| **W2 Connector models+repo** | 迁移、typed opt-in（双 mode）、intent 存储、fenced gate/claim/wait/budget/finalize、旁路封堵、P26 fence、纯单测/race | **已实现（attempt-1 v2 首片 + attempt-2 v2.1 泛化）** |
| W3 executor 接线 | executeSnapshotSync v1 分支（双 mode）：一次序列化→gate→预算→发送/重放→renew 心跳→finalize；门后失败改 RecordRecoveryWait | 对接面已冻结（`tools/smoke/business-platform-p2-6/W2-REPO-INTERFACE.md`） |
| W4 recover HTTP + coordinator ticket | `POST .../runs/{run_id}/recover`、独立 ticket、同 Run 去重、manual 预算规则 | 待 |
| W5 自动恢复 worker（默认关） | fresh localArm grant 验证、自动预算/backoff、waiting 扫描 | 待 |
| W6 验收工具 | `tools/smoke/business-platform-p2-6` | results/w2 已有 attempt-1/2 |

## 8. W2 验证记录（已执行：gofmt/vet/纯单测/race；未执行：PG/HTTP/容器部署）

- attempt-1（v2 首片）：`results/w2/attempt-1/`，P25 同族命令（未带 --pull=never/--network=none，早于 v2.1 澄清）；vet repo+models rc=0、纯单测 8/8、race 全包 ok；含 handlers 的 vet 因**并行工位** `runtime/dataset_recovery.go:214` 未用变量 rc=1（非 W2 文件，未触碰）。
- attempt-2（v2.1）：`results/w2/attempt-2/`，全部 `--pull=never --network=none`+镜像 ID 归档；vet rc=0（一次未用变量真错修复后）；models+repo 全包纯测 `ok`（双 mode 材料/实际内容交叉/candidate 配对/snapshot 指纹敏感性与顺序无关性；一次测试自身切片别名 bug 修复）；race（golang:1.25）全包 `ok`。
- attempt-3（receipt 扩验+PG 源码）、attempt-4（item 1 gate 原子性修复+回归 T2b）、attempt-5（items 2–7 修复+T1/T3/T5/T10 修订+新 T11/T12/T13）：各自 INDEX.md；attempt-5 后 vet（双 tag）/纯测/race 全绿。
- **当前状态（冻结点）**：review-w2-round1 items 1–7 全部修复；Root/只读 reviewer 复查通过，**已认可进入真实 PG，无新增架构项**；W2 源码冻结于 `results/w2/SOURCE-MANIFEST.txt`（13 文件 SHA256；[new] 6 + [shared] 7），等待 W6 新隔离 PG 运行 14 组 required names（清单见 `W2-REPO-INTERFACE.md` §8）——**真实失败回来才 fix，不做无关改动**。

## 9. 保全

不改 P25 tools/归档/封存；不重启/部署 q3；不触 PG/卷/端口/网络/凭据；env 密钥不打印/复制/入档；旧未提交 P0–P25 全保留，不 reset/stash/自动 commit。
