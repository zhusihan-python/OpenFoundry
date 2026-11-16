# HANDOFF-PRIMARY — P2.5 实施阶段（2026-11-14）

P2.5「发布后推进断点」**实施阶段交接**。授权链：用户→root `implementation-release.md`（放行编码＋靶向验证）→review-source-round1/repo-round1/docs-verification 九条修正（已全部落码并复验）。

## 本会话产物（Primary 轨道）

**契约文档**（`tools/smoke/business-platform-p2-5/`）：`design-draft.md` v2.1、`implementation-contract.md` **v1.2**（v1.2 变更日志逐条列九条修正）、`HANDOFF-PRIMARY.md`（本文件）。

**业务实现**（Connector，全部 Primary 归属文件）：
- `internal/models/`：`RESTAPIIncrementalConfig`（InitialValue `*string` presence）、校验（cursor-only/键冲突/dot path/U+FFFD+控制字符/字节界）、`IncrementalBinding`+`IncrementalBindingFingerprint`（含完整 Auth 身份与 file_glob）、`ValidateRESTAPISourceIncremental`（api_key 真实门控）、`SyncRun` 内部 `json:"-"` cp 字段＋`RunIncrementalView`。
- `internal/repo/migrations/20261116100000_sync_incremental_checkpoints.sql`：`sync_def_checkpoints`（octet_length 界、owner 反连接索引）＋`sync_runs` cp_* 内部列；追加式可重放。
- `internal/repo/checkpoint.go`（新，核心）：probe（`sourceIncrementalEnabled`，无全量解析）、`readIncrementalBinding`、`AcquireCheckpointForAdmission`（Def 强锁后、run INSERT 后置 owner、InitialValue 回退入 base）、`claimIncrementalSyncRun`（四重溯源验证）、`BeginIncrementalPublishGate`、`CompleteIncrementalSyncRun`（前置校验＋锁下当前绑定复验＋全有或全无）、`FailIncrementalSyncRun`（全守卫＋DB 断言 gate 边界条件释放）、`RequestCancelIncrementalSyncRun`（Source→Def→CP→Run）、`withDefCheckpointRunLock`（run-def 断言＋基线锁后重读＋有界回滚）、围栏助手、`GetCheckpointView`（当前状态 owner 观测）。
- 既有文件插段：`lease.go`（Enqueue 两段式 probe→Def 强锁→全量绑定；Claim/Complete 路由；Takeover 反连接）、`cancel.go`（gate/cancel 路由）、`schedule.go`（tick outcome＋claim 插段）、`admission.go`（Def 强锁）、`repo.go`（五处围栏、SetCredential 事务化、legacy RunSyncJob 拒增量、CompleteSyncRun cp_mode 守卫、bytes）。
- `internal/runtime/`：`rest_snapshot.go`（IncrementalFetchSpec/最终响应候选/U+FFFD/循环错误脱敏）、`sync.go`（DatasetCommitResult.Status＋adapter COMMITTED/target 校验）。
- `internal/handlers/`＋`server/`：executeSnapshotSync 增量分支（含 usable-ACK handler 校验、成功走 CompleteIncrementalSyncRun）、409 映射、`GET /syncs/{id}/checkpoint`、run 增量视图、Store 接口四方法＋两测试替身、mock dataset fixture 修正（回显精确 dataset id）。
- README P2.5 契约段（C14）。
- 非 PG 单测：`models/rest_api_sync_incremental_test.go`、`runtime/rest_snapshot_incremental_test.go`（真实 wire 畸形/脱敏/协议/空批次/无前进）、`runtime/sync_ack_test.go`。

## 已验证（真实 cmd+rc，证据 `/private/tmp/openfoundry-p25-repair-evidence/connector/`，INDEX.md 有出处说明）

- attempt15/16：gofmt（Primary 文件）＋`go build ./...`＋`go vet ./...` rc0（golang:1.26.3，容器、只读源、复用缓存卷、GOPROXY=off）。
- attempt17：`go test -count=1 ./...` 全套非 PG **38 包 ok、0 FAIL、原 Go rc0**——完整原 Go stdout/stderr 已落盘（直连重定向，无容器内 pipeline）；**全套通过主张以此为准**。
- attempt10（更早的全套运行）为 **summary-only**：其 log 只含容器内 `grep -c` 的计数 `38`，原 Go 输出未持久化（review-source-round1 第 6 条），不得作为全套验证证据引用；原件未改。
- attempt18：golang:1.25.14+CGO `go test -race -count=1`（handlers/runtime/models/server/repo，真实 4 包 race 输出＋钉定工具链行）＋`go vet -tags=integration ./...` **rc0**（integration 为 compile-only，不作 PG 通过计）。
- 历史尝试（1-14，含真实失败）全部保留：INDEX.md 如实标注 cmd 出处（会话历史重建）；attempt15 起 .cmd sidecar 先行落盘。后续源码修正的复测一律保存完整 stdout/stderr＋原 Go 退出码（无 wc/grep 冒充）。
- 未运行：真实 PG/q3/HTTP/业务栈容器切换（未 release）。

## 九条修正落点速查（review → 代码）

S1→`ValidateOpaqueCheckpointValue`(U+FFFD)+wire 测试；S2→cursor 循环错误增量脱敏+测试；S3→`ValidateRESTAPISourceIncremental` api_key 门控+测试；S4→`Repo.RunSyncJob` 探测拒 `ErrIncrementalRequiresControlled`+`CompleteSyncRun` `cp_mode IS NULL`；S9/R1→`sourceIncrementalEnabled` probe＋Def 强锁后 `readIncrementalBinding`（manual/scheduled 双路径）；R2→cancel 补 Def FOR UPDATE＋绑定校验；R3→Fail 四重守卫（含 retain 分支）；R4→Complete 前置校验（nil TxnID/负数/64-hex）＋锁下当前绑定；R5→claim 溯源（owner/绑定/epoch/target；输入＝冻结 `cp_base_value`）；R6→`withDefCheckpointRunLock` sync_def_id 断言＋基线重读；R7→`rollbackCheckpointTx` 5s 有界；R8→ownerView 当前状态+租约存活+fail-closed；S5→证据 INDEX/sidecar。

**追加固栏（review-repo-round1 附录，已闭环）**：对外 API 措辞去内部阶段名——`CheckpointOwnerView.BlockedReason`＝业务状态 `requires_reconciliation`（两处赋值+注释）、handler 409 `retry_hint` 改为未确认结果/需对账措辞（handlers.go 中不再出现 P26 字样）；contract §7 同步（`awaiting_p26_resolution` 全部清零）；P26 边界说明仅保留在实施计划/README。验证：attempt19（build+vet+靶向 unit，完整 stdout，原 Go rc0）。

## Round2 收口与 BUSINESS 候选冻结（2026-11-14）

- **review-source-round2 三补丁已落码**：F1 cancel 租户谓词 $3 起（真实 PG 绑定错误）；F2 scheduled tick 用 Acquire 实际 run.ID（防 FK 23503 整体回滚）；F3 probe 逐层 RawMessage（`sync:"x"` 旧 P23 源兼容，仅显式 true 激活全量校验）＋bearer 同名正控微修。契约同步至 **v1.2.1**（含 PG 轨道待补清单：pending-cancel 跨父回归、enqueued 正向、legacy ControlMode 断言）。
- **验证**：attempt20（`.cmd` 先行＋完整 stdout＋原 Go rc0）＝gofmt(3 文件)＋`go build ./... && go vet ./... && go test -count=1 ./...` 全套 **38 ok / 0 FAIL**。
- **冻结**：`BUSINESS-CANDIDATE-FROZEN.sha256`（165 业务文件；清单 sha256 `740cb43f…b1f0`）存证据目录；attempt20 后业务文件零改动；后续任何业务改动需 reviewed new revision＋新指纹。
- **等 PG 实际验收**（PG 轨道独立冻结）；真实 PG/q3/HTTP 仍未 release。

## 冻结后状态与 smoke 交接准备（2026-11-14，root 收口）

- **冻结被接受**：root 独立核对 3 补丁＋attempt20（38 ok/0 FAIL/真实 rc0）；外部 `business-protected-baseline.txt`＋`business-freeze-review.json`＝保护 163 项全匹配、无未索引 Go/SQL/README、216 项依赖/其他服务/cron 与 P24 基线不变。**我方 165 快照原样保留**（含 PG 专属 2 文件，其后续合法编辑不视为业务漂移）；不为补账重跑。业务/非 PG 测试已冻结；真实 PG 发现 prod bug 时由 root 解冻修复。
- **契约路由钉正**：checkpoint 实际路由 `/api/v1/data-connection/syncs/{id}/checkpoint`（server.go /api/v1 组），contract §7 省略前缀笔误已同步（root 提供，无 live 试错）。
- **smoke 交接准备（只读完成）**：通读 smoke_p25.py scaffold（h1–h15＋10 gates）、fixtures/rest_source_p25.py、dataset_fault_proxy_p25.py、tools-design；产出 **`smoke-handoff-note.md`**（本人）：路由/字段/knob↔业务行为逐点对齐（含 server.go 实测拼写：config PATCH＝`/api/v1/connections/{id}`、credentials＝`/api/v1/data-connection/sources/{id}/credentials` POST、scaffold 缺 run 单查/sources PATCH/credentials 三条 ROUTES）、h1–h15→机械判定映射、可选缺口（fixture surrogate knob）。**未写 smoke_p25.py**——等 Tools 确认无并发编辑＋root 正式交接。

## smoke_p25.py v2 交付（2026-11-14 晚，root 正式移交后）

- **引擎已实现并自测通过**：完整真实 HTTP 引擎（ROUTES+4、DATASET_ROUTES 实测拼写、h1–h15 实现、attempt/raw 纪律、owner/run roster 分账、finally quiet、双门执行闸 --released∧P25_CONFIRM=yes）。r2 修正语义内置；h12 仅绑定真实 PG 证明文件。
- **纯检查全绿（真实 rc）**：`--ast-check` rc0；`--contract-probe` rc0（含 requires_reconciliation 与全路径 checkpoint 路由 pin）；`--selftest-mocked` rc0 `ok:true`（256 attempts；编排逻辑 vs r2 fakes；非真实 connector 证据）。
- 决策：fixture 场景默认 aux connector；h13 需 aux 调度循环（driver 侧）。详细见 `smoke-handoff-note.md` §6。
- 业务候选保持冻结（165 快照原样）；未启动任何 HTTP/listener/q3/Docker/Go/DB。

## smoke round-1 十组阻断 → v3 修法已定（2026-11-14 深夜，未落码——下会话执行）

**状态**：v2 引擎 + fakes 已交付且纯检查绿（ast/probe/selftest-mocked rc0）；root `review-smoke-round1.md` 十组实际 wire 阻断已全部设计好修法，**v3 重写未写入文件**（本会话上下文接近 600k 上限，按指令先交接）。v3 完整设计要点（下会话直接照此改 smoke_p25.py，业务源/旧文件不动、不 live）：

1. **egress**：`incremental_source_config` 增加 `"egress": {"allow_insecure_http": True}`（rest_snapshot.go:163-172 要求的 per-source 覆盖）。
2. **dataset 链全部换 P1 实证路径（dataset-http-read-facts #5-#8 终版；路由 `/v1` 前缀）**：文件清单＝`GET /v1/datasets/{id}/files`（**信封 {branch,total,files:[…]}**，条目含 id＋transaction_id）→ filter `file.transaction_id == run.dataset_version_id` → `GET /v1/datasets/{id}/files/{fileID}/download`（302→200 bytes）→ sha256(raw)==run.content_hash。事务读＝`GET /v1/datasets/{id}/branches/main/transactions/{txnUUID}` 返回 **flat RuntimeTransaction {id,dataset_id,branch_id,branch_name,tx_type,status,metadata,committed_at}**：断言 id==run.dataset_version_id、dataset_id==目标、branch_name=="main"、**tx_type=="SNAPSHOT"**、status=="COMMITTED"、metadata.lineage.sync_run_id==run.id、metadata.content_hash/rows/bytes==run/下载。**latest 生效视图＝`GET /v1/datasets/{id}/views/at?branch=main`**（/v1 非 /api/v1；ViewOut={dataset_id,requested_branch,resolved_branch,head_transaction_id,files[{logical_path,physical_path,size_bytes,introduced_by=txnUUID}]}，无 fileID）：要求 200＋dataset/两 branch 字段正确＋head_transaction_id==最新 txn；每个 view 文件按 `introduced_by==file_meta.transaction_id`（＋logical_path/size，physical_path 需 local:/// 归一时显式处理）匹配到已知 file_meta，**只下载 view 选中的 fileID** 并全量比对 bytes/values/hash；B 的 SNAPSHOT 后 view.files 恰为 B 文件集、A 的 fileID 仍可历史下载不变；绝不用 /files 的 total 当生效计数。ComputeView 真实重放 COMMITTED 且最后 SNAPSHOT 清空集合。**禁用**：/preview（metadata/fallback）、/files/rows（parseNDJSONRows 强制 map，string 记录必拒）、/versions/{version}（整数-only）。
3. **全值断言（dataset-http-read-facts #9 终版）**：行是**复合字符串** `"%s-%03d|name-%s%d|qty-%d"`（p/h2 布局：h1=A i1..24/B i1..20/C i1..30；**h2=A1..24→B17..20**；**h6=A1..12→B1..8→C1..30**；**h13=A1..12→B1..8**；**p25h4=A1..24→B1..8（Tools 已加第二非空窗，h4 用回原生 src，纯测 34/34）**；p25empty 空）。期望 raw＝`b"".join(json.dumps(v).encode()+b"\n" for v in expectedStrings)`（全 ASCII；空批=b""）；断言精确 values/raw/len/sha，无 {value:…} 包装、无 r.get。latest 判定走 views/at 文件集合（introduced_by 匹配）＋集合内文件全量下载，不用 /files/rows。EXPECTED_WINDOWS/EXPECTED_PAGES 按上表重制（h6/h13 是多行窗，非单行）；逐 run：art rows==精确列表、bytes/sha 三方一致；**ledger 逐 run 切片**：每页 token==该 run 输入 token（首 run 为 None）、cursor 链首空后非空、条数==页数、有 window 键时等于窗口号。h4 六模式在 p25h4 的 B(8) 非空终页施加＋reached-proof。
4. **h4 换 p25h2**（A→B 多窗：seed 耗 A 得 T0，故障 run 达 B 非空终页）：六模式＋reached-proof（该 run ledger 切片成立）；echo=候选==请求 token。**Tools 协调项写入 note**：请求专用故障 src 家族（六模式独立 src、确定性 token 语义）。
5. **h6**：兄弟 def **重叠**（def1 slow 持有期间启动 def2 async，两 checkpoint owner 同时可见）再双双成功；busy 期 proxy 增量按 request_target 过滤==0。**h7**：retarget 用**真实第二 dataset**（create_dataset）；加 file_glob 围栏；name-only/cron-only 放行＋前后 checkpoint 快照相等；org-2 token（cfg.jwt_org2，必填否则 gate 失败）读 checkpoint/run 得 404。**h15**：manual busy＝**真 sync POST**（无 async 参）快速 409＋零新 run 行/零 dispatch/数据 CP 不动。
6. **h13**：make_sync_def 尊重 extra_cfg（schedule_cron/time_zone/enabled 入创建体）；arm 用 Authorization（token=arm_jwt）；断言每 tick.run_id==真实 succeeded run、CP epoch 随 tick 递增；busy due tick（持有者+等一槽）＝outcome skipped_incremental_busy＋无新 run＋数据 CP 不动＋之后 disable。
7. **h14**：ledger 真 key `ack_txn_id`（另有 request_target/ack_dataset_id/ack_txn_status/verified_committed）；过滤 request_target==ds 且**恰 1 条增量**；run.dataset_version_id **必须为空**（lost ACK 定义）；远端证明＝txn_get(ack_txn_id) COMMITTED＋lineage＋按该 txn 绑定文件全量 bytes/hash 下载。
8. **h10 双阶段真接口**：`--h10-stage pre|post`＋`--h10-defs/--h10-prior`；pre 落盘活态快照；post（driver 已重启）断言成功态逐字节稳定、owner 未变、无收养（无新 run id）、blocked_reason 业务词；两 roster 只收**实际行**（checkpoint owner 真行＋run 列表非终态真行）；`--tail-declaration <file>`：engine 观测的 retained owner 必须⊆声明集，否则记 undeclared。删除 h14 的"predeclared placeholder"行。
9. **final_quiet 严格化**：每 src HTTP 200 且 `inventory_started/inventory_finished/active` 为非 bool 整数、active==0、started==finished；proxy stats 200 且 inflight==0 且 started==finished；**任何 quiet 失败并入 failed → cmd_phase rc≠0**。
10. **h12/证据**：h12 证据格式＝{log_file, rc_file(含 rc=0), required_names 含 --pg-proof-name 钉名, source_meta.before/after}；HttpApi 记录**完整安全 JSON 响应体**（下载记 sha256+bytes）；AttemptRecorder 增 `persist_handle` 即时落 `handles.json`（source/def/dataset/run 全部含失败尝试的）。
11. fakes 镜像真实键形：FakeProxyDataset ledger 用 request_target/ack_*；FakeFixture 计数器 inventory_started/inventory_finished/active＋page_splits 对齐 EXPECTED_PAGES；FakeConnector full-ACK 校验＋schedule_enabled/cron 门控 due_tick；FakeApi token=org2 → 404；**FakeDataset 增加 views/at 镜像**（重放 txns：SNAPSHOT 新版本清空集、返回 files[].introduced_by=txnUUID、head_transaction_id），删除 branch_rows/files_rows。ast-check 新增标记（allow_insecure_http/ack_txn_id/request_target/files\/index/views\/at/inventory_started/handles.json/h10_stage）＋禁 /preview、/versions/ 与 **files/rows**（string 记录必拒）。
12. selftest 覆盖 h1-h4/h6-h9/h11-h15（h5/h10 跳过要显式列出 skipped_by_design）；h10 用同态 prior/post 等价路径测两阶段逻辑。

**已核实的外部事实（dataset-http-read-facts.md 终版＋Tools r3 布局，下会话免重查）**：P1 helper＝smoke.py:558 download_run_artifact（/v1 面）；/files 是 {branch,total,files} 信封；事务 GET＝flat RuntimeTransaction（tx_type/branch_name 必断言）；views/at 配方如第 2 条；禁 /preview、/files/rows、/versions；runtime 239-246 记录原样透传（逐行 loads 得 str）。**Tools r3：p25h4 已含第二非空窗 B(8)（34/34 纯测）——h4 用原生 p25h4，无需协调**；note §7 的故障族请求作废。smoke_p25.py 当前为 v2（87882B/1842 行，纯检查 rc0——fake 镜像旧契约，不作 wire 证据）。

## smoke_p25.py v3 已落码并纯自测通过（2026-11-14 深夜续会话，Primary 唯一写者）

**十组修法全部落码**（v3 全量重写，3202 行 / 154249 B，**stableSHA256 = `52f55aa200e38ffa8e6698b5f8031929c5321d2850bce5ac5e5a9fa14a228e1a`**）：

1. egress：`incremental_source_config` 顶层 `"egress": {"allow_insecure_http": true}`（rest_snapshot.go policy() 语义：service 天花板、仅显式 source override 放开 plain HTTP；json tag 核对 rest_api_sync.go:92）。
2. 数据链全部换 P1 实证 `/v1` 面：`files_envelope`（{branch,total,files}，按 `transaction_id==run.dataset_version_id` 绑定）→`file_download`→flat `txn_get`（断言 id/dataset_id/branch_name=main/tx_type=SNAPSHOT/status=COMMITTED/lineage.sync_run_id/content_hash/rows/bytes）→`views/at`（head_transaction_id＋`introduced_by` 匹配已知 file_meta＋只下载 view 选中 fileID＋**集合数恰等于已知集**）；另用 `files/index`（裸数组）作历史枚举交叉检查（h2 证明 A fileID 历史可下载）。versions/preview/rows 路由整体删除并列入 ast 禁词。
3. 全值断言：`SRC_WINDOWS` 钉死 Tools r3 布局（h1=A(12,12)/B(8,8,4)/C(30)；h2=A→b17..20；h3=A；h4=A→B(8)；h6/h13=A1..12/B1..8(/C1..30)；p25empty=[[[]]]）；期望 raw=`b"".join(json.dumps(v)+b"\n")`；每 run 断言 run 事实（rows_written/bytes_written/content_hash/dataset_version_id）＋txn flat＋下载三方一致；`verify_ledger_slice` 逐 run：页数恰等、每页 token==该 run 输入 token、cursor 链首空后非空、window 号精确（exhausted==len(windows)）。
4. h4 原生 p25h4：seed 耗 A 得 tok-0/epoch1 → 六模式在 B(8) 非空终页施加＋**reached-proof**（故障 run ledger 切片==window1 且 token==tok-0）＋泄漏负控（oversize/ctrl/echo 原值不出现在 error）。
5. h6 兄弟 def **重叠**（两 owner 同时可见，各自 full-value 证明）；busy 期 proxy 按 `request_target==ds` 过滤零增量。h7 retarget 用**真实第二 dataset**＋file_glob 围栏＋name/cron/enable 放行（前后 checkpoint 快照相等）＋**org2 token（cfg.jwt_org2，缺失即 gate 失败）读 checkpoint/run 404**＋释放后真 config 变更→409 stale。h15 manual busy＝真 sync POST（409＋runs 恰+1（仅 async 那条）＋零 dispatch＋数据位不动）。
6. h13：`make_sync_def` 尊重 extra_cfg（schedule_cron/time_zone/enabled 入创建体）；arm 走 Authorization 头（专用 arm_jwt api）；每个 enqueued tick.run_id 必须命中**真实 succeeded run**（逐 tick full-value 三方证明）；busy due tick＝skipped_incremental_busy＋无新 run 行＋数据 CP epoch 不动＋tick 已记录（cron 游标前进）；revoke+disable 后两次采样无新 tick。
7. h14：ledger 真 key **ack_txn_id**；过滤 `request_target==ds` 且恰 1 条增量 dispatch；`run.dataset_version_id 必须为空`（lost-ACK 定义）；远端证明＝独立 flat txn GET（COMMITTED/目标/main/SNAPSHOT/lineage.sync_run_id==run.id）＋按该 txn 绑定文件全量下载（values/raw/sha）；本地 failed＋owner 保留＋再触发 409；**删除全部 driver 占位行**（只声明实际执行的情景）。
8. h10 双阶段真接口：`--h10-stage pre|post`＋`--h10-defs`＋`--h10-prior`（pre 落盘活态快照；post 断言成功态逐字节稳定、owner 未变、无收养＝run id 集恒等、非终态行仍非终态、blocked_reason 业务词）；两 roster 只收**实际行**；`--tail-declaration` 被消费（观测 owner 超出声明集→记 undeclared 并入 failed）。
9. `final_quiet` 严格化：每 src HTTP200＋inventory_started/inventory_finished/active 为非 bool int＋active==0＋started==finished；proxy stats 同级严格；**任何 quiet 失败并入 failed→rc≠0**。
10. h12 绑定冻结 `pg-candidate-bindings-r17.json`：required names 从 binding 引用的 `p25-required-names.txt` 读取（绝不自选）；钉名 `TestPGCheckpointAdvanceAllOrNothingRollback`（--pg-proof-name 可覆写）；逐 stage 断言工件存在＋.rc 内容==original_rc＋.meta source sha before/after==candidate sha＋new-normal raw 含全部 15 名 `--- PASS:`；消费工件全部重哈希入 gate evidence（只读消费 /private/tmp 证据）。HttpApi 记录**完整安全 JSON body**（下载记 sha256+bytes）；AttemptRecorder `persist_handle` 即时写 `handles.json`（source/def/dataset/run，含失败尝试，409 触发也留 run-attempt 失败句柄）。

**fakes 镜像真实契约**：FakeFixture＝r3 布局＋pg-<sha12> 页键＋inventory_* 计数器＋ledger {token,cursor,page_size,window(-1 未知)}＋六 candidate 模式（echo 回显请求 token）；FakeProxyDataset＝r3 ledger 键（request_target/ack_dataset_id/ack_txn_id/ack_txn_status/verified_committed/fabricated_status）＋真实形状 dataset 存储（files 信封/index、flat txn、views/at 重放＝最后 SNAPSHOT 清空集、introduced_by=head txn）；FakeConnector＝契约 v1.2.1 状态机（全 usable-ACK 校验、gate-entry 边界、pre-gate 释放 vs post-dispatch 保留、围栏、schedule armed+enabled 门控 due_tick、busy tick 只记一次不加 run、org2 →404）。

**纯自测（真实 cmd+rc，全部本机可复现）**：
- `python3 -I smoke_p25.py --ast-check` → **rc0 ok:true**（11 个 v3 标记含 allow_insecure_http/ack_txn_id/request_target/views\/at/files\/index/inventory_started/handles.json/h10-stage/pg-candidate-bindings-r17/钉名/skipped_by_design；禁词 /versions/、files/rows、/preview、upstream_txn_id、awaiting_p26 均缺席——禁词以拼接串定义避免自匹配）。
- `--contract-probe` → **rc0**（契约 token＋connector /api/v1、dataset /v1 前缀钉死）。
- `--selftest-mocked` → **rc0 ok:true**：h1–h4/h6–h9/h11–h15 全绿（334 attempts、113 handles）；**h12 绑定真实 r17 工件全过**（15/15 PASS 于 new-normal raw、三 stage rc/meta/sha 全符）；h10 两阶段同构验证（pre 快照→post 稳定、owner_roster 恰 1 实际行、run_roster 空）；`skipped_by_design=[h5]` 显式列出；tail-declaration 纯函数正/负两例；strict quiet 零问题。
- 变异负控：篡改 token 格式（副本）→ selftest **rc1** 且 h1 三处真实失败（非同义反复）。
- `--phase healthy`（无 --released，即使 P25_CONFIRM=yes）→ **rc3 零网络**；双门缺失同 rc3。
- 未运行：真实 HTTP/listener/Go/Docker/DB/q3（均待 root 另行放行）；mock 绿仅证引擎编排/断言逻辑，**不冒充 connector 业务验收**。

**遗留协作点（不阻塞）**：h13 真实运行需 aux connector 调度循环（driver 侧，registry 已注明）；tail 声明文件由 Tools `p25_tail_roster.py declare` 产出（tag-only 声明按 count+terminal-failed 覆盖判定，def_id 声明按集合判定）；h10 由 driver 在 restart 前后分别调 pre/post。

## round-2 七组 wire 修正已落码并纯自测通过（2026-11-14 深夜，Primary）

root `review-smoke-round2.md` 同 scope 七组全部实现（v3.1，3605 行，**stableSHA256 = `1b528a0b472ea9c5b245390108b7973cff4e3bdceaca25537d0437bd07067be5`**）：

1. **h2/file-index**：`/files/index` 的 entry UUID 是 DatasetFileIndexEntry id、每次 publish 全删全建——与真实 DatasetFile id 无关。已删除该错断言与 files_index 路由/helper/fake 镜像；A/B 已知 fileID 历史下载＋views/at B 集合即证语义。
2. **h13 重设计**：真实 ticks created_at DESC→按 `scheduled_for` ASC 排序后才关联 run（须 Succeeded＋全值）。busy 换**专用新鲜非空 scheduled def**（原 def 两批后耗尽、exhausted 空确认在 slow knob 生效前就返回）；hold run 按 `scheduled_for+60s−3s lead` 确定性先于真实 next due 启动（5s knob；绝不假设固定 4s 跨任意 60s cron）；断言 busy tick `scheduled_for` 推进（cron 游标）＋无新 run 行＋DATA cp epoch/value 不动＋owner 真实被非终态 run 持有（**live 模式另证 fixture in-flight active>0**；mocked 只证 API 级事实并记录快照）；arm gate 升级为 view **`live:true`**（调度循环就绪检查）。准入 stamp（`connections.sync_last_admitted_at`）为 SQL 隐藏事实→由 driver 只读供给（HTTP-INTERFACE.md §7）。
3. **h1 ledger 基线**：`len(fixture_ledger)` 于本 attempt 首次拉取前捕获切片（原硬 0 在 fixture 残留历史下必错）；从不清 ledger、不把历史当 CP 位置。
4. **views 多重集合**：`verify_latest_view` 改为 (logical_path, 归一化 physical 路径（local:/// 剥离）, size_bytes, introduced_by) **精确多重集合＋基数**比较＋tuple 唯一映射（杜绝同 txn 双映射/重复文件混过），先全匹配后按映射 fileID 全量下载。
5. **h10**：默认 healthy 相**排除 h10**（只经 `--only h10` 由 driver 显式调）；post **必需 `--h10-restart-record`**（driver 真实容器重启的记录，`restarted:true` 硬断言；缺记录即 gate 失败——绝不以引擎自身调用冒充 restart）；比对升级为**全部公开保护字段**：run-id 集合恒等＋每行 status/generation/attempt/control_mode/publish_entered_at 逐字段相等（同 id 的 generation/attempt 移动＝takeover＝失败）＋非终态仍非终态＋业务 blocked_reason。
6. **h12 严格 schema**：**恰三 stage**（new-normal/new-race/unified，缺一不可绿）＋每 stage `original_rc==0`＋`.rc` 文件内容恰 "0"＋meta sha before==after==candidate＋**每个必需名在每个 stage raw 中恰 1 父级 `=== RUN` 行＋恰 1 父级 `--- PASS:` 行＋0 SKIP＋0 FAIL**（精确行匹配带边界，子测试行不算、子串不算）。对真实 r17 三 raw 复验成立。
7. **tail 严格化**：`--phase tail` **必需** `--tail-declaration`＋`--tail-bindings`（缺一 rc3 零网络）；引擎重验 schema=`p25-tail-roster-v1`/ok/`declaration_sha256` 重算相等；每个声明 tag 必须有 UUID 绑定（def/source/run 三字段）；**观测 owner 集与声明绑定集精确相等**（缺期望 owner＝失败；多观测＝失败；无 subset/count 回退）；run_ref 语义核验（terminal-failed↔failed/aborted，same↔pending/running）。新增 `final_rosters`：场景后（**含失败时**）按 handles.json 全部 def 句柄重列实际 owners＋非终态 runs（覆盖 503 已建 run 但调用方无 id 的情形；owner/run 双 roster 分账；读失败记 problem 并入 failed）。

**新增交付：`HTTP-INTERFACE.md`**（P25 目录内，给 Runtime GLM 独写 driver 的精确契约）：CLI 全面/退出码/env（jwt/jwt_org2/arm_jwt）、部署前提（**p25connector 需 deployment env `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=1`，就绪检查＝arm `live:true`；不改 compose**）、healthy→h10 pre/真实 restart/post→tail 声明→tail 的逐条命令、h10 观察集准备（真安静 def＋真 owner-held def）、restart 记录 JSON 形状、tail 声明/绑定文件形状（h11×4＋h14×1 恰 5 行 owner_rows）、driver 只读 SQL 事实输入（stamp/游标/checkpoint len+sha/ticks/runs 六查询）、输出句柄形状（summary/handles.json/attempt 记录/final rosters）、红线。

**纯自测（真实 cmd+rc）**：`--ast-check`/`--contract-probe`/`--plan`/`--list-gates` 全 rc0（标记更新：scheduled_for/restart-record/tail-bindings 入，files/index 出）；`--selftest-mocked` **rc0 ok:true**（407 attempts / 117 handles；h12 对真实 r17 三 raw 的 15 名×3stage 严格 1RUN/1PASS/0SKIP/0FAIL 全过；h10 无 restart 记录负例＝失败＋有记录正例＝过；tail-decl 四例：精确绑定过/多观测拒/缺观测拒/篡改 sha 拒；finally roster 恰 6 实际 owner 含 h11/h14/h10 全部句柄）；双门单缺各 rc3；tail 缺声明/绑定 rc3 零网络；变异负控两例（token 格式、stage 集合篡改）rc1 且失败原因正确。未跑任何 live HTTP/Go/Docker/DB/q3。

## round-2 附录：arm 精确 wire 字段窄修（2026-11-14 深夜，root 指正）

root 复核发现 v3.1 的 h13 arm gate 检查了不存在的字段：POST schedule:arm
真实返回（schedule_auth.go:666–674）＝`{armed:true, grant_id,
actor_subject, schedule_revision, authorization_expires_at,
scheduler_loop_running:bool, note}`，**无 `live`/`running`**（GET auth 的
live view 是另一契约，不混用）。已窄修：

- 引擎 `_arm_ready(view)`：`armed is True && scheduler_loop_running is
  True`（缺一/False/旧 `live` 形状一律拒绝）；h13 arm gate 报告两真实字段。
- Fake arm 镜像**精确 POST 形状**（armed/grant_id/actor_subject/
  schedule_revision/authorization_expires_at/scheduler_loop_running/note）；
  `scheduler_loop_running` 由 fake 的 `ScheduleRunner.Enabled()` 对应属性
  驱动且 `due_tick` 同受其门控（loop off＝无 tick）。
- selftest 负控五例：真形状通过；缺 flag / loop False / armed False /
  旧 `{live:true}` / None 全拒；fake loop-off 时 arm 报 False 且无 tick。
- HTTP-INTERFACE.md §2 就绪检查同步为 `armed:true 且
  scheduler_loop_running:true`。
- 变异负控：把旧 `live` 接受逻辑改回 → selftest **rc1**，首条失败恰为
  "arm-ready: wrong shape accepted: {'live': True}"——同构假绿已结构性消除。
- Tick list 裸数组＋升序排序维持（root 确认正确）。repo 根的
  `a1bff07f…` 为 PG-import consumed 证据（Runtime 误出路径，root 已纠
  偏），本人不动不清理。

**v3.2 stableSHA256 = `4ad9007092b12b82defe4cbfb6be222750520d04e50f145bd98180fb10955074`**（3658 行；ast/probe/selftest 复跑 rc0、SHA 复算两次一致）。

## round-3 五组真实接线修正已落码并纯自测通过（2026-11-15，Primary）

root `review-smoke-round3.md` 五组全部按其无环方案实现（v4，4432 行，**stableSHA256 = `c2e40de6ee8b6e11fb6c0291151dfc957f9724763e47cd754cd6f6bc5783d26c`**）：

1. **h13 SQL 事实读取器（引擎内建，非 caller JSON）**：`PsqlFactReader`（`--sql-env-dir`→`sql.env` 的 q3 `DATABASE_URL`；单一 READ ONLY 事务五条常量 SELECT、FACT_ 标签自定界解析；只接受引擎自有 Def/Source UUID；cmd 先行落盘、raw stdout/stderr＋原 rc＋SHA 入当次 attempt；DSN 永不记录）。实际 IO 仅在双门后武装（`_SQL_RELEASE_ARMED` tripwire；纯模式拒绝）。h13 重接线：source `request_timeout_ms=30000`＋terminal hold 15000＋due 前 3s 启动；**baseline 在 manual 准入后、busy 前**（manual 准入合法推进 stamp），post 在观测到 busy tick 且 owner 仍持有时；从 actual SQL 证 stamp 不变/cron next_fire 推进/无新 run 行/DATA cp 逐字段不变；busy 窗口错过＝诚实报 miss 绝不宣称；busy 后立刻 revoke+disable 再等 manual 合法结果。Fake reader 同契约（fake connector 补 stamp 每准入推进＋due_tick 推进 next_fire）。
2. **h10 崩溃改造**：pre **不要求全局 quiet**——`--h10-held-def` 的 source 期望真实 in flight（fixture active>0，live 断言），其它观察 def 安静成功（观察集至少 1 quietSucceeded＋1 held）。崩溃＝**一次性 p25connector SIGKILL＋同 accepted image 启动**（graceful restart 会正确取消/释放，与不变预期矛盾——由 Runtime 在 root 放行后执行）；post 要求**真 quiet**（被杀 in-flight 已断）＋零收养/零推进（run-id 集恒等＋每行 status/generation/attempt/control_mode/publish_entered_at 逐字段相等；held owner 行不变，blocked_reason 租约到期后合法翻业务词——两态皆可，已文档化）。
3. **crash record 强 schema**（`p25-crash-record-v1`）：project/service/container/full image sha、kill/start 原始 cmd＋rc=0＋工件 SHA、容器 StartedAt 前≠后、事件时间戳严格有序（pre<kill<start<post）、held 三元组＝pre 快照真实 owner、`pre_snapshot_sha256`＝prior 文件实际字节。`restarted:true` 单字段永不绿（六类畸形纯负控全拒）。
4. **tail 两步（无环）**：(a) 执行前 hash 冻结**语义声明**（tags/modes/期望状态；可选 `known_bindings` 放 spec 外不改冻结 sha；**不发明未来 UUID**）；(b) 引擎在 tail 相后自动 `final_rosters`（全 def 句柄重列＋真实父链 join）→ 按声明匹配输出**实际 bindings**（`tail-bindings-<ts>.json`，schema `p25-tail-bindings-v1`，含 declaration/bindings/summary 三个 SHA；run 侧键 `run/` 前缀防撞名；summary 同步携带 `tail_bindings`/`tail_bindings_file`）→ 用**同一不可变声明**做后置精确裁决。`--tail-bindings` 输入 flag 删除；`--phase tail` 只需语义声明（缺/坏→rc3 零网络）。
5. **双 roster 精确裁决**：owner 集与 nonterminal run 集**双双精确相等**（缺/多/重复 UUID 硬拒；**孤儿非终态 run 硬失败**绝不只记录）；UUID 形状不够——run.sync_def_id 必须＝其 roster def、def.source_id 真实 join（final_rosters 全部携带）。h11/h14 失败句柄与 h10 已有 owner/run 一并纳入（known_bindings 或 mode/semantics 匹配）。

fakes 同步：全部 def/source/run/dataset id 改 **UUID 形**（真 wire 如此，强校验器与父链 join 需要）；FakeApi 增 bare def GET（sync_get）；FakeConnector 增 def_view/准入 stamp/next_fire。`--only h1` 独立首跑入口保留。

**纯自测（真实 cmd+rc）**：ast/probe/plan/list-gates 全 rc0（标记增 sql-env-dir/h10-held-def/sigkill+start-same-image/p25-tail-bindings-v1）；`--selftest-mocked` **rc0 ok:true**（443 attempts/117 handles；SQL tripwire 负控：未武装/武装后外来 UUID 均拒；crash record 六负例：缺 start/坏 rc/同 StartedAt/乱序时间戳/错 prior sha/restarted-only 全拒＋缺记录 post 失败；tail 四负例：缺期望 owner/多余 owner/孤儿 run（父链一致隔离）/篡改 sha 全拒；arm 五负例维持）；变异负控三例：restarted-only 回退→rc1（"crash-record | restarted-only wrongly accepted"）、孤儿容忍回退→rc1（"tail | ORPHAN nonterminal run wrongly green"，负例已隔离父链）、旧 live 形状→rc1。未跑任何 live HTTP/Go/Docker/DB/q3；未重跑已绿 PG/build。

**HTTP-INTERFACE.md 已 round-3 全量重写**（§1 CLI/sql-env-dir、§2 部署、§4 SIGKILL 三步＋强 record schema、§5 tail 两步＋输出形状、§6 句柄、§7 driver 自捕 SQL、§8 红线）——Runtime 按此独写 driver，无缺口。

## 未决 / 下一步

1. 等 root 复核修正后的 source/repo 候选（本 HANDOFF＋contract v1.2 为准）。
2. PG 轨道按 contract v1.2 对齐 `checkpoint_integration_test.go`（新 PG 用例清单含：R1 的 Def 锁等待竞态子例、占用/推进 CAS 负集、R6 基线失配、legacy `{}` 源回归——见 contract §8/PG1-PG11）。
3. Tools 轨道按 §2 协议实现新 fixture（A→B→C、dropped-ACK 页）。
4. 后续 release 后：隔离真实 PG → HTTP 矩阵（design-draft §8）→ 旧回归＋六任务保全 → 归档。
5. q3/真实 PG/HTTP 仍未授权运行；P0-P24 dirty 状态、旧 72、六条 controlled、P24 owned17/archive859/tool27 未触碰。

## 保全红线（不变）

受控集合＝旧 4 pins＋P24 postboot B/C 共 6 条；不为年龄重启；ABC 后零 q3 写；`of-p24-pg`/env600/卷保留；秘密值永不输出。


## 2026-11-15 Sol 接管 / Root review

已按新授权替代 GLM，仅修改 HTTP own files。稳定 h1 SHA 94a57522818d9ec6f49720fc12ef0f26480dc13d159597daa7f824be00589228 已交 Root/Runtime，暂停 smoke 写入等待 h1 后续。h1 函数未动。纯检查 full mock rc0（445 records/119 handles，/private/tmp/http-selftest2.json）；新增 selftest_p25_http_round3.py 11 tests rc0（/private/tmp/p25-http-round3.stderr），无 live HTTP/Docker/SQL/Go。

修复 SQL DSN argv 脱敏错位（改 PGDATABASE 环境）+ raw evidence 原字节/rc/SHA；h13 exact due前baseline、同due唯一busySQL tick/noRun。h10 独立进程新增 --owned-handles-file 恢复source cfg，pre 不再 finally 假全局quiet，post 缺src fail；prior集和CLI强比，crash project/service/fullcontainer/image before-after/kill start exactcmd/raw artifactSHA/health/time严格检查。tail 导入known h10 def重列，mode不fallback/state/duplicates/missingparent硬拒；terminal owner run也作real parent/status join；修summary最后SHA及failed漏落盘，声明沿用启动时immutable内容。

仍待窄修/联调：h10 accepted image 外部pin、inspect labels provenance绑定，h10 protected owner字段完整比较；声明 schema 严格形状/allowed semantics，旧pure p25-lease-lost-dispatched 改 p25-crash-held-prepublish（Root已指定真实prepublish语义）；此文未声称full window已可放行。Root h1完成后恢复。详见 HTTP-INTERFACE。


## Sol 最终候选（覆盖上段待修项）

全部 five round3 groups 窄补完成，稳定 smoke SHA ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2。证据 `results/http-round3-sol/attempt-2/review.json` 及 targeted15 tests rc0/AST rc0；full mocked prior final narrow 476records/119handles rc0（非真实业务证明）。SQL已按Root固定compose exec postgres，不需要hostport/sql.env/DSN。h10新增--h10-expected-image、inspect before/after原工件与SHA，真fullID/labels/Image/StartedAt绑定；CP mode/initialized/last_confirmed与owner since/runstatus保护；post轮询fixture请求结束才真quiet。tail严格语义/count/state/knownUUID、p25-crash-held-prepublish且实际publish NULL，强模式匹配与真实parents。h13异常/早退finally revoke+disable本轮自有schedule，所有Run/Owner保留。

Root唯一例外授权 P25 fixture slow_terminal cap5→30秒（只有此数字改），SHA前1417947ae22d28993a57cd49f66a2ab9ec57d0eded7b6bb5e681e91f1a8bf4be/后8a98e7927f4d8a9e237a6c0a98258c48eb6ab0c0f9d2c5b8df24592820e9788b，pure确认15秒和30秒上限，无sleep/live。Runtime唯一负责保存boot/counters/ledger阶段边界并reload新fixture。h1 trial原失败保留，已wirefix canonicalGET；Root授权Runtime3既存Run readonly GET证明base_epoch0/1/2，未重trigger/reset。后续完整window由修后fixture覆盖h1。


Final h13 enqueue/terminal correction: exact tick.run_id is bounded-polled until terminal, succeeded and real parent asserted before artifact verification; busy-def first tick failure is explicit. Pure fake first GET running / next succeeded covers both scheduled batches and busy-def tick. Final source SHA ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2.
