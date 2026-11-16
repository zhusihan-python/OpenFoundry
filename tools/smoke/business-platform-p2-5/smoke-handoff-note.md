# smoke-handoff-note.md — Primary 侧 HTTP smoke 对齐备忘（2026-11-14）

作者：ZCode GLM-5.3（Primary/业务代码所有者）。用途：Root 正式把 `smoke_p25.py` 交接给 Primary 前，先固化**业务实现方视角**的场景/接口对齐点。只读调研产物；未写 smoke_p25.py（等 Tools 确认无并发编辑＋Root 正式交接）。依据：`fixtures/rest_source_p25.py`、`fixtures/dataset_fault_proxy_p25.py`、`smoke_p25.py`（scaffold）、`tools-design.md`、implementation-contract v1.2.1、已冻结业务代码。

## 1. 已读材料清单

- `smoke_p25.py`（229 行 scaffold：`--plan/--contract-probe/--list-gates` 纯本地，无 HTTP 引擎；h1–h15 注册表＋10 个 gate 常量）。
- `fixtures/rest_source_p25.py`（344 行：IncrementalFixture 纯核＋HTTP 适配；src p25h1/h2/h3/h4/h6/h13；knob 面）。
- `fixtures/dataset_fault_proxy_p25.py`（237 行：6 模式；drop_after_verify 先真转发取证再断连）。
- `tools-design.md`、`FIXTURE-PROXY-API.md`（admin 契约）、contract v1.2.1。

## 2. 路由/接口对齐（vs 已实现代码）

| Scaffold ROUTES | 实际实现 | 状态 |
| --- | --- | --- |
| checkpoint_view `/api/v1/data-connection/syncs/{id}/checkpoint` | server.go 挂载（`api.Get("/data-connection/syncs/{id}/checkpoint", h.GetSyncCheckpoint)`，前缀 `/api/v1`） | ✅ 已互相钉正（contract §7 已同步全路径） |
| sync_run/runs/schedule:arm/ticks | server.go 既有族 | ✅ |
| **scaffold 缺**：source config PATCH（h7 围栏） | **`/api/v1/connections/{id}` PATCH**（UpdateConnection——注意 sources/{id} 族无 PATCH；server.go:157，/api/v1 组内） | ⚠️ smoke 实现时补 ROUTES（已核 server.go 确切拼写） |
| **scaffold 缺**：credentials 写入口（h7 凭据围栏） | **`/api/v1/data-connection/sources/{id}/credentials` POST**（SetCredential，server.go:110） | ⚠️ 已核拼写 |
| **scaffold 缺**：source DELETE（h7 删除围栏可选） | `/api/v1/connections/{id}` DELETE 或 `/api/v1/data-connection/sources/{id}` DELETE（两族都挂 DeleteConnection，server.go:107/158） | 备忘 |
| **scaffold 缺**：run 单查 `syncs/{id}/runs/{run_id}`（h 场景要断言 `incremental` 投影） | 已实现（GetSyncRun 返回 `{...run, "incremental":{mode,base_epoch}}`） | ⚠️ 补 ROUTES |

响应字段对齐（已实现方确认）：409 busy body＝`{code:"sync_incremental_busy", active_run_id, active_since, retry_hint}`（retry_hint 为业务措辞 requires_reconciliation，无 P26 字样）；stale＝`sync_incremental_binding_stale`＋recovery 提示；围栏＝`sync_incremental_owner_unresolved`（message 含 def/run id）；tick outcome＝`skipped_incremental_busy`/`error_binding_stale`；CheckpointView＝`mode/initialized/checkpoint_len/checkpoint_sha256/confirmed_epoch/owner{run_id,run_status,since,blocked_reason}/last_confirmed{...}`，`blocked_reason` 仅 `requires_reconciliation|""`。

## 3. Fixture/Proxy 协议 ↔ 业务行为逐点核对

**Fixture（rest_source_p25.py）**
- 固定参数 `since_token`＋终页 `meta.completed_token`：与 contract §2 完全一致；我的 runtime 把 request_param 放进 baseQuery ⇒ **每页同值**，fixture ledger `/admin/log/<src>` 的 `token` 列即 per-page 证据（h1 G_LEDGER）。
- 分页：connector cursor 型**不发 page_size**；fixture 默认 50/页，各 batch 页（12/12、8/8/4、30、…）均 ≤50 ⇒ 不会意外分页裂页。records 路径 `records`、next 路径 `next_cursor` ⇒ smoke 的 source config 用 `records_path:"records"`、`next_cursor_path:"next_cursor"`。
- `window_for`：token 重放（重试）→ 重serve 同窗（有界 at-least-once）；未知/外来 token → 400 ⇒ connector 拉取失败（pre-gate）⇒ `failed`＋**释放**＋位置不动（h4/h8"外来 token"分支）。
- knob→业务错误映射：`candidate_mode=missing|non_string|empty|oversize|ctrl_chars` → `ErrIncrementalCandidateInvalid`；`echo_request_token`（rows>0 候选未前进）→ `ErrIncrementalNoProgress`；两者都走 pre-gate `FailIncrementalSyncRun`（gate 未入 ⇒ 释放）。`fail_mid_pages`/`fail_terminal_after_rows` → 拉取期 500 → 同上释放；**下一 run 同 token 整批重读**＝fixture 重放语义 ⇒ h8 断言"位置未消费"。`empty_batch`/无新窗（终页 records:[]＋candidate==confirmed）→ R14 成功＋epoch+1＋value 不变（h3）。
- **可选缺口**：fixture 无 U+FFFD/lone-surrogate 的 wire 级 candidate_mode（我的单测已覆盖 connector 侧；若 Tools 想在 HTTP 层复演，加一个 `surrogate` knob 即可——Tools 文件，仅备忘）。

**Proxy（dataset_fault_proxy_p25.py）**
- `no_txn_200` → adapter "response has no transaction id"；`not_committed` → "transaction status is not COMMITTED"；`wrong_target` → "response dataset does not match the requested target"；`http_500`(502) → DependencyError。四者全部 dispatch 后无 usable ACK ⇒ `failed`（Run-only CAS）＋**owner 保留**（h11 G_RETAIN_UNKNOWN）；proxy ledger `forwarded:false` ⇒ 零远端发布可证（"no new version"措辞禁用合规）。
- `drop_after_verify` → connector 见 publishCtx 超时 ⇒ failed＋保留；远端 COMMITTED 证明＝proxy ledger（upstream_txn_id/status）＋直连 dataset GET（txn/file/hash）（h14①）。
- passthrough(non-commit) 转发 GET ⇒ precheck GetDataset 经 proxy 正常。
- 注意：aux connector 的 `DATASET_SERVICE_URL` 指向 proxy；**C 直连/live 校验走真实服务**（scaffold 注释一致）。

## 4. 场景↔已实现断言点（h 编号→关键机械判定）

- h1：3 run A→B→C；断言 run.status=succeeded、`incremental.base_epoch` 递增、checkpoint epoch 1→3、每批文件按 run→txn→file 下载逐值==batch 行（含 provenance 前缀 a-/b-/c-）；ledger token 链 p25h1-tok-0/1/2。
- h2：V1(A)=A 整批、V2(B)＝分支 HEAD 逐值==B；**不得**用 files_total==1 证明（用分支生效视图/按版本下载）；两 txn 均 COMMITTED。
- h3：二跑空批 → succeeded＋epoch+1＋checkpoint_len/sha 不变＋owner 释放。
- h4：候选无效族 → failed＋释放＋断言 run.error 无 token 原值（只有长度类措辞）。
- h6/h15：占用中二触发 409 busy（sync+async）；proxy ledger 零第二次 forward；sibling def 并行成功；async 一例真实成功批次。
- h7：owner 未决时 PATCH source config / PATCH sync target|file_glob / credential 写 → 409 `sync_incremental_owner_unresolved`；同值 config/凭据与纯 name/cron/enable 改动不触发；binding_stale 触发路径＝改 config 后新 run 409 stale。
- h9：gate 前取消 → run aborted、`publish_entered_at` NULL（run 视图可见 NULL）、proxy 零 forward、checkpoint owner 释放、位置不动。
- h11/h14：见 §3；h14②③（lease loss/local commit unknown）按 addendum 走 PG/tail，不在 HTTP 引擎内伪造。
- h13：scheduled ≥2 tick 成批；busy tick：tick outcome＋cron 游标前进＋（DB 断言属 PG）——HTTP 侧可断言 ticks 列表与 checkpoint epoch 不动。
- h5：legacy def 走旧回归（scaffold 明确不新增断言）✓。

## 5. 交接前待办（等 Root/Tools）

1. Tools 确认 smoke_p25.py 无并发编辑 → Root 正式交接给 Primary。
2. 接手后补 ROUTES：run 单查、sources PATCH、credentials 写入口（核对 server.go 确切拼写）。
3. （可选）fixture surrogate knob；h4 的 wire 级 U+FFFD 复演。
4. HTTP 执行仍需 root 的 q3/runtime release——本 note 不构成执行授权。

## 6. 交接后实现状态（Primary，2026-11-14 晚）

**smoke_p25.py v2 已实现**（Primary 唯一写权；Tools 已确认不再编辑）：
- ROUTES 补齐 4 条：`run_get`、`connection_patch`（/api/v1/connections/{id}，实测拼写）、`credentials_post`、`run_cancel`；另钉 DATASET_ROUTES（version_get/files_list/file_download/branch_preview，dataset server.go 实测）。
- 场景 h1–h4/h6–h9/h11–h15 全部实现（h5 归因 only；h10 driver 重启探针；h12 绑定 --pg-proof-file，绝不伪造 PG）。r2 修正语义已内置（NULL→首窗、终页 token 由请求窗派生、单谓词 ACK 故障、exhausted 空批、echo=no-progress）。
- 证据纪律：AttemptRecorder 每次调用一条不可覆盖 raw 记录（-rN 防撞、attribution、无 Authorization/JWT 值）；phase 汇总含 owner_roster/run_roster 分账与 finally 全源 quiet（fixture active0＋started==finished；proxy inflight0＋started==finished）。
- 执行门：真实 HTTP 双门（--released ∧ env P25_CONFIRM=yes），否则 rc3 零网络调用；当前仅纯检查可跑：--plan/--contract-probe/--list-gates/--ast-check/--selftest-mocked（进程内 fakes，无 socket/listener）。
- **mocked selftest 现状**：`ok:true`（256 attempts；覆盖 h1-h4/h6-h9/h11-h15 编排＋h12 绑定＋取消释放直检＋tail roster 记账＋quiet）。fake 层＝r2 修正 fixture/proxy＋契约 v1.2.1 状态机（含 full usable-ACK 校验、异步延迟执行、ticks 累积合成、name-PATCH 不围栏）——**验证的是引擎逻辑，不是真实 connector**。
- 决策记录：所有 fixture 场景默认走 **aux connector（p25connector）**（egress 明确含 p25fixture；healthy 窗口 proxy=passthrough，零 dispatch 证明统一走 proxy 账本）；h13 需要 aux 开调度循环（driver/compose 侧，已在 registry 注明）。
- 待 root：runtime release 后先跑 --phase healthy（driver 按 tools-design §6 编排）；对 FIXTURE-PROXY-API 如有 r3 变更需同步本引擎（目前按 r2）。

## 7. 对 Tools 的协调请求（h4 故障族，2026-11-14；root 中转）

review-smoke-round1 #4：候选故障需要"专用多窗故障 src 家族"以避免与正向场景共用 src（当前 v3 方案暂用 p25h2 A→B 承载六模式＋reached-proof，可继续工作；专用族更干净）。请求（不自行修改 Tools 文件）：为六种 candidate_mode 各配一个独立 src（如 p25f_missing…p25echo），每个＝两窗（首窗正常、次窗非空终页施加该模式），保持 r2 确定性 token 语义（NULL→窗 0、T_i→窗 i+1、终页 token 由请求窗派生）。若 Tools 提供，smoke h4 改用之；否则维持 p25h2 方案。

## 8. §7 协调请求已满足（2026-11-14 深夜）

Tools 已作为唯一 fixture 写者为 **p25h4 增加第二非空窗 B(8)**（pure 34/34）——h4 直接使用原生 p25h4（seed A→B 非空终页施加 candidate_mode），§7 的专用故障族请求**作废**。smoke v3 的 EXPECTED_WINDOWS 已按 Tools r3 布局与复合行格式（`%s-%03d|name-%s%d|qty-%d`）钉入 HANDOFF。

## 9. v3 交付状态（2026-11-14 深夜续会话，Primary）

**smoke_p25.py v3 已落码**（review-smoke-round1 十组 wire 修法全部实现；3202 行，stableSHA256 `52f55aa200e38ffa8e6698b5f8031929c5321d2850bce5ac5e5a9fa14a228e1a`）。对 §6 的增量：

- ROUTES：dataset 面整体迁移到 **/v1**（dataset_create/files/files\/index/file_download/branches·transactions/views\/at）；versions/preview/rows 路由删除。connector 面不变（v2 已钉 4 条补充路由）。
- 新 CLI：`--h10-stage pre|post`、`--h10-defs`、`--h10-prior`、`--tail-declaration`、`--pg-proof-name`（默认钉 `TestPGCheckpointAdvanceAllOrNothingRollback`）、`--pg-proof-file` 默认指向本目录冻结 `pg-candidate-bindings-r17.json`；org2 JWT 走 env `OPENFOUNDRY_SMOKE_JWT_ORG2`（h7 必需，缺失即 gate 失败）。
- 证据：HttpApi 记录完整安全 JSON body；`handles.json` 每次 create/attempt 即时落盘（含失败尝试）；quiet 失败并入 failed→rc≠0。
- 纯自测全绿（ast/probe/selftest-mocked/plan/list-gates rc0；变异负控 rc1）；真实 HTTP 仍双门 rc3。mock 绿仅证引擎逻辑，不是 connector 验收。
- Driver 对接备忘：h10 restart 前后分别 `--only h10 --h10-stage pre/--h10-prior <f>` 与 `--h10-stage post --h10-prior <f>`（defs 来自 healthy 相）；tail 声明由 `p25_tail_roster.py declare` 产出后以 `--tail-declaration` 传入；h13 需 aux connector 开调度循环（armed 后 cron */1，busy tick 等待窗 ≤130s×2）。

## 10. round-2 交付状态（2026-11-14 深夜，Primary）

review-smoke-round2 七组全部落码（v3.1，stableSHA256 `1b528a0b472ea9c5b245390108b7973cff4e3bdceaca25537d0437bd07067be5`），§9 增量如下：

- 删除 files/index 断言与路由（entry UUID 独立且每 publish 重建）；h2 由历史下载＋views/at 证明。
- h13：scheduled_for ASC 排序；busy 用专用新鲜非空 scheduled def＋按实际 next due 确定性 hold（非固定延迟）；busy tick 证 scheduled_for 推进＋无新 run＋DATA cp 不动；arm gate 检查 view `live:true`；stamp 事实走 driver SQL。
- h1 ledger 基线＝attempt 首 fetch 前的 len。
- views/at 精确多重集合（logical/归一化 physical/size/introduced_by）＋唯一映射。
- h10 默认 healthy 排除；post 必需 driver 的 `--h10-restart-record`；比对含 generation/attempt/control_mode 全公开保护字段。
- h12 恰三 stage＋rc==0＋每名每 stage 1RUN/1PASS/0SKIP/0FAIL（精确行匹配）。
- tail：`--phase tail` 必需 declaration＋bindings（rc3 拒绝）；sha 重验＋精确集合匹配（缺/多皆败）；finally 按全部 def 句柄重列双 roster（含失败时）。
- **新文件 `HTTP-INTERFACE.md`**：Runtime GLM 独写 driver 的全部输入输出契约（CLI/env/bases/执行序/h10 三步与 restart 记录/声明与绑定形状/只读 SQL/输出句柄/红线），不留"driver 负责"缺口。
- round-2 附录窄修（v3.2，SHA `4ad9007092b12b82defe4cbfb6be222750520d04e50f145bd98180fb10955074`）：POST arm 真实形状＝`armed`+`scheduler_loop_running`（无 live/running）；`_arm_ready` 双 True 硬断言＋fake 精确镜像（loop 可关、due_tick 受门控）＋selftest 五负控＋旧形状变异必败（rc1 首条即 arm-ready 拒绝）＋HTTP-INTERFACE 就绪检查同步。

## 11. round-3 交付状态（2026-11-15，Primary）

review-smoke-round3 五组全部按 root 无环方案落码（v4，SHA `c2e40de6ee8b6e11fb6c0291151dfc957f9724763e47cd754cd6f6bc5783d26c`）：

- h13：引擎内建可注入只读 SQL 读取器（`--sql-env-dir`/q3 DATABASE_URL/READ ONLY/常量 SELECT/自有 UUID/原 cmd+raw+rc+SHA/tripwire）；baseline 在 manual 准入后 busy 前；30s 超时＋15s hold＋due 前 3s；busy 由 actual SQL 实证（stamp/next_fire/runs/cp）；miss 诚实上报。
- h10：pre 不要求全局 quiet（held src 真 active＋其余 quiet-succeeded）；SIGKILL＋同镜像（非 graceful restart）；post 真 quiet＋零收养（held owner 行不变，blocked_reason 到期翻业务词已文档化）。
- crash record 强 schema（六负例纯拒；restarted-only 永不绿）。
- tail 两步：语义声明（无未来 UUID；known_bindings 在冻结 sha 外）→ 引擎输出实际 bindings（`p25-tail-bindings-v1`＋三 SHA；run/ 前缀）→ 同声明双 roster 精确裁决（孤儿 run 硬败；父链 join 非仅 UUID 形）。`--tail-bindings` 输入已删除。
- fakes 全 UUID 化＋sync_get/stamp/next_fire 镜像；`--only h1` 独立首跑入口保留。HTTP-INTERFACE.md round-3 全量重写（Runtime driver 契约无缺口）。


## 2026-11-15 Sol 接管 / Root review

已按新授权替代 GLM，仅修改 HTTP own files。稳定 h1 SHA 94a57522818d9ec6f49720fc12ef0f26480dc13d159597daa7f824be00589228 已交 Root/Runtime，暂停 smoke 写入等待 h1 后续。h1 函数未动。纯检查 full mock rc0（445 records/119 handles，/private/tmp/http-selftest2.json）；新增 selftest_p25_http_round3.py 11 tests rc0（/private/tmp/p25-http-round3.stderr），无 live HTTP/Docker/SQL/Go。

修复 SQL DSN argv 脱敏错位（改 PGDATABASE 环境）+ raw evidence 原字节/rc/SHA；h13 exact due前baseline、同due唯一busySQL tick/noRun。h10 独立进程新增 --owned-handles-file 恢复source cfg，pre 不再 finally 假全局quiet，post 缺src fail；prior集和CLI强比，crash project/service/fullcontainer/image before-after/kill start exactcmd/raw artifactSHA/health/time严格检查。tail 导入known h10 def重列，mode不fallback/state/duplicates/missingparent硬拒；terminal owner run也作real parent/status join；修summary最后SHA及failed漏落盘，声明沿用启动时immutable内容。

仍待窄修/联调：h10 accepted image 外部pin、inspect labels provenance绑定，h10 protected owner字段完整比较；声明 schema 严格形状/allowed semantics，旧pure p25-lease-lost-dispatched 改 p25-crash-held-prepublish（Root已指定真实prepublish语义）；此文未声称full window已可放行。Root h1完成后恢复。详见 HTTP-INTERFACE。


## Sol 最终候选（覆盖上段待修项）

全部 five round3 groups 窄补完成，稳定 smoke SHA ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2。证据 `results/http-round3-sol/attempt-2/review.json` 及 targeted15 tests rc0/AST rc0；full mocked prior final narrow 476records/119handles rc0（非真实业务证明）。SQL已按Root固定compose exec postgres，不需要hostport/sql.env/DSN。h10新增--h10-expected-image、inspect before/after原工件与SHA，真fullID/labels/Image/StartedAt绑定；CP mode/initialized/last_confirmed与owner since/runstatus保护；post轮询fixture请求结束才真quiet。tail严格语义/count/state/knownUUID、p25-crash-held-prepublish且实际publish NULL，强模式匹配与真实parents。h13异常/早退finally revoke+disable本轮自有schedule，所有Run/Owner保留。

Root唯一例外授权 P25 fixture slow_terminal cap5→30秒（只有此数字改），SHA前1417947ae22d28993a57cd49f66a2ab9ec57d0eded7b6bb5e681e91f1a8bf4be/后8a98e7927f4d8a9e237a6c0a98258c48eb6ab0c0f9d2c5b8df24592820e9788b，pure确认15秒和30秒上限，无sleep/live。Runtime唯一负责保存boot/counters/ledger阶段边界并reload新fixture。h1 trial原失败保留，已wirefix canonicalGET；Root授权Runtime3既存Run readonly GET证明base_epoch0/1/2，未重trigger/reset。后续完整window由修后fixture覆盖h1。


Final h13 enqueue/terminal correction: exact tick.run_id is bounded-polled until terminal, succeeded and real parent asserted before artifact verification; busy-def first tick failure is explicit. Pure fake first GET running / next succeeded covers both scheduled batches and busy-def tick. Final source SHA ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2.
