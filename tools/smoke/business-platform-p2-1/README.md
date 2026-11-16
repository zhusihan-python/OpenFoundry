# business-platform P2.1（租约与持久状态）验收环境

> **状态：P2.1 已验收通过（2026-11-13，run b6ec49ab；证据见
> results/2026-11-13-p21-final/）。** 服务契约按已验收实现固化：HTTP
> 面保持 `POST syncs/{id}/run` + runs 列表；公开 SyncRun **新增**
> `owner_id/requested_by/attempt/generation`（原有公开字段全部保留）；
> `lease_holder/lease_token/lease_expires_at/heartbeat_at` 为私有 DB
> 列，仅可经隔离 connectors PG 白名单投影观测。P1 归档（443e0274）、
> q3v9 PG 与 `dataset-files` 卷全部保留，不清卷、不回填 c12 旧文件。

## 复用关系（不复制代码，指向即用）

| 能力 | 复用来源 |
| --- | --- |
| 隔离栈驱动（setup/up/smoke/tests/migrate-tests/restart/summarize、脱敏与泄漏扫描、guard 四态） | `../business-platform-p1/run.sh` + `run_tests.sh` |
| 合成分页 REST 源（151 条/前导零/失败模式/admin 翻内容） | `../business-platform-p1/fixtures/rest_source.py` |
| 9 场景 smoke（P1 回归门） | `../business-platform-p1/smoke/smoke.py` |
| 三域 Build/Job 与 artifact 持久性 probe | `../business-platform-p1/probe_build_persistence.py` / `probe_restart_persistence.py` |

P2.1 新脚本只放本目录；P1 目录冻结不再改（除根明示的通用修正）。

## 范围（按授权）

SyncRun 持久 `pending → 原子 claim → running → 终态`；holder/token/
generation/heartbeat/expiry；租户 fencing；过期/接管旧 token 拒绝。
手动 HTTP 触发为**同步拉取 + 真实发布 + 持久终态后 202**（已验收）：
不启后台恢复/cron，不存 JWT，不承诺 Dataset 远端 exactly-once。
source active 删除/配置变更策略由 PG 守卫
（`TestPGActiveGuardsBlockSourceAndTargetMutations` 等）验证。

## 验收清单（已全部执行并通过；结果见 results/2026-11-13-p21-final/）

1. **P1 回归门**：原 9 HTTP 场景全 PASS（0 uncovered）、统一 60 包
   unit 基线保持、26 项必需 PG 全 RUN/PASS（P1 名单不因 P2.1 缩减）——已达成（8582e649 / 7a6175a7、60 包、40/40）。
2. **P2.1 必测 PG 名单**：14 名已入 `run_tests_p21.sh`（SKIP→FAIL→
   PASS 优先级 + 顶层 RUN 前置守卫 + guard-probe 自测钩子）并全部
   RUN+PASS（总必测 = P1 26 + P2 14 = 40，logs/p21-required-guard.log）。
3. **HTTP 真实 run 持久状态**：手动触发 202 返回时 run 已达持久终态
   （已验收）；公开 SyncRun 仅**新增**四安全字段，私有租约键在任何
   公开 JSON 各层级缺席（递归键走查，已验收）；raw token 绝不写入
   result/logs/归档（scrub 之外"响应含私有键即 FAIL"守卫）。
4. **租约语义——repo 内部能力以真实 PG 多连接回归为主**：
   - 原子 claim/renew/takeover：14 名**新真实 PG 多连接测试**守卫
     （独立于 P1 的 26 项，P1 冻结名单保持不变）
   - **不新增、不猜任何 HTTP lease endpoint**；明确禁止用并发 POST
     （它只会创建两个不同 run）冒充"同 run 唯一领取"证明
   - HTTP 重点仅限：实际手动 run 的租约接线、heartbeat/失租可观察性、
     DB 不假成功（终态真实）——均已在 A/B/C 验收覆盖；**400/500 一律
     不得当权限/租约通过**
5. **restart 语义**：服务重启后 run/租约状态保留（白名单字段含
   attempt/generation/has_holder/has_token 布尔逐字段相同）；**不自动
   重复发布**、零新 run、零额外拉取——已验收（p21-hard-*.json，
   47s 观察 ≥2 心跳周期）；三域 Build/Job probe 复用通过。
6. **安全**：租约 token / JWT 绝不出现在 result/logs/summary；沿用
   P1 token 注册表 scrub + 泄漏扫描（LEAK_FILES 必须为 0）。
7. **source 生命周期策略**：active 删除/配置变更——PG 层由
   `TestPGActiveGuardsBlockSourceAndTargetMutations`、
   `TestPGDefLockVsRetargetDeterministic`、`TestPGEnqueueVsDeleteBarrier`
   守卫（已 RUN+PASS）。

## 证据目录（本目录内，脱敏）

`results/p2-1-<date>-<tag>/`：summary.md、result.json、probe JSON、
脱敏 logs；.env/token/password 零入库。历史失败同 P1 惯例保留并如实
分层，不以中间态冒充终态。

## 已确定本地契约（2026-11-13 通报；执行前仍按最终候选复核）

- HTTP 面**不变**：`POST syncs/{id}/run` 与 runs 列表；**不存在也不得
  发明 lease HTTP 端点**；租约内部仅可经本隔离栈 connectors PG 观测。
- Public SyncRun JSON 新增 `requested_by/owner_id/attempt/generation`；
  `lease_holder/lease_token/lease_expires/heartbeat` 为私有 DB 列
  （`sync_runs.lease_holder/lease_token/lease_expires_at/heartbeat_at`）——
  smoke 断言这些键在**任何公开响应中缺席**（递归键走查，已验收）。
- 正常手动 run：TTL 60s、heartbeat 20s。

### 正式 HTTP 专项 A/B/C（`smoke_p21.py`，骨架就绪待放行）

- **A heartbeat_progress**：src `delay_ms=25000, delay_from_page=2,
  page_size=1`（每页单条）、Source `request_timeout_ms` 合法大于延迟；
  POST 后台线程执行；PG 观察同 run `heartbeat_at` 实际推进且
  `generation/attempt` 不变；最终 202、DB 终态与真实 artifact hash 一致、
  全程响应无私有字段。
- **B lease_loss_fencing**：151 条慢 src；fixture `request_count` 观测到
  第 2 页 inflight 后，仅对该隔离 run 置 `lease_expires_at` 为过去；
  预期 HTTP **409 lease_lost**、无后续页拉取、无 Dataset 发布
  （inflight 中的那一页不算新增拉取）。
- **C terminal_commit_fault**：该 run 专用 PG `BEFORE UPDATE` 终态故障
  触发器（只匹配独立 syncDef、finally 撤销）：REST→Dataset 发布后终态
  落库报错——HTTP 必须明确非 202/不确定、DB 不得假 succeeded；**远端
  可能已发布**，不断言目标无变化、不自动重跑。
- 全部注入仅作用于 synthetic 目标；真实 handle 前后快照；无 token
  输出/归档。

### 候选 PG 名单（14 名，已从 `repo/lease_integration_test.go` 读出并经
静态审查；`TestPGEnqueueVsRetargetBarrier` 已被
`TestPGDefLockVsRetargetDeterministic` 取代，另增
`TestPGLeaseTTLAnchoredToPostLockClock` 与 `TestPGLeaseTenantIsolationMatrix`）

见 `run_tests_p21.sh`（SKIP→FAIL→PASS 优先级的四态守卫，含 guard-probe
自测钩子；P1 的 26 项冻结不删）。**总必测 = P1 26 + P2 14 = 40**，最终
名单与放行以服务实际全 PASS 后的根定稿为准。

### 公开/私有字段口径（已确定）

- 公开 SyncRun JSON **安全新增且仅**：`requested_by / owner_id /
  attempt / generation`。
- `lease_holder / lease_token / lease_expires / heartbeat(_at)` 为
  **私有 DB 列**（`sync_runs.lease_holder/lease_token/
  lease_expires_at/heartbeat_at`），只可经隔离 connectors PG 白名单
  投影观测（`run_row()` 永不 SELECT token 值）；smoke 以**递归键走查**
  断言私有键在任何公开 JSON 各层级缺席，泄漏证据只含 key+JSON 路径、
  绝不回显 body。

### C 场景反假阳性（已内建）

C 必须实际证明 REST→Dataset **已提交**（目标恰好新增 1 个已提交产物、
logical_path 绑定真实 run id、transaction 状态 COMMITTED、lineage
`sync_run_id` 匹配、下载字节 sha256==txn metadata hash 且 151 行逐值
匹配），同时故障 HTTP 为**含本次注入标记的 5xx**、DB 仍非终态且 lease
已领取、公开 JSON **无伪造 dataset_version_id**。缺真实发布（如
files_after=0）一律 FAIL；场景事实为“Dataset 已提交而 Connector 未确
认”，撤触发器后不自愈、不自动重跑。

## 放行与验收记录（已完结）

- [x] 公开 SyncRun 新增四字段（token 缺席为验收断言）——已验收（A/B/C 全 PASS）
- [x] 14 名真实 PG 多连接回归名单——已入守卫并 14/14 RUN+PASS
- [x] heartbeat/失租的 HTTP+DB 正式观察——A（心跳窗口内推进）与 B（409
      lease_lost/无翻页/无发布）已验收
- [x] source active 删除/配置变更策略——PG 守卫 RUN+PASS（见清单第 7 项）
- [x] 放行执行与归档——2026-11-13 全绿（P1 9×2 + 60 包 + 40/40 + A/B/C
      + 重启白名单探针；证据 results/2026-11-13-p21-final/）
