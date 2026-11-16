# business-platform P2.2（可中断 worker）验收工具 — 骨架 v2

> **状态：工具 v4.2 FINAL——静态终审全部通过**（D 的 rc/RFC3339/
> config-300 路径关闭）。**主修复已生效并只读核实**：触发体 empty/`{}`
> TCP 断连即时取消；>1MiB→413、读超 5s→408、读错→400（零持久行）；
> 队列满仍 503。剩余为测试 resp.Close 语义等收尾+race，非业务设计。
> **只读基线已实采归档**（含关键事实：运行栈为 P22 前旧构建，正式
> release 需一次 fixture restart 加载 barrier——计数清零语义已入
> §钉定与 EXECUTION-PLAN）。正式栈联调待 root formal release（勿 up）。**
> 本地自测现值：守卫 16、fixture 48、代理 33。P0/P1/P21 未提交改动与
> 归档原样保留；q3v9 栈与卷不动；50 必测名单与无 .env/私有材料归档
> 保持。当前任务与最终证据日期一律 2026-11-14（P21 历史日期不改）。

## 授权边界（两轮累计）

1. 首轮：改动限新 `tools/smoke/business-platform-p2-2/`。
2. 根补充（2026-11-14）：**唯一共享文件**
   `../business-platform-p1/fixtures/rest_source.py` 允许最小增量扩展
   ——按 src/page 的可释放 barrier 与 started/finished/active 安全计数，
   默认未启用时 P1/P21 行为不变；不得持全局锁等待；barrier 有限等待
   +finally 释放；源控制不保存/回显平台 JWT 或敏感 headers。P1 其余
   脚本与归档零改动（已核验）。

## 复用关系（不复制代码，指向即用）

| 能力 | 复用来源 |
| --- | --- |
| 隔离栈驱动/脱敏/泄漏扫描 | `../business-platform-p1/run.sh` + `run_tests.sh` |
| 身份铸造/HTTP/psql/fixture admin/artifact 下载/私有键走查 | `../business-platform-p1/smoke/smoke.py`（importlib 加载） |
| 合成分页 REST 源 + **P2.2 barrier 扩展**（已落地） | `../business-platform-p1/fixtures/rest_source.py` |
| P1 九场景 / P21 A/B/C 回归门 | `../business-platform-p1/`、`../business-platform-p2-1/`（冻结） |

## 文件清单

| 文件 | 作用 |
| --- | --- |
| `smoke_p22.py` | HTTP 专项骨架（场景 D–M ↔ 根八项，见矩阵）；契约候选常量置顶，缺列/缺路由/未钉定一律 FAIL closed |
| `run_tests_p22.sh` | P2.2 必测 PG 名单守卫（四态；顶层 RUN+PASS 精确同名；**distinct 名计数**；名单文件缺失/空=integration 拒绝放行） |
| `p22-required-names.txt` | **根定稿 10 名单**（2026-11-14；总必测 26+14+10=50；候选内部在补 before 快照与 flag+fence 组合但顶层名不变） |
| `p22-required-names.txt.example` | 名单文件格式说明（真实名单已就位，此文件仅存档格式规则） |
| `selftest_guard.sh` | 守卫合成日志自测（13 例） |
| `selftest_fixture.py` | fixture 扩展本地自测（48 例；RAW 字节奇偶性 + generation 反例） |
| `dataset_proxy.py` | **隔离栈受控代理**：真实上游先处理再扣响应，释放/到期后原样回放；永不伪造/改写字节；有界 hold；同 id 在途重装 409 拒绝；mid-flight 已释放请求不计 held；admin 面不携带敏感头 |
| `selftest_proxy.py` | 代理本地自测（33 例；busy-reject 与 release-during-upstream 确定顺序回归、精确 bearer 匹配记录） |
| `selftest_all.sh` | 统一准备自测驱动：跑全部本地检查并把**原始日志+退出码+manifest** 归档到 `results/preparation/` |
| `readonly_baseline.py` | **只读**预检/基线采集器（栈健康/镜像/.env 权限仅键名；P21 B/C 安全投影+父链+目标 files/txn+fixture 计数；P1 旧 artifact/Build/Job 真实 GET200+sha256；绝不启停任何服务；失败如实记） |
| `EXECUTION-PLAN.md` | 正式执行序（final，待 root formal release）：rc 捕获纪律（PIPESTATUS，不把 tee 0 当测试 0）、三 guard 分调同一真 PG 日志、unit 实际包数、PG50、P1 pre9/P21 ABC/P22 D..N/三服务 restart 保留 fixture+旧对象再读+P1 post9、分步归档与红线 |
| `compose-p22-connector2.yaml.template` | 增量 override：connector2（跨进程取消）、dataset-proxy、connector3（经代理发布） |
| `p22_connector2.sh` | aux 驱动（见下节）；`up` 需 `P22_CONFIRM_CONNECTOR2=yes` 联锁 |
| `results/preparation/` | 准备期自测原始证据（时间戳目录，不重构不覆盖） |

## aux 副本驱动（p22_connector2.sh，review 修正 1 后语义）

- **仅 aux 范围**：`up` 对**硬编码白名单三服务**（connector2/dataset-proxy/connector3）执行 `up -d --no-deps`——compose v5 up.go 中 `--no-deps`→IgnoreDependencies，不会把 override 声明的 base 依赖（postgres/dataset）纳入启动/创建/reconcile。服务名不出自环境变量、不可能空列表退化全栈 up。
- **先只读核 base**：up 前用 `compose ps --format json`（只读）确认 postgres healthy、dataset running；不满足即拒绝（exit 1），绝不代启 base。
- `down` 仅 `rm -sf` 三 aux（无 -v），base 不触碰。`render`/`status` 安全。

## 独立只读 review 修正记录（2026-11-14，已全部落地）

1. **aux up 依赖泄漏**（见上节）。
2. **proxy 同 hid 重装孤儿化**：Hold 增 `inflight` 捕获计数（match 即捕获、应答后释放）；在途重装 → **409 busy 拒绝**（旧 hold 上的等待者不可达释放/不可观测最长 900s 的问题不再可能）；确定顺序回归已入 `selftest_proxy.py` 6b/6c。
3. **fixture barrier 代数缺口**：改为**每次安装生成独立 gate 对象**，请求在 state.lock 内于入口捕获 gate——重装只影响后续请求，旧代请求按自己代的释放标记继续（pre-wait 窗口由构造闭合）；确定顺序反例（R1 停泊 g1 → release g1 → 立即重装 g2 → R1 仍完成；R2 停泊全新 g2 且无旧标记）已入 `selftest_fixture.py` 7b/7c。单次安装 release-before-wait 语义未回退（原 5 号用例保持）。
4. **held_now 语义澄清**：proxy 计数只证明**上游响应已收到**，不证明 Dataset COMMITTED——tooLate/丢响应场景的提交证明仍在真实 txn/file/hash（G 的 `prove_committed_artifact`）与 `publish_entered_at` 门观察。
5. **（A）proxy 自测精度**：stub 只记录 `Authorization == 本例 SECRET` 的**布尔精确匹配**（不存不回显实际 token；错误 bearer 记 False 为阴性对照）；新增 **release-during-upstream** 确定顺序用例（stub 慢处理中先 release、upstream 返回后立即应答且**不计 matched/held_now**）；fixture 奇偶性改为 **RAW 字节对比**（解析 JSON 相等不再标注 byte-identical，跨 src 仅声明 data 级一致）。
6. **（B）M 场景事务封套**：改用只读核实的 `models.Page {data:[...]|null, next_cursor?(omitempty), has_more}`（transactions.go:135-174）——未知封套**明确 FAIL**不退化恒等；null data 按合法 nil 空处理；本例要求 `has_more==false`；COMMITTED 目标须 ≥1 **真实 txn 且绑定本 Run 产物**（id ∈ 列表且 status==COMMITTED），重启前后按 **ID 集合**比较——0==0 不再可能冒充无重发。

### 正式 smoke 静态预审修正（第三批 8 项，2026-11-14，up 前整体修）

7. **同域构造**：`owned_org_sync` 支持 owner/org 显式复用——F 的第二 sync 与全部 fills、H 的错父 sync 都建在**同一可见租户**并由 owner 正向访问后再做拒绝；H 的“同 Sub 双 Org”改用 **owner 的真实 sub** 配他 Org token（不再随机 dual_sub）。
8. **G 去停机**：新 executor 无 DB 扫描——经 connector3 准入即归其队列；删除对 base/connector2 的 stop/start（曾使默认 via 的 get/cancel/poll 必连不上）；取消改经 **base 实例**（顺带即跨实例证明）；proxy release 保留在 finally。
9. **D 窗口证明**：双条件数值断言——heartbeat 推进 **且** 准入后**持续 inflight ≥35s** 才释放（`request_timeout_ms=60000`），`advanced_at/inflight_s/continuously_inflight` 全部入证据。
10. **J deadline 判别**：专用 aux——`p22_connector2.sh up-deadline <s>`（联锁+base 健康只读核+**活任务守卫** psql count + 仅 connector2 `--no-deps` + 白名单 env 读回）设 `TASK_TTL=7s`，source HTTP 60s/Auth 300s 更长；断言 elapsed 在 TTL 尺度且 **<50s**、失败详情含 deadline 味原因（确切措辞待根钉）、零发布、无僵尸拉取；`restore` 子命令恢复原 aux 配置。
11. **I 停机证据**：`stop -t 120`；每副本**白名单 inspect**（Running/ExitCode/OOMKilled/FinishedAt）要求 exit0/非强杀；门前阻塞 run **真实终态化**（aborted|failed）；停稳后、重启前取**稳定基线**（页数/文件/runs/全投影/legacy）；重启后**全投影相等**（扩展 run_row：+attempt/generation/content_hash/dataset_version_id/finished_at）；fixture 不重启。
12. **M 固定 cohort**：completed/cancelled/interrupted+P21 B/C 逐成员绑定；全投影、源 request_count、目标 file 集、txn id→status 映射前后比较；绑定 txn 仍 COMMITTED；artifact 两次下载**先断言 HTTP 200 再 hash**。
13. **K 断连语义**：断连≠取消 API——真实终态（aborted 或 failed **且失败原因非空**）；不再硬等 aborted。
14. **L JWT 证明链**：须**已入发布门** + 失败详情含认证拒绝标记（候选集，措辞待钉）+ **零 txn 零文件**；owner 新鲜 token 查询保持。

### v4.0 复审修正（第四批 5 项，2026-11-14，静态终审前）

15. **（A）J 活任务守卫进程边界**：全库 pending/running 必被必须保留的 P21 B/C running 阻塞——改为 **connector2 当前进程边界**：要求 aux2 存在/健康/白名单 `State.StartedAt` 合法，守卫 SQL 为 `control_mode='controlled' AND status IN ('pending','running') AND started_at >= aux2StartedAt`（保守可多计同窗其它实例、绝不漏本实例；不选 raw holder/token 列）；**psql rc 单独校验 + stdout 严格数字**后才能比 0——失败经 tr 变空默认 0 放行的路径已消除。
16. **（B）I 停机取证修正**：容器 ID **停前缓存**（带 override 的 compose ps；停后默认 ps 不列停容器、aux 名不在 base 文件）；白名单 State inspect 用缓存 CID；停后/重启前基线的 runs 改 **PG 白名单 run-ID 集合**（HTTP 已不可用；dataset/fixture 仍可读）；**确认进程退出后**先释放旧 source barrier 并**等 fixture active/waiters 归零收敛**再取稳定基线——释放引起的计数变化不再混入 restart 证明。
17. **（C）K 断连判别收紧**：断连单调计时，终态须 **≤15s**（明显早于 30s 路由/60s 源预算——忽略断连只会在 ~60s 源超时失败）；failed 原因须为精确 **context canceled/request canceled/client disconnected** 族；保留真实 `body={}`（无 body 绕过不做——Go HTTP/1 unread-body 行为由根主 GLM 以真实 TCP 单独验证）；失败如实报，不放宽到源超时。
18. **（D）J 恢复强验证**：finally 校验 restore **rc==0**、healthz 绿、容器**确实重建**（StartedAt 变化）、生效配置（非敏感 env 白名单，base+override 合并）**不再携带 7s deadline TTL**——旧的 7s 容器仅 healthy 不算恢复成功，任何一步失败即 FAIL。
19. **（E）L 原因精确钉定**：直接采用实际代码字符串——`dataset-versioning commit_output responded with status 401`（403 同理，DependencyError，Sanitize 不改写）为**完整片段**断言；宽泛 token/jwt 关键词路径删除；主实现已冻结（静态/standalone 全过），仅 TCP concern 在根侧验证且不影响本断言。

### 终审 D 残留最小修复（2026-11-14，v4.2）

20. **J 恢复链 fail-closed 补全**：两次容器 `StartedAt` 采集改为 `inspect_started_at()`——**cmd rc==0 且值匹配严格 RFC3339 正则**（空串/畸形不得进入比较，“空 != 非空”假 PASS 路径消除）；`compose config` 恢复校验改为 **rc==0 + 合并输出确含 `connector2:` 服务**（失败空 stdout 不再使 TTL 检查退化为空列表假过），并**白名单解析生效 TASK_TTL**：缺省按真实默认 300 解释（记录 `default-300` 证据）、显式存在则必须等于 300、绝不为 7s。README 同步清理矛盾描述（G 行“停其余副本”改为事实、main 状态改冻结+TCP 复现事实、自测计数统一 16/48/33）。

## 最小正式 HTTP 矩阵（↔ 根八项建议，1:1 映射）

| 场景 | 根项 | 证明义务（反假阳性内建） |
| --- | --- | --- |
| D `async_admission_and_completion` | 1 | barrier 扣页 1 期间 202 且 POST 已结束；**双条件数值断言**：heartbeat 实际推进 **且** 准入后持续 inflight ≥35s（`request_timeout_ms=60000` 合法覆盖窗口）才释放；释放后**同一 run** 真完成：151 行逐值 + sha256==txn content_hash + txn COMMITTED + lineage 绑定 |
| E `running_cancel_cross_process` | 3 | A 实例（connector）执行、**B 实例（connector2）HTTP 取消**——非 local map；只信持久终态 + cancel_requested 持久化；释放后页数严格冻结、零发布；重复取消幂等不复活 |
| F `queue_pending_cancel_and_queuefull` | 2 | 单 worker 被 barrier 占满：第二 run 保持 pending→cancel 后**该源零 REST 请求**、零产物、重复幂等；队满触发为**显式拒绝**且 runs 快照无孤儿行。需根钉 QUEUE_CONTRACT，未钉定 FAIL closed |
| G `gate_wins_too_late_via_proxy` | 5 | **不停任何其它副本**：新 executor 无 DB 扫描，经 connector3 准入即归其队列；代理在**真实 Dataset 已处理 commit**后扣住响应；`publish_entered_at` 非空观察到入门；窗口内取消经 **base 实例**（跨实例证明）→**409 code=cancel_too_late + body.run**；释放→收真响应→run **succeeded**，绝不改标 aborted |
| H `tenant_and_binding_denials` | 4 | 跨 Org 同 sub（cancel+GET）、跨 Org admin、个人域、**同 Org 无 dataset.write**、错父 Sync（cancel+GET）、畸形 run id：每次拒绝后 runs/目标快照不变；被攻击 run 仍可 owner 取消（活性） |
| I `sigterm_shutdown_no_redispatch` | 6a | run 停在 barrier（门前）时 SIGTERM 全部 connector 副本：**grace 120s**；CID **停前缓存**（带 override）后**白名单 inspect**（exit0/非强杀）；门前阻塞 run **真实终态化**+零新页/零产物；**进程确认退出后**释放 barrier 并等 fixture 收敛再取**稳定基线**（runs 走 **PG run-ID 集合**）；重启后**全投影**与各计数不变；P21 遗留行不动；fixture 不重启 |
| J `deadline_bounds` | 6b | **与普通 source 超时判别**：专用 aux（仅 connector2 受控重建，--no-deps/联锁/**进程边界活任务守卫**：controlled+started_at≥aux2 boot，严格数字校验）设 `TASK_TTL=7s`，source HTTP 60s、Auth 300s 更长；TTL 尺度内终态（elapsed<50s）+ deadline 味失败原因 + 零发布 + 无僵尸拉取；**finally 强验证恢复**（rc0/健康/容器重建/生效配置无 7s） |
| K `sync_disconnect_compat` | 6c | 默认同步 POST（真实 body={}）+ barrier 扣页 1：客户端**断开连接**（≠取消 API）：终态须 **≤15s**（早于 30s/60s 预算——忽略断连只会 60s 源超时失败）；failed 原因须为 **context canceled/request canceled 族**精确匹配；零发布、释放后无后续页 |
| L `jwt_expiry_no_fallback` | 7 | 5s 短 JWT 在 fetch 等待中过期：须**已入发布门** + 失败详情含**完整精确片段** `dataset-versioning commit_output responded with status 401/403`（DependencyError 实际渲染）+ 目标**零 txn 零文件**——源超时/未尝试提交/泛含 token 均不得冒充 |
| M `restart_state_preserved` | 8 | **固定 cohort**（completed/cancelled/interrupted+P21 B/C）逐成员绑定 run/source/target：全投影、源 request_count、目标 file 集与 txn id→status 映射前后比较；completed 绑定 txn 仍 COMMITTED；artifact 两次下载**先 200 再 hash**；仅 connector 副本重启（fixture 不动）；45s≥2 心跳 |
| N `legacy_cancel_unsupported` | 取消约定 | 对 P21 遗留 run（归档 C 句柄）取消 → **409 unsupported**：不改态、不重跑、不增 runs；owner 身份由公开列运行期解析（归档不留句柄） |

## 证据分层（根要求：HTTP 报告必须区分层次，不宣称全靠 HTTP）

| 验收点 | HTTP（本工具） | 真实 PG / 单测（根定稿名单） |
| --- | --- | --- |
| cancel→BeginPublish 两种锁顺序 | G 仅证"门已入+真提交后 too_late"的可观察面 | **确定性 PG 回归**（真实 PID 锁等待）+ handler 单测（Commit 调用=0）补证 |
| 队列内部/队满监听尾部 | F 仅证显式拒绝+无孤儿 | 服务单测/PG 补证队满与 admission 监听失败尾部 |
| 停机收敛尾部预算 | I 仅证零新页/零发布/不重派 | 服务单测补证 worker/heartbeat/观察协程收敛与 poolClose 顺序 |
| 发布丢响应 | （可选扩展）代理长 hold 跨停机：Dataset COMMITTED、连接器未确认、不假 aborted、不自动重发——分层事实如实归档 | P21 C 已有触发器版同构证明 |
| 其余 D–M 可观察行为 | 本工具全量 | — |

窄窗口**不用随机 sleep 冒充**：D/E/I/J/K 用 barrier 确定性窗口；G 用代理 hold 确定性入门窗口。

## 契约核定状态（2026-11-14 更新：已钉定 vs 待核）

**根已钉定 + handler 已落盘并经只读复核（2026-11-14，骨架已按此对齐）**
- **公开控制字段（models.SyncRun，models.go:202-211）**：`control_mode`（legacy/controlled）、`cancel_requested_at`、`cancelled_by`（UUID）、`publish_entered_at`——后三者为空可省略。**这些是公开字段，禁止被泄漏走查当 FAIL**；私有仍仅 lease 物料（lease_holder/lease_token/lease_expires_at/heartbeat_at 及变体）。P21 的 owner_id/requested_by/attempt/generation 保持公开。
- **GET /syncs/{id}/runs/{run_id}**（handlers.go GetSyncRun）→ 200 **直接 SyncRun**；不可见 404 `{"error"}`；畸形 uuid 400。
- **async 触发**（RunSyncJob）→ `?async=true`（其他值 400）；202 **直接 SyncRun 无 wrapper**（`id` 键成立）；**队列满/停机竞态 → 503 `{"error"}` 且先于任何持久行**（F 的 queuefull 断言已收紧到 503+无孤儿）。
- **取消统一外层 `{error?, code?, run?}`**（cancelResponse）：取消后的 run 状态**必须读 `body.run`**——外层无 status 键，缺 run 对象一律 FAIL 不算通过（骨架 `cancel_body_run()` 强制）。pending 已 aborted / 既有终态 → **200 {run}**；running 已持久 → **202 {run}，run 仍 running**；门后 → **409 `code=cancel_too_late` {run}**；legacy → **409 `code=cancel_unsupported_legacy` {run}**；不可见 → 404。E/F/G/H/N 已分别按确切码断言。
- **触发体 drain 语义（主修复已生效，只读核实 handlers.go drainTriggerBody）**：sync 触发请求体 drain-to-EOF（empty/`{}`）→ 客户端 TCP 断连**即时取消** r.Context()（K 的 ≤15s 路径）；体 >1MiB → **413**；读超 5s → **408**；其它读错 → **400**——全部在 Reserve/Enqueue 之前**零持久行**。队列满/停机 admission 仍 **503** 零新行（F 的 QUEUEFULL_STATUS 不变）。剩余工作为测试 resp.Close 语义等收尾+race，非业务设计。
- **fixture 一次性 restart 语义（正式 release 时）**：运行中 fixture 进程为旧版（bind-mount 脚本），加载 barrier 扩展需**一次** `restart fixture`——内存计数清零：升级前只读计数**只作历史记录，不跨 upgrade 比较**；先写 expected-counter-reset，新 boot 初始快照才是 I/M 与三服务 restart 的 counter 基线；此后 fixture 不再重启。旧 DB/file/txn/Build/Job/artifact 为持久对象，升级前后仍逐同句柄比较。
- **run 状态词表（domain/sync/status.go）**：`pending/running/succeeded/failed/aborted`——无 queued/cancelled/error 值；骨架 RUNNING/TERMINAL/CANCEL_TERMINAL 已收紧（`aborted` 为唯一取消终态，E 运行期钉定复核）。
- **默认值与 env 名（已核配置候选，根承诺不改键名）**：`CONNECTOR_SYNC_ASYNC_QUEUE`（默认 64）、`CONNECTOR_SYNC_ASYNC_WORKERS`（1）、`CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS`（300）、`CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS`（300）。**QUEUE=总 admission 容量**（queued+running、sync+async 合计，非额外等待槽）——F 按总容量填满至显式 503 并验证零孤儿；并发 1 约束两种模式。**TASK_TTL 在 Reserve 准入时固定**（含 DB 与排队时间）。放行后验收 runner 可经 compose env 调小 QUEUE/TTL 加速 F/J，场景需读实际运行值。
- **N 父链（钉定，不再运行期发现）**：`sync_runs r → batch_sync_defs d ON d.id=r.sync_def_id → connections c ON c.id=d.source_id`（Source 表是 **connections**）。只查公开归属列（r.org_id/r.owner_id/r.requested_by/d.source_id/c.owner_id/c.org_id），**禁选 credentials/config 全文/token**；JWT 按现有 M.Identity 在内存生成，身份=**真实 Source owner**（从行读取 sub/org，绝不按 label 生成 UUID）。
- **P2.2 必测 PG 名单（根钉定 2026-11-14，12 个真实顶层）**：原 10 名（Cancel×PublishGate 系）在**第一候选原始日志已 10 RUN/10 PASS**（raw 原样保留为第一候选证据，不改写）；新增 2 名 **TestPGConcurrentMigrateSerialized / TestPGMigrateLockWaitCancellation**（migration_concurrency_integration_test.go；primary 已落整批同 Tx + PG16 事务级 advisory 锁，`MigrationsAdvisoryLockKey=0x4F46434F4E4E4D47`=5712327185261284679）——**钉定时尚未 PASS**，最终候选日志必须 RUN+PASS。**总必测 = 26+14+12 = 52**（两旧守卫冻结）。守卫按 distinct 真实名计数；自测 16/16 保持（shipped 用例已随名单动态 12 名验证）。
- **新锁二进制替换纪律**：并发启动/重启前**三 Connector 副本必须全部换为新锁构建**，严禁新旧混跑（旧 binary 无锁 replay 会互删表——attempt1 的 I 场景已实证，证据 attempt1-diagnostics/）。fixture 已完成唯一一次 restart（新 boot 计数=正式基线），**此后任何构建/up 不得再 restart fixture**。最终统一验收 = unit 61 实际计数 + PG52 + HTTP 完整 11（最终候选证据）；已绿首轮 61/50 不需当前重跑，等 root 最后放行。
- **publish_entered_at 语义**：非空时间戳即发布门已进入（无 phase 枚举，骨架 G 即此断言）；默认 sync 仍按 HTTP request 取消，async 不受 HTTP 结束影响。

**仍未定稿（最终事实，2026-11-14）**
1. 主实现已冻结（静态/standalone 测试全过）。**真实 TCP 已复现：HTTP/1.1 `Content-Length:0`（CL0）请求 ctx 取消传播 PASS、带 body `{}` 的取消传播 FAIL**——主 GLM 正在提最小修复；K 场景保持现有断言（真实 `body={}`、≤15s、精确 context-cancel 原因）**不放宽**，若联调时该修复未落则 K 按事实失败呈现。
2. 正式栈联调待根最终放行（勿 up）。

**日期口径**：当前任务与最终证据一律 2026-11-14；P21 历史归档日期不改。

## 注入面（仅隔离栈工具，非 prod API）

- **fixture barrier**（根授权）：`barrier_pages`（[页号] 或 "all"）、`barrier_wait_s`（1..600，默认 120，超时 504 可观察）、`barrier_release`（[页号] 或 "all"，作用于**当前代** gate）；**每次安装生成独立代（generation gate）**，请求入口即捕获所属代——重装不改变已到达请求的规则；原子校验；快照含 `inventory_started/finished/active/barrier_waiters` 等（waiters 计全部代）。默认关闭零行为变化（48 例自测含奇偶性与代数反例）。**不保存/回显任何请求头或 JWT**。
- **dataset-proxy**（工具侧新服务）：hold 只按 method+路径后缀+dataset_id 匹配；上游**先真处理**再扣响应；释放/到期后回放**原样**响应；max_hold 1..900；**同 id 在途重装 409 busy 拒绝**；admin/snapshot 仅整数计数。**held_now≠COMMITTED**（见修正记录 4）。**不携带 Authorization 材料不进任何日志**（29 例自测覆盖）。

## 守卫与证据规则

- 守卫四态同 P21 已验收口径；P2.2 守卫名单=根定稿 10 名（见上），**distinct 真实名计数**；名单文件缺失/空 → integration exit 1 拒绝放行；总必测 50。
- **单测包数口径（根 2026-11-14）**：新增 `cmd/main_test.go` 后 Connector unit 包数可能 **38、统一 61**；原 37/60 基线仅作历史记录。正式验收按**原始 ok 日志实际计数**，不硬断言 60 不变。
- 结果归档 `results/p2-2-<date>-<tag>/`（suffix 唯一不覆盖；OFFICIAL 需 D–N 各恰好一次全 PASS；partial/fail 如实分层）。
- 私有键递归走查 + P1 token scrub；泄漏证据只含 key+JSON 路径；JWT/lease token/cancel 列值不入 result/logs。

## 放行后执行序（备忘，本轮不执行）

1. 根定稿契约 → 覆盖 `smoke_p22.py` 顶部常量（含 QUEUE/DEADLINE/PHASE 词表）+ 写 `p22-required-names.txt`。
2. P1 栈 up + 60 包 + migrate-tests（40+N₂₂）+ P1 九场景 + P21 A/B/C 回归。
3. `sh p22_connector2.sh up`（联锁确认）→ `smoke_p22.py --contract-probe` → D–M。
4. SIGTERM/重启证据（I/M）独立归档；`p22_connector2.sh down` 收尾。

## 明确不做

- 不发明任何服务侧故障入口/测试 API；注入只允许 fixture barrier / dataset-proxy / P21 同款 PG 触发器（仅 synthetic 目标）。
- 不改 P1/P21 其余脚本与归档；不动业务代码与计划；不 clear/reset/commit/stash。
- 不复制 .env/JWT/raw lease token；PG 仅白名单投影，泄漏只报 key+路径。
