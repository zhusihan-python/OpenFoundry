# business-platform P2.3（Source 级持久准入）独立验收工具

> **状态：工具骨架 v2，2026-11-14（root 三批 review + P 收敛已全部落实）。**
> 已放行：本目录编码 + 本地语法/助手自测（raw/rc 归档 `results/preparation/`）。
> **未放行**：正式 build/up/stop/restart/HTTP 栈回归（`formal_run_p23.sh` 变更步
> 全部 `P23_CONFIRM=yes` 联锁）；q3v9 不试错；P1/P21/P22 源码/归档/共享 fixture
> 不改，只 import/调用。

## 契约表（root 2026-11-14 钉定，驱动内 fail-closed 常量）

| 面 | 钉定内容 |
| --- | --- |
| config | `config.sync.admission` 可选 typed object：`max_inflight_runs` 可省略/null（维度关闭），显式 **1..4096**，**0 非法**；`min_interval_ms` 省略/null/**0** 关闭，正整数 **≤86400000**。缺省/null/`{}` 无维度启用→旧行为 |
| 占用 | 同 Source **所有 Sync/actor** 的 `pending+running` 共额；仅真实合法终态（succeeded/failed/aborted）释放；running 仅 cancel_requested / lease 过期 / legacy 未知**仍占**；取消不退还 interval |
| 429 | code=`source_sync_throttled`，reason=`capacity`\|`min_interval`\|`contention`；body `error`+`retry_after_seconds`（正整数），头 `Retry-After` 同值；interval=锁后 DB 剩余 **ceil 秒**，capacity/contention 为 **1 秒建议值不保证**（只断言 ≥1）；typed 拒绝 **零 run/零 stamp 推进/零额外拉取/零发布**，本地 reservation 释放 |
| 私有字段 | `connections.sync_last_admitted_at`：成功插 run（含 policy-off/legacy）同 tx 末尾按 DB clock 推进；不改 updated_at/config；旧 run 不回填；**首准入前 NULL 合法**；绝不出现在任何公共 JSON（测试仅作 PG safe 列读取） |
| 锁 | pre-read policy 仅选锁：启用→源行 `FOR UPDATE NOWAIT`（**精确 55P03**→typed `contention`）；未启用→普通 `FOR UPDATE`（保旧 delete-first nil）；锁后权威重读 policy/计数/clock；其他 DB 异常不变 429；本地队满/Stop 503 语义不混 |
| PG 名单 | 新 mandatory 7 顶层（`p23-required-names.txt`，**钉定时如实记 0 已 PASS**）：ConcurrentCapacity / IndependentSourcesAndContention / IntervalPostLockClock / ReleaseStateMatrix / TenantBindingAndLegacy / MutationAndDelete / RollbackAndMigrationReplay。终局 **52+7=59**（同一 fresh verbose 日志，四 guard：runner 内置 26 + p21 14 + p22 12 + p23 7，各自 rc0）；旧三 guard 冻结 |

## 证据分层（root P 收敛，2026-11-14）

HTTP 场景只证外部可观察行为：pending 真取消释放、真实 running 请求→终态、
终态后手动重试、间隔持久、typed 拒绝零副作用、租户拒绝先于限流、config 校验。
**不在 q3v9 伪造**过期 lease / 遗留 controlled running / gate / fenced——完整
释放矩阵由已钉 7 项真实私有 PG 测试证明；`swap_guard` controlled==0 语义不动、
不加白名单、不改旧行。窗口只用 fixture barrier 或**真实交互式 DB 行锁**
（主动 ROLLBACK、精确 55P03、holder 真实 backend PID + idle-in-transaction），
无随机 sleep。

## 文件清单

| 文件 | 作用 |
| --- | --- |
| `smoke_p23.py` | HTTP 场景驱动 O..V + `--contract-probe`（importlib 复用 P1 `smoke.py`、P22 `smoke_p22.py`；冻结 J/K 被 U 直接调用并捕捉最终 aux image） |
| `run_tests_p23.sh` | P2.3 必测 PG 名单守卫（四态；distinct 名、顶层 RUN+PASS、SKIP>FAIL；名单缺失/空=integration 拒绝放行） |
| `p23-required-names.txt` | root 钉定 7 名（**当前尚未 PASS 如实记**）；`.example` 存档格式规则 |
| `selftest_guard_p23.sh` | 守卫合成日志自测（16 例；合成日志绝不算执行证据） |
| `admission_snapshots.py` | 值级快照采集+严格比较：handle 全集/每键/类型/条目数/绑定校验；null 仅 stamp 合法；缺 Source/缺列/无 token=失败；files 严格 list 封套；**txn 全集独立遍历**（{data,next_cursor,has_more} 严格+有界 cursor 跟随，无 file 的 OPEN txn 不漏）；快照只存 sanitize 过 ID/绑定/计数（绝不存 token）；CLI 传 Path 给 load_env |
| `selftest_snapshots.py` | 快照严格性负例自测（40+ 例：空对、缺 Source/缺列、null 陷阱、skip 目标、ghost txn、坏封套×n、cursor 遍历有界、token 剥离） |
| `archive_standalone_p23.py` | standalone 归档（**缺必需根→rc1 fail-closed** 并保留失败 index；本轮自有 index；值级脱敏） |
| `formal_run_p23.sh` | 编排器：swap 容忍 aux 不存在（P22 收尾已 down，记录 absent 安全态；在场者必须优雅停）；baseline/reread 只认**本次真实新产出目录**（stale glob 拒绝）；exits/compare 均入 per-attempt 目录/sidecar，无 append/`||true` 误读旧结果；pg59 增 `05-pg59-exact`（四 guard 名单**逐名精确 1RUN/1PASS/0SKIP** 共 59，raw RUN 行数只作 run_lines_total 含子测试不称顶层）；09-p23 拷贝**绑定本次 result_p23.json 的 suffix**（snapshots_latest/<suffix> 为唯一判定集，整 TMP 历史另存 snapshots_history 不混入；必需对=**o-reject** 且 handles 全集=1 源/3 Sync/3 target/2 run）；render 失败即拒用旧 override；`10c-post-p22-images` 在 10-p22-full（冻结 J 对 aux 仅 **reshape** env）之后复核终态镜像归属；finalize：standalone **必选**（真 rc+index 内容+无缺根）、快照配对**仅最新 official attempt**（历史坏快照不压全档）、秘密扫描、hash 最后 |
| `selftest_all_p23.sh` | 本地自测驱动（语法+助手，raw/rc/sha256 归档 preparation；无 docker/无 q3v9） |

## 场景矩阵（O..V）

| 场景 | 要点（review 修订后） |
| --- | --- |
| O `admission_capacity_shared` | **两个填满 run 同经 connector**（1 running 停泊 + 1 pending，不依赖副本 worker 数）；第二 actor 为同 Org **roles=[admin]** 合法身份（默认无 connections:write，工具不改权限逻辑）；双副本各发拒绝 + sync 模式共门；typed 429 零副作用对照**全部实际 target**（含 ex1/ex2 的 target+token）；**快照 before-o-reject→拒绝→after-o-reject 真跨操作**且 targets 全集（extra sync 的 target 亦入档），仅显式允许 `runs.<run1>.heartbeat/expires`；饱和同时另源在**空闲副本 connector2 真拉取完成**+全量 txn 集 COMMITTED 非空 |
| P `admission_release_matrix` | PA(cap2)：pending 真取消（200 aborted）释放→再准入；真实 running 请求持久 cancel_requested。PB(cap1)：饱和拒绝→running cancel 202 body.run 仍 running+请求持久（**不断言窄窗口仍 429**——worker 可立即终态；cancelRequested 占用矩阵归 PG 固定场景）→释放 barrier 真终态→配额 0→**手动重试 202 真完成** |
| Q `admission_min_interval` | 快成功立即 429，Retry-After 用 PG 锚**独立重算**（expected_now≤header≤+3s）；拒绝零四联；取消不退还；**run3 成功后先等 DB 间隔再 run4**；重启持久用**独立 120s 长 interval 源**+DB 剩余≥60s 前提（严格小于上限、大于重启余量，自洽）→重启后仍 429→到期手动成功；swap_guard 前置 |
| R `denial_precede_throttle` | 饱和态越权（同 sub 跨 Org/跨 Org admin/个人域/外租户 Sync/畸形 id）→403/404 绝不 429+快照不变+owner 活性；文案不声称具体父链内部（父链归 PG 证） |
| S `admission_config_validation` | 11 组非法 create→400 零落库；create roundtrip 逐字段；**非法 PATCH 后立即 PG admission 投影核对原值**（不用后续合法写掩盖）；**合法 PATCH 携带显式合法 admission** 再比 roundtrip+投影；active run 下 409；缺省与 `{}` 双差分放行 2 并发 |
| T `contention_real_db_lock` | **交互式持锁会话**（BEGIN+FOR UPDATE+pg_backend_pid，`bufsize=0`+自管字节缓冲拆行——默认 BufferedReader 预取会把 pid 行滞留 Python 缓冲致 select 假超时，配本地 fake 子进程自测 `selftest_lock_holder.py`：两行一次写+stdin 保持打开，acquire/release 必须终结）；psql 探针 `-X -v ON_ERROR_STOP=1 -v VERBOSITY=sqlstate`（PG16 默认 stderr **无** SQLSTATE，root 已 find-docs 核实）；持锁证明=**精确 rc1（`psql -c` 单查询分支，PG16.15 startup.c）+独立 `ERROR: 55P03` 错误行**+holder PID `idle in transaction`（任意 nonzero 不算）；触发 429 contention+零副作用；**finally 经 communicate(input=ROLLBACK) 主动释放**（超时 kill+reap）；释放证明=NOWAIT **rc0**→触发 202 真完成 |
| U `public_private_field_sweep` | **不再直调旧 J/K**（正式流 10-p22-full 仍跑完整十一项含 J/K）。U=真实成功 run（PG stamp 非 NULL 前提）后递归扫全部公开面：source echo、run detail、runs list、typed 429 body，及候选 source-detail/syncs-list 路由（拼写未钉：200 则扫、404/405 如实记录不算已检面）——**新 stamp 键与旧 lease 秘密键零出现** |
| V `concurrent_inflight_invariant` | 双副本 8 发并发：**1≤admitted≤cap 合法**（不强求恰 cap），全 429 typed；**串行填满至恰 cap**+一次 429；fresh Source 的 **PG run 集与 admitted 集严格相等**（typed 429 造孤儿 run 或 admission 丢失均红）且总数==cap、inflight==cap；采样器 max≤cap；stamp 前置 NULL 合法→设值即推进；**sampler finally stop+有界 join**、barrier finally release、cleanup 按各自父 Sync |

## 正式执行序（备忘，未放行）

`baseline(仅认本次新产出) → build-final(5 目标) → swap(aux 容忍 absent/在场
优雅停+镜像归属+新 StartedAt；fixture 绝不重启) → unit(实际包数,61 为基线不
硬造) → pg59(同一 fresh verbose 日志四 guard 各自 rc) → p1pre9 → aux-up →
contract → 09-p23(--allow-restart) → 10-p22-full(冻结十一项含 J/K) →
10c-post-p22-images(J 重建后终态镜像归属复核) → 10b-p21-ABC → restart3(仅三
服务) → reread(新产出+sidecar compare) → p1post9 → standalone(必选) →
aux-down → finalize`。内部 tee 一律 `bash -o pipefail`；失败/partial 原样保留；
finalize 只取最新 attempt 且拒 partial；hash 最后。

## 已知缺口（已上报 root）

1. capacity 与 interval **同时**超限的 reason 优先级未钉（断言 code+三值之一）。
2. 429 与本地 503 同时满足的先后未钉（单边各自覆盖）。
3. invalid config 400 的确切 message 未钉（断言 400+非空 error，message 入 evidence）。
4. 归一化 echo 的额外默认键未钉（provided 键严格断言，extras 记录）。
5. sync 模式 POST 超额=typed 429 按"共门"实现（O 内断言）。
6. **txn cursor 参数拼写为候选**（`?cursor=`）：快照遍历按 models.Page 严格
   封套实现并 fail-closed；若真实路由拼写不同，收集会以 collection failure
   呈红而非伪空（正式联调首跑即暴露）。
7. S 的存储核对用 PG admission 子对象投影（避免整 config 凭据面）；U 已改用
   root 实读 server.go 的六个精确路由（无候选/无降级）。
8. **txn 状态词汇已按当前 runtime 模型钉定**（OPEN/COMMITTED/ABORTED；FAILED
   不存在于模型已删）：表外值以 collection failure 呈红。
9. collect 级 DB 绑定核查（sync 行存在+source_id/output_dataset_id 匹配、
   run.sync_def_id 属于受管 Sync 集、sync target 在 target 全集内）在正式
   首跑即执行；本地无栈不可负例自测（比较级绑定/全集/强类型已有负例覆盖）。

## 明确不做

分页 QPS/worker 睡眠额度/新恢复队列/cron/自动 retry 或 takeover；不发明服务
侧测试入口；不在 q3v9 造过期 lease/遗留 running；不改冻结源码与归档；不复制
.env/JWT/lease 值；PG 仅白名单投影（stamp 列仅 PG 文本读取）。
