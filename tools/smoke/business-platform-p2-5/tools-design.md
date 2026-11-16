# P2.5 发布后推进断点 — Tools 轨道设计（tools-design.md）

- 状态：**DRAFT v2，未放行执行**（v1 经 root review-tools-round1 修正；修正已全部落入正文，v1→v2 变更见 §12）。foundation 文件（§8 表内标注"本轮"）已按 root 放行范围实现并纯自测，真实 cmd/rc 存 `results/2026-11-14-p25-foundation/`；guard `RELEASED_FOR_EXECUTION=False`，尚无任何 live query/Go/DB/Docker/HTTP。
- 作者：Tools 轨道 GLM（ZCode GUI，GLM-5.3/最高），2026-11-14。后续阶段（readonly baseline / 正式窗口）由 root 单独 release，不向用户重复要授权。
- 授权边界：不改写/不重跑任何 P24 收尾；不触碰 859 归档与 27 工具、q3 fixture（PID1/boot/计数）、PG 卷与 env600；所有 P25 产物仅在 `tools/smoke/business-platform-p2-5/`（**handoff 类文件一律在本目录，不写 repo 根**）。fixture/proxy/smoke/全量 driver 待 primary 修正版契约 + root 复审后再实现（避免接口返工）。
- 正式业务契约由 **primary 轨道 + root** 钉死；依赖契约细节处一律标 `OPEN-ALIGN-n`。**primary v1 的"last page request cursor"协议已被 root 否决**，替换为"独立固定数据查询参数 + 终端响应完成断点"（精确字段待 primary v2；§4.2 已改写，不实现旧协议、不做允许页重放却冒充 A→B→C 的 fixture）。

## 0. 本设计的完成标准（机械判定，供 root review 用）

工具编码阶段完成的判定（本轮不执行）：`formal_run_p25.sh full` 按 §6 锁序跑完 rc0；`finalize_p25.py` 产出 FINAL_OK=true、必需步骤全原始 rc0、秘密扫描 0/0、hash-manifest 最后生成且双向覆盖；六任务 roster 校验（§3）在每个 gate 点通过且旧 6 行 27 字段零漂移；P24 归档（859 文件）与 27 工具哈希重验不变。

本轮（foundation 轮）完成判定：§8 标"本轮"文件全部存在；`run_foundation_selftests_p25.sh` rc0 且其产物（真实 cmd/rc/stdout）在 `results/2026-11-14-p25-foundation/`（两项纯自测 41+30 全过）；真实派生的 `p25_cohort6.py` 通过 `--check`；`source-p24-baseline.txt` 与冻结 `01-source-hash-pre.txt` **逐字节相等**（combined `8cd25d42…`），当前树相对它的 delta 如实入档（§3.3：primary 已并行开工，changed=4/added=1/removed=0，严格门 06b 如实记 rc1）；guard/collector `RELEASED_FOR_EXECUTION=False` 且本轮零 Docker/DB/HTTP/Go 调用；git 分支/HEAD 不变、无 P24 目录改动。

## 1. 已核实输入（本轮实读，file:line）

### 1.1 冻结资产与六任务事实

- P24 完成：`~/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-14-p24/completion.md`；P25 handoff：`…/2026-11-14-p25/handoff.md`；实施计划 P2.5 段：`docs/architecture/business-platform-implementation-plan.md:366-381`（8 条待核对验收标准，本设计 §5 逐条映射）。
- P24 归档 `tools/smoke/business-platform-p2-4/results/2026-11-14-p24-final/`（859 文件 hash-manifest 全索引；本轮只读重验其中两个派生输入的 SHA256）：
  - `FINAL-NEW-TWO-PROVENANCE.json` = `78f450a34dd2f23a524b6ee80ea21334af27518d3686c07613a2e25513f93167`（在 859 manifest 内有索引；含 postboot B/C 两行**完整 27 字段** + C 的 txn `98e0892f…` COMMITTED / file `2d252182…` 200/22766B / lineage / sha `7e69fbf3…` 活证据）。
  - `tools/smoke/business-platform-p2-4/p24_pins.py` = `93557081e8ebfc9130ac3f012f271ae04f1a25df9c0a13dec61ed05426b9e215`（旧 4 pin × 27 字段，P24 归档 16-tools-sha 覆盖；P25 只 import 不复制不改）。
- **transition 锚点（root round1 修正）**：P24 **final** Connector 真实 boot `2026-11-14T07:11:20.883030213Z` + **完整** image `sha256:fe3208b3fcbd4d5fe428647e106d6ba6aa5b71fd94e9ba51144a9565258dc4c3`，机器可读于冻结 `15-final-base3-images.json`（859 hash-manifest 索引；同文件还携带 dataset/pipeline 全 ID、connector2/3/dataset-proxy `containers:0`＝旧注入故障 trigger/功能已移除的证据）。postboot2 两行 started_at `2026-11-14 07:25:xx` > 该 final 锚点；old4 四行（19:22/22:02 Nov13）< 该锚点。**不得**把 Nov13T22:05 的 preflight-era boot 当作 final（那是 v1 错误，已废弃）。
- old4 **历史**锚点 `2026-11-13T22:05:01.092139798Z`（冻结 `00-preflight/preflight-verdict-r1.json`）仅作为组一"出生于 P24 窗口前史"的独立 provenance 保留（组一 started_at < 它是不变历史事实），**不替代** final transition 锚点。
- owned-history 冻结绑定：`13-persist-post-r1/persist-manifest.json` 携带 `owned_history_sha256=8c18999a723fef3fe819612a33d821fc7731aad16c0634506a5bc0a622a18001`、owned_ids（17 defs）、terminal_states、`p24_result_sha256=3beb1bce…`；配合 11e-pre-abc-quiesce-union17 与 16 文件 persist 集（859 索引）。
- 源码冻结基线：P24 373 文件内容哈希 `8cd25d4220a6bdae843a693e1c1afa928991f537f5902a31e3e970704db6386b`（P24 completion §Accepted verification；P25 以此为 pre-implementation gate，见 §7.2）。
- q3 运行时：fixture PID1 `python3 -I /srv/rest_source.py 0.0.0.0 8090`、boot `2026-11-13T18:29:14.176785055Z`、host 端口 17090（compose.yaml:29-43）；PG `of-p24-pg`/`of-p24-pg-net` 保持 UP、零 host 端口、env600 `/private/tmp/openfoundry-p24-pg.env` 永不输出/归档。P22 先例：aux `connector3` 的 `DATASET_SERVICE_URL` 指向 `dataset-proxy`（host 17091）实现受控发布故障（`compose-p22-connector2.yaml:16-36`），P25 受控 unknown 代理沿用此**模式**但全部新文件新容器（§4.3）。

### 1.2 业务源码事实（只读核查，2026-11-14）

- 分页 cursor 仅为 run 内：`services/connector-management-service/internal/runtime/rest_snapshot.go:316-341`（每次 fetch `cursor := ""` 起步、`seenCursors` 防环、超 max_pages 拒绝截断）。`SnapshotResult`（rest_snapshot.go:103-109）只有 NDJSON/Rows/Bytes/ContentHash/Pages，**无跨 run 断点**。
- 发布 ACK 现状：`runtime/sync.go:113-184` CommitOutput 默认 `SNAPSHOT`（:125-128），2xx 即解码，仅校验 `Transaction.ID != uuid.Nil`（:180-183）——**不校验 status==COMMITTED、不校验 dataset 回显==请求目标**。P25 "可用 ACK" 强化点即在此（OPEN-ALIGN-4）。
- 发布→终态路径：`handlers/handlers.go:1902-1998`——P2.2 publish gate（cancel-first 零发布可证）→ `CommitOutput`（publishCtx 有界、尊重取消）→ ACK 非 nil → `completeLeased(succeeded, version=TransactionID)`；ACK 不可用/失败 → `completeFailedAfterCommitAttempt`（:2012-2023，明确"上游结果 UNKNOWN、不声称未提交、不自动重试"——P24 C 类 committed-unconfirmed 即此路径上游已 COMMIT 而终态写失败）。
- 终态 CAS：`repo/lease.go:330-383` `CompleteLeasedSyncRun`——`status='running'` + holder/token/generation 精确匹配 + `clock_timestamp() < lease_expires_at` + control_mode/gate 守卫（succeeded 需 `publish_entered_at IS NOT NULL AND cancel_requested_at IS NULL`）。断点推进与终态同 tx 的落点（OPEN-ALIGN-7）。
- REST 同步分页配置模型：`internal/models/rest_api_sync.go:11-13,58-65`（`pagination.type=page|cursor|none`、`cursor_param` 默认 `cursor`、`next_cursor_path`）。P25 增量协议的字段命名待 primary（OPEN-ALIGN-2）。
- `sync_runs` 现无任何数据断点列（迁移仅有 lease/cancel-gate/tenant/dataset_version/content_hash 等；`batch_sync_defs` 只有 P24 调度游标列）。断点存储形态全 OPEN（OPEN-ALIGN-1）。

## 2. 轨道边界与文件所有权（沿 P24 惯例）

- Tools（本轨道）：`formal_run_p25.sh`、finalize、guard/cohort 派生与自测、readonly baseline wrapper、source-hash、exact-checker、fixture/proxy/aux compose 新文件、images 检查、adapter、归档。
- Primary：`smoke_p25.py`（HTTP 场景主体）契约内容以 primary 的 `implementation-contract.md` 等价物为准；PG 轨道：`run_pg_isolated_p25.sh` + postfacts/persist 采集验证 + PG 测试名。P25 目录内文件名先按 §8 预分配，root 可调整；**严禁编辑 P24 及更早目录任何文件**（只读 import）。

## 3. 六任务保全设计（本轮特殊重点；root round1 重构）

### 3.0 与 v1 的关键差异（root 裁决）

- 锚点更正：transition 锚点＝P24 **final** Connector boot `2026-11-14T07:11:20.883030213Z` + 完整 image `sha256:fe3208b3fcbd4d5fe428647e106d6ba6aa5b71fd94e9ba51144a9565258dc4c3`（§1.1）；Nov13T22:05 仅为组一历史 provenance。
- **首过渡（first-transition）与稳态（steady）是两个显式策略**：今天对 postboot2 做"全员 < 当前 boot"必然失败（它们 07:25 > 当前 boot 07:11）——这正是首过渡必须按"封存 final 锚点 + 分组两侧验证"裁决的原因。合法的、经 root 复审的 P25 候选部署/重启**不受禁止**（v1"old2 永不跨重启"措辞废除）；"old2 未跨 P24-final 重启"是历史事实，永不变。
- schema 强制升级：expected 与 live 两侧都做**恰好 27 键**集合校验（缺键/多键/类型错/时间不可析全部 BLOCK），不再只比较"出现过的键"。`cohort_gate_p24.py` 是未执行 DRAFT、无 27 键 schema 强制、postboot2 直接跳过 preboot 检查——**不作为 P25 release guard 复用**（仅作反面教材对照）。

### 3.1 Roster v6 派生（p25_cohort_derive.py → 生成 p25_cohort6.py）

派生而非转录；每一步失败即中止非零退出：

1. **归档封印**：加载 859 `hash-manifest.json`；下述每个输入文件必须 (a) 在 manifest 有索引 (b) 实测 sha256 == manifest 记录值。
2. **old4 链**：`p24_pins.py` 实测 sha == 归档 `16-tools-sha-final.json` 记录值（toolSHA 链）；import 后恰 4 行、每行**恰好 27 键**（键集 == 冻结 SCHEMA_27）。
3. **postboot2 链**：`FINAL-NEW-TWO-PROVENANCE.json`（manifest 索引+SHA）`rows` 恰 2 行、每行恰好 27 键；id 与组一互异。
4. **首过渡锚点**：`15-final-base3-images.json`（manifest 索引+SHA）→ connector `started_at`（严格 RFC3339）与**完整** image hex；同时记录 dataset/pipeline 全 ID 与 aux 三服务 `containers:0`（旧故障 trigger 已移除的冻结证据）。
5. **组一历史锚点**：`00-preflight/preflight-verdict-r1.json`（manifest 索引+SHA）中的 `2026-11-13T22:05:01.092139798Z` 值复验。
6. **派生时交叉断言**：组一全部 started < final 锚点 ∧ < 历史锚点；组二全部 final 锚点 < started；B `dcca308f…` `gate_entered=false`、C `885d1429…` `gate_entered=true`；6 id 互异。
7. **生成物**：`p25_cohort6.py`（GROUP1/GROUP2/FINAL_ANCHOR/FINAL_IMAGE/HISTORICAL_ANCHOR_OLD4/AUX_ABSENT/PROVENANCE_SHAS 字面量）；原子写（tmp+rename），写后 `ast.parse` + 重新 import + 字面量全等复验；`--check` 模式对既有生成物做同样复验；被篡改/过期的生成物 → 非零。

### 3.2 `p25_cohort_guard.py`（DRAFT，RELEASED_FOR_EXECUTION=False）

继承 P24 guard 运行纪律（ROW_SQL 27 字段投影、时区/DateStyle 钉死、compose ps 恰一容器、inspect 严格 RFC3339、holder/token 仅布尔、零写库、fail-closed；ROW_SQL 与冻结 P24 常量做文本一致性自测防转录漂移）。live 采集函数已备但被 release 旗标挡住（未 release 时 rc3 拒跑，无任何 Docker/DB 调用）。

**策略 A `adjudicate_first_transition`（root 复审具体 first-transition record 后、正常候选部署前的唯一合法 guard 语义）**：
- live Connector boot/image **精确等于** roster 封存 final 锚点（完整 64 hex image 比较，拒绝短 ID）；不等 = 当前栈已非 P24-final 状态 = BLOCK（此时只能走 root 重新裁决，不允许"就近比较"）。
- controlled pending/running **整集**恰 6 行：未知/缺/重/NULL-started/字段漂移/**缺键或多键**/类型错 → BLOCK。
- 组一：started < final 锚点 ∧ < 历史锚点；组二：final 锚点 < started；组错位 BLOCK。
- 配套 first-transition record（readonly preflight 采集、root 审后才部署）：postboot2 原**来源**独立复核（provenance json 之外，直接对照 859 内 ABC/postfacts 原始工件与 live 行——两个独立来源一致才认）；同 cohort 的 fixture 计数（仅同名受控 src，b/c 两个 src）两次读取稳定且 active0；C 保留证据新鲜复验（txn COMMITTED / file GET 200 / 22766 bytes / hash / lineage / target）；旧注入故障 trigger/功能 absent（aux 三服务 containers==0，live 复验）。
- 不以任何 standalone restart 把 postboot2 "变 old"。

**策略 B `adjudicate_steady`（合法 P25 候选切换之后的常规 guard）**：
- 期望目标 boot/image 为**参数**（由该次部署的 reviewed record 提供，不硬编码）；全员 6 行 started < 目标 boot；分组 provenance 保留；同样整集/27 键/漂移强校验；不自动收编未来行。
- 首过渡发生一次并留档后，后续一律 steady；两策略产物均写入证据。

### 3.3 P24 owned-history 与产物的新鲜保全（root round1 新增强制）

不只保 controlled6：P25 preflight 必须新鲜验证 P24 owned-history **精确 17 defs（全 disabled、游标 NULL）/ 32 ticks / 15 terminal runs / 0 grants**、`owned_history_sha256` 仍 == `8c18999a723fef3fe819612a33d821fc7731aad16c0634506a5bc0a622a18001`（对 live 重算 history SHA，与冻结 `13-persist-post-r1/persist-manifest.json` 绑定一致）、`p24_result_sha256=3beb1bce…` 绑定、旧 artifact/BuildJob reread 照旧。初始 source-only 基线＝本目录 sibling `source-p24-baseline.txt`（**冻结 `01-source-hash-pre.txt` 的逐字节副本**，combined == `8cd25d42…`）+ `p24-frozen-baseline.json`（全部冻结引用的机器可读 SHA 链，derive 全链通过才产出）。

**2026-11-14 foundation 轮实测发现（如实上报）**：primary 轨道已并行开始 P25 实现——当前树相对冻结基线 changed=4（`models/models.go`、`models/rest_api_source.go`、`models/rest_api_sync.go`、`runtime/rest_snapshot.go`）+ added=1（`repo/migrations/20261116100000_sync_incremental_checkpoints.sql`）、removed=0。因此"实现前 373 精确相等"门在今日已不可能为绿；runner 以 `06-current-tree-delta`（removed≠0 才 rc1）如实记账当前 delta，`06b-preimpl-gate-recheck` 保留严格 verify 语义并**如实记录非零 rc**（信息性，不计为工具失败），最终源指纹以"冻结基线 + 显式 delta 清单"双记录收尾（§7.2）。

### 3.4 tail/ABC 后的 roster 处置（修正）

- 一旦出现**有意**的 P25 unknown-tail 行，全局 strict6 guard 不再适用（硬跑必失败属预期）——改用**事前声明的 scoped transition roster** + **显式扩展 tail verifier**：不在每个 tail case 后盲跑 six guard，不自动把 unknown 收编进 guard。
- 终量精确 8+k ＝ old6 + 本轮 ABC2 + 有意 P25 残留 k；如实记数。
- **两个独立 roster**：全局非终态 run roster（pending/running 行）与 **durable checkpoint owner roster**（owner 可指向已 terminal 的 failed run——P25 语义允许 failed run 仍持有 owner）。清理/校验分别记账，互不混同。
- 未解决 owner 不是 cleanup 要抹掉的错误：worker 终态 vs 未解决能力/状态分开（quiesce 工具的正向断言＝owner 仍在且该 Def 新触发被拒；该断言所需的 POST 探测本身是写操作，必须 before-ABC 或以 readonly 已存证明替代）。

## 4. 新 P25 独立 fixture 与受控故障面

### 4.1 独立性红线

- 旧 fixture（`business-platform-p1/fixtures/rest_source.py`、q3 `fixture` 服务、17090）**只读**：不修改脚本、不重启容器、不重置计数。旧 fixture 的 cursor 只是 encoded page/time 静态值（P25 handoff 已核），**不得**通过改它冒充增量协议。
- P25 fixture 全部新文件：`tools/smoke/business-platform-p2-5/fixtures/rest_source_p25.py` + 新 compose override（只新增服务，不动既有服务定义）。宿主端口提案 17092（fixture）/17093（fault proxy）/17094（aux connector），避开 17078-17091（OPEN-ALIGN-13）。
- 加入方式沿 P22 aux 先例：`docker compose -f compose.yaml -f compose-p25-*.yaml up -d <新服务>` 只创建新容器，不触碰 base5 与旧 fixture；窗口结束 `rm -sf` 新服务（aux-down 等价阶段），全程旧 fixture boot/计数除旧回归套件固有请求外零增量（§6 各 gate 点快照对照，如实记录）。

### 4.2 fixture 上游协议（字段名 OPEN-ALIGN-2，行为先钉）

- 有状态批次机：预装 A/B/C 三个批次（行内容确定性、行数跨批次不同、含边界行；空批次档位；行 bytes/sha 可独立计算）。**协议按 root 否决后的方向**：增量位置由**独立固定数据查询参数**承载（与 run 内分页 cursor 参数分离、不混用），上游在**终端响应**返回完成断点（opaque；精确参数/响应字段待 primary v2，OPEN-ALIGN-2）。**禁止**实现"以最后承载页请求 cursor 作断点"的旧协议，**禁止**允许页重放还能冒充成功 A→B→C 的 fixture 行为——批次推进必须以新数据到位为前提，可证"无跳漏且旧数据不再重复整批"。run 内分页照旧独立（cursor_param 机制，页大小可配，制造跨页批次边界与空档）。
- 计数与日志端点：per-src 请求计数（含断点 echo 值记录）、最近 N 次请求的断点参数快照——供"输入断点↔已发布版本↔提交后断点"关联核对与"pre-publish 零 dispatch"证明。
- 受控故障旋钮：mid-pages 500、批间挂起（slow）、返回非法候选响应（未知断点→显式拒绝档 vs 2xx 空结果档，用于区分"上游拒绝"与"空批次"两种语义，OPEN-ALIGN-3）。
- healthz + 只读状态；全部 PID1 `python3 -I` 直跑，无 entrypoint 包装（同旧 fixture 惯例）。

### 4.3 发布侧受控故障（unknown 三态）

- 新 `dataset_fault_proxy_p25.py`（17093）+ 新 aux connector `p25connector`（17094，`DATASET_SERVICE_URL` 指向 proxy，模式同 `compose-p22-connector2.yaml` connector3→dataset-proxy）。proxy 档位：透传（默认）/转发后吞响应（no-ACK 超时）/200-无 txn/txn 状态非 COMMITTED/dataset 回显错靶/5xx。前两档制造"已 dispatch 结果未知"，后三档驱动**强化 ACK 校验**的可证拒绝（OPEN-ALIGN-4）。
- PG 侧中途故障（终态+断点同 tx 回滚证明）用独立隔离 PG 的受控注入（terminate backend / 唯一约束冲突注入），不重启 q3 PG，不碰 `of-p24-pg`。

## 5. P25 HTTP 验收场景矩阵（最小可 review 全量）

编号 h1-h14；每行=一个场景，映射实施计划 8 条标准（C1-C8）。触发路径三态覆盖：sync=POST `/syncs/{id}/run`（ROUTE 沿 P24 ROUTES 表，新增面 OPEN-ALIGN-10）、async=既有派发路径、scheduled=arm+cron。所有跨轮场景 ≥2 轮真实 HTTP；全值断言（行级全值非计数）、hash/bytes、txn COMMITTED、exact target（txn.dataset==目标 ∧ metadata.lineage.sync_run_id==run）、下载可验。

| # | 场景 | 路径 | 关键断言（证据） | 映射 |
|---|---|---|---|---|
| h1 | 基线跨 run 增量 A→B→C | sync×3 | run1 输入初值→发布 batch1（全值/bytes/sha、txn COMMITTED、lineage、exact target）→断点=P1（同 tx）；run2 输入 P1→batch2→P2；run3→batch3→P3；fixture 断点 echo 链 == 提交链 | C1,C2 |
| h2 | 每批独立 SNAPSHOT 不累计 | sync | batch1/batch2 txn **全程 COMMITTED**（成功批从无 OPEN 态）；**分别按具体 fileID 下载新旧两批**复验 bytes/hash/txn/lineage；latest 判定用**实际 branch 有效视图/preview 端点**返回内容（仅含最新批），lineage/txn/时间轴只作辅助不单独作证；`/files` 可列历史——**禁 files_total==1 断言** | C1 |
| h3 | 空批次 | sync | 空批返回+完成断点推进与否按契约（OPEN-ALIGN-3）；无论何者，不发布非空文件、不倒退 | C1 |
| h4 | 非法输入候选 | sync | 篡改/伪造/他源/含控制字符断点→**真实 failed pre-publish 终态 + 占用原子释放 + 位置不变 + 零发布**（准入/拉取后才发现的非法候选**允许**已建 Run/临时 owner——不得声称"从未占用"）；错误码钉死；候选值不得泄漏进错误/日志/metadata | C1 |
| h5 | 旧 snapshot 兼容回归 | sync | 未配增量的 def 行为==P24（整快照 SNAPSHOT），既有 7 场景语义不回退 | C1 |
| h6 | 同 Def 并发拒绝 | sync+async 并发 | active_run admission 占位：第二触发干净拒绝（码 OPEN-ALIGN-6），fixture/proxy 计数证明**零第二次发布**；不同 Def 并发两路独立成功 | C4 |
| h7 | 配置/目标变化绑定 | sync | 目标 dataset/输出配置变化后断点绑定行为按契约（拒绝或显式重置，OPEN-ALIGN-1/5）；租户/父链跨三路径一致；无凭据/租约秘密泄漏（响应与错误脱敏复验） | C5 |
| h8 | 拉取/分页失败 | sync | mid-pages 500→run 失败可证、断点不推进、**占用释放无残留**（pre-dispatch 已知失败≠unknown；已有 checkpoint 行则 resume 不变）；下一 run 同断点重拉完整批次；max_pages 拒截断复验 | C6 |
| h9 | pre-publish 取消/失败 | sync | cancel-first 证明用**行状态（未入 gate）+ proxy 零转发**；部分页已完成的 upstream 请求不等于已 dispatch——**fixture 完成计数 0 不是通用取消断言**；admission 释放、位置不动、无占位 | C6 |
| h10 | 租约到期/重启 | sync | 到期→fenced 不推进；目标容器受控 restart（tail 之前）→ 非终态行不收养、断点不动、roster 复核 | C6 |
| h11 | 强化 ACK 拒绝 | sync×fault proxy | 无 txn/非 COMMITTED/错靶回显→按不可用 ACK 处理：不 succeeded、不推进、不误报已提交；dispatch 已发生→failed+**占用保持**（conservative；归 h14 同类占位 roster）；proxy 未转发的档位可另证真实上游零到达（proxy 转发日志），但**不得**据此声称"远端无新版本"作为 connector 行为断言 | C2,C6 |
| h12 | 同 tx 推进+终态（隔离 PG） | 隔离 PG | ACK 成功后注入终态写前 PG 故障（SQL 触发器等）→**同 tx 回滚**：位置未推进∧run 未 succeeded；真实 rc/日志绑定。**定位＝回滚证明**，不得冒充"丢失 COMMIT ACK"（那需 h14 转发后吞响应） | C3 |
| h13 | scheduled 增量两轮+ | scheduled | arm→≥2 tick 各完成一批（真实 cron 循环）；tick/run/断点/发布四链关联；disable 后停止 | C2,C5 |
| h15 | 占用期调度/手动触发 | scheduled+sync | 占用中 due tick：如实记 skipped 类 tick outcome + **cron due 游标按 P24 策略推进**，不建 Run、不打成功 admission stamp、**数据断点不动**；手动 busy：不建 Run/stamp；两游标语义分别独立断言（outcome 名待 primary v2） | C4,C6 |
| h14 | unknown 三态（**controlled tail，与 h11 同段最后**） | fault proxy/PG | ①no-ACK：proxy **先转发、亲验上游真实 COMMITTED、再吞下游响应**→可同时证明"远端 txn/file/hash 已提交（直连 GET 复验 bytes）"与"本地位置未动 + owner 阻断再触发"——**不做假'无新版本'声明**；②lease 失效；③本地提交未知（全旧/全新原子）。全部：**checkpoint owner 保留**（不清理不 replay，P26 解决）；每态占位/宿主行入 §3.4 声明 roster；owner-roster 与 run-roster 分开记账（owner 可挂 terminal failed run） | C6 |

诚实规则：失败 attempt 全留（-rN，不覆盖）；cleanup 失败 gate 判定不吞；场景间 guard6 复跑；任何"expected SKIP/未验"单列（沿 P24 五项 optional 惯例）。

## 6. 运行顺序（锁序提案，`formal_run_p25.sh full` 编码依据）

```
preflight(readonly whitelist: first-transition record 采集 + owned-history17/32/15/0 新鲜复验
          + 同 cohort fixture 计数×2 稳定 active0 + C 保留证据复验 + aux absent 复验
          + final 锚点 boot/image 活体等于封存值[策略A前置条件])
→ baseline/fingerprints(source-p24-baseline 373==8cd25d42… pre-impl 门 + p24-frozen-baseline SHA 链)
→ unit(+race) → pg-isolated(新 of-p25-pg，old72+newN 逐名) → build(5 images)
→ swap[合法候选切换，root 复审 first-transition record 之后；切换后 guard 转 steady 策略]
→ p25-fixture-up(仅新服务) → p25-window: h1–h10,h13,h15 绿场景（每场景后 owned 静默清理；
   strict6 steady 巡检；h12 在隔离 PG；blocked-admission 等"POST 即写"的正向探测全在本段或更早)
→ 旧回归: p1pre9 → p23-adapted(8) → p22full(11+post-images) → P24full7(需 exact-count owned guard
   适配后重跑，适配显式标注、冻结文件不动) → 旧 artifact/BuildJob reread → P1post9
→ restart/recreate 回归(h10，严格在 tail 前) → pre-tail-gates(steady strict6 + scoped transition
   roster 声明冻结) → controlled tail: h11+h14（有意 P25 残留入声明 roster；此后不再盲跑 six guard）
→ p25-aux-down(新服务 rm -sf，原始 rc 入档) → finalbase3 实际 image/boot 校验
→ p21abc（沿 P24：ABC=最后 q3 写；新增 2 行 ABC-轮 unknown）
→ 只读尾[严格只读，无任何 POST/写探测]: postfacts(扩展 tail verifier 精确 8+k + owner-roster 与
   run-roster 分账 + 占位仍在/仍拒[readonly 已存证明] + 绑定) → persist → source-freeze
→ standalone(脱敏) → finalize(manifest→secret-scan→hash-manifest LAST, FINAL_OK)
```

- 所有 q3 restart/recreate/aux up-down、旧 artifact/BuildJob reread、P1post9 全部在 ABC 之前；ABC 之后严格只读——包括"admission 被拒"类正向断言所需的 POST 探测（本身是写）必须 before-ABC 完成或以只读已存证明替代。
- tail 产生有意残留后：不盲跑 strict6（必失败属预期），改 predeclared scoped transition roster + 扩展 tail verifier；终量精确 old6+ABC2+k=8+k。
- 计数断言只对**同名受控 src cohort**（b/c 两 src 的历史计数不变、active0）；旧回归套件固有使用新 src 名，不搞跨套件聚合计数相等。
- 每相 `P25_CONFIRM=yes` 门控 + step-log marker（RESULT_FILE/PERSIST_DIR/ROSTER_DECL/ATTEMPT_DIR 等沿 P24）；driver 用 bash（P24 教训：不用 sh）。
- 全程旧 fixture 计数在 gate 点快照：回归窗口内增量如实记录（P24 惯例），窗口外（preflight vs postfacts）必须零增量。

## 7. 证据与归档规约（P24 质量等价，全部 P25 owned）

1. `results/2026-11-14-p25-final/`：每步原始真实 cmd+rc+stdout/stderr、失败 attempt 保留、`-rN` 防撞、EXECUTIONS 诚实表（旧72+new 每名恰 1 RUN/1 PASS、required 0 SKIP/FAIL、optional 单列）、upstream/downstream/镜像/gate 状态。
2. source-gate 两时刻：实施前 373 集哈希==`8cd25d42…`；实施后全集新哈希 + 相对 P24 baseline 的**显式 delta 清单**（改/增文件枚举，删=异常 BLOCK）。Root deps 不变断言。
3. PG：新隔离实例（提案 `of-p25-pg`/`of-p25-pg-net`，零 host 端口，独立 env600；OPEN-ALIGN-12）；迁移空库/旧库/重跑三态；`of-p24-pg` 卷保持 UP 不碰。
4. 归档收尾顺序锁死：secret-scan(0/0) → manifest → tools-sha（P25 工具 + 只读 import 的冻结 P24 文件单列）→ hash-manifest 最后（双向覆盖、含自身外全部文件）；standalone 脱敏副本（DSN redaction 重放对照，沿 P24 218 条惯例）。
5. 六任务与占位证据：roster 声明、guard/verify 全跑产物、占位"仍在/仍拒"（readonly 证明）、旧 6 行 27 字段 pre==post 零漂移、owned-history 17/32/15/0 与 `8c18999a…` pre==post、同 cohort fixture 计数稳定 active0、owner-roster/run-roster 分账 —— 全部入 manifest 必需步。
6. 采集纪律（root round1）：任何 live 采集**开始前**，其 raw/derived/err/original-rc/SHA 消费链与**精确 catalog 期望**（期望哪些表/文件/命令产物）必须先设计并随工具落档；catalog 缺项＝验证失败，**禁止缺失表被静默映射为空**（空结果必须与"查询失败/表缺"可区分并显式判定）。

## 8. 工具清单（foundation 本轮已实现；其余待 primary v2 + root 复审后放行）

| 文件 | 职责 | 纯自测点 | 状态 |
|---|---|---|---|
| `p25_cohort_derive.py` | §3.1 roster 派生+全 SHA 链+锚点/分组/语义断言+原子 ast 生成+`--check`+`--emit-frozen-baseline` | 坏 SHA/坏锚点/缺行/键集错全拒（合成树） | **本轮** |
| `p25_cohort6.py` | 派生产物（生成，不手编） | ast+字面量复验+篡改检测 | **本轮（真实派生）** |
| `p24-frozen-baseline.json` | 冻结引用机器可读基线（SHA 链/锚点/owned-history 期望/373 指纹） | derive 全链通过才产出 | **本轮（真实派生）** |
| `source-p24-baseline.txt` | 当前树 373 文件基线（实现前验证 == 冻结） | 与 01-source-hash-pre 逐行/combined 相等 | **本轮（真实生成）** |
| `p25_cohort_guard.py` | §3.2 双策略 guard（DRAFT，RELEASED_FOR_EXECUTION=False；策略函数纯化） | 组错位/缺键/多键/漂移/未知/类型/时间全拒 | **本轮** |
| `selftest_p25_cohort.py` | derive/guard 严格纯自测（合成树+合成行） | — | **本轮** |
| `source_hash_p25.py` | emit/verify/delta 三模式源指纹 | 合成树语义用例 | **本轮** |
| `readonly_preflight_p25.py` | §6 preflight 采集器（白名单+精确 catalog+`--plan`；DRAFT 旗标，未 release 不执行任何 live 命令） | plan/catalog 纯函数 | **本轮** |
| `verify_preflight_p25.py` | preflight 判定器（catalog 完整性/owned-history/计数稳定/first-transition 输入纯函数） | 合成 artifact 用例 | **本轮** |
| `selftest_p25_source_preflight.py` | source_hash/preflight 纯自测 | — | **本轮** |
| `run_foundation_selftests_p25.sh` | 本轮全部纯自测+真实派生的 honest runner（真实 cmd/rc/stdout 入 `results/2026-11-14-p25-foundation/`） | — | **本轮（已跑）** |
| `fixtures/rest_source_p25.py` 等 fixture/proxy/compose | §4 新运行面 | — | 待放行（协议已变，防返工） |
| `smoke_p25.py`（Primary 主责） | h1–h15 场景 | — | 待放行 |
| `p25_tail_roster.py` / `check_owned_quiesced_p25.py` | §3.4 声明/分账/静默 | — | 待放行 |
| `collect_postfacts_p25.py` / `verify_postfacts_p25.py` / `collect_persist_state_p25.py` | 只读尾采集验证（含扩展 tail verifier） | — | 待放行 |
| `run_pg_isolated_p25.sh` + `p25-required-names.txt` + `pg_exact_check_p25.py` | 隔离 PG old72+newN | — | 待放行（PG 轨道对齐） |
| `check_running_images_p25.py` / `p23_q_guard_adapter_p25.py`（+P24full7 适配） | 镜像/旧套件内嵌 guard 重定向 | — | 待放行 |
| `formal_run_p25.sh` / `finalize_p25.py` / `standalone_p25.py` | §6 锁序 driver / 收尾 / 脱敏 | — | 待放行 |

## 9. OPEN-ALIGN 清单（primary+root 钉死后编码；编号供引用）

**2026-11-14 更新（round1 后）**：Primary v1 的 last-page-request-cursor 协议**已否决**（替换方向见 OPEN-ALIGN-2）；primary v2 待出。下表"primary v1"列仅作历史覆盖参考，**已否决项明确标注**；root 钉死 v2 后本表收敛为引用。

| # | 待钉项 | 影响面 | primary v1 覆盖 |
|---|---|---|---|
| 1 | 断点存储：表/列/形态/初值/per-def 粒度/配置变化重置策略 | guard 观测点、persist、h7 | §4.1-4.3（方向未被否决，v2 细节待钉） |
| 2 | **协议字段（重写）**：独立固定数据查询参数名 + 终端响应完成断点路径 + 候选校验规则（非法/过期/他源/控制字符）；query key 冲突规则；captured input 跨页固定；page2/解析/limit 失败不消耗位置 | fixture、smoke、h4 | v1 §2.2 **已否决**；等 v2 |
| 3 | 上游完成语义：空批次含义与推进、批内失败后位置不变性、上游一致性前提（不声称通用 CDC） | h3/h8、fixture | 部分（at-least-once 措辞随协议重审） |
| 4 | 可用 ACK 契约：txn id+COMMITTED+dataset 精确回显；生产 adapter 与**可注入 port/handler 双侧**契约显式化 | h11、proxy、sync.go | §4.6.4（root 已定方向） |
| 5 | 批次发布语义：独立 SNAPSHOT、branch 有效视图/preview 实际端点、新旧 fileID 分验 | h2 | §3（root round1 细化：COMMITTED 全程、禁 files_total1） |
| 6 | perDef active_run admission：拒绝语义（码）、pending 即占、与 P2.3/P24 交互、异 Def 独立 | h6、h15、quiesce | §4.4（方向保留；码待钉） |
| 7 | 推进事务：同 tx 落点、ACK 后 CAS/提交失败口径（全旧/全新原子） | h12/h14、postfacts | §4.6.5（方向保留） |
| 8 | unknown owner 观测面：工具如何读"owner 仍在/仍拒"；owner 可挂 terminal failed run 的分账口径；P26 钩子不提前实现 | §3.4、quiesce、tail roster | 部分；观测端点待钉 |
| 9 | pre-publish 失败/取消证明口径：行状态 + proxy 零转发（fixture 计数非通用） | h9 | root round1 已定口径 |
| 10 | 新增/变更 HTTP 路由与请求/响应字段全表（含 `checkpoint_not_advanced` 外显、skipped-busy tick outcome 名） | smoke ROUTES pin | 待 v2 |
| 11 | 首过渡 record 的 root 复审点与合法切换后 steady 目标 boot/image 参数来源 | §3.2/§6 | root round1 已定框架 |
| 12 | 隔离 PG 命名/网络（提案 of-p25-pg）与 of-p24-pg 保留 | run_pg_isolated | PG 轨道对接 |
| 13 | 新服务端口/egress allowlist 扩展（提案 17092-17094） | compose | tools 决 |
| 14 | P21ABC 入本轮（默认入；终量 8+k）与 k 上限 | §3.4、锁序 | root 决 |
| 15 | `p25-required-names.txt` 冻结 | pg exact | PG 轨道产出后 |
| 16 | **sticky dispatch marker/boundary**：`publish_entered_at` 仅保守 gate 状态、非"请求已达服务器"证明；选一个安全的 sticky dispatch 标记并写入契约（缺 ACK 不得推断远端未发布） | h9/h11/h14、postfacts | 待 v2+root |
| 17 | **Source input/config epoch 与 credential identity epoch**（或等强不可变语义绑定）：`Source UpdatedAt`（仅 name/status）与 `schedule_revision` 均非数据绑定；SetCredential 独立 UPSERT 须参与锁/保护/版本；同值语义与保守失效策略 | h7、PG 轨道 | 待 v2 |
| 18 | scheduled checkpoint-busy 的 tick outcome 命名与"两游标独立断言"口径 | h15 | 待 v2 |

## 10. 风险与确信度

- 高：§1 全部输入为冻结/源码实读且 foundation 派生链每步 SHA 复验；锚点/27 字段/owned-history 绑定机器可复核；端口/aux 模式有 P22 先例。
- 中：h12 同 tx 证明的注入手段（SQL 触发器/terminate backend 时序窗）需 PG 轨道细化；`/files` 历史列出行为以 h2 实测为准（设计已禁 count 伪验）；first-transition record 的"postboot2 原来源独立复核"具体对照工件集合在 readonly baseline 采集时由 root 过目。
- 低（依赖 OPEN-ALIGN）：占位观测面（#8）、空批次推进（#3）、admission 拒绝码（#6）、v2 协议字段（#2）——编码前必须消歧，本文已留位不预设。

## 11. 与用户/root 指令的逐条对照（v2 自查）

首 transition 锚点用 P24 FINAL `2026-11-14T07:11:20.883030213Z` 真实 boot + 完整 image `fe3208b3fcbd…`（§1.1/§3.1-3.2），不以 Nov13T22:05 preflight 冒充 final、不要求 postboot2 立即小于当前 boot、不禁止后续合法候选 switch（§3.0/§3.2 策略 B）；owned-history 17/32/15/0+`8c18999a…`+旧 artifact/BuildJob 新鲜保护（§3.3/§6）；cohort_gate_p24.py 不原样复用（§3.0）；guard 硬拒 partial/缺 field/错 sha/未知 id、首 transition 独立复核 postboot2 原来源（§3.1-3.2）；/files 不作 files_total1、新旧 fileID 分验、branch view/preview 另验（h2）；h4 非法候选允许临时 owner+failed 终态原子释放（h4）；取消证明用行+proxy 零 dispatch（h9）；noACK proxy 先转发亲验 COMMITTED 再吞响应、远端可证、不造假零发布声明；SQL RAISE 仅证回滚（h12/h14）；P24full7 适配入序、P1post9/artifact/BuildJob reread 在 ABC 前、ABC 后严格只读含写探测禁令（§6）；tail 后 scoped roster+扩展 verifier、8+k 精确、owner/run 双 roster 分账（§3.4）；计数仅同 cohort 比较（§6）；采集前 catalog 纪律禁静默空映射（§7.6）；handoff 文件全在本目录不写 repo 根（头部授权边界）；本轮仅 foundation 纯自测+真实派生、guard DRAFT false、无 runtime（§0/§8）；primary 旧协议已否决不写旧 fixture/HTTP 场景（头部/§4.2）；readonly baseline 由 root 单独 release、不重复问用户（头部）。

## 12. v1→v2 变更记录（root review-tools-round1 落实清单）

**round-7 补充（完整工具套件候选已就位）**：formal_run_p25.sh（23 相锁序 driver：list/dryrun、P25_CONFIRM+RELEASED_RUNTIME 双门、写相未放行一律 rc3、ABC=最终 q3 写、tail 声明先于 tail、read-only 相免 runtime 门）；p25_tail_roster.py（预声明哈希冻结双 roster——非终态 run roster 与 checkpoint owner roster 独立，terminal-failed owner 永不清理/收养）；check_owned_quiesced_p25.py（owner 感知静默：blocked 证明只消费 pre-ABC 已存工件，禁 POST）；collect/verify_postfacts + collect_persist_state（终量恰 6+ABC2+k，tail 行单列不重复计数，owner 分账保留，计数器稳定+真 quiet）；check_running_images（全 ID 相等+新服务相位存在性）；standalone（redaction 站点日志+重放对照，未记差异/空转站点均拒）；finalize（rc/verdict/必需步→secret 0/0→manifest→tools-sha→hash-manifest LAST）；旧套件 shim+adapter（精确 5-token 内嵌 guard argv→P25 首过渡 shim，余 argv 原样，畸形拒，记录+SHA 链，P23-Q 恰一；adapter DRAFT）。纯测 tailtools 25/25（attempt-14）。
**round-4 补充（root review-tools-code-round2 六闭环已全部落实）**：①collect 入口改载 guard 模块、纯测走 actual main/collect 入口（进程内旗标+模块级 IO mock）；②owned SQL 按 sealed-17 UUID 字面量 ANY 过滤、全局 controlled6 保持整集、新增全局 grants=0 与 nonempty-cron 精确集合门、mock 按 SQL 真实语义路由（缺过滤即拒）；③C 门补 branch_name=main 与 files-list ownership（transaction_id+logical_path run 绑定）；④正式 verifier/guard 全部从固定 P24 seal（SEAL_P24_*_DEFAULT+哈希链）重派生，sibling 文件必须与派生逐键相等，CLI 仅收 --attempt-dir，toy 只可注入内部纯函数；⑤attempt-summary=消费账本（逐文件 bytes/sha/rc+plan sha），verifier 双向重哈希核账，local-sha 恰三键且 MISSING≠MISSING、baseline SHA==8cd25d42；⑥计数器非布尔 int/active 恰 int 0/三计数齐全，畸形负例真实过内容检查器。
**round-2 补充（root review-tools-code-round1 11 条 must-fix 已全部落实，2026-11-14 foundation 轮 2）**：
1. derive 机械取值强化：sealed `01-source-hash-pre.txt` 的 combined 内容哈希**实测验证**==`8cd25d42…`（不再只输出硬编码串）+373 行断言；所有根钉常量改**单行字面量**（两次手拆续行各丢字符的教训：C-sha 与 source-combined 均被自检硬拒后修正）；消费新增 13 个 sealed 内容门文件（counters×2/C-live/volumes/env/fixture×2/persist fullstate×4/legacy×2，全部 manifest 索引+SHA 验证）；owned-history 文件按 SHA 实测绑定（`owned-history-20261114T150304.json`）；legacy 13 字段 SQL/KEYS/两 run id 从冻结 P22 `readonly_baseline.py` 机械 import；故障名从冻结 `smoke_p21.py:401-402` 实读正则验证。
2. guard：`load_roster` 全量校验（逐行恰 27 键/字符串类型/started 可析/组尺寸/组 id 恰等 frozen 集/锚点 RFC3339/全 hex image/provenance 链/生成物 sha==frozen baseline 绑定）；两 adjudicator 入口先校验 expected 行（截断 expected 记录+对应 live 漂移必拒，自测 F2 实证）；steady 必须同时携带 reviewed target boot+完整 image（`validate_steady_args` 纯函数，缺一拒）。
3. collector 重写：附加后缀命名（无 with_suffix 破坏）、cmd 先于执行落盘、TimeoutExpired/OSError 保留 partial stdout/stderr+原始 rc 诚实记 `unknown`+wrapper 另标+整 attempt 失败；catalog 扩为 13 组内容门步骤（dataset/pipeline 全 image、aux `ps -aq` 含 stopped 的空输出、卷名、双 env mode 600、fixture boot/PID1、legacy 13 字段、owned fullstate 精确 17/32/15/0、C-live txn+file HTTP、pg_proc/pg_trigger 真实零计数、9 个受保护 src 计数×2 全字段）；**零 CP 表查询**；执行核心 `execute_plan` 依赖注入 run_cli/http_get（供纯自测走真实命名/编排）。
4. verifier 重写：逐 plan 条目精确消费（cmd==plan 渲染 argv 含 CID 代换、rc 必真 0、http 200、local 类型化、unknown/timeout 拒、未声明多余文件拒、attempt-summary 一致性）；全部内容门对照 frozen baseline（owned fullstate 归一化后逐行精确比较、计数器 int/active0/inventory 相等/read1==read2 全字段、C 文件 bytes+sha、故障 0 计数、卷/模式/fixture）。
5. 纯自测：**ONE coherent mocked collector→verifier** happy（真实 plan 构建+真实命名+注入 mock IO；唯一软化=C 文件 bytes/sha 以确定性 mock blob 记入临时 frozen 副本，尺寸保持 22766，其余门全部消费真实 sealed 值）+ 删单一必需 artifact 负例（精确点名）+ timeout 负例（partial 保留/rc unknown/wrapper/verifier 拒）。三处既有自测错误（8 文件计数/image 手抄/mini 复用 mkdir/C2 with_suffix）全部按实测修正。
6. runner：attempt-N 目录永不覆盖；`--only` 过滤支持"只跑变更步"；历史失败 attempt（1/4/5）全部保留。


1. transition 锚点更正为 P24-final boot `2026-11-14T07:11:20.883030213Z`+完整 image（v1 误用 Nov13T22:05）；该值降级为组一历史 provenance（§1.1/§3）。
2. 废除"全员<当前boot"的一刀切与"old2 永不跨重启"：引入 first-transition/steady 双显式策略；合法 reviewed 部署不受禁（§3.0/§3.2）。
3. expected 与 live 双侧恰好 27 键 schema 强制（v1 仅比较存在键）；cohort_gate_p24.py 标注不可复用（§3.0/§3.1）。
4. 新增 owned-history 17/32/15/0+SHA+旧 artifact/BuildJob 的新鲜保护与 source-only 基线 siblings（§3.3）。
5. tail 处置改为 predeclared scoped transition roster+扩展 tail verifier；owner-roster 与 run-roster 分账（owner 可挂 terminal failed run）（§3.4）。
6. primary v1 last-page-request-cursor 协议否决；fixture 方向改为独立固定数据查询参数+终端响应完成断点，禁页重放冒充 A→B→C（头部/§4.2/OPEN-ALIGN-2）。
7. h2：成功批 txn 全程 COMMITTED（删"OPEN→history"误写）；latest 用实际 branch 有效视图/preview 端点；禁 files_total==1；新旧 fileID 分别下载（h2）。
8. h4：非法候选允许已建 Run/临时 owner，期望 failed pre-publish 终态+原子释放+位置不变（删"从未占用"）。
9. h9：取消证明改为行状态+proxy 零转发；fixture 完成计数 0 非通用断言。
10. h11/h12/h14：noACK=先转发亲验 COMMITTED 再吞响应、远端 txn/file/hash 可证；SQL RAISE 定位为回滚证明；"假零发布"禁令（§5）。
11. §6 补齐 P24full7 适配、P1post9、旧 artifact/BuildJob reread、finalbase3 image 校验、blocked-admission 写探测 before-ABC 禁令、tail 后不盲跑 six guard。
12. 计数断言限同名受控 src cohort；采集 catalog 纪律（禁静默空映射）（§6/§7）。
13. §8 改为 foundation 已实现/待放行两态；§9 增 16-18（sticky dispatch marker、source/credential epoch、busy tick outcome）并标注 v1 已否决项。
