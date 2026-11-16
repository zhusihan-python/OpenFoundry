# P2.6 W7 — HTTP 故障验收 harness 接口（code/pure 阶段交付，r3=R2 修订）

Owner: W7（`tools/smoke/business-platform-p2-6/http/`、`fixture/`、本文件、
`results/w7/`）。依据 root-decisions-v2（含 v2.1）、W1/W2/W3 冻结接口、
Root round-1/r1-fix 与 **round-2 review（`review-w7-round2.md`，五条全
部落实见 §11）**。W6 保全框架不重复、不修改；P25 工具只读参考。
本回合**零绑定/零服务/零 SQL/零真实 HTTP**；真实执行等 Root 逐条放行
（W6 正备新私有 PG；DVS 8/8 normal+race 已绿、Connector 17 在跑、W3
自动恢复细修中——均非本工位事实，如实引用）。

## 1. 文件清单

| 文件 | 作用 |
| --- | --- |
| `http/p26_http_lib.py` | redaction（Authorization/JWT/DSN/admin token + **since_token/cursor 不透明值→sha256+len 摘要**）、append-only 每请求证据、`BoundedWait`/`BoundedEvent`（≤30s、admin/timeout/close 三路释放）、release 门、**不跟随重定向**的 `UrllibTransport`（3xx 原样返回=失败，Authorization 永不去第二 origin）、凭据分离的 `Client`（auth 与 extra_headers 独立） |
| `fixture/p26_rest_source.py` | NEW REST fixture：纯核 + gated serve（admin fail-closed） |
| `fixture/p26_dataset_fault_proxy.py` | NEW Dataset 故障代理：纯核（可注入 transport）+ gated serve |
| `http/p26_scenarios.py` | 4 个端到端场景的可执行编排与硬断言（live-only；fake stack 只证 mechanics） |
| `http/run_p26_http.py` | CLI：`--list-scenarios` / `--plan-live` / `--selftest` / `--run-live`（未 release rc 3 零调用；缺任一凭据 env 亦拒绝） |
| `http/selftest_p26_http.py` | 纯自测 46 项（零 socket） |
| `http/p26_owned_ops.py` | owned aux 进程适配器（code/pure：CID/label 白名单、kill/start/inspect argv 规划、StartedAt+PID boot 身份、只读 lease 观察者 argv、私有 env 凭据端口；`http-owned-ops` release 门，未放行零执行） |
| `results/w7/` | 自测转录、mechanics-fake 证据、plan-live JSON、README |

## 2. release 执行模型（同 r1，未变）

`/private/tmp/openfoundry-p26-release.json` 列出 phase + 未过期，phase：
`http-fixture-serve` / `http-proxy-serve` / `http-live-scenarios`。
自测负控：无 release 时 serve rc 3 且零 bind（tripwire 未触发）；正控证明
release 是唯一闸。**r2 新增拒绝维度**：fixture/proxy serve 还要求
`--admin-token-env NAME` 且 env 非空（admin fail-closed），且拒绝
17090-17094 旧端口；proxy 另拒绝匹配旧底板标记（p25fixture/p25proxy/
p25connector/fixture-1/aux1709*/17092-94/17090）的 upstream 与非
http(s) upstream。

## 3. fixture（`p26_rest_source.py`）

- 协议面 `GET /data`（healthz/data 开放）；**全部 `/admin/*` 需
  `X-P26-Admin-Token` 精确匹配；核心未配置 token ⇒ 一律 403（fail
  closed）**；serve 启动缺 env/空值 ⇒ rc 3 拒绝。
- 位置语义/旋钮/计数同 r1（NULL⇒窗口0、T_i⇒i+1、耗尽重发原 token、
  未知 400；`hold_next_ms` 有界屏障）。
- **hold 释放（R1#2 修复）**：屏障在锁下从 pending 转为 active 并在
  wait 全程保持引用；`POST /admin/release/<src>` 命中**正在等待的**
  事件；`core.close()` 同时释放 pending+active；超时自动释放。纯线程
  测试覆盖 active 释放、pending 释放、close 解除阻塞。
- **期望 NDJSON 字节契约（R1#7 修复）**：expected 的 rows/bytes/sha256
  按"每条 REST 记录原样 JSON 字节 + '\n'"计算（字符串记录保留 JSON
  引号，与真实 connector `rest_snapshot.go:239-247` 的逐字复制一致）；
  自测断言"响应记录再序列化 == expected 字节"且首字节为 `"`；数据
  ASCII-only 有守卫断言。
- **不透明值摘要化（R1#3）**：`tokens_seen`/ledger 里的 token/cursor
  只存 `sha256-<16hex>:len=N`（与 lib `opaque_digest` 同格式）；原始
  值仅存在于协议处理所需的内存中。
- 请求体上限 1 MiB（413）；每连接 60s 读超时。

## 4. fault proxy（`p26_dataset_fault_proxy.py`）

- 固定单一 upstream；转发**原字节 body + Authorization**；live 复用
  `UrllibTransport`（**3xx 不跟随**——Authorization 永不被带去重定向
  目标）；账本永不存 Authorization/原始 body/原始 ACK。
- **strict 判定（R1#4 强化）**：`verified_committed` = 非零 UUID txn ∧
  COMMITTED ∧ dataset 精确 ∧ key 回显 ∧ **fingerprint == SHA256(本次
  实际转发的原始 body)**（W1 字节契约，不采信任何形式合法的外来 fp）∧
  **branch 回显一致** ∧ content_hash 为 64hex。
- **drop_ack_once 原子性（R1#4）**：一次性消费在锁下复核 mode——并发
  dispatch 中**恰一个**能 drop（自测 8 线程并发恰 1 drop）；非 201 或
  未过判定不消耗、不 drop。
- **boot 绑定观测（R1#5）**：每条 ledger 记录携带 `boot_id`；场景谓词
  按"本 Run 派生 key hash ∧ 本 boot_id"过滤，陈旧条目无法满足任何
  等待/断言（自测注入 stale 条目验证）。
- receipt GET 恒透传并单独记账（kind=receipt_query、key_hash、
  upstream_status、boot_id）——lookup-first 场景的独立凭证。
- `/admin/*` 需 X-P26-Admin-Token（fail closed）；`/admin/barrier/release`
  幂等成功语义（无屏障=无可释放=成功）；模式同 r1 五种；屏障
  ≤30s 有界、admin/超时/close 三路释放；请求体上限 64MiB+1（413）；
  60s 读超时。

## 5. 场景（9 个；全部可执行+断言，mechanics 纯测覆盖）

通用：**每请求（含全部 poll）独立 append-only 证据步**；等待超时以非零
rc 收尾再抛；观测过滤到本 Run key hash + 本 proxy boot（hold 观察条目
带 `held_phase`，不重复计 dispatch）；opaque 只以摘要进证据；facts 保存
白名单断言输入；**收尾清理走记录的 HTTP 并核验**（mode 复位、屏障释放、
proxy+fixture 真 quiet、owned roster 落档）；**CLI rc0 仅当
outcome==pass ∧ cleanup_verified**（`live_exit_code`，fail-cleanup 保持
非零）。场景创建的 dataset/source/def/run 全部入 `owned_roster`。

1. **healthy_snapshot_and_legacy**（同 r2）。
2. **healthy_incremental_epoch_once**（同 r2）。
3. **dropack_manual_owned_restart_lookup（R2#2 确定性改造）**：新 proxy
   模式 `hold_drop_once` 把真 committed 201 停在**有界屏障**（ledger 出
   现 held_phase 观察条目），此刻 connector 客户端可证仍阻塞在其请求
   内（重试不可能已发生）——在此窗口执行 **owned ConnectorOps**：
   stop → lease 过期（ops 模型 + run 行观测）→ restart（boot_id 可证
   变化、无持久 auth：旧平台凭据 recover 401 且**零外发**）→ fresh
   auth manual recover **lookup-first**（恰 1 次 receipt GET、零额外
   POST）→ finalize。全程无计时 sleep。
4. **dropack_auto_retry_exact_replay**（同 r2，独立自动重试场景）。
5. **inflight_receipt404_then_replay（R2 新增）**：`hold_request` 真实
   在飞（屏障 + 直连/proxy 双路 receipt GET 均为 404 且入账）→ 释放 →
   201 → finalize → 回执字节跨 GET 不可变（含一次经 proxy 的 200 查询，
   账本呈现 [404,200]）→ 已 finalized 的 recover = already_resolved 零
   新写。
6. **twoinstances_exactly_one_claim（R2 新增）**：A 持 recovery claim
   （run 行可观测 holder，owned `?hold_after_claim` 两段式对应 W4
   claim→dispatch 拆分）期间 B recover 得 **typed busy 409 且零外发**
   （错误体无 body/key/token 标记）；释放后 A 单独 finalize；全程恰 1
   个 body dispatch。
7. **budget_exhausted_then_lookup_finalize（R2 新增）**：send1 真提交
   但 ACK 被吞；sends2-3 `fail_before_forward`（零上游）——预算 3 持久
   （restart 不重置，send_attempts 观测）；耗尽后 run 保持 running；
   无 fresh auth 的 recover 被拒且**零外发**；fresh lookup-only finalize
   **不耗新 send**；DVS 全程恰 1 次 POST、1 版本。
8. **a_then_b_recover_a_head_unchanged（R2 新增）**：A finalize → B 新
   head → recover A 返回 already_resolved **零写**（同 txn/fingerprint
   回执、版本数不增、head 仍 B、CP epoch/last_confirmed 不回拨）。
9. **crossscope_permission_legacy_refusals（R2 新增）**：外域凭据
   receipt 404（与不可见不可区分）；无凭据 recover 403 零外发；owned
   proxy `tamper_receipt` 模式篡改回执**视图**一字段（上游存储不可变、
   只读转发）⇒ 严格客户端拒 finalize（run 不假绿）；legacy（非 P26）
   run recover typed 409；错误体无泄露标记。

**ConnectorOps 端口**（`p26_scenarios.ConnectorOps`）：stop_connector /
wait_stopped / restart_connector / wait_lease_expired / connector_boot_id
/ fresh_auth / release_recovery_claim / release_send_retries。live 实现
由 W6 协调（docker kill/inspect 等）；未接线时默认**fail-closed**（场景
明确报 "not wired"，绝不静默跳过）。纯测注入确定性 fake。

## 5b. R2 关键机制

- **live_exit_code**：rc0 ⇔ outcome==pass ∧ cleanup_verified（纯矩阵
  测试覆盖 pass/pass无cleanup/fail/fail-cleanup/None）。
- **origin 绑定（R2#3）**：release 文件 `w7_bases` 四 origins 为权威；
  CLI flag 必须与批准 origin **全等**，否则 rc 3；结构校验拒绝
  userinfo/query/fragment/path/旧端口（17090-94）；DVS 实际默认端口
  **50117**（r1 的 50078 是猜测，已废除）、q3 host 17078 记入账本，
  由 W6 部署事实定夺。
- **redact_json 精确键白名单（R2#5）**：仅精确命中
  authorization/token/jwt/secret/... 等键名才整体替换；`last_two_tokens`
  等普通键下的 sha256-len 安全摘要完整保留（有测试）；字符串值仍过
  pattern redact（header/JWT/opaque 查询值）。
- **hold_drop_once 的 held_phase 记账**：kill-window 的观察条目与最终
  dropped 条目同属一个 dispatch，计数断言按非 held_phase 条目。

## 6. 凭据分离（R1#1）

- **平台 JWT**（`OPENFOUNDRY_SMOKE_JWT`）只随 dvs/connector 客户端
  出门，绝不发往 fixture/proxy；
- **本地 admin 凭据**（`P26_HTTP_ADMIN_TOKEN`，随机值，fixture/proxy
  boot 与 runner 共享，仅进程内存/env）只以 `X-P26-Admin-Token` 鉴别
  fixture/proxy admin 面，两面 fail-closed；
- 两类值都不入 cmd/raw/rc/meta（redaction 另有兜底模式）。
  `--run-live` 缺任一 env ⇒ rc 3 零调用。自测在 transport 层断言
  "平台 JWT 从未出现在 fixture/proxy admin 请求头上"。

## 7. 资源账本（提议；plan-live-*.json 四份）

- 端口提议 fixture **17095** / proxy **17096** / connectorA **17097** /
  connectorB **17098**（放行前必跑 `lsof` 空检；serve 端已结构性拒绝
  17090-17094）；服务名 `p26fixture/p26proxy/p26connectora/
  p26connectorb`；绝不复用 q3 `fixture-1`(17090) 与 P25 aux(17092-94)。
- serve argv 带 `--admin-token-env P26_HTTP_ADMIN_TOKEN`；aux connector
  env：`DATASET_SERVICE_URL=http://p26proxy:8091`、egress 允许
  `p26fixture`；候选 image sha 由 W6 事实填充。
- boot 身份：`/admin/boot` boot_id（进程级 uuid4）。

## 8. 已验证（当前有效转录 `results/w7/selftest-20261115T051147Z/`，
`python3 -I -B run_p26_http.py --selftest` rc 0，**44/44 PASS**；r2 的
34/34 转录与 r1 的 26/26 转录均原件保留）

R2 新增证明（含负控）：`live_exit_code` 五态矩阵；origin 绑定（批准
origin 全等/结构拒绝/缺 w7_bases 拒绝，子进程级 CLI 测试）；精确键
redact 保留 `last_two_tokens` 安全摘要；`hold_drop_once` 确定性 kill
窗口（观察条目先于 drop、释放后 drop、一次性）；owned stop/restart 的
boot_id 变化 + 旧凭据 401 零外发 + fresh lookup-only；in-flight 404
（真实屏障 + 双路探测 + [404,200] 账本）；双实例 typed busy 零外发；
预算 3 持久跨 restart + lookup 收敛零新 send；A→B→recoverA 头/CP 不
回拨；tamper_receipt 视图篡改拒 finalize（上游只读未动）；legacy
recover typed 409；未接线 ops fail-closed；每请求（含 poll）独立证据
（场景证据步 ≥20、run-poll/proxy-log-poll 文件实测存在）。

未验项（诚实边界）：一切真实端口/HTTP/服务/PG/SQL/容器；场景对真实
connector/DVS 行为；端口实际空闲性。fake-stack 场景结果只证 harness
mechanics，**不构成任何场景验收绿**。

## 9. 后续 argv（供 Root review，均未执行）

```
# 0) 端口免占用检查（只读）
lsof -nP -iTCP:17095-17098 -sTCP:LISTEN     # 必须空

# 1) fixture/proxy（release 需含对应 phase；admin env 必须非空）
P26_HTTP_ADMIN_TOKEN=<random> python3 -I /srv/p26_rest_source.py \
    serve 0.0.0.0 8090 --admin-token-env P26_HTTP_ADMIN_TOKEN
P26_HTTP_ADMIN_TOKEN=<random> python3 -I /srv/p26_dataset_fault_proxy.py \
    serve 0.0.0.0 8091 http://<dvs-in-network> \
    --admin-token-env P26_HTTP_ADMIN_TOKEN

# 2) 场景（release 需含 http-live-scenarios + w7_bases 四 origins；
#    CLI flag 可省略——省略时用批准 origin；给出则必须全等）
OPENFOUNDRY_SMOKE_JWT=<jwt> P26_HTTP_ADMIN_TOKEN=<random> \
python3 -I run_p26_http.py --run-live dropack_manual_owned_restart_lookup
# 9 个场景见 --list-scenarios；plan-live-*.json 九份（results/w7/）
```

W6 依赖项：候选 image full ID；protected9/CP8 前后快照；最终 SQL 直证。

## 10. R1 review 逐条落实对照

| # | review 要求 | 落实 |
| --- | --- | --- |
| 1 | redirect 拒绝/JWT 不发 admin/独立 admin 鉴权 fail-closed/绑定新 owned 资源 | `_NoRedirectHandler`+`UrllibTransport` 双处统一；LiveCtx 双凭据分离；两面 admin 精确 token、缺配置 403、serve 缺 env rc 3；proxy 拒旧 upstream/旧端口 |
| 2 | active hold 可释放、有界、断言失败也清理 | pending/active 双引用、admin/close/timeout 三路释放；线程化纯测试 |
| 3 | raw opaque 不进 ledger/cmd | tokens_seen/ledger 全摘要；cmd 经 redact 查询脱敏；expected 匹配用 sha+len |
| 4 | ACK fp==实际转发 raw body SHA 且 drop_once 原子 | verify 六谓词；锁下复核消费；并发恰 1 drop；无伪造回执/原始体记录 |
| 5 | 观测按本 Run+boot 过滤 | ledger 携带 boot_id；谓词 key-hash+boot 过滤；stale 注入测试 |
| 6 | manual lookup-first 零额外 POST；auto retry 另场景 | 场景拆分为 manual（1 GET+finalize）/auto（同字节重发 200）；不强制第二 POST |
| 7 | string record 预期 NDJSON 带 JSON 引号 | `records_ndjson` 逐记录序列化；自测对账响应编码 |
| 8 | 每请求独立原件；保存白名单断言输入；span==精确页数 | ctx.http 每请求一步；facts 存 receipt/version/file 白名单；dropACK span 断言加入 |
| 9 | body/read/cleanup 有界且核结果 | 两面 body 上限 413+60s 超时；verify_cleanup 走 HTTP 记录并断言，失败降级 fail-cleanup |

## 11. R2 review 逐条落实对照（review-w7-round2.md）

| # | review 要求 | 落实 |
| --- | --- | --- |
| 1 | CLI 必须facts.outcome==pass∧cleanup_verified才rc0；失败清理保持非零；纯CLI失败用例 | `live_exit_code` 五态矩阵（pass+verified→0；其余全非零）+ 子进程/纯测双覆盖 |
| 2 | manual 与 W3 三次自动重试竞态⇒owned Connector 确定性 stop/lease过期/freshauth，无计时sleep；lookup-only 保持 | 新 `hold_drop_once` 模式 + ConnectorOps 端口（stop/wait/lease/boot/fresh_auth/claim释放/retry门）；场景3全序确定化；零额外POST保持（DVS恰1次POST自测） |
| 3 | live 4 bases 绑 Root 实批 origins；拒 userinfo/query/fragment/未知 host/旧端口；50078是猜测（实际50117/q3 17078） | `resolve_bases`+`validate_origin`；CLI flag 必须全等批准 origin；DVS 50117/17078 入账本，W6 部署事实定夺 |
| 4 | 全部 poll 走 ctx.http append-only；失败/清理步不覆盖主失败 | `wait_run_terminal`/checkpoint/status/admin-log/fixture-src 全部经 ctx.http 编号步骤；RESULT.json 单写；cleanup 独立步骤 |
| 5 | redact_json 保留 validated 安全 token 摘要；精确白名单/类型校验 | `_SENSITIVE_EXACT_KEYS` 精确匹配；`last_two_tokens` 摘要列表实测保留；Run keyhash+boot 过滤保持 |
| 扩展 | inflight404/replay、owned restart、双实例、预算、A-B-A、拒绝组、roster/cleanup safe | 场景 5-9 全部可执行+纯测；owned_roster 落档；verify_cleanup 加 fixture quiet |

## 12. First-live 协议修正（R2 附录，Root 对照实际 Go 源核实后落实）

1. **ListVersions = `models.Page{data,next_cursor,has_more}`**
   （handlers.go:487-512；分页 `limit`/base64 `cursor`）。场景侧
   `list_versions_all` 只解码该实际形状（`data` 列表 + 有界翻页 ≤8 页 +
   `has_more` 终止硬断言），不再接受 list/`.versions` 等未被实际支持的
   形状；fake DVS 镜像实际信封。
2. **DownloadFile = 302 presign + 单跳匿名取回**
   （handlers.go:748-796；two-step 见 output_commit_content_test.go:
   97-114）。全局 no-redirect 保持；仅此显式 download 手工跟一跳：
   - Location 校验 `validated_download_location`：与**已批准 files
     origin**（`w7_bases.dvs_files`，缺省回退 dvs；q3 实际
     `http://127.0.0.1:17078`）scheme/host/port 全等、无 userinfo/
     fragment、路径前缀 `/v1/_internal/local-fs/`；
   - 第二请求用**匿名客户端**（无 JWT、无 admin 头；fake 侧带凭据即
     403），最多 1 hop（后续 3xx 直接断言失败）；
   - 两步各自入证据（presign 步 rc 如实记 302）；HMAC 查询签名是凭据
     ——Location 原文/签名永不入 cmd/meta/raw（redaction 对
     sig/signature/hmac/x-amz-signature 等查询参数摘要化），只存
     path/query sha256+长度/白名单 expiry。
3. **端口事实修正**：q3 Dataset 容器 PORT **50078**（Go 默认 50117 在
   q3 未使用；proxy 内网 upstream `http://dataset:50078` 正确保留）；
   `DATASET_FILES_BASE_URL=http://127.0.0.1:17078`。Connector Source
   CRUD alias 与 Dataset create(text,201,id) 维持已核形状。

当前有效自测：`results/w7/selftest-20261115T052029Z/` rc 0
**45/45 PASS**（新增：Location 校验矩阵+签名脱敏、两步下载证据与匿名
hop 断言、presign 签名零泄露扫描）。first-live 小切片候选
（healthy_snapshot_and_legacy + dropack_auto_retry_exact_replay）就绪，
等 Root release（w7_bases + 双凭据 env + W6 origins）。

## 13. r4（round-3 actual-contracts 落实，当前有效）

**First-live checkpoint（供 Root 立即审）**：
`results/w7/CHECKPOINT-firstlive-shas.json`（终版，46/46 后生成）——
healthy_snapshot_and_legacy / dropack_auto_retry_exact_replay 两个场景
函数经 AST 提取比对，与 mid-stream checkpoint **逐字节一致**（重写过程
未触碰）；全文件 SHA 同步更新（prove_receipt_and_file 因 anon 客户端
移入 LiveCtx 而变化，两场景函数不受影响）。

**fake-only 契约清除（对照 memo 逐条）**：
- 无 W4/hold_after_claim/recovery_holder/retry-timer 钩子：双实例 claim
  强度改为**真实 receipt GET 停在 owned proxy**（新模式
  `hold_receipt_once`，一次性、锁内解除武装）观测；B recover 期间 409
  busy 且零外发；A 线程 finally 必 join。
- JWT 无状态：restart 不作废有效 Bearer；"无自动采纳" = 无 manual 调用
  的有界观测窗口内计数/外发冻结（`observe_no_auto_adoption`）；负控用
  实际语义（缺/坏 Bearer 401、有效只读同租户 403、有效异租户 404），
  凭据仅经**凭据端口**（live=私有 env；fake 常量为 stand-in，明确标注）。
- 计数在 `run.recovery.send_attempts/query_attempts`；恢复响应
  {run, recovery} 无 outcome 字段；already-resolved=真 200+行不变+零外
  发；legacy 409 钉实际 'not eligible' 措辞+行不变。
- inflight 全链路 same-body replay：`hold_request_once`（一次性，第二个
  POST 不再被全局模式拦）先于 DVS 停原 dispatch；owned crash+lease 过
  期；receipt 404 ⇒ 同 key 原字节 POST 成为真首提交(201)；再 admin 释放
  原 dispatch ⇒ 不可变 200 重放、崩溃执行器丢弃（无二次 finalize）；
  明确标注 proxy-held-before-DVS（非 DB 在飞，后者由真实 PG 测试证明）。
- 预算：新模式 `drop_then_fail_rest`——首个 verified 201 drop 与后续
  fail_before_forward 转换在**同一锁内原子完成**（W3 定时重试确定性耗
  尽 3 次，无外部轮询窗口）；场景改用**增量源**；owned crash/restart 后
  预算持久、零自动采纳；manual lookup 恰一次查询、无第四次 POST；CP
  owner 保持至 finalize 后恰 +1。
- 坏回执：`fail_receipt_once` 返回真实依赖错误 502（fabricated、
  forwarded=false、一次性），run 保持 running（绝不接受 failed）；释放
  后有效 recover 收敛。tamper_receipt 已删除。
- A→B→recoverA：head 绑定 **B 的 receipt Txn**（versions 列表 DESC 不
  作 head 依据）；already-resolved 真零外发；CP/last_confirmed 不回拨。
- 业务路径鉴权卫生：inflight 的代理 receipt 探测改用**业务 proxy 客户
  端**（平台 JWT）；proxy `_forward` 出向**剥离 X-P26-Admin-Token**（纵
  有误携带也不达业务上游）。
- owned 适配器 `http/p26_owned_ops.py`：仅 kill/start/inspect 新 owned
  CID（64hex+label 白名单、旧底板标记拒绝）；boot=StartedAt|PID；只读
  lease 观察者 argv（q3 psql，roster 精确 ID，无 holder/token/checkpoint
  值）；凭据端口私有 env；`http-owned-ops` release 门 + 逐操作
  cmd/raw/rc 台账；现仅 code/pure（真实执行等 Root 放行 + W6 names）。

**当前有效自测**：`results/w7/selftest-20261115T054811Z/` rc 0
**46/46 PASS**（45 项 + owned-ops 纯检查；中间 45/46、43/44 轮原件保
留）。fake 语义已对齐实际契约；旧轮 mechanics 目录按 append-only 保留
并在 README 标注其时代。q3 Dataset PORT 50078 / files 17078 / Go 默认
50117 未用于 q3 的事实保留在 plan-live `port_facts_r2`。

## 14. r5（round-4 live-wiring：first3 冻结 + 后 9 backlog）

**已落实的 first3-safe 窄修（本轮，已含入冻结集）**：
- lib `resolve_bases` 显式校验并返回 `w7_bases.connector_b`（第 6 个
  可选 origin；与 A 必须相异，否则拒绝）——B 不再被静默丢弃；
- CLI 安全接线：`requires` 含 `connector_ops` 的场景**只**从 Root
  release 的 `w7_aux`（精确 schema：64hex CID + 恰名 p26connectora/b +
  labels owner=p26-http/stage=p2-6/role=connector_a|b + 候选全 sha256
  image，A/B CID 相异、镜像相同、旧 q3 标记全拒）构建 OwnedAuxOps；
  缺 w7_aux / 坏 schema / 缺私有凭据 env（按场景 `credential_kinds`
  列名拒绝，值永不打印）/ B 场景无相异 connector_b origin ⇒ 全部
  **rc 3 且零 attempts**（子进程级测试覆盖四类拒绝）；
- twoinstances 场景入口门：B origin 缺失或与 A 相同 ⇒ 立即断言失败
  （防 LiveCtx 构造回退）。

**冻结 checkpoint（Root 放行 first3 用）**：
`results/w7/CHECKPOINT-firstlive-shas.json`（r5 FROZEN）——7 文件全量
SHA + first3 两场景函数 AST 提取 SHA（与历次 checkpoint **逐字节一
致**）+ 纯证据引用（selftest-20261115T062447Z，rc 0 `ALL 47 CHECKS
PASS`，原件未动）+ 就绪清单：
- **first3 ready**：healthy_snapshot_and_legacy /
  healthy_incremental_epoch_once / dropack_auto_retry_exact_replay
- **later-6 pending wiring**：其余六场景（见下 backlog）
- noactual 声明：本轮零端口/HTTP/PG/Docker 执行。

**后 9 项 backlog（Root round-4 memo 行号引用，冻结窗口后逐项实现；
本轮未动代码）**：
1. CLI→ops 已接线（本轮完成）；roster 驱动的 def/source 校验待
   owned-ops 实跑阶段并入。
2. lib B origin 显式返回（本轮完成）。
3. owned_ops 每次 mutate 前**真实身份复检**（docker inspect 全
   CID+name+labels+image 与 release 事实全等才动；A/B 相异；恒拒旧
   q3）。
4. lease 观察者：换 Root 批准的 q3 PG 精确容器/compose exec；SQL 改
   单行安全投影（DB clock/protocol/status/generation/lease_expired
   BOOL），完整解析返回值；Run→Def→Source typed-roster UUID 先行校
   验；禁 `docker exec -T` 裸写与任意 startswith 解析。
5. 每条真实 ops 命令：调用前 cmd、完整 stdout/stderr 原件 + 原始 rc
   + meta（复用 StepRecorder；stderr 亦脱敏；不落 secret/env 值）。
6. 实际窗口次序：twoinstances=真 hold/drop→stop 精确 A→release→DB
   expiry→start/ready→A claim 的 held receiptGET→B conflict；inflight
   =停 A 后不得再调死 A（restart/ready 或显式 B 恢复）；budget=重启
   后等旧 lease DB 过期再计第三次调用；crash 与 budget 至少一个保持
   增量源。
7. crossscope：foreign 凭据探测改**独立已批准 DVS foreign 客户端**
   （不得借 connector 视角 404 假阳性）；依赖错误=实际 502；第二次
   recover 前等 DB lease 过期（restart 本身不过期）；legacy 409 钉
   `run is not a p26-v1 recovery run` 实际措辞 + 全 safe 行不变。
8. A/B/A：head 证明改真实 `GET /v1/datasets/{id}/branches/main` 的
   `head_transaction_id`（recover A 前后 == B txn）；A 全 safe
   Run/recovery/budgets + 全 CP 相等；不得拿 B 的预算当 A 比较。
9. twoinstances finally：先 release+记录后 bounded join（断言无存活
   线程）；budget CP-held 断言改 200 且 owner.run_id==run、epoch==0
   （非 404/initialized-false 兜底）。

冻结窗口内不改以上文件；后 9 实现时 first3 两函数继续保持字节不变
（AST SHA 复验）。

## 15. Sol 接管后的实际 first3 与后续代码（2026-11-15，覆盖旧文档冲突）

正式 first3 已由 Root 独立接受，证据为
`results/w7/CHECKPOINT-first3-formal-actual.json`。三个原始 child Unix
`case.rc` 均 0、cleanup 均验证成功；actual candidate hash 前后均等于 S。
HTTP 每步 `.rc` 是 **normalized_harness** 值，原始 HTTP 状态在该步
`.meta.original_http_status`；HTTP 没有 child Unix rc，明确记 unavailable。
旧轮行为 pass 但原始轮询响应体不完整；旧 green、失败、当前补采目录均
保留，正式证据以新一轮为准。

`results/w7/formal-first3-tools-snapshot/` 保存正式执行时完整 7 个工具
blob + wrapper；程序逐项比对正式 checkpoint SHA 全匹配。后续代码与该
快照独立，不能把当前代码 SHA 冒充已执行 blob。

HTTP 证据新增安全响应投影：完整 Run/CP/proxy log/fixture counters、
版本、receipt、expected facts；Source.config/credential/raw publish body/
inline body 不存，不透明位置只留摘要；无响应头/presigned 原查询。
显式匿名文件下载另存 `.download.bin`，可独立重算字节数、行数及 SHA。
每个 before/after fixture 计数另入 RESULT，dropACK 使用本次 baseline
差值=1。运输失败或调用异常也先 cmd 后 raw/meta，原错误不吞掉。

### 后六场景 + 新 pregate：代码已接线，尚未 actual 验收

- Root release 中 `w7_cases` 若列出精确用例，CLI 拒绝越界；需要 ops 的
  场景在任何 HTTP/create 前必须具有 `http-owned-ops` phase、精确 A/B
  事实及 `w7_pg_observer`。LiveCtx 绑定随 HTTP 创建而更新的 typed roster
  与 EvidenceDir。没有新 owned Run 的 process 操作仍 fail closed。
- Docker 实际 label keys 为 **of-p26-owner / of-p26-stage / of-p26-role**；
  release 的 labels 是 normalized owner/stage/role。每次 mutate 前重新
  inspect exact full CID/name/image/三 labels；不读取 Config.Env。
- lease 观察只对 typed owned Run→Def→Source 做只读查询；使用真实
  `sync_runs.sync_def_id → batch_sync_defs.source_id → connections.id`，
  并要求 frozen recovery_source_id 匹配。PG argv 是 `docker exec -e
  PGOPTIONS=... <Root full CID> psql`，无非法 `-T`；只读 + 5s SQL 超时，
  安全投影为 DB clock/protocol/status/generation/expired。默认有界等待
  90s，绝不 force-expire/adopt old Run。
- 每条 ops 的 cmd 在调用前；stdout/stderr 完整脱敏分别落档，另 raw/
  meta/原始 rc。超时/启动失败不存在 child rc，明确 unavailable；不伪造
  124。readonly 凭据在 CLI/port 内存解析 shape：同 org、**不同 nonowner
  sub**、空 roles、只读 permissions（owner 总有写权），不输出 JWT。
- manual crash 的 boot-before 已移到新 Run 创建后；重启后记录实际 Docker
  boot 并等待真实 `/healthz`。inflight/crossscope 不再向刚 kill 的 A 请求。
- twoinstances：原 verified201 ACK held → SIGKILL A → release → proxy
  finished/DB expiry → same-CID restart/ready → A recovery receipt GET held
  → B 409 + 整个 safe Run/预算/generation 不变 + 零外发；finally **先
  release 并记录，再 bounded join**，线程残留使 cleanup 失败。
- budget 等 send count3 **且第三次 proxy 操作已结束**；CP 必须 200、
  owner.run_id==本 Run、epoch0。restart 后实际等 DB lease expired，再
  manual 单 query/零第四 send。自动窗口核 generation/query/send/外发均
  不变；JWT 重启后仍有 stateless 有效性。
- crossscope 的 foreign receipt 请求使用已批准 **DVS origin**，另验证
  valid writer + 另一新 owned Def 父链 404/整行不变/零外发；依赖错误
  必须 502 且 running；再次 recover 前 DB expiry。legacy 精确文本
  `run is not a p26-v1 recovery run` + whole safe row unchanged。
- **实际 recover 响应是平铺的 SyncRun 字段 + recovery**，不是早期 memo/
  fake 的 `{run,recovery}`。依据 `sync_recovery.go:531–543` embedded
  SyncRun；A/B/A 比较 recover flat fields 与 A GET 共享字段，另核 GET A
  full safe row/recovery 前后及 full CP 相同。`GET /v1/datasets/{id}/branches/main`
  的真实 head_transaction_id 前后都必须为 B receipt txn。

新第 10 场景 `pregate_no_intent_owned_restart`：新 incremental Source 的
fixture hold_next_ms=30000 → async 新 Run → active_hold 命中 → SIGKILL。
只读精确 DB 证明 running/p26-v1/generation、publish_entered_at NULL、
NOT EXISTS intent、send/query0、CP active owner 本 Run/epoch0；释放
fixture并核 quiet，实际等 lease expired，same CID restart/ready；未 manual
窗口无自动 source/claim/budget；fresh manual 同 Run 从 frozen baseline
重拉。核 one lost held request + expected pages、新 generation、send1/query0、
finalized receipt/version1/download SHA、CP 仅 +1/owner released。
没有 production hook、SQL 写入或绕过 lease。

纯 tests 与 fake stack 只证明编排/脱敏/安全 argv 等 mechanics；包括新
pregate fake case，DB proof 明确 fake_only。真实后 7 个用例、process ops
及 PG observer 均等待 Root 精确 phase，当前尚未执行。

## 16. manual/pregate actual 与剩余五项 witness 修订

Root 已独立接受 `CHECKPOINT-manual-pregate-actual.json` 两项 actual：
原 Unix rc0/0、全 owned 命令 rc0、DB clock 自然 lease expiry、同 CID 新
boot、actual S 前后不变、完整 source/file/Run/CP 输入可复算。执行时
9 源文件 + wrapper 的精确 blob 保存在 `manual-pregate-tools-snapshot/`。

剩余五项代码 checkpoint 为 `results/w7/CHECKPOINT-remaining5-code.json`。
新增 Source baseline→after-intent→final 的明确计数 witness；twoinstance
核 original generation→A 恰 +1、held query1/send1、B 整个 safe Run 不变、
final generation 仍为 A。crossscope 的另一新 owned parent 在 baseline
前创建，授权/父链负例逐组零外发并保持整行；502 只称 dependency/transport
负例，**不声称 HTTP wrong receipt field mutation**。后者的严格校验是
另有 unit/PG 证据。专用 remaining5 wrapper 还显式比对 Root release 的
reviewed_later_tool_shas / reviewed_later_wrapper_sha256，不能凭本地
checkpoint 自行扩大审批范围。

剩五 code/pure 最新 full48 rc0；此前46/48两项是 fake 的query预算放在GET
后，以及纯测依赖真实Root release phase，已只修fake并保留失败原件。
`remaining5-tools-review-snapshot/` 预先保存当前9源码+wrapper精确blob。
尚未执行 remaining5，等待 Root 新精确 phase；不沿用 manual/pregate 的
scenarios SHA 或放行范围。
