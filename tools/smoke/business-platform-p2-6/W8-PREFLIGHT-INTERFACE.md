# P2.6 W8 — CURRENT-state preflight collector 接口（2026-11-15 r4=Root R2 窄修+base3 服务级 healthz 后）

Owner: W8（`preflight/p26_current_preflight.py` + 自有纯自测 + 本文件 +
`results/w8/`）。依据 `w8-current-preflight-authorized.md`（Root 独家 carve）
与 **`review-w8-round1.md` 四项修正（r2 落实）+ Root R2 三项窄修
（r3：parents SQL `AS def_id` 别名、缺 schema 键 structured 拒绝、
pre/post 镜像 expect 模式；Root 后续窄片：base3 **服务级**
`/healthz` 匿名读，见 §0b 末行）**。
W6 保全框架不重复、不修改；只读 import accepted helpers
（`common/p26_gates.py`、`cohort/p26_cohort_cli.py`）；P25 树零写入。
**Scope（Root 裁定）**：首片=`current_core_preservation`；
`full_old_regressions_complete=false`——全历史 Build/Job 域显式 defer 到
最终旧 P1/P2 HTTP 回归，绝不以"全 preflight complete"宣称；core 范围内
（9/27+parents、8 owner 全量 CP/Run/parents、base3 health/loops/grants、
fixture boot/counter/PG/卷/网络、H14/C 工件下载）**无一项可 skip 绿**。

## 0. Root R1 四项修正对照

| # | review 要求 | r2 落实 |
| --- | --- | --- |
| 1 | 8CP 尤其非 H10 缺当前 Def→Source/output/tenant/owner 完整安全 parents | 新 W8-own 只读 SELECT `sql-cp-parents`（见 §3B2；字段仅 IDs/org/owner/name，无 config/auth/opaque/token）+ 封存基线 `_load_owner_parents`（quiesce driver 真实公开响应 sync_get×31/connections×全 distinct sources，manifest-verified）+ `compare_parents`：整 8 集、逐字段、cross-field **Def.source==CP.source / Def.output==CP.target**、tenant 绑定 Def.org==Conn.org（==封存 Run.org 在档时）；**基线缺席字段显式 coverage 报告（baseline_absent），绝不猜值**（自测负控）。真实 plan 已核：8 defs / 7 distinct sources（含 p25-h10-held source，owner=main actor） |
| 2 | full safe CP/run 对象须落可复算 safeprojection，不能只存 hash | view-cp/view-runs/h14-txn 200 响应**完整公共对象 JSON**（sorted、标注 `[safe_projection]`）写入 `.raw` + status/sha/len 摘要；download follow 的**工件字节**全量落 `.raw`（与封存同字节可复算）；mismatch detail 字段级安全 |
| 3 | fail RESULT/stderr 绕过 redact；异常先 cmd 后原件不能逃逸 | `Attempt.step_begin/step_finish` 两阶段：**安全 cmd 先落盘再执行**；argv/http/download 每请求 try/except——异常时原件（类型+消息，经 redact）落 `.raw`+原始非零 rc，绝不丢步/不覆盖首错；`fail()` 与 RESULT.json 全部过 `att.redact`（registered secrets+sig=/Bearer 模式）；**JWT secret 在 run 一开始就预注册进 scrub 列表**（jwt-secret-preload 步），任何阶段的异常文本都压得住 |
| 4 | base3 health + network identity 补采；historical≠live 分开 | 每容器新增白名单探针 `health`（`{{if .State.Health}}…`：fixture/postgres 必须 `healthy`，base3 如实记 `no-healthcheck`——liveness 由 StartedAt/Running/RestartCount pin 承担）与 `network`（网络名+IP：全员恰 `{q3}_p1net`，fixture IP==compose pin `172.28.9.9`）；仍绝不读 `Config.Env` 原值（names-only 模板）。RESULT/plan 均带 `scope` 与 `full_old_regressions_complete` 独立旗标 |

## 0. 核心区分（本工位存在的理由）

`p26_switch_guard guard` 绿 == **历史封存链完整**（seal+derive），不证明
q3 现场仍是该状态。本工具做**当前 fresh 只读对比**，结果里
`historical` 与 `current` 两节严格分开；本回合只交付 code/pure+plan
（filesystem facts），**零 docker/HTTP/SQL**（自测以注入计数 fake 证明）。

## 0b. Root R2 三项窄修（r3）

| 项 | 落实 |
| --- | --- |
| parents SQL `d.id::text` 缺 `AS def_id`（row_to_json 键为 `id`，比较器读 `def_id`，首 run 必错） | 已改 `d.id::text AS def_id`；自测新增 **SQL 投影 shape 断言**（别名/未别名列 ⇄ `PARENTS_REQUIRED_KEYS` 键集对应，防再犯） |
| 缺 schema 返回 structured fail-closed | `compare_parents` 每行先做必需键集校验：缺键 → `row[i] missing schema keys [...]` 计入 problems（fail），绝不 KeyError 崩 |
| pre/post 小模式 | 默认=严格旧 pins；新增 `--expect-dataset-image / --expect-connector-image`（`sha256:<64hex>`，Root release 绑定，格式错 rc2）：**仅**已切服务以新镜像 pin 替代旧 image+StartedAt pin（新 boot 合法，StartedAt 记为事实），health/Running/RestartCount/loops/network/volume 核不变；Pipeline/fixture/PG 及未传 expect 的服务永远严格旧 pins；RESULT 记 `image_expectations`，plan 记 `switch_policy`（自测 4 例+格式校验负控） |

| base3 服务级 health（Root 后续窄片） | 3 个**服务** `/healthz` 匿名 GET 步（dataset 17078 / pipeline 17081 / connector 17088——三个 server.go root router 均注册 `GET /healthz`，已核实）：每个必须 200，cmd/raw/rc 与其它 HTTP 步同款；docker `no-healthcheck` 只证明无探针、不替代服务健康，不扩 scope |

## 1. 文件清单（r4 最终）

| 文件 | SHA256 |
| --- | --- |
| `preflight/p26_current_preflight.py` | `1afe4a0bfdf4fb58dd0501a287c9bc76ff8969bb39ebcf93dc48331547f7ae9a` |
| `preflight/selftest_p26_current_preflight.py` | `7069e168cc201e316f2086fdc6f2e1d141c075d85222e3e2c27e81e64b0d63ee` |
| `W8-PREFLIGHT-INTERFACE.md` | 本文件 |
| `results/w8/selftest-20261115T062453Z/`（rc 0，**46/46**） | r4 纯自测正式转录 |
| `results/w8/plan-20261115T062454Z-f476bf/`（rc 0，**67 步**，默认 old pins） | r4 真实 plan（零 live；`tool_sha256` 与本表一致） |
| `results/w8/plan-20261115T062107Z-d646a1/` | post 形态 shape 演示（`--expect-dataset-image` 管道证明；其 sha 为全零占位，非真实候选 pin） |
| 早于 r3 的 selftest/plan 目录 | 历史转录，append-only 保留 |

import 的 accepted helpers（只读）：`common/p26_gates.py`
`b504a9f893be45c2…2a1691`、`cohort/p26_cohort_cli.py`
`879ebd44fd649d8e2f…d0a618`（SHA 同时程序化记录于每个 plan.json 的
`tool_sha256`）。

## 2. 执行模型

```
python3 -I -B tools/smoke/business-platform-p2-6/preflight/p26_current_preflight.py plan
python3 -I -B tools/smoke/business-platform-p2-6/preflight/p26_current_preflight.py run \
    --execute --release-file /private/tmp/openfoundry-p26-release.json
```

- `plan`（默认模式）：仅 filesystem facts——封存 seal FULL 验证（5016 文件
  逐哈希）+ derive + 固定 origin 底板文件（compose/env **key 名**）。产出
  Root 可核的精确 argv/URL/pins/allowlist/SHA。`live:false`，永不冒充现场。
- `run`：真实只读 collector。必须 `--execute` 且 release token
  `released_for` 含 **`q3-readonly-preflight`**，否则整轮 REFUSED（rc 2、
  零 live 调用，自测证明）。历史基线不绿 ⇒ 现场检查前即拒（rc 2）。
- 每步独立 append-only 证据（UTC+token 唯一目录，不覆盖）：
  `NN-<sid>.cmd.json`（redacted）/`.raw`（scrubbed）/`.rc`（**原始 rc**）/
  `.meta.json`；wrapper 判定独立于原始 rc（`NN-*-gate.gate.json`+
  `.gate.rc`）。原始 rc≠0 与 wrapper 拒绝在 RESULT/first_error 里区分。
- fail-closed：错误/缺失/多余/重复/partial 一律失败；空输出绝不静默当
  绿；未知 step kind 拒绝（防静默跳过——这是自测抓出来的真实 bug 修掉的）。

## 3. current 检查内容（run 阶段，53 步）

A. **受控 9（SQL）**：verbatim 复用 accepted
   `OLDPG_READONLY_ARGV["controlled_rows_27col"]`（docker compose exec q3
   postgres，SELECT-only，`SET TimeZone='UTC'; SET DateStyle='ISO,MDY'`
   仅会话格式，无 DML）。当前行整集恰 9、逐行 27 安全列+父链与封存
   `public_projection_27` 全等；不按 UUID 过滤，多/缺/重/改任一字段即败。
B. **CP owner 8（SQL）**：verbatim `OLDPG_READONLY_ARGV["cp_owners"]`。
   整集恰 8（run 集=def 集=roster），逐字段全等（含
   `blocked_reason=requires_reconciliation`）；**protected9 身份的额外
   owner 同样拒绝**（无豁免，自测负控覆盖）；仅 count 相等不算过。
B2. **owner-8 完整安全 parents（SQL，R1 #1 新增）**：W8-own 只读 SELECT
   （def ids 程序化注入 roster；列名已对当前 migration DDL 核实）：
   `SELECT json_agg(row_to_json(t)) FROM ( SELECT d.id::text, d.org_id::text,
   d.source_id::text, d.output_dataset_id::text, c.id::text AS conn_id,
   c.org_id::text AS conn_org_id, c.owner_id::text AS conn_owner_id,
   c.name AS source_name, c.connector_type FROM batch_sync_defs d JOIN
   connections c ON c.id = d.source_id WHERE d.id IN (…8 ids…)) t;`
   ——无 config/auth/opaque/token。与封存基线（quiesce driver 真实
   sync_get/connections 公开响应，manifest-verified；实测 8 defs/7
   distinct sources）整集+逐字段比对，cross-field Def.source==CP.source /
   Def.output==CP.target，tenant Def.org==Conn.org（==封存 Run.org 在档
   时）；基线缺席字段只进 `coverage.baseline_absent`，绝不猜值（自测覆盖）。
C. **公共视图（HTTP×17）**：对 8 个 owner def（真实封存形状：H10 的
   def 本身就是 owner-8 之一——`owner+h10` 一员；protected9∩cp8 重叠恰
   为该 def，代码断言之）：GET
   `/api/v1/data-connection/syncs/{def}/checkpoint` 与 `.../runs`，与封存
   before.json 的完整 checkpoint 对象（mode/initialized/confirmed_epoch/
   owner{run_id,run_status,since,blocked_reason}）及**完整 Run 全集**逐字段
   全等；过期/失败 owner 也必须在（checkpoint 404=owner 丢失即败）。
D. **grants 0（SQL）**：verbatim 重放封存 `grants.cmd.json` 的 argv
   （31 个 owned def 的 `SELECT json_agg(... FROM sync_schedule_grants
   WHERE sync_def_id IN (...)`）；输出须恰 `SET/SET` 且零行。
E. **容器/底板（docker 只读白名单）**：`docker ps`（q3 5 容器在列）；
   每容器 inspect 仅 `--format` 白名单字段（Id/Image/Config.Image/
   StartedAt/Running/RestartCount/Config.Cmd/Mounts/Ports）——**绝不
   `Config.Env` 原值**：env 仅以 names-only 模板读取（`split "="` 取
   键名，值不出 docker）。谓词：fixture `StartedAt`==Root pin
   `2026-11-13T18:29:14.176785055Z`、Config.Image==`python:3.12-alpine`、
   PID1 仍 `python3 -I /srv/rest_source.py`、RestartCount 0；base3
   image/StartedAt==封存 `final-running.json`（程序化 pin：
   connector `sha256:6b556702…7592` / dataset `sha256:dc4eca36…9264` /
   pipeline `sha256:d50858d8…520`）；postgres Config.Image==
   `postgres:16-alpine` 且 Ports/PortBindings 全空（零 host port，用
   accepted `parse_ports_mapping`/`parse_empty_bindings`）；dataset 容器
   保有 `dataset-files:/home/nonroot` 卷挂载；**health 探针**（R1 #4）：fixture/postgres 必须
   `healthy`（compose healthcheck），base3 如实记 `no-healthcheck`
   （liveness 由 identity pin 承担）；**network 探针**（R1 #4）：全员
   网络名恰 `openfoundry-platform-p1-q3v9_p1net`，fixture IP==compose
   pin `172.28.9.9`；connector 循环 env 名单
   （`OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS` 等 5 项）**缺席=默认关**，
   出现即偏差。q3 `.env`：mode 0600、key 名集合恰 compose `${...}` 引用集
   （`OF_P1_PG_PASSWORD/OF_P1_JWT_SECRET/OF_P1_CREDENTIAL_KEY/
   OF_P1_SERVICE_JWT`）——plan 模式不读值；run 模式仅把
   `OF_P1_JWT_SECRET` 值读入内存签 JWT，scrub 列保障不出现在任何证据。
F. **旧 fixture（HTTP×10，全部匿名）**：`GET :17090/healthz` 200；9 个
   counter `GET :17090/admin/src/<name>` 200 且 body sha256 == 封存
   `03-counters.read1.derived` 逐名 pin（admin 读不增 request_count——
   仅 /data 路径自增，rest_source.py 已核）。**不向 fixture 发任何
   Authorization**（fake transport 断言层证明）。
G. **旧工件（HTTP×2）**：H14 txn `GET
   /v1/datasets/cb5f193c…/branches/main/transactions/bac73c15…` 安全投影
   （status/COMMITTED/content_hash/rows/bytes/file_count）与封存全等；
   H14 download `GET /v1/datasets/cb5f193c…/files/a6fef0ca…/download`
   期望 302 → Location 必须同为 `http://127.0.0.1:17078` origin 且前缀
   `/v1/_internal/local-fs/` 带签名 query → **恰一次匿名跟随**（不带
   Authorization；presigned query 只留 `sha256-16:len` 摘要，HMAC 绝不
   落证据）→ 200 且 bytes sha256==`0ac0b30ba6c571c4…dd79f`、len==558。
   **Build/Job：封存内无 catalog，明确跳过**（RESULT `skipped` 记录），
   不为查全历史扩框架，留待旧回归阶段。

## 4. origin allowlist（Root 冻结；plan 与 compose 交叉核对）

| 用途 | origin |
| --- | --- |
| Dataset（txn/download/302 目标） | `http://127.0.0.1:17078` |
| Pipeline（本轮不触） | `http://127.0.0.1:17081` |
| Connector（checkpoint/runs 视图） | `http://127.0.0.1:17088` |
| 旧 fixture（healthz/counters，匿名） | `http://127.0.0.1:17090` |

重定向：**禁自动跟随**（自有 no-redirect urllib；W7 http_lib 尚未
accepted 故不耦合）。任何其它 origin/前缀 ⇒ fail-closed（自测负控）。

## 5. JWT 政策（供 Root 裁决的显式选择）

- HS256，stdlib HMAC；secret 仅内存读 `OF_P1_JWT_SECRET`；claims 形状=
  accepted P1 `mint_token.py`：`sub`=封存 main actor（受控行
  `requested_by`）、`org_id`=封存租户 org、`roles=[]`、
  `permissions=["dataset.write","dataset:write"]`、iat/exp/jti/email/name。
- **旧 main actor 的确切 roles 未见封存记录**——本选择镜像 P1 accepted
  token 形状；若现场 401/403，run 以该 status fail-closed 并如实报告，
  绝不绕过/换 claims 重试。此点请 Root review 时特别裁决。
- JWT/secret/DSN/presigned sig 全部 scrub（自测扫描全部证据文件证明）。

## 6. 纯自测（49/49，最新转录 selftest-20261115T063258Z，rc 0）

负控要点：受控 9 缺/多（不过滤）/重复/改字段/空输出；owner 8 多
（protected9 身份仍拒）/改 blocked_reason/缺；公共视图多 Run/丢
matched Run（过期 owner 也不豁免）/checkpoint 404 owner 丢失/改
checkpoint.owner/改 Run 字段；grants 出行/坏前导；H14 txn 漂移；
redirect 错 origin/错前缀/无签名 query；release 缺失或未列 phase ⇒
rc2 零调用；baseline 拒绝 ⇒ 零 live 调用；plan 模式注入计数 fake ⇒
零 HTTP/零 subprocess；秘密/JWT/签名 query 不在任何证据文件；download
步骤必须真实执行（防静默跳过回归）；Bearer/sig= 正则 redaction。
r2 新增负控：parents 比较器（org 漂移/Def.source≠CP.source/多缺行/
tenant 不一致/基线缺席→coverage 不猜）；health 谓词（unhealthy 拒）；
network 谓词（错网络/fixture IP 漂移拒）；**异常逃逸**（transport 抛含
secret+sig= 的异常：cmd 先行落盘、原件保留、RESULT/raw 全 redact、零泄
漏）。r3 新增：parents SQL 投影 shape 断言、缺 schema 键 structured 拒
绝、post 模式 4 例（合法新 boot/镜像不符拒/RestartCount 仍核/pipeline
不受 expect 影响）+ expect 格式校验负控；r4：base3 服务 `/healthz`
500 负控（fail-closed 于该步）。fake 全绿只证明 mechanics，
**不构成任何现场验收绿**。

## 7. 本回合明确未执行（等 Root 逐条放行）

一切真实 docker/psql/HTTP（run 模式整个）；全历史 Build/Job 域（Root
裁定 defer 至最终旧 P1/P2 HTTP 回归，`full_old_regressions_complete=
false` 如实标注——非 skip 绿）；候选镜像切换前后检查（W6/W8 后续联
合）。本回合真实动作=只读 filesystem（封存 seal/quiesce 公开响应/
compose/env key 名）+ 写自有 `results/w8/`。

## 7b. 实际只读执行记录（Root release `p26-S-b94e6b2f-q3-current-old-readonly-pre`）

| attempt | 结果 | 说明 |
| --- | --- | --- |
| `real-q3-readonly-pre-attempt1` + `run-20261115T062622Z-ab06a0` | outer rc **1**（fail-closed） | 102/103 门绿（SQL whole9/8/parents/grants、fixture boot/counter、容器身份/网络/卷全等）；唯一红=view-cp-800aeeb5 404。**诊断更正（Root 裁定）：wrong-tenant token**——jwt-sign 误用 `protected9[0]`（P21 旧租户 c767…）作 org/sub；保全断言（404→owner lost）语义正确且未改动；attempt1 与 404 原件完整保留 |
| `real-q3-readonly-pre-attempt2` + `run-20261115T063308Z-57b547` | outer rc **0**（**PASS**） | 131 步记录、32 HTTP、**59/59 gate 全绿**；8×checkpoint+8×runs 全量 safe projection 落档、H14 txn/558B download 工件字节落档；old pins（无 expect）；泄漏扫描（JWT/sig 值）零命中；q3 零写（全 SELECT/GET） |

## 8. 放行后首跑命令（供 Root reference）

```
# release: {"released_for":["q3-readonly-preflight"],"released_by":"root",
#           "expires_utc":"<UTC>"} @ /private/tmp/openfoundry-p26-release.json
python3 -I -B tools/smoke/business-platform-p2-6/preflight/p26_current_preflight.py \
    run --execute --release-file /private/tmp/openfoundry-p26-release.json
# （默认=严格旧 pins；Root 首个只读放行即用上式）
# 候选切换后（Root 在 release 中绑定新镜像时）：
#   … run --execute --release-file … \
#     --expect-dataset-image sha256:<新dataset全ID> \
#     --expect-connector-image sha256:<新connector全ID>
```

一切 SQL/HTTP 均只读；不 clear/release/recover/adopt 任何旧 Run；不重
启/复用旧 fixture；不绑 17090-17094；不写任何 W6/W7/services/P25 路径。
