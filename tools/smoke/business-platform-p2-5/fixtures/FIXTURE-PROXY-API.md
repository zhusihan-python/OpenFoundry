# P25 fixture/proxy/compose API 说明 — for Primary's smoke_p25.py work

- 作者：Tools 轨道 GLM，2026-11-14。状态：**冻结交接 r2**（吸收
  review-fixture-proxy-round1 六修正；语义修订处已标注）。本文 + 所述
  文件由 Tools 拥有；`smoke_p25.py` 已移交 Primary 续写（Tools 不再
  编辑）。
- 一切服务均**未部署**（runtime 未 release）；本文是代码级 API 契约，
  语义由 `selftest_p25_fixture_proxy.py`（33/33 纯测）与
  `selftest_p25_loopback.py`（27/27，root 放行的 127.0.0.1:0 进程内
  真实 Handler/stub 测试）覆盖。

## 1. fixture：`fixtures/rest_source_p25.py`（host 127.0.0.1:17092）

协议（implementation-contract v1.1 §2.1/2.2 + fixture-proxy-round1）：
- 增量位置走**独立固定数据参数** `since_token`（默认；可按 src 配置），
  逐页同值；与 run 内分页参数 `cursor` 完全分离。
- **completed checkpoint 只出现在终止页**（`next_cursor==""`）的
  `meta.completed_token`。
- **r2 语义（关键）**：消费位点**只由请求 token 决定**——
  - `since_token` **缺失（NULL）⇒ 永远是首个窗口 A**（无论服务端交付
    过什么；发布失败后 NULL 重试必然重见 A，不会跳到 B）；
  - `T_i ⇒ 窗口 i+1`；只有**最新位置 token** 映射"已耗尽"（空行 +
    原 token 重发=确认位不动的空批）；未知/外部 token ⇒ 400；
  - 终止页 token **由请求窗口派生**，与任何可变状态无关：重复同
    token ⇒ 完全相同窗口与 token；旧 token 重放不移动任何调用方位置；
  - 分页期间窗口快照稳定（页 key 为内容 sha256，确定性跨进程）。
- **r2：`empty_batch` 旋钮已删除**。空批语义=耗尽位置（如上）；
  初始源为空的形态由专用 src `p25empty`（单批单空页：首请求即终止、
  0 行、token `p25empty-tok-0`）覆盖。
- 计数器为**真实请求生命周期**（r2）：每个 `/data` 进入
  started++/active++，任意退出（成功/注入错误/客户端断连）finally
  finished++/active--；延迟在锁外（阻塞某 src 时 healthz/兄弟 src/
  admin 照常进展；阻塞期 `/admin/src` 可见 active>0，结束真 quiet 0）。
- src 布局：`p25h1`=A(12+12)→B(8+8+4)→C(30)，`p25h2`=A→B(4)，
  `p25h3`=A；`p25h4`=A→B(8)（第二非空窗口，供 seed 后candidate 场景；nil 仍恒 A），`p25h6`/`p25h13`=三/二单页批，`p25empty`=空。

端点：
- `GET /data?src=<n>[&since_token=<t>][&cursor=<pg>][&page_size=N]` →
  `{"records":[…], "next_cursor":"pg-…"|"", "meta":{"completed_token":…}}`
  （终止页才有 meta；故障旋钮见下）。
- `GET /healthz`、`GET /admin/src/<n>`（旧 fixture 同形计数器+active）、
  `GET /admin/log/<n>`（请求账本，含 window/-1=未知）。
- `POST /admin/knob/<n>`：`fail_mid_pages`（第 2 页起 500）、
  `fail_terminal_after_rows`（终止页 500，**位置不消费**，NULL/同 token
  重试整批重服务）、`slow_terminal_ms`（锁外延迟）、`candidate_mode`
  （`missing|non_string|empty|oversize|ctrl_chars|echo_request_token`；
  echo=rows>0 且 candidate==**请求** token ⇒ 连接器应判
  ErrIncrementalNoProgress）。未知旋钮 404。

## 2. fault proxy：`fixtures/dataset_fault_proxy_p25.py`（host 17093）

aux connector（`p25connector`:17094）的 `DATASET_SERVICE_URL` 指向它；
仅 `POST …/outputs:commit` 受模式影响，其余流量透传（也入账本，标
`passthrough(non-commit)`）。
- **r2：每一次 commit dispatch 都入账本（含正常 passthrough）**——空
  账本即可证零发布。`GET /admin/stats`：
  `{commit_started, commit_finished, commit_inflight, modes{…}}`；
  aux 移除前要求 **true quiet**（inflight==0 且 started==finished，
  非固定零）。账本绝不记录 Authorization/JWT/DSN 值。
- **r2：故障模式各只破坏一个 ACK 谓词**（目标从 commit path 派生）：
  - `no_txn_200`：目标正确 + status COMMITTED，但**无 transaction id**；
  - `not_committed`：目标正确 + 合法 txn id，status=OPEN；
  - `wrong_target`：**仅** dataset_id 错（COMMITTED+合法 txn 保留）；
  - `http_500`：立即 502；
  - 以上伪造档**不转发**（账本 `forwarded:false` ⇒ 真实上游零到达可证）。
- `drop_after_verify`：先转发到 owned upstream→记录**完整上游 ACK**
  （body sha256 + 解析出的 dataset/txn id/status）+ 请求目标 +
  `verified_committed`（**仅** nonnil txn ∧ COMMITTED ∧ 精确目标 才
  true）→ 然后吞响应（连接器超时）。注意：proxy 账本不是提交证明，
  smoke 须独立直读 Dataset 真 COMMITTED/target/lineage/file bytes+hash。
- 模式切换：`POST /admin/mode {"mode":…}`；`GET /admin/mode`；
  `GET /admin/log`。

## 3. compose：`compose-p25-fixture.yaml` / `compose-p25-aux.yaml`

- 仅新增服务、**零 depends_on**；driver 必须
  `docker compose -f compose.yaml -f <override> --project-name
  openfoundry-platform-p1-q3v9 up -d --no-deps <显式服务名>`（root
  addendum）；端口 17092/17093/17094。
- `p25fixture`：PID1 `python3 -I /srv/rest_source_p25.py 0.0.0.0 8090`，
  ro 挂载，restart "no"，healthz 探活。
- `p25connector` egress `ALLOWED_EGRESS_HOSTS: p25fixture`；凭据/env
  沿 q3 `.env`（不新增秘密）。

## 4. 与 smoke 场景的对接要点（Primary 续写时）

- h1–h15 registry/ROUTES/FIELDS/错误码/tick outcome 已在 `smoke_p25.py`
  scaffold 钉死（含 root 确认的
  `GET /api/v1/data-connection/syncs/{id}/checkpoint`，server.go:95+130）。
- 计数器断言只比**同名受控 src cohort**（9 个受保护 src 见
  `p24-frozen-baseline.json` content_gates.protected_counters）。
- 零 dispatch 证明用 **proxy 账本 forwarded 计数**；fixture
  request_count 只作辅助（root round-1）。
- h2 禁 `files_total==1`；新旧批按具体 fileID 分别 download，latest 用
  实际 branch view。
- h14 tail 行为与 predeclared roster 由 Tools 的
  `p25_tail_roster.py`（待编）配合；ABC 锚序见 tools-design §6。

## 5. Tools 侧后续（不属 Primary）

formal driver（formal_run_p25.sh）、tail roster/quiesce、postfacts/
persist collectors、images 检查、finalize/standalone、P23/P24 套件
adapter——均不改本文件与 smoke_p25.py。
