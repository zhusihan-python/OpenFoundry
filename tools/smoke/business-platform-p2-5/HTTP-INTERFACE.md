# HTTP-INTERFACE.md — smoke_p25.py 与 Runtime driver 的精确接口契约

- 作者：Primary（GPT-6.1 Sol 接管 smoke_p25.py），2026-11-14；round-3 全面更新。
- 读者：**Runtime GLM（driver 唯一作者）**、Archive 与 root。本文把 driver 需要
  做/提供的每件事钉死为可机械判定的输入输出形状——不留"driver 负责"式缺口。
- 归属：引擎/本文件＝Primary；driver＝Runtime；fixtures/compose＝Tools；
  q3/PG/业务源码＝冻结。**任何一方不改他人文件**。
- Runtime 已完成候选 switch 与 h1 trial/只读补证，Root 已接受 h1 事实。本文不构成新增执行授权；full window/crash/tail 继续按 Root 分阶段放行。

## 1. 引擎 CLI 精确面

```
python3 -I smoke_p25.py --plan | --contract-probe | --list-gates
python3 -I smoke_p25.py --ast-check | --selftest-mocked     # 纯，随时可跑
python3 -I smoke_p25.py --phase healthy|tail [--only hN] --released \
    [--evidence-dir DIR] [--result-file FILE] \
    [--pg-proof-file FILE] [--pg-proof-name NAME] \
    [--connector-base URL] [--dataset-base URL] \
    [--fixture-base URL] [--proxy-base URL] [--fixture-src-url URL] \
    [--sql-env-dir DIR] [--owned-handles-file FILE ...] \
    [--only h10 --h10-stage pre|post --h10-defs ID,ID \
        --h10-held-def ID --h10-prior FILE --h10-restart-record FILE] \
    [--phase tail: --tail-declaration FILE]   # 语义声明，必需
```

- 退出码：**0** 全绿；**1** 任何 gate/quiet/roster/裁决失败；**3** 拒绝（未
  `--released`、缺 `P25_CONFIRM=yes`、tail 缺/坏语义声明）——rc3 零网络。
- 环境变量（driver 注入，值永不入证据）：`OPENFOUNDRY_SMOKE_JWT`（org-1 主
  JWT）、`OPENFOUNDRY_SMOKE_JWT_ORG2`（真实第二组织 JWT，h7 必需）、
  `OPENFOUNDRY_SMOKE_ARM_JWT`（schedule:arm 授权；arm 走 Authorization 头）。
- `--sql-env-dir` 直接传 q3 env dir（含 `compose.yaml`）。Root 选择唯一固定 transport：`docker compose -f <dir>/compose.yaml --project-name openfoundry-platform-p1-q3v9 exec -T postgres psql -U openfoundry -d connectors -X -A -t -v ON_ERROR_STOP=1 -c <SQL>`。q3 postgres 不开 host port，无 sql.env/DSN 依赖。SQL 为单一 BEGIN READ ONLY / 五条常量 SELECT / ROLLBACK，仅接受已登记自有 Def/Source UUID；cmd 先落盘，原 stdout/stderr 文件、original rc、各 SHA / capture timestamp 落同 attempt。Runtime 仅传目录，不提供事实 JSON；双 release gate 之前不执行 SQL。
- `--owned-handles-file` 可重复传先前真实 `handles.json`，导入 source config 和 def 供独立 h10 pre/post 恢复 fixture src；tail 同样传 h10 准备步骤句柄，所有导入记录保留原文件 SHA。不得用此参数注入未创建 UUID。known_bindings 的已存 def 也会重新枚举。
- tail 只需 `--tail-declaration`（见 §5）；**不存在 `--tail-bindings` 输入**。

## 2. 部署前提（Runtime；不改他人 compose，用 deployment env）

1. **p25connector 显式 `OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=1`**（缺省
   0＝调度循环关）。就绪检查＝h13 arm gate：POST schedule:arm 返回体
   （schedule_auth.go:666–674）必须 `armed:true` 且 `scheduler_loop_running:
   true`（无 live/running 字段）。
2. aux connector `DATASET_SERVICE_URL` 指向 fault proxy；egress 含
   p25fixture；fixture/proxy 按 Tools compose override `--no-deps` 拉起。
3. 三个 JWT＋q3 env dir（固定 compose exec 只读用途）就绪。

## 3. 执行顺序（driver 编排）

1. **healthy**（h10 已被引擎从默认选择排除）：建议先 `--only h1`（fresh
   -fixture 首跑隔离正控），再 h2,h3,h4,h6,h7,h8,h9,h12,h13 逐个/全量。
   h13 必须带 `--sql-env-dir`（busy SQL 实证是引擎内建的，缺 reader 该 gate
   诚实失败）。
2. **h10 三步**（§4；SIGKILL 崩溃，不是 docker restart）。
3. **tail 语义声明先行冻结**（§5a），然后 `--phase tail --tail-declaration
   <decl>`（覆盖 h11×4＋h14×1 retained owner；h10 held 行已在 h10 相存在，
   声明里 retained h10 必须按 known_bindings 先绑）。
4. driver 汇总各步 `--result-file`＋引擎输出的 `tail-bindings-*.json`。

## 4. h10 精确序列（round-3 #2/#3：真实 SIGKILL 崩溃）

**准备（pre 之前，driver）**：
- 观察集＝**至少一个安静成功 def ＋ 一个真实 owner-held def**。
- held run 的准备：对 held 用的 fixture src 先 `POST /admin/knob/<src>`
  `{"slow_terminal_ms":15000}`，其 source config 带
  `sync.limits.request_timeout_ms=30000`，然后 `POST .../run?async=true`，
  轮询 `GET .../checkpoint` 至 `owner.run_id` 非空。**pre 不要求全局
  quiet**：引擎断言 held source 真实 in flight（fixture active>0）且其它
  观察 def 安静成功。
- 顺带做 §7 的只读 SQL pre 快照（driver 侧字节比对用）。

**pre**：
```
python3 -I smoke_p25.py --phase healthy --only h10 --h10-stage pre \
    --h10-defs <quietDef>,<heldDef> --h10-held-def <heldDef> \
    --h10-prior <workdir>/h10-prior.json --owned-handles-file <prepare>/handles.json --released <bases...>
```

**崩溃（driver；root 放行后执行）**：**只对一次性 p25connector 容器
`docker kill -s KILL`（SIGKILL），随后以同一 accepted image 启动**。不用
graceful docker restart——P2.2 的正确 graceful Stop 会取消 pre-gate 工作并
释放 owner，与"不变"预期矛盾。记录真实 kill/start 命令、rc、工件 SHA、
容器 StartedAt（前≠后）与事件时间戳。

**crash record（driver 写，post 必需；`restarted:true` 单字段永不绿）**：
```json
{"schema": "p25-crash-record-v1",
 "project": "openfoundry-platform-p1-q3v9", "service": "p25connector",
 "container": "<full-64-hex-id>", "container_id": "<same-full-id>",
 "image": "sha256:<accepted-full-64-hex>",
 "image_before": "<same-image>", "image_after": "<same-image>",
 "method": "sigkill+start-same-image",
 "kill":  {"cmd": "<raw>", "rc": 0, "artifact": "<absolute raw output file>", "artifact_sha256": "<actual-file-64hex>"},
 "start": {"cmd": "<raw>", "rc": 0, "artifact": "<absolute raw output file>", "artifact_sha256": "<actual-file-64hex>"},
 "health": {"cmd": "<raw health command>", "rc": 0, "artifact": "<absolute raw health output>", "artifact_sha256": "<actual-file-64hex>"},
 "started_at_before": "<RFC3339>", "started_at_after": "<RFC3339>",
 "timestamps": {"pre_at": "...", "kill_at": "...",
                "started_after_at": "...", "post_at": "..."},
 "held": {"def_id": "<uuid>", "source_id": "<uuid>", "run_id": "<uuid>"},
 "inspect_before": {"artifact": "<absolute minimal inspect JSON>", "artifact_sha256": "<actual sha>"},
 "inspect_after": {"artifact": "<absolute minimal inspect JSON>", "artifact_sha256": "<actual sha>"},
 "pre_snapshot_sha256": "<64hex = sha256(h10-prior.json 原字节)>",
 "performed_by": "runtime-driver"}
```
kill/start cmd 必须精确为 `docker kill -s KILL <full-id>` / `docker start <full-id>`；project 硬 pin q3，service 硬 pin p25connector，原始 kill/start/health 工件必须存在且 SHA 对实际字节。Runtime 用 `--h10-expected-image` 传外部 accepted image pin，HTTP 对比该 pin。HTTP 读取两份真实 inspect 工件，验证 Id/Image/Compose project+service labels/State.StartedAt。工件格式 `[ {"Id":"<full64>","Image":"sha256:<full64>","Config":{"Labels":{"com.docker.compose.project":"openfoundry-platform-p1-q3v9","com.docker.compose.service":"p25connector"}},"State":{"StartedAt":"..."}} ]`，只采最小字段，避免 Env 秘密。

引擎逐项强校验（pre_at 必须等于 prior.taken_at，时序严格递增、StartedAt 前≠后、held 三元组＝pre 快照里的
真实 owner、pre 快照 sha＝实际文件字节）。

**post**：
```
python3 -I smoke_p25.py --phase healthy --only h10 --h10-stage post \
    --h10-defs <quietDef>,<heldDef> --h10-held-def <heldDef> \
    --h10-prior <workdir>/h10-prior.json --owned-handles-file <prepare>/handles.json \
    --h10-expected-image sha256:<accepted-full64> \
    --h10-restart-record <workdir>/h10-crash.json --released <bases...>
```
引擎断言：**真实 quiet**（轮询至 fixture 原慢请求自然结束、各观察 src
active==0）；每 def checkpoint epoch/len/sha 不变＋owner 行不变（held 的
blocked_reason 在租约到期后合法翻成业务词 `requires_reconciliation`，到期
前为 ""，两者皆可）；owner run_id/run_status/since 恒等，last_confirmed/mode/initialized 恒等；run-id 集合恒等且每行
status/generation/attempt/control_mode/publish_entered_at 逐字段相等
（同 id 代数移动＝takeover＝失败）；held run 保持非终态并入 run roster
（无自动收养/推进）。

## 5. tail 两步契约（round-3 #4/#5）

**(a) 执行前：hash 冻结语义声明**（tags/modes/期望状态；**不发明未来
UUID**；h10 已存在的三元组必须在 `known_bindings` 里先绑，置于 spec 外不改
冻结 sha）：
```json
{"schema": "p25-tail-roster-v1", "ok": true,
 "spec": {
   "owner_rows": [
     {"def_tag": "h11-no_txn_200", "run_ref": "terminal-failed",
      "mode": "no_txn_200", "blocked_reason": "requires_reconciliation"},
     {"def_tag": "h11-not_committed", "...": "..."},
     {"def_tag": "h11-wrong_target", "...": "..."},
     {"def_tag": "h11-http_500", "...": "..."},
     {"def_tag": "h14-drop", "run_ref": "terminal-failed",
      "mode": "drop_after_verify", "blocked_reason": "requires_reconciliation"},
     {"def_tag": "h10-held", "run_ref": "same",
      "blocked_reason": "requires_reconciliation"}],
   "run_rows": [
     {"def_tag": "h10-held", "source_tag": "h10-src",
      "semantics": "p25-crash-held-prepublish", "count": 1}],
   "produced_by": "p25-tail"},
 "declaration_sha256": "<sha256 of canonical spec json>",
 "known_bindings": {"h10-held": {"def_id": "<uuid>", "source_id": "<uuid>",
                                 "run_id": "<uuid>"}}}
```
（`mode` 是引擎为 h11/h14 自动附加的实际场景模式；`run_ref=terminal-failed`
要求观测 run_status∈{failed,aborted}，`same` 要求∈{pending,running}。）

**(b) 执行后：引擎输出实际绑定＋精确裁决**。tail 相结束后引擎：
- `final_rosters` 按全部 def 句柄重列**两个 roster**（owner 含 terminal
  failed；nonterminal run 含 sync_def_id/generation/attempt——真实父链），
  失败时同样枚举；
- 按声明逐 tag 匹配（known 优先精确 (def,run)；其余 owner 按严格 mode 匹配，禁止缺 mode 时 fallback；retained run 必须 known_bindings 精确绑定），输出 `tail-bindings-<ts>.json`：
```json
{"schema": "p25-tail-bindings-v1",
 "declaration": "<abs path>", "declaration_sha256": "...",
 "bindings": {"h11-no_txn_200": {"def_id": "...", "source_id": "...",
                                  "run_id": "...", "kind": "owner",
                                  "mode": "no_txn_200"},
              "run/h10-held": {"def_id": "...", "source_id": "...",
                               "run_id": "...", "kind": "run",
                               "semantics": "p25-crash-held-prepublish"}},
 "bindings_sha256": "<sha256 of canonical bindings json>",
 "summary_sha256": "<sha256 of summary.json bytes>", "problems": []}
```
  （run 侧键带 `run/` 前缀避免与 owner tag 撞名；summary 里同步有
  `tail_bindings` 与 `tail_bindings_file` 字段——Runtime/Archive 消费这个
  **实际输出形状**，不存在需要猜测的 summary.bound。）
- 裁决硬规则：owner 集＋nonterminal run 集**双双精确相等**（缺任一期望＝
  失败；多任何观测＝失败；重复 UUID＝失败；**孤儿非终态 run＝硬失败**，绝
  不只是记录）；UUID 形状不够——run.sync_def_id 必须等于其 roster def、
  def.source_id 必须真实 join。本轮 run semantics 只接受 `p25-crash-held-prepublish`，必须匹配真实 controlled/nonterminal/publish_entered_at=NULL；不接受未执行的 dispatch/COMMIT unknown 声明。

## 6. 引擎输出句柄（driver/Archive 消费）

- `summary.json`：`scenarios[]`（每 gate ok/detail/evidence）、`quiet`、
  `owner_roster/run_roster`（场景级）、`final_owner_roster/final_run_roster`
  （finally 全句柄重列，含 source_id/sync_def_id/generation/attempt）、
  `tail_adjudication{ok,problems,declaration_sha256}`、`tail_bindings`、
  `tail_bindings_file`、`attempts{evidence_dir,records,handles,
  handles_file}`、`failed[]`（含 `finally-quiet`/`finally-rosters`/
  `tail-adjudication`/`sql-reader` 记账项）。
- `handles.json`：每次创建/失败尝试即时落盘（kind=source|def|dataset|run|
  h10-def；source 句柄含 config——供崩溃后 src 名恢复，无秘密）。
- attempt 记录：JSON 响应含完整安全 body；下载记 sha256+bytes；SQL facts
  记 cmd_file+raw stdout/stderr+原 rc+SHA；永无 Authorization/JWT/DSN 值。

## 7. driver 只读 SQL（自家取证/归档用；h13 busy 实证已由引擎内建）

引擎的 h13 SQL facts（stamp/next_fire/checkpoint len+sha/ticks/runs）在
`--sql-env-dir` 下自动采集。driver 仍可为 h10 pre/post 与归档自捕同形快照
（同一只读约定）：

```sql
BEGIN READ ONLY;
SELECT sync_last_admitted_at FROM connections WHERE id = '<src>';
SELECT schedule_next_fire_at, schedule_last_tick_outcome
  FROM batch_sync_defs WHERE id = '<def>';
SELECT confirmed_epoch, octet_length(checkpoint_value),
       encode(sha256(checkpoint_value::bytea),'hex'), active_run_id
  FROM sync_def_checkpoints WHERE sync_def_id = '<def>';
SELECT scheduled_for, outcome, run_id FROM sync_schedule_ticks
  WHERE sync_def_id = '<def>' ORDER BY scheduled_for;
SELECT id, status, generation, attempt, control_mode FROM sync_runs
  WHERE sync_def_id = '<def>' ORDER BY started_at;
ROLLBACK;
```

## 8. 红线

- Runtime 不改：smoke_p25.py、fixtures/、proxy、Tools compose/guard/归档、
  业务源码；调度 interval 用 deployment env，不动 compose 文件。
- 崩溃只按 §4 的 SIGKILL+同镜像流程，且只发生在 pre→post 之间并留强
  记录；引擎自身调用永不构成崩溃证明。
- 未真实执行的语义（lease-loss/local-commit-unknown）不得进 tail 声明；
  声明与观测必须精确相等。
- h5 由旧 HTTP 回归套件证明；driver 不为它跑新断言。
- 秘密（JWT/DSN/口令）永不写入任何证据/记录/summary/crash record。

## Sol 接管最终审查 / 放行边界

稳定候选 smoke SHA `ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2`。h1 第一次真实 trial 原 rc1 保留（POST SyncRun 无 incremental）；已修所有 wait=True trigger 必须 canonical GET，即使 POST 已 terminal。Fake POST/list 同样无 incremental，GET 独有安全投影。Runtime 已按 Root 授权只读补三既存 Run GET，观测 base_epoch 0/1/2；未重触发或 reset fixture。

最新 targeted pure 15 tests rc0、AST rc0；full mock 在最终窄修之前 476 records/119 handles rc0。只证明工具逻辑，未证明实际 h13/crash/tail。新 P25 fixture 唯一授权修改 slow_terminal cap5→30秒，pure 验证 15000=15s 与上限30s。Runtime 需保存旧新 boot/counters/ledger 边界后仅重启新 fixture reload；HTTP worker 未 live 操作。

semantic spec 强校验 tags/counts/states，无未来 UUID。`known_bindings` 必须已真实存在的完整三元组。声明同时用于启动及最终裁决，期间内容变化失败。h10 held slow fetch 语义为 `p25-crash-held-prepublish`；声明 blocked_reason 依实际 tail 观测，若 spec 要求 requires_reconciliation，必须等租约到期实际出现。

summary 先完整写最终 tail_bindings_file / failed，再计算 bindings.summary_sha256。h13 任意异常/早退均 finally revoke+disable 自有 schedule，保留已有 run/owner/句柄；从不恢复、删除或重放未知 owner。


Final h13 enqueue/terminal correction: exact tick.run_id is bounded-polled until terminal, succeeded and real parent asserted before artifact verification; busy-def first tick failure is explicit. Pure fake first GET running / next succeeded covers both scheduled batches and busy-def tick. Final source SHA ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2.

## 9. Runtime h10 逐步接线（冻结代码的精确用法；docs-only）

本节覆盖 §4 缩略示例：**每次 h10 都必须显式 `--phase healthy`**。本节不授予 live 权限；Runtime 等 Root 对 crash/tail 分别放行。所有目录使用不同 fresh attempt 子目录，原失败和 fixture ledger 保留。当前 smoke SHA 仍为 `ab32a145249770df34b7bd664ec4a41d8cf8d0a91e460f1d517735d6ae1741e2`。

### 9.1 prepare：先完成静态对象与 quiet 成功，再开始计时

Runtime 已持有主 JWT（仅内存）、四个 base URL、container 内 fixture URL、accepted p25connector full image ID、disposable p25connector full container ID。以下函数可直接接入 Runtime Python 调用上下文；`m` 是通过绝对路径 import 的当前 `smoke_p25.py` 模块，`work` 是已有 fresh 目录，`bases` 键精确为 `connector/dataset/fixture/proxy_admin`。函数只准备本轮两个实际 def，不使用旧业务对象或 schedule。

```python
# 必须只在 Root crash-window release 后调用；不是新的 CLI 模式。
def prepare_h10(m, work, bases, jwt, fixture_src_url):
    import json, os, time
    assert os.environ.get("P25_CONFIRM") == "yes"
    rec = m.AttemptRecorder(str(work / "prepare-evidence"), "h10-prepare")
    api = m.HttpApi(bases, jwt, rec, "h10-prepare")
    engine = m.P25Engine(lambda *a, **kw: api,
                         {"fixture_src_url": fixture_src_url, "live": True}, rec)
    assert m.proxy_set_mode(api, "passthrough")
    objects = {}
    # 不同 fixture src：quiet p25h3，held p25h13；两者都从各自新def初始CP开始。
    for tag, src in (("quiet", "p25h3"), ("held", "p25h13")):
        ds = engine.create_dataset(api, "p25-h10-" + tag, scenario="h10")
        assert ds and m._is_uuidish(ds.get("id"))
        cfg = m.incremental_source_config(fixture_src_url, src,
                  {"sync": {"limits": {"request_timeout_ms": 30000}}})
        source = engine.create_source(api, "p25-h10-" + tag, cfg, scenario="h10")
        assert source and m._is_uuidish(source.get("id"))
        sync, response = engine.make_sync_def(api, "p25-h10-" + tag,
                       source["id"], ds["id"], scenario="h10")
        assert sync and m._is_uuidish(sync.get("id"))
        objects[tag] = {"def_id": sync["id"], "source_id": source["id"],
                        "dataset_id": ds["id"], "fixture_src": src}
    q = objects["quiet"]
    run, _ = engine.trigger_run(api, q["def_id"], scenario="h10")
    assert run and run.get("status") == "succeeded"
    assert run.get("sync_def_id") == q["def_id"]
    assert not (m.checkpoint_of(api, q["def_id"]).get("owner") or {}).get("run_id")
    assert m.fixture_snapshot(api, q["fixture_src"])["active"] == 0
    q["run_id"] = run["id"]
    # Runtime 应在此以前已完成 inspect_before / accepted image&labels 核验、
    # pre argv/工件目录准备；从下一段开始不要再运行构建、旧套件或其它慢阶段。
    h = objects["held"]
    assert m.fixture_set_knob(api, h["fixture_src"], {"slow_terminal_ms": 15000})
    trigger_at = time.time()
    run, _ = engine.trigger_run(api, h["def_id"], async_mode=True,
                                wait=False, scenario="h10")
    assert run and m._is_uuidish(run.get("id"))
    h["run_id"] = run["id"]
    deadline = trigger_at + 3
    while time.time() < deadline:
        rr = api.call("connector", "GET", m.route("run_get", id=h["def_id"], run_id=h["run_id"]))
        cp = m.checkpoint_of(api, h["def_id"]) or {}
        fx = m.fixture_snapshot(api, h["fixture_src"]) or {}
        actual = rr.get("json") or {}
        if (rr["status"] == 200 and actual.get("id") == h["run_id"]
            and actual.get("sync_def_id") == h["def_id"]
            and actual.get("status") == "running"
            and actual.get("publish_entered_at") is None
            and (cp.get("owner") or {}).get("run_id") == h["run_id"]
            and fx.get("active", 0) > 0):
            break
        time.sleep(.1)
    else:
        raise RuntimeError("held run not proven running/prepublish/active in budget; retain evidence, no kill claim")
    prepared = {"objects": objects,
                "handles_file": rec.handles_path, "held_trigger_epoch": trigger_at}
    (work / "prepared.json").write_text(json.dumps(prepared, indent=2) + "\n")
    return prepared  # 立即运行 pre；不要暂停给人工确认。
```

此处 prepared.json 只是 Runtime 自身传递实际 IDs 与路径的示例；smoke 不读取它，也不要求新增准备文件协议。smoke 只读取其 `handles_file` 指向的真实 `handles.json`。记录中必须含两个 source（`config.sync.query_params.src`）及两个 def；RunID 同样由 trigger helper 即时留档。不能手造 source handle 来替代真实创建。

### 9.2 pre → SIGKILL → start → post

由 `prepared.objects.quiet/held` 取 `QDEF/HDEF/HSOURCE/HRUN`，`HANDLES=prepared.handles_file`。四 base、fixture source URL 与 healthy 一致。命令的环境继承内存注入 JWT 和 `P25_CONFIRM=yes`，不要把 JWT 放 argv 或 cmd 工件。

```text
python3 -I <smoke> --phase healthy --only h10 --h10-stage pre --released
  --h10-defs <QDEF>,<HDEF> --h10-held-def <HDEF>
  --owned-handles-file <HANDLES> --h10-prior <work>/prior.json
  --connector-base <connector> --dataset-base <dataset>
  --fixture-base <fixture-admin> --proxy-base <proxy-admin>
  --fixture-src-url http://p25fixture:8090
  --evidence-dir <work>/pre --result-file <work>/pre-result.json
```

上面的 text 块是一个 argv 序列（换行仅供阅读），不能省略 `--phase healthy`。pre rc 必须为 0，`quiet.scope=h10-pre-expected-inflight`、`quiet.global_quiet=false` 是预期输出。`prior.json` 的 `defs` 顺序必须与之后 post 的 `--h10-defs` 完全相同。pre 本身不制造 Run，也不 kill/start。

计时从 held async trigger 开始；建议 3 秒内进入 pre、5 秒内实际 SIGKILL。15 秒是 fixture 请求 hold，不是 pre 获准再等待15秒。pre HTTP transport 单次 timeout 可能30秒，因此超过时间预算不能继续声称成功 crash。kill 之前 Runtime 再次只读核 same Run running、CP owner same、publish NULL、fixture active>0；如窗口已过，留原现场与失败，不重触发、取消、清 ledger 或把自然完成算作 crash。

kill/start 必须由 Runtime 预先写 cmd 工件并捕获原 rc/raw stdout+stderr，然后记录事件实际时间：

```text
docker kill -s KILL <full64 CID>
docker start <same full64 CID>
```

只允许该一次性 p25connector；先验 `project=openfoundry-platform-p1-q3v9`、`service=p25connector`，不是 base3 的 restart，也不是 `docker compose restart`。同一 container ID 的 restart 后 `.Image` 必须等于外部 accepted image pin。

inspect before/after 只需要 §4 的最小 JSON array，可用只输出 Id/Image/指定 Compose labels/State.StartedAt 的 `docker inspect --format`；不归档 Env。before 工件可在 held trigger 前捕获，after 工件必须 start 后实际采。health 用实际探活命令成功 rc0，保存原工件；无 health 工件不可置真。

在写 §4 crash record 时：

- `pre_snapshot_sha256=sha256(prior.json 原字节)`；`timestamps.pre_at=prior.taken_at` 精确值（该值格式为 UTC RFC3339 秒）。
- `timestamps.kill_at` 为真实 kill 事件时间，严格晚于 pre_at；`timestamps.started_after_at` **必须等于 inspect_after.State.StartedAt**。
- `started_at_before/after` 分别逐字等于两份 inspect 的 State.StartedAt；after 必须晚于 before，before 早于 kill。
- `timestamps.post_at` 在 start+health 完成后取实际时钟，严格晚于 started_after_at；应保留亚秒精度，避免把同秒事件都截成同一秒。
- `held={def_id:HDEF,source_id:HSOURCE,run_id:HRUN}` 必须与 prior owner 和真实 source join 一致。
- kill/start/health 三个 action 都必须是 `{cmd, rc:<int 0>, artifact:<absolute existing raw file>, artifact_sha256:<actual raw bytes sha>}`，不能用 bool false 充 rc0。
- `inspect_before/after={artifact:<absolute JSON file>,artifact_sha256:<actual bytes sha>}`；`image/image_before/image_after` 都等于 accepted full `sha256:64hex`，`container/container_id` 都为同一 full64 CID。

```text
python3 -I <smoke> --phase healthy --only h10 --h10-stage post --released
  --h10-defs <QDEF>,<HDEF> --h10-held-def <HDEF>
  --owned-handles-file <HANDLES> --h10-prior <work>/prior.json
  --h10-expected-image sha256:<accepted64> --h10-restart-record <work>/crash.json
  --connector-base <connector> --dataset-base <dataset>
  --fixture-base <fixture-admin> --proxy-base <proxy-admin>
  --fixture-src-url http://p25fixture:8090
  --evidence-dir <work>/post --result-file <work>/post-result.json
```

post 等 fixture 原 sleep 请求自然退出（最多30秒轮询），再断言 quiet；不需要等 lease 过期才允许 post。其输出 protected 状态必须保持相等。`post-result.final_owner_roster` 包含 held owner，`final_run_roster` 包含该 nonterminal Run；source/def/run 的真实 UUID 不变。

### 9.3 tail 前 freeze 与 tail 后输出

post 成功后，使用只读 CP/Run GET 观察实际 held owner。只有当前 CP 返回 `owner.blocked_reason=requires_reconciliation` 才能把该值冻结进语义 spec。不能凭 elapsed 秒数或“已经 start”推断；业务源码 checkpoint.go 的规则是 **running 且 lease_expires_at <= DB clock** 才翻为该词，pending 的 reason 仍是空。h10 prepublish Run 仍须 running、publish NULL、同 generation/attempt；没有 takeover/replay。若选择冻结 `blocked_reason=""`，整个 tail 的最后实际观测也必须仍为空，否则 exact 裁决会失败；所以等待实际到期后再 freeze 更稳定。

在产生 h11/h14 新 UUID 之前构造 §5 完整语义声明：owner rows＝h11四种 mode + h14-drop + h10-held；run rows＝h10-held 一行 `count=1, semantics=p25-crash-held-prepublish`。所有终态 failed owner reason 为 requires_reconciliation。`known_bindings.h10-held` 使用 **已经真实存在** 的 HDEF/HSOURCE/HRUN，不能省略（retained non-mode owner/run 要求 known binding）。`declaration_sha256=sha256(json.dumps(spec,sort_keys=True).encode())`，默认 separators/ensure_ascii，与实际代码一致；known_bindings 放 spec 外，整个声明文件也记录 SHA 并禁止执行中改写。

```text
python3 -I <smoke> --phase tail --released --tail-declaration <work>/tail-declaration.json
  --owned-handles-file <HANDLES>
  --owned-handles-file <healthy-result.attempts.handles_file>
  --connector-base <connector> --dataset-base <dataset>
  --fixture-base <fixture-admin> --proxy-base <proxy-admin>
  --fixture-src-url http://p25fixture:8090
  --evidence-dir <work>/tail --result-file <work>/tail-result.json
```

第二个 handles 输入是先前 fullhealthy 真正成功/失败创建的全部 def/source；如果还有其他本轮已创建 def 的实际 handle 文件，也逐个重复传入，以便 finally 枚举不遗漏。禁止导入旧 P24/别的组织对象。tail 不需要 `--h10-*` 或 SQL reader 参数，也不接受 `--tail-bindings`；h10 已知 def 同时由 known_bindings 纳入枚举，但传 prepare handles 才能保留完整来源/父句柄链。

tail 完成后读取 `tail-result.json.tail_bindings_file`（不是猜文件 glob、不是 summary.bound）。该实际文件包含完整 bindings、bindings_sha256、summary_sha256、problems；summary 包含 `final_owner_roster/final_run_roster` 与 `tail_adjudication`。两份实际字节交 Archive；它的 verifier 输入 `--tail-bindings <实际文件> --tail-summary <tail-result.json>`。最终 owner 数预期6（含5 terminal failed），nonterminal Run 数预期1；数量只是辅助，裁决以精确 UUID/真实 parent/mode/state 和两个集合无 extras/duplicate/orphan 为准。
