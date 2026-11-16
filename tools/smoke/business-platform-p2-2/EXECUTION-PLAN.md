# P2.2 正式验收执行序（FINAL，等待 root 明确 formal release 后执行）

> 2026-11-14 定稿。本文只准备命令与顺序；**未获 root 明确放行前不得
> 执行任何 build/up/正式回归**。执行者：本 GLM-5.3（最高档）。
> 只读基线已归档：`results/preparation/2026-11-14T021922-readonly-baseline/`
> （运行中栈为 P22 迁移前旧构建——P2.2 列尚未存在于 connectors 库；
> B=running/0 files，C=running/1 COMMITTED file sha=7e69fbf3… 与归档
> 一致；P1 旧 artifact/Build/Job 三域 200 可读）。

## 0. rc 捕获纪律（P1/P21 run.sh 管道可能吞上游 rc）

任何 `cmd | tee log` 一律用 bash 并取 **PIPESTATUS[0]**，绝不把 tee 的 0
当测试 0：

```bash
bash -c 'set -o pipefail; <cmd> 2>&1 | tee <log>; rc=${PIPESTATUS[0]}; echo "RC=$rc" | tee -a <log>; exit $rc'
```

或最稳（无管道）：`<cmd> > <log> 2>&1; rc=$?`，审原始日志本身。所有步骤
把 `RC=<n>` 追加进该步日志尾部并写入步骤 manifest。

## 1. 证据布局（每步独立目录，suffix/JSON 分开，不覆盖）

```
results/2026-11-14-p22-final/
  00-baseline/         # 复制只读基线 manifest + expected-counter-reset.json + 新 boot 初始快照（不重跑旧采集）
  01-unit/             # unit-tests.log + rc + 实际 ok 包数统计
  02-pg50/             # migrate-tests.log + rc + 三个 guard 输出
  03-p1-pre9/          # P1 九场景 result.json + log + rc
  04-aux-up/           # p22_connector2.sh up 输出 + rc
  05-contract-probe/   # probe 输出 + rc
  06-p22-smoke/        # result_p22.json + stdout log + rc（D..N）
  07-p21-abc/          # P21 A/B/C result_p21.json + log + rc
  08-restart/          # 三服务 restart（保留 fixture）+ 旧对象再读 + P1 post9
  99-aux-down/         # p22_connector2.sh down 输出 + rc
  manifest.json        # 全步骤 {cmd, rc, artifacts} 汇总
```

## 2. 前置条件（缺一即停，不得代启/代改）

- root 明确 formal release（文字记录入 manifest）。
- **新锁二进制全量替换**：迁移串行保护（整批同 Tx + PG16 事务级 advisory
  锁 0x4F46434F4E4E4D47）落地后，**三 Connector 副本必须全部以新构建
  替换后才允许任何并发启动/重启**（I/M 场景即并发重启执行者）——严禁
  新旧混跑（attempt1 已实证旧无锁 replay 互删表，见
  attempt1-diagnostics/）。
- **fixture 不再重启**：唯一一次 restart 已完成（新 boot 计数=正式
  counter 基线）；后续任何 build/up 均不得触碰 fixture。
- `export P22_CONFIRM_CONNECTOR2=yes`；`OF_P1_TMP=/private/tmp/openfoundry-platform-p1-q3v9`。
- 只读预检（复用 `readonly_baseline.py`；期望**P2.2 迁移后镜像**：
  projection variant 应变为 `p22-full-13col`——否则说明栈未重建，停止
  并上报，不自行 up）。
- **fixture 一次性 restart（root 授权的预期动作，仅此一次）**：fixture
  是 `python:3.12` bind-mount 脚本，当前进程仍是旧版——正式 release
  重建栈后执行
  `docker compose -f $OF_P1_TMP/compose.yaml --project-name openfoundry-platform-p1-q3v9 restart fixture`
  加载 barrier 扩展。**内存计数将清零**：
  a. 先写 `00-baseline/expected-counter-reset.json`：引用升级前基线
     （`…T021922-readonly-baseline`）中的 fixture 计数为**历史值**，
     显式声明"跨 upgrade 不比较、预期归零、新 boot 起算"；
  b. restart 后取**新 boot 初始快照**（含 inventory/active 新字段）；
     此快照才是 I/M 与三服务 restart 阶段 counter 比较的**唯一基线**；
  c. **此后 fixture 不再重启**（08 步只 restart dataset/pipeline/
     connector 三服务，保持不变）。
- 旧 DB 行/file/txn/Build/Job/artifact 为**持久对象**：升级前后仍按
  **同一句柄**逐项比较（B/C 投影、C 产物 sha、P1 旧 artifact sha、三域
  Build/Job GET），与 counter 复位无关。
- P21 历史 B/C 行与目标/产物：正式全程**不碰**（不取消/不改写/不重跑）。
- 主修复已生效并经只读核实（handlers.go drainTriggerBody）：触发体
  drain-to-EOF（empty/`{}`）→ TCP 断连即时取消 r.Context()（K 的 ≤15s
  断言即对应此路径）；体 >1MiB → **413**、读超 5s → **408**、其它读错
  → 400，全部在 Reserve/Enqueue 之前**零持久行**；队列满/停机 admission
  仍为 **503** 零新行（F 常量不变）。

## 3. 步骤与命令（均在 repo 根；每步按 §0 捕获真实 rc）

1. **unit 实际包数**：`bash -c '… tools/smoke/business-platform-p1/run.sh tests …'`
   → 统计原始 ok 行数与 FAIL 行数入 manifest；**按实际计数记录**
   （Connector 可能 38 / 统一 61；不硬断言 60）。
2. **PG 52**（26+14+12；P22 名单含 2 个 migration-concurrency 新顶层，第一候选 50 raw 原样保留）：`run.sh migrate-tests`（同一份 migrate-tests.log，随后
   **三个 guard 显式分别调用**）：
   - P1 26：runner 内部已强制（run_tests.sh integration 自带，冻结不动）；
   - P21 14：`sh tools/smoke/business-platform-p2-1/run_tests_p21.sh integration <log>`；
   - P22 10：`sh tools/smoke/business-platform-p2-2/run_tests_p22.sh integration <log>`；
   三者 rc 全 0 才算 52/52；任一非 0 该步 FAIL。
3. **P1 pre-9**：`run.sh smoke` → result.json 记 pass=9/fail=0/uncovered=0
   （按实际值记录断言）。
4. **aux up**：`sh tools/smoke/business-platform-p2-2/p22_connector2.sh up`
   （白名单三服务 --no-deps；自动先只读核 base 健康；失败即停）。
5. **contract-probe**：`python3 -I tools/smoke/business-platform-p2-2/smoke_p22.py
   --env-dir $OF_P1_TMP --contract-probe`。
6. **P22 D..N**：`python3 -I …/smoke_p22.py --env-dir $OF_P1_TMP`
   （默认顺序 D→N；**默认 queue=64/worker=1/TTL=300 全默认跑**；J 自带
   up-deadline 7s/restore 且强验证恢复；K 依赖的 `{}` 取消传播主修复
   已落则过、未落则如实红，不放宽）。result_p22.json 必 `official:true`。
7. **P21 A/B/C 回归**：`python3 -I …/business-platform-p2-1/smoke_p21.py
   --env-dir $OF_P1_TMP`（脚本冻结不改；A/B/C 各一次全 PASS）。
8. **restart 阶段（保留 fixture）**：
   a. `docker compose -f $OF_P1_TMP/compose.yaml --project-name
      openfoundry-platform-p1-q3v9 restart dataset pipeline connector`
      （**只这三服务；fixture 绝不重启**——计数是证据）。
   b. 等健康后：**旧对象再读**——用只读基线同法重取：P21 B/C 投影+目标
      files/txn、P1 旧 artifact GET200+sha256 比对基线、三域 Build/Job
      GET200（可复用 `readonly_baseline.py` 输出做前后 diff；差异即 FAIL）。
   c. **P1 post-9**：再跑 `run.sh smoke`，与 pre-9 同标准。
9. **aux down**：`sh …/p22_connector2.sh down`（仅三 aux，无 -v）。
10. 汇总 manifest.json：每步 {cmd, rc, 关键产物路径}；任何 rc≠0 如实
    分层记录（不重构日志、不贴错标签）。

## 4. 加速配置（仅提议，未授权不执行）

默认按 queue=64 全量跑（F 填充 ~65 次准入 + 240s 收敛上限）。若 root
要求加速：可给**三副本同值**设 `CONNECTOR_SYNC_ASYNC_QUEUE=8`（override
env，仅 aux+base 同改需 root 批准——**不改 base 需授权**；方案仅此备案）。

## 5. 红线（全程）

- 不 stop/restart fixture；不 clear/reset 卷；不 commit/stash；
- P21 历史 B/C 不取消不改写；P1/P21 脚本与归档冻结；
- .env/JWT/lease token 不入任何日志/JSON（scrub+私有键走查在跑）；
- 旧对象只读再读，绝不重建冒充；失败如实记。
