# P2.6 W1b — Dataset COMMIT ACK 丢失 wire 证明接口（候选冻结版）

状态：W1b 新增测试源码交付。依据
`handoffs/2026-11-15-p26/wire-commit-ackloss-tests.md`（W1b ownership）与
`root-decisions-v2.md`。**本阶段只做了离线 cached Go 编译/vet/纯 mechanics
测试；真实 PG 运行未执行，等待 Root 审核后由 W6 统一执行。**

## 1. Ownership（W1b 独占新增，其余文件零改动）

| 文件 | 说明 |
|---|---|
| `services/dataset-versioning-service/internal/repo/output_commit_ackloss_integration_test.go` | `//go:build integration`，必需名 `TestPGP26DatasetWireCommitAckLossReplaysReceipt`（W6 才真跑） |
| `services/dataset-versioning-service/internal/repo/output_commit_ackloss_wire_test.go` | 无 build tag 的私有 pure helper（frame parser + net.Conn wrapper）与 `TestW1b*` 纯 mechanics 测试 |
| `tools/smoke/business-platform-p2-6/W1b-ACK-INTERFACE.md` | 本文件 |
| `tools/smoke/business-platform-p2-6/results/w1b/` | 离线验证原始 cmd/rc/stdout/stderr 存档 |

复用（只读，不改动）：`bootPool`/`w1Setup`/`w1Build`/`w1BuildOpts`/`w1Result`/
`w1RecvT`/`w1Count`/`w1SnapshotPublished`/`w1SecondPool`/`keyedFixtureBody`/
`keyedFingerprint`（W1 冻结 fixture）。production、W1 既有测试、其他 worker
文件、W2 侧 `recovery_commit_ackloss_integration_test.go` 均未触碰。

## 2. Wire 证明机制（真实 socket ACK 抑制，非 mock Commit 先成功再报错）

对照 handoff「Actual wire proof required」逐条：

1. **fault pool** `pgxpool` `MaxConns=1`、`MinConns=0`，并**保留 ParseConfig
   的 positive 默认** `MaxConnLifetime=1h`/`MaxConnIdleTime=30m`（review
   round 1 阻断 1：pgx v5.7.4 中 0 表示"立即过期"，PID 预热的 QueryRow
   Release 即销毁连接、publish 换新 PID，破坏缓存 PID 证明；positive
   默认下 warm-up Release 后连接留在池内被复用，backend PID 稳定）；
   `DialFunc` 包装 `w1bFaultConn`。迁移/setup/PID 查询
   （`SELECT pg_backend_pid()` 缓存 `faultPID`）全部完成于 fault 事务开始
   之前；`connCount()!=1` 即失败。
2. **独立 LISTEN 连接**：单独 `pgx.Conn`（非 pool），`LISTEN <channel>` 在
   fault 事务开始前完成；PG16 NOTIFY 只在外层事务 commit 后投递，且要求
   LISTEN 先于目标事务。
3. **测试专属 trigger**：`AFTER INSERT ON dataset_output_commit_receipts`，
   `WHEN (NEW.dataset_id='<fixture>'::uuid AND NEW.idempotency_key='<key>')`
   仅限本 fixture 行；`pg_notify('<唯一channel>', '<payloadUUID>:' ||
   pg_backend_pid()::text)`；channel/payload UUID 唯一；标识符经
   `w1bSafeSQLLiteral` 字符集校验后内插；cleanup 有界 `DROP TRIGGER/
   FUNCTION IF EXISTS`。
4. **小型 test-only parser**（`w1bFrameParser`）：按 PG frontend 协议
   type(1)+length(4) 逐帧重组，跨任意 Write 碎片（一 Write 可含多帧/一帧跨
   多 Write），绝不子串匹配；正确穿过 SSLRequest/GSSENCRequest(8B)、
   CancelRequest(16B)、typeless startup 前导。仅当帧类型 `'Q'` 且 payload
   逐字节等于 `commit\0`（pgx v5.7.4 `Tx.Commit` → 无参 `Exec("commit")` →
   simple query 的精确形态）才置位；其余 SQL（含 `READ COMMITTED`、
   `COMMIT` 大写、`commit;`、内含单词 commit 的查询、extended 协议
   Parse 帧）一律放行不触发。缓冲上界 1MiB，超限转纯透传（不再可能触发，
   测试以 commitFrameSeen=false 诚实失败）。**只在完整 COMMIT 帧转发出去
   之前置位抑制位**（服务器必须收到并执行该 COMMIT 请求）。
5. **Read 双重检查**（`w1bFaultConn.Read`）：抑制位激活时，进入底层读之前
   检查一次，底层读返回之后**无条件**再核一次（review round 1 阻断 2：
   net.Conn 允许 n>0 与 err 非 nil（含 io.EOF）同次返回，n 与 err 须一并
   吞掉，绝不向调用方暴露）——命中则扣住全部返回内容并停等；不猜
   rollback、不伪造错误。测试在**独立 LISTEN 连接拿到并校验通知之后**才
   `CloseAll()` 切断 fault socket → 在飞的 `tx.Commit` 读到 EOF →
   `WithIdempotentOutputCommit` 返回原始 `ErrCommitOutcomeUnknown`。
6. **无通知/超时/无 trigger ⇒ FAIL**：`w1bAwaitNotification` 对
   channel/payload(含 fault backend PID)/NotificationResponse 发送方 PID
   三者全匹配才通过；任一不匹配或 20s 超时（含 LISTEN 连接先出错）直接
   `t.Fatalf`，绝不退化为“人为报错也算过”。若 fault 连接中途换了 backend
   PID，payload/PID 校验即失败。
7. **证据位**：`commitFrameSeen`+`suppressActivated`+`EOFDelivered`
   三位全部为真才继续断言。
8. **有界与清理顺序**：所有 wait（commit frame 20s、通知 20s、worker join
   10s、rollback/trigger-drop/listener close 5s）均有界；结果用单值
   `done` channel 传递，worker 结束另以 `close(workerExited)` 广播——
   cleanup（review round 1 阻断 3：cleanup 不得重复消费单值结果，成功路径
   也会 10s 假失败）join 观察的是 closed channel。失败清理顺序：先
   `state.CloseAll()`（切 fault socket）→ `cancelNotif()` + listener 有界
   close（先释放 notifier）→ 有界 join `workerExited` → 之后 t.Cleanup
   LIFO 依次 trigger drop → faultPool.Close → observer pool close。
   不新增端口/依赖/框架；fault pool 与 listener 显式
   `TLSConfig=nil`+`Fallbacks=nil`（sslmode=disable）仅限这两条测试自有
   PG 连接，不动生产 TLS。

**边界声明**：本测试注入的是外层事务真实 COMMIT 的 wire ACK 丢失
（外层 `tx.Commit` 的 'Q' `commit\0` 帧），不是嵌套 SAVEPOINT RELEASE 的
ACK，也不是 DB 接口层"Commit 先成功再返回错误"的应用边界故障——后者由
handoff 明确列为不满足 criterion 的弱形态。

## 3. 必需测试名与验收点（W6 真实 PG 执行）

| 测试 | 验收点 |
|---|---|
| `TestPGP26DatasetWireCommitAckLossReplaysReceipt` | 正常 `WithIdempotentOutputCommit` build 经真实 PG 发布后，wire 上丢失外层 COMMIT 的 ACK：原始调用返回 `ErrCommitOutcomeUnknown`（不伪造 receipt）；observer（普通 pool）看到真实 receipt + COMMITTED txn + 1 版本/head/index/lineage/dataset_files 投影；同 key+fingerprint 重试在独立 pool 上逐字节返回保存的 response bytes，build 不再进入（计数恒 1），版本/receipt 计数仍为 1，投影快照不变 |

W6 建议加入 cohort 的执行命令（与既有 W1 清单同容器同 DSN 前提）：

```
go test -tags integration -count=1 -race -v \
  -run 'PGP26DatasetWireCommitAckLoss' \
  ./services/dataset-versioning-service/internal/repo/
```

## 4. 纯 mechanics 测试（本阶段已执行，无 DB 依赖）

| 测试 | 验证 |
|---|---|
| `TestW1bCommitFrameDetectedAcrossFragmentedWrites` | 逐字节碎片喂入：抑制位恰在完成 COMMIT 帧的那一字节处置位（先于转发），字节零丢失零乱序，尾部未完成帧被扣留 |
| `TestW1bNonExactCommitPayloadsNeverArmSuppress` | 大写 `COMMIT`、`commit;`、`commit `、` commit`、`READ COMMITTED`、内含 commit 词的 SELECT、RELEASE SAVEPOINT、extended 协议 Parse 帧携 `commit\0` 文本——全部不触发；3 字节任意碎片下字节完整转发 |
| `TestW1bReadPreCheckHoldsBackUntilClose` | 抑制位先于 Read 激活：Read 停等不触 socket，Close 后返回 EOF 且 eofDelivered 置位 |
| `TestW1bReadPostCheckSwallowsBytesAfterSuppressArms` | Read 已阻塞在底层读（readEntered barrier 先于置位）、抑制位后激活：底层返回的 ACK 字节被吞掉不暴露，Close 后 EOF |
| `TestW1bReadPostCheckSwallowsBytesWithEOFAfterSuppressArms` | review round 1 反例：底层一次 Read 返回 ACK 字节**和** io.EOF（n>0 且 err 非 nil）——两者一并吞掉，Close 后才是干净 EOF |
| `TestW1bStartupSSLAndCancelFramesPassthrough` | SSLRequest+startup 前导下帧同步不破坏（随后 commit 帧仍精确触发）；CancelRequest 独立连接（pgconn 取消用）16 字节透传不误触 |

## 5. 本阶段（W1b）已执行的离线验证

全部在 cached `golang:1.26.3`（`--pull=never --network=none GOPROXY=off`，
project 只读挂载，仅复用 `of-go-mod`/`of-go-build` 卷）中执行。当前有效
存档为 `results/w1b/2026-11-15-w1b-attempt3/`（review round 1 三项阻断
窄修后的重跑）；`attempt1/` 为首轮原件保留；`attempt2-broken-runner/`
是一次引号折叠导致的无效 run（实际只执行了 docker usage，README 内已
说明，不作任何验证依据）：

| step | 命令要点 | rc |
|---|---|---|
| 01-gofmt | `gofmt -l`（两个新文件） | 0（无输出） |
| 02-vet | `go vet ./services/dataset-versioning-service/internal/repo/` | 0 |
| 03-vet-integration | `go vet -tags integration ./services/dataset-versioning-service/internal/repo/` | 0 |
| 04-unit-w1b-verbose | `go test -count=1 -v -run W1b ./…/internal/repo/` | 0（6/6 PASS） |
| 05-race-w1b | `go test -race -count=1 -run W1b ./…/internal/repo/` | 0 |
| 06-build-service | `go build ./services/dataset-versioning-service/...` | 0 |
| 07-unit-repo-full | `go test -count=1 ./…/internal/repo/`（非 tag 既有纯测回归） | 0 |

未执行：任何真实 PG/HTTP/Docker 资源变更、服务容器、网络下载。
`TestPGP26DatasetWireCommitAckLossReplaysReceipt` 尚未在真实 PG 上运行——
其 PASS 声明只能由 W6 统一执行后作出。

## 6. 交接给 W6 的注意事项

- 需要 `DATASET_DB_DSN` 或 testcontainers fixture 与 W1 cohort 相同；
  trigger/channel/payload 每次运行随机命名，可重复执行。
- 若 W6 真跑中发现 `commitFrameSeen` 超时失败，优先核对 pgx 版本仍为
  v5.7.4 且 `Tx.Commit` 仍走无参 simple query（`commit\0` 精确匹配是硬
  契约，不做大小写/空白容错——检测不到就诚实失败）。
- 生产代码零改动；本测试若暴露生产行为问题，只报告不改。
