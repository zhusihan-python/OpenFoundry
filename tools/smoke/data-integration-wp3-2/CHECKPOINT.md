# WP3.2 worker-B checkpoint（producer/infra/验收）

更新：2026-11-17（B worker，round 4 收口）。作用：向 root/Codex 同步 B 侧合同事实、
版本锁定、DDL、实现状态与**分层**的验收结果。**基础链=已实测；V1–V8 全量验收
≠完成（消费方侧等 A final）。**

## 0. 当前状态速览（实测口径，round 4 收口）

| 项 | 状态 | 证据 |
| --- | --- | --- |
| TickOutbox/quarantine/registry/CDC-gate 单测+PG16 集成 | ✅ 连续两次全量 `go test` ok（R1=0 R2=0，20 tests；go1.25.14 linux/arm64 容器，GOTOOLCHAIN=local，未动 go.mod；PG 16.15 wal_level=logical 一次性容器） | 本会话命令记录；结果文件不入库 |
| 负控（wrapper 模型） | ✅ **6/6**（C1/C2/C3 定点中止+C3b 真失败通过/C4 **完整正例先过+仅改 event_id 定点拒**） | `bash negative_controls.sh` pass=6 fail=0 |
| 4 项排队 review 修复 | ✅ ①pipefail ②完整机会 tuple（含 scheduled_for）③source pre/post 严格 64hex+**完整必需路径集合**（26 项：scheduler 实现/CLI/Dockerfile/迁移/schema/runner/verify/status_check/全部 k8s fixtures/go.mod/go.sum；fixture 从 verifier 导入集合防漂移；C5“删1关键输入”负控定点拒）④node inspect+crictl json+每 completed Job 各自 imageID 链 | 本轮 diff；负控 **10/10** |
| 隔离基础链 | ❌ **最后一轮未通过（如实），且此前的“task 启动 >600s”归因已被 root 复核撤回**：真因是 61-connector.sh 用 `grep -c` 数**行数**——status 为单行紧凑 JSON，running/total 恒为 1，全任务门槛永假；raw 中重复 1/1 即此 bug，不能证明 task 未出现（07:26/08:0x 两次手工 REST 实查 task 0 均 RUNNING）。已重做：删 61-connector.sh，改 `kubectl exec -i`（stdin 转发为已验证原语）PUT config + 宿主 `status_check.py` 严格解析原始 body（connector.state==RUNNING 且非空 tasks 全 RUNNING），原始 status 存证；C6 负控三例（单行双 RUNNING 正例/task FAILED/空 tasks）全过，不再延长等待窗。此前步骤全过：pull/load、staging 构建、PG+迁移、Kafka(localhost quorum)、Connect/Apicurio、schema 注册（RETURN_OR_UPDATE+版本核对+字节指纹）。 | results/20261117T074626_25228/ 及各轮 raw（全保留）；ofwp32-acc Server v1.36.4，独立 kubeconfig |
| V1–V8 全量验收 | ❌ 未跑不宣称；当前链即使通过也**只证生产者/CDC 层**，不得称全 WP3.2/V1–V8（A 执行后端就绪后联跑） | — |

### 环境事实（round 3–4 排障实录，raw 全保留）
- kind node 内存阶梯 5g→6g→**6.5g 封顶**（VM 7.748g/旧栈 ~0.9g，root 指令不再上调）；kind 默认 coredns 170Mi 在 JVM 负载下双 OOMKill CrashLoop → runner 内置 stabilize（coredns 512Mi，仅隔离集群）；无 requests 的 Burstable pod 被 oom-killer 首选击杀（137/OOMKilled 实证）→ 三个 curl 型 pod 加 requests/limits。
- Kafka KRaft 单节点两连坑（已修+注释入清单）：Service 未暴露 9093→quorum 不通；补 9093 仍死锁（未 Ready pod 不进 legacy Endpoints）→ **QUORUM_VOTERS=1@localhost:9093**。
- Apicurio 2.6.13.Final v2 活体纠错：`ifExists` 枚举为**大写** FAIL|UPDATE|RETURN|RETURN_OR_UPDATE（小写 update→404 "No enum constant"原始报文已录）；注册 gate 加 sysinfo 版本核对。
- 仓库历史归档 8.1G（tools/smoke/business-platform-p2-6）在慢外置卷使全仓 Docker context 超时（4.18GB@541s）→ tick 构建改最小 staging 上下文；不动全局 .dockerignore。
- BuildKit `# syntax=` 前端经 auth.docker.io 超时（同错两次即停）→ 移除 syntax 行用内建前端。
- kubectl 1.36.1 `run` 实核：`-i/--stdin/--attach` 存在，但无 -i 的本地重定向不进容器、created≠远端 rc → connector 注册改 CM 挂载脚本+terminated.exitCode 断言。
- bash `[ A ]||[ B ]&&break` 两判皆假时 rc=1 被 set -e 杀循环（改显式 if）。
- Dockerfile TARGETOS/TARGETARCH 无值重声明（BuildKit 自动 platform ARG），runner 显式传 linux/arm64，不硬默认。

## 1. 合同事实（已冻结）

- 合同：`docs/architecture/data-integration-t8-scheduling-decision.md` v2.1；
  M1 `libs/schedule-events` 公开 API 冻结；A 独占 `services/pipeline-build-service/**`
  含全部迁移（`20261117100000_scheduling_consumer.sql`，dispatch_audit 实际含
  `tenant_kind` 列——以迁移 DDL 为准，合同 §3 列清单漏写）。
- M1 outcome schema 2026-11-17 12:40 修订（schedule_key pattern 对齐 trigger），
  **已重算指纹**（drift-guard 单测锁定文件字节）：
  - trigger `pipeline.schedule.triggered.v1.json` = `cd18c6212d2d9d35b0c092669daee58049c36e15d8a3174ca173e95e63eea92f`
  - outcome  `pipeline.schedule.outcome.v1.json`  = `aef7a88509a097fe41c8ec50d484ca75d75910c2bc1881c3104138a56fc06a12`
- 事实更正（root 2026-11-17）：`pg_create_logical_replication_slot` 是自 PG9.4
  官方本名；此前"backpatch 改名"说法错误，已从代码/测试移除 fallback。
  两本地 PG 镜像（postgres:16 / 16-alpine）实测均为 **16.15**（`postgres -V`）。

## 2. root 已裁定的 quarantine DDL（A 并入同一 service 迁移；B 未改 service 迁移）

```sql
ALTER TABLE schedules.definitions
    ADD COLUMN IF NOT EXISTS quarantined_at    TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT        NULL;
```

语义（已实现）：永久坏定义→**同一认领事务** `enabled=false`+两列脱敏原因
（截 512、去换行、绝不含 token——producer 从不 SELECT token_ciphertext），继续
其它行；Registry 瞬时故障（transport/非200/404未注册/鉴权/指纹不匹配）→不
quarantine、认领前中止、全部保留 due、返回可重试错误；恢复由 A 的定义 API 清列。
claim 过滤：`dispatch_mode='pipeline_outbox_v1' AND enabled AND next_run_at<=$1
AND quarantined_at IS NULL`。

## 3. B 实现清单（libs/event-scheduler/**，全部已过测试）

- `TickOutbox`：单事务 claim(FOR UPDATE OF d SKIP LOCKED)→dispatch_audit
  (ON CONFLICT DO NOTHING)→outbox.Enqueue(8字段 M1 envelope)→advance→COMMIT。
- CDC gate（`WithCDCSlot`/`SCHEDULES_CDC_SLOT`，CLI opt-in、生产 CronJob 显式设置）：
  pg_replication_slots **cluster 级**——同时核 `database=current_database()`、
  `slot_type='logical'`、`plugin='pgoutput'`；注释区分两态：slot 从未建（此前
  INSERT+DELETE 不可恢复，硬顺序要求）vs 已建未激活（**WAL 保留可续传**，保守等待
  而非数据丢失）。nil verifier → fail-closed ErrRegistryUnreachable（公开 API
  TickOutbox(nil,nil) 有测试，无 panic）。
- 旧 `Tick`：claim 加 `dispatch_mode IS DISTINCT FROM 'pipeline_outbox_v1'`
  （双发排除）；缺列→`ErrSchemaMissing` 明确报错（不静默、不破坏已迁移库直发模式）。
- CLI：默认 OFF；`SCHEDULES_DISPATCH_MODE=pipeline_outbox_v1` 时缺
  `SCHEDULES_REGISTRY_URL`→exit 1（fail-closed）；不要求 Kafka env。
- lib 迁移 `0002`：CHECK 已对齐 A 修正后的形式（`tenant_kind IS NOT NULL AND
  IN(...)`；org grant `org_id IS NOT NULL AND org_id=tenant_id`）+
  `ALTER TABLE outbox.events REPLICA IDENTITY FULL`（全仓 canonical：compose×2、
  pg-policy bootstrap-sql:96、libs/outbox 文档；root 已让 A 在 service 迁移补）。
- 单测 20 个：指纹 drift-guard、nil-verifier fail-closed（公开API级）、
  registry 三态、v2 wire path（httptest）、quarantine 分类、
  PG 集成（fire/隔离/legacy排除/同槽重放同event_id且audit单行/CDC gate 三分支
  含他库同名 slot 拒绝/缺迁移 fail-closed）。

## 4. infra 交付（B 所有，均默认 OFF）

| 文件 | 说明 |
| --- | --- |
| `infra/helm/infra/debezium/templates/kafka-connector-outbox-pipeline.yaml` + values | **pipeline 源库**（pg-schemas-rw，dbname=app）connector；`transforms.outbox.table.expand.json.payload=true`+JsonConverter schemas.enable=false（root 已核 2.7 官方，jsonb 不展开=escaped string 会被拒）；route=topic列/key=aggregate_id/id=event_id；headers:header=**单个 JSON map header（非 flatten ol-\*）**；snapshot.mode=never；slot `debezium_outbox_pipeline`；双 gate：values 默认 off + 落地 paused |
| `infra/helm/apps/of-data-engine/templates/schedules-tick-cronjob.yaml` + values | CronJob `*/1` Forbid；**显式 SCHEDULES_CDC_SLOT**（与 connector slot 同值约定写死在两边 values 注释）；默认 off |
| `infra/helm/infra/kafka-cluster/values.yaml` | +`pipeline.schedule.outcomes.v1` + `__dlq.pipeline.schedule.outcomes.v1` |
| `.../kafka-cluster/templates/apicurio-schemas-schedule-events-job.yaml` | 独立 v2 注册 job（不动旧 v3 job）；CM 块标量 `|`（**保留尾 LF**，`|-` 会剥 7d0a 导致指纹差一字节——root round3 已修）；GET→临时文件再 hash（emptyDir 挂 /tmp 兼容只读 rootfs；管道会吞 curl rc）；注册后内容==pin SHA 否则 Job fail |
| `.../kafka-cluster/values-apicurio-2-6.yaml` | 2.6 分流 profile：`registerSchemas=false`（旧 v3 hook 对 2.6 必失败）+ `scheduleEvents.enabled=true` |
| `.../kafka-cluster/files/schemas/*.json` | M1 schema 字节副本（cp，sha 与 pin 一致）；runner 每轮 diff 源文件 |
| `libs/event-scheduler/Dockerfile` | distroless 多阶段（同 pbs 范式，-p 2） |

**所有权变更（2026-11-17）**：infra/** 已归 worker C 并冻结（root 复核通过）。
B 此后只写 libs/event-scheduler/** 与 tools/smoke/data-integration-wp3-2/**（隔离
fixture 用 vanilla Debezium，与 C 的生产模板互相独立）。**移交 C 的已证实 bug**：
infra/helm/infra/kafka-cluster/templates/apicurio-schemas-schedule-events-job.yaml 的
`ifExists=update` 在 Apicurio 2.6.13.Final v2 上 404（须大写 RETURN_OR_UPDATE；
活体报文见 §0）——B 无权改，请 root 转 C；B 的隔离 fixture（50-register.sh）已用
正确值。helm render 后 CM 字节提取比对仍待（本机无 helm CLI；runner 以 kubectl
--from-file 字节精确路径+挂载后实测 hash 等价覆盖隔离链）。

## 5. 版本锁定（全部实际命令验证，原始输出在会话记录）

| 组件 | 版本 | 验证方式 |
| --- | --- | --- |
| kind | v0.33.0（darwin/arm64，官方 tag 源码容器内 `go build sigs.k8s.io/kind@v0.33.0`） | `kind version 0.33.0` |
| K8s（隔离集群 ofwp32-acc） | **Server v1.36.4**（kindest/node digest 锁定） | `kubectl version`（kubeconfig 隔离） |
| PostgreSQL | postgres:16-alpine（**实际 16.15**） | `postgres -V` |
| Kafka | apache/kafka:3.7.1（KRaft 单节点，env=compose 已验证形态） | `docker pull` exit 0 + 已入 kind |
| Debezium Connect | quay.io/debezium/connect:2.7.2.Final（2.7.0.Final 亦实存） | `docker manifest inspect` OK |
| Schema Registry | apicurio/apicurio-registry-mem:2.6.13.Final + **v2 API**（-mem/-sql 在 hub/quay 均无 3.x tag；v2 端点核自 tag 2.6.13.Final 官方 OpenAPI：getLatestArtifact=GET /apis/registry/v2/groups/{g}/artifacts/{id}） | hub+quay tag API + spec 原文 |
| Go 工具链 | 容器 go1.25.14 linux/arm64（repo 声明 go 1.25.0；GOTOOLCHAIN=local 不升级） | `go version` |

网络通道原始错误（已录未伪造）：宿主 release-assets.githubusercontent.com:443
拒连（curl 7）；宿主 hub.docker.com:443 不通（curl 28）→ manifest 假阴性，以
daemon pull 为准；raw.githubusercontent 连接重置（curl 35，两次即停）→ 改
api.github.com contents API。

## 6. runner（tools/smoke/data-integration-wp3-2/）

- `runner_lib.sh`：**persist-then-assert** 模型——无条件先落 `step = cmd_rc:X
  tee_rc:Y` 两个 raw 状态再断言；run_step 要求双 0；expect_fail_step 要求
  cmd≠0 且 tee=0；fail_run 强制非零退出（C2 bug 已修）。
- `run.sh`：phase0 预检/预算/拉镜像/录实际版本 → phase1 构建 tick 镜像 →
  phase2 PG(wal logical)+迁移+Kafka(4 topics)+Connect+Apicurio+**字节级注册核验**
  （挂载 bytes==pin==注册内容）+connector PUT+RUNNING 轮询+slot active 实测 →
  phase3 seed(稳定UUID)→**≥2 个 seed 后自动 CronJob 周期**（creationTimestamp
  过滤，空 tick 不计）→Kafka/DB/tick日志采集+CDC gate 负控（错slot必须
  fail-closed）→ phase4 严格校验+负控。
- `verify_results.py`：basechain 模式严格校验（envelope 8字段类型、event_id
  Python 侧 UUIDv5 重算防伪造、aggregate 域格式、audit⊆kafka 覆盖、
  badcron 隔离断言、legacy 不发、outbox 空稳态、DLQ 空、≥2 seed后自动周期、
  gate 负控文本）；exitcodes 模式双 rc 全零；expect_reject 供负控。
- `negative_controls.sh`：真实 source 同一 wrapper（C1–C4b 见 §0）。

## 7. 分层声明（防过度宣称）

- **已可宣称（若 runner 通过）**：producer 侧基础链在真实 K8s v1.36.4 上闭环——
  真实 CronJob 自动周期→tick（registry 指纹+CDC slot 双 gate）→事务①→Debezium
  CDC→Kafka envelope（event_id 可复算）；坏定义同事务隔离；直发行不受影响。
- **不可宣称**：V1–V8（需 A 消费方/builds/datasets 产物/JWT租户边界/撤权重放/
  重启恢复/DLQ 消费/崩溃窗口语义——A final checkpoint 后联跑）。
- Kafka wire 形态记录：key converter 为 JsonConverter 时 aggregate_id 编码为
  quoted JSON string key（分区稳定；M1 不读 key，不换 converter）。

## 8. B 不做/不做完清单

- 不改 `services/**`、`libs/schedule-events/**`、go.mod/go.sum、旧
  apicurio-schemas-job（v3）、旧 topics、用户旧容器/卷/kube context、真实 .env。
- 不部署生产；新调度默认 OFF（values 双 gate）。
- 本包不 commit/push。

## 9. Sol 接管复验（2026-11-17，本节覆盖历史归因）

- 最新旧 run `20261117T081213_18876` 的 connector/CDC slot/两个自动周期均实际
  成功，失败在 collect-evidence SQL 的 `[]` 引号转义；原失败 raw 保留不改。
  已改为 `json_build_array()`，对真实隔离 PG 只读执行 rc0。
- connector 解析采用宿主 `status_check.py` 的结构化 JSON，单行多个 state 不再按
  行数统计。独立负控 10/10 rc0：`/private/tmp/ofwp32-runtime-sol-evidence/standalone-negative*`。
- source verifier 必需集合补齐 source_sha_list 已承诺的生产 infra/schema 路径；
  镜像缓存先独立 inspect 并保存 raw，未缓存才真实 pull，不再把 pull 失败覆盖为成功。
- C Apicurio 生产 registrar 的 `RETURN_OR_UPDATE` 活体首次/重复注册、两 SHA、
  版本数均验证通过，小写负控 404；详 deployment-profile §8.1。C artifacts 恢复冻结。
- shared events helper 冻结后清空**仅本轮隔离 ofwp32 namespace**，重新完整运行
  `20261117T083422_32202`，stdout/最终退出码放
  `/private/tmp/ofwp32-runtime-sol-evidence/fresh-run.*`。当前到达自动周期等待，
  尚不宣称基础链 PASS，更不宣称 V1–V8。

### 9.1 首个完整 producer basechain PASS（等待 root review）

- **run** `20261117T084515_1177`，整体原始退出码 **0**；完整stdout/stderr与rc：
  `/private/tmp/ofwp32-runtime-sol-evidence/fresh-run-r3.*`，逐步证据在本目录
  `results/20261117T084515_1177/`。全部 cmd_rc/tee_rc 为0，末项
  `exit-codes-all-zero` 也为0。source pre/post 一致，关键输入完整。
- 两个 seed 后自动成功 CronJob：`29915087`、`29915088`，各自Pod均Succeeded；
  Kafka触发2条、审计完整机会tuple对应、outbox清空、DLQ显式offset为0；
  wrong-slot负控真实进程exit1且日志包含slot与cdc not ready。负控10/10。
- 同类镜像身份：Docker29 `.Id` 为OCI index，不能直接与CRI config digest比较。
  runner保存 `docker save` tar、manifest、docker inspect，计算实际config bytes SHA256，
  加载**同一tar**；node CRI `images.id` 必须匹配该config；每个成功Pod的manifest
  imageID必须映射到同条CRI镜像记录的repoDigests，进而匹配该config。
  本轮configSHA：`e0f86233a20077dbbf86bfc2cb726f5204a86f7fce3b716915cd5613941ccc5f`。
- 已修两处后续边界：`kubectl run --requests/--limits`已移除，改受支持JSON patch并
  client render验证资源；`job.batch/`名称前缀完整剥离，逐Pod采集不再为空。
- **失败证据不改**：`083422_32202`因kubectl废弃flag失败；`083955_13084`因Sol在
  运行时修改脚本导致shell读取偏移语法失败。后者明确为执行失误，未算PASS。
- 本节只证明producer基础链，**不证明V1–V8**。消费方真实HTTP/PG/Kafka/JWT合流仍待。

### 9.2 Canonical v3 合流进展（V8 尚待完整结果）

本轮证据根：`/private/tmp/ofwp32-runtime-sol-evidence/`。真实服务 manifest 与
可执行 case helpers 已落本目录，启动/执行说明见 `CANONICAL_RUNTIME.md`。

- **v3 身份**：实际Pod manifest→CRI config→docker-save layer `/service` SHA
  `c384b635c151a9cc130b85a41d39f18cd65a6e8af219c5c74f518fc4aed3d4a9`，
  证据 `e2e-v3/running-v3-provenance.json`；168个PBS/M1源码、迁移/config与
  pre指纹不变。DVS HEAD/binarySHA及未改源码事实另录。测试harness在诊断过程中
  有修正，失败raw与更正后独立case记录保留；不声称整个开发阶段harness从未变化。
- **自动周期/重启产品**：`counted-v3-cycles-r2.rc=0`，09:02/09:03/09:04 UTC
  全为v3，其中09:04为真实PBS重启后。各event→Build→receipt→DVS COMMITTED
  txn→精确version/view→历史文件HTTP bytes，均2行A2/B5、40B，SHA与receipt及
  request `content_base64`匹配。09:01 v2仅作诊断，不计正式v3周期。
- **重复/并发**：`duplicate-complete.rc=0`，两并发真实Kafka producer重放，消费
  lag0，PG副作用计数与真实DVS版本完全不变。`forbid-r3.rc=0`，首run活租约/
  activity+真实rows hold期间另一scheduled_for blocked，release后首run仅产1版本。
- **撤权**：`revocation-r3.rc=0`，org/personal旧ref与当前ref旧generation均
  skipped且无Build/新版本；同UUID跨kind HTTP404。`running-revocation-r3.rc=0`，
  org/personal在真实rows hold后撤权，release后上游rows正常200，授权gate拒绝
  发布，产品不变；未将proxy超时或重启当作授权拒绝。
- **失败保旧**：`single-failure-r2.rc=0`，真实scheduled failed，不改旧版本/rows。
- **崩溃窗口**：`admission-crash-{before,after}-result.json`与其run.log由core记录
  两实际Pod crash rc0；可见准入前0/0、准入后1/1 Build/dispatch及等待锁证据，
  新Pod同event最终唯一Build/receipt、真实产品+1，测试trigger/holder清理为0。
- **丢ACK跨重启**：`lost-ack-r3.rc=0`，真实DVS COMMITTED后丢ACK并挂起重放，
  实际PBS停止/Pod删除→pending原bytes+remote commit证据→新PBS同key/原bytes/
  原transaction恢复，无额外版本。r2的7ms同run立即恢复未算跨restart通过。
- **并发tick/漏期**：`tick-coalesce-r3.rc=0`。首真实Job持定义行锁/audit barrier，
  第二真实Job完成JSON `fired:0`；释放后首Job `fired:1`。三小时旧due与唯一audit
  scheduled_for精确一致，真实产品仅+1，nextdue未来。finally清testtrigger/holder
  并API暂停该分钟定义。坏定义隔离/健康定义继续由已接受producer run覆盖。
- **尚待**：V8 poison budget+DLQ事务失败/ACK故障跨重启完整case，由core独占本轮
  PBS/Kafka故障时隙执行。Kafka没有PVC，只允许精准STOP/CONT，不删其Pod。

### 9.3 V8 完整通过（覆盖9.2的尚待项，等待root终审）

- `poison-tx-failure-r2-result.json`：实际poison offset91，持久attempts=5，
  DLQ事务失败后真实PBS crash/restart，唯一DLQ event
  `7578ae9c-3db9-5dc2-a41a-c2b531e052a2`，同分区健康机会成功产出产品。
- `poison-pending-ack-r6-result.json`：实际poison offset114，持久attempts=5；
  Kafka进程STOP+其Pod网络命名空间内仅旧PBSIP:9092的DROP（barrier释放前安装），
  DB DLQ提交后真实PBS crash，再CONT broker；重启前groupoffset114/end115
  证明未ACK。新PBS不同IP重读只ACK，attempt不增，唯一真实DLQ event
  `e6139ddf-0827-5d7a-a084-24fdc0698458`，健康事件唯一Build/真实产品+1，再重启
  不增加DLQ。规则保留到新PBS健康恢复后精确删除。
- 两最终case rc0，所有较早失败raw按独立runid保留。精确socket销毁被内核禁用的
  尝试没有算通过；仅暂停后排队commit最终成功的尝试也未算ACK失败恢复通过。
- `poison-final-cleanup.json`及只读post：测试trigger/function/sequence/holder均0；
  唯一网络规则删除，Kafka保持原PodUID、restart0、进程RUNNING，PBS1。
  未删除/scale Kafka Pod（其无PVC），未改全局Docker或node网络规则。
- 因此worker侧V1–V8各边界已取得真实case通过记录，producer基础链此前已由
  root接受；整体最终结论以root复核及acceptance文档为准。生产未部署、未commit/push。

### 9.4 清理准备（尚未执行删除）

已只读列出本轮专属容器/镜像、创建时间/挂载、port-forward、buildctx及Go cache。
本轮results约57MiB；11份buildctx共5423文件/40,827,356字节。旧138k结果/P2.6
归档未扫描或修改。清单在evidence根的`cleanup-*`，删除等待root明确启动。
应先验证归档源副本、raw成功/失败记录、数据库/PVC文件和镜像/binary身份，再回收
本轮展开staging/临时资源；不得project级compose down（test容器复用旧P1 labels），
不得global image/volume/buildcache prune，旧基础镜像与旧栈保留。

### 9.5 最终归档及授权清理完成（2026-11-17 10:15:32 UTC）

root已接受producer及V1–V8全部工程/真实隔离验收PASS，并明确启动收尾清理。
长期证据位于本项目ignored目录
`results/acceptance-20261117/`：

- `evidence.tar.gz`：R/A2/D/C全部成功/失败raw，3份host PG逻辑dump、2份kind FC
  dump（pg_restore目录/完整内容解析均成功）、3份PVC历史产品/原proxy请求bytes。
- `source-snapshot.tar.gz`：244项受限源码/配置/本轮文档（含168 PBS/M1输入及
  新增未tracked文件），不含.env/.git/cache/旧results。
- `buildctx.tar.gz`：5423原始member、40,827,356字节；`reference-evidence.tar.gz`
  保存C官方材料及注册临时原始bytes。每包member数量/逐文件SHA全部验证，两次
  验证后才删除展开staging；各包SHA与内部路径映射见`manifest.json`和members索引。
- `cleanup.json`精确记录删除对象、创建归属、命令/rc、回收量、protected前后对照。
  已删除本轮kind/ns、5个host专属容器、4个独占匿名卷、8个专属镜像tag，以及
  11份重复buildctx/私有缓存/工具/staging；没有global prune/project compose down。
- 保留全部raw原日志/JSON/正式binary；旧20容器、73镜像引用、118卷逐项未变。
  删除文件逻辑体积561,520,702字节和专属容器可写层2,002,513,920字节；未将
  未测量kind卷/共享镜像层夸报为回收量。旧138k/P2.6历史与.env未遍历/删除。
- 最终168项生产输入仍与pre一致，见`post-clean-source-verification.json`；本节为
  收尾文档追加，未改变生产代码。最终索引见`FINAL-SHA256SUMS`。

没有生产部署、commit或push。
