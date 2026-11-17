# WP3.2 生产配置/部署 Profile（worker C 交付）— 2026-11-17

- **编制**：GLM-5.3（WP3.2 worker C）。范围：收口 4 组 review 实缺——①同源 DSN、②PG CDC
  准备、③connector Secret/ConfigProvider、④Kafka topics/ACL——以及本文件。所有权：
  `infra/**` 本轮 production 模板/values/准备 SQL + 本文档；不触碰 A/B 范围、go.mod、真实
  `.env`、P0–P3.1 历史正文。
- **基线**：branch `codex/dataset-pipeline-file-io` @ `f4c6194c` 工作树（未提交，本任务不
  commit/push）。上游授权：用户 2026-11-17 以 ADR-0037 CronJob→Kafka 为基线补齐接线。
- **三态声明**（防过度宣称）：
  1. **工程交付完成** = 本文 + 4 组修复 + 验证证据（§8），全部命令可复核。
  2. **不等于部署**：本轮**未向任何真实集群 apply**（含 B 的 ofwp32-acc kind——按分工 C 只做
     client render/parse；CRD 缺失处未做 server 端校验）。
  3. **不等于生产可用**：旧生产拓扑的限制如实登记（§2.4），不能宣称旧 production 拓扑已运行
     支持本链路。

## 1. 拓扑事实（同源契约的依据）

| 事实 | 证据 |
| --- | --- |
| pipeline 库 = BC `pipeline_authoring`，落在 **pg-runtime-config 集群 / database `app`**（search_path=pipeline_authoring） | `postgres-clusters/values.yaml` `dsnSecrets.bcs.pg-runtime-config` 含 `pipeline_authoring` |
| postgres-clusters 为该 BC 生成的 DSN Secret 名为 **`pipeline-authoring-db-dsn`**（键 `writer_url`/`reader_url`） | `postgres-clusters/templates/dsn-secrets.yaml`（`<bc-kebab>-db-dsn` 命名） |
| **别名不一致（既有，不修）**：of-data-engine dev overlay 引用 `pipeline-build-db-dsn`（postgres-clusters 从不生成该名字）；生产 values 中 PBS 无 DATABASE_URL envSecret 声明 | `of-data-engine/values-dev.yaml`（pipeline-build-service 段）|
| dataset 元数据在 **pg-schemas 集群**（`dataset_versioning` BC），与 pipeline 库**跨集群**；PBS canonical resolver 走 HTTP 适配（P1.5），分库旧共库 SQL 路径已 503 拒绝 | S1 契约 §2、`build_resolution.go:210-243` |
| PBS 服务迁移（含 20261117100000、quarantine 列）经 `migrations.pg-runtime-config`（`pg-runtime-config-app` DSN）应用到同一 database `app` | `of-data-engine/values.yaml` migrations 段 + `_shared/_migrations.tpl` |

## 2. Fix 1 — 同源 DSN（移除猜测默认，required + 测试 profile）

### 2.1 改动

- `of-data-engine/values.yaml` / `schedules-tick-cronjob.yaml`：删除 `pg-schemas-app`/`dsn`
  猜测默认；`schedulesTick.enabled=true` 时 `dsnSecret.name/key` **render 期必填**
  （`required`，负控实测缺省即 render 失败）。
- `debezium/values.yaml` / `kafka-connector-outbox-pipeline.yaml`：删除 `pg-schemas-rw`
  猜测默认；`pipelineOutbox.hostname/dbname` 同样 render 期必填；label
  `openfoundry.io/source` 与 `topic.prefix` 改为 `pg-runtime-config`（真实源集群，沿
  pg-policy「前缀=集群名」惯例）。
- 新增 co-located 测试 profile（仅渲染/校验用，非生产声明）：
  - `of-data-engine/values-schedule-outbox.yaml`（CronJob → `pipeline-authoring-db-dsn/writer_url`）
  - `debezium/values-schedule-outbox.yaml`（connector → `pg-runtime-config-rw…:5432/app`）
  - `of-data-engine/values-schedule-consumer.yaml`（PBS 消费方 SSL/mTLS env + 挂载，§5.1）

### 2.2 同源机械校验（已跑，见 §8）

渲染 postgres-clusters 提取 `pipeline-authoring-db-dsn.writer_url`，断言其 **host ==
debezium profile 的 hostname、database == dbname、search_path==pipeline_authoring**；
并断言新工件（CronJob env / connector spec / CDC Secret）内不再出现 `pg-schemas`。
65/65 断言通过（`assert_renders.py`，exit 0）。

### 2.3 生产拓扑如实限制（登记，不擅改）

1. **别名不一致**：`pipeline-build-db-dsn` 引用是既有缺口（dev overlay）；本轮不重排 BC/别名
   （review 明令禁止），合流时由 Codex 裁是否统一到 `pipeline-authoring-db-dsn`。
2. **tick 角色权限**：CronJob DSN 的登录角色是 `svc_pipeline_authoring`；库内
   `schedules.*`/`outbox`/`scheduling.*` 对象由 `pg-runtime-config-app`（owner）迁移建出，
   未见对 `svc_pipeline_authoring` 的 GRANT——**旧生产库直接启用 tick 会遇权限拒绝**。
   需要的操作员决定（未在本轮擅自实施）：对该角色授 schedules.definitions
   SELECT/UPDATE、outbox.events INSERT+DELETE、scheduling.dispatch_audit INSERT。
   隔离验收链（B runner）用 owner DSN，不受影响。
3. **跨集群限制**：调度消费方解析 dataset 元数据只能走 canonical HTTP 适配（分库 503 拒绝，
   S1 F9）；本 profile 不改变该布局，也不宣称 dataset 元数据对 pipeline 库 SQL 可见。
4. **不能宣称**旧 production 拓扑已运行支持本链路；生产启用必须走 §6 步骤。

## 3. Fix 2 — PG CDC 准备（可重跑、范围限定）

### 3.1 Artifact

`infra/helm/infra/postgres-clusters/files/pipeline-outbox-cdc-prep.sql`（psql，幂等，
ON_ERROR_STOP；密码经 `-v cdc_password` 注入，文件内不出现明文；生产建议从 Secret 注入
shell 变量，注意 psql argv 对同机进程可见）。**执行者 = CNPG 集群 superuser（`postgres`）**
——DB/表 owner 不足：PG16 实测（16.16，CREATEROLE 非超管）`CREATE ROLE … REPLICATION` →
"Only roles with the REPLICATION attribute may create roles with the REPLICATION attribute"、
`ALTER ROLE … REPLICATION` → "Only roles with the CREATEROLE attribute and the ADMIN option
on role … may alter this role"；superuser 同时满足 publication/GRANT，故单阶段不拆分。
**密码守卫**：`cdc_password` 缺失或空串时经真实 SQL 异常中止（psql 16 的 `\quit` 无退出码
参数，直接 quit 会 rc=0 误报“已准备”）——实测缺失 rc=3、空串 rc=3、正常 rc=0（连跑两次）。
作用域**仅限**：

- 角色 `debezium_pipeline_cdc`：`LOGIN REPLICATION NOINHERIT NOSUPERUSER NOBYPASSRLS`；
- `GRANT USAGE ON SCHEMA outbox` + `GRANT SELECT ON outbox.events`——**无** INSERT/UPDATE/
  DELETE、**无**任何其他 schema/表；防御性 `REVOKE CREATE ON DATABASE app`；
- publication `debezium_outbox_pipeline_pub` 由 **owner 侧预建**且**只含 outbox.events**
  （存在但内容不符时显式报错，不猜测修复）；
- 前置守卫：outbox.events 存在且 REPLICA IDENTITY FULL（B 迁移 0002 负责设置）、
  wal_level=logical、slots/senders 数值报告、slot 存在性仅 **NOTICE**（槽由 connector 首启
  创建，tick 的 CDC gate 等 slot active）。

为什么不用 bootstrap：CNPG bootstrap SQL 仅首次 init 生效，不覆盖既有库（review 指定）。
为什么 `publication.autocreate.mode=disabled`：Debezium 2.7.2.Final jar 内嵌属性文档原文
——"The connector will not attempt to create a publication at all. The expectation is that
the user has created the publication up-front. If the publication isn't found to exist upon
startup, the connector will throw an exception and stop."；且 PG16 下仅 REPLICATION+SELECT 的
角色**无权** CREATE PUBLICATION（需数据库 CREATE 权限，已实测拒绝），filtered 自动建表
publication 的路径对本角色根本不可行——预建+disabled 是唯一自洽组合。

### 3.2 values 变更

`postgres-clusters/values.yaml`：`pgRuntimeConfig.walLevel: replica→logical`、
`maxReplicationSlots 4→8`、`maxWalSenders 6→10`（对齐 pg-policy CDC 源形状；CNPG HA 2
standby 已占 2 walsender/slot，connector 增加 1）。**既有集群**由 CNPG operator 经滚动重启
应用 wal_level（operator 步骤 §6）；bootstrap 只覆盖新库，故 prep SQL 仍是必备步骤。
pg-schemas/pg-lakekeeper 不动。

### 3.3 隔离 PG16 验证（唯一 `ofwp32-c-c` 容器，已清理）

postgres:16-alpine（**实测 16.16**，本机 tag 已从 B 记录的 16.15 漂移——如实登记），wal_level=
logical，`--memory 512m`。fixture = B 的迁移 0001+0002（superuser 应用）。结果
（`pg16_prep_verify.out` / `pg16_slot_verify.out`，全部含原始 rc）：

| 检查 | 结果 |
| --- | --- |
| prep SQL 连跑 2 次 | 均 rc=0（幂等；R2 重验 run1/run2 rc=0） |
| 密码守卫：`cdc_password` 缺失 / 空串 | rc=3 / rc=3（真实 SQL 异常路径，错误消息明确） |
| 执行者证据：CREATEROLE 非超管 | CREATE ROLE…REPLICATION rc=1（"Only roles with the REPLICATION attribute…"）；ALTER ROLE…REPLICATION rc=1（需 CREATEROLE+ADMIN） |
| 角色属性 | rep=true / super=false / bypassrls=false / inherit=false / login=true |
| publication 内容 | 恰好一行 `outbox\|events` |
| 权限矩阵（superuser 侧） | usage_outbox=t、sel_outbox_events=t、ins=**f**、del=**f**、sel_sched_defs=**f**、create_db=**f**（usage_public=t 为 PG16 对 PUBLIC 的默认，无表权限即无暴露面） |
| cdc 角色直连（TCP+口令） | SELECT outbox.events OK；SELECT schedules.definitions **permission denied**；INSERT outbox.events **denied**；`CREATE PUBLICATION` **denied: permission denied for database app** |
| 槽创建（REPLICATION 权限） | test_decoding 槽 rc=0；**pgoutput 两参形式 rc=0**（16.16 实测；Debezium 实际走 replication 协议，SQL 侧成功是更强证据） |
| REPLICA IDENTITY 守卫负控 | 改回 DEFAULT 后 prep rc≠0 且给出明确错误，恢复 FULL rc=0 |

（测试口令均为一次性本地值，仅存在于隔离容器与证据文件，非任何真实凭据。）

## 4. Fix 3 — Connector 凭据与 ConfigProvider（不留占位）

### 4.1 选型：builtin FileConfigProvider + 挂载特定 Secret

- 证据：`org.apache.kafka.common.config.provider.FileConfigProvider` 位于
  **kafka-clients** jar（quay.io/debezium/connect:2.7.2.Final 内 `/kafka/libs/
  kafka-clients-3.7.0.jar` 实测含该类；kafka-clients 是任何 Kafka worker 镜像的必带库，
  Strimzi 4.1.0 构建镜像同理）——无额外插件、**零 Kubernetes API 读取权限**，符合
  「避免多余 API 权限」的 review 指引。
- 挂载方式（**Strimzi 1.0 事实修正**）：`spec.externalConfiguration` 已在 Strimzi 1.0.0
  **删除**（对 1.0.0 发布 CRD `041-Crd-kafkaconnect.yaml` 全文 grep，externalConfiguration
  0 处；`spec.template.pod.volumes[].secret.secretName` 与
  `spec.template.connectContainer.volumeMounts[]` 存在）。按官方 1.0 configuring
  「Loading configuration values from external sources」改用 pod template 卷挂载，
  路径 `/mnt/pipeline-outbox-cdc/cdc.properties`；connector 引用
  `${secrets:/mnt/pipeline-outbox-cdc/cdc.properties:username|password}`。CRD 字段有效性
  以发布 CRD 为证，非仅 render 断言。
- KafkaConnect `spec.config` 增 `config.providers: secrets` +
  `config.providers.secrets.class=…FileConfigProvider`；挂载**仅**
  `pg-runtime-config-debezium` 这一个 Secret（`pipelineOutbox.enabled` 时）。
- 新模板 `secret-pipeline-outbox-cdc.yaml` 渲染该 Secret（`cdc.properties` 键 =
  username/password 两行 properties）；**值门槛用 `dig` 不用 `default true`**——显式
  `cdcCredentials.create=false`（ESO 生产）时不渲染占位 Secret（负控 render 实测：
  connector/挂载仍在、Secret 不在）；dev/test 用 values 占位口令，生产交 ESO/Vault
  轮换，口令**必须与 prep SQL 设置的角色口令一致**。
- provider 别名保留 `secrets`：若未来切 Strimzi
  `io.strimzi.kafka.KubernetesSecretConfigProvider`（`${secrets:kafka/<name>:key}` 格式，
  需限 resourceNames 的 Secret 读取 Role），pg-policy 连接器现有引用格式即自然兼容——
  但那需要额外 RBAC 与镜像类核验，本轮不做。

### 4.2 顺带修正（同文件、同一 review 批次必要项）

- **apiVersion**：`kafka.strimzi.io/v1beta2 → v1`。Strimzi 1.0.0 已**移除** v1beta2（proposal
  #113；仓内 KafkaConnect/KafkaUser/KafkaTopic 均为 v1，旧 pg-policy connector 亦 v1）——原
  模板会被 1.0 operator 拒绝。
- **heartbeat**（Codex review 修正后的事实）：`heartbeat.interval.ms=30s` 时 connector 周期产
  心跳，topic 名由 `io.debezium.schema.AbstractTopicNamingStrategy#heartbeatTopic`（**v2.7.2.Final
  源码逐行核对**）决定：`String.join(topic.delimiter, topic.heartbeat.prefix, topic.prefix)`，
  默认 `topic.heartbeat.prefix=__debezium-heartbeat`、`topic.delimiter="."` —— 即本 connector
  为 **`__debezium-heartbeat.pg-runtime-config`**（非 `<topicPrefix>-heartbeat`；初版曾据
  jar 内 `-heartbeat` 字面量误推，已按官方属性纠正）。broker 禁 auto-create，故预置该 topic
  （compact，1 分区，kafka-cluster values）+ Connect 同名 Write/Describe ACL；connector config
  **显式 pin** `topic.heartbeat.prefix: __debezium-heartbeat` 使默认漂移可见。改 topicPrefix
  或该前缀需同步重备 topic 与 ACL（两处 values 注释已写）。
- **已知遗留（登记不修）**：pg-policy connector 的 `${secrets:kafka/pg-policy-debezium:*}`
  仍是占位（其 Secret/来源历史上就缺失，S4 遗留）；worker 配了 `secrets` provider 后它
  unpaused 时会 fail-fast（此前是无 provider 的字面量注入、连库时才失败）——行为从「静默坏」
  变「显式坏」，不构成回退。升级 pg-policy 凭据链路需另立任务（挂载其 Secret 或切 K8s
  provider + 受限 RBAC）。

## 5. Fix 4 — Kafka topics/ACL 最小补齐

写入主体结论（review 要求核实）：**outbox 真实发送者是 Debezium Connect**（trigger/outcome/
毒消息 DLQ 都是 outbox 行经 EventRouter 路由）；PBS 调度消费方**只读不写**；connector 自身的
处理错误 DLQ 由 Connect 写。因此：

| 主体 | 资源 | 权限 | 位置 |
| --- | --- | --- | --- |
| `debezium-connect-user`（Connect） | `pipeline.builds.v1`、`pipeline.schedule.outcomes.v1` | Write, Describe | debezium chart（`pipelineOutbox.enabled` 门控） |
| 同上 | `__dlq.pipeline.builds.v1`（消费方毒消息经 outbox 路由） | Write, Describe | 同上 |
| 同上 | `__dlq.outbox-pipeline.v1`（connector 处理错误 DLQ） | Write, Create, Describe | 同上（形状对齐 pg-policy DLQ ACL） |
| 同上 | `__debezium-heartbeat.pg-runtime-config`（heartbeat；命名=AbstractTopicNamingStrategy#heartbeatTopic，§4.2） | Write, Describe | 同上 |
| `pipeline-build-service-scheduler`（PBS 消费方，新 KafkaUser，默认关 `pipelineSchedulerUser.enabled`） | `pipeline.builds.v1` | Read, Describe | kafka-cluster chart 新模板 |
| 同上 | group `pipeline-build-service-scheduler` | Read | 同上（**无任何 Write**） |

topics：kafka-cluster values 增 `__dlq.outbox-pipeline.v1`（dlq，14d）与
`__debezium-heartbeat.pg-runtime-config`（cdc/compact/1 分区，名字按 §4.2 源码级命名规则）；
B 已加的
`pipeline.schedule.outcomes.v1`/`__dlq.pipeline.schedule.outcomes.v1`/`__dlq.pipeline.builds.v1`
保持不动（断言覆盖）。

**ACL 执法前置（如实）**：以上 KafkaUser ACL（含 PBS read-only）在 broker 配置
authorizer 前均为期望工件——见 §5.1「认证≠授权」。

### 5.1 A 侧所需 config 名（跨任务报 Codex → A；2026-11-17 与 A skeleton 对齐）

A 的 `SchedulerConfig` skeleton 在
[config.go:68](../../services/pipeline-build-service/internal/config/config.go)。
本轮 infra 侧已知/冻结口径：

- env 名（A 已定）：`SCHEDULER_CONSUMER_ENABLED`、`SCHEDULER_KAFKA_BOOTSTRAP_SERVERS`
  （prod 指向 `openfoundry-kafka-bootstrap.kafka.svc:9093`）、`SCHEDULER_GROUP_ID`
  （默认 `pipeline-build-service-scheduler`，与 C 预置的 KafkaUser/group ACL 同名）、
  `SCHEDULER_CREDENTIAL_ENCRYPTION_KEY`、`SCHEDULER_LEASE_TTL`、
  `SCHEDULER_HEARTBEAT_INTERVAL`、`SCHEDULER_MAX_EVENT_ATTEMPTS`。
- **KEY 语义（root 已冻结）**：沿 Connector 约定 **base64 解码后 32 字节**（不是 raw len32）；
  拒绝 JWT 等值/已知旧 JWT 派生；新 scheduler **无 dev fallback**（A 正在同步
  cipher/config/decision 文本）。
- 时间单位=秒；`SCHEDULER_MAX_EVENT_ATTEMPTS` 范围 1..5，默认 5。
- **Kafka auth 字段（root 已同步定名，A 实现这三个 + 协议）**：
  `SCHEDULER_KAFKA_SECURITY_PROTOCOL=SSL`（沿用现有 9093 mTLS listener，**不新建全局
  SASL listener**；SASL_SSL 仅当未来存在真实 listener 场景才可选）；
  `SCHEDULER_KAFKA_TLS_CA_FILE=/mnt/scheduler-ca/ca.crt`、
  `SCHEDULER_KAFKA_TLS_CERT_FILE=/mnt/scheduler-tls/user.crt`、
  `SCHEDULER_KAFKA_TLS_KEY_FILE=/mnt/scheduler-tls/user.key`。C 交付可审阅的挂载/env
  profile：`of-data-engine/values-schedule-consumer.yaml`（两 Secret 两目录只读挂载——
  K8s 禁止两个卷同 mountPath；不含其它新字段，KEY 注入留给 ESO/operator）。
- **认证≠授权（期望工件声明）**：现有 Kafka CR 只渲染 listeners/config/rack，**未发
  `spec.kafka.authorization`**——broker 当前无 authorizer，新 KafkaUser 的 Read-only ACL
  **不执法**，「PBS 已被限制只读」不成立；启用 authorizer 是独立生产前置决策（不随本
  feature 自改全仓 listener/authorization）。真实 mTLS/ACL/CNPG runtime 本轮未验（§7）。
- 消费 topic 固定 `pipeline.builds.v1`；毒消息 DLQ 走 outbox 行（topic 列 =
  `__dlq.pipeline.builds.v1`）——**PBS 不需要任何 Kafka Write 权限**。
- **跨 namespace 投送契约（启用前置）**：上述 TLS 材料 Secret 生成于 `kafka` ns，PBS 默认
  在 `openfoundry` ns——不可跨 ns 直接挂载，须先按 §6 步骤 7 以**同名同键**投送（ESO 或
  operator 同步，轮换自动同步；A 启动时加载证书，轮换后滚动重启 PBS 重载）。
- B 侧不变：`SCHEDULES_CDC_SLOT=debezium_outbox_pipeline` 与 connector slot 同值（两侧
  values 已注释互指）。

## 6. Operator 启用步骤（生产 gate 链，默认全关）

1. postgres-clusters 升级（wal_level=logical + slots/senders；CNPG 滚动重启生效）。
2. 确认 pipeline 迁移已应用（PBS migrator Job → database `app`）。
3. 在 `app` 库以 **CNPG 集群 superuser（`postgres`，或确具同等集群角色管理权限者——§3.1
   实测 DB/表 owner 不足）** 跑 `pipeline-outbox-cdc-prep.sql`（口令与将写入 Secret 的
   一致；重复跑安全）。
4. `pipelineOutbox.enabled=true`（+ 生产口令 Secret：`cdcCredentials.create=false` + ESO）→
   落地后 connector **仍 paused**；核对 publication/slot 前置。
5. Apicurio 注册：2.6.x 注册用 `values-apicurio-2-6.yaml`（v2 API job + 旧 v3 job 关闭）；
   注册内容 sha 与 pin 核对（Job 内置失败即 fail）。
6. 解除 `outbox-pipeline` connector pause（annotate）→ RUNNING、slot active。
7. **前置（不可跳过，跳过即 FailedMount）——跨 namespace Secret 投送**：consumer profile
   挂载的两个 Secret 都生成于 `kafka` namespace（KafkaUser
   `pipeline-build-service-scheduler` → user.crt/user.key；集群 CA
   `openfoundry-cluster-ca-cert` → ca.crt），而 PBS 按 Helmfile 默认部署在 `openfoundry`
   namespace——**K8s 不允许跨 namespace 直接挂载**。启用前必须把两个 Secret 以**同名同键**
   安全投送到 PBS 实际 namespace：用既有 **ESO**（在目标 namespace 建 ExternalSecret +
   指向 kafka ns 来源的 SecretStore/refreshInterval，轮换自动同步）或 operator 侧等价同步
   （不引入新平台服务，不手工复制生产 secret 值）。**轮换语义**：A 的消费方在**进程启动时
   加载证书**，Secret 更新/轮换后必须**滚动重启 PBS** 才重载。字段名与目标 Secret 名以
   `values-schedule-consumer.yaml` 为准（`pipeline-build-service-scheduler`、
   `openfoundry-cluster-ca-cert`；键 user.crt/user.key/ca.crt；挂载
   /mnt/scheduler-tls、/mnt/scheduler-ca）。
8. `schedulesTick.enabled=true` + 必填 DSN（同源 `pipeline-authoring-db-dsn/writer_url`，
   并先解决 §2.3-2 的 tick 角色授权决定）；启用 `pipelineSchedulerUser.enabled=true` 与
   PBS 消费方（`of-data-engine -f values-schedule-consumer.yaml`——SSL/mTLS env 与挂载，
   §5.1 与上一步投送前置；并注入 `SCHEDULER_CREDENTIAL_ENCRYPTION_KEY`；若要 ACL 真实
   执法需先裁 authorizer 前置）。
9. 回退：schedulesTick.enabled=false（tick 停）→ connector pause → slot 保留（WAL 续传）；
   彻底下线再 drop slot/publication/role（prep SQL 不含 DROP，需人工）。

## 7. 已验证 / 未运行（如实）

**已验证（本机）**：helm 3.16.4 渲染 4 chart 默认值（新工件全不出现）+ profile/flag 渲染 +
缺省必填负控（render 失败并给指引）+ ESO 负控（`cdcCredentials.create=false` 显式生效：
Secret 不渲染、connector/挂载仍在）；**78 项结构断言 exit 0**（含同源交叉校验、heartbeat
topic 名与 connector 显式 pin/预置 topic/ACL 三方一致、Strimzi 1.0 挂载形态——无
externalConfiguration、template.pod.volumes + connectContainer.volumeMounts 于 /mnt、
consumer profile 的 SSL/3-TLS-env/双 Secret 双目录挂载、B 的 CM schema
**字节级 sha 与 pin 一致**——B checkpoint 中「helm 侧待办」已补）；helm lint 4 chart 全过；
隔离 PG16.16 上 prep SQL 幂等/最小权限/守卫负控（§3.3，含 R2 密码守卫 rc=3 与 CREATEROLE
拒绝证据）。CRD 字段有效性以 Strimzi 1.0.0 发布 CRD 原文核对为准（§4.1），非仅 render。

**未运行（禁止或本轮不可用，如实登记）**：生产/任何真实集群未 apply（含 B 的 ofwp32-acc
kind）；CRD 不在本机 → KafkaConnector/KafkaUser/KafkaTopic 未做 server 端校验（kubectl 1.36
client dry-run 亦需 API discovery，离线不可用）；**production SASL/TLS 链路未实测**（Connect
mTLS、ACL 生效、Apicurio 2.6 实例上的注册 job）——隔离链中 B 的 runner 覆盖 PLAINTEXT 侧；
**跨 namespace TLS Secret 投送（§6-7 契约）未实际操作**——本轮未触碰任何生产 secret 值，
按契约执行属启用时 operator 步骤；CNPG 既有集群 wal_level 滚动重启未实测；
`svc_pipeline_authoring` 在真实库的表权限未核（§2.3-2）。B 的基础链 runner 因 C 修改 infra
而失败的轮次应保留原始失败证据，待 C freeze（=本文件定稿）后 B 最终重跑，不得放宽证据口径。

**遗留登记（本轮不改，非本 feature 资源）**：`infra/runbooks/kafka.md` 内 3 处示例仍写
`kafka.strimzi.io/v1beta2`（runbook 文档，非渲染资源；Strimzi 1.0 已移除该版本——示例与
目标 operator 不匹配，待 runbook 专项更新，不随本 feature 顺手改）；BC 跨库/别名拓扑限制见
§2.3；不做全平台 apiVersion 升级或搬库。

## 8. 证据索引（`/private/tmp/ofwp32-c-evidence/`，会话外持久）

`render_status.txt`（render rc 组）、`assert_renders.py`/`.out`（78 断言 exit 0）、
`*-default.yaml`/`*-profile.yaml`/`dbz-eso.yaml`/`de-consumer.yaml`/`kc-apicurio26.yaml`/
`kc-user.yaml`/`*-neg.err`、`lint-*.out`、`pg16_prep_verify.sh`/`.out`、
`pg16_r2_guard.out`（密码守卫/执行者证据）、`pg16_prep_{missing,empty,r2_ok1,r2_ok2}.out`、
`pg16_cr_neg{1,2}.out`、`pg16_prep_run{1,2}.out`、`pg16_role_*.{out}`、
`pg16_slot_verify.out`、`kubectl-*.err`（discovery 失败原文）。Debezium/Strimzi 版本核对证据在
`/private/tmp/ofwp32-c-doccheck/`：jar 级字符串提取（doccheck_*.txt，含 publication
autocreate 枚举原文、`__debezium-heartbeat` 常量、FileConfigProvider 所在 jar）+ 官方源码
v2.7.2.Final（`Heartbeat.java`、`AbstractTopicNamingStrategy.java`、经 api.github.com 取回，
`heartbeatTopic() = String.join(delimiter, heartbeatPrefix, prefix)`）。

## 9. A/B 合流信息

- **A（pbs）**：§5.1 config 名——含 root 定名的 `SCHEDULER_KAFKA_SECURITY_PROTOCOL=SSL` +
  三个 `SCHEDULER_KAFKA_TLS_{CA,CERT,KEY}_FILE`（A 实现，C 不再新增字段）；KafkaUser
  `pipeline-build-service-scheduler` 由 infra 预置（默认关）；DLQ 经 outbox 路由（无直接
  Kafka 写）；§2.3 的 tick/consumer 表权限属 A 侧迁移/运营决定，未代签。
- **B（producer/验收）**：C 已 freeze 的 infra 变更清单见 git status；B 基础链 source hash
  含 infra，最终重跑应在 C 变更之上；CM 字节比对 helm 侧已补（§7）；slot/registry/env 契约
  未变（R2 仅改 Connect worker 挂载形态与 connector 凭据引用路径——`/mnt`，隔离链若用
  Strimzi 需同步该形态；B 当前直连 debezium/connect 镜像不受影响）。B 的 apicurio 2.6
  profile、schema 副本、CronJob/connector wire 契约**均未回滚**，断言覆盖。
- **root 待裁**：Kafka broker authorizer 前置（现 Kafka CR 无 `spec.kafka.authorization`，
  所有 KafkaUser ACL 为期望工件）；legacy `pipeline-build-db-dsn` 别名统一；
  `infra/runbooks/kafka.md` 的 v1beta2 示例更新。

### 8.1 Apicurio v2 注册枚举修正复验（2026-11-17，Sol 接管）

生产 registrar 的 `ifExists` 已改为 `RETURN_OR_UPDATE`；固定官方
2.6.13.Final OpenAPI `components.schemas.IfExists` 枚举为
`FAIL|UPDATE|RETURN|RETURN_OR_UPDATE`。对真实隔离 Registry 2.6.13.Final，
从本轮 Helm 渲染结果提取 `register_and_verify` 原函数执行，首次和重复运行
退出码均为 0，两工件 GET 原始 bytes 的 SHA256 均匹配上述生产 pin，重复后
两工件版本数仍为 1；小写 `update` 负控真实返回 HTTP 404。

证据：`/private/tmp/ofwp32-runtime-sol-evidence/` 内 `kc-render.yaml`、
`kc-lint.txt`、`production-registrar.sh`、`registrar-{first,repeat}.{log,rc}`、
`registry-info.json`、`{trigger,outcome}-versions.json`、`lowercase-*`。
执行仅替换临时 group、schema 文件路径及宿主 SHA256 命令名称
（`sha256sum` → `shasum -a 256`）；未 apply 生产 Job，未改变生产配置。
受影响 kafka-cluster chart 的 render 与 lint 均退出 0。此项恢复冻结。

补充原始 HTTP 记录（同日本轮）：`c-final-http-raw.json` 保存独立测试 group
`ofwp32-c-final-20261117` 中两工件首次/重复注册的真实 HTTP **200/200**、完整
响应 headers/body 与成功退出记录；`c-final-*.registered.json` 保存真实 GET
原始 bytes，逐字节与生产 schema 文件相等。该补验仅连接本轮隔离 Registry，
不操作 connector、slot、PBS 或 Kafka；未 apply 生产模板。
