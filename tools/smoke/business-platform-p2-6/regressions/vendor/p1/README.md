# business-platform P1 验收环境（隔离 Docker + 合成 REST 源）

支撑 `docs/architecture/business-platform-implementation-plan.md` P1 租户
隔离 + 真实同步 + **Build/Job HTTP 正向链（P1.5 强制真实 9 场景）** 的
可重复黑盒验收。本目录只包含脚本与 fixture，不含任何凭据或真实客户
数据；不改三个服务的源代码，也不改实施计划。

- 实施会话：Dataset 隔离与 schema、Pipeline 隔离与 Build/Job 生命周期、
  Source/Sync 隔离 + 真实 REST 分页。
- 本目录由验收会话维护：合成 REST 源、隔离 compose 环境、smoke 断言、
  Go 测试与迁移测试执行路径。
- smoke 共 **9 场景**（7 个 P1 主线 + `build_positive`/`build_isolation`）；
  Build/Job 面在当前契约下**必须真实通过**——不存在 503 降级或空 jobs
  跳过路径（旧“不可解析 UNCOVERED”语义已废除）。

## 一键命令

前置：Docker Desktop（BuildKit）、`python3`（stdlib 即可）、`openssl`。
所有命令从任意目录执行均可。

```sh
SMOKE=tools/smoke/business-platform-p1   # 仓库内路径
$SMOKE/run.sh setup            # 生成 .env（仅存在于 /private/tmp，600）并渲染 compose
$SMOKE/run.sh up               # 构建 dataset/pipeline/connector 并等健康检查通过
$SMOKE/run.sh smoke            # 执行全部验收场景 → $OF_P1_TMP/result.json
$SMOKE/run.sh tests            # 三服务全量 go test（compose profile tests）
$SMOKE/run.sh migrate-tests    # PG16 空库 + 重跑迁移集成测试（DATASET_DB_DSN）
$SMOKE/run.sh restart          # 重启三服务（迁移重跑黑盒证据），再等健康
$SMOKE/run.sh status | logs <svc> | down | down-volumes
```

环境与结果位置（`OF_P1_TMP` 可覆盖，默认固定以便重跑）：

| 路径 | 内容 |
| --- | --- |
| `/private/tmp/openfoundry-platform-p1-q3v9/.env` | 随机生成的 PG 密码、JWT secret、凭据加密 key、服务 fallback JWT（600，勿提交、勿打印） |
| `/private/tmp/openfoundry-platform-p1-q3v9/compose.yaml` | 渲染后的 compose（`__OF_ROOT__` 已替换） |
| `/private/tmp/openfoundry-platform-p1-q3v9/result.json` | smoke 断言结果与证据（含每场景 pass/fail、关键计数、错误摘要） |
| `/private/tmp/openfoundry-platform-p1-q3v9/logs/` | boot/smoke/unit-tests/migrate-tests 日志 |

端口与项目名与既有环境完全隔离（ compose project
`openfoundry-platform-p1-q3v9`；host 端口 dataset **17078**、pipeline
**17081**、connector **17088**、fixture **17090**，全部只绑 127.0.0.1；
不占用 16078/16081、15078/15081，Postgres 不对 host 发布）。镜像复用
`golang:1.26.3-alpine3.22` 与 BuildKit 缓存 `of-go-mod` / `of-go-build`。

## 执行前必须完成的契约核对（不猜接口）

`smoke/smoke.py` 顶部的 `CONTRACT` 块集中了请求形状，已按**当前代码**
对齐（字段名逐一读自实现，非猜测）：

1. **Dataset**：`/v1/datasets` 创建/列表/详情/删除、`upload`（multipart
   `file`）、`files` 列表、`files/{id}/download`、`files/rows`、
   `outputs:commit`、`lineage-links`、`/v1/_internal/local-fs/*` presign。
2. **Pipeline**：`/api/v1/pipelines` CRUD、`nodes/{node}/preview`、
   `runs`、`builds`/`jobs` 面（build 最小计划若被拒，记录证据降级）。
3. **Source/Sync（connector）**：`sources|syncs` 路径、`PATCH syncs`、
   `syncs/{id}/run`、run 状态枚举。
4. **rest_api sync 配置已对齐
   `internal/models/rest_api_sync.go`（RESTAPISyncConfig）**：
   `resource_path=/v1/inventory`（纯路径）、`records_path=items`、
   `pagination{type=page|cursor,page_param,page_size_param,page_size,start_page,cursor_param,next_cursor_path}`、
   `limits{max_pages,max_records,max_response_bytes,max_total_bytes,request_timeout_ms}`、
   `query_params{src=...}`（src 走 query，不进 path）。page 模式按
   **空数组终止**，越界页 fixture 返回 200 空页；loop/超限场景用
   `max_pages`（真实超限机制）触发。
5. **服务间身份**：pipeline/connector 调 dataset 是否转发调用者 JWT
   （compose 保留 `DATASET_SERVICE_BEARER` 无 Org 回落 token）。

测试 runner（`tests.Dockerfile` + `run_tests.sh`）：**测试在
`docker compose run` 时执行**（非 build RUN），源码 bind-mount 进容器，
全部集成测试经 compose 网络直连 PG 服务（无 docker socket、无
testcontainers）：`DATASET_DB_DSN=migrate_fresh`（每次重置，空库证据）、
`PIPELINE_DB_DSN` 与 `CONNECTOR_DB_DSN`（指向维护库，各测试自建临时库）。
必测名单硬校验（缺失或 `--- SKIP` 即失败，绝不静默；当前 **26 项**，
每项必须顶层 `--- PASS`）：Dataset（6）`TestDatasetOrgTenancyMigrationAndVisibility`、
`TestMigrationsFreshDBRerunAndRepeatedSnapshots`、
`TestCommitDatasetOutputPublishFaultInjection`、
`TestSchemaBranchHTTPRoundTrip`、`TestSchemaOwnershipCannotBeClaimed`、
`TestSchemaColumnsRepairLegacyDatabase`；Pipeline（17）
`TestTenantIsolationMigrationsAndBoundary`、
`TestTenantMigrationOnPopulatedLegacyDB`、
`TestTenantRepairRegressionBundle`、
`TestBuildFlowCancelledContextFinalizesState`、
`TestBuildFlowPublishDedupRebindsMapping`、
`TestBuildFlowLedgerUpgradeAppliesNewMigrations`、
`TestBuildFlowAbortTransactionTerminalSemantics`、
`TestBuildFlowPendingRetryClosure`、
`TestBuildFlowSecondRecordFailureCompensatesEverything`、
`TestBuildFlowSecondOpenFailureCompensatesFirst`、
`TestBuildFlowReleaseFailureKeepsReference`、
`TestBuildFlowCancelAfterOpenCompensatesOnDetachedContext`、
`TestBuildFlowJobSpecPublishOwnership`、
`TestBuildFlowForeignInputSpecRejectedZeroSideEffects`、
`TestBuildFlowMultiOutputPGStateAssertions`、
`TestBuildFlowRecordAndAbortBothFailKeepsRecoverableHandle`、
`TestBuildFlowEmitJobLogTenantGating`；Connector（3）
`TestPGMigrateEmptyDatabaseAndRerun`、
`TestPGMigrateLegacyDatabaseGainsOrgColumns`、`TestPGSourceTenantCRUDMatrix`。
后续新增专项随名字发布同步（runner 与 summary 一致性由静态校验保证，
summary 标题按名单长度动态计数）。

**同步产物下载绑定链**（不取 `files[0]`）：
`run.dataset_version_id`（即 `outputs:commit` 返回的 `transaction.id`）
== `file.transaction_id`；对选中文件原始字节计算
`SHA256 == run.content_hash == transaction.metadata.content_hash`，
三方一致才算通过。失败 run 之后的保护性断言分别覆盖：branch 文件集合、
**公开 `/v1/datasets/{rid}/files/index`**、血缘链接、旧版本下载字节
（按 run 绑定 file id 重下比对）。空源成功时发布的是绑定的 0 行空
artifact（逐值断言为空）。

**跨 org 探针语义**：详情/下载/上传/执行/输出提交必须 403/404；
**外域父资源下的子列表**（files/runs/jobs 等）允许 403/404 或 200+**显式
断言为空**的 data（租户过滤正确的空列表不算失败，也绝不把 400 当越权
证据）；**全局** `/api/v1/builds` 列表因调用方在本域拥有合法 build，断言
改为 200 且外域 build UUID/RID（含文本 RID 扫描）零出现、允许本域行。
Build/Job 面由 `build_positive`/`build_isolation` 以**真实 202 正向链**覆盖：
契约合法 body 的任何非 202 一律 FAIL（详见下文场景段）。
`UNCOVERED` 注册表仍存在，用于如实记录真正无法经 HTTP 构造的方面
（result.json/控制台/summarize 显式列出并标注 "full acceptance NOT
claimed"）；Build/Job 正向不属于可降级项。
分页循环场景用 cursor 分页 + `next_cursor_path`（page 模式 runtime 只递增
页码不看 next_page）；loop 的失败须命中**回环检测**文案（"pagination
loop"/"repeated"），页数超限须命中 `max_pages`，且快速失败（<120s）——
轮询超时本身是 FAIL，不算"正确失败"。

## 验收覆盖矩阵（P1.1/P1.2/P1.3 → smoke 场景）

| 验收标准 | 场景 |
| --- | --- |
| 同用户（同 sub）在 A/B 两组织创建同名资源各自可用 | dataset/pipeline/connector isolation |
| 列表/详情/下载/预览/运行/输出提交跨组织被拒 | dataset/pipeline isolation（get/list/files/download/rows/outputs:commit/preview/run） |
| 修改 owner_id、组织头、请求体 tenant 字段不扩大访问 | 三场景 forged headers（X-Org-Id/X-Tenant-Id/X-Organization-Id）+ body `owner_id/tenant_id/organization_id` |
| 组织管理员跨租户被拒（同租户可用） | admin_a 对 B 资源被拒、对 A 资源可用 |
| 无 OrgID 旧个人域回归可用且他人不可读 | legacy_regression（151 行 CSV→148 行 NDJSON 全量核对 + presign 回读） |
| Source/Sync/SyncRun 列表/详情/触发隔离；跨组织 source/目标 Dataset 绑定被拒 | connector isolation（拒绝可发生在 create 或 run，两分支都断言目标无残留） |
| ≥3 页 151 条、SKU 前导零、逐值一致 | sync_real_rest（fixture 默认 50/页 ×4 页；NDJSON 逐行 dict 全等比较） |
| 同配置改源内容 → hash 变化 | hash_change（fixture admin 翻转 content=2，同 sync 重跑，hash 不同 + 新内容逐值核对） |
| 第二页 500 / 无效 JSON / 分页循环 / 页数超限 → run 失败 | sync_failures（每模式独立 src + 全新目标；循环=cursor 真实重复触发 max_pages，超限=页模式 151 页 × max_pages=5） |
| 失败不留半成品、旧有效版本仍可用 | sync_failures（失败后目标无文件；有 v1 的目标失败后**文件集合、逐文件字节、文件索引、血缘链接**全量不变） |
| 空源（total=0）不产生半成品 | sync_failures（第 1 页空数组；成功则 0 行、失败则目标无变化） |
| 跨 org 输入/输出 Dataset 不改变目标 | pipeline isolation（运行前后完整快照对比：file ids+逐文件字节） |
| build/job 面正向 + 跨 org | build_positive / build_isolation（真实 202 链；任何非 202 一律 FAIL） |
| cursor 分页机制 | sync_real_rest（cursor 源 151 行逐值核对） |
| Pipeline 直接消费同步产物并可下载 | sync_real_rest（filter qty>3 → 148 行下载全等核对） |
| 空库迁移/重跑 | `migrate-tests`（集成测试 + 空库）与 `restart`（黑盒重跑） |

不覆盖（按计划属后续阶段）：定时/增量/断点恢复（P2）、Excel 与 push
（P3）、对象存储大批量（P4）、Ontology/App/搜索/工作流边界（P1 后纳入
矩阵）。

## Artifact 字节持久性（dataset-files 命名卷）

Dataset local backing 实际落盘于进程 CWD（`RootDir` 空 → `.`，distroless
nonroot 的 `/home/nonroot`）。验收 compose 为该目录挂命名卷
`dataset-files:/home/nonroot`（top-level 声明；BaseDir/key/DB 不变，不遮
`/usr/local/bin`、`/etc`）。验证环境 **Docker Compose v5.3.0**；命名卷跨
容器重建复用依据官方 Compose Spec：
https://docs.docker.com/reference/compose-file/volumes/ 与
https://docs.docker.com/reference/compose-file/services/#volumes 。
证据分层见 `results/2026-11-13-build-http/`（historical FAIL /
recreate PASS / restart PASS / 三域 Build 句柄四个独立 JSON）。

## Build/Job 正向链场景（build_positive / build_isolation）

补齐 P1 唯一 Build/Job HTTP 正向缺口。契约全部读自当前代码（不猜路由）：

- **正式前置**：`POST /v1/job-specs/TRANSFORM`（`CreateJobSpecRequest`：
  `pipeline_rid/branch_name/inputs[{dataset_rid}]/output_dataset_rids/logic_payload`）
  → 201 `{rid, logic_kind, content_hash}`（canonical，幂等去重）。
  Pipeline CRUD 不自动发布定义。
- **输入**：普通 upload + metadata-only commit **携带 schema**（sku/qty 两列），
  `GET /v1/datasets/{rid}/schema` 必须 200。**输出**：`POST .../branches
  {name}` 显式建 main（409 已存在可接受），不靠 DryRun 隐式建分支。
- **正例（build_positive，同 sub 双 Org + 无 OrgID 个人域三租户各跑全链，
  P1.5）**：每租户（org A、org B、legacy 个人域）独立完成 DryRun 200 无
  errors，且响应含**非空 resolved jobs**——`job_spec_rid` 引用发布的
  canonical spec RID、resolved_inputs/resolved_outputs 非空；DryRun 前后
  以严格七维快照（files/index/lineage/builds/OPEN txn/branch head/bytes，
  每个 GET 必须 200）加 PG `jobs`/`build_input_locks`/
  `build_pending_output_txns` 三表计数（隔离栈无并行负载，全局计数即
  基线）**完全不变**证明"不新增 Build、Job、OPEN 事务或锁"，不凭
  builds 列表一项下结论；CreateBuild 202（`job_count>=1`、state
  WAITING/BUILD_RESOLUTION 系，不等 executor/Spark）；随后由共享的
  `verify_build_and_abort` 严格生命周期函数取证（三租户的 main 与
  feature-x 分支同一套断言，非空壳）：Build UUID 与 RID 双读、jobs 非空
  且 `job_spec_rid == 发布的 canonical RID`、abort 后 job RID 集合与输出
  txn 集合与 abort 前**完整相等**、远端逐 txn ABORTED、幂等 abort、
  文件集不变、**PG `build_input_locks` 残留为 0（在 abort 后、任何再建
  重新拿锁之前断言）**；主链另验"同输出再 CreateBuild 202 + 清理 abort
  必须成功”；feature-x 的 abort（含幂等重试）**首次即走 RID 路由**
  （`/v1/builds/{rid}:abort`，无 UUID 回退——真实 RID 取消而非仅幂等），
  随后共用同一套非空集合/远端 ABORTED/零锁断言；三租户 spec RID 互不
  相同。
- **反例（build_isolation，前置=三租户正例句柄）**：B→A 与 **adminA→B
  对等**（UUID+RID 读、UUID abort、RID abort、outputs、input-resolutions、
  logs 读+写、Publish/DryRun/CreateBuild 三入口）全拒；**个人域**：
  legacy2 与 org-A 用户对无 OrgID 真实 Build/Job 的同套探针全拒、列表
  隔离；列表 200-empty 不含外域 ID；同 org admin 正向可用；伪造
  header/body 无效。
  入口按变体区分：**全外域 / own-pipe+foreign-output** 三入口（Publish/
  DryRun/CreateBuild）严格 403/404；**foreign-input** 的 HTTP 覆盖 =
  Publish（inputs 数组）+ DryRun **inline_specs 正式契约**（models.JobSpec
  形状携外域 input）严格 403/404；其 **CreateBuild 语义不在此经 HTTP 构造**
  （请求体无 input 字段；注入裸 `job_specs` 行不会改 `pipeline_job_specs`
  的 canonical 映射，拒绝/接受都不可归因）——由 Pipeline 侧新增的**真实
  PG 历史映射用例**覆盖（服务会话负责，result.json 中
  `foreign_input_create_coverage` 记录该分层，不称三变体全部由 HTTP 覆盖）。
  合法性证明：B 自链 CreateBuild 202（先于快照）、adminA 同 body 发布
  201。**两域**
  快照（files/index/lineage/builds 列表/OPEN txn 集/branch head/原文件
  bytes；每个 GET 必须 200，失败即 FAIL 不退 None）前后不变。job 日志
  POST 写唯一 marker 同域读回、跨域读写全拒。
- **abort 证据口径**：Build 状态用正式枚举 `BUILD_ABORTING/BUILD_ABORTED`、
  Job `ABORT_PENDING/ABORTED`、Dataset txn `OPEN→ABORTED`；锁证据 =
  abort 后 PG 零残留 + 同输出合法重建 202（HTTP 无法列锁，不宣称
  "全无锁"）。
- 候选就绪核对点：Pipeline 的 Dataset HTTP adapter 与 Build/Job 生命周期
  补偿落地后，spec 发布/Lookup 双表 RID 绑定按 canonical 定义核对
  （`job.job_spec_rid` 必须等于发布返回的 RID）；旧 503 boundary gate
  移除后此场景**必须真实通过**——不得再以 UNCOVERED 或空 jobs 跳过。

## 合成 REST 源（fixtures/rest_source.py）

- 确定性 151 条库存：`sku` 为六位前导零字符串（`"000001"`），`qty`
  1..151，另含名称/价格分/类目/布尔/固定时间戳；无随机数。
- 分页三风格并存：`page`/`page_size`、`offset`/`limit`、`cursor`，
  envelope 支持 `items`（默认）/`data`/裸数组（`wrap=`），并带
  `Link: <...>; rel="next"` 与 `X-Next-Page` 头。
- 行为由 `?src=<key>` 隔离；`POST /admin/src/<key>/set` 运行时切换
  `mode=ok|fail_page2|invalid_json|loop|many_pages`、`content=1|2`、
  `page_size`、`total`、`require_api_key`——切内容不改 Source 配置，
  这就是“同配置改源”的机制。
- `expected_record`（smoke 侧）与 `record`（fixture 侧）是同一公式的
  两份实现，故意不复用，避免用被测物验证被测物。

## 失败模式与降级路径（如实报告，勿静默放行）

- smoke 逐场景 try/except：**全部场景都会执行**，失败记录进
  `result.json`，退出码非 0。修复后可 `run.sh smoke --only <场景>`
  单跑。
- 文件级错配通过 upload→metadata-only commit→真实 dataset_files.id
  全覆盖，不存在“无 file id”降级路径。
- “目标发布失败”由 datasets PG 的 NOT VALID CHECK 注入实现（真实
  outputs:commit 500）；删除目标保留为独立 precheck 负例。
- 分页循环/超限依赖实现的 max_pages/循环检测；若实现缺失，轮询超时
  会如实报 FAIL（这正是要抓的缺陷，不是脚本问题）。

## 安全与凭据

- 所有密钥 `openssl rand` 逐次生成，只存在于 `$OF_P1_TMP/.env`（umask
  077），仓库零凭据；JWT 由 smoke 内存铸造（HS256，匹配
  `libs/auth-middleware.Claims`，`org_id` 可空），打印前经 token 注册表
  脱敏，`result.json` 不含任何 token/密码。
- 服务容器只绑 127.0.0.1；fixture 为容器网络内网源
  （`ALLOWED_EGRESS_HOSTS=fixture`）。
