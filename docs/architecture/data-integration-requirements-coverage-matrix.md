# 数据连接与集成：本期 12 项需求覆盖矩阵（DI-01..DI-12）

起草 2026-11-16，基线 commit `664165f3`（分支 `codex/dataset-pipeline-file-io`，本次规划调整前工作区干净）。文中源码定位均于 2026-11-16 对当前代码静态复核；本文是**将来验收的标准**，不是完成报告：所有验收条款均未勾选，现有实现只如实分级，不把 README 描述、路由空壳或未被调用的源码算作需求完成。

## 来源

- 需求与优先级（主依据，完整 base）：`/Volumes/ExternalSD/projects/palantir-research/knowledge-base/_index/` 下 `裁剪汇总.md`（「数据连接与集成」227-238 行原始 12 项顺序，DI-01..DI-12 与之一一对应）、`裁剪判据.md`（定稿 v4）。
- 阶段与规模：同目录 `路线图.md`——阶段 1 九项、阶段 2 血缘一项、阶段 4 SQL 与 S3 API 两项；含第一阶段细点现状粗查表。
- 官方原文（完整 base，与本仓不同 repo）：`/Volumes/ExternalSD/projects/palantir-research/knowledge-base/docs/foundry/` 下 `dataset-preview/time-travel/en.md`、`dataset-preview/csv-parsing/en.md`、`microsoft-excel/transforms-excel-parser/en.md`、`data-integration/foundry-s3-api/en.md`、`health-checks/checks-reference/en.md`。
- 本仓证据入口：[business-platform-implementation-plan.md](business-platform-implementation-plan.md)（历史验收章节）；P1 最终验收 [2026-11-13-build-http/summary.md](../../tools/smoke/business-platform-p1/results/2026-11-13-build-http/summary.md)（9 PASS、60 包单测、26 项必需 PG16；更早的 p1-mainline-baseline 是 7/0/1 未覆盖时点，不混用）；P2 最终封存 [FINAL-BRIEF.md](../../tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/FINAL-BRIEF.md) 及同目录批准 seal/terminal；P3.1 为计划历史章节 + 仓库内测试（commit `664165f3` 为代码定位；`/private/tmp` 报告为本机临时证据，不视为入仓闭包）。
- **证据存放与可移交性**：上述 P1/P2 的 `tools/smoke/**/results/` 归档是**本机工作树文件，非 Git 跟踪**（`git ls-files` 无输出，`.gitignore` 第 92 行 `/tools/smoke/**/results/` 命中），fresh clone 不自带，获取位置=本机该路径；源码/测试/fixture/验收工具脚本在 Git 内。历史验收结论仍有效，本矩阵引用时均已注明获取位置；不复制归档、不改 ignore、不将其纳入提交。

## 证据分级

| 级 | 含义 |
| --- | --- |
| A | 历史真实验收：明确验收范围与结果的验收档或计划文档内验收记录。物证分两类：计划文档记录（Git 跟踪）与 `tools/smoke/**/results/` 本机归档（**非 Git 跟踪、fresh clone 不自带**，须注明获取位置）；源码/测试/验收工具在 Git。源码 commit 本身不构成 A |
| B | 源码存在且有单测/类型定义，但未进入真实验收链 |
| C | 部分实现或未接线：catalog 声明、planned 标记、adapter 未注册、handler 壳 |
| D | 未核实/待盘点（含有限检索未定位——**不推定全无**，他组声明亦入此级） |
| E | 本仓确认无实现（须以盘点为前提，本期需新建） |

## 总览

| ID | 需求点（裁剪汇总顺序） | 优先级 | 路线图阶段 | 当前结论 |
| --- | --- | --- | --- | --- |
| DI-01 | Core concepts | P0 | 1 | 数据集/构建已验收，定时限于同步链调度；健康检查概念未验收（他组声明待对接）；Views 有元数据接口无 SQL 物化；Iceberg/Media sets 独立后续、Virtual tables/Views 建议后挪，冲突待对齐 |
| DI-02 | Data Connection | P0 | 1 | 目录/租户/凭据分离与 REST 链已验收；S3/MySQL 源码未接线、PostgreSQL 专用实现未定位（有 JDBC/CDC 线索）、电商三个待盘点、Webhook 有租户缺口且无自动重试 |
| DI-03 | Building pipelines ★ | P0 | 1 | 管道=代码已定（d769）；轻量执行链已验收但无聚合/窗口/反连接；管道调度仅源码单测；CSV/JSON 推断参数不全 |
| DI-04 | Maintaining pipelines | P1 | 1 | 健康检查仅 4 枚举+形态上报，无真实数据求值；「6 类」与官方 5 类映射待定 |
| DI-05 | Pipeline Builder Expressions ★ | P1 | 1 | 标量签名目录与求值子集并存且不一致；需清单核对，不做官方 338 全量 |
| DI-06 | Pipeline Builder Transforms | P1 | 1 | 产品口径 25 项待映射：源码静态核对为 24 静态项+动态 UDF，大量 planned；3 个新建变换执行面均无 |
| DI-07 | Data Lineage | P1 | 2 | 服务/前端/发布侧写入已有；NodeKind 仅 3 类；nodes/relations 有 PG 表与接线，read-model 默认内存；组织/项目隔离未验收；生产事件接线缺 |
| DI-08 | Dataset Preview | P1 | 1 | 预览参数仅 limit/offset/format；Time Travel 有 ts/txn 原语未接到预览；CSV 参数固定 |
| DI-09 | SQL in Foundry | P2 | 4 | FlightSQL 真实现+远程 delegate；本地查询引擎仅 literal SELECT；Dataset 与 Ontology 双侧整项目标未动工验收 |
| DI-10 | Pipeline Builder ★ | P2 | 1 | 画布与编辑器仍在，readonly 策略有差距；需保留节点查看/真预览 |
| DI-11 | Microsoft Excel | P2 | 1 | P3.1 普通上传已验收≠管道 Excel Parser；raw workbook 变换待接线/待核实，分批范围待裁 |
| DI-12 | S3-compatible API | 本期做·优先级未定（路线图暂按 P2） | 4 | 有限检索未定位对外 S3 协议端点（D）；盘点后确定新建范围；不依赖 Iceberg 上线 |

★ = 需关注（做法与 Palantir 不同，d769：管道与表达式由研发写代码，画布只读，配置台不做编辑）。

---

## DI-01 Core concepts（数据集、构建、定时、健康检查概念）

**优先级/阶段**：P0 / 阶段 1（规模 L，路线图排 11-23~11-30，做法「补齐」）。细点 11 项，顺序照官方：Datasets、Streams、Media sets、Branching、Builds、Schedules、Health checks、Iceberg tables、Virtual tables、CDC、Views。

**本期范围**：数据集、构建、定时、健康检查这套概念照官方立；流（Streams）后续。**范围冲突按细点区分（待对齐 T1）**：Media sets 与 Iceberg tables 各有独立需求点标「后续」；Virtual tables 与 Views 无独立后续条目，是路线图第一阶段细点「建议往后挪」；四者与「细点默认跟上级（P0）」的关系需产品/路线图裁决，本矩阵不代决。

**已有实现与证据**：
- A：Datasets——DVS 数据集/分支/事务/版本模型与租户隔离（P1 最终验收 [summary.md](../../tools/smoke/business-platform-p1/results/2026-11-13-build-http/summary.md)）；发布时版本/文件索引/血缘同事务原子写入 [publish.go:208](../../services/dataset-versioning-service/internal/repo/publish.go)。
- A：Builds——Build/Job 分库 HTTP 流程与取消/补偿（P1.5，同上 summary 9/9）。
- A（限定）：Schedules——仅 Connector REST snapshot 同步链的 cron 调度已验收（P2.4，见计划历史章节）；管道自身调度归 DI-03；ADR-0025 明确它不是通用调度框架。
- D（他组声明待对接）：Health checks 概念的数据侧基础「数据停更按店通知」（G-09）只有路线图「已有/理顺」标签，本仓未核对，不作为已验收证据；类型映射见 DI-04。
- B：Branching——DVS main/feature 分支读写（P1.5 验收含非 main 分支）；数据集分支与全局分支（BR-01..26，开发者工具链）的边界待对齐。
- B：Views——view/schema 元数据读写与按 ts/txn 取视图已实现（[views_schema_preview.go:116](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)、[parity.go:1266](../../services/dataset-versioning-service/internal/repo/parity.go)），DVS README「views 未做」的说法已过期；但 `RefreshDatasetView`（[parity.go:1236](../../services/dataset-versioning-service/internal/repo/parity.go)）目前主要刷新时间戳——有元数据接口不等于 SQL 物化。
- C：Virtual tables——连接注册查询会拼虚拟表响应（[registrations.go:457](../../services/connector-management-service/internal/handlers/registrations.go)、`:517`），无 `sample_rows` 时生成 `selector`/`row_number` 假行（`:528`），不读真实源。
- C：Iceberg——`services/iceberg-catalog-service` 有实质源码、ADR-0041 为设计决策，均未纳入本轮已核对验收链（现存设计/待接线，不等于上线；亦不断言全无相邻能力）。
- C：CDC——[applier.go:335](../../services/ingestion-replication-service/internal/reconcile/applier.go) 构建Debezium PostgreSQL 连接器的 Strimzi CR，未纳入本轮已核对验收链。
- D：Media sets——`services/media-sets-service` 有实质源码目录，未纳入本轮已核对验收链，接线与能力未核实。
- 后续（不在本期）：Streams。

**缺口**：T1 裁决（区分「独立后续」与「建议后挪」两类）；Views 物化语义；CDC 真实链路或明确后续；Media sets 与「CSV/JSON 结构推断不扩成 PDF/图片 Media sets」的边界（后者归 DI-03）。

**前置依赖/协作边界**：健康检查概念与可观测性组共用类型口径；Branching 与全局分支协作；Iceberg 关联 ADR-0029/0041 决策（架构组）。

**验收标准（将来，未勾选）**：
1. 11 个细点逐项落「照官方概念对齐」状态表：概念在代码/文档中的对应物、名称是否照官方、结论（本期/后挪/后续），经产品确认。
2. T1 裁决后：留在本期的细点各有可运行证据或验收记录；后挪/后续的细点在矩阵更新中有明确决定编号。
3. Views：按裁定的物化语义有真实查询验收（若裁定仅保留元数据接口+时间戳刷新，则写入状态表并如实表述）。
4. CDC：一条真实变更→数据集可见链路的端到端验收，或产品明确后续的记录。
5. Builds/Schedules 概念命名与官方对齐核对（Build/Job/Schedule 术语表落盘）。

## DI-02 Data Connection（连接目录与连接器）

**优先级/阶段**：P0 / 阶段 1（规模 L，12-07~12-14，「补齐」；24 细点）。已有基础：连接独立应用、统一连接目录（G-15，d740，他组规划声明）。

**本期范围**：统一连接目录与连接安全；本期连接器=电商三个（Amazon 经紫鸟、noon、领星）+通用**五个**（文件上传、REST、S3、PostgreSQL、MySQL）（产品 11-15 定）。细点裁决：Webhooks（4 页）本期 P1=动作触发后调外部系统的出站通道；Agents/agent worker、Listeners（HTTP/WS/Email）、Exports、OIDC、外部代码直连均后续；Legacy 文档不做。

**已有实现与证据**：
- A：连接目录与租户——Source/Sync/凭据的租户守卫（[handlers.go:212](../../services/connector-management-service/internal/handlers/handlers.go) tenantFromClaims、`:225` canReadSource、`:237` canWriteSource；P1.2/P1 验收链）；凭据加密与平台身份分离（[credentials.go:45](../../services/connector-management-service/internal/domain/credentials.go)；快照授权解析 `handlers.go:2354`）。
- A：REST——**唯一进入本轮 Source/Sync 快照发布执行链的源类型**（`handlers.go:2035-2036` 明确仅支持 rest_api；此限定不外推其他链路）：P1.3 真实分页同步→Dataset→下载、P2.4 定时、P2.5 断点、P2.6 幂等全部验收（入口见「来源」）。
- A：文件上传——DVS `POST /datasets/{rid}/upload`（[server.go:167](../../services/dataset-versioning-service/internal/server/server.go)）：CSV/NDJSON（P0 基线，验收记录见计划历史章节）与 Excel 导入（P3.1）。
- C：S3 源——[s3/adapter.go:5](../../services/connector-management-service/internal/adapters/s3/adapter.go) 自述不读对象 payload、`:168` StreamArrow 未实现、`:174` 仅返回 ingest descriptor。
- C：MySQL——[mysql/adapter.go:14](../../services/connector-management-service/internal/adapters/mysql/adapter.go) 标注 driver gap（走 HTTP bridge 无 native driver），`:107`/`:113` Arrow/IngestSpec 未实现。
- C/D：PostgreSQL——专用 adapter 未定位；存在 JDBC 泛型目录（`internal/adapters/jdbc/`）与 Debezium PG CDC 线索（见 DI-01 CDC 证据）。实际缺口=可执行服务端源读取的发布接线，盘点后定复用或新建。
- C：adapter 接线面——[main.go:106](../../services/connector-management-service/cmd/connector-management-service/main.go) 仅 `MustRegister("kafka", …)`；其余 adapter 源码均未在主服务注册，「源码存在≠可用连接器」。PG16 平台库迁移测试不等于 PostgreSQL 源连接器能力。
- D：电商三个（紫鸟/noon/领星）——本仓 services/libs/apps/docs 有限检索未定位专用实现，标「待映射/证据核实」，不推定跨仓没有。
- B：出站 Webhook——[registrations.go:806](../../services/connector-management-service/internal/handlers/registrations.go) 真实 `client.Do`（当前每次调用仅一次请求、无自动重试循环）+ 历史 + 429 限流，单测覆盖（handlers_test.go:2000/2147）；缺口：`:737` 用非租户 `GetConnection`、`:746` 广义 admin 判定——不满足 P1 已验收链的租户标准，未纳入租户验收。
- 后续（不在本期）：agent worker、Listeners 全家、Exports、OIDC。Email listeners 另有知识库注记：已有专属邮箱收单证（F091 故事 4），理顺时先与接入组核现有实现。

**缺口**：S3/PostgreSQL/MySQL 从「源码/线索」到真实源→发布端到端（复用现有 adapter 或补新实现由执行契约确定）；电商三个实现位置映射；Webhook 租户修复及与 Action 副作用的归属；虚拟表假行问题（随连接器补齐一并处理或明确后续）。

**前置依赖/协作边界**：出站 Webhook 是「动作类型的副作用」通道，与本体构建 Action types（Side effects 细点 P1）、用例开发 Automate 协作（T6）；连接安全与安全与治理组协作；电商连接器可能涉及桌面端取件（紫鸟），与用例开发组边界待映射。

**验收标准（将来，未勾选）**：
1. 每个本期连接器（REST/文件/S3/PostgreSQL/MySQL/紫鸟/noon/领星）各有一条真实源→Dataset 发布→下载端到端验收（口径同 REST 现有链：真实字节、行数、hash、租户隔离、失败不发布半成品），矩阵登记证据路径。
2. 三个电商连接器完成实现位置映射表（在哪个仓/哪个服务/缺口清单），或确认需新建。
3. 出站 Webhook：修复 `registrations.go:737`/`:746` 租户缺口后有跨组织拒绝的真实 HTTP 验收；超时/失败/历史/限流行为有回归（自动重试如要做，先另定副作用幂等合同）。
4. 连接器凭据永不进入日志/公开 JSON（沿用 P1.2 已验收口径对新连接器逐个复验）。
5. adapter 注册表与「本期连接器清单」一致：未注册/未完成的 adapter 在目录中不显示为可用类型。

## DI-03 Building pipelines（管道=研发代码）★

**优先级/阶段**：P0 / 阶段 1（规模 M，12-03~12-04，「补齐」，依赖 DI-01；12 细点）。做法与 Palantir 不同（d769，产品 11-13 定）：管道由研发写成代码、走评审、随版本发布；画布编辑不做。

**本期范围**：功能细点两个——Scheduling（管道定时）与 Pipelines on unstructured data（对 CSV、JSON 文件推断结构；不扩成 PDF/图片 Media sets）。Incremental 后续、Streaming 后续；Pipeline security 官方标签为**后续**（其中移除安全标记类 11-07 判不做：我们没有标记体系）——本期不扩该特性，但**既有租户隔离与鉴权是本模块全部任务的验收底线**，不因「后续」放松；Getting started/Best practices 为研发参考说明文档。

**已有实现与证据**：
- B：管道 IR——[pipeline_ir.go](../../services/pipeline-build-service/internal/models/pipeline_ir.go)（pipeline_ir.v1，含测试）。
- A：定义与构建链——JobSpec canonical 定义、DryRun、发布映射、Build/Job 生命周期（P1.5 验收 9/9、26 项 PG）。
- A：执行——REST→Dataset→FASTER 轻量管道→下载主链验收（P1 最终）；执行体 [lightweight_runtime.go](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go)。
- B：管道调度——[data_integration.go:199](../../services/pipeline-build-service/internal/handler/data_integration.go) RunDueScheduledPipelines、[repository.go:995](../../services/pipeline-build-service/internal/postgres/repository.go) ListDuePipelines/`:1020` UpdatePipelineNextRunAt、单测 data_integration_test.go:133（cron 重算 next run）。仅源码+单测，未做真实周期验收，不能用 P2.4 同步链的调度验收替代。
- C：变换执行面——lightweight_runtime.go:558 stack switch 无聚合/窗口；`:727` runJoin 无反连接（详见 DI-06）。
- C：CSV/JSON 推断——DVS CSV reader [file_rows.go:367-455](../../services/dataset-versioning-service/internal/handlers/file_rows.go) 固定首行表头/逗号 tab/BOM，非参数全集；管道侧 dataset_input_real_dvs_test.go:16 读的是已导入 NDJSON，不是 raw 文件推断。
- 待对齐 T2：路线图与源规划按 PipelineDef/TypeScript/DuckDB 描述（「管道写成代码（PipelineDef）已有」），本仓实际是 Go `pipeline_ir.v1` + local-table runtime；不自行换引擎/改语言，「研发写代码→IR」的交付契约需架构组与数据与接入组共同裁定。

**缺口**：调度真实周期验收（两次周期+重启+并发+身份+失败保旧）；CSV/JSON 推断参数化（分隔符/表头行/编码/嵌套 JSON 展开策略）；代码交付契约落地。

**前置依赖/协作边界**：依赖 DI-01 概念对齐；契约裁定涉及架构组；调度长期授权与 ADR-0025 边界（现 P2.4 arm 身份重启/过期需重授权——不等于长期后台身份或统一 Automate）。

**验收标准（将来，未勾选）**：
1. 管道调度：**至少两个自动到期周期**触发→Build→产物发布可核对（不以人工重复调用 run-due 冒充周期调度）；重启后按计划恢复；并发触发去重；身份失效可观察；失败不破坏旧产物（口径对照 P2.4/P2.5 已验收语义，但以管道自身链路执行）。
2. CSV/JSON 推断：多分隔符、无表头/指定表头行、嵌套 JSON 各有明确行为与单测；推断结果进 schema 可查。
3. 代码交付契约按 T2 裁决落地后：一条「研发代码→评审→IR→Build→产物」示范链路验收。
4. 不做画布编辑的边界保持：本项验收不新增任何画布编辑功能；租户/鉴权底线全项适用。

## DI-04 Maintaining pipelines（数据期望与健康检查）

**优先级/阶段**：P1 / 阶段 1（规模 S，01-25，「补齐」，依赖 DI-03；5 细点）。PL-26 更正：数据期望与健康检查照官方。

**本期范围**：Recommended health checks 与 Define data expectations 两个功能细点；其余三细点为说明文档。**类型口径待对齐（T3）**：产品 PL-26 写「6 类」，但官方 `health-checks/checks-reference/en.md:16-45` 按 Status/Time/Size/Content/Schema 五类列多种 check，data expectations 是独立定义——不发明 6 个名称，映射表与可观测性组共同确定。

**已有实现与证据**：
- B：[runners.go:297](../../services/pipeline-build-service/internal/domain/runners/runners.go) 健康检查枚举（当前仅 4 个）；`:457-476` evaluateCheck 只依据 `params.expect_passed`，不基于真实数据求值。
- B：runners_test.go:305 TestHealthCheckJobRunnerPostsQualityResult 仅证明上报形态。
- C：telemetry-governance 的 generic CRUD 不构成求值语义。
- D（他组声明待对接）：数据停更按店通知（G-09，可观测性 Data Health，路线图「已有/理顺」标签），本仓未核对。

**缺口**：类型映射；真实数据求值（行数/schema/空值/新鲜度等按官方 check 语义）；期望失败对构建的阻断语义。

**前置依赖/协作边界**：与可观测性组共用检查类型与通知口径；本项可先用 build 输入/输出合同落地，不等 DI-07 完整血缘应用链。

**验收标准（将来，未勾选）**：
1. 与可观测性组共同落「官方 check 类型 ↔ 本仓枚举 ↔ 通知渠道」映射表，经产品确认后实施。
2. 每类已落地 check 有基于真实数据的求值单测+一次真实构建集成验收。
3. 数据期望：input/pre 与 output/post 两个挂点、WARN/ABORT 两级处置、ABORT 后坏输出不可见（不发布）、结果绑定 build/version 可查，均有真实回归。
4. 现有 4 枚举与上报形态迁移不破坏（旧测试保持通过）。

## DI-05 Pipeline Builder Expressions（表达式库）★

**优先级/阶段**：P1 / 阶段 1（规模 M，02-01~02-02，「补齐」，依赖 DI-03）。官方 338 细点；本期按现有清单做，不做 338 全量（d769）。

**本期范围**：表达式库照现有清单核对+补缺，给研发写管道代码时调用；配置台不做表达式编辑。细点以官方全集为参照系，本期交付边界=冻结后的产品清单。

**已有实现与证据**：
- B：标量签名目录——`ScalarSignatureFor`（[catalog.go:75](../../libs/pipeline-expression/catalog.go)）。
- B：求值子集——[eval.go:462](../../libs/pipeline-expression/eval.go) evalCall 的 switch 仅覆盖部分函数（静态核对如 abs/lower/upper 等）；未知函数明确报错（保持该行为）。
- B：包内已有 haversine/type_inference 等单测（`haversine_test.go`、`type_inference_test.go`）。
- 关键事实：**签名目录 ≠ 可执行**——`ScalarSignatureFor` 声明的函数未必进了 evalCall 执行 switch；一致性待核对。（group_by/window/pivot 属 TransformSignature，归 DI-06。）

**缺口**：「现有清单」成文化并与代码对表；签名/推导/求值三层一致性核对与补缺。

**前置依赖/协作边界**：依赖 DI-03 契约（表达式如何被研发代码调用）；旧 P3.2 类型规则（decimal/日期/时区/空值/标识符）归入本项与 DI-06/DI-08 的解析需要项（见计划批次映射）。

**验收标准（将来，未勾选）**：
1. 冻结产品表达式清单，逐项落五列核对：签名 / 类型推导 / 实际求值 / 测试 / 别名映射；补缺项列清单。
2. 每个清单内表达式：预览与正式执行对同样本输出一致；覆盖 null、类型错误、数值与日期边界用例（不以一个单测通过算完成）。
3. 目录声明但不可执行的项在 UI/API 不显示为可用；未知函数明确错误行为保持（回归用例）。
4. 不新增配置台表达式编辑入口。

## DI-06 Pipeline Builder Transforms（变换）

**优先级/阶段**：P1 / 阶段 1（规模 S，02-05，「补齐」，依赖 DI-03）。官方 89 细点；本期=现有变换核对 + 补 3 项（11-07 判）。

**本期范围**：**目标仍为产品口径的 25 个现有变换**；本仓源码静态核对为 24 个静态目录项+动态 UDF 挂载——需建立 25→24+UDF 的逐项映射表（缺失/别名/重复），不能把验收清单直接缩成 24。新建 3 项：Aggregate on condition（按条件聚合）、Aggregate over window（窗口聚合）、Anti join（反连接）；先查当前代码避免重复实现（源码静态核对：三者在执行面均无实现；其语义依赖表达式求值，见 DI-05）。

**已有实现与证据**：
- B：静态目录——[transform_catalog.go:111-135](../../services/pipeline-build-service/internal/handler/transform_catalog.go) 24 项（select/drop/rename/cast/filter/formula/derive_column/normalize_columns/normalize_units/sort/join/union/haversine_distance/geo_intersection_join/geo_distance_join/geo_nearest_join/aggregate/explode/json_extract/csv_parse/gpx_parse/python_transform/llm_node/output_mapping），另有动态 PipelineFunction（UDF）挂载。
- C：planned 标记——formula/derive_column/normalize_units/sort/aggregate/explode/json_extract/csv_parse 等标 `planned`/`catalog_only`（`:313`/`:325`/`:348`/`:362`/`:530`/`:542`/`:553`/`:566`）——目录声明不等于可执行。
- C：执行面——lightweight_runtime.go:558 无聚合/窗口、`:727` join 族无反连接。
- C（关联）：`libs/pipeline-expression/catalog.go` 的 group_by/window/pivot 仅有 TransformSignature，无对应执行（归本项处置，不与 DI-05 标量混计）。

**缺口**：25→24+UDF 映射（可并行盘点）；目录-执行一致性；3 个新建变换的执行实现与端到端验收。

**前置依赖/协作边界**：依赖 DI-03（执行 runtime 与契约）与 DI-05（聚合/窗口中的表达式语义）。

**验收标准（将来，未勾选）**：
1. 25→静态 24+UDF 逐项映射表落盘（每项标注：同义/别名/缺失/重复/仅目录），经产品确认后作为验收清单基准；planned 项明确「本期补齐或移出目录」处置。
2. 3 个新建变换各有一条定义→Build→产物逐值比对的端到端验收，**同一样本的预览与正式执行输出逐值一致**，边界含：null 参与聚合、重复 key、反连接零匹配、窗口排序边界。
3. 动态 UDF 与静态目录的边界（何者可声明何者可执行）成文。

## DI-07 Data Lineage（数据血缘）

**优先级/阶段**：P1 / 阶段 2（规模 M，02-22，「新建」，依赖 DI-03；7 细点）。数据集到管道到对象到应用，改动前看影响面。

**本期范围**：全链血缘图谱与影响面；Understand and manage datasets 细点集中「血缘图里看预览、构建时间线、过期数据集」为 P2 子项；Understand permissions 后续（有标记体系后）。

**已有实现与证据**：
- B：服务——`services/lineage-service`：[server.go:88](../../services/lineage-service/internal/server/server.go) 触发血缘构建路由、handlers.go:117 起 handler 集。
- B：持久层——同一 main 接 PG（pgxpool，[main.go:78-117](../../services/lineage-service/cmd/lineage-service/main.go)），`lineage_nodes`/`lineage_relations` 有建表迁移与索引（[generalized_lineage.sql](../../services/lineage-service/internal/repo/migrations/20260425190000_generalized_lineage.sql)）——图谱数据有 PG 读写路径，**不得表述为整体不持久**；缺的是生产事件接线与全链重启验收。
- C：runtime read-model store 默认内存（main.go:68 runtime mode；`internal/lineagestore/` memory/cassandra 两种实现）。
- C：模型——[lineage.go:221](../../services/lineage-service/internal/models/lineage.go) NodeKind 仅 dataset/pipeline/workflow，**无 ontology/app 节点**，全链未成立。
- C：过滤——[filter.go:20](../../services/lineage-service/internal/lineage/filter.go) FilterGraphForClaims 是分类过滤；组织/项目隔离**未验收**，不因存在过滤代码视为已通过。
- B：前端——[LineagePage.tsx:444](../../apps/web/src/routes/lineage/LineagePage.tsx) 血缘图/影响面页面。
- A：发布侧写入——DVS 发布事务原子写血缘（publish.go:208；P1 链内血缘校验已验收）。

**缺口**：Ontology/App 节点与跨组资源/事件合同；生产事件接线；全链重启验收；branch/history；组织/项目隔离验收；「图里看预览/构建时间线/过期数据集」P2 子项。

**前置依赖/协作边界**：对象侧节点依赖本体构建 Indexing（同步入口记录数据集→对象映射）；应用节点依赖对象应用端；分支语义依赖全局分支（BR-01..26）；**不把延期的 Kafka/Iceberg 作为强制前置**。接口合同在批次 B/C 提前对齐，Ontology/App 全链在批次 E。

**验收标准（将来，未勾选）**：
1. 跨组 resource/event 合同（四类节点的事件来源与字段）评审通过并落 ADR 或接口文档。
2. 四类节点全链图端到端：一次真实数据变更后图上路径正确；影响面计算返回受影响下游清单。
3. PG 持久路径接入生产事件流；重启后全链图不丢（真实重启验收）。
4. 组织/项目隔离有跨租户拒绝的真实验收（对齐 P1 租户口径）。
5. branch/history 各有回归；P2 子项按产品节奏单独验收，不阻塞主链。

## DI-08 Dataset Preview（数据集预览与 Time Travel）

**优先级/阶段**：P1 / 阶段 1（规模 S，01-14~01-15，「补齐」，依赖 DI-01；4 细点：Overview、CSV parsing、Time Travel、FAQ）。

**本期范围**：看数据集内容、按版本回看；CSV 解析。官方 `time-travel/en.md:18` 能力面（单版本 data+schema、range diff、key matching、schema diff、query 随时间移动、bisect、retention）**未被产品裁剪——保留为整体目标，分批验收**，不默认不做。

**已有实现与证据**：
- C：预览参数——[views_schema_preview.go:423](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go) previewQuery 仅 limit/offset/format，无 version/ts 入参。
- B：Time Travel 原语——GetViewAt（views_schema_preview.go:116 + [parity.go:1266](../../services/dataset-versioning-service/internal/repo/parity.go) ts/txnID 取视图）已实现但未接到预览 API。
- C：前端——[DatasetDetailPage.tsx:687](../../apps/web/src/routes/datasets/DatasetDetailPage.tsx) 版本列表（VersionsTable），列表≠按版本回看。
- C：CSV——[file_rows.go:367-455](../../services/dataset-versioning-service/internal/handlers/file_rows.go) 固定首行/逗号 tab/BOM；官方 csv-parsing 细点的参数全集未覆盖。
- A：版本绑定下载（P1 smoke「版本绑定下载」场景）；A：Excel 导入数据集的 /preview、/schema、/files/rows 三端一致（P3.1 验收）。

**缺口**：预览 API 的 version/ts 参数映射；CSV 参数化；官方 diff/key matching/schema diff/query-over-time/bisect/retention 的分批清单。

**前置依赖/协作边界**：query 随版本移动依赖 DI-09 SQL；diff/retention 涉及版本保留策略（架构组）。

**跨期依赖**：批次 C 只交付历史版本 data/schema 回看与 CSV 基础；依赖 SQL 的能力（query 随版本、refine 等）在批次 F 随 DI-09 补验收；**DI-08 的所有保留能力验收完成前不得宣告整项完成**（非永久禁令：保留能力全部验收后即可关闭）；也不把整个 DI-09 提前或形成环。

**验收标准（将来，未勾选）**：
1. `?branch=…&version=…/ts=…` 预览与该版本真实字节逐值一致（含 GetViewAt 语义复用）；版本列表点击进入历史预览。
2. CSV 参数（分隔符/表头行/编码/BOM）可配并有默认值，**预览与执行使用同一份持久化版本配置**；各参数有单测。
3. 官方能力面落分批表（本批/随 DI-09 批/后续），经产品确认；range diff、key matching、schema diff、bisect、retention 各自定义可判定样本或映射（如 retention=按保留策略的历史版本读取与过期不可读）；已裁剪项写明决定编号。
4. 随 DI-09 批次：query 随版本移动等 SQL 相关预览有端到端验收。

## DI-09 SQL in Foundry（SQL 查数据集与本体）

**优先级/阶段**：P2 / 阶段 4（规模 L，05-14~05-21，「新建」，依赖 DI-01；14 细点）。d775 进本期；Ontology SQL 官方 11-12 正式上线，属于自由分析。

**本期范围**：**SQL 查数据集与本体，双侧均为整项目标，14 细点全部保留为参照**（Overview、SQL Studio、worksheets、Marketplace、console、permissions、Ontology SQL functions、dialect、outputs、Furnace、Ontology SQL、examples、Arrow Flight SQL、Compute usage）。实施顺序 Dataset 首批、对象随后；**只有新的产品裁剪记录才能删减双侧任一保留能力**，本文不代决。

**已有实现与证据**：
- B：[flightsql/server.go:481](../../services/sql-bi-gateway-service/internal/flightsql/server.go) 是**真 Arrow FlightSQL gRPC** 服务（不是 TCP 壳——旧 README 说法过期）；[delegate.go:19](../../services/sql-bi-gateway-service/internal/flightsql/delegate.go) 远程 delegate 查询为真实实现；round_trip_test.go:25 SELECT 1 往返测试。
- C：**本地**查询引擎——[query-engine/context.go:118](../../libs/query-engine/context.go) ExecuteSQL 仅支持 literal SELECT，RegisterBatch/RegisterParquet 均 unsupported——真实数据集注册不成立（此限定不涉及远程 delegate）。
- C：saved query——handler/handlers.go:108 附近 owner=nil/stub。
- 设计存量（不等于上线）：ADR-0009（DataFusion+FlightSQL 内查询织构）、ADR-0014（退役 Trino、FlightSQL only）、ADR-0029（为 Iceberg 分析重新引入 Trino）。

**缺口**：数据集（Parquet/Arrow 批）注册进本地引擎；Ontology SQL；权限；worksheet/console 前端与保存执行；配额与计量；saved query 持久化与租户。

**前置依赖/协作边界**：引擎选型受 ADR-0009/0014/0029 约束（架构组）；Ontology SQL 与本体构建/查询侧协作；DI-08 的 SQL 相关预览在本项补验收。

**验收标准（将来，未勾选）**：
1. 标准 FlightSQL 客户端（含至少一种官方 SDK/CLI）对真实数据集 SELECT 返回逐值正确结果：覆盖过滤、关联、聚合，多类型列、null 处理、绑定数据集版本。
2. Ontology 侧整项目标保留：对象查询最小链路（过滤/关联/聚合）或产品裁剪记录，二者必有其一才能关闭对应细点。
3. worksheet/console：保存后重开可执行同样查询；权限按项目/角色（跨组织拒绝）；配额与取消可观察、审计可查。
4. 14 细点逐项对应「范围+验收/明确裁剪记录」；不以 SELECT 1 往返或 SQL 文本能保存代替查询能力验收。

## DI-10 Pipeline Builder（画布只读）★

**优先级/阶段**：P2 / 阶段 1（规模 S，02-19，「理顺」；14 细点）。d769：配置台画布只留着看（节点图、节点预览、构建详情），编辑不再加；撤销重做/复制粘贴/自动布局等后放。

**本期范围**：收敛为只读查看：Overview/Core concepts/Navigation/Tips 为说明；Functions Index、Input datasets、Transforms、Pipeline outputs 四功能细点「已有理顺」；Pipeline management、Schedules、Data expectations、Marketplace 等画布编辑细点按 d769 后续/不做；Branches 细点不做（管道分支归全局分支，11-07 判）。

**已有实现与证据**：
- C：[PipelineCanvas.tsx:87](../../apps/web/src/lib/components/pipeline/PipelineCanvas.tsx) 默认 `readOnly = false`；`:150` selectNode 与 readOnly 耦合——直接改默认值会连带禁用节点选择。
- C：[PipelineEditPage.tsx:807](../../apps/web/src/routes/pipelines/PipelineEditPage.tsx) 编辑页仍渲染画布且保留节点/Join/Union 修改、TransformStackEditor/JoinEditor、JSON/Save 等编辑入口。
- D：Functions Index/Input datasets/Transforms/Pipeline outputs 四细点路线图判「已有」，本轮未逐页核验完成度（批次 D 先核对再收敛）。

**缺口**：readonly 策略落地（保留节点选择/节点预览/构建详情，普通用户无编辑模式）；与研发代码发布路径的关系保持。

**前置依赖/协作边界**：只读收敛以批次 B2 研发代码发布路径已可用为前提；不删除研发发布 API；UI 负责人对界面收敛方案确认。

**验收标准（将来，未勾选）**：
1. 普通配置台用户进入画布**无编辑入口，且网络层无相关 mutation 请求**（可抓包核对）；编辑能力可门控保留仅用于研发发布链路，不给普通用户留可编辑模式。
2. 节点图、节点预览、构建历史/详情在只读模式全部可用（含节点选择）。
3. 不做撤销重做/复制粘贴/自动布局（出现即违规）；现有代码发布 API 保持可用（回归证明）。
4. 四个「已有」功能细点核对表落盘（页面存在/内容与官方概念对齐）。

## DI-11 Microsoft Excel（Transforms Excel Parser）

**优先级/阶段**：P2 / 阶段 1（规模 S，02-08~02-16，「补齐」，依赖 DI-01；1 细点：Transforms Excel Parser）。

**本期范围**：解析 Excel（管道变换），卖家报表大多是 Excel 或 CSV。**与 P3.1 已交付的普通上传导入是两回事**。官方格式面覆盖 xls/xlt/xltm/xltx/xlsx/xlsm（xlsb 不支持）及 `transforms-excel-parser/en.md:18-30` 的文件数据集输入、多 sheet/多输出、非表格抽取、errorDataframe、跨文件 schema——**均未被产品裁剪，保留为整体目标差距**；分批交付需产品明确清单（T5），禁止单方面把整需求缩为单 sheet/单格式。P3.1 的单 xlsx、≤1000 数据行、单 worksheet 是**已验收子能力的限制，不自动成为整需求的永久边界**。语言运行时按本仓代码管道契约，不引 Java/Spark 依赖。

**已有实现与证据**：
- A：普通上传导入——DVS [xlsximp/parser.go](../../services/dataset-versioning-service/internal/xlsximp/parser.go)（纯标准库有界 OOXML 解析，parser_test.go），多 sheet 中文工作簿真实 excelize 验收（P3.1；验收记录见计划历史章节，commit `664165f3` 为代码定位）。
- C/D：管道变换——raw workbook 作为管道输入的解析变换**待接线/待核实**：有限检索未定位实际生产入口（dataset_input_real_dvs_test.go:16 读的是已导入 NDJSON，不是 raw workbook）。
- C：连接器侧 [adapters/excel/](../../services/connector-management-service/internal/adapters/excel) 源码目录存在、未注册接线（同 DI-02 接线面）。

**缺口**：raw workbook 管道输入链路；多格式/多 sheet/多输出/错误输出的分批范围裁定。

**前置依赖/协作边界**：依赖 DI-03 契约与 DI-06 变换框架；P3.1 OOXML 解析器可复用性由实现时评估（不预设）。

**验收标准（将来，未勾选）**：
1. 管道直接以 .xlsx 数据集为输入完成解析→变换→发布端到端验收（与上传导入路径并存且可区分）。
2. T5 裁决后分批清单逐批验收：官方格式集（xls/xlt/xltm/xltx/xlsx/xlsm）、多 sheet、错误行/errorDataframe、非表格抽取、跨文件 schema 至少各有「本批做/后续」的明确记录。
3. 大文件/损坏文件的拒绝行为明确（沿用 P3.1 有界口径）。

## DI-12 S3-compatible API for Foundry datasets

**优先级/阶段**：本期做·**优先级未定**（路线图暂按 P2 排，05-10~05-11）/ 阶段 4（规模 M，「新建」，依赖 DI-01；官方单页）。外部系统按 S3 协议读写数据集，有对接需求时。

**本期范围**：对外 S3 兼容端点。官方 `foundry-s3-api/en.md:18`：协议动作子集、RID/bucket 映射、path/key、branch/committed 事务、自动事务、凭据、multipart。**三件事严格区分**：S3 作为源连接器（DI-02）、内部对象存储/presign、对外 S3 兼容 API（本项）——前两者不能替代本项验收。普通 Dataset 的 S3 API **不依赖 Iceberg 上线**。

**已有实现与证据**：
- D：有限检索未定位对外 S3 协议处理（List/Head/Get/Put/Delete/SigV4/STS）与验收记录；不推定全仓绝无，批次 F 实施前先全仓盘点，盘点后才能确定需新建的范围。
- 设计存量：ADR-0041 Iceberg catalog（S3 相邻能力）待接线，不等于本项、也非其前置。

**缺口**：待盘点确认后定（动作子集、认证 profile、RID/bucket 映射、事务语义、multipart）。

**前置依赖/协作边界**：优先级待产品定（T4）；认证与安全组（凭据/签名/STS）、edge-gateway 暴露面协作；与 DI-01 Iceberg 细点裁决联动但不互为前置。

**验收标准（将来，未勾选）**：
1. 批次 A 裁定优先级与认证 profile 后：官方支持动作逐项映射并验收 List/Head/Get/Put/Delete 及 multipart（对应原文的支持子集，无需全 AWS API）；首批交付 List/Get/Put **不是全部目标**，其余动作在整项关闭前补齐或有裁剪记录。用标准 AWS SDK/CLI 验收（不用内部 presign 或 curl 仿造替代）。
2. RID/path/branch 语义：branch/committed 读取已提交视图、历史 txn 只读、写产生新事务，与 DVS 语义一致的真实回归；自动事务失败不提交。
3. 凭据/签名（SigV4/STS profile）与租户拒绝各有真实用例。

---

## 跨期依赖与分阶段验收（防环、防提前宣告）

- DI-08 ↔ DI-09：Time Travel 的 query 随版本移动需要 SQL。批次 C 交付历史 data/schema 回看与 CSV 基础；SQL 相关能力批次 F 随 DI-09 补验收；DI-08 所有保留能力验收完成前不宣告整项完成，也不把整 DI-09 提前。
- DI-07：接口合同批次 B/C 提前对齐；Ontology/App 全链放批次 E。
- DI-04：先用 build 输入/输出合同落地，不等完整血缘应用链。
- DI-10：只读收敛以批次 B2 研发代码发布可用为前提；保留节点选择/预览。
- 以上均为分阶段验收，整体目标不变、不隐式删减。

## 待对齐项（不擅自决策；均只阻塞各自受影响工作，不构成批次 B 的整体前置）

| # | 事项 | Owner | 影响需求 | 未决期间可推进 |
| --- | --- | --- | --- | --- |
| T1 | Core 细点继承冲突：Media sets/Iceberg 有独立「后续」条目；Virtual tables/Views 无独立条目、属路线图「建议后挪」——与「细点默认跟上级 P0」的关系需裁决 | 产品 + 路线图（数据与接入组复核） | DI-01、DI-12 | Datasets/Builds/Schedules/Branching/CDC 细节、Views 元数据现状核对 |
| T2 | 管道代码交付契约：源规划 PipelineDef/TS/DuckDB vs 本仓 Go pipeline_ir.v1/local-table（不自行换引擎/改语言） | 架构组 + 数据与接入组 | DI-03/05/06/11 | 调度真实周期验收、表达式/变换执行核对（不依赖契约终稿） |
| T3 | 健康检查类型映射：PL-26「6 类」vs 官方 checks-reference 五类 + expectations 独立 | 数据与接入组 × 可观测性组 | DI-04、DI-01 | input/pre+output/post 合同、求值框架 |
| T4 | S3 兼容 API 优先级（路线图暂按 P2） | 产品 | DI-12 | 动作子集与认证 profile 草案、全仓盘点 |
| T5 | Excel Parser 分批范围（格式集/多 sheet/多输出/错误隔离等未裁剪） | 产品（数据与接入组起草） | DI-11 | raw workbook 管道输入链路 |
| T6 | 出站 Webhook 归属（Data Connection 细点 vs Action 副作用）与租户修法 | 数据与接入组 × 本体构建/用例开发 | DI-02 | registrations.go:737/:746 租户修复 |
| T7 | 电商连接器（紫鸟/noon/领星）实现位置映射 | 数据与接入组（Allen） | DI-02 | 通用连接器补差不依赖此项 |

## 跨模块协作边界

- **本体 Indexing（对象侧同步入口）**：DI-01 数据集概念的消费方；DI-07 对象节点的数据来源；对外叫法照官方、内部实现走自研同步入口。
- **对象应用端（Workshop/Object Views/Object Explorer）**：DI-07 应用节点；DI-09 Ontology SQL 的查询对象。
- **可观测性（Data Health G-09、Health checks 平台）**：DI-04 类型映射与通知渠道；DI-01 健康检查概念。
- **全局分支（BR-01..26，开发者工具链）**：DI-01 Branching 与管道分支归属（画布 Branches 细点不做）；DI-07 分支血缘。
- **安全与治理（Concepts：项目/角色/标记）**：各 DI 资源的项目/角色权限统一模型；受限视图后续联动 DI-01 Views。
- **Action 出站调用（本体构建 Side effects、用例开发 Automate）**：DI-02 Webhooks 是其通道之一（T6）。

## 证据入口清单（不扫描 results 大目录，只引以下入口）

- P1 最终：[tools/smoke/business-platform-p1/results/2026-11-13-build-http/summary.md](../../tools/smoke/business-platform-p1/results/2026-11-13-build-http/summary.md)
- P2 最终：[tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/FINAL-BRIEF.md](../../tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/FINAL-BRIEF.md)（及同目录 seal/terminal）
- P3.1：本文档「来源」节 + [business-platform-implementation-plan.md](business-platform-implementation-plan.md) P3.1 章节（commit `664165f3` 为代码定位）
- 各 DI 节内 file:line 定位（2026-11-16 静态复核；后续代码演进请随验收更新）。
- 存放说明：P1/P2 的 results 归档为本机工作树文件、非 Git 跟踪（fresh clone 不自带，获取位置即上述本机路径）；源码/测试/fixture/验收工具脚本在 Git 内。
