# 通用业务平台数据模块分阶段实施计划

更新日期为 2026-11-17（本轮登记：S1 工程契约 v1 冻结、WP2.1/WP2.2 安全修复工程验收通过，见文末「当前进度登记」与「当前证据位置」；此前更新 2026-11-16）。以多个客户共用平台、客户数据和业务配置分别管理为产品前提。

## 产品目标与实施约束

平台提供数据接入、版本化数据资产、处理、质量检查、业务对象及动作执行能力。电商与 B2B 贸易通过行业方案复用公共能力，客户通过数据源绑定、字段映射、业务规则和权限配置使用方案。

**本模块（数据连接与集成）交付范围限定为本期 12 项需求（DI-01..DI-12）**，需求清单、优先级、路线图阶段、已有实现证据分级与逐项验收标准见[数据连接与集成需求覆盖矩阵](data-integration-requirements-coverage-matrix.md)。范围依据：知识库 `_index/裁剪汇总.md`（数据连接与集成 12 项，227-238 行原始顺序）、`_index/路线图.md`（阶段 1 九项、阶段 2 血缘一项、阶段 4 SQL 与 S3 API 两项）、`_index/裁剪判据.md`（定稿 v4）。此前 P3 剩余、P4、P5 的通用平台任务不再作为默认主线，映射说明见文末批次计划；平台其余模块目标不因本计划改变。

继续迭代 OpenFoundry，复用 Source、Sync、Dataset、Pipeline、Ontology、Action 和应用模板。按实际可运行路径交付。编码、构建与测试通过 ZCode GUI 的 GLM-5.3 执行，由 Codex 审查实现、维护阶段状态并核对证据。每个 GLM 会话以低于 60% 上下文为目标，70% 为上限，切换会话时传递范围、改动和未决问题。

基线与各阶段成果均已提交 Git：P0–P2.6 代码 checkpoint 随 `e4172624` 提交、验收工具与可提交夹具随 `2128e55d` 提交（`tools/smoke/**/results/` 归档为本机工作树文件、非 Git 跟踪，fresh clone 不自带，验收结论有效性与获取位置见文末「当前证据位置」），P3.1 随 `664165f3` 提交；HEAD `664165f3`、分支 `codex/dataset-pipeline-file-io`、工作区干净为**本次规划调整前的起点**（历史快照）。早期基线验收（151 行 CSV 输入、148 行 NDJSON 输出、两次完整下载、NDJSON 回读、main 分支读取、文件输入的非所有者拒绝，以及两个服务的 Go 测试和 PostgreSQL 16 迁移测试）的记录见下文历史章节与本仓 `tools/smoke/` 归档。

租户从验证后的 JWT OrgID 推导。请求参数及转发头不能自行扩大数据权限。无 OrgID 的旧数据保留个人域兼容，归属由原 owner 约束。旧数据转入组织必须有明确迁移过程。组织管理员的资源访问仍受所属租户约束。

## 历史阶段与验收状态（截至 P3.1；当前执行顺序以矩阵优先，见文末批次计划）

| 阶段 | 结果 | 任务 | 验收门槛 | 依赖与状态 |
| --- | --- | --- | --- | --- |
| P0 | 基线与任务契约 | 保留文件 I/O 验收记录，记录租户兼容策略和数据发布语义 | 原 CSV→Pipeline→下载回归通过，实施范围和未验证项可查 | 基线已完成，计划已建立 |
| P1 | 租户隔离和真实同步首轮 | P1.1–P1.5 | 两个组织隔离，通过真实分页 REST 获取数据并发布可下载 Dataset，Pipeline 能消费产物 | 已完成当前范围：60 包单测、26 项必需 PG16、HTTP 9/9、零未覆盖；含 Build/Job 分库流程及文件卷重建/重启回归 |
| P2 | 可恢复的异步同步 | 任务租约、取消、重试、定时调度、增量断点、幂等发布和故障恢复 | 重启后恢复，重复投递无重复发布，发布失败不推进断点，任务超时与取消可观察 | P2.1–P2.6 已完成当前范围；P2.6 验收8/8，最终封存、完整独立离线复验与三处临时副本清理均已通过，P2 当前范围完成 |
| P3 | 文件和外部推送扩展 | Excel、多文件解析配置、标准 push/manifest、爬虫结果接入、质量规则 | 多 sheet 和类型保真，错误行可追溯，外部任务重传可恢复，全部经过相同租户与发布边界 | P3.1 已完成（后端/网关/前端/真实浏览器验收）；P3.2 及之后未启动，已被 12 项新计划取代（类型规则归 DI-05/DI-06/DI-08；push/listeners/爬虫非默认主线，见文末批次计划） |
| P4 | 正式批量处理与查询 | 对象存储、批次读取、产物 manifest、输入版本固定、查询引擎及统一发布 | 首档十万行多文件基准全量正确，输入固定可重现，内存有界，失败不暴露半成品，容量记录可复查 | 依赖 P2/P3，待实施；已被 12 项新计划取代（查询并入 DI-09；对象存储配置与授权、分批读写、manifest 提交、固定输入快照、schema/逻辑版本关联、真实查询引擎、统一多输出发布与容量基准按[矩阵](data-integration-requirements-coverage-matrix.md)「旧 P4 逐项映射」逐项承接，不笼统交 T1/T2） |
| P5 | 双行业与客户配置复用 | 电商方案、B2B 贸易方案，版本化模型/映射/流程/页面，客户安装与升级 | 同平台运行两个行业、两个客户，复用内核，分别绑定数据源与配置，独立升级与审计 | 依赖 P1–P4，待实施；行业方案转为跨组验收用例，不再作为本模块默认主线（见文末批次计划） |

P1 完成意味着当前接入与处理路径具备已验收的隔离及真实数据同步。全平台的 Ontology、App、搜索、日志、工作流和管理员边界需逐项纳入后续租户验收矩阵。

以下各阶段的历史进度、失败及“下一项/待执行”叙述保留原时点含义，不替代本页阶段表与 P2.6 最终收尾的当前结论；当前与后续执行顺序以[数据连接与集成需求覆盖矩阵](data-integration-requirements-coverage-matrix.md)及其批次计划（见文末）为准。

## 2026-11-13 中断后进度核查（恢复前快照）

本次核对了旧 Codex 对话、四个 ZCode GLM-5.3 会话的最终报告、当前工作区代码和本机证据文件。旧对话以 `Bad Request` 失败，随后一次“授权继续”也未执行工作；三个实现会话后来均留下了完成报告，验收会话停在“等待代码就绪信号”。代码仍在 `codex/dataset-pipeline-file-io`，HEAD 为 `d7c40cf5`，本轮及上一轮文件 I/O 改动均尚未提交。

以下“测试通过”来自已有 GLM 会话报告；本次仅做只读代码核查，没有重新构建或运行测试。验收标准仍保留未勾选，待修复与普通 HTTP 联调后逐项确认。

| 任务 | 已实现 | 已有验证记录 | 当前状态 |
| --- | --- | --- | --- |
| P0 基线 | CSV→Pipeline→下载、NDJSON 回读、非 owner 拒绝及分支读取 | 本机 `openfoundry-file-io-sryufy3u/result.json` 仍在：151 行输入、148 行输出、两份快照 | 已完成；该结果属于上一轮，不能替代当前改动后的回归 |
| P1.1 Dataset | JWT OrgID 归属、个人域兼容、租户化查询与资源守卫、输出创建、view/schema 父子校验及存储引用检查 | Dataset 会话报告 8 包单测通过、PG16 两项迁移/隔离集成测试通过、vet 通过 | 候选实现完成；纯 UUID 血缘引用检查仍有缺口，双组织 HTTP 验收未做 |
| P1.1 Pipeline | 租户字段与迁移、路由 scope、Pipeline/运行/构建/日志查询隔离、调用者 Bearer 转发 | Pipeline 会话报告 15 包单测通过、PG16 空库/旧库迁移及隔离两项测试通过、63 条业务路由鉴权审计通过 | 候选实现完成；版本恢复参数有误，Dataset 直查等路径待收口，三服务联调未做 |
| P1.2 Source/Sync | Source/Sync/运行/凭据的租户字段、查询谓词及权限守卫 | Source/Sync 会话报告构建、vet、internal 下 37 包单测及 handlers/runtime race 通过 | 已编码；SQL 别名缺陷阻断真实数据库访问，尚无 PG 迁移实测 |
| P1.3 REST 同步 | page/cursor/none 分页、有界拉取、真实 NDJSON/hash/行数/字节数、空快照、凭据分离、调用 Dataset 提交后才成功 | 单测覆盖内容变化、分页/解析/发布失败、空源及 JWT 转发 | 候选实现完成；真实 REST→Dataset→Pipeline→下载尚未跑通验收 |
| P1.4 验收 | `tools/smoke/business-platform-p1/` 的 9 个脚本/配置文件与 7 个 smoke 场景齐备 | 验收会话报告 shell/Python 语法检查与合成源本地自测通过 | 仅脚本准备阶段完成；正式契约未对齐，setup/up/smoke/统一 tests/migrate-tests/restart 未执行 |
| P2–P5 | 本轮未进入这些阶段 | 无本轮交付证据 | 待实施 |

### 联调前必须处理的事项

1. **Source SQL 别名（已确认）**：`services/connector-management-service/internal/repo/repo.go` 的 `connectionTenantPredicate` 生成 `c.org_id`/`c.owner_id`，但 `connectionSelect`、`UPDATE connections` 和 `DELETE FROM connections` 未声明 `c`。Source 列表/详情/修改/删除及依赖这些查询的 Sync、凭据入口会在真实 PostgreSQL 报错；现有 handler mock 测试未覆盖此问题。
2. **Dataset UUID 血缘引用（静态核查发现，待回归实证）**：`internal/repo/tenant.go` 的 `ResolveDatasetForTenant` 在纯 UUID 分支对不可见目标返回 `(nil, nil)`，`internal/handlers/tenant.go` 的 `validateLineageTargetsInTenant` 仅检查 error。应统一不可见引用的返回语义或显式拒绝 nil，并补充纯 UUID 的跨组织/不存在引用用例；已有血缘测试只覆盖完整 RID。
3. **Pipeline 恢复发布（静态核查确认，待回归实证）**：`internal/postgres/pipelines.go` 的 `RestorePipelineVersion` 复用按 draft 参数生成的租户 WHERE。非 draft 分支的 `$8` 已用于 `restoredVersion.ID`，租户实际位于 `$9`，需分别生成条件并覆盖组织域和个人域的恢复发布测试。
4. **Smoke REST 契约与分页（已确认）**：`smoke/smoke.py` 仍使用平铺的 `sync.path/items_path/page_size/max_pages`。最终实现需要 `resource_path`、`records_path`、嵌套 `pagination` 与 `limits`；合成源的 `src` 参数应放入 `query_params`。实现的 page 模式读到空数组才结束，但 fixture 越过末页返回 400；需对齐终止语义或正确使用 cursor 模式。当前脚本不能直接用于证明真实同步通过。
5. **Smoke 旧 CSV 回归（已确认）**：`upload_csv` 只上传 `sku,qty` 两列，`scenario_legacy_regression` 却将输出与含名称、价格等全部字段的 `expected_records` 比较；必须对齐输入字段与期望输出，才能得到有效的逐值回归结果。
6. **测试驱动（已确认）**：`tests.Dockerfile` 在镜像构建的 `RUN` 中执行单测/集成测试，未配置运行时测试命令，而 `run.sh` 随后调用 `compose run`；compose 也仅在运行时设置 `DATASET_DB_DSN`，无法供构建阶段集成测试使用。迁移任务只选了 Dataset 的 `TestMigrations`，未包含新增的 Dataset/Pipeline 租户集成测试，Source 的 PG 迁移验证也待补。
7. **其他边界（实现会话已列出）**：Pipeline 的 `OpenTransaction`/`ViewSchema`/`ListBranches` 直查 Dataset 表、个人域不同 owner 的 spec 同 key upsert，以及 `GetSparkRun` 的 Kubernetes 直查，仍需按 P1 范围复核并记录验收结论。Source 的目标 Dataset 归属目前在发布时裁决，创建时未检查，联调需验证失败不会改变目标。

### 恢复执行位置

沿用 ZCode GUI 的 GLM-5.3 编码和执行约定：先修复上述实现缺口并对齐验收脚本，再通知验收会话进入第二阶段，依次执行隔离环境启动、7 场景 smoke、三服务测试、PG16 空库/旧库/重跑迁移验证与服务 restart 回归。保存实际 `result.json` 和脱敏日志后，再更新各项验收勾选并决定 P1 是否完成。

核查时默认目录 `/private/tmp/openfoundry-platform-p1-q3v9` 尚不存在，也没有该目录下的 P1 `result.json`。目前可读取的端到端结果仍只有上一轮文件 I/O 基线。已有 GUI 测试报告是恢复工作的线索，尚未形成仓库内可复查的完整 P1 交付证据。

## 2026-11-13 授权恢复后的执行进度

编码、构建与测试继续由 ZCode GUI 的 GLM-5.3 执行，Codex 只读审查业务代码、核对实际日志并维护本计划。以下服务修复记录由最终统一验收补证，结论限定于 Source/Sync→Dataset→FASTER 轻量 Pipeline→下载主链；未覆盖项另列。

- Dataset：修复纯 UUID 血缘引用，前置方向校验；版本/head/文件投影/索引/血缘及响应读取纳入同一数据库事务。8 包单测通过，PG16 三项集成通过。完整 handler 故障注入证明索引、血缘和响应读取失败均保留 v1 数据库状态，失败事务最终 ABORTED；下载字节与 hash 的证据由后续 smoke 补充。
- Pipeline：修复 UUID/RID 查询 OR 条件、恢复发布参数、个人 owner 的 spec 唯一边界与创建者归属，以及 Spark 映射查询和跨域冲突。15 包单测通过，PG16 三项集成通过。
- Source/Sync：修复 SQL 别名与扫描列数；创建、修改、运行均以调用者 JWT 预检 Dataset，修改先检查 Source 写权限，运行在拉取前再次校验；运行错误移除 URL 查询串及 userinfo。37 包单测通过，PG16 空库/旧库/重跑及 CRUD 隔离三项集成通过，handlers/runtime race 通过。
- 验收：已对齐真实 REST 配置、空源/循环分页、版本绑定下载、内容 hash/字节/行数核对、旧快照保护和运行时测试 runner；统一测试、九项必需 PG16、重启后完整 smoke 与汇总均已实际执行。

当前能力边界：正向验收使用 REST→Dataset→FASTER 轻量 Pipeline→下载。带可信 TenantScope 的分布式 Spark 提交在产生 Kubernetes 副作用前返回明确 503，受约束执行身份在后续阶段实施；分库部署中的旧 CreateBuild/DryRunResolve 共库 Dataset 解析仍明确拒绝未具备租户能力的调用。Build/Job 的数据库隔离已由真实 PG16 UUID/RID 回归证明，普通 HTTP 正向构造的覆盖情况由验收报告单列，不能据此宣称全平台已完成租户化。

当前可读取的服务修复日志在 `/private/tmp/openfoundry-p1-repair-evidence/{dataset,pipeline,connector}/`；隔离验收工作目录为 `/private/tmp/openfoundry-platform-p1-q3v9`。全部工作仍未提交 Git，原有文件 I/O 改动继续保留。

### HTTP 验收结果与额度恢复后的最终验证

最终使用 `http://fixture:8090` 主机名执行的完整 HTTP 结果为 **7 pass、0 fail、1 uncovered**，运行 suffix 为 `c12cad5c`，开始时间为北京时间 2026-11-13 17:55:59。真实结果和控制台断言已保存到仓库中的 `tools/smoke/business-platform-p1/results/2026-11-13-http-review/{result.json,smoke.log}`。

- 双组织 Dataset/Pipeline/Source/Sync 隔离、管理员边界、伪造身份、文件父子错配及旧 CSV 回归通过。
- REST page/cursor 两种分页均下载到 151 条逐值一致的记录，前导零 SKU 保留；Pipeline 过滤后 148 条逐值核对通过。
- 同步下载为 22,766 字节，文件的事务 ID 与 run 的 `dataset_version_id` 一致；下载 SHA-256 与 run hash、事务 metadata hash 一致。同配置下修改源内容后 hash 改变。
- 第二页 500、无效 JSON、cursor 循环和页数超限均明确失败，未发布不完整产物；空源成功发布可下载的零行零字节文件。
- 通过当前隔离 PG 的临时 CHECK 注入真实 POST 发布 500，确认 run 失败且已记录抓取 hash，无新版本引用，分支 head、索引、血缘和旧下载字节不变；finally 移除约束后同步恢复成功。删除目标另列为预检失败，未混作发布失败证据。
- 未覆盖项是分库部署下旧 Build/Job HTTP 正向构造：前置返回 `503 dataset_tenant_boundary_unsupported`，未用其他 500 冒充边界；UUID/RID 数据库隔离由独立 PG16 测试覆盖。分布式 Spark 执行也继续明确不支持。

联调追加修复均由 GLM 完成并验证：Source `egress` 配置保留并传入同步运行，显式 HTTP 例外可用，Source 不能扩大全局主机/私网限制；Build 的 nil fallback 默认空数组，CreateBuild/DryRunResolve 在写库或解析前检查分库租户能力。三个服务均有对应单测及 PG16 实际通过记录，新增回归覆盖这些修复。

ZCode 随后显示：“已达到 5 小时的使用上限。您的限额将在 2026-11-13 22:12:38 重置。” 因用户要求编码和执行继续通过 ZCode GLM-5.3，Codex 没有代跑剩余检查。已请求用户选择保持 GLM、处理额度，或明确授权 Codex 执行余下测试。

2026-11-13 18:16 后，用户确认额度已重置并授权继续。GLM 会话恢复后确认四个端点健康，并完成以下统一验收；编码和执行仍全部通过 ZCode GLM-5.3。

| 最终检查 | 实际结果 | 仓库证据（相对 `tools/smoke/business-platform-p1/results/2026-11-13-p1-mainline-baseline/`） |
| --- | --- | --- |
| 三服务统一单测 | 60 包成功：Dataset 8、Pipeline 15、Connector 37；0 FAIL、0 panic | `logs/unit-tests.log` |
| 同一隔离 PG16 集成 | 九项必需用例全部有 RUN 与 PASS，无必需 SKIP；空库、已有库、迁移重跑、隔离与原子发布故障均覆盖 | `logs/migrate-tests.log` |
| 重启回归 | 三服务在原数据卷重启并健康；18:30:05 开始的完整 HTTP suffix `b0e0bcf8` 为 7 pass、0 fail、1 uncovered | `logs/restart-health.log`、`result.json`、`logs/smoke.log` |
| 旧产物持久性 | 重读重启前 `c12cad5c` 的 Dataset、文件与事务：HTTP 200，22,766 字节 SHA-256 及事务 metadata hash 与重启前一致 | `logs/restart-persistence-probe.json` |
| 必测守卫 | 修复 Go 输出 SKIP 的空格匹配并要求顶层 PASS；缺失、FAIL、SKIP、RUN 无终态、PASS、名称边界的合成日志自测符合预期 | `logs/guard-selftest.log` |
| 汇总交付 | 摘要、原始结果及脱敏日志已落盘；未复制 `.env` 或密钥 | `summary.md` 及 `logs/` |

另有五项可选集成测试因 AIP/LLM provider、分布式集群或真实 Python sidecar 未配置而跳过，已在摘要逐项列出，不计入本轮已验证能力，也不冒充九项必需 PG16 通过。

**关闭判定：**本轮剩余执行和 P1.4 收证工作已完成，当前 REST→Dataset→FASTER→下载主链通过。仍保留一项旧 Build/Job 的跨组织 HTTP 正向构造与访问未覆盖；随机 UUID 的 404 只作为未知资源入口检查，不能替代该缺口。分布式 Spark、旧 resolver 的分库 HTTP 适配及其他平台模块租户矩阵仍待后续实施，因此不声明整个 P1/全平台已经全量完成。P2–P5 未开始。

复跑仍使用 `run.sh tests → migrate-tests → restart → smoke → summarize`。隔离栈保留用于复查，代码、计划和证据均尚未提交 Git；`go.mod`/`go.sum` 未改动。

## P1.1 数据资产和处理资源的租户隔离

### 结果

Dataset、文件、事务、版本、Pipeline 和运行记录均按受信任租户身份访问。相同用户切换到另一个组织后，原组织资源继续受到隔离。

### 任务

- 为当前资源增加可持久化的组织归属，并提供可重跑的增量迁移。
- 创建时由认证上下文赋值。列表、详情、更新、删除、预览、文件下载、产物提交、运行与重试统一执行资源归属验证。
- 子资源按其父资源执行隔离。跨 Dataset 的输入、输出与血缘引用也要校验归属。
- 保留无 OrgID 的个人域行为，旧数据继续可被原 owner 正常访问。组织请求不可自动认领旧数据。
- 请求发起的 Pipeline 继续转发调用者身份，后台任务保存所属租户并使用受约束身份执行。

### 验收标准

- [x] 组织 A/B 使用相同用户、相同资源名称和业务编号，各自正常创建、读写和执行。
- [x] 列表、按 ID 访问、下载、版本/事务、预览、运行、输出提交均拒绝跨组织资源。
- [x] 修改 owner_id、组织头或请求体 tenant 字段无法扩大访问范围。
- [x] 组织管理员跨租户访问被拒。
- [x] 无 OrgID 的原文件处理回归保持可用，其他 owner 无法读取旧资源。
- [x] 空库迁移、已有库迁移及迁移重跑通过。

上述勾选限定于 Dataset 与当前 FASTER Pipeline 路径：隔离主链由 HTTP 验证，版本/事务/血缘细项与迁移由服务测试和 PG16 补证。旧 Build/Job 的 HTTP 正向流程未覆盖，不能把仓储测试等同于该流程已经验收。

## P1.2 Source 和 Sync 的租户边界

### 结果

Source、凭据、Sync 定义与运行记录具有相同租户边界，目标 Dataset 必须属于运行所属域。

### 任务与验收标准

- [x] 创建 Source/Sync 时由认证上下文确定租户，凭据引用按 Source 验证。
- [x] A/B 的列表、详情、修改、测试连接、触发和查看运行记录均受到隔离。
- [x] 同一 owner 跨组织访问 Source/Sync 被拒。
- [x] 跨组织绑定目标 Dataset 被拒，运行失败不得改变目标内容。
- [x] 外部系统凭据与平台身份分别处理，诊断和运行记录不暴露凭据。

Source/Sync 同用户跨组织、绑定/修改/触发及运行记录有 HTTP 证据；凭据归属、解密后外部凭据与平台 JWT 分离及错误不泄漏由合成凭据的服务测试和 PG16 验证，不声明已接入真实客户凭据。

依赖 P1.1 定义的租户兼容语义，可并行实现，联调时统一验证。

## P1.3 真实 REST 分页同步

### 结果

调用现有 Sync 入口能真实读取受控 REST 源的多页记录，将实际数据字节发布到 Dataset，返回真实行数、字节数、内容 hash 和版本引用。同步状态在发布完成后进入成功。

### 任务

- 复用现有 Source/Sync 与 HTTP 适配器，定义明确的分页、数据提取、超时、页数及字节限制。
- 使用实际源响应计算内容 hash，去除将配置 envelope 当作数据产物的路径。
- 使用 Dataset 的文件写入与提交机制，传播调用者身份并验证目标资源归属。
- 上游失败、解析失败、发布失败均传播到运行状态。已发布版本必须可以通过普通下载接口核对。
- 首轮交付手工触发、单目标、全量快照同步。异步恢复、定时与增量按 P2 实施。

### 验收标准

- [x] 受控源至少三页，下载产物与全部源记录逐值一致，保留 SKU 前导零等标识符。
- [x] 相同配置下修改源数据，新运行产物/hash 随真实内容变化。
- [x] 第二页失败、无效 JSON、分页循环/超限及目标发布失败均进入失败状态。
- [x] 失败运行不会发布不完整的数据快照，目标已有有效版本仍可正常使用。
- [x] 同步产物可由 Pipeline 直接读取、转换并下载，复用上一轮全量核对方式。

依赖 P1.1/P1.2。代码可并行编写，验收按依赖顺序完成。

## P1.4 自动化验收与交付证据

- [x] 为当前新增行为补充必要回归用例，相关 Go 服务测试通过（各实现会话分别执行，日志可查）。
- [x] 隔离 Docker 环境运行 PostgreSQL、Dataset、Pipeline 与本轮接入服务及合成源。
- [x] 通过普通 HTTP 入口执行双组织隔离和真实 REST→Dataset→Pipeline→下载场景（7 主场景通过，旧 Build/Job HTTP 正向未覆盖）。
- [x] 保存可重跑脚本、脱敏配置说明、实际断言结果和未覆盖项。
- [x] 更新本计划的实际任务状态，逐项区分已实现、已验证和待实施。
- [x] 在同一隔离环境执行三服务统一 `tests` 和九项必需 PG16 `migrate-tests`，确认没有必需用例缺失或跳过。
- [x] 重启三服务后回归并执行 `summarize`，形成最终交付汇总；明确未覆盖项，判断 P1 关闭条件（主链通过，保留旧 Build/Job HTTP 缺口，不声明全量关闭）。

测试使用合成数据与临时凭据。版本、文件与运行结果必须相互对应，成功状态须有真实产物证据。

## P1.5 分库 Build/Job HTTP 流程（已完成，验收通过）

本轮已关闭 P1 最后一项 HTTP 未覆盖，下一步进入 P2。编码、构建和执行均通过 ZCode GLM-5.3，由 Codex 审查、维护状态与核对证据；保留当前工作区和上一轮验收产物。

### 任务

- 为 Build resolver 接入 Dataset HTTP adapter，覆盖资源验证、只读分页分支、schema、开启及取消事务。CreateBuild、DryRun、发布定义和取消全程保留调用者 JWT；缺身份或依赖失败时明确拒绝，不能回落到无租户约束的共库 SQL。
- 修复 Dataset schema 仓储与真实表的列契约、迁移和触发器，按正确分支读取 schema；缺 schema、无权访问和依赖故障分别处理，DryRun 不得通过 UUID 分支列表隐式创建 main。
- 发布、解析前验证 Pipeline、所有输入/输出 Dataset 及内联定义的所属域；发布映射必须引用实际 canonical JobSpec，定义更新后新 Build 引用新定义，已有 Job 的定义不能被替换。
- 补齐远端事务与本地锁的生命周期。创建失败须取消本请求已开事务、只释放本 Build 的锁；取消须真正终止 Dataset 事务，保留真实分支，重复取消幂等，清理错误可观察，不能假报 ABORTED。
- 只交付解析、持久化、查询和取消；Job 可处于 WAITING，分布式 Spark 的租户执行仍属于后续阶段。

### 验收标准

- [x] 分库部署通过普通 HTTP 发布 JobSpec、DryRun、CreateBuild，各组织返回真实非空 Build/Job，定义与输入/输出事务引用一致。
- [x] DryRun 不新增 Build、Job、OPEN 事务或锁；跨组织 Pipeline/输入/输出与伪造身份在发布或创建副作用前拒绝。
- [x] 同 Subject 双组织、组织管理员和无 OrgID 个人域按 UUID/RID 访问 Build/Job、日志、输出及输入解析均隔离；不得用未知随机 ID 或空 Job 列表冒充通过。
- [x] owner 取消后 Build、Jobs、远端输出事务状态一致，重复取消幂等，同一输出可再次创建；非 main 分支同样正确。
- [x] 第二输出失败、拿锁失败、最终持久化失败等场景正确补偿，不能取消或释放其他 Build/租户的事务和锁。
- [x] Dataset schema 空库/旧库/重跑迁移与 main/feature 分支回归通过；原七场景、统一服务测试和必需 PG16 回归保持通过。
- [x] 重启后新 HTTP 场景仍通过，旧 Build/Job 未覆盖项能由实际断言关闭，脱敏原始结果与汇总可重跑、可复查。

完成分工：Dataset、Pipeline 与验收会话通过 ZCode GLM-5.3 编码、构建和执行；Codex 审查代码、复核原始证据并维护本计划。未 commit、stash 或清理数据库卷，保留既有工作区改动。

最终证据归档于 `tools/smoke/business-platform-p1/results/2026-11-13-build-http/`，入口为 `summary.md`，原始结果为 `result.json`，完整日志在 `logs/`。最终 HTTP suffix 为 `443e0274`，汇总、JSON 与原始 smoke 日志对应同一次运行；原主链 `b0e0bcf8` 基线另存于 `results/2026-11-13-p1-mainline-baseline/`。

| 检查 | 实际结果 |
| --- | --- |
| 三服务统一单测 | 60 包通过（Dataset 8、Pipeline 15、Connector 37），零 FAIL |
| 必需真实 PG16 | 26 项全部 RUN/PASS（Dataset 6、Pipeline 17、Connector 3），零必需 SKIP |
| 分库普通 HTTP | 原七场景 + Build 正向/隔离，共 9 PASS、0 FAIL、0 UNCOVERED |
| Build/Job 生命周期 | 三域非空 Job 与 canonical 定义绑定；main/feature 取消、远端 ABORTED、同输出重建通过；feature 首次取消走 RID |
| 失败恢复 | 多输出、记录/释放锁/持久化故障、成功 OPEN 后取消及双重故障恢复通过；FAILED 重试可重新追踪并清理事务，其他 Build 锁保持不变 |
| 重启后的历史 Build/Job | 双组织及个人域原句柄可读，UUID/RID、状态与定义绑定一致，见 `logs/build-persistence-probe.json` |
| 文件持久性 | 新配置下固定样本跨 force-recreate 与 restart 下载均为 22,766 字节，SHA-256 与原 run 和事务 metadata 一致 |

隔离证据分层：外域输入的 Publish 与 inline DryRun 经 HTTP 拒绝；CreateBuild 不接收输入定义，历史外域输入引用由真实 PG canonical 映射用例 `TestBuildFlowForeignInputSpecRejectedZeroSideEffects` 验证，不伪造不存在的 HTTP 构造。日志 GET 允许明确的空列表；日志 POST 跨域或未知 Job 返回 404，真实 PG 验证无新增日志。五项外部 provider、分布式集群和 Python sidecar 集成仍 SKIP，不在已验证范围内；Spark 租户执行仍保持 503。

验收环境修复与历史限制：原隔离 compose 只有数据库卷，旧 `c12cad5c` 合成文件在重建无卷容器时丢失（下载 404，DB/事务 metadata 尚存），未恢复或回填。原 FAIL JSON 后被覆盖，`logs/historical-pre-volume-artifact-loss.json` 如实标记为从观察记录重建的历史说明，不能当作原始 JSON。现为 Dataset 实际 backing 目录挂载 `dataset-files:/home/nonroot`，挂载前备份并迁入本轮仍存的 182 个文件；完整说明见 `VOLUME_MIGRATION.md`。独立原始证据为 `logs/volume-recreate-pre.json` 与 `logs/volume-recreate-probe.json`（固定样本 `d64a7353`），以及 `logs/volume-restart-probe.json`（固定样本 `69a5b86f`）。持久性通过仅适用于新卷配置，不能据此声称旧文件已找回。

## P2.1 任务租约与持久状态（已完成）

沿用 ZCode GUI GLM-5.3 编码、构建和执行约定，由 Codex 审查与维护状态。业务实现仅增量修改 Connector；验收会话维护 smoke 工具。保留 P1 的工作区、`443e0274` 归档、隔离栈数据库卷和 `dataset-files` 文件卷，不 commit、reset、stash 或清卷。

本项在现有 `batch_sync_defs` / `sync_runs` 上建立持久任务和数据库租约，接入当前手动同步的实际路径：可信租户鉴权、持久入队、原子领取、拉取、Dataset 提交、持久终态。每个 Run 具有本次领取凭证、持有者、递增代次、数据库过期时间和心跳。不同手动 POST 的合并或 Source 并发限制留给后续任务；本项保证同一 Run 的领取唯一性。

租约续期及完成必须同时验证 Run、租户、运行状态、持有者、本次凭证/代次和数据库时钟下未过期。过期后尚未接管时旧执行者也不得续租或完成，接管后不得通过相同持有者名称绕过代次保护。终态不可被旧接口覆盖；数据库落库失败或失租必须明确返回失败/不确定，移除当前 `completeRun` 在内存中伪造终态的兜底。公开 JSON 不暴露租约凭证，持久记录不保存 caller JWT 或解密凭据。

迁移须适配 Connector 每次启动重放全部 SQL 的实际机制；旧终态及产物引用保持，旧无租约运行的处理必须明确且不能自动重复发布，重放不能重置有效新租约。明确停旧执行器后升级，不能把新列兼容等同于旧版绕过保护的滚动兼容。活跃任务下的 Source 删除、相关配置和 Sync 输出目标变更须有并发安全策略，不允许持久任务被级联删除或执行目标被悄悄替换。

### 验收标准

- [x] 持久 pending 入队和状态查询接入实际代码；重新打开数据库连接仍可读取任务、可信租户及执行绑定。
- [x] 真实 PG16 多连接竞争同一 Run，只有一个领取成功；未过期租约拒绝他人领取，合法心跳不增加领取代次。
- [x] 到期、接管、同名持有者再次领取及错误凭证均有 fencing 回归；拒绝更新后整行状态、计数、hash、版本和时间戳保持不变。
- [x] 终态不可覆写；旧无凭证完成方法不能修改新租约任务；旧库升级/迁移重跑保留终态和有效新租约。
- [x] 同 Subject 双 Org、跨域管理员及个人域相互隔离，可信 owner 与发起人分别取正确来源，活跃删除/变更策略通过真实数据库并发验证。
- [x] 手动 HTTP 同步使用真实租约和心跳，失租停止继续执行，提交前核验；落库失败不返回伪造成功，仍保持 P1 的真实内容、JWT 转发和错误脱敏契约。
- [x] Connector 单测、必需真实 PG、P1 全部九场景和统一服务回归通过；重启后任务状态及文件/Build/Job 基线可读，脱敏原始证据独立归档。

边界：P2.1 交付持久租约与本地状态保护，不启用后台恢复执行、取消 API、cron、Source 限流、增量断点或自动重复发布。Connector 数据库的条件更新不能原子阻止已经发往 Dataset 的 HTTP 提交；远端幂等、发布丢响应恢复与断点提交在后续 P2 任务验证，不能据此主张恰好一次发布。

完成分工：业务编码、构建和测试执行继续通过 ZCode GLM-5.3，Codex 完成仓储、handler、测试与原始证据审查并维护本计划。业务代码冻结，既有工作区、P1 归档与隔离栈数据卷保留，全部改动尚未提交 Git。

最终证据归档于 `tools/smoke/business-platform-p2-1/results/2026-11-13-p21-final/`，入口为 `summary.md`。P2 专项原始结果 `result_p21.json` 的 suffix 为 `b6ec49ab`；P1 重启前 `8582e649` 与重启后 `7a6175a7` 分开保存，不能混作同一轮。

| 检查 | 实际结果 |
| --- | --- |
| 三服务统一单测 | 60 包通过（Dataset 8、Pipeline 15、Connector 37），零 FAIL |
| 必需真实 PG16 | 原 26 + 新 14，共 40 项全部有 RUN 与顶层 PASS，零必需 SKIP；数据库 PostgreSQL 16.15 |
| Connector 独立检查 | 构建、vet、37 包单测、17 项 PG（既有 3 + 新增 14）、handlers/runtime race 与 repo integration race 均通过；常规 Go 1.26.3，race Go 1.25.14 |
| P2 真实 HTTP | 3 PASS：慢源等待中周期心跳推进且领取代次不变；失租返回 409、请求数严格 2→2、零发布；Dataset 已提交而 Connector 终态故障返回 503，数据库保持 running，没有伪造成功 |
| P1 普通 HTTP | 重启前后均为 9 PASS、0 FAIL、0 UNCOVERED；重启后三域旧 Build/Job 与固定产物下载、SHA-256、事务 metadata 通过 |
| 加强重启观察 | 原 A/B/C 句柄重启后观察 47 秒，状态、attempt/generation、心跳/到期时间、凭证存在布尔值、终态时间、hash/版本等白名单投影相同；总运行数 231→231，REST 请求数 2/2/5 不变，目标文件/事务数 1/0/1 不变，无自动拉取或发布 |

仓储回归覆盖获锁后的数据库时钟、实际 PID 锁等待、锁后 TTL、真实旧表缺列升级、组织/个人域矩阵及活跃变更的并发保护。变异反证确认错误的单语句续租会被回归抓到，随后按字节恢复代码并重跑通过；该预期失败日志和已修复的首轮历史失败须与最终 PASS 分开记录。重启探针只比较白名单字段，不宣称完整数据库行或原始 holder/token 值逐字节相同。租约凭证不进入公开 JSON 或归档，caller JWT 不持久化。

终态故障专项的准确事实是“Dataset 已提交而 Connector 未确认”：真实文件绑定该 Run，事务 COMMITTED，下载 SHA-256 和 151 行逐值核对通过；不能将其描述为没有副作用或已获得跨服务恰好一次保证。故障与失租留下的 running 记录重启后保留，不自动重跑。五项外部 provider、分布式集群和 Python sidecar 可选集成仍跳过，未计入已验证能力。

**P2.2 可中断 worker** 与 **P2.3 按 Source 限流** 已完成，验收记录见后续章节。后台恢复、调度、增量断点、幂等重试与提交恢复的后续完成记录见 P2.4–P2.6，本节仍仅证明 P2.1 范围。

## P2.2 可中断 worker（已完成，2026-11-14）

沿用 ZCode GLM-5.3 负责编码、构建与执行，Codex 负责只读 review、证据核对与本计划维护。业务实现限于 Connector，验收工具另建 P2.2 目录；保留 P1/P2.1 原始归档、工作区与数据库/文件卷。

目标是将实际同步执行交给受服务生命周期管理的有界执行器。默认手动 POST 保留原同步行为，显式 `?async=true` 在持久入队后返回 202，并提供绑定 Sync 与 Run 的单次状态查询和取消入口。异步任务只在进程内短期持有本次调用者 Authorization，使用原身份访问 Dataset；不存 JWT、解密凭据，不新增全域后台身份。进程重启不会自动重放或接管没有可用调用身份的任务，尤其不重跑 P2.1 留下的“已提交但未确认”运行。

取消请求必须持久化并具有租户与 Source 写权限校验。执行器在拉取中响应请求、期限与停机信号，停止后续翻页和新发布，收敛心跳与观察协程。取消和开始发布须在同一 Run 行锁下序列化：取消先胜则禁止发出 Dataset 提交，发布门先胜则取消明确返回过晚，不把已经或可能提交的运行伪记为 aborted。旧任务没有可信发布阶段，不能仅因新列为 NULL 就推定未发布。

### 验收标准

- [x] 显式异步触发在慢 REST 请求完成前返回，脱离 HTTP 请求后仍由服务执行器完成真实 Dataset 发布；默认同步路径保持兼容。
- [x] 队列和并发有界，队满/停机与持久入队的竞争无静默丢失任务；执行期限与凭据内存生命周期有界。
- [x] queued 与 running 取消可查询、重复取消幂等，运行实例通过数据库观察取消，跨进程入口不依赖本地 map 才能生效。
- [x] 取消与发布门的两种锁顺序均有确定性真实 PG 回归；取消先胜无远端提交，发布先胜不假报取消成功或未发布。
- [x] 取消、超时、断连与停机各自使用正确 context；worker/heartbeat/观察协程有界收敛，数据库连接池在执行器收尾后关闭。
- [x] 同 Subject 双 Org、组织管理员、个人域以及 Sync/Run 父子错配按 UUID 验证；公开响应与归档无私有租约凭证或 JWT。
- [x] 迁移可重放且不改变旧任务/有效租约；重启保留状态，不自动重复拉取或发布 P2.1/P2.2 的未知结果任务。
- [x] 服务构建、单测、race、原 40 项及新增必需 PG、P1 九场景与 P2.1 三专项保持通过，P2.2 普通 HTTP 专项和重启/停机证据独立归档。

完成结论（2026-11-14）：ZCode GLM-5.3 已完成实现、执行与归档，Codex 及只读审查代理完成代码、真实日志和结果核对。最终档案位于 [P2.2 验收报告](../../tools/smoke/business-platform-p2-2/results/2026-11-14-p22-final/final-report.md)，`manifest.json` 的 `FINAL_OK=true`，值级秘密扫描零命中。工作区保持未提交，P0/P1/P2.1 的既有改动与归档保留。

最终契约：默认并发 1、准入总容量 64（同步/异步排队及执行合计）；整体任务与内存授权期限默认 300 秒，从 Reserve 开始包含数据库与排队等待。取消返回 pending 已终止/既有终态 200、running 请求已持久 202、发布门后 409 `cancel_too_late`；旧任务返回 409 `cancel_unsupported_legacy`。过期 running 只保存取消请求，不由失效 holder 假确认终态。请求、任务、服务停机与发布收尾的 context 分离；先关准入、再取消与有界 join，未完成 join 不关闭 pool。所有新配置均有界校验，JWT 不持久化、不替换为后台全域身份。

最终候选验证：Go 1.26.3 / race Go 1.25.14 / PostgreSQL 16.15。统一单测 **61 包**（Dataset 8、Pipeline 15、Connector 38）；必需 PG **52/52**（原 26 + P2.1 14 + P2.2 12，每名恰一次顶层 RUN/PASS，零必需 SKIP/FAIL，三守卫均成功）。新增十二项由十项取消/发布门回归与两项并发迁移/等锁取消回归组成，独立定向与 race 也通过。另五项依赖外部 AIP/LLM、集群和真实 Python sidecar 的可选测试跳过，不计入已验证范围。

HTTP 与持久性：最终完整 P2.2 D–N **11/11**，suffix `7aeaf5c9`，`official=true`、`partial=false`；P2.1 A/B/C **3/3**（`96351e3b`）；P1 前后各 **9/9**（`aa169e93`、`ff92e6ce`）。三副本并发停机/启动正常，deadline 7 秒场景实际 7.0 秒终止并恢复 300 秒，真实 `{}` TCP 断连及时取消，同一 owner 短 JWT 过期后 Dataset 拒绝提交。五成员重启对照的全投影/源计数/文件/事务/运行集合由已审代码在本轮执行断言并通过，HTTP JSON 保留摘要；该部分不声称留有可独立复算的全部逐字段原值。

额外执行 Dataset/Pipeline/Connector 三项原服务重启，fixture 保持同一进程计数。归档中升级前后共同十列、重启前后十三列安全投影和目标快照可直接比对，固定旧 P2.1 B/C 仍为 legacy running，未取消、改写或重触发。旧 C 文件和 P1 固定旧产物下载 200，hash、COMMITTED 与 metadata hash 保持；三域旧 Build/Job 读取、父 Build 与身份绑定断言通过。比较的是白名单安全投影，holder/token 仅比较存在性；真实 PG 回归另有脱敏前整行比较。

验收发现并修复了两个实际缺口：未读取 `{}` 请求体导致 HTTP/1 断连不及时传播，现准入前以 5 秒、1 MiB 上限读取到 EOF，仅成功后清除期限，失败零入队；三副本并发迁移曾产生 `sync_jobs` 不存在，现整批迁移在同一事务取得固定 advisory 锁后重放，失败显式有界回滚。真实 TCP 八项与真实 PG 锁等待/取消/有效租约不变回归均通过，最终三副本全部替换新镜像后完成并发重启验收。

证据完整性：57 份独立测试材料已归入 `standalone/`，52 份字节一致、5 份密码脱敏转换在索引中明确标记；历史 FAIL、partial 与中断记录独立保留，不计入最终 PASS。全档 SHA256 索引、命令退出码、原始结果、升级/重启对照、当前栈状态和秘密扫描均在最终目录中。临时辅助三服务已移除，原隔离栈与卷保留。**P2.3 按 Source 限流的完成记录见下节**；自动接管、调度、断点、幂等重试、丢响应恢复和远端恰好一次语义仍未纳入 P2.2；P2 后续各项的完成范围见 P2.3–P2.6。

## P2.3 按 Source 限流（已完成）

沿用 ZCode GLM-5.3 编码与执行、Codex review 和核对证据的分工。P0–P2.2 的未提交改动、归档、隔离栈与持久卷保持保留；业务改动限于 Connector，新验收工具位于独立 P2.3 目录。

本项约束同步任务准入：同一 Source 的多个 Sync、多个执行实例及合法调用者共享未终结任务上限与最小成功准入间隔。未终结计 `pending + running`，不是上游分页 HTTP QPS；未配置新策略的旧 Source 保持兼容。契约已审定为 optional typed `sync.admission`：`max_inflight_runs` 显式取 1–4096，`min_interval_ms` 取 0–86400000（0 关闭该维度）；省略、null 或空对象不启用维度，显式非法类型/范围拒绝。

裁决使用数据库持久状态，先验证租户与 Source/Sync 父链，在 Source 锁下重读策略、复查计数及数据库时间；Run 插入和成功准入时间推进在同一短事务中完成。超限返回明确 429 与重试提示，不创建 Run、不消耗间隔，并释放本地 Reservation；P2.2 的本地队满/停机 503、任务总期限、内存授权期限、真实断连和有界停机语义保持。启用策略的 Source 使用 NOWAIT 快速拒绝行锁争用，旧无策略 Source 保留阻塞锁，以兼容删除/入队竞争。

pending 真正取消或运行提交合法终态后释放容量。running 仅记录取消请求、发布门后拒绝取消、过期租约或 legacy 未知结果仍保守占位，不以本项引入自动清理、接管或重放；已发生的准入不会因取消退还间隔。重启或迁移重放不得清除限流时间历史。

### 验收标准

- [x] 同 Source 的不同 Sync、同步/异步及 legacy 新建路径共享限额；两个实际实例不能各自绕过上限。
- [x] 不同 Source 独立，热点 Source 不占满执行 worker 等待额度；本地队列与 Stop 原有边界保持。
- [x] 限流拒绝有稳定错误码和重试提示，Run 数、准入时间、上游拉取和产物均无额外副作用。
- [x] 最小间隔以锁后数据库时间裁决，仅成功准入推进，跨配置更新、迁移重放与重启保持明确语义。
- [x] pending/running 取消、过期、发布门及 fenced 终态的容量释放规则通过真实 PG 确定性回归。
- [x] 同 Subject 双 Org、个人域、合法不同 actor 共享额度与 Source/Sync 错配按可信身份验证；拒绝先于限流状态变更。
- [x] 配置合法性、旧配置兼容、失败回滚、Source 修改/删除竞争及旧任务/有效租约不变均有验证。
- [x] 最终候选构建、单测、race、原 52 项及新增必需 PG、P1/P2.1/P2.2 与 P2.3 HTTP/重启回归通过，证据独立脱敏归档。

验收结果（2026-11-14）：最终归档为 `tools/smoke/business-platform-p2-3/results/2026-11-14-p23-final/`，报告 `final-report.md`，`manifest.json` 的 `FINAL_OK=true`。五镜像构建与三 Connector 实例切换通过；统一单测实际 61 包通过；原 52 + 新 7 共 59 项必需 PG 在同一新日志中逐名恰好 1 RUN / 1 PASS，零必需 SKIP/FAIL。Connector 全包 vet、integration 标签编译、全包 race，以及新增 7 项 PG 的 race 均通过。5 项 AIP/LLM/分布式集群/真实 Python sidecar 外部依赖测试 SKIP，不计已验证能力。

HTTP：P2.3 正式 8/8（suffix `728ea96b`），P2.2 正式 11/11（`81380418`），P2.1 A/B/C 3/3（`528455c2`），P1 前后各 9/9（`d398a48d`、`63e66f3e`）。另显式重启原 Dataset/Pipeline/Connector 三服务，旧 B/C 全 13 列安全投影、父链身份、fixture 计数、旧产物 hash/COMMITTED 与三域 Build/Job 回读保持。Q 最小间隔子例只重启基础 Connector；O 的完整前后原值快照可独立复算，Q/T/B 部分性质由已审代码在本轮执行断言通过，JSON 保留摘要，不宣称全部逐字段原值齐备。

运行与证据：fixture 启动时间早于本轮窗口、PID1 为直接服务进程，本轮未重启；原五服务保留，辅助三服务已移除，PG/文件卷与环境文件保留。旧两条 controlled 未确认历史行经精确父链及安全投影例外允许切换，其余 controlled 未终结行仍阻断；旧两条与本轮新增两条均未取消或改写，四行共同 20 字段前后相同，新两条未自动加入固定名单，仍保守占位。首轮守卫拒绝和 HTTP 7/8 工具预期错误均独立保留，不计正式 PASS；最初两项 vet 及部分诊断失败仅有终端记录的缺口在报告中明示。

Codex 独立复核 572 条 standalone 材料：571 条字节一致、1 条明确标记脱敏转换，零缺根；全档 695 文件 SHA256 与磁盘双向一致，秘密扫描零命中。354 个业务 Go/SQL 文件及根依赖清单在验收前后哈希一致。代码保留在未提交工作区。**P2.4 及 P2 整体的完成记录见后节；本节保留 P2.3 验收时点的事实。**

锁与响应约定已钉定：DB 预读策略只选启用策略的 `FOR UPDATE NOWAIT` 或旧无策略的普通 `FOR UPDATE`，锁后另语句重读策略并裁决；仅该 NOWAIT Source 锁语句的 55P03 映射 contention，取消和其他 DB 错误保留原义。429 使用 `code=source_sync_throttled`、`reason=capacity|min_interval|contention`，`retry_after_seconds` 与 `Retry-After` 一致；间隔向上取整秒，容量/锁争用的 1 秒仅为重试建议。私有 `connections.sync_last_admitted_at` 仅在成功插入后的同事务最后以数据库时钟推进，旧运行不回填；模型写入和仓储写入都需拒绝非法新策略。

## P2.4 可观察定时调度（已完成当前范围）

2026-11-14 完成验收。编码、构建、数据库、HTTP 与运行环境操作均由 ZCode GUI 的 GLM-5.3 执行，Codex 与只读代理审查实现并独立核对原始证据。最终 [验收报告](../../tools/smoke/business-platform-p2-4/results/2026-11-14-p24-final/final-report.md) 与 `manifest.json` 均已归档：52 个必需步骤实际 rc0、19 项内容判定全绿、`FINAL_OK=true`。全部改动仍未提交，P0–P2.3 工作继续保留。

当前 Connector REST snapshot batch Sync 已从“只保存 cron”进入可观察的定时执行。复用仓库 cron 库的 5/6 字段和 IANA/UTC 时区，持久保存游标与调度机会；同机会的去重记录、Run 与游标在 Source→Def 短事务内提交。错过多个时点合并处理一个游标窗口，再跳至未来；不逐历史槽补发。`?` 不受支持，不宣称完整 Quartz 兼容。旧 cron 默认休眠，调度循环默认关闭，启用时扫描间隔为 1–30 秒。

后台执行使用显式 arm/续期的有界本机授权，默认 300 秒且不超过已验证 JWT 到期；每次 arm 使用独立授权代次及实例启动身份，数据库只保存非秘密身份、归属与时效信息。JWT 仅在内存保存。授权过期、撤销、配置变更及重启丢失本机能力均可观察；重启后须重新 arm。后台调度复用租户/Source/Sync 父链、P2.2 有界执行器及 P2.3 Source 准入，不接管或重放历史未知结果。已入发布门的任务保持 P2.2 有界收尾与未知提交语义，撤销不承诺撤回远端提交。

本项对 ADR-0025 作明确的实施范围例外：仅在当前 business-platform 的 Connector REST snapshot batch Sync 路径提供数据库认领式触发，不创建平台通用调度框架，不复活旧 pipeline 调度服务，也不宣称完成或替代 Temporal 迁移。跨实例长期无人值守授权与平台统一生产调度引擎仍须单独实施和验收。

| 最终检查 | 实际结果与边界 |
| --- | --- |
| 构建、单测与并发 | 五个候选镜像构建 rc0；三服务统一单测 61 包通过；新增授权器、真实 handler、Stop、截止时间与清理并发回归及相关 race 通过 |
| 真实 PG16 | 新增 13 项普通版和 13 项 race 均逐名通过；统一旧 59 + 新 13 共 72 项，每项恰好 1 RUN / 1 PASS、零必需 SKIP/FAIL，真实 Go rc0；使用已绑定归档导入，不为补步骤重复执行 |
| P2.4 普通 HTTP | suffix `150041b036`：7/7 场景、127 项断言全真，含真实 REST 151 行逐值/hash/COMMITTED/血缘、多实例去重、配置零变更、授权与租户边界、共享 Source 准入及 G4 受控停机→释放 Source 行锁→启动→重新 arm；6 份探针 sidecar 按 SHA/字节绑定 |
| 旧路径回归 | P1 前后各 9/9，P2.2 完整 11/11，P2.3 适配版完整 8/8，P2.1 ABC 3/3；P2.3 冻结场景仅将 Q guard 命令透明替换为本轮四条固定历史任务 guard，实际替换恰一次，不称逐字未改 |
| 重启与持久状态 | 最终三服务重启后旧产物、Build/Job 回读和 P1 回归通过；两轮 P2.4 尝试的归属并集 17 defs / 32 ticks / 15 terminal runs / 0 grants 在前后快照精确一致，result 与 history SHA 实配；所有调度已禁用、游标 NULL |
| 状态保全 | 旧四条 controlled 任务的 27 字段不变；最后 ABC 新增 B/C 两条有完整父链和 C 的实时 COMMITTED/下载/hash 证明。最终受控集合恰为旧四加新二，固定 pins 仍为四条；新两条在最终重启之后创建，未声称跨该次重启 |
| 候选与归档完整性 | 373 文件源码在构建前、PG 绑定及验收后相同；最终 base3 为已构建候选，base5 运行、aux/proxy 移除、循环 OFF；fixture 同进程、旧计数和卷保留。859 份归档双向覆盖、27 份工具 SHA 均独立核对，secret scan 0/0；standalone 218 份中 205 份字节一致、13 份有明确 DSN 脱敏记录，前轮 P2.3 695 份归档不变 |

队满/Stop 的证明由新增单测及 P2.2 回归绑定，父链/事务回滚/迁移由真实 PG 补证，不将这些统称为新增 HTTP 场景。5 项 AIP/LLM provider、分布式集群和真实 Python sidecar 可选测试仍为 SKIP、未验证，不计入已验收能力。

额度中断、用户暂停与恢复、首轮 `?` cron 导致的 HTTP 失败、PG 构造/冷缓存失败，以及镜像检查和 postfacts 首次校验失败均保留原件；修复后另立尝试，没有覆盖失败日志或补造早期退出码。首轮 HTTP 创建的四个定义已清理并纳入 17 个归属并集。全部 q3 重启、重建与辅助容器移除都在最终 ABC 之前完成；ABC 为最后一次 q3 写入，其后只读收证与归档。保留 q3v9 与隔离 PG 数据卷及 env600，不进行额外清理。

### 验收标准

- [x] cron 格式、时区、启停/修改/清空、下次时间及错过时刻策略明确且可验证，非法新配置零变更。
- [x] 普通 HTTP 配置后自动执行真实 REST→Dataset 发布，调度机会、结果和 Run 可关联查询。
- [x] 同一调度机会在多个实际实例下去重；记录、Run 创建与调度时间推进保持事务一致，故障回滚有真实 PG 证据。
- [x] 调度授权有界、可续期且失效/重启后可观察；组织/个人域、同 Subject 双 Org 和父子错配不能扩大权限。
- [x] Source 容量/间隔与手动触发共享，拒绝、队满、取消、停机及其他 Source 进展有明确可观察结果。
- [x] 配置修改/禁用与到期竞争、重启、迁移重放不重复运行旧任务，不改变既有租约、未知结果和旧产物。
- [x] 最终候选构建、单测、race、旧 59 项及新增必需 PG、P1/P2.1/P2.2/P2.3 与 P2.4 HTTP 回归通过，失败和跳过如实保留。
- [x] 前后安全原值快照、fixture 进程/计数、源码指纹、真实退出码、全档 SHA256 与秘密扫描形成独立可核对归档。

P2.4 当前范围已完成；P2.5、P2.6 及 P2 整体的完成记录见后节。

## P2.5 发布后推进断点（已完成当前范围）

截至 2026-11-15，P2.5 当前范围已完成。业务实现、真实 PG、增量 HTTP、崩溃与丢 ACK、最终旧回归、只读状态与 22 阶段总检查全部通过；证据已封存，封存后复验和最终完整包离线复验均为原始 rc0。P2.6 后续启动记录见下节。

2026-11-14 用户授权推进，最初编码和执行通过 ZCode GLM-5.3。2026-11-15 GLM 达到五小时额度后，用户明确授权 GPT-6.1 Sol 等模型作为 worker；Runtime、HTTP、Archive 三组由 Sol 接续，Codex 负责审查、阶段放行和进度。已有绿色 Go/PG/构建使用原始退出码与 SHA 绑定复用。

目标是跨 Run 的真实增量读取与持久断点：仅在明确发布成功且本地租约终态 CAS 成功的事务内推进；拉取失败、取消、旧 generation、发布超时或未知结果不得提前推进。每页固定携带本轮数据位点，最终响应给出候选；每轮发布独立 SNAPSHOT，当前有效视图为最新批次、历史版本仍可读。按 Def 持久占用并在 admission 捕获基线；发布门后保留未决占用，仅门前可证失败/取消才释放。源配置、完整身份与目标绑定不匹配时拒绝续读；重置采用新建 Def。单次分页 cursor 和调度时间游标均不充当数据断点，自动重试与未知提交恢复仍属 P2.6。

P0–P2.4 未提交改动、旧归档与工具、六条原受控未终结任务及原 fixture/数据卷保持保全。P2.4 后两条任务未跨其最终重启的历史事实已按首次候选切换记录核验，没有用额外重启改写该边界。业务候选指纹冻结为 `a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27`，Sol 接续期间未改业务 Go/SQL。新增证据位于 `tools/smoke/business-platform-p2-5/results/2026-11-14-p25-final/`。

| 已取得的实证 | 当前结论 | 主要原件/接口 |
| --- | --- | --- |
| 单测、race、PG | Connector 38 包单测、相关 4 包 race；新增普通 PG 15/15、race 15/15、旧 72＋新 15 统一 87/87，必需项逐名 RUN/PASS，无必需 SKIP/FAIL；迁移、回滚、fencing 和兼容均有真实 PG | `pg-candidate-bindings-r17.json`、原始 cmd/raw/rc/meta |
| 候选和保全切换 | 六个服务候选已构建并部署；构建 24 份原件、严格只读预检、运行 full image IDs、新 boot 和旧六任务整集/父链已核；原 fixture/PG/卷保留 | `build-candidate-bindings-r18.json`、`deployment-record*.json`、各 images/guard steps |
| healthy HTTP | 12 场景按原件汇总通过；三批 24/20/30 条逐值下载及 epoch 0→1→2→3、最新 SNAPSHOT/历史文件、空批次、非法候选、并发、租户/配置绑定、取消和失败重拉均有证据 | `window-aggregate.json`、`window-union-handles.json`、原 full 与三个独立补跑 |
| 调度及忙碌窗口 | 两次定时增量发布成功；四个成功 Run 的输入位点日志匹配。真实 busy tick 的 SQL 前后证明无新 Run、无数据断点推进、无成功准入时间更新，仅 cron 游标前进 | h13 原 HTTP/SQL、`h13-ledger-supplement/`、全七源 quiet 原件 |
| 持久状态/强制崩溃 | 主三服务真实重启后所采断点不变；一次性辅助 Connector 被 SIGKILL 后同容器、同镜像启动，原 Run/gen/attempt/pub 状态与占用保留，无自动接管；租约过期理由已实际读到 | `restart-*-running.json`、`restart-boot-cp-verdict.json`、`h10/attempt1/` |
| 发布 ACK 故障 | 四种错误 ACK 已拒绝；真实远端 201 COMMITTED 后本地 EOF，独立事务/文件/558 字节下载同 hash，本地 txn 为空、epoch 0、占用保留并拒绝新触发 | 原 `tail-result.json`、`tail-aggregate.json`、`tail-h14-supplement/`、`h14-supplement-proof.json` |
| 旧 HTTP 回归 | P1 pre9/post9、P2.3 8/8、P2.2 11/11、P2.4 7/7、最终 P2.1 ABC 3/3 通过；guard 适配实际各 1/2 次，重启侧证及恢复 loop0 均通过 | 各 regressions/p1post9/p21abc steps、adapter records、P2.4 sidecars |
| 最终状态与旧产物 | 31 个 P25 定义停调度、授权为零，八条占用逐一 409 且完整 CP/Run 前后一致；六辅助实例移除、主三服务 boot/image 不变；旧产物与 Build/Job 比较无差异；44 个定义身份、最终受控九条/占用八条、旧 P24 封存投影 17 Def/32 tick/15 Run/0 grant 均核对通过 | `owned-quiesce/`、`final-images-verdict.json`、`12-reread-compare/`、`postfacts-verdict.json`、`persist-attempts/attempt-2/` |

最终交付位于 `tools/smoke/business-platform-p2-5/results/2026-11-14-p25-final-seal-meta/final-bundle/attempt-1/`，离线报告为其外层的 `final-bundle-attempt-1.stdout`。22/22 阶段通过，秘密扫描零命中，旧归档 859 文件及 27 工具无漂移。封存包含 5,016 文件；最终包包含完整封存 EV（含 seal）及依赖，共 7,352 文件，离线 21 个语义阶段与包结构全部通过。Root 已独立重算封存与包的完整文件集合及所有 SHA，确认无遗漏、无 `.env`，封存后 EV 未变。

- EV `hash-manifest.json` SHA：`a1e760b7ae9d56d0771337ae307b2c0b5a7068005537a26d4094a1450c0f0ad0`。
- 最终包 `bundle-hash-manifest.json` SHA：`1fd0329e2f9322e71316d266ab03440f0678f97bd05734de88b09973d39705bd`。

失败尝试均保留。首轮 PG 的两处测试预期已修正并实跑；HTTP 工具修正了触发响应/单 Run GET 区别、错误字段、未初始化断点及 201 ACK 解析。调度测试首次使用过窄 ARM 身份造成两条发布 403，已用正确的同一主体/组织身份在新 Def 补跑，旧两条失败占用没有清理或重放。原 full HTTP rc1、外层驱动执行中改写造成的 rc2、原 tail rc1 与错误 ACK 对象 ID 下载的 404 均独立保留；派生通过结果严格绑定原件，只替换已核实的具体断言。当前 P25 checkpoint owner 为原失败两条＋崩溃 held 一条＋故障尾部五条，共八条；非终态 P25 Run 仅崩溃 held 一条。最终 ABC 已作为最后一次 q3 写入完成，之后仅只读。最终受控非终态总九条为原六条、H10 held 一条及 ABC 两条，均有实际 UUID 父链。两处采集 grants 空集合的 JSON 投影修复只补采只读 SQL，原 rc1 保留。首次离线包与 prepare-r1 的原 rc1 均保留；多行 JSON/完整报告选择、tuple 与 JSON array 比较、受限副本路径映射及账本自身完成行校验已修复，最终 standalone-a3、prepare-r2 均通过，未重跑绿色业务验收。

### 验收标准

- [x] 断点协议、初值、候选提取、空批次、边界与批次输出语义明确，旧 snapshot 默认行为兼容。
- [x] 普通 HTTP 连续两轮以上真实增量拉取，输入断点、已发布版本与提交后断点可关联核对。
- [x] 断点推进与成功终态在同一数据库事务，仅可用发布 ACK 和当前租约持有者可完成；真实 PG 回滚与远端丢 ACK 均已核。
- [x] 同 Def 的 PG 竞争及真实 HTTP 并发无重复占用，不同 Def 独立，失败和未知结果保留明确状态。
- [x] 配置/目标绑定、租户/父链和手动/异步/调度路径已验证，公开投影不暴露凭据或租约内部值；最终证据秘密扫描另列。
- [x] 拉取、分页、发布失败、取消、到期、重启与提交未知有实际证据，未提前推进或自动重放未知任务。
- [x] 空库/旧库/重跑迁移、候选构建、单测/race、必需 PG 与全部相关旧 HTTP 回归通过，包括最终 P1 post9/P2.1 ABC。
- [x] 原安全状态、旧产物、源码与运行镜像、真实退出码、秘密扫描及最终归档 SHA 可独立核对。

## P2.6 幂等重试与提交恢复（已完成当前范围，8/8）

2026-11-15 用户在 GLM 额度恢复后授权继续，最初由 ZCode GLM-5.3 最高分工实现 Dataset 幂等提交、Connector 持久恢复核心、执行器/恢复端点/coordinator 接线及验收工具，Codex 负责 review、接口协调与进度。真实五小时额度触顶后，按用户再次明确的备用授权，由 GPT-6.1 Sol worker 接续部署与保全、HTTP 故障工具、旧回归适配与归档验证；原 ZCode 会话已确认自然停止，未终止或清除其状态。当前业务候选 S2 已冻结，正式 PG、全量单测和两个镜像构建通过。运行服务已完成 S2 切换；HTTP 十项、P1/P2.1–P2.5 全部旧回归、主服务重启及最终只读保全已通过，本轮10个辅助容器已清理。保护20项输入的全字节 scan1 已实际通过，精确覆盖138881个成员、2663996453字节，实际值命中和可疑模式均为0；2项公开 selftest 固定值经独立源码和内存相等证据确认后另行登记，未按路径豁免实际值。新 secret-v2 的138889份实际原件闭包及原始退出码0已独立核对。正式包 assembly CLI 原始rc0，进程内 fullcore 与 copied verifier 均通过；完整独立 copy/OS sandbox、scan2、外置 seal 与三处临时副本清理均已实际通过。P2.6 验收8/8，P2 当前范围完成；P3 尚未启动。

范围是新显式 opt-in 的受控 REST 同步（增量及普通 snapshot）的有限重试与重启后恢复。协议选择在 Run admission 冻结，旧 Run 不回填、不因当前 Source 配置而被采用。Run 级发布 key 不含 attempt/generation；发布意图在门前持久化 exact 有界请求字节、候选、身份/目标/明确 branch 与 fingerprint。门前无意图可在冻结基线上重拉；有意图后仅查证或原 key/原字节重放。Dataset 在单外层事务中耦合版本发布与不可变 receipt，同 key 重放返回原回执，不重新应用当前 head/index。

以下首轮进展保留为历史候选与失败恢复记录，段内“待执行”不表示当前 S2 缺口，旧候选通过不计作 S2 通过。当前状态以本节后续 S2 与最终收尾记录为准。

首轮真实执行进展：新 `of-p26-pg` 隔离 PG16.15 已准备成功，零 host port、env600，原始 rc0 与镜像/版本/所有权证据保存在 `tools/smoke/business-platform-p2-6/results/w6/pg/real-prepare-attempt2/`；首次网络缺失提示分类不全的 rc2 尝试保留，未创建资源。Dataset 首次 normal 的分支夹具错误已窄修，原 7/8 失败原件保留；重跑 normal 与随后 race 均为 8/8，每项恰一次 RUN/PASS、零 SKIP/FAIL，原 Go rc、version/source/names gate 和 wrapper 全 0，证据在 `real-dataset-normal-attempt2/`、`real-dataset-race-attempt1/`。Connector 17 项首次 normal 为 15/17，原 Go rc1、version/source gate0；两项失败为并发证明要求两个 waiter 都直接等待同一 PID、以及预算逆序复用仍有合法 owner 的 Def，原件在 `real-connector-normal-attempt1/`，失败后未执行 race。夹具已改为可观测阻塞链及独立 Def，待重跑。另补 Dataset/Connector 本地 COMMIT 真实 wire ACK 丢失专项；HTTP、部署、重启与最终统一验收尚未完成。

恢复仍经过租约/generation/token fencing。新协议发布结果未知时保留非终态及占用，404、超时、错误回执或预算耗尽不作为未发布证明；只有可证门前失败/取消或匹配 committed receipt 的原子成功才能收敛。成功、增量 epoch 推进、owner 释放与 intent finalized 同事务；后续批次已经推进后，旧成功恢复仍应零写幂等返回。普通 snapshot 继续已有 last-commit-wins，不新增跨 Run 排序保证。

冻结候选 `b94e6b2f…ff14c`（402 个 Go/SQL 文件）的正式 PG 验收已通过：新增 Dataset9＋Connector18，普通与 race 各 27/27；同一源码上的旧87＋新27统一 114/114（Dataset15、Pipeline17、Connector82），每项逐名 RUN/PASS、零必需 SKIP/FAIL。七轮原始进程、工具链版本、源码、名单及 wrapper 门禁全为0，证据为 `tools/smoke/business-platform-p2-6/results/w6/pg/real-S-*/` 及对应内层 cmd/raw/rc/gates。新增 wire 测试实际抑制外层 COMMIT 回应，并以独立 LISTEN、相同 backend PID 及提交后投影证明已提交；Dataset 精确回执重放、本地 snapshot/增量 finalize 重复调用均保持一次效果。两个候选镜像已在冻结源码上构建并完成 full image ID、运行配置与源码复核，临时构建容器已清理；构建首次验证因缺失 Cmd 的模板错误返回非零，原件保留，采用 null-safe 模板在既有镜像上续验通过，没有重构建替换失败记录。Dataset 候选为 `sha256:4217c402…19ef75b`，Connector 为 `sha256:f23f6916…1df34`，Pipeline 保持旧候选。三服务最终全量单测原始 rc 均为0（Dataset8包、Pipeline15包、Connector39包），没有将可选外部集成的条件跳过算作已覆盖。旧状态当前值核对、部署、真实 HTTP/最终旧回归和最终封存仍待完成。

实际部署进展：切换前 W8 当前保全核对为 131 份原始退出码全0（包含59个比较门），旧九条安全27列/父链、八份完整 CP/Run 与父链、原 fixture boot/九项计数、旧 H14 下载均一致。Dataset→中途保全→Connector→后置保全已逐步通过，镜像为上述冻结候选，旧 Pipeline/PG/fixture 未改。新的四个隔离资源与三份0600私有文件已建立，四项健康检查均200；新资源启动后保全再次通过。健康快照/legacy、增量0→1→2、真实201丢ACK后200原字节重放三个HTTP场景已执行并清理通过。首轮轮询响应体证据不足的历史保留；增强记录后，新建对象的正式三项复验均为原始 Unix rc0，完整安全 CP/Run、源计数、代理记录和下载字节已留档，Root 已独立复算通过。旧 P2.5 h12 所需的同源码回滚用例另补普通/race 各一次，原始 Go/名单/源码/版本/wrapper 全0，统一阶段复用已有114项中的原始通过记录；派生兼容 KV metadata 与原始 JSON metadata 的关系单列，没有改写原件。真实崩溃、双实例、预算、旧 HTTP 回归和最终封存继续推进。

新增 HTTP 正式验收已完成 **10/10**：健康 snapshot/legacy、增量、丢 ACK 自动重放、崩溃后手动查证、门前无 intent 崩溃、在飞 404 后同正文重放、双实例竞争、持久发送预算、恢复旧 A 不回退 B 的 head，以及租户/权限/旧协议拒绝。十项原始 Unix 退出码均为0，Root 已逐批核对原始安全响应、进程操作、自然租期到期、预算、断点和下载字节；完整 HTTP 原件校验闭包为5034个文件。代理在飞屏障位于转发 Dataset 前，数据库串行化由独立 PG 用例证明；HTTP 502 只证明依赖/传输失败，未将其称为错误回执字段注入。跨域拒绝场景的下载另有明确关联的当前只读产物补证，没有倒填为历史时点证明。十项结束后的 W8 核对再次为131份原始退出码全0、59个比较门通过，旧九条任务/八份断点及完整父链无漂移，主服务保持已接受的实际 boot。旧 HTTP 回归已开始：P1 pre9 为9 PASS、0 FAIL、0 UNCOVERED，外层/子进程/源码核对原始退出码均0，28份实际工具前后哈希相同，临时 CHECK 已清理；原件在 `tools/smoke/business-platform-p2-6/results/w10/p1pre9-ready-attempt2/outer-actual/`，真实外层退出码为 `p1pre9-outer-launch-attempt1/rc`。P2.2–P2.5、最终重启后的 P1 post9、P2.1 ABC、旧产物可读性、最终封存和独立离线包复验仍待执行。

旧 P2.2 首轮完整回归为8 PASS、3 FAIL，原始退出码1保留。J 的辅助驱动把 Docker 名称中的前导斜杠混入逻辑身份，导致已成功切换到7秒 TTL 后未进入恢复段；已修投影并通过显式 A-only 操作恢复300秒，原成功/失败证据未改。M 是由该身份链未更新引出的后续拒绝。L 则揭示真实业务兼容性回归：普通 NULL 协议任务也被无条件套用只要求于新 v1 协议的 JWT 到期执行上限，拉取在发布门前提前超时。Root 与独立审查已确认应按 admission 返回的持久协议标记选择 Dispatch，保留旧 L 的发布门与 Dataset 401/403 断言。现已仅为这两条正常入口和对应真实入口测试重新开放源码；旧 `b94e6b2f…ff14c` 的402份 Go/SQL及 go.mod/go.sum 共404原字节已独立封存和重算，以上通过记录均保留为该历史候选的事实。修复后将重新冻结指纹、构建并执行当前候选验收，不把旧候选结果算作新候选通过。

兼容性修复后的新候选已冻结为 `ff9cf1df…ecf0e`，仍为402份 Go/SQL，仅两份 handler/测试文件变化。真实 RunSyncJob 同步/异步的 NULL/v1 入口先红后绿，完整 handler 普通与 race 各239项通过；新候选重新实跑新增27项普通及27项 race、统一114项、h12回滚普通/race补证，均逐名一次 RUN/PASS、零必需 SKIP/FAIL。三服务模块单测为8/15/39包通过，Connector 首次仅覆盖 internal 的38包原件保留，已单独补齐包含 cmd 的全模块39包。两个镜像重新构建并校验各17份原始退出码全0：Dataset `sha256:521f1712…e95c2`，Connector `sha256:ea8244f0…685d6`。原件在 `tools/smoke/business-platform-p2-6/results/w6/S2/ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e/`。

S2 Dataset 已实际切换并通过健康及完整保全检查。55份步骤记录为36份 Unix 子进程 rc0＋19份 HTTP 200/transport0，旧九任务、八断点、两条旧 legacy 和新 N 均无漂移。Connector 的切换命令为0，但工具错误地要求环境变量名称顺序相同，导致外层返回1；原32步为23份 Unix 子进程 rc0＋9份 HTTP 200/transport0，两侧10个名称集合相同且无重复。原失败、授权和工具字节保留；只读补验的外层 rc0、25步（15 Unix＋10 HTTP）全部通过，没有再次切换。主 Dataset/Connector 的 S2 运行镜像现已验收。

回归实例 C 也已切换到 S2 并通过健康及完整保全。前置工具曾因安全配置尾部空行拒绝，未执行切换；随后实际切换成功，但 PG 挂载列表换序触发后置误判，原外层 rc1 与39步原件保留。独立只读补验的26步（16 Unix＋10 HTTP）和外层 rc0 均通过，没有重复切换。挂载比较现按唯一目标路径核对完整字段，名称仍要求合法且无重复。A/B 随后各57步（38 Unix＋19 HTTP）及外层 rc0 均通过，C/A/B 全部采用 S2 镜像。旧 TTL 测试驱动也已绑定 S2 镜像覆盖文件，避免测试时切回旧镜像。

S2 的真实 HTTP 十项已重新全部通过，使用独立 a2/b2 实例（17101/17102），没有复用旧候选的通过记录。Root 独立核对4906份原件、434份 owned 操作的原始 Unix rc0、自然数据库租期、回执与下载字节；随后139份原件的旧状态核对通过，九任务、八断点及 BC/N 未漂移。权限场景的当前回执/完整父链/CP404/head/version1/827字节32行下载另以57份只读原件补证，明确是后续 CURRENT 观察，没有倒填历史。首次受限环境预检无响应、以及补证工具额外要求 JSON object 的两次失败均保留，后者只修正为契约允许的合法 JSON 行；业务源码、十场景断言和历史结果未改。

S2 旧回归已通过 P1 pre9（9/9，512份原件）、P2.2（11/11，1578份原件）和 P2.3（8/8，1896份原件及三份字节一致的实际快照）。Root 均独立核对原始退出码、源码、工具和清理证据；P2.2 L 按原断言证明进入发布门后由 Dataset 返回401，先前的真实兼容性回归已关闭。P2.4 已将 C 以同一 S2 镜像切到 scheduler=1，前后完整保全通过；首次七项调用为2 PASS、5 FAIL，发现只读 SQL 包装引入命令标签，以及第二实例地址未显式注入两处适配问题。原失败与426份证据保留，不计为通过；两处工具问题修正后，170份只读原件确认清理。完整重跑为5 PASS、2 FAIL，已按原件确认第二实例 A 的 scheduler 未开启，而原 g2/g4 要求其运行；真实 C 重启及13个新对象清理通过，826份原件保留。A 的临时调度配置现已切换通过，582份原件独立核对完成：全库启用调度列表为空、due/授权为零，旧任务与断点及其他12个容器不变。C/A 均满足原验收拓扑后的完整重跑已为7 PASS、0 FAIL、official=true，原始外层/子进程/套件退出码全0，guard2与13个新对象清理通过；826份原件和真实 C 新 boot 已独立核对。该阶段后续的 P2.5 完整15项回归、主服务重启和运行环境收尾均已通过，详见下文；失败与配置操作原件独立保留。最终封存结果另列，本段保留历史时点的进度。

S2 的 P2.5 完整15项回归已通过；h1–h8中，h5依原契约仅作前序兼容性归属标记。h9 的8个业务检查通过，但取消后的 fixture 6秒等待尚未结束，原 finally-quiet 为失败；447份失败原件保留。后续266份只读原件确认请求数不变且已自然静默，未改写失败结论。四处工具窄修及租约到期跨采样边界经定向纯测和独立 review 后，fresh h9 已按原8门、自然等待与严格 finally 全部通过；648份原件记录13次采样、26次GET，在3.74秒内 active1→0且计数未重置。h12 也已成功消费同一 S2 的原始 PG 回滚证据（无重复执行），366份原件已独立核对。h13 的42个检查与 h15 的7个检查也已通过，分别核对1242份和541份原件；定时 busy tick、禁用后静默、手动与异步增量互斥均符合原断言。独立 h13 位点日志、7个源及代理的全源静默，以及保留13源原件的12项健康回归汇总均已通过。h10 首次在重启前子进程耗尽原5秒预算，963份失败原件保留，kill/start均未执行。导入副本仅去除重复来源元数据后，完整原件及来源映射保留，原5秒预算和断言未改；fresh 重跑已真实 kill/start 同一实例 C，3.14秒完成 kill，pre6/post12检查全真，3282份原件独立核对，22个历史 Def 与完整23个 Def 清单保持。另245份只读原件确认租约自然到期，完整27列与父链不变，CP仅出现允许的需要对账标记；h11/h14尾部已通过，1022份原件及315份独立读取原件完成核对，远端已提交但本地未知的占用保留。P2.5 完整15项回归的独立离线合同已通过，涵盖14545份原件及21项负向验证。业务源码与原验收断言保持冻结；P2.6 归档与封存的最终结果见本节最后记录。

截至 2026-11-16 凌晨，最终重启前的本轮归属294个 Def/280个 Source 已按当前父链只读核对；调度全部关闭、授权为零，五个本轮辅助 Connector 已停止。主 Dataset→Pipeline→Connector 按原容器/原镜像各重启一次通过，PG 与旧 fixture 未重启，旧任务、断点、父链及九项计数保持。重启后的 P1 post9 全部通过（529份原件），旧 artifact 六域完整语义与封存基线一致，三份历史下载的字节与哈希相同（159份原件）。本轮 post9 实际新建13个 Def、11个 Source 和33个 Dataset，已依据实际创建原件追加到最终归属清单，没有假定已删除。回执原正文已在重启后单次重放成功，89份安全原件证明返回原回执、版本仍为1，head/文件/Run/断点投影全部不变。首次自动审批在进程前拒绝，补充本机合成数据事实后以同一命令获放行，拒绝记录保留。最后 ABC 也已3/3通过，487份原件及临时故障清理核对完成，新 B/C 的未知状态保持；此后仅进行只读核验、指定辅助清理与归档。最终只读状态、保护20项输入的 scan1、完整独立离线归档及 scan2/seal 均已通过，原始结果见下述最终收尾记录。GLM 额度恢复后可按用户授权作为备用 worker。

原 q3 九条受控非终态、八条 checkpoint owner、旧 Source/目标/产物与所有 P0–P2.5 未提交改动继续保全。P2.5 工具 85 文件及封存归档/完整离线包不可改写；P2.6 工具和原始证据使用新目录 `tools/smoke/business-platform-p2-6/`。P2.5 的 `a1bff07f…ffd27` 是历史源码基线；P2.6 新 Go/SQL 候选会另算指纹，不使用旧外部源码锁判断新阶段是否合法。

最终运行环境收尾已通过：补采后的保护并集为9条新类型化任务，旧7条原值未改，新B/C完整27字段与首轮 CURRENT 观察一致；其初始历史原件分别为B完整27字段、C真实5字段，未补造旧投影。独立整集核验为受控12、CP owner14、legacy3、完整父链17（228份原件）。最终归属310个 Def/294个 Source 均在、调度关闭、next fire为空、全库授权为零；包括1个未被Def引用的Source。清理前后该整集与公开投影保持一致；10个本轮辅助容器已各移除一次，主5容器及旧PG/fixture保留，587份清理原件由Root独立核对。两次只读工具适配失败、初始不完整索引及纯测失败均保留，未以重跑业务验收替换。保护20项输入的 scan1、fullcore/正式包、完整独立 copy/offline、scan2、外置 seal 与三处临时副本清理均已实际通过，P2.6 已完成当前范围。

### 最终归档与收尾（已完成）

当前已确认：S2 源码仍为 `ff9cf1dfcdfdf275005687a4edbc0da3ae88a036335b1b3a236f857d59eecf0e`，旧 `b94e6b2f…ff14c` 的成功、失败与 partial 仍作为历史保留。原22项 scan1 首次失败及3490次命中保留；candidate-17/19 经公开 creator 固定值与两项真实内存相等结果核对，另列为公开 fixture，其他20项任何实际值命中仍致命。正式包排除186份潜在私密原 body/command（147＋26＋13），本机原件不动，原 inventory/pin 不改；26项私密命令由实际安全投影支撑 GET 形状，未假称包内存在原字节或重建未记录的 headers。

| 最终门 | 实际证据与状态 |
| --- | --- |
| protected20 scan1 | 已通过，outer/provider/scanner 原始0/0/0；138881成员、2663996453字节，actual hit0、suspect0；[原始报告](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-scan1-memory-audit-attempt2/scan-audit.json) |
| secret-v2 独立原件闭包 | 已通过，138889份实际原件、原始rc0；Root original roster 已接受。[闭合合同](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt89/SECRET-V2-ACTUAL-CLOSED-CONTRACT.json) |
| 全部 final core | 已通过；[assembly 原始审计](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt93/ORIGINAL-AUDIT.json)记录所属 assemble CLI 原始rc0、进程内 fullcore 通过与 copied verifier 的 VERIFIED-NOT-SEALED 判定；二者均为进程内判定，没有各自独立 Unix rc。actual required union 为138889份原件 |
| 正式 canonical 包 | [永久保留包](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-official-package-attempt1/package)；[manifest](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-official-package-attempt1/package/manifest.json)文件SHA `b5b1a068b609ad41f5963b80dce25313c0c05e7f42bb18e952245d6e16ec717f`，semantic SHA `2645a9f4056c7b0cdbcebf3a31fee04224bf32f709ca732aad202fadee048236`；manifest.file_count 为138940（不含 manifest 自身），physical regular files 为138941、总3524247845字节（约3.524GB，含 manifest） |
| 完整独立 copy/offline | 已通过，[94 原始审计](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt94/independent-copy-outer-attempt1/ORIGINAL-AUDIT.json)、[持久化 RESULT](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/independent-offline-evidence/RESULT.json)；driver/child 原始rc0，独立副本与 official manifest 完全一致。实际两份 nonsecret marker 读拒绝及 network/exec 拒绝均通过；配置5个 deny roots、原22项全部12个 env deny 路径，未逐项探测或读取/stat真实 env；1200秒预算不变 |
| scan2 与外置 seal | [scan2 报告](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/scan2-memory-audit/scan-audit.json) SHA `2e35f364e4c0ea92851397a0303de7769de943e694e1858462e53ceb3b01cc46`，outer/provider/scanner 原始0/0/0，覆盖138941个实际 regular file、3524247845字节（含 manifest/copy-ledger/late metadata）；保护20项输入，actual hit0、suspect0。[外置 Root seal](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/SEAL.RootAPPROVED.json) SHA `a6b21885b502e55cd84a0b0e3a8e82be2676cff2c9911919f29291343a0297ad`；[终端报告](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/TERMINAL-REPORT.RootAPPROVED.json) SHA `7f8edbdab3df34540032cef7d86336ee787c6cf2edc6ee3c60df47c2218b0441`。报告/seal 在包外，scan2 后包未改 |
| 临时副本清理 | 已通过，[实际 RESULT](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-temporary-payload-cleanup-actual-attempt1/RESULT.json)、[97 原始审计](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt97/ORIGINAL-AUDIT.json)、[Root 批准的外置 addendum](/Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/TEMPORARY-CLEANUP-ADDENDUM.RootAPPROVED.json)；外层/内层原始rc0/0，三处目标均不存在。删除138610＋138881＋138941＝416432个临时文件，按文件大小合计8540135396逻辑字节（约8.54GB，不等同于 df 实际回收量）。official manifest、seal 与 terminal report 前后哈希不变，父目录、audit/capture/profile 和本机原件均保留 |

验收、封存和清理的实际原件均已独立核对，P2.6 8/8、P2 已完成当前范围。P3 仍未启动。本次清理只移除 `tools/smoke/business-platform-p2-6/results/w9/final-scan1-payload-staging-attempt1/payload`、`tools/smoke/business-platform-p2-6/results/w9/final-scan1-payload-staging-attempt2/payload` 和 `/private/tmp/openfoundry-p26-final-independent-copy-attempt1/package`。正式 canonical 包、外置报告/seal、audit、capture、profile 及其父目录、本机潜在私密原件及 P0–P2.5 原归档全部保留；本机业务数据、环境文件、数据库和卷不清理。

### 验收标准

- [x] Dataset 同 key/正文顺序与并发提交至多一个已提交版本；改正文/目标/branch/身份绑定拒绝；无 key 旧行为兼容。
- [x] 真实 PG 证明 intent/gate、发布/receipt、本地终态/增量断点/占用/intent finalized 各自原子，迁移空库/旧库/重放通过。
- [x] 新协议 snapshot 与增量均支持：门前崩溃按冻结基线恢复，有 intent 后零 Source 重拉且原字节重放。
- [x] 远端提交丢 ACK、在飞查询未见记录、Dataset/Connector 重启及本地 COMMIT 回应丢失均不重复发布或重复推进。
- [x] 两实例竞争恢复、锁等待过租期、旧 generation、取消与 Stop/排队失败正确 fencing，授权不会串用，slot 不泄漏。
- [x] 发送/查询预算与 backoff 持久、跨重启不重置；每次发送前验证 lease/fresh auth，未知与耗尽有安全可观察状态。
- [x] 租户/完整父链/权限、明文与 opaque checkpoint 安全、既有单测/race/PG 与 P1/P2.1–P2.5 相关 HTTP 回归通过。
- [x] 旧九 Run/八 owner及封存投影无漂移；源码/镜像/真实退出码/秘密扫描/最终归档和完整包离线复验可独立核对；最终临时副本清理已通过，P2.6 验收8/8。

## P3.1 Excel sheet/表头映射（已完成，2026-11-16）

### 最终范围

普通 Dataset 上传一个 .xlsx 工作簿：预创建入口 `POST /v1/datasets/imports/excel/inspect|preview`（JWT，无 dataset 边界）列出 worksheet（chart/dialog 排除）并按 sheet + 1-based 表头行（或无表头）+ 源列物理 index→唯一非空目标名映射完成服务端预览；`POST /v1/datasets/{rid}/excel/import`（写权限 + dataset.format=json 校验）重验证同一配置后规范化为 NDJSON（普通 Dataset format=json，不新增 format=excel），仅登记 .ndjson/.jsonl 为数据文件，原工作簿 SHA/文件名/配置/列/行数记入条目 metadata。Parser 为纯标准库有界 OOXML 实现（zip 中央目录预检、生效 ContentType 拒真实 VBA、坐标/重复/命名空间严格、ST_Xstring 单遍解码、公式仅取缓存且被映射列无缓存即拒、NDJSON 逐 cell 预算 1MiB 行/32MiB 总量与 rows 读端一致）。每次导入写入 UUID 唯一对象，file index 与 catalog 统计（size=Σ全部条目、row_count=Σ excel 条目精确计数）同事务发布，失败全回滚且仅留无害孤儿。/schema 与 /preview 默认从最新可读文件真实字节派生（TEXT/STRING、branch=null）；显式 branch 仅对 excel-import 数据集走与 /files/rows 相同的 committed 字节路径（失败即错误），旧非 Excel 数据集完整保留原 metadata/view 预览。UI：.xlsx 识别进入工作簿导入面板（sheet/header/mapping 配置、服务端预览、generation/abort/canSubmit 防护），非 xlsx 旧流程与 4 个旧 upload 调用方不变。

### 关键验收（隔离 P31 fixture：PG16+DVS+edge-gateway 3931-3933，全部实测）

- 后端：xlsximp 34 项单测、DVS 四包全绿、pipeline handler 全包绿；真实 excelize 生成多 sheet 中文工作簿（说明页/第 3 行表头/007/30 位编号/富文本/空单元格/无表头页）经网关 inspect→preview→create→import→/preview+/schema+/files/rows 三端列值一致；viewer 403、跨组织 404、无认证 401、非法配置 422；缺失 backing/坏 NDJSON 经真实 writer 稳定 422（typed error 回归）；发布阶段精确触发器注入后旧 index/字节/schema/rows/统计全不变仅孤儿 +1；同文件不同映射新 UUID 不覆盖；大 offset 空页；伪造 preview_rows 对默认读取无效且显式 branch 拒样本、index 查询失败 fail-closed 5xx；旧 Parquet metadata 预览与旧 CSV/NDJSON 上传完整兼容；真实 committed branch（committed NDJSON+view schema）可经 ?branch=main 读取并与默认 excel 读数互不混淆。
- Pipeline：dataset_input 生产路径（execution 与实际 preview 入口）连隔离 DVS 读真实 NDJSON，两路径列值一致（env 门控集成测试实测 PASS，默认 skip 已记录）。
- 前端与浏览器：UI worker 74 unit + 7 mock UI + 2 真实数据 API 浏览器 workflow 通过；Root ego-browser 终验通过（dataset 56c8d9eb-715e-4ade-997f-6800da83df80：订单表/header3/四 target→2 行 181B，刷新不变，长编号/007/中文/null 预览正确，schema 四 STRING 正确，旧 Excel aside 与假 Transactional 选项已隐藏）。

### 边界与限制

- 网关沿用既有租户请求配额：10MiB envelope（超出 413）；parser 文件上限 32MiB 仅 DVS 层直测，服务上传上限受网关配额约束。
- 单 worksheet、表头行 1..100 或无表头、≤1000 数据行、≤256 目标列、≤512 源列宽、≤64 表；超出明确拒绝不截断。
- 单元格一律 raw 字符串（富文本拼接、_xHHHH_ 解码、编号/长数字保真）；blank=null 为本轮固定行为，decimal/日期/时区/空值与标识符规则（P3.2）、.xls/.xlsm、多文件选择、外部批次上传、爬虫来源、质量报告与错误行隔离均未实施，属 P3 后续范围。

## 矩阵驱动的执行顺序（取代原「后续阶段的具体任务」）

本节取代上文 P3 剩余、P4、P5 的任务序列，改由[数据连接与集成需求覆盖矩阵](data-integration-requirements-coverage-matrix.md)（DI-01..DI-12）驱动。以下批次只定义依赖顺序与并行关系，**不承诺日期，不自动推进业务编码**；接口合同可提前跨组对齐，不机械串行等待其他组。P2 六项与 P3.1 的历史完成状态不变（见上文历史章节）。

### 当前进度登记（2026-11-17，未提交工作树）

- **S1（WP1.2）工程契约 v1 已冻结**：[data-integration-s1-dataset-contract.md](data-integration-s1-dataset-contract.md)（Dataset 身份、版本、Schema、输入/输出发布与租户权限契约；B2 开工门槛据此满足，见批次 B）。
- **WP2.1 / WP2.2 安全修复本轮工程验收通过**（GLM 编制执行 / Codex review，最终证据 run `20261117T024942_29906`）：[data-integration-s1-security-acceptance.md](data-integration-s1-security-acceptance.md)。区分两层事实：**基线 `424921bd` 上的缺口**（Connector `registrations.go` InvokeWebhook/ListWebhookHistory 跨租户读取与广义 admin 判定；`deriveCredentialKey` 缺独立 key 时 JWT 派生 fallback；`InvalidEnvError` 携带密钥明文进 fatal log）为矩阵登记的基线状态；**当前未提交工作树已修复并通过工程验收**（跨租户统一 404 且零出站/零历史副作用、生产及未知环境 fail-closed、错误脱敏、开发 compose 显式 canonical 信号），代码尚未提交，提交前矩阵基线条目不回改。
- DI-01（Core concepts）与 DI-02（指定连接器）**整项均未完成**：上述仅 S1 契约冻结与两项安全缺口修复，B1/B3 的业务实现仍按批次计划待做。
- **WP3.1（T8 决策支持）工程评估完成（2026-11-17）**：[data-integration-wp3-1-t8-scheduling-assessment.md](data-integration-wp3-1-t8-scheduling-assessment.md)（GLM 编制，Codex 两轮 review+最终局部条件复核）。接线差距盘点/四方案比较/待裁决清单/WP3.2 验收草案齐备；**工程评估完成≠机制获批≠接线验收**——T8 仍未裁决，WP3.2 未实施、未验收。提交状态更正：S1 契约、安全验收与本节此前规划登记**已随 `f4c6194ca93d2899162887bbde10ff4126fdf618` 提交并推送至 `origin/codex/dataset-pipeline-file-io`**，上文「未提交工作树」等措辞保留为验收时点快照；WP3.1 评估为该提交之后新增，不包含在该提交中。
**WP3.2 工程实现与真实隔离验收完成（2026-11-17，本轮纳入分支提交；提交与远端状态以 Git 记录为准）**：用户已选 ADR-0037 CronJob→Kafka 本模块基线；GLM-5.3 初始实现，额度耗尽后 GPT-6.1 Sol 继续，Codex review。API/producer/CDC/Apicurio/canonical 产品及 V1–V8 均通过，见[隔离验收报告](data-integration-wp3-2-acceptance.md)、[决策记录](data-integration-t8-scheduling-decision.md)、[部署 profile](data-integration-wp3-2-deployment-profile.md)。非生产部署、默认 Off；生产 authorizer/DSN/Secret 等前置仍须满足。DI-03/B2 不整体关闭、manual F15 不全局关闭；T2 及其他 T 项、其它 WP 状态不变，WP3.3 不自动启动。历史 P0–P3.1 与原未决措辞保留为登记快照；T0 314–555 与各 WP 冻结估算、源版本均不变。

### 批次 A：范围与执行契约对齐

- 逐项裁决/推进矩阵「待对齐项」T0–T9：**T0 交付库重估——工程重估与复核完成（2026-11-17）**：[T0 重估报告](data-integration-openfoundry-t0-reestimate.md)按本仓证据完成 12 项复核（编制 GLM-5.3、工程确认 Codex，12 项实际 review 及算术/范围/依赖复核通过；已知必做合计 314–555 工程人日+替代情景/未知量单列，口径已可适用）。批次 B 正式业务实现与正式排期的 T0 前置**已满足**；源路线图任务点与日期仍仅作参照、不换算（Allen 人日/日期确认与 T1–T9 裁决另行进行，不因 T0 工程完成而视为已批准）；T1 Core 完整能力（Media sets/Iceberg/Virtual tables/Views 继承或后挪，四细点仍待源标注统一）；T2 管道代码交付契约（源规划 PipelineDef/TS/DuckDB 与本仓 Go pipeline_ir.v1/local-table runtime 的映射，不自行换引擎/改语言）；T3 健康检查类型映射（PL-26 六类与官方 checks-reference 五类+独立 expectations）；T4 S3 兼容 API 优先级；T5 Excel Parser 分批范围；T6 出站 Webhook 归属；T7 电商连接器映射；T8 管道调度机制（本模块已选 ADR-0037 CronJob→Kafka，WP3.2 工程实现与真实隔离验收已完成，当前调度 gate 已满足，见[验收报告](data-integration-wp3-2-acceptance.md)；非生产部署，生产前置仍见[部署 profile](data-integration-wp3-2-deployment-profile.md)）；T9 DI-08 基础预览与 SQL 扩展的产品关闭条件（C2/F1 按其两种方案写条件，不替产品决定）。
- 与跨组对齐接口合同：本体 Indexing（对象侧）、对象应用端（血缘应用节点）、可观测性健康平台、Global Branching、安全项目角色、Action 出站调用。
- T0 重估完成是批次 B 正式业务实现与正式排期的前置；T1–T9 各只阻塞各自受影响工作（见矩阵待对齐表「未决期间可推进」列），不构成批次 B 的整体硬阻塞。本仓盘点、契约整理（S1 已冻结）与两项安全缺口修复（WP2.1/2.2 已工程验收，见「当前进度登记」）已于 2026-11-17 完成，均未提交。

### 批次 B：三个 P0 补差（T0 重估前置已满足；B1/B3 与 B2 非调度部分可并行）

- 本批正式业务实现与正式排期的 **T0 重估前置已满足**（2026-11-17 工程重估与复核完成，见批次 A；源路线图估算不转成本仓承诺——正式承诺须交付负责人确认与相关裁决落定）。**B2 开工门槛（Dataset 身份、版本、Schema、输入/输出发布与租户权限契约冻结，不必等 DI-01 全部 Core 细点）已由 S1 工程契约 v1 满足**（2026-11-17 冻结，见[契约文档](data-integration-s1-dataset-contract.md)与「当前进度登记」）；**B2 管道调度实现另以 T8 为前置**，不能用 P2.4 同步链验收替代。
- **B1 Core concepts（DI-01）**：数据集/构建/定时/健康检查概念照官方立；Branching、CDC 补齐；冲突细点按批次 A 裁决执行（Media sets/Iceberg 独立后续、Virtual tables/Views 建议后挪，两类分别处理）。
- **B2 代码管道（DI-03）**：Dataset 身份、版本、Schema、输入/输出发布与租户权限契约**已由 S1 工程契约 v1 冻结（2026-11-17），后续以该契约为依据**；管道调度分项已由 WP3.2 完成工程实现及真实隔离 V1–V8 验收（[验收报告](data-integration-wp3-2-acceptance.md)），本模块 T8 gate 已满足；DI-03/B2 不整体关闭，T2 与其他交付缺口保留；**CSV/JSON 结构推断不在本批 P0 范围**——CSV 配置由 DI-08 持久化、本项消费同一份配置不另建模型，非结构化结构推断为 P2、不阻塞 B2 P0 收尾，排在 C2 配置交付之后的批次 D P2 子任务；代码交付契约按批次 A 结论接线。
- **B3 指定连接器（DI-02）**：REST 已验收；补 S3、PostgreSQL、MySQL 的**可执行服务端源读取/接线/发布**（现状：S3 adapter 不读 payload、MySQL 走 HTTP bridge、PostgreSQL 专用实现未定位仅有 JDBC/CDC 线索；复用现有 adapter 源码或补新实现由执行契约确定，不预设 native driver/Arrow 方案）；电商三个连接器（Amazon 经紫鸟、noon、领星）先完成实现映射盘点；出站 Webhook 租户缺口（基线 `424921bd` 的 registrations.go 非租户读取与广义 admin 判定）**已在当前未提交工作树修复并通过工程验收**（WP2.1，见[验收报告](data-integration-s1-security-acceptance.md)），DI-02 其余实现仍待做。

### 批次 C：P1 支撑四项（可并行）

- **C1 维护管道（DI-04）**：健康检查真实数据求值与类型映射落地；数据期望 input/pre、output/post、WARN/ABORT、坏输出不可见、结果绑定 build/version。先用 build 输入输出合同，不等完整血缘应用链。
- **C2 预览（DI-08）**：本批交付基础预览（历史版本 data/schema 回看，diff/key matching/schema diff/bisect/retention 分批清单）与 CSV 解析持久化配置（预览、DI-03 推断与正式执行读同一份配置）。「Follow a SQL query through time」等 SQL 扩展按 **T9 两种产品关闭方案**写条件，不替产品决定：若裁为随 DI-09 关闭，则基础预览全部验收后 DI-08 即可关闭、SQL 扩展在批次 F 补验收；若保留为 DI-08 关闭门槛，则本批不宣告 DI-08 完成、批次 F 补验收后才关闭。两种方案都不把整个 DI-09 提前、不形成环；裁决前不宣告 DI-08 整项完成。
- **C3 表达式（DI-05）**：现有清单核对+补缺（不照官方 338 全量）；签名目录与执行子集一致性。
- **C4 变换（DI-06）**：目标仍为产品口径 25 项——建立 25→本仓静态 24 项+动态 UDF 的逐项映射（缺失/别名/重复），映射盘点可与实现并行，不能把验收清单直接缩成 24；新建条件聚合/窗口聚合/反连接三项（源码静态核对确认执行面均无；注意三项对表达式求值语义的依赖，与 C3 协同）。
- DI-07 血缘的跨组接口合同在本批完成对齐（不实现全链）。

### 批次 D：阶段 1 收尾验收

- **D1 只读画布（DI-10）**：以批次 B2 研发代码发布路径已可用为前提收敛编辑入口；保留节点选择、节点预览、构建详情；不删除研发发布 API，不机械全局 readOnly（PipelineCanvas 的 selectNode 与 readOnly 耦合需先解耦）。
- **D2 Excel 管道变换（DI-11）**：raw workbook 作为管道输入的解析变换（区别于 P3.1 普通上传），分批范围按批次 A 裁决；不引 Java/Spark 依赖。
- **D3 结构推断（DI-03 的 P2 细点，不阻塞 B2 P0 收尾）**：排在 C2 的 CSV 持久化配置交付之后——CSV 消费 DI-08 同一份配置（预览与执行一致），JSON 嵌套展开与类型推断，推断结果进 schema 可查；不另建 CSV 参数模型。

### 批次 E：阶段 2 全链血缘（DI-07）

- 数据集→管道→对象→应用四类节点全链图与影响面；持久化存储、分支、历史、租户隔离验证。依赖批次 A 的跨组合同与本体/应用侧节点接入；不把延期的 Kafka/Iceberg 作为强制前置。

### 批次 F：阶段 4 SQL 与 S3 兼容 API（DI-09、DI-12）

- **F1 SQL（DI-09）**：数据集与本体**双侧整项目标保留**（Dataset 首批、对象随后，14 细点逐项对应范围与验收，仅新产品裁剪记录可删减）；经真实查询引擎可查；DI-08 的 SQL 扩展（Refine query 随版本移动）按 T9 裁决在此补验收——若 T9 裁为随 DI-09 关闭，本批补验收即关闭 DI-08 的 SQL 扩展；若保留为 DI-08 门槛，本批补验收后 DI-08 方可整项关闭；两种方案的关闭条件均写入矩阵。
- **F2 S3 兼容 API（DI-12）**：优先级与动作子集、认证 profile 按批次 A 裁决后实施，用标准 AWS SDK/CLI 验收；内部 presign 与 S3 源 adapter 不能替代。

### 与旧阶段任务的映射

- 旧 P3.2（decimal/日期/时区/空值与标识符规则）归入 DI-05/DI-06/DI-08 的解析与表达式需要项，不单独排期。
- 旧 P4 不笼统交 T1/T2：对象存储配置与授权、分批读写（不能只读）、产物 manifest 提交、固定输入快照、schema/逻辑版本关联、真实数据查询引擎、统一多输出发布、容量基准八项逐项映射（当前证据、缺口、责任人与验收批次）见[矩阵](data-integration-requirements-coverage-matrix.md)「旧 P4 逐项映射」；十万行为初始建议基准，客户规模/延迟目标到手后记录并重新定标，正确性/内存有界/失败不暴露半成品门槛不因重新定标取消。
- 旧 push 入口、listeners、爬虫结果接入不是默认主线：push/listeners 在裁剪汇总属后续细点（Exports、Listeners、Email listeners 等），爬虫来源未列入本期 12 项；有客户诉求按矩阵外需求另行立项。
- 旧 P5 行业方案（电商/B2B 样板、客户安装升级）转为跨组验收用例，不再作为本模块交付主线。

## 当前证据位置

- P1 最终验收：`tools/smoke/business-platform-p1/results/2026-11-13-build-http/summary.md`（9 PASS、0 FAIL、0 UNCOVERED；60 包单测；26 项必需 PG16）。更早的 `results/2026-11-13-p1-mainline-baseline/` 为 7/0/1 未覆盖时点基线，不与最终结论混用。
- P2 最终封存：`tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/FINAL-BRIEF.md` 及同目录批准 seal/terminal 报告。
- P3.1：本计划 P3.1 章节 + commit `664165f3` + 仓库内测试；`/private/tmp` 下报告为本机临时证据，不视为入仓闭包。
- T0 重估：[data-integration-openfoundry-t0-reestimate.md](data-integration-openfoundry-t0-reestimate.md)（2026-11-17 工程重估与复核完成：编制 GLM-5.3、工程确认 Codex；已知必做合计 314–555 工程人日+替代情景/未知量；Allen 人日/日期确认与 T1–T9 裁决另行进行）。
- S1 工程契约 v1（已冻结）：[data-integration-s1-dataset-contract.md](data-integration-s1-dataset-contract.md)（2026-11-17；B2 开工门槛据此满足）。
- WP2.1/2.2 安全验收：[data-integration-s1-security-acceptance.md](data-integration-s1-security-acceptance.md)（工程验收通过 2026-11-17，GLM 编制执行 / Codex review；最终证据 run `20261117T024942_29906`：HTTP+PG 29/29、二进制门槛 11/11、11 条 command/tee/verify 退出码全 0、负控 16 拒/3 受/4 真实包装层；代码在未提交工作树）。
- 上轮文件 I/O 基线 `/private/tmp/openfoundry-file-io-sryufy3u/result.json` 仍为本机临时证据。
- 证据存放说明：上述 P1/P2 的 `tools/smoke/**/results/` 归档为本机工作树文件、非 Git 跟踪（`.gitignore` 忽略，fresh clone 不自带，获取位置即上述本机路径）；源码/测试/fixture/验收工具脚本在 Git 内（`2128e55d`）。历史验收结论不受存放位置影响。
- 当前文件读取为 local 后端与轻量数据量，后续阶段将单独验证正式批处理能力。
- 当前 REST snapshot batch Sync 定时调度已按 P2.4 验收；长期无人值守授权、Connector Excel adapter 未注册接线（源码目录存在、主服务未注册，见矩阵 DI-02/DI-11）、Iceberg 输出接线（ADR-0041 现存设计待接线）和其余跨模块租户约束仍待后续实施。
