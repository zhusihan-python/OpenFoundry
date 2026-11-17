# S1：Dataset 身份、版本、Schema、发布与权限契约

**状态：工程契约 v1 冻结（2026-11-17）。** 编制 GLM-5.3（S1 文档 worker）；Codex 工程 review 两轮：第一轮（2026-11-17）修正 7 类事实并登记 §10 三项工程兼容裁定；第二轮（2026-11-17）复核通过（仅余一处零 sub 表述已修正），**本轮工程复核通过、登记冻结**。冻结范围=接口与规则；F15（Pipeline ACK）/F16（零 sub）等 GAP 保持未实施，不代产品/架构批准，不宣告 vNext 交付。本文是矩阵「S1=WP1.2」的产物（[覆盖矩阵](data-integration-requirements-coverage-matrix.md) DI-01、[T0 重估](data-integration-openfoundry-t0-reestimate.md) §3 共享包 S1/WP1.2；主计划[批次 B2 开工门槛](business-platform-implementation-plan.md)）。工程冻结只固定接口与规则：不代替产品/架构签字，不替 T1–T9 裁决（尤其 T2、T8），也不构成任何 WP 的实现验收。

- **源码基线**：分支 `codex/dataset-pipeline-file-io` @ `424921bd`（与 `664165f3` 源码相同）。文中行号均于 2026-11-17 静态核对。**当前工作树含并行安全 worker 对 [registrations.go](../../services/connector-management-service/internal/handlers/registrations.go) 的未提交修复（Webhook 租户校验）：WP2.1 已于 2026-11-17 工程验收通过（见[安全验收报告](data-integration-s1-security-acceptance.md)），修复代码本身仍属未提交工作树、不属于本基线，基线缺口事实照旧登记（§2 及文末确认记录）。**
- **证据分级沿用矩阵 A–E**。规则编号 CR-*；每条分「当前真实支持」「v1 承诺」「vNext」（S1 明确不承诺）。

---

## 1. Dataset 稳定身份与租户归属

### CR-1.1 身份形式：UUID 主键 + 服务端 RID

- Dataset 的稳定身份是 `datasets.id`（UUID，[models.go:34](../../services/dataset-versioning-service/internal/models/models.go)）；对外 RID 一律为 `ri.foundry.main.dataset.<uuid>`，由服务在创建时拼接（[repo/tenant.go:208](../../services/dataset-versioning-service/internal/repo/tenant.go)），**调用方不能自定义 RID 字符串**。
- API 引用双形等价：裸 UUID 与 RID 均可寻址（`ResolveDatasetForTenant` 先按 UUID 解析、失败按 RID，[repo/tenant.go:75-93](../../services/dataset-versioning-service/internal/repo/tenant.go)）。`CreateDatasetRequest.ID` 允许调用方预指派 UUID（[models.go:60](../../services/dataset-versioning-service/internal/models/models.go)），但 outputs:commit 的 `create_if_missing` 路径发现该 UUID 已全局存在（任何租户）时拒绝创建并 404，不覆盖、不泄露存在性（[output_commit.go:590-602](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。
- 其他实体的 RID：branch `ri.foundry.main.branch.<uuid>`（[models.go:144-149](../../services/dataset-versioning-service/internal/models/models.go)）、transaction `ri.foundry.main.transaction.<uuid>`（[models.go:171-177](../../services/dataset-versioning-service/internal/models/models.go)）。v1 承诺：这三种 RID 前缀互不混用、不做别名。

### CR-1.2 租户推导：只从验证过的 JWT claims

- 可信身份 = `authmw.Claims`（[claims.go:30-48](../../libs/auth-middleware/claims.go)）：`sub`（UUID）与 `org_id`（可空 UUID）。DVS 侧投影为 `TenantScope{OrgID, Subject}`（[handlers/tenant.go:30-32](../../services/dataset-versioning-service/internal/handlers/tenant.go)；[repo/tenant.go:24-30](../../services/dataset-versioning-service/internal/repo/tenant.go)）；Pipeline 侧为同构 `TenantScope`，经中间件挂到请求上下文（[tenancy.go:22-76](../../services/pipeline-build-service/internal/handler/tenancy.go)）；Connector 侧为 `repo.Tenant{OwnerID, OrgID}`（[handlers.go:212-218](../../services/connector-management-service/internal/handlers/handlers.go)）。
- **v1 规则：请求参数、请求体、转发头永远不参与租户推导**。创建 Dataset 的 org 归属只取自 claims（`CreateDatasetForTenant`，[repo/tenant.go:170-212](../../services/dataset-versioning-service/internal/repo/tenant.go)；请求体无 org 字段）。历史验收：P1.1「修改 owner_id、组织头或请求体 tenant 字段无法扩大访问范围」。
- **GAP（现状，第一轮 review 修正、第二轮复核定稿）**：`DecodeToken`（[jwt.go:133-165](../../libs/auth-middleware/jwt.go)）不校验 `sub` 缺失或 `uuid.Nil`——缺/零 `sub` 未被显式拒绝；通过其余 JWT 校验后 `Sub` 可为零 UUID。**租户域仍由 `OrgID` 是否存在决定**：带 `org_id` 的令牌即使缺 `sub` 也走组织域（`Covers` 第一臂只比对 OrgID，[repo/tenant.go:40-44](../../services/dataset-versioning-service/internal/repo/tenant.go)）；仅无 `org_id` 时才使用个人域谓词（此时零 `Sub` 几乎不匹配任何行，但这是谓词巧合而非显式拒绝）。v1 不改 auth 库；将「缺/零 `sub` 显式拒绝」列为后续要求（F16），当前不得声称已验证身份模型拒绝零 sub。无 Authorization/坏 JWT/过期仍为 401（含 issuer/audience 校验与 exp 检查，[jwt.go:143-164](../../libs/auth-middleware/jwt.go)）。

### CR-1.3 可见性规则（单一规则，全路由挂点）

`TenantScope.Covers`（[repo/tenant.go:40-45](../../services/dataset-versioning-service/internal/repo/tenant.go)）：

| 调用方 claims | 可见 Dataset |
| --- | --- |
| 带 `org_id`（组织域） | `datasets.org_id == org_id` 的全部行（同组织全员可读，owner 字段不限制读） |
| 无 `org_id`（个人域） | 仅 `org_id IS NULL AND owner_id == sub` 的行 |

SQL 形式 `tenantVisibility`（[repo/tenant.go:52-54](../../services/dataset-versioning-service/internal/repo/tenant.go)）。推论（v1 冻结）：

- **CR-1.3a 组织是租户边界：admin 角色、权限 grant、owner 身份都不得跨越它**（DVS 删除路由注释 "cross-tenant datasets 404 regardless of the caller's scopes (admins included)"，[handlers.go:346-360](../../services/dataset-versioning-service/internal/handlers/handlers.go)；行读授权 `requireDatasetRead` 只做租户判定，[file_rows.go:147-168](../../services/dataset-versioning-service/internal/handlers/file_rows.go)）。
- **CR-1.3b 同一 Sub 跨组织≠同域**：组织请求永远够不到个人行，反之亦然（[repo/tenant.go:47-54](../../services/dataset-versioning-service/internal/repo/tenant.go) 注释）。P1/P1.5 HTTP 验收均含同 Subject 双 Org 场景。
- **CR-1.3c 历史（无 Org）数据留在个人域**：`org_id IS NULL` 旧数据仅原 owner 可访问；组织请求不得自动认领。旧数据转入组织必须有显式迁移过程（v1 不定义迁移 API，vNext）。
- **CR-1.3d 跨租户与不存在同形**：不可见引用（含纯 UUID 血缘目标、外域 view id）一律 404（[repo/tenant.go:56-68](../../services/dataset-versioning-service/internal/repo/tenant.go)；血缘校验 [handlers/tenant.go:59-88](../../services/dataset-versioning-service/internal/handlers/tenant.go)；view 越域 [handlers/tenant.go:90-112](../../services/dataset-versioning-service/internal/handlers/tenant.go)）。

### CR-1.4 权限模型现状（裁定②）

- 写授权统一为 scope 判定：`admin` 角色 **或** `dataset.write`/`dataset:write`（通配 `dataset:*`、`*:*` 命中，[claims.go:73-98](../../libs/auth-middleware/claims.go)）——`canWriteDataset`/`requireDatasetWrite`（[handlers.go:377-385](../../services/dataset-versioning-service/internal/handlers/handlers.go)；[catalog_parity.go:35-46](../../services/dataset-versioning-service/internal/handlers/catalog_parity.go)）。管理面另用 `dataset.admin`（[catalog_parity.go:48-59](../../services/dataset-versioning-service/internal/handlers/catalog_parity.go)）。**DVS 写授权没有 owner 旁路：个人域 owner 若无 scope 同样 403**。
- **Codex 裁定②（2026-11-17）**：v1 以当前已验收 tenant/claims 语义为兼容边界；`PUT /v1/datasets/{rid}/permissions` 的 `DatasetPermissionEdge`（[models.go:377-388](../../services/dataset-versioning-service/internal/models/models.go)；[parity.go:203-246](../../services/dataset-versioning-service/internal/repo/parity.go)）**仅为目录元数据，不参与任何授权判定**，B2 不得依赖其执法、本裁定也不由此批准任何访问扩大；细粒度项目 RBAC 为后续（不属 S1 代签）。

## 2. 授权挂点：逐路由真实顺序（第一轮 review 修正）

**顺序不是全路由一致**；各路由真实错误顺序如下（401=无/坏 JWT；403=scope 不足；404=租户不可见或不存在，同形）：

| 路由类 | 真实顺序与通过条件 | 代码挂点 |
| --- | --- | --- |
| `POST /v1/datasets`（创建） | 401 → **403（scope）** → 创建成功（org 随 claims） | [handlers.go:222-224](../../services/dataset-versioning-service/internal/handlers/handlers.go) |
| `POST {/v1,/api/v1}/datasets/{rid}/outputs:commit`（含 keyed 重放） | 401 → **403（canWriteDataset 先判）** → 404（目标租户解析） → 语义校验 | [output_commit.go:73-82](../../services/dataset-versioning-service/internal/handlers/output_commit.go)；legacy 404 在 [output_commit.go:141-158](../../services/dataset-versioning-service/internal/handlers/output_commit.go)，keyed 在外层事务租户锁内 |
| `/v1` parity 写面（views/branches/file index/lineage/markings/permissions 的 PUT/POST） | 401 → **404（resolveDatasetForCatalog 租户）** → 403（requireDatasetWrite/Admin） | [catalog_parity.go:27-59](../../services/dataset-versioning-service/internal/handlers/catalog_parity.go) |
| legacy `PATCH/DELETE /datasets/{id}` | 401 → 404（GetDatasetForTenant） → 403（canWriteDataset） | [handlers.go:346-360](../../services/dataset-versioning-service/internal/handlers/handlers.go) |
| 全部读面（dataset/versions/branches/files/preview/schema/files/rows）与 `GET /outputs/commits/{key}` | 401 → 404（仅租户判定，无读 scope） | [file_rows.go:147-168](../../services/dataset-versioning-service/internal/handlers/file_rows.go)；[output_commit.go:516-542](../../services/dataset-versioning-service/internal/handlers/output_commit.go) |

场景矩阵（按上述顺序合成）：

| 场景 | 同租户 owner（无 scope） | 同租户普通成员 | 同租户 admin/`dataset.write` | 异租户（同 Sub 切组织/他 org admin/他 org 持 scope） | 无 Org 个人域对他人旧数据 |
| --- | --- | --- | --- | --- | --- |
| 读（含 rows、GET receipt） | ✅ | ✅（同 org 全员） | ✅ | 404 | 404 |
| 写/发布（outputs:commit、PUT 面、PATCH/DELETE） | **403（owner 无旁路）** | 403 | ✅ | outputs:commit：403 先于 404；PUT/PATCH 面：404 | 404 |
| 创建 Dataset | 403 | 403 | ✅（新归属=调用者域） | n/a | n/a |
| Connector 触发同步→发布 | 触发面：**owner / admin / `connections:write` 三者 OR**（租户命中的资源上，任一即可，[handlers.go:225-245](../../services/connector-management-service/internal/handlers/handlers.go)）；发布面：转发调用者 JWT 走 outputs:commit（scope 规则同上行） | | ✅ | 404 | 404 |
| Pipeline 消费（rows 读 + resolver 预检） | ✅（读面） | ✅ | ✅ | 404 / resolver 拒绝（[build_resolution.go:86-95, 129-151](../../services/pipeline-build-service/internal/handler/build_resolution.go)） | 404 |

- Pipeline 的输入/输出/定义在 CreateBuild/DryRun 副作用前逐一租户预检（`validatePipelineParent` + `preflightRequestedDatasetVisibility`，[build_resolution.go:60, 129-151](../../services/pipeline-build-service/internal/handler/build_resolution.go)）；分库且 Dataset schema 无租户列时显式 503 `dataset_tenant_boundary_unsupported`，不回落共库无租户 SQL（[build_resolution.go:96-101, 210-243](../../services/pipeline-build-service/internal/handler/build_resolution.go)；P1 uncovered 项由 P1.5 HTTP 适配关闭）。
- 跨租户写路径纵深防御（v1 冻结）：caller 提供的 storage_path 只能落在本 dataset 命名空间或本 dataset 事务下（`validateStorageRefForDataset`，[handlers/tenant.go:114-169](../../services/dataset-versioning-service/internal/handlers/tenant.go)）；content-bearing 提交的物理路径由服务生成（[output_commit.go:676-681](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。
- **已知例外（基线 `424921bd` 事实；当前工作树已修复待提交）**：Connector 出站 Webhook 在基线按非租户 `GetConnection` 读取（[registrations.go:737](../../services/connector-management-service/internal/handlers/registrations.go) 起），不满足 CR-1.3a；并行安全 worker 在当前未提交工作树的修复（WP2.1）已于 2026-11-17 工程验收通过（真实 HTTP+PG16 证明跨租户 404、零出站/零历史副作用，见[安全验收报告](data-integration-s1-security-acceptance.md)），该验收不改变本契约的基线口径，提交入库前本条按基线保留。

## 3. 五个身份概念的区别与读取语义

### CR-3.1 概念身份对照表

| 概念 | 身份/字段 | 归属粒度 | 生成方 | API 面 | 主要调用方 |
| --- | --- | --- | --- | --- | --- |
| Branch | `name`（dataset 内唯一）+ branch UUID/RID；`is_default` 标记 main（[repo.go:280-300](../../services/dataset-versioning-service/internal/repo/repo.go) 创建 `'main'`） | Dataset | DVS | `GET/POST /v1/datasets/{rid}/branches…`（[server.go:122-136](../../services/dataset-versioning-service/internal/server/server.go)） | 前端、pipeline resolver（`ListBranches` adapter，[dataset_http_versioning.go:160](../../services/pipeline-build-service/internal/handler/dataset_http_versioning.go)） |
| 逻辑版本 version | **三字段并列**（[models.go:81-91](../../services/dataset-versioning-service/internal/models/models.go)）：`DatasetVersion.id`（UUID，版本行身份）、`version`（int32，dataset 内发布时 `nextVersionForUpdate` 递增切割，[publish.go:107-120](../../services/dataset-versioning-service/internal/repo/publish.go)）、`transaction_id`（UUID，来源事务） | Dataset（`dataset_branches.version` 跟随，[publish.go:121-125](../../services/dataset-versioning-service/internal/repo/publish.go)） | DVS 发布事务 | `GET /v1/datasets/{rid}/versions`；`GET /api/v1/datasets/{id}/versions/{version}`（[server.go:57-59, 120](../../services/dataset-versioning-service/internal/server/server.go)） | 前端版本列表（P3.1） |
| 事务 txn | UUID + RID `ri.foundry.main.transaction.<uuid>`；OPEN/COMMITTED/ABORTED；SNAPSHOT/APPEND/UPDATE/DELETE（[models.go:237-244, 837-851](../../services/dataset-versioning-service/internal/models/models.go)） | Dataset × Branch（每分支同时至多一个 OPEN，[transactions.go:46-49](../../services/dataset-versioning-service/internal/handlers/transactions.go) 409） | DVS `StartTransaction` | `POST/GET /v1/datasets/{rid}/branches/{branch}/transactions…`、`transactions:batchGet`（[server.go:138-144](../../services/dataset-versioning-service/internal/server/server.go)） | pipeline resolver（Open/Abort，P1.5）、Connector（读回执） |
| Schema/视图 | view UUID（`dataset_views`）；schema 行按 view_id 存储，**无独立版本字段**，仅 `content_hash`（[models.go:1080-1094](../../services/dataset-versioning-service/internal/models/models.go)） | Dataset × view | DVS（`PutViewSchema` upsert） | `GET/PUT /v1/datasets/{rid}/views/{view_id}/schema`、`GET /v1/datasets/{rid}/schema`、`GET /v1/datasets/{rid}/views/at?branch&ts&transaction_id`（[server.go:148-158, 174](../../services/dataset-versioning-service/internal/server/server.go)） | 前端、pipeline（`ViewSchema` adapter） |
| file index | 条目 path（dataset 内唯一）+ 存储物理 URI；**dataset 级、无 branch/版本维度**（[models.go:404-416](../../services/dataset-versioning-service/internal/models/models.go)；发布时整表替换 [publish.go:139-164](../../services/dataset-versioning-service/internal/repo/publish.go)） | Dataset | DVS 发布事务 / upload 直写 | `GET/PUT /v1/datasets/{rid}/files/index`、`GET /v1/datasets/{rid}/files`（[server.go:117-118, 160](../../services/dataset-versioning-service/internal/server/server.go)） | 前端、行读（无 branch 时）、Excel 导入统计 |

### CR-3.1b 提交身份链（第一轮 review 修正：谁是"实际提交身份"）

- **实际提交身份 = 发布响应的 `transaction.id`**（`CommitDatasetOutputResponse.Transaction` 为 `RuntimeTransaction`，wire 字段 `id`，[models.go:837-843](../../services/dataset-versioning-service/internal/models/models.go)）。
- **Connector 的事实**：`SyncRun` 的版本引用字段实际存 `commit.TransactionID`（事务 UUID，[handlers.go:2316-2322](../../services/connector-management-service/internal/handlers/handlers.go)），**不是 `DatasetVersion.id`**；opaque completion checkpoint（[rest_api_sync.go:78-83](../../services/connector-management-service/internal/models/rest_api_sync.go)，分页 cursor 绝不充当数据 checkpoint）既非事务 UUID 也非版本行 UUID。
- **Pipeline 的事实**：resolver 预开的输出 `TransactionRID` 只进入提交体的 `provenance` 元数据（[dataset_output_committer.go:221-227](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)；请求体本身无 transaction 字段，:118-132）；DVS legacy 提交另开自己的事务（[output_commit.go:218](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）——预开事务与发布事务是**两个不同对象**，两者仅靠 provenance 关联，无服务端绑定。
- **禁止混淆（v1 冻结）**：`DatasetVersion.id` ≠ `version` int32 ≠ `transaction_id`；Connector 的 `dataset_version_id` 字段名与其存储内容（事务 UUID）不一致，消费方不得按字面当版本行引用；checkpoint ≠ 上述任一。
- **GAP（F15）**：Pipeline 输出客户端只校验 HTTP 2xx（[dataset_output_committer.go:64-73](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)），**没有** Connector 同级的 usable-ACK 校验（`Status=="COMMITTED"` 且 `DatasetID` 匹配，[runtime/sync.go:103-113](../../services/connector-management-service/internal/runtime/sync.go)），也不读回执——不得宣称两链路发布确认语义等价。

### CR-3.2 active branch、默认分支与显式 branch（裁定①贯穿）

- `datasets.active_branch` 创建即 `'main'`（[repo/tenant.go:204](../../services/dataset-versioning-service/internal/repo/tenant.go)），默认分支行由 `EnsureDefaultBranch` 建立（[repo.go:291-293](../../services/dataset-versioning-service/internal/repo/repo.go)）。
- outputs:commit（legacy 无 key 路径）分支解析顺序：`body.branch` → `dataset.active_branch` → `"main"`；显式名找不到时**回落 main**（[output_commit.go:184-200](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。keyed 路径必须显式 branch、无任何回落（[output_commit.go:331-335, 409-420](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。
- `current_version`/`size_bytes` 仅当提交 branch == active_branch 时推进 dataset 行（[publish.go:126-131](../../services/dataset-versioning-service/internal/repo/publish.go)）。
- **Codex 裁定①（2026-11-17）**：**新 B2 调用一律显式传 `branch`（推荐 `branch=main`）**，这是 v1 契约要求；既有的不一致省缺——views/schema/compare 读端点省缺 `"master"`（[views_schema_preview.go:104-107, 121-124, 377-380](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)；[parity.go:1278-1279](../../services/dataset-versioning-service/internal/repo/parity.go)）与发布链省缺 `main`——**作为兼容 GAP 保留（F14），本轮不改路由默认**；省缺行为是各路由的兼容现状，**不构成跨路由一致的默认保证**。

### CR-3.3 读取语义：version / ts / txn

- **当前真实支持**：`GET /views/at?branch=B&ts=<RFC3339>` 与 `?transaction_id=<uuid>`（txn 折算为其 committed_at 再计算视图，[views_schema_preview.go:116-149](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)；[parity.go:1266-1275, 1311-1339](../../services/dataset-versioning-service/internal/repo/parity.go)）——时间旅行原语已在（矩阵 DI-08 B 级）。行读端点带 `branch=` 读该分支 committed 事务文件（[file_rows.go:232-265](../../services/dataset-versioning-service/internal/handlers/file_rows.go)）。
- **v1 承诺（B2 可依赖，按裁定①显式 branch）**：带 `branch` 走该分支 head 的 committed 视图；无 `branch` 的 latest 读取（file index 最新条目；execution 模式多文件歧义拒绝、preview 模式取最新，[file_rows.go:194-230](../../services/dataset-versioning-service/internal/handlers/file_rows.go)）仅为兼容现状。**不承诺**：`files/rows` 的 version/ts 入参（不存在）；预览端点的 version/ts 入参（previewQuery 仅 limit/offset/format，[views_schema_preview.go:423-439](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)）；`/v1` 面按 version 的下载绑定（P1 smoke「版本绑定下载」经事务/文件投影达成，属历史验收路径）。
- **vNext（S1 不承诺，DI-08 WP8.1）**：`?branch&version/ts` 预览参数、range diff、schema diff、bisect、retention 读取边界。

## 4. Schema 契约

### CR-4.1 归属与读取路径（第一轮 review 修正）

- schema 行挂在 view 上（`PutViewSchema(viewID, datasetID, …)` upsert，[parity.go:1540-1553](../../services/dataset-versioning-service/internal/repo/parity.go)）。`GET /v1/datasets/{rid}/schema?branch=B` **返回该分支当前视图已存储的 schema**（`GetCurrentSchema`，[parity.go:1554-1570](../../services/dataset-versioning-service/internal/repo/parity.go)），**不是**从 committed 文件重新派生。
- 唯一的按字节派生路径是 Excel 导入数据集的**无显式 branch 兼容分支**：从最新真实 NDJSON 字节派生 TEXT 列 schema（[views_schema_preview.go:381-392](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)；P3.1「三端一致」验收即此路径）；显式 branch 仍走 `GetCurrentSchema`。
- **v1 冻结**：dataset 级 schema 读取入口=`GET /schema`（+ view 级 `GET /views/{view_id}/schema`），均过 `resolveDatasetForCatalog` 租户解析（CR-1.3）。

### CR-4.2 越域 view 防护

view id 全局唯一，跨 dataset/租户的 view id 在 list-files/schema/preview/put-schema 四条路径统一 404（`viewForeignToDataset`，[handlers/tenant.go:90-112](../../services/dataset-versioning-service/internal/handlers/tenant.go)；挂点 [views_schema_preview.go:201-210, 238-246, 263-271, 317-325](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)）。**v1 冻结**：不存在「借他域 view id 读/写 schema」的合法路径。

### CR-4.3 schema 版本关联与可复现性（如实边界，第一轮 review 修正）

- **GAP（v1 如实声明）**：`DatasetSchema` 无版本字段；schema 与 dataset 逻辑版本的关联合同未冻结（矩阵「旧 P4：schema/逻辑版本关联」=WP3.6）。**当前没有任何消费方做 schema 可复现校验**：Pipeline 的 `ViewSchema` adapter 只返回 schema、**丢弃 `content_hash`**（[dataset_http_versioning.go:220-251](../../services/pipeline-build-service/internal/handler/dataset_http_versioning.go)）；resolver 输入只存 dataset RID/branch/schema，无 txn/schema-hash 锁定、无不兼容拒绝。**v1 结论：B2 当前拿不到可复现的 schema 绑定保证**；DVS 与 Pipeline 双侧都不做不兼容检出。
- **未来准入要求（未实现，WP3.6/后续，不作为 B2 已有能力）**：需要可复现 schema 的消费方届时须记录 (dataset RID, branch, content_hash) 并在不匹配时拒绝；该规则在本契约中仅作为 vNext 方向登记。
- **输出侧 schema 的事实**：outputs:commit 的 `body.Schema` 只写入文件条目 metadata 与事务 metadata 并在响应中 echo（[output_commit.go:697-731, 295-300, 466-471](../../services/dataset-versioning-service/internal/handlers/output_commit.go)），**不是不可变的 schema 版本绑定**；v1 不承诺按提交冻结 schema。
- **CSV 解析配置持久化是 DI-08（S2/WP8.2）未来交付，本文不假写已实现**：`CsvOptions` wire 模型已存在（[models.go:1065-1074](../../services/dataset-versioning-service/internal/models/models.go)）但 previewQuery 不读取任何 CSV 参数（[views_schema_preview.go:423-439](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go)），行读 CSV/NDJSON 用固定规则（首行表头、逗号/tab、BOM，[file_rows.go:367-455](../../services/dataset-versioning-service/internal/handlers/file_rows.go)）。v1 期间「预览与执行读同一份配置」仅指同一固定解析规则。

## 5. Build/Job canonical 输入输出与「发布→可见→消费」链

### CR-5.1 canonical 引用（JobSpec）

- canonical 定义=`JobSpec`（[models/resolver.go:25-34](../../services/pipeline-build-service/internal/models/resolver.go)）：`rid`/`pipeline_rid`/`branch_name`/`inputs[]{dataset_rid, fallback_chain, view_filter, require_fresh}`/`output_dataset_rids[]`/`logic_kind+logic_payload`/`content_hash`。发布映射引用实际 canonical JobSpec，定义更新后新 Build 引用新定义、已有 Job 的定义不可替换（P1.5 验收记录）。
- 输入引用四选一解析顺序：`config.dataset_rid` → `dataset_id` → `input_dataset_ids[0]` → 节点 metadata `input_dataset_ids`（[dataset_input_loader.go:213-240](../../services/pipeline-build-service/internal/handler/dataset_input_loader.go)）；引用只接受裸 UUID 或合法 RID（`normaliseDatasetRID` 拒绝其余，[dataset_input_loader.go:172-188](../../services/pipeline-build-service/internal/handler/dataset_input_loader.go)）。

### CR-5.2 B2 输入绑定规则（v1 承诺 = 显式 branch 的 latest 绑定）

- **按裁定①：B2 新调用显式 `branch`（推荐 main）**；不显式 branch 的 latest 读取是兼容现状（CR-3.3）。
- **pinned version 明确拒绝**：节点配置带 `dataset_version` 时，正式执行与预览一致地失败 `dataset_input_version_not_supported`，**绝不回落读 latest**（[lightweight_runtime.go:376-378](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go)；单测断言零调用 [dataset_input_runtime_test.go:138-148](../../services/pipeline-build-service/internal/handler/dataset_input_runtime_test.go)）。
- **不可保证之处（v1 如实列出）**：同一 Build 生命周期内不锁输入快照——上游在两次读取间可发布新版本（SNAPSHOT last-commit-wins）；无 manifest、无逐值可重现承诺（矩阵「旧 P4：固定输入快照」）。**固定输入快照（WP3.4）、产物 manifest（S5/WP3.5）、正式分批读写（S3/WP3.3）、多输出原子可见（S6/WP3.8）、十万行基准（S4/WP3.7）全部是 vNext**，S1 不借冻结自动实施。
- **读取实现边界（不得夸大）**：轻量 runtime 的 DatasetRowSource 是**一次 GET + 一次 JSON decode**，行数上限 1000（`maxDatasetInputRows`，[dataset_input_loader.go:18-20, 70-127, 190-198](../../services/pipeline-build-service/internal/handler/dataset_input_loader.go)）；execution 模式超限/歧义显式失败，preview 模式截断（`AllowTruncation`，同文件 :39-44）。这不是正式分批读取。
- **身份转发**：手动预览/运行优先转发调用者 Bearer（读与输出提交都「以调用者身份」），无调用者上下文时才用静态服务 token（[dataset_input_loader.go:57-63, 159-167](../../services/pipeline-build-service/internal/handler/dataset_input_loader.go)；[dataset_output_committer.go:54-59](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)）。不跟随重定向、拒绝任意 host/path（[dataset_input_loader.go:108-113](../../services/pipeline-build-service/internal/handler/dataset_input_loader.go)）。

### CR-5.3 输出提交（轻量链整文件口径）

- 轻量 runtime 把完整结果行编码为**单个 NDJSON 整文件**经 `content_base64` 提交（[dataset_output_committer.go:169-254](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)；logical_path 强制 `.ndjson`，:205-207）；sample-only 结果退回 metadata-only 注册（:179-190）。默认 branch=`main`（:235）、类型 SNAPSHOT（:236）。**不是分批写，也不承诺跨 Dataset 多输出原子可见**（CR-6.3）。
- 提交身份链与确认语义见 CR-3.1b：预开事务 RID 仅入 provenance；发布确认仅 2xx（GAP F15）。
- 同步链（Connector 快照）：`executeSnapshotSync` → `HTTPDatasetVersioningClient.CommitOutput` POST `/api/v1/datasets/{id}/outputs:commit` 转发调用者 JWT，仅支持 `rest_api` 源（[runtime/sync.go:67-82, 125-174](../../services/connector-management-service/internal/runtime/sync.go)；[handlers.go:2035-2036](../../services/connector-management-service/internal/handlers/handlers.go)）；可用 ACK 的定义=响应携带 `Status=="COMMITTED"` 且 `DatasetID` 与请求一致（[runtime/sync.go:103-113](../../services/connector-management-service/internal/runtime/sync.go)）。

## 6. 发布原子性、幂等、取消与未知结果

### CR-6.1 原子发布的精确边界（第一轮 review 修正）

- **原子性范围 = 单个 Dataset 的单个数据库事务**：datasets 行 `FOR UPDATE`（固定锁序）→ 校验 OPEN 事务 → 推进分支 head → 切割 version → 替换 file index → 替换 lineage → 响应快照读取 → COMMIT（[publish.go:39-135, 195-272](../../services/dataset-versioning-service/internal/repo/publish.go)）。任一步失败整体回滚：**旧版本/旧索引/旧血缘的权威 DB 可见性保持，无半成品可见**（P1 故障注入验收）。
- **三个如实的非无限保证**：
  1. **物理对象先于 DB 事务提交写入**（`writeOutputCommitObjects`，[output_commit_objects.go:81-99](../../services/dataset-versioning-service/internal/handlers/output_commit_objects.go)）：对象写成功后提交链后续失败时，DB 回滚但**已写对象成为无害孤儿**（无引用、不删除）——「失败保旧」指权威可见性，不指无物理残留。
  2. **空 slice 不替换**：`len(files)>0` / `len(links)>0` 才执行替换（[publish.go:227-235](../../services/dataset-versioning-service/internal/repo/publish.go)）——提交不带 files/links **不是**「清空索引/血缘」语义。
  3. **deferred abort 是尽力行为**：legacy 提交失败后 DB 已回滚、事务行留 OPEN，由 deferred abort 定格 ABORTED（[output_commit.go:223-228, 280-293](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）；abort 本身失败则事务行可能停留 OPEN——不承诺补偿必成功，也不覆盖 P2 的发布未知结果恢复规则（P2.6 keyed 路径的失败语义见 CR-6.2，Connector 侧见 CR-6.4）。
- SNAPSHOT 提交把同分支旧 COMMITTED 事务标记 `historical`（[publish.go:94-100](../../services/dataset-versioning-service/internal/repo/publish.go)）。

### CR-6.2 幂等（P2.6，A 级；opt-in）

- 显式 `idempotency_key` 进入 keyed 路径：约束 `create_if_missing=false` + 显式 branch + key 字符集 `[A-Za-z0-9._~:=-]{1,200}`（[output_commit.go:28-40, 318-341](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。指纹=服务端对**原始请求字节**的 SHA256（:56-63）。
- 同 key 同指纹重放：返回**首次提交的原始响应字节**（200），不重放副作用、不按当前 head 重建（[output_commit.go:307-365](../../services/dataset-versioning-service/internal/handlers/output_commit.go)；receipt 模型 [models.go:1153-1167](../../services/dataset-versioning-service/internal/models/models.go)）。同 key 异指纹：409 `IDEMPOTENCY_KEY_CONFLICT` 零副作用。
- **授权区分（第一轮 review 修正）**：`GET /v1/datasets/{rid}/outputs/commits/{key}` 只需租户可见（404 于不可见，无写 scope 要求，[output_commit.go:516-542](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）；**POST 重放走完整 POST 授权（先 403 scope 后 404 租户，CR-3.1b/§2）**——receipt 不含 JWT/subject 只意味着回执字节本身无身份绑定，**不是「任意租户 principal 可重放」**。404 只说明「当前无可见回执」，不证明无在飞操作。
- 无 key 提交保持历史非幂等语义（last-commit-wins），与 P2.6 前逐字节一致（[output_commit.go:136-141](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）。

### CR-6.3 多输出与跨 Dataset 原子性（如实边界）

- P1.5 已验收：Build resolver 为每个输出远端开事务+本地锁，第二输出失败/拿锁失败/持久化失败有补偿（取消本请求已开事务、只释放本 Build 锁）；取消幂等、真实 ABORTED（主计划 P1.5 章节；[dataset_http_versioning.go:252-282](../../services/pipeline-build-service/internal/handler/dataset_http_versioning.go)）。注意：预开事务与最终发布事务的关系见 CR-3.1b。
- **v1 明确不承诺**：跨 Dataset 的「全部输出要么可见要么不可见」——单 Dataset 内 CR-6.1 原子，跨 Dataset 只有失败补偿（S6/WP3.8 为 vNext）。B2 消费方不得把多输出构建当作原子单元。

### CR-6.4 发布未知结果与断点推进（Connector 侧边界，引用 P2 历史，不改实现）

- Dataset 发布是 HTTP 调用，不受 Connector 数据库 CAS fence（[handlers.go:1843-1847](../../services/connector-management-service/internal/handlers/handlers.go) 注释：no exactly-once publish claim）。丢 ACK/未知结果：keyed 协议同 key 原字节重放安全（CR-6.2）；无 key legacy 路径结果未知时保守占位、不自动重发（P2.5/P2.6 验收语义）。
- 断点只在「发布 ACK 成功且本地租约终态 CAS 成功」的同事务推进；发布失败/取消/未知不推进（P2.5 验收；模型 [rest_api_sync.go:51-83](../../services/connector-management-service/internal/models/rest_api_sync.go)）。取消语义：发布门前取消→无远端提交；门后→409 `cancel_too_late` 不伪报（P2.2 验收）。B2 管道侧等价物以 P1.5 为准，不外推。

## 7. 服务生产/消费责任与最小示例

### CR-7.1 责任矩阵

| 服务 | 对 Dataset 契约的责任 |
| --- | --- |
| dataset-versioning-service（DVS） | 唯一权威：身份/RID 生成、租户判定（CR-1.3）、分支/事务/版本状态机、发布原子性（CR-6.1）、幂等回执（CR-6.2）、schema 存储、file index、行读端点。所有授权在 DVS 侧最终裁定，调用方预检只是优化。 |
| pipeline-build-service（Pipeline） | 消费方+发布方：输入经 `/v1/…/files/rows`（调用者身份）；输出经 `/v1/…/outputs:commit`；canonical JobSpec 持有输入/输出 RID 引用（CR-5.1）；不得绕过 DVS 直查其数据库（分库旧共库路径已 503 拒绝，[build_resolution.go:210-243](../../services/pipeline-build-service/internal/handler/build_resolution.go)）。输出确认仅 2xx（GAP F15）。 |
| connector-management-service（Connector） | 发布方：快照/增量同步经 `/api/v1/datasets/{id}/outputs:commit`（转发调用者 JWT，usable-ACK 校验）；自身 Source/Sync/凭据的租户边界独立于 Dataset 契约（[handlers.go:212-245](../../services/connector-management-service/internal/handlers/handlers.go)）。 |

### CR-7.2 最小请求/响应示例（合成值；不含任何真实 token/凭据）

发布（keyed，Pipeline/Connector 通用形态）：

```text
POST /v1/datasets/ri.foundry.main.dataset.11111111-1111-4111-8111-111111111111/outputs:commit
Authorization: Bearer <caller JWT>
{ "idempotency_key": "run-22222222-2222-4222-8222-222222222222",
  "branch": "main", "transaction_type": "SNAPSHOT",
  "files": [{ "logical_path": "part-00000.ndjson", "size_bytes": 81,
              "content_type": "application/x-ndjson", "operation": "ADD",
              "content_base64": "<base64 NDJSON bytes>" }],
  "lineage_links": [{ "direction": "upstream",
                      "target_rid": "ri.foundry.main.dataset.33333333-3333-4333-8333-333333333333" }] }
→ 201 { "dataset_rid": "ri.foundry.main.dataset.1111…", "branch": "main",
        "transaction": { "id": "44444444-4444-4444-8444-444444444444",
                         "branch_name": "main", "tx_type": "SNAPSHOT", "status": "COMMITTED" },
        "files": [...], "preview": {...}, "idempotency_key": "run-2222…",
        "request_fingerprint": "<sha256-hex>" }
重放同 key 同字节 → 200 原始响应字节；同 key 异字节 → 409 {"code":"IDEMPOTENCY_KEY_CONFLICT"}
```

（提交身份取响应 `transaction.id`，wire 字段见 `RuntimeTransaction`，[models.go:837-843](../../services/dataset-versioning-service/internal/models/models.go)。）

消费（B2 输入；按裁定①显式 branch）：

```text
GET /v1/datasets/ri.foundry.main.dataset.33333333-3333-4333-8333-333333333333/files/rows?branch=main&limit=1000&mode=execution
Authorization: Bearer <caller JWT>
→ 200 { "dataset_id": "33333333-…", "path": "part-00000.ndjson", "format": "ndjson",
        "columns": ["sku","qty"], "rows": [{"sku":"0123","qty":"7"}], "truncated": false }
多数据文件且未指定 path → 400 dataset_file_selection_ambiguous；pinned version → 构建失败 dataset_input_version_not_supported
```

错误语义汇总：401=无/坏 JWT；403=scope 不足（顺序按 §2 逐路由）；404=不可见或不存在（跨租户同形）；409=幂等冲突/分支已有 OPEN 事务；413=提交请求体超限（outputs:commit 的 MaxBytesReader）；422=无可读数据文件；503=`OUTCOME_UNKNOWN`（keyed 提交结果不可判）/ `dataset_tenant_boundary_unsupported`（分库旧 schema）/ `spark_client_not_wired`（分布式未接线，明确不支持而非失败）。**「不支持能力」必须显式错误码，不得静默降级**——pinned version 拒绝（CR-5.2）是范式。

### CR-7.3 上限的事实区分（第一轮 review 修正）与兼容规则

- **各面真实上限，不混同**：行读 rows limit ≤1000（硬上限，[file_rows.go:32-33](../../services/dataset-versioning-service/internal/handlers/file_rows.go)）；行读单文件字节硬上限 32MiB（`datasetFileRowsMaxBytes`，[file_rows.go:34](../../services/dataset-versioning-service/internal/handlers/file_rows.go)）；outputs:commit 请求体硬上限由 MaxBytesReader 强制（[output_commit.go:83](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）；Excel 导入总量 32MiB 预算（P3.1 口径）。
- **GAP（F13 附注）**：普通 `POST /upload` 的 `ParseMultipartForm(32<<20)` 是**内存阈值**而非文件体硬上限——其后 `io.ReadAll` 无大小限制（[backing_files.go:101, 116](../../services/dataset-versioning-service/internal/handlers/backing_files.go)）。v1 不把 32MiB 当作 upload 的硬限承诺；硬上限缺失随 upload legacy 定位（裁定③）一并处置。
- 兼容规则（v1 冻结）：本契约 v1 面（RID 形式、Covers 租户规则、outputs:commit 字段、keyed 幂等语义、files/rows 参数与硬上限、pinned version 拒绝）为兼容基线，语义变更须新字段/端点并行过渡，不允许改写既有字段含义（对照 P2.6「无 key 请求逐字节不变」先例，[output_commit.go:136-141](../../services/dataset-versioning-service/internal/handlers/output_commit.go)）；**硬上限**可收紧需发版通告、放宽不视为破坏（upload 内存阈值不在此列）；404 同形（CR-1.3d）与「显式拒绝优于静默降级」为不可破坏项；vNext 能力以新字段/新端点进入，S1 不预设其形状。

## 8. S1 冻结条目检查表与证据映射（第一轮 review 后更新）

| # | 冻结条目 | 规则 | 证据 | 状态 |
| --- | --- | --- | --- | --- |
| F1 | UUID+RID 身份、双形引用、跨租户 404 同形 | CR-1.1/1.3d | A：P1/P1.5 HTTP；B：[repo/tenant.go:75-93](../../services/dataset-versioning-service/internal/repo/tenant.go) + tenant_isolation PG 回归 | 已验收 |
| F2 | 租户只从 JWT OrgID/Sub 推导；admin/grant/请求体不扩大 | CR-1.2/1.3a | A：P1.1 六项勾选；B：[repo/tenant.go:40-54](../../services/dataset-versioning-service/internal/repo/tenant.go) | 已验收 |
| F3 | 无 Org 个人域兼容；组织不认领旧数据；同 Sub 跨组织≠同域 | CR-1.3b/c | A：P1.1；B：Covers 第二臂 | 已验收 |
| F4 | 发布原子性=单 Dataset DB 事务；失败保旧=权威可见性（物理孤儿/空 slice 不替换/abort 尽力等边界如实） | CR-6.1 | A：P1 故障注入 + 26 项 PG16；B：[publish.go:213-272](../../services/dataset-versioning-service/internal/repo/publish.go)、[output_commit_objects.go:81-99](../../services/dataset-versioning-service/internal/handlers/output_commit_objects.go) | 已验收（按修正后口径） |
| F5 | keyed 幂等（原字节重放/409/无 key 兼容；GET 与 POST 授权区分） | CR-6.2 | A：P2.6 8/8；B：[output_commit.go:307-365, 516-542](../../services/dataset-versioning-service/internal/handlers/output_commit.go) | 已验收 |
| F6 | 行读语义（limit≤1000、execution/preview、branch=committed 视图） | CR-3.3 | A：P1 下载/行数核对、P3.1 三端一致；B：[file_rows.go:109-230](../../services/dataset-versioning-service/internal/handlers/file_rows.go) | 已验收 |
| F7 | pinned version 显式拒绝；B2 输入=显式 branch 的 latest 绑定 | CR-5.2 | B：[lightweight_runtime.go:376-378](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go) + 单测断言零回落 | 源码+单测（未进 HTTP 验收链） |
| F8 | 输出整文件 NDJSON、默认 main、lineage upstream | CR-5.3 | A：P1 主链（148 行逐值）；B：[dataset_output_committer.go:169-254](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go) | 已验收（口径=轻量整文件） |
| F9 | resolver 输入/输出/定义租户预检；分库旧路径 503 | §2 | A：P1.5 9/9；B：[build_resolution.go:129-151, 210-243](../../services/pipeline-build-service/internal/handler/build_resolution.go) | 已验收 |
| F10 | 概念区分：`DatasetVersion.id`/`version`/`transaction_id` 三列；提交身份=响应 `transaction.id`；Connector 版本字段实存事务 UUID；预开 txn 仅 provenance | CR-3.1/3.1b | B：[models.go:81-91, 837-843](../../services/dataset-versioning-service/internal/models/models.go)、[handlers.go:2316-2322](../../services/connector-management-service/internal/handlers/handlers.go)、[dataset_output_committer.go:221-227](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go) | 源码级（规则冻结，无独立验收） |
| F11 | schema 无版本绑定；`GET /schema?branch` 走已存 view schema（仅 Excel 无 branch 派生）；ViewSchema adapter 丢弃 content_hash；输出 Schema 仅入 metadata/echo；可复现准入要求=未实现 | CR-4.1/4.3 | C：[parity.go:1554-1570](../../services/dataset-versioning-service/internal/repo/parity.go)、[dataset_http_versioning.go:220-251](../../services/pipeline-build-service/internal/handler/dataset_http_versioning.go)、[output_commit.go:697-731](../../services/dataset-versioning-service/internal/handlers/output_commit.go) | **GAP 如实登记** |
| F12 | permission edge 仅元数据、不执法（裁定②） | CR-1.4 | C：[parity.go:203-246](../../services/dataset-versioning-service/internal/repo/parity.go) 无 enforcement 消费方 | **GAP（裁定②登记）** |
| F13 | upload=legacy 直写、不产生 version/txn、B2 不新增依赖（裁定③）；32MiB 为内存阈值非硬限 | §10-③ | B：[backing_files.go:88-160](../../services/dataset-versioning-service/internal/handlers/backing_files.go)；A：P0 基线 | 已验收但与版本模型并存（裁定③定位） |
| F14 | 分支省缺不一致（master vs main）；裁定①=新 B2 调用显式 branch、省缺为兼容现状非跨路由保证 | CR-3.2 | C：[views_schema_preview.go:106,123,379](../../services/dataset-versioning-service/internal/handlers/views_schema_preview.go) vs [repo.go:293](../../services/dataset-versioning-service/internal/repo/repo.go) | **GAP（裁定①登记，本轮不改默认）** |
| F15 | Pipeline 输出确认仅 2xx，无 Connector 级 usable-ACK/回执校验 | CR-3.1b | C：[dataset_output_committer.go:64-73](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go) vs [runtime/sync.go:103-113](../../services/connector-management-service/internal/runtime/sync.go) | **GAP 如实登记** |
| F16 | `sub` 缺失/零 UUID 不被 DecodeToken 拒绝 | CR-1.2 | C：[jwt.go:133-165](../../libs/auth-middleware/jwt.go) | **GAP（后续要求，本轮不改 auth 库）** |

P1 证据入口：[summary.md](../../tools/smoke/business-platform-p1/results/2026-11-13-build-http/summary.md)（本机工作树、非 Git 跟踪，获取位置即此路径）；P2：[FINAL-BRIEF.md](../../tools/smoke/business-platform-p2-6/results/w9/final-seal-attempt1/FINAL-BRIEF.md)。P3.1 见主计划章节 + commit `664165f3`。

## 9. B2 准入条件（S1 冻结后 B2 可开工的边界）

1. **已满足（2026-11-17）：Codex 工程 review 两轮通过，本文登记为「工程契约 v1 冻结」**（第一轮 7 类事实修正已落实；§10 三项工程兼容裁定已采纳）。
2. B2 实现包（WP3.2–3.10）以本文 CR 条目为接口依据；**S1 冻结≠实现验收**——F7/F10 未进验收链的条目在对应 WP 验收时补证据；F11/F12/F13/F14/F15/F16 的 GAP 不因冻结消失，由对应 WP/后续任务关闭。
3. 不等 DI-01 其余细点；管道调度实现（WP3.2）另以 T8 裁决为就绪条件；WP3.10 由 S1 与 T2 共同 gate。
4. **新 B2 调用显式 branch（裁定①）**。
5. B2 不得依赖的能力清单（本文 vNext）：固定输入版本、产物 manifest、正式分批读写、跨 Dataset 多输出原子可见、schema 版本关联/可复现绑定（含消费方 hash 校验）、CSV 参数持久化、按 permission edge 的访问控制、Pipeline 侧发布确认强校验（F15）。

## 10. Codex 已裁定的工程兼容规则（2026-11-17 第一轮 review 裁定、第二轮复核维持采纳；产品/架构扩展非代签）

| # | 裁定内容 | 对应 GAP |
| --- | --- | --- |
| ① | **新 B2 调用显式 branch**（推荐 `branch=main`）；既有 master/main 省缺差异作为兼容 GAP 保留，本轮不改路由默认；省缺行为是各路由兼容现状，不构成跨路由默认一致保证 | F14（CR-3.2/CR-5.2/示例已按此贯穿） |
| ② | **v1 以当前已验收 tenant/claims 语义为兼容边界**；permission edge 仅元数据、不能依赖其执法，本裁定不由此批准扩大访问；细粒度项目 RBAC 后续另行裁决 | F12（CR-1.4） |
| ③ | **普通 upload 定位 legacy 直写**；B2 新产物一律走 outputs:commit，不新增对 upload 直写的依赖 | F13（含 upload 无硬体上限附注） |

三项均为**工程兼容规则**：不改需求、不代替产品/架构签字；F14 的路由默认对齐、F12 的 RBAC、F13 的 upload 事务化（如做）均属后续工作包，不在 S1 内实施。第一轮 review 修正的 7 类事实（版本/提交身份链、零 sub GAP、逐路由授权顺序、schema 现状、原子性边界、32MiB 区分、显式 branch 贯穿）已全部落入正文并经第二轮复核通过（零 sub 表述一句已按复核意见修正）。

---

### 确认记录与冻结登记

- **编制**：GLM-5.3（S1 文档 worker），2026-11-17。
- **工程复核**：Codex 两轮——第一轮（2026-11-17）提出 7 类事实修正与 §10 三项工程兼容裁定；第二轮（2026-11-17）复核通过，仅余一处零 sub 表述（原「缺 sub 令牌落入个人域」不准确，已改为「缺/零 sub 未被显式拒绝，租户域仍由 OrgID 是否存在决定，仅无 OrgID 时使用个人域谓词」）。
- **冻结登记**：据第二轮复核结论，本文登记为**工程契约 v1 冻结（2026-11-17）**。F15（Pipeline 输出确认仅 2xx）、F16（缺/零 sub 不被拒绝）等 GAP **保持未实施**，由对应后续工作包关闭；本冻结不代产品/架构批准任何范围，不宣告任何 vNext 能力交付。
- **并行安全修复的边界**：Connector Webhook 租户缺口是 `424921bd` 基线事实（§2 已知例外）；当前工作树中安全 worker 的修复（WP2.1）**已于 2026-11-17 通过工程验收（含 Codex 四轮 review；[安全验收报告](data-integration-s1-security-acceptance.md)）**，修复代码未提交、不改变基线口径，本文按「基线缺口事实 / 未提交工作树已修复并验收」分开登记。
- 本文未修改矩阵/主计划/T0 三份文档及任何代码。
