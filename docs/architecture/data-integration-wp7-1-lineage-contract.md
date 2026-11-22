# WP7.1 数据血缘资源与事件合同草案

供数据与接入、本体构建、应用端和全局分支维护者评审。源码盘点基线为
`19623cb98dff8d7e2d7b173d7b4a4df119ceb1ea`。字段和事件语义是拟议的内部合同。
[排期](data-integration-openfoundry-t0-reestimate.md#di-07-data-lineage)
· [验收范围](data-integration-requirements-coverage-matrix.md#di-07-data-lineage-数据血缘)

## 资源和事实来源

图以稳定资源为节点，以版本证据解释每一条边。基础路径是输入数据集 → 管道 →
输出数据集 → 本体对象类型 → 已发布应用。同一类型的数百万个实例共享一个对象类型
节点，实例搜索和实例级溯源留待另行定义。

| 节点 | 资源标识 | 生产者与生效事实 | 已有入口和缺口 |
| --- | --- | --- | --- |
| 数据集 | Dataset RID；版本证据为分支、已提交事务 RID | DVS 提交事务，组发布以整个组成功提交为准 | 普通发布写文件索引与数据集血缘；组发布另有成员回执。目标数据集的发布不能证明输入取自哪个事务，需随发布补齐已解析的输入版本。 |
| 管道 | Pipeline RID；执行证据为 build ID、管道版本或图快照标识 | PBS 提供实际读取的输入及生成的输出；输出边以 DVS 已提交回执为证 | 输出提交发送上游 Dataset RID 与 Pipeline RID。拟议投影使用这些身份，把数据集间的关系拆成两段有方向的边。 |
| 本体对象类型 | Object Type ID；证据为 binding ID、源数据集事务和稳定的物化运行 ID | 本体数据绑定的实际物化结果 | 绑定模型保存数据集、分支与可选版本。现有物化汇总更新最后运行状态，稳定运行身份和实际解析的源事务需随物化事实持久化。对象或链接索引事件用于搜索投影，单凭索引接收无法证明来源事务。 |
| 应用 | App ID；版本证据为已发布 App Version ID 与递增版本号 | 应用编排事务发布或提升版本，解析已生效快照中显式引用的对象类型 | 发布和提升更新 `published_version_id`。Workshop 变量已有 `object_type_id`。首批覆盖快照可解析的显式引用，动态查询、派生变量和隐式引用单独评估。 |

[DVS 发布事务](../../services/dataset-versioning-service/internal/repo/publish.go)
· [DVS 组发布](../../services/dataset-versioning-service/internal/repo/output_commit_group.go)
· [PBS 发布请求构造](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)
· [本体绑定模型](../../libs/ontology-kernel/models/object_type_binding.go)
· [物化入口](../../libs/ontology-kernel/handlers/bindings/bindings.go)
· [搜索索引入口](../../services/ontology-indexer/internal/runtime/runtime.go)
· [应用版本发布与提升](../../services/application-composition-service/internal/repo/repo.go)
· [Workshop 变量](../../services/application-composition-service/internal/models/workshop_schema.go)

## 逻辑字段与投影规则

以下字段用于合同评审，传输和序列化由各生产者与血缘服务共同确定。

| 记录 | 必填信息及语义 |
| --- | --- |
| 资源引用 | `kind`、`resource_id`、经资源服务验证的租户作用域与项目归属。组织租户用组织 ID，个人域用所属主体 ID。显示名可选，改名不改变资源身份。 |
| 关系事实 | `event_id`、`schema_version`、权威生产者、`committed_at`、两端资源引用、关系类型、操作以及下表的版本证据。时间只用于展示和审计。去重使用生产者身份与稳定事件 ID。 |
| 更新顺序 | 声明哪一个持久化对象拥有这组边，并携带该对象范围内可比较的提交顺序或等价的前任标识。只有更新的生效版本能替换当前边。不同生产者的时钟不参与排序。 |
| 基本读模型 | 当前图按资源和选定分支查找已生效关系。历史图按当时保存的版本证据查询。输入事务未知时不从当前分支头推断历史；未绑定的旧记录只保留可核实的资源级关系。 |

| 方向 | 拟议关系 | 版本证据与边拥有者 |
| --- | --- | --- |
| 输入数据集 → 管道 | `consumes` | 实际输入事务、输入分支、build ID 与管道版本。按输出提交证据确认，避免将失败运行计入当前影响图。 |
| 管道 → 输出数据集 | `produces` | 同一 build ID、同一管道版本，输出分支、已提交事务及存在时的组回执关联。以输出数据集及分支为替换范围；重算该范围时只关闭它拥有的边。 |
| 输出数据集 → 对象类型 | `materializes` | 实际读取的事务与分支、binding ID 与物化运行 ID。由绑定拥有，两个绑定可独立贡献同一对资源的边。干跑和完全失败不替换已生效事实。部分写入需生产者提供明确的持久证据，供人工确认其投影规则。 |
| 对象类型 → 应用 | `used_by` | App Version ID 与版本号。由应用拥有；发布的新快照移除某绑定时，关闭该应用拥有的当前边。历史可回看旧快照。 |

管道的两段关系必须共享发布所证实的同一次执行。构建有多个输出时，应记录各
结果实际依赖的输入；现有数据集级链接只能支持当前已知粒度。历史与影响查询
不得跨 build 拼接出一条从未成立的路径。

同一已提交事实重送只产生一次效果。相同事件身份承载不同内容时拒绝覆盖并留下
可排查记录。删除、替换与没有依赖的成功发布均需产生可重放的关闭事实。组发布在
投影前先确认整组已提交，可按组重试。写入持久的源事实、记录消费进度及恢复点的
方式留给 WP7.4，由评审决定接入已有事务记录或新增可靠交付。基础图的验收可用
PostgreSQL 完成。现有 OpenLineage run payload 保持兼容，Kafka/Iceberg 分析链
仍按独立计划推进。
[OpenLineage helper](../../libs/outbox/lineage_event.go)
· [血缘服务启动与持久化接线](../../services/lineage-service/cmd/lineage-service/main.go)
· [现有节点类型](../../services/lineage-service/internal/models/lineage.go)

## 可见性、验收与评审责任

拟议读端先验证请求者对根节点的访问权限，再沿双方资源均可见的边遍历。
租户、项目与数据标记都参与可见性；不可见节点不能通过边、路径、计数或影响面
结果泄露。组织内跨项目关系仍需各项目权限，跨租户写入拒绝。数据集已有组织
和个人域区分，血缘现有标记过滤仅覆盖其中一部分，项目归属解析与跨租户真栈
验证由 WP7.5 明确。
[DVS 租户规则](../../services/dataset-versioning-service/internal/repo/tenant.go)
· [血缘标记过滤](../../services/lineage-service/internal/lineage/filter.go)

| 工作包 | 可观察验收 |
| --- | --- |
| WP7.2–7.3 | 固定输入 `raw@t1`，管道 `sales` 的构建 `b1` 发布 `clean@t2`。绑定 `orders` 从 `t2` 成功物化，应用 `dashboard@v1` 发布并引用该对象类型。从 `raw` 正向遍历出现完整路径，影响列表返回 `clean`、`Orders`、`dashboard` 及所经管道，边携带正确版本。查询固定分支时保留该分支的结果。 |
| WP7.3 | 失败构建、失败组发布、干跑及应用草稿保存后，当前图仍返回原路径。单独引用另一个版本的关系无法从旧输入拼出跨 build 影响路径。 |
| WP7.4 | 重送 `b1/t2` 不增边。较旧事实晚到时不回退当前图。应用发布 `v2` 去掉引用后，关闭当前应用边。停启服务并重放，当前图与历史图仍返回相同结果。 |
| WP7.5 | 另一租户用户访问根节点被拒绝。组织内仅有根项目权限的用户无法从路径、计数或影响列表看到另一项目的受限节点。 |
| WP7.6 | 在分支上发布新输入和输出后，主分支关系保持原样。按发布历史选 `dashboard@v1` 可回看原对象引用，当前 `v2` 没有这条边。 |
| WP7.7 | 图内预览、构建时间线与过期数据集提示按产品批次各自验收。 |

数据与接入维护者确认输入绑定、提交组边界和投影规则；本体构建维护者确认物化
运行身份、事务解析及部分成功口径；应用端维护者确认显式引用的覆盖面、发布与
提升；全局分支、权限和架构维护者确认版本键、项目归属与交付方式。WP7.1 完成
条件是跨组确认并记录决议。以上场景为后续实现验收，当前尚未执行真实全链。
