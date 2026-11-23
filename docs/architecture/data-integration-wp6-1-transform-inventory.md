# WP6.1 变换目录与产品范围对照

**2026-11-23 产品范围决定。** 本批保留 01–24，包含第 24 项输出映射。24 项均在提交 `2aa0eecd` 完成功能验收。第 25 项可复用函数（UDF）继续待定。产品资料 `5069fbd0` 给出既有 25 项的总数，尚缺 11 月 7 日逐项原名单。条件聚合、窗口聚合和反连接属于另列的三项新增。[工程排期](data-integration-openfoundry-t0-reestimate.md#di-06-pipeline-builder-transforms)

## 目录与逐项功能验收

目录盘点以 #56 合并提交 `308e15e3` 为基线，静态目录含 26 个唯一 ID。下表 01–24 的验收使用同一干净提交 `2aa0eecd219bb8502e02d87eb63e3f54c3d27e71`。证据摘录按编号关联各套件的回读值、断言或下载摘要，并保留原始文件 SHA-256。订单链等 12 套复跑的退出码均为 0。[功能验收摘录](evidence/wp6-5/transforms.json) · [功能合并记录](https://github.com/zhusihan-python/OpenFoundry/pull/59)

| 编号 | 目录 ID | 能力 | 目录状态 | 执行入口与语义 | 已验证的行为 |
| --- | --- | --- | --- | --- | --- |
| 01 | `select` | 选择列 | available | 轻量节点和变换栈；节点别名 `project`、`projection` | 订单发布物只含选定字段。 |
| 02 | `drop` | 删除列 | available | 轻量节点和变换栈；节点别名 `drop_columns` | 清洗后的预览和下载均移除内部备注，未知列被拒绝。 |
| 03 | `rename` | 重命名列 | available | 轻量节点和变换栈；节点别名 `rename_columns` | 订单和退款使用统一业务列名。 |
| 04 | `cast` | 转换列类型 | available | 轻量变换栈。NDJSON 数据集记录整列 cast 的目标类型，保留 Integer、Long、Double 以及日期和时间戳的区别。列更名或筛选为空后仍保留类型。WP5.5 的标量 `cast()` 单独验收 | 七种目标类型、64 位整数精度、整值 Double、空值、非法输入保留旧版。 |
| 05 | `filter` | 过滤行 | available | 轻量节点和变换栈；节点别名 `row_filter` | 已取消或不符合条件的订单按业务谓词过滤。 |
| 06 | `formula` | 公式列 | available | 轻量变换栈写入或覆盖目标列。与派生列共享表达式求值，覆盖语义不同。见 `838a6aa9`。 | 表达式计算并写入目标列。 |
| 07 | `derive_column` | 派生列 | available | 轻量变换栈追加列，目标列重名时报错。见 `2e793275`。 | 用表达式增加新列，验证已有名称的冲突行为。 |
| 08 | `normalize_columns` | 规范化列名 | available | 轻量变换栈的块类型为 `normalize` | 大小写、缩写、符号、中文及名称碰撞经真实预览和下载核对。 |
| 09 | `normalize_units` | 数值单位换算 | available | 轻量变换栈支持长度、质量、温度的同维度换算。见 `58c3fd8b`。 | 同维度的单位换算及校验。 |
| 10 | `sort` | 排序行 | available | 轻量变换栈按多键稳定排序，NULL 置尾。见 `fb768f75`。 | 多键排序、稳定顺序及空值。 |
| 11 | `join` | 连接 | available | 轻量结构化连接；节点别名 `table_join`；通用连接与地理连接各模式分别核对 | 内、左、右、全外和交叉连接，以及类型、空键和大整数。 |
| 12 | `union` | 合并行 | available | 轻量结构化合并，节点别名 `union_all`。按位置模式按各输入的列顺序映射到首个输入的列名。没有声明或类型 schema 的行数据按列名排序。 | 按列名和按位置两种模式均验证发布与短预览。 |
| 13 | `haversine_distance` | 球面距离列 | available | 轻量变换栈也接受 `haversine`、`geo_distance`；与地理距离连接输出和配对行为不同 | 坐标和距离列经米、千米的独立期望对照。 |
| 14 | `geo_intersection_join` | 几何相交连接 | available | `geo_join` 的 `intersection` 模式；节点别名 `geometry_intersection_join` | 按声明的左右角色匹配地物。 |
| 15 | `geo_distance_join` | 几何距离阈值连接 | available | `geo_join` 的 `distance` 模式；节点别名 `geometry_distance_join` | 核对阈值内外的配对及距离。 |
| 16 | `geo_nearest_neighbor_join` | 几何最近邻连接 | available | `geo_join` 的 `nearest` 模式；节点也接受 `geo_nearest_join`、`geometry_nearest_neighbor_join`、`knn_geo_join`。API 目录使用本行 ID | 核对候选、距离、排名和短样本窗口之外的近邻。 |
| 17 | `aggregate` | 普通分组聚合 | available | 轻量变换栈支持普通分组聚合。条件聚合与窗口聚合单列验收。见 `2f102880`。 | 按键分组并与已知聚合值对照。 |
| 18 | `explode` | 展开数组 | available | 轻量变换栈展开数组，保留元素顺序。见 `805c2181`。 | 数组元素展开为对应输出行。 |
| 19 | `json_extract` | 提取 JSON 字段 | available | 轻量变换栈按路径提取字段。见 `b5f54874`。 | 按路径提取列值并检查错误分支。 |
| 20 | `csv_parse` | 解析 CSV 内容 | available | 轻量变换栈解析 CSV 文本列。数据连接的 CSV 文件摄取有独立配置和验收。见 `b03a187f`。 | 从输入列解析 CSV 内容。 |
| 21 | `gpx_parse` | 解析 GPX | available | 轻量 GPX 节点；别名 `parse_gpx`、`gpx`、`gpx_parser` | 轨迹与路线的坐标、边界和错误输入。 |
| 22 | `python_transform` | Python 变换 | requires_python_sidecar | Python 节点依赖 sidecar | 执行已保存管道的 sidecar 节点，核对计算后的发布物及失败保留。 |
| 23 | `llm_node` | 调用 LLM | requires_ai_service | Go agent-runtime 的鉴权 chat 接口支持接入实际模型。提供方返回的模型标识和 token 用量可在节点执行记录核对。 | 真实本地模型写入结果列。构建六行，预览一行，核对零输入、token 用量和模型离线后的旧版。 |
| 24 | `output_mapping` | 输出映射 | available | 图运行支持数据集、对象和链接发布。数据集字段投影及固定格式边界见下文。 | 按声明顺序投影数据集字段，核对实际对象主键和属性以及链接端点。 |
| 25 | `function.<name>@<version>` | 可复用函数（动态族） | 表达式可用 / Python 仅声明 | 动态族。注入注册表后追加各函数版本，生产启动当前未接注册表。调用别名和版本共享函数引用语义。 | 产品待定，纳入条件见下文。 |

[目录与状态](../../services/pipeline-build-service/internal/handler/transform_catalog.go) · [轻量执行与别名](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go) · [地理连接](../../services/pipeline-build-service/internal/handler/geo_join_runtime.go) · [生产接线](../../services/pipeline-build-service/cmd/pipeline-build-service/main.go)

### 轻量通用连接

普通等值连接要求每对连接键都有值。任一侧的键缺失或为 NULL 时，两行无法匹配。两个空字符串可以匹配。连接按所选列和右侧前缀生成输出列名，遇到重名会报错。要保留左右两侧的同名列，可用 `right_prefix` 区分它们。如果只需其中一列，可在投影中选取该列。

含通用连接的轻量预览按构建的输入上限读取数据，再从执行结果中取 `sample_size` 指定的前几行。因此，即使右侧匹配行位于展示行数之外，预览也能显示这次匹配。若连接产生的行数超过预览允许的连接输出上限，即使只请求展示一行，预览也会报错。[预览执行入口](../../services/pipeline-build-service/internal/handler/preview.go)

普通等值连接保留 64 位整数键的精度。字符串与数字按各自的类型匹配；数值键 `1` 和 `1.0` 匹配，两个数值相邻的大整数订单号分别匹配。

数据集输出可在 `_output.columns` 中按顺序指定 NDJSON 字段，例如 `["order_id", "amount"]`。保存预览、草稿预览和发布 schema 沿用该顺序，输入为空时仍保留列和类型。未配置时输出全部结果列。未知或重复的列名、错型配置会使操作失败，原发布版本仍可读取。固定的 `orders.v1` 文件及显式 Parquet 输出沿用各自的 schema，拒绝可变列投影。

对象和链接通过已保存管道的 `POST /api/v1/pipelines/{id}/runs` 发布。MANUAL 和 FORCE 构建支持混合管道中的独立数据集目标。选择了对象或链接目标时，创建构建返回 409。已解析的手动构建按保存的 JobSpec 执行。图运行提交本体时若某一行失败，先前的写入可能已经可见。修复输入后通过 `/runs/{id}/retry` 创建新运行，按对象主键或链接端点更新已有记录。

几何相交、阈值及最近邻连接按管道中声明的左右顺序取表。短预览先读取有界输入，再从匹配结果取前几行，因此样本窗口之外的右侧对象仍可成为最近邻。

## WP6.3 新增能力

| 产品新增项 | 本仓入口 | 已验收的轻量路径 |
| --- | --- | --- |
| Aggregate on condition | 新 ID `conditional_aggregate` | 条件为 true 的行贡献聚合，覆盖 false 与 NULL。见 `fad00a42`。 |
| Aggregate over window | 新 ID `window_aggregate` | 有界 ROWS，覆盖分区、相同排序键、窗口边界、NULL 和短预览。见 `e720818b`。 |
| Anti join | 既有 `join` 下的 `left_anti` 模式 | 保留左侧未匹配行，覆盖重复键、NULL、零匹配、全匹配和短预览。见 `d5467bdb`、`178740b5`。 |

三个新增能力只增加两个静态 ID。动态函数版本按注册表变化，具体可执行条件见
[WP6.4 动态 UDF 边界](data-integration-wp6-4-udf-catalog-boundary.md)。
三项验收覆盖轻量路径。Spark 集群执行仍待对应验证。
[分布式入口](../../services/pipeline-build-service/internal/handler/distributed_runtime.go)

## 本批验收边界

第 22 项通过已保存管道调用真实 Python sidecar。第 23 项通过 agent-runtime 调用本地 TranslateGemma，构建处理六行、短预览处理一行，并验证空输入和模型失联后已发布文件可读。第 24 项验证 NDJSON 数据集列投影，以及已保存图通过 `/runs` 发布对象、链接后回读主键、属性和链接两端。手动构建选本体目标返回 409。媒体集、时序和虚拟表尚未纳入本批输出验收。[功能验收摘录](evidence/wp6-5/transforms.json)

**第 25 项仍待产品决定。** 取得原始名单中的条目，或出现静态节点无法满足、明确需要由管道原生执行已发布函数的用例时，再确认是否纳入、执行语言与固定版本的要求，并估算增量工作量。[UDF 目录与执行边界](data-integration-wp6-4-udf-catalog-boundary.md)

## 用途审计的本地端到端验收

2026-11-23，提交 `ace69f182470403bd7d2ef57a74251afe143eaae` 合并写入器和连接器后通过 Docker Desktop 验收。Helm 渲染的连接器配置通过 Kafka Connect REST 应用。PostgreSQL 已提交 WAL 中的批准、拒绝回执进入 Kafka，两个独立写入器共享 Lakekeeper 目录和 S3 兼容存储。独立读取器核对了 Iceberg 元数据、Parquet 文件和实际行值。[审计运行记录](evidence/wp6-5/audit.json) · [写入器 PR](https://github.com/zhusihan-python/OpenFoundry/pull/65) · [连接器 PR](https://github.com/zhusihan-python/OpenFoundry/pull/66)

| 场景 | 观察结果 |
| --- | --- |
| 两个实例同时写相同批次 | 两个请求均返回 204，表中只有 1 行、1 个快照。同一 ID 携带不同内容时，冲突批次整批返回 409。 |
| 成功应答丢失 | Kafka 位点保持在 1，另一实例收到相同请求后推进到 2。重放前后同为 8 行，快照 ID 与数据文件清单一致。 |
| S3 拒绝写入或表保留配置不匹配 | 目录健康时 S3 拒写返回 403，写入器返回 503。恢复后重放成功，全套场景累计 10 行、8 个快照。缩短保留期限的表拒绝追加，返回 409。 |

`of-apps-ops` chart 的 `services.audit-sink.env.ICEBERG_CATALOG_URL` 默认指向 Lakekeeper。部署独立写入器时，将该值设为 audit-sink 可访问的写入器 HTTP 基址。例：同命名空间的写入器 Service 名为 `audit-table-writer`、端口为 8080 时，覆盖值为 `http://audit-table-writer:8080`。共享 Kubernetes 上的 Strimzi 调谐、Secret 注入和服务身份鉴权仍待环境验收。[chart 配置](../../infra/helm/apps/of-apps-ops/values.yaml) · [写入器启动说明](../../services/audit-sink/table-writer/README.md)
