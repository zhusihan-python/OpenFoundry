# WP6.1 变换目录与产品范围对照

**产品逐项映射待确认。** 产品资料修订 `5069fbd0` 中，《路线图》与《裁剪汇总》均把范围定为既有 **25 项**，另增按条件聚合、窗口聚合和反连接。两处均未给出既有 25 项的名称或稳定 ID。WP6.5 根据确认后的逐行差异重估。[工程排期](data-integration-openfoundry-t0-reestimate.md#di-06-pipeline-builder-transforms)

## 仓库侧候选清单

以 PR #56 的合并提交 `308e15e3` 核对 PBS 目录。静态返回 26 个唯一 ID，其中 24 个来自原 WP6.1 盘点。当前有 24 项为 `available`，另两项声明 Python sidecar 和 AI 服务依赖。下表记录 24 个既有候选的当前状态。目录状态表示当前声明，实际运行覆盖见各实现提交与验收记录。

| 目录 ID | 产品能力候选 | 目录状态 | 执行入口、别名或与其他行的关系 |
| --- | --- | --- | --- |
| `select` | 选择列 | available | 轻量节点和变换栈；节点别名 `project`、`projection` |
| `drop` | 删除列 | available | 轻量节点和变换栈；节点别名 `drop_columns` |
| `rename` | 重命名列 | available | 轻量节点和变换栈；节点别名 `rename_columns` |
| `cast` | 转换列类型 | available | 轻量变换栈。NDJSON 数据集记录整列 cast 的目标类型，保留 Integer、Long、Double 以及日期和时间戳的区别。列更名或筛选为空后仍保留类型。WP5.5 的标量 `cast()` 单独验收 |
| `filter` | 过滤行 | available | 轻量节点和变换栈；节点别名 `row_filter` |
| `formula` | 公式列 | available | 轻量变换栈写入或覆盖目标列。与派生列共享表达式求值，覆盖语义不同。见 `838a6aa9`。 |
| `derive_column` | 派生列 | available | 轻量变换栈追加列，目标列重名时报错。见 `2e793275`。 |
| `normalize_columns` | 规范化列名 | available | 轻量变换栈的块类型为 `normalize` |
| `normalize_units` | 数值单位换算 | available | 轻量变换栈支持长度、质量、温度的同维度换算。见 `58c3fd8b`。 |
| `sort` | 排序行 | available | 轻量变换栈按多键稳定排序，NULL 置尾。见 `fb768f75`。 |
| `join` | 连接 | available | 轻量结构化连接；节点别名 `table_join`；通用连接与地理连接各模式分别核对 |
| `union` | 合并行 | available | 轻量结构化合并，节点别名 `union_all`。按位置模式按各输入的列顺序映射到首个输入的列名。没有声明或类型 schema 的行数据按列名排序。 |
| `haversine_distance` | 球面距离列 | available | 轻量变换栈也接受 `haversine`、`geo_distance`；与地理距离连接输出和配对行为不同 |
| `geo_intersection_join` | 几何相交连接 | available | `geo_join` 的 `intersection` 模式；节点别名 `geometry_intersection_join` |
| `geo_distance_join` | 几何距离阈值连接 | available | `geo_join` 的 `distance` 模式；节点别名 `geometry_distance_join` |
| `geo_nearest_neighbor_join` | 几何最近邻连接 | available | `geo_join` 的 `nearest` 模式；节点也接受 `geo_nearest_join`、`geometry_nearest_neighbor_join`、`knn_geo_join`。API 目录使用本行 ID |
| `aggregate` | 普通分组聚合 | available | 轻量变换栈支持普通分组聚合。条件聚合与窗口聚合单列验收。见 `2f102880`。 |
| `explode` | 展开数组 | available | 轻量变换栈展开数组，保留元素顺序。见 `805c2181`。 |
| `json_extract` | 提取 JSON 字段 | available | 轻量变换栈按路径提取字段。见 `b5f54874`。 |
| `csv_parse` | 解析 CSV 内容 | available | 轻量变换栈解析 CSV 文本列。数据连接的 CSV 文件摄取有独立配置和验收。见 `b03a187f`。 |
| `gpx_parse` | 解析 GPX | available | 轻量 GPX 节点；别名 `parse_gpx`、`gpx`、`gpx_parser` |
| `python_transform` | Python 变换 | requires_python_sidecar | Python 节点依赖 sidecar；与动态 Python UDF 复用入口的计数由产品确认 |
| `llm_node` | 调用 LLM | requires_ai_service | Go agent-runtime 的鉴权 chat 接口支持接入实际模型。提供方返回的模型标识和 token 用量可在节点执行记录核对。 |
| `output_mapping` | 输出映射 | available | 图运行支持数据集、对象和链接发布。数据集字段投影及固定格式边界见下文。 |
| `function.<name>@<version>` | 可复用函数（动态族） | 表达式可用 / Python 仅声明 | 动态族。注入注册表后追加各函数版本，生产启动当前未接注册表。调用别名和版本共享函数引用语义。 |

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

## 产品确认后重估 WP6.5

按产品名称或稳定 ID 确认 25 项，逐行记录目录 ID 或模式、对应关系、验收范围和决定编号。映射关系可标为同义、别名、重复、缺失或仅目录。下列选择会改变映射或验收工作量。

| 待确认 | 本仓可核对的事实 | 对重估的影响 |
| --- | --- | --- |
| 公式与派生列各计一项，还是同一能力的两种写入方式 | 两个目录 ID，共用表达式求值。公式允许覆盖，派生列拒绝重名。 | 计数可合并，写入边界仍需分别验证。 |
| 连接和地理连接按目录、模式还是结果语义计数 | 通用连接以 `join_type` 区分模式。相交、距离阈值和最近邻是独立目录项，Haversine 计算行内距离。 | 多个产品名称可能映射到同一目录项的不同参数。 |
| Union 是否要求异名列按位置对齐 | 轻量路径按首个输入输出 schema 对齐异名列，缺值写 NULL，列数不一致时报错。 | 产品确认该模式是否属于既有 25 项及对应验收范围。 |
| 是否将输出映射计为变换，以及覆盖哪些目标 | 一个目录项提供多个输出目标。 | 每个保留目标需要对应的发布证据。 |
| UDF 是否作为单独能力计数 | 同一 UDF 可有多个版本。表达式 UDF 依赖注册表，Python UDF 目前仅声明。静态 Python 变换走独立 sidecar 分发。 | 需明确产品计数单位和运行态要求，再估注册表接线或其他执行工作。 |

完成映射后，按实际缺失、别名补齐或语义对齐量列出 WP6.5 工作项。仅凭 25 与 24 的差值无法确定缺口数量。
[Union 执行](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go) ·
[Union Schema 校验](../../services/pipeline-build-service/internal/handler/schema_validation.go)
