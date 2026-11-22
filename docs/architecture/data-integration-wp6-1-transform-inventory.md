# WP6.1 变换目录与产品范围对照

**状态：工程侧盘点完成，产品逐项映射待确认。** 源码基线为 `9733e62938b7d252e0fb88e3c32fe122e11a68d5`。产品《裁剪汇总》「Pipeline Builder Transforms」和《路线图》同名章节一致，目标为既有 **25 项**，另增按条件聚合、窗口聚合和反连接。核对使用产品资料库修订 `5069fbd0`，这两处给出既有项目数量，未逐项列名。下表以仓库实际目录为反查清单，能力名称供产品匹配。待收到 25 项正式名称或稳定 ID 后，逐行加入对应关系和裁决号。[工程排期](data-integration-openfoundry-t0-reestimate.md#di-06-pipeline-builder-transforms)

## 仓库侧逐项反查

静态目录共 24 个唯一 ID，其中 14 项标为 `available`，8 项标为 `planned`，2 项标明外部运行依赖。「执行入口」来自源码核对；目录状态与真实部署及端到端验收分别记录。别名列只列代码中能确认的映射。所有产品匹配均待签署。

| 目录 ID | 产品能力候选 | 目录状态 | 执行入口、别名或与其他行的关系 |
| --- | --- | --- | --- |
| `select` | 选择列 | available | 轻量节点和变换栈；节点别名 `project`、`projection` |
| `drop` | 删除列 | available | 轻量节点和变换栈；节点别名 `drop_columns` |
| `rename` | 重命名列 | available | 轻量节点和变换栈；节点别名 `rename_columns` |
| `cast` | 转换列类型 | available | 轻量变换栈；与 WP5.5 的标量 `cast()` 分别验收 |
| `filter` | 过滤行 | available | 轻量节点和变换栈；节点别名 `row_filter` |
| `formula` | 公式列 | planned | 仅目录；与派生列均可从表达式得到列，产品确认写入、覆盖和计数边界 |
| `derive_column` | 派生列 | planned | 仅目录；目录描述为新增列并保留原列，产品确认与公式列是否一项 |
| `normalize_columns` | 规范化列名 | available | 轻量变换栈的块类型为 `normalize` |
| `normalize_units` | 数值单位换算 | planned | 仅目录；Haversine 的距离输出单位不覆盖通用换算 |
| `sort` | 排序行 | planned | 仅目录；需定义多键、NULL 与相等键顺序 |
| `join` | 连接 | available | 轻量结构化连接；节点别名 `table_join`；通用连接与地理连接各模式分别核对 |
| `union` | 合并行 | available | 轻量结构化合并；按名称补齐缺列为 NULL，`by_position` 分支直接追加记录，异名列的位置重映射待验收；节点别名 `union_all` |
| `haversine_distance` | 球面距离列 | available | 轻量变换栈也接受 `haversine`、`geo_distance`；与地理距离连接输出和配对行为不同 |
| `geo_intersection_join` | 几何相交连接 | available | `geo_join` 的 `intersection` 模式；节点别名 `geometry_intersection_join` |
| `geo_distance_join` | 几何距离阈值连接 | available | `geo_join` 的 `distance` 模式；节点别名 `geometry_distance_join` |
| `geo_nearest_neighbor_join` | 几何最近邻连接 | available | `geo_join` 的 `nearest` 模式；节点也接受 `geo_nearest_join`、`geometry_nearest_neighbor_join`、`knn_geo_join`。API 目录使用本行 ID |
| `aggregate` | 普通分组聚合 | planned | 仅目录；`group_by` 类型签名为关联项；条件聚合与窗口聚合另验收 |
| `explode` | 展开数组 | planned | 仅目录；需明确空数组、NULL、多元素与输出顺序 |
| `json_extract` | 提取 JSON 字段 | planned | 仅目录；需明确路径、缺失值和错误处理 |
| `csv_parse` | 解析 CSV 内容 | planned | 仅目录；与数据连接的 CSV 文件摄取入口分别验收 |
| `gpx_parse` | 解析 GPX | available | 轻量 GPX 节点；别名 `parse_gpx`、`gpx`、`gpx_parser` |
| `python_transform` | Python 变换 | requires_python_sidecar | Python 节点依赖 sidecar；与动态 Python UDF 复用入口的计数由产品确认 |
| `llm_node` | 调用 LLM | requires_ai_service | LLM 节点依赖 AI 服务与实际提供方 |
| `output_mapping` | 输出映射 | available | 构建器输出配置与发布路径；数据集、对象、链接和目录表单中其他选项需按目标分别核对 |
| `function.<name>@<version>` | 可复用函数（动态族） | 表达式可用 / Python 依赖 sidecar | 注入注册表后按函数和版本追加；表达式体经 WP5.4 检查；生产启动当前未接注册表。同一函数的版本和 `function_call`、`udf`、`reusable_function` 路由别名需统一归类 |

[目录与状态](../../services/pipeline-build-service/internal/handler/transform_catalog.go) · [轻量执行与别名](../../services/pipeline-build-service/internal/handler/lightweight_runtime.go) · [地理连接](../../services/pipeline-build-service/internal/handler/geo_join_runtime.go) · [生产接线](../../services/pipeline-build-service/cmd/pipeline-build-service/main.go)

## 新增项与缺口

| 产品新增项 | 当前缺口 | WP6.3 可观察验收 |
| --- | --- | --- |
| Aggregate on condition | 普通 `aggregate` 仍为 planned，轻量执行器缺专用路径 | 固定样本覆盖条件为 true、false、NULL，预览和发布按组逐值一致 |
| Aggregate over window | `window` 有类型签名，轻量执行器缺窗口路径 | 覆盖分区、相等排序键、窗口边界和 NULL；预览和发布一致 |
| Anti join | 通用轻量连接无反连接分支 | 保留左侧未匹配项，覆盖重复键、NULL、全匹配和零匹配 |

结构化轻量路径、Spark SQL 提交入口和真实 Spark 集群运行各有验收要求。源码中的通用提交能力无法单独证明上述三项交付。[变换签名](../../libs/pipeline-expression/catalog.go) · [分布式入口](../../services/pipeline-build-service/internal/handler/distributed_runtime.go)

## 产品确认为验收基准

1. 提供或批准带名称或稳定 ID 的 25 项清单，逐行标记同义、别名、重复、缺失或仅目录，并确认地理连接模式、公式与派生列、输出映射和动态 UDF 的计数方式。
2. WP6.2 默认补齐并语义验收全部 8 个 planned 项。任何裁剪均需产品决定编号。核对保留能力的预览、正式构建和结果发布。
3. WP6.4 记录动态 UDF 的目录、接线与可执行边界。逐项映射产生的额外实现量归入 WP6.5 重估。

WP6.1 的可执行检查核对当前目录响应和现有测试；变换逐项端到端验收仍在后续工作包中。
