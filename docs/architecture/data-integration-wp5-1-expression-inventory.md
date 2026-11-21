# WP5.1 管道标量表达式清单

工程盘点以 `3070618f` 为基线，产品冻结清单待确认。
现有标量目录有 20 个函数名，14 个进入求值分支，另 6 个能通过类型推导，
实际求值返回 `EvalErrUnknownFunction`。下表按本仓函数名逐项记录。
[工作包排期](data-integration-openfoundry-t0-reestimate.md#di-05-pipeline-builder-expressions)
· [需求矩阵](data-integration-requirements-coverage-matrix.md#di-05-pipeline-builder-expressions-表达式库-★)

## 签名、推导、求值、测试、别名

签名参数中的文本指 `ParamTextual`，数值指 `ParamNumeric`。测试列先列
仓内的直接证据，再区分本次逐项探针。每个函数均通过实际
`ParseExpr → InferExpr → Eval` 探针，其中六项确认了预期的拒绝。
探针也覆盖了 20 个大写函数名。别名列仅记录真实存在的函数或参数映射。

| 函数 | 签名 | 类型推导 | 当前求值 | 仓内测试与本次观察 | 别名 |
| --- | --- | --- | --- | --- | --- |
| `title_case` | 文本 | STRING | 按字母词边界转标题大小写，NULL → NULL | 标题大小写类型；探针求值 | — |
| `lower` | 文本 | STRING | 转小写，NULL → NULL | 大小写与 trim 嵌套推导；探针求值及 NULL | — |
| `upper` | 文本 | STRING | 转大写，NULL → NULL | 同一嵌套推导；探针求值 | — |
| `trim` | 文本 | STRING | 去首尾空白，NULL → NULL | 同一嵌套推导；探针求值 | — |
| `clean_string` | 文本 | STRING | 将空白分隔的片段用单个空格重连，NULL → NULL | 类型推导；探针求值 | — |
| `concat` | 文本、文本 | STRING | 拼接两个字符串；NULL 参数触发类型错误 | 类型与参数个数；探针求值及 NULL 错误 | — |
| `abs` | 数值 | 采用参数类型 | 支持整数和 Double；NULL 返回类型错误，最小 int64 保持负值 | 整数、Double 和拒绝文本的推导；探针复现两项边界 | — |
| `floor` | 数值 | 采用参数类型 | 缺求值分支 | 探针推导成功并确认求值拒绝 | — |
| `ceil` | 数值 | 采用参数类型 | 缺求值分支 | 探针推导成功并确认求值拒绝 | 无 `ceiling` 映射 |
| `round` | 数值 | 采用参数类型 | 缺求值分支 | LONG 推导；探针确认求值拒绝 | — |
| `haversine_meters` | 四个数值坐标 | DOUBLE | 球面距离，以米返回 | 仓内四形式推导；探针求值 | 等价于 `haversine_distance(..., 'meters')` |
| `haversine_km` | 四个数值坐标 | DOUBLE | 球面距离，以公里返回；空坐标返回 NULL | 仓内 NULL 求值；探针求值 | 等价于通用形式的 `km` 单位 |
| `haversine_miles` | 四个数值坐标 | DOUBLE | 球面距离，以英里返回 | 仓内类型推导；探针求值 | 等价于通用形式的 `miles` 单位 |
| `haversine_distance` | 四个数值坐标、文本单位 | DOUBLE | 依单位返回距离，空坐标或空单位返回 NULL | 仓内一度经差的英里值；探针确认单位归一化 | 米 `m` 等，公里 `km` 等，英里 `mi` 等 |
| `to_date` | 文本 | DATE | 缺求值分支 | DATE 和拒绝整数的推导；探针确认求值拒绝 | — |
| `to_timestamp` | 文本 | TIMESTAMP | 缺求值分支 | TIMESTAMP 推导；探针确认求值拒绝 | — |
| `cast` | 任意值、目标类型的 STRING 字面量 | 按类型字面量解析 | 对标量执行；目标支持范围比推导窄，DECIMAL 落到 Double | 目标类型、参数个数；探针观察 DATE、NULL、INTEGER、DECIMAL 边界 | 目标类型名忽略大小写和首尾空白 |
| `is_null` | 任意值 | BOOLEAN | 仅运行时 NULL 返回 true | 类型推导；探针求值 | — |
| `is_not_null` | 任意值 | BOOLEAN | 运行时非 NULL 返回 true，空字符串也返回 true | 类型推导；探针求值 | — |
| `geom_within` | 两个 GEOMETRY | BOOLEAN | 缺求值分支 | 几何类型与拒绝字符串的推导；探针确认求值拒绝 | — |

[目录与类型字面量](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/libs/pipeline-expression/catalog.go)
· [推导器](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/libs/pipeline-expression/infer.go)
· [求值器](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/libs/pipeline-expression/eval.go)
· [类型测试](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/libs/pipeline-expression/type_inference_test.go)
· [距离测试](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/libs/pipeline-expression/haversine_test.go)
· [距离单位](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/libs/geospatial-core/distance.go)

## 下一阶段的验收

- **WP5.2** 补齐 `ceil`、`floor`、`round`、`to_date`、`to_timestamp`、
  `geom_within`。开工时明确负数舍入、半值、Decimal/整数范围、日期格式、
  时区、几何坐标系和边界点的合同。分别覆盖 NULL、非法输入和边界值。
- **WP5.3** 针对已有分支核查语义。优先处理观测到的最小 int64 取绝对值、
  `concat` 与单参字符串函数的 NULL 差异，以及 `cast` 的 DATE 目标、
  INTEGER 范围、DECIMAL 精度和 NULL → 空字符串。用同一固定数据在预览和
  正式构建中逐项对照；本次直接探针只覆盖表达式引擎。
- **WP5.4** 明确实际可执行的目录，并在消费者入口保留未知函数的错误行为。
  构建器的变换目录、变换别名和 `group_by`、`window`、`pivot` 等
  TransformSignature 另在 DI-06 核对。

PBS 的 filter 和管道函数使用标量求值器。变换栈中同名的距离变换有自己的
入口和测试。WP5.1 记录的是工程现状，产品清单冻结和端到端兼容承诺仍待验收。
[PBS 轻量运行入口](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/pipeline-build-service/internal/handler/lightweight_runtime.go)
