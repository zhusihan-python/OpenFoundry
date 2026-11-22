# WP5.5 管道标量表达式冻结评审

**状态：待产品签署。** 候选清单以已合并 WP5.4 的 `47f96addcf722325d80222dd3d01414cd502337a` 为工程基线。建议阶段一采用下表 20 个标量函数，并逐项批准兼容性口径。使用范围为 PBS 标量表达式中的过滤与可复用函数体。产品范围维持现有配置台入口，变换目录另按 DI-06 管理。[排期与职责](data-integration-openfoundry-t0-reestimate.md#di-05-pipeline-builder-expressions)

## 候选清单

函数名忽略大小写。距离简式参数顺序为起点纬度、起点经度、终点纬度、终点经度。「保持数值类型」指类型推导沿用参数类型，运行时按 int64 或 Double 求值。证据列的 WP5.2、WP5.3、WP5.4 对应下方各次验收的实际基线。

| 函数 | 签名 | 类型推导 | 实际求值 | 回归证据 | 别名或映射 |
| --- | --- | --- | --- | --- | --- |
| `title_case` | 文本 | STRING | 按字母词边界转标题大小写，NULL 传播 | WP5.3 Unicode 和 NULL | — |
| `lower` | 文本 | STRING | 转小写，NULL 传播 | WP5.3 Unicode 和错误类型 | — |
| `upper` | 文本 | STRING | 转大写，NULL 传播 | WP5.3 Unicode 和错误类型 | — |
| `trim` | 文本 | STRING | 去首尾空白，NULL 传播 | WP5.3 Unicode 空白和错误类型 | — |
| `clean_string` | 文本 | STRING | 用单个空格连接原文本的非空白片段，NULL 传播 | WP5.3 Unicode 空白和错误类型 | — |
| `concat` | 两个文本 | STRING | 拼接两个字符串，NULL 参数报类型错误 | WP5.3 空文本及 NULL | — |
| `abs` | 数值 | 保持数值类型 | 取绝对值；NULL 报类型错误；最小 int64 报 `abs integer overflow` | WP5.4 真实发布、相邻边界与溢出回归 | — |
| `floor` | 数值 | 保持数值类型 | 向负无穷取整，NULL 传播 | WP5.2 正负值及 int64 边界 | — |
| `ceil` | 数值 | 保持数值类型 | 向正无穷取整，NULL 传播 | WP5.2 正负值及 int64 边界 | — |
| `round` | 数值 | 保持数值类型 | 取最近整数，正负半值向远离零的方向取整，NULL 传播 | WP5.2 半值与精确整数；WP5.4 目录控制 | — |
| `haversine_meters` | 四个数值坐标 | DOUBLE | 球面距离，返回米；空坐标返回 NULL | WP5.3 正常距离与非法纬度 | 通用形式的 `meters` |
| `haversine_km` | 四个数值坐标 | DOUBLE | 球面距离，返回公里；空坐标返回 NULL | WP5.3 正常距离与非法经度 | 通用形式的 `km` |
| `haversine_miles` | 四个数值坐标 | DOUBLE | 球面距离，返回英里；空坐标返回 NULL | WP5.3 正常距离与非法经度 | 通用形式的 `miles` |
| `haversine_distance` | 四个数值坐标、文本单位 | DOUBLE | 按单位返回球面距离，空坐标或 NULL 单位返回 NULL | WP5.3 单位等价与未知单位错误 | `m`、`km`、`mi` 等，详见单位表 |
| `to_date` | 文本 | DATE | 严格公历 `YYYY-MM-DD`，发布为 ISO 日期文本，NULL 传播 | WP5.2 闰日与非法格式 | — |
| `to_timestamp` | 文本 | TIMESTAMP | 带显式偏移的 RFC 3339 时间转 UTC，发布为文本，NULL 传播 | WP5.2 时区偏移与无时区拒绝 | — |
| `cast` | 值、目标类型的 STRING 字面量 | 按字面量确定目标类型 | 非空标量转换支持 STRING、INTEGER、LONG、DOUBLE、DECIMAL 和 BOOLEAN，支持的源类型依目标而异 | WP5.3 正常转换、DATE 拒绝和下节兼容性用例 | 目标类型名忽略大小写及首尾空白 |
| `is_null` | 值 | BOOLEAN | 运行时值为 NULL 时返回 true；对象和数组目前会降为 NULL | WP5.3 NULL、空字符串和对象；数组行为见解码器 | — |
| `is_not_null` | 值 | BOOLEAN | 与运行时 NULL 判断互补，空字符串返回 true | WP5.3 NULL、空字符串与错误参数个数 | — |
| `geom_within` | 两个 GEOMETRY | BOOLEAN | EPSG:4326 平面 Within；边界点与洞内点返回 false，显式 NULL 传播 | WP5.2 边界、洞与非法几何 | — |

[原始五列盘点](data-integration-wp5-1-expression-inventory.md) · [日期、舍入与几何合同](data-integration-wp5-2-scalar-evaluation.md) · [距离单位表](../../libs/geospatial-core/distance.go) · [签名](../../libs/pipeline-expression/catalog.go) · [求值器](../../libs/pipeline-expression/eval.go)

## 产品需逐项确认

| 决策项 | 已观察到的行为 | 建议冻结口径与影响 |
| --- | --- | --- |
| INTEGER 范围 | `cast('2147483648','INTEGER')` 成功，现有整数路径使用 int64 | 阶段一保留旧行为；若改为有符号 32 位，先盘点超界值，明确拒绝、转换与迁移规则 |
| DECIMAL 精度 | `9007199254740993` 和 `9007199254740992` 经 DECIMAL 转换后相等 | 把当前语义标为 binary64；精确 Decimal 单独确定精度、标度、舍入、序列化与迁移方式 |
| STRING 的 NULL 转换 | `cast(NULL,'STRING')` 返回空字符串 | 阶段一保留；若改为传播 NULL，要回归空文本过滤、拼接及输出内容 |
| 拼接与绝对值的 NULL | `concat(NULL,'x')` 和 `abs(NULL)` 返回类型错误，单参文本函数传播 NULL | 阶段一分别保留；统一传播会让部分旧失败构建成功，须单独决定 |
| 结构化值的 NULL 判断 | 对象在真实用例中被 `is_null` 判为 true；数组走同一 JSON 解码分支 | 保留标量运行时行为并记录限制；支持对象或数组时，成对回归两个 NULL 判断函数及旧过滤结果 |
| 日期转换入口 | `cast('2024-02-29','DATE')` 推导成功，运行时拒绝 | 明确日期和时间戳使用 `to_date` / `to_timestamp`；若扩展 `cast`，单独对齐推导、求值、时区规则和既有失败行为 |

本轮评审还需确认冻结生效版本、额外函数的优先级，以及偏离上述建议的处理方式。产品签署后将逐行结论落盘，有行为变化的条目需补真实预览与发布回归。新增函数实施量归 WP5.6 重新估算。

## 验收依据

- WP5.2 在 `2c7837f5` 的真实 PBS/DVS 固定输入上核对六个新增求值分支。发布内容与预览一致，当时图运行发布的版本行数错误另立修复。
- WP5.3 在本地验证合并 `f89b8f34` 上跑完 14 个既有函数的正常与拒绝用例。最小 int64 的旧溢出行为随后在 #34 修复。
- WP5.4 在 `bf730569` 验证 20 个函数的代表性求值，并用移除 `round` 求值器的负对照证明目录会隐藏仍有签名的不可执行条目。真实栈验证未知函数与 `abs` 溢出报错、失败后保留旧版本，以及正常发布的文件与版本行数均为一行。目录用例注入了可复用函数注册表，当前生产启动尚未接入该注册表。[WP5.4 审查 PR](https://github.com/zhusihan-python/OpenFoundry/pull/35)

WP5.5 为文档评审增量。以上证据引用各自实际执行的代码版本；产品签署仍待完成。
