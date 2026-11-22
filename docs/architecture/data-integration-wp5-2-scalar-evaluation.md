# WP5.2 标量表达式求值语义

本阶段为六个已存在的签名补齐 Go 标量求值分支，使用同一求值器处理 PBS
预览与正式运行。函数名称按现有规则忽略大小写。参数个数错误、输入类型错误
及非法值返回明确错误，显式 NULL 返回 NULL。

| 函数 | 输入与结果 |
| --- | --- |
| `ceil` | 数值向正无穷取整 |
| `floor` | 数值向负无穷取整 |
| `round` | 数值取最近的整数，半值向远离零的方向取整 |
| `to_date` | 接受 `YYYY-MM-DD` 日历日期，发布为 ISO 日期字符串 |
| `to_timestamp` | 接受带 `Z` 或显式偏移的 RFC 3339 时间，最多九位小数，转换到 UTC 并发布为 RFC 3339 字符串 |
| `geom_within` | 判断首个 GeoJSON 几何是否位于第二个几何内，返回布尔值 |

JSON 中的有符号 int64 整数精确解析后直接通过三个舍入函数。有限 Double
按浮点规则舍入。表达式运行时的浮点数仍使用 binary64，精确 Decimal 运算
属于后续语义核对。对未带上下文类型的数值 NULL，推导类型为 DOUBLE。

DATE 和 TIMESTAMP 在一次表达式内部按时间比较。DATE 在跨类型比较时按 UTC
午夜处理。日期列和时间列通过 NDJSON 保存为文本，从该格式重读后可再次显式
调用对应转换。日期使用公历校验。时间戳缺偏移、格式错误、闰秒和转为 UTC
后超出四位年份范围均返回错误。

几何沿用本仓的 EPSG:4326 GeoJSON 约定，坐标按经度、纬度读取。接受
Point、LineString 和 Polygon，以及列中以字符串承载的上述 GeoJSON。
使用平面 Simple Features 的 Within 关系，候选几何的任何部分都不得落在
容器外部，且两者内部必须相交。仅触及边界的点和落在洞内的点返回 false。
非法几何、其他坐标参考系和未支持的几何类型返回错误。该计算使用展开的
经纬度平面，跨日期变更线的球面面积语义留待独立契约确认。
[Simple Features Within](https://pkg.go.dev/github.com/peterstace/simplefeatures@v0.59.0/geom#Within)

[WP5.1 盘点快照](data-integration-wp5-1-expression-inventory.md)
保留实现前的观察。WP5.3 后续核对原有函数的 null、数值和类型语义。

当前 20 个函数的候选清单与待裁定的兼容性口径见
[WP5.5 冻结评审](data-integration-wp5-5-expression-review.md)。
