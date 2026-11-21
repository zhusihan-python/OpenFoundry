# WP4.1 健康检查与数据期望的 T3 映射草案

待产品与可观测性组确认。盘点基线为 OpenFoundry `3070618f`。
PL-26 的“六类”措辞尚无对应的六个名称。建议以官方检查参考的五类为分类，
以推荐页正文的七项为安装场景，并将数据期望保留为独立的构建内契约。
推荐页末尾的五列用于说明案例中的安装位置。
[排期](data-integration-openfoundry-t0-reestimate.md#di-04-maintaining-pipelines)
· [需求矩阵](data-integration-requirements-coverage-matrix.md#di-04-maintaining-pipelines-数据期望与健康检查)
· [官方检查参考](https://www.palantir.com/docs/foundry/health-checks/checks-reference/)
· [官方推荐检查](https://www.palantir.com/docs/foundry/maintaining-pipelines/recommended-health-checks/)

## 五类健康检查与现有枚举

| 官方类别 | 可观察输入与候选范围 | PBS 现有枚举 |
| --- | --- | --- |
| Status 状态 | 指定 schedule、build、job 或 sync 的运行状态，保留各自的作业身份 | 无直接对应 |
| Time 时间 | 构建或同步时长、距最近更新的时间、源数据内容新鲜度 | `FRESHNESS_SLA` 可保留兼容入口，旧配置的时钟须逐项确认 |
| Size 大小 | 已提交视图的行数、文件数或事务大小。区分零行与统计不可用 | `ROW_COUNT_NONZERO` 对应行数大于零这一条规则 |
| Content 内容 | 例如指定列的空值比例、允许值或数值范围，明确分母与空集合行为 | `CUSTOM_SQL` 属于现有扩展枚举，不能推导出具体检查或 SQL 执行权限 |
| Schema 结构 | 所选已提交版本的列、列数或 schema 与配置基线比较 | `SCHEMA_DRIFT` 可映射到列结构比较，比较规则待确认 |

PBS runner 接受这四个枚举，从 `params.expect_passed` 读取结果，缺省为通过，
并用 Dataset RID、构建分支、job RID 等字段发起上报。现有测试用模拟接收端
确认 HTTP 路径和负结果载荷，覆盖范围为上报形态。仓内未定位到该
Dataset 路径与 telemetry 健康检查 CRUD 的接线。后者以检查 ID 接收结果，
分类映射落地时需明确持久化归属、接收路径与身份关联。现有四个枚举及上报
兼容性需保留，真实运行须以数据或运行事实求值。
[PBS runner](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/pipeline-build-service/internal/domain/runners/runners.go)
· [现有测试](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/pipeline-build-service/internal/domain/runners/runners_test.go)
· [Telemetry 路由](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/telemetry-governance-service/internal/server/server.go)

## 七项安装建议与通知归属

| 推荐检查 | 分类与求值信号 | 建议收件范围 |
| --- | --- | --- |
| Schedule status | Status。取指定 schedule 最近一次实际关联 build 的结果 | 调度维护者 |
| Schedule duration | Time。推荐页以 schedule 叙述，参考页以 Build duration 定义。先裁决是否按单次关联 build 计时 | 调度维护者 |
| Schema | Schema。分别裁定上游允许新增列、对外输出严格匹配的比较模式 | Dataset owner 与受影响的下游维护者 |
| Time since last updated | Time。求值时刻与目标分支最近符合条件的已提交更新时间之差，可配置是否忽略空事务 | Dataset owner 与长期停更订阅者 |
| Data freshness | Time。固定事务提交时间与该版本中配置时间列的最大值比较 | 数据所有者 |
| Sync status | Status。取指定同步对象与方向的最近一次运行状态 | 同步维护者 |
| Sync freshness | Time。指定同步完成时间与其可证明关联的数据版本中时间列最大值比较 | 同步维护者与消费方 |

Job status 是推荐页提供的可选替代。等待依赖、缺失版本、空时间列、错误时区、
未发生同步和统计未计算需保留各自的语义，具体状态及阈值由 T3 裁决。
通知渠道适用于订阅这些结果的策略，收件范围仍需按租户和权限核查。

| 通知策略 | 现有能力与待接线 |
| --- | --- |
| 站内通知和邮件 | 官方健康检查支持两者。本仓通知服务有 `in_app` 与 SMTP `email`，拟作为首批候选；实际投递链、偏好和恢复去重待验收 |
| Slack、Teams 和按店外发 | 通知服务有通用 webhook 通道。G-09 按店路由由可观测性组对齐，并确认店铺身份、收件人和出站授权 |
| Issue | 官方支持按检查失败创建 Issue 及恢复时自动关闭；本仓检查事件到 Issue 的映射和去重策略待裁决 |

[官方通知与 Issue](https://www.palantir.com/docs/foundry/health-checks/notifications/)
· [本仓通知服务](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/notification-alerting-service/README.md)

## 数据期望与实现验收

数据期望由管道代码定义，运行时绑定输入和输出。建议在转换前对固定输入快照
运行 pre，在输出暂存完成后、提交前对待发布内容运行 post。对于增量写入，
完整视图规则需按候选提交后的视图求值。WARN 保留结果并继续，ABORT 阻止
受影响输出提交，并核对旧版本和唯一的失败结果。多输出组的任一阻断项需
覆盖原子发布。结果关联 tenant、dataset、branch、build、job 和实际输入或
输出版本，失败时可用尝试标识关联暂存产物，版本 ID 仅在提交后取得。
[官方数据期望](https://www.palantir.com/docs/foundry/maintaining-pipelines/define-data-expectations/)
· [本仓数据集契约](data-integration-s1-dataset-contract.md)

1. 产品与可观测性先确认五类、七项、首批规则及通知渠道，并裁定 PL-26 的统计口径。
2. WP4.2 用同一个真实构建验证各获批检查，覆盖空数据、观测不可用、旧版本和时间阈值。
3. WP4.3 串行验证 pre/post、WARN/ABORT、失败时旧输出可读、结果查询和组发布无半成品。
4. 用既有四个枚举及上报载荷做兼容回归，并验证租户隔离、按权限投递和重复事件去重。

本草案处于 WP4.1 审查阶段。实际求值和投递链仍待 T3 定型后实现。
