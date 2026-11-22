# WP10.2 只读画布与研发发布验收

验收基线为 #49 合并后的 `62b80d6e7acbd78061b25b80c662a6d66db46963`。
在隔离的 PBS、DVS 和 PostgreSQL 上使用两行合成 NDJSON。先固定源数据集
版本 1，再将源分支头推进到版本 2，写入可以识别的新值 `999`。
[WP10.3 核对表](data-integration-wp10-3-pipeline-readonly-audit.md)

## 研发代码发布

以测试作者凭证在同一条管道上执行以下步骤。JobSpec 对应的构建和画布的 graph
run 在服务端分别记录。本轮两条路径均执行，只有下表前三个接口用于证明
canonical 手工构建。

| 步骤 | 实际入口与断言 |
| --- | --- |
| 发布代码定义 | `POST /v1/job-specs/TRANSFORM` 发布包含已提交输入版本、事务 RID 和输出数据集的 JobSpec。 |
| 解析并创建构建 | `POST /v1/builds` 创建 MANUAL 构建。PBS 作业记录绑定已发布的 JobSpec RID。 |
| 执行与读取成果 | `POST /api/v1/execute` 返回 HTTP 200 和 `BUILD_COMPLETED`。DVS 仅新增一个已提交版本。作业输出事务、版本元数据与实际签名下载一致，文件只有固定输入的两行 `007` 和 `012`。 |
| 失败用例 | 同一管道重新发布 JobSpec，保留版本 1 的编号并指定版本 2 的真实事务。新构建接纳后以 `BUILD_FAILED` 结束，包含 `batched_scan_binding_invalid`。 |
| 失败后读取 | 上次成功发布的输出版本 3 保持为分支头。版本列表、索引、实际存储文件和签名下载 SHA-256 与失败前相同。PBS 未保留未结算的输出预留或输入锁。 |

[canonical 客户端和栈](../../tools/smoke/data-integration-wp3-3/run3c.sh)
· [固定输入与发布回归](../../tools/smoke/data-integration-wp3-4/harness34.py)
· [DVS 文件验证辅助](../../tools/smoke/data-integration-wp3-13/acceptance.py)

## 只读浏览器回归

使用无管道写入权限的测试查看者。保存的画布渲染三个节点与两条连线；点击输入、
选择列和输出节点可读取真实预览，三处均显示 `007` 和 `012`。版本历史及
graph run 详情可打开。独立的构建详情页分别显示 canonical 的
`BUILD_COMPLETED` 和 `BUILD_FAILED`。在失败后，浏览器按版本 3 请求
DVS 并读回两行。浏览器为这些动作发送的 24 个 API 请求均为 GET。

[画布路由](../../apps/web/src/routes/pipelines/PipelineEditPage.tsx)
· [构建详情](../../apps/web/src/routes/builds/BuildDetailPage.tsx)
· [版本预览客户端](../../apps/web/src/lib/api/datasets.ts)

## 证据和适用范围

验收使用 Go 1.26.3 构建 PBS 和 DVS，运行于 Docker 29.6.1，
PostgreSQL 为 16.16。前端使用与合并基线源代码一致的构建产物。测试代理
提供合成浏览器身份和外围页面响应，将画布、构建详情与数据预览请求转发给真实
服务。数据集预览路径采用现有网关的前缀重写。生产登录、角色配发及网关部署
仍需对应的环境验收。

本地验收包包含栈启动脚本、合成输入、API 和浏览器断言、实际请求日志、
源代码与运行时二进制摘要以及下载摘要。复跑时使用空白的隔离栈，以作者凭证
完成发布和失败用例，再以查看者凭证刷新固定版本。按精确行值、事务、版本数
和请求方法判断，构建记录中的状态只覆盖对应路径。
