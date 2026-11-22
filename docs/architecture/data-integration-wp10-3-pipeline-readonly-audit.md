# WP10.3 Pipeline Builder 四项能力核对

盘点和复测基线为包含 PR #45、#46 的 `eb25ec6c26f1e788e5c26a3430b3c43169c91513`。
WP10.3 核对现有页面、实际服务能力和只读后需要保留的行为。
[工作包排期](data-integration-openfoundry-t0-reestimate.md) · [DI-10 验收矩阵](data-integration-requirements-coverage-matrix.md)

## 页面与产品概念

| 功能细点 | 官方概念 | OpenFoundry 当前入口与已核实范围 | 只读验收 |
| --- | --- | --- | --- |
| Functions Index | 按行级表达式、聚合、生成器与表级变换查找能力。 [官方索引](https://www.palantir.com/docs/foundry/pipeline-builder/functions-index) | 变换栈通过 PBS 目录加载名称、分类、参数和执行状态。真实目录返回 `pipeline_transform_catalog.v1`，`csv_parse` 为 `available`。独立 `/functions` 页面管理函数包。管道路由中未找到独立的内建表达式索引。 | 点选已有变换能查看名称和已配置参数。目录项的运行支持以服务状态为准。表达式和变换目标数仍由 WP5/WP6 对齐。 |
| Input datasets | 从已有数据集或其他受支持来源开始构建。 [官方输入概览](https://www.palantir.com/docs/foundry/pipeline-builder/datasets-overview) | 画布有 Add Foundry data 入口，选中现有数据集生成带 dataset ID 的输入节点。已提交事务固定由管道读取路径处理。合成 FASTER 管道通过固定事务读取实际 DVS 数据。 | 查看输入节点及其上游位置，展示已保存的绑定信息，能刷新节点预览。 |
| Transforms | 表达式处理列或行值，表级变换处理数据表，组合在同一图中预览。 [官方变换概览](https://www.palantir.com/docs/foundry/pipeline-builder/transforms-overview) | 画布接入变换栈、Join 和 Union 编辑器。节点预览面板调用 PBS 的预览接口。真实 CSV 样例产生预期值，非法内容失败后保留先前输出。 | 节点可选，已有参数可查看，样本来自 PBS。预览请求使用已保存的图。编辑控件和草稿状态交由研发路径管理。 |
| Pipeline outputs | 变换结果可发布为数据集或其他受支持产物。 [官方输出概览](https://www.palantir.com/docs/foundry/pipeline-builder/outputs-overview) | 画布有数据集、对象类型与链接类型入口，时间序列和虚拟表的菜单项处于禁用态。实测仅覆盖数据集发布，预览、下载和版本行数一致。构建详情路由显示实际成功运行。 | 点选输出显示目的地与构建结果。从构建详情定位运行；保持研发侧发布和失败恢复回归。其他输出类型沿对应能力验收。 |

[管道路由](../../apps/web/src/router.tsx)
· [函数包页面](../../apps/web/src/routes/functions/FunctionsPage.tsx)
· [画布入口](../../apps/web/src/routes/pipelines/PipelineEditPage.tsx)
· [数据集选择](../../apps/web/src/lib/components/pipeline/AddFoundryDataDialog.tsx)
· [变换栈](../../apps/web/src/lib/components/pipeline/TransformStackEditor.tsx)
· [变换目录](../../services/pipeline-build-service/internal/handler/transform_catalog.go)
· [节点预览](../../apps/web/src/lib/components/pipeline/NodePreviewPanel.tsx)
· [出参发布](../../services/pipeline-build-service/internal/handler/dataset_output_committer.go)

## 已复现的画布阻塞

在 Chrome 中打开由真实 PBS 创建并完成构建的三节点管道，编辑路由进入
错误页，显示 `depends_on is not iterable`。同一响应中有依赖节点的
`depends_on` 是数组，无依赖的根节点省略此字段。PBS 的 JSON 模型使用
`omitempty`，画布拓扑遍历直接迭代字段。现有预览浏览器用例使用自带空数组
的夹具，未覆盖实际返回形态。修复应在前端读取边界覆盖缺省空集合，并保留
有依赖节点的拓扑与既有保存语义。
[PBS 模型](../../services/pipeline-build-service/internal/models/pipeline.go)
· [画布拓扑与节点选择](../../apps/web/src/lib/components/pipeline/PipelineCanvas.tsx)
· [现有浏览器用例](../../apps/web/e2e/pipeline-preview.spec.ts)

同一测试身份可打开构建详情并看到 `succeeded`。浏览器直接调用真实 PBS
预览返回 HTTP 200 与 `code: "007"`，画布内点击预览因上述异常受阻。
PBS、DVS 和 PostgreSQL 均为本地真实服务。浏览器身份与外围页面接口使用
夹具，访问控制未在本轮验证。

## WP10.1 和 WP10.2 的交接条件

1. 用 PBS 实际返回的三节点样本回归编辑与查看路由，包括省略空依赖字段。
   在只读模式点选输入、变换、输出并保留拓扑和预览。
2. 验证普通用户没有 Save、Publish draft、Run now、图编辑入口。抓包核对
   无保存、发布、触发及编辑写请求。预览使用 POST，但只生成样本，测试应按
   请求语义识别。现有 `readOnly` 同时挡住非空节点选择，需拆开此行为。
3. 回归只读下的构建历史、构建详情和固定输入预览。另以研发身份重跑代码
   发布，核对成功版本和失败后的旧版本可读。
4. 由产品和前端负责人确认已有参数的只读呈现位置及研发入口。WP10.3 提供
   核对结果，WP10.1/10.2 的只读行为与权限验收尚未执行。
