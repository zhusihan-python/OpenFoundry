# WP12.1 S3 兼容 API 的 T4 决策草案

状态为待产品与安全裁决。以 OpenFoundry 合并基线 `3070618f` 为工程盘点依据。
建议保留阶段四 P2，在首个外部对接方确认客户端、所需操作和传输规模后启动
WP12.2。若对接只有读取需求，另行裁决只读先行，并明确写入交付条件。
[工作包排期](data-integration-openfoundry-t0-reestimate.md#di-12-s3-compatible-api)
· [需求验收](data-integration-requirements-coverage-matrix.md#di-12-s3-compatible-api-for-foundry-datasets)

## 首批客户端范围

提议支持 path-style 访问预先配置的 Dataset bucket，先用标准 AWS CLI
`s3api` 与一个 AWS SDK 客户端逐动作验收。版本、端点、region、寻址模式及
实际请求使用的校验和方式在 WP12.2 启动时固定。若目标客户端依赖高级传输命令，
先采集它实际发出的请求，再决定是否把 HEAD 或 multipart 提前。

| 对照动作 | 拟议阶段 | 可观察行为 |
| --- | --- | --- |
| ListObjectsV2 | WP12.2 | 只返回所选已提交视图的逻辑路径。验证 prefix、delimiter、确定性排序和分页续读 |
| GetObject | WP12.2 | 从同一授权视图读取对象原始字节。明确 Range、条件请求和返回元数据的首批范围 |
| PutObject | WP12.2 | 完整校验并原子提交后返回成功。失败保留之前可见的文件与分支头 |
| ListObjects、HeadBucket、HeadObject | WP12.3 候选 | 由客户端探测和兼容需求决定是否提前。分别验证列表协议和对象元数据 |
| DeleteObject、DeleteObjects、CopyObject | WP12.3 候选 | 定义删除和复制如何进入事务。CopyObject 要单独核查来源和目标的权限 |
| CreateMultipartUpload、UploadPart、ListParts、ListMultipartUploads、CompleteMultipartUpload、AbortMultipartUpload | WP12.3 候选 | 定义上传会话归属、分片限额、完成时的发布边界及中止或超期清理 |

表中覆盖 Foundry 文档列出的 15 项动作。产品需要在 DI-12 关闭前逐项确认实现或裁剪。
Bucket 创建和删除留在 Dataset 生命周期接口。高级 CLI 命令、文件系统挂载和其他
S3 接口由各自的调用轨迹决定是否列入兼容声明。
[Foundry 支持动作](https://www.palantir.com/docs/foundry/data-integration/foundry-s3-api/#supported-actions)
· [AWS 列表协议](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
· [AWS 下载协议](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
· [AWS 上传协议](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)

## Dataset RID、分支和已提交事务

- **推荐方案**。在租户内登记唯一、符合 bucket 命名约束的短别名。每个别名只绑定一个
  `dataset RID + branch`，或一个 `dataset RID + branch + committed transaction RID`。
  绑定明确记录读写模式，历史绑定始终只读。创建别名及查询对象均按调用者当前权限
  校验租户、数据集和分支，权限变化应影响后续请求。
- **兼容方案**。评估直接用 Dataset RID，分支或历史事务沿用 Foundry 的 bucket 后缀。
  需验证客户端对带点、长名称和特殊分支字符的处理，并固定消歧义规则。
  AWS 通用 bucket 名称长度上限为 63 个字符，完整事务后缀明显超过该上限。
  是否需要兼容 Foundry bucket 字符串由产品裁决。
- **对象 key**。映射到 DVS 的逻辑文件路径。分页续读绑定租户、授权视图及过滤条件，
  续页继续读取第一页锁定的已提交版本。失效或无法读取该版本时返回明确错误。
  网关、签名校验和 DVS 必须对目标路径有一致解释，验收覆盖嵌套路径、空格、
  Unicode、编码歧义和保留命名空间。
- **分支和写入**。读 live 别名时定位当前已提交视图。首批提议一个 Put 对应一个
  UPDATE 事务，已有键的替换、并发写入冲突和失败重试统一按批准的规则验收。
  成功响应后，新版本立即可读。响应丢失需要能确定提交结果，或明确记录普通 SDK
  重试可产生重复版本的边界。Foundry 的空闲后自动提交和批量共用事务单独裁决。

[Foundry 资源映射与事务](https://www.palantir.com/docs/foundry/data-integration/foundry-s3-api/#concepts)
· [AWS bucket 命名规则](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)

## 认证方案与现有实现

建议首批使用 HTTPS 上的 SigV4 请求头及 OpenFoundry 签发的短期会话凭据。签发时采用
已验证的用户或服务身份，限制租户、资源及操作范围，且有效期受来源身份有效期约束。
数据面每次核对会话与资源权限。过期、撤销、篡改请求或越租户访问均应拒绝。在集成
方确认自动续签需求后，裁决是否提供 `AssumeRoleWithWebIdentity` 兼容入口，并明确
受信 issuer、audience、role 映射、续签和撤销策略。长期 access key、URL 签名、
流式签名及校验和组合按实际客户端需求另议。安全组确认 TTL、密钥托管和审计字段。
[AWS SigV4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html)
· [临时凭据](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html)
· [AWS STS API](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html)

本次检索了 `services/`、`libs/`、`infra/` 内的 S3 处理、网关路由和 DVS
发布入口，未找到面向 Dataset 的 S3 协议路由。现有 DVS 文件列表、下载与事务接口
可供复用。发布层已有原子快照发布与有键回执，接入前仍须确认 UPDATE 文件集合、
流式上传及 SDK 重试语义。S3 源连接器、内部 presign 和 Ceph RGW 属于相邻能力；
Iceberg 接线可以独立推进。
[DVS 路由](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/dataset-versioning-service/internal/server/server.go)
· [原子发布](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/dataset-versioning-service/internal/repo/publish.go)
· [发布回执](https://github.com/zhusihan-python/OpenFoundry/blob/3070618f1d304c64e91115ed8afd7e5aee4241c2/services/dataset-versioning-service/internal/repo/output_commit_receipts.go)

## T4 裁决与 WP12.2 验收门槛

| 裁决方 | 需要签署的内容 |
| --- | --- |
| 产品及对接方 | 优先级、首批客户端和动作、真实文件大小及并发、Foundry bucket 字符串兼容需求 |
| 安全与认证 | 签发与续签方案、作用域、密钥托管、撤销、跨租户拒绝和审计行为 |
| DVS 与网关 | 资源绑定、版本视图和分页、原子提交与重试、协议错误与资源限额 |

裁决后在隔离真实栈用固定版本的 SDK 和 CLI 执行以下场景，并将请求结果、DVS
版本与文件证据绑定到提交 SHA。

1. 两个分支和一个历史事务包含同键不同内容。使用 ListObjectsV2 和 GetObject
   核对分页、筛选和文件哈希。并发推进分支头后续页仍匹配第一页的已提交版本。
2. Put 成功后核对新版本、旧版本和无关对象。中断请求、校验失败、提交失败后
   核对旧分支头、旧文件、回执和临时对象。模拟丢失成功响应并验证批准的重试行为。
3. 用有效只读会话、过期或撤销会话、错误签名及另一租户的会话重放读写请求。
   核对 S3 响应、DVS 状态和审计记录。历史别名上的写请求必须被拒绝。
4. 记录文件大小、并行数、列表页数、上传/下载耗时与峰值内存，覆盖达限及超限。
   确认首批之外的操作返回稳定协议错误，并核对请求未产生副作用。

本草案记录选项和验收条件。T4 仍待决，协议端点、兼容客户端和容量上限均待实测。
