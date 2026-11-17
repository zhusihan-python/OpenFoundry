# WP3.2 隔离验收记录（持续更新）

**总体状态：PASS——WP3.2 工程实现与真实隔离验收完成，非生产部署。** 日期 2026-11-17；全文周期时间均为 **UTC**。按 WP3.1 assessment §6 的 V1–V8 分层登记，不将局部通过提升为整项通过。本轮为代码与隔离环境验收，不是生产部署；默认 Off，不改变其它 WP 状态或 T0 冻结估算。

## 版本与证据范围

生产服务 v3 二进制 SHA256：`c384b635c151a9cc130b85a41d39f18cd65a6e8af219c5c74f518fc4aed3d4a9`；核验文件 `R/e2e-v3/pbs-v3-image-binary.json` 与 `R/e2e-v3/running-v3-provenance.json`（运行 Pod image→CRI config→save layer binary）。`production-sha-verified-unchanged.txt` 记录168项相关生产源码/迁移/config 与 pre 一致；`dvs-source-binary-provenance.json` 记录 DVS HEAD/binary SHA 与 source_changed=false。R 为 `/private/tmp/ofwp32-runtime-sol-evidence`。09:01 首周期属于 v2，只保留为诊断，不计入正式 v3 周期。testproxy 后续 gzip/hold_replay 调整仅为测试设施变化，生产 v3 冻结。

Producer 正式基链为 r3 `20261117T084515_1177`（本轮 results）；不扫描或复制历史旧 results。D API 原始证据位于 `/private/tmp/ofwp32-d-evidence`。

## 已接受的分层检查

| 层 | 实际检查 | 证据及边界 |
|---|---|---|
| Producer/CDC/Kafka | r3 总 rc0、双 tick exit0、pre/post 一致、两个自动 Pod 与 Kafka2、10 个 negative | root 已接受 r3；不单独代表 canonical 产品成功 |
| Apicurio | first/repeat 真实 HTTP 注册，枚举修复后通过 | R/registrar-first.rc、registrar-repeat.rc；C/root 已接受 |
| API | 13 个真实 JWT HTTP/PG 用例；CRUD unit、whole PBS build | D/10-api-sol-test.txt、12-unit-build.txt 均 exit0；含 owner/admin/personal、代次、过期、并发首次/历史创建及两锁等待跨过期 |
| Core | v3 三包、PG、race、whole build、unit HTTP recovery | root 已接受；命令细节见 core checkpoint，不替代完整 V 场景 |

## V1–V8 实证矩阵

| 项目 | 实际观察与断言 | 证据 | 状态 |
|---|---|---|---|
| V1 真实周期 | v3 09:02/09:03/09:04 三个周期分别 event→Build→receipt→真实 DVS transaction COMMITTED→精确 transaction 对应 version→views/at→签名 FS HTTP 下载历史文件。每份 40B、2 行 A/2 与 B/5；文件 SHA 与 receipt.metadata.sha256、原请求 content_base64 一致 | R/counted-v3-cycles-r2.rc=0、counted-v3-cycles-summary.json、counted-v3-cycle-{0,1,2}.json、同前缀 -view.json/-files.json/-file-0.ndjson、counted-v3-cycle-http.jsonl | **PASS** |
| V2 重启恢复 | 真实 rollout，前后 Pod 身份变化；09:04/09:05 成功，其中 09:04 已有上述逐产品强核验。tick-coalesce-r3 额外证明三小时旧 due 合并一次，audit 精确旧 due、只有一个产品、next due 未来；重复事件场景另证不重复 | R/v3-before-restart-pod.json、v3-after-restart-pod.json、v3-after-restart-db.json、v3-restart.log；counted-v3-cycle-2.json；tick-coalesce-r3.rc=0、tick-coalesce-r3-result.json | **PASS** |
| V3 并发/重复 | 两 producer 并发投递同 slot，dispatch/build/receipt 与真实 DVS version 前后完全相等，consumer lag0；独立 Forbid-r3 在首 run hold/live lease/activity 时投不同 slot，第二机会 blocked/overlap 且无 Build，释放首 run 成功，仅 DVS +1 | R/duplicate-complete.rc=0、duplicate-complete-pre.json、duplicate-complete-post.json、duplicate-complete.jsonl、duplicate-complete-producer-{0,1}.json；forbid-r3.rc=0、forbid-r3.log、runtime-hold-r3-forbid-org-*；tick-coalesce-r3-first.log/second.log | **PASS** |
| V4 身份/租户 | org/personal 旧 ref、当前 ref 旧 generation 均 skipped，无 Build/新 version；相同 UUID 跨 kind HTTP404；org/personal 执行中 hold/live lease 后撤权，释放上游返回真实 rows200，发布授权 gate 拒绝，versions 不变 | R/revocation-r3.rc=0、org-revocation-summary.json、personal-revocation-summary.json、org-revocation.jsonl、personal-revocation.jsonl；running-revocation-r3.rc=0、running-revocation-r3.log、runtime-hold-r3-revoke-{org,personal}-* | **PASS** |
| V5 失败保旧 | 真实业务 failed，旧 version 与 rows 不变，无半成品发布；pre/post 产品快照完全相等 | R/single-failure-r2.rc=0、single-failure-r2.log、failure-pre-product.json、failure-post-product.json、runtime-single-failure-r2-event.ndjson | **PASS**（单输出执行失败场景；不扩展为任意多输出原子性） |
| V6 提交/崩溃窗口 | 三个真实窗口均通过：准入提交前屏障为 dispatch/build 0/0；提交后执行前为 1/1/0，强杀 Pod 后唯一 Build/receipt 恢复、DVS +1，真实行与 input 一致；外部 COMMITTED 后 ACK drop+hold，PBS0/删 Pod，pending 原 bytes 且远端 +1，release/PBS1 同 key/SHA/原 txn 恢复，版本不再增。cleanup triggers/holders 均0 | R/admission-crash-exitcodes.json（prepare/before/after/product_comparison 均0）、admission-crash-{before,after}-result.json、同前缀 -barrier-snapshot.json/-barrier-wait.json/-kill.json/-replacement.json/-final-snapshot.json/-final-dispatch.json/-versions-before.json/-versions-after.json/-rows-after.json/-product.jsonl；admission-crash-input-comparison.jsonl；R/lost-ack-r3.rc=0、lost-ack-r3-before-restart-dispatch.json、lost-ack-r3-pending-receipts.json、lost-ack-r3-remote-committed-versions.json、lost-ack-r3-final-dispatch.json、lost-ack-r3-final-receipts.json、lost-ack-r3-before-versions.json、lost-ack-r3-after-versions.json、lost-ack-r3-control.jsonl、lost-ack-r3-final-proxy-events.json、lost-ack-r3-product.jsonl | **PASS**（三窗口场景；不声称任意外部系统恰好一次） |
| V7 漏期/重叠/坏定义 | 独立 tick-coalesce-r3：首真实 Job 持定义行锁/audit barrier；并发 second SKIP LOCKED fired0，释放 first fired1；旧 due 三小时仅 audit1/product1，nextdue 未来。Forbid 独立通过；坏定义串行不阻塞由已接受 producer r3 的 wp32-badcron quarantined/disabled 与 good audit2/Kafka2 证明；这是 producer 层与 canonical tick coalesce 层的跨批次组合，不冒充同一次运行 | tick-coalesce-r3.rc=0、tick-coalesce-r3-{waiting,live-wait,old-due,result,final-dispatch,before-versions,after-versions}.json、tick-coalesce-r3-first.log/second.log；forbid-r3.rc=0；`tools/smoke/data-integration-wp3-2/results/20261117T084515_1177/db-state.json`（runtime 提供 case results 相对路径；实际 runner stdout 含 basechain OK auto_cycles2/kafkaevents2/distinctslots2/auditrows2） | **PASS** |
| V8 毒消息/DLQ | tx-failure-r2 offset91、pending-ack-r6 offset114 均 attempts5、唯一 DLQ ID，健康后续真实产品；r6 精确本轮 Kafka Pod netns 对 old PBS IP:9092 DROP，offset 不越毒消息，新 PBS IP 重读 onlyACK，健康完成后 finally 清理；未删 broker/改全局网络 | R/poison-tx-failure-r2-result.json PASS、poison-pending-ack-r6-result.json PASS、poison-final-cleanup.json（实际 exitcodes 均0；broker 原 UID/restarts0/RUNNING、group lag0、INPUT policy无rule、SQL triggers/functions/sequences/holders0） | **PASS** |

## 历史失败及观察边界

- `lost-ack-r2.rc=1` 保留为失败：真实 ACK drop 后 7ms 在同 run 恢复，没有跨实际重启。r3 新通过不改写 r2 历史。
- 初版 counted cycle 使用 legacy `/files` runtimeid 下载返回404，失败日志保留；r2 改用 DVS 既有签名 local-FS HTTP 协议，明确使用 synthetic fixture signing key，并逐历史文件校验。
- 09:01 v2 first-product A2/B5 仅诊断。重启后 aggregate 五 builds/五 receipts 含该 v2 周期，不能计为五个正式 v3 周期。
- manual canonical execute 的预开 reservation 409/F15 仍是旧手动路径限制；scheduled 专用 handoff 已有真实产品证明，不全局关闭 F15。
- canonical identity JobSpec 使用 `logic_payload.transform_type:dataset_input`；旧单 spec `kind:filter` 不是 canonical filter 验收。

## 部署前置及范围边界

生产 authorizer、DSN alias/同一 PBS 库约束、Secret namespace/轮换仍按 deployment profile 列明，不能因隔离验收跳过。未部署生产，生产调度默认关闭。本轮工程代码/Helm/验收 harness/文档纳入同一分支，原始证据与归档保持 ignored；提交与推送以 Git 记录为准。V2/V3/V4 已按独立 rc0 收敛；V7 已由 producer 坏定义层与 canonical coalesce 层组合覆盖，V8 最终独立批次现已通过；历史 P0–P3.1 正文不改，四规划仅更新当前状态注记。

独立 `forbid-r3.rc=0` 是有效验收批次；早期组合脚本因后续 org reason 命名断言失败的 rc1 不作为本批次 rc0 引用，历史不改写。测试 proxy 按各次 scenario 批次 pin，不能用最终 fixture 行为倒推旧批次。

精确 case 索引补充：Forbid 的 `runtime-hold-r3-forbid-org-{live-held-dispatch,blocked-dispatch,final-dispatch,proxy-events,before-versions,after-versions}.json` 及 `runtime-hold-r3-forbid-org-second-event.ndjson`；执行撤权的 `runtime-hold-r3-revoke-{org,personal}-{live-held-dispatch,final-dispatch,proxy-events,before-versions,after-versions}.json`，对应 revoke/control/http 为 jsonl。

## 最终 V8 运行边界

root 已审代码/raw，确认上述两个独立最终批次通过；r1–r4 等失败历史保留。DLQ 经 outbox/CDC 可观察，处置要求 on-call review（ADR-0038）：先核验原事件、reason/attempts、授权与副作用状态，再由值班人员决定修复/重放，不自动盲重放；部署接线与权限参见[部署 profile](data-integration-wp3-2-deployment-profile.md)。本轮验证可观察 DLQ 和处理合同，不伪称实际生产告警已发送。全部 V1–V8 已覆盖；模型执行为 GLM 初始实现→quota 耗尽 Sol 继续，Codex review。本轮证据已持久归档且专属临时资源清理完成：见[最终 checkpoint](../../tools/smoke/data-integration-wp3-2/results/acceptance-20261117/final-checkpoint.md)、[归档 manifest](../../tools/smoke/data-integration-wp3-2/results/acceptance-20261117/manifest.json)、[清理记录](../../tools/smoke/data-integration-wp3-2/results/acceptance-20261117/cleanup.json)、[最终 SHA256 清单](../../tools/smoke/data-integration-wp3-2/results/acceptance-20261117/FINAL-SHA256SUMS)、[清理后源码核验](../../tools/smoke/data-integration-wp3-2/results/acceptance-20261117/post-clean-source-verification.json)。这些是项目 ignored 目录中的本机证据，未入 Git。archive/member SHA 已全验；本轮 kind/namespace、5 个 host 专属容器、4 个确认独占匿名卷、8 个精确 own 镜像、已验证归档的 5423 个 buildctx 文件及私有缓存/工具已清理，skipped0；旧20容器/73镜像 refs/118卷保持不变，168生产文件 pre/post 一致。原始 raw、正式 binary 与源码快照保留。source-snapshot 为封存时版本，本次末段链接追加不改封存、不重写历史。
