# P2.6 最终封存包 README（模板 — 装配时由 W9 复制入包并按实况填充）

> 本文件是模板。装配阶段复制为包根 `README.md`，填充 `<…>` 占位；凡与实际
> 证据不一致处以实际证据为准，不造 pass。包内一切断言以
> `roster/p26_formal_roster.json`（Root 定稿版）与 `root-acceptance.json`
> 为准；缺席或不满 ⇒ verifier 判 `NOTREADY`。

## 这是什么

OpenFoundry P2.6（幂等重试与提交恢复）在冻结源
`S = b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c`
上的自包含证据包：Go+SQL 源快照、正式证据（PG 27 normal + 27 race 新名、
unified 114、三服务全量非 tag unit 6 rc0、两个 COMPOSITE build）、失败历史
（原样保留）、roster 与离线 verifier。**离线验证 = 校验
证据/源/manifest/规则，不需要 Docker / PG / HTTP / 原仓库 / 私有环境。**

## 如何离线验证（stdlib Python，`python3 -I -B`）

在任意新目录解开本包后（不要在原工程树内跑，也不要让原 workspace/私有
q3 路径可读——独立拷贝验证是 Root 的要求）：

```bash
# 1) 确定性 manifest（成员/内容/总SHA；多余或缺失即拒绝）
python3 -I -B tools/p26_archive_manifest.py verify . manifest.json

# 2) 独立总验证（源S标签recipe + manifest + 证据规则 + secret扫描 + fail-closed）
python3 -I -B tools/p26_archive_verifier.py verify . \
    --canonical-root-label /Volumes/ExternalSD/projects/OpenFoundry \
    --emit-report-skeleton ../verification-report.json
# rc 0 = VERIFIED-NOT-SEALED；rc 1 = NOTREADY（problems 逐条列出）

# 3)（封存阶段）ACTUAL 秘密值经 stdin 注册进内存（值绝不落盘/入包）：
# 由已批准的凭据提供器通过匿名管道向 scanner stdin 提供值；值不得出现在 argv/cmd文件/报告中。
# python3 -I -B tools/p26_secret_scan.py scan . --values-stdin
```

判定语义：最好情形 `VERIFIED-NOT-SEALED`。**seal/发布是 Root/W10 阶段**；
本包与工具永不打印 PASS/SEALED 替代词。

## 证据规则（必须知道的口径）

- `classification` 必须为 `actual`；`fake / plan / synthetic / mechanics-fake /
  history-only` 只是历史，永不满足任何必需项。
- `rc_kind=unix` ⇔ `rc == 0`；`rc_kind=http-status` ⇔ `rc` 是 100..599 的
  HTTP 状态、等于 `expected_status`，且**外层 CLI 的 Unix rc 单独记录为
  `outer_rc == 0`**——HTTP 步的 `.rc` 是状态不是 Unix 退出码，二者绝不混写。
- 两个 build 为 **COMPOSITE 接受**：原始 build 目录中 08verify rc1/outer rc2
  失败原样保留（`evidence/w6-build/build-*_20261115T*`），resume
  verify/cleanup 全 rc0 才是 satisfier；historical builder 脚本 sha
  `3a546d36…cf46` 与当前 null-safe 变体不同，包内分别标注，不伪造旧源。
- unit 证据**不**宣称 optional external provider/cluster/sidecar 已验证；
  非 verbose 输出无法枚举 per-test skip（静态清单见
  `evidence/w1b-final-unit/SKIP-LIST.md`，源扫描口径）。
- `<…如 W7 HTTP / legacy 回归 / 保全 / 部署在本包中的最终状态，按 Root
  定稿 roster 填写；未完成项保持 pending 且包整体 NOTREADY>`

## 边界

- 旧 P25 工具与 sealed archives 仅是只读历史参照：不导入、不执行、不修改。
- 本包 `source/` 只含 Go+SQL recipe 所需文件集（go.mod/go.sum/services/*/
  libs/scheduling-cron），用于复算 S；完整工程与私有环境不在包内。
- 私有文件（.env / DSN / JWT / 签名材料 / 源 body payload）永不入包；
  secret 扫描报告只含 kind+file:line+长度。
- 工具版本：`<python3 --version>`；工具 SHA 见 `SOURCES-SHA256.txt`。

## W9 draft-3 原件合同（覆盖早期模板简写）

manifest.json 必需；正式 roster 路径必须是包内相对 manifest 成员，不能传工作区
绝对路径。复制本目录的五个 Python 工具（包括 p26_original_proofs.py），从包内
执行 verifier。报告必须输出到包外，避免修改 manifest 成员。

每个 accepted/pending 项必须提供非空唯一 satisfiers、contracts；contract 必须
与 evidence/evidence.json 中该步逐字段一致，并绑定原始 rc 文件与 proof。proof
列出固定必需测试名/版本、原件相对路径和 original_files 的精确 SHA/size 集合。
Root acceptance 的 accepted 必须是 {id,satisfiers} 对象，且相同原件校验通过；
只给 pending ID 不构成关闭。PG/unit/build-composite 已实现；其他类型尚未实现
业务语义读取，fail-closed NOTREADY，待 Root final phase 明确合同后补齐。

本次原件复核不等于独立离线验证。最终副本运行、原仓库/私env不可读验证、
HTTP/legacy/typed保全与最终 secret scan 均未执行，不能按模板声明已完成。
