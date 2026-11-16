# P2.5 BUILD Candidates — PG-Track Handoff（BUILD-HANDOFF.md）

2026-11-14（按证据时间戳；早前误写 11-15）。PG 轨道独占（root 释放：仅
BUILD 候选，非部署；Tools 不并写本 helper）。上一任务（PG 三阶段 87/87
正式绿）已被 root 接受，原件分析在其外部 pg-three-stage-raw-review.json。
root 已接受 BUILD 候选（142804/142810/142811Z 三 build meta + sourcegate
raw；6 完整 imageID + source a1bff07f…ffd27），**不重 build**。

## Scope（root 精确释放）

- 允许执行：`docker compose build`（固定 q3 project
  `openfoundry-platform-p1-q3v9`）——
  - base3：dataset / pipeline / connector（仅 q3 base compose）；
  - oldaux2：connector2 / connector3（base + 冻结 P22 override
    `$Q3/compose-p22-connector2.yaml`）；
  - newaux1：p25connector（base + **新生成** P25 aux override——由 helper
    写入本轨道 `tools/smoke/business-platform-p2-5/build/compose-p25-aux.yaml`，
    q3 目录与一切旧 yaml 零触碰）；
  - python:3.12-alpine（dataset-proxy/fixture 运行镜像）：**仅本地缓存
    inspect**，不 pull、不启动。
- 禁止：up/down/restart/改旧 yaml/secret/volume/渲染旧工具/改生产源码；
  `.env`（600）仅在 compose 插值内 source，值/Config.Env/DSN 永不打印。
- 硬门：整轮 source content hash before==after==**a1bff07f…ffd27**
  （已接受 PG 候选）；violation ⇒ wrapper rc=2，原始 rc 不动。

## 产物与接口

- Helper：`tools/smoke/business-platform-p2-5/build_candidates_p25.sh`
  （`all` / `status`）。证据：
  `/private/tmp/openfoundry-p25-repair-evidence/build/`——每组件
  `render-aux / build-base3 / build-oldaux2 / build-newaux1 /
  cache-python312alpine / source-gate` 各自
  `.cmd/.meta`（先行）+ `.raw` + 真实 `.rc`；build 组 meta 含每服务
  **完整 image sha256 Id**（`docker image inspect`，非运行容器字段/短ID）
  与 RepoTags。
- **Tools driver 接口**：读取该证据目录即可获得 base3+oldaux2+newaux1
  六服务 build→image SHA 映射与门结果；本 helper 及其结果不改动
  formal/fixture/smoke 任何 Tools 文件。

## 执行状态（已完成，2026-11-14T14:28Z 轮）

- 首跑（142634Z）：build-base3 构建 rc=0，但 per-service 镜像定位用
  `compose images -q` 失败（无容器时返回不存在引用）——helper 修复为
  直查 `<project>-<svc>:latest` tag；首跑原件保留。
- 完整轮（**142804Z**）：render-aux / build-base3 / build-oldaux2 /
  build-newaux1 / cache-python312alpine / source-gate **rc 全 0**；
  source gate before==after==**a1bff07f…ffd27**（已接受 PG 候选）。
- image SHA（docker image inspect 完整 Id，见各 meta）：
  dataset `dc4eca36…c9264`；pipeline `d50858d8…00520`；
  connector `6b556702…97592`；connector2 `9e46fab4…311fd`；
  connector3 `45419b68…3a86b`；**p25connector `59472329…a72db`**；
  python:3.12-alpine（仅缓存 inspect，未 pull/未启动）`c5ba3dee…f6dd89`。
- 指纹：build_candidates_p25.sh `dae008c0…d402`；生成式
  build/compose-p25-aux.yaml `fdbcc99a…57cd`（q3 目录零触碰）。
- 未 up/down/restart；未打印任何 env 值；生产源码零改动。

## r18 机器接口（最终交付）

- **`build-candidate-bindings-r18.json`**（自 SHA
  `a4e5774152c4ea9146f34dcc5968e987921e8c3e6f310c855137726acac9feef`）：
  精确选定 6 组绿 attempt（render-aux/build-base3 142804Z、
  build-oldaux2 142810Z、build-newaux1 142811Z、cache-python 与
  source-gate 142814Z），每组 4 文件（.cmd/.raw/.rc/.meta）**原字节
  SHA256 + 真实 rc**；6 service tag→完整 imageID；python:3.12-alpine
  仅缓存 image；inputs 绑定（project、base/P22 冻结 compose 原字节 SHA、
  NEW aux yaml 路径+SHA、service.Dockerfile SHA、helper SHA、.env 只注
  用途）；失败 142634Z 独立列入 historical_not_selected。
- 消费规约：新 Runtime/Archive 轨道**只读 import** 该 JSON 并把所列
  精确文件复制进最终 bundle；**禁止 ls -t/latest/通配**选取；本轨道
  由此 idle，不跨轨接活。

## 接续点（本任务已完结；如 root 后续指示）

1. 一切以 bindings JSON 为唯一机器事实源；任何重建/复算须 root 新释放。
2. 本轨道 PG 与 BUILD 两阶段均已 root 接受；无未完成事项。

## 接续点（新会话从这开始）

1. 若 `all` 尚在跑：等后台任务完成通知，检查
   `/private/tmp/openfoundry-p25-repair-evidence/build/*.rc` 全 0 +
   source-gate 0。
2. 若 build 失败：保留原件；区分 helper/环境 vs 生产问题——生产问题先报
  root，不自修 prod；helper 小修后重跑并更新本文件 SHA 记录。
3. 全绿后：在 §执行状态 写入逐组 rc+image SHA 表；报告 root（含
   build_candidates_p25.sh SHA 与证据路径清单）；PG/HANDOFF-PG 无需再动
   （PG 阶段已闭）。
