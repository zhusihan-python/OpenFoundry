# P2.6 W6 — Runtime/PG/build/cohort/preflight 工具接口（2026-11-15）

W6 工位（PG 构建与保全验收）独占文件。**本回合只交付 plan/dryrun CLI +
纯自测 + 真实只读验证**；任何真实执行（Docker 写、HTTP、Go 测试、build、
deploy）需 Root 逐条 review 下述 argv 后，经 release token 放行。不改
W1-DATASET-INTERFACE / W2-REPO-INTERFACE / W3-CLIENT-INTERFACE、contract、
任何 services 代码；P25 全部工具/归档/封存只读（`find` 实测 45 分钟窗口
0 修改，含 `__pycache__`）。

## 文件清单（全部本回合新增）

| 文件 | 作用 |
| --- | --- |
| `common/p26_gates.py` | 共享拒绝门：argv lint（禁 pull/`-p/-P/--publish`/`--network host/--privileged`）、旧底板写拒绝（of-p2X-pg、q3 容器/网络、错 compose project）、secrets redaction、`go test -v` 日志解析 + required-names 门、source 哈希 before/after、release token 执行通道 |
| `pg/p26_pg_runner.py` | 可执行 PG runner：`facts / prepare / run-tests(--service×--test-mode) / down / check-log`；mutating 需 `--execute`+release；首错即停、有界 readiness 重试（round-1 重做） |
| `pg/p26-dataset-pg-names.txt` | dataset 冻结 required names（8，W1 §5） |
| `pg/p26-connector-pg-names.txt` | connector 冻结 required names（10，TestPGP26*） |
| `pg/p26-pg-required-names.txt` | superseded 指针（历史保留） |
| `common/p26_source_hash.py` | P25-parity source hash CLI（真实 repo parity 已证） |
| `cohort/p26_cohort_cli.py` | 保全 cohort CLI：`verify-seal / derive-protected / export-baseline / plan-quiet-checks / plan-oldpg-readonly` |
| `build/p26_build_plan.py` | `plan-build / plan-swap / plan-fixture-http`（候选构建、保全 switch、新 fixture HTTP 验收的计划与拒绝门） |
| `w6/selftest_*.py`, `w6/run_selftest.py` | 纯自测（合成夹具 + injectable fake runner；负控=零 Docker/HTTP 调用） |
| `results/w6/` | 自测转录、真实只读验证输出、docker facts、baseline-a1 导出 |

## 执行模型（Root 放行机制）

一切真实执行必须同时满足：(1) `--execute`；(2) release 文件
`/private/tmp/openfoundry-p26-release.json`（schema：
`{"released_for": ["up","test-normal",…], "released_by": "root",
"expires_utc": "YYYYMMDDTHHMMSSZ"}`）列出了该 phase；(3) 每条 argv 过
`common/p26_gates.py` 全部门。任一不满足 → 拒绝且 runner 零调用（自测
`execute-no-release-zero-calls` 等 4 项负控验证）。本回合不存在 release
文件，故本回合任何 `--execute` 也只会拒绝。

## PG 轨道（round-1 重做：可执行 runner；真实执行待 Root 窄放行）

`pg/p26_pg_runner.py` 模式：`facts`（白名单只读，已真实执行 rc0）/
`prepare` / `run-tests --service {connector,dataset} --test-mode
{normal,race}` / `down` / `check-log`（纯）。可执行语义：

- **引擎**（common `run_phase`）：lint（允许 `--pull=never`，拒其余 pull/
  `-p/-P/--publish`/`--network host`/旧底板写/错 compose project）→ 真实
  subprocess（无 shell）→ check 谓词；**首错即停**；仅 readiness 有界重试
  （≤20×3s）；mutating 步需 `--execute` + release token 列名 phase，否则整
  phase 拒绝、零调用。
- **prepare 实际序列**：三资源存在性预检（not-found≠docker 失败；任一存在
  →保留+exit 3 零写）→ `docker image inspect postgres:16 --format {{.Id}}`
  内存解析校验 64-hex（无 pull；当前缓存 `e17e8606…3dd3d5`）→
  `os.open(O_CREAT|O_EXCL,0o600)` 写 `POSTGRES_PASSWORD=<48hex>` →
  `docker network create --label of-p26-owner=p26-pg-isolated of-p26-pg-net`
  → `docker run -d --pull=never --name of-p26-pg --network of-p26-pg-net
  --label … --env-file /private/tmp/openfoundry-p26-pg.env -e POSTGRES_USER=
  ofp26 -e POSTGRES_DB=postgres sha256:<fullID>`（无 host port；密码只在
  env-file）→ `docker exec of-p26-pg pg_isready -U ofp26 -d postgres`（有界
  重试）→ 只读谓词复核（label/Running/.Image==fullID/Ports=={}/
  `docker port` 空）。
- **run-tests 实际 argv**（两服务 cohort，事实取自 W1/W2 原始 cmd）：

  | | connector | dataset |
  |---|---|---|
  | 镜像 | `golang:1.25`(go1.25.14) | `golang:1.26.3` |
  | cache 卷 | openfoundry-gomod/gobuild | of-go-mod/of-go-build（缺失即拒） |
  | -w | /src/services/connector-management-service | /src |
  | packages | ./internal/repo/ ./internal/handlers/ | ./services/dataset-versioning-service/internal/repo/ |
  | DSN env | CONNECTOR_DB_DSN | DATASET_DB_DSN |

  `docker run --pull=never --rm --network of-p26-pg-net -v <SRC>:/src:ro
  -v <modvol>:/go/pkg/mod -v <buildvol>:/root/.cache/go-build -w <cwd>
  --env-file /private/tmp/openfoundry-p26-pg-test-<svc>.env -e
  GOTOOLCHAIN=local -e GOFLAGS=-mod=readonly -e GOPROXY=off -e
  CGO_ENABLED={0|1} sha256:<goID> sh -c "go version && go vet
  -tags=integration <pkgs> && go test -tags=integration {-race }
  -count=1 -v -run '^(<frozen names>)$' <pkgs>"`
- **门（代码内评估）**：version（首行==cohort go 版本）、names（每名恰
  1 RUN/1 PASS/0 SKIP/0 FAIL+package ok；缺名 FAIL）、source（before==
  after，P25 `candidate_stream` 逐字节同构——真实 repo parity 已证）；产出
  `.raw`（全量、secret scrubbed）/.rc（原始）/.cmd（redacted）/.meta
  （UTC start/end、image、names、双哈希、scrub 次数）+ 四个独立 gate rc。
- **frozen names**：`pg/p26-dataset-pg-names.txt`（8，W1 §5）、
  `pg/p26-connector-pg-names.txt`（10，TestPGP26*；recovery_integration_
  test.go 落地源）；首跑前冻结，无循环 pinned；旧单文件已 superseded。

## 保全轨道（真实只读验证已跑，输出在 results/w6/）

- `verify-seal`：sha256(`hash-manifest.json`) == `a1e760b7…f0ad0`，5016 文件
  逐哈希（本回合抽样 50 全对；全量扫描属后续释放形态）→
  `results/w6/real-verify-seal-limit50.json` rc 0。
- `derive-protected`：从封存链派生 protected9（原6+H10 held+最终ABC2）与
  8CP（旧失败2+H10+tail5），**全链结构门**：9 行恰 27 列 schema、UUID/org
  父链、4 个 fixture 对组（3 原 + ABC，ABC hex 必须等于 abc-result
  suffix，且 abc logical_path 必须携带 C 侧 run id）、原6 必含 P24 GROUP2
  B/C run id（dcca308f…/885d1429…）、roster 8 恰 kind=owner、origin 组成
  恰 {failedhealthy:2, h10-prepublish-crash:1, intentional-tail:5}、tag
  白名单、h10 roster run_id == p25h13 受控行 id、cp-owners 整集（未知
  run id 直接拒，绝不 count-过滤）、quiesce before.json 交集内 def 必须
  父链成立（实测 9 中恰 1 个 def 在 before.json=H10，其余 8 个记
  `in_quiesce_before=false` 事实）→
  `results/w6/real-derive-protected.json` rc 0。组成实测：
  9 = original6×6 + h10-held×1 + abc-final×2；8 = 5+2+1。
- `export-baseline`：bundle manifest sha == `1fd0329e…05bd`；三个最终
  attempt（new-normal/new-race/unified，compile-* 排除）meta 的
  source_content_sha256 before/after 一致且 == 封存 a1
  `a1bff07f3760fce7b5661b169ba26a165712c950035d4e4ec9948639d51ffd27`
  （注：该常量首版手抄错 1 字符，已被门拒绝后按封存原件修正）；按 hash
  导出 bundle 内历史 repo 副本 1405 文件到 `results/w6/baseline-a1/` →
  `results/w6/real-export-baseline.json` rc 0。当前树**不**用 a1 判绿。
- `plan-oldpg-readonly`：列出（不执行）两条与封存 .cmd 逐字一致的只读
  SELECT argv（27 列受控行投影 + cp-owners），供 Root 后续单独放行；本
  回合不发 SQL。
- `plan-quiet-checks`：输入 docker-ps 白名单快照；规则：容器不在清单 →
  `unknown_not_quiet`；命中已移除 P25 fixture/proxy pattern →
  `removed_not_quiet(NEVER green)`；q3 base3+fixture 在清单才可判 quiet。

## build/swap/fixture-HTTP（计划+拒绝门，均未执行）

- `plan-build`：q3 base compose **build only**（`--project-name` 必须恰为
  `openfoundry-platform-p1-q3v9`；`down/kill/stop/restart/start/run` 一律
  拒绝）+ P26 aux override 只生成于本目录；source before/after 硬门。
- `plan-swap` 输入 binding schema：`{candidate_service, candidate_image_
  sha256(全64hex), old_running:[{name,image}](必填), protected_roster(
  derive-protected 输出)}`；缺 old 身份 / 短 sha / roster 非法 → 拒绝且
  steps 为空（自测证实 zero runner calls）。swap 后只读复核用封存 SELECT。
- `plan-fixture-http`：只允许新 P26 fixture URL；命中 removed P25
  pattern 的 URL 在计划阶段即拒（不生成 curl 步）。

## 负控矩阵（自测 111/111，含下列"零 Docker/HTTP 写"证明）

| 负控 | 结果 |
| --- | --- |
| 无 release / phase 未列 release / gate 违例 | runner 0 调用（`execute-*-zero-calls`、`aborts-zero-extra-calls`） |
| required-names 缺文件 / 零 active / 缺名日志 / SKIP / FAIL / 重复 RUN | 全部 FAIL/拒绝 |
| 旧底板（rm/stop of-p2X-pg、q3 容器；network create 旧名；exec 旧 PG） | 静态拒绝 |
| pull / `-p` / `-P` / `--network host(=)` / `--privileged` | 静态拒绝 |
| compose 错 project / compose down 等整栈动词 | 静态拒绝 |
| 坏 seal SHA / 文件数错 / 封存文件篡改 | verify-seal rc 1 |
| 未知 fixture_src / 26 列残行 / roster 7 行或未知 origin / cp-owners 未知 run / legacy B/C 缺失 / h10 quiesce 链断 | derive 拒绝（rc 1，写入 refused JSON） |
| 输出路径落在 p2-5 树内 | CLI rc 2 / CohortRefusal |
| swap 缺 old 身份 / 短 sha / 坏 roster；fixture-http 命中 removed pattern | 计划阶段拒绝，零步骤 |

## 本回合明确未执行（等 Root 逐条放行）

真实 PG up/测试（normal/race/unified）、go vet/test 容器执行、候选
build、swap、任何 HTTP 验收、旧 PG 只读 SQL、seal 全量 5016 文件扫描、
任何 service/PG/网络/卷的启停。

## 事实（2026-11-15 r2 重查核实，报 Root）

1. **P26 PG 集成测试名已落地并冻结**（修正 r1"为零"的过时判断）：
   dataset 8 名（W1 keyed receipts）、connector 10 名（TestPGP26*），
   分别冻结于两个 names 文件；W1 §5 给出的标准命令为
   `go test -tags integration -count=1 -race -v -run 'W1|WithIdempotent…'`
   （本 runner 用等价且更严的 exact-list 正则）。
2. W2 INDEX 记录 `go vet` 全量失败点在并行工位
   `internal/runtime/dataset_recovery.go:214`（未用变量）——非本工位，未碰。
3. docker 只读快照（`results/w6/pg/facts_20261115T024026Z/` 与
   `preflight-docker-facts.json`）：q3 base3+fixture 在跑；of-p22/23/24/25-pg
   全部原样；postgres:16 / golang:1.25(=go1.25.14) / golang:1.26.3 均已缓存。
4. 两次手抄 pin 教训：封存 a1 错 1 字符（被 export-baseline 门拒绝后按原件
   修正）；postgres:16 recorded ID 漏 2 字符（被 hex64 校验拒绝后按 docker
   原件程序化修正）——校验门按设计工作。
