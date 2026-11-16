# P2.4 implementation-contract.md — 精确实现契约（对 PG/工具轨道的权威对齐源）

2026-11-14，rev3：已纳入 root round-1 + round-2 review 全部修正。本文签名即实现目标——PG 测试据此编写；实现内必要微调同步更新本文并注记。

**rev3 增量（round-2，签名/语义变化处）**：
1. `ArmScheduleGrant`：写前校验扩展——`in.OwnerSubject==in.ActorSubject==t.OwnerID`（caller sub，即使 org 域；Source owner 只快照 run.owner_id）、`in.OrgID` 与 `t.OrgID` 及锁后 def→source 域一致（org: g.org=d.org=c.org；个人: 全 NULL 且 actor==c.owner_id）、`in.Revision==expectRevision ∧ in.OutputDatasetID==expectOutput`；锁后读现有 grant 行，**当前 grant_id 复用（原地 renew）→ ErrScheduleArmInput**（每次 renew 必须新 UUID）；UPSERT 前后各采样一次 DB 时钟——**UPSERT 可等待 grant 行锁（如心跳事务），写后再读 fresh clock，过期 ⇒ ErrScheduleArmExpired 并回滚整个事务**（旧 grant 字节保持）。PG 测试需补：纯 grant FOR UPDATE 阻塞、阻塞期间输入仍有效、释放时已过期 → 完整回滚（旧 Grant/Def/游标/stamp 不变、无新 run/tick）。
2. `GetScheduleGrantView`：占位符修正为 `$1=defID, $2=freshness, $3=tenant arg`；JOIN connections；live 判定含**域真值**（org: g.org=d.org=c.org ∧ owner_subject=actor_subject；个人: 三 org NULL ∧ actor==c.owner_id）。
3. `HeartbeatScheduleGrant`：phase-3 MVCC 读扩展为 def+source（revision/source/output/org/owner）并以 `grantDomainMatches` 复核域。
4. `ClaimDueScheduleSlot`：live 判定同样加域真值（锁后 def org + source org/owner）。
5. Authorizer：`LockDef(ctx, defID) (unlock func(), ok bool)`——channel-key + 引用计数（refs==0 即回收，辅助状态有界）、等待可取消（ctx）、超过 RegistryCap 且无既有 key ⇒ busy false（HTTP 503）；`ReserveArmSlot` **同 def 已有活跃条目时允许满容量 renew 替换**（新 def 拒绝）；`AbsoluteDeadline` 输出 `UTC().Truncate(time.Microsecond)`（DB 微秒与内存精确相等）；`ActivateArm` 严格化：要求 `view.Live ∧ grant_id/boot/actor/revision 全等 ∧ view.ExpiresAt.Equal(material.ExpiresAt)`，否则 superseded；`ArmClaim` 命中已过期条目**立即删除**并返回 nil；Start 拆两条独立循环——**1s 纯内存 expiry 清扫**（不被心跳 IO/满 registry/调度关闭阻塞）+ 30s 心跳轮（单次 5s、整轮 25s）。
6. HTTP：revoke 也在 `LockDef` 同键序列内做 DB revoke + 精确 Drop（防 Activate 读后写回竞态）；`ListScheduleTicks` 增加 `GetConnectionForTenant + canReadSource`（Source 404 / 403），与 sync list 面一致。
7. 单测新增要求：满容量新 def 拒绝/既有 def renew 成功；nano-origin 过期精度（非秒对齐）的 MatchesLiveGeneration；TTL≈1s 条目在 1s 清扫内消失（不被 25s 心跳轮拖住）。

模块路径前缀 `github.com/openfoundry/openfoundry-go/services/connector-management-service`（下称 `connector/`）。变更摘要（相对 rev1）：Arm 父链一致性校验+锁后时钟拒绝过期输入；Heartbeat 两阶段同连接锁后 DB 时钟（不拿 Def 锁，MVCC 复核当前绑定）；Claim 的 Def 锁也 NOWAIT；全部新 DB 方法内部 5s 操作界（rollback 5s 另算）；enqueued TickID 必返；GetScheduleGrantView 去 defRevision 参数、同 SQL JOIN 实际 Def 判 live；Revoke 返回实际删除的 grant_id；新增 DeleteScheduleGrantExact；authorizer 改 identified ticket + 同键 stage/DB/activate 序列；TTL 锚定 Reserve 时刻（precheck 之前）；UpdateSyncJob legacy 兼容校验（只校验显式新供字段与 enable，不再全量重验 merged）；ResolveScheduleTimeZone 显式拒绝 "Local"。

## 1. Migration（唯一新文件）

`connector/internal/repo/migrations/20261115120000_sync_schedule_observability.sql`（已写入）：与 design-draft v2 §5 一致——`batch_sync_defs` 增 7 列（time_zone/enabled/revision/next_fire_at/last_fire_at/last_tick_at/last_tick_outcome）、partial due 索引、`sync_schedule_ticks`（UNIQUE `sync_schedule_ticks_slot_uniq (sync_def_id, schedule_revision, scheduled_for)`、detail≤256 CHECK、`idx_sync_schedule_ticks_def_time`）、`sync_schedule_grants`（PK sync_def_id、grant_id/boot_id/actor/owner/source/output/org/revision/armed/expires/last_seen）。幂等、无 CONCURRENTLY、无回填、`sync_runs` 零改动。

## 2. models（已写入 `models/schedule.go` + models.go 扩列）

关键签名（已实现）：
```go
const ScheduleCronMaxLen = 256
type InvalidScheduleError struct{ Field, Reason string }   // Error() = "invalid_schedule: <field>: <reason>"
func InferCronFlavor(expr string) (schedulingcron.CronFlavor, error)  // 5字段→Unix5, 6字段→Quartz6
func ParseScheduleCron(expr, timeZone string) (*schedulingcron.CronSchedule, error)
func ResolveScheduleTimeZone(timeZone string) (*time.Location, error) // 空→UTC；"Local" 显式拒绝（Go1.26.3/1.25.14 已核验）
func NextScheduleFire(expr, timeZone string, after time.Time) (time.Time, error)
func NormalizeScheduleSpec(cron, timeZone string, enabled bool) (ScheduleSpec, error)
// SyncJob 增：ScheduleTimeZone string / ScheduleEnabled bool / ScheduleRevision int /
//   ScheduleNextFireAt|ScheduleLastFireAt|ScheduleLastTickAt *time.Time /
//   ScheduleLastTickOutcome *string / ScheduleAuthorization *ScheduleAuthorizationView（仅 GET 单读填充）
// Create/Update 请求增：ScheduleTimeZone *string、ScheduleEnabled *bool
type ScheduleAuthorizationView struct{ DBGrant *ScheduleGrantView `json:"db_grant,omitempty"`; LocallyExecutable bool `json:"locally_executable"` }
type ScheduleGrantView struct{ GrantID, ActorSubject, BootID uuid.UUID; Revision int; ArmedAt, ExpiresAt, LastSeenAt time.Time; Live bool }  // JSON snake_case
type ScheduleGrantWrite struct{ SyncDefID, GrantID, BootID, ActorSubject, OwnerSubject, SourceID, OutputDatasetID uuid.UUID; OrgID *uuid.UUID; Revision int; ExpiresAt time.Time }
type ScheduleTick struct{ ID, SyncDefID uuid.UUID; ScheduleRevision int; ScheduledFor, ClaimedAt, CreatedAt time.Time; Outcome string; RunID, Actor *uuid.UUID; Reason, Detail *string }
```

## 3. repo（已写入 `repo/schedule.go` + repo.go/lease.go 改造）

```go
const scheduleOwnerFreshness = 90 * time.Second   // owner 新鲜度窗口
const scheduleTickRetention = 500                 // 每 def tick 保留
const scheduleOpBudget   = 5 * time.Second        // 每个新 DB 入口内部操作界（rollback 预算另 5s）

type ScheduleTickOutcome string  // enqueued | dispatch_failed | auth_required |
                                 // skipped_admission_capacity | skipped_admission_min_interval | error_invalid_schedule
// auth_required reason: never_armed | window_too_short | stale_revision | grant_expired | owner_stale
type ScheduleClaimSkip string    // not_visible | contention | stale_candidate | not_due | remote_owner

type ScheduleScanKey struct{ NextFireAt time.Time; DefID uuid.UUID }
func (r *Repo) ListDueScheduleCandidates(ctx, hint time.Time, after *ScheduleScanKey, limit int) ([]ScheduleCandidate, error)
// keyset 轮转：(schedule_next_fire_at, id) > after；hint 仅扫描提示。

type ScheduleCandidate struct{ SyncDefID, SourceID uuid.UUID; Revision int; ScheduledFor time.Time }
type ScheduleArmClaim struct{ GrantID, BootID, Actor uuid.UUID; Revision int; Tenant Tenant; SourceID, OutputDatasetID uuid.UUID; Authorization string; ExpiresAt time.Time }
type ScheduleClaimResult struct{ Claimed bool; Skip ScheduleClaimSkip; Outcome ScheduleTickOutcome; TickID uuid.UUID; ScheduledFor time.Time; NextFireAt *time.Time; Run *models.SyncRun; ArmTenant Tenant }

var (
    ErrScheduleArmTargetChanged = errors.New("schedule arm target changed")     // → HTTP 409 重试
    ErrScheduleArmExpired       = errors.New("schedule arm authorization already expired") // → HTTP 400/重arm
    ErrScheduleArmInput         = errors.New("schedule arm input inconsistent with locked definition") // → HTTP 400
)

func (r *Repo) ClaimDueScheduleSlot(ctx, cand ScheduleCandidate, arm *ScheduleArmClaim) (*ScheduleClaimResult, error)
// 内部 5s 界。Source FOR UPDATE NOWAIT（恒）→ Def FOR UPDATE NOWAIT（恒，含 policy-off；
// 55P03=contention skip，事务 abort 后仅允许有界回滚）→ 锁后 clock_timestamp() 权威 →
// 候选 CAS(revision∧slot) → due → 锁后 cron 解析（先于一切分支；失败=error_invalid_schedule、
// 游标 NULL、无 run）→ grant 裁决（live=行∧绑定匹配锁后 def∧expiry∧freshness；本 arm 全代次
// 匹配→准入；!live→auth_required+推进；live 他人→remote_owner 让位）→ admissionGate（typed
// 拒绝→skipped_*+推进，不推 stamp）→ insertPendingSyncRun（共享助手）→ 游标 UPDATE → tick
// INSERT（ON CONFLICT ON CONSTRAINT sync_schedule_ticks_slot_uniq DO NOTHING，0 行=stale 回滚）
// → prune → advanceSyncLastAdmitted（成功事务最后一条 DB 语句）→ COMMIT。enqueued 结果必带
// 实际 TickID 与 Run。

func (r *Repo) ArmScheduleGrant(ctx, defID uuid.UUID, t Tenant, in models.ScheduleGrantWrite, expectRevision int, expectOutput uuid.UUID) (*models.ScheduleGrantView, error)
// 内部 5s 界。写前校验 trusted 输入一致性：in.SyncDefID==defID、全 ID 非 zero；Def FOR UPDATE
// （tenant 谓词）→ 复核 revision/output/source 与 in 一致（否则 ErrScheduleArmTargetChanged）→
// 锁后独立 clock_timestamp()：in.ExpiresAt ≤ dbNow ⇒ ErrScheduleArmExpired（绝不写过期 arm）→
// UPSERT（全部写 defID，非 in.SyncDefID）→ RETURNING 视图，Live 按实际值计算（非硬编码）→
// COMMIT。def 游标/revision 不动。不可见 def → (nil,nil)。

func (r *Repo) RevokeScheduleGrant(ctx, defID uuid.UUID, t Tenant) (*uuid.UUID, error)
// 内部 5s 界。Def FOR UPDATE → DELETE RETURNING grant_id：返回**实际删除代次**的 grant_id
// （无行/不可见 → nil）。调用方只 Drop 该代次，绝不无条件 Clear 后来新 arm。

func (r *Repo) DeleteScheduleGrantExact(ctx, defID, grantID uuid.UUID, t Tenant) (bool, error)
// 内部 5s 界。Def FOR UPDATE → DELETE WHERE grant_id=$2：只删自己的代次（DB 已 commit 但
// 未激活的 arm 的迟到清理）。

func (r *Repo) HeartbeatScheduleGrant(ctx, defID, grantID, bootID, actor uuid.UUID, revision int, freshness time.Duration) (bool, error)
// 内部 5s 界。两阶段同连接（P21 EPQ 证明的形状）：① SELECT ... WHERE 精确代次（无时钟谓词）
// FOR UPDATE；② 锁后独立 SELECT clock_timestamp()；③ MVCC 读当前 def（revision/source/output，
// 无 Def 锁——不逆序）并复核绑定/expiry/freshness（不过→false，行原样）；④ UPDATE last_seen_at
// （绝不写 expires_at、不复活）。false ⇒ 调用方丢弃该本地代次。

func (r *Repo) GetScheduleGrantView(ctx, defID uuid.UUID, t Tenant) (*models.ScheduleGrantView, error)
// 内部 5s 界。单 SQL JOIN 实际 def：live = revision∧source∧output 绑定匹配 ∧ expiry>clock ∧
// last_seen>clock-90s；tenant 谓词经 def。无行/不可见 → (nil,nil)。**无 defRevision 参数**。

func (r *Repo) ListScheduleTicks(ctx, defID uuid.UUID, t Tenant, limit int) ([]models.ScheduleTick, error)  // limit 1..200 默认50，def 谓词 join
func (r *Repo) RecordScheduleTickDispatchFailure(ctx, defID uuid.UUID, revision int, scheduledFor time.Time, aftermath string) (bool, error)
// UPDATE 精确 (def, revision, scheduled_for) ∧ outcome=enqueued → dispatch_failed，detail=left(,256)。

// repo.go：RunDueSyncJobs 已删；syncJobSelect/scanSyncJob/CreateSyncJob/UpdateSyncJob 已扩。
// UpdateSyncJob 校验规则（round-1）：显式新供非空 cron/tz 必须合法（即使 disabled）；enable
// 要求 merged cron 合法非空；**未触碰的 legacy 非法 disabled cron 在无关 PATCH 下原样存活**；
// 空 cron 显式清除并停用（若同请求显式 enabled=true 则报错）；实际变更（cron/tz/enabled/
// output/file_glob 任一真变）→ revision+1 ∧ enabled 时以锁后 DB 时钟重算游标、disabled 时置
// NULL；同值 PATCH 不 bump 不动游标。CreateSyncJob 全量校验（新行无 legacy），revision=1，
// enabled 时游标锚 tx 内 DB 时钟。
// lease.go：EnqueueSyncRun 的 INSERT 抽为 insertPendingSyncRun 共享（SQL 逐字相同）。
```

## 4. handlers — coordinator（已写入）

```go
func (a *Admission) Dispatch(t coordinatorTask, authorization string) bool                        // 手工路径不变
func (a *Admission) DispatchScheduled(runID, syncID, sourceID uuid.UUID, tenant repo.Tenant,
    requestedBy uuid.UUID, authorization string, deadlineCap time.Time) bool
// 内部 dispatchCapped：auth 内存过期与任务截止 = min(Reserve 锚定值, deadlineCap)；零值 cap
// 等价 Dispatch。排队等待消耗同一预算；过期后不新起 pull/预检/发布门；已进门保持 P2.2 收敛。
```

## 5. handlers — 授权器（ticket API）与循环（本轮实现中，签名如下）

```go
// schedule_auth.go
type ScheduleAuthorizerConfig struct{ MemoryTTL time.Duration /*默认300s,1..86400*/; RegistryCap int /*1024*/; Freshness time.Duration /*90s*/; MaintainEvery time.Duration /*30s*/ }

type scheduleArmTicket struct{ defID, grantID uuid.UUID; stagedAt time.Time }  // identified 预留
type activateStatus int
const (activateActivated activateStatus = iota; activateSuperseded; activateStopped)

type ScheduleAuthorizer struct{...}
func NewScheduleAuthorizer(cfg ScheduleAuthorizerConfig, store ScheduleAuthStore, bootID uuid.UUID) *ScheduleAuthorizer
// ScheduleAuthStore：HeartbeatScheduleGrant + GetScheduleGrantView（子集接口）
func (a *ScheduleAuthorizer) Start(ctx context.Context)   // 独立维护循环：每轮先无锁清扫过期本地凭据
                                                          // （纯内存，绝不持 registry 锁做 IO），再快照后逐个
                                                          // 心跳（单次 5s 界、整轮预算 25s，剩余下轮）；
                                                          // IO 期间不持 registry 读 mutex；心跳 0 行 ⇒ 丢弃该代次
func (a *ScheduleAuthorizer) Stop() error                 // 同一 gate 下原子关闭 stage/activate → 清零凭据 →
                                                          // 有界 join 维护；Stop 后 Activate=stopped，绝不重填 JWT
func (a *ScheduleAuthorizer) ReserveArmSlot(defID uuid.UUID) *scheduleArmTicket  // DB arm 前占本机容量；满/关→nil
func (a *ScheduleAuthorizer) ReleaseArmSlot(t *scheduleArmTicket)                // 只归还该 ticket 的预留
func (a *ScheduleAuthorizer) ActivateArm(t *scheduleArmTicket, material models.ScheduleGrantWrite,
    authorization string) (activateStatus, error)
// 完整内部 material（非公开 DTO；secret 不入 DTO）。同 def 键下串行：per-def activate mutex 内
// 先以 store.GetScheduleGrantView 复核 DB 行 grant_id == t.grantID（不匹配→superseded，迟到旧
// arm 不得覆盖新成功者；DB 读失败→typed error）→ 匹配→安装本机条目（同代次的 DB 元数据与本机
// token 一起原子落位）。staged→DB→activate 全程同键序列化。
func (a *ScheduleAuthorizer) Drop(defID, grantID uuid.UUID)  // 只丢弃指定代次（revoke/心跳失败路径）
func (a *ScheduleAuthorizer) MatchesLiveGeneration(defID uuid.UUID, view *models.ScheduleGrantView) bool
// locally_executable 的判定：本机条目存在 ∧ view 非 nil ∧ view.Live ∧ 代次全匹配
// （grant_id/boot/revision/actor/expires_at 相等）。单纯 map 命中不算。
func (a *ScheduleAuthorizer) ArmClaim(defID uuid.UUID) *repo.ScheduleArmClaim  // 本机活跃代次→claim 材料
                                                             // （含 Authorization；不匹配当前 revision 也返回，
                                                             //   由 claim 锁后裁决）
func (a *ScheduleAuthorizer) BootID() uuid.UUID

// schedule_loop.go
type ScheduleRunnerConfig struct{ Interval time.Duration; BatchSize int /*8*/ }
type ScheduleTickSummary struct{ Scanned, Claimed, Enqueued int; Skips map[string]int; Errors int }
type ScheduleRunner struct{...}
func NewScheduleRunner(cfg ScheduleRunnerConfig, store ScheduleLoopStore, coord *RunCoordinator, authz *ScheduleAuthorizer, log *slog.Logger) *ScheduleRunner
type ScheduleLoopStore interface {
    ListDueScheduleCandidates(ctx context.Context, hint time.Time, after *repo.ScheduleScanKey, limit int) ([]repo.ScheduleCandidate, error)
    ClaimDueScheduleSlot(ctx context.Context, cand repo.ScheduleCandidate, arm *repo.ScheduleArmClaim) (*repo.ScheduleClaimResult, error)
    RecordScheduleTickDispatchFailure(ctx context.Context, defID uuid.UUID, revision int, scheduledFor time.Time, aftermath string) (bool, error)
    RequestCancelSyncRun(ctx context.Context, runID, syncID uuid.UUID, t repo.Tenant, cancelledBy *uuid.UUID, cause string) (*repo.CancelResult, error)
}
func (r *ScheduleRunner) RunOnce(ctx context.Context) ScheduleTickSummary  // keyset 轮转、每候选 Reserve→claim→
                                                                          // enqueued 则 DispatchScheduled(cap=arm.ExpiresAt)
                                                                          // （失败⇒有界关闭+补记 dispatch_failed）
func (r *ScheduleRunner) RunLoop(ctx context.Context) error
```

## 6. HTTP（本轮接线）

```
POST   /api/v1/data-connection/syncs/{id}/schedule:arm   → h.ArmScheduleAuthorization
DELETE /api/v1/data-connection/syncs/{id}/schedule:arm   → h.RevokeScheduleAuthorization
GET    /api/v1/data-connection/syncs/{id}/schedule/ticks → h.ListScheduleTicks
```
- arm 流：claims→GetSyncJob(404)→GetConnectionForTenant+canWriteSource(403)→`ReserveArmSlot`（锚定
  `expiresAt=min(JWT exp, ticket.stagedAt+TTL)`，**先于输出预检**）→precheckOutputDataset→
  ArmScheduleGrant（409 target_changed / 400 expired / 400 input / 5xx）→ActivateArm
  （stopped→503；superseded→409 且用 DeleteScheduleGrantExact 清理己方已 commit 未激活代次；
  DB error→5xx）→200 `{armed,grant_id,actor_subject,schedule_revision,authorization_expires_at,
  scheduler_loop_running,note}`。循环关闭时 `scheduler_loop_running:false` 如实提示。
- revoke：同前置链→DB RevokeScheduleGrant 返回实际删除 grant_id→`Drop(defID,该id)`→
  200 `{revoked:bool}`。
- ticks：claims→GetSyncJob(tenant,404)→ListScheduleTicks→200 裸数组（与 ListRuns 同权限形状）。
- Create/Update SyncJob：handler 预校验（models）→400 `{"error":"invalid_schedule","detail":...}`；
  repo 再权威校验（legacy 规则见 §3）。GET 单读填 `schedule_authorization`
  （GetScheduleGrantView + MatchesLiveGeneration）。

## 7. workers / main / config / README

- `workers/sync_scheduler.go` + `sync_scheduler_test.go` 删除（stub 退役；workers.Clock/sleepLoop 保留给 auto-registration）；循环宿主=handlers.ScheduleRunner。
- main：`bootID=uuid.New()`（独立 schedule boot UUID，不复用 leaseHolder）→ScheduleAuthorizer(Start)→Interval>0 时 Runner.RunLoop(bg)→生命周期：cancelBG → authorizer.Stop()（有界）→ coordinator.Stop → joins → pool.Close。
- config：`OPENFOUNDRY_SYNC_SCHEDULER_AUTH_TTL_SECS`（默认300；显式 1..86400，越界启动错误）；`OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS` 显式 >30 启动错误（0=off）。
- README：ADR-0025 受限例外、调度开关、arm 实例级语义、重启后 auth_required。

## 8. 单测归属

主 GLM（非 PG）：models spec 校验（flavor/长度/Local 拒绝/enable）；authorizer 并发 stage/activate
CAS、迟到不覆盖、Stop 后不重填、过期清扫、MatchesLiveGeneration；coordinator cap；runner 摘要与
派发失败补记；HTTP 形状。PG GLM：`internal/repo/schedule_integration_test.go`（round-1 新增强制项：
arm 跨 def/跨租户不匹配零写、heartbeat 纯 FOR UPDATE 阻塞/跨期/释放后 false 且行原样、claim 两级
NOWAIT 争用、唯一键、锁后时钟、各阶段回滚、revision/CAS、迁移重放）。

## 9. 冻结面纪律

59 必测名/断言不删不弱化；不改 Dataset/Pipeline/go.mod/go.sum/冻结工具；`sync_runs` 与
lease/cancel/gate 语义零改动。
