# 验证记录

每个里程碑的实际命令、环境与结果。完成定义要求：正常路径 + 关键反例 +
资源上限 + CI 可执行验证（DESIGN §12）。

## M0（2026-11-17）

- 环境：go1.26.0 windows/amd64（本机 Go 位于 `%LOCALAPPDATA%\Programs\Go\bin`）。
- 命令：`gofmt -l .`（无输出）、`go vet ./...`、`go test -race ./...`、`go build ./...`。
- 结果：7 测试全绿（ActorID round-trip/反例 + golden identity/hash/errors 契约）。
- CI：run 44048828247，ubuntu+windows 双 job success。

## M1：语义契约与最小单机核心（2026-11-17）

- 环境：go1.26.0 windows/amd64。
- 命令：`gofmt -l .`（无输出）、`go vet ./...`、`go test -race -count=1 ./...`。
- 结果：**39 测试全绿（含 -race）**。验收对照（PLAN M1）：

| 验收项 | 证据（测试） |
|---|---|
| 并发首调只激活一次 | `TestConcurrentFirstCallActivatesOnce`（32 并发同步起跑，activated 事件恰 1 次） |
| 100 个写 Turn 不交错 | `TestStrictSerialTurns`（100 并发经背压重试入队，items=100、revision=101、FIFO 无丢失更新） |
| 排队过期无副作用 | `TestQueuedExpiryZeroSideEffects` + `TestRehearsalVirtualClockDeadline`（CALL_TIMEOUT{queued}，零写入，turn_expired 事件） |
| 执行超时不放行旧 Turn | `TestExecutionTimeoutUnknownOutcome`（CALL_TIMEOUT{executing}；后续 Turn 只在效果提交后执行；迟到成功仍按结果未知拒绝） |
| CAS/保存失败从最后提交快照恢复 | `TestSaveConflictInvalidatesAndReactivates`（外部写者制造冲突 → save_failed → 失效 → 重激活看到已提交快照，未提交状态被丢弃） |
| 任意目录名下 Rehearsal 一致 | `TestRehearsalNamespaceIndependence` + `TestRehearsalDeterministicPump`（固定 namespace、虚拟时钟、显式 pump） |
| v1→v3 逐级迁移且失败不写 | `TestMigrationChainV1ToV3` + `TestMigrationFailureNoWrite`（步进 1→2→3；步骤失败 → ActivationError 且记录保持 v1 原始数据） |
| Stage 关闭零 goroutine 泄漏（Go 增项） | `TestStageCloseNoGoroutineLeak` + `TestConsumerExitedAssertion`（8 激活 +8 消费者；Close 后回落 baseline；exited 通道断言） |
| 背压显式拒绝（边界） | `TestBackpressureExplicitRejection`（深度 1：第二个调用即拒绝 backpressure/retryable；消费后重试成功） |
| 激活上限（边界） | `TestMaxActivationsLimit`（MaxActivations=1：新 slot 拒绝，已激活 actor 的调用不受影响） |
| 停机拒绝（边界） | `TestShutdownRejectsNewAndQueued`（队列内调用排空时逐个 shutdown 拒绝；停机后新 Call 拒绝） |
| 生命周期钩子（边界） | `TestHooksOnOffStage` + `TestOnStageFailureActivationError`（OnStage 失败 → ActivationError；OffStage 随下线执行） |
| 白名单/保留名（边界） | `TestUnknownRoleAndWhitelist` + `TestManifestRegistration`（保留名不可注册/不可 RPC；参数校验失败零 Props 创建） |
| store 契约（边界） | props_test（create exists / CAS revision / fencing 优先于 CAS / read 隔离 / cross_node_safe=false / renew） |

- 与 troupe.cr 的刻意差异：`MaxActivations` 只约束新 slot 的创建
  （troupe.cr 在 slot 查找前无条件检查，导致已激活 actor 的后续调用在满载时
  也被拒绝）；本仓语义 = "上限约束激活槽数量"，热 actor 调用复用既有 slot。
- M1 未实现（对齐 PLAN）：TCP、分布式所有权（epoch fence 已由 store 契约锚定，
  跨节点接管在 M3 验证）、交错（read_only/improv）、Show、持久 reminder、热替换。

## M2：本地持久化、语义硬化、观测与基准（2026-11-17）

- 环境：go1.26.0 windows/amd64，modernc.org/sqlite v1.62.0（纯 Go，无 CGO）。
- 命令：`gofmt -l .`（无输出）、`go vet ./...`、
  `go test -race -count=1 -timeout 120s ./...`、
  `go test -bench=. -benchtime=200ms -benchmem -run='^$' .`。
- 结果：**54 测试全绿（含 -race）**。验收对照（PLAN M2）：

| 验收项 | 证据 |
|---|---|
| SQLite 重开恢复 | `TestSQLiteReopenPersistence`（revision/数据/ownership 跨进程恢复）；`TestStageWithSQLitePersistenceRoundTrip`（新 Stage 重激活读回 3 次写入） |
| 单语句 CAS+fencing | `TestSQLiteCreateWriteRead`（fencing 优先：新 epoch 取代后旧 epoch 拒绝 ownership，即使 revision 匹配） |
| 幂等与 Props 原子提交 | `TestIdempotentReplayAndConflict`（同键同指纹重放 fn 跳过 / 不同指纹 IdempotencyConflict）+ `TestIdempotentFailureRecordedAndReplayed`（失败记录→重抛→修复后新键成功）+ `TestSQLiteJournalAtomicCommit`（CAS 失败 journal 回滚、过期过滤） |
| 协作取消 | `TestCooperativeCancel`（Turn 检查点提前收束、部分进度提交、已 settle 不可再取消） |
| W3C trace | `TestTraceparentValidation`（全零/大写/ff/缺 flags 拒绝）+ `TestTraceparentDerivation`（同 traceId 新 spanId、非法父静默降级）+ `TestTurnBoundaryTraceEvents`（completed/expired 边界事件含父子上下文） |
| metrics | `TestStageMetrics`（activations/turns_completed/turns_failed/rejections per role 计数准确） |
| 类型安全调用 | `TestInvokeTyped`（泛型结果类型、白名单运行时拒绝、参数预检零副作用） |
| KB/Actor | `TestMemoryPerActor` ≈ **2.10 KB/Actor**（5000 激活，含 slot/队列/实例；troupe.cr 基线 5.3 KB 同口径自比） |
| CPU 阻塞调度延迟 | `TestCpuSpinNeighborLatency`：spin 80ms 期间邻居 Actor 调用延迟 ≈ 0——Go 异步抢占下 CPU 密集 Turn 不阻塞节点调度（troupe.cr Fiber 场景 victim≈spin 全长，此为 Go 相对优势的量化） |
| 基线基准（windows 本机，正式数据需 release+固定机复核） | 激活 37.4µs/op（≈26.8k/s，5.2KB/61 allocs）；热点 11.7µs（85k/s）；分散 6.1µs（并行 165k/s）；满队拒绝 2.2µs——本仓自基线，不与其他语言异口径比较 |
| goroutine 生命周期 | M1 断言延续全绿 |

- 实现注记：SQLite `writeCore` 的拒绝分类在同一事务连接内读当前 epoch
  （单连接池 `SetMaxOpenConns(1)` 下外部查询会互相等锁——首轮验证发现并修复）；
  `Invoke` 仅适用于 live 调度（Manual 模式下 Call 后立即 Await 会阻塞，
  Rehearsal 用法为 Pump 后经 Future.Await——已写入测试注释与文档）。

## M3：共享状态与生产容错（2026-11-17，A/B/C/D 四次迭代提交）

- 环境：go1.26.0 windows/amd64；PostgreSQL 15（本机 docker postgres:15-alpine +
  CI postgres service）；pgx/v5 v5.13.0；modernc.org/sqlite v1.62.0。
- 命令：`TROUPE_PG_URL=... go test -race -count=1 ./troupepg/`、
  `go test -race -count=1 ./...`、`go test -count=1 -v . ./transport/`。
- 结果：**根包 66 + transport 11 + PG 7 = 84 用例 `-race` 全绿**。验收对照（PLAN M3）：

| 验收项 | 证据 |
|---|---|
| ≥3 独立进程连真实 PG + kill -9 恢复 | `TestPGMultiProcessRecovery`（两独立子进程 os.Exit(1) 硬退出≈kill -9，父进程接管后状态 2 续到 3） |
| 端到端接管反例 | `TestPGStageTakeover`（双 Stage 实例：接管→旧实例 save fencing 拒绝失效 save_failures=1→重激活从已提交快照继续 1→2→3）+ `TestPGLeaseTakeoverFencing`（租期内跨实例 acquire 拒绝、过期接管 epoch 单调+1、旧 epoch 写拒绝） |
| acquire 租期守卫/单语句 | `TestPGCreateWriteCAS` + store.go acquire upsert WHERE 租期守卫 RETURNING epoch |
| journal 同事务 | `TestPGJournalAtomicCommit`（CAS 失败 journal 回滚、过期过滤） |
| TCP 帧协议 | `TestFrameRoundTrip` + `TestFrameAgainstGoldenEnvelope`（golden/envelope.json sample_request 帧锚点）+ `TestFrameProtocolViolations`（CRC 损坏/magic/超限读前拒绝） |
| 认证轮换 | `TestAuthenticatorRotation`（rotate 宽限/revoke 立即失效/常量时间比较）+ `TestAuthOverWire`（未认证断连/错误 token/轮换宽限 e2e/撤销后失效） |
| 错误过线分类 | `TestErrorClassificationOverWire`（whitelist/unknown role/invalid argument 分类保持）；ec 用 troupe-errors/1 稳定码（对齐 ADR 提案；troupe.cr 实现为 Crystal 类名——差异记录，跨语言对齐以 golden/errors.json 为准） |
| deadline/cancel/断连 | `TestDeadlineExecutingUnknownOutcomeOverWire`（executing 未知、迟到结果不放行、效果照常提交）+ `TestCancelEnvelopePropagation`（cancel 信封→Turn 检查点提前收束）+ `TestDisconnectUnknownOutcome`（断连 UnknownOutcome、服务端效果照常提交） |
| 连接级 in-flight | `TestInflightCap`（拒绝经 rep 送达 future：backpressure retryable） |
| Roster incarnation | `TestRosterIncarnationNegotiation`（活成员重入团拒绝、死亡重入团+1、心跳守卫）+ `TestRosterMarkDeadIncarnationGuard` |
| HashRing 相邻区间 | `TestHashRingGoldenCRCAndMovement`（1000 键快照：移除只动该节点键、重加入归属恢复） |
| Router 键漂移 | `TestRouterDriftOnDeath`（死亡节点键漂移后继、其他键不动、复活恢复） |
| 优雅 drain 三段式 | `TestGracefulDrain`（拒新 draining retryable→未排空 finish 拒绝→排空后停机；Manual 模式确定性） |
| Reminder claim fencing + 归属过滤 | `TestReminderClaimFencingAndLeaseTakeover`（租期内他实例不可认领/接管认领 version+1/stale complete 拒绝）+ `TestPGReminderOwnershipFilterAndFencing`（归属过滤：他实例不认领非本节点 actor；claim 租期 fencing 语义澄清——所有权转移阻止新认领不撤销在途认领） |
| Reminder 至少一次 | `TestReminderDeliversTurnAndCompletes` + `TestReminderFailureRedeliversAtLeastOnce`（失败 release→重投成功、fn 尝试=2） |
| Outbox 同事务+至少一次 | `TestOutboxAtomicCommitAndDispatch`（emit+save 同事务、handler 失败重投、MarkSent 后停止）+ `TestPGOutboxCommitAndDispatch`（CAS 失败 outbox 回滚）+ `TestOutboxSurvivesReactivation`（失效重激活事件不丢）+ `TestEmitEventWithoutOutboxStoreFailsFast`（不支持 store 立即报错不静默丢失） |

- 实现注记：
  - reminder fencing 是 **claim 租期**（status_version+holder+claim 租期，DB 时钟）——
    所有权转移只经归属过滤阻止新认领，不撤销在途认领（交付中效果保持，至少一次语义）。
  - PG claim SQL 占位符需显式类型标注（`$1::text`/`$2::float8`）——pgx 42P18
    （could not determine data type）实测坑位。
  - PG 测试 openPair 以 TRUNCATE 清共享库残留（容器跨 go test 持久）。
  - ec 稳定码 vs troupe.cr 类名差异：以 ADR 0001 提案（troupe-errors/1 码表）为准，
    待 troupe-planning 采纳后全语言统一。

## M4：调度扩展与实时 Show（2026-11-17）

- 环境：go1.26.0 windows/amd64。
- 命令：`go test -race -count=1 ./...`、`go test -race -count=3 -run "TestShow|TestDirector|TestHTTP|TestReadOnly|TestImprov|TestQuiesce" .`、
  `go run ./examples/cart`。
- 结果：全仓用例 `-race` 全绿（root 77 / transport 11 / PG 7）。验收对照（PLAN M4）：

| 验收项 | 证据 |
|---|---|
| 写饥饿反例 | `TestReadOnlyNoWriteStarvation`（长只读阻塞闸门期间 strict 立即完成，延迟阈值断言） |
| 只读快照隔离 + 禁写 | `TestReadOnlySnapshotIsolationAndNoWrite`（只读见已提交快照；Save → InvalidActorStateError） |
| improv 冲突显式拒绝 | `TestImprovConflictExplicitRejection`（快照读取与 Save 间外部交错 → ConflictError{revision}；影子失败不失效主激活、不产生 save_failed；外部写后主激活 strict Save 按既有语义冲突失效重激活） |
| quiesce 等待并发在飞 | `TestQuiesceWaitsConcurrentInflight`（并发在飞期间 Idle=false、finish 拒绝；完成后排空停机） |
| Show Actor Audience 经 Turn | `TestShowActorAudienceViaFullTurn`（交付经 Stage 管道：Props 持久化副作用可见；未订阅 topic 全零报告） |
| 边界回调分离 + panic 隔离 | `TestShowExternalCallbackAndIsolation`（回调 panic 计 failed 不影响其他订阅者） |
| 慢订阅者背压隔离 | `TestShowSlowSubscriberBackpressureIsolation`（跨 Stage 绑定；慢订阅者队列满 failed=1、健康订阅者 delivered=1） |
| 离线不保证 | `TestShowOfflineNotGuaranteed`（draining/closed 发布 → failed） |
| 退订/超时放弃 | `TestShowUnsubscribeAndPublishTimeout`（退订后不交付；deadline 到期 failed 返回不阻塞） |
| Director 健康/指标/事件 | `TestDirectorHealthMetricsEvents`（running/draining/closed、metrics 快照、recent_events 环形 ≤200、管理 API drain/shutdown） |
| HTTP 端点 + pprof | `TestHTTPHealthEndpoint`（/health /metrics /events /debug/pprof/ 全可达；管理写不经 HTTP——无写路由） |
| 示例冒烟 | `go run ./examples/cart`（激活钩子/类型安全 Turn/只读 peek/Invoke）+ examples/director 构建 |

- 实现注记：
  - **emit 重入死锁（实测发现并修复）**：Director.AddStage 运行时重挂 OnEvent
    与消费者 goroutine 的 emit 读存在数据竞争（-race 捕获）；修复时初版用
    stage.mu 导致 prepareCall 持锁路径上的 emit 自死锁（白名单拒绝悬挂）——
    最终以独立 `handlerMu sync.RWMutex` 解决。此为 Go 版相对 troupe.cr 的新坑：
    Crystal 同步原语不可重入但锁分离习惯不同。
  - Manual 模式并发 Turn 在 stage.Call 调用方 goroutine 内同步执行（确定性）；
    异步在飞场景（quiesce/背压）以 live 模式验证。
  - improv 的冲突 = Turn 开跑时快照 revision 与提交时 store revision 的 CAS 差；
    Manual 同步执行下快照读取与提交之间无法交错（天然无冲突），冲突测试需 live + gate。

## M6：高级模块与生态（2026-11-17）

- 命令：`go test -race -count=1 ./...`、`go test -race -count=3 -run "TestOTel|TestEventSourcing" .`。
- 结果：全仓 `-race` 全绿。验收对照（PLAN M6）：

| 验收项 | 证据 |
|---|---|
| OTel 导出适配器 | `TestOTelAdapterExportMetricsAndSpans`（OTLP/JSON v1：metrics monotonic Sum 计数、spans traceId/spanId/status OK、ExportAll 后清空）+ `TestOTelRingCapBounded`（环形 cap 有界溢出丢弃） |
| Attach 链式恢复 | Adapter Attach 捕获并恢复既有 OnEvent/OnTrace handler（handlerMu 快照） |
| 事件溯源一致性 | `TestEventSourcingRebuildConsistency`（Turn append 事件 → 独立重建者一致 175/3；快照+增量 == 全量 JSON 相等；apply 失败上抛） |
| 版本兼容矩阵 | docs/COMPATIBILITY.md（跨语言承诺冻结级/API alpha 级/Provider 能力矩阵/不承诺项） |
| 滚动升级指引 | docs/ROLLING_UPGRADE.md（存储先行/fencing 安全网/不兼容停机换/schemaVersion 逐级/已知边界） |
| 发布 | tag v0.1.0-alpha.1（Go module：github.com/ShiningRay/troupego） |

- 实现注记：Rebuild 每次调用唯一 consumer（全量读取——固定 consumer 游标会被
  PollEach ack 推进，第二次重建读不到事件——实测发现修正）；ev.Data 为信封内层
  data（decodeEnvelope 已解出——测试解码层级曾写错，已修正）。
