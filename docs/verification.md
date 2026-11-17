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
