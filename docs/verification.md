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
