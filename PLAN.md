# Troupe.go 实施计划

> M0（设计与验证基线）于 2026-11-17 本轮交付：仓库、方案/计划文档、
> canonical identity + 规范哈希 + 错误码表（golden 向量锚定）与 CI。
> 以下 M1+ 均未实施，未标注完成；不设虚构日期。
> 里程碑语义与 troupe.cr 对齐（其 M1–M6 已完成的验收记录可作 Go 版
> 反例测试的移植来源）。

## M0：设计与验证基线（已完成，2026-11-17）

- [x] 建立公开仓库、MIT、go.mod（`github.com/ShiningRay/troupego`，go 1.23+）。
- [x] 编写目标架构、执行/持久化/集群契约（DESIGN.md）与 Orleans 差距
      （docs/orleans-gap.md）。
- [x] `ActorID`/`ParseActorID`（troupe-identity/1）、`CanonicalHash`
      （troupe-hash-crc32/1）、错误类型与 `WireCode`（troupe-errors/1），
      全部由 `golden/*.json` 契约测试锚定。
- [x] CI 执行 `gofmt -l`、`go vet`、`go test -race`（ubuntu + windows 双平台）。
- [x] 明确没有 Actor 运行时，不声称单机或集群可用。

**门禁**：干净 checkout 可执行 fmt/vet/test；CI 名称不把脚手架测试描述为
Actor 验收。（✅ 本地 go1.26 windows 验证 `ok github.com/ShiningRay/troupego`）

## M1：语义契约与最小单机核心

- [ ] `Role` manifest / `Registry`：显式注册、方法白名单、拒绝生命周期方法 RPC；
      `Actor[P Props]` 泛型基类与 `save`（revision CAS + epoch fencing 显式提交）。
- [ ] Memory `PropStore`：原子 create/write CAS、read 独立快照（受控深拷贝
      + JSON round-trip 边界验证）、revision/schemaVersion。
- [ ] `Stage`/`Agent`：显式注册、位置透明本地调用、deadline、错误分类透出。
- [ ] `ActorSlot`：single-flight（并发首调只激活一次）、OnStage/OffStage、
      Intermission 空闲钝化、保存失败失效/重激活。
- [ ] CallBoard：有界 channel + 单消费者 goroutine，严格串行 FIFO、排队过期
      （零副作用）、执行占用（超时不放行旧 Turn）、queue depth/bytes、
      Stage activation 上限。
- [ ] `Rehearsal` 测试运行时：固定 namespace、Memory Provider、可注入虚拟时钟、
      显式 pump 确定性调度。
- [ ] goroutine 生命周期断言：Stage 关闭后消费者/钝化计时器 goroutine 退出
      （计数断言，`-race` 必跑）。

**验收**（对齐 troupe.cr M1 反例集并移植）：并发首调只激活一次；100 个写
Turn 不交错；排队过期无副作用；执行超时不放行旧 Turn；CAS/保存失败从最后
提交快照恢复；任意目录名下 Rehearsal 一致；v1→v3 逐级迁移且失败不写；
Stage 关闭零 goroutine 泄漏。

## M2：本地持久化、语义硬化和 API 收口

- [ ] SQLite `PropStore`（`modernc.org/sqlite` 纯 Go）：单条 SQL 原子 CAS 与
      fencing、重开恢复、`CrossNodeSafe()==false` 显式声明；
      若纯 Go driver 成为瓶颈再评估 CGO 备选（决策记录进 docs）。
- [ ] UnknownOutcome 模式证明（超时后原 Turn 占用通道至 settle、效果提交、
      无自动重试）；幂等记录与 Props 单事务原子提交（同键同指纹重放/
      不同指纹拒绝/失败重放/保留期过期）。
- [ ] 协作取消全链路：`CallContext` → Turn 检查点 → transport cancel 信封（M3）。
- [ ] 结构化 lifecycle/call metrics（计数器接口）、`slog` 事件、W3C traceparent
      解析/派生（非法父静默降级、全零拒绝）、Turn 边界 Trace 事件。
- [ ] 类型安全 `Agent`（泛型包装，编译期参数校验）、公开 API 文档
      （`go doc`/pkg.go.dev）、minimal Cart 示例。
- [ ] 基准五场景（对齐 troupe.cr bench/run.cr 口径：激活吞吐、KB/Actor、
      热点 vs 分散、调度延迟、队列饱和）；本仓自基线，
      跨语言比较交 troupe-bench-suite 统一口径。

**验收**：重启后恢复；复制边界一致（快照逃逸反例）；调用链可见；
泛型/手写注册行为一致；所有过载路径有界并显式拒绝。

## M3：共享状态与生产容错

- [ ] PostgreSQL `PropsStore`/`OwnershipStore`（`jackc/pgx/v5`，独立包）：
      acquire 单语句 RETURNING 带租期、跨实例租期内拒绝可重试、epoch 单调、
      renew 同 epoch 续租、fencing 在 UPDATE WHERE 内原子校验、幂等记录同事务；
      `CrossNodeSafe()==true`；CI 以 postgres service 必跑真实 PG 用例。
- [ ] 激活先 acquire 再读/迁移/写；端到端接管反例（旧实例 save 被 fencing
      拒绝并失效，新实例从 PG 已提交快照继续）。
- [ ] TCP transport（`troupe-envelope/1`，ADR 0001）：帧协议（magic+length
      +CRC32，超限读前拒绝不分配内存）、连接级 in-flight、方法白名单、
      deadline/断连错误分类、cancel 信封协作取消传播；
      认证 token + 轮换/宽限/撤销（常量时间比较）+ TLS 方案指引。
- [ ] `Roster` 成员视图（join/heartbeat/sweep、incarnation 单调协商）、
      `HashRing` vnodes+CRC32（golden 已锚定）、`Router` 死亡节点键漂移、
      优雅 drain 三段式（begin 拒新 retryable → idle → finish）。
- [ ] 持久 Reminder：`ReminderStore`（Memory/PG）+ `ReminderRunner`——
      稳定键、归属过滤、原子 claim（租期，DB 时钟）、complete/release fencing、
      失败重投至少一次（业务幂等兜底）；volatile timer 与 durable reminder 分离。
- [ ] 外部副作用 Outbox：`EmitEvent` 与 Props 同事务提交 + `OutboxDispatcher`
      至少一次派发。

**验收**：至少三个独立 Go 进程连接真实 PostgreSQL，扩容、kill -9、冻结恢复后
无未防护双活成功写；旧 epoch 写拒绝；Reminder 跨进程恢复与重复幂等；
TCP 移植 troupe.cr 7 类反例全绿；POSIX CI 必跑故障用例。

## M4：调度扩展与实时 Show

- [ ] 调度模式：strict（默认）/ `read_only`（并发已提交快照，禁写——save 显式
      拒绝）/ `improv`（并发可写副本，save 以激活时 revision CAS，冲突显式拒绝）；
      写饥饿反例（长只读阻塞期间 strict 立即完成）与 quiesce 反例
      （drain/idle 等待并发在飞）。
- [ ] Show realtime：`ShowHub`/`Show`——Actor Audience 经完整 Turn 交付
      （白名单/串行/持久化语义不变），边界回调（非 Actor 观察者）与 Actor
      订阅分离；在线尽力放弃不重投；跨 Stage 订阅显式绑定；
      慢订阅者背压隔离（failed 不影响其他订阅者）；离线不保证。
- [ ] `Director` + HTTP 端点（`net/http`）：/health /metrics /events +
      pprof；health（running/draining/closed）、per-Stage 计数
      （activation_failures/save_failures/turns_expired/turns_failed）、
      recent_events 环形样本（内存有界）；管理写操作不经 HTTP；CLI 演示。

**验收**：交错组合矩阵、无写饥饿、慢订阅者不耗尽全局内存；
不将 Show 称为持久流。

## M5：可靠消息、目录与互操作（统一 ADR 后）

- [ ] 持久流：`StreamStore` 契约 + Memory/PG Provider——事件日志 seq 全局单调
      per-topic 保序、per-consumer 持久游标（ack 只前进，回退拒绝）、不 ack
      重放 at-least-once、retain_before 保留裁剪（慢消费者丢失为显式代价）、
      replay 显式；优先复用成熟消息系统的适配评估。
- [ ] `VersionedDirectory`（per-entry version 单调 + view_version + 旧节点复活
      抢注册拒绝）、`Placer`（capabilities 子集匹配 + 负载最少者）、
      `Rebalancer`（死亡节点 handoff / 新节点 overflow 迁移，旧计划重放保护）。
- [ ] 公共协议完整锚点：envelope golden 向量 + 帧协议契约测试
      （对齐 troupe.cr spec/golden_fixtures_spec.cr 全部四文件）；
      troupe-planning 采纳后向量迁入统一仓。
- [ ] 与 troupe-planning golden fixtures/contract tests 对齐并接入跨语言
      验证流程。

**验收**：按 ADR 限定范围验证，不以共享 JSON 或 goroutine/channel 名称宣称
多语言集群。

## M6：高级模块与生态

- [ ] OTel 导出适配器（OTLP JSON v1，零外部依赖，sink 回调即 collector
      推送点）、版本兼容矩阵、滚动升级/回滚指引。
- [ ] 持久 Outbox 完善、事件溯源扩展点（rebuild 顺序折叠/快照+增量==全量
      契约）；分布式事务独立评估（DESIGN 边界内不做）。
- [ ] Provider/故障/性能持续验证；公开不支持项与容量边界（verification.md
      + COMPATIBILITY 矩阵 + 基线基准）。
- [ ] 发布：tag 版本、Go module proxy 可拉取、pkg.go.dev 文档可用。

## 风险与对应

| 风险 | 对策 |
|---|---|
| 数据竞争（多核抢占式默认风险） | Props 仅 activation goroutine 访问；CI `-race` 必跑；快照复制边界反例 |
| goroutine/channel 泄漏 | 每 goroutine 显式退出路径；Stage 关闭计数断言；Intermission 定时器回收 |
| channel 死锁（有界队列 + 关闭时序） | 入队选 default 分支显式拒绝；关闭经三段式 drain；drain 反例 |
| 强杀 goroutine 诱惑（取消不及时） | 只做协作取消；长 Turn 文档要求检查点；超时语义=UnknownOutcome 不放行 |
| 纯 Go SQLite 性能不达预期 | M2 基准先行；CGO 备选决策记录进 docs；开发/生产分级声明 |
| 把 goroutine 轻量当分布式正确性 | single-flight + fencing 反例作为 Provider 合同；统一原则 #2 |
| 静态类型与公共协议耦合过紧 | 显式版本化 DTO/manifest；协议独立 golden vectors（M0 已锚定三项） |
| 测试假阳性（时钟/调度依赖） | Rehearsal 虚拟时钟 + 显式 pump；断言真实旧快照与接管 epoch |
| 把脚手架当框架 | README/CI/版本均标 M0；功能只在可执行反例后声明 |

## 与其他 Troupe 仓库的分工

- 语义决策与跨语言向量：troupe-planning（本仓 golden/ 为其提案的锚点副本）。
- 参照实现：troupe.cr（M1–M6 验收与反例最完整，Go 版反例测试优先移植其
  spec/ 用例语义）；troupejs（TS 主线，功能面最广）。
- 示范应用：troupe-game-demo（TS）；Go 版示例应用待核心稳定后另议。
