# Troupe.go 设计

> 状态：M0 设计与验证基线（canonical identity / 规范哈希 / 错误码表已由 golden
> 向量锚定）。以 Orleans 10.0 为主要语义参照；继承 troupe.cr 全部里程碑经验
> （M1 单机核心 → M2 持久化/硬化 → M3 PG/TCP/集群 → M4 调度/Show → M5 流/目录
> → M6 生态）。跨语言共同决策由 [troupe-planning](https://github.com/ShiningRay/troupe) 协调。

## 1. 目标

- 提供 Go idiomatic、类型安全的 virtual actor 开发体验：goroutine + channel +
  `context.Context` + generics，而非 .NET 风格 API 的机械翻译。
- 从可验证的单机核心开始，逐步达到共享状态、跨进程故障接管和小型生产集群。
- 与 troupejs / trouperb / troupe.cr 共享核心语义、Provider 合同与 golden 测试向量，
  而非强求同一语法。
- 发挥 Go 运行时的真实优势：抢占式调度（CPU 密集 Turn 不饿死节点）、
  `-race` 数据竞争检测、内建 pprof、单二进制部署、跨平台交叉编译。
- 零外部依赖的核心包：`troupego` 本体只依赖标准库；SQL Provider、OTel 导出器
  等按需放独立子模块（见 §11）。

## 2. 非目标

- 不复制 Orleans 全部模块、.NET DI/serializer 或分布式事务。
- 不把 goroutine 轻量等同于 Actor 单激活或分布式容错：单激活由 ActorSlot
  single-flight + fencing 合同保证，不靠"谁也不该并发访问"的自觉。
- 不在没有共享存储/fencing 证据时宣称 production-ready。
- 不在公共协议 ADR（troupe-planning）采纳前宣称与其他 Troupe 语言互操作。
- 不强杀 goroutine 伪装资源已安全释放（Go 无法安全 kill 单个 goroutine，
  与 Crystal Fiber 同一教训）；取消永远是协作式，经 `context.Context` 传播。

## 3. 概念映射

| Troupe | 含义 | Go 设计方向 |
|---|---|---|
| Troupe / Stage | 集群 / 节点运行时 | `Troupe` 集群入口与 `Stage` 节点运行时（M1 起） |
| Actor / Role | 虚拟 Actor / 角色类型 | `Actor[P Props]` 泛型基类 + 显式 `Registry`；无宏，用显式注册 + 可选 codegen |
| Agent | 位置透明代理 | 泛型客户端 `Agent[I any]`，编译期类型安全；方法 ID 稳定字符串 |
| Props | 可恢复状态 | `schemaVersion`、`revision` CAS、显式序列化/复制边界；禁 func/chan/goroutine 入 Props |
| CallBoard / Turn | Actor 邮箱 / 串行执行 | 每 activation 一个有界 `chan CallEnvelope` + 单消费者 goroutine |
| Casting | 激活与 single-flight | Stage 内按 canonical ActorID 单飞（`sync.Map` + in-flight 去重） |
| Intermission | 空闲钝化 | 可注入时钟；等待 quiesce 后下场，释放 goroutine 与队列 |
| Improv / ReadOnly | 显式交错 | 后置实验（M4）：ReadOnly 走快照并发、Improv 走副本 + 显式冲突拒绝 |
| Cue / Reminder | 内存 timer / 持久提醒 | 分开 API：`time.Timer` 包装 vs 持久 claim + lease + 至少一次 |
| Show | 实时通知 | 在线尽力 fan-out；持久诉求走 Reminder/Outbox/持久流，不混称 |
| Roster / Playbill | 成员 / 放置与路由 | 先 HashRing（CRC32 golden 锚定）+ ownership；目录/资源放置后续 ADR |
| Rehearsal | 测试运行时 | 固定 namespace、内存 Provider、`testing/fclock` 虚拟时钟、确定性调度 |
| Director | 运维面 | `net/http` 健康端点 + metrics + pprof（M4），管理写操作不经 HTTP |

## 4. 最小单机核心（M1 交付范围）

1. `ActorID(namespace, role, key)` canonical identity（✅ M0 已锚定 golden 向量）。
2. 显式 `Role` 注册与方法白名单，拒绝生命周期/内部方法 RPC。
3. `PropStore` 接口与 Memory 实现：create/write 原子 CAS，read 返回独立快照。
4. `ActorSlot` 生命周期与 single-flight；失败取消 in-flight 并允许后续重试。
5. 每 Actor 单 goroutine 严格串行消费有界 CallBoard：FIFO、deadline、
   queue depth/bytes 上限。
6. `OnStage`/`OffStage` 回调、Intermission 空闲钝化、保存失败失效与重激活。
7. 可注入时钟、无网络的 Rehearsal、错误分类（✅ M0 已锚定码表）。

此阶段不支持 TCP、分布式所有权、交错、Show、持久 reminder 或热替换。

## 5. Go 执行模型

### 5.1 调度：抢占式是优势，不是正确性来源

- **一个 activation 一个消费者 goroutine**，顺序处理 CallBoard；strict 模式下
  等待用户异步 I/O 期间不放行下一 Turn（与 troupe.cr strict 语义一致）。
- Go 调度器是抢占式的（1.14+ 异步抢占）：CPU 密集 Turn 不会阻塞整个节点调度，
  这是相对 Crystal Fiber（协作式）的真实改进——troupe.cr 基准里"CPU 阻塞
  victim 延迟≈spin 全长"的问题在 Go 中不存在。
- 但抢占式 + 多核意味着**数据竞争是默认风险**：只有 activation goroutine
  能触碰 Props；一切跨 goroutine 状态经 channel 或显式同步原语。
  `-race` 在 CI 必跑（troupe.cr 无对应工具，这是 Go 的验证优势）。
- goroutine 泄漏是 Go 特有风险：每个 goroutine 必须有明确退出路径
  （context 取消 / 队列关闭 / Stage 关闭），测试用 goroutine 计数断言
  （Stage 关闭后消费者必须退出，替代 goleak 依赖的自研检查）。

### 5.2 CallBoard：有界 channel

- `CallBoard` = `chan CallEnvelope`，容量即 queue depth 上限；入队满即
  `CallRejectedError{Reason: backpressure, Retryable: true}`，明确拒绝而非无限排队。
- Stage 总预算：全局 in-flight 字节/调用数上限，防止单 Stage 无界内存。
- 响应 deadline 与执行占用分离：`context.WithDeadline` 到期释放调用方等待，
  不自动放行旧 Turn——原 Turn 占用队列直至 settle（UnknownOutcome 纤程模式的
  Go 等价物：超时后效果仍可能提交）。
- 协作取消：`context.Context` 从 Agent 一路传进 Turn
  （`CallContext` 包装携带 `Done()/Err()/Traceparent()`）；不能强行终止。

### 5.3 内存模型与复制边界

- Props 快照经显式复制边界交付：Memory Provider 走受控深拷贝
  （`Clone(Props) Props` 接口或 JSON round-trip 兜底），禁止调用方与 Actor
  意外共享可变结构——Go 没有值语义保护，这条比 Crystal 更需要纪律与测试。
- 激活期间 Props 只被 activation goroutine 访问；保存走 `revision` CAS。
- 后续 `ReadOnly` 基于已提交快照并发；`Improv` 必须定义提交/冲突规则
  （激活时 revision CAS，冲突显式拒绝），不采用隐式 last-write-wins。

## 6. 类型、序列化和版本

- Go 泛型提供 `Actor[P Props]` / `Agent` 的编译期类型安全；Go 没有宏，
  troupe.cr 的 `troupe_manifest` 宏等价物是**显式注册函数 + 可选的
  `go:generate` codegen**（M2 评估，首版手写注册与 troupe.cr 手写 manifest
  行为一致优先）。
- 方法注册产生稳定 role/method ID、参数 schema、版本与错误映射；
  参数校验在排队前完成（缺参/类型不符 → `InvalidArgumentError`，零副作用）。
- Props 通过显式 serializable DTO 定义；`encoding/json` 为默认边界，
  禁止任意对象图、goroutine、channel、func 进入 Props。
- `schemaVersion` 迁移逐级 v→v+1，失败即激活失败；所有权取得前不得
  创建/迁移共享 Props。
- 公共跨语言协议未冻结；不把 Go 结构体字段名默认当永久协议，
  golden 向量（identity/hash/errors/envelope）是唯一跨语言承诺。

## 7. 持久化与所有权

沿用 troupe.cr 已验证的三码分离合同，Provider 顺序：Memory → SQLite → PostgreSQL。

- `revision` 控制状态 CAS，`schemaVersion` 控制迁移，`epoch/fencingToken`
  控制所有权——三者独立，不可互相替代。
- 激活顺序：acquire → read → migrate/validate → fenced write → OnStage；
  旧 epoch 的 create/write/migrate/reminder-complete 必须失败。
- SQLite Provider（M2，开发用）：优先纯 Go driver（`modernc.org/sqlite`，
  无 CGO，保住交叉编译优势）；CAS/fencing 为单条 SQL 原子语句；
  `CrossNodeSafe() == false` 显式声明能力边界。
- PostgreSQL Provider（M3，生产）：acquire 单语句 RETURNING 带租期守卫、
  epoch 单调、fencing 在单条 UPDATE 的 WHERE 内原子校验；
  `CrossNodeSafe() == true`。
- 外部 HTTP/消息副作用需要接收方 epoch 或持久 Outbox（同事务提交 + 至少一次派发）。

## 8. 集群与演进

- 路由基础：统一 xxhash/CRC32 一致性环契约（golden/hash.json 已锚定 CRC32）；
  节点增删只移动相邻区间键（troupe.cr HashRing vnodes 语义）。
- 成员视图（Roster）与 ownership 分离；Roster incarnation 单调协商；
  分区期间依赖存储租约/fencing，不自欺为已解决脑裂。
- TCP transport 复用 troupe.cr ADR 0001 提议的 `troupe-envelope/1`：
  `magic("TRP1" u32 LE) + length(u32 LE) + crc32(u32 LE) + payload(JSON)`，
  长度上限读前拒绝、首帧认证、连接级 in-flight 上限、cancel 信封传播。
  Go 侧重读 troupe.cr 的 7 类反例（超限/坏帧/断连/取消传播）作为移植验收。
- 版本化目录、能力/资源放置、activation rebalancing 在共享容错后经
  统一 ADR 决定（troupe-planning M5 范围）。

## 9. 可靠性与运维

- 默认 RPC 不自动重试非幂等副作用；超时/断连是 `UnknownOutcomeError`。
- Cue 内存 timer 与持久 Reminder 分离；后者共享 claim、lease、至少一次、
  业务幂等兜底。
- Show 初始只做实时通知（在线尽力）；持久流需要日志、游标、ack/replay、
  保留与慢消费者策略（M5）。
- 可观测从 M1 纳入：结构化 lifecycle 事件（`slog` handler 接口）、
  metrics 计数器接口、W3C traceparent 经 `CallContext` 传播；
  OTel 导出适配器（OTLP JSON）作为独立可选包，核心零 OTel 依赖。
  pprof 端点随 Director（M4）默认提供——Go 运行时的免费运维面。
- 测试分层：单元（golden + 语义反例）、契约、真实 Provider、多进程、
  故障注入（kill -9 矩阵）、版本兼容与发布安装；`-race` 全程必跑。

## 10. 从 troupe.cr 迁移的经验清单

以下教训直接构成本仓的默认决策，不再重新发明：

1. **strict 串行先行**：交错（ReadOnly/Improv）在核心稳定后再引入，且必须
   带写饥饿/quiesce 反例（troupe.cr M4 教训）。
2. **有界与显式拒绝**：单 Actor depth + Stage 字节/激活/in-flight 多层预算；
   过载返回 `CallRejectedError{backpressure, retryable}`，不无限排队。
3. **超时≠失败**：`CALL_TIMEOUT{queued|executing}` 与 `UNKNOWN_OUTCOME`
   语义区分是跨线合同的一部分，第一版就定型（M0 已锚定码表）。
4. **激活顺序是 Provider 合同**：先 acquire 再读/迁移/写；旧 epoch 写拒绝
   以反例测试固化，不靠文档约定。
5. **测试运行时确定性**：Rehearsal 固定 namespace + 虚拟时钟 + 显式 pump，
   避免"看起来通过了"的假阳性（troupe.cr 从 Ruby 版继承的教训）。
6. **golden 向量驱动对齐**：跨语言一致性以读取同一 JSON 断言为锚点，
   不以相似 API 命名为锚点。
7. **完成定义**（见 §12）：每项功能必须有正常路径 + 关键反例 + 资源上限 +
   CI 可执行验证；分布式能力另需真实拓扑/Provider 证据。

## 11. 依赖策略

| 层 | 依赖 | 理由 |
|---|---|---|
| 核心包 `troupego` | 仅标准库 | 单二进制/交叉编译/供应链面最小；troupe.cr 同样零依赖 |
| SQLite Provider（M2） | `modernc.org/sqlite`（纯 Go） | 避免 CGO 破坏交叉编译；若性能不达标再评估 CGO 备选 |
| PostgreSQL Provider（M3） | `jackc/pgx/v5` | 生产 PG 事实标准；放独立包，核心不 import |
| OTel 导出（M6） | 零依赖（OTLP JSON 手写，同 troupe.cr） | sink 回调即 collector 推送点 |
| 测试 | 标准库 `testing` + 自研 goroutine 计数 | 不引入断言库/goleak，减少供应链面 |

## 12. 完成定义

“实现某功能”必须同时具备：

1. 代码与公开语义文档；
2. 正常路径与至少一个关键反例；
3. 资源上限、错误分类和失败恢复；
4. 对应 CI 可执行验证（含 `-race`）；
5. 对分布式能力，真实拓扑/Provider 证据。

仅有类型、接口、README 或单次 happy-path 调用不算完成。
